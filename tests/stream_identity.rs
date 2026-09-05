mod common;

use std::time::Duration;

use pipestream_search::node::{Bm25Shard, NodeConfig, NodeServiceImpl};
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::{
    stream_search_request as request, stream_search_response as response, DocumentIdentity,
    ResolveStreamIdentities, StartStreamSearch, StopStreamSearch, StreamIdentityLimits,
    StreamSearchRequest, StreamSearchResponse,
};
use pipestream_search::postings::{AnalyzedDoc, Bm25Store};
use pipestream_search::vector::{VectorIndex, EMBEDDED_TURBOVEC};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Channel, Server};

fn store(version: u64) -> Bm25Shard {
    let mut store = Bm25Store::new();
    for row in 0..4u32 {
        store.add_document(
            row,
            "word".into(),
            AnalyzedDoc::body(vec![("word".into(), 1, vec![(0, 4)])], 1),
        );
        if row != 2 {
            store
                .source_archive_mut()
                .attach_source_with_identity(
                    row,
                    &common::protobuf_source("word", "key"),
                    Some(row),
                    Some(&DocumentIdentity {
                        document_key: vec![0, 255, row as u8],
                        version,
                        chunk_ordinal: Some(row),
                    }),
                )
                .unwrap();
        }
    }
    Bm25Shard::Building(store)
}

async fn fixture() -> (
    NodeServiceImpl,
    NodeServiceClient<Channel>,
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    String,
) {
    let corpus = common::unit_vectors(4, 64, 721);
    let mut index = VectorIndex::create(EMBEDDED_TURBOVEC, 64, 4).unwrap();
    index.add(&corpus, 64).unwrap();
    index.prepare().unwrap();
    let mut live = pipestream_search::live_docs::LiveDocs::default();
    live.delete(1);
    let node = NodeServiceImpl::new(
        Some(index),
        NodeConfig {
            slot_offset: 100,
            coalesce: false,
            ..Default::default()
        },
    )
    .with_bm25(Some(store(7)))
    .with_live_docs(live)
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(
        Server::builder()
            .add_service(
                node.clone()
                    .into_server(pipestream_search::MAX_MESSAGE_BYTES),
            )
            .serve_with_incoming(pipestream_search::harness::nodelay_incoming(listener)),
    );
    let client = NodeServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    (node, client, server, format!("http://{addr}"))
}

fn limits() -> StreamIdentityLimits {
    StreamIdentityLimits {
        max_rows: 4,
        max_response_bytes: 4096,
        timeout_ms: 5000,
    }
}

async fn start(
    client: &mut NodeServiceClient<Channel>,
    limits: StreamIdentityLimits,
) -> (
    mpsc::Sender<StreamSearchRequest>,
    tonic::Streaming<StreamSearchResponse>,
) {
    let (tx, rx) = mpsc::channel(4);
    tx.send(StreamSearchRequest {
        payload: Some(request::Payload::Start(StartStreamSearch {
            vector: common::unit_vectors(1, 64, 721),
            identity_limits: Some(limits),
            ..Default::default()
        })),
    })
    .await
    .unwrap();
    let inbound = client
        .stream_search(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
    (tx, inbound)
}

async fn ready(inbound: &mut tonic::Streaming<StreamSearchResponse>) {
    loop {
        match inbound.message().await.unwrap().unwrap().payload.unwrap() {
            response::Payload::Batch(batch) => {
                assert_eq!(batch.hits.len() % 12, 0);
                assert!(batch
                    .hits
                    .chunks_exact(12)
                    .all(|row| u64::from_le_bytes(row[..8].try_into().unwrap()) != 101));
            }
            response::Payload::IdentityReady(ready) => {
                assert!(ready.scan.unwrap().completed);
                return;
            }
            other => panic!("unexpected {other:?}"),
        }
    }
}

async fn select(tx: &mpsc::Sender<StreamSearchRequest>, ids: Vec<u64>) {
    tx.send(StreamSearchRequest {
        payload: Some(request::Payload::ResolveIdentities(
            ResolveStreamIdentities { vector_ids: ids },
        )),
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nested_relays_resolve_original_identities_after_leaf_rows_are_replaced() {
    let (node, _client, server, addr) = fixture().await;
    let (relay, _, first) = pipestream_search::harness::start_relay(vec![addr]).await;
    let (relay, _, second) = pipestream_search::harness::start_relay(vec![relay]).await;
    let mut client = NodeServiceClient::connect(relay).await.unwrap();
    let (tx, mut inbound) = start(&mut client, limits()).await;
    ready(&mut inbound).await;
    tokio::time::timeout(
        Duration::from_secs(2),
        tokio::task::spawn_blocking(move || node.with_bm25(Some(store(99)))),
    )
    .await
    .expect("relays must not retain the leaf's scan lock")
    .unwrap();
    select(&tx, vec![103, 102, 100]).await;
    let response::Payload::Identities(found) =
        inbound.message().await.unwrap().unwrap().payload.unwrap()
    else {
        panic!("identities")
    };
    assert_eq!(
        found
            .rows
            .iter()
            .map(|row| row.vector_id)
            .collect::<Vec<_>>(),
        [103, 102, 100]
    );
    assert_eq!(found.rows[0].identity.as_ref().unwrap().version, 7);
    assert!(found.rows[1].identity.is_none());
    assert_eq!(
        found.rows[2].identity.as_ref().unwrap().document_key,
        [0, 255, 0]
    );
    let response::Payload::Summary(summary) =
        inbound.message().await.unwrap().unwrap().payload.unwrap()
    else {
        panic!("summary")
    };
    assert!(summary.completed);
    assert!(inbound.message().await.unwrap().is_none());
    first.abort();
    second.abort();
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn relays_enforce_selection_bounds_and_release_a_timed_out_child_exchange() {
    let (_, _, server, addr) = fixture().await;
    let (relay, _, relay_task) = pipestream_search::harness::start_relay(vec![addr]).await;
    let mut client = NodeServiceClient::connect(relay).await.unwrap();
    for ids in [vec![99], vec![100, 100], vec![104]] {
        let (tx, mut inbound) = start(&mut client, limits()).await;
        ready(&mut inbound).await;
        select(&tx, ids).await;
        assert_eq!(
            inbound.message().await.unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }
    let mut short = limits();
    short.timeout_ms = 50;
    let (_tx, mut inbound) = start(&mut client, short).await;
    ready(&mut inbound).await;
    assert!(
        tokio::time::timeout(Duration::from_secs(2), inbound.message())
            .await
            .unwrap()
            .is_err()
    );
    let (tx, mut inbound) = start(&mut client, limits()).await;
    ready(&mut inbound).await;
    tx.send(StreamSearchRequest {
        payload: Some(request::Payload::Stop(StopStreamSearch {})),
    })
    .await
    .unwrap();
    let response::Payload::Summary(summary) =
        inbound.message().await.unwrap().unwrap().payload.unwrap()
    else {
        panic!("stopped summary")
    };
    assert!(!summary.completed);
    relay_task.abort();
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn solo_classic_scan_carries_the_same_identity_and_excludes_deleted_rows() {
    let (_, mut client, server, _) = fixture().await;
    let mut inbound = client
        .search_shard(tokio_stream::iter([
            pipestream_search::pb::SearchShardRequest {
                payload: Some(pipestream_search::pb::search_shard_request::Payload::Start(
                    pipestream_search::pb::StartShardSearch {
                        vector: common::unit_vectors(1, 64, 721),
                        k: 4,
                        ..Default::default()
                    },
                )),
            },
        ]))
        .await
        .unwrap()
        .into_inner();
    let hits = loop {
        let message = inbound.message().await.unwrap().expect("Done");
        if let Some(pipestream_search::pb::search_shard_response::Payload::Done(done)) =
            message.payload
        {
            break done.hits;
        }
    };
    assert_eq!(hits.len(), 3);
    for hit in hits {
        assert_ne!(hit.vector_id, 101);
        if hit.vector_id == 102 {
            assert!(hit.identity.is_none());
        } else {
            assert_eq!(hit.identity.unwrap().version, 7);
        }
    }
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn identities_keep_the_scored_binding_after_row_reuse_without_holding_the_shard_lock() {
    let (node, mut client, server, _) = fixture().await;
    let (tx, mut inbound) = start(&mut client, limits()).await;
    ready(&mut inbound).await;
    // Replace exactly the same positional rows while the old scan waits.
    let changed = node.clone();
    tokio::time::timeout(
        Duration::from_secs(2),
        tokio::task::spawn_blocking(move || changed.with_bm25(Some(store(99)))),
    )
    .await
    .expect("identity retention must not hold the shard read lock")
    .unwrap();
    select(&tx, vec![103, 102, 100]).await;
    let response::Payload::Identities(found) =
        inbound.message().await.unwrap().unwrap().payload.unwrap()
    else {
        panic!("identities")
    };
    assert_eq!(
        found
            .rows
            .iter()
            .map(|row| row.vector_id)
            .collect::<Vec<_>>(),
        [103, 102, 100]
    );
    assert_eq!(found.rows[0].identity.as_ref().unwrap().version, 7);
    assert_eq!(
        found.rows[2].identity.as_ref().unwrap().document_key,
        [0, 255, 0]
    );
    assert!(found.rows[1].identity.is_none());
    let response::Payload::Summary(summary) =
        inbound.message().await.unwrap().unwrap().payload.unwrap()
    else {
        panic!("summary")
    };
    assert!(summary.completed);
    assert!(inbound.message().await.unwrap().is_none());
    let current = client
        .get_documents(pipestream_search::pb::GetDocumentsRequest { doc_ids: vec![100] })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(current.documents[0].identity.as_ref().unwrap().version, 99);
    drop(tx);
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn selection_rejects_deleted_out_of_range_duplicate_and_oversized_requests() {
    let (_, mut client, server, _) = fixture().await;
    for (ids, bytes, expected) in [
        (vec![101], 4096, tonic::Code::PermissionDenied),
        (vec![u64::MAX], 4096, tonic::Code::InvalidArgument),
        (vec![100, 100], 4096, tonic::Code::InvalidArgument),
        (vec![100], 2, tonic::Code::ResourceExhausted),
        (vec![100; 5], 4096, tonic::Code::ResourceExhausted),
    ] {
        let (tx, mut inbound) = start(
            &mut client,
            StreamIdentityLimits {
                max_response_bytes: bytes,
                ..limits()
            },
        )
        .await;
        ready(&mut inbound).await;
        select(&tx, ids).await;
        assert_eq!(inbound.message().await.unwrap_err().code(), expected);
    }
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn identity_wait_expires_and_stop_never_certifies_completion() {
    let (_, mut client, server, _) = fixture().await;
    let (_tx, mut inbound) = start(
        &mut client,
        StreamIdentityLimits {
            timeout_ms: 50,
            ..limits()
        },
    )
    .await;
    ready(&mut inbound).await;
    let error = tokio::time::timeout(Duration::from_secs(2), inbound.message())
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::DeadlineExceeded);
    let (tx, mut inbound) = start(&mut client, limits()).await;
    ready(&mut inbound).await;
    tx.send(StreamSearchRequest {
        payload: Some(request::Payload::Stop(StopStreamSearch {})),
    })
    .await
    .unwrap();
    let response::Payload::Summary(summary) =
        inbound.message().await.unwrap().unwrap().payload.unwrap()
    else {
        panic!("summary")
    };
    assert!(!summary.completed);
    let (tx, mut inbound) = start(&mut client, limits()).await;
    ready(&mut inbound).await;
    drop(tx);
    assert_eq!(
        inbound.message().await.unwrap_err().code(),
        tonic::Code::Cancelled
    );
    server.abort();
}

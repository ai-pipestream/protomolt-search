#![cfg(feature = "net")]

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use pipestream_search::analyzer::{body_spec, TURBO_ANALYSIS_BACKEND};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::link::NodeLink;
use pipestream_search::node::{NodeConfig, NodeServiceImpl};
use pipestream_search::pb::{AddDocumentsRequest, DeleteDocumentsRequest, DocLineage};
use tonic::Code;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_fallback_preserves_local_sentences_lineage_order_and_tombstones() {
    let node = Arc::new(NodeServiceImpl::new(
        None,
        NodeConfig {
            slot_offset: 100,
            analysis_addr: Some(TURBO_ANALYSIS_BACKEND.into()),
            sentence_fields: vec!["body".into()],
            ..Default::default()
        },
    ));
    let mut link = NodeLink::local(Arc::clone(&node));
    let texts = [
        "Café is open. The café closes tonight.",
        "An introduction. The appellate court affirmed jurisdiction.",
    ];
    link.add_documents(tokio_stream::iter(texts.into_iter().map(|text| {
        AddDocumentsRequest {
            text: text.into(),
            analysis: Some(body_spec()),
            lineage: Some(DocLineage {
                parent_id: 7,
                group_id: 1,
                span_start: 1000,
                span_end: 1000 + text.len() as u32,
            }),
            ..Default::default()
        }
    })))
    .await
    .unwrap();
    let local = CoordinatorServiceImpl::with_local_nodes(vec![Arc::clone(&node)]);
    let (address, server) = pipestream_search::harness::serve_node((*node).clone()).await;
    let remote = CoordinatorServiceImpl::new(vec![address]);
    let shortlist = [(101, 3.0), (100, 2.0), (101, 1.0), (999, 0.0)];
    let expected = local.fetch_rerank_candidates(&shortlist).await.unwrap();
    let actual = remote.fetch_rerank_candidates(&shortlist).await.unwrap();
    for (i, (left, right)) in expected.iter().zip(&actual).enumerate() {
        assert_eq!((left.doc_id, left.prior_score), shortlist[i]);
        assert_eq!((right.doc_id, right.prior_score), shortlist[i]);
        assert_eq!(left.text, right.text);
        assert_eq!(left.sentence_spans, right.sentence_spans);
        assert_eq!(left.lineage_span_start, right.lineage_span_start);
        if i < 3 {
            assert_eq!(right.lineage_span_start, 1000);
            assert_eq!(right.sentence_spans.len(), 2);
            for &(start, end) in &right.sentence_spans {
                assert!(!right
                    .text
                    .get(start as usize..end as usize)
                    .unwrap()
                    .is_empty());
            }
        } else {
            assert!(right.text.is_empty());
            assert!(right.sentence_spans.is_empty());
        }
    }
    assert_eq!(actual[0].sentence_spans, actual[2].sentence_spans);
    link.delete_documents(DeleteDocumentsRequest {
        doc_ids: vec![100],
        ..Default::default()
    })
    .await
    .unwrap();
    for coord in [&local, &remote] {
        let rows = coord.fetch_rerank_candidates(&[(100, 1.0)]).await.unwrap();
        assert!(rows[0].text.is_empty());
        assert!(rows[0].sentence_spans.is_empty());
        assert_eq!(rows[0].lineage_span_start, 0);
    }
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn stalled_remote_document_fetch_obeys_the_rerank_deadline() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let coordinator =
        CoordinatorServiceImpl::new(vec![format!("http://{}", listener.local_addr().unwrap())]);
    let deadline = (SystemTime::now().duration_since(UNIX_EPOCH).unwrap()
        + Duration::from_millis(500))
    .as_micros() as u64;
    let request = tokio::spawn(async move {
        coordinator
            .fetch_rerank_candidates_with_deadline(&[(0, 1.0)], deadline)
            .await
    });
    // Accept the TCP connection but never answer HTTP/2 or the document RPC.
    let (_connection, _) = tokio::time::timeout(Duration::from_secs(2), listener.accept())
        .await
        .unwrap()
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(3), request)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.unwrap_err().code(), Code::DeadlineExceeded);
}

#[tokio::test]
async fn expired_fallback_refuses_before_contacting_any_node() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let coordinator =
        CoordinatorServiceImpl::new(vec![format!("http://{}", listener.local_addr().unwrap())]);
    let error = coordinator
        .fetch_rerank_candidates_with_deadline(&[(0, 1.0)], 1)
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::DeadlineExceeded);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn routed_fallback_contacts_only_owning_shards_and_skips_unowned_siblings() {
    let owner_node = Arc::new(NodeServiceImpl::new(
        None,
        NodeConfig {
            slot_offset: 100,
            analysis_addr: Some(TURBO_ANALYSIS_BACKEND.into()),
            sentence_fields: vec!["body".into()],
            ..Default::default()
        },
    ));
    let mut link = NodeLink::local(Arc::clone(&owner_node));
    link.add_documents(tokio_stream::iter([
        AddDocumentsRequest {
            text: "First sentence. Second sentence.".into(),
            analysis: Some(body_spec()),
            lineage: Some(DocLineage {
                parent_id: 11,
                group_id: 1,
                span_start: 500,
                span_end: 532,
            }),
            ..Default::default()
        },
        AddDocumentsRequest {
            text: "Alpha lead. Beta conclusion.".into(),
            analysis: Some(body_spec()),
            lineage: Some(DocLineage {
                parent_id: 12,
                group_id: 1,
                span_start: 600,
                span_end: 628,
            }),
            ..Default::default()
        },
    ]))
    .await
    .unwrap();

    let (owner_addr, owner_server) =
        pipestream_search::harness::serve_node((*owner_node).clone()).await;
    // Sibling shard never answers HTTP/2; any broadcast RPC to it would stall until deadline.
    let stalled_sibling = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let sibling_addr = format!("http://{}", stalled_sibling.local_addr().unwrap());

    let coordinator = CoordinatorServiceImpl::new(vec![owner_addr, sibling_addr]);
    let shortlist = [(101, 2.5), (100, 1.5), (101, 0.5)];
    let shard_owners = std::collections::HashMap::from([(100_u64, 0_usize), (101_u64, 0_usize)]);
    let deadline = (SystemTime::now().duration_since(UNIX_EPOCH).unwrap()
        + Duration::from_millis(500))
    .as_micros() as u64;

    let rows = coordinator
        .fetch_rerank_candidates_routed(&shortlist, &shard_owners, deadline)
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].doc_id, 101);
    assert_eq!(rows[0].lineage_span_start, 600);
    assert_eq!(rows[0].sentence_spans.len(), 2);
    assert_eq!(rows[1].doc_id, 100);
    assert_eq!(rows[1].lineage_span_start, 500);
    assert_eq!(rows[1].sentence_spans.len(), 2);
    assert_eq!(rows[2].doc_id, 101);
    assert_eq!(rows[2].sentence_spans, rows[0].sentence_spans);

    assert!(
        tokio::time::timeout(Duration::from_millis(25), stalled_sibling.accept())
            .await
            .is_err(),
        "unowned sibling shard must not be dialed when shard owners are known"
    );

    owner_server.abort();
    let _ = owner_server.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unrouted_fallback_merges_candidates_across_multiple_remote_shards() {
    let shard0 = Arc::new(NodeServiceImpl::new(
        None,
        NodeConfig {
            slot_offset: 100,
            analysis_addr: Some(TURBO_ANALYSIS_BACKEND.into()),
            sentence_fields: vec!["body".into()],
            ..Default::default()
        },
    ));
    let shard1 = Arc::new(NodeServiceImpl::new(
        None,
        NodeConfig {
            slot_offset: 200,
            analysis_addr: Some(TURBO_ANALYSIS_BACKEND.into()),
            sentence_fields: vec!["body".into()],
            ..Default::default()
        },
    ));
    NodeLink::local(Arc::clone(&shard0))
        .add_documents(tokio_stream::iter([AddDocumentsRequest {
            text: "Shard zero opening. Shard zero closing.".into(),
            analysis: Some(body_spec()),
            lineage: Some(DocLineage {
                parent_id: 1,
                group_id: 0,
                span_start: 10,
                span_end: 49,
            }),
            ..Default::default()
        }]))
        .await
        .unwrap();
    NodeLink::local(Arc::clone(&shard1))
        .add_documents(tokio_stream::iter([AddDocumentsRequest {
            text: "Shard one opening. Shard one closing.".into(),
            analysis: Some(body_spec()),
            lineage: Some(DocLineage {
                parent_id: 2,
                group_id: 0,
                span_start: 20,
                span_end: 57,
            }),
            ..Default::default()
        }]))
        .await
        .unwrap();

    let (addr0, srv0) = pipestream_search::harness::serve_node((*shard0).clone()).await;
    let (addr1, srv1) = pipestream_search::harness::serve_node((*shard1).clone()).await;
    let coordinator = CoordinatorServiceImpl::new(vec![addr0, addr1]);

    let shortlist = [(200, 4.0), (100, 2.0), (404, 1.0)];
    let rows = coordinator
        .fetch_rerank_candidates(&shortlist)
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].doc_id, 200);
    assert_eq!(rows[0].lineage_span_start, 20);
    assert_eq!(rows[0].sentence_spans.len(), 2);
    assert_eq!(rows[1].doc_id, 100);
    assert_eq!(rows[1].lineage_span_start, 10);
    assert_eq!(rows[1].sentence_spans.len(), 2);
    assert!(rows[2].text.is_empty());
    assert!(rows[2].sentence_spans.is_empty());

    srv0.abort();
    srv1.abort();
    let _ = srv0.await;
    let _ = srv1.await;
}

#![cfg(feature = "net")]

mod common;

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::node::{NodeConfig, NodeServiceImpl};
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::node_service_server::NodeService;
use pipestream_search::pb::{ScoreSpladeRequest, SpladeTermWeight};
use pipestream_search::postings::{AnalyzedDoc, AnalyzedField, Bm25Store};
use tonic::{Code, Request};

struct Fixture {
    root: PathBuf,
    node: NodeServiceImpl,
    addr: String,
    server: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

impl Fixture {
    async fn new(max_k: u32) -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let root = std::env::temp_dir().join(format!(
            "splade-limits-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("index");
        let mut store = Bm25Store::new();
        for doc in 0..3 {
            store.add_document(
                doc,
                format!("document {doc}"),
                AnalyzedDoc {
                    fields: vec![AnalyzedField {
                        terms: vec![("a".into(), 1, Vec::new())],
                        length: 1,
                        splade: vec![("a".into(), 8)],
                        splade_scale_bits: 1.0_f32.to_bits(),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            );
        }
        let bm25 = root.join("index.bm25");
        store.save(&bm25).unwrap();
        common::catalog::seal_bm25_file(&path, &bm25);
        let node = NodeServiceImpl::open(
            NodeConfig {
                max_k,
                index_path: Some(path),
                ..Default::default()
            },
            None,
        )
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = format!("http://{}", listener.local_addr().unwrap());
        let service = node.clone();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(NodeServiceImpl::into_server(
                    service,
                    pipestream_search::MAX_MESSAGE_BYTES,
                ))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
        });
        Self {
            root,
            node,
            addr,
            server,
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn request(k: u32, terms: bool) -> ScoreSpladeRequest {
    ScoreSpladeRequest {
        k,
        query_weights: if terms {
            vec![SpladeTermWeight {
                term: "a".into(),
                weight: 1.0,
            }]
        } else {
            Vec::new()
        },
        ..Default::default()
    }
}

#[tokio::test]
async fn node_rejects_oversized_k_before_empty_and_missing_field_shortcuts() {
    let f = Fixture::new(2).await;
    let mut client = NodeServiceClient::connect(f.addr.clone()).await.unwrap();
    for k in [3, u32::MAX] {
        for terms in [false, true] {
            for field in ["", "missing"] {
                let mut req = request(k, terms);
                req.field = field.into();
                let error = client.score_splade(req).await.unwrap_err();
                assert_eq!(error.code(), Code::InvalidArgument);
                assert!(error.message().contains("max_k=2"));
            }
        }
    }
    let diagnostic = f
        .node
        .knobs()
        .list()
        .knobs
        .into_iter()
        .find(|knob| knob.name == "score_splade_max_k")
        .unwrap();
    assert_eq!(diagnostic.value, "2");
    assert!(!diagnostic.mutable);
}

#[tokio::test]
async fn node_and_coordinator_caps_preserve_boundary_ties_and_zero() {
    let f = Fixture::new(2).await;
    let mut client = NodeServiceClient::connect(f.addr.clone()).await.unwrap();
    let hits = client
        .score_splade(request(2, true))
        .await
        .unwrap()
        .into_inner()
        .hits;
    assert_eq!(
        hits.iter().map(|hit| hit.doc_id).collect::<Vec<_>>(),
        [0, 1]
    );
    assert_eq!(
        hits.iter().map(|hit| hit.score).collect::<Vec<_>>(),
        [8.0, 8.0]
    );
    assert!(client
        .score_splade(request(0, true))
        .await
        .unwrap()
        .into_inner()
        .hits
        .is_empty());
    let coordinator = CoordinatorServiceImpl::new(vec![f.addr.clone()]).with_max_k(2);
    let query = [("a".into(), 1.0)];
    assert_eq!(
        coordinator
            .score_splade("body", &query, 0.0, 2, "")
            .await
            .unwrap(),
        [(0, 8.0), (1, 8.0)]
    );
    assert!(coordinator
        .score_splade("body", &query, 0.0, 0, "")
        .await
        .unwrap()
        .is_empty());
    coordinator.knobs().set("max_k", "1").unwrap();
    assert_eq!(
        coordinator
            .score_splade("body", &query, 0.0, 2, "")
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    // The coordinator's mutable knob does not change the node's startup cap.
    assert_eq!(
        f.node
            .score_splade(Request::new(request(2, true)))
            .await
            .unwrap()
            .into_inner()
            .hits
            .len(),
        2
    );
}

#[tokio::test]
async fn coordinator_rejects_pathological_k_before_shortcuts_or_wire_conversion() {
    let coordinator = CoordinatorServiceImpl::new(vec!["http://127.0.0.1:1".into()]).with_max_k(2);
    let mut limits = vec![3, usize::MAX];
    if let Ok(over_wire) = usize::try_from(u64::from(u32::MAX) + 1) {
        limits.push(over_wire);
    }
    for k in limits {
        for query in [Vec::new(), vec![("a".into(), 1.0)]] {
            let error = coordinator
                .score_splade("missing", &query, 0.0, k, "")
                .await
                .unwrap_err();
            assert_eq!(error.code(), Code::InvalidArgument);
            assert!(error.message().contains("max_k=2"));
        }
    }
}

#[tokio::test]
async fn node_rust_helper_rejects_oversized_k_before_wire_conversion() {
    let f = Fixture::new(2).await;
    let mut limits = vec![3, usize::MAX];
    if let Ok(over_wire) = usize::try_from(u64::from(u32::MAX) + 1) {
        limits.push(over_wire);
    }
    for k in limits {
        for query in [Vec::new(), vec![("a".into(), 1.0)]] {
            let error = f
                .node
                .score_splade_field("missing", &query, 0.0, k, None)
                .unwrap_err();
            assert_eq!(error.code(), Code::InvalidArgument);
            assert!(error.message().contains("max_k=2"));
        }
    }
    assert_eq!(
        f.node
            .score_splade_field("body", &[("a".into(), 1.0)], 0.0, 2, None)
            .unwrap(),
        [(0, 8.0), (1, 8.0)]
    );
}

#[tokio::test]
async fn coordinator_splade_enforces_deadline_before_and_during_remote_fanout() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let coordinator =
        CoordinatorServiceImpl::new(vec![format!("http://{}", listener.local_addr().unwrap())]);
    let query = [("a".to_string(), 1.0_f32)];

    let expired = coordinator
        .score_splade_with_deadline("body", &query, 0.0, 2, "", 1)
        .await
        .unwrap_err();
    assert_eq!(expired.code(), Code::DeadlineExceeded);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), listener.accept())
            .await
            .is_err(),
        "an expired SPLADE deadline must fail before dialing any shard"
    );

    let deadline = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        + std::time::Duration::from_millis(400))
    .as_micros() as u64;
    let inflight = tokio::spawn(async move {
        coordinator
            .score_splade_with_deadline("body", &query, 0.0, 2, "", deadline)
            .await
    });
    let (_conn, _) = tokio::time::timeout(std::time::Duration::from_secs(2), listener.accept())
        .await
        .unwrap()
        .unwrap();
    let result = tokio::time::timeout(std::time::Duration::from_secs(3), inflight)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.unwrap_err().code(), Code::DeadlineExceeded);
}

//! Metrics exporter acceptance (`docs/metrics.md`): the page serves
//! over plain HTTP, counters move with real traffic, and shard gauges
//! sample live state at scrape time.
//!
//! Counters are process-wide statics shared by every test in this
//! binary, so every assertion here is a DELTA around this test's own
//! traffic, never an absolute value.

mod common;

use pipestream_search::metrics;
use pipestream_search::node::{NodeConfig, NodeServiceImpl};
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::AddDocumentsRequest;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use common::mock::start_mock_analysis;
use common::start_empty_node;

fn counter(page: &str, needle: &str) -> u64 {
    page.lines()
        .find(|l| l.starts_with(needle) && !l.starts_with('#'))
        .and_then(|l| l.rsplit_once(' '))
        .and_then(|(_, v)| v.parse().ok())
        .unwrap_or_else(|| panic!("no sample line starting {needle:?}"))
}

/// One scrape over real HTTP: status line, content type, counters,
/// and a gauge sampled from a live (empty) shard.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_page_serves_over_http() {
    let node = NodeServiceImpl::new(None, NodeConfig::default());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(metrics::serve(listener, vec![node.metrics_provider()]));

    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    socket
        .write_all(b"GET /metrics HTTP/1.1\r\nHost: test\r\n\r\n")
        .await
        .unwrap();
    let mut response = String::new();
    socket.read_to_string(&mut response).await.unwrap();

    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"), "{response}");
    assert!(response.contains("Content-Type: text/plain; version=0.0.4"));
    let body = response.split("\r\n\r\n").nth(1).unwrap();
    assert!(body.contains("# TYPE turbovec_requests_total counter"));
    assert!(
        body.contains("turbovec_shard_vectors{slot_offset=\"0\"} 0"),
        "the empty shard's gauge samples zero"
    );
}

/// Counters move with real traffic: an ingest advances the request
/// and document counters by at least this test's own contribution.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn counters_move_with_traffic() {
    let (analysis, _mock) = start_mock_analysis().await;
    let (addr, _node) = start_empty_node(NodeConfig {
        analysis_addr: Some(analysis),
        ..Default::default()
    })
    .await;

    let before = metrics::render(&[]);
    let mut client = NodeServiceClient::connect(addr).await.unwrap();
    let (tx, rx) = mpsc::channel(8);
    for text in ["metrics move", "with traffic"] {
        tx.send(AddDocumentsRequest {
            text: text.to_string(),
            ..Default::default()
        })
        .await
        .unwrap();
    }
    drop(tx);
    client.add_documents(ReceiverStream::new(rx)).await.unwrap();
    let after = metrics::render(&[]);

    let delta = |needle: &str| counter(&after, needle) - counter(&before, needle);
    assert!(delta("turbovec_requests_total{rpc=\"add_documents\"}") >= 1);
    assert!(delta("turbovec_documents_added_total") >= 2);
}

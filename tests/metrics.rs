//! Metrics exporter acceptance (`docs/metrics.md`): the page serves
//! over plain HTTP, counters move with real traffic, latency histograms
//! and error rows move with the requests that produced them (on the
//! node, the coordinator, and both phases of a response stream), and
//! shard gauges sample live state at scrape time.
//!
//! Counters are process-wide statics shared by every test in this
//! binary, so every assertion here is a DELTA around this test's own
//! traffic, never an absolute value.

mod common;

use std::net::SocketAddr;

use pipestream_search::metrics;
use pipestream_search::node::{NodeConfig, NodeServiceImpl};
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_client::SearchServiceClient;
use pipestream_search::pb::{
    query_stream_response, search_query, selection_query, AddDocumentsRequest, BrowseShardRequest,
    DenseQuery, QueryRequest, QueryStreamRequest, SearchQuery, SearchRequest, SelectionQuery,
};
use pipestream_search::vector::{VectorIndex, EMBEDDED_TURBOVEC};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

use common::mock::start_mock_analysis;
use common::{start_coordinator, start_empty_node, start_node, unit_vectors, BIT_WIDTH, DIM};

const DURATION: &str = "turbovec_request_duration_seconds";

fn sample_text<'a>(page: &'a str, needle: &str) -> &'a str {
    page.lines()
        .find(|l| l.starts_with(needle) && !l.starts_with('#'))
        .and_then(|l| l.rsplit_once(' '))
        .map(|(_, v)| v)
        .unwrap_or_else(|| panic!("no sample line starting {needle:?}"))
}

fn counter(page: &str, needle: &str) -> u64 {
    sample_text(page, needle)
        .parse()
        .unwrap_or_else(|_| panic!("integer sample for {needle:?}"))
}

fn seconds(page: &str, needle: &str) -> f64 {
    let text = sample_text(page, needle);
    assert!(text.contains('.'), "{needle}: float sample, got {text:?}");
    text.parse().expect("float sample")
}

/// Serve the exporter on a fresh loopback port with the given gauges.
async fn serve_metrics(gauges: Vec<metrics::GaugeProvider>) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(metrics::serve(listener, gauges));
    addr
}

/// One scrape over real HTTP: the raw response, status line included.
async fn scrape_raw(addr: SocketAddr) -> String {
    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    socket
        .write_all(b"GET /metrics HTTP/1.1\r\nHost: test\r\n\r\n")
        .await
        .unwrap();
    let mut response = String::new();
    socket.read_to_string(&mut response).await.unwrap();
    response
}

/// The page body of one HTTP scrape.
async fn scrape(addr: SocketAddr) -> String {
    let response = scrape_raw(addr).await;
    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"), "{response}");
    response.split("\r\n\r\n").nth(1).unwrap().to_string()
}

fn error_rows(page: &str, rpc: &str) -> Vec<(String, u64)> {
    page.lines()
        .filter(|l| l.starts_with(&format!("turbovec_request_errors_total{{rpc=\"{rpc}\",")))
        .map(|l| {
            let (name, value) = l.rsplit_once(' ').unwrap();
            (name.to_string(), value.parse().unwrap())
        })
        .collect()
}

/// One scrape over real HTTP: status line, content type, counters,
/// the histogram and gauge families, and a gauge sampled from a live
/// (empty) shard.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_page_serves_over_http() {
    let node = NodeServiceImpl::new(None, NodeConfig::default());
    let addr = serve_metrics(vec![node.metrics_provider()]).await;

    let response = scrape_raw(addr).await;
    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"), "{response}");
    assert!(response.contains("Content-Type: text/plain; version=0.0.4"));
    let body = response.split("\r\n\r\n").nth(1).unwrap();
    assert!(body.contains("# TYPE turbovec_requests_total counter"));
    assert!(body.contains("# TYPE turbovec_requests_in_flight gauge"));
    assert!(body.contains(&format!("# TYPE {DURATION} histogram")));
    assert!(body.contains("# TYPE turbovec_request_errors_total counter"));
    // Pre-declared from the first scrape: a route nothing here has
    // called still has its histogram and every error row.
    assert!(body.contains(&format!(
        "{DURATION}_bucket{{rpc=\"rollback_cluster\",le=\"+Inf\"}} "
    )));
    assert!(body.contains(
        "turbovec_request_errors_total{rpc=\"rollback_cluster\",code=\"unauthenticated\"} "
    ));
    assert!(
        body.contains("turbovec_shard_vectors{slot_offset=\"0\"} 0"),
        "the empty shard's gauge samples zero"
    );
}

/// Counters and the route's histogram move with real traffic, seen
/// over the HTTP scrape: an ingest advances the request counter, the
/// document counter, the histogram's `_count`, its `+Inf` bucket, and
/// its `_sum` by at least this test's own contribution.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn served_requests_move_the_histogram() {
    let (analysis, _mock) = start_mock_analysis().await;
    let (addr, _node) = start_empty_node(NodeConfig {
        analysis_addr: Some(analysis),
        ..Default::default()
    })
    .await;
    let exporter = serve_metrics(Vec::new()).await;

    let before = scrape(exporter).await;
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
    let after = scrape(exporter).await;

    let delta = |needle: &str| counter(&after, needle) - counter(&before, needle);
    assert!(delta("turbovec_requests_total{rpc=\"add_documents\"} ") >= 1);
    assert!(delta("turbovec_documents_added_total ") >= 2);
    assert!(delta(&format!("{DURATION}_count{{rpc=\"add_documents\"}} ")) >= 1);
    assert!(
        delta(&format!(
            "{DURATION}_bucket{{rpc=\"add_documents\",le=\"+Inf\"}} "
        )) >= 1
    );
    let sum = format!("{DURATION}_sum{{rpc=\"add_documents\"}} ");
    assert!(seconds(&after, &sum) > seconds(&before, &sum));
    assert_eq!(
        counter(
            &after,
            "turbovec_requests_in_flight{rpc=\"add_documents\"} "
        ),
        0
    );
    // A unary route carries no phase label.
    assert!(!after.contains(&format!("{DURATION}_count{{rpc=\"add_documents\",phase=")));
}

/// A refusal is counted by the code that left the handler: an
/// `INVALID_ARGUMENT` browse moves exactly that error row, still
/// counts as an arrival and a histogram observation, and leaves the
/// other codes alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refusals_count_by_grpc_code() {
    let (addr, _node) = start_empty_node(NodeConfig::default()).await;
    let exporter = serve_metrics(Vec::new()).await;

    let before = scrape(exporter).await;
    let mut client = NodeServiceClient::connect(addr).await.unwrap();
    let err = client
        .browse_shard(BrowseShardRequest {
            k: 0,
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(err.message(), "browse requires k > 0");
    let after = scrape(exporter).await;

    let delta = |needle: &str| counter(&after, needle) - counter(&before, needle);
    assert_eq!(
        delta("turbovec_request_errors_total{rpc=\"browse_shard\",code=\"invalid_argument\"} "),
        1
    );
    assert_eq!(delta("turbovec_requests_total{rpc=\"browse_shard\"} "), 1);
    assert_eq!(
        delta(&format!("{DURATION}_count{{rpc=\"browse_shard\"}} ")),
        1
    );
    let before_rows = error_rows(&before, "browse_shard");
    let after_rows = error_rows(&after, "browse_shard");
    assert_eq!(before_rows.len(), 10);
    for ((name, was), (_, now)) in before_rows.iter().zip(&after_rows) {
        let expected = if name.contains("code=\"invalid_argument\"") {
            was + 1
        } else {
            *was
        };
        assert_eq!(*now, expected, "{name}");
    }
    assert_eq!(
        counter(&after, "turbovec_requests_in_flight{rpc=\"browse_shard\"} "),
        0
    );
}

fn dense_leaf(vector: &[f32]) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Search(SearchQuery {
            id: "vec".into(),
            query: Some(search_query::Query::Dense(DenseQuery {
                vector: vector.to_vec(),
                ..Default::default()
            })),
        })),
    }
}

/// The coordinator's public routes are counted, and a response stream
/// reports both phases: over a served coordinator in front of a served
/// shard, `Search` and `Query` move their unary rows, `QueryStream`
/// moves `first_response` and `complete`, and the shard's `SearchShard`
/// stream (the fan-out's transport) moves both phases too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn coordinator_routes_and_streaming_phases_move() {
    const N: usize = 256;
    const K: u32 = 5;
    let corpus = unit_vectors(N, DIM, 0x0E7A_1C5E);
    let mut index = VectorIndex::create(EMBEDDED_TURBOVEC, DIM, BIT_WIDTH).unwrap();
    index.add(&corpus, DIM).unwrap();
    index.prepare().unwrap();
    let (node_addr, _node) = start_node(index, NodeConfig::default()).await;
    let (coordinator_addr, _coordinator) = start_coordinator(vec![node_addr]).await;
    let exporter = serve_metrics(Vec::new()).await;
    let query = corpus[..DIM].to_vec();

    let before = scrape(exporter).await;
    let mut client = SearchServiceClient::connect(coordinator_addr)
        .await
        .unwrap();
    let search = client
        .search(SearchRequest {
            request_id: "metrics-search".into(),
            k: K,
            vector: query.clone(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(search.hits.len(), K as usize);
    assert_eq!(search.hits[0].vector_id, 0, "the query is document 0");

    let public = client
        .query(QueryRequest {
            request_id: "metrics-query".into(),
            k: K,
            selection: Some(dense_leaf(&query)),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(public.hits.len(), K as usize);

    let mut stream = client
        .query_stream(QueryStreamRequest {
            collection: String::new(),
            query: Some(QueryRequest {
                request_id: "metrics-query-stream".into(),
                k: K,
                selection: Some(dense_leaf(&query)),
                ..Default::default()
            }),
            timeout_ms: 0,
        })
        .await
        .unwrap()
        .into_inner();
    let mut events = 0;
    let mut completed = false;
    while let Some(event) = stream.next().await {
        events += 1;
        if let Some(query_stream_response::Payload::Completion(done)) = event.unwrap().payload {
            assert!(done.completed, "{done:?}");
            completed = true;
        }
    }
    assert!(completed && events >= 2, "revision then completion");
    let after = scrape(exporter).await;

    let delta = |needle: &str| counter(&after, needle) - counter(&before, needle);
    assert_eq!(delta("turbovec_requests_total{rpc=\"search\"} "), 1);
    assert_eq!(delta(&format!("{DURATION}_count{{rpc=\"search\"}} ")), 1);
    assert_eq!(delta("turbovec_requests_total{rpc=\"query\"} "), 1);
    assert_eq!(delta(&format!("{DURATION}_count{{rpc=\"query\"}} ")), 1);
    assert_eq!(delta("turbovec_requests_total{rpc=\"query_stream\"} "), 1);
    for phase in ["first_response", "complete"] {
        assert_eq!(
            delta(&format!(
                "{DURATION}_count{{rpc=\"query_stream\",phase=\"{phase}\"}} "
            )),
            1,
            "{phase}"
        );
        assert_eq!(
            delta(&format!(
                "{DURATION}_bucket{{rpc=\"query_stream\",phase=\"{phase}\",le=\"+Inf\"}} "
            )),
            1,
            "{phase}"
        );
        // The shard's streams are the transport underneath: `Search`
        // and the dense `Query` leaf fan out over SearchShard, the
        // streaming query over StreamSearch, each counted once per
        // phase on the shard as well.
        let shard = delta(&format!(
            "{DURATION}_count{{rpc=\"search_shard\",phase=\"{phase}\"}} "
        ));
        let streamed = delta(&format!(
            "{DURATION}_count{{rpc=\"stream_search\",phase=\"{phase}\"}} "
        ));
        assert_eq!((shard, streamed), (2, 1), "{phase}");
    }
    // Neither phase's clock can run ahead of the other on one stream.
    let sum = |page: &str, phase: &str| {
        seconds(
            page,
            &format!("{DURATION}_sum{{rpc=\"query_stream\",phase=\"{phase}\"}} "),
        )
    };
    let first = sum(&after, "first_response") - sum(&before, "first_response");
    let complete = sum(&after, "complete") - sum(&before, "complete");
    assert!(first > 0.0 && complete >= first, "{first} <= {complete}");
    // Nothing here refused, and no coordinator route is nested-counted
    // twice: the error rows are untouched and no route is left in flight.
    for rpc in ["search", "query", "query_stream"] {
        assert_eq!(error_rows(&before, rpc), error_rows(&after, rpc), "{rpc}");
        assert_eq!(
            counter(
                &after,
                &format!("turbovec_requests_in_flight{{rpc=\"{rpc}\"}} ")
            ),
            0
        );
    }
}

//! The diagnostics service (`docs/diagnostics.md`): runtime knobs that
//! flip live, metrics snapshots equal to the rendered page, a snapshot
//! stream, per-shard layout diagnostics, the recent-request ring, and
//! the admin rule on the coordinator.

mod common;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{fit_calibration, start_empty_node, unit_vectors};
use pipestream_search::analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND};
use pipestream_search::collections::CollectionSet;
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::metrics;
use pipestream_search::node::{Layout, NodeConfig, NodeServiceImpl};
use pipestream_search::pb::diagnostics_service_client::DiagnosticsServiceClient;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_client::SearchServiceClient;
use pipestream_search::pb::{
    selection_query, AddDocumentsRequest, AddVectorsRequest, CompositeSearchStrategy, DenseQuery,
    FacetValue, FilterQuery, FlushRequest, GetRuntimeKnobsRequest, IntegerValue, KnobScope,
    MetricsSnapshot, MetricsSnapshotRequest, QueryRequest, QueryResponse, RecentQueriesRequest,
    SearchQuery, SelectionOperator, SelectionQuery, SetCalibrationRequest, SetRuntimeKnobRequest,
    ShardDiagnosticsRequest, StreamMetricsRequest,
};
use pipestream_search::security::{PrincipalConfig, Principals};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::transport::{Channel, Server};
use tonic::{Code, Request};

const DIM: usize = 16;
const BIT_WIDTH: usize = 4;
const PER_YEAR: usize = 4;
const YEARS: [i64; 3] = [2000, 2001, 2002];
const ADMIN: &str = "ops-admin-token-0123456789";
const PLAIN: &str = "console-token-0123456789";

fn tempdir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("diag-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn config(index_path: PathBuf, layout: Layout) -> NodeConfig {
    NodeConfig {
        index_path: Some(index_path),
        analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        facet_fields: vec!["court".to_string()],
        integer_fields: vec!["year".to_string()],
        layout,
        seal_tail_docs: PER_YEAR as u32,
        wal: false,
        ..Default::default()
    }
}

fn rows() -> Vec<(i64, String, &'static str)> {
    let mut out = Vec::new();
    for (yi, year) in YEARS.iter().enumerate() {
        for j in 0..PER_YEAR {
            let i = yi * PER_YEAR + j;
            let court = if i.is_multiple_of(2) { "ca9" } else { "scotus" };
            out.push((*year, format!("opinion {i} about search"), court));
        }
    }
    out
}

fn corpus() -> Vec<f32> {
    unit_vectors(rows().len(), DIM, 0xD1A6_0001)
}

async fn ingest(addr: &str) {
    let all = rows();
    let vectors = corpus();
    let sample = &vectors[..vectors.len().min(8 * DIM)];
    let (shift, scale) = fit_calibration(DIM, BIT_WIDTH, sample);
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    client
        .set_calibration(SetCalibrationRequest {
            dim: DIM as u32,
            bit_width: BIT_WIDTH as u32,
            shift,
            scale,
        })
        .await
        .unwrap();
    for (block, chunk) in all.chunks(PER_YEAR).enumerate() {
        let (tx, rx) = mpsc::channel(8);
        for (year, text, court) in chunk {
            tx.send(AddDocumentsRequest {
                text: text.clone(),
                analysis: Some(body_spec()),
                facets: vec![FacetValue {
                    field: "court".into(),
                    value: (*court).to_string(),
                }],
                integers: vec![IntegerValue {
                    field: "year".into(),
                    value: *year,
                }],
                ..Default::default()
            })
            .await
            .unwrap();
        }
        drop(tx);
        client.add_documents(ReceiverStream::new(rx)).await.unwrap();
        let start = block * PER_YEAR * DIM;
        let end = (start + chunk.len() * DIM).min(vectors.len());
        let (tx, rx) = mpsc::channel(2);
        tx.send(AddVectorsRequest {
            vectors: vectors[start..end].to_vec(),
            dim: DIM as u32,
        })
        .await
        .unwrap();
        drop(tx);
        client.add_vectors(ReceiverStream::new(rx)).await.unwrap();
    }
    client.flush(FlushRequest {}).await.unwrap();
}

fn coordinator(addr: &str) -> CoordinatorServiceImpl {
    CoordinatorServiceImpl::new(vec![addr.to_string()])
        .with_bm25(
            Some(NATIVE_ANALYSIS_BACKEND.to_string()),
            Default::default(),
        )
        .with_max_k(50)
}

/// A coordinator listener with the search set and its diagnostics.
async fn serve_set(set: CollectionSet) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    let max = pipestream_search::MAX_MESSAGE_BYTES;
    let diagnostics = set.diagnostics().into_server(max);
    tokio::spawn(
        Server::builder()
            .add_service(set.into_server(max))
            .add_service(diagnostics)
            .serve_with_incoming(pipestream_search::harness::nodelay_incoming(listener)),
    );
    format!("http://{addr}")
}

async fn channel(addr: &str) -> Channel {
    Channel::from_shared(addr.to_string())
        .unwrap()
        .connect()
        .await
        .unwrap()
}

fn bearer<T>(inner: T, token: Option<&str>) -> Request<T> {
    let mut request = Request::new(inner);
    if let Some(token) = token {
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
    }
    request
}

fn cel(cel: &str) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Filter(FilterQuery {
            id: "f".to_string(),
            predicate: Some(pipestream_search::pb::filter_query::Predicate::Cel(
                cel.to_string(),
            )),
        })),
    }
}

fn dense() -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Search(SearchQuery {
            id: "v".to_string(),
            query: Some(pipestream_search::pb::search_query::Query::Dense(
                DenseQuery {
                    vector: corpus()[..DIM].to_vec(),
                    ..Default::default()
                },
            )),
        })),
    }
}

fn filtered_dense(filter: &str) -> QueryRequest {
    QueryRequest {
        request_id: "diag".into(),
        k: 20,
        selection: Some(SelectionQuery {
            node: Some(selection_query::Node::Composite(CompositeSearchStrategy {
                operator: SelectionOperator::And as i32,
                clauses: vec![cel(filter), dense()],
                scoring: None,
            })),
        }),
        profile: true,
        ..Default::default()
    }
}

async fn query(coord: &str, request: QueryRequest) -> QueryResponse {
    SearchServiceClient::new(channel(coord).await)
        .query(request)
        .await
        .unwrap()
        .into_inner()
}

fn ids(response: &QueryResponse) -> Vec<(u64, f32)> {
    response.hits.iter().map(|h| (h.doc_id, h.score)).collect()
}

fn skipped(response: &QueryResponse) -> (u32, u32) {
    let p = response.profile.as_ref().expect("profile requested");
    (p.segments_total, p.segments_skipped)
}

/// A sealed, summarized shard plus a coordinator over it.
async fn fleet(tag: &str) -> (String, String) {
    let dir = tempdir(tag);
    let (node, _handle) = start_empty_node(config(dir.join("d.tv"), Layout::Segments)).await;
    ingest(&node).await;
    let coord = serve_set(CollectionSet::single(coordinator(&node))).await;
    (node, coord)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn knobs_list_and_flip_live() {
    let (node, coord) = fleet("knobs").await;
    let mut diag = DiagnosticsServiceClient::new(channel(&node).await);

    let listed = diag
        .get_runtime_knobs(GetRuntimeKnobsRequest {})
        .await
        .unwrap()
        .into_inner();
    assert!(listed.process.starts_with("node:"), "{}", listed.process);
    let find = |name: &str| {
        listed
            .knobs
            .iter()
            .find(|k| k.name == name)
            .unwrap_or_else(|| panic!("knob {name} listed"))
            .clone()
    };
    for live in [
        "floor_sharing",
        "segment_pruning",
        "floor_delta",
        "floor_warmup_chunks",
        "floor_min_interval_ms",
    ] {
        let knob = find(live);
        assert!(knob.mutable, "{live} is live");
        assert_eq!(knob.scope, KnobScope::Node as i32);
        assert_eq!(knob.value, knob.startup_value);
        assert!(!knob.description.is_empty());
    }
    let fixed = find("chunk_blocks");
    assert!(!fixed.mutable);
    assert_eq!(find("segment_pruning").value, "true");

    // With pruning on, a selective filter skips segments.
    let before = query(&coord, filtered_dense("year >= 2002")).await;
    let (total, skipped_on) = skipped(&before);
    assert!(total >= 3, "sealed segments: {total}");
    assert!(skipped_on > 0, "pruning skips: {skipped_on}");

    let flipped = diag
        .set_runtime_knob(SetRuntimeKnobRequest {
            name: "segment_pruning".into(),
            value: "false".into(),
        })
        .await
        .unwrap()
        .into_inner();
    let knob = flipped
        .knobs
        .iter()
        .find(|k| k.name == "segment_pruning")
        .unwrap();
    assert_eq!(knob.value, "false");
    assert_eq!(knob.startup_value, "true");

    let after = query(&coord, filtered_dense("year >= 2002")).await;
    assert_eq!(skipped(&after), (total, 0), "no skipping with the knob off");
    assert_eq!(ids(&after), ids(&before), "the answer is the same");

    diag.set_runtime_knob(SetRuntimeKnobRequest {
        name: "segment_pruning".into(),
        value: "on".into(),
    })
    .await
    .unwrap();
    let again = query(&coord, filtered_dense("year >= 2002")).await;
    assert_eq!(skipped(&again), (total, skipped_on));

    // The other live knobs take effect on their next read.
    for (name, value) in [
        ("floor_sharing", "false"),
        ("floor_delta", "0.5"),
        ("floor_warmup_chunks", "3"),
        ("floor_min_interval_ms", "20"),
    ] {
        let out = diag
            .set_runtime_knob(SetRuntimeKnobRequest {
                name: name.into(),
                value: value.into(),
            })
            .await
            .unwrap()
            .into_inner();
        let knob = out.knobs.iter().find(|k| k.name == name).unwrap();
        assert_eq!(knob.value, value, "{name}");
    }
    let layout = diag
        .get_shard_diagnostics(ShardDiagnosticsRequest { shard: None })
        .await
        .unwrap()
        .into_inner();
    assert!(!layout.shards[0].floor_sharing);
    assert!(layout.shards[0].segment_pruning);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn immutable_unknown_and_malformed_knobs_are_rejected() {
    let dir = tempdir("reject");
    let (node, _handle) = start_empty_node(config(dir.join("r.tv"), Layout::Segments)).await;
    let mut diag = DiagnosticsServiceClient::new(channel(&node).await);
    let set = |name: &str, value: &str| SetRuntimeKnobRequest {
        name: name.into(),
        value: value.into(),
    };
    let fixed = diag
        .set_runtime_knob(set("chunk_blocks", "4"))
        .await
        .unwrap_err();
    assert_eq!(fixed.code(), Code::FailedPrecondition);
    assert!(
        fixed.message().contains("chunk_blocks"),
        "{}",
        fixed.message()
    );
    let unknown = diag.set_runtime_knob(set("max_k", "5")).await.unwrap_err();
    assert_eq!(unknown.code(), Code::InvalidArgument);
    assert!(unknown.message().contains("max_k"), "{}", unknown.message());
    let bad = diag
        .set_runtime_knob(set("floor_delta", "wide"))
        .await
        .unwrap_err();
    assert_eq!(bad.code(), Code::InvalidArgument);
    assert!(bad.message().contains("floor_delta"), "{}", bad.message());
    let still = diag
        .get_runtime_knobs(GetRuntimeKnobsRequest {})
        .await
        .unwrap()
        .into_inner();
    let delta = still
        .knobs
        .iter()
        .find(|k| k.name == "floor_delta")
        .unwrap();
    assert_eq!(delta.value, "0");
}

/// The rendered Prometheus page, as (name, labels) -> value text.
fn page_samples(page: &str) -> std::collections::BTreeMap<(String, String), String> {
    page.lines()
        .filter(|line| !line.starts_with('#') && !line.trim().is_empty())
        .map(|line| {
            let (head, value) = line.rsplit_once(' ').unwrap();
            let (name, labels) = match head.split_once('{') {
                Some((name, rest)) => (name.to_string(), rest.trim_end_matches('}').to_string()),
                None => (head.to_string(), String::new()),
            };
            ((name, labels), value.to_string())
        })
        .collect()
}

fn labels_text(labels: &[pipestream_search::pb::MetricLabel]) -> String {
    labels
        .iter()
        .map(|l| format!("{}=\"{}\"", l.name, l.value))
        .collect::<Vec<_>>()
        .join(",")
}

fn le_text(le: f64) -> String {
    if le.is_infinite() {
        return "+Inf".to_string();
    }
    let mut text = format!("{le}");
    if !text.contains('.') {
        text.push_str(".0");
    }
    // The page prints "0.001", "1", "10": normalize both sides through f64.
    text
}

fn check_snapshot_against_page(snapshot: &MetricsSnapshot, page: &str) {
    let samples = page_samples(page);
    assert!(!snapshot.samples.is_empty());
    for sample in &snapshot.samples {
        let key = (sample.name.clone(), labels_text(&sample.labels));
        let printed = samples
            .get(&key)
            .unwrap_or_else(|| panic!("page has {key:?}"));
        assert_eq!(printed.parse::<f64>().unwrap(), sample.value, "{key:?}");
    }
    for h in &snapshot.histograms {
        let labels = labels_text(&h.labels);
        for bucket in &h.buckets {
            let want = bucket.cumulative_count as f64;
            let found = samples
                .iter()
                .find(|((name, l), _)| {
                    name == &format!("{}_bucket", h.name)
                        && l.starts_with(&labels)
                        && l.ends_with(&format!(",le=\"{}\"", le_text(bucket.le)))
                        || (name == &format!("{}_bucket", h.name)
                            && l.starts_with(&labels)
                            && l.rsplit_once("le=\"").is_some_and(|(_, le)| {
                                le.trim_end_matches('"').parse::<f64>().ok() == Some(bucket.le)
                                    || (le.trim_end_matches('"') == "+Inf"
                                        && bucket.le.is_infinite())
                            }))
                })
                .map(|(_, v)| v.parse::<f64>().unwrap())
                .unwrap_or_else(|| panic!("page has bucket {labels} le={}", bucket.le));
            assert_eq!(found, want, "{labels} le={}", bucket.le);
        }
        let count = samples
            .get(&(format!("{}_count", h.name), labels.clone()))
            .unwrap()
            .parse::<f64>()
            .unwrap();
        assert_eq!(count, h.count as f64, "{labels} count");
        let sum = samples
            .get(&(format!("{}_sum", h.name), labels.clone()))
            .unwrap()
            .parse::<f64>()
            .unwrap();
        assert!(
            (sum - h.sum).abs() < 1e-6,
            "{labels} sum {sum} vs {}",
            h.sum
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_snapshot_equals_the_rendered_page() {
    let dir = tempdir("snapshot");
    let node = NodeServiceImpl::new(None, config(dir.join("s.tv"), Layout::Segments));
    let gauges = vec![node.metrics_provider()];
    // Move the registry off zero so the comparison is not trivial.
    metrics::inc_request(metrics::Route::Query);
    // One reading, two views: other tests in this binary drive the same
    // process-wide registry from their own threads, so two separate reads
    // would be two moments, and the claim here is about one.
    let reading = metrics::read(&gauges);
    let page = metrics::render_reading(&reading);
    let snapshot = metrics::snapshot_reading("test", &reading);
    assert_eq!(snapshot.process, "test");
    assert!(snapshot.unix_ms > 0);
    check_snapshot_against_page(&snapshot, &page);
    assert!(snapshot
        .samples
        .iter()
        .any(|s| s.name == "turbovec_shard_vectors"));
    assert_eq!(
        snapshot
            .histograms
            .iter()
            .filter(|h| h.labels.iter().any(|l| l.name == "phase"))
            .count()
            % 2,
        0,
        "streaming routes carry both phases"
    );

    // Over the wire, the node's snapshot is the same shape.
    let (addr, _handle) = start_empty_node(config(dir.join("w.tv"), Layout::Segments)).await;
    let mut diag = DiagnosticsServiceClient::new(channel(&addr).await);
    let wire = diag
        .get_metrics_snapshot(MetricsSnapshotRequest {})
        .await
        .unwrap()
        .into_inner();
    assert!(wire.process.starts_with("node:"));
    assert_eq!(wire.samples.len(), snapshot.samples.len());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_stream_delivers_at_its_interval() {
    let dir = tempdir("stream");
    let (addr, _handle) = start_empty_node(config(dir.join("t.tv"), Layout::Segments)).await;
    let mut diag = DiagnosticsServiceClient::new(channel(&addr).await);
    let too_fast = diag
        .stream_metrics(StreamMetricsRequest { interval_ms: 50 })
        .await
        .unwrap_err();
    assert_eq!(too_fast.code(), Code::InvalidArgument);

    let started = Instant::now();
    let mut stream = diag
        .stream_metrics(StreamMetricsRequest { interval_ms: 100 })
        .await
        .unwrap()
        .into_inner();
    let mut seen = Vec::new();
    while seen.len() < 3 {
        let message = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("a snapshot within the deadline")
            .expect("the stream is open")
            .unwrap();
        seen.push(message.unix_ms);
    }
    assert!(
        started.elapsed() >= Duration::from_millis(200),
        "three snapshots at 100 ms take at least 200 ms: {:?}",
        started.elapsed()
    );
    assert!(seen.windows(2).all(|w| w[1] >= w[0]));
    drop(stream);
    // The producer stops once the receiver is gone; a fresh stream still
    // works, which is what a hung producer would break.
    let mut again = diag
        .stream_metrics(StreamMetricsRequest { interval_ms: 0 })
        .await
        .unwrap()
        .into_inner();
    assert!(tokio::time::timeout(Duration::from_secs(5), again.next())
        .await
        .unwrap()
        .is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shard_diagnostics_describe_both_layouts() {
    let (node, coord) = fleet("layout").await;
    let mut diag = DiagnosticsServiceClient::new(channel(&node).await);
    let out = diag
        .get_shard_diagnostics(ShardDiagnosticsRequest { shard: None })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(out.shards.len(), 1);
    let shard = &out.shards[0];
    assert_eq!(shard.layout, "segments");
    assert_eq!(shard.rows, rows().len() as u64);
    assert_eq!(shard.live_rows, rows().len() as u64);
    assert_eq!(shard.tombstones, 0);
    assert!(shard.catalog_epoch > 0);
    assert!(shard.segments.len() >= 3, "{}", shard.segments.len());
    assert!(shard.segments.iter().all(|s| s.has_summary));
    let year_ranges: Vec<(i64, i64)> = shard
        .segments
        .iter()
        .map(|s| {
            let year = s
                .columns
                .iter()
                .find(|c| c.column == "year")
                .expect("year summarized");
            assert!(!year.floating);
            assert_eq!(year.present, s.rows);
            (year.lo, year.hi)
        })
        .collect();
    assert!(year_ranges.iter().all(|(lo, hi)| lo <= hi));
    assert_eq!(year_ranges[0], (2000, 2000));
    assert!(shard.segments.iter().all(|s| s.partition.is_none()));
    assert_eq!(shard.tail_rows, 0, "flushed");
    assert!(shard.segment_pruning && shard.floor_sharing);

    // The coordinator fans out to its node and stamps shard and address.
    let mut cdiag = DiagnosticsServiceClient::new(channel(&coord).await);
    let fan = cdiag
        .get_shard_diagnostics(ShardDiagnosticsRequest { shard: None })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(fan.shards.len(), 1);
    assert_eq!(fan.shards[0].shard, 0);
    assert_eq!(fan.shards[0].address, node);
    assert_eq!(fan.shards[0].segments.len(), shard.segments.len());
    let none = cdiag
        .get_shard_diagnostics(ShardDiagnosticsRequest { shard: Some(7) })
        .await
        .unwrap()
        .into_inner();
    assert!(none.shards.is_empty());

    // A single-image shard reports its layout and no segments.
    let dir = tempdir("single");
    let (single, _handle) = start_empty_node(config(dir.join("one.tv"), Layout::SingleImage)).await;
    ingest(&single).await;
    let mut sdiag = DiagnosticsServiceClient::new(channel(&single).await);
    let out = sdiag
        .get_shard_diagnostics(ShardDiagnosticsRequest { shard: None })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(out.shards[0].layout, "single-image");
    assert!(out.shards[0].segments.is_empty());
    assert_eq!(out.shards[0].rows, rows().len() as u64);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_ring_holds_recent_requests_newest_first() {
    let (node, coord) = fleet("ring").await;
    let mut cdiag = DiagnosticsServiceClient::new(channel(&coord).await);
    let empty = cdiag
        .recent_queries(RecentQueriesRequest { limit: 0 })
        .await
        .unwrap()
        .into_inner();
    assert!(empty.queries.is_empty());
    assert_eq!(empty.total_seen, 0);

    query(&coord, filtered_dense("year >= 2002")).await;
    query(&coord, filtered_dense("year >= 2000")).await;
    let mut client = SearchServiceClient::new(channel(&coord).await);
    let too_deep = client
        .query(QueryRequest {
            k: 5000,
            ..filtered_dense("year >= 2000")
        })
        .await
        .unwrap_err();
    assert_eq!(too_deep.code(), Code::InvalidArgument);

    let recent = cdiag
        .recent_queries(RecentQueriesRequest { limit: 2 })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(recent.total_seen, 3);
    assert_eq!(recent.queries.len(), 2);
    let newest = &recent.queries[0];
    assert_eq!(newest.route, "query");
    assert_eq!(newest.status, "InvalidArgument");
    assert_eq!(newest.k, 5000);
    assert_eq!(newest.hits, 0);
    let ok = &recent.queries[1];
    assert_eq!(ok.status, "OK");
    assert_eq!(ok.k, 20);
    assert_eq!(ok.hits, rows().len() as u32);
    assert!(ok.total_ms > 0.0);
    assert!(ok.segments_total >= 3);
    assert!(!ok.executed.is_empty(), "the Query route reports what ran");
    assert!(ok.unix_ms >= newest.unix_ms - 60_000);
    let all = cdiag
        .recent_queries(RecentQueriesRequest { limit: 0 })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(all.queries.len(), 3);
    assert!(all.queries.windows(2).all(|w| w[0].unix_ms >= w[1].unix_ms));
    // A node has no ring.
    let mut ndiag = DiagnosticsServiceClient::new(channel(&node).await);
    let none = ndiag
        .recent_queries(RecentQueriesRequest { limit: 0 })
        .await
        .unwrap()
        .into_inner();
    assert!(none.queries.is_empty());
    assert_eq!(none.total_seen, 0);
}

fn principals() -> Arc<Principals> {
    Arc::new(
        Principals::from_configs(&[
            PrincipalConfig {
                name: "console".into(),
                token: PLAIN.into(),
                ..Default::default()
            },
            PrincipalConfig {
                name: "ops".into(),
                token: ADMIN.into(),
                admin: true,
                ..Default::default()
            },
        ])
        .unwrap(),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_coordinator_service_needs_an_admin_principal() {
    let dir = tempdir("admin");
    let (node, _handle) = start_empty_node(config(dir.join("a.tv"), Layout::Segments)).await;
    ingest(&node).await;
    let guarded =
        serve_set(CollectionSet::single(coordinator(&node)).with_principals(principals())).await;
    let mut diag = DiagnosticsServiceClient::new(channel(&guarded).await);

    let anonymous = diag
        .get_runtime_knobs(bearer(GetRuntimeKnobsRequest {}, None))
        .await
        .unwrap_err();
    assert_eq!(anonymous.code(), Code::Unauthenticated);
    let plain = diag
        .get_runtime_knobs(bearer(GetRuntimeKnobsRequest {}, Some(PLAIN)))
        .await
        .unwrap_err();
    assert_eq!(plain.code(), Code::PermissionDenied);
    assert!(plain.message().contains("console"), "{}", plain.message());
    for (name, denied) in [
        ("knobs", {
            diag.set_runtime_knob(bearer(
                SetRuntimeKnobRequest {
                    name: "max_k".into(),
                    value: "10".into(),
                },
                Some(PLAIN),
            ))
            .await
            .map(|_| ())
        }),
        ("snapshot", {
            diag.get_metrics_snapshot(bearer(MetricsSnapshotRequest {}, Some(PLAIN)))
                .await
                .map(|_| ())
        }),
        ("shards", {
            diag.get_shard_diagnostics(bearer(ShardDiagnosticsRequest { shard: None }, Some(PLAIN)))
                .await
                .map(|_| ())
        }),
        ("recent", {
            diag.recent_queries(bearer(RecentQueriesRequest { limit: 0 }, Some(PLAIN)))
                .await
                .map(|_| ())
        }),
        ("stream", {
            diag.stream_metrics(bearer(StreamMetricsRequest { interval_ms: 0 }, Some(PLAIN)))
                .await
                .map(|_| ())
        }),
    ] {
        assert_eq!(denied.unwrap_err().code(), Code::PermissionDenied, "{name}");
    }

    let admin = diag
        .get_runtime_knobs(bearer(GetRuntimeKnobsRequest {}, Some(ADMIN)))
        .await
        .unwrap()
        .into_inner();
    let max_k = admin.knobs.iter().find(|k| k.name == "max_k").unwrap();
    assert_eq!(max_k.value, "50");
    assert!(max_k.mutable);
    assert_eq!(max_k.scope, KnobScope::Coordinator as i32);
    assert!(admin
        .knobs
        .iter()
        .any(|k| k.name == "nodes" && k.value == "1"));

    // A live max_k change caps the next request.
    diag.set_runtime_knob(bearer(
        SetRuntimeKnobRequest {
            name: "max_k".into(),
            value: "5".into(),
        },
        Some(ADMIN),
    ))
    .await
    .unwrap();
    let mut client = SearchServiceClient::new(channel(&guarded).await);
    let capped = client
        .query(bearer(filtered_dense("year >= 2000"), Some(PLAIN)))
        .await
        .unwrap_err();
    assert_eq!(capped.code(), Code::InvalidArgument);
    assert!(capped.message().contains("max_k=5"), "{}", capped.message());
    let shards = diag
        .get_shard_diagnostics(bearer(ShardDiagnosticsRequest { shard: None }, Some(ADMIN)))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(shards.shards[0].layout, "segments");
    let recent = diag
        .recent_queries(bearer(RecentQueriesRequest { limit: 0 }, Some(ADMIN)))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(recent.queries[0].principal, "console");
    assert_eq!(recent.queries[0].status, "InvalidArgument");

    // Without principals the service is open, like the rest.
    let open = serve_set(CollectionSet::single(coordinator(&node))).await;
    let mut odiag = DiagnosticsServiceClient::new(channel(&open).await);
    odiag
        .get_runtime_knobs(GetRuntimeKnobsRequest {})
        .await
        .unwrap();
}

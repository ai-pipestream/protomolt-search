//! Process-wide metrics and the Prometheus text-format exporter
//! (`docs/metrics.md`).
//!
//! Hand-rolled on purpose. The exposition format is a few lines of
//! `name{label="value"} 123\n` text and the scrape protocol is one GET,
//! so neither a metrics framework nor an HTTP framework earns a place
//! in the serving binary for it — the same argument that keeps CEL and
//! regex crates out (`docs/cel-filters.md`).
//!
//! The design splits along lifetime lines:
//!
//! - **Counters and histograms** are process-wide statics, fed at the
//!   one instrumentation seam every handler passes through ([`timed`]
//!   and [`timed_stream`]) plus the few places the engine already
//!   counted things (`record_scan`, `add_ingested`). Monotone, cheap
//!   (one `Instant::now()` at arrival, one per phase end, and a few
//!   relaxed atomic adds), and always on — whether or not an exporter
//!   serves them. Nothing is formatted per request: label strings are
//!   rendered at scrape time only.
//! - **Gauges** are read at SCRAPE time from live shard state, through
//!   closures the binary hands to [`serve`]. Nothing has to remember
//!   to update a gauge on every mutation, so a gauge can never go
//!   stale — it is the state, sampled. The one exception is the
//!   in-flight gauge, which is a counter pair by nature (arrivals minus
//!   departures) and lives with the counters.
//!
//! The route set is fixed at compile time (`REQUEST_ROUTES`), which is
//! what lets every table be a plain static indexed by the route's
//! discriminant: no label registry, no map, no lock.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Instant;

use tonic::{Code, Request, Response, Status};

/// One RPC route the exporter counts. The discriminant is the index
/// into every per-route table, so the table order in `REQUEST_ROUTES`
/// must match this declaration order (a unit test pins it).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(usize)]
pub enum Route {
    // Node (shard) routes.
    SearchShard,
    StreamSearch,
    BrowseShard,
    ResolveFilterBitmap,
    HybridShard,
    ShardLegs,
    Bm25Query,
    Bm25PhraseQuery,
    Bm25QueryStream,
    ReadWal,
    TermStats,
    ExpandTermPrefix,
    VectorRescore,
    ExactVectorRescore,
    Bm25Rescore,
    FetchValues,
    AggregateShard,
    QuantileCounts,
    GetDocuments,
    ResolveParents,
    AddDocuments,
    AddVectors,
    IngestMapped,
    DeleteDocuments,
    CommitReplacements,
    SuggestTerms,
    Suggest,
    TermSuggest,
    CompactShard,
    ExportSnapshot,
    StreamSnapshot,
    InstallSnapshotFrom,
    // Coordinator public routes.
    Search,
    Bm25Search,
    PhraseSearch,
    HybridSearch,
    Query,
    QueryStream,
    PlanIndex,
    RoutedIngestMapped,
    FreezeTopologyWrites,
    PublishTopology,
    AbortTopologyCutover,
    Aggregate,
    ClusterHealth,
    BroadcastVectorBackend,
    BroadcastCalibration,
    VariantSearch,
    // Cluster control routes.
    RegisterNode,
    RenewNodeLease,
    DrainNode,
    ReportShard,
    CompletePlacementAction,
    ReconcileCluster,
    GetClusterPlan,
    RollbackCluster,
    // Diagnostics (docs/diagnostics.md), on both nodes and coordinators.
    GetRuntimeKnobs,
    SetRuntimeKnob,
    GetMetricsSnapshot,
    StreamMetrics,
    GetShardDiagnostics,
    RecentQueries,
}

/// Route names as they appear in the `rpc` label, parallel to the
/// counter tables, with whether the route answers with a response
/// stream (and so reports two latency phases).
const REQUEST_ROUTES: [(Route, &str, bool); 62] = [
    (Route::SearchShard, "search_shard", true),
    (Route::StreamSearch, "stream_search", true),
    (Route::BrowseShard, "browse_shard", false),
    (Route::ResolveFilterBitmap, "resolve_filter_bitmap", false),
    (Route::HybridShard, "hybrid_shard", false),
    (Route::ShardLegs, "shard_legs", false),
    (Route::Bm25Query, "bm25_query", false),
    (Route::Bm25PhraseQuery, "bm25_phrase_query", false),
    (Route::Bm25QueryStream, "bm25_query_stream", true),
    (Route::ReadWal, "read_wal", true),
    (Route::TermStats, "term_stats", false),
    (Route::ExpandTermPrefix, "expand_term_prefix", false),
    (Route::VectorRescore, "vector_rescore", false),
    (Route::ExactVectorRescore, "exact_vector_rescore", false),
    (Route::Bm25Rescore, "bm25_rescore", false),
    (Route::FetchValues, "fetch_values", false),
    (Route::AggregateShard, "aggregate_shard", false),
    (Route::QuantileCounts, "quantile_counts", false),
    (Route::GetDocuments, "get_documents", false),
    (Route::ResolveParents, "resolve_parents", false),
    (Route::AddDocuments, "add_documents", false),
    (Route::AddVectors, "add_vectors", false),
    (Route::IngestMapped, "ingest_mapped", false),
    (Route::DeleteDocuments, "delete_documents", false),
    (Route::CommitReplacements, "commit_replacements", false),
    (Route::SuggestTerms, "suggest_terms", false),
    (Route::Suggest, "suggest", false),
    (Route::TermSuggest, "term_suggest", false),
    (Route::CompactShard, "compact_shard", false),
    (Route::ExportSnapshot, "export_snapshot", false),
    (Route::StreamSnapshot, "stream_snapshot", true),
    (Route::InstallSnapshotFrom, "install_snapshot_from", false),
    (Route::Search, "search", false),
    (Route::Bm25Search, "bm25_search", false),
    (Route::PhraseSearch, "phrase_search", false),
    (Route::HybridSearch, "hybrid_search", false),
    (Route::Query, "query", false),
    (Route::QueryStream, "query_stream", true),
    (Route::PlanIndex, "plan_index", false),
    (Route::RoutedIngestMapped, "routed_ingest_mapped", false),
    (Route::FreezeTopologyWrites, "freeze_topology_writes", false),
    (Route::PublishTopology, "publish_topology", false),
    (Route::AbortTopologyCutover, "abort_topology_cutover", false),
    (Route::Aggregate, "aggregate", false),
    (Route::ClusterHealth, "cluster_health", false),
    (
        Route::BroadcastVectorBackend,
        "broadcast_vector_backend",
        false,
    ),
    (Route::BroadcastCalibration, "broadcast_calibration", false),
    (Route::VariantSearch, "variant_search", false),
    (Route::RegisterNode, "register_node", false),
    (Route::RenewNodeLease, "renew_node_lease", false),
    (Route::DrainNode, "drain_node", false),
    (Route::ReportShard, "report_shard", false),
    (
        Route::CompletePlacementAction,
        "complete_placement_action",
        false,
    ),
    (Route::ReconcileCluster, "reconcile_cluster", false),
    (Route::GetClusterPlan, "get_cluster_plan", false),
    (Route::RollbackCluster, "rollback_cluster", false),
    (Route::GetRuntimeKnobs, "get_runtime_knobs", false),
    (Route::SetRuntimeKnob, "set_runtime_knob", false),
    (Route::GetMetricsSnapshot, "get_metrics_snapshot", false),
    (Route::StreamMetrics, "stream_metrics", true),
    (Route::GetShardDiagnostics, "get_shard_diagnostics", false),
    (Route::RecentQueries, "recent_queries", false),
];

const N_ROUTES: usize = REQUEST_ROUTES.len();

/// Histogram bucket upper bounds, chosen once (`docs/metrics.md`):
/// the `le` label text and the bound in nanoseconds. Prometheus
/// defines `le` as inclusive, and the compare below is integer, so
/// an observation of exactly 1ms lands in the 0.001 bucket.
const BUCKETS: [(&str, u64); 14] = [
    ("0.001", 1_000_000),
    ("0.002", 2_000_000),
    ("0.005", 5_000_000),
    ("0.01", 10_000_000),
    ("0.02", 20_000_000),
    ("0.05", 50_000_000),
    ("0.1", 100_000_000),
    ("0.2", 200_000_000),
    ("0.5", 500_000_000),
    ("1", 1_000_000_000),
    ("2", 2_000_000_000),
    ("5", 5_000_000_000),
    ("10", 10_000_000_000),
    ("+Inf", u64::MAX),
];

/// Error rows, one per canonical gRPC code the engine emits on
/// purpose, plus `other` for every remaining code (cancelled, unknown,
/// already_exists, aborted, out_of_range, unimplemented, data_loss).
/// Every (route, code) row is pre-declared so series exist from the
/// first scrape.
const ERROR_CODES: [&str; 10] = [
    "invalid_argument",
    "failed_precondition",
    "not_found",
    "resource_exhausted",
    "unavailable",
    "deadline_exceeded",
    "unauthenticated",
    "permission_denied",
    "internal",
    "other",
];

const N_CODES: usize = ERROR_CODES.len();

/// The error row for a status code; `None` for `Ok`, which is not an
/// error.
fn code_index(code: Code) -> Option<usize> {
    Some(match code {
        Code::Ok => return None,
        Code::InvalidArgument => 0,
        Code::FailedPrecondition => 1,
        Code::NotFound => 2,
        Code::ResourceExhausted => 3,
        Code::Unavailable => 4,
        Code::DeadlineExceeded => 5,
        Code::Unauthenticated => 6,
        Code::PermissionDenied => 7,
        Code::Internal => 8,
        _ => 9,
    })
}

/// One latency histogram: per-bucket (non-cumulative) counts and the
/// sum in nanoseconds. Rendering makes the buckets cumulative and the
/// sum a float, as the exposition format wants; storing them this way
/// keeps an observation at two relaxed adds.
struct Histogram {
    buckets: [AtomicU64; BUCKETS.len()],
    sum_ns: AtomicU64,
}

impl Histogram {
    const fn new() -> Self {
        Histogram {
            buckets: [ZERO; BUCKETS.len()],
            sum_ns: AtomicU64::new(0),
        }
    }

    fn observe(&self, ns: u64) {
        let bucket = BUCKETS
            .iter()
            .position(|&(_, le)| ns <= le)
            .expect("+Inf catches everything");
        self.buckets[bucket].fetch_add(1, Ordering::Relaxed);
        self.sum_ns.fetch_add(ns, Ordering::Relaxed);
    }
}

// A `const` here is the repeat-element initializer for the arrays
// below, not a shared value: each array slot gets its OWN atomic (the
// interior-mutability lint's hazard — accidentally sharing one — is
// exactly what the repeat semantics avoid).
#[allow(clippy::declare_interior_mutable_const)]
const ZERO: AtomicU64 = AtomicU64::new(0);
#[allow(clippy::declare_interior_mutable_const)]
const ZERO_ROW: [AtomicU64; N_CODES] = [ZERO; N_CODES];
#[allow(clippy::declare_interior_mutable_const)]
const EMPTY: Histogram = Histogram::new();

static REQUESTS: [AtomicU64; N_ROUTES] = [ZERO; N_ROUTES];
static IN_FLIGHT: [AtomicU64; N_ROUTES] = [ZERO; N_ROUTES];
/// Arrival to the handler's terminal outcome (unary) or the response
/// stream's terminal event (streaming): the `complete` phase.
static COMPLETE: [Histogram; N_ROUTES] = [EMPTY; N_ROUTES];
/// Arrival to the first message handed to the transport; observed only
/// on streaming routes.
static FIRST_RESPONSE: [Histogram; N_ROUTES] = [EMPTY; N_ROUTES];
static ERRORS: [[AtomicU64; N_CODES]; N_ROUTES] = [ZERO_ROW; N_ROUTES];

static SCAN_CHUNK_CALLS: AtomicU64 = AtomicU64::new(0);
static SCAN_CANDIDATES: AtomicU64 = AtomicU64::new(0);
static SCAN_FLOORS_OFFERED: AtomicU64 = AtomicU64::new(0);
static SCAN_FLOORS_PUBLISHED: AtomicU64 = AtomicU64::new(0);
static SCAN_FLOOR_UPDATES_APPLIED: AtomicU64 = AtomicU64::new(0);
static DOCUMENTS_ADDED: AtomicU64 = AtomicU64::new(0);
static VECTORS_ADDED: AtomicU64 = AtomicU64::new(0);

/// Count one served request on `route`, at the top of its handler —
/// arrivals, not successes, so a shard erroring under load is visible
/// as traffic rather than invisible as silence. [`timed`] calls this;
/// it stays public for the one-off counting a caller without a
/// request body may need.
pub fn inc_request(route: Route) {
    REQUESTS[route as usize].fetch_add(1, Ordering::Relaxed);
}

/// Marker on a request that is a re-dispatch of one already counted —
/// the coordinator delegating to its topology snapshot, a variant
/// search running its arms through the ordinary routes. [`timed`]
/// runs a marked request's body without counting it, so one arrival
/// is one count however many trait methods it passes through.
#[derive(Debug, Clone, Copy)]
struct Nested;

/// Mark `request` as already counted (see [`Nested`]).
pub fn nested<T>(mut request: Request<T>) -> Request<T> {
    request.extensions_mut().insert(Nested);
    request
}

/// The in-flight bookkeeping for one counted request: arrival counted
/// and the clock started on construction, the terminal outcome
/// recorded exactly once by `finish` or — if the future or the stream
/// is dropped first, the client having gone away — by `Drop`, as
/// `CANCELLED`, so the in-flight gauge always returns to zero.
struct Ticket {
    idx: usize,
    start: Instant,
    first_done: bool,
    done: bool,
}

impl Ticket {
    fn start(route: Route) -> Self {
        let idx = route as usize;
        REQUESTS[idx].fetch_add(1, Ordering::Relaxed);
        IN_FLIGHT[idx].fetch_add(1, Ordering::Relaxed);
        Ticket {
            idx,
            start: Instant::now(),
            first_done: false,
            done: false,
        }
    }

    fn elapsed_ns(&self) -> u64 {
        u64::try_from(self.start.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }

    /// The first message of a response stream reached the transport.
    fn first_response(&mut self) {
        if !self.first_done {
            self.first_done = true;
            FIRST_RESPONSE[self.idx].observe(self.elapsed_ns());
        }
    }

    /// The terminal outcome. A streaming route that never produced a
    /// first message (refused before the stream opened, or an empty
    /// stream) records its first-response phase here too, so both
    /// phases count every request.
    fn finish(&mut self, code: Code) {
        if self.done {
            return;
        }
        self.done = true;
        let ns = self.elapsed_ns();
        if REQUEST_ROUTES[self.idx].2 && !self.first_done {
            self.first_done = true;
            FIRST_RESPONSE[self.idx].observe(ns);
        }
        COMPLETE[self.idx].observe(ns);
        if let Some(code) = code_index(code) {
            ERRORS[self.idx][code].fetch_add(1, Ordering::Relaxed);
        }
        IN_FLIGHT[self.idx].fetch_sub(1, Ordering::Relaxed);
    }
}

impl Drop for Ticket {
    fn drop(&mut self) {
        self.finish(Code::Cancelled);
    }
}

fn is_nested<T>(request: &Request<T>) -> bool {
    request.extensions().get::<Nested>().is_some()
}

/// The instrumentation seam for a unary-response handler: count the
/// arrival, time `body`, and record the outcome by the gRPC code that
/// left it. A [`nested`] request runs uncounted. The handler keeps its
/// own shape — `timed(route, request, |request| async move { .. })`.
pub async fn timed<R, T, F, Fut>(route: Route, request: Request<R>, body: F) -> Result<T, Status>
where
    F: FnOnce(Request<R>) -> Fut,
    Fut: Future<Output = Result<T, Status>>,
{
    if is_nested(&request) {
        return body(request).await;
    }
    let mut ticket = Ticket::start(route);
    let out = body(request).await;
    ticket.finish(out.as_ref().err().map_or(Code::Ok, Status::code));
    out
}

/// The seam for a handler that answers with a response stream. The
/// stream comes back wrapped in [`Timed`], which records the
/// `first_response` phase when the first message is handed to the
/// transport and the `complete` phase (plus the error row and the
/// in-flight departure) at the stream's terminal event: exhaustion, a
/// terminal `Err`, or the stream being dropped unread. A refusal
/// before the stream opens records both phases at once.
pub async fn timed_stream<R, S, F, Fut>(
    route: Route,
    request: Request<R>,
    body: F,
) -> Result<Response<Timed<S>>, Status>
where
    F: FnOnce(Request<R>) -> Fut,
    Fut: Future<Output = Result<Response<S>, Status>>,
{
    if is_nested(&request) {
        return body(request).await.map(|response| {
            response.map(|inner| Timed {
                inner,
                ticket: None,
            })
        });
    }
    let mut ticket = Ticket::start(route);
    match body(request).await {
        Ok(response) => Ok(response.map(|inner| Timed {
            inner,
            ticket: Some(ticket),
        })),
        Err(status) => {
            ticket.finish(status.code());
            Err(status)
        }
    }
}

/// A response stream carrying its request's [`Ticket`]; see
/// [`timed_stream`]. A nested request's stream carries none.
pub struct Timed<S> {
    inner: S,
    ticket: Option<Ticket>,
}

impl<S> Timed<S> {
    /// Unwrap a NESTED re-dispatch's stream so the outer handler can
    /// hand it to its own seam. Only a nested stream carries no
    /// ticket; unwrapping a counted one would drop its ticket and
    /// record a cancellation for a stream that is still running.
    pub fn into_inner(self) -> S {
        debug_assert!(
            self.ticket.is_none(),
            "Timed::into_inner is for nested re-dispatch streams only"
        );
        self.inner
    }
}

impl<S, M> tokio_stream::Stream for Timed<S>
where
    S: tokio_stream::Stream<Item = Result<M, Status>> + Unpin,
{
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        let next = Pin::new(&mut this.inner).poll_next(cx);
        if let Some(ticket) = this.ticket.as_mut() {
            match &next {
                Poll::Ready(Some(Ok(_))) => ticket.first_response(),
                Poll::Ready(Some(Err(status))) => ticket.finish(status.code()),
                Poll::Ready(None) => ticket.finish(Code::Ok),
                Poll::Pending => {}
            }
        }
        next
    }
}

/// Fold one completed vector scan's stats into the process totals.
/// Called where the scan produces its `ScanOutcome`, so every route
/// through the scheduler (batched or solo) is counted once.
pub fn record_scan(stats: &crate::chunked::ScanStats) {
    SCAN_CHUNK_CALLS.fetch_add(u64::from(stats.chunk_calls), Ordering::Relaxed);
    SCAN_CANDIDATES.fetch_add(stats.candidates_collected, Ordering::Relaxed);
    SCAN_FLOORS_OFFERED.fetch_add(stats.floors_offered, Ordering::Relaxed);
    SCAN_FLOORS_PUBLISHED.fetch_add(stats.floors_published, Ordering::Relaxed);
    SCAN_FLOOR_UPDATES_APPLIED.fetch_add(stats.floor_updates_applied, Ordering::Relaxed);
}

/// Count ingested items: `documents` from AddDocuments streams,
/// `vectors` from AddVectors batches.
pub fn add_ingested(documents: u64, vectors: u64) {
    if documents > 0 {
        DOCUMENTS_ADDED.fetch_add(documents, Ordering::Relaxed);
    }
    if vectors > 0 {
        VECTORS_ADDED.fetch_add(vectors, Ordering::Relaxed);
    }
}

/// One shard's gauges, sampled at SCRAPE time from live state, so a
/// gauge can never go stale and never needs an update site.
#[derive(Debug, Clone)]
pub struct ShardGauges {
    /// The shard's identity label value: its slot offset, the shard's
    /// name in the global id space.
    pub slot_offset: u64,
    /// Vectors in the shard's index.
    pub vectors: u64,
    /// Documents in the shard's postings.
    pub documents: u64,
    /// The BM25 statistics epoch (advances on every mutation).
    pub stats_epoch: u64,
}

/// A live-state gauge sampler for one shard. Returns VALUES rather
/// than rendering text so [`render`] can group all shards' samples of
/// one metric under a single `# TYPE` header, as the exposition
/// format requires.
pub type GaugeProvider = Box<dyn Fn() -> ShardGauges + Send + Sync>;

fn write_sample_head(out: &mut String, name: &str, labels: &str) {
    out.push_str(name);
    if !labels.is_empty() {
        out.push('{');
        out.push_str(labels);
        out.push('}');
    }
    out.push(' ');
}

/// Append one metric line. Public so gauge providers in other modules
/// render values the same way counters are rendered (Prometheus wants
/// consistent float formatting; integers print as integers).
pub fn write_metric(out: &mut String, name: &str, labels: &str, value: u64) {
    write_sample_head(out, name, labels);
    out.push_str(&value.to_string());
    out.push('\n');
}

/// Append one metric line whose value is a duration in seconds,
/// rendered as an exact decimal from the nanosecond total (no float
/// arithmetic, so `_sum` never shows a rounding tail); always carries
/// a decimal point so the value reads as a float.
pub fn write_metric_seconds(out: &mut String, name: &str, labels: &str, ns: u64) {
    write_sample_head(out, name, labels);
    out.push_str(&seconds_text(ns));
    out.push('\n');
}

/// `ns` nanoseconds as decimal seconds: `1_500_000` renders `0.0015`,
/// `0` renders `0.0`.
fn seconds_text(ns: u64) -> String {
    let mut text = format!("{}.{:09}", ns / 1_000_000_000, ns % 1_000_000_000);
    while text.ends_with('0') {
        text.pop();
    }
    if text.ends_with('.') {
        text.push('0');
    }
    text
}

fn header(out: &mut String, name: &str, kind: &str, help: &str) {
    out.push_str("# HELP ");
    out.push_str(name);
    out.push(' ');
    out.push_str(help);
    out.push_str("\n# TYPE ");
    out.push_str(name);
    out.push(' ');
    out.push_str(kind);
    out.push('\n');
}

const DURATION: &str = "turbovec_request_duration_seconds";

/// One histogram's exposition rows under `labels` (the `rpc` and, for
/// a streaming phase, `phase` labels): cumulative buckets, `_sum`,
/// `_count`. `_count` is the `+Inf` bucket by construction.
fn write_histogram(out: &mut String, labels: &str, histogram: &Histogram) {
    let mut cumulative = 0u64;
    for (i, (le, _)) in BUCKETS.iter().enumerate() {
        cumulative += histogram.buckets[i].load(Ordering::Relaxed);
        write_metric(
            out,
            &format!("{DURATION}_bucket"),
            &format!("{labels},le=\"{le}\""),
            cumulative,
        );
    }
    write_metric_seconds(
        out,
        &format!("{DURATION}_sum"),
        labels,
        histogram.sum_ns.load(Ordering::Relaxed),
    );
    write_metric(out, &format!("{DURATION}_count"), labels, cumulative);
}

/// Render the whole exposition page: the process-wide counters, then
/// every gauge provider in order.
pub fn render(gauges: &[GaugeProvider]) -> String {
    let mut out = String::with_capacity(96 * 1024);

    header(
        &mut out,
        "turbovec_requests_total",
        "counter",
        "Requests served, by RPC route (counted at arrival).",
    );
    for (i, (_, name, _)) in REQUEST_ROUTES.iter().enumerate() {
        write_metric(
            &mut out,
            "turbovec_requests_total",
            &format!("rpc=\"{name}\""),
            REQUESTS[i].load(Ordering::Relaxed),
        );
    }

    header(
        &mut out,
        "turbovec_requests_in_flight",
        "gauge",
        "Requests inside a handler or with an open response stream, by RPC route.",
    );
    for (i, (_, name, _)) in REQUEST_ROUTES.iter().enumerate() {
        write_metric(
            &mut out,
            "turbovec_requests_in_flight",
            &format!("rpc=\"{name}\""),
            IN_FLIGHT[i].load(Ordering::Relaxed),
        );
    }

    header(
        &mut out,
        DURATION,
        "histogram",
        "Request latency in seconds from arrival, by RPC route; streaming routes \
         report phase=\"first_response\" (first message on the wire) and \
         phase=\"complete\" (stream ended).",
    );
    for (i, (_, name, streaming)) in REQUEST_ROUTES.iter().enumerate() {
        if *streaming {
            write_histogram(
                &mut out,
                &format!("rpc=\"{name}\",phase=\"first_response\""),
                &FIRST_RESPONSE[i],
            );
            write_histogram(
                &mut out,
                &format!("rpc=\"{name}\",phase=\"complete\""),
                &COMPLETE[i],
            );
        } else {
            write_histogram(&mut out, &format!("rpc=\"{name}\""), &COMPLETE[i]);
        }
    }

    header(
        &mut out,
        "turbovec_request_errors_total",
        "counter",
        "Requests that ended in a gRPC error, by RPC route and canonical status code.",
    );
    for (i, (_, name, _)) in REQUEST_ROUTES.iter().enumerate() {
        for (c, code) in ERROR_CODES.iter().enumerate() {
            write_metric(
                &mut out,
                "turbovec_request_errors_total",
                &format!("rpc=\"{name}\",code=\"{code}\""),
                ERRORS[i][c].load(Ordering::Relaxed),
            );
        }
    }

    for (name, help, counter) in [
        (
            "turbovec_scan_chunk_calls_total",
            "Per-chunk kernel calls made by vector scans.",
            &SCAN_CHUNK_CALLS,
        ),
        (
            "turbovec_scan_candidates_total",
            "Real candidates collected by vector scans (floor sharing's savings show here).",
            &SCAN_CANDIDATES,
        ),
        (
            "turbovec_scan_floors_offered_total",
            "Floors the scan offered to publish (its own behavior, knob-independent).",
            &SCAN_FLOORS_OFFERED,
        ),
        (
            "turbovec_scan_floors_published_total",
            "Floors actually put on the wire (what the floor knobs move).",
            &SCAN_FLOORS_PUBLISHED,
        ),
        (
            "turbovec_scan_floor_updates_applied_total",
            "Chunks that ran under a coordinator-pushed floor.",
            &SCAN_FLOOR_UPDATES_APPLIED,
        ),
        (
            "turbovec_documents_added_total",
            "Documents ingested over AddDocuments streams.",
            &DOCUMENTS_ADDED,
        ),
        (
            "turbovec_vectors_added_total",
            "Vectors ingested over AddVectors batches.",
            &VECTORS_ADDED,
        ),
    ] {
        header(&mut out, name, "counter", help);
        write_metric(&mut out, name, "", counter.load(Ordering::Relaxed));
    }

    let (batches, jobs) = crate::node::scan_batch_counters();
    header(
        &mut out,
        "turbovec_scan_batches_total",
        "counter",
        "Batched kernel passes (coalesced scans).",
    );
    write_metric(&mut out, "turbovec_scan_batches_total", "", batches);
    header(
        &mut out,
        "turbovec_scan_batched_jobs_total",
        "counter",
        "Scan jobs that rode a batched pass.",
    );
    write_metric(&mut out, "turbovec_scan_batched_jobs_total", "", jobs);

    if !gauges.is_empty() {
        let samples: Vec<ShardGauges> = gauges.iter().map(|g| g()).collect();
        for (name, help, read) in [
            (
                "turbovec_shard_vectors",
                "Vectors in the shard's index.",
                (|s| s.vectors) as fn(&ShardGauges) -> u64,
            ),
            (
                "turbovec_shard_documents",
                "Documents in the shard's postings.",
                |s| s.documents,
            ),
            (
                "turbovec_shard_stats_epoch",
                "BM25 statistics epoch (advances on every mutation).",
                |s| s.stats_epoch,
            ),
        ] {
            header(&mut out, name, "gauge", help);
            for sample in &samples {
                write_metric(
                    &mut out,
                    name,
                    &format!("slot_offset=\"{}\"", sample.slot_offset),
                    read(sample),
                );
            }
        }
    }
    out
}

/// Serve `render` over HTTP on `listener`, forever. The server answers
/// every request on the socket with the metrics page — it parses
/// nothing beyond draining the request head, because the only client
/// is a scraper and the only resource is the page. The page is plain
/// HTTP with no auth (TLS and bearer principals cover the gRPC
/// listeners, `docs/security.md`, not this one): bind the listener to
/// a trusted interface.
#[cfg(feature = "net")]
/// The registry as values (`docs/diagnostics.md`): the same counters,
/// gauges, and histograms [`render`] prints, in the same order, with the
/// same names and labels, so a dashboard and a scraper never disagree.
pub fn snapshot(process: &str, gauges: &[GaugeProvider]) -> crate::pb::MetricsSnapshot {
    use crate::pb::{
        HistogramBucketSample, HistogramSample, MetricKind, MetricLabel, MetricSample,
    };
    fn label(name: &str, value: &str) -> MetricLabel {
        MetricLabel {
            name: name.to_string(),
            value: value.to_string(),
        }
    }
    fn counter(name: &str, labels: Vec<MetricLabel>, value: u64) -> MetricSample {
        MetricSample {
            name: name.to_string(),
            labels,
            kind: MetricKind::Counter as i32,
            value: value as f64,
        }
    }
    fn gauge(name: &str, labels: Vec<MetricLabel>, value: u64) -> MetricSample {
        MetricSample {
            name: name.to_string(),
            labels,
            kind: MetricKind::Gauge as i32,
            value: value as f64,
        }
    }
    fn histogram(labels: Vec<MetricLabel>, h: &Histogram) -> HistogramSample {
        let mut cumulative = 0u64;
        let mut buckets = Vec::with_capacity(BUCKETS.len());
        for (i, (_, le)) in BUCKETS.iter().enumerate() {
            cumulative += h.buckets[i].load(Ordering::Relaxed);
            buckets.push(HistogramBucketSample {
                le: if *le == u64::MAX {
                    f64::INFINITY
                } else {
                    *le as f64 / 1e9
                },
                cumulative_count: cumulative,
            });
        }
        HistogramSample {
            name: DURATION.to_string(),
            labels,
            buckets,
            sum: h.sum_ns.load(Ordering::Relaxed) as f64 / 1e9,
            count: cumulative,
        }
    }

    let mut samples = Vec::new();
    let mut histograms = Vec::new();
    for (i, (_, name, _)) in REQUEST_ROUTES.iter().enumerate() {
        samples.push(counter(
            "turbovec_requests_total",
            vec![label("rpc", name)],
            REQUESTS[i].load(Ordering::Relaxed),
        ));
    }
    for (i, (_, name, _)) in REQUEST_ROUTES.iter().enumerate() {
        samples.push(gauge(
            "turbovec_requests_in_flight",
            vec![label("rpc", name)],
            IN_FLIGHT[i].load(Ordering::Relaxed),
        ));
    }
    for (i, (_, name, streaming)) in REQUEST_ROUTES.iter().enumerate() {
        if *streaming {
            histograms.push(histogram(
                vec![label("rpc", name), label("phase", "first_response")],
                &FIRST_RESPONSE[i],
            ));
            histograms.push(histogram(
                vec![label("rpc", name), label("phase", "complete")],
                &COMPLETE[i],
            ));
        } else {
            histograms.push(histogram(vec![label("rpc", name)], &COMPLETE[i]));
        }
    }
    for (i, (_, name, _)) in REQUEST_ROUTES.iter().enumerate() {
        for (c, code) in ERROR_CODES.iter().enumerate() {
            samples.push(counter(
                "turbovec_request_errors_total",
                vec![label("rpc", name), label("code", code)],
                ERRORS[i][c].load(Ordering::Relaxed),
            ));
        }
    }
    for (name, atomic) in [
        ("turbovec_scan_chunk_calls_total", &SCAN_CHUNK_CALLS),
        ("turbovec_scan_candidates_total", &SCAN_CANDIDATES),
        ("turbovec_scan_floors_offered_total", &SCAN_FLOORS_OFFERED),
        (
            "turbovec_scan_floors_published_total",
            &SCAN_FLOORS_PUBLISHED,
        ),
        (
            "turbovec_scan_floor_updates_applied_total",
            &SCAN_FLOOR_UPDATES_APPLIED,
        ),
        ("turbovec_documents_added_total", &DOCUMENTS_ADDED),
        ("turbovec_vectors_added_total", &VECTORS_ADDED),
    ] {
        samples.push(counter(name, Vec::new(), atomic.load(Ordering::Relaxed)));
    }
    let (batches, jobs) = crate::node::scan_batch_counters();
    samples.push(counter("turbovec_scan_batches_total", Vec::new(), batches));
    samples.push(counter(
        "turbovec_scan_batched_jobs_total",
        Vec::new(),
        jobs,
    ));
    if !gauges.is_empty() {
        let shards: Vec<ShardGauges> = gauges.iter().map(|g| g()).collect();
        for (name, read) in [
            (
                "turbovec_shard_vectors",
                (|s| s.vectors) as fn(&ShardGauges) -> u64,
            ),
            ("turbovec_shard_documents", |s| s.documents),
            ("turbovec_shard_stats_epoch", |s| s.stats_epoch),
        ] {
            for shard in &shards {
                samples.push(gauge(
                    name,
                    vec![label("slot_offset", &shard.slot_offset.to_string())],
                    read(shard),
                ));
            }
        }
    }
    crate::pb::MetricsSnapshot {
        unix_ms: crate::diagnostics::unix_ms(),
        process: process.to_string(),
        samples,
        histograms,
    }
}

pub async fn serve(listener: tokio::net::TcpListener, gauges: Vec<GaugeProvider>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let gauges = std::sync::Arc::new(gauges);
    loop {
        let Ok((mut socket, _)) = listener.accept().await else {
            continue;
        };
        let gauges = gauges.clone();
        tokio::spawn(async move {
            // Drain the request head (up to a bound; a scraper's GET is
            // tiny) so the peer never sees a reset before our response.
            let mut buf = [0u8; 4096];
            let mut head = Vec::new();
            loop {
                match socket.read(&mut buf).await {
                    Ok(0) | Err(_) => return,
                    Ok(n) => {
                        head.extend_from_slice(&buf[..n]);
                        if head.windows(4).any(|w| w == b"\r\n\r\n") || head.len() > 16 * 1024 {
                            break;
                        }
                    }
                }
            }
            let body = render(&gauges);
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4; \
                 charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = socket.write_all(response.as_bytes()).await;
            let _ = socket.shutdown().await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_stream::StreamExt;

    /// The value of the first sample line starting with `needle`.
    fn sample(page: &str, needle: &str) -> u64 {
        page.lines()
            .find(|l| l.starts_with(needle))
            .and_then(|l| l.rsplit_once(' '))
            .and_then(|(_, v)| v.parse().ok())
            .unwrap_or_else(|| panic!("no sample line starting {needle:?}"))
    }

    /// Every per-route static is indexed by the discriminant, so the
    /// table must list the routes in declaration order, once each,
    /// under distinct names.
    #[test]
    fn route_table_matches_discriminants() {
        let mut names = std::collections::HashSet::new();
        for (i, (route, name, _)) in REQUEST_ROUTES.iter().enumerate() {
            assert_eq!(*route as usize, i, "{route:?} sits at row {i}");
            assert!(names.insert(*name), "duplicate route name {name}");
        }
    }

    /// `le` is inclusive, as Prometheus defines it; buckets are
    /// cumulative on the page; `_count` equals the `+Inf` bucket.
    #[test]
    fn buckets_are_inclusive_and_cumulative() {
        let h = Histogram::new();
        h.observe(1_000_000); // exactly 1ms: the 0.001 bucket
        h.observe(1_000_001); // just over: the 0.002 bucket
        h.observe(0);
        h.observe(999_999_999_999); // beyond 10s: +Inf only
        assert_eq!(h.buckets[0].load(Ordering::Relaxed), 2);
        assert_eq!(h.buckets[1].load(Ordering::Relaxed), 1);
        assert_eq!(h.buckets[13].load(Ordering::Relaxed), 1);
        assert_eq!(h.sum_ns.load(Ordering::Relaxed), 1_000_002_000_000);

        let mut out = String::new();
        write_histogram(&mut out, "rpc=\"t\"", &h);
        let bucket = |le: &str| {
            sample(
                &out,
                &format!("{DURATION}_bucket{{rpc=\"t\",le=\"{le}\"}} "),
            )
        };
        assert_eq!(bucket("0.001"), 2);
        assert_eq!(bucket("0.002"), 3);
        assert_eq!(bucket("10"), 3);
        assert_eq!(bucket("+Inf"), 4);
        assert_eq!(sample(&out, &format!("{DURATION}_count{{rpc=\"t\"}} ")), 4);
        assert!(
            out.contains(&format!("{DURATION}_sum{{rpc=\"t\"}} 1000.002\n")),
            "{out}"
        );
        assert_eq!(BUCKETS.len(), 14);
        assert_eq!(BUCKETS[13].0, "+Inf");
    }

    /// Seconds render as exact decimals with a decimal point, never in
    /// exponent form and never with a float rounding tail.
    #[test]
    fn seconds_render_as_exact_decimals() {
        assert_eq!(seconds_text(0), "0.0");
        assert_eq!(seconds_text(1), "0.000000001");
        assert_eq!(seconds_text(1_500_000), "0.0015");
        assert_eq!(seconds_text(2_000_000_000), "2.0");
        assert_eq!(seconds_text(300_000_000), "0.3");
        assert_eq!(seconds_text(12_345_678_901), "12.345678901");
        for text in [seconds_text(0), seconds_text(1), seconds_text(u64::MAX)] {
            assert!(text.contains('.'), "{text}");
            assert!(text.parse::<f64>().is_ok(), "{text}");
        }
    }

    /// Every canonical code the engine emits on purpose has its own
    /// row; the rest fold into `other`; `Ok` is not an error.
    #[test]
    fn codes_map_to_rows() {
        assert_eq!(code_index(Code::Ok), None);
        for (code, name) in [
            (Code::InvalidArgument, "invalid_argument"),
            (Code::FailedPrecondition, "failed_precondition"),
            (Code::NotFound, "not_found"),
            (Code::ResourceExhausted, "resource_exhausted"),
            (Code::Unavailable, "unavailable"),
            (Code::DeadlineExceeded, "deadline_exceeded"),
            (Code::Unauthenticated, "unauthenticated"),
            (Code::PermissionDenied, "permission_denied"),
            (Code::Internal, "internal"),
        ] {
            assert_eq!(ERROR_CODES[code_index(code).unwrap()], name);
        }
        for code in [
            Code::Cancelled,
            Code::Unknown,
            Code::AlreadyExists,
            Code::Aborted,
            Code::OutOfRange,
            Code::Unimplemented,
            Code::DataLoss,
        ] {
            assert_eq!(ERROR_CODES[code_index(code).unwrap()], "other", "{code:?}");
        }
    }

    /// The page is valid exposition text for what this module emits:
    /// every non-comment line is `name[{labels}] value` with a numeric
    /// value (integers everywhere except the histogram `_sum`, which is
    /// a float), every metric has HELP and TYPE, and gauges render
    /// after counters in provider order.
    #[test]
    fn page_shape_is_exposition_text() {
        inc_request(Route::SearchShard);
        inc_request(Route::SearchShard);
        add_ingested(3, 7);
        let gauges: Vec<GaugeProvider> = vec![
            Box::new(|| ShardGauges {
                slot_offset: 0,
                vectors: 42,
                documents: 17,
                stats_epoch: 3,
            }),
            Box::new(|| ShardGauges {
                slot_offset: 1000,
                vectors: 7,
                documents: 7,
                stats_epoch: 1,
            }),
        ];
        let page = render(&gauges);
        assert!(sample(&page, "turbovec_requests_total{rpc=\"search_shard\"} ") >= 2);
        assert!(page.contains("turbovec_documents_added_total 3"));
        assert!(page.contains("turbovec_vectors_added_total 7"));
        assert!(page.contains("turbovec_shard_vectors{slot_offset=\"0\"} 42"));
        assert!(page.contains("turbovec_shard_vectors{slot_offset=\"1000\"} 7"));
        assert!(page.contains("turbovec_shard_documents{slot_offset=\"0\"} 17"));
        assert!(page.contains("# TYPE turbovec_request_duration_seconds histogram"));
        assert!(page.contains("# TYPE turbovec_requests_in_flight gauge"));
        // Grouping: both shards' samples of one metric sit under ONE
        // TYPE header, as the exposition format requires.
        assert_eq!(page.matches("# TYPE turbovec_shard_vectors").count(), 1);
        assert_eq!(
            page.matches("# TYPE turbovec_request_duration_seconds ")
                .count(),
            1
        );
        let mut sums = 0;
        for line in page.lines() {
            if line.starts_with('#') {
                continue;
            }
            let (name, value) = line.rsplit_once(' ').expect("name value");
            let bare = name.split('{').next().unwrap();
            if bare == format!("{DURATION}_sum") {
                sums += 1;
                assert!(value.contains('.'), "float sum in {line:?}");
                assert!(value.parse::<f64>().is_ok(), "numeric value in {line:?}");
            } else {
                assert!(value.parse::<u64>().is_ok(), "numeric value in {line:?}");
            }
            // Histogram children hang off the histogram's own TYPE line.
            let family = bare
                .strip_suffix("_bucket")
                .or_else(|| bare.strip_suffix("_sum"))
                .or_else(|| bare.strip_suffix("_count"))
                .filter(|family| *family == DURATION)
                .unwrap_or(bare);
            assert!(
                page.contains(&format!("# TYPE {family} ")),
                "{bare} has a TYPE line"
            );
        }
        // One `_sum` per unary route plus two per streaming route.
        let streaming = REQUEST_ROUTES.iter().filter(|r| r.2).count();
        assert_eq!(sums, N_ROUTES + streaming);
    }

    /// Every route pre-declares its histogram and all ten error rows
    /// from the first scrape; streaming routes carry both phases and
    /// unary routes carry no phase label.
    #[test]
    fn every_route_has_histogram_and_error_rows() {
        let page = render(&[]);
        for (_, name, streaming) in REQUEST_ROUTES {
            for code in ERROR_CODES {
                assert!(
                    page.contains(&format!(
                        "turbovec_request_errors_total{{rpc=\"{name}\",code=\"{code}\"}} "
                    )),
                    "{name}/{code}"
                );
            }
            assert!(page.contains(&format!("turbovec_requests_in_flight{{rpc=\"{name}\"}} ")));
            if streaming {
                for phase in ["first_response", "complete"] {
                    assert!(page.contains(&format!(
                        "{DURATION}_count{{rpc=\"{name}\",phase=\"{phase}\"}} "
                    )));
                    assert!(page.contains(&format!(
                        "{DURATION}_bucket{{rpc=\"{name}\",phase=\"{phase}\",le=\"+Inf\"}} "
                    )));
                }
                assert!(!page.contains(&format!("{DURATION}_count{{rpc=\"{name}\"}} ")));
            } else {
                assert!(page.contains(&format!("{DURATION}_count{{rpc=\"{name}\"}} ")));
                assert!(page.contains(&format!("{DURATION}_bucket{{rpc=\"{name}\",le=\"+Inf\"}} ")));
                assert!(!page.contains(&format!("{DURATION}_count{{rpc=\"{name}\",phase=")));
            }
        }
    }

    /// Every route increments its own row and only its own row.
    #[test]
    fn routes_count_independently() {
        let before = render(&[]);
        let count = |page: &str, rpc: &str| -> u64 {
            sample(page, &format!("turbovec_requests_total{{rpc=\"{rpc}\"}} "))
        };
        inc_request(Route::Bm25Query);
        let after = render(&[]);
        assert_eq!(
            count(&after, "bm25_query"),
            count(&before, "bm25_query") + 1
        );
        assert_eq!(count(&after, "term_stats"), count(&before, "term_stats"));
    }

    fn snapshot(route: Route) -> (u64, u64, u64, u64, [u64; N_CODES]) {
        let i = route as usize;
        let count = |h: &Histogram| h.buckets.iter().map(|b| b.load(Ordering::Relaxed)).sum();
        let mut errors = [0u64; N_CODES];
        for (c, slot) in errors.iter_mut().enumerate() {
            *slot = ERRORS[i][c].load(Ordering::Relaxed);
        }
        (
            REQUESTS[i].load(Ordering::Relaxed),
            IN_FLIGHT[i].load(Ordering::Relaxed),
            count(&COMPLETE[i]),
            count(&FIRST_RESPONSE[i]),
            errors,
        )
    }

    /// `timed` counts the arrival, holds the in-flight slot for the
    /// body's duration, records the outcome by code, and lets the
    /// gauge return to zero — on success, on refusal, and when the
    /// handler future is dropped before it answers.
    #[tokio::test]
    async fn timed_records_outcomes_and_in_flight_returns_to_zero() {
        let route = Route::RollbackCluster;
        let (req0, inf0, done0, _, err0) = snapshot(route);
        assert_eq!(inf0, 0);

        let ok: Result<u32, Status> = timed(route, Request::new(()), |request| async move {
            assert_eq!(IN_FLIGHT[route as usize].load(Ordering::Relaxed), 1);
            let () = request.into_inner();
            Ok(7)
        })
        .await;
        assert_eq!(ok.unwrap(), 7);
        let (req1, inf1, done1, _, err1) = snapshot(route);
        assert_eq!((req1, inf1, done1, err1), (req0 + 1, 0, done0 + 1, err0));

        let refused: Result<u32, Status> = timed(route, Request::new(()), |_| async move {
            Err(Status::failed_precondition("no"))
        })
        .await;
        assert_eq!(refused.unwrap_err().code(), Code::FailedPrecondition);
        let (req2, inf2, done2, _, err2) = snapshot(route);
        assert_eq!((req2, inf2, done2), (req0 + 2, 0, done0 + 2));
        assert_eq!(err2[1], err1[1] + 1);

        // The client goes away: the handler future is dropped mid-body.
        let abandoned = tokio::time::timeout(
            std::time::Duration::from_millis(5),
            timed(route, Request::new(()), |_| async move {
                std::future::pending::<Result<u32, Status>>().await
            }),
        )
        .await;
        assert!(abandoned.is_err());
        let (req3, inf3, done3, _, err3) = snapshot(route);
        assert_eq!((req3, inf3, done3), (req0 + 3, 0, done0 + 3));
        assert_eq!(err3[9], err2[9] + 1, "a dropped handler counts as other");

        // A nested re-dispatch is not a second arrival.
        let nested_ok: Result<u32, Status> =
            timed(route, nested(Request::new(())), |_| async move { Ok(1) }).await;
        assert_eq!(nested_ok.unwrap(), 1);
        assert_eq!(snapshot(route).0, req0 + 3);
    }

    /// A streaming route reports two phases: `first_response` when the
    /// first message is polled out and `complete` at the terminal
    /// event; a stream that ends in `Err` records that code; a stream
    /// dropped unread records `other`; a refusal before the stream
    /// opens records both phases at once.
    #[tokio::test]
    async fn streaming_phases_record_first_response_and_completion() {
        let route = Route::QueryStream;
        let (req0, _, done0, first0, err0) = snapshot(route);

        let items: Vec<Result<u32, Status>> = vec![Ok(1), Ok(2)];
        let response = timed_stream(route, Request::new(()), |_| async move {
            Ok(Response::new(tokio_stream::iter(items)))
        })
        .await
        .unwrap();
        let mut stream = response.into_inner();
        assert_eq!(stream.next().await.unwrap().unwrap(), 1);
        let (_, inf, done, first, _) = snapshot(route);
        assert_eq!((inf, done, first), (1, done0, first0 + 1));
        assert_eq!(stream.next().await.unwrap().unwrap(), 2);
        assert_eq!(snapshot(route).3, first0 + 1, "first response is once");
        assert!(stream.next().await.is_none());
        let (req1, inf1, done1, first1, err1) = snapshot(route);
        assert_eq!(
            (req1, inf1, done1, first1, err1),
            (req0 + 1, 0, done0 + 1, first0 + 1, err0)
        );

        let items: Vec<Result<u32, Status>> = vec![Ok(1), Err(Status::unavailable("gone"))];
        let mut stream = timed_stream(route, Request::new(()), |_| async move {
            Ok(Response::new(tokio_stream::iter(items)))
        })
        .await
        .unwrap()
        .into_inner();
        assert_eq!(stream.next().await.unwrap().unwrap(), 1);
        assert_eq!(
            stream.next().await.unwrap().unwrap_err().code(),
            Code::Unavailable
        );
        let (_, inf2, done2, _, err2) = snapshot(route);
        assert_eq!((inf2, done2), (0, done0 + 2));
        assert_eq!(err2[4], err1[4] + 1);

        let items: Vec<Result<u32, Status>> = vec![Ok(1)];
        let stream = timed_stream(route, Request::new(()), |_| async move {
            Ok(Response::new(tokio_stream::iter(items)))
        })
        .await
        .unwrap()
        .into_inner();
        drop(stream);
        let (_, inf3, done3, first3, err3) = snapshot(route);
        assert_eq!((inf3, done3, first3), (0, done0 + 3, first0 + 3));
        assert_eq!(
            err3[9],
            err2[9] + 1,
            "a stream dropped unread counts as other"
        );

        let refused = timed_stream(route, Request::new(()), |_| async move {
            Err::<Response<tokio_stream::Iter<std::vec::IntoIter<Result<u32, Status>>>>, _>(
                Status::invalid_argument("bad"),
            )
        })
        .await;
        assert_eq!(refused.err().unwrap().code(), Code::InvalidArgument);
        let (req4, inf4, done4, first4, err4) = snapshot(route);
        assert_eq!(
            (req4, inf4, done4, first4),
            (req0 + 4, 0, done0 + 4, first0 + 4)
        );
        assert_eq!(err4[0], err3[0] + 1);
    }
}

//! The process-wide registry: histogram buckets, error rows, the counter
//! statics, and one reading of all of them.

use std::sync::atomic::{AtomicU64, Ordering};

use tonic::Code;

use super::gauges::{GaugeProvider, MemberGaugeProvider, MemberGauges, ShardGauges};
use super::routes::N_ROUTES;

/// Histogram bucket upper bounds, chosen once:
/// the `le` label text and the bound in nanoseconds. Prometheus
/// defines `le` as inclusive, and the compare below is integer, so
/// an observation of exactly 1ms lands in the 0.001 bucket.
pub(super) const BUCKETS: [(&str, u64); 14] = [
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
pub(super) const ERROR_CODES: [&str; 10] = [
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

pub(super) const N_CODES: usize = ERROR_CODES.len();

/// The error row for a status code; `None` for `Ok`, which is not an
/// error.
pub(super) fn code_index(code: Code) -> Option<usize> {
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
pub(super) struct Histogram {
    pub(super) buckets: [AtomicU64; BUCKETS.len()],
    pub(super) sum_ns: AtomicU64,
}

impl Histogram {
    pub(super) const fn new() -> Self {
        Histogram {
            buckets: [ZERO; BUCKETS.len()],
            sum_ns: AtomicU64::new(0),
        }
    }

    pub(super) fn observe(&self, ns: u64) {
        let bucket = BUCKETS
            .iter()
            .position(|&(_, le)| ns <= le)
            .expect("+Inf catches everything");
        self.buckets[bucket].fetch_add(1, Ordering::Relaxed);
        self.sum_ns.fetch_add(ns, Ordering::Relaxed);
    }

    pub(super) fn reading(&self) -> HistogramReading {
        let mut buckets = [0u64; BUCKETS.len()];
        for (slot, bucket) in buckets.iter_mut().zip(&self.buckets) {
            *slot = bucket.load(Ordering::Relaxed);
        }
        HistogramReading {
            buckets,
            sum_ns: self.sum_ns.load(Ordering::Relaxed),
        }
    }
}

/// One histogram's values at the moment of a [`read`]: per-bucket
/// counts (not cumulative) and the sum in nanoseconds.
#[derive(Clone, Copy)]
pub struct HistogramReading {
    pub(super) buckets: [u64; BUCKETS.len()],
    pub(super) sum_ns: u64,
}

/// The registry at one moment: every counter, gauge, and histogram the
/// page prints and the snapshot carries. [`super::render()`] and [`super::snapshot()`]
/// are both views of one of these, so the scraper's page and the
/// dashboard's values agree by construction when they come from the
/// same reading, and a caller that needs both takes one [`read`].
pub struct Reading {
    pub(super) requests: [u64; N_ROUTES],
    pub(super) in_flight: [u64; N_ROUTES],
    pub(super) complete: [HistogramReading; N_ROUTES],
    pub(super) first_response: [HistogramReading; N_ROUTES],
    pub(super) errors: [[u64; N_CODES]; N_ROUTES],
    pub(super) scan: [u64; SCAN_COUNTERS.len()],
    pub(super) batches: u64,
    pub(super) batched_jobs: u64,
    pub(super) shards: Vec<ShardGauges>,
    pub(super) members: Vec<MemberGauges>,
}

/// The process-wide scan and ingest counters, in page order: name,
/// help text, the atomic.
pub(super) const SCAN_COUNTERS: [(&str, &str, &AtomicU64); 10] = [
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
        "turbovec_scan_bytes_total",
        "Encoded index bytes the vector kernel streamed, one count per kernel call \
         however many queries shared it (docs/bandwidth-budget.md).",
        &SCAN_BYTES,
    ),
    (
        "turbovec_scan_active_nanoseconds_total",
        "Wall time inside vector kernel calls, in nanoseconds, counted the same way.",
        &SCAN_ACTIVE_NANOS,
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
    (
        "turbovec_dense_policy_drift_total",
        "AUTO dense queries that fell back to exhaustive execution due to live policy or hardware drift.",
        &DENSE_POLICY_DRIFT,
    ),
];

/// Read the registry once. Each value is one relaxed load; the reading
/// is not a transaction across values, but it is the one set of values
/// both [`super::render()`] and [`super::snapshot()`] see when they are given it.
pub fn read(gauges: &[GaugeProvider]) -> Reading {
    read_with_member(gauges, &[])
}

/// Read the registry once, with the member gauges sampled alongside
/// the shard gauges.
pub fn read_with_member(gauges: &[GaugeProvider], members: &[MemberGaugeProvider]) -> Reading {
    let mut requests = [0u64; N_ROUTES];
    let mut in_flight = [0u64; N_ROUTES];
    let mut errors = [[0u64; N_CODES]; N_ROUTES];
    let empty = HistogramReading {
        buckets: [0; BUCKETS.len()],
        sum_ns: 0,
    };
    let mut complete = [empty; N_ROUTES];
    let mut first_response = [empty; N_ROUTES];
    for i in 0..N_ROUTES {
        requests[i] = REQUESTS[i].load(Ordering::Relaxed);
        in_flight[i] = IN_FLIGHT[i].load(Ordering::Relaxed);
        complete[i] = COMPLETE[i].reading();
        first_response[i] = FIRST_RESPONSE[i].reading();
        for (c, slot) in errors[i].iter_mut().enumerate() {
            *slot = ERRORS[i][c].load(Ordering::Relaxed);
        }
    }
    let mut scan = [0u64; SCAN_COUNTERS.len()];
    for (slot, (_, _, atomic)) in scan.iter_mut().zip(SCAN_COUNTERS.iter()) {
        *slot = atomic.load(Ordering::Relaxed);
    }
    let (batches, batched_jobs) = crate::node::scan_batch_counters();
    Reading {
        requests,
        in_flight,
        complete,
        first_response,
        errors,
        scan,
        batches,
        batched_jobs,
        shards: gauges.iter().map(|g| g()).collect(),
        members: members.iter().map(|g| g()).collect(),
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

pub(super) static REQUESTS: [AtomicU64; N_ROUTES] = [ZERO; N_ROUTES];
pub(super) static IN_FLIGHT: [AtomicU64; N_ROUTES] = [ZERO; N_ROUTES];
/// Arrival to the handler's terminal outcome (unary) or the response
/// stream's terminal event (streaming): the `complete` phase.
pub(super) static COMPLETE: [Histogram; N_ROUTES] = [EMPTY; N_ROUTES];
/// Arrival to the first message handed to the transport; observed only
/// on streaming routes.
pub(super) static FIRST_RESPONSE: [Histogram; N_ROUTES] = [EMPTY; N_ROUTES];
pub(super) static ERRORS: [[AtomicU64; N_CODES]; N_ROUTES] = [ZERO_ROW; N_ROUTES];

static SCAN_CHUNK_CALLS: AtomicU64 = AtomicU64::new(0);
static SCAN_CANDIDATES: AtomicU64 = AtomicU64::new(0);
static SCAN_FLOORS_OFFERED: AtomicU64 = AtomicU64::new(0);
static SCAN_FLOORS_PUBLISHED: AtomicU64 = AtomicU64::new(0);
static SCAN_FLOOR_UPDATES_APPLIED: AtomicU64 = AtomicU64::new(0);
/// Encoded index bytes the vector kernel streamed, once per kernel call
/// however many queries shared it.
static SCAN_BYTES: AtomicU64 = AtomicU64::new(0);
/// Wall time inside those kernel calls, in nanoseconds, the same way.
static SCAN_ACTIVE_NANOS: AtomicU64 = AtomicU64::new(0);
static DOCUMENTS_ADDED: AtomicU64 = AtomicU64::new(0);
static VECTORS_ADDED: AtomicU64 = AtomicU64::new(0);
static DENSE_POLICY_DRIFT: AtomicU64 = AtomicU64::new(0);

/// Count one AUTO dense query falling back to exhaustive execution due to
/// policy identity or hardware drift.
pub fn record_dense_policy_drift() {
    DENSE_POLICY_DRIFT.fetch_add(1, Ordering::Relaxed);
}

/// Process-wide count of AUTO dense policy drift fallbacks.
pub fn dense_policy_drift_total() -> u64 {
    DENSE_POLICY_DRIFT.load(Ordering::Relaxed)
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
    // Bytes and active time are NOT taken from per-query stats: a batched
    // query carries the pass it shared, and the pass is counted once by
    // `record_scan_pass`.
}

/// Count one kernel pass: the encoded bytes it streamed and its wall
/// time. Called once per kernel call whatever the batch size, so a shared
/// pass is one count.
pub fn record_scan_pass(bytes: u64, nanos: u64) {
    SCAN_BYTES.fetch_add(bytes, Ordering::Relaxed);
    SCAN_ACTIVE_NANOS.fetch_add(nanos, Ordering::Relaxed);
}

/// The process-wide scan byte and active-time totals, for tests and the
/// node's own accounting.
pub fn scan_pass_totals() -> (u64, u64) {
    (
        SCAN_BYTES.load(Ordering::Relaxed),
        SCAN_ACTIVE_NANOS.load(Ordering::Relaxed),
    )
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

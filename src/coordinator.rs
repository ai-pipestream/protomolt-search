//! Coordinator side: client-facing [`SearchService`] that fans queries out
//! to shard nodes, aggregates their floors mid-scan, and merges results.

use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status, Streaming};

use crate::bm25::{Bm25Params, CorpusStats};
use crate::clustered_turbovec::{
    ClusteredCandidateEvent, ClusteredLabelFilter, ClusteredTurboVecBackend,
};
use crate::fusion::{self, Leg};
use crate::merge::{cmp_hits, merge_topk, FloorTracker, MergedHit};
use crate::pb::node_service_client::NodeServiceClient;
use crate::pb::search_service_server::{SearchService, SearchServiceServer};
use crate::pb::{
    bm25_query_stream_request, bm25_query_stream_response, Bm25QueryResponse,
    Bm25QueryStreamRequest, Bm25QueryStreamResponse,
};
use crate::pb::{
    search_shard_request, search_shard_response, AbortTopologyCutoverRequest,
    AbortTopologyCutoverResponse, Bm25Hit, Bm25QueryRequest, Bm25RescoreRequest, Bm25SearchRequest,
    Bm25SearchResponse, BroadcastCalibrationRequest, BroadcastCalibrationResponse,
    BroadcastVectorBackendRequest, BroadcastVectorBackendResponse, CalibrationApplyResult,
    CascadeHit, ClusterHealthRequest, ClusterHealthResponse, ClusteredVectorHealth,
    ConfigureVectorBackendRequest, ExactVectorRescoreRequest, FloorUpdate,
    FreezeTopologyWritesRequest, FreezeTopologyWritesResponse, FusionMode, HealthRequest,
    HybridDebug, HybridHit, HybridLegHit, HybridSearchRequest, HybridSearchResponse,
    HybridShardDebug, HybridShardRequest, ParentGroup, PublishTopologyRequest,
    PublishTopologyResponse, RoutedIngestMappedRequest, RoutedIngestMappedResponse,
    RoutedShardIngest, ScoredHit, SearchRequest, SearchResponse, SearchShardDone,
    SearchShardRequest, SearchShardResponse, SetCalibrationRequest, ShardHealth, ShardLegsRequest,
    ShardScanStats, StartShardSearch, StartStreamSearch, StopStreamSearch, StreamSearchRequest,
    StreamSearchResponse, StreamSearchSummary, TermStatsRequest, VectorBackendApplyResult,
    VectorRescoreRequest,
};
use crate::pb::{
    search_variant, InterleaveTeam, Interleaving, RankedHit, RankingDiff, VariantResult,
    VariantSearchRequest, VariantSearchResponse,
};
use crate::pb::{stream_search_request, stream_search_response};
use crate::rankdiff;

#[derive(Clone, Debug, PartialEq)]
struct QueryProgress {
    phase: crate::pb::QueryStreamPhase,
    hits: Vec<(u64, f32)>,
    scoring_fingerprint: String,
}

fn query_stream_content_fingerprint(
    phase: crate::pb::QueryStreamPhase,
    hits: &[(u64, f32)],
) -> String {
    let mut bytes = Vec::with_capacity(32 + hits.len() * 12);
    bytes.extend_from_slice(b"protomolt-query-revision-v1\0");
    bytes.extend_from_slice(&(phase as i32).to_le_bytes());
    for &(id, score) in hits {
        bytes.extend_from_slice(&id.to_le_bytes());
        bytes.extend_from_slice(&score.to_bits().to_le_bytes());
    }
    crate::sha256::hex_digest(&bytes)
}

fn query_stream_revision(
    revision: u64,
    phase: crate::pb::QueryStreamPhase,
    hits: Vec<(u64, f32)>,
    scoring_fingerprint: String,
) -> crate::pb::QueryStreamRevision {
    let content_fingerprint = query_stream_content_fingerprint(phase, &hits);
    crate::pb::QueryStreamRevision {
        revision,
        phase: phase as i32,
        hits: hits
            .into_iter()
            .enumerate()
            .map(|(rank, (doc_id, score))| crate::pb::QueryStreamHit {
                doc_id,
                score,
                rank: rank as u32 + 1,
            })
            .collect(),
        content_fingerprint,
        scoring_fingerprint,
    }
}

fn combined_scoring_fingerprint(fingerprints: &[String], request_fallback: &str) -> String {
    if fingerprints.is_empty() {
        return request_fallback.to_string();
    }
    let mut canonical = fingerprints.to_vec();
    canonical.sort();
    canonical.dedup();
    crate::sha256::hex_digest(canonical.join("\0").as_bytes())
}

/// Process-unique request id counter for coordinator-assigned ids.
static REQUEST_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Default hard cap on any client-facing `k` (`--max-k` overrides).
/// Bounds the coordinator's heap and keeps the shared floor rising: an
/// unbounded k would hold the floor at -inf and stream every shard dry.
pub const DEFAULT_MAX_K: u32 = 10_000;
pub const DEFAULT_MAX_RERANK_BYTES: u64 = 256 * 1024 * 1024;

/// Per-shard timing controls for the fan-out (all off by default).
#[derive(Debug, Clone, Copy, Default)]
pub struct FanoutLimits {
    /// Hard bound on one shard's whole attempt (primary + any hedge).
    /// A shard that blows the deadline fails the query with
    /// DEADLINE_EXCEEDED rather than stalling it forever.
    pub shard_deadline: Option<Duration>,
    /// How long to wait on the primary before opening the identical
    /// search on the shard's replica and racing the two. Only acts on
    /// shards that have a replica configured.
    pub hedge_delay: Option<Duration>,
}

/// One immutable product-shard route in a topology generation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopologyRoute {
    pub addr: String,
    pub replica: Option<String>,
    /// Inclusive stable-key hash range. Every route must either provide a
    /// range or omit one; mixed/ragged maps are refused.
    pub hash_range: Option<(u64, u64)>,
}

struct CoordinatorTopology {
    generation: u64,
    routes: Vec<TopologyRoute>,
    stats_cache: Arc<crate::stats_cache::StatsCache>,
}

fn build_topology(
    generation: u64,
    routes: Vec<TopologyRoute>,
) -> Result<CoordinatorTopology, String> {
    if routes.is_empty() {
        return Err("topology requires at least one primary shard".to_string());
    }
    let mut addresses = std::collections::HashSet::new();
    for (shard, route) in routes.iter().enumerate() {
        if route.addr.is_empty() {
            return Err(format!(
                "topology shard {shard} has an empty primary address"
            ));
        }
        if !addresses.insert(route.addr.as_str()) {
            return Err(format!("duplicate topology endpoint {:?}", route.addr));
        }
        if let Some(replica) = route.replica.as_deref() {
            if replica.is_empty() {
                return Err(format!(
                    "topology shard {shard} has an empty replica address"
                ));
            }
            if !addresses.insert(replica) {
                return Err(format!("duplicate topology endpoint {replica:?}"));
            }
        }
        if let Some((lo, hi)) = route.hash_range {
            if lo > hi {
                return Err(format!(
                    "topology shard {shard} has inverted hash range {lo}..={hi}"
                ));
            }
        }
    }
    let ranged = routes
        .iter()
        .filter(|route| route.hash_range.is_some())
        .count();
    if ranged != 0 && ranged != routes.len() {
        return Err("topology must provide hash ranges for every shard or none".to_string());
    }
    if ranged != 0 {
        let mut ranges: Vec<(u64, u64, usize)> = routes
            .iter()
            .enumerate()
            .map(|(shard, route)| {
                let (lo, hi) = route.hash_range.expect("all routes ranged");
                (lo, hi, shard)
            })
            .collect();
        ranges.sort_by_key(|range| range.0);
        let mut expected = 0u64;
        for (position, (lo, hi, shard)) in ranges.iter().copied().enumerate() {
            if lo != expected {
                return Err(format!(
                    "topology hash space has a gap or overlap before shard {shard}: expected {expected}, got {lo}"
                ));
            }
            if position + 1 == ranges.len() {
                if hi != u64::MAX {
                    return Err(format!(
                        "topology hash space ends at {hi}, not {}",
                        u64::MAX
                    ));
                }
            } else {
                expected = hi.checked_add(1).ok_or_else(|| {
                    format!("topology shard {shard} reaches the hash-space end too early")
                })?;
            }
        }
    }
    Ok(CoordinatorTopology {
        generation,
        stats_cache: Arc::new(crate::stats_cache::StatsCache::new(routes.len())),
        routes,
    })
}

/// Stable FNV-1a over opaque product identity bytes. Unlike the historical
/// WAL bucket hash, this never depends on generation-local numeric slots.
pub fn stable_routing_hash(key: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in key {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

#[derive(Clone, Copy, Debug)]
struct ProductLabelRange {
    start: u64,
    end: u64,
    shard: u32,
}

impl ProductLabelRange {
    fn contains(self, label: u64) -> bool {
        label >= self.start && label < self.end
    }
}

struct ClusteredVectorResult {
    hits: Vec<(u64, f32)>,
}

pub(crate) struct ExactRerankScores {
    pub scores: HashMap<u64, f32>,
    pub rows: u64,
    pub logical_bytes: u64,
    pub pages_touched: u64,
    pub tasks: u32,
}

type CutoverLease = (u64, tokio::sync::OwnedRwLockWriteGuard<()>);

/// The coordinator gRPC service.
///
/// Every request snapshots one immutable topology generation. A coordinator
/// may keep its construction-time map or publish newer hot generations.
/// Connections are pooled: one lazily-established HTTP/2 channel per node
/// address, multiplexing every concurrent query and reconnecting on its own
/// after a node restart.
#[derive(Clone)]
pub struct CoordinatorServiceImpl {
    /// Node addresses in `http://host:port` form, in stable shard order
    /// (index in this list is the shard index used for tie-breaking).
    node_addrs: Vec<String>,
    /// Optional replica address per shard (same data, exact same
    /// results), the target for hedged retries.
    replica_addrs: Vec<Option<String>>,
    /// Lexical analysis backend for query analysis in Bm25Search.
    analysis_addr: Option<String>,
    /// BM25 tuning sent to every shard (identical scoring everywhere).
    bm25_params: Bm25Params,
    /// Per-shard deadline and hedging controls.
    limits: FanoutLimits,
    /// Serve `SearchService.Search` over the streaming protocol
    /// (`fanout_stream_search`) instead of the per-shard top-k fan-out.
    stream_search: bool,
    /// Run `Bm25Search` over the exact `Bm25QueryStream` candidate
    /// protocol instead of the unary per-shard top-k round. The
    /// coordinator owns the only authoritative global heap, relays its
    /// emission-safe floor, and accepts a result only after every shard
    /// supplies a matching score-space fingerprint and completion
    /// certificate (docs/block-max.md).
    bm25_stream: bool,
    /// Hard upper bound on any client-facing `k`. A request above it is
    /// refused (never clamped), and a request that omits `k` (proto3 0)
    /// runs at exactly this depth. This bounds the coordinator's heap; it is
    /// not a node quota or scan-completion signal.
    max_k: u32,
    /// Hard request-wide logical FP32 row-byte bound for reranking.
    max_rerank_bytes: u64,
    /// One reusable channel per address, created on first use.
    channels: Arc<Mutex<HashMap<String, Channel>>>,
    /// Whether a channel cache miss may create a network connection. The
    /// embedded runtime preloads every shard channel and keeps this false,
    /// making a missing channel a hard error instead of a possible egress.
    allow_network: bool,
    /// Lazily bound UDP socket for the typed stream-signal fast lane (`None`
    /// when the bind failed; signals then ride the gRPC streams alone).
    floor_socket: Arc<std::sync::OnceLock<Option<Arc<std::net::UdpSocket>>>>,
    /// Resolved UDP floor target per node address (`None` =
    /// unresolvable), cached on first use. IPv4 preferred.
    floor_targets: Arc<Mutex<HashMap<String, Option<std::net::SocketAddr>>>>,
    /// Per-node BM25 term-stat shares, keyed by each node's
    /// `stats_epoch` (src/stats_cache.rs). Sound because the nodes
    /// enforce the epoch claim on every scoring request built from it.
    stats_cache: Arc<crate::stats_cache::StatsCache>,
    /// Optional distributed vector collection. The product coordinator calls
    /// it once as one provider; it never learns or re-fans its shard topology.
    clustered_vectors: Option<ClusteredTurboVecBackend>,
    /// Optional measured candidate-depth contract for FP32 reranking.
    dense_quality_profile: Option<Arc<crate::quality::DenseQualityProfile>>,
    /// Product shard-map generation (zero for the implicit static list).
    topology_generation: u64,
    /// Inclusive stable-key ranges parallel to `node_addrs`. Empty for the
    /// legacy explicitly addressed topology.
    hash_ranges: Vec<Option<(u64, u64)>>,
    /// Hot topology authority. Public RPC entry points snapshot this once and
    /// recurse into a frozen clone with this field cleared, so no request can
    /// observe half of two generations.
    live_topology: Option<Arc<RwLock<Arc<CoordinatorTopology>>>>,
    /// Routed writes take a read guard. A cutover holds the owned write guard
    /// while the final WAL tail is verified and the new map is published;
    /// queries do not use this gate.
    write_gate: Arc<tokio::sync::RwLock<()>>,
    cutover_guard: Arc<Mutex<Option<CutoverLease>>>,
    cutover_pending: Arc<std::sync::atomic::AtomicBool>,
    /// Product-owned phrase vocabulary used to derive canonical query terms.
    phrase_index: Option<Arc<crate::phrases::PhraseIndex>>,
    /// Request-local observer installed only by public QueryStream. A watch
    /// cell intentionally conflates intermediate snapshots: every revision
    /// is a full replacement, so a slow client only needs the newest one.
    query_progress: Option<watch::Sender<Option<QueryProgress>>>,
}

/// A process-unique, well-mixed stream token for the UDP signal lane
/// (0 is reserved for "no UDP"). Tokens route datagrams to in-flight
/// streams on a trusted network; they are unique, not secret.
fn floor_token() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let raw = (u64::from(std::process::id()) << 32) ^ NEXT.fetch_add(1, AtomicOrdering::Relaxed);
    // splitmix64 finalizer.
    let mut z = raw.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^= z >> 31;
    z.max(1)
}

/// Whether a node refused a scoring request because its stats epoch
/// moved past the request's claim. The retry contract: invalidate the
/// cache, refetch fresh, and repeat the round ONCE with no claim
/// (`expected_stats_epoch = 0`) — the pre-cache semantics, which cannot
/// be refused, so a shard under continuous ingest degrades to exactly
/// the behavior it had before the cache existed instead of livelocking.
fn is_stale_stats(status: &Status) -> bool {
    status.code() == tonic::Code::FailedPrecondition
        && status.message().starts_with(crate::node::STALE_STATS_EPOCH)
}

struct Bm25StreamHeap {
    heap: std::collections::BinaryHeap<StreamHeapEntry>,
    floors_sent: u64,
    progress: Option<watch::Sender<Option<QueryProgress>>>,
}

struct Bm25StreamShardResult {
    response: Bm25QueryResponse,
    scoring_fingerprint: String,
}

/// One shard's leg of the exact lexical candidate stream. Compact
/// candidates feed the coordinator-owned global heap immediately; its
/// emission-safe k-th score shares the same conflated watch cell as local
/// shard floors. The terminal completion supplies rich local top-k details
/// for the global winners and certifies that the whole shard was scanned.
async fn stream_bm25_shard(
    shard: u32,
    k: usize,
    mut client: NodeServiceClient<Channel>,
    request: Bm25QueryRequest,
    floor_tx: Arc<watch::Sender<f32>>,
    mut floor_rx: watch::Receiver<f32>,
    global_heap: Arc<Mutex<Bm25StreamHeap>>,
) -> Result<Bm25StreamShardResult, Status> {
    let (out_tx, out_rx) = mpsc::channel::<Bm25QueryStreamRequest>(8);
    out_tx
        .send(Bm25QueryStreamRequest {
            payload: Some(bm25_query_stream_request::Payload::Start(request)),
        })
        .await
        .map_err(|_| Status::internal("bm25 stream request channel closed before start"))?;
    let mut inbound = client
        .bm25_query_stream(ReceiverStream::new(out_rx))
        .await?
        .into_inner();
    let control_tx = out_tx.clone();
    // Forward only raises above what this stream last saw. Conflation
    // is the watch cell itself: a burst of raises collapses to whatever
    // is newest when the forwarder wakes, and a dropped intermediate
    // value loses nothing because floors are monotone.
    let forwarder = tokio::spawn(async move {
        let mut last = *floor_rx.borrow_and_update();
        while floor_rx.changed().await.is_ok() {
            let floor = *floor_rx.borrow_and_update();
            if floor > last {
                last = floor;
                let update = Bm25QueryStreamRequest {
                    payload: Some(bm25_query_stream_request::Payload::FloorUpdate(
                        FloorUpdate { floor },
                    )),
                };
                if out_tx.send(update).await.is_err() {
                    break;
                }
            }
        }
    });
    let mut received = 0u64;
    let result = loop {
        match inbound.message().await {
            Ok(Some(Bm25QueryStreamResponse {
                payload: Some(bm25_query_stream_response::Payload::FloorUpdate(u)),
            })) => {
                floor_tx.send_if_modified(|cur| {
                    if !u.floor.is_nan() && u.floor > *cur {
                        *cur = u.floor;
                        true
                    } else {
                        false
                    }
                });
            }
            Ok(Some(Bm25QueryStreamResponse {
                payload: Some(bm25_query_stream_response::Payload::CandidateBatch(batch)),
            })) => {
                if batch.candidates.len() % 12 != 0 {
                    break Err(Status::data_loss(format!(
                        "shard {shard}: BM25 candidate batch has {} bytes, not 12-byte records",
                        batch.candidates.len()
                    )));
                }
                if batch.candidates.as_chunks::<12>().0.iter().any(|rec| {
                    !f32::from_le_bytes(rec[8..12].try_into().expect("4-byte score")).is_finite()
                }) {
                    break Err(Status::data_loss(format!(
                        "shard {shard}: BM25 candidate batch contains a non-finite score"
                    )));
                }
                received += (batch.candidates.len() / 12) as u64;
                let mut state = global_heap.lock().expect("BM25 stream heap poisoned");
                let mut raised = None;
                for rec in batch.candidates.as_chunks::<12>().0 {
                    if k == 0 {
                        continue;
                    }
                    let entry = StreamHeapEntry(MergedHit {
                        vector_id: u64::from_le_bytes(rec[..8].try_into().expect("8-byte id")),
                        shard,
                        score: f32::from_le_bytes(rec[8..12].try_into().expect("4-byte score")),
                    });
                    if state.heap.len() < k {
                        state.heap.push(entry);
                    } else if cmp_hits(&entry.0, &state.heap.peek().expect("heap is full").0)
                        == std::cmp::Ordering::Less
                    {
                        state.heap.pop();
                        state.heap.push(entry);
                    }
                }
                if state.heap.len() == k {
                    let floor =
                        crate::bm25::floor_seed(state.heap.peek().expect("heap is full").0.score);
                    if floor > *floor_tx.borrow() {
                        state.floors_sent += 1;
                        raised = Some(floor);
                    }
                }
                let progress = state.progress.clone();
                let mut snapshot: Vec<(u64, f32)> = state
                    .heap
                    .iter()
                    .map(|entry| (entry.0.vector_id, entry.0.score))
                    .collect();
                snapshot.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
                drop(state);
                if let Some(progress) = progress {
                    let next = QueryProgress {
                        phase: crate::pb::QueryStreamPhase::Lexical,
                        hits: snapshot,
                        scoring_fingerprint: String::new(),
                    };
                    progress.send_if_modified(|current| {
                        if current.as_ref() == Some(&next) {
                            false
                        } else {
                            *current = Some(next);
                            true
                        }
                    });
                }
                if let Some(floor) = raised {
                    floor_tx.send_if_modified(|cur| {
                        if floor > *cur {
                            *cur = floor;
                            true
                        } else {
                            false
                        }
                    });
                }
            }
            Ok(Some(Bm25QueryStreamResponse {
                payload: Some(bm25_query_stream_response::Payload::Completion(completion)),
            })) => {
                if !completion.completed {
                    break Err(Status::cancelled(format!(
                        "shard {shard}: BM25 scan did not complete"
                    )));
                }
                if completion.scoring_fingerprint.is_empty() {
                    break Err(Status::data_loss(format!(
                        "shard {shard}: BM25 completion omitted scoring fingerprint"
                    )));
                }
                if completion.candidates_emitted != received {
                    break Err(Status::data_loss(format!(
                        "shard {shard}: completion counted {} candidates but {} arrived",
                        completion.candidates_emitted, received
                    )));
                }
                let Some(response) = completion.response else {
                    break Err(Status::data_loss(format!(
                        "shard {shard}: BM25 completion omitted its response"
                    )));
                };
                break Ok(Bm25StreamShardResult {
                    response,
                    scoring_fingerprint: completion.scoring_fingerprint,
                });
            }
            Ok(Some(Bm25QueryStreamResponse {
                payload: Some(bm25_query_stream_response::Payload::Done(_)),
            })) => {
                break Err(Status::failed_precondition(format!(
                    "shard {shard}: BM25 stream used the obsolete uncertified terminal response"
                )));
            }
            // Empty payload: ignore (forward compatibility).
            Ok(Some(_)) => {}
            Ok(None) => {
                break Err(Status::data_loss(format!(
                    "shard {shard}: BM25 stream ended without a completion certificate"
                )))
            }
            Err(e) => break Err(e),
        }
    };
    if result.is_err() {
        let _ = control_tx
            .send(Bm25QueryStreamRequest {
                payload: Some(bm25_query_stream_request::Payload::Stop(
                    crate::pb::StopBm25Query {},
                )),
            })
            .await;
    }
    forwarder.abort();
    result
}

/// What a faceted BM25 fan-out returns: the global top-k plus the
/// merged counts of each facet kind that was asked for (empty lists for
/// the kinds that were not). Named because three of the fan-out
/// entry points share the shape and a bare triple reads as noise at
/// every call site.
type FacetedHits = (
    Vec<Bm25Hit>,
    Vec<crate::pb::FacetFieldCounts>,
    Vec<crate::pb::RangeFacetCounts>,
);

/// [`FacetedHits`] plus the column aggregations (docs/facets.md):
/// merged match-set stats and exact distinct-value counts.
type AggregatedHits = (
    Vec<Bm25Hit>,
    Vec<crate::pb::FacetFieldCounts>,
    Vec<crate::pb::RangeFacetCounts>,
    Vec<crate::pb::ColumnStats>,
    Vec<crate::pb::FacetCardinality>,
);

/// Merge per-shard column stats: counts and sums add, mins and maxes
/// fold — additive across shards exactly as facet counts are, so the
/// coordinator merge is the same positional walk. `mean` is computed
/// HERE (sum / count) so clients cannot get it wrong; a column NO
/// shard knows is refused by name, the usual typo rule.
/// The evaluated values for one candidate set (`fetch_values`).
pub struct FetchedValues {
    /// doc -> projected values, aligned with the request projections.
    pub rows: HashMap<u64, Vec<crate::pb::ProjectedValue>>,
    /// Per stage: doc -> identity-score contribution. A doc absent
    /// from a map has no value for that stage's column.
    pub stage_rows: Vec<HashMap<u64, f64>>,
}

/// Compile the public projection list (docs/cel-values.md): names
/// non-empty and request-unique, expressions through the value
/// front-end, ONCE, here at the coordinator.
pub(crate) fn compile_projections(
    projections: &[crate::pb::NamedProjection],
) -> Result<Vec<crate::pb::CompiledProjection>, Status> {
    let mut names = std::collections::HashSet::new();
    let mut compiled = Vec::with_capacity(projections.len());
    for p in projections {
        if p.name.is_empty() {
            return Err(Status::invalid_argument(
                "projection: a projection needs a non-empty name",
            ));
        }
        if !names.insert(p.name.as_str()) {
            return Err(Status::invalid_argument(format!(
                "projection: duplicate projection name {:?}",
                p.name
            )));
        }
        let expr = crate::cel::compile_value(&p.expression).map_err(|e| {
            Status::invalid_argument(format!("projection {:?}: {}", p.name, e.message()))
        })?;
        compiled.push(crate::pb::CompiledProjection {
            name: p.name.clone(),
            expr: Some(expr),
        });
    }
    Ok(compiled)
}

/// The compiled aggregate request: everything the fan-out sends and
/// the merge needs.
pub(crate) struct CompiledAggregate {
    aggregations: Vec<crate::pb::CompiledAggregation>,
    histograms: Vec<crate::pb::CompiledHistogram>,
    percentiles: Vec<crate::pb::CompiledPercentile>,
    percentile_specs: Vec<crate::pb::PercentileSpec>,
    group_by: String,
    max_groups: u32,
}

/// Compile the public aggregation list: names checked, ops decoded,
/// expressions compiled once into the ValueExpr IR the shards resolve.
pub(crate) fn compile_aggregations(
    req: &crate::pb::AggregateRequest,
) -> Result<CompiledAggregate, Status> {
    let (aggregations, histograms, percentiles) =
        (&req.aggregations, &req.histograms, &req.percentiles);
    if aggregations.is_empty() && histograms.is_empty() && percentiles.is_empty() {
        return Err(Status::invalid_argument(
            "aggregate requires at least one aggregation, histogram, or percentile",
        ));
    }
    if percentiles.len() > 8 {
        return Err(Status::invalid_argument(format!(
            "aggregate takes at most 8 percentile specs per request, got {}",
            percentiles.len()
        )));
    }
    if aggregations.len() > 32 {
        return Err(Status::invalid_argument(format!(
            "aggregate takes at most 32 aggregations per request, got {}",
            aggregations.len()
        )));
    }
    if histograms.len() > 8 {
        return Err(Status::invalid_argument(format!(
            "aggregate takes at most 8 histograms per request, got {}",
            histograms.len()
        )));
    }
    // One name namespace across aggregations and histograms.
    let mut names = std::collections::HashSet::new();
    let mut compiled = Vec::with_capacity(aggregations.len());
    for a in aggregations {
        if a.name.is_empty() {
            return Err(Status::invalid_argument(
                "aggregation: an aggregation needs a non-empty name",
            ));
        }
        if !names.insert(a.name.as_str()) {
            return Err(Status::invalid_argument(format!(
                "aggregation: duplicate aggregation name {:?}",
                a.name
            )));
        }
        crate::node::agg_op_of(a.op).map_err(|e| {
            Status::invalid_argument(format!("aggregation {:?}: {}", a.name, e.message()))
        })?;
        let expr = crate::cel::compile_value(&a.expression).map_err(|e| {
            Status::invalid_argument(format!("aggregation {:?}: {}", a.name, e.message()))
        })?;
        compiled.push(crate::pb::CompiledAggregation {
            expr: Some(expr),
            op: a.op,
            name: a.name.clone(),
        });
    }
    let mut compiled_hists = Vec::with_capacity(histograms.len());
    for h in histograms {
        if h.name.is_empty() {
            return Err(Status::invalid_argument(
                "histogram: a histogram needs a non-empty name",
            ));
        }
        if !names.insert(h.name.as_str()) {
            return Err(Status::invalid_argument(format!(
                "aggregation: duplicate aggregation name {:?}",
                h.name
            )));
        }
        if !(h.interval > 0.0 && h.interval.is_finite()) {
            return Err(Status::invalid_argument(format!(
                "histogram {:?}: the interval must be positive and finite, got {}",
                h.name, h.interval
            )));
        }
        let expr = crate::cel::compile_value(&h.expression).map_err(|e| {
            Status::invalid_argument(format!("histogram {:?}: {}", h.name, e.message()))
        })?;
        compiled_hists.push(crate::pb::CompiledHistogram {
            expr: Some(expr),
            interval: h.interval,
            max_buckets: if h.max_buckets == 0 {
                1024
            } else {
                h.max_buckets
            },
            name: h.name.clone(),
        });
    }
    let mut compiled_pcts = Vec::with_capacity(percentiles.len());
    for spec in percentiles {
        if spec.name.is_empty() {
            return Err(Status::invalid_argument(
                "percentile: a percentile spec needs a non-empty name",
            ));
        }
        if !names.insert(spec.name.as_str()) {
            return Err(Status::invalid_argument(format!(
                "aggregation: duplicate aggregation name {:?}",
                spec.name
            )));
        }
        if spec.percentiles.is_empty() || spec.percentiles.len() > 16 {
            return Err(Status::invalid_argument(format!(
                "percentile {:?}: 1 to 16 percentile values per spec, got {}",
                spec.name,
                spec.percentiles.len()
            )));
        }
        for &p in &spec.percentiles {
            if !(p.is_finite() && (0.0..=100.0).contains(&p)) {
                return Err(Status::invalid_argument(format!(
                    "percentile {:?}: {p} is not a percentile; values are finite in \
                     [0, 100]",
                    spec.name
                )));
            }
        }
        let expr = crate::cel::compile_value(&spec.expression).map_err(|e| {
            Status::invalid_argument(format!("percentile {:?}: {}", spec.name, e.message()))
        })?;
        compiled_pcts.push(crate::pb::CompiledPercentile {
            expr: Some(expr),
            name: spec.name.clone(),
        });
    }
    Ok(CompiledAggregate {
        aggregations: compiled,
        histograms: compiled_hists,
        percentiles: compiled_pcts,
        percentile_specs: req.percentiles.clone(),
        group_by: req.group_by.clone(),
        max_groups: if req.max_groups == 0 {
            1000
        } else {
            req.max_groups
        },
    })
}

/// One percentile expression's merged phase-1 statistics.
struct PctMerge {
    vt: Option<crate::pb::AggregateValueType>,
    present: u64,
    unrankable: u64,
    min_bits: u64,
    max_bits: u64,
}

impl PctMerge {
    fn new() -> Self {
        Self {
            vt: None,
            present: 0,
            unrankable: 0,
            min_bits: 0,
            max_bits: 0,
        }
    }

    fn fold(&mut self, p: &crate::pb::PercentilePartial, name: &str) -> Result<(), Status> {
        use crate::pb::AggregateValueType as T;
        let vt = match T::try_from(p.vtype) {
            Ok(T::Absent) => return Ok(()),
            Ok(T::Int) => T::Int,
            Ok(T::Double) => T::Double,
            _ => {
                return Err(Status::internal(
                    "shard answered a percentile partial without a numeric type",
                ));
            }
        };
        match self.vt {
            None => self.vt = Some(vt),
            Some(prev) if prev != vt => {
                return Err(Status::failed_precondition(format!(
                    "percentile {name:?}: shards disagree on the expression's type \
                     ({} against {}); the column families diverge across shards",
                    agg_vt_name(prev),
                    agg_vt_name(vt)
                )));
            }
            Some(_) => {}
        }
        self.unrankable += p.unrankable;
        if p.present == 0 {
            return Ok(());
        }
        if self.present == 0 {
            self.min_bits = p.min_bits;
            self.max_bits = p.max_bits;
        } else {
            self.min_bits = self.min_bits.min(p.min_bits);
            self.max_bits = self.max_bits.max(p.max_bits);
        }
        self.present += p.present;
        Ok(())
    }
}

/// One aggregation's merged fleet-wide statistics: a type vote plus
/// every fold, gated per type. Extrema and moments fold only over
/// shards that HELD values; the type vote counts on any shard whose
/// columns resolve, so cross-shard type disagreement stays loud even
/// when one side is empty.
struct AggMerge {
    vt: Option<crate::pb::AggregateValueType>,
    present: u64,
    int_sum: i128,
    int_min: i64,
    int_max: i64,
    dsum: f64,
    dcomp: f64,
    dmin: f64,
    dmax: f64,
    mean: f64,
    m2: f64,
}

impl AggMerge {
    fn new() -> Self {
        Self {
            vt: None,
            present: 0,
            int_sum: 0,
            int_min: 0,
            int_max: 0,
            dsum: 0.0,
            dcomp: 0.0,
            dmin: 0.0,
            dmax: 0.0,
            mean: 0.0,
            m2: 0.0,
        }
    }

    /// Neumaier step: fold one addend into (sum, compensation).
    fn neumaier(&mut self, x: f64) {
        let t = self.dsum + x;
        self.dcomp += if self.dsum.abs() >= x.abs() {
            (self.dsum - t) + x
        } else {
            (x - t) + self.dsum
        };
        self.dsum = t;
    }

    /// Fold one shard's partial in. Shard order is the caller's
    /// contract; every fold here is deterministic given that order.
    fn fold(&mut self, p: &crate::pb::AggregatePartial, name: &str) -> Result<(), Status> {
        use crate::pb::AggregateValueType as T;
        let vt = match T::try_from(p.vtype) {
            Ok(T::Absent) => return Ok(()),
            Ok(T::Unspecified) | Err(_) => {
                return Err(Status::internal(
                    "shard answered an aggregation partial without a type",
                ));
            }
            Ok(vt) => vt,
        };
        match self.vt {
            None => self.vt = Some(vt),
            Some(prev) if prev != vt => {
                return Err(Status::failed_precondition(format!(
                    "aggregation {name:?}: shards disagree on the expression's type \
                     ({} against {}); the column families diverge across shards",
                    agg_vt_name(prev),
                    agg_vt_name(vt)
                )));
            }
            Some(_) => {}
        }
        if p.present == 0 {
            return Ok(());
        }
        match vt {
            T::Int => {
                self.int_sum += (i128::from(p.int_sum_hi) << 64) | i128::from(p.int_sum_lo);
                if self.present == 0 {
                    self.int_min = p.int_min;
                    self.int_max = p.int_max;
                } else {
                    self.int_min = self.int_min.min(p.int_min);
                    self.int_max = self.int_max.max(p.int_max);
                }
            }
            T::Double => {
                // The shard's compensated sum folds as its two exact
                // halves, keeping the coordinator's own compensation.
                self.neumaier(p.double_sum);
                self.neumaier(p.double_compensation);
                if self.present == 0 {
                    self.dmin = p.double_min;
                    self.dmax = p.double_max;
                } else if p.double_min.is_nan() || self.dmin.is_nan() {
                    self.dmin = f64::NAN;
                    self.dmax = f64::NAN;
                } else {
                    self.dmin = self.dmin.min(p.double_min);
                    self.dmax = self.dmax.max(p.double_max);
                }
                // Chan's parallel Welford merge.
                let (na, nb) = (self.present, p.present);
                if na == 0 {
                    self.mean = p.mean;
                    self.m2 = p.m2;
                } else {
                    let n = (na + nb) as f64;
                    let delta = p.mean - self.mean;
                    self.mean += delta * (nb as f64 / n);
                    self.m2 += p.m2 + delta * delta * (na as f64 * nb as f64 / n);
                }
            }
            T::String => {}
            T::Absent | T::Unspecified => unreachable!("handled above"),
        }
        self.present += p.present;
        Ok(())
    }

    /// The final result for one op. `present == 0` reports no value
    /// (COUNT aside, which reports the zero).
    fn result(
        &self,
        name: &str,
        op: crate::pb::AggregateOp,
    ) -> Result<crate::pb::AggregateResult, Status> {
        use crate::pb::aggregate_result::Value as W;
        use crate::pb::{AggregateOp as O, AggregateValueType as T};
        let int_typed = self.vt == Some(T::Int);
        let value = match op {
            O::Count => Some(W::IntValue(self.present as i64)),
            _ if self.present == 0 => None,
            O::Sum if int_typed => match i64::try_from(self.int_sum) {
                Ok(v) => Some(W::IntValue(v)),
                Err(_) => {
                    return Err(Status::failed_precondition(format!(
                        "aggregation {name:?}: the exact int sum {} does not fit i64; \
                         aggregate double(...) for an IEEE sum, or ask for the mean",
                        self.int_sum
                    )));
                }
            },
            O::Sum => Some(W::DoubleValue(self.dsum + self.dcomp)),
            O::Min if int_typed => Some(W::IntValue(self.int_min)),
            O::Min => Some(W::DoubleValue(self.dmin)),
            O::Max if int_typed => Some(W::IntValue(self.int_max)),
            O::Max => Some(W::DoubleValue(self.dmax)),
            O::Mean => Some(W::DoubleValue(self.mean)),
            O::Variance => Some(W::DoubleValue(self.m2 / self.present as f64)),
            O::Stddev => Some(W::DoubleValue((self.m2 / self.present as f64).sqrt())),
            O::Unspecified => unreachable!("compile refused the unspecified op"),
        };
        Ok(crate::pb::AggregateResult {
            name: name.to_string(),
            present: self.present,
            value,
        })
    }
}

/// Type name for the disagreement refusal.
fn agg_vt_name(vt: crate::pb::AggregateValueType) -> &'static str {
    use crate::pb::AggregateValueType as T;
    match vt {
        T::Int => "int",
        T::Double => "double",
        T::String => "string",
        T::Absent | T::Unspecified => "absent",
    }
}

fn merge_column_stats(
    requested: &[String],
    shard_stats: &[Vec<crate::pb::ColumnStats>],
) -> Result<Vec<crate::pb::ColumnStats>, Status> {
    let mut out: Vec<crate::pb::ColumnStats> = requested
        .iter()
        .map(|name| crate::pb::ColumnStats {
            field: name.clone(),
            known: false,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            ..Default::default()
        })
        .collect();
    for shard in shard_stats {
        if shard.len() != requested.len() {
            return Err(Status::internal(format!(
                "shard answered {} stats columns for {} requested",
                shard.len(),
                requested.len()
            )));
        }
        for (acc, s) in out.iter_mut().zip(shard) {
            acc.known |= s.known;
            if s.count > 0 {
                acc.count += s.count;
                acc.sum += s.sum;
                acc.min = acc.min.min(s.min);
                acc.max = acc.max.max(s.max);
            }
        }
    }
    for acc in &mut out {
        if !acc.known {
            return Err(Status::invalid_argument(format!(
                "no shard has stats column {:?}: check the spelling, or the nodes' \
                 --numeric-fields / --integer-fields",
                acc.field
            )));
        }
        if acc.count > 0 {
            acc.mean = acc.sum / acc.count as f64;
        } else {
            acc.min = 0.0;
            acc.max = 0.0;
        }
    }
    Ok(out)
}

/// Union per-shard distinct facet values into exact global
/// cardinalities. Values, not ordinals — ordinals are shard-local —
/// and exact by construction: the union of exact per-shard distinct
/// sets is the exact global distinct set. Cost is the value strings on
/// the wire, which is the caller's explicit choice (docs/facets.md).
fn merge_cardinality(
    requested: &[String],
    shard_distinct: &[Vec<crate::pb::FacetDistinct>],
) -> Result<Vec<crate::pb::FacetCardinality>, Status> {
    let mut known = vec![false; requested.len()];
    let mut sets: Vec<std::collections::HashSet<String>> =
        requested.iter().map(|_| Default::default()).collect();
    for shard in shard_distinct {
        if shard.len() != requested.len() {
            return Err(Status::internal(format!(
                "shard answered {} cardinality columns for {} requested",
                shard.len(),
                requested.len()
            )));
        }
        for ((k, set), d) in known.iter_mut().zip(&mut sets).zip(shard) {
            *k |= d.known;
            set.extend(d.values.iter().cloned());
        }
    }
    requested
        .iter()
        .zip(known)
        .zip(sets)
        .map(|((name, known), set)| {
            if !known {
                return Err(Status::invalid_argument(format!(
                    "no shard has facet column {:?} for cardinality: check the spelling, \
                     or the nodes' --facet-fields",
                    name
                )));
            }
            Ok(crate::pb::FacetCardinality {
                field: name.clone(),
                cardinality: set.len() as u64,
            })
        })
        .collect()
}

/// Merge per-shard facet counts into global counts: the plain per-value
/// sum (counts are additive — no node's count depends on another's, so
/// there is no analog of the global-df trap), `known` when at least one
/// shard has the field, sorted count-descending with ties by value
/// ascending. A facet field NO shard knows is refused — the same rule
/// as an unknown scoring field: zeros everywhere would make a typo read
/// as "no results per anything".
fn merge_facet_counts(
    requested: &[String],
    map_requested: &[crate::pb::MapFacetField],
    shard_facets: &[Vec<crate::pb::FacetFieldCounts>],
) -> Result<Vec<crate::pb::FacetFieldCounts>, Status> {
    // Response order is the request order: plain entries, then map
    // entries — merged positionally.
    let want: Vec<(String, String)> = requested
        .iter()
        .map(|name| (name.clone(), String::new()))
        .chain(
            map_requested
                .iter()
                .map(|m| (m.column.clone(), m.key.clone())),
        )
        .collect();
    if want.is_empty() {
        return Ok(Vec::new());
    }
    let mut known = vec![false; want.len()];
    let mut sums: Vec<HashMap<String, u64>> = want.iter().map(|_| HashMap::new()).collect();
    for per_shard in shard_facets {
        if per_shard.len() != want.len() {
            return Err(Status::internal(format!(
                "shard returned {} facet fields for {} requested",
                per_shard.len(),
                want.len()
            )));
        }
        for (fi, ff) in per_shard.iter().enumerate() {
            known[fi] |= ff.known;
            for c in &ff.counts {
                *sums[fi].entry(c.value.clone()).or_default() += c.count;
            }
        }
    }
    let unknown: Vec<String> = want
        .iter()
        .zip(&known)
        .filter(|(_, k)| !**k)
        .map(|((field, key), _)| {
            if key.is_empty() {
                format!("{field:?}")
            } else {
                format!("{field:?}[{key:?}]")
            }
        })
        .collect();
    if !unknown.is_empty() {
        return Err(Status::invalid_argument(format!(
            "no shard has facet field {}: counting an unknown field would silently answer \
             zero everywhere. Check the spelling, or the nodes' --facet-fields / \
             --map-facet-fields.",
            unknown.join(", ")
        )));
    }
    Ok(want
        .into_iter()
        .zip(sums)
        .map(|((field, key), sum)| {
            let mut counts: Vec<crate::pb::FacetCount> = sum
                .into_iter()
                .map(|(value, count)| crate::pb::FacetCount { value, count })
                .collect();
            counts.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.value.cmp(&b.value)));
            crate::pb::FacetFieldCounts {
                field,
                known: true,
                counts,
                key,
            }
        })
        .collect())
}

/// Merge per-shard range-facet counts into global counts
/// (`docs/range-facets.md`): the positional per-bucket sum — bucket i
/// means the same interval on every shard, because the coordinator
/// forwarded one edge list — `known` when at least one shard could
/// resolve the column, and a column NO shard knows refused, exactly as
/// for plain facets.
fn merge_range_counts(
    requested: &[crate::pb::RangeFacetField],
    shard_ranges: &[Vec<crate::pb::RangeFacetCounts>],
) -> Result<Vec<crate::pb::RangeFacetCounts>, Status> {
    if requested.is_empty() {
        return Ok(Vec::new());
    }
    let mut known = vec![false; requested.len()];
    let mut sums: Vec<Vec<u64>> = requested
        .iter()
        .map(|r| vec![0u64; r.edges.len() - 1])
        .collect();
    for per_shard in shard_ranges {
        if per_shard.len() != requested.len() {
            return Err(Status::internal(format!(
                "shard returned {} range facets for {} requested",
                per_shard.len(),
                requested.len()
            )));
        }
        for (ri, rf) in per_shard.iter().enumerate() {
            known[ri] |= rf.known;
            // A shard that could not resolve the column contributes
            // nothing; one that could must answer a bucket per edge
            // interval, or the positional sum would be meaningless.
            if !rf.known {
                continue;
            }
            if rf.buckets.len() != sums[ri].len() {
                return Err(Status::internal(format!(
                    "shard returned {} buckets for {} edges on range facet {:?}",
                    rf.buckets.len(),
                    requested[ri].edges.len(),
                    requested[ri].column
                )));
            }
            for (acc, b) in sums[ri].iter_mut().zip(&rf.buckets) {
                *acc += b.count;
            }
        }
    }
    let unknown: Vec<String> = requested
        .iter()
        .zip(&known)
        .filter(|(_, k)| !**k)
        .map(|(r, _)| {
            if r.key.is_empty() {
                format!("{:?}", r.column)
            } else {
                format!("{:?}[{:?}]", r.column, r.key)
            }
        })
        .collect();
    if !unknown.is_empty() {
        return Err(Status::invalid_argument(format!(
            "no shard has range-facet column {}: bucketing an unknown column would \
             silently answer zero everywhere. Check the spelling, or the nodes' \
             --numeric-fields / --integer-fields / --map-numeric-fields.",
            unknown.join(", ")
        )));
    }
    Ok(requested
        .iter()
        .zip(sums)
        .map(|(r, counts)| crate::pb::RangeFacetCounts {
            column: r.column.clone(),
            key: r.key.clone(),
            known: true,
            buckets: counts
                .into_iter()
                .enumerate()
                .map(|(i, count)| crate::pb::RangeBucket {
                    from: r.edges[i],
                    to: r.edges[i + 1],
                    count,
                })
                .collect(),
        })
        .collect())
}

/// Refuse a geo-filter column NO shard knows (`docs/geo-columns.md`).
/// The same typo rule as fields, facets, chains, and range facets, and
/// the sharpest case of it: a filter over a misspelled column would
/// remove EVERY document on every shard and return an empty result set
/// that looks exactly like an honest "nothing matched". A partially
/// known column is the heterogeneous fleet and is exact — the shards
/// without it hold documents with no location, and no location is
/// inside no region.
fn refuse_unknown_geo_columns(
    filters: &[crate::pb::GeoFilter],
    known: &[bool],
) -> Result<(), Status> {
    let unknown: Vec<String> = filters
        .iter()
        .zip(known)
        .filter(|(_, k)| !**k)
        .map(|(f, _)| format!("{:?}", f.column))
        .collect();
    if !unknown.is_empty() {
        return Err(Status::invalid_argument(format!(
            "no shard has geo column {}: filtering on an unknown column would remove every \
             document and read as an empty result set. Check the spelling, or the nodes' \
             --geo-fields.",
            unknown.join(", ")
        )));
    }
    Ok(())
}

/// The typo rule for compiled filter trees: a leaf NO shard can
/// resolve is a name spelled wrong (or a literal whose type picked the
/// wrong table), and filtering on it would read as an empty result
/// set. Known flags are positional over
/// [`crate::filter::walk_leaves`] order; the nodes derive theirs from
/// the same walk, so the zip below cannot misattribute a flag.
fn refuse_unknown_filter_leaves(
    filter: Option<&crate::pb::FilterExpr>,
    known: &[bool],
) -> Result<(), Status> {
    let Some(expr) = filter else {
        return Ok(());
    };
    let mut leaves = Vec::new();
    crate::filter::walk_leaves(expr, &mut |l| leaves.push(l));
    let unknown: Vec<String> = leaves
        .iter()
        .zip(known)
        .filter(|(_, k)| !**k)
        .map(|(l, _)| l.describe())
        .collect();
    if !unknown.is_empty() {
        return Err(Status::invalid_argument(format!(
            "no shard can resolve filter {}: filtering on an unknown name would read as \
             an empty result set. Check the spelling and the literal's type (a string \
             literal selects the facet tables, a number the i64/f64 tables), or the \
             nodes' --facet-fields / --numeric-fields / --integer-fields / \
             --map-facet-fields / --map-numeric-fields / --geo-fields.",
            unknown.join(", ")
        )));
    }
    Ok(())
}

/// A request's filters after compilation: the geo family verbatim and
/// the CEL surface compiled ONCE into the predicate IR
/// (`docs/cel-filters.md`). Every shard receives this same tree and
/// none ever sees CEL text. Shared by the lexical and vector legs, so
/// a fused result cannot mix a filtered half with an unfiltered one
/// (`docs/vector-filters.md`).
#[derive(Debug, Clone, Default)]
pub struct RequestFilters {
    /// `geo_filters` verbatim.
    pub geo: Vec<crate::pb::GeoFilter>,
    /// The compiled tree; `None` when the request sent no CEL.
    pub tree: Option<crate::pb::FilterExpr>,
}

/// A browse resume boundary: the last returned id, plus its adjusted
/// sort-key bits when the browse is column-ordered.
#[derive(Debug, Clone, Copy)]
pub struct BrowseAfter {
    pub id: u64,
    pub key_bits: u64,
}

/// One merged browse page.
#[derive(Debug, Clone)]
pub struct BrowseRows {
    /// Global doc ids in final order.
    pub ids: Vec<u64>,
    /// Adjusted order-preserving key bits, parallel to `ids` (the ids
    /// themselves unsorted).
    pub key_bits: Vec<u64>,
    /// Reported sort-column values, parallel (0.0 unsorted).
    pub keys: Vec<f64>,
    /// Whether a column order was applied.
    pub sorted: bool,
}

/// One exact distributed membership bitmap decoded into stable product ids.
/// `epochs` is populated for lexical membership, parallel to the coordinator's
/// node order, so the scoring phase can refuse a store mutation between set
/// planning and candidate rescoring.
#[derive(Debug, Clone, Default)]
pub struct MembershipSet {
    pub ids: BTreeSet<u64>,
    pub epochs: Vec<u64>,
    pub wire_bytes: u64,
    pub terms: Vec<String>,
    pub(crate) ranges: Vec<(u64, u64)>,
}

impl RequestFilters {
    /// Compile a request's filter surface, validating both families.
    pub(crate) fn compile(geo: &[crate::pb::GeoFilter], cel: &str) -> Result<Self, Status> {
        crate::node::validate_geo_filters(geo)?;
        let tree = crate::cel::compile_filter(cel)?;
        if let Some(f) = tree.as_ref() {
            crate::filter::validate_filter(f)?;
        }
        Ok(Self {
            geo: geo.to_vec(),
            tree,
        })
    }
}

/// Accumulates the per-shard known-column handshakes for one request:
/// a column or leaf counts as known when ANY shard resolves it, and
/// the request is refused when NO shard does. A shard that answers the
/// wrong number of flags is a protocol break, not a partial answer.
struct FilterKnown {
    geo: Vec<bool>,
    tree: Vec<bool>,
    leaves: usize,
}

impl FilterKnown {
    fn new(filters: &RequestFilters) -> Self {
        let leaves = filters.tree.as_ref().map_or(0, crate::filter::leaf_count);
        Self {
            geo: vec![false; filters.geo.len()],
            tree: vec![false; leaves],
            leaves,
        }
    }

    /// Fold one shard's answer in.
    fn merge(&mut self, geo: &[bool], tree: &[bool]) -> Result<(), Status> {
        if geo.len() != self.geo.len() {
            return Err(Status::internal(format!(
                "shard answered {} geo-column flags for {} filters",
                geo.len(),
                self.geo.len()
            )));
        }
        if tree.len() != self.leaves {
            return Err(Status::internal(format!(
                "shard answered {} filter-leaf flags for {} leaves",
                tree.len(),
                self.leaves
            )));
        }
        for (acc, k) in self.geo.iter_mut().zip(geo) {
            *acc |= *k;
        }
        for (acc, k) in self.tree.iter_mut().zip(tree) {
            *acc |= *k;
        }
        Ok(())
    }

    /// Refuse a name NO shard resolved.
    fn refuse_unknown(&self, filters: &RequestFilters) -> Result<(), Status> {
        refuse_unknown_geo_columns(&filters.geo, &self.geo)?;
        refuse_unknown_filter_leaves(filters.tree.as_ref(), &self.tree)
    }
}

/// Merged global stats for a fused multi-field query, with the per-node
/// epochs the shares were valid at (parallel to the node list).
struct FusedGlobals {
    doc_count: u64,
    /// Per field: global sum of that field's document lengths.
    totals: Vec<u64>,
    /// Per field: global df per term, in that field's term order.
    dfs: Vec<Vec<u32>>,
    epochs: Vec<u64>,
    /// Number of primary shards whose field table contains each field.
    known_shards: Vec<usize>,
    /// Number of primary shards whose field carries token positions
    /// (docs/phrase-proximity.md); a positional phrase needs every one.
    positions_shards: Vec<usize>,
}

impl CoordinatorServiceImpl {
    /// A coordinator over the given shard nodes (fan-out order = shard
    /// index for merge tie-breaks).
    pub fn new(node_addrs: Vec<String>) -> Self {
        let stats_cache = Arc::new(crate::stats_cache::StatsCache::new(node_addrs.len()));
        Self {
            node_addrs,
            replica_addrs: Vec::new(),
            analysis_addr: None,
            bm25_params: Bm25Params::default(),
            limits: FanoutLimits::default(),
            stream_search: false,
            bm25_stream: false,
            max_k: DEFAULT_MAX_K,
            max_rerank_bytes: DEFAULT_MAX_RERANK_BYTES,
            channels: Arc::new(Mutex::new(HashMap::new())),
            allow_network: true,
            floor_socket: Arc::new(std::sync::OnceLock::new()),
            floor_targets: Arc::new(Mutex::new(HashMap::new())),
            stats_cache,
            clustered_vectors: None,
            dense_quality_profile: None,
            topology_generation: 0,
            hash_ranges: Vec::new(),
            live_topology: None,
            write_gate: Arc::new(tokio::sync::RwLock::new(())),
            cutover_guard: Arc::new(Mutex::new(None)),
            cutover_pending: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            phrase_index: None,
            query_progress: None,
        }
    }

    /// A coordinator whose shard traffic is restricted to the supplied
    /// in-process channels. Cache misses never fall back to TCP and the UDP
    /// signal lane is disabled. The labels are diagnostic identities only;
    /// no address parsing or name resolution occurs.
    pub fn with_in_process_channels(channels: Vec<Channel>) -> Self {
        let node_addrs: Vec<String> = (0..channels.len())
            .map(|shard| format!("in-process://shard-{shard}"))
            .collect();
        let channel_map = node_addrs
            .iter()
            .cloned()
            .zip(channels)
            .collect::<HashMap<_, _>>();
        let mut coordinator = Self::new(node_addrs);
        coordinator.channels = Arc::new(Mutex::new(channel_map));
        coordinator.allow_network = false;
        coordinator
    }

    /// True only for coordinators that may create network transports.
    pub fn allows_network(&self) -> bool {
        self.allow_network
    }

    pub fn max_k(&self) -> u32 {
        self.max_k
    }

    /// The term-stats cache, exposed for tests (`fetch_count` is how a
    /// test proves the hit path issued no RPCs).
    pub fn stats_cache(&self) -> &crate::stats_cache::StatsCache {
        &self.stats_cache
    }

    /// Enable atomic topology replacement. The current fields become the
    /// configured generation's immutable request snapshot.
    pub fn with_hot_topology(
        mut self,
        hash_ranges: Vec<Option<(u64, u64)>>,
    ) -> Result<Self, String> {
        if hash_ranges.len() != self.node_addrs.len() {
            return Err(format!(
                "topology has {} shard addresses but {} hash ranges",
                self.node_addrs.len(),
                hash_ranges.len()
            ));
        }
        let routes = self
            .node_addrs
            .iter()
            .enumerate()
            .map(|(shard, addr)| TopologyRoute {
                addr: addr.clone(),
                replica: self.replica_addrs.get(shard).cloned().flatten(),
                hash_range: hash_ranges.get(shard).copied().flatten(),
            })
            .collect();
        let topology = build_topology(self.topology_generation, routes)?;
        self.hash_ranges = hash_ranges;
        self.live_topology = Some(Arc::new(RwLock::new(Arc::new(topology))));
        Ok(self)
    }

    /// Atomically publish a strictly newer topology generation. Existing
    /// requests retain their prior `Arc`; later requests snapshot this map.
    pub fn reload_topology(
        &self,
        generation: u64,
        routes: Vec<TopologyRoute>,
    ) -> Result<(), String> {
        if self.cutover_pending.load(AtomicOrdering::Acquire) {
            return Err(
                "topology cutover has frozen writes; publish or abort it first".to_string(),
            );
        }
        self.publish_topology_inner(generation, routes)
    }

    fn publish_topology_inner(
        &self,
        generation: u64,
        routes: Vec<TopologyRoute>,
    ) -> Result<(), String> {
        let authority = self
            .live_topology
            .as_ref()
            .ok_or_else(|| "hot topology is not enabled".to_string())?;
        let replacement = Arc::new(build_topology(generation, routes)?);
        let mut current = authority
            .write()
            .map_err(|_| "topology authority lock is poisoned".to_string())?;
        if generation <= current.generation {
            return Err(format!(
                "topology generation must increase: current {}, proposed {generation}",
                current.generation
            ));
        }
        *current = replacement;
        Ok(())
    }

    pub fn current_topology_generation(&self) -> u64 {
        self.live_topology
            .as_ref()
            .and_then(|authority| authority.read().ok().map(|topology| topology.generation))
            .unwrap_or(self.topology_generation)
    }

    /// Current routes for control-plane workers. The returned vector is one
    /// immutable generation snapshot; callers never borrow the live lock.
    pub fn current_topology_routes(&self) -> Vec<TopologyRoute> {
        if let Some(authority) = &self.live_topology {
            return authority
                .read()
                .expect("topology authority lock poisoned")
                .routes
                .clone();
        }
        self.node_addrs
            .iter()
            .enumerate()
            .map(|(shard, addr)| TopologyRoute {
                addr: addr.clone(),
                replica: self.replica_addrs.get(shard).cloned().flatten(),
                hash_range: self.hash_ranges.get(shard).copied().flatten(),
            })
            .collect()
    }

    /// Resolve an opaque stable product identity under one immutable map.
    /// Returns `(generation, shard_index)` for ingest stamping.
    pub fn route_stable_key(&self, key: &[u8]) -> Result<(u64, usize), String> {
        if key.is_empty() {
            return Err("stable routing key is empty".to_string());
        }
        let hash = stable_routing_hash(key);
        let (generation, ranges): (u64, Vec<Option<(u64, u64)>>) =
            if let Some(authority) = &self.live_topology {
                let topology = authority
                    .read()
                    .map_err(|_| "topology authority lock is poisoned".to_string())?
                    .clone();
                (
                    topology.generation,
                    topology
                        .routes
                        .iter()
                        .map(|route| route.hash_range)
                        .collect(),
                )
            } else {
                (self.topology_generation, self.hash_ranges.clone())
            };
        if ranges.is_empty() || ranges.iter().any(Option::is_none) {
            return Err("topology has no complete stable hash ranges".to_string());
        }
        let shard = ranges
            .iter()
            .position(|range| range.is_some_and(|(lo, hi)| hash >= lo && hash <= hi))
            .ok_or_else(|| format!("stable hash {hash} is not covered by the topology"))?;
        Ok((generation, shard))
    }

    fn request_snapshot(&self) -> Option<Self> {
        let authority = self.live_topology.as_ref()?;
        let topology = authority
            .read()
            .expect("topology authority lock poisoned")
            .clone();
        let mut frozen = self.clone();
        frozen.node_addrs = topology
            .routes
            .iter()
            .map(|route| route.addr.clone())
            .collect();
        frozen.replica_addrs = topology
            .routes
            .iter()
            .map(|route| route.replica.clone())
            .collect();
        frozen.topology_generation = topology.generation;
        frozen.hash_ranges = topology
            .routes
            .iter()
            .map(|route| route.hash_range)
            .collect();
        frozen.stats_cache = topology.stats_cache.clone();
        frozen.live_topology = None;
        Some(frozen)
    }

    fn require_topology_generation(&self, requested: u64) -> Result<(), Status> {
        if requested != 0 && requested != self.topology_generation {
            return Err(Status::failed_precondition(format!(
                "request requires topology generation {requested}, but this request was assigned generation {}",
                self.topology_generation
            )));
        }
        Ok(())
    }

    /// The UDP signal socket, bound once (nonblocking: a full local
    /// buffer drops the datagram, which a monotone hint tolerates).
    fn floor_socket(&self) -> Option<&Arc<std::net::UdpSocket>> {
        if !self.allow_network {
            return None;
        }
        self.floor_socket
            .get_or_init(|| {
                std::net::UdpSocket::bind(("0.0.0.0", 0)).ok().map(|s| {
                    let _ = s.set_nonblocking(true);
                    Arc::new(s)
                })
            })
            .as_ref()
    }

    /// The UDP signal target for a node address: the same host:port as
    /// its gRPC listener, in the UDP namespace. Resolved once and
    /// cached; IPv4 preferred (the fleet pins IPv4).
    fn floor_target(&self, addr: &str) -> Option<std::net::SocketAddr> {
        if !self.allow_network {
            return None;
        }
        let mut cache = self
            .floor_targets
            .lock()
            .expect("floor target cache poisoned");
        if let Some(target) = cache.get(addr) {
            return *target;
        }
        let stripped = addr
            .strip_prefix("http://")
            .or_else(|| addr.strip_prefix("https://"))
            .unwrap_or(addr);
        let resolved = std::net::ToSocketAddrs::to_socket_addrs(stripped)
            .ok()
            .and_then(|addrs| {
                let all: Vec<std::net::SocketAddr> = addrs.collect();
                all.iter()
                    .find(|a| a.is_ipv4())
                    .copied()
                    .or(all.first().copied())
            });
        cache.insert(addr.to_string(), resolved);
        resolved
    }

    /// Serve plain vector `Search` over the streaming protocol: shards
    /// emit above the relayed floor and this coordinator holds the only
    /// top-k. Results are identical to the default fan-out; the modes
    /// differ in where pruning happens, not in what they return.
    pub fn with_stream_search(mut self, on: bool) -> Self {
        self.stream_search = on;
        self
    }

    /// Serve flat `Bm25Search` over the `Bm25QueryStream` floor relay:
    /// shards publish their running k-th best, this coordinator relays
    /// the fleet maximum back, and block-max turns each raise into
    /// blocks never read. Results are identical to the unary fan-out.
    pub fn with_bm25_stream(mut self, on: bool) -> Self {
        self.bm25_stream = on;
        self
    }

    fn with_query_progress(mut self, progress: watch::Sender<Option<QueryProgress>>) -> Self {
        self.query_progress = Some(progress);
        self
    }

    fn publish_progress(
        &self,
        phase: crate::pb::QueryStreamPhase,
        mut hits: Vec<(u64, f32)>,
        scoring_fingerprint: impl Into<String>,
    ) {
        let Some(progress) = self.query_progress.as_ref() else {
            return;
        };
        hits.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        let next = QueryProgress {
            phase,
            hits,
            scoring_fingerprint: scoring_fingerprint.into(),
        };
        progress.send_if_modified(|current| {
            if current.as_ref() == Some(&next) {
                false
            } else {
                *current = Some(next);
                true
            }
        });
    }

    /// Set the hard cap on client-facing `k` (also the depth a request
    /// omitting `k` runs at). Zero is rejected at config parse time, so
    /// this takes the already-validated value.
    pub fn with_max_k(mut self, max_k: u32) -> Self {
        self.max_k = max_k;
        self
    }

    pub fn with_max_rerank_bytes(mut self, bytes: u64) -> Self {
        self.max_rerank_bytes = bytes;
        self
    }

    /// Resolve a request's `k` against the configured cap. Proto3 makes
    /// an omitted field 0, so 0 selects `max_k` (the documented sentinel
    /// idiom here, like `rbo_p`); anything above the cap is refused with
    /// both numbers named rather than silently clamped.
    fn resolve_k(&self, requested: u32) -> Result<u32, Status> {
        if requested == 0 {
            return Ok(self.max_k);
        }
        if requested > self.max_k {
            return Err(Status::invalid_argument(format!(
                "k={requested} exceeds this coordinator's max_k={}; \
                 lower k or raise --max-k",
                self.max_k
            )));
        }
        Ok(requested)
    }

    /// Configure the BM25 path: lexical analyzer for query analysis and
    /// the scoring parameters every shard is told to use.
    pub fn with_bm25(mut self, analysis_addr: Option<String>, params: Bm25Params) -> Self {
        self.analysis_addr = analysis_addr;
        self.bm25_params = params;
        self
    }

    /// Attach the same immutable phrase vocabulary used by every shard.
    pub fn with_phrase_index(
        mut self,
        phrase_index: Option<Arc<crate::phrases::PhraseIndex>>,
    ) -> Self {
        self.phrase_index = phrase_index;
        self
    }

    /// Configure per-shard deadlines and hedging.
    pub fn with_limits(mut self, limits: FanoutLimits) -> Self {
        self.limits = limits;
        self
    }

    /// Configure replica addresses (one optional entry per shard, same
    /// order as the node list). A replica must serve identical data —
    /// searches are exact, so either copy returns identical results.
    pub fn with_replicas(mut self, replica_addrs: Vec<Option<String>>) -> Self {
        self.replica_addrs = replica_addrs;
        self
    }

    /// Route vector work through one distributed TurboVec collection. The
    /// backend itself owns its only global heap and shard completion.
    pub fn with_clustered_turbovec(mut self, backend: ClusteredTurboVecBackend) -> Self {
        self.clustered_vectors = Some(backend);
        self
    }

    pub fn with_dense_quality_profile(
        mut self,
        profile: crate::quality::DenseQualityProfile,
    ) -> Self {
        self.dense_quality_profile = Some(Arc::new(profile));
        self
    }

    pub fn with_topology_generation(mut self, generation: u64) -> Self {
        self.topology_generation = generation;
        self
    }

    /// Prove the dense traversal contract against every live shard before the
    /// public query route runs. Provider identity is generation-wide: mixed
    /// quality contracts, score spaces, or dimensions are refused rather than
    /// hidden behind one coordinator response.
    pub(crate) async fn resolve_dense_execution(
        &self,
        requested: crate::pb::DenseExecutionMode,
        query_dim: usize,
    ) -> Result<crate::pb::DenseExecutionOutcome, Status> {
        let (provider, scoring_fingerprint, quality, exhaustive, dimensions) = if let Some(
            clustered,
        ) =
            &self.clustered_vectors
        {
            let identity = clustered.quality_identity().await?;
            (
                "clustered-turbovec".to_string(),
                identity.scoring_fingerprint,
                crate::pb::VectorQualityContract::ExhaustiveNativeScore,
                true,
                identity.dimensions,
            )
        } else {
            let mut tasks = Vec::with_capacity(self.node_addrs.len());
            for addr in &self.node_addrs {
                let mut client = self.node_client(addr)?;
                tasks.push(tokio::spawn(async move {
                    client
                        .get_vector_backend(crate::pb::GetVectorBackendRequest {})
                        .await
                        .map(tonic::Response::into_inner)
                }));
            }

            let mut provider = None;
            let mut fingerprint = None;
            let mut quality = None;
            let mut exhaustive = None;
            let mut dimensions = None;
            for task in tasks {
                let backend = task.await.map_err(|error| {
                    Status::internal(format!("dense execution preflight failed: {error}"))
                })??;
                let descriptor = backend.descriptor.ok_or_else(|| {
                    Status::failed_precondition(
                        "dense execution preflight found an unconfigured vector shard",
                    )
                })?;
                if descriptor.backend_kind.is_empty() {
                    return Err(Status::failed_precondition(
                        "dense execution preflight found an unnamed vector provider",
                    ));
                }
                if descriptor.scoring_fingerprint.is_empty() {
                    return Err(Status::failed_precondition(
                        "dense execution preflight found an empty scoring fingerprint",
                    ));
                }
                let shard_quality =
                    crate::pb::VectorQualityContract::try_from(descriptor.quality_contract)
                        .map_err(|_| {
                            Status::failed_precondition(format!(
                                "vector provider {} advertises unknown quality contract {}",
                                descriptor.backend_kind, descriptor.quality_contract
                            ))
                        })?;
                if shard_quality == crate::pb::VectorQualityContract::Unspecified {
                    return Err(Status::failed_precondition(format!(
                        "vector provider {} does not declare a quality contract",
                        descriptor.backend_kind
                    )));
                }
                let direction = crate::pb::VectorScoreDirection::try_from(
                    descriptor.score_direction,
                )
                .map_err(|_| {
                    Status::failed_precondition(format!(
                        "vector provider {} advertises unknown score direction {}",
                        descriptor.backend_kind, descriptor.score_direction
                    ))
                })?;
                if direction != crate::pb::VectorScoreDirection::HigherIsBetter {
                    return Err(Status::failed_precondition(format!(
                            "vector provider {} is not compatible with the coordinator's higher-is-better heap",
                            descriptor.backend_kind
                        )));
                }
                if !descriptor
                    .capabilities
                    .iter()
                    .any(|capability| capability == "batch_query")
                {
                    return Err(Status::failed_precondition(format!(
                        "vector provider {} does not advertise batch_query",
                        descriptor.backend_kind
                    )));
                }
                let shard_exhaustive = shard_quality
                    == crate::pb::VectorQualityContract::ExhaustiveNativeScore
                    && descriptor
                        .capabilities
                        .iter()
                        .any(|capability| capability == "exhaustive_completion");

                for (held, actual, mismatch) in [
                    (
                        &mut provider,
                        descriptor.backend_kind,
                        "mixed vector providers",
                    ),
                    (
                        &mut fingerprint,
                        descriptor.scoring_fingerprint,
                        "mixed scoring fingerprints",
                    ),
                ] {
                    match held {
                        Some(value) if value != &actual => {
                            return Err(Status::failed_precondition(format!(
                                "dense execution preflight found {mismatch}"
                            )))
                        }
                        None => *held = Some(actual),
                        _ => {}
                    }
                }
                match quality {
                    Some(value) if value != shard_quality => {
                        return Err(Status::failed_precondition(
                            "dense execution preflight found mixed quality contracts",
                        ))
                    }
                    None => quality = Some(shard_quality),
                    _ => {}
                }
                match exhaustive {
                    Some(value) if value != shard_exhaustive => {
                        return Err(Status::failed_precondition(
                            "dense execution preflight found mixed completion capabilities",
                        ))
                    }
                    None => exhaustive = Some(shard_exhaustive),
                    _ => {}
                }
                match dimensions {
                    Some(value) if value != descriptor.dim => {
                        return Err(Status::failed_precondition(
                            "dense execution preflight found mixed vector dimensions",
                        ))
                    }
                    None => dimensions = Some(descriptor.dim),
                    _ => {}
                }
            }
            (
                provider.unwrap_or_default(),
                fingerprint.unwrap_or_default(),
                quality.unwrap_or(crate::pb::VectorQualityContract::Unspecified),
                exhaustive.unwrap_or(false),
                dimensions.unwrap_or_default(),
            )
        };

        if dimensions as usize != query_dim {
            return Err(Status::failed_precondition(format!(
                "dense query dimension {query_dim} does not match live provider dimension {dimensions}"
            )));
        }

        let exact_available =
            quality == crate::pb::VectorQualityContract::ExhaustiveNativeScore && exhaustive;
        let (resolved, planner_reason) = match requested {
            crate::pb::DenseExecutionMode::Unspecified => {
                if !exact_available {
                    return Err(Status::failed_precondition(format!(
                        "unspecified dense execution preserves the exact contract, but provider {provider} advertises {quality:?} without exhaustive completion; request ANN explicitly or install an exact backend"
                    )));
                }
                (
                    crate::pb::DenseExecutionMode::Exact,
                    "legacy unspecified mode preserves exact traversal".to_string(),
                )
            }
            crate::pb::DenseExecutionMode::Exact => {
                if !exact_available {
                    return Err(Status::failed_precondition(format!(
                        "EXACT dense execution requires exhaustive native scoring and completion, but provider {provider} advertises {quality:?}"
                    )));
                }
                (
                    crate::pb::DenseExecutionMode::Exact,
                    "caller required exact traversal".to_string(),
                )
            }
            crate::pb::DenseExecutionMode::Ann => {
                if exact_available {
                    return Err(Status::failed_precondition(format!(
                        "ANN dense execution was requested, but provider {provider} exposes only exhaustive traversal"
                    )));
                }
                if !matches!(
                    quality,
                    crate::pb::VectorQualityContract::ConfiguredAnn
                        | crate::pb::VectorQualityContract::ProbabilisticBound
                ) {
                    return Err(Status::failed_precondition(format!(
                        "provider {provider} does not expose a configured ANN traversal"
                    )));
                }
                (
                    crate::pb::DenseExecutionMode::Ann,
                    "caller accepted the provider's configured approximate traversal".to_string(),
                )
            }
            crate::pb::DenseExecutionMode::Auto => {
                if !exact_available {
                    return Err(Status::failed_precondition(format!(
                        "AUTO has no qualified adaptive policy for provider {provider} ({quality:?}); use ANN explicitly to accept its configured traversal"
                    )));
                }
                (
                    crate::pb::DenseExecutionMode::Exact,
                    "AUTO selected exact because the live provider proves exhaustive completion"
                        .to_string(),
                )
            }
        };

        Ok(crate::pb::DenseExecutionOutcome {
            requested_mode: requested as i32,
            resolved_mode: resolved as i32,
            provider_backend: provider,
            quality_contract: quality as i32,
            scoring_fingerprint,
            exhaustive_completion: resolved == crate::pb::DenseExecutionMode::Exact,
            planner_reason,
        })
    }

    /// Resolve and prove one measured dense quality request against the live
    /// provider and product exact-row generation. Drift is a hard failure.
    pub(crate) async fn resolve_dense_quality(
        &self,
        k: u32,
        query_dim: usize,
        policy: &crate::pb::DenseQualityPolicy,
    ) -> Result<crate::quality::DenseQualityResolution, Status> {
        let profile = self.dense_quality_profile.as_ref().ok_or_else(|| {
            Status::failed_precondition(
                "dense quality policy requested but this coordinator has no --dense-quality-profile",
            )
        })?;
        let resolution = profile
            .resolve(
                k,
                policy.target_recall_ppm,
                &policy.required_profile_fingerprint,
                policy.max_candidates,
            )
            .map_err(Status::invalid_argument)?;
        if resolution.selection_k > self.max_k {
            return Err(Status::failed_precondition(format!(
                "quality profile resolves selection_k={} above coordinator max_k={}; raise --max-k or measure a bounded policy",
                resolution.selection_k, self.max_k
            )));
        }
        if resolution.dimensions as usize != query_dim {
            return Err(Status::failed_precondition(format!(
                "quality profile dimension {} does not match query dimension {query_dim}",
                resolution.dimensions
            )));
        }

        let (provider, scoring_fingerprint, rows, generation, dimensions) = if let Some(clustered) =
            &self.clustered_vectors
        {
            let identity = clustered.quality_identity().await?;
            let mut tasks = Vec::with_capacity(self.node_addrs.len());
            for addr in &self.node_addrs {
                let mut client = self.node_client(addr)?;
                tasks.push(tokio::spawn(async move {
                    client
                        .health(HealthRequest {})
                        .await
                        .map(|reply| reply.into_inner())
                }));
            }
            let mut exact_rows = 0u64;
            for task in tasks {
                let health = task.await.map_err(|error| {
                    Status::internal(format!("quality exact-row preflight failed: {error}"))
                })??;
                if !health.exact_vectors_available {
                    return Err(Status::failed_precondition(format!(
                        "clustered quality profile requires aligned product-owned exact rows, but product shard at slot_offset={} has none",
                        health.slot_offset
                    )));
                }
                if health.deleted_docs != 0 {
                    return Err(Status::failed_precondition(format!(
                        "clustered quality profile was measured on an all-live generation, but product shard at slot_offset={} has {} tombstoned rows; compact and remeasure",
                        health.slot_offset, health.deleted_docs
                    )));
                }
                exact_rows = exact_rows
                    .checked_add(health.exact_vector_rows)
                    .ok_or_else(|| Status::internal("quality exact-row count overflow"))?;
            }
            if exact_rows != identity.rows {
                return Err(Status::failed_precondition(format!(
                    "clustered vector collection has {} rows but product shards own {exact_rows} exact rows",
                    identity.rows
                )));
            }
            (
                "clustered-turbovec".to_string(),
                identity.scoring_fingerprint,
                identity.rows,
                identity.topology_generation,
                identity.dimensions,
            )
        } else {
            let mut tasks = Vec::with_capacity(self.node_addrs.len());
            for addr in &self.node_addrs {
                let mut client = self.node_client(addr)?;
                tasks.push(tokio::spawn(async move {
                    let backend = client
                        .get_vector_backend(crate::pb::GetVectorBackendRequest {})
                        .await?
                        .into_inner();
                    let health = client.health(HealthRequest {}).await?.into_inner();
                    Ok::<_, Status>((backend, health))
                }));
            }
            let mut provider = None;
            let mut fingerprint = None;
            let mut rows = 0u64;
            let mut dimensions = None;
            for task in tasks {
                let (backend, health) = task.await.map_err(|error| {
                    Status::internal(format!("quality preflight task failed: {error}"))
                })??;
                let descriptor = backend.descriptor.ok_or_else(|| {
                    Status::failed_precondition(
                        "quality preflight found a shard without a vector descriptor",
                    )
                })?;
                if !health.exact_vectors_available {
                    return Err(Status::failed_precondition(format!(
                            "quality profile requires aligned exact rows, but shard at slot_offset={} has none",
                            health.slot_offset
                        )));
                }
                if health.deleted_docs != 0 {
                    return Err(Status::failed_precondition(format!(
                        "quality profile was measured on an all-live generation, but shard at slot_offset={} has {} tombstoned rows; compact and remeasure",
                        health.slot_offset, health.deleted_docs
                    )));
                }
                match &provider {
                    Some(held) if held != &descriptor.backend_kind => {
                        return Err(Status::failed_precondition(
                            "quality preflight found mixed vector providers",
                        ))
                    }
                    None => provider = Some(descriptor.backend_kind.clone()),
                    _ => {}
                }
                match &fingerprint {
                    Some(held) if held != &descriptor.scoring_fingerprint => {
                        return Err(Status::failed_precondition(
                            "quality preflight found mixed scoring fingerprints",
                        ))
                    }
                    None => fingerprint = Some(descriptor.scoring_fingerprint.clone()),
                    _ => {}
                }
                match dimensions {
                    Some(held) if held != descriptor.dim => {
                        return Err(Status::failed_precondition(
                            "quality preflight found mixed vector dimensions",
                        ))
                    }
                    None => dimensions = Some(descriptor.dim),
                    _ => {}
                }
                rows = rows
                    .checked_add(backend.num_vectors)
                    .ok_or_else(|| Status::internal("quality preflight row count overflow"))?;
            }
            (
                provider.unwrap_or_default(),
                fingerprint.unwrap_or_default(),
                rows,
                self.topology_generation,
                dimensions.unwrap_or_default(),
            )
        };

        for (name, expected, actual) in [
            (
                "provider_backend",
                resolution.provider_backend.as_str(),
                provider.as_str(),
            ),
            (
                "scoring_fingerprint",
                resolution.scoring_fingerprint.as_str(),
                scoring_fingerprint.as_str(),
            ),
        ] {
            if expected != actual {
                return Err(Status::failed_precondition(format!(
                    "quality profile {name} {expected:?} does not match live {actual:?}"
                )));
            }
        }
        if resolution.corpus_rows != rows
            || resolution.corpus_generation != generation
            || resolution.dimensions != dimensions
        {
            return Err(Status::failed_precondition(format!(
                "quality profile generation mismatch: profile rows/generation/dim={}/{}/{}, live={rows}/{generation}/{dimensions}",
                resolution.corpus_rows,
                resolution.corpus_generation,
                resolution.dimensions
            )));
        }
        Ok(resolution)
    }

    /// The pooled channel for `addr`, created lazily on first use.
    /// `connect_lazy` defers the TCP/HTTP2 handshake to the first RPC and
    /// transparently reconnects after failures, so one entry serves the
    /// address for the process lifetime.
    fn channel_to(&self, addr: &str) -> Result<Channel, Status> {
        let mut cache = self.channels.lock().expect("channel cache mutex poisoned");
        if let Some(ch) = cache.get(addr) {
            return Ok(ch.clone());
        }
        if !self.allow_network {
            return Err(Status::failed_precondition(format!(
                "in-process coordinator has no channel for {addr}; network fallback is disabled"
            )));
        }
        let endpoint = Endpoint::from_shared(addr.to_string())
            .map_err(|e| Status::unavailable(format!("invalid node address {addr}: {e}")))?
            .tcp_nodelay(true)
            // The client end is the RECEIVER of stream batches, so these
            // windows are what let a shard's pre-floor burst flow without
            // window-update round trips (see H2_STREAM_WINDOW).
            .initial_stream_window_size(crate::H2_STREAM_WINDOW)
            .initial_connection_window_size(crate::H2_CONN_WINDOW);
        let ch = endpoint.connect_lazy();
        cache.insert(addr.to_string(), ch.clone());
        Ok(ch)
    }

    /// A client over the pooled channel for `addr` with the message size
    /// limits applied. Cheap: clones the channel, no new connection.
    fn node_client(&self, addr: &str) -> Result<NodeServiceClient<Channel>, Status> {
        Ok(NodeServiceClient::new(self.channel_to(addr)?)
            .max_decoding_message_size(crate::MAX_MESSAGE_BYTES)
            .max_encoding_message_size(crate::MAX_MESSAGE_BYTES))
    }

    /// Distributed BM25 with the two-phase global-stats flow (see the
    /// proto comments on `SearchService.Bm25Search`), unseeded.
    pub async fn fanout_bm25(
        &self,
        text: &str,
        k: u32,
        spec: Option<&crate::pb::AnalysisSpec>,
    ) -> Result<Vec<Bm25Hit>, Status> {
        self.fanout_bm25_seeded(text, k, spec, 0.0).await
    }

    /// [`Self::fanout_bm25`] with a client-supplied floor: `min_score`
    /// is forwarded verbatim to every shard's `Bm25QueryRequest`, which
    /// prunes postings that provably cannot reach it (docs/block-max.md,
    /// stage 4). 0 means unseeded. There is deliberately NO mid-query
    /// relay: `Bm25Query` is unary, so a fleet-wide floor can only ever
    /// arrive with the request.
    pub async fn fanout_bm25_seeded(
        &self,
        text: &str,
        k: u32,
        spec: Option<&crate::pb::AnalysisSpec>,
        min_score: f32,
    ) -> Result<Vec<Bm25Hit>, Status> {
        self.fanout_bm25_faceted(text, k, spec, min_score, &[], &[], &[], &[], &[], None)
            .await
            .map(|(hits, _, _)| hits)
    }

    /// [`Self::fanout_bm25_seeded`] with count-then-rank facets and a
    /// score-function chain. Facet fields are forwarded to every
    /// shard, counted there over the full match set, and summed here —
    /// counts are additive, so the global counts are the plain
    /// per-value sum over shards. Score stages are forwarded verbatim
    /// in list order (the pinned evaluation order,
    /// `docs/score-functions.md`), so hits, `min_score`, and
    /// `kth_best` are on the FINAL scale; a stage column NO shard
    /// knows is refused after the round, like an unknown facet field.
    /// Returns `(hits, merged facet counts, merged range-facet
    /// counts)`; both count lists are empty when none were requested or
    /// the query analyzed to no terms (no match set to count).
    #[allow(clippy::too_many_arguments)]
    pub async fn fanout_bm25_faceted(
        &self,
        text: &str,
        k: u32,
        spec: Option<&crate::pb::AnalysisSpec>,
        min_score: f32,
        facet_fields: &[String],
        map_facet_fields: &[crate::pb::MapFacetField],
        range_facet_fields: &[crate::pb::RangeFacetField],
        score_stages: &[crate::pb::ScoreStage],
        geo_filters: &[crate::pb::GeoFilter],
        filter: Option<&crate::pb::FilterExpr>,
    ) -> Result<FacetedHits, Status> {
        self.fanout_bm25_aggregated(
            text,
            k,
            spec,
            min_score,
            facet_fields,
            map_facet_fields,
            range_facet_fields,
            score_stages,
            geo_filters,
            filter,
            &[],
            &[],
            &[],
        )
        .await
        .map(|r| (r.0, r.1, r.2))
    }

    /// [`Self::fanout_bm25_faceted`] plus column aggregations: match-set
    /// stats over numeric / integer columns and exact distinct-value
    /// counts over facet columns (docs/facets.md). Returns
    /// `(hits, facets, ranges, stats, cardinality)`.
    #[allow(clippy::too_many_arguments)]
    pub async fn fanout_bm25_aggregated(
        &self,
        text: &str,
        k: u32,
        spec: Option<&crate::pb::AnalysisSpec>,
        min_score: f32,
        facet_fields: &[String],
        map_facet_fields: &[crate::pb::MapFacetField],
        range_facet_fields: &[crate::pb::RangeFacetField],
        score_stages: &[crate::pb::ScoreStage],
        geo_filters: &[crate::pb::GeoFilter],
        filter: Option<&crate::pb::FilterExpr>,
        stats_fields: &[String],
        cardinality_fields: &[String],
        projections: &[crate::pb::CompiledProjection],
    ) -> Result<AggregatedHits, Status> {
        // Edge-list validation needs no shard, so it must not hide
        // behind the zero-term early return below: a malformed request
        // refuses even when there is no match set to count. (The nodes
        // validate again; this is the coordinator honoring the same
        // contract on the paths that never reach one.)
        crate::node::validate_range_facet_fields(range_facet_fields)?;
        // Geo-filter validation is local for the same reason
        // (docs/geo-columns.md): an antimeridian bbox or a zero radius
        // must refuse whether or not the query has a match set.
        crate::node::validate_geo_filters(geo_filters)?;
        // And filter-tree validation, for the same reason again
        // (docs/cel-filters.md).
        if let Some(f) = filter {
            crate::filter::validate_filter(f)?;
        }
        let addr = self.analysis_addr.clone().ok_or_else(|| {
            Status::unavailable("no analysis backend configured on the coordinator (analysis_addr)")
        })?;
        // (a) Query analysis with the SAME options as ingest: query terms
        // share identity with indexed terms (stems when SOURCE_STEMS).
        let analyzed = crate::analyzer::analyze_document(&addr, text, spec).await?;
        let mut terms: Vec<String> = Vec::new();
        for (term, _, _) in analyzed.into_body().terms {
            if !terms.contains(&term) {
                terms.push(term);
            }
        }
        if terms.is_empty() || k == 0 {
            return Ok((Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new()));
        }

        // (b) each shard's share of the corpus stats, cached per node;
        // (c)+(d) run as a round so a stale-stats refusal can rerun
        // them once against fresh stats with no claim.
        let mut fresh = false;
        loop {
            let (global, epochs) = self.body_stats(&terms, fresh).await?;
            let claims = if fresh { vec![0; epochs.len()] } else { epochs };
            match self
                .bm25_query_round(
                    &terms,
                    k,
                    min_score,
                    &global,
                    &claims,
                    facet_fields,
                    map_facet_fields,
                    range_facet_fields,
                    score_stages,
                    geo_filters,
                    filter,
                    stats_fields,
                    cardinality_fields,
                    projections,
                )
                .await
            {
                Err(e) if !fresh && is_stale_stats(&e) => {
                    self.stats_cache.invalidate_all();
                    fresh = true;
                }
                other => return other,
            }
        }
    }

    /// One Bm25Query fan-out with the GLOBAL stats: every shard scores
    /// identically, so the merge is a straight top-k. `claims[shard]`
    /// travels as that shard's `expected_stats_epoch`; `facet_fields`
    /// as its `facet_fields` (shard-local counts merge by plain sum);
    /// `score_stages` verbatim (list order is the pinned evaluation
    /// order, so every shard must see the same list).
    #[allow(clippy::too_many_arguments)]
    async fn bm25_query_round(
        &self,
        terms: &[String],
        k: u32,
        min_score: f32,
        global: &CorpusStats,
        claims: &[u64],
        facet_fields: &[String],
        map_facet_fields: &[crate::pb::MapFacetField],
        range_facet_fields: &[crate::pb::RangeFacetField],
        score_stages: &[crate::pb::ScoreStage],
        geo_filters: &[crate::pb::GeoFilter],
        filter: Option<&crate::pb::FilterExpr>,
        stats_fields: &[String],
        cardinality_fields: &[String],
        projections: &[crate::pb::CompiledProjection],
    ) -> Result<AggregatedHits, Status> {
        if self.node_addrs.is_empty() {
            return Err(Status::failed_precondition("no shard nodes configured"));
        }
        let mut query_tasks = Vec::with_capacity(self.node_addrs.len());
        // The streaming route's relay state: one conflated global floor
        // cell per query, seeded with the client floor. Shards' published
        // k-th bests fold into it (monotone max), and a per-stream
        // forwarder pushes whatever is newest when it wakes — the same
        // shape the vector relay uses.
        let relay = self
            .bm25_stream
            .then(|| watch::channel(min_score))
            .map(|(tx, rx)| (Arc::new(tx), rx));
        let stream_heap = self.bm25_stream.then(|| {
            Arc::new(Mutex::new(Bm25StreamHeap {
                heap: std::collections::BinaryHeap::with_capacity(k as usize + 1),
                floors_sent: 0,
                progress: self.query_progress.clone(),
            }))
        });
        for (shard, node) in self.node_addrs.iter().enumerate() {
            let request = Bm25QueryRequest {
                projections: projections.to_vec(),
                terms: terms.to_vec(),
                k,
                global_doc_count: global.doc_count,
                global_total_doc_length: global.total_doc_length,
                global_doc_frequencies: global.dfs.clone(),
                k1: self.bm25_params.k1 as f32,
                b: self.bm25_params.b as f32,
                min_score,
                fields: Vec::new(),
                expected_stats_epoch: claims[shard],
                facet_fields: facet_fields.to_vec(),
                map_facet_fields: map_facet_fields.to_vec(),
                range_facet_fields: range_facet_fields.to_vec(),
                score_stages: score_stages.to_vec(),
                geo_filters: geo_filters.to_vec(),
                filter: filter.cloned(),
                stats_fields: stats_fields.to_vec(),
                cardinality_fields: cardinality_fields.to_vec(),
                phrase: None,
            };
            let mut client = self.node_client(node)?;
            if let Some((floor_tx, floor_rx)) = relay.clone() {
                let global_heap = Arc::clone(stream_heap.as_ref().expect("relay has heap"));
                let deadline = self.limits.shard_deadline;
                query_tasks.push(tokio::spawn(async move {
                    let run = stream_bm25_shard(
                        shard as u32,
                        k as usize,
                        client,
                        request,
                        floor_tx,
                        floor_rx,
                        global_heap,
                    );
                    let result = match deadline {
                        Some(limit) => tokio::time::timeout(limit, run).await.map_err(|_| {
                            Status::deadline_exceeded(format!(
                                "BM25 shard {shard} exceeded its {}ms deadline",
                                limit.as_millis()
                            ))
                        })?,
                        None => run.await,
                    };
                    result.map(|r| {
                        let response = r.response;
                        (
                            shard as u32,
                            response.hits,
                            response.facets,
                            response.range_facets,
                            response.stage_columns_known,
                            response.geo_columns_known,
                            response.filter_columns_known,
                            response.stats,
                            response.distinct,
                            response.projection_leaves_known,
                            Some(r.scoring_fingerprint),
                        )
                    })
                }));
                continue;
            }
            query_tasks.push(tokio::spawn(async move {
                client.bm25_query(request).await.map(|r| {
                    let r = r.into_inner();
                    (
                        shard as u32,
                        r.hits,
                        r.facets,
                        r.range_facets,
                        r.stage_columns_known,
                        r.geo_columns_known,
                        r.filter_columns_known,
                        r.stats,
                        r.distinct,
                        r.projection_leaves_known,
                        None,
                    )
                })
            }));
        }
        let mut all: Vec<(u32, Bm25Hit)> = Vec::new();
        let mut shard_facets: Vec<Vec<crate::pb::FacetFieldCounts>> = Vec::new();
        let mut shard_ranges: Vec<Vec<crate::pb::RangeFacetCounts>> = Vec::new();
        let mut stage_known = vec![false; score_stages.len()];
        let mut geo_known = vec![false; geo_filters.len()];
        let filter_leaves = filter.map_or(0, crate::filter::leaf_count);
        let mut filter_known = vec![false; filter_leaves];
        // Projection column-read leaves, in the wire's flag order
        // (docs/cel-values.md).
        let projection_leaves: Vec<crate::values::ValueLeaf> = {
            let mut leaves = Vec::new();
            for p in projections {
                if let Some(expr) = p.expr.as_ref() {
                    crate::values::column_leaves(expr, &mut leaves);
                }
            }
            leaves
        };
        let mut projection_known = vec![false; projection_leaves.len()];
        let mut shard_stats: Vec<Vec<crate::pb::ColumnStats>> = Vec::new();
        let mut shard_distinct: Vec<Vec<crate::pb::FacetDistinct>> = Vec::new();
        let mut scoring_fingerprint: Option<String> = None;
        for task in query_tasks {
            let (
                shard,
                hits,
                facets,
                ranges,
                known,
                geo,
                fknown,
                sstats,
                sdistinct,
                pknown,
                fingerprint,
            ) = task
                .await
                .map_err(|e| Status::internal(format!("bm25 query task failed: {e}")))??;
            if let Some(fingerprint) = fingerprint {
                match scoring_fingerprint.as_ref() {
                    Some(expected) if expected != &fingerprint => {
                        return Err(Status::failed_precondition(format!(
                            "BM25 shard {shard} scoring fingerprint {fingerprint} differs from {expected}"
                        )));
                    }
                    None => scoring_fingerprint = Some(fingerprint),
                    _ => {}
                }
            }
            if pknown.len() != projection_leaves.len() {
                return Err(Status::internal(format!(
                    "shard answered {} projection-leaf flags for {} leaves",
                    pknown.len(),
                    projection_leaves.len()
                )));
            }
            for (acc, k) in projection_known.iter_mut().zip(&pknown) {
                *acc |= *k;
            }
            all.extend(hits.into_iter().map(|h| (shard, h)));
            shard_facets.push(facets);
            shard_ranges.push(ranges);
            shard_stats.push(sstats);
            shard_distinct.push(sdistinct);
            if geo.len() != geo_filters.len() {
                return Err(Status::internal(format!(
                    "shard answered {} geo-column flags for {} filters",
                    geo.len(),
                    geo_filters.len()
                )));
            }
            for (acc, k) in geo_known.iter_mut().zip(&geo) {
                *acc |= *k;
            }
            if fknown.len() != filter_leaves {
                return Err(Status::internal(format!(
                    "shard answered {} filter-leaf flags for {} leaves",
                    fknown.len(),
                    filter_leaves
                )));
            }
            for (acc, k) in filter_known.iter_mut().zip(&fknown) {
                *acc |= *k;
            }
            if known.len() != score_stages.len() {
                return Err(Status::internal(format!(
                    "shard answered {} stage-column flags for {} stages",
                    known.len(),
                    score_stages.len()
                )));
            }
            for (acc, k) in stage_known.iter_mut().zip(known) {
                *acc |= k;
            }
        }
        // A stage column NO shard knows is a typo wearing an identity
        // chain — the whole request would be a silent no-op. Refuse,
        // naming the column and the knob; a partially-known column is
        // the heterogeneous fleet and is exact (absent = identity).
        let unknown: Vec<String> = score_stages
            .iter()
            .zip(&stage_known)
            .filter(|(_, known)| !**known)
            .map(|(s, _)| {
                if s.key.is_empty() {
                    format!("{:?}", s.column)
                } else {
                    format!("{:?}[{:?}]", s.column, s.key)
                }
            })
            .collect();
        if !unknown.is_empty() {
            // A geo decay stage reads a geo column, so pointing the
            // caller at --numeric-fields alone would send them looking
            // in the wrong table for a name they spelled wrong.
            let any_geo = score_stages.iter().zip(&stage_known).any(|(s, known)| {
                !known
                    && matches!(
                        crate::pb::ScoreOp::try_from(s.op),
                        Ok(crate::pb::ScoreOp::MultGeoDecayHaversine
                            | crate::pb::ScoreOp::MultGeoDecayManhattan)
                    )
            });
            let knobs = if any_geo {
                "--numeric-fields / --integer-fields / --map-numeric-fields / --geo-fields"
            } else {
                "--numeric-fields / --map-numeric-fields"
            };
            return Err(Status::invalid_argument(format!(
                "no shard has numeric column {}: the chain would be a silent no-op. \
                 Check the spelling, or the nodes' {knobs}.",
                unknown.join(", ")
            )));
        }
        refuse_unknown_geo_columns(geo_filters, &geo_known)?;
        refuse_unknown_filter_leaves(filter, &filter_known)?;
        // A projection column NO shard knows is a typo answering
        // all-absent — refuse it by name. A partially-known column is
        // the heterogeneous fleet and is exact (absent documents hold
        // nothing).
        let unknown_projection: Vec<String> = projection_leaves
            .iter()
            .zip(&projection_known)
            .filter(|(_, known)| !**known)
            .map(|(leaf, _)| leaf.describe())
            .collect();
        if !unknown_projection.is_empty() {
            return Err(Status::invalid_argument(format!(
                "projection: no shard has column {}: every value would be absent. \
                 Check the spelling, or the nodes' --numeric-fields / --integer-fields \
                 / --facet-fields / --map-numeric-fields / --map-facet-fields.",
                unknown_projection.join(", ")
            )));
        }
        let facets = merge_facet_counts(facet_fields, map_facet_fields, &shard_facets)?;
        let ranges = merge_range_counts(range_facet_fields, &shard_ranges)?;
        let stats = merge_column_stats(stats_fields, &shard_stats)?;
        let cardinality = merge_cardinality(cardinality_fields, &shard_distinct)?;
        if let (Some(heap), Some(fingerprint)) = (&stream_heap, scoring_fingerprint.as_ref()) {
            let snapshot = heap
                .lock()
                .expect("BM25 stream heap poisoned")
                .heap
                .iter()
                .map(|entry| (entry.0.vector_id, entry.0.score))
                .collect();
            self.publish_progress(
                crate::pb::QueryStreamPhase::Lexical,
                snapshot,
                fingerprint.clone(),
            );
        }
        if let Some(stream_heap) = stream_heap {
            let winners: Vec<MergedHit> = {
                let state = stream_heap.lock().expect("BM25 stream heap poisoned");
                let mut winners: Vec<MergedHit> = state.heap.iter().map(|entry| entry.0).collect();
                winners.sort_by(cmp_hits);
                winners
            };
            let mut details: HashMap<(u32, u64), Bm25Hit> = all
                .drain(..)
                .map(|(shard, hit)| ((shard, hit.doc_id), hit))
                .collect();
            all = winners
                .into_iter()
                .map(|winner| {
                    let hit = details
                        .remove(&(winner.shard, winner.vector_id))
                        .ok_or_else(|| {
                            Status::data_loss(format!(
                                "BM25 global winner {} from shard {} was absent from its certified local top-k",
                                winner.vector_id, winner.shard
                            ))
                        })?;
                    if hit.score.to_bits() != winner.score.to_bits() {
                        return Err(Status::data_loss(format!(
                            "BM25 global winner {} from shard {} changed score from {:?} in the candidate stream to {:?} in the certified response",
                            winner.vector_id,
                            winner.shard,
                            winner.score,
                            hit.score
                        )));
                    }
                    Ok((winner.shard, hit))
                })
                .collect::<Result<_, _>>()?;
        } else {
            all.sort_by(|(sa, a), (sb, b)| {
                b.score
                    .total_cmp(&a.score)
                    .then_with(|| sa.cmp(sb))
                    .then_with(|| a.doc_id.cmp(&b.doc_id))
            });
            all.truncate(k as usize);
        }
        Ok((
            all.into_iter().map(|(_, h)| h).collect(),
            facets,
            ranges,
            stats,
            cardinality,
        ))
    }

    /// Fused multi-field Bm25Search (`docs/multi-field.md`): `text` is
    /// analyzed once per entry under THAT entry's analysis (term
    /// identity is per field), ONE TermStats round carries every
    /// field's terms, and the Bm25Query fan-out sends per-field legs in
    /// entry order — the pinned accumulation order, identical on every
    /// shard, so distributed fused scores match the monolith's bits.
    pub async fn fanout_bm25_fused(
        &self,
        text: &str,
        k: u32,
        fields: &[crate::pb::QueryField],
        min_score: f32,
    ) -> Result<Vec<Bm25Hit>, Status> {
        self.fanout_bm25_fused_faceted(text, k, fields, min_score, &[], &[], &[], &[], None)
            .await
            .map(|(hits, _, _)| hits)
    }

    /// [`Self::fanout_bm25_fused`] with count-then-rank facets; see
    /// [`Self::fanout_bm25_faceted`] for the facet contract (on a fused
    /// query the match set is the union over every leg's terms).
    #[allow(clippy::too_many_arguments)]
    pub async fn fanout_bm25_fused_faceted(
        &self,
        text: &str,
        k: u32,
        fields: &[crate::pb::QueryField],
        min_score: f32,
        facet_fields: &[String],
        map_facet_fields: &[crate::pb::MapFacetField],
        range_facet_fields: &[crate::pb::RangeFacetField],
        geo_filters: &[crate::pb::GeoFilter],
        filter: Option<&crate::pb::FilterExpr>,
    ) -> Result<FacetedHits, Status> {
        self.fanout_bm25_fused_routed(
            text,
            k,
            fields,
            min_score,
            facet_fields,
            map_facet_fields,
            range_facet_fields,
            geo_filters,
            filter,
        )
        .await
        .map(|(hits, _)| hits)
    }

    /// [`Self::fanout_bm25_fused_faceted`] that also reports which
    /// payload served each field's PhraseMatch (docs/phrase-proximity.md).
    ///
    /// A field with a phrase is analyzed like any other, and its query's
    /// TOKEN ORDER is read off the positioned analysis. The fleet's
    /// capabilities then pick the route from the stats round: a two-term
    /// exact phrase whose bigram column every shard indexes becomes one
    /// term of that column; anything else rides the field's token
    /// positions as a shard-side gate, and only when every shard carries
    /// them. Neither being true refuses by name — the query is never
    /// narrowed to "all terms present" and adjacency is never guessed.
    #[allow(clippy::too_many_arguments)]
    pub async fn fanout_bm25_fused_routed(
        &self,
        text: &str,
        k: u32,
        fields: &[crate::pb::QueryField],
        min_score: f32,
        facet_fields: &[String],
        map_facet_fields: &[crate::pb::MapFacetField],
        range_facet_fields: &[crate::pb::RangeFacetField],
        geo_filters: &[crate::pb::GeoFilter],
        filter: Option<&crate::pb::FilterExpr>,
    ) -> Result<(FacetedHits, Vec<crate::pb::PhraseRouting>), Status> {
        // Same rule as fanout_bm25_faceted: edge-list validation needs
        // no shard, so it runs before the all-legs-empty early return.
        crate::node::validate_range_facet_fields(range_facet_fields)?;
        crate::node::validate_geo_filters(geo_filters)?;
        if let Some(f) = filter {
            crate::filter::validate_filter(f)?;
        }
        // Phase timing, off unless PIPESTREAM_SEARCH_TRACE_BM25 is set.
        // route and the single-field route reach the same node scorer,
        // so when they disagree by orders of magnitude the question is
        // which phase, and that cannot be answered from outside.
        let trace = std::env::var_os("PIPESTREAM_SEARCH_TRACE_BM25").is_some()
            || std::env::var_os("TURBOVEC_TRACE_BM25").is_some();
        let t0 = std::time::Instant::now();
        let addr = self.analysis_addr.clone().ok_or_else(|| {
            Status::unavailable("no analysis backend configured on the coordinator (analysis_addr)")
        })?;
        let mut seen: Vec<&str> = Vec::new();
        for f in fields {
            if f.field.is_empty() {
                return Err(Status::invalid_argument("QueryField.field must be named"));
            }
            if seen.contains(&f.field.as_str()) {
                return Err(Status::invalid_argument(format!(
                    "field {:?} repeats in the query",
                    f.field
                )));
            }
            if f.weight < 0.0 || f.weight.is_nan() {
                return Err(Status::invalid_argument(format!(
                    "field {:?}: weight must be >= 0",
                    f.field
                )));
            }
            seen.push(&f.field);
        }
        // (a) Query analysis per field, each under its own spec. A
        // field with a phrase also needs the query's token ORDER, which
        // the positioned analysis carries (docs/phrase-proximity.md).
        let mut field_terms: Vec<Vec<String>> = Vec::with_capacity(fields.len());
        // Per field: the phrase's term sequence (indexes into that
        // field's terms) and slop, when the field carries a PhraseMatch.
        let mut phrase_requests: Vec<Option<(Vec<usize>, u32)>> = Vec::with_capacity(fields.len());
        for f in fields {
            let analyzed = crate::analyzer::analyze_document(&addr, text, f.analysis.as_ref())
                .await?
                .into_body();
            // The analysis already lists each distinct term once, in
            // first-occurrence order; `remap` keeps the sequence honest
            // should a provider ever repeat one.
            let mut terms: Vec<String> = Vec::new();
            let mut remap: Vec<usize> = Vec::with_capacity(analyzed.terms.len());
            for (term, _, _) in &analyzed.terms {
                let at = match terms.iter().position(|t| t == term) {
                    Some(i) => i,
                    None => {
                        terms.push(term.clone());
                        terms.len() - 1
                    }
                };
                remap.push(at);
            }
            let phrase = match f.phrase.as_ref() {
                Some(m) => {
                    if terms.is_empty() {
                        return Err(Status::invalid_argument(format!(
                            "field {:?}: the phrase text analyzed to no terms; there is no \
                             window to match",
                            f.field
                        )));
                    }
                    let Some(positions) = analyzed.positions.as_ref() else {
                        return Err(Status::failed_precondition(format!(
                            "field {:?}: the query analysis carried no token positions, so the \
                             phrase's token order cannot be established; the analysis backend \
                             must return its token layer",
                            f.field
                        )));
                    };
                    let sequence = crate::proximity::query_sequence(&analyzed.terms, positions)
                        .map_err(|error| Status::internal(format!("query positions: {error}")))?;
                    Some((
                        sequence.into_iter().map(|ti| remap[ti]).collect::<Vec<_>>(),
                        m.slop,
                    ))
                }
                None => None,
            };
            field_terms.push(terms);
            phrase_requests.push(phrase);
        }
        let t_analyzed = t0.elapsed();
        if k == 0 || field_terms.iter().all(|t| t.is_empty()) {
            return Ok(((Vec::new(), Vec::new(), Vec::new()), Vec::new()));
        }
        // (b) every field's stats, served from the per-node cache, plus
        // one PROBE per two-term exact phrase for its bigram column: the
        // route is decided from what the fleet answers, and a column no
        // shard has is an answer, not a typo.
        let mut stats_fields: Vec<crate::pb::FieldTerms> = fields
            .iter()
            .zip(&field_terms)
            .map(|(f, terms)| crate::pb::FieldTerms {
                field: f.field.clone(),
                terms: terms.clone(),
            })
            .collect();
        let probe_from = stats_fields.len();
        let mut bigram_probe: Vec<Option<usize>> = vec![None; fields.len()];
        for (fi, f) in fields.iter().enumerate() {
            if let Some((sequence, 0)) = phrase_requests[fi].as_ref().map(|(s, slop)| (s, *slop)) {
                if sequence.len() == 2 {
                    let bigram = crate::proximity::bigram_term(
                        &field_terms[fi][sequence[0]],
                        &field_terms[fi][sequence[1]],
                    );
                    bigram_probe[fi] = Some(stats_fields.len());
                    stats_fields.push(crate::pb::FieldTerms {
                        field: crate::proximity::bigram_field_name(&f.field),
                        terms: vec![bigram],
                    });
                }
            }
        }
        // (c)+(d) run as a round so a stale-stats refusal can rerun
        // them once against fresh stats with no claim.
        let n_shards = self.node_addrs.len();
        let mut fresh = false;
        loop {
            let globals = self
                .fused_stats_probing(&stats_fields, fresh, probe_from)
                .await?;
            let claims = if fresh {
                vec![0; globals.epochs.len()]
            } else {
                globals.epochs.clone()
            };
            let t_stats = t0.elapsed();
            // Resolve every field's route against what the fleet
            // answered, producing the leg list the round scores.
            let mut resolved_fields: Vec<crate::pb::QueryField> = Vec::with_capacity(fields.len());
            let mut resolved_terms: Vec<Vec<String>> = Vec::with_capacity(fields.len());
            let mut resolved = FusedGlobals {
                doc_count: globals.doc_count,
                totals: Vec::with_capacity(fields.len()),
                dfs: Vec::with_capacity(fields.len()),
                epochs: globals.epochs.clone(),
                known_shards: Vec::with_capacity(fields.len()),
                positions_shards: Vec::with_capacity(fields.len()),
            };
            let mut phrase_legs: Vec<Option<crate::pb::PhraseLeg>> =
                Vec::with_capacity(fields.len());
            let mut fingerprints: Vec<u64> = Vec::with_capacity(fields.len());
            let mut routing: Vec<crate::pb::PhraseRouting> = Vec::new();
            for (fi, f) in fields.iter().enumerate() {
                let base_fingerprint = crate::analyzer::analysis_fingerprint(f.analysis.as_ref());
                let mut take = |source: usize| {
                    resolved.totals.push(globals.totals[source]);
                    resolved.dfs.push(globals.dfs[source].clone());
                    resolved.known_shards.push(globals.known_shards[source]);
                    resolved
                        .positions_shards
                        .push(globals.positions_shards[source]);
                };
                match phrase_requests[fi].as_ref() {
                    // A one-term "phrase" is the ordinary term query and
                    // constrains nothing, so it reports no routing.
                    Some((sequence, slop)) if sequence.len() >= 2 => {
                        let bigram_everywhere =
                            bigram_probe[fi].is_some_and(|pi| globals.known_shards[pi] == n_shards);
                        let positions_everywhere = globals.known_shards[fi] == n_shards
                            && globals.positions_shards[fi] == n_shards;
                        match crate::proximity::choose_route(
                            &f.field,
                            sequence.len(),
                            *slop,
                            bigram_everywhere,
                            positions_everywhere,
                        ) {
                            Ok(crate::proximity::PhraseRoute::BigramColumn(column)) => {
                                let pi = bigram_probe[fi].expect("bigram route implies a probe");
                                resolved_fields.push(crate::pb::QueryField {
                                    field: column.clone(),
                                    phrase: None,
                                    ..f.clone()
                                });
                                resolved_terms.push(stats_fields[pi].terms.clone());
                                take(pi);
                                phrase_legs.push(None);
                                fingerprints
                                    .push(crate::proximity::bigram_fingerprint(base_fingerprint));
                                routing.push(crate::pb::PhraseRouting {
                                    field: f.field.clone(),
                                    served_field: column,
                                    bigram_column: true,
                                    slop: 0,
                                });
                            }
                            Ok(crate::proximity::PhraseRoute::Positions) => {
                                resolved_fields.push(f.clone());
                                resolved_terms.push(field_terms[fi].clone());
                                take(fi);
                                phrase_legs.push(Some(crate::pb::PhraseLeg {
                                    sequence: sequence.iter().map(|&i| i as u32).collect(),
                                    slop: *slop,
                                }));
                                fingerprints.push(base_fingerprint);
                                routing.push(crate::pb::PhraseRouting {
                                    field: f.field.clone(),
                                    served_field: f.field.clone(),
                                    bigram_column: false,
                                    slop: *slop,
                                });
                            }
                            Err(reason) => return Err(Status::invalid_argument(reason)),
                        }
                    }
                    _ => {
                        resolved_fields.push(crate::pb::QueryField {
                            phrase: None,
                            ..f.clone()
                        });
                        resolved_terms.push(field_terms[fi].clone());
                        take(fi);
                        phrase_legs.push(None);
                        fingerprints.push(base_fingerprint);
                    }
                }
            }
            match self
                .bm25_fused_round(
                    k,
                    &resolved_fields,
                    &resolved_terms,
                    &resolved,
                    &claims,
                    None,
                    &phrase_legs,
                    &fingerprints,
                    min_score,
                    facet_fields,
                    map_facet_fields,
                    range_facet_fields,
                    geo_filters,
                    filter,
                    trace,
                    t0,
                    t_analyzed,
                    t_stats,
                )
                .await
            {
                Err(e) if !fresh && is_stale_stats(&e) => {
                    self.stats_cache.invalidate_all();
                    fresh = true;
                }
                other => return other.map(|hits| (hits, routing)),
            }
        }
    }

    /// Phrase-aware BM25 orchestration. Ordinary fields are analyzed exactly
    /// as on the fused route; the final field is populated directly from the
    /// product glossary and scored as a max-group on every shard.
    async fn fanout_phrase(
        &self,
        base: &crate::pb::Bm25SearchRequest,
        k: u32,
        weight_per_token: f32,
        max_weight: f32,
        filter: Option<&crate::pb::FilterExpr>,
    ) -> Result<FacetedHits, Status> {
        crate::node::validate_range_facet_fields(&base.range_facet_fields)?;
        crate::node::validate_geo_filters(&base.geo_filters)?;
        if let Some(filter) = filter {
            crate::filter::validate_filter(filter)?;
        }
        let phrase_index = self.phrase_index.as_ref().ok_or_else(|| {
            Status::failed_precondition("this coordinator has no phrase glossary configured")
        })?;
        if !weight_per_token.is_finite() || weight_per_token <= 0.0 {
            return Err(Status::invalid_argument(
                "phrase weight_per_token must be finite and greater than zero",
            ));
        }
        if !max_weight.is_finite() || max_weight <= 0.0 {
            return Err(Status::invalid_argument(
                "phrase max_weight must be finite and greater than zero",
            ));
        }
        let mut fields = if base.fields.is_empty() {
            vec![crate::pb::QueryField {
                field: "body".to_string(),
                analysis: base.analysis.clone(),
                weight: 1.0,
                k1: 0.0,
                b: 0.0,
                phrase: None,
            }]
        } else {
            if base.analysis.is_some() {
                return Err(Status::invalid_argument(
                    "PhraseSearch base.analysis is ignored when base.fields is set; move the spec onto each QueryField.analysis",
                ));
            }
            base.fields.clone()
        };
        let mut seen = std::collections::HashSet::new();
        for field in &fields {
            if field.field.is_empty() || !seen.insert(field.field.as_str()) {
                return Err(Status::invalid_argument(
                    "phrase search base fields must be named and unique",
                ));
            }
            if field.field == phrase_index.phrase_field() {
                return Err(Status::invalid_argument(format!(
                    "phrase field {:?} is derived by PhraseSearch and must not appear in base.fields",
                    phrase_index.phrase_field()
                )));
            }
            if field.weight < 0.0 || field.weight.is_nan() {
                return Err(Status::invalid_argument(format!(
                    "field {:?}: weight must be non-negative",
                    field.field
                )));
            }
        }
        let addr = self.analysis_addr.clone().ok_or_else(|| {
            Status::unavailable("no analysis backend configured on the coordinator (analysis_addr)")
        })?;
        let trace = std::env::var_os("PIPESTREAM_SEARCH_TRACE_BM25").is_some()
            || std::env::var_os("TURBOVEC_TRACE_BM25").is_some();
        let t0 = std::time::Instant::now();
        let mut field_terms = Vec::with_capacity(fields.len() + 1);
        for field in &fields {
            let analyzed =
                crate::analyzer::analyze_document(&addr, &base.text, field.analysis.as_ref())
                    .await?;
            let mut terms = Vec::new();
            for (term, _, _) in analyzed.into_body().terms {
                if !terms.contains(&term) {
                    terms.push(term);
                }
            }
            field_terms.push(terms);
        }
        let phrase_pairs = phrase_index.query_terms(&base.text);
        let phrase_terms: Vec<String> = phrase_pairs.iter().map(|(term, _)| term.clone()).collect();
        let phrase_weights: Vec<f32> = phrase_pairs
            .iter()
            .map(|(_, token_count)| ((*token_count as f32) * weight_per_token).min(max_weight))
            .collect();
        let phrase_leg = fields.len();
        fields.push(crate::pb::QueryField {
            field: phrase_index.phrase_field().to_string(),
            analysis: None,
            weight: 1.0,
            k1: 0.0,
            b: 0.0,
            phrase: None,
        });
        field_terms.push(phrase_terms);
        if k == 0 || field_terms.iter().all(Vec::is_empty) {
            return Ok((Vec::new(), Vec::new(), Vec::new()));
        }
        let t_analyzed = t0.elapsed();
        let stats_fields: Vec<crate::pb::FieldTerms> = fields
            .iter()
            .zip(&field_terms)
            .map(|(field, terms)| crate::pb::FieldTerms {
                field: field.field.clone(),
                terms: terms.clone(),
            })
            .collect();
        let mut fresh = false;
        loop {
            let globals = self.fused_stats(&stats_fields, fresh).await?;
            if globals.known_shards[phrase_leg] != self.node_addrs.len() {
                return Err(Status::failed_precondition(format!(
                    "phrase field {:?} exists on only {}/{} shards; phrase search requires a complete rebuilt generation",
                    phrase_index.phrase_field(),
                    globals.known_shards[phrase_leg],
                    self.node_addrs.len()
                )));
            }
            let claims = if fresh {
                vec![0; globals.epochs.len()]
            } else {
                globals.epochs.clone()
            };
            let t_stats = t0.elapsed();
            let fingerprints: Vec<u64> = fields
                .iter()
                .enumerate()
                .map(|(fi, f)| {
                    if fi == phrase_leg {
                        phrase_index.fingerprint()
                    } else {
                        crate::analyzer::analysis_fingerprint(f.analysis.as_ref())
                    }
                })
                .collect();
            let phrase_legs: Vec<Option<crate::pb::PhraseLeg>> = vec![None; fields.len()];
            let round = self
                .bm25_fused_round(
                    k,
                    &fields,
                    &field_terms,
                    &globals,
                    &claims,
                    Some((phrase_leg, &phrase_weights, phrase_index.fingerprint())),
                    &phrase_legs,
                    &fingerprints,
                    base.min_score,
                    &base.facet_fields,
                    &base.map_facet_fields,
                    &base.range_facet_fields,
                    &base.geo_filters,
                    filter,
                    trace,
                    t0,
                    t_analyzed,
                    t_stats,
                )
                .await;
            match round {
                Err(error) if !fresh && is_stale_stats(&error) => {
                    self.stats_cache.invalidate_all();
                    fresh = true;
                }
                other => return other,
            }
        }
    }

    /// Per-field global stats for a fused query, merged over per-node
    /// shares served from the stats cache (`fresh` bypasses it; see
    /// [`Self::body_stats`]). Shares merge elementwise per field, N
    /// summed once (it is shared — a document is a document).
    ///
    /// A partially-known field is tolerated: that is a real
    /// heterogeneous fleet, and the shards that have it still
    /// contribute. A field NO shard has is a typo, not a query —
    /// scoring it as "contributes nothing" would silently return the
    /// ranking of the REMAINING fields, so a misspelled arm of an A/B
    /// reads as "no difference". Refused instead.
    async fn fused_stats(
        &self,
        stats_fields: &[crate::pb::FieldTerms],
        fresh: bool,
    ) -> Result<FusedGlobals, Status> {
        self.fused_stats_probing(stats_fields, fresh, stats_fields.len())
            .await
    }

    /// [`Self::fused_stats`] where entries from `probe_from` on are
    /// PROBES: fields the query may route onto if the fleet has them
    /// (a phrase's bigram column, docs/phrase-proximity.md). A probe no
    /// shard knows is answered, not refused — the typo rule still
    /// applies to every entry before `probe_from`, which the caller
    /// named on purpose.
    async fn fused_stats_probing(
        &self,
        stats_fields: &[crate::pb::FieldTerms],
        fresh: bool,
        probe_from: usize,
    ) -> Result<FusedGlobals, Status> {
        let n = self.node_addrs.len();
        let mut shares: Vec<Option<crate::stats_cache::FusedShare>> = vec![None; n];
        if !fresh {
            for (i, share) in shares.iter_mut().enumerate() {
                *share = self.stats_cache.lookup_fused(i, stats_fields);
            }
        }
        let mut fetch_tasks = Vec::new();
        for (i, share) in shares.iter().enumerate() {
            if share.is_some() {
                continue;
            }
            let request = TermStatsRequest {
                terms: Vec::new(),
                fields: stats_fields.to_vec(),
            };
            let mut client = self.node_client(&self.node_addrs[i])?;
            self.stats_cache.note_fetch();
            fetch_tasks.push((
                i,
                tokio::spawn(
                    async move { client.term_stats(request).await.map(|r| r.into_inner()) },
                ),
            ));
        }
        for (i, task) in fetch_tasks {
            let resp = task
                .await
                .map_err(|e| Status::internal(format!("term stats task failed: {e}")))??;
            if resp.field_stats.len() != stats_fields.len() {
                return Err(Status::internal(format!(
                    "shard returned {} field stats for {} fields",
                    resp.field_stats.len(),
                    stats_fields.len()
                )));
            }
            for (ft, fs) in stats_fields.iter().zip(&resp.field_stats) {
                if fs.doc_frequencies.len() != ft.terms.len() {
                    return Err(Status::internal("shard field stats df length mismatch"));
                }
            }
            self.stats_cache.store(i, &[], stats_fields, &resp);
            shares[i] = Some(crate::stats_cache::FusedShare {
                epoch: resp.stats_epoch,
                doc_count: resp.doc_count,
                fields: resp
                    .field_stats
                    .iter()
                    .map(|fs| crate::stats_cache::FusedFieldShare {
                        total_doc_length: fs.total_doc_length,
                        known: fs.known,
                        positions: fs.positions,
                        dfs: fs.doc_frequencies.clone(),
                    })
                    .collect(),
            });
        }
        let mut doc_count = 0u64;
        let mut totals = vec![0u64; stats_fields.len()];
        let mut dfs: Vec<Vec<u32>> = stats_fields
            .iter()
            .map(|ft| vec![0u32; ft.terms.len()])
            .collect();
        let mut known_somewhere = vec![false; stats_fields.len()];
        let mut known_shards = vec![0usize; stats_fields.len()];
        let mut positions_shards = vec![0usize; stats_fields.len()];
        let mut epochs = Vec::with_capacity(n);
        for share in shares {
            let s = share.expect("looked up or fetched above");
            doc_count += s.doc_count;
            for (fi, fs) in s.fields.iter().enumerate() {
                totals[fi] += fs.total_doc_length;
                known_somewhere[fi] |= fs.known;
                known_shards[fi] += usize::from(fs.known);
                positions_shards[fi] += usize::from(fs.positions);
                for (acc, df) in dfs[fi].iter_mut().zip(&fs.dfs) {
                    *acc += df;
                }
            }
            epochs.push(s.epoch);
        }
        let unknown: Vec<&str> = stats_fields
            .iter()
            .zip(&known_somewhere)
            .take(probe_from)
            .filter(|(_, known)| !**known)
            .map(|(f, _)| f.field.as_str())
            .collect();
        if !unknown.is_empty() {
            return Err(Status::invalid_argument(format!(
                "no shard indexes {}: scoring an unknown field would silently return the \
                 remaining fields' ranking. Check the spelling, or the nodes' --bm25-fields.",
                unknown
                    .iter()
                    .map(|f| format!("{f:?}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }
        Ok(FusedGlobals {
            doc_count,
            totals,
            dfs,
            epochs,
            known_shards,
            positions_shards,
        })
    }

    /// One fused Bm25Query fan-out: phases (c) and (d) of
    /// [`Self::fanout_bm25_fused`]. `claims[shard]` travels as that
    /// shard's `expected_stats_epoch`.
    #[allow(clippy::too_many_arguments)]
    async fn bm25_fused_round(
        &self,
        k: u32,
        fields: &[crate::pb::QueryField],
        field_terms: &[Vec<String>],
        globals: &FusedGlobals,
        claims: &[u64],
        // `(field index, parallel term weights, vocabulary fingerprint)`.
        phrase: Option<(usize, &[f32], u64)>,
        // Per field: the positional phrase constraint the shard applies
        // at its heap gate (docs/phrase-proximity.md), `None` on an
        // ordinary leg or one already rewritten onto a bigram column.
        phrase_legs: &[Option<crate::pb::PhraseLeg>],
        // Per field: the analyzer fingerprint the shard is told its
        // terms came from (a bigram column's is derived from its
        // source's; a glossary field's is the vocabulary's).
        fingerprints: &[u64],
        min_score: f32,
        facet_fields: &[String],
        map_facet_fields: &[crate::pb::MapFacetField],
        range_facet_fields: &[crate::pb::RangeFacetField],
        geo_filters: &[crate::pb::GeoFilter],
        filter: Option<&crate::pb::FilterExpr>,
        trace: bool,
        t0: std::time::Instant,
        t_analyzed: std::time::Duration,
        t_stats: std::time::Duration,
    ) -> Result<FacetedHits, Status> {
        let doc_count = globals.doc_count;
        let totals = &globals.totals;
        let dfs = &globals.dfs;
        // (c) Bm25Query fan-out with per-field legs in entry order.
        // Entry k1/b of 0 pick up the coordinator's configured params,
        // so tuning reaches this path too.
        let legs: Vec<crate::pb::Bm25FieldLeg> = fields
            .iter()
            .enumerate()
            .map(|(fi, f)| crate::pb::Bm25FieldLeg {
                field: f.field.clone(),
                terms: field_terms[fi].clone(),
                global_total_doc_length: totals[fi],
                global_doc_frequencies: dfs[fi].clone(),
                weight: f.weight,
                k1: if f.k1 == 0.0 {
                    self.bm25_params.k1 as f32
                } else {
                    f.k1
                },
                b: if f.b == 0.0 {
                    self.bm25_params.b as f32
                } else {
                    f.b
                },
                // Declare which analyzer produced these terms so the
                // shard can refuse a column built under a different one.
                // The caller computed it from THE SPEC ACTUALLY USED,
                // not from what the caller meant.
                analysis_fingerprint: fingerprints[fi],
                phrase: phrase_legs[fi].clone(),
            })
            .collect();
        debug_assert_eq!(phrase_legs.len(), fields.len());
        debug_assert_eq!(fingerprints.len(), fields.len());
        let _ = phrase.map(|(leg, _, _)| leg);
        if trace {
            for l in &legs {
                eprintln!(
                    "bm25-fused leg: field={:?} terms={:?} dfs={:?} total_len={} w={} k1={} b={} \
                     | req k={k} N={doc_count} min_score={min_score}",
                    l.field,
                    l.terms,
                    l.global_doc_frequencies,
                    l.global_total_doc_length,
                    l.weight,
                    l.k1,
                    l.b
                );
            }
        }
        let mut query_tasks = Vec::with_capacity(self.node_addrs.len());
        let stream_enabled = self.bm25_stream && phrase.is_none();
        let relay = stream_enabled
            .then(|| watch::channel(min_score))
            .map(|(tx, rx)| (Arc::new(tx), rx));
        let stream_heap = stream_enabled.then(|| {
            Arc::new(Mutex::new(Bm25StreamHeap {
                heap: std::collections::BinaryHeap::with_capacity(k as usize + 1),
                floors_sent: 0,
                progress: self.query_progress.clone(),
            }))
        });
        for (shard, node) in self.node_addrs.iter().enumerate() {
            let request = Bm25QueryRequest {
                projections: Vec::new(),
                terms: Vec::new(),
                k,
                global_doc_count: doc_count,
                global_total_doc_length: 0,
                global_doc_frequencies: Vec::new(),
                k1: 0.0,
                b: 0.0,
                min_score,
                fields: legs.clone(),
                expected_stats_epoch: claims[shard],
                facet_fields: facet_fields.to_vec(),
                map_facet_fields: map_facet_fields.to_vec(),
                range_facet_fields: range_facet_fields.to_vec(),
                // The fused route does not carry score stages yet; the
                // public handler refuses the combination.
                score_stages: Vec::new(),
                geo_filters: geo_filters.to_vec(),
                filter: filter.cloned(),
                stats_fields: Vec::new(),
                cardinality_fields: Vec::new(),
                phrase: None,
            };
            let mut client = self.node_client(node)?;
            let phrase_request =
                phrase.map(
                    |(phrase_leg, weights, _)| crate::pb::Bm25PhraseQueryRequest {
                        query: Some(request.clone()),
                        phrase_leg: phrase_leg as u32,
                        phrase_term_weights: weights.to_vec(),
                    },
                );
            if let Some((floor_tx, floor_rx)) = relay.clone() {
                let global_heap = Arc::clone(stream_heap.as_ref().expect("relay has heap"));
                let deadline = self.limits.shard_deadline;
                query_tasks.push(tokio::spawn(async move {
                    let started = std::time::Instant::now();
                    let run = stream_bm25_shard(
                        shard as u32,
                        k as usize,
                        client,
                        request,
                        floor_tx,
                        floor_rx,
                        global_heap,
                    );
                    let result = match deadline {
                        Some(limit) => tokio::time::timeout(limit, run).await.map_err(|_| {
                            Status::deadline_exceeded(format!(
                                "BM25 shard {shard} exceeded its {}ms deadline",
                                limit.as_millis()
                            ))
                        })?,
                        None => run.await,
                    }?;
                    let response = result.response;
                    Ok::<_, Status>((
                        shard as u32,
                        response.hits,
                        response.facets,
                        response.range_facets,
                        response.geo_columns_known,
                        response.filter_columns_known,
                        started.elapsed().as_secs_f64() * 1000.0,
                        Some(result.scoring_fingerprint),
                    ))
                }));
                continue;
            }
            query_tasks.push(tokio::spawn(async move {
                let started = std::time::Instant::now();
                let response = match phrase_request {
                    Some(request) => client.bm25_phrase_query(request).await,
                    None => client.bm25_query(request).await,
                };
                response.map(|r| {
                    let r = r.into_inner();
                    (
                        shard as u32,
                        r.hits,
                        r.facets,
                        r.range_facets,
                        r.geo_columns_known,
                        r.filter_columns_known,
                        started.elapsed().as_secs_f64() * 1000.0,
                        None,
                    )
                })
            }));
        }
        let mut all: Vec<(u32, Bm25Hit)> = Vec::new();
        let mut shard_facets: Vec<Vec<crate::pb::FacetFieldCounts>> = Vec::new();
        let mut shard_ranges: Vec<Vec<crate::pb::RangeFacetCounts>> = Vec::new();
        let mut per_shard: Vec<(u32, f64)> = Vec::new();
        let mut geo_known = vec![false; geo_filters.len()];
        let filter_leaves = filter.map_or(0, crate::filter::leaf_count);
        let mut filter_known = vec![false; filter_leaves];
        let mut scoring_fingerprint: Option<String> = None;
        for task in query_tasks {
            let (shard, hits, facets, ranges, geo, fknown, ms, fingerprint) = task
                .await
                .map_err(|e| Status::internal(format!("bm25 query task failed: {e}")))??;
            if let Some(fingerprint) = fingerprint {
                match scoring_fingerprint.as_ref() {
                    Some(expected) if expected != &fingerprint => {
                        return Err(Status::failed_precondition(format!(
                            "BM25 shard {shard} scoring fingerprint {fingerprint} differs from {expected}"
                        )));
                    }
                    None => scoring_fingerprint = Some(fingerprint),
                    _ => {}
                }
            }
            per_shard.push((shard, ms));
            all.extend(hits.into_iter().map(|h| (shard, h)));
            shard_facets.push(facets);
            shard_ranges.push(ranges);
            if geo.len() != geo_filters.len() {
                return Err(Status::internal(format!(
                    "shard answered {} geo-column flags for {} filters",
                    geo.len(),
                    geo_filters.len()
                )));
            }
            for (acc, k) in geo_known.iter_mut().zip(&geo) {
                *acc |= *k;
            }
            if fknown.len() != filter_leaves {
                return Err(Status::internal(format!(
                    "shard answered {} filter-leaf flags for {} leaves",
                    fknown.len(),
                    filter_leaves
                )));
            }
            for (acc, k) in filter_known.iter_mut().zip(&fknown) {
                *acc |= *k;
            }
        }
        refuse_unknown_geo_columns(geo_filters, &geo_known)?;
        refuse_unknown_filter_leaves(filter, &filter_known)?;
        let facets = merge_facet_counts(facet_fields, map_facet_fields, &shard_facets)?;
        let ranges = merge_range_counts(range_facet_fields, &shard_ranges)?;
        if let (Some(heap), Some(fingerprint)) = (&stream_heap, scoring_fingerprint.as_ref()) {
            let snapshot = heap
                .lock()
                .expect("BM25 stream heap poisoned")
                .heap
                .iter()
                .map(|entry| (entry.0.vector_id, entry.0.score))
                .collect();
            self.publish_progress(
                crate::pb::QueryStreamPhase::Lexical,
                snapshot,
                fingerprint.clone(),
            );
        }
        let t_query = t0.elapsed();
        if trace {
            eprintln!(
                "bm25-fused per-shard ms: {}",
                per_shard
                    .iter()
                    .map(|(s, ms)| format!("{s}:{ms:.0}"))
                    .collect::<Vec<_>>()
                    .join(" ")
            );
        }
        if let Some(stream_heap) = stream_heap {
            let winners: Vec<MergedHit> = {
                let state = stream_heap.lock().expect("BM25 stream heap poisoned");
                let mut winners: Vec<MergedHit> = state.heap.iter().map(|entry| entry.0).collect();
                winners.sort_by(cmp_hits);
                winners
            };
            let mut details: HashMap<(u32, u64), Bm25Hit> = all
                .drain(..)
                .map(|(shard, hit)| ((shard, hit.doc_id), hit))
                .collect();
            all = winners
                .into_iter()
                .map(|winner| {
                    let hit = details
                        .remove(&(winner.shard, winner.vector_id))
                        .ok_or_else(|| {
                            Status::data_loss(format!(
                                "BM25 global winner {} from shard {} was absent from its certified local top-k",
                                winner.vector_id, winner.shard
                            ))
                        })?;
                    if hit.score.to_bits() != winner.score.to_bits() {
                        return Err(Status::data_loss(format!(
                            "BM25 global winner {} from shard {} changed score from {:?} in the candidate stream to {:?} in the certified response",
                            winner.vector_id,
                            winner.shard,
                            winner.score,
                            hit.score
                        )));
                    }
                    Ok((winner.shard, hit))
                })
                .collect::<Result<_, _>>()?;
        } else {
            all.sort_by(|(sa, a), (sb, b)| {
                b.score
                    .total_cmp(&a.score)
                    .then_with(|| sa.cmp(sb))
                    .then_with(|| a.doc_id.cmp(&b.doc_id))
            });
            all.truncate(k as usize);
        }
        if trace {
            let ms = |d: std::time::Duration| d.as_secs_f64() * 1000.0;
            eprintln!(
                "bm25-fused: analyze {:.1} ms, term-stats {:.1} ms, query fan-out {:.1} ms, \
                 merge {:.1} ms  ({} fields, {} terms)",
                ms(t_analyzed),
                ms(t_stats - t_analyzed),
                ms(t_query - t_stats),
                ms(t0.elapsed() - t_query),
                fields.len(),
                field_terms.iter().map(Vec::len).sum::<usize>(),
            );
        }
        Ok((all.into_iter().map(|(_, h)| h).collect(), facets, ranges))
    }

    /// Hybrid vector + BM25 search:
    ///
    /// 1. analyze `text` into query terms (same analysis options as
    ///    ingest, as in [`Self::fanout_bm25`]);
    /// 2. TermStats fan-out and merge into GLOBAL corpus stats;
    /// 3. fuse per `legs.fusion_mode`: GLOBAL_RANK and SCORE_BLEND fetch
    ///    raw shard legs and fuse once on the coordinator (RRF over
    ///    global ranks, or normalize-and-combine over global scores);
    ///    TWO_LEVEL lets each shard RRF-fuse locally and RRF-merges the
    ///    shard lists (the fallback for incomparable scores).
    #[allow(clippy::too_many_arguments)]
    pub async fn fanout_hybrid(
        &self,
        request_id: &str,
        text: &str,
        vector: &[f32],
        k: u32,
        spec: Option<&crate::pb::AnalysisSpec>,
        legs: HybridLegs,
        debug: bool,
        filters: &RequestFilters,
    ) -> Result<(Vec<HybridHit>, Option<HybridDebug>), Status> {
        if k == 0 || vector.is_empty() {
            return Ok((Vec::new(), None));
        }
        let t_total = std::time::Instant::now();
        // Query analysis for the BM25 leg (same options as ingest).
        let addr = self.analysis_addr.clone().ok_or_else(|| {
            Status::unavailable("no analysis backend configured on the coordinator (analysis_addr)")
        })?;
        let t = std::time::Instant::now();
        let analyzed = crate::analyzer::analyze_document(&addr, text, spec).await?;
        let analysis_ms = t.elapsed().as_secs_f32() * 1e3;
        let mut terms: Vec<String> = Vec::new();
        for (term, _, _) in analyzed.into_body().terms {
            if !terms.contains(&term) {
                terms.push(term);
            }
        }

        let t = std::time::Instant::now();
        // Stats + fusion run as a round: a stale-stats refusal from any
        // shard reruns them once against fresh stats with no claim.
        let mut fresh = false;
        let (hits, mut dbg, stats_ms) = loop {
            let (global, epochs) = self.body_stats(&terms, fresh).await?;
            let claims = if fresh { vec![0; epochs.len()] } else { epochs };
            let stats_ms = t.elapsed().as_secs_f32() * 1e3;
            let round = match legs.fusion_mode {
                FusionMode::TwoLevel => {
                    self.fanout_hybrid_two_level(
                        request_id, vector, k, &terms, &global, &claims, legs, debug, filters,
                    )
                    .await
                }
                FusionMode::Decomposed => {
                    self.fanout_hybrid_decomposed(
                        request_id, vector, k, &terms, &global, &claims, legs, debug, filters,
                    )
                    .await
                }
                _ => {
                    self.fanout_hybrid_global_rank(
                        request_id, vector, k, &terms, &global, &claims, legs, debug, filters,
                    )
                    .await
                }
            };
            match round {
                Err(e) if !fresh && is_stale_stats(&e) => {
                    self.stats_cache.invalidate_all();
                    fresh = true;
                }
                Err(e) => return Err(e),
                Ok((hits, dbg)) => break (hits, dbg, stats_ms),
            }
        };
        if let Some(d) = dbg.as_mut() {
            d.terms = terms;
            d.analysis_ms = analysis_ms;
            d.stats_ms = stats_ms;
            d.total_ms = t_total.elapsed().as_secs_f32() * 1e3;
        }
        Ok((hits, dbg))
    }

    /// GLOBAL BM25 corpus stats for `terms`, summed over per-node shares
    /// served from the stats cache wherever the cached epoch still
    /// stands; only nodes with a missing or incomplete share are asked.
    /// `fresh` bypasses the cache entirely — the retry path after a
    /// shard refused an epoch claim, which is today's uncached
    /// semantics. Also returns the per-node epochs the shares were
    /// valid at, for stamping onto the scoring requests as
    /// `expected_stats_epoch` (the enforcement that makes caching sound;
    /// see src/stats_cache.rs).
    async fn body_stats(
        &self,
        terms: &[String],
        fresh: bool,
    ) -> Result<(CorpusStats, Vec<u64>), Status> {
        let n = self.node_addrs.len();
        let mut shares: Vec<Option<crate::stats_cache::BodyShare>> = vec![None; n];
        if !fresh {
            for (i, share) in shares.iter_mut().enumerate() {
                *share = self.stats_cache.lookup_body(i, terms);
            }
        }
        let mut fetch_tasks = Vec::new();
        for (i, share) in shares.iter().enumerate() {
            if share.is_some() {
                continue;
            }
            let terms_owned = terms.to_vec();
            let mut client = self.node_client(&self.node_addrs[i])?;
            self.stats_cache.note_fetch();
            fetch_tasks.push((
                i,
                tokio::spawn(async move {
                    client
                        .term_stats(TermStatsRequest {
                            terms: terms_owned,
                            fields: Vec::new(),
                        })
                        .await
                        .map(|r| r.into_inner())
                }),
            ));
        }
        for (i, task) in fetch_tasks {
            let resp = task
                .await
                .map_err(|e| Status::internal(format!("term stats task failed: {e}")))??;
            if resp.doc_frequencies.len() != terms.len() {
                return Err(Status::internal("shard stats df length mismatch"));
            }
            self.stats_cache.store(i, terms, &[], &resp);
            shares[i] = Some(crate::stats_cache::BodyShare {
                epoch: resp.stats_epoch,
                doc_count: resp.doc_count,
                total_doc_length: resp.total_doc_length,
                dfs: resp.doc_frequencies,
            });
        }
        let mut global = CorpusStats {
            doc_count: 0,
            total_doc_length: 0,
            dfs: vec![0; terms.len()],
        };
        let mut epochs = Vec::with_capacity(n);
        for share in shares {
            let s = share.expect("looked up or fetched above");
            global.doc_count += s.doc_count;
            global.total_doc_length += s.total_doc_length;
            for (acc, df) in global.dfs.iter_mut().zip(&s.dfs) {
                *acc += df;
            }
            epochs.push(s.epoch);
        }
        Ok((global, epochs))
    }

    #[allow(clippy::too_many_arguments)]
    async fn clustered_hybrid_global_rank(
        &self,
        request_id: &str,
        vector: &[f32],
        k: u32,
        terms: &[String],
        global: &CorpusStats,
        claims: &[u64],
        legs: HybridLegs,
        debug: bool,
        filters: &RequestFilters,
    ) -> Result<(Vec<HybridHit>, Option<HybridDebug>), Status> {
        let t_legs = std::time::Instant::now();
        let ranges = self.product_label_ranges().await?;
        let vector_legs = if legs.vector_weight == 0.0 {
            vec![Vec::new(); self.node_addrs.len()]
        } else {
            self.clustered_local_vector_legs(request_id, vector, legs.leg_k, filters, &ranges)
                .await?
        };
        let mut owner: HashMap<u64, u32> = HashMap::new();
        let mut vector_counts = vec![0u32; self.node_addrs.len()];
        let mut vector_shards = Vec::with_capacity(vector_legs.len());
        for (shard, hits) in vector_legs.into_iter().enumerate() {
            vector_counts[shard] = u32::try_from(hits.len()).unwrap_or(u32::MAX);
            for &(doc_id, _) in &hits {
                owner.insert(doc_id, shard as u32);
            }
            vector_shards.push((shard as u32, hits));
        }
        let mut vector_global = fusion::merge_legs_by_score(vector_shards);

        let (_, leg_terms, leg_dfs) = leg_payloads(vector, terms, global, legs);
        let mut shard_tasks = Vec::with_capacity(self.node_addrs.len());
        for (shard, node) in self.node_addrs.iter().enumerate() {
            let request = ShardLegsRequest {
                request_id: request_id.to_string(),
                k: legs.leg_k,
                vector: Vec::new(),
                terms: leg_terms.clone(),
                global_doc_count: global.doc_count,
                global_total_doc_length: global.total_doc_length,
                global_doc_frequencies: leg_dfs.clone(),
                k1: self.bm25_params.k1 as f32,
                b: self.bm25_params.b as f32,
                expected_stats_epoch: claims[shard],
                geo_filters: filters.geo.clone(),
                filter: filters.tree.clone(),
            };
            let mut client = self.node_client(node)?;
            shard_tasks.push(tokio::spawn(async move {
                let started = std::time::Instant::now();
                client
                    .shard_legs(request)
                    .await
                    .map(|response| (shard as u32, started.elapsed(), response.into_inner()))
            }));
        }
        let mut bm25_shards = Vec::with_capacity(shard_tasks.len());
        let mut shard_debug = Vec::new();
        let mut known = FilterKnown::new(filters);
        for task in shard_tasks {
            let (shard, elapsed, response) = task
                .await
                .map_err(|error| Status::internal(format!("shard legs task failed: {error}")))??;
            known.merge(&response.geo_columns_known, &response.filter_columns_known)?;
            for hit in &response.bm25_hits {
                owner.entry(hit.doc_id).or_insert(shard);
            }
            if debug {
                shard_debug.push(HybridShardDebug {
                    shard,
                    rpc_ms: elapsed.as_secs_f32() * 1e3,
                    vector_hits: vector_counts[shard as usize],
                    bm25_hits: response.bm25_hits.len() as u32,
                    scan: None,
                });
            }
            bm25_shards.push((
                shard,
                response
                    .bm25_hits
                    .into_iter()
                    .map(|hit| (hit.doc_id, f64::from(hit.score)))
                    .collect::<Vec<_>>(),
            ));
        }
        known.refuse_unknown(filters)?;
        let legs_ms = t_legs.elapsed().as_secs_f32() * 1e3;
        let t_fusion = std::time::Instant::now();
        let mut bm25_global = fusion::merge_legs_by_score(bm25_shards);
        if legs.min_vector_score > 0.0 {
            let min = f64::from(legs.min_vector_score);
            vector_global.retain(|&(_, score, _)| score >= min);
            let allowed: std::collections::HashSet<u64> =
                vector_global.iter().map(|&(id, _, _)| id).collect();
            bm25_global.retain(|&(id, _, _)| allowed.contains(&id));
        }
        let leg_inputs = [
            Leg {
                hits: vector_global
                    .iter()
                    .map(|&(id, score, _)| (id, score))
                    .collect(),
                weight: f64::from(legs.vector_weight),
            },
            Leg {
                hits: bm25_global
                    .iter()
                    .map(|&(id, score, _)| (id, score))
                    .collect(),
                weight: f64::from(legs.bm25_weight),
            },
        ];
        let blend = legs.fusion_mode == FusionMode::ScoreBlend;
        let fused = if blend {
            fusion::blend_fuse(
                &leg_inputs,
                legs.leg_k as usize,
                legs.normalization,
                legs.combination,
                k as usize,
            )
        } else {
            fusion::rrf_fuse(&leg_inputs, legs.rrf_k, k as usize)
        };
        let hits = fused
            .into_iter()
            .map(|hit| HybridHit {
                doc_id: hit.doc_id,
                fused_score: hit.fused_score as f32,
                shard: owner.get(&hit.doc_id).copied().unwrap_or(0),
                vector_rank: hit.leg_ranks[0],
                vector_score: hit.leg_scores[0].unwrap_or(0.0) as f32,
                bm25_rank: hit.leg_ranks[1],
                bm25_score: hit.leg_scores[1].unwrap_or(0.0) as f32,
                boost_score: 0.0,
            })
            .collect();
        let debug = debug.then(|| {
            shard_debug.sort_by_key(|shard| shard.shard);
            HybridDebug {
                fusion_mode: if blend {
                    FusionMode::ScoreBlend as i32
                } else {
                    FusionMode::GlobalRank as i32
                },
                leg_k: legs.leg_k,
                terms: Vec::new(),
                analysis_ms: 0.0,
                stats_ms: 0.0,
                legs_ms,
                fusion_ms: t_fusion.elapsed().as_secs_f32() * 1e3,
                total_ms: 0.0,
                shards: shard_debug,
                boost_ms: 0.0,
                boost_terms: Vec::new(),
            }
        });
        Ok((hits, debug))
    }

    /// FUSION_MODE_GLOBAL_RANK: shards return RAW per-leg lists; the
    /// coordinator merges each leg across shards by raw score into global
    /// rankings and applies single-level RRF over them. With globally
    /// comparable scores per leg this is exactly the monolithic result
    /// for k <= leg_k (see the proto's FusionMode comments).
    #[allow(clippy::too_many_arguments)]
    async fn fanout_hybrid_global_rank(
        &self,
        request_id: &str,
        vector: &[f32],
        k: u32,
        terms: &[String],
        global: &CorpusStats,
        claims: &[u64],
        legs: HybridLegs,
        debug: bool,
        filters: &RequestFilters,
    ) -> Result<(Vec<HybridHit>, Option<HybridDebug>), Status> {
        if self.clustered_vectors.is_some() {
            return self
                .clustered_hybrid_global_rank(
                    request_id, vector, k, terms, global, claims, legs, debug, filters,
                )
                .await;
        }
        let t_legs = std::time::Instant::now();
        let (leg_vector, leg_terms, leg_dfs) = leg_payloads(vector, terms, global, legs);
        let mut shard_tasks = Vec::with_capacity(self.node_addrs.len());
        for (shard, node) in self.node_addrs.iter().enumerate() {
            let request = ShardLegsRequest {
                request_id: String::new(),
                k: legs.leg_k,
                vector: leg_vector.clone(),
                terms: leg_terms.clone(),
                global_doc_count: global.doc_count,
                global_total_doc_length: global.total_doc_length,
                global_doc_frequencies: leg_dfs.clone(),
                k1: self.bm25_params.k1 as f32,
                b: self.bm25_params.b as f32,
                expected_stats_epoch: claims[shard],
                geo_filters: filters.geo.clone(),
                filter: filters.tree.clone(),
            };
            let mut client = self.node_client(node)?;
            shard_tasks.push(tokio::spawn(async move {
                let t0 = std::time::Instant::now();
                client.shard_legs(request).await.map(|r| {
                    (
                        shard as u32,
                        t0.elapsed().as_secs_f32() * 1e3,
                        r.into_inner(),
                    )
                })
            }));
        }
        let mut vector_shards = Vec::with_capacity(shard_tasks.len());
        let mut bm25_shards = Vec::with_capacity(shard_tasks.len());
        let mut shard_debug: Vec<HybridShardDebug> = Vec::new();
        let mut owner: std::collections::HashMap<u64, u32> = std::collections::HashMap::new();
        let mut known = FilterKnown::new(filters);
        for task in shard_tasks {
            let (shard, rpc_ms, response) = task
                .await
                .map_err(|e| Status::internal(format!("shard legs task failed: {e}")))??;
            known.merge(&response.geo_columns_known, &response.filter_columns_known)?;
            if debug {
                shard_debug.push(HybridShardDebug {
                    shard,
                    rpc_ms,
                    vector_hits: response.vector_hits.len() as u32,
                    bm25_hits: response.bm25_hits.len() as u32,
                    scan: None,
                });
            }
            for h in &response.vector_hits {
                owner.entry(h.doc_id).or_insert(shard);
            }
            for h in &response.bm25_hits {
                owner.entry(h.doc_id).or_insert(shard);
            }
            vector_shards.push((
                shard,
                response
                    .vector_hits
                    .into_iter()
                    .map(|h| (h.doc_id, f64::from(h.score)))
                    .collect::<Vec<_>>(),
            ));
            bm25_shards.push((
                shard,
                response
                    .bm25_hits
                    .into_iter()
                    .map(|h| (h.doc_id, f64::from(h.score)))
                    .collect::<Vec<_>>(),
            ));
        }

        // A filter name NO shard resolves is a typo, and it would read
        // as an honest empty result set on both legs at once.
        known.refuse_unknown(filters)?;

        let legs_ms = t_legs.elapsed().as_secs_f32() * 1e3;
        // Merge each leg into a GLOBAL ranking by raw score (deterministic
        // total order; see merge_legs_by_score), then fuse once: RRF for
        // GLOBAL_RANK, normalize-and-combine for SCORE_BLEND. The two
        // modes share this whole leg-fetch path; only the fusion function
        // differs. Weights arrive RESOLVED (the handler defaults absent
        // to 1.0); an exact 0 disables its leg, which both fusion
        // functions honor by skipping it.
        let t_fusion = std::time::Instant::now();
        let mut vector_global = fusion::merge_legs_by_score(vector_shards);
        let mut bm25_global = fusion::merge_legs_by_score(bm25_shards);
        // The vector-score floor: a hit must have a qualifying vector
        // score to survive, so docs below it (or absent from the vector
        // leg) drop from BOTH legs BEFORE fusion and truncation —
        // deeper qualifying docs are promoted, and blend statistics see
        // only the filtered set. Score-defined, hence layout-invariant.
        if legs.min_vector_score > 0.0 {
            let min = f64::from(legs.min_vector_score);
            vector_global.retain(|&(_, score, _)| score >= min);
            let allowed: std::collections::HashSet<u64> =
                vector_global.iter().map(|&(id, _, _)| id).collect();
            bm25_global.retain(|&(id, _, _)| allowed.contains(&id));
        }
        let leg_inputs = [
            Leg {
                hits: vector_global
                    .iter()
                    .map(|&(id, score, _)| (id, score))
                    .collect(),
                weight: f64::from(legs.vector_weight),
            },
            Leg {
                hits: bm25_global
                    .iter()
                    .map(|&(id, score, _)| (id, score))
                    .collect(),
                weight: f64::from(legs.bm25_weight),
            },
        ];
        let blend = legs.fusion_mode == FusionMode::ScoreBlend;
        let fused = if blend {
            fusion::blend_fuse(
                &leg_inputs,
                legs.leg_k as usize,
                legs.normalization,
                legs.combination,
                k as usize,
            )
        } else {
            fusion::rrf_fuse(&leg_inputs, legs.rrf_k, k as usize)
        };

        // Provenance: global per-leg ranks, raw scores, and the owning
        // shard (a doc lives on exactly one shard's lists).
        let hits: Vec<HybridHit> = fused
            .into_iter()
            .map(|f| HybridHit {
                doc_id: f.doc_id,
                fused_score: f.fused_score as f32,
                shard: owner.get(&f.doc_id).copied().unwrap_or(0),
                vector_rank: f.leg_ranks[0],
                vector_score: f.leg_scores[0].unwrap_or(0.0) as f32,
                bm25_rank: f.leg_ranks[1],
                bm25_score: f.leg_scores[1].unwrap_or(0.0) as f32,
                boost_score: 0.0,
            })
            .collect();
        let dbg = debug.then(|| {
            shard_debug.sort_by_key(|s| s.shard);
            HybridDebug {
                fusion_mode: if blend {
                    FusionMode::ScoreBlend as i32
                } else {
                    FusionMode::GlobalRank as i32
                },
                leg_k: legs.leg_k,
                terms: Vec::new(),
                analysis_ms: 0.0,
                stats_ms: 0.0,
                legs_ms,
                fusion_ms: t_fusion.elapsed().as_secs_f32() * 1e3,
                total_ms: 0.0,
                shards: shard_debug,
                boost_ms: 0.0,
                boost_terms: Vec::new(),
            }
        });
        Ok((hits, dbg))
    }

    #[allow(clippy::too_many_arguments)]
    async fn clustered_hybrid_two_level(
        &self,
        request_id: &str,
        vector: &[f32],
        k: u32,
        terms: &[String],
        global: &CorpusStats,
        claims: &[u64],
        legs: HybridLegs,
        debug: bool,
        filters: &RequestFilters,
    ) -> Result<(Vec<HybridHit>, Option<HybridDebug>), Status> {
        let t_legs = std::time::Instant::now();
        let ranges = self.product_label_ranges().await?;
        let vector_legs = self
            .clustered_local_vector_legs(request_id, vector, legs.leg_k, filters, &ranges)
            .await?;
        let (_, leg_terms, leg_dfs) = leg_payloads(vector, terms, global, legs);
        let mut tasks = Vec::with_capacity(self.node_addrs.len());
        for (shard, node) in self.node_addrs.iter().enumerate() {
            let request = ShardLegsRequest {
                request_id: request_id.to_string(),
                k: legs.leg_k,
                vector: Vec::new(),
                terms: leg_terms.clone(),
                global_doc_count: global.doc_count,
                global_total_doc_length: global.total_doc_length,
                global_doc_frequencies: leg_dfs.clone(),
                k1: self.bm25_params.k1 as f32,
                b: self.bm25_params.b as f32,
                expected_stats_epoch: claims[shard],
                geo_filters: filters.geo.clone(),
                filter: filters.tree.clone(),
            };
            let mut client = self.node_client(node)?;
            tasks.push(tokio::spawn(async move {
                let started = std::time::Instant::now();
                client
                    .shard_legs(request)
                    .await
                    .map(|response| (shard as u32, started.elapsed(), response.into_inner()))
            }));
        }
        let mut shard_lists: Vec<(u32, Vec<HybridLegHit>)> = Vec::with_capacity(tasks.len());
        let mut shard_debug = Vec::new();
        let mut known = FilterKnown::new(filters);
        for task in tasks {
            let (shard, elapsed, response) = task
                .await
                .map_err(|error| Status::internal(format!("shard legs task failed: {error}")))??;
            known.merge(&response.geo_columns_known, &response.filter_columns_known)?;
            let vector_leg = vector_legs[shard as usize].clone();
            let bm25_leg: Vec<(u64, f64)> = response
                .bm25_hits
                .iter()
                .map(|hit| (hit.doc_id, f64::from(hit.score)))
                .collect();
            let mut local: Vec<HybridLegHit> = fusion::rrf_fuse(
                &[
                    Leg {
                        hits: vector_leg,
                        weight: f64::from(legs.vector_weight),
                    },
                    Leg {
                        hits: bm25_leg,
                        weight: f64::from(legs.bm25_weight),
                    },
                ],
                legs.rrf_k,
                legs.leg_k as usize,
            )
            .into_iter()
            .map(|hit| HybridLegHit {
                doc_id: hit.doc_id,
                fused_score: hit.fused_score as f32,
                vector_rank: hit.leg_ranks[0],
                vector_score: hit.leg_scores[0].unwrap_or(0.0) as f32,
                bm25_rank: hit.leg_ranks[1],
                bm25_score: hit.leg_scores[1].unwrap_or(0.0) as f32,
            })
            .collect();
            if legs.min_vector_score > 0.0 {
                local.retain(|hit| {
                    hit.vector_rank.is_some() && hit.vector_score >= legs.min_vector_score
                });
            }
            if debug {
                shard_debug.push(HybridShardDebug {
                    shard,
                    rpc_ms: elapsed.as_secs_f32() * 1e3,
                    vector_hits: local.iter().filter(|hit| hit.vector_rank.is_some()).count()
                        as u32,
                    bm25_hits: local.iter().filter(|hit| hit.bm25_rank.is_some()).count() as u32,
                    scan: None,
                });
            }
            shard_lists.push((shard, local));
        }
        known.refuse_unknown(filters)?;
        let legs_ms = t_legs.elapsed().as_secs_f32() * 1e3;
        let t_fusion = std::time::Instant::now();
        let fused_legs: Vec<Leg> = shard_lists
            .iter()
            .map(|(_, hits)| Leg {
                hits: hits
                    .iter()
                    .map(|hit| (hit.doc_id, f64::from(hit.fused_score)))
                    .collect(),
                weight: 1.0,
            })
            .collect();
        let fused = fusion::rrf_fuse(&fused_legs, legs.rrf_k, k as usize);
        let hits = fused
            .into_iter()
            .map(|hit| {
                let (shard, source) = shard_lists
                    .iter()
                    .find_map(|(shard, hits)| {
                        hits.iter()
                            .find(|source| source.doc_id == hit.doc_id)
                            .map(|source| (*shard, source))
                    })
                    .expect("fused hit comes from a product shard");
                HybridHit {
                    doc_id: hit.doc_id,
                    fused_score: hit.fused_score as f32,
                    shard,
                    vector_rank: source.vector_rank,
                    vector_score: source.vector_score,
                    bm25_rank: source.bm25_rank,
                    bm25_score: source.bm25_score,
                    boost_score: 0.0,
                }
            })
            .collect();
        let debug = debug.then(|| {
            shard_debug.sort_by_key(|shard| shard.shard);
            HybridDebug {
                fusion_mode: FusionMode::TwoLevel as i32,
                leg_k: legs.leg_k,
                terms: Vec::new(),
                analysis_ms: 0.0,
                stats_ms: 0.0,
                legs_ms,
                fusion_ms: t_fusion.elapsed().as_secs_f32() * 1e3,
                total_ms: 0.0,
                shards: shard_debug,
                boost_ms: 0.0,
                boost_terms: Vec::new(),
            }
        });
        Ok((hits, debug))
    }

    /// FUSION_MODE_TWO_LEVEL (fallback for incomparable scores): each
    /// shard fuses locally; the coordinator RRF-merges the shard lists.
    /// NOT partition-independent — see the proto's FusionMode comments.
    #[allow(clippy::too_many_arguments)]
    async fn fanout_hybrid_two_level(
        &self,
        request_id: &str,
        vector: &[f32],
        k: u32,
        terms: &[String],
        global: &CorpusStats,
        claims: &[u64],
        legs: HybridLegs,
        debug: bool,
        filters: &RequestFilters,
    ) -> Result<(Vec<HybridHit>, Option<HybridDebug>), Status> {
        if self.clustered_vectors.is_some() {
            return self
                .clustered_hybrid_two_level(
                    request_id, vector, k, terms, global, claims, legs, debug, filters,
                )
                .await;
        }
        let t_legs = std::time::Instant::now();
        // Level one: per-shard local fusion.
        let (leg_vector, leg_terms, leg_dfs) = leg_payloads(vector, terms, global, legs);
        let mut shard_tasks = Vec::with_capacity(self.node_addrs.len());
        for (shard, node) in self.node_addrs.iter().enumerate() {
            let request = HybridShardRequest {
                request_id: request_id.to_string(),
                k: legs.leg_k,
                vector: leg_vector.clone(),
                terms: leg_terms.clone(),
                global_doc_count: global.doc_count,
                global_total_doc_length: global.total_doc_length,
                global_doc_frequencies: leg_dfs.clone(),
                vector_weight: legs.vector_weight,
                bm25_weight: legs.bm25_weight,
                rrf_k: legs.rrf_k as f32,
                k1: self.bm25_params.k1 as f32,
                b: self.bm25_params.b as f32,
                expected_stats_epoch: claims[shard],
                geo_filters: filters.geo.clone(),
                filter: filters.tree.clone(),
            };
            let mut client = self.node_client(node)?;
            shard_tasks.push(tokio::spawn(async move {
                let t0 = std::time::Instant::now();
                client.hybrid_shard(request).await.map(|r| {
                    let r = r.into_inner();
                    (
                        shard as u32,
                        t0.elapsed().as_secs_f32() * 1e3,
                        r.hits,
                        r.geo_columns_known,
                        r.filter_columns_known,
                    )
                })
            }));
        }
        let mut shard_lists: Vec<(u32, Vec<crate::pb::HybridLegHit>)> = Vec::new();
        let mut shard_debug: Vec<HybridShardDebug> = Vec::new();
        let mut known = FilterKnown::new(filters);
        for task in shard_tasks {
            let (shard, rpc_ms, mut hits, geo_known, filter_known) = task
                .await
                .map_err(|e| Status::internal(format!("hybrid shard task failed: {e}")))??;
            known.merge(&geo_known, &filter_known)?;
            // Vector-score floor: drop non-qualifying docs from the
            // shard's fused list before level-two fusion.
            if legs.min_vector_score > 0.0 {
                hits.retain(|h| h.vector_rank.is_some() && h.vector_score >= legs.min_vector_score);
            }
            if debug {
                // A two-level shard returns one FUSED list; per-leg
                // membership is what provenance carries.
                shard_debug.push(HybridShardDebug {
                    shard,
                    rpc_ms,
                    vector_hits: hits.iter().filter(|h| h.vector_rank.is_some()).count() as u32,
                    bm25_hits: hits.iter().filter(|h| h.bm25_rank.is_some()).count() as u32,
                    scan: None,
                });
            }
            shard_lists.push((shard, hits));
        }
        known.refuse_unknown(filters)?;
        let legs_ms = t_legs.elapsed().as_secs_f32() * 1e3;
        let t_fusion = std::time::Instant::now();

        // Level two: RRF over the per-shard fused lists (unweighted; the
        // leg weights already acted at shard level).
        let mut fused_legs: Vec<Leg> = Vec::with_capacity(shard_lists.len());
        for (_, hits) in &shard_lists {
            fused_legs.push(Leg {
                hits: hits
                    .iter()
                    .map(|h| (h.doc_id, f64::from(h.fused_score)))
                    .collect(),
                weight: 1.0,
            });
        }
        let fused = fusion::rrf_fuse(&fused_legs, legs.rrf_k, k as usize);

        // Attach per-leg provenance from the owning shard's HybridLegHit.
        let hits: Vec<HybridHit> = fused
            .into_iter()
            .map(|f| {
                let (shard, source) = shard_lists
                    .iter()
                    .find_map(|(shard, hits)| {
                        hits.iter()
                            .find(|h| h.doc_id == f.doc_id)
                            .map(|h| (*shard, h))
                    })
                    .expect("fused hit comes from some shard");
                HybridHit {
                    doc_id: f.doc_id,
                    fused_score: f.fused_score as f32,
                    shard,
                    vector_rank: source.vector_rank,
                    vector_score: source.vector_score,
                    bm25_rank: source.bm25_rank,
                    bm25_score: source.bm25_score,
                    boost_score: 0.0,
                }
            })
            .collect();
        let dbg = debug.then(|| {
            let mut shard_debug = shard_debug;
            shard_debug.sort_by_key(|s| s.shard);
            HybridDebug {
                fusion_mode: FusionMode::TwoLevel as i32,
                leg_k: legs.leg_k,
                terms: Vec::new(),
                analysis_ms: 0.0,
                stats_ms: 0.0,
                legs_ms,
                fusion_ms: t_fusion.elapsed().as_secs_f32() * 1e3,
                total_ms: 0.0,
                shards: shard_debug,
                boost_ms: 0.0,
                boost_terms: Vec::new(),
            }
        });
        Ok((hits, dbg))
    }

    /// FUSION_MODE_DECOMPOSED: the EXACT fused weighted-sum top-k
    ///
    ///   fused(d) = w_v * v(d) + w_b * b(d)
    ///
    /// executed BM25-first with decomposed floors over the streaming
    /// vector path (the proto's FusionMode comment walks the phases;
    /// docs/multi-field.md "Hybrid streaming interplay" has the floor
    /// algebra). The result set is the vector index: b(d) joins on the
    /// shared positional id space, exactly 0 for docs no query term
    /// matches.
    ///
    /// The exactness ledger, kept by construction:
    /// - the BM25 leg is the exact global top-leg_k, so its top score
    ///   `b_1` is the exact global maximum of b(d), and its boundary
    ///   score bounds b(d) for every doc outside the shard lists;
    /// - every vector floor F pushed here satisfies: v(d) < F implies
    ///   fused(d) < s_lb <= the final k-th best fused score, so a doc
    ///   the floor suppressed can never belong to the top-k, and ties
    ///   AT the floor survive shard-side (emission is score >= floor);
    /// - every candidate ends with EXACT leg scores: v from the stream
    ///   or from `VectorRescore` (bitwise identical — one kernel, one
    ///   calibration), b from the leg, from `Bm25Rescore`, or exactly
    ///   0 when the unfilled leg proves no further doc matches.
    #[allow(clippy::too_many_arguments)]
    async fn fanout_hybrid_decomposed(
        &self,
        request_id: &str,
        vector: &[f32],
        k: u32,
        terms: &[String],
        global: &CorpusStats,
        claims: &[u64],
        legs: HybridLegs,
        debug: bool,
        filters: &RequestFilters,
    ) -> Result<(Vec<HybridHit>, Option<HybridDebug>), Status> {
        let n_nodes = self.node_addrs.len();
        if n_nodes == 0 {
            return Err(Status::failed_precondition("no shard nodes configured"));
        }
        // Both legs carry the same filters. The decomposed floor algebra
        // is untouched by them: a filter only REMOVES documents, so
        // every bound that dominated the unfiltered corpus still
        // dominates the survivors (docs/vector-filters.md).
        let mut known = FilterKnown::new(filters);
        let w_v = f64::from(legs.vector_weight);
        let w_b = f64::from(legs.bm25_weight);
        let fused_of = |v: f32, b: f32| w_v * f64::from(v) + w_b * f64::from(b);
        let min_v = (legs.min_vector_score > 0.0).then_some(legs.min_vector_score);
        let t_legs = std::time::Instant::now();

        // Phase 1: the BM25 leg, exact global top-leg_k. Every score
        // any shard returned is exact for its doc, so all of them pin
        // b(d) — the merge only decides b_1, the boundary, and ranks.
        let mut bm25_of: HashMap<u64, (f32, u32)> = HashMap::new();
        let mut leg_counts: HashMap<u32, u32> = HashMap::new();
        let mut merged: Vec<(u64, f32, u32)> = Vec::new();
        if !terms.is_empty() {
            let mut leg_tasks = Vec::with_capacity(n_nodes);
            for (shard, node) in self.node_addrs.iter().enumerate() {
                let request = Bm25QueryRequest {
                    projections: Vec::new(),
                    terms: terms.to_vec(),
                    k: legs.leg_k,
                    global_doc_count: global.doc_count,
                    global_total_doc_length: global.total_doc_length,
                    global_doc_frequencies: global.dfs.clone(),
                    k1: self.bm25_params.k1 as f32,
                    b: self.bm25_params.b as f32,
                    min_score: 0.0,
                    fields: Vec::new(),
                    expected_stats_epoch: claims[shard],
                    // Hybrid queries do not carry facets (yet): the
                    // vector leg's match set is the whole corpus, so
                    // "counts over the matches" has no single honest
                    // answer there. Score stages likewise wait for the
                    // hybrid composition story.
                    facet_fields: Vec::new(),
                    map_facet_fields: Vec::new(),
                    range_facet_fields: Vec::new(),
                    score_stages: Vec::new(),
                    // The SAME filters the vector stream runs under, so
                    // neither leg can contribute a document the other
                    // would have removed.
                    geo_filters: filters.geo.clone(),
                    filter: filters.tree.clone(),
                    stats_fields: Vec::new(),
                    cardinality_fields: Vec::new(),
                    phrase: None,
                };
                let mut client = self.node_client(node)?;
                leg_tasks.push(tokio::spawn(async move {
                    client.bm25_query(request).await.map(|r| {
                        let r = r.into_inner();
                        (
                            shard as u32,
                            r.hits,
                            r.geo_columns_known,
                            r.filter_columns_known,
                        )
                    })
                }));
            }
            for task in leg_tasks {
                let (shard, hits, geo_known, filter_known) = task
                    .await
                    .map_err(|e| Status::internal(format!("bm25 leg task failed: {e}")))??;
                known.merge(&geo_known, &filter_known)?;
                leg_counts.insert(shard, hits.len() as u32);
                for h in &hits {
                    bm25_of.insert(h.doc_id, (h.score, shard));
                    merged.push((h.doc_id, h.score, shard));
                }
            }
            merged.sort_by(|a, b| {
                b.1.total_cmp(&a.1)
                    .then_with(|| a.2.cmp(&b.2))
                    .then_with(|| a.0.cmp(&b.0))
            });
            merged.truncate(legs.leg_k as usize);
        }
        let b_1 = merged.first().map_or(0.0f32, |&(_, s, _)| s);
        let wb_b1 = w_b * f64::from(b_1);
        // The leg filled: docs outside every shard list score at most
        // the boundary. It did not: every matching doc is in some list,
        // so an absent doc scores exactly 0.
        let filled = merged.len() == legs.leg_k as usize && !merged.is_empty();
        let b_out: f64 = if filled {
            f64::from(merged.last().expect("filled leg is non-empty").1)
        } else {
            0.0
        };
        let bm25_rank: HashMap<u64, u32> = merged
            .iter()
            .enumerate()
            .map(|(i, &(doc, _, _))| (doc, i as u32 + 1))
            .collect();

        // Phase 2: pin v(d) for the leg's top k docs. Their true fused
        // scores are the first k-th-best lower bound — the seed a
        // BM25-only bound can never provide (the top lexical doc could
        // sit at any vector score, so no shard-wide vector floor
        // follows from the leg alone).
        let mut seed_ids: HashMap<u32, Vec<u64>> = HashMap::new();
        for &(doc, _, shard) in merged.iter().take(k as usize) {
            seed_ids.entry(shard).or_default().push(doc);
        }
        let v_of = self.fanout_vector_rescore(vector, seed_ids).await?;

        // Known fused LOWER bounds, tracked as a k-sized min-heap: its
        // root is s_lb, the k-th best known bound, and every pushed
        // floor decomposes from it. A doc outside the leg contributes
        // w_v * v alone (b >= 0), still a valid lower bound.
        let mut flb_heap: std::collections::BinaryHeap<std::cmp::Reverse<F64Ord>> =
            std::collections::BinaryHeap::with_capacity(k as usize + 1);
        let push_flb = |heap: &mut std::collections::BinaryHeap<std::cmp::Reverse<F64Ord>>,
                        value: f64| {
            if heap.len() < k as usize {
                heap.push(std::cmp::Reverse(F64Ord(value)));
            } else if heap.peek().is_some_and(|r| value > r.0 .0) {
                heap.pop();
                heap.push(std::cmp::Reverse(F64Ord(value)));
            }
        };
        // Candidates: doc -> (v, owning shard). Seeded with the phase-2
        // rescores so a seed doc is a candidate even if the floor it
        // funded later suppresses its own emission.
        let mut v_seen: HashMap<u64, (f32, u32)> = HashMap::new();
        for (&doc, &v) in &v_of {
            let (b, shard) = bm25_of[&doc];
            v_seen.insert(doc, (v, shard));
            if min_v.is_none_or(|m| v >= m) {
                push_flb(&mut flb_heap, fused_of(v, b));
            }
        }
        let mut s_lb =
            (flb_heap.len() == k as usize).then(|| flb_heap.peek().expect("full heap").0 .0);
        let decomposed = s_lb.map(|s| decomposed_floor(s, wb_b1, w_v));
        // min_vector_score is a result-set gate, so it doubles as a
        // free starting floor: suppressed docs are excluded docs.
        let initial_floor = match (decomposed, min_v) {
            (Some(f), Some(m)) => Some(f.max(m)),
            (Some(f), None) => Some(f),
            (None, Some(m)) => Some(m),
            (None, None) => None,
        };

        // Phase 3: the streaming vector scan, floored from the first
        // block, with re-decomposed floors chasing the rising k-th
        // best known bound.
        let mut summaries: Vec<Option<StreamSearchSummary>> = vec![None; n_nodes];
        let mut clustered_counts = vec![0u32; n_nodes];
        let mut last_floor = initial_floor.unwrap_or(f32::NEG_INFINITY);
        if let Some(backend) = &self.clustered_vectors {
            let ranges = self.product_label_ranges().await?;
            let allowed = self.clustered_allowed_labels(filters).await?;
            let mut stream = backend
                .candidate_stream(request_id, vector.to_vec(), allowed, initial_floor)
                .await?;
            let mut provider_labels = std::collections::HashSet::new();
            let completion = loop {
                match stream.next_event().await? {
                    ClusteredCandidateEvent::Batch(batch) => {
                        for candidate in batch {
                            if !candidate.score.is_finite() {
                                return Err(Status::internal(format!(
                                    "clustered TurboVec label {} has non-finite score {}",
                                    candidate.label, candidate.score
                                )));
                            }
                            if !provider_labels.insert(candidate.label) {
                                return Err(Status::failed_precondition(format!(
                                    "clustered TurboVec emitted duplicate stable label {}",
                                    candidate.label
                                )));
                            }
                            let shard = Self::product_owner(&ranges, candidate.label)?;
                            clustered_counts[shard as usize] =
                                clustered_counts[shard as usize].saturating_add(1);
                            // A re-emitted phase-2 seed carries the identical
                            // score (one kernel); keep the first sighting.
                            if v_seen.contains_key(&candidate.label) {
                                continue;
                            }
                            v_seen.insert(candidate.label, (candidate.score, shard));
                            if min_v.is_some_and(|minimum| candidate.score < minimum) {
                                continue;
                            }
                            let b = bm25_of
                                .get(&candidate.label)
                                .map_or(0.0, |&(score, _)| score);
                            push_flb(&mut flb_heap, fused_of(candidate.score, b));
                        }
                        if flb_heap.len() == k as usize {
                            let bound = flb_heap.peek().expect("full heap").0 .0;
                            if s_lb.is_none_or(|score| bound > score) {
                                s_lb = Some(bound);
                                let floor = decomposed_floor(bound, wb_b1, w_v);
                                if floor > last_floor {
                                    stream.raise_floor(floor)?;
                                    last_floor = floor;
                                }
                            }
                        }
                    }
                    ClusteredCandidateEvent::Completion(completion) => break completion,
                }
            };
            if completion.emitted != provider_labels.len() as u64 {
                return Err(Status::internal(format!(
                    "clustered TurboVec completion counted {} candidates but decomposed search received {}",
                    completion.emitted,
                    provider_labels.len()
                )));
            }
        } else {
            let mut fanout =
                self.open_stream_fanout(request_id, vector, initial_floor, false, filters)?;
            let mut remaining = n_nodes;
            while remaining > 0 {
                let (shard, msg) = match fanout.next_message(&summaries).await {
                    Ok(Some(pair)) => pair,
                    Ok(None) => continue,
                    Err(status) => return fanout.cancel_with(status).await,
                };
                match msg.payload {
                    Some(stream_search_response::Payload::Batch(batch)) => {
                        if batch.hits.len() % 12 != 0 {
                            let status = Status::internal(format!(
                                "shard {shard} sent a misaligned batch of {} bytes",
                                batch.hits.len()
                            ));
                            return fanout.cancel_with(status).await;
                        }
                        for rec in batch.hits.as_chunks::<12>().0 {
                            let doc = u64::from_le_bytes(rec[..8].try_into().expect("8-byte id"));
                            let v =
                                f32::from_le_bytes(rec[8..12].try_into().expect("4-byte score"));
                            if v_seen.contains_key(&doc) {
                                continue;
                            }
                            v_seen.insert(doc, (v, shard as u32));
                            if min_v.is_some_and(|m| v < m) {
                                continue;
                            }
                            let b = bm25_of.get(&doc).map_or(0.0f32, |&(s, _)| s);
                            push_flb(&mut flb_heap, fused_of(v, b));
                        }
                        if flb_heap.len() == k as usize {
                            let m = flb_heap.peek().expect("full heap").0 .0;
                            if s_lb.is_none_or(|s| m > s) {
                                s_lb = Some(m);
                                let floor = decomposed_floor(m, wb_b1, w_v);
                                if floor > last_floor {
                                    last_floor = floor;
                                    self.push_stream_floor(&fanout, floor);
                                }
                            }
                        }
                    }
                    Some(stream_search_response::Payload::Summary(summary)) => {
                        if !summary.completed {
                            let status = Status::internal(format!(
                                "shard {shard} stopped before completing its scan"
                            ));
                            return fanout.cancel_with(status).await;
                        }
                        if let Err(error) =
                            known.merge(&summary.geo_columns_known, &summary.filter_columns_known)
                        {
                            return fanout.cancel_with(error).await;
                        }
                        summaries[shard] = Some(summary);
                        fanout.mark_completed(shard);
                        remaining -= 1;
                    }
                    None => {}
                }
            }
            known.refuse_unknown(filters)?;
        }
        let legs_ms = t_legs.elapsed().as_secs_f32() * 1e3;

        // Close-out. Candidates with a leg score (or proven-zero b)
        // have exact fused scores already; the rest hold b in
        // [0, b_out] and need Bm25Rescore — unless even b_out cannot
        // lift them to the k-th best exact fused score known so far.
        let t_fusion = std::time::Instant::now();
        let mut exact: Vec<(u64, u32, f32, f32)> = Vec::new(); // (doc, shard, v, b)
        let mut unknown: Vec<(u64, u32, f32)> = Vec::new(); // (doc, shard, v)
        for (&doc, &(v, shard)) in &v_seen {
            if min_v.is_some_and(|m| v < m) {
                continue;
            }
            match bm25_of.get(&doc) {
                Some(&(b, _)) => exact.push((doc, shard, v, b)),
                None if !filled => exact.push((doc, shard, v, 0.0)),
                None => unknown.push((doc, shard, v)),
            }
        }
        let kth_known: f64 = {
            let mut fused: Vec<f64> = exact.iter().map(|&(_, _, v, b)| fused_of(v, b)).collect();
            if fused.len() >= k as usize {
                let idx = k as usize - 1;
                *fused.select_nth_unstable_by(idx, |a, b| b.total_cmp(a)).1
            } else {
                f64::NEG_INFINITY
            }
        };
        let mut rescore_ids: HashMap<u32, Vec<u64>> = HashMap::new();
        let mut rescore_docs: Vec<(u64, u32, f32)> = Vec::new();
        for (doc, shard, v) in unknown {
            // Conservative upper bound: round the two products and the
            // sum up by a magnitude-relative slack that dwarfs the f64
            // rounding error, so a doc dropped here provably cannot
            // reach the k-th best.
            let ub = w_v * f64::from(v) + w_b * b_out;
            let mag = ub.abs() + (w_v * f64::from(v)).abs() + w_b * b_out;
            let ub_safe = ub + mag * SLACK_REL + f64::MIN_POSITIVE;
            if ub_safe >= kth_known {
                rescore_ids.entry(shard).or_default().push(doc);
                rescore_docs.push((doc, shard, v));
            }
        }
        let rescored_b = self
            .fanout_bm25_rescore_scores(terms, global, claims, rescore_ids, &[])
            .await?;
        for (doc, shard, v) in rescore_docs {
            // Absent from the rescore response = no query term matches
            // the doc: b is exactly 0.
            let b = rescored_b.get(&doc).copied().unwrap_or(0.0);
            exact.push((doc, shard, v, b));
        }

        let mut ranked: Vec<(u64, u32, f32, f32, f64)> = exact
            .into_iter()
            .map(|(doc, shard, v, b)| (doc, shard, v, b, fused_of(v, b)))
            .collect();
        ranked.sort_by(|a, b| {
            b.4.total_cmp(&a.4)
                .then_with(|| a.1.cmp(&b.1))
                .then_with(|| a.0.cmp(&b.0))
        });
        ranked.truncate(k as usize);
        let hits: Vec<HybridHit> = ranked
            .into_iter()
            .map(|(doc, shard, v, b, fused)| HybridHit {
                doc_id: doc,
                fused_score: fused as f32,
                shard,
                vector_rank: None,
                vector_score: v,
                bm25_rank: bm25_rank.get(&doc).copied(),
                bm25_score: b,
                boost_score: 0.0,
            })
            .collect();
        let dbg = debug.then(|| {
            let shards: Vec<HybridShardDebug> = summaries
                .iter()
                .enumerate()
                .map(|(shard, summary)| HybridShardDebug {
                    shard: shard as u32,
                    rpc_ms: 0.0,
                    vector_hits: summary.as_ref().map_or(clustered_counts[shard], |s| {
                        u32::try_from(s.emitted).unwrap_or(u32::MAX)
                    }),
                    bm25_hits: leg_counts.get(&(shard as u32)).copied().unwrap_or(0),
                    scan: None,
                })
                .collect();
            HybridDebug {
                fusion_mode: FusionMode::Decomposed as i32,
                leg_k: legs.leg_k,
                terms: Vec::new(),
                analysis_ms: 0.0,
                stats_ms: 0.0,
                legs_ms,
                fusion_ms: t_fusion.elapsed().as_secs_f32() * 1e3,
                total_ms: 0.0,
                shards,
                boost_ms: 0.0,
                boost_terms: Vec::new(),
            }
        });
        Ok((hits, dbg))
    }

    /// Candidate-scoped vector scoring fan-out: `VectorRescore` on
    /// every shard that owns candidates, merged into doc -> score.
    async fn fanout_vector_rescore(
        &self,
        vector: &[f32],
        by_shard: HashMap<u32, Vec<u64>>,
    ) -> Result<HashMap<u64, f32>, Status> {
        if let Some(clustered) = &self.clustered_vectors {
            let mut ids: Vec<u64> = by_shard.into_values().flatten().collect();
            ids.sort_unstable();
            ids.dedup();
            if ids.is_empty() {
                return Ok(HashMap::new());
            }
            let response = clustered
                .search(
                    vector.to_vec(),
                    u32::try_from(ids.len()).map_err(|_| {
                        Status::resource_exhausted(
                            "clustered vector rescore candidate count exceeds u32",
                        )
                    })?,
                    Some(ClusteredLabelFilter::Labels(ids)),
                    None,
                    false,
                )
                .await?;
            if response.results.len() != 1 {
                return Err(Status::internal(format!(
                    "clustered TurboVec returned {} rescore results for one query",
                    response.results.len()
                )));
            }
            return response.results[0]
                .neighbours
                .iter()
                .map(|neighbour| {
                    neighbour
                        .label
                        .map(|label| (label, neighbour.score))
                        .ok_or_else(|| {
                            Status::failed_precondition(
                                "clustered TurboVec rescore returned an unlabelled row",
                            )
                        })
                })
                .collect();
        }
        let mut tasks = Vec::with_capacity(by_shard.len());
        for (shard, ids) in by_shard {
            let request = VectorRescoreRequest {
                vector: vector.to_vec(),
                candidate_ids: ids,
            };
            let mut client = self.node_client(&self.node_addrs[shard as usize])?;
            tasks.push(tokio::spawn(async move {
                client
                    .vector_rescore(request)
                    .await
                    .map(|r| r.into_inner().hits)
            }));
        }
        let mut scores = HashMap::new();
        for task in tasks {
            let hits = task
                .await
                .map_err(|e| Status::internal(format!("vector rescore task failed: {e}")))??;
            for hit in hits {
                scores.insert(hit.doc_id, hit.score);
            }
        }
        Ok(scores)
    }

    /// Candidate-scoped BM25 fan-out (the cascade phase-2 seam),
    /// reduced to doc -> score. Docs absent from the response match no
    /// query term and score exactly 0.
    async fn fanout_bm25_rescore_scores(
        &self,
        terms: &[String],
        global: &CorpusStats,
        claims: &[u64],
        by_shard: HashMap<u32, Vec<u64>>,
        score_stages: &[crate::pb::ScoreStage],
    ) -> Result<HashMap<u64, f32>, Status> {
        let mut tasks = Vec::with_capacity(by_shard.len());
        for (shard, ids) in by_shard {
            let request = Bm25RescoreRequest {
                terms: terms.to_vec(),
                global_doc_count: global.doc_count,
                global_total_doc_length: global.total_doc_length,
                global_doc_frequencies: global.dfs.clone(),
                candidate_ids: ids,
                k1: self.bm25_params.k1 as f32,
                b: self.bm25_params.b as f32,
                expected_stats_epoch: claims[shard as usize],
                score_stages: score_stages.to_vec(),
            };
            let mut client = self.node_client(&self.node_addrs[shard as usize])?;
            tasks.push(tokio::spawn(async move {
                client.bm25_rescore(request).await.map(|r| r.into_inner())
            }));
        }
        let mut scores = HashMap::new();
        let mut stage_known = vec![false; score_stages.len()];
        for task in tasks {
            let response = task
                .await
                .map_err(|e| Status::internal(format!("bm25 rescore task failed: {e}")))??;
            if response.stage_columns_known.len() != score_stages.len() {
                return Err(Status::failed_precondition(format!(
                    "BM25 rescore returned {} stage-known flags for {} stages",
                    response.stage_columns_known.len(),
                    score_stages.len()
                )));
            }
            for (known, shard_known) in stage_known.iter_mut().zip(&response.stage_columns_known) {
                *known |= shard_known;
            }
            for hit in response.hits {
                scores.insert(hit.doc_id, hit.score);
            }
        }
        for (stage, known) in score_stages.iter().zip(stage_known) {
            if !known {
                return Err(Status::invalid_argument(format!(
                    "no shard has numeric column {}: the score stage would be a silent no-op",
                    stage.column
                )));
            }
        }
        Ok(scores)
    }

    /// One cascade phase-2 rescore fan-out: candidates routed to their
    /// owning shards, scored with the GLOBAL stats. Returns doc -> BM25
    /// score plus per-shard (rpc ms, hit count) for the debug surface.
    /// `claims[shard]` travels as that shard's `expected_stats_epoch`.
    async fn cascade_rescore_round(
        &self,
        terms: &[String],
        global: &CorpusStats,
        claims: &[u64],
        by_shard: &std::collections::HashMap<u32, Vec<u64>>,
    ) -> Result<
        (
            std::collections::HashMap<u64, f64>,
            std::collections::HashMap<u32, (f32, u32)>,
        ),
        Status,
    > {
        let mut rescore_tasks = Vec::with_capacity(by_shard.len());
        for (&shard, ids) in by_shard {
            let node = &self.node_addrs[shard as usize];
            let request = Bm25RescoreRequest {
                terms: terms.to_vec(),
                global_doc_count: global.doc_count,
                global_total_doc_length: global.total_doc_length,
                global_doc_frequencies: global.dfs.clone(),
                candidate_ids: ids.clone(),
                k1: self.bm25_params.k1 as f32,
                b: self.bm25_params.b as f32,
                expected_stats_epoch: claims[shard as usize],
                score_stages: Vec::new(),
            };
            let mut client = self.node_client(node)?;
            rescore_tasks.push(tokio::spawn(async move {
                let t0 = std::time::Instant::now();
                client
                    .bm25_rescore(request)
                    .await
                    .map(|r| (shard, t0.elapsed().as_secs_f32() * 1e3, r.into_inner().hits))
            }));
        }
        let mut bm25_of: std::collections::HashMap<u64, f64> = std::collections::HashMap::new();
        let mut rescore_debug: std::collections::HashMap<u32, (f32, u32)> =
            std::collections::HashMap::new();
        for task in rescore_tasks {
            let (shard, rpc_ms, hits) = task
                .await
                .map_err(|e| Status::internal(format!("bm25 rescore task failed: {e}")))??;
            rescore_debug.insert(shard, (rpc_ms, hits.len() as u32));
            for hit in hits {
                bm25_of.insert(hit.doc_id, f64::from(hit.score));
            }
        }
        Ok((bm25_of, rescore_debug))
    }

    /// Build the tonic server for this service with explicit message size
    /// limits (see [`crate::MAX_MESSAGE_BYTES`]).
    pub fn into_server(self, max_message_bytes: usize) -> SearchServiceServer<Self> {
        SearchServiceServer::new(self)
            .max_decoding_message_size(max_message_bytes)
            .max_encoding_message_size(max_message_bytes)
    }

    /// Run one fan-out search against every configured node. Broken out
    /// from the gRPC handler so tests and the binary can drive it directly.
    ///
    /// Floor flow per query:
    /// 1. open a `SearchShard` stream per node and send `StartShardSearch`;
    /// 2. each stream pump feeds shard floor updates into one shared
    ///    [`FloorTracker`]; raises land in a conflating `watch` cell that
    ///    per-stream forwarders relay — a burst of raises collapses to the
    ///    latest value instead of one message per raise;
    /// 3. each pump ends on the node's terminal `SearchShardDone`.
    ///
    /// Per shard, [`FanoutLimits`] adds a hedged retry to the shard's
    /// replica after `hedge_delay` (first success wins; identical data
    /// plus exact search means identical results either way) and bounds
    /// the whole attempt with `shard_deadline`.
    pub async fn fanout_search(
        &self,
        request_id: &str,
        vector: &[f32],
        k: u32,
        tie_complete: bool,
        filters: &RequestFilters,
    ) -> Result<FanoutResult, Status> {
        let n_nodes = self.node_addrs.len();
        if n_nodes == 0 {
            return Err(Status::failed_precondition("no shard nodes configured"));
        }

        let ctx = ShardQueryCtx {
            request_id: Arc::from(request_id),
            vector: Arc::new(vector.to_vec()),
            k,
            tie_complete,
            collapse: false,
            filters: Arc::new(filters.clone()),
            tracker: Arc::new(Mutex::new(FloorTracker::new())),
            gfloor: Arc::new(watch::channel(f32::NEG_INFINITY).0),
            hedges: Arc::new(AtomicU64::new(0)),
            hedge_wins: Arc::new(AtomicU64::new(0)),
        };
        let (hedges, hedge_wins) = (Arc::clone(&ctx.hedges), Arc::clone(&ctx.hedge_wins));
        let mut known = FilterKnown::new(filters);

        let (done_tx, mut done_rx) =
            mpsc::channel::<(u32, f32, Result<SearchShardDone, Status>)>(n_nodes);
        for shard in 0..n_nodes {
            let primary = self.node_client(&self.node_addrs[shard])?;
            let replica = match self.replica_addrs.get(shard).and_then(|r| r.as_deref()) {
                Some(addr) => Some(self.node_client(addr)?),
                None => None,
            };
            let ctx = ctx.clone();
            let limits = self.limits;
            let done_tx = done_tx.clone();
            tokio::spawn(async move {
                let t0 = std::time::Instant::now();
                let result =
                    run_shard_with_hedge(shard as u32, primary, replica, ctx, limits).await;
                let wall_ms = t0.elapsed().as_secs_f32() * 1e3;
                let _ = done_tx.send((shard as u32, wall_ms, result)).await;
            });
        }
        drop(done_tx);

        let mut shard_hits: Vec<(u32, Vec<(u64, f32)>)> = Vec::with_capacity(n_nodes);
        let mut shard_stats: Vec<Option<ShardScanStats>> = Vec::with_capacity(n_nodes);
        let mut shard_wall_ms: Vec<(u32, f32)> = Vec::with_capacity(n_nodes);
        for _ in 0..n_nodes {
            match done_rx.recv().await {
                Some((shard, wall_ms, Ok(done))) => {
                    known.merge(&done.geo_columns_known, &done.filter_columns_known)?;
                    shard_hits.push((
                        shard,
                        done.hits
                            .into_iter()
                            .map(|h| (h.vector_id, h.score))
                            .collect(),
                    ));
                    shard_stats.push(done.stats);
                    shard_wall_ms.push((shard, wall_ms));
                }
                Some((shard, _, Err(e))) => {
                    return Err(Status::internal(format!("shard {shard} failed: {e}")));
                }
                None => {
                    return Err(Status::internal("fan-out completed without all shards"));
                }
            }
        }

        known.refuse_unknown(filters)?;

        let hits = merge_topk(shard_hits.iter().cloned(), k as usize)
            .into_iter()
            .map(|h| ScoredHit {
                vector_id: h.vector_id,
                score: h.score,
                parent_id: 0,
            })
            .collect();
        Ok(FanoutResult {
            hits,
            shard_stats,
            shard_hits,
            shard_wall_ms,
            hedges_fired: hedges.load(AtomicOrdering::Relaxed),
            hedge_wins: hedge_wins.load(AtomicOrdering::Relaxed),
        })
    }

    /// Open one `StreamSearch` per shard: Start flows through a held
    /// sender that later carries floor raises or an authoritative Stop. Each
    /// stream also gets a UDP token so both signals reach the shard first on
    /// the fast lossy lane and then on the gRPC stream.
    fn open_stream_fanout(
        &self,
        request_id: &str,
        vector: &[f32],
        initial_floor: Option<f32>,
        collapse_parents: bool,
        filters: &RequestFilters,
    ) -> Result<StreamFanout, Status> {
        let n_nodes = self.node_addrs.len();
        let udp_socket = self.floor_socket().cloned();
        let (merged_tx, merged_rx) =
            mpsc::channel::<(usize, Result<Option<StreamSearchResponse>, Status>)>(4 * n_nodes);
        let mut floor_txs: Vec<Option<mpsc::Sender<StreamSearchRequest>>> =
            Vec::with_capacity(n_nodes);
        let mut udp_lanes: Vec<Option<(u64, std::net::SocketAddr)>> = Vec::with_capacity(n_nodes);
        for shard in 0..n_nodes {
            let mut client = self.node_client(&self.node_addrs[shard])?;
            let lane = self
                .floor_target(&self.node_addrs[shard])
                .map(|target| (floor_token(), target));
            let (req_tx, req_rx) = mpsc::channel::<StreamSearchRequest>(64);
            req_tx
                .try_send(StreamSearchRequest {
                    payload: Some(stream_search_request::Payload::Start(StartStreamSearch {
                        request_id: request_id.to_string(),
                        vector: vector.to_vec(),
                        initial_floor,
                        floor_token: lane.map_or(0, |(token, _)| token),
                        collapse_parents,
                        geo_filters: filters.geo.clone(),
                        filter: filters.tree.clone(),
                    })),
                })
                .expect("fresh channel accepts the Start message");
            floor_txs.push(Some(req_tx));
            udp_lanes.push(lane);
            let merged_tx = merged_tx.clone();
            tokio::spawn(async move {
                let mut inbound = match client
                    .stream_search(Request::new(ReceiverStream::new(req_rx)))
                    .await
                {
                    Ok(response) => response.into_inner(),
                    Err(e) => {
                        let _ = merged_tx.send((shard, Err(e))).await;
                        return;
                    }
                };
                loop {
                    match inbound.message().await {
                        Ok(Some(msg)) => {
                            if merged_tx.send((shard, Ok(Some(msg)))).await.is_err() {
                                return;
                            }
                        }
                        Ok(None) => {
                            let _ = merged_tx.send((shard, Ok(None))).await;
                            return;
                        }
                        Err(e) => {
                            let _ = merged_tx.send((shard, Err(e))).await;
                            return;
                        }
                    }
                }
            });
        }
        Ok(StreamFanout {
            merged_rx,
            floor_txs,
            udp_lanes,
            udp_socket,
        })
    }

    /// Push a floor raise to every still-open stream of `fanout`: UDP
    /// first (the fast lossy copy), then the reliable stream. Both are
    /// monotone max-folds shard-side, so double delivery and loss are
    /// equally free; a full stream channel just means the next raise
    /// supersedes this one.
    fn push_stream_floor(&self, fanout: &StreamFanout, floor: f32) {
        let update = StreamSearchRequest {
            payload: Some(stream_search_request::Payload::FloorUpdate(FloorUpdate {
                floor,
            })),
        };
        for (si, tx) in fanout.floor_txs.iter().enumerate() {
            let Some(tx) = tx.as_ref() else {
                continue;
            };
            if let (Some(socket), Some((token, target))) =
                (fanout.udp_socket.as_deref(), fanout.udp_lanes[si])
            {
                let dgram = crate::stream_signal::encode_floor(token, floor);
                let _ = socket.send_to(&dgram, target);
            }
            let _ = tx.try_send(update.clone());
        }
    }

    /// [`Self::fanout_search`] over the streaming protocol
    /// (`NodeService.StreamSearch`): shards hold no top-k and emit
    /// every candidate at or above the live floor; the heap here — the
    /// only one in the system — defines k. Whenever its k-th best
    /// tightens, `floor_seed(kth)` (one f32 ULP below, so boundary ties
    /// survive) is pushed to every open stream.
    ///
    /// Exactness: the successful path never sends Stop, every shard's
    /// terminal summary must certify `completed = true`, every emission
    /// scored at or above the floor in effect when its block was scanned,
    /// and every pushed floor is a lower bound on the global k-th best. An
    /// error cancels unfinished streams, whose summaries are necessarily
    /// incomplete and unusable. Successful results are identical to
    /// [`Self::fanout_search`] (same scores, same `merge_topk` total order).
    ///
    /// `initial_floor` seeds every shard's starting floor — the hybrid
    /// seam, where a finished BM25 leg's decomposed floor prunes the
    /// vector scan from the first block.
    pub async fn fanout_stream_search(
        &self,
        request_id: &str,
        vector: &[f32],
        k: u32,
        initial_floor: Option<f32>,
        filters: &RequestFilters,
    ) -> Result<StreamFanoutResult, Status> {
        let n_nodes = self.node_addrs.len();
        if n_nodes == 0 {
            return Err(Status::failed_precondition("no shard nodes configured"));
        }
        if initial_floor.is_some_and(f32::is_nan) {
            return Err(Status::invalid_argument("initial_floor must not be NaN"));
        }
        // Without k the heap never fills, no floor ever rises, and
        // every shard would emit itself entirely for nothing.
        if k == 0 {
            return Ok(StreamFanoutResult {
                hits: Vec::new(),
                summaries: Vec::new(),
                floors_sent: 0,
            });
        }

        let mut known = FilterKnown::new(filters);
        let mut fanout =
            self.open_stream_fanout(request_id, vector, initial_floor, false, filters)?;

        // The global top-k: a max-heap whose top is the WORST survivor
        // under the merge's total order, so peek() is the k-th best.
        let mut heap: std::collections::BinaryHeap<StreamHeapEntry> =
            std::collections::BinaryHeap::with_capacity(k as usize + 1);
        let mut summaries: Vec<Option<StreamSearchSummary>> = vec![None; n_nodes];
        let mut remaining = n_nodes;
        let mut last_floor = initial_floor.unwrap_or(f32::NEG_INFINITY);
        let mut floors_sent = 0u64;
        let mut scoring_fingerprint: Option<String> = None;
        while remaining > 0 {
            let (shard, msg) = match fanout.next_message(&summaries).await {
                Ok(Some(pair)) => pair,
                Ok(None) => continue,
                Err(status) => return fanout.cancel_with(status).await,
            };
            match msg.payload {
                Some(stream_search_response::Payload::Batch(batch)) => {
                    // Packed 12-byte LE records: u64 global id, f32
                    // score (see StreamSearchBatch).
                    if batch.hits.len() % 12 != 0 {
                        let status = Status::internal(format!(
                            "shard {shard} sent a misaligned batch of {} bytes",
                            batch.hits.len()
                        ));
                        return fanout.cancel_with(status).await;
                    }
                    for rec in batch.hits.as_chunks::<12>().0 {
                        let entry = StreamHeapEntry(MergedHit {
                            vector_id: u64::from_le_bytes(rec[..8].try_into().expect("8-byte id")),
                            shard: shard as u32,
                            score: f32::from_le_bytes(rec[8..12].try_into().expect("4-byte score")),
                        });
                        if heap.len() < k as usize {
                            heap.push(entry);
                        } else if cmp_hits(&entry.0, &heap.peek().expect("heap is full").0)
                            == std::cmp::Ordering::Less
                        {
                            heap.pop();
                            heap.push(entry);
                        }
                    }
                    // Full heap: its worst survivor bounds the global
                    // k-th best from below; seed one ULP down so shard
                    // ties at the boundary keep flowing. next_down, not
                    // bm25::floor_seed: vector scores are SIGNED (dot
                    // products), and clamping a negative k-th best to 0
                    // would push a floor ABOVE it — a recall bug on any
                    // corpus whose top-k dips below zero.
                    if heap.len() == k as usize {
                        let kth = heap.peek().expect("heap is full").0.score;
                        let floor = kth.next_down();
                        if floor > last_floor {
                            last_floor = floor;
                            floors_sent += 1;
                            self.push_stream_floor(&fanout, floor);
                        }
                    }
                    self.publish_progress(
                        crate::pb::QueryStreamPhase::Dense,
                        heap.iter()
                            .map(|entry| (entry.0.vector_id, entry.0.score))
                            .collect(),
                        scoring_fingerprint.clone().unwrap_or_default(),
                    );
                }
                Some(stream_search_response::Payload::Summary(summary)) => {
                    if !summary.completed {
                        let status = Status::internal(format!(
                            "shard {shard} stopped before completing its scan"
                        ));
                        return fanout.cancel_with(status).await;
                    }
                    if summary.scoring_fingerprint.is_empty() {
                        let status = Status::failed_precondition(format!(
                            "shard {shard} completed without a vector scoring fingerprint"
                        ));
                        return fanout.cancel_with(status).await;
                    }
                    match scoring_fingerprint.as_ref() {
                        Some(expected) if expected != &summary.scoring_fingerprint => {
                            let status = Status::failed_precondition(format!(
                                "shard {shard} vector scoring fingerprint {} differs from {expected}",
                                summary.scoring_fingerprint
                            ));
                            return fanout.cancel_with(status).await;
                        }
                        None => scoring_fingerprint = Some(summary.scoring_fingerprint.clone()),
                        _ => {}
                    }
                    // The vector leg's half of the typo handshake: a
                    // filter column no shard resolves must refuse even
                    // when the stream completed cleanly.
                    if let Err(e) =
                        known.merge(&summary.geo_columns_known, &summary.filter_columns_known)
                    {
                        return fanout.cancel_with(e).await;
                    }
                    summaries[shard] = Some(summary);
                    fanout.mark_completed(shard);
                    remaining -= 1;
                }
                None => {}
            }
        }

        known.refuse_unknown(filters)?;

        self.publish_progress(
            crate::pb::QueryStreamPhase::Dense,
            heap.iter()
                .map(|entry| (entry.0.vector_id, entry.0.score))
                .collect(),
            scoring_fingerprint.unwrap_or_default(),
        );

        let mut all: Vec<MergedHit> = heap.into_iter().map(|e| e.0).collect();
        all.sort_by(cmp_hits);
        Ok(StreamFanoutResult {
            hits: all
                .into_iter()
                .map(|h| ScoredHit {
                    vector_id: h.vector_id,
                    score: h.score,
                    parent_id: 0,
                })
                .collect(),
            summaries: summaries
                .into_iter()
                .map(|s| s.expect("all summaries arrived"))
                .collect(),
            floors_sent,
        })
    }

    /// Document-mode streaming: [`Self::fanout_stream_search`] with
    /// `k` meaning k distinct PARENT documents. Shards emit chunks
    /// tagged with their parents (lineage `parent_id`, or tagged
    /// self-parents); the coordinator owns the whole document
    /// aggregation — a parent's score is its best chunk's, the floor
    /// is one ULP below the k-th best parent score, and each returned
    /// parent carries EVERY chunk at or above the final floor,
    /// whichever shards held them. No colocation: an opinion whose
    /// chunks straddle a shard cut aggregates here exactly like one
    /// whose chunks share a shard.
    ///
    /// Lossless: a chunk below the parent floor can neither beat its
    /// own parent's best nor introduce a new top-k parent, and every
    /// chunk scoring at or above the FINAL floor cleared every earlier
    /// (lower) floor, so filtering the retained chunks to the final
    /// floor makes the groups exact and layout-invariant.
    pub async fn fanout_stream_search_collapse(
        &self,
        request_id: &str,
        vector: &[f32],
        k: u32,
        filters: &RequestFilters,
    ) -> Result<CollapseStreamResult, Status> {
        struct ParentAgg {
            best_score: f32,
            best_id: u64,
            chunks: Vec<(u64, f32)>,
        }
        let n_nodes = self.node_addrs.len();
        if n_nodes == 0 {
            return Err(Status::failed_precondition("no shard nodes configured"));
        }
        if k == 0 {
            return Ok(CollapseStreamResult {
                hits: Vec::new(),
                groups: Vec::new(),
                chunk_floor: f32::NEG_INFINITY,
                summaries: Vec::new(),
                floors_sent: 0,
            });
        }
        let mut known = FilterKnown::new(filters);
        let mut fanout = self.open_stream_fanout(request_id, vector, None, true, filters)?;
        let mut parents: HashMap<u64, ParentAgg> = HashMap::new();
        let mut summaries: Vec<Option<StreamSearchSummary>> = vec![None; n_nodes];
        let mut remaining = n_nodes;
        // The k-th best parent score, recomputed lazily: only a batch
        // that raised some parent's best (or added a parent) can move
        // it, and floors derive from nothing else.
        let mut kth = f32::NEG_INFINITY;
        let mut last_floor = f32::NEG_INFINITY;
        let mut floors_sent = 0u64;
        while remaining > 0 {
            let (shard, msg) = match fanout.next_message(&summaries).await {
                Ok(Some(pair)) => pair,
                Ok(None) => continue,
                Err(status) => return fanout.cancel_with(status).await,
            };
            match msg.payload {
                Some(stream_search_response::Payload::Batch(batch)) => {
                    // Packed 20-byte LE records: u64 global id, f32
                    // score, u64 parent (see StreamSearchBatch).
                    if batch.hits.len() % 20 != 0 {
                        let status = Status::internal(format!(
                            "shard {shard} sent a misaligned collapse batch of {} bytes",
                            batch.hits.len()
                        ));
                        return fanout.cancel_with(status).await;
                    }
                    let mut dirty = false;
                    for rec in batch.hits.as_chunks::<20>().0 {
                        let doc = u64::from_le_bytes(rec[..8].try_into().expect("8-byte id"));
                        let score =
                            f32::from_le_bytes(rec[8..12].try_into().expect("4-byte score"));
                        let parent =
                            u64::from_le_bytes(rec[12..20].try_into().expect("8-byte parent"));
                        let agg = parents.entry(parent).or_insert_with(|| {
                            dirty = true;
                            ParentAgg {
                                best_score: f32::NEG_INFINITY,
                                best_id: u64::MAX,
                                chunks: Vec::new(),
                            }
                        });
                        agg.chunks.push((doc, score));
                        if score > agg.best_score || (score == agg.best_score && doc < agg.best_id)
                        {
                            if score > agg.best_score && score > kth {
                                dirty = true;
                            }
                            agg.best_score = score;
                            agg.best_id = doc;
                        }
                    }
                    if dirty && parents.len() >= k as usize {
                        let mut bests: Vec<f32> = parents.values().map(|a| a.best_score).collect();
                        let idx = k as usize - 1;
                        let new_kth = *bests.select_nth_unstable_by(idx, |a, b| b.total_cmp(a)).1;
                        if new_kth > kth {
                            kth = new_kth;
                            // next_down, not bm25::floor_seed: parent
                            // scores are signed vector scores.
                            let floor = kth.next_down();
                            if floor > last_floor {
                                last_floor = floor;
                                floors_sent += 1;
                                self.push_stream_floor(&fanout, floor);
                            }
                        }
                    }
                }
                Some(stream_search_response::Payload::Summary(summary)) => {
                    if !summary.completed {
                        let status = Status::internal(format!(
                            "shard {shard} stopped before completing its scan"
                        ));
                        return fanout.cancel_with(status).await;
                    }
                    // The vector leg's half of the typo handshake: a
                    // filter column no shard resolves must refuse even
                    // when the stream completed cleanly.
                    if let Err(e) =
                        known.merge(&summary.geo_columns_known, &summary.filter_columns_known)
                    {
                        return fanout.cancel_with(e).await;
                    }
                    summaries[shard] = Some(summary);
                    fanout.mark_completed(shard);
                    remaining -= 1;
                }
                None => {}
            }
        }

        known.refuse_unknown(filters)?;

        // Rank parents (best desc, best chunk id asc — the collapse
        // fan-out's order), keep k, and filter each kept parent's
        // chunks to the final floor for a deterministic group: every
        // chunk at or above it was emitted, every chunk below it is
        // dropped whether or not it happened to arrive.
        let mut ranked: Vec<(u64, ParentAgg)> = parents.into_iter().collect();
        ranked.sort_by(|a, b| {
            b.1.best_score
                .total_cmp(&a.1.best_score)
                .then_with(|| a.1.best_id.cmp(&b.1.best_id))
        });
        let chunk_floor = if ranked.len() >= k as usize {
            ranked[k as usize - 1].1.best_score.next_down()
        } else {
            f32::NEG_INFINITY
        };
        ranked.truncate(k as usize);
        let mut hits = Vec::with_capacity(ranked.len());
        let mut groups = Vec::with_capacity(ranked.len());
        for (parent, agg) in ranked {
            hits.push(ScoredHit {
                vector_id: agg.best_id,
                score: agg.best_score,
                parent_id: parent,
            });
            let mut chunks: Vec<(u64, f32)> = agg
                .chunks
                .into_iter()
                .filter(|&(_, score)| score >= chunk_floor)
                .collect();
            chunks.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            groups.push(ParentGroup {
                parent_id: parent,
                chunks: chunks
                    .into_iter()
                    .map(|(doc, score)| ScoredHit {
                        vector_id: doc,
                        score,
                        parent_id: parent,
                    })
                    .collect(),
            });
        }
        Ok(CollapseStreamResult {
            hits,
            groups,
            chunk_floor,
            summaries: summaries
                .into_iter()
                .map(|s| s.expect("all summaries arrived"))
                .collect(),
            floors_sent,
        })
    }

    /// [`Self::fanout_search`] in collapse-by-parent mode: `k` means k
    /// distinct parent documents, each represented by its best chunk.
    /// Shards collapse locally (their floors are k-th best PARENT
    /// scores, so collaborative pruning strengthens); the coordinator
    /// dedupes parents that appear on multiple shards (opinions
    /// straddling a shard cut) keeping the better representative, then
    /// takes the global top-k parents.
    pub async fn fanout_search_collapse(
        &self,
        request_id: &str,
        vector: &[f32],
        k: u32,
        filters: &RequestFilters,
    ) -> Result<FanoutResult, Status> {
        let n_nodes = self.node_addrs.len();
        if n_nodes == 0 {
            return Err(Status::failed_precondition("no shard nodes configured"));
        }
        let ctx = ShardQueryCtx {
            request_id: Arc::from(request_id),
            vector: Arc::new(vector.to_vec()),
            k,
            tie_complete: false,
            collapse: true,
            filters: Arc::new(filters.clone()),
            tracker: Arc::new(Mutex::new(FloorTracker::new())),
            gfloor: Arc::new(watch::channel(f32::NEG_INFINITY).0),
            hedges: Arc::new(AtomicU64::new(0)),
            hedge_wins: Arc::new(AtomicU64::new(0)),
        };
        let (hedges, hedge_wins) = (Arc::clone(&ctx.hedges), Arc::clone(&ctx.hedge_wins));
        let mut known = FilterKnown::new(filters);

        let (done_tx, mut done_rx) =
            mpsc::channel::<(u32, f32, Result<SearchShardDone, Status>)>(n_nodes);
        for shard in 0..n_nodes {
            let primary = self.node_client(&self.node_addrs[shard])?;
            let replica = match self.replica_addrs.get(shard).and_then(|r| r.as_deref()) {
                Some(addr) => Some(self.node_client(addr)?),
                None => None,
            };
            let ctx = ctx.clone();
            let limits = self.limits;
            let done_tx = done_tx.clone();
            tokio::spawn(async move {
                let t0 = std::time::Instant::now();
                let result =
                    run_shard_with_hedge(shard as u32, primary, replica, ctx, limits).await;
                let wall_ms = t0.elapsed().as_secs_f32() * 1e3;
                let _ = done_tx.send((shard as u32, wall_ms, result)).await;
            });
        }
        drop(done_tx);

        let mut shard_hits: Vec<(u32, Vec<(u64, f32)>)> = Vec::with_capacity(n_nodes);
        let mut shard_stats: Vec<Option<ShardScanStats>> = Vec::with_capacity(n_nodes);
        let mut shard_wall_ms: Vec<(u32, f32)> = Vec::with_capacity(n_nodes);
        // Parent -> best hit. Tie-break inside a parent: score desc, then
        // vector id asc (globally unique), deterministic across arrival
        // orders.
        let mut best: HashMap<u64, ScoredHit> = HashMap::new();
        for _ in 0..n_nodes {
            match done_rx.recv().await {
                Some((shard, wall_ms, Ok(done))) => {
                    known.merge(&done.geo_columns_known, &done.filter_columns_known)?;
                    shard_hits.push((
                        shard,
                        done.hits.iter().map(|h| (h.vector_id, h.score)).collect(),
                    ));
                    for hit in done.hits {
                        let entry = best.entry(hit.parent_id).or_insert(hit);
                        if hit.score > entry.score
                            || (hit.score == entry.score && hit.vector_id < entry.vector_id)
                        {
                            *entry = hit;
                        }
                    }
                    shard_stats.push(done.stats);
                    shard_wall_ms.push((shard, wall_ms));
                }
                Some((shard, _, Err(e))) => {
                    return Err(Status::internal(format!("shard {shard} failed: {e}")));
                }
                None => {
                    return Err(Status::internal("fan-out completed without all shards"));
                }
            }
        }

        known.refuse_unknown(filters)?;

        let mut hits: Vec<ScoredHit> = best.into_values().collect();
        hits.sort_by(|a, b| {
            b.score
                .total_cmp(&a.score)
                .then_with(|| a.vector_id.cmp(&b.vector_id))
        });
        hits.truncate(k as usize);
        Ok(FanoutResult {
            hits,
            shard_stats,
            shard_hits,
            shard_wall_ms,
            hedges_fired: hedges.load(AtomicOrdering::Relaxed),
            hedge_wins: hedge_wins.load(AtomicOrdering::Relaxed),
        })
    }

    /// Cascade hybrid (FUSION_MODE_CASCADE): no score fusion.
    ///
    /// Phase 1 — candidate generation through the EXISTING floor-sharing
    /// bidi vector path with the tie-complete option: cross-shard early
    /// termination applies (the cutoff is the savings), and every doc
    /// tied at the boundary score survives on every shard. The pool is
    /// `{score >= s_k}` where `s_k` is the global k-th vector score, so
    /// it can exceed k by the boundary tie-group size — score-defined,
    /// hence layout-invariant.
    ///
    /// Phase 2 — BM25 rerank behind the rescore seam: analyze the query,
    /// route each candidate to its owning shard (Bm25Rescore with the
    /// global stats), then rerank the pool by BM25 desc, vector desc,
    /// doc id asc, and return the top `k`. More rankers plug in behind
    /// this same seam later (one stage, no framework).
    #[allow(clippy::too_many_arguments)]
    pub async fn fanout_cascade(
        &self,
        request_id: &str,
        text: &str,
        vector: &[f32],
        k: u32,
        spec: Option<&crate::pb::AnalysisSpec>,
        min_vector_score: f32,
        debug: bool,
        filters: &RequestFilters,
    ) -> Result<(Vec<CascadeHit>, Option<HybridDebug>), Status> {
        if k == 0 || vector.is_empty() {
            return Ok((Vec::new(), None));
        }
        let t_total = std::time::Instant::now();
        // Phase 1: floor-shared, tie-complete vector candidates.
        let t_legs = std::time::Instant::now();
        // Phase 1 carries the filters, so the candidate gate is the
        // filtered corpus; phase 2 reranks that pool and never widens
        // it, so no unfiltered document can reappear.
        let phase1 = if self.clustered_vectors.is_some() {
            let candidates = self
                .clustered_vector_candidates(request_id, vector, k, None, true, filters)
                .await?;
            let ranges = self.product_label_ranges().await?;
            let mut shard_hits: Vec<(u32, Vec<(u64, f32)>)> = (0..self.node_addrs.len())
                .map(|shard| (shard as u32, Vec::new()))
                .collect();
            for (doc_id, score) in candidates.hits {
                let owner = Self::product_owner(&ranges, doc_id)?;
                shard_hits[owner as usize].1.push((doc_id, score));
            }
            FanoutResult {
                shard_stats: vec![None; self.node_addrs.len()],
                shard_wall_ms: (0..self.node_addrs.len())
                    .map(|shard| (shard as u32, 0.0))
                    .collect(),
                shard_hits,
                hits: Vec::new(),
                hedges_fired: 0,
                hedge_wins: 0,
            }
        } else {
            self.fanout_search(request_id, vector, k, true, filters)
                .await?
        };
        let phase1_ms = t_legs.elapsed().as_secs_f32() * 1e3;
        let mut all: Vec<(u64, u32, f64)> = Vec::new(); // (doc_id, shard, score)
        for (shard, hits) in &phase1.shard_hits {
            for &(doc_id, score) in hits {
                all.push((doc_id, *shard, f64::from(score)));
            }
        }
        all.sort_by(|a, b| b.2.total_cmp(&a.2).then_with(|| a.0.cmp(&b.0)));
        let boundary = if all.len() >= k as usize {
            all[k as usize - 1].2
        } else {
            f64::NEG_INFINITY
        };
        // The vector-score floor tightens the gate: the effective cutoff
        // is max(k-th score, floor), applied before the rescore fan-out
        // so filtered-out candidates cost nothing in phase 2.
        let boundary = boundary.max(if min_vector_score > 0.0 {
            f64::from(min_vector_score)
        } else {
            f64::NEG_INFINITY
        });
        let pool: Vec<(u64, u32, f64)> = all.into_iter().filter(|h| h.2 >= boundary).collect();

        // Query analysis + global BM25 stats for phase 2.
        let addr = self.analysis_addr.clone().ok_or_else(|| {
            Status::unavailable("no analysis backend configured on the coordinator (analysis_addr)")
        })?;
        let t = std::time::Instant::now();
        let analyzed = crate::analyzer::analyze_document(&addr, text, spec).await?;
        let analysis_ms = t.elapsed().as_secs_f32() * 1e3;
        let mut terms: Vec<String> = Vec::new();
        for (term, _, _) in analyzed.into_body().terms {
            if !terms.contains(&term) {
                terms.push(term);
            }
        }
        // Phase 2: route candidates to their owning shards for
        // rescoring. Stats + rescore run as a round (a stale-stats
        // refusal reruns them once with fresh stats and no claim).
        let t = std::time::Instant::now();
        let mut by_shard: std::collections::HashMap<u32, Vec<u64>> =
            std::collections::HashMap::new();
        for (doc_id, shard, _) in &pool {
            by_shard.entry(*shard).or_default().push(*doc_id);
        }
        let mut fresh = false;
        let (stats_ms, t_rescore, bm25_of, rescore_debug) = loop {
            let (global, epochs) = self.body_stats(&terms, fresh).await?;
            let claims = if fresh { vec![0; epochs.len()] } else { epochs };
            let stats_ms = t.elapsed().as_secs_f32() * 1e3;
            let t_rescore = std::time::Instant::now();
            match self
                .cascade_rescore_round(&terms, &global, &claims, &by_shard)
                .await
            {
                Err(e) if !fresh && is_stale_stats(&e) => {
                    self.stats_cache.invalidate_all();
                    fresh = true;
                }
                Err(e) => return Err(e),
                Ok((bm25_of, rescore_debug)) => {
                    break (stats_ms, t_rescore, bm25_of, rescore_debug);
                }
            }
        };
        let rescore_ms = t_rescore.elapsed().as_secs_f32() * 1e3;

        // Rerank: BM25 desc, vector score desc, doc id asc. Top k of the
        // (possibly larger) tie-extended pool.
        let t_fusion = std::time::Instant::now();
        let mut ranked: Vec<CascadeHit> = pool
            .into_iter()
            .map(|(doc_id, shard, vector_score)| CascadeHit {
                doc_id,
                rank: 0,
                shard,
                vector_score: vector_score as f32,
                bm25_score: bm25_of.get(&doc_id).copied().unwrap_or(0.0) as f32,
                boost_score: 0.0,
            })
            .collect();
        ranked.sort_by(|a, b| {
            b.bm25_score
                .total_cmp(&a.bm25_score)
                .then_with(|| b.vector_score.total_cmp(&a.vector_score))
                .then_with(|| a.doc_id.cmp(&b.doc_id))
        });
        ranked.truncate(k as usize);
        for (i, hit) in ranked.iter_mut().enumerate() {
            hit.rank = i as u32 + 1;
        }
        let dbg = debug.then(|| {
            // Per-shard: phase-1 wall + this shard's rescore wall, the
            // phase-1 candidate count, the rescored count, and the vector
            // scan's stats. Vectors from fanout_search are in completion
            // order; re-key by shard.
            let mut walls: std::collections::HashMap<u32, f32> =
                phase1.shard_wall_ms.iter().copied().collect();
            let mut scans: std::collections::HashMap<u32, Option<ShardScanStats>> = phase1
                .shard_hits
                .iter()
                .zip(&phase1.shard_stats)
                .map(|((shard, _), stats)| (*shard, *stats))
                .collect();
            let mut shards: Vec<HybridShardDebug> = phase1
                .shard_hits
                .iter()
                .map(|(shard, hits)| {
                    let (rescore_wall, rescored) =
                        rescore_debug.get(shard).copied().unwrap_or((0.0, 0));
                    HybridShardDebug {
                        shard: *shard,
                        rpc_ms: walls.remove(shard).unwrap_or(0.0) + rescore_wall,
                        vector_hits: hits.len() as u32,
                        bm25_hits: rescored,
                        scan: scans.remove(shard).flatten(),
                    }
                })
                .collect();
            shards.sort_by_key(|s| s.shard);
            HybridDebug {
                fusion_mode: FusionMode::Cascade as i32,
                leg_k: k,
                terms,
                analysis_ms,
                stats_ms,
                legs_ms: phase1_ms + rescore_ms,
                fusion_ms: t_fusion.elapsed().as_secs_f32() * 1e3,
                total_ms: t_total.elapsed().as_secs_f32() * 1e3,
                shards,
                boost_ms: 0.0,
                boost_terms: Vec::new(),
            }
        });
        Ok((ranked, dbg))
    }
    /// Candidate-scoped LEXICAL signal for the public route's boost
    /// phase: analyze `text` under `spec`, then score `ids` against
    /// the analyzed terms with GLOBAL stats through the `Bm25Rescore`
    /// seam, broadcast to every shard — the rescore contract ignores
    /// ids a shard does not own, so no id-to-shard map is needed.
    /// Returns doc -> score for exactly the candidates matching at
    /// least one term; absence means the document matched nothing.
    pub async fn lexical_signal(
        &self,
        text: &str,
        spec: Option<&crate::pb::AnalysisSpec>,
        ids: &[u64],
    ) -> Result<HashMap<u64, f32>, Status> {
        if text.is_empty() {
            return Err(Status::invalid_argument(
                "boost.text must be non-empty when boost is present",
            ));
        }
        let addr = self.analysis_addr.clone().ok_or_else(|| {
            Status::unavailable("no analysis backend configured on the coordinator (analysis_addr)")
        })?;
        let analyzed = crate::analyzer::analyze_document(&addr, text, spec).await?;
        let mut terms: Vec<String> = Vec::new();
        for (term, _, _) in analyzed.into_body().terms {
            if !terms.contains(&term) {
                terms.push(term);
            }
        }
        self.lexical_signal_terms(&terms, ids, None).await
    }

    /// Candidate-scoped lexical scoring when the planner already analyzed the
    /// clause during membership resolution. `expected_epochs` closes the gap
    /// between that bitmap and this rescore: any lexical mutation aborts the
    /// plan so the caller can rebuild it once from a fresh snapshot.
    pub async fn lexical_signal_terms(
        &self,
        terms: &[String],
        ids: &[u64],
        expected_epochs: Option<&[u64]>,
    ) -> Result<HashMap<u64, f32>, Status> {
        self.lexical_signal_terms_with_stages(terms, ids, expected_epochs, &[])
            .await
    }

    /// [`Self::lexical_signal_terms`] with the ordinary lexical score-stage
    /// chain applied on each owning shard before the final f32 conversion.
    pub async fn lexical_signal_terms_with_stages(
        &self,
        terms: &[String],
        ids: &[u64],
        expected_epochs: Option<&[u64]>,
        score_stages: &[crate::pb::ScoreStage],
    ) -> Result<HashMap<u64, f32>, Status> {
        if terms.is_empty() || ids.is_empty() {
            return Ok(HashMap::new());
        }
        let by_shard: HashMap<u32, Vec<u64>> = (0..self.node_addrs.len())
            .map(|s| (s as u32, ids.to_vec()))
            .collect();
        // Stats + rescore run as a round (a stale-stats refusal reruns
        // them once with fresh stats and no claim) — the same protocol
        // as every other stats consumer.
        let mut fresh = false;
        loop {
            let (global, epochs) = self.body_stats(terms, fresh).await?;
            if let Some(expected) = expected_epochs {
                if expected != epochs {
                    self.stats_cache.invalidate_all();
                    return Err(Status::aborted(
                        "boolean membership epoch changed before lexical scoring",
                    ));
                }
            }
            let claims = if fresh { vec![0; epochs.len()] } else { epochs };
            match self
                .fanout_bm25_rescore_scores(terms, &global, &claims, by_shard.clone(), score_stages)
                .await
            {
                Err(e) if !fresh && is_stale_stats(&e) => {
                    if expected_epochs.is_some() {
                        self.stats_cache.invalidate_all();
                        return Err(Status::aborted(
                            "boolean membership epoch changed during lexical scoring",
                        ));
                    }
                    self.stats_cache.invalidate_all();
                    fresh = true;
                }
                Err(e) => return Err(e),
                Ok(scores) => return Ok(scores),
            }
        }
    }

    /// Candidate-scoped DENSE signal for the public route's boost
    /// phase (the `VectorRescore` seam), broadcast to every shard.
    /// Present exactly for the candidates that carry a vector; scores
    /// are bitwise the same calibrated products a full search emits.
    pub async fn dense_signal(
        &self,
        vector: &[f32],
        ids: &[u64],
    ) -> Result<HashMap<u64, f32>, Status> {
        if vector.is_empty() {
            return Err(Status::invalid_argument(
                "a dense boost needs a non-empty vector",
            ));
        }
        if ids.is_empty() {
            return Ok(HashMap::new());
        }
        if let Some(clustered) = &self.clustered_vectors {
            let k = u32::try_from(ids.len()).map_err(|_| {
                Status::resource_exhausted("dense boost candidate set does not fit u32")
            })?;
            let response = clustered
                .search(
                    vector.to_vec(),
                    k,
                    Some(ClusteredLabelFilter::Labels(ids.to_vec())),
                    None,
                    false,
                )
                .await?;
            if response.results.len() != 1 {
                return Err(Status::internal(format!(
                    "clustered TurboVec returned {} query results for one dense boost",
                    response.results.len()
                )));
            }
            return response.results[0]
                .neighbours
                .iter()
                .map(|neighbour| {
                    neighbour
                        .label
                        .map(|label| (label, neighbour.score))
                        .ok_or_else(|| {
                            Status::failed_precondition(
                                "clustered TurboVec dense boosts require stable labels",
                            )
                        })
                })
                .collect();
        }
        let by_shard: HashMap<u32, Vec<u64>> = (0..self.node_addrs.len())
            .map(|s| (s as u32, ids.to_vec()))
            .collect();
        self.fanout_vector_rescore(vector, by_shard).await
    }

    /// Score a fixed candidate set against the product-owned original FP32
    /// rows. Every requested id must resolve exactly once across the product
    /// shards. Selection may come from the embedded or clustered provider;
    /// stable labels route back to these product-owned rows in either case.
    pub(crate) async fn exact_vector_scores(
        &self,
        vector: &[f32],
        ids: &[u64],
    ) -> Result<ExactRerankScores, Status> {
        if vector.is_empty() {
            return Err(Status::invalid_argument(
                "FP32 rerank needs a non-empty query vector",
            ));
        }
        if ids.is_empty() {
            return Ok(ExactRerankScores {
                scores: HashMap::new(),
                rows: 0,
                logical_bytes: 0,
                pages_touched: 0,
                tasks: 0,
            });
        }
        let mut requested = ids.to_vec();
        requested.sort_unstable();
        requested.dedup();
        let logical_bytes = (requested.len() as u64)
            .checked_mul(vector.len() as u64)
            .and_then(|bytes| bytes.checked_mul(4))
            .ok_or_else(|| Status::resource_exhausted("FP32 rerank byte count overflow"))?;
        if logical_bytes > self.max_rerank_bytes {
            return Err(Status::resource_exhausted(format!(
                "FP32 rerank needs {logical_bytes} logical row bytes for {} candidates at dim {}, above coordinator max_rerank_bytes={}",
                requested.len(),
                vector.len(),
                self.max_rerank_bytes
            )));
        }
        let mut tasks = Vec::with_capacity(self.node_addrs.len());
        for addr in &self.node_addrs {
            let request = ExactVectorRescoreRequest {
                vector: vector.to_vec(),
                candidate_ids: requested.clone(),
                max_logical_bytes: self.max_rerank_bytes,
            };
            let mut client = self.node_client(addr)?;
            let deadline = self.limits.shard_deadline;
            tasks.push(tokio::spawn(async move {
                let call = client.exact_vector_rescore(request);
                match deadline {
                    Some(limit) => tokio::time::timeout(limit, call)
                        .await
                        .map_err(|_| {
                            Status::deadline_exceeded(
                                "exact vector rescore shard deadline exceeded",
                            )
                        })?
                        .map(tonic::Response::into_inner),
                    None => call.await.map(tonic::Response::into_inner),
                }
            }));
        }
        let mut scores = HashMap::with_capacity(requested.len());
        let mut observed_bytes = 0u64;
        let mut pages_touched = 0u64;
        let mut worker_tasks = 0u32;
        for task in tasks {
            let response = task.await.map_err(|e| {
                Status::internal(format!("exact vector rescore task failed: {e}"))
            })??;
            observed_bytes = observed_bytes
                .checked_add(response.logical_bytes)
                .ok_or_else(|| Status::internal("exact rerank byte metrics overflow"))?;
            pages_touched = pages_touched
                .checked_add(response.pages_touched)
                .ok_or_else(|| Status::internal("exact rerank page metrics overflow"))?;
            worker_tasks = worker_tasks.saturating_add(response.tasks);
            for hit in response.hits {
                if scores.insert(hit.doc_id, hit.score).is_some() {
                    return Err(Status::failed_precondition(format!(
                        "FP32 rerank candidate {} is owned by more than one product shard; \
                         slot ranges overlap",
                        hit.doc_id
                    )));
                }
            }
        }
        if let Some(missing) = requested.iter().find(|id| !scores.contains_key(id)) {
            return Err(Status::failed_precondition(format!(
                "FP32 rerank candidate {missing} has no exact-vector row on any product shard"
            )));
        }
        if observed_bytes != logical_bytes {
            return Err(Status::failed_precondition(format!(
                "FP32 rerank shards reported {observed_bytes} logical bytes, expected {logical_bytes}; exact-row dimensions or ownership disagree"
            )));
        }
        Ok(ExactRerankScores {
            scores,
            rows: requested.len() as u64,
            logical_bytes,
            pages_touched,
            tasks: worker_tasks,
        })
    }

    /// Candidate-scoped value fan-out (the `FetchValues` seam),
    /// broadcast — shards ignore ids they do not own. Projections keep
    /// the ordinary value semantics (absence in, absence out) and the
    /// ordinary typo rule (a column-read leaf NO shard resolves is
    /// refused by name); stages evaluate at their identity score with
    /// the same typo rule as the lexical route's chain.
    pub async fn fetch_values(
        &self,
        ids: &[u64],
        projections: &[crate::pb::CompiledProjection],
        stages: &[crate::pb::ScoreStage],
    ) -> Result<FetchedValues, Status> {
        // Stage parameters validate here too, so a malformed stage is
        // refused by name before any fan-out.
        crate::node::parse_score_stages(stages)?;
        let mut out = FetchedValues {
            rows: HashMap::new(),
            stage_rows: vec![HashMap::new(); stages.len()],
        };
        // An empty candidate list still fans out when anything was
        // named: the typo rules run on the flags, not the rows.
        if projections.is_empty() && stages.is_empty() {
            return Ok(out);
        }
        let mut tasks = Vec::with_capacity(self.node_addrs.len());
        for node in &self.node_addrs {
            let request = crate::pb::FetchValuesRequest {
                candidate_ids: ids.to_vec(),
                projections: projections.to_vec(),
                stages: stages.to_vec(),
            };
            let mut client = self.node_client(node)?;
            tasks.push(tokio::spawn(async move {
                client.fetch_values(request).await.map(|r| r.into_inner())
            }));
        }
        let projection_leaves: Vec<crate::values::ValueLeaf> = {
            let mut leaves = Vec::new();
            for p in projections {
                if let Some(expr) = p.expr.as_ref() {
                    crate::values::column_leaves(expr, &mut leaves);
                }
            }
            leaves
        };
        let mut stage_known = vec![false; stages.len()];
        let mut projection_known = vec![false; projection_leaves.len()];
        for task in tasks {
            let resp = task
                .await
                .map_err(|e| Status::internal(format!("fetch values task failed: {e}")))??;
            for (known, shard) in stage_known.iter_mut().zip(&resp.stage_columns_known) {
                *known |= shard;
            }
            for (known, shard) in projection_known
                .iter_mut()
                .zip(&resp.projection_leaves_known)
            {
                *known |= shard;
            }
            for row in resp.rows {
                for (i, sv) in row.stage_values.iter().enumerate() {
                    if let Some(crate::pb::projected_value::Value::DoubleValue(v)) = sv.value {
                        out.stage_rows[i].insert(row.doc_id, v);
                    }
                }
                out.rows.insert(row.doc_id, row.values);
            }
        }
        for (stage, known) in stages.iter().zip(&stage_known) {
            if !known {
                return Err(Status::invalid_argument(format!(
                    "no shard has numeric column {}: the stored-value dimension would be \
                     a silent no-op. Check the spelling, or the nodes' --numeric-fields / \
                     --integer-fields / --map-numeric-fields / --geo-fields.",
                    stage.column
                )));
            }
        }
        let unknown_projection: Vec<String> = projection_leaves
            .iter()
            .zip(&projection_known)
            .filter(|(_, known)| !**known)
            .map(|(leaf, _)| leaf.describe())
            .collect();
        if !unknown_projection.is_empty() {
            return Err(Status::invalid_argument(format!(
                "projection: no shard has column {}: every value would be absent. \
                 Check the spelling, or the nodes' --numeric-fields / --integer-fields \
                 / --facet-fields / --map-numeric-fields / --map-facet-fields.",
                unknown_projection.join(", ")
            )));
        }
        Ok(out)
    }

    /// Second-pass lexical boost (see the proto's `BoostRescore`): score
    /// the top-`window` hits against the boost query's terms through the
    /// candidate-scoped `Bm25Rescore` seam and reorder the window by
    /// `base_weight * base + boost_weight * boost`; hits beyond the
    /// window keep their relative order after it. Exactly one of
    /// `hits` / `cascade_hits` is non-empty; `base` is the fused score
    /// for the former, the phase-2 BM25 score for the latter (whose
    /// ranks are reassigned to the final order).
    pub async fn apply_boost(
        &self,
        boost: &crate::pb::BoostRescore,
        spec: Option<&crate::pb::AnalysisSpec>,
        hits: &mut [HybridHit],
        cascade_hits: &mut [CascadeHit],
        debug: &mut Option<HybridDebug>,
    ) -> Result<(), Status> {
        if boost.text.is_empty() {
            return Err(Status::invalid_argument(
                "boost.text must be non-empty when boost is present",
            ));
        }
        let t0 = std::time::Instant::now();
        let base_w = if boost.base_weight == 0.0 {
            1.0
        } else {
            f64::from(boost.base_weight)
        };
        let boost_w = if boost.boost_weight == 0.0 {
            1.0
        } else {
            f64::from(boost.boost_weight)
        };
        let len = hits.len().max(cascade_hits.len());
        let window = if boost.window == 0 {
            len
        } else {
            (boost.window as usize).min(len)
        };

        // Analyze the boost text with the SAME options as the main query
        // (term identity must match the index, as everywhere).
        let addr = self.analysis_addr.clone().ok_or_else(|| {
            Status::unavailable("no analysis backend configured on the coordinator (analysis_addr)")
        })?;
        let analyzed = crate::analyzer::analyze_document(&addr, &boost.text, spec).await?;
        let mut terms: Vec<String> = Vec::new();
        for (term, _, _) in analyzed.into_body().terms {
            if !terms.contains(&term) {
                terms.push(term);
            }
        }

        // Candidate-scoped scoring of the window, routed by owning
        // shard. Stats + rescore run as a round (a stale-stats refusal
        // reruns them once with fresh stats and no claim).
        let mut scores: HashMap<u64, f64> = HashMap::new();
        if window > 0 && !terms.is_empty() {
            let mut by_shard: HashMap<u32, Vec<u64>> = HashMap::new();
            for (doc_id, shard) in hits[..window.min(hits.len())]
                .iter()
                .map(|h| (h.doc_id, h.shard))
                .chain(
                    cascade_hits[..window.min(cascade_hits.len())]
                        .iter()
                        .map(|h| (h.doc_id, h.shard)),
                )
            {
                by_shard.entry(shard).or_default().push(doc_id);
            }
            let mut fresh = false;
            let rescored = loop {
                let (global, epochs) = self.body_stats(&terms, fresh).await?;
                let claims = if fresh { vec![0; epochs.len()] } else { epochs };
                match self
                    .fanout_bm25_rescore_scores(&terms, &global, &claims, by_shard.clone(), &[])
                    .await
                {
                    Err(e) if !fresh && is_stale_stats(&e) => {
                        self.stats_cache.invalidate_all();
                        fresh = true;
                    }
                    Err(e) => return Err(e),
                    Ok(rescored) => break rescored,
                }
            };
            for (doc_id, score) in rescored {
                scores.insert(doc_id, f64::from(score));
            }
        }

        if !hits.is_empty() {
            let window = window.min(hits.len());
            for h in &mut hits[..window] {
                h.boost_score = scores.get(&h.doc_id).copied().unwrap_or(0.0) as f32;
            }
            hits[..window].sort_by(|a, b| {
                let fa = base_w * f64::from(a.fused_score) + boost_w * f64::from(a.boost_score);
                let fb = base_w * f64::from(b.fused_score) + boost_w * f64::from(b.boost_score);
                fb.total_cmp(&fa)
                    .then_with(|| a.shard.cmp(&b.shard))
                    .then_with(|| a.doc_id.cmp(&b.doc_id))
            });
        } else if !cascade_hits.is_empty() {
            let window = window.min(cascade_hits.len());
            for h in &mut cascade_hits[..window] {
                h.boost_score = scores.get(&h.doc_id).copied().unwrap_or(0.0) as f32;
            }
            cascade_hits[..window].sort_by(|a, b| {
                let fa = base_w * f64::from(a.bm25_score) + boost_w * f64::from(a.boost_score);
                let fb = base_w * f64::from(b.bm25_score) + boost_w * f64::from(b.boost_score);
                fb.total_cmp(&fa)
                    .then_with(|| b.vector_score.total_cmp(&a.vector_score))
                    .then_with(|| a.doc_id.cmp(&b.doc_id))
            });
            for (i, hit) in cascade_hits.iter_mut().enumerate() {
                hit.rank = i as u32 + 1;
            }
        }

        if let Some(d) = debug.as_mut() {
            d.boost_terms = terms;
            d.boost_ms = t0.elapsed().as_secs_f32() * 1e3;
            d.total_ms += d.boost_ms;
        }
        Ok(())
    }

    /// Push one TQ+ calibration to every configured node (the
    /// shared-calibration handshake that makes vector scores globally
    /// comparable). Per-node outcomes are reported, not fail-fast: a
    /// non-empty shard legitimately refuses (calibration is locked for
    /// the index lifetime), and the caller needs to know which nodes
    /// diverged.
    /// Filter-only browse fan-out (docs/query-api.md): each shard's
    /// admitted ids above the boundary, merged and truncated to k —
    /// ascending by id unsorted, by (order-preserving key bits, id)
    /// under `sort`. The typo rule holds here as on every filtered
    /// route: a column NO shard knows refuses by name, the sort column
    /// included.
    pub async fn fanout_browse(
        &self,
        k: u32,
        after: Option<BrowseAfter>,
        sort: Option<&crate::pb::BrowseSort>,
        filters: &RequestFilters,
    ) -> Result<BrowseRows, Status> {
        let k = self.resolve_k(k)?;
        let mut tasks = Vec::with_capacity(self.node_addrs.len());
        for node in &self.node_addrs {
            let request = crate::pb::BrowseShardRequest {
                k,
                after: after.as_ref().map_or(0, |a| a.id),
                first_page: after.is_none(),
                geo_filters: filters.geo.clone(),
                filter: filters.tree.clone(),
                sort: sort.cloned(),
                after_key_bits: after.as_ref().map_or(0, |a| a.key_bits),
            };
            let client = self.node_client(node);
            tasks.push(tokio::spawn(async move {
                client?.browse_shard(request).await.map(|r| r.into_inner())
            }));
        }
        let mut known = FilterKnown::new(filters);
        let mut sort_known = sort.is_none();
        // (merge key, id, reported value): key = adjusted key bits
        // sorted, or the id itself unsorted — one ascending comparison
        // either way.
        let mut rows: Vec<(u64, u64, f64)> = Vec::new();
        for task in tasks {
            let response = task
                .await
                .map_err(|e| Status::internal(format!("browse task failed: {e}")))??;
            known.merge(&response.geo_columns_known, &response.filter_columns_known)?;
            sort_known |= response.sort_column_known;
            if sort.is_some() {
                if response.sort_key_bits.len() != response.doc_ids.len()
                    || response.sort_keys.len() != response.doc_ids.len()
                {
                    return Err(Status::internal(
                        "shard answered a sorted browse with mismatched key columns",
                    ));
                }
                for ((&id, &bits), &value) in response
                    .doc_ids
                    .iter()
                    .zip(&response.sort_key_bits)
                    .zip(&response.sort_keys)
                {
                    rows.push((bits, id, value));
                }
            } else {
                rows.extend(response.doc_ids.iter().map(|&id| (id, id, 0.0)));
            }
        }
        known.refuse_unknown(filters)?;
        if !sort_known {
            let column = sort.map(|s| s.column.as_str()).unwrap_or_default();
            return Err(Status::invalid_argument(format!(
                "sort column {column:?} is not declared on any shard's numeric or integer \
                 table (--numeric-fields / --integer-fields)"
            )));
        }
        rows.sort_unstable_by_key(|r| (r.0, r.1));
        rows.truncate(k as usize);
        Ok(BrowseRows {
            ids: rows.iter().map(|r| r.1).collect(),
            key_bits: rows.iter().map(|r| r.0).collect(),
            keys: rows.iter().map(|r| r.2).collect(),
            sorted: sort.is_some(),
        })
    }

    fn merge_membership_bitmap(
        out: &mut MembershipSet,
        response: &crate::pb::MembershipBitmapResponse,
    ) -> Result<(), Status> {
        let label_count = usize::try_from(response.label_count).map_err(|_| {
            Status::resource_exhausted("membership label count does not fit this platform")
        })?;
        let expected = usize::try_from(response.label_count.div_ceil(8))
            .map_err(|_| Status::resource_exhausted("membership bitmap does not fit usize"))?;
        if response.bits.len() != expected {
            return Err(Status::internal(format!(
                "membership bitmap has {} bytes for {} labels; expected {expected}",
                response.bits.len(),
                response.label_count
            )));
        }
        let end = response
            .base_label
            .checked_add(response.label_count)
            .ok_or_else(|| Status::internal("membership bitmap label range overflows u64"))?;
        if response.label_count > 0 {
            if out
                .ranges
                .iter()
                .any(|&(held_base, held_end)| response.base_label < held_end && held_base < end)
            {
                return Err(Status::failed_precondition(format!(
                    "membership label range [{}, {end}) overlaps another shard",
                    response.base_label
                )));
            }
            out.ranges.push((response.base_label, end));
        }
        if !response.label_count.is_multiple_of(8)
            && response.bits.last().is_some_and(|last| {
                let used = (response.label_count % 8) as u8;
                *last & !((1u8 << used) - 1) != 0
            })
        {
            return Err(Status::internal(
                "membership bitmap sets padding bits beyond its label count",
            ));
        }
        out.wire_bytes = out
            .wire_bytes
            .checked_add(response.bits.len() as u64)
            .ok_or_else(|| Status::resource_exhausted("membership byte count overflow"))?;
        for (byte_index, byte) in response.bits.iter().copied().enumerate() {
            let mut held = byte;
            while held != 0 {
                let bit = held.trailing_zeros() as usize;
                let position = byte_index * 8 + bit;
                if position < label_count {
                    let id = response
                        .base_label
                        .checked_add(position as u64)
                        .ok_or_else(|| Status::internal("membership stable-id overflow"))?;
                    if !out.ids.insert(id) {
                        return Err(Status::failed_precondition(format!(
                            "membership id {id} is owned by more than one shard; slot ranges overlap"
                        )));
                    }
                }
                held &= held - 1;
            }
        }
        Ok(())
    }

    /// Resolve the live document universe, or one CEL/geo predicate, without
    /// paging through `max_k`-sized browse responses.
    pub async fn filter_membership(
        &self,
        filters: &RequestFilters,
    ) -> Result<MembershipSet, Status> {
        let mut tasks = Vec::with_capacity(self.node_addrs.len());
        for node in &self.node_addrs {
            let request = crate::pb::FilterBitmapRequest {
                geo_filters: filters.geo.clone(),
                filter: filters.tree.clone(),
            };
            let client = self.node_client(node);
            tasks.push(tokio::spawn(async move {
                client?
                    .resolve_filter_bitmap(request)
                    .await
                    .map(|response| response.into_inner())
            }));
        }
        let mut known = FilterKnown::new(filters);
        let mut merged = MembershipSet::default();
        for task in tasks {
            let response = task.await.map_err(|error| {
                Status::internal(format!("filter membership task failed: {error}"))
            })??;
            known.merge(&response.geo_columns_known, &response.filter_columns_known)?;
            Self::merge_membership_bitmap(
                &mut merged,
                &crate::pb::MembershipBitmapResponse {
                    base_label: response.base_label,
                    label_count: response.label_count,
                    bits: response.bits,
                    stats_epoch: 0,
                },
            )?;
        }
        known.refuse_unknown(filters)?;
        Ok(merged)
    }

    /// Analyze one lexical clause and resolve its exact positive-score
    /// membership. No score bytes cross this phase.
    pub async fn lexical_membership(
        &self,
        text: &str,
        spec: Option<&crate::pb::AnalysisSpec>,
    ) -> Result<MembershipSet, Status> {
        if text.is_empty() {
            return Err(Status::invalid_argument("lexical clause text is empty"));
        }
        let addr = self.analysis_addr.clone().ok_or_else(|| {
            Status::unavailable("no analysis backend configured on the coordinator (analysis_addr)")
        })?;
        let analyzed = crate::analyzer::analyze_document(&addr, text, spec).await?;
        let mut terms = Vec::new();
        for (term, _, _) in analyzed.into_body().terms {
            if !terms.contains(&term) {
                terms.push(term);
            }
        }
        if terms.is_empty() {
            return Ok(MembershipSet {
                epochs: vec![0; self.node_addrs.len()],
                ..Default::default()
            });
        }
        let mut tasks = Vec::with_capacity(self.node_addrs.len());
        for node in &self.node_addrs {
            let client = self.node_client(node);
            let request = crate::pb::LexicalBitmapRequest {
                terms: terms.clone(),
            };
            tasks.push(tokio::spawn(async move {
                client?
                    .resolve_lexical_bitmap(request)
                    .await
                    .map(|response| response.into_inner())
            }));
        }
        let mut merged = MembershipSet::default();
        for task in tasks {
            let response = task.await.map_err(|error| {
                Status::internal(format!("lexical membership task failed: {error}"))
            })??;
            merged.epochs.push(response.stats_epoch);
            Self::merge_membership_bitmap(&mut merged, &response)?;
        }
        merged.terms = terms;
        Ok(merged)
    }

    /// Resolve every live provider-backed vector row. This is the membership
    /// rule of a dense Boolean clause; native scores are fetched only for the
    /// candidates that survive the Boolean set plan.
    pub async fn vector_membership(&self) -> Result<MembershipSet, Status> {
        let mut tasks = Vec::with_capacity(self.node_addrs.len());
        for node in &self.node_addrs {
            let client = self.node_client(node);
            tasks.push(tokio::spawn(async move {
                client?
                    .resolve_vector_bitmap(crate::pb::VectorBitmapRequest {})
                    .await
                    .map(|response| response.into_inner())
            }));
        }
        let mut merged = MembershipSet::default();
        for task in tasks {
            let response = task.await.map_err(|error| {
                Status::internal(format!("vector membership task failed: {error}"))
            })??;
            Self::merge_membership_bitmap(&mut merged, &response)?;
        }
        Ok(merged)
    }

    /// Resolve a product filter into one packed stable-id bitmap per product
    /// shard for a vector provider that does not own document columns. No
    /// filter remains `None` and therefore costs no shard pass. An explicitly
    /// present empty bitmap set is an intentional match-none set and stays
    /// distinguishable at the provider boundary.
    async fn clustered_allowed_labels(
        &self,
        filters: &RequestFilters,
    ) -> Result<Option<ClusteredLabelFilter>, Status> {
        if filters.geo.is_empty() && filters.tree.is_none() {
            return Ok(None);
        }

        let mut tasks = Vec::with_capacity(self.node_addrs.len());
        for node in &self.node_addrs {
            let request = crate::pb::FilterBitmapRequest {
                geo_filters: filters.geo.clone(),
                filter: filters.tree.clone(),
            };
            let client = self.node_client(node);
            tasks.push(tokio::spawn(async move {
                client?
                    .resolve_filter_bitmap(request)
                    .await
                    .map(|response| response.into_inner())
            }));
        }

        let mut known = FilterKnown::new(filters);
        let mut bitmaps = Vec::with_capacity(tasks.len());
        for task in tasks {
            let response = task.await.map_err(|error| {
                Status::internal(format!("filter bitmap task failed: {error}"))
            })??;
            known.merge(&response.geo_columns_known, &response.filter_columns_known)?;
            if response.label_count == 0 {
                if !response.bits.is_empty() {
                    return Err(Status::internal(
                        "shard answered an empty filter bitmap with payload bytes",
                    ));
                }
                continue;
            }
            let expected_bytes =
                usize::try_from(response.label_count.div_ceil(8)).map_err(|_| {
                    Status::internal("shard filter bitmap length does not fit this process")
                })?;
            if response.bits.len() != expected_bytes {
                return Err(Status::internal(format!(
                    "shard answered {} filter bitmap bytes for {} labels; expected {expected_bytes}",
                    response.bits.len(),
                    response.label_count
                )));
            }
            response
                .base_label
                .checked_add(response.label_count)
                .ok_or_else(|| Status::internal("shard filter bitmap label range overflows u64"))?;
            bitmaps.push(turbovec_grpc::proto::LabelBitmap {
                base_label: response.base_label,
                label_count: response.label_count,
                bits: response.bits,
            });
        }
        known.refuse_unknown(filters)?;
        bitmaps.sort_unstable_by_key(|bitmap| bitmap.base_label);
        for pair in bitmaps.windows(2) {
            let previous_end = pair[0]
                .base_label
                .checked_add(pair[0].label_count)
                .expect("bitmap range was validated above");
            if pair[1].base_label < previous_end {
                return Err(Status::internal(
                    "product shard filter bitmap label ranges overlap",
                ));
            }
        }
        Ok(Some(ClusteredLabelFilter::Bitmaps(bitmaps)))
    }

    /// Product ownership ranges, independent of vector shard cuts. Hybrid
    /// provenance, BM25 rescoring, and lineage lookup all route by this map;
    /// the clustered provider remains unaware of product shard meaning.
    async fn product_label_ranges(&self) -> Result<Vec<ProductLabelRange>, Status> {
        let mut tasks = Vec::with_capacity(self.node_addrs.len());
        for (shard, node) in self.node_addrs.iter().enumerate() {
            let client = self.node_client(node);
            tasks.push(tokio::spawn(async move {
                client?
                    .health(HealthRequest {})
                    .await
                    .map(|response| (shard as u32, response.into_inner()))
            }));
        }
        let mut ranges = Vec::with_capacity(tasks.len());
        for task in tasks {
            let (shard, health) = task
                .await
                .map_err(|error| Status::internal(format!("health task failed: {error}")))??;
            let count = health.num_vectors.max(health.bm25_docs);
            if count == 0 {
                continue;
            }
            let end = health.slot_offset.checked_add(count).ok_or_else(|| {
                Status::failed_precondition("product shard label range overflows u64")
            })?;
            ranges.push(ProductLabelRange {
                start: health.slot_offset,
                end,
                shard,
            });
        }
        ranges.sort_unstable_by_key(|range| range.start);
        for pair in ranges.windows(2) {
            if pair[1].start < pair[0].end {
                return Err(Status::failed_precondition(format!(
                    "product shard label ranges overlap: [{}, {}) and [{}, {})",
                    pair[0].start, pair[0].end, pair[1].start, pair[1].end
                )));
            }
        }
        Ok(ranges)
    }

    fn product_owner(ranges: &[ProductLabelRange], label: u64) -> Result<u32, Status> {
        ranges
            .iter()
            .copied()
            .find(|range| range.contains(label))
            .map(|range| range.shard)
            .ok_or_else(|| {
                Status::failed_precondition(format!(
                    "clustered TurboVec label {label} has no product shard owner"
                ))
            })
    }

    /// Resolve lineage in compact batches on the owning product shards. A
    /// document without stored lineage parents itself under the same tagged
    /// domain as the embedded path; raw document text never crosses this seam.
    async fn product_parent_ids(
        &self,
        ranges: &[ProductLabelRange],
        labels: &[u64],
    ) -> Result<HashMap<u64, u64>, Status> {
        let mut by_shard: HashMap<u32, Vec<u64>> = HashMap::new();
        for &label in labels {
            by_shard
                .entry(Self::product_owner(ranges, label)?)
                .or_default()
                .push(label);
        }
        let mut tasks = Vec::with_capacity(by_shard.len());
        for (shard, labels) in by_shard {
            let mut client = self.node_client(&self.node_addrs[shard as usize])?;
            tasks.push(tokio::spawn(async move {
                client
                    .resolve_parents(crate::pb::ResolveParentsRequest {
                        doc_ids: labels.clone(),
                    })
                    .await
                    .map(|response| (labels, response.into_inner()))
            }));
        }
        let mut parents = HashMap::with_capacity(labels.len());
        for task in tasks {
            let (requested, response) = task.await.map_err(|error| {
                Status::internal(format!("document lineage task failed: {error}"))
            })??;
            let requested: std::collections::HashSet<u64> = requested.into_iter().collect();
            for resolved in response.parents {
                if !requested.contains(&resolved.doc_id) {
                    return Err(Status::internal(format!(
                        "product shard returned unrequested document {}",
                        resolved.doc_id
                    )));
                }
                if parents
                    .insert(resolved.doc_id, resolved.parent_id)
                    .is_some()
                {
                    return Err(Status::internal(format!(
                        "product shard returned duplicate parent resolution for {}",
                        resolved.doc_id
                    )));
                }
            }
        }
        for &label in labels {
            if !parents.contains_key(&label) {
                return Err(Status::failed_precondition(format!(
                    "product shard did not resolve parent identity for clustered label {label}"
                )));
            }
        }
        Ok(parents)
    }

    /// Collect one exact provider top-k. The product owns the only heap and
    /// drives its inclusive live floor. Tie-complete mode retains the entire
    /// final boundary group for cascade.
    async fn clustered_vector_candidates(
        &self,
        request_id: &str,
        vector: &[f32],
        k: u32,
        initial_floor: Option<f32>,
        tie_complete: bool,
        filters: &RequestFilters,
    ) -> Result<ClusteredVectorResult, Status> {
        if k == 0 {
            return Ok(ClusteredVectorResult { hits: Vec::new() });
        }
        let backend = self.clustered_vectors.as_ref().ok_or_else(|| {
            Status::failed_precondition("clustered TurboVec backend is not configured")
        })?;
        let allowed = self.clustered_allowed_labels(filters).await?;
        let mut stream = backend
            .candidate_stream(request_id, vector.to_vec(), allowed, initial_floor)
            .await?;
        let mut heap: std::collections::BinaryHeap<StreamHeapEntry> =
            std::collections::BinaryHeap::with_capacity(k as usize + 1);
        let mut tie_candidates = Vec::new();
        let mut labels = std::collections::HashSet::new();
        let mut last_floor = initial_floor.unwrap_or(f32::NEG_INFINITY);
        let completion = loop {
            match stream.next_event().await? {
                ClusteredCandidateEvent::Batch(batch) => {
                    for candidate in batch {
                        if !candidate.score.is_finite() {
                            return Err(Status::internal(format!(
                                "clustered TurboVec label {} has non-finite score {}",
                                candidate.label, candidate.score
                            )));
                        }
                        if !labels.insert(candidate.label) {
                            return Err(Status::failed_precondition(format!(
                                "clustered TurboVec emitted duplicate stable label {}",
                                candidate.label
                            )));
                        }
                        let entry = StreamHeapEntry(MergedHit {
                            vector_id: candidate.label,
                            shard: 0,
                            score: candidate.score,
                        });
                        if tie_complete {
                            tie_candidates.push(entry.0);
                        }
                        if heap.len() < k as usize {
                            heap.push(entry);
                        } else if cmp_hits(&entry.0, &heap.peek().expect("heap is full").0)
                            == std::cmp::Ordering::Less
                        {
                            heap.pop();
                            heap.push(entry);
                        }
                    }
                    if heap.len() == k as usize {
                        let floor = heap.peek().expect("heap is full").0.score.next_down();
                        if floor > last_floor {
                            stream.raise_floor(floor)?;
                            last_floor = floor;
                            if tie_complete {
                                tie_candidates.retain(|candidate| candidate.score >= floor);
                            }
                        }
                    }
                }
                ClusteredCandidateEvent::Completion(completion) => break completion,
            }
        };
        if completion.emitted != labels.len() as u64 {
            return Err(Status::internal(format!(
                "clustered TurboVec completion counted {} candidates but the product received {}",
                completion.emitted,
                labels.len()
            )));
        }
        self.publish_progress(
            crate::pb::QueryStreamPhase::Dense,
            heap.iter()
                .map(|entry| (entry.0.vector_id, entry.0.score))
                .collect(),
            completion.scoring_fingerprint.clone(),
        );

        let mut hits = if tie_complete {
            let boundary = heap.peek().map_or(f32::NEG_INFINITY, |entry| entry.0.score);
            tie_candidates.retain(|candidate| candidate.score >= boundary);
            tie_candidates
        } else {
            heap.into_iter().map(|entry| entry.0).collect()
        };
        hits.sort_by(cmp_hits);
        if !tie_complete {
            hits.truncate(k as usize);
        }
        Ok(ClusteredVectorResult {
            hits: hits
                .into_iter()
                .map(|hit| (hit.vector_id, hit.score))
                .collect(),
        })
    }

    async fn clustered_parent_collapse(
        &self,
        request_id: &str,
        vector: &[f32],
        k: u32,
        filters: &RequestFilters,
    ) -> Result<CollapseStreamResult, Status> {
        struct ParentAgg {
            best_score: f32,
            best_id: u64,
            chunks: Vec<(u64, f32)>,
        }
        if k == 0 {
            return Ok(CollapseStreamResult {
                hits: Vec::new(),
                groups: Vec::new(),
                chunk_floor: f32::NEG_INFINITY,
                summaries: Vec::new(),
                floors_sent: 0,
            });
        }
        let backend = self
            .clustered_vectors
            .as_ref()
            .expect("clustered parent route is selected only with a backend");
        let ranges = self.product_label_ranges().await?;
        let allowed = self.clustered_allowed_labels(filters).await?;
        let mut stream = backend
            .candidate_stream(request_id, vector.to_vec(), allowed, None)
            .await?;
        let mut parents: HashMap<u64, ParentAgg> = HashMap::new();
        let mut labels = std::collections::HashSet::new();
        let mut kth = f32::NEG_INFINITY;
        let mut last_floor = f32::NEG_INFINITY;
        let mut floors_sent = 0;
        let completion = loop {
            match stream.next_event().await? {
                ClusteredCandidateEvent::Batch(batch) => {
                    let batch_labels: Vec<u64> = batch.iter().map(|item| item.label).collect();
                    for &label in &batch_labels {
                        if !labels.insert(label) {
                            return Err(Status::failed_precondition(format!(
                                "clustered TurboVec emitted duplicate stable label {label}"
                            )));
                        }
                    }
                    let parent_ids = self.product_parent_ids(&ranges, &batch_labels).await?;
                    let mut dirty = false;
                    for candidate in batch {
                        if !candidate.score.is_finite() {
                            return Err(Status::internal(format!(
                                "clustered TurboVec label {} has non-finite score {}",
                                candidate.label, candidate.score
                            )));
                        }
                        let parent = parent_ids[&candidate.label];
                        let agg = parents.entry(parent).or_insert_with(|| {
                            dirty = true;
                            ParentAgg {
                                best_score: f32::NEG_INFINITY,
                                best_id: u64::MAX,
                                chunks: Vec::new(),
                            }
                        });
                        agg.chunks.push((candidate.label, candidate.score));
                        if candidate.score > agg.best_score
                            || (candidate.score == agg.best_score && candidate.label < agg.best_id)
                        {
                            if candidate.score > agg.best_score && candidate.score > kth {
                                dirty = true;
                            }
                            agg.best_score = candidate.score;
                            agg.best_id = candidate.label;
                        }
                    }
                    if dirty && parents.len() >= k as usize {
                        let mut bests: Vec<f32> =
                            parents.values().map(|parent| parent.best_score).collect();
                        let new_kth = *bests
                            .select_nth_unstable_by(k as usize - 1, |a, b| b.total_cmp(a))
                            .1;
                        if new_kth > kth {
                            kth = new_kth;
                            let floor = kth.next_down();
                            if floor > last_floor {
                                stream.raise_floor(floor)?;
                                last_floor = floor;
                                floors_sent += 1;
                            }
                        }
                    }
                }
                ClusteredCandidateEvent::Completion(completion) => break completion,
            }
        };
        if completion.emitted != labels.len() as u64 {
            return Err(Status::internal(format!(
                "clustered TurboVec completion counted {} candidates but collapse received {}",
                completion.emitted,
                labels.len()
            )));
        }

        let mut ranked: Vec<(u64, ParentAgg)> = parents.into_iter().collect();
        ranked.sort_by(|a, b| {
            b.1.best_score
                .total_cmp(&a.1.best_score)
                .then_with(|| a.1.best_id.cmp(&b.1.best_id))
        });
        let chunk_floor = if ranked.len() >= k as usize {
            ranked[k as usize - 1].1.best_score.next_down()
        } else {
            f32::NEG_INFINITY
        };
        ranked.truncate(k as usize);
        let mut hits = Vec::with_capacity(ranked.len());
        let mut groups = Vec::with_capacity(ranked.len());
        for (parent, agg) in ranked {
            hits.push(ScoredHit {
                vector_id: agg.best_id,
                score: agg.best_score,
                parent_id: parent,
            });
            let mut chunks: Vec<(u64, f32)> = agg
                .chunks
                .into_iter()
                .filter(|&(_, score)| score >= chunk_floor)
                .collect();
            chunks.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            groups.push(ParentGroup {
                parent_id: parent,
                chunks: chunks
                    .into_iter()
                    .map(|(vector_id, score)| ScoredHit {
                        vector_id,
                        score,
                        parent_id: parent,
                    })
                    .collect(),
            });
        }
        Ok(CollapseStreamResult {
            hits,
            groups,
            chunk_floor,
            summaries: Vec::new(),
            floors_sent,
        })
    }

    /// Exact product-shard-local vector legs over one provider stream. The
    /// safe collection floor is the minimum filled local k-th score because a
    /// lower candidate cannot enter any product shard's local list.
    async fn clustered_local_vector_legs(
        &self,
        request_id: &str,
        vector: &[f32],
        k: u32,
        filters: &RequestFilters,
        ranges: &[ProductLabelRange],
    ) -> Result<Vec<Vec<(u64, f64)>>, Status> {
        let mut result = vec![Vec::new(); self.node_addrs.len()];
        if k == 0 {
            return Ok(result);
        }
        let backend = self
            .clustered_vectors
            .as_ref()
            .expect("clustered local legs require a configured backend");
        let allowed = self.clustered_allowed_labels(filters).await?;
        let mut stream = backend
            .candidate_stream(request_id, vector.to_vec(), allowed, None)
            .await?;
        let mut heaps: Vec<std::collections::BinaryHeap<StreamHeapEntry>> =
            (0..self.node_addrs.len())
                .map(|_| std::collections::BinaryHeap::with_capacity(k as usize + 1))
                .collect();
        let active: std::collections::HashSet<u32> =
            ranges.iter().map(|range| range.shard).collect();
        let mut labels = std::collections::HashSet::new();
        let mut last_floor = f32::NEG_INFINITY;
        let completion = loop {
            match stream.next_event().await? {
                ClusteredCandidateEvent::Batch(batch) => {
                    for candidate in batch {
                        if !candidate.score.is_finite() {
                            return Err(Status::internal(format!(
                                "clustered TurboVec label {} has non-finite score {}",
                                candidate.label, candidate.score
                            )));
                        }
                        if !labels.insert(candidate.label) {
                            return Err(Status::failed_precondition(format!(
                                "clustered TurboVec emitted duplicate stable label {}",
                                candidate.label
                            )));
                        }
                        let shard = Self::product_owner(ranges, candidate.label)? as usize;
                        let entry = StreamHeapEntry(MergedHit {
                            vector_id: candidate.label,
                            shard: shard as u32,
                            score: candidate.score,
                        });
                        if heaps[shard].len() < k as usize {
                            heaps[shard].push(entry);
                        } else if cmp_hits(
                            &entry.0,
                            &heaps[shard].peek().expect("local heap is full").0,
                        ) == std::cmp::Ordering::Less
                        {
                            heaps[shard].pop();
                            heaps[shard].push(entry);
                        }
                    }
                    if !active.is_empty()
                        && active
                            .iter()
                            .all(|&shard| heaps[shard as usize].len() == k as usize)
                    {
                        let floor = active
                            .iter()
                            .map(|&shard| {
                                heaps[shard as usize]
                                    .peek()
                                    .expect("active local heap is full")
                                    .0
                                    .score
                            })
                            .min_by(f32::total_cmp)
                            .expect("active set is non-empty")
                            .next_down();
                        if floor > last_floor {
                            stream.raise_floor(floor)?;
                            last_floor = floor;
                        }
                    }
                }
                ClusteredCandidateEvent::Completion(completion) => break completion,
            }
        };
        if completion.emitted != labels.len() as u64 {
            return Err(Status::internal(format!(
                "clustered TurboVec completion counted {} candidates but local legs received {}",
                completion.emitted,
                labels.len()
            )));
        }
        for (shard, heap) in heaps.into_iter().enumerate() {
            let mut hits: Vec<MergedHit> = heap.into_iter().map(|entry| entry.0).collect();
            hits.sort_by(cmp_hits);
            result[shard] = hits
                .into_iter()
                .map(|hit| (hit.vector_id, f64::from(hit.score)))
                .collect();
        }
        Ok(result)
    }

    /// Aggregation fan-out (docs/aggregations.md): every shard folds
    /// exact partials over its admitted documents; the coordinator
    /// merges them IN SHARD ORDER so the folds are deterministic
    /// bit-for-bit across runs. The typo rules hold as on every
    /// filtered route, and expression column leaves follow the
    /// projection contract: a leaf NO shard knows refuses by name.
    pub(crate) async fn fanout_aggregate(
        &self,
        filters: &RequestFilters,
        compiled: &CompiledAggregate,
        doc_ids: Option<&[u64]>,
    ) -> Result<crate::pb::AggregateResponse, Status> {
        let CompiledAggregate {
            aggregations,
            histograms,
            percentiles,
            percentile_specs,
            group_by,
            max_groups,
        } = compiled;
        let grouping = !group_by.is_empty();
        let group_cap = *max_groups as usize;
        let mut tasks = Vec::with_capacity(self.node_addrs.len());
        for node in &self.node_addrs {
            let request = crate::pb::AggregateShardRequest {
                filter: filters.tree.clone(),
                geo_filters: filters.geo.clone(),
                aggregations: aggregations.to_vec(),
                group_by: group_by.to_string(),
                max_groups: *max_groups,
                histograms: histograms.to_vec(),
                percentiles: percentiles.to_vec(),
                doc_ids: doc_ids.unwrap_or_default().to_vec(),
                restrict_doc_ids: doc_ids.is_some(),
            };
            let client = self.node_client(node);
            tasks.push(tokio::spawn(async move {
                client?
                    .aggregate_shard(request)
                    .await
                    .map(|r| r.into_inner())
            }));
        }
        // The same leaf enumeration the shards answer positionally:
        // aggregations first, then histograms.
        let mut leaves = Vec::new();
        for expr in aggregations
            .iter()
            .filter_map(|a| a.expr.as_ref())
            .chain(histograms.iter().filter_map(|h| h.expr.as_ref()))
            .chain(percentiles.iter().filter_map(|p| p.expr.as_ref()))
        {
            crate::values::column_leaves(expr, &mut leaves);
        }
        let mut known = FilterKnown::new(filters);
        let mut leaves_known = vec![false; leaves.len()];
        let mut group_column_known = !grouping;
        let mut matched = 0u64;
        let mut ungrouped = 0u64;
        let mut merged: Vec<AggMerge> = aggregations.iter().map(|_| AggMerge::new()).collect();
        // Groups merge by VALUE: the BTreeMap is both the cross-shard
        // join and the deterministic ascending order the response
        // promises.
        let mut groups: std::collections::BTreeMap<String, (u64, Vec<AggMerge>)> =
            std::collections::BTreeMap::new();
        let mut hist_buckets: Vec<std::collections::BTreeMap<i64, u64>> =
            histograms.iter().map(|_| Default::default()).collect();
        let mut hist_present = vec![0u64; histograms.len()];
        let mut hist_unbucketable = vec![0u64; histograms.len()];
        let mut pct_merged: Vec<PctMerge> = percentiles.iter().map(|_| PctMerge::new()).collect();
        for task in tasks {
            let response = task
                .await
                .map_err(|e| Status::internal(format!("aggregate task failed: {e}")))??;
            known.merge(&response.geo_columns_known, &response.filter_columns_known)?;
            if response.expr_leaves_known.len() != leaves_known.len() {
                return Err(Status::internal(format!(
                    "shard answered {} expression-leaf flags for {} leaves",
                    response.expr_leaves_known.len(),
                    leaves_known.len()
                )));
            }
            for (acc, k) in leaves_known.iter_mut().zip(&response.expr_leaves_known) {
                *acc |= *k;
            }
            if response.partials.len() != merged.len() {
                return Err(Status::internal(format!(
                    "shard answered {} aggregation partials for {} aggregations",
                    response.partials.len(),
                    merged.len()
                )));
            }
            group_column_known |= response.group_column_known;
            matched += response.matched;
            ungrouped += response.ungrouped;
            for (m, (p, agg)) in merged
                .iter_mut()
                .zip(response.partials.iter().zip(aggregations))
            {
                m.fold(p, &agg.name)?;
            }
            for shard_group in &response.groups {
                if shard_group.partials.len() != aggregations.len() {
                    return Err(Status::internal(format!(
                        "shard answered {} group partials for {} aggregations",
                        shard_group.partials.len(),
                        aggregations.len()
                    )));
                }
                let entry = groups
                    .entry(shard_group.value.clone())
                    .or_insert_with(|| (0, aggregations.iter().map(|_| AggMerge::new()).collect()));
                entry.0 += shard_group.matched;
                for (m, (p, agg)) in entry
                    .1
                    .iter_mut()
                    .zip(shard_group.partials.iter().zip(aggregations))
                {
                    m.fold(p, &agg.name)?;
                }
                if groups.len() > group_cap {
                    return Err(Status::failed_precondition(format!(
                        "group_by {group_by:?} exceeds {group_cap} distinct values \
                         across the fleet; tighten the filter or raise max_groups"
                    )));
                }
            }
            if response.histograms.len() != histograms.len() {
                return Err(Status::internal(format!(
                    "shard answered {} histograms for {} requested",
                    response.histograms.len(),
                    histograms.len()
                )));
            }
            if response.percentile_partials.len() != percentiles.len() {
                return Err(Status::internal(format!(
                    "shard answered {} percentile partials for {} requested",
                    response.percentile_partials.len(),
                    percentiles.len()
                )));
            }
            for (m, (p, spec)) in pct_merged
                .iter_mut()
                .zip(response.percentile_partials.iter().zip(percentiles))
            {
                m.fold(p, &spec.name)?;
            }
            for (i, (shard_hist, spec)) in response.histograms.iter().zip(histograms).enumerate() {
                if shard_hist.bucket_index.len() != shard_hist.bucket_count.len() {
                    return Err(Status::internal(
                        "shard answered a histogram with mismatched columns",
                    ));
                }
                hist_present[i] += shard_hist.present;
                hist_unbucketable[i] += shard_hist.unbucketable;
                for (&idx, &count) in shard_hist.bucket_index.iter().zip(&shard_hist.bucket_count) {
                    *hist_buckets[i].entry(idx).or_insert(0) += count;
                }
                if hist_buckets[i].len() > spec.max_buckets as usize {
                    return Err(Status::failed_precondition(format!(
                        "histogram {:?} exceeds {} buckets across the fleet; use a \
                         coarser interval or a tighter filter",
                        spec.name, spec.max_buckets
                    )));
                }
            }
        }
        known.refuse_unknown(filters)?;
        for (leaf, k) in leaves.iter().zip(&leaves_known) {
            if !k {
                return Err(Status::invalid_argument(format!(
                    "aggregation: no shard has column {}",
                    leaf.describe()
                )));
            }
        }
        if !group_column_known {
            return Err(Status::invalid_argument(format!(
                "group_by column {group_by:?} is not a facet column on any shard"
            )));
        }
        let mut results = Vec::with_capacity(aggregations.len());
        for (agg, m) in aggregations.iter().zip(&merged) {
            results.push(m.result(&agg.name, crate::node::agg_op_of(agg.op)?)?);
        }
        let mut group_results = Vec::with_capacity(groups.len());
        for (value, (group_matched, ms)) in &groups {
            let mut rs = Vec::with_capacity(aggregations.len());
            for (agg, m) in aggregations.iter().zip(ms) {
                rs.push(m.result(&agg.name, crate::node::agg_op_of(agg.op)?)?);
            }
            group_results.push(crate::pb::AggregateGroup {
                value: value.clone(),
                matched: *group_matched,
                results: rs,
            });
        }
        let hist_results = histograms
            .iter()
            .enumerate()
            .map(|(i, spec)| crate::pb::HistogramResult {
                name: spec.name.clone(),
                buckets: hist_buckets[i]
                    .iter()
                    .map(|(&idx, &count)| crate::pb::HistogramBucket {
                        lower: idx as f64 * spec.interval,
                        count,
                    })
                    .collect(),
                present: hist_present[i],
                unbucketable: hist_unbucketable[i],
            })
            .collect();
        let pct_results = self
            .solve_percentiles(filters, percentile_specs, percentiles, &pct_merged, doc_ids)
            .await?;
        Ok(crate::pb::AggregateResponse {
            results,
            matched,
            groups: group_results,
            ungrouped: if grouping { ungrouped } else { 0 },
            histograms: hist_results,
            percentiles: pct_results,
        })
    }

    /// The exact-percentile binary search (docs/aggregations.md): every
    /// requested (spec, percentile) target converges simultaneously
    /// over at most 64 count-below rounds in the order-bits domain, and
    /// each answer is the nearest-rank order statistic — a value some
    /// admitted document actually holds, never an interpolation.
    async fn solve_percentiles(
        &self,
        filters: &RequestFilters,
        specs: &[crate::pb::PercentileSpec],
        compiled: &[crate::pb::CompiledPercentile],
        merged: &[PctMerge],
        doc_ids: Option<&[u64]>,
    ) -> Result<Vec<crate::pb::PercentileResult>, Status> {
        struct Target {
            spec: usize,
            pct_index: usize,
            k: u64,
            lo: u64,
            hi: u64,
        }
        let mut targets = Vec::new();
        for (si, (spec, m)) in specs.iter().zip(merged).enumerate() {
            if m.present == 0 {
                continue;
            }
            for (pi, &p) in spec.percentiles.iter().enumerate() {
                // Nearest rank: the k-th smallest, k = ceil(p/100 * n)
                // clamped into [1, n].
                let k = ((p / 100.0 * m.present as f64).ceil() as u64).clamp(1, m.present);
                targets.push(Target {
                    spec: si,
                    pct_index: pi,
                    k,
                    lo: m.min_bits,
                    hi: m.max_bits,
                });
            }
        }
        // Invariant per target: the answer (the smallest bits value b
        // with count(<= b) >= k) lies in [lo, hi]. Bit space is 64
        // wide, so at most 64 rounds close every window.
        while targets.iter().any(|t| t.lo < t.hi) {
            let active: Vec<usize> = (0..targets.len())
                .filter(|&i| targets[i].lo < targets[i].hi)
                .collect();
            let probes: Vec<crate::pb::QuantileTarget> = active
                .iter()
                .map(|&i| {
                    let t = &targets[i];
                    crate::pb::QuantileTarget {
                        expr_index: t.spec as u32,
                        threshold_bits: t.lo + (t.hi - t.lo) / 2,
                    }
                })
                .collect();
            let counts = self
                .quantile_round(filters, compiled, &probes, doc_ids)
                .await?;
            for (&i, (probe, count)) in active.iter().zip(probes.iter().zip(counts)) {
                let t = &mut targets[i];
                if count >= t.k {
                    t.hi = probe.threshold_bits;
                } else {
                    t.lo = probe.threshold_bits + 1;
                }
            }
        }
        let mut answers: Vec<Vec<Option<(u64, u64)>>> = specs
            .iter()
            .map(|s| vec![None; s.percentiles.len()])
            .collect();
        for t in &targets {
            answers[t.spec][t.pct_index] = Some((t.k, t.lo));
        }
        let mut results = Vec::with_capacity(specs.len());
        for (si, (spec, m)) in specs.iter().zip(merged).enumerate() {
            use crate::pb::percentile_value::Value as W;
            let int_typed = m.vt == Some(crate::pb::AggregateValueType::Int);
            let values = spec
                .percentiles
                .iter()
                .enumerate()
                .map(|(pi, &p)| {
                    let (rank, value) = match answers[si][pi] {
                        None => (0, None),
                        Some((k, bits)) => (
                            k,
                            Some(if int_typed {
                                W::IntValue(crate::node::i64_from_order_bits(bits))
                            } else {
                                W::DoubleValue(crate::node::f64_from_order_bits(bits))
                            }),
                        ),
                    };
                    crate::pb::PercentileValue {
                        percentile: p,
                        rank,
                        value,
                    }
                })
                .collect();
            results.push(crate::pb::PercentileResult {
                name: spec.name.clone(),
                present: m.present,
                unrankable: m.unrankable,
                values,
            });
        }
        Ok(results)
    }

    /// One count-below round against every shard, counts summed per
    /// target.
    async fn quantile_round(
        &self,
        filters: &RequestFilters,
        exprs: &[crate::pb::CompiledPercentile],
        targets: &[crate::pb::QuantileTarget],
        doc_ids: Option<&[u64]>,
    ) -> Result<Vec<u64>, Status> {
        let mut tasks = Vec::with_capacity(self.node_addrs.len());
        for node in &self.node_addrs {
            let request = crate::pb::QuantileCountsRequest {
                filter: filters.tree.clone(),
                geo_filters: filters.geo.clone(),
                exprs: exprs.to_vec(),
                targets: targets.to_vec(),
                doc_ids: doc_ids.unwrap_or_default().to_vec(),
                restrict_doc_ids: doc_ids.is_some(),
            };
            let client = self.node_client(node);
            tasks.push(tokio::spawn(async move {
                client?
                    .quantile_counts(request)
                    .await
                    .map(|r| r.into_inner())
            }));
        }
        let mut totals = vec![0u64; targets.len()];
        for task in tasks {
            let response = task
                .await
                .map_err(|e| Status::internal(format!("quantile task failed: {e}")))??;
            if response.counts.len() != totals.len() {
                return Err(Status::internal(format!(
                    "shard answered {} quantile counts for {} targets",
                    response.counts.len(),
                    totals.len()
                )));
            }
            for (total, c) in totals.iter_mut().zip(&response.counts) {
                *total += *c;
            }
        }
        Ok(totals)
    }

    pub async fn fanout_vector_backend(
        &self,
        req: &BroadcastVectorBackendRequest,
    ) -> Vec<VectorBackendApplyResult> {
        let mut tasks = Vec::with_capacity(self.node_addrs.len());
        for node in &self.node_addrs {
            let node = node.clone();
            let request = ConfigureVectorBackendRequest {
                dim: req.dim,
                config: req.config.clone(),
            };
            let client = self.node_client(&node);
            tasks.push(tokio::spawn(async move {
                let result = match client {
                    Ok(mut client) => client.configure_vector_backend(request).await,
                    Err(e) => Err(e),
                };
                match result {
                    Ok(resp) => VectorBackendApplyResult {
                        node: node.clone(),
                        ok: true,
                        already_configured: resp.into_inner().already_configured,
                        error: String::new(),
                    },
                    Err(e) => VectorBackendApplyResult {
                        node: node.clone(),
                        ok: false,
                        already_configured: false,
                        error: e.message().to_string(),
                    },
                }
            }));
        }
        let mut results = Vec::with_capacity(tasks.len());
        for task in tasks {
            match task.await {
                Ok(result) => results.push(result),
                Err(e) => results.push(VectorBackendApplyResult {
                    node: String::new(),
                    ok: false,
                    already_configured: false,
                    error: format!("task failed: {e}"),
                }),
            }
        }
        results
    }

    pub async fn fanout_calibration(
        &self,
        req: &BroadcastCalibrationRequest,
    ) -> Vec<CalibrationApplyResult> {
        let mut tasks = Vec::with_capacity(self.node_addrs.len());
        for node in &self.node_addrs {
            let node = node.clone();
            let request = SetCalibrationRequest {
                dim: req.dim,
                bit_width: req.bit_width,
                shift: req.shift.clone(),
                scale: req.scale.clone(),
            };
            let client = self.node_client(&node);
            tasks.push(tokio::spawn(async move {
                let result = match client {
                    Ok(mut client) => client.set_calibration(request).await,
                    Err(e) => Err(e),
                };
                match result {
                    Ok(resp) => CalibrationApplyResult {
                        node: node.clone(),
                        ok: true,
                        already_seeded: resp.into_inner().already_seeded,
                        error: String::new(),
                    },
                    Err(e) => CalibrationApplyResult {
                        node: node.clone(),
                        ok: false,
                        already_seeded: false,
                        error: e.message().to_string(),
                    },
                }
            }));
        }
        let mut results = Vec::with_capacity(tasks.len());
        for task in tasks {
            match task.await {
                Ok(r) => results.push(r),
                Err(e) => results.push(CalibrationApplyResult {
                    node: String::new(),
                    ok: false,
                    already_seeded: false,
                    error: format!("task failed: {e}"),
                }),
            }
        }
        results
    }

    /// Run one A/B arm through the ordinary handler for its kind.
    ///
    /// This dispatches to `bm25_search`/`hybrid_search` rather than
    /// reimplementing them: the comparison is only trustworthy if each
    /// arm executes the path it would execute in production, and a
    /// parallel scoring path written for the A/B would drift from the
    /// served one until the diff was measuring the drift.
    async fn run_variant(
        &self,
        query: &search_variant::Query,
        k: u32,
    ) -> Result<Vec<RankedHit>, Status> {
        match query {
            search_variant::Query::Bm25(req) => {
                // `k` is the request's shared depth; an arm's own k is
                // ignored so the rankings stay comparable.
                let mut req = req.clone();
                req.k = k;
                let resp = SearchService::bm25_search(self, Request::new(req))
                    .await?
                    .into_inner();
                Ok(resp
                    .hits
                    .into_iter()
                    .map(|h| RankedHit {
                        doc_id: h.doc_id,
                        score: h.score,
                    })
                    .collect())
            }
            search_variant::Query::Hybrid(req) => {
                let mut req = req.clone();
                req.k = k;
                // The profile block is per-arm noise here and the caller
                // asked for a comparison, not a trace.
                req.debug = false;
                let resp = SearchService::hybrid_search(self, Request::new(req))
                    .await?
                    .into_inner();
                // CASCADE reports in `cascade_hits` and leaves `hits`
                // empty; the other modes do the reverse. Both are a
                // ranking, which is all a diff needs.
                if resp.hits.is_empty() {
                    Ok(resp
                        .cascade_hits
                        .into_iter()
                        .map(|h| RankedHit {
                            doc_id: h.doc_id,
                            score: h.bm25_score,
                        })
                        .collect())
                } else {
                    Ok(resp
                        .hits
                        .into_iter()
                        .map(|h| RankedHit {
                            doc_id: h.doc_id,
                            score: h.fused_score,
                        })
                        .collect())
                }
            }
        }
    }
}

/// Diff one arm against the reference arm.
///
/// Split out from the handler so the measure wiring is testable without a
/// cluster: everything below the fan-out is a pure function of two
/// rankings.
fn diff_against(
    reference: &VariantResult,
    variant: &VariantResult,
    k: usize,
    rbo_p: f64,
) -> RankingDiff {
    let ref_ids: Vec<u64> = reference.hits.iter().map(|h| h.doc_id).collect();
    let var_ids: Vec<u64> = variant.hits.iter().map(|h| h.doc_id).collect();
    let depth = k.min(ref_ids.len()).min(var_ids.len());
    let overlap_fraction = rankdiff::overlap_at_k(&ref_ids, &var_ids, k);
    // The reference's own scores are the yardstick for both sides, so
    // regret never compares a BM25 score with a fused one.
    let scored: Vec<(u64, f32)> = reference.hits.iter().map(|h| (h.doc_id, h.score)).collect();
    let regret = rankdiff::score_regret(&scored, &var_ids, k);
    RankingDiff {
        reference: reference.label.clone(),
        variant: variant.label.clone(),
        depth: depth as u32,
        // Recovered from the fraction rather than recounted, so the two
        // can never disagree.
        overlap: (overlap_fraction * depth as f64).round() as u32,
        overlap_fraction: overlap_fraction as f32,
        kendall_tau: rankdiff::kendall_tau(&ref_ids, &var_ids) as f32,
        rbo: rankdiff::rbo(&ref_ids, &var_ids, rbo_p) as f32,
        score_regret: regret.mean as f32,
        regret_counted: regret.counted as u32,
        regret_unscored: regret.unscored as u32,
        top1_flipped: rankdiff::top1_flipped(&ref_ids, &var_ids),
    }
}

/// Total-order f64 wrapper for heap keys.
#[derive(PartialEq)]
struct F64Ord(f64);
impl Eq for F64Ord {}
impl PartialOrd for F64Ord {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for F64Ord {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}

/// Magnitude-relative slack for the decomposed-floor bounds: 2^-40 of
/// the participating magnitudes, about four thousand times the worst
/// accumulated f64 rounding error of the three operations involved and
/// one part in 10^12 of the scores — provably conservative, immeasurably
/// loose.
const SLACK_REL: f64 = 1.0 / (1u64 << 40) as f64;

/// The decomposed vector floor for a fused lower bound `s_lb`:
/// mathematically (s_lb - w_b * b_1) / w_v, rounded DOWN past every
/// f64 rounding error and the f32 cast, so that v(d) < result implies
/// w_v * v(d) + w_b * b(d) < s_lb for every doc (b(d) <= b_1 by the
/// exactness of the BM25 leg). `wb_b1` arrives premultiplied.
fn decomposed_floor(s_lb: f64, wb_b1: f64, w_v: f64) -> f32 {
    let t = (s_lb - wb_b1) / w_v;
    let mag = t.abs() + (s_lb.abs() + wb_b1.abs()) / w_v;
    let safe = t - mag * SLACK_REL - f64::MIN_POSITIVE;
    (safe as f32).next_down()
}

/// One open `StreamSearch` fan-out: a merged inbound lane plus the
/// per-shard signal lanes (authoritative stream sender + optional UDP token).
/// Shared by every streaming consumer — plain top-k, document mode,
/// and the decomposed hybrid — which differ only in what they do with
/// the batches and which floor they derive.
struct StreamFanout {
    merged_rx: mpsc::Receiver<(usize, Result<Option<StreamSearchResponse>, Status>)>,
    floor_txs: Vec<Option<mpsc::Sender<StreamSearchRequest>>>,
    udp_lanes: Vec<Option<(u64, std::net::SocketAddr)>>,
    udp_socket: Option<Arc<std::net::UdpSocket>>,
}

impl StreamFanout {
    fn send_udp_cancel(&self) {
        let Some(socket) = self.udp_socket.as_deref() else {
            return;
        };
        for (shard, tx) in self.floor_txs.iter().enumerate() {
            if tx.is_none() {
                continue;
            }
            if let Some((token, target)) = self.udp_lanes[shard] {
                let frame = crate::stream_signal::encode_cancel(token);
                let _ = socket.send_to(&frame, target);
            }
        }
    }

    /// Abandon every unfinished shard. UDP goes first for low latency; the
    /// matching gRPC Stop is then awaited on every open request stream and is
    /// the authoritative signal. A stopped node can only return
    /// `completed = false`.
    async fn cancel(&mut self) {
        self.send_udp_cancel();
        let senders: Vec<mpsc::Sender<StreamSearchRequest>> =
            self.floor_txs.iter_mut().filter_map(Option::take).collect();
        for tx in senders {
            let _ = tx
                .send(StreamSearchRequest {
                    payload: Some(stream_search_request::Payload::Stop(StopStreamSearch {})),
                })
                .await;
        }
    }

    async fn cancel_with<T>(&mut self, status: Status) -> Result<T, Status> {
        self.cancel().await;
        Err(status)
    }

    fn mark_completed(&mut self, shard: usize) {
        self.floor_txs[shard] = None;
        self.udp_lanes[shard] = None;
    }

    /// The next inbound message: `Ok(Some((shard, msg)))` for a payload,
    /// `Ok(None)` for a clean post-summary stream close (callers just
    /// continue), and an error for a shard failure or a close without a
    /// summary (a protocol break — the summary is the exactness
    /// certificate, so a stream that vanishes without one aborts the
    /// query).
    async fn next_message(
        &mut self,
        summaries: &[Option<StreamSearchSummary>],
    ) -> Result<Option<(usize, StreamSearchResponse)>, Status> {
        let Some((shard, item)) = self.merged_rx.recv().await else {
            return Err(Status::internal("stream fan-out ended without all shards"));
        };
        match item {
            Ok(Some(msg)) => Ok(Some((shard, msg))),
            Ok(None) => {
                if summaries[shard].is_none() {
                    return Err(Status::internal(format!(
                        "shard {shard} closed its stream without a summary"
                    )));
                }
                Ok(None)
            }
            Err(e) => Err(Status::internal(format!("shard {shard} failed: {e}"))),
        }
    }
}

impl Drop for StreamFanout {
    fn drop(&mut self) {
        if self.floor_txs.iter().all(Option::is_none) {
            return;
        }
        self.send_udp_cancel();
        let senders: Vec<mpsc::Sender<StreamSearchRequest>> =
            self.floor_txs.iter_mut().filter_map(Option::take).collect();
        let send_stops = async move {
            for tx in senders {
                let _ = tx
                    .send(StreamSearchRequest {
                        payload: Some(stream_search_request::Payload::Stop(StopStreamSearch {})),
                    })
                    .await;
            }
        };
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(send_stops);
        }
    }
}

/// Everything one shard-stream attempt needs, cheap to clone per attempt
/// (a hedged retry is just a second attempt with the same context).
#[derive(Clone)]
struct ShardQueryCtx {
    request_id: Arc<str>,
    vector: Arc<Vec<f32>>,
    k: u32,
    tie_complete: bool,
    /// Collapse-by-parent mode (see StartShardSearch.collapse_parents).
    collapse: bool,
    /// The request's filters, compiled once and shipped verbatim to
    /// every shard (and to a hedge leg, which must run the identical
    /// query or its result would not be interchangeable).
    filters: Arc<RequestFilters>,
    /// Merges every shard's published floor into the running global max.
    tracker: Arc<Mutex<FloorTracker>>,
    /// Conflating broadcast cell for the global floor: pumps write raises
    /// here; per-stream forwarders relay whatever is LATEST when they
    /// wake, so a burst of raises becomes one message per stream.
    gfloor: Arc<watch::Sender<f32>>,
    /// Hedge legs launched (a shard's primary outran its hedge delay) and
    /// hedge legs that beat their primary. Both are pure accounting: a
    /// benchmark cannot otherwise tell "no hedge fired" from "the hedge
    /// fired and did not help".
    hedges: Arc<AtomicU64>,
    hedge_wins: Arc<AtomicU64>,
}

fn floor_message(floor: f32) -> SearchShardRequest {
    SearchShardRequest {
        payload: Some(search_shard_request::Payload::FloorUpdate(FloorUpdate {
            floor,
        })),
    }
}

/// One `SearchShard` stream attempt against one node: Start, pump floors
/// both ways, return the terminal Done.
async fn run_shard_stream(
    shard: u32,
    mut client: NodeServiceClient<Channel>,
    ctx: ShardQueryCtx,
) -> Result<SearchShardDone, Status> {
    let (req_tx, req_rx) = mpsc::channel::<SearchShardRequest>(8);
    req_tx
        .send(SearchShardRequest {
            payload: Some(search_shard_request::Payload::Start(StartShardSearch {
                request_id: ctx.request_id.to_string(),
                k: ctx.k,
                vector: ctx.vector.as_ref().clone(),
                tie_complete: ctx.tie_complete,
                collapse_parents: ctx.collapse,
                geo_filters: ctx.filters.geo.clone(),
                filter: ctx.filters.tree.clone(),
            })),
        })
        .await
        .map_err(|_| Status::internal("node request channel closed before Start"))?;
    let mut responses = client
        .search_shard(ReceiverStream::new(req_rx))
        .await?
        .into_inner();

    // A late starter (a hedged replica) joins with the floor already
    // raised — seed it immediately instead of waiting for the next raise.
    let current = *ctx.gfloor.borrow();
    if current != f32::NEG_INFINITY {
        let _ = req_tx.try_send(floor_message(current));
    }

    // Conflating forwarder: on every wake, relay only the LATEST floor.
    // try_send on a full channel just drops this raise — floors are
    // monotone, so the next raise supersedes it; a dropped floor delays
    // pruning but never affects results.
    let mut floor_rx = ctx.gfloor.subscribe();
    let forwarder = tokio::spawn(async move {
        while floor_rx.changed().await.is_ok() {
            let floor = *floor_rx.borrow_and_update();
            if let Err(mpsc::error::TrySendError::Closed(_)) = req_tx.try_send(floor_message(floor))
            {
                break;
            }
        }
    });

    let result = loop {
        match responses.message().await {
            Ok(Some(SearchShardResponse {
                payload: Some(search_shard_response::Payload::FloorUpdate(u)),
            })) => {
                let raised = ctx
                    .tracker
                    .lock()
                    .expect("floor tracker mutex poisoned")
                    .observe(u.floor);
                if let Some(floor) = raised {
                    // send_if_modified with a strict raise: two racing
                    // pumps can never lower the broadcast value.
                    ctx.gfloor.send_if_modified(|cur| {
                        if floor > *cur {
                            *cur = floor;
                            true
                        } else {
                            false
                        }
                    });
                }
            }
            Ok(Some(SearchShardResponse {
                payload: Some(search_shard_response::Payload::Done(done)),
            })) => break Ok(done),
            Ok(Some(_)) => {}
            Ok(None) => {
                break Err(Status::data_loss(format!(
                    "shard {shard}: stream closed before Done"
                )))
            }
            Err(e) => break Err(e),
        }
    };
    forwarder.abort();
    result
}

/// One shard's full query attempt: primary stream, hedged replica after
/// `hedge_delay` (first success wins), failover on primary error, all
/// bounded by `shard_deadline`.
async fn run_shard_with_hedge(
    shard: u32,
    primary: NodeServiceClient<Channel>,
    replica: Option<NodeServiceClient<Channel>>,
    ctx: ShardQueryCtx,
    limits: FanoutLimits,
) -> Result<SearchShardDone, Status> {
    let attempt = async {
        let primary_run = run_shard_stream(shard, primary, ctx.clone());
        match (replica, limits.hedge_delay) {
            (Some(rep), Some(delay)) => {
                tokio::pin!(primary_run);
                tokio::select! {
                    r = &mut primary_run => match r {
                        Ok(done) => Ok(done),
                        // Primary failed before the hedge window: go
                        // straight to the replica.
                        Err(pe) => run_shard_stream(shard, rep, ctx.clone())
                            .await
                            .map_err(|re| both_failed(shard, &pe, &re)),
                    },
                    _ = tokio::time::sleep(delay) => {
                        ctx.hedges.fetch_add(1, AtomicOrdering::Relaxed);
                        let replica_run = run_shard_stream(shard, rep, ctx.clone());
                        tokio::pin!(replica_run);
                        tokio::select! {
                            r = &mut primary_run => match r {
                                Ok(done) => Ok(done),
                                Err(pe) => replica_run
                                    .await
                                    .map_err(|re| both_failed(shard, &pe, &re)),
                            },
                            r = &mut replica_run => match r {
                                Ok(done) => {
                                    ctx.hedge_wins.fetch_add(1, AtomicOrdering::Relaxed);
                                    Ok(done)
                                }
                                Err(re) => primary_run
                                    .await
                                    .map_err(|pe| both_failed(shard, &pe, &re)),
                            },
                        }
                    }
                }
            }
            // Replica without a hedge delay: pure failover.
            (Some(rep), None) => match primary_run.await {
                Ok(done) => Ok(done),
                Err(pe) => run_shard_stream(shard, rep, ctx.clone())
                    .await
                    .map_err(|re| both_failed(shard, &pe, &re)),
            },
            (None, _) => primary_run.await,
        }
    };
    match limits.shard_deadline {
        Some(deadline) => tokio::time::timeout(deadline, attempt).await.map_err(|_| {
            Status::deadline_exceeded(format!(
                "shard {shard} exceeded its {}ms deadline",
                deadline.as_millis()
            ))
        })?,
        None => attempt.await,
    }
}

fn both_failed(shard: u32, primary: &Status, replica: &Status) -> Status {
    Status::unavailable(format!(
        "shard {shard}: primary failed ({primary}); replica failed ({replica})"
    ))
}

/// Resolved per-leg options for one hybrid query.
#[derive(Debug, Clone, Copy)]
pub struct HybridLegs {
    /// Depth each leg (and each shard's fused list) is fetched to.
    pub leg_k: u32,
    /// Vector-leg weight (RRF weight, or blend weight under
    /// SCORE_BLEND). RESOLVED: exactly 0 disables the leg.
    pub vector_weight: f32,
    /// BM25-leg weight (same dual role, same disable rule).
    pub bm25_weight: f32,
    /// RRF constant.
    pub rrf_k: f64,
    /// Fusion strategy (default GLOBAL_RANK).
    pub fusion_mode: FusionMode,
    /// SCORE_BLEND: per-leg score normalization.
    pub normalization: fusion::Normalization,
    /// SCORE_BLEND: combination of normalized leg scores.
    pub combination: fusion::Combination,
    /// Vector-score floor on the result set (see the proto); 0 = off.
    pub min_vector_score: f32,
}

/// The per-leg payload to put on the wire, blanked for a leg this query
/// has disabled.
///
/// A weight of exactly 0 disables a leg (see `HybridLegs`) and both
/// fusion functions honor it — but they do so AFTER the shard has
/// already scanned. A shard has no weight field to read; it gates the
/// vector scan on a non-empty `vector` and the BM25 scan on non-empty
/// `terms`, so sending a payload for a disabled leg buys a full scan
/// whose result is then discarded. Measured on the 86.6M-chunk fleet: a
/// `vector_weight: 0` query still paid ~320 ms of vector scan out of
/// ~340 ms total, so "bm25-only" cost 20x what the lexical leg costs
/// alone.
///
/// `dfs` travels with `terms` because `shard_legs` and `hybrid_shard`
/// both reject a request whose lengths disagree.
fn leg_payloads(
    vector: &[f32],
    terms: &[String],
    global: &CorpusStats,
    legs: HybridLegs,
) -> (Vec<f32>, Vec<String>, Vec<u32>) {
    let vector = if legs.vector_weight == 0.0 {
        Vec::new()
    } else {
        vector.to_vec()
    };
    let (terms, dfs) = if legs.bm25_weight == 0.0 {
        (Vec::new(), Vec::new())
    } else {
        (terms.to_vec(), global.dfs.clone())
    };
    (vector, terms, dfs)
}

/// Outcome of one coordinator fan-out: the merged global top-k plus the
/// per-shard scan statistics (in completion order), the latter powering the
/// floor-sharing benchmark.
#[derive(Debug)]
pub struct FanoutResult {
    /// Merged global top-k (score desc, shard/id tie-break).
    pub hits: Vec<ScoredHit>,
    /// Per-shard scan stats, one entry per shard, in completion order.
    pub shard_stats: Vec<Option<ShardScanStats>>,
    /// The raw per-shard lists (shard index, `(doc_id, score)`), in
    /// completion order. With `tie_complete` set, a shard's list may
    /// exceed k by its boundary tie group. The cascade pipeline routes
    /// candidates by these shard assignments.
    pub shard_hits: Vec<(u32, Vec<(u64, f32)>)>,
    /// Per-shard wall time in milliseconds as measured at the
    /// coordinator (shard index, ms), in completion order.
    pub shard_wall_ms: Vec<(u32, f32)>,
    /// Hedge legs launched during this fan-out, and how many of them
    /// returned before their primary.
    pub hedges_fired: u64,
    pub hedge_wins: u64,
}

/// Result of [`CoordinatorServiceImpl::fanout_stream_search`].
/// What a document-mode streaming fan-out returns (see
/// [`CoordinatorServiceImpl::fanout_stream_search_collapse`]).
pub struct CollapseStreamResult {
    /// The top-k parents' representatives: each parent's best chunk,
    /// score descending, ties by chunk id; `parent_id` set.
    pub hits: Vec<ScoredHit>,
    /// One group per entry of `hits`, same order: every chunk of that
    /// parent scoring at or above `chunk_floor`.
    pub groups: Vec<ParentGroup>,
    /// One ULP below the k-th best parent score, or -inf when fewer
    /// than k parents exist.
    pub chunk_floor: f32,
    /// Every shard's terminal summary (all certified `completed`).
    pub summaries: Vec<StreamSearchSummary>,
    /// Parent-floor raises pushed to the fleet.
    pub floors_sent: u64,
}

pub struct StreamFanoutResult {
    /// Merged global top-k, in the same total order as
    /// [`FanoutResult::hits`].
    pub hits: Vec<ScoredHit>,
    /// Per-shard terminal summaries, shard order; every one certified
    /// `completed`.
    pub summaries: Vec<StreamSearchSummary>,
    /// Floor raises this coordinator broadcast.
    pub floors_sent: u64,
}

/// Heap wrapper whose `Ord` IS the merge's total order ([`cmp_hits`]:
/// better entries compare Less), so a max-heap's peek is the worst
/// survivor — the running k-th best.
struct StreamHeapEntry(MergedHit);

impl PartialEq for StreamHeapEntry {
    fn eq(&self, other: &Self) -> bool {
        cmp_hits(&self.0, &other.0) == std::cmp::Ordering::Equal
    }
}
impl Eq for StreamHeapEntry {}
impl PartialOrd for StreamHeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for StreamHeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        cmp_hits(&self.0, &other.0)
    }
}

#[tonic::async_trait]
impl SearchService for CoordinatorServiceImpl {
    type QueryStreamStream = ReceiverStream<Result<crate::pb::QueryStreamResponse, Status>>;

    async fn search(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        if let Some(snapshot) = self.request_snapshot() {
            return Box::pin(SearchService::search(&snapshot, request)).await;
        }
        let req = request.into_inner();
        let k = self.resolve_k(req.k)?;
        if req.vector.is_empty() {
            return Err(Status::invalid_argument("empty query vector"));
        }
        if req.vector.iter().any(|x| !x.is_finite()) {
            return Err(Status::invalid_argument(
                "query vector has non-finite coordinates",
            ));
        }
        let request_id = if req.request_id.is_empty() {
            format!(
                "req-{}-{}",
                std::process::id(),
                REQUEST_COUNTER.fetch_add(1, AtomicOrdering::Relaxed)
            )
        } else {
            req.request_id.clone()
        };
        // CEL text compiles ONCE, here, into the predicate IR the
        // shards execute; no shard ever sees CEL text.
        let filters = RequestFilters::compile(&req.geo_filters, &req.filter)?;

        if self.clustered_vectors.is_some() {
            if req.collapse_parents {
                let collapsed = self
                    .clustered_parent_collapse(&request_id, &req.vector, k, &filters)
                    .await?;
                return Ok(Response::new(SearchResponse {
                    request_id,
                    hits: collapsed.hits,
                    groups: collapsed.groups,
                    chunk_floor: collapsed.chunk_floor,
                }));
            }
            let result = self
                .clustered_vector_candidates(&request_id, &req.vector, k, None, false, &filters)
                .await?;
            let hits = result
                .hits
                .into_iter()
                .map(|(vector_id, score)| ScoredHit {
                    vector_id,
                    score,
                    parent_id: 0,
                })
                .collect();
            return Ok(Response::new(SearchResponse {
                request_id,
                hits,
                groups: Vec::new(),
                chunk_floor: 0.0,
            }));
        }

        let result = if req.collapse_parents {
            // Document mode on a streaming coordinator: parents
            // aggregate here from tagged chunk emissions, and the
            // response carries the per-parent chunk groups. The bidi
            // path collapses shard-side and returns representatives
            // only.
            if self.stream_search {
                let doc = self
                    .fanout_stream_search_collapse(&request_id, &req.vector, k, &filters)
                    .await?;
                return Ok(Response::new(SearchResponse {
                    request_id,
                    hits: doc.hits,
                    groups: doc.groups,
                    chunk_floor: doc.chunk_floor,
                }));
            }
            self.fanout_search_collapse(&request_id, &req.vector, k, &filters)
                .await?
        } else if self.stream_search {
            let streamed = self
                .fanout_stream_search(&request_id, &req.vector, k, None, &filters)
                .await?;
            return Ok(Response::new(SearchResponse {
                request_id,
                hits: streamed.hits,
                groups: Vec::new(),
                chunk_floor: 0.0,
            }));
        } else {
            self.fanout_search(&request_id, &req.vector, k, false, &filters)
                .await?
        };
        Ok(Response::new(SearchResponse {
            request_id,
            hits: result.hits,
            groups: Vec::new(),
            chunk_floor: 0.0,
        }))
    }

    async fn bm25_search(
        &self,
        request: Request<Bm25SearchRequest>,
    ) -> Result<Response<Bm25SearchResponse>, Status> {
        if let Some(snapshot) = self.request_snapshot() {
            return Box::pin(SearchService::bm25_search(&snapshot, request)).await;
        }
        let req = request.into_inner();
        let k = self.resolve_k(req.k)?;
        if req.min_score.is_nan() || req.min_score == f32::NEG_INFINITY {
            return Err(Status::invalid_argument(
                "min_score must be finite (NaN and -inf are not valid floors)",
            ));
        }
        // CEL text compiles ONCE, here, into the predicate IR the
        // shards execute (docs/cel-filters.md): every shard sees the
        // same tree, and none ever sees CEL text.
        let filter = crate::cel::compile_filter(&req.filter)?;
        // Projection text compiles ONCE, here, into the ValueExpr IR
        // the shards resolve and evaluate (docs/cel-values.md).
        let projections = compile_projections(&req.projections)?;
        let mut phrase_routing = Vec::new();
        let (hits, facets, range_facets, stats, cardinality) =
            if req.fields.is_empty() && req.phrase.is_some() {
                // A phrase on the flat route is the body field's phrase on
                // the fused route (docs/phrase-proximity.md); the fused
                // route's uncertified combinations refuse by name here too.
                if !req.score_stages.is_empty() {
                    return Err(Status::invalid_argument(
                    "score stages are not yet certified with a phrase constraint; drop `phrase` \
                     or drop `score_stages`",
                ));
                }
                if !req.stats_fields.is_empty() || !req.cardinality_fields.is_empty() {
                    return Err(Status::invalid_argument(
                        "stats/cardinality are not yet certified with a phrase constraint; drop \
                     `phrase` or drop the aggregations",
                    ));
                }
                if !req.projections.is_empty() {
                    return Err(Status::invalid_argument(
                    "projections are not yet certified with a phrase constraint; drop `phrase` \
                     or drop the projections",
                ));
                }
                let body = vec![crate::pb::QueryField {
                    field: "body".to_string(),
                    analysis: req.analysis.clone(),
                    weight: 1.0,
                    k1: 0.0,
                    b: 0.0,
                    phrase: req.phrase.clone(),
                }];
                let ((hits, facets, ranges), routing) = self
                    .fanout_bm25_fused_routed(
                        &req.text,
                        k,
                        &body,
                        req.min_score,
                        &req.facet_fields,
                        &req.map_facet_fields,
                        &req.range_facet_fields,
                        &req.geo_filters,
                        filter.as_ref(),
                    )
                    .await?;
                phrase_routing = routing;
                (hits, facets, ranges, Vec::new(), Vec::new())
            } else if req.fields.is_empty() {
                self.fanout_bm25_aggregated(
                    &req.text,
                    k,
                    req.analysis.as_ref(),
                    req.min_score,
                    &req.facet_fields,
                    &req.map_facet_fields,
                    &req.range_facet_fields,
                    &req.score_stages,
                    &req.geo_filters,
                    filter.as_ref(),
                    &req.stats_fields,
                    &req.cardinality_fields,
                    &projections,
                )
                .await?
            } else {
                if !req.score_stages.is_empty() {
                    return Err(Status::invalid_argument(
                        "score stages are not yet supported on the fused multi-field route; \
                     drop `fields` to use the flat route, or drop `score_stages`",
                    ));
                }
                if !req.stats_fields.is_empty() || !req.cardinality_fields.is_empty() {
                    return Err(Status::invalid_argument(
                        "stats/cardinality are not yet supported on the fused multi-field \
                     route; drop `fields` to use the flat route, or drop the aggregations",
                    ));
                }
                if !req.projections.is_empty() {
                    return Err(Status::invalid_argument(
                        "projections are not yet supported on the fused multi-field route; \
                     drop `fields` to use the flat route, or drop the projections",
                    ));
                }
                // `analysis` is documented as ignored once `fields` is set,
                // because term identity is per field. Ignoring it QUIETLY is
                // the trap: the caller believes it asked for the ingest
                // analysis, every field falls back to the sidecar default,
                // and the query runs against terms that do not exist in the
                // index. That returns a confident ranking over whichever
                // tokens happened to survive, so it does not look like a
                // failure -- it looks like bad relevance.
                if req.analysis.is_some() {
                    return Err(Status::invalid_argument(
                    "Bm25SearchRequest.analysis is ignored when `fields` is set (term identity \
                     is per field). Move the spec onto each QueryField.analysis, or drop \
                     `fields` to use the single-field route.",
                ));
                }
                if req.phrase.is_some() {
                    return Err(Status::invalid_argument(
                    "Bm25SearchRequest.phrase is the flat route's constraint; with `fields` set, \
                     put the PhraseMatch on the QueryField it constrains",
                ));
                }
                let ((hits, facets, ranges), routing) = self
                    .fanout_bm25_fused_routed(
                        &req.text,
                        k,
                        &req.fields,
                        req.min_score,
                        &req.facet_fields,
                        &req.map_facet_fields,
                        &req.range_facet_fields,
                        &req.geo_filters,
                        filter.as_ref(),
                    )
                    .await?;
                phrase_routing = routing;
                (hits, facets, ranges, Vec::new(), Vec::new())
            };
        // The merged k-th best: one f32 ULP below the last hit's score
        // when k hits were returned (see `bm25::floor_seed` — a later
        // seed can never exceed the true k-th best), 0 otherwise.
        let kth_best = if hits.len() == k as usize {
            hits.last()
                .map(|h| crate::bm25::floor_seed(h.score))
                .unwrap_or(0.0)
        } else {
            0.0
        };
        Ok(Response::new(Bm25SearchResponse {
            hits,
            kth_best,
            facets,
            range_facets,
            stats,
            cardinality,
            phrase_routing,
        }))
    }

    async fn phrase_search(
        &self,
        request: Request<crate::pb::PhraseSearchRequest>,
    ) -> Result<Response<Bm25SearchResponse>, Status> {
        if let Some(snapshot) = self.request_snapshot() {
            return Box::pin(SearchService::phrase_search(&snapshot, request)).await;
        }
        let request = request.into_inner();
        let base = request
            .base
            .ok_or_else(|| Status::invalid_argument("PhraseSearchRequest.base is required"))?;
        let k = self.resolve_k(base.k)?;
        if base.min_score.is_nan() || base.min_score == f32::NEG_INFINITY {
            return Err(Status::invalid_argument(
                "min_score must be finite (NaN and -inf are not valid floors)",
            ));
        }
        if !base.score_stages.is_empty()
            || !base.stats_fields.is_empty()
            || !base.cardinality_fields.is_empty()
            || !base.projections.is_empty()
        {
            return Err(Status::invalid_argument(
                "phrase max-group scoring is not yet certified with score stages, stats, cardinality, or projections; use filters and facets, or issue an ordinary Bm25Search",
            ));
        }
        let options = request.options.unwrap_or_default();
        let weight_per_token = if options.weight_per_token == 0.0 {
            1.0
        } else {
            options.weight_per_token
        };
        let max_weight = if options.max_weight == 0.0 {
            3.0
        } else {
            options.max_weight
        };
        let filter = crate::cel::compile_filter(&base.filter)?;
        let (hits, facets, range_facets) = self
            .fanout_phrase(&base, k, weight_per_token, max_weight, filter.as_ref())
            .await?;
        let kth_best = if hits.len() == k as usize {
            hits.last()
                .map(|hit| crate::bm25::floor_seed(hit.score))
                .unwrap_or(0.0)
        } else {
            0.0
        };
        Ok(Response::new(Bm25SearchResponse {
            hits,
            kth_best,
            facets,
            range_facets,
            stats: Vec::new(),
            cardinality: Vec::new(),
            phrase_routing: Vec::new(),
        }))
    }

    async fn hybrid_search(
        &self,
        request: Request<HybridSearchRequest>,
    ) -> Result<Response<HybridSearchResponse>, Status> {
        if let Some(snapshot) = self.request_snapshot() {
            return Box::pin(SearchService::hybrid_search(&snapshot, request)).await;
        }
        let req = request.into_inner();
        let k = self.resolve_k(req.k)?;
        if req.text.is_empty() {
            return Err(Status::invalid_argument(
                "hybrid search requires query text",
            ));
        }
        if req.vector.is_empty() {
            return Err(Status::invalid_argument(
                "hybrid search requires a query vector",
            ));
        }
        if req.vector.iter().any(|x| !x.is_finite()) {
            return Err(Status::invalid_argument(
                "query vector has non-finite coordinates",
            ));
        }
        let request_id = if req.request_id.is_empty() {
            format!(
                "req-{}-{}",
                std::process::id(),
                REQUEST_COUNTER.fetch_add(1, AtomicOrdering::Relaxed)
            )
        } else {
            req.request_id.clone()
        };
        // One compilation, both legs. The hybrid route used to refuse
        // filters outright because the vector leg had no filter
        // machinery and filtering only the lexical half would have
        // misdescribed the result set (docs/vector-filters.md).
        let filters = RequestFilters::compile(&req.geo_filters, &req.filter)?;
        // leg_k: default max(k, rrf_k) so the RRF constant never exceeds a
        // leg's depth; explicit values below k are clamped to k.
        let options = req.legs.unwrap_or_default();
        let rrf_k = if options.rrf_k == 0.0 {
            fusion::DEFAULT_RRF_K
        } else {
            f64::from(options.rrf_k)
        };
        // Weights: absent = 1.0; an explicit 0 disables the leg (the
        // proto documents which modes support that).
        let vector_weight = options.vector_weight.unwrap_or(1.0);
        let bm25_weight = options.bm25_weight.unwrap_or(1.0);
        if vector_weight == 0.0 && bm25_weight == 0.0 {
            return Err(Status::invalid_argument(
                "both legs disabled: at least one of vector_weight/bm25_weight must be nonzero",
            ));
        }
        if options.fusion_mode() == FusionMode::TwoLevel
            && (vector_weight == 0.0 || bm25_weight == 0.0)
        {
            return Err(Status::invalid_argument(
                "TWO_LEVEL cannot disable a leg (its node wire format cannot distinguish \
                 0 from unset); use GLOBAL_RANK or SCORE_BLEND",
            ));
        }
        // The decomposed floor algebra divides by vector_weight and
        // scales bounds by bm25_weight; both must be strictly positive
        // (a single-leg query belongs to GLOBAL_RANK or SCORE_BLEND).
        if options.fusion_mode() == FusionMode::Decomposed
            && !(vector_weight > 0.0
                && vector_weight.is_finite()
                && bm25_weight > 0.0
                && bm25_weight.is_finite())
        {
            return Err(Status::invalid_argument(
                "DECOMPOSED requires finite leg weights > 0 for both legs",
            ));
        }
        if options.min_vector_score.is_nan() {
            return Err(Status::invalid_argument("min_vector_score must not be NaN"));
        }
        // A floor on the vector leg's score cannot be met by a query that
        // does not run the vector leg. Fusion would drop every hit and
        // return an empty result set, which reads as "nothing matched"
        // rather than "you asked for two contradictory things".
        if options.min_vector_score > 0.0 && vector_weight == 0.0 {
            return Err(Status::invalid_argument(
                "min_vector_score is set but the vector leg is disabled (vector_weight 0); \
                 no hit can carry a qualifying vector score",
            ));
        }
        let legs = HybridLegs {
            leg_k: if options.leg_k == 0 {
                k.max(rrf_k as u32)
            } else {
                options.leg_k.max(k)
            },
            vector_weight,
            bm25_weight,
            rrf_k,
            fusion_mode: options.fusion_mode(),
            normalization: match options.normalization() {
                crate::pb::ScoreNormalization::ZScore => fusion::Normalization::ZScore,
                crate::pb::ScoreNormalization::None => fusion::Normalization::None,
                _ => fusion::Normalization::MinMax,
            },
            combination: match options.combination() {
                crate::pb::ScoreCombination::Geometric => fusion::Combination::Geometric,
                crate::pb::ScoreCombination::Harmonic => fusion::Combination::Harmonic,
                _ => fusion::Combination::Arithmetic,
            },
            min_vector_score: options.min_vector_score,
        };
        let (mut hits, mut cascade_hits, mut debug) = match legs.fusion_mode {
            FusionMode::Cascade | FusionMode::Unspecified => {
                let (cascade_hits, debug) = self
                    .fanout_cascade(
                        &request_id,
                        &req.text,
                        &req.vector,
                        k,
                        req.analysis.as_ref(),
                        legs.min_vector_score,
                        req.debug,
                        &filters,
                    )
                    .await?;
                (Vec::new(), cascade_hits, debug)
            }
            _ => {
                let (hits, debug) = self
                    .fanout_hybrid(
                        &request_id,
                        &req.text,
                        &req.vector,
                        k,
                        req.analysis.as_ref(),
                        legs,
                        req.debug,
                        &filters,
                    )
                    .await?;
                (hits, Vec::new(), debug)
            }
        };
        if let Some(boost) = &req.boost {
            self.apply_boost(
                boost,
                req.analysis.as_ref(),
                &mut hits,
                &mut cascade_hits,
                &mut debug,
            )
            .await?;
        }
        Ok(Response::new(HybridSearchResponse {
            request_id,
            hits,
            cascade_hits,
            debug,
        }))
    }

    /// The public query surface: an adapter over the routes above
    /// (`docs/query-api.md`, `src/query.rs`). Delegation, never a fork.
    async fn query(
        &self,
        request: Request<crate::pb::QueryRequest>,
    ) -> Result<Response<crate::pb::QueryResponse>, Status> {
        if let Some(snapshot) = self.request_snapshot() {
            return Box::pin(SearchService::query(&snapshot, request)).await;
        }
        let request = request.into_inner();
        self.require_topology_generation(request.required_topology_generation)?;
        let mut response = crate::query::execute(self, request).await?;
        response.served_topology_generation = self.topology_generation;
        Ok(Response::new(response))
    }

    async fn query_stream(
        &self,
        request: Request<crate::pb::QueryStreamRequest>,
    ) -> Result<Response<Self::QueryStreamStream>, Status> {
        if let Some(snapshot) = self.request_snapshot() {
            return Box::pin(SearchService::query_stream(&snapshot, request)).await;
        }
        let request = request.into_inner();
        if let Some(query) = request.query.as_ref() {
            self.require_topology_generation(query.required_topology_generation)?;
        }
        let (tx, rx) = mpsc::channel::<Result<crate::pb::QueryStreamResponse, Status>>(8);
        let service = self.clone();
        tokio::spawn(async move {
            let mut revision = 1u64;
            let accepted = query_stream_revision(
                revision,
                crate::pb::QueryStreamPhase::Accepted,
                Vec::new(),
                String::new(),
            );
            if tx
                .send(Ok(crate::pb::QueryStreamResponse {
                    payload: Some(crate::pb::query_stream_response::Payload::Revision(
                        accepted,
                    )),
                }))
                .await
                .is_err()
            {
                return;
            }

            let Some(query) = request.query else {
                let status = Status::invalid_argument("QueryStream requires query");
                let _ = tx
                    .send(Ok(crate::pb::QueryStreamResponse {
                        payload: Some(crate::pb::query_stream_response::Payload::Completion(
                            crate::pb::QueryStreamCompletion {
                                completed: false,
                                response: None,
                                final_revision: revision,
                                scoring_fingerprints: Vec::new(),
                                error_code: status.code() as u32,
                                error_message: status.message().to_string(),
                            },
                        )),
                    }))
                    .await;
                return;
            };
            let request_fingerprint =
                crate::sha256::hex_digest(&prost::Message::encode_to_vec(&query));
            let (progress_tx, mut progress_rx) = watch::channel(None);
            // Public streaming always takes the candidate protocols. The
            // ordinary Query adapter still owns validation, boosts, scorer,
            // projection, paging, and final response construction.
            let runner = service
                .with_stream_search(true)
                .with_bm25_stream(true)
                .with_query_progress(progress_tx);
            let mut execution = Box::pin(crate::query::execute(&runner, query));
            let timeout =
                (request.timeout_ms > 0).then(|| Duration::from_millis(request.timeout_ms));
            let deadline = async move {
                match timeout {
                    Some(duration) => tokio::time::sleep(duration).await,
                    None => std::future::pending::<()>().await,
                }
            };
            tokio::pin!(deadline);
            let mut scoring_fingerprints: Vec<String> = Vec::new();
            let mut last_content_fingerprint: Option<String> = None;
            loop {
                tokio::select! {
                    _ = tx.closed() => {
                        // Dropping `execution` cancels every in-flight fan-out
                        // future. Candidate stream destructors send Stop where
                        // available; no completion is manufactured for a client
                        // that cancelled its own response stream.
                        return;
                    }
                    changed = progress_rx.changed() => {
                        if changed.is_err() {
                            continue;
                        }
                        let Some(progress) = progress_rx.borrow_and_update().clone() else {
                            continue;
                        };
                        if !progress.scoring_fingerprint.is_empty()
                            && !scoring_fingerprints.contains(&progress.scoring_fingerprint)
                        {
                            scoring_fingerprints.push(progress.scoring_fingerprint.clone());
                        }
                        let next_revision = revision + 1;
                        let snapshot = query_stream_revision(
                            next_revision,
                            progress.phase,
                            progress.hits,
                            progress.scoring_fingerprint,
                        );
                        if last_content_fingerprint.as_ref()
                            == Some(&snapshot.content_fingerprint)
                        {
                            continue;
                        }
                        let content_fingerprint = snapshot.content_fingerprint.clone();
                        let event = crate::pb::QueryStreamResponse {
                            payload: Some(crate::pb::query_stream_response::Payload::Revision(
                                snapshot,
                            )),
                        };
                        match tx.try_send(Ok(event)) {
                            Ok(()) => {
                                revision = next_revision;
                                last_content_fingerprint = Some(content_fingerprint);
                            }
                            Err(mpsc::error::TrySendError::Full(_)) => {
                                // Revisions are full replacements. The watch
                                // cell and next successful send supersede this
                                // one without losing correctness.
                            }
                            Err(mpsc::error::TrySendError::Closed(_)) => return,
                        }
                    }
                    result = &mut execution => {
                        // The execution and the last collector update can
                        // become ready in the same scheduler turn. Drain the
                        // watch cell here so every scored stream exposes at
                        // least its last provisional collector order before
                        // the authoritative FINAL revision.
                        let pending_progress = progress_rx.borrow().clone();
                        if let Some(progress) = pending_progress {
                            if !progress.scoring_fingerprint.is_empty()
                                && !scoring_fingerprints.contains(&progress.scoring_fingerprint)
                            {
                                scoring_fingerprints.push(progress.scoring_fingerprint.clone());
                            }
                            let next_revision = revision + 1;
                            let snapshot = query_stream_revision(
                                next_revision,
                                progress.phase,
                                progress.hits,
                                progress.scoring_fingerprint,
                            );
                            if last_content_fingerprint.as_ref()
                                != Some(&snapshot.content_fingerprint)
                            {
                                if tx
                                    .send(Ok(crate::pb::QueryStreamResponse {
                                        payload: Some(
                                            crate::pb::query_stream_response::Payload::Revision(
                                                snapshot,
                                            ),
                                        ),
                                    }))
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                                revision = next_revision;
                            }
                        }
                        match result {
                            Ok(mut response) => {
                                response.served_topology_generation =
                                    runner.topology_generation;
                                scoring_fingerprints.sort();
                                scoring_fingerprints.dedup();
                                let final_scoring = combined_scoring_fingerprint(
                                    &scoring_fingerprints,
                                    &request_fingerprint,
                                );
                                revision += 1;
                                let final_hits = response
                                    .hits
                                    .iter()
                                    .map(|hit| (hit.doc_id, hit.score))
                                    .collect();
                                if tx
                                    .send(Ok(crate::pb::QueryStreamResponse {
                                        payload: Some(
                                            crate::pb::query_stream_response::Payload::Revision(
                                                query_stream_revision(
                                                    revision,
                                                    crate::pb::QueryStreamPhase::Final,
                                                    final_hits,
                                                    final_scoring,
                                                ),
                                            ),
                                        ),
                                    }))
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                                let _ = tx
                                    .send(Ok(crate::pb::QueryStreamResponse {
                                        payload: Some(
                                            crate::pb::query_stream_response::Payload::Completion(
                                                crate::pb::QueryStreamCompletion {
                                                    completed: true,
                                                    response: Some(response),
                                                    final_revision: revision,
                                                    scoring_fingerprints,
                                                    error_code: 0,
                                                    error_message: String::new(),
                                                },
                                            ),
                                        ),
                                    }))
                                    .await;
                            }
                            Err(status) => {
                                scoring_fingerprints.sort();
                                scoring_fingerprints.dedup();
                                let _ = tx
                                    .send(Ok(crate::pb::QueryStreamResponse {
                                        payload: Some(
                                            crate::pb::query_stream_response::Payload::Completion(
                                                crate::pb::QueryStreamCompletion {
                                                    completed: false,
                                                    response: None,
                                                    final_revision: revision,
                                                    scoring_fingerprints,
                                                    error_code: status.code() as u32,
                                                    error_message: status.message().to_string(),
                                                },
                                            ),
                                        ),
                                    }))
                                    .await;
                            }
                        }
                        return;
                    }
                    _ = &mut deadline => {
                        let status = Status::deadline_exceeded(format!(
                            "QueryStream exceeded its {}ms deadline",
                            request.timeout_ms
                        ));
                        scoring_fingerprints.sort();
                        scoring_fingerprints.dedup();
                        let _ = tx
                            .send(Ok(crate::pb::QueryStreamResponse {
                                payload: Some(
                                    crate::pb::query_stream_response::Payload::Completion(
                                        crate::pb::QueryStreamCompletion {
                                            completed: false,
                                            response: None,
                                            final_revision: revision,
                                            scoring_fingerprints,
                                            error_code: status.code() as u32,
                                            error_message: status.message().to_string(),
                                        },
                                    ),
                                ),
                            }))
                            .await;
                        return;
                    }
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn plan_index(
        &self,
        request: Request<crate::pb::PlanIndexRequest>,
    ) -> Result<Response<crate::pb::PlanIndexResponse>, Status> {
        if let Some(snapshot) = self.request_snapshot() {
            return Box::pin(SearchService::plan_index(&snapshot, request)).await;
        }
        // Derivation is local and deterministic (docs/descriptor-mappings.md):
        // nothing fans out, nothing binds, and the same request returns the
        // same fingerprint on every coordinator.
        let req = request.into_inner();
        let plan = crate::mapping::derive_plan(&req.descriptor_set, &req.message_type)?;
        Ok(Response::new(crate::pb::PlanIndexResponse {
            plan: Some(plan),
        }))
    }

    async fn routed_ingest_mapped(
        &self,
        request: Request<Streaming<RoutedIngestMappedRequest>>,
    ) -> Result<Response<RoutedIngestMappedResponse>, Status> {
        // Gate before snapshotting: a write that arrived during a cutover
        // must resume onto the new map, never retain the old snapshot while
        // waiting behind the final-tail barrier.
        let _write_guard = if self.live_topology.is_some() {
            Some(self.write_gate.clone().read_owned().await)
        } else {
            None
        };
        if let Some(snapshot) = self.request_snapshot() {
            return Box::pin(SearchService::routed_ingest_mapped(&snapshot, request)).await;
        }
        let mut inbound = request.into_inner();
        let bind = match inbound.message().await? {
            Some(RoutedIngestMappedRequest {
                payload: Some(crate::pb::routed_ingest_mapped_request::Payload::Bind(bind)),
            }) => bind,
            _ => {
                return Err(Status::invalid_argument(
                    "first RoutedIngestMappedRequest must be a RoutedMappedBind",
                ))
            }
        };
        if bind.required_topology_generation == 0 {
            return Err(Status::invalid_argument(
                "routed writes require required_topology_generation; zero is not accepted",
            ));
        }
        self.require_topology_generation(bind.required_topology_generation)?;
        let mapped_bind = bind
            .bind
            .ok_or_else(|| Status::invalid_argument("routed mapped bind is missing bind"))?;
        let mut batches: Vec<Vec<crate::pb::IngestMappedRequest>> =
            vec![Vec::new(); self.node_addrs.len()];
        while let Some(message) = inbound.message().await? {
            let document = match message.payload {
                Some(crate::pb::routed_ingest_mapped_request::Payload::Document(document)) => {
                    document
                }
                Some(crate::pb::routed_ingest_mapped_request::Payload::Bind(_)) => {
                    return Err(Status::invalid_argument(
                        "routed mapped bind repeats mid-stream",
                    ))
                }
                None => {
                    return Err(Status::invalid_argument(
                        "empty RoutedIngestMappedRequest payload",
                    ))
                }
            };
            let (_, shard) = self
                .route_stable_key(&document.stable_key)
                .map_err(Status::invalid_argument)?;
            if batches[shard].is_empty() {
                batches[shard].push(crate::pb::IngestMappedRequest {
                    payload: Some(crate::pb::ingest_mapped_request::Payload::Bind(
                        mapped_bind.clone(),
                    )),
                });
            }
            batches[shard].push(crate::pb::IngestMappedRequest {
                payload: Some(crate::pb::ingest_mapped_request::Payload::RoutedDocument(
                    document,
                )),
            });
        }

        let mut tasks = tokio::task::JoinSet::new();
        for (shard, batch) in batches.into_iter().enumerate() {
            if batch.is_empty() {
                continue;
            }
            let addr = self.node_addrs[shard].clone();
            let mut client = self.node_client(&addr)?;
            tasks.spawn(async move {
                let response = client
                    .ingest_mapped(tokio_stream::iter(batch))
                    .await?
                    .into_inner();
                Ok::<_, Status>((shard, addr, response))
            });
        }
        let mut shards = Vec::new();
        let mut added = 0u64;
        let mut parents = 0u64;
        while let Some(result) = tasks.join_next().await {
            let (shard, addr, response) = result.map_err(|error| {
                Status::internal(format!("routed ingest task failed: {error}"))
            })??;
            added = added.saturating_add(response.added);
            parents = parents.saturating_add(response.parents);
            shards.push(RoutedShardIngest {
                shard: shard as u32,
                addr,
                added: response.added,
                parents: response.parents,
                first_id: response.first_id,
            });
        }
        shards.sort_by_key(|result| result.shard);
        Ok(Response::new(RoutedIngestMappedResponse {
            added,
            parents,
            served_topology_generation: self.topology_generation,
            shards,
        }))
    }

    async fn freeze_topology_writes(
        &self,
        request: Request<FreezeTopologyWritesRequest>,
    ) -> Result<Response<FreezeTopologyWritesResponse>, Status> {
        if self.live_topology.is_none() {
            return Err(Status::failed_precondition(
                "topology cutover requires a generation-stamped hot shard map",
            ));
        }
        let requested = request.into_inner().required_topology_generation;
        if requested == 0 {
            return Err(Status::invalid_argument(
                "freeze requires a nonzero topology generation",
            ));
        }
        if requested != self.current_topology_generation() {
            return Err(Status::failed_precondition(format!(
                "freeze requires topology generation {requested}, live {}",
                self.current_topology_generation()
            )));
        }
        if self
            .cutover_pending
            .compare_exchange(false, true, AtomicOrdering::AcqRel, AtomicOrdering::Acquire)
            .is_err()
        {
            return Err(Status::already_exists(
                "another topology cutover is already pending",
            ));
        }
        let guard = self.write_gate.clone().write_owned().await;
        if requested != self.current_topology_generation() {
            self.cutover_pending.store(false, AtomicOrdering::Release);
            drop(guard);
            return Err(Status::failed_precondition(
                "topology changed while the write barrier was being acquired",
            ));
        }
        let token = floor_token();
        *self
            .cutover_guard
            .lock()
            .expect("cutover guard mutex poisoned") = Some((token, guard));
        Ok(Response::new(FreezeTopologyWritesResponse {
            topology_generation: requested,
            cutover_token: token,
        }))
    }

    async fn publish_topology(
        &self,
        request: Request<PublishTopologyRequest>,
    ) -> Result<Response<PublishTopologyResponse>, Status> {
        let req = request.into_inner();
        let routes = req
            .shards
            .into_iter()
            .map(|shard| TopologyRoute {
                addr: crate::config::normalize_addr(shard.addr),
                replica: (!shard.replica.is_empty())
                    .then(|| crate::config::normalize_addr(shard.replica)),
                hash_range: Some((shard.hash_lo, shard.hash_hi)),
            })
            .collect();
        let mut held = self
            .cutover_guard
            .lock()
            .expect("cutover guard mutex poisoned");
        let Some((token, _)) = held.as_ref() else {
            return Err(Status::failed_precondition(
                "no topology cutover write freeze is active",
            ));
        };
        if *token != req.cutover_token {
            return Err(Status::permission_denied("cutover token does not match"));
        }
        self.publish_topology_inner(req.generation, routes)
            .map_err(Status::invalid_argument)?;
        held.take();
        self.cutover_pending.store(false, AtomicOrdering::Release);
        Ok(Response::new(PublishTopologyResponse {
            topology_generation: req.generation,
        }))
    }

    async fn abort_topology_cutover(
        &self,
        request: Request<AbortTopologyCutoverRequest>,
    ) -> Result<Response<AbortTopologyCutoverResponse>, Status> {
        let token = request.into_inner().cutover_token;
        let mut held = self
            .cutover_guard
            .lock()
            .expect("cutover guard mutex poisoned");
        let Some((expected, _)) = held.as_ref() else {
            return Err(Status::failed_precondition(
                "no topology cutover write freeze is active",
            ));
        };
        if *expected != token {
            return Err(Status::permission_denied("cutover token does not match"));
        }
        held.take();
        self.cutover_pending.store(false, AtomicOrdering::Release);
        Ok(Response::new(AbortTopologyCutoverResponse {
            topology_generation: self.current_topology_generation(),
        }))
    }

    /// Exact aggregates over the filtered corpus
    /// (docs/aggregations.md). CEL compiles ONCE, here: the filter
    /// into the predicate IR, each expression into the ValueExpr IR;
    /// no shard ever sees text.
    async fn aggregate(
        &self,
        request: Request<crate::pb::AggregateRequest>,
    ) -> Result<Response<crate::pb::AggregateResponse>, Status> {
        if let Some(snapshot) = self.request_snapshot() {
            return Box::pin(SearchService::aggregate(&snapshot, request)).await;
        }
        let req = request.into_inner();
        let filters = RequestFilters::compile(&req.geo_filters, &req.filter)?;
        let compiled = compile_aggregations(&req)?;
        self.fanout_aggregate(&filters, &compiled, None)
            .await
            .map(Response::new)
    }

    async fn cluster_health(
        &self,
        _request: Request<ClusterHealthRequest>,
    ) -> Result<Response<ClusterHealthResponse>, Status> {
        if let Some(snapshot) = self.request_snapshot() {
            return Box::pin(SearchService::cluster_health(
                &snapshot,
                Request::new(ClusterHealthRequest {}),
            ))
            .await;
        }
        // Probe every primary, then every configured replica. Unreachable
        // is a reported outcome, never an error; a 2s probe timeout keeps
        // one filtered port from stalling the whole report.
        let mut probes: Vec<(u32, String, bool)> = Vec::new();
        for (shard, addr) in self.node_addrs.iter().enumerate() {
            probes.push((shard as u32, addr.clone(), false));
        }
        for (shard, replica) in self.replica_addrs.iter().enumerate() {
            if let Some(addr) = replica {
                probes.push((shard as u32, addr.clone(), true));
            }
        }
        let mut tasks = Vec::with_capacity(probes.len());
        for (shard, addr, is_replica) in probes {
            let client = self.node_client(&addr);
            tasks.push(tokio::spawn(async move {
                let outcome = match client {
                    Ok(mut client) => {
                        match tokio::time::timeout(
                            Duration::from_secs(2),
                            client.health(HealthRequest {}),
                        )
                        .await
                        {
                            Ok(reply) => reply.map(|r| r.into_inner()),
                            Err(_) => Err(Status::deadline_exceeded("health probe timed out")),
                        }
                    }
                    Err(e) => Err(e),
                };
                match outcome {
                    Ok(health) => ShardHealth {
                        shard,
                        addr,
                        is_replica,
                        reachable: true,
                        error: String::new(),
                        health: Some(health),
                    },
                    Err(e) => ShardHealth {
                        shard,
                        addr,
                        is_replica,
                        reachable: false,
                        error: e.message().to_string(),
                        health: None,
                    },
                }
            }));
        }
        let mut targets = Vec::with_capacity(tasks.len());
        for task in tasks {
            match task.await {
                Ok(target) => targets.push(target),
                Err(e) => return Err(Status::internal(format!("health probe task failed: {e}"))),
            }
        }
        let clustered_vector = if let Some(backend) = &self.clustered_vectors {
            Some(match backend.health().await {
                Ok(health) => ClusteredVectorHealth {
                    backend_kind: "clustered-turbovec".to_string(),
                    transport: backend.transport_name().to_string(),
                    reachable: true,
                    servable: health.servable,
                    error: health.error,
                    rows: health.rows,
                    topology_generation: health.topology_generation,
                },
                Err(status) => ClusteredVectorHealth {
                    backend_kind: "clustered-turbovec".to_string(),
                    transport: backend.transport_name().to_string(),
                    reachable: false,
                    servable: false,
                    error: status.to_string(),
                    rows: 0,
                    topology_generation: 0,
                },
            })
        } else {
            None
        };
        Ok(Response::new(ClusterHealthResponse {
            targets,
            clustered_vector,
        }))
    }

    async fn broadcast_vector_backend(
        &self,
        request: Request<BroadcastVectorBackendRequest>,
    ) -> Result<Response<BroadcastVectorBackendResponse>, Status> {
        if let Some(snapshot) = self.request_snapshot() {
            return Box::pin(SearchService::broadcast_vector_backend(&snapshot, request)).await;
        }
        let req = request.into_inner();
        if req.dim == 0 || req.config.is_none() {
            return Err(Status::invalid_argument(
                "positive dim and vector backend config are required",
            ));
        }
        let results = self.fanout_vector_backend(&req).await;
        Ok(Response::new(BroadcastVectorBackendResponse { results }))
    }

    async fn broadcast_calibration(
        &self,
        request: Request<BroadcastCalibrationRequest>,
    ) -> Result<Response<BroadcastCalibrationResponse>, Status> {
        if let Some(snapshot) = self.request_snapshot() {
            return Box::pin(SearchService::broadcast_calibration(&snapshot, request)).await;
        }
        let req = request.into_inner();
        if req.shift.len() != req.dim as usize || req.scale.len() != req.dim as usize {
            return Err(Status::invalid_argument(
                "shift and scale must have length dim",
            ));
        }
        let results = self.fanout_calibration(&req).await;
        Ok(Response::new(BroadcastCalibrationResponse { results }))
    }

    async fn variant_search(
        &self,
        request: Request<VariantSearchRequest>,
    ) -> Result<Response<VariantSearchResponse>, Status> {
        if let Some(snapshot) = self.request_snapshot() {
            return Box::pin(SearchService::variant_search(&snapshot, request)).await;
        }
        let req = request.into_inner();
        if req.variants.len() < 2 {
            return Err(Status::invalid_argument(format!(
                "variant search compares configurations: at least 2 variants required, got {}",
                req.variants.len()
            )));
        }
        // 0 = unset selects max_k, like every other client-facing k.
        let k = self.resolve_k(req.k)?;
        // Labels carry the whole result: a blank or duplicated one makes
        // the diffs unreadable, so reject rather than disambiguate.
        let mut seen: Vec<&str> = Vec::with_capacity(req.variants.len());
        for (i, v) in req.variants.iter().enumerate() {
            if v.label.is_empty() {
                return Err(Status::invalid_argument(format!(
                    "variant {i} has an empty label; every arm must be named"
                )));
            }
            if seen.contains(&v.label.as_str()) {
                return Err(Status::invalid_argument(format!(
                    "duplicate variant label {:?}: labels identify arms in the diffs",
                    v.label
                )));
            }
            seen.push(&v.label);
            if v.query.is_none() {
                return Err(Status::invalid_argument(format!(
                    "variant {:?} has no query set (expected bm25 or hybrid)",
                    v.label
                )));
            }
        }
        // RBO's persistence is a probability; 1.0 would never terminate
        // its weighting and is as much an error as 2.0.
        let rbo_p = if req.rbo_p == 0.0 {
            0.9
        } else {
            f64::from(req.rbo_p)
        };
        if !(rbo_p.is_finite() && rbo_p > 0.0 && rbo_p < 1.0) {
            return Err(Status::invalid_argument(format!(
                "rbo_p must be in (0, 1); got {}",
                req.rbo_p
            )));
        }
        if req.interleave && req.variants.len() != 2 {
            return Err(Status::invalid_argument(format!(
                "interleaving is a two-way method (team draft); got {} variants. \
                 Compare more arms with diffs, or interleave them pairwise.",
                req.variants.len()
            )));
        }
        let request_id = if req.request_id.is_empty() {
            format!(
                "req-{}-{}",
                std::process::id(),
                REQUEST_COUNTER.fetch_add(1, AtomicOrdering::Relaxed)
            )
        } else {
            req.request_id.clone()
        };

        // Sequential, not concurrent. Running the arms together would
        // make each one's `elapsed_ms` a measure of how hard the other
        // arms were hitting the same shards, and per-arm cost is part of
        // what an A/B is asked to report.
        let mut results = Vec::with_capacity(req.variants.len());
        for variant in &req.variants {
            let query = variant
                .query
                .as_ref()
                .expect("query presence checked above");
            let started = std::time::Instant::now();
            let hits = self.run_variant(query, k).await.map_err(|e| {
                // Name the arm: with several in flight, an unadorned
                // status leaves the caller guessing which one failed.
                Status::new(
                    e.code(),
                    format!("variant {:?}: {}", variant.label, e.message()),
                )
            })?;
            results.push(VariantResult {
                label: variant.label.clone(),
                hits,
                elapsed_ms: started.elapsed().as_secs_f32() * 1000.0,
            });
        }

        let diffs: Vec<RankingDiff> = results[1..]
            .iter()
            .map(|v| diff_against(&results[0], v, k as usize, rbo_p))
            .collect();

        let interleaving = req.interleave.then(|| {
            let a: Vec<u64> = results[0].hits.iter().map(|h| h.doc_id).collect();
            let b: Vec<u64> = results[1].hits.iter().map(|h| h.doc_id).collect();
            // A seed derived from the query text keeps a re-run of the
            // same query byte-identical while still varying across
            // queries, so determinism does not become a first-position
            // bias for one arm.
            let seed = if req.interleave_seed == 0 {
                crate::interleave::seed_for(variant_text(&req.variants[0]))
            } else {
                req.interleave_seed
            };
            let merged = crate::interleave::team_draft(&a, &b, k as usize, seed);
            Interleaving {
                doc_ids: merged.ids,
                teams: merged
                    .team
                    .into_iter()
                    .map(|t| match t {
                        crate::interleave::Team::A => InterleaveTeam::A as i32,
                        crate::interleave::Team::B => InterleaveTeam::B as i32,
                    })
                    .collect(),
                seed,
            }
        });

        Ok(Response::new(VariantSearchResponse {
            request_id,
            results,
            diffs,
            interleaving,
        }))
    }
}

/// The query text of an arm, whatever kind it is — the stable thing to
/// seed an interleaving from.
fn variant_text(variant: &crate::pb::SearchVariant) -> &str {
    match &variant.query {
        Some(search_variant::Query::Bm25(r)) => &r.text,
        Some(search_variant::Query::Hybrid(r)) => &r.text,
        None => "",
    }
}

#[cfg(test)]
mod stream_cancel_tests {
    use super::*;

    fn route(addr: &str, lo: u64, hi: u64) -> TopologyRoute {
        TopologyRoute {
            addr: addr.to_string(),
            replica: None,
            hash_range: Some((lo, hi)),
        }
    }

    #[test]
    fn topology_refuses_ragged_or_incomplete_hash_space() {
        assert!(
            build_topology(1, vec![route("a", 0, 9), route("b", 11, u64::MAX)])
                .err()
                .expect("gap must be refused")
                .contains("gap or overlap")
        );
        assert!(
            build_topology(1, vec![route("a", 0, 10), route("b", 10, u64::MAX)])
                .err()
                .expect("overlap must be refused")
                .contains("gap or overlap")
        );
        assert!(build_topology(
            1,
            vec![
                route("a", 0, u64::MAX),
                TopologyRoute {
                    addr: "b".to_string(),
                    replica: None,
                    hash_range: None,
                }
            ]
        )
        .err()
        .expect("ragged ranges must be refused")
        .contains("every shard or none"));
        assert!(build_topology(
            1,
            vec![
                TopologyRoute {
                    addr: "a".into(),
                    replica: Some("b".into()),
                    hash_range: Some((0, 9)),
                },
                route("b", 10, u64::MAX),
            ],
        )
        .err()
        .expect("a replica cannot also serve another logical shard")
        .contains("duplicate topology endpoint"));
    }

    #[test]
    fn hot_topology_snapshots_one_generation_per_request() {
        let coordinator = CoordinatorServiceImpl::new(vec!["old".to_string()])
            .with_topology_generation(4)
            .with_hot_topology(vec![Some((0, u64::MAX))])
            .unwrap();
        let old = coordinator.request_snapshot().unwrap();

        coordinator
            .reload_topology(5, vec![route("new", 0, u64::MAX)])
            .unwrap();
        let new = coordinator.request_snapshot().unwrap();

        assert_eq!(old.topology_generation, 4);
        assert_eq!(old.node_addrs, vec!["old"]);
        assert_eq!(new.topology_generation, 5);
        assert_eq!(new.node_addrs, vec!["new"]);
        assert_eq!(coordinator.current_topology_generation(), 5);
        assert!(coordinator
            .reload_topology(5, vec![route("newer", 0, u64::MAX)])
            .unwrap_err()
            .contains("must increase"));
    }

    #[tokio::test]
    async fn cutover_barrier_publishes_a_new_shard_count_atomically() {
        let coordinator = CoordinatorServiceImpl::new(vec!["old".to_string()])
            .with_topology_generation(4)
            .with_hot_topology(vec![Some((0, u64::MAX))])
            .unwrap();
        let frozen = SearchService::freeze_topology_writes(
            &coordinator,
            Request::new(FreezeTopologyWritesRequest {
                required_topology_generation: 4,
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert!(coordinator
            .reload_topology(5, vec![route("other", 0, u64::MAX)])
            .unwrap_err()
            .contains("frozen writes"));

        let split = u64::MAX / 2;
        SearchService::publish_topology(
            &coordinator,
            Request::new(PublishTopologyRequest {
                cutover_token: frozen.cutover_token,
                generation: 5,
                shards: vec![
                    crate::pb::PublishedTopologyShard {
                        addr: "a:50051".into(),
                        replica: String::new(),
                        hash_lo: 0,
                        hash_hi: split,
                    },
                    crate::pb::PublishedTopologyShard {
                        addr: "b:50051".into(),
                        replica: String::new(),
                        hash_lo: split + 1,
                        hash_hi: u64::MAX,
                    },
                ],
            }),
        )
        .await
        .unwrap();
        assert_eq!(coordinator.current_topology_generation(), 5);
        assert_eq!(coordinator.current_topology_routes().len(), 2);
        let (_, routed) = coordinator.route_stable_key(b"stable-product-id").unwrap();
        assert!(routed < 2);

        let frozen = SearchService::freeze_topology_writes(
            &coordinator,
            Request::new(FreezeTopologyWritesRequest {
                required_topology_generation: 5,
            }),
        )
        .await
        .unwrap()
        .into_inner();
        let aborted = SearchService::abort_topology_cutover(
            &coordinator,
            Request::new(AbortTopologyCutoverRequest {
                cutover_token: frozen.cutover_token,
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(aborted.topology_generation, 5);
    }

    #[test]
    fn stable_product_keys_route_deterministically() {
        let split = u64::MAX / 2;
        let coordinator = CoordinatorServiceImpl::new(vec!["a".to_string(), "b".to_string()])
            .with_topology_generation(9)
            .with_hot_topology(vec![Some((0, split)), Some((split + 1, u64::MAX))])
            .unwrap();
        let key = b"courtlistener:opinion:123/chunk:7";
        let expected = usize::from(stable_routing_hash(key) > split);
        assert_eq!(coordinator.route_stable_key(key), Ok((9, expected)));
        assert_eq!(coordinator.route_stable_key(key), Ok((9, expected)));
        assert!(coordinator
            .route_stable_key(b"")
            .unwrap_err()
            .contains("empty"));
    }

    #[test]
    fn in_process_mode_has_no_network_fallback_or_udp_lane() {
        let coordinator = CoordinatorServiceImpl::with_in_process_channels(Vec::new());
        assert!(!coordinator.allows_network());
        let error = coordinator
            .channel_to("http://must-not-resolve.invalid:50051")
            .expect_err("a missing in-process channel must not dial");
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(coordinator.floor_socket().is_none());
        assert!(coordinator
            .floor_target("must-not-resolve.invalid:50051")
            .is_none());
    }

    #[tokio::test]
    async fn cancellation_uses_typed_udp_then_authoritative_grpc_stop() {
        let udp_rx = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let target = udp_rx.local_addr().unwrap();
        let udp_tx = Arc::new(std::net::UdpSocket::bind("127.0.0.1:0").unwrap());
        udp_tx.set_nonblocking(true).unwrap();

        let (request_tx, mut request_rx) = mpsc::channel(1);
        let (_merged_tx, merged_rx) = mpsc::channel(1);
        let token = 0x0A11_CE11_u64;
        let mut fanout = StreamFanout {
            merged_rx,
            floor_txs: vec![Some(request_tx)],
            udp_lanes: vec![Some((token, target))],
            udp_socket: Some(udp_tx),
        };

        fanout.cancel().await;

        let request = request_rx.recv().await.expect("authoritative Stop");
        assert!(matches!(
            request.payload,
            Some(stream_search_request::Payload::Stop(_))
        ));
        assert!(
            request_rx.recv().await.is_none(),
            "request stream must close"
        );

        let mut frame = [0u8; crate::stream_signal::FRAME_LEN];
        let (len, _) = tokio::time::timeout(Duration::from_secs(1), udp_rx.recv_from(&mut frame))
            .await
            .expect("UDP cancel timed out")
            .unwrap();
        assert_eq!(len, crate::stream_signal::FRAME_LEN);
        assert_eq!(
            crate::stream_signal::decode(&frame),
            Some(crate::stream_signal::StreamSignal::Cancel { token })
        );
    }

    #[tokio::test]
    async fn dropping_an_unfinished_fanout_still_stops_both_lanes() {
        let udp_rx = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let target = udp_rx.local_addr().unwrap();
        let udp_tx = Arc::new(std::net::UdpSocket::bind("127.0.0.1:0").unwrap());
        udp_tx.set_nonblocking(true).unwrap();

        let (request_tx, mut request_rx) = mpsc::channel(1);
        let (_merged_tx, merged_rx) = mpsc::channel(1);
        let token = 0x0D09_CE11_u64;
        drop(StreamFanout {
            merged_rx,
            floor_txs: vec![Some(request_tx)],
            udp_lanes: vec![Some((token, target))],
            udp_socket: Some(udp_tx),
        });

        let request = tokio::time::timeout(Duration::from_secs(1), request_rx.recv())
            .await
            .expect("gRPC Stop task timed out")
            .expect("authoritative Stop");
        assert!(matches!(
            request.payload,
            Some(stream_search_request::Payload::Stop(_))
        ));

        let mut frame = [0u8; crate::stream_signal::FRAME_LEN];
        let (len, _) = tokio::time::timeout(Duration::from_secs(1), udp_rx.recv_from(&mut frame))
            .await
            .expect("UDP cancel timed out")
            .unwrap();
        assert_eq!(len, crate::stream_signal::FRAME_LEN);
        assert_eq!(
            crate::stream_signal::decode(&frame),
            Some(crate::stream_signal::StreamSignal::Cancel { token })
        );
    }
}

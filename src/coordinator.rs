//! Coordinator side: client-facing [`SearchService`] that fans queries out
//! to shard nodes, aggregates their floors mid-scan, and merges results.

use crate::stats_identity::StatsClaim;
use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
#[cfg(feature = "net")]
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status, Streaming};

use crate::bm25::{Bm25Params, CorpusStats};
#[cfg(feature = "net")]
use crate::clustered_turbovec::{
    ClusteredCandidateEvent, ClusteredLabelFilter, ClusteredTurboVecBackend,
};
use crate::fusion::{self, Leg};
use crate::merge::{cmp_hits, merge_topk, FloorTracker, MergedHit};
use crate::metrics::Route;
use crate::pb::search_service_server::{SearchService, SearchServiceServer};
#[cfg(feature = "net")]
use crate::pb::HybridLegHit;
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
    HybridDebug, HybridHit, HybridSearchRequest, HybridSearchResponse, HybridShardDebug,
    HybridShardRequest, ParentGroup, PublishTopologyRequest, PublishTopologyResponse,
    RoutedIngestMappedRequest, RoutedIngestMappedResponse, RoutedShardIngest, ScoredHit,
    SearchRequest, SearchResponse, SearchShardDone, SearchShardRequest, SearchShardResponse,
    SetCalibrationRequest, ShardHealth, ShardLegsRequest, ShardScanStats, StartShardSearch,
    StartStreamSearch, StopStreamSearch, StreamSearchRequest, StreamSearchResponse,
    StreamSearchSummary, TermStatsRequest, VectorBackendApplyResult, VectorRescoreRequest,
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

struct QueryProgressPublisher {
    sender: watch::Sender<Option<QueryProgress>>,
    // Set before selection. This reader has no progress publisher, so the
    // retained read context cannot form a cycle through the watch channel.
    reader: std::sync::OnceLock<Arc<CoordinatorServiceImpl>>,
}

fn query_stream_content_fingerprint(
    phase: crate::pb::QueryStreamPhase,
    hits: &[crate::pb::QueryStreamHit],
    identity_state: crate::pb::QueryStreamIdentityState,
) -> String {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"protomolt-query-revision-v2\0");
    bytes.extend_from_slice(&(phase as i32).to_le_bytes());
    bytes.extend_from_slice(&(identity_state as i32).to_le_bytes());
    for hit in hits {
        bytes.extend_from_slice(&hit.doc_id.to_le_bytes());
        // The implicit protobuf float field omits both signed zeros. Hash
        // the value a decoder receives, including for in-process clients.
        let score_bits = if hit.score == 0.0 {
            0
        } else {
            hit.score.to_bits()
        };
        bytes.extend_from_slice(&score_bits.to_le_bytes());
        bytes.extend_from_slice(&hit.rank.to_le_bytes());
        match &hit.identity {
            None => bytes.push(0),
            Some(identity) => {
                bytes.push(1);
                bytes.extend_from_slice(&(identity.document_key.len() as u64).to_le_bytes());
                bytes.extend_from_slice(&identity.document_key);
                bytes.extend_from_slice(&identity.version.to_le_bytes());
                match identity.chunk_ordinal {
                    None => bytes.push(0),
                    Some(ordinal) => {
                        bytes.push(1);
                        bytes.extend_from_slice(&ordinal.to_le_bytes());
                    }
                }
            }
        }
    }
    crate::sha256::hex_digest(&bytes)
}

fn query_stream_revision(
    revision: u64,
    phase: crate::pb::QueryStreamPhase,
    hits: Vec<(u64, f32, Option<crate::pb::DocumentIdentity>)>,
    scoring_fingerprint: String,
    identity_state: crate::pb::QueryStreamIdentityState,
) -> crate::pb::QueryStreamRevision {
    let hits = hits
        .into_iter()
        .enumerate()
        .map(
            |(rank, (doc_id, score, identity))| crate::pb::QueryStreamHit {
                doc_id,
                score,
                rank: rank as u32 + 1,
                identity: if identity_state == crate::pb::QueryStreamIdentityState::Resolved {
                    identity
                } else {
                    None
                },
            },
        )
        .collect::<Vec<_>>();
    let content_fingerprint = query_stream_content_fingerprint(phase, &hits, identity_state);
    crate::pb::QueryStreamRevision {
        revision,
        phase: phase as i32,
        hits,
        content_fingerprint,
        scoring_fingerprint,
        identity_state: identity_state as i32,
        content_fingerprint_version: 2,
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
/// Candidate ids per rescore call (`Bm25Rescore`, `VectorRescore`) when a
/// boolean group scores a clause over its surviving ids. A lexical call
/// is one cursor walk over its candidates and a dense call is one masked
/// scan of the shard, so the pieces only pipeline the wire with the
/// shards' work: on the local benchmark a 600,000-row membership over
/// 2,000,000 rows scored in 1.20 s at pieces of 10,000 and 1.27 s in one
/// call (docs/benchmarks/partition-pruning-2026-09.md). The knob exists
/// so the batch is its own setting and not `max_k`; this default is the
/// measured one.
pub const DEFAULT_SIGNAL_BATCH: u32 = 10_000;
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
    /// range or omit one; mixed/ragged maps are refused. Under a
    /// placement tree the ranges tile the hash space per leaf.
    pub hash_range: Option<(u64, u64)>,
    /// The placement code this shard serves (`docs/placement.md`);
    /// required on every route when the map has a tree, refused
    /// without one.
    pub placement: Option<i64>,
}

struct CoordinatorTopology {
    generation: u64,
    routes: Vec<TopologyRoute>,
    stats_cache: Arc<crate::stats_cache::StatsCache>,
    /// The validated placement tree of this generation, if any.
    placement: Option<Arc<crate::placement::Placement>>,
}

/// One generation's routing inputs: generation, hash range per shard,
/// placement code per shard, and whether the map has a tree.
type RoutingView = (u64, Vec<Option<(u64, u64)>>, Vec<Option<i64>>, bool);

/// Inclusive hash ranges `(lo, hi, shard)` must tile `0..=u64::MAX`
/// with no gap and no overlap.
fn check_hash_tiling(mut ranges: Vec<(u64, u64, usize)>, scope: &str) -> Result<(), String> {
    ranges.sort_by_key(|range| range.0);
    let mut expected = 0u64;
    for (position, (lo, hi, shard)) in ranges.iter().copied().enumerate() {
        if lo != expected {
            return Err(format!(
                "topology hash space{scope} has a gap or overlap before shard {shard}: expected {expected}, got {lo}"
            ));
        }
        if position + 1 == ranges.len() {
            if hi != u64::MAX {
                return Err(format!(
                    "topology hash space{scope} ends at {hi}, not {}",
                    u64::MAX
                ));
            }
        } else {
            expected = hi.checked_add(1).ok_or_else(|| {
                format!("topology shard {shard} reaches the hash-space end too early")
            })?;
        }
    }
    Ok(())
}

fn build_topology(
    generation: u64,
    routes: Vec<TopologyRoute>,
    tree: Option<&crate::placement::PlacementTreeConfig>,
) -> Result<CoordinatorTopology, String> {
    if routes.is_empty() {
        return Err("topology requires at least one primary shard".to_string());
    }
    let placement = match tree {
        Some(tree) => Some(Arc::new(crate::placement::Placement::validate(tree)?)),
        None => None,
    };
    match placement.as_ref() {
        None => {
            if let Some(shard) = routes.iter().position(|route| route.placement.is_some()) {
                return Err(format!(
                    "topology shard {shard} carries a placement code but the map has no \
                     placement tree"
                ));
            }
        }
        Some(placement) => {
            let mut served = vec![0usize; placement.leaves().len()];
            for (shard, route) in routes.iter().enumerate() {
                let Some(code) = route.placement else {
                    return Err(format!(
                        "topology shard {shard} has no placement code; the map has a placement \
                         tree, so every shard names the leaf it serves"
                    ));
                };
                let Some(index) = placement.leaves().iter().position(|leaf| leaf.code == code)
                else {
                    return Err(format!(
                        "topology shard {shard} names placement code {code}, which is no leaf \
                         of the tree"
                    ));
                };
                served[index] += 1;
            }
            for (leaf, count) in placement.leaves().iter().zip(&served) {
                if *count == 0 {
                    return Err(format!(
                        "placement leaf {:?} (code {}) has no shard",
                        leaf.name, leaf.code
                    ));
                }
            }
        }
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
        let ranges = |code: Option<i64>| -> Vec<(u64, u64, usize)> {
            routes
                .iter()
                .enumerate()
                .filter(|(_, route)| code.is_none() || route.placement == code)
                .map(|(shard, route)| {
                    let (lo, hi) = route.hash_range.expect("all routes ranged");
                    (lo, hi, shard)
                })
                .collect()
        };
        match placement.as_ref() {
            None => check_hash_tiling(ranges(None), "")?,
            Some(placement) => {
                for leaf in placement.leaves() {
                    check_hash_tiling(
                        ranges(Some(leaf.code)),
                        &format!(" of placement leaf {:?}", leaf.name),
                    )?;
                }
            }
        }
    }
    Ok(CoordinatorTopology {
        generation,
        stats_cache: Arc::new(crate::stats_cache::StatsCache::new(routes.len())),
        routes,
        placement,
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

#[cfg(feature = "net")]
#[derive(Clone, Copy, Debug)]
struct ProductLabelRange {
    start: u64,
    end: u64,
    shard: u32,
}

#[cfg(feature = "net")]
impl ProductLabelRange {
    fn contains(self, label: u64) -> bool {
        label >= self.start && label < self.end
    }
}

#[cfg(feature = "net")]
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
    /// The collection this coordinator serves (`docs/collections.md`):
    /// empty for the unnamed single dataset. A request naming another
    /// collection is refused by `admit`; a node reporting another is
    /// refused by `verify_collection_membership` and flagged in health.
    collection: String,
    /// Public paging tokens share a key across clones, including hot snapshots.
    cursor_signer: Arc<crate::query_cursor::CursorSigner>,
    /// TLS material for the channels to shards (`docs/security.md`);
    /// `None` uses the process-wide material when installed, plaintext
    /// otherwise.
    client_tls: Option<crate::security::ClientTls>,
    /// The key that signs UDP floor and cancel datagrams; without one
    /// the datagrams are plain, which a node accepts on loopback only.
    udp_hmac_key: Option<crate::security::UdpKey>,
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
    /// Runtime knobs (`max_k`, the hedge delay) read at request time
    /// (docs/diagnostics.md); shared by every clone of this coordinator.
    knobs: Arc<crate::diagnostics::Knobs>,
    /// Hard request-wide logical FP32 row-byte bound for reranking.
    max_rerank_bytes: u64,
    /// One reusable channel per address, created on first use.
    links: Arc<Mutex<HashMap<String, crate::link::NodeLink>>>,
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
    document_visibility: Option<crate::pb::DocumentVisibility>,
    field_permissions: Option<crate::field_permissions::FieldScope>,
    /// Versions captured before a public query starts, shared with every
    /// delegate. Final disclosure verifies them again; value fetches require
    /// them at the read boundary as well.
    query_read_versions: Option<Arc<Vec<StatsClaim>>>,
    vector_read_field: Option<String>,
    /// Optional distributed vector collection. The product coordinator calls
    /// it once as one provider; it never learns or re-fans its shard topology.
    #[cfg(feature = "net")]
    clustered_vectors: Option<ClusteredTurboVecBackend>,
    /// Optional measured candidate-depth contract for FP32 reranking.
    dense_quality_profile: Option<Arc<crate::quality::DenseQualityProfile>>,
    /// The coordinator's synonym table (`docs/synonyms.md`), applied to
    /// every lexical query unless the request turns it off.
    synonyms: Option<Arc<crate::synonyms::SynonymTable>>,
    /// The generation-bound policy AUTO consults on a non-exhaustive
    /// provider (`docs/dense-execution-policy.md`).
    dense_execution_policy: Option<Arc<crate::dense_policy::DenseExecutionPolicy>>,
    /// Product shard-map generation (zero for the implicit static list).
    topology_generation: u64,
    /// Inclusive stable-key ranges parallel to `node_addrs`. Empty for the
    /// legacy explicitly addressed topology.
    hash_ranges: Vec<Option<(u64, u64)>>,
    /// Placement codes parallel to `node_addrs` (`docs/placement.md`),
    /// and the validated tree they name. Empty and `None` without one.
    placement_codes: Vec<Option<i64>>,
    placement: Option<Arc<crate::placement::Placement>>,
    /// Hot topology authority. Public RPC entry points snapshot this once and
    /// recurse into a frozen clone with this field cleared, so no request can
    /// observe half of two generations.
    live_topology: Option<Arc<RwLock<Arc<CoordinatorTopology>>>>,
    /// The generation the live topology currently serves, as a watch so a
    /// relay (`docs/relay-coordinators.md`, "Map interface") wakes when the
    /// map moves under it. Replaced on every publication.
    topology_watch: Arc<watch::Sender<u64>>,
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
    query_progress: Option<Arc<QueryProgressPublisher>>,
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

/// A stale lifetime or epoch invalidates cached shares. Retry once with fresh
/// statistics and their complete claim; a second mutation returns the refusal.
/// Never drop the fence to manufacture success under continuous writes.
pub(crate) fn is_stale_stats(status: &Status) -> bool {
    status.code() == tonic::Code::FailedPrecondition
        && status.message().starts_with(crate::node::STALE_STATS_EPOCH)
}

struct Bm25StreamHeap {
    heap: std::collections::BinaryHeap<StreamHeapEntry>,
    floors_sent: u64,
    progress: Option<Arc<QueryProgressPublisher>>,
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
    mut client: crate::link::NodeLink,
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
                    progress.sender.send_if_modified(|current| {
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
    crate::segment_prune::PruneStats,
);

/// Merge per-shard column stats: counts and sums add, mins and maxes
/// fold — additive across shards exactly as facet counts are, so the
/// coordinator merge is the same positional walk. `mean` is computed
/// HERE (sum / count) so clients cannot get it wrong; a column NO
/// shard knows is refused by name, the usual typo rule.
/// The evaluated values for one candidate set (`fetch_values`).
pub struct FetchedValues {
    /// Explicit identity/absence records when requested, under the same receipt.
    pub identities: HashMap<u64, Option<crate::pb::DocumentIdentity>>,
    /// doc -> projected values, aligned with the request projections.
    pub rows: HashMap<u64, Vec<crate::pb::ProjectedValue>>,
    /// Per stage: doc -> identity-score contribution. A doc absent
    /// from a map has no value for that stage's column.
    pub stage_rows: Vec<HashMap<u64, f64>>,
    /// Versions read, in node order. Empty only when an unscoped request had
    /// neither projections nor stages and performed no fan-out.
    pub epochs: Vec<StatsClaim>,
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
/// The distinct-value cap a CARDINALITY aggregation gets when the
/// request names none (docs/aggregations.md "Cardinality").
pub(crate) const DEFAULT_MAX_DISTINCT: u32 = 100_000;

#[derive(Clone)]
pub(crate) struct CompiledAggregate {
    pub(crate) aggregations: Vec<crate::pb::CompiledAggregation>,
    pub(crate) histograms: Vec<crate::pb::CompiledHistogram>,
    pub(crate) percentiles: Vec<crate::pb::CompiledPercentile>,
    pub(crate) percentile_specs: Vec<crate::pb::PercentileSpec>,
    pub(crate) group_by: String,
    pub(crate) max_groups: u32,
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
        let op = crate::node::agg_op_of(a.op).map_err(|e| {
            Status::invalid_argument(format!("aggregation {:?}: {}", a.name, e.message()))
        })?;
        let cardinality = op == crate::pb::AggregateOp::Cardinality;
        if a.max_distinct != 0 && !cardinality {
            return Err(Status::invalid_argument(format!(
                "aggregation {:?}: max_distinct applies to CARDINALITY, not {}",
                a.name,
                crate::node::agg_op_name(op)
            )));
        }
        let expr = crate::cel::compile_value(&a.expression).map_err(|e| {
            Status::invalid_argument(format!("aggregation {:?}: {}", a.name, e.message()))
        })?;
        compiled.push(crate::pb::CompiledAggregation {
            expr: Some(expr),
            op: a.op,
            name: a.name.clone(),
            max_distinct: match (cardinality, a.max_distinct) {
                (false, _) => 0,
                (true, 0) => DEFAULT_MAX_DISTINCT,
                (true, cap) => cap,
            },
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
        let calendar = if h.calendar != 0 {
            let unit = crate::calendar::interval_of(h.calendar).ok_or_else(|| {
                Status::invalid_argument(format!(
                    "histogram {:?}: unknown calendar interval {}",
                    h.name, h.calendar
                ))
            })?;
            if h.interval != 0.0 {
                return Err(Status::invalid_argument(format!(
                    "histogram {:?}: a calendar histogram buckets by {}; its fixed \
                     interval must be zero, got {}",
                    h.name,
                    crate::calendar::interval_name(unit),
                    h.interval
                )));
            }
            if h.utc_offset_minutes.abs() > crate::calendar::MAX_UTC_OFFSET_MINUTES {
                return Err(Status::invalid_argument(format!(
                    "histogram {:?}: utc_offset_minutes {} is outside +-{}",
                    h.name,
                    h.utc_offset_minutes,
                    crate::calendar::MAX_UTC_OFFSET_MINUTES
                )));
            }
            Some(unit)
        } else {
            if h.utc_offset_minutes != 0 {
                return Err(Status::invalid_argument(format!(
                    "histogram {:?}: utc_offset_minutes applies to a calendar histogram; \
                     name a calendar interval",
                    h.name
                )));
            }
            if !(h.interval > 0.0 && h.interval.is_finite()) {
                return Err(Status::invalid_argument(format!(
                    "histogram {:?}: the interval must be positive and finite, got {}",
                    h.name, h.interval
                )));
            }
            None
        };
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
            calendar: calendar.map_or(0, |unit| unit as i32),
            utc_offset_minutes: h.utc_offset_minutes,
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

/// Exact nearest rank for a validated IEEE percentile in [0, 100].
/// Integer arithmetic avoids rounding the count or p/100 before taking ceil.
fn nearest_percentile_rank(p: f64, count: u64) -> u64 {
    if count == 0 {
        return 0;
    }
    let bits = p.to_bits();
    let exponent = ((bits >> 52) & 0x7ff) as u32;
    let fraction = bits & ((1u64 << 52) - 1);
    let (mantissa, shift) = if exponent == 0 {
        (fraction, 1074)
    } else {
        (fraction | (1u64 << 52), 1075 - exponent)
    };
    // The numerator has at most 117 bits. A denominator too large for
    // u128 therefore yields a fractional rank below one (or zero).
    if shift > 121 {
        return 1;
    }
    let numerator = u128::from(mantissa) * u128::from(count);
    let denominator = 100u128 << shift;
    (numerator.div_ceil(denominator) as u64).clamp(1, count)
}

/// One percentile expression's merged phase-1 statistics.
pub(crate) struct PctMerge {
    vt: Option<crate::pb::AggregateValueType>,
    present: u64,
    unrankable: u64,
    min_bits: u64,
    max_bits: u64,
}

impl PctMerge {
    pub(crate) fn new() -> Self {
        Self {
            vt: None,
            present: 0,
            unrankable: 0,
            min_bits: 0,
            max_bits: 0,
        }
    }

    pub(crate) fn fold(
        &mut self,
        p: &crate::pb::PercentilePartial,
        name: &str,
    ) -> Result<(), Status> {
        use crate::pb::AggregateValueType as T;
        let vt = match T::try_from(p.vtype) {
            Ok(T::Absent) => return Ok(()),
            Ok(T::Int) => T::Int,
            Ok(T::Uint) => T::Uint,
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
        self.unrankable = self.unrankable.checked_add(p.unrankable).ok_or_else(|| {
            Status::failed_precondition(format!(
                "percentile {name:?}: unrankable count overflows u64"
            ))
        })?;
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
        self.present = self.present.checked_add(p.present).ok_or_else(|| {
            Status::failed_precondition(format!("percentile {name:?}: present count overflows u64"))
        })?;
        Ok(())
    }

    /// The merged state as one shard's partial: what a relay answers
    /// its parent after folding its children in child order, so the
    /// parent folds it as it folds a shard's.
    pub(crate) fn partial(&self) -> crate::pb::PercentilePartial {
        crate::pb::PercentilePartial {
            vtype: self
                .vt
                .unwrap_or(crate::pb::AggregateValueType::Absent)
                .into(),
            present: self.present,
            unrankable: self.unrankable,
            min_bits: self.min_bits,
            max_bits: self.max_bits,
        }
    }
}

/// One aggregation's merged fleet-wide statistics: a type vote plus
/// every fold, gated per type. Extrema and moments fold only over
/// shards that HELD values; the type vote counts on any shard whose
/// columns resolve, so cross-shard type disagreement stays loud even
/// when one side is empty.
pub(crate) struct AggMerge {
    vt: Option<crate::pb::AggregateValueType>,
    present: u64,
    /// CARDINALITY: the fleet-wide union of the shards' distinct
    /// values, typed by the vote. A BTreeSet so the union is
    /// deterministic whatever the shard order.
    distinct: Distinct,
    uint_sum: u128,
    uint_min: u64,
    uint_max: u64,
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

/// The exact distinct-value union behind CARDINALITY, one set per
/// type; doubles by canonical bits, strings by dictionary term.
#[derive(Default)]
struct Distinct {
    ints: std::collections::BTreeSet<i64>,
    uints: std::collections::BTreeSet<u64>,
    doubles: std::collections::BTreeSet<u64>,
    strings: std::collections::BTreeSet<String>,
    bools: std::collections::BTreeSet<bool>,
}

impl Distinct {
    fn len(&self) -> usize {
        self.ints.len()
            + self.uints.len()
            + self.doubles.len()
            + self.strings.len()
            + self.bools.len()
    }
}

impl AggMerge {
    pub(crate) fn new() -> Self {
        Self {
            vt: None,
            present: 0,
            distinct: Distinct::default(),
            uint_sum: 0,
            uint_min: 0,
            uint_max: 0,
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
    pub(crate) fn fold(
        &mut self,
        p: &crate::pb::AggregatePartial,
        agg: &crate::pb::CompiledAggregation,
    ) -> Result<(), Status> {
        use crate::pb::AggregateValueType as T;
        let name = agg.name.as_str();
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
        let next_present = self.present.checked_add(p.present).ok_or_else(|| {
            Status::failed_precondition(format!(
                "aggregation {name:?}: present count overflows u64"
            ))
        })?;
        match vt {
            T::Int => {
                let sum = (i128::from(p.int_sum_hi) << 64) | i128::from(p.int_sum_lo);
                self.int_sum = self.int_sum.checked_add(sum).ok_or_else(|| {
                    Status::failed_precondition(format!(
                        "aggregation {name:?}: int partial sum overflows i128"
                    ))
                })?;
                if self.present == 0 {
                    self.int_min = p.int_min;
                    self.int_max = p.int_max;
                } else {
                    self.int_min = self.int_min.min(p.int_min);
                    self.int_max = self.int_max.max(p.int_max);
                }
            }
            T::Uint => {
                let sum = (u128::from(p.uint_sum_hi) << 64) | u128::from(p.uint_sum_lo);
                self.uint_sum = self.uint_sum.checked_add(sum).ok_or_else(|| {
                    Status::failed_precondition(format!(
                        "aggregation {name:?}: uint partial sum overflows u128"
                    ))
                })?;
                if self.present == 0 {
                    self.uint_min = p.uint_min;
                    self.uint_max = p.uint_max;
                } else {
                    self.uint_min = self.uint_min.min(p.uint_min);
                    self.uint_max = self.uint_max.max(p.uint_max);
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
            T::String | T::Bool => {}
            T::Absent | T::Unspecified => unreachable!("handled above"),
        }
        self.present = next_present;
        if agg.op == crate::pb::AggregateOp::Cardinality as i32 {
            self.distinct.ints.extend(p.distinct_ints.iter().copied());
            self.distinct.uints.extend(p.distinct_uints.iter().copied());
            self.distinct
                .doubles
                .extend(p.distinct_double_bits.iter().copied());
            self.distinct
                .strings
                .extend(p.distinct_strings.iter().cloned());
            self.distinct.bools.extend(p.distinct_bools.iter().copied());
            if self.distinct.len() > agg.max_distinct as usize {
                return Err(Status::failed_precondition(format!(
                    "aggregation {name:?}: more than {} distinct values across the \
                     fleet; raise max_distinct or tighten the filter",
                    agg.max_distinct
                )));
            }
        }
        Ok(())
    }

    /// The merged state as one shard's partial (`PctMerge::partial`
    /// says why): the exact int sum split into its halves, the
    /// compensated double sum as its pair, the distinct sets as sorted
    /// lists.
    pub(crate) fn partial(&self) -> crate::pb::AggregatePartial {
        crate::pb::AggregatePartial {
            vtype: self
                .vt
                .unwrap_or(crate::pb::AggregateValueType::Absent)
                .into(),
            present: self.present,
            int_sum_hi: (self.int_sum >> 64) as i64,
            int_sum_lo: self.int_sum as u64,
            double_sum: self.dsum,
            double_compensation: self.dcomp,
            int_min: self.int_min,
            int_max: self.int_max,
            double_min: self.dmin,
            double_max: self.dmax,
            mean: self.mean,
            m2: self.m2,
            distinct_ints: self.distinct.ints.iter().copied().collect(),
            distinct_double_bits: self.distinct.doubles.iter().copied().collect(),
            distinct_strings: self.distinct.strings.iter().cloned().collect(),
            distinct_bools: self.distinct.bools.iter().copied().collect(),
            uint_sum_hi: (self.uint_sum >> 64) as u64,
            uint_sum_lo: self.uint_sum as u64,
            uint_min: self.uint_min,
            uint_max: self.uint_max,
            distinct_uints: self.distinct.uints.iter().copied().collect(),
        }
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
        let uint_typed = self.vt == Some(T::Uint);
        let value = match op {
            O::Count => Some(W::IntValue(i64::try_from(self.present).map_err(|_| {
                Status::failed_precondition(format!("aggregation {name:?}: count does not fit the int result; tighten the filter"))
            })?)),
            O::Cardinality => Some(W::IntValue(self.distinct.len() as i64)),
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
            O::Sum if uint_typed => Some(W::UintValue(u64::try_from(self.uint_sum).map_err(|_| {
                Status::failed_precondition(format!(
                    "aggregation {name:?}: the exact uint sum {} does not fit u64; aggregate double(...) for an IEEE sum",
                    self.uint_sum
                ))
            })?)),
            O::Sum => Some(W::DoubleValue(self.dsum + self.dcomp)),
            O::Min if uint_typed => Some(W::UintValue(self.uint_min)),
            O::Min if int_typed => Some(W::IntValue(self.int_min)),
            O::Min => Some(W::DoubleValue(self.dmin)),
            O::Max if uint_typed => Some(W::UintValue(self.uint_max)),
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
        T::Uint => "uint",
        T::Double => "double",
        T::String => "string",
        T::Bool => "bool",
        T::Absent | T::Unspecified => "absent",
    }
}

fn merge_column_stats(
    requested: &[String],
    shard_stats: &[Vec<crate::pb::ColumnStats>],
) -> Result<Vec<crate::pb::ColumnStats>, Status> {
    crate::column_stats::merge(requested, shard_stats)
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
    crate::rangefacet::merge(requested, shard_ranges, true)
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
             literal selects the facet tables, a number the i64/u64/f64 tables), or the \
             nodes' --facet-fields / --numeric-fields / --integer-fields / --unsigned-integer-fields / \
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

/// The provider-mismatch line of a cluster health report: empty when
/// every reachable, configured shard scores under one backend kind and
/// scoring fingerprint, else every distinct pair with the shards that
/// serve it.
fn provider_mismatch_of(targets: &[ShardHealth]) -> String {
    let mut seen: Vec<((String, String), Vec<String>)> = Vec::new();
    for target in targets {
        let Some(health) = &target.health else {
            continue;
        };
        if health.vector_backend.is_empty() {
            continue;
        }
        let key = (
            health.vector_backend.clone(),
            health.scoring_fingerprint.clone(),
        );
        let who = format!("shard {} ({})", target.shard, target.addr);
        match seen.iter_mut().find(|(k, _)| *k == key) {
            Some((_, shards)) => shards.push(who),
            None => seen.push((key, vec![who])),
        }
    }
    if seen.len() <= 1 {
        return String::new();
    }
    seen.iter()
        .map(|((kind, fingerprint), shards)| format!("{kind}/{fingerprint}: {}", shards.join(", ")))
        .collect::<Vec<_>>()
        .join("; ")
}

/// One provider identity the shard fleet agrees on
/// (`CoordinatorServiceImpl::fleet_vector_identity`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FleetVectorIdentity {
    pub provider: String,
    pub scoring_fingerprint: String,
    pub dimensions: u32,
    pub rows: u64,
}

/// The request side of a dense execution policy key
/// (`docs/dense-execution-policy.md`): the requested `k` as sent (0 is
/// refused by the policy, not defaulted), the candidate depth the
/// request named (`selection_k`, 0 when none), and the request's
/// filters, whose live selectivity the coordinator measures when AUTO
/// consults the policy.
#[derive(Debug, Clone, Copy)]
pub struct DenseRequestKey<'a> {
    pub k: u32,
    pub candidate_depth: u32,
    pub filters: Option<&'a RequestFilters>,
}

/// A browse resume boundary: the last returned id, plus its adjusted
/// sort-key bits when the browse is column-ordered.
#[derive(Debug, Clone)]
pub struct BrowseAfter {
    pub id: u64,
    /// The boundary's sort keys, parallel to the request's sort list
    /// (empty for an id-ordered browse).
    pub keys: Vec<crate::sortkeys::Key>,
}

/// One merged browse page.
#[derive(Debug, Clone)]
pub struct BrowseRows {
    /// Sealed segments the shards consulted and ruled out
    /// (docs/segment-pruning.md).
    pub prune: crate::segment_prune::PruneStats,
    /// Global doc ids in final order.
    pub ids: Vec<u64>,
    /// Each row's sort keys in merge form, parallel to `ids` (empty
    /// rows unsorted).
    pub keys: Vec<Vec<crate::sortkeys::Key>>,
    /// Each row's reported sort values, parallel to `ids`.
    pub values: Vec<Vec<crate::sortkeys::Value>>,
    /// Whether a column order was applied.
    pub sorted: bool,
}

/// One exact distributed membership bitmap decoded into generation-local row IDs.
/// `epochs` is parallel to the coordinator's node order on every route. A
/// pruned filter shard (or a lexical request with no analyzed terms and no
/// mandatory view) has no read claim. Query-bound reads must match the admitted
/// read set before any bitmap enters the plan.
#[derive(Debug, Clone, Default)]
pub struct MembershipSet {
    pub ids: BTreeSet<u64>,
    pub epochs: Vec<StatsClaim>,
    pub wire_bytes: u64,
    pub terms: Vec<String>,
    /// Sealed segments the shards consulted and ruled out while
    /// resolving this set (docs/segment-pruning.md).
    pub prune: crate::segment_prune::PruneStats,
    pub(crate) ranges: Vec<(u64, u64)>,
}

/// Whose match set the percentile rounds count over: an id allowlist
/// (the pooled shapes and the public route), or the planned Boolean
/// tree the shards resolve again per round (`docs/query-api.md`).
#[derive(Clone, Copy)]
pub(crate) enum PercentileScope<'a> {
    Ids(Option<&'a [u64]>),
    Boolean(&'a [Option<crate::pb::BooleanShardRequest>]),
}

/// One planned `BooleanQuery` ready for the shards (`docs/query-api.md`,
/// "Recursive boolean execution"): the wire tree with every filter
/// leaf's full compiled tree, plus what the coordinator keeps beside
/// it to prune per shard and to apply the typo rules.
pub(crate) struct BooleanFanoutPlan {
    pub root: crate::pb::BooleanPlanGroup,
    pub leaves: Vec<crate::pb::BooleanPlanLeaf>,
    /// Parallel to `leaves`: the compiled filters of a filter leaf.
    pub filters: Vec<Option<RequestFilters>>,
    /// Leaf indices of the root group's MUST filter leaves, in order:
    /// the AND spine the placement tree prunes shards by.
    pub root_must_filters: Vec<usize>,
    /// Leaf indices of the lexical leaves a MUST/SHOULD chain reaches.
    pub positive_lexical: Vec<usize>,
    pub depth: u32,
    pub aggregate: Option<(crate::pb::BooleanShardAggregate, CompiledAggregate)>,
}

/// The merged answer of one Boolean fan-out.
pub(crate) struct BooleanFanout {
    /// Score descending, doc id ascending, at most `depth`.
    pub candidates: Vec<crate::pb::BooleanCandidate>,
    pub prune: crate::segment_prune::PruneStats,
    pub shards_total: u32,
    pub shards_skipped: u32,
    pub aggregate: Option<crate::pb::AggregateResponse>,
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
    /// Per shard, the request-tree indices of the leaves that shard's
    /// tree keeps, when its placement leaf implied some
    /// (`ShardMask::implied`); the shard's flags come back in that
    /// shorter order and are mapped through it. `None`: the full tree.
    kept: Vec<Option<Vec<usize>>>,
}

impl FilterKnown {
    fn new(filters: &RequestFilters) -> Self {
        let leaves = filters.tree.as_ref().map_or(0, crate::filter::leaf_count);
        Self {
            geo: vec![false; filters.geo.len()],
            tree: vec![false; leaves],
            leaves,
            kept: Vec::new(),
        }
    }

    /// Fold one shard's answer in, flags over the full request tree.
    fn merge(&mut self, geo: &[bool], tree: &[bool]) -> Result<(), Status> {
        self.merge_kept(geo, tree, None)
    }

    /// Fold `shard`'s answer in; its tree flags are over the tree that
    /// shard received, which [`Self::merge_pruned`] recorded.
    fn merge_shard(&mut self, shard: usize, geo: &[bool], tree: &[bool]) -> Result<(), Status> {
        let kept = self.kept.get(shard).and_then(|k| k.clone());
        self.merge_kept(geo, tree, kept.as_deref())
    }

    fn merge_kept(
        &mut self,
        geo: &[bool],
        tree: &[bool],
        kept: Option<&[usize]>,
    ) -> Result<(), Status> {
        if geo.len() != self.geo.len() {
            return Err(Status::internal(format!(
                "shard answered {} geo-column flags for {} filters",
                geo.len(),
                self.geo.len()
            )));
        }
        for (acc, k) in self.geo.iter_mut().zip(geo) {
            *acc |= *k;
        }
        match kept {
            Some(kept) if tree.len() == kept.len() => {
                for (&index, k) in kept.iter().zip(tree) {
                    if let Some(acc) = self.tree.get_mut(index) {
                        *acc |= *k;
                    }
                }
                Ok(())
            }
            Some(kept) => Err(Status::internal(format!(
                "shard answered {} filter-leaf flags for the {} leaves it was sent (of {})",
                tree.len(),
                kept.len(),
                self.leaves
            ))),
            None => {
                if tree.len() != self.leaves {
                    return Err(Status::internal(format!(
                        "shard answered {} filter-leaf flags for {} leaves",
                        tree.len(),
                        self.leaves
                    )));
                }
                for (acc, k) in self.tree.iter_mut().zip(tree) {
                    *acc |= *k;
                }
                Ok(())
            }
        }
    }

    /// Count the filter leaves that excluded a shard before fan-out as
    /// resolved (`docs/placement.md`): the leaf predicate that ruled the
    /// shard out named the same column, so the column is real.
    fn merge_pruned(&mut self, mask: Option<&crate::placement::ShardMask>) {
        if let Some(mask) = mask {
            mark_known(&mut self.tree, &mask.known);
            self.kept = mask
                .implied
                .iter()
                .map(|dropped| {
                    (!dropped.is_empty()).then(|| {
                        (0..self.leaves)
                            .filter(|index| !dropped.contains(index))
                            .collect()
                    })
                })
                .collect();
        }
    }

    /// Refuse a name NO shard resolved.
    fn refuse_unknown(&self, filters: &RequestFilters) -> Result<(), Status> {
        refuse_unknown_geo_columns(&filters.geo, &self.geo)?;
        refuse_unknown_filter_leaves(filters.tree.as_ref(), &self.tree)
    }
}

/// Mark filter leaves (indices in walk order) as resolved.
fn mark_known(flags: &mut [bool], leaves: &[usize]) {
    for &leaf in leaves {
        if let Some(flag) = flags.get_mut(leaf) {
            *flag = true;
        }
    }
}

/// Merged global stats for a fused multi-field query, with the per-node
/// epochs the shares were valid at (parallel to the node list).
/// Default expansion cap of a [`crate::pb::TermPrefix`] with
/// `max_expansions` unset (`docs/prefix-terms.md`).
pub const DEFAULT_PREFIX_EXPANSIONS: usize = 128;
/// The largest cap a request may ask for; every expansion is a scored
/// term, and past this a prefix is a scan, not a query.
pub const MAX_PREFIX_EXPANSIONS: usize = 1024;

/// Suggestions a `Suggest` request returns when `limit` is unset
/// (`docs/suggest.md`).
/// Candidates per term a did-you-mean request returns by default.
pub const DEFAULT_TERM_SUGGEST_LIMIT: usize = 5;
/// The largest edit bound a did-you-mean request may name.
pub const MAX_TERM_SUGGEST_EDITS: u32 = 2;
pub const DEFAULT_SUGGEST_LIMIT: usize = 10;
/// The most suggestions one request may ask for.
pub const MAX_SUGGEST_LIMIT: usize = 100;
/// Dictionary terms under the prefix a `Suggest` request may scan when
/// `max_scan` is unset, per shard and fleet-wide.
pub const DEFAULT_SUGGEST_SCAN: usize = 100_000;
/// The most dictionary terms one request may scan.
pub const MAX_SUGGEST_SCAN: usize = 1_000_000;

/// Query analysis for one field, or the empty analysis for a
/// prefix-only query (docs/prefix-terms.md). `cour*` has no text to
/// analyze — its terms are the expansions alone — so empty text with a
/// prefix present skips the analyzer rather than refusing. Empty text
/// with no prefixes keeps the analyzer's refusal: a query with no terms
/// and no prefixes has nothing to match, and saying so beats an empty
/// result that looks like a miss.
async fn analyze_query(
    addr: &str,
    text: &str,
    spec: Option<&crate::pb::AnalysisSpec>,
    prefixes: &[crate::pb::TermPrefix],
) -> Result<crate::postings::AnalyzedField, Status> {
    if text.is_empty() && !prefixes.is_empty() {
        return Ok(crate::postings::AnalyzedDoc::body(Vec::new(), 0).into_body());
    }
    Ok(crate::analyzer::analyze_document(addr, text, spec)
        .await?
        .into_body())
}

struct FusedGlobals {
    doc_count: u64,
    /// Per field: global sum of that field's document lengths.
    totals: Vec<u64>,
    /// Per field: global df per term, in that field's term order.
    dfs: Vec<Vec<u32>>,
    epochs: Vec<StatsClaim>,
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
            collection: String::new(),
            cursor_signer: Arc::new(crate::query_cursor::CursorSigner::default()),
            client_tls: None,
            udp_hmac_key: None,
            replica_addrs: Vec::new(),
            analysis_addr: None,
            bm25_params: Bm25Params::default(),
            limits: FanoutLimits::default(),
            stream_search: false,
            bm25_stream: false,
            knobs: Arc::new(crate::diagnostics::Knobs::coordinator(
                "coordinator",
                crate::diagnostics::CoordinatorKnobValues {
                    max_k: DEFAULT_MAX_K,
                    hedge_delay_ms: 0,
                    shard_pruning: true,
                    signal_batch: DEFAULT_SIGNAL_BATCH,
                },
                Vec::new(),
            )),
            max_rerank_bytes: DEFAULT_MAX_RERANK_BYTES,
            links: Arc::new(Mutex::new(HashMap::new())),
            allow_network: true,
            floor_socket: Arc::new(std::sync::OnceLock::new()),
            floor_targets: Arc::new(Mutex::new(HashMap::new())),
            stats_cache,
            document_visibility: None,
            field_permissions: None,
            query_read_versions: None,
            vector_read_field: None,
            #[cfg(feature = "net")]
            clustered_vectors: None,
            dense_quality_profile: None,
            synonyms: None,
            dense_execution_policy: None,
            topology_generation: 0,
            hash_ranges: Vec::new(),
            placement_codes: Vec::new(),
            placement: None,
            live_topology: None,
            topology_watch: Arc::new(watch::channel(0).0),
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
    /// A coordinator over nodes living in this process, reached through
    /// [`crate::link::NodeLink::Local`]: no network fallback, no UDP lane.
    pub fn with_local_nodes(nodes: Vec<Arc<crate::node::NodeServiceImpl>>) -> Self {
        let node_addrs: Vec<String> = (0..nodes.len())
            .map(|shard| format!("in-process://shard-{shard}"))
            .collect();
        let links = node_addrs
            .iter()
            .cloned()
            .zip(nodes.into_iter().map(crate::link::NodeLink::local))
            .collect::<HashMap<_, _>>();
        let mut coordinator = Self::new(node_addrs);
        coordinator.links = Arc::new(Mutex::new(links));
        coordinator.allow_network = false;
        coordinator
    }

    /// Bind an authority decision to a private execution clone. Every public
    /// route calls this before work; uncertified restricted routes refuse.
    pub(crate) fn for_access(
        &self,
        access: Option<&crate::pb::AccessDecision>,
        route: &str,
    ) -> Result<Self, Status> {
        let mut scoped = self.clone();
        let view = access.and_then(|a| a.document_visibility.as_ref());
        let fields = access.and_then(|a| a.field_permissions.as_ref());
        if view.is_none() && fields.is_none() {
            return Ok(scoped);
        }
        crate::visibility::VisibilityScope::new(view)?;
        scoped.field_permissions = fields
            .map(crate::field_permissions::FieldScope::new)
            .transpose()
            .map_err(|_| Status::permission_denied("invalid field permission decision"))?;
        if access.is_none_or(|a| a.action != crate::pb::AccessAction::Search as i32) {
            return Err(Status::permission_denied(
                "restricted grants require search authorization",
            ));
        }
        if !matches!(
            route,
            "query" | "query_stream" | "bm25_search" | "suggest" | "term_suggest" | "aggregate"
        ) {
            return Err(Status::permission_denied(
                "this route does not yet enforce document or field grants",
            ));
        }
        if self.allow_network
            || self.has_clustered_vectors()
            || self.live_topology.is_some()
            || self
                .links
                .lock()
                .map_err(|_| Status::internal("node link cache lock poisoned"))?
                .values()
                .any(|link| !link.is_local())
        {
            return Err(Status::failed_precondition(
                "restricted grants currently require private in-process shards",
            ));
        }
        scoped.document_visibility = view.cloned();
        Ok(scoped)
    }

    pub(crate) fn for_vector_field(&self, field: &str) -> Result<Self, Status> {
        if let Some(fields) = &self.field_permissions {
            fields.vector(field)?;
        }
        let mut scoped = self.clone();
        scoped.vector_read_field = (!field.is_empty()).then(|| field.to_string());
        Ok(scoped)
    }

    pub(crate) fn scoped_vector_scan(&self) -> bool {
        self.vector_read_field.is_some()
            || self.document_visibility.is_some()
            || self.field_permissions.is_some()
    }

    fn check_vector_scan(&self, filters: &RequestFilters, collapse: bool) -> Result<(), Status> {
        if let Some(fields) = &self.field_permissions {
            fields.vector(self.vector_read_field.as_deref().unwrap_or(""))?;
            fields.filter(&filters.geo, filters.tree.as_ref())?;
            if collapse {
                fields.dictionary("parent_id")?;
            }
        }
        if self.scoped_vector_scan() && self.has_clustered_vectors() {
            return Err(Status::failed_precondition(
                "scoped vector scans require product-node read receipts",
            ));
        }
        Ok(())
    }

    fn vector_read_barrier(
        &self,
    ) -> Result<Option<Arc<crate::vector_read::VectorReadBarrier>>, Status> {
        if !self.scoped_vector_scan() {
            return Ok(None);
        }
        let claims = self
            .query_read_versions
            .as_ref()
            .filter(|claims| claims.len() == self.node_addrs.len())
            .ok_or_else(|| {
                Status::failed_precondition("vector scans require an admitted physical read set")
            })?;
        crate::vector_read::VectorReadBarrier::new(
            self.vector_read_field.clone().unwrap_or_default(),
            self.document_visibility.clone(),
            claims.as_ref().clone(),
        )
        .map(Some)
    }

    fn vector_scan_mask(
        &self,
        filter: Option<&crate::pb::FilterExpr>,
    ) -> Option<crate::placement::ShardMask> {
        if self.scoped_vector_scan() {
            None
        } else {
            self.shard_mask(filter)
        }
    }

    fn visible_filter(
        &self,
        user: Option<crate::pb::FilterExpr>,
    ) -> Result<Option<crate::pb::FilterExpr>, Status> {
        crate::visibility::intersect_filter(self.document_visibility.as_ref(), user)
    }

    fn check_visibility_columns(&self, known: &[bool]) -> Result<(), Status> {
        if known.iter().any(|known| !known) {
            return Err(Status::failed_precondition(
                "document grant references a column unavailable in this collection",
            ));
        }
        Ok(())
    }

    /// True only for coordinators that may create network transports.
    pub fn allows_network(&self) -> bool {
        self.allow_network
    }

    /// Whether dense search runs on a clustered TurboVec backend rather
    /// than the shard fleet.
    fn has_clustered_vectors(&self) -> bool {
        #[cfg(feature = "net")]
        {
            self.clustered_vectors.is_some()
        }
        #[cfg(not(feature = "net"))]
        {
            false
        }
    }

    pub fn max_k(&self) -> u32 {
        self.knobs.max_k()
    }

    /// The term-stats cache, exposed for tests (`fetch_count` is how a
    /// test proves the hit path issued no RPCs).
    pub fn stats_cache(&self) -> &crate::stats_cache::StatsCache {
        &self.stats_cache
    }

    /// Enable atomic topology replacement. The current fields become the
    /// configured generation's immutable request snapshot.
    pub fn with_hot_topology(self, hash_ranges: Vec<Option<(u64, u64)>>) -> Result<Self, String> {
        self.with_hot_topology_placed(hash_ranges, None)
    }

    /// [`Self::with_hot_topology`] under a placement tree: one code per
    /// shard, parallel to the addresses (`docs/placement.md`).
    pub fn with_hot_topology_placed(
        mut self,
        hash_ranges: Vec<Option<(u64, u64)>>,
        placement: Option<(crate::placement::PlacementTreeConfig, Vec<Option<i64>>)>,
    ) -> Result<Self, String> {
        if hash_ranges.len() != self.node_addrs.len() {
            return Err(format!(
                "topology has {} shard addresses but {} hash ranges",
                self.node_addrs.len(),
                hash_ranges.len()
            ));
        }
        let (tree, codes) = match placement {
            Some((tree, codes)) => {
                if codes.len() != self.node_addrs.len() {
                    return Err(format!(
                        "topology has {} shard addresses but {} placement codes",
                        self.node_addrs.len(),
                        codes.len()
                    ));
                }
                (Some(tree), codes)
            }
            None => (None, vec![None; self.node_addrs.len()]),
        };
        let routes = self
            .node_addrs
            .iter()
            .enumerate()
            .map(|(shard, addr)| TopologyRoute {
                addr: addr.clone(),
                replica: self.replica_addrs.get(shard).cloned().flatten(),
                hash_range: hash_ranges.get(shard).copied().flatten(),
                placement: codes.get(shard).copied().flatten(),
            })
            .collect();
        let topology = build_topology(self.topology_generation, routes, tree.as_ref())?;
        self.hash_ranges = hash_ranges;
        self.placement_codes = codes;
        self.placement = topology.placement.clone();
        self.live_topology = Some(Arc::new(RwLock::new(Arc::new(topology))));
        self.topology_watch.send_replace(self.topology_generation);
        Ok(self)
    }

    /// Atomically publish a strictly newer topology generation. Existing
    /// requests retain their prior `Arc`; later requests snapshot this map.
    pub fn reload_topology(
        &self,
        generation: u64,
        routes: Vec<TopologyRoute>,
        placement: Option<&crate::placement::PlacementTreeConfig>,
    ) -> Result<(), String> {
        if self.cutover_pending.load(AtomicOrdering::Acquire) {
            return Err(
                "topology cutover has frozen writes; publish or abort it first".to_string(),
            );
        }
        self.publish_topology_inner(generation, routes, placement)
    }

    fn publish_topology_inner(
        &self,
        generation: u64,
        routes: Vec<TopologyRoute>,
        placement: Option<&crate::placement::PlacementTreeConfig>,
    ) -> Result<(), String> {
        let authority = self
            .live_topology
            .as_ref()
            .ok_or_else(|| "hot topology is not enabled".to_string())?;
        let replacement = Arc::new(build_topology(generation, routes, placement)?);
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
        drop(current);
        self.topology_watch.send_replace(generation);
        Ok(())
    }

    /// A receiver that changes whenever a new topology generation is
    /// published on this coordinator; its value is that generation.
    pub fn topology_changes(&self) -> watch::Receiver<u64> {
        self.topology_watch.subscribe()
    }

    /// A frozen clone of this coordinator over exactly `routes`: the
    /// links, keys, limits, and TLS of this one, the shard set of the
    /// caller's map snapshot, no live topology. A relay fans out through
    /// this so the children it talks to are the children its pinned map
    /// revision names, whatever source that map came from.
    pub(crate) fn frozen_over(
        &self,
        generation: u64,
        routes: &[TopologyRoute],
        placement: Option<Arc<crate::placement::Placement>>,
    ) -> Self {
        let mut frozen = self.clone();
        frozen.node_addrs = routes.iter().map(|route| route.addr.clone()).collect();
        frozen.replica_addrs = routes.iter().map(|route| route.replica.clone()).collect();
        frozen.hash_ranges = routes.iter().map(|route| route.hash_range).collect();
        frozen.placement_codes = routes.iter().map(|route| route.placement).collect();
        frozen.topology_generation = generation;
        frozen.placement = placement;
        frozen.stats_cache = Arc::new(crate::stats_cache::StatsCache::new(routes.len()));
        frozen.live_topology = None;
        frozen
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
                placement: self.placement_codes.get(shard).copied().flatten(),
            })
            .collect()
    }

    /// The placement tree of the current generation, if the map has one.
    pub fn current_placement(&self) -> Option<Arc<crate::placement::Placement>> {
        if let Some(authority) = &self.live_topology {
            return authority
                .read()
                .expect("topology authority lock poisoned")
                .placement
                .clone();
        }
        self.placement.clone()
    }

    /// Resolve an opaque stable product identity under one immutable map.
    /// Returns `(generation, shard_index)` for ingest stamping.
    pub fn route_stable_key(&self, key: &[u8]) -> Result<(u64, usize), String> {
        self.route_stable_key_in(key, None)
    }

    /// [`Self::route_stable_key`] inside one placement leaf: under a
    /// tree the hash ranges tile the space per leaf, so the leaf's code
    /// is part of the route. A placed topology refuses a leafless route
    /// and an unplaced one refuses a leaf.
    pub fn route_stable_key_in(
        &self,
        key: &[u8],
        leaf: Option<i64>,
    ) -> Result<(u64, usize), String> {
        if key.is_empty() {
            return Err("stable routing key is empty".to_string());
        }
        let hash = stable_routing_hash(key);
        let (generation, ranges, codes, placed): RoutingView =
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
                    topology
                        .routes
                        .iter()
                        .map(|route| route.placement)
                        .collect(),
                    topology.placement.is_some(),
                )
            } else {
                (
                    self.topology_generation,
                    self.hash_ranges.clone(),
                    self.placement_codes.clone(),
                    self.placement.is_some(),
                )
            };
        if ranges.is_empty() || ranges.iter().any(Option::is_none) {
            return Err("topology has no complete stable hash ranges".to_string());
        }
        match (placed, leaf) {
            (true, None) => {
                return Err(
                    "the topology has a placement tree; a stable key routes inside a leaf"
                        .to_string(),
                )
            }
            (false, Some(code)) => {
                return Err(format!(
                    "placement code {code} given, but the topology has no placement tree"
                ))
            }
            _ => {}
        }
        let shard = ranges
            .iter()
            .enumerate()
            .position(|(shard, range)| {
                leaf.is_none_or(|code| codes.get(shard).copied().flatten() == Some(code))
                    && range.is_some_and(|(lo, hi)| hash >= lo && hash <= hi)
            })
            .ok_or_else(|| match leaf {
                Some(code) => {
                    format!("stable hash {hash} is not covered inside placement leaf {code}")
                }
                None => format!("stable hash {hash} is not covered by the topology"),
            })?;
        Ok((generation, shard))
    }

    /// The shards `filter` cannot match under this generation's placement
    /// tree (`docs/placement.md`), or `None` when there is no tree, no
    /// filter, or the `shard_pruning` knob is off. Every fan-out that
    /// carries a filter asks once and skips the masked shards.
    pub(crate) fn shard_mask(
        &self,
        filter: Option<&crate::pb::FilterExpr>,
    ) -> Option<crate::placement::ShardMask> {
        let placement = self.placement.as_ref()?;
        let filter = filter?;
        if !self.knobs.shard_pruning() {
            return None;
        }
        Some(crate::placement::ShardMask::compute(
            placement,
            &self.placement_codes,
            filter,
        ))
    }

    /// The filter tree to send to `shard`: the request's tree with the
    /// clauses the shard's placement leaf implies removed
    /// (`docs/placement.md`, "Implied clauses"); the tree as it is with
    /// no tree, no mask, or nothing implied. Same answer either way, one
    /// bitmap less to resolve on the shard.
    pub(crate) fn shard_filter_tree(
        filters: &RequestFilters,
        mask: Option<&crate::placement::ShardMask>,
        shard: usize,
    ) -> Option<crate::pb::FilterExpr> {
        match (filters.tree.as_ref(), mask) {
            (Some(tree), Some(mask)) => mask.filter_for(shard, tree),
            (tree, _) => tree.cloned(),
        }
    }

    /// [`Self::shard_filter_tree`] for every shard of the topology, for
    /// a context that outlives the mask (a stream and its hedge leg must
    /// send one shard the identical tree).
    fn shard_filter_trees(
        &self,
        filters: &RequestFilters,
        mask: Option<&crate::placement::ShardMask>,
    ) -> Vec<Option<crate::pb::FilterExpr>> {
        (0..self.node_addrs.len())
            .map(|shard| Self::shard_filter_tree(filters, mask, shard))
            .collect()
    }

    /// `(shards in the topology, shards the filter skips)` for a profile.
    pub fn shard_prune_counts(&self, filters: &RequestFilters) -> (u32, u32) {
        let total = self.node_addrs.len() as u32;
        let skipped = self
            .shard_mask(filters.tree.as_ref())
            .map_or(0, |mask| mask.skipped_count());
        (total, skipped)
    }

    /// The known-column accumulator for one fan-out, with the leaves a
    /// mask already resolved marked.
    fn filter_known(
        filters: &RequestFilters,
        mask: Option<&crate::placement::ShardMask>,
    ) -> FilterKnown {
        let mut known = FilterKnown::new(filters);
        known.merge_pruned(mask);
        known
    }

    pub(crate) fn request_snapshot(&self) -> Option<Self> {
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
        frozen.placement_codes = topology
            .routes
            .iter()
            .map(|route| route.placement)
            .collect();
        frozen.placement = topology.placement.clone();
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
        self.query_progress = Some(Arc::new(QueryProgressPublisher {
            sender: progress,
            reader: std::sync::OnceLock::new(),
        }));
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
        progress.sender.send_if_modified(|current| {
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
        self.rebuild_knobs(
            max_k,
            self.knobs.hedge_delay(),
            self.knobs.shard_pruning(),
            self.knobs.signal_batch(),
        );
        self.refresh_fixed_knobs();
        self
    }

    /// Candidate ids per rescore call for a boolean group's clauses
    /// ([`DEFAULT_SIGNAL_BATCH`]). Zero is rejected at config parse
    /// time. Live afterwards as the `signal_batch` knob; `max_k` is the
    /// earlier behavior for an A/B.
    pub fn with_signal_batch(mut self, batch: u32) -> Self {
        self.rebuild_knobs(
            self.knobs.max_k(),
            self.knobs.hedge_delay(),
            self.knobs.shard_pruning(),
            batch,
        );
        self.refresh_fixed_knobs();
        self
    }

    /// Ids per rescore call, live.
    pub fn signal_batch(&self) -> usize {
        self.knobs.signal_batch().max(1) as usize
    }

    /// Whether a filtered request skips the shards its placement leaf
    /// rules out (`docs/placement.md`); `false` is the A/B switch and
    /// changes no answer. Live afterwards as the `shard_pruning` knob.
    pub fn with_shard_pruning(mut self, enabled: bool) -> Self {
        self.rebuild_knobs(
            self.knobs.max_k(),
            self.knobs.hedge_delay(),
            enabled,
            self.knobs.signal_batch(),
        );
        self.refresh_fixed_knobs();
        self
    }

    /// Builders run before the coordinator is shared, so a knob change
    /// at build time replaces the set; the live values carry over.
    fn rebuild_knobs(
        &mut self,
        max_k: u32,
        hedge_delay: Option<Duration>,
        shard_pruning: bool,
        signal_batch: u32,
    ) {
        self.knobs = Arc::new(crate::diagnostics::Knobs::coordinator(
            self.knobs.process().to_string(),
            crate::diagnostics::CoordinatorKnobValues {
                max_k,
                hedge_delay_ms: hedge_delay.map_or(0, |d| d.as_millis() as u64),
                shard_pruning,
                signal_batch,
            },
            Vec::new(),
        ));
    }

    /// The read-at-startup settings listed beside the live knobs.
    fn refresh_fixed_knobs(&self) {
        use crate::diagnostics::FixedKnob;
        use crate::pb::KnobKind;
        self.knobs.set_fixed(vec![
            FixedKnob {
                name: "collection",
                kind: KnobKind::String,
                value: self.collection.clone(),
                description: "The collection this coordinator serves.",
            },
            FixedKnob {
                name: "nodes",
                kind: KnobKind::Int,
                value: self.node_addrs.len().to_string(),
                description: "Shard nodes in the construction-time shard map.",
            },
            FixedKnob {
                name: "replicas",
                kind: KnobKind::Int,
                value: self
                    .replica_addrs
                    .iter()
                    .filter(|r| r.is_some())
                    .count()
                    .to_string(),
                description: "Shards with a replica configured.",
            },
            FixedKnob {
                name: "stream_search",
                kind: KnobKind::Bool,
                value: self.stream_search.to_string(),
                description: "Vector legs over the streaming shard search (--stream-search).",
            },
            FixedKnob {
                name: "bm25_stream",
                kind: KnobKind::Bool,
                value: self.bm25_stream.to_string(),
                description: "Lexical legs over the streaming BM25 query (--bm25-stream).",
            },
            FixedKnob {
                name: "max_rerank_bytes",
                kind: KnobKind::Int,
                value: self.max_rerank_bytes.to_string(),
                description: "Largest exact-rerank pool in FP32 bytes (--max-rerank-bytes).",
            },
            FixedKnob {
                name: "shard_deadline_ms",
                kind: KnobKind::Int,
                value: self
                    .limits
                    .shard_deadline
                    .map_or(0, |d| d.as_millis() as u64)
                    .to_string(),
                description: "Bound on one shard's attempt; 0 is none (--shard-deadline-ms).",
            },
            FixedKnob {
                name: "dense_execution_policy",
                kind: KnobKind::Bool,
                value: self.dense_execution_policy.is_some().to_string(),
                description: "A dense execution policy is installed (--dense-execution-policy).",
            },
        ]);
    }

    /// The knobs this coordinator reads at request time.
    pub fn knobs(&self) -> &Arc<crate::diagnostics::Knobs> {
        &self.knobs
    }

    /// Fan-out limits with the live hedge delay.
    fn limits(&self) -> FanoutLimits {
        FanoutLimits {
            shard_deadline: self.limits.shard_deadline,
            hedge_delay: self.knobs.hedge_delay(),
        }
    }

    /// Layouts of this coordinator's shard nodes (docs/diagnostics.md):
    /// in-process nodes answer directly, remote ones through their own
    /// diagnostics service; a node without it is listed with the status
    /// it returned in `layout`.
    pub async fn shard_diagnostics(
        &self,
        only: Option<u32>,
    ) -> Vec<crate::pb::ShardLayoutDiagnostics> {
        let mut out = Vec::new();
        for (shard, addr) in self.node_addrs.iter().enumerate() {
            let shard = shard as u32;
            if only.is_some_and(|s| s != shard) {
                continue;
            }
            let layout = match self.node_client(addr) {
                Ok(crate::link::NodeLink::Local(node)) => {
                    node.shard_diagnostics(shard, addr.clone())
                }
                #[cfg(feature = "net")]
                Ok(crate::link::NodeLink::Remote(_)) => match self.connect(addr) {
                    Ok(channel) => {
                        let mut client =
                            crate::pb::diagnostics_service_client::DiagnosticsServiceClient::new(
                                channel,
                            );
                        match tokio::time::timeout(
                            Duration::from_secs(5),
                            client.get_shard_diagnostics(crate::pb::ShardDiagnosticsRequest {
                                shard: None,
                            }),
                        )
                        .await
                        {
                            Ok(Ok(reply)) => match reply.into_inner().shards.into_iter().next() {
                                Some(mut layout) => {
                                    layout.shard = shard;
                                    layout.address = addr.clone();
                                    layout
                                }
                                None => crate::diagnostics::unserved_layout(
                                    shard,
                                    addr.clone(),
                                    "EMPTY: the node reported no shard",
                                ),
                            },
                            Ok(Err(status)) => crate::diagnostics::unserved_layout(
                                shard,
                                addr.clone(),
                                &format!("{:?}: {}", status.code(), status.message()),
                            ),
                            Err(_) => crate::diagnostics::unserved_layout(
                                shard,
                                addr.clone(),
                                "DEADLINE_EXCEEDED: diagnostics probe timed out",
                            ),
                        }
                    }
                    Err(status) => crate::diagnostics::unserved_layout(
                        shard,
                        addr.clone(),
                        &format!("{:?}: {}", status.code(), status.message()),
                    ),
                },
                Err(status) => crate::diagnostics::unserved_layout(
                    shard,
                    addr.clone(),
                    &format!("{:?}: {}", status.code(), status.message()),
                ),
            };
            out.push(layout);
        }
        out
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
            return Ok(self.knobs.max_k());
        }
        if requested > self.knobs.max_k() {
            return Err(Status::invalid_argument(format!(
                "k={requested} exceeds this coordinator's max_k={}; \
                 lower k or raise --max-k",
                self.knobs.max_k()
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
        self.rebuild_knobs(
            self.knobs.max_k(),
            limits.hedge_delay,
            self.knobs.shard_pruning(),
            self.knobs.signal_batch(),
        );
        self.refresh_fixed_knobs();
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
    #[cfg(feature = "net")]
    pub fn with_clustered_turbovec(mut self, backend: ClusteredTurboVecBackend) -> Self {
        self.clustered_vectors = Some(backend);
        self
    }

    /// Install the synonym table (`docs/synonyms.md`).
    pub fn with_synonyms(mut self, table: crate::synonyms::SynonymTable) -> Self {
        self.synonyms = Some(Arc::new(table));
        self
    }

    /// Expand one field's analyzed query terms under the table (unless
    /// `off`) and the request's rules, through the coordinator's
    /// analysis backend; the added terms are appended to `terms`.
    async fn expand_synonyms(
        &self,
        field: &str,
        spec: Option<&crate::pb::AnalysisSpec>,
        rules: &[crate::pb::SynonymRule],
        off: bool,
        terms: &mut Vec<String>,
    ) -> Result<Vec<crate::pb::SynonymExpansion>, Status> {
        if rules.is_empty() && (off || self.synonyms.as_ref().is_none_or(|t| t.is_empty())) {
            return Ok(Vec::new());
        }
        let addr = self.analysis_addr.clone().ok_or_else(|| {
            Status::unavailable("no analysis backend configured on the coordinator (analysis_addr)")
        })?;
        let analyze = move |text: String, spec: Option<crate::pb::AnalysisSpec>| {
            let addr = addr.clone();
            async move {
                let analyzed =
                    crate::analyzer::analyze_document(&addr, &text, spec.as_ref()).await?;
                let mut out: Vec<String> = Vec::new();
                for (term, _, _) in analyzed.into_body().terms {
                    if !out.contains(&term) {
                        out.push(term);
                    }
                }
                Ok(out)
            }
        };
        crate::synonyms::expand(
            self.synonyms.as_deref(),
            off,
            rules,
            field,
            spec,
            terms,
            analyze,
        )
        .await
    }

    pub fn with_dense_quality_profile(
        mut self,
        profile: crate::quality::DenseQualityProfile,
    ) -> Self {
        self.dense_quality_profile = Some(Arc::new(profile));
        self
    }

    pub fn with_dense_execution_policy(
        mut self,
        policy: crate::dense_policy::DenseExecutionPolicy,
    ) -> Self {
        self.dense_execution_policy = Some(Arc::new(policy));
        self
    }

    /// One provider identity across the shard fleet, or a refusal that
    /// names the mismatch (`docs/mmap-vectors.md`): every reachable shard
    /// must advertise the same backend kind, scoring fingerprint, and
    /// dimension before the coordinator scores anything across them.
    /// With `require_configured`, a shard without a vector backend is a
    /// refusal too; without it (ingest, health), unconfigured shards are
    /// skipped and an all-unconfigured fleet is an empty identity.
    pub(crate) async fn fleet_vector_identity(
        &self,
        require_configured: bool,
    ) -> Result<FleetVectorIdentity, Status> {
        let mut tasks = Vec::with_capacity(self.node_addrs.len());
        for (shard, addr) in self.node_addrs.iter().enumerate() {
            let mut client = self.node_client(addr)?;
            let addr = addr.clone();
            tasks.push(tokio::spawn(async move {
                client
                    .get_vector_backend(crate::pb::GetVectorBackendRequest {})
                    .await
                    .map(|response| (shard, addr, response.into_inner()))
            }));
        }
        let mut identity: Option<FleetVectorIdentity> = None;
        let mut first: Option<(usize, String)> = None;
        for task in tasks {
            let (shard, addr, backend) = task.await.map_err(|error| {
                Status::internal(format!("provider preflight failed: {error}"))
            })??;
            let Some(descriptor) = backend.descriptor else {
                if require_configured {
                    return Err(Status::failed_precondition(format!(
                        "provider preflight: shard {shard} ({addr}) has no vector backend \
                         configured"
                    )));
                }
                continue;
            };
            let seen = FleetVectorIdentity {
                provider: descriptor.backend_kind.clone(),
                scoring_fingerprint: descriptor.scoring_fingerprint.clone(),
                dimensions: descriptor.dim,
                rows: backend.num_vectors,
            };
            match &mut identity {
                None => {
                    identity = Some(seen);
                    first = Some((shard, addr));
                }
                Some(held) => {
                    let (first_shard, first_addr) =
                        first.as_ref().expect("identity and first are set together");
                    if held.provider != seen.provider
                        || held.scoring_fingerprint != seen.scoring_fingerprint
                        || held.dimensions != seen.dimensions
                    {
                        return Err(Status::failed_precondition(format!(
                            "provider preflight: shard {first_shard} ({first_addr}) scores \
                             under {}/{} dim {}, but shard {shard} ({addr}) under {}/{} dim {}; \
                             a fleet scores in one space, so nothing is searched until every \
                             shard serves the same provider state",
                            held.provider,
                            held.scoring_fingerprint,
                            held.dimensions,
                            seen.provider,
                            seen.scoring_fingerprint,
                            seen.dimensions
                        )));
                    }
                    held.rows = held
                        .rows
                        .checked_add(seen.rows)
                        .ok_or_else(|| Status::internal("provider preflight row count overflow"))?;
                }
            }
        }
        Ok(identity.unwrap_or_default())
    }

    /// The clustered backend's live identity, when one is configured.
    #[cfg(feature = "net")]
    async fn clustered_quality_identity(
        &self,
    ) -> Result<Option<crate::clustered_turbovec::ClusteredQualityIdentity>, Status> {
        match &self.clustered_vectors {
            Some(clustered) => clustered.quality_identity().await.map(Some),
            None => Ok(None),
        }
    }

    #[cfg(not(feature = "net"))]
    async fn clustered_quality_identity(
        &self,
    ) -> Result<Option<crate::clustered_turbovec::ClusteredQualityIdentity>, Status> {
        Ok(None)
    }

    pub fn with_topology_generation(mut self, generation: u64) -> Self {
        self.topology_generation = generation;
        self
    }

    /// Supply a host-managed cursor signing key. Hosts sharing a key still need
    /// identical request, authorization and routing contexts to resume a token.
    /// Without this option the key is random and lost when the coordinator drops.
    pub fn with_cursor_signing_key(mut self, key: [u8; 32]) -> Self {
        self.cursor_signer = Arc::new(crate::query_cursor::CursorSigner::from_key(key));
        self
    }

    /// Read fresh versions without using the statistics cache. A version-only
    /// TermStats request is supported by nodes and relays, including shards
    /// without lexical rows. Its version covers the shard's physical rows.
    async fn read_query_versions(
        &self,
        allow_replicas: bool,
    ) -> Result<Vec<(String, StatsClaim)>, Status> {
        let scope = crate::visibility::VisibilityScope::new(self.document_visibility.as_ref())?;
        let mut known = vec![false; scope.column_count()];
        let mut tasks = tokio::task::JoinSet::new();
        for (shard, node) in self.node_addrs.iter().enumerate() {
            let primary = (node.clone(), self.node_client(node)?);
            let replica = if allow_replicas {
                self.replica_addrs
                    .get(shard)
                    .and_then(Option::as_ref)
                    .map(|addr| self.node_client(addr).map(|link| (addr.clone(), link)))
                    .transpose()?
            } else {
                None
            };
            let visibility = self.document_visibility.clone();
            let limits = self.limits();
            tasks.spawn(async move {
                async fn read(
                    (address, mut client): (String, crate::link::NodeLink),
                    visibility: Option<crate::pb::DocumentVisibility>,
                ) -> Result<(String, crate::pb::TermStatsResponse), Status> {
                    let response = client.term_stats(TermStatsRequest {
                        version_only: true,
                        terms: Vec::new(), fields: Vec::new(), visibility,
                    }).await?.into_inner();
                    Ok((address, response))
                }
                let attempt = async move {
                    let mut primary = Box::pin(read(primary, visibility.clone()));
                    let Some(replica) = replica else { return primary.await; };
                    if let Some(delay) = limits.hedge_delay {
                        match tokio::time::timeout(delay, &mut primary).await {
                            Ok(Ok(response)) => return Ok(response),
                            Ok(Err(_)) => return read(replica, visibility).await,
                            Err(_) => {}
                        }
                        let mut replica = Box::pin(read(replica, visibility));
                        tokio::select! {
                            result = &mut primary => match result { Ok(response) => Ok(response), Err(_) => replica.await },
                            result = &mut replica => match result { Ok(response) => Ok(response), Err(_) => primary.await },
                        }
                    } else {
                        match primary.await {
                            Ok(response) => Ok(response),
                            Err(_) => read(replica, visibility).await,
                        }
                    }
                };
                let result = match limits.shard_deadline {
                    Some(deadline) => tokio::time::timeout(deadline, attempt).await
                        .unwrap_or_else(|_| Err(Status::deadline_exceeded("query version probe exceeded the shard deadline"))),
                    None => attempt.await,
                };
                (shard, result)
            });
        }
        let mut versions = vec![None; self.node_addrs.len()];
        while let Some(task) = tasks.join_next().await {
            let (shard, response) = task
                .map_err(|error| Status::internal(format!("query version task failed: {error}")))?;
            let (address, response) = response?;
            scope.validate_response(&response)?;
            crate::visibility::validate_stats_mode(true, &response)?;
            for (known, present) in known.iter_mut().zip(response.visibility_columns_known) {
                *known |= present;
            }
            versions[shard] = Some((
                address,
                StatsClaim::required(response.stats_epoch, &response.stats_incarnation)?,
            ));
        }
        self.check_visibility_columns(&known)?;
        Ok(versions
            .into_iter()
            .map(|version| version.expect("every version task completed"))
            .collect())
    }

    pub(crate) fn admitted_read_claims(&self) -> Result<Vec<StatsClaim>, Status> {
        self.query_read_versions
            .as_ref()
            .map(|claims| claims.as_ref().clone())
            .ok_or_else(|| Status::failed_precondition("query has no admitted physical read set"))
    }

    async fn pin_read_versions(&self) -> Result<(Self, Vec<(String, StatsClaim)>), Status> {
        let reads = self.read_query_versions(true).await?;
        let mut pinned = self.clone();
        pinned.node_addrs = reads.iter().map(|(address, _)| address.clone()).collect();
        pinned.replica_addrs.clear();
        pinned.query_read_versions =
            Some(Arc::new(reads.iter().map(|(_, claim)| *claim).collect()));
        Ok((pinned, reads))
    }

    async fn validate_read_versions(&self, reads: &[(String, StatsClaim)]) -> Result<(), Status> {
        if self.read_query_versions(false).await? != reads {
            return Err(Status::failed_precondition(
                "query data changed during execution; restart from the first page",
            ));
        }
        Ok(())
    }

    /// Capture before selection and validate after every phase. Every shard's
    /// unchanged interval includes the interval between the two fan-outs;
    /// mutations or lifetime replacement invalidate the response, never retry
    /// just its value fetch against a newer version.
    async fn execute_query(
        &self,
        mut request: crate::pb::QueryRequest,
        access: Option<&crate::pb::AccessDecision>,
    ) -> Result<crate::pb::QueryResponse, Status> {
        let disclosure = self
            .field_permissions
            .as_ref()
            .map(|fields| fields.query(&request))
            .transpose()?;
        let started = std::time::Instant::now();
        let mut cursor = self.bind_query_cursor(&mut request, access)?;
        let (scoped, reads) = self.pin_read_versions().await?;
        cursor.bind_read_versions(
            scoped
                .query_read_versions
                .as_ref()
                .expect("pinned read versions"),
        )?;
        if let Some(publisher) = &self.query_progress {
            let mut reader = scoped.clone();
            reader.query_progress = None;
            publisher
                .reader
                .set(Arc::new(reader))
                .map_err(|_| Status::internal("query stream read context was initialized twice"))?;
        }
        let result = async {
            let mut response = crate::query::execute(&scoped, request).await?;
            if self
                .field_permissions
                .as_ref()
                .is_none_or(|fields| fields.can_disclose_identity())
            {
                scoped.fill_query_identities(&mut response).await?;
            }
            Ok::<_, Status>(response)
        }
        .await;
        scoped.validate_read_versions(&reads).await?;
        let mut response = result?;
        if let Some(disclosure) = disclosure {
            disclosure.apply(&mut response);
        }
        if self.document_visibility.is_some() {
            crate::query_disclosure::redact_execution(&mut response);
        }
        cursor.finish(&mut response)?;
        response.served_topology_generation = scoped.topology_generation;
        if let Some(profile) = response.profile.as_mut() {
            profile.total_ms = started.elapsed().as_secs_f32() * 1000.0;
        }
        Ok(response)
    }

    async fn fill_query_identities(
        &self,
        response: &mut crate::pb::QueryResponse,
    ) -> Result<(), Status> {
        let ids: Vec<_> = response
            .hits
            .iter()
            .chain(response.groups.iter().flat_map(|group| &group.hits))
            .map(|hit| hit.doc_id)
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        if ids.is_empty() {
            return Ok(());
        }
        let claims = self.admitted_read_claims()?;
        let identities = self.resolve_candidate_identities_at(&ids, &claims).await?;
        if identities.len() != ids.len() {
            return Err(Status::failed_precondition(
                "query identity fetch omitted a selected row",
            ));
        }
        for hit in response
            .hits
            .iter_mut()
            .chain(response.groups.iter_mut().flat_map(|group| &mut group.hits))
        {
            let identity = identities
                .get(&hit.doc_id)
                .expect("complete candidate identity set");
            if hit.identity.is_some() && &hit.identity != identity {
                return Err(Status::failed_precondition(
                    "query identity differs from the selection identity",
                ));
            }
            hit.identity = identity.clone();
        }
        Ok(())
    }

    async fn resolve_query_progress(
        &self,
        progress: QueryProgress,
        revision: u64,
    ) -> Result<crate::pb::QueryStreamRevision, Status> {
        let reader = self
            .query_progress
            .as_ref()
            .and_then(|publisher| publisher.reader.get())
            .ok_or_else(|| {
                Status::failed_precondition("query progress has no admitted read context")
            })?;
        let claims = reader.admitted_read_claims()?;
        let disclose = reader
            .field_permissions
            .as_ref()
            .is_none_or(|fields| fields.can_disclose_identity());
        let ids: Vec<_> = progress.hits.iter().map(|hit| hit.0).collect();
        let identities = if disclose {
            reader.resolve_candidate_identities_at(&ids, &claims).await
        } else {
            Ok(HashMap::new())
        };
        let reads: Vec<_> = reader.node_addrs.iter().cloned().zip(claims).collect();
        reader.validate_read_versions(&reads).await?;
        let identities = identities?;
        if disclose
            && (identities.len() != ids.len() || ids.iter().any(|id| !identities.contains_key(id)))
        {
            return Err(Status::failed_precondition(
                "stream identity fetch omitted a candidate",
            ));
        }
        let hits = progress
            .hits
            .into_iter()
            .map(|(id, score)| (id, score, identities.get(&id).cloned().flatten()))
            .collect();
        Ok(query_stream_revision(
            revision,
            progress.phase,
            hits,
            progress.scoring_fingerprint,
            if disclose {
                crate::pb::QueryStreamIdentityState::Resolved
            } else {
                crate::pb::QueryStreamIdentityState::Withheld
            },
        ))
    }

    async fn query_stream_attempt(
        &self,
        query: crate::pb::QueryRequest,
        access: Option<crate::pb::AccessDecision>,
        mut progress_rx: watch::Receiver<Option<QueryProgress>>,
        tx: &mpsc::Sender<Result<crate::pb::QueryStreamResponse, Status>>,
        revision: &mut u64,
        scoring_fingerprints: &mut Vec<String>,
        request_fingerprint: &str,
    ) -> Result<crate::pb::QueryResponse, Status> {
        let runner = self.clone();
        let mut execution = tokio::task::JoinSet::new();
        execution.spawn(async move { runner.execute_query(query, access.as_ref()).await });
        let mut last_content_fingerprint: Option<String> = None;
        let mut progress_open = true;
        loop {
            tokio::select! {
                changed = progress_rx.changed(), if progress_open => {
                    if changed.is_err() { progress_open = false; continue; }
                    let Some(progress) = progress_rx.borrow_and_update().clone() else { continue; };
                    if !progress.scoring_fingerprint.is_empty() && !scoring_fingerprints.contains(&progress.scoring_fingerprint) {
                        scoring_fingerprints.push(progress.scoring_fingerprint.clone());
                    }
                    let snapshot = self.resolve_query_progress(progress, *revision + 1).await?;
                    if last_content_fingerprint.as_ref() == Some(&snapshot.content_fingerprint) { continue; }
                    let fingerprint = snapshot.content_fingerprint.clone();
                    let event = crate::pb::QueryStreamResponse {
                        payload: Some(crate::pb::query_stream_response::Payload::Revision(snapshot)),
                    };
                    match tx.try_send(Ok(event)) {
                        Ok(()) => { *revision += 1; last_content_fingerprint = Some(fingerprint); }
                        Err(mpsc::error::TrySendError::Full(_)) => {},
                        Err(mpsc::error::TrySendError::Closed(_)) => return Err(Status::cancelled("query stream closed")),
                    }
                }
                joined = execution.join_next() => {
                    let mut response = joined.ok_or_else(|| Status::internal("query execution task disappeared"))?
                        .map_err(|_| Status::internal("query execution task failed"))??;
                    // Preserve the last collector order even when execution and
                    // progress become ready together. Identity reads still use
                    // the original admitted context, never a fresh generation.
                    let pending = progress_rx.borrow().clone();
                    if let Some(progress) = pending {
                        if !progress.scoring_fingerprint.is_empty() && !scoring_fingerprints.contains(&progress.scoring_fingerprint) {
                            scoring_fingerprints.push(progress.scoring_fingerprint.clone());
                        }
                        let snapshot = self.resolve_query_progress(progress, *revision + 1).await?;
                        if last_content_fingerprint.as_ref() != Some(&snapshot.content_fingerprint) {
                            tx.send(Ok(crate::pb::QueryStreamResponse {
                                payload: Some(crate::pb::query_stream_response::Payload::Revision(snapshot)),
                            })).await.map_err(|_| Status::cancelled("query stream closed"))?;
                            *revision += 1;
                        }
                    }
                    response.served_topology_generation = self.topology_generation;
                    scoring_fingerprints.sort(); scoring_fingerprints.dedup();
                    let final_scoring = combined_scoring_fingerprint(scoring_fingerprints, request_fingerprint);
                    let final_hits = response.hits.iter().map(|hit| (hit.doc_id, hit.score, hit.identity.clone())).collect();
                    let state = if self.field_permissions.as_ref().is_some_and(|fields| !fields.can_disclose_identity()) {
                        crate::pb::QueryStreamIdentityState::Withheld
                    } else { crate::pb::QueryStreamIdentityState::Resolved };
                    let snapshot = query_stream_revision(*revision + 1, crate::pb::QueryStreamPhase::Final,
                        final_hits, final_scoring, state);
                    tx.send(Ok(crate::pb::QueryStreamResponse {
                        payload: Some(crate::pb::query_stream_response::Payload::Revision(snapshot)),
                    })).await.map_err(|_| Status::cancelled("query stream closed"))?;
                    *revision += 1;
                    return Ok(response);
                }
            }
        }
    }

    fn bind_query_cursor(
        &self,
        request: &mut crate::pb::QueryRequest,
        access: Option<&crate::pb::AccessDecision>,
    ) -> Result<crate::query_cursor::CursorBinding, Status> {
        self.admit(&request.collection)?;
        let routes = self
            .current_topology_routes()
            .into_iter()
            .map(|route| crate::pb::QueryCursorRoute {
                address: route.addr,
                replica: route.replica,
                hash_start: route.hash_range.map(|range| range.0),
                hash_end: route.hash_range.map(|range| range.1),
                placement: route.placement,
            })
            .collect();
        crate::query_cursor::CursorBinding::prepare(
            self.cursor_signer.clone(),
            request,
            crate::pb::QueryCursorContext {
                query_sha256: Vec::new(),
                collection: self.collection.clone(),
                access: access.cloned(),
                topology_generation: self.topology_generation,
                routes,
            },
        )
    }

    /// Name the collection this coordinator serves (`docs/collections.md`).
    pub fn with_collection(mut self, name: &str) -> Self {
        self.collection = name.to_string();
        self
    }

    /// The collection this coordinator serves; empty for the unnamed
    /// single dataset.
    pub fn collection(&self) -> &str {
        &self.collection
    }

    /// TLS material for every channel this coordinator opens to a shard
    /// (`docs/security.md`).
    pub fn with_client_tls(mut self, tls: crate::security::ClientTls) -> Self {
        self.client_tls = Some(tls);
        self
    }

    /// The key that signs this coordinator's UDP floor and cancel
    /// datagrams (`docs/security.md`).
    pub fn with_udp_hmac_key(mut self, key: crate::security::UdpKey) -> Self {
        self.udp_hmac_key = Some(key);
        self
    }

    /// The key that signs this coordinator's UDP datagrams, when one is
    /// configured (`docs/security.md`); a relay signs its own parent-facing
    /// lane with the same key.
    pub(crate) fn udp_key(&self) -> Option<&crate::security::UdpKey> {
        self.udp_hmac_key.as_ref()
    }

    /// The shard addresses this coordinator fans out to, in shard order.
    pub fn node_addresses(&self) -> &[String] {
        &self.node_addrs
    }

    /// Admit a request only for this coordinator's collection: an empty
    /// name gets to the unnamed dataset, or a named collection through a
    /// [`crate::collections::CollectionSet`] that written the name; any
    /// other name refuses rather than answering from the wrong dataset.
    fn admit(&self, requested: &str) -> Result<(), Status> {
        if requested.is_empty() || requested == self.collection {
            return Ok(());
        }
        Err(if self.collection.is_empty() {
            Status::invalid_argument(format!(
                "unknown collection {requested:?}: this coordinator serves one unnamed dataset"
            ))
        } else {
            Status::invalid_argument(format!(
                "this coordinator serves collection {:?}, not {requested:?}",
                self.collection
            ))
        })
    }

    /// Ask every shard which collection it serves and refuse when any
    /// answer differs from this coordinator's: a shard belongs to precisely
    /// one collection, and a fleet that disagrees never serves.
    pub async fn verify_collection_membership(&self) -> Result<(), Status> {
        let mut addrs: Vec<String> = self.node_addrs.clone();
        addrs.extend(self.replica_addrs.iter().flatten().cloned());
        for addr in addrs {
            let mut client = self.node_client(&addr)?;
            let health = client
                .health(HealthRequest {})
                .await
                .map_err(|e| {
                    Status::unavailable(format!(
                        "collection {:?}: node {addr} did not answer a health probe: {}",
                        self.collection,
                        e.message()
                    ))
                })?
                .into_inner();
            if health.collection != self.collection {
                return Err(Status::failed_precondition(format!(
                    "node {addr} serves collection {:?}, but this coordinator is {:?}; a shard \
                     belongs to only one collection",
                    health.collection, self.collection
                )));
            }
        }
        Ok(())
    }

    /// Prove the dense traversal contract against every live shard before the
    /// public query route runs. Provider identity is generation-wide: mixed
    /// quality contracts, score spaces, or dimensions are refused rather than
    /// hidden behind one coordinator response.
    pub(crate) async fn resolve_dense_execution(
        &self,
        requested: crate::pb::DenseExecutionMode,
        query_dim: usize,
        key: DenseRequestKey<'_>,
    ) -> Result<crate::pb::DenseExecutionOutcome, Status> {
        let (provider, scoring_fingerprint, quality, exhaustive, dimensions, rows, generation) =
            if let Some(identity) = self.clustered_quality_identity().await? {
                (
                    "clustered-turbovec".to_string(),
                    identity.scoring_fingerprint,
                    crate::pb::VectorQualityContract::ExhaustiveNativeScore,
                    true,
                    identity.dimensions,
                    identity.rows,
                    identity.topology_generation,
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
                let mut rows: u64 = 0;
                for task in tasks {
                    let backend = task.await.map_err(|error| {
                        Status::internal(format!("dense execution preflight failed: {error}"))
                    })??;
                    rows = rows.checked_add(backend.num_vectors).ok_or_else(|| {
                        Status::internal("dense execution preflight row count overflow")
                    })?;
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
                    let direction =
                        crate::pb::VectorScoreDirection::try_from(descriptor.score_direction)
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
                    rows,
                    self.topology_generation,
                )
            };

        if dimensions as usize != query_dim {
            return Err(Status::failed_precondition(format!(
                "dense query dimension {query_dim} does not match live provider dimension {dimensions}"
            )));
        }

        let exact_available =
            quality == crate::pb::VectorQualityContract::ExhaustiveNativeScore && exhaustive;
        let mut qualified: Option<(crate::dense_policy::Qualified, u32)> = None;
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
            crate::pb::DenseExecutionMode::Auto if exact_available => (
                crate::pb::DenseExecutionMode::Exact,
                "AUTO selected exact because the live provider proves exhaustive completion"
                    .to_string(),
            ),
            // AUTO on a provider without exhaustive completion goes only
            // through the generation-bound policy: identity first, then
            // the exact request key. Nothing is interpolated or defaulted.
            crate::pb::DenseExecutionMode::Auto => {
                let policy = self.dense_execution_policy.as_ref().ok_or_else(|| {
                    Status::failed_precondition(format!(
                        "AUTO has no dense execution policy for provider {provider} ({quality:?}); \
                         install one with --dense-execution-policy, or use ANN explicitly to \
                         accept its configured traversal"
                    ))
                })?;
                if !matches!(
                    quality,
                    crate::pb::VectorQualityContract::ConfiguredAnn
                        | crate::pb::VectorQualityContract::ProbabilisticBound
                ) {
                    return Err(Status::failed_precondition(format!(
                        "provider {provider} does not expose a configured ANN traversal for AUTO \
                         to qualify"
                    )));
                }
                policy
                    .verify_identity(&crate::dense_policy::LiveIdentity {
                        provider_backend: provider.clone(),
                        scoring_fingerprint: scoring_fingerprint.clone(),
                        corpus_generation: generation,
                        corpus_rows: rows,
                        dimensions,
                    })
                    .map_err(Status::failed_precondition)?;
                let selectivity = self.dense_filter_selectivity(key.filters, rows).await?;
                let hit = policy
                    .qualify(crate::dense_policy::RequestKey {
                        k: key.k,
                        candidate_depth: key.candidate_depth,
                        filter_selectivity_ppm: selectivity,
                    })
                    .map_err(Status::failed_precondition)?;
                let point = hit.point;
                let reason = format!(
                    "AUTO selected the provider's configured approximate traversal through \
                     policy {:?} point k={} selectivity {}..={} ppm depth {} (measured recall {} \
                     ppm over {} queries) at live selectivity {selectivity} ppm",
                    hit.policy_id,
                    point.k,
                    point.selectivity_min_ppm,
                    point.selectivity_max_ppm,
                    point.candidates,
                    point.measured_recall_ppm,
                    policy.measured_queries()
                );
                qualified = Some((hit, selectivity));
                (crate::pb::DenseExecutionMode::Ann, reason)
            }
        };

        let mut outcome = crate::pb::DenseExecutionOutcome {
            requested_mode: requested as i32,
            resolved_mode: resolved as i32,
            provider_backend: provider,
            quality_contract: quality as i32,
            scoring_fingerprint,
            exhaustive_completion: resolved == crate::pb::DenseExecutionMode::Exact,
            planner_reason,
            policy_id: String::new(),
            policy_fingerprint: String::new(),
            policy_point: None,
            filter_selectivity_ppm: 0,
            candidate_depth: 0,
            evidence_scope: crate::pb::DenseEvidenceScope::NotApplicable as i32,
        };
        if let Some((hit, selectivity)) = qualified {
            outcome.evidence_scope = crate::pb::DenseEvidenceScope::SelectivityBandBenchmark as i32;
            outcome.policy_id = hit.policy_id;
            outcome.policy_fingerprint = hit.policy_fingerprint;
            outcome.policy_point = Some(crate::pb::DensePolicyPoint {
                k: hit.point.k,
                filter_selectivity_ppm_min: hit.point.selectivity_min_ppm,
                filter_selectivity_ppm_max: hit.point.selectivity_max_ppm,
                candidates: hit.point.candidates,
                measured_recall_ppm: hit.point.measured_recall_ppm,
            });
            outcome.filter_selectivity_ppm = selectivity;
            outcome.candidate_depth = hit.point.candidates;
        }
        Ok(outcome)
    }

    async fn dense_filter_selectivity(
        &self,
        filters: Option<&RequestFilters>,
        rows: u64,
    ) -> Result<u32, Status> {
        let caller_filtered = filters.is_some_and(|f| !f.geo.is_empty() || f.tree.is_some());
        if !caller_filtered && self.document_visibility.is_none() {
            return Ok(crate::dense_policy::UNFILTERED_PPM);
        }
        // Both membership passes must describe the same physical read. A
        // standalone planner has no public Query envelope to pin them for it.
        if self.query_read_versions.is_none() {
            let (pinned, reads) = self.pin_read_versions().await?;
            let result = Box::pin(pinned.dense_filter_selectivity(filters, rows)).await;
            pinned.validate_read_versions(&reads).await?;
            return result;
        }
        let vectors = self
            .vector_membership(self.vector_read_field.as_deref().unwrap_or(""))
            .await?;
        let admitted = if caller_filtered {
            let documents = self
                .filter_membership(filters.expect("caller filter"))
                .await?;
            vectors.ids.intersection(&documents.ids).count()
        } else {
            vectors.ids.len()
        };
        Ok(crate::dense_policy::selectivity_ppm(admitted as u64, rows))
    }

    /// `DENSE_EXECUTION_MODE_AUTO` with FP32 rerank, no policy, and no
    /// `selection_k`: resolve through the installed profile's default
    /// target exactly as an explicit `DenseQualityPolicy` naming it would
    /// (`docs/dense-quality-profile.md`). No profile, or a profile without
    /// a default, refuses by name rather than running at `selection_k = k`.
    pub(crate) async fn resolve_dense_quality_default(
        &self,
        k: u32,
        query_dim: usize,
    ) -> Result<crate::quality::DenseQualityResolution, Status> {
        const NEEDS: &str = "AUTO with FP32 rerank needs a measured quality profile with \
                             default_target_recall_ppm, or an explicit DenseQualityPolicy or \
                             selection_k";
        let profile = self.dense_quality_profile.as_ref().ok_or_else(|| {
            Status::failed_precondition(format!(
                "{NEEDS}; this coordinator has no --dense-quality-profile"
            ))
        })?;
        let target = profile.default_target_recall_ppm().ok_or_else(|| {
            Status::failed_precondition(format!(
                "{NEEDS}; profile {:?} carries no default_target_recall_ppm",
                profile.profile_id()
            ))
        })?;
        self.resolve_dense_quality(
            k,
            query_dim,
            &crate::pb::DenseQualityPolicy {
                target_recall_ppm: target,
                max_candidates: 0,
                required_profile_fingerprint: String::new(),
            },
        )
        .await
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
        if resolution.selection_k > self.knobs.max_k() {
            return Err(Status::failed_precondition(format!(
                "quality profile resolves selection_k={} above coordinator max_k={}; raise --max-k or measure a bounded policy",
                resolution.selection_k, self.knobs.max_k()
            )));
        }
        if resolution.dimensions as usize != query_dim {
            return Err(Status::failed_precondition(format!(
                "quality profile dimension {} does not match query dimension {query_dim}",
                resolution.dimensions
            )));
        }

        let (provider, scoring_fingerprint, rows, generation, dimensions) = if let Some(identity) =
            self.clustered_quality_identity().await?
        {
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
    /// Dial a node across the network (the `net` feature): a lazy channel
    /// with the engine's window sizes and the process's client TLS.
    #[cfg(feature = "net")]
    fn connect(&self, addr: &str) -> Result<Channel, Status> {
        let tls = self
            .client_tls
            .as_ref()
            .or(crate::security::process_client_tls());
        let endpoint = Endpoint::from_shared(crate::security::secure_url(addr, tls))
            .map_err(|e| Status::unavailable(format!("invalid node address {addr}: {e}")))?
            .tcp_nodelay(true)
            // The client end is the RECEIVER of stream batches, so these
            // windows are what let a shard's pre-floor burst flow without
            // window-update round trips (see H2_STREAM_WINDOW).
            .initial_stream_window_size(crate::H2_STREAM_WINDOW)
            .initial_connection_window_size(crate::H2_CONN_WINDOW);
        let endpoint = crate::security::apply_client_tls(endpoint, tls)
            .map_err(Status::failed_precondition)?;
        Ok(endpoint.connect_lazy())
    }

    /// The link to a node: the cached one (a local node, or a channel
    /// dialed before), else a new channel when the network is allowed.
    pub(crate) fn node_client(&self, addr: &str) -> Result<crate::link::NodeLink, Status> {
        #[cfg_attr(not(feature = "net"), allow(unused_mut))]
        let mut cache = self.links.lock().expect("node link cache mutex poisoned");
        if let Some(link) = cache.get(addr) {
            return Ok(link.clone());
        }
        if !self.allow_network {
            return Err(Status::failed_precondition(format!(
                "in-process coordinator has no link for {addr}; network fallback is disabled"
            )));
        }
        #[cfg(feature = "net")]
        {
            let link = crate::link::NodeLink::remote(self.connect(addr)?);
            cache.insert(addr.to_string(), link.clone());
            Ok(link)
        }
        #[cfg(not(feature = "net"))]
        {
            Err(Status::failed_precondition(format!(
                "this build has no network transport (feature `net` is off); no link for {addr}"
            )))
        }
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
            &[],
            &mut Vec::new(),
            None,
            &[],
            false,
            &mut Vec::new(),
            false,
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
        prefixes: &[crate::pb::TermPrefix],
        expansions: &mut Vec<crate::pb::PrefixExpansion>,
        highlight: Option<&crate::pb::HighlightSpec>,
        synonyms: &[crate::pb::SynonymRule],
        synonyms_off: bool,
        synonym_expansions: &mut Vec<crate::pb::SynonymExpansion>,
        explain: bool,
    ) -> Result<AggregatedHits, Status> {
        let analysis_fingerprint = crate::analyzer::analysis_fingerprint(spec);
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
        let analyzed = analyze_query(&addr, text, spec, prefixes).await?;
        let mut terms: Vec<String> = Vec::new();
        for (term, _, _) in analyzed.terms {
            if !terms.contains(&term) {
                terms.push(term);
            }
        }
        // Term prefixes join the analyzed terms (docs/prefix-terms.md).
        expansions.extend(
            self.expand_prefixes("body", spec, prefixes, &mut terms)
                .await?,
        );
        // Synonym rules join them the same way (docs/synonyms.md).
        synonym_expansions.extend(
            self.expand_synonyms("body", spec, synonyms, synonyms_off, &mut terms)
                .await?,
        );
        // An empty lexical selection still needs the projection schema
        // agreement. Request metadata without scoring when analysis is empty.
        let k = if terms.is_empty() { 0 } else { k };
        if k == 0 && projections.is_empty() {
            if self.document_visibility.is_some() {
                self.body_stats(&[], false).await?;
            }
            return Ok((
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                crate::segment_prune::PruneStats::default(),
            ));
        }

        // (b) each shard's share of the corpus stats, cached per node;
        // (c)+(d) run as a round so a stale-stats refusal can rerun
        // them once against fresh stats with a new fenced claim.
        let mut fresh = false;
        loop {
            let (global, epochs) = self.body_stats(&terms, fresh).await?;
            let claims = epochs;
            match self
                .bm25_query_round(
                    &terms,
                    analysis_fingerprint,
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
                    highlight,
                    explain,
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
        analysis_fingerprint: u64,
        k: u32,
        min_score: f32,
        global: &CorpusStats,
        claims: &[StatsClaim],
        facet_fields: &[String],
        map_facet_fields: &[crate::pb::MapFacetField],
        range_facet_fields: &[crate::pb::RangeFacetField],
        score_stages: &[crate::pb::ScoreStage],
        geo_filters: &[crate::pb::GeoFilter],
        filter: Option<&crate::pb::FilterExpr>,
        stats_fields: &[String],
        cardinality_fields: &[String],
        projections: &[crate::pb::CompiledProjection],
        highlight: Option<&crate::pb::HighlightSpec>,
        explain: bool,
    ) -> Result<AggregatedHits, Status> {
        if self.node_addrs.is_empty() {
            return Err(Status::failed_precondition("no shard nodes configured"));
        }
        let mut query_tasks = tokio::task::JoinSet::new();
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
        let mask = self.shard_mask(filter);
        for (shard, node) in self.node_addrs.iter().enumerate() {
            if mask.as_ref().is_some_and(|m| m.skipped[shard]) {
                continue;
            }
            let request = Bm25QueryRequest {
                analysis_fingerprint,
                highlight: highlight.cloned(),
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
                expected_stats_epoch: claims[shard].epoch,
                expected_stats_incarnation: claims[shard].incarnation(),
                facet_fields: facet_fields.to_vec(),
                map_facet_fields: map_facet_fields.to_vec(),
                range_facet_fields: range_facet_fields.to_vec(),
                score_stages: score_stages.to_vec(),
                geo_filters: geo_filters.to_vec(),
                filter: filter.cloned(),
                stats_fields: stats_fields.to_vec(),
                cardinality_fields: cardinality_fields.to_vec(),
                phrase: None,
                explain,
            };
            let mut client = self.node_client(node)?;
            if let Some((floor_tx, floor_rx)) = relay.clone() {
                let global_heap = Arc::clone(stream_heap.as_ref().expect("relay has heap"));
                let deadline = self.limits.shard_deadline;
                query_tasks.spawn(async move {
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
                            response.projection_types,
                            (response.segments_total, response.segments_skipped),
                            Some(r.scoring_fingerprint),
                        )
                    })
                });
                continue;
            }
            query_tasks.spawn(async move {
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
                        r.projection_types,
                        (r.segments_total, r.segments_skipped),
                        None,
                    )
                })
            });
        }
        let mut all: Vec<(u32, Bm25Hit)> = Vec::new();
        let mut prune = crate::segment_prune::PruneStats::default();
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
        let mut projection_types = vec![crate::pb::ScalarValueType::Unspecified; projections.len()];
        let mut shard_stats: Vec<Vec<crate::pb::ColumnStats>> = Vec::new();
        let mut shard_distinct: Vec<Vec<crate::pb::FacetDistinct>> = Vec::new();
        let mut scoring_fingerprint: Option<String> = None;
        let mut responses = Vec::new();
        while let Some(joined) = query_tasks.join_next().await {
            responses.push(
                joined.map_err(|e| Status::internal(format!("bm25 query task failed: {e}")))??,
            );
        }
        responses.sort_by_key(|response| response.0);
        for response in responses {
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
                ptypes,
                sprune,
                fingerprint,
            ) = response;
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
            crate::values::merge_projection_types(projections, &mut projection_types, &ptypes)?;
            for hit in &hits {
                crate::values::validate_projection_row(&hit.projected, &ptypes)?;
            }
            all.extend(hits.into_iter().map(|h| (shard, h)));
            shard_facets.push(facets);
            shard_ranges.push(ranges);
            shard_stats.push(sstats);
            shard_distinct.push(sdistinct);
            prune.add(crate::segment_prune::PruneStats {
                segments_total: sprune.0,
                segments_skipped: sprune.1,
            });
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
            .map(|(s, _)| match s.map_key() {
                None => format!("{:?}", s.column),
                Some(key) => format!("{:?}[{:?}]", s.column, key),
            })
            .collect();
        if !unknown.is_empty() {
            // A geo decay stage reads a geo column, so pointing the
            // caller at --numeric-fields alone would send them looking
            // in the wrong table for a name they spelled wrong.
            let any_geo = score_stages.iter().zip(&stage_known).any(|(s, known)| {
                !known
                    && matches!(
                        crate::pb::ScoreOp::try_from(s.operation_code()),
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
        if let Some(mask) = mask.as_ref() {
            mark_known(&mut filter_known, &mask.known);
        }
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
            prune,
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
            &mut Vec::new(),
            None,
            &mut Vec::new(),
            false,
        )
        .await
        .map(|((hits, _), _)| hits)
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
        expansions: &mut Vec<crate::pb::PrefixExpansion>,
        highlight: Option<&crate::pb::HighlightSpec>,
        synonym_expansions: &mut Vec<crate::pb::SynonymExpansion>,
        explain: bool,
    ) -> Result<
        (
            (FacetedHits, crate::segment_prune::PruneStats),
            Vec<crate::pb::PhraseRouting>,
        ),
        Status,
    > {
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
            let analyzed = analyze_query(&addr, text, f.analysis.as_ref(), &f.prefixes).await?;
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
            // Term prefixes join this field's terms after the phrase
            // sequence was taken, so the sequence's indexes stay put.
            expansions.extend(
                self.expand_prefixes(&f.field, f.analysis.as_ref(), &f.prefixes, &mut terms)
                    .await?,
            );
            synonym_expansions.extend(
                self.expand_synonyms(
                    &f.field,
                    f.analysis.as_ref(),
                    &f.synonyms,
                    f.synonyms_off,
                    &mut terms,
                )
                .await?,
            );
            field_terms.push(terms);
            phrase_requests.push(phrase);
        }
        let t_analyzed = t0.elapsed();
        if k == 0 || field_terms.iter().all(|t| t.is_empty()) {
            if self.document_visibility.is_some() {
                self.body_stats(&[], false).await?;
            }
            return Ok((
                (
                    (Vec::new(), Vec::new(), Vec::new()),
                    crate::segment_prune::PruneStats::default(),
                ),
                Vec::new(),
            ));
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
                    let column = crate::proximity::bigram_field_name(&f.field);
                    if self.field_permissions.as_ref().is_some_and(|fields| {
                        !fields.can_use(&column) || (explain && !fields.can_disclose(&column))
                    }) {
                        continue;
                    }
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
        // them once against fresh stats with a new fenced claim.
        let n_shards = self.node_addrs.len();
        let mut fresh = false;
        loop {
            let globals = self
                .fused_stats_probing(&stats_fields, fresh, probe_from)
                .await?;
            let claims = globals.epochs.clone();
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
                    highlight,
                    trace,
                    t0,
                    t_analyzed,
                    t_stats,
                    explain,
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
                prefixes: Vec::new(),
                synonyms: Vec::new(),
                synonyms_off: false,
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
            if field.phrase.is_some() || !field.prefixes.is_empty() {
                return Err(Status::invalid_argument(format!(
                    "field {:?}: PhraseSearch scores glossary concepts; PhraseMatch and \
                     TermPrefix constraints are served by Bm25Search",
                    field.field
                )));
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
            prefixes: Vec::new(),
            synonyms: Vec::new(),
            synonyms_off: false,
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
            let claims = globals.epochs.clone();
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
                    None,
                    trace,
                    t0,
                    t_analyzed,
                    t_stats,
                    base.explain,
                )
                .await;
            match round {
                Err(error) if !fresh && is_stale_stats(&error) => {
                    self.stats_cache.invalidate_all();
                    fresh = true;
                }
                other => return other.map(|(hits, _)| hits),
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
    /// Expand every [`crate::pb::TermPrefix`] of `field` across the fleet
    /// (`docs/prefix-terms.md`) and add the expansions to `terms`. The
    /// prefix is normalized under the field's char filters (never
    /// stemmed), every shard answers from its byte-sorted directory, and
    /// the fleet-wide union is the term list — the same list a monolith
    /// would expand, so distributed scoring equals monolithic scoring.
    /// Past the cap on any shard or in the union, the request is
    /// INVALID_ARGUMENT naming the count: a prefix is never silently
    /// truncated to a quieter match set.
    async fn expand_prefixes(
        &self,
        field: &str,
        spec: Option<&crate::pb::AnalysisSpec>,
        prefixes: &[crate::pb::TermPrefix],
        terms: &mut Vec<String>,
    ) -> Result<Vec<crate::pb::PrefixExpansion>, Status> {
        let mut out = Vec::with_capacity(prefixes.len());
        for prefix in prefixes {
            let cap = match prefix.max_expansions {
                0 => DEFAULT_PREFIX_EXPANSIONS,
                n if n as usize > MAX_PREFIX_EXPANSIONS => {
                    return Err(Status::invalid_argument(format!(
                        "prefix {:?}: max_expansions {n} exceeds the maximum \
                         {MAX_PREFIX_EXPANSIONS}",
                        prefix.prefix
                    )))
                }
                n => n as usize,
            };
            let normalized = crate::analyzer::normalize_prefix(&prefix.prefix, spec)?;
            let mut tasks = Vec::with_capacity(self.node_addrs.len());
            for (i, node) in self.node_addrs.iter().enumerate() {
                let mut client = self.node_client(node)?;
                let request = crate::pb::ExpandTermPrefixRequest {
                    visibility: self.document_visibility.clone(),
                    field: field.to_string(),
                    prefix: normalized.clone(),
                    cap: cap as u32,
                };
                tasks.push((
                    i,
                    tokio::spawn(async move {
                        client
                            .expand_term_prefix(request)
                            .await
                            .map(|r| r.into_inner())
                    }),
                ));
            }
            let mut union = std::collections::BTreeSet::new();
            let scope = crate::visibility::VisibilityScope::new(self.document_visibility.as_ref())?;
            let mut visibility_known = vec![false; scope.column_count()];
            let mut known = false;
            for (shard, task) in tasks {
                let resp = task.await.map_err(|e| {
                    Status::internal(format!("prefix expansion task failed: {e}"))
                })??;
                scope
                    .validate_echo(&resp.visibility_fingerprint, &resp.visibility_columns_known)?;
                for (known, present) in visibility_known
                    .iter_mut()
                    .zip(&resp.visibility_columns_known)
                {
                    *known |= present;
                }
                if !resp.known {
                    continue;
                }
                known = true;
                if resp.count as usize > cap {
                    return Err(Status::invalid_argument(format!(
                        "prefix {normalized:?} on field {field:?} expands to {} terms on shard \
                         {shard}; the cap is {cap} (raise max_expansions up to \
                         {MAX_PREFIX_EXPANSIONS}, or lengthen the prefix)",
                        resp.count
                    )));
                }
                union.extend(resp.terms);
            }
            self.check_visibility_columns(&visibility_known)?;
            if !known {
                return Err(Status::invalid_argument(format!(
                    "no shard indexes field {field:?}; prefix {normalized:?} has no dictionary \
                     to expand in"
                )));
            }
            if union.len() > cap {
                return Err(Status::invalid_argument(format!(
                    "prefix {normalized:?} on field {field:?} expands to {} terms across the \
                     fleet; the cap is {cap} (raise max_expansions up to \
                     {MAX_PREFIX_EXPANSIONS}, or lengthen the prefix)",
                    union.len()
                )));
            }
            let expanded: Vec<String> = union.into_iter().collect();
            for term in &expanded {
                if !terms.contains(term) {
                    terms.push(term.clone());
                }
            }
            out.push(crate::pb::PrefixExpansion {
                field: field.to_string(),
                prefix: normalized,
                terms: expanded,
            });
        }
        Ok(out)
    }

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
        let scope = crate::visibility::VisibilityScope::new(self.document_visibility.as_ref())?;
        let mut visibility_known = vec![false; scope.column_count()];
        let mut shares: Vec<Option<crate::stats_cache::FusedShare>> = vec![None; n];
        if !fresh {
            for (i, share) in shares.iter_mut().enumerate() {
                *share = self
                    .stats_cache
                    .lookup_fused_scoped(i, stats_fields, &scope);
            }
        }
        let mut fetch_tasks = Vec::new();
        for (i, share) in shares.iter().enumerate() {
            if share.is_some() {
                continue;
            }
            let request = TermStatsRequest {
                version_only: false,
                visibility: self.document_visibility.clone(),
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
            self.stats_cache
                .store_scoped(i, &[], stats_fields, &scope, &resp)?;
            shares[i] = Some(crate::stats_cache::FusedShare {
                visibility_columns_known: resp.visibility_columns_known.clone(),
                epoch: StatsClaim::required(resp.stats_epoch, &resp.stats_incarnation)?,
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
            for (known, present) in visibility_known.iter_mut().zip(&s.visibility_columns_known) {
                *known |= present;
            }
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
        self.check_visibility_columns(&visibility_known)?;
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
        claims: &[StatsClaim],
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
        highlight: Option<&crate::pb::HighlightSpec>,
        trace: bool,
        t0: std::time::Instant,
        t_analyzed: std::time::Duration,
        t_stats: std::time::Duration,
        explain: bool,
    ) -> Result<(FacetedHits, crate::segment_prune::PruneStats), Status> {
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
        let mut query_tasks = tokio::task::JoinSet::new();
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
        let mask = self.shard_mask(filter);
        for (shard, node) in self.node_addrs.iter().enumerate() {
            if mask.as_ref().is_some_and(|m| m.skipped[shard]) {
                continue;
            }
            let request = Bm25QueryRequest {
                analysis_fingerprint: 0,
                highlight: highlight.cloned(),
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
                expected_stats_epoch: claims[shard].epoch,
                expected_stats_incarnation: claims[shard].incarnation(),
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
                explain,
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
                query_tasks.spawn(async move {
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
                        (response.segments_total, response.segments_skipped),
                        started.elapsed().as_secs_f64() * 1000.0,
                        Some(result.scoring_fingerprint),
                    ))
                });
                continue;
            }
            query_tasks.spawn(async move {
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
                        (r.segments_total, r.segments_skipped),
                        started.elapsed().as_secs_f64() * 1000.0,
                        None,
                    )
                })
            });
        }
        let mut all: Vec<(u32, Bm25Hit)> = Vec::new();
        let mut prune = crate::segment_prune::PruneStats::default();
        let mut shard_facets: Vec<Vec<crate::pb::FacetFieldCounts>> = Vec::new();
        let mut shard_ranges: Vec<Vec<crate::pb::RangeFacetCounts>> = Vec::new();
        let mut per_shard: Vec<(u32, f64)> = Vec::new();
        let mut geo_known = vec![false; geo_filters.len()];
        let filter_leaves = filter.map_or(0, crate::filter::leaf_count);
        let mut filter_known = vec![false; filter_leaves];
        let mut scoring_fingerprint: Option<String> = None;
        let mut responses = Vec::new();
        while let Some(joined) = query_tasks.join_next().await {
            responses.push(
                joined.map_err(|e| Status::internal(format!("bm25 query task failed: {e}")))??,
            );
        }
        responses.sort_by_key(|response| response.0);
        for response in responses {
            let (shard, hits, facets, ranges, geo, fknown, sprune, ms, fingerprint) = response;
            prune.add(crate::segment_prune::PruneStats {
                segments_total: sprune.0,
                segments_skipped: sprune.1,
            });
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
        if let Some(mask) = mask.as_ref() {
            mark_known(&mut filter_known, &mask.known);
        }
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
        Ok((
            (all.into_iter().map(|(_, h)| h).collect(), facets, ranges),
            prune,
        ))
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
        self.check_vector_scan(filters, false)?;
        if self.scoped_vector_scan() && self.query_read_versions.is_none() {
            let (pinned, reads) = self.pin_read_versions().await?;
            let result = Box::pin(
                pinned.fanout_hybrid(request_id, text, vector, k, spec, legs, debug, filters),
            )
            .await;
            pinned.validate_read_versions(&reads).await?;
            return result;
        }
        let analysis_fingerprint = crate::analyzer::analysis_fingerprint(spec);
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
        // shard reruns them once against fresh stats with a new fenced claim.
        let mut fresh = false;
        let (hits, mut dbg, stats_ms) = loop {
            let (global, epochs) = self.body_stats(&terms, fresh).await?;
            let claims = epochs;
            let stats_ms = t.elapsed().as_secs_f32() * 1e3;
            let round = match legs.fusion_mode {
                FusionMode::TwoLevel => {
                    self.fanout_hybrid_two_level(
                        request_id,
                        vector,
                        k,
                        &terms,
                        analysis_fingerprint,
                        &global,
                        &claims,
                        legs,
                        debug,
                        filters,
                    )
                    .await
                }
                FusionMode::Decomposed => {
                    self.fanout_hybrid_decomposed(
                        request_id,
                        vector,
                        k,
                        &terms,
                        analysis_fingerprint,
                        &global,
                        &claims,
                        legs,
                        debug,
                        filters,
                    )
                    .await
                }
                _ => {
                    self.fanout_hybrid_global_rank(
                        request_id,
                        vector,
                        k,
                        &terms,
                        analysis_fingerprint,
                        &global,
                        &claims,
                        legs,
                        debug,
                        filters,
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
    pub(crate) async fn body_stats(
        &self,
        terms: &[String],
        fresh: bool,
    ) -> Result<(CorpusStats, Vec<StatsClaim>), Status> {
        let n = self.node_addrs.len();
        let scope = crate::visibility::VisibilityScope::new(self.document_visibility.as_ref())?;
        let mut visibility_known = vec![false; scope.column_count()];
        let mut shares: Vec<Option<crate::stats_cache::BodyShare>> = vec![None; n];
        if !fresh {
            for (i, share) in shares.iter_mut().enumerate() {
                *share = self.stats_cache.lookup_body_scoped(i, terms, &scope);
            }
        }
        let mut fetch_tasks = Vec::new();
        for (i, share) in shares.iter().enumerate() {
            if share.is_some() {
                continue;
            }
            let terms_owned = terms.to_vec();
            let visibility = self.document_visibility.clone();
            let mut client = self.node_client(&self.node_addrs[i])?;
            self.stats_cache.note_fetch();
            fetch_tasks.push((
                i,
                tokio::spawn(async move {
                    client
                        .term_stats(TermStatsRequest {
                            version_only: false,
                            visibility,
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
            self.stats_cache
                .store_scoped(i, terms, &[], &scope, &resp)?;
            shares[i] = Some(crate::stats_cache::BodyShare {
                visibility_columns_known: resp.visibility_columns_known.clone(),
                epoch: StatsClaim::required(resp.stats_epoch, &resp.stats_incarnation)?,
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
            for (known, present) in visibility_known.iter_mut().zip(&s.visibility_columns_known) {
                *known |= present;
            }
            global.doc_count += s.doc_count;
            global.total_doc_length += s.total_doc_length;
            for (acc, df) in global.dfs.iter_mut().zip(&s.dfs) {
                *acc += df;
            }
            epochs.push(s.epoch);
        }
        self.check_visibility_columns(&visibility_known)?;
        Ok((global, epochs))
    }

    #[allow(clippy::too_many_arguments)]
    #[cfg(feature = "net")]
    async fn clustered_hybrid_global_rank(
        &self,
        request_id: &str,
        vector: &[f32],
        k: u32,
        terms: &[String],
        analysis_fingerprint: u64,
        global: &CorpusStats,
        claims: &[StatsClaim],
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
        let mask = self.shard_mask(filters.tree.as_ref());
        let mut shard_tasks = Vec::with_capacity(self.node_addrs.len());
        for (shard, node) in self.node_addrs.iter().enumerate() {
            if mask.as_ref().is_some_and(|m| m.skipped[shard]) {
                continue;
            }
            let request = ShardLegsRequest {
                read_context: None,
                analysis_fingerprint,
                request_id: request_id.to_string(),
                k: legs.leg_k,
                vector: Vec::new(),
                terms: leg_terms.clone(),
                global_doc_count: global.doc_count,
                global_total_doc_length: global.total_doc_length,
                global_doc_frequencies: leg_dfs.clone(),
                k1: self.bm25_params.k1 as f32,
                b: self.bm25_params.b as f32,
                expected_stats_epoch: claims[shard].epoch,
                expected_stats_incarnation: claims[shard].incarnation(),
                geo_filters: filters.geo.clone(),
                filter: Self::shard_filter_tree(filters, mask.as_ref(), shard),
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
        let mut known = Self::filter_known(filters, mask.as_ref());
        for task in shard_tasks {
            let (shard, elapsed, response) = task
                .await
                .map_err(|error| Status::internal(format!("shard legs task failed: {error}")))??;
            known.merge_shard(
                shard as usize,
                &response.geo_columns_known,
                &response.filter_columns_known,
            )?;
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
                vector_normalized: hit.leg_norms.first().copied().flatten(),
                bm25_normalized: hit.leg_norms.get(1).copied().flatten(),
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
    // Without the network stack the clustered arm is compiled out and
    // its inputs go unread; the exact arm is the whole function then.
    #[cfg_attr(not(feature = "net"), allow(unused_variables, unused_mut))]
    async fn fanout_hybrid_global_rank(
        &self,
        request_id: &str,
        vector: &[f32],
        k: u32,
        terms: &[String],
        analysis_fingerprint: u64,
        global: &CorpusStats,
        claims: &[StatsClaim],
        legs: HybridLegs,
        debug: bool,
        filters: &RequestFilters,
    ) -> Result<(Vec<HybridHit>, Option<HybridDebug>), Status> {
        #[cfg(feature = "net")]
        if self.clustered_vectors.is_some() {
            return self
                .clustered_hybrid_global_rank(
                    request_id,
                    vector,
                    k,
                    terms,
                    analysis_fingerprint,
                    global,
                    claims,
                    legs,
                    debug,
                    filters,
                )
                .await;
        }
        let t_legs = std::time::Instant::now();
        let (leg_vector, leg_terms, leg_dfs) = leg_payloads(vector, terms, global, legs);
        let admission = self.vector_read_barrier()?;
        let mask = self.vector_scan_mask(filters.tree.as_ref());
        let mut shard_tasks = Vec::with_capacity(self.node_addrs.len());
        for (shard, node) in self.node_addrs.iter().enumerate() {
            if mask.as_ref().is_some_and(|m| m.skipped[shard]) {
                continue;
            }
            let request = ShardLegsRequest {
                read_context: admission.as_ref().map(|a| a.context(shard)).transpose()?,
                analysis_fingerprint,
                request_id: String::new(),
                k: legs.leg_k,
                vector: leg_vector.clone(),
                terms: leg_terms.clone(),
                global_doc_count: global.doc_count,
                global_total_doc_length: global.total_doc_length,
                global_doc_frequencies: leg_dfs.clone(),
                k1: self.bm25_params.k1 as f32,
                b: self.bm25_params.b as f32,
                expected_stats_epoch: claims[shard].epoch,
                expected_stats_incarnation: claims[shard].incarnation(),
                geo_filters: filters.geo.clone(),
                filter: Self::shard_filter_tree(filters, mask.as_ref(), shard),
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
        let mut known = Self::filter_known(filters, mask.as_ref());
        for task in shard_tasks {
            let (shard, rpc_ms, response) = task
                .await
                .map_err(|e| Status::internal(format!("shard legs task failed: {e}")))??;
            match (&admission, response.read_receipt.as_ref()) {
                (Some(admission), Some(receipt)) => admission.accept(shard as usize, receipt)?,
                (None, None) => {}
                _ => {
                    return Err(Status::failed_precondition(
                        "hybrid leg read receipt mismatch",
                    ))
                }
            }
            known.merge_shard(
                shard as usize,
                &response.geo_columns_known,
                &response.filter_columns_known,
            )?;
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
        if let Some(admission) = &admission {
            admission.wait().await?;
        }
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
                vector_normalized: f.leg_norms.first().copied().flatten(),
                bm25_normalized: f.leg_norms.get(1).copied().flatten(),
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
    #[cfg(feature = "net")]
    async fn clustered_hybrid_two_level(
        &self,
        request_id: &str,
        vector: &[f32],
        k: u32,
        terms: &[String],
        analysis_fingerprint: u64,
        global: &CorpusStats,
        claims: &[StatsClaim],
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
        let mask = self.shard_mask(filters.tree.as_ref());
        let mut tasks = Vec::with_capacity(self.node_addrs.len());
        for (shard, node) in self.node_addrs.iter().enumerate() {
            if mask.as_ref().is_some_and(|m| m.skipped[shard]) {
                continue;
            }
            let request = ShardLegsRequest {
                read_context: None,
                analysis_fingerprint,
                request_id: request_id.to_string(),
                k: legs.leg_k,
                vector: Vec::new(),
                terms: leg_terms.clone(),
                global_doc_count: global.doc_count,
                global_total_doc_length: global.total_doc_length,
                global_doc_frequencies: leg_dfs.clone(),
                k1: self.bm25_params.k1 as f32,
                b: self.bm25_params.b as f32,
                expected_stats_epoch: claims[shard].epoch,
                expected_stats_incarnation: claims[shard].incarnation(),
                geo_filters: filters.geo.clone(),
                filter: Self::shard_filter_tree(filters, mask.as_ref(), shard),
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
        let mut known = Self::filter_known(filters, mask.as_ref());
        for task in tasks {
            let (shard, elapsed, response) = task
                .await
                .map_err(|error| Status::internal(format!("shard legs task failed: {error}")))??;
            known.merge_shard(
                shard as usize,
                &response.geo_columns_known,
                &response.filter_columns_known,
            )?;
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
                    vector_normalized: None,
                    bm25_normalized: None,
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
        analysis_fingerprint: u64,
        global: &CorpusStats,
        claims: &[StatsClaim],
        legs: HybridLegs,
        debug: bool,
        filters: &RequestFilters,
    ) -> Result<(Vec<HybridHit>, Option<HybridDebug>), Status> {
        #[cfg(feature = "net")]
        if self.clustered_vectors.is_some() {
            return self
                .clustered_hybrid_two_level(
                    request_id,
                    vector,
                    k,
                    terms,
                    analysis_fingerprint,
                    global,
                    claims,
                    legs,
                    debug,
                    filters,
                )
                .await;
        }
        let t_legs = std::time::Instant::now();
        // Level one: per-shard local fusion.
        let (leg_vector, leg_terms, leg_dfs) = leg_payloads(vector, terms, global, legs);
        let admission = self.vector_read_barrier()?;
        let mask = self.vector_scan_mask(filters.tree.as_ref());
        let mut shard_tasks = Vec::with_capacity(self.node_addrs.len());
        for (shard, node) in self.node_addrs.iter().enumerate() {
            if mask.as_ref().is_some_and(|m| m.skipped[shard]) {
                continue;
            }
            let request = HybridShardRequest {
                read_context: admission.as_ref().map(|a| a.context(shard)).transpose()?,
                analysis_fingerprint,
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
                expected_stats_epoch: claims[shard].epoch,
                expected_stats_incarnation: claims[shard].incarnation(),
                geo_filters: filters.geo.clone(),
                filter: Self::shard_filter_tree(filters, mask.as_ref(), shard),
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
                        r.read_receipt,
                    )
                })
            }));
        }
        let mut shard_lists: Vec<(u32, Vec<crate::pb::HybridLegHit>)> = Vec::new();
        let mut shard_debug: Vec<HybridShardDebug> = Vec::new();
        let mut known = Self::filter_known(filters, mask.as_ref());
        for task in shard_tasks {
            let (shard, rpc_ms, mut hits, geo_known, filter_known, receipt) = task
                .await
                .map_err(|e| Status::internal(format!("hybrid shard task failed: {e}")))??;
            match (&admission, receipt.as_ref()) {
                (Some(admission), Some(receipt)) => admission.accept(shard as usize, receipt)?,
                (None, None) => {}
                _ => {
                    return Err(Status::failed_precondition(
                        "hybrid fusion read receipt mismatch",
                    ))
                }
            }
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
        if let Some(admission) = &admission {
            admission.wait().await?;
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
                    vector_normalized: None,
                    bm25_normalized: None,
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
    // Without the network stack the clustered arm is compiled out and
    // its inputs go unread; the exact arm is the whole function then.
    #[cfg_attr(not(feature = "net"), allow(unused_variables, unused_mut))]
    async fn fanout_hybrid_decomposed(
        &self,
        request_id: &str,
        vector: &[f32],
        k: u32,
        terms: &[String],
        analysis_fingerprint: u64,
        global: &CorpusStats,
        claims: &[StatsClaim],
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
        let mask = self.vector_scan_mask(filters.tree.as_ref());
        let mut known = Self::filter_known(filters, mask.as_ref());
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
            let admission = self.vector_read_barrier()?;
            let mut leg_tasks = Vec::with_capacity(n_nodes);
            for (shard, node) in self.node_addrs.iter().enumerate() {
                if mask.as_ref().is_some_and(|m| m.skipped[shard]) {
                    continue;
                }
                // Use the same guarded authority view as the vector pass.
                // A private lexical winner must not set the visible leg's
                // boundary or erase a visible document's rank provenance.
                let request = ShardLegsRequest {
                    analysis_fingerprint,
                    request_id: request_id.to_string(),
                    k: legs.leg_k,
                    vector: Vec::new(),
                    terms: terms.to_vec(),
                    global_doc_count: global.doc_count,
                    global_total_doc_length: global.total_doc_length,
                    global_doc_frequencies: global.dfs.clone(),
                    k1: self.bm25_params.k1 as f32,
                    b: self.bm25_params.b as f32,
                    expected_stats_epoch: claims[shard].epoch,
                    expected_stats_incarnation: claims[shard].incarnation(),
                    geo_filters: filters.geo.clone(),
                    filter: Self::shard_filter_tree(filters, mask.as_ref(), shard),
                    read_context: admission.as_ref().map(|a| a.context(shard)).transpose()?,
                };
                let mut client = self.node_client(node)?;
                leg_tasks.push(tokio::spawn(async move {
                    client.shard_legs(request).await.map(|r| {
                        let r = r.into_inner();
                        (
                            shard as u32,
                            r.bm25_hits,
                            r.geo_columns_known,
                            r.filter_columns_known,
                            r.read_receipt,
                        )
                    })
                }));
            }
            for task in leg_tasks {
                let (shard, hits, geo_known, filter_known, receipt) = task
                    .await
                    .map_err(|e| Status::internal(format!("bm25 leg task failed: {e}")))??;
                match (&admission, receipt.as_ref()) {
                    (Some(admission), Some(receipt)) => {
                        admission.accept(shard as usize, receipt)?
                    }
                    (None, None) => {}
                    _ => {
                        return Err(Status::failed_precondition(
                            "decomposed leg read receipt mismatch",
                        ))
                    }
                }
                known.merge(&geo_known, &filter_known)?;
                leg_counts.insert(shard, hits.len() as u32);
                for h in &hits {
                    bm25_of.insert(h.doc_id, (h.score, shard));
                    merged.push((h.doc_id, h.score, shard));
                }
            }
            if let Some(admission) = &admission {
                admission.wait().await?;
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
        let v_of = self
            .fanout_vector_rescore(
                vector,
                seed_ids,
                self.vector_read_field.as_deref().unwrap_or(""),
            )
            .await?;

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
        #[cfg(feature = "net")]
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
            for (shard, summary) in summaries.iter_mut().enumerate() {
                if mask.as_ref().is_some_and(|m| m.skipped[shard]) {
                    *summary = Some(StreamSearchSummary {
                        completed: true,
                        ..Default::default()
                    });
                    remaining -= 1;
                }
            }
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
                        if let Err(error) = known.merge_shard(
                            shard,
                            &summary.geo_columns_known,
                            &summary.filter_columns_known,
                        ) {
                            return fanout.cancel_with(error).await;
                        }
                        summaries[shard] = Some(summary);
                        fanout.mark_completed(shard);
                        remaining -= 1;
                    }
                    Some(_) => {
                        return fanout
                            .cancel_with(Status::internal(
                                "unexpected identity exchange on a legacy stream",
                            ))
                            .await
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
            .fanout_bm25_rescore_scores(
                terms,
                analysis_fingerprint,
                global,
                claims,
                rescore_ids,
                &[],
            )
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
                vector_normalized: None,
                bm25_normalized: None,
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
        field: &str,
    ) -> Result<HashMap<u64, f32>, Status> {
        if let Some(fields) = &self.field_permissions {
            fields.vector(field)?;
        }
        if self.has_clustered_vectors()
            && (!field.is_empty()
                || self.document_visibility.is_some()
                || self.field_permissions.is_some())
        {
            return Err(Status::failed_precondition(
                "scoped vector scoring requires a product-node field binding",
            ));
        }
        #[cfg(feature = "net")]
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
        if self.query_read_versions.is_none() {
            let (pinned, reads) = self.pin_read_versions().await?;
            let result = Box::pin(pinned.fanout_vector_rescore(vector, by_shard, field)).await;
            pinned.validate_read_versions(&reads).await?;
            return result;
        }
        let scope = crate::visibility::VisibilityScope::new(self.document_visibility.as_ref())?;
        let mut visibility_known = vec![false; scope.column_count()];
        let mut by_shard = by_shard;
        if !field.is_empty() || self.document_visibility.is_some() {
            for shard in 0..self.node_addrs.len() {
                by_shard.entry(shard as u32).or_default();
            }
        }
        let mut tasks = Vec::with_capacity(by_shard.len());
        for (shard, ids) in by_shard {
            let claim = self.query_read_versions.as_ref().expect("pinned reads")[shard as usize];
            let requested: std::collections::HashSet<u64> = ids.iter().copied().collect();
            let request = VectorRescoreRequest {
                field: field.into(),
                visibility: self.document_visibility.clone(),
                expected_stats_epoch: claim.epoch,
                expected_stats_incarnation: claim.incarnation(),
                vector: vector.to_vec(),
                candidate_ids: ids,
            };
            let mut client = self.node_client(&self.node_addrs[shard as usize])?;
            tasks.push((
                shard as usize,
                requested,
                tokio::spawn(async move {
                    client.vector_rescore(request).await.map(|r| r.into_inner())
                }),
            ));
        }
        let mut scores = HashMap::new();
        let mut held_binding = None;
        for (shard, requested, task) in tasks {
            let response = task
                .await
                .map_err(|e| Status::internal(format!("vector rescore task failed: {e}")))??;
            Self::check_vector_binding(field, response.vector_binding.as_ref(), &mut held_binding)?;
            self.check_read_view(shard, &scope, &response, &mut visibility_known)?;
            for hit in response.hits {
                if !requested.contains(&hit.doc_id)
                    || !hit.score.is_finite()
                    || scores.insert(hit.doc_id, hit.score).is_some()
                {
                    return Err(Status::failed_precondition(
                        "vector rescore returned an unrequested, duplicate or invalid score",
                    ));
                }
            }
        }
        self.check_visibility_columns(&visibility_known)?;
        Ok(scores)
    }

    /// Candidate-scoped BM25 fan-out (the cascade phase-2 seam),
    /// reduced to doc -> score. Docs absent from the response match no
    /// query term and score exactly 0.
    async fn fanout_bm25_rescore_scores(
        &self,
        terms: &[String],
        analysis_fingerprint: u64,
        global: &CorpusStats,
        claims: &[StatsClaim],
        by_shard: HashMap<u32, Vec<u64>>,
        score_stages: &[crate::pb::ScoreStage],
    ) -> Result<HashMap<u64, f32>, Status> {
        let (scores, _) = self
            .bm25_rescore_round(
                terms,
                analysis_fingerprint,
                global,
                claims,
                &by_shard,
                score_stages,
            )
            .await?;
        Ok(scores
            .into_iter()
            .map(|(id, score)| (id, score as f32))
            .collect())
    }

    /// Candidate-scoped lexical scoring shared by decomposed, cascade and
    /// legacy boosts. Admission precedes scoring; every result carries the
    /// authority view and the same physical claim as its global statistics.
    #[allow(clippy::too_many_arguments)]
    async fn bm25_rescore_round(
        &self,
        terms: &[String],
        analysis_fingerprint: u64,
        global: &CorpusStats,
        claims: &[StatsClaim],
        by_shard: &HashMap<u32, Vec<u64>>,
        score_stages: &[crate::pb::ScoreStage],
    ) -> Result<(HashMap<u64, f64>, HashMap<u32, (f32, u32)>), Status> {
        if let Some(fields) = &self.field_permissions {
            fields.lexical_scores(score_stages)?;
        }
        if claims.len() != self.node_addrs.len() {
            return Err(Status::failed_precondition(
                "BM25 rescore needs the complete statistics read set",
            ));
        }
        let scope = crate::visibility::VisibilityScope::new(self.document_visibility.as_ref())?;
        let mut visibility_known = vec![false; scope.column_count()];
        let mut by_shard = by_shard.clone();
        if self.document_visibility.is_some() || !score_stages.is_empty() {
            // Empty owners still acknowledge authority and stage columns.
            for shard in 0..self.node_addrs.len() {
                by_shard.entry(shard as u32).or_default();
            }
        }
        let mut tasks = Vec::with_capacity(by_shard.len());
        for (shard, ids) in by_shard {
            let node = self.node_addrs.get(shard as usize).ok_or_else(|| {
                Status::failed_precondition("BM25 rescore candidate owner is outside the read set")
            })?;
            let requested: std::collections::HashSet<_> = ids.iter().copied().collect();
            let request = Bm25RescoreRequest {
                analysis_fingerprint,
                terms: terms.to_vec(),
                global_doc_count: global.doc_count,
                global_total_doc_length: global.total_doc_length,
                global_doc_frequencies: global.dfs.clone(),
                candidate_ids: ids,
                k1: self.bm25_params.k1 as f32,
                b: self.bm25_params.b as f32,
                expected_stats_epoch: claims[shard as usize].epoch,
                expected_stats_incarnation: claims[shard as usize].incarnation(),
                score_stages: score_stages.to_vec(),
                visibility: self.document_visibility.clone(),
            };
            let mut client = self.node_client(node)?;
            tasks.push((
                shard,
                requested,
                tokio::spawn(async move {
                    let started = std::time::Instant::now();
                    client.bm25_rescore(request).await.map(|response| {
                        (started.elapsed().as_secs_f32() * 1e3, response.into_inner())
                    })
                }),
            ));
        }
        let mut scores = HashMap::new();
        let mut debug = HashMap::new();
        let mut stage_known = vec![false; score_stages.len()];
        for (shard, requested, task) in tasks {
            let (rpc_ms, response) = task.await.map_err(|error| {
                Status::internal(format!("BM25 rescore task failed: {error}"))
            })??;
            let claim =
                self.check_read_view(shard as usize, &scope, &response, &mut visibility_known)?;
            if claim != claims[shard as usize] {
                return Err(Status::failed_precondition(
                    "BM25 rescore changed its statistics read version",
                ));
            }
            if response.stage_columns_known.len() != stage_known.len() {
                return Err(Status::failed_precondition(
                    "BM25 rescore stage-column handshake has the wrong length",
                ));
            }
            for (held, present) in stage_known.iter_mut().zip(&response.stage_columns_known) {
                *held |= present;
            }
            debug.insert(shard, (rpc_ms, response.hits.len() as u32));
            for hit in response.hits {
                if !requested.contains(&hit.doc_id)
                    || !hit.score.is_finite()
                    || scores.insert(hit.doc_id, f64::from(hit.score)).is_some()
                {
                    return Err(Status::failed_precondition(
                        "BM25 rescore returned an unrequested, duplicate or nonfinite score",
                    ));
                }
            }
        }
        self.check_visibility_columns(&visibility_known)?;
        for (stage, known) in score_stages.iter().zip(stage_known) {
            if !known {
                return Err(Status::invalid_argument(format!(
                    "no shard has numeric column {}: the score stage would be a silent no-op",
                    stage.column,
                )));
            }
        }
        Ok((scores, debug))
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
        self.check_vector_scan(filters, false)?;
        if self.scoped_vector_scan() && self.query_read_versions.is_none() {
            let (pinned, reads) = self.pin_read_versions().await?;
            let result =
                Box::pin(pinned.fanout_search(request_id, vector, k, tie_complete, filters)).await;
            pinned.validate_read_versions(&reads).await?;
            return result;
        }
        let n_nodes = self.node_addrs.len();
        if n_nodes == 0 {
            return Err(Status::failed_precondition("no shard nodes configured"));
        }

        let admission = self.vector_read_barrier()?;
        let mut workers = tokio::task::JoinSet::new();
        let mask = self.vector_scan_mask(filters.tree.as_ref());
        let ctx = ShardQueryCtx {
            admission: admission.clone(),
            request_id: Arc::from(request_id),
            vector: Arc::new(vector.to_vec()),
            k,
            tie_complete,
            collapse: false,
            filters: Arc::new(filters.clone()),
            shard_filters: Arc::new(self.shard_filter_trees(filters, mask.as_ref())),
            tracker: Arc::new(Mutex::new(FloorTracker::new())),
            gfloor: Arc::new(watch::channel(f32::NEG_INFINITY).0),
            hedges: Arc::new(AtomicU64::new(0)),
            hedge_wins: Arc::new(AtomicU64::new(0)),
        };
        let (hedges, hedge_wins) = (Arc::clone(&ctx.hedges), Arc::clone(&ctx.hedge_wins));
        let mut known = Self::filter_known(filters, mask.as_ref());
        let active = (0..n_nodes)
            .filter(|shard| !mask.as_ref().is_some_and(|m| m.skipped[*shard]))
            .count();

        let (done_tx, mut done_rx) =
            mpsc::channel::<(u32, f32, Result<SearchShardDone, Status>)>(n_nodes);
        for shard in 0..n_nodes {
            if mask.as_ref().is_some_and(|m| m.skipped[shard]) {
                continue;
            }
            let primary = self.node_client(&self.node_addrs[shard])?;
            let replica = match self.replica_addrs.get(shard).and_then(|r| r.as_deref()) {
                Some(addr) => Some(self.node_client(addr)?),
                None => None,
            };
            let ctx = ctx.clone();
            let limits = self.limits();
            let done_tx = done_tx.clone();
            workers.spawn(async move {
                let t0 = std::time::Instant::now();
                let admission = ctx.admission.clone();
                let result =
                    run_shard_with_hedge(shard as u32, primary, replica, ctx, limits).await;
                if let (Some(admission), Err(error)) = (admission, &result) {
                    admission.fail(error.clone());
                }
                let wall_ms = t0.elapsed().as_secs_f32() * 1e3;
                let _ = done_tx.send((shard as u32, wall_ms, result)).await;
            });
        }
        drop(done_tx);

        let mut shard_hits: Vec<(u32, Vec<(u64, f32)>)> = Vec::with_capacity(n_nodes);
        let mut identities = HashMap::new();
        let mut shard_stats: Vec<Option<ShardScanStats>> = Vec::with_capacity(n_nodes);
        let mut shard_wall_ms: Vec<(u32, f32)> = Vec::with_capacity(n_nodes);
        // A skipped shard is a shard with no matching row: no hits, no
        // stats, no wall time.
        for shard in 0..n_nodes {
            if mask.as_ref().is_some_and(|m| m.skipped[shard]) {
                shard_hits.push((shard as u32, Vec::new()));
                shard_stats.push(None);
                shard_wall_ms.push((shard as u32, 0.0));
            }
        }
        for _ in 0..active {
            match done_rx.recv().await {
                Some((shard, wall_ms, Ok(done))) => {
                    known.merge_shard(
                        shard as usize,
                        &done.geo_columns_known,
                        &done.filter_columns_known,
                    )?;
                    for hit in &done.hits {
                        if let Some(identity) = hit.identity.as_ref() {
                            identities.insert((shard, hit.vector_id), identity.clone());
                        }
                    }
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
                    return Err(Status::new(
                        if admission.is_some() {
                            e.code()
                        } else {
                            tonic::Code::Internal
                        },
                        format!("shard {shard} failed: {e}"),
                    ));
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
                identity: identities.get(&(h.shard, h.vector_id)).cloned(),
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
    pub(crate) fn open_stream_fanout(
        &self,
        request_id: &str,
        vector: &[f32],
        initial_floor: Option<f32>,
        collapse_parents: bool,
        filters: &RequestFilters,
    ) -> Result<StreamFanout, Status> {
        self.open_stream_fanout_with_identities(
            request_id,
            vector,
            initial_floor,
            collapse_parents,
            filters,
            None,
        )
    }

    pub(crate) fn open_stream_fanout_with_identities(
        &self,
        request_id: &str,
        vector: &[f32],
        initial_floor: Option<f32>,
        collapse_parents: bool,
        filters: &RequestFilters,
        identity_limits: Option<crate::pb::StreamIdentityLimits>,
    ) -> Result<StreamFanout, Status> {
        self.check_vector_scan(filters, collapse_parents)?;
        let admission = self.vector_read_barrier()?;
        let n_nodes = self.node_addrs.len();
        let udp_socket = self.floor_socket().cloned();
        let (merged_tx, merged_rx) =
            mpsc::channel::<(usize, Result<Option<StreamSearchResponse>, Status>)>(4 * n_nodes);
        let mut floor_txs: Vec<Option<mpsc::Sender<StreamSearchRequest>>> =
            Vec::with_capacity(n_nodes);
        let mut udp_lanes: Vec<Option<(u64, std::net::SocketAddr)>> = Vec::with_capacity(n_nodes);
        let mut readers = tokio::task::JoinSet::new();
        let mask = self.vector_scan_mask(filters.tree.as_ref());
        for shard in 0..n_nodes {
            if mask.as_ref().is_some_and(|m| m.skipped[shard]) {
                floor_txs.push(None);
                udp_lanes.push(None);
                continue;
            }
            let mut client = self.node_client(&self.node_addrs[shard])?;
            let lane = self
                .floor_target(&self.node_addrs[shard])
                .map(|target| (floor_token(), target));
            let (req_tx, req_rx) = mpsc::channel::<StreamSearchRequest>(64);
            req_tx
                .try_send(StreamSearchRequest {
                    payload: Some(stream_search_request::Payload::Start(StartStreamSearch {
                        read_context: admission
                            .as_ref()
                            .map(|barrier| barrier.context(shard))
                            .transpose()?,
                        request_id: request_id.to_string(),
                        vector: vector.to_vec(),
                        initial_floor,
                        floor_token: lane.map_or(0, |(token, _)| token),
                        collapse_parents,
                        geo_filters: filters.geo.clone(),
                        filter: Self::shard_filter_tree(filters, mask.as_ref(), shard),
                        identity_limits: identity_limits.clone(),
                    })),
                })
                .expect("fresh channel accepts the Start message");
            floor_txs.push(Some(req_tx));
            udp_lanes.push(lane);
            let merged_tx = merged_tx.clone();
            let admission = admission.clone();
            let deadline = self.limits().shard_deadline.filter(|_| admission.is_some());
            readers.spawn(async move {
                let read = async {
                    let mut inbound = client
                        .stream_search(Request::new(ReceiverStream::new(req_rx)))
                        .await?
                        .into_inner();
                    if let Some(admission) = &admission {
                        admission.admit(shard, &mut inbound).await?;
                    }
                    loop {
                        match crate::vector_read::next(&mut inbound).await {
                            Ok(Some(msg)) => {
                                if merged_tx.send((shard, Ok(Some(msg)))).await.is_err() {
                                    return Ok(());
                                }
                            }
                            Ok(None) => {
                                let _ = merged_tx.send((shard, Ok(None))).await;
                                return Ok(());
                            }
                            Err(e) => return Err(e),
                        }
                    }
                };
                let result = if let Some(deadline) = deadline {
                    tokio::time::timeout(deadline, read)
                        .await
                        .unwrap_or_else(|_| {
                            Err(Status::deadline_exceeded(
                                "vector stream exceeded shard deadline",
                            ))
                        })
                } else {
                    read.await
                };
                if let Err(error) = result {
                    if let Some(admission) = &admission {
                        admission.fail(error.clone());
                    }
                    let _ = merged_tx.send((shard, Err(error))).await;
                }
            });
        }
        Ok(StreamFanout {
            scoped: admission.is_some(),
            readers,
            merged_rx,
            floor_txs,
            udp_lanes,
            udp_socket,
            udp_key: self.udp_hmac_key.clone(),
            udp_seq: Arc::new(std::sync::atomic::AtomicU32::new(0)),
        })
    }

    /// Push a floor raise to every still-open stream of `fanout`: UDP
    /// first (the fast lossy copy), then the reliable stream. Both are
    /// monotone max-folds shard-side, so double delivery and loss are
    /// equally free; a full stream channel just means the next raise
    /// supersedes this one.
    pub(crate) fn push_stream_floor(&self, fanout: &StreamFanout, floor: f32) {
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
                fanout.send_signal(
                    socket,
                    target,
                    crate::stream_signal::encode_floor(token, floor),
                );
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
        self.check_vector_scan(filters, false)?;
        if self.scoped_vector_scan() && self.query_read_versions.is_none() {
            let (pinned, reads) = self.pin_read_versions().await?;
            let result = Box::pin(pinned.fanout_stream_search(
                request_id,
                vector,
                k,
                initial_floor,
                filters,
            ))
            .await;
            pinned.validate_read_versions(&reads).await?;
            return result;
        }
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
            if self.scoped_vector_scan() {
                return Err(Status::invalid_argument(
                    "a scoped vector scan requires positive k",
                ));
            }
            return Ok(StreamFanoutResult {
                hits: Vec::new(),
                summaries: Vec::new(),
                floors_sent: 0,
            });
        }

        let mask = self.vector_scan_mask(filters.tree.as_ref());
        let mut known = Self::filter_known(filters, mask.as_ref());
        let identity_limits = crate::pb::StreamIdentityLimits {
            max_rows: k,
            max_response_bytes: 32 * 1024 * 1024,
            timeout_ms: 60_000,
        };
        crate::query_identity::validate_limits(&identity_limits)?;
        let mut fanout = self.open_stream_fanout_with_identities(
            request_id,
            vector,
            initial_floor,
            false,
            filters,
            Some(identity_limits.clone()),
        )?;

        // The global top-k: a max-heap whose top is the WORST survivor
        // under the merge's total order, so peek() is the k-th best.
        let mut heap: std::collections::BinaryHeap<StreamHeapEntry> =
            std::collections::BinaryHeap::with_capacity(k as usize + 1);
        let mut summaries: Vec<Option<StreamSearchSummary>> = vec![None; n_nodes];
        let mut remaining = n_nodes;
        for (shard, summary) in summaries.iter_mut().enumerate() {
            if mask.as_ref().is_some_and(|m| m.skipped[shard]) {
                *summary = Some(StreamSearchSummary {
                    completed: true,
                    ..Default::default()
                });
                remaining -= 1;
            }
        }
        let mut terminal = summaries.clone();
        let mut last_floor = initial_floor.unwrap_or(f32::NEG_INFINITY);
        let mut floors_sent = 0u64;
        let mut scoring_fingerprint: Option<String> = None;
        while remaining > 0 {
            let (shard, msg) = match fanout.next_message(&terminal).await {
                Ok(Some(pair)) => pair,
                Ok(None) => continue,
                Err(status) => return fanout.cancel_with(status).await,
            };
            match msg.payload {
                Some(stream_search_response::Payload::Batch(batch)) => {
                    if summaries[shard].is_some() {
                        return fanout
                            .cancel_with(Status::internal("candidate batch after IdentityReady"))
                            .await;
                    }
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
                Some(stream_search_response::Payload::IdentityReady(ready)) => {
                    if summaries[shard].is_some() {
                        return fanout
                            .cancel_with(Status::internal("duplicate IdentityReady"))
                            .await;
                    }
                    let Some(summary) = ready.scan else {
                        return fanout
                            .cancel_with(Status::internal("IdentityReady has no scan certificate"))
                            .await;
                    };
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
                    if let Err(e) = known.merge_shard(
                        shard,
                        &summary.geo_columns_known,
                        &summary.filter_columns_known,
                    ) {
                        return fanout.cancel_with(e).await;
                    }
                    summaries[shard] = Some(summary);
                    remaining -= 1;
                }
                Some(stream_search_response::Payload::Summary(_)) => {
                    return fanout
                        .cancel_with(Status::failed_precondition(
                            "stream ended without snapshot-bound identity support or completion",
                        ))
                        .await;
                }
                Some(stream_search_response::Payload::Identities(_))
                | Some(stream_search_response::Payload::ReadReady(_))
                | None => {
                    return fanout
                        .cancel_with(Status::internal("unexpected message before IdentityReady"))
                        .await;
                }
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
        let identities = fanout
            .resolve_identities(&all, &summaries, &mut terminal, &identity_limits)
            .await?;
        Ok(StreamFanoutResult {
            hits: all
                .into_iter()
                .map(|h| ScoredHit {
                    vector_id: h.vector_id,
                    score: h.score,
                    parent_id: 0,
                    identity: identities.get(&(h.shard, h.vector_id)).cloned().flatten(),
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
        self.check_vector_scan(filters, true)?;
        if self.scoped_vector_scan() && self.query_read_versions.is_none() {
            let (pinned, reads) = self.pin_read_versions().await?;
            let result =
                Box::pin(pinned.fanout_stream_search_collapse(request_id, vector, k, filters))
                    .await;
            pinned.validate_read_versions(&reads).await?;
            return result;
        }
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
            if self.scoped_vector_scan() {
                return Err(Status::invalid_argument(
                    "a scoped vector scan requires positive k",
                ));
            }
            return Ok(CollapseStreamResult {
                hits: Vec::new(),
                groups: Vec::new(),
                chunk_floor: f32::NEG_INFINITY,
                summaries: Vec::new(),
                floors_sent: 0,
            });
        }
        let mask = self.vector_scan_mask(filters.tree.as_ref());
        let mut known = Self::filter_known(filters, mask.as_ref());
        let mut fanout = self.open_stream_fanout(request_id, vector, None, true, filters)?;
        let mut parents: HashMap<u64, ParentAgg> = HashMap::new();
        let mut summaries: Vec<Option<StreamSearchSummary>> = vec![None; n_nodes];
        let mut remaining = n_nodes;
        for (shard, summary) in summaries.iter_mut().enumerate() {
            if mask.as_ref().is_some_and(|m| m.skipped[shard]) {
                *summary = Some(StreamSearchSummary {
                    completed: true,
                    ..Default::default()
                });
                remaining -= 1;
            }
        }
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
                    if let Err(e) = known.merge_shard(
                        shard,
                        &summary.geo_columns_known,
                        &summary.filter_columns_known,
                    ) {
                        return fanout.cancel_with(e).await;
                    }
                    summaries[shard] = Some(summary);
                    fanout.mark_completed(shard);
                    remaining -= 1;
                }
                Some(_) => {
                    return fanout
                        .cancel_with(Status::internal(
                            "unexpected identity exchange on a legacy stream",
                        ))
                        .await
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
                identity: None,
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
                        identity: None,
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
        self.check_vector_scan(filters, true)?;
        if self.scoped_vector_scan() && self.query_read_versions.is_none() {
            let (pinned, reads) = self.pin_read_versions().await?;
            let result =
                Box::pin(pinned.fanout_search_collapse(request_id, vector, k, filters)).await;
            pinned.validate_read_versions(&reads).await?;
            return result;
        }
        let n_nodes = self.node_addrs.len();
        if n_nodes == 0 {
            return Err(Status::failed_precondition("no shard nodes configured"));
        }
        let admission = self.vector_read_barrier()?;
        let mut workers = tokio::task::JoinSet::new();
        let mask = self.vector_scan_mask(filters.tree.as_ref());
        let ctx = ShardQueryCtx {
            admission: admission.clone(),
            request_id: Arc::from(request_id),
            vector: Arc::new(vector.to_vec()),
            k,
            tie_complete: false,
            collapse: true,
            filters: Arc::new(filters.clone()),
            shard_filters: Arc::new(self.shard_filter_trees(filters, mask.as_ref())),
            tracker: Arc::new(Mutex::new(FloorTracker::new())),
            gfloor: Arc::new(watch::channel(f32::NEG_INFINITY).0),
            hedges: Arc::new(AtomicU64::new(0)),
            hedge_wins: Arc::new(AtomicU64::new(0)),
        };
        let (hedges, hedge_wins) = (Arc::clone(&ctx.hedges), Arc::clone(&ctx.hedge_wins));
        let mut known = Self::filter_known(filters, mask.as_ref());
        let active = (0..n_nodes)
            .filter(|shard| !mask.as_ref().is_some_and(|m| m.skipped[*shard]))
            .count();

        let (done_tx, mut done_rx) =
            mpsc::channel::<(u32, f32, Result<SearchShardDone, Status>)>(n_nodes);
        for shard in 0..n_nodes {
            if mask.as_ref().is_some_and(|m| m.skipped[shard]) {
                continue;
            }
            let primary = self.node_client(&self.node_addrs[shard])?;
            let replica = match self.replica_addrs.get(shard).and_then(|r| r.as_deref()) {
                Some(addr) => Some(self.node_client(addr)?),
                None => None,
            };
            let ctx = ctx.clone();
            let limits = self.limits();
            let done_tx = done_tx.clone();
            workers.spawn(async move {
                let t0 = std::time::Instant::now();
                let admission = ctx.admission.clone();
                let result =
                    run_shard_with_hedge(shard as u32, primary, replica, ctx, limits).await;
                if let (Some(admission), Err(error)) = (admission, &result) {
                    admission.fail(error.clone());
                }
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
        // A skipped shard is a shard with no matching row: no hits, no
        // stats, no wall time.
        for shard in 0..n_nodes {
            if mask.as_ref().is_some_and(|m| m.skipped[shard]) {
                shard_hits.push((shard as u32, Vec::new()));
                shard_stats.push(None);
                shard_wall_ms.push((shard as u32, 0.0));
            }
        }
        for _ in 0..active {
            match done_rx.recv().await {
                Some((shard, wall_ms, Ok(done))) => {
                    known.merge_shard(
                        shard as usize,
                        &done.geo_columns_known,
                        &done.filter_columns_known,
                    )?;
                    shard_hits.push((
                        shard,
                        done.hits.iter().map(|h| (h.vector_id, h.score)).collect(),
                    ));
                    for hit in done.hits {
                        match best.entry(hit.parent_id) {
                            std::collections::hash_map::Entry::Vacant(entry) => {
                                entry.insert(hit);
                            }
                            std::collections::hash_map::Entry::Occupied(mut entry) => {
                                let previous = entry.get();
                                if hit.score > previous.score
                                    || (hit.score == previous.score
                                        && hit.vector_id < previous.vector_id)
                                {
                                    entry.insert(hit);
                                }
                            }
                        }
                    }
                    shard_stats.push(done.stats);
                    shard_wall_ms.push((shard, wall_ms));
                }
                Some((shard, _, Err(e))) => {
                    return Err(Status::new(
                        if admission.is_some() {
                            e.code()
                        } else {
                            tonic::Code::Internal
                        },
                        format!("shard {shard} failed: {e}"),
                    ));
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
        let analysis_fingerprint = crate::analyzer::analysis_fingerprint(spec);
        if k == 0 || vector.is_empty() {
            return Ok((Vec::new(), None));
        }
        let t_total = std::time::Instant::now();
        // Phase 1: floor-shared, tie-complete vector candidates.
        let t_legs = std::time::Instant::now();
        // Phase 1 carries the filters, so the candidate gate is the
        // filtered corpus; phase 2 reranks that pool and never widens
        // it, so no unfiltered document can reappear.
        #[cfg(feature = "net")]
        let clustered_phase1: Option<FanoutResult> = if self.clustered_vectors.is_some() {
            Some({
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
            })
        } else {
            None
        };
        #[cfg(not(feature = "net"))]
        let clustered_phase1: Option<FanoutResult> = None;
        let phase1 = match clustered_phase1 {
            Some(phase1) => phase1,
            None => {
                self.fanout_search(request_id, vector, k, true, filters)
                    .await?
            }
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
        // refusal reruns them once with fresh stats and a new fenced claim).
        let t = std::time::Instant::now();
        let mut by_shard: std::collections::HashMap<u32, Vec<u64>> =
            std::collections::HashMap::new();
        for (doc_id, shard, _) in &pool {
            by_shard.entry(*shard).or_default().push(*doc_id);
        }
        let mut fresh = false;
        let (stats_ms, t_rescore, bm25_of, rescore_debug) = loop {
            let (global, epochs) = self.body_stats(&terms, fresh).await?;
            let claims = epochs;
            let stats_ms = t.elapsed().as_secs_f32() * 1e3;
            let t_rescore = std::time::Instant::now();
            match self
                .bm25_rescore_round(
                    &terms,
                    analysis_fingerprint,
                    &global,
                    &claims,
                    &by_shard,
                    &[],
                )
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
        let analysis_fingerprint = crate::analyzer::analysis_fingerprint(spec);
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
        self.lexical_signal_terms(&terms, analysis_fingerprint, ids, None)
            .await
    }

    /// Candidate-scoped lexical scoring when the planner already analyzed the
    /// clause during membership resolution. `expected_epochs` closes the gap
    /// between that bitmap and this rescore: any lexical mutation aborts the
    /// plan so the caller can rebuild it once from a fresh snapshot.
    pub async fn lexical_signal_terms(
        &self,
        terms: &[String],
        analysis_fingerprint: u64,
        ids: &[u64],
        expected_epochs: Option<&[StatsClaim]>,
    ) -> Result<HashMap<u64, f32>, Status> {
        self.lexical_signal_terms_with_stages(
            terms,
            analysis_fingerprint,
            ids,
            expected_epochs,
            &[],
        )
        .await
    }

    /// [`Self::lexical_signal_terms`] with the ordinary lexical score-stage
    /// chain applied on each owning shard before the final f32 conversion.
    pub async fn lexical_signal_terms_with_stages(
        &self,
        terms: &[String],
        analysis_fingerprint: u64,
        ids: &[u64],
        expected_epochs: Option<&[StatsClaim]>,
        score_stages: &[crate::pb::ScoreStage],
    ) -> Result<HashMap<u64, f32>, Status> {
        if terms.is_empty() || ids.is_empty() {
            return Ok(HashMap::new());
        }
        let by_shard: HashMap<u32, Vec<u64>> = (0..self.node_addrs.len())
            .map(|s| (s as u32, ids.to_vec()))
            .collect();
        // Stats + rescore run as a round (a stale-stats refusal reruns
        // them once with fresh stats and a new fenced claim) — the same protocol
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
            let claims = epochs;
            match self
                .fanout_bm25_rescore_scores(
                    terms,
                    analysis_fingerprint,
                    &global,
                    &claims,
                    by_shard.clone(),
                    score_stages,
                )
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
        field: &str,
    ) -> Result<HashMap<u64, f32>, Status> {
        if let Some(fields) = &self.field_permissions {
            fields.vector(field)?;
        }
        if self.has_clustered_vectors()
            && (!field.is_empty()
                || self.document_visibility.is_some()
                || self.field_permissions.is_some())
        {
            return Err(Status::failed_precondition(
                "scoped vector scoring requires a product-node field binding",
            ));
        }
        if vector.is_empty() {
            return Err(Status::invalid_argument(
                "a dense boost needs a non-empty vector",
            ));
        }
        if ids.is_empty() && field.is_empty() && self.document_visibility.is_none() {
            return Ok(HashMap::new());
        }
        #[cfg(feature = "net")]
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
        self.fanout_vector_rescore(vector, by_shard, field).await
    }

    /// Score a fixed candidate set against the product-owned original FP32
    /// rows. Every requested id must resolve exactly once across the product
    /// shards. Selection may come from the embedded or clustered provider;
    /// stable labels route back to these product-owned rows in either case.
    pub(crate) async fn exact_vector_scores(
        &self,
        vector: &[f32],
        ids: &[u64],
        field: &str,
    ) -> Result<ExactRerankScores, Status> {
        if let Some(fields) = &self.field_permissions {
            fields.vector(field)?;
        }
        if self.query_read_versions.is_none() {
            let (pinned, reads) = self.pin_read_versions().await?;
            let result = Box::pin(pinned.exact_vector_scores(vector, ids, field)).await;
            pinned.validate_read_versions(&reads).await?;
            return result;
        }
        let scope = crate::visibility::VisibilityScope::new(self.document_visibility.as_ref())?;
        let mut visibility_known = vec![false; scope.column_count()];
        if vector.is_empty() {
            return Err(Status::invalid_argument(
                "FP32 rerank needs a non-empty query vector",
            ));
        }
        if ids.is_empty() && field.is_empty() && self.document_visibility.is_none() {
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
        for (shard, addr) in self.node_addrs.iter().enumerate() {
            let claim = self.query_read_versions.as_ref().expect("pinned reads")[shard];
            let request = ExactVectorRescoreRequest {
                field: field.into(),
                visibility: self.document_visibility.clone(),
                expected_stats_epoch: claim.epoch,
                expected_stats_incarnation: claim.incarnation(),
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
        let mut held_binding = None;
        for (shard, task) in tasks.into_iter().enumerate() {
            let response = task.await.map_err(|e| {
                Status::internal(format!("exact vector rescore task failed: {e}"))
            })??;
            Self::check_vector_binding(field, response.vector_binding.as_ref(), &mut held_binding)?;
            self.check_read_view(shard, &scope, &response, &mut visibility_known)?;
            observed_bytes = observed_bytes
                .checked_add(response.logical_bytes)
                .ok_or_else(|| Status::internal("exact rerank byte metrics overflow"))?;
            pages_touched = pages_touched
                .checked_add(response.pages_touched)
                .ok_or_else(|| Status::internal("exact rerank page metrics overflow"))?;
            worker_tasks = worker_tasks.saturating_add(response.tasks);
            for hit in response.hits {
                if requested.binary_search(&hit.doc_id).is_err()
                    || !hit.score.is_finite()
                    || scores.insert(hit.doc_id, hit.score).is_some()
                {
                    return Err(Status::failed_precondition(format!(
                        "FP32 rerank returned an unrequested, duplicate or invalid score for candidate {}",
                        hit.doc_id
                    )));
                }
            }
        }
        self.check_visibility_columns(&visibility_known)?;
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
        self.fetch_values_impl(
            ids,
            projections,
            stages,
            self.query_read_versions.as_deref().map(Vec::as_slice),
            false,
        )
        .await
    }

    /// Fetch against versions captured during selection. Claims must cover
    /// every node in this pinned coordinator's order; an absent claim is an
    /// error, not permission to read a newer generation.
    pub async fn fetch_values_at(
        &self,
        ids: &[u64],
        projections: &[crate::pb::CompiledProjection],
        stages: &[crate::pb::ScoreStage],
        epochs: &[StatsClaim],
    ) -> Result<FetchedValues, Status> {
        if epochs.len() != self.node_addrs.len() || epochs.iter().any(|claim| claim.epoch == 0) {
            return Err(Status::failed_precondition(
                "candidate fetch requires one complete selection claim per node",
            ));
        }
        for claim in epochs {
            StatsClaim::required(claim.epoch, &claim.incarnation())?;
        }
        self.fetch_values_impl(ids, projections, stages, Some(epochs), false)
            .await
    }

    /// Resolve identity and explicit absence for live, authorized candidate rows
    /// under a complete selection-time read set. Omitted rows were not served;
    /// callers publishing selected hits must require complete coverage.
    pub async fn resolve_candidate_identities_at(
        &self,
        ids: &[u64],
        epochs: &[StatsClaim],
    ) -> Result<HashMap<u64, Option<crate::pb::DocumentIdentity>>, Status> {
        if self
            .field_permissions
            .as_ref()
            .is_some_and(|fields| !fields.can_disclose_identity())
        {
            return Err(Status::permission_denied(
                "document identity disclosure is not granted",
            ));
        }
        if epochs.len() != self.node_addrs.len() || epochs.iter().any(|claim| claim.epoch == 0) {
            return Err(Status::failed_precondition(
                "identity fetch requires one complete selection claim per node",
            ));
        }
        for claim in epochs {
            StatsClaim::required(claim.epoch, &claim.incarnation())?;
        }
        Ok(self
            .fetch_values_impl(ids, &[], &[], Some(epochs), true)
            .await?
            .identities)
    }

    async fn fetch_values_impl(
        &self,
        ids: &[u64],
        projections: &[crate::pb::CompiledProjection],
        stages: &[crate::pb::ScoreStage],
        epochs: Option<&[StatsClaim]>,
        include_identities: bool,
    ) -> Result<FetchedValues, Status> {
        if let Some(fields) = &self.field_permissions {
            fields.fetch_values(projections, stages)?;
        }
        let scope = crate::visibility::VisibilityScope::new(self.document_visibility.as_ref())?;
        let mut visibility_known = vec![false; scope.column_count()];
        let candidates: std::collections::HashSet<u64> = ids.iter().copied().collect();
        // Stage parameters validate here too, so a malformed stage is
        // refused by name before any fan-out.
        crate::node::parse_score_stages(stages)?;
        if include_identities && ids.len() > 1_000_000 {
            return Err(Status::resource_exhausted(
                "identity fetch exceeds 1000000 input IDs",
            ));
        }
        let mut identity_bytes = 0usize;
        let mut out = FetchedValues {
            identities: HashMap::new(),
            rows: HashMap::new(),
            stage_rows: vec![HashMap::new(); stages.len()],
            epochs: Vec::with_capacity(self.node_addrs.len()),
        };
        // An empty candidate list still fans out when anything was
        // named: the typo rules run on the flags, not the rows.
        if !include_identities
            && projections.is_empty()
            && stages.is_empty()
            && epochs.is_none()
            && self.document_visibility.is_none()
        {
            return Ok(out);
        }
        let mut tasks = tokio::task::JoinSet::new();
        for (shard, node) in self.node_addrs.iter().enumerate() {
            let claim = epochs.map_or(StatsClaim::default(), |epochs| epochs[shard]);
            let request = crate::pb::FetchValuesRequest {
                candidate_ids: ids.to_vec(),
                include_identities,
                projections: projections.to_vec(),
                stages: stages.to_vec(),
                visibility: self.document_visibility.clone(),
                expected_stats_epoch: claim.epoch,
                expected_stats_incarnation: claim.incarnation(),
            };
            let mut client = self.node_client(node)?;
            tasks.spawn(async move {
                (
                    shard,
                    client.fetch_values(request).await.map(|r| r.into_inner()),
                )
            });
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
        let mut projection_types = vec![crate::pb::ScalarValueType::Unspecified; projections.len()];
        // JoinSet aborts outstanding requests if this read is cancelled or a
        // peer fails. Keep deterministic node order when folding the replies.
        let mut responses = std::collections::BTreeMap::new();
        while let Some(joined) = tasks.join_next().await {
            let (shard, response) =
                joined.map_err(|e| Status::internal(format!("fetch values task failed: {e}")))?;
            responses.insert(shard, response?);
        }
        for (shard, resp) in responses {
            scope.validate_echo(&resp.visibility_fingerprint, &resp.visibility_columns_known)?;
            let claim = StatsClaim::required(resp.stats_epoch, &resp.stats_incarnation)?;
            if epochs.is_some_and(|epochs| epochs[shard] != claim) {
                return Err(Status::failed_precondition(
                    "candidate fetch returned a different selection version",
                ));
            }
            if resp.identities_included != include_identities
                || (!include_identities && !resp.identities.is_empty())
            {
                return Err(Status::failed_precondition(
                    "shard omitted or changed identity fetch certificate",
                ));
            }
            for row in resp.identities {
                if !candidates.contains(&row.doc_id) || out.identities.contains_key(&row.doc_id) {
                    return Err(Status::failed_precondition(
                        "candidate fetch returned an unrequested or duplicate identity",
                    ));
                }
                crate::query_identity::charge_candidate_identity(&row, &mut identity_bytes)?;
                out.identities.insert(row.doc_id, row.identity);
            }
            out.epochs.push(claim);
            for (known, shard) in visibility_known
                .iter_mut()
                .zip(&resp.visibility_columns_known)
            {
                *known |= shard;
            }
            if resp.projection_types.len() != projections.len()
                || resp.projection_leaves_known.len() != projection_leaves.len()
                || resp.stage_columns_known.len() != stages.len()
            {
                return Err(Status::failed_precondition("shard omitted projection type or known-column metadata; use matching server builds"));
            }
            crate::values::merge_projection_types(
                projections,
                &mut projection_types,
                &resp.projection_types,
            )?;
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
                if !candidates.contains(&row.doc_id) || out.rows.contains_key(&row.doc_id) {
                    return Err(Status::failed_precondition(
                        "candidate fetch returned an unrequested or duplicate row",
                    ));
                }
                if row.values.len() != projections.len() || row.stage_values.len() != stages.len() {
                    return Err(Status::failed_precondition(
                        "shard returned a projection row with the wrong width",
                    ));
                }
                crate::values::validate_projection_row(&row.values, &resp.projection_types)?;
                for (i, sv) in row.stage_values.iter().enumerate() {
                    match sv.value {
                        Some(crate::pb::projected_value::Value::DoubleValue(v)) => {
                            out.stage_rows[i].insert(row.doc_id, v);
                        }
                        None => {}
                        _ => {
                            return Err(Status::failed_precondition(
                                "candidate fetch returned a nonnumeric stage contribution",
                            ))
                        }
                    }
                }
                out.rows.insert(row.doc_id, row.values);
            }
        }
        self.check_visibility_columns(&visibility_known)?;
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
        let analysis_fingerprint = crate::analyzer::analysis_fingerprint(spec);
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
        // reruns them once with fresh stats and a new fenced claim).
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
                let claims = epochs;
                match self
                    .fanout_bm25_rescore_scores(
                        &terms,
                        analysis_fingerprint,
                        &global,
                        &claims,
                        by_shard.clone(),
                        &[],
                    )
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
        sort: &[crate::pb::BrowseSort],
        lexical_terms: &[String],
        analysis_fingerprint: u64,
        filters: &RequestFilters,
    ) -> Result<BrowseRows, Status> {
        use crate::sortkeys::{cmp_rows, Key, Value};
        let k = self.resolve_k(k)?;
        if let Some(fields) = &self.field_permissions {
            fields.browse(filters, sort, lexical_terms)?;
        }
        if self.query_read_versions.is_none() {
            let (pinned, reads) = self.pin_read_versions().await?;
            let result = Box::pin(pinned.fanout_browse(
                k,
                after,
                sort,
                lexical_terms,
                analysis_fingerprint,
                filters,
            ))
            .await;
            pinned.validate_read_versions(&reads).await?;
            return result;
        }
        let scope = crate::visibility::VisibilityScope::new(self.document_visibility.as_ref())?;
        let mut visibility_known = vec![false; scope.column_count()];
        let mask = self
            .document_visibility
            .is_none()
            .then(|| self.shard_mask(filters.tree.as_ref()))
            .flatten();
        let mut tasks = Vec::with_capacity(self.node_addrs.len());
        for (shard, node) in self.node_addrs.iter().enumerate() {
            if mask.as_ref().is_some_and(|m| m.skipped[shard]) {
                continue;
            }
            let claim = self.query_read_versions.as_ref().expect("pinned read set")[shard];
            let request = crate::pb::BrowseShardRequest {
                visibility: self.document_visibility.clone(),
                expected_stats_epoch: claim.epoch,
                expected_stats_incarnation: claim.incarnation(),
                analysis_fingerprint,
                k,
                after: after.as_ref().map_or(0, |a| a.id),
                first_page: after.is_none(),
                geo_filters: filters.geo.clone(),
                filter: Self::shard_filter_tree(filters, mask.as_ref(), shard),
                sort: sort.to_vec(),
                after_keys: after
                    .as_ref()
                    .map(|a| a.keys.iter().map(Key::to_pb).collect())
                    .unwrap_or_default(),
                lexical_terms: lexical_terms.to_vec(),
            };
            let client = self.node_client(node);
            tasks.push((
                shard,
                tokio::spawn(
                    async move { client?.browse_shard(request).await.map(|r| r.into_inner()) },
                ),
            ));
        }
        let mut known = Self::filter_known(filters, mask.as_ref());
        let mut sort_known = vec![false; sort.len()];
        let mut sort_types = vec![crate::pb::ScalarValueType::Unspecified; sort.len()];
        struct Row {
            keys: Vec<Key>,
            values: Vec<Value>,
            id: u64,
        }
        let mut rows: Vec<Row> = Vec::new();
        let mut prune = crate::segment_prune::PruneStats::default();
        for (shard, task) in tasks {
            let response = task
                .await
                .map_err(|e| Status::internal(format!("browse task failed: {e}")))??;
            self.check_read_view(shard, &scope, &response, &mut visibility_known)?;
            known.merge_shard(
                shard,
                &response.geo_columns_known,
                &response.filter_columns_known,
            )?;
            prune.add(crate::segment_prune::PruneStats {
                segments_total: response.segments_total,
                segments_skipped: response.segments_skipped,
            });
            if response.sort_columns_known.len() != sort.len()
                || response.sort_column_types.len() != sort.len()
            {
                return Err(Status::failed_precondition("shard omitted sorted column type metadata; all nodes must use the matching sort contract"));
            }
            for (i, (&known, &raw_type)) in response
                .sort_columns_known
                .iter()
                .zip(&response.sort_column_types)
                .enumerate()
            {
                let kind = crate::pb::ScalarValueType::try_from(raw_type).map_err(|_| {
                    Status::failed_precondition("shard returned an unknown sort column type")
                })?;
                if kind == crate::pb::ScalarValueType::Boolean
                    || known != (kind != crate::pb::ScalarValueType::Unspecified)
                {
                    return Err(Status::failed_precondition(
                        "shard sort column type disagrees with its known flag",
                    ));
                }
                if known {
                    if sort_known[i] && sort_types[i] != kind {
                        return Err(Status::failed_precondition(format!(
                            "sort column {:?} has incompatible types across shards: {:?} and {:?}",
                            sort[i].column, sort_types[i], kind
                        )));
                    }
                    sort_known[i] = true;
                    sort_types[i] = kind;
                }
            }
            if sort.is_empty() {
                rows.extend(response.doc_ids.iter().map(|&id| Row {
                    keys: Vec::new(),
                    values: Vec::new(),
                    id,
                }));
                continue;
            }
            if response.sort_rows.len() != response.doc_ids.len() {
                return Err(Status::internal(
                    "shard answered a sorted browse with mismatched key rows",
                ));
            }
            for (&id, row) in response.doc_ids.iter().zip(&response.sort_rows) {
                let keys: Option<Vec<Key>> = row.keys.iter().map(Key::from_pb).collect();
                let values: Option<Vec<Value>> = row
                    .values
                    .iter()
                    .map(crate::sortkeys::value_from_pb)
                    .collect();
                let (Some(keys), Some(values)) = (keys, values) else {
                    return Err(Status::internal(
                        "shard answered a sorted browse with an empty key",
                    ));
                };
                if keys.len() != sort.len() || values.len() != sort.len() {
                    return Err(Status::internal(format!(
                        "shard answered a sorted browse with {} keys for {} sort columns",
                        keys.len(),
                        sort.len()
                    )));
                }
                for (i, (key, value)) in keys.iter().zip(&values).enumerate() {
                    let kind = crate::pb::ScalarValueType::try_from(response.sort_column_types[i])
                        .expect("validated metadata");
                    if value.column_type() != kind || !crate::sortkeys::key_matches_type(key, kind)
                    {
                        return Err(Status::failed_precondition(
                            "shard sort row disagrees with its declared column type",
                        ));
                    }
                }
                rows.push(Row { keys, values, id });
            }
        }
        self.check_visibility_columns(&visibility_known)?;
        known.refuse_unknown(filters)?;
        for (sort, known) in sort.iter().zip(&sort_known) {
            if !known {
                return Err(Status::invalid_argument(format!(
                    "sort column {:?} is not declared on any shard's numeric, integer, unsigned integer, or \
                     facet table (--numeric-fields / --integer-fields / --unsigned-integer-fields / --facet-fields), \
                     and is not a lineage key (parent_id, group_id)",
                    sort.column
                )));
            }
        }
        let descending: Vec<bool> = sort.iter().map(|s| s.descending).collect();
        rows.sort_by(|a, b| cmp_rows(&a.keys, a.id, &b.keys, b.id, &descending));
        rows.truncate(k as usize);
        Ok(BrowseRows {
            prune,
            ids: rows.iter().map(|r| r.id).collect(),
            keys: rows.iter().map(|r| r.keys.clone()).collect(),
            values: rows.iter().map(|r| r.values.clone()).collect(),
            sorted: !sort.is_empty(),
        })
    }

    /// The lineage keys of every requested document across the shards:
    /// doc id to (parent_id, group_id). A document without lineage
    /// parents itself in the high-bit-tagged domain and has group 0. A
    /// deleted or unknown id is absent.
    pub async fn lineage_keys(&self, ids: &[u64]) -> Result<HashMap<u64, (u64, u64)>, Status> {
        self.read_lineage(ids, &crate::lineage::LineageSelection::new(&[])?, None)
            .await
    }

    /// Read exactly one lineage column, including its field-disclosure check.
    pub async fn lineage_key(
        &self,
        ids: &[u64],
        column: &str,
    ) -> Result<HashMap<u64, u64>, Status> {
        let field = match column {
            "parent_id" => crate::pb::LineageField::ParentId,
            "group_id" => crate::pb::LineageField::GroupId,
            _ => return Err(Status::invalid_argument("unknown lineage column")),
        };
        let selection = crate::lineage::LineageSelection::new(&[field as i32])?;
        Ok(self
            .read_lineage(ids, &selection, None)
            .await?
            .into_iter()
            .map(|(id, (parent, group))| {
                (
                    id,
                    if field == crate::pb::LineageField::ParentId {
                        parent
                    } else {
                        group
                    },
                )
            })
            .collect())
    }

    async fn read_lineage(
        &self,
        ids: &[u64],
        selection: &crate::lineage::LineageSelection,
        by_shard: Option<&HashMap<usize, Vec<u64>>>,
    ) -> Result<HashMap<u64, (u64, u64)>, Status> {
        if let Some(fields) = &self.field_permissions {
            for column in selection.columns() {
                fields.dictionary(column)?;
            }
        }
        let scope = crate::visibility::VisibilityScope::new(self.document_visibility.as_ref())?;
        let mut known = vec![false; scope.column_count()];
        let mut tasks = Vec::with_capacity(self.node_addrs.len());
        for (shard, node) in self.node_addrs.iter().enumerate() {
            let requested = match by_shard {
                None => ids,
                Some(owners) => owners.get(&shard).map_or(&[][..], Vec::as_slice),
            };
            if by_shard.is_some() && requested.is_empty() && self.document_visibility.is_none() {
                continue;
            }
            let claim = self
                .query_read_versions
                .as_ref()
                .map(|claims| claims[shard])
                .unwrap_or_default();
            let request = crate::pb::ResolveParentsRequest {
                doc_ids: requested.to_vec(),
                fields: selection.wire(),
                visibility: self.document_visibility.clone(),
                expected_stats_epoch: claim.epoch,
                expected_stats_incarnation: claim.incarnation(),
            };
            let mut client = self.node_client(node)?;
            let requested: std::collections::HashSet<u64> = requested.iter().copied().collect();
            tasks.push((
                shard,
                requested,
                tokio::spawn(async move {
                    client
                        .resolve_parents(request)
                        .await
                        .map(|r| r.into_inner())
                }),
            ));
        }
        let mut out = HashMap::with_capacity(ids.len());
        for (shard, requested, task) in tasks {
            let response = task
                .await
                .map_err(|e| Status::internal(format!("lineage task failed: {e}")))??;
            self.check_read_view(shard, &scope, &response, &mut known)?;
            selection.validate_echo(&response.fields)?;
            for resolved in response.parents {
                selection.validate_row(&resolved)?;
                if !requested.contains(&resolved.doc_id) {
                    return Err(Status::failed_precondition(
                        "lineage response returned an unrequested row",
                    ));
                }
                if out
                    .insert(resolved.doc_id, (resolved.parent_id, resolved.group_id))
                    .is_some()
                {
                    return Err(Status::failed_precondition(
                        "lineage response returned duplicate row ownership",
                    ));
                }
            }
        }
        self.check_visibility_columns(&known)?;
        Ok(out)
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

    fn membership_from_filter(
        response: crate::pb::FilterBitmapResponse,
    ) -> crate::pb::MembershipBitmapResponse {
        crate::pb::MembershipBitmapResponse {
            vector_binding: None,
            base_label: response.base_label,
            label_count: response.label_count,
            bits: response.bits,
            stats_epoch: response.stats_epoch,
            stats_incarnation: response.stats_incarnation,
            visibility_fingerprint: response.visibility_fingerprint,
            visibility_columns_known: response.visibility_columns_known,
            segments_total: response.segments_total,
            segments_skipped: response.segments_skipped,
        }
    }

    fn check_vector_binding(
        field: &str,
        actual: Option<&crate::pb::MappedVectorBinding>,
        held: &mut Option<Option<crate::pb::MappedVectorBinding>>,
    ) -> Result<(), Status> {
        crate::vector_read::check_binding(field, actual, held)
    }

    fn check_read_view(
        &self,
        shard: usize,
        scope: &crate::visibility::VisibilityScope,
        response: &impl crate::visibility::ScopedReadResponse,
        known: &mut [bool],
    ) -> Result<StatsClaim, Status> {
        let response = response.read_view();
        scope.validate_echo(response.fingerprint, response.columns_known)?;
        let claim = StatsClaim::required(response.epoch, response.incarnation)?;
        if let Some(expected) = &self.query_read_versions {
            if expected.get(shard) != Some(&claim) {
                return Err(Status::failed_precondition(
                    "query data changed during scoped read; restart from the first page",
                ));
            }
        }
        for (held, present) in known.iter_mut().zip(response.columns_known) {
            *held |= present;
        }
        Ok(claim)
    }

    /// Resolve the live document universe, or one CEL/geo predicate, without
    /// paging through `max_k`-sized browse responses.
    pub async fn filter_membership(
        &self,
        filters: &RequestFilters,
    ) -> Result<MembershipSet, Status> {
        if let Some(fields) = &self.field_permissions {
            fields.filter(&filters.geo, filters.tree.as_ref())?;
        }
        let scope = crate::visibility::VisibilityScope::new(self.document_visibility.as_ref())?;
        let mut visibility_known = vec![false; scope.column_count()];
        // Every shard supplies the mandatory column handshake, even when the
        // caller's predicate would prune it from the user-only membership.
        let mask = self
            .document_visibility
            .is_none()
            .then(|| self.shard_mask(filters.tree.as_ref()))
            .flatten();
        let mut tasks = Vec::with_capacity(self.node_addrs.len());
        for (shard, node) in self.node_addrs.iter().enumerate() {
            if mask.as_ref().is_some_and(|m| m.skipped[shard]) {
                continue;
            }
            let request = crate::pb::FilterBitmapRequest {
                visibility: self.document_visibility.clone(),
                geo_filters: filters.geo.clone(),
                filter: Self::shard_filter_tree(filters, mask.as_ref(), shard),
            };
            let client = self.node_client(node);
            tasks.push((
                shard,
                tokio::spawn(async move {
                    client?
                        .resolve_filter_bitmap(request)
                        .await
                        .map(|response| response.into_inner())
                }),
            ));
        }
        let mut known = Self::filter_known(filters, mask.as_ref());
        let mut merged = MembershipSet {
            epochs: vec![StatsClaim::default(); self.node_addrs.len()],
            ..Default::default()
        };
        for (shard, task) in tasks {
            let response = task.await.map_err(|error| {
                Status::internal(format!("filter membership task failed: {error}"))
            })??;
            known.merge_shard(
                shard,
                &response.geo_columns_known,
                &response.filter_columns_known,
            )?;
            merged.prune.add(crate::segment_prune::PruneStats {
                segments_total: response.segments_total,
                segments_skipped: response.segments_skipped,
            });
            let response = Self::membership_from_filter(response);
            merged.epochs[shard] =
                self.check_read_view(shard, &scope, &response, &mut visibility_known)?;
            Self::merge_membership_bitmap(&mut merged, &response)?;
        }
        self.check_visibility_columns(&visibility_known)?;
        known.refuse_unknown(filters)?;
        Ok(merged)
    }

    /// One shard-side Boolean evaluation over the fleet
    /// (`docs/query-api.md`, "Recursive boolean execution"). The root's
    /// MUST filter leaves are the AND spine the placement tree prunes
    /// shards by: a shard the spine excludes is not asked, and a leaf a
    /// shard's placement implies is dropped from that shard's copy of
    /// the leaf. Each consulted shard answers its best `depth` members;
    /// the merge is the ranked union cut to `depth`. `claims[shard]` is
    /// that shard's stats-epoch claim for the lexical leaves.
    pub(crate) async fn evaluate_boolean_fanout(
        &self,
        plan: &BooleanFanoutPlan,
        claims: &[StatsClaim],
    ) -> Result<BooleanFanout, Status> {
        use crate::pb::boolean_plan_leaf::Leaf as L;
        if claims.len() != self.node_addrs.len() {
            return Err(Status::internal(format!(
                "Boolean fan-out has {} epoch claims for {} shards",
                claims.len(),
                self.node_addrs.len()
            )));
        }
        let scope = crate::visibility::VisibilityScope::new(self.document_visibility.as_ref())?;
        let mut visibility_known = vec![false; scope.column_count()];
        let mut binding = None;
        let dense_fields: Vec<&str> = plan
            .leaves
            .iter()
            .filter_map(|leaf| match leaf.leaf.as_ref() {
                Some(L::Dense(dense)) => Some(dense.field.as_str()),
                _ => None,
            })
            .collect();
        if let Some(fields) = &self.field_permissions {
            for leaf in &plan.leaves {
                fields.boolean_leaf(leaf)?;
            }
            if let Some((_, aggregate)) = &plan.aggregate {
                fields.aggregate(&RequestFilters::default(), aggregate)?;
            }
        }
        // The spine: the root MUST filter leaves' trees conjoined, with
        // each leaf's flags at a known offset of the conjunction's walk.
        let mut spine_trees = Vec::new();
        let mut spine_offsets: Vec<(usize, usize, usize)> = Vec::new();
        let mut offset = 0usize;
        for &leaf in &plan.root_must_filters {
            let Some(tree) = plan
                .filters
                .get(leaf)
                .and_then(|f| f.as_ref()?.tree.as_ref())
            else {
                continue;
            };
            let count = crate::filter::leaf_count(tree);
            spine_offsets.push((leaf, offset, count));
            spine_trees.push(tree.clone());
            offset += count;
        }
        let spine = match spine_trees.len() {
            0 => None,
            1 => spine_trees.pop(),
            _ => Some(crate::pb::FilterExpr {
                expr: Some(crate::pb::filter_expr::Expr::And(crate::pb::FilterList {
                    exprs: spine_trees,
                })),
            }),
        };
        let mask = (self.document_visibility.is_none()
            && dense_fields.iter().all(|field| field.is_empty()))
        .then(|| self.shard_mask(spine.as_ref()))
        .flatten();
        let shards_total = self.node_addrs.len() as u32;
        let shards_skipped = mask.as_ref().map_or(0, |m| m.skipped_count());
        // One known-column accumulator per filter leaf; the spine leaves
        // learn what the mask resolved, at their own offsets.
        let mut known: Vec<Option<FilterKnown>> = plan
            .filters
            .iter()
            .map(|filters| filters.as_ref().map(FilterKnown::new))
            .collect();
        if let Some(mask) = mask.as_ref() {
            for &(leaf, base, count) in &spine_offsets {
                let Some(acc) = known[leaf].as_mut() else {
                    continue;
                };
                let local: Vec<usize> = mask
                    .known
                    .iter()
                    .filter(|&&index| index >= base && index < base + count)
                    .map(|&index| index - base)
                    .collect();
                mark_known(&mut acc.tree, &local);
                acc.kept = mask
                    .implied
                    .iter()
                    .map(|dropped| {
                        let local: Vec<usize> = dropped
                            .iter()
                            .filter(|&&index| index >= base && index < base + count)
                            .map(|&index| index - base)
                            .collect();
                        (!local.is_empty())
                            .then(|| (0..count).filter(|index| !local.contains(index)).collect())
                    })
                    .collect();
            }
        }
        // Per shard: the plan with the spine leaves pruned of what the
        // shard's placement implies.
        let mut shard_requests: Vec<Option<crate::pb::BooleanShardRequest>> =
            Vec::with_capacity(self.node_addrs.len());
        for (shard, claim) in claims.iter().enumerate() {
            if mask.as_ref().is_some_and(|m| m.skipped[shard]) {
                shard_requests.push(None);
                continue;
            }
            let mut leaves = plan.leaves.clone();
            if let Some(mask) = mask.as_ref() {
                for &(leaf, base, count) in &spine_offsets {
                    let local: Vec<usize> = mask.implied[shard]
                        .iter()
                        .filter(|&&index| index >= base && index < base + count)
                        .map(|&index| index - base)
                        .collect();
                    if local.is_empty() {
                        continue;
                    }
                    if let Some(L::Filter(filter)) = leaves[leaf].leaf.as_mut() {
                        filter.filter = filter
                            .filter
                            .as_ref()
                            .and_then(|tree| crate::placement::without_leaves(tree, &local));
                    }
                }
            }
            shard_requests.push(Some(crate::pb::BooleanShardRequest {
                root: Some(plan.root.clone()),
                leaves,
                depth: plan.depth,
                expected_stats_epoch: claim.epoch,
                expected_stats_incarnation: claim.incarnation(),
                visibility: self.document_visibility.clone(),
                max_logical_bytes: self.max_rerank_bytes,
                exact_batch: self.signal_batch() as u32,
                aggregate: plan.aggregate.as_ref().map(|(spec, _)| spec.clone()),
            }));
        }
        let mut tasks = Vec::with_capacity(self.node_addrs.len());
        for (shard, request) in shard_requests.iter().enumerate() {
            let Some(request) = request.clone() else {
                continue;
            };
            let mut client = self.node_client(&self.node_addrs[shard])?;
            let deadline = self.limits.shard_deadline;
            tasks.push(tokio::spawn(async move {
                let call = client.evaluate_boolean(request);
                let response = match deadline {
                    Some(limit) => tokio::time::timeout(limit, call)
                        .await
                        .map_err(|_| {
                            Status::deadline_exceeded("boolean evaluation shard deadline exceeded")
                        })?
                        .map(tonic::Response::into_inner),
                    None => call.await.map(tonic::Response::into_inner),
                }?;
                Ok::<_, Status>((shard, response))
            }));
        }
        let mut candidates = Vec::new();
        let mut prune = crate::segment_prune::PruneStats::default();
        let mut stage_known: Vec<Option<Vec<bool>>> = plan.leaves.iter().map(|_| None).collect();
        let mut folds = Vec::new();
        for task in tasks {
            let (shard, response) = task
                .await
                .map_err(|e| Status::internal(format!("boolean evaluation task failed: {e}")))??;
            let receipt = response.read_receipt.as_ref().ok_or_else(|| {
                Status::failed_precondition("Boolean response omitted its read receipt")
            })?;
            let actual = self.check_read_view(shard, &scope, receipt, &mut visibility_known)?;
            if actual != claims[shard] || response.stats_epoch != actual.epoch {
                return Err(Status::failed_precondition(
                    "Boolean response changed its admitted read version",
                ));
            }
            for field in &dense_fields {
                Self::check_vector_binding(field, receipt.vector_binding.as_ref(), &mut binding)?;
            }
            if response.filters_known.len() != plan.leaves.len()
                || response.stages_known.len() != plan.leaves.len()
            {
                return Err(Status::internal(format!(
                    "shard {shard} answered {} filter and {} stage flag lists for {} leaves",
                    response.filters_known.len(),
                    response.stages_known.len(),
                    plan.leaves.len()
                )));
            }
            for (index, flags) in response.filters_known.iter().enumerate() {
                if let Some(acc) = known[index].as_mut() {
                    acc.merge_shard(shard, &flags.geo_columns_known, &flags.filter_columns_known)?;
                }
            }
            for &index in &plan.positive_lexical {
                let Some(L::Lexical(lexical)) = plan.leaves[index].leaf.as_ref() else {
                    continue;
                };
                let flags = &response.stages_known[index].stage_columns_known;
                if flags.len() != lexical.score_stages.len() {
                    return Err(Status::failed_precondition(format!(
                        "shard {shard} returned {} stage-known flags for {} stages",
                        flags.len(),
                        lexical.score_stages.len()
                    )));
                }
                let acc = stage_known[index].get_or_insert_with(|| vec![false; flags.len()]);
                for (a, k) in acc.iter_mut().zip(flags) {
                    *a |= *k;
                }
            }
            candidates.extend(response.candidates);
            prune.add(crate::segment_prune::PruneStats {
                segments_total: response.segments_total,
                segments_skipped: response.segments_skipped,
            });
            if let Some(fold) = response.aggregate {
                folds.push((shard, fold));
            }
        }
        for (index, acc) in known.iter().enumerate() {
            if let (Some(acc), Some(filters)) = (acc, plan.filters[index].as_ref()) {
                acc.refuse_unknown(filters)?;
            }
        }
        for &index in &plan.positive_lexical {
            let Some(L::Lexical(lexical)) = plan.leaves[index].leaf.as_ref() else {
                continue;
            };
            let flags = stage_known[index]
                .clone()
                .unwrap_or_else(|| vec![false; lexical.score_stages.len()]);
            for (stage, known) in lexical.score_stages.iter().zip(flags) {
                if !known {
                    return Err(Status::invalid_argument(format!(
                        "no shard has numeric column {}: the score stage would be a silent no-op",
                        stage.column
                    )));
                }
            }
        }
        self.check_visibility_columns(&visibility_known)?;
        candidates.sort_by(|a, b| {
            b.score
                .total_cmp(&a.score)
                .then_with(|| a.doc_id.cmp(&b.doc_id))
        });
        candidates.truncate(plan.depth as usize);
        let aggregate = match plan.aggregate.as_ref() {
            Some((_, compiled)) => {
                let empty = RequestFilters::compile(&[], "")?;
                Some(
                    self.merge_aggregate_responses(
                        &empty,
                        compiled,
                        None,
                        folds,
                        PercentileScope::Boolean(&shard_requests),
                    )
                    .await?,
                )
            }
            None => None,
        };
        Ok(BooleanFanout {
            candidates,
            prune,
            shards_total,
            shards_skipped,
            aggregate,
        })
    }

    /// The BM25 parameters every lexical leaf scores under.
    pub fn bm25_params(&self) -> Bm25Params {
        self.bm25_params
    }

    /// Forget every cached term statistic (the stale-epoch retry).
    pub(crate) fn invalidate_stats(&self) {
        self.stats_cache.invalidate_all();
    }

    /// The distinct body terms of one lexical clause under `spec`, in
    /// first-occurrence order: the membership vocabulary of the clause.
    pub async fn analyze_terms(
        &self,
        text: &str,
        spec: Option<&crate::pb::AnalysisSpec>,
    ) -> Result<Vec<String>, Status> {
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
        Ok(terms)
    }

    /// Analyze one lexical clause and resolve its exact positive-score
    /// membership. No score bytes cross this phase.
    pub async fn lexical_membership(
        &self,
        text: &str,
        spec: Option<&crate::pb::AnalysisSpec>,
    ) -> Result<MembershipSet, Status> {
        if let Some(fields) = &self.field_permissions {
            fields.lexical_membership()?;
        }
        let scope = crate::visibility::VisibilityScope::new(self.document_visibility.as_ref())?;
        let mut visibility_known = vec![false; scope.column_count()];
        let analysis_fingerprint = crate::analyzer::analysis_fingerprint(spec);
        let terms = self.analyze_terms(text, spec).await?;
        if terms.is_empty() && self.document_visibility.is_none() {
            return Ok(MembershipSet {
                epochs: vec![StatsClaim::default(); self.node_addrs.len()],
                ..Default::default()
            });
        }
        let mut tasks = Vec::with_capacity(self.node_addrs.len());
        for node in &self.node_addrs {
            let client = self.node_client(node);
            let request = crate::pb::LexicalBitmapRequest {
                visibility: self.document_visibility.clone(),
                analysis_fingerprint,
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
        for (shard, task) in tasks.into_iter().enumerate() {
            let response = task.await.map_err(|error| {
                Status::internal(format!("lexical membership task failed: {error}"))
            })??;
            merged.epochs.push(self.check_read_view(
                shard,
                &scope,
                &response,
                &mut visibility_known,
            )?);
            merged.prune.add(crate::segment_prune::PruneStats {
                segments_total: response.segments_total,
                segments_skipped: response.segments_skipped,
            });
            Self::merge_membership_bitmap(&mut merged, &response)?;
        }
        self.check_visibility_columns(&visibility_known)?;
        merged.terms = terms;
        Ok(merged)
    }

    /// Resolve every live provider-backed vector row. This is the membership
    /// rule of a dense Boolean clause; native scores are fetched only for the
    /// candidates that survive the Boolean set plan.
    pub async fn vector_membership(&self, field: &str) -> Result<MembershipSet, Status> {
        if let Some(fields) = &self.field_permissions {
            fields.vector(field)?;
        }
        let scope = crate::visibility::VisibilityScope::new(self.document_visibility.as_ref())?;
        let mut visibility_known = vec![false; scope.column_count()];
        let mut tasks = Vec::with_capacity(self.node_addrs.len());
        for node in &self.node_addrs {
            let client = self.node_client(node);
            let request = crate::pb::VectorBitmapRequest {
                visibility: self.document_visibility.clone(),
                field: field.to_string(),
            };
            tasks.push(tokio::spawn(async move {
                client?
                    .resolve_vector_bitmap(request)
                    .await
                    .map(|response| response.into_inner())
            }));
        }
        let mut merged = MembershipSet::default();
        let mut held_binding = None;
        for (shard, task) in tasks.into_iter().enumerate() {
            let response = task.await.map_err(|error| {
                Status::internal(format!("vector membership task failed: {error}"))
            })??;
            Self::check_vector_binding(field, response.vector_binding.as_ref(), &mut held_binding)?;
            merged.epochs.push(self.check_read_view(
                shard,
                &scope,
                &response,
                &mut visibility_known,
            )?);
            Self::merge_membership_bitmap(&mut merged, &response)?;
        }
        self.check_visibility_columns(&visibility_known)?;
        Ok(merged)
    }

    /// Resolve a product filter into one packed stable-id bitmap per product
    /// shard for a vector provider that does not own document columns. No
    /// filter remains `None` and therefore costs no shard pass. An explicitly
    /// present empty bitmap set is an intentional match-none set and stays
    /// distinguishable at the provider boundary.
    #[cfg(feature = "net")]
    async fn clustered_allowed_labels(
        &self,
        filters: &RequestFilters,
    ) -> Result<Option<ClusteredLabelFilter>, Status> {
        if filters.geo.is_empty() && filters.tree.is_none() && self.document_visibility.is_none() {
            return Ok(None);
        }

        if let Some(fields) = &self.field_permissions {
            fields.filter(&filters.geo, filters.tree.as_ref())?;
        }
        let scope = crate::visibility::VisibilityScope::new(self.document_visibility.as_ref())?;
        let mut visibility_known = vec![false; scope.column_count()];
        let mask = self
            .document_visibility
            .is_none()
            .then(|| self.shard_mask(filters.tree.as_ref()))
            .flatten();
        let mut tasks = Vec::with_capacity(self.node_addrs.len());
        for (shard, node) in self.node_addrs.iter().enumerate() {
            if mask.as_ref().is_some_and(|m| m.skipped[shard]) {
                continue;
            }
            let request = crate::pb::FilterBitmapRequest {
                visibility: self.document_visibility.clone(),
                geo_filters: filters.geo.clone(),
                filter: Self::shard_filter_tree(filters, mask.as_ref(), shard),
            };
            let client = self.node_client(node);
            tasks.push((
                shard,
                tokio::spawn(async move {
                    client?
                        .resolve_filter_bitmap(request)
                        .await
                        .map(|response| response.into_inner())
                }),
            ));
        }

        let mut known = Self::filter_known(filters, mask.as_ref());
        let mut bitmaps = Vec::with_capacity(tasks.len());
        for (shard, task) in tasks {
            let response = task.await.map_err(|error| {
                Status::internal(format!("filter bitmap task failed: {error}"))
            })??;
            known.merge_shard(
                shard,
                &response.geo_columns_known,
                &response.filter_columns_known,
            )?;
            let response = Self::membership_from_filter(response);
            self.check_read_view(shard, &scope, &response, &mut visibility_known)?;
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
        self.check_visibility_columns(&visibility_known)?;
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

    #[cfg(feature = "net")]
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

    #[cfg(feature = "net")]
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

    #[cfg(feature = "net")]
    /// Resolve lineage in compact batches on the owning product shards. A
    /// document without stored lineage parents itself under the same tagged
    /// domain as the embedded path; raw document text never crosses this seam.
    async fn product_parent_ids(
        &self,
        ranges: &[ProductLabelRange],
        labels: &[u64],
    ) -> Result<HashMap<u64, u64>, Status> {
        let mut by_shard: HashMap<usize, Vec<u64>> = HashMap::new();
        for &label in labels {
            by_shard
                .entry(Self::product_owner(ranges, label)? as usize)
                .or_default()
                .push(label);
        }
        let selection =
            crate::lineage::LineageSelection::new(&[crate::pb::LineageField::ParentId as i32])?;
        let parents: HashMap<u64, u64> = self
            .read_lineage(labels, &selection, Some(&by_shard))
            .await?
            .into_iter()
            .map(|(id, (parent, _))| (id, parent))
            .collect();
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
    #[cfg(feature = "net")]
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

    #[cfg(feature = "net")]
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
                identity: None,
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
                        identity: None,
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
    #[cfg(feature = "net")]
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
        if let Some(fields) = &self.field_permissions {
            fields.aggregate(filters, compiled)?;
        }
        if self.query_read_versions.is_none() {
            let (pinned, reads) = self.pin_read_versions().await?;
            let result = Box::pin(pinned.fanout_aggregate(filters, compiled, doc_ids)).await;
            pinned.validate_read_versions(&reads).await?;
            return result;
        }
        let CompiledAggregate {
            aggregations,
            histograms,
            percentiles,
            group_by,
            max_groups,
            ..
        } = compiled;
        let mask = self
            .document_visibility
            .is_none()
            .then(|| self.shard_mask(filters.tree.as_ref()))
            .flatten();
        let mut tasks = Vec::with_capacity(self.node_addrs.len());
        for (shard, node) in self.node_addrs.iter().enumerate() {
            if mask.as_ref().is_some_and(|m| m.skipped[shard]) {
                continue;
            }
            let claim = self.query_read_versions.as_ref().expect("pinned read set")[shard];
            let request = crate::pb::AggregateShardRequest {
                visibility: self.document_visibility.clone(),
                expected_stats_epoch: claim.epoch,
                expected_stats_incarnation: claim.incarnation(),
                filter: Self::shard_filter_tree(filters, mask.as_ref(), shard),
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
            tasks.push((
                shard,
                tokio::spawn(async move {
                    client?
                        .aggregate_shard(request)
                        .await
                        .map(|r| r.into_inner())
                }),
            ));
        }
        let mut responses = Vec::with_capacity(tasks.len());
        for (shard, task) in tasks {
            responses.push((
                shard,
                task.await
                    .map_err(|e| Status::internal(format!("aggregate task failed: {e}")))??,
            ));
        }
        self.merge_aggregate_responses(
            filters,
            compiled,
            mask.as_ref(),
            responses,
            PercentileScope::Ids(doc_ids),
        )
        .await
    }

    /// The merge of the shards' folds (`AggregateShard` answers, or the
    /// folds a Boolean evaluation carried) into one response: partials
    /// fold in shard order, groups join by value, histograms sum by
    /// bucket, and the percentiles converge over count-below rounds
    /// under `scope`.
    pub(crate) async fn merge_aggregate_responses(
        &self,
        filters: &RequestFilters,
        compiled: &CompiledAggregate,
        mask: Option<&crate::placement::ShardMask>,
        responses: Vec<(usize, crate::pb::AggregateShardResponse)>,
        scope: PercentileScope<'_>,
    ) -> Result<crate::pb::AggregateResponse, Status> {
        let visibility_scope =
            crate::visibility::VisibilityScope::new(self.document_visibility.as_ref())?;
        let mut visibility_known = vec![false; visibility_scope.column_count()];
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
        let mut known = Self::filter_known(filters, mask);
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
        for (shard, response) in responses {
            self.check_read_view(shard, &visibility_scope, &response, &mut visibility_known)?;
            known.merge_shard(
                shard,
                &response.geo_columns_known,
                &response.filter_columns_known,
            )?;
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
                m.fold(p, agg)?;
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
                    m.fold(p, agg)?;
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
        self.check_visibility_columns(&visibility_known)?;
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
                    .map(|(&idx, &count)| {
                        if spec.calendar != 0 {
                            // A calendar bucket's key IS its start
                            // instant, in the expression's micros.
                            crate::pb::HistogramBucket {
                                lower: idx as f64,
                                lower_int: idx,
                                count,
                            }
                        } else {
                            crate::pb::HistogramBucket {
                                lower: idx as f64 * spec.interval,
                                lower_int: 0,
                                count,
                            }
                        }
                    })
                    .collect(),
                present: hist_present[i],
                unbucketable: hist_unbucketable[i],
            })
            .collect();
        let pct_results = self
            .solve_percentiles(filters, percentile_specs, percentiles, &pct_merged, scope)
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
        scope: PercentileScope<'_>,
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
                let k = nearest_percentile_rank(p, m.present);
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
                .quantile_round(filters, compiled, &probes, scope)
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
                            } else if m.vt == Some(crate::pb::AggregateValueType::Uint) {
                                W::UintValue(bits)
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
        selection_scope: PercentileScope<'_>,
    ) -> Result<Vec<u64>, Status> {
        let scope = crate::visibility::VisibilityScope::new(self.document_visibility.as_ref())?;
        let mut visibility_known = vec![false; scope.column_count()];
        let claims = self
            .query_read_versions
            .as_ref()
            .ok_or_else(|| Status::internal("quantile rounds require a pinned read set"))?;
        let mask = self
            .document_visibility
            .is_none()
            .then(|| self.shard_mask(filters.tree.as_ref()))
            .flatten();
        let mut tasks = Vec::with_capacity(self.node_addrs.len());
        for (shard, node) in self.node_addrs.iter().enumerate() {
            if mask.as_ref().is_some_and(|m| m.skipped[shard]) {
                continue;
            }
            let request = match selection_scope {
                PercentileScope::Ids(doc_ids) => crate::pb::QuantileCountsRequest {
                    visibility: self.document_visibility.clone(),
                    expected_stats_epoch: claims[shard].epoch,
                    expected_stats_incarnation: claims[shard].incarnation(),
                    filter: Self::shard_filter_tree(filters, mask.as_ref(), shard),
                    geo_filters: filters.geo.clone(),
                    exprs: exprs.to_vec(),
                    targets: targets.to_vec(),
                    doc_ids: doc_ids.unwrap_or_default().to_vec(),
                    restrict_doc_ids: doc_ids.is_some(),
                    boolean: None,
                },
                PercentileScope::Boolean(plans) => {
                    // A shard the Boolean plan skipped counts nothing.
                    let Some(plan) = plans.get(shard).and_then(|p| p.as_ref()) else {
                        continue;
                    };
                    crate::pb::QuantileCountsRequest {
                        visibility: self.document_visibility.clone(),
                        expected_stats_epoch: claims[shard].epoch,
                        expected_stats_incarnation: claims[shard].incarnation(),
                        filter: None,
                        geo_filters: Vec::new(),
                        exprs: exprs.to_vec(),
                        targets: targets.to_vec(),
                        doc_ids: Vec::new(),
                        restrict_doc_ids: false,
                        boolean: Some(plan.clone()),
                    }
                }
            };
            let client = self.node_client(node);
            tasks.push((
                shard,
                tokio::spawn(async move {
                    client?
                        .quantile_counts(request)
                        .await
                        .map(|r| r.into_inner())
                }),
            ));
        }
        let mut totals = vec![0u64; targets.len()];
        for (shard, task) in tasks {
            let response = task
                .await
                .map_err(|e| Status::internal(format!("quantile task failed: {e}")))??;
            self.check_read_view(shard, &scope, &response, &mut visibility_known)?;
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
        self.check_visibility_columns(&visibility_known)?;
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
                let resp =
                    SearchService::bm25_search(self, crate::metrics::nested(Request::new(req)))
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
                let resp =
                    SearchService::hybrid_search(self, crate::metrics::nested(Request::new(req)))
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
pub(crate) struct StreamFanout {
    scoped: bool,
    // Own response readers so cancellation, setup failure and successful
    // completion all release the underlying RPC streams.
    readers: tokio::task::JoinSet<()>,
    merged_rx: mpsc::Receiver<(usize, Result<Option<StreamSearchResponse>, Status>)>,
    floor_txs: Vec<Option<mpsc::Sender<StreamSearchRequest>>>,
    udp_lanes: Vec<Option<(u64, std::net::SocketAddr)>>,
    udp_socket: Option<Arc<std::net::UdpSocket>>,
    /// The signing key and a sequence shared by every lane of this
    /// fan-out (docs/security.md); a node ignores a sequence at or
    /// behind the newest it applied, so each token sees them ascend.
    udp_key: Option<crate::security::UdpKey>,
    udp_seq: Arc<std::sync::atomic::AtomicU32>,
}

impl StreamFanout {
    pub(crate) async fn resolve_identities(
        &mut self,
        hits: &[MergedHit],
        scans: &[Option<StreamSearchSummary>],
        terminal: &mut [Option<StreamSearchSummary>],
        limits: &crate::pb::StreamIdentityLimits,
    ) -> Result<HashMap<(u32, u64), Option<crate::pb::DocumentIdentity>>, Status> {
        let exchange = async {
            let mut expected = vec![Vec::new(); self.floor_txs.len()];
            for hit in hits {
                expected[hit.shard as usize].push(hit.vector_id);
            }
            let mut remaining = 0usize;
            for (shard, tx) in self.floor_txs.iter().enumerate() {
                if let Some(tx) = tx {
                    remaining += 1;
                    tx.send(StreamSearchRequest {
                        payload: Some(stream_search_request::Payload::ResolveIdentities(
                            crate::pb::ResolveStreamIdentities {
                                vector_ids: expected[shard].clone(),
                            },
                        )),
                    })
                    .await
                    .map_err(|_| Status::unavailable("identity request stream closed"))?;
                }
            }
            let mut received = vec![false; expected.len()];
            let mut identities = HashMap::with_capacity(hits.len());
            let mut payload_bytes = 0usize;
            while remaining > 0 {
                let Some((shard, message)) = self.next_message(terminal).await? else {
                    continue;
                };
                if terminal[shard].is_some() {
                    return Err(Status::internal("identity message after terminal summary"));
                }
                if matches!(
                    message.payload.as_ref(),
                    Some(stream_search_response::Payload::Identities(_))
                ) && prost::Message::encoded_len(&message) > limits.max_response_bytes as usize
                {
                    return Err(Status::resource_exhausted(
                        "shard exceeded the identity response budget",
                    ));
                }
                match message.payload {
                    Some(stream_search_response::Payload::Identities(found)) => {
                        if received[shard] || found.rows.len() != expected[shard].len() {
                            return Err(Status::internal(
                                "shard identity response count or sequence differs",
                            ));
                        }
                        for (row, expected_id) in found.rows.into_iter().zip(&expected[shard]) {
                            if row.vector_id != *expected_id {
                                return Err(Status::internal(
                                    "shard returned another ID's identity",
                                ));
                            }
                            let len = prost::Message::encoded_len(&row);
                            payload_bytes = payload_bytes
                                .checked_add(
                                    1 + prost::encoding::encoded_len_varint(len as u64) + len,
                                )
                                .ok_or_else(|| {
                                    Status::resource_exhausted("identity response length overflow")
                                })?;
                            let response_bytes = 1
                                + prost::encoding::encoded_len_varint(payload_bytes as u64)
                                + payload_bytes;
                            if response_bytes > limits.max_response_bytes as usize {
                                return Err(Status::resource_exhausted(
                                    "combined identity response exceeds max_response_bytes",
                                ));
                            }
                            identities.insert((shard as u32, row.vector_id), row.identity);
                        }
                        received[shard] = true;
                    }
                    Some(stream_search_response::Payload::Summary(summary)) => {
                        if !received[shard] || Some(&summary) != scans[shard].as_ref() {
                            return Err(Status::failed_precondition(
                                "identity exchange did not certify the captured scan",
                            ));
                        }
                        terminal[shard] = Some(summary);
                        self.mark_completed(shard);
                        remaining -= 1;
                    }
                    _ => {
                        return Err(Status::internal(
                            "unexpected message during identity selection",
                        ))
                    }
                }
            }
            Ok(identities)
        };
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(u64::from(limits.timeout_ms)),
            exchange,
        )
        .await
        .unwrap_or_else(|_| Err(Status::deadline_exceeded("identity fan-out timed out")));
        match result {
            Ok(identities) => Ok(identities),
            Err(status) => self.cancel_with(status).await,
        }
    }

    /// Send one typed frame to a lane, signed when a key is configured.
    fn send_signal(
        &self,
        socket: &std::net::UdpSocket,
        target: std::net::SocketAddr,
        frame: [u8; crate::stream_signal::FRAME_LEN],
    ) {
        match &self.udp_key {
            Some(key) => {
                let seq = self
                    .udp_seq
                    .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
                    + 1;
                let signed = crate::stream_signal::sign(key, seq, &frame);
                let _ = socket.send_to(&signed, target);
            }
            None => {
                let _ = socket.send_to(&frame, target);
            }
        }
    }

    fn send_udp_cancel(&self) {
        let Some(socket) = self.udp_socket.as_deref() else {
            return;
        };
        for (shard, tx) in self.floor_txs.iter().enumerate() {
            if tx.is_none() {
                continue;
            }
            if let Some((token, target)) = self.udp_lanes[shard] {
                self.send_signal(socket, target, crate::stream_signal::encode_cancel(token));
            }
        }
    }

    /// Abandon every unfinished shard. UDP goes first for low latency; the
    /// matching gRPC Stop is enqueued within one bounded grace period. If a
    /// peer no longer drains requests, abort its response reader to close the
    /// RPC instead of waiting forever. Neither path certifies completion.
    pub(crate) async fn cancel(&mut self) {
        self.send_udp_cancel();
        let senders: Vec<mpsc::Sender<StreamSearchRequest>> =
            self.floor_txs.iter_mut().filter_map(Option::take).collect();
        send_stream_stops(senders).await;
        self.readers.abort_all();
    }

    pub(crate) async fn cancel_with<T>(&mut self, status: Status) -> Result<T, Status> {
        self.cancel().await;
        Err(status)
    }

    pub(crate) fn mark_completed(&mut self, shard: usize) {
        self.floor_txs[shard] = None;
        self.udp_lanes[shard] = None;
    }

    /// The next inbound message: `Ok(Some((shard, msg)))` for a payload,
    /// `Ok(None)` for a clean post-summary stream close (callers just
    /// continue), and an error for a shard failure or a close without a
    /// summary (a protocol break — the summary is the exactness
    /// certificate, so a stream that vanishes without one aborts the
    /// query).
    pub(crate) async fn next_message(
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
            Err(e) => Err(Status::new(
                if self.scoped {
                    e.code()
                } else {
                    tonic::Code::Internal
                },
                format!("shard {shard} failed: {e}"),
            )),
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
        let readers = std::mem::take(&mut self.readers);
        let send_stops = async move {
            send_stream_stops(senders).await;
            drop(readers);
        };
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(send_stops);
        }
    }
}

/// One grace period for the entire fan-out, including full request lanes.
/// Dropping the owned senders and reader tasks then closes abandoned RPCs.
async fn send_stream_stops(senders: Vec<mpsc::Sender<StreamSearchRequest>>) {
    let _ = tokio::time::timeout(Duration::from_millis(250), async move {
        let mut sends = tokio::task::JoinSet::new();
        for tx in senders {
            sends.spawn(async move {
                let _ = tx
                    .send(StreamSearchRequest {
                        payload: Some(stream_search_request::Payload::Stop(StopStreamSearch {})),
                    })
                    .await;
            });
        }
        while sends.join_next().await.is_some() {}
    })
    .await;
}

/// Everything one shard-stream attempt needs, cheap to clone per attempt
/// (a hedged retry is just a second attempt with the same context).
#[derive(Clone)]
struct ShardQueryCtx {
    admission: Option<Arc<crate::vector_read::VectorReadBarrier>>,
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
    /// Per shard, the filter tree that shard receives: the request's
    /// tree with the clauses its placement leaf implies removed. A hedge
    /// leg reads the same entry as its primary.
    shard_filters: Arc<Vec<Option<crate::pb::FilterExpr>>>,
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
    mut client: crate::link::NodeLink,
    ctx: ShardQueryCtx,
) -> Result<SearchShardDone, Status> {
    let (req_tx, req_rx) = mpsc::channel::<SearchShardRequest>(8);
    req_tx
        .send(SearchShardRequest {
            payload: Some(search_shard_request::Payload::Start(StartShardSearch {
                read_context: ctx
                    .admission
                    .as_ref()
                    .map(|barrier| barrier.context(shard as usize))
                    .transpose()?,
                request_id: ctx.request_id.to_string(),
                k: ctx.k,
                vector: ctx.vector.as_ref().clone(),
                tie_complete: ctx.tie_complete,
                collapse_parents: ctx.collapse,
                geo_filters: ctx.filters.geo.clone(),
                filter: ctx.shard_filters.get(shard as usize).cloned().flatten(),
            })),
        })
        .await
        .map_err(|_| Status::internal("node request channel closed before Start"))?;
    let mut responses = client
        .search_shard(ReceiverStream::new(req_rx))
        .await?
        .into_inner();

    if let Some(admission) = &ctx.admission {
        admission.admit(shard as usize, &mut responses).await?;
    }

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
        match crate::vector_read::next(&mut responses).await {
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
    primary: crate::link::NodeLink,
    replica: Option<crate::link::NodeLink>,
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

impl CoordinatorServiceImpl {
    /// The first message of a routed mapped ingest stream, which must be
    /// the bind: it names the collection the whole stream writes into
    /// (`docs/collections.md`).
    pub async fn routed_bind(
        inbound: &mut (impl tokio_stream::Stream<Item = Result<RoutedIngestMappedRequest, Status>>
                  + Unpin),
    ) -> Result<crate::pb::RoutedMappedBind, Status> {
        match tokio_stream::StreamExt::next(inbound).await.transpose()? {
            Some(RoutedIngestMappedRequest {
                payload: Some(crate::pb::routed_ingest_mapped_request::Payload::Bind(bind)),
            }) => Ok(bind),
            _ => Err(Status::invalid_argument(
                "first RoutedIngestMappedRequest must be a RoutedMappedBind",
            )),
        }
    }

    /// Routed mapped ingest after its bind was read (and, on a collection
    /// set, resolved): the write gate, the topology snapshot, and the
    /// per-shard fan-out.
    pub async fn routed_ingest_mapped_bound<S>(
        &self,
        bind: crate::pb::RoutedMappedBind,
        mut inbound: S,
    ) -> Result<RoutedIngestMappedResponse, Status>
    where
        S: tokio_stream::Stream<Item = Result<RoutedIngestMappedRequest, Status>> + Unpin + Send,
    {
        // Gate before snapshotting: a write that arrived during a cutover
        // must resume into the new map, never retain the old snapshot while
        // waiting behind the final-tail barrier.
        let _write_guard = if self.live_topology.is_some() {
            Some(self.write_gate.clone().read_owned().await)
        } else {
            None
        };
        if let Some(snapshot) = self.request_snapshot() {
            return Box::pin(snapshot.routed_ingest_mapped_bound(bind, inbound)).await;
        }
        if bind.required_topology_generation == 0 {
            return Err(Status::invalid_argument(
                "routed writes require required_topology_generation; zero is not accepted",
            ));
        }
        self.require_topology_generation(bind.required_topology_generation)?;
        // Ingest lands rows on shards that must score alike: a fleet that
        // already scores in two spaces is refused before a row moves.
        self.fleet_vector_identity(false).await?;
        let mapped_bind = bind
            .bind
            .ok_or_else(|| Status::invalid_argument("routed mapped bind is missing bind"))?;
        // Under a placement tree the coordinator evaluates the tree over
        // each document's own columns and routes inside the leaf it
        // picks (docs/placement.md); the leaf's shards fill the
        // placement column from their pinned code.
        let placer = match self.placement.as_ref() {
            Some(placement) => Some(PlacementRouter::new(Arc::clone(placement), &mapped_bind)?),
            None => None,
        };
        let mut batches: Vec<Vec<crate::pb::IngestMappedRequest>> =
            vec![Vec::new(); self.node_addrs.len()];
        let mut position = 0u64;
        while let Some(message) = tokio_stream::StreamExt::next(&mut inbound)
            .await
            .transpose()?
        {
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
            let leaf = match placer.as_ref() {
                Some(placer) => Some(placer.leaf_of(&document.document, position)?),
                None => None,
            };
            position += 1;
            let (_, shard) = self
                .route_stable_key_in(&document.stable_key, leaf)
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
        Ok(RoutedIngestMappedResponse {
            added,
            parents,
            served_topology_generation: self.topology_generation,
            shards,
        })
    }
}

/// One routed ingest stream's placement evaluator: the bind's plan as
/// an extractor, its materialize spec compiled, and the tree. A source
/// document's rows (one per chunk on a chunked plan) must agree on the
/// leaf, since the rows of one source go to one shard.
struct PlacementRouter {
    placement: Arc<crate::placement::Placement>,
    extractor: crate::mapping::Extractor,
    materialize: Vec<(String, crate::pb::ValueExpr, crate::pb::MaterializeKind)>,
}

impl PlacementRouter {
    fn new(
        placement: Arc<crate::placement::Placement>,
        bind: &crate::pb::MappedBind,
    ) -> Result<Self, Status> {
        let extractor = crate::mapping::Extractor::new(
            &bind.descriptor_set,
            &bind.message_type,
            &bind.body_path,
        )?;
        let materialize = match bind.materialize.as_ref() {
            Some(spec) => crate::node::compile_materialize_spec(spec)?,
            None => Vec::new(),
        };
        Ok(PlacementRouter {
            placement,
            extractor,
            materialize,
        })
    }

    /// The placement code of one source document at `position` in the
    /// stream. Quality and geography columns are derived on the node
    /// after analysis and are absent here, so a predicate on one is
    /// UNKNOWN at routing time and falls through to the default.
    fn leaf_of(&self, bytes: &[u8], position: u64) -> Result<i64, Status> {
        let rows = self.extractor.extract(bytes).map_err(|status| {
            Status::new(
                status.code(),
                format!("document {position}: {}", status.message()),
            )
        })?;
        if rows.is_empty() {
            let empty = crate::pb::AddDocumentsRequest::default();
            let leaf = self
                .placement
                .evaluate(&empty)
                .map_err(Status::invalid_argument)?;
            return Ok(leaf.code);
        }
        let mut chosen: Option<(i64, String)> = None;
        for extracted in rows {
            let doc = crate::node::apply_materialize(extracted.request, &self.materialize)
                .map_err(|status| {
                    Status::new(
                        status.code(),
                        format!("document {position}: {}", status.message()),
                    )
                })?;
            let leaf = self
                .placement
                .evaluate(&doc)
                .map_err(|e| Status::invalid_argument(format!("document {position}: {e}")))?;
            match chosen.as_ref() {
                None => chosen = Some((leaf.code, leaf.name.clone())),
                Some((code, name)) if *code != leaf.code => {
                    return Err(Status::invalid_argument(format!(
                        "document {position}: its rows evaluate to placement leaves {name:?} and \
                         {:?}; the rows of one source document go to one shard, so a placement \
                         predicate reads parent-scope columns, not chunk-scope ones",
                        leaf.name
                    )));
                }
                Some(_) => {}
            }
        }
        Ok(chosen.expect("at least one row").0)
    }
}

/// The dry run's counting state: filtered counts per shard, memoized by
/// node and restriction, so a default node's subtraction reuses its
/// siblings' counts (`src/placement_plan.rs` for the arithmetic).
struct PlanCounter<'a> {
    coordinator: &'a CoordinatorServiceImpl,
    base: Option<crate::pb::FilterExpr>,
    column: String,
    memo: std::collections::HashMap<(String, Option<i64>), Vec<u64>>,
}

impl PlanCounter<'_> {
    /// Rows per shard passing `tree` (all live rows for `None`). With
    /// `refuse_unknown`, a leaf no shard can resolve refuses by name.
    async fn count(
        &self,
        tree: Option<crate::pb::FilterExpr>,
        refuse_unknown: bool,
        node_name: &str,
    ) -> Result<Vec<u64>, Status> {
        if let Some(expr) = tree.as_ref() {
            crate::filter::validate_filter(expr).map_err(|status| {
                Status::invalid_argument(format!(
                    "plan_placement: node {node_name:?}: the combined predicate {}",
                    status.message()
                ))
            })?;
        }
        let filters = RequestFilters {
            geo: Vec::new(),
            tree,
        };
        let count = crate::pb::CompiledAggregation {
            expr: Some(crate::cel::compile_value("1")?),
            op: crate::pb::AggregateOp::Count as i32,
            name: "rows".to_string(),
            max_distinct: 0,
        };
        let mut tasks = Vec::with_capacity(self.coordinator.node_addrs.len());
        for node in &self.coordinator.node_addrs {
            let request = crate::pb::AggregateShardRequest {
                visibility: None,
                expected_stats_epoch: 0,
                expected_stats_incarnation: Vec::new(),
                filter: filters.tree.clone(),
                geo_filters: Vec::new(),
                aggregations: vec![count.clone()],
                group_by: String::new(),
                max_groups: 0,
                histograms: Vec::new(),
                percentiles: Vec::new(),
                doc_ids: Vec::new(),
                restrict_doc_ids: false,
            };
            let client = self.coordinator.node_client(node);
            tasks.push(tokio::spawn(async move {
                client?
                    .aggregate_shard(request)
                    .await
                    .map(|r| r.into_inner())
            }));
        }
        let mut known = FilterKnown::new(&filters);
        let mut counts = Vec::with_capacity(tasks.len());
        for task in tasks {
            let response = task
                .await
                .map_err(|e| Status::internal(format!("plan_placement task failed: {e}")))??;
            known.merge(&response.geo_columns_known, &response.filter_columns_known)?;
            counts.push(response.matched);
        }
        if refuse_unknown {
            known.refuse_unknown(&filters).map_err(|status| {
                Status::new(
                    status.code(),
                    format!("plan_placement: node {node_name:?}: {}", status.message()),
                )
            })?;
        }
        Ok(counts)
    }

    /// Rows per shard that land on the node at `path` (indices from the
    /// root chain), restricted to `extra` when given.
    fn first<'s>(
        &'s mut self,
        plan: &'s [crate::placement_plan::PlanNode],
        path: &'s [usize],
        extra: Option<crate::pb::FilterExpr>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<u64>, Status>> + Send + 's>>
    {
        Box::pin(async move {
            let node = node_at(plan, path);
            let extra_code = extra.as_ref().map(|_| self.code_of(extra.as_ref()));
            let key = (node.name.clone(), extra_code);
            if let Some(counts) = self.memo.get(&key) {
                return Ok(counts.clone());
            }
            let refuse_unknown = extra.is_none();
            let counts = if !node.is_default {
                let (a, ab) = crate::placement_plan::first_match_trees(
                    node,
                    self.base.as_ref(),
                    extra.as_ref(),
                );
                let all = self.count(a, refuse_unknown, &node.name).await?;
                match ab {
                    Some(ab) => {
                        let taken = self.count(Some(ab), refuse_unknown, &node.name).await?;
                        all.iter()
                            .zip(&taken)
                            .map(|(a, b)| a.saturating_sub(*b))
                            .collect()
                    }
                    None => all,
                }
            } else {
                // The default: the parent's rows minus the siblings'.
                let parent_path = &path[..path.len() - 1];
                let mut counts = if parent_path.is_empty() {
                    let (a, _) = crate::placement_plan::first_match_trees(
                        node,
                        self.base.as_ref(),
                        extra.as_ref(),
                    );
                    self.count(a, refuse_unknown, &node.name).await?
                } else {
                    self.first(plan, parent_path, extra.clone()).await?
                };
                let siblings = if parent_path.is_empty() {
                    plan
                } else {
                    &node_at(plan, parent_path).children
                };
                for (i, sibling) in siblings.iter().enumerate() {
                    if sibling.is_default {
                        continue;
                    }
                    let mut sibling_path = parent_path.to_vec();
                    sibling_path.push(i);
                    let taken = self.first(plan, &sibling_path, extra.clone()).await?;
                    for (c, t) in counts.iter_mut().zip(&taken) {
                        *c = c.saturating_sub(*t);
                    }
                }
                counts
            };
            self.memo.insert(key, counts.clone());
            Ok(counts)
        })
    }

    /// The code an `extra` restriction names (the memo key); the
    /// restriction is always `column == code` here.
    fn code_of(&self, extra: Option<&crate::pb::FilterExpr>) -> i64 {
        match extra.and_then(|e| e.expr.as_ref()) {
            Some(crate::pb::filter_expr::Expr::Number(n)) => {
                match n.min.as_ref().and_then(|b| b.value.as_ref()) {
                    Some(crate::pb::filter_bound::Value::Int(code)) => *code,
                    _ => 0,
                }
            }
            _ => 0,
        }
    }
}

fn node_at<'p>(
    plan: &'p [crate::placement_plan::PlanNode],
    path: &[usize],
) -> &'p crate::placement_plan::PlanNode {
    let mut node = &plan[path[0]];
    for &i in &path[1..] {
        node = &node.children[i];
    }
    node
}

#[tonic::async_trait]
impl SearchService for CoordinatorServiceImpl {
    type QueryStreamStream =
        crate::metrics::Timed<ReceiverStream<Result<crate::pb::QueryStreamResponse, Status>>>;

    async fn search(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        crate::metrics::timed(Route::Search, request, |request| async move {
            self.admit(&request.get_ref().collection)?;
            if let Some(snapshot) = self.request_snapshot() {
                return Box::pin(SearchService::search(
                    &snapshot,
                    crate::metrics::nested(request),
                ))
                .await;
            }
            let mut req = request.into_inner();
            if !req.field.is_empty() {
                let scoped = self.for_vector_field(&req.field)?;
                req.field.clear();
                return Box::pin(SearchService::search(
                    &scoped,
                    crate::metrics::nested(Request::new(req)),
                ))
                .await;
            }
            if self.scoped_vector_scan() && self.query_read_versions.is_none() {
                let filters = RequestFilters::compile(&req.geo_filters, &req.filter)?;
                self.check_vector_scan(&filters, req.collapse_parents)?;
                let (pinned, reads) = self.pin_read_versions().await?;
                let result = Box::pin(SearchService::search(
                    &pinned,
                    crate::metrics::nested(Request::new(req)),
                ))
                .await;
                pinned.validate_read_versions(&reads).await?;
                return result;
            }
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
            self.check_vector_scan(&filters, req.collapse_parents)?;
            // The fleet scores in one space or not at all: mixed provider
            // kinds or fingerprints are refused before any shard is asked
            // (docs/mmap-vectors.md).
            if !self.has_clustered_vectors() {
                self.fleet_vector_identity(true).await?;
            }

            #[cfg(feature = "net")]
            if self.clustered_vectors.is_some() {
                if req.collapse_parents {
                    let collapsed = self
                        .clustered_parent_collapse(&request_id, &req.vector, k, &filters)
                        .await?;
                    return Ok(Response::new(SearchResponse {
                        segments_total: 0,
                        segments_skipped: 0,
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
                        identity: None,
                        vector_id,
                        score,
                        parent_id: 0,
                    })
                    .collect();
                return Ok(Response::new(SearchResponse {
                    segments_total: 0,
                    segments_skipped: 0,
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
                        segments_total: 0,
                        segments_skipped: 0,
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
                    segments_total: 0,
                    segments_skipped: 0,
                    request_id,
                    hits: streamed.hits,
                    groups: Vec::new(),
                    chunk_floor: 0.0,
                }));
            } else {
                self.fanout_search(&request_id, &req.vector, k, false, &filters)
                    .await?
            };
            let (segments_total, segments_skipped) = result.shard_stats.iter().flatten().fold(
                (0u32, 0u32),
                |(total, skipped), stats| {
                    (
                        total.saturating_add(stats.segments_total),
                        skipped.saturating_add(stats.segments_skipped),
                    )
                },
            );
            Ok(Response::new(SearchResponse {
                segments_total,
                segments_skipped,
                request_id,
                hits: result.hits,
                groups: Vec::new(),
                chunk_floor: 0.0,
            }))
        })
        .await
    }

    async fn bm25_search(
        &self,
        request: Request<Bm25SearchRequest>,
    ) -> Result<Response<Bm25SearchResponse>, Status> {
        crate::metrics::timed(Route::Bm25Search, request, |request| async move {
            self.admit(&request.get_ref().collection)?;
            if let Some(snapshot) = self.request_snapshot() {
                return Box::pin(SearchService::bm25_search(
                    &snapshot,
                    crate::metrics::nested(request),
                ))
                .await;
            }
            let req = request.into_inner();
            let k = self.resolve_k(req.k)?;
            // A malformed HighlightSpec refuses here, before any shard is
            // asked, so an empty fleet answers the same as a full one
            // (docs/highlighting.md).
            if let Some(spec) = req.highlight.as_ref() {
                crate::highlight::Plan::from_spec(spec)?;
            }
            if req.min_score.is_nan() || req.min_score == f32::NEG_INFINITY {
                return Err(Status::invalid_argument(
                    "min_score must be finite (NaN and -inf are not valid floors)",
                ));
            }
            // CEL text compiles ONCE, here, into the predicate IR the
            // shards execute (docs/cel-filters.md): every shard sees the
            // same tree, and none ever sees CEL text.
            let user_filter = crate::cel::compile_filter(&req.filter)?;
            // Projection text compiles ONCE, here, into the ValueExpr IR
            // the shards resolve and evaluate (docs/cel-values.md).
            let projections = compile_projections(&req.projections)?;
            if let Some(fields) = &self.field_permissions {
                fields.bm25(&req, user_filter.as_ref(), &projections)?;
            }
            let filter = self.visible_filter(user_filter)?;
            let mut phrase_routing = Vec::new();
            let mut prefix_expansions = Vec::new();
            let mut synonym_expansions = Vec::new();
            let (hits, facets, range_facets, stats, cardinality, prune) =
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
                        phrase: req.phrase,
                        prefixes: req.prefixes.clone(),
                        synonyms: req.synonyms.clone(),
                        synonyms_off: req.synonyms_off,
                    }];
                    let (((hits, facets, ranges), fused_prune), routing) = self
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
                            &mut prefix_expansions,
                            req.highlight.as_ref(),
                            &mut synonym_expansions,
                            req.explain,
                        )
                        .await?;
                    phrase_routing = routing;
                    (hits, facets, ranges, Vec::new(), Vec::new(), fused_prune)
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
                        &req.prefixes,
                        &mut prefix_expansions,
                        req.highlight.as_ref(),
                        &req.synonyms,
                        req.synonyms_off,
                        &mut synonym_expansions,
                        req.explain,
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
                    if !req.prefixes.is_empty() {
                        return Err(Status::invalid_argument(
                        "Bm25SearchRequest.prefixes expand in the body; with `fields` set, put \
                         the prefixes on the QueryField whose dictionary they expand in",
                    ));
                    }
                    let (((hits, facets, ranges), fused_prune), routing) = self
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
                            &mut prefix_expansions,
                            req.highlight.as_ref(),
                            &mut synonym_expansions,
                            req.explain,
                        )
                        .await?;
                    phrase_routing = routing;
                    (hits, facets, ranges, Vec::new(), Vec::new(), fused_prune)
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
            let mut response = Bm25SearchResponse {
                field_details_redacted: false,
                execution_details_redacted: self.document_visibility.is_some(),
                segments_total: if self.document_visibility.is_some() {
                    0
                } else {
                    prune.segments_total
                },
                segments_skipped: if self.document_visibility.is_some() {
                    0
                } else {
                    prune.segments_skipped
                },
                hits,
                kth_best,
                facets,
                range_facets,
                stats,
                cardinality,
                phrase_routing,
                prefix_expansions,
                synonym_expansions,
            };
            if let Some(fields) = &self.field_permissions {
                fields.disclose(&mut response)?;
            }
            Ok(Response::new(response))
        })
        .await
    }

    async fn phrase_search(
        &self,
        request: Request<crate::pb::PhraseSearchRequest>,
    ) -> Result<Response<Bm25SearchResponse>, Status> {
        crate::metrics::timed(Route::PhraseSearch, request, |request| async move {
        self.admit(&request.get_ref().collection)?;
        if let Some(snapshot) = self.request_snapshot() {
                return Box::pin(SearchService::phrase_search(&snapshot, crate::metrics::nested(request))).await;
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
            execution_details_redacted: false,
            field_details_redacted: false,
            segments_total: 0,
            segments_skipped: 0,
            hits,
            kth_best,
            facets,
            range_facets,
            stats: Vec::new(),
            cardinality: Vec::new(),
            phrase_routing: Vec::new(),
            prefix_expansions: Vec::new(),
            synonym_expansions: Vec::new(),
        }))
        })
        .await
    }

    async fn hybrid_search(
        &self,
        request: Request<HybridSearchRequest>,
    ) -> Result<Response<HybridSearchResponse>, Status> {
        crate::metrics::timed(Route::HybridSearch, request, |request| async move {
            self.admit(&request.get_ref().collection)?;
            if let Some(snapshot) = self.request_snapshot() {
                return Box::pin(SearchService::hybrid_search(
                    &snapshot,
                    crate::metrics::nested(request),
                ))
                .await;
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
        })
        .await
    }

    /// The public query surface: an adapter over the routes above
    /// (`docs/query-api.md`, `src/query.rs`). Delegation, never a fork.
    async fn query(
        &self,
        request: Request<crate::pb::QueryRequest>,
    ) -> Result<Response<crate::pb::QueryResponse>, Status> {
        crate::metrics::timed(Route::Query, request, |request| async move {
            self.admit(&request.get_ref().collection)?;
            if let Some(snapshot) = self.request_snapshot() {
                return Box::pin(SearchService::query(
                    &snapshot,
                    crate::metrics::nested(request),
                ))
                .await;
            }
            let access = request
                .extensions()
                .get::<crate::pb::AccessDecision>()
                .cloned();
            let request = request.into_inner();
            self.require_topology_generation(request.required_topology_generation)?;
            let response = self.execute_query(request, access.as_ref()).await?;
            Ok(Response::new(response))
        })
        .await
    }

    async fn query_stream(
        &self,
        request: Request<crate::pb::QueryStreamRequest>,
    ) -> Result<Response<Self::QueryStreamStream>, Status> {
        crate::metrics::timed_stream(Route::QueryStream, request, |request| async move {
        self.admit(&request.get_ref().collection)?;
        if let Some(snapshot) = self.request_snapshot() {
                return Box::pin(SearchService::query_stream(
                    &snapshot,
                    crate::metrics::nested(request),
                ))
                .await
                .map(|response| response.map(crate::metrics::Timed::into_inner));
        }
        let access = request.extensions().get::<crate::pb::AccessDecision>().cloned();
        let mut request = request.into_inner();
        let request_fingerprint = request.query.as_ref().map(|query| crate::sha256::hex_digest(&prost::Message::encode_to_vec(query))).unwrap_or_default();
        if let Some(query) = request.query.as_mut() {
            self.require_topology_generation(query.required_topology_generation)?;
            // Reject collection, query, authority and topology mismatches before
            // opening the stream. Keep the original token for the execution's
            // data-version binding, which runs under the stream deadline.
            self.bind_query_cursor(&mut query.clone(), access.as_ref())?;
            if query.explain {
                return Err(Status::invalid_argument(
                    "explain is served on the unary Query route: a stream's revisions carry                      candidate hits without their trees, and a tree over a revision that a                      later one replaces would explain a score that was never served",
                ));
            }
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
                crate::pb::QueryStreamIdentityState::Unspecified,
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
                                error_disclosure: crate::error_disclosure::status_detail(&status),
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
            let (progress_tx, progress_rx) = watch::channel(None);
            let runner = service.with_stream_search(true).with_bm25_stream(true)
                .with_query_progress(progress_tx);
            let timeout = (request.timeout_ms > 0).then(|| Duration::from_millis(request.timeout_ms));
            let deadline = async move {
                match timeout {
                    Some(duration) => tokio::time::sleep(duration).await,
                    None => std::future::pending::<()>().await,
                }
            };
            tokio::pin!(deadline);
            let mut scoring_fingerprints = Vec::new();
            let result = {
                let attempt = runner.query_stream_attempt(query, access, progress_rx, &tx,
                    &mut revision, &mut scoring_fingerprints, &request_fingerprint);
                tokio::pin!(attempt);
                tokio::select! {
                    biased;
                    _ = tx.closed() => return,
                    _ = &mut deadline => Err(Status::deadline_exceeded(format!(
                        "QueryStream exceeded its {}ms deadline", request.timeout_ms))),
                    result = &mut attempt => result,
                }
            };
            // Dropping the attempt aborts its JoinSet on deadline, cancellation
            // or identity refusal. No collector is left running behind a
            // terminal stream. The completion itself may await client capacity.
            scoring_fingerprints.sort(); scoring_fingerprints.dedup();
            let completion = match result {
                Ok(response) => crate::pb::QueryStreamCompletion {
                    completed: true, response: Some(response), final_revision: revision,
                    scoring_fingerprints, error_code: 0, error_message: String::new(), error_disclosure: None,
                },
                Err(status) => crate::pb::QueryStreamCompletion {
                    completed: false, response: None, final_revision: revision,
                    scoring_fingerprints, error_code: status.code() as u32,
                    error_message: status.message().to_string(),
                    error_disclosure: crate::error_disclosure::status_detail(&status),
                },
            };
            let _ = tx.send(Ok(crate::pb::QueryStreamResponse {
                payload: Some(crate::pb::query_stream_response::Payload::Completion(completion)),
            })).await;
        });
        Ok(Response::new(ReceiverStream::new(rx)))
        })
        .await
    }

    async fn plan_index(
        &self,
        request: Request<crate::pb::PlanIndexRequest>,
    ) -> Result<Response<crate::pb::PlanIndexResponse>, Status> {
        crate::metrics::timed(Route::PlanIndex, request, |request| async move {
            self.admit(&request.get_ref().collection)?;
            if let Some(snapshot) = self.request_snapshot() {
                return Box::pin(SearchService::plan_index(
                    &snapshot,
                    crate::metrics::nested(request),
                ))
                .await;
            }
            // Derivation is local and deterministic (docs/descriptor-mappings.md):
            // nothing fans out, nothing binds, and the same request returns the
            // same fingerprint on every coordinator.
            let req = request.into_inner();
            let plan = crate::mapping::derive_plan_with_definition(
                &req.descriptor_set,
                &req.message_type,
                req.index_definition.as_ref(),
            )?;
            Ok(Response::new(crate::pb::PlanIndexResponse {
                plan: Some(plan),
            }))
        })
        .await
    }

    async fn describe_schema(
        &self,
        request: Request<crate::pb::DescribeSchemaRequest>,
    ) -> Result<Response<crate::pb::DescribeSchemaResponse>, Status> {
        crate::metrics::timed(Route::DescribeSchema, request, |request| async move {
            self.admit(&request.get_ref().collection)?;
            if let Some(snapshot) = self.request_snapshot() {
                return Box::pin(SearchService::describe_schema(
                    &snapshot,
                    crate::metrics::nested(request),
                ))
                .await;
            }
            let req = request.into_inner();
            crate::mapping::describe_schema(&req.descriptor_set, &req.message_type)
                .map(Response::new)
        })
        .await
    }

    async fn routed_ingest_mapped(
        &self,
        request: Request<Streaming<RoutedIngestMappedRequest>>,
    ) -> Result<Response<RoutedIngestMappedResponse>, Status> {
        crate::metrics::timed(Route::RoutedIngestMapped, request, |request| async move {
            let mut inbound = request.into_inner();
            let bind = Self::routed_bind(&mut inbound).await?;
            self.admit(&bind.collection)?;
            self.routed_ingest_mapped_bound(bind, inbound)
                .await
                .map(Response::new)
        })
        .await
    }

    async fn freeze_topology_writes(
        &self,
        request: Request<FreezeTopologyWritesRequest>,
    ) -> Result<Response<FreezeTopologyWritesResponse>, Status> {
        crate::metrics::timed(Route::FreezeTopologyWrites, request, |request| async move {
            self.admit(&request.get_ref().collection)?;
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
        })
        .await
    }

    async fn publish_topology(
        &self,
        request: Request<PublishTopologyRequest>,
    ) -> Result<Response<PublishTopologyResponse>, Status> {
        crate::metrics::timed(Route::PublishTopology, request, |request| async move {
            self.admit(&request.get_ref().collection)?;
            let req = request.into_inner();
            let routes = req
                .shards
                .into_iter()
                .map(|shard| TopologyRoute {
                    addr: crate::config::normalize_addr(shard.addr),
                    replica: (!shard.replica.is_empty())
                        .then(|| crate::config::normalize_addr(shard.replica)),
                    hash_range: Some((shard.hash_lo, shard.hash_hi)),
                    placement: shard.has_placement.then_some(shard.placement as i64),
                })
                .collect();
            let tree = req
                .placement
                .as_ref()
                .map(crate::placement::PlacementTreeConfig::from_proto);
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
            self.publish_topology_inner(req.generation, routes, tree.as_ref())
                .map_err(Status::invalid_argument)?;
            held.take();
            self.cutover_pending.store(false, AtomicOrdering::Release);
            Ok(Response::new(PublishTopologyResponse {
                topology_generation: req.generation,
            }))
        })
        .await
    }

    async fn abort_topology_cutover(
        &self,
        request: Request<AbortTopologyCutoverRequest>,
    ) -> Result<Response<AbortTopologyCutoverResponse>, Status> {
        crate::metrics::timed(Route::AbortTopologyCutover, request, |request| async move {
            self.admit(&request.get_ref().collection)?;
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
        })
        .await
    }

    /// Exact aggregates over the filtered corpus
    /// (docs/aggregations.md). CEL compiles ONCE, here: the filter
    /// into the predicate IR, each expression into the ValueExpr IR;
    /// no shard ever sees text.
    async fn aggregate(
        &self,
        request: Request<crate::pb::AggregateRequest>,
    ) -> Result<Response<crate::pb::AggregateResponse>, Status> {
        crate::metrics::timed(Route::Aggregate, request, |request| async move {
            self.admit(&request.get_ref().collection)?;
            if let Some(snapshot) = self.request_snapshot() {
                return Box::pin(SearchService::aggregate(
                    &snapshot,
                    crate::metrics::nested(request),
                ))
                .await;
            }
            let req = request.into_inner();
            let filters = RequestFilters::compile(&req.geo_filters, &req.filter)?;
            let compiled = compile_aggregations(&req)?;
            self.fanout_aggregate(&filters, &compiled, None)
                .await
                .map(Response::new)
        })
        .await
    }

    /// Placement dry run (`docs/placement.md`, "The dry run"): what a
    /// proposed tree would do to the rows this topology holds, from
    /// filtered counts (`src/placement_plan.rs` has the arithmetic). It
    /// reads only. A predicate naming a column no shard holds is refused
    /// by name, the rule every filtered route applies to a typo.
    async fn plan_placement(
        &self,
        request: Request<crate::pb::PlanPlacementRequest>,
    ) -> Result<Response<crate::pb::PlanPlacementResponse>, Status> {
        crate::metrics::timed(Route::PlanPlacement, request, |request| async move {
            self.admit(&request.get_ref().collection)?;
            if let Some(snapshot) = self.request_snapshot() {
                return Box::pin(SearchService::plan_placement(
                    &snapshot,
                    crate::metrics::nested(request),
                ))
                .await;
            }
            let req = request.into_inner();
            let proposed = req.proposed.as_ref().ok_or_else(|| {
                Status::invalid_argument("plan_placement: proposed tree is absent")
            })?;
            let config = crate::placement::PlacementTreeConfig::from_proto(proposed);
            let placement =
                crate::placement::Placement::validate(&config).map_err(Status::invalid_argument)?;
            let plan = crate::placement_plan::plan(&placement).map_err(Status::invalid_argument)?;
            let base = RequestFilters::compile(&[], &req.filter)?;
            let mut counter = PlanCounter {
                coordinator: self,
                base: base.tree,
                column: placement.column().to_string(),
                memo: std::collections::HashMap::new(),
            };
            let mut cells = Vec::new();
            let mut rows = 0u64;
            let mut moving_rows = 0u64;
            let mut defaulted_rows = 0u64;
            let mut stack: Vec<(Vec<usize>, &crate::placement_plan::PlanNode)> =
                plan.iter().enumerate().map(|(i, n)| (vec![i], n)).collect();
            // Leaves in code order: depth-first, chain order.
            let mut leaves: Vec<(Vec<usize>, &crate::placement_plan::PlanNode)> = Vec::new();
            stack.reverse();
            while let Some((path, node)) = stack.pop() {
                if node.is_leaf() {
                    leaves.push((path, node));
                } else {
                    for (i, child) in node.children.iter().enumerate().rev() {
                        let mut p = path.clone();
                        p.push(i);
                        stack.push((p, child));
                    }
                }
            }
            for (path, leaf) in leaves {
                let per_shard = counter.first(&plan, &path, None).await?;
                let staying = counter
                    .first(
                        &plan,
                        &path,
                        Some(crate::placement_plan::code_equals(
                            &counter.column.clone(),
                            leaf.code,
                        )),
                    )
                    .await?;
                for (shard, (count, stay)) in per_shard.iter().zip(&staying).enumerate() {
                    if *count == 0 {
                        continue;
                    }
                    let moving = count.saturating_sub(*stay);
                    rows += count;
                    moving_rows += moving;
                    if leaf.is_default {
                        defaulted_rows += count;
                    }
                    cells.push(crate::pb::PlacementCell {
                        shard: shard as u32,
                        code: leaf.code as u64,
                        leaf: leaf.name.clone(),
                        rows: *count,
                        moving_rows: moving,
                    });
                }
            }
            Ok(Response::new(crate::pb::PlanPlacementResponse {
                topology_generation: self.current_topology_generation(),
                cells,
                rows,
                moving_rows,
                defaulted_rows,
            }))
        })
        .await
    }

    /// Autocomplete over one field's dictionary (`docs/suggest.md`):
    /// normalize the prefix as a prefix term is normalized (the field's
    /// char filters, never its stemmer), ask every shard for the terms
    /// under it with their posting df, union by term summing df, and
    /// rank by df descending then term bytes ascending. The union IS the
    /// dictionary one image of the rows would hold and the summed df is
    /// that image's posting df, so the fleet's answer equals the
    /// monolith's bitwise. Past `max_scan` on any shard or in the union
    /// the request refuses naming the count; nothing is truncated to a
    /// quieter match set.
    async fn suggest(
        &self,
        request: Request<crate::pb::SuggestRequest>,
    ) -> Result<Response<crate::pb::SuggestResponse>, Status> {
        crate::metrics::timed(Route::Suggest, request, |request| async move {
            self.admit(&request.get_ref().collection)?;
            if let Some(snapshot) = self.request_snapshot() {
                return Box::pin(SearchService::suggest(&snapshot, request)).await;
            }
            let req = request.into_inner();
            if let Some(fields) = &self.field_permissions {
                fields.suggest(&req)?;
            }
            let limit = match req.limit as usize {
                0 => DEFAULT_SUGGEST_LIMIT,
                n if n > MAX_SUGGEST_LIMIT => {
                    return Err(Status::invalid_argument(format!(
                        "limit {n} exceeds the maximum {MAX_SUGGEST_LIMIT}"
                    )))
                }
                n => n,
            };
            let max_scan = match req.max_scan {
                0 => DEFAULT_SUGGEST_SCAN,
                n if n > MAX_SUGGEST_SCAN as u64 => {
                    return Err(Status::invalid_argument(format!(
                        "max_scan {n} exceeds the maximum {MAX_SUGGEST_SCAN}"
                    )))
                }
                n => n as usize,
            };
            if req.field.is_empty() {
                return Err(Status::invalid_argument(
                    "suggest needs a field: name the indexed BM25 field whose dictionary to \
                 complete (\"body\" for the body)",
                ));
            }
            let field = req.field.as_str();
            let normalized = crate::analyzer::normalize_prefix(&req.prefix, req.analysis.as_ref())?;
            let mut tasks = Vec::with_capacity(self.node_addrs.len());
            for (i, node) in self.node_addrs.iter().enumerate() {
                let mut client = self.node_client(node)?;
                let request = crate::pb::SuggestTermsRequest {
                    visibility: self.document_visibility.clone(),
                    field: field.to_string(),
                    prefix: normalized.clone(),
                    max_scan: max_scan as u64,
                };
                tasks.push((
                    i,
                    tokio::spawn(async move {
                        client.suggest_terms(request).await.map(|r| r.into_inner())
                    }),
                ));
            }
            // Term -> (summed df, shards holding it), in byte order.
            let mut union: std::collections::BTreeMap<String, (u64, u32)> =
                std::collections::BTreeMap::new();
            let scope = crate::visibility::VisibilityScope::new(self.document_visibility.as_ref())?;
            let mut visibility_known = vec![false; scope.column_count()];
            let mut known = false;
            let mut tombstoned = false;
            for (shard, task) in tasks {
                let resp = task
                    .await
                    .map_err(|e| Status::internal(format!("suggest task failed: {e}")))??;
                tombstoned |= resp.tombstoned_rows > 0;
                scope
                    .validate_echo(&resp.visibility_fingerprint, &resp.visibility_columns_known)?;
                for (known, present) in visibility_known
                    .iter_mut()
                    .zip(&resp.visibility_columns_known)
                {
                    *known |= present;
                }
                if !resp.known {
                    continue;
                }
                known = true;
                if resp.count as usize > max_scan {
                    return Err(Status::invalid_argument(format!(
                        "prefix {normalized:?} on field {field:?} matches {} dictionary terms on \
                     shard {shard}; the scan bound is {max_scan} (raise max_scan up to \
                     {MAX_SUGGEST_SCAN}, or lengthen the prefix)",
                        resp.count
                    )));
                }
                for entry in resp.entries {
                    let slot = union.entry(entry.term).or_insert((0, 0));
                    slot.0 += entry.df;
                    slot.1 += 1;
                }
            }
            self.check_visibility_columns(&visibility_known)?;
            if !known {
                return Err(Status::invalid_argument(format!(
                    "no shard indexes field {field:?}; prefix {normalized:?} has no dictionary to \
                 complete in"
                )));
            }
            if union.len() > max_scan {
                return Err(Status::invalid_argument(format!(
                    "prefix {normalized:?} on field {field:?} matches {} dictionary terms across \
                 the fleet; the scan bound is {max_scan} (raise max_scan up to \
                 {MAX_SUGGEST_SCAN}, or lengthen the prefix)",
                    union.len()
                )));
            }
            let dictionary_terms_with_prefix = union.len() as u64;
            let mut ranked: Vec<(String, (u64, u32))> = union.into_iter().collect();
            // df descending; the map's byte order breaks ties (stable sort).
            ranked.sort_by_key(|(_, (df, _))| std::cmp::Reverse(*df));
            ranked.truncate(limit);
            Ok(Response::new(crate::pb::SuggestResponse {
                suggestions: ranked
                    .into_iter()
                    .map(|(term, (df, shards))| crate::pb::Suggestion { term, df, shards })
                    .collect(),
                dictionary_terms_with_prefix,
                df_includes_tombstoned_rows: tombstoned,
            }))
        })
        .await
    }

    async fn term_suggest(
        &self,
        request: Request<crate::pb::TermSuggestRequest>,
    ) -> Result<Response<crate::pb::TermSuggestResponse>, Status> {
        crate::metrics::timed(Route::TermSuggest, request, |request| async move {
            self.admit(&request.get_ref().collection)?;
            if let Some(snapshot) = self.request_snapshot() {
                return Box::pin(SearchService::term_suggest(&snapshot, request)).await;
            }
            let req = request.into_inner();
            if let Some(fields) = &self.field_permissions {
                fields.term_suggest(&req)?;
            }
            let max_edits = match req.max_edits {
                0 => 1usize,
                n if n > MAX_TERM_SUGGEST_EDITS => {
                    return Err(Status::invalid_argument(format!(
                        "max_edits {n} exceeds the maximum {MAX_TERM_SUGGEST_EDITS}"
                    )))
                }
                n => n as usize,
            };
            let prefix_length = match req.prefix_length {
                0 => 1usize,
                n => n as usize,
            };
            let limit = match req.limit as usize {
                0 => DEFAULT_TERM_SUGGEST_LIMIT,
                n if n > MAX_SUGGEST_LIMIT => {
                    return Err(Status::invalid_argument(format!(
                        "limit {n} exceeds the maximum {MAX_SUGGEST_LIMIT}"
                    )))
                }
                n => n,
            };
            let max_scan = match req.max_scan {
                0 => DEFAULT_SUGGEST_SCAN,
                n if n > MAX_SUGGEST_SCAN as u64 => {
                    return Err(Status::invalid_argument(format!(
                        "max_scan {n} exceeds the maximum {MAX_SUGGEST_SCAN}"
                    )))
                }
                n => n as usize,
            };
            if req.field.is_empty() {
                return Err(Status::invalid_argument(
                    "term suggestions need a field: name the indexed BM25 field whose \
                     dictionary to consult (\"body\" for the body)",
                ));
            }
            if req.analysis.is_none() {
                return Err(Status::invalid_argument(
                    "term suggestions need the field's analysis spec: the text is analyzed \
                     under it, and the sidecar's default chain is not known here",
                ));
            }
            if req.text.trim().is_empty() {
                return Err(Status::invalid_argument("term suggestions need text"));
            }
            let always = req.mode == crate::pb::TermSuggestMode::Always as i32;
            let field = req.field.clone();
            let addr = self.analysis_addr.clone().ok_or_else(|| {
                Status::unavailable(
                    "no analysis backend configured on the coordinator (analysis_addr)",
                )
            })?;
            let analyzed =
                crate::analyzer::analyze_document(&addr, &req.text, req.analysis.as_ref())
                    .await?
                    .into_body();
            let mut terms: Vec<String> = Vec::new();
            for (term, _, _) in analyzed.terms {
                if !terms.contains(&term) {
                    terms.push(term);
                }
            }
            // One bounded prefix scan per term per shard; a term shorter
            // than the prefix length is looked up whole (its own df) and
            // gets no candidates.
            let mut tasks = Vec::with_capacity(terms.len() * self.node_addrs.len());
            for (ti, term) in terms.iter().enumerate() {
                let prefix: String = term.chars().take(prefix_length).collect();
                let scan = if term.chars().count() >= prefix_length {
                    prefix
                } else {
                    term.clone()
                };
                for (shard, node) in self.node_addrs.iter().enumerate() {
                    let mut client = self.node_client(node)?;
                    let request = crate::pb::SuggestTermsRequest {
                        visibility: self.document_visibility.clone(),
                        field: field.clone(),
                        prefix: scan.clone(),
                        max_scan: max_scan as u64,
                    };
                    tasks.push((
                        ti,
                        shard,
                        scan.clone(),
                        tokio::spawn(async move {
                            client.suggest_terms(request).await.map(|r| r.into_inner())
                        }),
                    ));
                }
            }
            let mut unions: Vec<std::collections::BTreeMap<String, (u64, u32)>> =
                terms.iter().map(|_| Default::default()).collect();
            let scope = crate::visibility::VisibilityScope::new(self.document_visibility.as_ref())?;
            let mut visibility_known = vec![false; scope.column_count()];
            let mut known = false;
            let mut tombstoned = false;
            for (ti, shard, scan, task) in tasks {
                let resp = task
                    .await
                    .map_err(|e| Status::internal(format!("term suggest task failed: {e}")))??;
                tombstoned |= resp.tombstoned_rows > 0;
                scope
                    .validate_echo(&resp.visibility_fingerprint, &resp.visibility_columns_known)?;
                for (known, present) in visibility_known
                    .iter_mut()
                    .zip(&resp.visibility_columns_known)
                {
                    *known |= present;
                }
                if !resp.known {
                    continue;
                }
                known = true;
                if resp.count as usize > max_scan {
                    return Err(Status::invalid_argument(format!(
                        "prefix {scan:?} of term {:?} on field {field:?} matches {} dictionary \
                         terms on shard {shard}; the scan bound is {max_scan} (raise max_scan \
                         up to {MAX_SUGGEST_SCAN}, or raise prefix_length)",
                        terms[ti], resp.count
                    )));
                }
                for entry in resp.entries {
                    let slot = unions[ti].entry(entry.term).or_insert((0, 0));
                    slot.0 += entry.df;
                    slot.1 += 1;
                }
            }
            self.check_visibility_columns(&visibility_known)?;
            if !known {
                return Err(Status::invalid_argument(format!(
                    "no shard indexes field {field:?}; there is no dictionary to suggest from"
                )));
            }
            let mut out = Vec::with_capacity(terms.len());
            for (ti, term) in terms.iter().enumerate() {
                let union = &unions[ti];
                if union.len() > max_scan {
                    return Err(Status::invalid_argument(format!(
                        "the prefix of term {term:?} on field {field:?} matches {} dictionary \
                         terms across the fleet; the scan bound is {max_scan} (raise max_scan \
                         up to {MAX_SUGGEST_SCAN}, or raise prefix_length)",
                        union.len()
                    )));
                }
                let df = union.get(term).map_or(0, |(df, _)| *df);
                let candidates = if term.chars().count() < prefix_length || (df > 0 && !always) {
                    Vec::new()
                } else {
                    crate::synonyms::rank_candidates(term, union, max_edits, limit)
                };
                out.push(crate::pb::TermSuggestion {
                    term: term.clone(),
                    df,
                    candidates,
                    dictionary_terms_scanned: union.len() as u64,
                });
            }
            Ok(Response::new(crate::pb::TermSuggestResponse {
                terms: out,
                df_includes_tombstoned_rows: tombstoned,
            }))
        })
        .await
    }

    async fn cluster_health(
        &self,
        _request: Request<ClusterHealthRequest>,
    ) -> Result<Response<ClusterHealthResponse>, Status> {
        crate::metrics::timed(Route::ClusterHealth, _request, |_request| async move {
            self.admit(&_request.get_ref().collection)?;
            if let Some(snapshot) = self.request_snapshot() {
                return Box::pin(SearchService::cluster_health(
                    &snapshot,
                    crate::metrics::nested(Request::new(ClusterHealthRequest {
                        collection: String::new(),
                    })),
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
                    Err(e) => {
                        return Err(Status::internal(format!("health probe task failed: {e}")))
                    }
                }
            }
            // A reachable node that serves another collection is a
            // misconfiguration, named here rather than counted here.
            for target in &mut targets {
                if let Some(health) = &target.health {
                    if health.collection != self.collection {
                        target.error = format!(
                            "node serves collection {:?}, but this coordinator is {:?}",
                            health.collection, self.collection
                        );
                    }
                }
            }
            let provider_mismatch = provider_mismatch_of(&targets);
            #[cfg(feature = "net")]
            let clustered_vector = if let Some(backend) = &self.clustered_vectors {
                Some(match backend.health().await {
                    Ok(health) => {
                        // The score-space identity a quality profile binds to;
                        // a servable cluster that cannot state it reports that
                        // as its error rather than an empty fingerprint.
                        let (scoring_fingerprint, dimensions, error) = if health.servable {
                            match backend.quality_identity().await {
                                Ok(identity) => (
                                    identity.scoring_fingerprint,
                                    identity.dimensions,
                                    health.error,
                                ),
                                Err(status) => (String::new(), 0, status.message().to_string()),
                            }
                        } else {
                            (String::new(), 0, health.error)
                        };
                        ClusteredVectorHealth {
                            backend_kind: "clustered-turbovec".to_string(),
                            transport: backend.transport_name().to_string(),
                            reachable: true,
                            servable: health.servable,
                            error,
                            rows: health.rows,
                            topology_generation: health.topology_generation,
                            scoring_fingerprint,
                            dimensions,
                        }
                    }
                    Err(status) => ClusteredVectorHealth {
                        backend_kind: "clustered-turbovec".to_string(),
                        transport: backend.transport_name().to_string(),
                        reachable: false,
                        servable: false,
                        error: status.to_string(),
                        rows: 0,
                        topology_generation: 0,
                        scoring_fingerprint: String::new(),
                        dimensions: 0,
                    },
                })
            } else {
                None
            };
            #[cfg(not(feature = "net"))]
            let clustered_vector: Option<ClusteredVectorHealth> = None;
            Ok(Response::new(ClusterHealthResponse {
                collections: Vec::new(),
                targets,
                clustered_vector,
                provider_mismatch,
                topology_generation: self.topology_generation,
            }))
        })
        .await
    }

    async fn broadcast_vector_backend(
        &self,
        request: Request<BroadcastVectorBackendRequest>,
    ) -> Result<Response<BroadcastVectorBackendResponse>, Status> {
        crate::metrics::timed(
            Route::BroadcastVectorBackend,
            request,
            |request| async move {
                self.admit(&request.get_ref().collection)?;
                if let Some(snapshot) = self.request_snapshot() {
                    return Box::pin(SearchService::broadcast_vector_backend(
                        &snapshot,
                        crate::metrics::nested(request),
                    ))
                    .await;
                }
                let req = request.into_inner();
                if req.dim == 0 || req.config.is_none() {
                    return Err(Status::invalid_argument(
                        "positive dim and vector backend config are required",
                    ));
                }
                let results = self.fanout_vector_backend(&req).await;
                Ok(Response::new(BroadcastVectorBackendResponse { results }))
            },
        )
        .await
    }

    async fn broadcast_calibration(
        &self,
        request: Request<BroadcastCalibrationRequest>,
    ) -> Result<Response<BroadcastCalibrationResponse>, Status> {
        crate::metrics::timed(Route::BroadcastCalibration, request, |request| async move {
            self.admit(&request.get_ref().collection)?;
            if let Some(snapshot) = self.request_snapshot() {
                return Box::pin(SearchService::broadcast_calibration(
                    &snapshot,
                    crate::metrics::nested(request),
                ))
                .await;
            }
            let req = request.into_inner();
            if req.shift.len() != req.dim as usize || req.scale.len() != req.dim as usize {
                return Err(Status::invalid_argument(
                    "shift and scale must have length dim",
                ));
            }
            let results = self.fanout_calibration(&req).await;
            Ok(Response::new(BroadcastCalibrationResponse { results }))
        })
        .await
    }

    async fn variant_search(
        &self,
        request: Request<VariantSearchRequest>,
    ) -> Result<Response<VariantSearchResponse>, Status> {
        crate::metrics::timed(Route::VariantSearch, request, |request| async move {
            self.admit(&request.get_ref().collection)?;
            if let Some(snapshot) = self.request_snapshot() {
                return Box::pin(SearchService::variant_search(
                    &snapshot,
                    crate::metrics::nested(request),
                ))
                .await;
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
        })
        .await
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

    fn identity_fanout() -> (
        StreamFanout,
        mpsc::Sender<(usize, Result<Option<StreamSearchResponse>, Status>)>,
        mpsc::Receiver<StreamSearchRequest>,
    ) {
        let (tx, rx) = mpsc::channel(8);
        let (request_tx, request_rx) = mpsc::channel(1);
        (
            StreamFanout {
                scoped: false,
                readers: tokio::task::JoinSet::new(),
                merged_rx: rx,
                floor_txs: vec![Some(request_tx)],
                udp_lanes: vec![None],
                udp_socket: None,
                udp_key: None,
                udp_seq: Arc::new(std::sync::atomic::AtomicU32::new(0)),
            },
            tx,
            request_rx,
        )
    }

    #[tokio::test]
    async fn identity_budget_applies_to_the_combined_reply_from_multiple_children() {
        let (mut fanout, tx, first) = identity_fanout();
        let (second_tx, second) = mpsc::channel(1);
        fanout.floor_txs.push(Some(second_tx));
        fanout.udp_lanes.push(None);
        let scan = StreamSearchSummary {
            completed: true,
            emitted: 1,
            ..Default::default()
        };
        let mut children = tokio::task::JoinSet::new();
        for (shard, mut requests) in [first, second].into_iter().enumerate() {
            let tx = tx.clone();
            let scan = scan.clone();
            children.spawn(async move {
                requests.recv().await.unwrap();
                let row = crate::pb::StreamIdentity {
                    vector_id: 100 + shard as u64,
                    identity: Some(crate::pb::DocumentIdentity {
                        document_key: vec![shard as u8; 32],
                        version: 7,
                        chunk_ordinal: None,
                    }),
                };
                let response = StreamSearchResponse {
                    payload: Some(stream_search_response::Payload::Identities(
                        crate::pb::StreamIdentities { rows: vec![row] },
                    )),
                };
                assert!(prost::Message::encoded_len(&response) < 64);
                tx.send((shard, Ok(Some(response)))).await.unwrap();
                tx.send((
                    shard,
                    Ok(Some(StreamSearchResponse {
                        payload: Some(stream_search_response::Payload::Summary(scan)),
                    })),
                ))
                .await
                .unwrap();
                while let Some(request) = requests.recv().await {
                    if matches!(
                        request.payload,
                        Some(stream_search_request::Payload::Stop(_))
                    ) {
                        break;
                    }
                }
            });
        }
        let hits = [
            MergedHit {
                shard: 0,
                vector_id: 100,
                score: 1.0,
            },
            MergedHit {
                shard: 1,
                vector_id: 101,
                score: 1.0,
            },
        ];
        let limits = crate::pb::StreamIdentityLimits {
            max_rows: 2,
            max_response_bytes: 64,
            timeout_ms: 1000,
        };
        let error = fanout
            .resolve_identities(
                &hits,
                &[Some(scan.clone()), Some(scan)],
                &mut [None, None],
                &limits,
            )
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::ResourceExhausted);
        assert!(error.message().contains("combined"));
        while let Some(done) = children.join_next().await {
            done.unwrap();
        }
    }

    #[tokio::test]
    async fn completed_fanout_releases_a_peer_that_never_closes_its_response() {
        let (mut fanout, _tx, _requests) = identity_fanout();
        let (started, ready) = tokio::sync::oneshot::channel();
        let (retained, released) = tokio::sync::oneshot::channel::<()>();
        fanout.readers.spawn(async move {
            let _retained = retained;
            let _ = started.send(());
            std::future::pending::<()>().await;
        });
        ready.await.unwrap();
        fanout.mark_completed(0);
        drop(fanout);
        assert!(tokio::time::timeout(Duration::from_secs(1), released)
            .await
            .expect("terminal completion must release response readers")
            .is_err());
    }

    #[tokio::test]
    async fn identity_timeout_is_bounded_even_when_the_request_lane_is_full() {
        let (mut fanout, _tx, mut requests) = identity_fanout();
        let limits = crate::pb::StreamIdentityLimits {
            max_rows: 1,
            max_response_bytes: 1024,
            timeout_ms: 10,
        };
        let scan = StreamSearchSummary {
            completed: true,
            ..Default::default()
        };
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            fanout.resolve_identities(&[], &[Some(scan)], &mut [None], &limits),
        )
        .await
        .expect("error cleanup must not wait forever behind the unanswered selection");
        assert_eq!(result.unwrap_err().code(), tonic::Code::DeadlineExceeded);
        assert!(matches!(
            requests.recv().await.unwrap().payload,
            Some(stream_search_request::Payload::ResolveIdentities(_))
        ));
        assert!(requests.recv().await.is_none());
    }

    #[tokio::test]
    async fn identity_exchange_rejects_wrong_ids_missing_or_changed_certificates_and_early_close() {
        let scan = StreamSearchSummary {
            completed: true,
            emitted: 1,
            ..Default::default()
        };
        let rows = |id| StreamSearchResponse {
            payload: Some(stream_search_response::Payload::Identities(
                crate::pb::StreamIdentities {
                    rows: vec![crate::pb::StreamIdentity {
                        vector_id: id,
                        identity: None,
                    }],
                },
            )),
        };
        let summary = |scan| StreamSearchResponse {
            payload: Some(stream_search_response::Payload::Summary(scan)),
        };
        let cases = [
            (vec![Some(rows(999))], tonic::Code::Internal),
            (
                vec![Some(summary(scan.clone()))],
                tonic::Code::FailedPrecondition,
            ),
            (
                vec![
                    Some(rows(100)),
                    Some(summary(StreamSearchSummary {
                        emitted: 2,
                        ..scan.clone()
                    })),
                ],
                tonic::Code::FailedPrecondition,
            ),
            (vec![Some(rows(100)), None], tonic::Code::Internal),
            (
                vec![Some(rows(100)), Some(rows(100))],
                tonic::Code::Internal,
            ),
        ];
        for (messages, expected) in cases {
            let (mut fanout, tx, mut requests) = identity_fanout();
            let child = tokio::spawn(async move {
                let request = requests.recv().await.unwrap();
                assert!(matches!(
                    request.payload,
                    Some(stream_search_request::Payload::ResolveIdentities(_))
                ));
                for message in messages {
                    tx.send((0, Ok(message))).await.unwrap();
                }
                assert!(matches!(
                    requests.recv().await.unwrap().payload,
                    Some(stream_search_request::Payload::Stop(_))
                ));
            });
            let limits = crate::pb::StreamIdentityLimits {
                max_rows: 1,
                max_response_bytes: 1024,
                timeout_ms: 1000,
            };
            let hit = MergedHit {
                shard: 0,
                vector_id: 100,
                score: 1.0,
            };
            assert_eq!(
                fanout
                    .resolve_identities(&[hit], &[Some(scan.clone())], &mut [None], &limits)
                    .await
                    .unwrap_err()
                    .code(),
                expected
            );
            child.await.unwrap();
        }
    }

    fn route(addr: &str, lo: u64, hi: u64) -> TopologyRoute {
        TopologyRoute {
            addr: addr.to_string(),
            replica: None,
            hash_range: Some((lo, hi)),
            placement: None,
        }
    }

    #[test]
    fn topology_refuses_ragged_or_incomplete_hash_space() {
        assert!(
            build_topology(1, vec![route("a", 0, 9), route("b", 11, u64::MAX)], None)
                .err()
                .expect("gap must be refused")
                .contains("gap or overlap")
        );
        assert!(
            build_topology(1, vec![route("a", 0, 10), route("b", 10, u64::MAX)], None)
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
                    placement: None,
                }
            ],
            None
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
                    placement: None,
                },
                route("b", 10, u64::MAX),
            ],
            None,
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
            .reload_topology(5, vec![route("new", 0, u64::MAX)], None)
            .unwrap();
        let new = coordinator.request_snapshot().unwrap();

        assert_eq!(old.topology_generation, 4);
        assert_eq!(old.node_addrs, vec!["old"]);
        assert_eq!(new.topology_generation, 5);
        assert_eq!(new.node_addrs, vec!["new"]);
        assert_eq!(coordinator.current_topology_generation(), 5);
        assert!(coordinator
            .reload_topology(5, vec![route("newer", 0, u64::MAX)], None)
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
                collection: String::new(),
                required_topology_generation: 4,
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert!(coordinator
            .reload_topology(5, vec![route("other", 0, u64::MAX)], None)
            .unwrap_err()
            .contains("frozen writes"));

        let split = u64::MAX / 2;
        SearchService::publish_topology(
            &coordinator,
            Request::new(PublishTopologyRequest {
                collection: String::new(),
                placement: None,
                cutover_token: frozen.cutover_token,
                generation: 5,
                shards: vec![
                    crate::pb::PublishedTopologyShard {
                        addr: "a:50051".into(),
                        replica: String::new(),
                        hash_lo: 0,
                        hash_hi: split,
                        has_placement: false,
                        placement: 0,
                    },
                    crate::pb::PublishedTopologyShard {
                        addr: "b:50051".into(),
                        replica: String::new(),
                        hash_lo: split + 1,
                        hash_hi: u64::MAX,
                        has_placement: false,
                        placement: 0,
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
                collection: String::new(),
                required_topology_generation: 5,
            }),
        )
        .await
        .unwrap()
        .into_inner();
        let aborted = SearchService::abort_topology_cutover(
            &coordinator,
            Request::new(AbortTopologyCutoverRequest {
                collection: String::new(),
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
        let coordinator = CoordinatorServiceImpl::with_local_nodes(Vec::new());
        assert!(!coordinator.allows_network());
        let error = coordinator
            .node_client("http://must-not-resolve.invalid:50051")
            .expect_err("a missing in-process link must not dial");
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
            scoped: false,
            readers: tokio::task::JoinSet::new(),
            merged_rx,
            floor_txs: vec![Some(request_tx)],
            udp_lanes: vec![Some((token, target))],
            udp_socket: Some(udp_tx),
            udp_key: None,
            udp_seq: Arc::new(std::sync::atomic::AtomicU32::new(0)),
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
            scoped: false,
            readers: tokio::task::JoinSet::new(),
            merged_rx,
            floor_txs: vec![Some(request_tx)],
            udp_lanes: vec![Some((token, target))],
            udp_socket: Some(udp_tx),
            udp_key: None,
            udp_seq: Arc::new(std::sync::atomic::AtomicU32::new(0)),
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

#[cfg(test)]
mod candidate_fetch_tests {
    use super::*;
    use crate::pb::*;

    #[tokio::test]
    async fn membership_planning_keeps_authority_separate_from_user_fields_and_versions() {
        let mut nodes = Vec::new();
        for shard in 0..2 {
            let node = Arc::new(crate::node::NodeServiceImpl::new(
                None,
                crate::node::NodeConfig {
                    slot_offset: shard * 100,
                    analysis_addr: Some(crate::analyzer::NATIVE_ANALYSIS_BACKEND.into()),
                    facet_fields: vec!["audience".into(), "color".into()],
                    ..Default::default()
                },
            ));
            crate::link::NodeLink::local(node.clone())
                .add_documents(tokio_stream::iter(
                    [("public", "red", "alpha"), ("private", "blue", "secret")]
                        .into_iter()
                        .map(|(audience, color, text)| AddDocumentsRequest {
                            text: text.into(),
                            analysis: Some(crate::analyzer::body_spec()),
                            facets: vec![
                                FacetValue {
                                    field: "audience".into(),
                                    value: audience.into(),
                                },
                                FacetValue {
                                    field: "color".into(),
                                    value: color.into(),
                                },
                            ],
                            ..Default::default()
                        }),
                ))
                .await
                .unwrap();
            nodes.push(node);
        }
        let owner = CoordinatorServiceImpl::with_local_nodes(nodes.clone()).with_bm25(
            Some(crate::analyzer::NATIVE_ANALYSIS_BACKEND.into()),
            Default::default(),
        );
        let access = AccessDecision {
            action: AccessAction::Search as i32,
            document_visibility: Some(DocumentVisibility {
                filter: crate::cel::compile_filter("audience == 'public'").unwrap(),
            }),
            field_permissions: Some(FieldPermissions {
                grants: ["body", "color"]
                    .into_iter()
                    .map(|field| FieldGrant {
                        field: field.into(),
                        actions: vec![FieldAction::Use as i32],
                    })
                    .collect(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut reader = owner.for_access(Some(&access), "bm25_search").unwrap();
        reader.query_read_versions = Some(Arc::new(
            reader
                .read_query_versions(false)
                .await
                .unwrap()
                .into_iter()
                .map(|(_, claim)| claim)
                .collect(),
        ));
        let all = RequestFilters::default();
        let universe = reader.filter_membership(&all).await.unwrap();
        assert_eq!(universe.ids, [0, 100].into_iter().collect());
        assert_eq!(universe.epochs.len(), 2);
        let selected = reader
            .lexical_membership("alpha", Some(&crate::analyzer::body_spec()))
            .await
            .unwrap();
        assert_eq!(selected.ids, universe.ids);
        assert_eq!(selected.epochs, universe.epochs);
        assert!(reader
            .lexical_membership("secret", Some(&crate::analyzer::body_spec()))
            .await
            .unwrap()
            .ids
            .is_empty());
        assert!(reader
            .filter_membership(&RequestFilters::compile(&[], "color == 'blue'").unwrap())
            .await
            .unwrap()
            .ids
            .is_empty());
        let denied = reader
            .filter_membership(&RequestFilters::compile(&[], "audience == 'public'").unwrap())
            .await
            .unwrap_err();
        assert_eq!(denied.code(), tonic::Code::PermissionDenied);
        assert_eq!(denied.message(), "field access is not granted");
        assert_eq!(
            reader.vector_membership("").await.unwrap_err().code(),
            tonic::Code::PermissionDenied
        );
        // Exercise the actual negative-only planner. Its starting universe must
        // exclude private rows even when the negative clause matches no visible row.
        let query = QueryRequest {
            k: 4,
            selection: Some(SelectionQuery {
                node: Some(selection_query::Node::Boolean(BooleanQuery {
                    must_not: vec![SelectionQuery {
                        node: Some(selection_query::Node::Search(SearchQuery {
                            id: "excluded".into(),
                            query: Some(search_query::Query::Lexical(LexicalQuery {
                                text: "secret".into(),
                                analysis: Some(crate::analyzer::body_spec()),
                                ..Default::default()
                            })),
                        })),
                    }],
                    ..Default::default()
                })),
            }),
            ..Default::default()
        };
        let planned = crate::query::execute(&reader, query).await.unwrap();
        assert_eq!(
            planned
                .hits
                .iter()
                .map(|hit| hit.doc_id)
                .collect::<BTreeSet<_>>(),
            universe.ids
        );
        let mut doc_only = access.clone();
        doc_only.field_permissions = None;
        let mut document_reader = owner.for_access(Some(&doc_only), "bm25_search").unwrap();
        document_reader.query_read_versions = reader.query_read_versions.clone();
        assert_eq!(
            document_reader.vector_membership("").await.unwrap().epochs,
            universe.epochs
        );
        crate::link::NodeLink::local(nodes[0].clone())
            .add_documents(tokio_stream::iter([AddDocumentsRequest {
                text: "new".into(),
                analysis: Some(crate::analyzer::body_spec()),
                ..Default::default()
            }]))
            .await
            .unwrap();
        for kind in 0..3 {
            let error = match kind {
                0 => document_reader.filter_membership(&all).await.unwrap_err(),
                1 => document_reader
                    .lexical_membership("alpha", Some(&crate::analyzer::body_spec()))
                    .await
                    .unwrap_err(),
                _ => document_reader.vector_membership("").await.unwrap_err(),
            };
            assert_eq!(error.code(), tonic::Code::FailedPrecondition);
            assert!(error
                .message()
                .contains("query data changed during scoped read"));
        }
        let mut absent = doc_only;
        absent.document_visibility = Some(DocumentVisibility {
            filter: crate::cel::compile_filter("private_column == 'yes'").unwrap(),
        });
        let unknown = owner.for_access(Some(&absent), "bm25_search").unwrap();
        for kind in 0..3 {
            let error = match kind {
                0 => unknown.filter_membership(&all).await.unwrap_err(),
                1 => unknown
                    .lexical_membership("alpha", Some(&crate::analyzer::body_spec()))
                    .await
                    .unwrap_err(),
                _ => unknown.vector_membership("").await.unwrap_err(),
            };
            assert_eq!(error.code(), tonic::Code::FailedPrecondition);
            assert!(!error.message().contains("private_column"));
        }
    }

    #[test]
    fn membership_view_validation_refuses_legacy_wrong_scope_and_incomplete_claims() {
        let owner = CoordinatorServiceImpl::with_local_nodes(Vec::new());
        let view = DocumentVisibility {
            filter: crate::cel::compile_filter("audience == 'public'").unwrap(),
        };
        let scope = crate::visibility::VisibilityScope::new(Some(&view)).unwrap();
        let valid = MembershipBitmapResponse {
            stats_epoch: 4,
            stats_incarnation: vec![1; 32],
            visibility_fingerprint: scope.fingerprint().to_vec(),
            visibility_columns_known: vec![true],
            ..Default::default()
        };
        for case in 0..5 {
            let mut malformed = valid.clone();
            match case {
                0 => malformed.visibility_fingerprint.clear(),
                1 => malformed.visibility_columns_known.clear(),
                2 => malformed.stats_epoch = 0,
                3 => malformed.stats_incarnation.clear(),
                _ => malformed.visibility_fingerprint[0] ^= 1,
            }
            let mut known = vec![false];
            assert_eq!(
                owner
                    .check_read_view(0, &scope, &malformed, &mut known)
                    .unwrap_err()
                    .code(),
                tonic::Code::FailedPrecondition
            );
            assert_eq!(known, vec![false]);
        }
        let mut known = vec![false];
        assert_eq!(
            owner
                .check_read_view(0, &scope, &valid, &mut known)
                .unwrap(),
            StatsClaim::required(4, &[1; 32]).unwrap()
        );
        assert_eq!(known, vec![true]);
    }

    #[tokio::test]
    async fn candidate_fetch_enforces_authority_fields_and_documents_before_disclosure() {
        let node = Arc::new(crate::node::NodeServiceImpl::new(
            None,
            crate::node::NodeConfig {
                analysis_addr: Some(crate::analyzer::NATIVE_ANALYSIS_BACKEND.into()),
                facet_fields: vec!["audience".into(), "color".into()],
                numeric_fields: vec!["boost".into()],
                ..Default::default()
            },
        ));
        crate::link::NodeLink::local(node.clone())
            .add_documents(tokio_stream::iter(
                [("public", "red"), ("private", "secret")]
                    .into_iter()
                    .map(|(audience, color)| AddDocumentsRequest {
                        text: "alpha".into(),
                        analysis: Some(crate::analyzer::body_spec()),
                        facets: vec![
                            FacetValue {
                                field: "audience".into(),
                                value: audience.into(),
                            },
                            FacetValue {
                                field: "color".into(),
                                value: color.into(),
                            },
                        ],
                        numerics: vec![NumericValue {
                            field: "boost".into(),
                            value: 2.0,
                        }],
                        ..Default::default()
                    }),
            ))
            .await
            .unwrap();
        let owner = CoordinatorServiceImpl::with_local_nodes(vec![node]);
        let access = AccessDecision {
            action: AccessAction::Search as i32,
            document_visibility: Some(DocumentVisibility {
                filter: crate::cel::compile_filter("audience == 'public'").unwrap(),
            }),
            field_permissions: Some(FieldPermissions {
                grants: vec![
                    FieldGrant {
                        field: "color".into(),
                        actions: vec![FieldAction::Use as i32, FieldAction::Disclose as i32],
                    },
                    FieldGrant {
                        field: "boost".into(),
                        actions: vec![FieldAction::Use as i32],
                    },
                ],
                ..Default::default()
            }),
            ..Default::default()
        };
        // The same authority-bound clone used by certified public routes.
        // Query remains refused until all of its phases consume this context.
        let reader = owner.for_access(Some(&access), "bm25_search").unwrap();
        let projection = |expr: &str| CompiledProjection {
            name: "color".into(),
            expr: Some(crate::cel::compile_value(expr).unwrap()),
        };
        let stages = vec![ScoreStage {
            column: "boost".into(),
            operation: Some(crate::pb::score_stage::Operation::Op(
                ScoreOp::AddLinear as i32,
            )),
            weight: 1.0,
            ..Default::default()
        }];
        let rows = reader
            .fetch_values(&[0, 1], &[projection("color")], &stages)
            .await
            .unwrap();
        assert_eq!(rows.rows.len(), 1);
        assert!(rows.rows.contains_key(&0));
        assert_eq!(rows.stage_rows[0].len(), 1);
        assert!(rows.stage_rows[0].contains_key(&0));
        for forbidden in ["audience", "boost", "true ? color : audience"] {
            let error = reader
                .fetch_values(&[], &[projection(forbidden)], &[])
                .await
                .err()
                .unwrap();
            assert_eq!(error.code(), tonic::Code::PermissionDenied);
            assert_eq!(error.message(), "field access is not granted");
        }
        let mut unknown = access;
        unknown.document_visibility.as_mut().unwrap().filter =
            crate::cel::compile_filter("absent == 'public'").unwrap();
        let reader = owner.for_access(Some(&unknown), "bm25_search").unwrap();
        let error = reader
            .fetch_values(&[], &[projection("color")], &[])
            .await
            .err()
            .unwrap();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(!error.message().contains("absent"));
    }
}

#[cfg(test)]
mod unsigned_aggregate_tests {
    use super::*;
    use crate::pb::{AggregateOp as O, AggregateValueType as T};

    #[test]
    fn percentile_ranks_preserve_wide_counts_and_adjacent_ieee_percentiles() {
        for count in [0, 1, 2, 3, (1u64 << 53) - 1, (1u64 << 53) + 1, u64::MAX] {
            for (p, numerator, denominator) in [
                (0.0, 0, 1),
                (25.0, 1, 4),
                (50.0, 1, 2),
                (75.0, 3, 4),
                (100.0, 1, 1),
            ] {
                let expected = if count == 0 {
                    0
                } else {
                    ((u128::from(count) * numerator).div_ceil(denominator) as u64).max(1)
                };
                assert_eq!(
                    nearest_percentile_rank(p, count),
                    expected,
                    "p={p} n={count}"
                );
            }
            assert_eq!(
                nearest_percentile_rank(f64::from_bits(1), count),
                u64::from(count > 0)
            );
        }
        assert_eq!(
            nearest_percentile_rank(f64::from_bits(50.0f64.to_bits() - 1), 2),
            1
        );
        assert_eq!(
            nearest_percentile_rank(f64::from_bits(50.0f64.to_bits() + 1), 2),
            2
        );
    }

    #[test]
    fn unsigned_partials_merge_wide_before_checked_result_narrowing() {
        let agg = crate::pb::CompiledAggregation {
            name: "total".into(),
            op: O::Sum as i32,
            ..Default::default()
        };
        let mut merged = AggMerge::new();
        let partial = crate::pb::AggregatePartial {
            vtype: T::Uint as i32,
            present: 1,
            uint_sum_lo: u64::MAX,
            uint_min: u64::MAX,
            uint_max: u64::MAX,
            ..Default::default()
        };
        merged.fold(&partial, &agg).unwrap();
        assert_eq!(
            merged.result("total", O::Sum).unwrap().value,
            Some(crate::pb::aggregate_result::Value::UintValue(u64::MAX))
        );
        merged.fold(&partial, &agg).unwrap();
        assert_eq!(merged.uint_sum, 2 * u128::from(u64::MAX));
        assert!(merged
            .result("total", O::Sum)
            .unwrap_err()
            .message()
            .contains("does not fit u64"));
        assert_eq!(
            merged.result("total", O::Max).unwrap().value,
            Some(crate::pb::aggregate_result::Value::UintValue(u64::MAX))
        );
        let conflicting = crate::pb::AggregatePartial {
            vtype: T::Int as i32,
            present: 0,
            ..Default::default()
        };
        assert!(merged
            .fold(&conflicting, &agg)
            .unwrap_err()
            .message()
            .contains("shards disagree"));
        let mut pct = PctMerge::new();
        pct.fold(
            &crate::pb::PercentilePartial {
                vtype: T::Uint as i32,
                ..Default::default()
            },
            "p",
        )
        .unwrap();
        assert!(pct
            .fold(
                &crate::pb::PercentilePartial {
                    vtype: T::Int as i32,
                    ..Default::default()
                },
                "p"
            )
            .unwrap_err()
            .message()
            .contains("shards disagree"));
    }

    #[test]
    fn aggregate_count_and_partial_overflows_refuse_without_wrapping() {
        let agg = crate::pb::CompiledAggregation {
            name: "total".into(),
            op: O::Sum as i32,
            ..Default::default()
        };
        let mut merged = AggMerge::new();
        let huge = crate::pb::AggregatePartial {
            vtype: T::Uint as i32,
            present: u64::MAX,
            ..Default::default()
        };
        merged.fold(&huge, &agg).unwrap();
        assert!(merged
            .result("total", O::Count)
            .unwrap_err()
            .message()
            .contains("does not fit"));
        let single = crate::pb::AggregatePartial {
            vtype: T::Uint as i32,
            present: 1,
            uint_sum_lo: 1,
            ..Default::default()
        };
        assert!(merged
            .fold(&single, &agg)
            .unwrap_err()
            .message()
            .contains("count overflows"));
        let mut merged = AggMerge::new();
        let bad_sum = crate::pb::AggregatePartial {
            vtype: T::Uint as i32,
            present: 1,
            uint_sum_hi: u64::MAX,
            uint_sum_lo: u64::MAX,
            ..Default::default()
        };
        merged.fold(&bad_sum, &agg).unwrap();
        assert!(merged
            .fold(&single, &agg)
            .unwrap_err()
            .message()
            .contains("sum overflows u128"));
    }
}

#[cfg(test)]
mod scoped_fold_tests {
    use super::*;
    use crate::pb::*;

    #[tokio::test]
    async fn browse_checks_fields_before_io_and_uses_the_authority_view() {
        let node = Arc::new(crate::node::NodeServiceImpl::new(
            None,
            crate::node::NodeConfig {
                analysis_addr: Some(crate::analyzer::NATIVE_ANALYSIS_BACKEND.into()),
                facet_fields: vec!["audience".into(), "color".into()],
                ..Default::default()
            },
        ));
        let mut link = crate::link::NodeLink::local(node.clone());
        for (audience, color) in [("public", "red"), ("private", "blue")] {
            link.add_documents(tokio_stream::iter([AddDocumentsRequest {
                text: "alpha".into(),
                analysis: Some(crate::analyzer::body_spec()),
                facets: vec![
                    FacetValue {
                        field: "audience".into(),
                        value: audience.into(),
                    },
                    FacetValue {
                        field: "color".into(),
                        value: color.into(),
                    },
                ],
                ..Default::default()
            }]))
            .await
            .unwrap();
        }
        let owner = CoordinatorServiceImpl::with_local_nodes(vec![node]);
        let access = AccessDecision {
            action: AccessAction::Search as i32,
            document_visibility: Some(DocumentVisibility {
                filter: crate::cel::compile_filter("audience == 'public'").unwrap(),
            }),
            field_permissions: Some(FieldPermissions {
                grants: vec![
                    FieldGrant {
                        field: "body".into(),
                        actions: vec![FieldAction::Use as i32],
                    },
                    FieldGrant {
                        field: "color".into(),
                        actions: vec![FieldAction::Use as i32, FieldAction::Disclose as i32],
                    },
                ],
                ..Default::default()
            }),
            ..Default::default()
        };
        let reader = owner.for_access(Some(&access), "aggregate").unwrap();
        let sort = [BrowseSort {
            column: "color".into(),
            descending: false,
        }];
        let rows = reader
            .fanout_browse(10, None, &sort, &[], 0, &RequestFilters::default())
            .await
            .unwrap();
        assert_eq!(rows.ids, vec![0]);
        assert_eq!(
            rows.values,
            vec![vec![crate::sortkeys::Value::Text("red".into())]]
        );
        assert!(reader
            .fanout_browse(
                10,
                None,
                &sort,
                &[],
                0,
                &RequestFilters::compile(&[], "color == 'blue'").unwrap()
            )
            .await
            .unwrap()
            .ids
            .is_empty());
        let (pinned, _) = reader.pin_read_versions().await.unwrap();
        link.add_documents(tokio_stream::iter([AddDocumentsRequest {
            text: "changed".into(),
            analysis: Some(crate::analyzer::body_spec()),
            ..Default::default()
        }]))
        .await
        .unwrap();
        assert_eq!(
            pinned
                .fanout_browse(10, None, &[], &[], 0, &RequestFilters::default())
                .await
                .err()
                .unwrap()
                .code(),
            tonic::Code::FailedPrecondition
        );
        // Even a broken transport must not be touched for a forbidden input.
        let mut denied = reader;
        denied.node_addrs = vec!["http://must-not-resolve.invalid:50051".into()];
        for (sort, terms, filter) in [
            (
                vec![BrowseSort {
                    column: "body".into(),
                    descending: false,
                }],
                vec![],
                "",
            ),
            (vec![], vec![], "audience == 'public'"),
            (
                vec![BrowseSort {
                    column: "parent_id".into(),
                    descending: false,
                }],
                vec![],
                "",
            ),
        ] {
            assert_eq!(
                denied
                    .fanout_browse(
                        10,
                        None,
                        &sort,
                        &terms,
                        0,
                        &RequestFilters::compile(&[], filter).unwrap()
                    )
                    .await
                    .err()
                    .unwrap()
                    .code(),
                tonic::Code::PermissionDenied
            );
        }
        denied.field_permissions =
            Some(crate::field_permissions::FieldScope::new(&FieldPermissions::default()).unwrap());
        assert_eq!(
            denied
                .fanout_browse(
                    10,
                    None,
                    &[],
                    &["alpha".into()],
                    0,
                    &RequestFilters::default()
                )
                .await
                .err()
                .unwrap()
                .code(),
            tonic::Code::PermissionDenied
        );
    }

    #[test]
    fn fold_response_metadata_is_required_before_any_partial_is_merged() {
        let view = DocumentVisibility {
            filter: crate::cel::compile_filter("audience == 'public'").unwrap(),
        };
        let scope = crate::visibility::VisibilityScope::new(Some(&view)).unwrap();
        let mut owner = CoordinatorServiceImpl::with_local_nodes(vec![]);
        owner.query_read_versions =
            Some(Arc::new(vec![StatsClaim::required(4, &[1; 32]).unwrap()]));
        macro_rules! verify {
            ($ty:ident) => {{
                let valid = $ty {
                    stats_epoch: 4,
                    stats_incarnation: vec![1; 32],
                    visibility_fingerprint: scope.fingerprint().to_vec(),
                    visibility_columns_known: vec![true],
                    ..Default::default()
                };
                for case in 0..7 {
                    let mut response = valid.clone();
                    match case {
                        0 => response.visibility_fingerprint.clear(),
                        1 => response.visibility_fingerprint = vec![5; 32],
                        2 => response.visibility_columns_known.clear(),
                        3 => response.stats_epoch = 0,
                        4 => response.stats_incarnation.clear(),
                        5 => response.stats_epoch += 1,
                        _ => response.stats_incarnation = vec![2; 32],
                    }
                    let mut known = vec![false];
                    assert_eq!(
                        owner
                            .check_read_view(0, &scope, &response, &mut known)
                            .unwrap_err()
                            .code(),
                        tonic::Code::FailedPrecondition
                    );
                    assert_eq!(known, vec![false]);
                }
                let mut known = vec![false];
                owner
                    .check_read_view(0, &scope, &valid, &mut known)
                    .unwrap();
                assert_eq!(known, vec![true]);
            }};
        }
        verify!(BrowseShardResponse);
        verify!(AggregateShardResponse);
        verify!(QuantileCountsResponse);
    }
}

#[cfg(test)]
mod lineage_read_tests {
    use super::*;
    use crate::pb::*;
    #[tokio::test]
    async fn lineage_authority_projects_each_key_and_guards_the_selection_version() {
        let node = Arc::new(crate::node::NodeServiceImpl::new(
            None,
            crate::node::NodeConfig {
                analysis_addr: Some(crate::analyzer::NATIVE_ANALYSIS_BACKEND.into()),
                facet_fields: vec!["audience".into()],
                ..Default::default()
            },
        ));
        let mut link = crate::link::NodeLink::local(node.clone());
        for (audience, parent) in [("public", 10), ("private", 99)] {
            link.add_documents(tokio_stream::iter([AddDocumentsRequest {
                text: "alpha".into(),
                analysis: Some(crate::analyzer::body_spec()),
                facets: vec![FacetValue {
                    field: "audience".into(),
                    value: audience.into(),
                }],
                lineage: Some(DocLineage {
                    parent_id: parent,
                    group_id: 200 + parent,
                    ..Default::default()
                }),
                ..Default::default()
            }]))
            .await
            .unwrap();
        }
        let owner = CoordinatorServiceImpl::with_local_nodes(vec![node]);
        let access = AccessDecision {
            action: AccessAction::Search as i32,
            document_visibility: Some(DocumentVisibility {
                filter: crate::cel::compile_filter("audience == 'public'").unwrap(),
            }),
            field_permissions: Some(FieldPermissions {
                grants: vec![FieldGrant {
                    field: "parent_id".into(),
                    actions: vec![FieldAction::Use as i32, FieldAction::Disclose as i32],
                }],
                ..Default::default()
            }),
            ..Default::default()
        };
        let reader = owner.for_access(Some(&access), "aggregate").unwrap();
        assert_eq!(
            reader.lineage_key(&[0, 1], "parent_id").await.unwrap(),
            [(0, 10)].into_iter().collect()
        );
        assert_eq!(
            reader
                .lineage_key(&[0], "group_id")
                .await
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
        assert_eq!(
            reader.lineage_keys(&[]).await.unwrap_err().code(),
            tonic::Code::PermissionDenied
        );
        let (pinned, _) = reader.pin_read_versions().await.unwrap();
        link.add_documents(tokio_stream::iter([AddDocumentsRequest {
            text: "changed".into(),
            analysis: Some(crate::analyzer::body_spec()),
            ..Default::default()
        }]))
        .await
        .unwrap();
        assert_eq!(
            pinned
                .lineage_key(&[0], "parent_id")
                .await
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
        let mut denied = reader.clone();
        denied.node_addrs = vec!["http://must-not-resolve.invalid:50051".into()];
        for actions in [
            vec![FieldAction::Use as i32],
            vec![FieldAction::Disclose as i32],
        ] {
            denied.field_permissions = Some(
                crate::field_permissions::FieldScope::new(&FieldPermissions {
                    grants: vec![FieldGrant {
                        field: "parent_id".into(),
                        actions,
                    }],
                    ..Default::default()
                })
                .unwrap(),
            );
            assert_eq!(
                denied
                    .lineage_key(&[], "parent_id")
                    .await
                    .unwrap_err()
                    .code(),
                tonic::Code::PermissionDenied
            );
        }
        let mut unknown = reader;
        unknown.document_visibility = Some(DocumentVisibility {
            filter: crate::cel::compile_filter("internal_secret == 'public'").unwrap(),
        });
        let error = unknown.lineage_key(&[], "parent_id").await.unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(!error.message().contains("internal_secret"));
    }
}

#[cfg(test)]
mod vector_field_read_tests {
    use super::*;
    use crate::pb::node_service_server::NodeService;
    use crate::pb::*;
    use prost::Message;

    fn node(offset: u64, fingerprint_override: bool) -> Arc<crate::node::NodeServiceImpl> {
        node_rows(offset, fingerprint_override, 2, 2)
    }
    fn node_rows(
        offset: u64,
        fingerprint_override: bool,
        docs: u32,
        rows: usize,
    ) -> Arc<crate::node::NodeServiceImpl> {
        node_rows_with_private_terms(offset, fingerprint_override, docs, rows, 1)
    }
    fn node_rows_with_private_terms(
        offset: u64,
        fingerprint_override: bool,
        docs: u32,
        rows: usize,
        private_terms: u32,
    ) -> Arc<crate::node::NodeServiceImpl> {
        let plan = crate::mapping::derive_plan(
            include_bytes!("../tests/fixtures/vector-binding/descriptor.bin"),
            "vector_binding.Named",
        )
        .unwrap();
        let mut binding = plan.vector_binding.unwrap();
        if fingerprint_override {
            binding.plan_fingerprint = "b".repeat(64);
        }
        let mut store = crate::postings::Bm25Store::new().with_facets(&["audience"]);
        store.set_binding(Some(crate::postings::StoredBinding {
            plan_fingerprint: binding.plan_fingerprint.clone(),
            body_path: "body".into(),
            vector_binding: binding.encode_to_vec(),
            ..Default::default()
        }));
        for row in 0..docs {
            let count = if row == 0 { 1 } else { private_terms };
            store.add_document(
                row,
                vec!["word"; count as usize].join(" "),
                crate::postings::AnalyzedDoc::body(
                    vec![(
                        "word".into(),
                        count,
                        (0..count).map(|i| (i * 5, i * 5 + 4)).collect(),
                    )],
                    count,
                ),
            );
            store.set_facet(0, row, if row == 0 { "public" } else { "private" });
        }
        let vectors = vec![0.25; rows * 16];
        let config = crate::vector::embedded_turbovec_config(4, &[0.0; 16], &[1.0; 16]).unwrap();
        let mut index = crate::vector::VectorIndex::from_backend_config(16, &config).unwrap();
        index.add(&vectors, 16).unwrap();
        Arc::new(
            crate::node::NodeServiceImpl::new(
                Some(index),
                crate::node::NodeConfig {
                    slot_offset: offset,
                    facet_fields: vec!["audience".into()],
                    ..Default::default()
                },
            )
            .with_bm25(Some(crate::node::Bm25Shard::Building(store)))
            .with_exact_vectors(Some(
                crate::exact_vectors::ExactVectorStore::from_values(16, vectors).unwrap(),
            ))
            .unwrap(),
        )
    }
    fn access(field: &str, action: FieldAction) -> AccessDecision {
        AccessDecision {
            action: AccessAction::Search as i32,
            document_visibility: Some(DocumentVisibility {
                filter: crate::cel::compile_filter("audience == 'public'").unwrap(),
            }),
            field_permissions: Some(FieldPermissions {
                grants: vec![FieldGrant {
                    field: field.into(),
                    actions: vec![action as i32],
                }],
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn auto_ann_qualifies_the_document_view_instead_of_the_unfiltered_point() {
        let node = node(0, false);
        let fingerprint = {
            let mut guard = crate::node::write_shard(&node.state);
            let index = guard.index.take().unwrap();
            let fingerprint = crate::harness::fake_ann::fingerprint_of(&index);
            guard.index = Some(crate::harness::fake_ann::fake_ann_index(index));
            fingerprint
        };
        let policy = crate::dense_policy::DenseExecutionPolicy::parse(&format!(
            r#"
format_version = 1
policy_id = "document-view"
embedding_model = "test"
corpus_generation = 0
corpus_rows = 2
dimensions = 16
provider_backend = "fake-ann"
scoring_fingerprint = "{fingerprint}"
measured_queries = 20
[[points]]
k = 1
filter_selectivity_ppm_min = 1000000
filter_selectivity_ppm_max = 1000000
candidates = 1
measured_recall_ppm = 990000
[[points]]
k = 1
filter_selectivity_ppm_min = 500000
filter_selectivity_ppm_max = 500000
candidates = 2
measured_recall_ppm = 980000
"#
        ))
        .unwrap();
        let owner = CoordinatorServiceImpl::with_local_nodes(vec![node])
            .with_dense_execution_policy(policy);
        let key = DenseRequestKey {
            k: 1,
            candidate_depth: 0,
            filters: None,
        };
        let unrestricted = owner
            .resolve_dense_execution(DenseExecutionMode::Auto, 16, key)
            .await
            .unwrap();
        assert_eq!(unrestricted.candidate_depth, 1);
        let reader = owner
            .for_access(Some(&access("semantic", FieldAction::Use)), "bm25_search")
            .unwrap()
            .for_vector_field("semantic")
            .unwrap();
        let restricted = reader
            .resolve_dense_execution(DenseExecutionMode::Auto, 16, key)
            .await
            .unwrap();
        assert_eq!(restricted.resolved_mode, DenseExecutionMode::Ann as i32);
        assert_eq!(restricted.filter_selectivity_ppm, 500_000);
        assert_eq!(restricted.candidate_depth, 2);
        assert_eq!(
            restricted.policy_point.unwrap().measured_recall_ppm,
            980_000
        );
        // An explicit depth from the unfiltered point cannot borrow its evidence.
        assert_eq!(
            reader
                .resolve_dense_execution(
                    DenseExecutionMode::Auto,
                    16,
                    DenseRequestKey {
                        candidate_depth: 1,
                        ..key
                    }
                )
                .await
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
    }

    #[tokio::test]
    async fn dense_policy_selectivity_uses_authorized_vector_membership() {
        let nodes = vec![node_rows(0, false, 4, 2)];
        let owner = CoordinatorServiceImpl::with_local_nodes(nodes.clone());
        let reader = owner
            .for_access(Some(&access("semantic", FieldAction::Use)), "bm25_search")
            .unwrap()
            .for_vector_field("semantic")
            .unwrap();
        assert_eq!(
            reader.dense_filter_selectivity(None, 2).await.unwrap(),
            500_000
        );
        assert_eq!(
            reader
                .dense_filter_selectivity(Some(&RequestFilters::default()), 2)
                .await
                .unwrap(),
            500_000
        );
        let private = RequestFilters {
            tree: crate::cel::compile_filter("audience == 'private'").unwrap(),
            ..Default::default()
        };
        // Three matching documents, but only one owns an indexed vector.
        assert_eq!(
            owner
                .dense_filter_selectivity(Some(&private), 2)
                .await
                .unwrap(),
            500_000
        );
        assert_eq!(
            owner.dense_filter_selectivity(None, 2).await.unwrap(),
            1_000_000
        );
        assert_eq!(
            reader
                .dense_filter_selectivity(Some(&private), 2)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
        let mut decision = access("semantic", FieldAction::Use);
        decision
            .field_permissions
            .as_mut()
            .unwrap()
            .grants
            .push(FieldGrant {
                field: "audience".into(),
                actions: vec![FieldAction::Use as i32],
            });
        let reader = owner
            .for_access(Some(&decision), "bm25_search")
            .unwrap()
            .for_vector_field("semantic")
            .unwrap();
        assert_eq!(
            reader
                .dense_filter_selectivity(Some(&private), 2)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            owner
                .for_vector_field("missing")
                .unwrap()
                .dense_filter_selectivity(Some(&private), 2)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
        let (pinned, _) = reader.pin_read_versions().await.unwrap();
        nodes[0]
            .delete_documents(Request::new(DeleteDocumentsRequest {
                doc_ids: vec![0],
                ..Default::default()
            }))
            .await
            .unwrap();
        assert_eq!(
            pinned
                .dense_filter_selectivity(None, 2)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
        assert_eq!(reader.dense_filter_selectivity(None, 2).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn public_field_granted_dense_queries_require_the_indexed_name() {
        use crate::pb::{
            search_query, selection_query, DenseQuery, QueryRequest, SearchQuery, SelectionQuery,
        };
        let owner =
            CoordinatorServiceImpl::with_local_nodes(vec![node(0, false), node(100, false)]);
        let mut decision = access("semantic", FieldAction::Use);
        decision.document_visibility = None;
        let reader = owner.for_access(Some(&decision), "query").unwrap();
        for streaming in [false, true] {
            let reader = reader.clone().with_stream_search(streaming);
            for mode in [
                crate::pb::DenseScoreMode::Native,
                crate::pb::DenseScoreMode::Fp32Rerank,
            ] {
                let make_query = |field: &str| QueryRequest {
                    k: 4,
                    selection: Some(SelectionQuery {
                        node: Some(selection_query::Node::Search(SearchQuery {
                            id: "dense".into(),
                            query: Some(search_query::Query::Dense(DenseQuery {
                                field: field.into(),
                                vector: vec![0.25; 16],
                                score_mode: mode as i32,
                                ..Default::default()
                            })),
                        })),
                    }),
                    ..Default::default()
                };
                let expected = SearchService::query(&owner, Request::new(make_query("semantic")))
                    .await
                    .unwrap()
                    .into_inner();
                let actual = SearchService::query(&reader, Request::new(make_query("semantic")))
                    .await
                    .unwrap()
                    .into_inner();
                assert_eq!(actual.hits, expected.hits);
                for field in ["", "signal", "body"] {
                    assert_eq!(
                        SearchService::query(&reader, Request::new(make_query(field)))
                            .await
                            .unwrap_err()
                            .code(),
                        tonic::Code::PermissionDenied
                    );
                }
            }
        }
    }

    fn hybrid_query(strategy: selection_score_strategy::Strategy, field: &str) -> QueryRequest {
        let operator = if matches!(strategy, selection_score_strategy::Strategy::Cascade(_)) {
            SelectionOperator::Unspecified
        } else {
            SelectionOperator::Or
        };
        QueryRequest {
            k: 4,
            selection: Some(SelectionQuery {
                node: Some(selection_query::Node::Composite(CompositeSearchStrategy {
                    operator: operator as i32,
                    clauses: vec![
                        SelectionQuery {
                            node: Some(selection_query::Node::Search(SearchQuery {
                                id: "dense".into(),
                                query: Some(search_query::Query::Dense(DenseQuery {
                                    field: field.into(),
                                    vector: vec![0.25; 16],
                                    ..Default::default()
                                })),
                            })),
                        },
                        SelectionQuery {
                            node: Some(selection_query::Node::Search(SearchQuery {
                                id: "lexical".into(),
                                query: Some(search_query::Query::Lexical(LexicalQuery {
                                    text: "word".into(),
                                    analysis: Some(crate::analyzer::body_spec()),
                                    ..Default::default()
                                })),
                            })),
                        },
                    ],
                    scoring: Some(SelectionScoreStrategy {
                        strategy: Some(strategy),
                    }),
                })),
            }),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn document_query_executor_redacts_physical_details_across_selection_shapes() {
        use selection_score_strategy::Strategy;
        let mut decision = access("semantic", FieldAction::Use);
        decision
            .field_permissions
            .as_mut()
            .unwrap()
            .grants
            .push(FieldGrant {
                field: "body".into(),
                actions: vec![FieldAction::Use as i32, FieldAction::Disclose as i32],
            });
        let owner =
            CoordinatorServiceImpl::with_local_nodes(vec![node(0, false), node(100, false)])
                .with_bm25(
                    Some(crate::analyzer::NATIVE_ANALYSIS_BACKEND.into()),
                    Default::default(),
                );
        let reader = owner.for_access(Some(&decision), "query").unwrap();
        let mut requests = vec![];
        for mode in [DenseScoreMode::Unspecified, DenseScoreMode::Fp32Rerank] {
            requests.push(QueryRequest {
                k: 4,
                selection_k: 4,
                selection: Some(SelectionQuery {
                    node: Some(selection_query::Node::Search(SearchQuery {
                        id: "dense".into(),
                        query: Some(search_query::Query::Dense(DenseQuery {
                            field: "semantic".into(),
                            vector: vec![0.25; 16],
                            score_mode: mode as i32,
                            ..Default::default()
                        })),
                    })),
                }),
                ..Default::default()
            });
        }
        for strategy in [
            Strategy::Rrf(RrfScore::default()),
            Strategy::ScoreBlend(BlendScore::default()),
            Strategy::Decomposed(DecomposedScore::default()),
            Strategy::Cascade(CascadeScore {
                gate_id: "dense".into(),
            }),
        ] {
            requests.push(hybrid_query(strategy, "semantic"));
        }
        let dense = requests[0].selection.clone().unwrap();
        requests.push(QueryRequest {
            k: 4,
            selection: Some(SelectionQuery {
                node: Some(selection_query::Node::Boolean(BooleanQuery {
                    must: vec![dense],
                    ..Default::default()
                })),
            }),
            ..Default::default()
        });
        requests.push(QueryRequest {
            k: 4,
            selection: Some(SelectionQuery {
                node: Some(selection_query::Node::Search(SearchQuery {
                    id: "lexical".into(),
                    query: Some(search_query::Query::Lexical(LexicalQuery {
                        text: "word".into(),
                        analysis: Some(crate::analyzer::body_spec()),
                        ..Default::default()
                    })),
                })),
            }),
            ..Default::default()
        });
        let mut saw_physical_work = false;
        for mut request in requests {
            request.profile = true;
            let (pinned, _) = reader.pin_read_versions().await.unwrap();
            let raw = crate::query::execute(&pinned, request.clone())
                .await
                .unwrap();
            let reply = reader
                .execute_query(request.clone(), Some(&decision))
                .await
                .unwrap();
            assert_eq!(reply.hits, raw.hits, "{}", reply.executed);
            assert!(reply
                .hits
                .iter()
                .all(|hit| hit.doc_id == 0 || hit.doc_id == 100));
            assert!(reply.execution_details_redacted);
            let profile = reply.profile.as_ref().unwrap();
            saw_physical_work |= raw.profile.as_ref().unwrap().rerank_rows > 0;
            assert_eq!(
                (
                    profile.rerank_rows,
                    profile.rerank_logical_bytes,
                    profile.rerank_pages
                ),
                (0, 0, 0)
            );
            assert_eq!(
                (
                    profile.rerank_tasks,
                    profile.segments_total,
                    profile.segments_skipped,
                    profile.shards_total,
                    profile.shards_skipped
                ),
                (0, 0, 0, 0, 0)
            );
            if let Some(execution) = reply.dense_execution {
                assert!(execution.planner_reason.is_empty());
                assert!(execution.exhaustive_completion);
                assert_eq!(
                    execution.evidence_scope,
                    DenseEvidenceScope::NotApplicable as i32
                );
            }
            use tokio_stream::StreamExt;
            let mut stream_request = Request::new(QueryStreamRequest {
                query: Some(request.clone()),
                ..Default::default()
            });
            stream_request.extensions_mut().insert(decision.clone());
            let mut stream = SearchService::query_stream(&reader, stream_request)
                .await
                .unwrap()
                .into_inner();
            let mut completed = false;
            let mut provisional = false;
            while let Some(event) = stream.next().await {
                assert!(!completed);
                match event.unwrap().payload.unwrap() {
                    query_stream_response::Payload::Revision(revision) => {
                        assert!(
                            revision
                                .hits
                                .iter()
                                .all(|hit| matches!(hit.doc_id, 0 | 100)),
                            "{}: private provisional hit",
                            raw.executed
                        );
                        provisional |= !revision.hits.is_empty()
                            && revision.phase != QueryStreamPhase::Final as i32;
                    }
                    query_stream_response::Payload::Completion(end) => {
                        assert!(end.completed, "{}: {}", raw.executed, end.error_message);
                        let response = end.response.unwrap();
                        assert_eq!(response.hits, raw.hits);
                        assert!(response.execution_details_redacted);
                        completed = true;
                    }
                }
            }
            assert!(completed);
            // Single scored leaves have a streaming collector; composite
            // membership can execute without publishing a provisional heap.
            if matches!(
                request.selection.as_ref().and_then(|s| s.node.as_ref()),
                Some(selection_query::Node::Search(_))
            ) {
                assert!(
                    provisional,
                    "{} must exercise provisional results",
                    raw.executed
                );
            }
            let unrestricted = owner.execute_query(request, None).await.unwrap();
            assert!(!unrestricted.execution_details_redacted);
        }
        assert!(
            saw_physical_work,
            "the test must exercise actual FP32 row reads"
        );
    }

    #[tokio::test]
    async fn public_hybrid_field_grants_validate_actual_bindings_in_every_fusion_mode() {
        use selection_score_strategy::Strategy;
        let mut decision = access("semantic", FieldAction::Use);
        decision.document_visibility = None;
        decision.field_permissions.as_mut().unwrap().grants.extend([
            FieldGrant {
                field: "body".into(),
                actions: vec![FieldAction::Use as i32],
            },
            // A permitted name is still not proof the shard indexed that field.
            FieldGrant {
                field: "signal".into(),
                actions: vec![FieldAction::Use as i32],
            },
        ]);
        let cluster = |different| {
            CoordinatorServiceImpl::with_local_nodes(vec![node(0, false), node(100, different)])
                .with_bm25(
                    Some(crate::analyzer::NATIVE_ANALYSIS_BACKEND.into()),
                    Default::default(),
                )
        };
        let owner = cluster(false);
        let reader = owner.for_access(Some(&decision), "query").unwrap();
        let incompatible = cluster(true).for_access(Some(&decision), "query").unwrap();
        for strategy in [
            Strategy::Rrf(RrfScore::default()),
            Strategy::Rrf(RrfScore {
                dense_weight: Some(0.0),
                ..Default::default()
            }),
            Strategy::ScoreBlend(BlendScore::default()),
            Strategy::Decomposed(DecomposedScore::default()),
            Strategy::Cascade(CascadeScore {
                gate_id: "dense".into(),
            }),
        ] {
            let query = hybrid_query(strategy, "semantic");
            let expected = SearchService::query(&owner, Request::new(query.clone()))
                .await
                .unwrap()
                .into_inner();
            let actual = SearchService::query(&reader, Request::new(query.clone()))
                .await
                .unwrap()
                .into_inner();
            assert!(!actual.hits.is_empty());
            assert_eq!(actual.hits, expected.hits);
            let error = SearchService::query(&incompatible, Request::new(query.clone()))
                .await
                .unwrap_err();
            assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
            assert!(error.message().contains("binding"), "{error}");
            use tokio_stream::StreamExt;
            let mut stream = SearchService::query_stream(
                &incompatible,
                Request::new(QueryStreamRequest {
                    query: Some(query.clone()),
                    timeout_ms: 5_000,
                    ..Default::default()
                }),
            )
            .await
            .unwrap()
            .into_inner();
            let mut failed = false;
            while let Some(frame) = stream.next().await {
                match frame.unwrap().payload.unwrap() {
                    query_stream_response::Payload::Revision(revision) => {
                        assert!(revision.hits.is_empty())
                    }
                    query_stream_response::Payload::Completion(completion) => {
                        assert!(!completion.completed);
                        assert!(completion.response.is_none());
                        assert_eq!(
                            completion.error_code,
                            tonic::Code::FailedPrecondition as u32
                        );
                        failed = true;
                    }
                }
            }
            assert!(failed);
            let mut alias = query;
            let Some(selection_query::Node::Composite(composite)) =
                alias.selection.as_mut().unwrap().node.as_mut()
            else {
                unreachable!()
            };
            let Some(selection_query::Node::Search(leaf)) = composite.clauses[0].node.as_mut()
            else {
                unreachable!()
            };
            let Some(search_query::Query::Dense(dense)) = leaf.query.as_mut() else {
                unreachable!()
            };
            dense.field = "signal".into();
            let error = SearchService::query(&reader, Request::new(alias))
                .await
                .unwrap_err();
            assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
        }
    }

    #[tokio::test]
    async fn fused_legs_admit_empty_shards_and_apply_the_document_view_to_both_lists() {
        let mut decision = access("semantic", FieldAction::Use);
        decision
            .field_permissions
            .as_mut()
            .unwrap()
            .grants
            .push(FieldGrant {
                field: "body".into(),
                actions: vec![FieldAction::Use as i32],
            });
        for mode in [FusionMode::GlobalRank, FusionMode::TwoLevel] {
            for empty_incompatible in [false, true] {
                let nodes = vec![node(0, false), node_rows(100, empty_incompatible, 0, 0)];
                let reader = CoordinatorServiceImpl::with_local_nodes(nodes)
                    .with_bm25(
                        Some(crate::analyzer::NATIVE_ANALYSIS_BACKEND.into()),
                        Default::default(),
                    )
                    .for_access(Some(&decision), "bm25_search")
                    .unwrap()
                    .for_vector_field("semantic")
                    .unwrap();
                let result = reader
                    .fanout_hybrid(
                        "scoped-legs",
                        "word",
                        &[0.25; 16],
                        4,
                        Some(&crate::analyzer::body_spec()),
                        HybridLegs {
                            leg_k: 4,
                            vector_weight: 1.0,
                            bm25_weight: 1.0,
                            rrf_k: 60.0,
                            fusion_mode: mode,
                            normalization: fusion::Normalization::MinMax,
                            combination: fusion::Combination::Arithmetic,
                            min_vector_score: 0.0,
                        },
                        false,
                        &RequestFilters::default(),
                    )
                    .await;
                if empty_incompatible {
                    let error = result.unwrap_err();
                    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
                    assert!(error.message().contains("binding"), "{error}");
                } else {
                    let hits = result.unwrap().0;
                    assert_eq!(
                        hits.iter().map(|hit| hit.doc_id).collect::<Vec<_>>(),
                        vec![0]
                    );
                    assert!(hits[0].vector_rank.is_some());
                    assert!(hits[0].bm25_rank.is_some());
                }
            }
        }
    }

    #[tokio::test]
    async fn decomposed_lexical_admission_matches_a_physically_restricted_corpus() {
        let reference = CoordinatorServiceImpl::with_local_nodes(vec![
            node_rows(0, false, 1, 1),
            node_rows(100, false, 1, 1),
        ])
        .with_bm25(
            Some(crate::analyzer::NATIVE_ANALYSIS_BACKEND.into()),
            Default::default(),
        )
        .for_vector_field("semantic")
        .unwrap();
        let mut decision = access("semantic", FieldAction::Use);
        decision
            .field_permissions
            .as_mut()
            .unwrap()
            .grants
            .push(FieldGrant {
                field: "body".into(),
                actions: vec![FieldAction::Use as i32],
            });
        let scoped = CoordinatorServiceImpl::with_local_nodes(vec![
            node_rows_with_private_terms(0, false, 2, 2, 100),
            node_rows_with_private_terms(100, false, 2, 2, 100),
        ])
        .with_bm25(
            Some(crate::analyzer::NATIVE_ANALYSIS_BACKEND.into()),
            Default::default(),
        )
        .for_access(Some(&decision), "bm25_search")
        .unwrap()
        .for_vector_field("semantic")
        .unwrap();
        let legs = HybridLegs {
            leg_k: 1,
            vector_weight: 1.0,
            bm25_weight: 1.0,
            rrf_k: 60.0,
            fusion_mode: FusionMode::Decomposed,
            normalization: fusion::Normalization::MinMax,
            combination: fusion::Combination::Arithmetic,
            min_vector_score: 0.0,
        };
        let expected = reference
            .fanout_hybrid(
                "restricted",
                "word",
                &[0.25; 16],
                1,
                Some(&crate::analyzer::body_spec()),
                legs,
                false,
                &RequestFilters::default(),
            )
            .await
            .unwrap()
            .0;
        let actual = scoped
            .fanout_hybrid(
                "scoped",
                "word",
                &[0.25; 16],
                1,
                Some(&crate::analyzer::body_spec()),
                legs,
                false,
                &RequestFilters::default(),
            )
            .await
            .unwrap()
            .0;
        assert_eq!(expected[0].bm25_rank, Some(1));
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn lexical_candidate_round_enforces_views_fields_and_empty_read_versions() {
        let nodes = vec![node(0, false), node(100, false)];
        let owner = CoordinatorServiceImpl::with_local_nodes(nodes.clone());
        let reader = owner
            .for_access(Some(&access("body", FieldAction::Use)), "bm25_search")
            .unwrap();
        let terms = vec!["word".into()];
        let (global, claims) = reader.body_stats(&terms, false).await.unwrap();
        let (scores, debug) = reader
            .bm25_rescore_round(
                &terms,
                0,
                &global,
                &claims,
                &HashMap::from([(0, vec![0, 1, 0]), (1, vec![100, 101])]),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            scores
                .keys()
                .copied()
                .collect::<std::collections::BTreeSet<_>>(),
            [0, 100].into_iter().collect()
        );
        assert_eq!(debug.len(), 2);
        assert!(debug.values().all(|(_, hits)| *hits == 1));
        let (scores, debug) = reader
            .bm25_rescore_round(&terms, 0, &global, &claims, &HashMap::new(), &[])
            .await
            .unwrap();
        assert!(scores.is_empty());
        assert_eq!(
            debug.len(),
            2,
            "empty owners must acknowledge the authority view"
        );
        let denied = owner
            .for_access(Some(&access("body", FieldAction::Disclose)), "bm25_search")
            .unwrap();
        assert_eq!(
            denied
                .bm25_rescore_round(&terms, 0, &global, &claims, &HashMap::new(), &[])
                .await
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
        assert_eq!(
            reader
                .bm25_rescore_round(
                    &terms,
                    0,
                    &global,
                    &claims,
                    &HashMap::new(),
                    &[ScoreStage {
                        column: "audience".into(),
                        operation: Some(crate::pb::score_stage::Operation::Op(
                            ScoreOp::AddLinear as i32
                        )),
                        ..Default::default()
                    },]
                )
                .await
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
        nodes[0]
            .delete_documents(Request::new(DeleteDocumentsRequest {
                doc_ids: vec![0],
                ..Default::default()
            }))
            .await
            .unwrap();
        assert_eq!(
            reader
                .bm25_rescore_round(&terms, 0, &global, &claims, &HashMap::new(), &[])
                .await
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
    }

    #[tokio::test]
    async fn vector_candidate_reads_enforce_actual_fields_views_and_versions() {
        let nodes = vec![node(0, false), node(100, false)];
        let owner = CoordinatorServiceImpl::with_local_nodes(nodes.clone());
        let reader = owner
            .for_access(Some(&access("semantic", FieldAction::Use)), "bm25_search")
            .unwrap();
        let vector = vec![0.25; 16];
        let members = reader.vector_membership("semantic").await.unwrap();
        assert_eq!(members.ids, [0, 100].into_iter().collect());
        let scores = reader
            .dense_signal(&vector, &[0, 1, 100, 101], "semantic")
            .await
            .unwrap();
        assert_eq!(
            scores
                .keys()
                .copied()
                .collect::<std::collections::BTreeSet<_>>(),
            members.ids
        );
        let reference = owner
            .dense_signal(&vector, &[0, 100], "semantic")
            .await
            .unwrap();
        assert_eq!(scores, reference);
        let exact = reader
            .exact_vector_scores(&vector, &[0, 100], "semantic")
            .await
            .unwrap();
        assert_eq!(exact.scores.len(), 2);
        assert_eq!(exact.logical_bytes, 128);
        assert!(reader
            .dense_signal(&vector, &[], "semantic")
            .await
            .unwrap()
            .is_empty());
        assert!(reader
            .exact_vector_scores(&vector, &[], "semantic")
            .await
            .unwrap()
            .scores
            .is_empty());
        for decision in [
            access("body", FieldAction::Use),
            access("semantic", FieldAction::Disclose),
        ] {
            let denied = owner.for_access(Some(&decision), "bm25_search").unwrap();
            assert_eq!(
                denied
                    .vector_membership("semantic")
                    .await
                    .unwrap_err()
                    .code(),
                tonic::Code::PermissionDenied
            );
            assert_eq!(
                denied
                    .dense_signal(&[], &[], "semantic")
                    .await
                    .unwrap_err()
                    .code(),
                tonic::Code::PermissionDenied
            );
            assert_eq!(
                denied
                    .exact_vector_scores(&[], &[], "semantic")
                    .await
                    .err()
                    .unwrap()
                    .code(),
                tonic::Code::PermissionDenied
            );
        }
        let alias = owner
            .for_access(Some(&access("signal", FieldAction::Use)), "bm25_search")
            .unwrap();
        assert_eq!(
            alias
                .dense_signal(&vector, &[], "signal")
                .await
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
        assert_eq!(
            reader
                .dense_signal(&vector, &[], "")
                .await
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
        let (pinned, _) = reader.pin_read_versions().await.unwrap();
        nodes[0]
            .delete_documents(Request::new(DeleteDocumentsRequest {
                doc_ids: vec![0],
                ..Default::default()
            }))
            .await
            .unwrap();
        assert_eq!(
            pinned
                .dense_signal(&vector, &[], "semantic")
                .await
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
        assert_eq!(
            pinned
                .exact_vector_scores(&vector, &[], "semantic")
                .await
                .err()
                .unwrap()
                .code(),
            tonic::Code::FailedPrecondition
        );
        let decision = access("semantic", FieldAction::Use);
        for route in ["query", "query_stream"] {
            let admitted = owner.for_access(Some(&decision), route).unwrap();
            assert_eq!(admitted.document_visibility, decision.document_visibility);
            assert!(admitted
                .field_permissions
                .as_ref()
                .unwrap()
                .vector("signal")
                .is_err());
        }
    }

    #[tokio::test]
    async fn empty_vector_reads_refuse_mixed_generations_and_missing_binding_receipts() {
        let owner = CoordinatorServiceImpl::with_local_nodes(vec![node(0, false), node(100, true)]);
        let vector = vec![0.25; 16];
        assert_eq!(
            owner
                .vector_membership("semantic")
                .await
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
        assert_eq!(
            owner
                .dense_signal(&vector, &[], "semantic")
                .await
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
        assert_eq!(
            owner
                .exact_vector_scores(&vector, &[], "semantic")
                .await
                .err()
                .unwrap()
                .code(),
            tonic::Code::FailedPrecondition
        );
        let binding = crate::mapping::derive_plan(
            include_bytes!("../tests/fixtures/vector-binding/descriptor.bin"),
            "vector_binding.Named",
        )
        .unwrap()
        .vector_binding
        .unwrap();
        assert!(CoordinatorServiceImpl::check_vector_binding("semantic", None, &mut None).is_err());
        let mut bad = binding.clone();
        bad.field = "signal".into();
        assert!(
            CoordinatorServiceImpl::check_vector_binding("semantic", Some(&bad), &mut None)
                .is_err()
        );
        let response = VectorRescoreResponse {
            vector_binding: Some(binding),
            ..Default::default()
        };
        assert!(owner
            .check_read_view(
                0,
                &crate::visibility::VisibilityScope::default(),
                &response,
                &mut []
            )
            .is_err());
        let unbound = CoordinatorServiceImpl::with_local_nodes(vec![Arc::new(
            crate::node::NodeServiceImpl::new(None, Default::default()),
        )]);
        assert!(unbound
            .dense_signal(&vector, &[], "semantic")
            .await
            .is_err());
        assert!(unbound
            .exact_vector_scores(&vector, &[], "semantic")
            .await
            .is_err());
    }

    #[tokio::test]
    async fn every_scoped_scan_mode_uses_the_same_authorized_read_set() {
        let owner =
            CoordinatorServiceImpl::with_local_nodes(vec![node(0, false), node(100, false)]);
        let mut decision = access("semantic", FieldAction::Use);
        decision
            .field_permissions
            .as_mut()
            .unwrap()
            .grants
            .push(FieldGrant {
                field: "parent_id".into(),
                actions: vec![FieldAction::Use as i32, FieldAction::Disclose as i32],
            });
        let reader = owner
            .for_access(Some(&decision), "bm25_search")
            .unwrap()
            .for_vector_field("semantic")
            .unwrap();
        let vector = vec![0.25; 16];
        let filters = RequestFilters::compile(&[], "").unwrap();
        let classic = reader
            .fanout_search("scoped-classic", &vector, 4, false, &filters)
            .await
            .unwrap();
        let streaming = reader
            .fanout_stream_search("scoped-stream", &vector, 4, None, &filters)
            .await
            .unwrap();
        let collapsed = reader
            .fanout_search_collapse("scoped-collapse", &vector, 4, &filters)
            .await
            .unwrap();
        let stream_collapsed = reader
            .fanout_stream_search_collapse("scoped-stream-collapse", &vector, 4, &filters)
            .await
            .unwrap();
        let reference = owner
            .dense_signal(&vector, &[0, 100], "semantic")
            .await
            .unwrap();
        for hits in [
            &classic.hits,
            &streaming.hits,
            &collapsed.hits,
            &stream_collapsed.hits,
        ] {
            assert_eq!(hits.len(), 2);
            for hit in hits {
                assert_eq!(hit.score.to_bits(), reference[&hit.vector_id].to_bits());
            }
        }
        let forbidden = RequestFilters::compile(&[], "audience == 'private'").unwrap();
        assert_eq!(
            reader
                .fanout_search("denied-filter", &vector, 4, false, &forbidden)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
        let no_parent = owner
            .for_access(Some(&access("semantic", FieldAction::Use)), "bm25_search")
            .unwrap()
            .for_vector_field("semantic")
            .unwrap();
        assert_eq!(
            no_parent
                .fanout_search_collapse("denied-parent", &vector, 4, &filters)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
    }

    #[tokio::test]
    async fn incompatible_scan_receipts_refuse_before_provisional_disclosure() {
        let (progress, observed) = watch::channel(None);
        let reader =
            CoordinatorServiceImpl::with_local_nodes(vec![node(0, false), node(100, true)])
                .for_vector_field("semantic")
                .unwrap()
                .with_query_progress(progress);
        let filters = RequestFilters::compile(&[], "").unwrap();
        let error = reader
            .fanout_stream_search("mixed-bindings", &[0.25; 16], 4, None, &filters)
            .await
            .err()
            .unwrap();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(observed.borrow().is_none());
        let nodes = vec![node(0, false), node(100, false)];
        let reader = CoordinatorServiceImpl::with_local_nodes(nodes.clone())
            .for_vector_field("semantic")
            .unwrap();
        let (pinned, _) = reader.pin_read_versions().await.unwrap();
        nodes[0]
            .delete_documents(Request::new(DeleteDocumentsRequest {
                doc_ids: vec![0],
                ..Default::default()
            }))
            .await
            .unwrap();
        assert_eq!(
            pinned
                .fanout_search("stale-read", &[0.25; 16], 4, false, &filters)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
    }

    #[tokio::test]
    async fn pushed_boolean_dense_domains_preserve_optional_negative_and_vector_only_members() {
        use crate::pb::search_service_server::SearchService;
        let coordinator = CoordinatorServiceImpl::with_local_nodes(vec![
            node_rows(0, false, 2, 1),
            node_rows(100, false, 1, 2),
        ]);
        let docs = || SelectionQuery {
            node: Some(selection_query::Node::Filter(FilterQuery {
                id: "docs".into(),
                predicate: Some(filter_query::Predicate::Cel("audience != 'missing'".into())),
            })),
        };
        for exact in [false, true] {
            let dense = || SelectionQuery {
                node: Some(selection_query::Node::Search(SearchQuery {
                    id: "dense".into(),
                    query: Some(search_query::Query::Dense(DenseQuery {
                        field: "semantic".into(),
                        vector: vec![0.25; 16],
                        score_mode: if exact {
                            DenseScoreMode::Fp32Rerank as i32
                        } else {
                            DenseScoreMode::Native as i32
                        },
                        ..Default::default()
                    })),
                })),
            };
            for (must, should, must_not, want) in [
                (vec![dense()], vec![], vec![], vec![0, 100, 101]),
                (vec![docs()], vec![dense()], vec![], vec![0, 1, 100]),
                (vec![], vec![docs(), dense()], vec![], vec![0, 1, 100, 101]),
                (vec![docs()], vec![], vec![dense()], vec![1]),
                (vec![], vec![], vec![dense()], vec![1]),
                (vec![dense()], vec![], vec![docs()], vec![101]),
            ] {
                let response = coordinator
                    .query(Request::new(QueryRequest {
                        k: 10,
                        selection: Some(SelectionQuery {
                            node: Some(selection_query::Node::Boolean(BooleanQuery {
                                must,
                                should,
                                must_not,
                                ..Default::default()
                            })),
                        }),
                        ..Default::default()
                    }))
                    .await
                    .unwrap()
                    .into_inner();
                assert_eq!(
                    response
                        .hits
                        .iter()
                        .map(|hit| hit.doc_id)
                        .collect::<std::collections::BTreeSet<_>>(),
                    want.into_iter().collect(),
                    "exact={exact}"
                );
                for hit in response.hits.iter().filter(|hit| hit.doc_id == 1) {
                    assert!(!hit.matched.iter().any(|id| id == "dense"));
                    assert!(!hit.signals.iter().any(|signal| signal.id == "dense"));
                    assert_eq!(hit.score, 0.0);
                }
            }
        }
    }
}

#[cfg(test)]
mod query_stream_identity_tests {
    use super::*;
    use crate::pb::{
        DocumentIdentity, QueryStreamIdentityState as State, QueryStreamPhase as Phase,
    };

    #[test]
    fn fingerprint_is_reproducible_from_the_protobuf_revision() {
        use prost::Message;
        for score in [0.0, -0.0, f32::MIN_POSITIVE, f32::MAX] {
            let revision = query_stream_revision(
                1,
                Phase::Dense,
                vec![(9, score, None)],
                String::new(),
                State::Resolved,
            );
            let decoded =
                crate::pb::QueryStreamRevision::decode(revision.encode_to_vec().as_slice())
                    .unwrap();
            assert_eq!(
                decoded.content_fingerprint,
                query_stream_content_fingerprint(Phase::Dense, &decoded.hits, State::Resolved),
                "score bits {}",
                score.to_bits()
            );
        }
    }

    #[test]
    fn fingerprints_bind_disclosed_identity_presence_and_score_bits() {
        let identity = DocumentIdentity {
            document_key: vec![0, 255],
            version: u64::MAX,
            chunk_ordinal: Some(0),
        };
        let make = |identity, score, state| {
            query_stream_revision(
                1,
                Phase::Dense,
                vec![(9, score, identity)],
                String::new(),
                state,
            )
        };
        let base = make(Some(identity.clone()), 0.0, State::Resolved);
        let mut changed_key = identity.clone();
        changed_key.document_key.push(0);
        let mut changed_version = identity.clone();
        changed_version.version -= 1;
        let mut absent_ordinal = identity.clone();
        absent_ordinal.chunk_ordinal = None;
        for revision in [
            make(Some(changed_key.clone()), 0.0, State::Resolved),
            make(Some(changed_version), 0.0, State::Resolved),
            make(Some(absent_ordinal), 0.0, State::Resolved),
            make(None, 0.0, State::Resolved),
            make(Some(identity.clone()), f32::MIN_POSITIVE, State::Resolved),
        ] {
            assert_ne!(base.content_fingerprint, revision.content_fingerprint);
        }
        assert_eq!(
            base.content_fingerprint,
            make(Some(identity.clone()), -0.0, State::Resolved).content_fingerprint
        );
        let hidden = make(Some(identity.clone()), 0.0, State::Withheld);
        assert!(hidden.hits[0].identity.is_none());
        assert_eq!(
            hidden.content_fingerprint,
            make(Some(changed_key), 0.0, State::Withheld).content_fingerprint
        );
        assert_ne!(
            hidden.content_fingerprint,
            make(None, 0.0, State::Resolved).content_fingerprint
        );
        assert_ne!(
            hidden.content_fingerprint,
            make(None, 0.0, State::Unspecified).content_fingerprint
        );
        assert_eq!(
            base.content_fingerprint,
            query_stream_revision(
                99,
                Phase::Dense,
                vec![(9, 0.0, Some(identity))],
                "later-score-certificate".into(),
                State::Resolved
            )
            .content_fingerprint
        );
    }
}

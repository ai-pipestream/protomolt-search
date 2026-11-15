//! Coordinator service and public query interfaces.

mod access;
mod aggregate_fanout;
mod aggregate_plan;
mod boolean;
mod boost;
mod browse;
mod builders;
#[cfg(test)]
mod candidate_fetch_tests;
mod cascade;
mod clustered;
mod dense_execution;
mod filters;
mod hybrid;
mod hybrid_context;
mod hybrid_decomposed;
mod hybrid_reduce;
#[cfg(test)]
mod hybrid_tests;
mod ingest;
mod lexical;
mod lexical_context;
mod lexical_fused;
mod lexical_fused_plan;
mod lexical_reduce;
mod lexical_round;
#[cfg(test)]
mod lexical_round_tests;
mod lexical_stats;
mod lexical_stream;
mod lineage;
#[cfg(test)]
mod lineage_read_tests;
mod membership;
mod phrase;
mod placement;
mod query_progress;
#[cfg(test)]
mod query_stream_identity_tests;
mod rerank_candidates;
mod rescore;
mod results;
mod rpc;
mod rpc_hybrid;
mod rpc_lexical;
mod rpc_query;
mod rpc_query_stream;
#[cfg(test)]
mod scoped_fold_tests;
mod signals;
#[cfg(test)]
mod stream_cancel_tests;
mod stream_collapse;
mod stream_run;
mod stream_search;
mod stream_transport;
mod suggest;
mod topology;
mod turbo;
#[cfg(test)]
mod unsigned_aggregate_tests;
mod values;
mod variants;
mod vector_collapse;
mod vector_fanout;
#[cfg(test)]
mod vector_field_read_tests;
mod vector_rows;

pub(crate) use vector_rows::ShardVectorRowProbe;

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
use crate::pb::{
    bm25_query_stream_request, bm25_query_stream_response, Bm25QueryResponse,
    Bm25QueryStreamRequest, Bm25QueryStreamResponse,
};
use crate::pb::{
    search_shard_request, search_shard_response, AbortTopologyCutoverRequest,
    AbortTopologyCutoverResponse, Bm25Hit, Bm25QueryRequest, Bm25RescoreRequest, Bm25SearchRequest,
    Bm25SearchResponse, BroadcastVectorBackendRequest, BroadcastVectorBackendResponse, CascadeHit,
    ClusterHealthRequest, ClusterHealthResponse, ClusteredVectorHealth,
    ConfigureVectorBackendRequest, ExactVectorRescoreRequest, FloorUpdate,
    FreezeTopologyWritesRequest, FreezeTopologyWritesResponse, FusionMode, HealthRequest,
    HybridDebug, HybridHit, HybridSearchRequest, HybridSearchResponse, HybridShardDebug,
    HybridShardRequest, ParentGroup, PublishTopologyRequest, PublishTopologyResponse,
    RoutedIngestMappedRequest, RoutedIngestMappedResponse, RoutedShardIngest, ScoredHit,
    SearchRequest, SearchResponse, SearchShardDone, SearchShardRequest, SearchShardResponse,
    ShardHealth, ShardLegsRequest, ShardScanStats, StartShardSearch, StartStreamSearch,
    StopStreamSearch, StreamSearchRequest, StreamSearchResponse, StreamSearchSummary,
    TermStatsRequest, VectorBackendApplyResult, VectorRescoreRequest,
};
use crate::pb::{
    search_variant, InterleaveTeam, Interleaving, RankedHit, RankingDiff, VariantResult,
    VariantSearchRequest, VariantSearchResponse,
};
use crate::pb::{stream_search_request, stream_search_response};
use crate::rankdiff;

pub(crate) use aggregate_plan::compile_aggregations;
pub(crate) use aggregate_plan::compile_projections;
use aggregate_plan::merge_cardinality;
use aggregate_plan::merge_column_stats;
pub(crate) use aggregate_plan::merge_facet_counts;
use aggregate_plan::merge_range_counts;
use aggregate_plan::nearest_percentile_rank;
use aggregate_plan::refuse_unknown_filter_leaves;
use aggregate_plan::refuse_unknown_geo_columns;
pub(crate) use aggregate_plan::AggMerge;
use aggregate_plan::AggregatedHits;
pub(crate) use aggregate_plan::CompiledAggregate;
use aggregate_plan::FacetedHits;
pub use aggregate_plan::FetchedValues;
pub(crate) use aggregate_plan::PctMerge;
pub use browse::BrowseAfter;
pub use browse::BrowseRows;
use dense_execution::provider_mismatch_of;
pub use dense_execution::DenseRequestKey;
pub(crate) use dense_execution::ExactRerankScores;
pub use dense_execution::FleetVectorIdentity;
use filters::mark_known;
pub(crate) use filters::BooleanFanout;
pub(crate) use filters::BooleanFanoutPlan;
use filters::FilterKnown;
pub use filters::MembershipSet;
pub(crate) use filters::PercentileScope;
pub use filters::RequestFilters;
pub(crate) use hybrid_context::{CascadeQuery, HybridQuery};
use lexical_context::LexicalExpansions;
pub(crate) use lexical_context::{FlatLexicalQuery, FusedLexicalQuery};
use lexical_context::{FlatRound, FusedRound, RoundTiming};
use lexical_stats::analyze_query;
use lexical_stats::FusedGlobals;
use lexical_stream::floor_token;
pub(crate) use lexical_stream::is_stale_stats;
use lexical_stream::stream_bm25_shard;
use lexical_stream::Bm25StreamHeap;
use placement::PlacementRouter;
use placement::PlanCounter;
#[cfg(test)]
use query_progress::query_stream_content_fingerprint;
use query_progress::query_stream_revision;
use query_progress::QueryProgress;
use query_progress::QueryProgressPublisher;
use results::count_shards_finished_early;
use results::count_shards_floor_pruned;
use results::finish_quorum;
use results::leg_payloads;
use results::summed_filter_regimes;
use results::summed_patience_stopped;
use results::summed_tail_enumerated_rows;
pub use results::CollapseStreamResult;
pub use results::FanoutResult;
pub use results::HybridLegs;
pub use results::StreamFanoutResult;
use results::StreamHeapEntry;
pub(crate) use stream_transport::StreamFanout;
pub(crate) use stream_transport::StreamOpen;
pub use suggest::DEFAULT_PREFIX_EXPANSIONS;
pub use suggest::DEFAULT_SUGGEST_LIMIT;
pub use suggest::DEFAULT_SUGGEST_SCAN;
pub use suggest::DEFAULT_TERM_SUGGEST_LIMIT;
pub use suggest::MAX_PREFIX_EXPANSIONS;
pub use suggest::MAX_SUGGEST_LIMIT;
pub use suggest::MAX_SUGGEST_SCAN;
pub use suggest::MAX_TERM_SUGGEST_EDITS;
#[cfg(test)]
use topology::build_topology;
pub use topology::stable_routing_hash;
use topology::CoordinatorTopology;
pub use topology::TopologyRoute;
use variants::diff_against;
use variants::variant_text;
use vector_fanout::run_shard_with_hedge;
use vector_fanout::ShardQueryCtx;

/// The distinct-value cap a CARDINALITY aggregation gets when the
/// request names none (docs/aggregations.md "Cardinality").
pub(crate) const DEFAULT_MAX_DISTINCT: u32 = 100_000;

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
    /// Optional coordinator-wide default stillness window before sending
    /// `FinishStreamSearch` to unfinished shards once top-`k` is full and a
    /// shard quorum has completed (`docs/hnsw.md`, "Graceful stream close").
    stream_finish_settle: Option<Duration>,
    /// Optional coordinator-wide default deadline before sending
    /// `FinishStreamSearch { deadline: true }` to unfinished shards.
    stream_finish_deadline: Option<Duration>,
    /// Run `Bm25Search` over the exact `Bm25QueryStream` candidate
    /// protocol instead of the unary per-shard top-k round. The
    /// coordinator owns the only authoritative global heap, relays its
    /// emission-safe floor, and accepts a result only after every shard
    /// supplies a matching score-space fingerprint and completion
    /// certificate (docs/block-max.md).
    bm25_stream: bool,
    /// Runtime knobs (`max_k`, hedge delay, shard pruning, signal batch) read
    /// at request time and shared across clones of this coordinator.
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
    /// Cached vector row count per shard address from `GetVectorBackend`,
    /// used to compute each shard's eligible-row share `p` (`floor_share_ppm`)
    /// for `#72`'s ascent gate.
    shard_vector_rows: Arc<Mutex<HashMap<String, u64>>>,
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
    /// The index's derived-column declaration (`docs/derived-columns.md`),
    /// from the shard map's `[[derived]]` table: evaluated over each
    /// routed document so a placement predicate on a derived column
    /// routes the way the shards store it, and consulted by field
    /// permissions so a derived column discloses no more than its
    /// inputs. Static for the process, like a node's; a reloaded map
    /// that declares otherwise is refused.
    derived: Option<Arc<crate::derived::Declaration>>,
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
    /// Optional memory-mapped stem-to-surface FST lexicon for stemmed `Suggest`.
    surface_lexicon: Option<Arc<crate::turbo_bridge::query_plan::SharedSurfaceLexicon>>,
    /// Optional contextual bigram spellchecker for `TermSuggest`.
    spellchecker: Option<Arc<turbo_client::QuerySpellchecker>>,
    /// Optional one-pass query analyzer + typed NLP column mapping.
    one_pass_query: Option<
        Arc<(
            turbo_client::OnePassQueryAnalyzer,
            crate::turbo_bridge::NlpColumnSpec,
        )>,
    >,
    /// Optional Stage-2 calibrated `RerankAndHighlight` policy.
    rerank_spec: Option<Arc<crate::turbo_bridge::rerank::RerankSpec>>,
}

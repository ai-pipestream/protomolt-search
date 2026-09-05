//! Cluster configuration: TOML file + environment variables + CLI flags.
//!
//! Precedence (highest wins): CLI flag, then environment variable, then
//! config file, then built-in default. `--config <path>` (or
//! `PIPESTREAM_SEARCH_CONFIG`) selects the file; every other flag is
//! `--key=value`. Legacy `TURBOVEC_*` environment names remain fallbacks.
//!
//! A `--nodes` membership list is static. A generation-stamped `--shard-map`
//! is different: the coordinator polls and atomically publishes newer complete
//! maps, while each request remains pinned to one immutable generation.
//!
//! Example (`cluster.toml`):
//!
//! ```toml
//! role = "both"                        # node | coordinator | both
//! coord_listen = "0.0.0.0:50050"
//! metrics_listen = "127.0.0.1:9100"    # optional Prometheus page (docs/metrics.md)
//! nodes = ["host-a:50051", "host-b:50051"]  # fan-out order = tie-break order
//! chunk_blocks = 64
//! floor_sharing = true
//! max_message_mib = 64
//!
//! [[shards]]                           # shards this process owns/serves
//! listen = "0.0.0.0:50051"
//! index = "/data/search/shard-0.vector"
//! slot_offset = 0
//! ```

use std::net::SocketAddr;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::chunked::DEFAULT_CHUNK_BLOCKS;
use crate::MAX_MESSAGE_BYTES;

/// Which role(s) this process serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Shard owner: serves `NodeService` only.
    Node,
    /// Query fan-out: serves `SearchService` only.
    Coordinator,
    /// One process serving both (single-machine demos/tests).
    Both,
}

/// Demo index shape for `--demo-vectors` (random unit vectors, calibration
/// fitted on a sample and seeded — the same flow real deployments use).
#[derive(Debug, Clone, Copy)]
pub struct DemoConfig {
    /// Number of random vectors to generate.
    pub vectors: usize,
    /// Vector dimensionality.
    pub dim: usize,
    /// Quantization bit width (2, 3, or 4).
    pub bit_width: usize,
}

/// One shard this process serves (one `NodeService` listener per shard).
#[derive(Debug, Clone)]
pub struct ShardConfig {
    /// Listen address for this shard's `NodeService`.
    pub listen: SocketAddr,
    /// Vector provider used to load or construct this shard.
    pub vector_backend: String,
    /// Path to a provider-owned vector image. Mutually exclusive with `demo`.
    pub index_path: Option<PathBuf>,
    /// Build a random demo index instead of loading one.
    pub demo: Option<DemoConfig>,
    /// This shard's global id base (added to local slots).
    pub slot_offset: u64,
    /// Lexical analysis backend for AddDocuments: `native` or a sidecar address.
    pub analysis_addr: Option<String>,
    /// Keep a write-ahead log at `<index path>.wal/`. Defaults on for
    /// shards with an index path, off for demo shards.
    pub wal: bool,
    /// Number of WAL hash buckets (a power of two, max 1024). Fixed at
    /// WAL creation; a resumed log keeps its own.
    pub wal_buckets: u32,
    /// Accumulate vocabulary statistics inline in this shard's ingest,
    /// snapshotting to `<index path>.vocab/` (see `docs/VOCABULARY-INDEX.md`).
    /// Defaults OFF — zero overhead when off. Requires an index path.
    pub vocab: bool,
    /// The collection this shard serves (`docs/collections.md`); empty
    /// for a shard outside any named collection.
    pub collection: String,
    /// The id this shard is reported under to the control plane
    /// (`docs/cluster-control.md`); `slot-<offset>` when unset.
    pub shard_id: Option<String>,
    /// The inclusive stable-key hash range the shard covers, when the
    /// configuration names it; else the plane's records or the published
    /// topology supply it.
    pub hash_range: Option<(u64, u64)>,
}

/// Node membership in the control plane (`--node-id` and friends,
/// docs/cluster-control.md "Node lifecycle").
#[derive(Debug, Clone)]
pub struct NodeMembershipConfig {
    pub node_id: String,
    /// The coordinator's ClusterControl endpoint.
    pub control_addr: String,
    pub failure_domain: String,
    /// Where placed replicas live (`<data-dir>/<shard_id>/`).
    pub data_dir: PathBuf,
    /// `host:port` other nodes and the coordinator reach this node at;
    /// the host is what every listener is advertised under. Defaults to
    /// the first shard listener when that binds a concrete address.
    pub advertise_addr: Option<String>,
    /// Where placed replicas listen: an interface and a first port
    /// (port 0 lets the OS choose). Defaults to the first shard
    /// listener's interface with port 0.
    pub replica_listen: Option<SocketAddr>,
    pub report_ms: u64,
    pub reconcile_ms: u64,
    /// Requested lease; 0 takes the plane's policy.
    pub lease_ms: u64,
    pub lag_bound: u64,
}

/// One shard entry of a coordinator shard map (`--shard-map`): which node
/// owns which id range, plus the hash range it covers when the map was
/// produced by a hash-partitioned split (see `examples/reshard.rs`).
/// `hash_lo`/`hash_hi` are inclusive and default to the full range.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ShardMapShard {
    /// Node address (`host:port` or `http://host:port`).
    pub addr: String,
    /// Optional replica serving the same data — the target for the
    /// coordinator's hedged retries and failover. Exact search over
    /// identical data returns identical results from either copy.
    pub replica: Option<String>,
    /// The shard's global id base.
    #[serde(default)]
    pub slot_offset: u64,
    /// Inclusive hash-range bounds (`fnv1a64(vector_id)` space).
    pub hash_lo: Option<u64>,
    pub hash_hi: Option<u64>,
    /// The placement code this shard serves (`docs/placement.md`), when
    /// the map has a `[placement]` tree. Required on every shard then.
    pub placement: Option<u64>,
}

/// The id-to-shard authority for one cluster generation
/// (`--shard-map=<file>`). Replaces `--nodes`; bumping `generation` is how
/// a split/merge rollout distinguishes old topology from new.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ShardMap {
    /// Topology generation (0 for the implicit `--nodes` topology).
    #[serde(default)]
    pub generation: u64,
    /// One entry per shard, in fan-out order (= merge tie-break order).
    pub shards: Vec<ShardMapShard>,
    /// The placement tree (`docs/placement.md`): which leaf each shard
    /// serves and the predicates that route a document there.
    #[serde(default)]
    pub placement: Option<crate::placement::PlacementTreeConfig>,
}

/// Read one complete shard-map candidate. Callers validate its routing
/// geometry before publishing it; a parse failure never changes live state.
pub fn load_shard_map(path: &std::path::Path) -> Result<ShardMap, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("read shard map {}: {e}", path.display()))?;
    toml::from_str(&text).map_err(|e| format!("parse shard map {}: {e}", path.display()))
}

/// Normalize one node address the same way at startup and during hot reload.
pub fn normalize_addr(addr: String) -> String {
    normalize_addrs(vec![addr]).remove(0)
}

/// Product-level transport to one distributed TurboVec collection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusteredTurboVecConfig {
    /// The global heap and topology owner execute in this process; only shard
    /// node calls cross the network.
    InProcess {
        /// `turbovec-grpc` node-table entries, including optional index ids,
        /// replicas, and required durable generations.
        nodes: Vec<String>,
        /// Durable coordinator topology. Required unless the operator opts
        /// into an ephemeral development collection.
        state: Option<PathBuf>,
        allow_ephemeral: bool,
    },
    /// Reach a separately managed `turbovec-coordinator` process.
    External { endpoint: String },
}

/// Full process configuration.
#[derive(Debug, Clone)]
pub struct Config {
    /// Role(s) to serve.
    pub role: Role,
    /// Listen address for `SearchService` (roles coordinator/both).
    pub coord_listen: SocketAddr,
    /// Listen address for the Prometheus metrics page
    /// (`docs/metrics.md`). `None` (the default) serves no metrics.
    /// There is no auth on the page; bind a trusted interface.
    pub metrics_listen: Option<SocketAddr>,
    /// Shard node addresses (`http://host:port`) for the coordinator, in
    /// fan-out order (= shard index for merge tie-breaks).
    pub node_addrs: Vec<String>,
    /// Optional product-level distributed TurboVec collection. When set,
    /// vector queries use this backend instead of the vector indexes held by
    /// the product shard nodes.
    pub clustered_turbovec: Option<ClusteredTurboVecConfig>,
    /// Shards this process owns and serves (roles node/both).
    pub shards: Vec<ShardConfig>,
    /// Scan chunk size in SIMD blocks.
    pub chunk_blocks: usize,
    /// Participate in floor sharing (publish + adopt floors).
    pub share_floors: bool,
    /// Use block-max pruning on BM25 queries when shards support it
    /// (v5 files). `false` forces the exhaustive scorer — the A/B
    /// baseline; results are identical either way.
    pub block_max: bool,
    /// Skip sealed segments a request's filter cannot match, from
    /// their column summaries (docs/segment-pruning.md). `false` keeps
    /// every segment in the scan; the answer is the same either way.
    pub segment_pruning: bool,
    /// Coordinator: skip shards a request's filter cannot match, from
    /// the placement leaf each shard serves (docs/placement.md).
    /// `false` consults every shard; the answer is the same either way.
    pub shard_pruning: bool,
    /// Node: the i64 column each row carries with its placement code
    /// (`--placement-column`, docs/placement.md). Joins the integer
    /// table.
    pub placement_column: Option<String>,
    /// Node: the placement code this shard serves
    /// (`--placement-leaf`). Fills the placement column on a direct
    /// ingest that lacks it and refuses another code.
    pub placement_leaf: Option<i64>,
    /// Serve a shard whose BM25 bulk build was interrupted: a
    /// `.bm25.build` spill directory with no `.bm25` beside it.
    ///
    /// Off by default. `Flush` removes the spill directory on success,
    /// so that pair cannot occur on a shard that finished, and serving it
    /// is the bad kind of quiet: the node comes up healthy, answers
    /// vector queries normally, and contributes NOTHING to every lexical
    /// query, so a fleet ranks against a corpus short one shard's share
    /// with nothing anywhere saying so.
    ///
    /// A shard with neither file is NOT affected — that is what a
    /// vector-only deployment looks like, and it is a real one.
    pub allow_missing_bm25: bool,
    /// Coalesce concurrent vector scans into batched kernel calls (up to
    /// four queries per pass over the packed codes). `false` runs one
    /// scan per RPC — the A/B baseline; results are identical either way.
    pub coalesce: bool,
    /// Concurrent batched scans per node (0 = half the cores).
    pub scan_parallel: usize,
    /// Bounded worker lanes per shard for page-local FP32 reranking. Zero
    /// auto-sizes and caps itself at four.
    pub rerank_parallel: usize,
    /// Minimum score improvement before a node publishes its next floor
    /// (0.0 = publish every raise).
    pub floor_delta: f32,
    /// Publish opportunities a node skips before its first floor goes
    /// out (0 = publish from the first chunk, the historical behavior).
    pub floor_warmup_chunks: u32,
    /// Minimum milliseconds between two published floors (0 = no
    /// debounce).
    pub floor_min_interval_ms: u64,
    /// Coordinator: per-shard wall-clock deadline in milliseconds for one
    /// query's shard attempt (0 = no deadline).
    pub shard_deadline_ms: u64,
    /// Coordinator: delay before hedging a slow shard to its replica
    /// (0 = no hedging; replicas then serve as failover only).
    pub hedge_delay_ms: u64,
    /// Coordinator: optional replica address per shard, aligned with
    /// `node_addrs` (from the shard map's `replica` fields).
    pub replica_addrs: Vec<Option<String>>,
    /// Coordinator: serve plain vector Search over the streaming
    /// protocol (shards emit above the relayed floor; the coordinator
    /// holds the only top-k). Identical results, different pruning
    /// locus. Off by default.
    pub stream_search: bool,
    /// Coordinator: serve as a RELAY (`docs/relay-coordinators.md`): the
    /// node-facing surface over this coordinator's shard set, presented
    /// to a parent coordinator as one shard. StreamSearch, TermStats,
    /// and Health only; every other node route refuses by name. Needs
    /// the coordinator role with one unnamed collection. Off by default.
    pub relay: bool,
    /// Coordinator: run BM25 fan-out over the exact candidate stream.
    /// The coordinator owns the global heap and inclusive floor; every
    /// shard must return a matching scoring fingerprint and successful
    /// completion certificate. Enabled by default; false retains the
    /// unary path as the correctness/performance baseline.
    pub bm25_stream: bool,
    /// Coordinator: hard cap on any client-facing `k`. Requests above it
    /// are refused (never clamped); a request omitting `k` runs at this
    /// depth. Must be at least 1.
    pub max_k: u32,
    /// Coordinator-wide logical FP32 payload bound for one rerank request.
    pub max_rerank_bytes: u64,
    /// Optional generation-bound measured candidate-depth profile.
    pub dense_quality_profile: Option<PathBuf>,
    /// Optional synonym table (`docs/synonyms.md`), a TOML file.
    pub synonyms: Option<PathBuf>,
    /// Optional generation-bound dense execution policy for AUTO
    /// (`docs/dense-execution-policy.md`).
    pub dense_execution_policy: Option<PathBuf>,
    /// gRPC message size cap applied to clients and servers.
    pub max_message_bytes: usize,
    /// Issue one demo search against the coordinator at startup.
    pub demo_query: bool,
    /// Dimension of the demo-query vector.
    pub query_dim: usize,
    /// Bit width for from-scratch index construction via AddVectors.
    pub bit_width: usize,
    /// Flush shards to their index paths on graceful shutdown.
    pub save_on_shutdown: bool,
    /// Lexical analysis backend for coordinator queries: `native` or a
    /// sidecar address. Required for BM25 queries.
    pub analysis_addr: Option<String>,
    /// BM25 k1 parameter sent to every shard.
    pub bm25_k1: f32,
    /// BM25 b parameter sent to every shard.
    pub bm25_b: f32,
    /// The BM25 field table for NEW shard builders
    /// (`docs/multi-field.md`): "body" first, then the extra indexed
    /// fields. Existing `.bm25` files keep the table they were written
    /// with. Documents naming fields outside the table are refused.
    pub bm25_fields: Vec<String>,
    /// The facet field table for NEW shard builders
    /// (`--facet-fields=court,year`): dictionary-encoded per-doc
    /// columns counted by facet queries. Same rules as `bm25_fields`;
    /// non-empty makes new builders persist as v7.
    pub facet_fields: Vec<String>,
    /// The numeric field table for NEW shard builders
    /// (`--numeric-fields=decision_date`): f64 columns read by
    /// score-function chains (docs/score-functions.md). Same rules as
    /// `facet_fields`; names must not collide with them (the v7 column
    /// table holds both and refuses duplicates).
    pub numeric_fields: Vec<String>,
    /// The map<string, string> column table for NEW shard builders
    /// (`--map-facet-fields=meta`, docs/map-columns.md). Same rules;
    /// one name space across all column kinds.
    pub map_facet_fields: Vec<String>,
    /// Optional `concept-id<TAB>surface-form` glossary. When present, the
    /// phrase field and optional entity map are derived at ingest and query.
    pub phrase_glossary: Option<PathBuf>,
    /// Dedicated BM25 field holding canonical glossary concept postings.
    pub phrase_field: String,
    /// Optional map<string,string> column holding glossary and NER identities.
    pub entity_map_field: Option<String>,
    /// Whether glossary matching uses Unicode full case folding.
    pub phrase_ignore_case: bool,
    /// Request OpenNLP NER and materialize mentions into the entity map.
    pub phrase_ner: bool,
    /// BM25 fields that keep token positions per occurrence
    /// (`--position-fields=body`, docs/phrase-proximity.md): the opt-in
    /// payload behind exact phrase and proximity queries. Each name must
    /// be in `bm25_fields`. Shards loaded from existing files keep the
    /// declaration they were written with; ingest refuses a positional
    /// field the file never declared.
    pub position_fields: Vec<String>,
    /// Source fields whose adjacent-token pairs are derived into a
    /// bigram column (`--bigram-fields=body`, docs/phrase-proximity.md).
    /// The column is the BM25 field named `<source>.bigrams`, which must
    /// be declared in `bm25_fields`; clients never supply it.
    pub bigram_fields: Vec<String>,
    /// Fields whose sentence spans are stored per document for
    /// server-side snippets (`--sentence-fields=body`,
    /// docs/highlighting.md). Snippets are cut from stored text, and
    /// only the body's text is stored, so the only legal entry is the
    /// body. Shards loaded from existing files keep the declaration they
    /// were written with; ingest refuses a sentence field the file never
    /// declared.
    pub sentence_fields: Vec<String>,
    /// The map<string, f64> column table for NEW shard builders
    /// (`--map-numeric-fields=attrs`). Same rules.
    pub map_numeric_fields: Vec<String>,
    /// The i64 column table for NEW shard builders
    /// (`--integer-fields=citations,filed_at`, docs/range-facets.md):
    /// exact integers past 2^53, and where Timestamp ingest lands as
    /// epoch micros. Same rules; one name space across all kinds.
    pub integer_fields: Vec<String>,
    /// The geo-point column table for NEW shard builders
    /// (`--geo-fields=courthouse`, docs/geo-columns.md): the columns
    /// bbox/radius filters and distance-decay stages read. Same rules;
    /// one name space across all kinds.
    pub geo_fields: Vec<String>,
    /// The shard map the coordinator's `node_addrs` came from, when
    /// `--shard-map` was given (`None` for the implicit `--nodes`
    /// topology, generation 0).
    pub shard_map: Option<ShardMap>,
    /// Source path retained for atomic hot reloads.
    pub shard_map_path: Option<PathBuf>,
    /// Coordinator polling interval for a newer complete shard map. Zero
    /// disables reload while preserving the startup map.
    pub shard_map_reload_ms: u64,
    /// Poll interval for automatic primary-WAL to replica catch-up. Zero
    /// disables synchronization.
    pub replica_sync_ms: u64,
    /// Durable per-pair WAL cursors. Required whenever replica sync is on.
    pub replica_state_path: Option<PathBuf>,
    /// Durable autonomous membership and placement authority. When set, the
    /// coordinator also serves ClusterControl and publishes its topology.
    pub control_state_path: Option<PathBuf>,
    /// Autonomous lease/placement reconciliation interval. Zero disables the
    /// timer while retaining the explicit ReconcileCluster RPC.
    pub control_reconcile_ms: u64,
    pub control_lease_ms: u64,
    pub control_replication_factor: usize,
    pub control_split_rows: u64,
    pub control_merge_rows: u64,
    pub control_compact_segments: u32,
    pub control_compact_tombstone_ppm: u32,
    /// Node membership in the control plane (`--node-id`,
    /// `--control-addr`, `--failure-domain`, `--data-dir`); `None` for a
    /// node that does not register.
    pub membership: Option<NodeMembershipConfig>,
    /// Named collections this coordinator serves (`docs/collections.md`);
    /// empty means the one unnamed dataset described by `node_addrs` and
    /// the top-level knobs. When present, `node_addrs` is empty and every
    /// per-dataset setting lives on the collection.
    pub collections: Vec<CollectionConfig>,
    /// The collection an unnamed request gets to, when configured; with
    /// named collections and no default, unnamed requests refuse.
    pub default_collection: Option<String>,
    /// Listener TLS (`docs/security.md`): `--tls-cert`, `--tls-key`, and
    /// the cluster CA (`--tls-client-ca`) client certificates chain to.
    /// Once set, every listener speaks TLS and plaintext is refused.
    pub tls: Option<crate::security::ServerTls>,
    /// What this process presents and trusts on cluster-internal
    /// channels (`--tls-ca`, `--tls-client-cert`, `--tls-client-key`,
    /// `--tls-domain`).
    pub client_tls: Option<crate::security::ClientTls>,
    /// Serve plaintext gRPC on a non-loopback listener without TLS
    /// (`--allow-plaintext`). Loopback listeners never need it.
    pub allow_plaintext: bool,
    /// Bearer principals for the public search surface
    /// (`--bearer-tokens=<toml>`); unset serves anonymous callers.
    pub principals: Option<std::sync::Arc<crate::security::Principals>>,
    /// The key that authenticates UDP floor and cancel datagrams
    /// (`--udp-hmac-key=<file>`).
    pub udp_hmac_key: Option<crate::security::UdpKey>,
    /// The layout a NEW persisted shard gets (`--layout=segments|single-image`,
    /// docs/immutable-segments.md); an existing shard keeps its own.
    pub layout: crate::node::Layout,
    /// Documents a segmented shard's tail may hold before it seals a
    /// segment on its own (`--seal-tail-docs`); 0 seals on flush only.
    pub seal_tail_docs: u32,
    /// Serve sealed segments' vector images through memory maps
    /// (`--vector-mmap=true|false`, docs/mmap-vectors.md); on by default.
    pub vector_mmap: bool,
    /// Documents per vocabulary window before automatic rollover (only
    /// relevant to shards with `vocab` enabled).
    pub vocab_window_docs: u64,
    /// Heavy-hitter list size per vocabulary channel.
    pub vocab_top_k: usize,
}

/// Raw TOML file shape; every field optional (file < env < CLI).
/// One collection's dataset settings (`docs/collections.md`): everything
/// a coordinator needs that differs per dataset. Knobs not listed here
/// (limits, streaming, message sizes, the phrase index) are shared.
#[derive(Debug, Clone)]
pub struct CollectionConfig {
    pub name: String,
    pub node_addrs: Vec<String>,
    pub replica_addrs: Vec<Option<String>>,
    pub shard_map: Option<ShardMap>,
    pub shard_map_path: Option<PathBuf>,
    pub analysis_addr: Option<String>,
    pub bm25_k1: f32,
    pub bm25_b: f32,
    pub dense_quality_profile: Option<PathBuf>,
    pub synonyms: Option<PathBuf>,
    pub dense_execution_policy: Option<PathBuf>,
    pub replica_state_path: Option<PathBuf>,
    pub control_state_path: Option<PathBuf>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct FileCollection {
    name: String,
    nodes: Option<Vec<String>>,
    shard_map: Option<String>,
    analysis_addr: Option<String>,
    bm25_k1: Option<f32>,
    bm25_b: Option<f32>,
    dense_quality_profile: Option<String>,
    synonyms: Option<String>,
    dense_execution_policy: Option<String>,
    replica_state: Option<String>,
    control_state: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct FileConfig {
    collections: Vec<FileCollection>,
    default_collection: Option<String>,
    tls_cert: Option<String>,
    tls_key: Option<String>,
    tls_client_ca: Option<String>,
    tls_ca: Option<String>,
    tls_client_cert: Option<String>,
    tls_client_key: Option<String>,
    tls_domain: Option<String>,
    allow_plaintext: Option<bool>,
    bearer_tokens: Option<String>,
    udp_hmac_key: Option<String>,
    layout: Option<String>,
    seal_tail_docs: Option<u32>,
    vector_mmap: Option<bool>,
    role: Option<String>,
    node_listen: Option<String>,
    coord_listen: Option<String>,
    metrics_listen: Option<String>,
    nodes: Option<Vec<String>>,
    clustered_turbovec: Option<FileClusteredTurboVec>,
    index: Option<String>,
    slot_offset: Option<u64>,
    demo_vectors: Option<usize>,
    dim: Option<usize>,
    bit_width: Option<usize>,
    vector_backend: Option<String>,
    chunk_blocks: Option<usize>,
    floor_sharing: Option<bool>,
    block_max: Option<bool>,
    segment_pruning: Option<bool>,
    shard_pruning: Option<bool>,
    placement_column: Option<String>,
    placement_leaf: Option<i64>,
    allow_missing_bm25: Option<bool>,
    coalesce: Option<bool>,
    scan_parallel: Option<usize>,
    rerank_parallel: Option<usize>,
    floor_delta: Option<f32>,
    floor_warmup_chunks: Option<u32>,
    floor_min_interval_ms: Option<u64>,
    shard_deadline_ms: Option<u64>,
    hedge_delay_ms: Option<u64>,
    max_message_mib: Option<usize>,
    demo_query: Option<bool>,
    stream_search: Option<bool>,
    relay: Option<bool>,
    bm25_stream: Option<bool>,
    max_k: Option<u32>,
    max_rerank_mib: Option<u64>,
    dense_quality_profile: Option<String>,
    synonyms: Option<String>,
    dense_execution_policy: Option<String>,
    query_dim: Option<usize>,
    save_on_shutdown: Option<bool>,
    analysis_addr: Option<String>,
    bm25_k1: Option<f32>,
    bm25_b: Option<f32>,
    bm25_fields: Option<Vec<String>>,
    facet_fields: Option<Vec<String>>,
    numeric_fields: Option<Vec<String>>,
    map_facet_fields: Option<Vec<String>>,
    phrase_glossary: Option<String>,
    phrase_field: Option<String>,
    entity_map_field: Option<String>,
    phrase_ignore_case: Option<bool>,
    phrase_ner: Option<bool>,
    position_fields: Option<Vec<String>>,
    bigram_fields: Option<Vec<String>>,
    sentence_fields: Option<Vec<String>>,
    map_numeric_fields: Option<Vec<String>>,
    integer_fields: Option<Vec<String>>,
    geo_fields: Option<Vec<String>>,
    wal: Option<bool>,
    wal_buckets: Option<u32>,
    vocab: Option<bool>,
    vocab_window_docs: Option<u64>,
    vocab_top_k: Option<usize>,
    shard_map: Option<String>,
    shard_map_reload_ms: Option<u64>,
    replica_sync_ms: Option<u64>,
    replica_state: Option<String>,
    control_state: Option<String>,
    control_reconcile_ms: Option<u64>,
    control_lease_ms: Option<u64>,
    control_replication_factor: Option<usize>,
    control_split_rows: Option<u64>,
    control_merge_rows: Option<u64>,
    control_compact_segments: Option<u32>,
    control_compact_tombstone_ppm: Option<u32>,
    node_id: Option<String>,
    control_addr: Option<String>,
    failure_domain: Option<String>,
    data_dir: Option<String>,
    advertise_addr: Option<String>,
    replica_listen: Option<String>,
    node_report_ms: Option<u64>,
    node_reconcile_ms: Option<u64>,
    node_lease_ms: Option<u64>,
    replica_lag_bound: Option<u64>,
    shards: Vec<FileShard>,
}

/// `[clustered_turbovec]` accepts exactly one of `coordinator` (external) or
/// `nodes` (embedded coordinator).
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct FileClusteredTurboVec {
    coordinator: Option<String>,
    nodes: Option<Vec<String>>,
    state: Option<String>,
    allow_ephemeral: Option<bool>,
}

/// One `[[shards]]` table in the TOML file.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct FileShard {
    collection: Option<String>,
    listen: Option<String>,
    vector_backend: Option<String>,
    index: Option<String>,
    slot_offset: Option<u64>,
    demo_vectors: Option<usize>,
    analysis_addr: Option<String>,
    wal: Option<bool>,
    wal_buckets: Option<u32>,
    vocab: Option<bool>,
    shard_id: Option<String>,
    hash_lo: Option<u64>,
    hash_hi: Option<u64>,
}

fn arg_value(args: &[String], key: &str) -> Option<String> {
    let prefix = format!("--{key}=");
    if let Some(v) = args
        .iter()
        .find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
    {
        return Some(v);
    }
    // Also accept the space-separated form (`--key value`).
    let flag = format!("--{key}");
    args.windows(2)
        .find(|w| w[0] == flag && !w[1].starts_with("--"))
        .map(|w| w[1].clone())
}

/// Both bounds or neither; an inverted range is refused.
fn parse_hash_range(
    lo: Option<u64>,
    hi: Option<u64>,
    what: &str,
) -> Result<Option<(u64, u64)>, String> {
    match (lo, hi) {
        (Some(lo), Some(hi)) if lo <= hi => Ok(Some((lo, hi))),
        (Some(lo), Some(hi)) => Err(format!("{what}: range {lo}..={hi} is inverted")),
        (None, None) => Ok(None),
        _ => Err(format!("{what}: pass both bounds or neither")),
    }
}

fn flag_present(args: &[String], key: &str) -> bool {
    args.iter().any(|a| a == &format!("--{key}"))
}

/// CLI > env > file, for string-valued options.
fn opt(args: &[String], key: &str, env: &str, file: Option<&str>) -> Option<String> {
    let neutral_env = env
        .strip_prefix("TURBOVEC_")
        .map(|suffix| format!("PIPESTREAM_SEARCH_{suffix}"));
    arg_value(args, key)
        .or_else(|| {
            neutral_env
                .as_deref()
                .and_then(|name| std::env::var(name).ok())
        })
        .or_else(|| std::env::var(env).ok())
        .or_else(|| file.map(str::to_string))
}

fn parse_env_bool(s: &str) -> bool {
    matches!(s, "1" | "true" | "on" | "yes")
}

fn normalize_addrs(addrs: Vec<String>) -> Vec<String> {
    addrs
        .into_iter()
        .map(|s| {
            if s.starts_with("http://") || s.starts_with("https://") {
                s
            } else {
                format!("http://{s}")
            }
        })
        .collect()
}

fn normalize_analysis_backend(value: String) -> String {
    if matches!(value.trim(), "native" | "native://") {
        crate::analyzer::NATIVE_ANALYSIS_BACKEND.to_string()
    } else {
        normalize_addrs(vec![value]).remove(0)
    }
}

/// Parse configuration from process args (excluding argv[0]).
pub fn parse(args: &[String]) -> Result<Config, String> {
    // The config file sits at the bottom of the precedence stack.
    let file: FileConfig = match opt(args, "config", "TURBOVEC_CONFIG", None) {
        Some(path) => {
            let text =
                std::fs::read_to_string(&path).map_err(|e| format!("read config {path}: {e}"))?;
            toml::from_str(&text).map_err(|e| format!("parse config {path}: {e}"))?
        }
        None => FileConfig::default(),
    };

    let role = match opt(args, "role", "TURBOVEC_ROLE", file.role.as_deref())
        .unwrap_or_else(|| "node".to_string())
        .as_str()
    {
        "node" => Role::Node,
        "coordinator" => Role::Coordinator,
        "both" => Role::Both,
        other => return Err(format!("unknown role {other:?} (node|coordinator|both)")),
    };

    let coord_listen = opt(
        args,
        "coord-listen",
        "TURBOVEC_COORD_LISTEN",
        file.coord_listen.as_deref(),
    )
    .unwrap_or_else(|| "0.0.0.0:50050".to_string())
    .parse::<SocketAddr>()
    .map_err(|e| format!("invalid coordinator listen address: {e}"))?;

    let metrics_listen = opt(
        args,
        "metrics-listen",
        "TURBOVEC_METRICS_LISTEN",
        file.metrics_listen.as_deref(),
    )
    .map(|a| {
        a.parse::<SocketAddr>()
            .map_err(|e| format!("invalid metrics listen address: {e}"))
    })
    .transpose()?;

    // Coordinator fan-out list. A shard map (--shard-map) REPLACES
    // --nodes: it carries the same addresses plus topology metadata.
    let shard_map_path = opt(
        args,
        "shard-map",
        "TURBOVEC_SHARD_MAP",
        file.shard_map.as_deref(),
    )
    .map(PathBuf::from);
    let shard_map = shard_map_path.as_deref().map(load_shard_map).transpose()?;
    let shard_map_reload_ms = opt(
        args,
        "shard-map-reload-ms",
        "TURBOVEC_SHARD_MAP_RELOAD_MS",
        file.shard_map_reload_ms.map(|v| v.to_string()).as_deref(),
    )
    .map(|value| {
        value
            .parse::<u64>()
            .map_err(|e| format!("invalid --shard-map-reload-ms: {e}"))
    })
    .transpose()?
    .unwrap_or_else(|| u64::from(shard_map_path.is_some()) * 1_000);
    if shard_map_reload_ms > 0 && shard_map_path.is_none() {
        return Err("--shard-map-reload-ms requires --shard-map".to_string());
    }
    let nodes_given = opt(args, "nodes", "TURBOVEC_NODES", None).is_some() || file.nodes.is_some();
    if shard_map.is_some() && nodes_given {
        return Err("--shard-map replaces --nodes; pass exactly one".to_string());
    }
    let node_addrs = match &shard_map {
        Some(map) => normalize_addrs(map.shards.iter().map(|s| s.addr.clone()).collect()),
        None => match opt(args, "nodes", "TURBOVEC_NODES", None) {
            Some(s) => normalize_addrs(
                s.split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string)
                    .collect(),
            ),
            None => normalize_addrs(file.nodes.clone().unwrap_or_default()),
        },
    };

    let clustered_file = file.clustered_turbovec.as_ref();
    let clustered_endpoint = opt(
        args,
        "turbovec-coordinator",
        "TURBOVEC_COORDINATOR",
        clustered_file.and_then(|config| config.coordinator.as_deref()),
    );
    let clustered_nodes = opt(
        args,
        "turbovec-cluster-nodes",
        "TURBOVEC_CLUSTER_NODES",
        clustered_file
            .and_then(|config| config.nodes.as_ref())
            .map(|nodes| nodes.join(","))
            .as_deref(),
    );
    if clustered_endpoint.is_some() && clustered_nodes.is_some() {
        return Err(
            "clustered TurboVec accepts exactly one of coordinator or nodes, not both".to_string(),
        );
    }
    let clustered_state = opt(
        args,
        "turbovec-cluster-state",
        "TURBOVEC_CLUSTER_STATE",
        clustered_file.and_then(|config| config.state.as_deref()),
    )
    .map(PathBuf::from);
    let clustered_allow_ephemeral = flag_present(args, "allow-ephemeral-turbovec-cluster")
        || match opt(
            args,
            "allow-ephemeral-turbovec-cluster",
            "TURBOVEC_ALLOW_EPHEMERAL_CLUSTER",
            None,
        ) {
            Some(value) => parse_env_bool(&value),
            None => clustered_file
                .and_then(|config| config.allow_ephemeral)
                .unwrap_or(false),
        };
    let clustered_turbovec = match (clustered_endpoint, clustered_nodes) {
        (Some(endpoint), None) => {
            if clustered_state.is_some() || clustered_allow_ephemeral {
                return Err(
                    "cluster state and allow_ephemeral apply only to an in-process TurboVec coordinator"
                        .to_string(),
                );
            }
            Some(ClusteredTurboVecConfig::External {
                endpoint: normalize_addrs(vec![endpoint]).remove(0),
            })
        }
        (None, Some(nodes)) => {
            let nodes: Vec<String> = nodes
                .split(',')
                .map(str::trim)
                .filter(|entry| !entry.is_empty())
                .map(str::to_string)
                .collect();
            if nodes.is_empty() {
                return Err("clustered TurboVec node table is empty".to_string());
            }
            if clustered_state.is_none() && !clustered_allow_ephemeral {
                return Err(
                    "an in-process TurboVec coordinator requires turbovec_cluster_state; set allow_ephemeral only for tests or demos"
                        .to_string(),
                );
            }
            Some(ClusteredTurboVecConfig::InProcess {
                nodes,
                state: clustered_state,
                allow_ephemeral: clustered_allow_ephemeral,
            })
        }
        (None, None) => {
            if clustered_state.is_some() || clustered_allow_ephemeral {
                return Err(
                    "cluster state or allow_ephemeral was set without clustered TurboVec nodes"
                        .to_string(),
                );
            }
            None
        }
        (Some(_), Some(_)) => unreachable!("mutual exclusion checked above"),
    };

    let dim = opt(
        args,
        "dim",
        "TURBOVEC_DIM",
        file.dim.map(|d| d.to_string()).as_deref(),
    )
    .unwrap_or_else(|| "128".to_string())
    .parse::<usize>()
    .map_err(|e| format!("invalid dim: {e}"))?;
    let bit_width = opt(
        args,
        "bit-width",
        "TURBOVEC_BIT_WIDTH",
        file.bit_width.map(|b| b.to_string()).as_deref(),
    )
    .unwrap_or_else(|| "4".to_string())
    .parse::<usize>()
    .map_err(|e| format!("invalid bit width: {e}"))?;
    let vector_backend = opt(
        args,
        "vector-backend",
        "TURBOVEC_VECTOR_BACKEND",
        file.vector_backend.as_deref(),
    )
    .unwrap_or_else(|| crate::vector::EMBEDDED_TURBOVEC.to_string());

    // Shard set. CLI --index/--demo-vectors (with --node-listen and
    // --slot-offset) describes a single shard and overrides the file's
    // [[shards]] entirely; otherwise the file's shards are used.
    let node_listen_default = opt(
        args,
        "node-listen",
        "TURBOVEC_NODE_LISTEN",
        file.node_listen.as_deref(),
    )
    .unwrap_or_else(|| "0.0.0.0:50051".to_string());
    let cli_index = opt(args, "index", "TURBOVEC_INDEX", file.index.as_deref());
    let cli_demo = opt(
        args,
        "demo-vectors",
        "TURBOVEC_DEMO_VECTORS",
        file.demo_vectors.map(|d| d.to_string()).as_deref(),
    );
    let cli_offset = opt(
        args,
        "slot-offset",
        "TURBOVEC_SLOT_OFFSET",
        file.slot_offset.map(|o| o.to_string()).as_deref(),
    )
    .unwrap_or_else(|| "0".to_string())
    .parse::<u64>()
    .map_err(|e| format!("invalid slot offset: {e}"))?;

    // Write-ahead log: default on for persisted shards, always off for
    // demo shards (they have no index path to log next to). Bucket count
    // is a power of two (max 1024) and caps cheap split granularity.
    let wal_default = match opt(args, "wal", "TURBOVEC_WAL", None) {
        Some(s) => parse_env_bool(&s),
        None => file.wal.unwrap_or(true),
    };
    let wal_buckets_default = opt(
        args,
        "wal-buckets",
        "TURBOVEC_WAL_BUCKETS",
        file.wal_buckets.map(|b| b.to_string()).as_deref(),
    )
    .map(|s| {
        s.parse::<u32>()
            .map_err(|e| format!("invalid wal bucket count: {e}"))
    })
    .transpose()?
    .unwrap_or(64);
    let parse_buckets = |b: u32, what: &str| -> Result<u32, String> {
        if !b.is_power_of_two() || b == 0 || b > 1024 {
            return Err(format!(
                "{what}: wal bucket count must be a power of two in 1..=1024, got {b}"
            ));
        }
        Ok(b)
    };
    let wal_buckets_default = parse_buckets(wal_buckets_default, "--wal-buckets")?;

    // Vocabulary accumulation: default OFF (zero overhead); per-shard file
    // entries may override the process default. Window and top-K sizing is
    // process-wide (see docs/VOCABULARY-INDEX.md).
    let vocab_default = match opt(args, "vocab", "TURBOVEC_VOCAB", None) {
        Some(s) => parse_env_bool(&s),
        None => file.vocab.unwrap_or(false),
    };
    let vocab_window_docs = opt(
        args,
        "vocab-window-docs",
        "TURBOVEC_VOCAB_WINDOW_DOCS",
        file.vocab_window_docs.map(|v| v.to_string()).as_deref(),
    )
    .map(|s| {
        s.parse::<u64>()
            .map_err(|e| format!("invalid vocab window docs: {e}"))
    })
    .transpose()?
    .unwrap_or(crate::vocab::DEFAULT_WINDOW_DOCS);
    if vocab_window_docs == 0 {
        return Err("vocab window docs must be positive".to_string());
    }
    let vocab_top_k = opt(
        args,
        "vocab-top-k",
        "TURBOVEC_VOCAB_TOP_K",
        file.vocab_top_k.map(|v| v.to_string()).as_deref(),
    )
    .map(|s| {
        s.parse::<usize>()
            .map_err(|e| format!("invalid vocab top-K: {e}"))
    })
    .transpose()?
    .unwrap_or(crate::vocab::HeavyHitters::DEFAULT_CAPACITY);
    if vocab_top_k == 0 {
        return Err("vocab top-K must be positive".to_string());
    }

    let cli_collection = opt(args, "collection", "PIPESTREAM_SEARCH_COLLECTION", None);
    if let Some(name) = &cli_collection {
        crate::collections::validate_name(name).map_err(|e| format!("--collection: {e}"))?;
    }
    for (i, shard) in file.shards.iter().enumerate() {
        if let Some(name) = &shard.collection {
            crate::collections::validate_name(name)
                .map_err(|e| format!("shards[{i}].collection: {e}"))?;
        }
    }
    let mut shards: Vec<ShardConfig> = if cli_index.is_some() || cli_demo.is_some() {
        if cli_index.is_some() && cli_demo.is_some() {
            return Err("--index and --demo-vectors are mutually exclusive".to_string());
        }
        let listen = node_listen_default
            .parse::<SocketAddr>()
            .map_err(|e| format!("invalid node listen address: {e}"))?;
        let demo = cli_demo
            .map(|s| {
                s.parse::<usize>()
                    .map(|vectors| DemoConfig {
                        vectors,
                        dim,
                        bit_width,
                    })
                    .map_err(|e| format!("invalid demo vector count: {e}"))
            })
            .transpose()?;
        vec![ShardConfig {
            listen,
            vector_backend: vector_backend.clone(),
            wal: wal_default && demo.is_none(),
            wal_buckets: wal_buckets_default,
            vocab: vocab_default && demo.is_none(),
            index_path: cli_index.map(PathBuf::from),
            demo,
            slot_offset: cli_offset,
            analysis_addr: None,
            collection: cli_collection.clone().unwrap_or_default(),
            shard_id: opt(args, "shard-id", "PIPESTREAM_SEARCH_SHARD_ID", None),
            hash_range: parse_hash_range(
                opt(args, "hash-lo", "PIPESTREAM_SEARCH_HASH_LO", None)
                    .map(|v| {
                        v.parse::<u64>()
                            .map_err(|e| format!("invalid --hash-lo: {e}"))
                    })
                    .transpose()?,
                opt(args, "hash-hi", "PIPESTREAM_SEARCH_HASH_HI", None)
                    .map(|v| {
                        v.parse::<u64>()
                            .map_err(|e| format!("invalid --hash-hi: {e}"))
                    })
                    .transpose()?,
                "--hash-lo/--hash-hi",
            )?,
        }]
    } else {
        file.shards
            .iter()
            .enumerate()
            .map(|(i, shard)| {
                let listen = shard
                    .listen
                    .clone()
                    .unwrap_or_else(|| node_listen_default.clone())
                    .parse::<SocketAddr>()
                    .map_err(|e| format!("shards[{i}]: invalid listen address: {e}"))?;
                let demo = shard.demo_vectors.map(|vectors| DemoConfig {
                    vectors,
                    dim,
                    bit_width,
                });
                if shard.index.is_some() == demo.is_some() {
                    return Err(format!(
                        "shards[{i}]: exactly one of index / demo_vectors is required"
                    ));
                }
                Ok(ShardConfig {
                    listen,
                    vector_backend: shard
                        .vector_backend
                        .clone()
                        .unwrap_or_else(|| vector_backend.clone()),
                    wal: shard.wal.unwrap_or(wal_default) && demo.is_none(),
                    wal_buckets: parse_buckets(
                        shard.wal_buckets.unwrap_or(wal_buckets_default),
                        &format!("shards[{i}]"),
                    )?,
                    vocab: shard.vocab.unwrap_or(vocab_default) && demo.is_none(),
                    index_path: shard.index.as_ref().map(PathBuf::from),
                    demo,
                    slot_offset: shard.slot_offset.unwrap_or(0),
                    analysis_addr: shard.analysis_addr.clone().map(normalize_analysis_backend),
                    collection: shard.collection.clone().unwrap_or_default(),
                    shard_id: shard.shard_id.clone(),
                    hash_range: parse_hash_range(
                        shard.hash_lo,
                        shard.hash_hi,
                        &format!("shards[{i}].hash_lo/hash_hi"),
                    )?,
                })
            })
            .collect::<Result<_, String>>()?
    };

    let chunk_blocks = opt(
        args,
        "chunk-blocks",
        "TURBOVEC_CHUNK_BLOCKS",
        file.chunk_blocks.map(|c| c.to_string()).as_deref(),
    )
    .unwrap_or_else(|| DEFAULT_CHUNK_BLOCKS.to_string())
    .parse::<usize>()
    .map_err(|e| format!("invalid chunk blocks: {e}"))?;

    let share_floors = match opt(args, "floor-sharing", "TURBOVEC_FLOOR_SHARING", None) {
        Some(s) => parse_env_bool(&s),
        None => file.floor_sharing.unwrap_or(true),
    };
    let block_max = match opt(args, "block-max", "TURBOVEC_BLOCK_MAX", None) {
        Some(s) => parse_env_bool(&s),
        None => file.block_max.unwrap_or(true),
    };
    let segment_pruning = match opt(args, "segment-pruning", "TURBOVEC_SEGMENT_PRUNING", None) {
        Some(s) => parse_env_bool(&s),
        None => file.segment_pruning.unwrap_or(true),
    };
    let shard_pruning = match opt(args, "shard-pruning", "TURBOVEC_SHARD_PRUNING", None) {
        Some(s) => parse_env_bool(&s),
        None => file.shard_pruning.unwrap_or(true),
    };
    let placement_column = opt(
        args,
        "placement-column",
        "TURBOVEC_PLACEMENT_COLUMN",
        file.placement_column.as_deref(),
    )
    .map(|s| s.trim().to_string())
    .filter(|s| !s.is_empty());
    let placement_leaf = opt(
        args,
        "placement-leaf",
        "TURBOVEC_PLACEMENT_LEAF",
        file.placement_leaf.map(|v| v.to_string()).as_deref(),
    )
    .map(|s| {
        s.trim()
            .parse::<i64>()
            .map_err(|e| format!("invalid placement leaf {s:?}: {e}"))
            .and_then(|code| {
                if code < 0 {
                    Err(format!(
                        "placement leaf {code} is negative; codes are non-negative path codes"
                    ))
                } else {
                    Ok(code)
                }
            })
    })
    .transpose()?;
    if placement_leaf.is_some() && placement_column.is_none() {
        return Err(
            "--placement-leaf needs --placement-column, the column that holds the code".to_string(),
        );
    }
    let allow_missing_bm25 = flag_present(args, "allow-missing-bm25")
        || match opt(
            args,
            "allow-missing-bm25",
            "TURBOVEC_ALLOW_MISSING_BM25",
            None,
        ) {
            Some(s) => parse_env_bool(&s),
            None => file.allow_missing_bm25.unwrap_or(false),
        };
    let coalesce = match opt(args, "coalesce", "TURBOVEC_COALESCE", None) {
        Some(s) => parse_env_bool(&s),
        None => file.coalesce.unwrap_or(true),
    };
    let scan_parallel = opt(
        args,
        "scan-parallel",
        "TURBOVEC_SCAN_PARALLEL",
        file.scan_parallel.map(|v| v.to_string()).as_deref(),
    )
    .map(|s| {
        s.parse::<usize>()
            .map_err(|e| format!("invalid scan parallel: {e}"))
    })
    .transpose()?
    .unwrap_or(0);
    let rerank_parallel = opt(
        args,
        "rerank-parallel",
        "TURBOVEC_RERANK_PARALLEL",
        file.rerank_parallel.map(|v| v.to_string()).as_deref(),
    )
    .map(|s| {
        s.parse::<usize>()
            .map_err(|e| format!("invalid rerank parallel: {e}"))
    })
    .transpose()?
    .unwrap_or(0);
    if rerank_parallel > crate::node::MAX_RERANK_PARALLEL {
        return Err(format!(
            "rerank parallel must be <= {}, got {rerank_parallel}",
            crate::node::MAX_RERANK_PARALLEL
        ));
    }
    let floor_delta = opt(
        args,
        "floor-delta",
        "TURBOVEC_FLOOR_DELTA",
        file.floor_delta.map(|v| v.to_string()).as_deref(),
    )
    .map(|s| {
        s.parse::<f32>()
            .map_err(|e| format!("invalid floor delta: {e}"))
            .and_then(|v| {
                if v.is_finite() && v >= 0.0 {
                    Ok(v)
                } else {
                    Err(format!("floor delta must be finite and >= 0, got {v}"))
                }
            })
    })
    .transpose()?
    .unwrap_or(0.0);
    let floor_warmup_chunks = opt(
        args,
        "floor-warmup-chunks",
        "TURBOVEC_FLOOR_WARMUP_CHUNKS",
        file.floor_warmup_chunks.map(|v| v.to_string()).as_deref(),
    )
    .map(|s| {
        s.parse::<u32>()
            .map_err(|e| format!("invalid floor-warmup-chunks: {e}"))
    })
    .transpose()?
    .unwrap_or(0);
    let floor_min_interval_ms = opt(
        args,
        "floor-min-interval-ms",
        "TURBOVEC_FLOOR_MIN_INTERVAL_MS",
        file.floor_min_interval_ms.map(|v| v.to_string()).as_deref(),
    )
    .map(|s| {
        s.parse::<u64>()
            .map_err(|e| format!("invalid floor-min-interval-ms: {e}"))
    })
    .transpose()?
    .unwrap_or(0);
    let parse_ms = |key: &str, env: &str, file_val: Option<u64>| -> Result<u64, String> {
        opt(args, key, env, file_val.map(|v| v.to_string()).as_deref())
            .map(|s| {
                s.parse::<u64>()
                    .map_err(|e| format!("invalid --{key}: {e}"))
            })
            .transpose()
            .map(|v| v.unwrap_or(0))
    };
    let shard_deadline_ms = parse_ms(
        "shard-deadline-ms",
        "TURBOVEC_SHARD_DEADLINE_MS",
        file.shard_deadline_ms,
    )?;
    let hedge_delay_ms = parse_ms(
        "hedge-delay-ms",
        "TURBOVEC_HEDGE_DELAY_MS",
        file.hedge_delay_ms,
    )?;
    let replica_addrs: Vec<Option<String>> = match &shard_map {
        Some(map) => map
            .shards
            .iter()
            .map(|s| {
                s.replica
                    .clone()
                    .map(|a| normalize_addrs(vec![a]).remove(0))
            })
            .collect(),
        None => Vec::new(),
    };
    let replica_sync_ms = opt(
        args,
        "replica-sync-ms",
        "TURBOVEC_REPLICA_SYNC_MS",
        file.replica_sync_ms.map(|v| v.to_string()).as_deref(),
    )
    .map(|value| {
        value
            .parse::<u64>()
            .map_err(|e| format!("invalid --replica-sync-ms: {e}"))
    })
    .transpose()?
    .unwrap_or_else(|| u64::from(replica_addrs.iter().any(Option::is_some)) * 1_000);
    let replica_state_path = opt(
        args,
        "replica-state",
        "TURBOVEC_REPLICA_STATE",
        file.replica_state.as_deref(),
    )
    .map(PathBuf::from)
    .or_else(|| {
        shard_map_path.as_ref().map(|path| {
            let mut name = path.as_os_str().to_owned();
            name.push(".replica-sync.toml");
            PathBuf::from(name)
        })
    });
    if replica_sync_ms > 0 && replica_state_path.is_none() {
        return Err("automatic replica sync requires --replica-state or --shard-map".to_string());
    }
    let control_state_path = opt(
        args,
        "control-state",
        "PIPESTREAM_SEARCH_CONTROL_STATE",
        file.control_state.as_deref(),
    )
    .map(PathBuf::from);
    if control_state_path.is_some() && shard_map.is_none() {
        return Err("--control-state requires a generation-stamped --shard-map".to_string());
    }
    let parse_control_u64 =
        |key: &str, env: &str, file_value: Option<u64>, default: u64| -> Result<u64, String> {
            opt(
                args,
                key,
                env,
                file_value.map(|value| value.to_string()).as_deref(),
            )
            .map(|value| {
                value
                    .parse::<u64>()
                    .map_err(|error| format!("invalid --{key}: {error}"))
            })
            .transpose()
            .map(|value| value.unwrap_or(default))
        };
    let control_reconcile_ms = parse_control_u64(
        "control-reconcile-ms",
        "PIPESTREAM_SEARCH_CONTROL_RECONCILE_MS",
        file.control_reconcile_ms,
        u64::from(control_state_path.is_some()) * 1_000,
    )?;
    let control_lease_ms = parse_control_u64(
        "control-lease-ms",
        "PIPESTREAM_SEARCH_CONTROL_LEASE_MS",
        file.control_lease_ms,
        15_000,
    )?;
    let control_replication_factor = usize::try_from(parse_control_u64(
        "control-replication-factor",
        "PIPESTREAM_SEARCH_CONTROL_REPLICATION_FACTOR",
        file.control_replication_factor.map(|value| value as u64),
        2,
    )?)
    .map_err(|_| "--control-replication-factor does not fit usize".to_string())?;
    let control_split_rows = parse_control_u64(
        "control-split-rows",
        "PIPESTREAM_SEARCH_CONTROL_SPLIT_ROWS",
        file.control_split_rows,
        25_000_000,
    )?;
    let control_merge_rows = parse_control_u64(
        "control-merge-rows",
        "PIPESTREAM_SEARCH_CONTROL_MERGE_ROWS",
        file.control_merge_rows,
        2_000_000,
    )?;
    let control_compact_segments = u32::try_from(parse_control_u64(
        "control-compact-segments",
        "PIPESTREAM_SEARCH_CONTROL_COMPACT_SEGMENTS",
        file.control_compact_segments.map(u64::from),
        8,
    )?)
    .map_err(|_| "--control-compact-segments exceeds u32".to_string())?;
    let control_compact_tombstone_ppm = u32::try_from(parse_control_u64(
        "control-compact-tombstone-ppm",
        "PIPESTREAM_SEARCH_CONTROL_COMPACT_TOMBSTONE_PPM",
        file.control_compact_tombstone_ppm.map(u64::from),
        100_000,
    )?)
    .map_err(|_| "--control-compact-tombstone-ppm exceeds u32".to_string())?;
    if control_state_path.is_some()
        && (!matches!(role, Role::Coordinator | Role::Both)
            || control_replication_factor == 0
            || control_lease_ms < 1_000
            || control_split_rows == 0
            || control_merge_rows == 0
            || control_compact_segments == 0
            || control_compact_tombstone_ppm == 0
            || control_compact_tombstone_ppm > 1_000_000)
    {
        return Err("control plane requires coordinator/both role, positive thresholds and replication factor, lease >= 1000ms, and tombstone ppm <= 1000000".to_string());
    }

    // Node membership (docs/cluster-control.md "Node lifecycle").
    let node_id = opt(
        args,
        "node-id",
        "PIPESTREAM_SEARCH_NODE_ID",
        file.node_id.as_deref(),
    );
    let control_addr = opt(
        args,
        "control-addr",
        "PIPESTREAM_SEARCH_CONTROL_ADDR",
        file.control_addr.as_deref(),
    );
    let failure_domain = opt(
        args,
        "failure-domain",
        "PIPESTREAM_SEARCH_FAILURE_DOMAIN",
        file.failure_domain.as_deref(),
    );
    let data_dir = opt(
        args,
        "data-dir",
        "PIPESTREAM_SEARCH_DATA_DIR",
        file.data_dir.as_deref(),
    );
    let advertise_addr = opt(
        args,
        "advertise-addr",
        "PIPESTREAM_SEARCH_ADVERTISE_ADDR",
        file.advertise_addr.as_deref(),
    );
    let replica_listen = opt(
        args,
        "replica-listen",
        "PIPESTREAM_SEARCH_REPLICA_LISTEN",
        file.replica_listen.as_deref(),
    )
    .map(|a| {
        a.parse::<SocketAddr>()
            .map_err(|e| format!("invalid --replica-listen: {e}"))
    })
    .transpose()?;
    let node_report_ms = parse_control_u64(
        "node-report-ms",
        "PIPESTREAM_SEARCH_NODE_REPORT_MS",
        file.node_report_ms,
        10_000,
    )?;
    let node_reconcile_ms = parse_control_u64(
        "node-reconcile-ms",
        "PIPESTREAM_SEARCH_NODE_RECONCILE_MS",
        file.node_reconcile_ms,
        2_000,
    )?;
    let node_lease_ms = parse_control_u64(
        "node-lease-ms",
        "PIPESTREAM_SEARCH_NODE_LEASE_MS",
        file.node_lease_ms,
        0,
    )?;
    let replica_lag_bound = parse_control_u64(
        "replica-lag-bound",
        "PIPESTREAM_SEARCH_REPLICA_LAG_BOUND",
        file.replica_lag_bound,
        0,
    )?;
    let membership = match node_id {
        Some(node_id) => {
            if node_id.trim().is_empty() {
                return Err("--node-id must not be empty".to_string());
            }
            if !matches!(role, Role::Node | Role::Both) {
                return Err(
                    "--node-id registers a node: it needs the node or both role".to_string()
                );
            }
            let control_addr = control_addr.ok_or_else(|| {
                "--node-id needs --control-addr (the coordinator's ClusterControl endpoint)"
                    .to_string()
            })?;
            let data_dir = data_dir.ok_or_else(|| {
                "--node-id needs --data-dir (where placed replicas live)".to_string()
            })?;
            if node_report_ms == 0 || node_reconcile_ms == 0 {
                return Err("--node-report-ms and --node-reconcile-ms must be positive".to_string());
            }
            if let Some(addr) = &advertise_addr {
                let well_formed = addr
                    .rsplit_once(':')
                    .is_some_and(|(host, port)| !host.is_empty() && port.parse::<u16>().is_ok());
                if !well_formed {
                    return Err(format!("--advertise-addr={addr:?} is not host:port"));
                }
            }
            Some(NodeMembershipConfig {
                node_id,
                control_addr: normalize_addr(control_addr),
                failure_domain: failure_domain.unwrap_or_default(),
                data_dir: PathBuf::from(data_dir),
                advertise_addr,
                replica_listen,
                report_ms: node_report_ms,
                reconcile_ms: node_reconcile_ms,
                lease_ms: node_lease_ms,
                lag_bound: replica_lag_bound,
            })
        }
        None => {
            if control_addr.is_some()
                || data_dir.is_some()
                || failure_domain.is_some()
                || advertise_addr.is_some()
                || replica_listen.is_some()
            {
                return Err(
                    "--control-addr, --data-dir, --failure-domain, --advertise-addr, and \
                     --replica-listen describe a registered node: pass --node-id with them"
                        .to_string(),
                );
            }
            None
        }
    };

    let max_message_bytes = opt(
        args,
        "max-message-mib",
        "TURBOVEC_MAX_MESSAGE_MIB",
        file.max_message_mib.map(|m| m.to_string()).as_deref(),
    )
    .map(|s| {
        s.parse::<usize>()
            .map(|mib| mib * 1024 * 1024)
            .map_err(|e| format!("invalid max message MiB: {e}"))
    })
    .transpose()?
    .unwrap_or(MAX_MESSAGE_BYTES);

    let demo_query = flag_present(args, "demo-query")
        || std::env::var("PIPESTREAM_SEARCH_DEMO_QUERY")
            .or_else(|_| std::env::var("TURBOVEC_DEMO_QUERY"))
            .map(|s| parse_env_bool(&s))
            .unwrap_or(false)
        || file.demo_query.unwrap_or(false);
    let query_dim = opt(
        args,
        "query-dim",
        "TURBOVEC_QUERY_DIM",
        file.query_dim.map(|d| d.to_string()).as_deref(),
    )
    .map(|s| {
        s.parse::<usize>()
            .map_err(|e| format!("invalid query dim: {e}"))
    })
    .transpose()?
    .unwrap_or(dim);

    if matches!(role, Role::Node | Role::Both) && shards.is_empty() {
        return Err(
            "node/both role requires at least one shard (--index/--demo-vectors or [[shards]])"
                .to_string(),
        );
    }
    if matches!(role, Role::Coordinator | Role::Both)
        && node_addrs.is_empty()
        && file.collections.is_empty()
    {
        return Err(
            "coordinator role requires --nodes or --shard-map (or `nodes` or `collections` in \
             the config file)"
                .to_string(),
        );
    }
    if role == Role::Node && clustered_turbovec.is_some() {
        return Err(
            "clustered TurboVec is a product-coordinator backend and cannot be configured on a node-only process"
                .to_string(),
        );
    }
    if demo_query && role == Role::Node {
        return Err("--demo-query requires the coordinator or both role".to_string());
    }

    let save_on_shutdown = match opt(args, "save-on-shutdown", "TURBOVEC_SAVE_ON_SHUTDOWN", None) {
        Some(s) => parse_env_bool(&s),
        None => file.save_on_shutdown.unwrap_or(true),
    };

    let stream_search = flag_present(args, "stream-search")
        || std::env::var("PIPESTREAM_SEARCH_STREAM_SEARCH")
            .or_else(|_| std::env::var("TURBOVEC_STREAM_SEARCH"))
            .map(|s| parse_env_bool(&s))
            .unwrap_or(false)
        || file.stream_search.unwrap_or(false);

    let relay = flag_present(args, "relay")
        || std::env::var("PIPESTREAM_SEARCH_RELAY")
            .map(|s| parse_env_bool(&s))
            .unwrap_or(false)
        || file.relay.unwrap_or(false);
    if relay {
        if role != Role::Coordinator {
            return Err(
                "--relay serves the node-facing surface over a coordinator's shard set and \
                 takes --role=coordinator only"
                    .to_string(),
            );
        }
        if !file.collections.is_empty() {
            return Err(
                "--relay serves one unnamed collection on a dedicated endpoint; named \
                 [[collections]] are not multiplexed through a relay"
                    .to_string(),
            );
        }
    }

    let bm25_stream = if flag_present(args, "bm25-stream") {
        true
    } else {
        opt(
            args,
            "bm25-stream",
            "TURBOVEC_BM25_STREAM",
            file.bm25_stream.map(|value| value.to_string()).as_deref(),
        )
        .map(|value| parse_env_bool(&value))
        .unwrap_or(true)
    };

    let max_k = opt(
        args,
        "max-k",
        "TURBOVEC_MAX_K",
        file.max_k.map(|v| v.to_string()).as_deref(),
    )
    .map(|s| {
        s.parse::<u32>()
            .map_err(|e| format!("invalid max k: {e}"))
            .and_then(|v| {
                if v == 0 {
                    Err("max k must be at least 1 (0 would refuse every query)".to_string())
                } else {
                    Ok(v)
                }
            })
    })
    .transpose()?
    .unwrap_or(crate::coordinator::DEFAULT_MAX_K);
    let max_rerank_bytes = opt(
        args,
        "max-rerank-mib",
        "TURBOVEC_MAX_RERANK_MIB",
        file.max_rerank_mib.map(|v| v.to_string()).as_deref(),
    )
    .map(|s| {
        s.parse::<u64>()
            .map_err(|e| format!("invalid max rerank MiB: {e}"))
            .and_then(|mib| {
                if mib == 0 {
                    return Err("max rerank MiB must be positive".to_string());
                }
                mib.checked_mul(1024 * 1024)
                    .ok_or_else(|| "max rerank MiB overflows bytes".to_string())
            })
    })
    .transpose()?
    .unwrap_or(crate::coordinator::DEFAULT_MAX_RERANK_BYTES);
    let dense_quality_profile = opt(
        args,
        "dense-quality-profile",
        "PIPESTREAM_SEARCH_DENSE_QUALITY_PROFILE",
        file.dense_quality_profile.as_deref(),
    )
    .filter(|path| !path.trim().is_empty())
    .map(PathBuf::from);
    let synonyms = opt(
        args,
        "synonyms",
        "PIPESTREAM_SEARCH_SYNONYMS",
        file.synonyms.as_deref(),
    )
    .filter(|path| !path.trim().is_empty())
    .map(PathBuf::from);
    let dense_execution_policy = opt(
        args,
        "dense-execution-policy",
        "PIPESTREAM_SEARCH_DENSE_EXECUTION_POLICY",
        file.dense_execution_policy.as_deref(),
    )
    .filter(|path| !path.trim().is_empty())
    .map(PathBuf::from);

    let analysis_addr = opt(
        args,
        "analysis-addr",
        "TURBOVEC_ANALYSIS_ADDR",
        file.analysis_addr.as_deref(),
    )
    .map(normalize_analysis_backend);
    // A single-shard CLI setup shares the analysis backend with its shard.
    if analysis_addr.is_some() && shards.len() == 1 && shards[0].analysis_addr.is_none() {
        shards[0].analysis_addr.clone_from(&analysis_addr);
    }
    let bm25_k1 = opt(
        args,
        "bm25-k1",
        "TURBOVEC_BM25_K1",
        file.bm25_k1.map(|v| v.to_string()).as_deref(),
    )
    .map(|s| {
        s.parse::<f32>()
            .map_err(|e| format!("invalid bm25 k1: {e}"))
    })
    .transpose()?
    .unwrap_or(1.2);
    let bm25_b = opt(
        args,
        "bm25-b",
        "TURBOVEC_BM25_B",
        file.bm25_b.map(|v| v.to_string()).as_deref(),
    )
    .map(|s| s.parse::<f32>().map_err(|e| format!("invalid bm25 b: {e}")))
    .transpose()?
    .unwrap_or(0.75);

    let bm25_fields = match opt(args, "bm25-fields", "TURBOVEC_BM25_FIELDS", None) {
        Some(s) => s
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect(),
        None => file
            .bm25_fields
            .clone()
            .unwrap_or_else(|| vec!["body".to_string()]),
    };
    if bm25_fields.first().map(String::as_str) != Some("body") {
        return Err(
            "bm25 fields must start with \"body\" (field 0 is the stored body)".to_string(),
        );
    }
    for (i, name) in bm25_fields.iter().enumerate() {
        if bm25_fields[..i].contains(name) {
            return Err(format!("bm25 field {name:?} repeats in the field table"));
        }
    }

    let facet_fields: Vec<String> = match opt(args, "facet-fields", "TURBOVEC_FACET_FIELDS", None) {
        Some(s) => s
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect(),
        None => file.facet_fields.clone().unwrap_or_default(),
    };
    for (i, name) in facet_fields.iter().enumerate() {
        if facet_fields[..i].contains(name) {
            return Err(format!("facet field {name:?} repeats in the facet table"));
        }
    }

    let numeric_fields: Vec<String> =
        match opt(args, "numeric-fields", "TURBOVEC_NUMERIC_FIELDS", None) {
            Some(s) => s
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect(),
            None => file.numeric_fields.clone().unwrap_or_default(),
        };
    for (i, name) in numeric_fields.iter().enumerate() {
        if numeric_fields[..i].contains(name) {
            return Err(format!(
                "numeric field {name:?} repeats in the numeric table"
            ));
        }
        if facet_fields.contains(name) {
            return Err(format!(
                "column {name:?} is declared as both a facet and a numeric field; \
                 the v7 column table holds one column per name"
            ));
        }
    }

    let parse_list = |flag: &str, env: &str, file_val: &Option<Vec<String>>| -> Vec<String> {
        match opt(args, flag, env, None) {
            Some(s) => s
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect(),
            None => file_val.clone().unwrap_or_default(),
        }
    };
    let map_facet_fields = parse_list(
        "map-facet-fields",
        "TURBOVEC_MAP_FACET_FIELDS",
        &file.map_facet_fields,
    );
    let map_numeric_fields = parse_list(
        "map-numeric-fields",
        "TURBOVEC_MAP_NUMERIC_FIELDS",
        &file.map_numeric_fields,
    );
    let integer_fields = parse_list(
        "integer-fields",
        "TURBOVEC_INTEGER_FIELDS",
        &file.integer_fields,
    );
    let geo_fields = parse_list("geo-fields", "TURBOVEC_GEO_FIELDS", &file.geo_fields);
    // One name space across all column kinds: the v7 column table
    // refuses duplicates, so the config does too, early and by name.
    // The placement column is an integer column whether or not the
    // integer list names it.
    {
        let mut all: Vec<&String> = Vec::new();
        let placement_as_integer: Vec<String> = placement_column
            .iter()
            .filter(|column| !integer_fields.contains(column))
            .cloned()
            .collect();
        for name in facet_fields
            .iter()
            .chain(&numeric_fields)
            .chain(&map_facet_fields)
            .chain(&map_numeric_fields)
            .chain(&integer_fields)
            .chain(&placement_as_integer)
            .chain(&geo_fields)
        {
            if all.contains(&name) {
                return Err(format!(
                    "column {name:?} is declared under more than one column kind; \
                     the v7 column table holds one column per name"
                ));
            }
            all.push(name);
        }
    }

    let phrase_glossary = opt(
        args,
        "phrase-glossary",
        "PIPESTREAM_SEARCH_PHRASE_GLOSSARY",
        file.phrase_glossary.as_deref(),
    )
    .filter(|value| !value.trim().is_empty())
    .map(PathBuf::from);
    let phrase_field = opt(
        args,
        "phrase-field",
        "PIPESTREAM_SEARCH_PHRASE_FIELD",
        file.phrase_field.as_deref(),
    )
    .unwrap_or_else(|| "phrases".to_string());
    let entity_map_field = opt(
        args,
        "entity-map-field",
        "PIPESTREAM_SEARCH_ENTITY_MAP_FIELD",
        file.entity_map_field.as_deref(),
    )
    .filter(|value| !value.trim().is_empty());
    let phrase_ignore_case = opt(
        args,
        "phrase-ignore-case",
        "PIPESTREAM_SEARCH_PHRASE_IGNORE_CASE",
        file.phrase_ignore_case
            .map(|value| value.to_string())
            .as_deref(),
    )
    .map(|value| parse_env_bool(&value))
    .unwrap_or(true);
    let phrase_ner = if flag_present(args, "phrase-ner") {
        true
    } else {
        opt(
            args,
            "phrase-ner",
            "PIPESTREAM_SEARCH_PHRASE_NER",
            file.phrase_ner.map(|value| value.to_string()).as_deref(),
        )
        .map(|value| parse_env_bool(&value))
        .unwrap_or(false)
    };
    if phrase_glossary.is_some() {
        if phrase_field == "body" || !bm25_fields.contains(&phrase_field) {
            return Err(format!(
                "phrase glossary requires phrase field {phrase_field:?} as a non-body entry in --bm25-fields"
            ));
        }
        if let Some(field) = &entity_map_field {
            if !map_facet_fields.contains(field) {
                return Err(format!(
                    "entity map field {field:?} must be declared in --map-facet-fields"
                ));
            }
        }
        if phrase_ner && entity_map_field.is_none() {
            return Err("--phrase-ner requires --entity-map-field".to_string());
        }
    } else if entity_map_field.is_some() || phrase_ner {
        return Err("--entity-map-field and --phrase-ner require --phrase-glossary".to_string());
    }

    // Proximity payloads (docs/phrase-proximity.md). Both are explicit
    // storage declarations, like the phrase field: a positional field
    // must be in the table, and a bigram column is a declared table
    // entry named after its source, never an implicit extra field.
    let list = |name: &str, env: &str, file: Option<&Vec<String>>| -> Vec<String> {
        match opt(args, name, env, None) {
            Some(s) => s
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect(),
            None => file.cloned().unwrap_or_default(),
        }
    };
    let position_fields = list(
        "position-fields",
        "PIPESTREAM_SEARCH_POSITION_FIELDS",
        file.position_fields.as_ref(),
    );
    let bigram_fields = list(
        "bigram-fields",
        "PIPESTREAM_SEARCH_BIGRAM_FIELDS",
        file.bigram_fields.as_ref(),
    );
    let sentence_fields = list(
        "sentence-fields",
        "PIPESTREAM_SEARCH_SENTENCE_FIELDS",
        file.sentence_fields.as_ref(),
    );
    for (i, name) in sentence_fields.iter().enumerate() {
        if sentence_fields[..i].contains(name) {
            return Err(format!(
                "sentence field {name:?} repeats in --sentence-fields"
            ));
        }
        if !bm25_fields.contains(name) {
            return Err(format!(
                "sentence field {name:?} must be declared in --bm25-fields"
            ));
        }
        if name != "body" {
            return Err(format!(
                "sentence field {name:?}: snippets are cut from stored text, and only the body's text is stored; --sentence-fields accepts body only"
            ));
        }
    }
    for (i, name) in position_fields.iter().enumerate() {
        if position_fields[..i].contains(name) {
            return Err(format!(
                "positional field {name:?} repeats in --position-fields"
            ));
        }
        if !bm25_fields.contains(name) {
            return Err(format!(
                "positional field {name:?} must be declared in --bm25-fields"
            ));
        }
        if phrase_glossary.is_some() && *name == phrase_field {
            return Err(format!(
                "phrase field {name:?} holds glossary concepts, not tokens; it cannot keep token positions"
            ));
        }
    }
    for (i, source) in bigram_fields.iter().enumerate() {
        let derived = crate::proximity::bigram_field_name(source);
        if bigram_fields[..i].contains(source) {
            return Err(format!(
                "bigram source {source:?} repeats in --bigram-fields"
            ));
        }
        if !bm25_fields.contains(source) {
            return Err(format!(
                "bigram source field {source:?} must be declared in --bm25-fields"
            ));
        }
        if !bm25_fields.contains(&derived) {
            return Err(format!(
                "bigram column {derived:?} (derived from {source:?}) must be declared in --bm25-fields"
            ));
        }
        if crate::proximity::bigram_source(source).is_some() {
            return Err(format!(
                "bigram source {source:?} is itself a bigram column; columns derive from analyzed fields only"
            ));
        }
        if position_fields.contains(&derived) {
            return Err(format!(
                "bigram column {derived:?} cannot keep token positions; it is a term column derived from {source:?}"
            ));
        }
        if phrase_glossary.is_some() && (*source == phrase_field || derived == phrase_field) {
            return Err(format!(
                "bigram source {source:?} and the phrase field {phrase_field:?} must be distinct"
            ));
        }
    }

    // Named collections (docs/collections.md): each carries its own
    // dataset settings; the top-level dataset knobs must then be absent,
    // because there is no unnamed dataset to apply them to.
    let mut collections: Vec<CollectionConfig> = Vec::with_capacity(file.collections.len());
    if !file.collections.is_empty() {
        if !node_addrs.is_empty() {
            return Err(
                "`collections` replaces --nodes / --shard-map: put each collection's nodes or \
                 shard_map on the collection"
                    .to_string(),
            );
        }
        if control_state_path.is_some() {
            return Err(
                "`collections` replaces --control-state: put each collection's control_state on \
                 the collection"
                    .to_string(),
            );
        }
        if replica_state_path.is_some() {
            return Err(
                "`collections` replaces --replica-state: put each collection's replica_state on \
                 the collection"
                    .to_string(),
            );
        }
        if clustered_turbovec.is_some() {
            return Err(
                "clustered TurboVec serves one dataset and is not yet configurable per \
                 collection; drop `collections` or the clustered backend"
                    .to_string(),
            );
        }
        let mut owners: std::collections::BTreeMap<String, String> =
            std::collections::BTreeMap::new();
        for (i, c) in file.collections.iter().enumerate() {
            let at = format!("collections[{i}]");
            crate::collections::validate_name(&c.name).map_err(|e| format!("{at}: {e}"))?;
            if collections.iter().any(|known| known.name == c.name) {
                return Err(format!("{at}: collection {:?} is declared twice", c.name));
            }
            let shard_map_path = c.shard_map.as_ref().map(PathBuf::from);
            if shard_map_path.is_some() && c.nodes.is_some() {
                return Err(format!("{at}: shard_map replaces nodes; give only one"));
            }
            let shard_map = shard_map_path.as_deref().map(load_shard_map).transpose()?;
            let node_addrs = match &shard_map {
                Some(map) => normalize_addrs(map.shards.iter().map(|s| s.addr.clone()).collect()),
                None => normalize_addrs(c.nodes.clone().unwrap_or_default()),
            };
            if node_addrs.is_empty() {
                return Err(format!("{at}: a collection needs nodes or a shard_map"));
            }
            for addr in &node_addrs {
                if let Some(other) = owners.insert(addr.clone(), c.name.clone()) {
                    return Err(format!(
                        "{at}: node {addr} is already listed under collection {other:?}; a shard \
                         belongs to only one collection"
                    ));
                }
            }
            let replica_addrs: Vec<Option<String>> = match &shard_map {
                Some(map) => map
                    .shards
                    .iter()
                    .map(|s| {
                        s.replica
                            .clone()
                            .map(|r| normalize_addrs(vec![r]).remove(0))
                    })
                    .collect(),
                None => Vec::new(),
            };
            let control_state_path = c.control_state.as_ref().map(PathBuf::from);
            if control_state_path.is_some() && shard_map.is_none() {
                return Err(format!(
                    "{at}: control_state requires a generation-written shard_map"
                ));
            }
            let replica_state_path = c.replica_state.as_ref().map(PathBuf::from);
            if replica_sync_ms > 0
                && replica_addrs.iter().any(Option::is_some)
                && replica_state_path.is_none()
            {
                return Err(format!(
                    "{at}: automatic replica sync needs replica_state on the collection"
                ));
            }
            if shard_map_reload_ms > 0 && shard_map_path.is_none() {
                return Err(format!(
                    "{at}: --shard-map-reload-ms needs a shard_map on every collection"
                ));
            }
            collections.push(CollectionConfig {
                name: c.name.clone(),
                node_addrs,
                replica_addrs,
                shard_map,
                shard_map_path,
                analysis_addr: c
                    .analysis_addr
                    .clone()
                    .map(normalize_analysis_backend)
                    .or_else(|| analysis_addr.clone()),
                bm25_k1: c.bm25_k1.unwrap_or(bm25_k1),
                bm25_b: c.bm25_b.unwrap_or(bm25_b),
                dense_quality_profile: c
                    .dense_quality_profile
                    .as_ref()
                    .map(PathBuf::from)
                    .or_else(|| dense_quality_profile.clone()),
                synonyms: c
                    .synonyms
                    .as_ref()
                    .map(PathBuf::from)
                    .or_else(|| synonyms.clone()),
                dense_execution_policy: c
                    .dense_execution_policy
                    .as_ref()
                    .map(PathBuf::from)
                    .or_else(|| dense_execution_policy.clone()),
                replica_state_path,
                control_state_path,
            });
        }
    }
    // The security surface (docs/security.md). Files are read here so a
    // missing certificate refuses at startup, not at the first connection.
    let path_opt = |key: &str, env: &str, file_value: Option<&str>| {
        opt(args, key, env, file_value).map(PathBuf::from)
    };
    let tls_cert = path_opt(
        "tls-cert",
        "PIPESTREAM_SEARCH_TLS_CERT",
        file.tls_cert.as_deref(),
    );
    let tls_key = path_opt(
        "tls-key",
        "PIPESTREAM_SEARCH_TLS_KEY",
        file.tls_key.as_deref(),
    );
    let tls_client_ca = path_opt(
        "tls-client-ca",
        "PIPESTREAM_SEARCH_TLS_CLIENT_CA",
        file.tls_client_ca.as_deref(),
    );
    let tls = match (tls_cert, tls_key) {
        (Some(cert), Some(key)) => Some(crate::security::ServerTls::load(
            &cert,
            &key,
            tls_client_ca.as_deref(),
        )?),
        (None, None) => {
            if tls_client_ca.is_some() {
                return Err("--tls-client-ca needs --tls-cert and --tls-key".to_string());
            }
            None
        }
        _ => return Err("TLS needs both --tls-cert and --tls-key".to_string()),
    };
    let tls_ca = path_opt("tls-ca", "PIPESTREAM_SEARCH_TLS_CA", file.tls_ca.as_deref());
    let tls_client_cert = path_opt(
        "tls-client-cert",
        "PIPESTREAM_SEARCH_TLS_CLIENT_CERT",
        file.tls_client_cert.as_deref(),
    );
    let tls_client_key = path_opt(
        "tls-client-key",
        "PIPESTREAM_SEARCH_TLS_CLIENT_KEY",
        file.tls_client_key.as_deref(),
    );
    let tls_domain = opt(
        args,
        "tls-domain",
        "PIPESTREAM_SEARCH_TLS_DOMAIN",
        file.tls_domain.as_deref(),
    );
    let client_tls = match tls_ca {
        Some(ca) => Some(crate::security::ClientTls::load(
            &ca,
            tls_client_cert.as_deref(),
            tls_client_key.as_deref(),
            tls_domain,
        )?),
        None => {
            if tls_client_cert.is_some() || tls_client_key.is_some() || tls_domain.is_some() {
                return Err(
                    "--tls-client-cert / --tls-client-key / --tls-domain need --tls-ca".to_string(),
                );
            }
            None
        }
    };
    let allow_plaintext = flag_present(args, "allow-plaintext")
        || std::env::var("PIPESTREAM_SEARCH_ALLOW_PLAINTEXT")
            .map(|s| parse_env_bool(&s))
            .unwrap_or(false)
        || file.allow_plaintext.unwrap_or(false);
    if tls.is_some() && allow_plaintext {
        return Err(
            "--allow-plaintext contradicts --tls-cert: once TLS is set, plaintext is refused"
                .to_string(),
        );
    }
    if tls.is_none() && !allow_plaintext {
        let mut listeners: Vec<SocketAddr> = shards.iter().map(|s| s.listen).collect();
        if matches!(role, Role::Coordinator | Role::Both) {
            listeners.push(coord_listen);
        }
        if let Some(addr) = listeners
            .iter()
            .find(|addr| !crate::security::is_loopback(addr))
        {
            return Err(format!(
                "listener {addr} is not loopback and no TLS is configured; pass --tls-cert and \
                 --tls-key, or --allow-plaintext to serve plaintext gRPC there on purpose"
            ));
        }
    }
    if matches!(role, Role::Node | Role::Both)
        && tls.as_ref().is_some_and(|t| t.client_ca_pem.is_none())
    {
        return Err(
            "node listeners run mTLS: --tls-client-ca (the cluster CA) is required with --tls-cert"
                .to_string(),
        );
    }
    if matches!(role, Role::Coordinator | Role::Both) && tls.is_some() && client_tls.is_none() {
        return Err(
            "a TLS coordinator needs --tls-ca (and --tls-client-cert / --tls-client-key, its \
             membership) to reach its nodes"
                .to_string(),
        );
    }
    let principals = path_opt(
        "bearer-tokens",
        "PIPESTREAM_SEARCH_BEARER_TOKENS",
        file.bearer_tokens.as_deref(),
    )
    .map(|path| crate::security::Principals::load(&path).map(std::sync::Arc::new))
    .transpose()?;
    let udp_hmac_key = path_opt(
        "udp-hmac-key",
        "PIPESTREAM_SEARCH_UDP_HMAC_KEY",
        file.udp_hmac_key.as_deref(),
    )
    .map(|path| crate::security::UdpKey::load(&path))
    .transpose()?;
    let vector_mmap =
        match opt(args, "vector-mmap", "PIPESTREAM_SEARCH_VECTOR_MMAP", None).as_deref() {
            None => file.vector_mmap.unwrap_or(true),
            Some("true") => true,
            Some("false") => false,
            Some(other) => {
                return Err(format!(
                    "--vector-mmap={other:?} is not a boolean; use true (the default) or false"
                ))
            }
        };
    let layout = match opt(
        args,
        "layout",
        "PIPESTREAM_SEARCH_LAYOUT",
        file.layout.as_deref(),
    )
    .as_deref()
    {
        None | Some("segments") => crate::node::Layout::Segments,
        Some("single-image") => crate::node::Layout::SingleImage,
        Some(other) => {
            return Err(format!(
                "--layout={other:?} is not a layout; use segments (the default) or single-image"
            ))
        }
    };
    let seal_tail_docs = opt(
        args,
        "seal-tail-docs",
        "PIPESTREAM_SEARCH_SEAL_TAIL_DOCS",
        file.seal_tail_docs.map(|v| v.to_string()).as_deref(),
    )
    .map(|v| {
        v.parse::<u32>()
            .map_err(|e| format!("invalid --seal-tail-docs: {e}"))
    })
    .transpose()?
    .unwrap_or(500_000);
    let default_collection = file.default_collection.clone();
    if let Some(name) = &default_collection {
        if !collections.iter().any(|c| &c.name == name) {
            return Err(format!(
                "default_collection {name:?} is not a declared collection ({:?})",
                collections
                    .iter()
                    .map(|c| c.name.as_str())
                    .collect::<Vec<_>>()
            ));
        }
    }
    if !collections.is_empty() && matches!(role, Role::Node | Role::Both) {
        for (i, shard) in shards.iter().enumerate() {
            if !collections.iter().any(|c| c.name == shard.collection) {
                return Err(format!(
                    "shards[{i}] serves collection {:?}, which this process's `collections` \
                     does not declare ({:?})",
                    shard.collection,
                    collections
                        .iter()
                        .map(|c| c.name.as_str())
                        .collect::<Vec<_>>()
                ));
            }
        }
    }

    Ok(Config {
        role,
        coord_listen,
        metrics_listen,
        node_addrs,
        clustered_turbovec,
        shards,
        chunk_blocks,
        share_floors,
        block_max,
        segment_pruning,
        shard_pruning,
        placement_column,
        placement_leaf,
        allow_missing_bm25,
        coalesce,
        scan_parallel,
        rerank_parallel,
        floor_delta,
        floor_warmup_chunks,
        floor_min_interval_ms,
        shard_deadline_ms,
        hedge_delay_ms,
        replica_addrs,
        max_message_bytes,
        demo_query,
        stream_search,
        relay,
        bm25_stream,
        max_k,
        max_rerank_bytes,
        dense_quality_profile,
        synonyms,
        dense_execution_policy,
        query_dim,
        bit_width,
        save_on_shutdown,
        analysis_addr,
        bm25_k1,
        bm25_b,
        bm25_fields,
        facet_fields,
        numeric_fields,
        map_facet_fields,
        phrase_glossary,
        phrase_field,
        entity_map_field,
        phrase_ignore_case,
        phrase_ner,
        position_fields,
        bigram_fields,
        sentence_fields,
        map_numeric_fields,
        integer_fields,
        geo_fields,
        shard_map,
        shard_map_path,
        shard_map_reload_ms,
        replica_sync_ms,
        replica_state_path,
        control_state_path,
        control_reconcile_ms,
        control_lease_ms,
        control_replication_factor,
        control_split_rows,
        control_merge_rows,
        control_compact_segments,
        control_compact_tombstone_ppm,
        membership,
        collections,
        default_collection,
        tls,
        client_tls,
        allow_plaintext,
        principals,
        udp_hmac_key,
        layout,
        seal_tail_docs,
        vector_mmap,
        vocab_window_docs,
        vocab_top_k,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The arguments of one test plus `--allow-plaintext`: these tests
    /// bind the non-loopback defaults and are about other knobs, and
    /// plaintext off loopback needs the explicit flag (docs/security.md).
    fn args(pairs: &[&str]) -> Vec<String> {
        let mut v = args_raw(pairs);
        if !pairs
            .iter()
            .any(|p| p.starts_with("--tls-") || *p == "--allow-plaintext")
        {
            v.push("--allow-plaintext".to_string());
        }
        v
    }

    fn args_raw(pairs: &[&str]) -> Vec<String> {
        pairs.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn allow_missing_bm25_is_off_unless_asked_for() {
        // The default has to be the strict one: a shard silently serving
        // no postings is the failure this exists to catch, and a default
        // of "permit" would mean the check never fires where it matters.
        let base = [
            "--role=node",
            "--demo-vectors=10",
            "--node-listen=127.0.0.1:9001",
        ];
        assert!(!parse(&args(&base)).unwrap().allow_missing_bm25);
        let mut bare = base.to_vec();
        bare.push("--allow-missing-bm25");
        assert!(
            parse(&args(&bare)).unwrap().allow_missing_bm25,
            "the bare flag must work; an operator will not write =true"
        );
        let mut valued = base.to_vec();
        valued.push("--allow-missing-bm25=true");
        assert!(parse(&args(&valued)).unwrap().allow_missing_bm25);
        let mut off = base.to_vec();
        off.push("--allow-missing-bm25=false");
        assert!(!parse(&args(&off)).unwrap().allow_missing_bm25);
    }

    #[test]
    fn bm25_fields_default_body_and_validation() {
        let base = [
            "--role=node",
            "--demo-vectors=10",
            "--node-listen=127.0.0.1:9001",
        ];
        let cfg = parse(&args(&base)).unwrap();
        assert_eq!(cfg.bm25_fields, vec!["body".to_string()]);

        let mut with = base.to_vec();
        with.push("--bm25-fields=body, case_name");
        let cfg = parse(&args(&with)).unwrap();
        assert_eq!(
            cfg.bm25_fields,
            vec!["body".to_string(), "case_name".to_string()]
        );

        let mut bad = base.to_vec();
        bad.push("--bm25-fields=case_name,body");
        assert!(parse(&args(&bad)).is_err(), "body must come first");
        let mut dup = base.to_vec();
        dup.push("--bm25-fields=body,case_name,case_name");
        assert!(parse(&args(&dup)).is_err(), "duplicates are refused");
    }

    #[test]
    fn parses_node_role_flags() {
        let cfg = parse(&args(&[
            "--role=node",
            "--demo-vectors=1000",
            "--node-listen=127.0.0.1:9001",
            "--slot-offset=20000",
            "--chunk-blocks=8",
        ]))
        .unwrap();
        assert_eq!(cfg.role, Role::Node);
        assert_eq!(cfg.shards.len(), 1);
        assert_eq!(cfg.shards[0].listen.port(), 9001);
        assert_eq!(cfg.shards[0].slot_offset, 20000);
        assert_eq!(cfg.chunk_blocks, 8);
        assert_eq!(cfg.shards[0].demo.unwrap().vectors, 1000);
        assert_eq!(
            cfg.shards[0].vector_backend,
            crate::vector::EMBEDDED_TURBOVEC
        );
        assert!(cfg.share_floors);
        assert_eq!(cfg.max_message_bytes, MAX_MESSAGE_BYTES);
    }

    #[test]
    fn native_analysis_backend_is_not_rewritten_as_http() {
        let cfg = parse(&args(&[
            "--role=both",
            "--demo-vectors=10",
            "--nodes=127.0.0.1:9001",
            "--node-listen=127.0.0.1:9001",
            "--analysis-addr=native://",
        ]))
        .unwrap();
        assert_eq!(cfg.analysis_addr.as_deref(), Some("native"));
        assert_eq!(cfg.shards[0].analysis_addr.as_deref(), Some("native"));
    }

    #[test]
    fn coordinator_requires_nodes() {
        assert!(parse(&args(&["--role=coordinator"])).is_err());
        let cfg = parse(&args(&[
            "--role=coordinator",
            "--nodes=127.0.0.1:50051,127.0.0.1:50052",
        ]))
        .unwrap();
        assert_eq!(cfg.node_addrs.len(), 2);
        assert!(cfg.node_addrs[0].starts_with("http://"));
    }

    #[test]
    fn clustered_turbovec_transports_are_explicit_and_exclusive() {
        let external = parse(&args(&[
            "--role=coordinator",
            "--nodes=127.0.0.1:50051",
            "--turbovec-coordinator=127.0.0.1:51050",
        ]))
        .unwrap();
        assert_eq!(
            external.clustered_turbovec,
            Some(ClusteredTurboVecConfig::External {
                endpoint: "http://127.0.0.1:51050".to_string(),
            })
        );

        let missing_state = parse(&args(&[
            "--role=coordinator",
            "--nodes=127.0.0.1:50051",
            "--turbovec-cluster-nodes=127.0.0.1:52051 shard-a 7",
        ]));
        assert!(missing_state
            .unwrap_err()
            .contains("requires turbovec_cluster_state"));

        let embedded = parse(&args(&[
            "--role=coordinator",
            "--nodes=127.0.0.1:50051",
            "--turbovec-cluster-nodes=127.0.0.1:52051 shard-a 7",
            "--allow-ephemeral-turbovec-cluster",
        ]))
        .unwrap();
        assert_eq!(
            embedded.clustered_turbovec,
            Some(ClusteredTurboVecConfig::InProcess {
                nodes: vec!["127.0.0.1:52051 shard-a 7".to_string()],
                state: None,
                allow_ephemeral: true,
            })
        );

        let both = parse(&args(&[
            "--role=coordinator",
            "--nodes=127.0.0.1:50051",
            "--turbovec-coordinator=127.0.0.1:51050",
            "--turbovec-cluster-nodes=127.0.0.1:52051",
        ]));
        assert!(both.unwrap_err().contains("not both"));
    }

    #[test]
    fn vector_backend_is_provider_neutral_and_can_vary_per_shard() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("target/tmp");
        let path = dir.join(format!(
            "pipestream_search_backends_{}.toml",
            std::process::id()
        ));
        std::fs::write(
            &path,
            r#"
role = "node"
vector_backend = "default-provider"

[[shards]]
index = "/data/a.vector"

[[shards]]
index = "/data/b.vector"
vector_backend = "shard-provider"
"#,
        )
        .unwrap();
        let cfg = parse(&args(&[&format!("--config={}", path.display())])).unwrap();
        let _ = std::fs::remove_file(path);
        assert_eq!(cfg.shards[0].vector_backend, "default-provider");
        assert_eq!(cfg.shards[1].vector_backend, "shard-provider");

        let cli = parse(&args(&[
            "--role=node",
            "--index=/data/c.vector",
            "--vector-backend=cli-provider",
        ]))
        .unwrap();
        assert_eq!(cli.shards[0].vector_backend, "cli-provider");
    }

    #[test]
    fn node_requires_a_shard() {
        assert!(parse(&args(&["--role=node"])).is_err());
        assert!(parse(&args(&[
            "--role=node",
            "--index=/tmp/x.tv",
            "--demo-vectors=10"
        ]))
        .is_err());
    }

    #[test]
    fn toml_file_multi_shard() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("target/tmp");
        let path = dir.join(format!("pipestream_search_cfg_{}.toml", std::process::id()));
        std::fs::write(
            &path,
            r#"
role = "both"
coord_listen = "0.0.0.0:51050"
nodes = ["host-a:50051", "host-b:50052"]
chunk_blocks = 16
floor_sharing = false
max_message_mib = 32

[[shards]]
listen = "0.0.0.0:50051"
index = "/data/shard-0.tv"
slot_offset = 0

[[shards]]
listen = "0.0.0.0:50052"
index = "/data/shard-1.tv"
slot_offset = 20000
"#,
        )
        .unwrap();
        let cfg = parse(&args(&[&format!("--config={}", path.display())])).unwrap();
        let _ = std::fs::remove_file(&path);

        assert_eq!(cfg.role, Role::Both);
        assert_eq!(cfg.coord_listen.port(), 51050);
        assert_eq!(
            cfg.node_addrs,
            vec!["http://host-a:50051", "http://host-b:50052"]
        );
        assert_eq!(cfg.shards.len(), 2);
        assert_eq!(cfg.shards[1].listen.port(), 50052);
        assert_eq!(cfg.shards[1].slot_offset, 20000);
        assert_eq!(
            cfg.shards[1].index_path.as_deref(),
            Some(std::path::Path::new("/data/shard-1.tv"))
        );
        assert_eq!(cfg.chunk_blocks, 16);
        assert!(!cfg.share_floors);
        assert_eq!(cfg.max_message_bytes, 32 * 1024 * 1024);
    }

    #[test]
    fn cli_overrides_file() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("target/tmp");
        let path = dir.join(format!("pipestream_search_ovr_{}.toml", std::process::id()));
        std::fs::write(
            &path,
            "role = \"node\"\nchunk_blocks = 16\n\n[[shards]]\nindex = \"/data/a.tv\"\n",
        )
        .unwrap();
        let cfg = parse(&args(&[
            &format!("--config={}", path.display()),
            "--chunk-blocks=99",
        ]))
        .unwrap();
        let _ = std::fs::remove_file(&path);
        assert_eq!(cfg.chunk_blocks, 99);
        assert_eq!(cfg.shards.len(), 1);
    }

    #[test]
    fn file_shard_needs_exactly_one_source() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("target/tmp");
        let path = dir.join(format!("pipestream_search_bad_{}.toml", std::process::id()));
        std::fs::write(&path, "role = \"node\"\n\n[[shards]]\nslot_offset = 7\n").unwrap();
        let result = parse(&args(&[&format!("--config={}", path.display())]));
        let _ = std::fs::remove_file(&path);
        assert!(result.is_err());
    }

    #[test]
    fn floor_sharing_flag() {
        let cfg = parse(&args(&[
            "--role=node",
            "--demo-vectors=10",
            "--floor-sharing=false",
        ]))
        .unwrap();
        assert!(!cfg.share_floors);
    }

    #[test]
    fn bm25_candidate_stream_defaults_on_and_can_be_disabled() {
        let defaults = parse(&args(&["--role=coordinator", "--nodes=a:1"])).unwrap();
        assert!(defaults.bm25_stream);

        let unary = parse(&args(&[
            "--role=coordinator",
            "--nodes=a:1",
            "--bm25-stream=false",
        ]))
        .unwrap();
        assert!(!unary.bm25_stream);
    }

    #[test]
    fn demo_query_flag_and_dim() {
        let cfg = parse(&args(&[
            "--role=coordinator",
            "--nodes=a:1",
            "--demo-query",
            "--query-dim=256",
        ]))
        .unwrap();
        assert!(cfg.demo_query);
        assert_eq!(cfg.query_dim, 256);
    }

    #[test]
    fn wal_defaults_on_for_index_off_for_demo() {
        let demo = parse(&args(&["--role=node", "--demo-vectors=10"])).unwrap();
        assert!(!demo.shards[0].wal);
        let persisted = parse(&args(&["--role=node", "--index=/tmp/x.tv"])).unwrap();
        assert!(persisted.shards[0].wal);
        let off = parse(&args(&["--role=node", "--index=/tmp/x.tv", "--wal=false"])).unwrap();
        assert!(!off.shards[0].wal);
    }

    #[test]
    fn vocab_defaults_off_and_parses_knobs() {
        let defaults = parse(&args(&["--role=node", "--index=/tmp/x.tv"])).unwrap();
        assert!(!defaults.shards[0].vocab);
        assert_eq!(defaults.vocab_window_docs, 1_000_000);
        assert_eq!(defaults.vocab_top_k, 1024);

        let on = parse(&args(&[
            "--role=node",
            "--index=/tmp/x.tv",
            "--vocab=true",
            "--vocab-window-docs=500000",
            "--vocab-top-k=256",
        ]))
        .unwrap();
        assert!(on.shards[0].vocab);
        assert_eq!(on.vocab_window_docs, 500_000);
        assert_eq!(on.vocab_top_k, 256);

        // Demo shards have no index path to snapshot next to.
        let demo = parse(&args(&["--role=node", "--demo-vectors=10", "--vocab=true"])).unwrap();
        assert!(!demo.shards[0].vocab);

        assert!(parse(&args(&[
            "--role=node",
            "--index=/tmp/x.tv",
            "--vocab-window-docs=0"
        ]))
        .is_err());
        assert!(parse(&args(&[
            "--role=node",
            "--index=/tmp/x.tv",
            "--vocab-top-k=0"
        ]))
        .is_err());
    }

    #[test]
    fn maturity_knobs_parse_with_safe_defaults() {
        let defaults = parse(&args(&["--role=node", "--demo-vectors=10"])).unwrap();
        assert_eq!(defaults.floor_delta, 0.0);
        assert_eq!(defaults.shard_deadline_ms, 0);
        assert_eq!(defaults.hedge_delay_ms, 0);
        assert!(defaults.replica_addrs.is_empty());
        assert_eq!(defaults.rerank_parallel, 0);
        assert_eq!(
            defaults.max_rerank_bytes,
            crate::coordinator::DEFAULT_MAX_RERANK_BYTES
        );

        let cfg = parse(&args(&[
            "--role=node",
            "--demo-vectors=10",
            "--floor-delta=0.005",
            "--shard-deadline-ms=1500",
            "--hedge-delay-ms=200",
            "--rerank-parallel=8",
            "--max-rerank-mib=32",
        ]))
        .unwrap();
        assert_eq!(cfg.floor_delta, 0.005);
        assert_eq!(cfg.shard_deadline_ms, 1500);
        assert_eq!(cfg.hedge_delay_ms, 200);
        assert_eq!(cfg.rerank_parallel, 8);
        assert_eq!(cfg.max_rerank_bytes, 32 * 1024 * 1024);

        // Negative or non-finite deltas are rejected.
        assert!(parse(&args(&[
            "--role=node",
            "--demo-vectors=10",
            "--floor-delta=-0.1"
        ]))
        .is_err());
        assert!(parse(&args(&[
            "--role=node",
            "--demo-vectors=10",
            "--rerank-parallel=65"
        ]))
        .is_err());
    }

    #[test]
    fn membership_flags_need_each_other_and_shards_carry_their_identity() {
        let cfg = parse(&args(&[
            "--role=node",
            "--index=/tmp/x.tv",
            "--node-id=b",
            "--control-addr=10.0.0.1:50050",
            "--failure-domain=rack-2",
            "--data-dir=/var/lib/placed",
            "--advertise-addr=10.0.0.2:50051",
            "--replica-listen=10.0.0.2:0",
            "--node-report-ms=500",
            "--shard-id=s7",
            "--hash-lo=0",
            "--hash-hi=100",
        ]))
        .unwrap();
        let membership = cfg.membership.as_ref().unwrap();
        assert_eq!(membership.node_id, "b");
        assert_eq!(membership.control_addr, "http://10.0.0.1:50050");
        assert_eq!(membership.failure_domain, "rack-2");
        assert_eq!(membership.data_dir, PathBuf::from("/var/lib/placed"));
        assert_eq!(membership.advertise_addr.as_deref(), Some("10.0.0.2:50051"));
        assert_eq!(
            membership.replica_listen.map(|a| a.to_string()),
            Some("10.0.0.2:0".to_string())
        );
        assert_eq!(
            (membership.report_ms, membership.reconcile_ms),
            (500, 2_000)
        );
        assert_eq!((membership.lease_ms, membership.lag_bound), (0, 0));
        assert_eq!(cfg.shards[0].shard_id.as_deref(), Some("s7"));
        assert_eq!(cfg.shards[0].hash_range, Some((0, 100)));
        let plain = parse(&args(&["--role=node", "--index=/tmp/x.tv"])).unwrap();
        assert!(plain.membership.is_none());
        assert!(plain.shards[0].shard_id.is_none() && plain.shards[0].hash_range.is_none());
        for (flags, needle) in [
            (
                vec!["--role=node", "--index=/tmp/x.tv", "--node-id=b"],
                "needs --control-addr",
            ),
            (
                vec![
                    "--role=node",
                    "--index=/tmp/x.tv",
                    "--node-id=b",
                    "--control-addr=c:1",
                ],
                "needs --data-dir",
            ),
            (
                vec!["--role=node", "--index=/tmp/x.tv", "--data-dir=/d"],
                "pass --node-id",
            ),
            (
                vec![
                    "--role=coordinator",
                    "--nodes=a:1",
                    "--node-id=b",
                    "--control-addr=c:1",
                    "--data-dir=/d",
                ],
                "node or both role",
            ),
            (
                vec![
                    "--role=node",
                    "--index=/tmp/x.tv",
                    "--node-id=b",
                    "--control-addr=c:1",
                    "--data-dir=/d",
                    "--advertise-addr=nowhere",
                ],
                "is not host:port",
            ),
            (
                vec!["--role=node", "--index=/tmp/x.tv", "--hash-lo=5"],
                "both bounds or neither",
            ),
            (
                vec![
                    "--role=node",
                    "--index=/tmp/x.tv",
                    "--hash-lo=5",
                    "--hash-hi=4",
                ],
                "is inverted",
            ),
        ] {
            let error = parse(&args(&flags)).unwrap_err();
            assert!(error.contains(needle), "{flags:?}: {error}");
        }
    }

    #[test]
    fn durable_control_requires_a_coordinator_map_and_parses_policy() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("target/tmp");
        std::fs::create_dir_all(&dir).unwrap();
        let map = dir.join(format!("control-map-{}.toml", std::process::id()));
        let state = dir.join(format!("control-state-{}.json", std::process::id()));
        std::fs::write(
            &map,
            "generation = 4\n\n[[shards]]\naddr = \"node:50051\"\nslot_offset = 0\n",
        )
        .unwrap();
        let cfg = parse(&args(&[
            "--role=coordinator",
            &format!("--shard-map={}", map.display()),
            &format!("--control-state={}", state.display()),
            "--control-reconcile-ms=250",
            "--control-lease-ms=5000",
            "--control-replication-factor=3",
            "--control-split-rows=1000",
            "--control-merge-rows=100",
            "--control-compact-segments=6",
            "--control-compact-tombstone-ppm=50000",
        ]))
        .unwrap();
        assert_eq!(cfg.control_state_path.as_deref(), Some(state.as_path()));
        assert_eq!(cfg.control_reconcile_ms, 250);
        assert_eq!(cfg.control_lease_ms, 5_000);
        assert_eq!(cfg.control_replication_factor, 3);
        assert_eq!(cfg.control_split_rows, 1_000);
        assert_eq!(cfg.control_merge_rows, 100);
        assert_eq!(cfg.control_compact_segments, 6);
        assert_eq!(cfg.control_compact_tombstone_ppm, 50_000);
        assert!(parse(&args(&[
            "--role=coordinator",
            "--nodes=node:50051",
            &format!("--control-state={}", state.display()),
        ]))
        .is_err());
        assert!(parse(&args(&[
            "--role=coordinator",
            &format!("--shard-map={}", map.display()),
            &format!("--control-state={}", state.display()),
            "--control-compact-segments=4294967296",
        ]))
        .is_err());
        std::fs::remove_file(map).unwrap();
    }

    #[test]
    fn shard_map_parses_replicas() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("target/tmp");
        std::fs::create_dir_all(&dir).ok();
        let path = dir.join(format!("turbovec_replicas_{}.toml", std::process::id()));
        std::fs::write(
            &path,
            r#"
[[shards]]
addr = "host-a:50051"
replica = "host-a2:50051"

[[shards]]
addr = "host-b:50051"
"#,
        )
        .unwrap();
        let cfg = parse(&args(&[
            "--role=coordinator",
            &format!("--shard-map={}", path.display()),
        ]))
        .unwrap();
        let _ = std::fs::remove_file(&path);
        assert_eq!(
            cfg.replica_addrs,
            vec![Some("http://host-a2:50051".to_string()), None]
        );
    }

    #[test]
    fn shard_map_replaces_nodes() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("target/tmp");
        std::fs::create_dir_all(&dir).ok();
        let path = dir.join(format!("turbovec_shardmap_{}.toml", std::process::id()));
        std::fs::write(
            &path,
            r#"
generation = 7

[[shards]]
addr = "host-a:50051"
slot_offset = 0
hash_lo = 0
hash_hi = 9223372036854775807

[[shards]]
addr = "host-b:50051"
slot_offset = 25000000
"#,
        )
        .unwrap();
        let cfg = parse(&args(&[
            "--role=coordinator",
            &format!("--shard-map={}", path.display()),
        ]))
        .unwrap();
        let map = cfg.shard_map.expect("shard map parsed");
        assert_eq!(map.generation, 7);
        assert_eq!(map.shards.len(), 2);
        assert_eq!(map.shards[1].slot_offset, 25_000_000);
        assert_eq!(map.shards[1].hash_lo, None);
        assert_eq!(
            cfg.node_addrs,
            vec!["http://host-a:50051", "http://host-b:50051"]
        );
        // Both --nodes and --shard-map is an error.
        assert!(parse(&args(&[
            "--role=coordinator",
            &format!("--shard-map={}", path.display()),
            "--nodes=a:1",
        ]))
        .is_err());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn phrase_configuration_requires_explicit_storage_fields() {
        let base = [
            "--role=node",
            "--demo-vectors=10",
            "--node-listen=127.0.0.1:9001",
            "--phrase-glossary=/tmp/concepts.tsv",
        ];
        assert!(parse(&args(&base)).unwrap_err().contains("phrase field"));

        let mut configured = base.to_vec();
        configured.extend([
            "--bm25-fields=body,phrases",
            "--map-facet-fields=entities",
            "--entity-map-field=entities",
            "--phrase-ner",
        ]);
        let cfg = parse(&args(&configured)).unwrap();
        assert_eq!(cfg.phrase_field, "phrases");
        assert_eq!(cfg.entity_map_field.as_deref(), Some("entities"));
        assert!(cfg.phrase_ignore_case);
        assert!(cfg.phrase_ner);
    }
}

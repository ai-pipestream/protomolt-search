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

mod args;
mod columns;
mod dataset;
mod file;
mod membership;
mod option;
#[cfg(test)]
mod option_tests;
mod parse;
mod raft;
mod scan;
mod shard_map;
mod shards;
#[cfg(test)]
mod tests;
mod tls;
mod topology;
mod turbo;
mod validate;

pub use self::args::normalize_addr;
pub use self::parse::parse;
pub use self::raft::{
    parse_raft_local, RaftLocal, RaftManagedCatalog, RaftMapConfig, RaftMemberConfig,
};
pub use self::shard_map::{load_placement_tree, load_shard_map, ShardMap, ShardMapShard};

use std::net::SocketAddr;
use std::path::PathBuf;

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
    /// Node: the pinned leaf with its tree (`--placement-tree=<file>`,
    /// the coordinator's shard map or a file holding just the
    /// `[placement]` table). The shard then refuses a direct row the
    /// tree routes to another leaf. Needs `--placement-leaf`.
    pub placement_tree: Option<std::sync::Arc<crate::placement::PinnedLeaf>>,
    /// Node: the index's derived-column declaration
    /// (`--derived-columns=<file>` or an inline `[[derived]]` table,
    /// docs/derived-columns.md), compiled; the node checks it against
    /// its column tables and pins it to the store, the log and every
    /// segment.
    pub derived: Option<std::sync::Arc<crate::derived::Declaration>>,
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
    /// Coordinator: default top-`k` stillness window (µs) before sending
    /// `FinishStreamSearch` to unfinished ANN shards.
    pub stream_finish_settle_us: Option<u32>,
    /// Coordinator: default streaming deadline (µs) before sending
    /// `FinishStreamSearch { deadline: true }` to unfinished ANN shards.
    pub stream_finish_deadline_us: Option<u32>,
    /// Node: optional per-poll sleep (µs) injected on streaming searches for
    /// straggler fault injection in benchmarks and tests.
    pub stream_poll_delay_us: Option<u64>,
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
    /// Coordinator: hard cap on any client-facing `k`; node: admission cap
    /// for `ScoreSplade.k`. Requests above it are refused, never clamped.
    /// Omitted coordinator search k selects this depth; SPLADE k=0 still
    /// returns no hits. Must be at least 1.
    pub max_k: u32,
    /// Coordinator: candidate ids per rescore call when a boolean group
    /// scores a clause over its surviving ids (`--signal-batch`,
    /// docs/query-api.md). At least 1.
    pub signal_batch: u32,
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
    pub map_integer_fields: Vec<String>,
    pub map_unsigned_integer_fields: Vec<String>,
    /// The i64 column table for NEW shard builders
    /// (`--integer-fields=citations,filed_at`, docs/range-facets.md):
    /// exact integers past 2^53, and where Timestamp ingest lands as
    /// epoch micros. Same rules; one name space across all kinds.
    pub integer_fields: Vec<String>,
    /// Exact u64 columns (--unsigned-integer-fields), with explicit presence.
    pub unsigned_integer_fields: Vec<String>,
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
    /// This process as a member of the source authority's Raft group
    /// (`--raft-dir` and the other `--raft-*` options).
    pub raft: Option<RaftMemberConfig>,
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
    /// Declarative NLP column mapping for ingest and one-pass query synthesis.
    pub turbo_nlp_columns: Option<crate::turbo_bridge::NlpColumnSpec>,
    /// Enable one-pass query planning on the coordinator.
    pub turbo_one_pass_query: bool,
    /// Remote KServe endpoint for Stage-2 cross-encoder reranking.
    pub turbo_rerank_endpoint: Option<String>,
    /// Model name sent to the Stage-2 rerank endpoint.
    pub turbo_rerank_model: Option<String>,
    /// Matryoshka vector prefix dimensions stored alongside dense embeddings.
    pub turbo_matryoshka_dims: Vec<usize>,
    /// Active Matryoshka prefix dimension indexed into the shard vector backend.
    pub turbo_matryoshka_active_dim: Option<usize>,
    /// Remote KServe endpoint receiving live corpus vocabulary exports on flush.
    pub turbo_vocab_endpoint: Option<String>,
    /// Expected SHA-256 model digest verified at startup against remote TurboEmbed sidecars.
    pub turbo_model_sha256: Option<String>,
    /// Outbound bearer token attached to remote sidecar gRPC requests.
    pub turbo_bearer_token: Option<String>,
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

//! Cluster configuration: TOML file + environment variables + CLI flags.
//!
//! Precedence (highest wins): CLI flag, then environment variable, then
//! config file, then built-in default. `--config <path>` selects the file;
//! every other flag is `--key=value` or `--key value`. Each flag's environment
//! variable is `PIPESTREAM_SEARCH_` plus the flag in upper case with `_` for
//! `-` (`--shard-map-reload-ms` reads `PIPESTREAM_SEARCH_SHARD_MAP_RELOAD_MS`).
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
//! metrics_listen = "127.0.0.1:9100"    # optional Prometheus page
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
mod parse;
mod phrase;
mod placement;
mod raft;
mod scan;
mod shard_map;
mod shards;
#[cfg(test)]
mod tests;
mod tls;
mod topology;
mod turbo;
mod types;
mod validate;

pub use self::args::normalize_addr;
pub use self::parse::parse;
pub use self::raft::{
    parse_raft_local, RaftLocal, RaftManagedCatalog, RaftMapConfig, RaftMemberConfig,
};
pub use self::shard_map::{load_placement_tree, load_shard_map, ShardMap, ShardMapShard};
pub use self::types::{
    ClusteredTurboVecConfig, CollectionConfig, DemoConfig, NodeMembershipConfig, ShardConfig,
};

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

/// Full process configuration.
#[derive(Debug, Clone)]
pub struct Config {
    /// Role(s) to serve.
    pub role: Role,
    /// Listen address for `SearchService` (roles coordinator/both).
    pub coord_listen: SocketAddr,
    /// Listen address for the Prometheus metrics page. `None` (the
    /// default) serves no metrics.
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
    /// their column summaries. `false` keeps
    /// every segment in the scan; the answer is the same either way.
    pub segment_pruning: bool,
    /// Coordinator: skip shards a request's filter cannot match, from
    /// the placement leaf each shard serves.
    /// `false` consults every shard; the answer is the same either way.
    pub shard_pruning: bool,
    /// Node: the i64 column each row carries with its placement code
    /// (`--placement-column`). Joins the integer
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
    /// (`--derived-columns=<file>` or an inline `[[derived]]` table),
    /// compiled; the node checks it against
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
    /// out (0 = publish from the first chunk).
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
    /// Coordinator: serve as a RELAY: the
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
    /// scores a clause over its surviving ids (`--signal-batch`).
    /// At least 1.
    pub signal_batch: u32,
    /// Coordinator-wide logical FP32 payload bound for one rerank request.
    pub max_rerank_bytes: u64,
    /// Optional generation-bound measured candidate-depth profile.
    pub dense_quality_profile: Option<PathBuf>,
    /// Optional synonym table, a TOML file.
    pub synonyms: Option<PathBuf>,
    /// Optional generation-bound dense execution policy for AUTO.
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
    /// The BM25 field table for NEW shard builders:
    /// "body" first, then the extra indexed
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
    /// score-function chains. Same rules as
    /// `facet_fields`; names must not collide with them (the v7 column
    /// table holds both and refuses duplicates).
    pub numeric_fields: Vec<String>,
    /// The map<string, string> column table for NEW shard builders
    /// (`--map-facet-fields=meta`). Same rules;
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
    /// (`--position-fields=body`): the opt-in
    /// payload behind exact phrase and proximity queries. Each name must
    /// be in `bm25_fields`. Shards loaded from existing files keep the
    /// declaration they were written with; ingest refuses a positional
    /// field the file never declared.
    pub position_fields: Vec<String>,
    /// Source fields whose adjacent-token pairs are derived into a
    /// bigram column (`--bigram-fields=body`).
    /// The column is the BM25 field named `<source>.bigrams`, which must
    /// be declared in `bm25_fields`; clients never supply it.
    pub bigram_fields: Vec<String>,
    /// Fields whose sentence spans are stored per document for
    /// server-side snippets (`--sentence-fields=body`).
    /// Snippets are cut from stored text, and
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
    /// (`--integer-fields=citations,filed_at`):
    /// exact integers past 2^53, and where Timestamp ingest lands as
    /// epoch micros. Same rules; one name space across all kinds.
    pub integer_fields: Vec<String>,
    /// Exact u64 columns (--unsigned-integer-fields), with explicit presence.
    pub unsigned_integer_fields: Vec<String>,
    /// The geo-point column table for NEW shard builders
    /// (`--geo-fields=courthouse`): the columns
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
    /// Named collections this coordinator serves;
    /// empty means the one unnamed dataset described by `node_addrs` and
    /// the top-level knobs. When present, `node_addrs` is empty and every
    /// per-dataset setting lives on the collection.
    pub collections: Vec<CollectionConfig>,
    /// The collection an unnamed request gets to, when configured; with
    /// named collections and no default, unnamed requests refuse.
    pub default_collection: Option<String>,
    /// Listener TLS: `--tls-cert`, `--tls-key`, and
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
    /// The layout a NEW persisted shard gets (`--layout=segments|single-image`);
    /// an existing shard keeps its own.
    pub layout: crate::node::Layout,
    /// Documents a segmented shard's tail may hold before it seals a
    /// segment on its own (`--seal-tail-docs`); 0 seals on flush only.
    pub seal_tail_docs: u32,
    /// Serve sealed segments' vector images through memory maps
    /// (`--vector-mmap=true|false`); on by default.
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

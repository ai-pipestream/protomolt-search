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

mod parse;
mod parse_tail;
mod raft;
#[cfg(test)]
mod tests;
mod tls;

pub use parse::parse;
use raft::parse_raft;
use tls::parse_tls;

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
    /// The index's derived columns (`docs/derived-columns.md`), the
    /// same declaration every shard runs with; the coordinator
    /// evaluates them to route under the tree and to scope field
    /// permissions.
    #[serde(default)]
    pub derived: Vec<crate::derived::DerivedColumnConfig>,
}

/// Read one complete shard-map candidate. Callers validate its routing
/// geometry before publishing it; a parse failure never changes live state.
pub fn load_shard_map(path: &std::path::Path) -> Result<ShardMap, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("read shard map {}: {e}", path.display()))?;
    toml::from_str(&text).map_err(|e| format!("parse shard map {}: {e}", path.display()))
}

/// Read a placement tree for `--placement-tree`: the coordinator's shard
/// map (its `[placement]` table) or a file holding just that table.
/// A shard map without a tree, and a file that is neither, refuse by
/// name.
pub fn load_placement_tree(
    path: &std::path::Path,
) -> Result<crate::placement::PlacementTreeConfig, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("read placement tree {}: {e}", path.display()))?;
    if let Ok(map) = toml::from_str::<ShardMap>(&text) {
        return map.placement.ok_or_else(|| {
            format!(
                "placement tree {}: the shard map has no [placement] table",
                path.display()
            )
        });
    }
    toml::from_str::<crate::placement::PlacementTreeConfig>(&text).map_err(|e| {
        format!(
            "parse placement tree {}: neither a shard map with a [placement] table nor a \
             placement table on its own: {e}",
            path.display()
        )
    })
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

/// The resource a relay routes on from a Raft-hosted authority's
/// committed map (`--raft-map-principal`, `--raft-map-workspace`,
/// `--raft-map-collection`; docs/raft-hosting.md).
#[derive(Debug, Clone)]
pub struct RaftMapConfig {
    pub principal: String,
    pub workspace: String,
    pub collection: String,
}

/// One access-controlled source catalog this member serves owner writes
/// for (`--raft-managed-catalog=<collection>=<path>`, repeatable): a file
/// bound and activated for the named collection of the member's
/// authority, recovered at start before the service listens.
#[derive(Debug, Clone)]
pub struct RaftManagedCatalog {
    pub collection: String,
    pub path: PathBuf,
}

/// A Raft member of the source authority group (`--raft-*`,
/// docs/raft-hosting.md "Operator configuration"). Serving opens existing
/// state; `raft-prepare` creates it.
#[derive(Debug, Clone)]
pub struct RaftMemberConfig {
    /// The member's directory: store, log and snapshots.
    pub dir: PathBuf,
    pub node_id: u64,
    /// The group identity the store must carry (16 bytes each, given as
    /// 32 hex digits).
    pub group_id: Vec<u8>,
    pub authority_incarnation: Vec<u8>,
    pub listen: SocketAddr,
    /// The address peers dial, when it is not the bound one.
    pub advertise: Option<String>,
    /// The peers file (`[[peers]]` of `node_id` and `certificate`).
    pub peers: PathBuf,
    pub heartbeat_ms: u64,
    pub election_min_ms: u64,
    pub election_max_ms: u64,
    pub lease_ms: u64,
    pub skew_ms: u64,
    pub max_snapshot_bytes: u64,
    pub map: Option<RaftMapConfig>,
    /// Managed catalogs served through the hosted write service, with the
    /// actor holding the Admin that recovers them at start
    /// (`--raft-managed-principal`).
    pub managed_catalogs: Vec<RaftManagedCatalog>,
    pub managed_principal: Option<String>,
    /// The hosted write service's limits (`--raft-managed-max-request-bytes`,
    /// `--raft-managed-max-pending-bytes`, `--raft-managed-max-in-flight`):
    /// one write's bound, the admitted bytes in flight, the operations in
    /// flight. Validated where the service is built, by name.
    pub managed_max_request_bytes: u64,
    pub managed_max_pending_bytes: u64,
    pub managed_max_in_flight: u64,
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
    raft_dir: Option<String>,
    raft_node_id: Option<u64>,
    raft_group_id: Option<String>,
    raft_authority_incarnation: Option<String>,
    raft_listen: Option<String>,
    raft_advertise: Option<String>,
    raft_peers: Option<String>,
    raft_heartbeat_ms: Option<u64>,
    raft_election_min_ms: Option<u64>,
    raft_election_max_ms: Option<u64>,
    raft_lease_ms: Option<u64>,
    raft_skew_ms: Option<u64>,
    raft_max_snapshot_bytes: Option<u64>,
    raft_map_principal: Option<String>,
    raft_map_workspace: Option<String>,
    raft_map_collection: Option<String>,
    raft_managed_catalog: Option<Vec<String>>,
    raft_managed_principal: Option<String>,
    raft_managed_max_request_bytes: Option<u64>,
    raft_managed_max_pending_bytes: Option<u64>,
    raft_managed_max_in_flight: Option<u64>,
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
    placement_tree: Option<String>,
    /// `--derived-columns=<file>`: a shard map or a file with a
    /// `[[derived]]` table.
    derived_columns: Option<String>,
    /// The declaration inline (`[[derived]]` in this file).
    derived: Vec<crate::derived::DerivedColumnConfig>,
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
    signal_batch: Option<u32>,
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
    map_integer_fields: Option<Vec<String>>,
    map_unsigned_integer_fields: Option<Vec<String>>,
    integer_fields: Option<Vec<String>>,
    unsigned_integer_fields: Option<Vec<String>>,
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

/// Every value of a repeatable `--key=value` option, in both the `=` and
/// the space-separated form. Hand-rolled exact matching, no patterns.
fn arg_values(args: &[String], key: &str) -> Vec<String> {
    let prefix = format!("--{key}=");
    let mut out: Vec<String> = args
        .iter()
        .filter_map(|a| a.strip_prefix(&prefix).map(str::to_string))
        .collect();
    let flag = format!("--{key}");
    let mut i = 0;
    while i + 1 < args.len() {
        if args[i] == flag && !args[i + 1].starts_with("--") {
            out.push(args[i + 1].clone());
        }
        i += 1;
    }
    out
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
/// The config file at the bottom of the precedence stack, shared by
/// the serving parse and the local directory commands.
fn read_file_config(args: &[String]) -> Result<FileConfig, String> {
    match opt(args, "config", "TURBOVEC_CONFIG", None) {
        Some(path) => {
            let text =
                std::fs::read_to_string(&path).map_err(|e| format!("read config {path}: {e}"))?;
            toml::from_str(&text).map_err(|e| format!("parse config {path}: {e}"))
        }
        None => Ok(FileConfig::default()),
    }
}

/// The member and TLS material for the local directory commands
/// (`raft-bootstrap`): the raft flags and the listener and client TLS,
/// without the serving roles' shard and collection requirements, which
/// a command that creates a directory but serves nothing does not have.
pub struct RaftLocal {
    pub member: Option<RaftMemberConfig>,
    pub tls: Option<crate::security::ServerTls>,
    pub client_tls: Option<crate::security::ClientTls>,
}

pub fn parse_raft_local(args: &[String]) -> Result<RaftLocal, String> {
    let file = read_file_config(args)?;
    let relay = flag_present(args, "relay");
    let member = parse_raft(args, &file, relay)?;
    let (tls, client_tls) = parse_tls(args, &file)?;
    Ok(RaftLocal {
        member,
        tls,
        client_tls,
    })
}

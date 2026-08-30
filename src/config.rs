//! Cluster configuration: TOML file + environment variables + CLI flags.
//!
//! Precedence (highest wins): CLI flag, then environment variable, then
//! config file, then built-in default. `--config <path>` (or
//! `PIPESTREAM_SEARCH_CONFIG`) selects the file; every other flag is
//! `--key=value`. Legacy `TURBOVEC_*` environment names remain fallbacks.
//!
//! Membership is STATIC: the coordinator's node list and each node's shard
//! set are fixed at startup. There is no discovery, re-sharding, or
//! failover — changing the topology means editing the configs and
//! restarting. That is deliberate for this phase.
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

use serde::Deserialize;

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
    /// Analysis sidecar address for AddDocuments on this shard.
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
}

/// One shard entry of a coordinator shard map (`--shard-map`): which node
/// owns which id range, plus the hash range it covers when the map was
/// produced by a hash-partitioned split (see `examples/reshard.rs`).
/// `hash_lo`/`hash_hi` are inclusive and default to the full range.
#[derive(Debug, Clone, Deserialize)]
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
}

/// The id-to-shard authority for one cluster generation
/// (`--shard-map=<file>`). Replaces `--nodes`; bumping `generation` is how
/// a split/merge rollout distinguishes old topology from new.
#[derive(Debug, Clone, Deserialize)]
pub struct ShardMap {
    /// Topology generation (0 for the implicit `--nodes` topology).
    #[serde(default)]
    pub generation: u64,
    /// One entry per shard, in fan-out order (= merge tie-break order).
    pub shards: Vec<ShardMapShard>,
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
    /// Coordinator: run the flat Bm25Search fan-out over the
    /// `Bm25QueryStream` floor relay — shards publish their running
    /// k-th best, the coordinator relays the fleet maximum back, and
    /// block-max converts every raise into blocks never read.
    /// Identical results, less work (docs/block-max.md). Off by
    /// default.
    pub bm25_stream: bool,
    /// Coordinator: hard cap on any client-facing `k`. Requests above it
    /// are refused (never clamped); a request omitting `k` runs at this
    /// depth. Must be at least 1.
    pub max_k: u32,
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
    /// Analysis sidecar address for the coordinator's query analysis
    /// (Bm25Search). Required for BM25 queries.
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
    /// Documents per vocabulary window before automatic rollover (only
    /// relevant to shards with `vocab` enabled).
    pub vocab_window_docs: u64,
    /// Heavy-hitter list size per vocabulary channel.
    pub vocab_top_k: usize,
}

/// Raw TOML file shape; every field optional (file < env < CLI).
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct FileConfig {
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
    allow_missing_bm25: Option<bool>,
    coalesce: Option<bool>,
    scan_parallel: Option<usize>,
    floor_delta: Option<f32>,
    floor_warmup_chunks: Option<u32>,
    floor_min_interval_ms: Option<u64>,
    shard_deadline_ms: Option<u64>,
    hedge_delay_ms: Option<u64>,
    max_message_mib: Option<usize>,
    demo_query: Option<bool>,
    stream_search: Option<bool>,
    bm25_stream: Option<bool>,
    max_k: Option<u32>,
    query_dim: Option<usize>,
    save_on_shutdown: Option<bool>,
    analysis_addr: Option<String>,
    bm25_k1: Option<f32>,
    bm25_b: Option<f32>,
    bm25_fields: Option<Vec<String>>,
    facet_fields: Option<Vec<String>>,
    numeric_fields: Option<Vec<String>>,
    map_facet_fields: Option<Vec<String>>,
    map_numeric_fields: Option<Vec<String>>,
    integer_fields: Option<Vec<String>>,
    geo_fields: Option<Vec<String>>,
    wal: Option<bool>,
    wal_buckets: Option<u32>,
    vocab: Option<bool>,
    vocab_window_docs: Option<u64>,
    vocab_top_k: Option<usize>,
    shard_map: Option<String>,
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
    listen: Option<String>,
    vector_backend: Option<String>,
    index: Option<String>,
    slot_offset: Option<u64>,
    demo_vectors: Option<usize>,
    analysis_addr: Option<String>,
    wal: Option<bool>,
    wal_buckets: Option<u32>,
    vocab: Option<bool>,
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
    let shard_map = match opt(
        args,
        "shard-map",
        "TURBOVEC_SHARD_MAP",
        file.shard_map.as_deref(),
    ) {
        Some(path) => {
            let text = std::fs::read_to_string(&path)
                .map_err(|e| format!("read shard map {path}: {e}"))?;
            let map: ShardMap =
                toml::from_str(&text).map_err(|e| format!("parse shard map {path}: {e}"))?;
            Some(map)
        }
        None => None,
    };
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
                    analysis_addr: shard
                        .analysis_addr
                        .clone()
                        .map(|a| normalize_addrs(vec![a]).remove(0)),
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
    if matches!(role, Role::Coordinator | Role::Both) && node_addrs.is_empty() {
        return Err(
            "coordinator role requires --nodes or --shard-map (or `nodes` in the config file)"
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

    let bm25_stream = flag_present(args, "bm25-stream")
        || std::env::var("PIPESTREAM_SEARCH_BM25_STREAM")
            .or_else(|_| std::env::var("TURBOVEC_BM25_STREAM"))
            .map(|s| parse_env_bool(&s))
            .unwrap_or(false)
        || file.bm25_stream.unwrap_or(false);

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

    let analysis_addr = opt(
        args,
        "analysis-addr",
        "TURBOVEC_ANALYSIS_ADDR",
        file.analysis_addr.as_deref(),
    )
    .map(|a| normalize_addrs(vec![a]).remove(0));
    // A single-shard CLI setup shares the sidecar address with its shard.
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
    {
        let mut all: Vec<&String> = Vec::new();
        for name in facet_fields
            .iter()
            .chain(&numeric_fields)
            .chain(&map_facet_fields)
            .chain(&map_numeric_fields)
            .chain(&integer_fields)
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
        allow_missing_bm25,
        coalesce,
        scan_parallel,
        floor_delta,
        floor_warmup_chunks,
        floor_min_interval_ms,
        shard_deadline_ms,
        hedge_delay_ms,
        replica_addrs,
        max_message_bytes,
        demo_query,
        stream_search,
        bm25_stream,
        max_k,
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
        map_numeric_fields,
        integer_fields,
        geo_fields,
        shard_map,
        vocab_window_docs,
        vocab_top_k,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(pairs: &[&str]) -> Vec<String> {
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

        let cfg = parse(&args(&[
            "--role=node",
            "--demo-vectors=10",
            "--floor-delta=0.005",
            "--shard-deadline-ms=1500",
            "--hedge-delay-ms=200",
        ]))
        .unwrap();
        assert_eq!(cfg.floor_delta, 0.005);
        assert_eq!(cfg.shard_deadline_ms, 1500);
        assert_eq!(cfg.hedge_delay_ms, 200);

        // Negative or non-finite deltas are rejected.
        assert!(parse(&args(&[
            "--role=node",
            "--demo-vectors=10",
            "--floor-delta=-0.1"
        ]))
        .is_err());
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
}

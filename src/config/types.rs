//! The per-shard, per-collection, membership, and vector-backend settings a
//! [`Config`](super::Config) carries.

use std::net::SocketAddr;
use std::path::PathBuf;

use super::ShardMap;

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
    /// snapshotting to `<index path>.vocab/`.
    /// Defaults OFF — zero overhead when off. Requires an index path.
    pub vocab: bool,
    /// The collection this shard serves; empty
    /// for a shard outside any named collection.
    pub collection: String,
    /// The id this shard is reported under to the control plane;
    /// `slot-<offset>` when unset.
    pub shard_id: Option<String>,
    /// The inclusive stable-key hash range the shard covers, when the
    /// configuration names it; else the plane's records or the published
    /// topology supply it.
    pub hash_range: Option<(u64, u64)>,
}

/// Node membership in the control plane (`--node-id` and friends).
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

/// One collection's dataset settings: everything
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

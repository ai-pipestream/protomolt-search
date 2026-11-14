//! Durable lease, placement, and topology authority.
//!
//! Nodes report capacity and immutable shard generations. Reconciliation is
//! deterministic and idempotent: it expires leases, promotes ready replicas,
//! creates copy/drain/rebalance work, and schedules split, merge, or compaction
//! when the configured thresholds are crossed. Data movement stays on the
//! node-to-node WAL/snapshot paths; this module owns decisions, validated
//! action completion, and complete topology publication.

mod checkpoint;
#[cfg(feature = "net")]
pub(crate) mod retirement;
mod storage;
pub use checkpoint::LegacyControlCheckpoint;
#[cfg(feature = "net")]
pub use retirement::RetiredLegacyControl;

mod balance;
mod durable;
mod reconcile;
mod service;
#[cfg(test)]
mod tests;

use balance::plan_balance;
#[cfg(all(feature = "raft", feature = "tls"))]
pub(crate) use service::cluster_membership;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tonic::{Request, Response, Status};

use crate::coordinator::{CoordinatorServiceImpl, TopologyRoute};
use crate::metrics::Route;
use crate::pb::cluster_control_server::{ClusterControl, ClusterControlServer};
use crate::pb::{
    BalanceExclusion, BalanceMove, ClusterNode, ClusterNodeState, ClusterPlan,
    CompletePlacementActionRequest, DrainNodeRequest, GetClusterPlanRequest, NodeCapacity,
    NodeLease, NodeLoad, NodeResidency, PlacementAction, PlacementActionKind, PlanBalanceRequest,
    PlanBalanceResponse, ReconcileClusterRequest, RegisterNodeRequest, RenewNodeLeaseRequest,
    ReportShardRequest, RollbackClusterRequest, ShardReplicaRole, ShardReplicaState,
};

#[derive(Debug, Clone)]
pub struct ControlPolicy {
    pub lease_ms: u64,
    /// Total ready copies, including the primary.
    pub replication_factor: usize,
    pub split_rows: u64,
    pub merge_rows: u64,
    pub compact_segments: u32,
    pub compact_tombstone_ppm: u32,
    pub history_limit: usize,
}

impl Default for ControlPolicy {
    fn default() -> Self {
        Self {
            lease_ms: 15_000,
            replication_factor: 2,
            split_rows: 25_000_000,
            merge_rows: 2_000_000,
            compact_segments: 8,
            compact_tombstone_ppm: 100_000,
            history_limit: 32,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
enum StoredNodeState {
    Active,
    Draining,
    Expired,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct StoredCapacity {
    disk_bytes: u64,
    used_disk_bytes: u64,
    memory_bytes: u64,
    search_threads: u32,
    failure_domain: String,
    /// Absent in state files written before the fields existed.
    #[serde(default)]
    scan_bytes_per_second: u64,
    #[serde(default)]
    scan_rate_observed_unix_ms: u64,
    #[serde(default)]
    scan_rate_samples: u32,
    #[serde(default)]
    scan_rate_window_ms: u64,
    /// `NodeResidency` as its wire number; 0 is unspecified.
    #[serde(default)]
    residency: i32,
}

impl From<NodeCapacity> for StoredCapacity {
    fn from(value: NodeCapacity) -> Self {
        Self {
            disk_bytes: value.disk_bytes,
            used_disk_bytes: value.used_disk_bytes,
            memory_bytes: value.memory_bytes,
            search_threads: value.search_threads,
            failure_domain: value.failure_domain,
            scan_bytes_per_second: value.scan_bytes_per_second,
            scan_rate_observed_unix_ms: value.scan_rate_observed_unix_ms,
            scan_rate_samples: value.scan_rate_samples,
            scan_rate_window_ms: value.scan_rate_window_ms,
            residency: value.residency,
        }
    }
}

impl From<&StoredCapacity> for NodeCapacity {
    fn from(value: &StoredCapacity) -> Self {
        Self {
            disk_bytes: value.disk_bytes,
            used_disk_bytes: value.used_disk_bytes,
            memory_bytes: value.memory_bytes,
            search_threads: value.search_threads,
            failure_domain: value.failure_domain.clone(),
            scan_bytes_per_second: value.scan_bytes_per_second,
            scan_rate_observed_unix_ms: value.scan_rate_observed_unix_ms,
            scan_rate_samples: value.scan_rate_samples,
            scan_rate_window_ms: value.scan_rate_window_ms,
            residency: value.residency,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredNode {
    node_id: String,
    addr: String,
    state: StoredNodeState,
    lease_token: u64,
    expires_unix_ms: u64,
    capacity: StoredCapacity,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
enum StoredRole {
    Primary,
    Replica,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredReplica {
    shard_id: String,
    node_id: String,
    addr: String,
    generation: u64,
    hash_lo: u64,
    hash_hi: u64,
    slot_offset: u64,
    rows: u64,
    bytes: u64,
    role: StoredRole,
    ready: bool,
    scoring_fingerprint: String,
    analysis_fingerprint: String,
    immutable_segments: u32,
    tombstones: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct StoredRoute {
    addr: String,
    replica: Option<String>,
    hash_lo: u64,
    hash_hi: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredTopology {
    generation: u64,
    routes: Vec<StoredRoute>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredAction {
    action_id: u64,
    kind: i32,
    shard_id: String,
    peer_shard_id: String,
    #[serde(default)]
    peer_source_generation: u64,
    source_node_id: String,
    target_node_id: String,
    source_generation: u64,
    target_generation: u64,
    hash_lo: u64,
    hash_hi: u64,
    reason: String,
}

struct ActionSpec<'a> {
    kind: PlacementActionKind,
    peer_shard_id: String,
    peer_source_generation: u64,
    target_node_id: String,
    target_generation: u64,
    reason: &'a str,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredState {
    format: u32,
    /// The collection this plane governs (docs/collections.md); empty in
    /// state written before collections, written on the first open under
    /// a named collection.
    #[serde(default)]
    collection: String,
    revision: u64,
    next_token: u64,
    next_action: u64,
    topology: StoredTopology,
    history: Vec<StoredTopology>,
    #[serde(deserialize_with = "checkpoint::unique_map")]
    nodes: BTreeMap<String, StoredNode>,
    #[serde(deserialize_with = "checkpoint::unique_map")]
    replicas: BTreeMap<String, StoredReplica>,
    actions: Vec<StoredAction>,
    #[serde(default, deserialize_with = "checkpoint::unique_set")]
    completed_actions: BTreeSet<u64>,
}

impl Default for StoredState {
    fn default() -> Self {
        Self {
            format: 1,
            collection: String::new(),
            revision: 1,
            next_token: 1,
            next_action: 1,
            topology: StoredTopology {
                generation: 0,
                routes: Vec::new(),
            },
            history: Vec::new(),
            nodes: BTreeMap::new(),
            replicas: BTreeMap::new(),
            actions: Vec::new(),
            completed_actions: BTreeSet::new(),
        }
    }
}

/// Defaults of a [`PlanBalanceRequest`] (`docs/bandwidth-budget.md`).
pub const BALANCE_DEFAULT_MIN_GAIN: f64 = 0.10;
pub const BALANCE_DEFAULT_MAX_MOVES: u32 = 8;
pub const BALANCE_DEFAULT_MAX_RATE_AGE_MS: u64 = 10 * 60 * 1000;

/// The nodes a shard's primary may move among: the placement leaf's
/// node set when the topology has a tree and the leaf names nodes,
/// otherwise every node (`docs/placement.md`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BalancePool {
    pub leaf: String,
    /// `None` is "any node"; `Some` restricts to these node ids.
    pub node_ids: Option<BTreeSet<String>>,
}

/// One node as the planner sees it.
#[derive(Debug, Clone)]
struct BalanceNode {
    node_id: String,
    failure_domain: String,
    rate: u64,
    bytes: u64,
    /// `(shard index, shard id, replica addr, bytes)` of the primaries here.
    shards: Vec<(u32, String, String, u64)>,
    exclusion: Option<&'static str>,
}

impl BalanceNode {
    fn eligible(&self) -> bool {
        self.exclusion.is_none()
    }

    fn seconds(&self) -> f64 {
        match self.rate {
            0 => 0.0,
            rate => self.bytes as f64 / rate as f64,
        }
    }
}

/// Strip a scheme so a route address and a node's advertised address
/// compare as `host:port`.
fn bare_addr(addr: &str) -> &str {
    addr.trim_start_matches("http://")
        .trim_start_matches("https://")
        .trim_end_matches('/')
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

fn replica_key(shard_id: &str, node_id: &str) -> String {
    format!("{shard_id}\0{node_id}")
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StateWriteFault {
    BeforeRename,
    AfterRename,
    AfterSync,
    ExitBeforeRename,
    ExitAfterRename,
    ExitAfterSync,
}

#[cfg(test)]
fn inject_state_write_fault(
    fault: &Mutex<Option<StateWriteFault>>,
    boundary: StateWriteFault,
) -> Result<(), String> {
    let mut selected = fault.lock().unwrap();
    if matches!(
        (*selected, boundary),
        (
            Some(StateWriteFault::ExitBeforeRename),
            StateWriteFault::BeforeRename
        ) | (
            Some(StateWriteFault::ExitAfterRename),
            StateWriteFault::AfterRename
        ) | (
            Some(StateWriteFault::ExitAfterSync),
            StateWriteFault::AfterSync
        )
    ) {
        std::process::exit(87);
    }
    if *selected == Some(boundary) {
        *selected = None;
        return Err(format!(
            "injected control state write failure at {boundary:?}"
        ));
    }
    Ok(())
}

struct StateWriteError {
    message: String,
    may_have_published: bool,
}

impl From<String> for StateWriteError {
    fn from(message: String) -> Self {
        Self {
            message,
            may_have_published: false,
        }
    }
}

impl StateWriteError {
    fn publication(message: String) -> Self {
        Self {
            message,
            may_have_published: true,
        }
    }
}

const RETIRED_CONTROL_MAGIC: &[u8; 8] = b"PCTRLRET";
const RETIRED_CONTROL_STATE: &str = "legacy control authority is durably retired for import; recover its retirement record, never restart it as a writer";

const UNCERTAIN_CONTROL_STATE: &str =
    "control state persistence outcome is uncertain; close every clone and reopen existing durable state before continuing";

fn write_state(
    path: &Path,
    state: &StoredState,
    #[cfg(test)] fault: &Mutex<Option<StateWriteFault>>,
) -> Result<(), StateWriteError> {
    let bytes = serde_json::to_vec_pretty(state)
        .map_err(|error| format!("encode control state: {error}"))?;
    write_bytes(
        path,
        &bytes,
        #[cfg(test)]
        fault,
    )
}

fn write_bytes(
    path: &Path,
    bytes: &[u8],
    #[cfg(test)] fault: &Mutex<Option<StateWriteFault>>,
) -> Result<(), StateWriteError> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let candidate = storage::Candidate::write(path, bytes)?;
    #[cfg(test)]
    inject_state_write_fault(fault, StateWriteFault::BeforeRename)?;
    // Once publication has been attempted, a failed syscall must not be
    // interpreted as proof that the old state is still authoritative.
    candidate.publish(path).map_err(|error| {
        StateWriteError::publication(format!("replace {}: {error}", path.display()))
    })?;
    #[cfg(test)]
    inject_state_write_fault(fault, StateWriteFault::AfterRename)
        .map_err(StateWriteError::publication)?;
    std::fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| {
            StateWriteError::publication(format!("sync {}: {error}", parent.display()))
        })?;
    #[cfg(test)]
    inject_state_write_fault(fault, StateWriteFault::AfterSync)
        .map_err(StateWriteError::publication)?;
    Ok(())
}

#[derive(Clone)]
pub struct DurableControlPlane {
    state: Arc<Mutex<StoredState>>,
    path: Option<PathBuf>,
    policy: ControlPolicy,
    // Shared by every clone. Set while holding `state`; checked only after
    // acquiring that same lock, so a queued operation cannot miss the failure.
    persistence_uncertain: Arc<AtomicBool>,
    #[cfg(test)]
    write_fault: Arc<Mutex<Option<StateWriteFault>>>,
    // Every clone retains the same open lock file through its lifetime.
    _ownership_lock: Option<Arc<std::fs::File>>,
    #[cfg(feature = "net")]
    retirement: Arc<std::sync::OnceLock<Arc<crate::pb::storage::LegacyControlRetirement>>>,
}

#[derive(Clone)]
pub struct ClusterControlService {
    plane: DurableControlPlane,
    coordinator: Option<CoordinatorServiceImpl>,
    /// Whether every call must present a client certificate from the
    /// cluster CA (`docs/security.md`): membership, which a bearer
    /// token is not. Set when the listener runs TLS with a client CA.
    require_client_cert: bool,
}

#[cfg(test)]
mod checkpoint_tests;
#[cfg(test)]
pub(crate) mod test_fixtures {
    //! Legacy state files for tests in other modules.
    /// A populated legacy state file body under `collection`, with `routes`
    /// current routes (at least 2).
    pub(crate) fn populated_state_json(collection: &str, routes: usize) -> String {
        serde_json::to_string(&super::checkpoint_tests::complete_state_with(
            collection, routes,
        ))
        .unwrap()
    }
    /// The policy that state was captured under.
    pub(crate) fn populated_policy() -> super::ControlPolicy {
        super::checkpoint_tests::complete_policy()
    }
}

#[cfg(test)]
mod ownership_tests;

#[cfg(test)]
mod persistence_tests;

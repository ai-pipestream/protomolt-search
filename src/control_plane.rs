//! Durable lease, placement, and topology authority.
//!
//! Nodes report capacity and immutable shard generations. Reconciliation is
//! deterministic and idempotent: it expires leases, promotes ready replicas,
//! creates copy/drain/rebalance work, and schedules split, merge, or compaction
//! when the configured thresholds are crossed. Data movement stays on the
//! node-to-node WAL/snapshot paths; this module owns decisions, validated
//! action completion, and complete topology publication.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tonic::{Request, Response, Status};

use crate::coordinator::{CoordinatorServiceImpl, TopologyRoute};
use crate::metrics::Route;
use crate::pb::cluster_control_server::{ClusterControl, ClusterControlServer};
use crate::pb::{
    ClusterNode, ClusterNodeState, ClusterPlan, CompletePlacementActionRequest, DrainNodeRequest,
    GetClusterPlanRequest, NodeCapacity, NodeLease, PlacementAction, PlacementActionKind,
    ReconcileClusterRequest, RegisterNodeRequest, RenewNodeLeaseRequest, ReportShardRequest,
    RollbackClusterRequest, ShardReplicaRole, ShardReplicaState,
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
struct StoredCapacity {
    disk_bytes: u64,
    used_disk_bytes: u64,
    memory_bytes: u64,
    search_threads: u32,
    failure_domain: String,
    /// Absent in state files written before the field existed.
    #[serde(default)]
    scan_bytes_per_second: u64,
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
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
struct StoredRoute {
    addr: String,
    replica: Option<String>,
    hash_lo: u64,
    hash_hi: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredTopology {
    generation: u64,
    routes: Vec<StoredRoute>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    nodes: BTreeMap<String, StoredNode>,
    replicas: BTreeMap<String, StoredReplica>,
    actions: Vec<StoredAction>,
    #[serde(default)]
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

fn write_state(path: &Path, state: &StoredState) -> Result<(), String> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent)
        .map_err(|error| format!("mkdir {}: {error}", parent.display()))?;
    let mut temp = path.as_os_str().to_owned();
    temp.push(format!(".tmp-{}", std::process::id()));
    let temp = PathBuf::from(temp);
    let bytes = serde_json::to_vec_pretty(state)
        .map_err(|error| format!("encode control state: {error}"))?;
    {
        use std::io::Write;
        let mut file = std::fs::File::create(&temp)
            .map_err(|error| format!("create {}: {error}", temp.display()))?;
        file.write_all(&bytes)
            .and_then(|_| file.sync_all())
            .map_err(|error| format!("write {}: {error}", temp.display()))?;
    }
    std::fs::rename(&temp, path).map_err(|error| format!("replace {}: {error}", path.display()))?;
    std::fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| format!("sync {}: {error}", parent.display()))
}

#[derive(Clone)]
pub struct DurableControlPlane {
    state: Arc<Mutex<StoredState>>,
    path: Option<PathBuf>,
    policy: ControlPolicy,
}

impl DurableControlPlane {
    pub fn open(path: impl Into<PathBuf>, policy: ControlPolicy) -> Result<Self, String> {
        let path = path.into();
        let state = if path.exists() {
            let bytes = std::fs::read(&path)
                .map_err(|error| format!("read control state {}: {error}", path.display()))?;
            let state: StoredState = serde_json::from_slice(&bytes)
                .map_err(|error| format!("parse control state {}: {error}", path.display()))?;
            if state.format != 1 {
                return Err(format!(
                    "control state {} has format {}, expected 1",
                    path.display(),
                    state.format
                ));
            }
            state
        } else {
            StoredState::default()
        };
        let this = Self {
            state: Arc::new(Mutex::new(state)),
            path: Some(path),
            policy,
        };
        this.persist()?;
        Ok(this)
    }

    pub fn in_memory(policy: ControlPolicy) -> Self {
        Self {
            state: Arc::new(Mutex::new(StoredState::default())),
            path: None,
            policy,
        }
    }

    /// Bind this plane to a collection (`docs/collections.md`). State
    /// written before collections is adopted and written; state that
    /// names another collection is refused, because its shards are that
    /// dataset's.
    pub fn with_collection(self, name: &str) -> Result<Self, String> {
        {
            let mut state = self.state.lock().expect("control state lock poisoned");
            if state.collection.is_empty() {
                state.collection = name.to_string();
            } else if state.collection != name {
                return Err(format!(
                    "control state {} governs collection {:?}, not {name:?}",
                    self.path
                        .as_ref()
                        .map_or_else(|| "(in memory)".to_string(), |p| p.display().to_string()),
                    state.collection
                ));
            }
            self.persist_locked(&state)?;
        }
        Ok(self)
    }

    /// The collection this plane governs; empty for an unnamed dataset.
    pub fn collection(&self) -> String {
        self.state
            .lock()
            .expect("control state lock poisoned")
            .collection
            .clone()
    }

    /// Seed a pristine control store from the topology already loaded by the
    /// coordinator. Durable state is authoritative after this one-time
    /// bootstrap; a non-pristine store is never overwritten here.
    pub fn bootstrap_topology(
        &self,
        generation: u64,
        routes: &[TopologyRoute],
    ) -> Result<(), String> {
        let routes = routes
            .iter()
            .enumerate()
            .map(|(index, route)| {
                let (hash_lo, hash_hi) = route.hash_range.ok_or_else(|| {
                    format!("control topology route {index} is missing its hash range")
                })?;
                Ok(StoredRoute {
                    addr: route.addr.clone(),
                    replica: route.replica.clone(),
                    hash_lo,
                    hash_hi,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| "control state lock poisoned".to_string())?;
        let pristine = state.topology.generation == 0
            && state.topology.routes.is_empty()
            && state.history.is_empty()
            && state.nodes.is_empty()
            && state.replicas.is_empty()
            && state.actions.is_empty()
            && state.completed_actions.is_empty();
        if !pristine {
            return Ok(());
        }
        let before = state.clone();
        state.topology = StoredTopology { generation, routes };
        if let Err(error) = self.persist_locked(&state) {
            *state = before;
            return Err(error);
        }
        Ok(())
    }

    fn persist_locked(&self, state: &StoredState) -> Result<(), String> {
        match &self.path {
            Some(path) => write_state(path, state),
            None => Ok(()),
        }
    }

    fn persist(&self) -> Result<(), String> {
        let state = self
            .state
            .lock()
            .map_err(|_| "control state lock poisoned".to_string())?;
        self.persist_locked(&state)
    }

    fn lease_duration(&self, requested: u64) -> Result<u64, Status> {
        let duration = if requested == 0 {
            self.policy.lease_ms
        } else {
            requested
        };
        if !(1_000..=300_000).contains(&duration) {
            return Err(Status::invalid_argument(
                "lease_ms must be between 1000 and 300000",
            ));
        }
        Ok(duration)
    }

    fn validate_lease<'a>(
        state: &'a mut StoredState,
        node_id: &str,
        token: u64,
        now: u64,
    ) -> Result<&'a mut StoredNode, Status> {
        let node = state
            .nodes
            .get_mut(node_id)
            .ok_or_else(|| Status::not_found(format!("node {node_id:?} is not registered")))?;
        if token == 0 || token != node.lease_token {
            return Err(Status::failed_precondition(format!(
                "node {node_id:?} lease token does not match"
            )));
        }
        if node.expires_unix_ms <= now {
            return Err(Status::failed_precondition(format!(
                "node {node_id:?} lease expired at {}",
                node.expires_unix_ms
            )));
        }
        Ok(node)
    }

    fn register(&self, request: RegisterNodeRequest, now: u64) -> Result<NodeLease, Status> {
        if request.node_id.trim().is_empty() || request.addr.trim().is_empty() {
            return Err(Status::invalid_argument(
                "node registration needs non-empty node_id and addr",
            ));
        }
        let duration = self.lease_duration(request.lease_ms)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| Status::internal("control state lock poisoned"))?;
        if let Some(held) = state.nodes.get(&request.node_id) {
            if held.state != StoredNodeState::Expired && held.addr != request.addr {
                return Err(Status::already_exists(format!(
                    "node {:?} is already leased at {:?}",
                    request.node_id, held.addr
                )));
            }
        }
        let token = state.next_token;
        let next_token = token
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("lease token space exhausted"))?;
        let revision = state
            .revision
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("control revision overflow"))?;
        let expires = now
            .checked_add(duration)
            .ok_or_else(|| Status::resource_exhausted("lease expiry overflow"))?;
        let before = state.clone();
        state.next_token = next_token;
        state.nodes.insert(
            request.node_id.clone(),
            StoredNode {
                node_id: request.node_id.clone(),
                addr: request.addr,
                state: StoredNodeState::Active,
                lease_token: token,
                expires_unix_ms: expires,
                capacity: request.capacity.unwrap_or_default().into(),
            },
        );
        state.revision = revision;
        if let Err(error) = self.persist_locked(&state) {
            *state = before;
            return Err(Status::internal(error));
        }
        Ok(NodeLease {
            node_id: request.node_id,
            lease_token: token,
            expires_unix_ms: expires,
            control_revision: state.revision,
        })
    }

    fn renew(&self, request: RenewNodeLeaseRequest, now: u64) -> Result<NodeLease, Status> {
        let duration = self.lease_duration(request.lease_ms)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| Status::internal("control state lock poisoned"))?;
        let revision = state
            .revision
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("control revision overflow"))?;
        let before = state.clone();
        let node = Self::validate_lease(&mut state, &request.node_id, request.lease_token, now)?;
        node.expires_unix_ms = now
            .checked_add(duration)
            .ok_or_else(|| Status::resource_exhausted("lease expiry overflow"))?;
        if let Some(capacity) = request.capacity {
            node.capacity = capacity.into();
        }
        let lease = NodeLease {
            node_id: node.node_id.clone(),
            lease_token: node.lease_token,
            expires_unix_ms: node.expires_unix_ms,
            control_revision: revision,
        };
        state.revision = revision;
        if let Err(error) = self.persist_locked(&state) {
            *state = before;
            return Err(Status::internal(error));
        }
        Ok(lease)
    }

    fn drain(&self, request: DrainNodeRequest, now: u64) -> Result<(), Status> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Status::internal("control state lock poisoned"))?;
        let revision = state
            .revision
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("control revision overflow"))?;
        let before = state.clone();
        let node = Self::validate_lease(&mut state, &request.node_id, request.lease_token, now)?;
        node.state = StoredNodeState::Draining;
        state.revision = revision;
        if let Err(error) = self.persist_locked(&state) {
            *state = before;
            return Err(Status::internal(error));
        }
        Ok(())
    }

    fn report(&self, request: ReportShardRequest, now: u64) -> Result<(), Status> {
        let replica = request
            .replica
            .ok_or_else(|| Status::invalid_argument("shard report is missing replica state"))?;
        if replica.shard_id.trim().is_empty() {
            return Err(Status::invalid_argument("shard report needs shard_id"));
        }
        let role = match ShardReplicaRole::try_from(replica.role) {
            Ok(ShardReplicaRole::Primary) => StoredRole::Primary,
            Ok(ShardReplicaRole::Replica) => StoredRole::Replica,
            _ => {
                return Err(Status::invalid_argument(
                    "shard report needs PRIMARY or REPLICA role",
                ))
            }
        };
        if replica.hash_lo > replica.hash_hi {
            return Err(Status::invalid_argument(
                "shard report hash range is inverted",
            ));
        }
        if replica.tombstones > replica.rows {
            return Err(Status::invalid_argument(
                "shard tombstone count exceeds physical rows",
            ));
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| Status::internal("control state lock poisoned"))?;
        let revision = state
            .revision
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("control revision overflow"))?;
        let before = state.clone();
        let node_addr =
            Self::validate_lease(&mut state, &request.node_id, request.lease_token, now)?
                .addr
                .clone();
        if !replica.node_id.is_empty() && replica.node_id != request.node_id {
            return Err(Status::invalid_argument(
                "shard report node_id differs from the lease owner",
            ));
        }
        let addr = Self::shard_addr(&state, &request.node_id, &node_addr, replica.addr)?;
        let key = replica_key(&replica.shard_id, &request.node_id);
        state.replicas.insert(
            key,
            StoredReplica {
                shard_id: replica.shard_id,
                node_id: request.node_id,
                addr,
                generation: replica.generation,
                hash_lo: replica.hash_lo,
                hash_hi: replica.hash_hi,
                slot_offset: replica.slot_offset,
                rows: replica.rows,
                bytes: replica.bytes,
                role,
                ready: replica.ready,
                scoring_fingerprint: replica.scoring_fingerprint,
                analysis_fingerprint: replica.analysis_fingerprint,
                immutable_segments: replica.immutable_segments,
                tombstones: replica.tombstones,
            },
        );
        state.revision = revision;
        if let Err(error) = self.persist_locked(&state) {
            *state = before;
            return Err(Status::internal(error));
        }
        Ok(())
    }

    /// The listener a reported copy is served on: the node's registered
    /// address when the report leaves it empty, else the address the
    /// report names — the lease owner vouches for its own listeners
    /// (one per shard, one per placed replica), and another node's
    /// registered address is not one of them.
    fn shard_addr(
        state: &StoredState,
        node_id: &str,
        node_addr: &str,
        reported: String,
    ) -> Result<String, Status> {
        if reported.is_empty() {
            return Ok(node_addr.to_string());
        }
        if let Some(other) = state
            .nodes
            .values()
            .find(|other| other.node_id != node_id && other.addr == reported)
        {
            return Err(Status::invalid_argument(format!(
                "shard addr {reported:?} is the registered address of node {:?}, not a listener \
                 of node {node_id:?}",
                other.node_id
            )));
        }
        Ok(reported)
    }

    fn checked_output(
        state: &StoredState,
        target_node_id: &str,
        output: ShardReplicaState,
    ) -> Result<StoredReplica, Status> {
        if output.shard_id.trim().is_empty() || !output.ready {
            return Err(Status::invalid_argument(
                "action outputs need a non-empty shard_id and ready=true",
            ));
        }
        if output.hash_lo > output.hash_hi || output.tombstones > output.rows {
            return Err(Status::invalid_argument(
                "action output has an invalid range or tombstone count",
            ));
        }
        if !output.node_id.is_empty() && output.node_id != target_node_id {
            return Err(Status::invalid_argument(
                "action output node_id differs from the assigned target",
            ));
        }
        let node_addr = state
            .nodes
            .get(target_node_id)
            .ok_or_else(|| Status::failed_precondition("action target is no longer registered"))?
            .addr
            .clone();
        let addr = Self::shard_addr(state, target_node_id, &node_addr, output.addr)?;
        let role = match ShardReplicaRole::try_from(output.role) {
            Ok(ShardReplicaRole::Primary) => StoredRole::Primary,
            Ok(ShardReplicaRole::Replica) => StoredRole::Replica,
            _ => {
                return Err(Status::invalid_argument(
                    "action output needs PRIMARY or REPLICA role",
                ))
            }
        };
        Ok(StoredReplica {
            shard_id: output.shard_id,
            node_id: target_node_id.to_string(),
            addr,
            generation: output.generation,
            hash_lo: output.hash_lo,
            hash_hi: output.hash_hi,
            slot_offset: output.slot_offset,
            rows: output.rows,
            bytes: output.bytes,
            role,
            ready: true,
            scoring_fingerprint: output.scoring_fingerprint,
            analysis_fingerprint: output.analysis_fingerprint,
            immutable_segments: output.immutable_segments,
            tombstones: output.tombstones,
        })
    }

    fn require_same_contract(
        output: &StoredReplica,
        source: &StoredReplica,
        generation: u64,
    ) -> Result<(), Status> {
        if output.generation != generation
            || output.scoring_fingerprint != source.scoring_fingerprint
            || output.analysis_fingerprint != source.analysis_fingerprint
        {
            return Err(Status::failed_precondition(
                "action output generation or scoring/analysis fingerprint differs from the plan",
            ));
        }
        Ok(())
    }

    fn require_range_tiling(
        outputs: &[StoredReplica],
        hash_lo: u64,
        hash_hi: u64,
    ) -> Result<(), Status> {
        let mut ranges = outputs
            .iter()
            .map(|output| (output.hash_lo, output.hash_hi))
            .collect::<Vec<_>>();
        ranges.sort_unstable();
        let mut expected = hash_lo;
        for (lo, hi) in ranges {
            if lo != expected || hi < lo {
                return Err(Status::failed_precondition(
                    "action outputs do not exactly tile the source hash range",
                ));
            }
            expected = hi.checked_add(1).unwrap_or(0);
        }
        if outputs.is_empty()
            || outputs.iter().map(|output| output.hash_hi).max() != Some(hash_hi)
            || (hash_hi != u64::MAX && expected != hash_hi + 1)
        {
            return Err(Status::failed_precondition(
                "action outputs do not exactly tile the source hash range",
            ));
        }
        Ok(())
    }

    fn complete_action(
        &self,
        request: CompletePlacementActionRequest,
        now: u64,
    ) -> Result<(), Status> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Status::internal("control state lock poisoned"))?;
        Self::validate_lease(&mut state, &request.node_id, request.lease_token, now)?;
        if state.completed_actions.contains(&request.action_id) {
            return Ok(());
        }
        let action = state
            .actions
            .iter()
            .find(|action| action.action_id == request.action_id)
            .cloned()
            .ok_or_else(|| Status::not_found("placement action is not pending"))?;
        let revision = state
            .revision
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("control revision overflow"))?;
        if action.target_node_id != request.node_id {
            return Err(Status::permission_denied(
                "placement action belongs to a different target node",
            ));
        }
        let kind = PlacementActionKind::try_from(action.kind)
            .map_err(|_| Status::failed_precondition("placement action has unknown kind"))?;
        let source = state
            .replicas
            .get(&replica_key(&action.shard_id, &action.source_node_id))
            .or_else(|| {
                state.replicas.values().find(|replica| {
                    replica.shard_id == action.shard_id && replica.role == StoredRole::Primary
                })
            })
            .cloned()
            .ok_or_else(|| Status::failed_precondition("placement action source is missing"))?;
        if source.generation != action.source_generation {
            return Err(Status::failed_precondition(
                "placement action source generation changed after planning",
            ));
        }
        let mut output_ids = BTreeSet::new();
        let outputs = request
            .outputs
            .into_iter()
            .map(|output| Self::checked_output(&state, &request.node_id, output))
            .collect::<Result<Vec<_>, _>>()?;
        if outputs
            .iter()
            .any(|output| !output_ids.insert(output.shard_id.clone()))
        {
            return Err(Status::invalid_argument(
                "placement action repeats an output shard_id",
            ));
        }

        let before = state.clone();
        let mutation = (|| -> Result<(), Status> {
            match kind {
                PlacementActionKind::CopyReplica => {
                    let [output] = outputs.as_slice() else {
                        return Err(Status::invalid_argument(
                            "COPY_REPLICA completion needs exactly one output",
                        ));
                    };
                    Self::require_same_contract(output, &source, action.target_generation)?;
                    if output.role != StoredRole::Replica
                        || output.shard_id != source.shard_id
                        || output.hash_lo != source.hash_lo
                        || output.hash_hi != source.hash_hi
                        || output.rows != source.rows
                        || output.tombstones != source.tombstones
                    {
                        return Err(Status::failed_precondition(
                            "copied replica differs from its source",
                        ));
                    }
                    state.replicas.insert(
                        replica_key(&output.shard_id, &output.node_id),
                        output.clone(),
                    );
                    let source_state = state
                        .nodes
                        .get(&source.node_id)
                        .map(|node| node.state)
                        .unwrap_or(StoredNodeState::Expired);
                    if source_state != StoredNodeState::Active
                        || action.reason == "capacity rebalance"
                    {
                        for held in state
                            .replicas
                            .values_mut()
                            .filter(|held| held.shard_id == source.shard_id)
                        {
                            held.role = if held.node_id == output.node_id {
                                StoredRole::Primary
                            } else {
                                StoredRole::Replica
                            };
                        }
                        Self::push_action(
                            &mut state,
                            &source,
                            ActionSpec {
                                kind: PlacementActionKind::DropReplica,
                                peer_shard_id: String::new(),
                                peer_source_generation: 0,
                                target_node_id: source.node_id.clone(),
                                target_generation: source.generation,
                                reason: "replacement copy is ready and promoted",
                            },
                        )?;
                    }
                }
                PlacementActionKind::DropReplica => {
                    if !outputs.is_empty() {
                        return Err(Status::invalid_argument(
                            "DROP_REPLICA completion must not carry outputs",
                        ));
                    }
                    state
                        .replicas
                        .remove(&replica_key(&source.shard_id, &source.node_id));
                }
                PlacementActionKind::PromoteReplica => {
                    if !outputs.is_empty() {
                        return Err(Status::invalid_argument(
                            "PROMOTE_REPLICA completion must not carry outputs",
                        ));
                    }
                    for held in state
                        .replicas
                        .values_mut()
                        .filter(|held| held.shard_id == source.shard_id)
                    {
                        held.role = if held.node_id == request.node_id {
                            StoredRole::Primary
                        } else {
                            StoredRole::Replica
                        };
                    }
                }
                PlacementActionKind::CompactShard => {
                    let [output] = outputs.as_slice() else {
                        return Err(Status::invalid_argument(
                            "COMPACT_SHARD completion needs exactly one output",
                        ));
                    };
                    Self::require_same_contract(output, &source, action.target_generation)?;
                    if output.role != StoredRole::Primary
                        || output.shard_id != source.shard_id
                        || output.hash_lo != source.hash_lo
                        || output.hash_hi != source.hash_hi
                        || output.rows != source.rows - source.tombstones
                        || output.tombstones != 0
                    {
                        return Err(Status::failed_precondition(
                            "compaction output does not conserve the source live rows and range",
                        ));
                    }
                    state
                        .replicas
                        .retain(|_, held| held.shard_id != source.shard_id);
                    state.replicas.insert(
                        replica_key(&output.shard_id, &output.node_id),
                        output.clone(),
                    );
                }
                PlacementActionKind::SplitShard => {
                    if outputs.len() < 2 {
                        return Err(Status::invalid_argument(
                            "SPLIT_SHARD completion needs at least two children",
                        ));
                    }
                    Self::require_range_tiling(&outputs, source.hash_lo, source.hash_hi)?;
                    let mut rows = 0u64;
                    for output in &outputs {
                        Self::require_same_contract(output, &source, action.target_generation)?;
                        if output.role != StoredRole::Primary || output.tombstones != 0 {
                            return Err(Status::failed_precondition(
                                "split children must be dense primaries",
                            ));
                        }
                        rows = rows.checked_add(output.rows).ok_or_else(|| {
                            Status::resource_exhausted("split output row count overflow")
                        })?;
                    }
                    if rows != source.rows - source.tombstones {
                        return Err(Status::failed_precondition(
                            "split children do not conserve source live rows",
                        ));
                    }
                    if outputs.iter().any(|output| {
                        output.shard_id != source.shard_id
                            && state
                                .replicas
                                .values()
                                .any(|held| held.shard_id == output.shard_id)
                    }) {
                        return Err(Status::already_exists(
                            "a split child shard_id already exists",
                        ));
                    }
                    state
                        .replicas
                        .retain(|_, held| held.shard_id != source.shard_id);
                    for output in outputs {
                        state
                            .replicas
                            .insert(replica_key(&output.shard_id, &output.node_id), output);
                    }
                }
                PlacementActionKind::MergeShards => {
                    let [output] = outputs.as_slice() else {
                        return Err(Status::invalid_argument(
                            "MERGE_SHARDS completion needs exactly one output",
                        ));
                    };
                    let peer = state
                        .replicas
                        .values()
                        .find(|replica| {
                            replica.shard_id == action.peer_shard_id
                                && replica.role == StoredRole::Primary
                        })
                        .cloned()
                        .ok_or_else(|| Status::failed_precondition("merge peer is missing"))?;
                    if peer.generation != action.peer_source_generation {
                        return Err(Status::failed_precondition(
                            "merge peer generation changed after planning",
                        ));
                    }
                    Self::require_same_contract(output, &source, action.target_generation)?;
                    if peer.scoring_fingerprint != source.scoring_fingerprint
                        || peer.analysis_fingerprint != source.analysis_fingerprint
                        || source.hash_hi.checked_add(1) != Some(peer.hash_lo)
                        || output.role != StoredRole::Primary
                        || output.hash_lo != source.hash_lo
                        || output.hash_hi != peer.hash_hi
                        || output.tombstones != 0
                        || output.rows
                            != (source.rows - source.tombstones)
                                .checked_add(peer.rows - peer.tombstones)
                                .ok_or_else(|| {
                                    Status::resource_exhausted("merge live row count overflow")
                                })?
                    {
                        return Err(Status::failed_precondition(
                            "merge output does not conserve the adjacent inputs",
                        ));
                    }
                    if output.shard_id != source.shard_id
                        && output.shard_id != peer.shard_id
                        && state
                            .replicas
                            .values()
                            .any(|held| held.shard_id == output.shard_id)
                    {
                        return Err(Status::already_exists(
                            "merged output shard_id already exists",
                        ));
                    }
                    state.replicas.retain(|_, held| {
                        held.shard_id != source.shard_id && held.shard_id != peer.shard_id
                    });
                    state.replicas.insert(
                        replica_key(&output.shard_id, &output.node_id),
                        output.clone(),
                    );
                }
                PlacementActionKind::Unspecified => {
                    return Err(Status::failed_precondition(
                        "cannot complete an unspecified placement action",
                    ));
                }
            }
            Ok(())
        })();
        if let Err(error) = mutation {
            *state = before.clone();
            return Err(error);
        }
        state
            .actions
            .retain(|pending| pending.action_id != action.action_id);
        state.completed_actions.insert(action.action_id);
        while state.completed_actions.len() > 4_096 {
            if let Some(first) = state.completed_actions.first().copied() {
                state.completed_actions.remove(&first);
            }
        }
        state.revision = revision;
        if let Err(error) = self.persist_locked(&state) {
            *state = before;
            return Err(Status::internal(error));
        }
        Ok(())
    }

    fn active_node(state: &StoredState, node_id: &str) -> bool {
        state
            .nodes
            .get(node_id)
            .is_some_and(|node| node.state == StoredNodeState::Active)
    }

    fn choose_target(
        state: &StoredState,
        excluded: &BTreeSet<String>,
        source_domain: &str,
    ) -> Option<String> {
        state
            .nodes
            .values()
            .filter(|node| node.state == StoredNodeState::Active)
            .filter(|node| !excluded.contains(&node.node_id))
            .min_by(|left, right| {
                let left_same = (!source_domain.is_empty()
                    && left.capacity.failure_domain == source_domain)
                    as u8;
                let right_same = (!source_domain.is_empty()
                    && right.capacity.failure_domain == source_domain)
                    as u8;
                left_same.cmp(&right_same).then_with(|| {
                    let left_total = left.capacity.disk_bytes.max(1);
                    let right_total = right.capacity.disk_bytes.max(1);
                    let left_used = u128::from(left.capacity.used_disk_bytes);
                    let right_used = u128::from(right.capacity.used_disk_bytes);
                    (left_used * u128::from(right_total))
                        .cmp(&(right_used * u128::from(left_total)))
                        .then_with(|| left.node_id.cmp(&right.node_id))
                })
            })
            .map(|node| node.node_id.clone())
    }

    fn push_action(
        state: &mut StoredState,
        shard: &StoredReplica,
        spec: ActionSpec<'_>,
    ) -> Result<(), Status> {
        if state.actions.iter().any(|action| {
            action.kind == spec.kind as i32
                && action.shard_id == shard.shard_id
                && action.target_node_id == spec.target_node_id
        }) {
            return Ok(());
        }
        let action_id = state.next_action;
        state.next_action = state
            .next_action
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("placement action id space exhausted"))?;
        state.actions.push(StoredAction {
            action_id,
            kind: spec.kind as i32,
            shard_id: shard.shard_id.clone(),
            peer_shard_id: spec.peer_shard_id,
            peer_source_generation: spec.peer_source_generation,
            source_node_id: shard.node_id.clone(),
            target_node_id: spec.target_node_id,
            source_generation: shard.generation,
            target_generation: spec.target_generation,
            hash_lo: shard.hash_lo,
            hash_hi: shard.hash_hi,
            reason: spec.reason.to_string(),
        });
        Ok(())
    }

    fn desired_routes(state: &StoredState) -> Result<Option<Vec<StoredRoute>>, String> {
        let mut by_shard: BTreeMap<&str, Vec<&StoredReplica>> = BTreeMap::new();
        for replica in state.replicas.values().filter(|replica| replica.ready) {
            by_shard.entry(&replica.shard_id).or_default().push(replica);
        }
        let mut routes = Vec::new();
        for (_shard_id, replicas) in by_shard {
            let Some(primary) = replicas.iter().copied().find(|replica| {
                replica.role == StoredRole::Primary && Self::active_node(state, &replica.node_id)
            }) else {
                continue;
            };
            let replica = replicas
                .iter()
                .copied()
                .find(|replica| {
                    replica.role == StoredRole::Replica
                        && Self::active_node(state, &replica.node_id)
                        && replica.generation == primary.generation
                        && replica.scoring_fingerprint == primary.scoring_fingerprint
                        && replica.analysis_fingerprint == primary.analysis_fingerprint
                })
                .map(|replica| replica.addr.clone());
            routes.push(StoredRoute {
                addr: primary.addr.clone(),
                replica,
                hash_lo: primary.hash_lo,
                hash_hi: primary.hash_hi,
            });
        }
        routes.sort_by_key(|route| route.hash_lo);
        if !routes.is_empty() {
            let mut expected = 0u64;
            for route in &routes {
                if route.hash_lo != expected {
                    return Ok(None);
                }
                expected = route.hash_hi.checked_add(1).unwrap_or(0);
            }
            if routes.last().is_some_and(|route| route.hash_hi != u64::MAX) {
                return Ok(None);
            }
        }
        Ok((!routes.is_empty()).then_some(routes))
    }

    fn reconcile_locked(&self, state: &mut StoredState, now: u64) -> Result<bool, Status> {
        let revision = state
            .revision
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("control revision overflow"))?;
        for node in state.nodes.values_mut() {
            if node.expires_unix_ms <= now && node.state != StoredNodeState::Expired {
                node.state = StoredNodeState::Expired;
            }
        }

        let shard_ids: BTreeSet<String> = state
            .replicas
            .values()
            .map(|replica| replica.shard_id.clone())
            .collect();
        for shard_id in shard_ids {
            let replicas: Vec<StoredReplica> = state
                .replicas
                .values()
                .filter(|replica| replica.shard_id == shard_id && replica.ready)
                .cloned()
                .collect();
            let primary = replicas
                .iter()
                .find(|replica| replica.role == StoredRole::Primary)
                .cloned();
            let primary_available = primary
                .as_ref()
                .is_some_and(|replica| Self::active_node(state, &replica.node_id));
            if !primary_available {
                if let Some(promote) = replicas
                    .iter()
                    .filter(|replica| replica.role == StoredRole::Replica)
                    .filter(|replica| Self::active_node(state, &replica.node_id))
                    .max_by_key(|replica| replica.generation)
                    .cloned()
                {
                    for held in state
                        .replicas
                        .values_mut()
                        .filter(|held| held.shard_id == shard_id)
                    {
                        held.role = if held.node_id == promote.node_id {
                            StoredRole::Primary
                        } else {
                            StoredRole::Replica
                        };
                    }
                }
            }

            let primary = state
                .replicas
                .values()
                .find(|replica| {
                    replica.shard_id == shard_id
                        && replica.role == StoredRole::Primary
                        && replica.ready
                })
                .cloned();
            let Some(primary) = primary else { continue };
            let copies: Vec<StoredReplica> = state
                .replicas
                .values()
                .filter(|replica| {
                    replica.shard_id == shard_id
                        && replica.ready
                        && replica.generation == primary.generation
                        && replica.scoring_fingerprint == primary.scoring_fingerprint
                        && replica.analysis_fingerprint == primary.analysis_fingerprint
                })
                .cloned()
                .collect();
            let primary_state = state
                .nodes
                .get(&primary.node_id)
                .map(|node| node.state)
                .unwrap_or(StoredNodeState::Expired);
            let need_copy = copies.len() < self.policy.replication_factor
                || primary_state != StoredNodeState::Active;
            if need_copy {
                let excluded: BTreeSet<String> = copies
                    .iter()
                    .map(|replica| replica.node_id.clone())
                    .collect();
                let source_domain = state
                    .nodes
                    .get(&primary.node_id)
                    .map(|node| node.capacity.failure_domain.as_str())
                    .unwrap_or("");
                if let Some(target) = Self::choose_target(state, &excluded, source_domain) {
                    Self::push_action(
                        state,
                        &primary,
                        ActionSpec {
                            kind: PlacementActionKind::CopyReplica,
                            peer_shard_id: String::new(),
                            peer_source_generation: 0,
                            target_node_id: target,
                            target_generation: primary.generation,
                            reason: if primary_state == StoredNodeState::Draining {
                                "graceful drain"
                            } else if primary_state == StoredNodeState::Expired {
                                "replace expired primary"
                            } else {
                                "replication deficit"
                            },
                        },
                    )?;
                }
            }

            let copy_pending = state.actions.iter().any(|action| {
                action.shard_id == primary.shard_id
                    && action.kind == PlacementActionKind::CopyReplica as i32
            });
            if primary_state == StoredNodeState::Active && !copy_pending {
                let target_generation = primary.generation.checked_add(1);
                if primary.rows > self.policy.split_rows && primary.hash_lo < primary.hash_hi {
                    if let Some(target_generation) = target_generation {
                        Self::push_action(
                            state,
                            &primary,
                            ActionSpec {
                                kind: PlacementActionKind::SplitShard,
                                peer_shard_id: String::new(),
                                peer_source_generation: 0,
                                target_node_id: primary.node_id.clone(),
                                target_generation,
                                reason: "capacity-aware automatic split",
                            },
                        )?;
                    }
                } else if primary.immutable_segments >= self.policy.compact_segments
                    || (primary.rows > 0
                        && primary.tombstones.saturating_mul(1_000_000)
                            >= primary
                                .rows
                                .saturating_mul(u64::from(self.policy.compact_tombstone_ppm)))
                {
                    if let Some(target_generation) = target_generation {
                        Self::push_action(
                            state,
                            &primary,
                            ActionSpec {
                                kind: PlacementActionKind::CompactShard,
                                peer_shard_id: String::new(),
                                peer_source_generation: 0,
                                target_node_id: primary.node_id.clone(),
                                target_generation,
                                reason: "segment or tombstone compaction threshold",
                            },
                        )?;
                    }
                }
            }
        }

        // Merge only adjacent, small primaries. One action covers the pair.
        let mut primaries: Vec<StoredReplica> = state
            .replicas
            .values()
            .filter(|replica| replica.role == StoredRole::Primary && replica.ready)
            .cloned()
            .collect();
        primaries.sort_by_key(|replica| replica.hash_lo);
        for pair in primaries.windows(2) {
            let left = &pair[0];
            let right = &pair[1];
            if left.hash_hi.checked_add(1) == Some(right.hash_lo)
                && left.rows.saturating_add(right.rows) <= self.policy.merge_rows
                && left.scoring_fingerprint == right.scoring_fingerprint
                && left.analysis_fingerprint == right.analysis_fingerprint
                && !state.actions.iter().any(|action| {
                    action.shard_id == left.shard_id
                        || action.shard_id == right.shard_id
                        || action.peer_shard_id == left.shard_id
                        || action.peer_shard_id == right.shard_id
                })
            {
                if let Some(target_generation) =
                    left.generation.max(right.generation).checked_add(1)
                {
                    Self::push_action(
                        state,
                        left,
                        ActionSpec {
                            kind: PlacementActionKind::MergeShards,
                            peer_shard_id: right.shard_id.clone(),
                            peer_source_generation: right.generation,
                            target_node_id: left.node_id.clone(),
                            target_generation,
                            reason: "adjacent shards are below the merge threshold",
                        },
                    )?;
                }
            }
        }

        // Rebalance one primary at a time when active-node disk utilization
        // differs by more than 15 percentage points. Copy first; the ready
        // report promotes the target and emits a drop action for the source.
        let utilization = |node: &StoredNode| -> u64 {
            node.capacity.used_disk_bytes.saturating_mul(1_000_000)
                / node.capacity.disk_bytes.max(1)
        };
        let active: Vec<&StoredNode> = state
            .nodes
            .values()
            .filter(|node| node.state == StoredNodeState::Active)
            .collect();
        if let (Some(high), Some(low)) = (
            active.iter().copied().max_by_key(|node| utilization(node)),
            active.iter().copied().min_by_key(|node| utilization(node)),
        ) {
            if high.node_id != low.node_id
                && utilization(high).saturating_sub(utilization(low)) >= 150_000
            {
                let candidate = state
                    .replicas
                    .values()
                    .filter(|replica| {
                        replica.role == StoredRole::Primary
                            && replica.ready
                            && replica.node_id == high.node_id
                    })
                    .filter(|replica| {
                        !state.replicas.values().any(|held| {
                            held.shard_id == replica.shard_id && held.node_id == low.node_id
                        })
                    })
                    .filter(|replica| {
                        !state
                            .actions
                            .iter()
                            .any(|action| action.shard_id == replica.shard_id)
                    })
                    .max_by_key(|replica| replica.bytes)
                    .cloned();
                if let Some(candidate) = candidate {
                    Self::push_action(
                        state,
                        &candidate,
                        ActionSpec {
                            kind: PlacementActionKind::CopyReplica,
                            peer_shard_id: String::new(),
                            peer_source_generation: 0,
                            target_node_id: low.node_id.clone(),
                            target_generation: candidate.generation,
                            reason: "capacity rebalance",
                        },
                    )?;
                }
            }
        }

        let routes = Self::desired_routes(state).map_err(Status::failed_precondition)?;
        let topology_changed = routes
            .as_ref()
            .is_some_and(|routes| routes != &state.topology.routes);
        if topology_changed {
            state.history.push(state.topology.clone());
            if state.history.len() > self.policy.history_limit {
                state.history.remove(0);
            }
            state.topology = StoredTopology {
                generation: state
                    .topology
                    .generation
                    .checked_add(1)
                    .ok_or_else(|| Status::failed_precondition("topology generation overflow"))?
                    .max(1),
                routes: routes.expect("topology_changed requires complete routes"),
            };
        }
        state.revision = revision;
        Ok(topology_changed)
    }

    fn reconcile(&self, dry_run: bool, now: u64) -> Result<(ClusterPlan, bool), Status> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Status::internal("control state lock poisoned"))?;
        if dry_run {
            let mut candidate = state.clone();
            let changed = self.reconcile_locked(&mut candidate, now)?;
            return Ok((Self::plan_of(&candidate), changed));
        }
        let before = state.clone();
        let changed = self.reconcile_locked(&mut state, now)?;
        if let Err(error) = self.persist_locked(&state) {
            *state = before;
            return Err(Status::internal(error));
        }
        Ok((Self::plan_of(&state), changed))
    }

    fn rollback(&self, requested: u64) -> Result<(ClusterPlan, bool), Status> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Status::internal("control state lock poisoned"))?;
        let position = if requested == 0 {
            state.history.len().checked_sub(1)
        } else {
            state
                .history
                .iter()
                .position(|topology| topology.generation == requested)
        }
        .ok_or_else(|| Status::not_found("requested topology is not in durable history"))?;
        let generation = state
            .topology
            .generation
            .checked_add(1)
            .ok_or_else(|| Status::failed_precondition("topology generation overflow"))?;
        let revision = state
            .revision
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("control revision overflow"))?;
        let before = state.clone();
        let old = state.history.remove(position);
        let current = state.topology.clone();
        state.history.push(current);
        state.topology = StoredTopology {
            generation,
            routes: old.routes,
        };
        state.revision = revision;
        if let Err(error) = self.persist_locked(&state) {
            *state = before;
            return Err(Status::internal(error));
        }
        Ok((Self::plan_of(&state), true))
    }

    fn plan(&self) -> Result<ClusterPlan, Status> {
        let state = self
            .state
            .lock()
            .map_err(|_| Status::internal("control state lock poisoned"))?;
        Ok(Self::plan_of(&state))
    }

    fn topology_routes(&self) -> Result<(u64, Vec<TopologyRoute>), Status> {
        let state = self
            .state
            .lock()
            .map_err(|_| Status::internal("control state lock poisoned"))?;
        Ok((
            state.topology.generation,
            state
                .topology
                .routes
                .iter()
                .map(|route| TopologyRoute {
                    addr: route.addr.clone(),
                    replica: route.replica.clone(),
                    hash_range: Some((route.hash_lo, route.hash_hi)),
                    placement: None,
                })
                .collect(),
        ))
    }

    fn plan_of(state: &StoredState) -> ClusterPlan {
        ClusterPlan {
            collection: state.collection.clone(),
            control_revision: state.revision,
            topology_generation: state.topology.generation,
            nodes: state
                .nodes
                .values()
                .map(|node| ClusterNode {
                    collection: state.collection.clone(),
                    node_id: node.node_id.clone(),
                    addr: node.addr.clone(),
                    state: match node.state {
                        StoredNodeState::Active => ClusterNodeState::Active as i32,
                        StoredNodeState::Draining => ClusterNodeState::Draining as i32,
                        StoredNodeState::Expired => ClusterNodeState::Expired as i32,
                    },
                    expires_unix_ms: node.expires_unix_ms,
                    capacity: Some((&node.capacity).into()),
                })
                .collect(),
            replicas: state
                .replicas
                .values()
                .map(|replica| ShardReplicaState {
                    collection: state.collection.clone(),
                    shard_id: replica.shard_id.clone(),
                    node_id: replica.node_id.clone(),
                    addr: replica.addr.clone(),
                    generation: replica.generation,
                    hash_lo: replica.hash_lo,
                    hash_hi: replica.hash_hi,
                    slot_offset: replica.slot_offset,
                    rows: replica.rows,
                    bytes: replica.bytes,
                    role: match replica.role {
                        StoredRole::Primary => ShardReplicaRole::Primary as i32,
                        StoredRole::Replica => ShardReplicaRole::Replica as i32,
                    },
                    ready: replica.ready,
                    scoring_fingerprint: replica.scoring_fingerprint.clone(),
                    analysis_fingerprint: replica.analysis_fingerprint.clone(),
                    immutable_segments: replica.immutable_segments,
                    tombstones: replica.tombstones,
                })
                .collect(),
            actions: state
                .actions
                .iter()
                .map(|action| PlacementAction {
                    collection: state.collection.clone(),
                    action_id: action.action_id,
                    kind: action.kind,
                    shard_id: action.shard_id.clone(),
                    peer_shard_id: action.peer_shard_id.clone(),
                    peer_source_generation: action.peer_source_generation,
                    source_node_id: action.source_node_id.clone(),
                    target_node_id: action.target_node_id.clone(),
                    source_generation: action.source_generation,
                    target_generation: action.target_generation,
                    hash_lo: action.hash_lo,
                    hash_hi: action.hash_hi,
                    reason: action.reason.clone(),
                })
                .collect(),
            topology_history: state
                .history
                .iter()
                .map(|topology| topology.generation)
                .collect(),
            topology: state
                .topology
                .routes
                .iter()
                .map(|route| crate::pb::PublishedTopologyShard {
                    addr: route.addr.clone(),
                    replica: route.replica.clone().unwrap_or_default(),
                    hash_lo: route.hash_lo,
                    hash_hi: route.hash_hi,
                    has_placement: false,
                    placement: 0,
                })
                .collect(),
        }
    }
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

impl ClusterControlService {
    pub fn new(plane: DurableControlPlane) -> Self {
        Self {
            plane,
            coordinator: None,
            require_client_cert: false,
        }
    }

    /// Demand a client certificate on every call.
    pub fn with_client_cert_required(mut self, required: bool) -> Self {
        self.require_client_cert = required;
        self
    }

    /// Cluster membership: a client certificate the listener verified
    /// against the cluster CA. A missing one refuses by name.
    fn membership<T>(&self, request: &Request<T>) -> Result<(), Status> {
        if !self.require_client_cert {
            return Ok(());
        }
        #[cfg(feature = "tls")]
        if request.peer_certs().is_some_and(|certs| !certs.is_empty()) {
            return Ok(());
        }
        #[cfg(not(feature = "tls"))]
        let _ = request;
        Err(Status::unauthenticated(
            "cluster control requires a client certificate from the cluster CA; a bearer \
             token is not membership",
        ))
    }

    pub fn with_coordinator(mut self, coordinator: CoordinatorServiceImpl) -> Self {
        self.coordinator = Some(coordinator);
        self
    }

    /// The collection this plane governs (`docs/collections.md`).
    pub fn collection(&self) -> String {
        self.plane.collection()
    }

    /// Admit a request only for this plane's collection (the same rule
    /// as `CoordinatorServiceImpl::admit`).
    fn admit(&self, requested: &str) -> Result<(), Status> {
        let own = self.plane.collection();
        if requested.is_empty() || requested == own {
            return Ok(());
        }
        Err(if own.is_empty() {
            Status::invalid_argument(format!(
                "unknown collection {requested:?}: this control plane governs one unnamed dataset"
            ))
        } else {
            Status::invalid_argument(format!(
                "this control plane governs collection {own:?}, not {requested:?}"
            ))
        })
    }

    pub fn into_server(self, max_message_bytes: usize) -> ClusterControlServer<Self> {
        ClusterControlServer::new(self)
            .max_decoding_message_size(max_message_bytes)
            .max_encoding_message_size(max_message_bytes)
    }

    pub fn publish_current_topology(&self) -> Result<(), Status> {
        let Some(coordinator) = &self.coordinator else {
            return Ok(());
        };
        let (generation, routes) = self.plane.topology_routes()?;
        // The durable control state holds hash ranges and addresses, not
        // placement codes: publishing over a placed generation would
        // strip the tree, so it is refused until the plane carries it.
        if coordinator.current_placement().is_some() {
            return Err(Status::failed_precondition(
                "the live topology has a placement tree (docs/placement.md); the control \
                 plane does not publish placed topologies yet",
            ));
        }
        let current = coordinator.current_topology_generation();
        if generation == current {
            if routes == coordinator.current_topology_routes() {
                return Ok(());
            }
            return Err(Status::failed_precondition(format!(
                "durable control topology conflicts with live coordinator generation {current}"
            )));
        }
        if generation < current {
            return Err(Status::failed_precondition(format!(
                "durable control topology generation {generation} is behind live coordinator generation {current}"
            )));
        }
        coordinator
            .reload_topology(generation, routes, None)
            .map_err(|error| Status::failed_precondition(format!("publish topology: {error}")))
    }

    fn publish_if_needed(&self) -> Result<(), Status> {
        self.publish_current_topology()
    }
}

#[tonic::async_trait]
impl ClusterControl for ClusterControlService {
    async fn register_node(
        &self,
        request: Request<RegisterNodeRequest>,
    ) -> Result<Response<NodeLease>, Status> {
        crate::metrics::timed(Route::RegisterNode, request, |request| async move {
            self.membership(&request)?;
            self.admit(&request.get_ref().collection)?;
            self.plane
                .register(request.into_inner(), now_ms())
                .map(Response::new)
        })
        .await
    }

    async fn renew_node_lease(
        &self,
        request: Request<RenewNodeLeaseRequest>,
    ) -> Result<Response<NodeLease>, Status> {
        crate::metrics::timed(Route::RenewNodeLease, request, |request| async move {
            self.membership(&request)?;
            self.admit(&request.get_ref().collection)?;
            self.plane
                .renew(request.into_inner(), now_ms())
                .map(Response::new)
        })
        .await
    }

    async fn drain_node(
        &self,
        request: Request<DrainNodeRequest>,
    ) -> Result<Response<ClusterPlan>, Status> {
        crate::metrics::timed(Route::DrainNode, request, |request| async move {
            self.membership(&request)?;
            self.admit(&request.get_ref().collection)?;
            self.plane.drain(request.into_inner(), now_ms())?;
            let (plan, _changed) = self.plane.reconcile(false, now_ms())?;
            self.publish_if_needed()?;
            Ok(Response::new(plan))
        })
        .await
    }

    async fn report_shard(
        &self,
        request: Request<ReportShardRequest>,
    ) -> Result<Response<ClusterPlan>, Status> {
        crate::metrics::timed(Route::ReportShard, request, |request| async move {
            self.membership(&request)?;
            self.admit(&request.get_ref().collection)?;
            let req = request.into_inner();
            if let Some(replica) = &req.replica {
                self.admit(&replica.collection)?;
            }
            self.plane.report(req, now_ms())?;
            let (plan, _changed) = self.plane.reconcile(false, now_ms())?;
            self.publish_if_needed()?;
            Ok(Response::new(plan))
        })
        .await
    }

    async fn complete_placement_action(
        &self,
        request: Request<CompletePlacementActionRequest>,
    ) -> Result<Response<ClusterPlan>, Status> {
        crate::metrics::timed(
            Route::CompletePlacementAction,
            request,
            |request| async move {
                self.membership(&request)?;
                self.admit(&request.get_ref().collection)?;
                self.plane.complete_action(request.into_inner(), now_ms())?;
                let (plan, _changed) = self.plane.reconcile(false, now_ms())?;
                self.publish_if_needed()?;
                Ok(Response::new(plan))
            },
        )
        .await
    }

    async fn reconcile_cluster(
        &self,
        request: Request<ReconcileClusterRequest>,
    ) -> Result<Response<ClusterPlan>, Status> {
        crate::metrics::timed(Route::ReconcileCluster, request, |request| async move {
            self.membership(&request)?;
            self.admit(&request.get_ref().collection)?;
            let request = request.into_inner();
            let (plan, _changed) = self.plane.reconcile(request.dry_run, now_ms())?;
            if !request.dry_run {
                self.publish_if_needed()?;
            }
            Ok(Response::new(plan))
        })
        .await
    }

    async fn get_cluster_plan(
        &self,
        _request: Request<GetClusterPlanRequest>,
    ) -> Result<Response<ClusterPlan>, Status> {
        crate::metrics::timed(Route::GetClusterPlan, _request, |_request| async move {
            self.membership(&_request)?;
            self.admit(&_request.get_ref().collection)?;
            self.plane.plan().map(Response::new)
        })
        .await
    }

    /// Balance dry run (`docs/bandwidth-budget.md`). Reserved: the
    /// contract is in the proto; the measured scan rate on the lease and
    /// the balancer are the next branch's work, and until then the call
    /// refuses by name rather than answering with a plan it did not make.
    async fn plan_balance(
        &self,
        request: Request<crate::pb::PlanBalanceRequest>,
    ) -> Result<Response<crate::pb::PlanBalanceResponse>, Status> {
        crate::metrics::timed(Route::PlanBalance, request, |request| async move {
            self.membership(&request)?;
            self.admit(&request.get_ref().collection)?;
            Err(Status::unimplemented(
                "plan_balance: the balance dry run is not implemented yet \
                 (docs/bandwidth-budget.md)",
            ))
        })
        .await
    }

    async fn rollback_cluster(
        &self,
        request: Request<RollbackClusterRequest>,
    ) -> Result<Response<ClusterPlan>, Status> {
        crate::metrics::timed(Route::RollbackCluster, request, |request| async move {
            self.membership(&request)?;
            self.admit(&request.get_ref().collection)?;
            let (plan, _changed) = self
                .plane
                .rollback(request.into_inner().topology_generation)?;
            self.publish_if_needed()?;
            Ok(Response::new(plan))
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn register(
        plane: &DurableControlPlane,
        id: &str,
        addr: &str,
        domain: &str,
        now: u64,
    ) -> NodeLease {
        plane
            .register(
                RegisterNodeRequest {
                    collection: String::new(),
                    node_id: id.into(),
                    addr: addr.into(),
                    capacity: Some(NodeCapacity {
                        disk_bytes: 1_000,
                        failure_domain: domain.into(),
                        ..Default::default()
                    }),
                    lease_ms: 10_000,
                },
                now,
            )
            .unwrap()
    }

    fn report(
        plane: &DurableControlPlane,
        lease: &NodeLease,
        shard: &str,
        role: ShardReplicaRole,
        rows: u64,
        segments: u32,
        now: u64,
    ) {
        report_range(plane, lease, shard, role, rows, segments, 0, u64::MAX, now);
    }

    #[allow(clippy::too_many_arguments)]
    fn report_range(
        plane: &DurableControlPlane,
        lease: &NodeLease,
        shard: &str,
        role: ShardReplicaRole,
        rows: u64,
        segments: u32,
        hash_lo: u64,
        hash_hi: u64,
        now: u64,
    ) {
        plane
            .report(
                ReportShardRequest {
                    collection: String::new(),
                    node_id: lease.node_id.clone(),
                    lease_token: lease.lease_token,
                    replica: Some(ShardReplicaState {
                        shard_id: shard.into(),
                        generation: 7,
                        hash_lo,
                        hash_hi,
                        rows,
                        role: role as i32,
                        ready: true,
                        scoring_fingerprint: "score-v1".into(),
                        analysis_fingerprint: "analysis-v1".into(),
                        immutable_segments: segments,
                        ..Default::default()
                    }),
                },
                now,
            )
            .unwrap();
    }

    fn action_output(
        shard_id: &str,
        generation: u64,
        hash_lo: u64,
        hash_hi: u64,
        rows: u64,
    ) -> ShardReplicaState {
        ShardReplicaState {
            shard_id: shard_id.into(),
            generation,
            hash_lo,
            hash_hi,
            rows,
            role: ShardReplicaRole::Primary as i32,
            ready: true,
            scoring_fingerprint: "score-v1".into(),
            analysis_fingerprint: "analysis-v1".into(),
            immutable_segments: 1,
            ..Default::default()
        }
    }

    #[test]
    fn lease_expiry_promotes_ready_replica_and_preserves_history() {
        let plane = DurableControlPlane::in_memory(ControlPolicy {
            lease_ms: 10_000,
            replication_factor: 2,
            ..Default::default()
        });
        let primary = register(&plane, "a", "http://a", "az-a", 100);
        let replica = register(&plane, "b", "http://b", "az-b", 100);
        report(
            &plane,
            &primary,
            "s0",
            ShardReplicaRole::Primary,
            10,
            1,
            100,
        );
        report(
            &plane,
            &replica,
            "s0",
            ShardReplicaRole::Replica,
            10,
            1,
            100,
        );
        let (first, changed) = plane.reconcile(false, 100).unwrap();
        assert!(changed);
        assert_eq!(first.topology_generation, 1);
        plane
            .renew(
                RenewNodeLeaseRequest {
                    node_id: replica.node_id.clone(),
                    lease_token: replica.lease_token,
                    lease_ms: 10_000,
                    ..Default::default()
                },
                5_000,
            )
            .unwrap();

        let (promoted, changed) = plane.reconcile(false, 10_101).unwrap();
        assert!(changed);
        assert_eq!(promoted.topology_generation, 2);
        assert_eq!(promoted.topology_history, vec![0, 1]);
        assert_eq!(
            promoted
                .replicas
                .iter()
                .find(|held| held.node_id == "b")
                .unwrap()
                .role,
            ShardReplicaRole::Primary as i32
        );
    }

    #[test]
    fn drain_places_a_copy_across_failure_domains_without_a_competing_rewrite() {
        let plane = DurableControlPlane::in_memory(ControlPolicy {
            replication_factor: 2,
            compact_segments: 4,
            ..Default::default()
        });
        let primary = register(&plane, "a", "http://a", "az-a", 100);
        let target = register(&plane, "b", "http://b", "az-b", 100);
        report(
            &plane,
            &primary,
            "s0",
            ShardReplicaRole::Primary,
            10,
            5,
            100,
        );
        plane
            .drain(
                DrainNodeRequest {
                    collection: String::new(),
                    node_id: primary.node_id.clone(),
                    lease_token: primary.lease_token,
                },
                100,
            )
            .unwrap();
        let (plan, _) = plane.reconcile(false, 100).unwrap();
        assert!(plan.actions.iter().any(|action| {
            action.kind == PlacementActionKind::CopyReplica as i32
                && action.target_node_id == target.node_id
        }));
        assert!(!plan
            .actions
            .iter()
            .any(|action| { action.kind == PlacementActionKind::CompactShard as i32 }));
        let count = plan.actions.len();
        let (again, _) = plane.reconcile(false, 100).unwrap();
        assert_eq!(again.actions.len(), count);

        let copy = plan
            .actions
            .iter()
            .find(|action| action.kind == PlacementActionKind::CopyReplica as i32)
            .unwrap();
        let mut replica = action_output("s0", 7, 0, u64::MAX, 10);
        replica.role = ShardReplicaRole::Replica as i32;
        plane
            .complete_action(
                CompletePlacementActionRequest {
                    collection: String::new(),
                    node_id: target.node_id.clone(),
                    lease_token: target.lease_token,
                    action_id: copy.action_id,
                    outputs: vec![replica],
                },
                100,
            )
            .unwrap();
        let drop_action = plane
            .plan()
            .unwrap()
            .actions
            .into_iter()
            .find(|action| action.kind == PlacementActionKind::DropReplica as i32)
            .unwrap();
        plane
            .complete_action(
                CompletePlacementActionRequest {
                    collection: String::new(),
                    node_id: primary.node_id.clone(),
                    lease_token: primary.lease_token,
                    action_id: drop_action.action_id,
                    outputs: Vec::new(),
                },
                100,
            )
            .unwrap();
        let (published, changed) = plane.reconcile(false, 100).unwrap();
        assert!(changed);
        assert_eq!(published.topology_generation, 1);
        assert_eq!(published.replicas.len(), 1);
        assert_eq!(published.replicas[0].node_id, target.node_id);
        assert_eq!(published.replicas[0].role, ShardReplicaRole::Primary as i32);
    }

    #[test]
    fn a_report_names_its_own_listener_but_not_another_nodes_address() {
        let plane = DurableControlPlane::in_memory(ControlPolicy {
            replication_factor: 1,
            ..Default::default()
        });
        let a = register(&plane, "a", "http://a:1", "az-a", 100);
        let _b = register(&plane, "b", "http://b:1", "az-b", 100);
        let report = |addr: &str| ReportShardRequest {
            collection: String::new(),
            node_id: a.node_id.clone(),
            lease_token: a.lease_token,
            replica: Some(ShardReplicaState {
                shard_id: "s0".into(),
                addr: addr.into(),
                hash_hi: u64::MAX,
                rows: 10,
                role: ShardReplicaRole::Primary as i32,
                ready: true,
                scoring_fingerprint: "score-v1".into(),
                analysis_fingerprint: "analysis-v1".into(),
                ..Default::default()
            }),
        };
        // A second listener of the same node is the shard's address.
        plane.report(report("http://a:2"), 100).unwrap();
        let (plan, _) = plane.reconcile(false, 100).unwrap();
        assert_eq!(plan.replicas[0].addr, "http://a:2");
        assert_eq!(plan.topology.len(), 1);
        assert_eq!(plan.topology[0].addr, "http://a:2");
        assert_eq!(
            (plan.topology[0].hash_lo, plan.topology[0].hash_hi),
            (0, u64::MAX)
        );
        // Empty falls back to the registered address.
        plane.report(report(""), 100).unwrap();
        assert_eq!(plane.plan().unwrap().replicas[0].addr, "http://a:1");
        // Another node's registered address is not a listener of a.
        let error = plane.report(report("http://b:1"), 100).unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert!(
            error.message().contains("registered address of node \"b\""),
            "{}",
            error.message()
        );
    }

    #[test]
    fn active_compaction_action_is_idempotent() {
        let plane = DurableControlPlane::in_memory(ControlPolicy {
            replication_factor: 1,
            compact_segments: 4,
            split_rows: u64::MAX,
            ..Default::default()
        });
        let primary = register(&plane, "a", "http://a", "az-a", 100);
        report(
            &plane,
            &primary,
            "s0",
            ShardReplicaRole::Primary,
            10,
            5,
            100,
        );
        let (plan, _) = plane.reconcile(false, 100).unwrap();
        assert_eq!(
            plan.actions
                .iter()
                .filter(|action| action.kind == PlacementActionKind::CompactShard as i32)
                .count(),
            1
        );
        let (again, _) = plane.reconcile(false, 100).unwrap();
        assert_eq!(again.actions.len(), plan.actions.len());
    }

    #[test]
    fn durable_state_reopens_and_dry_run_does_not_mutate() {
        let dir = std::env::temp_dir().join(format!(
            "protomolt-control-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let path = dir.join("state.json");
        let plane = DurableControlPlane::open(&path, ControlPolicy::default()).unwrap();
        register(&plane, "a", "http://a", "az-a", 100);
        let revision = plane.plan().unwrap().control_revision;
        let _ = plane.reconcile(true, 20_000).unwrap();
        assert_eq!(plane.plan().unwrap().control_revision, revision);
        drop(plane);
        let reopened = DurableControlPlane::open(&path, ControlPolicy::default()).unwrap();
        assert_eq!(reopened.plan().unwrap().nodes.len(), 1);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn pristine_state_bootstraps_the_live_topology_without_renumbering_it() {
        let plane = DurableControlPlane::in_memory(ControlPolicy {
            replication_factor: 1,
            ..Default::default()
        });
        let routes = vec![TopologyRoute {
            addr: "http://a".into(),
            replica: None,
            hash_range: Some((0, u64::MAX)),
            placement: None,
        }];
        plane.bootstrap_topology(4, &routes).unwrap();
        let node = register(&plane, "a", "http://a", "az-a", 100);
        report(&plane, &node, "s0", ShardReplicaRole::Primary, 10, 1, 100);
        let (plan, changed) = plane.reconcile(false, 100).unwrap();
        assert!(!changed);
        assert_eq!(plan.topology_generation, 4);

        let coordinator = CoordinatorServiceImpl::new(vec!["http://a".into()])
            .with_topology_generation(4)
            .with_hot_topology(vec![Some((0, u64::MAX))])
            .unwrap();
        ClusterControlService::new(plane)
            .with_coordinator(coordinator)
            .publish_current_topology()
            .unwrap();
    }

    #[test]
    fn bootstrap_requires_ranges_and_never_overwrites_durable_authority() {
        let plane = DurableControlPlane::in_memory(ControlPolicy::default());
        let error = plane
            .bootstrap_topology(
                1,
                &[TopologyRoute {
                    addr: "http://a".into(),
                    replica: None,
                    hash_range: None,
                    placement: None,
                }],
            )
            .unwrap_err();
        assert!(error.contains("missing its hash range"));

        plane
            .bootstrap_topology(
                4,
                &[TopologyRoute {
                    addr: "http://a".into(),
                    replica: None,
                    hash_range: Some((0, u64::MAX)),
                    placement: None,
                }],
            )
            .unwrap();
        plane
            .bootstrap_topology(
                5,
                &[TopologyRoute {
                    addr: "http://b".into(),
                    replica: None,
                    hash_range: Some((0, u64::MAX)),
                    placement: None,
                }],
            )
            .unwrap();
        let (generation, routes) = plane.topology_routes().unwrap();
        assert_eq!(generation, 4);
        assert_eq!(routes[0].addr, "http://a");
    }

    #[test]
    fn split_completion_conserves_rows_and_atomically_replaces_topology() {
        let plane = DurableControlPlane::in_memory(ControlPolicy {
            replication_factor: 1,
            split_rows: 100,
            merge_rows: 0,
            compact_segments: u32::MAX,
            compact_tombstone_ppm: 1_000_000,
            ..Default::default()
        });
        let node = register(&plane, "a", "http://a", "az-a", 100);
        report(
            &plane,
            &node,
            "parent",
            ShardReplicaRole::Primary,
            101,
            1,
            100,
        );
        let (initial, _) = plane.reconcile(false, 100).unwrap();
        let action = initial
            .actions
            .iter()
            .find(|action| action.kind == PlacementActionKind::SplitShard as i32)
            .unwrap()
            .clone();
        let midpoint = u64::MAX / 2;
        let invalid = CompletePlacementActionRequest {
            collection: String::new(),
            node_id: node.node_id.clone(),
            lease_token: node.lease_token,
            action_id: action.action_id,
            outputs: vec![
                action_output("left", 8, 0, midpoint, 50),
                action_output("right", 8, midpoint + 1, u64::MAX, 50),
            ],
        };
        let error = plane.complete_action(invalid, 100).unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(plane
            .plan()
            .unwrap()
            .actions
            .iter()
            .any(|held| held.action_id == action.action_id));

        let completion = CompletePlacementActionRequest {
            collection: String::new(),
            node_id: node.node_id.clone(),
            lease_token: node.lease_token,
            action_id: action.action_id,
            outputs: vec![
                action_output("left", 8, 0, midpoint, 50),
                action_output("right", 8, midpoint + 1, u64::MAX, 51),
            ],
        };
        plane.complete_action(completion.clone(), 100).unwrap();
        // Crash-window retries are acknowledgements, not duplicate mutations.
        plane.complete_action(completion, 100).unwrap();
        let (published, changed) = plane.reconcile(false, 100).unwrap();
        assert!(changed);
        assert_eq!(published.topology_generation, 2);
        assert_eq!(published.replicas.len(), 2);
        assert!(published
            .replicas
            .iter()
            .all(|replica| replica.shard_id != "parent"));
    }

    #[test]
    fn merge_completion_replaces_both_adjacent_inputs() {
        let plane = DurableControlPlane::in_memory(ControlPolicy {
            replication_factor: 1,
            split_rows: u64::MAX,
            merge_rows: 100,
            compact_segments: u32::MAX,
            compact_tombstone_ppm: 1_000_000,
            ..Default::default()
        });
        let node = register(&plane, "a", "http://a", "az-a", 100);
        let midpoint = u64::MAX / 2;
        report_range(
            &plane,
            &node,
            "left",
            ShardReplicaRole::Primary,
            10,
            1,
            0,
            midpoint,
            100,
        );
        report_range(
            &plane,
            &node,
            "right",
            ShardReplicaRole::Primary,
            20,
            1,
            midpoint + 1,
            u64::MAX,
            100,
        );
        let (initial, _) = plane.reconcile(false, 100).unwrap();
        let action = initial
            .actions
            .iter()
            .find(|action| action.kind == PlacementActionKind::MergeShards as i32)
            .unwrap();
        plane
            .complete_action(
                CompletePlacementActionRequest {
                    collection: String::new(),
                    node_id: node.node_id.clone(),
                    lease_token: node.lease_token,
                    action_id: action.action_id,
                    outputs: vec![action_output("merged", 8, 0, u64::MAX, 30)],
                },
                100,
            )
            .unwrap();
        let (published, changed) = plane.reconcile(false, 100).unwrap();
        assert!(changed);
        assert_eq!(published.replicas.len(), 1);
        assert_eq!(published.replicas[0].shard_id, "merged");
        assert_eq!(published.topology_generation, 2);
    }
}

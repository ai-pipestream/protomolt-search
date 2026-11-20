//! The durable control state as stored: nodes, replicas, routes, and actions.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::pb::{NodeCapacity, PlacementActionKind};

use super::storage;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub(super) enum StoredNodeState {
    Active,
    Draining,
    Expired,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub(super) struct StoredCapacity {
    pub(super) disk_bytes: u64,
    pub(super) used_disk_bytes: u64,
    pub(super) memory_bytes: u64,
    pub(super) search_threads: u32,
    pub(super) failure_domain: String,
    /// Absent in state files written before the fields existed.
    #[serde(default)]
    pub(super) scan_bytes_per_second: u64,
    #[serde(default)]
    pub(super) scan_rate_observed_unix_ms: u64,
    #[serde(default)]
    pub(super) scan_rate_samples: u32,
    #[serde(default)]
    pub(super) scan_rate_window_ms: u64,
    /// `NodeResidency` as its wire number; 0 is unspecified.
    #[serde(default)]
    pub(super) residency: i32,
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
pub(super) struct StoredNode {
    pub(super) node_id: String,
    pub(super) addr: String,
    pub(super) state: StoredNodeState,
    pub(super) lease_token: u64,
    pub(super) expires_unix_ms: u64,
    pub(super) capacity: StoredCapacity,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub(super) enum StoredRole {
    Primary,
    Replica,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StoredReplica {
    pub(super) shard_id: String,
    pub(super) node_id: String,
    pub(super) addr: String,
    pub(super) generation: u64,
    pub(super) hash_lo: u64,
    pub(super) hash_hi: u64,
    pub(super) slot_offset: u64,
    pub(super) rows: u64,
    pub(super) bytes: u64,
    pub(super) role: StoredRole,
    pub(super) ready: bool,
    pub(super) scoring_fingerprint: String,
    pub(super) analysis_fingerprint: String,
    pub(super) immutable_segments: u32,
    pub(super) tombstones: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct StoredRoute {
    pub(super) addr: String,
    pub(super) replica: Option<String>,
    pub(super) hash_lo: u64,
    pub(super) hash_hi: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StoredTopology {
    pub(super) generation: u64,
    pub(super) routes: Vec<StoredRoute>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StoredAction {
    pub(super) action_id: u64,
    pub(super) kind: i32,
    pub(super) shard_id: String,
    pub(super) peer_shard_id: String,
    #[serde(default)]
    pub(super) peer_source_generation: u64,
    pub(super) source_node_id: String,
    pub(super) target_node_id: String,
    pub(super) source_generation: u64,
    pub(super) target_generation: u64,
    pub(super) hash_lo: u64,
    pub(super) hash_hi: u64,
    pub(super) reason: String,
}

pub(super) struct ActionSpec<'a> {
    pub(super) kind: PlacementActionKind,
    pub(super) peer_shard_id: String,
    pub(super) peer_source_generation: u64,
    pub(super) target_node_id: String,
    pub(super) target_generation: u64,
    pub(super) reason: &'a str,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StoredState {
    pub(super) format: u32,
    /// The collection this plane governs; empty in
    /// state written before collections, written on the first open under
    /// a named collection.
    #[serde(default)]
    pub(super) collection: String,
    pub(super) revision: u64,
    pub(super) next_token: u64,
    pub(super) next_action: u64,
    pub(super) topology: StoredTopology,
    pub(super) history: Vec<StoredTopology>,
    #[serde(deserialize_with = "storage::unique_map")]
    pub(super) nodes: BTreeMap<String, StoredNode>,
    #[serde(deserialize_with = "storage::unique_map")]
    pub(super) replicas: BTreeMap<String, StoredReplica>,
    pub(super) actions: Vec<StoredAction>,
    #[serde(default, deserialize_with = "storage::unique_set")]
    pub(super) completed_actions: BTreeSet<u64>,
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

pub(super) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

pub(super) fn replica_key(shard_id: &str, node_id: &str) -> String {
    format!("{shard_id}\0{node_id}")
}

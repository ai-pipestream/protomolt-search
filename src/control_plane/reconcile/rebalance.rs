//! Whole-cluster actions: merging small neighbours and rebalancing disk.

use tonic::Status;

use crate::control_plane::state::{
    ActionSpec, StoredNode, StoredNodeState, StoredReplica, StoredRole, StoredState,
};
use crate::control_plane::DurableControlPlane;
use crate::pb::PlacementActionKind;

/// A node's disk utilization in parts per million.
fn utilization(node: &StoredNode) -> u64 {
    node.capacity.used_disk_bytes.saturating_mul(1_000_000) / node.capacity.disk_bytes.max(1)
}

impl DurableControlPlane {
    /// Merge only adjacent, small primaries. One action covers the pair.
    pub(super) fn queue_merges(&self, state: &mut StoredState) -> Result<(), Status> {
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
        Ok(())
    }

    /// Rebalance one primary at a time when active-node disk utilization
    /// differs by more than 15 percentage points. Copy first; the ready
    /// report promotes the target and emits a drop action for the source.
    pub(super) fn queue_rebalance(state: &mut StoredState) -> Result<(), Status> {
        let Some((high, low)) = Self::utilization_extremes(state) else {
            return Ok(());
        };
        let candidate = state
            .replicas
            .values()
            .filter(|replica| {
                replica.role == StoredRole::Primary && replica.ready && replica.node_id == high
            })
            .filter(|replica| {
                !state
                    .replicas
                    .values()
                    .any(|held| held.shard_id == replica.shard_id && held.node_id == low)
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
                    target_node_id: low,
                    target_generation: candidate.generation,
                    reason: "capacity rebalance",
                },
            )?;
        }
        Ok(())
    }

    /// The most and least utilized active nodes, when they are distinct
    /// nodes at least 15 percentage points apart.
    fn utilization_extremes(state: &StoredState) -> Option<(String, String)> {
        let active: Vec<&StoredNode> = state
            .nodes
            .values()
            .filter(|node| node.state == StoredNodeState::Active)
            .collect();
        let high = active
            .iter()
            .copied()
            .max_by_key(|node| utilization(node))?;
        let low = active
            .iter()
            .copied()
            .min_by_key(|node| utilization(node))?;
        (high.node_id != low.node_id
            && utilization(high).saturating_sub(utilization(low)) >= 150_000)
            .then(|| (high.node_id.clone(), low.node_id.clone()))
    }
}

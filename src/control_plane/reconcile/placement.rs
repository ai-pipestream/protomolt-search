//! Per-shard reconciliation: promotion, replica copies, and rewrites.

use std::collections::BTreeSet;

use tonic::Status;

use crate::control_plane::state::{
    ActionSpec, StoredAction, StoredNodeState, StoredReplica, StoredRole, StoredState,
};
use crate::control_plane::DurableControlPlane;
use crate::pb::PlacementActionKind;

impl DurableControlPlane {
    pub(super) fn active_node(state: &StoredState, node_id: &str) -> bool {
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

    pub(in crate::control_plane) fn push_action(
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

    /// Reconcile one shard: restore a primary, then queue the copy or
    /// rewrite its ready primary needs.
    pub(super) fn reconcile_shard(
        &self,
        state: &mut StoredState,
        shard_id: &str,
    ) -> Result<(), Status> {
        Self::promote_ready_replica(state, shard_id);
        let primary = state
            .replicas
            .values()
            .find(|replica| {
                replica.shard_id == shard_id && replica.role == StoredRole::Primary && replica.ready
            })
            .cloned();
        let Some(primary) = primary else {
            return Ok(());
        };
        let primary_state = state
            .nodes
            .get(&primary.node_id)
            .map(|node| node.state)
            .unwrap_or(StoredNodeState::Expired);
        self.queue_copy(state, &primary, primary_state)?;
        self.queue_rewrite(state, &primary, primary_state)
    }

    /// When the shard's ready primary is missing or its node is not
    /// active, the ready replica on an active node with the highest
    /// generation becomes the primary and every other copy a replica.
    fn promote_ready_replica(state: &mut StoredState, shard_id: &str) {
        let replicas: Vec<StoredReplica> = state
            .replicas
            .values()
            .filter(|replica| replica.shard_id == shard_id && replica.ready)
            .cloned()
            .collect();
        let primary_available = replicas
            .iter()
            .find(|replica| replica.role == StoredRole::Primary)
            .is_some_and(|replica| Self::active_node(state, &replica.node_id));
        if primary_available {
            return;
        }
        let Some(promote) = replicas
            .iter()
            .filter(|replica| replica.role == StoredRole::Replica)
            .filter(|replica| Self::active_node(state, &replica.node_id))
            .max_by_key(|replica| replica.generation)
        else {
            return;
        };
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

    /// Queue a replica copy when the shard is under its replication
    /// factor or its primary's node is draining or expired.
    fn queue_copy(
        &self,
        state: &mut StoredState,
        primary: &StoredReplica,
        primary_state: StoredNodeState,
    ) -> Result<(), Status> {
        let copy_nodes: Vec<String> = state
            .replicas
            .values()
            .filter(|replica| {
                replica.shard_id == primary.shard_id
                    && replica.ready
                    && replica.generation == primary.generation
                    && replica.scoring_fingerprint == primary.scoring_fingerprint
                    && replica.analysis_fingerprint == primary.analysis_fingerprint
            })
            .map(|replica| replica.node_id.clone())
            .collect();
        let need_copy = copy_nodes.len() < self.policy.replication_factor
            || primary_state != StoredNodeState::Active;
        if !need_copy {
            return Ok(());
        }
        let excluded: BTreeSet<String> = copy_nodes.into_iter().collect();
        let source_domain = state
            .nodes
            .get(&primary.node_id)
            .map(|node| node.capacity.failure_domain.as_str())
            .unwrap_or("");
        let Some(target) = Self::choose_target(state, &excluded, source_domain) else {
            return Ok(());
        };
        Self::push_action(
            state,
            primary,
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
        )
    }

    /// Queue a split or a compaction for a shard whose primary is active
    /// and has no copy pending: a rewrite never competes with a copy.
    fn queue_rewrite(
        &self,
        state: &mut StoredState,
        primary: &StoredReplica,
        primary_state: StoredNodeState,
    ) -> Result<(), Status> {
        let copy_pending = state.actions.iter().any(|action| {
            action.shard_id == primary.shard_id
                && action.kind == PlacementActionKind::CopyReplica as i32
        });
        if primary_state != StoredNodeState::Active || copy_pending {
            return Ok(());
        }
        let (kind, reason) =
            if primary.rows > self.policy.split_rows && primary.hash_lo < primary.hash_hi {
                (
                    PlacementActionKind::SplitShard,
                    "capacity-aware automatic split",
                )
            } else if primary.immutable_segments >= self.policy.compact_segments
                || (primary.rows > 0
                    && primary.tombstones.saturating_mul(1_000_000)
                        >= primary
                            .rows
                            .saturating_mul(u64::from(self.policy.compact_tombstone_ppm)))
            {
                (
                    PlacementActionKind::CompactShard,
                    "segment or tombstone compaction threshold",
                )
            } else {
                return Ok(());
            };
        let Some(target_generation) = primary.generation.checked_add(1) else {
            return Ok(());
        };
        Self::push_action(
            state,
            primary,
            ActionSpec {
                kind,
                peer_shard_id: String::new(),
                peer_source_generation: 0,
                target_node_id: primary.node_id.clone(),
                target_generation,
                reason,
            },
        )
    }
}

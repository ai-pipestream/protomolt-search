//! Per-kind state changes of a completed placement action.

use tonic::Status;

use crate::pb::PlacementActionKind;

use super::super::state::{
    replica_key, ActionSpec, StoredAction, StoredNodeState, StoredReplica, StoredRole, StoredState,
};
use super::super::DurableControlPlane;

/// One pending action as its completing node reported it.
pub(super) struct Completion<'a> {
    pub(super) action: &'a StoredAction,
    pub(super) source: &'a StoredReplica,
    /// The node completing the action.
    pub(super) node_id: &'a str,
}

impl DurableControlPlane {
    /// Apply one completed action to the state. On error the caller restores
    /// the state it cloned beforehand.
    pub(super) fn apply_completion(
        state: &mut StoredState,
        kind: PlacementActionKind,
        completion: &Completion<'_>,
        outputs: Vec<StoredReplica>,
    ) -> Result<(), Status> {
        match kind {
            PlacementActionKind::CopyReplica => Self::complete_copy(state, completion, &outputs),
            PlacementActionKind::DropReplica => Self::complete_drop(state, completion, &outputs),
            PlacementActionKind::PromoteReplica => {
                Self::complete_promote(state, completion, &outputs)
            }
            PlacementActionKind::CompactShard => {
                Self::complete_compact(state, completion, &outputs)
            }
            PlacementActionKind::SplitShard => Self::complete_split(state, completion, outputs),
            PlacementActionKind::MergeShards => Self::complete_merge(state, completion, &outputs),
            PlacementActionKind::Unspecified => Err(Status::failed_precondition(
                "cannot complete an unspecified placement action",
            )),
        }
    }

    /// Record a ready copy; promote it and drop the source when the source is leaving.
    fn complete_copy(
        state: &mut StoredState,
        completion: &Completion<'_>,
        outputs: &[StoredReplica],
    ) -> Result<(), Status> {
        let source = completion.source;
        let [output] = outputs else {
            return Err(Status::invalid_argument(
                "COPY_REPLICA completion needs exactly one output",
            ));
        };
        Self::require_same_contract(output, source, completion.action.target_generation)?;
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
            || completion.action.reason == "capacity rebalance"
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
                state,
                source,
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
        Ok(())
    }

    /// Forget the dropped replica.
    fn complete_drop(
        state: &mut StoredState,
        completion: &Completion<'_>,
        outputs: &[StoredReplica],
    ) -> Result<(), Status> {
        let source = completion.source;
        if !outputs.is_empty() {
            return Err(Status::invalid_argument(
                "DROP_REPLICA completion must not carry outputs",
            ));
        }
        state
            .replicas
            .remove(&replica_key(&source.shard_id, &source.node_id));
        Ok(())
    }

    /// Make the completing node the shard's primary.
    fn complete_promote(
        state: &mut StoredState,
        completion: &Completion<'_>,
        outputs: &[StoredReplica],
    ) -> Result<(), Status> {
        let source = completion.source;
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
            held.role = if held.node_id == completion.node_id {
                StoredRole::Primary
            } else {
                StoredRole::Replica
            };
        }
        Ok(())
    }

    /// Replace the shard's copies with its compacted primary.
    fn complete_compact(
        state: &mut StoredState,
        completion: &Completion<'_>,
        outputs: &[StoredReplica],
    ) -> Result<(), Status> {
        let source = completion.source;
        let [output] = outputs else {
            return Err(Status::invalid_argument(
                "COMPACT_SHARD completion needs exactly one output",
            ));
        };
        Self::require_same_contract(output, source, completion.action.target_generation)?;
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
        Ok(())
    }

    /// Replace the source with dense children that tile its range.
    fn complete_split(
        state: &mut StoredState,
        completion: &Completion<'_>,
        outputs: Vec<StoredReplica>,
    ) -> Result<(), Status> {
        let source = completion.source;
        if outputs.len() < 2 {
            return Err(Status::invalid_argument(
                "SPLIT_SHARD completion needs at least two children",
            ));
        }
        Self::require_range_tiling(&outputs, source.hash_lo, source.hash_hi)?;
        let mut rows = 0u64;
        for output in &outputs {
            Self::require_same_contract(output, source, completion.action.target_generation)?;
            if output.role != StoredRole::Primary || output.tombstones != 0 {
                return Err(Status::failed_precondition(
                    "split children must be dense primaries",
                ));
            }
            rows = rows
                .checked_add(output.rows)
                .ok_or_else(|| Status::resource_exhausted("split output row count overflow"))?;
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
        Ok(())
    }

    /// Replace two adjacent shards with their merged primary.
    fn complete_merge(
        state: &mut StoredState,
        completion: &Completion<'_>,
        outputs: &[StoredReplica],
    ) -> Result<(), Status> {
        let source = completion.source;
        let [output] = outputs else {
            return Err(Status::invalid_argument(
                "MERGE_SHARDS completion needs exactly one output",
            ));
        };
        let peer = state
            .replicas
            .values()
            .find(|replica| {
                replica.shard_id == completion.action.peer_shard_id
                    && replica.role == StoredRole::Primary
            })
            .cloned()
            .ok_or_else(|| Status::failed_precondition("merge peer is missing"))?;
        if peer.generation != completion.action.peer_source_generation {
            return Err(Status::failed_precondition(
                "merge peer generation changed after planning",
            ));
        }
        Self::require_same_contract(output, source, completion.action.target_generation)?;
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
                    .ok_or_else(|| Status::resource_exhausted("merge live row count overflow"))?
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
        state
            .replicas
            .retain(|_, held| held.shard_id != source.shard_id && held.shard_id != peer.shard_id);
        state.replicas.insert(
            replica_key(&output.shard_id, &output.node_id),
            output.clone(),
        );
        Ok(())
    }
}

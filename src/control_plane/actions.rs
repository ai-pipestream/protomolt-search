//! Completing a placement action against the outputs a node reports.

mod apply;

use std::collections::BTreeSet;

use tonic::Status;

use crate::pb::{
    CompletePlacementActionRequest, PlacementActionKind, ShardReplicaRole, ShardReplicaState,
};

use super::state::{replica_key, StoredAction, StoredReplica, StoredRole, StoredState};
use super::DurableControlPlane;

use self::apply::Completion;

impl DurableControlPlane {
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

    pub(super) fn complete_action(
        &self,
        request: CompletePlacementActionRequest,
        now: u64,
    ) -> Result<(), Status> {
        let mut state = self.lock_state()?;
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
        let source = Self::action_source(&state, &action)?;
        let outputs = Self::checked_outputs(&state, &request.node_id, request.outputs)?;

        let before = state.clone();
        let completion = Completion {
            action: &action,
            source: &source,
            node_id: &request.node_id,
        };
        if let Err(error) = Self::apply_completion(&mut state, kind, &completion, outputs) {
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

    /// The replica the action reads from: the planned source, else the
    /// shard's primary, at the generation the plan saw.
    fn action_source(state: &StoredState, action: &StoredAction) -> Result<StoredReplica, Status> {
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
        Ok(source)
    }

    fn checked_outputs(
        state: &StoredState,
        target_node_id: &str,
        reported: Vec<ShardReplicaState>,
    ) -> Result<Vec<StoredReplica>, Status> {
        let mut output_ids = BTreeSet::new();
        let outputs = reported
            .into_iter()
            .map(|output| Self::checked_output(state, target_node_id, output))
            .collect::<Result<Vec<_>, _>>()?;
        if outputs
            .iter()
            .any(|output| !output_ids.insert(output.shard_id.clone()))
        {
            return Err(Status::invalid_argument(
                "placement action repeats an output shard_id",
            ));
        }
        Ok(outputs)
    }
}

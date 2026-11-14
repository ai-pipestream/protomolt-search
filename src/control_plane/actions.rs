//! Completing a placement action against the outputs a node reports.

use std::collections::BTreeSet;

use tonic::Status;

use crate::pb::{
    CompletePlacementActionRequest, PlacementActionKind, ShardReplicaRole, ShardReplicaState,
};

use super::state::{
    replica_key, ActionSpec, StoredNodeState, StoredReplica, StoredRole, StoredState,
};
use super::DurableControlPlane;

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
}

//! Reconciliation: derive the desired routes and queue the actions toward them.

use std::collections::{BTreeMap, BTreeSet};

use tonic::Status;

use crate::pb::{ClusterPlan, PlacementActionKind};

use super::state::{
    ActionSpec, StoredAction, StoredNode, StoredNodeState, StoredReplica, StoredRole, StoredRoute,
    StoredState, StoredTopology,
};
use super::DurableControlPlane;

impl DurableControlPlane {
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

    pub(super) fn push_action(
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

    pub(super) fn reconcile(&self, dry_run: bool, now: u64) -> Result<(ClusterPlan, bool), Status> {
        let mut state = self.lock_state()?;
        let mut candidate = state.clone();
        let changed = self.reconcile_locked(&mut candidate, now)?;
        let plan = Self::plan_of(&candidate);
        if !dry_run {
            self.persist_locked(&candidate).map_err(Status::internal)?;
            *state = candidate;
        }
        Ok((plan, changed))
    }

    pub(super) fn rollback(&self, requested: u64) -> Result<(ClusterPlan, bool), Status> {
        let mut state = self.lock_state()?;
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
}

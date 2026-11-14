//! Action completion, target choice, reconciliation, rollback, and the
//! plan and topology views of the durable state.

use super::*;

impl DurableControlPlane {
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

    pub(super) fn plan(&self) -> Result<ClusterPlan, Status> {
        let state = self.lock_state()?;
        Ok(Self::plan_of(&state))
    }

    /// The balance dry run over the durable state; see [`plan_balance`].
    pub fn plan_balance(
        &self,
        request: &PlanBalanceRequest,
        row_bytes: u64,
        pool_of: &dyn Fn(&str) -> BalancePool,
        now: u64,
    ) -> Result<PlanBalanceResponse, Status> {
        let state = self.lock_state()?;
        plan_balance(&state, request, row_bytes, pool_of, now)
    }

    #[cfg(test)]
    pub(super) fn topology_routes(&self) -> Result<(u64, Vec<TopologyRoute>), Status> {
        let state = self.lock_state()?;
        Ok(Self::topology_routes_of(&state))
    }

    pub(super) fn topology_routes_of(state: &StoredState) -> (u64, Vec<TopologyRoute>) {
        (
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
        )
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

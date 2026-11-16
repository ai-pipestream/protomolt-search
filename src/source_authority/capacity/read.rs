//! Authorized reads of capacity state and the planner's input.
use super::*;

impl SourceAuthorityStore {
    pub fn capacity_state(
        &self,
        principal: &str,
        key: &LogicalSourceOwner,
    ) -> Result<CapacityState, Status> {
        self.guarded(|| {
            contract::key(key, false)?;
            let tx = self.inner.database().begin_read().map_err(storage)?;
            self.read_policy(&tx, principal, key)?;
            let states = tx.open_table(CAPACITY).map_err(storage)?;
            contract::decode(
                states
                    .get(state_key(key).as_slice())
                    .map_err(storage)?
                    .ok_or_else(|| Status::not_found("resource has no capacity configuration"))?
                    .value(),
            )
        })
    }

    pub fn capacity_configure_decision(
        &self,
        principal: &str,
        key: &LogicalSourceOwner,
        command_id: &[u8],
    ) -> Result<CapacityConfigureDecision, Status> {
        self.guarded(|| {
            contract::operation_id(command_id)?;
            let tx = self.inner.database().begin_read().map_err(storage)?;
            self.read_policy(&tx, principal, key)?;
            let operations = tx.open_table(CAPACITY_OPERATIONS).map_err(storage)?;
            let bytes = contract::operation_key(principal, key, command_id).encode_to_vec();
            let operation: CapacityConfigureOperation = contract::decode(
                operations
                    .get(bytes.as_slice())
                    .map_err(storage)?
                    .ok_or_else(|| Status::not_found("capacity command has no decision"))?
                    .value(),
            )?;
            operation.decision.ok_or_else(|| missing("decision"))
        })
    }

    /// One consistent read of everything the planner binds to, transcribed
    /// into the planner's input. Nothing is validated or repaired here beyond
    /// the committed rows' own integrity; `TierSnapshot::validated` decides.
    pub fn planner_input(
        &self,
        principal: &str,
        key: &LogicalSourceOwner,
        context: &PlanningContext,
    ) -> Result<tiers::TierSnapshotInput, Status> {
        self.guarded(|| {
            contract::key(key, false)?;
            let tx = self.inner.database().begin_read().map_err(storage)?;
            let policy = self.read_policy(&tx, principal, key)?;
            let meta = tx.open_table(META).map_err(storage)?;
            let (header, _) = read_headers(&meta, &self.inner.identity)?;
            let states = tx.open_table(CAPACITY).map_err(storage)?;
            let state: CapacityState = contract::decode(
                states
                    .get(state_key(key).as_slice())
                    .map_err(storage)?
                    .ok_or_else(|| Status::not_found("resource has no capacity configuration"))?
                    .value(),
            )?;
            let configuration = state
                .configuration
                .as_ref()
                .ok_or_else(|| missing("capacity configuration"))?;
            let tier_policy = validate_configuration(configuration).map_err(corrupt)?;
            let committed = {
                let control = tx.open_table(CONTROL).map_err(storage)?;
                let topologies = tx.open_table(TOPOLOGIES).map_err(storage)?;
                let nodes = tx.open_table(NODES).map_err(storage)?;
                let replicas = tx.open_table(REPLICAS).map_err(storage)?;
                load_committed(&control, &topologies, &nodes, &replicas, key)?
                    .ok_or_else(|| corrupt("configured resource has no applied control state"))?
            };
            let derived = derive(&committed, key)?;
            let rows = {
                let reporters = tx.open_table(REPORTERS).map_err(storage)?;
                let observations = tx.open_table(OBSERVATIONS).map_err(storage)?;
                load_rows(&reporters, &observations, key)?
            };
            let mut observations = Vec::with_capacity(rows.observations.len());
            for (_, value) in &rows.observations {
                observations.push(observation_from_proto(value).map_err(corrupt)?);
            }
            Ok(tiers::TierSnapshotInput {
                planning_instant_unix_ms: context.planning_instant_unix_ms,
                max_moves: context.max_moves,
                max_observation_age_ms: context.max_observation_age_ms,
                clock_skew_bound_ms: context.clock_skew_bound_ms,
                authority_id: hex16(&self.inner.identity.group_id),
                authority_incarnation: fixed::<16>(
                    &self.inner.identity.authority_incarnation,
                    "authority_incarnation",
                )
                .map_err(corrupt)?,
                control_revision: header.source.control_revision,
                policy_revision: policy.revision,
                policy: tier_policy,
                observation_epoch: state.observation_epoch,
                topology_generation: committed.state.topology_generation,
                placement_tree_digest: derived.placement_tree_digest,
                workspace: key.workspace.clone(),
                collection: key.collection.clone(),
                derived_fingerprint: derived.derived_fingerprint,
                provider_geometry_digest: derived.provider_geometry_digest,
                cohort_length_ms: configuration.cohort_length_ms,
                cohort_phase_unix_ms: configuration.cohort_phase_unix_ms,
                nodes: node_records(&committed, context.planning_instant_unix_ms)?,
                observations,
                fragment_requests: context.fragment_requests.clone(),
                committed: derived.view,
                current_incarnations: rows.reporters.iter().cloned().collect(),
            })
        })
    }
}

//! The `SourceAuthorityStore` side of capacity: configuration and
//! transition commits, their replay, and the committed reads the planner
//! takes its input from.

use super::*;

impl SourceAuthorityStore {
    /// Commit the planner configuration of one resource: a control command
    /// with a retained decision, current Admin, revision CAS. The cohort
    /// cannot change while observations are retained; bounds cannot drop
    /// below what is held.
    pub fn configure_capacity(
        &self,
        principal: &str,
        command: &CapacityConfigureCommand,
    ) -> Result<CapacityConfigureDecision, Status> {
        self.direct()?;
        self.replay_capacity_configure(principal, command)
    }

    /// The committed-log path of `configure_capacity`: no direct-path check.
    pub(crate) fn replay_capacity_configure(
        &self,
        principal: &str,
        command: &CapacityConfigureCommand,
    ) -> Result<CapacityConfigureDecision, Status> {
        let _exclusive = self.exclusive()?;
        self.guarded(|| {
            let decision = self.configure_locked(principal, command)?;
            let tx = self.inner.database().begin_read().map_err(storage)?;
            self.read_policy(
                &tx,
                principal,
                command.key.as_ref().ok_or_else(|| missing("command key"))?,
            )?;
            Ok(decision)
        })
    }

    fn configure_locked(
        &self,
        principal: &str,
        command: &CapacityConfigureCommand,
    ) -> Result<CapacityConfigureDecision, Status> {
        let mut tx = self.inner.database().begin_write().map_err(storage)?;
        tx.set_durability(Durability::Immediate).map_err(storage)?;
        let decision;
        let mut retried = false;
        'apply: {
            let mut meta = tx.open_table(META).map_err(storage)?;
            let (mut header, policy) = read_headers(&meta, &self.inner.identity)?;
            let mut capacity_header = header_of(&meta)?;
            validate_configure(command, &self.inner.identity, &header.limits)?;
            let key = command.key.as_ref().expect("validated key");
            contract::authorize(&policy, principal, key)?;
            let operation_key = contract::operation_key(principal, key, &command.command_id);
            let operation_bytes = operation_key.encode_to_vec();
            let request_sha256 = command_digest(command);
            if tx
                .open_table(DECISIONS)
                .map_err(storage)?
                .get(operation_bytes.as_slice())
                .map_err(storage)?
                .is_some()
                || tx
                    .open_table(import::IMPORT_OPERATIONS)
                    .map_err(storage)?
                    .get(operation_bytes.as_slice())
                    .map_err(storage)?
                    .is_some()
            {
                return Err(Status::failed_precondition(
                    "command_id was already used by another source authority command",
                ));
            }
            let mut operations = tx.open_table(CAPACITY_OPERATIONS).map_err(storage)?;
            if let Some(saved) = operations
                .get(operation_bytes.as_slice())
                .map_err(storage)?
            {
                let operation: CapacityConfigureOperation = contract::decode(saved.value())?;
                if operation.request_sha256 != request_sha256
                    || operation.command.as_ref() != Some(command)
                {
                    return Err(Status::failed_precondition(
                        "capacity command_id was already used with different content",
                    ));
                }
                decision = operation
                    .decision
                    .ok_or_else(|| missing("retry decision"))?;
                retried = true;
                // Under Raft the entry is consumed: the applied position
                // advances for an exact retry too, with no other change.
                self.write_pending_applied(&mut meta)?;
                break 'apply;
            }
            let (reserved_bytes, reserved_decisions, retained) = import::reserved(&meta)?;
            if header
                .source
                .decision_count
                .saturating_add(retained)
                .saturating_add(reserved_decisions)
                >= header.limits.max_decisions
            {
                return Err(Status::resource_exhausted(
                    "source authority decision capacity is full; retry history is never evicted",
                ));
            }
            let mut states = tx.open_table(CAPACITY).map_err(storage)?;
            let state_bytes = state_key(key);
            let existing: Option<CapacityState> = states
                .get(state_bytes.as_slice())
                .map_err(storage)?
                .map(|v| contract::decode(v.value()))
                .transpose()?;
            let revision = header
                .source
                .control_revision
                .checked_add(1)
                .ok_or_else(|| {
                    Status::resource_exhausted("source authority control revision exhausted")
                })?;
            let mut result = CapacityConfigureDecision {
                format_version: 1,
                control_revision: header.source.control_revision,
                policy_revision: policy.revision,
                observation_epoch: existing.as_ref().map_or(0, |s| s.observation_epoch),
                ..Default::default()
            };
            let configuration = command
                .configuration
                .clone()
                .expect("validated configuration");
            let transition = (|| -> Result<CapacityState, Status> {
                if command.expected_control_revision != header.source.control_revision {
                    return Err(reject(
                        Code::FailedPrecondition,
                        "source authority control revision differs",
                    ));
                }
                if command.expected_policy_revision != policy.revision {
                    return Err(reject(
                        Code::FailedPrecondition,
                        "source authority policy revision differs",
                    ));
                }
                if tx
                    .open_table(CONTROL)
                    .map_err(storage)?
                    .get(key.encode_to_vec().as_slice())
                    .map_err(storage)?
                    .is_none()
                {
                    return Err(reject(
                        Code::FailedPrecondition,
                        "resource has no applied control state to plan over",
                    ));
                }
                let mut next = existing.clone().unwrap_or(CapacityState {
                    format_version: 1,
                    key: Some(key.clone()),
                    ..Default::default()
                });
                if let Some(current) = existing.as_ref() {
                    let held = current
                        .configuration
                        .as_ref()
                        .ok_or_else(|| missing("configuration"))?;
                    if current.observation_count > 0
                        && (held.cohort_length_ms != configuration.cohort_length_ms
                            || held.cohort_phase_unix_ms != configuration.cohort_phase_unix_ms)
                    {
                        return Err(reject(
                            Code::FailedPrecondition,
                            "cohort cannot change while observations are retained; commit an expiry first",
                        ));
                    }
                    if current.observation_count > configuration.max_total_records
                        || current.reporter_count > configuration.max_registered_nodes
                    {
                        return Err(reject(
                            Code::FailedPrecondition,
                            "configuration bounds are below the retained reporters or observations",
                        ));
                    }
                }
                next.configuration = Some(configuration);
                next.configured_control_revision = revision;
                Ok(next)
            })();
            let mut state_change = None;
            match transition {
                Ok(next) => {
                    result.control_revision = revision;
                    state_change = Some(next);
                }
                Err(error) => {
                    result.code = error.code() as u32;
                    result.message = error.message().to_string();
                }
            }
            decision = result;
            let operation = CapacityConfigureOperation {
                format_version: 1,
                request_sha256,
                command: Some(command.clone()),
                decision: Some(decision.clone()),
            };
            let encoded = operation.encode_to_vec();
            if encoded.len() > contract::MAX_RECORD_BYTES {
                return Err(Status::resource_exhausted(
                    "capacity decision record exceeds 2MiB",
                ));
            }
            header.source.payload_bytes = payload_change(
                header.source.payload_bytes,
                0,
                operation_bytes.len() + encoded.len(),
            )?;
            capacity_header.configure_decision_count = capacity_header
                .configure_decision_count
                .checked_add(1)
                .ok_or_else(|| corrupt("capacity decision counter overflow"))?;
            if let Some(next) = state_change {
                let bytes = next.encode_to_vec();
                let old = states
                    .get(state_bytes.as_slice())
                    .map_err(storage)?
                    .map_or(0, |v| v.value().len());
                if old == 0 {
                    capacity_header.state_count = capacity_header
                        .state_count
                        .checked_add(1)
                        .ok_or_else(|| corrupt("capacity state counter overflow"))?;
                    header.source.payload_bytes =
                        payload_change(header.source.payload_bytes, 0, state_bytes.len())?;
                }
                header.source.payload_bytes =
                    payload_change(header.source.payload_bytes, old, bytes.len())?;
                states
                    .insert(state_bytes.as_slice(), bytes.as_slice())
                    .map_err(storage)?;
                header.source.control_revision = revision;
            }
            if header.source.payload_bytes.saturating_add(reserved_bytes)
                > header.limits.max_payload_bytes
            {
                return Err(Status::resource_exhausted(
                    "source authority payload capacity is full; retry history is never evicted",
                ));
            }
            operations
                .insert(operation_bytes.as_slice(), encoded.as_slice())
                .map_err(storage)?;
            meta.insert("header", header.source.encode_to_vec().as_slice())
                .map_err(storage)?;
            meta.insert(CAPACITY_HEADER, capacity_header.encode_to_vec().as_slice())
                .map_err(storage)?;
            self.write_pending_applied(&mut meta)?;
        }
        if retried {
            self.finish_retry(tx)?;
            return Ok(decision);
        }
        #[cfg(any(test, feature = "fault-injection"))]
        self.inject(false)?;
        tx.commit().map_err(storage)?;
        self.inner.revisions.send_replace(decision.policy_revision);
        self.publish_applied(decision.control_revision);
        #[cfg(any(test, feature = "fault-injection"))]
        self.inject(true)?;
        Ok(decision)
    }

    /// Apply one observation transition under the committed configuration
    /// and view: register a reporter incarnation, land a report, or commit
    /// an expiry. Idempotent by content; refusals are returned, not stored.
    pub fn capacity_transition(
        &self,
        principal: &str,
        transition: &CapacityTransition,
    ) -> Result<CapacityTransitionReceipt, Status> {
        self.direct()?;
        self.replay_capacity_transition(principal, transition)
    }

    /// The committed-log path of `capacity_transition`: no direct-path check.
    pub(crate) fn replay_capacity_transition(
        &self,
        principal: &str,
        transition: &CapacityTransition,
    ) -> Result<CapacityTransitionReceipt, Status> {
        let _exclusive = self.exclusive()?;
        self.guarded(|| self.transition_locked(principal, transition))
    }

    fn transition_locked(
        &self,
        principal: &str,
        transition: &CapacityTransition,
    ) -> Result<CapacityTransitionReceipt, Status> {
        let mut tx = self.inner.database().begin_write().map_err(storage)?;
        tx.set_durability(Durability::Immediate).map_err(storage)?;
        let receipt;
        let changed_or_positioned;
        {
            let mut meta = tx.open_table(META).map_err(storage)?;
            let (mut header, policy) = read_headers(&meta, &self.inner.identity)?;
            let mut capacity_header = header_of(&meta)?;
            validate_transition(transition, &self.inner.identity, &header.limits)?;
            let key = transition.key.as_ref().expect("validated key");
            contract::authorize(&policy, principal, key)?;
            let mut states = tx.open_table(CAPACITY).map_err(storage)?;
            let state_bytes = state_key(key);
            let mut state: CapacityState = contract::decode(
                states
                    .get(state_bytes.as_slice())
                    .map_err(storage)?
                    .ok_or_else(|| {
                        Status::failed_precondition(
                            "resource has no committed capacity configuration",
                        )
                    })?
                    .value(),
            )?;
            let configuration = state
                .configuration
                .clone()
                .ok_or_else(|| missing("capacity configuration"))?;
            let committed = {
                let control = tx.open_table(CONTROL).map_err(storage)?;
                let topologies = tx.open_table(TOPOLOGIES).map_err(storage)?;
                let nodes = tx.open_table(NODES).map_err(storage)?;
                let replicas = tx.open_table(REPLICAS).map_err(storage)?;
                load_committed(&control, &topologies, &nodes, &replicas, key)?
                    .ok_or_else(|| corrupt("configured resource has no applied control state"))?
            };
            let derived = derive(&committed, key)?;
            let mut reporters = tx.open_table(REPORTERS).map_err(storage)?;
            let mut observations = tx.open_table(OBSERVATIONS).map_err(storage)?;
            let rows = load_rows(&reporters, &observations, key)?;
            let mut store = rebuild(&configuration, derived.view, &rows)?;
            let before = store.epoch();
            let mut removed_bytes = 0usize;
            let mut added_bytes = 0usize;
            let mut dropped = 0u64;
            let mut outcome = CapacityTransitionOutcome::Unspecified;
            match transition.action.as_ref().expect("validated action") {
                TransitionAction::Register(register) => {
                    let incarnation: [u8; 16] = register
                        .process_incarnation
                        .as_slice()
                        .try_into()
                        .expect("validated incarnation");
                    store
                        .register_incarnation(&register.node_id, incarnation)
                        .map_err(refusal)?;
                    let reporter_bytes = reporter_key(key, &register.node_id);
                    let value = CapacityReporter {
                        format_version: 1,
                        process_incarnation: incarnation.to_vec(),
                    }
                    .encode_to_vec();
                    let old = reporters
                        .get(reporter_bytes.as_slice())
                        .map_err(storage)?
                        .map(|v| v.value().to_vec());
                    if old.as_deref() != Some(value.as_slice()) {
                        if old.is_none() {
                            added_bytes += reporter_bytes.len();
                            state.reporter_count += 1;
                            capacity_header.reporter_count = capacity_header
                                .reporter_count
                                .checked_add(1)
                                .ok_or_else(|| corrupt("reporter counter overflow"))?;
                        }
                        removed_bytes += old.map_or(0, |v| v.len());
                        added_bytes += value.len();
                        reporters
                            .insert(reporter_bytes.as_slice(), value.as_slice())
                            .map_err(storage)?;
                        for (row_key, row) in &rows.observations {
                            if row.node_id == register.node_id
                                && row.process_incarnation != incarnation
                            {
                                observations.remove(row_key.as_slice()).map_err(storage)?;
                                removed_bytes += row_key.len() + row.encoded_len();
                                dropped += 1;
                            }
                        }
                    }
                }
                TransitionAction::Report(report) => {
                    let observation = observation_from_proto(report)?;
                    match store.ingest(observation).map_err(refusal)? {
                        tiers::IngestOutcome::Landed => {
                            outcome = CapacityTransitionOutcome::Landed;
                            let row_key = observation_key(key, report);
                            let value = report.encode_to_vec();
                            let old = observations
                                .get(row_key.as_slice())
                                .map_err(storage)?
                                .map(|v| v.value().len());
                            if old.is_none() {
                                added_bytes += row_key.len();
                                state.observation_count += 1;
                                capacity_header.observation_count = capacity_header
                                    .observation_count
                                    .checked_add(1)
                                    .ok_or_else(|| corrupt("observation counter overflow"))?;
                            }
                            removed_bytes += old.unwrap_or(0);
                            added_bytes += value.len();
                            observations
                                .insert(row_key.as_slice(), value.as_slice())
                                .map_err(storage)?;
                        }
                        tiers::IngestOutcome::Unchanged => {
                            outcome = CapacityTransitionOutcome::Unchanged
                        }
                        tiers::IngestOutcome::Superseded => {
                            outcome = CapacityTransitionOutcome::Superseded
                        }
                    }
                }
                TransitionAction::Expire(expire) => {
                    store
                        .commit_expiry(expire.now_unix_ms, expire.max_age_ms)
                        .map_err(refusal)?;
                    for (row_key, row) in &rows.observations {
                        if expire.now_unix_ms.saturating_sub(row.window_end_unix_ms)
                            > expire.max_age_ms
                        {
                            observations.remove(row_key.as_slice()).map_err(storage)?;
                            removed_bytes += row_key.len() + row.encoded_len();
                            dropped += 1;
                        }
                    }
                }
            }
            let changed = store.epoch() != before || dropped > 0 || added_bytes > 0;
            if dropped > 0 {
                state.observation_count = state
                    .observation_count
                    .checked_sub(dropped)
                    .ok_or_else(|| corrupt("observation count underflow"))?;
                capacity_header.observation_count = capacity_header
                    .observation_count
                    .checked_sub(dropped)
                    .ok_or_else(|| corrupt("observation counter underflow"))?;
            }
            if changed {
                if store.epoch() != before {
                    state.observation_epoch = state
                        .observation_epoch
                        .checked_add(1)
                        .ok_or_else(|| Status::resource_exhausted("observation epoch exhausted"))?;
                }
                let old_state = states
                    .get(state_bytes.as_slice())
                    .map_err(storage)?
                    .map_or(0, |v| v.value().len());
                let state_value = state.encode_to_vec();
                removed_bytes += old_state;
                added_bytes += state_value.len();
                states
                    .insert(state_bytes.as_slice(), state_value.as_slice())
                    .map_err(storage)?;
                header.source.payload_bytes =
                    payload_change(header.source.payload_bytes, removed_bytes, added_bytes)?;
                if header
                    .source
                    .payload_bytes
                    .saturating_add(header.control.reserved_bytes)
                    > header.limits.max_payload_bytes
                {
                    return Err(Status::resource_exhausted(
                        "source authority payload capacity is full; retry history is never evicted",
                    ));
                }
                meta.insert("header", header.source.encode_to_vec().as_slice())
                    .map_err(storage)?;
                meta.insert(CAPACITY_HEADER, capacity_header.encode_to_vec().as_slice())
                    .map_err(storage)?;
            }
            // An applied position is written whether or not the set changed:
            // under Raft the entry is consumed either way.
            let pending = self
                .write_pending_applied(&mut meta)
                .map(|_| self.inner.raft_pending.lock().unwrap().is_some())?;
            changed_or_positioned = changed || pending;
            receipt = CapacityTransitionReceipt {
                format_version: 1,
                outcome: outcome as i32,
                observation_epoch: state.observation_epoch,
                dropped,
                control_revision: header.source.control_revision,
            };
        }
        if !changed_or_positioned {
            tx.abort().map_err(storage)?;
            return Ok(receipt);
        }
        #[cfg(any(test, feature = "fault-injection"))]
        self.inject(false)?;
        tx.commit().map_err(storage)?;
        #[cfg(any(test, feature = "fault-injection"))]
        self.inject(true)?;
        Ok(receipt)
    }

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

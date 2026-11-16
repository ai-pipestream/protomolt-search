//! Observation transitions: register a reporter incarnation, land a report
//! or commit an expiry under the committed configuration and view.
use super::validation::validate_transition;
use super::*;

type RowTable<'txn> = redb::Table<'txn, &'static [u8], &'static [u8]>;

/// The rows and counters one transition works on inside its transaction.
struct Working<'txn> {
    state: CapacityState,
    capacity_header: CapacityStoreHeader,
    reporters: RowTable<'txn>,
    observations: RowTable<'txn>,
    rows: Rows,
    store: tiers::ObservationStore,
    removed_bytes: usize,
    added_bytes: usize,
    dropped: u64,
    outcome: CapacityTransitionOutcome,
}

impl SourceAuthorityStore {
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
        let (receipt, changed_or_positioned) = self.apply_transition(&tx, principal, transition)?;
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

    /// Apply `transition` to the tables of `tx` without committing it.
    /// Returns the receipt and whether the transaction holds a change or an
    /// applied position.
    fn apply_transition(
        &self,
        tx: &redb::WriteTransaction,
        principal: &str,
        transition: &CapacityTransition,
    ) -> Result<(CapacityTransitionReceipt, bool), Status> {
        let mut meta = tx.open_table(META).map_err(storage)?;
        let (mut header, policy) = read_headers(&meta, &self.inner.identity)?;
        let capacity_header = header_of(&meta)?;
        validate_transition(transition, &self.inner.identity, &header.limits)?;
        let key = transition.key.as_ref().expect("validated key");
        contract::authorize(&policy, principal, key)?;
        let mut states = tx.open_table(CAPACITY).map_err(storage)?;
        let state_bytes = state_key(key);
        let mut work = working(tx, &states, &state_bytes, key, capacity_header)?;
        let before = work.store.epoch();
        match transition.action.as_ref().expect("validated action") {
            TransitionAction::Register(register) => register_reporter(&mut work, key, register)?,
            TransitionAction::Report(report) => land_report(&mut work, key, report)?,
            TransitionAction::Expire(expire) => commit_expiry(&mut work, expire)?,
        }
        let epoch_moved = work.store.epoch() != before;
        let changed = epoch_moved || work.dropped > 0 || work.added_bytes > 0;
        if work.dropped > 0 {
            work.state.observation_count = work
                .state
                .observation_count
                .checked_sub(work.dropped)
                .ok_or_else(|| corrupt("observation count underflow"))?;
            work.capacity_header.observation_count = work
                .capacity_header
                .observation_count
                .checked_sub(work.dropped)
                .ok_or_else(|| corrupt("observation counter underflow"))?;
        }
        if changed {
            store_change(
                &mut work,
                &mut states,
                &state_bytes,
                &mut header,
                epoch_moved,
            )?;
            meta.insert("header", header.source.encode_to_vec().as_slice())
                .map_err(storage)?;
            meta.insert(
                CAPACITY_HEADER,
                work.capacity_header.encode_to_vec().as_slice(),
            )
            .map_err(storage)?;
        }
        // An applied position is written whether or not the set changed:
        // under Raft the entry is consumed either way.
        let pending = self
            .write_pending_applied(&mut meta)
            .map(|_| self.inner.raft_pending.lock().unwrap().is_some())?;
        let receipt = CapacityTransitionReceipt {
            format_version: 1,
            outcome: work.outcome as i32,
            observation_epoch: work.state.observation_epoch,
            dropped: work.dropped,
            control_revision: header.source.control_revision,
        };
        Ok((receipt, changed || pending))
    }
}

/// Load the configured state of `key` and rebuild its observation store
/// from the committed rows.
fn working<'txn>(
    tx: &'txn redb::WriteTransaction,
    states: &RowTable<'_>,
    state_bytes: &[u8],
    key: &LogicalSourceOwner,
    capacity_header: CapacityStoreHeader,
) -> Result<Working<'txn>, Status> {
    let state: CapacityState = contract::decode(
        states
            .get(state_bytes)
            .map_err(storage)?
            .ok_or_else(|| {
                Status::failed_precondition("resource has no committed capacity configuration")
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
    let reporters = tx.open_table(REPORTERS).map_err(storage)?;
    let observations = tx.open_table(OBSERVATIONS).map_err(storage)?;
    let rows = load_rows(&reporters, &observations, key)?;
    let store = rebuild(&configuration, derived.view, &rows)?;
    Ok(Working {
        state,
        capacity_header,
        reporters,
        observations,
        rows,
        store,
        removed_bytes: 0,
        added_bytes: 0,
        dropped: 0,
        outcome: CapacityTransitionOutcome::Unspecified,
    })
}

/// Register a reporter incarnation; a new incarnation drops the rows of the
/// node's earlier one.
fn register_reporter(
    work: &mut Working<'_>,
    key: &LogicalSourceOwner,
    register: &RegisterCapacityReporter,
) -> Result<(), Status> {
    let incarnation: [u8; 16] = register
        .process_incarnation
        .as_slice()
        .try_into()
        .expect("validated incarnation");
    work.store
        .register_incarnation(&register.node_id, incarnation)
        .map_err(refusal)?;
    let reporter_bytes = reporter_key(key, &register.node_id);
    let value = CapacityReporter {
        format_version: 1,
        process_incarnation: incarnation.to_vec(),
    }
    .encode_to_vec();
    let old = work
        .reporters
        .get(reporter_bytes.as_slice())
        .map_err(storage)?
        .map(|v| v.value().to_vec());
    if old.as_deref() == Some(value.as_slice()) {
        return Ok(());
    }
    if old.is_none() {
        work.added_bytes += reporter_bytes.len();
        work.state.reporter_count += 1;
        work.capacity_header.reporter_count = work
            .capacity_header
            .reporter_count
            .checked_add(1)
            .ok_or_else(|| corrupt("reporter counter overflow"))?;
    }
    work.removed_bytes += old.map_or(0, |v| v.len());
    work.added_bytes += value.len();
    work.reporters
        .insert(reporter_bytes.as_slice(), value.as_slice())
        .map_err(storage)?;
    for (row_key, row) in &work.rows.observations {
        if row.node_id == register.node_id && row.process_incarnation != incarnation {
            work.observations
                .remove(row_key.as_slice())
                .map_err(storage)?;
            work.removed_bytes += row_key.len() + row.encoded_len();
            work.dropped += 1;
        }
    }
    Ok(())
}

/// Ingest one report and write its row when it landed.
fn land_report(
    work: &mut Working<'_>,
    key: &LogicalSourceOwner,
    report: &CapacityObservation,
) -> Result<(), Status> {
    let observation = observation_from_proto(report)?;
    match work.store.ingest(observation).map_err(refusal)? {
        tiers::IngestOutcome::Landed => {
            work.outcome = CapacityTransitionOutcome::Landed;
            let row_key = observation_key(key, report);
            let value = report.encode_to_vec();
            let old = work
                .observations
                .get(row_key.as_slice())
                .map_err(storage)?
                .map(|v| v.value().len());
            if old.is_none() {
                work.added_bytes += row_key.len();
                work.state.observation_count += 1;
                work.capacity_header.observation_count = work
                    .capacity_header
                    .observation_count
                    .checked_add(1)
                    .ok_or_else(|| corrupt("observation counter overflow"))?;
            }
            work.removed_bytes += old.unwrap_or(0);
            work.added_bytes += value.len();
            work.observations
                .insert(row_key.as_slice(), value.as_slice())
                .map_err(storage)?;
        }
        tiers::IngestOutcome::Unchanged => work.outcome = CapacityTransitionOutcome::Unchanged,
        tiers::IngestOutcome::Superseded => work.outcome = CapacityTransitionOutcome::Superseded,
    }
    Ok(())
}

/// Commit an expiry and remove the rows past the age bound.
fn commit_expiry(work: &mut Working<'_>, expire: &CommitCapacityExpiry) -> Result<(), Status> {
    work.store
        .commit_expiry(expire.now_unix_ms, expire.max_age_ms)
        .map_err(refusal)?;
    for (row_key, row) in &work.rows.observations {
        if expire.now_unix_ms.saturating_sub(row.window_end_unix_ms) > expire.max_age_ms {
            work.observations
                .remove(row_key.as_slice())
                .map_err(storage)?;
            work.removed_bytes += row_key.len() + row.encoded_len();
            work.dropped += 1;
        }
    }
    Ok(())
}

/// Write the changed capacity state and account for the transition's bytes
/// in the source header.
fn store_change(
    work: &mut Working<'_>,
    states: &mut RowTable<'_>,
    state_bytes: &[u8],
    header: &mut control::Header,
    epoch_moved: bool,
) -> Result<(), Status> {
    if epoch_moved {
        work.state.observation_epoch = work
            .state
            .observation_epoch
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("observation epoch exhausted"))?;
    }
    let old_state = states
        .get(state_bytes)
        .map_err(storage)?
        .map_or(0, |v| v.value().len());
    let state_value = work.state.encode_to_vec();
    work.removed_bytes += old_state;
    work.added_bytes += state_value.len();
    states
        .insert(state_bytes, state_value.as_slice())
        .map_err(storage)?;
    header.source.payload_bytes = payload_change(
        header.source.payload_bytes,
        work.removed_bytes,
        work.added_bytes,
    )?;
    if header.source.payload_bytes > header.limits.max_payload_bytes {
        return Err(Status::resource_exhausted(
            "source authority payload capacity is full; retry history is never evicted",
        ));
    }
    Ok(())
}

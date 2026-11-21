//! Reading and resolving maintenance decisions against the durable
//! catalog.
use super::*;

impl DocumentCatalog {
    /// Current maintenance decision, only while its after-manifest is the
    /// durable physical tip. A later source write supersedes this observation.
    pub fn current_index_maintenance_decision(
        &self,
        key: &[u8],
        catalog: &SegmentCatalog,
    ) -> Result<Option<MaintenanceIntent>, Status> {
        index_key(key)?;
        if !self.validate_projection_journal()? {
            return Ok(None);
        }
        catalog.with_durable_snapshot(|snapshot| {
            let tx = self.database.begin_read().map_err(storage)?;
            let meta = tx.open_table(META).map_err(storage)?;
            let header = current_header(&meta)?;
            let states = tx.open_table(STATES).map_err(storage)?;
            let Some(bytes) = states.get(key).map_err(storage)? else {
                return Ok(None);
            };
            let state: ProjectionJournalState = decode(bytes.value())?;
            validate_state(&state, key, &header.history_id)?;
            if state.pending.is_some() || state.pending_maintenance.is_some() {
                return Err(Status::failed_precondition(
                    "resolve pending publication before observing maintenance activation",
                ));
            }
            let decisions = tx.open_table(DECISIONS).map_err(storage)?;
            let maintenance = maintenance_table!(tx);
            validate_tip(&state, &decisions, maintenance.as_ref(), &header.collection)?;
            if manifest_hash(snapshot.manifest())? != state.committed_manifest_sha256 {
                return Err(Status::failed_precondition(
                    "durable catalog differs from the committed physical manifest",
                ));
            }
            let Some(reference) = &state.maintenance_tip else {
                return Ok(None);
            };
            let table = maintenance
                .as_ref()
                .ok_or_else(|| Status::data_loss("maintenance decision table missing"))?;
            let intent = maintenance_record(table, &state, reference, &header.collection)?;
            if intent.accepted_sequence == state.committed_sequence
                && intent.after_epoch == snapshot.epoch()
            {
                Ok(Some(intent))
            } else {
                Ok(None)
            }
        })
    }

    /// An immutable artifact decision, not a claim of current visibility.
    pub fn index_maintenance_decision(
        &self,
        key: &[u8],
        after_epoch: u64,
    ) -> Result<Option<MaintenanceIntent>, Status> {
        index_key(key)?;
        if !self.validate_projection_journal()? {
            return Ok(None);
        }
        let tx = self.database.begin_read().map_err(storage)?;
        let Some(table) = maintenance_table!(tx) else {
            return Ok(None);
        };
        let encoded_key = maintenance_key(key, after_epoch);
        let Some(bytes) = table.get(encoded_key.as_slice()).map_err(storage)? else {
            return Ok(None);
        };
        let intent: MaintenanceIntent = decode(bytes.value())?;
        let meta = tx.open_table(META).map_err(storage)?;
        let header = current_header(&meta)?;
        validate_maintenance(&intent, key, &header.history_id, &header.collection)?;
        if intent.after_epoch != after_epoch || intent.encode_to_vec() != bytes.value() {
            return Err(Status::data_loss(
                "maintenance record differs from its canonical key",
            ));
        }
        let states = tx.open_table(STATES).map_err(storage)?;
        let state: ProjectionJournalState = decode(
            states
                .get(key)
                .map_err(storage)?
                .ok_or_else(|| Status::data_loss("maintenance decision has no index state"))?
                .value(),
        )?;
        validate_state(&state, key, &header.history_id)?;
        let decisions = tx.open_table(DECISIONS).map_err(storage)?;
        validate_tip(&state, &decisions, Some(&table), &header.collection)?;
        if state
            .maintenance_tip
            .as_ref()
            .is_none_or(|p| after_epoch > p.after_epoch)
            || intent.accepted_sequence > state.committed_sequence
        {
            return Err(Status::data_loss(
                "maintenance decision exceeds committed history",
            ));
        }
        let anchor: ProjectionIntent =
            record(&decisions, &decision_key(key, intent.accepted_sequence))?
                .ok_or_else(|| Status::data_loss("maintenance source anchor missing"))?;
        validate_intent(&anchor, key, &header.history_id)?;
        if anchor.intent_id != intent.source_intent_id
            || anchor.source.as_ref().map(|s| s.accepted_sequence) != Some(intent.accepted_sequence)
        {
            return Err(Status::data_loss("maintenance source anchor differs"));
        }
        Ok(Some(intent))
    }

    /// Resolve maintenance against a synced manifest under its publication lock.
    /// Neither acceptance sequence nor historical source receipts are changed.
    pub fn recover_index_maintenance(
        &self,
        key: &[u8],
        catalog: &SegmentCatalog,
    ) -> Result<MaintenanceRecovery, Status> {
        index_key(key)?;
        if !self.validate_projection_journal()? {
            return Err(Status::not_found("projection journal is absent"));
        }
        catalog.with_durable_snapshot(|snapshot| {
            let hash = manifest_hash(snapshot.manifest())?;
            let tx = self.recovery_transaction()?;
            let result;
            {
                let meta = tx.open_table(META).map_err(storage)?;
                let header = current_header(&meta)?;
                let mut states = tx.open_table(STATES).map_err(storage)?;
                let mut state: ProjectionJournalState = decode(states.get(key).map_err(storage)?.ok_or_else(|| Status::not_found("maintenance index is not registered"))?.value())?;
                validate_state(&state, key, &header.history_id)?;
                let decisions = tx.open_table(DECISIONS).map_err(storage)?;
                let mut maintenance = maintenance_table!(tx);
                validate_tip(&state, &decisions, maintenance.as_ref(), &header.collection)?;
                if state.pending.is_some() { return Err(Status::failed_precondition("resolve pending source publication before maintenance recovery")); }
                let Some(intent) = state.pending_maintenance.take() else {
                    if hash != state.committed_manifest_sha256 { return Err(Status::failed_precondition("durable catalog differs from committed maintenance/source history")); }
                    return Ok(MaintenanceRecovery::Idle);
                };
                if hash == intent.after_manifest_sha256 {
                    let table = maintenance.as_mut().ok_or_else(|| Status::data_loss("maintenance decision table missing"))?;
                    let encoded_key = maintenance_key(key, intent.after_epoch);
                    if table.get(encoded_key.as_slice()).map_err(storage)?.is_some() { return Err(Status::data_loss("pending maintenance already has a committed decision")); }
                    table.insert(encoded_key.as_slice(), intent.encode_to_vec().as_slice()).map_err(storage)?;
                    state.maintenance_tip = Some(cursor(&intent));
                    state.committed_manifest_sha256 = hash;
                    result = MaintenanceRecovery::Committed(intent);
                } else if hash == intent.before_manifest_sha256 {
                    result = MaintenanceRecovery::Aborted(intent);
                } else {
                    return Err(Status::failed_precondition("durable catalog matches neither maintenance manifest; retain intent and reconcile"));
                }
                states.insert(key, state.encode_to_vec().as_slice()).map_err(storage)?;
            }
            tx.commit().map_err(storage)?;
            Ok(result)
        })
    }
}

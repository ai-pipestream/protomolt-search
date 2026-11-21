//! Enabling the maintenance journal and preparing a verified rewrite.
use super::*;

/// The maintenance journal must exist, belong to `history` and be in
/// format 2.
fn require_maintenance_journal(tx: &redb::WriteTransaction, history: &[u8]) -> Result<(), Status> {
    let names = tx
        .list_tables()
        .map_err(storage)?
        .map(|t| t.name().to_owned())
        .collect::<Vec<_>>();
    let meta = tx.open_table(META).map_err(storage)?;
    let journal_bytes = meta
        .get(JOURNAL)
        .map_err(storage)?
        .ok_or_else(|| Status::failed_precondition("enable the maintenance journal first"))?;
    journal_header(Some(journal_bytes.value()), &names, history)?;
    let journal: ProjectionJournalHeader = decode(journal_bytes.value())?;
    if journal.format_version != 2 {
        return Err(Status::failed_precondition(
            "enable the maintenance journal first",
        ));
    }
    Ok(())
}

/// The intent for a verified rewrite on top of the journal's committed
/// source decision and maintenance tip.
fn maintenance_intent(
    verified: &VerifiedMaintenance<'_>,
    state: &ProjectionJournalState,
    source_intent_id: Vec<u8>,
) -> Result<MaintenanceIntent, Status> {
    let owner = &verified.owner;
    let mut prepared = MaintenanceIntent {
        format_version: 1,
        owner: Some(owner.clone()),
        accepted_sequence: state.committed_sequence,
        source_intent_id,
        before_manifest_sha256: manifest_hash(verified.before.manifest())?,
        after_manifest_sha256: manifest_hash(verified.after.manifest())?,
        before_epoch: verified.before.epoch(),
        after_epoch: verified.after.epoch(),
        previous: state.maintenance_tip.clone(),
        preservation: Some(verified.preservation.clone()),
        intent_id: vec![],
    };
    prepared.intent_id = maintenance_hash(&prepared);
    validate_maintenance(
        &prepared,
        &owner.index_key,
        &owner.history_id,
        &owner.collection,
    )?;
    Ok(prepared)
}

impl DocumentCatalog {
    /// Explicit, atomic journal-format migration. Old readers refuse format 2.
    /// Resolve every pending source transaction before enabling maintenance.
    pub fn enable_index_maintenance(&self) -> Result<(), Status> {
        if !self.durable {
            return Err(Status::failed_precondition(
                "maintenance requires a durable source catalog",
            ));
        }
        let tx = self.writable_transaction()?;
        {
            let names = tx
                .list_tables()
                .map_err(storage)?
                .map(|t| t.name().to_owned())
                .collect::<Vec<_>>();
            let mut meta = tx.open_table(META).map_err(storage)?;
            let header = current_header(&meta)?;
            let encoded = meta
                .get(JOURNAL)
                .map_err(storage)?
                .map(|v| v.value().to_vec());
            if !journal_header(encoded.as_deref(), &names, &header.history_id)? {
                return Err(Status::failed_precondition(
                    "publish source history before enabling maintenance",
                ));
            }
            let journal: ProjectionJournalHeader =
                decode(encoded.as_deref().expect("validated journal"))?;
            if journal.format_version == 2 {
                return Ok(());
            }
            let states = tx.open_table(STATES).map_err(storage)?;
            let decisions = tx.open_table(DECISIONS).map_err(storage)?;
            let maintenance = maintenance_table!(tx);
            for item in states.iter().map_err(storage)? {
                let (key, bytes) = item.map_err(storage)?;
                let state: ProjectionJournalState = decode(bytes.value())?;
                validate_state(&state, key.value(), &header.history_id)?;
                validate_tip(&state, &decisions, maintenance.as_ref(), &header.collection)?;
                if state.pending.is_some() || state.pending_maintenance.is_some() {
                    return Err(Status::failed_precondition(
                        "resolve pending source publications before journal migration",
                    ));
                }
            }
            tx.open_table(MAINTENANCE).map_err(storage)?;
            meta.insert(
                JOURNAL,
                ProjectionJournalHeader {
                    format_version: 2,
                    history_id: header.history_id,
                }
                .encode_to_vec()
                .as_slice(),
            )
            .map_err(storage)?;
        }
        tx.commit().map_err(storage)
    }

    /// Prepare maintenance from held immutable views, computing the preservation
    /// comparison locally. The caller must fence the owning index until commit.
    /// This records artifacts only; it does not activate a serving view.
    pub fn prepare_index_maintenance(
        &self,
        key: &[u8],
        before: &OpenedSegmentSet,
        after: &OpenedSegmentSet,
        scratch: &Path,
        batch_rows: usize,
    ) -> Result<MaintenanceIntent, Status> {
        {
            let read = self.database.begin_read().map_err(storage)?;
            let meta = read.open_table(META).map_err(storage)?;
            let encoded = meta.get(JOURNAL).map_err(storage)?.ok_or_else(|| {
                Status::failed_precondition("enable the maintenance journal first")
            })?;
            let journal: ProjectionJournalHeader = decode(encoded.value())?;
            if journal.format_version != 2 {
                return Err(Status::failed_precondition(
                    "enable the maintenance journal first",
                ));
            }
        }

        let verified = self.verify_index_maintenance(key, before, after, scratch, batch_rows)?;
        self.prepare_verified_index_maintenance(&verified)
    }

    pub(crate) fn verify_index_maintenance<'a>(
        &self,
        key: &[u8],
        before: &'a OpenedSegmentSet,
        after: &'a OpenedSegmentSet,
        scratch: &Path,
        batch_rows: usize,
    ) -> Result<VerifiedMaintenance<'a>, Status> {
        if !(1..=1_048_576).contains(&batch_rows) {
            return Err(Status::invalid_argument(
                "maintenance proof batch_rows must be 1..=1048576",
            ));
        }
        index_key(key)?;
        let owner = self.index_owner(key)?;
        if before.root() != after.root()
            || before.epoch().checked_add(1) != Some(after.epoch())
            || before.manifest().partition_key != after.manifest().partition_key
            || before
                .manifest()
                .source_owner
                .as_ref()
                .map(|o| o.decode())
                .transpose()
                .map_err(Status::data_loss)?
                .as_ref()
                != Some(&owner)
            || after.generation_declaration().is_none()
        {
            return Err(Status::failed_precondition(
                "maintenance requires the owning catalog's next declared generation",
            ));
        }
        // Recompute pruning metadata; an equivalent row set alone cannot prove
        // that caller-supplied ranges will not hide matching rows.
        for i in 0..after.len() {
            if let Some(summary) = &after.metadata(i).summary {
                let mut expected = crate::segments::summarize_columns(
                    after.bm25(i),
                    u32::try_from(after.metadata(i).rows).map_err(|_| {
                        Status::resource_exhausted("maintenance segment rows exceed u32")
                    })?,
                );
                if let Some(partition) = &summary.partition {
                    expected.partition = expected
                        .int_columns
                        .iter()
                        .find(|c| c.name == partition.column && c.present > 0)
                        .map(|c| crate::segments::PartitionRange {
                            column: c.name.clone(),
                            lo: c.min,
                            hi: c.max,
                        });
                }
                if &expected != summary {
                    return Err(Status::failed_precondition(
                        "maintenance pruning summary differs from stored rows",
                    ));
                }
            }
        }
        let preservation = before
            .verify_source_rewrite(after, scratch, batch_rows)
            .map_err(Status::failed_precondition)?;
        Ok(VerifiedMaintenance {
            owner,
            before,
            after,
            preservation,
        })
    }

    pub(crate) fn prepare_verified_index_maintenance(
        &self,
        verified: &VerifiedMaintenance<'_>,
    ) -> Result<MaintenanceIntent, Status> {
        let owner = &verified.owner;
        let key = owner.index_key.as_slice();
        if self.index_owner(key)? != *owner {
            return Err(Status::failed_precondition(
                "verified maintenance belongs to another source owner",
            ));
        }
        let tx = self.writable_transaction()?;
        let intent;
        {
            require_maintenance_journal(&tx, &owner.history_id)?;
            let mut states = tx.open_table(STATES).map_err(storage)?;
            let mut state: ProjectionJournalState = decode(
                states
                    .get(key)
                    .map_err(storage)?
                    .ok_or_else(|| {
                        Status::failed_precondition("maintenance index has no source decisions")
                    })?
                    .value(),
            )?;
            validate_state(&state, key, &owner.history_id)?;
            let decisions = tx.open_table(DECISIONS).map_err(storage)?;
            let maintenance = tx.open_table(MAINTENANCE).map_err(storage)?;
            let source = validate_tip(&state, &decisions, Some(&maintenance), &owner.collection)?
                .ok_or_else(|| {
                Status::failed_precondition("maintenance requires a committed source decision")
            })?;
            if state.pending.is_some() {
                return Err(Status::failed_precondition(
                    "resolve pending source publication before maintenance",
                ));
            }
            let prepared = maintenance_intent(verified, &state, source.intent_id)?;
            if let Some(pending) = &state.pending_maintenance {
                if pending == &prepared {
                    return Ok(prepared);
                }
                return Err(Status::failed_precondition(
                    "resolve pending maintenance before another publication",
                ));
            }
            if state.committed_manifest_sha256 != prepared.before_manifest_sha256 {
                return Err(Status::failed_precondition(
                    "maintenance must follow the committed physical manifest",
                ));
            }
            let record_key = maintenance_key(key, prepared.after_epoch);
            if maintenance
                .get(record_key.as_slice())
                .map_err(storage)?
                .is_some()
            {
                return Err(Status::data_loss(
                    "maintenance epoch already has an immutable decision",
                ));
            }
            state.pending_maintenance = Some(prepared.clone());
            validate_tip(&state, &decisions, Some(&maintenance), &owner.collection)?;
            states
                .insert(key, state.encode_to_vec().as_slice())
                .map_err(storage)?;
            intent = prepared;
        }
        tx.commit().map_err(storage)?;
        Ok(intent)
    }
}

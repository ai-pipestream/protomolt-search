//! Immutable physical maintenance decisions joined to accepted source history.
use super::*;
use crate::pb::storage::{MaintenanceCursor, MaintenanceDecisionKey, MaintenanceIntent};

pub(super) const MAINTENANCE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("index_maintenance_decisions");

#[derive(Debug, Clone, PartialEq)]
pub enum MaintenanceRecovery {
    Committed(MaintenanceIntent),
    Aborted(MaintenanceIntent),
    Idle,
}

pub(super) fn maintenance_key(key: &[u8], epoch: u64) -> Vec<u8> {
    MaintenanceDecisionKey {
        index_key: key.to_vec(),
        after_epoch: epoch,
    }
    .encode_to_vec()
}
fn maintenance_hash(intent: &MaintenanceIntent) -> Vec<u8> {
    let mut value = intent.clone();
    value.intent_id.clear();
    sha256::digest(&value.encode_to_vec()).to_vec()
}
pub(super) fn cursor(intent: &MaintenanceIntent) -> MaintenanceCursor {
    MaintenanceCursor {
        after_epoch: intent.after_epoch,
        intent_id: intent.intent_id.clone(),
    }
}
pub(super) fn validate_maintenance(
    intent: &MaintenanceIntent,
    key: &[u8],
    history: &[u8],
    collection: &str,
) -> Result<(), Status> {
    let fail = || {
        Status::data_loss("invalid maintenance identity, preservation proof or manifest transition")
    };
    let owner = intent.owner.as_ref().ok_or_else(fail)?;
    let proof = intent.preservation.as_ref().ok_or_else(fail)?;
    if intent.format_version != 1
        || owner.format_version != 1
        || owner.index_key != key
        || owner.history_id != history
        || owner.collection != collection
        || intent.accepted_sequence == 0
        || intent.source_intent_id.len() != 32
        || intent.before_manifest_sha256.len() != 32
        || intent.after_manifest_sha256.len() != 32
        || intent.before_manifest_sha256 == intent.after_manifest_sha256
        || intent.before_epoch.checked_add(1) != Some(intent.after_epoch)
        || intent.previous.as_ref().is_some_and(|p| {
            p.after_epoch == 0 || p.after_epoch > intent.before_epoch || p.intent_id.len() != 32
        })
        || proof.format_version != 1
        || proof.owner.as_ref() != Some(owner)
        || proof.schema_sha256.len() != 32
        || proof.identity_content_sha256.len() != 32
        || intent.intent_id != maintenance_hash(intent)
    {
        return Err(fail());
    }
    Ok(())
}
fn record<T: Message + Default>(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    key: &[u8],
) -> Result<Option<T>, Status> {
    table
        .get(key)
        .map_err(storage)?
        .map(|v| decode(v.value()))
        .transpose()
}
fn maintenance_record(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    state: &ProjectionJournalState,
    reference: &MaintenanceCursor,
    collection: &str,
) -> Result<MaintenanceIntent, Status> {
    let key = maintenance_key(&state.index_key, reference.after_epoch);
    let bytes = table
        .get(key.as_slice())
        .map_err(storage)?
        .ok_or_else(|| Status::data_loss("maintenance committed decision missing"))?;
    let intent: MaintenanceIntent = decode(bytes.value())?;
    validate_maintenance(&intent, &state.index_key, &state.history_id, collection)?;
    if cursor(&intent) != *reference || intent.encode_to_vec() != bytes.value() {
        return Err(Status::data_loss(
            "maintenance cursor differs from its canonical immutable decision",
        ));
    }
    Ok(intent)
}

/// Validate the active physical tip against immutable source and maintenance
/// records. The most recent source decision never changes during maintenance.
pub(super) fn validate_tip(
    state: &ProjectionJournalState,
    decisions: &impl ReadableTable<&'static [u8], &'static [u8]>,
    maintenance: Option<&impl ReadableTable<&'static [u8], &'static [u8]>>,
    collection: &str,
) -> Result<Option<ProjectionIntent>, Status> {
    let source: Option<ProjectionIntent> = record(
        decisions,
        &decision_key(&state.index_key, state.committed_sequence.max(1)),
    )?;
    if state.committed_sequence == 0 {
        if source.is_some()
            || state.maintenance_tip.is_some()
            || state.pending_maintenance.is_some()
        {
            return Err(Status::data_loss(
                "projection journal lost its committed cursor",
            ));
        }
        return Ok(None);
    }
    let source =
        source.ok_or_else(|| Status::data_loss("projection committed decision missing"))?;
    validate_intent(&source, &state.index_key, &state.history_id)?;
    if source.source.as_ref().map(|s| s.accepted_sequence) != Some(state.committed_sequence) {
        return Err(Status::data_loss(
            "projection cursor differs from its committed decision",
        ));
    }
    let mut expected_hash = &source.after_manifest_sha256;
    let mut expected_epoch = source.after_epoch;
    let latest = if let Some(reference) = &state.maintenance_tip {
        let table = maintenance
            .ok_or_else(|| Status::data_loss("maintenance cursor has no decision table"))?;
        let latest = maintenance_record(table, state, reference, collection)?;
        let anchor: ProjectionIntent = record(
            decisions,
            &decision_key(&state.index_key, latest.accepted_sequence),
        )?
        .ok_or_else(|| Status::data_loss("maintenance source anchor is missing"))?;
        validate_intent(&anchor, &state.index_key, &state.history_id)?;
        if anchor.source.as_ref().map(|s| s.accepted_sequence) != Some(latest.accepted_sequence)
            || latest.source_intent_id != anchor.intent_id
            || latest.accepted_sequence > state.committed_sequence
        {
            return Err(Status::data_loss(
                "maintenance differs from its accepted source anchor",
            ));
        }
        let previous = latest
            .previous
            .as_ref()
            .map(|p| maintenance_record(table, state, p, collection))
            .transpose()?;
        if previous
            .as_ref()
            .is_some_and(|p| p.accepted_sequence > latest.accepted_sequence)
        {
            return Err(Status::data_loss("maintenance source order regressed"));
        }
        let (before_hash, before_epoch) = match &previous {
            Some(p) if p.accepted_sequence == latest.accepted_sequence => {
                (&p.after_manifest_sha256, p.after_epoch)
            }
            Some(p) => {
                if anchor.before_epoch < p.after_epoch {
                    return Err(Status::data_loss(
                        "source anchor precedes previous maintenance",
                    ));
                }
                (&anchor.after_manifest_sha256, anchor.after_epoch)
            }
            None => (&anchor.after_manifest_sha256, anchor.after_epoch),
        };
        if &latest.before_manifest_sha256 != before_hash || latest.before_epoch != before_epoch {
            return Err(Status::data_loss(
                "maintenance does not compose with its preceding physical decision",
            ));
        }
        if latest.accepted_sequence < state.committed_sequence
            && source.before_epoch < latest.after_epoch
        {
            return Err(Status::data_loss(
                "current source decision precedes maintenance",
            ));
        }
        Some(latest)
    } else {
        None
    };
    if let Some(latest) = &latest {
        if latest.accepted_sequence == state.committed_sequence {
            expected_hash = &latest.after_manifest_sha256;
            expected_epoch = latest.after_epoch;
        }
    }
    if &state.committed_manifest_sha256 != expected_hash {
        return Err(Status::data_loss(
            "projection cursor differs from its committed physical decision",
        ));
    }
    if state
        .pending
        .as_ref()
        .is_some_and(|p| p.before_epoch != expected_epoch)
    {
        return Err(Status::data_loss(
            "pending source does not follow the committed catalog epoch",
        ));
    }
    if let Some(pending) = &state.pending_maintenance {
        if maintenance.is_none() {
            return Err(Status::data_loss(
                "pending maintenance has no decision table",
            ));
        }
        validate_maintenance(pending, &state.index_key, &state.history_id, collection)?;
        if pending.accepted_sequence != state.committed_sequence
            || pending.source_intent_id != source.intent_id
            || pending.before_manifest_sha256 != state.committed_manifest_sha256
            || pending.before_epoch != expected_epoch
            || pending.previous != state.maintenance_tip
        {
            return Err(Status::data_loss(
                "pending maintenance differs from the committed source and physical tips",
            ));
        }
    }
    Ok(Some(source))
}

/// Constructed only by the local row/metadata comparison. It is deliberately
/// not a protobuf credential that another caller can manufacture or deserialize.
pub(crate) struct VerifiedMaintenance<'a> {
    owner: crate::pb::storage::SourceIndexOwner,
    before: &'a OpenedSegmentSet,
    after: &'a OpenedSegmentSet,
    preservation: crate::pb::storage::SourceRewriteCertificate,
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
            let header: DocumentCatalogHeader = decode(
                meta.get("header")
                    .map_err(storage)?
                    .ok_or_else(|| Status::data_loss("catalog header missing"))?
                    .value(),
            )?;
            validate_current_header(&header)?;
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
        let before = verified.before;
        let after = verified.after;
        let tx = self.writable_transaction()?;
        let intent;
        {
            let names = tx
                .list_tables()
                .map_err(storage)?
                .map(|t| t.name().to_owned())
                .collect::<Vec<_>>();
            let meta = tx.open_table(META).map_err(storage)?;
            let journal_bytes = meta.get(JOURNAL).map_err(storage)?.ok_or_else(|| {
                Status::failed_precondition("enable the maintenance journal first")
            })?;
            journal_header(Some(journal_bytes.value()), &names, &owner.history_id)?;
            let journal: ProjectionJournalHeader = decode(journal_bytes.value())?;
            if journal.format_version != 2 {
                return Err(Status::failed_precondition(
                    "enable the maintenance journal first",
                ));
            }
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
            let mut prepared = MaintenanceIntent {
                format_version: 1,
                owner: Some(owner.clone()),
                accepted_sequence: state.committed_sequence,
                source_intent_id: source.intent_id,
                before_manifest_sha256: manifest_hash(before.manifest())?,
                after_manifest_sha256: manifest_hash(after.manifest())?,
                before_epoch: before.epoch(),
                after_epoch: after.epoch(),
                previous: state.maintenance_tip.clone(),
                preservation: Some(verified.preservation.clone()),
                intent_id: vec![],
            };
            prepared.intent_id = maintenance_hash(&prepared);
            validate_maintenance(&prepared, key, &owner.history_id, &owner.collection)?;
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
            let header: DocumentCatalogHeader = decode(
                meta.get("header")
                    .map_err(storage)?
                    .ok_or_else(|| Status::data_loss("catalog header missing"))?
                    .value(),
            )?;
            validate_current_header(&header)?;
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
        let header: DocumentCatalogHeader = decode(
            meta.get("header")
                .map_err(storage)?
                .ok_or_else(|| Status::data_loss("catalog header missing"))?
                .value(),
        )?;
        validate_current_header(&header)?;
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
                let header: DocumentCatalogHeader = decode(meta.get("header").map_err(storage)?.ok_or_else(|| Status::data_loss("catalog header missing"))?.value())?;
                validate_current_header(&header)?;
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

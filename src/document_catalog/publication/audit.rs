//! Walk the immutable physical history in epoch order using bounded point reads.
use super::*;
use crate::pb::storage::{MaintenanceCursor, MaintenanceIntent};

fn damaged(message: &str) -> Status {
    Status::data_loss(format!("source/index journal audit: {message}"))
}
fn required<T: Message + Default>(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    key: &[u8],
    limit: usize,
) -> Result<T, Status> {
    let bytes = table
        .get(key)
        .map_err(storage)?
        .ok_or_else(|| damaged("immutable decision or accepted version is missing"))?;
    if bytes.value().len() > limit {
        return Err(Status::resource_exhausted(
            "journal audit record exceeds source_batch_bytes",
        ));
    }
    decode(bytes.value())
}

struct Chain {
    epoch: u64,
    hash: Vec<u8>,
    source_sequence: u64,
    source_id: Vec<u8>,
    maintenance: Option<MaintenanceCursor>,
    maintenance_count: u64,
}

impl Chain {
    fn advance(
        &mut self,
        target: u64,
        table: Option<&impl ReadableTable<&'static [u8], &'static [u8]>>,
        state: &ProjectionJournalState,
        collection: &str,
        limit: usize,
    ) -> Result<(), Status> {
        if target < self.epoch {
            return Err(damaged("physical epoch regressed"));
        }
        while self.epoch < target {
            let table =
                table.ok_or_else(|| damaged("physical history gap has no maintenance table"))?;
            let after_epoch = self
                .epoch
                .checked_add(1)
                .ok_or_else(|| damaged("physical epoch overflow"))?;
            let key = maintenance_key(&state.index_key, after_epoch);
            let bytes = table
                .get(key.as_slice())
                .map_err(storage)?
                .ok_or_else(|| damaged("immutable maintenance decision is missing"))?;
            if bytes.value().len() > limit {
                return Err(Status::resource_exhausted(
                    "journal audit record exceeds source_batch_bytes",
                ));
            }
            let intent: MaintenanceIntent = decode(bytes.value())?;
            if intent.encode_to_vec() != bytes.value() {
                return Err(damaged("maintenance decision encoding is not canonical"));
            }
            validate_maintenance(&intent, &state.index_key, &state.history_id, collection)?;
            if intent.before_epoch != self.epoch
                || intent.after_epoch != after_epoch
                || intent.before_manifest_sha256 != self.hash
                || intent.accepted_sequence != self.source_sequence
                || intent.source_intent_id != self.source_id
                || intent.previous != self.maintenance
            {
                return Err(damaged(
                    "maintenance does not compose with the preceding source and physical decisions",
                ));
            }
            self.maintenance = Some(cursor(&intent));
            self.epoch = intent.after_epoch;
            self.hash = intent.after_manifest_sha256;
            self.maintenance_count = self
                .maintenance_count
                .checked_add(1)
                .ok_or_else(|| damaged("maintenance decision count overflow"))?;
        }
        Ok(())
    }
}

/// Verify all source and maintenance records, not only the latest pair. The
/// caller supplies states from this same pinned read transaction. No repair,
/// serving activation, historical artifact reopen or source rewrite occurs.
pub(in crate::document_catalog) fn verify_checkpoint_chain(
    tx: &redb::ReadTransaction,
    header: &DocumentCatalogHeader,
    states: &[ProjectionJournalState],
    record_limit: usize,
) -> Result<(), Status> {
    let meta = tx.open_table(META).map_err(storage)?;
    if meta.get(JOURNAL).map_err(storage)?.is_none() {
        return if states.is_empty() {
            Ok(())
        } else {
            Err(damaged("index states have no journal header"))
        };
    }
    let decisions = tx.open_table(DECISIONS).map_err(storage)?;
    let maintenance = maintenance_table!(tx);
    let versions = tx.open_table(VERSIONS).map_err(storage)?;
    let changes = tx.open_table(CHANGES).map_err(storage)?;
    let expected = states.iter().try_fold(0u64, |sum, state| {
        sum.checked_add(state.committed_sequence)
            .ok_or_else(|| damaged("source decision count overflow"))
    })?;
    if decisions.len().map_err(storage)? != expected {
        return Err(damaged(
            "source decision count differs from committed index sequences",
        ));
    }
    let mut maintenance_count = 0u64;
    for state in states {
        validate_state(state, &state.index_key, &header.history_id)?;
        if state.pending.is_some()
            || state.pending_maintenance.is_some()
            || state.committed_sequence > header.accepted_sequence
        {
            return Err(damaged(
                "index state is pending or ahead of accepted history",
            ));
        }
        if state.committed_sequence == 0 {
            if state.maintenance_tip.is_some() {
                return Err(damaged("unpublished index has maintenance"));
            }
            continue;
        }
        let first: ProjectionIntent =
            required(&decisions, &decision_key(&state.index_key, 1), record_limit)?;
        validate_intent(&first, &state.index_key, &state.history_id)?;
        let mut chain = Chain {
            epoch: first.before_epoch,
            hash: first.before_manifest_sha256,
            source_sequence: 0,
            source_id: Vec::new(),
            maintenance: None,
            maintenance_count: 0,
        };
        for sequence in 1..=state.committed_sequence {
            let intent: ProjectionIntent = required(
                &decisions,
                &decision_key(&state.index_key, sequence),
                record_limit,
            )?;
            validate_intent(&intent, &state.index_key, &state.history_id)?;
            let source = intent.source.as_ref().expect("validated source intent");
            let source_key = DocumentVersionKey {
                document_key: source.document_key.clone(),
                version: source.version,
            }
            .encode_to_vec();
            let version: DocumentVersion = required(&versions, &source_key, record_limit)?;
            if source.accepted_sequence != sequence
                || *source != version
                || changes
                    .get(sequence)
                    .map_err(storage)?
                    .is_none_or(|key| key.value() != source_key)
            {
                return Err(damaged(
                    "historical source decision differs from accepted history",
                ));
            }
            chain.advance(
                intent.before_epoch,
                maintenance.as_ref(),
                state,
                &header.collection,
                record_limit,
            )?;
            if chain.hash != intent.before_manifest_sha256 {
                return Err(damaged(
                    "source decision does not follow the preceding physical manifest",
                ));
            }
            chain.epoch = intent.after_epoch;
            chain.hash = intent.after_manifest_sha256;
            chain.source_sequence = sequence;
            chain.source_id = intent.intent_id;
        }
        let last_epoch = state
            .maintenance_tip
            .as_ref()
            .map_or(chain.epoch, |tip| tip.after_epoch.max(chain.epoch));
        chain.advance(
            last_epoch,
            maintenance.as_ref(),
            state,
            &header.collection,
            record_limit,
        )?;
        if chain.hash != state.committed_manifest_sha256
            || chain.maintenance != state.maintenance_tip
        {
            return Err(damaged(
                "committed index tip differs from its complete physical history",
            ));
        }
        maintenance_count = maintenance_count
            .checked_add(chain.maintenance_count)
            .ok_or_else(|| damaged("maintenance decision count overflow"))?;
    }
    if maintenance
        .as_ref()
        .map(|t| t.len())
        .transpose()
        .map_err(storage)?
        .unwrap_or(0)
        != maintenance_count
    {
        return Err(damaged(
            "maintenance decisions are outside the committed physical history",
        ));
    }
    Ok(())
}

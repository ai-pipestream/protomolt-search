//! Ordered, source-bound index decisions. This journal does not attest that a
//! node has installed the artifacts into its active serving state.
use super::*;
use crate::node::StagedDocumentCandidate;
use crate::pb::storage::{
    ProjectionDecisionKey, ProjectionIntent, ProjectionJournalHeader, ProjectionJournalState,
};
use crate::segments::{OpenedSegmentSet, SegmentCatalog, SegmentSetManifest};
use redb::TableHandle;

macro_rules! maintenance_table {
    ($tx:expr) => {{
        let exists = $tx
            .list_tables()
            .map_err(storage)?
            .any(|table| table.name() == MAINTENANCE.name());
        if exists {
            Some($tx.open_table(MAINTENANCE).map_err(storage)?)
        } else {
            None
        }
    }};
}
mod anchors;
mod audit;
mod decision;
mod maintenance;
mod prepare;
pub(super) use anchors::checkpoint_states;
pub(super) use audit::verify_checkpoint_chain;
pub use maintenance::MaintenanceRecovery;
use maintenance::*;

const JOURNAL: &str = "projection_journal";
const STATES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("projection_states");
const DECISIONS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("projection_decisions");

/// Resolution of one prepared artifact transaction, not a write receipt.
#[derive(Debug, Clone, PartialEq)]
pub enum ProjectionRecovery {
    /// The intended after-manifest is durable. Runtime activation is separate.
    Committed(ProjectionIntent),
    /// The before-manifest is still durable; source acceptance is unchanged.
    Aborted(ProjectionIntent),
    /// No transaction is pending. Historical decisions remain queryable.
    Idle,
}

fn manifest_hash(manifest: &SegmentSetManifest) -> Result<Vec<u8>, Status> {
    // Format 1 hashes the typed JSON encoding, independent of file whitespace.
    // The manifest contains the hashes of every artifact and the full binding.
    Ok(sha256::digest(&serde_json::to_vec(manifest).map_err(storage)?).to_vec())
}

fn intent_hash(intent: &ProjectionIntent) -> Vec<u8> {
    let mut value = intent.clone();
    value.intent_id.clear();
    sha256::digest(&value.encode_to_vec()).to_vec()
}

fn index_key(key: &[u8]) -> Result<(), Status> {
    if key.is_empty() || key.len() > 1024 {
        return Err(Status::invalid_argument(
            "projection index_key must contain 1 to 1024 bytes",
        ));
    }
    Ok(())
}

fn journal_header(bytes: Option<&[u8]>, names: &[String], history: &[u8]) -> Result<bool, Status> {
    let states = names.iter().any(|name| name == STATES.name());
    let decisions = names.iter().any(|name| name == DECISIONS.name());
    let maintenance = names.iter().any(|name| name == MAINTENANCE.name());
    let Some(bytes) = bytes else {
        if states || decisions || maintenance {
            return Err(Status::data_loss(
                "projection tables exist without their journal header",
            ));
        }
        return Ok(false);
    };
    let header: ProjectionJournalHeader = decode(bytes)?;
    if !matches!(header.format_version, 1 | 2)
        || (header.format_version == 2) != maintenance
        || header.history_id != history
        || !states
        || !decisions
    {
        return Err(Status::data_loss(
            "projection journal format, history or tables differ",
        ));
    }
    Ok(true)
}

fn validate_intent(intent: &ProjectionIntent, key: &[u8], history: &[u8]) -> Result<(), Status> {
    let source = intent
        .source
        .as_ref()
        .ok_or_else(|| Status::data_loss("projection intent source missing"))?;
    if intent.index_key != key
        || intent.history_id != history
        || source.document_key.is_empty()
        || source.version == 0
        || source.accepted_sequence == 0
        || (source.deleted && (!source.source_sha256.is_empty() || intent.rows != 0))
        || (!source.deleted && source.source_sha256.len() != 32)
        || intent.before_manifest_sha256.len() != 32
        || intent.after_manifest_sha256.len() != 32
        || intent.before_epoch.checked_add(1) != Some(intent.after_epoch)
        || intent.before_manifest_sha256 == intent.after_manifest_sha256
        || intent.intent_id != intent_hash(intent)
    {
        return Err(Status::data_loss(
            "invalid projection intent identity or manifest transition",
        ));
    }
    Ok(())
}

fn validate_state(
    state: &ProjectionJournalState,
    key: &[u8],
    history: &[u8],
) -> Result<(), Status> {
    if state.index_key != key
        || state.history_id != history
        || state.committed_manifest_sha256.len() != 32
        || (state.pending.is_some() && state.pending_maintenance.is_some())
    {
        return Err(Status::data_loss("invalid projection journal state"));
    }
    if let Some(pending) = &state.pending {
        validate_intent(pending, key, history)?;
        if pending.before_manifest_sha256 != state.committed_manifest_sha256
            || state.committed_sequence.checked_add(1)
                != pending.source.as_ref().map(|s| s.accepted_sequence)
        {
            return Err(Status::data_loss(
                "projection intent is not the next committed transition",
            ));
        }
    }
    Ok(())
}

fn decision_key(key: &[u8], sequence: u64) -> Vec<u8> {
    ProjectionDecisionKey {
        index_key: key.to_vec(),
        accepted_sequence: sequence,
    }
    .encode_to_vec()
}

/// Certify exactly the staged append and all live previous rows of this key.
/// Unrelated rows, bindings and artifacts cannot hitchhike on the decision.
fn validate_transition(
    candidate: &StagedDocumentCandidate,
    before: &OpenedSegmentSet,
    after: &OpenedSegmentSet,
) -> Result<(), Status> {
    let fail = || {
        Status::failed_precondition(
            "projection manifest is not the exact staged document replacement",
        )
    };
    let info = candidate.info();
    let staged = candidate.segments();
    if before.root() != after.root()
        || before.epoch().checked_add(1) != Some(after.epoch())
        || before.manifest().partition_key != after.manifest().partition_key
        || after.len() != before.len() + staged.map_or(0, OpenedSegmentSet::len)
        || after.binding()
            != staged
                .and_then(OpenedSegmentSet::binding)
                .or(before.binding())
        || (before.binding().is_some() && after.binding() != before.binding())
    {
        return Err(fail());
    }
    if let Some(previous) = before.generation_declaration() {
        let declaration = after.generation_declaration().ok_or_else(fail)?;
        crate::segments::generation::check_upgrade(previous, declaration)
            .map_err(Status::failed_precondition)?;
    }
    let retired = before
        .document_retirements(&info.document_key, 65536)
        .map_err(Status::failed_precondition)?;
    for i in 0..before.len() {
        let mut expected = before.metadata(i).clone();
        let actual = after.metadata(i);
        let mut live = before.live_docs(i).clone();
        if let Some(retired) = retired.iter().find(|r| r.segment_id == expected.segment_id) {
            for &row in &retired.rows {
                live.delete(row as usize);
            }
        }
        expected.live_docs = actual.live_docs.clone();
        expected.live_rows = expected
            .rows
            .checked_sub(live.deleted_count())
            .ok_or_else(fail)?;
        if &expected != actual || live.words() != after.live_docs(i).words() {
            return Err(fail());
        }
    }
    let mut rows = 0u64;
    if let Some(staged) = staged {
        for i in 0..staged.len() {
            let mut expected = staged.metadata(i).clone();
            let actual = after.metadata(before.len() + i);
            rows = rows.checked_add(expected.rows).ok_or_else(fail)?;
            // Physical locations may change during the copy; content may not.
            expected.segment_id = actual.segment_id.clone();
            expected.base_label = actual.base_label;
            expected.generation = actual.generation;
            expected.vector.file = actual.vector.file.clone();
            expected.exact_vectors.file = actual.exact_vectors.file.clone();
            if let (Some(expected), Some(actual)) = (&mut expected.multi_res, &actual.multi_res) {
                expected.file.clone_from(&actual.file);
            }
            expected.bm25.file = actual.bm25.file.clone();
            expected.live_docs.file = actual.live_docs.file.clone();
            if &expected != actual
                || staged.live_docs(i).has_deletes()
                || after.live_docs(before.len() + i).has_deletes()
            {
                return Err(fail());
            }
        }
    }
    if rows != info.rows {
        return Err(fail());
    }
    Ok(())
}

impl DocumentCatalog {
    pub(crate) fn index_owner(
        &self,
        key: &[u8],
    ) -> Result<crate::pb::storage::SourceIndexOwner, Status> {
        index_key(key)?;
        if !self.durable {
            return Err(Status::failed_precondition(
                "source-managed publication requires a durable source catalog",
            ));
        }
        let tx = self.database.begin_read().map_err(storage)?;
        let meta = tx.open_table(META).map_err(storage)?;
        let header: DocumentCatalogHeader = decode_header(
            meta.get("header")
                .map_err(storage)?
                .ok_or_else(|| Status::data_loss("catalog header missing"))?
                .value(),
        )?;
        validate_current_header(&header)?;
        Ok(crate::pb::storage::SourceIndexOwner {
            format_version: 1,
            history_id: header.history_id,
            index_key: key.to_vec(),
            collection: header.collection,
        })
    }

    /// Current artifact decision, checked against the durable catalog while its
    /// publication fence is held. Pending transactions must be resolved first.
    pub fn current_index_publication_decision(
        &self,
        key: &[u8],
        catalog: &SegmentCatalog,
    ) -> Result<Option<ProjectionIntent>, Status> {
        index_key(key)?;
        if !self.validate_projection_journal()? {
            return Ok(None);
        }
        catalog.with_durable_snapshot(|snapshot| {
            let tx = self.database.begin_read().map_err(storage)?;
            let meta = tx.open_table(META).map_err(storage)?;
            let header: DocumentCatalogHeader = decode_header(
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
                    "resolve pending projection before observing activation",
                ));
            }
            if manifest_hash(snapshot.manifest())? != state.committed_manifest_sha256 {
                return Err(Status::failed_precondition(
                    "durable catalog differs from the committed projection manifest",
                ));
            }
            let decisions = tx.open_table(DECISIONS).map_err(storage)?;
            let maintenance = maintenance_table!(tx);
            validate_tip(&state, &decisions, maintenance.as_ref(), &header.collection)
        })
    }

    pub(super) fn validate_projection_journal(&self) -> Result<bool, Status> {
        let tx = self.database.begin_read().map_err(storage)?;
        let meta = tx.open_table(META).map_err(storage)?;
        let header: DocumentCatalogHeader = decode_header(
            meta.get("header")
                .map_err(storage)?
                .ok_or_else(|| Status::data_loss("catalog header missing"))?
                .value(),
        )?;
        validate_current_header(&header)?;
        let names = tx
            .list_tables()
            .map_err(storage)?
            .map(|table| table.name().to_owned())
            .collect::<Vec<_>>();
        journal_header(
            meta.get(JOURNAL)
                .map_err(storage)?
                .as_ref()
                .map(|v| v.value()),
            &names,
            &header.history_id,
        )
    }
}

#[cfg(test)]
mod tests;

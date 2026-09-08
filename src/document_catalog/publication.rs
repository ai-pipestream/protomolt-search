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
mod audit;
mod maintenance;
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
        .map_err(|e| Status::failed_precondition(e))?;
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
        let header: DocumentCatalogHeader = decode(
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
        let header: DocumentCatalogHeader = decode(
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

    /// Persist an ordered intent before publishing the prepared segment set.
    /// `index_key` is a stable logical identity, never a path or shard address.
    /// The owner must fence the target writer across preparation/publication.
    /// This is storage groundwork; it does not activate the candidate or issue
    /// searchable receipts. An empty initial set starts at accepted sequence 1.
    pub fn prepare_index_publication(
        &self,
        key: &[u8],
        candidate: &StagedDocumentCandidate,
        before: &OpenedSegmentSet,
        after: &OpenedSegmentSet,
    ) -> Result<ProjectionIntent, Status> {
        index_key(key)?;
        if !self.durable {
            return Err(Status::failed_precondition(
                "projection publication requires a durable source catalog",
            ));
        }
        validate_transition(candidate, before, after)?;
        if before.manifest().source_owner.is_some()
            && before.manifest().source_owner != after.manifest().source_owner
        {
            return Err(Status::failed_precondition(
                "source publication cannot change or remove the index owner",
            ));
        }
        if let Some(owner) = &after.manifest().source_owner {
            if owner.decode().map_err(Status::data_loss)? != self.index_owner(key)? {
                return Err(Status::failed_precondition(
                    "source publication belongs to another index owner",
                ));
            }
        }
        let info = candidate.info();
        let read = self.database.begin_read().map_err(storage)?;
        let (source, bytes) = Self::get_from(&read, &info.document_key, Some(info.version))?
            .ok_or_else(|| Status::not_found("projection accepted version missing"))?;
        if source.accepted_sequence != info.accepted_sequence
            || source.deleted != info.deleted
            || bytes.as_ref() != candidate.source()
        {
            return Err(Status::failed_precondition(
                "staged source differs from accepted history",
            ));
        }
        let source_key = DocumentVersionKey {
            document_key: source.document_key.clone(),
            version: source.version,
        }
        .encode_to_vec();
        if read
            .open_table(CHANGES)
            .map_err(storage)?
            .get(source.accepted_sequence)
            .map_err(storage)?
            .is_none_or(|v| v.value() != source_key)
        {
            return Err(Status::data_loss(
                "projection version differs from ordered accepted history",
            ));
        }
        drop(read);
        let mut intent = ProjectionIntent {
            index_key: key.to_vec(),
            history_id: info.history_id.clone(),
            source: Some(source),
            before_manifest_sha256: manifest_hash(before.manifest())?,
            after_manifest_sha256: manifest_hash(after.manifest())?,
            before_epoch: before.epoch(),
            after_epoch: after.epoch(),
            rows: info.rows,
            intent_id: Vec::new(),
        };
        intent.intent_id = intent_hash(&intent);
        validate_intent(&intent, key, &info.history_id)?;
        let tx = self.writable_transaction()?;
        let names = tx
            .list_tables()
            .map_err(storage)?
            .map(|table| table.name().to_owned())
            .collect::<Vec<_>>();
        {
            let mut meta = tx.open_table(META).map_err(storage)?;
            let header: DocumentCatalogHeader = decode(
                meta.get("header")
                    .map_err(storage)?
                    .ok_or_else(|| Status::data_loss("catalog header missing"))?
                    .value(),
            )?;
            validate_current_header(&header)?;
            if header.history_id != info.history_id {
                return Err(Status::failed_precondition(
                    "projection belongs to another catalog history",
                ));
            }
            let exists = journal_header(
                meta.get(JOURNAL)
                    .map_err(storage)?
                    .as_ref()
                    .map(|v| v.value()),
                &names,
                &header.history_id,
            )?;
            if !exists {
                meta.insert(
                    JOURNAL,
                    ProjectionJournalHeader {
                        format_version: 1,
                        history_id: header.history_id,
                    }
                    .encode_to_vec()
                    .as_slice(),
                )
                .map_err(storage)?;
            }
        }
        {
            let mut states = tx.open_table(STATES).map_err(storage)?;
            let decisions = tx.open_table(DECISIONS).map_err(storage)?;
            let existing = states
                .get(key)
                .map_err(storage)?
                .map(|v| decode::<ProjectionJournalState>(v.value()))
                .transpose()?;
            let mut state = match existing {
                Some(state) => state,
                None if before.is_empty() => ProjectionJournalState {
                    index_key: key.to_vec(),
                    history_id: info.history_id.clone(),
                    committed_sequence: 0,
                    committed_manifest_sha256: intent.before_manifest_sha256.clone(),
                    pending: None,
                    pending_maintenance: None,
                    maintenance_tip: None,
                },
                None => {
                    return Err(Status::failed_precondition(
                        "cannot adopt populated segments as source-certified history",
                    ))
                }
            };
            validate_state(&state, key, &info.history_id)?;
            let maintenance = maintenance_table!(tx);
            let collection = self.collection()?;
            validate_tip(&state, &decisions, maintenance.as_ref(), &collection)?;
            if state.pending_maintenance.is_some() {
                return Err(Status::failed_precondition(
                    "resolve pending maintenance before source publication",
                ));
            }
            if let Some(pending) = &state.pending {
                if pending == &intent {
                    return Ok(intent);
                }
                return Err(Status::failed_precondition(
                    "resolve the existing projection intent before another publication",
                ));
            }
            let decision_key = ProjectionDecisionKey {
                index_key: key.to_vec(),
                accepted_sequence: info.accepted_sequence,
            }
            .encode_to_vec();
            if let Some(record) = decisions.get(decision_key.as_slice()).map_err(storage)? {
                let committed: ProjectionIntent = decode(record.value())?;
                validate_intent(&committed, key, &info.history_id)?;
                if committed == intent {
                    return Ok(intent);
                }
                return Err(Status::failed_precondition(
                    "accepted sequence already has a different projection decision",
                ));
            }
            if state.committed_sequence.checked_add(1) != Some(info.accepted_sequence)
                || state.committed_manifest_sha256 != intent.before_manifest_sha256
            {
                return Err(Status::failed_precondition(
                    "projection must follow the committed manifest and next accepted sequence",
                ));
            }
            state.pending = Some(intent.clone());
            states
                .insert(key, state.encode_to_vec().as_slice())
                .map_err(storage)?;
        }
        tx.commit().map_err(storage)?;
        Ok(intent)
    }

    /// Resolve against the actual durable catalog while holding its publication
    /// fence. Neither an arbitrary epoch nor a caller-supplied digest is proof.
    /// A third manifest refuses recovery and preserves the pending intent.
    pub fn recover_index_publication(
        &self,
        key: &[u8],
        catalog: &SegmentCatalog,
    ) -> Result<ProjectionRecovery, Status> {
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
                let mut state: ProjectionJournalState = decode(states.get(key).map_err(storage)?.ok_or_else(|| Status::not_found("projection index is not registered"))?.value())?;
                validate_state(&state, key, &header.history_id)?;
                let mut decisions = tx.open_table(DECISIONS).map_err(storage)?;
                let maintenance = maintenance_table!(tx);
                validate_tip(&state, &decisions, maintenance.as_ref(), &header.collection)?;
                if state.pending_maintenance.is_some() {
                    return Err(Status::failed_precondition("resolve pending maintenance before source recovery"));
                }
                let Some(intent) = state.pending.take() else {
                    if hash != state.committed_manifest_sha256 {
                        return Err(Status::failed_precondition("durable catalog differs from the committed projection manifest"));
                    }
                    return Ok(ProjectionRecovery::Idle);
                };
                if hash == intent.after_manifest_sha256 {
                    let sequence = intent.source.as_ref().expect("validated intent source").accepted_sequence;
                    let decision_key = ProjectionDecisionKey { index_key: key.to_vec(), accepted_sequence: sequence }.encode_to_vec();
                    if decisions.get(decision_key.as_slice()).map_err(storage)?.is_some() {
                        return Err(Status::data_loss("pending projection already has a committed decision"));
                    }
                    decisions.insert(decision_key.as_slice(), intent.encode_to_vec().as_slice()).map_err(storage)?;
                    state.committed_sequence = sequence;
                    state.committed_manifest_sha256 = hash;
                    result = ProjectionRecovery::Committed(intent);
                } else if hash == intent.before_manifest_sha256 {
                    result = ProjectionRecovery::Aborted(intent);
                } else {
                    return Err(Status::failed_precondition("durable catalog matches neither projection manifest; retain intent and reconcile"));
                }
                states.insert(key, state.encode_to_vec().as_slice()).map_err(storage)?;
            }
            tx.commit().map_err(storage)?;
            Ok(result)
        })
    }

    /// Immutable artifact decision for one accepted sequence. This does not
    /// claim that the source version is still the current searchable version.
    pub fn index_publication_decision(
        &self,
        key: &[u8],
        sequence: u64,
    ) -> Result<Option<ProjectionIntent>, Status> {
        index_key(key)?;
        if !self.validate_projection_journal()? {
            return Ok(None);
        }
        let tx = self.database.begin_read().map_err(storage)?;
        let meta = tx.open_table(META).map_err(storage)?;
        let header: DocumentCatalogHeader = decode(
            meta.get("header")
                .map_err(storage)?
                .ok_or_else(|| Status::data_loss("catalog header missing"))?
                .value(),
        )?;
        let table = tx.open_table(DECISIONS).map_err(storage)?;
        let encoded = ProjectionDecisionKey {
            index_key: key.to_vec(),
            accepted_sequence: sequence,
        }
        .encode_to_vec();
        let intent: Option<ProjectionIntent> = table
            .get(encoded.as_slice())
            .map_err(storage)?
            .map(|v| decode(v.value()))
            .transpose()?;
        if let Some(intent) = &intent {
            validate_intent(intent, key, &header.history_id)?;
            if intent.source.as_ref().map(|s| s.accepted_sequence) != Some(sequence) {
                return Err(Status::data_loss("projection decision sequence mismatch"));
            }
        }
        if intent.is_some() {
            let states = tx.open_table(STATES).map_err(storage)?;
            let state: ProjectionJournalState = decode(
                states
                    .get(key)
                    .map_err(storage)?
                    .ok_or_else(|| Status::data_loss("projection decision has no index state"))?
                    .value(),
            )?;
            validate_state(&state, key, &header.history_id)?;
            let maintenance = maintenance_table!(tx);
            validate_tip(&state, &table, maintenance.as_ref(), &header.collection)?;
            if sequence > state.committed_sequence {
                return Err(Status::data_loss(
                    "projection decision exceeds committed history",
                ));
            }
        }
        Ok(intent)
    }
}

#[cfg(test)]
mod tests;

/// Validate all physical journal anchors in the caller's pinned source read.
/// Do not begin a second transaction: accepted backlog and every index must
/// describe the same point in source history.
pub(super) fn checkpoint_states(
    tx: &redb::ReadTransaction,
    header: &DocumentCatalogHeader,
    metadata_limit: usize,
) -> Result<
    (
        Vec<ProjectionJournalState>,
        Vec<super::checkpoint::BinaryTable>,
    ),
    Status,
> {
    let meta = tx.open_table(META).map_err(storage)?;
    let names: Vec<_> = tx
        .list_tables()
        .map_err(storage)?
        .map(|t| t.name().to_string())
        .collect();
    let journal = meta.get(JOURNAL).map_err(storage)?;
    if !journal_header(
        journal.as_ref().map(|v| v.value()),
        &names,
        &header.history_id,
    )? {
        return Ok((Vec::new(), Vec::new()));
    }
    let states = tx.open_table(STATES).map_err(storage)?;
    let decisions = tx.open_table(DECISIONS).map_err(storage)?;
    let maintenance = maintenance_table!(tx);
    let mut tables = vec![STATES, DECISIONS];
    if maintenance.is_some() {
        tables.push(MAINTENANCE);
    }
    let mut indexes = Vec::new();
    let mut used = header.encoded_len();
    for row in states.iter().map_err(storage)? {
        let (key, value) = row.map_err(storage)?;
        used = used
            .checked_add(value.value().len())
            .and_then(|v| v.checked_add(64))
            .ok_or_else(|| Status::resource_exhausted("checkpoint metadata size overflow"))?;
        if used > metadata_limit {
            return Err(Status::resource_exhausted(
                "checkpoint metadata budget exceeded",
            ));
        }
        let state: ProjectionJournalState = decode(value.value())?;
        index_key(key.value()).map_err(|e| Status::data_loss(e.message().to_string()))?;
        validate_state(&state, key.value(), &header.history_id)?;
        if state.pending.is_some() || state.pending_maintenance.is_some() {
            return Err(Status::failed_precondition(
                "resolve pending source/index decisions before checkpoint capture",
            ));
        }
        if state.committed_sequence > header.accepted_sequence {
            return Err(Status::data_loss(
                "checkpoint index is ahead of accepted source history",
            ));
        }
        if let Some(intent) =
            validate_tip(&state, &decisions, maintenance.as_ref(), &header.collection)?
        {
            let source = intent.source.as_ref().expect("validated source intent");
            let key = DocumentVersionKey {
                document_key: source.document_key.clone(),
                version: source.version,
            }
            .encode_to_vec();
            let versions = tx.open_table(VERSIONS).map_err(storage)?;
            let version = versions
                .get(key.as_slice())
                .map_err(storage)?
                .ok_or_else(|| {
                    Status::data_loss("checkpoint journal anchor has no accepted source version")
                })?;
            if version.value().len() > metadata_limit {
                return Err(Status::resource_exhausted(
                    "checkpoint source anchor exceeds metadata budget",
                ));
            }
            let accepted: DocumentVersion = decode(version.value())?;
            let changes = tx.open_table(CHANGES).map_err(storage)?;
            if accepted != *source
                || changes
                    .get(source.accepted_sequence)
                    .map_err(storage)?
                    .is_none_or(|value| value.value() != key)
            {
                return Err(Status::data_loss(
                    "checkpoint journal anchor differs from accepted source history",
                ));
            }
        }
        indexes.push(state);
    }
    Ok((indexes, tables))
}

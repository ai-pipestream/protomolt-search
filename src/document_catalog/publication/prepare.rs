//! Preparing a source publication: the ordered intent persisted before
//! the prepared segment set is published.
use super::*;

/// Check the journal header against the catalog's history, writing a
/// format-1 header on the catalog's first publication.
fn ensure_journal(tx: &redb::WriteTransaction, history_id: &[u8]) -> Result<(), Status> {
    let names = tx
        .list_tables()
        .map_err(storage)?
        .map(|table| table.name().to_owned())
        .collect::<Vec<_>>();
    let mut meta = tx.open_table(META).map_err(storage)?;
    let header: DocumentCatalogHeader = decode_header(
        meta.get("header")
            .map_err(storage)?
            .ok_or_else(|| Status::data_loss("catalog header missing"))?
            .value(),
    )?;
    validate_current_header(&header)?;
    if header.history_id != history_id {
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
    Ok(())
}

impl DocumentCatalog {
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
        let source = self.accepted_source(candidate)?;
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
        ensure_journal(&tx, &info.history_id)?;
        if !self.stage_intent(&tx, key, candidate, &intent, before.is_empty())? {
            return Ok(intent);
        }
        tx.commit().map_err(storage)?;
        Ok(intent)
    }

    /// The accepted version the candidate was staged from, checked against
    /// ordered accepted history.
    fn accepted_source(
        &self,
        candidate: &StagedDocumentCandidate,
    ) -> Result<DocumentVersion, Status> {
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
        Ok(source)
    }

    /// Record `intent` as the index's pending transition. Returns false,
    /// writing nothing, when the same intent is already pending or decided.
    fn stage_intent(
        &self,
        tx: &redb::WriteTransaction,
        key: &[u8],
        candidate: &StagedDocumentCandidate,
        intent: &ProjectionIntent,
        before_is_empty: bool,
    ) -> Result<bool, Status> {
        let info = candidate.info();
        let mut states = tx.open_table(STATES).map_err(storage)?;
        let decisions = tx.open_table(DECISIONS).map_err(storage)?;
        let existing = states
            .get(key)
            .map_err(storage)?
            .map(|v| decode::<ProjectionJournalState>(v.value()))
            .transpose()?;
        let mut state = match existing {
            Some(state) => state,
            None if before_is_empty => ProjectionJournalState {
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
            if pending == intent {
                return Ok(false);
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
            if &committed == intent {
                return Ok(false);
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
        Ok(true)
    }
}

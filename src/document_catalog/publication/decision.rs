//! Resolving a prepared publication against the durable catalog, and
//! reading the immutable decisions it leaves.
use super::*;

impl DocumentCatalog {
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
                let header = current_header(&meta)?;
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
        let header: DocumentCatalogHeader = decode_header(
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

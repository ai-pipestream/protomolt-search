//! Publication of settled outcomes: an unconfirmed version stalls every
//! target at its sequence, and a fenced version is skipped by a decision
//! that stages no rows and changes nothing but the manifest epoch, so the
//! version it would have replaced stays visible.
use super::*;
use crate::pb::storage::WriteOutcome;
use crate::pb::{DocumentPublicationTarget, DocumentPublicationTargetState};

/// Refuse to stage or publish rows for a version whose outcome is not
/// ACCEPTED: an unconfirmed one may still be fenced, and a fenced one was
/// never admitted.
pub(in crate::document_catalog) fn require_publishable(
    version: &DocumentVersion,
) -> Result<(), Status> {
    match super::super::outcome::outcome_of(version.outcome)? {
        WriteOutcome::Accepted => Ok(()),
        WriteOutcome::Unconfirmed => Err(Status::failed_precondition(format!(
            "accepted version {} at sequence {} is unconfirmed; publication stalls until its \
             outcome is settled",
            version.version, version.accepted_sequence
        ))),
        WriteOutcome::Fenced => Err(Status::failed_precondition(format!(
            "accepted version {} at sequence {} is fenced; it is skipped without staging rows",
            version.version, version.accepted_sequence
        ))),
    }
}

/// Certify a skip: the after-manifest is the before-manifest at the next
/// epoch, apart from an owner the first decision installs.
fn validate_skip_transition(
    before: &OpenedSegmentSet,
    after: &OpenedSegmentSet,
) -> Result<(), Status> {
    let unchanged = before.root() == after.root()
        && before.epoch().checked_add(1) == Some(after.epoch())
        && before.manifest().partition_key == after.manifest().partition_key
        && before.len() == after.len()
        && before.binding() == after.binding()
        && before
            .generation_declaration()
            .is_none_or(|previous| after.generation_declaration() == Some(previous))
        && (0..before.len()).all(|i| {
            before.metadata(i) == after.metadata(i)
                && before.live_docs(i).words() == after.live_docs(i).words()
        });
    if !unchanged {
        return Err(Status::failed_precondition(
            "a fenced skip must leave every segment, row and binding unchanged",
        ));
    }
    Ok(())
}

impl DocumentCatalog {
    /// The accepted version at `sequence`, read through ordered history.
    pub fn accepted_at(&self, sequence: u64) -> Result<DocumentVersion, Status> {
        let read = self.database.begin_read().map_err(storage)?;
        let key = read
            .open_table(CHANGES)
            .map_err(storage)?
            .get(sequence)
            .map_err(storage)?
            .ok_or_else(|| {
                Status::not_found(format!("no accepted version at sequence {sequence}"))
            })?
            .value()
            .to_vec();
        let bytes = read
            .open_table(VERSIONS)
            .map_err(storage)?
            .get(key.as_slice())
            .map_err(storage)?
            .ok_or_else(|| Status::data_loss("ordered history names a missing version"))?
            .value()
            .to_vec();
        let version: DocumentVersion = decode(&bytes)?;
        if version.accepted_sequence != sequence {
            return Err(Status::data_loss(
                "ordered history names a version of another sequence",
            ));
        }
        Ok(version)
    }

    /// The journal state of `key`, when the index has one.
    fn index_state(&self, key: &[u8]) -> Result<Option<ProjectionJournalState>, Status> {
        index_key(key)?;
        if !self.validate_projection_journal()? {
            return Ok(None);
        }
        let read = self.database.begin_read().map_err(storage)?;
        let states = read.open_table(STATES).map_err(storage)?;
        let state = states
            .get(key)
            .map_err(storage)?
            .map(|bytes| decode::<ProjectionJournalState>(bytes.value()))
            .transpose()?;
        Ok(state)
    }

    /// The last accepted sequence `key` has committed a decision for; zero
    /// before its first.
    pub fn committed_index_sequence(&self, key: &[u8]) -> Result<u64, Status> {
        Ok(self
            .index_state(key)?
            .map_or(0, |state| state.committed_sequence))
    }

    /// Where `key` stands on the version at `sequence`, from its journal.
    pub fn index_publication_target(
        &self,
        key: &[u8],
        sequence: u64,
    ) -> Result<DocumentPublicationTarget, Status> {
        let state = self.index_state(key)?;
        let committed = state.as_ref().map_or(0, |s| s.committed_sequence);
        let pending = state
            .as_ref()
            .and_then(|s| s.pending.as_ref())
            .and_then(|intent| intent.source.as_ref())
            .is_some_and(|source| source.accepted_sequence == sequence);
        let target = if sequence <= committed {
            let decision = self
                .index_publication_decision(key, sequence)?
                .ok_or_else(|| Status::data_loss("committed sequence has no decision"))?;
            let source = decision.source.expect("validated decision source");
            if source.outcome == WriteOutcome::Fenced as i32 {
                DocumentPublicationTargetState::SkippedFenced
            } else {
                DocumentPublicationTargetState::Published
            }
        } else if pending {
            DocumentPublicationTargetState::Staged
        } else {
            DocumentPublicationTargetState::Pending
        };
        Ok(DocumentPublicationTarget {
            index_key: key.to_vec(),
            state: target as i32,
            committed_sequence: committed,
            stats_epoch: 0,
            stats_incarnation: Vec::new(),
        })
    }

    /// Persist the skip of the fenced `version` before its epoch-only
    /// manifest is published. The skip takes the version's sequence in the
    /// journal like any decision, so the index's cursor passes it.
    pub fn prepare_fenced_skip(
        &self,
        key: &[u8],
        version: &DocumentVersion,
        before: &OpenedSegmentSet,
        after: &OpenedSegmentSet,
    ) -> Result<ProjectionIntent, Status> {
        index_key(key)?;
        if !self.durable {
            return Err(Status::failed_precondition(
                "projection publication requires a durable source catalog",
            ));
        }
        let current = self.accepted_at(version.accepted_sequence)?;
        if &current != version || current.outcome != WriteOutcome::Fenced as i32 {
            return Err(Status::failed_precondition(
                "a fenced skip names the fenced version ordered history holds",
            ));
        }
        validate_skip_transition(before, after)?;
        if let Some(owner) = &after.manifest().source_owner {
            if owner.decode().map_err(Status::data_loss)? != self.index_owner(key)? {
                return Err(Status::failed_precondition(
                    "source publication belongs to another index owner",
                ));
            }
        }
        let history_id = self.index_owner(key)?.history_id;
        let mut intent = ProjectionIntent {
            index_key: key.to_vec(),
            history_id: history_id.clone(),
            source: Some(current),
            before_manifest_sha256: manifest_hash(before.manifest())?,
            after_manifest_sha256: manifest_hash(after.manifest())?,
            before_epoch: before.epoch(),
            after_epoch: after.epoch(),
            rows: 0,
            intent_id: Vec::new(),
        };
        intent.intent_id = intent_hash(&intent);
        validate_intent(&intent, key, &history_id)?;
        self.record_intent(key, &history_id, &intent, before.is_empty())?;
        Ok(intent)
    }
}

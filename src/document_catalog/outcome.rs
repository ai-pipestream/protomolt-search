//! Write outcomes (docs/document-writes.md, "Write outcomes"): the record
//! of what a durable write became once its lease was judged. A write whose
//! lease is still open when its commit returns is ACCEPTED in the same
//! transaction. One whose lease lapsed before its commit returned is
//! marked UNCONFIRMED in a transaction of its own, then settled under a
//! fresh lease: ACCEPTED when the actor's right is still current, FENCED
//! with the control revision at which the right was found gone. The marks
//! live on the operation record (so a retry replays the settled decision)
//! and on the version (so a history reader can tell a fenced version by
//! name). A version's bytes are never rewritten: only the outcome fields
//! move, and only forward, ACCEPTED to UNCONFIRMED to one of the two.
use super::*;

/// The settled decision of an unconfirmed write.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Settled {
    /// The actor's right was current under a fresh lease.
    Accepted,
    /// The right was gone at this control revision.
    Fenced { at_revision: u64 },
}

impl DocumentCatalog {
    /// Mark the operation and its version UNCONFIRMED: the commit returned
    /// durable after the lease lapsed. Idempotent for a record already
    /// unconfirmed; a record already settled is left as it is and its
    /// outcome returned, so a late caller learns the decision instead of
    /// moving it backwards.
    pub(crate) fn mark_unconfirmed(
        &self,
        principal: Option<&str>,
        operation_id: &[u8],
    ) -> Result<WriteOutcome, Status> {
        self.move_outcome(principal, operation_id, |current| match current {
            WriteOutcome::Accepted => Some((WriteOutcome::Unconfirmed, 0)),
            WriteOutcome::Unconfirmed => None,
            WriteOutcome::Fenced => None,
        })
    }

    /// Settle an UNCONFIRMED write. A record that is not unconfirmed is
    /// not moved: ACCEPTED stays (the decision was already made in the
    /// write's own transaction), FENCED stays (a fence is final).
    pub(crate) fn settle_outcome(
        &self,
        principal: Option<&str>,
        operation_id: &[u8],
        settled: Settled,
    ) -> Result<WriteOutcome, Status> {
        self.move_outcome(principal, operation_id, |current| {
            match (current, settled) {
                (WriteOutcome::Unconfirmed, Settled::Accepted) => Some((WriteOutcome::Accepted, 0)),
                (WriteOutcome::Unconfirmed, Settled::Fenced { at_revision }) => {
                    Some((WriteOutcome::Fenced, at_revision))
                }
                _ => None,
            }
        })
    }

    /// The operation record of one write, or `None` when the actor has no
    /// such operation: its request hash, receipt, outcome and the
    /// revision a fence was marked at.
    pub(crate) fn recorded_operation(
        &self,
        principal: Option<&str>,
        operation_id: &[u8],
    ) -> Result<Option<DocumentOperation>, Status> {
        let operation_key = actors::OperationKey::new(principal, operation_id)?;
        let read = self.database.begin_read().map_err(storage)?;
        let operations = read.open_table(operation_key.table()).map_err(storage)?;
        let Some(bytes) = operations
            .get(operation_key.bytes.as_slice())
            .map_err(storage)?
        else {
            return Ok(None);
        };
        Ok(Some(decode(bytes.value())?))
    }

    fn move_outcome(
        &self,
        principal: Option<&str>,
        operation_id: &[u8],
        next: impl FnOnce(WriteOutcome) -> Option<(WriteOutcome, u64)>,
    ) -> Result<WriteOutcome, Status> {
        let operation_key = actors::OperationKey::new(principal, operation_id)?;
        let mut transaction = self.database.begin_write().map_err(storage)?;
        transaction
            .set_durability(Durability::Immediate)
            .map_err(storage)?;
        let header = seal::header_from(self, &transaction)?;
        operation_key.check_header(&header)?;
        let mut operation: DocumentOperation = {
            let operations = transaction
                .open_table(operation_key.table())
                .map_err(storage)?;
            let bytes = operations
                .get(operation_key.bytes.as_slice())
                .map_err(storage)?
                .ok_or_else(|| {
                    Status::not_found("the actor has no operation with this id to settle")
                })?;
            decode(bytes.value())?
        };
        let current = outcome_of(operation.outcome)?;
        let Some((outcome, fenced_at_revision)) = next(current) else {
            return Ok(current);
        };
        let receipt = operation
            .receipt
            .as_ref()
            .ok_or_else(|| Status::data_loss("operation receipt missing"))?;
        let version_key = DocumentVersionKey {
            document_key: receipt.document_key.clone(),
            version: receipt.version,
        }
        .encode_to_vec();
        let mut version: DocumentVersion = {
            let versions = transaction.open_table(VERSIONS).map_err(storage)?;
            let bytes = versions
                .get(version_key.as_slice())
                .map_err(storage)?
                .ok_or_else(|| {
                    Status::data_loss("operation names a version the catalog does not hold")
                })?;
            decode(bytes.value())?
        };
        if version.accepted_sequence != receipt.accepted_sequence
            || outcome_of(version.outcome)? != current
        {
            return Err(Status::data_loss(
                "operation record and its version disagree on sequence or outcome",
            ));
        }
        version.outcome = outcome as i32;
        version.fenced_at_revision = fenced_at_revision;
        operation.outcome = outcome as i32;
        operation.fenced_at_revision = fenced_at_revision;
        let version_bytes = version.encode_to_vec();
        transaction
            .open_table(VERSIONS)
            .map_err(storage)?
            .insert(version_key.as_slice(), version_bytes.as_slice())
            .map_err(storage)?;
        {
            let mut heads = transaction.open_table(HEADS).map_err(storage)?;
            let is_head = heads
                .get(receipt.document_key.as_slice())
                .map_err(storage)?
                .map(|bytes| decode::<DocumentVersion>(bytes.value()))
                .transpose()?
                .is_some_and(|head| head.version == receipt.version);
            if is_head {
                heads
                    .insert(receipt.document_key.as_slice(), version_bytes.as_slice())
                    .map_err(storage)?;
            }
        }
        transaction
            .open_table(operation_key.table())
            .map_err(storage)?
            .insert(
                operation_key.bytes.as_slice(),
                operation.encode_to_vec().as_slice(),
            )
            .map_err(storage)?;
        transaction.commit().map_err(storage)?;
        Ok(outcome)
    }
}

pub(super) fn outcome_of(value: i32) -> Result<WriteOutcome, Status> {
    WriteOutcome::try_from(value)
        .map_err(|_| Status::data_loss("operation record carries an unknown write outcome"))
}

/// The rejection a fenced write replays: the row is durable and named,
/// and it was never admitted.
pub(super) fn fenced(receipt: &DocumentWriteReceipt, at_revision: u64) -> Status {
    Status::failed_precondition(format!(
        "write of version {} at sequence {} became durable after its admission lapsed and its right was gone at control revision {}: the version is fenced under write epoch {} and was never admitted; a retry replays this decision",
        receipt.version, receipt.accepted_sequence, at_revision, receipt.write_epoch
    ))
}

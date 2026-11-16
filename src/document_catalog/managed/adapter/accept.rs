//! Admitted writes and the settlement of writes whose lease lapsed
//! while their commit was becoming durable.
use super::*;

impl ActiveManagedCatalog {
    /// Accept one write under the admission: current Ingest for the actor
    /// and the owner ACTIVE under this handle's epoch at entry, and the
    /// admission still within its lease at the final check inside the
    /// source transaction, immediately before commit. Between the two
    /// checks the admission's shared guard keeps policy and ownership
    /// from changing on this store; only time passes, and the final check
    /// is where it is measured. Retries resolve from the actor-scoped
    /// record.
    ///
    /// The commit's return is judged against the lease once more: a write
    /// whose lease is still open is `Accepted`; one whose lease lapsed while
    /// the commit was becoming durable is recorded UNCONFIRMED and returned
    /// as `Unconfirmed`, for the caller to settle under a fresh lease
    /// with [`settle`](Self::settle). A retry that finds an UNCONFIRMED
    /// record settles it under its own admission, which is fresh at
    /// entry, so the retry answers with the decision.
    pub fn accept(
        &self,
        admission: &SourceAdmission<'_>,
        request: &crate::pb::AcceptDocumentRequest,
    ) -> Result<Acceptance, Status> {
        let _judging = self
            .judging
            .read()
            .map_err(|_| Status::internal("write judgement barrier poisoned"))?;
        // A retry of an operation awaiting its decision, or already
        // fenced, is answered from the record before the entry check:
        // the decision is what the retry asks for, and for an actor
        // whose right is gone the decision is the fence, not a
        // permission error that would hide it.
        if let Some(answer) = self.recorded_answer(admission, request)? {
            return Ok(answer);
        }
        let decision = self.admit(admission, AccessAction::Ingest)?;
        let fence = || admission.check_fresh();
        let written = self.inner.accept_as(
            request,
            Some(&decision.principal),
            Some(&fence),
            self.activation.write_epoch,
        )?;
        match written.outcome {
            WriteOutcome::Unconfirmed => {
                // The record was marked between this call's first look
                // and the write's own retry lookup, by the call that wrote
                // it: settle it here, as the first look would have.
                self.settled_replay(admission, &decision.principal, &request.operation_id)
            }
            WriteOutcome::Fenced => Err(Status::internal(
                "a fenced record was returned as a receipt",
            )),
            WriteOutcome::Accepted if written.receipt.replayed => {
                Ok(Acceptance::Accepted(written.receipt))
            }
            WriteOutcome::Accepted => match admission.check_fresh() {
                Ok(()) => Ok(Acceptance::Accepted(written.receipt)),
                Err(_) => {
                    self.inner
                        .mark_unconfirmed(Some(&decision.principal), &request.operation_id)?;
                    Ok(Acceptance::Unconfirmed(written.receipt))
                }
            },
        }
    }

    /// Answer a retry whose record awaits its decision or is fenced.
    /// `None` when the actor has no record of the operation, or the record
    /// is ACCEPTED and the write path replays it.
    fn recorded_answer(
        &self,
        admission: &SourceAdmission<'_>,
        request: &crate::pb::AcceptDocumentRequest,
    ) -> Result<Option<Acceptance>, Status> {
        let Some(operation) = self
            .inner
            .recorded_operation(Some(admission.principal()), &request.operation_id)?
        else {
            return Ok(None);
        };
        let request_sha = crate::sha256::digest(&request.encode_to_vec());
        if operation.request_sha256.as_slice() != request_sha {
            return Err(crate::raft::reasons::reason(
                Status::already_exists("operation_id was used for a different document write"),
                crate::raft::reasons::OUTCOME_OPERATION_REUSED,
            ));
        }
        match outcome::outcome_of(operation.outcome)? {
            WriteOutcome::Accepted => Ok(None),
            WriteOutcome::Unconfirmed => self
                .settled_replay(admission, admission.principal(), &request.operation_id)
                .map(Some),
            WriteOutcome::Fenced => {
                let receipt = operation
                    .receipt
                    .ok_or_else(|| Status::data_loss("operation receipt missing"))?;
                Err(outcome::fenced(&receipt, operation.fenced_at_revision))
            }
        }
    }

    /// Settle an UNCONFIRMED record for the retry that found it: the
    /// accepted receipt marked replayed, or the fence as the rejection.
    fn settled_replay(
        &self,
        admission: &SourceAdmission<'_>,
        principal: &str,
        operation_id: &[u8],
    ) -> Result<Acceptance, Status> {
        match self.settle_under(admission, principal, operation_id)? {
            Settlement::Accepted(receipt) => {
                Ok(Acceptance::Accepted(crate::pb::DocumentWriteReceipt {
                    replayed: true,
                    ..receipt
                }))
            }
            Settlement::Fenced(rejection) => Err(rejection),
        }
    }

    /// Settle a write returned `Unconfirmed` by [`accept`](Self::accept),
    /// under a fresh leased admission for the same actor: the entry
    /// check run again. Current right: the record becomes ACCEPTED and
    /// the receipt is returned. Right gone (the grant revoked, the owner
    /// no longer ACTIVE under this epoch): the record becomes FENCED at
    /// the store's applied control revision and the settlement names
    /// the durable version; every retry replays that rejection. An
    /// admission that is itself no longer fresh settles nothing and is
    /// an error by name, so the record stays UNCONFIRMED for the next
    /// attempt.
    pub fn settle(
        &self,
        admission: &SourceAdmission<'_>,
        principal: &str,
        operation_id: &[u8],
    ) -> Result<Settlement, Status> {
        require_authority(admission, &self.authority)?;
        if admission.principal() != principal {
            return Err(Status::permission_denied(
                "a write is settled under an admission for the actor that wrote it",
            ));
        }
        self.settle_under(admission, principal, operation_id)
    }

    fn settle_under(
        &self,
        admission: &SourceAdmission<'_>,
        principal: &str,
        operation_id: &[u8],
    ) -> Result<Settlement, Status> {
        let Some(operation) = self
            .inner
            .recorded_operation(Some(principal), operation_id)?
        else {
            return Err(crate::raft::reasons::reason(
                Status::not_found("the actor has no operation with this id to settle"),
                crate::raft::reasons::OUTCOME_NO_OPERATION,
            ));
        };
        let receipt = operation
            .receipt
            .ok_or_else(|| Status::data_loss("operation receipt missing"))?;
        match outcome::outcome_of(operation.outcome)? {
            WriteOutcome::Accepted => return Ok(Settlement::Accepted(receipt)),
            WriteOutcome::Fenced => {
                return Ok(Settlement::Fenced(outcome::fenced(
                    &receipt,
                    operation.fenced_at_revision,
                )))
            }
            WriteOutcome::Unconfirmed => {}
        }
        // A lease that lapsed again decides nothing.
        admission.check_fresh()?;
        #[cfg(any(test, feature = "fault-injection"))]
        if let Some(pause) = self.settle_pause.lock().unwrap().take() {
            std::thread::sleep(pause);
        }
        let settled = match self.admit(admission, AccessAction::Ingest) {
            Ok(_) => outcome::Settled::Accepted,
            Err(_) => {
                // The entry check fails for a lapsed lease as well as for
                // a right that is gone; only the second is a fence. A
                // lease that lapsed between the check above and the entry
                // check decides nothing, and the record stays
                // unconfirmed for the next attempt.
                admission.check_fresh()?;
                outcome::Settled::Fenced {
                    at_revision: admission.applied_control_revision(),
                }
            }
        };
        match self
            .inner
            .settle_outcome(Some(principal), operation_id, settled)?
        {
            WriteOutcome::Accepted => Ok(Settlement::Accepted(receipt)),
            WriteOutcome::Fenced => {
                // Fenced here, or fenced by a concurrent settlement whose
                // revision the record now names.
                let at_revision = match settled {
                    outcome::Settled::Fenced { at_revision } => at_revision,
                    outcome::Settled::Accepted => self
                        .inner
                        .recorded_operation(Some(principal), operation_id)?
                        .map(|operation| operation.fenced_at_revision)
                        .unwrap_or(0),
                };
                Ok(Settlement::Fenced(outcome::fenced(&receipt, at_revision)))
            }
            WriteOutcome::Unconfirmed => {
                Err(Status::internal("an unconfirmed record was not settled"))
            }
        }
    }
}

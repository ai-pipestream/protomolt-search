//! What the managed publisher and the pending-actor reconciler read from
//! an activated source, each under current Admin.
use super::*;
use crate::pb::storage::{ActorOperationKey, DocumentOperation, DocumentVersion};

/// A durable write whose outcome is still UNCONFIRMED: the actor whose
/// fresh lease settles it, and the operation that names it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingOperation {
    pub principal: String,
    pub operation_id: Vec<u8>,
    pub version: u64,
    pub accepted_sequence: u64,
}

/// What one reconciliation pass decided for the writes it found
/// UNCONFIRMED: settled ACCEPTED, settled FENCED, or left unconfirmed
/// because no fresh lease of the actor could decide them now.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Reconciled {
    pub accepted: Vec<PendingOperation>,
    pub fenced: Vec<PendingOperation>,
    pub unsettled: Vec<PendingOperation>,
}

impl Reconciled {
    /// File `operation` under what `settled` decided.
    pub fn record(&mut self, operation: PendingOperation, settled: Result<Settlement, Status>) {
        match settled {
            Ok(Settlement::Accepted(_)) => self.accepted.push(operation),
            Ok(Settlement::Fenced(_)) => self.fenced.push(operation),
            Err(_) => self.unsettled.push(operation),
        }
    }
}

impl ActiveManagedCatalog {
    /// Settle up to `limit` UNCONFIRMED writes, listed under `admin`'s
    /// Admin admission. `settle` decides each one under a fresh admission
    /// of the write's own actor, whose Ingest right is the one judged; an
    /// error leaves the write unconfirmed for the next pass.
    pub fn reconcile_unconfirmed(
        &self,
        admin: &SourceAdmission<'_>,
        limit: usize,
        mut settle: impl FnMut(&PendingOperation) -> Result<Settlement, Status>,
    ) -> Result<Reconciled, Status> {
        let mut reconciled = Reconciled::default();
        for operation in self.unconfirmed_operations(admin, limit)? {
            let settled = settle(&operation);
            reconciled.record(operation, settled);
        }
        Ok(reconciled)
    }

    /// The source a trusted publisher stages and journals from.
    pub fn publication_source(
        &self,
        admission: &SourceAdmission<'_>,
    ) -> Result<Arc<DocumentCatalog>, Status> {
        self.admit(admission, AccessAction::Admin)?;
        Ok(Arc::clone(&self.inner))
    }

    /// The source a status reader inspects under Admin or Ingest.
    pub fn publication_status_source(
        &self,
        admission: &SourceAdmission<'_>,
    ) -> Result<Arc<DocumentCatalog>, Status> {
        self.admit_publication_read(admission)?;
        Ok(Arc::clone(&self.inner))
    }

    /// The settled version visible to a publication-status reader.
    pub fn settled_publication_status(
        &self,
        admission: &SourceAdmission<'_>,
        sequence: u64,
    ) -> Result<Option<DocumentVersion>, Status> {
        self.admit_publication_read(admission)?;
        self.settled_version_at(sequence)
    }

    fn admit_publication_read(
        &self,
        admission: &SourceAdmission<'_>,
    ) -> Result<crate::pb::AccessDecision, Status> {
        match self.admit(admission, AccessAction::Admin) {
            Ok(decision) => Ok(decision),
            Err(error) if error.code() == tonic::Code::PermissionDenied => {
                self.admit(admission, AccessAction::Ingest)
            }
            Err(error) => Err(error),
        }
    }

    fn settled_version_at(&self, sequence: u64) -> Result<Option<DocumentVersion>, Status> {
        let _judged = self
            .judging
            .write()
            .map_err(|_| Status::internal("write judgement barrier poisoned"))?;
        match self.inner.accepted_at(sequence) {
            Ok(version) => Ok(Some(version)),
            Err(status) if status.code() == tonic::Code::NotFound => Ok(None),
            Err(status) => Err(status),
        }
    }

    /// The accepted version at `sequence` with an outcome no write is
    /// still judging, or `None` past the accepted history. The read waits
    /// out every write between its commit and its judgement, so an
    /// ACCEPTED outcome read here is final; an UNCONFIRMED one moves only
    /// by settlement.
    pub fn settled_version(
        &self,
        admission: &SourceAdmission<'_>,
        sequence: u64,
    ) -> Result<Option<DocumentVersion>, Status> {
        self.admit(admission, AccessAction::Admin)?;
        self.settled_version_at(sequence)
    }

    /// Up to `limit` writes still UNCONFIRMED, in accepted-sequence order,
    /// for a reconciler to settle each under a fresh lease of its own actor.
    pub fn unconfirmed_operations(
        &self,
        admission: &SourceAdmission<'_>,
        limit: usize,
    ) -> Result<Vec<PendingOperation>, Status> {
        self.admit(admission, AccessAction::Admin)?;
        let read = self.inner.database.begin_read().map_err(storage)?;
        let operations = read.open_table(actors::OPERATIONS).map_err(storage)?;
        let mut pending = Vec::new();
        for entry in operations.iter().map_err(storage)? {
            let (key, value) = entry.map_err(storage)?;
            let operation: DocumentOperation = decode(value.value())?;
            if outcome::outcome_of(operation.outcome)? != WriteOutcome::Unconfirmed {
                continue;
            }
            let key: ActorOperationKey = decode(key.value())?;
            let receipt = operation
                .receipt
                .ok_or_else(|| Status::data_loss("operation receipt missing"))?;
            pending.push(PendingOperation {
                principal: key.principal,
                operation_id: key.operation_id,
                version: receipt.version,
                accepted_sequence: receipt.accepted_sequence,
            });
        }
        pending.sort_by_key(|operation| operation.accepted_sequence);
        pending.truncate(limit);
        Ok(pending)
    }
}

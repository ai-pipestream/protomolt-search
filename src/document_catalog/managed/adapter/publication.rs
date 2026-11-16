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

    /// Up to `limit` writes still UNCONFIRMED, in the order of their
    /// encoded actor-scoped operation keys, for a
    /// reconciler to settle each under a fresh lease of its own actor.
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
            if pending.len() == limit {
                break;
            }
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
        Ok(pending)
    }
}

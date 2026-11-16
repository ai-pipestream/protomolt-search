//! The managed-source adapters: a bound source held closed, and the
//! activated source that admits writes under its committed fence.
use super::*;
use crate::pb::storage::WriteOutcome;
use crate::pb::storage::{
    DocumentCatalogCheckpoint, PreparedSourceOwner, SourceAuthorityCommand, SourceAuthorityDecision,
};
use crate::pb::AccessAction;
use crate::source_authority::{SourceAdmission, SourceAuthorityStore, VerifiedOwnerCompletion};
use std::sync::Arc;

mod accept;
mod active;
mod bind;
mod prepared;
mod publication;

pub use publication::{PendingOperation, Reconciled};

/// A locally bound source retained under its committed preparation. It has
/// no acceptance, journal mutation, export or inner-catalog accessor. Open
/// and metadata inspection require current committed administration rights.
pub struct PreparedManagedCatalog {
    pub(super) inner: DocumentCatalog,
    binding: SourceManagedBinding,
    authority: SourceAuthorityStore,
    #[cfg(any(test, feature = "fault-injection"))]
    pub(super) activate_fault: Option<BindFault>,
}

/// What [`ActiveManagedCatalog::accept`] returns: the receipt of a
/// write whose lease was still open when its commit returned, or the
/// receipt of one whose lease had lapsed by then, recorded UNCONFIRMED
/// and awaiting [`ActiveManagedCatalog::settle`].
#[derive(Clone, Debug, PartialEq)]
pub enum Acceptance {
    Accepted(crate::pb::DocumentWriteReceipt),
    Unconfirmed(crate::pb::DocumentWriteReceipt),
}

impl Acceptance {
    pub fn receipt(&self) -> &crate::pb::DocumentWriteReceipt {
        match self {
            Acceptance::Accepted(receipt) | Acceptance::Unconfirmed(receipt) => receipt,
        }
    }

    /// The receipt whatever the judgement; a caller that uses this
    /// answers for an unconfirmed write itself.
    pub fn into_receipt(self) -> crate::pb::DocumentWriteReceipt {
        match self {
            Acceptance::Accepted(receipt) | Acceptance::Unconfirmed(receipt) => receipt,
        }
    }
}

/// What [`ActiveManagedCatalog::settle`] decided: the write is accepted
/// with its receipt, or fenced with the rejection every retry replays.
/// An error from `settle` means no decision was made.
#[derive(Debug)]
pub enum Settlement {
    Accepted(crate::pb::DocumentWriteReceipt),
    Fenced(Status),
}

/// A managed source activated under its committed fence. Every write is
/// admitted through a `SourceAdmission` that finds the owner ACTIVE under
/// exactly this epoch and the actor permitted, and the admission is held
/// through the source commit.
pub struct ActiveManagedCatalog {
    pub(super) inner: Arc<DocumentCatalog>,
    binding: SourceManagedBinding,
    activation: SourceManagedActivation,
    authority: SourceAuthorityStore,
    /// Held shared by every write from before its commit until its
    /// outcome is judged, and exclusively by a publisher reading an
    /// outcome, so a version read ACCEPTED can no longer become
    /// UNCONFIRMED.
    judging: std::sync::RwLock<()>,
    #[cfg(any(test, feature = "fault-injection"))]
    settle_pause: std::sync::Mutex<Option<std::time::Duration>>,
}

/// Refuse an admission acquired from a different source authority.
fn require_authority(
    admission: &SourceAdmission<'_>,
    authority: &SourceAuthorityStore,
) -> Result<(), Status> {
    if admission.identity() != authority.identity() {
        return Err(crate::raft::reasons::reason(
            Status::failed_precondition("admission was acquired from another source authority"),
            crate::raft::reasons::ADMISSION_FOREIGN_AUTHORITY,
        ));
    }
    Ok(())
}

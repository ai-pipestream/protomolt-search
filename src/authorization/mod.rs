//! Revisioned workspace/collection capabilities at the public service boundary.

use crate::pb::{AccessAction, AccessDecision};
use tokio::sync::watch;
use tonic::Status;

mod authority;
mod permit;
mod policy;
mod stream;

pub use authority::PolicyAuthority;
pub use permit::{AccessPermit, PinnedAccess};
#[cfg(feature = "net")]
pub(crate) use policy::{authorize_policy_snapshot, validate_policy_snapshot};
pub use stream::AuthorizedStream;

/// Adapter for the ecosystem's workspace authority. Implementations must publish
/// a new revision whenever a decision may change, including revocation. The
/// revision channel and `authorize` must observe one ordered policy history.
pub trait Authorizer: std::fmt::Debug + Send + Sync {
    fn authorize(
        &self,
        principal: &str,
        collection: &str,
        action: AccessAction,
    ) -> Result<AccessDecision, Status>;
    fn subscribe(&self) -> watch::Receiver<u64>;

    /// Serialize policy replacement against a synchronous operation. The guard
    /// must keep this exact decision valid until dropped. Remote adapters need
    /// an equivalent enforcement protocol; snapshot checks are insufficient.
    fn pin(&self, _expected: &AccessDecision) -> Result<Box<dyn AuthorizationGuard + '_>, Status> {
        Err(Status::unimplemented(
            "authorization provider does not support pinned authorization",
        ))
    }
}

/// Provider-owned admission guard. Dropping it releases the policy fence.
pub trait AuthorizationGuard {
    fn decision(&self) -> &AccessDecision;
}

/// The one access policy format this build reads; any other is refused.
pub const POLICY_FORMAT: u32 = 3;

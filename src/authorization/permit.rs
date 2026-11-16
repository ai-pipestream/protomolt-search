//! Decisions held across asynchronous work and pinned through a commit.

use super::{AuthorizationGuard, Authorizer};
use crate::pb::{AccessAction, AccessDecision};
use std::sync::Arc;
use tokio::sync::watch;
use tonic::Status;

/// Permission held through synchronous work and its commit. Acquire before
/// node/catalog locks and the database writer. Never hold across an await or
/// reenter authorization or policy replacement while a pin is held.
#[must_use = "dropping the pin releases authorization admission"]
pub struct PinnedAccess<'a> {
    guard: Box<dyn AuthorizationGuard + 'a>,
}
impl PinnedAccess<'_> {
    pub fn decision(&self) -> &AccessDecision {
        self.guard.decision()
    }
}

/// A decision held across asynchronous work. A policy change invalidates the
/// operation even if the new policy would also allow it; callers must retry.
#[derive(Clone, Debug)]
pub struct AccessPermit {
    authority: Arc<dyn Authorizer>,
    decision: AccessDecision,
    pub(super) revisions: watch::Receiver<u64>,
}
impl AccessPermit {
    pub fn acquire(
        authority: Arc<dyn Authorizer>,
        principal: &str,
        collection: &str,
        action: AccessAction,
    ) -> Result<Self, Status> {
        // Subscribe first so no replacement between deciding and subscribing can be missed.
        let revisions = authority.subscribe();
        let decision = authority.authorize(principal, collection, action)?;
        if decision.principal != principal
            || decision.collection != collection
            || decision.action != action as i32
            || decision.workspace.is_empty()
            || decision.policy_revision == 0
        {
            return Err(Status::permission_denied("invalid authorization decision"));
        }
        if decision.document_visibility.is_some() {
            if action != AccessAction::Search {
                return Err(Status::permission_denied(
                    "document visibility requires a search decision",
                ));
            }
            crate::visibility::VisibilityScope::new(decision.document_visibility.as_ref())
                .map_err(|_| Status::permission_denied("invalid document visibility decision"))?;
        }
        if let Some(fields) = &decision.field_permissions {
            if action != AccessAction::Search {
                return Err(Status::permission_denied(
                    "field permissions require a search decision",
                ));
            }
            crate::field_permissions::FieldScope::new(fields)
                .map_err(|_| Status::permission_denied("invalid field permission decision"))?;
        }
        let permit = Self {
            authority,
            decision,
            revisions,
        };
        permit.check()?;
        Ok(permit)
    }
    pub fn decision(&self) -> &AccessDecision {
        &self.decision
    }
    /// Pin the admitted revision through a synchronous commit. Do not call
    /// `check` under this guard: providers may retain locks that make recursive
    /// authorization deadlock a queued policy replacement.
    pub fn pin(&self) -> Result<PinnedAccess<'_>, Status> {
        if *self.revisions.borrow() != self.decision.policy_revision {
            return Err(crate::error_disclosure::policy_changed());
        }
        let guard = self.authority.pin(&self.decision)?;
        if guard.decision() != &self.decision {
            return Err(Status::permission_denied(
                "invalid pinned authorization decision",
            ));
        }
        if *self.revisions.borrow() != self.decision.policy_revision {
            return Err(crate::error_disclosure::policy_changed());
        }
        Ok(PinnedAccess { guard })
    }
    pub fn check(&self) -> Result<(), Status> {
        if *self.revisions.borrow() != self.decision.policy_revision {
            return Err(crate::error_disclosure::policy_changed());
        }
        let current = self.authority.authorize(
            &self.decision.principal,
            &self.decision.collection,
            AccessAction::try_from(self.decision.action)
                .map_err(|_| Status::permission_denied("invalid authorization action"))?,
        )?;
        if current != self.decision || *self.revisions.borrow() != self.decision.policy_revision {
            return Err(crate::error_disclosure::policy_changed());
        }
        Ok(())
    }
}

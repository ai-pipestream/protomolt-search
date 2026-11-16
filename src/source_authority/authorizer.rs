//! The store as the workspace authorizer of managed hosting.
use super::*;

struct AdmittedDecision<'a> {
    _admission: SourceAdmission<'a>,
    decision: AccessDecision,
}

impl AuthorizationGuard for AdmittedDecision<'_> {
    fn decision(&self) -> &AccessDecision {
        &self.decision
    }
}

/// The committed collection policy is the workspace authority for managed
/// hosting: permits resolve against it, and a pin is one `SourceAdmission`.
impl Authorizer for SourceAuthorityStore {
    fn authorize(
        &self,
        principal: &str,
        collection: &str,
        action: AccessAction,
    ) -> Result<AccessDecision, Status> {
        SourceAuthorityStore::authorize(self, principal, collection, action)
    }

    fn subscribe(&self) -> watch::Receiver<u64> {
        self.inner.revisions.subscribe()
    }

    fn pin(&self, expected: &AccessDecision) -> Result<Box<dyn AuthorizationGuard + '_>, Status> {
        let admission = self.admission(&expected.principal)?;
        let action = AccessAction::try_from(expected.action)
            .map_err(|_| Status::permission_denied("invalid authorization action"))?;
        let decision = admission.authorize(&expected.collection, action)?;
        if &decision != expected {
            return Err(crate::error_disclosure::policy_changed());
        }
        Ok(Box::new(AdmittedDecision {
            _admission: admission,
            decision,
        }))
    }
}

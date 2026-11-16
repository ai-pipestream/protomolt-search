//! The in-process policy authority: publication, admission and drain.

use super::policy::Policy;
use super::{AuthorizationGuard, Authorizer};
use crate::pb::{AccessAction, AccessDecision, AccessPolicy};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use tokio::sync::watch;
use tonic::Status;

#[derive(Debug)]
struct PolicyEpoch {
    policy: Policy,
    active: AtomicUsize,
    notification: Mutex<()>,
    drained: Condvar,
}
impl PolicyEpoch {
    fn new(policy: Policy) -> Self {
        Self {
            policy,
            active: AtomicUsize::new(0),
            notification: Mutex::new(()),
            drained: Condvar::new(),
        }
    }

    // The caller must retain the publication read lock until admission returns.
    // fetch_update is deprecated in favor of try_update from Rust 1.99, but the
    // Pi binaries are built on 1.97, which lacks try_update.
    #[allow(deprecated)]
    fn admit(self: &Arc<Self>, decision: AccessDecision) -> Result<PolicyGuard, Status> {
        self.active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                active.checked_add(1)
            })
            .map_err(|_| Status::resource_exhausted("authorization admission count exhausted"))?;
        Ok(PolicyGuard {
            epoch: Arc::clone(self),
            decision,
        })
    }

    fn wait_drained(&self) {
        // This mutex protects only notification ordering. Recovering its poison
        // does not accept a partially mutated policy or admission counter.
        let mut notification = self
            .notification
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while self.active.load(Ordering::Acquire) != 0 {
            notification = self
                .drained
                .wait(notification)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
}

struct PolicyGuard {
    epoch: Arc<PolicyEpoch>,
    decision: AccessDecision,
}
impl AuthorizationGuard for PolicyGuard {
    fn decision(&self) -> &AccessDecision {
        &self.decision
    }
}
impl Drop for PolicyGuard {
    // fetch_update is deprecated in favor of try_update from Rust 1.99, but the
    // Pi binaries are built on 1.97, which lacks try_update.
    #[allow(deprecated)]
    fn drop(&mut self) {
        let previous = self
            .epoch
            .active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                active.checked_sub(1)
            })
            .expect("authorization admission count underflow");
        if previous == 1 {
            // Taking the same mutex as the waiter prevents a last-drop wakeup
            // from passing between its nonzero check and Condvar::wait.
            let _notification = self
                .epoch
                .notification
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            self.epoch.drained.notify_all();
        }
    }
}

/// In-process snapshot authority. Loading and validating a replacement happens
/// before publication; readers never observe a partially replaced policy.
#[derive(Debug)]
pub struct PolicyAuthority {
    policy: RwLock<Arc<PolicyEpoch>>,
    replacements: Mutex<()>,
    revisions: watch::Sender<u64>,
}
impl PolicyAuthority {
    pub fn new(policy: AccessPolicy) -> Result<Self, String> {
        let policy = Policy::validate(policy)?;
        let (revisions, _) = watch::channel(policy.revision);
        Ok(Self {
            policy: RwLock::new(Arc::new(PolicyEpoch::new(policy))),
            replacements: Mutex::new(()),
            revisions,
        })
    }

    /// Publish a policy, then drain synchronous operations admitted under the
    /// previous revision. New checks and admissions use the published policy
    /// during the drain. Success means the previous admissions have finished;
    /// observing the watched revision alone does not establish that barrier.
    pub fn replace(&self, policy: AccessPolicy) -> Result<(), String> {
        let next = Arc::new(PolicyEpoch::new(Policy::validate(policy)?));
        // Serialize through completed drain. After an unwind, fail closed:
        // a later replacement must not skip an old epoch that was published
        // and never drained.
        let _replacement = self.replacements.lock().map_err(|_| {
            "access policy replacement lock poisoned; prior drain may be incomplete"
        })?;
        let mut current = self
            .policy
            .write()
            .map_err(|_| "access policy lock poisoned")?;
        if next.policy.revision <= current.policy.revision {
            return Err("access policy revision must increase".into());
        }
        let previous = std::mem::replace(&mut *current, next);
        self.revisions.send_replace(current.policy.revision);
        drop(current);
        previous.wait_drained();
        Ok(())
    }
}
impl Authorizer for PolicyAuthority {
    fn authorize(
        &self,
        principal: &str,
        collection: &str,
        action: AccessAction,
    ) -> Result<AccessDecision, Status> {
        let epoch = Arc::clone(
            &*self
                .policy
                .read()
                .map_err(|_| Status::internal("access policy lock poisoned"))?,
        );
        epoch.policy.authorize(principal, collection, action)
    }
    fn pin(&self, expected: &AccessDecision) -> Result<Box<dyn AuthorizationGuard + '_>, Status> {
        let epoch = self
            .policy
            .read()
            .map_err(|_| Status::internal("access policy lock poisoned"))?;
        let action = AccessAction::try_from(expected.action)
            .map_err(|_| Status::permission_denied("invalid authorization action"))?;
        let decision = epoch
            .policy
            .authorize(&expected.principal, &expected.collection, action)?;
        if &decision != expected {
            return Err(crate::error_disclosure::policy_changed());
        }
        Ok(Box::new(epoch.admit(decision)?))
    }

    fn subscribe(&self) -> watch::Receiver<u64> {
        self.revisions.subscribe()
    }
}

#[cfg(test)]
mod pin_tests;

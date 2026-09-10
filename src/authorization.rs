//! Revisioned workspace/collection capabilities at the public service boundary.

use crate::pb::{AccessAction, AccessDecision, AccessPolicy};
use std::collections::{BTreeMap, BTreeSet};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::task::{Context, Poll};
use tokio::sync::watch;
use tokio_stream::{wrappers::WatchStream, Stream};
use tonic::Status;

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

#[derive(Debug)]
struct Policy {
    revision: u64,
    resources: BTreeMap<String, String>,
    grants: BTreeMap<(String, String), BTreeSet<i32>>,
    views: BTreeMap<(String, String), crate::pb::DocumentVisibility>,
    fields: BTreeMap<(String, String), crate::pb::FieldPermissions>,
}

impl Policy {
    fn validate(input: AccessPolicy) -> Result<Self, String> {
        if !matches!(input.format_version, 1 | 2 | 3) {
            return Err(format!(
                "unsupported access policy format_version {}; expected 1, 2 or 3",
                input.format_version
            ));
        }
        if input.revision == 0 {
            return Err("access policy revision must be nonzero".into());
        }
        let mut resources = BTreeMap::new();
        for resource in input.resources {
            if resource.workspace.is_empty() {
                return Err("access policy workspace must be nonempty".into());
            }
            crate::collections::validate_name(&resource.workspace)?;
            if !resource.collection.is_empty() {
                crate::collections::validate_name(&resource.collection)?;
            }
            if resources
                .insert(resource.collection, resource.workspace)
                .is_some()
            {
                return Err("access policy repeats a collection binding".into());
            }
        }
        let mut grants = BTreeMap::new();
        let mut views = BTreeMap::new();
        let mut fields = BTreeMap::new();
        for grant in input.grants {
            if grant.principal.is_empty() {
                return Err("access grant principal must be nonempty".into());
            }
            if resources.get(&grant.collection) != Some(&grant.workspace) {
                return Err("access grant does not match a workspace/collection binding".into());
            }
            if grant.actions.is_empty() {
                return Err("access grant must name at least one action".into());
            }
            let mut actions = BTreeSet::new();
            for action in grant.actions {
                match AccessAction::try_from(action) {
                    Ok(AccessAction::Search | AccessAction::Ingest | AccessAction::Admin) => {}
                    _ => return Err(format!("unknown access action {action}")),
                }
                if !actions.insert(action) {
                    return Err("access grant repeats an action".into());
                }
            }
            if let Some(view) = grant.document_visibility {
                if input.format_version < 2 {
                    return Err("document visibility requires access policy format 2".into());
                }
                if !actions.contains(&(AccessAction::Search as i32)) {
                    return Err("document visibility requires an explicit search action".into());
                }
                crate::visibility::VisibilityScope::new(Some(&view))
                    .map_err(|error| format!("invalid document grant: {}", error.message()))?;
                views.insert((grant.principal.clone(), grant.collection.clone()), view);
            }
            if let Some(permissions) = grant.field_permissions {
                if input.format_version != 3 {
                    return Err("field permissions require access policy format 3".into());
                }
                if !actions.contains(&(AccessAction::Search as i32)) {
                    return Err("field permissions require an explicit search action".into());
                }
                crate::field_permissions::FieldScope::new(&permissions)?;
                fields.insert(
                    (grant.principal.clone(), grant.collection.clone()),
                    permissions,
                );
            }
            if grants
                .insert((grant.principal, grant.collection), actions)
                .is_some()
            {
                return Err("access policy repeats a principal/collection grant".into());
            }
        }
        Ok(Self {
            revision: input.revision,
            resources,
            grants,
            views,
            fields,
        })
    }
}

// Reuse the same policy semantics in deterministic control-state application.
// The caller supplies committed state; this never reads a live policy or clock.
#[cfg(feature = "net")]
pub(crate) fn validate_policy_snapshot(input: &AccessPolicy) -> Result<(), String> {
    Policy::validate(input.clone()).map(|_| ())
}

#[cfg(feature = "net")]
pub(crate) fn authorize_policy_snapshot(
    input: &AccessPolicy,
    principal: &str,
    collection: &str,
    action: AccessAction,
) -> Result<AccessDecision, Status> {
    Policy::validate(input.clone())
        .map_err(|error| Status::data_loss(format!("committed control policy: {error}")))?
        .authorize(principal, collection, action)
}

impl Policy {
    fn authorize(
        &self,
        principal: &str,
        collection: &str,
        action: AccessAction,
    ) -> Result<AccessDecision, Status> {
        let policy = self;
        let allowed = policy
            .grants
            .get(&(principal.to_owned(), collection.to_owned()))
            .is_some_and(|actions| actions.contains(&(action as i32)));
        if !allowed {
            return Err(Status::permission_denied(
                "operation is not authorized for this collection",
            ));
        }
        Ok(AccessDecision {
            policy_revision: policy.revision,
            principal: principal.into(),
            collection: collection.into(),
            workspace: policy.resources[collection].clone(),
            action: action as i32,
            field_permissions: if action == AccessAction::Search {
                policy
                    .fields
                    .get(&(principal.to_owned(), collection.to_owned()))
                    .cloned()
            } else {
                None
            },
            document_visibility: if action == AccessAction::Search {
                policy
                    .views
                    .get(&(principal.to_owned(), collection.to_owned()))
                    .cloned()
            } else {
                None
            },
        })
    }
}

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
        // a later replacement must not skip a previously published old epoch.
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

/// A decision held across asynchronous work. A policy change invalidates the
/// operation even if the new policy would also allow it; callers must retry.
#[derive(Clone, Debug)]
pub struct AccessPermit {
    authority: Arc<dyn Authorizer>,
    decision: AccessDecision,
    revisions: watch::Receiver<u64>,
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

/// Rejects a revoked stream even while its producer is pending, then drops the
/// producer. A policy revision is checked before each disclosed item.
pub struct AuthorizedStream<S> {
    inner: Option<S>,
    permits: Vec<AccessPermit>,
    revisions: Vec<WatchStream<u64>>,
}
impl<S> AuthorizedStream<S> {
    pub fn new(inner: S, permit: Option<AccessPermit>) -> Self {
        Self::with_permits(inner, permit.into_iter().collect())
    }

    /// A stream whose disclosure requires every resource decision to remain
    /// valid. Subscribe to each authority so revocation wakes an idle producer.
    pub fn with_permits(inner: S, permits: Vec<AccessPermit>) -> Self {
        let revisions = permits
            .iter()
            .map(|p| WatchStream::new(p.revisions.clone()))
            .collect();
        Self {
            inner: Some(inner),
            permits,
            revisions,
        }
    }
}
impl<S, T> Stream for AuthorizedStream<S>
where
    S: Stream<Item = Result<T, Status>> + Unpin,
{
    type Item = Result<T, Status>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.inner.is_none() {
            return Poll::Ready(None);
        }
        for revisions in &mut self.revisions {
            while let Poll::Ready(Some(_)) = Pin::new(&mut *revisions).poll_next(cx) {}
        }
        for permit in &self.permits {
            if let Err(error) = permit.check() {
                self.inner = None;
                return Poll::Ready(Some(Err(error)));
            }
        }
        let result = Pin::new(self.inner.as_mut().expect("checked above")).poll_next(cx);
        // A producer may yield after doing work that spans a policy replacement.
        if matches!(result, Poll::Ready(Some(_))) {
            for permit in &self.permits {
                if let Err(error) = permit.check() {
                    self.inner = None;
                    return Poll::Ready(Some(Err(error)));
                }
            }
        }
        if matches!(result, Poll::Ready(None)) {
            self.inner = None;
        }
        result
    }
}

#[cfg(test)]
mod pin_tests {
    use super::*;
    use crate::pb::{CollectionGrant, CollectionResource};
    use std::sync::{mpsc, TryLockError};
    use std::time::Duration;

    fn policy() -> AccessPolicy {
        AccessPolicy {
            format_version: 1,
            revision: 1,
            resources: vec![CollectionResource {
                workspace: "workspace".into(),
                collection: "books".into(),
            }],
            grants: vec![CollectionGrant {
                principal: "reader".into(),
                workspace: "workspace".into(),
                collection: "books".into(),
                actions: vec![AccessAction::Search as i32],
                ..Default::default()
            }],
        }
    }

    fn policy_revision(revision: u64) -> AccessPolicy {
        let mut policy = policy();
        policy.revision = revision;
        policy
    }

    #[test]
    fn admitted_operation_delays_replacement_completion_until_drop() {
        let authority = Arc::new(PolicyAuthority::new(policy()).unwrap());
        let permit =
            AccessPermit::acquire(authority.clone(), "reader", "books", AccessAction::Search)
                .unwrap();
        let pin = permit.pin().unwrap();
        let (result_tx, result_rx) = mpsc::channel();
        let replacing = {
            let authority = authority.clone();
            std::thread::spawn(move || {
                result_tx
                    .send(authority.replace(policy_revision(2)))
                    .unwrap();
            })
        };
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while *authority.revisions.borrow() != 2 && std::time::Instant::now() < deadline {
            std::thread::yield_now();
        }
        let published = *authority.revisions.borrow() == 2;
        let serializer_held_through_drain = matches!(
            authority.replacements.try_lock(),
            Err(TryLockError::WouldBlock)
        );
        let before_drop = result_rx.try_recv();
        let pending = matches!(&before_drop, Err(mpsc::TryRecvError::Empty));
        drop(pin);
        let replacement = match before_drop {
            Ok(result) => Ok(result),
            Err(mpsc::TryRecvError::Empty) => result_rx.recv_timeout(Duration::from_secs(5)),
            Err(mpsc::TryRecvError::Disconnected) => Err(mpsc::RecvTimeoutError::Disconnected),
        };
        if replacement.is_ok() {
            replacing.join().unwrap();
        }
        assert!(
            published,
            "replacement did not publish within the observation bound"
        );
        assert!(
            serializer_held_through_drain,
            "replacement serializer was released before the admitted epoch drained"
        );
        assert!(
            pending,
            "replacement completed while an operation remained admitted"
        );
        replacement.unwrap().unwrap();
    }

    #[test]
    fn admission_counter_exhaustion_refuses_without_wrapping() {
        let epoch = Arc::new(PolicyEpoch::new(Policy::validate(policy()).unwrap()));
        let decision = epoch
            .policy
            .authorize("reader", "books", AccessAction::Search)
            .unwrap();
        epoch.active.store(usize::MAX, Ordering::Release);
        let error = epoch
            .admit(decision)
            .err()
            .expect("counter must refuse overflow");
        assert_eq!(error.code(), tonic::Code::ResourceExhausted);
        assert!(error.message().contains("admission count"));
        assert_eq!(epoch.active.load(Ordering::Acquire), usize::MAX);
    }

    #[test]
    fn drain_eventually_completes_after_last_guard_drop() {
        let epoch = Arc::new(PolicyEpoch::new(Policy::validate(policy()).unwrap()));
        let decision = epoch
            .policy
            .authorize("reader", "books", AccessAction::Search)
            .unwrap();
        let guard = epoch.admit(decision).unwrap();
        let (started_tx, started_rx) = mpsc::channel();
        let (drained_tx, drained_rx) = mpsc::channel();
        let waiter = {
            let epoch = epoch.clone();
            std::thread::spawn(move || {
                started_tx.send(()).unwrap();
                epoch.wait_drained();
                drained_tx.send(()).unwrap();
            })
        };
        started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(matches!(
            drained_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        drop(guard);
        drained_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("last guard drop did not wake the drain");
        waiter.join().unwrap();
        assert_eq!(epoch.active.load(Ordering::Acquire), 0);
    }

    #[test]
    fn post_publication_replacement_poison_fails_later_replacements_closed() {
        let authority = Arc::new(PolicyAuthority::new(policy()).unwrap());
        let permit =
            AccessPermit::acquire(authority.clone(), "reader", "books", AccessAction::Search)
                .unwrap();
        let pin = permit.pin().unwrap();
        let publishing = {
            let authority = authority.clone();
            std::thread::spawn(move || {
                let _replacement = authority.replacements.lock().unwrap();
                let next = Arc::new(PolicyEpoch::new(
                    Policy::validate(policy_revision(2)).unwrap(),
                ));
                let mut current = authority.policy.write().unwrap();
                let _undrained = std::mem::replace(&mut *current, next);
                authority.revisions.send_replace(2);
                drop(current);
                panic!("inject failure after publication before drain");
            })
        };
        assert!(publishing.join().is_err());
        assert_eq!(*authority.revisions.borrow(), 2);
        let error = authority
            .replace(policy_revision(3))
            .expect_err("a poisoned replacement serializer must fail closed");
        assert!(error.contains("replacement lock poisoned"), "{error}");
        drop(pin);
    }

    #[test]
    fn notification_poison_recovers_without_skipping_the_admission_count() {
        let epoch = Arc::new(PolicyEpoch::new(Policy::validate(policy()).unwrap()));
        let decision = epoch
            .policy
            .authorize("reader", "books", AccessAction::Search)
            .unwrap();
        let guard = epoch.admit(decision).unwrap();
        let poisoning = {
            let epoch = epoch.clone();
            std::thread::spawn(move || {
                let _notification = epoch.notification.lock().unwrap();
                panic!("inject notification-only mutex poison");
            })
        };
        assert!(poisoning.join().is_err());
        assert_eq!(epoch.active.load(Ordering::Acquire), 1);

        let (started_tx, started_rx) = mpsc::channel();
        let (drained_tx, drained_rx) = mpsc::channel();
        let waiter = {
            let epoch = epoch.clone();
            std::thread::spawn(move || {
                started_tx.send(()).unwrap();
                epoch.wait_drained();
                drained_tx.send(()).unwrap();
            })
        };
        started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(matches!(
            drained_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));

        drop(guard);
        drained_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("notification poison prevented the real zero-count drain");
        waiter.join().unwrap();
        assert_eq!(epoch.active.load(Ordering::Acquire), 0);
    }
}

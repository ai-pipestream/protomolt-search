//! Revisioned workspace/collection capabilities at the public service boundary.

use crate::pb::{AccessAction, AccessDecision, AccessPolicy};
use std::collections::{BTreeMap, BTreeSet};
use std::pin::Pin;
use std::sync::{Arc, RwLock};
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
}

#[derive(Debug)]
struct Policy {
    revision: u64,
    resources: BTreeMap<String, String>,
    grants: BTreeMap<(String, String), BTreeSet<i32>>,
    views: BTreeMap<(String, String), crate::pb::DocumentVisibility>,
}

impl Policy {
    fn validate(input: AccessPolicy) -> Result<Self, String> {
        if !matches!(input.format_version, 1 | 2) {
            return Err(format!(
                "unsupported access policy format_version {}; expected 1 or 2",
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
                if input.format_version != 2 {
                    return Err("document visibility requires access policy format 2".into());
                }
                if !actions.contains(&(AccessAction::Search as i32)) {
                    return Err("document visibility requires an explicit search action".into());
                }
                crate::visibility::VisibilityScope::new(Some(&view))
                    .map_err(|error| format!("invalid document grant: {}", error.message()))?;
                views.insert((grant.principal.clone(), grant.collection.clone()), view);
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
        })
    }
}

/// In-process snapshot authority. Loading and validating a replacement happens
/// before publication; readers never observe a partially replaced policy.
#[derive(Debug)]
pub struct PolicyAuthority {
    policy: RwLock<Policy>,
    revisions: watch::Sender<u64>,
}
impl PolicyAuthority {
    pub fn new(policy: AccessPolicy) -> Result<Self, String> {
        let policy = Policy::validate(policy)?;
        let (revisions, _) = watch::channel(policy.revision);
        Ok(Self {
            policy: RwLock::new(policy),
            revisions,
        })
    }
    pub fn replace(&self, policy: AccessPolicy) -> Result<(), String> {
        let next = Policy::validate(policy)?;
        let mut current = self
            .policy
            .write()
            .map_err(|_| "access policy lock poisoned")?;
        if next.revision <= current.revision {
            return Err("access policy revision must increase".into());
        }
        *current = next;
        self.revisions.send_replace(current.revision);
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
        let policy = self
            .policy
            .read()
            .map_err(|_| Status::internal("access policy lock poisoned"))?;
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
    pub fn check(&self) -> Result<(), Status> {
        let current = self.authority.authorize(
            &self.decision.principal,
            &self.decision.collection,
            AccessAction::try_from(self.decision.action)
                .map_err(|_| Status::permission_denied("invalid authorization action"))?,
        )?;
        if current != self.decision || *self.revisions.borrow() != self.decision.policy_revision {
            return Err(Status::permission_denied(
                "access policy changed; start a new operation",
            ));
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

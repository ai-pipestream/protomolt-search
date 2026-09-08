use std::sync::{mpsc, Arc};

use pipestream_search::authorization::{
    AccessPermit, AuthorizationGuard, Authorizer, PolicyAuthority,
};
use pipestream_search::pb::{
    AccessAction, AccessDecision, AccessPolicy, CollectionGrant, CollectionResource,
};
use tokio::sync::watch;
use tonic::{Code, Status};

fn policy(revision: u64, allowed: bool) -> AccessPolicy {
    AccessPolicy {
        format_version: 1,
        revision,
        resources: vec![CollectionResource {
            workspace: "workspace".into(),
            collection: "books".into(),
        }],
        grants: allowed
            .then(|| CollectionGrant {
                principal: "reader".into(),
                workspace: "workspace".into(),
                collection: "books".into(),
                actions: vec![AccessAction::Search as i32],
                ..Default::default()
            })
            .into_iter()
            .collect(),
    }
}

fn permit(authority: Arc<dyn Authorizer>) -> AccessPermit {
    AccessPermit::acquire(authority, "reader", "books", AccessAction::Search).unwrap()
}

#[test]
fn successful_pin_matches_the_admitted_decision() {
    let authority = Arc::new(PolicyAuthority::new(policy(1, true)).unwrap());
    let permit = permit(authority);
    let pinned = permit.pin().unwrap();
    assert_eq!(pinned.decision(), permit.decision());
}

#[test]
fn stale_and_revoked_permits_cannot_acquire_a_new_pin() {
    let authority = Arc::new(PolicyAuthority::new(policy(1, true)).unwrap());
    let stale = permit(authority.clone());
    authority.replace(policy(2, true)).unwrap();
    assert!(stale.pin().is_err());

    let revoked = permit(authority.clone());
    authority.replace(policy(3, false)).unwrap();
    assert!(revoked.pin().is_err());
}

#[test]
fn policy_replacement_waits_for_the_synchronous_pin_to_drop() {
    let authority = Arc::new(PolicyAuthority::new(policy(1, true)).unwrap());
    let permit = permit(authority.clone());
    let pinned = permit.pin().unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (finished_tx, finished_rx) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        authority.replace(policy(2, true)).unwrap();
        finished_tx.send(()).unwrap();
    });
    started_rx.recv().unwrap();
    assert_eq!(finished_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
    drop(pinned);
    finished_rx.recv().unwrap();
    worker.join().unwrap();
}

#[derive(Debug)]
struct DecisionGuard(AccessDecision);
impl AuthorizationGuard for DecisionGuard {
    fn decision(&self) -> &AccessDecision {
        &self.0
    }
}

#[derive(Debug)]
struct WrongPinnedAuthorizer {
    decision: AccessDecision,
    revisions: watch::Sender<u64>,
}
impl WrongPinnedAuthorizer {
    fn new() -> Self {
        let decision = AccessDecision {
            policy_revision: 1,
            principal: "reader".into(),
            collection: "books".into(),
            workspace: "workspace".into(),
            action: AccessAction::Search as i32,
            ..Default::default()
        };
        let (revisions, _) = watch::channel(1);
        Self {
            decision,
            revisions,
        }
    }
}
impl Authorizer for WrongPinnedAuthorizer {
    fn authorize(&self, _: &str, _: &str, _: AccessAction) -> Result<AccessDecision, Status> {
        Ok(self.decision.clone())
    }
    fn subscribe(&self) -> watch::Receiver<u64> {
        self.revisions.subscribe()
    }
    fn pin(&self, _: &AccessDecision) -> Result<Box<dyn AuthorizationGuard + '_>, Status> {
        let mut wrong = self.decision.clone();
        wrong.policy_revision += 1;
        Ok(Box::new(DecisionGuard(wrong)))
    }
}

#[test]
fn provider_returning_the_wrong_pinned_decision_is_refused() {
    let permit = permit(Arc::new(WrongPinnedAuthorizer::new()));
    assert_eq!(permit.pin().err().unwrap().code(), Code::PermissionDenied);
}

#[derive(Debug)]
struct LegacyAuthorizer {
    decision: AccessDecision,
    revisions: watch::Sender<u64>,
}
impl LegacyAuthorizer {
    fn new() -> Self {
        let provider = WrongPinnedAuthorizer::new();
        Self {
            decision: provider.decision,
            revisions: provider.revisions,
        }
    }
}
impl Authorizer for LegacyAuthorizer {
    fn authorize(&self, _: &str, _: &str, _: AccessAction) -> Result<AccessDecision, Status> {
        Ok(self.decision.clone())
    }
    fn subscribe(&self) -> watch::Receiver<u64> {
        self.revisions.subscribe()
    }
}

#[test]
fn legacy_authorizer_without_pin_support_is_explicitly_unimplemented() {
    let permit = permit(Arc::new(LegacyAuthorizer::new()));
    assert_eq!(permit.pin().err().unwrap().code(), Code::Unimplemented);
}

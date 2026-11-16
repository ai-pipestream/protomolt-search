use super::*;
use crate::authorization::{AccessPermit, POLICY_FORMAT};
use crate::pb::{CollectionGrant, CollectionResource};
use std::sync::{mpsc, TryLockError};
use std::time::Duration;

fn policy() -> AccessPolicy {
    AccessPolicy {
        format_version: POLICY_FORMAT,
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
        AccessPermit::acquire(authority.clone(), "reader", "books", AccessAction::Search).unwrap();
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
        AccessPermit::acquire(authority.clone(), "reader", "books", AccessAction::Search).unwrap();
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

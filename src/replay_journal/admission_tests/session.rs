//! Live authority sessions: freshness, expiry, and supersession.

use super::*;

#[test]
fn persisted_allow_decision_is_not_live_authority_after_reopen() {
    let dir = Directory::new();
    let journal = ReplayJournal::create(&dir.path(), binding()).unwrap();
    journal.publish_authorization(&policy(1)).unwrap();
    drop(journal);
    let journal = ReplayJournal::open(&dir.path(), binding()).unwrap();
    assert_eq!(
        journal
            .accept_inner(&frame(), &[10; 32])
            .unwrap_err()
            .code(),
        tonic::Code::Unavailable
    );
    assert_eq!(journal.head().unwrap().accepted_sequence, 0);
}

#[test]
fn session_drop_disconnect_and_late_refreshes_cannot_restore_permission() {
    let dir = Directory::new();
    let journal = ReplayJournal::create(&dir.path(), binding()).unwrap();
    let old = grant_live(&journal, &policy(1));
    let late = old.begin_refresh(MAX_AUTHORITY_VALIDITY).unwrap();
    journal.suspend_authorization().unwrap();
    assert_eq!(
        journal
            .accept_inner(&frame(), &[10; 32])
            .unwrap_err()
            .code(),
        tonic::Code::Unavailable
    );
    assert_eq!(
        late.publish(&policy(100)).unwrap_err().code(),
        tonic::Code::Unavailable
    );
    assert!(old.begin_refresh(MAX_AUTHORITY_VALIDITY).is_err());
    let current = grant_live(&journal, &policy(2));
    drop(old); // A superseded subscription must not close its replacement.
    journal.accept_inner(&frame(), &[10; 32]).unwrap();
    drop(current);
    assert_eq!(
        journal
            .accept_inner(&frame(), &[10; 32])
            .unwrap_err()
            .code(),
        tonic::Code::Unavailable
    );
    let _empty_session = journal.authority_session().unwrap();
    assert_eq!(
        journal
            .accept_inner(&frame(), &[10; 32])
            .unwrap_err()
            .code(),
        tonic::Code::Unavailable
    );
}

#[test]
fn delayed_refresh_uses_request_start_and_expiry_fences_exact_retries() {
    use std::{
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc,
        },
        time::{Duration, Instant},
    };
    let dir = Directory::new();
    let mut journal = ReplayJournal::create(&dir.path(), binding()).unwrap();
    let elapsed = Arc::new(AtomicU64::new(0));
    let clock = elapsed.clone();
    let base = Instant::now();
    journal.authority_clock =
        Arc::new(move || base + Duration::from_secs(clock.load(Ordering::SeqCst)));
    let session = journal.authority_session().unwrap();
    let refresh = session.begin_refresh(Duration::from_secs(10)).unwrap();
    elapsed.store(8, Ordering::SeqCst);
    refresh.publish(&policy(1)).unwrap();
    journal.accept_inner(&frame(), &[10; 32]).unwrap();
    elapsed.store(10, Ordering::SeqCst);
    assert_eq!(
        journal
            .accept_inner(&frame(), &[10; 32])
            .unwrap_err()
            .code(),
        tonic::Code::Unavailable
    );
    let refresh = session.begin_refresh(Duration::from_secs(3)).unwrap();
    elapsed.store(14, Ordering::SeqCst);
    assert_eq!(
        refresh.publish(&policy(2)).unwrap_err().code(),
        tonic::Code::Unavailable
    );
    assert_eq!(
        journal
            .accept_inner(&frame(), &[10; 32])
            .unwrap_err()
            .code(),
        tonic::Code::Unavailable
    );
    assert_eq!(journal.head().unwrap().accepted_sequence, 1);
}

#[test]
fn superseded_reply_cannot_replace_a_newer_proof_even_with_a_higher_revision() {
    let dir = Directory::new();
    let journal = ReplayJournal::create(&dir.path(), binding()).unwrap();
    let session = journal.authority_session().unwrap();
    let earlier = session.begin_refresh(MAX_AUTHORITY_VALIDITY).unwrap();
    let later = session.begin_refresh(MAX_AUTHORITY_VALIDITY).unwrap();
    later.publish(&policy(1)).unwrap();
    assert_eq!(
        earlier.publish(&policy(100)).unwrap_err().code(),
        tonic::Code::Unavailable
    );
    journal.accept_inner(&frame(), &[10; 32]).unwrap();
    session
        .begin_refresh(MAX_AUTHORITY_VALIDITY)
        .unwrap()
        .publish(&policy(2))
        .unwrap();
}

#[test]
fn offline_policy_installation_and_failed_live_replacement_close_permission() {
    let dir = Directory::new();
    let journal = ReplayJournal::create(&dir.path(), binding()).unwrap();
    let session = grant_live(&journal, &policy(1));
    let mut invalid = policy(2);
    invalid.contract_version = 99;
    assert_eq!(
        session
            .begin_refresh(MAX_AUTHORITY_VALIDITY)
            .unwrap()
            .publish(&invalid)
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
    assert_eq!(
        journal
            .accept_inner(&frame(), &[10; 32])
            .unwrap_err()
            .code(),
        tonic::Code::Unavailable
    );
    session
        .begin_refresh(MAX_AUTHORITY_VALIDITY)
        .unwrap()
        .publish(&policy(1))
        .unwrap();
    journal.publish_authorization(&policy(1)).unwrap();
    assert_eq!(
        journal
            .accept_inner(&frame(), &[10; 32])
            .unwrap_err()
            .code(),
        tonic::Code::Unavailable
    );
    assert!(session.begin_refresh(MAX_AUTHORITY_VALIDITY).is_err());
}

#[test]
fn refresh_validity_is_bounded_and_checked_after_waiting_for_admission() {
    use std::{
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc, Barrier,
        },
        time::{Duration, Instant},
    };
    let dir = Directory::new();
    let mut journal = ReplayJournal::create(&dir.path(), binding()).unwrap();
    let elapsed = Arc::new(AtomicU64::new(0));
    let clock = elapsed.clone();
    let base = Instant::now();
    journal.authority_clock =
        Arc::new(move || base + Duration::from_secs(clock.load(Ordering::SeqCst)));
    let journal = Arc::new(journal);
    let session = grant_live(&journal, &policy(1));
    assert!(session.begin_refresh(Duration::ZERO).is_err());
    assert!(session.begin_refresh(Duration::from_secs(31)).is_err());
    let gate = journal.lock_authority().unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let pending = {
        let journal = journal.clone();
        let barrier = barrier.clone();
        std::thread::spawn(move || {
            barrier.wait();
            journal.accept_inner(&frame(), &[10; 32])
        })
    };
    barrier.wait();
    elapsed.store(31, Ordering::SeqCst);
    drop(gate);
    assert_eq!(
        pending.join().unwrap().unwrap_err().code(),
        tonic::Code::Unavailable
    );
    assert_eq!(journal.head().unwrap().accepted_sequence, 0);
}

#[test]
fn a_journal_at_another_format_version_is_refused() {
    let dir = Directory::new();
    let journal = ReplayJournal::create(&dir.path(), binding()).unwrap();
    journal.publish_authorization(&policy(1)).unwrap();
    drop(journal);
    let db = Database::open(dir.path()).unwrap();
    let tx = db.begin_write().unwrap();
    {
        let mut meta = tx.open_table(META).unwrap();
        let mut header: ReplayJournalHeader =
            decode(meta.get("header").unwrap().unwrap().value()).unwrap();
        header.format_version = 2;
        meta.insert("header", header.encode_to_vec().as_slice())
            .unwrap();
    }
    tx.commit().unwrap();
    drop(db);
    assert_eq!(
        ReplayJournal::open(&dir.path(), binding())
            .err()
            .unwrap()
            .code(),
        tonic::Code::FailedPrecondition
    );
}

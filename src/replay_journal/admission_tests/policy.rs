//! The persisted decision: required, exact on retry, and a permanent fence.

use super::*;

#[test]
fn policy_is_required_for_peers_and_cannot_be_bypassed_locally() {
    let dir = Directory::new();
    let journal = ReplayJournal::create(&dir.path(), binding()).unwrap();
    assert_eq!(
        journal
            .accept_inner(&frame(), &[10; 32])
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    journal.publish_authorization(&policy(1)).unwrap();
    assert_eq!(journal.head().unwrap().format_version, FORMAT);
    assert_eq!(
        journal
            .accept_inner(&frame(), &[11; 32])
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    assert_eq!(journal.head().unwrap().accepted_sequence, 0);
    let _session = grant_live(&journal, &policy(1));
    assert!(journal.accept_inner(&frame(), &[10; 32]).unwrap().durable);
}

#[test]
fn policy_retries_are_exact_and_rollback_or_authority_replacement_refuses() {
    let dir = Directory::new();
    let journal = ReplayJournal::create(&dir.path(), binding()).unwrap();
    journal.publish_authorization(&policy(2)).unwrap();
    journal.publish_authorization(&policy(2)).unwrap();
    let mut conflict = policy(2);
    conflict.enabled = false;
    assert_eq!(
        journal.publish_authorization(&conflict).unwrap_err().code(),
        tonic::Code::AlreadyExists
    );
    assert_eq!(
        journal
            .publish_authorization(&policy(1))
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    let mut foreign = policy(3);
    foreign.authority_id[0] ^= 1;
    assert_eq!(
        journal.publish_authorization(&foreign).unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
    foreign = policy(3);
    foreign.binding_sha256[0] ^= 1;
    assert_eq!(
        journal.publish_authorization(&foreign).unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
    let _session = grant_live(&journal, &policy(2));
    journal.accept_inner(&frame(), &[10; 32]).unwrap();
}

#[test]
fn current_fences_and_device_residency_revoke_even_exact_retries_after_restart() {
    for case in 0..9 {
        let dir = Directory::new();
        let journal = ReplayJournal::create(&dir.path(), binding()).unwrap();
        journal.publish_authorization(&policy(1)).unwrap();
        let accepted = {
            let _session = grant_live(&journal, &policy(1));
            journal.accept_inner(&frame(), &[10; 32]).unwrap()
        };
        let mut denied = policy(2);
        match case {
            0 => denied.enabled = false,
            1 => denied.source_history_id[0] ^= 1,
            2 => denied.target_history_id[0] ^= 1,
            3 => denied.source_write_epoch += 1,
            4 => denied.target_write_epoch += 1,
            5 => denied.source_residency = 2,
            6 => denied.target_residency = 2,
            7 => denied.source_certificate_sha256.clear(),
            8 => denied.source_certificate_sha256 = vec![vec![11; 32]],
            _ => unreachable!(),
        }
        journal.publish_authorization(&denied).unwrap();
        drop(journal);
        let journal = ReplayJournal::open(&dir.path(), binding()).unwrap();
        assert_eq!(
            journal
                .accept_inner(&frame(), &[10; 32])
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied,
            "case {case}"
        );
        assert_eq!(journal.head().unwrap().accepted_sequence, 1);
        if (1..=6).contains(&case) {
            assert!(journal.head().unwrap().admission_fenced);
            assert_eq!(
                journal
                    .publish_authorization(&policy(3))
                    .unwrap_err()
                    .code(),
                tonic::Code::FailedPrecondition
            );
        } else {
            let _session = grant_live(&journal, &policy(3));
            assert_eq!(
                journal.accept_inner(&frame(), &[10; 32]).unwrap(),
                ReplayJournalReceipt {
                    replayed: true,
                    ..accepted.clone()
                }
            );
        }
    }
}

#[test]
fn unrelated_policy_revision_does_not_invalidate_an_unchanged_assignment() {
    let dir = Directory::new();
    let journal = ReplayJournal::create(&dir.path(), binding()).unwrap();
    let session = grant_live(&journal, &policy(1));
    session
        .begin_refresh(MAX_AUTHORITY_VALIDITY)
        .unwrap()
        .publish(&policy(100))
        .unwrap();
    let receipt = journal.accept_inner(&frame(), &[10; 32]).unwrap();
    assert!(receipt.accepted && !receipt.searchable);
    assert_eq!(
        journal.head().unwrap().binding.unwrap().topology_revision,
        5
    );
}

#[test]
fn invalid_publication_cannot_replace_an_existing_revocation() {
    let dir = Directory::new();
    let journal = ReplayJournal::create(&dir.path(), binding()).unwrap();
    let mut denied = policy(1);
    denied.enabled = false;
    journal.publish_authorization(&denied).unwrap();
    for case in 0..7 {
        let mut invalid = policy(2);
        match case {
            0 => invalid.contract_version = 2,
            1 => invalid.source_residency = 0,
            2 => invalid.target_residency = 3,
            3 => invalid.source_write_epoch = 0,
            4 => invalid.source_history_id.clear(),
            5 => invalid.source_certificate_sha256.push(vec![10; 32]),
            6 => invalid.source_certificate_sha256[0].clear(),
            _ => unreachable!(),
        }
        assert_eq!(
            journal.publish_authorization(&invalid).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            journal
                .accept_inner(&frame(), &[10; 32])
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
    }
}

#[test]
fn a_missing_policy_a_retired_format_or_undecided_history_is_refused_on_open() {
    for case in 0..3 {
        let dir = Directory::new();
        let journal = ReplayJournal::create(&dir.path(), binding()).unwrap();
        journal.publish_authorization(&policy(1)).unwrap();
        if case == 2 {
            let _session = grant_live(&journal, &policy(1));
            journal.accept_inner(&frame(), &[10; 32]).unwrap();
        }
        drop(journal);
        let db = Database::open(dir.path()).unwrap();
        let tx = db.begin_write().unwrap();
        {
            let mut meta = tx.open_table(META).unwrap();
            if case == 0 {
                meta.remove("admission").unwrap();
            } else if case == 2 {
                meta.insert("admission", NO_DECISION).unwrap();
            } else {
                let mut header: ReplayJournalHeader =
                    decode(meta.get("header").unwrap().unwrap().value()).unwrap();
                header.format_version = 1;
                meta.insert("header", header.encode_to_vec().as_slice())
                    .unwrap();
            }
        }
        tx.commit().unwrap();
        drop(db);
        let refused = ReplayJournal::open(&dir.path(), binding()).err().unwrap();
        use tonic::Code;
        let expected = [Code::DataLoss, Code::FailedPrecondition, Code::DataLoss];
        assert_eq!(refused.code(), expected[case]);
    }
}

#[test]
fn committed_revocation_fences_all_concurrent_deliveries() {
    let dir = Directory::new();
    let journal = std::sync::Arc::new(ReplayJournal::create(&dir.path(), binding()).unwrap());
    journal.publish_authorization(&policy(1)).unwrap();
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(9));
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let journal = journal.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                journal.accept_inner(&frame(), &[10; 32])
            })
        })
        .collect();
    let mut revoked = policy(2);
    revoked.enabled = false;
    journal.publish_authorization(&revoked).unwrap();
    barrier.wait();
    for t in threads {
        assert_eq!(
            t.join().unwrap().unwrap_err().code(),
            tonic::Code::PermissionDenied
        );
    }
    assert_eq!(journal.head().unwrap().accepted_sequence, 0);
}

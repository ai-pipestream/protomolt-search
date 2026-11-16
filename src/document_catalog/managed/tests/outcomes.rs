//! Write outcomes: a write whose commit returns after its lease lapsed is
//! unconfirmed until a fresh lease settles it; the decision is accepted
//! while the actor's right is current and fenced, by name, once it is
//! gone.
use super::*;
use crate::source_authority::SourceAdmission;

#[test]
fn a_write_durable_after_its_lease_lapsed_is_unconfirmed_until_settled() {
    let (fixture, active) = activated("lapsed-write");

    let receipt = a_lapsed_commit_is_durable_and_unconfirmed(&fixture, &active);
    only_the_actor_settles_under_a_fresh_lease(&fixture, &active, &receipt);
    a_retry_settles_an_unconfirmed_record(&fixture, &active);

    // The history carries the epoch on every version and no fence.
    let versions = history(&active);
    assert_eq!(versions.len(), 3);
    assert_eq!(
        versions[0].write_epoch, 0,
        "the fixture write predates activation"
    );
    assert!(versions[1..]
        .iter()
        .all(|v| v.write_epoch == 1 && !v.fenced));
    active_records(&active, 3);
}

/// The commit returns after the lease lapsed: durable, unconfirmed. The
/// lapsed admission decides nothing, by name, and the record stays.
fn a_lapsed_commit_is_durable_and_unconfirmed(
    fixture: &Fixture,
    active: &ActiveManagedCatalog,
) -> crate::pb::DocumentWriteReceipt {
    let admission = fixture
        .authority
        .leased_admission("alice", short_lease(40))
        .unwrap();
    active.arm_postcommit_pause(std::time::Duration::from_millis(80));
    let receipt = match active.accept(&admission, &second_write()).unwrap() {
        Acceptance::Unconfirmed(receipt) => receipt,
        Acceptance::Accepted(receipt) => panic!("accepted past its lease: {receipt:?}"),
    };
    assert_eq!(receipt.accepted_sequence, 2);
    assert_eq!(receipt.write_epoch, 1);
    assert!(receipt.accepted && receipt.durable && !receipt.replayed);
    assert_eq!(
        recorded(active, b"accept-two"),
        (WriteOutcome::Unconfirmed, 0)
    );

    // The lapsed admission decides nothing, by name, and the record stays.
    let error = active
        .settle(&admission, "alice", b"accept-two")
        .unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition, "{error}");
    assert_eq!(
        crate::raft::reasons::reason_of(&error),
        Some(crate::raft::reasons::LEASE_INTERVAL_ELAPSED),
        "{error}"
    );
    assert_eq!(
        recorded(active, b"accept-two"),
        (WriteOutcome::Unconfirmed, 0)
    );
    drop(admission);
    receipt
}

/// Another actor cannot settle the write; a fresh lease of its actor
/// does, and the retry replays the settled decision.
fn only_the_actor_settles_under_a_fresh_lease(
    fixture: &Fixture,
    active: &ActiveManagedCatalog,
    receipt: &crate::pb::DocumentWriteReceipt,
) {
    let bob = fixture
        .authority
        .leased_admission("bob", short_lease(500))
        .unwrap();
    let error = active.settle(&bob, "alice", b"accept-two").unwrap_err();
    assert_eq!(error.code(), Code::PermissionDenied, "{error}");
    drop(bob);

    // A fresh lease settles it: the right is current, the write is accepted
    // and the record moves with it.
    let fresh = fixture
        .authority
        .leased_admission("alice", short_lease(500))
        .unwrap();
    match active.settle(&fresh, "alice", b"accept-two").unwrap() {
        Settlement::Accepted(settled) => assert_eq!(settled, *receipt),
        Settlement::Fenced(rejection) => panic!("fenced with a current right: {rejection}"),
    }
    assert_eq!(recorded(active, b"accept-two"), (WriteOutcome::Accepted, 0));
    // Settling again is the same answer, and the retry replays it.
    match active.settle(&fresh, "alice", b"accept-two").unwrap() {
        Settlement::Accepted(settled) => assert_eq!(settled, *receipt),
        Settlement::Fenced(rejection) => panic!("{rejection}"),
    }
    assert_eq!(
        active.accept(&fresh, &second_write()).unwrap(),
        Acceptance::Accepted(replayed(receipt))
    );
    drop(fresh);
}

/// A retry that finds the record unconfirmed settles it under its own
/// admission, which is fresh at entry.
fn a_retry_settles_an_unconfirmed_record(fixture: &Fixture, active: &ActiveManagedCatalog) {
    let third = third_write();
    let lapsed = fixture
        .authority
        .leased_admission("alice", short_lease(40))
        .unwrap();
    active.arm_postcommit_pause(std::time::Duration::from_millis(80));
    let third_receipt = active.accept(&lapsed, &third).unwrap().into_receipt();
    assert_eq!(
        recorded(active, b"accept-three"),
        (WriteOutcome::Unconfirmed, 0)
    );
    drop(lapsed);
    let retry = fixture
        .authority
        .leased_admission("alice", short_lease(500))
        .unwrap();
    assert_eq!(
        active.accept(&retry, &third).unwrap(),
        Acceptance::Accepted(replayed(&third_receipt))
    );
    assert_eq!(
        recorded(active, b"accept-three"),
        (WriteOutcome::Accepted, 0)
    );
}

#[test]
fn a_write_settled_after_its_right_was_revoked_is_fenced_by_name() {
    let (fixture, active) = activated("fenced-write");
    let identity = authority_identity(7);

    let admission = fixture
        .authority
        .leased_admission("alice", short_lease(40))
        .unwrap();
    active.arm_postcommit_pause(std::time::Duration::from_millis(80));
    let receipt = match active.accept(&admission, &second_write()).unwrap() {
        Acceptance::Unconfirmed(receipt) => receipt,
        Acceptance::Accepted(receipt) => panic!("accepted past its lease: {receipt:?}"),
    };
    drop(admission);

    // alice loses Ingest before the write is settled: the command expects
    // control revision 4 and policy revision 1, and its commit is control
    // revision 5, the one the fence is marked with.
    assert_eq!(
        fixture
            .authority
            .execute(
                "alice",
                &grants_command(
                    &identity,
                    "admin-only",
                    4,
                    1,
                    vec![grant("alice", AccessAction::Admin)]
                )
            )
            .unwrap()
            .code,
        0
    );

    // Settling finds the right gone: the record is fenced at the revision
    // that took it, and the rejection names the durable version.
    let fresh = fixture
        .authority
        .leased_admission("alice", short_lease(500))
        .unwrap();
    let rejection = settling_fences_at_the_revoking_revision(&active, &fresh);
    a_fence_is_final(&active, &fresh, &rejection);
    drop(fresh);

    // The row stays in the history, marked, at the sequence it took; it
    // remains the head of its key, so the next version of that key counts
    // on from it.
    let versions = history(&active);
    assert_eq!(versions.len(), 2);
    assert_eq!(versions[1].accepted_sequence, receipt.accepted_sequence);
    assert!(versions[1].fenced);
    assert_eq!(versions[1].fenced_at_revision, 5);
    assert_eq!(versions[1].write_epoch, 1);
    assert!(!versions[0].fenced);

    the_next_version_counts_on_from_the_fence(&fixture, &active);
}

/// Settling finds the right gone: the record is fenced at the revision
/// that took it, and the rejection names the durable version.
fn settling_fences_at_the_revoking_revision(
    active: &ActiveManagedCatalog,
    fresh: &SourceAdmission<'_>,
) -> tonic::Status {
    let rejection = match active.settle(fresh, "alice", b"accept-two").unwrap() {
        Settlement::Fenced(rejection) => rejection,
        Settlement::Accepted(receipt) => panic!("accepted with the right gone: {receipt:?}"),
    };
    assert_eq!(rejection.code(), Code::FailedPrecondition, "{rejection}");
    assert!(
        rejection
            .message()
            .contains("version 1 at sequence 2 became durable after its admission lapsed"),
        "{rejection}"
    );
    assert!(
        rejection.message().contains("control revision 5"),
        "{rejection}"
    );
    assert!(
        rejection.message().contains("fenced under write epoch 1"),
        "{rejection}"
    );
    assert_eq!(recorded(active, b"accept-two"), (WriteOutcome::Fenced, 5));
    rejection
}

/// A fence is final: the retry replays the rejection, before any entry
/// check, and a settlement returns it again.
fn a_fence_is_final(
    active: &ActiveManagedCatalog,
    fresh: &SourceAdmission<'_>,
    rejection: &tonic::Status,
) {
    let retry = active.accept(fresh, &second_write()).unwrap_err();
    assert_eq!(retry.code(), Code::FailedPrecondition, "{retry}");
    assert_eq!(retry.message(), rejection.message());
    assert_eq!(
        crate::raft::reasons::reason_of(&retry),
        Some(crate::raft::reasons::OUTCOME_FENCED),
        "{retry}"
    );
    match active.settle(fresh, "alice", b"accept-two").unwrap() {
        Settlement::Fenced(again) => assert_eq!(again.message(), rejection.message()),
        Settlement::Accepted(receipt) => panic!("{receipt:?}"),
    }
    // The same operation id with another request is still a different
    // write.
    let mut other = second_write();
    other.document_key = b"document-other".to_vec();
    let error = active.accept(fresh, &other).unwrap_err();
    assert_eq!(error.code(), Code::AlreadyExists, "{error}");
    assert_eq!(
        crate::raft::reasons::reason_of(&error),
        Some(crate::raft::reasons::OUTCOME_OPERATION_REUSED),
        "{error}"
    );
    // An operation the actor never wrote has nothing to settle.
    let error = active.settle(fresh, "alice", b"never-written").unwrap_err();
    assert_eq!(error.code(), Code::NotFound, "{error}");
    assert_eq!(
        crate::raft::reasons::reason_of(&error),
        Some(crate::raft::reasons::OUTCOME_NO_OPERATION),
        "{error}"
    );
}

/// Granted again (the command at control revision 5, policy revision 2),
/// alice's next write of the key is version 2 at sequence 3, accepted
/// within its lease.
fn the_next_version_counts_on_from_the_fence(fixture: &Fixture, active: &ActiveManagedCatalog) {
    let identity = authority_identity(7);
    assert_eq!(
        fixture
            .authority
            .execute(
                "alice",
                &grants_command(
                    &identity,
                    "granted-again",
                    5,
                    2,
                    vec![{
                        let mut alice = grant("alice", AccessAction::Admin);
                        alice.actions.push(AccessAction::Ingest as i32);
                        alice
                    }]
                )
            )
            .unwrap()
            .code,
        0
    );
    let again = fixture
        .authority
        .leased_admission("alice", short_lease(500))
        .unwrap();
    let mut next = second_write();
    next.operation_id = b"accept-two-again".to_vec();
    next.expected_version = Some(1);
    let next_receipt = active.accept(&again, &next).unwrap().into_receipt();
    assert_eq!(
        (next_receipt.version, next_receipt.accepted_sequence),
        (2, 3)
    );
    assert_eq!(
        recorded(active, b"accept-two-again"),
        (WriteOutcome::Accepted, 0)
    );
    let versions = history(active);
    assert!(versions[1].fenced && !versions[2].fenced);
}

#[test]
fn a_lease_that_lapses_inside_the_settlement_decides_nothing() {
    let (fixture, active) = activated("settle-lapse");

    let admission = fixture
        .authority
        .leased_admission("alice", short_lease(40))
        .unwrap();
    active.arm_postcommit_pause(std::time::Duration::from_millis(80));
    let receipt = match active.accept(&admission, &second_write()).unwrap() {
        Acceptance::Unconfirmed(receipt) => receipt,
        Acceptance::Accepted(receipt) => panic!("accepted past its lease: {receipt:?}"),
    };
    drop(admission);

    // The settlement's freshness check passes, then the lease lapses in the
    // gap before the entry check: the entry check fails for the lapse, not
    // for the right, and the settlement decides nothing. The record stays
    // unconfirmed; the right was never judged gone.
    let short = fixture
        .authority
        .leased_admission("alice", short_lease(60))
        .unwrap();
    active.arm_settle_pause(std::time::Duration::from_millis(100));
    let error = active.settle(&short, "alice", b"accept-two").unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition, "{error}");
    assert!(error.message().contains("lease expired"), "{error}");
    assert_eq!(
        recorded(&active, b"accept-two"),
        (WriteOutcome::Unconfirmed, 0)
    );
    drop(short);

    // A fresh lease that stays open through the settlement accepts it.
    let fresh = fixture
        .authority
        .leased_admission("alice", short_lease(500))
        .unwrap();
    match active.settle(&fresh, "alice", b"accept-two").unwrap() {
        Settlement::Accepted(settled) => assert_eq!(settled, receipt),
        Settlement::Fenced(rejection) => panic!("fenced with a current right: {rejection}"),
    }
    assert_eq!(
        recorded(&active, b"accept-two"),
        (WriteOutcome::Accepted, 0)
    );
}

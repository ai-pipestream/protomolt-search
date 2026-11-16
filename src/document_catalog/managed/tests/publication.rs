//! Managed publication over an activated source: accepted versions are
//! published, an unconfirmed one stalls every target without staging, a
//! fenced one is skipped without rows while its predecessor stays visible,
//! and the reconciler settles what a restart left unconfirmed.
use super::publication_fixtures::*;
use super::*;
use crate::pb::{DocumentPublicationState as State, DocumentPublicationTargetState as Target};

const KEY: &[u8] = b"document-one";

fn assert_status(status: &crate::pb::DocumentPublicationStatus, state: State, target: Target) {
    assert_eq!(status.state, state as i32, "{status:?}");
    assert_eq!(status.targets.len(), 1);
    assert_eq!(status.targets[0].state, target as i32, "{status:?}");
    assert_eq!(status.targets[0].index_key, INDEX);
}

#[tokio::test]
async fn accepted_versions_publish_and_an_unconfirmed_one_stalls_until_fenced() {
    let (fixture, active) = activated_writing("publish-stall", &chunked(KEY, b"one", 1, 2));
    let target = target(&fixture.dir);
    let first = step(&fixture, &active, &target).await.unwrap();
    assert_status(&first, State::AcceptedFinal, Target::Published);
    assert_eq!((first.accepted_sequence, first.version), (1, 1));
    assert_eq!(live_rows(&target).await, 2);
    assert!(
        step(&fixture, &active, &target).await.is_none(),
        "caught up"
    );

    let receipt = lapsed_write(&fixture, &active, &chunked(KEY, b"two", 2, 3));
    assert_eq!(receipt.accepted_sequence, 2);
    for _ in 0..2 {
        let stalled = step(&fixture, &active, &target).await.unwrap();
        assert_status(&stalled, State::Unresolved, Target::Pending);
        assert_eq!(stalled.targets[0].committed_sequence, 1);
    }
    assert_eq!(live_rows(&target).await, 2, "nothing was staged");
    let refused = active
        .publication_source(&fixture.authority.admission("alice").unwrap())
        .unwrap()
        .prepare_projection(&crate::pb::PrepareDocumentProjectionRequest {
            history_id: first.history_id.clone(),
            document_key: KEY.to_vec(),
            version: 2,
            max_rows: 100,
            max_bytes: 1 << 20,
            ..Default::default()
        })
        .unwrap_err();
    assert!(refused.message().contains("is unconfirmed"), "{refused}");

    fence_the_unconfirmed_write(&fixture, &active);
    let skipped = step(&fixture, &active, &target).await.unwrap();
    assert_status(&skipped, State::Fenced, Target::SkippedFenced);
    assert_eq!(skipped.fenced_at_revision, 5);
    assert_eq!(skipped.targets[0].committed_sequence, 2);
    assert_eq!(live_rows(&target).await, 2, "version 1 stays visible");
    assert!(skipped.targets[0].stats_epoch > first.targets[0].stats_epoch);
    assert!(step(&fixture, &active, &target).await.is_none());
    published_history_reads_back(&fixture, &active, &target);
}

/// alice loses Ingest at control revision 5 and the write is settled
/// fenced under a fresh lease.
fn fence_the_unconfirmed_write(fixture: &Fixture, active: &ActiveManagedCatalog) {
    revoke_ingest(fixture, 4);
    let fresh = fixture
        .authority
        .leased_admission("alice", short_lease(500))
        .unwrap();
    match active.settle(&fresh, "alice", b"two").unwrap() {
        Settlement::Fenced(_) => {}
        Settlement::Accepted(receipt) => panic!("accepted with the right gone: {receipt:?}"),
    }
}

/// The status of each sequence reads back from the journal, and the
/// skip decision names the fenced version and stages no rows.
fn published_history_reads_back(
    fixture: &Fixture,
    active: &ActiveManagedCatalog,
    target: &crate::node::ManagedTarget,
) {
    let admission = fixture.authority.admission("alice").unwrap();
    let targets = std::slice::from_ref(target);
    let status =
        |sequence| crate::node::publication_status(active, &admission, sequence, targets).unwrap();
    assert_status(&status(1).unwrap(), State::AcceptedFinal, Target::Published);
    assert_status(&status(2).unwrap(), State::Fenced, Target::SkippedFenced);
    assert!(status(3).is_none());
    let source = active.publication_source(&admission).unwrap();
    let skip = source
        .index_publication_decision(INDEX, 2)
        .unwrap()
        .unwrap();
    assert_eq!(skip.rows, 0);
    assert_eq!(skip.before_epoch + 1, skip.after_epoch);
    let fenced = skip.source.unwrap();
    assert_eq!(fenced.outcome, WriteOutcome::Fenced as i32);
    assert_eq!((fenced.version, fenced.accepted_sequence), (2, 2));
}

#[tokio::test]
async fn a_settled_unconfirmed_write_publishes_and_its_key_counts_on() {
    let (fixture, active) = activated_writing("publish-settled", &chunked(KEY, b"one", 1, 1));
    let target = target(&fixture.dir);
    step(&fixture, &active, &target).await.unwrap();
    lapsed_write(&fixture, &active, &chunked(KEY, b"two", 2, 3));
    let stalled = step(&fixture, &active, &target).await.unwrap();
    assert_status(&stalled, State::Unresolved, Target::Pending);
    let fresh = fixture
        .authority
        .leased_admission("alice", short_lease(500))
        .unwrap();
    assert!(matches!(
        active.settle(&fresh, "alice", b"two").unwrap(),
        Settlement::Accepted(_)
    ));
    drop(fresh);
    let published = step(&fixture, &active, &target).await.unwrap();
    assert_status(&published, State::AcceptedFinal, Target::Published);
    assert_eq!(published.version, 2);
    assert_eq!(live_rows(&target).await, 3, "version 2 replaced version 1");
}

#[test]
fn the_reconciler_settles_what_a_restart_left_unconfirmed() {
    let (fixture, active) = activated_writing("reconcile", &chunked(KEY, b"one", 1, 1));
    lapsed_write(&fixture, &active, &chunked(KEY, b"two", 2, 1));
    lapsed_write(&fixture, &active, &chunked(b"document-two", b"three", 1, 1));
    let binding = active.binding().clone();
    drop(active);
    let admission = fixture.authority.admission("alice").unwrap();
    let active = ActiveManagedCatalog::recover(
        &fixture.dir.catalog(),
        &fixture.authority,
        &admission,
        &binding,
    )
    .unwrap();
    let pending = active.unconfirmed_operations(&admission, 10).unwrap();
    assert_eq!(
        pending
            .iter()
            .map(|p| (
                p.principal.as_str(),
                p.operation_id.as_slice(),
                p.accepted_sequence
            ))
            .collect::<Vec<_>>(),
        [
            ("alice", b"two".as_slice(), 2),
            ("alice", b"three".as_slice(), 3)
        ]
    );
    assert_eq!(
        active.unconfirmed_operations(&admission, 1).unwrap().len(),
        1
    );
    // A lease that lapses decides nothing: both stay for the next pass.
    let lapsed = fixture
        .authority
        .leased_admission("alice", short_lease(1))
        .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(5));
    let left = active
        .reconcile_unconfirmed(&admission, 10, |p| {
            active.settle(&lapsed, &p.principal, &p.operation_id)
        })
        .unwrap();
    assert_eq!((left.accepted.len(), left.fenced.len()), (0, 0));
    assert_eq!(left.unsettled, pending);
    let fresh = fixture
        .authority
        .leased_admission("alice", short_lease(500))
        .unwrap();
    let settled = active
        .reconcile_unconfirmed(&admission, 10, |p| {
            active.settle(&fresh, &p.principal, &p.operation_id)
        })
        .unwrap();
    assert_eq!(settled.accepted, pending);
    assert!(settled.fenced.is_empty() && settled.unsettled.is_empty());
    assert!(active
        .unconfirmed_operations(&admission, 10)
        .unwrap()
        .is_empty());
    assert_eq!(recorded(&active, b"two"), (WriteOutcome::Accepted, 0));
}

#[test]
fn listing_and_reconciling_need_admin() {
    let (fixture, active) = activated_writing("reconcile-admin", &chunked(KEY, b"one", 1, 1));
    let bob = fixture.authority.admission("bob").unwrap();
    for error in [
        active.unconfirmed_operations(&bob, 10).unwrap_err(),
        active.publication_source(&bob).err().unwrap(),
        active.settled_version(&bob, 1).unwrap_err(),
    ] {
        assert_eq!(error.code(), Code::PermissionDenied, "{error}");
    }
}

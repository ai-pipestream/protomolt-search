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
    let status = |sequence| {
        crate::node::publication_status_blocking(active, &admission, sequence, targets).unwrap()
    };
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
    assert!(*active.unconfirmed_updates().borrow());
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
    assert!(!*active.unconfirmed_updates().borrow());
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

fn lapsed_write_as(
    fixture: &Fixture,
    active: &ActiveManagedCatalog,
    actor: &str,
    request: &AcceptDocumentRequest,
) -> crate::pb::DocumentWriteReceipt {
    let admission = fixture
        .authority
        .leased_admission(actor, short_lease(40))
        .unwrap();
    active.arm_postcommit_pause(std::time::Duration::from_millis(80));
    match active.accept(&admission, request).unwrap() {
        Acceptance::Unconfirmed(receipt) => receipt,
        Acceptance::Accepted(receipt) => panic!("accepted past its lease: {receipt:?}"),
    }
}

#[test]
fn unconfirmed_writes_are_limited_in_sequence_order_not_actor_order() {
    let (fixture, active) = activated_writing("reconcile-sequence", &chunked(KEY, b"one", 1, 1));
    let mut grants = access_policy(false).grants;
    grants.push(grant("bob", AccessAction::Ingest));
    let command = grants_command(&authority_identity(7), "bob-ingest", 4, 1, grants);
    assert_eq!(
        fixture.authority.execute("alice", &command).unwrap().code,
        0
    );
    let earlier = lapsed_write_as(&fixture, &active, "bob", &chunked(KEY, b"bob", 2, 1));
    assert_eq!(earlier.accepted_sequence, 2);
    lapsed_write_as(&fixture, &active, "alice", &chunked(KEY, b"three", 3, 1));
    lapsed_write_as(&fixture, &active, "alice", &chunked(KEY, b"four", 4, 1));
    let admission = fixture.authority.admission("alice").unwrap();
    let first = active.unconfirmed_operations(&admission, 1).unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(
        (first[0].principal.as_str(), first[0].accepted_sequence),
        ("bob", 2)
    );
    let all = active.unconfirmed_operations(&admission, 10).unwrap();
    assert_eq!(
        all.iter().map(|p| p.accepted_sequence).collect::<Vec<_>>(),
        [2, 3, 4]
    );
}

#[tokio::test]
async fn a_staged_accepted_intent_recovers_and_publishes_on_retry() {
    let (fixture, active) = activated_writing("staged-accepted", &chunked(KEY, b"one", 1, 2));
    let target = target(&fixture.dir);
    stage_pending_accepted(&fixture, &active, &target).await;
    let admission = fixture.authority.admission("alice").unwrap();
    let staged = crate::node::publication_status_blocking(
        &active,
        &admission,
        1,
        std::slice::from_ref(&target),
    )
    .unwrap()
    .unwrap();
    assert_eq!(staged.targets[0].state, Target::Staged as i32);
    drop(target);
    let reopened =
        crate::document_catalog::managed::tests::publication_fixtures::target(&fixture.dir);
    let published = step(&fixture, &active, &reopened).await.unwrap();
    assert_status(&published, State::AcceptedFinal, Target::Published);
    assert_eq!(live_rows(&reopened).await, 2);
}

#[tokio::test]
async fn a_staged_fenced_intent_recovers_and_skips_on_retry() {
    let (fixture, active) = activated_writing("staged-fenced", &chunked(KEY, b"one", 1, 2));
    let target = target(&fixture.dir);
    step(&fixture, &active, &target).await.unwrap();
    lapsed_write(&fixture, &active, &chunked(KEY, b"two", 2, 3));
    fence_the_unconfirmed_write(&fixture, &active);
    stage_pending_fenced(&fixture, &active, &target);
    drop(target);
    let reopened =
        crate::document_catalog::managed::tests::publication_fixtures::target(&fixture.dir);
    let skipped = step(&fixture, &active, &reopened).await.unwrap();
    assert_status(&skipped, State::Fenced, Target::SkippedFenced);
    assert_eq!(live_rows(&reopened).await, 2);
}

#[tokio::test]
async fn publication_status_tracks_two_targets_until_lockstep() {
    let (fixture, active) = activated_writing("status-lockstep", &chunked(KEY, b"one", 1, 2));
    let first = target(&fixture.dir);
    let second = target_named(&fixture.dir, b"managed-index-two\0v1", "target-index-two");
    let initial = step(&fixture, &active, &first).await.unwrap();
    let admission = fixture.authority.admission("alice").unwrap();
    let targets = vec![first.clone(), second.clone()];
    let lagging = crate::node::publication_status_blocking(&active, &admission, 1, &targets)
        .unwrap()
        .unwrap();
    assert_eq!(lagging.history_id, initial.history_id);
    assert!(!lagging.history_id.is_empty());
    assert_eq!(lagging.targets[0].state, Target::Published as i32);
    assert_eq!(lagging.targets[1].state, Target::Pending as i32);
    let next = crate::node::next_publication_blocking(&active, &admission, &targets)
        .unwrap()
        .unwrap();
    let locked = crate::node::publish_next(next, &targets).await.unwrap();
    assert_eq!(locked.history_id, initial.history_id);
    assert!(locked
        .targets
        .iter()
        .all(|t| t.state == Target::Published as i32));
    let empty = crate::node::publication_status_blocking(&active, &admission, 1, &[]).unwrap_err();
    assert_eq!(empty.code(), Code::InvalidArgument);
}

fn status_principals() -> crate::security::Principals {
    crate::security::Principals::from_configs(&["alice", "bob"].map(|name| {
        crate::security::PrincipalConfig {
            name: name.into(),
            token: format!("{name}-token-0123456789"),
            ..Default::default()
        }
    }))
    .unwrap()
    .with_authorizer(Arc::new(
        PolicyAuthority::new(access_policy(false)).unwrap(),
    ))
}

fn status_request(
    principal: &str,
) -> tonic::Request<crate::pb::GetDocumentPublicationStatusRequest> {
    let mut request = tonic::Request::new(crate::pb::GetDocumentPublicationStatusRequest {
        collection: "books".into(),
        accepted_sequence: 1,
        index_keys: vec![INDEX.to_vec()],
    });
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {principal}-token-0123456789")
            .parse()
            .unwrap(),
    );
    request
}

#[tokio::test]
async fn publication_status_rpc_allows_admin_or_ingest_and_denies_others() {
    let (fixture, active) = activated_writing("status-rpc", &chunked(KEY, b"one", 1, 1));
    let target = target(&fixture.dir);
    step(&fixture, &active, &target).await.unwrap();
    let Fixture { catalog, .. } = fixture;
    let service = crate::document_write_service::DocumentWriteServiceImpl::new(
        status_principals(),
        vec![Arc::new(catalog)],
        crate::pb::DocumentWriteServiceLimits {
            max_request_bytes: 64 * 1024,
            max_pending_bytes: 256 * 1024,
            max_in_flight: 4,
        },
    )
    .unwrap()
    .with_publication(Arc::new(active), vec![target])
    .unwrap();
    let denied = crate::pb::document_write_service_server::DocumentWriteService::get_document_publication_status(
        &service,
        status_request("bob"),
    )
    .await
    .unwrap_err();
    assert_eq!(denied.code(), Code::PermissionDenied, "{denied}");
    let status = crate::pb::document_write_service_server::DocumentWriteService::get_document_publication_status(
        &service,
        status_request("alice"),
    )
    .await
    .unwrap()
    .into_inner();
    assert_status(&status, State::AcceptedFinal, Target::Published);
    assert!(!status.history_id.is_empty());
}

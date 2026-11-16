use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hosted_store_refuses_direct_mutation_and_local_admission() {
    let dir = Directory::new("hosted-refusals");
    let group = identity(7);
    let host = RaftHost::bootstrap_single(
        &dir.0,
        &group,
        1,
        "https://node-1:19291",
        &policy(),
        &limits(),
        &config(),
    )
    .await
    .unwrap();
    bootstrap_application_is_ready(&host).await;
    let store = host.store().unwrap();
    direct_mutations_refuse_as_raft_hosted(&store, &group);
    // The general proposal path never admits a readiness confirmation.
    let confirm = SourceAuthorityCommand {
        command_id: b"confirm".to_vec(),
        action: Some(Action::ConfirmReady(
            crate::pb::storage::ConfirmSourceOwnerReady {
                workflow_id: b"install-phone".to_vec(),
                completion: None,
            },
        )),
        ..prepare(&group, "phone", "prepare", 1)
    };
    assert_eq!(
        host.propose_command("alice", &confirm)
            .await
            .err()
            .unwrap()
            .code(),
        Code::PermissionDenied
    );
    // A leased admission from the host works while it leads; past its
    // deadline it admits nothing.
    host.with_admission("alice", |admission| {
        assert!(admission.expires_at().is_some());
        admission
            .authorize("books", AccessAction::Admin)
            .map(|d| assert_eq!(d.principal, "alice"))
    })
    .await
    .unwrap();
    elapsed_and_expiring_leases_admit_nothing(&store);
    drop(store);
    host.shutdown().await.unwrap();
}

/// Every direct mutation and the local admission of a hosted store is
/// refused, naming the Raft host.
fn direct_mutations_refuse_as_raft_hosted(
    store: &crate::source_authority::SourceAuthorityStore,
    group: &SourceAuthorityIdentity,
) {
    let hosted = |result: Result<(), tonic::Status>| {
        let error = result.err().unwrap();
        assert_eq!(error.code(), Code::FailedPrecondition);
        assert!(error.message().contains("Raft-hosted"), "{error}");
    };
    hosted(
        store
            .execute("alice", &prepare(group, "phone", "prepare", 1))
            .map(|_| ()),
    );
    let key = LogicalSourceOwner {
        workspace: "workspace-a".into(),
        collection: "books".into(),
        owner_id: Vec::new(),
    };
    hosted(
        store
            .install_control(
                "alice",
                &crate::source_authority::control_tests::command(
                    group,
                    "install",
                    1,
                    crate::source_authority::control_tests::state(2),
                ),
            )
            .map(|_| ()),
    );
    hosted(
        store
            .capacity_transition(
                "alice",
                &crate::pb::storage::CapacityTransition {
                    format_version: 1,
                    authority: Some(group.clone()),
                    key: Some(key.clone()),
                    action: Some(crate::pb::storage::capacity_transition::Action::Expire(
                        crate::pb::storage::CommitCapacityExpiry {
                            now_unix_ms: 1,
                            max_age_ms: 1,
                        },
                    )),
                },
            )
            .map(|_| ()),
    );
    hosted(store.admission("alice").map(|_| ()));
}

/// An interval that elapsed before the grant is not granted at all; one
/// that elapses while held admits nothing from then on.
fn elapsed_and_expiring_leases_admit_nothing(
    store: &crate::source_authority::SourceAuthorityStore,
) {
    let elapsed = crate::source_authority::AdmissionLease {
        anchor: std::time::Instant::now() - std::time::Duration::from_millis(200),
        anchor_wall: std::time::SystemTime::now() - std::time::Duration::from_millis(200),
        ttl: std::time::Duration::from_millis(100),
    };
    let error = match store.leased_admission("alice", elapsed) {
        Ok(_) => panic!("an elapsed interval was granted"),
        Err(error) => error,
    };
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(
        error.message().contains("elapsed before the grant"),
        "{error}"
    );
    let short = crate::source_authority::AdmissionLease {
        anchor: std::time::Instant::now(),
        anchor_wall: std::time::SystemTime::now(),
        ttl: std::time::Duration::from_millis(30),
    };
    let expiring = store.leased_admission("alice", short).unwrap();
    expiring.authorize("books", AccessAction::Admin).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(40));
    let error = expiring
        .authorize("books", AccessAction::Admin)
        .err()
        .unwrap();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("lease expired"), "{error}");
    drop(expiring);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lease_taken_on_one_thread_grants_on_another_within_its_interval() {
    let dir = Directory::new("owned-lease-grant");
    let group = identity(7);
    let host = RaftHost::bootstrap_single(
        &dir.0,
        &group,
        1,
        "https://node-1:19291",
        &policy(),
        &limits(),
        &config(),
    )
    .await
    .unwrap();
    bootstrap_application_is_ready(&host).await;
    let leased = host.lease("alice").await.unwrap();
    assert_eq!(leased.principal(), "alice");
    let granted = tokio::task::spawn_blocking(move || {
        let admission = leased.admission().unwrap();
        assert!(admission.expires_at().is_some());
        admission
            .authorize("books", AccessAction::Admin)
            .map(|d| assert_eq!(d.principal, "alice"))
    })
    .await
    .unwrap();
    granted.unwrap();
    host.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lease_kept_past_its_interval_is_rejected_at_the_grant() {
    let dir = Directory::new("owned-lease-expiry");
    let group = identity(7);
    let short = HostConfig {
        admission_lease_ms: 40,
        ..config()
    };
    let host = RaftHost::bootstrap_single(
        &dir.0,
        &group,
        1,
        "https://node-1:19291",
        &policy(),
        &limits(),
        &short,
    )
    .await
    .unwrap();
    bootstrap_application_is_ready(&host).await;
    let leased = host.lease("alice").await.unwrap();
    let applied = host.applied_position().unwrap().map(|id| id.index);
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    let error = leased.admission().err().unwrap();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(
        error
            .message()
            .contains("admission interval elapsed before the grant"),
        "{error}"
    );
    drop(leased);
    assert_eq!(host.applied_position().unwrap().map(|id| id.index), applied);
    host.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_recovery_admission_opens_a_handle_and_admits_no_write() {
    let dir = Directory::new("recovery-admission");
    let group = identity(7);
    let host = RaftHost::bootstrap_single(
        &dir.0,
        &group,
        1,
        "https://node-1:19291",
        &policy(),
        &limits(),
        &config(),
    )
    .await
    .unwrap();
    let store = host.store().unwrap();
    // The applied view opens a handle: the actor and identity resolve, and
    // the guard is taken, without a leader or a lease.
    let admission = store.recovery_admission("alice").unwrap();
    assert_eq!(admission.principal(), "alice");
    assert_eq!(admission.identity(), &group);
    assert!(admission.lease().is_none());
    // Every write-side check on it is rejected by name.
    for (what, result) in [
        ("check_fresh", admission.check_fresh()),
        (
            "authorize",
            admission
                .authorize("books", AccessAction::Ingest)
                .map(|_| ()),
        ),
        (
            "admit_write",
            admission
                .admit_write(
                    &crate::pb::storage::LogicalSourceOwner {
                        workspace: "workspace-a".into(),
                        collection: "books".into(),
                        owner_id: vec![1; 16],
                    },
                    1,
                    AccessAction::Ingest,
                )
                .map(|_| ()),
        ),
    ] {
        let error = result.err().unwrap_or_else(|| panic!("{what} admitted"));
        assert_eq!(error.code(), Code::FailedPrecondition, "{what}: {error}");
        assert!(
            error
                .message()
                .contains("a recovery admission opens a managed handle and admits no write"),
            "{what}: {error}"
        );
    }
    drop(admission);
    host.shutdown().await.unwrap();
}

/// I2's floor: a lease plus the skew budget that reaches past the
/// election floor is refused at validation, before any host is built; the
/// in-crate recipe validates.
#[test]
fn a_lease_beyond_the_election_floor_is_rejected_at_validation() {
    let sound = config();
    sound.validate().unwrap();
    let beyond = HostConfig {
        admission_lease_ms: sound.election_timeout_min_ms - sound.clock_skew_ms + 1,
        ..config()
    };
    let error = beyond.validate().unwrap_err();
    assert_eq!(error.code(), Code::InvalidArgument, "{error}");
    assert!(
        error.message().contains("election_timeout_min_ms"),
        "the refusal names the floor: {error}"
    );
    let zero = HostConfig {
        admission_lease_ms: 0,
        ..config()
    };
    let error = zero.validate().unwrap_err();
    assert_eq!(error.code(), Code::InvalidArgument, "{error}");
}

/// A member with no transport has no directory to change: a membership
/// operation refuses by name before the library sees it.
#[cfg(feature = "tls")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_member_without_a_transport_refuses_membership_changes_by_name() {
    let dir = Directory::new("not-networked");
    let group = identity(7);
    let host = RaftHost::bootstrap_single(
        &dir.0,
        &group,
        1,
        "https://node-1:19291",
        &policy(),
        &limits(),
        &config(),
    )
    .await
    .unwrap();
    let error = host.add_learner(2, "127.0.0.1:1").await.unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition, "{error}");
    assert_eq!(
        crate::raft::reasons::reason_of(&error),
        Some(crate::raft::reasons::MEMBERSHIP_NOT_NETWORKED),
        "{error}"
    );
    host.shutdown().await.unwrap();
}

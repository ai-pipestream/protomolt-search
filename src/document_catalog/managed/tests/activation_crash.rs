use super::*;

#[test]
fn activation_exit_worker() {
    let Some(mode) = std::env::var_os("PSEARCH_MANAGED_ACTIVATE_EXIT") else {
        return;
    };
    let root = PathBuf::from(std::env::var_os("PSEARCH_MANAGED_ACTIVATE_ROOT").unwrap());
    let local_authorizer: Arc<dyn Authorizer> =
        Arc::new(PolicyAuthority::new(access_policy(false)).unwrap());
    let admin = AccessPermit::acquire(
        local_authorizer.clone(),
        "alice",
        "books",
        AccessAction::Admin,
    )
    .unwrap();
    let ingest =
        AccessPermit::acquire(local_authorizer, "alice", "books", AccessAction::Ingest).unwrap();
    let catalog =
        AccessControlledCatalog::create(&root.join("source.redb"), &resource(), &admin).unwrap();
    let receipt = catalog.accept(&ingest, &write()).unwrap();
    let identity = authority_identity(7);
    let authority = SourceAuthorityStore::create(
        &root.join("authority.redb"),
        &identity,
        &access_policy(false),
        &authority_limits(),
    )
    .unwrap();
    let preparation = authority
        .execute("alice", &prepare_command(&identity, receipt.history_id))
        .unwrap()
        .owner
        .unwrap();
    let mut managed = catalog
        .bind_prepared_owner(
            &authority.admission("alice").unwrap(),
            &authority,
            &preparation,
            1 << 20,
        )
        .unwrap();
    let completion = managed.completion().unwrap().completion().clone();
    managed
        .confirm_ready("alice", &confirm_command(&identity, 2, completion))
        .unwrap();
    authority
        .execute("alice", &activate_command(&identity, 3))
        .unwrap();
    managed.activate_fault = Some(match mode.to_str().unwrap() {
        "before" => BindFault::ExitBeforeCommit,
        "after" => BindFault::ExitAfterCommit,
        other => panic!("unknown activation exit mode {other}"),
    });
    let admission = authority.admission("alice").unwrap();
    managed.activate(&admission).unwrap();
    panic!("activation exit fault did not terminate the worker");
}

#[test]
fn abrupt_activation_exit_recovers_format_nine_or_the_activated_source() {
    for mode in ["before", "after"] {
        let dir = Directory::new(&format!("activation-abrupt-{mode}"));
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("document_catalog::managed::tests::activation_crash::activation_exit_worker")
            .arg("--nocapture")
            .env("PSEARCH_MANAGED_ACTIVATE_EXIT", mode)
            .env("PSEARCH_MANAGED_ACTIVATE_ROOT", &dir.0)
            .status_guarded()
            .unwrap();
        assert_eq!(status.code(), Some(87));
        let identity = authority_identity(7);
        let authority = SourceAuthorityStore::open(&dir.authority(), &identity).unwrap();
        let owner = authority.owner("alice", &owner_key()).unwrap();
        assert_eq!(owner.phase, PreparedSourceOwnerPhase::Active as i32);
        let preparation = PreparedSourceOwner {
            phase: PreparedSourceOwnerPhase::Prepared as i32,
            control_revision: 2,
            last_command: Some(SourceAuthorityOperationKey {
                command_id: b"prepare-owner".to_vec(),
                ..owner.last_command.clone().unwrap()
            }),
            readiness: None,
            activation: None,
            ..owner.clone()
        };
        let binding = SourceManagedBinding {
            format_version: 1,
            authority: Some(identity.clone()),
            preparation: Some(preparation.clone()),
            bound_at_sequence: 1,
        };
        let admission = authority.admission("alice").unwrap();
        let active = if mode == "before" {
            assert_eq!(
                ActiveManagedCatalog::recover(&dir.catalog(), &authority, &admission, &binding)
                    .err()
                    .unwrap()
                    .code(),
                Code::FailedPrecondition
            );
            let managed = PreparedManagedCatalog::recover(
                &dir.catalog(),
                &authority,
                "alice",
                &identity,
                &preparation,
            )
            .unwrap();
            managed.activate(&admission).unwrap()
        } else {
            ActiveManagedCatalog::recover(&dir.catalog(), &authority, &admission, &binding).unwrap()
        };
        assert_eq!(
            active
                .accept(&admission, &second_write())
                .unwrap()
                .into_receipt()
                .accepted_sequence,
            2
        );
        active_records(&active, 2);
    }
}

/// A write admitted under a lease and paused inside its source transaction
/// past the lease is refused at its final check, before the commit: no
/// durable change and no retry record.
#[test]
fn a_write_paused_before_commit_past_its_lease_leaves_no_durable_change() {
    let fixture = fixture("paused-write", false);
    let identity = authority_identity(7);
    let (fixture, managed) = ready(fixture);
    assert_eq!(
        fixture
            .authority
            .execute("alice", &activate_command(&identity, 3))
            .unwrap()
            .code,
        0
    );
    let active = {
        let admission = fixture.authority.admission("alice").unwrap();
        managed.activate(&admission).unwrap()
    };
    let lease = crate::source_authority::AdmissionLease {
        anchor: std::time::Instant::now(),
        anchor_wall: std::time::SystemTime::now(),
        ttl: std::time::Duration::from_millis(40),
    };
    let admission = fixture.authority.leased_admission("alice", lease).unwrap();
    // Entry admits; the write then waits past the lease before its commit.
    active.arm_precommit_pause(std::time::Duration::from_millis(80));
    let error = active.accept(&admission, &second_write()).unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition, "{error}");
    assert!(error.message().contains("lease expired"), "{error}");
    drop(admission);
    // Nothing became durable and no retry record exists: a fresh admission
    // accepts the same request as new work at the next sequence.
    let admission = fixture.authority.admission("alice").unwrap();
    let receipt = active
        .accept(&admission, &second_write())
        .unwrap()
        .into_receipt();
    assert!(!receipt.replayed);
    assert_eq!(receipt.accepted_sequence, 2);
    // A pause that ends within the lease commits; the final check is where
    // the interval is measured.
    let lease = crate::source_authority::AdmissionLease {
        anchor: std::time::Instant::now(),
        anchor_wall: std::time::SystemTime::now(),
        ttl: std::time::Duration::from_millis(500),
    };
    let leased = fixture.authority.leased_admission("alice", lease).unwrap();
    active.arm_precommit_pause(std::time::Duration::from_millis(20));
    let mut third = second_write();
    third.document_key = b"document-three".to_vec();
    third.operation_id = b"accept-three".to_vec();
    assert_eq!(
        active
            .accept(&leased, &third)
            .unwrap()
            .into_receipt()
            .accepted_sequence,
        3
    );
}

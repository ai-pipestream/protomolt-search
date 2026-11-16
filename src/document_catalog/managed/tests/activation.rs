use super::*;

#[test]
fn activation_admits_writes_only_under_the_committed_fence() {
    let fixture = fixture("activation", false);
    let identity = authority_identity(7);
    let (fixture, managed) = ready(fixture);
    let managed = ready_alone_activates_nothing(&fixture, managed);
    the_control_activation_commits_the_fence(&fixture);
    let (active, receipt) = writes_are_admitted_under_the_fence(&fixture, managed);
    ingest_is_a_current_grant(&fixture, &active);
    active_is_terminal_for_the_generation(&fixture);
    let binding = active.binding().clone();
    drop(active);
    reopen_recovers_under_the_fence(&fixture, &binding, &receipt);
    // A revoked administrator cannot even reopen it.
    assert_eq!(
        fixture
            .authority
            .execute(
                "alice",
                &grants_command(&identity, "self-revoke", 6, 3, Vec::new())
            )
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    let admission = fixture.authority.admission("alice").unwrap();
    assert_eq!(
        ActiveManagedCatalog::recover(
            &fixture.dir.catalog(),
            &fixture.authority,
            &admission,
            &binding
        )
        .err()
        .unwrap()
        .code(),
        Code::PermissionDenied
    );
}

/// READY alone activates nothing on the source.
fn ready_alone_activates_nothing(
    fixture: &Fixture,
    managed: PreparedManagedCatalog,
) -> PreparedManagedCatalog {
    let identity = authority_identity(7);
    let admission = fixture.authority.admission("alice").unwrap();
    let error = match managed.activate(&admission) {
        Ok(_) => panic!("READY must not activate the source"),
        Err(error) => error,
    };
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("READY alone"), "{error}");
    PreparedManagedCatalog::recover(
        &fixture.dir.catalog(),
        &fixture.authority,
        "alice",
        &identity,
        &fixture.preparation,
    )
    .unwrap()
}

/// The control activation commits the fence: epoch = generation 1.
fn the_control_activation_commits_the_fence(fixture: &Fixture) {
    let identity = authority_identity(7);
    let activated = fixture
        .authority
        .execute("alice", &activate_command(&identity, 3))
        .unwrap();
    assert_eq!(activated.code, 0, "{}", activated.message);
    let owner = activated.owner.clone().unwrap();
    assert_eq!(owner.phase, PreparedSourceOwnerPhase::Active as i32);
    assert_eq!(owner.activation.as_ref().unwrap().write_epoch, 1);
    assert_eq!(
        owner
            .activation
            .as_ref()
            .unwrap()
            .activated_control_revision,
        4
    );
    assert_eq!(
        fixture
            .authority
            .execute("alice", &activate_command(&identity, 3))
            .unwrap(),
        activated
    );
}

/// The owner persists the fence and admits writes under it; returns the
/// active catalog and the second write's receipt.
fn writes_are_admitted_under_the_fence(
    fixture: &Fixture,
    managed: PreparedManagedCatalog,
) -> (ActiveManagedCatalog, crate::pb::DocumentWriteReceipt) {
    let active = {
        let admission = fixture.authority.admission("alice").unwrap();
        managed.activate(&admission).unwrap()
    };
    assert_eq!(active.activation().write_epoch, 1);
    assert_eq!(active.activation().activated_at_sequence, 1);
    active_records(&active, 1);
    let receipt = {
        let admission = fixture.authority.admission("alice").unwrap();
        active
            .accept(&admission, &second_write())
            .unwrap()
            .into_receipt()
    };
    assert_eq!(receipt.accepted_sequence, 2);
    assert_eq!(receipt.history_id, fixture.receipt.history_id);
    assert_eq!(receipt.write_epoch, active.activation().write_epoch);
    {
        let admission = fixture.authority.admission("alice").unwrap();
        assert_eq!(
            active
                .accept(&admission, &second_write())
                .unwrap()
                .into_receipt(),
            replayed(&receipt)
        );
        assert_eq!(
            active.write_target(&admission).unwrap().history_id,
            fixture.receipt.history_id
        );
    }
    active_records(&active, 2);
    (active, receipt)
}

/// Ingest is a current grant: another actor, and the same actor after a
/// revocation, are refused at the write's own admission.
fn ingest_is_a_current_grant(fixture: &Fixture, active: &ActiveManagedCatalog) {
    let identity = authority_identity(7);
    for actor in ["bob", "carol"] {
        let admission = fixture.authority.admission(actor).unwrap();
        assert_eq!(
            active
                .accept(&admission, &second_write())
                .err()
                .unwrap()
                .code(),
            Code::PermissionDenied,
            "{actor}"
        );
    }
    let admin_only = grants_command(
        &identity,
        "admin-only",
        4,
        1,
        vec![grant("alice", AccessAction::Admin)],
    );
    assert_eq!(
        fixture
            .authority
            .execute("alice", &admin_only)
            .unwrap()
            .code,
        0
    );
    {
        let admission = fixture.authority.admission("alice").unwrap();
        assert_eq!(
            active
                .accept(&admission, &third_write())
                .err()
                .unwrap()
                .code(),
            Code::PermissionDenied
        );
    }
    let mut both = grant("alice", AccessAction::Admin);
    both.actions.push(AccessAction::Ingest as i32);
    assert_eq!(
        fixture
            .authority
            .execute(
                "alice",
                &grants_command(&identity, "restore", 5, 2, vec![both])
            )
            .unwrap()
            .code,
        0
    );
}

/// ACTIVE is terminal for the generation: no second preparation, no
/// cancellation, and no command allocates a fence from a lease.
fn active_is_terminal_for_the_generation(fixture: &Fixture) {
    let identity = authority_identity(7);
    let mut again = prepare_command(&identity, fixture.receipt.history_id.clone());
    again.command_id = b"prepare-again".to_vec();
    again.expected_control_revision = 6;
    again.expected_policy_revision = 3;
    again.expected_ownership_generation = 1;
    if let Some(Action::Prepare(request)) = again.action.as_mut() {
        request.workflow_id = b"installation-two".to_vec();
    }
    let again = fixture.authority.execute("alice", &again).unwrap();
    assert_eq!(
        again.code,
        Code::FailedPrecondition as u32,
        "{}",
        again.message
    );
    let cancel = SourceAuthorityCommand {
        command_id: b"cancel".to_vec(),
        expected_control_revision: 6,
        expected_policy_revision: 3,
        action: Some(Action::Cancel(CancelPreparedSourceOwner {
            workflow_id: b"installation-one".to_vec(),
        })),
        ..activate_command(&identity, 6)
    };
    assert_eq!(
        fixture.authority.execute("alice", &cancel).unwrap().code,
        Code::FailedPrecondition as u32
    );
}

/// Reopen paths: the closed adapters refuse the activated file, the
/// active adapter recovers it under the committed fence.
fn reopen_recovers_under_the_fence(
    fixture: &Fixture,
    binding: &SourceManagedBinding,
    receipt: &crate::pb::DocumentWriteReceipt,
) {
    let identity = authority_identity(7);
    assert_eq!(
        AccessControlledCatalog::open(&fixture.dir.catalog(), &resource(), &fixture.admin)
            .err()
            .unwrap()
            .code(),
        Code::FailedPrecondition
    );
    assert_eq!(
        PreparedManagedCatalog::recover(
            &fixture.dir.catalog(),
            &fixture.authority,
            "alice",
            &identity,
            &fixture.preparation,
        )
        .err()
        .unwrap()
        .code(),
        Code::FailedPrecondition
    );
    let admission = fixture.authority.admission("alice").unwrap();
    let recovered = ActiveManagedCatalog::recover(
        &fixture.dir.catalog(),
        &fixture.authority,
        &admission,
        binding,
    )
    .unwrap();
    assert_eq!(
        recovered
            .accept(&admission, &second_write())
            .unwrap()
            .into_receipt(),
        replayed(receipt)
    );
    assert_eq!(
        recovered
            .accept(&admission, &third_write())
            .unwrap()
            .into_receipt()
            .accepted_sequence,
        3
    );
    active_records(&recovered, 3);
    drop(admission);
    drop(recovered);
}

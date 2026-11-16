use super::*;

#[test]
fn readiness_is_confirmed_from_the_held_binding_and_survives_reopen() {
    let fixture = fixture("readiness", false);
    let identity = authority_identity(7);
    let managed = fixture
        .catalog
        .bind_prepared_owner(
            &fixture.authority.admission("alice").unwrap(),
            &fixture.authority,
            &fixture.preparation,
            1 << 20,
        )
        .unwrap();
    let binding = managed.binding("alice").unwrap();
    let completion = managed.completion().unwrap().completion().clone();
    assert_eq!(completion.bound_at_sequence, 1);
    assert_eq!(
        completion.binding_sha256,
        crate::sha256::digest(&binding.encode_to_vec())
    );
    assert_eq!(completion.history_id, fixture.receipt.history_id);
    // Caller bytes that differ from the held binding are not admitted.
    let mut forged = completion.clone();
    forged.bound_at_sequence = 7;
    assert_eq!(
        managed
            .confirm_ready("alice", &confirm_command(&identity, 2, forged))
            .err()
            .unwrap()
            .code(),
        Code::PermissionDenied
    );
    // Another administrator's confirmation is his own decision; the binding is
    // the proof, so it commits under his actor scope.
    let ready = managed
        .confirm_ready("alice", &confirm_command(&identity, 2, completion.clone()))
        .unwrap();
    assert_eq!(ready.code, 0, "{}", ready.message);
    let owner = fixture.authority.owner("alice", &owner_key()).unwrap();
    assert_eq!(owner.phase, PreparedSourceOwnerPhase::Ready as i32);
    assert_eq!(
        owner.readiness.as_ref().unwrap().completion.as_ref(),
        Some(&completion)
    );
    // The source binding is unchanged; exact managed recovery still opens it
    // and confirms nothing twice.
    drop(managed);
    let recovered = PreparedManagedCatalog::recover(
        &fixture.dir.catalog(),
        &fixture.authority,
        "alice",
        &identity,
        &fixture.preparation,
    )
    .unwrap();
    assert_eq!(recovered.binding("alice").unwrap(), binding);
    assert_eq!(
        recovered
            .confirm_ready("alice", &confirm_command(&identity, 2, completion))
            .unwrap(),
        ready
    );
    assert_managed_records(&recovered, 1);
}

#[test]
fn a_binding_committed_before_a_crash_is_confirmed_after_recovery() {
    let dir = Directory::new("bind-then-confirm");
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("document_catalog::managed::tests::binding_crash::abrupt_binding_exit_worker")
        .arg("--nocapture")
        .env("PSEARCH_MANAGED_BIND_EXIT", "after")
        .env("PSEARCH_MANAGED_BIND_ROOT", &dir.0)
        .status_guarded()
        .unwrap();
    assert_eq!(status.code(), Some(87));
    let identity = authority_identity(7);
    let authority = SourceAuthorityStore::open(&dir.authority(), &identity).unwrap();
    let preparation = authority.owner("alice", &owner_key()).unwrap();
    assert_eq!(preparation.phase, PreparedSourceOwnerPhase::Prepared as i32);
    let managed = PreparedManagedCatalog::recover(
        &dir.catalog(),
        &authority,
        "alice",
        &identity,
        &preparation,
    )
    .unwrap();
    let completion = managed.completion().unwrap().completion().clone();
    let ready = managed
        .confirm_ready("alice", &confirm_command(&identity, 2, completion))
        .unwrap();
    assert_eq!(ready.code, 0, "{}", ready.message);
    assert_eq!(
        authority.owner("alice", &owner_key()).unwrap().phase,
        PreparedSourceOwnerPhase::Ready as i32
    );
}

#[test]
fn readiness_requires_current_authority_admin() {
    let fixture = fixture("readiness-revoked", false);
    let identity = authority_identity(7);
    let managed = fixture
        .catalog
        .bind_prepared_owner(
            &fixture.authority.admission("alice").unwrap(),
            &fixture.authority,
            &fixture.preparation,
            1 << 20,
        )
        .unwrap();
    let mut alice_policy = access_policy(false);
    alice_policy.grants.push(grant("bob", AccessAction::Admin));
    let bob = SourceAuthorityCommand {
        format_version: 1,
        authority: Some(identity.clone()),
        key: Some(LogicalSourceOwner {
            workspace: "workspace-a".into(),
            collection: "books".into(),
            owner_id: Vec::new(),
        }),
        command_id: b"alice-adds-bob".to_vec(),
        expected_control_revision: 2,
        expected_policy_revision: 1,
        expected_ownership_generation: 0,
        action: Some(Action::ReplaceGrants(ReplaceSourceCollectionGrants {
            grants: alice_policy.grants.clone(),
        })),
    };
    assert_eq!(fixture.authority.execute("alice", &bob).unwrap().code, 0);
    let revoke = SourceAuthorityCommand {
        command_id: b"bob-revokes-alice".to_vec(),
        expected_control_revision: 3,
        expected_policy_revision: 2,
        action: Some(Action::ReplaceGrants(ReplaceSourceCollectionGrants {
            grants: vec![grant("bob", AccessAction::Admin)],
        })),
        ..bob.clone()
    };
    assert_eq!(fixture.authority.execute("bob", &revoke).unwrap().code, 0);
    let completion = managed.completion().unwrap().completion().clone();
    let mut confirm = confirm_command(&identity, 4, completion);
    confirm.expected_policy_revision = 3;
    assert_eq!(
        managed
            .confirm_ready("alice", &confirm)
            .err()
            .unwrap()
            .code(),
        Code::PermissionDenied
    );
    assert_eq!(
        fixture.authority.owner("bob", &owner_key()).unwrap().phase,
        PreparedSourceOwnerPhase::Prepared as i32
    );
}

use super::*;

#[test]
fn malformed_expected_owner_is_invalid_input_without_changing_either_store() {
    let fixture = fixture("malformed-expected-owner", false);
    let mut malformed = fixture.preparation.clone();
    malformed.key.as_mut().unwrap().owner_id = b"different-from-last-command".to_vec();
    let error = fixture
        .catalog
        .bind_prepared_owner(
            &fixture.authority.admission("alice").unwrap(),
            &fixture.authority,
            &malformed,
            1 << 20,
        )
        .err()
        .unwrap();
    assert_eq!(error.code(), Code::InvalidArgument);
    assert_eq!(
        fixture.authority.owner("alice", &owner_key()).unwrap(),
        fixture.preparation
    );
    let controlled =
        AccessControlledCatalog::open(&fixture.dir.catalog(), &resource(), &fixture.admin).unwrap();
    assert_eq!(controlled.resource_binding(), &resource());
}

#[test]
fn binding_metadata_budget_covers_the_larger_managed_result() {
    let fixture = fixture("result-metadata-budget", false);
    let existing_metadata_bytes = {
        let checkpoint = fixture.catalog.inner.capture_checkpoint(1 << 20).unwrap();
        checkpoint.metadata().encoded_len()
    };
    let error = fixture
        .catalog
        .bind_prepared_owner(
            &fixture.authority.admission("alice").unwrap(),
            &fixture.authority,
            &fixture.preparation,
            existing_metadata_bytes,
        )
        .err()
        .unwrap();
    assert_eq!(error.code(), Code::ResourceExhausted);
    assert!(error.message().contains("metadata budget"), "{error}");
    assert_eq!(
        fixture.authority.owner("alice", &owner_key()).unwrap(),
        fixture.preparation
    );
    let controlled =
        AccessControlledCatalog::open(&fixture.dir.catalog(), &resource(), &fixture.admin).unwrap();
    assert_eq!(controlled.resource_binding(), &resource());
}

#[test]
fn a_committed_owner_for_another_resource_cannot_claim_the_catalog() {
    let dir = Directory::new("wrong-resource");
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
    let catalog = AccessControlledCatalog::create(&dir.catalog(), &resource(), &admin).unwrap();
    let receipt = catalog.accept(&ingest, &write()).unwrap();

    let mut authority_policy = access_policy(false);
    authority_policy.resources.push(CollectionResource {
        workspace: "workspace-a".into(),
        collection: "music".into(),
    });
    authority_policy.grants.push(CollectionGrant {
        principal: "alice".into(),
        workspace: "workspace-a".into(),
        collection: "music".into(),
        actions: vec![AccessAction::Admin as i32],
        ..Default::default()
    });
    let identity = authority_identity(7);
    let authority = SourceAuthorityStore::create(
        &dir.authority(),
        &identity,
        &authority_policy,
        &authority_limits(),
    )
    .unwrap();
    let mut command = prepare_command(&identity, receipt.history_id);
    command.key.as_mut().unwrap().collection = "music".into();
    let preparation = authority.execute("alice", &command).unwrap().owner.unwrap();
    let error = catalog
        .bind_prepared_owner(
            &authority.admission("alice").unwrap(),
            &authority,
            &preparation,
            1 << 20,
        )
        .err()
        .unwrap();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("resource"), "{error}");
    assert_eq!(
        authority
            .owner("alice", preparation.key.as_ref().unwrap())
            .unwrap(),
        preparation
    );
    let reopened = AccessControlledCatalog::open(&dir.catalog(), &resource(), &admin).unwrap();
    assert_eq!(reopened.resource_binding(), &resource());
}

#[test]
fn a_committed_owner_for_another_history_cannot_claim_the_catalog() {
    let dir = Directory::new("wrong-committed-history");
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
    let catalog = AccessControlledCatalog::create(&dir.catalog(), &resource(), &admin).unwrap();
    let receipt = catalog.accept(&ingest, &write()).unwrap();

    let identity = authority_identity(7);
    let authority = SourceAuthorityStore::create(
        &dir.authority(),
        &identity,
        &access_policy(false),
        &authority_limits(),
    )
    .unwrap();
    let preparation = authority
        .execute("alice", &prepare_command(&identity, vec![99; 16]))
        .unwrap()
        .owner
        .unwrap();
    assert_ne!(
        preparation.target.as_ref().unwrap().history_id,
        receipt.history_id
    );
    let error = catalog
        .bind_prepared_owner(
            &authority.admission("alice").unwrap(),
            &authority,
            &preparation,
            1 << 20,
        )
        .err()
        .unwrap();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("history"), "{error}");
    assert_eq!(authority.owner("alice", &owner_key()).unwrap(), preparation);
    let reopened = AccessControlledCatalog::open(&dir.catalog(), &resource(), &admin).unwrap();
    assert_eq!(reopened.resource_binding(), &resource());
}

#[test]
fn managed_metadata_requires_current_authority_admin() {
    let fixture = fixture("authority-revocation", false);
    let expected = SourceManagedBinding {
        format_version: 1,
        authority: Some(authority_identity(7)),
        preparation: Some(fixture.preparation.clone()),
        bound_at_sequence: fixture.receipt.accepted_sequence,
    };
    let managed = fixture
        .catalog
        .bind_prepared_owner(
            &fixture.authority.admission("alice").unwrap(),
            &fixture.authority,
            &fixture.preparation,
            1 << 20,
        )
        .unwrap();
    let revoke = SourceAuthorityCommand {
        format_version: 1,
        authority: Some(authority_identity(7)),
        key: Some(LogicalSourceOwner {
            workspace: "workspace-a".into(),
            collection: "books".into(),
            owner_id: Vec::new(),
        }),
        command_id: b"revoke-alice".to_vec(),
        expected_control_revision: 2,
        expected_policy_revision: 1,
        expected_ownership_generation: 0,
        action: Some(Action::ReplaceGrants(ReplaceSourceCollectionGrants {
            grants: Vec::new(),
        })),
    };
    assert_eq!(
        fixture
            .authority
            .execute("alice", &revoke)
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    assert_eq!(
        managed.binding("alice").unwrap_err().code(),
        Code::PermissionDenied
    );
    assert_eq!(
        managed.inspect("alice", 1 << 20).unwrap_err().code(),
        Code::PermissionDenied
    );
    drop(managed);
    assert_eq!(
        PreparedManagedCatalog::recover(
            &fixture.dir.catalog(),
            &fixture.authority,
            "alice",
            expected.authority.as_ref().unwrap(),
            &fixture.preparation,
        )
        .err()
        .unwrap()
        .code(),
        Code::PermissionDenied
    );
}

#[test]
fn managed_open_refuses_missing_and_malformed_or_mismatched_bindings() {
    let missing = Directory::new("missing");
    let identity = authority_identity(7);
    let authority = SourceAuthorityStore::create(
        &missing.authority(),
        &identity,
        &access_policy(false),
        &authority_limits(),
    )
    .unwrap();
    let preparation = authority
        .execute("alice", &prepare_command(&identity, vec![9; 16]))
        .unwrap()
        .owner
        .unwrap();
    let placeholder = SourceManagedBinding {
        format_version: 1,
        authority: Some(identity),
        preparation: Some(preparation),
        bound_at_sequence: 0,
    };
    assert_eq!(
        PreparedManagedCatalog::recover(
            &missing.catalog(),
            &authority,
            "alice",
            placeholder.authority.as_ref().unwrap(),
            placeholder.preparation.as_ref().unwrap(),
        )
        .err()
        .unwrap()
        .code(),
        Code::NotFound
    );
    assert!(!missing.catalog().exists());

    for (damage, expected_code) in [
        ("missing-binding", Code::DataLoss),
        ("newer-format", Code::FailedPrecondition),
        ("unknown-field", Code::DataLoss),
    ] {
        let fixture = fixture(damage, false);
        let path = fixture.dir.catalog();
        let expected = SourceManagedBinding {
            format_version: 1,
            authority: Some(authority_identity(7)),
            preparation: Some(fixture.preparation.clone()),
            bound_at_sequence: 1,
        };
        let managed = fixture
            .catalog
            .bind_prepared_owner(
                &fixture.authority.admission("alice").unwrap(),
                &fixture.authority,
                &fixture.preparation,
                1 << 20,
            )
            .unwrap();
        drop(managed);
        damage_header(&path, damage);
        let error = PreparedManagedCatalog::open(&path, &fixture.authority, "alice", &expected)
            .err()
            .unwrap();
        assert_eq!(error.code(), expected_code);
        if damage == "newer-format" {
            assert!(error.message().contains("format"), "{error}");
        }
    }
}

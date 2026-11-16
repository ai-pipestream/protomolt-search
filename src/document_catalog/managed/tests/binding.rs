use super::*;
use std::path::Path;

#[test]
fn binding_preserves_history_actor_retry_and_closes_every_source_writer() {
    let fixture = fixture("preservation", false);
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
    assert_eq!(managed.binding("alice").unwrap(), expected);
    let checkpoint = managed.inspect("alice", 1 << 20).unwrap();
    let header = checkpoint.header.unwrap();
    assert_eq!(header.format_version, MANAGED_FORMAT);
    assert_eq!(header.history_id, fixture.receipt.history_id);
    assert_eq!(header.accepted_sequence, 1);
    assert_eq!(header.managed_binding, Some(expected.clone()));
    let error = PreparedManagedCatalog::recover(
        &path,
        &fixture.authority,
        "alice",
        expected.authority.as_ref().unwrap(),
        &fixture.preparation,
    )
    .err()
    .unwrap();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("lock"), "{error}");

    stored_records_survive_the_binding(&managed, &fixture.receipt);
    let fresh_write = AcceptDocumentRequest {
        operation_id: b"after-binding".to_vec(),
        document_key: b"document-two".to_vec(),
        ..write()
    };
    every_source_writer_refuses(&managed, &fixture.receipt.history_id, &fresh_write);

    drop(managed);
    plain_and_mismatched_opens_refuse(
        &path,
        &fixture.dir,
        &fixture.authority,
        &fixture.admin,
        &fixture.preparation,
        &expected,
    );
    let cancel = cancel_command(&fixture.preparation, b"cancel-after-binding");
    assert_eq!(fixture.authority.execute("alice", &cancel).unwrap().code, 0);
    let reopened = PreparedManagedCatalog::recover(
        &path,
        &fixture.authority,
        "alice",
        expected.authority.as_ref().unwrap(),
        &fixture.preparation,
    )
    .unwrap();
    assert_eq!(reopened.binding("alice").unwrap(), expected);
    assert_managed_records(&reopened, 1);
    let error = reopened
        .inner
        .accept_as(&fresh_write, Some("alice"), None, 0)
        .unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("managed"), "{error}");
}

/// The actor-scoped retry record, the source and its descriptor are the
/// bytes accepted before the binding.
fn stored_records_survive_the_binding(
    managed: &PreparedManagedCatalog,
    receipt: &crate::pb::DocumentWriteReceipt,
) {
    let read = managed.inner.database.begin_read().unwrap();
    let operations = read.open_table(actors::OPERATIONS).unwrap();
    let operation_key = ActorOperationKey {
        format_version: 1,
        principal: "alice".into(),
        operation_id: write().operation_id,
    }
    .encode_to_vec();
    let operation: DocumentOperation = decode(
        operations
            .get(operation_key.as_slice())
            .unwrap()
            .unwrap()
            .value(),
    )
    .unwrap();
    assert_eq!(operation.receipt.as_ref().unwrap(), receipt);
    let expected_source = match write().mutation.unwrap() {
        Mutation::Source(source) => source,
        _ => unreachable!(),
    };
    let sources = read.open_table(SOURCES).unwrap();
    let source: SourceRecord = {
        let (_, value) = sources.iter().unwrap().next().unwrap().unwrap();
        decode(value.value()).unwrap()
    };
    assert_eq!(source.message_type, expected_source.message_type);
    assert_eq!(source.payload, expected_source.payload);
    let descriptors = read.open_table(DESCRIPTORS).unwrap();
    let descriptor = {
        let (_, value) = descriptors.iter().unwrap().next().unwrap().unwrap();
        value.value().to_vec()
    };
    assert_eq!(descriptor, expected_source.descriptor_set);
    drop(descriptors);
    drop(sources);
    drop(operations);
    drop(read);
}

/// Acceptance, retirement, sealing and recovery through the bound
/// catalog's inner handle all refuse, naming the managed state.
fn every_source_writer_refuses(
    managed: &PreparedManagedCatalog,
    history_id: &[u8],
    fresh_write: &AcceptDocumentRequest,
) {
    let error = managed
        .inner
        .accept_as(fresh_write, Some("alice"), None, 0)
        .unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("managed"), "{error}");
    let error = managed
        .inner
        .begin_retirement(&SourceRetirementRequest {
            history_id: history_id.to_vec(),
            operation_id: b"retire-managed".to_vec(),
        })
        .unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("managed"), "{error}");
    let error = managed
        .inner
        .seal_history(&SourceSealRequest {
            history_id: history_id.to_vec(),
            expected_accepted_sequence: 1,
            operation_id: b"seal-managed".to_vec(),
        })
        .unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("managed"), "{error}");
    let error = managed.inner.recovery_transaction().err().unwrap();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("managed"), "{error}");
}

/// The plain and access-controlled opens refuse a managed file, and a
/// managed recovery refuses another owner or another authority.
fn plain_and_mismatched_opens_refuse(
    path: &Path,
    dir: &Directory,
    authority: &SourceAuthorityStore,
    admin: &AccessPermit,
    preparation: &PreparedSourceOwner,
    expected: &SourceManagedBinding,
) {
    let error = DocumentCatalog::open(path, "books").err().unwrap();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("managed"), "{error}");
    let error = AccessControlledCatalog::open(path, &resource(), admin)
        .err()
        .unwrap();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("managed"), "{error}");
    let mut wrong_preparation = preparation.clone();
    rename_owner(&mut wrong_preparation, b"other-owner");
    assert_eq!(
        PreparedManagedCatalog::recover(
            path,
            authority,
            "alice",
            expected.authority.as_ref().unwrap(),
            &wrong_preparation,
        )
        .err()
        .unwrap()
        .code(),
        Code::FailedPrecondition
    );
    let wrong_authority = SourceAuthorityStore::create(
        &dir.0.join("wrong-open-authority.redb"),
        &authority_identity(19),
        &access_policy(false),
        &authority_limits(),
    )
    .unwrap();
    assert_eq!(
        PreparedManagedCatalog::recover(
            path,
            &wrong_authority,
            "alice",
            expected.authority.as_ref().unwrap(),
            preparation,
        )
        .err()
        .unwrap()
        .code(),
        Code::FailedPrecondition
    );
}

#[test]
fn binding_requires_the_exact_current_owner_authority_history_and_actor() {
    #[derive(Clone, Copy)]
    enum Invalid {
        Owner,
        Authority,
        History,
        Stale,
        Actor,
    }
    for (invalid, expected_code) in [
        (Invalid::Owner, Code::FailedPrecondition),
        (Invalid::Authority, Code::FailedPrecondition),
        (Invalid::History, Code::FailedPrecondition),
        (Invalid::Stale, Code::FailedPrecondition),
        (Invalid::Actor, Code::PermissionDenied),
    ] {
        let name = match invalid {
            Invalid::Owner => "wrong-owner",
            Invalid::Authority => "wrong-authority",
            Invalid::History => "wrong-history",
            Invalid::Stale => "stale-preparation",
            Invalid::Actor => "wrong-actor",
        };
        let fixture = fixture(name, true);
        let mut preparation = fixture.preparation.clone();
        let mut principal = "alice";
        match invalid {
            Invalid::Owner => rename_owner(&mut preparation, b"another-owner"),
            Invalid::Authority => {}
            Invalid::History => preparation.target.as_mut().unwrap().history_id = vec![91; 16],
            Invalid::Stale => {
                let cancel = cancel_command(&preparation, b"cancel-owner");
                assert_eq!(fixture.authority.execute("alice", &cancel).unwrap().code, 0);
            }
            Invalid::Actor => principal = "bob",
        }
        let rogue_authority = if matches!(invalid, Invalid::Authority) {
            Some(
                SourceAuthorityStore::create(
                    &fixture.dir.0.join("wrong-authority.redb"),
                    &authority_identity(19),
                    &access_policy(false),
                    &authority_limits(),
                )
                .unwrap(),
            )
        } else {
            None
        };
        let authority = rogue_authority.as_ref().unwrap_or(&fixture.authority);
        let error = authority
            .admission(principal)
            .and_then(|admission| {
                fixture
                    .catalog
                    .bind_prepared_owner(&admission, authority, &preparation, 1 << 20)
            })
            .err()
            .unwrap();
        assert_eq!(error.code(), expected_code, "{name}: {error}");
        assert!(fixture.dir.catalog().exists());
    }
}

use super::*;
use crate::test_support::ForkGuarded;
use crate::{
    authorization::{AccessPermit, Authorizer, PolicyAuthority},
    pb::{
        accept_document_request::Mutation, storage::source_authority_command::Action, storage::*,
        AcceptDocumentRequest, AccessAction, AccessPolicy, CollectionGrant, CollectionResource,
        ProtobufSource,
    },
    source_authority::SourceAuthorityStore,
};
use prost::Message;
use redb::{ReadableTable, ReadableTableMetadata};
use std::{path::PathBuf, sync::Arc};
use tonic::Code;

struct Directory(PathBuf);

impl Directory {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "managed-catalog-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn catalog(&self) -> PathBuf {
        self.0.join("source.redb")
    }

    fn authority(&self) -> PathBuf {
        self.0.join("authority.redb")
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

fn resource() -> SourceResourceBinding {
    SourceResourceBinding {
        format_version: 1,
        workspace: "workspace-a".into(),
        collection: "books".into(),
    }
}

fn grant(principal: &str, action: AccessAction) -> CollectionGrant {
    CollectionGrant {
        principal: principal.into(),
        workspace: "workspace-a".into(),
        collection: "books".into(),
        actions: vec![action as i32],
        ..Default::default()
    }
}

fn access_policy(include_bob: bool) -> AccessPolicy {
    let mut alice = grant("alice", AccessAction::Admin);
    alice.actions.push(AccessAction::Ingest as i32);
    let mut grants = vec![alice];
    if include_bob {
        grants.push(grant("bob", AccessAction::Admin));
    }
    AccessPolicy {
        format_version: 1,
        revision: 1,
        resources: vec![CollectionResource {
            workspace: "workspace-a".into(),
            collection: "books".into(),
        }],
        grants,
    }
}

fn authority_identity(seed: u8) -> SourceAuthorityIdentity {
    SourceAuthorityIdentity {
        format_version: 1,
        group_id: vec![seed; 16],
        authority_incarnation: vec![seed.wrapping_add(1); 16],
    }
}

fn authority_limits() -> SourceAuthorityLimits {
    SourceAuthorityLimits {
        max_owners: 16,
        max_decisions: 32,
        max_payload_bytes: 1 << 20,
        max_command_bytes: 64 << 10,
    }
}

fn write() -> AcceptDocumentRequest {
    AcceptDocumentRequest {
        contract_version: 1,
        document_key: b"document-one".to_vec(),
        operation_id: b"accept-one".to_vec(),
        expected_version: Some(0),
        mutation: Some(Mutation::Source(ProtobufSource {
            descriptor_set: include_bytes!(
                "../../../tests/fixtures/protobuf-semantics/descriptor.bin"
            )
            .to_vec(),
            message_type: "semantics.Doc".into(),
            payload: vec![8, 7],
        })),
        ..Default::default()
    }
}

fn prepare_command(
    identity: &SourceAuthorityIdentity,
    history_id: Vec<u8>,
) -> SourceAuthorityCommand {
    SourceAuthorityCommand {
        format_version: 1,
        authority: Some(identity.clone()),
        key: Some(LogicalSourceOwner {
            workspace: "workspace-a".into(),
            collection: "books".into(),
            owner_id: b"phone-owner".to_vec(),
        }),
        command_id: b"prepare-owner".to_vec(),
        expected_control_revision: 1,
        expected_policy_revision: 1,
        expected_ownership_generation: 0,
        action: Some(Action::Prepare(PrepareSourceOwner {
            workflow_id: b"installation-one".to_vec(),
            target: Some(SourceStorageTarget {
                node_id: "server-a".into(),
                storage_incarnation: vec![41; 16],
                history_id,
                residency: SourceResidency::Server as i32,
                resident_device_id: String::new(),
            }),
        })),
    }
}

fn owner_key() -> LogicalSourceOwner {
    LogicalSourceOwner {
        workspace: "workspace-a".into(),
        collection: "books".into(),
        owner_id: b"phone-owner".to_vec(),
    }
}

struct Fixture {
    dir: Directory,
    catalog: AccessControlledCatalog,
    authority: SourceAuthorityStore,
    admin: AccessPermit,
    preparation: PreparedSourceOwner,
    receipt: crate::pb::DocumentWriteReceipt,
}

fn fixture(name: &str, bob_has_local_admin: bool) -> Fixture {
    let dir = Directory::new(name);
    let local_authorizer: Arc<dyn Authorizer> =
        Arc::new(PolicyAuthority::new(access_policy(bob_has_local_admin)).unwrap());
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
        .execute(
            "alice",
            &prepare_command(&identity, receipt.history_id.clone()),
        )
        .unwrap()
        .owner
        .unwrap();
    Fixture {
        dir,
        catalog,
        authority,
        admin,
        preparation,
        receipt,
    }
}

fn confirm_command(
    identity: &SourceAuthorityIdentity,
    control_revision: u64,
    completion: SourceOwnerCompletion,
) -> SourceAuthorityCommand {
    SourceAuthorityCommand {
        format_version: 1,
        authority: Some(identity.clone()),
        key: Some(owner_key()),
        command_id: b"confirm-ready".to_vec(),
        expected_control_revision: control_revision,
        expected_policy_revision: 1,
        expected_ownership_generation: 1,
        action: Some(Action::ConfirmReady(ConfirmSourceOwnerReady {
            workflow_id: b"installation-one".to_vec(),
            completion: Some(completion),
        })),
    }
}

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
        .arg("document_catalog::managed::tests::abrupt_binding_exit_worker")
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

fn assert_managed_records(managed: &PreparedManagedCatalog, sequence: u64) {
    let checkpoint = managed.inspect("alice", 1 << 20).unwrap();
    assert_eq!(checkpoint.header.unwrap().accepted_sequence, sequence);
    let read = managed.inner.database.begin_read().unwrap();
    assert_eq!(
        read.open_table(actors::OPERATIONS).unwrap().len().unwrap(),
        1
    );
    assert_eq!(read.open_table(SOURCES).unwrap().len().unwrap(), 1);
    assert_eq!(read.open_table(DESCRIPTORS).unwrap().len().unwrap(), 1);
}

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
    assert_eq!(operation.receipt.as_ref().unwrap(), &fixture.receipt);
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

    let fresh_write = AcceptDocumentRequest {
        operation_id: b"after-binding".to_vec(),
        document_key: b"document-two".to_vec(),
        ..write()
    };
    let error = managed
        .inner
        .accept_as(&fresh_write, Some("alice"), None, 0)
        .unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("managed"), "{error}");
    let error = managed
        .inner
        .begin_retirement(&SourceRetirementRequest {
            history_id: fixture.receipt.history_id.clone(),
            operation_id: b"retire-managed".to_vec(),
        })
        .unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("managed"), "{error}");
    let error = managed
        .inner
        .seal_history(&SourceSealRequest {
            history_id: fixture.receipt.history_id.clone(),
            expected_accepted_sequence: 1,
            operation_id: b"seal-managed".to_vec(),
        })
        .unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("managed"), "{error}");
    let error = managed.inner.recovery_transaction().err().unwrap();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("managed"), "{error}");

    drop(managed);
    let error = DocumentCatalog::open(&path, "books").err().unwrap();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("managed"), "{error}");
    let error = AccessControlledCatalog::open(&path, &resource(), &fixture.admin)
        .err()
        .unwrap();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("managed"), "{error}");
    let mut wrong_preparation = fixture.preparation.clone();
    wrong_preparation.key.as_mut().unwrap().owner_id = b"other-owner".to_vec();
    wrong_preparation
        .last_command
        .as_mut()
        .unwrap()
        .key
        .as_mut()
        .unwrap()
        .owner_id = b"other-owner".to_vec();
    assert_eq!(
        PreparedManagedCatalog::recover(
            &path,
            &fixture.authority,
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
        &fixture.dir.0.join("wrong-open-authority.redb"),
        &authority_identity(19),
        &access_policy(false),
        &authority_limits(),
    )
    .unwrap();
    assert_eq!(
        PreparedManagedCatalog::recover(
            &path,
            &wrong_authority,
            "alice",
            expected.authority.as_ref().unwrap(),
            &fixture.preparation,
        )
        .err()
        .unwrap()
        .code(),
        Code::FailedPrecondition
    );
    let cancel = SourceAuthorityCommand {
        format_version: 1,
        authority: Some(authority_identity(7)),
        key: fixture.preparation.key.clone(),
        command_id: b"cancel-after-binding".to_vec(),
        expected_control_revision: 2,
        expected_policy_revision: 1,
        expected_ownership_generation: 1,
        action: Some(Action::Cancel(CancelPreparedSourceOwner {
            workflow_id: fixture.preparation.workflow_id.clone(),
        })),
    };
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
            Invalid::Owner => {
                preparation.key.as_mut().unwrap().owner_id = b"another-owner".to_vec();
                preparation
                    .last_command
                    .as_mut()
                    .unwrap()
                    .key
                    .as_mut()
                    .unwrap()
                    .owner_id = b"another-owner".to_vec();
            }
            Invalid::Authority => {}
            Invalid::History => preparation.target.as_mut().unwrap().history_id = vec![91; 16],
            Invalid::Stale => {
                let cancel = SourceAuthorityCommand {
                    format_version: 1,
                    authority: Some(authority_identity(7)),
                    key: preparation.key.clone(),
                    command_id: b"cancel-owner".to_vec(),
                    expected_control_revision: 2,
                    expected_policy_revision: 1,
                    expected_ownership_generation: 1,
                    action: Some(Action::Cancel(CancelPreparedSourceOwner {
                        workflow_id: preparation.workflow_id.clone(),
                    })),
                };
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
        {
            let database = redb::Database::open(&path).unwrap();
            let write = database.begin_write().unwrap();
            {
                let mut meta = write.open_table(META).unwrap();
                let bytes = meta.get("header").unwrap().unwrap().value().to_vec();
                let mut header = DocumentCatalogHeader::decode(bytes.as_slice()).unwrap();
                let encoded = if damage == "missing-binding" {
                    header.managed_binding = None;
                    header.encode_to_vec()
                } else if damage == "newer-format" {
                    header.format_version = ACTIVE_MANAGED_FORMAT + 1;
                    header.encode_to_vec()
                } else {
                    let mut encoded = header.encode_to_vec();
                    encoded.extend_from_slice(&[0x98, 0x06, 0x01]);
                    encoded
                };
                meta.insert("header", encoded.as_slice()).unwrap();
            }
            write.commit().unwrap();
        }
        let error = PreparedManagedCatalog::open(&path, &fixture.authority, "alice", &expected)
            .err()
            .unwrap();
        assert_eq!(error.code(), expected_code);
        if damage == "newer-format" {
            assert!(error.message().contains("format"), "{error}");
        }
    }
}

#[test]
fn binding_commit_faults_leave_control_usable_and_source_recoverable_by_format() {
    for (name, fault, code) in [
        ("before-commit", BindFault::BeforeCommit, Code::Aborted),
        ("after-commit", BindFault::AfterCommit, Code::Internal),
    ] {
        let mut fixture = fixture(name, false);
        let expected = SourceManagedBinding {
            format_version: 1,
            authority: Some(authority_identity(7)),
            preparation: Some(fixture.preparation.clone()),
            bound_at_sequence: 1,
        };
        fixture.catalog.bind_fault = Some(fault);
        let error = fixture
            .catalog
            .bind_prepared_owner(
                &fixture.authority.admission("alice").unwrap(),
                &fixture.authority,
                &fixture.preparation,
                1 << 20,
            )
            .err()
            .unwrap();
        assert_eq!(error.code(), code);
        assert_eq!(
            fixture.authority.owner("alice", &owner_key()).unwrap(),
            fixture.preparation
        );
        if matches!(fault, BindFault::BeforeCommit) {
            let controlled =
                AccessControlledCatalog::open(&fixture.dir.catalog(), &resource(), &fixture.admin)
                    .unwrap();
            assert_eq!(controlled.resource_binding(), &resource());
        } else {
            let managed = PreparedManagedCatalog::recover(
                &fixture.dir.catalog(),
                &fixture.authority,
                "alice",
                expected.authority.as_ref().unwrap(),
                &fixture.preparation,
            )
            .unwrap();
            assert_eq!(managed.binding("alice").unwrap(), expected);
            assert_managed_records(&managed, 1);
        }
    }
}

#[test]
fn abrupt_binding_exit_worker() {
    let Some(mode) = std::env::var_os("PSEARCH_MANAGED_BIND_EXIT") else {
        return;
    };
    let root = PathBuf::from(std::env::var_os("PSEARCH_MANAGED_BIND_ROOT").unwrap());
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
    let mut catalog =
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
    catalog.bind_fault = Some(match mode.to_str().unwrap() {
        "before" => BindFault::ExitBeforeCommit,
        "after" => BindFault::ExitAfterCommit,
        other => panic!("unknown managed binding exit mode {other}"),
    });
    catalog
        .bind_prepared_owner(
            &authority.admission("alice").unwrap(),
            &authority,
            &preparation,
            1 << 20,
        )
        .unwrap();
    panic!("binding exit fault did not terminate the worker");
}

#[test]
fn abrupt_binding_exit_recovers_format_eight_or_committed_format_nine() {
    for mode in ["before", "after"] {
        let dir = Directory::new(&format!("abrupt-{mode}"));
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("document_catalog::managed::tests::abrupt_binding_exit_worker")
            .arg("--nocapture")
            .env("PSEARCH_MANAGED_BIND_EXIT", mode)
            .env("PSEARCH_MANAGED_BIND_ROOT", &dir.0)
            .status_guarded()
            .unwrap();
        assert_eq!(status.code(), Some(87));

        let identity = authority_identity(7);
        let authority = SourceAuthorityStore::open(&dir.authority(), &identity).unwrap();
        let preparation = authority.owner("alice", &owner_key()).unwrap();
        let local_authorizer: Arc<dyn Authorizer> =
            Arc::new(PolicyAuthority::new(access_policy(false)).unwrap());
        let admin =
            AccessPermit::acquire(local_authorizer, "alice", "books", AccessAction::Admin).unwrap();
        if mode == "before" {
            let controlled =
                AccessControlledCatalog::open(&dir.catalog(), &resource(), &admin).unwrap();
            assert_eq!(controlled.resource_binding(), &resource());
        } else {
            let managed = PreparedManagedCatalog::recover(
                &dir.catalog(),
                &authority,
                "alice",
                &identity,
                &preparation,
            )
            .unwrap();
            let binding = SourceManagedBinding {
                format_version: 1,
                authority: Some(identity),
                preparation: Some(preparation),
                bound_at_sequence: 1,
            };
            assert_eq!(managed.binding("alice").unwrap(), binding);
            assert_managed_records(&managed, 1);
        }
        assert_eq!(
            authority
                .policy("alice", "workspace-a", "books")
                .unwrap()
                .revision,
            1
        );
    }
}

fn activate_command(
    identity: &SourceAuthorityIdentity,
    control_revision: u64,
) -> SourceAuthorityCommand {
    SourceAuthorityCommand {
        format_version: 1,
        authority: Some(identity.clone()),
        key: Some(owner_key()),
        command_id: b"activate".to_vec(),
        expected_control_revision: control_revision,
        expected_policy_revision: 1,
        expected_ownership_generation: 1,
        action: Some(Action::Activate(ActivateSourceOwner {
            workflow_id: b"installation-one".to_vec(),
        })),
    }
}

fn second_write() -> AcceptDocumentRequest {
    AcceptDocumentRequest {
        document_key: b"document-two".to_vec(),
        operation_id: b"accept-two".to_vec(),
        ..write()
    }
}

/// Bind the fixture's catalog and confirm readiness; the control revision
/// afterwards is 3 (prepare, confirm).
fn ready(fixture: Fixture) -> (Fixture, PreparedManagedCatalog) {
    let identity = authority_identity(7);
    let Fixture {
        dir,
        catalog,
        authority,
        admin,
        preparation,
        receipt,
    } = fixture;
    let managed = catalog
        .bind_prepared_owner(
            &authority.admission("alice").unwrap(),
            &authority,
            &preparation,
            1 << 20,
        )
        .unwrap();
    let completion = managed.completion().unwrap().completion().clone();
    assert_eq!(
        managed
            .confirm_ready("alice", &confirm_command(&identity, 2, completion))
            .unwrap()
            .code,
        0
    );
    let placeholder =
        AccessControlledCatalog::create(&dir.0.join("placeholder.redb"), &resource(), &admin)
            .unwrap();
    (
        Fixture {
            dir,
            catalog: placeholder,
            authority,
            admin,
            preparation,
            receipt,
        },
        managed,
    )
}

fn active_records(active: &ActiveManagedCatalog, sequence: u64) {
    let read = active.inner.database.begin_read().unwrap();
    let meta = read.open_table(META).unwrap();
    let header = decode_header(meta.get("header").unwrap().unwrap().value()).unwrap();
    assert_eq!(header.format_version, 11);
    assert_eq!(header.accepted_sequence, sequence);
    assert!(header.managed_binding.is_some());
    assert_eq!(header.managed_activation.as_ref().unwrap().write_epoch, 1);
    // One actor-scoped operation per accepted write; identical source bytes
    // are stored once.
    assert_eq!(
        read.open_table(actors::OPERATIONS).unwrap().len().unwrap(),
        sequence
    );
    assert_eq!(read.open_table(SOURCES).unwrap().len().unwrap(), 1);
}

fn replayed(receipt: &crate::pb::DocumentWriteReceipt) -> crate::pb::DocumentWriteReceipt {
    crate::pb::DocumentWriteReceipt {
        replayed: true,
        ..receipt.clone()
    }
}

#[test]
fn activation_admits_writes_only_under_the_committed_fence() {
    let fixture = fixture("activation", false);
    let identity = authority_identity(7);
    let (fixture, managed) = ready(fixture);
    // READY alone activates nothing on the source.
    let managed = {
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
    };
    // The control activation commits the fence: epoch = generation 1.
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
    // The owner persists the fence and admits writes under it.
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
    // Ingest is a current grant: another actor, and the same actor after a
    // revocation, are refused at the write's own admission.
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
    let revoke = |id: &str, revision: u64, policy: u64, grants: Vec<CollectionGrant>| {
        SourceAuthorityCommand {
            format_version: 1,
            authority: Some(identity.clone()),
            key: Some(LogicalSourceOwner {
                workspace: "workspace-a".into(),
                collection: "books".into(),
                owner_id: Vec::new(),
            }),
            command_id: id.as_bytes().to_vec(),
            expected_control_revision: revision,
            expected_policy_revision: policy,
            expected_ownership_generation: 0,
            action: Some(Action::ReplaceGrants(ReplaceSourceCollectionGrants {
                grants,
            })),
        }
    };
    assert_eq!(
        fixture
            .authority
            .execute(
                "alice",
                &revoke(
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
    {
        let admission = fixture.authority.admission("alice").unwrap();
        let mut third = second_write();
        third.document_key = b"document-three".to_vec();
        third.operation_id = b"accept-three".to_vec();
        assert_eq!(
            active.accept(&admission, &third).err().unwrap().code(),
            Code::PermissionDenied
        );
    }
    let mut both = grant("alice", AccessAction::Admin);
    both.actions.push(AccessAction::Ingest as i32);
    assert_eq!(
        fixture
            .authority
            .execute("alice", &revoke("restore", 5, 2, vec![both]))
            .unwrap()
            .code,
        0
    );
    // ACTIVE is terminal for the generation: no second preparation, no
    // cancellation, and no command allocates a fence from a lease.
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
    // Reopen paths: the closed adapters refuse the activated file, the
    // active adapter recovers it under the committed fence.
    let binding = active.binding().clone();
    drop(active);
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
        &binding,
    )
    .unwrap();
    assert_eq!(
        recovered
            .accept(&admission, &second_write())
            .unwrap()
            .into_receipt(),
        replayed(&receipt)
    );
    let mut third = second_write();
    third.document_key = b"document-three".to_vec();
    third.operation_id = b"accept-three".to_vec();
    assert_eq!(
        recovered
            .accept(&admission, &third)
            .unwrap()
            .into_receipt()
            .accepted_sequence,
        3
    );
    active_records(&recovered, 3);
    drop(admission);
    // A revoked administrator cannot even reopen it.
    drop(recovered);
    assert_eq!(
        fixture
            .authority
            .execute("alice", &revoke("self-revoke", 6, 3, Vec::new()))
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
            .arg("document_catalog::managed::tests::activation_exit_worker")
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
/// durable change and no retry record (docs/raft-admission.md, "Source
/// write boundary").
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

// ---------------------------------------------------------------------------
// Write outcomes (docs/document-writes.md, "Write outcomes"): a write whose
// commit returns after its lease lapsed is unconfirmed until a fresh lease
// settles it; the decision is accepted while the actor's right is current
// and fenced, by name, once it is gone.
// ---------------------------------------------------------------------------

fn short_lease(ttl_ms: u64) -> crate::source_authority::AdmissionLease {
    crate::source_authority::AdmissionLease {
        anchor: std::time::Instant::now(),
        anchor_wall: std::time::SystemTime::now(),
        ttl: std::time::Duration::from_millis(ttl_ms),
    }
}

fn grants_command(
    identity: &SourceAuthorityIdentity,
    id: &str,
    control_revision: u64,
    policy_revision: u64,
    grants: Vec<CollectionGrant>,
) -> SourceAuthorityCommand {
    SourceAuthorityCommand {
        format_version: 1,
        authority: Some(identity.clone()),
        key: Some(LogicalSourceOwner {
            workspace: "workspace-a".into(),
            collection: "books".into(),
            owner_id: Vec::new(),
        }),
        command_id: id.as_bytes().to_vec(),
        expected_control_revision: control_revision,
        expected_policy_revision: policy_revision,
        expected_ownership_generation: 0,
        action: Some(Action::ReplaceGrants(ReplaceSourceCollectionGrants {
            grants,
        })),
    }
}

fn history(active: &ActiveManagedCatalog) -> Vec<crate::pb::AcceptedDocumentVersion> {
    active
        .inner
        .read_accepted(&crate::pb::ReadAcceptedDocumentsRequest {
            after_sequence: 0,
            limit: 100,
            through_sequence: None,
            max_bytes: 1 << 20,
            history_id: Vec::new(),
        })
        .unwrap()
        .documents
}

fn recorded(active: &ActiveManagedCatalog, operation_id: &[u8]) -> (WriteOutcome, u64) {
    let operation = active
        .inner
        .recorded_operation(Some("alice"), operation_id)
        .unwrap()
        .unwrap();
    (
        WriteOutcome::try_from(operation.outcome).unwrap(),
        operation.fenced_at_revision,
    )
}

fn activated(name: &str) -> (Fixture, ActiveManagedCatalog) {
    let fixture = fixture(name, false);
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
    (fixture, active)
}

#[test]
fn a_write_durable_after_its_lease_lapsed_is_unconfirmed_until_settled() {
    let (fixture, active) = activated("lapsed-write");

    // The commit returns after the lease lapsed: durable, unconfirmed.
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
        recorded(&active, b"accept-two"),
        (WriteOutcome::Unconfirmed, 0)
    );

    // The lapsed admission decides nothing, by name, and the record stays.
    let error = active
        .settle(&admission, "alice", b"accept-two")
        .unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition, "{error}");
    assert!(error.message().contains("lease expired"), "{error}");
    assert_eq!(
        recorded(&active, b"accept-two"),
        (WriteOutcome::Unconfirmed, 0)
    );
    drop(admission);

    // Another actor cannot settle it.
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
        Settlement::Accepted(settled) => assert_eq!(settled, receipt),
        Settlement::Fenced(rejection) => panic!("fenced with a current right: {rejection}"),
    }
    assert_eq!(
        recorded(&active, b"accept-two"),
        (WriteOutcome::Accepted, 0)
    );
    // Settling again is the same answer, and the retry replays it.
    match active.settle(&fresh, "alice", b"accept-two").unwrap() {
        Settlement::Accepted(settled) => assert_eq!(settled, receipt),
        Settlement::Fenced(rejection) => panic!("{rejection}"),
    }
    assert_eq!(
        active.accept(&fresh, &second_write()).unwrap(),
        Acceptance::Accepted(replayed(&receipt))
    );
    drop(fresh);

    // A retry that finds the record unconfirmed settles it under its own
    // admission, which is fresh at entry.
    let mut third = second_write();
    third.document_key = b"document-three".to_vec();
    third.operation_id = b"accept-three".to_vec();
    let lapsed = fixture
        .authority
        .leased_admission("alice", short_lease(40))
        .unwrap();
    active.arm_postcommit_pause(std::time::Duration::from_millis(80));
    let third_receipt = active.accept(&lapsed, &third).unwrap().into_receipt();
    assert_eq!(
        recorded(&active, b"accept-three"),
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
        recorded(&active, b"accept-three"),
        (WriteOutcome::Accepted, 0)
    );

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
    let rejection = match active.settle(&fresh, "alice", b"accept-two").unwrap() {
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
    assert_eq!(recorded(&active, b"accept-two"), (WriteOutcome::Fenced, 5));

    // A fence is final: the retry replays the rejection, before any entry
    // check, and a settlement returns it again.
    let retry = active.accept(&fresh, &second_write()).unwrap_err();
    assert_eq!(retry.code(), Code::FailedPrecondition, "{retry}");
    assert_eq!(retry.message(), rejection.message());
    match active.settle(&fresh, "alice", b"accept-two").unwrap() {
        Settlement::Fenced(again) => assert_eq!(again.message(), rejection.message()),
        Settlement::Accepted(receipt) => panic!("{receipt:?}"),
    }
    // The same operation id with another request is still a different
    // write.
    let mut other = second_write();
    other.document_key = b"document-other".to_vec();
    let error = active.accept(&fresh, &other).unwrap_err();
    assert_eq!(error.code(), Code::AlreadyExists, "{error}");
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

    // Granted again (the command at control revision 5, policy revision
    // 2), alice's next write of the key is
    // version 2 at sequence 3, accepted within its lease.
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
        recorded(&active, b"accept-two-again"),
        (WriteOutcome::Accepted, 0)
    );
    let versions = history(&active);
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

use super::*;
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
        .status()
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
        .accept_as(&fresh_write, Some("alice"))
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
        .accept_as(&fresh_write, Some("alice"))
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
                    header.format_version = MANAGED_FORMAT + 1;
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
            .status()
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

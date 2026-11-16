//! Fixtures of the managed catalog tests: a local catalog with one
//! accepted write, a control authority that prepared its owner, and the
//! commands that move the owner to readiness.
use super::*;

pub(super) struct Directory(pub(super) PathBuf);

impl Directory {
    pub(super) fn new(name: &str) -> Self {
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

    pub(super) fn catalog(&self) -> PathBuf {
        self.0.join("source.redb")
    }

    pub(super) fn authority(&self) -> PathBuf {
        self.0.join("authority.redb")
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

pub(super) fn resource() -> SourceResourceBinding {
    SourceResourceBinding {
        format_version: 1,
        workspace: "workspace-a".into(),
        collection: "books".into(),
    }
}

pub(super) fn grant(principal: &str, action: AccessAction) -> CollectionGrant {
    CollectionGrant {
        principal: principal.into(),
        workspace: "workspace-a".into(),
        collection: "books".into(),
        actions: vec![action as i32],
        ..Default::default()
    }
}

pub(super) fn access_policy(include_bob: bool) -> AccessPolicy {
    let mut alice = grant("alice", AccessAction::Admin);
    alice.actions.push(AccessAction::Ingest as i32);
    let mut grants = vec![alice];
    if include_bob {
        grants.push(grant("bob", AccessAction::Admin));
    }
    AccessPolicy {
        format_version: 3,
        revision: 1,
        resources: vec![CollectionResource {
            workspace: "workspace-a".into(),
            collection: "books".into(),
        }],
        grants,
    }
}

pub(super) fn authority_identity(seed: u8) -> SourceAuthorityIdentity {
    SourceAuthorityIdentity {
        format_version: 1,
        group_id: vec![seed; 16],
        authority_incarnation: vec![seed.wrapping_add(1); 16],
    }
}

pub(super) fn authority_limits() -> SourceAuthorityLimits {
    SourceAuthorityLimits {
        max_owners: 16,
        max_decisions: 32,
        max_payload_bytes: 1 << 20,
        max_command_bytes: 64 << 10,
    }
}

pub(super) fn write() -> AcceptDocumentRequest {
    AcceptDocumentRequest {
        contract_version: 1,
        document_key: b"document-one".to_vec(),
        operation_id: b"accept-one".to_vec(),
        expected_version: Some(0),
        mutation: Some(Mutation::Source(ProtobufSource {
            descriptor_set: include_bytes!(
                "../../../../tests/fixtures/protobuf-semantics/descriptor.bin"
            )
            .to_vec(),
            message_type: "semantics.Doc".into(),
            payload: vec![8, 7],
        })),
        ..Default::default()
    }
}

pub(super) fn prepare_command(
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

pub(super) fn owner_key() -> LogicalSourceOwner {
    LogicalSourceOwner {
        workspace: "workspace-a".into(),
        collection: "books".into(),
        owner_id: b"phone-owner".to_vec(),
    }
}

pub(super) struct Fixture {
    pub(super) dir: Directory,
    pub(super) catalog: AccessControlledCatalog,
    pub(super) authority: SourceAuthorityStore,
    pub(super) admin: AccessPermit,
    pub(super) preparation: PreparedSourceOwner,
    pub(super) receipt: crate::pb::DocumentWriteReceipt,
}

pub(super) fn fixture(name: &str, bob_has_local_admin: bool) -> Fixture {
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

pub(super) fn confirm_command(
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

pub(super) fn assert_managed_records(managed: &PreparedManagedCatalog, sequence: u64) {
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

/// Cancel the prepared owner's workflow at control revision 2.
pub(super) fn cancel_command(
    preparation: &PreparedSourceOwner,
    command_id: &[u8],
) -> SourceAuthorityCommand {
    SourceAuthorityCommand {
        format_version: 1,
        authority: Some(authority_identity(7)),
        key: preparation.key.clone(),
        command_id: command_id.to_vec(),
        expected_control_revision: 2,
        expected_policy_revision: 1,
        expected_ownership_generation: 1,
        action: Some(Action::Cancel(CancelPreparedSourceOwner {
            workflow_id: preparation.workflow_id.clone(),
        })),
    }
}

/// Name another owner in the preparation and in its recorded command.
pub(super) fn rename_owner(preparation: &mut PreparedSourceOwner, owner_id: &[u8]) {
    preparation.key.as_mut().unwrap().owner_id = owner_id.to_vec();
    preparation
        .last_command
        .as_mut()
        .unwrap()
        .key
        .as_mut()
        .unwrap()
        .owner_id = owner_id.to_vec();
}

/// Rewrite the stored header of the catalog at `path` with one kind of
/// damage: no binding, a newer format, or an unknown field.
pub(super) fn damage_header(path: &std::path::Path, damage: &str) {
    let database = redb::Database::open(path).unwrap();
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

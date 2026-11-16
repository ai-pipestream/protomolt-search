//! Fixtures past readiness: activation under the committed fence, and
//! the leases, grants and records the write-outcome tests read.
use super::*;

pub(super) fn activate_command(
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

pub(super) fn second_write() -> AcceptDocumentRequest {
    AcceptDocumentRequest {
        document_key: b"document-two".to_vec(),
        operation_id: b"accept-two".to_vec(),
        ..write()
    }
}

/// Bind the fixture's catalog and confirm readiness; the control revision
/// afterwards is 3 (prepare, confirm).
pub(super) fn ready(fixture: Fixture) -> (Fixture, PreparedManagedCatalog) {
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

pub(super) fn active_records(active: &ActiveManagedCatalog, sequence: u64) {
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

pub(super) fn replayed(
    receipt: &crate::pb::DocumentWriteReceipt,
) -> crate::pb::DocumentWriteReceipt {
    crate::pb::DocumentWriteReceipt {
        replayed: true,
        ..receipt.clone()
    }
}

pub(super) fn short_lease(ttl_ms: u64) -> crate::source_authority::AdmissionLease {
    crate::source_authority::AdmissionLease {
        anchor: std::time::Instant::now(),
        anchor_wall: std::time::SystemTime::now(),
        ttl: std::time::Duration::from_millis(ttl_ms),
    }
}

pub(super) fn grants_command(
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

pub(super) fn history(active: &ActiveManagedCatalog) -> Vec<crate::pb::AcceptedDocumentVersion> {
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

pub(super) fn recorded(active: &ActiveManagedCatalog, operation_id: &[u8]) -> (WriteOutcome, u64) {
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

pub(super) fn activated(name: &str) -> (Fixture, ActiveManagedCatalog) {
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

/// A third document, after `second_write`.
pub(super) fn third_write() -> AcceptDocumentRequest {
    let mut third = second_write();
    third.document_key = b"document-three".to_vec();
    third.operation_id = b"accept-three".to_vec();
    third
}

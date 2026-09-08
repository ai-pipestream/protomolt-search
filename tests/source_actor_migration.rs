use pipestream_search::{
    authorization::{AccessPermit, Authorizer, PolicyAuthority},
    document_catalog::{AccessControlledCatalog, DocumentCatalog},
    pb::{
        accept_document_request::Mutation, storage::*, AcceptDocumentRequest, AccessAction,
        AccessPolicy, CollectionGrant, CollectionResource, ProtobufSource,
    },
    sha256,
};
use prost::Message;
use redb::{ReadableDatabase, ReadableTable, TableHandle};
use std::{path::PathBuf, sync::Arc};
use tonic::Code;

struct Directory(PathBuf);
impl Directory {
    fn new(name: &str) -> Self {
        let path = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
            "source-actor-migration-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
    fn catalog(&self) -> PathBuf {
        self.0.join("source.redb")
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

fn binding() -> SourceResourceBinding {
    SourceResourceBinding {
        format_version: 1,
        workspace: "workspace".into(),
        collection: "books".into(),
    }
}

fn policy(revision: u64, admin: bool, writer_a: bool, writer_b: bool) -> AccessPolicy {
    let mut grants = Vec::new();
    for (principal, action, allowed) in [
        ("administrator", AccessAction::Admin, admin),
        ("writer-a", AccessAction::Ingest, writer_a),
        ("writer-b", AccessAction::Ingest, writer_b),
    ] {
        if allowed {
            grants.push(CollectionGrant {
                principal: principal.into(),
                workspace: "workspace".into(),
                collection: "books".into(),
                actions: vec![action as i32],
                ..Default::default()
            });
        }
    }
    AccessPolicy {
        format_version: 1,
        revision,
        resources: vec![CollectionResource {
            workspace: "workspace".into(),
            collection: "books".into(),
        }],
        grants,
    }
}

fn permit(authority: Arc<dyn Authorizer>, principal: &str, action: AccessAction) -> AccessPermit {
    AccessPermit::acquire(authority, principal, "books", action).unwrap()
}

fn write(
    operation: &[u8],
    key: &[u8],
    payload: u8,
    expected: Option<u64>,
) -> AcceptDocumentRequest {
    AcceptDocumentRequest {
        contract_version: 1,
        document_key: key.to_vec(),
        operation_id: operation.to_vec(),
        expected_version: expected,
        mutation: Some(Mutation::Source(ProtobufSource {
            descriptor_set: include_bytes!("fixtures/protobuf-semantics/descriptor.bin").to_vec(),
            message_type: "semantics.Doc".into(),
            payload: vec![8, payload],
        })),
        ..Default::default()
    }
}

fn request_sha256(request: &AcceptDocumentRequest) -> Vec<u8> {
    sha256::digest(&request.encode_to_vec()).to_vec()
}

fn assignment(
    receipt: &pipestream_search::pb::DocumentWriteReceipt,
    request: &AcceptDocumentRequest,
    principal: &str,
) -> SourceActorAssignment {
    SourceActorAssignment {
        format_version: 1,
        history_id: receipt.history_id.clone(),
        operation_id: request.operation_id.clone(),
        principal: principal.into(),
        request_sha256: request_sha256(request),
    }
}

fn stored_header(path: &std::path::Path) -> DocumentCatalogHeader {
    let database = redb::Database::open(path).unwrap();
    let read = database.begin_read().unwrap();
    let metadata = read
        .open_table(redb::TableDefinition::<&str, &[u8]>::new("metadata"))
        .unwrap();
    DocumentCatalogHeader::decode(metadata.get("header").unwrap().unwrap().value()).unwrap()
}

fn make_legacy_controlled(path: &std::path::Path) {
    let database = redb::Database::open(path).unwrap();
    let tx = database.begin_write().unwrap();
    {
        let mut metadata = tx
            .open_table(redb::TableDefinition::<&str, &[u8]>::new("metadata"))
            .unwrap();
        let mut header =
            DocumentCatalogHeader::decode(metadata.get("header").unwrap().unwrap().value())
                .unwrap();
        header.format_version = 7;
        header.resource_binding = Some(binding());
        header.actor_namespace = None;
        metadata
            .insert("header", header.encode_to_vec().as_slice())
            .unwrap();
    }
    tx.commit().unwrap();
}

#[test]
fn empty_format_seven_catalog_auto_upgrades_and_accepts_new_actor_writes() {
    let dir = Directory::new("empty");
    drop(DocumentCatalog::create(&dir.catalog(), "books").unwrap());
    make_legacy_controlled(&dir.catalog());

    let authority: Arc<dyn Authorizer> =
        Arc::new(PolicyAuthority::new(policy(1, true, true, false)).unwrap());
    let admin = permit(authority.clone(), "administrator", AccessAction::Admin);
    let writer = permit(authority, "writer-a", AccessAction::Ingest);
    let catalog = AccessControlledCatalog::open(&dir.catalog(), &binding(), &admin).unwrap();
    let accepted = catalog
        .accept(&writer, &write(b"new", b"source\0one", 1, Some(0)))
        .unwrap();
    assert_eq!((accepted.version, accepted.accepted_sequence), (1, 1));
    drop(catalog);

    let header = stored_header(&dir.catalog());
    assert_eq!(header.format_version, 8);
    assert_eq!(
        header.actor_namespace,
        Some(SourceActorNamespace {
            format_version: 1,
            legacy_operations: 0,
            assigned_operations: 0,
        })
    );
}

#[test]
fn partial_assignment_keeps_new_writes_fenced_and_survives_reopen() {
    let dir = Directory::new("partial");
    let first = write(b"first", b"source\0one", 1, Some(0));
    let second = write(b"second", b"source\0two", 2, Some(0));
    let (first_receipt, second_receipt) = {
        let catalog = DocumentCatalog::create(&dir.catalog(), "books").unwrap();
        (
            catalog.accept(&first).unwrap(),
            catalog.accept(&second).unwrap(),
        )
    };
    make_legacy_controlled(&dir.catalog());

    let authority: Arc<dyn Authorizer> =
        Arc::new(PolicyAuthority::new(policy(1, true, true, true)).unwrap());
    let admin = permit(authority.clone(), "administrator", AccessAction::Admin);
    let writer_a = permit(authority.clone(), "writer-a", AccessAction::Ingest);
    let writer_b = permit(authority, "writer-b", AccessAction::Ingest);
    let catalog = AccessControlledCatalog::open(&dir.catalog(), &binding(), &admin).unwrap();

    for request in [&first, &write(b"third", b"source\0three", 3, Some(0))] {
        let error = catalog.accept(&writer_a, request).unwrap_err();
        assert_eq!(error.code(), Code::FailedPrecondition);
    }
    catalog
        .assign_legacy_actor(&admin, &assignment(&first_receipt, &first, "writer-a"))
        .unwrap();
    for request in [&first, &write(b"third", b"source\0three", 3, Some(0))] {
        let error = catalog.accept(&writer_a, request).unwrap_err();
        assert_eq!(error.code(), Code::FailedPrecondition);
    }
    drop(catalog);

    let catalog = AccessControlledCatalog::open(&dir.catalog(), &binding(), &admin).unwrap();
    assert_eq!(
        catalog.accept(&writer_a, &first).unwrap_err().code(),
        Code::FailedPrecondition
    );
    catalog
        .assign_legacy_actor(&admin, &assignment(&second_receipt, &second, "writer-b"))
        .unwrap();
    let mut expected_first = first_receipt.clone();
    expected_first.replayed = true;
    let mut expected_second = second_receipt.clone();
    expected_second.replayed = true;
    assert_eq!(catalog.accept(&writer_a, &first).unwrap(), expected_first);
    assert_eq!(catalog.accept(&writer_b, &second).unwrap(), expected_second);
    let accepted = catalog
        .accept(&writer_a, &write(b"third", b"source\0three", 3, Some(0)))
        .unwrap();
    assert_eq!(accepted.accepted_sequence, 3);
}

#[test]
fn assignment_is_exact_idempotent_and_requires_current_admin() {
    let dir = Directory::new("assignment-validation");
    let request = write(b"legacy", b"source\0one", 1, Some(0));
    let receipt = {
        let catalog = DocumentCatalog::create(&dir.catalog(), "books").unwrap();
        catalog.accept(&request).unwrap()
    };
    make_legacy_controlled(&dir.catalog());

    let authority = Arc::new(PolicyAuthority::new(policy(1, true, true, false)).unwrap());
    let admin = permit(authority.clone(), "administrator", AccessAction::Admin);
    let writer = permit(authority.clone(), "writer-a", AccessAction::Ingest);
    let catalog = AccessControlledCatalog::open(&dir.catalog(), &binding(), &admin).unwrap();
    let exact = assignment(&receipt, &request, "writer-a");
    assert_eq!(
        catalog
            .assign_legacy_actor(&writer, &exact)
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    let mut wrong_history = exact.clone();
    wrong_history.history_id[0] ^= 0xff;
    assert!(catalog.assign_legacy_actor(&admin, &wrong_history).is_err());
    catalog.assign_legacy_actor(&admin, &exact).unwrap();
    catalog.assign_legacy_actor(&admin, &exact).unwrap();

    let mut changed_sha = exact.clone();
    changed_sha.request_sha256[0] ^= 0xff;
    assert_eq!(
        catalog
            .assign_legacy_actor(&admin, &changed_sha)
            .unwrap_err()
            .code(),
        Code::AlreadyExists
    );
    let mut wrong_actor = exact.clone();
    wrong_actor.principal = "writer-b".into();
    assert!(catalog.assign_legacy_actor(&admin, &wrong_actor).is_err());
    let mut missing = exact.clone();
    missing.operation_id = b"missing".to_vec();
    assert!(catalog.assign_legacy_actor(&admin, &missing).is_err());

    authority.replace(policy(2, false, true, false)).unwrap();
    assert_eq!(
        catalog
            .assign_legacy_actor(&admin, &exact)
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
}

#[test]
fn failed_first_assignment_leaves_format_seven_and_creates_no_actor_table() {
    let dir = Directory::new("first-assignment-rollback");
    let request = write(b"legacy", b"source\0one", 1, Some(0));
    let receipt = {
        let catalog = DocumentCatalog::create(&dir.catalog(), "books").unwrap();
        catalog.accept(&request).unwrap()
    };
    make_legacy_controlled(&dir.catalog());

    let authority: Arc<dyn Authorizer> =
        Arc::new(PolicyAuthority::new(policy(1, true, true, false)).unwrap());
    let admin = permit(authority, "administrator", AccessAction::Admin);
    let catalog = AccessControlledCatalog::open(&dir.catalog(), &binding(), &admin).unwrap();
    let mut bad = assignment(&receipt, &request, "writer-a");
    bad.request_sha256[0] ^= 0xff;
    assert!(catalog.assign_legacy_actor(&admin, &bad).is_err());
    drop(catalog);

    let header = stored_header(&dir.catalog());
    assert_eq!(header.format_version, 7);
    assert_eq!(header.resource_binding, Some(binding()));
    assert_eq!(header.actor_namespace, None);
    let database = redb::Database::open(dir.catalog()).unwrap();
    let read = database.begin_read().unwrap();
    assert!(!read
        .list_tables()
        .unwrap()
        .any(|table| table.name() == "actor_operations"));
}

#[test]
fn sealed_legacy_assignment_only_attributes_metadata_and_preserves_the_terminal_fence() {
    let dir = Directory::new("sealed");
    let request = write(b"legacy", b"source\0one", 1, Some(0));
    let (receipt, seal) = {
        let catalog = DocumentCatalog::create(&dir.catalog(), "books").unwrap();
        let receipt = catalog.accept(&request).unwrap();
        let seal = catalog
            .seal_history(&SourceSealRequest {
                history_id: receipt.history_id.clone(),
                expected_accepted_sequence: receipt.accepted_sequence,
                operation_id: b"seal".to_vec(),
            })
            .unwrap();
        (receipt, seal)
    };
    make_legacy_controlled(&dir.catalog());

    let authority: Arc<dyn Authorizer> =
        Arc::new(PolicyAuthority::new(policy(1, true, true, false)).unwrap());
    let admin = permit(authority.clone(), "administrator", AccessAction::Admin);
    let writer = permit(authority, "writer-a", AccessAction::Ingest);
    let catalog = AccessControlledCatalog::open(&dir.catalog(), &binding(), &admin).unwrap();
    catalog
        .assign_legacy_actor(&admin, &assignment(&receipt, &request, "writer-a"))
        .unwrap();
    assert!(catalog.accept(&writer, &request).unwrap().replayed);
    assert_eq!(catalog.history_seal(&admin).unwrap(), Some(seal.clone()));
    let error = catalog
        .accept(&writer, &write(b"new", b"source\0two", 2, Some(0)))
        .unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("sealed"), "{error}");
    drop(catalog);

    let header = stored_header(&dir.catalog());
    assert_eq!(header.format_version, 8);
    assert_eq!(header.accepted_sequence, receipt.accepted_sequence);
    assert_eq!(header.history_id, receipt.history_id);
    assert_eq!(header.history_seal, Some(seal));
}

#[test]
fn a_fresh_actor_operation_cannot_be_reclassified_as_a_legacy_assignment() {
    let dir = Directory::new("fresh-is-not-legacy");
    let legacy = write(b"legacy", b"source\0one", 1, Some(0));
    let legacy_receipt = {
        let catalog = DocumentCatalog::create(&dir.catalog(), "books").unwrap();
        catalog.accept(&legacy).unwrap()
    };
    make_legacy_controlled(&dir.catalog());

    let authority: Arc<dyn Authorizer> =
        Arc::new(PolicyAuthority::new(policy(1, true, true, false)).unwrap());
    let admin = permit(authority.clone(), "administrator", AccessAction::Admin);
    let writer = permit(authority, "writer-a", AccessAction::Ingest);
    let catalog = AccessControlledCatalog::open(&dir.catalog(), &binding(), &admin).unwrap();
    catalog
        .assign_legacy_actor(&admin, &assignment(&legacy_receipt, &legacy, "writer-a"))
        .unwrap();
    let fresh = write(b"fresh", b"source\0two", 2, Some(0));
    let fresh_receipt = catalog.accept(&writer, &fresh).unwrap();
    assert!(!fresh_receipt.replayed);
    assert_eq!(fresh_receipt.accepted_sequence, 2);
    drop(catalog);

    let header_before = stored_header(&dir.catalog());
    let catalog = AccessControlledCatalog::open(&dir.catalog(), &binding(), &admin).unwrap();
    let error = catalog
        .assign_legacy_actor(&admin, &assignment(&fresh_receipt, &fresh, "writer-a"))
        .unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    let mut expected_replay = fresh_receipt.clone();
    expected_replay.replayed = true;
    assert_eq!(catalog.accept(&writer, &fresh).unwrap(), expected_replay);
    drop(catalog);
    assert_eq!(stored_header(&dir.catalog()), header_before);
}

fn append_varint(output: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        output.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

#[test]
fn oversized_legacy_attribution_record_refuses_without_a_partial_upgrade() {
    let dir = Directory::new("oversized-attribution");
    let request = write(b"legacy", b"source\0one", 1, Some(0));
    let receipt = {
        let catalog = DocumentCatalog::create(&dir.catalog(), "books").unwrap();
        catalog.accept(&request).unwrap()
    };
    make_legacy_controlled(&dir.catalog());

    {
        let database = redb::Database::open(dir.catalog()).unwrap();
        let tx = database.begin_write().unwrap();
        {
            let mut operations = tx
                .open_table(redb::TableDefinition::<&[u8], &[u8]>::new("operations"))
                .unwrap();
            let mut encoded = operations
                .get(request.operation_id.as_slice())
                .unwrap()
                .unwrap()
                .value()
                .to_vec();
            // Unknown length-delimited field 100. Its payload alone exceeds the
            // 64 KiB attribution-record bound while preserving the known value.
            append_varint(&mut encoded, (100 << 3) | 2);
            append_varint(&mut encoded, 65_537);
            encoded.resize(encoded.len() + 65_537, 0xa5);
            operations
                .insert(request.operation_id.as_slice(), encoded.as_slice())
                .unwrap();
        }
        tx.commit().unwrap();
    }

    let authority: Arc<dyn Authorizer> =
        Arc::new(PolicyAuthority::new(policy(1, true, true, false)).unwrap());
    let admin = permit(authority, "administrator", AccessAction::Admin);
    let catalog = AccessControlledCatalog::open(&dir.catalog(), &binding(), &admin).unwrap();
    let error = catalog
        .assign_legacy_actor(&admin, &assignment(&receipt, &request, "writer-a"))
        .unwrap_err();
    assert_eq!(error.code(), Code::ResourceExhausted);
    drop(catalog);

    let header = stored_header(&dir.catalog());
    assert_eq!(header.format_version, 7);
    assert_eq!(header.resource_binding, Some(binding()));
    assert_eq!(header.actor_namespace, None);
    let database = redb::Database::open(dir.catalog()).unwrap();
    let read = database.begin_read().unwrap();
    assert!(!read
        .list_tables()
        .unwrap()
        .any(|table| table.name() == "actor_operations"));
}

use pipestream_search::{
    authorization::{AccessPermit, Authorizer, PolicyAuthority},
    document_catalog::AccessControlledCatalog,
    pb::{
        accept_document_request::Mutation, storage::SourceResourceBinding, AcceptDocumentRequest,
        AccessAction, AccessPolicy, CollectionGrant, CollectionResource, ProtobufSource,
    },
};
use std::{path::PathBuf, sync::Arc};
use tonic::Code;

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
            "source-actor-retries-{}-{}",
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

fn policy(revision: u64, writer_a: bool, writer_b: bool) -> AccessPolicy {
    let mut grants = vec![CollectionGrant {
        principal: "administrator".into(),
        workspace: "workspace".into(),
        collection: "books".into(),
        actions: vec![AccessAction::Admin as i32],
        ..Default::default()
    }];
    for (principal, allowed) in [("writer-a", writer_a), ("writer-b", writer_b)] {
        if allowed {
            grants.push(CollectionGrant {
                principal: principal.into(),
                workspace: "workspace".into(),
                collection: "books".into(),
                actions: vec![AccessAction::Ingest as i32],
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
    operation_id: &[u8],
    document_key: &[u8],
    payload: u8,
    expected_version: Option<u64>,
) -> AcceptDocumentRequest {
    AcceptDocumentRequest {
        contract_version: 1,
        document_key: document_key.to_vec(),
        operation_id: operation_id.to_vec(),
        expected_version,
        mutation: Some(Mutation::Source(ProtobufSource {
            descriptor_set: include_bytes!("fixtures/protobuf-semantics/descriptor.bin").to_vec(),
            message_type: "semantics.Doc".into(),
            payload: vec![8, payload],
        })),
        ..Default::default()
    }
}

#[test]
fn shared_operation_id_does_not_let_one_principal_squat_another_principals_write() {
    let dir = Directory::new();
    let authority = Arc::new(PolicyAuthority::new(policy(1, true, true)).unwrap());
    let admin = permit(authority.clone(), "administrator", AccessAction::Admin);
    let writer_a = permit(authority.clone(), "writer-a", AccessAction::Ingest);
    let writer_b = permit(authority.clone(), "writer-b", AccessAction::Ingest);
    let request_a = write(b"shared-operation", b"source\0a", 1, Some(0));
    let request_b = write(b"shared-operation", b"source\0b", 2, Some(0));

    let catalog = AccessControlledCatalog::create(&dir.catalog(), &binding(), &admin).unwrap();
    let accepted_a = catalog.accept(&writer_a, &request_a).unwrap();
    let accepted_b = catalog.accept(&writer_b, &request_b).unwrap();
    assert_eq!(
        (accepted_a.accepted_sequence, accepted_b.accepted_sequence),
        (1, 2)
    );
    assert!(!accepted_a.replayed && !accepted_b.replayed);

    for (writer, original, changed_payload) in
        [(&writer_a, &request_a, 9), (&writer_b, &request_b, 10)]
    {
        let changed = write(
            &original.operation_id,
            &original.document_key,
            changed_payload,
            original.expected_version,
        );
        assert_eq!(
            catalog.accept(writer, &changed).unwrap_err().code(),
            Code::AlreadyExists
        );
    }
    drop(catalog);

    let catalog = AccessControlledCatalog::open(&dir.catalog(), &binding(), &admin).unwrap();
    let replay_a = catalog.accept(&writer_a, &request_a).unwrap();
    let replay_b = catalog.accept(&writer_b, &request_b).unwrap();
    assert!(replay_a.replayed && replay_b.replayed);
    assert_eq!(replay_a.accepted_sequence, accepted_a.accepted_sequence);
    assert_eq!(replay_b.accepted_sequence, accepted_b.accepted_sequence);
}

#[test]
fn identical_request_bytes_do_not_disclose_another_principals_receipt() {
    let dir = Directory::new();
    let authority = Arc::new(PolicyAuthority::new(policy(1, true, true)).unwrap());
    let admin = permit(authority.clone(), "administrator", AccessAction::Admin);
    let writer_a = permit(authority.clone(), "writer-a", AccessAction::Ingest);
    let writer_b = permit(authority.clone(), "writer-b", AccessAction::Ingest);
    let identical = write(b"identical-operation", b"source\0same", 3, None);

    let catalog = AccessControlledCatalog::create(&dir.catalog(), &binding(), &admin).unwrap();
    let accepted_a = catalog.accept(&writer_a, &identical).unwrap();
    let accepted_b = catalog.accept(&writer_b, &identical).unwrap();
    assert_eq!(
        (accepted_a.accepted_sequence, accepted_b.accepted_sequence),
        (1, 2)
    );
    assert_eq!((accepted_a.version, accepted_b.version), (1, 2));
    assert!(!accepted_a.replayed && !accepted_b.replayed);
    assert_ne!(accepted_a, accepted_b);
    drop(catalog);

    let catalog = AccessControlledCatalog::open(&dir.catalog(), &binding(), &admin).unwrap();
    let replay_a = catalog.accept(&writer_a, &identical).unwrap();
    let replay_b = catalog.accept(&writer_b, &identical).unwrap();
    assert!(replay_a.replayed && replay_b.replayed);
    assert_eq!(replay_a.accepted_sequence, accepted_a.accepted_sequence);
    assert_eq!(replay_b.accepted_sequence, accepted_b.accepted_sequence);

    authority.replace(policy(2, false, true)).unwrap();
    assert_eq!(
        catalog.accept(&writer_a, &identical).unwrap_err().code(),
        Code::PermissionDenied
    );
    let current_b = permit(authority, "writer-b", AccessAction::Ingest);
    assert!(catalog.accept(&current_b, &identical).unwrap().replayed);
}

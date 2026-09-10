use pipestream_search::test_support::ForkGuarded;
use pipestream_search::{
    authorization::{AccessPermit, Authorizer, PolicyAuthority},
    document_catalog::{AccessControlledCatalog, DocumentCatalog},
    pb::{
        accept_document_request::Mutation, storage::*, AcceptDocumentRequest, AccessAction,
        AccessDecision, AccessPolicy, CollectionGrant, CollectionResource, ProtobufSource,
    },
};
use prost::Message;
use redb::{ReadableDatabase, ReadableTable};
use std::{path::PathBuf, sync::Arc};
use tonic::{Code, Status};

struct Directory(PathBuf);
impl Directory {
    fn new(name: &str) -> Self {
        let path = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
            "source-access-{name}-{}-{}",
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

fn binding(workspace: &str) -> SourceResourceBinding {
    SourceResourceBinding {
        format_version: 1,
        workspace: workspace.into(),
        collection: "books".into(),
    }
}

fn policy(revision: u64, workspace: &str) -> AccessPolicy {
    AccessPolicy {
        format_version: 1,
        revision,
        resources: vec![CollectionResource {
            workspace: workspace.into(),
            collection: "books".into(),
        }],
        grants: [
            ("administrator", AccessAction::Admin),
            ("writer", AccessAction::Ingest),
            ("reader", AccessAction::Search),
        ]
        .into_iter()
        .map(|(principal, action)| CollectionGrant {
            principal: principal.into(),
            workspace: workspace.into(),
            collection: "books".into(),
            actions: vec![action as i32],
            ..Default::default()
        })
        .collect(),
    }
}

fn permit(authority: Arc<dyn Authorizer>, principal: &str, action: AccessAction) -> AccessPermit {
    AccessPermit::acquire(authority, principal, "books", action).unwrap()
}

fn write() -> AcceptDocumentRequest {
    AcceptDocumentRequest {
        contract_version: 1,
        document_key: b"source\0one".to_vec(),
        operation_id: b"accept-one".to_vec(),
        expected_version: Some(0),
        mutation: Some(Mutation::Source(ProtobufSource {
            descriptor_set: include_bytes!("fixtures/protobuf-semantics/descriptor.bin").to_vec(),
            message_type: "semantics.Doc".into(),
            payload: vec![8, 7],
        })),
        ..Default::default()
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

#[test]
fn actions_are_narrow_and_unauthorized_create_leaves_no_authority() {
    let dir = Directory::new("actions");
    let authority: Arc<dyn Authorizer> =
        Arc::new(PolicyAuthority::new(policy(1, "workspace-a")).unwrap());
    let admin = permit(authority.clone(), "administrator", AccessAction::Admin);
    let ingest = permit(authority.clone(), "writer", AccessAction::Ingest);
    let search = permit(authority, "reader", AccessAction::Search);

    for denied in [&ingest, &search] {
        let path = dir
            .0
            .join(format!("denied-{}.redb", denied.decision().action));
        let error = AccessControlledCatalog::create(&path, &binding("workspace-a"), denied)
            .err()
            .unwrap();
        assert_eq!(error.code(), Code::PermissionDenied);
        assert!(!path.exists());
    }
    let catalog =
        AccessControlledCatalog::create(&dir.catalog(), &binding("workspace-a"), &admin).unwrap();
    for denied in [&admin, &search] {
        assert_eq!(
            catalog.accept(denied, &write()).unwrap_err().code(),
            Code::PermissionDenied
        );
    }
    let receipt = catalog.accept(&ingest, &write()).unwrap();
    assert_eq!(receipt.accepted_sequence, 1);
    for denied in [&ingest, &search] {
        assert_eq!(
            catalog
                .begin_retirement(
                    denied,
                    &SourceRetirementRequest {
                        history_id: receipt.history_id.clone(),
                        operation_id: b"retire".to_vec(),
                    },
                )
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
    }
    drop(catalog);
    assert_eq!(
        AccessControlledCatalog::open(&dir.catalog(), &binding("workspace-a"), &ingest)
            .err()
            .unwrap()
            .code(),
        Code::PermissionDenied
    );
}

#[test]
fn permitted_source_reopens_with_exact_resource_history_and_retry() {
    let dir = Directory::new("reopen");
    let authority: Arc<dyn Authorizer> =
        Arc::new(PolicyAuthority::new(policy(1, "workspace-a")).unwrap());
    let admin = permit(authority.clone(), "administrator", AccessAction::Admin);
    let ingest = permit(authority, "writer", AccessAction::Ingest);
    let original;
    {
        let catalog =
            AccessControlledCatalog::create(&dir.catalog(), &binding("workspace-a"), &admin)
                .unwrap();
        assert_eq!(catalog.resource_binding(), &binding("workspace-a"));
        original = catalog.accept(&ingest, &write()).unwrap();
    }
    let reopened =
        AccessControlledCatalog::open(&dir.catalog(), &binding("workspace-a"), &admin).unwrap();
    assert_eq!(reopened.resource_binding(), &binding("workspace-a"));
    let mut replay = original.clone();
    replay.replayed = true;
    assert_eq!(reopened.accept(&ingest, &write()).unwrap(), replay);
    assert_eq!(replay.history_id, original.history_id);
    drop(reopened);
    assert_eq!(
        DocumentCatalog::open(&dir.catalog(), "books")
            .err()
            .unwrap()
            .code(),
        Code::FailedPrecondition
    );
}

#[test]
fn invalid_bindings_leave_no_file_and_unbound_catalogs_are_not_adopted() {
    let dir = Directory::new("binding-validation");
    let authority: Arc<dyn Authorizer> =
        Arc::new(PolicyAuthority::new(policy(1, "workspace-a")).unwrap());
    let admin = permit(authority, "administrator", AccessAction::Admin);
    for (name, invalid) in [
        (
            "format",
            SourceResourceBinding {
                format_version: 0,
                ..binding("workspace-a")
            },
        ),
        (
            "workspace",
            SourceResourceBinding {
                workspace: String::new(),
                ..binding("workspace-a")
            },
        ),
        (
            "collection",
            SourceResourceBinding {
                collection: "other".into(),
                ..binding("workspace-a")
            },
        ),
    ] {
        let path = dir.0.join(format!("invalid-{name}.redb"));
        assert!(AccessControlledCatalog::create(&path, &invalid, &admin).is_err());
        assert!(!path.exists(), "{name}");
    }

    let unbound = dir.0.join("unbound.redb");
    drop(DocumentCatalog::create(&unbound, "books").unwrap());
    assert_eq!(
        AccessControlledCatalog::open(&unbound, &binding("workspace-a"), &admin)
            .err()
            .unwrap()
            .code(),
        Code::FailedPrecondition
    );
    assert!(unbound.exists());
}

#[test]
fn controlled_header_without_its_binding_is_data_loss() {
    let dir = Directory::new("missing-binding");
    let authority: Arc<dyn Authorizer> =
        Arc::new(PolicyAuthority::new(policy(1, "workspace-a")).unwrap());
    let admin = permit(authority, "administrator", AccessAction::Admin);
    drop(AccessControlledCatalog::create(&dir.catalog(), &binding("workspace-a"), &admin).unwrap());
    {
        let database = redb::Database::open(dir.catalog()).unwrap();
        let tx = database.begin_write().unwrap();
        {
            let mut metadata = tx
                .open_table(redb::TableDefinition::<&str, &[u8]>::new("metadata"))
                .unwrap();
            let mut header =
                DocumentCatalogHeader::decode(metadata.get("header").unwrap().unwrap().value())
                    .unwrap();
            assert_eq!(header.format_version, 8);
            header.resource_binding = None;
            metadata
                .insert("header", header.encode_to_vec().as_slice())
                .unwrap();
        }
        tx.commit().unwrap();
    }
    let error = AccessControlledCatalog::open(&dir.catalog(), &binding("workspace-a"), &admin)
        .err()
        .unwrap();
    assert_eq!(error.code(), Code::DataLoss);
}

#[test]
fn policy_replacement_and_workspace_remapping_invalidate_access() {
    let dir = Directory::new("policy-change");
    let authority = Arc::new(PolicyAuthority::new(policy(1, "workspace-a")).unwrap());
    let admin = permit(authority.clone(), "administrator", AccessAction::Admin);
    let ingest = permit(authority.clone(), "writer", AccessAction::Ingest);
    let catalog =
        AccessControlledCatalog::create(&dir.catalog(), &binding("workspace-a"), &admin).unwrap();
    authority.replace(policy(2, "workspace-a")).unwrap();
    assert_eq!(
        catalog.accept(&ingest, &write()).unwrap_err().code(),
        Code::PermissionDenied
    );
    drop(catalog);

    let current_admin = permit(authority.clone(), "administrator", AccessAction::Admin);
    let catalog =
        AccessControlledCatalog::open(&dir.catalog(), &binding("workspace-a"), &current_admin)
            .unwrap();
    authority.replace(policy(3, "workspace-b")).unwrap();
    let moved_admin = permit(authority.clone(), "administrator", AccessAction::Admin);
    let moved_ingest = permit(authority, "writer", AccessAction::Ingest);
    assert_eq!(
        catalog.accept(&moved_ingest, &write()).unwrap_err().code(),
        Code::PermissionDenied
    );
    drop(catalog);
    assert_eq!(
        AccessControlledCatalog::open(&dir.catalog(), &binding("workspace-a"), &moved_admin)
            .err()
            .unwrap()
            .code(),
        Code::PermissionDenied
    );
    assert_eq!(
        AccessControlledCatalog::open(&dir.catalog(), &binding("workspace-b"), &moved_admin)
            .err()
            .unwrap()
            .code(),
        Code::FailedPrecondition
    );
    assert_eq!(moved_ingest.decision().workspace, "workspace-b");
}

#[test]
fn retirement_seal_and_private_copy_retain_controlled_binding() {
    let dir = Directory::new("lifecycle");
    let authority: Arc<dyn Authorizer> =
        Arc::new(PolicyAuthority::new(policy(1, "workspace-a")).unwrap());
    let admin = permit(authority.clone(), "administrator", AccessAction::Admin);
    let ingest = permit(authority, "writer", AccessAction::Ingest);
    let path = dir.catalog();
    let catalog = AccessControlledCatalog::create(&path, &binding("workspace-a"), &admin).unwrap();
    let receipt = catalog.accept(&ingest, &write()).unwrap();
    let retirement = catalog
        .begin_retirement(
            &admin,
            &SourceRetirementRequest {
                history_id: receipt.history_id.clone(),
                operation_id: b"move-source".to_vec(),
            },
        )
        .unwrap();
    drop(catalog);
    let catalog = AccessControlledCatalog::open(&path, &binding("workspace-a"), &admin).unwrap();
    let replayed = catalog.accept(&ingest, &write()).unwrap();
    assert!(replayed.replayed);
    assert_eq!(replayed.accepted_sequence, receipt.accepted_sequence);
    let mut new_write = write();
    new_write.operation_id = b"accept-two".to_vec();
    new_write.expected_version = Some(1);
    let error = catalog.accept(&ingest, &new_write).unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("retiring"), "{error}");
    let seal = catalog
        .seal_history(
            &admin,
            &SourceSealRequest {
                history_id: receipt.history_id,
                expected_accepted_sequence: 1,
                operation_id: b"move-source".to_vec(),
            },
        )
        .unwrap();
    assert_eq!(
        catalog.retirement_intent(&admin).unwrap(),
        Some(retirement.clone())
    );
    assert_eq!(catalog.history_seal(&admin).unwrap(), Some(seal.clone()));
    assert!(catalog.accept(&ingest, &write()).unwrap().replayed);
    let error = catalog.accept(&ingest, &new_write).unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("sealed"), "{error}");
    drop(catalog);
    let header = stored_header(&path);
    assert_eq!(header.format_version, 8);
    assert_eq!(header.resource_binding, Some(binding("workspace-a")));
    assert_eq!(header.retirement_intent, Some(retirement.clone()));
    assert_eq!(header.history_seal, Some(seal.clone()));

    let copy = dir.0.join("copied.redb");
    std::fs::copy(&path, &copy).unwrap();
    let copied = AccessControlledCatalog::open(&copy, &binding("workspace-a"), &admin).unwrap();
    assert_eq!(copied.resource_binding(), &binding("workspace-a"));
    assert_eq!(copied.retirement_intent(&admin).unwrap(), Some(retirement));
    assert_eq!(copied.history_seal(&admin).unwrap(), Some(seal));
    assert!(copied.accept(&ingest, &write()).unwrap().replayed);
    let error = copied.accept(&ingest, &new_write).unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("sealed"), "{error}");
    drop(copied);
    assert_eq!(stored_header(&copy).format_version, 8);
}

#[test]
fn direct_seal_retains_the_controlled_format_and_binding() {
    let dir = Directory::new("direct-seal");
    let authority: Arc<dyn Authorizer> =
        Arc::new(PolicyAuthority::new(policy(1, "workspace-a")).unwrap());
    let admin = permit(authority.clone(), "administrator", AccessAction::Admin);
    let ingest = permit(authority, "writer", AccessAction::Ingest);
    let catalog =
        AccessControlledCatalog::create(&dir.catalog(), &binding("workspace-a"), &admin).unwrap();
    let receipt = catalog.accept(&ingest, &write()).unwrap();
    let seal = catalog
        .seal_history(
            &admin,
            &SourceSealRequest {
                history_id: receipt.history_id,
                expected_accepted_sequence: 1,
                operation_id: b"direct-seal".to_vec(),
            },
        )
        .unwrap();
    drop(catalog);
    let header = stored_header(&dir.catalog());
    assert_eq!(header.format_version, 8);
    assert_eq!(header.resource_binding, Some(binding("workspace-a")));
    assert!(header.retirement_intent.is_none());
    assert_eq!(header.history_seal, Some(seal.clone()));
    let reopened =
        AccessControlledCatalog::open(&dir.catalog(), &binding("workspace-a"), &admin).unwrap();
    assert_eq!(reopened.history_seal(&admin).unwrap(), Some(seal));
}

#[derive(Debug)]
struct LegacyAuthorizer {
    inner: PolicyAuthority,
}
impl Authorizer for LegacyAuthorizer {
    fn authorize(
        &self,
        principal: &str,
        collection: &str,
        action: AccessAction,
    ) -> Result<AccessDecision, Status> {
        self.inner.authorize(principal, collection, action)
    }
    fn subscribe(&self) -> tokio::sync::watch::Receiver<u64> {
        self.inner.subscribe()
    }
}

#[test]
fn legacy_authorizer_without_pinning_cannot_create_a_source() {
    let dir = Directory::new("legacy-authorizer");
    let authority: Arc<dyn Authorizer> = Arc::new(LegacyAuthorizer {
        inner: PolicyAuthority::new(policy(1, "workspace-a")).unwrap(),
    });
    let admin = permit(authority, "administrator", AccessAction::Admin);
    let error = AccessControlledCatalog::create(&dir.catalog(), &binding("workspace-a"), &admin)
        .err()
        .unwrap();
    assert_eq!(error.code(), Code::Unimplemented);
    assert!(!dir.catalog().exists());
}

#[cfg(unix)]
#[test]
fn source_catalog_creation_is_private_under_a_permissive_umask() {
    let dir = Directory::new("creation-mode");
    let status = std::process::Command::new("/bin/sh")
        .args(["-c", "umask 000; exec \"$@\"", "sh"])
        .arg(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "source_catalog_creation_mode_worker",
            "--nocapture",
        ])
        .env("PSEARCH_SOURCE_MODE_DIR", &dir.0)
        .status_guarded()
        .unwrap();
    assert!(status.success(), "creation-mode worker failed: {status}");
}

#[cfg(unix)]
#[test]
fn source_catalog_creation_mode_worker() {
    use std::os::unix::fs::PermissionsExt;

    let Some(root) = std::env::var_os("PSEARCH_SOURCE_MODE_DIR") else {
        return;
    };
    let root = PathBuf::from(root);
    let ordinary = root.join("ordinary.redb");
    drop(DocumentCatalog::create(&ordinary, "books").unwrap());
    assert_eq!(
        std::fs::metadata(&ordinary).unwrap().permissions().mode() & 0o777,
        0o600
    );

    let authority: Arc<dyn Authorizer> =
        Arc::new(PolicyAuthority::new(policy(1, "workspace-a")).unwrap());
    let admin = permit(authority, "administrator", AccessAction::Admin);
    let controlled = root.join("controlled.redb");
    drop(AccessControlledCatalog::create(&controlled, &binding("workspace-a"), &admin).unwrap());
    assert_eq!(
        std::fs::metadata(&controlled).unwrap().permissions().mode() & 0o777,
        0o600
    );
}

#[test]
fn revoked_ingest_permission_denies_a_stored_retry_during_retirement() {
    let dir = Directory::new("revoked-retirement-retry");
    let authority = Arc::new(PolicyAuthority::new(policy(1, "workspace-a")).unwrap());
    let admin = permit(authority.clone(), "administrator", AccessAction::Admin);
    let ingest = permit(authority.clone(), "writer", AccessAction::Ingest);
    let catalog =
        AccessControlledCatalog::create(&dir.catalog(), &binding("workspace-a"), &admin).unwrap();
    let receipt = catalog.accept(&ingest, &write()).unwrap();
    catalog
        .begin_retirement(
            &admin,
            &SourceRetirementRequest {
                history_id: receipt.history_id,
                operation_id: b"move-source".to_vec(),
            },
        )
        .unwrap();
    let mut revoked = policy(2, "workspace-a");
    revoked.grants.retain(|grant| grant.principal != "writer");
    authority.replace(revoked).unwrap();
    let error = catalog.accept(&ingest, &write()).unwrap_err();
    assert_eq!(error.code(), Code::PermissionDenied);
}

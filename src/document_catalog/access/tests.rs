use super::*;
use crate::{
    authorization::{AccessPermit, AuthorizationGuard, Authorizer, PolicyAuthority},
    pb::{
        accept_document_request::Mutation,
        storage::{DocumentCatalogHeader, SourceResourceBinding},
        AcceptDocumentRequest, AccessAction, AccessDecision, AccessPolicy, CollectionGrant,
        CollectionResource, ProtobufSource,
    },
};
use prost::Message;
use redb::ReadableTable;
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, Weak,
    },
};
use tonic::{Code, Status};

fn policy() -> AccessPolicy {
    AccessPolicy {
        format_version: 1,
        revision: 1,
        resources: vec![CollectionResource {
            workspace: "workspace-a".into(),
            collection: "books".into(),
        }],
        grants: [
            ("administrator", AccessAction::Admin),
            ("writer", AccessAction::Ingest),
        ]
        .into_iter()
        .map(|(principal, action)| CollectionGrant {
            principal: principal.into(),
            workspace: "workspace-a".into(),
            collection: "books".into(),
            actions: vec![action as i32],
            ..Default::default()
        })
        .collect(),
    }
}

fn binding() -> SourceResourceBinding {
    SourceResourceBinding {
        format_version: 1,
        workspace: "workspace-a".into(),
        collection: "books".into(),
    }
}

fn write() -> AcceptDocumentRequest {
    AcceptDocumentRequest {
        contract_version: 1,
        document_key: b"source-one".to_vec(),
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

fn temp_path(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "source-access-pin-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&root).unwrap();
    root.join("source.redb")
}

#[derive(Debug)]
struct ObservedAuthority {
    inner: PolicyAuthority,
    catalog: Mutex<Weak<AccessControlledCatalog>>,
    observed_sequence: AtomicU64,
}

impl Authorizer for ObservedAuthority {
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

    fn pin(&self, expected: &AccessDecision) -> Result<Box<dyn AuthorizationGuard + '_>, Status> {
        Ok(Box::new(ObservedGuard {
            inner: self.inner.pin(expected)?,
            catalog: &self.catalog,
            observed_sequence: &self.observed_sequence,
        }))
    }
}

struct ObservedGuard<'a> {
    inner: Box<dyn AuthorizationGuard + 'a>,
    catalog: &'a Mutex<Weak<AccessControlledCatalog>>,
    observed_sequence: &'a AtomicU64,
}

impl AuthorizationGuard for ObservedGuard<'_> {
    fn decision(&self) -> &AccessDecision {
        self.inner.decision()
    }
}

impl Drop for ObservedGuard<'_> {
    fn drop(&mut self) {
        // `inner` remains held until after this Drop body observes the commit.
        let Some(catalog) = self.catalog.lock().unwrap().upgrade() else {
            return;
        };
        let read = catalog.inner.database.begin_read().unwrap();
        let metadata = read.open_table(META).unwrap();
        let header: DocumentCatalogHeader =
            decode(metadata.get("header").unwrap().unwrap().value()).unwrap();
        self.observed_sequence
            .store(header.accepted_sequence, Ordering::SeqCst);
    }
}

#[test]
fn ingest_permission_remains_pinned_through_the_source_commit() {
    let path = temp_path("commit-order");
    let root = path.parent().unwrap().to_path_buf();
    let authority = Arc::new(ObservedAuthority {
        inner: PolicyAuthority::new(policy()).unwrap(),
        catalog: Mutex::new(Weak::new()),
        observed_sequence: AtomicU64::new(0),
    });
    let admin = AccessPermit::acquire(
        authority.clone(),
        "administrator",
        "books",
        AccessAction::Admin,
    )
    .unwrap();
    let catalog = Arc::new(AccessControlledCatalog::create(&path, &binding(), &admin).unwrap());
    *authority.catalog.lock().unwrap() = Arc::downgrade(&catalog);
    let ingest =
        AccessPermit::acquire(authority.clone(), "writer", "books", AccessAction::Ingest).unwrap();

    assert_eq!(
        catalog.accept(&ingest, &write()).unwrap().accepted_sequence,
        1
    );
    assert_eq!(authority.observed_sequence.load(Ordering::SeqCst), 1);

    drop(catalog);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn persisted_binding_tamper_is_checked_before_source_mutation() {
    let path = temp_path("binding-tamper");
    let root = path.parent().unwrap().to_path_buf();
    let authority: Arc<dyn Authorizer> = Arc::new(PolicyAuthority::new(policy()).unwrap());
    let admin = AccessPermit::acquire(
        authority.clone(),
        "administrator",
        "books",
        AccessAction::Admin,
    )
    .unwrap();
    let ingest = AccessPermit::acquire(authority, "writer", "books", AccessAction::Ingest).unwrap();
    let catalog = AccessControlledCatalog::create(&path, &binding(), &admin).unwrap();
    {
        let mut tx = catalog.inner.database.begin_write().unwrap();
        tx.set_durability(redb::Durability::Immediate).unwrap();
        {
            let mut metadata = tx.open_table(META).unwrap();
            let mut header: DocumentCatalogHeader =
                decode(metadata.get("header").unwrap().unwrap().value()).unwrap();
            header.resource_binding.as_mut().unwrap().workspace = "workspace-b".into();
            metadata
                .insert("header", header.encode_to_vec().as_slice())
                .unwrap();
        }
        tx.commit().unwrap();
    }

    let error = catalog.accept(&ingest, &write()).unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    let read = catalog.inner.database.begin_read().unwrap();
    let changes = read.open_table(CHANGES).unwrap();
    assert_eq!(changes.len().unwrap(), 0);

    drop(changes);
    drop(read);
    drop(catalog);
    std::fs::remove_dir_all(root).unwrap();
}

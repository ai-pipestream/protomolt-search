//! Fixtures of the access-controlled catalog tests: the policy and its
//! permits, an authority that observes the catalog while a permit is
//! pinned, and raw-table readers for the actor namespaces.
use super::*;

pub(super) fn policy() -> AccessPolicy {
    AccessPolicy {
        format_version: 3,
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

pub(super) fn binding() -> SourceResourceBinding {
    SourceResourceBinding {
        format_version: 1,
        workspace: "workspace-a".into(),
        collection: "books".into(),
    }
}

pub(super) fn write() -> AcceptDocumentRequest {
    AcceptDocumentRequest {
        contract_version: 1,
        document_key: b"source-one".to_vec(),
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

pub(super) fn temp_path(name: &str) -> PathBuf {
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
pub(super) struct ObservedAuthority {
    pub(super) inner: PolicyAuthority,
    pub(super) catalog: Mutex<Weak<AccessControlledCatalog>>,
    pub(super) observed_sequence: AtomicU64,
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

pub(super) struct ObservedGuard<'a> {
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

pub(super) fn actor_write(
    operation_id: &[u8],
    document_key: &[u8],
    payload: u8,
) -> AcceptDocumentRequest {
    let mut request = write();
    request.operation_id = operation_id.to_vec();
    request.document_key = document_key.to_vec();
    request.expected_version = Some(0);
    request.mutation = Some(Mutation::Source(ProtobufSource {
        descriptor_set: include_bytes!(
            "../../../../tests/fixtures/protobuf-semantics/descriptor.bin"
        )
        .to_vec(),
        message_type: "semantics.Doc".into(),
        payload: vec![8, payload],
    }));
    request
}

pub(super) fn actor_assignment(
    receipt: &crate::pb::DocumentWriteReceipt,
    request: &AcceptDocumentRequest,
    principal: &str,
) -> crate::pb::storage::SourceActorAssignment {
    crate::pb::storage::SourceActorAssignment {
        format_version: 1,
        history_id: receipt.history_id.clone(),
        operation_id: request.operation_id.clone(),
        principal: principal.into(),
        request_sha256: crate::sha256::digest(&request.encode_to_vec()).to_vec(),
    }
}

pub(super) fn make_legacy_controlled(path: &Path) {
    let database = redb::Database::open(path).unwrap();
    let tx = database.begin_write().unwrap();
    {
        let mut metadata = tx.open_table(META).unwrap();
        let mut header: DocumentCatalogHeader =
            decode(metadata.get("header").unwrap().unwrap().value()).unwrap();
        header.format_version = LEGACY_ACCESS_CONTROLLED_FORMAT;
        header.resource_binding = Some(binding());
        header.actor_namespace = None;
        metadata
            .insert("header", header.encode_to_vec().as_slice())
            .unwrap();
    }
    tx.commit().unwrap();
}

pub(super) fn checkpoint_limits() -> crate::pb::storage::DocumentCatalogCheckpointLimits {
    crate::pb::storage::DocumentCatalogCheckpointLimits {
        max_file_bytes: 64 << 20,
        batch_bytes: 1 << 20,
    }
}

pub(super) fn raw_binary_rows_from(
    database: &redb::Database,
    table_name: &'static str,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let read = database.begin_read().unwrap();
    let table = read
        .open_table(redb::TableDefinition::<&[u8], &[u8]>::new(table_name))
        .unwrap();
    table
        .iter()
        .unwrap()
        .map(|entry| {
            let (key, value) = entry.unwrap();
            (key.value().to_vec(), value.value().to_vec())
        })
        .collect()
}

pub(super) fn raw_binary_rows(path: &Path, table_name: &'static str) -> Vec<(Vec<u8>, Vec<u8>)> {
    let database = redb::Database::open(path).unwrap();
    raw_binary_rows_from(&database, table_name)
}

pub(super) fn append_unknown_to_legacy_operation(path: &Path, operation_id: &[u8]) -> Vec<u8> {
    let database = redb::Database::open(path).unwrap();
    let tx = database.begin_write().unwrap();
    let preserved = {
        let mut operations = tx.open_table(OPERATIONS).unwrap();
        let mut bytes = operations
            .get(operation_id)
            .unwrap()
            .unwrap()
            .value()
            .to_vec();
        // Unknown field 99, varint 1. Attribution and checkpoint copying must
        // move these exact stored bytes without decoding and re-encoding them.
        bytes.extend_from_slice(&[0x98, 0x06, 0x01]);
        operations.insert(operation_id, bytes.as_slice()).unwrap();
        bytes
    };
    tx.commit().unwrap();
    preserved
}

pub(super) fn assert_replay(
    catalog: &AccessControlledCatalog,
    ingest: &AccessPermit,
    request: &AcceptDocumentRequest,
    original: &crate::pb::DocumentWriteReceipt,
) {
    let mut expected = original.clone();
    expected.replayed = true;
    assert_eq!(catalog.accept(ingest, request).unwrap(), expected);
}

/// The administrator's permit and the writer's ingest permit under
/// `policy()`.
pub(super) fn permits() -> (AccessPermit, AccessPermit) {
    let authority: Arc<dyn Authorizer> = Arc::new(PolicyAuthority::new(policy()).unwrap());
    let admin = AccessPermit::acquire(
        authority.clone(),
        "administrator",
        "books",
        AccessAction::Admin,
    )
    .unwrap();
    let ingest = AccessPermit::acquire(authority, "writer", "books", AccessAction::Ingest).unwrap();
    (admin, ingest)
}

/// Audit the catalog's pinned history and copy it to `{name}.redb` under
/// `root`.
pub(super) fn audited_copy(catalog: &AccessControlledCatalog, root: &Path, name: &str) -> PathBuf {
    let copy = root.join(format!("{name}.redb"));
    let checkpoint = catalog.inner.capture_checkpoint(1 << 20).unwrap();
    checkpoint
        .verify_source_history(&root.join(format!("{name}-audit.redb")), 1 << 20, 64 << 20)
        .unwrap();
    checkpoint.write_to(&copy, &checkpoint_limits()).unwrap();
    copy
}

pub(super) fn two_actor_operations(name: &str) -> (PathBuf, AccessControlledCatalog) {
    let path = temp_path(name);
    let (admin, ingest) = permits();
    let catalog = AccessControlledCatalog::create(&path, &binding(), &admin).unwrap();
    catalog
        .accept(&ingest, &actor_write(b"actor-one", b"source-one", 1))
        .unwrap();
    catalog
        .accept(&ingest, &actor_write(b"actor-two", b"source-two", 2))
        .unwrap();
    (path, catalog)
}

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
    assert_eq!(
        catalog.accept(&ingest, &write()).unwrap().accepted_sequence,
        1
    );
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
    assert_eq!(changes.len().unwrap(), 1);

    drop(changes);
    drop(read);
    drop(catalog);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn committed_exact_retry_does_not_wait_for_an_unrelated_database_writer() {
    use std::sync::mpsc::{self, RecvTimeoutError};
    use std::time::Duration;

    let path = temp_path("read-only-retry");
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
    let catalog = Arc::new(AccessControlledCatalog::create(&path, &binding(), &admin).unwrap());
    let request = write();
    let accepted = catalog.accept(&ingest, &request).unwrap();

    // Hold an unrelated redb writer open. A committed exact retry must resolve
    // from a read transaction instead of waiting to acquire another writer.
    let held_writer = catalog.inner.database.begin_write().unwrap();
    let worker_catalog = catalog.clone();
    let worker_ingest = ingest.clone();
    let worker_request = request.clone();
    let (result_tx, result_rx) = mpsc::channel();
    let result = std::thread::scope(|scope| {
        let worker = scope.spawn(move || {
            result_tx
                .send(worker_catalog.accept(&worker_ingest, &worker_request))
                .unwrap();
        });

        let observed = result_rx.recv_timeout(Duration::from_secs(2));
        // Release the writer before any assertion or join, including the
        // failure path, so a writer-first implementation cannot deadlock.
        drop(held_writer);
        let result = match observed {
            Ok(result) => result,
            Err(RecvTimeoutError::Timeout) => {
                let late = result_rx.recv_timeout(Duration::from_secs(2));
                worker.join().unwrap();
                panic!("exact retry waited for the unrelated writer: {late:?}");
            }
            Err(RecvTimeoutError::Disconnected) => {
                worker.join().unwrap();
                panic!("exact retry worker disconnected before returning");
            }
        };
        worker.join().unwrap();
        result
    });
    let replay = result.unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.accepted_sequence, accepted.accepted_sequence);
    assert_eq!(replay.version, accepted.version);

    // The operation ID still cannot bypass contract-2 history binding. This is
    // checked before returning the otherwise matching committed receipt.
    let mut wrong_history = request;
    wrong_history.contract_version = 2;
    wrong_history.history_id = accepted.history_id.clone();
    wrong_history.history_id[0] ^= 0xff;
    assert!(wrong_history.history_id.iter().any(|byte| *byte != 0));
    let error = catalog.accept(&ingest, &wrong_history).unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);

    drop(catalog);
    std::fs::remove_dir_all(root).unwrap();
}

fn actor_write(operation_id: &[u8], document_key: &[u8], payload: u8) -> AcceptDocumentRequest {
    let mut request = write();
    request.operation_id = operation_id.to_vec();
    request.document_key = document_key.to_vec();
    request.expected_version = Some(0);
    request.mutation = Some(Mutation::Source(ProtobufSource {
        descriptor_set: include_bytes!("../../../tests/fixtures/protobuf-semantics/descriptor.bin")
            .to_vec(),
        message_type: "semantics.Doc".into(),
        payload: vec![8, payload],
    }));
    request
}

fn actor_assignment(
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

fn make_legacy_controlled(path: &Path) {
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

fn checkpoint_limits() -> crate::pb::storage::DocumentCatalogCheckpointLimits {
    crate::pb::storage::DocumentCatalogCheckpointLimits {
        max_file_bytes: 64 << 20,
        batch_bytes: 1 << 20,
    }
}

fn raw_binary_rows_from(
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

fn raw_binary_rows(path: &Path, table_name: &'static str) -> Vec<(Vec<u8>, Vec<u8>)> {
    let database = redb::Database::open(path).unwrap();
    raw_binary_rows_from(&database, table_name)
}

fn append_unknown_to_legacy_operation(path: &Path, operation_id: &[u8]) -> Vec<u8> {
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

fn assert_replay(
    catalog: &AccessControlledCatalog,
    ingest: &AccessPermit,
    request: &AcceptDocumentRequest,
    original: &crate::pb::DocumentWriteReceipt,
) {
    let mut expected = original.clone();
    expected.replayed = true;
    assert_eq!(catalog.accept(ingest, request).unwrap(), expected);
}

#[test]
fn actor_migration_checkpoint_preserves_both_namespaces_and_audits_each_phase() {
    let path = temp_path("actor-checkpoint");
    let root = path.parent().unwrap().to_path_buf();
    let first = actor_write(b"legacy-one", b"source-one", 1);
    let second = actor_write(b"legacy-two", b"source-two", 2);
    let (first_receipt, second_receipt) = {
        let catalog = DocumentCatalog::create(&path, "books").unwrap();
        (
            catalog.accept(&first).unwrap(),
            catalog.accept(&second).unwrap(),
        )
    };
    let first_raw = append_unknown_to_legacy_operation(&path, &first.operation_id);
    make_legacy_controlled(&path);

    let authority: Arc<dyn Authorizer> = Arc::new(PolicyAuthority::new(policy()).unwrap());
    let admin = AccessPermit::acquire(
        authority.clone(),
        "administrator",
        "books",
        AccessAction::Admin,
    )
    .unwrap();
    let ingest = AccessPermit::acquire(authority, "writer", "books", AccessAction::Ingest).unwrap();
    let catalog = AccessControlledCatalog::open(&path, &binding(), &admin).unwrap();
    catalog
        .assign_legacy_actor(&admin, &actor_assignment(&first_receipt, &first, "writer"))
        .unwrap();
    assert_eq!(
        raw_binary_rows_from(&catalog.inner.database, "actor_operations")[0].1,
        first_raw
    );

    let partial_copy = root.join("partial.redb");
    {
        let checkpoint = catalog.inner.capture_checkpoint(1 << 20).unwrap();
        checkpoint
            .verify_source_history(&root.join("partial-audit.redb"), 1 << 20, 64 << 20)
            .unwrap();
        checkpoint
            .write_to(&partial_copy, &checkpoint_limits())
            .unwrap();
    }
    let partial = AccessControlledCatalog::open(&partial_copy, &binding(), &admin).unwrap();
    for request in [
        &first,
        &actor_write(b"new-during-migration", b"source-three", 3),
    ] {
        let error = partial.accept(&ingest, request).unwrap_err();
        assert_eq!(error.code(), Code::FailedPrecondition);
        assert!(error.message().contains("attribution"), "{error}");
    }
    drop(partial);
    let original_raw = raw_binary_rows_from(&catalog.inner.database, "operations");
    assert_eq!(original_raw, raw_binary_rows(&partial_copy, "operations"));
    let original_actor_raw = raw_binary_rows_from(&catalog.inner.database, "actor_operations");
    assert_eq!(
        original_actor_raw,
        raw_binary_rows(&partial_copy, "actor_operations")
    );

    catalog
        .assign_legacy_actor(
            &admin,
            &actor_assignment(&second_receipt, &second, "writer"),
        )
        .unwrap();
    let third = actor_write(b"actor-three", b"source-three", 3);
    let third_receipt = catalog.accept(&ingest, &third).unwrap();
    assert_eq!(third_receipt.accepted_sequence, 3);

    let complete_copy = root.join("complete.redb");
    {
        let checkpoint = catalog.inner.capture_checkpoint(1 << 20).unwrap();
        checkpoint
            .verify_source_history(&root.join("complete-audit.redb"), 1 << 20, 64 << 20)
            .unwrap();
        checkpoint
            .write_to(&complete_copy, &checkpoint_limits())
            .unwrap();
    }
    drop(catalog);
    let complete = AccessControlledCatalog::open(&complete_copy, &binding(), &admin).unwrap();
    assert_replay(&complete, &ingest, &first, &first_receipt);
    assert_replay(&complete, &ingest, &second, &second_receipt);
    assert_replay(&complete, &ingest, &third, &third_receipt);
    drop(complete);
    assert_eq!(
        raw_binary_rows(&path, "operations"),
        raw_binary_rows(&complete_copy, "operations")
    );
    assert_eq!(
        raw_binary_rows(&path, "actor_operations"),
        raw_binary_rows(&complete_copy, "actor_operations")
    );

    std::fs::remove_dir_all(root).unwrap();
}

fn two_actor_operations(name: &str) -> (PathBuf, AccessControlledCatalog) {
    let path = temp_path(name);
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
    catalog
        .accept(&ingest, &actor_write(b"actor-one", b"source-one", 1))
        .unwrap();
    catalog
        .accept(&ingest, &actor_write(b"actor-two", b"source-two", 2))
        .unwrap();
    (path, catalog)
}

#[test]
fn source_history_audit_refuses_a_noncanonical_actor_operation_key() {
    let (path, catalog) = two_actor_operations("actor-key-canonical");
    {
        let tx = catalog.inner.database.begin_write().unwrap();
        {
            let mut operations = tx.open_table(actors::OPERATIONS).unwrap();
            let (mut noncanonical, value) = {
                let (key, value) = operations.iter().unwrap().next().unwrap().unwrap();
                (key.value().to_vec(), value.value().to_vec())
            };
            operations.remove(noncanonical.as_slice()).unwrap();
            // Unknown field 99, varint 1: same decoded ActorOperationKey meaning,
            // but not its unique canonical encoding.
            noncanonical.extend_from_slice(&[0x98, 0x06, 0x01]);
            operations
                .insert(noncanonical.as_slice(), value.as_slice())
                .unwrap();
        }
        tx.commit().unwrap();
    }

    let checkpoint = catalog.inner.capture_checkpoint(1 << 20).unwrap();
    let error = checkpoint
        .verify_source_history(
            &path.parent().unwrap().join("noncanonical-audit.redb"),
            1 << 20,
            64 << 20,
        )
        .unwrap_err();
    assert_eq!(error.code(), Code::DataLoss);
    assert!(error.message().contains("actor operation key"), "{error}");
    drop(checkpoint);
    drop(catalog);
    std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
}

#[test]
fn source_history_audit_refuses_two_actor_keys_claiming_one_sequence() {
    let (path, catalog) = two_actor_operations("actor-sequence-bijection");
    {
        let tx = catalog.inner.database.begin_write().unwrap();
        {
            let mut operations = tx.open_table(actors::OPERATIONS).unwrap();
            let rows: Vec<_> = operations
                .iter()
                .unwrap()
                .map(|entry| {
                    let (key, value) = entry.unwrap();
                    (key.value().to_vec(), value.value().to_vec())
                })
                .collect();
            assert_eq!(rows.len(), 2);
            operations
                .insert(rows[1].0.as_slice(), rows[0].1.as_slice())
                .unwrap();
        }
        tx.commit().unwrap();
    }

    let checkpoint = catalog.inner.capture_checkpoint(1 << 20).unwrap();
    let error = checkpoint
        .verify_source_history(
            &path.parent().unwrap().join("duplicate-sequence-audit.redb"),
            1 << 20,
            64 << 20,
        )
        .unwrap_err();
    assert_eq!(error.code(), Code::DataLoss);
    assert!(
        error
            .message()
            .contains("multiple operations claim one accepted sequence"),
        "{error}"
    );
    drop(checkpoint);
    drop(catalog);
    std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
}

#[test]
fn actor_namespace_refuses_new_acceptance_during_incomplete_attribution() {
    let (path, catalog) = two_actor_operations("actor-migration-watermark");
    let tx = catalog.inner.database.begin_write().unwrap();
    {
        let mut actor = tx.open_table(actors::OPERATIONS).unwrap();
        let (key, value) = {
            let (key, value) = actor.iter().unwrap().next().unwrap().unwrap();
            (key.value().to_vec(), value.value().to_vec())
        };
        let decoded: crate::pb::storage::ActorOperationKey = decode(&key).unwrap();
        let operation: DocumentOperation = decode(&value).unwrap();
        assert_eq!(operation.receipt.unwrap().accepted_sequence, 1);
        actor.remove(key.as_slice()).unwrap();
        tx.open_table(OPERATIONS)
            .unwrap()
            .insert(decoded.operation_id.as_slice(), value.as_slice())
            .unwrap();
        let mut meta = tx.open_table(META).unwrap();
        let mut header: DocumentCatalogHeader =
            decode(meta.get("header").unwrap().unwrap().value()).unwrap();
        // Table counts still agree: one unassigned legacy record, one actor
        // record, two accepted versions. The impossible state is accepting
        // sequence two before attribution of sequence one finished.
        header.actor_namespace = Some(actors::namespace(1));
        meta.insert("header", header.encode_to_vec().as_slice())
            .unwrap();
    }
    tx.commit().unwrap();
    let error = catalog
        .inner
        .capture_checkpoint(1 << 20)
        .err()
        .expect("impossible migration must refuse");
    assert_eq!(error.code(), Code::DataLoss);
    assert!(error.message().contains("actor namespace"), "{error}");
    drop(catalog);
    std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
}

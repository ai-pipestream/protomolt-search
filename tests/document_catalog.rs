use std::path::PathBuf;
use std::sync::{Arc, Barrier};

use pipestream_search::document_catalog::DocumentCatalog;
use pipestream_search::embedded::{
    EmbeddedDocumentCatalogConfig, EmbeddedSearch, EmbeddedSearchConfig, EmbeddedShardConfig,
};
use pipestream_search::pb::storage::SourceSealRequest;
use pipestream_search::pb::{
    accept_document_request::Mutation, AcceptDocumentRequest, ProtobufSource,
};
use pipestream_search::pb::{accepted_document_version, ReadAcceptedDocumentsRequest};
use prost::Message;
use redb::ReadableTable;
use tonic::Code;

fn page(after_sequence: u64) -> ReadAcceptedDocumentsRequest {
    ReadAcceptedDocumentsRequest {
        after_sequence,
        limit: 1000,
        through_sequence: None,
        max_bytes: 1024 * 1024,
        ..Default::default()
    }
}

struct Directory(PathBuf);
impl Directory {
    fn new(name: &str) -> Self {
        let path = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
            "catalog-{name}-{}-{}",
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
        self.0.join("documents.redb")
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

fn source() -> ProtobufSource {
    ProtobufSource {
        descriptor_set: include_bytes!("fixtures/protobuf-semantics/descriptor.bin").to_vec(),
        message_type: "semantics.Doc".into(),
        // Noncanonical varint and an unknown field must remain exact.
        payload: vec![8, 0x81, 0, 0xa0, 6, 99],
    }
}
fn write(operation: &[u8], expected: Option<u64>) -> AcceptDocumentRequest {
    AcceptDocumentRequest {
        contract_version: 1,
        document_key: b"source\0stable-key".to_vec(),
        operation_id: operation.to_vec(),
        expected_version: expected,
        mutation: Some(Mutation::Source(source())),
        ..Default::default()
    }
}

#[test]
fn retry_and_source_history_survive_replacement_delete_and_restart() {
    let dir = Directory::new("history");
    let first = write(b"create", Some(0));
    let original_receipt;
    {
        let catalog = DocumentCatalog::create(&dir.catalog(), "books").unwrap();
        original_receipt = catalog.accept(&first).unwrap();
        assert!(original_receipt.accepted && original_receipt.durable);
        assert!(!original_receipt.searchable && !original_receipt.replayed);
        assert_eq!(
            (original_receipt.version, original_receipt.accepted_sequence),
            (1, 1)
        );
        let mut replacement = write(b"replace", Some(1));
        if let Some(Mutation::Source(source)) = &mut replacement.mutation {
            source.payload.clear();
        }
        assert_eq!(catalog.accept(&replacement).unwrap().version, 2);
        let mut delete = write(b"delete", Some(2));
        delete.mutation = Some(Mutation::Delete(true));
        assert_eq!(catalog.accept(&delete).unwrap().version, 3);
    }
    let catalog = DocumentCatalog::open(&dir.catalog(), "books").unwrap();
    let mut replay = original_receipt;
    replay.replayed = true;
    assert_eq!(catalog.accept(&first).unwrap(), replay);
    assert_eq!(
        catalog
            .get(&first.document_key, Some(1))
            .unwrap()
            .unwrap()
            .1,
        Some(source())
    );
    assert!(catalog
        .get(&first.document_key, Some(2))
        .unwrap()
        .unwrap()
        .1
        .unwrap()
        .payload
        .is_empty());
    let (head, source) = catalog.get(&first.document_key, None).unwrap().unwrap();
    assert!(head.deleted && source.is_none());
    assert_eq!(head.version, 3);
    assert_eq!(
        catalog
            .accept(&write(b"stale", Some(0)))
            .unwrap_err()
            .code(),
        Code::Aborted
    );
    assert_eq!(
        catalog.accept(&write(b"stale", Some(3))).unwrap().version,
        4
    );
    let mut reused = first;
    reused.document_key.push(1);
    assert_eq!(
        catalog.accept(&reused).unwrap_err().code(),
        Code::AlreadyExists
    );
}

#[test]
fn concurrent_compare_and_set_has_one_winner_and_retries_converge() {
    let catalog = Arc::new(DocumentCatalog::in_memory("books").unwrap());
    let barrier = Arc::new(Barrier::new(8));
    let threads: Vec<_> = (0u8..8)
        .map(|i| {
            let catalog = catalog.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                (i, catalog.accept(&write(&[i], Some(0))))
            })
        })
        .collect();
    let results: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
    assert_eq!(results.iter().filter(|(_, r)| r.is_ok()).count(), 1);
    for (_, result) in &results {
        if let Err(error) = result {
            assert_eq!(error.code(), Code::Aborted);
        }
    }
    let (winner, receipt) = results.into_iter().find(|(_, r)| r.is_ok()).unwrap();
    assert!(!receipt.unwrap().durable);
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let catalog = catalog.clone();
            std::thread::spawn(move || catalog.accept(&write(&[winner], Some(0))).unwrap())
        })
        .collect();
    for thread in threads {
        let receipt = thread.join().unwrap();
        assert!(receipt.replayed);
        assert_eq!((receipt.version, receipt.accepted_sequence), (1, 1));
    }
}

#[test]
fn file_authority_is_exclusive_and_collection_binding_is_persistent() {
    let dir = Directory::new("lock");
    let catalog = DocumentCatalog::create(&dir.catalog(), "books").unwrap();
    assert_eq!(
        DocumentCatalog::open(&dir.catalog(), "books")
            .err()
            .unwrap()
            .code(),
        Code::FailedPrecondition
    );
    assert_eq!(
        DocumentCatalog::create(&dir.catalog(), "books")
            .err()
            .unwrap()
            .code(),
        Code::AlreadyExists
    );
    catalog.accept(&write(b"one", None)).unwrap();
    drop(catalog);
    assert_eq!(
        DocumentCatalog::open(&dir.catalog(), "other")
            .err()
            .unwrap()
            .code(),
        Code::FailedPrecondition
    );
    let catalog = DocumentCatalog::open(&dir.catalog(), "books").unwrap();
    assert_eq!(catalog.accept(&write(b"two", Some(1))).unwrap().version, 2);
}

#[test]
fn empty_or_incomplete_existing_catalog_cannot_reset_versions() {
    let dir = Directory::new("corruption");
    std::fs::write(dir.catalog(), []).unwrap();
    assert_eq!(
        DocumentCatalog::open(&dir.catalog(), "books")
            .err()
            .unwrap()
            .code(),
        Code::DataLoss
    );
    std::fs::remove_file(dir.catalog()).unwrap();
    {
        let catalog = DocumentCatalog::create(&dir.catalog(), "books").unwrap();
        catalog.accept(&write(b"first", Some(0))).unwrap();
    }
    {
        let database = redb::Database::open(dir.catalog()).unwrap();
        let transaction = database.begin_write().unwrap();
        transaction
            .delete_table(redb::TableDefinition::<&[u8], &[u8]>::new("operations"))
            .unwrap();
        transaction.commit().unwrap();
    }
    assert!(DocumentCatalog::open(&dir.catalog(), "books").is_err());
}

#[test]
fn committed_receipt_survives_process_exit_without_dropping_database() {
    let dir = Directory::new("crash");
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "abrupt_exit_worker", "--nocapture"])
        .env("PSEARCH_CATALOG_CRASH_PATH", dir.catalog())
        .env_remove("PSEARCH_CATALOG_CRASH_SEAL")
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(73));
    let catalog = DocumentCatalog::open(&dir.catalog(), "books").unwrap();
    let receipt = catalog.accept(&write(b"crash-retry", Some(0))).unwrap();
    assert!(receipt.replayed && receipt.durable);
    assert_eq!(receipt.version, 1);
    assert_eq!(
        catalog.get(&receipt.document_key, None).unwrap().unwrap().1,
        Some(source())
    );
}

#[test]
fn committed_seal_survives_process_exit_without_dropping_database() {
    let dir = Directory::new("seal-crash");
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "abrupt_exit_worker", "--nocapture"])
        .env("PSEARCH_CATALOG_CRASH_PATH", dir.catalog())
        .env("PSEARCH_CATALOG_CRASH_SEAL", "1")
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(73));
    let catalog = DocumentCatalog::open(&dir.catalog(), "books").unwrap();
    let seal = catalog
        .history_seal()
        .unwrap()
        .expect("seal committed before abrupt exit");
    assert_eq!((seal.format_version, seal.accepted_sequence), (1, 1));
    assert_eq!(seal.operation_id, b"crash-seal");
    assert_eq!(
        catalog
            .seal_history(&SourceSealRequest {
                history_id: seal.history_id.clone(),
                expected_accepted_sequence: 1,
                operation_id: b"crash-seal".to_vec(),
            })
            .unwrap(),
        seal
    );
    for request in [
        write(b"crash-retry", Some(0)),
        write(b"new-after-crash-seal", Some(1)),
    ] {
        let error = catalog.accept(&request).unwrap_err();
        assert_eq!(error.code(), Code::FailedPrecondition);
        assert!(error.message().contains("sealed"), "{error}");
    }
}

#[test]
fn abrupt_exit_worker() {
    let Some(path) = std::env::var_os("PSEARCH_CATALOG_CRASH_PATH") else {
        return;
    };
    let catalog = DocumentCatalog::create(&PathBuf::from(path), "books").unwrap();
    assert!(
        catalog
            .accept(&write(b"crash-retry", Some(0)))
            .unwrap()
            .durable
    );
    if std::env::var_os("PSEARCH_CATALOG_CRASH_SEAL").is_some() {
        let receipt = catalog.accept(&write(b"crash-retry", Some(0))).unwrap();
        let seal = catalog
            .seal_history(&SourceSealRequest {
                history_id: receipt.history_id,
                expected_accepted_sequence: receipt.accepted_sequence,
                operation_id: b"crash-seal".to_vec(),
            })
            .unwrap();
        assert_eq!(seal.accepted_sequence, 1);
    }
    // Deliberately skip database Drop and the test harness cleanup.
    std::process::exit(73);
}

fn embedded_config(path: PathBuf, shards: usize) -> EmbeddedSearchConfig {
    let mut config = EmbeddedSearchConfig::new(
        (0..shards)
            .map(|i| {
                let mut shard = EmbeddedShardConfig::in_memory(i as u64 * 1_000_000);
                shard.node.collection = "books".into();
                shard
            })
            .collect(),
    );
    config.document_catalog = Some(EmbeddedDocumentCatalogConfig {
        collection: "books".into(),
        path: Some(path),
    });
    config
}

#[tokio::test]
async fn embedded_sources_need_no_rows_and_authority_survives_shard_layout_change() {
    let dir = Directory::new("embedded");
    let config = embedded_config(dir.catalog(), 2);
    let first = write(b"empty-parent", Some(0));
    let receipt;
    {
        let search = EmbeddedSearch::create(config.clone()).await.unwrap();
        assert!(!search.allows_network());
        receipt = search.accept_document(&first).unwrap();
        assert!(receipt.durable && !receipt.searchable);
        // No calibration, vectors, postings, or mapped binding were needed.
        assert!(search.flush_all().await.unwrap().iter().all(|r| !r.written));
    }
    assert!(EmbeddedSearch::create(config).await.is_err());
    let search = EmbeddedSearch::open(embedded_config(dir.catalog(), 3))
        .await
        .unwrap();
    let mut replay = receipt;
    replay.replayed = true;
    assert_eq!(search.accept_document(&first).unwrap(), replay);
    assert_eq!(
        search
            .accepted_document(&first.document_key, None)
            .unwrap()
            .unwrap()
            .1,
        Some(source())
    );
    let unconfigured = EmbeddedSearch::open(EmbeddedSearchConfig::single(
        EmbeddedShardConfig::in_memory(0),
    ))
    .await
    .unwrap();
    assert_eq!(
        unconfigured.accept_document(&first).unwrap_err().code(),
        Code::FailedPrecondition
    );
}

#[tokio::test]
async fn catalog_cannot_live_in_a_disposable_shard_directory() {
    let dir = Directory::new("overlap");
    let index = dir.0.join("shard.index");
    for owned in [
        pipestream_search::wal::wal_dir(&index),
        pipestream_search::node::generation_dir(&index),
        pipestream_search::node::segments_root(&index),
        pipestream_search::node::bm25_build_dir(&pipestream_search::node::bm25_sidecar_path(
            &index,
        )),
        pipestream_search::compaction::default_work_dir(&index),
    ] {
        std::fs::create_dir(&owned).unwrap();
        let mut config =
            EmbeddedSearchConfig::single(EmbeddedShardConfig::persistent(index.clone(), 0));
        config.document_catalog = Some(EmbeddedDocumentCatalogConfig {
            collection: String::new(),
            path: Some(owned.join("documents.redb")),
        });
        let error = EmbeddedSearch::open(config).await.err().unwrap();
        assert!(error.to_string().contains("overlaps shard storage"));
        assert!(!owned.join("documents.redb").exists());
    }
}

#[test]
fn accepted_pages_pin_history_and_retain_replaced_sources() {
    let catalog = DocumentCatalog::in_memory("books").unwrap();
    let original = write(b"first", Some(0));
    catalog.accept(&original).unwrap();
    let mut second = write(b"second", Some(1));
    second.mutation = Some(Mutation::Delete(true));
    catalog.accept(&second).unwrap();
    let mut request = page(0);
    request.limit = 1;
    let first = catalog.read_accepted(&request).unwrap();
    assert_eq!(
        (first.through_sequence, first.next_sequence, first.complete),
        (2, 1, false)
    );
    assert_eq!(
        first.documents[0].mutation,
        Some(accepted_document_version::Mutation::Source(source()))
    );
    catalog.accept(&write(b"third", Some(2))).unwrap();
    request.history_id = first.history_id.clone();
    request.after_sequence = first.next_sequence;
    request.through_sequence = Some(first.through_sequence);
    let last = catalog.read_accepted(&request).unwrap();
    assert!(last.complete);
    assert_eq!((last.documents[0].version, last.next_sequence), (2, 2));
    assert_eq!(
        last.documents[0].mutation,
        Some(accepted_document_version::Mutation::Deleted(true))
    );
    let latest = catalog
        .read_accepted(&ReadAcceptedDocumentsRequest {
            history_id: first.history_id,
            ..page(2)
        })
        .unwrap();
    assert_eq!(latest.documents[0].version, 3);
    request.after_sequence = 0;
    request.through_sequence = Some(0);
    assert!(catalog
        .read_accepted(&request)
        .unwrap()
        .documents
        .is_empty());
    request.after_sequence = 1;
    assert_eq!(
        catalog.read_accepted(&request).unwrap_err().code(),
        Code::InvalidArgument
    );
}

#[test]
fn history_byte_budget_never_advances_over_an_unreturned_source() {
    let catalog = DocumentCatalog::in_memory("books").unwrap();
    catalog.accept(&write(b"first", Some(0))).unwrap();
    catalog.accept(&write(b"second", Some(1))).unwrap();
    let whole = catalog.read_accepted(&page(0)).unwrap();
    let mut request = page(0);
    request.max_bytes = whole.documents[0].encoded_len() as u64;
    let first = catalog.read_accepted(&request).unwrap();
    assert_eq!(
        (first.documents.len(), first.next_sequence, first.complete),
        (1, 1, false)
    );
    request.max_bytes -= 1;
    assert_eq!(
        catalog.read_accepted(&request).unwrap_err().code(),
        Code::ResourceExhausted
    );
    assert_eq!(catalog.read_accepted(&page(0)).unwrap(), whole);
}

fn downgrade_history_for_migration(path: &std::path::Path, format: u32, break_sequence: bool) {
    use pipestream_search::pb::storage::DocumentCatalogHeader;
    let database = redb::Database::open(path).unwrap();
    let transaction = database.begin_write().unwrap();
    if format == 1 {
        transaction
            .delete_table(redb::TableDefinition::<u64, &[u8]>::new("changes"))
            .unwrap();
    }
    {
        let mut meta = transaction
            .open_table(redb::TableDefinition::<&str, &[u8]>::new("metadata"))
            .unwrap();
        let mut header =
            DocumentCatalogHeader::decode(meta.get("header").unwrap().unwrap().value()).unwrap();
        header.format_version = format;
        header.history_id.clear();
        header.legacy_receipts_through_sequence = 0;
        if break_sequence {
            header.accepted_sequence += 1;
        }
        meta.insert("header", header.encode_to_vec().as_slice())
            .unwrap();
    }
    {
        use pipestream_search::pb::storage::DocumentOperation;
        let mut operations = transaction
            .open_table(redb::TableDefinition::<&[u8], &[u8]>::new("operations"))
            .unwrap();
        let rows: Vec<_> = operations
            .iter()
            .unwrap()
            .map(|row| {
                let (key, value) = row.unwrap();
                let mut operation = DocumentOperation::decode(value.value()).unwrap();
                operation.receipt.as_mut().unwrap().history_id.clear();
                (key.value().to_vec(), operation.encode_to_vec())
            })
            .collect();
        for (key, value) in rows {
            operations.insert(key.as_slice(), value.as_slice()).unwrap();
        }
    }
    transaction.commit().unwrap();
}

#[test]
fn legacy_history_upgrades_atomically_and_keeps_retry_receipts() {
    let dir = Directory::new("upgrade");
    let first = write(b"first", Some(0));
    let before;
    {
        let catalog = DocumentCatalog::create(&dir.catalog(), "books").unwrap();
        catalog.accept(&first).unwrap();
        catalog.accept(&write(b"second", Some(1))).unwrap();
        before = catalog.read_accepted(&page(0)).unwrap();
    }
    downgrade_history_for_migration(&dir.catalog(), 1, false);
    {
        let catalog = DocumentCatalog::open(&dir.catalog(), "books").unwrap();
        let after = catalog.read_accepted(&page(0)).unwrap();
        assert_eq!(after.documents, before.documents);
        assert_eq!(after.through_sequence, before.through_sequence);
        assert_eq!(after.history_id.len(), 16);
        assert_eq!(catalog.accept(&first).unwrap().history_id, after.history_id);
        assert!(catalog.accept(&first).unwrap().replayed);
        assert_eq!(
            catalog
                .accept(&write(b"third", Some(2)))
                .unwrap()
                .accepted_sequence,
            3
        );
    }
    downgrade_history_for_migration(&dir.catalog(), 1, true);
    assert_eq!(
        DocumentCatalog::open(&dir.catalog(), "books")
            .err()
            .unwrap()
            .code(),
        Code::DataLoss
    );
    // Failed migration must not commit the new format or a partial change index.
    use redb::ReadableDatabase;
    let database = redb::Database::open(dir.catalog()).unwrap();
    let transaction = database.begin_read().unwrap();
    let meta = transaction
        .open_table(redb::TableDefinition::<&str, &[u8]>::new("metadata"))
        .unwrap();
    let header = pipestream_search::pb::storage::DocumentCatalogHeader::decode(
        meta.get("header").unwrap().unwrap().value(),
    )
    .unwrap();
    assert_eq!(header.format_version, 1);
    assert!(transaction
        .open_table(redb::TableDefinition::<u64, &[u8]>::new("changes"))
        .is_err());
}

#[test]
fn missing_catalog_on_reopen_cannot_reset_versions_or_retry_history() {
    let dir = Directory::new("missing-authority");
    let saved = dir.0.join("saved.redb");
    let request = write(b"accepted-before-move", Some(0));
    let receipt;
    {
        let catalog = DocumentCatalog::create(&dir.catalog(), "books").unwrap();
        receipt = catalog.accept(&request).unwrap();
    }
    std::fs::rename(dir.catalog(), &saved).unwrap();
    assert_eq!(
        DocumentCatalog::open(&dir.catalog(), "books")
            .err()
            .expect("reopen must not create a new authority")
            .code(),
        Code::NotFound
    );
    assert!(!dir.catalog().exists());
    std::fs::rename(&saved, dir.catalog()).unwrap();
    let catalog = DocumentCatalog::open(&dir.catalog(), "books").unwrap();
    assert_eq!(
        catalog.accept(&request).unwrap(),
        pipestream_search::pb::DocumentWriteReceipt {
            replayed: true,
            ..receipt
        }
    );
}

#[tokio::test]
async fn embedded_reopen_refuses_missing_authority_before_opening_shards() {
    let dir = Directory::new("missing-embedded-authority");
    let mut config = embedded_config(dir.catalog(), 1);
    config.shards[0] = EmbeddedShardConfig::persistent(dir.0.join("index"), 0);
    config.shards[0].node.collection = "books".into();
    let error = EmbeddedSearch::open(config.clone())
        .await
        .err()
        .expect("reopen must require its configured durable catalog");
    assert!(error.to_string().contains("document catalog"));
    assert!(!dir.catalog().exists());
    assert_eq!(
        std::fs::read_dir(&dir.0).unwrap().count(),
        0,
        "failed recovery must not initialize shard storage"
    );
    let search = EmbeddedSearch::create(config).await.unwrap();
    assert!(
        search
            .accept_document(&write(b"explicit-create", Some(0)))
            .unwrap()
            .durable
    );
}

#[test]
fn history_identity_survives_reopen_and_rejects_another_catalog() {
    let dir = Directory::new("history-identity");
    let request;
    let id;
    {
        let catalog = DocumentCatalog::create(&dir.catalog(), "books").unwrap();
        let mut probe = page(0);
        probe.through_sequence = Some(0);
        let first = catalog.read_accepted(&probe).unwrap();
        assert_eq!(first.history_id.len(), 16);
        assert!(first.history_id.iter().any(|b| *b != 0));
        assert!(first.documents.is_empty());
        id = first.history_id;
        let mut pinned = write(b"pinned", Some(0));
        pinned.contract_version = 2;
        pinned.history_id = id.clone();
        assert_eq!(catalog.accept(&pinned).unwrap().history_id, id);
        request = pinned;
    }
    let catalog = DocumentCatalog::open(&dir.catalog(), "books").unwrap();
    let retry = catalog.accept(&request).unwrap();
    assert!(retry.replayed);
    assert_eq!(retry.history_id, id);
    let other = DocumentCatalog::in_memory("books").unwrap();
    other.accept(&write(b"other", Some(0))).unwrap();
    assert_ne!(other.read_accepted(&page(0)).unwrap().history_id, id);
    assert_eq!(
        other.accept(&request).unwrap_err().code(),
        Code::FailedPrecondition
    );
    let mut cursor = page(1);
    cursor.history_id = id;
    assert!(catalog.read_accepted(&cursor).unwrap().complete);
    assert_eq!(
        other.read_accepted(&cursor).unwrap_err().code(),
        Code::FailedPrecondition
    );
}

#[test]
fn history_identity_is_required_before_resuming_an_existing_cursor() {
    let catalog = DocumentCatalog::in_memory("books").unwrap();
    catalog.accept(&write(b"first", Some(0))).unwrap();
    catalog.accept(&write(b"second", Some(1))).unwrap();
    assert_eq!(
        catalog.read_accepted(&page(1)).unwrap_err().code(),
        Code::InvalidArgument
    );
    let mut request = page(0);
    request.through_sequence = Some(2);
    assert_eq!(
        catalog.read_accepted(&request).unwrap_err().code(),
        Code::InvalidArgument
    );
}

fn change_catalog_header(
    path: &std::path::Path,
    change: impl FnOnce(&mut pipestream_search::pb::storage::DocumentCatalogHeader),
) {
    let database = redb::Database::open(path).unwrap();
    let transaction = database.begin_write().unwrap();
    {
        let mut meta = transaction
            .open_table(redb::TableDefinition::<&str, &[u8]>::new("metadata"))
            .unwrap();
        let mut header = pipestream_search::pb::storage::DocumentCatalogHeader::decode(
            meta.get("header").unwrap().unwrap().value(),
        )
        .unwrap();
        change(&mut header);
        meta.insert("header", header.encode_to_vec().as_slice())
            .unwrap();
    }
    transaction.commit().unwrap();
}

fn stored_operation(path: &std::path::Path, key: &[u8]) -> Vec<u8> {
    use redb::ReadableDatabase;
    let database = redb::Database::open(path).unwrap();
    let transaction = database.begin_read().unwrap();
    let operations = transaction
        .open_table(redb::TableDefinition::<&[u8], &[u8]>::new("operations"))
        .unwrap();
    operations.get(key).unwrap().unwrap().value().to_vec()
}

#[test]
fn format_two_migration_pins_identity_without_rewriting_retry_history() {
    let dir = Directory::new("format-two-identity");
    let request = write(b"legacy", Some(0));
    {
        let catalog = DocumentCatalog::create(&dir.catalog(), "books").unwrap();
        catalog.accept(&request).unwrap();
    }
    downgrade_history_for_migration(&dir.catalog(), 2, false);
    let stored = stored_operation(&dir.catalog(), &request.operation_id);
    let id;
    {
        let catalog = DocumentCatalog::open(&dir.catalog(), "books").unwrap();
        let receipt = catalog.accept(&request).unwrap();
        assert!(receipt.replayed && receipt.durable && !receipt.searchable);
        assert_eq!((receipt.version, receipt.accepted_sequence), (1, 1));
        id = receipt.history_id;
        assert_eq!(id.len(), 16);
        assert_eq!(catalog.read_accepted(&page(0)).unwrap().history_id, id);
        assert_eq!(
            catalog.accept(&write(b"new", Some(1))).unwrap().history_id,
            id
        );
    }
    assert_eq!(
        stored_operation(&dir.catalog(), &request.operation_id),
        stored
    );
    let catalog = DocumentCatalog::open(&dir.catalog(), "books").unwrap();
    assert_eq!(catalog.accept(&request).unwrap().history_id, id);
}

#[test]
fn malformed_history_identity_is_never_recreated_on_open() {
    for id in [vec![], vec![1; 15], vec![0; 16]] {
        let dir = Directory::new("damaged-history-id");
        drop(DocumentCatalog::create(&dir.catalog(), "books").unwrap());
        change_catalog_header(&dir.catalog(), |header| header.history_id = id.clone());
        for _ in 0..2 {
            assert_eq!(
                DocumentCatalog::open(&dir.catalog(), "books")
                    .err()
                    .unwrap()
                    .code(),
                Code::DataLoss
            );
        }
        change_catalog_header(&dir.catalog(), |header| assert_eq!(header.history_id, id));
    }
    let dir = Directory::new("damaged-migration-boundary");
    drop(DocumentCatalog::create(&dir.catalog(), "books").unwrap());
    change_catalog_header(&dir.catalog(), |header| {
        header.legacy_receipts_through_sequence = 1
    });
    assert_eq!(
        DocumentCatalog::open(&dir.catalog(), "books")
            .err()
            .unwrap()
            .code(),
        Code::DataLoss
    );
}

#[test]
fn pinned_write_refusals_consume_neither_sequence_nor_operation_id() {
    let catalog = DocumentCatalog::in_memory("books").unwrap();
    let id = catalog.read_accepted(&page(0)).unwrap().history_id;
    let mut request = write(b"pin", Some(0));
    request.contract_version = 2;
    for bad in [vec![], vec![1; 15], vec![0; 16]] {
        request.history_id = bad;
        assert_eq!(
            catalog.accept(&request).unwrap_err().code(),
            Code::InvalidArgument
        );
    }
    request.history_id = id.clone();
    request.history_id[0] ^= 1;
    assert_eq!(
        catalog.accept(&request).unwrap_err().code(),
        Code::FailedPrecondition
    );
    request.history_id = id;
    request.contract_version = 1;
    assert_eq!(
        catalog.accept(&request).unwrap_err().code(),
        Code::InvalidArgument
    );
    request.contract_version = 2;
    assert_eq!(catalog.accept(&request).unwrap().accepted_sequence, 1);
    assert!(catalog.accept(&request).unwrap().replayed);
    let mut bad_cursor = page(1);
    bad_cursor.history_id = vec![0; 16];
    assert_eq!(
        catalog.read_accepted(&bad_cursor).unwrap_err().code(),
        Code::InvalidArgument
    );
}

#[test]
fn new_receipts_cannot_use_the_legacy_missing_identity_exception() {
    let dir = Directory::new("receipt-id-corruption");
    let request = write(b"new", Some(0));
    {
        let catalog = DocumentCatalog::create(&dir.catalog(), "books").unwrap();
        catalog.accept(&request).unwrap();
    }
    {
        let database = redb::Database::open(dir.catalog()).unwrap();
        let transaction = database.begin_write().unwrap();
        {
            let mut operations = transaction
                .open_table(redb::TableDefinition::<&[u8], &[u8]>::new("operations"))
                .unwrap();
            let mut operation = pipestream_search::pb::storage::DocumentOperation::decode(
                operations
                    .get(request.operation_id.as_slice())
                    .unwrap()
                    .unwrap()
                    .value(),
            )
            .unwrap();
            operation.receipt.as_mut().unwrap().history_id.clear();
            operations
                .insert(
                    request.operation_id.as_slice(),
                    operation.encode_to_vec().as_slice(),
                )
                .unwrap();
        }
        transaction.commit().unwrap();
    }
    let catalog = DocumentCatalog::open(&dir.catalog(), "books").unwrap();
    assert_eq!(catalog.accept(&request).unwrap_err().code(), Code::DataLoss);
}

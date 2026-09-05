use std::path::PathBuf;
use std::sync::{Arc, Barrier};

use pipestream_search::document_catalog::DocumentCatalog;
use pipestream_search::embedded::{
    EmbeddedDocumentCatalogConfig, EmbeddedSearch, EmbeddedSearchConfig, EmbeddedShardConfig,
};
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
    request.after_sequence = first.next_sequence;
    request.through_sequence = Some(first.through_sequence);
    let last = catalog.read_accepted(&request).unwrap();
    assert!(last.complete);
    assert_eq!((last.documents[0].version, last.next_sequence), (2, 2));
    assert_eq!(
        last.documents[0].mutation,
        Some(accepted_document_version::Mutation::Deleted(true))
    );
    let latest = catalog.read_accepted(&page(2)).unwrap();
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

fn downgrade_history_for_migration(path: &std::path::Path, break_sequence: bool) {
    use pipestream_search::pb::storage::DocumentCatalogHeader;
    let database = redb::Database::open(path).unwrap();
    let transaction = database.begin_write().unwrap();
    transaction
        .delete_table(redb::TableDefinition::<u64, &[u8]>::new("changes"))
        .unwrap();
    {
        let mut meta = transaction
            .open_table(redb::TableDefinition::<&str, &[u8]>::new("metadata"))
            .unwrap();
        let mut header =
            DocumentCatalogHeader::decode(meta.get("header").unwrap().unwrap().value()).unwrap();
        header.format_version = 1;
        if break_sequence {
            header.accepted_sequence += 1;
        }
        meta.insert("header", header.encode_to_vec().as_slice())
            .unwrap();
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
    downgrade_history_for_migration(&dir.catalog(), false);
    {
        let catalog = DocumentCatalog::open(&dir.catalog(), "books").unwrap();
        assert_eq!(catalog.read_accepted(&page(0)).unwrap(), before);
        assert!(catalog.accept(&first).unwrap().replayed);
        assert_eq!(
            catalog
                .accept(&write(b"third", Some(2)))
                .unwrap()
                .accepted_sequence,
            3
        );
    }
    downgrade_history_for_migration(&dir.catalog(), true);
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

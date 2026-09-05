use std::path::PathBuf;
use std::sync::{Arc, Barrier};

use pipestream_search::document_catalog::DocumentCatalog;
use pipestream_search::embedded::{
    EmbeddedDocumentCatalogConfig, EmbeddedSearch, EmbeddedSearchConfig, EmbeddedShardConfig,
};
use pipestream_search::pb::{
    accept_document_request::Mutation, AcceptDocumentRequest, ProtobufSource,
};
use tonic::Code;

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
    let wal = pipestream_search::wal::wal_dir(&index);
    std::fs::create_dir(&wal).unwrap();
    let mut config = EmbeddedSearchConfig::single(EmbeddedShardConfig::persistent(index, 0));
    config.document_catalog = Some(EmbeddedDocumentCatalogConfig {
        collection: String::new(),
        path: Some(wal.join("documents.redb")),
    });
    let error = EmbeddedSearch::open(config).await.err().unwrap();
    assert!(error.to_string().contains("overlaps shard storage"));
    assert!(!wal.join("documents.redb").exists());
}

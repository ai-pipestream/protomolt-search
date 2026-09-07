use super::*;
use std::path::PathBuf;
use tonic::Code;

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "catalog-checkpoint-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
fn request(sequence: u64) -> AcceptDocumentRequest {
    AcceptDocumentRequest {
        contract_version: 1,
        document_key: b"key\0one".to_vec(),
        operation_id: sequence.to_be_bytes().to_vec(),
        expected_version: Some(sequence - 1),
        mutation: Some(if sequence % 3 == 0 {
            Mutation::Delete(true)
        } else {
            Mutation::Source(ProtobufSource {
                descriptor_set: include_bytes!(
                    "../../../tests/fixtures/protobuf-semantics/descriptor.bin"
                )
                .to_vec(),
                message_type: "semantics.Doc".into(),
                payload: if sequence % 3 == 2 {
                    Vec::new()
                } else {
                    vec![8, 0x81, 0, 0xa0, 6, 99]
                },
            })
        }),
        ..Default::default()
    }
}
fn limits() -> DocumentCatalogCheckpointLimits {
    DocumentCatalogCheckpointLimits {
        batch_bytes: 32 << 10,
        max_file_bytes: 32 << 20,
    }
}
fn records<K: redb::Key + 'static>(
    tx: &ReadTransaction,
    table: TableDefinition<K, &'static [u8]>,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    tx.open_table(table)
        .unwrap()
        .iter()
        .unwrap()
        .map(|r| {
            let (k, v) = r.unwrap();
            let result = (
                K::as_bytes(&k.value()).as_ref().to_vec(),
                v.value().to_vec(),
            );
            result
        })
        .collect()
}

#[test]
fn pinned_checkpoint_copies_exact_history_while_acceptance_continues() {
    let directory = Directory::new();
    let source = DocumentCatalog::create(&directory.0.join("source"), "books").unwrap();
    let first = source.accept(&request(1)).unwrap();
    for n in 2..=90 {
        source.accept(&request(n)).unwrap();
    }
    // Preserve unknown protobuf bytes in an existing operation record too.
    {
        let tx = source.database.begin_write().unwrap();
        {
            let mut ops = tx.open_table(OPERATIONS).unwrap();
            let (key, value) =
                records(&source.database.begin_read().unwrap(), OPERATIONS).remove(0);
            let mut extended = value;
            extended.extend_from_slice(&[0xa0, 6, 0x81, 0]);
            ops.insert(key.as_slice(), extended.as_slice()).unwrap();
        }
        tx.commit().unwrap();
    }
    let checkpoint = source.capture_checkpoint(1 << 20).unwrap();
    assert_eq!(
        checkpoint
            .metadata()
            .header
            .as_ref()
            .unwrap()
            .accepted_sequence,
        90
    );
    assert!(checkpoint.metadata().indexes.is_empty());
    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                for n in 91..=180 {
                    source.accept(&request(n)).unwrap();
                }
            })
            .join()
            .unwrap();
    });
    let output = directory.0.join("copy");
    checkpoint
        .verify_source_history(&directory.0.join("audit"), 1 << 20, 32 << 20)
        .unwrap();
    assert!(!directory.0.join("audit").exists());
    let info = checkpoint.write_to(&output, &limits()).unwrap();
    assert_eq!(info.header, checkpoint.metadata().header);
    assert_eq!(info.bytes, std::fs::metadata(&output).unwrap().len());
    assert_eq!(
        info.sha256,
        sha256::digest(&std::fs::read(&output).unwrap())
    );
    let restored = DocumentCatalog::open(&output, "books").unwrap();
    {
        let read = restored.database.begin_read().unwrap();
        let mut count = 0;
        for table in &checkpoint.binary_tables {
            let before = records(&checkpoint.read, *table);
            assert_eq!(records(&read, *table), before);
            count += before.len();
        }
        for (a, b) in [
            (records(&checkpoint.read, META), records(&read, META)),
            (records(&checkpoint.read, CHANGES), records(&read, CHANGES)),
        ] {
            count += a.len();
            assert_eq!(a, b);
        }
        assert_eq!(info.records, count as u64);
    }
    assert_eq!(
        restored.get(b"key\0one", None).unwrap().unwrap().0.version,
        90
    );
    assert_eq!(
        source.get(b"key\0one", None).unwrap().unwrap().0.version,
        180
    );
    assert_eq!(
        restored.get(b"key\0one", Some(1)).unwrap().unwrap().1,
        source.get(b"key\0one", Some(1)).unwrap().unwrap().1
    );
    assert!(restored
        .get(b"key\0one", Some(2))
        .unwrap()
        .unwrap()
        .1
        .unwrap()
        .payload
        .is_empty());
    let mut replay = first;
    replay.replayed = true;
    assert_eq!(restored.accept(&request(1)).unwrap(), replay);
    assert_eq!(
        restored
            .capture_checkpoint(1 << 20)
            .unwrap()
            .metadata()
            .header
            .as_ref()
            .unwrap()
            .accepted_sequence,
        90
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&output).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
}

#[test]
fn checkpoint_refuses_bad_budgets_and_preserves_existing_destinations() {
    let directory = Directory::new();
    let source = DocumentCatalog::create(&directory.0.join("source"), "books").unwrap();
    source.accept(&request(1)).unwrap();
    assert_eq!(
        source.capture_checkpoint(0).err().unwrap().code(),
        Code::InvalidArgument
    );
    assert_eq!(
        source.capture_checkpoint(1).err().unwrap().code(),
        Code::ResourceExhausted
    );
    assert_eq!(
        DocumentCatalog::in_memory("books")
            .unwrap()
            .capture_checkpoint(1024)
            .err()
            .unwrap()
            .code(),
        Code::FailedPrecondition
    );
    let checkpoint = source.capture_checkpoint(1 << 20).unwrap();
    let output = directory.0.join("copy");
    for config in [
        DocumentCatalogCheckpointLimits {
            batch_bytes: 0,
            ..limits()
        },
        DocumentCatalogCheckpointLimits {
            batch_bytes: 65 << 20,
            ..limits()
        },
        DocumentCatalogCheckpointLimits {
            max_file_bytes: 0,
            ..limits()
        },
        DocumentCatalogCheckpointLimits {
            batch_bytes: 1,
            ..limits()
        },
        DocumentCatalogCheckpointLimits {
            max_file_bytes: 1,
            ..limits()
        },
    ] {
        assert!(checkpoint.write_to(&output, &config).is_err());
        assert!(!output.exists());
    }
    std::fs::write(&output, b"existing checkpoint").unwrap();
    assert_eq!(
        checkpoint.write_to(&output, &limits()).unwrap_err().code(),
        Code::AlreadyExists
    );
    assert_eq!(std::fs::read(&output).unwrap(), b"existing checkpoint");
    assert_eq!(source.get(b"key\0one", None).unwrap().unwrap().0.version, 1);
}

#[test]
fn unknown_tables_are_refused_instead_of_silently_dropped() {
    let directory = Directory::new();
    let source = DocumentCatalog::create(&directory.0.join("source"), "books").unwrap();
    let tx = source.database.begin_write().unwrap();
    tx.open_table(TableDefinition::<&[u8], &[u8]>::new("future-extension"))
        .unwrap();
    tx.commit().unwrap();
    let error = source.capture_checkpoint(1 << 20).err().unwrap();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("unknown or missing"));
}

#[test]
fn empty_source_tables_survive_the_copy() {
    let directory = Directory::new();
    let source = DocumentCatalog::create(&directory.0.join("source"), "books").unwrap();
    let checkpoint = source.capture_checkpoint(1 << 20).unwrap();
    assert!(checkpoint.metadata().indexes.is_empty());
    let output = directory.0.join("copy");
    checkpoint.write_to(&output, &limits()).unwrap();
    let restored = DocumentCatalog::open(&output, "books").unwrap();
    let read = restored.database.begin_read().unwrap();
    for table in &checkpoint.binary_tables {
        assert_eq!(records(&read, *table), records(&checkpoint.read, *table));
    }
    assert_eq!(
        restored.capture_checkpoint(1 << 20).unwrap().metadata(),
        checkpoint.metadata()
    );
}

#[test]
fn source_history_audit_rejects_corruption_beyond_the_latest_head() {
    for damage in [
        "descriptor",
        "source",
        "source_missing",
        "head",
        "receipt_duplicate",
        "receipt_missing",
        "receipt_flags",
        "receipt_history",
        "sequence",
        "predecessor",
    ] {
        let dir = Directory::new();
        let source = DocumentCatalog::create(&dir.0.join("source"), "books").unwrap();
        for n in 1..=3 {
            source.accept(&request(n)).unwrap();
        }
        let tx = source.database.begin_write().unwrap();
        match damage {
            "descriptor" | "source" | "source_missing" => {
                let definition = if damage == "descriptor" {
                    DESCRIPTORS
                } else {
                    SOURCES
                };
                let (key, mut value) =
                    records(&source.database.begin_read().unwrap(), definition).remove(0);
                let mut table = tx.open_table(definition).unwrap();
                if damage == "source_missing" {
                    table.remove(key.as_slice()).unwrap();
                } else {
                    value[0] ^= 1;
                    table.insert(key.as_slice(), value.as_slice()).unwrap();
                }
            }
            "head" => {
                let key = DocumentVersionKey {
                    document_key: request(1).document_key,
                    version: 1,
                }
                .encode_to_vec();
                let versions = tx.open_table(VERSIONS).unwrap();
                let old = versions.get(key.as_slice()).unwrap().unwrap();
                tx.open_table(HEADS)
                    .unwrap()
                    .insert(request(1).document_key.as_slice(), old.value())
                    .unwrap();
            }
            "receipt_duplicate" | "receipt_missing" | "receipt_flags" | "receipt_history" => {
                let key = 3u64.to_be_bytes();
                let mut table = tx.open_table(OPERATIONS).unwrap();
                if damage == "receipt_missing" {
                    table.remove(key.as_slice()).unwrap();
                } else {
                    let bytes = table.get(key.as_slice()).unwrap().unwrap().value().to_vec();
                    let mut operation: DocumentOperation = decode(&bytes).unwrap();
                    match damage {
                        "receipt_duplicate" => {
                            let first: DocumentOperation = decode(
                                table
                                    .get(1u64.to_be_bytes().as_slice())
                                    .unwrap()
                                    .unwrap()
                                    .value(),
                            )
                            .unwrap();
                            operation.receipt = first.receipt;
                        }
                        "receipt_flags" => operation.receipt.as_mut().unwrap().searchable = true,
                        "receipt_history" => operation.receipt.as_mut().unwrap().history_id.clear(),
                        _ => unreachable!(),
                    }
                    table
                        .insert(key.as_slice(), operation.encode_to_vec().as_slice())
                        .unwrap();
                }
            }
            "sequence" => {
                let mut table = tx.open_table(CHANGES).unwrap();
                let key = table.remove(1).unwrap().unwrap().value().to_vec();
                table.insert(4, key.as_slice()).unwrap();
            }
            "predecessor" => {
                let old = DocumentVersionKey {
                    document_key: request(1).document_key,
                    version: 3,
                }
                .encode_to_vec();
                let mut versions = tx.open_table(VERSIONS).unwrap();
                let bytes = versions
                    .remove(old.as_slice())
                    .unwrap()
                    .unwrap()
                    .value()
                    .to_vec();
                let mut version: DocumentVersion = decode(&bytes).unwrap();
                version.version = 4;
                let key = DocumentVersionKey {
                    document_key: version.document_key.clone(),
                    version: 4,
                }
                .encode_to_vec();
                versions
                    .insert(key.as_slice(), version.encode_to_vec().as_slice())
                    .unwrap();
                tx.open_table(CHANGES)
                    .unwrap()
                    .insert(3, key.as_slice())
                    .unwrap();
                tx.open_table(HEADS)
                    .unwrap()
                    .insert(
                        version.document_key.as_slice(),
                        version.encode_to_vec().as_slice(),
                    )
                    .unwrap();
            }
            _ => unreachable!(),
        }
        tx.commit().unwrap();
        let checkpoint = source.capture_checkpoint(1 << 20).unwrap();
        let scratch = dir.0.join("audit");
        let error = checkpoint
            .verify_source_history(&scratch, 1 << 20, 32 << 20)
            .unwrap_err();
        assert_eq!(error.code(), Code::DataLoss, "{damage}: {error}");
        assert!(!scratch.exists(), "{damage}");
    }
}

#[test]
fn source_history_audit_preserves_legacy_retry_bytes_and_checks_budgets() {
    let dir = Directory::new();
    let source = DocumentCatalog::create(&dir.0.join("source"), "books").unwrap();
    source.accept(&request(1)).unwrap();
    let tx = source.database.begin_write().unwrap();
    {
        let mut table = tx.open_table(OPERATIONS).unwrap();
        let bytes = table
            .get(1u64.to_be_bytes().as_slice())
            .unwrap()
            .unwrap()
            .value()
            .to_vec();
        let mut operation: DocumentOperation = decode(&bytes).unwrap();
        operation.receipt.as_mut().unwrap().history_id.clear();
        let mut bytes = operation.encode_to_vec();
        bytes.extend_from_slice(&[0xa0, 6, 0x81, 0]);
        table
            .insert(1u64.to_be_bytes().as_slice(), bytes.as_slice())
            .unwrap();
        let mut meta = tx.open_table(META).unwrap();
        let mut header: DocumentCatalogHeader =
            decode(meta.get("header").unwrap().unwrap().value()).unwrap();
        header.legacy_receipts_through_sequence = 1;
        meta.insert("header", header.encode_to_vec().as_slice())
            .unwrap();
    }
    tx.commit().unwrap();
    let checkpoint = source.capture_checkpoint(1 << 20).unwrap();
    let before = records(&checkpoint.read, OPERATIONS);
    let scratch = dir.0.join("audit");
    checkpoint
        .verify_source_history(&scratch, 1 << 20, 32 << 20)
        .unwrap();
    assert!(!scratch.exists());
    assert_eq!(records(&checkpoint.read, OPERATIONS), before);
    for (record_bytes, scratch_bytes) in [(1, 32 << 20), (1 << 20, 1)] {
        assert_eq!(
            checkpoint
                .verify_source_history(&scratch, record_bytes, scratch_bytes)
                .unwrap_err()
                .code(),
            Code::ResourceExhausted
        );
        assert!(!scratch.exists());
    }
    std::fs::write(&scratch, b"existing scratch belongs to caller").unwrap();
    assert!(checkpoint
        .verify_source_history(&scratch, 1 << 20, 32 << 20)
        .is_err());
    assert_eq!(
        std::fs::read(&scratch).unwrap(),
        b"existing scratch belongs to caller"
    );
}

#[test]
fn source_history_audit_detects_duplicate_receipts_across_scratch_batches() {
    let dir = Directory::new();
    let source = DocumentCatalog::create(&dir.0.join("source"), "books").unwrap();
    for n in 1..=1030 {
        source.accept(&request(n)).unwrap();
    }
    let checkpoint = source.capture_checkpoint(1 << 20).unwrap();
    checkpoint
        .verify_source_history(&dir.0.join("valid"), 1 << 20, 32 << 20)
        .unwrap();
    let tx = source.database.begin_write().unwrap();
    {
        let mut table = tx.open_table(OPERATIONS).unwrap();
        let bytes = table
            .get(1u64.to_be_bytes().as_slice())
            .unwrap()
            .unwrap()
            .value()
            .to_vec();
        table
            .insert(1030u64.to_be_bytes().as_slice(), bytes.as_slice())
            .unwrap();
    }
    tx.commit().unwrap();
    // The held view remains coherent; a new capture sees the damaged receipt.
    checkpoint
        .verify_source_history(&dir.0.join("held"), 1 << 20, 32 << 20)
        .unwrap();
    let fresh = source.capture_checkpoint(1 << 20).unwrap();
    let error = fresh
        .verify_source_history(&dir.0.join("bad"), 1 << 20, 32 << 20)
        .unwrap_err();
    assert!(error.message().contains("multiple operations"), "{error}");
    for name in ["valid", "held", "bad"] {
        assert!(!dir.0.join(name).exists());
    }
}

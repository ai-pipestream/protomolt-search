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

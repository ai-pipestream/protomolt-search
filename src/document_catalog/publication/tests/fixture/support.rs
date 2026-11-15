//! Request builders, budgets and file helpers shared by the publication
//! tests.
use super::*;

/// The history identity of a source catalog.
pub(in super::super) fn history_id(source: &DocumentCatalog) -> Vec<u8> {
    source
        .capture_checkpoint(1 << 20)
        .unwrap()
        .metadata()
        .header
        .as_ref()
        .unwrap()
        .history_id
        .clone()
}

/// Backup and restore budgets wide enough for every bundle these tests
/// write.
pub(in super::super) fn backup_limits() -> SourceBackupLimits {
    SourceBackupLimits {
        metadata_bytes: 4 << 20,
        max_files: 256,
        max_bytes: 64 << 20,
        source_batch_bytes: 64 << 10,
    }
}

/// Checkpoint copy budgets wide enough for these tests' catalogs.
pub(in super::super) fn checkpoint_limits() -> DocumentCatalogCheckpointLimits {
    DocumentCatalogCheckpointLimits {
        batch_bytes: 64 << 10,
        max_file_bytes: 32 << 20,
    }
}

/// The restore request of an owner that takes the bundle's identity from
/// `manifest`.
pub(in super::super) fn restore_request(manifest: &SourceBackupManifest) -> SourceRestoreRequest {
    let header = manifest.source.as_ref().unwrap().header.as_ref().unwrap();
    SourceRestoreRequest {
        expected_manifest_sha256: manifest.manifest_sha256.clone(),
        collection: header.collection.clone(),
        history_id: header.history_id.clone(),
        limits: Some(backup_limits()),
    }
}

/// Flip the lowest bit of a file's first byte in place.
pub(in super::super) fn flip_first_byte(path: &Path) {
    use std::io::{Read, Seek, SeekFrom, Write};
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    let mut byte = [0];
    file.read_exact(&mut byte).unwrap();
    byte[0] ^= 1;
    file.seek(SeekFrom::Start(0)).unwrap();
    file.write_all(&byte).unwrap();
    file.sync_all().unwrap();
}

/// A version-1 delete of a document no write has touched, with `name` as
/// both its key and its operation id.
pub(in super::super) fn fresh_delete(name: &[u8]) -> AcceptDocumentRequest {
    AcceptDocumentRequest {
        contract_version: 1,
        document_key: name.to_vec(),
        operation_id: name.to_vec(),
        expected_version: Some(0),
        mutation: Some(Mutation::Delete(true)),
        ..Default::default()
    }
}

/// The first write of `KEY` exactly as `Fixture::stage` sends it, carrying
/// `source`: a retry of that operation when `source` is what it stored.
pub(in super::super) fn first_write_retry(source: ProtobufSource) -> AcceptDocumentRequest {
    AcceptDocumentRequest {
        contract_version: 1,
        document_key: KEY.to_vec(),
        operation_id: [KEY, &1u64.to_be_bytes()].concat(),
        expected_version: Some(0),
        mutation: Some(Mutation::Source(source)),
        ..Default::default()
    }
}

/// Create a catalog under a directory named for `case`, let `corrupt`
/// rewrite its stored header (given the catalog's history identity), and
/// require that reopening the file refuses with DATA_LOSS.
pub(in super::super) fn assert_reopen_refuses_header(
    prefix: &str,
    case: &str,
    corrupt: impl FnOnce(&mut DocumentCatalogHeader, &[u8]),
) {
    let root = std::env::temp_dir().join(format!("{prefix}-{}-{case}", std::process::id()));
    std::fs::create_dir(&root).unwrap();
    let path = root.join("source.redb");
    let source = DocumentCatalog::create(&path, "books").unwrap();
    let history = history_id(&source);
    drop(source);
    {
        let database = redb::Database::open(&path).unwrap();
        let tx = database.begin_write().unwrap();
        {
            let mut meta = tx.open_table(META).unwrap();
            let mut header: DocumentCatalogHeader =
                decode(meta.get("header").unwrap().unwrap().value()).unwrap();
            corrupt(&mut header, &history);
            meta.insert("header", header.encode_to_vec().as_slice())
                .unwrap();
        }
        tx.commit().unwrap();
    }
    let error = DocumentCatalog::open(&path, "books").err().unwrap();
    assert_eq!(error.code(), tonic::Code::DataLoss, "{case}");
    std::fs::remove_dir_all(root).unwrap();
}

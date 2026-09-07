use super::*;
use crate::{
    pb::storage::{SourceBackupLimits, SourceBackupManifest, SourceRestoreRequest},
    segments::OpenedSegmentSet,
};
use std::path::{Path, PathBuf};
use tonic::Code;

fn limits() -> SourceBackupLimits {
    SourceBackupLimits {
        metadata_bytes: 4 << 20,
        max_files: 256,
        max_bytes: 64 << 20,
        source_batch_bytes: 64 << 10,
    }
}

async fn publish(f: &Fixture, key: &[u8], version: u64, rows: usize) {
    let (_, candidate) = f.stage(key, version, Some(rows)).await;
    let node = f.node.clone();
    let source = f.source.clone();
    tokio::task::spawn_blocking(move || {
        node.publish_document_projection_blocking(&source, INDEX, &candidate)
    })
    .await
    .unwrap()
    .unwrap();
}

async fn backup(f: &Fixture, name: &str) -> (PathBuf, SourceBackupManifest) {
    publish(f, KEY, 1, 3).await;
    publish(f, KEY, 2, 2).await;
    let _ = f.stage(KEY, 3, Some(0)).await;
    let catalog = f.node.document_backup_catalog(&f.source, INDEX).unwrap();
    let path = f.root.join(name);
    let manifest = f
        .source
        .capture_backup(&[(INDEX, &catalog)], &limits())
        .unwrap()
        .write_to(&path)
        .unwrap();
    (path, manifest)
}

fn request(manifest: &SourceBackupManifest) -> SourceRestoreRequest {
    let header = manifest.source.as_ref().unwrap().header.as_ref().unwrap();
    SourceRestoreRequest {
        expected_manifest_sha256: manifest.manifest_sha256.clone(),
        collection: header.collection.clone(),
        history_id: header.history_id.clone(),
        limits: Some(limits()),
    }
}

fn read_manifest(bundle: &Path) -> SourceBackupManifest {
    SourceBackupManifest::decode(
        std::fs::read(bundle.join("source-backup.pb"))
            .unwrap()
            .as_slice(),
    )
    .unwrap()
}

fn write_manifest(bundle: &Path, manifest: &mut SourceBackupManifest) {
    manifest.manifest_sha256.clear();
    manifest.manifest_sha256 = sha256::digest(&manifest.encode_to_vec()).to_vec();
    std::fs::write(bundle.join("source-backup.pb"), manifest.encode_to_vec()).unwrap();
}

fn copy_tree(from: &Path, to: &Path) {
    std::fs::create_dir(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

fn flip_first_byte(path: &Path) {
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
}

#[tokio::test]
async fn restore_stages_an_exact_private_copy_with_backlog_and_physical_identity() {
    let f = Fixture::new();
    let (bundle, manifest) = backup(&f, "restore-source").await;
    let completion_before = std::fs::read(bundle.join("source-backup.pb")).unwrap();
    let source_before = std::fs::read(bundle.join("sources.redb")).unwrap();
    let original_index =
        OpenedSegmentSet::open(bundle.join(&manifest.indexes[0].directory)).unwrap();
    let original_index_manifest = original_index.manifest().clone();
    let destination = f.root.join("verified-restore");

    let verified =
        DocumentCatalog::stage_backup_restore(&bundle, &destination, &request(&manifest)).unwrap();
    assert_eq!(verified.directory(), destination);
    assert_eq!(verified.manifest(), &manifest);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&destination)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        assert_eq!(
            std::fs::metadata(destination.join("sources.redb"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
    let competing_writer = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(destination.join("sources.redb"))
        .unwrap();
    assert!(competing_writer.try_lock().is_err());
    assert_eq!(
        std::fs::read(bundle.join("source-backup.pb")).unwrap(),
        completion_before
    );
    assert_eq!(
        std::fs::read(destination.join("source-backup.pb")).unwrap(),
        completion_before
    );
    assert_eq!(
        std::fs::read(destination.join("sources.redb")).unwrap(),
        source_before
    );
    let checkpoint = verified.manifest().source.as_ref().unwrap();
    assert_eq!(checkpoint.header.as_ref().unwrap().accepted_sequence, 3);
    assert_eq!(checkpoint.indexes[0].committed_sequence, 2);
    let staged_index =
        OpenedSegmentSet::open(destination.join(&verified.manifest().indexes[0].directory))
            .unwrap();
    assert_eq!(staged_index.manifest(), &original_index_manifest);
    assert_eq!(
        staged_index.manifest().source_owner,
        original_index.manifest().source_owner
    );

    drop(original_index);
    std::fs::remove_dir_all(&bundle).unwrap();
    let reopened =
        OpenedSegmentSet::open(destination.join(&verified.manifest().indexes[0].directory))
            .unwrap();
    assert_eq!(reopened.manifest(), &original_index_manifest);
    assert_eq!(
        std::fs::read(destination.join("sources.redb")).unwrap(),
        source_before
    );
    drop(reopened);
    drop(staged_index);
    drop(competing_writer);
    drop(verified);
    assert!(!destination.exists());
}

#[tokio::test]
async fn restore_binds_the_canonical_manifest_identity_and_source_identity() {
    for mismatch in ["digest", "history", "collection"] {
        let f = Fixture::new();
        let (bundle, manifest) = backup(&f, "identity-source").await;
        let destination = f.root.join("identity-destination");
        let mut request = request(&manifest);
        match mismatch {
            "digest" => request.expected_manifest_sha256[0] ^= 1,
            "history" => request.history_id[0] ^= 1,
            "collection" => request.collection.push_str("-other"),
            _ => unreachable!(),
        }
        assert!(DocumentCatalog::stage_backup_restore(&bundle, &destination, &request).is_err());
        assert!(!destination.exists(), "{mismatch}");
    }
}

#[tokio::test]
async fn restore_audits_every_file_and_the_complete_inventory() {
    for damage in [
        "corrupt",
        "truncate",
        "missing",
        "malformed-path",
        "duplicate",
        "traversal",
        "unreferenced-inventory",
    ] {
        let f = Fixture::new();
        let (clean, _) = backup(&f, "inventory-clean").await;
        let bundle = f.root.join(format!("inventory-{damage}"));
        copy_tree(&clean, &bundle);
        let mut manifest = read_manifest(&bundle);
        let artifact = manifest
            .artifacts
            .iter()
            .find(|a| a.file != "sources.redb")
            .unwrap()
            .clone();
        match damage {
            "corrupt" => flip_first_byte(&bundle.join(&artifact.file)),
            "truncate" => {
                let file = std::fs::OpenOptions::new()
                    .write(true)
                    .open(bundle.join(&artifact.file))
                    .unwrap();
                file.set_len(artifact.bytes - 1).unwrap();
            }
            "missing" => std::fs::remove_file(bundle.join(&artifact.file)).unwrap(),
            "malformed-path" => manifest.artifacts[0].file = "./sources.redb".into(),
            "duplicate" => manifest.artifacts.push(manifest.artifacts[0].clone()),
            "traversal" => manifest.artifacts[0].file = "../outside".into(),
            "unreferenced-inventory" => {
                std::fs::write(bundle.join("unowned"), b"extra").unwrap();
                manifest.artifacts.push(crate::pb::SnapshotArtifact {
                    file: "unowned".into(),
                    bytes: 5,
                    sha256: sha256::hex_digest(b"extra"),
                });
            }
            _ => unreachable!(),
        }
        if matches!(
            damage,
            "malformed-path" | "duplicate" | "traversal" | "unreferenced-inventory"
        ) {
            write_manifest(&bundle, &mut manifest);
        }
        let request = request(&read_manifest(&bundle));
        let destination = f.root.join("inventory-destination");
        let error = DocumentCatalog::stage_backup_restore(&bundle, &destination, &request)
            .err()
            .unwrap();
        assert!(matches!(
            error.code(),
            Code::DataLoss | Code::FailedPrecondition
        ));
        assert!(!destination.exists(), "{damage}");
    }
}

#[tokio::test]
async fn restore_ignores_unlisted_input_files_and_directories() {
    let f = Fixture::new();
    let (bundle, manifest) = backup(&f, "unlisted-input").await;
    std::fs::write(bundle.join("operator-note"), b"not part of the bundle").unwrap();
    std::fs::create_dir(bundle.join("unlisted-directory")).unwrap();
    std::fs::write(
        bundle.join("unlisted-directory/arbitrary-bytes"),
        [0, 1, 2, 3, 255],
    )
    .unwrap();
    let destination = f.root.join("unlisted-destination");
    let verified =
        DocumentCatalog::stage_backup_restore(&bundle, &destination, &request(&manifest)).unwrap();
    assert!(!destination.join("operator-note").exists());
    assert!(!destination.join("unlisted-directory").exists());
    assert_eq!(verified.manifest(), &manifest);
}

#[test]
fn restore_applies_inventory_budget_before_decoding_nested_completion_metadata() {
    for (case, bytes) in [
        ("one-source-two-indexes", &[0x12, 4, 0x1a, 0, 0x1a, 0][..]),
        (
            "two-source-fields",
            &[0x12, 2, 0x1a, 0, 0x12, 2, 0x1a, 0][..],
        ),
    ] {
        let root = std::env::temp_dir().join(format!(
            "restore-predecode-budget-{}-{case}",
            std::process::id()
        ));
        let bundle = root.join("bundle");
        let destination = root.join("destination");
        std::fs::create_dir_all(&bundle).unwrap();
        std::fs::write(bundle.join("source-backup.pb"), bytes).unwrap();
        let request = SourceRestoreRequest {
            expected_manifest_sha256: vec![1; 32],
            collection: "books".into(),
            history_id: vec![1; 16],
            limits: Some(SourceBackupLimits {
                max_files: 1,
                ..limits()
            }),
        };
        let error = DocumentCatalog::stage_backup_restore(&bundle, &destination, &request)
            .err()
            .unwrap();
        assert_eq!(error.code(), Code::ResourceExhausted, "{case}: {error}");
        assert!(!destination.exists());
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn restore_round_trips_a_source_only_backup() {
    for (case, collection) in [("named", "books"), ("empty", "")] {
        let root =
            std::env::temp_dir().join(format!("restore-source-only-{}-{case}", std::process::id()));
        std::fs::create_dir(&root).unwrap();
        let source = DocumentCatalog::create(&root.join("authority.redb"), collection).unwrap();
        let bundle = root.join("bundle");
        let manifest = source
            .capture_backup(&[], &limits())
            .unwrap()
            .write_to(&bundle)
            .unwrap();
        assert!(manifest.indexes.is_empty());
        assert_eq!(
            manifest
                .source
                .as_ref()
                .unwrap()
                .header
                .as_ref()
                .unwrap()
                .collection,
            collection
        );
        let source_bytes = std::fs::read(bundle.join("sources.redb")).unwrap();
        let destination = root.join("destination");
        let verified =
            DocumentCatalog::stage_backup_restore(&bundle, &destination, &request(&manifest))
                .unwrap();
        assert!(verified.manifest().indexes.is_empty());
        assert_eq!(
            verified
                .manifest()
                .source
                .as_ref()
                .unwrap()
                .header
                .as_ref()
                .unwrap()
                .collection,
            collection
        );
        assert_eq!(
            std::fs::read(destination.join("sources.redb")).unwrap(),
            source_bytes
        );
        drop(verified);
        drop(source);
        assert!(!destination.exists());
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[tokio::test]
async fn restore_rechecks_self_consistent_source_metadata_against_the_database() {
    for damage in ["records", "accepted-sequence"] {
        let f = Fixture::new();
        let (bundle, _) = backup(&f, "source-metadata").await;
        let mut manifest = read_manifest(&bundle);
        let checkpoint = manifest.source.as_mut().unwrap();
        match damage {
            "records" => checkpoint.records += 1,
            "accepted-sequence" => checkpoint.header.as_mut().unwrap().accepted_sequence += 1,
            _ => unreachable!(),
        }
        write_manifest(&bundle, &mut manifest);
        let destination = f.root.join("source-metadata-destination");
        assert!(
            DocumentCatalog::stage_backup_restore(&bundle, &destination, &request(&manifest))
                .is_err()
        );
        assert!(!destination.exists(), "{damage}");
    }
}

#[tokio::test]
async fn restore_runs_the_full_historical_source_audit_after_outer_integrity_passes() {
    let f = Fixture::new();
    let (bundle, _) = backup(&f, "historical-audit").await;
    {
        let database = redb::Database::open(bundle.join("sources.redb")).unwrap();
        let tx = database.begin_write().unwrap();
        {
            let mut versions = tx.open_table(VERSIONS).unwrap();
            let key = DocumentVersionKey {
                document_key: KEY.to_vec(),
                version: 1,
            }
            .encode_to_vec();
            let mut version: crate::pb::storage::DocumentVersion =
                decode(versions.get(key.as_slice()).unwrap().unwrap().value()).unwrap();
            version.source_sha256 = vec![0x55; 32];
            versions
                .insert(key.as_slice(), version.encode_to_vec().as_slice())
                .unwrap();
        }
        tx.commit().unwrap();
    }
    let source_bytes = std::fs::read(bundle.join("sources.redb")).unwrap();
    let source_sha = sha256::digest(&source_bytes);
    let mut manifest = read_manifest(&bundle);
    let checkpoint = manifest.source.as_mut().unwrap();
    checkpoint.bytes = source_bytes.len() as u64;
    checkpoint.sha256 = source_sha.to_vec();
    let source_artifact = manifest
        .artifacts
        .iter_mut()
        .find(|artifact| artifact.file == "sources.redb")
        .unwrap();
    source_artifact.bytes = source_bytes.len() as u64;
    source_artifact.sha256 = sha256::hex_digest(&source_bytes);
    write_manifest(&bundle, &mut manifest);

    let destination = f.root.join("historical-audit-destination");
    let error = DocumentCatalog::stage_backup_restore(&bundle, &destination, &request(&manifest))
        .err()
        .unwrap();
    assert_eq!(error.code(), Code::DataLoss);
    assert!(error.message().contains("journal audit"), "{error}");
    assert!(!destination.exists());
}

#[tokio::test]
async fn restore_preserves_existing_destinations_and_refuses_nested_destinations() {
    let f = Fixture::new();
    let (bundle, manifest) = backup(&f, "ownership-source").await;
    let occupied = f.root.join("occupied");
    std::fs::create_dir(&occupied).unwrap();
    std::fs::write(occupied.join("keep"), b"mine").unwrap();
    assert!(
        DocumentCatalog::stage_backup_restore(&bundle, &occupied, &request(&manifest)).is_err()
    );
    assert_eq!(std::fs::read(occupied.join("keep")).unwrap(), b"mine");
    let nested = bundle.join("nested-restore");
    assert!(DocumentCatalog::stage_backup_restore(&bundle, &nested, &request(&manifest)).is_err());
    assert!(!nested.exists());
}

#[tokio::test]
async fn restore_enforces_positive_request_budgets() {
    let f = Fixture::new();
    let (bundle, manifest) = backup(&f, "budget-source").await;
    for (name, limits) in [
        (
            "files",
            SourceBackupLimits {
                max_files: 1,
                ..limits()
            },
        ),
        (
            "metadata",
            SourceBackupLimits {
                metadata_bytes: 1,
                ..limits()
            },
        ),
        (
            "bytes",
            SourceBackupLimits {
                max_bytes: 1,
                ..limits()
            },
        ),
    ] {
        let destination = f.root.join(format!("budget-{name}"));
        let mut request = request(&manifest);
        request.limits = Some(limits);
        let error = DocumentCatalog::stage_backup_restore(&bundle, &destination, &request)
            .err()
            .unwrap();
        assert_eq!(error.code(), Code::ResourceExhausted, "{name}: {error}");
        assert!(!destination.exists());
    }
    for (name, limits) in [
        (
            "files",
            SourceBackupLimits {
                max_files: 0,
                ..limits()
            },
        ),
        (
            "metadata",
            SourceBackupLimits {
                metadata_bytes: 0,
                ..limits()
            },
        ),
        (
            "bytes",
            SourceBackupLimits {
                max_bytes: 0,
                ..limits()
            },
        ),
        (
            "batch",
            SourceBackupLimits {
                source_batch_bytes: 0,
                ..limits()
            },
        ),
    ] {
        let destination = f.root.join(format!("invalid-budget-{name}"));
        let mut request = request(&manifest);
        request.limits = Some(limits);
        let error = DocumentCatalog::stage_backup_restore(&bundle, &destination, &request)
            .err()
            .unwrap();
        assert_eq!(error.code(), Code::InvalidArgument, "{name}: {error}");
        assert!(!destination.exists());
    }
}

#[cfg(unix)]
#[tokio::test]
async fn restore_refuses_symlink_roots_intermediate_directories_and_leaves() {
    use std::os::unix::fs::symlink;
    for damage in ["root", "directory", "leaf"] {
        let f = Fixture::new();
        let (clean, manifest) = backup(&f, "symlink-clean").await;
        let external = f.root.join("external");
        copy_tree(&clean, &external);
        let bundle = f.root.join(format!("symlink-{damage}"));
        match damage {
            "root" => symlink(&external, &bundle).unwrap(),
            "directory" => {
                copy_tree(&clean, &bundle);
                let directory = Path::new(&manifest.indexes[0].directory);
                let top = directory.components().next().unwrap().as_os_str();
                std::fs::remove_dir_all(bundle.join(top)).unwrap();
                symlink(external.join(top), bundle.join(top)).unwrap();
            }
            "leaf" => {
                copy_tree(&clean, &bundle);
                let artifact = manifest
                    .artifacts
                    .iter()
                    .find(|a| a.file != "sources.redb")
                    .unwrap();
                std::fs::remove_file(bundle.join(&artifact.file)).unwrap();
                symlink(external.join(&artifact.file), bundle.join(&artifact.file)).unwrap();
            }
            _ => unreachable!(),
        }
        let destination = f.root.join("symlink-destination");
        assert!(
            DocumentCatalog::stage_backup_restore(&bundle, &destination, &request(&manifest))
                .is_err()
        );
        assert!(!destination.exists());
        assert_eq!(
            std::fs::read(external.join("source-backup.pb")).unwrap(),
            std::fs::read(clean.join("source-backup.pb")).unwrap()
        );
    }
}

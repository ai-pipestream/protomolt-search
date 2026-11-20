use super::*;
use crate::{
    pb::storage::{SourceBackupLimits, SourceBackupManifest, SourceRestoreRequest},
    segments::OpenedSegmentSet,
};
use std::path::{Path, PathBuf};
use tonic::Code;

/// A bundle of `KEY` published at versions 1 and 2 with an unpublished
/// third version, written under `name` in the fixture root.
pub(super) async fn backup(f: &Fixture, name: &str) -> (PathBuf, SourceBackupManifest) {
    f.publish_through_node(KEY, 1, Some(3)).await;
    f.publish_through_node(KEY, 2, Some(2)).await;
    let _ = f.stage(KEY, 3, Some(0)).await;
    let catalog = f.node.document_backup_catalog(&f.source, INDEX).unwrap();
    let path = f.root.join(name);
    let manifest = f
        .source
        .capture_backup(&[(INDEX, &catalog)], &backup_limits())
        .unwrap()
        .write_to(&path)
        .unwrap();
    (path, manifest)
}

pub(super) fn read_manifest(bundle: &Path) -> SourceBackupManifest {
    SourceBackupManifest::decode(
        std::fs::read(bundle.join("source-backup.pb"))
            .unwrap()
            .as_slice(),
    )
    .unwrap()
}

pub(super) fn write_manifest(bundle: &Path, manifest: &mut SourceBackupManifest) {
    manifest.manifest_sha256.clear();
    manifest.manifest_sha256 = turbo_sha256::sha256(&manifest.encode_to_vec()).0.to_vec();
    std::fs::write(bundle.join("source-backup.pb"), manifest.encode_to_vec()).unwrap();
}

pub(super) fn copy_tree(from: &Path, to: &Path) {
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

    let request = restore_request(&manifest);
    let verified = DocumentCatalog::stage_backup_restore(&bundle, &destination, &request).unwrap();
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
        let mut request = restore_request(&manifest);
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
        DocumentCatalog::stage_backup_restore(&bundle, &destination, &restore_request(&manifest))
            .unwrap();
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
                ..backup_limits()
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
            .capture_backup(&[], &backup_limits())
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
        let verified = DocumentCatalog::stage_backup_restore(
            &bundle,
            &destination,
            &restore_request(&manifest),
        )
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
async fn restore_preserves_existing_destinations_and_refuses_nested_destinations() {
    let f = Fixture::new();
    let (bundle, manifest) = backup(&f, "ownership-source").await;
    let occupied = f.root.join("occupied");
    std::fs::create_dir(&occupied).unwrap();
    std::fs::write(occupied.join("keep"), b"mine").unwrap();
    assert!(
        DocumentCatalog::stage_backup_restore(&bundle, &occupied, &restore_request(&manifest))
            .is_err()
    );
    assert_eq!(std::fs::read(occupied.join("keep")).unwrap(), b"mine");
    let nested = bundle.join("nested-restore");
    assert!(
        DocumentCatalog::stage_backup_restore(&bundle, &nested, &restore_request(&manifest))
            .is_err()
    );
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
                ..backup_limits()
            },
        ),
        (
            "metadata",
            SourceBackupLimits {
                metadata_bytes: 1,
                ..backup_limits()
            },
        ),
        (
            "bytes",
            SourceBackupLimits {
                max_bytes: 1,
                ..backup_limits()
            },
        ),
    ] {
        let destination = f.root.join(format!("budget-{name}"));
        let mut request = restore_request(&manifest);
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
                ..backup_limits()
            },
        ),
        (
            "metadata",
            SourceBackupLimits {
                metadata_bytes: 0,
                ..backup_limits()
            },
        ),
        (
            "bytes",
            SourceBackupLimits {
                max_bytes: 0,
                ..backup_limits()
            },
        ),
        (
            "batch",
            SourceBackupLimits {
                source_batch_bytes: 0,
                ..backup_limits()
            },
        ),
    ] {
        let destination = f.root.join(format!("invalid-budget-{name}"));
        let mut request = restore_request(&manifest);
        request.limits = Some(limits);
        let error = DocumentCatalog::stage_backup_restore(&bundle, &destination, &request)
            .err()
            .unwrap();
        assert_eq!(error.code(), Code::InvalidArgument, "{name}: {error}");
        assert!(!destination.exists());
    }
}

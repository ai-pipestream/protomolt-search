use super::restore::{backup, copy_tree, read_manifest, write_manifest};
use super::*;
use std::path::Path;
use tonic::Code;

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
        let request = restore_request(&read_manifest(&bundle));
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
        assert!(DocumentCatalog::stage_backup_restore(
            &bundle,
            &destination,
            &restore_request(&manifest)
        )
        .is_err());
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
    let error =
        DocumentCatalog::stage_backup_restore(&bundle, &destination, &restore_request(&manifest))
            .err()
            .unwrap();
    assert_eq!(error.code(), Code::DataLoss);
    assert!(error.message().contains("journal audit"), "{error}");
    assert!(!destination.exists());
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
        assert!(DocumentCatalog::stage_backup_restore(
            &bundle,
            &destination,
            &restore_request(&manifest)
        )
        .is_err());
        assert!(!destination.exists());
        assert_eq!(
            std::fs::read(external.join("source-backup.pb")).unwrap(),
            std::fs::read(clean.join("source-backup.pb")).unwrap()
        );
    }
}

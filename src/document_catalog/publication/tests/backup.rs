use super::*;
use crate::pb::storage::{SourceBackupLimits, SourceBackupManifest};
use crate::segments::OpenedSegmentSet;
use std::io::{Read, Seek, SeekFrom, Write};
use tonic::Code;

fn limits() -> SourceBackupLimits {
    SourceBackupLimits {
        metadata_bytes: 4 << 20,
        max_files: 256,
        max_bytes: 64 << 20,
        source_batch_bytes: 64 << 10,
    }
}
async fn publish(f: &Fixture, key: &[u8], version: u64, rows: Option<usize>) {
    let (_, candidate) = f.stage(key, version, rows).await;
    let node = f.node.clone();
    let source = f.source.clone();
    tokio::task::spawn_blocking(move || {
        node.publish_document_projection_blocking(&source, INDEX, &candidate)
    })
    .await
    .unwrap()
    .unwrap();
}
async fn compact(f: &Fixture) {
    let node = f.node.clone();
    let source = f.source.clone();
    tokio::task::spawn_blocking(move || {
        node.compact_document_index_blocking(
            &source,
            CompactDocumentIndexRequest {
                index_key: INDEX.to_vec(),
                batch_rows: 10,
                batch_bytes: 1 << 20,
                max_staged_bytes: 32 << 20,
                proof_batch_rows: 2,
            },
        )
    })
    .await
    .unwrap()
    .unwrap();
}
fn check_files(path: &std::path::Path, manifest: &SourceBackupManifest) {
    assert!(!path.join(".source-audit.redb").exists());
    assert_eq!(
        SourceBackupManifest::decode(
            std::fs::read(path.join("source-backup.pb"))
                .unwrap()
                .as_slice()
        )
        .unwrap(),
        *manifest
    );
    let mut encoded = manifest.clone();
    let id = std::mem::take(&mut encoded.manifest_sha256);
    assert_eq!(sha256::digest(&encoded.encode_to_vec()).as_slice(), id);
    for artifact in &manifest.artifacts {
        let bytes = std::fs::read(path.join(&artifact.file)).unwrap();
        assert_eq!(bytes.len() as u64, artifact.bytes);
        assert_eq!(sha256::hex_digest(&bytes), artifact.sha256);
    }
}

#[tokio::test]
async fn captured_backup_survives_source_compaction_and_keeps_unpublished_backlog() {
    let f = Fixture::new();
    publish(&f, KEY, 1, Some(3)).await;
    publish(&f, b"other", 1, Some(3)).await;
    compact(&f).await;
    publish(&f, KEY, 2, Some(2)).await;
    let _ = f.stage(KEY, 3, Some(0)).await;
    let catalog = f.node.document_backup_catalog(&f.source, INDEX).unwrap();
    let before = catalog.snapshot();
    assert_eq!(before.len(), 2);
    assert_eq!(before.metadata(0).live_rows, 3);
    let captured = f
        .source
        .capture_backup(&[(INDEX, &catalog)], &limits())
        .unwrap();
    compact(&f).await;
    for segment in &before.manifest().segments {
        assert!(
            !SegmentCatalog::segment_dir(before.root(), &segment.segment_id).exists(),
            "input path should have been retired while pinned"
        );
    }
    let output = f.root.join("backup");
    let manifest = captured.write_to(&output).unwrap();
    check_files(&output, &manifest);
    let checkpoint = manifest.source.as_ref().unwrap();
    assert_eq!(checkpoint.header.as_ref().unwrap().accepted_sequence, 4);
    assert_eq!(checkpoint.indexes[0].committed_sequence, 3);
    let copied_catalog = SegmentCatalog::open(output.join(&manifest.indexes[0].directory)).unwrap();
    let copied = copied_catalog.snapshot();
    assert_eq!(copied.manifest(), before.manifest());
    before
        .verify_source_rewrite(&copied, &f.root.join("backup-proof"), 2)
        .unwrap();
    let source = DocumentCatalog::open(&output.join("sources.redb"), "books").unwrap();
    assert_eq!(
        source.get(KEY, Some(3)).unwrap(),
        f.source.get(KEY, Some(3)).unwrap()
    );
    assert_eq!(
        source
            .current_index_publication_decision(INDEX, &copied_catalog)
            .unwrap()
            .unwrap()
            .source
            .unwrap()
            .version,
        2
    );
    let mut config = f.node.config.clone();
    config.index_path = Some(
        output
            .join(&manifest.indexes[0].directory)
            .parent()
            .unwrap()
            .join("index"),
    );
    let restored = NodeServiceImpl::open(config, None, false).unwrap();
    let activation = tokio::task::spawn_blocking(move || {
        restored.recover_document_projection_blocking(&source, INDEX)
    })
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    assert_eq!(activation.accepted_sequence, 3);
    assert_eq!(activation.version, 2);
}

#[tokio::test]
async fn backup_refuses_missing_duplicate_and_foreign_indexes_and_preserves_destinations() {
    let f = Fixture::new();
    publish(&f, KEY, 1, Some(2)).await;
    let catalog = f.node.document_backup_catalog(&f.source, INDEX).unwrap();
    for supplied in [
        vec![],
        vec![(INDEX, &catalog), (INDEX, &catalog)],
        vec![(&b"wrong"[..], &catalog)],
    ] {
        assert_eq!(
            f.source
                .capture_backup(&supplied, &limits())
                .err()
                .unwrap()
                .code(),
            Code::FailedPrecondition
        );
    }
    let foreign = Fixture::new();
    publish(&foreign, KEY, 1, Some(2)).await;
    let wrong = foreign
        .node
        .document_backup_catalog(&foreign.source, INDEX)
        .unwrap();
    assert_eq!(
        f.source
            .capture_backup(&[(INDEX, &wrong)], &limits())
            .err()
            .unwrap()
            .code(),
        Code::FailedPrecondition
    );
    for config in [
        SourceBackupLimits {
            max_files: 1,
            ..limits()
        },
        SourceBackupLimits {
            metadata_bytes: 1,
            ..limits()
        },
        SourceBackupLimits {
            max_bytes: 1,
            ..limits()
        },
    ] {
        assert_eq!(
            f.source
                .capture_backup(&[(INDEX, &catalog)], &config)
                .err()
                .unwrap()
                .code(),
            Code::ResourceExhausted
        );
    }
    let output = f.root.join("occupied");
    std::fs::create_dir(&output).unwrap();
    std::fs::write(output.join("keep"), b"existing").unwrap();
    let captured = f
        .source
        .capture_backup(&[(INDEX, &catalog)], &limits())
        .unwrap();
    assert_eq!(
        captured.write_to(&output).unwrap_err().code(),
        Code::AlreadyExists
    );
    assert_eq!(std::fs::read(output.join("keep")).unwrap(), b"existing");
}

#[tokio::test]
async fn backup_copy_rejects_changed_pinned_bytes_and_cleans_only_its_private_output() {
    let f = Fixture::new();
    publish(&f, KEY, 1, Some(2)).await;
    let catalog = f.node.document_backup_catalog(&f.source, INDEX).unwrap();
    let captured = f
        .source
        .capture_backup(&[(INDEX, &catalog)], &limits())
        .unwrap();
    let set = catalog.snapshot();
    let segment = set.metadata(0);
    let path =
        SegmentCatalog::segment_dir(set.root(), &segment.segment_id).join(&segment.bm25.file);
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
    let output = f.root.join("bad-backup");
    assert_eq!(
        captured.write_to(&output).unwrap_err().code(),
        Code::DataLoss
    );
    assert!(!output.exists());
    assert!(f.root.join("source.redb").exists());
}

#[tokio::test]
async fn backup_retains_an_empty_declared_index_and_later_source_acceptance() {
    let f = Fixture::new();
    publish(&f, KEY, 1, Some(2)).await;
    publish(&f, KEY, 2, None).await;
    compact(&f).await;
    let catalog = f.node.document_backup_catalog(&f.source, INDEX).unwrap();
    assert!(catalog.snapshot().is_empty());
    assert!(catalog.snapshot().generation_declaration().is_some());
    let output = f.root.join("empty-backup");
    let node = f.node.clone();
    let source = f.source.clone();
    let target = output.clone();
    let manifest = tokio::task::spawn_blocking(move || {
        node.backup_documents_blocking(&source, INDEX, &target, &limits())
    })
    .await
    .unwrap()
    .unwrap();
    check_files(&output, &manifest);
    let reopened = OpenedSegmentSet::open(output.join(&manifest.indexes[0].directory)).unwrap();
    assert_eq!(reopened.manifest(), catalog.snapshot().manifest());
    let copied = DocumentCatalog::open(&output.join("sources.redb"), "books").unwrap();
    assert_eq!(
        copied.get(KEY, None).unwrap(),
        f.source.get(KEY, None).unwrap()
    );
    let request = AcceptDocumentRequest {
        contract_version: 1,
        document_key: KEY.to_vec(),
        operation_id: b"after-backup".to_vec(),
        expected_version: Some(2),
        mutation: Some(Mutation::Delete(true)),
        ..Default::default()
    };
    assert_eq!(copied.accept(&request).unwrap().version, 3);
    assert_eq!(f.source.get(KEY, None).unwrap().unwrap().0.version, 2);
}

#[tokio::test]
async fn backup_captures_two_owned_indexes_in_key_order() {
    let f = Fixture::new();
    let (_, candidate) = f.stage(KEY, 1, Some(2)).await;
    let mut config = f.node.config.clone();
    config.index_path = Some(f.root.join("second-node"));
    let second = NodeServiceImpl::open(config, None, false).unwrap();
    let second_candidate = second
        .stage_document_projection(
            f.source.clone(),
            StageDocumentProjectionRequest {
                projection: Some(PrepareDocumentProjectionRequest {
                    history_id: candidate.info().history_id.clone(),
                    document_key: KEY.to_vec(),
                    version: 1,
                    expected_plan_fingerprint: mapping::derive_plan(DESCRIPTOR, TYPE)
                        .unwrap()
                        .fingerprint,
                    max_rows: 100,
                    max_bytes: 1 << 20,
                    ..Default::default()
                }),
                field_analysis: vec![MappedFieldAnalysis {
                    path: "chunks.body".into(),
                    analysis: Some(body_spec()),
                }],
                max_staged_bytes: 16 << 20,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let one = f.node.clone();
    let two = second.clone();
    let source = f.source.clone();
    tokio::task::spawn_blocking(move || {
        one.publish_document_projection_blocking(&source, INDEX, &candidate)
            .unwrap();
        two.publish_document_projection_blocking(&source, b"second-index", &second_candidate)
            .unwrap();
    })
    .await
    .unwrap();
    let first_catalog = f.node.document_backup_catalog(&f.source, INDEX).unwrap();
    let second_catalog = second
        .document_backup_catalog(&f.source, b"second-index")
        .unwrap();
    let captured = f
        .source
        .capture_backup(
            &[(b"second-index", &second_catalog), (INDEX, &first_catalog)],
            &limits(),
        )
        .unwrap();
    let output = f.root.join("two-indexes");
    let manifest = captured.write_to(&output).unwrap();
    check_files(&output, &manifest);
    assert_eq!(
        manifest
            .indexes
            .iter()
            .map(|i| i.index_key.as_slice())
            .collect::<Vec<_>>(),
        [INDEX, b"second-index"]
    );
    let copied = DocumentCatalog::open(&output.join("sources.redb"), "books").unwrap();
    for index in &manifest.indexes {
        let catalog = SegmentCatalog::open(output.join(&index.directory)).unwrap();
        assert_eq!(
            copied
                .current_index_publication_decision(&index.index_key, &catalog)
                .unwrap()
                .unwrap()
                .source
                .unwrap()
                .accepted_sequence,
            1
        );
    }
}

#[tokio::test]
async fn backup_database_budget_failure_removes_partial_output() {
    let f = Fixture::new();
    publish(&f, KEY, 1, Some(2)).await;
    let catalog = f.node.document_backup_catalog(&f.source, INDEX).unwrap();
    let reference = f
        .source
        .capture_backup(&[(INDEX, &catalog)], &limits())
        .unwrap()
        .write_to(&f.root.join("reference"))
        .unwrap();
    let artifact_bytes: u64 = reference
        .artifacts
        .iter()
        .filter(|a| a.file != "sources.redb")
        .map(|a| a.bytes)
        .sum();
    let tight = SourceBackupLimits {
        max_bytes: artifact_bytes + 1,
        ..limits()
    };
    let captured = f
        .source
        .capture_backup(&[(INDEX, &catalog)], &tight)
        .unwrap();
    let output = f.root.join("too-small");
    assert_eq!(
        captured.write_to(&output).unwrap_err().code(),
        Code::ResourceExhausted
    );
    assert!(!output.exists());
    assert!(f.root.join("reference/source-backup.pb").exists());
}

#[tokio::test]
async fn backup_without_any_index_retains_all_accepted_source() {
    let f = Fixture::new();
    let _ = f.stage(KEY, 1, Some(0)).await;
    let output = f.root.join("source-only");
    let manifest = f
        .source
        .capture_backup(
            &[],
            &SourceBackupLimits {
                max_files: 1,
                ..limits()
            },
        )
        .unwrap()
        .write_to(&output)
        .unwrap();
    check_files(&output, &manifest);
    assert!(manifest.indexes.is_empty());
    assert_eq!(manifest.artifacts.len(), 1);
    let copied = DocumentCatalog::open(&output.join("sources.redb"), "books").unwrap();
    assert_eq!(
        copied.get(KEY, None).unwrap(),
        f.source.get(KEY, None).unwrap()
    );
}

#[tokio::test]
async fn backup_rejects_destinations_inside_a_live_catalog() {
    let f = Fixture::new();
    publish(&f, KEY, 1, Some(2)).await;
    let catalog = f.node.document_backup_catalog(&f.source, INDEX).unwrap();
    let output = catalog.snapshot().root().join("nested-backup");
    let captured = f
        .source
        .capture_backup(&[(INDEX, &catalog)], &limits())
        .unwrap();
    assert_eq!(
        captured.write_to(&output).unwrap_err().code(),
        Code::FailedPrecondition
    );
    assert!(!output.exists());
}

#[tokio::test]
async fn backup_rejects_a_hole_in_older_accepted_history() {
    let f = Fixture::new();
    let _ = f.stage(KEY, 1, Some(0)).await;
    let _ = f.stage(KEY, 2, Some(0)).await;
    let key = DocumentVersionKey {
        document_key: KEY.to_vec(),
        version: 1,
    }
    .encode_to_vec();
    let tx = f.source.database.begin_write().unwrap();
    tx.open_table(VERSIONS)
        .unwrap()
        .remove(key.as_slice())
        .unwrap();
    tx.commit().unwrap();
    // The latest head still exists and there are no index tips to expose the hole.
    assert!(f.source.get(KEY, None).unwrap().is_some());
    let output = f.root.join("incomplete-history");
    let result = f
        .source
        .capture_backup(&[], &limits())
        .unwrap()
        .write_to(&output);
    assert_eq!(result.unwrap_err().code(), Code::DataLoss);
    assert!(!output.exists());
}

#[tokio::test]
async fn backup_rejects_missing_older_publication_decisions() {
    let f = Fixture::new();
    for version in 1..=3 {
        publish(&f, KEY, version, Some(2)).await;
    }
    let key = decision_key(INDEX, 1);
    let tx = f.source.database.begin_write().unwrap();
    tx.open_table(DECISIONS)
        .unwrap()
        .remove(key.as_slice())
        .unwrap();
    tx.commit().unwrap();
    let catalog = f.node.document_backup_catalog(&f.source, INDEX).unwrap();
    let output = f.root.join("missing-decision");
    let captured = f
        .source
        .capture_backup(&[(INDEX, &catalog)], &limits())
        .unwrap();
    assert_eq!(
        captured.write_to(&output).unwrap_err().code(),
        Code::DataLoss
    );
    assert!(!output.exists());
}

#[tokio::test]
async fn backup_checks_historical_source_links_even_with_new_record_hashes() {
    for damage in ["before", "after", "source", "orphan"] {
        let f = Fixture::new();
        for version in 1..=3 {
            publish(&f, KEY, version, Some(2)).await;
        }
        let tx = f.source.database.begin_write().unwrap();
        {
            let mut table = tx.open_table(DECISIONS).unwrap();
            let key = decision_key(INDEX, 2);
            let mut intent: ProjectionIntent =
                decode(table.get(key.as_slice()).unwrap().unwrap().value()).unwrap();
            match damage {
                "before" => intent.before_manifest_sha256 = vec![44; 32],
                "after" => intent.after_manifest_sha256 = vec![45; 32],
                "source" => intent.source = Some(f.source.get(KEY, Some(1)).unwrap().unwrap().0),
                "orphan" => intent.index_key = b"unregistered-index".to_vec(),
                _ => unreachable!(),
            }
            intent.intent_id = intent_hash(&intent);
            let key = decision_key(&intent.index_key, 2);
            table
                .insert(key.as_slice(), intent.encode_to_vec().as_slice())
                .unwrap();
        }
        tx.commit().unwrap();
        let catalog = f.node.document_backup_catalog(&f.source, INDEX).unwrap();
        let captured = f
            .source
            .capture_backup(&[(INDEX, &catalog)], &limits())
            .unwrap();
        let output = f.root.join("broken-source-chain");
        let error = captured.write_to(&output).unwrap_err();
        assert_eq!(error.code(), Code::DataLoss, "{damage}: {error}");
        assert!(
            error.message().contains("journal audit"),
            "{damage}: {error}"
        );
        assert!(!output.exists());
    }
}

#[tokio::test]
async fn backup_walks_maintenance_older_than_the_current_tip_and_its_predecessor() {
    use crate::pb::storage::{MaintenanceCursor, MaintenanceIntent};
    for damage in [
        "missing", "previous", "anchor", "hash", "encoding", "orphan",
    ] {
        let f = Fixture::new();
        publish(&f, KEY, 1, Some(2)).await;
        compact(&f).await;
        publish(&f, KEY, 2, Some(3)).await;
        for _ in 0..3 {
            compact(&f).await;
        }
        publish(&f, KEY, 3, Some(1)).await;
        let _ = f.stage(KEY, 4, Some(0)).await;
        // A coherent interleaved chain and unpublished backlog must pass first.
        let catalog = f.node.document_backup_catalog(&f.source, INDEX).unwrap();
        let clean = f
            .source
            .capture_backup(&[(INDEX, &catalog)], &limits())
            .unwrap()
            .write_to(&f.root.join("clean-chain"))
            .unwrap();
        assert_eq!(
            clean
                .source
                .as_ref()
                .unwrap()
                .header
                .as_ref()
                .unwrap()
                .accepted_sequence,
            4
        );
        assert_eq!(
            clean.source.as_ref().unwrap().indexes[0].committed_sequence,
            3
        );
        let tx = f.source.database.begin_write().unwrap();
        {
            let mut table = tx.open_table(MAINTENANCE).unwrap();
            assert_eq!(table.len().unwrap(), 4);
            let first: MaintenanceIntent = table
                .iter()
                .unwrap()
                .map(|entry| decode::<MaintenanceIntent>(entry.unwrap().1.value()).unwrap())
                .min_by_key(|intent| intent.after_epoch)
                .unwrap();
            let key = maintenance_key(INDEX, first.after_epoch);
            if damage == "missing" {
                table.remove(key.as_slice()).unwrap();
            } else if damage == "encoding" {
                let mut bytes = first.encode_to_vec();
                bytes.extend_from_slice(&[0xa0, 6, 1]);
                table.insert(key.as_slice(), bytes.as_slice()).unwrap();
            } else {
                let mut intent = first;
                match damage {
                    "previous" => {
                        intent.previous = Some(MaintenanceCursor {
                            after_epoch: intent.before_epoch,
                            intent_id: vec![46; 32],
                        })
                    }
                    "anchor" => intent.source_intent_id = vec![47; 32],
                    "hash" => intent.before_manifest_sha256 = vec![48; 32],
                    "orphan" => {
                        intent.owner.as_mut().unwrap().index_key = b"unregistered-index".to_vec();
                        intent.preservation.as_mut().unwrap().owner = intent.owner.clone();
                    }
                    _ => unreachable!(),
                }
                intent.intent_id.clear();
                intent.intent_id = sha256::digest(&intent.encode_to_vec()).to_vec();
                let key = maintenance_key(
                    &intent.owner.as_ref().unwrap().index_key,
                    intent.after_epoch,
                );
                table
                    .insert(key.as_slice(), intent.encode_to_vec().as_slice())
                    .unwrap();
            }
        }
        tx.commit().unwrap();
        // The tip validator sees only the most recent maintenance pair; the
        // damaged first decision sits before both and before later source writes.
        let captured = f
            .source
            .capture_backup(&[(INDEX, &catalog)], &limits())
            .unwrap();
        let output = f.root.join("broken-maintenance-chain");
        let error = captured.write_to(&output).unwrap_err();
        assert_eq!(error.code(), Code::DataLoss, "{damage}: {error}");
        assert!(!output.exists());
    }
}

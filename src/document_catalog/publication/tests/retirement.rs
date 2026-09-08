use super::*;
use crate::pb::storage::{
    SourceBackupLimits, SourceHistorySeal, SourceRestoreRequest, SourceRetirementIntent,
    SourceRetirementRequest, SourceSealRequest,
};
use crate::stats_identity::StatsClaim;
use std::sync::{Arc, Barrier};
use tonic::Code;

fn limits() -> SourceBackupLimits {
    SourceBackupLimits {
        metadata_bytes: 4 << 20,
        max_files: 256,
        max_bytes: 64 << 20,
        source_batch_bytes: 64 << 10,
    }
}

fn history(f: &Fixture) -> Vec<u8> {
    f.source
        .capture_checkpoint(1 << 20)
        .unwrap()
        .metadata()
        .header
        .as_ref()
        .unwrap()
        .history_id
        .clone()
}

fn request(f: &Fixture, operation: &[u8]) -> SourceRetirementRequest {
    SourceRetirementRequest {
        history_id: history(f),
        operation_id: operation.to_vec(),
    }
}

fn seal_request(intent: &SourceRetirementIntent) -> SourceSealRequest {
    SourceSealRequest {
        history_id: intent.history_id.clone(),
        expected_accepted_sequence: intent.accepted_sequence,
        operation_id: intent.operation_id.clone(),
    }
}

async fn publish(f: &Fixture, version: u64, rows: usize) -> DocumentProjectionActivation {
    let (_, candidate) = f.stage(KEY, version, Some(rows)).await;
    let node = f.node.clone();
    let source = f.source.clone();
    tokio::task::spawn_blocking(move || {
        node.publish_document_projection_blocking(&source, INDEX, &candidate)
    })
    .await
    .unwrap()
    .unwrap()
}

#[tokio::test]
async fn retirement_is_persistent_idempotent_and_closes_source_writes() {
    let mut f = Fixture::new();
    let (_, candidate) = f.stage(KEY, 1, Some(0)).await;
    let original = f.source.get(KEY, Some(1)).unwrap().unwrap().1.unwrap();
    let request = request(&f, b"move-books");
    for malformed in [
        SourceRetirementRequest {
            history_id: vec![],
            ..request.clone()
        },
        SourceRetirementRequest {
            operation_id: vec![],
            ..request.clone()
        },
    ] {
        assert_eq!(
            f.source.begin_retirement(&malformed).unwrap_err().code(),
            Code::InvalidArgument
        );
    }
    let mut wrong = request.clone();
    wrong.history_id[0] ^= 1;
    assert_eq!(
        f.source.begin_retirement(&wrong).unwrap_err().code(),
        Code::FailedPrecondition
    );
    let intent = f.source.begin_retirement(&request).unwrap();
    assert_eq!(intent.format_version, 1);
    assert_eq!(intent.accepted_sequence, 1);
    assert_eq!(intent.history_id, request.history_id);
    assert_eq!(intent.operation_id, request.operation_id);
    assert_eq!(f.source.begin_retirement(&request).unwrap(), intent);
    assert_eq!(f.source.retirement_intent().unwrap(), Some(intent.clone()));
    let error = f.source.enable_index_maintenance().unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("retiring"), "{error}");
    let mut changed = request.clone();
    changed.operation_id = b"another-move".to_vec();
    assert_eq!(
        f.source.begin_retirement(&changed).unwrap_err().code(),
        Code::AlreadyExists
    );
    for write in [
        AcceptDocumentRequest {
            contract_version: 1,
            document_key: KEY.to_vec(),
            operation_id: [KEY, &1u64.to_be_bytes()].concat(),
            expected_version: Some(0),
            mutation: Some(Mutation::Source(original)),
            ..Default::default()
        },
        AcceptDocumentRequest {
            contract_version: 1,
            document_key: b"new-during-retirement".to_vec(),
            operation_id: b"new-during-retirement".to_vec(),
            expected_version: Some(0),
            mutation: Some(Mutation::Delete(true)),
            ..Default::default()
        },
    ] {
        let error = f.source.accept(&write).unwrap_err();
        assert_eq!(error.code(), Code::FailedPrecondition);
        assert!(error.message().contains("retiring"), "{error}");
    }
    assert!(f.source.get(KEY, Some(1)).unwrap().is_some());
    let node = f.node.clone();
    let source = f.source.clone();
    let error = tokio::task::spawn_blocking(move || {
        node.publish_document_projection_blocking(&source, INDEX, &candidate)
    })
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("retiring"), "{error}");
    let mut mismatched_seal = seal_request(&intent);
    mismatched_seal.operation_id = b"another-seal".to_vec();
    assert_eq!(
        f.source.seal_history(&mismatched_seal).unwrap_err().code(),
        Code::AlreadyExists
    );
    let mut stale_seal = seal_request(&intent);
    stale_seal.expected_accepted_sequence -= 1;
    assert_eq!(
        f.source.seal_history(&stale_seal).unwrap_err().code(),
        Code::FailedPrecondition
    );
    assert_eq!(f.source.retirement_intent().unwrap(), Some(intent.clone()));
    assert!(f.source.history_seal().unwrap().is_none());
    let seal = f.source.seal_history(&seal_request(&intent)).unwrap();
    let error = f
        .source
        .accept(&AcceptDocumentRequest {
            contract_version: 1,
            document_key: b"new-after-seal".to_vec(),
            operation_id: b"new-after-seal".to_vec(),
            expected_version: Some(0),
            mutation: Some(Mutation::Delete(true)),
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("sealed"), "{error}");

    let old = std::mem::replace(
        &mut f.source,
        Arc::new(DocumentCatalog::in_memory("books").unwrap()),
    );
    drop(old);
    let reopened = DocumentCatalog::open(&f.root.join("source.redb"), "books").unwrap();
    assert_eq!(reopened.retirement_intent().unwrap(), Some(intent.clone()));
    assert_eq!(reopened.begin_retirement(&request).unwrap(), intent);
    assert_eq!(reopened.history_seal().unwrap(), Some(seal));
    assert_eq!(
        DocumentCatalog::in_memory("books")
            .unwrap()
            .begin_retirement(&SourceRetirementRequest {
                history_id: vec![1; 16],
                operation_id: b"move".to_vec(),
            })
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
}

#[tokio::test]
async fn retirement_allows_pending_publication_recovery_before_matching_seal() {
    for committed in [false, true] {
        let f = Fixture::new();
        let (_, candidate) = f.stage(KEY, 1, Some(1)).await;
        let retirement = request(&f, if committed { b"commit" } else { b"abort" });
        f.bind(&candidate);
        let result = f.publish(&candidate, None, !committed);
        assert_eq!(result.is_ok(), committed);
        let intent = f.source.begin_retirement(&retirement).unwrap();
        let recovery = f
            .source
            .recover_index_publication(INDEX, &f.target)
            .unwrap();
        assert_eq!(
            matches!(&recovery, ProjectionRecovery::Committed(_)),
            committed
        );
        assert_eq!(
            matches!(&recovery, ProjectionRecovery::Aborted(_)),
            !committed
        );
        let seal = f.source.seal_history(&seal_request(&intent)).unwrap();
        assert_eq!(seal.accepted_sequence, intent.accepted_sequence);
        assert_eq!(f.source.begin_retirement(&retirement).unwrap(), intent);
    }
}

#[tokio::test]
async fn retirement_allows_pending_maintenance_recovery_before_matching_seal() {
    for phase in [1, 2] {
        let f = Fixture::new();
        let active = publish(&f, 1, 2).await;
        let retirement = request(
            &f,
            if phase == 1 {
                b"maint-abort"
            } else {
                b"maint-commit"
            },
        );
        let root = crate::node::segments_root(f.node.config.index_path.as_ref().unwrap());
        let before = Arc::new(OpenedSegmentSet::open(&root).unwrap());
        let selected = before.metadata(0).clone();
        let directory = SegmentCatalog::segment_dir(&root, &selected.segment_id);
        let node = f.node.clone();
        let source = f.source.clone();
        let scratch = f.root.join(format!("maintenance-proof-{phase}"));
        let epoch = before.epoch();
        tokio::task::spawn_blocking(move || {
            crate::node::INTERRUPT_MAINTENANCE.with(|point| point.set(phase));
            node.publish_document_maintenance_blocking(
                &source,
                INDEX,
                StatsClaim::required(active.stats_epoch, &active.stats_incarnation).unwrap(),
                epoch,
                vec![SegmentSource {
                    segment_id: "retirement-maintenance-output",
                    generation: epoch + 1,
                    base_label: 0,
                    backend_kind: &selected.backend_kind,
                    vector_path: Some(&directory.join(&selected.vector.file)),
                    exact_vector_path: Some(&directory.join(&selected.exact_vectors.file)),
                    bm25_path: &directory.join(&selected.bm25.file),
                    live_docs_path: &directory.join(&selected.live_docs.file),
                    partition_column: None,
                }],
                &scratch,
                1,
            )
        })
        .await
        .unwrap()
        .unwrap_err();
        let intent = f.source.begin_retirement(&retirement).unwrap();
        let catalog = SegmentCatalog::open(root).unwrap();
        let recovery = f.source.recover_index_maintenance(INDEX, &catalog).unwrap();
        assert_eq!(
            matches!(&recovery, MaintenanceRecovery::Committed(_)),
            phase == 2
        );
        assert_eq!(
            matches!(&recovery, MaintenanceRecovery::Aborted(_)),
            phase == 1
        );
        f.source.seal_history(&seal_request(&intent)).unwrap();
    }
}

#[tokio::test]
async fn retiring_backup_and_restore_preserve_intent_and_backlog() {
    let f = Fixture::new();
    publish(&f, 1, 2).await;
    let _ = f.stage(KEY, 2, Some(0)).await;
    let catalog = f.node.document_backup_catalog(&f.source, INDEX).unwrap();
    let intent = f
        .source
        .begin_retirement(&request(&f, b"transfer-backup"))
        .unwrap();
    let bundle = f.root.join("retiring-bundle");
    let manifest = f
        .source
        .capture_backup(&[(INDEX, &catalog)], &limits())
        .unwrap()
        .write_to(&bundle)
        .unwrap();
    let header = manifest.source.as_ref().unwrap().header.as_ref().unwrap();
    assert_eq!(header.accepted_sequence, 2);
    assert_eq!(header.retirement_intent, Some(intent.clone()));
    assert_eq!(
        manifest.source.as_ref().unwrap().indexes[0].committed_sequence,
        1
    );
    let destination = f.root.join("retiring-restore");
    let verified = DocumentCatalog::stage_backup_restore(
        &bundle,
        &destination,
        &SourceRestoreRequest {
            expected_manifest_sha256: manifest.manifest_sha256.clone(),
            collection: "books".into(),
            history_id: intent.history_id.clone(),
            limits: Some(limits()),
        },
    )
    .unwrap();
    assert_eq!(
        verified
            .manifest()
            .source
            .as_ref()
            .unwrap()
            .header
            .as_ref()
            .unwrap()
            .retirement_intent,
        Some(intent.clone())
    );
    let copied = DocumentCatalog::open(&bundle.join("sources.redb"), "books").unwrap();
    assert_eq!(copied.retirement_intent().unwrap(), Some(intent.clone()));
    let error = copied
        .accept(&AcceptDocumentRequest {
            contract_version: 1,
            document_key: b"after-copy".to_vec(),
            operation_id: b"after-copy".to_vec(),
            expected_version: Some(0),
            mutation: Some(Mutation::Delete(true)),
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("retiring"), "{error}");
    let seal = f.source.seal_history(&seal_request(&intent)).unwrap();
    let sealed_bundle = f.root.join("retired-bundle");
    let sealed_manifest = f
        .source
        .capture_backup(&[(INDEX, &catalog)], &limits())
        .unwrap()
        .write_to(&sealed_bundle)
        .unwrap();
    let sealed_header = sealed_manifest
        .source
        .as_ref()
        .unwrap()
        .header
        .as_ref()
        .unwrap();
    assert_eq!(sealed_header.format_version, 6);
    assert_eq!(sealed_header.retirement_intent, Some(intent.clone()));
    assert_eq!(sealed_header.history_seal, Some(seal.clone()));
    assert_eq!(sealed_header.accepted_sequence, 2);
    assert_eq!(
        sealed_manifest.source.as_ref().unwrap().indexes[0].committed_sequence,
        1
    );
    let sealed_destination = f.root.join("retired-restore");
    let sealed_verified = DocumentCatalog::stage_backup_restore(
        &sealed_bundle,
        &sealed_destination,
        &SourceRestoreRequest {
            expected_manifest_sha256: sealed_manifest.manifest_sha256.clone(),
            collection: "books".into(),
            history_id: intent.history_id.clone(),
            limits: Some(limits()),
        },
    )
    .unwrap();
    let staged_header = sealed_verified
        .manifest()
        .source
        .as_ref()
        .unwrap()
        .header
        .as_ref()
        .unwrap();
    assert_eq!(staged_header.format_version, 6);
    assert_eq!(staged_header.retirement_intent, Some(intent));
    assert_eq!(staged_header.history_seal, Some(seal));
    assert_eq!(staged_header.accepted_sequence, 2);
    assert_eq!(
        sealed_verified.manifest().source.as_ref().unwrap().indexes[0].committed_sequence,
        1
    );
}

#[test]
fn acceptance_racing_retirement_is_serialized_at_one_captured_watermark() {
    let f = Fixture::new();
    let history = history(&f);
    let barrier = Arc::new(Barrier::new(3));
    let accepting = {
        let source = f.source.clone();
        let barrier = barrier.clone();
        let history = history.clone();
        std::thread::spawn(move || {
            barrier.wait();
            source.accept(&AcceptDocumentRequest {
                contract_version: 2,
                history_id: history,
                document_key: KEY.to_vec(),
                operation_id: b"racing-accept".to_vec(),
                expected_version: Some(0),
                mutation: Some(Mutation::Delete(true)),
                ..Default::default()
            })
        })
    };
    let retiring = {
        let source = f.source.clone();
        let barrier = barrier.clone();
        std::thread::spawn(move || {
            barrier.wait();
            source.begin_retirement(&SourceRetirementRequest {
                history_id: history,
                operation_id: b"racing-retirement".to_vec(),
            })
        })
    };
    barrier.wait();
    let accepted = accepting.join().unwrap();
    let intent = retiring.join().unwrap().unwrap();
    match accepted {
        Ok(receipt) => {
            assert_eq!(receipt.accepted_sequence, 1);
            assert_eq!(intent.accepted_sequence, 1);
        }
        Err(error) => {
            assert_eq!(error.code(), Code::FailedPrecondition);
            assert!(error.message().contains("retiring"), "{error}");
            assert_eq!(intent.accepted_sequence, 0);
        }
    }
}

#[test]
fn reopen_refuses_incoherent_retirement_header_states() {
    for (case, format_version, retirement, seal) in [
        ("active-with-intent", 3, true, false),
        ("retiring-without-intent", 5, false, false),
        ("retiring-with-seal", 5, true, true),
        ("retired-without-intent", 6, false, true),
        ("retired-without-seal", 6, true, false),
        ("direct-seal-with-intent", 4, true, true),
        ("retired-operation-mismatch", 6, true, true),
        ("retiring-wrong-history", 5, true, false),
        ("retiring-wrong-watermark", 5, true, false),
        ("retiring-wrong-inner-format", 5, true, false),
        ("retiring-empty-operation", 5, true, false),
    ] {
        let root =
            std::env::temp_dir().join(format!("retirement-header-{}-{case}", std::process::id()));
        std::fs::create_dir(&root).unwrap();
        let path = root.join("source.redb");
        let source = DocumentCatalog::create(&path, "books").unwrap();
        let history = source
            .capture_checkpoint(1 << 20)
            .unwrap()
            .metadata()
            .header
            .as_ref()
            .unwrap()
            .history_id
            .clone();
        drop(source);
        {
            let database = redb::Database::open(&path).unwrap();
            let tx = database.begin_write().unwrap();
            {
                let mut meta = tx.open_table(META).unwrap();
                let mut header: DocumentCatalogHeader =
                    decode(meta.get("header").unwrap().unwrap().value()).unwrap();
                header.format_version = format_version;
                header.retirement_intent = retirement.then(|| SourceRetirementIntent {
                    format_version: 1,
                    history_id: history.clone(),
                    accepted_sequence: 0,
                    operation_id: b"transfer".to_vec(),
                });
                header.history_seal = seal.then(|| SourceHistorySeal {
                    format_version: 1,
                    history_id: history.clone(),
                    accepted_sequence: 0,
                    operation_id: b"transfer".to_vec(),
                });
                match case {
                    "retired-operation-mismatch" => {
                        header.history_seal.as_mut().unwrap().operation_id = b"other".to_vec()
                    }
                    "retiring-wrong-history" => {
                        header.retirement_intent.as_mut().unwrap().history_id[0] ^= 1
                    }
                    "retiring-wrong-watermark" => {
                        header.retirement_intent.as_mut().unwrap().accepted_sequence = 1
                    }
                    "retiring-wrong-inner-format" => {
                        header.retirement_intent.as_mut().unwrap().format_version = 2
                    }
                    "retiring-empty-operation" => header
                        .retirement_intent
                        .as_mut()
                        .unwrap()
                        .operation_id
                        .clear(),
                    _ => {}
                }
                meta.insert("header", header.encode_to_vec().as_slice())
                    .unwrap();
            }
            tx.commit().unwrap();
        }
        let error = DocumentCatalog::open(&path, "books").err().unwrap();
        assert_eq!(error.code(), Code::DataLoss, "{case}");
        std::fs::remove_dir_all(root).unwrap();
    }
}

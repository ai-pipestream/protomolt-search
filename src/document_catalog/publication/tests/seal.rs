use super::*;
use crate::pb::storage::{SourceBackupLimits, SourceRestoreRequest, SourceSealRequest};
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

fn seal_request(f: &Fixture, sequence: u64, operation: &[u8]) -> SourceSealRequest {
    SourceSealRequest {
        history_id: f
            .source
            .capture_checkpoint(1 << 20)
            .unwrap()
            .metadata()
            .header
            .as_ref()
            .unwrap()
            .history_id
            .clone(),
        expected_accepted_sequence: sequence,
        operation_id: operation.to_vec(),
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
async fn seal_is_terminal_persistent_idempotent_and_bound_to_its_watermark() {
    let mut f = Fixture::new();
    publish(&f, 1, 1).await;
    let catalog = f.node.document_backup_catalog(&f.source, INDEX).unwrap();
    let request = seal_request(&f, 1, b"retire-books");
    let expected_history = request.history_id.clone();
    for changed in ["history", "sequence"] {
        let mut wrong = request.clone();
        if changed == "history" {
            wrong.history_id[0] ^= 1;
        } else {
            wrong.expected_accepted_sequence = 0;
        }
        assert_eq!(
            f.source.seal_history(&wrong).unwrap_err().code(),
            Code::FailedPrecondition
        );
        assert!(f.source.history_seal().unwrap().is_none());
    }
    let sealed = f.source.seal_history(&request).unwrap();
    assert_eq!(sealed.format_version, 1);
    assert_eq!(sealed.history_id, expected_history);
    assert_eq!(sealed.accepted_sequence, 1);
    assert_eq!(sealed.operation_id, b"retire-books");
    assert_eq!(f.source.seal_history(&request).unwrap(), sealed);
    for error in [
        f.source.enable_index_maintenance().unwrap_err(),
        f.source
            .recover_index_publication(INDEX, &catalog)
            .unwrap_err(),
        f.source
            .recover_index_maintenance(INDEX, &catalog)
            .unwrap_err(),
    ] {
        assert_eq!(error.code(), Code::FailedPrecondition);
        assert!(error.message().contains("sealed"), "{error}");
    }
    let mut changed = request.clone();
    changed.operation_id = b"different-retirement".to_vec();
    assert_eq!(
        f.source.seal_history(&changed).unwrap_err().code(),
        Code::AlreadyExists
    );
    assert_eq!(f.source.history_seal().unwrap(), Some(sealed.clone()));

    let old = std::mem::replace(
        &mut f.source,
        Arc::new(DocumentCatalog::in_memory("books").unwrap()),
    );
    drop(old);
    let reopened = DocumentCatalog::open(&f.root.join("source.redb"), "books").unwrap();
    assert_eq!(reopened.history_seal().unwrap(), Some(sealed.clone()));
    assert_eq!(reopened.seal_history(&request).unwrap(), sealed);
    let (_, original_source) = reopened.get(KEY, Some(1)).unwrap().unwrap();
    let retry = AcceptDocumentRequest {
        contract_version: 1,
        document_key: KEY.to_vec(),
        operation_id: [KEY, &1u64.to_be_bytes()].concat(),
        expected_version: Some(0),
        mutation: Some(Mutation::Source(original_source.unwrap())),
        ..Default::default()
    };
    let replayed = reopened.accept(&retry).unwrap();
    assert!(replayed.replayed);
    assert_eq!((replayed.version, replayed.accepted_sequence), (1, 1));
    let new_write = AcceptDocumentRequest {
        contract_version: 1,
        document_key: b"new-after-seal".to_vec(),
        operation_id: b"new-after-seal".to_vec(),
        expected_version: Some(0),
        mutation: Some(Mutation::Delete(true)),
        ..Default::default()
    };
    let error = reopened.accept(&new_write).unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("sealed"), "{error}");
    assert_eq!(
        DocumentCatalog::in_memory("books")
            .unwrap()
            .seal_history(&SourceSealRequest {
                history_id: vec![1; 16],
                expected_accepted_sequence: 0,
                operation_id: b"seal".to_vec(),
            })
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
}

#[tokio::test]
async fn seal_refuses_pending_publication_then_succeeds_after_recovery() {
    let f = Fixture::new();
    let (_, candidate) = f.stage(KEY, 1, Some(1)).await;
    let request = seal_request(&f, 1, b"seal-after-recovery");
    f.bind(&candidate);
    assert!(f.publish(&candidate, None, true).is_err());
    let error = f.source.seal_history(&request).unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("pending"), "{error}");
    assert!(f.source.history_seal().unwrap().is_none());
    assert!(matches!(
        f.source
            .recover_index_publication(INDEX, &f.target)
            .unwrap(),
        ProjectionRecovery::Aborted(_)
    ));
    assert_eq!(
        f.source.seal_history(&request).unwrap().accepted_sequence,
        1
    );
}

#[tokio::test]
async fn seal_refuses_pending_maintenance_then_succeeds_after_recovery() {
    let f = Fixture::new();
    let active = publish(&f, 1, 2).await;
    let request = seal_request(&f, 1, b"seal-after-maintenance-recovery");
    let root = crate::node::segments_root(f.node.config.index_path.as_ref().unwrap());
    let before = Arc::new(OpenedSegmentSet::open(&root).unwrap());
    let selected = before.metadata(0).clone();
    let directory = SegmentCatalog::segment_dir(&root, &selected.segment_id);
    let node = f.node.clone();
    let source = f.source.clone();
    let scratch = f.root.join("pending-maintenance-proof");
    let epoch = before.epoch();
    let error = tokio::task::spawn_blocking(move || {
        crate::node::INTERRUPT_MAINTENANCE.with(|point| point.set(1));
        node.publish_document_maintenance_blocking(
            &source,
            INDEX,
            StatsClaim::required(active.stats_epoch, &active.stats_incarnation).unwrap(),
            epoch,
            vec![SegmentSource {
                segment_id: "pending-maintenance-output",
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
    assert!(
        error.message().contains("after maintenance intent"),
        "{error}"
    );
    let error = f.source.seal_history(&request).unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("pending"), "{error}");
    let catalog = SegmentCatalog::open(root.clone()).unwrap();
    assert!(matches!(
        f.source.recover_index_maintenance(INDEX, &catalog).unwrap(),
        MaintenanceRecovery::Aborted(_)
    ));
    let mut next_manifest = before.published_manifest();
    next_manifest.epoch = before.epoch() + 1;
    let after = SegmentCatalog::open_staged(&root, next_manifest, Default::default())
        .unwrap()
        .snapshot();
    f.source
        .prepare_index_maintenance(
            INDEX,
            &before,
            &after,
            &f.root.join("direct-maintenance-proof-before-seal"),
            1,
        )
        .unwrap();
    assert!(matches!(
        f.source.recover_index_maintenance(INDEX, &catalog).unwrap(),
        MaintenanceRecovery::Aborted(_)
    ));
    assert_eq!(
        f.source.seal_history(&request).unwrap().accepted_sequence,
        1
    );
    let error = f
        .source
        .prepare_index_maintenance(
            INDEX,
            &before,
            &after,
            &f.root.join("direct-maintenance-proof-after-seal"),
            1,
        )
        .unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("sealed"), "{error}");
}

#[tokio::test]
async fn candidate_staged_before_seal_cannot_publish_or_advance_the_journal() {
    let f = Fixture::new();
    let (_, candidate) = f.stage(KEY, 1, Some(2)).await;
    let seal = f
        .source
        .seal_history(&seal_request(&f, 1, b"seal-staged"))
        .unwrap();
    let node = f.node.clone();
    let source = f.source.clone();
    let error = tokio::task::spawn_blocking(move || {
        node.publish_document_projection_blocking(&source, INDEX, &candidate)
    })
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("sealed"), "{error}");
    assert_eq!(f.source.history_seal().unwrap(), Some(seal));
    assert!(f
        .source
        .index_publication_decision(INDEX, 1)
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn sealed_backup_and_restore_retain_the_terminal_marker() {
    let f = Fixture::new();
    publish(&f, 1, 2).await;
    let _ = f.stage(KEY, 2, Some(0)).await;
    let catalog = f.node.document_backup_catalog(&f.source, INDEX).unwrap();
    let seal = f
        .source
        .seal_history(&seal_request(&f, 2, b"sealed-backup"))
        .unwrap();
    let bundle = f.root.join("sealed-bundle");
    let manifest = f
        .source
        .capture_backup(&[(INDEX, &catalog)], &limits())
        .unwrap()
        .write_to(&bundle)
        .unwrap();
    assert_eq!(
        manifest
            .source
            .as_ref()
            .unwrap()
            .header
            .as_ref()
            .unwrap()
            .history_seal,
        Some(seal.clone())
    );
    assert_eq!(
        manifest
            .source
            .as_ref()
            .unwrap()
            .header
            .as_ref()
            .unwrap()
            .accepted_sequence,
        2
    );
    assert_eq!(
        manifest.source.as_ref().unwrap().indexes[0].committed_sequence,
        1
    );
    let request = SourceRestoreRequest {
        expected_manifest_sha256: manifest.manifest_sha256.clone(),
        collection: "books".into(),
        history_id: seal.history_id.clone(),
        limits: Some(limits()),
    };
    let destination = f.root.join("sealed-restore");
    let verified = DocumentCatalog::stage_backup_restore(&bundle, &destination, &request).unwrap();
    assert_eq!(
        verified
            .manifest()
            .source
            .as_ref()
            .unwrap()
            .header
            .as_ref()
            .unwrap()
            .history_seal,
        Some(seal.clone())
    );
    assert_eq!(
        verified
            .manifest()
            .source
            .as_ref()
            .unwrap()
            .header
            .as_ref()
            .unwrap()
            .accepted_sequence,
        2
    );
    assert_eq!(
        verified.manifest().source.as_ref().unwrap().indexes[0].committed_sequence,
        1
    );
    let copied = DocumentCatalog::open(&bundle.join("sources.redb"), "books").unwrap();
    assert_eq!(copied.history_seal().unwrap(), Some(seal));
    let write = AcceptDocumentRequest {
        contract_version: 1,
        document_key: b"after-seal".to_vec(),
        operation_id: b"after-seal".to_vec(),
        expected_version: Some(0),
        mutation: Some(Mutation::Delete(true)),
        ..Default::default()
    };
    let error = copied.accept(&write).unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("sealed"), "{error}");
}

#[test]
fn reopen_refuses_headers_where_format_and_seal_presence_disagree() {
    for (case, format_version, include_seal) in [
        ("format-three-with-seal", 3, true),
        ("format-four-without-seal", 4, false),
    ] {
        let root =
            std::env::temp_dir().join(format!("source-seal-header-{}-{case}", std::process::id()));
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
                header.history_seal = include_seal.then(|| crate::pb::storage::SourceHistorySeal {
                    format_version: 1,
                    history_id: history,
                    accepted_sequence: 0,
                    operation_id: b"malformed-header".to_vec(),
                });
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

#[tokio::test]
async fn accept_racing_seal_has_one_serialized_winner() {
    let f = Fixture::new();
    let history = seal_request(&f, 0, b"race-seal").history_id;
    let barrier = Arc::new(Barrier::new(3));
    let accepting = {
        let source = f.source.clone();
        let barrier = barrier.clone();
        std::thread::spawn(move || {
            barrier.wait();
            source.accept(&AcceptDocumentRequest {
                contract_version: 2,
                history_id: history,
                document_key: KEY.to_vec(),
                operation_id: b"race-accept".to_vec(),
                expected_version: Some(0),
                mutation: Some(Mutation::Delete(true)),
                ..Default::default()
            })
        })
    };
    let sealing = {
        let source = f.source.clone();
        let barrier = barrier.clone();
        let request = seal_request(&f, 0, b"race-seal");
        std::thread::spawn(move || {
            barrier.wait();
            source.seal_history(&request)
        })
    };
    barrier.wait();
    let accepted = accepting.join().unwrap();
    let sealed = sealing.join().unwrap();
    match (accepted, sealed) {
        (Ok(receipt), Err(error)) => {
            assert_eq!(receipt.accepted_sequence, 1);
            assert_eq!(error.code(), Code::FailedPrecondition);
            assert!(f.source.history_seal().unwrap().is_none());
        }
        (Err(error), Ok(seal)) => {
            assert_eq!(error.code(), Code::FailedPrecondition);
            assert_eq!(seal.accepted_sequence, 0);
            assert_eq!(f.source.history_seal().unwrap(), Some(seal));
        }
        outcomes => panic!("accept and seal were not serialized: {outcomes:?}"),
    }
}

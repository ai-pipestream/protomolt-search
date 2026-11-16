//! Snapshot export, install, and publication fencing.

use super::*;
use crate::postings::StoredBinding;

fn persisted_config(index_path: PathBuf) -> NodeConfig {
    NodeConfig {
        index_path: Some(index_path),
        ..Default::default()
    }
}

#[test]
fn configuring_and_appending_vectors_require_a_new_snapshot_flush() {
    let root = std::env::temp_dir().join(format!(
        "snapshot-vector-publication-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&root).unwrap();
    for wal in [false, true] {
        {
            let path = root.join(format!("{wal}.tv"));
            let service = NodeServiceImpl::open(
                NodeConfig {
                    index_path: Some(path.clone()),
                    wal,
                    ..Default::default()
                },
                None,
            )
            .unwrap();
            service.flush_index().unwrap();
            let epoch = read_shard(&service.state).stats_epoch;
            service
                .apply_backend_config(crate::harness::embedded_backend_request(
                    8, 4, &[0.0; 8], &[1.0; 8],
                ))
                .unwrap();
            assert_ne!(read_shard(&service.state).stats_epoch, epoch);
            let directory = root.join(format!("copy-{wal}"));
            std::fs::create_dir_all(&directory).unwrap();
            assert!(service
                .export_if_flushed(&path, &directory)
                .unwrap()
                .is_none());
            service.flush_index().unwrap();
            let epoch = read_shard(&service.state).stats_epoch;
            service
                .apply_batch(
                    AddVectorsRequest {
                        materialized_rows: Vec::new(),
                        dim: 8,
                        vectors: vec![0.25; 8],
                    },
                    None,
                )
                .unwrap();
            assert_ne!(read_shard(&service.state).stats_epoch, epoch);
            assert!(service
                .export_if_flushed(&path, &directory)
                .unwrap()
                .is_none());
            assert_eq!(std::fs::read_dir(&directory).unwrap().count(), 0);
            service.flush_index().unwrap();
            let (manifest, _) = service
                .export_if_flushed(&path, &directory)
                .unwrap()
                .unwrap();
            assert_eq!(manifest.vector_rows, 1);
            assert_eq!(manifest.layout, LAYOUT_SEGMENTS);
            let set = crate::segments::OpenedSegmentSet::open(directory.join(CATALOG_DIR)).unwrap();
            let image = set.vector(0).unwrap();
            assert_eq!(image.len(), 1);
            assert_eq!(image.dim_opt(), Some(8));
        }
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn metadata_only_snapshots_reject_false_shape_claims_before_install() {
    let root = std::env::temp_dir().join(format!("snapshot-empty-shape-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let source_path = root.join("source.tv");
    let source = NodeServiceImpl::new(None, persisted_config(source_path.clone()));
    let binding = StoredBinding {
        plan_fingerprint: "a".repeat(64),
        body_path: "body".into(),
        ..Default::default()
    };
    NodeServiceImpl::apply_binding_locked(&mut write_shard(&source.state), binding.clone())
        .unwrap();
    source.flush_index().unwrap();
    let directory = root.join("repository");
    std::fs::create_dir_all(&directory).unwrap();
    let (manifest, _) = source
        .export_if_flushed(&source_path, &directory)
        .unwrap()
        .unwrap();
    let target_path = root.join("target.tv");
    let target = NodeServiceImpl::new(None, persisted_config(target_path.clone()));
    for case in 0..5 {
        let mut invalid = manifest.clone();
        match case {
            0 => invalid.vector_rows = 1,
            1 => invalid.document_rows = 1,
            2 => invalid.live_rows = 1,
            3 => invalid.dim = 8,
            _ => invalid.scoring_fingerprint = "invented".into(),
        }
        assert_eq!(
            target
                .install_staged_repository(&directory, &invalid)
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
        assert!(read_shard(&target.state).mapped_binding.is_none());
        assert!(!segments_root(&target_path).join("segments.json").exists());
    }
    target
        .install_staged_repository(&directory, &manifest)
        .unwrap();
    assert_eq!(
        read_shard(&target.state).mapped_binding,
        Some(binding.clone())
    );
    drop(target);
    // A crash after retiring the old catalog but before adopting its
    // replacement restores the binding-only catalog.
    std::fs::rename(segments_root(&target_path), segments_old_dir(&target_path)).unwrap();
    let reopened = NodeServiceImpl::open(persisted_config(target_path), None).unwrap();
    assert_eq!(read_shard(&reopened.state).mapped_binding, Some(binding));
    assert!(read_shard(&reopened.state).index.is_none());
    drop(reopened);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn copy_rechecks_mutations_after_acquiring_the_seal_fence_with_or_without_wal() {
    let root = std::env::temp_dir().join(format!("snapshot-publication-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    for wal in [false, true] {
        let path = root.join(format!("{wal}.tv"));
        let service = NodeServiceImpl::new(
            None,
            NodeConfig {
                index_path: Some(path.clone()),
                wal,
                ..Default::default()
            },
        );
        let binding = crate::postings::StoredBinding {
            plan_fingerprint: "a".repeat(64),
            body_path: "body".into(),
            ..Default::default()
        };
        NodeServiceImpl::apply_binding_locked(&mut write_shard(&service.state), binding.clone())
            .unwrap();
        service.flush_index().unwrap();
        let directory = root.join(format!("copy-{wal}"));
        std::fs::create_dir_all(&directory).unwrap();
        let sealing = service.seal_lock.lock().unwrap();
        let copying = service.clone();
        let copy_path = path.clone();
        let copy_directory = directory.clone();
        let copy =
            std::thread::spawn(move || copying.export_if_flushed(&copy_path, &copy_directory));
        // A real append after Flush, while publication is held out.
        service
            .apply_document_locked(
                &mut write_shard(&service.state),
                AddDocumentsRequest {
                    text: "word".into(),
                    ..Default::default()
                },
                crate::postings::AnalyzedDoc::body(vec![("word".into(), 1, vec![(0, 4)])], 1),
                None,
                None,
                &mut 0,
                &mut 0,
            )
            .unwrap();
        drop(sealing);
        assert!(copy.join().unwrap().unwrap().is_none());
        assert_eq!(std::fs::read_dir(&directory).unwrap().count(), 0);
        service.flush_index().unwrap();
        let (manifest, _) = service
            .export_if_flushed(&path, &directory)
            .unwrap()
            .unwrap();
        assert_eq!(manifest.document_rows, 1);
        let set = crate::segments::OpenedSegmentSet::open(directory.join(CATALOG_DIR)).unwrap();
        assert_eq!(set.bm25(0).next_doc_id(), 1);
        assert_eq!(set.binding(), Some(&binding));
    }
    std::fs::remove_dir_all(root).unwrap();
}

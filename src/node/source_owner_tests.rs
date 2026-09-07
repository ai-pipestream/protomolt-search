//! Low-level callers must obey the same source owner as the ingest handlers.
use super::*;
use crate::segments::{SegmentSetManifest, SegmentSourceOwner};

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let root = std::env::temp_dir().join(format!(
            "source-owner-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, AtomicOrdering::Relaxed)
        ));
        std::fs::create_dir(&root).unwrap();
        Self(root)
    }
    fn owned_catalog(&self, root: &Path) {
        std::fs::create_dir_all(root).unwrap();
        let manifest = SegmentSetManifest {
            format: 3,
            source_owner: Some(
                SegmentSourceOwner::encode(&crate::pb::storage::SourceIndexOwner {
                    format_version: 1,
                    history_id: vec![1; 16],
                    index_key: b"index".to_vec(),
                    collection: "books".into(),
                })
                .unwrap(),
            ),
            ..Default::default()
        };
        // Simulate a persisted source publication; generic manifest writers
        // must not be able to create this authority.
        std::fs::write(
            root.join("segments.json"),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
    }
    fn config(&self, name: &str) -> NodeConfig {
        NodeConfig {
            collection: "books".into(),
            index_path: Some(self.0.join(name)),
            wal: false,
            layout: Layout::Segments,
            ..Default::default()
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn refused<T>(result: Result<T, Status>) {
    let error = result
        .err()
        .expect("source ownership must refuse legacy mutation");
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(error.message().contains("source-managed index"), "{error}");
}

#[test]
fn every_row_commit_boundary_refuses_an_owned_empty_index() {
    let fixture = Fixture::new();
    let config = fixture.config("index");
    fixture.owned_catalog(&segments_root(config.index_path.as_ref().unwrap()));
    let node = NodeServiceImpl::open(config, None, false).unwrap();
    {
        let mut state = write_shard(&node.state);
        let mut added = 0;
        let mut first = 0;
        refused(node.apply_batch_locked(
            &mut state,
            AddVectorsRequest {
                dim: 8,
                vectors: vec![0.2; 8],
            },
            None,
        ));
        refused(node.apply_document_locked(
            &mut state,
            AddDocumentsRequest::default(),
            crate::postings::AnalyzedDoc::default(),
            None,
            None,
            &mut added,
            &mut first,
        ));
        refused(node.delete_documents_locked(&mut state, &[0], None));
        refused(node.commit_replacements_locked(&mut state, &[], None));
        refused(NodeServiceImpl::apply_binding_locked(
            &mut state,
            crate::postings::StoredBinding::default(),
        ));
        assert_eq!(added, 0);
        assert_eq!(physical_rows(&state), 0);
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let _runtime = runtime.enter();
    refused(node.compact_shard(&crate::pb::CompactShardRequest::default()));
    refused(node.apply_snapshot(&fixture.0.join("missing"), false, false, false));
}

#[test]
fn index_only_snapshot_cannot_import_or_overwrite_source_ownership() {
    let fixture = Fixture::new();
    let repository = fixture.0.join("repository");
    fixture.owned_catalog(&repository.join(CATALOG_DIR));
    let manifest = RepositoryManifest {
        format_version: crate::snapshot_repository::FORMAT_VERSION,
        layout: LAYOUT_SEGMENTS.into(),
        backend_kind: String::new(),
        scoring_fingerprint: String::new(),
        dim: 0,
        slot_offset: 0,
        collection: "books".into(),
        vector_rows: 0,
        document_rows: 0,
        live_rows: 0,
        analysis_fingerprints: vec![],
        wal_generation: 0,
        wal_high_watermark: 0,
        wal_clocked: false,
        artifacts: vec![],
    };
    let fresh = fixture.config("fresh");
    let root = segments_root(fresh.index_path.as_ref().unwrap());
    let node = NodeServiceImpl::open(fresh, None, false).unwrap();
    let before = std::fs::read(root.join("segments.json")).ok();
    refused(node.apply_segment_snapshot(&repository, &manifest));
    assert_eq!(std::fs::read(root.join("segments.json")).ok(), before);
    assert!(read_shard(&node.state)
        .bm25
        .as_ref()
        .is_some_and(|s| s.doc_count() == 0));
    let config = fixture.config("owned");
    let root = segments_root(config.index_path.as_ref().unwrap());
    fixture.owned_catalog(&root);
    let owned = NodeServiceImpl::open(config, None, false).unwrap();
    let before = std::fs::read(root.join("segments.json")).unwrap();
    refused(owned.install_staged_repository(&repository, &manifest));
    refused(owned.apply_segment_snapshot(&repository, &manifest));
    assert_eq!(std::fs::read(root.join("segments.json")).unwrap(), before);
}

#[test]
fn owned_catalog_cannot_be_hidden_by_a_competing_snapshot_generation() {
    let fixture = Fixture::new();
    for old in [false, true] {
        let config = fixture.config(if old { "old" } else { "current" });
        let path = config.index_path.as_ref().unwrap();
        let root = segments_root(path);
        fixture.owned_catalog(&root);
        let directory = if old {
            generation_old_dir(path)
        } else {
            generation_dir(path)
        };
        std::fs::create_dir(&directory).unwrap();
        let before = std::fs::read(root.join("segments.json")).unwrap();
        let error = NodeServiceImpl::open(config.clone(), None, false)
            .err()
            .expect("competing generation must refuse before recovery");
        assert!(error.contains("source-managed index"));
        assert!(directory.exists());
        assert_eq!(std::fs::read(root.join("segments.json")).unwrap(), before);
    }
}

#[test]
fn initial_owner_attachment_cannot_certify_an_unrelated_provider_image() {
    let fixture = Fixture::new();
    let config = fixture.config("index");
    let root = segments_root(config.index_path.as_ref().unwrap());
    fixture.owned_catalog(&root);
    let values: Vec<f32> = (0..64).map(|i| (i as f32 * 0.3).sin()).collect();
    let backend =
        VectorIndex::fit_backend_config(crate::vector::EMBEDDED_TURBOVEC, 8, 4, &values).unwrap();
    let mut index = VectorIndex::from_backend_config(8, &backend).unwrap();
    index.add(&values, 8).unwrap();
    let shard =
        SegmentedShard::open_with(&root, heap_store(&config).unwrap(), config.vector_load())
            .unwrap();
    let node = NodeServiceImpl::new(Some(index), config);
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(
        || node.with_bm25(Some(Bm25Shard::Segmented(shard)))
    ))
    .is_err());
}

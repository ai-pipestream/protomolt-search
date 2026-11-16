use super::*;
use crate::pb::node_service_server::NodeService;
use crate::segments::{SegmentCatalog, SegmentSource};

fn fixture(label: &str) -> (PathBuf, NodeConfig, Arc<OpenedSegmentSet>) {
    let root =
        std::env::temp_dir().join(format!("node-publication-{label}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("index");
    let bm25_path = root.join("documents.bm25");
    let live_path = root.join("live.bin");
    let mut store = crate::postings::Bm25Store::with_fields(&["body"]);
    for row in 0..2 {
        store.add_document(
            row,
            "document".into(),
            crate::analyzer::analyze_document_native(
                "document",
                Some(&crate::analyzer::body_spec()),
            )
            .unwrap(),
        );
    }
    store.save(&bm25_path).unwrap();
    LiveDocs::default().write(&live_path, 2).unwrap();
    let catalog = SegmentCatalog::open(segments_root(&path)).unwrap();
    let old = catalog
        .append(SegmentSource {
            multi_res_path: None,
            segment_id: "old",
            generation: 1,
            base_label: 0,
            backend_kind: "",
            vector_path: None,
            exact_vector_path: None,
            bm25_path: &bm25_path,
            live_docs_path: &live_path,
            partition_column: None,
            parents_colocated: false,
        })
        .unwrap();
    let config = NodeConfig {
        index_path: Some(path),
        wal: false,
        ..Default::default()
    };
    (root, config, old)
}

fn claim(node: &NodeServiceImpl) -> StatsClaim {
    let guard = read_shard(&node.state);
    StatsClaim::required(guard.stats_epoch, &guard.stats_incarnation.bytes().unwrap()).unwrap()
}

#[test]
fn uncertain_manifest_keeps_old_runtime_and_blocks_flush_until_reopen() {
    let (root, config, old) = fixture("uncertain");
    let node = NodeServiceImpl::open(config.clone(), None).unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let claim = {
        let guard = read_shard(&node.state);
        StatsClaim::required(guard.stats_epoch, &guard.stats_incarnation.bytes().unwrap()).unwrap()
    };
    node.flush_index().unwrap();
    assert!(read_shard(&node.state).files_current);
    crate::segments::FAIL_SET_SYNC.with(|fail| fail.set(true));
    let error = node
        .publish_segment_rows_blocking(
            claim,
            old.epoch(),
            &[SegmentRowRetirement {
                segment_id: "old".into(),
                rows: vec![0],
            }],
            vec![],
        )
        .unwrap_err();
    assert!(error.message().contains("after manifest rename"), "{error}");
    let held = runtime
        .block_on(node.health(Request::new(crate::pb::HealthRequest {})))
        .unwrap()
        .into_inner();
    assert_eq!(read_shard(&node.state).stats_epoch, claim.epoch);
    assert_eq!(held.deleted_docs, 0);
    assert!(node.ingest_fence().is_some());
    // Model an export whose initial flush preceded the uncertain commit.
    let export = root.join("export");
    std::fs::create_dir(&export).unwrap();
    let copy_error = node
        .export_if_flushed(config.index_path.as_ref().unwrap(), &export)
        .expect_err("an already-flushed snapshot must reject uncertain publication");
    assert!(copy_error.message().contains("uncertain"));
    assert_eq!(std::fs::read_dir(&export).unwrap().count(), 0);
    assert!(node
        .flush_index()
        .unwrap_err()
        .message()
        .contains("uncertain"));
    assert_eq!(old.live_docs(0).deleted_count(), 0);
    drop(node);
    let recovered = NodeServiceImpl::open(config, None).unwrap();
    let recovered_health = runtime
        .block_on(recovered.health(Request::new(crate::pb::HealthRequest {})))
        .unwrap()
        .into_inner();
    assert_eq!(recovered_health.deleted_docs, 1);
    drop(recovered);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn publication_refuses_stale_owners_and_unsealed_rows_without_changing_disk() {
    let (root, config, old) = fixture("preconditions");
    let node = NodeServiceImpl::open(config.clone(), None).unwrap();
    let expected = claim(&node);
    let manifest_path = segments_root(config.index_path.as_ref().unwrap()).join("segments.json");
    let before = std::fs::read(&manifest_path).unwrap();
    let retirement = [SegmentRowRetirement {
        segment_id: "old".into(),
        rows: vec![0],
    }];
    for (version, epoch) in [
        (StatsClaim::default(), old.epoch()),
        (
            StatsClaim::required(expected.epoch, &[1; 32]).unwrap(),
            old.epoch(),
        ),
        (expected, old.epoch() + 1),
    ] {
        assert!(node
            .publish_segment_rows_blocking(version, epoch, &retirement, vec![])
            .is_err());
        assert_eq!(std::fs::read(&manifest_path).unwrap(), before);
    }
    {
        let mut guard = write_shard(&node.state);
        let Some(Bm25Shard::Segmented(shard)) = guard.bm25.as_mut() else {
            panic!("segmented fixture")
        };
        shard
            .add_document(
                2,
                "pending".into(),
                crate::analyzer::analyze_document_native(
                    "pending",
                    Some(&crate::analyzer::body_spec()),
                )
                .unwrap(),
                None,
            )
            .unwrap();
    }
    let error = node
        .publish_segment_rows_blocking(expected, old.epoch(), &retirement, vec![])
        .unwrap_err();
    assert!(error.message().contains("sealed document tail"), "{error}");
    assert_eq!(std::fs::read(&manifest_path).unwrap(), before);
    assert_eq!(
        read_shard(&node.state).bm25.as_ref().unwrap().doc_count(),
        3
    );
    assert!(node.ingest_fence().is_none());
    drop(node);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn publication_cannot_bypass_wal_recovery() {
    let (root, mut config, old) = fixture("wal");
    config.wal = true;
    let node = NodeServiceImpl::open(config.clone(), None).unwrap();
    let manifest_path = segments_root(config.index_path.as_ref().unwrap()).join("segments.json");
    let before = std::fs::read(&manifest_path).unwrap();
    let error = node
        .publish_segment_rows_blocking(
            claim(&node),
            old.epoch(),
            &[SegmentRowRetirement {
                segment_id: "old".into(),
                rows: vec![0],
            }],
            vec![],
        )
        .unwrap_err();
    assert!(error.message().contains("without WAL"), "{error}");
    assert_eq!(std::fs::read(&manifest_path).unwrap(), before);
    assert!(node.ingest_fence().is_none());
    // Retiring a failed writer does not change the configured recovery
    // contract. It must not turn this into an unlogged publication target.
    write_shard(&node.state).wal = None;
    let error = node
        .publish_segment_rows_blocking(
            claim(&node),
            old.epoch(),
            &[SegmentRowRetirement {
                segment_id: "old".into(),
                rows: vec![0],
            }],
            vec![],
        )
        .unwrap_err();
    assert!(error.message().contains("without WAL"), "{error}");
    assert_eq!(std::fs::read(&manifest_path).unwrap(), before);
    drop(node);
    std::fs::remove_dir_all(root).unwrap();
}

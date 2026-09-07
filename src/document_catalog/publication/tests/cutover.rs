use super::*;
use crate::pb::node_service_server::NodeService;
use crate::stats_identity::StatsClaim;
use tonic::Request;

async fn publish(fixture: &Fixture, version: u64, rows: usize) -> DocumentProjectionActivation {
    let (_, candidate) = fixture.stage(KEY, version, Some(rows)).await;
    let node = fixture.node.clone();
    let source = fixture.source.clone();
    tokio::task::spawn_blocking(move || {
        node.publish_document_projection_blocking(&source, INDEX, &candidate)
    })
    .await
    .unwrap()
    .unwrap()
}

#[tokio::test]
async fn maintenance_cutover_recovers_both_runtime_windows_and_an_uncertain_manifest() {
    for phase in [1, 2, 3] {
        let mut fixture = Fixture::new();
        publish(&fixture, 1, 2).await;
        let active = publish(&fixture, 2, 1).await;
        let root = crate::node::segments_root(fixture.node.config.index_path.as_ref().unwrap());
        let before = Arc::new(OpenedSegmentSet::open(&root).unwrap());
        let old_manifest = std::fs::read(root.join("segments.json")).unwrap();
        let selected = before.metadata(1).clone();
        let directory = SegmentCatalog::segment_dir(&root, &selected.segment_id);
        let node = fixture.node.clone();
        let source = fixture.source.clone();
        let scratch = fixture.root.join("proof");
        let epoch = before.epoch();
        let error = tokio::task::spawn_blocking(move || {
            if phase == 3 {
                crate::segments::FAIL_SET_SYNC.with(|fail| fail.set(true));
            } else {
                crate::node::INTERRUPT_MAINTENANCE.with(|point| point.set(phase));
            }
            node.publish_document_maintenance_blocking(
                &source,
                INDEX,
                StatsClaim::required(active.stats_epoch, &active.stats_incarnation).unwrap(),
                epoch,
                vec![SegmentSource {
                    segment_id: "maintenance-output",
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
            error.message().contains(match phase {
                1 => "after maintenance intent",
                2 => "after maintenance activation",
                _ => "after manifest rename",
            }),
            "{error}"
        );
        assert!(fixture.node.ingest_fence().is_some());
        let health = fixture
            .node
            .health(Request::new(HealthRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(health.document_slots, if phase == 2 { 1 } else { 3 });
        assert_eq!(
            std::fs::read(root.join("segments.json")).unwrap() == old_manifest,
            phase == 1
        );
        assert!(fixture
            .source
            .index_maintenance_decision(INDEX, epoch + 1)
            .unwrap()
            .is_none());
        // The old held reader remains valid regardless of which files won.
        assert_eq!(before.bm25(1).document_identity(0).unwrap().version, 2);
        let old = std::mem::replace(
            &mut fixture.source,
            Arc::new(DocumentCatalog::in_memory("books").unwrap()),
        );
        drop(old);
        fixture.source =
            Arc::new(DocumentCatalog::open(&fixture.root.join("source.redb"), "books").unwrap());
        fixture.node = NodeServiceImpl::open(fixture.node.config.clone(), None, false).unwrap();
        let node = fixture.node.clone();
        let source = fixture.source.clone();
        let recovered = tokio::task::spawn_blocking(move || {
            node.recover_document_maintenance_blocking(&source, INDEX)
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(recovered.is_some(), phase != 1);
        if let Some(recovered) = recovered {
            assert_eq!(recovered.accepted_sequence, 2);
            assert_eq!(recovered.live_rows, 1);
            assert_eq!(recovered.catalog_epoch, epoch + 1);
            let fetch = fixture
                .node
                .fetch_values(Request::new(FetchValuesRequest {
                    candidate_ids: vec![0, 1, 2],
                    include_identities: true,
                    expected_stats_epoch: recovered.stats_epoch,
                    expected_stats_incarnation: recovered.stats_incarnation,
                    ..Default::default()
                }))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(fetch.identities.len(), 1);
            assert_eq!(fetch.identities[0].doc_id, 0);
            assert_eq!(fetch.identities[0].identity.as_ref().unwrap().version, 2);
        }
        assert!(fixture.node.ingest_fence().is_none());
    }
}

#[tokio::test]
async fn a_source_write_can_finish_during_verification_and_invalidates_the_rewrite() {
    let fixture = Fixture::new();
    publish(&fixture, 1, 2).await;
    let active = publish(&fixture, 2, 1).await;
    let (_, candidate) = fixture.stage(KEY, 3, Some(1)).await;
    let root = crate::node::segments_root(fixture.node.config.index_path.as_ref().unwrap());
    let before = OpenedSegmentSet::open(&root).unwrap();
    let selected = before.metadata(1).clone();
    let directory = SegmentCatalog::segment_dir(&root, &selected.segment_id);
    let node = fixture.node.clone();
    let source = fixture.source.clone();
    let scratch = fixture.root.join("proof");
    let epoch = before.epoch();
    let error = tokio::task::spawn_blocking(move || {
        let writer = node.clone();
        let history = source.clone();
        crate::node::AFTER_MAINTENANCE_VERIFIED.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                writer
                    .publish_document_projection_blocking(&history, INDEX, &candidate)
                    .unwrap();
            }));
        });
        node.publish_document_maintenance_blocking(
            &source,
            INDEX,
            StatsClaim::required(active.stats_epoch, &active.stats_incarnation).unwrap(),
            epoch,
            vec![SegmentSource {
                segment_id: "stale-output",
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
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(fixture.node.ingest_fence().is_none());
    assert_eq!(
        fixture
            .source
            .index_publication_decision(INDEX, 3)
            .unwrap()
            .unwrap()
            .source
            .unwrap()
            .version,
        3
    );
    assert!(!SegmentCatalog::segment_dir(&root, "stale-output").exists());
    let after = OpenedSegmentSet::open(&root).unwrap();
    assert_eq!(after.epoch(), before.epoch() + 1);
    assert_eq!(after.bm25(2).document_identity(0).unwrap().version, 3);
    assert!(fixture
        .source
        .index_maintenance_decision(INDEX, after.epoch())
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn cancelling_embedded_awaiter_keeps_rewrite_files_and_finishes_the_owned_operation() {
    use crate::embedded::{
        EmbeddedDocumentCatalogConfig, EmbeddedSearch, EmbeddedSearchConfig, EmbeddedShardConfig,
    };
    let mut fixture = Fixture::new();
    publish(&fixture, 1, 3).await;
    publish(&fixture, 2, 2).await;
    let mut config = EmbeddedSearchConfig::single(EmbeddedShardConfig {
        node: fixture.node.config.clone(),
        allow_missing_bm25: false,
    });
    config.document_catalog = Some(EmbeddedDocumentCatalogConfig {
        collection: "books".into(),
        path: Some(fixture.root.join("source.redb")),
    });
    let source = std::mem::replace(
        &mut fixture.source,
        Arc::new(DocumentCatalog::in_memory("books").unwrap()),
    );
    drop(source);
    let runtime = Arc::new(EmbeddedSearch::open(config).await.unwrap());
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();
    crate::node::set_rewrite_test_hooks(
        crate::node::segments_root(fixture.node.config.index_path.as_ref().unwrap()),
        Box::new(move || {
            let _ = entered_tx.send(());
            release_rx.recv().unwrap();
        }),
        Box::new(move || {
            let _ = finished_tx.send(());
        }),
    );
    let worker = runtime.clone();
    let task = tokio::spawn(async move {
        worker
            .compact_document_index(
                0,
                CompactDocumentIndexRequest {
                    index_key: INDEX.to_vec(),
                    batch_rows: 1,
                    batch_bytes: 1024 * 1024,
                    max_staged_bytes: 32 * 1024 * 1024,
                    proof_batch_rows: 1,
                },
            )
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(10), entered_rx)
        .await
        .unwrap()
        .unwrap();
    let private_count = || {
        std::fs::read_dir(&fixture.root)
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".document-rewrite-")
            })
            .count()
    };
    assert_eq!(private_count(), 1);
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(private_count(), 1);
    // The builder held no serving fence while assembling its private output.
    assert_eq!(
        runtime
            .search(SearchRequest {
                k: 2,
                vector: vec![0.25; 8],
                ..Default::default()
            })
            .await
            .unwrap()
            .hits
            .len(),
        2
    );
    release_tx.send(()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10), finished_rx)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(private_count(), 0);
    let maintained = runtime
        .recover_document_maintenance(0, INDEX.to_vec())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(maintained.accepted_sequence, 2);
    assert_eq!(maintained.live_rows, 2);
    assert_eq!(
        runtime
            .search(SearchRequest {
                k: 2,
                vector: vec![0.25; 8],
                ..Default::default()
            })
            .await
            .unwrap()
            .hits
            .len(),
        2
    );
}

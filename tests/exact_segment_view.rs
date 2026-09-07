//! Sealed FP32 reads use the same physical row space as the segment catalog.
use pipestream_search::analyzer::{analyze_document_native, body_spec};
use pipestream_search::exact_vectors::ExactVectorStore;
use pipestream_search::harness::{fit_calibration, seeded_index, unit_vectors};
use pipestream_search::live_docs::LiveDocs;
use pipestream_search::node::{
    exact_vector_sidecar_path, segments_root, NodeConfig, NodeServiceImpl,
};
use pipestream_search::pb::node_service_server::NodeService;
use pipestream_search::pb::*;
use pipestream_search::postings::Bm25Store;
use pipestream_search::segments::{SegmentCatalog, SegmentSource};
use pipestream_search::vector::EMBEDDED_TURBOVEC;
use std::path::PathBuf;
use tonic::Request;

const DIM: usize = 16;
struct Fixture {
    directory: PathBuf,
    index: PathBuf,
    vectors: Vec<f32>,
}
impl Fixture {
    fn new(gap: bool) -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let directory = std::env::temp_dir().join(format!(
            "exact-segment-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).unwrap();
        let index = directory.join("index");
        let vectors = unit_vectors(8, DIM, 731);
        let (shift, scale) = fit_calibration(DIM, 4, &vectors);
        let catalog = SegmentCatalog::open(segments_root(&index)).unwrap();
        for part in 0..3 {
            let stage = directory.join(format!("stage-{part}"));
            std::fs::create_dir_all(&stage).unwrap();
            let bm25 = stage.join("documents.bm25");
            let live = stage.join("live.bin");
            let image = stage.join("vector.index");
            let exact = stage.join("vectors.f32");
            let mut documents = Bm25Store::with_fields(&["body"]);
            for row in 0..2 {
                documents.add_document(
                    row,
                    "word".into(),
                    analyze_document_native("word", Some(&body_spec())).unwrap(),
                );
            }
            documents.save(&bm25).unwrap();
            LiveDocs::default().write(&live, 2).unwrap();
            let has_vectors = !gap || part != 1;
            if has_vectors {
                let mut provider = seeded_index(DIM, 4, &shift, &scale);
                provider
                    .add(&vectors[part * 2 * DIM..(part + 1) * 2 * DIM], DIM)
                    .unwrap();
                provider.prepare().unwrap();
                provider.write(&image).unwrap();
                ExactVectorStore::from_values(
                    DIM,
                    vectors[part * 2 * DIM..(part + 1) * 2 * DIM].to_vec(),
                )
                .unwrap()
                .write(&exact)
                .unwrap();
            }
            catalog
                .append(SegmentSource {
                    segment_id: &format!("part-{part}"),
                    generation: part as u64 + 1,
                    base_label: (part * 2) as u64,
                    backend_kind: if has_vectors { EMBEDDED_TURBOVEC } else { "" },
                    vector_path: has_vectors.then_some(image.as_path()),
                    exact_vector_path: has_vectors.then_some(exact.as_path()),
                    bm25_path: &bm25,
                    live_docs_path: &live,
                    partition_column: None,
                })
                .unwrap();
        }
        Self {
            directory,
            index,
            vectors,
        }
    }
    fn config(&self) -> NodeConfig {
        NodeConfig {
            index_path: Some(self.index.clone()),
            wal: false,
            ..Default::default()
        }
    }
    async fn scores(&self, node: &NodeServiceImpl) -> ExactVectorRescoreResponse {
        node.exact_vector_rescore(Request::new(ExactVectorRescoreRequest {
            vector: self.vectors[..DIM].to_vec(),
            candidate_ids: (0..6).collect(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner()
    }
    fn expected(&self, slots: &[usize]) -> Vec<(u64, f32)> {
        ExactVectorStore::from_values(DIM, self.vectors.clone())
            .unwrap()
            .score_slots(&self.vectors[..DIM], slots)
            .unwrap()
            .into_iter()
            .map(|(slot, score)| (slot as u64, score))
            .collect()
    }
}

#[tokio::test]
async fn reopen_preserves_document_only_gaps_without_writing_a_sidecar() {
    let fixture = Fixture::new(true);
    let node = NodeServiceImpl::open(fixture.config(), None, false)
        .unwrap_or_else(|error| panic!("a catalog with vector gaps must reopen: {error}"));
    let response = fixture.scores(&node).await;
    assert_eq!(
        response
            .hits
            .iter()
            .map(|hit| (hit.doc_id, hit.score))
            .collect::<Vec<_>>(),
        fixture.expected(&[0, 1, 4, 5])
    );
    assert_eq!(response.logical_bytes, 4 * DIM as u64 * 4);
    let limited = node
        .exact_vector_rescore(Request::new(ExactVectorRescoreRequest {
            vector: fixture.vectors[..DIM].to_vec(),
            candidate_ids: (0..6).collect(),
            max_logical_bytes: 4 * DIM as u64 * 4,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(limited.hits, response.hits);
    assert!(!exact_vector_sidecar_path(&fixture.index).exists());
}

#[tokio::test]
async fn a_same_sized_stale_sidecar_cannot_replace_sealed_vector_values() {
    let fixture = Fixture::new(false);
    let path = exact_vector_sidecar_path(&fixture.index);
    ExactVectorStore::from_values(DIM, vec![0.0; 6 * DIM])
        .unwrap()
        .write(&path)
        .unwrap();
    let node = NodeServiceImpl::open(fixture.config(), None, false).unwrap();
    let response = fixture.scores(&node).await;
    assert_eq!(
        response
            .hits
            .iter()
            .map(|hit| (hit.doc_id, hit.score))
            .collect::<Vec<_>>(),
        fixture.expected(&[0, 1, 2, 3, 4, 5])
    );
    drop(node);
    std::fs::write(&path, b"obsolete sidecar").unwrap();
    let reopened = NodeServiceImpl::open(fixture.config(), None, false).unwrap();
    assert_eq!(fixture.scores(&reopened).await.hits, response.hits);
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

#[test]
fn held_views_share_images_across_retirements_and_keep_gaps_during_appends() {
    use pipestream_search::segments::SegmentRowRetirement;
    let fixture = Fixture::new(true);
    let catalog = SegmentCatalog::open(segments_root(&fixture.index)).unwrap();
    let before = catalog.snapshot();
    let mut exact = ExactVectorStore::from_segments(&before, DIM).unwrap();
    assert_eq!(exact.len(), 6);
    assert_eq!(
        (0..6)
            .filter(|&row| exact.contains_row(row))
            .collect::<Vec<_>>(),
        [0, 1, 4, 5]
    );
    assert!(exact.row_values(1, 5).is_err());
    assert!(exact.write(&fixture.directory.join("dense.f32")).is_err());
    assert!(!fixture.directory.join("dense.f32").exists());
    let after = catalog
        .commit_rows(
            before.epoch(),
            &[SegmentRowRetirement {
                segment_id: "part-0".into(),
                rows: vec![0],
            }],
            vec![],
        )
        .unwrap();
    assert!(std::sync::Arc::ptr_eq(
        before.exact_vectors(0).unwrap(),
        after.exact_vectors(0).unwrap()
    ));
    assert!(!before.live_docs(0).is_deleted(0));
    assert!(after.live_docs(0).is_deleted(0));
    let mut overlay = LiveDocs::default();
    overlay.delete(5);
    let held_overlay = overlay.clone();
    after.merge_tombstones(&mut overlay).unwrap();
    assert!(overlay.is_deleted(0) && overlay.is_deleted(5));
    assert!(!held_overlay.is_deleted(0) && held_overlay.is_deleted(5));
    // Presence is storage metadata; the serving view applies its own tombstones.
    assert!(exact.contains_row(0));
    exact.append(&fixture.vectors[6 * DIM..], DIM).unwrap();
    assert_eq!(exact.len(), 8);
    let slots = [7, 2, 4, 6, 0, 3, 7];
    assert_eq!(
        exact
            .score_slots(&fixture.vectors[..DIM], &slots)
            .unwrap()
            .into_iter()
            .map(|(row, score)| (row as u64, score))
            .collect::<Vec<_>>(),
        fixture.expected(&[7, 4, 6, 0, 7])
    );
    assert_eq!(exact.row_values(5, 8).unwrap(), fixture.vectors[5 * DIM..]);
    assert_eq!(before.exact_vectors(2).unwrap().len(), 2);
    exact.verify_payload().unwrap();
    let many: Vec<usize> = slots.into_iter().cycle().take(2100).collect();
    let scored = exact
        .score_slots_profiled(&fixture.vectors[..DIM], &many, 4)
        .unwrap();
    assert!(scored.tasks > 1);
    assert_eq!(
        scored.pages_touched, 2,
        "equal offsets in different mapped files are different pages"
    );
    let present: Vec<usize> = many
        .into_iter()
        .filter(|row| ![2, 3].contains(row))
        .collect();
    assert_eq!(
        scored
            .rows
            .into_iter()
            .map(|(row, score)| (row as u64, score))
            .collect::<Vec<_>>(),
        fixture.expected(&present)
    );
}

#[tokio::test]
async fn sparse_flush_export_install_and_reopen_keep_exact_scores() {
    let fixture = Fixture::new(true);
    let node = NodeServiceImpl::open(fixture.config(), None, false).unwrap();
    let before = fixture.scores(&node).await;
    node.flush_index().unwrap();
    assert!(!exact_vector_sidecar_path(&fixture.index).exists());
    let repository = fixture.directory.join("snapshot");
    let exported = node.export_snapshot_blocking(&repository).unwrap();
    assert!(!repository.join("vectors.f32").exists());
    let installed_path = fixture.directory.join("installed");
    let receiver = NodeServiceImpl::open(
        NodeConfig {
            index_path: Some(installed_path.clone()),
            wal: false,
            ..Default::default()
        },
        None,
        false,
    )
    .unwrap();
    receiver
        .install_snapshot_from(Request::new(InstallSnapshotFromRequest {
            source: Some(install_snapshot_from_request::Source::Directory(
                repository.display().to_string(),
            )),
            expected_manifest_sha256: exported.manifest_sha256,
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(fixture.scores(&receiver).await.hits, before.hits);
    drop(receiver);
    let reopened = NodeServiceImpl::open(
        NodeConfig {
            index_path: Some(installed_path.clone()),
            wal: false,
            ..Default::default()
        },
        None,
        false,
    )
    .unwrap();
    assert_eq!(fixture.scores(&reopened).await.hits, before.hits);
    assert!(!exact_vector_sidecar_path(&installed_path).exists());
}

#[test]
fn mismatched_exact_dimensions_cannot_enter_a_catalog() {
    let fixture = Fixture::new(false);
    let catalog = SegmentCatalog::open(segments_root(&fixture.index)).unwrap();
    let before = catalog.snapshot();
    let stage = fixture.directory.join("stage-0");
    let path = stage.join("wrong-dim.f32");
    ExactVectorStore::from_values(DIM + 1, vec![0.0; 2 * (DIM + 1)])
        .unwrap()
        .write(&path)
        .unwrap();
    let error = catalog
        .append(SegmentSource {
            segment_id: "wrong-dim",
            generation: 4,
            base_label: 6,
            backend_kind: EMBEDDED_TURBOVEC,
            vector_path: Some(&stage.join("vector.index")),
            exact_vector_path: Some(&path),
            bm25_path: &stage.join("documents.bm25"),
            live_docs_path: &stage.join("live.bin"),
            partition_column: None,
        })
        .unwrap_err();
    assert!(error.contains("exact-vector dimension"), "{error}");
    assert_eq!(catalog.snapshot().epoch(), before.epoch());
    assert_eq!(catalog.snapshot().len(), before.len());
}

#[tokio::test]
async fn sealed_retirements_remain_hidden_on_reopen_and_snapshot_install() {
    use pipestream_search::segments::SegmentRowRetirement;
    let fixture = Fixture::new(true);
    let catalog = SegmentCatalog::open(segments_root(&fixture.index)).unwrap();
    catalog
        .commit_rows(
            catalog.snapshot().epoch(),
            &[SegmentRowRetirement {
                segment_id: "part-0".into(),
                rows: vec![0],
            }],
            vec![],
        )
        .unwrap();
    let node = NodeServiceImpl::open(fixture.config(), None, false).unwrap();
    let scores = fixture.scores(&node).await;
    assert_eq!(
        scores.hits.iter().map(|hit| hit.doc_id).collect::<Vec<_>>(),
        [1, 4, 5]
    );
    let repository = fixture.directory.join("retired-snapshot");
    node.export_snapshot_blocking(&repository).unwrap();
    // The catalog alone must carry its tombstones, even without the optional
    // generation-wide overlay supplied by a current exporter.
    let (mut manifest, _) =
        pipestream_search::snapshot_repository::read_manifest(&repository).unwrap();
    manifest
        .artifacts
        .retain(|artifact| artifact.file != "live-docs.bin");
    let (_, digest) =
        pipestream_search::snapshot_repository::write_manifest(&repository, &manifest).unwrap();
    let receiver = NodeServiceImpl::open(
        NodeConfig {
            index_path: Some(fixture.directory.join("receiver")),
            wal: false,
            ..Default::default()
        },
        None,
        false,
    )
    .unwrap();
    receiver
        .install_snapshot_from(Request::new(InstallSnapshotFromRequest {
            source: Some(install_snapshot_from_request::Source::Directory(
                repository.display().to_string(),
            )),
            expected_manifest_sha256: digest,
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(fixture.scores(&receiver).await.hits, scores.hits);
}

#[tokio::test]
async fn an_empty_configured_catalog_does_not_reuse_retired_sidecar_rows() {
    let fixture = Fixture::new(false);
    let index = fixture.directory.join("empty");
    let vectors = &fixture.vectors;
    let (shift, scale) = fit_calibration(DIM, 4, vectors);
    let empty = seeded_index(DIM, 4, &shift, &scale);
    empty.write(&index).unwrap();
    let catalog = SegmentCatalog::open(segments_root(&index)).unwrap();
    catalog.commit_current(1).unwrap();
    ExactVectorStore::from_values(DIM, vectors.clone())
        .unwrap()
        .write(&exact_vector_sidecar_path(&index))
        .unwrap();
    let node = NodeServiceImpl::open(
        NodeConfig {
            index_path: Some(index.clone()),
            wal: false,
            ..Default::default()
        },
        None,
        false,
    )
    .unwrap();
    let health = node
        .health(Request::new(HealthRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(health.num_vectors, 0);
    assert_eq!(health.exact_vector_rows, 0);
    let path = fixture.directory.join("empty-snapshot");
    node.export_snapshot_blocking(&path).unwrap();
    assert!(!path.join("vectors.f32").exists());
}

#[test]
fn publication_switches_sealed_rows_and_read_version_together() {
    use pipestream_search::segments::{OpenedSegmentSet, SegmentRowRetirement};
    use pipestream_search::stats_identity::StatsClaim;
    let fixture = Fixture::new(true);
    let node = NodeServiceImpl::open(fixture.config(), None, false).unwrap();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let before = runtime.block_on(fixture.scores(&node));
    let claim = StatsClaim::required(before.stats_epoch, &before.stats_incarnation).unwrap();
    let root = segments_root(&fixture.index);
    let held = OpenedSegmentSet::open(&root).unwrap();
    let stage = fixture.directory.join("stage-0");
    let next = node
        .publish_segment_rows_blocking(
            claim,
            held.epoch(),
            &[SegmentRowRetirement {
                segment_id: "part-0".into(),
                rows: vec![0],
            }],
            vec![SegmentSource {
                segment_id: "replacement",
                generation: held.epoch() + 1,
                base_label: 6,
                backend_kind: EMBEDDED_TURBOVEC,
                vector_path: Some(&stage.join("vector.index")),
                exact_vector_path: Some(&stage.join("vectors.f32")),
                bm25_path: &stage.join("documents.bm25"),
                live_docs_path: &stage.join("live.bin"),
                partition_column: None,
            }],
        )
        .unwrap();
    assert_eq!(next.epoch, claim.epoch + 1);
    assert_eq!(next.incarnation(), claim.incarnation());
    let scores = runtime
        .block_on(
            node.exact_vector_rescore(Request::new(ExactVectorRescoreRequest {
                vector: fixture.vectors[..DIM].to_vec(),
                candidate_ids: (0..8).collect(),
                expected_stats_epoch: next.epoch,
                expected_stats_incarnation: next.incarnation(),
                ..Default::default()
            })),
        )
        .unwrap()
        .into_inner();
    assert_eq!(
        scores.hits.iter().map(|hit| hit.doc_id).collect::<Vec<_>>(),
        [1, 4, 5, 6, 7]
    );
    assert_eq!(scores.hits[3].score, before.hits[0].score);
    assert_eq!(scores.hits[4].score, before.hits[1].score);
    let lexical = runtime
        .block_on(node.browse_shard(Request::new(BrowseShardRequest {
            k: 20,
            first_page: true,
            lexical_terms: vec!["word".into()],
            expected_stats_epoch: next.epoch,
            expected_stats_incarnation: next.incarnation(),
            ..Default::default()
        })))
        .unwrap()
        .into_inner();
    assert_eq!(lexical.doc_ids, [1, 2, 3, 4, 5, 6, 7]);
    assert!(!held.live_docs(0).is_deleted(0));
    let stale = runtime
        .block_on(
            node.exact_vector_rescore(Request::new(ExactVectorRescoreRequest {
                vector: fixture.vectors[..DIM].to_vec(),
                candidate_ids: vec![0, 6],
                expected_stats_epoch: claim.epoch,
                expected_stats_incarnation: claim.incarnation(),
                ..Default::default()
            })),
        )
        .unwrap_err();
    assert_eq!(stale.code(), tonic::Code::FailedPrecondition);
    assert!(node
        .publish_segment_rows_blocking(claim, held.epoch(), &[], vec![])
        .is_err());
    drop(node);
    let reopened = NodeServiceImpl::open(fixture.config(), None, false).unwrap();
    let restored = runtime
        .block_on(
            reopened.exact_vector_rescore(Request::new(ExactVectorRescoreRequest {
                vector: fixture.vectors[..DIM].to_vec(),
                candidate_ids: (0..8).collect(),
                ..Default::default()
            })),
        )
        .unwrap()
        .into_inner();
    assert_eq!(restored.hits, scores.hits);
}

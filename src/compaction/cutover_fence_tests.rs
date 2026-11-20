//! Exercise WAL replay across live catalog advancement and rejected cutovers.

use super::super::super::{marker_path, PreflightKind};
use super::*;
use crate::analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND};
use crate::node::NodeConfig;
use crate::pb::{AddDocumentsRequest, CompactShardRequest};
use crate::postings::AnalyzedDoc;
use crate::vector::EMBEDDED_TURBOVEC;

struct Fixture {
    node: NodeServiceImpl,
    config: NodeConfig,
    directory: std::path::PathBuf,
    analyzed: Vec<AnalyzedDoc>,
    vector: Vec<f32>,
}

impl Fixture {
    fn new() -> Self {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "wal-catalog-cutover-{}-{nonce}",
            std::process::id()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let sample = crate::harness::unit_vectors(32, 64, 19);
        let backend = VectorIndex::fit_backend_config(EMBEDDED_TURBOVEC, 64, 4, &sample).unwrap();
        let index = VectorIndex::from_backend_config(64, &backend).unwrap();
        let config = NodeConfig {
            index_path: Some(directory.join("shard.vector")),
            analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
            wal: true,
            ..Default::default()
        };
        let node = NodeServiceImpl::new(Some(index), config.clone());
        let analyzed = ["seed document", "replayed tail"]
            .iter()
            .map(|text| {
                tokio::runtime::Handle::current()
                    .block_on(crate::analyzer::analyze_document(
                        NATIVE_ANALYSIS_BACKEND,
                        text,
                        Some(&body_spec()),
                    ))
                    .unwrap()
            })
            .collect();
        Self {
            node,
            config,
            directory,
            analyzed,
            vector: sample[..64].to_vec(),
        }
    }

    fn append(&self, row: usize) {
        let mut guard = self.node.state.write().unwrap();
        self.node
            .apply_document_locked(
                &mut guard,
                AddDocumentsRequest {
                    text: ["seed document", "replayed tail"][row].into(),
                    analysis: Some(body_spec()),
                    ..Default::default()
                },
                self.analyzed[row].clone(),
                Some(self.vector.clone()),
                None,
                &mut 0,
                &mut 0,
            )
            .unwrap();
    }

    fn texts(node: &NodeServiceImpl) -> Vec<String> {
        use crate::pb::node_service_server::NodeService;
        tokio::runtime::Handle::current()
            .block_on(
                node.get_documents(tonic::Request::new(crate::pb::GetDocumentsRequest {
                    doc_ids: vec![0, 1],
                })),
            )
            .unwrap()
            .into_inner()
            .documents
            .into_iter()
            .map(|doc| doc.text)
            .collect()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

#[derive(Clone, Copy)]
enum Change {
    LiveSeal,
    Superseded,
    SharedPublication,
    Uncertain,
}

fn advance_live_catalog(fixture: &Fixture, change: Change) {
    fixture.node.seal_tail().unwrap();
    let root = crate::node::segments_root(fixture.config.index_path.as_ref().unwrap());
    match change {
        Change::LiveSeal => {}
        Change::Superseded => {
            let other = SegmentCatalog::open(root).unwrap();
            other.commit_current(other.snapshot().epoch() + 1).unwrap();
        }
        Change::SharedPublication => {
            let held = fixture.node.state.read().unwrap();
            let Some(Bm25Shard::Segmented(shard)) = held.bm25.as_ref() else {
                panic!("segments")
            };
            let shared = shard.catalog().clone();
            shared.commit_current(shard.snapshot().epoch() + 1).unwrap();
        }
        Change::Uncertain => {
            let held = fixture.node.state.read().unwrap();
            let Some(Bm25Shard::Segmented(shard)) = held.bm25.as_ref() else {
                panic!("segments")
            };
            crate::segments::FAIL_SET_SYNC.with(|fail| fail.set(true));
            assert!(shard
                .catalog()
                .commit_current(shard.snapshot().epoch() + 1)
                .is_err());
        }
    }
}

fn run_compaction(change: Change) {
    let fixture = Fixture::new();
    fixture.append(0);
    fixture.node.flush_index().unwrap();
    let PreflightKind::Wal(pre) = fixture
        .node
        .preflight_at_row_boundary(&CompactShardRequest::default())
        .unwrap()
    else {
        panic!("WAL preflight")
    };
    std::fs::create_dir_all(&pre.work_dir).unwrap();
    let mut calls = 0;
    let mut before_cutover = None;
    let mut analyze = |docs: &[(
        &str,
        Option<&crate::pb::AnalysisSpec>,
        crate::analyzer::SessionLayers,
    )]| {
        calls += 1;
        if calls == 1 {
            fixture.append(1);
        }
        if calls == 2 {
            advance_live_catalog(&fixture, change);
            before_cutover = Some(
                std::fs::read(SegmentCatalog::manifest_path(&crate::node::segments_root(
                    &pre.index_path,
                )))
                .unwrap(),
            );
        }
        tokio::runtime::Handle::current()
            .block_on(crate::analyzer::analyze_batch_streams(
                NATIVE_ANALYSIS_BACKEND,
                docs,
                2,
            ))
            .map_err(|error| error.to_string())
    };
    let result = fixture.node.build_and_cut_over(&pre, 256, &mut analyze);
    assert_eq!(calls, 2, "second pass replays the row after shadow open");
    verify_cutover(&fixture, &pre, change, result, before_cutover);
}

fn verify_cutover(
    fixture: &Fixture,
    pre: &Preflight,
    change: Change,
    result: Result<crate::pb::CompactShardResponse, Status>,
    before_cutover: Option<Vec<u8>>,
) {
    match change {
        Change::LiveSeal => {
            assert_eq!(result.unwrap().rows_after, 2);
            assert_retired_seals_removed(pre, before_cutover.as_deref().unwrap());
            assert_eq!(
                fixture
                    .node
                    .state
                    .read()
                    .unwrap()
                    .wal
                    .as_ref()
                    .unwrap()
                    .generation(),
                1
            );
            let reopened = NodeServiceImpl::open(fixture.config.clone(), None).unwrap();
            assert_eq!(
                Fixture::texts(&reopened),
                ["seed document", "replayed tail"]
            );
        }
        Change::Superseded | Change::SharedPublication | Change::Uncertain => {
            assert_eq!(result.unwrap_err().code(), tonic::Code::Aborted);
            assert!(!marker_path(&pre.index_path).exists());
            assert!(!wal::gen_dir(&wal::wal_dir(&pre.index_path), 1).exists());
            assert_eq!(
                fixture
                    .node
                    .state
                    .read()
                    .unwrap()
                    .wal
                    .as_ref()
                    .unwrap()
                    .generation(),
                0
            );
            assert_eq!(
                std::fs::read(SegmentCatalog::manifest_path(&crate::node::segments_root(
                    &pre.index_path
                )))
                .unwrap(),
                before_cutover.unwrap()
            );
        }
    }
    assert_eq!(
        Fixture::texts(&fixture.node),
        ["seed document", "replayed tail"]
    );
}

fn assert_retired_seals_removed(pre: &Preflight, before_cutover: &[u8]) {
    use std::collections::BTreeSet;

    let root = crate::node::segments_root(&pre.index_path);
    let before: crate::segments::SegmentSetManifest =
        serde_json::from_slice(before_cutover).unwrap();
    assert_eq!(
        before.segments.len(),
        2,
        "the tail sealed after shadow open"
    );
    let current = SegmentCatalog::read_manifest(&root).unwrap().unwrap();
    let expected: BTreeSet<_> = current
        .segments
        .iter()
        .map(|segment| segment.segment_id.clone())
        .collect();
    for retired in &before.segments {
        assert!(
            !expected.contains(&retired.segment_id),
            "old segment remains published"
        );
    }
    let actual: BTreeSet<_> = std::fs::read_dir(root.join("segments"))
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            assert!(entry.file_type().unwrap().is_dir());
            entry.file_name().into_string().unwrap()
        })
        .collect();
    assert_eq!(
        actual, expected,
        "closing Flush must retire every replaced seal"
    );
    assert!(!marker_path(&pre.index_path).exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn caught_up_wal_shadow_accepts_intervening_live_seal() {
    tokio::task::spawn_blocking(|| run_compaction(Change::LiveSeal))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn superseded_serving_manifest_refuses_before_cutover_io() {
    tokio::task::spawn_blocking(|| run_compaction(Change::Superseded))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn uncertain_serving_manifest_refuses_before_cutover_io() {
    tokio::task::spawn_blocking(|| run_compaction(Change::Uncertain))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unserved_shared_catalog_publication_refuses_before_cutover_io() {
    tokio::task::spawn_blocking(|| run_compaction(Change::SharedPublication))
        .await
        .unwrap();
}

#[path = "pending_snapshot_tests.rs"]
mod pending_snapshot;

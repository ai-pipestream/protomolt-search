//! Deferred closing-flush regressions: a tail whose documents arrived
//! before their vectors must neither seal documents-only nor persist a
//! live overlay claiming rows the catalog has not sealed.

use super::*;
use crate::analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND};
use crate::live_docs::LiveDocs;
use crate::pb::{AddDocumentsRequest, AddVectorsRequest};
use crate::vector::EMBEDDED_TURBOVEC;

const DIM: usize = 64;

struct Fixture {
    node: NodeServiceImpl,
    config: NodeConfig,
    directory: PathBuf,
    vector: Vec<f32>,
}

impl Fixture {
    fn new() -> Self {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "seal-flush-deferred-{}-{nonce}",
            std::process::id()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let sample = crate::harness::unit_vectors(32, DIM, 19);
        let backend = VectorIndex::fit_backend_config(EMBEDDED_TURBOVEC, DIM, 4, &sample).unwrap();
        let index = VectorIndex::from_backend_config(DIM, &backend).unwrap();
        let config = NodeConfig {
            index_path: Some(directory.join("shard.vector")),
            analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
            wal: true,
            ..Default::default()
        };
        Self {
            node: NodeServiceImpl::new(Some(index), config.clone()),
            config,
            vector: sample[..DIM].to_vec(),
            directory,
        }
    }

    async fn append(&self, text: &str, vector: Option<Vec<f32>>) {
        let analyzed =
            crate::analyzer::analyze_document(NATIVE_ANALYSIS_BACKEND, text, Some(&body_spec()))
                .await
                .unwrap();
        let mut guard = write_shard(&self.node.state);
        self.node
            .apply_document_locked(
                &mut guard,
                AddDocumentsRequest {
                    text: text.into(),
                    analysis: Some(body_spec()),
                    ..Default::default()
                },
                analyzed,
                vector,
                None,
                &mut 0,
                &mut 0,
            )
            .unwrap();
    }

    fn sealed_rows(&self) -> u64 {
        let root = segments_root(self.config.index_path.as_ref().unwrap());
        let manifest = crate::segments::SegmentCatalog::read_manifest(&root)
            .unwrap()
            .expect("a flushed shard has a catalog");
        manifest.segments.iter().map(|s| s.rows).sum()
    }

    /// Drop the node without removing its directory: a crash, after
    /// which the caller reopens from the same path and cleans up.
    fn crash(mut self) -> PathBuf {
        let directory = self.directory.clone();
        self.directory = directory.join("already-gone");
        directory
    }

    fn live_rows(&self) -> u64 {
        let path = live_docs_sidecar_path(self.config.index_path.as_ref().unwrap());
        LiveDocs::open(&path).unwrap().persisted_rows()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deferred_closing_flush_keeps_live_overlay_at_the_sealed_prefix() {
    let fixture = Fixture::new();
    fixture
        .append("aligned seed", Some(fixture.vector.clone()))
        .await;
    fixture.node.flush_index().unwrap();
    assert_eq!(fixture.sealed_rows(), 1);
    // A document whose vector has not arrived: the misaligned tail.
    fixture.append("documents first", None).await;
    {
        let mut guard = write_shard(&fixture.node.state);
        assert!(guard.live_docs.delete(1), "tail tombstone recorded");
    }
    // The closing flush must defer, not seal the document alone.
    fixture.node.flush_closing_cutover().unwrap();
    assert_eq!(fixture.sealed_rows(), 1, "deferred tail must not seal");
    // The persisted overlay must claim only the sealed prefix, while the
    // unsealed tombstone stays live in memory.
    assert_eq!(fixture.live_rows(), 1);
    {
        let guard = read_shard(&fixture.node.state);
        assert!(guard.live_docs.is_deleted(1), "tail tombstone retained");
    }
    // Simulated crash: drop without another flush; reopen must not brick.
    let config = fixture.config.clone();
    let directory = fixture.crash();
    let reopened = NodeServiceImpl::open(config, None).unwrap();
    assert_eq!(physical_rows(&read_shard(&reopened.state)), 1);
    drop(reopened);
    let _ = std::fs::remove_dir_all(&directory);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vectors_can_follow_a_deferred_tail_and_then_seal() {
    let fixture = Fixture::new();
    fixture
        .append("aligned seed", Some(fixture.vector.clone()))
        .await;
    fixture.node.flush_index().unwrap();
    fixture.append("documents first", None).await;
    fixture.node.flush_closing_cutover().unwrap();
    assert_eq!(fixture.sealed_rows(), 1);
    // The vector half arrives after the deferred closing flush; it must
    // land on the waiting document instead of refusing against a
    // documents-only segment.
    let (added, _) = fixture
        .node
        .apply_batch(
            AddVectorsRequest {
                materialized_rows: Vec::new(),
                vectors: fixture.vector.clone(),
                dim: DIM as u32,
            },
            None,
        )
        .unwrap();
    assert_eq!(added, 1);
    fixture.node.flush_index().unwrap();
    assert_eq!(fixture.sealed_rows(), 2);
    assert_eq!(fixture.live_rows(), 2);
    let config = fixture.config.clone();
    let directory = fixture.crash();
    let reopened = NodeServiceImpl::open(config, None).unwrap();
    assert_eq!(physical_rows(&read_shard(&reopened.state)), 2);
    drop(reopened);
    let _ = std::fs::remove_dir_all(&directory);
}

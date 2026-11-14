#![cfg(feature = "net")]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use pipestream_search::analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND};
use pipestream_search::link::NodeLink;
use pipestream_search::node::{Layout, NodeConfig, NodeServiceImpl};
use pipestream_search::pb::{
    AddDocumentsRequest, AddVectorsRequest, CompactShardRequest, DeleteDocumentsRequest,
    FlushRequest,
};
use pipestream_search::turbo_bridge::MultiResVectorOutput;

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
            "retained-lifecycle-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn config(path: &Path, layout: Layout) -> NodeConfig {
    NodeConfig {
        index_path: Some(path.to_path_buf()),
        layout,
        wal: true,
        analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
        matryoshka_prefix_dims: vec![8, 16],
        matryoshka_active_dim: Some(8),
        ..Default::default()
    }
}

fn raw(row: usize) -> Vec<f32> {
    let mut values = vec![0.0; 16];
    values[0] = 0.6 + row as f32 * 0.03;
    values[8] = 0.8 - row as f32 * 0.02;
    values[9] = -0.0;
    values
}

async fn seed(cfg: &NodeConfig) -> Arc<NodeServiceImpl> {
    let index = pipestream_search::harness::seeded_index(8, 4, &[0.0; 8], &[1.0; 8]);
    let node = Arc::new(NodeServiceImpl::new(Some(index), cfg.clone()));
    let mut link = NodeLink::local(Arc::clone(&node));
    link.add_vectors(tokio_stream::iter([AddVectorsRequest {
        vectors: (0..3).flat_map(raw).collect(),
        dim: 16,
        ..Default::default()
    }]))
    .await
    .unwrap();
    link.add_documents(tokio_stream::iter((0..3).map(|i| AddDocumentsRequest {
        text: format!("retained document {i}"),
        analysis: Some(body_spec()),
        ..Default::default()
    })))
    .await
    .unwrap();
    node
}

fn equal_bits(a: &MultiResVectorOutput, b: &MultiResVectorOutput) {
    let bits = |v: &[f32]| v.iter().map(|v| v.to_bits()).collect::<Vec<_>>();
    assert_eq!(bits(&a.full), bits(&b.full));
    assert_eq!(a.inv_norm.to_bits(), b.inv_norm.to_bits());
    assert_eq!(a.sha256, b.sha256);
    assert_eq!(a.quantized_codes, b.quantized_codes);
    assert_eq!(
        a.prefixes
            .iter()
            .map(|(d, v)| (*d, bits(v)))
            .collect::<Vec<_>>(),
        b.prefixes
            .iter()
            .map(|(d, v)| (*d, bits(v)))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        a.sentence_vectors
            .iter()
            .map(|v| bits(v))
            .collect::<Vec<_>>(),
        b.sentence_vectors
            .iter()
            .map(|v| bits(v))
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn both_layouts_preserve_tier_bits_and_tombstones_after_flush_reopen() {
    for layout in [Layout::SingleImage, Layout::Segments] {
        let dir = Directory::new();
        let cfg = config(&dir.0.join("index"), layout);
        let node = seed(&cfg).await;
        let before: Vec<_> = (0..3).map(|i| node.multi_res_row(i).unwrap()).collect();
        let mut link = NodeLink::local(Arc::clone(&node));
        link.delete_documents(DeleteDocumentsRequest {
            doc_ids: vec![1],
            expected_wal_generation: Some(0),
        })
        .await
        .unwrap();
        let scores = node
            .rescore_matryoshka_tier(16, &raw(0), &[0, 1, 2])
            .unwrap();
        assert_eq!(scores.len(), 2);
        link.flush(FlushRequest {}).await.unwrap();
        drop(link);
        drop(node);
        let reopened = NodeServiceImpl::open(cfg, None, false).unwrap();
        for (i, row) in before.iter().enumerate() {
            equal_bits(row, &reopened.multi_res_row(i as u32).unwrap());
        }
        assert_eq!(
            scores,
            reopened
                .rescore_matryoshka_tier(16, &raw(0), &[0, 1, 2])
                .unwrap()
        );
    }
}

#[tokio::test]
async fn missing_declared_payload_refuses_before_wal_reconciliation() {
    let dir = Directory::new();
    let path = dir.0.join("index");
    let cfg = config(&path, Layout::SingleImage);
    let node = seed(&cfg).await;
    let mut link = NodeLink::local(Arc::clone(&node));
    link.flush(FlushRequest {}).await.unwrap();
    drop(link);
    drop(node);
    let gen = pipestream_search::wal::latest_gen(&pipestream_search::wal::wal_dir(&path))
        .unwrap()
        .unwrap()
        .1;
    let before: Vec<_> = std::fs::read_dir(&gen)
        .unwrap()
        .map(|entry| {
            let path = entry.unwrap().path();
            (path.clone(), std::fs::read(path).unwrap())
        })
        .collect();
    std::fs::remove_file(dir.0.join("index.multi")).unwrap();
    let error = NodeServiceImpl::open(cfg, None, false)
        .err()
        .expect("missing retained payload was accepted");
    assert!(error.contains("retained vectors"), "{error}");
    for (path, bytes) in before {
        assert_eq!(bytes, std::fs::read(path).unwrap());
    }
}

#[tokio::test]
async fn repository_install_replaces_retained_rows_instead_of_reusing_old_slots() {
    for layout in [Layout::SingleImage, Layout::Segments] {
        let dir = Directory::new();
        let source = seed(&config(&dir.0.join("source"), layout)).await;
        let expected = source.multi_res_row(0).unwrap();
        let repository = dir.0.join("repository");
        source.export_snapshot_blocking(&repository).unwrap();
        let manifest = pipestream_search::snapshot_repository::RepositoryManifest::parse(
            &std::fs::read(repository.join("snapshot-manifest.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(manifest.format_version, 2);
        let cfg = config(&dir.0.join("target"), layout);
        let target = seed(&cfg).await;
        let mut target_link = NodeLink::local(Arc::clone(&target));
        target_link
            .add_vectors(tokio_stream::iter([AddVectorsRequest {
                vectors: raw(3),
                dim: 16,
                ..Default::default()
            }]))
            .await
            .unwrap();
        assert!(target.multi_res_row(3).is_some());
        drop(target_link);
        target
            .install_staged_repository(&repository, &manifest)
            .unwrap();
        equal_bits(&expected, &target.multi_res_row(0).unwrap());
        assert!(target.multi_res_row(3).is_none());
        drop(target);
        let reopened = NodeServiceImpl::open(cfg, None, false).unwrap();
        equal_bits(&expected, &reopened.multi_res_row(0).unwrap());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn online_compaction_remaps_full_tiers_and_keeps_them_after_reopen() {
    for layout in [Layout::SingleImage, Layout::Segments] {
        let dir = Directory::new();
        let cfg = config(&dir.0.join("index"), layout);
        let node = seed(&cfg).await;
        let expected = [
            node.multi_res_row(0).unwrap(),
            node.multi_res_row(2).unwrap(),
        ];
        let mut link = NodeLink::local(Arc::clone(&node));
        link.delete_documents(DeleteDocumentsRequest {
            doc_ids: vec![1],
            expected_wal_generation: Some(0),
        })
        .await
        .unwrap();
        link.flush(FlushRequest {}).await.unwrap();
        let work = Arc::clone(&node);
        let result = tokio::task::spawn_blocking(move || {
            work.compact_shard(&CompactShardRequest::default())
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(result.rows_after, 2);
        link.flush(FlushRequest {}).await.unwrap();
        drop(link);
        drop(node);
        let reopened = NodeServiceImpl::open(cfg, None, false).unwrap();
        for (i, row) in expected.iter().enumerate() {
            equal_bits(row, &reopened.multi_res_row(i as u32).unwrap());
        }
        assert!(reopened.multi_res_row(2).is_none());
    }
}

#[tokio::test]
async fn prefix_only_configuration_reopens_the_full_active_dimension() {
    let dir = Directory::new();
    let mut cfg = config(&dir.0.join("index"), Layout::SingleImage);
    cfg.matryoshka_prefix_dims = vec![8];
    cfg.matryoshka_active_dim = None;
    let index = pipestream_search::harness::seeded_index(16, 4, &[0.0; 16], &[1.0; 16]);
    let node = Arc::new(NodeServiceImpl::new(Some(index), cfg.clone()));
    let mut link = NodeLink::local(Arc::clone(&node));
    link.add_vectors(tokio_stream::iter([AddVectorsRequest {
        vectors: raw(0),
        dim: 16,
        ..Default::default()
    }]))
    .await
    .unwrap();
    let before = node.multi_res_row(0).unwrap();
    link.flush(FlushRequest {}).await.unwrap();
    drop(link);
    drop(node);
    let reopened = NodeServiceImpl::open(cfg, None, true).unwrap();
    equal_bits(&before, &reopened.multi_res_row(0).unwrap());
    assert_eq!(
        reopened
            .rescore_matryoshka_tier(8, &raw(0), &[0])
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn streamed_snapshot_keeps_retention_across_chunk_boundaries() {
    use pipestream_search::pb::{snapshot_chunk, SnapshotChunk, SnapshotManifest};
    let dir = Directory::new();
    let source = seed(&config(&dir.0.join("source"), Layout::SingleImage)).await;
    let expected = source.multi_res_row(2).unwrap();
    let repository = dir.0.join("repository");
    source.export_snapshot_blocking(&repository).unwrap();
    let files = [
        "vector.index",
        "vectors.f32",
        "documents.bm25",
        "live-docs.bin",
        "vectors.multi",
    ]
    .map(|name| std::fs::read(repository.join(name)).unwrap());
    let manifest = SnapshotManifest {
        vector_bytes: files[0].len() as u64,
        exact_vector_bytes: files[1].len() as u64,
        bm25_bytes: files[2].len() as u64,
        live_docs_bytes: files[3].len() as u64,
        multi_res_vector_bytes: files[4].len() as u64,
        multi_res_declaration: std::fs::read(repository.join("vectors.multi.json")).unwrap(),
    };
    let bytes: Vec<_> = files.into_iter().flatten().collect();
    let mut chunks = vec![SnapshotChunk {
        payload: Some(snapshot_chunk::Payload::Manifest(manifest)),
    }];
    chunks.extend(bytes.chunks(17).map(|data| SnapshotChunk {
        payload: Some(snapshot_chunk::Payload::Data(data.to_vec())),
    }));
    let cfg = config(&dir.0.join("target"), Layout::SingleImage);
    let target = Arc::new(NodeServiceImpl::new(None, cfg.clone()));
    let mut link = NodeLink::local(Arc::clone(&target));
    link.install_snapshot(tokio_stream::iter(chunks))
        .await
        .unwrap();
    equal_bits(&expected, &target.multi_res_row(2).unwrap());
    drop(link);
    drop(target);
    let reopened = NodeServiceImpl::open(cfg, None, false).unwrap();
    equal_bits(&expected, &reopened.multi_res_row(2).unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wal_replication_preserves_materialized_bits_and_retry_is_idempotent() {
    use pipestream_search::replication::{sync_once, ReplicaCursor};
    let dir = Directory::new();
    let source = seed(&config(&dir.0.join("source"), Layout::SingleImage)).await;
    let expected: Vec<_> = (0..3)
        .map(|row| source.multi_res_row(row).unwrap())
        .collect();
    NodeLink::local(Arc::clone(&source))
        .flush(FlushRequest {})
        .await
        .unwrap();
    let cfg = config(&dir.0.join("target"), Layout::SingleImage);
    let index = pipestream_search::harness::seeded_index(8, 4, &[0.0; 8], &[1.0; 8]);
    let target = NodeServiceImpl::new(Some(index), cfg.clone());
    let target_view = target.clone();
    let (primary, source_task) = pipestream_search::harness::serve_node((*source).clone()).await;
    let (replica, target_task) = pipestream_search::harness::serve_node(target).await;
    let cursor = sync_once(&ReplicaCursor {
        primary,
        replica,
        ..Default::default()
    })
    .await
    .unwrap();
    let retried = sync_once(&cursor).await.unwrap();
    assert_eq!(cursor, retried);
    for (row, before) in expected.iter().enumerate() {
        equal_bits(before, &target_view.multi_res_row(row as u32).unwrap());
    }
    assert!(target_view.multi_res_row(3).is_none());
    source_task.abort();
    target_task.abort();
    let _ = source_task.await;
    let _ = target_task.await;
    drop(target_view);
    let reopened = NodeServiceImpl::open(cfg, None, false).unwrap();
    for (row, before) in expected.iter().enumerate() {
        equal_bits(before, &reopened.multi_res_row(row as u32).unwrap());
    }
}

#[tokio::test]
async fn wal_disabled_retention_has_an_old_reader_gate_and_cannot_lose_its_declaration() {
    let dir = Directory::new();
    let path = dir.0.join("index");
    let mut cfg = config(&path, Layout::SingleImage);
    cfg.wal = false;
    let node = seed(&cfg).await;
    let before = node.multi_res_row(0).unwrap();
    let mut link = NodeLink::local(Arc::clone(&node));
    link.flush(FlushRequest {}).await.unwrap();
    drop(link); drop(node);
    let live = std::fs::read(pipestream_search::node::live_docs_sidecar_path(&path)).unwrap();
    assert_eq!(u32::from_le_bytes(live[8..12].try_into().unwrap()), 2);
    let reopened = NodeServiceImpl::open(cfg.clone(), None, false).unwrap();
    equal_bits(&before, &reopened.multi_res_row(0).unwrap());
    drop(reopened);
    let live_path = pipestream_search::node::live_docs_sidecar_path(&path);
    std::fs::remove_file(&live_path).unwrap();
    let error = NodeServiceImpl::open(cfg.clone(), None, false).err().expect("missing reader gate accepted");
    assert!(error.contains("requires its live-doc reader gate"), "{error}");
    let mut legacy = live.clone();
    legacy[8..12].copy_from_slice(&1u32.to_le_bytes());
    let crc = pipestream_search::wal::crc32(&legacy[..72]);
    legacy[72..76].copy_from_slice(&crc.to_le_bytes());
    std::fs::write(&live_path, &legacy).unwrap();
    let error = NodeServiceImpl::open(cfg.clone(), None, false).err().expect("downgraded reader gate accepted");
    assert!(error.contains("requires its live-doc reader gate"), "{error}");
    assert_eq!(legacy, std::fs::read(&live_path).unwrap());
    std::fs::write(&live_path, &live).unwrap();
    std::fs::remove_file(dir.0.join("index.multi")).unwrap();
    std::fs::remove_file(dir.0.join("index.multi.json")).unwrap();
    // Replicas can receive retained tiers with no local projection config;
    // the persisted reader gate must still require the declared artifact.
    cfg.matryoshka_prefix_dims.clear();
    cfg.matryoshka_active_dim = None;
    let error = NodeServiceImpl::open(cfg, None, false).err().expect("missing retention accepted");
    assert!(error.contains("requires a retained vector declaration"), "{error}");
    assert_eq!(live, std::fs::read(pipestream_search::node::live_docs_sidecar_path(&path)).unwrap());
}

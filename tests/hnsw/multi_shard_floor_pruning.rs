//! Multi-shard live-floor pruning, underfilled shard handling, and stream deduplication.

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn floor_prune_multi_shard_prunes_cold_shard_handles_underfilled_and_dedupes_stream() {
    use pipestream_search::coordinator::RequestFilters;
    use pipestream_search::harness::serve_node;
    use pipestream_search::hnsw::{BuildMode, HnswIndex};
    use pipestream_search::node::NodeServiceImpl;
    use pipestream_search::vector::{
        FilterRegimeCounts, VectorBackendConfig, VectorBackendDescriptor, VectorError,
        VectorProvider, VectorSearchOptions, VectorSearchResults, VectorStreamBatch,
        VectorStreamControl, VectorStreamEndReason, VectorStreamSummary,
    };
    use std::ops::Range;
    use std::path::Path;
    use std::time::Duration;

    const LOCAL_DIM: usize = 16;
    const K: u32 = 32;

    let mut q = vec![0.0_f32; LOCAL_DIM];
    q[0] = 1.0;

    let shared_cfg = HnswConfig {
        m: 12,
        ef_construction: 64,
        build_mode: BuildMode::FixedOrder,
        seed: 0x7200,
        ..HnswConfig::default()
    };
    let backend_cfg = shared_cfg.backend_config().unwrap();

    let make_hnsw_shard = |vectors: &[f32], offset: u64, delay_ms: u64| {
        let mut index = VectorIndex::from_backend_config(LOCAL_DIM, &backend_cfg).unwrap();
        if !vectors.is_empty() {
            index.add(vectors, LOCAL_DIM).unwrap();
        }
        index.prepare().unwrap();
        let fingerprint = index.descriptor().scoring_fingerprint;
        let mut node = NodeServiceImpl::new(
            Some(index),
            NodeConfig {
                slot_offset: offset,
                ..Default::default()
            },
        );
        if delay_ms > 0 {
            node = node.with_stream_poll_delay(Duration::from_millis(delay_ms));
        }
        (node, fingerprint)
    };

    // Shard 0 (hot, 256 rows): scores in ~[2.5, 5.05].
    let mut hot = unit_vectors(256, LOCAL_DIM, 0x7201);
    for r in 0..256 {
        hot[r * LOCAL_DIM] = 2.5 + (r as f32) * 0.01;
    }
    // Shard 1 (cold, 512 rows, delayed 2 ms/hop so Shard 0's first-flush floor
    // arrives early in Shard 1's layer-0 walk): negative scores in [-5.06, -2.5].
    let mut cold = unit_vectors(512, LOCAL_DIM, 0x7202);
    for r in 0..512 {
        cold[r * LOCAL_DIM] = -2.5 - (r as f32) * 0.005;
    }
    // Shard 2 (empty participant, 0 rows).
    let (node_hot, _) = make_hnsw_shard(&hot, 0, 0);
    let (node_cold, _) = make_hnsw_shard(&cold, 1_000, 2);
    let (node_empty, _) = make_hnsw_shard(&[], 2_000, 0);

    let (addr_hot, h0) = serve_node(node_hot).await;
    let (addr_cold, h1) = serve_node(node_cold).await;
    let (addr_empty, h2) = serve_node(node_empty).await;
    let addrs = vec![addr_hot, addr_cold, addr_empty];

    let coordinator = CoordinatorServiceImpl::new(addrs.clone()).with_stream_search(true);

    // 1. Baseline (`floor_prune = None`) vs `gateK` (`g = 0`) vs `clamp` (`g = 500_000`):
    //    exact top-K matches, cold shard prunes sub-floor candidates, empty shard completes.
    let base = coordinator
        .fanout_stream_search("fp-off", &q, K, None, &RequestFilters::default())
        .await
        .unwrap();
    assert_eq!(base.hits.len(), K as usize);
    assert_eq!(base.shards_floor_pruned, 0);

    for greediness in [0u32, 500_000u32] {
        let pruned = coordinator
            .fanout_stream_search(
                "fp-on",
                &q,
                K,
                None,
                &RequestFilters {
                    floor_prune: Some(true),
                    floor_k: Some(K),
                    floor_greediness_ppm: Some(greediness),
                    floor_clamp_min: Some(8),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(pruned.hits.len(), K as usize, "g={greediness}");
        assert_eq!(
            pruned.hits.iter().map(|h| h.vector_id).collect::<Vec<_>>(),
            base.hits.iter().map(|h| h.vector_id).collect::<Vec<_>>(),
            "g={greediness}"
        );
        assert!(
            pruned.shards_floor_pruned >= 1,
            "expected cold shard to prune candidates at g={greediness}"
        );
        assert!(
            pruned.summaries[1].blocks_scanned < base.summaries[1].blocks_scanned,
            "cold shard hops should drop under floor pruning (g={greediness}): got {} vs {}",
            pruned.summaries[1].blocks_scanned,
            base.summaries[1].blocks_scanned
        );
        assert!(pruned.summaries[2].completed);
        assert_eq!(pruned.summaries[2].emitted, 0);
    }

    // 2. Underfilled `< k` corpus (`k = 2_000` > 768 total rows): no floor is
    //    published (`floors_sent == 0`) and every live row is returned.
    let underfilled = coordinator
        .fanout_stream_search(
            "fp-underfilled",
            &q,
            2_000,
            None,
            &RequestFilters {
                floor_prune: Some(true),
                floor_k: Some(2_000),
                floor_greediness_ppm: Some(500_000),
                floor_clamp_min: Some(8),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(underfilled.floors_sent, 0);
    assert_eq!(underfilled.shards_floor_pruned, 0);
    assert_eq!(underfilled.hits.len(), 256 + 512);

    h0.abort();
    h1.abort();
    h2.abort();

    // 4. Duplicate `(shard, vector_id)` stream emissions cannot manufacture `k`
    //    heap entries or publish a bogus floor that drops another shard's hits.
    struct DupStreamProvider {
        inner: HnswIndex,
        spam_copies: usize,
        spam_score: f32,
    }

    impl VectorProvider for DupStreamProvider {
        fn descriptor(&self) -> VectorBackendDescriptor {
            self.inner.descriptor()
        }
        fn backend_config(&self) -> Result<VectorBackendConfig, VectorError> {
            self.inner.backend_config()
        }
        fn len(&self) -> usize {
            self.inner.len()
        }
        fn dimension(&self) -> Option<usize> {
            self.inner.dimension()
        }
        fn add(&mut self, vectors: &[f32], dimension: usize) -> Result<(), VectorError> {
            self.inner.add(vectors, dimension)
        }
        fn prepare(&mut self) -> Result<(), VectorError> {
            self.inner.prepare()
        }
        fn write(&self, path: &Path) -> Result<(), VectorError> {
            self.inner.write(path)
        }
        fn search(
            &self,
            queries: &[f32],
            k: usize,
            options: VectorSearchOptions<'_>,
        ) -> Result<VectorSearchResults, VectorError> {
            self.inner.search(queries, k, options)
        }
        fn search_streaming_controlled(
            &self,
            _queries: &[f32],
            _options: VectorSearchOptions<'_>,
            sink: &mut dyn FnMut(&VectorStreamBatch<'_>) -> VectorStreamControl,
            _control: &mut dyn FnMut() -> VectorStreamControl,
        ) -> Result<VectorStreamSummary, VectorError> {
            let scores = vec![self.spam_score; self.spam_copies];
            let slots = vec![0i64; self.spam_copies];
            let _ = sink(&VectorStreamBatch {
                query_index: 0,
                block_base: 0,
                scores: &scores,
                slots: &slots,
            });
            Ok(VectorStreamSummary {
                query_count: 1,
                emitted: self.spam_copies,
                units_scanned: 1,
                completed: true,
                end_reason: VectorStreamEndReason::Exhausted,
                parts: Default::default(),
                regimes: FilterRegimeCounts::default(),
                tail_enumerated_rows: 0,
                patience_stopped: 0,
                floor_pruned: 0,
            })
        }
        fn row_transcripts(
            &self,
            rows: Range<usize>,
            sink: &mut dyn FnMut(usize, &[u8]),
        ) -> Result<(), VectorError> {
            self.inner.row_transcripts(rows, sink)
        }
    }

    let mut dup_inner = HnswIndex::new(LOCAL_DIM, shared_cfg).unwrap();
    dup_inner
        .add(&unit_vectors(1, LOCAL_DIM, 0x7298), LOCAL_DIM)
        .unwrap();
    dup_inner.prepare().unwrap();
    let dup_node = NodeServiceImpl::new(
        Some(VectorIndex::from_provider(DupStreamProvider {
            inner: dup_inner,
            spam_copies: 8,
            spam_score: 100.0,
        })),
        NodeConfig {
            slot_offset: 42,
            ..Default::default()
        },
    );
    let (dup_addr, dup_handle) = serve_node(dup_node).await;
    let (slow_real, _) = make_hnsw_shard(&unit_vectors(256, LOCAL_DIM, 0x7299), 5_000, 5);
    let (slow_addr, slow_handle) = serve_node(slow_real).await;

    let dedup_coord =
        CoordinatorServiceImpl::new(vec![dup_addr, slow_addr]).with_stream_search(true);
    let dedup_res = dedup_coord
        .fanout_stream_search(
            "dedup-stream",
            &q,
            4,
            None,
            &RequestFilters {
                floor_prune: Some(true),
                floor_k: Some(4),
                floor_greediness_ppm: Some(1_000_000),
                floor_clamp_min: Some(1),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    // Without deduplication, 8 copies of `(42, 100.0)` would fill the k=4 heap
    // with 4 copies of id 42 and publish a floor ~100.0, starving `slow_real`!
    assert_eq!(dedup_res.hits.len(), 4);
    assert_eq!(
        dedup_res.hits.iter().filter(|h| h.vector_id == 42).count(),
        1,
        "duplicate (shard, vector_id) emissions must be deduplicated in heap"
    );
    assert_eq!(
        dedup_res
            .hits
            .iter()
            .filter(|h| h.vector_id >= 5_000)
            .count(),
        3,
        "real lower-scoring hits from shard 1 must survive duplicate spam on shard 0"
    );

    dup_handle.abort();
    slow_handle.abort();
}

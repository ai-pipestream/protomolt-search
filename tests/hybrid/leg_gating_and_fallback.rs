//! Zero-weight leg elision, vector score floors, and two-level RRF fallback.

use super::*;

/// Leg disabling and the vector-score floor through the HybridSearch
/// handler: an explicit weight of 0 turns a leg off (vector-only order
/// == the plain Search RPC's order; bm25-only surfaces only matching
/// docs), disabling both legs or a TWO_LEVEL leg is rejected, and
/// min_vector_score drops every hit below the floor BEFORE fusion in
/// every mode (deeper qualifying docs get promoted, not truncated).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leg_disabling_and_vector_floor() {
    use pipestream_search::pb::search_service_server::SearchService;
    use pipestream_search::pb::{HybridLegOptions, HybridSearchRequest, SearchRequest};

    let (analysis, mock) = start_mock_analysis().await;
    let corpus = unit_vectors(2 * SHARD_DOCS, DIM, 0x8888_0001);
    let (shift, scale) = fit_calibration(DIM, 4, &corpus);
    let mut texts: Vec<String> = (0..2 * SHARD_DOCS)
        .map(|i| format!("plain document number {i} about nothing special"))
        .collect();
    texts[0] = "zebra stripes everywhere".to_string();
    texts[5] = "another zebra crossing".to_string();

    let mut addrs = Vec::new();
    let mut handles = Vec::new();
    for shard in 0..2usize {
        let start = shard * SHARD_DOCS;
        let vecs = corpus[start * DIM..(start + SHARD_DOCS) * DIM].to_vec();
        let (addr, handle) = start_hybrid_shard(
            &analysis,
            (shard * SHARD_DOCS) as u64,
            &texts[start..start + SHARD_DOCS],
            vecs,
            &shift,
            &scale,
        )
        .await;
        addrs.push(addr);
        handles.push(handle);
    }
    let coordinator =
        CoordinatorServiceImpl::new(addrs).with_bm25(Some(analysis), Default::default());
    let query = corpus[..DIM].to_vec();
    let request = |legs: HybridLegOptions| {
        tonic::Request::new(HybridSearchRequest {
            request_id: String::new(),
            text: "zebra".to_string(),
            vector: query.clone(),
            k: 8,
            analysis: None,
            legs: Some(legs),
            debug: false,
            boost: None,
            ..Default::default()
        })
    };
    let global_rank = HybridLegOptions {
        fusion_mode: pipestream_search::pb::FusionMode::GlobalRank as i32,
        ..Default::default()
    };

    // Vector-only: the fused order must be exactly the Search RPC's.
    let vector_only = coordinator
        .hybrid_search(request(HybridLegOptions {
            bm25_weight: Some(0.0),
            ..global_rank
        }))
        .await
        .unwrap()
        .into_inner();
    let plain = coordinator
        .search(tonic::Request::new(SearchRequest {
            request_id: String::new(),
            k: 8,
            vector: query.clone(),
            collapse_parents: false,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        ids(&vector_only.hits),
        plain.hits.iter().map(|h| h.vector_id).collect::<Vec<_>>(),
        "vector-only hybrid must reproduce the plain vector order"
    );
    assert!(vector_only.hits.iter().all(|h| h.bm25_rank.is_none()));

    // BM25-only: exactly the zebra docs, no vector provenance.
    let bm25_only = coordinator
        .hybrid_search(request(HybridLegOptions {
            vector_weight: Some(0.0),
            ..global_rank
        }))
        .await
        .unwrap()
        .into_inner();
    let mut bm25_ids = ids(&bm25_only.hits);
    bm25_ids.sort_unstable();
    assert_eq!(bm25_ids, vec![0, 5], "bm25-only surfaces only matches");
    assert!(bm25_only.hits.iter().all(|h| h.vector_rank.is_none()));

    // Both legs off, or a TWO_LEVEL leg off: rejected.
    let err = coordinator
        .hybrid_search(request(HybridLegOptions {
            vector_weight: Some(0.0),
            bm25_weight: Some(0.0),
            ..global_rank
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    let err = coordinator
        .hybrid_search(request(HybridLegOptions {
            fusion_mode: pipestream_search::pb::FusionMode::TwoLevel as i32,
            bm25_weight: Some(0.0),
            ..global_rank
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    // Vector floor: pick the 3rd-best vector score as the floor; every
    // mode must then return exactly the docs at or above it, with the
    // bm25-strong-but-vector-weak docs gone unless they qualify.
    let scores: Vec<f32> = plain.hits.iter().map(|h| h.score).collect();
    let floor = scores[2];
    let qualifying: Vec<u64> = plain
        .hits
        .iter()
        .filter(|h| h.score >= floor)
        .map(|h| h.vector_id)
        .collect();
    for mode in [
        pipestream_search::pb::FusionMode::GlobalRank,
        pipestream_search::pb::FusionMode::ScoreBlend,
        pipestream_search::pb::FusionMode::TwoLevel,
    ] {
        let filtered = coordinator
            .hybrid_search(request(HybridLegOptions {
                fusion_mode: mode as i32,
                min_vector_score: floor,
                ..global_rank
            }))
            .await
            .unwrap()
            .into_inner();
        let mut got = ids(&filtered.hits);
        got.sort_unstable();
        let mut want = qualifying.clone();
        want.sort_unstable();
        assert_eq!(
            got, want,
            "{mode:?}: floor must keep exactly the qualifying docs"
        );
        assert!(filtered
            .hits
            .iter()
            .all(|h| h.vector_rank.is_some() && h.vector_score >= floor));
    }
    // Cascade: same floor, applied to the phase-1 pool.
    let cascade = coordinator
        .hybrid_search(request(HybridLegOptions {
            fusion_mode: pipestream_search::pb::FusionMode::Cascade as i32,
            min_vector_score: floor,
            ..global_rank
        }))
        .await
        .unwrap()
        .into_inner();
    let mut got = cascade
        .cascade_hits
        .iter()
        .map(|h| h.doc_id)
        .collect::<Vec<_>>();
    got.sort_unstable();
    let mut want = qualifying;
    want.sort_unstable();
    assert_eq!(
        got, want,
        "cascade floor must keep exactly the qualifying docs"
    );
    assert!(cascade.cascade_hits.iter().all(|h| h.vector_score >= floor));

    for h in handles {
        h.abort();
    }
    mock.abort();
}

/// The two-level fallback stays reachable and deterministic under
/// FUSION_MODE_TWO_LEVEL, with shard-local (compressed) ranks in the
/// provenance — its documented, non-partition-independent semantics.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_level_fallback_is_reachable_and_deterministic() {
    let (analysis, mock) = start_mock_analysis().await;
    let corpus = unit_vectors(2 * SHARD_DOCS, DIM, 0x4444_0001);
    let (shift, scale) = fit_calibration(DIM, 4, &corpus);
    let mut texts: Vec<String> = (0..2 * SHARD_DOCS)
        .map(|i| format!("plain document number {i}"))
        .collect();
    texts[0] = "zebra stripes".to_string();

    let mut addrs = Vec::new();
    let mut handles = Vec::new();
    for shard in 0..2usize {
        let start = shard * SHARD_DOCS;
        let vecs = corpus[start * DIM..(start + SHARD_DOCS) * DIM].to_vec();
        let (addr, handle) = start_hybrid_shard(
            &analysis,
            (shard * SHARD_DOCS) as u64,
            &texts[start..start + SHARD_DOCS],
            vecs,
            &shift,
            &scale,
        )
        .await;
        addrs.push(addr);
        handles.push(handle);
    }
    let coordinator =
        CoordinatorServiceImpl::new(addrs).with_bm25(Some(analysis), Default::default());
    let query = corpus[..DIM].to_vec();

    let first = common::fanout::hybrid(&coordinator, "t1", "zebra", &query, 8, legs_two_level())
        .await
        .unwrap()
        .0;
    let second = common::fanout::hybrid(&coordinator, "t2", "zebra", &query, 8, legs_two_level())
        .await
        .unwrap()
        .0;
    assert_eq!(ids(&first), ids(&second));
    // Doc 0 (both legs on its shard) wins; provenance is shard-local.
    assert_eq!(first[0].doc_id, 0);
    assert_eq!(first[0].vector_rank, Some(1));
    assert_eq!(first[0].bm25_rank, Some(1));

    for h in handles {
        h.abort();
    }
    mock.abort();
}

/// A leg disabled by a zero weight must not be SCANNED, not merely
/// discarded at fusion time.
///
/// Both fusion functions skip a zero-weight leg, so results were always
/// correct and this looked settled. But the shard has no weight field to
/// read: it gates the vector scan on a non-empty `vector` and the BM25
/// scan on non-empty `terms`, so the coordinator sending both payloads
/// bought a full scan whose output was then thrown away. On the
/// 86.6M-chunk fleet a `vector_weight: 0` query still paid ~320 ms of
/// vector scan out of ~340 ms total — "bm25-only" cost 20x the lexical
/// leg's own 17 ms.
///
/// Result equality is asserted by the leg-disabling test above; what is
/// under test here is the per-shard hit counts in the debug block, which
/// report what the shard actually produced.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_zero_weight_leg_is_not_scanned_by_the_shard() {
    let (analysis, mock) = start_mock_analysis().await;
    let corpus = unit_vectors(2 * SHARD_DOCS, DIM, 0x1111_000a);
    let (shift, scale) = fit_calibration(DIM, 4, &corpus);
    let mut texts: Vec<String> = (0..2 * SHARD_DOCS)
        .map(|i| format!("plain document number {i} about nothing special"))
        .collect();
    texts[0] = "zebra stripes everywhere".to_string();
    texts[5] = "another zebra crossing".to_string();

    let mut addrs = Vec::new();
    let mut handles = Vec::new();
    for shard in 0..2usize {
        let start = shard * SHARD_DOCS;
        let vecs = corpus[start * DIM..(start + SHARD_DOCS) * DIM].to_vec();
        let (addr, handle) = start_hybrid_shard(
            &analysis,
            (shard * SHARD_DOCS) as u64,
            &texts[start..start + SHARD_DOCS],
            vecs,
            &shift,
            &scale,
        )
        .await;
        addrs.push(addr);
        handles.push(handle);
    }
    let coordinator =
        CoordinatorServiceImpl::new(addrs).with_bm25(Some(analysis), Default::default());
    let query = corpus[..DIM].to_vec();

    // Control: both legs on, both legs report work.
    let (_, debug) =
        common::fanout::hybrid_debug(&coordinator, "both", "zebra", &query, 8, legs_default())
            .await
            .unwrap();
    let debug = debug.unwrap();
    assert!(
        debug.shards.iter().all(|s| s.vector_hits > 0),
        "control: every shard should run the vector leg"
    );
    assert!(
        debug.shards.iter().any(|s| s.bm25_hits > 0),
        "control: zebra should reach some shard's lexical leg"
    );

    // Vector leg off: no shard may report a single vector hit.
    let (_, debug) = common::fanout::hybrid_debug(
        &coordinator,
        "novec",
        "zebra",
        &query,
        8,
        HybridLegs {
            vector_weight: 0.0,
            ..legs_default()
        },
    )
    .await
    .unwrap();
    let debug = debug.unwrap();
    assert!(
        debug.shards.iter().all(|s| s.vector_hits == 0),
        "vector_weight 0 must not scan: {:?}",
        debug
            .shards
            .iter()
            .map(|s| s.vector_hits)
            .collect::<Vec<_>>()
    );
    assert!(
        debug.shards.iter().any(|s| s.bm25_hits > 0),
        "the surviving leg must still run"
    );

    // BM25 leg off: the mirror image.
    let (_, debug) = common::fanout::hybrid_debug(
        &coordinator,
        "nobm25",
        "zebra",
        &query,
        8,
        HybridLegs {
            bm25_weight: 0.0,
            ..legs_default()
        },
    )
    .await
    .unwrap();
    let debug = debug.unwrap();
    assert!(
        debug.shards.iter().all(|s| s.bm25_hits == 0),
        "bm25_weight 0 must not scan: {:?}",
        debug.shards.iter().map(|s| s.bm25_hits).collect::<Vec<_>>()
    );
    assert!(
        debug.shards.iter().all(|s| s.vector_hits > 0),
        "the surviving leg must still run"
    );

    mock.abort();
    for handle in handles {
        handle.abort();
    }
}

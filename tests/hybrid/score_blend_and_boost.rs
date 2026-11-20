//! Normalized score blending arithmetic and boost-window reranking.

use super::*;

/// SCORE_BLEND end to end: deterministic, provenance-carrying, and the
/// fused scores follow the documented normalize-and-combine arithmetic.
/// The corpus is built so the hand calculation is exact: doc 0 tops BOTH
/// legs (vector self-match + "zebra"), doc 5 is the only other BM25
/// match, so the BM25 retained set is {0, 5} — both at the same score
/// (identical tf and dl), a degenerate leg that min-max maps to 1.0.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn score_blend_follows_documented_arithmetic() {
    let (analysis, mock) = start_mock_analysis().await;
    let corpus = unit_vectors(2 * SHARD_DOCS, DIM, 0x6666_0001);
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
    let blend = legs_blend(Normalization::MinMax, Combination::Arithmetic);

    let first = common::fanout::hybrid(&coordinator, "b1", "zebra", &query, 8, blend)
        .await
        .unwrap()
        .0;
    let second = common::fanout::hybrid(&coordinator, "b2", "zebra", &query, 8, blend)
        .await
        .unwrap()
        .0;
    let sig = |hits: &[pipestream_search::pb::HybridHit]| {
        hits.iter()
            .map(|h| (h.doc_id, h.fused_score.to_bits()))
            .collect::<Vec<_>>()
    };
    assert_eq!(sig(&first), sig(&second), "blend must be deterministic");

    // Doc 0: normalized 1.0 in the vector leg (its own vector is the
    // max) and 1.0 in the degenerate BM25 leg -> fused (1+1)/2 = 1.0.
    let top = &first[0];
    assert_eq!(top.doc_id, 0);
    assert!(
        (top.fused_score - 1.0).abs() < 1e-6,
        "doc 0 blends to 1.0, got {}",
        top.fused_score
    );
    assert_eq!(top.vector_rank, Some(1));
    assert_eq!(top.bm25_rank, Some(1));
    assert!(top.vector_score > 0.0 && top.bm25_score > 0.0);

    // Vector-only docs blend to at most vector_weight/(2 weights) = 0.5.
    for h in first.iter().filter(|h| h.bm25_rank.is_none()) {
        assert!(
            h.fused_score <= 0.5 + 1e-6,
            "vector-only doc {} above the weight ceiling: {}",
            h.doc_id,
            h.fused_score
        );
    }

    // Weight sensitivity: bm25_weight 4 makes doc 5 (BM25 1.0, vector
    // weak) blend to at least 0.8 while every vector-only doc is capped
    // at 0.2 -> docs 0 and 5 must take the top two positions.
    let weighted = HybridLegs {
        bm25_weight: 4.0,
        ..blend
    };
    let boosted = common::fanout::hybrid(&coordinator, "b3", "zebra", &query, 8, weighted)
        .await
        .unwrap()
        .0;
    assert_eq!(boosted[0].doc_id, 0);
    assert_eq!(boosted[1].doc_id, 5, "high bm25_weight must lift doc 5");
    assert!(boosted[1].fused_score >= 0.8 - 1e-6);
    for h in boosted.iter().filter(|h| h.bm25_rank.is_none()) {
        assert!(h.fused_score <= 0.2 + 1e-6);
    }

    // The other normalizations and combinations stay deterministic and
    // keep the both-legs leader on top.
    for legs in [
        legs_blend(Normalization::ZScore, Combination::Arithmetic),
        legs_blend(Normalization::MinMax, Combination::Geometric),
        legs_blend(Normalization::MinMax, Combination::Harmonic),
    ] {
        let hits = common::fanout::hybrid(&coordinator, "bx", "zebra", &query, 8, legs)
            .await
            .unwrap()
            .0;
        let again = common::fanout::hybrid(&coordinator, "by", "zebra", &query, 8, legs)
            .await
            .unwrap()
            .0;
        assert_eq!(sig(&hits), sig(&again));
        assert_eq!(
            hits[0].doc_id, 0,
            "{:?}/{:?}",
            legs.normalization, legs.combination
        );
    }

    for h in handles {
        h.abort();
    }
    mock.abort();
}

/// Boost-rescore end to end through the HybridSearch handler: the top
/// `window` hits are rescored by base_weight*base + boost_weight*bm25
/// (boost terms, candidate-scoped) and reordered; hits outside the
/// window keep their order; cascade ranks are reassigned. The corpus
/// plants "quagga" on doc 3 only, so the boost lifts exactly one doc.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn boost_rescore_reorders_the_window() {
    use pipestream_search::pb::search_service_server::SearchService;
    use pipestream_search::pb::{BoostRescore, HybridLegOptions, HybridSearchRequest};

    let (analysis, mock) = start_mock_analysis().await;
    let corpus = unit_vectors(2 * SHARD_DOCS, DIM, 0x7777_0001);
    let (shift, scale) = fit_calibration(DIM, 4, &corpus);
    let mut texts: Vec<String> = (0..2 * SHARD_DOCS)
        .map(|i| format!("plain document number {i} about nothing special"))
        .collect();
    texts[0] = "zebra stripes everywhere".to_string();
    texts[5] = "another zebra crossing".to_string();
    texts[3] = "quagga herds roam free".to_string();

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
    let request = |boost: Option<BoostRescore>, legs: Option<HybridLegOptions>, debug: bool| {
        tonic::Request::new(HybridSearchRequest {
            request_id: String::new(),
            text: "zebra".to_string(),
            vector: query.clone(),
            k: 8,
            analysis: None,
            legs,
            debug,
            boost,
            ..Default::default()
        })
    };
    let global_rank = || {
        Some(HybridLegOptions {
            fusion_mode: pipestream_search::pb::FusionMode::GlobalRank as i32,
            ..Default::default()
        })
    };
    let boost = |window: u32| {
        Some(BoostRescore {
            text: "quagga".to_string(),
            window,
            base_weight: 0.0,
            boost_weight: 0.0,
        })
    };

    let baseline = coordinator
        .hybrid_search(request(None, global_rank(), false))
        .await
        .unwrap()
        .into_inner();
    // Doc 0 and 5 (both legs) lead; doc 3 is vector-only somewhere below.
    assert_eq!(baseline.hits[0].doc_id, 0);
    assert_eq!(baseline.hits[1].doc_id, 5);
    assert!(baseline.hits.iter().all(|h| h.boost_score == 0.0));

    // Full-window boost: doc 3 is the only quagga match, its boost BM25
    // dwarfs every RRF fused score, so it takes position 0 and everyone
    // else keeps the fused order behind it.
    let boosted = coordinator
        .hybrid_search(request(boost(0), global_rank(), false))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(boosted.hits[0].doc_id, 3, "boost must lift the quagga doc");
    assert!(boosted.hits[0].boost_score > 0.0);
    assert!(
        boosted
            .hits
            .iter()
            .filter(|h| h.doc_id != 3)
            .all(|h| h.boost_score == 0.0),
        "only doc 3 matches the boost terms"
    );
    let rest: Vec<u64> = boosted.hits.iter().skip(1).map(|h| h.doc_id).collect();
    let baseline_minus_3: Vec<u64> = baseline
        .hits
        .iter()
        .map(|h| h.doc_id)
        .filter(|&id| id != 3)
        .collect();
    assert_eq!(rest, baseline_minus_3, "non-boosted hits keep fused order");
    // The returned fields obey the documented formula (weights 1.0).
    let finals: Vec<f64> = boosted
        .hits
        .iter()
        .map(|h| f64::from(h.fused_score) + f64::from(h.boost_score))
        .collect();
    assert!(finals.windows(2).all(|w| w[0] >= w[1]), "{finals:?}");

    // A window of 2 excludes doc 3: nothing changes, no boost scores.
    let windowed = coordinator
        .hybrid_search(request(boost(2), global_rank(), false))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ids(&windowed.hits), ids(&baseline.hits));
    assert!(windowed.hits.iter().all(|h| h.boost_score == 0.0));

    // Debug: the boost pass is profiled and does not change results.
    let debugged = coordinator
        .hybrid_search(request(boost(0), global_rank(), true))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ids(&debugged.hits), ids(&boosted.hits));
    let dbg = debugged.debug.unwrap();
    assert_eq!(dbg.boost_terms, vec!["quagga".to_string()]);
    assert!(dbg.boost_ms > 0.0);
    assert!(dbg.total_ms >= dbg.boost_ms);

    // Cascade (legs absent): base is the phase-2 BM25 score; the boost
    // lifts doc 3 to rank 1 and ranks are reassigned sequentially.
    let cascade = coordinator
        .hybrid_search(request(boost(0), None, false))
        .await
        .unwrap()
        .into_inner();
    assert!(cascade.hits.is_empty());
    assert_eq!(cascade.cascade_hits[0].doc_id, 3);
    assert!(cascade.cascade_hits[0].boost_score > 0.0);
    for (i, hit) in cascade.cascade_hits.iter().enumerate() {
        assert_eq!(hit.rank, i as u32 + 1, "ranks reassigned after boost");
    }

    for h in handles {
        h.abort();
    }
    mock.abort();
}

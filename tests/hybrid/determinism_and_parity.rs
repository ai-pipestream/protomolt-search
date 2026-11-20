//! Hybrid RRF determinism, provenance, and distributed-vs-monolithic parity.

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hybrid_is_deterministic_and_carries_provenance() {
    let (analysis, mock) = start_mock_analysis().await;
    let corpus = unit_vectors(2 * SHARD_DOCS, DIM, 0x1111_0001);
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
    // Query vector: doc 0's own vector — doc 0 tops its vector leg.
    let query = corpus[..DIM].to_vec();

    let first = common::fanout::hybrid(&coordinator, "h1", "zebra", &query, 8, legs_default())
        .await
        .unwrap()
        .0;
    let second = common::fanout::hybrid(&coordinator, "h2", "zebra", &query, 8, legs_default())
        .await
        .unwrap()
        .0;
    assert_eq!(
        ids(&first),
        ids(&second),
        "fused output must be deterministic"
    );
    let sig = |hits: &[pipestream_search::pb::HybridHit]| {
        hits.iter()
            .map(|h| (h.doc_id, h.fused_score.to_bits()))
            .collect::<Vec<_>>()
    };
    assert_eq!(sig(&first), sig(&second));

    // Doc 0 is in BOTH legs (vector self-match + "zebra") and wins.
    let top = &first[0];
    assert_eq!(top.doc_id, 0);
    assert_eq!(top.vector_rank, Some(1));
    assert_eq!(top.bm25_rank, Some(1));
    assert!(top.vector_score > 0.0 && top.bm25_score > 0.0);

    // A vector-only hit carries no BM25 rank.
    let vector_only = first.iter().find(|h| h.bm25_rank.is_none());
    assert!(vector_only.is_some(), "expected single-leg hits");
    assert!(vector_only.unwrap().vector_rank.is_some());

    // Doc 5 (the other zebra doc, on shard 1) is in both legs too.
    let doc5 = first
        .iter()
        .find(|h| h.doc_id == 5)
        .expect("doc 5 in results");
    assert!(doc5.vector_rank.is_some() && doc5.bm25_rank.is_some());

    for h in handles {
        h.abort();
    }
    mock.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distributed_hybrid_matches_monolithic_on_partition_stable_corpus() {
    let (analysis, mock) = start_mock_analysis().await;
    let corpus = unit_vectors(N_DOCS, DIM, 0x2222_0001);
    let (shift, scale) = fit_calibration(DIM, 4, &corpus);

    // The vector query: doc 0's own vector. The monolithic vector leg
    // order O over all 12 docs determines the shard assignment below.
    let query = corpus[..DIM].to_vec();
    let mut mono_vectors = pipestream_search::harness::seeded_index(DIM, 4, &shift, &scale);
    mono_vectors.add(&corpus, DIM).unwrap();
    let order: Vec<usize> = mono_vectors
        .search_unfiltered(&query, N_DOCS)
        .indices_for_query(0)
        .iter()
        .map(|&i| i as usize)
        .collect();
    assert_eq!(order.len(), N_DOCS);
    assert_eq!(order[0], 0, "self-query must rank its own vector first");

    // Partition round-robin by the monolithic fused order: original doc
    // O[3j+i] becomes shard i's local doc j (global id 4i+j).
    let mut texts: Vec<String> = vec!["plain text document".to_string(); N_DOCS];
    texts[0] = "zebra stripes everywhere".to_string();

    let mut shard_texts: [Vec<String>; N_SHARDS] = Default::default();
    let mut shard_vectors: [Vec<f32>; N_SHARDS] = Default::default();
    for (i, texts_i) in shard_texts.iter_mut().enumerate() {
        for j in 0..SHARD_DOCS {
            let original = order[3 * j + i];
            texts_i.push(texts[original].clone());
            shard_vectors[i].extend_from_slice(&corpus[original * DIM..(original + 1) * DIM]);
        }
    }

    let mut addrs = Vec::new();
    let mut handles = Vec::new();
    for i in 0..N_SHARDS {
        let (addr, handle) = start_hybrid_shard(
            &analysis,
            (i * SHARD_DOCS) as u64,
            &shard_texts[i],
            std::mem::take(&mut shard_vectors[i]),
            &shift,
            &scale,
        )
        .await;
        addrs.push(addr);
        handles.push(handle);
    }

    // Monolithic: one node with every document and vector in corpus order.
    let (mono_addr, mono) =
        start_hybrid_shard(&analysis, 0, &texts, corpus.clone(), &shift, &scale).await;

    let distributed =
        CoordinatorServiceImpl::new(addrs).with_bm25(Some(analysis.clone()), Default::default());
    let monolithic =
        CoordinatorServiceImpl::new(vec![mono_addr]).with_bm25(Some(analysis), Default::default());

    let got = common::fanout::hybrid(
        &distributed,
        "d",
        "zebra",
        &query,
        N_DOCS as u32,
        legs_default(),
    )
    .await
    .unwrap()
    .0;
    let want = common::fanout::hybrid(
        &monolithic,
        "m",
        "zebra",
        &query,
        N_DOCS as u32,
        legs_default(),
    )
    .await
    .unwrap()
    .0;

    // Monolithic sanity: doc 0 (both legs) first, then the vector order.
    let want_originals: Vec<usize> = want
        .iter()
        .map(|h| {
            // global id on the monolithic node == corpus position
            h.doc_id as usize
        })
        .collect();
    assert_eq!(want_originals, order, "monolithic fused != expected order");

    // Exactness: the distributed id sequence must be the monolithic order
    // mapped through the round-robin layout: original O[p] sits at global
    // id SHARD_DOCS*(p%N_SHARDS) + p/N_SHARDS.
    let expected: Vec<u64> = (0..N_DOCS)
        .map(|p| (SHARD_DOCS * (p % N_SHARDS) + p / N_SHARDS) as u64)
        .collect();
    assert_eq!(
        ids(&got),
        expected,
        "distributed fused order != monolithic order mapped through the shard layout"
    );

    // Provenance survives the two levels: distributed top hit is doc 0
    // with both legs at rank 1.
    let top = &got[0];
    assert_eq!(top.doc_id, 0);
    assert_eq!(top.vector_rank, Some(1));
    assert_eq!(top.bm25_rank, Some(1));

    for h in handles {
        h.abort();
    }
    mono.abort();
    mock.abort();
}

/// ADVERSARIAL partition exactness: the shard layout is the worst case
/// for two-level fusion — the strongest vector docs are banded onto shard
/// 0, so shards 1 and 2 have LOCAL ranks that are heavily compressed
/// relative to the global ranks (their local rank 1 is globally rank 5 / rank 9).
/// Two-level fusion overvalues those docs; GLOBAL_RANK must reproduce the
/// monolithic result EXACTLY (id order, fused scores, per-leg ranks and
/// raw scores). SCORE_BLEND shares the same leg-fetch path and global
/// merge, so the same exactness must hold for every normalization and
/// combination — its stats are computed over the GLOBAL retained set,
/// which is identical in both layouts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn global_rank_fusion_is_exact_on_adversarial_partition() {
    let (analysis, mock) = start_mock_analysis().await;
    let corpus = unit_vectors(N_DOCS, DIM, 0x3333_0001);
    let (shift, scale) = fit_calibration(DIM, 4, &corpus);

    let query = corpus[..DIM].to_vec();
    let mut mono_index = pipestream_search::harness::seeded_index(DIM, 4, &shift, &scale);
    mono_index.add(&corpus, DIM).unwrap();
    let order: Vec<usize> = mono_index
        .search_unfiltered(&query, N_DOCS)
        .indices_for_query(0)
        .iter()
        .map(|&i| i as usize)
        .collect();
    assert_eq!(order[0], 0, "self-query must rank its own vector first");
    // position in the monolithic vector order, per original doc id
    let mut pos_in_order = [0usize; N_DOCS];
    for (q, &d) in order.iter().enumerate() {
        pos_in_order[d] = q;
    }

    // Banded assignment: shard i gets the docs at vector-order positions
    // [4i, 4i+4) — maximal local-rank compression on shards 1 and 2.
    // BM25 matches land on shards 1 and 2, away from the vector leader.
    let mut texts: Vec<String> = vec!["plain text document".to_string(); N_DOCS];
    texts[order[5]] = "a zebra appears here".to_string();
    texts[order[11]] = "another zebra far away".to_string();

    let mut addrs = Vec::new();
    let mut handles = Vec::new();
    for i in 0..N_SHARDS {
        let mut shard_texts = Vec::new();
        let mut shard_vectors = Vec::new();
        for j in 0..SHARD_DOCS {
            let original = order[4 * i + j];
            shard_texts.push(texts[original].clone());
            shard_vectors.extend_from_slice(&corpus[original * DIM..(original + 1) * DIM]);
        }
        let (addr, handle) = start_hybrid_shard(
            &analysis,
            (i * SHARD_DOCS) as u64,
            &shard_texts,
            shard_vectors,
            &shift,
            &scale,
        )
        .await;
        addrs.push(addr);
        handles.push(handle);
    }
    // Monolithic: one node, everything in corpus order (global id ==
    // original doc id).
    let (mono_addr, mono) =
        start_hybrid_shard(&analysis, 0, &texts, corpus.clone(), &shift, &scale).await;

    let distributed =
        CoordinatorServiceImpl::new(addrs).with_bm25(Some(analysis.clone()), Default::default());
    let monolithic =
        CoordinatorServiceImpl::new(vec![mono_addr]).with_bm25(Some(analysis), Default::default());

    // Every normalization and combination is covered, in collapse-free
    // pairings: Z_SCORE with GEOMETRIC/HARMONIC zeroes every doc whose
    // legs are all non-positive (the proto documents the skip rule), and
    // ordering INSIDE a fused-score tie group is by layout-dependent doc
    // ids — the same documented caveat as RRF ties, just bigger groups.
    for legs in [
        legs_default(),
        legs_blend(Normalization::MinMax, Combination::Arithmetic),
        legs_blend(Normalization::ZScore, Combination::Arithmetic),
        legs_blend(Normalization::MinMax, Combination::Geometric),
        legs_blend(Normalization::None, Combination::Harmonic),
    ] {
        let got = common::fanout::hybrid(&distributed, "adv", "zebra", &query, N_DOCS as u32, legs)
            .await
            .unwrap()
            .0;
        let want =
            common::fanout::hybrid(&monolithic, "adv-m", "zebra", &query, N_DOCS as u32, legs)
                .await
                .unwrap()
                .0;

        let mode = legs.fusion_mode;
        assert_eq!(got.len(), want.len(), "{mode:?}");
        for (pos, (g, w)) in got.iter().zip(want.iter()).enumerate() {
            // Original doc w.doc_id sits in the distributed layout at
            // global id = its position in the monolithic vector order.
            assert_eq!(
                g.doc_id as usize, pos_in_order[w.doc_id as usize],
                "{mode:?}: id mismatch at fused position {pos}"
            );
            assert_eq!(
                g.fused_score.to_bits(),
                w.fused_score.to_bits(),
                "{mode:?}: pos {pos} fused"
            );
            assert_eq!(g.vector_rank, w.vector_rank, "{mode:?}: pos {pos} vrank");
            assert_eq!(g.bm25_rank, w.bm25_rank, "{mode:?}: pos {pos} brank");
            assert_eq!(
                g.vector_score.to_bits(),
                w.vector_score.to_bits(),
                "{mode:?}: pos {pos} vscore: got id {} want id {}",
                g.doc_id,
                w.doc_id
            );
            assert_eq!(
                g.bm25_score.to_bits(),
                w.bm25_score.to_bits(),
                "{mode:?}: pos {pos} bscore"
            );
        }

        // The zebra docs are the fused leaders (both legs) and both
        // surface with bm25_rank present; the vector leader is doc 0.
        let top = &got[0];
        assert!(top.bm25_rank.is_some() || top.doc_id as usize == pos_in_order[0]);
    }

    for h in handles {
        h.abort();
    }
    mono.abort();
    mock.abort();
}

//! Hybrid (RRF-fused vector + BM25) integration tests over the mock
//! analyzer and seeded shards.
//!
//! Two properties are under test:
//!
//! 1. Determinism and provenance: identical results across runs; hits
//!    carry per-leg ranks, and a doc in both legs outranks single-leg
//!    docs at its shard.
//! 2. Distributed exactness on a partition-stable corpus: 3 shards fused
//!    == monolithic fused, exactly (id sequence). Two-level RRF is NOT
//!    partition-independent in general (see the README's counterexample),
//!    so the corpus is constructed partition-stable: docs are assigned to
//!    shards round-robin by the monolithic fused order, which makes the
//!    coordinator's (shard-rank, shard-index) ordering coincide with the
//!    monolithic order by construction. The test is deterministic — fixed
//!    seeds, fixed corpus — and proves the distribution machinery (id
//!    mapping, both RPC levels, fusion application) is lossless on it.

mod common;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use turbovec_search::coordinator::{CoordinatorServiceImpl, HybridLegs};
use turbovec_search::node::NodeConfig;
use turbovec_search::pb::node_service_client::NodeServiceClient;
use turbovec_search::pb::{AddDocumentsRequest, AddVectorsRequest, SetCalibrationRequest};

use common::{fit_calibration, mock::start_mock_analysis, start_empty_node, unit_vectors};
use turbovec_search::fusion::{Combination, Normalization};

const DIM: usize = 64;
const SHARD_DOCS: usize = 4;
const N_SHARDS: usize = 3;
const N_DOCS: usize = SHARD_DOCS * N_SHARDS;

async fn set_calibration(addr: &str, shift: &[f32], scale: &[f32]) {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    client
        .set_calibration(SetCalibrationRequest {
            dim: DIM as u32,
            bit_width: 4,
            shift: shift.to_vec(),
            scale: scale.to_vec(),
        })
        .await
        .unwrap();
}

async fn add_documents(addr: &str, texts: &[String]) {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let (tx, rx) = mpsc::channel(8);
    // Feed the stream from a task: the RPC only starts reading once it is
    // initiated, so sending more messages than the channel capacity before
    // the call would deadlock.
    let texts = texts.to_vec();
    let feeder = tokio::spawn(async move {
        for text in texts {
            tx.send(AddDocumentsRequest {
                text,
                analysis: None,
                lineage: None,
                fields: Vec::new(),
            })
            .await
            .unwrap();
        }
    });
    client.add_documents(ReceiverStream::new(rx)).await.unwrap();
    feeder.await.unwrap();
}

async fn add_vectors(addr: &str, vectors: Vec<f32>) {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let (tx, rx) = mpsc::channel(4);
    // dim is sent explicitly: harmless when the shard already knows it
    // (validated then), required for a from-scratch shard.
    tx.send(AddVectorsRequest {
        vectors,
        dim: DIM as u32,
    })
    .await
    .unwrap();
    drop(tx);
    client.add_vectors(ReceiverStream::new(rx)).await.unwrap();
}

fn legs_default() -> HybridLegs {
    HybridLegs {
        leg_k: 60,
        vector_weight: 1.0,
        bm25_weight: 1.0,
        rrf_k: 60.0,
        fusion_mode: turbovec_search::pb::FusionMode::GlobalRank,
        normalization: Normalization::MinMax,
        combination: Combination::Arithmetic,
        min_vector_score: 0.0,
    }
}

fn legs_two_level() -> HybridLegs {
    HybridLegs {
        fusion_mode: turbovec_search::pb::FusionMode::TwoLevel,
        ..legs_default()
    }
}

fn legs_blend(normalization: Normalization, combination: Combination) -> HybridLegs {
    HybridLegs {
        fusion_mode: turbovec_search::pb::FusionMode::ScoreBlend,
        normalization,
        combination,
        ..legs_default()
    }
}

fn ids(hits: &[turbovec_search::pb::HybridHit]) -> Vec<u64> {
    hits.iter().map(|h| h.doc_id).collect()
}

/// Start a shard, seed it, ingest docs then vectors (in that order so doc
/// ids and vector slots align 1:1 in the shared positional id space).
async fn start_hybrid_shard(
    analysis: &str,
    slot_offset: u64,
    texts: &[String],
    vectors: Vec<f32>,
    shift: &[f32],
    scale: &[f32],
) -> (
    String,
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
) {
    let (addr, handle) = start_empty_node(NodeConfig {
        slot_offset,
        analysis_addr: Some(analysis.to_string()),
        ..Default::default()
    })
    .await;
    set_calibration(&addr, shift, scale).await;
    add_documents(&addr, texts).await;
    add_vectors(&addr, vectors).await;
    (addr, handle)
}

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

    let first = coordinator
        .fanout_hybrid("h1", "zebra", &query, 8, None, legs_default(), false)
        .await
        .unwrap().0;
    let second = coordinator
        .fanout_hybrid("h2", "zebra", &query, 8, None, legs_default(), false)
        .await
        .unwrap().0;
    assert_eq!(
        ids(&first),
        ids(&second),
        "fused output must be deterministic"
    );
    let sig = |hits: &[turbovec_search::pb::HybridHit]| {
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
    let mut mono_vectors =
        turbovec::TurboQuantIndex::new_with_calibration(DIM, 4, &shift, &scale).unwrap();
    mono_vectors.add(&corpus);
    let order: Vec<usize> = mono_vectors
        .search(&query, N_DOCS)
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

    let got = distributed
        .fanout_hybrid("d", "zebra", &query, N_DOCS as u32, None, legs_default(), false)
        .await
        .unwrap().0;
    let want = monolithic
        .fanout_hybrid("m", "zebra", &query, N_DOCS as u32, None, legs_default(), false)
        .await
        .unwrap().0;

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
/// relative to the global ranks (their local #1 is globally #5 / #9).
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
    let mut mono_index =
        turbovec::TurboQuantIndex::new_with_calibration(DIM, 4, &shift, &scale).unwrap();
    mono_index.add(&corpus);
    let order: Vec<usize> = mono_index
        .search(&query, N_DOCS)
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
        let got = distributed
            .fanout_hybrid("adv", "zebra", &query, N_DOCS as u32, None, legs, false)
            .await
            .unwrap()
            .0;
        let want = monolithic
            .fanout_hybrid("adv-m", "zebra", &query, N_DOCS as u32, None, legs, false)
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

    let first = coordinator
        .fanout_hybrid("b1", "zebra", &query, 8, None, blend, false)
        .await
        .unwrap()
        .0;
    let second = coordinator
        .fanout_hybrid("b2", "zebra", &query, 8, None, blend, false)
        .await
        .unwrap()
        .0;
    let sig = |hits: &[turbovec_search::pb::HybridHit]| {
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
    let boosted = coordinator
        .fanout_hybrid("b3", "zebra", &query, 8, None, weighted, false)
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
        let hits = coordinator
            .fanout_hybrid("bx", "zebra", &query, 8, None, legs, false)
            .await
            .unwrap()
            .0;
        let again = coordinator
            .fanout_hybrid("by", "zebra", &query, 8, None, legs, false)
            .await
            .unwrap()
            .0;
        assert_eq!(sig(&hits), sig(&again));
        assert_eq!(hits[0].doc_id, 0, "{:?}/{:?}", legs.normalization, legs.combination);
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
    use turbovec_search::pb::search_service_server::SearchService as _;
    use turbovec_search::pb::{BoostRescore, HybridLegOptions, HybridSearchRequest};

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
        })
    };
    let global_rank = || {
        Some(HybridLegOptions {
            fusion_mode: turbovec_search::pb::FusionMode::GlobalRank as i32,
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

/// Leg disabling and the vector-score floor through the HybridSearch
/// handler: an explicit weight of 0 turns a leg off (vector-only order
/// == the plain Search RPC's order; bm25-only surfaces only matching
/// docs), disabling both legs or a TWO_LEVEL leg is rejected, and
/// min_vector_score drops every hit below the floor BEFORE fusion in
/// every mode (deeper qualifying docs get promoted, not truncated).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leg_disabling_and_vector_floor() {
    use turbovec_search::pb::search_service_server::SearchService as _;
    use turbovec_search::pb::{HybridLegOptions, HybridSearchRequest, SearchRequest};

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
        })
    };
    let global_rank = HybridLegOptions {
        fusion_mode: turbovec_search::pb::FusionMode::GlobalRank as i32,
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
            fusion_mode: turbovec_search::pb::FusionMode::TwoLevel as i32,
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
        turbovec_search::pb::FusionMode::GlobalRank,
        turbovec_search::pb::FusionMode::ScoreBlend,
        turbovec_search::pb::FusionMode::TwoLevel,
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
        assert_eq!(got, want, "{mode:?}: floor must keep exactly the qualifying docs");
        assert!(filtered
            .hits
            .iter()
            .all(|h| h.vector_rank.is_some() && h.vector_score >= floor));
    }
    // Cascade: same floor, applied to the phase-1 pool.
    let cascade = coordinator
        .hybrid_search(request(HybridLegOptions {
            fusion_mode: turbovec_search::pb::FusionMode::Cascade as i32,
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
    assert_eq!(got, want, "cascade floor must keep exactly the qualifying docs");
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

    let first = coordinator
        .fanout_hybrid("t1", "zebra", &query, 8, None, legs_two_level(), false)
        .await
        .unwrap().0;
    let second = coordinator
        .fanout_hybrid("t2", "zebra", &query, 8, None, legs_two_level(), false)
        .await
        .unwrap().0;
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

/// BroadcastCalibration fans one calibration out to every shard;
/// GetCalibration agrees afterwards, re-broadcast is idempotent, and a
/// non-empty shard refuses without breaking the fan-out.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn broadcast_calibration_reaches_all_shards() {
    let corpus = unit_vectors(2_000, DIM, 0x5555_0001);
    let (shift, scale) = fit_calibration(DIM, 4, &corpus);

    let (addr_a, handle_a) = start_empty_node(NodeConfig::default()).await;
    let (addr_b, handle_b) = start_empty_node(NodeConfig::default()).await;
    // A third node that already holds vectors: must refuse.
    let (addr_c, handle_c) = start_empty_node(NodeConfig::default()).await;
    add_vectors(&addr_c, corpus[..100 * DIM].to_vec()).await;

    let coordinator =
        CoordinatorServiceImpl::new(vec![addr_a.clone(), addr_b.clone(), addr_c.clone()]);
    let request = turbovec_search::pb::BroadcastCalibrationRequest {
        dim: DIM as u32,
        bit_width: 4,
        shift: shift.clone(),
        scale: scale.clone(),
    };
    let results = coordinator.fanout_calibration(&request).await;
    assert_eq!(results.len(), 3);
    assert!(
        results[0].ok && !results[0].already_seeded,
        "node A: {:?}",
        results[0].error
    );
    assert!(
        results[1].ok && !results[1].already_seeded,
        "node B: {:?}",
        results[1].error
    );
    assert!(!results[2].ok, "non-empty node must refuse");
    assert!(!results[2].error.is_empty());

    // Every seeded node reports the broadcast calibration.
    for addr in [&addr_a, &addr_b] {
        let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
        let cal = client
            .get_calibration(turbovec_search::pb::GetCalibrationRequest {})
            .await
            .unwrap()
            .into_inner();
        assert_eq!(cal.dim as usize, DIM);
        assert_eq!(cal.shift, shift);
        assert_eq!(cal.scale, scale);
    }

    // Re-broadcast: idempotent no-op on the seeded nodes, still refused
    // on the non-empty one.
    let again = coordinator.fanout_calibration(&request).await;
    assert!(again[0].ok && again[0].already_seeded);
    assert!(again[1].ok && again[1].already_seeded);
    assert!(!again[2].ok);

    handle_a.abort();
    handle_b.abort();
    handle_c.abort();
}

/// The HybridSearch lexical leg routes through block-max
/// (`top_k_pruned`) on a v5-resident shard: the fused result must be
/// bit-identical to the heap-backed fallback path (which keeps top_k).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hybrid_lexical_leg_matches_between_heap_and_v5_resident() {
    let (analysis, mock) = start_mock_analysis().await;
    let corpus = unit_vectors(SHARD_DOCS, DIM, 0x1111_0002);
    let (shift, scale) = fit_calibration(DIM, 4, &corpus);
    let texts: Vec<String> = (0..SHARD_DOCS)
        .map(|i| format!("plain document number {i} about nothing special"))
        .collect();
    let mut zebra = texts.clone();
    zebra[0] = "zebra stripes everywhere".to_string();
    zebra[3] = "another zebra crossing".to_string();

    // Heap-backed shard (Building store → top_k fallback).
    let (addr_heap, handle_heap) = start_hybrid_shard(
        &analysis,
        0,
        &zebra,
        corpus.clone(),
        &shift,
        &scale,
    )
    .await;
    // v5-resident shard (index path → Flush → Bm25Reader → pruned leg).
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("hybrid_v5_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let (addr_v5, handle_v5) = start_empty_node(NodeConfig {
        analysis_addr: Some(analysis.clone()),
        index_path: Some(dir.join("shard.tv")),
        ..Default::default()
    })
    .await;
    set_calibration(&addr_v5, &shift, &scale).await;
    add_documents(&addr_v5, &zebra).await;
    add_vectors(&addr_v5, corpus.clone()).await;
    {
        let mut client = NodeServiceClient::connect(addr_v5.clone()).await.unwrap();
        let flushed = client
            .flush(turbovec_search::pb::FlushRequest {})
            .await
            .unwrap()
            .into_inner();
        assert!(flushed.written);
    }

    let query = corpus[..DIM].to_vec();
    let mut runs = Vec::new();
    for addr in [addr_heap, addr_v5] {
        let coordinator =
            CoordinatorServiceImpl::new(vec![addr]).with_bm25(Some(analysis.clone()), Default::default());
        runs.push(
            coordinator
                .fanout_hybrid("h", "zebra", &query, 8, None, legs_default(), false)
                .await
                .unwrap()
                .0,
        );
    }
    let sig = |hits: &[turbovec_search::pb::HybridHit]| {
        hits.iter()
            .map(|h| {
                (
                    h.doc_id,
                    h.fused_score.to_bits(),
                    h.vector_rank,
                    h.bm25_rank,
                    h.vector_score.to_bits(),
                    h.bm25_score.to_bits(),
                )
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(
        sig(&runs[0]),
        sig(&runs[1]),
        "heap and v5-resident hybrid results diverged"
    );
    // Sanity: the zebra docs really came through the lexical leg.
    assert!(runs[1].iter().any(|h| h.bm25_rank.is_some()));

    handle_heap.abort();
    handle_v5.abort();
    mock.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// The debug/profile block: absent unless requested, mode and depth
/// echoed, one entry per shard with real timings, terms carried, cascade
/// rich with the vector scan's stats — and enabling it never changes
/// results.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn debug_block_profiles_every_fusion_mode() {
    let (analysis, mock) = start_mock_analysis().await;
    let corpus = unit_vectors(2 * SHARD_DOCS, DIM, 0x1111_0009);
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

    for legs in [
        legs_default(),
        legs_two_level(),
        legs_blend(Normalization::MinMax, Combination::Arithmetic),
    ] {
        let (plain, no_debug) = coordinator
            .fanout_hybrid("p", "zebra", &query, 8, None, legs, false)
            .await
            .unwrap();
        assert!(no_debug.is_none(), "debug=false must not build a profile");
        let (hits, debug) = coordinator
            .fanout_hybrid("d", "zebra", &query, 8, None, legs, true)
            .await
            .unwrap();
        assert_eq!(ids(&plain), ids(&hits), "debug changed the results");
        let debug = debug.unwrap();
        assert_eq!(debug.fusion_mode, legs.fusion_mode as i32);
        assert_eq!(debug.leg_k, legs.leg_k);
        assert_eq!(debug.terms, vec!["zebra".to_string()]);
        assert_eq!(debug.shards.len(), 2, "one entry per shard");
        for (i, shard) in debug.shards.iter().enumerate() {
            assert_eq!(shard.shard, i as u32, "shards sorted by index");
            assert!(shard.rpc_ms > 0.0);
        }
        assert!(
            debug.shards.iter().any(|s| s.bm25_hits > 0),
            "zebra must reach some shard's lexical leg"
        );
        assert!(debug.analysis_ms > 0.0);
        assert!(debug.stats_ms > 0.0);
        assert!(debug.legs_ms > 0.0);
        assert!(debug.total_ms >= debug.legs_ms);
    }

    let (plain, no_debug) = coordinator
        .fanout_cascade("cp", "zebra", &query, 4, None, 0.0, false)
        .await
        .unwrap();
    assert!(no_debug.is_none());
    let (hits, debug) = coordinator
        .fanout_cascade("cd", "zebra", &query, 4, None, 0.0, true)
        .await
        .unwrap();
    let cascade_sig = |hits: &[turbovec_search::pb::CascadeHit]| {
        hits.iter().map(|h| (h.doc_id, h.rank)).collect::<Vec<_>>()
    };
    assert_eq!(cascade_sig(&plain), cascade_sig(&hits));
    let debug = debug.unwrap();
    assert_eq!(
        debug.fusion_mode,
        turbovec_search::pb::FusionMode::Cascade as i32
    );
    assert_eq!(debug.terms, vec!["zebra".to_string()]);
    assert_eq!(debug.shards.len(), 2);
    for shard in &debug.shards {
        assert!(shard.scan.is_some(), "cascade carries the vector scan stats");
        assert!(shard.vector_hits > 0, "phase-1 candidates counted");
    }
    assert!(debug.total_ms > 0.0);

    mock.abort();
    for handle in handles {
        handle.abort();
    }
}

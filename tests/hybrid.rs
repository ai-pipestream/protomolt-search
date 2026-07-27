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
    }
}

fn legs_two_level() -> HybridLegs {
    HybridLegs {
        fusion_mode: turbovec_search::pb::FusionMode::TwoLevel,
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
        .fanout_hybrid("h1", "zebra", &query, 8, None, legs_default())
        .await
        .unwrap();
    let second = coordinator
        .fanout_hybrid("h2", "zebra", &query, 8, None, legs_default())
        .await
        .unwrap();
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
        .fanout_hybrid("d", "zebra", &query, N_DOCS as u32, None, legs_default())
        .await
        .unwrap();
    let want = monolithic
        .fanout_hybrid("m", "zebra", &query, N_DOCS as u32, None, legs_default())
        .await
        .unwrap();

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
/// raw scores).
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

    let got = distributed
        .fanout_hybrid("adv", "zebra", &query, N_DOCS as u32, None, legs_default())
        .await
        .unwrap();
    let want = monolithic
        .fanout_hybrid(
            "adv-m",
            "zebra",
            &query,
            N_DOCS as u32,
            None,
            legs_default(),
        )
        .await
        .unwrap();

    assert_eq!(got.len(), want.len());
    for (pos, (g, w)) in got.iter().zip(want.iter()).enumerate() {
        // Original doc w.doc_id sits in the distributed layout at global
        // id = its position in the monolithic vector order.
        assert_eq!(
            g.doc_id as usize, pos_in_order[w.doc_id as usize],
            "id mismatch at fused position"
        );
        assert_eq!(
            g.fused_score.to_bits(),
            w.fused_score.to_bits(),
            "pos {pos} fused"
        );
        assert_eq!(g.vector_rank, w.vector_rank, "pos {pos} vrank");
        assert_eq!(g.bm25_rank, w.bm25_rank, "pos {pos} brank");
        assert_eq!(
            g.vector_score.to_bits(),
            w.vector_score.to_bits(),
            "pos {pos} vscore: got id {} want id {}",
            g.doc_id,
            w.doc_id
        );
        assert_eq!(
            g.bm25_score.to_bits(),
            w.bm25_score.to_bits(),
            "pos {pos} bscore"
        );
    }

    // The zebra docs are the fused leaders (both legs) and both surface
    // with bm25_rank present; the leader of the vector leg is doc 0.
    let top = &got[0];
    assert!(top.bm25_rank.is_some() || top.doc_id as usize == pos_in_order[0]);

    for h in handles {
        h.abort();
    }
    mono.abort();
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
        .fanout_hybrid("t1", "zebra", &query, 8, None, legs_two_level())
        .await
        .unwrap();
    let second = coordinator
        .fanout_hybrid("t2", "zebra", &query, 8, None, legs_two_level())
        .await
        .unwrap();
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

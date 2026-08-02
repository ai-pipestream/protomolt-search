//! FUSION_MODE_DECOMPOSED gates: the streaming BM25-first execution
//! (decomposed floors, VectorRescore seeding, Bm25Rescore close-out)
//! must reproduce the exhaustive fused weighted sum EXACTLY — same doc
//! ids, same fused/vector/BM25 score bits — for every weighting, both
//! leg-depth regimes (filled leg with a real boundary bound, unfilled
//! leg with proven-zero absentees), and the min_vector_score gate.
//!
//! The oracle is same-cluster: v(d) from a full unfloored streaming
//! fan-out, b(d) from a corpus-deep Bm25Search, fused in the test with
//! the identical f64 expression. Same shards, same kernel paths, so
//! bitwise equality is the specification, not an aspiration (the
//! documented cross-shape ULP drift of the vector kernel never enters).

mod common;

use std::collections::HashMap;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use turbovec_search::coordinator::{CoordinatorServiceImpl, HybridLegs};
use turbovec_search::fusion::{Combination, Normalization};
use turbovec_search::node::NodeConfig;
use turbovec_search::pb::node_service_client::NodeServiceClient;
use turbovec_search::pb::search_service_server::SearchService as _;
use turbovec_search::pb::{
    AddDocumentsRequest, AddVectorsRequest, FusionMode, HybridHit, HybridLegOptions,
    HybridSearchRequest, SetCalibrationRequest, VectorRescoreRequest,
};

use common::{fit_calibration, mock::start_mock_analysis, start_empty_node, unit_vectors};

const DIM: usize = 64;
const SHARD_DOCS: usize = 16;
const N_SHARDS: usize = 3;
const N_DOCS: usize = SHARD_DOCS * N_SHARDS;

/// Doc texts: a third of the corpus mentions "zebra" with varying term
/// frequency, a handful add "crossing", the rest match nothing — so the
/// BM25 leg has real spread, real absentees, and a boundary that moves
/// with leg_k.
fn build_texts() -> Vec<String> {
    (0..N_DOCS)
        .map(|i| {
            if i % 3 == 0 {
                let zebras = vec!["zebra"; 1 + (i % 5)].join(" ");
                if i % 6 == 0 {
                    format!("{zebras} crossing ahead document {i}")
                } else {
                    format!("{zebras} in the savanna document {i}")
                }
            } else {
                format!("plain filler text about nothing number {i}")
            }
        })
        .collect()
}

struct Fixture {
    coordinator: CoordinatorServiceImpl,
    addrs: Vec<String>,
    handles: Vec<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
    mock: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    corpus: Vec<f32>,
}

async fn start_fixture() -> Fixture {
    let (analysis, mock) = start_mock_analysis().await;
    let corpus = unit_vectors(N_DOCS, DIM, 0xDEC0_0001);
    let (shift, scale) = fit_calibration(DIM, 4, &corpus);
    let texts = build_texts();

    let mut addrs = Vec::new();
    let mut handles = Vec::new();
    for shard in 0..N_SHARDS {
        let start = shard * SHARD_DOCS;
        let (addr, handle) = start_empty_node(NodeConfig {
            slot_offset: start as u64,
            analysis_addr: Some(analysis.clone()),
            ..Default::default()
        })
        .await;
        let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
        client
            .set_calibration(SetCalibrationRequest {
                dim: DIM as u32,
                bit_width: 4,
                shift: shift.clone(),
                scale: scale.clone(),
            })
            .await
            .unwrap();
        let (tx, rx) = mpsc::channel(8);
        let shard_texts = texts[start..start + SHARD_DOCS].to_vec();
        let feeder = tokio::spawn(async move {
            for text in shard_texts {
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
        let (tx, rx) = mpsc::channel(4);
        tx.send(AddVectorsRequest {
            vectors: corpus[start * DIM..(start + SHARD_DOCS) * DIM].to_vec(),
            dim: DIM as u32,
        })
        .await
        .unwrap();
        drop(tx);
        client.add_vectors(ReceiverStream::new(rx)).await.unwrap();
        addrs.push(addr);
        handles.push(handle);
    }
    let coordinator =
        CoordinatorServiceImpl::new(addrs.clone()).with_bm25(Some(analysis), Default::default());
    Fixture {
        coordinator,
        addrs,
        handles,
        mock,
        corpus,
    }
}

impl Fixture {
    async fn shutdown(self) {
        for h in self.handles {
            h.abort();
        }
        self.mock.abort();
    }
}

fn legs_decomposed(vector_weight: f32, bm25_weight: f32, leg_k: u32) -> HybridLegs {
    HybridLegs {
        leg_k,
        vector_weight,
        bm25_weight,
        rrf_k: 60.0,
        fusion_mode: FusionMode::Decomposed,
        normalization: Normalization::MinMax,
        combination: Combination::Arithmetic,
        min_vector_score: 0.0,
    }
}

/// The exhaustive oracle: every doc's exact leg scores from
/// corpus-deep queries on the SAME cluster, fused with the identical
/// f64 expression and ranked (fused desc, shard, doc).
async fn oracle_fused(
    coordinator: &CoordinatorServiceImpl,
    text: &str,
    query: &[f32],
    w_v: f32,
    w_b: f32,
    min_vector_score: f32,
) -> Vec<(u64, u32, u32, u32)> {
    let deep_v = coordinator
        .fanout_stream_search("oracle-v", query, N_DOCS as u32, None)
        .await
        .expect("deep vector fan-out");
    let deep_b = coordinator
        .fanout_bm25(text, N_DOCS as u32, None)
        .await
        .expect("deep bm25 fan-out");
    let b_of: HashMap<u64, f32> = deep_b.iter().map(|h| (h.doc_id, h.score)).collect();
    let mut fused: Vec<(u64, u32, f32, f32, f64)> = deep_v
        .hits
        .iter()
        .filter(|h| min_vector_score <= 0.0 || h.score >= min_vector_score)
        .map(|h| {
            let b = b_of.get(&h.vector_id).copied().unwrap_or(0.0);
            let shard = (h.vector_id / SHARD_DOCS as u64) as u32;
            (
                h.vector_id,
                shard,
                h.score,
                b,
                f64::from(w_v) * f64::from(h.score) + f64::from(w_b) * f64::from(b),
            )
        })
        .collect();
    fused.sort_by(|a, b| {
        b.4.total_cmp(&a.4)
            .then_with(|| a.1.cmp(&b.1))
            .then_with(|| a.0.cmp(&b.0))
    });
    fused
        .into_iter()
        .map(|(doc, _, v, b, f)| (doc, (f as f32).to_bits(), v.to_bits(), b.to_bits()))
        .collect()
}

fn signature(hits: &[HybridHit]) -> Vec<(u64, u32, u32, u32)> {
    hits.iter()
        .map(|h| {
            (
                h.doc_id,
                h.fused_score.to_bits(),
                h.vector_score.to_bits(),
                h.bm25_score.to_bits(),
            )
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn decomposed_matches_exhaustive_fused_oracle() {
    let fx = start_fixture().await;
    let query = fx.corpus[..DIM].to_vec();
    let k = 10u32;

    // Weightings on both sides of the scale gap, and leg depths that
    // exercise both close-out regimes: leg_k == k fills the leg (real
    // boundary bound, Bm25Rescore close-out), leg_k == N_DOCS leaves
    // it unfilled (absent docs proven b = 0, no rescore needed).
    for &(w_v, w_b) in &[(1.0f32, 1.0f32), (0.02, 5.0), (5.0, 0.02)] {
        for &leg_k in &[k, N_DOCS as u32] {
            let want: Vec<_> =
                oracle_fused(&fx.coordinator, "zebra crossing", &query, w_v, w_b, 0.0)
                    .await
                    .into_iter()
                    .take(k as usize)
                    .collect();
            let got = fx
                .coordinator
                .fanout_hybrid(
                    &format!("dec-{w_v}-{w_b}-{leg_k}"),
                    "zebra crossing",
                    &query,
                    k,
                    None,
                    legs_decomposed(w_v, w_b, leg_k),
                    false,
                )
                .await
                .expect("decomposed fan-out")
                .0;
            assert_eq!(
                signature(&got),
                want,
                "w_v={w_v} w_b={w_b} leg_k={leg_k}: decomposed != exhaustive fused oracle"
            );
        }
    }
    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn decomposed_is_deterministic_across_racy_floor_timing() {
    let fx = start_fixture().await;
    let query = fx.corpus[DIM..2 * DIM].to_vec();
    let legs = || legs_decomposed(1.0, 0.5, 12);
    let first = fx
        .coordinator
        .fanout_hybrid("det-1", "zebra", &query, 12, None, legs(), false)
        .await
        .unwrap()
        .0;
    let second = fx
        .coordinator
        .fanout_hybrid("det-2", "zebra", &query, 12, None, legs(), false)
        .await
        .unwrap()
        .0;
    assert_eq!(
        signature(&first),
        signature(&second),
        "mid-scan floor race must never reach the results"
    );
    // Provenance: BM25 ranks are global leg ranks; a ranked doc always
    // carries a positive leg score, and the stream is not a ranking.
    assert!(first.iter().any(|h| h.bm25_rank.is_some()));
    for h in &first {
        if h.bm25_rank.is_some() {
            assert!(h.bm25_score > 0.0);
        }
        assert!(h.vector_rank.is_none(), "the stream is not a ranking");
    }
    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn decomposed_empty_bm25_leg_degenerates_to_scaled_vector_ranking() {
    let fx = start_fixture().await;
    let query = fx.corpus[2 * DIM..3 * DIM].to_vec();
    let k = 8u32;
    // No document contains this term: b(d) = 0 everywhere, fused is
    // w_v * v(d), and the ranking must be the plain streaming top-k.
    let got = fx
        .coordinator
        .fanout_hybrid(
            "deg-1",
            "xyzzyplugh",
            &query,
            k,
            None,
            legs_decomposed(2.5, 1.0, 20),
            false,
        )
        .await
        .unwrap()
        .0;
    let want = oracle_fused(&fx.coordinator, "xyzzyplugh", &query, 2.5, 1.0, 0.0).await;
    assert_eq!(signature(&got), want[..k as usize].to_vec());
    for h in &got {
        assert_eq!(h.bm25_score, 0.0);
        assert_eq!(h.bm25_rank, None);
    }
    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn decomposed_min_vector_score_gates_the_result_set_exactly() {
    let fx = start_fixture().await;
    let query = fx.corpus[..DIM].to_vec();
    let k = 8u32;
    // A floor at the deep vector list's 20th score keeps exactly 20
    // docs eligible (ties by construction of distinct unit vectors are
    // absent at this boundary).
    let deep_v = fx
        .coordinator
        .fanout_stream_search("gate-deep", &query, N_DOCS as u32, None)
        .await
        .unwrap();
    let min_v = deep_v.hits[19].score;
    let mut legs = legs_decomposed(1.0, 1.0, k);
    legs.min_vector_score = min_v;
    let got = fx
        .coordinator
        .fanout_hybrid("gate-1", "zebra crossing", &query, k, None, legs, false)
        .await
        .unwrap()
        .0;
    let want: Vec<_> = oracle_fused(&fx.coordinator, "zebra crossing", &query, 1.0, 1.0, min_v)
        .await
        .into_iter()
        .take(k as usize)
        .collect();
    assert_eq!(
        signature(&got),
        want,
        "min_vector_score gate must stay exact"
    );
    for h in &got {
        assert!(h.vector_score >= min_v);
    }
    fx.shutdown().await;
}

/// The node-level gate for the phase-2 seed: a masked candidate-scoped
/// rescore returns bitwise the scores the full scan produces — one
/// kernel, one calibration — and ignores ids outside the shard.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vector_rescore_matches_full_search_bitwise() {
    let fx = start_fixture().await;
    let query = fx.corpus[3 * DIM..4 * DIM].to_vec();
    let deep = fx
        .coordinator
        .fanout_stream_search("vr-deep", &query, N_DOCS as u32, None)
        .await
        .unwrap();
    let score_of: HashMap<u64, u32> = deep
        .hits
        .iter()
        .map(|h| (h.vector_id, h.score.to_bits()))
        .collect();

    // A few ids per shard, plus ids no shard owns.
    for (shard, addr) in fx.addrs.iter().enumerate() {
        let base = (shard * SHARD_DOCS) as u64;
        let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
        let candidates = vec![base, base + 3, base + 7, 9_999_999];
        let hits = client
            .vector_rescore(VectorRescoreRequest {
                vector: query.clone(),
                candidate_ids: candidates,
            })
            .await
            .unwrap()
            .into_inner()
            .hits;
        assert_eq!(
            hits.len(),
            3,
            "shard {shard}: out-of-range id must be ignored"
        );
        for hit in hits {
            assert_eq!(
                Some(&hit.score.to_bits()),
                score_of.get(&hit.doc_id),
                "shard {shard} doc {}: rescore must be bitwise the scan score",
                hit.doc_id
            );
        }
        // Empty candidate list: empty answer, not an error.
        let empty = client
            .vector_rescore(VectorRescoreRequest {
                vector: query.clone(),
                candidate_ids: vec![],
            })
            .await
            .unwrap()
            .into_inner()
            .hits;
        assert!(empty.is_empty());
    }
    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn decomposed_rejects_disabled_or_negative_legs() {
    let fx = start_fixture().await;
    let query = fx.corpus[..DIM].to_vec();
    for (v_w, b_w) in [(0.0f32, 1.0f32), (1.0, 0.0), (-1.0, 1.0), (1.0, -0.5)] {
        let status = fx
            .coordinator
            .hybrid_search(tonic::Request::new(HybridSearchRequest {
                request_id: String::new(),
                text: "zebra".to_string(),
                vector: query.clone(),
                k: 5,
                analysis: None,
                legs: Some(HybridLegOptions {
                    fusion_mode: FusionMode::Decomposed as i32,
                    vector_weight: Some(v_w),
                    bm25_weight: Some(b_w),
                    ..Default::default()
                }),
                debug: false,
                boost: None,
            }))
            .await
            .expect_err("weights outside (0, inf) must be refused");
        assert_eq!(
            status.code(),
            tonic::Code::InvalidArgument,
            "({v_w}, {b_w})"
        );
    }
    fx.shutdown().await;
}

/// The handler path end to end, with debug: the mode echoes in the
/// profile, every shard reports leg and emission counts, and the
/// result equals the direct fan-out call.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn decomposed_serves_through_hybrid_search_with_debug() {
    let fx = start_fixture().await;
    let query = fx.corpus[..DIM].to_vec();
    let response = fx
        .coordinator
        .hybrid_search(tonic::Request::new(HybridSearchRequest {
            request_id: String::new(),
            text: "zebra crossing".to_string(),
            vector: query.clone(),
            k: 6,
            analysis: None,
            legs: Some(HybridLegOptions {
                fusion_mode: FusionMode::Decomposed as i32,
                ..Default::default()
            }),
            debug: true,
            boost: None,
        }))
        .await
        .unwrap()
        .into_inner();
    let direct = fx
        .coordinator
        .fanout_hybrid(
            "dbg-direct",
            "zebra crossing",
            &query,
            6,
            None,
            legs_decomposed(1.0, 1.0, 60),
            false,
        )
        .await
        .unwrap()
        .0;
    assert_eq!(signature(&response.hits), signature(&direct));
    let debug = response.debug.expect("debug requested");
    assert_eq!(debug.fusion_mode, FusionMode::Decomposed as i32);
    assert_eq!(debug.shards.len(), N_SHARDS);
    assert!(
        !debug.terms.is_empty(),
        "analyzed terms echo in the profile"
    );
    fx.shutdown().await;
}

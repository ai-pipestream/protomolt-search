//! Cascade fusion integration tests (FUSION_MODE_CASCADE, the default).
//!
//! Corpus design (12 docs, 3 shards): doc A is the query vector itself
//! (top score); three copies of a near-query vector V form the boundary
//! tie group at k=2, spread across all three shards; the rest are random
//! fillers scoring far below V. The tie-complete pool is therefore
//! {A, B1, B2, B3} — 4 candidates at k=2 — and truncating any shard's tie
//! group would drop a tied doc from candidacy and change the rerank.

mod common;

use turbovec_search::coordinator::CoordinatorServiceImpl;
use turbovec_search::pb::node_service_client::NodeServiceClient;

use common::{fit_calibration, mock::start_mock_analysis, unit_vectors, DIM};

const N: usize = 12;

/// Normalize v in place.
fn normalize(v: &mut [f32]) {
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    for x in v.iter_mut() {
        *x /= norm;
    }
}

struct Corpus {
    vectors: Vec<f32>,
    texts: Vec<String>,
    query: Vec<f32>,
}

/// A=query at id 0, V copies at ids 1, 4, 8 (one per shard band), fillers
/// elsewhere. B2 (id 4) and B3 (id 8) carry the zebra text; B3's text has
/// the higher tf so the BM25 rerank order is deterministic.
fn build_corpus() -> Corpus {
    let query = unit_vectors(1, DIM, 0xCA5C_0001);
    let fillers = unit_vectors(N, DIM, 0xCA5C_0002);
    // V: near-query, comfortably above random fillers. The 0.85/0.15 mix
    // keeps V clearly below the exact self-match under v5 (Hadamard)
    // quantization: v5's approximate scores can reorder vectors whose true
    // cosines are within ~0.001 of 1.0 (at 0.95/0.05, V outscored the
    // self-match by ~0.0008 and fell OUTSIDE the intended boundary tie
    // group). At 0.85/0.15 the margin is ~0.008, so A is top, the three
    // V copies tie exactly at the k=2 boundary, and fillers sit far below.
    let mut v: Vec<f32> = query
        .iter()
        .zip(fillers[..DIM].iter())
        .map(|(q, f)| 0.85 * q + 0.15 * f)
        .collect();
    normalize(&mut v);

    let mut vectors = vec![0.0f32; N * DIM];
    let mut put = |id: usize, src: &[f32]| {
        vectors[id * DIM..(id + 1) * DIM].copy_from_slice(src);
    };
    put(0, &query);
    put(1, &v);
    put(4, &v);
    put(8, &v);
    for (i, chunk) in vectors.chunks_mut(DIM).enumerate() {
        if ![0, 1, 4, 8].contains(&i) {
            chunk.copy_from_slice(&fillers[i * DIM..(i + 1) * DIM]);
        }
    }

    let mut texts: Vec<String> = (0..N)
        .map(|i| format!("plain filler document {i}"))
        .collect();
    texts[4] = "a zebra appears".to_string();
    texts[8] = "zebra zebra zebra everywhere".to_string();

    Corpus {
        vectors,
        texts,
        query,
    }
}

async fn start_cluster(
    analysis: &str,
    corpus: &Corpus,
    n_shards: usize,
    shift: &[f32],
    scale: &[f32],
    share_floors: bool,
) -> (
    Vec<String>,
    Vec<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
) {
    let per = N / n_shards;
    let mut addrs = Vec::new();
    let mut handles = Vec::new();
    for shard in 0..n_shards {
        let start = shard * per;
        let texts: Vec<String> = corpus.texts[start..start + per].to_vec();
        let vectors = corpus.vectors[start * DIM..(start + per) * DIM].to_vec();
        let (addr, handle) = common::start_empty_node(turbovec_search::node::NodeConfig {
            slot_offset: (shard * per) as u64,
            analysis_addr: Some(analysis.to_string()),
            share_floors,
            ..Default::default()
        })
        .await;
        // Seed, ingest docs first (ids 0..), then vectors (slots align).
        let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
        client
            .set_calibration(turbovec_search::pb::SetCalibrationRequest {
                dim: DIM as u32,
                bit_width: 4,
                shift: shift.to_vec(),
                scale: scale.to_vec(),
            })
            .await
            .unwrap();
        {
            let (tx, rx) = tokio::sync::mpsc::channel(8);
            let feed = tokio::spawn(async move {
                for text in texts {
                    tx.send(turbovec_search::pb::AddDocumentsRequest {
                        facets: Vec::new(),
                        text,
                        analysis: None,
                        lineage: None,
                        fields: Vec::new(),
                    })
                    .await
                    .unwrap();
                }
            });
            client
                .add_documents(tokio_stream::wrappers::ReceiverStream::new(rx))
                .await
                .unwrap();
            feed.await.unwrap();
        }
        {
            let (tx, rx) = tokio::sync::mpsc::channel(4);
            tx.send(turbovec_search::pb::AddVectorsRequest {
                vectors,
                dim: DIM as u32,
            })
            .await
            .unwrap();
            drop(tx);
            client
                .add_vectors(tokio_stream::wrappers::ReceiverStream::new(rx))
                .await
                .unwrap();
        }
        addrs.push(addr);
        handles.push(handle);
    }
    (addrs, handles)
}

fn signature(hits: &[turbovec_search::pb::CascadeHit]) -> Vec<(u64, u32, u32, u32)> {
    hits.iter()
        .map(|h| {
            (
                h.doc_id,
                h.rank,
                h.vector_score.to_bits(),
                h.bm25_score.to_bits(),
            )
        })
        .collect()
}

/// ULP distance between two f32 bit patterns (monotone map via ordering
/// transform, so the distance is a plain integer difference).
fn ulp_distance(a: f32, b: f32) -> u32 {
    let key = |x: f32| -> i64 {
        let b = x.to_bits() as i32;
        if b < 0 {
            i64::from(i32::MIN) - i64::from(b)
        } else {
            i64::from(b)
        }
    };
    (key(a) - key(b)).unsigned_abs() as u32
}

/// Exact on ids, ranks, and BM25 score bits; vector scores within a few
/// ULPs. Rationale (documented in the README): turbovec's score
/// accumulation order depends on the index's shape, so the same vector
/// can score a couple of ULPs differently in differently-sized shards.
/// Bitwise identity only holds within same-shape kernel paths.
fn assert_cascade_equivalent(
    got: &[turbovec_search::pb::CascadeHit],
    want: &[turbovec_search::pb::CascadeHit],
) {
    assert_eq!(got.len(), want.len());
    for (g, w) in got.iter().zip(want.iter()) {
        assert_eq!(g.doc_id, w.doc_id, "doc id");
        assert_eq!(g.rank, w.rank, "rank");
        assert_eq!(g.bm25_score.to_bits(), w.bm25_score.to_bits(), "bm25 bits");
        assert!(
            ulp_distance(g.vector_score, w.vector_score) <= 8,
            "vector score ULP drift: {:08x} vs {:08x}",
            g.vector_score.to_bits(),
            w.vector_score.to_bits()
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cascade_includes_whole_boundary_tie_group_and_is_deterministic() {
    let (analysis, mock) = start_mock_analysis().await;
    let corpus = build_corpus();
    let (shift, scale) = fit_calibration(DIM, 4, &corpus.vectors);
    let (addrs, handles) = start_cluster(&analysis, &corpus, 3, &shift, &scale, true).await;
    let coordinator =
        CoordinatorServiceImpl::new(addrs).with_bm25(Some(analysis), Default::default());

    let first = coordinator
        .fanout_cascade("c1", "zebra", &corpus.query, 2, None, 0.0, false)
        .await
        .unwrap()
        .0;
    let second = coordinator
        .fanout_cascade("c2", "zebra", &corpus.query, 2, None, 0.0, false)
        .await
        .unwrap()
        .0;
    assert_eq!(
        signature(&first),
        signature(&second),
        "cascade must be deterministic"
    );

    // The rerank saw the WHOLE tie group: the two zebra docs (ids 8 and
    // 4, both in the boundary tie group on shards 2 and 1) take the top
    // two spots, B3 first on tf. Had either been truncated from its
    // shard's candidates, a plain tied doc (id 0 or 1) would appear.
    let got_ids: Vec<u64> = first.iter().map(|h| h.doc_id).collect();
    assert_eq!(got_ids, vec![8, 4]);
    assert_eq!(first[0].rank, 1);
    assert_eq!(first[1].rank, 2);
    assert!(first[0].bm25_score > first[1].bm25_score);
    assert!(first[0].vector_score > 0.0);

    for h in handles {
        h.abort();
    }
    mock.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distributed_cascade_matches_monolithic_exactly() {
    let (analysis, mock) = start_mock_analysis().await;
    let corpus = build_corpus();
    let (shift, scale) = fit_calibration(DIM, 4, &corpus.vectors);

    let (addrs, handles) = start_cluster(&analysis, &corpus, 3, &shift, &scale, true).await;
    let (mono_addrs, mono_handles) =
        start_cluster(&analysis, &corpus, 1, &shift, &scale, true).await;

    let distributed =
        CoordinatorServiceImpl::new(addrs).with_bm25(Some(analysis.clone()), Default::default());
    let monolithic =
        CoordinatorServiceImpl::new(mono_addrs).with_bm25(Some(analysis), Default::default());

    for k in [2u32, 4, 12] {
        let got = distributed
            .fanout_cascade("d", "zebra", &corpus.query, k, None, 0.0, false)
            .await
            .unwrap()
            .0;
        let want = monolithic
            .fanout_cascade("m", "zebra", &corpus.query, k, None, 0.0, false)
            .await
            .unwrap()
            .0;
        assert_cascade_equivalent(&got, &want);
    }

    for h in handles {
        h.abort();
    }
    for h in mono_handles {
        h.abort();
    }
    mock.abort();
}

/// Early-termination equivalence: phase-1 candidates through the
/// floor-sharing bidi path (sharing ON) are identical to the full-scan
/// result (sharing OFF), tie group included.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn floor_shared_candidates_equal_full_scan_candidates() {
    let (analysis, mock) = start_mock_analysis().await;
    let corpus = build_corpus();
    let (shift, scale) = fit_calibration(DIM, 4, &corpus.vectors);

    let mut per_mode = Vec::new();
    for share in [true, false] {
        let (addrs, handles) = start_cluster(&analysis, &corpus, 3, &shift, &scale, share).await;
        let coordinator = CoordinatorServiceImpl::new(addrs);
        let result = coordinator
            .fanout_search("e", &corpus.query, 2, true)
            .await
            .unwrap();
        per_mode.push(result);
        for h in handles {
            h.abort();
        }
    }

    // The raw shard lists may legitimately differ: floor sharing prunes
    // candidates that cannot reach the pool (that pruning IS the
    // savings). The equivalence claim is on the POOL: merge each mode's
    // lists, take the global k-th score s_k, and compare
    // {score >= s_k} — it must be identical, tie group included.
    let pool_of = |result: &turbovec_search::coordinator::FanoutResult| {
        let mut all: Vec<(u64, u32)> = result
            .shard_hits
            .iter()
            .flat_map(|(_, hits)| hits.iter().map(|&(id, s)| (id, s.to_bits())))
            .collect();
        all.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        let boundary = if all.len() >= 2 { all[1].1 } else { 0 };
        let mut pool: Vec<(u64, u32)> = all.into_iter().filter(|h| h.1 >= boundary).collect();
        pool.sort();
        pool
    };
    let pool_on = pool_of(&per_mode[0]);
    let pool_off = pool_of(&per_mode[1]);
    assert_eq!(
        pool_on, pool_off,
        "floor sharing changed the candidate pool"
    );

    // The pool is the tie-extended set: doc 0 (top) plus all three
    // boundary-tied docs 1, 4, 8 — 4 members at k=2.
    let pool_ids: Vec<u64> = pool_on.iter().map(|(id, _)| *id).collect();
    assert_eq!(pool_ids, vec![0, 1, 4, 8]);

    mock.abort();
}

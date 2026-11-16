//! Regression tests verifying `SegmentedProvider` preserves `FilterRouting`
//! across unary `try_search` and streaming `try_search_streaming_controlled`.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use pipestream_search::analyzer::{analyze_document_native, body_spec};
use pipestream_search::boolean_bits::Bits;
use pipestream_search::exact_vectors::{dot, ExactVectorStore};
use pipestream_search::hnsw::{HnswConfig, HnswIndex, HNSW};
use pipestream_search::live_docs::LiveDocs;
use pipestream_search::postings::Bm25Store;
use pipestream_search::segmented_vectors::SegmentedProvider;
use pipestream_search::segments::{OpenedSegmentSet, SegmentCatalog, SegmentSource};
use pipestream_search::vector::{
    FilterRouting, VectorIndex, VectorProvider, VectorSearchOptions, VectorStreamControl,
    VectorStreamEndReason,
};

const DIM: usize = 16;
const SEG_ROWS: usize = 600;
const TAIL_ROWS: usize = 50;
const TOTAL_ROWS: usize = SEG_ROWS * 2 + TAIL_ROWS;

static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(1);

struct TempDir(PathBuf);

impl TempDir {
    fn new(prefix: &str) -> Self {
        let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("psearch-{prefix}-{}-{id}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create temp dir");
        Self(dir)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn deterministic_unit_vectors(rows: usize, dim: usize, seed: u64) -> Vec<f32> {
    let mut state = seed;
    let mut next_f32 = || -> f32 {
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        ((z >> 11) as f64 / (1u64 << 53) as f64) as f32 * 2.0 - 1.0
    };
    let mut out = Vec::with_capacity(rows * dim);
    for _ in 0..rows {
        let start = out.len();
        for _ in 0..dim {
            out.push(next_f32());
        }
        let norm: f32 = out[start..].iter().map(|v| v * v).sum::<f32>().sqrt();
        for v in &mut out[start..] {
            *v /= norm;
        }
    }
    out
}

fn build_empty_bm25(rows: usize) -> Bm25Store {
    let mut store = Bm25Store::with_text_fields(&["body"]);
    let doc = analyze_document_native("token", Some(&body_spec())).expect("analyze doc");
    for slot in 0..rows as u32 {
        store.add_document(slot, "token".to_string(), doc.clone());
    }
    store
}

fn build_segmented_hnsw_fixture(dir: &TempDir) -> (VectorIndex, Vec<f32>, Bits) {
    let corpus = deterministic_unit_vectors(TOTAL_ROWS, DIM, 0xC0DE_1650);
    let cfg = HnswConfig {
        m: 4,
        ef_construction: 64,
        ..HnswConfig::default()
    };
    let catalog_dir = dir.0.join("catalog");
    let catalog = SegmentCatalog::open(&catalog_dir).expect("open catalog");

    for seg in 0..2usize {
        let stage = dir.0.join(format!("stage-{seg}"));
        std::fs::create_dir_all(&stage).expect("stage dir");
        let base = seg * SEG_ROWS;
        let slice = &corpus[base * DIM..(base + SEG_ROWS) * DIM];

        let mut hnsw = HnswIndex::new(DIM, cfg).expect("hnsw segment");
        hnsw.add(slice, DIM).expect("add slice");
        let vec_path = stage.join("vector.index");
        hnsw.write(&vec_path).expect("write hnsw");

        let exact_path = stage.join("vectors.f32");
        ExactVectorStore::from_values(DIM, slice.to_vec())
            .expect("exact store")
            .write(&exact_path)
            .expect("write exact");

        let bm25_path = stage.join("documents.bm25");
        build_empty_bm25(SEG_ROWS)
            .save(&bm25_path)
            .expect("save bm25");
        let live_path = stage.join("live.bin");
        LiveDocs::default()
            .write(&live_path, SEG_ROWS as u64)
            .expect("write live");

        catalog
            .append(SegmentSource {
                multi_res_path: None,
                segment_id: &format!("seg-{seg:04}"),
                generation: seg as u64 + 1,
                base_label: base as u64,
                backend_kind: HNSW,
                vector_path: Some(&vec_path),
                exact_vector_path: Some(&exact_path),
                bm25_path: &bm25_path,
                live_docs_path: &live_path,
                partition_column: None,
            })
            .expect("append segment");
    }

    let set = Arc::new(OpenedSegmentSet::open(&catalog_dir).expect("open set"));
    assert!(set.vector(0).expect("seg 0").is_graph_linked());
    assert!(set.vector(1).expect("seg 1").is_graph_linked());

    let tail = HnswIndex::new(DIM, cfg).expect("tail hnsw").into_index();
    let provider = SegmentedProvider::open(set, tail).expect("open segmented provider");
    let mut index = VectorIndex::from_provider(provider);
    index
        .add(&corpus[SEG_ROWS * 2 * DIM..], DIM)
        .expect("add tail");

    // Allow every even row: 300 rows per 600-row segment and 25 rows in the tail.
    // At m=4 (m0=8) and ef=16, walk_units = 128:
    // - Speed routing has outright = 128 (< 300) and 50% local share -> Plain walk.
    // - Exact routing has outright = 512 (>= 300) -> Enumerate on every segment.
    let mut allow = Bits::empty(TOTAL_ROWS);
    for row in (0..TOTAL_ROWS).step_by(2) {
        allow.set(row);
    }
    (index, corpus, allow)
}

fn exact_allowed_topk(corpus: &[f32], query: &[f32], allow: &Bits, k: usize) -> Vec<(i64, f32)> {
    let mut scored: Vec<(i64, f32)> = (0..TOTAL_ROWS)
        .filter(|&row| allow.test(row))
        .map(|row| {
            let start = row * DIM;
            (row as i64, dot(&corpus[start..start + DIM], query))
        })
        .collect();
    scored.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    scored.truncate(k);
    scored
}

#[test]
fn segmented_try_search_forwards_exact_filter_routing() {
    let dir = TempDir::new("wave2-seg-routing-unary");
    let (index, corpus, allow) = build_segmented_hnsw_fixture(&dir);
    let queries = deterministic_unit_vectors(4, DIM, 0xBEEF_0165);
    let k = 8usize;

    for query in queries.as_chunks::<DIM>().0 {
        let speed = index
            .try_search(
                query,
                k,
                VectorSearchOptions::new()
                    .with_bits(&allow)
                    .with_candidate_depth(16)
                    .with_filter_routing(FilterRouting::Speed),
            )
            .expect("speed search");
        assert_eq!(
            speed.regimes.plain, 2,
            "speed routing walks linked segments plain"
        );

        let exact = index
            .try_search(
                query,
                k,
                VectorSearchOptions::new()
                    .with_bits(&allow)
                    .with_candidate_depth(16)
                    .with_filter_routing(FilterRouting::Exact),
            )
            .expect("exact search");
        assert_eq!(
            exact.regimes.enumerate, 3,
            "FilterRouting::Exact must enumerate both sealed segments and the tail"
        );
        assert_eq!(exact.regimes.plain, 0);
        assert_eq!(exact.regimes.filter_aware, 0);

        let want = exact_allowed_topk(&corpus, query, &allow, k);
        let got: Vec<(i64, u32)> = exact
            .slots_for_query(0)
            .iter()
            .copied()
            .zip(exact.scores_for_query(0).iter().map(|s| s.to_bits()))
            .collect();
        let want_bits: Vec<(i64, u32)> =
            want.into_iter().map(|(id, s)| (id, s.to_bits())).collect();
        assert_eq!(got, want_bits);
    }
}

#[test]
fn segmented_streaming_search_forwards_exact_filter_routing() {
    let dir = TempDir::new("wave2-seg-routing-stream");
    let (index, corpus, allow) = build_segmented_hnsw_fixture(&dir);
    let query = deterministic_unit_vectors(1, DIM, 0xCAFE_0165);

    let mut emitted_hits: Vec<(i64, f32)> = Vec::new();
    let summary = index
        .try_search_streaming_controlled(
            &query,
            VectorSearchOptions::new()
                .with_bits(&allow)
                .with_candidate_depth(16)
                .with_filter_routing(FilterRouting::Exact),
            |batch| {
                for (&slot, &score) in batch.slots.iter().zip(batch.scores) {
                    if slot >= 0 {
                        emitted_hits.push((slot, score));
                    }
                }
                VectorStreamControl::Continue
            },
            || VectorStreamControl::Continue,
        )
        .expect("streaming search");

    assert_eq!(
        summary.regimes.enumerate, 3,
        "streaming FilterRouting::Exact must enumerate both sealed segments and the tail"
    );
    assert_eq!(summary.regimes.plain, 0);
    assert_eq!(summary.regimes.filter_aware, 0);
    assert_eq!(summary.end_reason, VectorStreamEndReason::Exhausted);
    assert_eq!(emitted_hits.len(), allow.count() as usize);

    for &(slot, score) in &emitted_hits {
        let row = slot as usize;
        assert!(allow.test(row));
        let want = dot(&corpus[row * DIM..(row + 1) * DIM], &query);
        assert_eq!(score.to_bits(), want.to_bits());
    }
}

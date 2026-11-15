//! Regression tests for IEEE-754 subnormal inner-product slack across HNSW
//! `minimum_score` floors, top-k exact rescore windows, streaming pre-filters,
//! and layer-0 floor pruning.

use pipestream_search::boolean_bits::Bits;
use pipestream_search::exact_vectors::dot;
use pipestream_search::hnsw::{HnswConfig, HnswIndex, MIN_GRAPH_ROWS};
use pipestream_search::vector::{
    FilterRegime, FloorPruneSpec, VectorProvider, VectorSearchOptions, VectorStreamControl,
};

const DIM: usize = 64;

fn unit_pad_rows(count: usize, dim: usize) -> Vec<f32> {
    let mut rows = vec![0.0f32; count * dim];
    for r in 0..count {
        // Orthogonal-to-query negative-direction filler on coordinate `dim - 1`.
        rows[r * dim + (dim - 1)] = -1.0;
    }
    rows
}

fn subnormal_query_and_target() -> (Vec<f32>, Vec<f32>, f32) {
    let u = f32::from_bits(1);
    let mut query = vec![0.0f32; DIM];
    query[0] = u;
    query[32] = u;
    query[DIM - 1] = u;

    let mut target = vec![0.0f32; DIM];
    target[0] = 1.0;
    target[32] = -0.5;

    let exact_target = dot(&target, &query);
    assert_eq!(exact_target.to_bits(), u.to_bits());
    (query, target, exact_target)
}

#[test]
fn subnormal_min_score_is_not_falsely_pruned_in_unlinked_search() {
    let (query, target, exact_target) = subnormal_query_and_target();

    let mut unlinked = HnswIndex::new(DIM, HnswConfig::default()).expect("new hnsw");
    let mut small_corpus = unit_pad_rows(16, DIM);
    small_corpus[5 * DIM..6 * DIM].copy_from_slice(&target);
    unlinked.add(&small_corpus, DIM).expect("add small");
    unlinked.prepare().expect("prepare unlinked");
    assert!(!unlinked.is_graph_linked());

    let res = unlinked
        .search(
            &query,
            1,
            VectorSearchOptions::new().with_minimum_score(exact_target),
        )
        .expect("unlinked search");
    assert_eq!(res.slots, vec![5]);
    assert_eq!(res.scores[0].to_bits(), exact_target.to_bits());
}

#[test]
fn subnormal_min_score_is_not_falsely_pruned_in_enumerate_or_floor_prune_search() {
    let (query, target, exact_target) = subnormal_query_and_target();

    let total = MIN_GRAPH_ROWS + 64;
    let mut linked_corpus = unit_pad_rows(total, DIM);
    let target_row = total - 7;
    linked_corpus[target_row * DIM..(target_row + 1) * DIM].copy_from_slice(&target);

    let mut linked = HnswIndex::new(DIM, HnswConfig::default()).expect("new linked hnsw");
    linked.add(&linked_corpus, DIM).expect("add linked");
    linked.prepare().expect("prepare linked");
    assert!(linked.is_graph_linked());

    let mut allow = Bits::empty(total);
    for r in (0..total).step_by(4) {
        allow.set(r);
    }
    allow.set(target_row);

    let enum_res = linked
        .search(
            &query,
            1,
            VectorSearchOptions::new()
                .with_bits(&allow)
                .with_filter_regime(FilterRegime::Enumerate)
                .with_minimum_score(exact_target),
        )
        .expect("enumerate search");
    assert_eq!(enum_res.slots, vec![target_row as i64]);
    assert_eq!(enum_res.scores[0].to_bits(), exact_target.to_bits());

    let prune_res = linked
        .search(
            &query,
            1,
            VectorSearchOptions::new()
                .with_candidate_depth(64)
                .with_minimum_score(exact_target)
                .with_floor_prune(FloorPruneSpec {
                    enabled: true,
                    floor_k: 1,
                    share_ppm: 0,
                    greediness_ppm: 1_000_000,
                    clamp_min: 1,
                }),
        )
        .expect("floor-pruned search");
    assert_eq!(prune_res.slots, vec![target_row as i64]);
    assert_eq!(prune_res.scores[0].to_bits(), exact_target.to_bits());
}

#[test]
fn subnormal_topk_rescore_window_keeps_exact_winner_over_fma_distractor() {
    let u = f32::from_bits(1);
    let u3 = f32::from_bits(3);

    let mut query = vec![0.0f32; DIM];
    query[0] = u;
    query[1] = u;
    query[32] = u3;
    query[33] = u;

    // Row 0 (distractor): scalar dot = u + u + (-2u) = 0.0, while FMA lane 0
    // rounds fma(3u, -0.5, u) = -0.5u to -0.0 and lane 1 keeps +u -> FMA = +u.
    let mut distractor = vec![0.0f32; DIM];
    distractor[0] = 1.0;
    distractor[1] = 1.0;
    distractor[32] = -0.5;

    // Row 1 (true winner): scalar dot = u + (-0.0) = +u, while FMA lane 1
    // rounds fma(u, -0.5, u) = +0.5u to 0.0 -> FMA = 0.0.
    let mut winner = vec![0.0f32; DIM];
    winner[1] = 1.0;
    winner[33] = -0.5;

    assert_eq!(dot(&distractor, &query), 0.0);
    assert_eq!(dot(&winner, &query).to_bits(), u.to_bits());

    let mut corpus = Vec::with_capacity(2 * DIM);
    corpus.extend_from_slice(&distractor);
    corpus.extend_from_slice(&winner);

    let mut index = HnswIndex::new(DIM, HnswConfig::default()).expect("new hnsw");
    index.add(&corpus, DIM).expect("add rows");
    index.prepare().expect("prepare");

    let res = index
        .search(&query, 1, VectorSearchOptions::new())
        .expect("top-1 search");
    assert_eq!(
        res.slots,
        vec![1],
        "true exact winner (row 1, +u) must not be pruned behind FMA distractor (row 0, 0.0)"
    );
    assert_eq!(res.scores[0].to_bits(), u.to_bits());
}

#[test]
fn subnormal_stream_rows_and_exponent_split_preserve_exact_floor_hits() {
    let u = f32::from_bits(1);

    // Split the subnormal product exponent across normal `query` and `row`
    // coordinates (`2^-75 * 2^-74 = 2^-149 = u`) so `inv_norm` is ~2^74.
    let q_scale = 2.0f32.powi(-75);
    let r_hi = 2.0f32.powi(-74);
    let r_lo = -2.0f32.powi(-75);

    let mut query = vec![0.0f32; DIM];
    query[0] = q_scale;
    query[32] = q_scale;

    let mut target = vec![0.0f32; DIM];
    target[0] = r_hi;
    target[32] = r_lo;
    assert_eq!(dot(&target, &query).to_bits(), u.to_bits());

    let mut corpus = unit_pad_rows(40, DIM);
    corpus[37 * DIM..38 * DIM].copy_from_slice(&target);

    let mut index = HnswIndex::new(DIM, HnswConfig::default()).expect("new hnsw");
    index.add(&corpus, DIM).expect("add rows");
    index.prepare().expect("prepare");

    let mut seen = Vec::new();
    let summary = index
        .search_streaming_controlled(
            &query,
            VectorSearchOptions::new()
                .with_candidate_depth(16)
                .with_minimum_score(u),
            &mut |batch| {
                for (&slot, &score) in batch.slots.iter().zip(batch.scores) {
                    if slot >= 0 {
                        seen.push((slot, score));
                    }
                }
                VectorStreamControl::Continue
            },
            &mut || VectorStreamControl::Continue,
        )
        .expect("stream search");

    assert!(summary.completed);
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].0, 37);
    assert_eq!(seen[0].1.to_bits(), u.to_bits());
}

//! Segment merges: donor reinsertion and join-set grafting against a full rebuild.

use std::path::Path;

use super::super::{MergeStrategy, SegmentMergeInput, MIN_GRAPH_ROWS};
use super::*;
use crate::vector::Mask;

/// The corpus, queries, and build configuration every merge layout shares.
struct MergeCase {
    dim: usize,
    n: usize,
    qn: usize,
    vectors: Vec<f32>,
    queries: Vec<f32>,
    cfg: HnswConfig,
}

/// One index built over the whole corpus, and its recall@10.
struct Rebuilt {
    index: HnswIndex,
    recall: f64,
}

fn merge_case() -> MergeCase {
    let dim = 32;
    let n = 2400usize;
    let qn = 60usize;
    let (vectors, queries) = corpus_with_queries(n, qn, dim, 0x9999_2480);
    let cfg = HnswConfig {
        m: 16,
        ef_construction: 120,
        seed: 101,
        build_mode: BuildMode::FixedOrder,
        locality_order: super::super::config::LocalityOrder::None,
    };
    MergeCase {
        dim,
        n,
        qn,
        vectors,
        queries,
        cfg,
    }
}

fn rebuilt_baseline(c: &MergeCase) -> Rebuilt {
    let (dim, qn, cfg) = (c.dim, c.qn, c.cfg);
    let (vectors, queries) = (&c.vectors[..], &c.queries[..]);
    let mut rebuilt = HnswIndex::new(dim, cfg).unwrap();
    rebuilt.add(vectors, dim).unwrap();
    rebuilt.prepare().unwrap();
    let rebuilt_res = rebuilt
        .search(
            queries,
            10,
            VectorSearchOptions::new().with_candidate_depth(80),
        )
        .unwrap();
    let mut rebuilt_hits = 0usize;
    for q_idx in 0..qn {
        let q = &queries[q_idx * dim..(q_idx + 1) * dim];
        let truth = exact_top(vectors, dim, q, 10);
        rebuilt_hits += truth
            .iter()
            .filter(|&&id| rebuilt_res.slots_for_query(q_idx).contains(&i64::from(id)))
            .count();
    }
    let rebuilt_recall = rebuilt_hits as f64 / (qn * 10) as f64;
    Rebuilt {
        index: rebuilt,
        recall: rebuilt_recall,
    }
}

/// One prepared segment per count, in corpus order.
fn staged_segments(c: &MergeCase, dir: &Path, label: &str, counts: &[usize]) -> Vec<HnswIndex> {
    let (dim, n, cfg, vectors) = (c.dim, c.n, c.cfg, &c.vectors[..]);
    let mut segs: Vec<HnswIndex> = Vec::with_capacity(counts.len());
    let mut cursor = 0usize;
    for (i, &cnt) in counts.iter().enumerate() {
        let mut s = HnswIndex::new(dim, cfg).unwrap();
        s.add(&vectors[cursor * dim..(cursor + cnt) * dim], dim)
            .unwrap();
        s.prepare().unwrap();
        // Mix Mapped and Heap source segments to verify generic `LinkCells` dispatch.
        if i % 2 == 1 {
            let p = dir.join(format!("{label}-{i}.hnsw"));
            s.write(&p).unwrap();
            segs.push(HnswIndex::load_mapped(&p).unwrap());
        } else {
            segs.push(s);
        }
        cursor += cnt;
    }
    assert_eq!(cursor, n);
    segs
}

fn every_strategy_matches_the_rebuild(
    c: &MergeCase,
    dir: &Path,
    (label, counts): (&str, &[usize]),
    inputs: &[SegmentMergeInput<'_>],
    baseline: &Rebuilt,
) {
    let (dim, n, qn, cfg) = (c.dim, c.n, c.qn, c.cfg);
    let (vectors, queries) = (&c.vectors[..], &c.queries[..]);
    let (rebuilt, rebuilt_recall) = (&baseline.index, baseline.recall);
    for strategy in [MergeStrategy::DonorReinsert, MergeStrategy::JoinSet] {
        let (merged, stats) = HnswIndex::merge_segments(dim, cfg, inputs, &[], strategy).unwrap();
        assert_eq!(merged.len(), n);
        assert_eq!(merged.unreachable_from_entry(), 0, "{label} {strategy:?}");
        assert_eq!(stats.donor_segment, Some(0));
        assert_eq!(stats.donor_rows, counts[0]);
        if strategy == MergeStrategy::JoinSet {
            let non_donor = n - counts[0];
            assert!(
                stats.join_set_rows < non_donor,
                "{label}: expected join_set ({}) < non_donor ({non_donor})",
                stats.join_set_rows
            );
            assert!(stats.grafted_rows > 0);
        }

        let out_path = dir.join(format!("{label}-{strategy:?}.hnsw"));
        merged.write(&out_path).unwrap();
        let mapped = HnswIndex::load_mapped(&out_path).unwrap();
        assert_eq!(mapped.row_identity(), rebuilt.row_identity());

        let res = mapped
            .search(
                queries,
                10,
                VectorSearchOptions::new().with_candidate_depth(80),
            )
            .unwrap();
        let mut hits = 0usize;
        for q_idx in 0..qn {
            let q = &queries[q_idx * dim..(q_idx + 1) * dim];
            let truth = exact_top(vectors, dim, q, 10);
            hits += truth
                .iter()
                .filter(|&&id| res.slots_for_query(q_idx).contains(&i64::from(id)))
                .count();
        }
        let recall = hits as f64 / (qn * 10) as f64;
        assert!(
            recall + 0.005 >= rebuilt_recall,
            "{label} {strategy:?}: recall {recall:.4} vs rebuild {rebuilt_recall:.4}"
        );
    }
}

fn a_parallel_join_set_merge_keeps_recall(c: &MergeCase, inputs: &[SegmentMergeInput<'_>]) {
    let (dim, qn, cfg) = (c.dim, c.qn, c.cfg);
    let (vectors, queries) = (&c.vectors[..], &c.queries[..]);
    let par_cfg = HnswConfig {
        build_mode: BuildMode::Parallel,
        ..cfg
    };
    let (par_merged, _) =
        HnswIndex::merge_segments(dim, par_cfg, inputs, &[], MergeStrategy::JoinSet).unwrap();
    assert_eq!(par_merged.unreachable_from_entry(), 0);
    let par_res = par_merged
        .search(
            queries,
            10,
            VectorSearchOptions::new().with_candidate_depth(80),
        )
        .unwrap();
    let mut par_hits = 0usize;
    for q_idx in 0..qn {
        let q = &queries[q_idx * dim..(q_idx + 1) * dim];
        let truth = exact_top(vectors, dim, q, 10);
        par_hits += truth
            .iter()
            .filter(|&&id| par_res.slots_for_query(q_idx).contains(&i64::from(id)))
            .count();
    }
    let par_recall = par_hits as f64 / (qn * 10) as f64;
    assert!(
        par_recall >= 0.985,
        "parallel 4-equal recall {par_recall:.4}"
    );
}

fn degenerate_merges_rebuild_or_stay_unlinked(c: &MergeCase) {
    let (dim, cfg, vectors) = (c.dim, c.cfg, &c.vectors[..]);
    // No-donor rebuild fallback when the sole linked donor is 100% deleted and
    // surviving rows come from raw/unlinked vectors >= MIN_GRAPH_ROWS.
    let mut donor_dead = HnswIndex::new(dim, cfg).unwrap();
    donor_dead.add(&vectors[..300 * dim], dim).unwrap();
    donor_dead.prepare().unwrap();
    let dead_mask = vec![false; 300];
    let (no_donor, nd_stats) = HnswIndex::merge_segments(
        dim,
        cfg,
        &[SegmentMergeInput {
            index: &donor_dead,
            live: Some(Mask::Bools(&dead_mask)),
        }],
        &vectors[300 * dim..650 * dim],
        MergeStrategy::JoinSet,
    )
    .unwrap();
    assert!(nd_stats.used_no_donor_rebuild);
    assert!(no_donor.is_graph_linked());
    assert_eq!(no_donor.len(), 350);
    assert_eq!(no_donor.unreachable_from_entry(), 0);

    // Output below MIN_GRAPH_ROWS stays unlinked.
    let (tiny, _) = HnswIndex::merge_segments(
        dim,
        cfg,
        &[],
        &vectors[..(MIN_GRAPH_ROWS - 16) * dim],
        MergeStrategy::JoinSet,
    )
    .unwrap();
    assert!(!tiny.is_graph_linked());
}

#[test]
fn join_set_merge_2_4_8_equal_and_skewed_segments_matches_rebuild_recall() {
    let c = merge_case();
    let baseline = rebuilt_baseline(&c);
    let layouts: &[(&str, &[usize])] = &[
        ("2-equal", &[1200, 1200]),
        ("4-equal", &[600, 600, 600, 600]),
        ("8-equal", &[300, 300, 300, 300, 300, 300, 300, 300]),
        ("4-skewed", &[1200, 500, 400, 300]),
    ];

    let dir = temp_dir("join-set-merge-layouts");
    for &(label, counts) in layouts {
        let segs = staged_segments(&c, &dir, label, counts);

        let inputs: Vec<SegmentMergeInput<'_>> = segs
            .iter()
            .map(|index| SegmentMergeInput { index, live: None })
            .collect();

        every_strategy_matches_the_rebuild(&c, &dir, (label, counts), &inputs, &baseline);
        // Also exercise parallel mode on 4-equal to cover concurrent Locks + graft.
        if label == "4-equal" {
            a_parallel_join_set_merge_keeps_recall(&c, &inputs);
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
    degenerate_merges_rebuild_or_stay_unlinked(&c);
}

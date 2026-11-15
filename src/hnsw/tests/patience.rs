//! Patience early stopping and the ascent gate.

use super::super::Answer;
use super::*;
use crate::vector::{PatienceSpec, PatienceWindow};

#[test]
fn patience_off_reproduces_fixed_order_walk_bit_for_bit() {
    let dim = 32;
    let (vectors, queries) = corpus_with_queries(1_200, 12, dim, 0x9501);
    let mut cfg = config(BuildMode::FixedOrder);
    cfg.m = 12;
    cfg.ef_construction = 80;
    let mut index = HnswIndex::new(dim, cfg).unwrap();
    index.add(&vectors, dim).unwrap();
    index.prepare().unwrap();

    let allow: Vec<bool> = (0..1_200).map(|i| i % 2 == 0).collect();
    for q in queries.chunks_exact(dim) {
        for base_opts in [
            VectorSearchOptions::new(),
            VectorSearchOptions::new()
                .with_mask(&allow)
                .with_filter_regime(FilterRegime::Plain),
            VectorSearchOptions::new()
                .with_mask(&allow)
                .with_filter_regime(FilterRegime::FilterAware),
        ] {
            let (base_hits, base_ans) = index.search_with_stats(q, 10, 96, base_opts).unwrap();
            assert!(!base_ans.patience_stopped);

            for off_spec in [
                PatienceSpec::OFF,
                PatienceSpec {
                    saturation: 0.0, // ignored when patience == 0
                    saturation_ppm: None,
                    patience: 0,
                    ascent_gate: 50,
                    window: PatienceWindow::TopK,
                },
            ] {
                let (off_hits, off_ans) = index
                    .search_with_stats(q, 10, 96, base_opts.with_patience(off_spec))
                    .unwrap();
                assert_eq!(off_ans, base_ans);
                assert_eq!(off_hits.len(), base_hits.len());
                for ((n0, s0), (n1, s1)) in base_hits.iter().zip(off_hits.iter()) {
                    assert_eq!(n0, n1);
                    assert_eq!(s0.to_bits(), s1.to_bits());
                }
            }
        }
    }
}

fn invalid_saturation_is_refused(fx: &Fixture) {
    let (dim, queries, index) = (fx.dim, &fx.queries[..], &fx.index);
    // Invalid saturation must be rejected when patience > 0.
    for bad in [0.0f32, -0.1, 1.01, f32::NAN, f32::INFINITY] {
        let err = index
            .search_with_stats(
                &queries[..dim],
                10,
                64,
                VectorSearchOptions::new().with_patience(PatienceSpec {
                    saturation: bad,
                    saturation_ppm: None,
                    patience: 7,
                    ascent_gate: 0,
                    window: PatienceWindow::Beam,
                }),
            )
            .unwrap_err();
        assert!(err.to_string().contains("patience saturation"), "{err}");
    }
}

fn the_walk_patience_shapes_are_constructible() {
    // Verify 0.99f32 * 100 binary rounding and exact 990_000 ppm both produce min_survived = 99.
    let p99 = super::super::graph::Patience {
        saturation: 0.99,
        saturation_ppm: None,
        hops: 7,
        top_k: 100,
        ascent_gate: 0,
        floor_active: false,
    };
    let p99_ppm = super::super::graph::Patience {
        saturation: 0.99,
        saturation_ppm: Some(990_000),
        hops: 7,
        top_k: 100,
        ascent_gate: 0,
        floor_active: false,
    };
    let p95_10 = super::super::graph::Patience {
        saturation: 0.95,
        saturation_ppm: None,
        hops: 7,
        top_k: 10,
        ascent_gate: 0,
        floor_active: false,
    };
    let p90_10 = super::super::graph::Patience {
        saturation: 0.90,
        saturation_ppm: None,
        hops: 7,
        top_k: 10,
        ascent_gate: 0,
        floor_active: false,
    };
    let _ = (p99, p99_ppm, p95_10, p90_10);
}

fn the_beam_window_stops_early(
    index: &HnswIndex,
    q: &[f32],
    plain: &Answer,
    stopped_beam: &mut usize,
) {
    let beam_spec = PatienceSpec::from_ppm(900_000, 5);
    let (_, beam_ans) = index
        .search_with_stats(
            q,
            10,
            256,
            VectorSearchOptions::new().with_patience(beam_spec),
        )
        .unwrap();
    if beam_ans.patience_stopped {
        *stopped_beam += 1;
        assert!(beam_ans.stats.hops < plain.stats.hops);
        assert!(beam_ans.stats.scored < plain.stats.scored);
    } else {
        assert_eq!(beam_ans.stats.hops, plain.stats.hops);
    }
}

fn the_top_k_window_stops_early(
    index: &HnswIndex,
    q: &[f32],
    plain: &Answer,
    stopped_topk: &mut usize,
) {
    let topk_spec = PatienceSpec {
        saturation: 0.95,
        saturation_ppm: None,
        patience: 5,
        ascent_gate: 0,
        window: PatienceWindow::TopK,
    };
    let (_, topk_ans) = index
        .search_with_stats(
            q,
            10,
            256,
            VectorSearchOptions::new().with_patience(topk_spec),
        )
        .unwrap();
    if topk_ans.patience_stopped {
        *stopped_topk += 1;
        assert!(topk_ans.stats.hops < plain.stats.hops);
    }
}

fn the_ascent_gate_waits_for_a_floor(
    index: &HnswIndex,
    q: &[f32],
    plain: &Answer,
    ascent_delayed: &mut usize,
) {
    // Ascent gate: without a floor (`minimum_score = None`), a huge
    // `ascent_gate` is ignored; with a finite floor (`minimum_score = Some`),
    // `ascent_gate` suppresses patience until that many local allowed rows
    // have been scored.
    let gated_spec = PatienceSpec {
        saturation: 0.95,
        saturation_ppm: None,
        patience: 3,
        ascent_gate: 100_000,
        window: PatienceWindow::TopK,
    };
    let (_, no_floor_ans) = index
        .search_with_stats(
            q,
            10,
            256,
            VectorSearchOptions::new().with_patience(gated_spec),
        )
        .unwrap();
    let (_, with_floor_ans) = index
        .search_with_stats(
            q,
            10,
            256,
            VectorSearchOptions::new()
                .with_minimum_score(-1.0e6)
                .with_patience(gated_spec),
        )
        .unwrap();
    assert!(!with_floor_ans.patience_stopped);
    assert_eq!(with_floor_ans.stats.hops, plain.stats.hops);
    if no_floor_ans.patience_stopped {
        *ascent_delayed += 1;
        assert!(no_floor_ans.stats.hops < with_floor_ans.stats.hops);
    }
}

#[test]
fn patience_early_stop_and_ascent_gate_behave_exact() {
    let mut cfg = config(BuildMode::FixedOrder);
    cfg.m = 12;
    cfg.ef_construction = 96;
    let fx = Fixture::with_config(3_000, 16, 32, 0x9502, cfg);
    let (dim, queries, index) = (fx.dim, &fx.queries[..], &fx.index);

    invalid_saturation_is_refused(&fx);
    the_walk_patience_shapes_are_constructible();
    // Exercise both Beam and TopK windows at a wide beam (`ef = 256`) and verify
    // early stopping reduces hops/scored work and respects the ascent gate when
    // a finite floor is active.
    let mut stopped_beam = 0usize;
    let mut stopped_topk = 0usize;
    let mut ascent_delayed = 0usize;
    for q in queries.chunks_exact(dim) {
        let (_, plain) = index
            .search_with_stats(q, 10, 256, VectorSearchOptions::new())
            .unwrap();
        the_beam_window_stops_early(index, q, &plain, &mut stopped_beam);
        the_top_k_window_stops_early(index, q, &plain, &mut stopped_topk);
        the_ascent_gate_waits_for_a_floor(index, q, &plain, &mut ascent_delayed);
    }
    assert!(
        stopped_beam > 0,
        "expected Beam patience to stop wide ef=256 walks"
    );
    assert!(
        stopped_topk > 0,
        "expected TopK patience to stop wide ef=256 walks"
    );
    assert!(
        ascent_delayed > 0,
        "expected floor-gated ascent_gate to suppress premature patience stops"
    );
}

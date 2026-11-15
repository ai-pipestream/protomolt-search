//! Filter regimes: how the regime follows the mask, exact enumeration, and regimes under streaming.

use super::*;

fn the_regime_follows_the_mask_cardinality(fx: &Fixture) {
    let index = &fx.index;
    let query = &fx.queries[..fx.dim];
    let n = fx.rows();
    let regime_at = |allowed_every: usize, invert: bool| {
        let allow: Vec<bool> = (0..n).map(|i| (i % allowed_every == 0) != invert).collect();
        index
            .search_with_stats(query, 10, 16, VectorSearchOptions::new().with_mask(&allow))
            .unwrap()
            .1
            .regime
    };
    // ef 16 at M 16: enumerate up to 16 * 32 = 512 allowed rows outright,
    // and up to 4 * 16 * 32 = 2048 where the far probe finds the rows
    // around the query mostly disallowed.
    assert_eq!(regime_at(100, false), Some(FilterRegime::Enumerate)); // 200 rows
    assert_eq!(regime_at(12, false), Some(FilterRegime::Enumerate)); // 1667 rows, 8%
    assert_eq!(regime_at(8, false), Some(FilterRegime::FilterAware)); // 2500 rows, 12.5%
                                                                      // From a 20% local share the walk goes plain.
    assert_eq!(regime_at(3, false), Some(FilterRegime::Plain)); // 33%
    assert_eq!(regime_at(20, true), Some(FilterRegime::Plain)); // 95%
                                                                // A mask that allows everything is no filter at all.
    assert_eq!(regime_at(1, false), None);
}

fn routing_decides_between_walking_and_enumerating(fx: &Fixture) {
    let (dim, vectors, index) = (fx.dim, &fx.vectors[..], &fx.index);
    let query = &fx.queries[..fx.dim];
    let n = fx.rows();
    // A mask gathered around the query is allowed locally whatever its
    // global share: walked plain.
    let near = region_mask(vectors, dim, query, 10);
    let (_, answer) = index
        .search_with_stats(query, 10, 16, VectorSearchOptions::new().with_mask(&near))
        .unwrap();
    assert_eq!(answer.regime, Some(FilterRegime::Plain)); // 2000 rows, 10%
                                                          // Speed walks a mask gathered around the query that it could have
                                                          // enumerated; exact routing enumerates every mask up to the threshold
                                                          // and returns the exact answer.
    let small = region_mask(vectors, dim, query, 5); // 1000 rows: 512 < 1000 <= 2048
    let truth = exact_top_masked(vectors, dim, query, &small, 10);
    let (_, speed) = index
        .search_with_stats(query, 10, 16, VectorSearchOptions::new().with_mask(&small))
        .unwrap();
    assert_eq!(speed.regime, Some(FilterRegime::Plain));
    let exact_opts = VectorSearchOptions::new()
        .with_mask(&small)
        .with_filter_routing(FilterRouting::Exact);
    let (hits, exact) = index.search_with_stats(query, 10, 16, exact_opts).unwrap();
    assert_eq!(exact.regime, Some(FilterRegime::Enumerate));
    assert_eq!(hits, truth);
    // Above the threshold both route alike.
    let wide = region_mask(vectors, dim, query, 20);
    let (_, answer) = index
        .search_with_stats(
            query,
            10,
            16,
            VectorSearchOptions::new()
                .with_mask(&wide)
                .with_filter_routing(FilterRouting::Exact),
        )
        .unwrap();
    assert_eq!(answer.regime, Some(FilterRegime::Plain));
    // A wider beam moves the enumerate threshold with it.
    let sparse: Vec<bool> = (0..n).map(|i| i % 8 == 0).collect();
    let (_, answer) = index
        .search_with_stats(query, 10, 64, VectorSearchOptions::new().with_mask(&sparse))
        .unwrap();
    assert_eq!(answer.regime, Some(FilterRegime::Enumerate)); // 2500 <= 8192
}

fn a_pinned_regime_is_honored_in_every_shape(fx: &Fixture) {
    let index = &fx.index;
    let query = &fx.queries[..fx.dim];
    let n = fx.rows();
    let allow: Vec<bool> = (0..n).map(|i| i % 4 == 0).collect();

    for regime in [
        FilterRegime::Enumerate,
        FilterRegime::FilterAware,
        FilterRegime::Plain,
    ] {
        let options = VectorSearchOptions::new()
            .with_mask(&allow)
            .with_filter_regime(regime);
        let (hits, answer) = index.search_with_stats(query, 10, 16, options).unwrap();
        assert_eq!(answer.regime, Some(regime));
        assert_eq!(hits.len(), 10);
        // The mask as a bitmap is the same search.
        let bits = Bits::from_bools(&allow);
        let options = VectorSearchOptions::new()
            .with_bits(&bits)
            .with_filter_regime(regime);
        let (same, as_bits) = index.search_with_stats(query, 10, 16, options).unwrap();
        assert_eq!(same, hits, "{regime:?}");
        assert_eq!(as_bits, answer, "{regime:?}");
        assert!(
            hits.iter().all(|(row, _)| allow[*row as usize]),
            "{regime:?}"
        );
        // No mask, no regime to pin.
        let error = index
            .search_with_stats(
                query,
                10,
                16,
                VectorSearchOptions::new().with_filter_regime(regime),
            )
            .unwrap_err();
        assert!(error.to_string().contains("no mask"), "{error}");
        // An empty mask returns nothing in every regime.
        let none = vec![false; n];
        let options = VectorSearchOptions::new()
            .with_mask(&none)
            .with_filter_regime(regime);
        let (hits, _) = index.search_with_stats(query, 10, 16, options).unwrap();
        assert!(hits.is_empty(), "{regime:?}");
    }
}

#[test]
fn the_regime_follows_the_mask_cardinality_and_a_pinned_regime_is_honored() {
    let fx = Fixture::new(20_000, 1, 16, 0x55A1, BuildMode::Parallel);
    the_regime_follows_the_mask_cardinality(&fx);
    routing_decides_between_walking_and_enumerating(&fx);
    a_pinned_regime_is_honored_in_every_shape(&fx);
}

#[test]
fn enumerate_is_the_exact_top_k_of_the_mask_on_every_shape() {
    let dim = 17;
    let n = 6000;
    let (mut vectors, queries) = corpus_with_queries(n, 6, dim, 0xE0E0);
    // Rows of very different lengths, negated rows, and a zero row: the
    // kernel bound has to hold for all of them.
    let mut rng = Rng(0xBEEF);
    for (i, row) in vectors.chunks_exact_mut(dim).enumerate() {
        let scale = match i % 5 {
            0 => 1e-3,
            1 => 1.0,
            2 => -7.5,
            3 => 250.0,
            _ => 1.0 + rng.unit() as f32,
        };
        for x in row.iter_mut() {
            *x *= scale;
        }
    }
    vectors[dim * 40..dim * 41].fill(0.0);
    // Exact duplicates: ties are settled by row.
    let (head, tail) = vectors.split_at_mut(dim * 100);
    tail[..dim].copy_from_slice(&head[dim * 7..dim * 8]);
    for mode in [BuildMode::FixedOrder, BuildMode::Parallel] {
        let index = built(&vectors, dim, mode);
        for (every, k) in [(3usize, 10usize), (10, 1), (10, 100), (50, 200), (997, 10)] {
            let allow: Vec<bool> = (0..n)
                .map(|i| i % every == 0 || i == 7 || i == 100)
                .collect();
            let allowed = allow.iter().filter(|&&on| on).count();
            for query in queries.chunks_exact(dim) {
                let options = VectorSearchOptions::new()
                    .with_mask(&allow)
                    .with_filter_regime(FilterRegime::Enumerate);
                let (hits, answer) = index.search_with_stats(query, k, 16, options).unwrap();
                let truth = exact_top_masked(&vectors, dim, query, &allow, k);
                assert_eq!(hits.len(), k.min(allowed));
                assert_eq!(answer.stats.scored as usize, allowed);
                for (got, want) in hits.iter().zip(&truth) {
                    assert_eq!(got.0, want.0, "{mode:?} every {every} k {k}");
                    assert_eq!(got.1.to_bits(), want.1.to_bits());
                }
                // A floor keeps exactly the rows at or above it.
                if let Some(&(_, mid)) = truth.get(truth.len() / 2) {
                    let (floored, _) = index
                        .search_with_stats(query, k, 16, options.with_minimum_score(mid))
                        .unwrap();
                    let want: Vec<u32> = truth.iter().filter(|t| t.1 >= mid).map(|t| t.0).collect();
                    let got: Vec<u32> = floored.iter().map(|h| h.0).collect();
                    assert_eq!(got, want);
                }
            }
        }
    }
}

#[test]
fn streaming_follows_the_regimes() {
    let dim = 16;
    let n = 8000;
    let (vectors, queries) = corpus_with_queries(n, 1, dim, 0x57AE);
    let index = built(&vectors, dim, BuildMode::Parallel);
    let query = &queries[..dim];
    let allow: Vec<bool> = (0..n).map(|i| i % 5 == 0).collect();
    let stream = |regime: FilterRegime, stop_after: Option<usize>| {
        let mut rows: Vec<(i64, f32)> = Vec::new();
        let mut batches = 0usize;
        let options = VectorSearchOptions::new()
            .with_mask(&allow)
            .with_candidate_depth(32)
            .with_filter_regime(regime);
        let summary = index
            .search_streaming_controlled(
                query,
                options,
                &mut |batch| {
                    batches += 1;
                    rows.extend(
                        batch
                            .slots
                            .iter()
                            .copied()
                            .zip(batch.scores.iter().copied()),
                    );
                    match stop_after {
                        Some(limit) if batches >= limit => VectorStreamControl::Stop,
                        _ => VectorStreamControl::Continue,
                    }
                },
                &mut || VectorStreamControl::Continue,
            )
            .unwrap();
        (rows, summary)
    };
    // Enumerate streams every allowed row, each exactly scored, once.
    let (rows, summary) = stream(FilterRegime::Enumerate, None);
    assert!(summary.completed);
    assert_eq!(rows.len(), n / 5);
    assert_eq!(summary.emitted, n / 5);
    let mut seen: Vec<i64> = rows.iter().map(|r| r.0).collect();
    seen.sort_unstable();
    seen.dedup();
    assert_eq!(seen.len(), n / 5);
    for (slot, score) in &rows {
        assert!(allow[*slot as usize]);
        let exact = crate::exact_vectors::dot(&vectors[*slot as usize * dim..][..dim], query);
        assert_eq!(score.to_bits(), exact.to_bits());
    }
    // And stops when told to.
    let (rows, summary) = stream(FilterRegime::Enumerate, Some(2));
    assert!(!summary.completed);
    assert!(rows.len() < n / 5);
    // The walks emit allowed rows only, and the filter-aware one covers the
    // exact top of the mask.
    for regime in [FilterRegime::FilterAware, FilterRegime::Plain] {
        let (rows, summary) = stream(regime, None);
        assert!(summary.completed);
        assert!(
            rows.iter().all(|(slot, _)| allow[*slot as usize]),
            "{regime:?}"
        );
        let truth = exact_top_masked(&vectors, dim, query, &allow, 5);
        let hit = truth
            .iter()
            .filter(|t| rows.iter().any(|r| r.0 == i64::from(t.0)))
            .count();
        assert!(hit >= 4, "{regime:?}: stream covers {hit} of the top 5");
    }
}

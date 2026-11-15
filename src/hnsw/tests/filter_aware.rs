//! The filter-aware walk: recall under correlated and uncorrelated masks, and the island handoff.

use super::super::Answer;
use super::*;

/// One query under one mask: the mask, its exact answer, and the
/// provider's enumerate threshold at this beam.
struct MaskCase<'a> {
    name: &'a str,
    qi: usize,
    query: &'a [f32],
    allow: &'a [bool],
    truth: &'a [(u32, f32)],
    k: usize,
    enumerated: usize,
}

/// Totals per regime slot: the provider's own choice, then the two walks
/// pinned.
#[derive(Default)]
struct WalkTally {
    found: [usize; 3],
    scored: [u64; 3],
    bridges: u64,
    total: usize,
}

/// "near": the region around the query. "far": the region around the row
/// least like the query, a desert away from where the unfiltered descent
/// lands.
fn mask_for(
    fx: &Fixture,
    name: &str,
    percent: usize,
    query: &[f32],
    uncorrelated: &[bool],
) -> Vec<bool> {
    let (dim, vectors) = (fx.dim, &fx.vectors[..]);
    match name {
        "uncorrelated" => uncorrelated.to_vec(),
        "near" => region_mask(vectors, dim, query, percent),
        _ => {
            let opposite: Vec<f32> = query.iter().map(|x| -x).collect();
            let far = exact_top(vectors, dim, &opposite, 1)[0] as usize;
            region_mask(vectors, dim, &vectors[far * dim..][..dim], percent)
        }
    }
}

fn check_the_regime_that_answered(
    case: &MaskCase<'_>,
    regime: Option<FilterRegime>,
    hits: &[(u32, f32)],
    answer: &Answer,
    tally: &mut WalkTally,
) {
    let (name, qi, allow, truth, enumerated) =
        (case.name, case.qi, case.allow, case.truth, case.enumerated);
    match regime {
        // A mask away from the query is handed to enumerate
        // before the walk starts: the probe reads links only.
        None if name == "far" => {
            assert!(allow.iter().filter(|&&on| on).count() > enumerated);
            assert_eq!(answer.regime, Some(FilterRegime::Enumerate), "query {qi}");
            assert!(answer.stats.probed > 0);
            assert_eq!(answer.stats.bridges, 0);
            assert_eq!(hits, truth, "query {qi}");
        }
        None => {
            let allowed = allow.iter().filter(|&&on| on).count();
            assert!(allowed > enumerated);
            if name == "near" {
                // The rows around the query are all allowed.
                assert_eq!(answer.regime, Some(FilterRegime::Plain), "query {qi}");
            } else {
                // A 30% share sampled around the query lands
                // either side of the plain share.
                assert!(
                    matches!(
                        answer.regime,
                        Some(FilterRegime::Plain | FilterRegime::FilterAware)
                    ),
                    "query {qi}: {answer:?}"
                );
            }
        }
        Some(FilterRegime::FilterAware) => {
            assert_eq!(answer.regime, regime);
            assert_eq!(answer.stats.probed, 0);
            tally.bridges += answer.stats.bridges;
        }
        Some(_) => assert_eq!(answer.stats.bridges, 0),
    }
}

fn walk_under_a_regime(
    fx: &Fixture,
    case: &MaskCase<'_>,
    slot: usize,
    regime: Option<FilterRegime>,
    tally: &mut WalkTally,
) {
    let (dim, vectors, index) = (fx.dim, &fx.vectors[..], &fx.index);
    let (qi, query, allow, truth, k) = (case.qi, case.query, case.allow, case.truth, case.k);
    let mut options = VectorSearchOptions::new().with_mask(allow);
    options.filter_regime = regime;
    let (hits, answer) = index.search_with_stats(query, k, 64, options).unwrap();
    assert!(
        hits.iter().all(|(row, _)| allow[*row as usize]),
        "query {qi}"
    );
    for (row, score) in &hits {
        let exact = crate::exact_vectors::dot(&vectors[*row as usize * dim..][..dim], query);
        assert_eq!(score.to_bits(), exact.to_bits());
    }
    tally.found[slot] += hits
        .iter()
        .filter(|h| truth.iter().any(|t| t.0 == h.0))
        .count();
    tally.scored[slot] += answer.stats.scored;
    check_the_regime_that_answered(case, regime, &hits, &answer, tally);
}

#[test]
fn the_filter_aware_walk_keeps_recall_and_scores_only_allowed_rows() {
    let n = 30_000;
    let fx = Fixture::new(n, 24, 24, 0xAC02, BuildMode::Parallel);
    let (dim, vectors, queries) = (fx.dim, &fx.vectors[..], &fx.queries[..]);
    let k = 10;
    let mut rng = Rng(0x5EED);
    let uncorrelated: Vec<bool> = (0..n).map(|_| rng.next_u64() % 10 < 3).collect();
    // Beam 64 at M 16: the provider enumerates a mask of up to this many
    // rows and walks a larger one.
    let enumerated = super::super::ENUMERATE_ROWS_PER_WALK_ROW * 64 * 32;
    for (name, percent) in [("uncorrelated", 30), ("near", 30), ("far", 30), ("far", 60)] {
        // The provider's own choice, then the two walks pinned.
        let regimes = [
            None,
            Some(FilterRegime::FilterAware),
            Some(FilterRegime::Plain),
        ];
        let mut tally = WalkTally::default();
        for (qi, query) in queries.chunks_exact(dim).enumerate() {
            let allow = mask_for(&fx, name, percent, query, &uncorrelated);
            let truth = exact_top_masked(vectors, dim, query, &allow, k);
            tally.total += truth.len();
            let case = MaskCase {
                name,
                qi,
                query,
                allow: &allow,
                truth: &truth,
                k,
                enumerated,
            };
            for (slot, regime) in regimes.into_iter().enumerate() {
                walk_under_a_regime(&fx, &case, slot, regime, &mut tally);
            }
        }
        let WalkTally {
            found,
            scored,
            bridges,
            total,
        } = tally;
        let recall = found[0] as f64 / total as f64;
        assert!(recall >= 0.95, "{name} {percent}%: recall {recall}");
        if name == "uncorrelated" {
            // The plain walk pays for every disallowed row it routes
            // through; the filter-aware walk looks through them.
            assert!(bridges > 0, "no bridge was looked through");
            assert!(
                scored[1] * 2 < scored[2],
                "filter-aware scored {} rows, plain {}",
                scored[1],
                scored[2]
            );
        }
    }
}

#[test]
fn a_walk_stranded_on_an_island_is_finished_by_enumerate() {
    // A narrow graph and a mask of two kinds of rows: the three nearest the
    // query, and a region away from it too small a share of the rows for
    // the probe to judge. The walk reaches the three and runs dry.
    let dim = 16;
    let n = 8000;
    let (vectors, queries) = corpus_with_queries(n, 4, dim, 0x15A0);
    let mut config = config(BuildMode::FixedOrder);
    config.m = 4;
    let mut index = HnswIndex::new(dim, config).unwrap();
    index.add(&vectors, dim).unwrap();
    index.prepare().unwrap();
    let k = 10;
    for query in queries.chunks_exact(dim) {
        let opposite: Vec<f32> = query.iter().map(|x| -x).collect();
        let far = exact_top(&vectors, dim, &opposite, 1)[0] as usize;
        let mut allow = region_mask(&vectors, dim, &vectors[far * dim..][..dim], 10);
        for row in exact_top(&vectors, dim, query, 3) {
            allow[row as usize] = true;
        }
        let allowed = allow.iter().filter(|&&on| on).count();
        let truth = exact_top_masked(&vectors, dim, query, &allow, k);
        let options = VectorSearchOptions::new().with_mask(&allow);
        let (hits, answer) = index.search_with_stats(query, k, 16, options).unwrap();
        assert_eq!(answer.regime, Some(FilterRegime::Enumerate));
        // The walk ran (the probe let it) before enumerate scored the mask.
        assert!(answer.stats.bridges > 0);
        assert!(answer.stats.scored as usize > allowed);
        assert_eq!(hits, truth);
        // Pinned, the walk keeps what it reached.
        let pinned = options.with_filter_regime(FilterRegime::FilterAware);
        let (short, answer) = index.search_with_stats(query, k, 16, pinned).unwrap();
        assert_eq!(answer.regime, Some(FilterRegime::FilterAware));
        assert!(short.len() < k);

        // A stream emits what the walk reached, then every other allowed
        // row: each once, each exactly scored.
        let mut rows: Vec<(i64, f32)> = Vec::new();
        let summary = index
            .search_streaming_controlled(
                query,
                options.with_candidate_depth(16),
                &mut |batch| {
                    rows.extend(
                        batch
                            .slots
                            .iter()
                            .copied()
                            .zip(batch.scores.iter().copied()),
                    );
                    VectorStreamControl::Continue
                },
                &mut || VectorStreamControl::Continue,
            )
            .unwrap();
        assert!(summary.completed);
        assert_eq!(summary.emitted, allowed);
        let mut seen: Vec<i64> = rows.iter().map(|r| r.0).collect();
        seen.sort_unstable();
        seen.dedup();
        assert_eq!(seen.len(), allowed);
        for (slot, score) in &rows {
            assert!(allow[*slot as usize]);
            let exact = crate::exact_vectors::dot(&vectors[*slot as usize * dim..][..dim], query);
            assert_eq!(score.to_bits(), exact.to_bits());
        }
    }
}

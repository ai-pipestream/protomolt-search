//! Fusion of scored legs: reciprocal rank fusion (RRF) and score
//! blending.
//!
//! RRF needs no score normalization — vector and BM25 scores have
//! unrelated scales, and fusing RANKS sidesteps that entirely:
//!
//! ```text
//! fused_score(doc) = Σ_legs weight_leg / (rrf_k + rank_leg(doc))
//! ```
//!
//! with 1-based ranks. The same function fuses the two legs on a shard
//! (level one) and the per-shard fused lists on the coordinator (level
//! two); see the crate README for why two-level fusion is an
//! approximation of single-level global fusion, not an identity.
//!
//! [`blend_fuse`] is the score-based alternative (normalize each leg,
//! weighted-combine per doc). Where RRF compresses every score gap to
//! the fixed distance between adjacent ranks, blending preserves the
//! gaps — at the price of needing globally comparable scores per leg.

/// The default RRF constant (the value used in the RRF literature).
pub const DEFAULT_RRF_K: f64 = 60.0;

/// One leg's contribution to a fused list: a best-first ranked list of
/// `(doc_id, raw_score)` plus the leg's RRF weight.
#[derive(Debug, Clone)]
pub struct Leg {
    /// Ranked hits, best first. Ids are global.
    pub hits: Vec<(u64, f64)>,
    /// RRF weight for this leg.
    pub weight: f64,
}

/// One fused hit with per-leg provenance (1-based ranks and raw scores;
/// `None` for legs the doc is absent from).
#[derive(Debug, Clone, PartialEq)]
pub struct FusedHit {
    /// Global doc id.
    pub doc_id: u64,
    /// Fused RRF score.
    pub fused_score: f64,
    /// Per-leg 1-based rank (`None` when the doc is not in the leg).
    pub leg_ranks: Vec<Option<u32>>,
    /// Per-leg raw score (`None` when the doc is not in the leg).
    pub leg_scores: Vec<Option<f64>>,
}

/// Fuse `legs` with RRF and return the top-`depth`, fused score
/// descending. Ties break by leg presence (more legs first), then by
/// ascending doc id — both deterministic.
///
/// Leg ranks use COMPETITION ranking: docs with exactly equal raw scores
/// in a leg share one rank (rank = 1 + number of strictly-better docs,
/// so ranks skip after a tie). This is what makes the GLOBAL_RANK
/// coordinator mode layout-invariant: tied scores are common with
/// quantized vectors, and a positional rank would depend on the
/// (shard, doc id) tie-break, which differs between a sharded and a
/// monolithic layout. Shared ranks make fused scores identical across
/// layouts; only docs that are indistinguishable in EVERY leg could
/// still order differently, and only by their (layout-dependent) ids.
pub fn rrf_fuse(legs: &[Leg], rrf_k: f64, depth: usize) -> Vec<FusedHit> {
    let mut fused: std::collections::HashMap<u64, FusedHit> = std::collections::HashMap::new();
    for (li, leg) in legs.iter().enumerate() {
        if leg.weight == 0.0 {
            continue;
        }
        let mut last_score = f64::NAN;
        let mut current_rank = 0u32;
        for (position, &(doc_id, score)) in leg.hits.iter().enumerate() {
            // Legs are score-descending, so equal scores are contiguous.
            if score != last_score {
                current_rank = position as u32 + 1;
                last_score = score;
            }
            let rank = current_rank;
            let contribution = leg.weight / (rrf_k + f64::from(rank));
            let entry = fused.entry(doc_id).or_insert_with(|| FusedHit {
                doc_id,
                fused_score: 0.0,
                leg_ranks: vec![None; legs.len()],
                leg_scores: vec![None; legs.len()],
            });
            entry.fused_score += contribution;
            entry.leg_ranks[li] = Some(rank);
            entry.leg_scores[li] = Some(score);
        }
    }
    let mut hits: Vec<FusedHit> = fused.into_values().collect();
    hits.sort_by(|a, b| {
        b.fused_score
            .total_cmp(&a.fused_score)
            .then_with(|| {
                let legs_of = |h: &FusedHit| h.leg_ranks.iter().filter(|r| r.is_some()).count();
                legs_of(b).cmp(&legs_of(a))
            })
            .then_with(|| a.doc_id.cmp(&b.doc_id))
    });
    hits.truncate(depth);
    hits
}

/// How score-blend fusion rescales each leg's retained scores before
/// combining (see the proto's `ScoreNormalization` for full semantics).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Normalization {
    /// `(s - min) / (max - min)` onto [0, 1]; a degenerate leg (every
    /// retained score equal) maps to 1.0.
    #[default]
    MinMax,
    /// `(s - mean) / stddev` (population); a degenerate leg maps to 0.0.
    ZScore,
    /// Raw scores pass through unchanged.
    None,
}

/// How score-blend fusion combines one doc's normalized per-leg scores
/// (see the proto's `ScoreCombination` for full semantics).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Combination {
    /// Weighted arithmetic mean over ALL weighted legs; absent legs
    /// contribute 0 with their weight still in the denominator.
    #[default]
    Arithmetic,
    /// Weighted geometric mean over the doc's positive legs, weights
    /// renormalized over those legs; no positive leg scores 0.
    Geometric,
    /// Weighted harmonic mean over the positive legs (same skip rule).
    Harmonic,
}

/// Score-blend fusion (FUSION_MODE_SCORE_BLEND): truncate each leg
/// tie-complete to `leg_k`, normalize the retained scores per leg, and
/// combine each doc's normalized leg scores into its fused score.
///
/// Legs must be score-descending (as [`merge_legs_by_score`] produces).
/// The retained set per leg is `{score >= s_k}` where `s_k` is the leg's
/// `leg_k`-th best score — score-defined, hence layout-invariant wherever
/// the merged leg contents agree (the same argument as GLOBAL_RANK's
/// exactness). Normalization statistics are computed over the retained
/// set only: computing them over the raw shard union would let a deep
/// straggler present in one layout but not another shift every
/// normalized score.
///
/// Provenance mirrors [`rrf_fuse`]: competition leg ranks and RAW leg
/// scores. The fused order breaks ties by leg presence (more legs
/// first), then ascending doc id.
pub fn blend_fuse(
    legs: &[Leg],
    leg_k: usize,
    normalization: Normalization,
    combination: Combination,
    depth: usize,
) -> Vec<FusedHit> {
    let mut acc: std::collections::HashMap<u64, (FusedHit, Vec<Option<f64>>)> =
        std::collections::HashMap::new();
    for (li, leg) in legs.iter().enumerate() {
        if leg.weight == 0.0 || leg.hits.is_empty() || leg_k == 0 {
            continue;
        }
        // Tie-complete truncation: keep the whole boundary tie group.
        let retained: &[(u64, f64)] = if leg.hits.len() > leg_k {
            let boundary = leg.hits[leg_k - 1].1;
            let end = leg.hits[leg_k..]
                .iter()
                .position(|&(_, s)| s < boundary)
                .map_or(leg.hits.len(), |p| leg_k + p);
            &leg.hits[..end]
        } else {
            &leg.hits
        };
        let normalize: Box<dyn Fn(f64) -> f64> = match normalization {
            Normalization::MinMax => {
                let min = retained.last().expect("retained is non-empty").1;
                let max = retained[0].1;
                if max > min {
                    Box::new(move |s| (s - min) / (max - min))
                } else {
                    Box::new(|_| 1.0)
                }
            }
            Normalization::ZScore => {
                let n = retained.len() as f64;
                let mean = retained.iter().map(|&(_, s)| s).sum::<f64>() / n;
                let variance =
                    retained.iter().map(|&(_, s)| (s - mean).powi(2)).sum::<f64>() / n;
                let sigma = variance.sqrt();
                if sigma > 0.0 {
                    Box::new(move |s| (s - mean) / sigma)
                } else {
                    Box::new(|_| 0.0)
                }
            }
            Normalization::None => Box::new(|s| s),
        };
        let mut last_score = f64::NAN;
        let mut current_rank = 0u32;
        for (position, &(doc_id, score)) in retained.iter().enumerate() {
            // Competition ranking, exactly as in rrf_fuse.
            if score != last_score {
                current_rank = position as u32 + 1;
                last_score = score;
            }
            let (hit, norms) = acc.entry(doc_id).or_insert_with(|| {
                (
                    FusedHit {
                        doc_id,
                        fused_score: 0.0,
                        leg_ranks: vec![None; legs.len()],
                        leg_scores: vec![None; legs.len()],
                    },
                    vec![None; legs.len()],
                )
            });
            hit.leg_ranks[li] = Some(current_rank);
            hit.leg_scores[li] = Some(score);
            norms[li] = Some(normalize(score));
        }
    }

    let total_weight: f64 = legs
        .iter()
        .map(|l| l.weight)
        .filter(|&w| w != 0.0)
        .sum();
    let mut hits: Vec<FusedHit> = acc
        .into_values()
        .map(|(mut hit, norms)| {
            hit.fused_score = combine(&norms, legs, combination, total_weight);
            hit
        })
        .collect();
    hits.sort_by(|a, b| {
        b.fused_score
            .total_cmp(&a.fused_score)
            .then_with(|| {
                let legs_of = |h: &FusedHit| h.leg_ranks.iter().filter(|r| r.is_some()).count();
                legs_of(b).cmp(&legs_of(a))
            })
            .then_with(|| a.doc_id.cmp(&b.doc_id))
    });
    hits.truncate(depth);
    hits
}

/// One doc's fused score from its normalized per-leg scores (`None` for
/// legs the doc is absent from).
fn combine(
    norms: &[Option<f64>],
    legs: &[Leg],
    combination: Combination,
    total_weight: f64,
) -> f64 {
    match combination {
        Combination::Arithmetic => {
            let sum: f64 = norms
                .iter()
                .zip(legs)
                .filter(|(_, leg)| leg.weight != 0.0)
                .filter_map(|(n, leg)| n.map(|n| n * leg.weight))
                .sum();
            if total_weight > 0.0 {
                sum / total_weight
            } else {
                0.0
            }
        }
        Combination::Geometric => {
            let mut weighted_ln = 0.0;
            let mut used = 0.0;
            for (n, leg) in norms.iter().zip(legs) {
                if let Some(n) = *n {
                    if n > 0.0 && leg.weight != 0.0 {
                        weighted_ln += leg.weight * n.ln();
                        used += leg.weight;
                    }
                }
            }
            if used > 0.0 {
                (weighted_ln / used).exp()
            } else {
                0.0
            }
        }
        Combination::Harmonic => {
            let mut weighted_inv = 0.0;
            let mut used = 0.0;
            for (n, leg) in norms.iter().zip(legs) {
                if let Some(n) = *n {
                    if n > 0.0 && leg.weight != 0.0 {
                        weighted_inv += leg.weight / n;
                        used += leg.weight;
                    }
                }
            }
            if used > 0.0 {
                used / weighted_inv
            } else {
                0.0
            }
        }
    }
}

/// Merge per-shard leg lists (each already score-descending) into one
/// GLOBAL ranked list by raw score, returning `(doc_id, score,
/// global_rank)` with 1-based ranks.
///
/// Tie-break: score descending, then shard index ascending, then doc id
/// ascending — a total order, so the merge is deterministic and
/// independent of shard arrival order. This is what makes the
/// GLOBAL_RANK fusion mode exactly reproducible: every coordinator (and
/// the monolithic single-shard path) derives identical global ranks from
/// identical leg contents.
pub fn merge_legs_by_score(shard_legs: Vec<(u32, Vec<(u64, f64)>)>) -> Vec<(u64, f64, u32)> {
    let mut all: Vec<(u64, u32, f64)> = shard_legs
        .into_iter()
        .flat_map(|(shard, hits)| {
            hits.into_iter()
                .map(move |(doc_id, score)| (doc_id, shard, score))
        })
        .collect();
    all.sort_by(|a, b| {
        b.2.total_cmp(&a.2)
            .then_with(|| a.1.cmp(&b.1))
            .then_with(|| a.0.cmp(&b.0))
    });
    // Competition ranking (tied scores share a rank; ranks skip after a
    // tie) — layout-invariant, see rrf_fuse's doc comment.
    let mut last_score = f64::NAN;
    let mut current_rank = 0u32;
    all.into_iter()
        .enumerate()
        .map(|(i, (doc_id, _, score))| {
            if score != last_score {
                current_rank = i as u32 + 1;
                last_score = score;
            }
            (doc_id, score, current_rank)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_legs_reconstructs_global_ranking() {
        // Two interleaved shard lists (already score-descending locally).
        let merged = merge_legs_by_score(vec![
            (0, vec![(10, 0.9), (11, 0.5), (12, 0.1)]),
            (1, vec![(20, 0.8), (21, 0.6)]),
        ]);
        let ids: Vec<u64> = merged.iter().map(|h| h.0).collect();
        assert_eq!(ids, vec![10, 20, 21, 11, 12]);
        // Global ranks are 1-based positions in the merged list.
        assert_eq!(merged[0].2, 1);
        assert_eq!(merged[4].2, 5);
    }

    #[test]
    fn merge_legs_ties_break_by_shard_then_doc_id() {
        let merged = merge_legs_by_score(vec![
            (2, vec![(7, 1.0)]),
            (0, vec![(9, 1.0)]),
            (1, vec![(3, 1.0), (1, 1.0)]),
        ]);
        // All scores tie at 1.0: shard 0's doc 9 first, then shard 1's
        // docs 1 and 3 (doc id ascending), then shard 2's doc 7.
        let order: Vec<u64> = merged.iter().map(|h| h.0).collect();
        assert_eq!(order, vec![9, 1, 3, 7]);
        // Same total order regardless of input shard order.
        let again = merge_legs_by_score(vec![
            (1, vec![(3, 1.0), (1, 1.0)]),
            (2, vec![(7, 1.0)]),
            (0, vec![(9, 1.0)]),
        ]);
        assert_eq!(merged, again);
    }

    #[test]
    fn merge_legs_empty_inputs() {
        assert!(merge_legs_by_score(Vec::new()).is_empty());
        assert!(merge_legs_by_score(vec![(0, vec![]), (1, vec![])]).is_empty());
    }

    fn leg(hits: &[(u64, f64)], weight: f64) -> Leg {
        Leg {
            hits: hits.to_vec(),
            weight,
        }
    }

    #[test]
    fn known_rankings_produce_expected_order_and_scores() {
        // Leg A: [1, 2, 3]; leg B: [3, 1, 9]; both weight 1, rrf_k=60.
        let fused = rrf_fuse(
            &[
                leg(&[(1, 0.9), (2, 0.8), (3, 0.7)], 1.0),
                leg(&[(3, 5.0), (1, 4.0), (9, 3.0)], 1.0),
            ],
            60.0,
            10,
        );
        // Doc 1: 1/61 + 1/62; doc 3: 1/63 + 1/61; doc 2: 1/62; doc 9: 1/63.
        let s1 = 1.0 / 61.0 + 1.0 / 62.0;
        let s3 = 1.0 / 63.0 + 1.0 / 61.0;
        assert_eq!(fused.len(), 4);
        assert_eq!(fused[0].doc_id, 1);
        assert!((fused[0].fused_score - s1).abs() < 1e-15);
        assert_eq!(fused[1].doc_id, 3);
        assert!((fused[1].fused_score - s3).abs() < 1e-15);
        assert_eq!(fused[2].doc_id, 2);
        assert_eq!(fused[3].doc_id, 9);
        // Provenance: doc 1 is rank 1 in leg A, rank 2 in leg B.
        assert_eq!(fused[0].leg_ranks, vec![Some(1), Some(2)]);
        assert_eq!(fused[0].leg_scores, vec![Some(0.9), Some(4.0)]);
    }

    #[test]
    fn weights_shift_order() {
        // Doc 1 tops leg A, doc 2 tops leg B. Unweighted they tie (id
        // tie-break); upweighting leg B must put doc 2 first.
        let legs = || [leg(&[(1, 0.9)], 1.0), leg(&[(2, 5.0)], 1.0)];
        let fused = rrf_fuse(
            &[
                Leg {
                    weight: 1.0,
                    ..leg(&[(1, 0.9)], 1.0)
                },
                Leg {
                    weight: 3.0,
                    ..leg(&[(2, 5.0)], 1.0)
                },
            ],
            60.0,
            10,
        );
        assert_eq!(fused[0].doc_id, 2);
        assert!((fused[0].fused_score - 3.0 / 61.0).abs() < 1e-15);
        let _ = legs;
    }

    #[test]
    fn rrf_k_constant_is_respected() {
        let fused60 = rrf_fuse(&[leg(&[(1, 1.0), (2, 0.5)], 1.0)], 60.0, 10);
        let fused10 = rrf_fuse(&[leg(&[(1, 1.0), (2, 0.5)], 1.0)], 10.0, 10);
        assert!((fused60[0].fused_score - 1.0 / 61.0).abs() < 1e-15);
        assert!((fused10[0].fused_score - 1.0 / 11.0).abs() < 1e-15);
        // Smaller constant: steeper rank discount, larger spread.
        let spread60 = fused60[0].fused_score - fused60[1].fused_score;
        let spread10 = fused10[0].fused_score - fused10[1].fused_score;
        assert!(spread10 > spread60);
    }

    #[test]
    fn doc_in_one_gets_no_rank_for_the_other() {
        let fused = rrf_fuse(
            &[leg(&[(1, 0.9), (2, 0.8)], 1.0), leg(&[(2, 5.0)], 1.0)],
            60.0,
            10,
        );
        assert_eq!(fused[0].doc_id, 2, "two legs beat one");
        assert_eq!(fused[1].doc_id, 1);
        assert_eq!(fused[1].leg_ranks, vec![Some(1), None]);
        assert_eq!(fused[1].leg_scores, vec![Some(0.9), None]);
    }

    #[test]
    fn empty_leg_preserves_the_other_legs_order() {
        let fused = rrf_fuse(&[leg(&[(5, 0.1), (3, 0.05)], 1.0), leg(&[], 1.0)], 60.0, 10);
        let ids: Vec<u64> = fused.iter().map(|h| h.doc_id).collect();
        assert_eq!(ids, vec![5, 3]);
        assert!(fused.iter().all(|h| h.leg_ranks[1].is_none()));
    }

    #[test]
    fn empty_legs_and_zero_weights() {
        assert!(rrf_fuse(&[leg(&[], 1.0), leg(&[], 1.0)], 60.0, 10).is_empty());
        assert!(rrf_fuse(&[leg(&[(1, 0.5)], 0.0)], 60.0, 10).is_empty());
    }

    #[test]
    fn tied_scores_share_a_competition_rank() {
        // Two docs tie at 0.5 in the leg: both get rank 2, the next doc
        // gets rank 4 (ranks skip). Fused scores of the tied docs are
        // identical — the layout-invariance property GLOBAL_RANK relies on.
        let fused = rrf_fuse(
            &[leg(&[(1, 0.9), (2, 0.5), (3, 0.5), (4, 0.1)], 1.0)],
            60.0,
            10,
        );
        let by_id = |id: u64| fused.iter().find(|h| h.doc_id == id).unwrap();
        assert_eq!(by_id(2).leg_ranks[0], Some(2));
        assert_eq!(by_id(3).leg_ranks[0], Some(2));
        assert_eq!(by_id(4).leg_ranks[0], Some(4));
        assert_eq!(by_id(2).fused_score, by_id(3).fused_score);
        assert_eq!(by_id(2).fused_score, 1.0 / 62.0);
        assert_eq!(by_id(4).fused_score, 1.0 / 64.0);
    }

    #[test]
    fn merge_legs_assigns_competition_ranks() {
        let merged = merge_legs_by_score(vec![
            (0, vec![(10, 1.0), (11, 1.0)]),
            (1, vec![(20, 1.0), (21, 0.5)]),
        ]);
        let rank_of = |id: u64| merged.iter().find(|h| h.0 == id).unwrap().2;
        assert_eq!(rank_of(10), 1);
        assert_eq!(rank_of(11), 1);
        assert_eq!(rank_of(20), 1);
        assert_eq!(rank_of(21), 4);
    }

    #[test]
    fn depth_truncates() {
        let fused = rrf_fuse(&[leg(&[(1, 0.9), (2, 0.8), (3, 0.7)], 1.0)], 60.0, 2);
        assert_eq!(fused.len(), 2);
        assert_eq!(fused[1].doc_id, 2);
    }

    #[test]
    fn blend_min_max_arithmetic_known_values() {
        // Leg A spans [0, 10] -> normalized [1.0, 0.5, 0.0]; leg B spans
        // [2, 4] -> doc 2: 1.0, doc 1: 0.0. Equal weights:
        //   doc 2 = (0.5 + 1.0)/2 = 0.75
        //   doc 1 = (1.0 + 0.0)/2 = 0.50
        //   doc 3 = (0.0 + absent)/2 = 0.0
        let fused = blend_fuse(
            &[
                leg(&[(1, 10.0), (2, 5.0), (3, 0.0)], 1.0),
                leg(&[(2, 4.0), (1, 2.0)], 1.0),
            ],
            10,
            Normalization::MinMax,
            Combination::Arithmetic,
            10,
        );
        assert_eq!(fused.len(), 3);
        assert_eq!(fused[0].doc_id, 2);
        assert!((fused[0].fused_score - 0.75).abs() < 1e-12);
        assert_eq!(fused[1].doc_id, 1);
        assert!((fused[1].fused_score - 0.50).abs() < 1e-12);
        assert_eq!(fused[2].doc_id, 3);
        assert_eq!(fused[2].fused_score, 0.0);
        // Provenance carries competition ranks and RAW scores.
        assert_eq!(fused[0].leg_ranks, vec![Some(2), Some(1)]);
        assert_eq!(fused[0].leg_scores, vec![Some(5.0), Some(4.0)]);
        assert_eq!(fused[2].leg_ranks, vec![Some(3), None]);
    }

    #[test]
    fn blend_weights_scale_arithmetic() {
        // Upweighting leg B (weight 3): doc 2 = (0.5 + 3*1.0)/4 = 0.875,
        // doc 1 = (1.0 + 3*0.0)/4 = 0.25.
        let fused = blend_fuse(
            &[
                leg(&[(1, 10.0), (2, 5.0), (3, 0.0)], 1.0),
                leg(&[(2, 4.0), (1, 2.0)], 3.0),
            ],
            10,
            Normalization::MinMax,
            Combination::Arithmetic,
            10,
        );
        assert_eq!(fused[0].doc_id, 2);
        assert!((fused[0].fused_score - 0.875).abs() < 1e-12);
        let doc1 = fused.iter().find(|h| h.doc_id == 1).unwrap();
        assert!((doc1.fused_score - 0.25).abs() < 1e-12);
    }

    #[test]
    fn blend_degenerate_leg_normalizes_to_one() {
        // Every retained score equal (min == max): min-max maps all to
        // 1.0 rather than dividing by zero.
        let fused = blend_fuse(
            &[leg(&[(1, 7.0), (2, 7.0)], 1.0)],
            10,
            Normalization::MinMax,
            Combination::Arithmetic,
            10,
        );
        assert!(fused.iter().all(|h| (h.fused_score - 1.0).abs() < 1e-12));
        // Both docs tie: shared competition rank 1.
        assert!(fused.iter().all(|h| h.leg_ranks[0] == Some(1)));
    }

    #[test]
    fn blend_z_score_centers_and_preserves_order() {
        // Scores [2, 4, 6]: mean 4, population sigma sqrt(8/3). One leg,
        // weight 1 -> fused = z directly (negative allowed).
        let fused = blend_fuse(
            &[leg(&[(3, 6.0), (2, 4.0), (1, 2.0)], 1.0)],
            10,
            Normalization::ZScore,
            Combination::Arithmetic,
            10,
        );
        let sigma = (8.0f64 / 3.0).sqrt();
        assert_eq!(fused[0].doc_id, 3);
        assert!((fused[0].fused_score - 2.0 / sigma).abs() < 1e-12);
        assert_eq!(fused[1].doc_id, 2);
        assert!(fused[1].fused_score.abs() < 1e-12);
        assert_eq!(fused[2].doc_id, 1);
        assert!((fused[2].fused_score + 2.0 / sigma).abs() < 1e-12);
    }

    #[test]
    fn blend_geometric_and_harmonic_skip_nonpositive_legs() {
        // Doc 1: normalized 1.0 in A, 0.0 in B (boundary) -> B skipped,
        // geometric = 1.0. Doc 2: 0.5 in A, 1.0 in B -> geometric
        // sqrt(0.5), harmonic 2/(1/0.5 + 1/1.0) = 2/3. Doc 3: 0.0 in A
        // only -> no positive leg, fused 0.
        let legs_ab = [
            leg(&[(1, 10.0), (2, 5.0), (3, 0.0)], 1.0),
            leg(&[(2, 4.0), (1, 2.0)], 1.0),
        ];
        let geo = blend_fuse(
            &legs_ab,
            10,
            Normalization::MinMax,
            Combination::Geometric,
            10,
        );
        let by_id = |hits: &[FusedHit], id: u64| {
            hits.iter().find(|h| h.doc_id == id).unwrap().fused_score
        };
        assert!((by_id(&geo, 1) - 1.0).abs() < 1e-12);
        assert!((by_id(&geo, 2) - 0.5f64.sqrt()).abs() < 1e-12);
        assert_eq!(by_id(&geo, 3), 0.0);
        let har = blend_fuse(
            &legs_ab,
            10,
            Normalization::MinMax,
            Combination::Harmonic,
            10,
        );
        assert!((by_id(&har, 1) - 1.0).abs() < 1e-12);
        assert!((by_id(&har, 2) - 2.0 / 3.0).abs() < 1e-12);
        assert_eq!(by_id(&har, 3), 0.0);
    }

    #[test]
    fn blend_truncates_tie_complete_and_stats_follow() {
        // leg_k = 3 with scores [9, 8, 7, 7, 1]: boundary 7 keeps the
        // whole tie group (4 docs), drops the 1. Min-max stats over the
        // RETAINED set: min 7, max 9 -> doc 1: 1.0, doc 2: 0.5, boundary
        // docs 0.0. The dropped doc appears nowhere.
        let fused = blend_fuse(
            &[leg(&[(1, 9.0), (2, 8.0), (3, 7.0), (4, 7.0), (5, 1.0)], 1.0)],
            3,
            Normalization::MinMax,
            Combination::Arithmetic,
            10,
        );
        assert_eq!(fused.len(), 4);
        assert!(fused.iter().all(|h| h.doc_id != 5));
        assert!((fused[0].fused_score - 1.0).abs() < 1e-12);
        assert!((fused[1].fused_score - 0.5).abs() < 1e-12);
        assert_eq!(fused[2].fused_score, 0.0);
        assert_eq!(fused[3].fused_score, 0.0);
        // Boundary tie group shares competition rank 3.
        assert_eq!(fused[2].leg_ranks[0], Some(3));
        assert_eq!(fused[3].leg_ranks[0], Some(3));
    }

    #[test]
    fn blend_none_passes_raw_scores_through() {
        let fused = blend_fuse(
            &[leg(&[(1, 0.8), (2, 0.2)], 1.0), leg(&[(1, 0.4)], 1.0)],
            10,
            Normalization::None,
            Combination::Arithmetic,
            10,
        );
        assert_eq!(fused[0].doc_id, 1);
        assert!((fused[0].fused_score - 0.6).abs() < 1e-12);
        assert!((fused[1].fused_score - 0.1).abs() < 1e-12);
    }

    #[test]
    fn blend_empty_legs_and_zero_weights() {
        assert!(blend_fuse(
            &[leg(&[], 1.0)],
            10,
            Normalization::MinMax,
            Combination::Arithmetic,
            10
        )
        .is_empty());
        assert!(blend_fuse(
            &[leg(&[(1, 0.5)], 0.0)],
            10,
            Normalization::MinMax,
            Combination::Arithmetic,
            10
        )
        .is_empty());
    }
}

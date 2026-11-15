//! Score-blend fusion: per-leg normalization and weighted combination.

use super::{FusedHit, Leg};

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
        let retained = retain_tie_complete(&leg.hits, leg_k);
        let normalize = normalizer(normalization, retained);
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
                        leg_norms: Vec::new(),
                    },
                    vec![None; legs.len()],
                )
            });
            hit.leg_ranks[li] = Some(current_rank);
            hit.leg_scores[li] = Some(score);
            norms[li] = Some(normalize(score));
        }
    }

    let total_weight: f64 = legs.iter().map(|l| l.weight).filter(|&w| w != 0.0).sum();
    let mut hits: Vec<FusedHit> = acc
        .into_values()
        .map(|(mut hit, norms)| {
            hit.fused_score = combine(&norms, legs, combination, total_weight);
            hit.leg_norms = norms;
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

/// Tie-complete truncation to `leg_k`: keeps the whole boundary tie
/// group. `leg_k` is positive.
fn retain_tie_complete(hits: &[(u64, f64)], leg_k: usize) -> &[(u64, f64)] {
    if hits.len() > leg_k {
        let boundary = hits[leg_k - 1].1;
        let end = hits[leg_k..]
            .iter()
            .position(|&(_, s)| s < boundary)
            .map_or(hits.len(), |p| leg_k + p);
        &hits[..end]
    } else {
        hits
    }
}

/// The rescaling of one leg's scores, with statistics taken over its
/// non-empty, score-descending `retained` set.
fn normalizer(normalization: Normalization, retained: &[(u64, f64)]) -> Box<dyn Fn(f64) -> f64> {
    match normalization {
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
            let variance = retained
                .iter()
                .map(|&(_, s)| (s - mean).powi(2))
                .sum::<f64>()
                / n;
            let sigma = variance.sqrt();
            if sigma > 0.0 {
                Box::new(move |s| (s - mean) / sigma)
            } else {
                Box::new(|_| 0.0)
            }
        }
        Normalization::None => Box::new(|s| s),
    }
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

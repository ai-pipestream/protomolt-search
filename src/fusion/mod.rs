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

mod blend;
#[cfg(test)]
mod tests;

pub use blend::{blend_fuse, Combination, Normalization};

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
    /// SCORE_BLEND only: each leg's normalized score, the value the
    /// combination consumed; empty for RRF.
    pub leg_norms: Vec<Option<f64>>,
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
                leg_norms: Vec::new(),
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

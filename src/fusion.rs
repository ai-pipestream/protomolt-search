//! Reciprocal rank fusion (RRF): rank-based fusion of scored legs.
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
}

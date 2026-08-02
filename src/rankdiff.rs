//! Comparing two rankings of the same query.
//!
//! Every A/B over this engine reduces to one question: how far apart are
//! these two result lists, and does the distance mean anything? These are
//! the measures that answer it, kept as pure functions over id lists so
//! they can be applied to any pair of rankings, whatever produced them.
//!
//! Why the engine computes them rather than the client: an exact,
//! layout-invariant engine makes a ranking diff *attributable*. An
//! approximate index cannot separate a real change from its own recall
//! variance, so comparisons there need large samples and stay arguable;
//! here two runs of the same query are bitwise identical (see
//! `tests/score_layout.rs`), so any difference between two variants IS
//! the variant. Metrics computed next to the rankings inherit that
//! guarantee, and every caller stops re-deriving them.
//!
//! What these do NOT tell you is which ranking is better. They measure
//! disagreement, not quality; a large diff with no judgment signal is a
//! reason to look, not a verdict. Pair them with labels, or with
//! [`score_regret`] against a ranking you already trust.

use std::collections::{HashMap, HashSet};

/// Fraction of `a`'s top `k` that also appears in `b`'s top `k`.
///
/// The bluntest measure and often the right one: it answers "would a user
/// looking at k results see a different set", ignoring order entirely.
/// Returns 1.0 when both lists are empty, and divides by the shorter
/// effective depth so a truncated list is not punished for being short.
pub fn overlap_at_k(a: &[u64], b: &[u64], k: usize) -> f64 {
    let depth = k.min(a.len()).min(b.len());
    if depth == 0 {
        return if a.is_empty() && b.is_empty() {
            1.0
        } else {
            0.0
        };
    }
    let top_b: HashSet<u64> = b[..depth].iter().copied().collect();
    let shared = a[..depth].iter().filter(|id| top_b.contains(id)).count();
    shared as f64 / depth as f64
}

/// Kendall tau-b over the UNION of both rankings.
///
/// +1.0 is identical order, -1.0 exactly reversed, 0.0 no association.
///
/// Ranking comparisons are usually partial: a doc in `a`'s top k may be
/// absent from `b`'s entirely. Dropping those would measure only the
/// agreement of the docs both already agree to include, which flatters
/// every comparison. Instead the union is ranked, and ids missing from a
/// list share one tied rank just past its end: "b did not return this"
/// is a real, and equal, statement about all of them. Tau-b is the
/// variant that corrects for exactly those ties.
pub fn kendall_tau(a: &[u64], b: &[u64]) -> f64 {
    let union: Vec<u64> = {
        let mut seen = HashSet::new();
        a.iter()
            .chain(b.iter())
            .copied()
            .filter(|id| seen.insert(*id))
            .collect()
    };
    if union.len() < 2 {
        return 1.0;
    }
    let rank_of = |list: &[u64]| -> HashMap<u64, usize> {
        list.iter().enumerate().map(|(i, id)| (*id, i)).collect()
    };
    let (ra, rb) = (rank_of(a), rank_of(b));
    // Absent ids all share the rank just past the list, so they tie with
    // each other and lose to everything present.
    let pos = |r: &HashMap<u64, usize>, len: usize, id: u64| *r.get(&id).unwrap_or(&len);

    let (mut concordant, mut discordant, mut ties_a, mut ties_b) = (0i64, 0i64, 0i64, 0i64);
    for i in 0..union.len() {
        for j in (i + 1)..union.len() {
            let (x, y) = (union[i], union[j]);
            let da = pos(&ra, a.len(), x) as i64 - pos(&ra, a.len(), y) as i64;
            let db = pos(&rb, b.len(), x) as i64 - pos(&rb, b.len(), y) as i64;
            match (da.signum(), db.signum()) {
                (0, 0) => {
                    ties_a += 1;
                    ties_b += 1;
                }
                (0, _) => ties_a += 1,
                (_, 0) => ties_b += 1,
                (sa, sb) if sa == sb => concordant += 1,
                _ => discordant += 1,
            }
        }
    }
    let pairs = (union.len() * (union.len() - 1) / 2) as i64;
    let denom = (((pairs - ties_a) as f64) * ((pairs - ties_b) as f64)).sqrt();
    if denom == 0.0 {
        return 1.0;
    }
    (concordant - discordant) as f64 / denom
}

/// Truncated rank-biased overlap with persistence `p`.
///
/// Top-weighted by construction: a disagreement at rank 1 costs far more
/// than one at rank 50, which is what a ranking diff should measure and
/// what plain overlap ignores. `p` is the probability a user continues to
/// the next rank, so p = 0.9 weights roughly the first 10 results, p =
/// 0.98 roughly the first 50.
///
/// This is the TRUNCATED form: it sums only to the depth supplied and
/// makes no assumption about the unseen tail, so it is a LOWER bound on
/// the full RBO. That is the honest choice when comparing top-k lists,
/// where the tail genuinely was not observed.
pub fn rbo(a: &[u64], b: &[u64], p: f64) -> f64 {
    let depth = a.len().min(b.len());
    if depth == 0 {
        return if a.is_empty() && b.is_empty() {
            1.0
        } else {
            0.0
        };
    }
    let (mut seen_a, mut seen_b) = (HashSet::new(), HashSet::new());
    let (mut shared, mut sum) = (0usize, 0.0f64);
    for d in 0..depth {
        let (x, y) = (a[d], b[d]);
        seen_a.insert(x);
        seen_b.insert(y);
        // Grow the prefix intersection by whatever this depth added to it:
        // the same id on both sides is one new member, otherwise each side
        // contributes one if the other has already shown it.
        if x == y {
            shared += 1;
        } else {
            shared += usize::from(seen_b.contains(&x)) + usize::from(seen_a.contains(&y));
        }
        sum += p.powi(d as i32) * (shared as f64 / (d + 1) as f64);
    }
    (1.0 - p) * sum
}

/// Mean score the variant gives up against a ranking you already trust.
///
/// `reference` is (id, score) in reference rank order (normally score
/// descending); `variant` is the ids the other run returned. At each rank
/// the reference's own score there is compared with the reference's score
/// OF THE DOC THE VARIANT PUT THERE, so both sides are measured by the
/// same yardstick and only the selection differs.
///
/// This measures the SET, not the order. Over a sorted reference the
/// per-rank differences of any reordering of the same documents cancel
/// exactly, so a pure permutation scores 0 no matter how violently it
/// reorders. That is the useful half of the division of labour: pair it
/// with [`rbo`] or [`kendall_tau`], which see order and not score. A
/// large tau change with zero regret means the variant shuffled
/// equivalent documents, which is the near-duplicate tie signature; a
/// large regret means it actually reached for worse ones.
///
/// Consequently the mean is >= 0 for a properly sorted reference. A
/// negative value means the reference list handed in was not in
/// descending score order, which is a caller error worth noticing rather
/// than a discovery.
///
/// A variant doc the reference never scored is skipped and counted in the
/// returned `unscored`: it may be genuinely better or genuinely worse and
/// this measure cannot tell, so it must not be quietly averaged in.
pub fn score_regret(reference: &[(u64, f32)], variant: &[u64], k: usize) -> ScoreRegret {
    let table: HashMap<u64, f32> = reference.iter().copied().collect();
    let depth = k.min(reference.len()).min(variant.len());
    let (mut total, mut counted, mut unscored) = (0.0f64, 0usize, 0usize);
    for d in 0..depth {
        match table.get(&variant[d]) {
            Some(score) => {
                total += f64::from(reference[d].1) - f64::from(*score);
                counted += 1;
            }
            None => unscored += 1,
        }
    }
    ScoreRegret {
        mean: if counted == 0 {
            0.0
        } else {
            total / counted as f64
        },
        counted,
        unscored,
    }
}

/// The result of [`score_regret`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ScoreRegret {
    /// Mean score given up per compared rank, >= 0 for a reference in
    /// descending score order. A negative value means the reference was
    /// not sorted, which is a caller error rather than a finding.
    pub mean: f64,
    /// Ranks where both sides had a reference score.
    pub counted: usize,
    /// Variant docs the reference never scored, so not comparable here.
    pub unscored: usize,
}

/// Whether the two rankings disagree about the single best result.
///
/// Tracked separately because it is the difference a user is most likely
/// to notice, and because it moves independently of the aggregate
/// measures: a pair of rankings can hold a high overlap and still flip
/// the top result.
pub fn top1_flipped(a: &[u64], b: &[u64]) -> bool {
    match (a.first(), b.first()) {
        (Some(x), Some(y)) => x != y,
        (None, None) => false,
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identical_rankings_agree_on_every_measure() {
        let a = [10u64, 20, 30, 40];
        assert_eq!(overlap_at_k(&a, &a, 4), 1.0);
        assert!((kendall_tau(&a, &a) - 1.0).abs() < 1e-12);
        assert!((rbo(&a, &a, 0.9) - (1.0 - 0.9f64.powi(4))).abs() < 1e-12);
        assert!(!top1_flipped(&a, &a));
    }

    #[test]
    fn reversed_ranking_is_tau_minus_one() {
        let a = [1u64, 2, 3, 4];
        let b = [4u64, 3, 2, 1];
        assert!((kendall_tau(&a, &b) + 1.0).abs() < 1e-12);
        // Same SET, so set overlap is total even though order inverted:
        // this is exactly why overlap alone is not enough.
        assert_eq!(overlap_at_k(&a, &b, 4), 1.0);
        assert!(top1_flipped(&a, &b));
    }

    #[test]
    fn disjoint_rankings_score_zero_overlap() {
        let a = [1u64, 2, 3];
        let b = [4u64, 5, 6];
        assert_eq!(overlap_at_k(&a, &b, 3), 0.0);
        assert_eq!(rbo(&a, &b, 0.9), 0.0);
        assert!(top1_flipped(&a, &b));
    }

    #[test]
    fn rbo_weights_the_top_more_than_the_tail() {
        let base = [1u64, 2, 3, 4, 5];
        let swap_top = [2u64, 1, 3, 4, 5];
        let swap_tail = [1u64, 2, 3, 5, 4];
        assert!(
            rbo(&base, &swap_top, 0.9) < rbo(&base, &swap_tail, 0.9),
            "a disagreement at rank 1 must cost more than one at rank 4"
        );
        // Plain overlap cannot see the difference at all.
        assert_eq!(
            overlap_at_k(&base, &swap_top, 5),
            overlap_at_k(&base, &swap_tail, 5)
        );
    }

    #[test]
    fn absent_documents_count_against_agreement() {
        // b returns only the first two of a's four: the two it dropped are
        // real disagreement, not something to average away.
        let a = [1u64, 2, 3, 4];
        let b = [1u64, 2];
        let tau = kendall_tau(&a, &b);
        assert!(
            tau < 1.0,
            "dropping half the results cannot be perfect agreement"
        );
        assert!(tau > 0.0, "the order it did return still agrees: {tau}");
    }

    #[test]
    fn score_regret_ignores_reordering_of_the_same_documents() {
        // Same set, violently reordered. The per-rank differences cancel,
        // which is the point: regret answers "did it reach for worse
        // documents", and reordering equivalents is not that.
        let reference = [(1u64, 0.9f32), (2, 0.8), (3, 0.7)];
        let r = score_regret(&reference, &[3u64, 2, 1], 3);
        assert_eq!((r.counted, r.unscored), (3, 0));
        assert!(
            r.mean.abs() < 1e-12,
            "a permutation must not register: {r:?}"
        );
        // The order-sensitive measures DO see it, which is why both exist.
        assert!(kendall_tau(&[1u64, 2, 3], &[3u64, 2, 1]) < 0.0);
    }

    #[test]
    fn score_regret_is_positive_when_the_variant_reaches_for_worse_documents() {
        // Reference top-2 is 0.9, 0.8; the variant keeps the best and
        // swaps the runner-up for a doc the reference scored 0.3.
        let reference = [(1u64, 0.9f32), (2, 0.8), (3, 0.7), (4, 0.3)];
        let r = score_regret(&reference, &[1u64, 4], 2);
        assert_eq!((r.counted, r.unscored), (2, 0));
        assert!(
            (r.mean - 0.25).abs() < 1e-6,
            "mean of (0.9-0.9) and (0.8-0.3): {r:?}"
        );
    }

    #[test]
    fn score_regret_reports_rather_than_hides_unscored_documents() {
        let reference = [(1u64, 0.7f32), (2, 0.8)];
        let variant = [99u64, 2];
        let r = score_regret(&reference, &variant, 2);
        assert_eq!(r.unscored, 1, "doc 99 has no reference score");
        assert_eq!(r.counted, 1);
    }

    #[test]
    fn empty_rankings_are_defined() {
        let empty: [u64; 0] = [];
        assert_eq!(overlap_at_k(&empty, &empty, 10), 1.0);
        assert_eq!(rbo(&empty, &empty, 0.9), 1.0);
        assert!(!top1_flipped(&empty, &empty));
        assert!(top1_flipped(&[1u64], &empty));
    }
}

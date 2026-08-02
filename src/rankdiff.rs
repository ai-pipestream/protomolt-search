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
    // Exact agreement is the only thing that earns 1.0, and it is worth
    // deciding up front: below, a zero denominator ALSO produces no
    // information, and the two must not be confused.
    if a == b {
        return 1.0;
    }
    let union: Vec<u64> = {
        let mut seen = HashSet::new();
        a.iter()
            .chain(b.iter())
            .copied()
            .filter(|id| seen.insert(*id))
            .collect()
    };
    if union.len() < 2 {
        // The lists differ but there is no pair to compare them on: one
        // is empty and the other holds a single result. No association.
        return 0.0;
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
        // One side ties every pair, so it carries no ordering
        // information: an EMPTY ranking is the common case, since every
        // union member is equally absent from it. Tau-b is undefined
        // here, and 0.0 (no association) is the honest report.
        //
        // Returning 1.0 was the original behaviour and is badly wrong:
        // it says an arm that returned nothing agrees perfectly with one
        // that returned ten results. Found by running a 36-query set
        // where one arm matched no document -- overlap 0%, rbo 0.0, and
        // tau 1.000 side by side in the same row.
        return 0.0;
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
/// A variant doc the reference never scored is skipped and counted in the
/// returned `unscored`: it may be genuinely better or genuinely worse and
/// this measure cannot tell, so it must not be quietly averaged in.
///
/// READ THE SIGN ONLY WHEN `unscored` IS 0. The cancellation above needs
/// the compared ranks to be a permutation of the reference's own prefix.
/// Skipping unscored ranks breaks that: the surviving comparisons are a
/// SUBSET, the terms no longer pair off, and the mean can come out
/// negative without the reference being mis-sorted. A negative mean then
/// says only that on the ranks where a comparison was possible, the
/// variant happened to hold documents the reference scored above its own
/// documents at those positions -- an artifact of which ranks dropped
/// out, not a claim that the variant beat the reference.
///
/// So `unscored` is not a footnote, it is the precondition. At
/// `unscored == 0` the mean is >= 0 and means what it says; as `unscored`
/// grows the two arms have diverged past what this measure can judge, and
/// the ranking-order measures ([`rbo`], [`kendall_tau`]) are what remain
/// interpretable.
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
        // NaN, not 0.0, when nothing was comparable. Zero regret reads
        // as "gave up nothing", which is the opposite of "could not
        // measure": it is the BEST possible value standing in for no
        // value at all. NaN cannot be mistaken for a result, does not
        // average into a summary, and shows up as NaN wherever it is
        // printed -- it fails loud instead of looking good.
        mean: if counted == 0 {
            f64::NAN
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
    /// Mean score given up per compared rank, or NaN when `counted` is 0
    /// and there was nothing to compare.
    ///
    /// Interpretable only when `unscored` is 0, where it is >= 0 for a
    /// descending reference; with unscored ranks the comparison is a
    /// subset and the sign carries no meaning. See [`score_regret`].
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
    fn unmeasurable_regret_is_nan_not_a_flattering_zero() {
        // counted == 0 means nothing could be compared. Reporting 0.0
        // there hands back the BEST possible value in place of no value,
        // which is how an unmeasured arm comes to look like a perfect
        // one. NaN cannot be read as a result and will not average into
        // a summary.
        let reference = [(1u64, 0.9f32), (2, 0.5)];
        let r = score_regret(&reference, &[97u64, 98], 2);
        assert_eq!(r.counted, 0, "neither variant doc is in the reference");
        assert_eq!(r.unscored, 2);
        assert!(r.mean.is_nan(), "unmeasurable regret must be NaN: {r:?}");
        // An empty variant is the same situation.
        assert!(score_regret(&reference, &[], 2).mean.is_nan());
        // And a real comparison still produces a real number.
        assert!(score_regret(&reference, &[1u64, 2], 2).mean.is_finite());
    }

    #[test]
    fn an_empty_ranking_never_reads_as_agreement() {
        // Found live: an arm that matched no document scored tau 1.000
        // against an arm that returned ten, on the same row as overlap
        // 0% and rbo 0.0000. Every union member is equally absent from
        // the empty list, so every pair ties, the denominator goes to
        // zero, and the old code called that perfect agreement.
        let full = [10u64, 20, 30];
        assert_eq!(kendall_tau(&[], &full), 0.0, "nothing agrees with everything");
        assert_eq!(kendall_tau(&full, &[]), 0.0, "and it is symmetric");
        // The one-result case has no pair to compare at all.
        assert_eq!(kendall_tau(&[], &[10u64]), 0.0);
        assert_eq!(kendall_tau(&[10u64], &[]), 0.0);
        // Two empties really are identical, and still agree.
        assert_eq!(kendall_tau(&[], &[]), 1.0);
        assert_eq!(kendall_tau(&full, &full), 1.0);
        // The other measures already said so; tau now matches them.
        assert_eq!(overlap_at_k(&[], &full, 3), 0.0);
        assert_eq!(rbo(&[], &full, 0.9), 0.0);
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
    fn unscored_ranks_break_the_cancellation_so_the_sign_stops_meaning_anything() {
        // Observed live on the canary cluster before it was understood:
        // a caption-weighted arm reported NEGATIVE regret, which the docs
        // then called impossible against a sorted reference.
        //
        // The cancellation needs the compared ranks to be a permutation
        // of the reference's prefix. Here the variant drops doc 4 and
        // introduces unknown 99, so rank 3 is skipped and the survivors
        // no longer pair off: doc 1 (the reference's best) is compared
        // against the reference's rank-1 score, and the sum goes
        // negative without the reference being mis-sorted.
        let reference = [(1u64, 0.9f32), (2, 0.6), (3, 0.5), (4, 0.4)];
        let variant = [2u64, 1, 99, 3];
        let r = score_regret(&reference, &variant, 4);
        assert_eq!(r.unscored, 1, "99 is outside the reference");
        assert_eq!(r.counted, 3);
        assert!(
            r.mean < 0.0,
            "negative regret must be reachable via unscored ranks: {r:?}"
        );

        // And with every variant doc scored, the same reordering cancels
        // exactly -- the sign is only trustworthy at unscored == 0.
        let whole = score_regret(&reference, &[2u64, 1, 4, 3], 4);
        assert_eq!(whole.unscored, 0);
        assert!(
            whole.mean.abs() < 1e-12,
            "a pure permutation must still cancel: {whole:?}"
        );
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

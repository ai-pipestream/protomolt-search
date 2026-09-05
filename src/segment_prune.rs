//! Segment pruning from per-segment summaries (`docs/segment-pruning.md`).
//!
//! A sealed segment records, per integer and double column, the least
//! and greatest stored value and how many rows carry one
//! (`crate::segments::SegmentSummary`). A request whose filter cannot be
//! satisfied by any value in a segment's range holds no matching row in
//! that segment, so the scan may leave the segment unopened and the
//! answer is unchanged. This module decides that, and only that: it
//! answers "no row of this segment can pass" or "maybe", never "some
//! row passes".
//!
//! Soundness rules, all conservative:
//! - a range leaf is impossible when the summary range for its column is
//!   disjoint from the predicate, or when the segment holds no value for
//!   the column at all (a missing value never satisfies a range);
//! - `And` is impossible when any child is; `Or` when every child is;
//! - `Not`, and every leaf kind the summary cannot bound (facets, maps,
//!   geo, strings, an unresolved number), are "maybe";
//! - a segment without a summary is "maybe".
//!
//! Columns are matched by NAME: the filter carries the shard's table
//! indices, and a segment's summary names its columns, so the caller
//! supplies the shard's index-to-name mapping.

use crate::filter::{Edge, ResolvedFilter, ResolvedLeaf};
use crate::segments::SegmentSummary;

/// The shard's column tables, by index, as the resolved filter refers to
/// them.
pub trait ColumnNames {
    /// The name of integer column `ii`, if the index is in range.
    fn integer_name(&self, ii: usize) -> Option<&str>;
    /// The name of double column `ni`, if the index is in range.
    fn numeric_name(&self, ni: usize) -> Option<&str>;
}

impl ColumnNames for crate::segmented::SegmentedShard {
    fn integer_name(&self, ii: usize) -> Option<&str> {
        (ii < self.integer_count()).then(|| self.integer_name(ii))
    }
    fn numeric_name(&self, ni: usize) -> Option<&str> {
        (ni < self.numeric_count()).then(|| self.numeric_name(ni))
    }
}

/// How many segments a request looked at and how many it ruled out.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PruneStats {
    /// Sealed segments in the shard's snapshot.
    pub segments_total: u32,
    /// Segments ruled out from their summaries without being opened.
    pub segments_skipped: u32,
}

impl PruneStats {
    /// Fold another shard's or route's counts into this one.
    pub fn add(&mut self, other: PruneStats) {
        self.segments_total = self.segments_total.saturating_add(other.segments_total);
        self.segments_skipped = self.segments_skipped.saturating_add(other.segments_skipped);
    }
}

/// `true` when NO row of a segment described by `summary` can satisfy
/// `filter`. `false` means "maybe": the segment must be evaluated.
pub fn no_row_can_pass(
    filter: &ResolvedFilter,
    summary: &SegmentSummary,
    names: &dyn ColumnNames,
) -> bool {
    match filter {
        ResolvedFilter::And(children) => children
            .iter()
            .any(|child| no_row_can_pass(child, summary, names)),
        ResolvedFilter::Or(children) => {
            !children.is_empty()
                && children
                    .iter()
                    .all(|child| no_row_can_pass(child, summary, names))
        }
        // NOT of "maybe" is "maybe", and NOT of "impossible" would need
        // "every row passes", which a range summary cannot establish
        // (rows without the value are Unknown, and NOT Unknown is
        // Unknown). Never prune under a negation.
        ResolvedFilter::Not(_) => false,
        ResolvedFilter::Leaf(leaf) => leaf_impossible(leaf, summary, names),
    }
}

fn leaf_impossible(leaf: &ResolvedLeaf, summary: &SegmentSummary, names: &dyn ColumnNames) -> bool {
    match leaf {
        ResolvedLeaf::IntRange { column, lo, hi } => {
            let Some(name) = names.integer_name(*column) else {
                return false;
            };
            int_range_impossible(summary, name, *lo, *hi)
        }
        ResolvedLeaf::F64Range { column, lo, hi } => {
            let Some(name) = names.numeric_name(*column) else {
                return false;
            };
            f64_range_impossible(summary, name, lo.as_ref(), hi.as_ref())
        }
        // A presence test is total: it is impossible only when every
        // family the name resolves to is known empty here. Facet and
        // geo tables are not summarized, so a name in either is "maybe".
        ResolvedLeaf::Has {
            facet,
            numeric,
            integer,
            geo,
        } => {
            if facet.is_some() || geo.is_some() {
                return false;
            }
            let int_empty = integer.is_none_or(|ii| {
                names
                    .integer_name(ii)
                    .is_some_and(|name| int_present(summary, name) == Some(0))
            });
            let num_empty = numeric.is_none_or(|ni| {
                names
                    .numeric_name(ni)
                    .is_some_and(|name| numeric_present(summary, name) == Some(0))
            });
            // A `has` that resolved to no family at all is False on
            // every row already; leave that verdict to the evaluator.
            (integer.is_some() || numeric.is_some()) && int_empty && num_empty
        }
        ResolvedLeaf::Facet { .. }
        | ResolvedLeaf::NumberUnknown
        | ResolvedLeaf::MapFacet { .. }
        | ResolvedLeaf::MapNumber { .. }
        | ResolvedLeaf::MapHasKey(_)
        | ResolvedLeaf::Geo { .. }
        | ResolvedLeaf::FacetOrdRange { .. }
        | ResolvedLeaf::MapFacetOrdRange { .. } => false,
    }
}

fn int_present(summary: &SegmentSummary, name: &str) -> Option<u64> {
    summary
        .int_columns
        .iter()
        .find(|c| c.name == name)
        .map(|c| c.present)
}

fn numeric_present(summary: &SegmentSummary, name: &str) -> Option<u64> {
    summary
        .numeric_columns
        .iter()
        .find(|c| c.name == name)
        .map(|c| c.present)
}

/// An inclusive integer predicate `lo..=hi` against the segment's
/// range for `name`. `lo > hi` is the empty predicate and is impossible
/// everywhere. A partition range on the same column narrows further:
/// every row that carries the column has a value inside it.
fn int_range_impossible(summary: &SegmentSummary, name: &str, lo: i64, hi: i64) -> bool {
    if lo > hi {
        return true;
    }
    let Some(column) = summary.int_columns.iter().find(|c| c.name == name) else {
        // The summary does not know the column: written before the
        // column existed on this shard, or a different table. Maybe.
        return false;
    };
    if column.present == 0 {
        return true;
    }
    if column.max < lo || column.min > hi {
        return true;
    }
    match summary.partition.as_ref() {
        Some(range) if range.column == name => range.hi < lo || range.lo > hi,
        _ => false,
    }
}

/// A double predicate with optional exact edges against the segment's
/// range for `name`. Some value in `[min, max]` passes both edges
/// exactly when `max` passes the lower edge and `min` passes the upper
/// one, because each edge is monotone over the reals.
fn f64_range_impossible(
    summary: &SegmentSummary,
    name: &str,
    lo: Option<&Edge>,
    hi: Option<&Edge>,
) -> bool {
    let Some(column) = summary.numeric_columns.iter().find(|c| c.name == name) else {
        return false;
    };
    if column.present == 0 {
        return true;
    }
    if column.min.is_nan() || column.max.is_nan() || column.min > column.max {
        // Never written by the seal for a populated column; treat as
        // unknown rather than trust it.
        return false;
    }
    let lower_ok = lo.is_none_or(|edge| edge.admits_from_below(column.max));
    let upper_ok = hi.is_none_or(|edge| edge.admits_from_above(column.min));
    !(lower_ok && upper_ok)
}

/// One verdict per sealed segment of a snapshot, in catalog order:
/// `true` = pruned. Segments without a summary are never pruned. With
/// `enabled == false` every verdict is `false` and only the total is
/// counted, so an A/B run reports the same denominator.
pub fn verdicts_over(
    filter: Option<&ResolvedFilter>,
    summaries: &[Option<&SegmentSummary>],
    names: &dyn ColumnNames,
    enabled: bool,
) -> (Vec<bool>, PruneStats) {
    let mut stats = PruneStats::default();
    let mut out = Vec::with_capacity(summaries.len());
    for summary in summaries {
        stats.segments_total += 1;
        let pruned = enabled
            && match (filter, summary) {
                (Some(filter), Some(summary)) => no_row_can_pass(filter, summary, names),
                _ => false,
            };
        if pruned {
            stats.segments_skipped += 1;
        }
        out.push(pruned);
    }
    (out, stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter::NumBound;
    use crate::segments::{IntColumnSummary, NumericColumnSummary, PartitionRange};

    struct Names;
    impl ColumnNames for Names {
        fn integer_name(&self, ii: usize) -> Option<&str> {
            [Some("year"), Some("pages")].get(ii).copied().flatten()
        }
        fn numeric_name(&self, ni: usize) -> Option<&str> {
            [Some("score")].get(ni).copied().flatten()
        }
    }

    fn summary() -> SegmentSummary {
        SegmentSummary {
            int_columns: vec![
                IntColumnSummary {
                    name: "year".into(),
                    min: 2000,
                    max: 2009,
                    present: 10,
                },
                IntColumnSummary {
                    name: "pages".into(),
                    min: i64::MAX,
                    max: i64::MIN,
                    present: 0,
                },
            ],
            numeric_columns: vec![NumericColumnSummary {
                name: "score".into(),
                min: 0.25,
                max: 0.75,
                present: 3,
            }],
            partition: None,
        }
    }

    fn int(column: usize, lo: i64, hi: i64) -> ResolvedFilter {
        ResolvedFilter::Leaf(ResolvedLeaf::IntRange { column, lo, hi })
    }

    fn edge(value: NumBound, exclusive: bool) -> Edge {
        Edge { value, exclusive }
    }

    fn f64r(lo: Option<Edge>, hi: Option<Edge>) -> ResolvedFilter {
        ResolvedFilter::Leaf(ResolvedLeaf::F64Range { column: 0, lo, hi })
    }

    #[test]
    fn int_range_boundaries_are_inclusive() {
        let s = summary();
        // Touching the range at either end is possible.
        assert!(!no_row_can_pass(&int(0, 2009, 2100), &s, &Names));
        assert!(!no_row_can_pass(&int(0, 1900, 2000), &s, &Names));
        // One past either end is impossible.
        assert!(no_row_can_pass(&int(0, 2010, 2100), &s, &Names));
        assert!(no_row_can_pass(&int(0, 1900, 1999), &s, &Names));
        // Inside, and covering, are possible.
        assert!(!no_row_can_pass(&int(0, 2003, 2004), &s, &Names));
        assert!(!no_row_can_pass(&int(0, i64::MIN, i64::MAX), &s, &Names));
        // The empty predicate is impossible everywhere.
        assert!(no_row_can_pass(&int(0, 5, 4), &s, &Names));
    }

    #[test]
    fn absent_column_values_never_satisfy_a_range() {
        let s = summary();
        assert!(no_row_can_pass(&int(1, i64::MIN, i64::MAX), &s, &Names));
    }

    #[test]
    fn unknown_column_index_or_name_is_maybe() {
        let s = summary();
        assert!(!no_row_can_pass(&int(7, 1, 2), &s, &Names));
        let mut stripped = s.clone();
        stripped.int_columns.clear();
        assert!(!no_row_can_pass(&int(0, 3000, 3001), &stripped, &Names));
    }

    #[test]
    fn f64_edges_are_exact() {
        let s = summary();
        let f = |b: f64, ex: bool| edge(NumBound::F(b), ex);
        // >= max is possible; > max is impossible.
        assert!(!no_row_can_pass(
            &f64r(Some(f(0.75, false)), None),
            &s,
            &Names
        ));
        assert!(no_row_can_pass(
            &f64r(Some(f(0.75, true)), None),
            &s,
            &Names
        ));
        // <= min is possible; < min is impossible.
        assert!(!no_row_can_pass(
            &f64r(None, Some(f(0.25, false))),
            &s,
            &Names
        ));
        assert!(no_row_can_pass(
            &f64r(None, Some(f(0.25, true))),
            &s,
            &Names
        ));
        // Integer bounds compare exactly against the doubles.
        let i = |n: i64, ex: bool| edge(NumBound::I(n), ex);
        assert!(no_row_can_pass(&f64r(Some(i(1, false)), None), &s, &Names));
        assert!(!no_row_can_pass(&f64r(Some(i(0, false)), None), &s, &Names));
        assert!(!no_row_can_pass(
            &f64r(Some(i(0, true)), Some(i(1, true))),
            &s,
            &Names
        ));
        // A window strictly inside the range is possible.
        assert!(!no_row_can_pass(
            &f64r(Some(f(0.3, true)), Some(f(0.4, true))),
            &s,
            &Names
        ));
        // A window below the range is impossible.
        assert!(no_row_can_pass(
            &f64r(Some(f(0.0, false)), Some(f(0.2, false))),
            &s,
            &Names
        ));
    }

    #[test]
    fn and_or_not_compose_soundly() {
        let s = summary();
        let possible = int(0, 2005, 2006);
        let impossible = int(0, 2050, 2060);
        assert!(no_row_can_pass(
            &ResolvedFilter::And(vec![possible.clone(), impossible.clone()]),
            &s,
            &Names
        ));
        assert!(!no_row_can_pass(
            &ResolvedFilter::Or(vec![possible.clone(), impossible.clone()]),
            &s,
            &Names
        ));
        assert!(no_row_can_pass(
            &ResolvedFilter::Or(vec![impossible.clone(), impossible.clone()]),
            &s,
            &Names
        ));
        // An empty OR is never pruned here; the evaluator owns it.
        assert!(!no_row_can_pass(&ResolvedFilter::Or(vec![]), &s, &Names));
        // NOT of an impossible leaf is NOT prunable: rows without the
        // value are Unknown under the leaf and Unknown under the NOT.
        assert!(!no_row_can_pass(
            &ResolvedFilter::Not(Box::new(impossible.clone())),
            &s,
            &Names
        ));
        assert!(!no_row_can_pass(
            &ResolvedFilter::Not(Box::new(possible)),
            &s,
            &Names
        ));
        // AND under NOT under AND: the NOT branch is maybe, the sibling
        // decides.
        assert!(no_row_can_pass(
            &ResolvedFilter::And(vec![
                ResolvedFilter::Not(Box::new(impossible.clone())),
                impossible,
            ]),
            &s,
            &Names
        ));
    }

    #[test]
    fn partition_range_narrows_the_column() {
        let mut s = summary();
        s.partition = Some(PartitionRange {
            column: "year".into(),
            lo: 2000,
            hi: 2004,
        });
        assert!(no_row_can_pass(&int(0, 2005, 2009), &s, &Names));
        assert!(!no_row_can_pass(&int(0, 2004, 2009), &s, &Names));
        s.partition = Some(PartitionRange {
            column: "pages".into(),
            lo: 0,
            hi: 1,
        });
        // A partition on another column says nothing about year.
        assert!(!no_row_can_pass(&int(0, 2005, 2009), &s, &Names));
    }

    #[test]
    fn presence_prunes_only_known_empty_families() {
        let s = summary();
        let has = |facet, numeric, integer, geo| {
            ResolvedFilter::Leaf(ResolvedLeaf::Has {
                facet,
                numeric,
                integer,
                geo,
            })
        };
        assert!(no_row_can_pass(&has(None, None, Some(1), None), &s, &Names));
        assert!(!no_row_can_pass(
            &has(None, None, Some(0), None),
            &s,
            &Names
        ));
        assert!(!no_row_can_pass(
            &has(Some(0), None, Some(1), None),
            &s,
            &Names
        ));
        assert!(!no_row_can_pass(
            &has(None, Some(0), Some(1), None),
            &s,
            &Names
        ));
        assert!(!no_row_can_pass(&has(None, None, None, None), &s, &Names));
    }

    #[test]
    fn verdicts_count_totals_with_pruning_off() {
        let s = summary();
        let filter = int(0, 2050, 2060);
        let sums = [Some(&s), None, Some(&s)];
        let (on, stats_on) = verdicts_over(Some(&filter), &sums, &Names, true);
        assert_eq!(on, vec![true, false, true]);
        assert_eq!(
            stats_on,
            PruneStats {
                segments_total: 3,
                segments_skipped: 2
            }
        );
        let (off, stats_off) = verdicts_over(Some(&filter), &sums, &Names, false);
        assert_eq!(off, vec![false, false, false]);
        assert_eq!(
            stats_off,
            PruneStats {
                segments_total: 3,
                segments_skipped: 0
            }
        );
        let (none, _) = verdicts_over(None, &sums, &Names, true);
        assert_eq!(none, vec![false, false, false]);
    }
}

//! Segment pruning from per-segment summaries.
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

#![deny(clippy::result_large_err)]

mod leaf;
#[cfg(test)]
mod tests;

use crate::filter::ResolvedFilter;
use crate::segments::SegmentSummary;

use leaf::leaf_impossible;

/// The shard's column tables, by index, as the resolved filter refers to
/// them.
pub trait ColumnNames {
    /// The name of integer column `ii`, if the index is in range.
    fn integer_name(&self, ii: usize) -> Option<&str>;
    /// The name of unsigned column `ii`.
    fn unsigned_integer_name(&self, ii: usize) -> Option<&str>;
    /// The name of double column `ni`, if the index is in range.
    fn numeric_name(&self, ni: usize) -> Option<&str>;
}

impl ColumnNames for crate::segmented::SegmentedShard {
    fn integer_name(&self, ii: usize) -> Option<&str> {
        (ii < self.integer_count()).then(|| self.integer_name(ii))
    }
    fn unsigned_integer_name(&self, ii: usize) -> Option<&str> {
        (ii < self.unsigned_integer_count()).then(|| self.unsigned_integer_name(ii))
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

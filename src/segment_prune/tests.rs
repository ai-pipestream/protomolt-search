use super::*;
use crate::filter::{Edge, NumBound, ResolvedFilter, ResolvedLeaf};
use crate::segments::{IntColumnSummary, NumericColumnSummary, PartitionRange, SegmentSummary};

struct Names;
impl ColumnNames for Names {
    fn unsigned_integer_name(&self, _ii: usize) -> Option<&str> {
        None
    }
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
        uint_columns: Vec::new(),
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
            unsigned_integer: None,
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

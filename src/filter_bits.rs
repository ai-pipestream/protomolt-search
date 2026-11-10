//! A resolved filter evaluated a column at a time into a slot bitmap
//! (`docs/vector-filters.md`, "The mask from the columns").
//!
//! [`crate::filter::DocFilter::passes`] answers one row. A vector search
//! needs the answer for every row of the shard before it starts, and asking
//! row by row costs a walk of the filter tree and a routed column read per
//! row, per query: at a million rows that is longer than the graph walk the
//! mask is for. Here each node of the tree answers for all rows at once:
//!
//! - a facet leaf is the union of the row sets of the values it accepts,
//!   read from the inverted column ([`ColumnSets`]), so it costs the rows it
//!   admits, not the rows of the shard;
//! - a number range is a run of the column sorted by value
//!   ([`crate::number_rows`]), or a read of the rows still in question when
//!   those are the fewer ([`range_part`]);
//! - every other leaf is asked row by row, but only over the rows still in
//!   question when its turn comes;
//! - AND, OR and NOT are word-wise.
//!
//! The filter language is three-valued (a comparison against an absent
//! value is unknown, and unknown is not admitted), so a node cannot hand up
//! "the rows where I am true" alone: NOT needs the rows where its child is
//! false, which are not the complement of the true ones. [`members`]
//! therefore answers for one verdict at a time, the true rows or the false
//! rows, inside a domain; NOT swaps the verdict, and unknown rows are in
//! neither answer.

use std::ops::Range;

use crate::boolean_bits::Bits;
use crate::filter::{Edge, ResolvedFilter, ResolvedLeaf, Tri};
use crate::number_rows::NumberRows;
use crate::scorefn::NumericRead;

/// A range over one number column, as a resolved leaf carries it.
#[derive(Debug, Clone, Copy)]
pub enum NumberRange<'a> {
    /// Inclusive bounds over an i64 column.
    Int { column: usize, lo: i64, hi: i64 },
    /// Inclusive bounds over a u64 column.
    Uint { column: usize, lo: u64, hi: u64 },
    /// Edges over an f64 column; `None` is unbounded.
    F64 {
        column: usize,
        lo: Option<&'a Edge>,
        hi: Option<&'a Edge>,
    },
}

/// The number columns of one part of a shard: a row's value, and the column
/// sorted by value where the part keeps that (a sealed part does, a part
/// still being written does not). A column the part lacks holds no value.
pub trait PartNumbers {
    fn int_value(&self, column: usize, row: u32) -> Option<i64>;
    fn uint_value(&self, column: usize, row: u32) -> Option<u64>;
    fn f64_value(&self, column: usize, row: u32) -> Option<f64>;
    fn int_sorted(&self, _column: usize) -> Option<&NumberRows<i64>> {
        None
    }
    fn uint_sorted(&self, _column: usize) -> Option<&NumberRows<u64>> {
        None
    }
    fn f64_sorted(&self, _column: usize) -> Option<&NumberRows<f64>> {
        None
    }
}

impl NumberRange<'_> {
    /// Answer the range over one part whose rows are the slots of `span`
    /// ([`range_part`]).
    pub fn part(
        &self,
        part: &dyn PartNumbers,
        span: Range<usize>,
        want: bool,
        domain: &Bits,
        out: &mut Bits,
    ) {
        match *self {
            NumberRange::Int { column, lo, hi } => range_part(
                part.int_sorted(column),
                span,
                &|row| part.int_value(column, row),
                &|v| v >= lo,
                &|v| v <= hi,
                want,
                domain,
                out,
            ),
            NumberRange::Uint { column, lo, hi } => range_part(
                part.uint_sorted(column),
                span,
                &|row| part.uint_value(column, row),
                &|v| v >= lo,
                &|v| v <= hi,
                want,
                domain,
                out,
            ),
            NumberRange::F64 { column, lo, hi } => range_part(
                part.f64_sorted(column),
                span,
                &|row| part.f64_value(column, row),
                &|v| lo.is_none_or(|edge| edge.admits_from_below(v)),
                &|v| hi.is_none_or(|edge| edge.admits_from_above(v)),
                want,
                domain,
                out,
            ),
        }
    }
}

/// Reading a row's value costs about this many times what setting one bit
/// from a sorted run does: a run is used while it is no longer than this
/// many times the rows still in question.
const READ_PER_RUN_ROW: usize = 4;

/// One part of a shard answering a range: set in `out` the slots of
/// `domain` within `span` where the range evaluates to `want`. `sorted` is
/// the part's column in value order, `None` for a part that keeps none (a
/// tail still being written); `read` gives a part-local row's value. The
/// range is `from_below` and `from_above`, as [`NumberRows::run`] takes
/// them.
///
/// The run costs the rows it names wherever they are; the read costs the
/// rows of `domain`. Whichever is fewer is used, so a range under an AND
/// with a selective facet still reads only the facet's rows.
#[allow(clippy::too_many_arguments)]
pub fn range_part<T: Copy>(
    sorted: Option<&NumberRows<T>>,
    span: Range<usize>,
    read: &dyn Fn(u32) -> Option<T>,
    from_below: &dyn Fn(T) -> bool,
    from_above: &dyn Fn(T) -> bool,
    want: bool,
    domain: &Bits,
    out: &mut Bits,
) {
    let end = span.end.min(domain.len());
    let start = span.start.min(end);
    let asked = domain.count_range(start, end);
    if asked == 0 {
        return;
    }
    if let Some(sorted) = sorted {
        let run = sorted.run(from_below, from_above);
        let named = if want {
            run.len()
        } else {
            sorted.present() - run.len()
        };
        if named <= READ_PER_RUN_ROW * asked {
            let mut take = |rows: &[u32]| {
                for &row in rows {
                    let slot = start + row as usize;
                    if slot < end && domain.test(slot) {
                        out.set(slot);
                    }
                }
            };
            if want {
                take(sorted.inside(&run));
            } else {
                for rows in sorted.outside(&run) {
                    take(rows);
                }
            }
            return;
        }
    }
    for slot in domain.iter_range(start, end) {
        if let Some(value) = read((slot - start) as u32) {
            if (from_below(value) && from_above(value)) == want {
                out.set(slot);
            }
        }
    }
}

/// The columns of one shard, turned around: rows by the value they hold.
pub trait ColumnSets {
    /// Set in `out` the slots whose value in facet column `fi` has an
    /// ordinal `accept` admits. Ordinals are the shard's, the ones a
    /// resolved filter carries.
    fn facet_rows(&self, fi: usize, accept: &dyn Fn(u32) -> bool, out: &mut Bits);
    /// Set in `out` the slots that hold a value in facet column `fi`.
    fn facet_present(&self, fi: usize, out: &mut Bits);
    /// Set in `out` the slots of `domain` where `range` evaluates to
    /// `want`: the value is inside it, or, for `false`, there is a value
    /// and it is outside. A slot that holds no value is in neither.
    fn number_rows(&self, range: &NumberRange<'_>, want: bool, domain: &Bits, out: &mut Bits);
}

/// The slots of `domain` where `filter` evaluates to `want`: `Tri::True`
/// for `true`, `Tri::False` for `false`. A slot where it is unknown is in
/// neither.
pub fn members(
    filter: &ResolvedFilter,
    want: bool,
    domain: &Bits,
    cols: &dyn NumericRead,
    sets: &dyn ColumnSets,
) -> Bits {
    match filter {
        ResolvedFilter::Not(child) => members(child, !want, domain, cols, sets),
        ResolvedFilter::Leaf(leaf) => leaf_members(leaf, want, domain, cols, sets),
        // AND is true where every child is true and false where any child
        // is false; OR is the mirror image.
        ResolvedFilter::And(children) if want => all_of(children, true, domain, cols, sets),
        ResolvedFilter::And(children) => any_of(children, false, domain, cols, sets),
        ResolvedFilter::Or(children) if want => any_of(children, true, domain, cols, sets),
        ResolvedFilter::Or(children) => all_of(children, false, domain, cols, sets),
    }
}

/// Children in the order that leaves the least row-by-row work: the ones
/// answered from inverted columns first, so they narrow the rows the others
/// are asked about.
fn cheapest_first(children: &[ResolvedFilter]) -> Vec<&ResolvedFilter> {
    let mut ordered: Vec<&ResolvedFilter> = children.iter().collect();
    ordered.sort_by_key(|child| cost(child));
    ordered
}

/// 0 for a node answered from inverted facet columns alone, 1 for one with
/// a number range (a sorted run, or a read of the rows left), 2 for one
/// with a leaf asked row by row.
fn cost(filter: &ResolvedFilter) -> u8 {
    match filter {
        ResolvedFilter::Leaf(leaf) if inverted(leaf) => 0,
        ResolvedFilter::Leaf(leaf) if number_range(leaf).is_some() => 1,
        ResolvedFilter::Leaf(_) => 2,
        ResolvedFilter::Not(child) => cost(child),
        ResolvedFilter::And(children) | ResolvedFilter::Or(children) => {
            children.iter().map(cost).max().unwrap_or(0)
        }
    }
}

fn inverted(leaf: &ResolvedLeaf) -> bool {
    matches!(
        leaf,
        ResolvedLeaf::Facet { .. }
            | ResolvedLeaf::FacetOrdRange { .. }
            | ResolvedLeaf::NumberUnknown
    )
}

fn number_range(leaf: &ResolvedLeaf) -> Option<NumberRange<'_>> {
    match leaf {
        ResolvedLeaf::IntRange { column, lo, hi } => Some(NumberRange::Int {
            column: *column,
            lo: *lo,
            hi: *hi,
        }),
        ResolvedLeaf::UintRange { column, lo, hi } => Some(NumberRange::Uint {
            column: *column,
            lo: *lo,
            hi: *hi,
        }),
        ResolvedLeaf::F64Range { column, lo, hi } => Some(NumberRange::F64 {
            column: *column,
            lo: lo.as_ref(),
            hi: hi.as_ref(),
        }),
        _ => None,
    }
}

/// The slots of `domain` where every child evaluates to `want`. Each child
/// is asked only about the slots the ones before it left.
fn all_of(
    children: &[ResolvedFilter],
    want: bool,
    domain: &Bits,
    cols: &dyn NumericRead,
    sets: &dyn ColumnSets,
) -> Bits {
    let mut left = domain.clone();
    for child in cheapest_first(children) {
        if left.count() == 0 {
            break;
        }
        left = members(child, want, &left, cols, sets);
    }
    left
}

/// The slots of `domain` where some child evaluates to `want`. Each child
/// is asked only about the slots no child before it claimed.
fn any_of(
    children: &[ResolvedFilter],
    want: bool,
    domain: &Bits,
    cols: &dyn NumericRead,
    sets: &dyn ColumnSets,
) -> Bits {
    let mut found = Bits::empty(domain.len());
    let mut open = domain.clone();
    for child in cheapest_first(children) {
        if open.count() == 0 {
            break;
        }
        let claimed = members(child, want, &open, cols, sets);
        open.and_not(&claimed);
        found.or_with(&claimed);
    }
    found
}

fn leaf_members(
    leaf: &ResolvedLeaf,
    want: bool,
    domain: &Bits,
    cols: &dyn NumericRead,
    sets: &dyn ColumnSets,
) -> Bits {
    let from_column = |fi: usize, accept: &dyn Fn(u32) -> bool| {
        let mut holding = Bits::empty(domain.len());
        sets.facet_rows(fi, accept, &mut holding);
        if want {
            holding.and_with(domain);
            holding
        } else {
            // False where the row holds a value, and not an accepted one.
            let mut present = Bits::empty(domain.len());
            sets.facet_present(fi, &mut present);
            present.and_with(domain);
            present.and_not(&holding);
            present
        }
    };
    match leaf {
        // A column the shard lacks is unknown for every row.
        ResolvedLeaf::Facet { column: None, .. }
        | ResolvedLeaf::FacetOrdRange { column: None, .. }
        | ResolvedLeaf::NumberUnknown => Bits::empty(domain.len()),
        ResolvedLeaf::Facet {
            column: Some(fi),
            ords,
        } => from_column(*fi, &|ord| ords.binary_search(&ord).is_ok()),
        ResolvedLeaf::FacetOrdRange {
            column: Some(fi),
            lo,
            hi,
        } => from_column(*fi, &|ord| *lo <= ord && ord < *hi),
        other => {
            if let Some(range) = number_range(other) {
                let mut found = Bits::empty(domain.len());
                sets.number_rows(&range, want, domain, &mut found);
                return found;
            }
            let verdict = Tri::from(want);
            let mut kept = domain.clone();
            kept.retain(|slot| other.eval(slot as u32, cols) == verdict);
            kept
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A small column store: one facet column and one integer column, some
    /// rows holding neither.
    struct Columns {
        facet: Vec<Option<u32>>,
        integer: Vec<Option<i64>>,
        row_reads: std::cell::Cell<usize>,
    }

    impl NumericRead for Columns {
        fn value(&self, _ni: usize, _doc: u32) -> Option<f64> {
            None
        }
        fn map_value(&self, _c: usize, _k: u32, _doc: u32) -> Option<f64> {
            None
        }
        fn map_int_value(&self, _c: usize, _k: u32, _doc: u32) -> Option<i64> {
            None
        }
        fn map_uint_value(&self, _c: usize, _k: u32, _doc: u32) -> Option<u64> {
            None
        }
        fn int_value(&self, _ii: usize, doc: u32) -> Option<i64> {
            self.row_reads.set(self.row_reads.get() + 1);
            self.integer[doc as usize]
        }
        fn uint_value(&self, _ii: usize, _doc: u32) -> Option<u64> {
            None
        }
        fn geo_value(&self, _gi: usize, _doc: u32) -> Option<(f64, f64)> {
            None
        }
        fn facet_ord(&self, _fi: usize, doc: u32) -> Option<u32> {
            self.facet[doc as usize]
        }
        fn map_facet_value_ord(&self, _c: usize, _k: u32, _doc: u32) -> Option<u32> {
            None
        }
    }

    impl ColumnSets for Columns {
        fn number_rows(&self, range: &NumberRange<'_>, want: bool, domain: &Bits, out: &mut Bits) {
            let NumberRange::Int { lo, hi, .. } = *range else {
                unreachable!("the test store has one integer column")
            };
            let rows = self.integer.len();
            // The first half is a part with a sorted column, the second a
            // part without one.
            let half = rows / 2;
            let sorted = NumberRows::build(half, |row| self.integer[row], i64::cmp);
            for (sorted, span) in [(Some(&sorted), 0..half), (None, half..rows)] {
                let base = span.start;
                range_part(
                    sorted,
                    span,
                    &|row| self.int_value(0, row + base as u32),
                    &|v| v >= lo,
                    &|v| v <= hi,
                    want,
                    domain,
                    out,
                );
            }
        }

        fn facet_rows(&self, _fi: usize, accept: &dyn Fn(u32) -> bool, out: &mut Bits) {
            for (slot, ord) in self.facet.iter().enumerate() {
                if ord.is_some_and(accept) {
                    out.set(slot);
                }
            }
        }
        fn facet_present(&self, _fi: usize, out: &mut Bits) {
            for (slot, ord) in self.facet.iter().enumerate() {
                if ord.is_some() {
                    out.set(slot);
                }
            }
        }
    }

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    fn leaf(rng: &mut Rng) -> ResolvedFilter {
        ResolvedFilter::Leaf(match rng.below(6) {
            0 => ResolvedLeaf::Facet {
                column: Some(0),
                ords: {
                    let mut ords: Vec<u32> =
                        (0..rng.below(4)).map(|_| rng.below(9) as u32).collect();
                    ords.sort_unstable();
                    ords.dedup();
                    ords
                },
            },
            1 => ResolvedLeaf::FacetOrdRange {
                column: Some(0),
                lo: rng.below(8) as u32,
                hi: rng.below(10) as u32,
            },
            2 => ResolvedLeaf::Facet {
                column: None,
                ords: vec![1],
            },
            3 => ResolvedLeaf::NumberUnknown,
            4 => ResolvedLeaf::Has {
                facet: Some(0),
                numeric: None,
                integer: Some(0),
                unsigned_integer: None,
                geo: None,
            },
            _ => {
                let lo = rng.below(60) as i64 - 10;
                ResolvedLeaf::IntRange {
                    column: 0,
                    lo,
                    hi: lo + rng.below(40) as i64,
                }
            }
        })
    }

    fn tree(rng: &mut Rng, depth: u32) -> ResolvedFilter {
        if depth == 0 || rng.below(3) == 0 {
            return leaf(rng);
        }
        let children = |rng: &mut Rng| (0..rng.below(4)).map(|_| tree(rng, depth - 1)).collect();
        match rng.below(3) {
            0 => ResolvedFilter::And(children(rng)),
            1 => ResolvedFilter::Or(children(rng)),
            _ => ResolvedFilter::Not(Box::new(tree(rng, depth - 1))),
        }
    }

    #[test]
    fn the_bitmap_answer_is_the_row_by_row_answer_for_both_verdicts() {
        let mut rng = Rng(0x00F1_17E2_B175);
        let n = 333;
        let columns = Columns {
            facet: (0..n)
                .map(|_| (rng.below(5) != 0).then(|| rng.below(9) as u32))
                .collect(),
            integer: (0..n)
                .map(|_| (rng.below(4) != 0).then(|| rng.below(60) as i64))
                .collect(),
            row_reads: std::cell::Cell::new(0),
        };
        let domains = [
            Bits::full(n),
            Bits::empty(n),
            Bits::from_bools(&(0..n).map(|i| i % 3 != 1).collect::<Vec<_>>()),
        ];
        for round in 0..400 {
            let filter = tree(&mut rng, 4);
            for domain in &domains {
                for want in [true, false] {
                    let got = members(&filter, want, domain, &columns, &columns);
                    let verdict = Tri::from(want);
                    let expected: Vec<usize> = domain
                        .iter()
                        .filter(|&slot| filter.eval(slot as u32, &columns) == verdict)
                        .collect();
                    assert_eq!(
                        got.iter().collect::<Vec<_>>(),
                        expected,
                        "round {round} want {want}: {filter:?}"
                    );
                }
            }
        }
    }

    /// A range over rows nothing has narrowed is a run of the sorted
    /// column: the part that keeps one reads no row, the part that keeps
    /// none reads its rows.
    #[test]
    fn a_range_alone_is_a_run_of_the_sorted_column() {
        let n = 1000;
        let columns = Columns {
            facet: vec![None; n],
            integer: (0..n).map(|i| Some((i % 50) as i64)).collect(),
            row_reads: std::cell::Cell::new(0),
        };
        let filter = ResolvedFilter::Leaf(ResolvedLeaf::IntRange {
            column: 0,
            lo: 10,
            hi: 11,
        });
        let got = members(&filter, true, &Bits::full(n), &columns, &columns);
        let expected: Vec<usize> = (0..n).filter(|i| matches!(i % 50, 10 | 11)).collect();
        assert_eq!(got.iter().collect::<Vec<_>>(), expected);
        assert_eq!(columns.row_reads.get(), n / 2);
        // The rows that hold a value outside it are the runs around it.
        columns.row_reads.set(0);
        let outside = ResolvedFilter::Leaf(ResolvedLeaf::IntRange {
            column: 0,
            lo: 0,
            hi: 47,
        });
        let got = members(&outside, false, &Bits::full(n), &columns, &columns);
        let expected: Vec<usize> = (0..n).filter(|i| i % 50 >= 48).collect();
        assert_eq!(got.iter().collect::<Vec<_>>(), expected);
        assert_eq!(columns.row_reads.get(), n / 2);
    }

    #[test]
    fn a_range_is_read_only_for_the_rows_the_facet_left() {
        let n = 1000;
        let columns = Columns {
            facet: (0..n).map(|i| Some((i % 100) as u32)).collect(),
            integer: (0..n).map(|i| Some(i as i64)).collect(),
            row_reads: std::cell::Cell::new(0),
        };
        // The range is written first; the facet still goes first.
        let filter = ResolvedFilter::And(vec![
            ResolvedFilter::Leaf(ResolvedLeaf::IntRange {
                column: 0,
                lo: 0,
                hi: 499,
            }),
            ResolvedFilter::Leaf(ResolvedLeaf::Facet {
                column: Some(0),
                ords: vec![7],
            }),
        ]);
        let got = members(&filter, true, &Bits::full(n), &columns, &columns);
        assert_eq!(got.iter().collect::<Vec<_>>(), [7, 107, 207, 307, 407]);
        assert_eq!(columns.row_reads.get(), 10);
    }
}

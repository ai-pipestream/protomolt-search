//! A resolved filter evaluated a column at a time into a slot bitmap.
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
//!   (the crate-private `number_rows` index), or a read of the rows still in question when
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

mod range;
#[cfg(test)]
mod tests;

use crate::boolean_bits::Bits;
use crate::filter::{Edge, ResolvedFilter, ResolvedLeaf, Tri};
use crate::number_rows::NumberRows;
use crate::scorefn::NumericRead;

pub use range::{range_part, PartRange};

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

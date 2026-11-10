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
//!   read from the inverted column ([`FacetSets`]), so it costs the rows it
//!   admits, not the rows of the shard;
//! - every other leaf is asked row by row, but only over the rows still in
//!   question when its turn comes (a number range under an AND with a
//!   facet is evaluated on the facet's rows);
//! - AND, OR and NOT are word-wise.
//!
//! The filter language is three-valued (a comparison against an absent
//! value is unknown, and unknown is not admitted), so a node cannot hand up
//! "the rows where I am true" alone: NOT needs the rows where its child is
//! false, which are not the complement of the true ones. [`members`]
//! therefore answers for one verdict at a time, the true rows or the false
//! rows, inside a domain; NOT swaps the verdict, and unknown rows are in
//! neither answer.

use crate::boolean_bits::Bits;
use crate::filter::{ResolvedFilter, ResolvedLeaf, Tri};
use crate::scorefn::NumericRead;

/// The inverted facet columns of one shard.
pub trait FacetSets {
    /// Set in `out` the slots whose value in facet column `fi` has an
    /// ordinal `accept` admits. Ordinals are the shard's, the ones a
    /// resolved filter carries.
    fn facet_rows(&self, fi: usize, accept: &dyn Fn(u32) -> bool, out: &mut Bits);
    /// Set in `out` the slots that hold a value in facet column `fi`.
    fn facet_present(&self, fi: usize, out: &mut Bits);
}

/// The slots of `domain` where `filter` evaluates to `want`: `Tri::True`
/// for `true`, `Tri::False` for `false`. A slot where it is unknown is in
/// neither.
pub fn members(
    filter: &ResolvedFilter,
    want: bool,
    domain: &Bits,
    cols: &dyn NumericRead,
    sets: &dyn FacetSets,
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

/// 0 for a node answered from inverted columns alone, 1 for one with a
/// leaf asked row by row.
fn cost(filter: &ResolvedFilter) -> u8 {
    match filter {
        ResolvedFilter::Leaf(leaf) => u8::from(!inverted(leaf)),
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

/// The slots of `domain` where every child evaluates to `want`. Each child
/// is asked only about the slots the ones before it left.
fn all_of(
    children: &[ResolvedFilter],
    want: bool,
    domain: &Bits,
    cols: &dyn NumericRead,
    sets: &dyn FacetSets,
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
    sets: &dyn FacetSets,
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
    sets: &dyn FacetSets,
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

    impl FacetSets for Columns {
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

    #[test]
    fn a_row_by_row_leaf_is_asked_only_about_the_rows_the_facet_left() {
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

use super::*;
use crate::number_rows::NumberRows;

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
                &PartRange {
                    sorted,
                    read: &|row| self.int_value(0, row + base as u32),
                    from_below: &|v| v >= lo,
                    from_above: &|v| v <= hi,
                },
                span,
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
                let mut ords: Vec<u32> = (0..rng.below(4)).map(|_| rng.below(9) as u32).collect();
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

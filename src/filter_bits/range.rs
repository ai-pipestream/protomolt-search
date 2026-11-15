//! One part of a shard answering a number range, from its sorted column
//! or from a read of the rows still in question.

use std::ops::Range;

use super::{NumberRange, PartNumbers};
use crate::boolean_bits::Bits;
use crate::number_rows::NumberRows;

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
                &PartRange {
                    sorted: part.int_sorted(column),
                    read: &|row| part.int_value(column, row),
                    from_below: &|v| v >= lo,
                    from_above: &|v| v <= hi,
                },
                span,
                want,
                domain,
                out,
            ),
            NumberRange::Uint { column, lo, hi } => range_part(
                &PartRange {
                    sorted: part.uint_sorted(column),
                    read: &|row| part.uint_value(column, row),
                    from_below: &|v| v >= lo,
                    from_above: &|v| v <= hi,
                },
                span,
                want,
                domain,
                out,
            ),
            NumberRange::F64 { column, lo, hi } => range_part(
                &PartRange {
                    sorted: part.f64_sorted(column),
                    read: &|row| part.f64_value(column, row),
                    from_below: &|v| lo.is_none_or(|edge| edge.admits_from_below(v)),
                    from_above: &|v| hi.is_none_or(|edge| edge.admits_from_above(v)),
                },
                span,
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

/// A range over one part's column. `sorted` is the part's column in value
/// order, `None` for a part that keeps none (a tail still being written);
/// `read` gives a part-local row's value. The range is `from_below` and
/// `from_above`, as [`NumberRows::run`] takes them.
pub struct PartRange<'a, T> {
    pub sorted: Option<&'a NumberRows<T>>,
    pub read: &'a dyn Fn(u32) -> Option<T>,
    pub from_below: &'a dyn Fn(T) -> bool,
    pub from_above: &'a dyn Fn(T) -> bool,
}

/// One part of a shard answering a range: set in `out` the slots of
/// `domain` within `span` where `range` evaluates to `want`.
///
/// The run costs the rows it names wherever they are; the read costs the
/// rows of `domain`. Whichever is fewer is used, so a range under an AND
/// with a selective facet still reads only the facet's rows.
pub fn range_part<T: Copy>(
    range: &PartRange<'_, T>,
    span: Range<usize>,
    want: bool,
    domain: &Bits,
    out: &mut Bits,
) {
    let &PartRange {
        sorted,
        read,
        from_below,
        from_above,
    } = range;
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

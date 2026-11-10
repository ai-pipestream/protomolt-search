//! A number column sorted: the rows that hold a value, in value order
//! (`docs/vector-filters.md`, "The mask from the columns").
//!
//! A number column stores one value per row, which answers "what does this
//! row hold". A range filter asks "which rows hold a value in this range",
//! and answering it from the forward column costs a read per row of the
//! shard on every query. Sorted by value, the rows of any range are one
//! run, found by two binary searches, and the rows that hold a value
//! outside it are the two runs around it. Built once per sealed part the
//! first time a filter names the column, and kept, since a sealed part
//! never changes: 4 bytes of row id and one value per row that holds one.
//!
//! Rows with equal values are kept in row order, so a column of few
//! distinct values (a year) sets its bits front to back.

use std::cmp::Ordering;
use std::ops::Range;

/// The rows of one part by the value they hold in one number column.
#[derive(Debug)]
pub struct NumberRows<T> {
    /// The held values, ascending.
    keys: Box<[T]>,
    /// `order[i]` is the row that holds `keys[i]`.
    order: Box<[u32]>,
}

impl<T: Copy> NumberRows<T> {
    /// Sort a column of `rows` rows. `value_at` gives a row's value, `None`
    /// when the row holds none; `cmp` is the column's order.
    pub fn build(
        rows: usize,
        value_at: impl Fn(usize) -> Option<T>,
        cmp: impl Fn(&T, &T) -> Ordering,
    ) -> Self {
        let mut held: Vec<(T, u32)> = (0..rows)
            .filter_map(|row| value_at(row).map(|value| (value, row as u32)))
            .collect();
        held.sort_unstable_by(|a, b| cmp(&a.0, &b.0).then(a.1.cmp(&b.1)));
        Self {
            keys: held.iter().map(|pair| pair.0).collect(),
            order: held.iter().map(|pair| pair.1).collect(),
        }
    }

    /// Rows that hold a value.
    pub fn present(&self) -> usize {
        self.order.len()
    }

    /// The run of sorted positions whose values pass both edges of a
    /// range. `from_below` must be false then true along ascending values
    /// (the lower edge) and `from_above` true then false (the upper edge).
    pub fn run(
        &self,
        from_below: impl Fn(T) -> bool,
        from_above: impl Fn(T) -> bool,
    ) -> Range<usize> {
        let start = self.keys.partition_point(|&key| !from_below(key));
        let end = self.keys.partition_point(|&key| from_above(key));
        start..end.max(start)
    }

    /// The rows of a run, in value order then row order.
    pub fn inside(&self, run: &Range<usize>) -> &[u32] {
        &self.order[run.clone()]
    }

    /// The rows that hold a value outside a run: below it, then above it.
    pub fn outside(&self, run: &Range<usize>) -> [&[u32]; 2] {
        [&self.order[..run.start], &self.order[run.end..]]
    }

    pub fn heap_bytes(&self) -> usize {
        std::mem::size_of_val(&*self.keys) + std::mem::size_of_val(&*self.order)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_range_is_one_run_and_its_complement_the_rows_around_it() {
        // Rows 0..10 hold (row * 7) % 5, except rows 3 and 8, which hold
        // nothing.
        let value = |row: usize| (row != 3 && row != 8).then_some((row as i64 * 7) % 5);
        let sorted = NumberRows::build(10, value, i64::cmp);
        assert_eq!(sorted.present(), 8);
        for lo in -1..6 {
            for hi in -1..6 {
                let run = sorted.run(|v| v >= lo, |v| v <= hi);
                let mut inside = sorted.inside(&run).to_vec();
                inside.sort_unstable();
                let want: Vec<u32> = (0..10)
                    .filter(|&row| value(row).is_some_and(|v| v >= lo && v <= hi))
                    .map(|row| row as u32)
                    .collect();
                assert_eq!(inside, want, "{lo}..={hi}");
                let mut outside: Vec<u32> = sorted.outside(&run).concat();
                outside.sort_unstable();
                let want: Vec<u32> = (0..10)
                    .filter(|&row| value(row).is_some_and(|v| !(v >= lo && v <= hi)))
                    .map(|row| row as u32)
                    .collect();
                assert_eq!(outside, want, "outside {lo}..={hi}");
            }
        }
    }

    #[test]
    fn equal_values_keep_row_order_and_an_empty_column_has_no_run() {
        let sorted = NumberRows::build(6, |row| Some((row % 2) as u64), u64::cmp);
        let run = sorted.run(|v| v >= 1, |v| v <= 1);
        assert_eq!(sorted.inside(&run), [1, 3, 5]);
        let empty = NumberRows::<f64>::build(4, |_| None, f64::total_cmp);
        assert_eq!(empty.present(), 0);
        assert_eq!(empty.run(|v| v >= 0.0, |v| v <= 1.0), 0..0);
        assert_eq!(empty.heap_bytes(), 0);
    }
}

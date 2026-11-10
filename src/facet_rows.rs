//! A facet column inverted: for each value ordinal, the rows that hold it
//! (`docs/vector-filters.md`, "The mask from the columns").
//!
//! A facet column stores one ordinal per row, which answers "what does this
//! row hold". A filter asks the other question, "which rows hold this
//! value", and answering it from the forward column costs a read per row of
//! the shard on every query. This is the same column the other way around:
//! built once per sealed part the first time a filter names the column, and
//! kept, since a sealed part never changes.
//!
//! A value held by few rows keeps them as a sorted list; a value held by at
//! least one row in [`DENSE_ONE_IN`] keeps a bitmap, so a union over a
//! popular value is a pass over words, not over rows. At most
//! `DENSE_ONE_IN` values can be that popular, which bounds the whole
//! structure at 4 bytes per row for the lists, plus `rows / 8` bytes per
//! popular value, plus the bitmap of the rows that hold any value.

use crate::boolean_bits::Bits;

/// A value held by at least one row in this many is kept as a bitmap.
const DENSE_ONE_IN: usize = 32;

/// The rows of one part by the value they hold in one facet column.
#[derive(Debug)]
pub struct FacetRows {
    rows: usize,
    /// Value ordinal `v`'s listed rows are `listed[starts[v]..starts[v + 1]]`,
    /// ascending; empty for a value kept as a bitmap.
    starts: Vec<u32>,
    listed: Vec<u32>,
    /// `(ordinal, rows bitmap)` of the values kept as bitmaps, by ordinal.
    dense: Vec<(u32, Vec<u64>)>,
    /// The rows that hold a value.
    present: Vec<u64>,
}

impl FacetRows {
    /// Invert a column of `rows` rows over `values` ordinals. `ord_at`
    /// gives a row's ordinal, `None` when the row holds no value; an
    /// ordinal at or past `values` is treated as no value.
    pub fn build(rows: usize, values: usize, ord_at: impl Fn(usize) -> Option<u32>) -> Self {
        let held = |row: usize| ord_at(row).filter(|&ord| (ord as usize) < values);
        let mut counts = vec![0u32; values];
        let words = rows.div_ceil(64);
        let mut present = vec![0u64; words];
        for row in 0..rows {
            if let Some(ord) = held(row) {
                counts[ord as usize] += 1;
                present[row / 64] |= 1u64 << (row % 64);
            }
        }
        let is_dense = |count: u32| count as usize * DENSE_ONE_IN >= rows.max(1);
        let mut dense_slot = vec![u32::MAX; values];
        let mut dense: Vec<(u32, Vec<u64>)> = Vec::new();
        let mut starts = Vec::with_capacity(values + 1);
        let mut total = 0u32;
        for (ord, &count) in counts.iter().enumerate() {
            starts.push(total);
            if is_dense(count) {
                dense_slot[ord] = dense.len() as u32;
                dense.push((ord as u32, vec![0u64; words]));
            } else {
                total += count;
            }
        }
        starts.push(total);
        let mut listed = vec![0u32; total as usize];
        let mut next: Vec<u32> = starts[..values].to_vec();
        for row in 0..rows {
            if let Some(ord) = held(row) {
                let ord = ord as usize;
                match dense_slot[ord] {
                    u32::MAX => {
                        listed[next[ord] as usize] = row as u32;
                        next[ord] += 1;
                    }
                    slot => dense[slot as usize].1[row / 64] |= 1u64 << (row % 64),
                }
            }
        }
        Self {
            rows,
            starts,
            listed,
            dense,
            present,
        }
    }

    /// Value ordinals the column was inverted over.
    pub fn values(&self) -> usize {
        self.starts.len() - 1
    }

    /// Set in `out` the rows that hold value `ord`, each at `base` plus its
    /// row. Rows landing at or past `out`'s length are dropped.
    pub fn union_into(&self, ord: u32, base: usize, out: &mut Bits) {
        if ord as usize >= self.values() {
            return;
        }
        if let Ok(at) = self.dense.binary_search_by_key(&ord, |entry| entry.0) {
            out.or_words_at(&self.dense[at].1, self.rows, base);
            return;
        }
        let span = self.starts[ord as usize] as usize..self.starts[ord as usize + 1] as usize;
        let len = out.len();
        for &row in &self.listed[span] {
            let slot = base + row as usize;
            if slot < len {
                out.set(slot);
            }
        }
    }

    /// Set in `out` the rows that hold any value, each at `base` plus its
    /// row.
    pub fn present_into(&self, base: usize, out: &mut Bits) {
        out.or_words_at(&self.present, self.rows, base);
    }

    /// Bytes this inversion holds on the heap.
    pub fn heap_bytes(&self) -> usize {
        4 * (self.starts.len() + self.listed.len())
            + 8 * (self.present.len() + self.dense.iter().map(|d| d.1.len()).sum::<usize>())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_inverted_column_gives_back_the_rows_of_each_value() {
        // Value 0 is popular (a bitmap), the rest are lists; every seventh
        // row holds nothing and one row names a value past the dictionary.
        let rows = 1000;
        let values = 40;
        let ord_at = |row: usize| match row {
            r if r % 7 == 0 => None,
            500 => Some(99),
            r if r % 3 == 0 => Some(0),
            r => Some(1 + (r % 39) as u32),
        };
        let inverted = FacetRows::build(rows, values, ord_at);
        assert_eq!(inverted.values(), values);
        assert_eq!(inverted.dense.len(), 1);
        for base in [0usize, 5, 64, 130] {
            let len = base + rows - 3;
            let mut all = Bits::empty(len);
            for ord in 0..values as u32 + 2 {
                let mut got = Bits::empty(len);
                inverted.union_into(ord, base, &mut got);
                let want: Vec<usize> = (0..rows)
                    .filter(|&r| r != 500 && ord_at(r) == Some(ord) && base + r < len)
                    .map(|r| base + r)
                    .collect();
                assert_eq!(
                    got.iter().collect::<Vec<_>>(),
                    want,
                    "ord {ord} base {base}"
                );
                all.or_with(&got);
            }
            let mut present = Bits::empty(len);
            inverted.present_into(base, &mut present);
            assert_eq!(present, all, "base {base}");
        }
        assert!(inverted.heap_bytes() >= 4 * inverted.listed.len());
        // An empty column inverts to nothing.
        let empty = FacetRows::build(0, 0, |_| None);
        let mut out = Bits::empty(10);
        empty.union_into(0, 0, &mut out);
        empty.present_into(3, &mut out);
        assert_eq!(out.count(), 0);
    }
}

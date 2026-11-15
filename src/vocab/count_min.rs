//! The count-min sketch.

use super::hash::{hash_base, hash_row};

/// A count-min sketch: approximate per-term frequencies with a one-sided
/// error (estimates are never below the true count). Depth 5, width 2^17 —
/// five independent hash functions over a 131072-column table of 64-bit
/// counters (5 MiB). Sketches merge cell-wise. Not thread-safe; the owning
/// window state serializes access.
pub struct CountMinSketch {
    depth: usize,
    width: usize,
    table: Vec<u64>,
    total: u64,
}

impl CountMinSketch {
    /// Sketch depth: the number of rows (independent hash functions).
    pub const DEPTH: usize = 5;
    /// Sketch width: columns per row, a power of two for cheap indexing.
    pub const WIDTH: usize = 1 << 17;

    /// A fresh, empty sketch of the standard shape.
    pub fn new() -> Self {
        Self::with_shape(Self::DEPTH, Self::WIDTH)
    }

    /// A sketch of `depth` rows by `width` columns; width must be a
    /// positive power of two.
    pub fn with_shape(depth: usize, width: usize) -> Self {
        assert!(depth > 0 && width > 0 && width.is_power_of_two());
        Self {
            depth,
            width,
            table: vec![0; depth * width],
            total: 0,
        }
    }

    /// Adds `count` occurrences of `term`.
    pub fn add(&mut self, term: &str, count: u64) {
        if count == 0 {
            return;
        }
        let base = hash_base(term);
        for row in 0..self.depth {
            let column = (hash_row(base, row) & (self.width as u64 - 1)) as usize;
            self.table[row * self.width + column] += count;
        }
        self.total += count;
    }

    /// The estimated frequency of `term`: the minimum over the rows.
    /// Never below the true count; within the sketch's error bound above it.
    pub fn estimate(&self, term: &str) -> u64 {
        let base = hash_base(term);
        let mut min = u64::MAX;
        for row in 0..self.depth {
            let column = (hash_row(base, row) & (self.width as u64 - 1)) as usize;
            min = min.min(self.table[row * self.width + column]);
        }
        min
    }

    /// Merges `other` into this sketch, cell-wise. Only meaningful for
    /// sketches of the same shape.
    pub fn merge(&mut self, other: &Self) -> Result<(), String> {
        if other.depth != self.depth || other.width != self.width {
            return Err(format!(
                "sketch shapes differ: {}x{} vs {}x{}",
                other.depth, other.width, self.depth, self.width
            ));
        }
        for (cell, &other_cell) in self.table.iter_mut().zip(&other.table) {
            *cell += other_cell;
        }
        self.total += other.total;
        Ok(())
    }

    /// The total number of occurrences ever added.
    pub fn total_count(&self) -> u64 {
        self.total
    }

    /// The number of rows.
    pub fn depth(&self) -> usize {
        self.depth
    }

    /// The number of columns per row.
    pub fn width(&self) -> usize {
        self.width
    }

    /// Serializes the table in row-major order as little-endian unsigned
    /// 64-bit counters — the `cms_table` encoding of
    /// `VocabChannelSnapshot` (`depth * width * 8` bytes).
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = vec![0u8; self.table.len() * 8];
        for (i, &value) in self.table.iter().enumerate() {
            out[i * 8..(i + 1) * 8].copy_from_slice(&value.to_le_bytes());
        }
        out
    }

    /// Restores a sketch from [`Self::to_bytes`] output, with the total
    /// occurrence count carried alongside the table.
    pub fn from_bytes(
        depth: usize,
        width: usize,
        bytes: &[u8],
        total: u64,
    ) -> Result<Self, String> {
        let sketch = Self::with_shape(depth, width);
        if bytes.len() != sketch.table.len() * 8 {
            return Err(format!(
                "table byte length {} does not match {}x{}",
                bytes.len(),
                depth,
                width
            ));
        }
        let table = bytes
            .as_chunks::<8>()
            .0
            .iter()
            .map(|chunk| u64::from_le_bytes(*chunk))
            .collect();
        Ok(Self {
            depth,
            width,
            table,
            total,
        })
    }
}

impl Default for CountMinSketch {
    fn default() -> Self {
        Self::new()
    }
}

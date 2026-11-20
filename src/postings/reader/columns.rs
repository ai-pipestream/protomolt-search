//! Typed column and inverted/sorted index accessors on `Bm25Reader`.

use super::super::columns::{dictionary_lookup, integer_present, FACET_ABSENT, INTEGER_ABSENT};
use super::{sorted_column, Bm25Reader};

impl Bm25Reader {
    /// Numeric column `ni` sorted by value. The first call reads the whole
    /// column once; later calls are a pointer read.
    pub fn numeric_rows(&self, ni: usize) -> &crate::number_rows::NumberRows<f64> {
        sorted_column(&self.number_rows.numeric, self.numerics.len(), ni, || {
            crate::number_rows::NumberRows::build(
                self.n_slots(),
                |slot| self.numeric_value(ni, slot as u32),
                f64::total_cmp,
            )
        })
    }

    /// Integer column `ii` sorted by value.
    pub fn integer_rows(&self, ii: usize) -> &crate::number_rows::NumberRows<i64> {
        sorted_column(&self.number_rows.integer, self.integers.len(), ii, || {
            crate::number_rows::NumberRows::build(
                self.n_slots(),
                |slot| self.integer_value(ii, slot as u32),
                i64::cmp,
            )
        })
    }

    /// Unsigned integer column `column` sorted by value.
    pub fn unsigned_integer_rows(&self, column: usize) -> &crate::number_rows::NumberRows<u64> {
        sorted_column(
            &self.number_rows.unsigned_integer,
            self.unsigned_integers.len(),
            column,
            || {
                crate::number_rows::NumberRows::build(
                    self.n_slots(),
                    |slot| self.unsigned_integer_value(column, slot as u32),
                    u64::cmp,
                )
            },
        )
    }

    /// Facet column `fi` inverted: the rows that hold each value ordinal.
    /// The first call reads the whole ords section once; later calls are a
    /// pointer read.
    pub fn facet_rows(&self, fi: usize) -> &crate::facet_rows::FacetRows {
        let columns = self.facet_rows.get_or_init(|| {
            (0..self.facets.len())
                .map(|_| std::sync::OnceLock::new())
                .collect()
        });
        columns[fi].get_or_init(|| {
            crate::facet_rows::FacetRows::build(
                self.n_slots(),
                self.facets[fi].dict.len(),
                |slot| self.facet_ord(fi, slot as u32),
            )
        })
    }

    /// Number of facet fields in the facet table (0 for pre-v7 files).
    pub fn facet_count(&self) -> usize {
        self.facets.len()
    }

    /// The name of facet field `fi`. Panics when out of range.
    pub fn facet_name(&self, fi: usize) -> &str {
        &self.facets[fi].name
    }

    /// The index of the facet field named `name`, if the table has it.
    pub fn facet_index(&self, name: &str) -> Option<usize> {
        self.facets.iter().position(|f| f.name == name)
    }

    /// Number of distinct values facet field `fi` holds.
    pub fn facet_value_count(&self, fi: usize) -> usize {
        self.facets[fi].dict.len()
    }

    /// The value of facet field `fi` at ordinal `ord`. Panics when out
    /// of range.
    pub fn facet_value(&self, fi: usize, ord: u32) -> &str {
        &self.facets[fi].dict[ord as usize]
    }

    /// The ordinal of `value` in facet field `fi`'s dictionary, `None`
    /// when this file never ingested it. A binary search on a file
    /// whose dictionary is in byte order (every file this writer
    /// produces), a linear scan on an older first-seen file; either
    /// way resolution runs once per request, not per document.
    pub fn facet_value_ord_of(&self, fi: usize, value: &str) -> Option<u32> {
        dictionary_lookup(&self.facets[fi].dict, self.facets[fi].sorted, value)
    }

    /// Facet field `fi`'s dictionary, in ordinal order.
    pub fn facet_dictionary(&self, fi: usize) -> &[String] {
        &self.facets[fi].dict
    }

    /// Whether facet field `fi`'s dictionary is in byte order, so an
    /// ordinal range is a string range.
    pub fn facet_dictionary_sorted(&self, fi: usize) -> bool {
        self.facets[fi].sorted
    }

    /// Map-facet column `ci`'s value dictionary, in ordinal order.
    pub fn map_facet_values(&self, ci: usize) -> &[String] {
        &self.map_facets[ci].values
    }

    /// Whether map-facet column `ci`'s value dictionary is in byte order.
    pub fn map_facet_values_sorted(&self, ci: usize) -> bool {
        self.map_facets[ci].values_sorted
    }

    /// The ordinal of `doc_id`'s value for facet field `fi`, `None`
    /// when the document has no value. One 4 B read of the mmapped
    /// fixed-stride ords section.
    pub fn facet_ord(&self, fi: usize, doc_id: u32) -> Option<u32> {
        let facet = &self.facets[fi];
        let slot = doc_id as usize;
        if slot >= self.n_slots() {
            return None;
        }
        let off = facet.ords_off as usize + 4 * slot;
        let ord = u32::from_le_bytes(self.map[off..off + 4].try_into().expect("4 bytes"));
        if ord == FACET_ABSENT {
            None
        } else {
            Some(ord)
        }
    }

    /// Number of numeric fields in the numeric table (0 pre-v7).
    pub fn numeric_count(&self) -> usize {
        self.numerics.len()
    }

    /// The name of numeric field `ni`. Panics when out of range.
    pub fn numeric_name(&self, ni: usize) -> &str {
        &self.numerics[ni].name
    }

    /// The index of the numeric field named `name`, if the table has it.
    pub fn numeric_index(&self, name: &str) -> Option<usize> {
        self.numerics.iter().position(|n| n.name == name)
    }

    /// (min, max) of numeric field `ni` over present values, from the
    /// column table's write-time metadata (validated against a full
    /// scan at open); (NaN, NaN) when no document has a value.
    pub fn numeric_min_max(&self, ni: usize) -> (f64, f64) {
        (self.numerics[ni].min, self.numerics[ni].max)
    }

    /// `doc_id`'s value for numeric field `ni`, `None` when absent.
    /// One fixed-width value read and, for kind 10, one presence-bit read.
    pub fn numeric_value(&self, ni: usize, doc_id: u32) -> Option<f64> {
        let numeric = &self.numerics[ni];
        let slot = doc_id as usize;
        if slot >= self.n_slots() {
            return None;
        }
        let off = numeric.vals_off as usize + 8 * slot;
        let v = f64::from_bits(u64::from_le_bytes(
            self.map[off..off + 8].try_into().expect("8 bytes"),
        ));
        if v.is_nan() {
            None
        } else {
            Some(v)
        }
    }

    /// Number of i64 fields in the integer table (0 pre-v7).
    pub fn integer_count(&self) -> usize {
        self.integers.len()
    }

    /// The name of integer field `ii`. Panics when out of range.
    pub fn integer_name(&self, ii: usize) -> &str {
        &self.integers[ii].name
    }

    /// The index of the integer field named `name`, if the table has it.
    pub fn integer_index(&self, name: &str) -> Option<usize> {
        self.integers.iter().position(|n| n.name == name)
    }

    /// (min, max) of integer field `ii` over present values, from the
    /// column table's write-time metadata (validated against a full
    /// scan at open); the empty range `(i64::MAX, i64::MIN)` when no
    /// document has a value.
    pub fn integer_min_max(&self, ii: usize) -> (i64, i64) {
        (self.integers[ii].min, self.integers[ii].max)
    }

    /// `doc_id`'s value for integer field `ii`, `None` when absent.
    /// One fixed-width value read and, for kind 10, one presence-bit read.
    pub fn integer_value(&self, ii: usize, doc_id: u32) -> Option<i64> {
        let integer = &self.integers[ii];
        let slot = doc_id as usize;
        if slot >= self.n_slots() {
            return None;
        }
        let off = integer.vals_off as usize + 8 * slot;
        let v = u64::from_le_bytes(self.map[off..off + 8].try_into().expect("8 bytes")) as i64;
        let present = integer.presence_off.map_or(v != INTEGER_ABSENT, |off| {
            integer_present(
                &self.map[off as usize..off as usize + self.n_slots().div_ceil(8)],
                slot,
            )
        });
        present.then_some(v)
    }

    /// Number of full-domain unsigned integer columns.
    pub fn unsigned_integer_count(&self) -> usize {
        self.unsigned_integers.len()
    }
    /// The name of one unsigned column.
    pub fn unsigned_integer_name(&self, column: usize) -> &str {
        &self.unsigned_integers[column].name
    }
    /// Resolve an unsigned column by name.
    pub fn unsigned_integer_index(&self, name: &str) -> Option<usize> {
        self.unsigned_integers
            .iter()
            .position(|column| column.name == name)
    }
    /// Bounds over present values. (MAX, 0) denotes an empty column.
    pub fn unsigned_integer_min_max(&self, column: usize) -> (u64, u64) {
        let column = &self.unsigned_integers[column];
        (column.min, column.max)
    }
    /// Read exact unsigned bits only when the row is present.
    pub fn unsigned_integer_value(&self, column: usize, row: u32) -> Option<u64> {
        let slot = row as usize;
        if slot >= self.n_slots() {
            return None;
        }
        let off = self.unsigned_integers[column].vals_off as usize;
        let bitmap = off + 8 * self.n_slots();
        if !integer_present(&self.map[bitmap..bitmap + self.n_slots().div_ceil(8)], slot) {
            return None;
        }
        Some(u64::from_le_bytes(
            self.map[off + 8 * slot..off + 8 * slot + 8]
                .try_into()
                .expect("8 bytes"),
        ))
    }

    /// Number of geo fields in the geo table (0 pre-v7).
    pub fn geo_count(&self) -> usize {
        self.geos.len()
    }

    /// The name of geo field `gi`. Panics when out of range.
    pub fn geo_name(&self, gi: usize) -> &str {
        &self.geos[gi].name
    }

    /// The index of the geo field named `name`, if the table has it.
    pub fn geo_index(&self, name: &str) -> Option<usize> {
        self.geos.iter().position(|n| n.name == name)
    }

    /// Geo field `gi`'s bounding box `(min_lat, max_lat, min_lon,
    /// max_lon)` from the column table's write-time metadata (validated
    /// against a full scan at open); all four NaN when no document has a
    /// point.
    pub fn geo_bbox(&self, gi: usize) -> (f64, f64, f64, f64) {
        self.geos[gi].bbox
    }

    /// `doc_id`'s (lat, lon) for geo field `gi`, `None` when absent.
    /// One 16 B read of the mmapped fixed-stride vals section.
    pub fn geo_value(&self, gi: usize, doc_id: u32) -> Option<(f64, f64)> {
        let geo = &self.geos[gi];
        let slot = doc_id as usize;
        if slot >= self.n_slots() {
            return None;
        }
        let off = geo.vals_off as usize + 16 * slot;
        let lat = f64::from_bits(u64::from_le_bytes(
            self.map[off..off + 8].try_into().expect("8 bytes"),
        ));
        let lon = f64::from_bits(u64::from_le_bytes(
            self.map[off + 8..off + 16].try_into().expect("8 bytes"),
        ));
        // Validation refused half-NaN pairs at open, so testing one half
        // decides both.
        if lat.is_nan() {
            None
        } else {
            Some((lat, lon))
        }
    }
}

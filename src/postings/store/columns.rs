//! Typed column, facet, and map-column accessors of the heap store.

use super::Bm25Store;
use crate::postings::columns::ColumnKind;
use std::io;

impl Bm25Store {
    /// Number of fields in the field table.
    pub fn field_count(&self) -> usize {
        self.fields.len()
    }

    /// Number of facet fields in the facet table.
    pub fn facet_count(&self) -> usize {
        self.column_count(ColumnKind::Facet)
    }

    /// The name of facet field `fi`. Panics when out of range.
    pub fn facet_name(&self, fi: usize) -> &str {
        self.column_name(ColumnKind::Facet, fi)
    }

    /// The index of the facet field named `name`, if the table has it.
    pub fn facet_index(&self, name: &str) -> Option<usize> {
        self.column_index(ColumnKind::Facet, name)
    }

    /// Number of distinct values facet field `fi` holds.
    pub fn facet_value_count(&self, fi: usize) -> usize {
        self.columns.facets[fi].dict.len()
    }

    /// The value of facet field `fi` at ordinal `ord`. Panics when out of range.
    pub fn facet_value(&self, fi: usize, ord: u32) -> &str {
        &self.columns.facets[fi].dict[ord as usize]
    }

    /// The ordinal of `doc_id`'s value for facet field `fi`, `None` when absent.
    pub fn facet_ord(&self, fi: usize, doc_id: u32) -> Option<u32> {
        self.columns.facets[fi].ord(doc_id as usize)
    }

    /// The ordinal of `value` in facet field `fi`'s dictionary, `None` when absent.
    pub fn facet_value_ord_of(&self, fi: usize, value: &str) -> Option<u32> {
        self.columns.facets[fi].index.get(value).copied()
    }

    /// Facet field `fi`'s dictionary in ordinal (first-seen) order.
    pub fn facet_dictionary(&self, fi: usize) -> &[String] {
        &self.columns.facets[fi].dict
    }

    /// Whether facet field `fi`'s first-seen dictionary is in strict byte order.
    pub fn facet_dictionary_sorted(&self, fi: usize) -> bool {
        self.columns.facet_dictionary_sorted(fi)
    }

    /// Map-facet column `ci`'s value dictionary in ordinal order.
    pub fn map_facet_values(&self, ci: usize) -> &[String] {
        &self.columns.map_facets[ci].values
    }

    /// Whether map-facet column `ci`'s value dictionary is in strict byte order.
    pub fn map_facet_values_sorted(&self, ci: usize) -> bool {
        self.columns.map_facet_values_sorted(ci)
    }

    /// Record `doc_id`'s value for facet field `fi`, interning it in the dictionary.
    pub fn set_facet(&mut self, fi: usize, doc_id: u32, value: &str) {
        self.columns.set_facet(fi, doc_id, value);
    }

    /// Number of numeric fields in the numeric table.
    pub fn numeric_count(&self) -> usize {
        self.column_count(ColumnKind::Numeric)
    }

    /// The name of numeric field `ni`. Panics when out of range.
    pub fn numeric_name(&self, ni: usize) -> &str {
        self.column_name(ColumnKind::Numeric, ni)
    }

    /// The index of the numeric field named `name`, if the table has it.
    pub fn numeric_index(&self, name: &str) -> Option<usize> {
        self.column_index(ColumnKind::Numeric, name)
    }

    /// `doc_id`'s value for numeric field `ni`, `None` when absent.
    pub fn numeric_value(&self, ni: usize, doc_id: u32) -> Option<f64> {
        self.columns.numerics[ni].value(doc_id as usize)
    }

    /// `(min, max)` of numeric field `ni` over present values; `(NaN, NaN)` when empty.
    pub fn numeric_min_max(&self, ni: usize) -> (f64, f64) {
        self.columns.numerics[ni].min_max()
    }

    /// Record `doc_id`'s finite value for numeric field `ni`.
    pub fn set_numeric(&mut self, ni: usize, doc_id: u32, value: f64) {
        self.columns.set_numeric(ni, doc_id, value);
    }

    /// Number of `i64` fields in the integer table.
    pub fn integer_count(&self) -> usize {
        self.column_count(ColumnKind::Integer)
    }

    /// The name of integer field `ii`. Panics when out of range.
    pub fn integer_name(&self, ii: usize) -> &str {
        self.column_name(ColumnKind::Integer, ii)
    }

    /// The index of the integer field named `name`, if the table has it.
    pub fn integer_index(&self, name: &str) -> Option<usize> {
        self.column_index(ColumnKind::Integer, name)
    }

    /// `doc_id`'s value for integer field `ii`, `None` when absent.
    pub fn integer_value(&self, ii: usize, doc_id: u32) -> Option<i64> {
        self.columns.integers[ii].value(doc_id as usize)
    }

    /// `(min, max)` of integer field `ii` over present values; `(i64::MAX, i64::MIN)` when empty.
    pub fn integer_min_max(&self, ii: usize) -> (i64, i64) {
        self.columns.integers[ii].min_max()
    }

    /// Record `doc_id`'s value for integer field `ii`.
    pub fn set_integer(&mut self, ii: usize, doc_id: u32, value: i64) {
        self.columns.set_integer(ii, doc_id, value);
    }

    /// Number of `u64` fields in the unsigned integer table.
    pub fn unsigned_integer_count(&self) -> usize {
        self.column_count(ColumnKind::UnsignedInteger)
    }

    /// The name of unsigned integer field `ii`. Panics when out of range.
    pub fn unsigned_integer_name(&self, ii: usize) -> &str {
        self.column_name(ColumnKind::UnsignedInteger, ii)
    }

    /// The index of the unsigned integer field named `name`, if the table has it.
    pub fn unsigned_integer_index(&self, name: &str) -> Option<usize> {
        self.column_index(ColumnKind::UnsignedInteger, name)
    }

    /// `doc_id`'s value for unsigned integer field `ii`, `None` when absent.
    pub fn unsigned_integer_value(&self, ii: usize, doc_id: u32) -> Option<u64> {
        self.columns.unsigned_integers[ii].value(doc_id as usize)
    }

    /// `(min, max)` of unsigned integer field `ii` over present values; `(u64::MAX, u64::MIN)` when empty.
    pub fn unsigned_integer_min_max(&self, ii: usize) -> (u64, u64) {
        self.columns.unsigned_integers[ii].min_max()
    }

    /// Record `doc_id`'s value for unsigned integer field `ii`.
    pub fn set_unsigned_integer(&mut self, ii: usize, doc_id: u32, value: u64) {
        self.columns.set_unsigned_integer(ii, doc_id, value);
    }

    /// Number of geo fields in the geo table.
    pub fn geo_count(&self) -> usize {
        self.column_count(ColumnKind::Geo)
    }

    /// The name of geo field `gi`. Panics when out of range.
    pub fn geo_name(&self, gi: usize) -> &str {
        self.column_name(ColumnKind::Geo, gi)
    }

    /// The index of the geo field named `name`, if the table has it.
    pub fn geo_index(&self, name: &str) -> Option<usize> {
        self.column_index(ColumnKind::Geo, name)
    }

    /// `doc_id`'s `(lat, lon)` for geo field `gi`, `None` when absent.
    pub fn geo_value(&self, gi: usize, doc_id: u32) -> Option<(f64, f64)> {
        self.columns.geos[gi].value(doc_id as usize)
    }

    /// Geo field `gi`'s bounding box `(min_lat, max_lat, min_lon, max_lon)` over present points.
    pub fn geo_bbox(&self, gi: usize) -> (f64, f64, f64, f64) {
        self.columns.geos[gi].bbox()
    }

    /// Record `doc_id`'s finite WGS-84 point for geo field `gi`.
    pub fn set_geo(&mut self, gi: usize, doc_id: u32, lat: f64, lon: f64) {
        self.columns.set_geo(gi, doc_id, lat, lon);
    }

    /// Number of `map<string, string>` columns.
    pub fn map_facet_count(&self) -> usize {
        self.column_count(ColumnKind::MapFacet)
    }

    /// The name of map-facet column `ci`. Panics when out of range.
    pub fn map_facet_name(&self, ci: usize) -> &str {
        self.column_name(ColumnKind::MapFacet, ci)
    }

    /// Set an exact value in an existing document slot. Duplicate keys return an error.
    pub fn set_map_integer(
        &mut self,
        ci: usize,
        doc_id: u32,
        key: &str,
        value: i64,
    ) -> io::Result<()> {
        self.columns
            .set_map_integer(self.texts.len(), ci, doc_id, key, value)
    }

    /// Map keys in this store's ordinal order.
    pub fn map_integer_keys(&self, ci: usize) -> &[String] {
        &self.columns.map_integers[ci].keys
    }

    /// Find a key, including the empty string.
    pub fn map_integer_key_ord(&self, ci: usize, key: &str) -> Option<u32> {
        self.columns.map_integers[ci].key_ord(key)
    }

    /// Exact bounds, or `None` when the key has no entries.
    pub fn map_integer_key_min_max(&self, ci: usize, key: u32) -> Option<(i64, i64)> {
        self.columns.map_integers[ci].bounds(key)
    }

    /// Exact value; an absent entry or out-of-range key or row returns `None`.
    pub fn map_integer_value(&self, ci: usize, key: u32, row: u32) -> Option<i64> {
        self.columns.map_integers[ci].value(key, row)
    }

    /// Set an exact value in an existing document slot. Duplicate keys return an error.
    pub fn set_map_unsigned_integer(
        &mut self,
        ci: usize,
        doc_id: u32,
        key: &str,
        value: u64,
    ) -> io::Result<()> {
        self.columns
            .set_map_unsigned_integer(self.texts.len(), ci, doc_id, key, value)
    }

    /// Map keys in this store's ordinal order.
    pub fn map_unsigned_integer_keys(&self, ci: usize) -> &[String] {
        &self.columns.map_unsigned_integers[ci].keys
    }

    /// Find a key, including the empty string.
    pub fn map_unsigned_integer_key_ord(&self, ci: usize, key: &str) -> Option<u32> {
        self.columns.map_unsigned_integers[ci].key_ord(key)
    }

    /// Exact bounds, or `None` when the key has no entries.
    pub fn map_unsigned_integer_key_min_max(&self, ci: usize, key: u32) -> Option<(u64, u64)> {
        self.columns.map_unsigned_integers[ci].bounds(key)
    }

    /// Exact value; an absent entry or out-of-range key or row returns `None`.
    pub fn map_unsigned_integer_value(&self, ci: usize, key: u32, row: u32) -> Option<u64> {
        self.columns.map_unsigned_integers[ci].value(key, row)
    }

    /// The index of the map-facet column named `name`.
    pub fn map_facet_index(&self, name: &str) -> Option<usize> {
        self.column_index(ColumnKind::MapFacet, name)
    }

    /// The key ordinal of `key` in map-facet column `ci`, if any document carried it.
    pub fn map_facet_key_ord(&self, ci: usize, key: &str) -> Option<u32> {
        self.columns.map_facets[ci].key_index.get(key).copied()
    }

    /// The keys of map-facet column `ci`, in ordinal order.
    pub fn map_facet_keys(&self, ci: usize) -> &[String] {
        &self.columns.map_facets[ci].keys
    }

    /// The keys of map-numeric column `ci`, in ordinal order.
    pub fn map_numeric_keys(&self, ci: usize) -> &[String] {
        &self.columns.map_numerics[ci].keys
    }

    /// Number of distinct values map-facet column `ci` holds.
    pub fn map_facet_value_count(&self, ci: usize) -> usize {
        self.columns.map_facets[ci].values.len()
    }

    /// The value of map-facet column `ci` at ordinal `ord`.
    pub fn map_facet_value(&self, ci: usize, ord: u32) -> &str {
        &self.columns.map_facets[ci].values[ord as usize]
    }

    /// The ordinal of `value` in map-facet column `ci`'s value dictionary, `None` when absent.
    pub fn map_facet_value_ord_of(&self, ci: usize, value: &str) -> Option<u32> {
        self.columns.map_facets[ci].value_index.get(value).copied()
    }

    /// The value ordinal of `doc_id`'s entry under `key_ord` in map-facet column `ci`.
    pub fn map_facet_value_ord(&self, ci: usize, key_ord: u32, doc_id: u32) -> Option<u32> {
        self.columns.map_facets[ci].value_ord(doc_id as usize, key_ord)
    }

    /// Record `doc_id`'s map-facet entry.
    pub fn set_map_facet(&mut self, ci: usize, doc_id: u32, key: &str, value: &str) {
        self.columns.set_map_facet(ci, doc_id, key, value);
    }

    /// Number of `map<string, f64>` columns.
    pub fn map_numeric_count(&self) -> usize {
        self.column_count(ColumnKind::MapNumeric)
    }

    /// The name of map-numeric column `ci`. Panics when out of range.
    pub fn map_numeric_name(&self, ci: usize) -> &str {
        self.column_name(ColumnKind::MapNumeric, ci)
    }

    /// The index of the map-numeric column named `name`.
    pub fn map_numeric_index(&self, name: &str) -> Option<usize> {
        self.column_index(ColumnKind::MapNumeric, name)
    }

    /// The key ordinal of `key` in map-numeric column `ci`.
    pub fn map_numeric_key_ord(&self, ci: usize, key: &str) -> Option<u32> {
        self.columns.map_numerics[ci].key_index.get(key).copied()
    }

    /// `(min, max)` of map-numeric column `ci` under `key_ord`.
    pub fn map_numeric_key_min_max(&self, ci: usize, key_ord: u32) -> (f64, f64) {
        self.columns.map_numerics[ci].key_min_max()[key_ord as usize]
    }

    /// `doc_id`'s value under `key_ord` in map-numeric column `ci`.
    pub fn map_numeric_value(&self, ci: usize, key_ord: u32, doc_id: u32) -> Option<f64> {
        self.columns.map_numerics[ci].value(doc_id as usize, key_ord)
    }

    /// Record `doc_id`'s finite map-numeric entry.
    pub fn set_map_numeric(&mut self, ci: usize, doc_id: u32, key: &str, value: f64) {
        self.columns.set_map_numeric(ci, doc_id, key, value);
    }

    /// Number of exact `i64` map columns.
    pub fn map_integer_count(&self) -> usize {
        self.column_count(ColumnKind::MapInteger)
    }

    /// Column name at `ci`; panics for an invalid column index.
    pub fn map_integer_name(&self, ci: usize) -> &str {
        self.column_name(ColumnKind::MapInteger, ci)
    }

    /// Look up an exact `i64` map column by name.
    pub fn map_integer_index(&self, name: &str) -> Option<usize> {
        self.column_index(ColumnKind::MapInteger, name)
    }

    /// Number of exact `u64` map columns.
    pub fn map_unsigned_integer_count(&self) -> usize {
        self.column_count(ColumnKind::MapUnsignedInteger)
    }

    /// Column name at `ci`; panics for an invalid column index.
    pub fn map_unsigned_integer_name(&self, ci: usize) -> &str {
        self.column_name(ColumnKind::MapUnsignedInteger, ci)
    }

    /// Look up an exact `u64` map column by name.
    pub fn map_unsigned_integer_index(&self, name: &str) -> Option<usize> {
        self.column_index(ColumnKind::MapUnsignedInteger, name)
    }
}

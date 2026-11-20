//! Field-table and typed-column declarations and accessors on [`SpillBuilder`].

use std::io;

use super::SpillBuilder;
use crate::postings::columns::ColumnKind;
#[cfg(doc)]
use crate::postings::Bm25Store;

impl SpillBuilder {
    /// Declare a typed column table for `kind` before any documents are added.
    pub fn with_columns(mut self, kind: ColumnKind, names: &[&str]) -> Self {
        assert!(
            self.fields[0].doc_lengths.is_empty(),
            "{} must be declared before documents are added",
            kind.plural_label()
        );
        self.columns.configure(kind, names);
        self
    }

    /// Number of columns declared for `kind`.
    pub fn column_count(&self, kind: ColumnKind) -> usize {
        self.columns.count(kind)
    }

    /// Name of column `index` within `kind`. Panics when out of range.
    pub fn column_name(&self, kind: ColumnKind, index: usize) -> &str {
        self.columns.name(kind, index)
    }

    /// Index of column `name` within `kind`, if declared.
    pub fn column_index(&self, kind: ColumnKind, name: &str) -> Option<usize> {
        self.columns.index(kind, name)
    }

    /// Declare the facet field table, in facet-id order; same contract as
    /// [`Bm25Store::with_facets`].
    pub fn with_facet_fields(self, names: &[&str]) -> Self {
        self.with_columns(ColumnKind::Facet, names)
    }

    /// Declare which fields keep token positions; same contract as
    /// [`Bm25Store::with_positions`].
    pub fn with_position_fields(mut self, names: &[&str]) -> Self {
        assert!(
            self.fields[0].doc_lengths.is_empty(),
            "positional fields must be declared before documents are added"
        );
        for name in names {
            let fi = self
                .fields
                .iter()
                .position(|f| f.name == *name)
                .unwrap_or_else(|| panic!("positional field {name:?} is not in the field table"));
            self.fields[fi].positions = true;
        }
        self
    }

    /// Declare the fields that keep sentence spans per document,
    /// matching [`Bm25Store::with_sentences`].
    pub fn with_sentence_fields(mut self, names: &[&str]) -> Self {
        assert!(
            self.fields[0].doc_lengths.is_empty(),
            "sentence fields must be declared before documents are added"
        );
        for name in names {
            let fi = self
                .fields
                .iter()
                .position(|f| f.name == *name)
                .unwrap_or_else(|| panic!("sentence field {name:?} is not in the field table"));
            self.fields[fi].sentences = true;
        }
        self
    }

    /// Whether field `f` keeps sentence spans. Panics when out of range.
    pub fn field_has_sentences(&self, f: usize) -> bool {
        self.fields[f].sentences
    }

    /// Whether field `f` keeps token positions. Panics when out of range.
    pub fn field_has_positions(&self, f: usize) -> bool {
        self.fields[f].positions
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

    /// Record `doc_id`'s value for facet field `fi`.
    pub fn set_facet(&mut self, fi: usize, doc_id: u32, value: &str) {
        self.columns.set_facet(fi, doc_id, value);
    }

    /// Declare the numeric field table; same contract as [`Bm25Store::with_numerics`].
    pub fn with_numeric_fields(self, names: &[&str]) -> Self {
        self.with_columns(ColumnKind::Numeric, names)
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

    /// Record `doc_id`'s finite value for numeric field `ni`.
    pub fn set_numeric(&mut self, ni: usize, doc_id: u32, value: f64) {
        self.columns.set_numeric(ni, doc_id, value);
    }

    /// Declare the `i64` field table; same contract as [`Bm25Store::with_integers`].
    pub fn with_integer_fields(self, names: &[&str]) -> Self {
        self.with_columns(ColumnKind::Integer, names)
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

    /// Record `doc_id`'s value for integer field `ii`.
    pub fn set_integer(&mut self, ii: usize, doc_id: u32, value: i64) {
        self.columns.set_integer(ii, doc_id, value);
    }

    /// Declare the `u64` field table; same contract as [`Bm25Store::with_unsigned_integers`].
    pub fn with_unsigned_integer_fields(self, names: &[&str]) -> Self {
        self.with_columns(ColumnKind::UnsignedInteger, names)
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

    /// Record `doc_id`'s value for unsigned integer field `ii`.
    pub fn set_unsigned_integer(&mut self, ii: usize, doc_id: u32, value: u64) {
        self.columns.set_unsigned_integer(ii, doc_id, value);
    }

    /// Declare the geo-point column table; same contract as [`Bm25Store::with_geos`].
    pub fn with_geo_fields(self, names: &[&str]) -> Self {
        self.with_columns(ColumnKind::Geo, names)
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

    /// Record `doc_id`'s finite WGS-84 point for geo field `gi`.
    pub fn set_geo(&mut self, gi: usize, doc_id: u32, lat: f64, lon: f64) {
        self.columns.set_geo(gi, doc_id, lat, lon);
    }

    /// Declare the `map<string, string>` column table; same contract as
    /// [`Bm25Store::with_map_facets`].
    pub fn with_map_facet_fields(self, names: &[&str]) -> Self {
        self.with_columns(ColumnKind::MapFacet, names)
    }

    /// Declare the `map<string, f64>` column table; same contract as
    /// [`Bm25Store::with_map_numerics`].
    pub fn with_map_numeric_fields(self, names: &[&str]) -> Self {
        self.with_columns(ColumnKind::MapNumeric, names)
    }

    /// Configure exact `i64` map columns before adding documents.
    pub fn with_map_integer_fields(self, names: &[&str]) -> Self {
        self.with_columns(ColumnKind::MapInteger, names)
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
            .set_map_integer(self.fields[0].doc_lengths.len(), ci, doc_id, key, value)
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

    /// Map keys in this builder's ordinal order.
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

    /// Configure exact `u64` map columns before adding documents.
    pub fn with_map_unsigned_integer_fields(self, names: &[&str]) -> Self {
        self.with_columns(ColumnKind::MapUnsignedInteger, names)
    }

    /// Set an exact value in an existing document slot. Duplicate keys return an error.
    pub fn set_map_unsigned_integer(
        &mut self,
        ci: usize,
        doc_id: u32,
        key: &str,
        value: u64,
    ) -> io::Result<()> {
        self.columns.set_map_unsigned_integer(
            self.fields[0].doc_lengths.len(),
            ci,
            doc_id,
            key,
            value,
        )
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

    /// Map keys in this builder's ordinal order.
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

    /// The index of the map-numeric column named `name`.
    pub fn map_numeric_index(&self, name: &str) -> Option<usize> {
        self.column_index(ColumnKind::MapNumeric, name)
    }

    /// Record `doc_id`'s map-facet entry.
    pub fn set_map_facet(&mut self, ci: usize, doc_id: u32, key: &str, value: &str) {
        self.columns.set_map_facet(ci, doc_id, key, value);
    }

    /// Record `doc_id`'s finite map-numeric entry.
    pub fn set_map_numeric(&mut self, ci: usize, doc_id: u32, key: &str, value: f64) {
        self.columns.set_map_numeric(ci, doc_id, key, value);
    }
}

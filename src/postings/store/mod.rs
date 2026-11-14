//! Heap-resident BM25 postings and column builder (`Bm25Store`).

mod loader;
mod view;
mod writer;

pub use self::view::StoreFieldView;

use std::io;

use super::columns::{
    merge_analysis_fingerprint, validate_text_field_names, ColumnKind, ColumnSet, StoredBinding,
    StoredDerived,
};
#[cfg(doc)]
use super::Bm25Index;
use super::{AnalyzedDoc, DocLineage, Posting};

/// One field's slice of the store: its own postings, per-document
/// lengths, and running total. Structurally a complete single-field
/// BM25 index over the shared slot space (`docs/multi-field.md`).
#[derive(Debug)]
pub(in crate::postings) struct FieldStore {
    /// Field name from the schema ("body" for field 0).
    pub(in crate::postings) name: String,
    /// Hash of the field's `AnalysisSpec`, persisted in the v6 field
    /// table. 0 until the ingest layer wires real fingerprints.
    analysis_fingerprint: u64,
    /// term → postings, kept ascending by doc id (append-only), in a
    /// map ordered by term bytes: the heap store's dictionary is the same
    /// byte-sorted order the on-disk directory has, so prefix expansion is
    /// a range walk here and a binary search there (`docs/prefix-terms.md`).
    pub(in crate::postings) postings: std::collections::BTreeMap<String, Vec<Posting>>,
    /// Per-document length in terms, indexed by local doc id. Sparse
    /// slots (ids consumed by the vector side) hold 0. Every field's
    /// table has the same length (the shared slot count).
    pub(in crate::postings) doc_lengths: Vec<u32>,
    /// Sum of this field's document lengths (for avgdl).
    pub(in crate::postings) total_length: u64,
    /// Whether this field keeps token positions per occurrence
    /// (`docs/phrase-proximity.md`). Declared at construction (or read
    /// from the file's kind-7 entry); a positional field refuses a
    /// document without positions instead of storing a hole.
    positions: bool,
    /// Whether at least one document in this positional field carried
    /// token-lattice arc lengths (`COLUMN_KIND_POS_LENGTHS`).
    has_lattice: bool,
    /// Whether this field keeps sentence spans per document
    /// (`docs/highlighting.md`), declared at construction or read from
    /// the file's kind-8 entry.
    sentences: bool,
    /// Per-document sentence spans, indexed by local doc id, populated
    /// only when `sentences` is set (sparse slots hold an empty table).
    doc_sentences: Vec<Vec<(u32, u32)>>,
    /// Optional calibrated quantized SPLADE inverted index (`COLUMN_KIND_SPLADE`).
    splade: Option<crate::turbo_bridge::splade::SpladeFieldIndex>,
}

impl FieldStore {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            analysis_fingerprint: 0,
            postings: std::collections::BTreeMap::new(),
            doc_lengths: Vec::new(),
            total_length: 0,
            positions: false,
            has_lattice: false,
            sentences: false,
            doc_sentences: Vec::new(),
            splade: None,
        }
    }
}

/// The shard's lexical half: per-field postings and corpus stats over a
/// shared slot space, plus raw texts and typed per-document columns.
#[derive(Debug)]
pub struct Bm25Store {
    /// Per-field indexes; field 0 is the body (the stored text). The
    /// single-field public surface reads field 0.
    pub(in crate::postings) fields: Vec<FieldStore>,
    /// Raw texts indexed by local doc id; sparse slots hold `None`.
    pub(in crate::postings) texts: Vec<Option<String>>,
    /// Per-document lineage, parallel to `texts` (`None` when the
    /// document was ingested without lineage).
    pub(in crate::postings) lineages: Vec<Option<DocLineage>>,
    /// Per-document typed column tables and inline binding/derived metadata.
    pub(in crate::postings) columns: ColumnSet,
    sources: crate::source_archive::SourceArchive,
}

impl Default for Bm25Store {
    fn default() -> Self {
        Self {
            fields: vec![FieldStore::new("body")],
            texts: Vec::new(),
            lineages: Vec::new(),
            columns: ColumnSet::default(),
            sources: Default::default(),
        }
    }
}

impl Bm25Store {
    /// Visit stored rows for an exact key; does not apply visibility or grants.
    pub fn visit_document_rows(
        &self,
        key: &[u8],
        visitor: &mut dyn FnMut(u32, u64, Option<u32>) -> bool,
    ) -> bool {
        self.sources.visit_document_rows(key, visitor)
    }

    pub fn document_identity(&self, row: u32) -> Option<crate::pb::DocumentIdentity> {
        self.sources.identity(row)
    }

    pub fn identity_snapshot(&self) -> crate::source_archive::IdentitySnapshot {
        self.sources.identity_snapshot()
    }

    pub fn source_archive_mut(&mut self) -> &mut crate::source_archive::SourceArchive {
        &mut self.sources
    }

    pub fn protobuf_source(
        &self,
        row: u32,
    ) -> io::Result<Option<(crate::pb::ProtobufSource, Option<u32>)>> {
        self.sources.row(row)
    }

    /// The mapped-plan binding this store was written under, if any.
    pub fn binding(&self) -> Option<&StoredBinding> {
        self.columns.binding.as_ref()
    }

    /// Set the binding persisted by the next save. The caller (the
    /// shard's flush path) owns the no-contradiction rule; this is
    /// plain storage.
    pub fn set_binding(&mut self, binding: Option<StoredBinding>) {
        self.columns.binding = binding;
    }

    /// The derived-column declaration this store was written under.
    pub fn derived(&self) -> Option<&StoredDerived> {
        self.columns.derived.as_ref()
    }

    /// Set the declaration persisted by the next save. Plain storage,
    /// like [`Self::set_binding`].
    pub fn set_derived(&mut self, derived: Option<StoredDerived>) {
        self.columns.derived = derived;
    }

    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// An empty store with the given field table, in field-id order.
    /// Field 0 is the body (the stored text); `new()` is
    /// `with_fields(&["body"])`. Names must be non-empty and unique
    /// (the v6 reader validates the same on open).
    pub fn with_fields(names: &[&str]) -> Self {
        validate_text_field_names(names, "store");
        Self {
            fields: names.iter().map(|n| FieldStore::new(n)).collect(),
            texts: Vec::new(),
            lineages: Vec::new(),
            columns: ColumnSet::default(),
            sources: Default::default(),
        }
    }

    /// Unified text-field table constructor matching [`Self::with_fields`].
    pub fn with_text_fields(names: &[&str]) -> Self {
        Self::with_fields(names)
    }

    /// Declare a typed column table for `kind` before any documents are added.
    pub fn with_columns(mut self, kind: ColumnKind, names: &[&str]) -> Self {
        assert!(
            self.texts.is_empty(),
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

    /// Declare which fields keep token positions (`docs/phrase-proximity.md`).
    /// Must precede any document, and every name must be in the field table.
    pub fn with_positions(mut self, names: &[&str]) -> Self {
        assert!(
            self.texts.is_empty(),
            "positional fields must be declared before documents are added"
        );
        for name in names {
            let fi = self
                .field_index(name)
                .unwrap_or_else(|| panic!("positional field {name:?} is not in the field table"));
            self.fields[fi].positions = true;
        }
        self
    }

    /// Whether field `f` keeps token positions. Panics when out of range.
    pub fn field_has_positions(&self, f: usize) -> bool {
        self.fields[f].positions
    }

    /// Names of the fields that keep token positions, field-id order.
    pub fn positional_fields(&self) -> Vec<&str> {
        self.fields
            .iter()
            .filter(|f| f.positions)
            .map(|f| f.name.as_str())
            .collect()
    }

    /// Declare the fields that keep sentence spans per document
    /// (`docs/highlighting.md`). Must precede any document, and every
    /// name must be in the field table.
    pub fn with_sentences(mut self, names: &[&str]) -> Self {
        assert!(
            self.texts.is_empty(),
            "sentence fields must be declared before documents are added"
        );
        for name in names {
            let fi = self
                .field_index(name)
                .unwrap_or_else(|| panic!("sentence field {name:?} is not in the field table"));
            self.fields[fi].sentences = true;
        }
        self
    }

    /// Whether field `f` keeps sentence spans. Panics when out of range.
    pub fn field_has_sentences(&self, f: usize) -> bool {
        self.fields[f].sentences
    }

    /// Names of the fields that keep sentence spans, field-id order.
    pub fn sentence_fields(&self) -> Vec<&str> {
        self.fields
            .iter()
            .filter(|f| f.sentences)
            .map(|f| f.name.as_str())
            .collect()
    }

    /// Field `f`'s sentence spans for `doc_id`: `None` when the field
    /// keeps none, an empty table for a slot without a document.
    pub fn field_doc_sentences(&self, f: usize, doc_id: u32) -> Option<&[(u32, u32)]> {
        let field = &self.fields[f];
        if !field.sentences {
            return None;
        }
        Some(
            field
                .doc_sentences
                .get(doc_id as usize)
                .map_or(&[][..], Vec::as_slice),
        )
    }

    /// Declare the facet field table, in facet-id order.
    pub fn with_facets(self, names: &[&str]) -> Self {
        self.with_columns(ColumnKind::Facet, names)
    }

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

    /// Declare the numeric field table, in numeric-id order.
    pub fn with_numerics(self, names: &[&str]) -> Self {
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

    /// Declare the `i64` field table, in integer-id order.
    pub fn with_integers(self, names: &[&str]) -> Self {
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

    /// Declare the unsigned `u64` column table before adding documents.
    pub fn with_unsigned_integers(self, names: &[&str]) -> Self {
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

    /// Declare the geo-point column table, in geo-id order.
    pub fn with_geos(self, names: &[&str]) -> Self {
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

    /// Declare the `map<string, string>` column table.
    pub fn with_map_facets(self, names: &[&str]) -> Self {
        self.with_columns(ColumnKind::MapFacet, names)
    }

    /// Declare the `map<string, f64>` column table.
    pub fn with_map_numerics(self, names: &[&str]) -> Self {
        self.with_columns(ColumnKind::MapNumeric, names)
    }

    /// Number of `map<string, string>` columns.
    pub fn map_facet_count(&self) -> usize {
        self.column_count(ColumnKind::MapFacet)
    }

    /// The name of map-facet column `ci`. Panics when out of range.
    pub fn map_facet_name(&self, ci: usize) -> &str {
        self.column_name(ColumnKind::MapFacet, ci)
    }

    /// Configure exact `i64` map columns before adding documents.
    pub fn with_map_integers(self, names: &[&str]) -> Self {
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
            .set_map_integer(self.texts.len(), ci, doc_id, key, value)
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

    /// Configure exact `u64` map columns before adding documents.
    pub fn with_map_unsigned_integers(self, names: &[&str]) -> Self {
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
        self.columns
            .set_map_unsigned_integer(self.texts.len(), ci, doc_id, key, value)
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

    /// The name of field `f`. Panics when out of range.
    pub fn field_name(&self, f: usize) -> &str {
        &self.fields[f].name
    }

    /// Record field `f`'s analyzer fingerprint, or refuse if it
    /// contradicts what the field already holds.
    pub fn set_analysis_fingerprint(&mut self, f: usize, fingerprint: u64) -> Result<(), String> {
        let field = &mut self.fields[f];
        merge_analysis_fingerprint(&mut field.analysis_fingerprint, &field.name, fingerprint)
    }

    /// Field `f`'s analyzer fingerprint (0 = unknown).
    pub fn analysis_fingerprint(&self, f: usize) -> u64 {
        self.fields[f].analysis_fingerprint
    }

    /// The index of the field named `name`, if the table has it.
    pub fn field_index(&self, name: &str) -> Option<usize> {
        self.fields.iter().position(|f| f.name == name)
    }

    /// Field `f` as its own [`Bm25Index`]: per-field postings, lengths,
    /// and total; shared texts, lineages, and doc count. Panics when `f`
    /// is out of range.
    pub fn field(&self, f: usize) -> StoreFieldView<'_> {
        StoreFieldView {
            store: self,
            field: &self.fields[f],
        }
    }

    /// The number of document slots ever allocated (the next local doc id).
    pub fn next_doc_id(&self) -> u32 {
        self.texts.len() as u32
    }

    /// Number of documents with postings (in any field).
    pub fn doc_count(&self) -> u64 {
        (0..self.texts.len())
            .filter(|&slot| self.fields.iter().any(|f| f.doc_lengths[slot] > 0))
            .count() as u64
    }

    /// Sum of all body document lengths (BM25 avgdl numerator).
    pub fn total_doc_length(&self) -> u64 {
        self.fields[0].total_length
    }

    /// Body postings for `term`, if present.
    pub fn postings(&self, term: &str) -> Option<&[Posting]> {
        self.fields[0].postings.get(term).map(Vec::as_slice)
    }

    /// Body document length in terms (0 for unknown/sparse slots).
    pub fn doc_length(&self, doc_id: u32) -> u32 {
        self.fields[0]
            .doc_lengths
            .get(doc_id as usize)
            .copied()
            .unwrap_or(0)
    }

    /// The raw text of a document, if stored.
    pub fn text(&self, doc_id: u32) -> Option<&str> {
        self.texts.get(doc_id as usize).and_then(|t| t.as_deref())
    }

    /// The lineage of a document, if it was ingested with one.
    pub fn lineage(&self, doc_id: u32) -> Option<DocLineage> {
        self.lineages.get(doc_id as usize).copied().flatten()
    }

    /// Append one analyzed document with the given local doc id.
    ///
    /// `doc_id` must be `>= next_doc_id()` (append-only); ids above the
    /// current tip create sparse slots, which is how the vector and
    /// document sides share one positional id space.
    pub fn add_document(&mut self, doc_id: u32, text: String, doc: AnalyzedDoc) {
        self.add_document_with_lineage(doc_id, text, doc, None);
    }

    /// Like [`Self::add_document`], with corpus lineage attached.
    pub fn add_document_with_lineage(
        &mut self,
        doc_id: u32,
        text: String,
        doc: AnalyzedDoc,
        lineage: Option<DocLineage>,
    ) {
        let slot = doc_id as usize;
        assert!(slot >= self.texts.len(), "doc id {doc_id} already used");
        assert!(
            doc.fields.len() <= self.fields.len(),
            "document carries {} fields, store has {}",
            doc.fields.len(),
            self.fields.len()
        );
        for field in &mut self.fields {
            field.doc_lengths.resize(slot + 1, 0);
            if field.sentences {
                field.doc_sentences.resize_with(slot + 1, Vec::new);
            }
        }
        self.texts.resize_with(slot + 1, || None);
        self.lineages.resize_with(slot + 1, || None);
        self.texts[slot] = Some(text);
        self.lineages[slot] = lineage;
        for (fi, analyzed) in doc.fields.into_iter().enumerate() {
            let field = &mut self.fields[fi];
            field.doc_lengths[slot] = analyzed.length;
            field.total_length += u64::from(analyzed.length);
            if field.sentences {
                field.doc_sentences[slot] = analyzed.sentences.unwrap_or_else(|| {
                    panic!(
                        "field {:?} keeps sentence spans but the document carried none",
                        field.name
                    )
                });
            }
            let positions = if field.positions {
                Some(analyzed.positions.unwrap_or_else(|| {
                    panic!(
                        "field {:?} keeps positions but the document carried none",
                        field.name
                    )
                }))
            } else {
                None
            };
            let pos_lengths = if field.positions {
                if analyzed.pos_lengths.is_some() {
                    field.has_lattice = true;
                }
                analyzed.pos_lengths
            } else {
                None
            };
            if !analyzed.splade.is_empty() {
                field
                    .splade
                    .get_or_insert_with(Default::default)
                    .insert_doc(doc_id, analyzed.splade_scale_bits, &analyzed.splade);
            }
            for (ti, (term, tf, offsets)) in analyzed.terms.into_iter().enumerate() {
                let positions = match &positions {
                    Some(table) => {
                        assert_eq!(
                            table[ti].len(),
                            offsets.len(),
                            "positions must be parallel to offsets"
                        );
                        table[ti].clone()
                    }
                    None => Vec::new(),
                };
                let pos_lengths = match &pos_lengths {
                    Some(table) => {
                        assert_eq!(
                            table[ti].len(),
                            offsets.len(),
                            "pos_lengths must be parallel to offsets"
                        );
                        table[ti].clone()
                    }
                    None => Vec::new(),
                };
                field.postings.entry(term).or_default().push(Posting {
                    doc_id,
                    tf,
                    offsets,
                    positions,
                    pos_lengths,
                });
            }
        }
    }
}

/// A part still being written is read forward.
impl crate::filter_bits::PartNumbers for Bm25Store {
    fn int_value(&self, column: usize, row: u32) -> Option<i64> {
        (column < self.integer_count())
            .then(|| self.integer_value(column, row))
            .flatten()
    }
    fn uint_value(&self, column: usize, row: u32) -> Option<u64> {
        (column < self.unsigned_integer_count())
            .then(|| self.unsigned_integer_value(column, row))
            .flatten()
    }
    fn f64_value(&self, column: usize, row: u32) -> Option<f64> {
        (column < self.numeric_count())
            .then(|| self.numeric_value(column, row))
            .flatten()
    }
}

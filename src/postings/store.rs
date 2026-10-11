use super::*;

use std::io::{self, Write};
use std::path::{Path, PathBuf};

/// The shard's lexical half: per-field postings and corpus stats over a
/// shared slot space, plus the raw texts.
#[derive(Debug)]
pub struct Bm25Store {
    /// Per-field indexes; field 0 is the body (the stored text). The
    /// single-field public surface reads field 0.
    pub(crate) fields: Vec<FieldStore>,
    /// Raw texts indexed by local doc id; sparse slots hold `None`.
    pub(crate) texts: Vec<Option<String>>,
    /// Per-document lineage, parallel to `texts` (`None` when the
    /// document was ingested without lineage).
    pub(crate) lineages: Vec<Option<DocLineage>>,
    /// Facet columns in facet-id order.
    pub(crate) facets: Vec<FacetStore>,
    /// Numeric columns in numeric-id order.
    pub(crate) numerics: Vec<NumericStore>,
    /// map<string, string> columns in map-facet-id order.
    pub(crate) map_facets: Vec<MapFacetStore>,
    /// map<string, f64> columns in map-numeric-id order.
    pub(crate) map_numerics: Vec<MapNumericStore>,
    pub(crate) map_integers: Vec<crate::integer_map::Store<i64>>,
    pub(crate) map_unsigned_integers: Vec<crate::integer_map::Store<u64>>,
    /// i64 columns in integer-id order.
    pub(crate) integers: Vec<IntStore>,
    pub(crate) unsigned_integers: Vec<UintStore>,
    /// Geo-point columns in geo-id order (`docs/geo-columns.md`). A
    /// store with no columns of any kind persists as v6.
    pub(crate) geos: Vec<GeoStore>,
    /// The mapped-plan binding, persisted as the kind-6 table entry.
    /// `Some` forces v7 even with no columns.
    pub(crate) binding: Option<StoredBinding>,
    /// The derived-column declaration, persisted as the kind-15 entry.
    /// `Some` forces v7 like the binding.
    pub(crate) derived: Option<StoredDerived>,
    pub(crate) sources: crate::source_archive::SourceArchive,
}

impl Default for Bm25Store {
    fn default() -> Self {
        Self {
            fields: vec![FieldStore::new("body")],
            texts: Vec::new(),
            lineages: Vec::new(),
            facets: Vec::new(),
            numerics: Vec::new(),
            map_facets: Vec::new(),
            map_numerics: Vec::new(),
            map_integers: Vec::new(),
            map_unsigned_integers: Vec::new(),
            integers: Vec::new(),
            unsigned_integers: Vec::new(),
            geos: Vec::new(),
            binding: None,
            derived: None,
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
        self.binding.as_ref()
    }

    /// Set the binding persisted by the next save. The caller (the
    /// shard's flush path) owns the no-contradiction rule; this is
    /// plain storage.
    pub fn set_binding(&mut self, binding: Option<StoredBinding>) {
        self.binding = binding;
    }

    /// The derived-column declaration this store was written under.
    pub fn derived(&self) -> Option<&StoredDerived> {
        self.derived.as_ref()
    }

    /// Set the declaration persisted by the next save. Plain storage,
    /// like [`Self::set_binding`].
    pub fn set_derived(&mut self, derived: Option<StoredDerived>) {
        self.derived = derived;
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
        assert!(!names.is_empty(), "a store needs at least one field");
        for (i, name) in names.iter().enumerate() {
            assert!(!name.is_empty(), "field {i} has an empty name");
            assert!(
                u16::try_from(name.len()).is_ok(),
                "field name exceeds u16 length"
            );
            assert!(!names[..i].contains(name), "duplicate field name {name:?}");
        }
        Self {
            fields: names.iter().map(|n| FieldStore::new(n)).collect(),
            texts: Vec::new(),
            lineages: Vec::new(),
            facets: Vec::new(),
            numerics: Vec::new(),
            map_facets: Vec::new(),
            map_numerics: Vec::new(),
            map_integers: Vec::new(),
            map_unsigned_integers: Vec::new(),
            integers: Vec::new(),
            unsigned_integers: Vec::new(),
            geos: Vec::new(),
            binding: None,
            derived: None,
            sources: Default::default(),
        }
    }

    /// Declare which fields keep token positions
    /// (`docs/phrase-proximity.md`). Must precede any document: a field
    /// that changes its mind mid-corpus would hold a phrase index over
    /// half its documents. Every name must be in the field table, and a
    /// positional store persists as v7 (the kind-7 entry lives in the
    /// column table). Panics on either violation, like the other
    /// declaration builders.
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
    /// name must be in the field table; a sentence store persists as v7
    /// (the kind-8 entry lives in the column table). Panics on either
    /// violation, like the other declaration builders.
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

    /// Declare the facet field table, in facet-id order (builder style:
    /// `Bm25Store::with_fields(&["body"]).with_facets(&["court"])`).
    /// Must be called before any document is added. A store with facet
    /// fields persists as v7; without, as v6.
    pub fn with_facets(mut self, names: &[&str]) -> Self {
        assert!(
            self.texts.is_empty(),
            "facet fields must be declared before documents are added"
        );
        validate_facet_names(names);
        self.facets = names.iter().map(|n| FacetStore::new(n)).collect();
        self
    }

    /// Number of fields in the field table.
    pub fn field_count(&self) -> usize {
        self.fields.len()
    }

    /// Number of facet fields in the facet table.
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

    /// The ordinal of `doc_id`'s value for facet field `fi`, `None`
    /// when the document has no value.
    pub fn facet_ord(&self, fi: usize, doc_id: u32) -> Option<u32> {
        self.facets[fi].ord(doc_id as usize)
    }

    /// The ordinal of `value` in facet field `fi`'s dictionary, `None`
    /// when this store never ingested it — the value then matches none
    /// of its documents, which is the exact answer, not a refusal
    /// (`docs/cel-filters.md`: the typo rule guards structure, not
    /// data).
    pub fn facet_value_ord_of(&self, fi: usize, value: &str) -> Option<u32> {
        self.facets[fi].index.get(value).copied()
    }

    /// Facet field `fi`'s dictionary in ordinal (first-seen) order.
    pub fn facet_dictionary(&self, fi: usize) -> &[String] {
        &self.facets[fi].dict
    }

    /// Whether facet field `fi`'s first-seen dictionary happens to be in
    /// byte order (it is after a reload of a file this writer produced).
    pub fn facet_dictionary_sorted(&self, fi: usize) -> bool {
        dictionary_sorted(&self.facets[fi].dict)
    }

    /// Map-facet column `ci`'s value dictionary in ordinal order.
    pub fn map_facet_values(&self, ci: usize) -> &[String] {
        &self.map_facets[ci].values
    }

    /// Whether map-facet column `ci`'s value dictionary is in byte order.
    pub fn map_facet_values_sorted(&self, ci: usize) -> bool {
        dictionary_sorted(&self.map_facets[ci].values)
    }

    /// Record `doc_id`'s value for facet field `fi`, interning it in
    /// the dictionary; see [`FacetStore::set`] for the contract.
    pub fn set_facet(&mut self, fi: usize, doc_id: u32, value: &str) {
        self.facets[fi].set(doc_id, value);
    }

    /// Declare the numeric field table, in numeric-id order (builder
    /// style, like [`Self::with_facets`]). Must be called before any
    /// document is added; a store with columns persists as v7.
    pub fn with_numerics(mut self, names: &[&str]) -> Self {
        assert!(
            self.texts.is_empty(),
            "numeric fields must be declared before documents are added"
        );
        validate_facet_names(names);
        self.numerics = names.iter().map(|n| NumericStore::new(n)).collect();
        self
    }

    /// Number of numeric fields in the numeric table.
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

    /// `doc_id`'s value for numeric field `ni`, `None` when absent.
    pub fn numeric_value(&self, ni: usize, doc_id: u32) -> Option<f64> {
        self.numerics[ni].value(doc_id as usize)
    }

    /// (min, max) of numeric field `ni` over present values; (NaN, NaN)
    /// when no document has one.
    pub fn numeric_min_max(&self, ni: usize) -> (f64, f64) {
        self.numerics[ni].min_max()
    }

    /// Record `doc_id`'s value for numeric field `ni`; see
    /// [`NumericStore::set`] for the contract (finite values only).
    pub fn set_numeric(&mut self, ni: usize, doc_id: u32, value: f64) {
        self.numerics[ni].set(doc_id, value);
    }

    /// Declare the i64 field table, in integer-id order (builder
    /// style, like [`Self::with_numerics`]). Must be called before any
    /// document is added; a store with columns persists as v7.
    pub fn with_integers(mut self, names: &[&str]) -> Self {
        assert!(
            self.texts.is_empty(),
            "integer fields must be declared before documents are added"
        );
        validate_facet_names(names);
        self.integers = names.iter().map(|n| IntStore::new(n)).collect();
        self
    }

    /// Number of i64 fields in the integer table.
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

    /// `doc_id`'s value for integer field `ii`, `None` when absent.
    pub fn integer_value(&self, ii: usize, doc_id: u32) -> Option<i64> {
        self.integers[ii].value(doc_id as usize)
    }

    /// (min, max) of integer field `ii` over present values; the empty
    /// range `(i64::MAX, i64::MIN)` when no document has one (see
    /// [`IntStore::min_max`]).
    pub fn integer_min_max(&self, ii: usize) -> (i64, i64) {
        self.integers[ii].min_max()
    }

    /// Record `doc_id`'s value for integer field `ii`; see
    /// [`IntStore::set`] for the contract.
    pub fn set_integer(&mut self, ii: usize, doc_id: u32, value: i64) {
        self.integers[ii].set(doc_id, value);
    }

    /// Declare the unsigned column table before adding documents. The v8
    /// writer records kind 11 columns with explicit presence.
    pub fn with_unsigned_integers(mut self, names: &[&str]) -> Self {
        assert!(
            self.texts.is_empty(),
            "integer fields must be declared before documents are added"
        );
        validate_facet_names(names);
        self.unsigned_integers = names.iter().map(|n| UintStore::new(n)).collect();
        self
    }

    /// Number of u64 fields in the integer table.
    pub fn unsigned_integer_count(&self) -> usize {
        self.unsigned_integers.len()
    }

    /// The name of integer field `ii`. Panics when out of range.
    pub fn unsigned_integer_name(&self, ii: usize) -> &str {
        &self.unsigned_integers[ii].name
    }

    /// The index of the integer field named `name`, if the table has it.
    pub fn unsigned_integer_index(&self, name: &str) -> Option<usize> {
        self.unsigned_integers.iter().position(|n| n.name == name)
    }

    /// `doc_id`'s value for integer field `ii`, `None` when absent.
    pub fn unsigned_integer_value(&self, ii: usize, doc_id: u32) -> Option<u64> {
        self.unsigned_integers[ii].value(doc_id as usize)
    }

    /// (min, max) of integer field `ii` over present values; the empty
    /// range `(u64::MAX, u64::MIN)` when no document has one (see
    /// [`UintStore::min_max`]).
    pub fn unsigned_integer_min_max(&self, ii: usize) -> (u64, u64) {
        self.unsigned_integers[ii].min_max()
    }

    /// Record `doc_id`'s value for integer field `ii`; see
    /// [`UintStore::set`] for the contract.
    pub fn set_unsigned_integer(&mut self, ii: usize, doc_id: u32, value: u64) {
        self.unsigned_integers[ii].set(doc_id, value);
    }

    /// Declare the geo-point column table, in geo-id order (builder
    /// style, like [`Self::with_numerics`]). Must be called before any
    /// document is added; a store with columns persists as v7.
    pub fn with_geos(mut self, names: &[&str]) -> Self {
        assert!(
            self.texts.is_empty(),
            "geo fields must be declared before documents are added"
        );
        validate_facet_names(names);
        self.geos = names.iter().map(|n| GeoStore::new(n)).collect();
        self
    }

    /// Number of geo fields in the geo table.
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

    /// `doc_id`'s (lat, lon) for geo field `gi`, `None` when absent.
    pub fn geo_value(&self, gi: usize, doc_id: u32) -> Option<(f64, f64)> {
        self.geos[gi].value(doc_id as usize)
    }

    /// Geo field `gi`'s bounding box `(min_lat, max_lat, min_lon,
    /// max_lon)` over present points; all four NaN when there are none.
    pub fn geo_bbox(&self, gi: usize) -> (f64, f64, f64, f64) {
        self.geos[gi].bbox()
    }

    /// Record `doc_id`'s point for geo field `gi`; see [`GeoStore::set`]
    /// for the contract (finite degrees on the globe, one per document).
    pub fn set_geo(&mut self, gi: usize, doc_id: u32, lat: f64, lon: f64) {
        self.geos[gi].set(doc_id, lat, lon);
    }

    /// Declare the map<string, string> column table (builder style,
    /// like [`Self::with_facets`]); columns make the store persist v7.
    pub fn with_map_facets(mut self, names: &[&str]) -> Self {
        assert!(
            self.texts.is_empty(),
            "map columns must be declared before documents are added"
        );
        validate_facet_names(names);
        self.map_facets = names.iter().map(|n| MapFacetStore::new(n)).collect();
        self
    }

    /// Declare the map<string, f64> column table (builder style).
    pub fn with_map_numerics(mut self, names: &[&str]) -> Self {
        assert!(
            self.texts.is_empty(),
            "map columns must be declared before documents are added"
        );
        validate_facet_names(names);
        self.map_numerics = names.iter().map(|n| MapNumericStore::new(n)).collect();
        self
    }

    /// Number of map<string, string> columns.
    pub fn map_facet_count(&self) -> usize {
        self.map_facets.len()
    }

    /// The name of map-facet column `ci`. Panics when out of range.
    pub fn map_facet_name(&self, ci: usize) -> &str {
        &self.map_facets[ci].name
    }

    /// Configure exact i64 map columns before adding documents.
    pub fn with_map_integers(mut self, names: &[&str]) -> Self {
        assert!(
            self.fields[0].doc_lengths.is_empty(),
            "map columns must be configured before documents"
        );
        validate_facet_names(names);
        self.map_integers = names
            .iter()
            .map(|name| crate::integer_map::Store::new(name))
            .collect();
        self
    }
    /// Set an exact value in an existing document slot. Duplicate keys return an error.
    pub fn set_map_integer(
        &mut self,
        ci: usize,
        doc_id: u32,
        key: &str,
        value: i64,
    ) -> io::Result<()> {
        if doc_id as usize >= self.fields[0].doc_lengths.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "integer map document slot does not exist",
            ));
        }
        self.map_integers
            .get_mut(ci)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "integer map column does not exist",
                )
            })?
            .set(doc_id, key, value)
    }
    /// Number of exact i64 map columns.
    pub fn map_integer_count(&self) -> usize {
        self.map_integers.len()
    }
    /// Column name at `ci`; panics for an invalid column index.
    pub fn map_integer_name(&self, ci: usize) -> &str {
        &self.map_integers[ci].name
    }
    /// Look up an exact i64 map column by name.
    pub fn map_integer_index(&self, name: &str) -> Option<usize> {
        self.map_integers.iter().position(|c| c.name == name)
    }
    /// Map keys in this reader's ordinal order.
    pub fn map_integer_keys(&self, ci: usize) -> &[String] {
        &self.map_integers[ci].keys
    }
    /// Find a key, including the empty string.
    pub fn map_integer_key_ord(&self, ci: usize, key: &str) -> Option<u32> {
        self.map_integers[ci].key_ord(key)
    }
    /// Exact bounds, or `None` when the key has no entries.
    pub fn map_integer_key_min_max(&self, ci: usize, key: u32) -> Option<(i64, i64)> {
        self.map_integers[ci].bounds(key)
    }
    /// Exact value; an absent entry or out-of-range key or row returns `None`.
    pub fn map_integer_value(&self, ci: usize, key: u32, row: u32) -> Option<i64> {
        self.map_integers[ci].value(key, row)
    }

    /// Configure exact u64 map columns before adding documents.
    pub fn with_map_unsigned_integers(mut self, names: &[&str]) -> Self {
        assert!(
            self.fields[0].doc_lengths.is_empty(),
            "map columns must be configured before documents"
        );
        validate_facet_names(names);
        self.map_unsigned_integers = names
            .iter()
            .map(|name| crate::integer_map::Store::new(name))
            .collect();
        self
    }
    /// Set an exact value in an existing document slot. Duplicate keys return an error.
    pub fn set_map_unsigned_integer(
        &mut self,
        ci: usize,
        doc_id: u32,
        key: &str,
        value: u64,
    ) -> io::Result<()> {
        if doc_id as usize >= self.fields[0].doc_lengths.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "integer map document slot does not exist",
            ));
        }
        self.map_unsigned_integers
            .get_mut(ci)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "integer map column does not exist",
                )
            })?
            .set(doc_id, key, value)
    }
    /// Number of exact u64 map columns.
    pub fn map_unsigned_integer_count(&self) -> usize {
        self.map_unsigned_integers.len()
    }
    /// Column name at `ci`; panics for an invalid column index.
    pub fn map_unsigned_integer_name(&self, ci: usize) -> &str {
        &self.map_unsigned_integers[ci].name
    }
    /// Look up an exact u64 map column by name.
    pub fn map_unsigned_integer_index(&self, name: &str) -> Option<usize> {
        self.map_unsigned_integers
            .iter()
            .position(|c| c.name == name)
    }
    /// Map keys in this reader's ordinal order.
    pub fn map_unsigned_integer_keys(&self, ci: usize) -> &[String] {
        &self.map_unsigned_integers[ci].keys
    }
    /// Find a key, including the empty string.
    pub fn map_unsigned_integer_key_ord(&self, ci: usize, key: &str) -> Option<u32> {
        self.map_unsigned_integers[ci].key_ord(key)
    }
    /// Exact bounds, or `None` when the key has no entries.
    pub fn map_unsigned_integer_key_min_max(&self, ci: usize, key: u32) -> Option<(u64, u64)> {
        self.map_unsigned_integers[ci].bounds(key)
    }
    /// Exact value; an absent entry or out-of-range key or row returns `None`.
    pub fn map_unsigned_integer_value(&self, ci: usize, key: u32, row: u32) -> Option<u64> {
        self.map_unsigned_integers[ci].value(key, row)
    }

    /// The index of the map-facet column named `name`.
    pub fn map_facet_index(&self, name: &str) -> Option<usize> {
        self.map_facets.iter().position(|c| c.name == name)
    }

    /// The key ordinal of `key` in map-facet column `ci`, if any
    /// document ever carried it.
    pub fn map_facet_key_ord(&self, ci: usize, key: &str) -> Option<u32> {
        self.map_facets[ci].key_index.get(key).copied()
    }

    /// The keys of map-facet column `ci`, in ordinal order.
    pub fn map_facet_keys(&self, ci: usize) -> &[String] {
        &self.map_facets[ci].keys
    }

    /// The keys of map-numeric column `ci`, in ordinal order.
    pub fn map_numeric_keys(&self, ci: usize) -> &[String] {
        &self.map_numerics[ci].keys
    }

    /// Number of distinct values map-facet column `ci` holds.
    pub fn map_facet_value_count(&self, ci: usize) -> usize {
        self.map_facets[ci].values.len()
    }

    /// The value of map-facet column `ci` at ordinal `ord`.
    pub fn map_facet_value(&self, ci: usize, ord: u32) -> &str {
        &self.map_facets[ci].values[ord as usize]
    }

    /// The ordinal of `value` in map-facet column `ci`'s value
    /// dictionary, `None` when never ingested — the exact
    /// matches-nothing answer, like [`Self::facet_value_ord_of`].
    pub fn map_facet_value_ord_of(&self, ci: usize, value: &str) -> Option<u32> {
        self.map_facets[ci].value_index.get(value).copied()
    }

    /// The value ordinal of `doc_id`'s entry under `key_ord` in
    /// map-facet column `ci`, `None` when absent.
    pub fn map_facet_value_ord(&self, ci: usize, key_ord: u32, doc_id: u32) -> Option<u32> {
        self.map_facets[ci].value_ord(doc_id as usize, key_ord)
    }

    /// Record `doc_id`'s map-facet entry; see [`MapFacetStore::set`].
    pub fn set_map_facet(&mut self, ci: usize, doc_id: u32, key: &str, value: &str) {
        self.map_facets[ci].set(doc_id, key, value);
    }

    /// Number of map<string, f64> columns.
    pub fn map_numeric_count(&self) -> usize {
        self.map_numerics.len()
    }

    /// The name of map-numeric column `ci`. Panics when out of range.
    pub fn map_numeric_name(&self, ci: usize) -> &str {
        &self.map_numerics[ci].name
    }

    /// The index of the map-numeric column named `name`.
    pub fn map_numeric_index(&self, name: &str) -> Option<usize> {
        self.map_numerics.iter().position(|c| c.name == name)
    }

    /// The key ordinal of `key` in map-numeric column `ci`.
    pub fn map_numeric_key_ord(&self, ci: usize, key: &str) -> Option<u32> {
        self.map_numerics[ci].key_index.get(key).copied()
    }

    /// (min, max) of map-numeric column `ci` under `key_ord`.
    pub fn map_numeric_key_min_max(&self, ci: usize, key_ord: u32) -> (f64, f64) {
        self.map_numerics[ci].key_min_max()[key_ord as usize]
    }

    /// `doc_id`'s value under `key_ord` in map-numeric column `ci`.
    pub fn map_numeric_value(&self, ci: usize, key_ord: u32, doc_id: u32) -> Option<f64> {
        self.map_numerics[ci].value(doc_id as usize, key_ord)
    }

    /// Record `doc_id`'s map-numeric entry; see [`MapNumericStore::set`].
    pub fn set_map_numeric(&mut self, ci: usize, doc_id: u32, key: &str, value: f64) {
        self.map_numerics[ci].set(doc_id, key, value);
    }

    /// The name of field `f`. Panics when out of range.
    pub fn field_name(&self, f: usize) -> &str {
        &self.fields[f].name
    }

    /// Record field `f`'s analyzer fingerprint, or refuse if it
    /// contradicts what the field already holds.
    ///
    /// A fingerprint is written once, by the first document that carries
    /// one, and is immutable after. A LATER document analyzed differently
    /// into the same column is exactly the drift this exists to catch:
    /// the two halves of the column would hold different term identities
    /// and every score over it would silently mix them. 0 means the
    /// caller does not know its own spec, which neither sets nor checks.
    pub fn set_analysis_fingerprint(&mut self, f: usize, fingerprint: u64) -> Result<(), String> {
        if fingerprint == 0 {
            return Ok(());
        }
        let field = &mut self.fields[f];
        match field.analysis_fingerprint {
            0 => {
                field.analysis_fingerprint = fingerprint;
                Ok(())
            }
            held if held == fingerprint => Ok(()),
            held => Err(format!(
                "field {:?} was built with analyzer fingerprint {held:#x} but this \
                 document carries {fingerprint:#x}; one column holds one term identity",
                field.name
            )),
        }
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
    /// and total; shared texts, lineages, and doc count. Every scorer
    /// runs against one of these views (`docs/multi-field.md`). Panics
    /// when `f` is out of range.
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
            // A sentence field takes the document's table verbatim under
            // the same opt-in rule as positions; the node checked its
            // shape and refused a sentence-less document upstream.
            if field.sentences {
                field.doc_sentences[slot] = analyzed.sentences.unwrap_or_else(|| {
                    panic!(
                        "field {:?} keeps sentence spans but the document carried none",
                        field.name
                    )
                });
            }
            // A positional field takes the document's ordinals verbatim;
            // every other field drops them, which is the whole opt-in:
            // the payload exists only where it was asked for. The shape
            // check and the refusal of a positionless document into a
            // positional field happened upstream (the node), so here a
            // violation is a programming error, not an input error.
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
                field.postings.entry(term).or_default().push(Posting {
                    doc_id,
                    tf,
                    offsets,
                    positions,
                });
            }
        }
    }

    /// Persist to `path` (atomically: write tmp, fsync, rename, fsync
    /// parent). Writes v8: the payload is [`Self::write_v6_to`]'s v6
    /// bytes when no columns are declared and v7 bytes when they are,
    /// then [`finalize_v8`] stamps the integrity table over it, so
    /// every saved file can prove its bytes are the ones this write
    /// produced.
    pub fn save(&self, path: &Path) -> io::Result<()> {
        let tmp: PathBuf = path.with_extension("bm25tmp");
        {
            let mut w = io::BufWriter::new(std::fs::File::create(&tmp)?);
            self.write_v6_to(&mut w)?;
            w.flush()?;
        }
        finalize_v8(&tmp)?;
        std::fs::rename(&tmp, path)?;
        fsync_parent(path)
    }

    /// Persist with first-seen dictionaries under the ordinary v8
    /// envelope: the exact file a pre-2026-09 build wrote. Legacy oracle
    /// for the tests only (see [`Self::write_v6_first_seen_dictionaries`]).
    pub fn save_first_seen_dictionaries(&self, path: &Path) -> io::Result<()> {
        let tmp: PathBuf = path.with_extension("bm25tmp");
        {
            let mut w = io::BufWriter::new(std::fs::File::create(&tmp)?);
            self.write_v6_first_seen_dictionaries(&mut w)?;
            w.flush()?;
        }
        finalize_v8(&tmp)?;
        std::fs::rename(&tmp, path)?;
        fsync_parent(path)
    }

    /// Persist in the v5 format. Correctness oracle only — the
    /// v5-vs-v6 section-parity and query-identity tests; production
    /// saves are v6 and multi-field stores are refused here.
    pub fn save_v5(&self, path: &Path) -> io::Result<()> {
        let tmp: PathBuf = path.with_extension("bm25tmp");
        {
            let mut w = io::BufWriter::new(std::fs::File::create(&tmp)?);
            self.write_to(&mut w)?;
            w.flush()?;
            w.get_ref().sync_all()?;
        }
        std::fs::rename(&tmp, path)?;
        fsync_parent(path)
    }

    /// Load from `path`.
    pub fn load(path: &Path) -> io::Result<Self> {
        let bytes = std::fs::read(path)?;
        Self::read_from(&mut &bytes[..])
    }
}

/// One field of a [`Bm25Store`] as its own [`Bm25Index`]
/// (`docs/multi-field.md`): postings, document lengths, and total
/// length come from the field; texts, lineages, and the document count
/// are the store's shared ones (a document is a document — idf's N is
/// corpus-wide, never per field).
pub struct StoreFieldView<'a> {
    pub(crate) store: &'a Bm25Store,
    pub(crate) field: &'a FieldStore,
}

impl<'a> StoreFieldView<'a> {
    pub(crate) fn prefix_iter(&self, prefix: &str) -> Box<dyn Iterator<Item = String> + 'a> {
        let prefix = prefix.to_owned();
        Box::new(
            self.field
                .postings
                .range(prefix.clone()..)
                .take_while(move |(term, _)| term.starts_with(&prefix))
                .map(|(term, _)| term.clone()),
        )
    }
}

impl Bm25Index for StoreFieldView<'_> {
    fn doc_count(&self) -> u64 {
        self.store.doc_count()
    }
    fn total_doc_length(&self) -> u64 {
        self.field.total_length
    }
    fn doc_length(&self, doc_id: u32) -> u32 {
        self.field
            .doc_lengths
            .get(doc_id as usize)
            .copied()
            .unwrap_or(0)
    }
    fn df(&self, term: &str) -> u32 {
        self.field.postings.get(term).map_or(0, |p| p.len() as u32)
    }
    fn for_each_posting(&self, term: &str, f: &mut PostingCallback) {
        if let Some(postings) = self.field.postings.get(term) {
            for p in postings {
                f(p.doc_id, p.tf, &p.offsets);
            }
        }
    }
    fn for_each_doc_tf(&self, term: &str, f: &mut dyn FnMut(u32, u32)) {
        if let Some(postings) = self.field.postings.get(term) {
            for p in postings {
                f(p.doc_id, p.tf);
            }
        }
    }
    fn posting_offsets(&self, term: &str, doc_id: u32) -> Vec<(u32, u32)> {
        let Some(postings) = self.field.postings.get(term) else {
            return Vec::new();
        };
        match postings.binary_search_by_key(&doc_id, |p| p.doc_id) {
            Ok(i) => postings[i].offsets.clone(),
            Err(_) => Vec::new(),
        }
    }
    fn text(&self, doc_id: u32) -> Option<String> {
        self.store.text(doc_id).map(str::to_string)
    }
    fn lineage(&self, doc_id: u32) -> Option<DocLineage> {
        self.store.lineage(doc_id)
    }
    fn has_positions(&self) -> bool {
        self.field.positions
    }
    fn posting_positions(&self, term: &str, doc_id: u32) -> Option<Vec<u32>> {
        let postings = self.field.postings.get(term)?;
        postings
            .binary_search_by_key(&doc_id, |p| p.doc_id)
            .ok()
            .map(|i| postings[i].positions.clone())
    }
    fn has_sentences(&self) -> bool {
        self.field.sentences
    }
    fn doc_sentences(&self, doc_id: u32) -> Option<Vec<(u32, u32)>> {
        if !self.field.sentences {
            return None;
        }
        Some(
            self.field
                .doc_sentences
                .get(doc_id as usize)
                .cloned()
                .unwrap_or_default(),
        )
    }
    fn prefix_terms(&self, prefix: &str) -> Box<dyn Iterator<Item = String> + '_> {
        self.prefix_iter(prefix)
    }
    fn expand_prefix(&self, prefix: &str, cap: usize) -> Result<Vec<String>, usize> {
        let matches: Vec<&String> = self
            .field
            .postings
            .range(prefix.to_string()..)
            .map(|(term, _)| term)
            .take_while(|term| term.starts_with(prefix))
            .collect();
        if matches.len() > cap {
            Err(matches.len())
        } else {
            Ok(matches.into_iter().cloned().collect())
        }
    }
    fn suggest_prefix(&self, prefix: &str, max_scan: usize) -> Result<Vec<(String, u32)>, usize> {
        // Borrow through the walk; clone the terms only once the count
        // is known to fit, so a scan past the bound allocates nothing.
        let matches: Vec<(&String, u32)> = self
            .field
            .postings
            .range(prefix.to_string()..)
            .take_while(|(term, _)| term.starts_with(prefix))
            .map(|(term, postings)| (term, postings.len() as u32))
            .collect();
        if matches.len() > max_scan {
            Err(matches.len())
        } else {
            Ok(matches
                .into_iter()
                .map(|(term, df)| (term.clone(), df))
                .collect())
        }
    }
}

/// The store itself scores as its body field (field 0) — the surface
/// every single-field caller uses; multi-field scoring goes through
/// [`Bm25Store::field`].
impl Bm25Index for Bm25Store {
    fn doc_count(&self) -> u64 {
        self.field(0).doc_count()
    }
    fn total_doc_length(&self) -> u64 {
        self.field(0).total_doc_length()
    }
    fn doc_length(&self, doc_id: u32) -> u32 {
        self.field(0).doc_length(doc_id)
    }
    fn df(&self, term: &str) -> u32 {
        self.field(0).df(term)
    }
    fn for_each_posting(&self, term: &str, f: &mut PostingCallback) {
        self.field(0).for_each_posting(term, f);
    }
    fn for_each_doc_tf(&self, term: &str, f: &mut dyn FnMut(u32, u32)) {
        self.field(0).for_each_doc_tf(term, f);
    }
    fn posting_offsets(&self, term: &str, doc_id: u32) -> Vec<(u32, u32)> {
        self.field(0).posting_offsets(term, doc_id)
    }
    fn text(&self, doc_id: u32) -> Option<String> {
        self.field(0).text(doc_id)
    }
    fn lineage(&self, doc_id: u32) -> Option<DocLineage> {
        self.field(0).lineage(doc_id)
    }
    fn has_positions(&self) -> bool {
        self.field(0).has_positions()
    }
    fn posting_positions(&self, term: &str, doc_id: u32) -> Option<Vec<u32>> {
        self.field(0).posting_positions(term, doc_id)
    }
    fn has_sentences(&self) -> bool {
        self.field(0).has_sentences()
    }
    fn doc_sentences(&self, doc_id: u32) -> Option<Vec<(u32, u32)>> {
        self.field(0).doc_sentences(doc_id)
    }
    fn prefix_terms(&self, prefix: &str) -> Box<dyn Iterator<Item = String> + '_> {
        self.field(0).prefix_iter(prefix)
    }
    fn expand_prefix(&self, prefix: &str, cap: usize) -> Result<Vec<String>, usize> {
        self.field(0).expand_prefix(prefix, cap)
    }
    fn suggest_prefix(&self, prefix: &str, max_scan: usize) -> Result<Vec<(String, u32)>, usize> {
        self.field(0).suggest_prefix(prefix, max_scan)
    }
}

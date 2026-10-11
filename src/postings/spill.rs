//! The disk-backed external sort for corpus-scale ingests: pending postings
//! buffer per field, spilled runs, and `finish_v6`/`finish_v4`, whose output is
//! byte-identical to `Bm25Store::save` — pinned by the byte-identity tests.

use super::*;

use std::io::{self, Write};
use std::path::{Path, PathBuf};

/// One field's slice of a [`SpillBuilder`]: its pending postings
/// buffer, spilled runs, and per-document lengths — the spill-side
/// mirror of [`FieldStore`].
pub(crate) type PendingPosting = (String, u32, u32, Vec<(u32, u32)>, Vec<u32>);
pub(crate) type PostingDirectoryEntry = (String, u64, u64, u64, u32);

pub(crate) struct FieldSpill {
    pub(crate) name: String,
    /// Hash of the field's AnalysisSpec, persisted in the v6 field
    /// table. 0 until the ingest layer wires real fingerprints.
    pub(crate) analysis_fingerprint: u64,
    /// Pending postings: (term, doc_id, tf, offsets, positions).
    pub(crate) buf: Vec<PendingPosting>,
    pub(crate) runs: Vec<PathBuf>,
    pub(crate) doc_lengths: Vec<u32>,
    pub(crate) total_length: u64,
    /// Whether this field keeps token positions (the spill-side mirror
    /// of `FieldStore::positions`): runs then carry one ordinal per
    /// occurrence after the offset pairs, and `finish` writes the
    /// field's kind-7 section.
    pub(crate) positions: bool,
    /// Whether this field keeps sentence spans (the spill-side mirror of
    /// `FieldStore::sentences`): spans stream to a stage file as
    /// documents arrive, and `finish` writes the field's kind-8 section.
    pub(crate) sentences: bool,
    /// The stage, opened on the first document (`docs/highlighting.md`).
    pub(crate) sentence_stage: Option<SentencesStage>,
}

/// The ingest-time stage of one sentence field's kind-8 section: spans
/// stream to a file in slot order as documents arrive, and the per-slot
/// bases accumulate in heap (4 B per slot, the doc-length table's own
/// budget).
pub(crate) struct SentencesStage {
    pub(crate) writer: io::BufWriter<std::fs::File>,
    pub(crate) path: PathBuf,
    pub(crate) bases: Vec<u32>,
    pub(crate) total: u64,
}

impl FieldSpill {
    pub(crate) fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            analysis_fingerprint: 0,
            buf: Vec::new(),
            runs: Vec::new(),
            doc_lengths: Vec::new(),
            total_length: 0,
            positions: false,
            sentences: false,
            sentence_stage: None,
        }
    }
}

/// Disk-spilling builder producing the SAME v6 file as [`Bm25Store::save`]
/// (byte-identical), with bounded memory at any corpus size.
///
/// The in-memory store keeps every posting and every text in heap until
/// flush — ~100 GB for a 10M-chunk shard. This builder instead:
///
/// - streams document texts to a spill file AT ADD TIME, already in the
///   final texts-section encoding (gap slots get their `u32::MAX` marker
///   immediately), so flush byte-copies the file into place;
/// - accumulates postings in a bounded per-field buffer; when the total
///   fills, every field's buffer is sorted by `(term, doc_id)` and
///   written out as that field's next run. Doc ids only grow, so runs
///   never overlap within a term and the flush-time merge is a heap of
///   run heads concatenating per-term lists in run order — one
///   sequential pass per field, no random access.
///
/// Heap while building: the sort buffers (default 256 MB total) plus the
/// per-doc length/lineage tables. A spilling shard is NOT searchable
/// (that would mean scanning every run per term); flush it first.
pub struct SpillBuilder {
    pub(crate) dir: PathBuf,
    /// Per-field state, field-id order; never empty. Field 0 is the
    /// body.
    pub(crate) fields: Vec<FieldSpill>,
    /// Total bytes buffered across every field's `buf`.
    pub(crate) buf_bytes: usize,
    pub(crate) cap_bytes: usize,
    /// Texts spill, encoded exactly as the v3 texts section.
    pub(crate) texts: io::BufWriter<std::fs::File>,
    pub(crate) texts_bytes: u64,
    /// Per-slot text byte length (`u32::MAX` = absent); sizes the final
    /// sections without rereading the spill.
    pub(crate) text_lens: Vec<u32>,
    pub(crate) lineages: Vec<Option<DocLineage>>,
    /// Documents with postings in any field.
    pub(crate) doc_count: u64,
    /// Facet columns in facet-id order (dict + per-slot ordinals stay
    /// in heap: kilobytes of dictionary plus 4 B per slot, never the
    /// memory ceiling the spill exists for). Non-empty makes `finish`
    /// write v7.
    pub(crate) facets: Vec<FacetStore>,
    /// Numeric columns in numeric-id order (8 B per slot in heap, same
    /// argument as `facets`). Non-empty makes `finish` write v7.
    pub(crate) numerics: Vec<NumericStore>,
    /// map<string, string> columns (dictionaries plus per-doc pair
    /// lists in heap — bytes per entry, never the spill's memory
    /// ceiling). Non-empty makes `finish` write v7.
    pub(crate) map_facets: Vec<MapFacetStore>,
    /// map<string, f64> columns. Non-empty makes `finish` write v7.
    pub(crate) map_numerics: Vec<MapNumericStore>,
    pub(crate) map_integers: Vec<crate::integer_map::Store<i64>>,
    pub(crate) map_unsigned_integers: Vec<crate::integer_map::Store<u64>>,
    /// i64 columns (8 B per slot in heap, same argument as `numerics`).
    /// Non-empty makes `finish` write v7.
    pub(crate) integers: Vec<IntStore>,
    pub(crate) unsigned_integers: Vec<UintStore>,
    /// Geo-point columns (16 B per slot in heap). Non-empty makes
    /// `finish` write v7.
    pub(crate) geos: Vec<GeoStore>,
    /// The mapped-plan binding, persisted as the kind-6 table entry
    /// when `finish` writes v7 (`Some` forces v7). The v4 oracle
    /// format cannot carry it and refuses.
    pub(crate) binding: Option<StoredBinding>,
    /// The derived-column declaration `finish` persists as the kind-15
    /// entry (`Some` forces v7).
    pub(crate) derived: Option<StoredDerived>,
    pub(crate) sources: crate::source_archive::SourceArchive,
    /// Write the v4 format instead of v6 (benchmarking/migration only).
    pub(crate) v4_only: bool,
}

impl SpillBuilder {
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

    /// Default sort-buffer capacity before runs are spilled.
    pub const DEFAULT_BUF_BYTES: usize = 256 << 20;

    /// Create a single-field ("body") builder spilling into `dir`
    /// (created; must not hold a previous builder's files). `finish`
    /// writes the v6 format.
    pub fn create(dir: &Path) -> io::Result<Self> {
        Self::create_format(dir, &["body"], false)
    }

    /// Create a builder with the given field table; same contract as
    /// [`Bm25Store::with_fields`].
    pub fn create_with_fields(dir: &Path, names: &[&str]) -> io::Result<Self> {
        Self::create_format(dir, names, false)
    }

    /// A builder whose `finish` writes the v4 format. Exists for
    /// benchmarking (v4-vs-v6 scorer comparisons) and migration checks;
    /// new shards are always v6.
    pub fn create_v4_for_bench(dir: &Path) -> io::Result<Self> {
        Self::create_format(dir, &["body"], true)
    }

    pub(crate) fn create_format(dir: &Path, names: &[&str], v4_only: bool) -> io::Result<Self> {
        assert!(!names.is_empty(), "a builder needs at least one field");
        for (i, name) in names.iter().enumerate() {
            assert!(!name.is_empty(), "field {i} has an empty name");
            assert!(
                u16::try_from(name.len()).is_ok(),
                "field name exceeds u16 length"
            );
            assert!(!names[..i].contains(name), "duplicate field name {name:?}");
        }
        std::fs::create_dir_all(dir)?;
        let texts = io::BufWriter::new(std::fs::File::create(dir.join("texts.spill"))?);
        Ok(Self {
            dir: dir.to_path_buf(),
            fields: names.iter().map(|n| FieldSpill::new(n)).collect(),
            buf_bytes: 0,
            cap_bytes: Self::DEFAULT_BUF_BYTES,
            texts,
            texts_bytes: 0,
            text_lens: Vec::new(),
            lineages: Vec::new(),
            doc_count: 0,
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
            sources: crate::source_archive::SourceArchive::spilling(&dir.join("sources.spill"))?,
            v4_only,
        })
    }

    /// Declare the facet field table, in facet-id order; same contract
    /// as [`Bm25Store::with_facets`]. Must be called before any
    /// document is added; makes `finish` write v7.
    pub fn with_facet_fields(mut self, names: &[&str]) -> Self {
        assert!(
            self.fields[0].doc_lengths.is_empty(),
            "facet fields must be declared before documents are added"
        );
        assert!(!self.v4_only, "the v4 format carries no facet columns");
        validate_facet_names(names);
        self.facets = names.iter().map(|n| FacetStore::new(n)).collect();
        self
    }

    /// Declare which fields keep token positions; same contract as
    /// [`Bm25Store::with_positions`]. Must be called before any
    /// document is added; makes `finish` write v7.
    pub fn with_position_fields(mut self, names: &[&str]) -> Self {
        assert!(
            self.fields[0].doc_lengths.is_empty(),
            "positional fields must be declared before documents are added"
        );
        assert!(!self.v4_only, "the v4 format carries no token positions");
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

    /// Declare the fields that keep sentence spans per document
    /// (`docs/highlighting.md`), the spill-side twin of
    /// [`Bm25Store::with_sentences`]: same rules, same panics.
    pub fn with_sentence_fields(mut self, names: &[&str]) -> Self {
        assert!(
            self.fields[0].doc_lengths.is_empty(),
            "sentence fields must be declared before documents are added"
        );
        assert!(!self.v4_only, "the v4 format carries no sentence spans");
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

    /// Record `doc_id`'s value for facet field `fi`; see
    /// [`FacetStore::set`] for the contract.
    pub fn set_facet(&mut self, fi: usize, doc_id: u32, value: &str) {
        self.facets[fi].set(doc_id, value);
    }

    /// Declare the numeric field table; same contract as
    /// [`Bm25Store::with_numerics`]. Must be called before any
    /// document is added; makes `finish` write v7.
    pub fn with_numeric_fields(mut self, names: &[&str]) -> Self {
        assert!(
            self.fields[0].doc_lengths.is_empty(),
            "numeric fields must be declared before documents are added"
        );
        assert!(!self.v4_only, "the v4 format carries no columns");
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

    /// Record `doc_id`'s value for numeric field `ni`; see
    /// [`NumericStore::set`] for the contract (finite values only).
    pub fn set_numeric(&mut self, ni: usize, doc_id: u32, value: f64) {
        self.numerics[ni].set(doc_id, value);
    }

    /// Declare the i64 field table; same contract as
    /// [`Bm25Store::with_integers`]. Must be called before any
    /// document is added; makes `finish` write v7.
    pub fn with_integer_fields(mut self, names: &[&str]) -> Self {
        assert!(
            self.fields[0].doc_lengths.is_empty(),
            "integer fields must be declared before documents are added"
        );
        assert!(!self.v4_only, "the v4 format carries no columns");
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

    /// Record `doc_id`'s value for integer field `ii`; see
    /// [`IntStore::set`] for the contract.
    pub fn set_integer(&mut self, ii: usize, doc_id: u32, value: i64) {
        self.integers[ii].set(doc_id, value);
    }

    /// Declare the u64 field table; same contract as
    /// [`Bm25Store::with_unsigned_integers`]. Must be called before any
    /// document is added; `finish` writes kind 11 columns in the v8 container.
    pub fn with_unsigned_integer_fields(mut self, names: &[&str]) -> Self {
        assert!(
            self.fields[0].doc_lengths.is_empty(),
            "integer fields must be declared before documents are added"
        );
        assert!(!self.v4_only, "the v4 format carries no columns");
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

    /// Record `doc_id`'s value for integer field `ii`; see
    /// [`UintStore::set`] for the contract.
    pub fn set_unsigned_integer(&mut self, ii: usize, doc_id: u32, value: u64) {
        self.unsigned_integers[ii].set(doc_id, value);
    }

    /// Declare the geo-point column table; same contract as
    /// [`Bm25Store::with_geos`]. Must be called before any document is
    /// added; makes `finish` write v7.
    pub fn with_geo_fields(mut self, names: &[&str]) -> Self {
        assert!(
            self.fields[0].doc_lengths.is_empty(),
            "geo fields must be declared before documents are added"
        );
        assert!(!self.v4_only, "the v4 format carries no columns");
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

    /// Record `doc_id`'s point for geo field `gi`; see [`GeoStore::set`]
    /// for the contract.
    pub fn set_geo(&mut self, gi: usize, doc_id: u32, lat: f64, lon: f64) {
        self.geos[gi].set(doc_id, lat, lon);
    }

    /// Declare the map<string, string> column table; same contract as
    /// [`Bm25Store::with_map_facets`].
    pub fn with_map_facet_fields(mut self, names: &[&str]) -> Self {
        assert!(
            self.fields[0].doc_lengths.is_empty(),
            "map columns must be declared before documents are added"
        );
        assert!(!self.v4_only, "the v4 format carries no columns");
        validate_facet_names(names);
        self.map_facets = names.iter().map(|n| MapFacetStore::new(n)).collect();
        self
    }

    /// Declare the map<string, f64> column table; same contract as
    /// [`Bm25Store::with_map_numerics`].
    pub fn with_map_numeric_fields(mut self, names: &[&str]) -> Self {
        assert!(
            self.fields[0].doc_lengths.is_empty(),
            "map columns must be declared before documents are added"
        );
        assert!(!self.v4_only, "the v4 format carries no columns");
        validate_facet_names(names);
        self.map_numerics = names.iter().map(|n| MapNumericStore::new(n)).collect();
        self
    }

    /// Configure exact i64 map columns before adding documents.
    pub fn with_map_integer_fields(mut self, names: &[&str]) -> Self {
        assert!(
            self.fields[0].doc_lengths.is_empty(),
            "map columns must be configured before documents"
        );
        assert!(!self.v4_only, "the v4 format carries no columns");
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
    pub fn with_map_unsigned_integer_fields(mut self, names: &[&str]) -> Self {
        assert!(
            self.fields[0].doc_lengths.is_empty(),
            "map columns must be configured before documents"
        );
        assert!(!self.v4_only, "the v4 format carries no columns");
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

    /// The index of the map-numeric column named `name`.
    pub fn map_numeric_index(&self, name: &str) -> Option<usize> {
        self.map_numerics.iter().position(|c| c.name == name)
    }

    /// Record `doc_id`'s map-facet entry; see [`MapFacetStore::set`].
    pub fn set_map_facet(&mut self, ci: usize, doc_id: u32, key: &str, value: &str) {
        self.map_facets[ci].set(doc_id, key, value);
    }

    /// Record `doc_id`'s map-numeric entry; see [`MapNumericStore::set`].
    pub fn set_map_numeric(&mut self, ci: usize, doc_id: u32, key: &str, value: f64) {
        self.map_numerics[ci].set(doc_id, key, value);
    }

    /// Override the sort-buffer capacity (tests force multi-run merges
    /// with tiny caps).
    pub fn with_buffer_bytes(mut self, cap: usize) -> Self {
        self.cap_bytes = cap.max(1);
        self
    }

    /// The number of document slots ever allocated (the next local doc id).
    pub fn next_doc_id(&self) -> u32 {
        self.fields[0].doc_lengths.len() as u32
    }

    /// Number of documents with postings (in any field).
    pub fn doc_count(&self) -> u64 {
        self.doc_count
    }

    /// Number of fields in the field table.
    pub fn field_count(&self) -> usize {
        self.fields.len()
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

    /// Sum of all body document lengths (BM25 avgdl numerator).
    pub fn total_doc_length(&self) -> u64 {
        self.fields[0].total_length
    }

    /// Append one analyzed document; same contract as
    /// [`Bm25Store::add_document_with_lineage`].
    pub fn add_document_with_lineage(
        &mut self,
        doc_id: u32,
        text: String,
        doc: AnalyzedDoc,
        lineage: Option<DocLineage>,
    ) -> io::Result<()> {
        assert!(
            doc.fields.len() <= self.fields.len(),
            "document carries {} fields, builder has {}",
            doc.fields.len(),
            self.fields.len()
        );
        let slot = doc_id as usize;
        assert!(
            slot >= self.fields[0].doc_lengths.len(),
            "doc id {doc_id} already used"
        );
        // Gap slots (ids consumed by the vector side) are written to the
        // spill NOW so the final texts section is a straight copy.
        // A sentence field's stage opens on the first document so the
        // builder chain stays infallible (docs/highlighting.md).
        for (fi, field) in self.fields.iter_mut().enumerate() {
            if field.sentences && field.sentence_stage.is_none() {
                let path = self.dir.join(format!("sentences-f{fi:03}.stage"));
                field.sentence_stage = Some(SentencesStage {
                    writer: io::BufWriter::new(std::fs::File::create(&path)?),
                    path,
                    bases: Vec::new(),
                    total: 0,
                });
            }
        }
        while self.fields[0].doc_lengths.len() < slot {
            write_u32(&mut self.texts, u32::MAX)?;
            self.texts_bytes += 4;
            self.text_lens.push(u32::MAX);
            for field in &mut self.fields {
                field.doc_lengths.push(0);
                if let Some(stage) = field.sentence_stage.as_mut() {
                    stage
                        .bases
                        .push(u32::try_from(stage.total).expect("sentences exceed u32"));
                }
            }
            self.lineages.push(None);
        }
        write_u32(&mut self.texts, text.len() as u32)?;
        self.texts.write_all(text.as_bytes())?;
        self.texts_bytes += 4 + text.len() as u64;
        self.text_lens.push(text.len() as u32);
        self.lineages.push(lineage);
        // Every sentence field's base for this slot is its running total
        // BEFORE the slot's spans, whether or not the document carries
        // the field.
        for field in &mut self.fields {
            if let Some(stage) = field.sentence_stage.as_mut() {
                stage
                    .bases
                    .push(u32::try_from(stage.total).expect("sentences exceed u32"));
            }
        }
        let mut lengths = vec![0u32; self.fields.len()];
        for (fi, analyzed) in doc.fields.into_iter().enumerate() {
            lengths[fi] = analyzed.length;
            let field = &mut self.fields[fi];
            field.total_length += u64::from(analyzed.length);
            if let Some(stage) = field.sentence_stage.as_mut() {
                let spans = analyzed.sentences.as_deref().unwrap_or_else(|| {
                    panic!(
                        "field {:?} keeps sentence spans but the document carried none",
                        field.name
                    )
                });
                for &(start, end) in spans {
                    write_u32(&mut stage.writer, start)?;
                    write_u32(&mut stage.writer, end)?;
                }
                stage.total += spans.len() as u64;
            }
            // Same opt-in rule as the heap store: a positional field
            // takes the document's ordinals, every other field drops
            // them. The node validated the shape and refused a
            // positionless document into a positional field upstream.
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
                self.buf_bytes += term.len() + 24 + 16 * offsets.len() + 4 * positions.len();
                field.buf.push((term, doc_id, tf, offsets, positions));
            }
        }
        if lengths.iter().any(|&l| l > 0) {
            self.doc_count += 1;
        }
        for (field, &length) in self.fields.iter_mut().zip(&lengths) {
            field.doc_lengths.push(length);
        }
        if self.buf_bytes >= self.cap_bytes {
            self.spill_run()?;
        }
        Ok(())
    }

    /// Sort every field's buffer by `(term, doc_id)` and write each as
    /// that field's next run.
    pub(crate) fn spill_run(&mut self) -> io::Result<()> {
        for (fi, field) in self.fields.iter_mut().enumerate() {
            if field.buf.is_empty() {
                continue;
            }
            field
                .buf
                .sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
            let path = self
                .dir
                .join(format!("run-f{fi:03}-{:06}", field.runs.len()));
            let mut w = io::BufWriter::new(std::fs::File::create(&path)?);
            let mut i = 0;
            while i < field.buf.len() {
                let term = &field.buf[i].0;
                let group_end = field.buf[i..]
                    .iter()
                    .position(|e| &e.0 != term)
                    .map_or(field.buf.len(), |p| i + p);
                write_u16(&mut w, term.len() as u16)?;
                w.write_all(term.as_bytes())?;
                write_u32(&mut w, (group_end - i) as u32)?;
                for (_, doc_id, tf, offsets, positions) in &field.buf[i..group_end] {
                    write_u32(&mut w, *doc_id)?;
                    write_u32(&mut w, *tf)?;
                    write_u32(&mut w, offsets.len() as u32)?;
                    for &(start, end) in offsets {
                        write_u32(&mut w, start)?;
                        write_u32(&mut w, end)?;
                    }
                    // A positional field's run carries one ordinal per
                    // occurrence right after the pairs; the run reader
                    // knows the field's flag and reads them back.
                    if field.positions {
                        debug_assert_eq!(positions.len(), offsets.len());
                        for &ordinal in positions {
                            write_u32(&mut w, ordinal)?;
                        }
                    }
                }
                i = group_end;
            }
            w.flush()?;
            field.runs.push(path);
            field.buf.clear();
        }
        self.buf_bytes = 0;
        Ok(())
    }

    /// Merge the runs and assemble the file at `path` (atomically:
    /// write tmp, rename). The spill directory is removed on success.
    /// Writes v6, byte-identical to [`Bm25Store::save`] on the same
    /// corpus, unless built with [`Self::create_v4_for_bench`].
    pub fn finish(&mut self, path: &Path) -> io::Result<()> {
        if self.v4_only {
            if self.binding.is_some() || self.derived.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "the v4 oracle format cannot carry a mapped-plan binding or a derived-column declaration",
                ));
            }
            self.finish_v4(path)
        } else {
            self.finish_v6(path)
        }
    }

    /// The mapped-plan binding the finish will persist, if any.
    pub fn binding(&self) -> Option<&StoredBinding> {
        self.binding.as_ref()
    }

    /// Set the binding `finish` persists (see [`Bm25Store::set_binding`]).
    pub fn set_binding(&mut self, binding: Option<StoredBinding>) {
        self.binding = binding;
    }

    /// The derived-column declaration the finish will persist, if any.
    pub fn derived(&self) -> Option<&StoredDerived> {
        self.derived.as_ref()
    }

    /// Set the declaration `finish` persists (see [`Bm25Store::set_derived`]).
    pub fn set_derived(&mut self, derived: Option<StoredDerived>) {
        self.derived = derived;
    }

    /// Merge one field's runs into a postings-section body file at
    /// `body_path` (per-term doc/occurrence/skip runs, WITHOUT the
    /// section's leading u32 n_terms), returning the directory
    /// `(term, doc_rel, skip_rel, occ_rel, df)` with offsets relative
    /// to the body start. Merge side of the v5-shaped postings layout:
    /// the doc run streams into the body while occurrence bytes divert
    /// to a per-term stage file and the skip builder accumulates
    /// `(tf, dl)` per 128-posting block — all single-pass with O(1)
    /// state per term, so the sub-1 GB build memory the spill builder
    /// buys is untouched.
    pub(crate) fn merge_field_runs(
        runs: &[PathBuf],
        doc_lengths: &[u32],
        body_path: &Path,
        occ_stage_path: &Path,
        positions: Option<&mut PositionsStage>,
    ) -> io::Result<Vec<PostingDirectoryEntry>> {
        let mut directory: Vec<PostingDirectoryEntry> = Vec::new();
        let mut out = io::BufWriter::new(std::fs::File::create(body_path)?);
        let mut heads: Vec<RunHead> = Vec::new();
        let positional = positions.is_some();
        let mut positions = positions;
        for run in runs {
            if let Some(head) = RunHead::open(run, positional)? {
                heads.push(head);
            }
        }
        let mut cursor = 0u64;
        while !heads.is_empty() {
            // Smallest term across run heads; runs are time-ordered,
            // and ids only grow, so draining matching heads in run
            // order keeps postings doc-ascending.
            let term = heads
                .iter()
                .map(|h| h.term.clone())
                .min()
                .expect("nonempty heads");
            let doc_rel = cursor;
            let mut df = 0u64;
            let mut occ_bytes = 0u64;
            let mut occ_start = 0u32;
            let mut skip_l0: Vec<u8> = Vec::new();
            let mut skip = SkipRunBuilder::new();
            // The kind-7 base of this term: occurrences merged so far, in
            // the same (sorted) term order the directory will have.
            if let Some(stage) = positions.as_deref_mut() {
                stage
                    .bases
                    .push(u32::try_from(stage.total).expect("positions exceed u32"));
            }
            {
                let mut occ_stage = io::BufWriter::new(std::fs::File::create(occ_stage_path)?);
                let mut idx = 0;
                while idx < heads.len() {
                    if heads[idx].term == term {
                        for _ in 0..heads[idx].n_postings {
                            let (doc_id, tf, offsets, ordinals) = heads[idx].next_posting_raw()?;
                            let dl = doc_lengths[doc_id as usize];
                            write_u32(&mut out, doc_id)?;
                            write_u32(&mut out, tf)?;
                            write_u32(&mut out, occ_start)?;
                            // The run encoding's offset bytes are
                            // exactly the occurrence-run pairs, and its
                            // ordinal bytes exactly the kind-7 entries.
                            occ_stage.write_all(&offsets)?;
                            if let Some(stage) = positions.as_deref_mut() {
                                stage.writer.write_all(&ordinals)?;
                                stage.total += ordinals.len() as u64 / 4;
                            }
                            skip.push(tf, dl, doc_id, &mut skip_l0)?;
                            occ_start += offsets.len() as u32 / 8;
                            occ_bytes += offsets.len() as u64;
                            df += 1;
                        }
                        if !heads[idx].advance()? {
                            heads.remove(idx);
                            continue;
                        }
                    }
                    idx += 1;
                }
                occ_stage.flush()?;
            }
            write_u32(&mut out, occ_start)?; // sentinel
            {
                let mut stage = std::fs::File::open(occ_stage_path)?;
                io::copy(&mut stage, &mut out)?;
            }
            let (l0_bytes, l1) = skip.finish(&mut skip_l0)?;
            debug_assert_eq!(l0_bytes, skip_l0.len() as u64);
            let occ_rel = doc_rel + 12 * df + 4;
            let skip_rel = occ_rel + occ_bytes;
            let skip_bytes = skip_run_size(l0_bytes, &l1);
            write_skip_run(&mut out, &skip_l0, &l1)?;
            directory.push((term, doc_rel, skip_rel, occ_rel, df as u32));
            cursor = skip_rel + skip_bytes;
        }
        out.flush()?;
        Ok(directory)
    }

    /// Merge every field's runs and assemble the v6 file at `path` —
    /// v7 when facet fields are declared — mirroring
    /// [`Bm25Store::write_v6_to`] section for section (the dual-writer
    /// byte-identity test pins the two).
    pub(crate) fn finish_v6(&mut self, path: &Path) -> io::Result<()> {
        self.spill_run()?;
        self.texts.flush()?;

        let occ_stage_path = self.dir.join("occ.stage");
        let mut directories: Vec<Vec<PostingDirectoryEntry>> =
            Vec::with_capacity(self.fields.len());
        let mut body_paths: Vec<PathBuf> = Vec::with_capacity(self.fields.len());
        let mut body_lens: Vec<u64> = Vec::with_capacity(self.fields.len());
        // Per positional field: its kind-7 stage (ordinals streamed to
        // a file during the merge, bases kept in heap at 4 B per term).
        let mut position_stages: Vec<Option<PositionsStage>> =
            Vec::with_capacity(self.fields.len());
        for (fi, field) in self.fields.iter().enumerate() {
            let body_path = self.dir.join(format!("postings-f{fi:03}.body"));
            let mut stage = if field.positions {
                let path = self.dir.join(format!("positions-f{fi:03}.stage"));
                Some(PositionsStage {
                    writer: io::BufWriter::new(std::fs::File::create(&path)?),
                    path,
                    bases: Vec::new(),
                    total: 0,
                })
            } else {
                None
            };
            let directory = Self::merge_field_runs(
                &field.runs,
                &field.doc_lengths,
                &body_path,
                &occ_stage_path,
                stage.as_mut(),
            )?;
            if let Some(stage) = stage.as_mut() {
                stage.writer.flush()?;
            }
            body_lens.push(std::fs::metadata(&body_path)?.len());
            body_paths.push(body_path);
            directories.push(directory);
            position_stages.push(stage);
        }

        // Section geometry, identical to Bm25Store::write_v6_to.
        let n_slots = self.fields[0].doc_lengths.len() as u64;
        let has_columns = !self.facets.is_empty()
            || !self.numerics.is_empty()
            || !self.map_facets.is_empty()
            || !self.map_numerics.is_empty()
            || !self.map_integers.is_empty()
            || !self.map_unsigned_integers.is_empty()
            || !self.integers.is_empty()
            || !self.unsigned_integers.is_empty()
            || !self.geos.is_empty()
            || self.binding.is_some()
            || self.derived.is_some()
            || !self.sources.is_empty()
            || self.fields.iter().any(|f| f.positions || f.sentences);
        let column_table_size: u64 = if !has_columns {
            0
        } else {
            4 + self
                .facets
                .iter()
                .map(|f| 2 + f.name.len() as u64 + 1 + 4 + 8 + 8)
                .sum::<u64>()
                + self
                    .numerics
                    .iter()
                    .map(|n| 2 + n.name.len() as u64 + 1 + 8 + 8 + 8)
                    .sum::<u64>()
                + self
                    .map_facets
                    .iter()
                    .map(|c| 2 + c.name.len() as u64 + 1 + 4 + 4 + 8 * 4)
                    .sum::<u64>()
                + self
                    .map_numerics
                    .iter()
                    .map(|c| 2 + c.name.len() as u64 + 1 + 4 + 8 * 3)
                    .sum::<u64>()
                + self
                    .map_integers
                    .iter()
                    .map(|c| 2 + c.name.len() as u64 + 1 + 16)
                    .sum::<u64>()
                + self
                    .map_unsigned_integers
                    .iter()
                    .map(|c| 2 + c.name.len() as u64 + 1 + 16)
                    .sum::<u64>()
                + self
                    .integers
                    .iter()
                    .map(|c| 2 + c.name.len() as u64 + 1 + 8 * 3)
                    .sum::<u64>()
                + self
                    .unsigned_integers
                    .iter()
                    .map(|c| 2 + c.name.len() as u64 + 1 + 8 * 3)
                    .sum::<u64>()
                + self
                    .geos
                    .iter()
                    .map(|c| 2 + c.name.len() as u64 + 1 + 8 * 5)
                    .sum::<u64>()
                + self
                    .fields
                    .iter()
                    .filter(|f| f.positions)
                    .map(|f| 2 + (POSITIONS_ENTRY_PREFIX.len() + f.name.len()) as u64 + 1 + 8 * 2)
                    .sum::<u64>()
                + self
                    .fields
                    .iter()
                    .filter(|f| f.sentences)
                    .map(|f| 2 + (SENTENCES_ENTRY_PREFIX.len() + f.name.len()) as u64 + 1 + 8 * 2)
                    .sum::<u64>()
                + binding_entry_size(self.binding.as_ref())
                + derived_entry_size(self.derived.as_ref())
                + if self.sources.is_empty() {
                    0
                } else {
                    SOURCE_ENTRY_BYTES
                }
        };
        let header_size: u64 = 8
            + 4
            + 4
            + 8 * 3
            + self
                .fields
                .iter()
                .map(|f| 2 + f.name.len() as u64 + 8 * 5)
                .sum::<u64>()
            + column_table_size;
        let texts_off = header_size;
        let text_index_off = texts_off + self.texts_bytes;
        let lineages_off = text_index_off + 12 * n_slots;
        let lineages_size: u64 = self
            .lineages
            .iter()
            .map(|l| if l.is_some() { 25 } else { 1 })
            .sum();
        let mut cursor = lineages_off + lineages_size;
        // (doc_lengths_off, postings_off, directory_off) per field.
        let mut section_offs: Vec<(u64, u64, u64)> = Vec::with_capacity(self.fields.len());
        for fi in 0..self.fields.len() {
            let doc_lengths_off = cursor;
            let postings_off = doc_lengths_off + 4 * n_slots;
            let directory_off = postings_off + 4 + body_lens[fi];
            let directory_size = 4
                + 34 * directories[fi].len() as u64
                + directories[fi]
                    .iter()
                    .map(|(t, ..)| t.len() as u64)
                    .sum::<u64>();
            section_offs.push((doc_lengths_off, postings_off, directory_off));
            cursor = directory_off + directory_size;
        }
        // (dict_off, ords_off) per facet, then vals_off per numeric,
        // after the last field group — column-table order.
        let mut facet_offs: Vec<(u64, u64)> = Vec::with_capacity(self.facets.len());
        for facet in &self.facets {
            let dict_off = cursor;
            let dict_size: u64 = facet.dict.iter().map(|v| 2 + v.len() as u64).sum();
            let ords_off = dict_off + dict_size;
            facet_offs.push((dict_off, ords_off));
            cursor = ords_off + 4 * n_slots;
        }
        let mut numeric_offs: Vec<u64> = Vec::with_capacity(self.numerics.len());
        for _ in &self.numerics {
            numeric_offs.push(cursor);
            cursor += 8 * n_slots;
        }
        let mut map_facet_offs: Vec<(u64, u64, u64, u64)> =
            Vec::with_capacity(self.map_facets.len());
        for c in &self.map_facets {
            let keys_off = cursor;
            let keys_size: u64 = c.keys.iter().map(|k| 2 + k.len() as u64).sum();
            let values_off = keys_off + keys_size;
            let values_size: u64 = c.values.iter().map(|v| 2 + v.len() as u64).sum();
            let offsets_off = values_off + values_size;
            let pairs_off = offsets_off + 4 * (n_slots + 1);
            let total_pairs: u64 = c.pairs.iter().map(|l| l.len() as u64).sum();
            map_facet_offs.push((keys_off, values_off, offsets_off, pairs_off));
            cursor = pairs_off + 8 * total_pairs;
        }
        let mut map_numeric_offs: Vec<(u64, u64, u64)> =
            Vec::with_capacity(self.map_numerics.len());
        for c in &self.map_numerics {
            let keys_off = cursor;
            let keys_size: u64 = c.keys.iter().map(|k| 2 + k.len() as u64 + 16).sum();
            let offsets_off = keys_off + keys_size;
            let pairs_off = offsets_off + 4 * (n_slots + 1);
            let total_pairs: u64 = c.pairs.iter().map(|l| l.len() as u64).sum();
            map_numeric_offs.push((keys_off, offsets_off, pairs_off));
            cursor = pairs_off + 12 * total_pairs;
        }
        // Integer values followed by their explicit presence bitmap.
        let mut integer_offs: Vec<u64> = Vec::with_capacity(self.integers.len());
        for _ in &self.integers {
            integer_offs.push(cursor);
            cursor += 8 * n_slots + n_slots.div_ceil(8);
        }
        // Geo sections follow the integer groups.
        let mut geo_offs: Vec<u64> = Vec::with_capacity(self.geos.len());
        for _ in &self.geos {
            geo_offs.push(cursor);
            cursor += 16 * n_slots;
        }
        // (field id, section_off, total) per positional field, last in
        // table order — identical geometry to Bm25Store::write_v6_to.
        let mut positions_offs: Vec<(usize, u64, u64)> = Vec::new();
        for (fi, stage) in position_stages.iter().enumerate() {
            let Some(stage) = stage else {
                continue;
            };
            positions_offs.push((fi, cursor, stage.total));
            cursor += positions_section_size(directories[fi].len() as u64, stage.total);
        }
        // Kind-8 sentence sections after the positions sections, from
        // the stages the ingest filled — same geometry as the heap
        // writer (docs/highlighting.md).
        let n_slots_u64 = self.fields[0].doc_lengths.len() as u64;
        let mut sentences_offs: Vec<(usize, u64, u64)> = Vec::new();
        for (fi, field) in self.fields.iter().enumerate() {
            if !field.sentences {
                continue;
            }
            let total = field.sentence_stage.as_ref().map_or(0, |s| s.total);
            sentences_offs.push((fi, cursor, total));
            cursor += sentences_section_size(n_slots_u64, total);
        }

        let mut unsigned_offs = Vec::with_capacity(self.unsigned_integers.len());
        for _ in &self.unsigned_integers {
            unsigned_offs.push(cursor);
            cursor += 8 * n_slots + n_slots.div_ceil(8);
        }
        let map_integer_sections =
            integer_map_sections(&self.map_integers, n_slots as u32, &mut cursor)?;
        let map_unsigned_sections =
            integer_map_sections(&self.map_unsigned_integers, n_slots as u32, &mut cursor)?;
        let source_section = if self.sources.is_empty() {
            None
        } else {
            Some((cursor, self.sources.encoded_len(n_slots as u32)?))
        };
        let tmp = path.with_extension("bm25tmp");
        {
            let mut w = io::BufWriter::new(std::fs::File::create(&tmp)?);
            w.write_all(if has_columns { MAGIC_V7 } else { MAGIC_V6 })?;
            write_u32(&mut w, self.fields.len() as u32)?;
            write_u32(&mut w, n_slots as u32)?;
            write_u64(&mut w, texts_off)?;
            write_u64(&mut w, text_index_off)?;
            write_u64(&mut w, lineages_off)?;
            for (field, &(dl_off, p_off, d_off)) in self.fields.iter().zip(&section_offs) {
                write_u16(&mut w, field.name.len() as u16)?;
                w.write_all(field.name.as_bytes())?;
                write_u64(&mut w, field.analysis_fingerprint)?;
                write_u64(&mut w, field.total_length)?;
                write_u64(&mut w, dl_off)?;
                write_u64(&mut w, p_off)?;
                write_u64(&mut w, d_off)?;
            }
            if has_columns {
                write_u32(
                    &mut w,
                    (self.facets.len()
                        + self.numerics.len()
                        + self.map_facets.len()
                        + self.map_numerics.len()
                        + self.map_integers.len()
                        + self.map_unsigned_integers.len()
                        + self.integers.len()
                        + self.unsigned_integers.len()
                        + self.geos.len()
                        + positions_offs.len()
                        + sentences_offs.len()
                        + usize::from(self.binding.is_some())
                        + usize::from(self.derived.is_some())
                        + usize::from(!self.sources.is_empty())) as u32,
                )?;
                for (facet, &(dict_off, ords_off)) in self.facets.iter().zip(&facet_offs) {
                    write_u16(&mut w, facet.name.len() as u16)?;
                    w.write_all(facet.name.as_bytes())?;
                    w.write_all(&[COLUMN_KIND_FACET])?;
                    write_u32(&mut w, facet.dict.len() as u32)?;
                    write_u64(&mut w, dict_off)?;
                    write_u64(&mut w, ords_off)?;
                }
                for (numeric, &vals_off) in self.numerics.iter().zip(&numeric_offs) {
                    let (min, max) = numeric.min_max();
                    write_u16(&mut w, numeric.name.len() as u16)?;
                    w.write_all(numeric.name.as_bytes())?;
                    w.write_all(&[COLUMN_KIND_F64])?;
                    write_u64(&mut w, min.to_bits())?;
                    write_u64(&mut w, max.to_bits())?;
                    write_u64(&mut w, vals_off)?;
                }
                for (c, &(keys_off, values_off, offsets_off, pairs_off)) in
                    self.map_facets.iter().zip(&map_facet_offs)
                {
                    write_u16(&mut w, c.name.len() as u16)?;
                    w.write_all(c.name.as_bytes())?;
                    w.write_all(&[COLUMN_KIND_MAP_FACET])?;
                    write_u32(&mut w, c.keys.len() as u32)?;
                    write_u32(&mut w, c.values.len() as u32)?;
                    write_u64(&mut w, keys_off)?;
                    write_u64(&mut w, values_off)?;
                    write_u64(&mut w, offsets_off)?;
                    write_u64(&mut w, pairs_off)?;
                }
                for (c, &(keys_off, offsets_off, pairs_off)) in
                    self.map_numerics.iter().zip(&map_numeric_offs)
                {
                    write_u16(&mut w, c.name.len() as u16)?;
                    w.write_all(c.name.as_bytes())?;
                    w.write_all(&[COLUMN_KIND_MAP_F64])?;
                    write_u32(&mut w, c.keys.len() as u32)?;
                    write_u64(&mut w, keys_off)?;
                    write_u64(&mut w, offsets_off)?;
                    write_u64(&mut w, pairs_off)?;
                }
                for (c, &vals_off) in self.integers.iter().zip(&integer_offs) {
                    let (min, max) = c.min_max();
                    write_u16(&mut w, c.name.len() as u16)?;
                    w.write_all(c.name.as_bytes())?;
                    w.write_all(&[COLUMN_KIND_I64_PRESENT])?;
                    write_u64(&mut w, min as u64)?;
                    write_u64(&mut w, max as u64)?;
                    write_u64(&mut w, vals_off)?;
                }
                for (c, &vals_off) in self.geos.iter().zip(&geo_offs) {
                    let (min_lat, max_lat, min_lon, max_lon) = c.bbox();
                    write_u16(&mut w, c.name.len() as u16)?;
                    w.write_all(c.name.as_bytes())?;
                    w.write_all(&[COLUMN_KIND_GEO])?;
                    write_u64(&mut w, min_lat.to_bits())?;
                    write_u64(&mut w, max_lat.to_bits())?;
                    write_u64(&mut w, min_lon.to_bits())?;
                    write_u64(&mut w, max_lon.to_bits())?;
                    write_u64(&mut w, vals_off)?;
                }
                for &(fi, off, total) in &positions_offs {
                    let name = format!("{POSITIONS_ENTRY_PREFIX}{}", self.fields[fi].name);
                    write_u16(&mut w, name.len() as u16)?;
                    w.write_all(name.as_bytes())?;
                    w.write_all(&[COLUMN_KIND_POSITIONS])?;
                    write_u64(&mut w, off)?;
                    write_u64(&mut w, total)?;
                }
                for &(fi, off, total) in &sentences_offs {
                    let name = format!("{SENTENCES_ENTRY_PREFIX}{}", self.fields[fi].name);
                    write_u16(&mut w, name.len() as u16)?;
                    w.write_all(name.as_bytes())?;
                    w.write_all(&[COLUMN_KIND_SENTENCES])?;
                    write_u64(&mut w, off)?;
                    write_u64(&mut w, total)?;
                }
                for (column, &off) in self.unsigned_integers.iter().zip(&unsigned_offs) {
                    let (min, max) = column.min_max();
                    write_u16(&mut w, column.name.len() as u16)?;
                    w.write_all(column.name.as_bytes())?;
                    w.write_all(&[COLUMN_KIND_U64])?;
                    write_u64(&mut w, min)?;
                    write_u64(&mut w, max)?;
                    write_u64(&mut w, off)?;
                }
                write_integer_map_entries(
                    &mut w,
                    &self.map_integers,
                    &map_integer_sections,
                    COLUMN_KIND_MAP_I64,
                )?;
                write_integer_map_entries(
                    &mut w,
                    &self.map_unsigned_integers,
                    &map_unsigned_sections,
                    COLUMN_KIND_MAP_U64,
                )?;
                write_binding_entry(&mut w, self.binding.as_ref())?;
                write_derived_entry(&mut w, self.derived.as_ref())?;
                write_source_entry(&mut w, source_section)?;
            }
            // texts: byte-copy of the spill (already section-encoded).
            let mut spill = std::fs::File::open(self.dir.join("texts.spill"))?;
            io::copy(&mut spill, &mut w)?;
            // text_index, from the in-memory length table; entries are
            // relative to the texts section start (v6).
            let mut tcur = 0u64;
            for &len in &self.text_lens {
                if len == u32::MAX {
                    write_u64(&mut w, 0)?;
                    write_u32(&mut w, u32::MAX)?;
                    tcur += 4;
                } else {
                    write_u64(&mut w, tcur + 4)?;
                    write_u32(&mut w, len)?;
                    tcur += 4 + len as u64;
                }
            }
            for lineage in &self.lineages {
                match lineage {
                    Some(l) => {
                        w.write_all(&[1u8])?;
                        write_u64(&mut w, l.parent_id)?;
                        write_u64(&mut w, l.group_id)?;
                        write_u32(&mut w, l.span_start)?;
                        write_u32(&mut w, l.span_end)?;
                    }
                    None => w.write_all(&[0u8])?,
                }
            }
            for (fi, field) in self.fields.iter().enumerate() {
                for &len in &field.doc_lengths {
                    write_u32(&mut w, len)?;
                }
                write_u32(&mut w, directories[fi].len() as u32)?;
                let mut body = std::fs::File::open(&body_paths[fi])?;
                io::copy(&mut body, &mut w)?;
                write_u32(&mut w, directories[fi].len() as u32)?;
                let mut blob_off = 0u64; // relative to the term blob start
                for (term, doc_rel, skip_rel, occ_rel, df) in &directories[fi] {
                    // Section-relative run offsets: past the section's
                    // leading u32 n_terms.
                    write_u64(&mut w, 4 + doc_rel)?;
                    write_u64(&mut w, 4 + skip_rel)?;
                    write_u64(&mut w, 4 + occ_rel)?;
                    write_u32(&mut w, *df)?;
                    write_u32(
                        &mut w,
                        u32::try_from(blob_off).expect("term blob exceeds u32"),
                    )?;
                    write_u16(&mut w, term.len() as u16)?;
                    blob_off += term.len() as u64;
                }
                for (term, ..) in &directories[fi] {
                    w.write_all(term.as_bytes())?;
                }
            }
            for facet in &self.facets {
                write_facet_column(&mut w, facet, n_slots as usize, true)?;
            }
            for numeric in &self.numerics {
                for slot in 0..n_slots as usize {
                    write_u64(
                        &mut w,
                        numeric
                            .vals
                            .get(slot)
                            .copied()
                            .unwrap_or(f64::NAN)
                            .to_bits(),
                    )?;
                }
            }
            for c in &self.map_facets {
                write_map_facet_column(&mut w, c, n_slots as usize, true)?;
            }
            for c in &self.map_numerics {
                write_map_numeric_column(&mut w, c, n_slots as usize, true)?;
            }
            for c in &self.integers {
                c.write(&mut w, n_slots as usize)?;
            }
            for c in &self.geos {
                for slot in 0..n_slots as usize {
                    let (lat, lon) = c.vals.get(slot).copied().unwrap_or(GEO_ABSENT);
                    write_u64(&mut w, lat.to_bits())?;
                    write_u64(&mut w, lon.to_bits())?;
                }
            }
            for &(fi, _, total) in &positions_offs {
                let stage = position_stages[fi]
                    .as_ref()
                    .expect("positional field has a stage");
                write_u32(&mut w, directories[fi].len() as u32)?;
                debug_assert_eq!(stage.bases.len(), directories[fi].len());
                for &base in &stage.bases {
                    write_u32(&mut w, base)?;
                }
                write_u32(&mut w, u32::try_from(total).expect("positions exceed u32"))?;
                let mut ordinals = std::fs::File::open(&stage.path)?;
                io::copy(&mut ordinals, &mut w)?;
            }
            for &(fi, _, total) in &sentences_offs {
                write_u32(&mut w, n_slots_u64 as u32)?;
                match self.fields[fi].sentence_stage.as_mut() {
                    Some(stage) => {
                        debug_assert_eq!(stage.bases.len() as u64, n_slots_u64);
                        for &base in &stage.bases {
                            write_u32(&mut w, base)?;
                        }
                        write_u32(&mut w, u32::try_from(total).expect("sentences exceed u32"))?;
                        stage.writer.flush()?;
                        let mut spans = std::fs::File::open(&stage.path)?;
                        io::copy(&mut spans, &mut w)?;
                    }
                    None => {
                        // Declared, no document arrived: an empty table.
                        for _ in 0..=n_slots_u64 {
                            write_u32(&mut w, 0)?;
                        }
                    }
                }
            }
            for column in &self.unsigned_integers {
                column.write(&mut w, n_slots as usize)?;
            }
            for column in &self.map_integers {
                column.write(&mut w, n_slots as u32)?;
            }
            for column in &self.map_unsigned_integers {
                column.write(&mut w, n_slots as u32)?;
            }
            if source_section.is_some() {
                self.sources.write(&mut w, n_slots as u32)?;
            }
            w.flush()?;
        }
        finalize_v8(&tmp)?;
        std::fs::rename(&tmp, path)?;
        fsync_parent(path)?;
        std::fs::remove_dir_all(&self.dir)?;
        Ok(())
    }

    /// Merge the runs and assemble the v4 file at `path` (the pre-v5
    /// behavior, kept for benchmarking and migration checks).
    pub(crate) fn finish_v4(&mut self, path: &Path) -> io::Result<()> {
        if !self.sources.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "legacy writer cannot preserve protobuf sources",
            ));
        }
        if self.fields[0].positions {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "the v4 oracle format cannot carry token positions",
            ));
        }
        if self.fields[0].sentences {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "the v4 oracle format cannot carry sentence spans",
            ));
        }
        self.spill_run()?;
        self.texts.flush()?;

        // Pass 1: merge runs into the postings section body, collecting
        // the directory (term, absolute offset comes later, df).
        let postings_body = self.dir.join("postings.body");
        let mut directory: Vec<(String, u64, u32)> = Vec::new();
        {
            let mut out = io::BufWriter::new(std::fs::File::create(&postings_body)?);
            let mut heads: Vec<RunHead> = Vec::new();
            for run in &self.fields[0].runs {
                if let Some(head) = RunHead::open(run, false)? {
                    heads.push(head);
                }
            }
            let mut cursor = 0u64;
            while !heads.is_empty() {
                // Smallest term across run heads; runs are time-ordered,
                // and ids only grow, so draining matching heads in run
                // order keeps postings doc-ascending.
                let term = heads
                    .iter()
                    .map(|h| h.term.clone())
                    .min()
                    .expect("nonempty heads");
                let df: u32 = heads
                    .iter()
                    .filter(|h| h.term == term)
                    .map(|h| h.n_postings)
                    .sum();
                write_str(&mut out, &term)?;
                write_u32(&mut out, df)?;
                directory.push((term.clone(), cursor, df));
                cursor += 4 + term.len() as u64 + 4;
                let mut idx = 0;
                while idx < heads.len() {
                    if heads[idx].term == term {
                        cursor += heads[idx].copy_postings(&mut out)?;
                        if !heads[idx].advance()? {
                            heads.remove(idx);
                            continue;
                        }
                    }
                    idx += 1;
                }
            }
            out.flush()?;
        }
        let postings_body_len = std::fs::metadata(&postings_body)?.len();

        // Section geometry, identical to Bm25Store::write_to.
        let field = &self.fields[0];
        let n_slots = field.doc_lengths.len() as u64;
        let header_size = 8 + 8 + 8 * 4 + 4;
        let doc_lengths_off = header_size as u64;
        let texts_off = doc_lengths_off + 4 * n_slots;
        let text_index_off = texts_off + self.texts_bytes;
        let lineages_off = text_index_off + 12 * n_slots;
        let lineages_size: u64 = self
            .lineages
            .iter()
            .map(|l| if l.is_some() { 25 } else { 1 })
            .sum();
        let postings_off = lineages_off + lineages_size;
        let directory_off = postings_off + 4 + postings_body_len;

        let tmp = path.with_extension("bm25tmp");
        {
            let mut w = io::BufWriter::new(std::fs::File::create(&tmp)?);
            w.write_all(MAGIC_V4)?;
            write_u64(&mut w, field.total_length)?;
            write_u64(&mut w, texts_off)?;
            write_u64(&mut w, lineages_off)?;
            write_u64(&mut w, postings_off)?;
            write_u64(&mut w, directory_off)?;
            write_u32(&mut w, field.doc_lengths.len() as u32)?;
            for &len in &field.doc_lengths {
                write_u32(&mut w, len)?;
            }
            // texts: byte-copy of the spill (already section-encoded).
            let mut spill = std::fs::File::open(self.dir.join("texts.spill"))?;
            io::copy(&mut spill, &mut w)?;
            // text_index, from the in-memory length table.
            let mut cursor = texts_off;
            for &len in &self.text_lens {
                if len == u32::MAX {
                    write_u64(&mut w, 0)?;
                    write_u32(&mut w, u32::MAX)?;
                    cursor += 4;
                } else {
                    write_u64(&mut w, cursor + 4)?;
                    write_u32(&mut w, len)?;
                    cursor += 4 + len as u64;
                }
            }
            for lineage in &self.lineages {
                match lineage {
                    Some(l) => {
                        w.write_all(&[1u8])?;
                        write_u64(&mut w, l.parent_id)?;
                        write_u64(&mut w, l.group_id)?;
                        write_u32(&mut w, l.span_start)?;
                        write_u32(&mut w, l.span_end)?;
                    }
                    None => w.write_all(&[0u8])?,
                }
            }
            write_u32(&mut w, directory.len() as u32)?;
            let mut body = std::fs::File::open(&postings_body)?;
            io::copy(&mut body, &mut w)?;
            write_u32(&mut w, directory.len() as u32)?;
            let mut blob_off = 0u64; // relative to the term blob start (v4)
            for (term, rel_off, df) in &directory {
                write_u64(&mut w, postings_off + 4 + rel_off)?;
                write_u32(&mut w, *df)?;
                write_u32(
                    &mut w,
                    u32::try_from(blob_off).expect("term blob exceeds u32"),
                )?;
                write_u16(&mut w, term.len() as u16)?;
                blob_off += term.len() as u64;
            }
            for (term, _, _) in &directory {
                w.write_all(term.as_bytes())?;
            }
            w.flush()?;
            w.get_ref().sync_all()?;
        }
        std::fs::rename(&tmp, path)?;
        fsync_parent(path)?;
        std::fs::remove_dir_all(&self.dir)?;
        Ok(())
    }
}

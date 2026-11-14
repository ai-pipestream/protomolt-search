//! Bounded-memory disk-spilling `.bm25` builder (`SpillBuilder`).

mod merge;

use std::io::{self, Write};
use std::path::{Path, PathBuf};

use super::columns::{
    validate_facet_names, FacetStore, GeoStore, IntStore, MapFacetStore, MapNumericStore,
    NumericStore, StoredBinding, StoredDerived, UintStore,
};
use super::format::{write_u16, write_u32};
#[cfg(doc)]
use super::store::FieldStore;
#[cfg(doc)]
use super::Bm25Store;
use super::{AnalyzedDoc, DocLineage};

type PendingPosting = (String, u32, u32, Vec<(u32, u32)>, Vec<u32>, Vec<u32>);
type PostingDirectoryEntry = (String, u64, u64, u64, u32);

/// One field's slice of a [`SpillBuilder`]: its pending postings
/// buffer, spilled runs, and per-document lengths — the spill-side
/// mirror of [`FieldStore`].
struct FieldSpill {
    name: String,
    /// Hash of the field's AnalysisSpec, persisted in the v6 field
    /// table. 0 until the ingest layer wires real fingerprints.
    analysis_fingerprint: u64,
    /// Pending postings: (term, doc_id, tf, offsets, positions, pos_lengths).
    buf: Vec<PendingPosting>,
    runs: Vec<PathBuf>,
    doc_lengths: Vec<u32>,
    total_length: u64,
    /// Whether this field keeps token positions (the spill-side mirror
    /// of `FieldStore::positions`): runs then carry one ordinal per
    /// occurrence after the offset pairs, and `finish` writes the
    /// field's kind-7 section.
    positions: bool,
    has_lattice: bool,
    splade: Option<crate::turbo_bridge::splade::SpladeFieldIndex>,
    /// Whether this field keeps sentence spans (the spill-side mirror of
    /// `FieldStore::sentences`): spans stream to a stage file as
    /// documents arrive, and `finish` writes the field's kind-8 section.
    sentences: bool,
    /// The stage, opened on the first document (`docs/highlighting.md`).
    sentence_stage: Option<SentencesStage>,
}

/// The ingest-time stage of one sentence field's kind-8 section: spans
/// stream to a file in slot order as documents arrive, and the per-slot
/// bases accumulate in heap (4 B per slot, the doc-length table's own
/// budget).
struct SentencesStage {
    writer: io::BufWriter<std::fs::File>,
    path: PathBuf,
    bases: Vec<u32>,
    total: u64,
}

impl FieldSpill {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            analysis_fingerprint: 0,
            buf: Vec::new(),
            runs: Vec::new(),
            doc_lengths: Vec::new(),
            total_length: 0,
            positions: false,
            has_lattice: false,
            splade: None,
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
    dir: PathBuf,
    /// Per-field state, field-id order; never empty. Field 0 is the
    /// body.
    fields: Vec<FieldSpill>,
    /// Total bytes buffered across every field's `buf`.
    buf_bytes: usize,
    cap_bytes: usize,
    /// Texts spill, encoded exactly as the v3 texts section.
    texts: io::BufWriter<std::fs::File>,
    texts_bytes: u64,
    /// Per-slot text byte length (`u32::MAX` = absent); sizes the final
    /// sections without rereading the spill.
    text_lens: Vec<u32>,
    lineages: Vec<Option<DocLineage>>,
    /// Documents with postings in any field.
    doc_count: u64,
    /// Facet columns in facet-id order (dict + per-slot ordinals stay
    /// in heap: kilobytes of dictionary plus 4 B per slot, never the
    /// memory ceiling the spill exists for). Non-empty makes `finish`
    /// write v7.
    facets: Vec<FacetStore>,
    /// Numeric columns in numeric-id order (8 B per slot in heap, same
    /// argument as `facets`). Non-empty makes `finish` write v7.
    numerics: Vec<NumericStore>,
    /// map<string, string> columns (dictionaries plus per-doc pair
    /// lists in heap — bytes per entry, never the spill's memory
    /// ceiling). Non-empty makes `finish` write v7.
    map_facets: Vec<MapFacetStore>,
    /// map<string, f64> columns. Non-empty makes `finish` write v7.
    map_numerics: Vec<MapNumericStore>,
    map_integers: Vec<crate::integer_map::Store<i64>>,
    map_unsigned_integers: Vec<crate::integer_map::Store<u64>>,
    /// i64 columns (8 B per slot in heap, same argument as `numerics`).
    /// Non-empty makes `finish` write v7.
    integers: Vec<IntStore>,
    unsigned_integers: Vec<UintStore>,
    /// Geo-point columns (16 B per slot in heap). Non-empty makes
    /// `finish` write v7.
    geos: Vec<GeoStore>,
    /// The mapped-plan binding, persisted as the kind-6 table entry
    /// when `finish` writes v7 (`Some` forces v7). The v4 oracle
    /// format cannot carry it and refuses.
    binding: Option<StoredBinding>,
    /// The derived-column declaration `finish` persists as the kind-15
    /// entry (`Some` forces v7).
    derived: Option<StoredDerived>,
    sources: crate::source_archive::SourceArchive,
    /// Write the v4 format instead of v6 (benchmarking/migration only).
    v4_only: bool,
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

    fn create_format(dir: &Path, names: &[&str], v4_only: bool) -> io::Result<Self> {
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
            if !analyzed.splade.is_empty() {
                field
                    .splade
                    .get_or_insert_with(crate::turbo_bridge::splade::SpladeFieldIndex::default)
                    .insert_doc(doc_id, analyzed.splade_scale_bits, &analyzed.splade);
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
                self.buf_bytes += term.len()
                    + 24
                    + 16 * offsets.len()
                    + 4 * positions.len()
                    + 4 * pos_lengths.len();
                field
                    .buf
                    .push((term, doc_id, tf, offsets, positions, pos_lengths));
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
    fn spill_run(&mut self) -> io::Result<()> {
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
                for (_, doc_id, tf, offsets, positions, pos_lengths) in &field.buf[i..group_end] {
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
                        if pos_lengths.is_empty() {
                            w.write_all(&[0u8])?;
                        } else {
                            debug_assert_eq!(pos_lengths.len(), offsets.len());
                            w.write_all(&[1u8])?;
                            for &len in pos_lengths {
                                write_u32(&mut w, len)?;
                            }
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
}

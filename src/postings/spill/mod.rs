//! Bounded-memory disk-spilling `.bm25` builder (`SpillBuilder`).

mod merge;

use std::io::{self, Write};
use std::path::{Path, PathBuf};

use super::columns::{
    merge_analysis_fingerprint, validate_text_field_names, ColumnKind, ColumnSet, StoredBinding,
    StoredDerived,
};
use super::format::{write_u16, write_u32};
#[cfg(doc)]
use super::store::FieldStore;
#[cfg(doc)]
use super::Bm25Store;
use super::{clone_parallel_ordinals, AnalyzedDoc, AnalyzedField, DocLineage};

type PendingPosting = (String, u32, u32, Vec<(u32, u32)>, Vec<u32>, Vec<u32>);
type PostingDirectoryEntry = (String, u64, u64, u64, u32);

/// One field's slice of a [`SpillBuilder`]: its pending postings
/// buffer, spilled runs, and per-document lengths — the spill-side
/// mirror of [`FieldStore`].
struct FieldSpill {
    name: String,
    /// Hash of the field's `AnalysisSpec`, persisted in the v6 field table.
    analysis_fingerprint: u64,
    /// Pending postings: `(term, doc_id, tf, offsets, positions, pos_lengths)`.
    buf: Vec<PendingPosting>,
    runs: Vec<PathBuf>,
    doc_lengths: Vec<u32>,
    total_length: u64,
    positions: bool,
    has_lattice: bool,
    splade: Option<crate::turbo_bridge::splade::SpladeFieldIndex>,
    sentences: bool,
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

/// Disk-spilling builder producing the SAME v8-framed v6/v7 file as
/// [`Bm25Store::save`] (byte-identical), with bounded memory at any corpus size.
pub struct SpillBuilder {
    dir: PathBuf,
    /// Per-field state, field-id order; never empty. Field 0 is the body.
    fields: Vec<FieldSpill>,
    /// Total bytes buffered across every field's `buf`.
    buf_bytes: usize,
    cap_bytes: usize,
    /// Texts spill (`u32 len` + UTF-8 bytes per slot; `u32::MAX` = absent).
    texts: io::BufWriter<std::fs::File>,
    texts_bytes: u64,
    /// Per-slot text byte length (`u32::MAX` = absent).
    text_lens: Vec<u32>,
    lineages: Vec<Option<DocLineage>>,
    /// Documents with postings in any field.
    doc_count: u64,
    /// Per-document typed column tables and inline binding/derived metadata.
    columns: ColumnSet,
    sources: crate::source_archive::SourceArchive,
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

    /// Create a single-field ("body") builder spilling into `dir`.
    pub fn create(dir: &Path) -> io::Result<Self> {
        Self::create_with_fields(dir, &["body"])
    }

    /// Create a builder with the given field table; same contract as
    /// [`Bm25Store::with_fields`].
    pub fn create_with_fields(dir: &Path, names: &[&str]) -> io::Result<Self> {
        validate_text_field_names(names, "builder");
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
            columns: ColumnSet::default(),
            sources: crate::source_archive::SourceArchive::spilling(&dir.join("sources.spill"))?,
        })
    }

    /// Unified text-field table constructor matching [`Bm25Store::with_text_fields`].
    pub fn create_with_text_fields(dir: &Path, names: &[&str]) -> io::Result<Self> {
        Self::create_with_fields(dir, names)
    }

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

    /// Declare the fields that keep sentence spans per document (`docs/highlighting.md`),
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

    /// Override the sort-buffer capacity (tests force multi-run merges with tiny caps).
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

    /// Record field `f`'s analyzer fingerprint, or refuse if it contradicts
    /// what the field already holds.
    pub fn set_analysis_fingerprint(&mut self, f: usize, fingerprint: u64) -> Result<(), String> {
        let field = &mut self.fields[f];
        merge_analysis_fingerprint(&mut field.analysis_fingerprint, &field.name, fingerprint)
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
        self.advance_to_slot(slot, &text, lineage)?;
        let mut lengths = vec![0u32; self.fields.len()];
        for (fi, analyzed) in doc.fields.into_iter().enumerate() {
            lengths[fi] = analyzed.length;
            self.buf_bytes += Self::append_analyzed_field(&mut self.fields[fi], doc_id, analyzed)?;
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

    fn advance_to_slot(
        &mut self,
        slot: usize,
        text: &str,
        lineage: Option<DocLineage>,
    ) -> io::Result<()> {
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
        for field in &mut self.fields {
            if let Some(stage) = field.sentence_stage.as_mut() {
                stage
                    .bases
                    .push(u32::try_from(stage.total).expect("sentences exceed u32"));
            }
        }
        Ok(())
    }

    fn append_analyzed_field(
        field: &mut FieldSpill,
        doc_id: u32,
        analyzed: AnalyzedField,
    ) -> io::Result<usize> {
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
        let mut added_bytes = 0usize;
        for (ti, (term, tf, offsets)) in analyzed.terms.into_iter().enumerate() {
            let positions = clone_parallel_ordinals(&positions, ti, offsets.len(), "positions");
            let pos_lengths =
                clone_parallel_ordinals(&pos_lengths, ti, offsets.len(), "pos_lengths");
            added_bytes +=
                term.len() + 24 + 16 * offsets.len() + 4 * positions.len() + 4 * pos_lengths.len();
            field
                .buf
                .push((term, doc_id, tf, offsets, positions, pos_lengths));
        }
        Ok(added_bytes)
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

    /// The mapped-plan binding `finish` will persist, if any.
    pub fn binding(&self) -> Option<&StoredBinding> {
        self.columns.binding.as_ref()
    }

    /// Set the binding `finish` persists (see [`Bm25Store::set_binding`]).
    pub fn set_binding(&mut self, binding: Option<StoredBinding>) {
        self.columns.binding = binding;
    }

    /// The derived-column declaration `finish` will persist, if any.
    pub fn derived(&self) -> Option<&StoredDerived> {
        self.columns.derived.as_ref()
    }

    /// Set the declaration `finish` persists (see [`Bm25Store::set_derived`]).
    pub fn set_derived(&mut self, derived: Option<StoredDerived>) {
        self.columns.derived = derived;
    }
}

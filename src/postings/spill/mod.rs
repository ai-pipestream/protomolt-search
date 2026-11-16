//! Bounded-memory disk-spilling `.bm25` builder (`SpillBuilder`).

mod ingest;
mod merge;
mod run_head;
mod schema;

use std::io;
use std::path::{Path, PathBuf};

use super::columns::{
    merge_analysis_fingerprint, validate_text_field_names, ColumnSet, StoredBinding, StoredDerived,
};
#[cfg(doc)]
use super::store::FieldStore;
#[cfg(doc)]
use super::Bm25Store;
use super::DocLineage;

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
        Self::create_with_text_fields(dir, &["body"])
    }

    /// Create a builder with the given field table; same contract as
    /// [`Bm25Store::with_text_fields`].
    pub fn create_with_text_fields(dir: &Path, names: &[&str]) -> io::Result<Self> {
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

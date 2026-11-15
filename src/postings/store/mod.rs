//! Heap-resident BM25 postings and column builder (`Bm25Store`).

mod columns;
mod ingest;
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
use super::{DocLineage, Posting};

/// One field's slice of the store: its own postings, per-document
/// lengths, and running total. Structurally a complete single-field
/// BM25 index over the shared slot space.
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
    /// a range walk here and a binary search there.
    pub(in crate::postings) postings: std::collections::BTreeMap<String, Vec<Posting>>,
    /// Per-document length in terms, indexed by local doc id. Sparse
    /// slots (ids consumed by the vector side) hold 0. Every field's
    /// table has the same length (the shared slot count).
    pub(in crate::postings) doc_lengths: Vec<u32>,
    /// Sum of this field's document lengths (for avgdl).
    pub(in crate::postings) total_length: u64,
    /// Whether this field keeps token positions per occurrence.
    /// Declared at construction (or read
    /// from the file's kind-7 entry); a positional field refuses a
    /// document without positions instead of storing a hole.
    positions: bool,
    /// Whether at least one document in this positional field carried
    /// token-lattice arc lengths (`COLUMN_KIND_POS_LENGTHS`).
    has_lattice: bool,
    /// Whether this field keeps sentence spans per document,
    /// declared at construction or read from
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

    /// Declare which fields keep token positions.
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

    /// Declare the fields that keep sentence spans per document.
    /// Must precede any document, and every
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
}

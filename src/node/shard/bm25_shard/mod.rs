//! Storage-shape dispatch and field/column accessors for [`Bm25Shard`].

use crate::node::*;

mod chain;
mod map_columns;
mod scalar_columns;

/// The BM25 half's two storage shapes: the heap builder used during
/// ingest, and the disk-resident mmap reader used after Flush and on
/// startup. Once resident, a shard holds no postings or document texts
/// in heap — only the small per-doc tables.
#[allow(clippy::large_enum_variant)]
pub enum Bm25Shard {
    /// Heap builder (small or append ingests; searchable mid-build).
    Building(Bm25Store),
    /// Disk-spilling bulk builder (fresh persisted shards): bounded heap,
    /// NOT searchable until flushed.
    Spilling(SpillBuilder),
    /// Disk-resident mmap reader over a saved `.bm25` file.
    Resident(Bm25Reader),
    /// Segment catalog plus searchable heap tail (default persisted layout).
    Segmented(SegmentedShard),
}

impl Bm25Shard {
    /// The searchable read surface; `None` while bulk-building (a spill
    /// builder cannot answer term lookups without scanning every run).
    pub(in crate::node) fn as_index(&self) -> Option<&dyn Bm25Index> {
        match self {
            Bm25Shard::Building(s) => Some(s),
            Bm25Shard::Spilling(_) => None,
            Bm25Shard::Resident(r) => Some(r),
            Bm25Shard::Segmented(g) => Some(g),
        }
    }

    pub(crate) fn next_doc_id(&self) -> u32 {
        match self {
            Bm25Shard::Building(s) => s.next_doc_id(),
            Bm25Shard::Spilling(s) => s.next_doc_id(),
            Bm25Shard::Resident(r) => r.next_doc_id(),
            Bm25Shard::Segmented(g) => g.next_doc_id(),
        }
    }

    pub(in crate::node) fn doc_count(&self) -> u64 {
        match self {
            Bm25Shard::Building(s) => s.doc_count(),
            Bm25Shard::Spilling(s) => s.doc_count(),
            Bm25Shard::Resident(r) => Bm25Index::doc_count(r),
            Bm25Shard::Segmented(g) => Bm25Index::doc_count(g),
        }
    }

    /// Number of fields in the active table.
    pub(crate) fn field_count(&self) -> usize {
        match self {
            Bm25Shard::Building(s) => s.field_count(),
            Bm25Shard::Spilling(s) => s.field_count(),
            Bm25Shard::Resident(r) => r.field_count(),
            Bm25Shard::Segmented(g) => g.field_count(),
        }
    }

    /// The name of field `f` in the active table.
    pub(crate) fn field_name(&self, f: usize) -> &str {
        match self {
            Bm25Shard::Building(s) => s.field_name(f),
            Bm25Shard::Spilling(s) => s.field_name(f),
            Bm25Shard::Resident(r) => r.field_name(f),
            Bm25Shard::Segmented(g) => g.field_name(f),
        }
    }

    /// The mapped-plan binding persisted with this shard, if any.
    pub(crate) fn binding(&self) -> Option<&crate::postings::StoredBinding> {
        match self {
            Bm25Shard::Building(s) => s.binding(),
            Bm25Shard::Spilling(s) => s.binding(),
            Bm25Shard::Resident(r) => r.binding(),
            Bm25Shard::Segmented(g) => g.binding(),
        }
    }

    /// The derived-column declaration the store was written under.
    pub(crate) fn derived(&self) -> Option<&crate::postings::StoredDerived> {
        match self {
            Bm25Shard::Building(s) => s.derived(),
            Bm25Shard::Spilling(s) => s.derived(),
            Bm25Shard::Resident(r) => r.derived(),
            Bm25Shard::Segmented(g) => g.derived(),
        }
    }

    pub fn protobuf_source(
        &self,
        doc: u32,
    ) -> std::io::Result<Option<(crate::pb::ProtobufSource, Option<u32>)>> {
        match self {
            Bm25Shard::Building(s) => s.protobuf_source(doc),
            Bm25Shard::Spilling(s) => s.protobuf_source(doc),
            Bm25Shard::Resident(s) => s.protobuf_source(doc),
            Bm25Shard::Segmented(s) => s.protobuf_source(doc),
        }
    }

    /// Capture bindings from the state being scored, before releasing its lock.
    pub fn identity_snapshot(&self) -> Result<crate::source_archive::IdentitySnapshot, String> {
        match self {
            Bm25Shard::Building(s) => Ok(s.identity_snapshot()),
            Bm25Shard::Spilling(s) => Ok(s.identity_snapshot()),
            Bm25Shard::Resident(s) => Ok(s.identity_snapshot()),
            Bm25Shard::Segmented(s) => s.identity_snapshot(),
        }
    }

    pub fn document_identity(&self, doc: u32) -> Option<crate::pb::DocumentIdentity> {
        match self {
            Bm25Shard::Building(s) => s.document_identity(doc),
            Bm25Shard::Spilling(s) => s.document_identity(doc),
            Bm25Shard::Resident(s) => s.document_identity(doc),
            Bm25Shard::Segmented(s) => s.document_identity(doc),
        }
    }

    /// Field `f`'s analyzer fingerprint in the active table (`0` = unknown,
    /// which never enforces).
    pub(crate) fn analysis_fingerprint(&self, f: usize) -> u64 {
        match self {
            Bm25Shard::Building(s) => s.analysis_fingerprint(f),
            Bm25Shard::Spilling(s) => s.analysis_fingerprint(f),
            Bm25Shard::Resident(r) => r.analysis_fingerprint(f),
            Bm25Shard::Segmented(g) => g.analysis_fingerprint(f),
        }
    }

    /// Whether field `f` stores token positions in active storage.
    pub(in crate::node) fn field_has_positions(&self, f: usize) -> bool {
        match self {
            Bm25Shard::Building(s) => s.field_has_positions(f),
            Bm25Shard::Spilling(s) => s.field_has_positions(f),
            Bm25Shard::Resident(r) => r.field_has_positions(f),
            Bm25Shard::Segmented(g) => g.field_has_positions(f),
        }
    }

    /// Whether field `f` stores sentence spans in active storage.
    pub(in crate::node) fn field_has_sentences(&self, f: usize) -> bool {
        match self {
            Bm25Shard::Building(s) => s.field_has_sentences(f),
            Bm25Shard::Spilling(s) => s.field_has_sentences(f),
            Bm25Shard::Resident(r) => r.field_has_sentences(f),
            Bm25Shard::Segmented(g) => g.field_has_sentences(f),
        }
    }

    /// Record field `f`'s analyzer fingerprint, refusing a contradiction.
    /// A disk-resident shard is not being written to, so it has nothing
    /// to record.
    pub(in crate::node) fn set_analysis_fingerprint(
        &mut self,
        f: usize,
        fingerprint: u64,
    ) -> Result<(), String> {
        match self {
            Bm25Shard::Building(s) => s.set_analysis_fingerprint(f, fingerprint),
            Bm25Shard::Spilling(s) => s.set_analysis_fingerprint(f, fingerprint),
            Bm25Shard::Resident(_) => Ok(()),
            Bm25Shard::Segmented(g) => g.set_analysis_fingerprint(f, fingerprint),
        }
    }

    /// The table index of the field named `name`, if present. `None`
    /// while bulk-building (no searchable surface to resolve against).
    pub(in crate::node) fn field_index(&self, name: &str) -> Option<usize> {
        match self {
            Bm25Shard::Building(s) => s.field_index(name),
            Bm25Shard::Spilling(_) => None,
            Bm25Shard::Resident(r) => r.field_index(name),
            Bm25Shard::Segmented(g) => g.field_index(name),
        }
    }
    /// Field `f` as its own searchable [`Bm25Index`]; `None` while
    /// bulk-building, exactly like [`Self::as_index`].
    pub(in crate::node) fn field_view(&self, f: usize) -> Option<Box<dyn Bm25Index + '_>> {
        match self {
            Bm25Shard::Building(s) => Some(Box::new(s.field(f))),
            Bm25Shard::Spilling(_) => None,
            Bm25Shard::Resident(r) => Some(Box::new(r.field(f))),
            Bm25Shard::Segmented(g) => Some(Box::new(g.field(f))),
        }
    }

    /// [`Self::field_view`] restricted to sealed parts admitted by `mask` on a
    /// segmented shard (falling back to [`Self::field_view`] on other layouts).
    pub(in crate::node) fn field_view_masked(
        &self,
        f: usize,
        mask: &Arc<[bool]>,
    ) -> Option<Box<dyn Bm25Index + '_>> {
        match self {
            Bm25Shard::Segmented(g) => Some(Box::new(g.field_masked(f, Arc::clone(mask)))),
            _ => self.field_view(f),
        }
    }
    /// Open a saved `.bm25` file (`v6` through `v8`) as a disk-resident mmap reader.
    pub fn open(path: &std::path::Path) -> std::io::Result<Self> {
        Bm25Reader::open(path).map(Bm25Shard::Resident)
    }
}

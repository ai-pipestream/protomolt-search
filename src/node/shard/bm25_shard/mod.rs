//! Storage-shape dispatch and field/column accessors for [`Bm25Shard`].

use crate::node::*;

mod chain;
mod map_columns;
mod scalar_columns;

/// The BM25 half's two storage shapes: the heap store of a shard with no
/// index path, and the segment catalog plus heap tail of a persisted shard.
#[allow(clippy::large_enum_variant)]
pub enum Bm25Shard {
    /// Heap store of a shard with no index path; nothing reaches disk.
    Building(Bm25Store),
    /// Segment catalog plus searchable heap tail of a persisted shard.
    Segmented(SegmentedShard),
}

impl Bm25Shard {
    /// The searchable read surface.
    pub(in crate::node) fn as_index(&self) -> &dyn Bm25Index {
        match self {
            Bm25Shard::Building(s) => s,
            Bm25Shard::Segmented(g) => g,
        }
    }

    pub(crate) fn next_doc_id(&self) -> u32 {
        match self {
            Bm25Shard::Building(s) => s.next_doc_id(),
            Bm25Shard::Segmented(g) => g.next_doc_id(),
        }
    }

    pub(in crate::node) fn doc_count(&self) -> u64 {
        match self {
            Bm25Shard::Building(s) => s.doc_count(),
            Bm25Shard::Segmented(g) => Bm25Index::doc_count(g),
        }
    }

    /// Number of fields in the active table.
    pub(crate) fn field_count(&self) -> usize {
        match self {
            Bm25Shard::Building(s) => s.field_count(),
            Bm25Shard::Segmented(g) => g.field_count(),
        }
    }

    /// The name of field `f` in the active table.
    pub(crate) fn field_name(&self, f: usize) -> &str {
        match self {
            Bm25Shard::Building(s) => s.field_name(f),
            Bm25Shard::Segmented(g) => g.field_name(f),
        }
    }

    /// The mapped-plan binding persisted with this shard, if any.
    pub(crate) fn binding(&self) -> Option<&crate::postings::StoredBinding> {
        match self {
            Bm25Shard::Building(s) => s.binding(),
            Bm25Shard::Segmented(g) => g.binding(),
        }
    }

    /// The derived-column declaration the store was written under.
    pub(crate) fn derived(&self) -> Option<&crate::postings::StoredDerived> {
        match self {
            Bm25Shard::Building(s) => s.derived(),
            Bm25Shard::Segmented(g) => g.derived(),
        }
    }

    pub fn protobuf_source(
        &self,
        doc: u32,
    ) -> std::io::Result<Option<(crate::pb::ProtobufSource, Option<u32>)>> {
        match self {
            Bm25Shard::Building(s) => s.protobuf_source(doc),
            Bm25Shard::Segmented(s) => s.protobuf_source(doc),
        }
    }

    /// Capture bindings from the state being scored, before releasing its lock.
    pub fn identity_snapshot(&self) -> Result<crate::source_archive::IdentitySnapshot, String> {
        match self {
            Bm25Shard::Building(s) => Ok(s.identity_snapshot()),
            Bm25Shard::Segmented(s) => s.identity_snapshot(),
        }
    }

    pub fn document_identity(&self, doc: u32) -> Option<crate::pb::DocumentIdentity> {
        match self {
            Bm25Shard::Building(s) => s.document_identity(doc),
            Bm25Shard::Segmented(s) => s.document_identity(doc),
        }
    }

    /// Field `f`'s analyzer fingerprint in the active table (`0` = unknown,
    /// which never enforces).
    pub(crate) fn analysis_fingerprint(&self, f: usize) -> u64 {
        match self {
            Bm25Shard::Building(s) => s.analysis_fingerprint(f),
            Bm25Shard::Segmented(g) => g.analysis_fingerprint(f),
        }
    }

    /// Whether field `f` stores token positions in active storage.
    pub(in crate::node) fn field_has_positions(&self, f: usize) -> bool {
        match self {
            Bm25Shard::Building(s) => s.field_has_positions(f),
            Bm25Shard::Segmented(g) => g.field_has_positions(f),
        }
    }

    /// Whether field `f` stores sentence spans in active storage.
    pub(in crate::node) fn field_has_sentences(&self, f: usize) -> bool {
        match self {
            Bm25Shard::Building(s) => s.field_has_sentences(f),
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
            Bm25Shard::Segmented(g) => g.set_analysis_fingerprint(f, fingerprint),
        }
    }

    /// The table index of the field named `name`, if present.
    pub(in crate::node) fn field_index(&self, name: &str) -> Option<usize> {
        match self {
            Bm25Shard::Building(s) => s.field_index(name),
            Bm25Shard::Segmented(g) => g.field_index(name),
        }
    }
    /// Field `f` as its own searchable [`Bm25Index`].
    pub(in crate::node) fn field_view(&self, f: usize) -> Box<dyn Bm25Index + '_> {
        match self {
            Bm25Shard::Building(s) => Box::new(s.field(f)),
            Bm25Shard::Segmented(g) => Box::new(g.field(f)),
        }
    }

    /// [`Self::field_view`] restricted to sealed parts admitted by `mask` on a
    /// segmented shard; a heap store has no sealed parts to mask.
    pub(in crate::node) fn field_view_masked(
        &self,
        f: usize,
        mask: &Arc<[bool]>,
    ) -> Box<dyn Bm25Index + '_> {
        match self {
            Bm25Shard::Segmented(g) => Box::new(g.field_masked(f, Arc::clone(mask))),
            Bm25Shard::Building(_) => self.field_view(f),
        }
    }
}

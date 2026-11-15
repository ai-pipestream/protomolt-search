//! Per-shard BM25 postings, per-document lengths, and the document store.
//!
//! Three shapes share one read surface, [`Bm25Index`]:
//!
//! - [`Bm25Store`]: the in-memory builder. Ingest appends here; `save`
//!   writes a v6 (multi-field) or v7 (columns) payload inside the v8
//!   per-section integrity envelope, atomically, as `<index>.bm25`.
//! - [`SpillBuilder`]: the bounded-memory builder for bulk loads. It
//!   spills sorted runs to disk and merges them into the same file format.
//! - [`Bm25Reader`]: the memory-mapped reader. Postings and stored texts
//!   are read from the map on demand, so a shard larger than RAM serves
//!   from a few MB of heap. Every open validates the file's structure.
//!
//! Postings are append-only and doc-id ordered. Deletes and replacements
//! never rewrite a posting: they are tombstones in the shard's
//! [`crate::live_docs::LiveDocs`], applied as a filter at query time and
//! reclaimed by compaction.
//!
//! Each field's postings section splits every term into a fixed-stride doc
//! run (`doc_id | tf | occ_start`), an occurrence run of `(start, end)`
//! offsets, and a skip run of per-block `(tf, doc_len)` frontiers (level 0
//! per 128 postings, level 1 per 32 blocks). The impact-pruned scorers in
//! [`crate::bm25`] walk the skip run through [`ImpactCursor`] and never
//! decode occurrence offsets for documents that do not make the top-k.

mod analyzed;
mod codec;
mod columns;
mod format;
mod index;
mod reader;
mod section_writer;
mod spill;
mod store;

use self::analyzed::clone_parallel_ordinals;
pub use self::analyzed::{
    AnalyzedDoc, AnalyzedField, DocGeography, DocPositions, DocQuality, DocTerms,
};
pub use self::codec::{FileImpacts, ImpactCursor};
pub use self::columns::{
    ColumnKind, StoredBinding, StoredDerived, FACET_ABSENT, GEO_ABSENT, INTEGER_ABSENT,
};
pub(crate) use self::format::fsync_parent;
pub use self::index::{Bm25Index, PostingCallback};
pub use self::reader::{Bm25Reader, FieldTranspose, FieldView};
pub use self::spill::SpillBuilder;
pub use self::store::{Bm25Store, StoreFieldView};

/// Postings per level-0 skip block.
pub(super) const BLOCK: usize = 128;
/// Level-0 blocks per level-1 record.
pub(super) const LEVEL1_FACTOR: usize = 32;
/// Max `(tf, dl)` pairs kept per skip record; larger frontiers collapse
/// (never drop) to their dominating corner, which only raises the bound.
pub(super) const MAX_FRONTIER: usize = 8;

/// One posting: a term occurrence set within one document.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Posting {
    /// Local document id (global id = slot_offset + this).
    pub doc_id: u32,
    /// Term frequency within the document.
    pub tf: u32,
    /// Occurrence spans `(start, end)` in original-text UTF-8 bytes
    /// (half-open). Empty when ingested with SCORING_ONLY term vectors.
    pub offsets: Vec<(u32, u32)>,
    /// Token ordinal per occurrence, parallel to `offsets`
    ///. Populated only in a field declared
    /// positional; every other field stores none, so the payload costs
    /// nothing where nobody asked for it.
    pub positions: Vec<u32>,
    /// Token-lattice arc length (`pos_len >= 1`) per occurrence, parallel to
    /// `positions` when the field records lattice arcs; empty when all arcs
    /// have unit length.
    pub pos_lengths: Vec<u32>,
}

/// Where a document came from in the source corpus (court pipeline).
/// Persisted with the doc store; `None` for documents ingested without
/// lineage (all pre-lineage shards).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DocLineage {
    /// Source opinion id.
    pub parent_id: u64,
    /// Source cluster id.
    pub group_id: u64,
    /// Chunk span start in original-text (char) coordinates.
    pub span_start: u32,
    /// Chunk span end (exclusive).
    pub span_end: u32,
}

#[cfg(test)]
mod format_tests;
#[cfg(test)]
mod format_v6_tests;
#[cfg(test)]
mod malformed_tests;
#[cfg(test)]
mod positions_tests;
#[cfg(test)]
mod spill_tests;
#[cfg(test)]
mod store_tests;
#[cfg(test)]
mod v8_tests;

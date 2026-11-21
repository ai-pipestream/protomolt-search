//! The segmented shard: sealed segments from a [`SegmentCatalog`] plus
//! one heap tail, read as one shard.
//!
//! Local document ids are one positional space: segment `i` covers
//! `[base_i, base_i + rows_i)` in catalog order, and the tail starts
//! where the last segment ends. Every read routes a document id to its
//! part and offsets the answer back; every aggregate sums the parts.
//! Dictionaries are the one place a union needs its own state: each
//! part's facet, map-key, and map-value ordinals are local to that
//! part's file, so this shard keeps one global dictionary per column
//! (byte-sorted over the sealed parts, with the tail's new values after
//! it) and a remap from each part's ordinals into it. Callers see global
//! ordinals everywhere, so the filter, facet, projection, and highlight
//! machinery works unchanged.
//!
//! Block-max pruning survives sealing: a term's impacts across the
//! sealed parts chain into one [`ImpactCursor`]. A term the tail holds
//! has no impacts until the next flush seals the tail, and the scorer
//! takes its exact heap path for that query — the same answer, slower.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::boolean_bits::Bits;

use crate::postings::{
    AnalyzedDoc, Bm25Index, Bm25Store, DocLineage, FileImpacts, ImpactCursor, PostingCallback,
    StoredBinding,
};
use crate::segments::{OpenedSegmentSet, SegmentCatalog, SegmentSummary};

// Heap tails and mapped segments expose the same ordered schema tables.
// Compare them before changing which part supplies a column ordinal.
macro_rules! same_tables {
    ($other:expr, $tail:expr) => {{
        let other = $other;
        let tail = $tail;
        let same_fields = other.field_count() == tail.field_count()
            && (0..tail.field_count()).all(|f| {
                other.field_name(f) == tail.field_name(f)
                    && other.field_has_positions(f) == tail.field_has_positions(f)
                    && other.field_has_sentences(f) == tail.field_has_sentences(f)
            });
        let same_columns = other.facet_count() == tail.facet_count()
            && (0..tail.facet_count()).all(|i| other.facet_name(i) == tail.facet_name(i))
            && other.numeric_count() == tail.numeric_count()
            && (0..tail.numeric_count()).all(|i| other.numeric_name(i) == tail.numeric_name(i))
            && other.integer_count() == tail.integer_count()
            && (0..tail.integer_count()).all(|i| other.integer_name(i) == tail.integer_name(i))
            && other.unsigned_integer_count() == tail.unsigned_integer_count()
            && (0..tail.unsigned_integer_count())
                .all(|i| other.unsigned_integer_name(i) == tail.unsigned_integer_name(i))
            && other.geo_count() == tail.geo_count()
            && (0..tail.geo_count()).all(|i| other.geo_name(i) == tail.geo_name(i))
            && other.map_facet_count() == tail.map_facet_count()
            && (0..tail.map_facet_count())
                .all(|i| other.map_facet_name(i) == tail.map_facet_name(i))
            && other.map_numeric_count() == tail.map_numeric_count()
            && (0..tail.map_numeric_count())
                .all(|i| other.map_numeric_name(i) == tail.map_numeric_name(i))
            && other.map_integer_count() == tail.map_integer_count()
            && (0..tail.map_integer_count())
                .all(|i| other.map_integer_name(i) == tail.map_integer_name(i))
            && other.map_unsigned_integer_count() == tail.map_unsigned_integer_count()
            && (0..tail.map_unsigned_integer_count())
                .all(|i| other.map_unsigned_integer_name(i) == tail.map_unsigned_integer_name(i));
        same_fields && same_columns
    }};
}

mod body;
mod build;
mod columns;
mod dict;
mod map_columns;
mod publication;
mod shard;
mod tables;
mod union_field;

pub use body::MaskedShard;
use dict::{KeyReverse, Source, UnionDict};
pub use union_field::UnionField;

/// One sealed part's placement in the shard's id space.
#[derive(Debug, Clone, Copy)]
struct Part {
    base: u32,
    rows: u32,
}

/// Where a local document id lives: a sealed part, the frozen part a
/// seal in flight is writing out, or the tail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Placement {
    Sealed { part: usize, local: u32 },
    Frozen { local: u32 },
    Tail { local: u32 },
}

/// A former tail, frozen while a seal writes it out: read-only, shared
/// with the seal through the `Arc`, and served as one more part until
/// the catalog publishes it as a segment.
struct Frozen {
    base: u32,
    rows: u32,
    store: Arc<Bm25Store>,
    /// Whether every lineage parent of the frozen rows is colocated on
    /// this shard (see [`SegmentedShard::note_scattered_parent`]).
    parents_colocated: bool,
}

pub struct SegmentedShard {
    catalog: SegmentCatalog,
    set: Arc<OpenedSegmentSet>,
    parts: Vec<Part>,
    frozen: Option<Frozen>,
    tail: Bm25Store,
    tail_base: u32,
    /// Whether every lineage parent of the tail's rows is colocated on
    /// this shard; a row that cannot prove it clears it until the tail
    /// freezes.
    tail_parents_colocated: bool,
    facets: Vec<UnionDict>,
    map_keys: Vec<UnionDict>,
    map_values: Vec<UnionDict>,
    map_key_reverse: Vec<KeyReverse>,
    map_numeric_keys: Vec<UnionDict>,
    map_integer_keys: Vec<UnionDict>,
    map_unsigned_integer_keys: Vec<UnionDict>,
    map_numeric_key_reverse: Vec<KeyReverse>,
    map_integer_key_reverse: Vec<KeyReverse>,
    map_unsigned_integer_key_reverse: Vec<KeyReverse>,
}

impl std::fmt::Debug for SegmentedShard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentedShard")
            .field("root", &self.set.root())
            .field("parts", &self.parts)
            .field("frozen_rows", &self.frozen.as_ref().map(|f| f.rows))
            .field("tail_base", &self.tail_base)
            .field("tail_docs", &self.tail.next_doc_id())
            .finish()
    }
}

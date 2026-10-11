//! Per-shard BM25 postings index and doc store, with persistence.
//!
//! The shard owns: term → postings (doc id, tf, occurrence offsets in
//! original-text UTF-16 code units), per-document lengths and corpus totals, and
//! the raw document texts (the highlight source). Append-only: no deletes,
//! no updates, so postings for a term stay doc-id-ordered by construction.
//!
//! Two storage shapes share one read surface ([`Bm25Index`]):
//!
//! - [`Bm25Store`] — the heap builder. Ingest appends here; `save`
//!   writes the current format (v8: a v6/v7 payload wearing a CRC
//!   integrity table, see `MAGIC_V8`) atomically next to the shard's
//!   `.tv` as `<index>.bm25`.
//! - [`Bm25Reader`] — the disk-resident shape. The file is memory
//!   mapped; postings slices and document texts are read from the map on
//!   demand (the OS page cache is the buffer pool, the Lucene model), so
//!   a shard far larger than RAM serves from a few MB of heap (per-doc
//!   length/offset tables only). v3 through v7 files stay readable.
//!
//! v5 (`TVBM2505`, see `docs/block-max.md`) splits each term into three
//! runs so the scorer never decodes occurrence offsets for non-survivors:
//! a fixed-stride doc run (12 B/posting: `doc_id | tf | occ_start`, one
//! trailing sentinel `occ_start` per term), an occurrence run (`start,
//! end` u32 pairs in posting order), and a skip run of per-block `(tf,
//! dl)` Pareto frontiers (level 0: one record per 128 postings; level 1:
//! one per 32 level-0 blocks). Stage 1 writes the skip run but no scorer
//! consumes it yet.

mod binding;
mod columns;
mod format;
mod impacts;
mod reader;
mod schema;
mod skip;
mod spill;
mod spill_merge;
mod store;
mod store_read;
mod store_write;
#[cfg(test)]
mod tests;
mod transcode;
mod validate;
mod view;

pub use binding::{StoredBinding, StoredDerived};
pub use impacts::{FileImpacts, ImpactCursor};
pub use reader::Bm25Reader;
pub use schema::{
    AnalyzedDoc, AnalyzedField, DocGeography, DocLineage, DocPositions, DocQuality, DocTerms,
    Posting,
};
pub use spill::SpillBuilder;
pub use store::{Bm25Store, StoreFieldView};
pub use transcode::{transcode_to_v5, TranscodeStats};
pub use view::{FieldTranspose, FieldView};

pub(crate) use binding::{
    binding_entry_size, derived_entry_size, write_binding_entry, write_derived_entry,
};
pub(crate) use columns::*;
pub(crate) use format::*;
pub(crate) use reader::{u16_at, FieldSlice};
pub(crate) use skip::*;
pub(crate) use spill_merge::*;
pub(crate) use validate::*;

/// Callback for [`Bm25Index::for_each_posting`]: `(doc_id, tf,
/// original-text offsets)`; the offsets slice is valid only inside the
/// call.
pub type PostingCallback<'a> = dyn FnMut(u32, u32, &[(u32, u32)]) + 'a;

/// The read surface of a shard's lexical half, shared by the heap
/// builder ([`Bm25Store`]) and the disk-resident reader ([`Bm25Reader`]).
/// Scoring (`bm25::top_k`, `bm25::score_candidates`) and the node's RPC
/// handlers code against this trait only.
pub trait Bm25Index {
    /// Documents with postings.
    fn doc_count(&self) -> u64;
    /// Sum of all document lengths (BM25 avgdl numerator).
    fn total_doc_length(&self) -> u64;
    /// Document length in terms (0 for unknown/sparse slots).
    fn doc_length(&self, doc_id: u32) -> u32;
    /// Document frequency of `term` (0 when absent).
    fn df(&self, term: &str) -> u32;
    /// Call `f` for every posting of `term`, in doc-id order, as
    /// `(doc_id, tf, original-text offsets)`. Implementations may decode
    /// lazily; the offsets slice is valid only inside the call.
    fn for_each_posting(&self, term: &str, f: &mut PostingCallback);
    /// Call `f` for every posting of `term`, in doc-id order, as
    /// `(doc_id, tf)` — the scored path, which must NOT touch occurrence
    /// bytes. The default falls back to [`Self::for_each_posting`] and
    /// drops the offsets; the v5 reader overrides it to walk only the
    /// fixed-stride doc run.
    fn for_each_doc_tf(&self, term: &str, f: &mut dyn FnMut(u32, u32)) {
        self.for_each_posting(term, &mut |doc_id, tf, _offsets| {
            f(doc_id, tf);
        });
    }
    /// The occurrence spans `(start, end)` of `term` within `doc_id`
    /// (empty when the doc has no posting for the term or was ingested
    /// SCORING_ONLY). The default scans [`Self::for_each_posting`]; the
    /// v5 reader overrides it to binary-search the doc run and decode
    /// only that posting's occurrence slice.
    fn posting_offsets(&self, term: &str, doc_id: u32) -> Vec<(u32, u32)> {
        let mut found = Vec::new();
        self.for_each_posting(term, &mut |d, _tf, offsets| {
            if d == doc_id {
                found = offsets.to_vec();
            }
        });
        found
    }
    /// An impact cursor over `term`'s skip run and doc run — the
    /// block-max surface (`docs/block-max.md`). `None` for the heap
    /// store and v3/v4 files, which keep the exhaustive scorers.
    fn impacts(&self, term: &str) -> Option<ImpactCursor<'_>> {
        let _ = term;
        None
    }
    /// Whether [`Self::impacts`] would return a cursor, WITHOUT
    /// building one (the v5 reader answers with a directory lookup
    /// only). Used by callers selecting the pruned path, which builds
    /// the cursors itself.
    fn has_impacts(&self, term: &str) -> bool {
        self.impacts(term).is_some()
    }
    /// The raw text of a document, if stored.
    fn text(&self, doc_id: u32) -> Option<String>;
    /// The lineage of a document, if ingested with one.
    fn lineage(&self, doc_id: u32) -> Option<DocLineage>;
    /// Whether this field keeps token positions
    /// (`docs/phrase-proximity.md`). A phrase or proximity query on a
    /// field answering false is refused by name; nothing approximates
    /// it from character offsets.
    fn has_positions(&self) -> bool {
        false
    }
    /// The token ordinals of `term`'s occurrences in `doc_id`, ascending,
    /// or `None` when the document has no posting for the term. Only
    /// meaningful when [`Self::has_positions`] is true; a field without
    /// positions answers `Some(empty)` for a present posting, so callers
    /// check the flag first and never read absence as "no positions".
    fn posting_positions(&self, term: &str, doc_id: u32) -> Option<Vec<u32>> {
        let _ = (term, doc_id);
        None
    }
    /// Whether this field stores sentence spans per document
    /// (`docs/highlighting.md`). Sentence-mode snippets over a field
    /// answering false refuse by name.
    fn has_sentences(&self) -> bool {
        false
    }
    /// The sentence spans of `doc_id` in UTF-16 units of the stored
    /// text, sorted; `None` when the field stores none, an empty table
    /// for a slot without a document.
    fn doc_sentences(&self, doc_id: u32) -> Option<Vec<(u32, u32)>> {
        let _ = doc_id;
        None
    }
    /// Every term of this field starting with `prefix`, in byte order,
    /// when there are at most `cap` of them; `Err(count)` with the exact
    /// count otherwise (`docs/prefix-terms.md`). The heap store walks its
    /// ordered map, the file reader binary-searches its byte-sorted
    /// directory and scans while the prefix holds — never the whole
    /// dictionary. A prefix past the cap is refused with the count, never
    /// truncated to a quieter match set.
    fn expand_prefix(&self, prefix: &str, cap: usize) -> Result<Vec<String>, usize>;
    /// Every term of this field starting with `prefix` with its posting
    /// document frequency, in byte order, when there are at most
    /// `max_scan` of them; `Err(count)` with the exact count otherwise
    /// (`docs/suggest.md`). The df is the posting list's length as
    /// stored — tombstoned rows included until compaction — so it costs
    /// one directory read per term and no posting walk. The scan is the
    /// prefix scan: lower bound, then forward while the prefix holds,
    /// never the whole dictionary; past the bound nothing is
    /// materialized.
    fn suggest_prefix(&self, prefix: &str, max_scan: usize) -> Result<Vec<(String, u32)>, usize>;
    /// Unique terms in byte order, retaining only a cursor per physical part.
    fn prefix_terms(&self, prefix: &str) -> Box<dyn Iterator<Item = String> + '_>;

    /// Count only admitted postings before applying the visible-term bound.
    /// Hidden-only terms neither consume the bound nor appear in its count.
    fn suggest_visible_prefix(
        &self,
        prefix: &str,
        cap: usize,
        admitted: &dyn Fn(u32) -> bool,
    ) -> Result<Vec<(String, u32)>, usize> {
        let mut entries = Vec::new();
        let mut count = 0;
        for term in self.prefix_terms(prefix) {
            let mut df = 0;
            self.for_each_doc_tf(&term, &mut |doc, _| {
                if admitted(doc) {
                    df += 1;
                }
            });
            if df == 0 {
                continue;
            }
            count += 1;
            if count <= cap {
                entries.push((term, df));
            }
        }
        if count > cap {
            Err(count)
        } else {
            Ok(entries)
        }
    }
}

#[cfg(test)]
#[path = "postings_derived_compat_tests.rs"]
mod derived_compat_tests;

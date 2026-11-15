//! The read surface shared by every postings shape.

#[cfg(doc)]
use super::{Bm25Reader, Bm25Store};
use super::{DocLineage, ImpactCursor};

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
    /// drops the offsets; [`Bm25Reader`] overrides it to walk only the
    /// fixed-stride doc run.
    fn for_each_doc_tf(&self, term: &str, f: &mut dyn FnMut(u32, u32)) {
        self.for_each_posting(term, &mut |doc_id, tf, _offsets| {
            f(doc_id, tf);
        });
    }
    /// The occurrence spans `(start, end)` of `term` within `doc_id`
    /// (empty when the doc has no posting for the term or was ingested
    /// SCORING_ONLY). The default scans [`Self::for_each_posting`];
    /// [`Bm25Reader`] overrides it to binary-search the doc run and decode
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
    /// block-max surface. `None` for the heap
    /// store, which uses the exhaustive scorer.
    fn impacts(&self, _term: &str) -> Option<ImpactCursor<'_>> {
        None
    }
    /// Whether [`Self::impacts`] would return a cursor, WITHOUT
    /// building one ([`Bm25Reader`] answers with a directory lookup
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
    ///. A phrase or proximity query on a
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
    fn posting_positions(&self, _term: &str, _doc_id: u32) -> Option<Vec<u32>> {
        None
    }
    /// Whether this field carries token-lattice arc lengths (`pos_len`) parallel
    /// to [`Self::posting_positions`].
    fn has_pos_lengths(&self) -> bool {
        false
    }
    /// Token-lattice arc lengths (`pos_len >= 1`) parallel to
    /// [`Self::posting_positions`], or `None` when absent.
    fn posting_pos_lengths(&self, _term: &str, _doc_id: u32) -> Option<Vec<u32>> {
        None
    }
    /// Whether this field carries quantized SPLADE Block-Max postings (`COLUMN_KIND_SPLADE`).
    fn has_splade(&self) -> bool {
        false
    }
    /// Score top-`k` documents against quantized SPLADE Block-Max postings with
    /// L1 query rounding slack.
    fn score_splade(
        &self,
        query_weights: &[(String, f32)],
        query_l1_slack: f32,
        k: usize,
        admitted: &dyn Fn(u32) -> bool,
    ) -> (
        Vec<(u32, f32)>,
        crate::turbo_bridge::splade::SpladeTraversalStats,
    ) {
        let (hits, _floor, stats) =
            self.score_splade_with_floor(query_weights, query_l1_slack, 0.0, k, admitted);
        (hits, stats)
    }
    /// Score top-`k` documents against quantized SPLADE Block-Max postings
    /// seeded with an initial dynamic pruning floor, returning `(hits, final_floor, stats)`.
    fn score_splade_with_floor(
        &self,
        _query_weights: &[(String, f32)],
        _query_l1_slack: f32,
        initial_floor: f32,
        _k: usize,
        _admitted: &dyn Fn(u32) -> bool,
    ) -> (
        Vec<(u32, f32)>,
        f32,
        crate::turbo_bridge::splade::SpladeTraversalStats,
    ) {
        (Vec::new(), initial_floor.max(0.0), Default::default())
    }
    /// Whether this field stores sentence spans per document
    ///. Sentence-mode snippets over a field
    /// answering false refuse by name.
    fn has_sentences(&self) -> bool {
        false
    }
    /// The sentence spans of `doc_id` in UTF-8 bytes of the stored
    /// text, sorted; `None` when the field stores none, an empty table
    /// for a slot without a document.
    fn doc_sentences(&self, _doc_id: u32) -> Option<Vec<(u32, u32)>> {
        None
    }
    /// Every term of this field starting with `prefix`, in byte order,
    /// when there are at most `cap` of them; `Err(count)` with the exact
    /// count otherwise. The heap store walks its
    /// ordered map, the file reader binary-searches its byte-sorted
    /// directory and scans while the prefix holds — never the whole
    /// dictionary. A prefix past the cap is refused with the count, never
    /// truncated to a quieter match set.
    fn expand_prefix(&self, prefix: &str, cap: usize) -> Result<Vec<String>, usize>;
    /// Every term of this field starting with `prefix` with its posting
    /// document frequency, in byte order, when there are at most
    /// `max_scan` of them; `Err(count)` with the exact count otherwise
    ///. The df is the posting list's length as
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

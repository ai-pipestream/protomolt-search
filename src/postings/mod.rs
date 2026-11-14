//! Per-shard BM25 postings index and doc store, with persistence.
//!
//! The shard owns: term → postings (doc id, tf, occurrence offsets in
//! original-text UTF-8 bytes), per-document lengths and corpus totals, and
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

mod codec;
mod columns;
mod format;
mod reader;
mod spill;
mod store;

pub use self::codec::{FileImpacts, ImpactCursor};
pub use self::columns::{StoredBinding, StoredDerived, FACET_ABSENT, GEO_ABSENT, INTEGER_ABSENT};
pub(crate) use self::format::fsync_parent;
pub use self::reader::{transcode_to_v5, Bm25Reader, FieldTranspose, FieldView, TranscodeStats};
pub use self::spill::SpillBuilder;
pub use self::store::{Bm25Store, StoreFieldView};

/// Postings per level-0 skip block (Lucene uses 128/256; 128 here).
pub(super) const BLOCK: usize = 128;
/// Level-0 blocks per level-1 record.
pub(super) const LEVEL1_FACTOR: usize = 32;
/// Max `(tf, dl)` pairs kept per skip record; larger frontiers are
/// COLLAPSED (never dropped) to their dominating corner, which only
/// raises the bound — see `docs/block-max.md`.
pub(super) const MAX_FRONTIER: usize = 8;

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
    /// Whether this field carries token-lattice arc lengths (`pos_len`) parallel
    /// to [`Self::posting_positions`].
    fn has_pos_lengths(&self) -> bool {
        false
    }
    /// Token-lattice arc lengths (`pos_len >= 1`) parallel to
    /// [`Self::posting_positions`], or `None` when absent.
    fn posting_pos_lengths(&self, term: &str, doc_id: u32) -> Option<Vec<u32>> {
        let _ = (term, doc_id);
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
        query_weights: &[(String, f32)],
        query_l1_slack: f32,
        initial_floor: f32,
        k: usize,
        admitted: &dyn Fn(u32) -> bool,
    ) -> (
        Vec<(u32, f32)>,
        f32,
        crate::turbo_bridge::splade::SpladeTraversalStats,
    ) {
        let _ = (query_weights, query_l1_slack, k, admitted);
        (Vec::new(), initial_floor.max(0.0), Default::default())
    }
    /// Whether this field stores sentence spans per document
    /// (`docs/highlighting.md`). Sentence-mode snippets over a field
    /// answering false refuse by name.
    fn has_sentences(&self) -> bool {
        false
    }
    /// The sentence spans of `doc_id` in UTF-8 bytes of the stored
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
    /// (`docs/phrase-proximity.md`). Populated only in a field declared
    /// positional; every other field stores none, so the payload costs
    /// nothing where nobody asked for it.
    pub positions: Vec<u32>,
    /// Token-lattice arc length (`pos_len >= 1`) per occurrence, parallel to
    /// `positions` when the field records lattice arcs; empty when all arcs
    /// have unit length.
    pub pos_lengths: Vec<u32>,
}

/// Term data for one document, as produced from the sidecar's term
/// vectors: `(term, tf, original-text offsets)`.
pub type DocTerms = Vec<(String, u32, Vec<(u32, u32)>)>;

/// Token ordinals per occurrence, parallel to a [`DocTerms`] table:
/// `positions[i][j]` is the ordinal of `terms[i].2[j]`. An ordinal counts
/// every token the tokenizer produced, emitted or not, so ordinal
/// adjacency is token adjacency — the fact a phrase query needs and a
/// pair of character spans cannot state.
pub type DocPositions = Vec<Vec<u32>>;

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

/// One analyzed field of one document: per-term data plus the field's
/// length in terms (sum of frequencies, the BM25 length-normalization
/// input).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AnalyzedField {
    /// Per-term data; see [`DocTerms`].
    pub terms: DocTerms,
    /// Field length in terms.
    pub length: u32,
    /// Token ordinals parallel to `terms` (see [`DocPositions`]), or
    /// `None` when the analysis carried no token layer to derive them
    /// from. A positional field refuses a `None` document by name rather
    /// than indexing it without positions, because half a column with
    /// positions is a phrase index that lies about the other half.
    pub positions: Option<DocPositions>,
    /// Sentence spans of the field's text in UTF-8 bytes, sorted and
    /// non-overlapping, or `None` when the analysis carried no sentence
    /// layer. A sentence field refuses a `None` document by name
    /// (`docs/highlighting.md`); every occurrence must lie inside one
    /// sentence, which [`Self::check_sentences`] enforces at ingest so
    /// the query path can trust the table.
    pub sentences: Option<Vec<(u32, u32)>>,
    /// Optional token-lattice arc lengths (`pos_len >= 1`) parallel to
    /// `positions` (`#103`, `#2340`). `None` when every token has unit length
    /// and distinct ordinals; `Some` activates lattice traversal and kind-18
    /// `.bm25` persistence.
    pub pos_lengths: Option<DocPositions>,
    /// Optional calibrated quantized SPLADE `(term, impact)` postings for this
    /// field (`#104`, `#2343`).
    pub splade: Vec<(String, u16)>,
    /// IEEE-754 `f32::to_bits(scale)` for `splade` (`0` when empty).
    pub splade_scale_bits: u32,
}

impl AnalyzedField {
    /// Check that `sentences`, when present, is a sorted, non-overlapping
    /// table of non-empty spans covering every occurrence of every term.
    pub fn check_sentences(&self) -> Result<(), String> {
        let Some(sentences) = &self.sentences else {
            return Ok(());
        };
        crate::highlight::check_sentence_table(sentences)?;
        let occurrences = self
            .terms
            .iter()
            .flat_map(|(_, _, offsets)| offsets.iter().copied());
        crate::highlight::check_coverage(sentences, occurrences).map_err(|(start, end)| {
            format!("occurrence [{start}, {end}) lies outside every sentence span")
        })
    }

    /// Check that `positions` (and optional `pos_lengths`), when present, is
    /// shaped exactly like the term table: one list per term, one ordinal per
    /// occurrence, each list strictly ascending.
    pub fn check_positions(&self) -> Result<(), String> {
        let Some(positions) = &self.positions else {
            if self.pos_lengths.is_some() {
                return Err("pos_lengths present without positions".into());
            }
            return Ok(());
        };
        if positions.len() != self.terms.len() {
            return Err(format!(
                "positions table has {} entries for {} terms",
                positions.len(),
                self.terms.len()
            ));
        }
        for ((term, _, offsets), ordinals) in self.terms.iter().zip(positions) {
            if ordinals.len() != offsets.len() {
                return Err(format!(
                    "term {term:?} has {} occurrences but {} positions",
                    offsets.len(),
                    ordinals.len()
                ));
            }
            if ordinals.windows(2).any(|pair| pair[1] <= pair[0]) {
                return Err(format!(
                    "term {term:?} positions are not strictly ascending"
                ));
            }
        }
        if let Some(pos_lengths) = &self.pos_lengths {
            if pos_lengths.len() != self.terms.len() {
                return Err(format!(
                    "pos_lengths table has {} entries for {} terms",
                    pos_lengths.len(),
                    self.terms.len()
                ));
            }
            for ((term, _, offsets), lengths) in self.terms.iter().zip(pos_lengths) {
                if lengths.len() != offsets.len() {
                    return Err(format!(
                        "term {term:?} has {} occurrences but {} pos_lengths",
                        offsets.len(),
                        lengths.len()
                    ));
                }
                if lengths.contains(&0) {
                    return Err(format!("term {term:?} has zero pos_len"));
                }
            }
        }
        Ok(())
    }
}

/// One analyzed document ready to be indexed: one entry per field,
/// positionally matching the store's field table. Field 0 is the body
/// (the stored text). A document may carry fewer entries than the
/// store has fields; missing trailing fields index as empty.
#[derive(Debug, Clone, Default)]
pub struct AnalyzedDoc {
    /// Per-field analyzed data, indexed by field id.
    pub fields: Vec<AnalyzedField>,
    /// The body's cased term identity from the SAME analysis pass
    /// (`docs/dual-cased.md`), when the ingest asked for one: the same
    /// tokens under the same chain minus case folding, spans and
    /// ordinals coinciding with the body's. The node places it at the
    /// field the request named; it is never a second analysis.
    pub cased: Option<AnalyzedField>,
    /// Per-document quality scalars from the sidecar's noise and
    /// artifact layers, when the ingest asked for them
    /// (`docs/quality-columns.md`). `None` means they were not
    /// requested, which is deliberately distinct from a clean document
    /// (all zeros): the ingest writes a column only when it has a real
    /// measurement for it.
    pub quality: Option<DocQuality>,
    /// Per-document geography reduction from the sidecar's geocoding
    /// layer, when the ingest asked for it
    /// (`docs/geography-columns.md`). `None` means not requested;
    /// `Some` with empty fields means measured-and-found-nothing,
    /// which materializes as column ABSENCE (there is no neutral
    /// coordinate to write).
    pub geography: Option<DocGeography>,
    /// Named-entity mentions retained from the sidecar until ingest
    /// materializes them into an ordinary map column. They are never mixed
    /// into body postings.
    pub entities: Vec<crate::phrases::DocEntity>,
    /// Optional one-pass TurboEmbed NLP projection (`#103`, `#2345`) for
    /// pre-WAL materialization of typed columns.
    pub nlp: Option<Box<turbo_client::DocumentNlpProjection>>,
}

impl PartialEq for AnalyzedDoc {
    fn eq(&self, other: &Self) -> bool {
        self.fields == other.fields
            && self.cased == other.cased
            && self.quality == other.quality
            && self.geography == other.geography
            && self.entities == other.entities
            && self.nlp.is_some() == other.nlp.is_some()
    }
}

/// One document's geography reduction, derived at INGEST and stored
/// as ordinary typed columns (`docs/geography-columns.md`). Every
/// field is optional on purpose: a document that mentions no
/// resolvable place has nothing honest to store.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct DocGeography {
    /// Best resolved location `(lat, lon)`: the highest-confidence
    /// finding, ties broken by text order.
    pub point: Option<(f64, f64)>,
    /// The chosen location's resolution confidence in [0, 1].
    /// Meaningless without `point`; read it only when `point` is set.
    pub confidence: f64,
    /// Top region vote's ISO country code, empty when the document's
    /// location evidence voted for nothing.
    pub country: String,
}

/// One document's quality measurements, all derived at INGEST and
/// stored as ordinary typed columns (`docs/quality-columns.md`). The
/// query path never recomputes them and never calls a model.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct DocQuality {
    /// Worst noise finding's score, in [0, 1]; exactly 0 with no
    /// findings.
    pub noise: f64,
    /// Characters covered by the UNION of the noise findings' spans.
    pub noise_chars: i64,
    /// Number of flagged text artifacts.
    pub artifacts: i64,
}

impl AnalyzedDoc {
    /// A document with only the body field — every single-field ingest
    /// path.
    pub fn body(terms: DocTerms, length: u32) -> Self {
        Self {
            fields: vec![AnalyzedField {
                terms,
                length,
                positions: None,
                sentences: None,
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    /// [`Self::body`] with the token positions the analysis produced.
    pub fn body_positioned(terms: DocTerms, positions: DocPositions, length: u32) -> Self {
        Self {
            fields: vec![AnalyzedField {
                terms,
                length,
                positions: Some(positions),
                sentences: None,
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    /// The body field's analyzed data, consuming the document (empty
    /// when the document carried no fields).
    pub fn into_body(self) -> AnalyzedField {
        self.fields.into_iter().next().unwrap_or_default()
    }
}

#[cfg(test)]
mod derived_compat_tests;
#[cfg(test)]
mod format_tests;
#[cfg(test)]
mod malformed_tests;
#[cfg(test)]
mod store_tests;

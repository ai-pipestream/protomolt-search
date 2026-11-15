//! BM25 scoring over postings with externally supplied (global) corpus
//! stats.
//!
//! Scores are comparable across shards only when every shard scores with
//! the same corpus stats, so nothing here reads shard-local stats: the
//! caller supplies N, total document length, and per-term df.
//!
//! Every top-k scorer takes a [`TopK`] request. The four entry points are
//! [`top_k`] (single-field, block-max pruned), [`top_k_fused`] (multi-field,
//! block-max pruned), and their exhaustive oracles [`top_k_exhaustive`] and
//! [`top_k_fused_exhaustive`] (which also scores phrase groups).

use crate::postings::Posting;

mod candidates;
mod exhaustive;
mod pruned;
mod pruned_cursor;
mod pruned_scan;
mod ranking;
mod request;

pub use candidates::{
    breakdown, score_candidates, score_candidates_plain, score_candidates_plain_walk,
    CandidateWalk, TermPresence,
};
pub use request::{top_k, top_k_exhaustive, top_k_fused, top_k_fused_exhaustive, Phrase, TopK};

/// A resolved score-function chain plus this shard's numeric-column
/// read surface; `None` means no chain, and every scorer is then
/// bit-identical to its unchained form.
pub type ChainCtx<'a> = Option<(
    &'a crate::scorefn::ScoreChain,
    &'a dyn crate::scorefn::NumericRead,
)>;

/// The request's resolved filters (geo family plus the compiled predicate
/// tree) with this shard's column read surface; `None` means no filters.
///
/// A filter only removes documents, so every block-max bound stays a valid
/// upper bound over the survivors. The test sits immediately before the
/// floor comparison and heap insertion, and nowhere else.
pub type FilterCtx<'a> = Option<(
    &'a crate::filter::DocFilter<'a>,
    &'a dyn crate::scorefn::NumericRead,
)>;

/// Whether `doc_id` survives `filter` (vacuously true with no filters).
fn passes(filter: FilterCtx, doc_id: u32) -> bool {
    match filter {
        Some((f, cols)) => f.passes(doc_id, cols),
        None => true,
    }
}

/// Default BM25 k1 (term-frequency saturation).
pub const DEFAULT_K1: f64 = 1.2;
/// Default BM25 b (document-length normalization).
pub const DEFAULT_B: f64 = 0.75;

/// BM25 tuning parameters.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Bm25Params {
    /// k1: term-frequency saturation.
    pub k1: f64,
    /// b: document-length normalization.
    pub b: f64,
}

impl Default for Bm25Params {
    fn default() -> Self {
        Self {
            k1: DEFAULT_K1,
            b: DEFAULT_B,
        }
    }
}

/// Corpus statistics for one scoring run, in query-term order.
#[derive(Debug, Clone, PartialEq)]
pub struct CorpusStats {
    /// Total document count N.
    pub doc_count: u64,
    /// Sum of all document lengths (avgdl = total / N).
    pub total_doc_length: u64,
    /// Document frequency per query term.
    pub dfs: Vec<u32>,
}

impl CorpusStats {
    /// Average document length; 1.0 when the corpus is empty (avoids
    /// division by zero; an empty corpus scores nothing anyway).
    pub fn avgdl(&self) -> f64 {
        if self.doc_count == 0 || self.total_doc_length == 0 {
            1.0
        } else {
            self.total_doc_length as f64 / self.doc_count as f64
        }
    }
}

/// BM25 inverse document frequency, the Lucene-style plus-one form so the
/// value stays positive even for very common terms:
/// `ln(1 + (N - df + 0.5) / (df + 0.5))`.
pub fn idf(n_docs: u64, df: u32) -> f64 {
    (1.0 + (n_docs as f64 - f64::from(df) + 0.5) / (f64::from(df) + 0.5)).ln()
}

/// BM25 tf/length normalization factor:
/// `tf*(k1+1) / (tf + k1*(1 - b + b*dl/avgdl))`.
pub fn tf_norm(params: Bm25Params, tf: u32, doc_len: u32, avgdl: f64) -> f64 {
    let tf = f64::from(tf);
    tf * (params.k1 + 1.0)
        / (tf + params.k1 * (1.0 - params.b + params.b * f64::from(doc_len) / avgdl))
}

/// The floor to emit as `kth_best`: one f32 ULP below the wire k-th best.
///
/// Scoring is f64 while `kth_best` travels as f32, and the rounding can go
/// up; one ULP down never exceeds the true f64 k-th best, so ties at the
/// floor survive. Zero and below map to 0 (no seedable floor).
pub fn floor_seed(kth_best: f32) -> f32 {
    if kth_best > 0.0 {
        kth_best.next_down()
    } else {
        0.0
    }
}

/// Score one posting for one query term.
pub fn score_posting(
    params: Bm25Params,
    stats: &CorpusStats,
    term_index: usize,
    posting: &Posting,
    doc_len: u32,
) -> f64 {
    idf(stats.doc_count, stats.dfs[term_index])
        * tf_norm(params, posting.tf, doc_len, stats.avgdl())
}

/// One scored document.
#[derive(Debug, Clone, PartialEq)]
pub struct ScoredDoc {
    /// Local doc id.
    pub doc_id: u32,
    /// Summed BM25 score over all query terms.
    pub score: f64,
    /// `(query term index, offsets)` for the terms present in this doc.
    pub term_offsets: Vec<(usize, Vec<(u32, u32)>)>,
}

/// One field's slice of a fused multi-field query: the field's index view,
/// its query terms under the field's analysis, its global per-field stats,
/// its k1/b, and the query-time weight.
pub struct FieldQuery<'a> {
    /// The field's index (`Bm25Store::field` / `Bm25Reader::field`).
    pub index: &'a dyn crate::postings::Bm25Index,
    /// Query terms analyzed with this field's spec.
    pub terms: &'a [String],
    /// Global stats for this field, in `terms` order.
    pub stats: CorpusStats,
    /// This field's k1/b.
    pub params: Bm25Params,
    /// Query-time weight w_f.
    pub weight: f64,
}

/// One field-and-term key plus the matching source offsets.
pub type FusedTermOffset = (usize, usize, Vec<(u32, u32)>);

/// One document scored by the fused weighted per-field sum.
#[derive(Debug, Clone, PartialEq)]
pub struct FusedDoc {
    /// Local doc id.
    pub doc_id: u32,
    /// `sum over fields f of w_f * bm25_f(q, d)`.
    pub score: f64,
    /// `(field id, term index within that field's terms, offsets)` for
    /// the (field, term) pairs present in this doc.
    pub term_offsets: Vec<FusedTermOffset>,
}

/// Skip accounting for the block-max scorers.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PruneStats {
    /// Level-0 blocks (128 postings) bypassed by shallow advance.
    pub blocks_skipped: u64,
    /// Level-1 groups (32 blocks = 4096 postings) leapt without reading
    /// a level-0 record inside them.
    pub l1_groups_skipped: u64,
    /// Level-0 blocks across all query terms' postings lists.
    pub blocks_total: u64,
    /// Postings scored during full candidate evaluations.
    pub postings_scored: u64,
    /// Documents fully evaluated (scored against every term).
    pub candidates_evaluated: u64,
}

/// Exchange emission-safe live floors, including negative scores.
/// The seed is one f32 ULP below the local k-th score, or `None` until full.
/// The hook returns a monotonic external lower bound, or `None`.
pub type LiveFloorHook<'a> = &'a mut dyn FnMut(Option<f32>) -> Option<f32>;

/// Compact candidate emission for the coordinator-owned lexical heap.
///
/// On the block-max path it is called for every fully evaluated,
/// filter-admitted document at or above the live inclusive floor. The
/// exhaustive fallbacks publish their complete local top-k after scoring,
/// which suffices because no global winner falls outside its shard's top-k.
pub type CandidateHook<'a> = &'a mut dyn FnMut(u32, f32);

/// `scale * max over the block frontier of tf_norm(..)`: the term's upper
/// bound anywhere in the cursor's current block. The scale is idf for a
/// single-field term, `w_f * idf` for a fused (field, term) pair.
fn block_max(
    cursor: &crate::postings::ImpactCursor,
    scale: f64,
    params: Bm25Params,
    avgdl: f64,
) -> f64 {
    frontier_max(cursor.block_frontier(), scale, params, avgdl)
}

/// `scale * max tf_norm` over a `(tf, doc_len)` frontier.
fn frontier_max(frontier: &[(u32, u32)], scale: f64, params: Bm25Params, avgdl: f64) -> f64 {
    scale
        * frontier
            .iter()
            .map(|&(tf, dl)| tf_norm(params, tf, dl, avgdl))
            .fold(0.0, f64::max)
}

/// The seeded-floor contract applied to a fused fallback result: keep
/// docs with `score >= floor` (ties at the floor survive).
pub fn filter_fused_to_floor(mut docs: Vec<FusedDoc>, floor: f64) -> Vec<FusedDoc> {
    if floor.is_finite() {
        docs.retain(|d| d.score >= floor);
    }
    docs
}

/// The seeded-floor contract applied to a fallback result: keep docs
/// with `score >= floor` (ties at the floor survive).
pub fn filter_to_floor(mut docs: Vec<ScoredDoc>, floor: f64) -> Vec<ScoredDoc> {
    if floor.is_finite() {
        docs.retain(|d| d.score >= floor);
    }
    docs
}

/// Merge per-shard TermStats shares into global corpus stats.
pub fn merge_stats(shares: &[(u64, u64, Vec<u32>)]) -> CorpusStats {
    let mut stats = CorpusStats {
        doc_count: 0,
        total_doc_length: 0,
        dfs: Vec::new(),
    };
    for (doc_count, total_len, dfs) in shares {
        stats.doc_count += doc_count;
        stats.total_doc_length += total_len;
        if stats.dfs.len() < dfs.len() {
            stats.dfs.resize(dfs.len(), 0);
        }
        for (i, &df) in dfs.iter().enumerate() {
            stats.dfs[i] += df;
        }
    }
    stats
}

#[cfg(test)]
mod fused_tests;
#[cfg(test)]
mod tests;

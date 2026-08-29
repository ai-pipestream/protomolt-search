//! BM25 scoring over postings with externally supplied (global) corpus
//! stats.
//!
//! Scores are comparable across shards only when every shard scores with
//! the same corpus stats, so NOTHING here reads shard-local stats: the
//! caller supplies N, total document length, and per-term df (the
//! coordinator's summed globals). The shard-local share of those stats
//! lives on [`crate::postings::Bm25Store`] and is exposed via TermStats.

use crate::postings::{Bm25Index, Posting};

/// A resolved score-function chain plus this shard's numeric-column
/// read surface (`docs/score-functions.md`); `None` = no chain, and
/// every chained scorer is then bit-identical to its unchained twin
/// (the additions are gated, not forked).
pub type ChainCtx<'a> = Option<(
    &'a crate::scorefn::ScoreChain,
    &'a dyn crate::scorefn::NumericRead,
)>;

/// The request's resolved filters — the standalone geo family plus the
/// compiled predicate tree (`docs/geo-columns.md`,
/// `docs/cel-filters.md`) — with this shard's column read surface;
/// `None` = no filters, and every filtered scorer is then
/// bit-identical to its unfiltered twin.
///
/// A filter only REMOVES documents. Every block-max bound therefore
/// stays a valid upper bound over the surviving documents with no new
/// pruning math — the argument `docs/score-functions.md` made for this
/// layer when geo filters landed first. What DOES change is where the
/// test goes: a filtered document must never reach the heap, so the
/// test sits immediately before the floor comparison and insertion,
/// and nowhere else. The heap's k-th best then tracks the k-th best
/// SURVIVOR, which rises no faster than the unfiltered one, so the
/// floor stays conservative.
pub type FilterCtx<'a> = Option<(
    &'a crate::filter::DocFilter,
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

/// The floor to EMIT as `kth_best`: one f32 ULP below the wire k-th
/// best. Scoring is f64 while `kth_best`/`min_score` travel as f32, and
/// `f32(kth)` rounds UP half the time — seeding with the rounded value
/// would filter the boundary hit, contradicting "ties at the floor
/// survive". One ULP down can never exceed the true f64 k-th best
/// (rounded down: strictly below it; rounded up: at most half a ULP
/// above it, minus a full ULP). 0 stays 0 (no seedable floor).
/// (`f32::next_down` is stable since Rust 1.86; the toolchain is 1.97.)
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

/// Score only the given candidate docs (local ids, any order, duplicates
/// tolerated) against the postings — phase 2 of the cascade fusion mode.
///
/// Postings lists are append-only, hence doc-id-sorted, so scoring is a
/// per-term merge-join between the sorted candidate list and each term's
/// postings: O(candidates + matched postings), never a full postings
/// walk. On a v5 reader the merge-join becomes a shallow-advance walk:
/// each candidate positions the cursor through the skip run (whole
/// blocks jumped by `last_doc_id`, binary search only inside the
/// landing block). Returns one [`ScoredDoc`] per candidate that scored
/// above zero, doc id ascending.
pub fn score_candidates(
    store: &dyn Bm25Index,
    terms: &[String],
    stats: &CorpusStats,
    params: Bm25Params,
    candidate_ids: &[u32],
) -> Vec<ScoredDoc> {
    debug_assert_eq!(terms.len(), stats.dfs.len());
    let mut candidates: Vec<u32> = candidate_ids.to_vec();
    candidates.sort_unstable();
    candidates.dedup();
    let avgdl = stats.avgdl();

    // v5 path: per-term impact cursors advance to each candidate
    // (candidates are sorted, so each cursor walks forward once). Any
    // term without impacts (heap store, v3/v4) falls back to the
    // merge-join below.
    let mut cursors = Vec::new();
    let mut all_have_impacts = true;
    for (ti, term) in terms.iter().enumerate() {
        if stats.dfs[ti] == 0 {
            continue;
        }
        match store.impacts(term) {
            Some(cursor) => cursors.push((ti, cursor)),
            None => {
                all_have_impacts = false;
                break;
            }
        }
    }
    let mut docs: Vec<ScoredDoc> = Vec::new();
    if all_have_impacts {
        for (ti, mut cursor) in cursors {
            let idf = idf(stats.doc_count, stats.dfs[ti]);
            for &cand in &candidates {
                if cursor.exhausted() {
                    break;
                }
                cursor.advance_shallow(cand);
                if cursor.doc_id() == cand {
                    let contribution =
                        idf * tf_norm(params, cursor.tf(), store.doc_length(cand), avgdl);
                    let entry = match docs.iter_mut().find(|d| d.doc_id == cand) {
                        Some(d) => d,
                        None => {
                            docs.push(ScoredDoc {
                                doc_id: cand,
                                score: 0.0,
                                term_offsets: Vec::new(),
                            });
                            docs.last_mut().expect("just pushed")
                        }
                    };
                    entry.score += contribution;
                    entry.term_offsets.push((ti, cursor.offsets()));
                }
            }
        }
        return docs;
    }

    for (ti, term) in terms.iter().enumerate() {
        if stats.dfs[ti] == 0 {
            continue;
        }
        let idf = idf(stats.doc_count, stats.dfs[ti]);
        // Merge-join: postings stream ascending by doc id; a cursor per
        // term walks the sorted candidates.
        let mut ci = 0usize;
        store.for_each_posting(term, &mut |doc_id, tf, offsets| {
            while ci < candidates.len() && candidates[ci] < doc_id {
                ci += 1;
            }
            if ci < candidates.len() && candidates[ci] == doc_id {
                let contribution = idf * tf_norm(params, tf, store.doc_length(doc_id), avgdl);
                let entry = match docs.iter_mut().find(|d| d.doc_id == doc_id) {
                    Some(d) => d,
                    None => {
                        docs.push(ScoredDoc {
                            doc_id,
                            score: 0.0,
                            term_offsets: Vec::new(),
                        });
                        docs.last_mut().expect("just pushed")
                    }
                };
                entry.score += contribution;
                entry.term_offsets.push((ti, offsets.to_vec()));
            }
        });
    }
    docs
}

/// Score `terms` over the shard's postings with the supplied (global)
/// stats; return the top-k, score descending, ties by ascending doc id.
///
/// The scored walk uses [`Bm25Index::for_each_doc_tf`] — on a v5 reader
/// that touches only the fixed-stride doc run, never the occurrence
/// bytes. Per-doc term membership rides in an allocation-free bitmask
/// (a `Vec` only beyond 64 query terms, which never happens here), and
/// occurrence spans are fetched afterwards, only for the k survivors,
/// via [`Bm25Index::posting_offsets`]. The result is bit-identical to
/// [`top_k_exhaustive`], which is kept as the test oracle.
pub fn top_k(
    store: &dyn Bm25Index,
    terms: &[String],
    stats: &CorpusStats,
    params: Bm25Params,
    k: usize,
) -> Vec<ScoredDoc> {
    top_k_chained(store, terms, stats, params, k, None)
}

/// [`top_k`] with a score-function chain applied to each document's
/// BM25 score BEFORE ranking (`docs/score-functions.md`): the returned
/// top-k and scores are on the FINAL scale.
pub fn top_k_chained(
    store: &dyn Bm25Index,
    terms: &[String],
    stats: &CorpusStats,
    params: Bm25Params,
    k: usize,
    chain: ChainCtx,
) -> Vec<ScoredDoc> {
    top_k_chained_filtered(store, terms, stats, params, k, chain, None)
}

/// [`top_k_chained`] with geo filters (`docs/geo-columns.md`): a
/// document failing any filter is dropped BEFORE ranking, so this is
/// the exhaustive-in-heap-shape twin of the filtered pruned scorer and
/// its fallback. With `filter` `None`, bit-identical to
/// [`top_k_chained`].
#[allow(clippy::too_many_arguments)]
pub fn top_k_chained_filtered(
    store: &dyn Bm25Index,
    terms: &[String],
    stats: &CorpusStats,
    params: Bm25Params,
    k: usize,
    chain: ChainCtx,
    filter: FilterCtx,
) -> Vec<ScoredDoc> {
    debug_assert_eq!(terms.len(), stats.dfs.len());
    let avgdl = stats.avgdl();
    let finish = |score: f64, doc_id: u32| match chain {
        Some((c, cols)) => c.eval(score, doc_id, cols),
        None => score,
    };
    fn sort_truncate<T>(docs: &mut Vec<(u32, f64, T)>, k: usize) {
        docs.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.0.cmp(&b.0))
        });
        docs.truncate(k);
    }
    if terms.len() <= 64 {
        // Accumulate per-doc (score, term-membership bitmask). Query
        // workloads here are small (a handful of terms, postings lists
        // walked once), so a HashMap merge is fine.
        let mut scores: std::collections::HashMap<u32, (f64, u64)> =
            std::collections::HashMap::new();
        for (ti, term) in terms.iter().enumerate() {
            if stats.dfs[ti] == 0 {
                continue;
            }
            let idf = idf(stats.doc_count, stats.dfs[ti]);
            store.for_each_doc_tf(term, &mut |doc_id, tf| {
                let contribution = idf * tf_norm(params, tf, store.doc_length(doc_id), avgdl);
                let entry = scores.entry(doc_id).or_default();
                entry.0 += contribution;
                entry.1 |= 1u64 << ti;
            });
        }
        let mut docs: Vec<(u32, f64, u64)> = scores
            .into_iter()
            .filter(|&(doc_id, _)| passes(filter, doc_id))
            .map(|(doc_id, (score, mask))| (doc_id, finish(score, doc_id), mask))
            .collect();
        sort_truncate(&mut docs, k);
        // Occurrences are fetched for the k survivors only — on a v5
        // reader this is a binary search plus one occurrence-slice
        // decode per surviving (term, doc), instead of a decode per
        // posting.
        docs.into_iter()
            .map(|(doc_id, score, mask)| ScoredDoc {
                doc_id,
                score,
                term_offsets: (0..terms.len())
                    .filter(|&ti| mask >> ti & 1 == 1)
                    .map(|ti| (ti, store.posting_offsets(&terms[ti], doc_id)))
                    .collect(),
            })
            .collect()
    } else {
        // More than 64 query terms: same shape with a Vec membership
        // list (one allocation per doc-term hit; correctness fallback,
        // not a path anyone should time).
        let mut scores: std::collections::HashMap<u32, (f64, Vec<usize>)> =
            std::collections::HashMap::new();
        for (ti, term) in terms.iter().enumerate() {
            if stats.dfs[ti] == 0 {
                continue;
            }
            let idf = idf(stats.doc_count, stats.dfs[ti]);
            store.for_each_doc_tf(term, &mut |doc_id, tf| {
                let contribution = idf * tf_norm(params, tf, store.doc_length(doc_id), avgdl);
                let entry = scores.entry(doc_id).or_default();
                entry.0 += contribution;
                entry.1.push(ti);
            });
        }
        let mut docs: Vec<(u32, f64, Vec<usize>)> = scores
            .into_iter()
            .filter(|&(doc_id, _)| passes(filter, doc_id))
            .map(|(doc_id, (score, tis))| (doc_id, finish(score, doc_id), tis))
            .collect();
        sort_truncate(&mut docs, k);
        docs.into_iter()
            .map(|(doc_id, score, tis)| ScoredDoc {
                doc_id,
                score,
                term_offsets: tis
                    .into_iter()
                    .map(|ti| (ti, store.posting_offsets(&terms[ti], doc_id)))
                    .collect(),
            })
            .collect()
    }
}

/// The pre-v5 exhaustive scorer: walks every posting WITH its occurrence
/// offsets and attaches them as it goes. Kept as the exactness oracle
/// for tests; production code calls [`top_k`].
pub fn top_k_exhaustive(
    store: &dyn Bm25Index,
    terms: &[String],
    stats: &CorpusStats,
    params: Bm25Params,
    k: usize,
) -> Vec<ScoredDoc> {
    top_k_exhaustive_chained(store, terms, stats, params, k, None)
}

/// [`top_k_exhaustive`] with a score-function chain — the exactness
/// oracle for [`top_k_pruned_chained`]: walk everything, score, chain,
/// rank.
pub fn top_k_exhaustive_chained(
    store: &dyn Bm25Index,
    terms: &[String],
    stats: &CorpusStats,
    params: Bm25Params,
    k: usize,
    chain: ChainCtx,
) -> Vec<ScoredDoc> {
    top_k_exhaustive_chained_filtered(store, terms, stats, params, k, chain, None)
}

/// [`top_k_exhaustive_chained`] with geo filters — the exactness oracle
/// for the filtered pruned scorer. The filter is applied at exactly one
/// place here too (before ranking), so "pruned == exhaustive bitwise"
/// is a statement about the same predicate on both sides.
#[allow(clippy::too_many_arguments)]
pub fn top_k_exhaustive_chained_filtered(
    store: &dyn Bm25Index,
    terms: &[String],
    stats: &CorpusStats,
    params: Bm25Params,
    k: usize,
    chain: ChainCtx,
    filter: FilterCtx,
) -> Vec<ScoredDoc> {
    debug_assert_eq!(terms.len(), stats.dfs.len());
    type TermHits = Vec<(usize, Vec<(u32, u32)>)>;
    let mut scores: std::collections::HashMap<u32, (f64, TermHits)> =
        std::collections::HashMap::new();
    for (ti, term) in terms.iter().enumerate() {
        if stats.dfs[ti] == 0 {
            continue;
        }
        store.for_each_posting(term, &mut |doc_id, tf, offsets| {
            let doc_len = store.doc_length(doc_id);
            let posting = Posting {
                doc_id,
                tf,
                offsets: offsets.to_vec(),
            };
            let contribution = score_posting(params, stats, ti, &posting, doc_len);
            let entry = scores.entry(doc_id).or_default();
            entry.0 += contribution;
            entry.1.push((ti, posting.offsets));
        });
    }
    let mut docs: Vec<ScoredDoc> = scores
        .into_iter()
        .filter(|&(doc_id, _)| passes(filter, doc_id))
        .map(|(doc_id, (score, term_offsets))| ScoredDoc {
            doc_id,
            score: match chain {
                Some((c, cols)) => c.eval(score, doc_id, cols),
                None => score,
            },
            term_offsets,
        })
        .collect();
    docs.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.doc_id.cmp(&b.doc_id))
    });
    docs.truncate(k);
    docs
}

/// One field's slice of a fused multi-field query
/// (`docs/multi-field.md`): the field's index view, its query terms
/// under the FIELD'S analysis (term identity is per field), its global
/// per-field stats (shared N, per-field total length and dfs), its
/// per-field k1/b, and the query-time weight.
pub struct FieldQuery<'a> {
    /// The field's index (`Bm25Store::field` / `Bm25Reader::field`).
    pub index: &'a dyn Bm25Index,
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

/// The fused multi-field exhaustive scorer (`docs/multi-field.md`):
/// weighted per-field sum, each field saturating independently (NOT
/// true BM25F), so every single-field contract holds per field
/// unchanged and floors decompose into weighted per-field bounds.
///
/// Determinism rule: contributions accumulate in field-id order, and
/// within a field in term-index order — IEEE addition is not
/// associative, and [`top_k_fused_pruned`] reproduces these exact
/// bits. With one field at weight 1.0 the result is bit-identical to
/// [`top_k_exhaustive`] (`1.0 * x == x` exactly).
pub fn top_k_fused_exhaustive(fields: &[FieldQuery], k: usize) -> Vec<FusedDoc> {
    top_k_fused_exhaustive_filtered(fields, k, None)
}

/// [`top_k_fused_exhaustive`] with geo filters (`docs/geo-columns.md`);
/// the oracle for the filtered fused pruned scorer. The fused route
/// carries filters for the same reason it carries range facets: the
/// match set is the union over every leg's terms, and a filter narrows
/// that union exactly as it narrows a single leg's.
pub fn top_k_fused_exhaustive_filtered(
    fields: &[FieldQuery],
    k: usize,
    filter: FilterCtx,
) -> Vec<FusedDoc> {
    type Hits = Vec<(usize, usize, Vec<(u32, u32)>)>;
    let mut scores: std::collections::HashMap<u32, (f64, Hits)> = std::collections::HashMap::new();
    for (fi, fq) in fields.iter().enumerate() {
        debug_assert_eq!(fq.terms.len(), fq.stats.dfs.len());
        let avgdl = fq.stats.avgdl();
        for (ti, term) in fq.terms.iter().enumerate() {
            if fq.stats.dfs[ti] == 0 {
                continue;
            }
            let idf = idf(fq.stats.doc_count, fq.stats.dfs[ti]);
            fq.index.for_each_posting(term, &mut |doc_id, tf, offsets| {
                let contribution =
                    fq.weight * idf * tf_norm(fq.params, tf, fq.index.doc_length(doc_id), avgdl);
                let entry = scores.entry(doc_id).or_default();
                entry.0 += contribution;
                entry.1.push((fi, ti, offsets.to_vec()));
            });
        }
    }
    let mut docs: Vec<FusedDoc> = scores
        .into_iter()
        .filter(|&(doc_id, _)| passes(filter, doc_id))
        .map(|(doc_id, (score, term_offsets))| FusedDoc {
            doc_id,
            score,
            term_offsets,
        })
        .collect();
    docs.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.doc_id.cmp(&b.doc_id))
    });
    docs.truncate(k);
    docs
}

/// Skip accounting for [`top_k_pruned_stats`].
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

/// Mid-scan floor exchange for the streaming lexical protocol
/// (`docs/block-max.md`, the bidi relay). Called once per outer loop
/// iteration of the pruned scorer with the scan's current emission-safe
/// k-th best (`floor_seed`, `None` until the heap fills); the hook may
/// publish it. It returns the highest external floor currently known,
/// or `None`. External floors are emission-safe seeds too, so they are
/// proven lower bounds on the final global k-th best: raising the
/// cutoff to one mid-scan only strengthens `inert`, which is monotone
/// in the floor, and ties at the floor keep surviving — the result is
/// the seeded-floor contract, delivered while the scan runs.
pub type LiveFloorHook<'a> = &'a mut dyn FnMut(Option<f32>) -> Option<f32>;

/// One heap entry: a candidate and the query-term membership mask.
#[derive(Debug, Clone, Copy, PartialEq)]
struct HeapEntry {
    score: f64,
    doc_id: u32,
    /// Bitmask of the query terms present in this doc (ti < 128).
    mask: u128,
}

impl Eq for HeapEntry {}

/// Max-heap whose TOP is the current k-th best (worst survivor): lowest
/// score, and among equal scores the LARGEST doc id — the entry a
/// strictly-better candidate displaces.
impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .score
            .partial_cmp(&self.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(self.doc_id.cmp(&other.doc_id))
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Block-max top-k over the v5 skip run (`docs/block-max.md`, stage 2).
///
/// Per term, a cursor holds the current 128-posting block's frontier —
/// `idf * max over the frontier of tf_norm(.., avgdl)` upper-bounds that
/// term's contribution anywhere in the block — a bound for the current
/// level-1 group (4096 postings), and a static whole-term bound. Each
/// iteration:
///
/// 1. **Termination**: the sum of the static whole-term bounds can no
///    longer clear the floor — nothing remaining can enter, stop.
/// 2. **Level skips**: if the sum of level-1 group bounds cannot clear
///    the floor, every cursor leaps past the shallowest group end (4096
///    postings per term at one test); else if the sum of level-0 block
///    bounds cannot, every cursor shallow-advances past the shallowest
///    block end.
/// 3. **MaxScore partition**: inside a competitive window (up to the
///    shallowest block end, where every block bound is valid), the
///    largest prefix of terms — sorted by block max — whose maxes sum
///    inert is non-essential. Only essential terms generate candidates.
/// 4. **Candidate test**: a candidate's bound is its essential (true)
///    contributions plus the non-essential block maxes; if inert, the
///    doc is dropped unevaluated AND every cursor advances past it —
///    cursors move in lockstep, so candidate selection is strictly
///    doc-id increasing (debug_assert'ed; see the drop path for the
///    soundness proof). Otherwise the doc is fully evaluated (every
///    cursor advances to it inside the window), scored in term order,
///    and inserted into the heap.
///
/// EXACTNESS CONTRACT (load-bearing): the skip test is `bound <=
/// cutoff`; candidates are evaluated in doc-id order; the heap replaces
/// only on a strictly greater score, so at a tie the incumbent is always
/// the smaller doc id. A seeded floor keeps only docs with `score >=
/// floor` (ties at the floor survive). All bound sums are accumulated in
/// term-index order so they dominate the true (term-ordered) score in
/// IEEE arithmetic exactly. The result is bit-identical to
/// [`top_k_exhaustive`] (with the seed filter applied).
///
/// Falls back to [`top_k`] when any query term lacks impacts (heap
/// store, v3/v4 files, or more than 128 query terms — the membership
/// mask width); the caller selects, this is the safety net.
pub fn top_k_pruned(
    store: &dyn Bm25Index,
    terms: &[String],
    stats: &CorpusStats,
    params: Bm25Params,
    k: usize,
    floor: f64,
) -> Vec<ScoredDoc> {
    let mut prune = PruneStats::default();
    top_k_pruned_stats(store, terms, stats, params, k, floor, &mut prune)
}

/// [`top_k_pruned`] with skip accounting.
pub fn top_k_pruned_stats(
    store: &dyn Bm25Index,
    terms: &[String],
    stats: &CorpusStats,
    params: Bm25Params,
    k: usize,
    floor: f64,
    prune: &mut PruneStats,
) -> Vec<ScoredDoc> {
    top_k_pruned_chained_stats(store, terms, stats, params, k, floor, None, prune)
}

/// [`top_k_pruned`] with a score-function chain
/// (`docs/score-functions.md`): every bound sum is lifted through
/// [`crate::scorefn::ScoreChain::bound`] before its inert test and
/// every fully evaluated candidate is chained through
/// [`crate::scorefn::ScoreChain::eval`] before insertion, so the heap,
/// the seeded floor, and `kth_best` all operate on FINAL scores. Every
/// stage is monotone non-decreasing in the incoming score and its
/// bound covers the column's whole domain including absence, so "upper
/// bound in, upper bound out" survives the composition and the skip
/// tests stay sound. Bit-identical to [`top_k_exhaustive_chained`]
/// (with the seed filter applied); with `chain` `None`, bit-identical
/// to [`top_k_pruned`].
pub fn top_k_pruned_chained(
    store: &dyn Bm25Index,
    terms: &[String],
    stats: &CorpusStats,
    params: Bm25Params,
    k: usize,
    floor: f64,
    chain: ChainCtx,
) -> Vec<ScoredDoc> {
    let mut prune = PruneStats::default();
    top_k_pruned_chained_stats(store, terms, stats, params, k, floor, chain, &mut prune)
}

/// [`top_k_pruned_chained`] with skip accounting.
#[allow(clippy::too_many_arguments)]
pub fn top_k_pruned_chained_stats(
    store: &dyn Bm25Index,
    terms: &[String],
    stats: &CorpusStats,
    params: Bm25Params,
    k: usize,
    floor: f64,
    chain: ChainCtx,
    prune: &mut PruneStats,
) -> Vec<ScoredDoc> {
    top_k_pruned_chained_filtered_stats(store, terms, stats, params, k, floor, chain, None, prune)
}

/// [`top_k_pruned_chained_stats`] with geo filters
/// (`docs/geo-columns.md`). The filter gates ONE thing — heap
/// insertion — and touches no bound, no skip test, and no cursor
/// advance: removing documents cannot invalidate an upper bound over a
/// superset, so the whole pruning argument carries over untouched, and
/// candidates keep being evaluated (and cursors advanced) in exactly
/// the same order whether or not they survive. Bit-identical to
/// [`top_k_exhaustive_chained_filtered`] with the seed filter applied;
/// with `filter` `None`, bit-identical to
/// [`top_k_pruned_chained_stats`].
#[allow(clippy::too_many_arguments)]
pub fn top_k_pruned_chained_filtered_stats(
    store: &dyn Bm25Index,
    terms: &[String],
    stats: &CorpusStats,
    params: Bm25Params,
    k: usize,
    floor: f64,
    chain: ChainCtx,
    filter: FilterCtx,
    prune: &mut PruneStats,
) -> Vec<ScoredDoc> {
    top_k_pruned_chained_filtered_stats_live(
        store, terms, stats, params, k, floor, chain, filter, prune, None,
    )
}

/// [`top_k_pruned_chained_filtered_stats`] with a mid-scan floor
/// exchange ([`LiveFloorHook`]): the hook is polled once per outer loop
/// iteration, the shard's running k-th best flows out through it, and
/// any external floor it returns raises the cutoff for every later
/// bound test and insertion. With `live` `None` (or a hook that never
/// raises), bit-identical to the plain variant. A raised floor only
/// prunes candidates a proven global lower bound already excludes, so
/// the merged fleet result is identical whatever the relay timing —
/// only the work skipped changes. The exhaustive fallbacks ignore the
/// hook: with no skip surface a floor cannot save work, and the seed
/// filter already applies at the end.
#[allow(clippy::too_many_arguments)]
pub fn top_k_pruned_chained_filtered_stats_live(
    store: &dyn Bm25Index,
    terms: &[String],
    stats: &CorpusStats,
    params: Bm25Params,
    k: usize,
    floor: f64,
    chain: ChainCtx,
    filter: FilterCtx,
    prune: &mut PruneStats,
    mut live: Option<LiveFloorHook>,
) -> Vec<ScoredDoc> {
    debug_assert_eq!(terms.len(), stats.dfs.len());
    let mut floor = floor;
    let avgdl = stats.avgdl();
    // Lift a bound to the final-score scale / finish a true score.
    // With no chain both are identity, so every float op below is
    // exactly the unchained scorer's.
    let lift = |b: f64| match chain {
        Some((c, _)) => c.bound(b),
        None => b,
    };
    let finish = |score: f64, doc_id: u32| match chain {
        Some((c, cols)) => c.eval(score, doc_id, cols),
        None => score,
    };

    // One cursor per scored term. Any missing impact surface falls the
    // whole query back to the exhaustive-in-heap-shape scorer.
    struct TermState<'a> {
        ti: usize,
        idf: f64,
        cursor: crate::postings::ImpactCursor<'a>,
        /// idf-scaled upper bound of the current level-0 block.
        block_max: f64,
        block: u32,
        /// idf-scaled upper bound of the current level-1 group.
        l1_max: f64,
        l1_group: u32,
        /// idf-scaled upper bound over the whole term (from level-1
        /// records; static, so always a valid remainder bound).
        term_max: f64,
    }
    impl TermState<'_> {
        /// Re-derive bounds after the cursor moved (no-op when it stayed
        /// inside the current block).
        fn refresh(&mut self, params: Bm25Params, avgdl: f64) {
            if self.cursor.block() == self.block {
                return;
            }
            self.block = self.cursor.block();
            self.block_max = block_max(&self.cursor, self.idf, params, avgdl);
            if self.cursor.l1_group() != self.l1_group {
                self.l1_group = self.cursor.l1_group();
                self.l1_max = self.idf
                    * self
                        .cursor
                        .l1_frontier()
                        .iter()
                        .map(|&(tf, dl)| tf_norm(params, tf, dl, avgdl))
                        .fold(0.0, f64::max);
            }
        }
    }
    /// Drop exhausted cursors, harvesting their skip counters.
    fn retain_live(state: &mut Vec<TermState>, skips: &mut (u64, u64)) {
        state.retain(|ts| {
            if ts.cursor.exhausted() {
                skips.0 += ts.cursor.blocks_skipped;
                skips.1 += ts.cursor.l1_groups_skipped;
                false
            } else {
                true
            }
        });
    }
    let mut state: Vec<TermState> = Vec::new();
    if terms.len() > 128 {
        return filter_to_floor(
            top_k_chained_filtered(store, terms, stats, params, k, chain, filter),
            floor,
        );
    }
    for (ti, term) in terms.iter().enumerate() {
        // A term absent from THIS shard contributes 0 to every document
        // here, so it is skipped rather than scored. `stats.dfs` is the
        // GLOBAL df, which is non-zero for a term that merely lives on
        // another shard: checking only that sent every such query down
        // the exhaustive path below, because a locally-absent term has no
        // impact surface to open. On a sharded corpus that is the common
        // case for exactly the rare, discriminative terms worth pruning
        // with -- measured at 2710 ms vs 9 ms for "of 12b6" on the 86.6M
        // corpus, where 7 of 8 shards lacked the rare term and each one
        // then walked all 83.7M postings of "of" exhaustively.
        if stats.dfs[ti] == 0 || store.df(term) == 0 {
            continue;
        }
        let Some(cursor) = store.impacts(term) else {
            // Present here but no impact surface: a genuine format
            // limitation, and the only case that still forfeits pruning.
            return filter_to_floor(
                top_k_chained_filtered(store, terms, stats, params, k, chain, filter),
                floor,
            );
        };
        let idf = idf(stats.doc_count, stats.dfs[ti]);
        let l1_max = idf
            * cursor
                .l1_frontier()
                .iter()
                .map(|&(tf, dl)| tf_norm(params, tf, dl, avgdl))
                .fold(0.0, f64::max);
        let term_max = idf
            * cursor
                .term_frontier()
                .iter()
                .map(|&(tf, dl)| tf_norm(params, tf, dl, avgdl))
                .fold(0.0, f64::max);
        prune.blocks_total += u64::from(cursor.n_blocks());
        state.push(TermState {
            ti,
            idf,
            block: cursor.block(),
            block_max: block_max(&cursor, idf, params, avgdl),
            l1_group: cursor.l1_group(),
            l1_max,
            cursor,
            term_max,
        });
    }

    let mut heap: std::collections::BinaryHeap<HeapEntry> = std::collections::BinaryHeap::new();
    if k == 0 {
        return Vec::new();
    }
    // Reusable per-candidate accumulation: contributions by term index,
    // summed in term order so the float op sequence matches the
    // exhaustive scorer bit for bit. The partition buffers are reused
    // across iterations (they would otherwise dominate allocations).
    let mut contrib: Vec<f64> = vec![0.0; terms.len()];
    let mut touched: Vec<usize> = Vec::new();
    let mut order: Vec<usize> = Vec::new();
    let mut nonessential: Vec<bool> = Vec::new();
    let mut prefix: Vec<usize> = Vec::new();
    // Skip counts (level-0 blocks, level-1 groups) of retired cursors.
    let mut finished: (u64, u64) = (0, 0);
    // Last processed candidate doc id (doc-order invariant witness).
    let mut last_selected: Option<u32> = None;

    while !state.is_empty() {
        let heap_full = heap.len() >= k;
        let kth = if heap_full {
            heap.peek().expect("full heap").score
        } else {
            f64::NEG_INFINITY
        };
        // Mid-scan floor exchange: offer the running k-th best (as the
        // emission-safe f32 seed the wire carries) and adopt any higher
        // external floor before this iteration's bound tests. Floors
        // only rise, and `inert` is monotone in the floor, so a raise
        // here can only skip candidates a proven global lower bound
        // already excludes.
        if let Some(hook) = live.as_mut() {
            let seed = heap_full.then(|| floor_seed(kth as f32));
            if let Some(external) = hook(seed) {
                let external = f64::from(external);
                if external > floor {
                    floor = external;
                }
            }
        }
        // Inert = cannot enter the heap: below the seed, or (once the
        // heap is full) not strictly above the k-th best. Every bound
        // test below mirrors the insertion rule exactly, and every bound
        // sum is accumulated in term-index order (see the contract).
        let inert = |acc: f64| acc < floor || (heap_full && acc <= kth);
        let mut static_sum = 0.0;
        let mut l1_sum = 0.0;
        let mut block_sum = 0.0;
        for ti in 0..terms.len() {
            if let Some(ts) = state.iter().find(|ts| ts.ti == ti) {
                static_sum += ts.term_max;
                l1_sum += ts.l1_max;
                block_sum += ts.block_max;
            }
        }
        // 1. Termination on the static whole-term bounds.
        if inert(lift(static_sum)) {
            break;
        }
        // 2a. Level-1 skip: whole 4096-posting groups at one test.
        if inert(lift(l1_sum)) {
            let d = state
                .iter()
                .map(|ts| ts.cursor.l1_last_doc())
                .min()
                .expect("nonempty state");
            for ts in state.iter_mut() {
                ts.cursor.advance_shallow(d.saturating_add(1));
                ts.refresh(params, avgdl);
            }
            retain_live(&mut state, &mut finished);
            continue;
        }
        // 2b. Level-0 range skip.
        if inert(lift(block_sum)) {
            let d = state
                .iter()
                .map(|ts| ts.cursor.block_last_doc())
                .min()
                .expect("nonempty state");
            for ts in state.iter_mut() {
                ts.cursor.advance_shallow(d.saturating_add(1));
                ts.refresh(params, avgdl);
            }
            retain_live(&mut state, &mut finished);
            continue;
        }
        // 3. Competitive window [_, window_end]: MaxScore partition.
        // window_end is the shallowest block end, so every term's block
        // bound is valid over the whole window. Sorted by block max
        // ascending, greedily grow the non-essential prefix while its
        // ti-order bound sum stays inert: docs formed only of those
        // terms can never enter the heap. At least one essential term
        // remains, because block_sum is not inert.
        let window_end = state
            .iter()
            .map(|ts| ts.cursor.block_last_doc())
            .min()
            .expect("nonempty state");
        order.clear();
        order.extend(0..state.len());
        order.sort_by(|&a, &b| {
            state[a]
                .block_max
                .partial_cmp(&state[b].block_max)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        nonessential.clear();
        nonessential.resize(state.len(), false);
        prefix.clear();
        for &j in &order {
            let mut acc = 0.0;
            for ti in 0..terms.len() {
                if let Some(pos) = state.iter().position(|ts| ts.ti == ti) {
                    if pos == j || prefix.contains(&pos) {
                        acc += state[pos].block_max;
                    }
                }
            }
            if inert(lift(acc)) {
                prefix.push(j);
            } else {
                break;
            }
        }
        for &j in &prefix {
            nonessential[j] = true;
        }
        // 4. The next candidate is the smallest current doc over the
        // essential terms only.
        let Some(doc) = state
            .iter()
            .zip(&nonessential)
            .filter(|&(_, &ne)| !ne)
            .map(|(ts, _)| ts.cursor.doc_id())
            .min()
        else {
            unreachable!("block_sum not inert but every term non-essential");
        };
        if doc > window_end {
            // The shallowest term is necessarily non-essential (an
            // essential one would place doc <= window_end), so its docs
            // up to window_end are either formed of non-essential terms
            // alone (inert by the partition) or already evaluated.
            // Advancing it past the window end is safe.
            for ts in state.iter_mut() {
                if ts.cursor.block_last_doc() == window_end {
                    ts.cursor.advance_shallow(window_end.saturating_add(1));
                    ts.refresh(params, avgdl);
                }
            }
            retain_live(&mut state, &mut finished);
            continue;
        }
        // 5. Candidate test with an exact bound: essential terms
        // contribute their TRUE value when present (zero when absent —
        // their cursor proves it), non-essential terms their block max.
        // Every candidate is processed exactly once and strictly in
        // doc-id order: the inert-drop path below advances ALL cursors
        // past the doc, so no cursor can lag the wavefront.
        if let Some(prev) = last_selected {
            debug_assert!(
                doc > prev,
                "candidate selection out of doc order: {doc} after {prev}"
            );
        }
        last_selected = Some(doc);
        let doc_len = store.doc_length(doc);
        let mut bound = 0.0;
        for ti in 0..terms.len() {
            if let Some(pos) = state.iter().position(|ts| ts.ti == ti) {
                let ts = &state[pos];
                if nonessential[pos] {
                    bound += ts.block_max;
                } else if ts.cursor.doc_id() == doc {
                    bound += ts.idf * tf_norm(params, ts.cursor.tf(), doc_len, avgdl);
                }
            }
        }
        if inert(lift(bound)) {
            // Not insertable now, and the floor only rises: drop it.
            // Advance EVERY cursor past the doc — essential and
            // non-essential alike — so candidate selection stays
            // strictly doc-id ordered. Sound: an unconsumed doc behind
            // the wavefront has postings only in currently
            // non-essential terms (every essential cursor sits at or
            // past the wavefront, so its earlier postings are already
            // consumed), the partition just proved the sum of those
            // terms' block bounds inert — valid over the whole window,
            // which covers the wavefront — and inertness only
            // strengthens as the floor and the k-th best rise
            // (`inert` is monotone in both). Such docs can never become
            // insertable later, so skipping them changes nothing.
            for ts in state.iter_mut() {
                ts.cursor.advance_shallow(doc);
                if ts.cursor.doc_id() == doc {
                    ts.cursor.next_posting();
                }
                ts.refresh(params, avgdl);
            }
            retain_live(&mut state, &mut finished);
            continue;
        }
        // 6. Full evaluation: every cursor to the doc (within-block —
        // the window covers it), contributions in term order, insert on
        // the exact contract, advance.
        touched.clear();
        let mut mask: u128 = 0;
        for ts in state.iter_mut() {
            ts.cursor.advance_shallow(doc);
            if ts.cursor.doc_id() == doc {
                contrib[ts.ti] = ts.idf * tf_norm(params, ts.cursor.tf(), doc_len, avgdl);
                touched.push(ts.ti);
                mask |= 1u128 << ts.ti;
            }
        }
        touched.sort_unstable();
        let mut score = 0.0;
        for &ti in &touched {
            score += contrib[ti];
        }
        // The FINAL score: chain applied after the pinned-order sum,
        // before the floor test and insertion, so the heap and kth are
        // on the same scale the lifted bounds are.
        let score = finish(score, doc);
        prune.candidates_evaluated += 1;
        prune.postings_scored += touched.len() as u64;
        // Insert on the exact contract: ties at the seed survive,
        // displacement is strictly-greater — and, first, the document
        // must survive every geo filter. A filtered document is still a
        // fully evaluated candidate (it was scored, and the counters
        // above say so); it simply never enters the heap, and the
        // cursor advance below runs for it unchanged.
        if passes(filter, doc) && score >= floor && (!heap_full || score > kth) {
            if heap_full {
                heap.pop();
            }
            heap.push(HeapEntry {
                score,
                doc_id: doc,
                mask,
            });
        }
        for ts in state.iter_mut() {
            if ts.cursor.doc_id() == doc {
                ts.cursor.next_posting();
                ts.refresh(params, avgdl);
            }
        }
        retain_live(&mut state, &mut finished);
    }
    prune.blocks_skipped =
        finished.0 + state.iter().map(|ts| ts.cursor.blocks_skipped).sum::<u64>();
    prune.l1_groups_skipped = finished.1
        + state
            .iter()
            .map(|ts| ts.cursor.l1_groups_skipped)
            .sum::<u64>();

    let mut out: Vec<HeapEntry> = heap.into_vec();
    out.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.doc_id.cmp(&b.doc_id))
    });
    out.into_iter()
        .map(|e| ScoredDoc {
            doc_id: e.doc_id,
            score: e.score,
            term_offsets: (0..terms.len())
                .filter(|&ti| e.mask >> ti & 1 == 1)
                .map(|ti| (ti, store.posting_offsets(&terms[ti], e.doc_id)))
                .collect(),
        })
        .collect()
}

/// `scale * max over the block frontier of tf_norm(..)` — the term's
/// upper bound anywhere in the cursor's current block. The scale is idf
/// for a single-field term, `w_f * idf` for a fused (field, term) pair.
fn block_max(
    cursor: &crate::postings::ImpactCursor,
    scale: f64,
    params: Bm25Params,
    avgdl: f64,
) -> f64 {
    scale
        * cursor
            .block_frontier()
            .iter()
            .map(|&(tf, dl)| tf_norm(params, tf, dl, avgdl))
            .fold(0.0, f64::max)
}

/// [`top_k_pruned`] generalized to the fused multi-field query
/// (`docs/multi-field.md`, build order step 3): block-max top-k over
/// every (field, term) pair's skip run, bit-identical to
/// [`top_k_fused_exhaustive`] (with the seed filter applied).
///
/// One cursor per scored (field, term) pair, its bounds scaled by
/// `w_f * idf` — per-field saturation means every single-field bound
/// argument holds per pair unchanged, and a fused upper bound is the
/// sum of pair bounds ("floors decompose", `docs/multi-field.md`). The
/// pinned accumulation order extends across fields: every bound sum
/// and every candidate score accumulates in field-id-then-term-index
/// order, so bounds dominate the true score in IEEE arithmetic exactly
/// (correctly-rounded `+` and `*` by a non-negative scale are
/// monotone) and full evaluations reproduce the exhaustive scorer's
/// bits. The skip/heap contract is [`top_k_pruned`]'s verbatim: skips
/// on `bound <= cutoff`, candidates in doc-id order, strictly-greater
/// displacement, `score >= floor` keeps ties at a seeded floor.
///
/// Falls back to [`top_k_fused_exhaustive`] (floor-filtered) when any
/// scored pair lacks impacts (heap store, v3/v4 files), when the pair
/// count exceeds 128 (the membership mask width), or when any field
/// weight is negative or NaN — the bound algebra needs `w_f >= 0`;
/// negative weights are not a scoring mode, but the fallback keeps the
/// function total.
pub fn top_k_fused_pruned(fields: &[FieldQuery], k: usize, floor: f64) -> Vec<FusedDoc> {
    let mut prune = PruneStats::default();
    top_k_fused_pruned_stats(fields, k, floor, &mut prune)
}

/// [`top_k_fused_pruned`] with skip accounting.
pub fn top_k_fused_pruned_stats(
    fields: &[FieldQuery],
    k: usize,
    floor: f64,
    prune: &mut PruneStats,
) -> Vec<FusedDoc> {
    top_k_fused_pruned_filtered_stats(fields, k, floor, None, prune)
}

/// [`top_k_fused_pruned_stats`] with geo filters
/// (`docs/geo-columns.md`), gating heap insertion and nothing else —
/// see [`top_k_pruned_chained_filtered_stats`] for why that is the only
/// place a filter belongs in a block-max loop.
pub fn top_k_fused_pruned_filtered_stats(
    fields: &[FieldQuery],
    k: usize,
    floor: f64,
    filter: FilterCtx,
    prune: &mut PruneStats,
) -> Vec<FusedDoc> {
    // The pinned accumulation order: oi enumerates (field id, term
    // index) lexicographically.
    let pair_meta: Vec<(usize, usize)> = fields
        .iter()
        .enumerate()
        .flat_map(|(fi, fq)| (0..fq.terms.len()).map(move |ti| (fi, ti)))
        .collect();
    let n_pairs = pair_meta.len();
    if n_pairs > 128
        || fields
            .iter()
            .any(|fq| fq.weight < 0.0 || fq.weight.is_nan())
    {
        return filter_fused_to_floor(top_k_fused_exhaustive_filtered(fields, k, filter), floor);
    }

    // One cursor per scored pair. Any missing impact surface falls the
    // whole query back to the exhaustive scorer.
    struct PairState<'a> {
        /// Position in the pinned (field, term) accumulation order.
        oi: usize,
        /// `w_f * idf`: the scale on every tf_norm from this pair.
        widf: f64,
        /// The pair's field k1/b and avgdl (bounds and contributions
        /// are per-field functions).
        params: Bm25Params,
        avgdl: f64,
        cursor: crate::postings::ImpactCursor<'a>,
        /// widf-scaled upper bound of the current level-0 block.
        block_max: f64,
        block: u32,
        /// widf-scaled upper bound of the current level-1 group.
        l1_max: f64,
        l1_group: u32,
        /// widf-scaled upper bound over the whole pair (from level-1
        /// records; static, so always a valid remainder bound).
        term_max: f64,
    }
    impl PairState<'_> {
        /// Re-derive bounds after the cursor moved (no-op when it
        /// stayed inside the current block).
        fn refresh(&mut self) {
            if self.cursor.block() == self.block {
                return;
            }
            self.block = self.cursor.block();
            self.block_max = block_max(&self.cursor, self.widf, self.params, self.avgdl);
            if self.cursor.l1_group() != self.l1_group {
                self.l1_group = self.cursor.l1_group();
                self.l1_max = self.widf
                    * self
                        .cursor
                        .l1_frontier()
                        .iter()
                        .map(|&(tf, dl)| tf_norm(self.params, tf, dl, self.avgdl))
                        .fold(0.0, f64::max);
            }
        }
    }
    /// Drop exhausted cursors, harvesting their skip counters.
    fn retain_live(state: &mut Vec<PairState>, skips: &mut (u64, u64)) {
        state.retain(|ps| {
            if ps.cursor.exhausted() {
                skips.0 += ps.cursor.blocks_skipped;
                skips.1 += ps.cursor.l1_groups_skipped;
                false
            } else {
                true
            }
        });
    }
    let mut state: Vec<PairState> = Vec::new();
    for (oi, &(fi, ti)) in pair_meta.iter().enumerate() {
        let fq = &fields[fi];
        debug_assert_eq!(fq.terms.len(), fq.stats.dfs.len());
        // A (field, term) pair absent from THIS shard contributes 0 to
        // every document here, so it is skipped rather than scored.
        // `fq.stats.dfs` is the GLOBAL df, which is non-zero for a term
        // that merely lives on another shard; checking it alone sends
        // every such query down the exhaustive path.
        if fq.stats.dfs[ti] == 0 || fq.index.df(&fq.terms[ti]) == 0 {
            continue;
        }
        let Some(cursor) = fq.index.impacts(&fq.terms[ti]) else {
            // Present here but no impact surface: a genuine format
            // limitation, and the only case that still forfeits pruning.
            return filter_fused_to_floor(
                top_k_fused_exhaustive_filtered(fields, k, filter),
                floor,
            );
        };
        let avgdl = fq.stats.avgdl();
        let widf = fq.weight * idf(fq.stats.doc_count, fq.stats.dfs[ti]);
        let l1_max = widf
            * cursor
                .l1_frontier()
                .iter()
                .map(|&(tf, dl)| tf_norm(fq.params, tf, dl, avgdl))
                .fold(0.0, f64::max);
        let term_max = widf
            * cursor
                .term_frontier()
                .iter()
                .map(|&(tf, dl)| tf_norm(fq.params, tf, dl, avgdl))
                .fold(0.0, f64::max);
        prune.blocks_total += u64::from(cursor.n_blocks());
        state.push(PairState {
            oi,
            widf,
            params: fq.params,
            avgdl,
            block: cursor.block(),
            block_max: block_max(&cursor, widf, fq.params, avgdl),
            l1_group: cursor.l1_group(),
            l1_max,
            cursor,
            term_max,
        });
    }

    let mut heap: std::collections::BinaryHeap<HeapEntry> = std::collections::BinaryHeap::new();
    if k == 0 {
        return Vec::new();
    }
    // Reusable per-candidate accumulation, indexed by oi so the sum
    // runs in the pinned pair order (see [`top_k_pruned_stats`]; the
    // loop below is that function pair-generalized, step for step).
    let mut contrib: Vec<f64> = vec![0.0; n_pairs];
    let mut touched: Vec<usize> = Vec::new();
    let mut order: Vec<usize> = Vec::new();
    let mut nonessential: Vec<bool> = Vec::new();
    let mut prefix: Vec<usize> = Vec::new();
    // Per-candidate document length by field id (dl is per field).
    let mut dls: Vec<u32> = Vec::new();
    // Skip counts (level-0 blocks, level-1 groups) of retired cursors.
    let mut finished: (u64, u64) = (0, 0);
    // Last processed candidate doc id (doc-order invariant witness).
    let mut last_selected: Option<u32> = None;

    while !state.is_empty() {
        let heap_full = heap.len() >= k;
        let kth = if heap_full {
            heap.peek().expect("full heap").score
        } else {
            f64::NEG_INFINITY
        };
        // Inert = cannot enter the heap; every bound sum below runs in
        // pinned pair order (see the contract).
        let inert = |acc: f64| acc < floor || (heap_full && acc <= kth);
        let mut static_sum = 0.0;
        let mut l1_sum = 0.0;
        let mut block_sum = 0.0;
        for oi in 0..n_pairs {
            if let Some(ps) = state.iter().find(|ps| ps.oi == oi) {
                static_sum += ps.term_max;
                l1_sum += ps.l1_max;
                block_sum += ps.block_max;
            }
        }
        // 1. Termination on the static whole-pair bounds.
        if inert(static_sum) {
            break;
        }
        // 2a. Level-1 skip: whole 4096-posting groups at one test.
        if inert(l1_sum) {
            let d = state
                .iter()
                .map(|ps| ps.cursor.l1_last_doc())
                .min()
                .expect("nonempty state");
            for ps in state.iter_mut() {
                ps.cursor.advance_shallow(d.saturating_add(1));
                ps.refresh();
            }
            retain_live(&mut state, &mut finished);
            continue;
        }
        // 2b. Level-0 range skip.
        if inert(block_sum) {
            let d = state
                .iter()
                .map(|ps| ps.cursor.block_last_doc())
                .min()
                .expect("nonempty state");
            for ps in state.iter_mut() {
                ps.cursor.advance_shallow(d.saturating_add(1));
                ps.refresh();
            }
            retain_live(&mut state, &mut finished);
            continue;
        }
        // 3. Competitive window [_, window_end]: MaxScore partition
        // over pairs, exactly the single-field step 3 with ti -> oi.
        let window_end = state
            .iter()
            .map(|ps| ps.cursor.block_last_doc())
            .min()
            .expect("nonempty state");
        order.clear();
        order.extend(0..state.len());
        order.sort_by(|&a, &b| {
            state[a]
                .block_max
                .partial_cmp(&state[b].block_max)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        nonessential.clear();
        nonessential.resize(state.len(), false);
        prefix.clear();
        for &j in &order {
            let mut acc = 0.0;
            for oi in 0..n_pairs {
                if let Some(pos) = state.iter().position(|ps| ps.oi == oi) {
                    if pos == j || prefix.contains(&pos) {
                        acc += state[pos].block_max;
                    }
                }
            }
            if inert(acc) {
                prefix.push(j);
            } else {
                break;
            }
        }
        for &j in &prefix {
            nonessential[j] = true;
        }
        // 4. The next candidate is the smallest current doc over the
        // essential pairs only.
        let Some(doc) = state
            .iter()
            .zip(&nonessential)
            .filter(|&(_, &ne)| !ne)
            .map(|(ps, _)| ps.cursor.doc_id())
            .min()
        else {
            unreachable!("block_sum not inert but every pair non-essential");
        };
        if doc > window_end {
            // The shallowest pair is necessarily non-essential (an
            // essential one would place doc <= window_end); advancing
            // it past the window end is safe (see the single-field
            // step 4 argument, which is per cursor and field-blind).
            for ps in state.iter_mut() {
                if ps.cursor.block_last_doc() == window_end {
                    ps.cursor.advance_shallow(window_end.saturating_add(1));
                    ps.refresh();
                }
            }
            retain_live(&mut state, &mut finished);
            continue;
        }
        // 5. Candidate test with an exact bound: essential pairs their
        // TRUE contribution when present, non-essential their block
        // max. Doc-order and soundness arguments are the single-field
        // step 5's, per cursor and field-blind.
        if let Some(prev) = last_selected {
            debug_assert!(
                doc > prev,
                "candidate selection out of doc order: {doc} after {prev}"
            );
        }
        last_selected = Some(doc);
        dls.clear();
        dls.extend(fields.iter().map(|fq| fq.index.doc_length(doc)));
        let mut bound = 0.0;
        for oi in 0..n_pairs {
            if let Some(pos) = state.iter().position(|ps| ps.oi == oi) {
                let ps = &state[pos];
                if nonessential[pos] {
                    bound += ps.block_max;
                } else if ps.cursor.doc_id() == doc {
                    bound += ps.widf
                        * tf_norm(ps.params, ps.cursor.tf(), dls[pair_meta[oi].0], ps.avgdl);
                }
            }
        }
        if inert(bound) {
            // Not insertable now, and the floor only rises: drop it,
            // advancing EVERY cursor past the doc (the single-field
            // step 5 soundness proof, applied to pairs).
            for ps in state.iter_mut() {
                ps.cursor.advance_shallow(doc);
                if ps.cursor.doc_id() == doc {
                    ps.cursor.next_posting();
                }
                ps.refresh();
            }
            retain_live(&mut state, &mut finished);
            continue;
        }
        // 6. Full evaluation: every cursor to the doc, contributions
        // summed in pinned pair order, insert on the exact contract.
        touched.clear();
        let mut mask: u128 = 0;
        for ps in state.iter_mut() {
            ps.cursor.advance_shallow(doc);
            if ps.cursor.doc_id() == doc {
                contrib[ps.oi] =
                    ps.widf * tf_norm(ps.params, ps.cursor.tf(), dls[pair_meta[ps.oi].0], ps.avgdl);
                touched.push(ps.oi);
                mask |= 1u128 << ps.oi;
            }
        }
        touched.sort_unstable();
        let mut score = 0.0;
        for &oi in &touched {
            score += contrib[oi];
        }
        prune.candidates_evaluated += 1;
        prune.postings_scored += touched.len() as u64;
        // Insert on the exact contract: ties at the seed survive,
        // displacement is strictly-greater, and the document must
        // survive every geo filter first (see the flat scorer).
        if passes(filter, doc) && score >= floor && (!heap_full || score > kth) {
            if heap_full {
                heap.pop();
            }
            heap.push(HeapEntry {
                score,
                doc_id: doc,
                mask,
            });
        }
        for ps in state.iter_mut() {
            if ps.cursor.doc_id() == doc {
                ps.cursor.next_posting();
                ps.refresh();
            }
        }
        retain_live(&mut state, &mut finished);
    }
    prune.blocks_skipped =
        finished.0 + state.iter().map(|ps| ps.cursor.blocks_skipped).sum::<u64>();
    prune.l1_groups_skipped = finished.1
        + state
            .iter()
            .map(|ps| ps.cursor.l1_groups_skipped)
            .sum::<u64>();

    let mut out: Vec<HeapEntry> = heap.into_vec();
    out.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.doc_id.cmp(&b.doc_id))
    });
    out.into_iter()
        .map(|e| FusedDoc {
            doc_id: e.doc_id,
            score: e.score,
            term_offsets: (0..n_pairs)
                .filter(|&oi| e.mask >> oi & 1 == 1)
                .map(|oi| {
                    let (fi, ti) = pair_meta[oi];
                    (
                        fi,
                        ti,
                        fields[fi]
                            .index
                            .posting_offsets(&fields[fi].terms[ti], e.doc_id),
                    )
                })
                .collect(),
        })
        .collect()
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
mod tests {
    use super::*;
    use crate::postings::{AnalyzedDoc, AnalyzedField, Bm25Store};

    /// Hand-computed BM25 on a 3-doc corpus (N=3, avgdl=3):
    /// doc0 "rust rust search", doc1 "rust vector vector", doc2 "search".
    /// Query "rust": df=2.
    #[test]
    fn bm25_matches_hand_computed_values() {
        let mut store = Bm25Store::new();
        store.add_document(
            0,
            "a".to_string(),
            AnalyzedDoc::body(
                vec![("rust".into(), 2, vec![]), ("search".into(), 1, vec![])],
                3,
            ),
        );
        store.add_document(
            1,
            "b".to_string(),
            AnalyzedDoc::body(
                vec![("rust".into(), 1, vec![]), ("vector".into(), 2, vec![])],
                3,
            ),
        );
        store.add_document(
            2,
            "c".to_string(),
            AnalyzedDoc::body(vec![("search".into(), 1, vec![])], 3),
        );

        let params = Bm25Params::default();
        let stats = CorpusStats {
            doc_count: 3,
            total_doc_length: 9,
            dfs: vec![2],
        };
        let terms = vec!["rust".to_string()];
        let hits = top_k(&store, &terms, &stats, params, 10);
        assert_eq!(hits.len(), 2);

        // idf = ln(1 + (3 - 2 + 0.5)/(2 + 0.5)) = ln(1.6)
        let idf = (1.6f64).ln();
        let expect = |tf: f64, dl: f64| idf * tf * 2.2 / (tf + 1.2 * (0.25 + 0.75 * dl / 3.0));
        assert_eq!(hits[0].doc_id, 0);
        assert!((hits[0].score - expect(2.0, 3.0)).abs() < 1e-12);
        assert_eq!(hits[1].doc_id, 1);
        assert!((hits[1].score - expect(1.0, 3.0)).abs() < 1e-12);
        // tf saturation: doc0 (tf=2) beats doc1 (tf=1), sub-linearly.
        assert!(hits[0].score > hits[1].score);
        assert!(hits[0].score < 2.0 * hits[1].score);
    }

    #[test]
    fn length_normalization_penalizes_long_docs() {
        let params = Bm25Params::default();
        let avgdl = 4.0;
        let short = tf_norm(params, 1, 2, avgdl);
        let long = tf_norm(params, 1, 8, avgdl);
        assert!(short > long);
        // b=0 disables length normalization.
        let no_norm = Bm25Params { k1: 1.2, b: 0.0 };
        assert_eq!(tf_norm(no_norm, 1, 2, avgdl), tf_norm(no_norm, 1, 8, avgdl));
    }

    #[test]
    fn idf_is_positive_and_decreasing_in_df() {
        let a = idf(100, 1);
        let b = idf(100, 50);
        let c = idf(100, 100);
        assert!(a > b && b > c && c > 0.0);
    }

    #[test]
    fn merge_stats_sums_shares() {
        let merged = merge_stats(&[
            (10, 300, vec![2, 0, 5]),
            (5, 100, vec![1, 3, 0]),
            (0, 0, vec![0, 0, 0]),
        ]);
        assert_eq!(merged.doc_count, 15);
        assert_eq!(merged.total_doc_length, 400);
        assert_eq!(merged.dfs, vec![3, 3, 5]);
        assert!((merged.avgdl() - 400.0 / 15.0).abs() < 1e-12);
    }

    #[test]
    fn empty_corpus_scores_nothing() {
        let store = Bm25Store::new();
        let stats = CorpusStats {
            doc_count: 0,
            total_doc_length: 0,
            dfs: vec![0],
        };
        let hits = top_k(
            &store,
            &["rust".to_string()],
            &stats,
            Bm25Params::default(),
            10,
        );
        assert!(hits.is_empty());
    }

    /// A single body field at weight 1.0 fuses to exactly the
    /// single-field exhaustive scorer, bit for bit (`1.0 * x == x`).
    #[test]
    fn fused_single_field_matches_exhaustive_bitwise() {
        let mut store = Bm25Store::new();
        for i in 0..30u32 {
            let terms = vec![
                ("rust".to_string(), 1 + i % 3, vec![(i, i + 4)]),
                (format!("t{}", i % 5), 1, vec![(i + 6, i + 8)]),
            ];
            let length = terms.iter().map(|t| t.1).sum();
            store.add_document(i, format!("doc {i}"), AnalyzedDoc::body(terms, length));
        }
        let terms = vec!["rust".to_string(), "t2".to_string()];
        let stats = CorpusStats {
            doc_count: store.doc_count(),
            total_doc_length: store.total_doc_length(),
            dfs: terms.iter().map(|t| store.df(t)).collect(),
        };
        let params = Bm25Params { k1: 0.9, b: 0.4 };

        let want = top_k_exhaustive(&store, &terms, &stats, params, 10);
        let fused = top_k_fused_exhaustive(
            &[FieldQuery {
                index: &store,
                terms: &terms,
                stats: stats.clone(),
                params,
                weight: 1.0,
            }],
            10,
        );
        assert_eq!(fused.len(), want.len());
        for (f, w) in fused.iter().zip(&want) {
            assert_eq!(f.doc_id, w.doc_id);
            assert_eq!(f.score.to_bits(), w.score.to_bits(), "doc {}", w.doc_id);
            let mapped: Vec<FusedTermOffset> = w
                .term_offsets
                .iter()
                .map(|(ti, o)| (0, *ti, o.clone()))
                .collect();
            assert_eq!(f.term_offsets, mapped, "doc {}", w.doc_id);
        }
    }

    /// Hand-computed fused scores on a two-field corpus, per-field
    /// params and query-time weights included; the expectation is
    /// accumulated in the pinned field-id-then-term-index order, so the
    /// comparison is bitwise. Reweighting reorders without a rebuild.
    #[test]
    fn fused_two_fields_hand_computed() {
        let mut store = Bm25Store::with_fields(&["body", "name"]);
        // doc0: body "rust rust search", name "smith"
        store.add_document(
            0,
            "a".to_string(),
            AnalyzedDoc {
                fields: vec![
                    AnalyzedField {
                        terms: vec![
                            ("rust".into(), 2, vec![(0, 4)]),
                            ("search".into(), 1, vec![]),
                        ],
                        length: 3,
                    },
                    AnalyzedField {
                        terms: vec![("smith".into(), 1, vec![(0, 5)])],
                        length: 1,
                    },
                ],
                quality: None,
                geography: None,
            },
        );
        // doc1: body "rust vector vector", no name
        store.add_document(
            1,
            "b".to_string(),
            AnalyzedDoc::body(
                vec![("rust".into(), 1, vec![]), ("vector".into(), 2, vec![])],
                3,
            ),
        );
        // doc2: body "search", name "rust smith"
        store.add_document(
            2,
            "c".to_string(),
            AnalyzedDoc {
                fields: vec![
                    AnalyzedField {
                        terms: vec![("search".into(), 1, vec![])],
                        length: 1,
                    },
                    AnalyzedField {
                        terms: vec![
                            ("rust".into(), 1, vec![(0, 4)]),
                            ("smith".into(), 1, vec![]),
                        ],
                        length: 2,
                    },
                ],
                quality: None,
                geography: None,
            },
        );

        let body_terms = vec!["rust".to_string()];
        let name_terms = vec!["rust".to_string(), "smith".to_string()];
        let body_params = Bm25Params::default();
        let name_params = Bm25Params { k1: 0.5, b: 0.0 };
        // N shared; totals and dfs per field.
        let body_stats = CorpusStats {
            doc_count: 3,
            total_doc_length: 7,
            dfs: vec![2],
        };
        let name_stats = CorpusStats {
            doc_count: 3,
            total_doc_length: 3,
            dfs: vec![1, 2],
        };
        let fused = |w_name: f64| {
            top_k_fused_exhaustive(
                &[
                    FieldQuery {
                        index: &store.field(0),
                        terms: &body_terms,
                        stats: body_stats.clone(),
                        params: body_params,
                        weight: 1.0,
                    },
                    FieldQuery {
                        index: &store.field(1),
                        terms: &name_terms,
                        stats: name_stats.clone(),
                        params: name_params,
                        weight: w_name,
                    },
                ],
                10,
            )
        };

        let hits = fused(2.0);
        assert_eq!(hits.len(), 3);
        // Expectations in the pinned accumulation order.
        let body_avgdl = body_stats.avgdl();
        let name_avgdl = name_stats.avgdl();
        // doc2: no body hit; name rust then name smith.
        let d2 = 2.0 * idf(3, 1) * tf_norm(name_params, 1, 2, name_avgdl)
            + 2.0 * idf(3, 2) * tf_norm(name_params, 1, 2, name_avgdl);
        // doc0: body rust, then name smith.
        let d0 = 1.0 * idf(3, 2) * tf_norm(body_params, 2, 3, body_avgdl)
            + 2.0 * idf(3, 2) * tf_norm(name_params, 1, 1, name_avgdl);
        // doc1: body rust only.
        let d1 = 1.0 * idf(3, 2) * tf_norm(body_params, 1, 3, body_avgdl);
        assert_eq!(hits[0].doc_id, 2);
        assert_eq!(hits[0].score.to_bits(), d2.to_bits());
        assert_eq!(
            hits[0].term_offsets,
            vec![(1, 0, vec![(0, 4)]), (1, 1, vec![])]
        );
        assert_eq!(hits[1].doc_id, 0);
        assert_eq!(hits[1].score.to_bits(), d0.to_bits());
        assert_eq!(hits[2].doc_id, 1);
        assert_eq!(hits[2].score.to_bits(), d1.to_bits());

        // The name field dominant at w=2.0, near-mute at w=0.1: the
        // ranking flips with no index change.
        let hits = fused(0.1);
        assert_eq!(hits[0].doc_id, 0);
        assert_eq!(hits[1].doc_id, 1);
        assert_eq!(hits[2].doc_id, 2);
    }

    /// Contract 3 of `docs/multi-field.md` at the store level: fused
    /// multi-field scoring over two shard stores with per-field GLOBAL
    /// stats, merged by (score desc, global id asc), equals monolithic
    /// fused scoring of the union corpus bit for bit.
    #[test]
    fn fused_distributed_equals_monolithic() {
        // The document for global id `g`, identical however the corpus
        // is sharded.
        fn doc_for(g: u32) -> AnalyzedDoc {
            let body = vec![
                ("rust".to_string(), 1 + g % 3, vec![(g, g + 2)]),
                (format!("t{}", g % 4), 1, vec![(g + 3, g + 5)]),
            ];
            let body_len: u32 = body.iter().map(|t| t.1).sum();
            let mut fields = vec![AnalyzedField {
                terms: body,
                length: body_len,
            }];
            if g % 3 != 1 {
                fields.push(AnalyzedField {
                    terms: vec![
                        ("smith".to_string(), 1, vec![(0, 5)]),
                        (format!("n{}", g % 2), 1, vec![]),
                    ],
                    length: 2,
                });
            }
            AnalyzedDoc {
                fields,
                quality: None,
                geography: None,
            }
        }
        fn build(range: std::ops::Range<u32>, offset: u32) -> Bm25Store {
            let mut store = Bm25Store::with_fields(&["body", "name"]);
            for g in range {
                store.add_document(g - offset, format!("doc {g}"), doc_for(g));
            }
            store
        }
        let monolith = build(0..50, 0);
        let shards = [(build(0..23, 0), 0u32), (build(23..50, 23), 23u32)];

        let body_terms = vec!["rust".to_string(), "t2".to_string()];
        let name_terms = vec!["smith".to_string(), "n1".to_string()];
        let body_params = Bm25Params::default();
        let name_params = Bm25Params { k1: 0.8, b: 0.3 };
        let weights = [1.0, 1.7];

        // Per-field global stats from per-shard shares (the TermStats
        // flow): N is the shared any-field count, totals and dfs are
        // per field.
        let field_stats = |f: usize, terms: &[String]| {
            let shares: Vec<(u64, u64, Vec<u32>)> = shards
                .iter()
                .map(|(s, _)| {
                    (
                        s.doc_count(),
                        s.field(f).total_doc_length(),
                        terms.iter().map(|t| s.field(f).df(t)).collect(),
                    )
                })
                .collect();
            merge_stats(&shares)
        };
        let body_stats = field_stats(0, &body_terms);
        let name_stats = field_stats(1, &name_terms);
        assert_eq!(body_stats.doc_count, monolith.doc_count());
        assert_eq!(
            body_stats.total_doc_length,
            monolith.field(0).total_doc_length()
        );
        assert_eq!(
            name_stats.total_doc_length,
            monolith.field(1).total_doc_length()
        );

        let k = 12;
        let run = |store: &Bm25Store| {
            top_k_fused_exhaustive(
                &[
                    FieldQuery {
                        index: &store.field(0),
                        terms: &body_terms,
                        stats: body_stats.clone(),
                        params: body_params,
                        weight: weights[0],
                    },
                    FieldQuery {
                        index: &store.field(1),
                        terms: &name_terms,
                        stats: name_stats.clone(),
                        params: name_params,
                        weight: weights[1],
                    },
                ],
                k,
            )
        };

        let want = run(&monolith);
        let mut merged: Vec<FusedDoc> = shards
            .iter()
            .flat_map(|(s, offset)| {
                run(s).into_iter().map(move |mut d| {
                    d.doc_id += offset;
                    d
                })
            })
            .collect();
        merged.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.doc_id.cmp(&b.doc_id))
        });
        merged.truncate(k);

        assert_eq!(want.len(), merged.len());
        for (w, m) in want.iter().zip(&merged) {
            assert_eq!(w.doc_id, m.doc_id);
            assert_eq!(w.score.to_bits(), m.score.to_bits(), "doc {}", w.doc_id);
            assert_eq!(w.term_offsets, m.term_offsets, "doc {}", w.doc_id);
        }
    }
}

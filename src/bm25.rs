//! BM25 scoring over postings with externally supplied (global) corpus
//! stats.
//!
//! Scores are comparable across shards only when every shard scores with
//! the same corpus stats, so NOTHING here reads shard-local stats: the
//! caller supplies N, total document length, and per-term df (the
//! coordinator's summed globals). The shard-local share of those stats
//! lives on [`crate::postings::Bm25Store`] and is exposed via TermStats.

use crate::postings::{Bm25Index, Posting};

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
    debug_assert_eq!(terms.len(), stats.dfs.len());
    let avgdl = stats.avgdl();
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
            .map(|(doc_id, (score, mask))| (doc_id, score, mask))
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
            .map(|(doc_id, (score, tis))| (doc_id, score, tis))
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
        .map(|(doc_id, (score, term_offsets))| ScoredDoc {
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
    debug_assert_eq!(terms.len(), stats.dfs.len());
    let avgdl = stats.avgdl();

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
        return filter_to_floor(top_k(store, terms, stats, params, k), floor);
    }
    for (ti, term) in terms.iter().enumerate() {
        if stats.dfs[ti] == 0 {
            continue;
        }
        let Some(cursor) = store.impacts(term) else {
            return filter_to_floor(top_k(store, terms, stats, params, k), floor);
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
        if inert(static_sum) {
            break;
        }
        // 2a. Level-1 skip: whole 4096-posting groups at one test.
        if inert(l1_sum) {
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
        if inert(block_sum) {
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
        if inert(bound) {
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
        prune.candidates_evaluated += 1;
        prune.postings_scored += touched.len() as u64;
        // Insert on the exact contract: ties at the seed survive,
        // displacement is strictly-greater.
        if score >= floor && (!heap_full || score > kth) {
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
    prune.blocks_skipped = finished.0 + state.iter().map(|ts| ts.cursor.blocks_skipped).sum::<u64>();
    prune.l1_groups_skipped =
        finished.1 + state.iter().map(|ts| ts.cursor.l1_groups_skipped).sum::<u64>();

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

/// `idf * max over the block frontier of tf_norm(..)` — the term's upper
/// bound anywhere in the cursor's current block.
fn block_max(
    cursor: &crate::postings::ImpactCursor,
    idf: f64,
    params: Bm25Params,
    avgdl: f64,
) -> f64 {
    idf * cursor
        .block_frontier()
        .iter()
        .map(|&(tf, dl)| tf_norm(params, tf, dl, avgdl))
        .fold(0.0, f64::max)
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
    use crate::postings::{AnalyzedDoc, Bm25Store};

    /// Hand-computed BM25 on a 3-doc corpus (N=3, avgdl=3):
    /// doc0 "rust rust search", doc1 "rust vector vector", doc2 "search".
    /// Query "rust": df=2.
    #[test]
    fn bm25_matches_hand_computed_values() {
        let mut store = Bm25Store::new();
        store.add_document(
            0,
            "a".to_string(),
            AnalyzedDoc {
                terms: vec![("rust".into(), 2, vec![]), ("search".into(), 1, vec![])],
                length: 3,
            },
        );
        store.add_document(
            1,
            "b".to_string(),
            AnalyzedDoc {
                terms: vec![("rust".into(), 1, vec![]), ("vector".into(), 2, vec![])],
                length: 3,
            },
        );
        store.add_document(
            2,
            "c".to_string(),
            AnalyzedDoc {
                terms: vec![("search".into(), 1, vec![])],
                length: 3,
            },
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
}

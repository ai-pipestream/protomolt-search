//! Scoring and explaining a known candidate set rather than a top-k.

use super::{idf, tf_norm, Bm25Params, CorpusStats, FieldQuery, ScoredDoc};
use crate::postings::{Bm25Index, ImpactCursor};

/// Visit every candidate the cursor's term occurs in, positioning the
/// cursor on each in turn. `candidates` is sorted ascending and distinct.
fn visit_by_cursor(
    cursor: &mut ImpactCursor,
    candidates: &[u32],
    visit: &mut dyn FnMut(usize, &ImpactCursor),
) {
    for (ci, &cand) in candidates.iter().enumerate() {
        if cursor.exhausted() {
            break;
        }
        cursor.advance_shallow(cand);
        if cursor.doc_id() == cand {
            visit(ci, cursor);
        }
    }
}

/// Merge-join the sorted candidates against `term`'s posting stream,
/// visiting `(candidate index, doc id, tf)` for each match.
fn visit_by_postings(
    index: &dyn Bm25Index,
    term: &str,
    candidates: &[u32],
    visit: &mut dyn FnMut(usize, u32, u32),
) {
    let mut ci = 0usize;
    index.for_each_doc_tf(term, &mut |doc_id, tf| {
        while ci < candidates.len() && candidates[ci] < doc_id {
            ci += 1;
        }
        if ci < candidates.len() && candidates[ci] == doc_id {
            visit(ci, doc_id, tf);
        }
    });
}

/// Score only the given candidate docs (local ids, any order, duplicates
/// tolerated) against the postings.
///
/// Per term this is a merge-join between the sorted candidates and the
/// term's postings, or a shallow-advance walk of its impact cursor when
/// every scored term has one. Returns one [`ScoredDoc`] per candidate
/// that any term occurs in, doc id ascending.
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
    let cursors = impact_cursors(store, terms, stats);
    // One slot per sorted candidate keeps accumulation O(1) per posting.
    let mut slots: Vec<Option<ScoredDoc>> = candidates.iter().map(|_| None).collect();
    let mut add =
        |ci: usize, doc_id: u32, ti: usize, contribution: f64, offsets: Vec<(u32, u32)>| {
            let entry = slots[ci].get_or_insert_with(|| ScoredDoc {
                doc_id,
                score: 0.0,
                term_offsets: Vec::new(),
            });
            entry.score += contribution;
            entry.term_offsets.push((ti, offsets));
        };
    if let Some(cursors) = cursors {
        for (ti, mut cursor) in cursors {
            let idf = idf(stats.doc_count, stats.dfs[ti]);
            visit_by_cursor(&mut cursor, &candidates, &mut |ci, cursor| {
                let doc = candidates[ci];
                let contribution = idf * tf_norm(params, cursor.tf(), store.doc_length(doc), avgdl);
                add(ci, doc, ti, contribution, cursor.offsets());
            });
        }
    } else {
        for (ti, term) in terms.iter().enumerate() {
            if stats.dfs[ti] == 0 {
                continue;
            }
            let idf = idf(stats.doc_count, stats.dfs[ti]);
            let mut ci = 0usize;
            store.for_each_posting(term, &mut |doc_id, tf, offsets| {
                while ci < candidates.len() && candidates[ci] < doc_id {
                    ci += 1;
                }
                if ci < candidates.len() && candidates[ci] == doc_id {
                    let contribution = idf * tf_norm(params, tf, store.doc_length(doc_id), avgdl);
                    add(ci, doc_id, ti, contribution, offsets.to_vec());
                }
            });
        }
    }
    slots.into_iter().flatten().collect()
}

/// An impact cursor for every scored term, or `None` when any scored term
/// lacks one (heap store, v3/v4 file).
fn impact_cursors<'a>(
    store: &'a dyn Bm25Index,
    terms: &[String],
    stats: &CorpusStats,
) -> Option<Vec<(usize, ImpactCursor<'a>)>> {
    let mut cursors = Vec::new();
    for (ti, term) in terms.iter().enumerate() {
        if stats.dfs[ti] != 0 {
            cursors.push((ti, store.impacts(term)?));
        }
    }
    Some(cursors)
}

/// How [`score_candidates_plain_walk`] visits one term's postings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CandidateWalk {
    /// Per term, whichever of the two walks reads fewer entries: the
    /// candidates when the term has more postings than there are
    /// candidates, the postings otherwise.
    Auto,
    /// Position the term's impact cursor on each candidate in turn
    /// (`advance_shallow`): the cost grows with the candidates.
    Candidates,
    /// Read every posting of the term and merge-join the sorted
    /// candidates against it: the cost grows with the term's df.
    Postings,
}

/// [`score_candidates`] without the offsets: the summed BM25 score of
/// each of `candidates` (local ids, sorted ascending, distinct), parallel
/// to the input, `None` for a candidate no query term occurs in.
///
/// Each candidate takes at most one contribution per term, added in term
/// order, so a score here is bitwise the score [`score_candidates`]
/// computes whichever walk visits the postings.
pub fn score_candidates_plain(
    store: &dyn Bm25Index,
    terms: &[String],
    stats: &CorpusStats,
    params: Bm25Params,
    candidates: &[u32],
) -> Vec<Option<f64>> {
    score_candidates_plain_walk(store, terms, stats, params, candidates, CandidateWalk::Auto)
}

/// [`score_candidates_plain`] with the walk chosen by the caller. A term
/// the store holds no impact cursor for is merge-joined over its postings
/// whatever `walk` says.
pub fn score_candidates_plain_walk(
    store: &dyn Bm25Index,
    terms: &[String],
    stats: &CorpusStats,
    params: Bm25Params,
    candidates: &[u32],
    walk: CandidateWalk,
) -> Vec<Option<f64>> {
    debug_assert_eq!(terms.len(), stats.dfs.len());
    debug_assert!(candidates.windows(2).all(|w| w[0] < w[1]));
    let avgdl = stats.avgdl();
    let mut scores: Vec<Option<f64>> = vec![None; candidates.len()];
    for (ti, term) in terms.iter().enumerate() {
        if stats.dfs[ti] == 0 || candidates.is_empty() {
            continue;
        }
        let idf = idf(stats.doc_count, stats.dfs[ti]);
        let mut add = |ci: usize, doc: u32, tf: u32| {
            let contribution = idf * tf_norm(params, tf, store.doc_length(doc), avgdl);
            *scores[ci].get_or_insert(0.0) += contribution;
        };
        let cursor = store.impacts(term).filter(|cursor| match walk {
            CandidateWalk::Candidates => true,
            CandidateWalk::Postings => false,
            CandidateWalk::Auto => u64::from(cursor.df()) > candidates.len() as u64,
        });
        match cursor {
            Some(mut cursor) => visit_by_cursor(&mut cursor, candidates, &mut |ci, cursor| {
                add(ci, candidates[ci], cursor.tf())
            }),
            None => visit_by_postings(store, term, candidates, &mut add),
        }
    }
    scores
}

/// One (field, term) occurrence in a document, as the explain tree
/// reports it: the raw inputs the contribution is computed from. `field`
/// indexes the `fields` slice handed to [`breakdown`]; `term` indexes
/// that field's terms.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TermPresence {
    pub field: usize,
    pub term: usize,
    pub tf: u32,
    pub doc_len: u32,
}

/// The (field, term) pairs each of `doc_ids` contains, with tf and
/// document length, in accumulation order (fields in slice order, terms
/// in leg order). The result is parallel to `doc_ids`; a term with global
/// df 0 is skipped exactly as the scorers skip it.
pub fn breakdown(fields: &[FieldQuery], doc_ids: &[u32]) -> Vec<Vec<TermPresence>> {
    let mut sorted: Vec<u32> = doc_ids.to_vec();
    sorted.sort_unstable();
    sorted.dedup();
    let mut found: Vec<Vec<TermPresence>> = vec![Vec::new(); sorted.len()];
    for (fi, fq) in fields.iter().enumerate() {
        debug_assert_eq!(fq.terms.len(), fq.stats.dfs.len());
        for (ti, term) in fq.terms.iter().enumerate() {
            if fq.stats.dfs[ti] == 0 {
                continue;
            }
            let mut record = |ci: usize, doc_id: u32, tf: u32| {
                found[ci].push(TermPresence {
                    field: fi,
                    term: ti,
                    tf,
                    doc_len: fq.index.doc_length(doc_id),
                });
            };
            match fq.index.impacts(term) {
                Some(mut cursor) => visit_by_cursor(&mut cursor, &sorted, &mut |ci, cursor| {
                    record(ci, sorted[ci], cursor.tf())
                }),
                None => visit_by_postings(fq.index, term, &sorted, &mut record),
            }
        }
    }
    doc_ids
        .iter()
        .map(|id| {
            let ci = sorted.binary_search(id).expect("every id was sorted in");
            found[ci].clone()
        })
        .collect()
}

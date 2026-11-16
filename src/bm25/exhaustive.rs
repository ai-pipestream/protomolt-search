//! Exhaustive lexical accumulation with wire-score ranking.

use super::request::{Controls, Flat};
use super::{idf, passes, tf_norm, FieldQuery, FusedDoc, Phrase, ScoredDoc};
use std::collections::HashMap;

/// Which query terms a document matched, recorded during accumulation.
trait Members: Default {
    fn insert(&mut self, term: usize);
    fn terms(self, count: usize) -> Vec<usize>;
}

impl Members for u64 {
    fn insert(&mut self, term: usize) {
        *self |= 1u64 << term;
    }

    fn terms(self, count: usize) -> Vec<usize> {
        (0..count).filter(|&ti| self >> ti & 1 == 1).collect()
    }
}

impl Members for Vec<usize> {
    fn insert(&mut self, term: usize) {
        self.push(term);
    }

    fn terms(self, _count: usize) -> Vec<usize> {
        self
    }
}

/// The production exhaustive scorer: accumulates over the doc/tf run and
/// fetches offsets only for the `k` survivors. A finite floor drops
/// documents below it before ranking.
pub(super) fn flat(query: &Flat<'_>, controls: &Controls<'_>) -> Vec<ScoredDoc> {
    debug_assert_eq!(query.terms.len(), query.stats.dfs.len());
    if query.terms.len() <= 64 {
        flat_with::<u64>(query, controls)
    } else {
        flat_with::<Vec<usize>>(query, controls)
    }
}

fn flat_with<M: Members>(query: &Flat<'_>, controls: &Controls<'_>) -> Vec<ScoredDoc> {
    let Flat {
        store,
        terms,
        stats,
        params,
    } = *query;
    let avgdl = stats.avgdl();
    let mut scores: HashMap<u32, (f64, M)> = HashMap::new();
    for (ti, term) in terms.iter().enumerate() {
        if stats.dfs[ti] == 0 {
            continue;
        }
        let idf = idf(stats.doc_count, stats.dfs[ti]);
        store.for_each_doc_tf(term, &mut |doc_id, tf| {
            let contribution = idf * tf_norm(params, tf, store.doc_length(doc_id), avgdl);
            let entry = scores.entry(doc_id).or_default();
            entry.0 += contribution;
            entry.1.insert(ti);
        });
    }
    let floor = controls.floor.is_finite().then_some(controls.floor);
    let mut docs: Vec<(u32, f64, M)> = scores
        .into_iter()
        .filter(|&(doc_id, _)| passes(controls.filter, doc_id))
        .map(|(doc_id, (score, members))| (doc_id, controls.finish(score, doc_id), members))
        .filter(|(_, score, _)| floor.is_none_or(|floor| *score >= floor))
        .collect();
    super::ranking::sort_truncate(&mut docs, controls.k);
    docs.into_iter()
        .map(|(doc_id, score, members)| {
            let term_offsets = if controls.include_offsets {
                members
                    .terms(terms.len())
                    .into_iter()
                    .map(|ti| (ti, store.posting_offsets(&terms[ti], doc_id)))
                    .collect()
            } else {
                Vec::new()
            };
            ScoredDoc {
                doc_id,
                score,
                term_offsets,
            }
        })
        .collect()
}

/// The exactness oracle: walks every posting with its occurrence offsets
/// and attaches them as it goes. Honors `k`, the chain, the filter, and a
/// finite floor, which drops hits below it before ranking and truncation.
pub(super) fn oracle(query: &Flat<'_>, controls: &Controls<'_>) -> Vec<ScoredDoc> {
    let Flat {
        store,
        terms,
        stats,
        params,
    } = *query;
    debug_assert_eq!(terms.len(), stats.dfs.len());
    type TermHits = Vec<(usize, Vec<(u32, u32)>)>;
    let mut scores: HashMap<u32, (f64, TermHits)> = HashMap::new();
    for (ti, term) in terms.iter().enumerate() {
        if stats.dfs[ti] == 0 {
            continue;
        }
        store.for_each_posting(term, &mut |doc_id, tf, offsets| {
            let posting = crate::postings::Posting {
                doc_id,
                tf,
                offsets: offsets.to_vec(),
                positions: Vec::new(),
                pos_lengths: Vec::new(),
            };
            let doc_len = store.doc_length(doc_id);
            let contribution = super::score_posting(params, stats, ti, &posting, doc_len);
            let entry = scores.entry(doc_id).or_default();
            entry.0 += contribution;
            entry.1.push((ti, posting.offsets));
        });
    }
    let mut docs: Vec<ScoredDoc> = scores
        .into_iter()
        .filter(|&(doc_id, _)| passes(controls.filter, doc_id))
        .map(|(doc_id, (score, term_offsets))| ScoredDoc {
            doc_id,
            score: controls.finish(score, doc_id),
            term_offsets,
        })
        .filter(|doc| !controls.floor.is_finite() || doc.score >= controls.floor)
        .collect();
    docs.sort_by(|a, b| super::ranking::compare((a.doc_id, a.score), (b.doc_id, b.score)));
    docs.truncate(controls.k);
    docs
}

/// Per-document fused accumulation: the additive base, the strongest
/// phrase-group contribution, and every matched (field, term) offset list.
type FusedEntry = (f64, Option<f64>, Vec<super::FusedTermOffset>);

fn accumulate_fused(
    entry: &mut FusedEntry,
    contribution: f64,
    group: Option<Phrase<'_>>,
    term: usize,
) {
    match group {
        Some(group) => {
            let weighted = contribution * group.weights[term];
            entry.1 = Some(entry.1.unwrap_or(0.0).max(weighted));
        }
        None => entry.0 += contribution,
    }
}

/// The fused exhaustive scorer: a weighted per-field sum, accumulated in
/// field-id then term-index order. With a [`Phrase`] group, that field
/// contributes only its strongest weighted term instead of a sum. A finite
/// floor drops documents below it before ranking.
pub(super) fn fused(
    fields: &[FieldQuery<'_>],
    controls: &Controls<'_>,
    phrase: Option<Phrase<'_>>,
) -> Vec<FusedDoc> {
    let mut scores: HashMap<u32, FusedEntry> = HashMap::new();
    for (fi, fq) in fields.iter().enumerate() {
        debug_assert_eq!(fq.terms.len(), fq.stats.dfs.len());
        let group = phrase.filter(|phrase| phrase.field == fi);
        let avgdl = fq.stats.avgdl();
        for (ti, term) in fq.terms.iter().enumerate() {
            if fq.stats.dfs[ti] == 0 {
                continue;
            }
            let idf = idf(fq.stats.doc_count, fq.stats.dfs[ti]);
            if controls.include_offsets {
                fq.index.for_each_posting(term, &mut |doc_id, tf, offsets| {
                    let contribution = fq.weight
                        * idf
                        * tf_norm(fq.params, tf, fq.index.doc_length(doc_id), avgdl);
                    let entry = scores.entry(doc_id).or_default();
                    accumulate_fused(entry, contribution, group, ti);
                    entry.2.push((fi, ti, offsets.to_vec()));
                });
            } else {
                fq.index.for_each_doc_tf(term, &mut |doc_id, tf| {
                    let contribution = fq.weight
                        * idf
                        * tf_norm(fq.params, tf, fq.index.doc_length(doc_id), avgdl);
                    accumulate_fused(scores.entry(doc_id).or_default(), contribution, group, ti);
                });
            }
        }
    }
    let floor = controls.floor;
    let mut docs: Vec<FusedDoc> = scores
        .into_iter()
        .filter(|&(doc_id, _)| passes(controls.filter, doc_id))
        .map(|(doc_id, (base, group, term_offsets))| FusedDoc {
            doc_id,
            score: match (phrase, group) {
                (Some(_), group) => base + group.unwrap_or(0.0),
                (None, _) => base,
            },
            term_offsets,
        })
        .filter(|doc| !floor.is_finite() || doc.score >= floor)
        .collect();
    docs.sort_by(|a, b| super::ranking::compare((a.doc_id, a.score), (b.doc_id, b.score)));
    docs.truncate(controls.k);
    docs
}

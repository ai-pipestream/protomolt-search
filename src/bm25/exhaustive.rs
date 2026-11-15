//! Exhaustive lexical accumulation with wire-score ranking.

use super::{idf, passes, tf_norm, FilterCtx, ScoredDoc};

pub(super) use super::ScoringQuery as Query;

impl Query<'_> {
    pub(super) fn run(self, k: usize) -> Vec<ScoredDoc> {
        self.filtered(k, None)
    }

    pub(super) fn above(self, k: usize, floor: f64) -> Vec<ScoredDoc> {
        self.filtered(k, floor.is_finite().then_some(floor))
    }

    fn filtered(self, k: usize, floor: Option<f64>) -> Vec<ScoredDoc> {
        debug_assert_eq!(self.terms.len(), self.stats.dfs.len());
        if self.terms.len() <= 64 {
            self.small(k, floor)
        } else {
            self.wide(k, floor)
        }
    }

    fn small(self, k: usize, floor: Option<f64>) -> Vec<ScoredDoc> {
        let Self {
            store,
            terms,
            stats,
            params,
            chain,
            filter,
        } = self;
        let avgdl = stats.avgdl();
        let finish = |score: f64, doc_id: u32| match chain {
            Some((chain, columns)) => chain.eval(score, doc_id, columns),
            None => score,
        };
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
            .filter(|(_, score, _)| floor.is_none_or(|floor| super::ranking::clears(*score, floor)))
            .collect();
        super::ranking::sort_truncate(&mut docs, k);
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
    }

    fn wide(self, k: usize, floor: Option<f64>) -> Vec<ScoredDoc> {
        let Self {
            store,
            terms,
            stats,
            params,
            chain,
            filter,
        } = self;
        let avgdl = stats.avgdl();
        let finish = |score: f64, doc_id: u32| match chain {
            Some((chain, columns)) => chain.eval(score, doc_id, columns),
            None => score,
        };
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
            .filter(|(_, score, _)| floor.is_none_or(|floor| super::ranking::clears(*score, floor)))
            .collect();
        super::ranking::sort_truncate(&mut docs, k);
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

pub(super) fn fused(
    fields: &[super::FieldQuery<'_>],
    k: usize,
    filter: FilterCtx<'_>,
    floor: Option<f64>,
) -> Vec<super::FusedDoc> {
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
    let mut docs: Vec<super::FusedDoc> = scores
        .into_iter()
        .filter(|&(doc_id, _)| passes(filter, doc_id))
        .map(|(doc_id, (score, term_offsets))| super::FusedDoc {
            doc_id,
            score,
            term_offsets,
        })
        .collect();
    docs.retain(|doc| floor.is_none_or(|floor| super::ranking::clears(doc.score, floor)));
    docs.sort_by(|a, b| super::ranking::compare((a.doc_id, a.score), (b.doc_id, b.score)));
    docs.truncate(k);
    docs
}

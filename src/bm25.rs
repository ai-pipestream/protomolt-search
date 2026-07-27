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
/// walk. Returns one [`ScoredDoc`] per candidate that scored above zero,
/// doc id ascending.
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

    let mut docs: Vec<ScoredDoc> = Vec::new();
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
pub fn top_k(
    store: &dyn Bm25Index,
    terms: &[String],
    stats: &CorpusStats,
    params: Bm25Params,
    k: usize,
) -> Vec<ScoredDoc> {
    debug_assert_eq!(terms.len(), stats.dfs.len());
    // Accumulate per-doc scores. Query workloads here are small (a handful
    // of terms, postings lists walked once), so a HashMap merge is fine.
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

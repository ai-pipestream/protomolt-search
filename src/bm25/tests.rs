//! Scoring formula and single-field scorer checks.

use super::*;
use crate::postings::{AnalyzedDoc, Bm25Index, Bm25Store};

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
        None,
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

//! Fused multi-field and phrase-group scorer checks.

use super::*;
use crate::postings::{AnalyzedDoc, AnalyzedField, Bm25Index, Bm25Store};

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
            cased: None,
            fields: vec![
                AnalyzedField {
                    sentences: None,
                    terms: vec![
                        ("rust".into(), 2, vec![(0, 4)]),
                        ("search".into(), 1, vec![]),
                    ],
                    length: 3,
                    positions: None,
                    ..Default::default()
                },
                AnalyzedField {
                    sentences: None,
                    terms: vec![("smith".into(), 1, vec![(0, 5)])],
                    length: 1,
                    positions: None,
                    ..Default::default()
                },
            ],
            ..Default::default()
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
            cased: None,
            fields: vec![
                AnalyzedField {
                    sentences: None,
                    terms: vec![("search".into(), 1, vec![])],
                    length: 1,
                    positions: None,
                    ..Default::default()
                },
                AnalyzedField {
                    sentences: None,
                    terms: vec![
                        ("rust".into(), 1, vec![(0, 4)]),
                        ("smith".into(), 1, vec![]),
                    ],
                    length: 2,
                    positions: None,
                    ..Default::default()
                },
            ],
            ..Default::default()
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
            None,
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
            sentences: None,
            terms: body,
            length: body_len,
            positions: None,
            ..Default::default()
        }];
        if g % 3 != 1 {
            fields.push(AnalyzedField {
                sentences: None,
                terms: vec![
                    ("smith".to_string(), 1, vec![(0, 5)]),
                    (format!("n{}", g % 2), 1, vec![]),
                ],
                length: 2,
                positions: None,
                ..Default::default()
            });
        }
        AnalyzedDoc {
            fields,
            ..Default::default()
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
            None,
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

#[test]
fn phrase_group_uses_strongest_nested_concept_without_stacking() {
    let mut store = Bm25Store::with_fields(&["body", "phrases"]);
    store.add_document(
        0,
        "New York City".into(),
        AnalyzedDoc {
            cased: None,
            fields: vec![
                AnalyzedField {
                    sentences: None,
                    terms: vec![
                        ("new".into(), 1, vec![(0, 3)]),
                        ("york".into(), 1, vec![(4, 8)]),
                        ("city".into(), 1, vec![(9, 13)]),
                    ],
                    length: 3,
                    positions: None,
                    ..Default::default()
                },
                AnalyzedField {
                    sentences: None,
                    terms: vec![
                        ("$phrase:nyc".into(), 1, vec![(0, 13)]),
                        ("$phrase:new-york".into(), 1, vec![(0, 8)]),
                    ],
                    length: 2,
                    positions: None,
                    ..Default::default()
                },
            ],
            ..Default::default()
        },
    );
    let body_terms = vec!["new".to_string(), "york".to_string(), "city".to_string()];
    let phrase_terms = vec!["$phrase:nyc".to_string(), "$phrase:new-york".to_string()];
    let queries = [
        FieldQuery {
            index: &store.field(0),
            terms: &body_terms,
            stats: CorpusStats {
                doc_count: 1,
                total_doc_length: 3,
                dfs: vec![1, 1, 1],
            },
            params: Bm25Params::default(),
            weight: 1.0,
        },
        FieldQuery {
            index: &store.field(1),
            terms: &phrase_terms,
            stats: CorpusStats {
                doc_count: 1,
                total_doc_length: 2,
                dfs: vec![1, 1],
            },
            params: Bm25Params::default(),
            weight: 1.0,
        },
    ];
    let max_group = top_k_fused_exhaustive(
        &queries,
        1,
        Some(Phrase {
            field: 1,
            weights: &[1.0, 1.0],
        }),
    );
    let additive = top_k_fused_exhaustive(&queries, 1, None);
    assert!(max_group[0].score < additive[0].score);
    assert_eq!(max_group[0].term_offsets.len(), 5);

    let full_only = top_k_fused_exhaustive(
        &queries,
        1,
        Some(Phrase {
            field: 1,
            weights: &[3.0, 0.0],
        }),
    );
    let prefix_only = top_k_fused_exhaustive(
        &queries,
        1,
        Some(Phrase {
            field: 1,
            weights: &[0.0, 2.0],
        }),
    );
    let weighted = top_k_fused_exhaustive(
        &queries,
        1,
        Some(Phrase {
            field: 1,
            weights: &[3.0, 2.0],
        }),
    );
    assert_eq!(
        weighted[0].score.to_bits(),
        full_only[0].score.max(prefix_only[0].score).to_bits()
    );
}

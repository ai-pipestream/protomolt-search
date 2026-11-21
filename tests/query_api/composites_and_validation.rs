//! Filter conjunction, composite fusion strategies, boost rescoring, and shape refusals.

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn filters_ride_the_and_wrapper_and_conjoin() {
    let (coordinator, _qvec, _handles) = start_fleet().await;

    let public = query(
        &coordinator,
        QueryRequest {
            k: 10,
            selection: Some(composite(
                SelectionOperator::And,
                vec![
                    lexical_leaf("lex", "document"),
                    cel_filter("recent", "year >= 2"),
                    cel_filter("old", "year < 6"),
                ],
                None,
            )),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let direct = coordinator
        .bm25_search(Request::new(Bm25SearchRequest {
            text: "document".into(),
            k: 10,
            filter: "(year >= 2) && (year < 6)".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    let got: Vec<u64> = public.hits.iter().map(|h| h.doc_id).collect();
    let want: Vec<u64> = direct.hits.iter().map(|h| h.doc_id).collect();
    assert_eq!(got, want);
    assert!(got.iter().all(|&id| (2..6).contains(&id)));
    for hit in &public.hits {
        assert_eq!(hit.matched, vec!["lex", "recent", "old"]);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn composites_execute_their_fusion_modes() {
    let (coordinator, qvec, _handles) = start_fleet().await;

    let two_leaves = || vec![dense_leaf("vec", &qvec), lexical_leaf("lex", "zebra")];
    for (strategy, operator, mode, executed) in [
        (
            selection_score_strategy::Strategy::Rrf(RrfScore::default()),
            SelectionOperator::Or,
            FusionMode::GlobalRank,
            "hybrid_search:global_rank",
        ),
        (
            selection_score_strategy::Strategy::Decomposed(DecomposedScore::default()),
            SelectionOperator::Or,
            FusionMode::Decomposed,
            "hybrid_search:decomposed",
        ),
        (
            selection_score_strategy::Strategy::Cascade(CascadeScore {
                gate_id: "vec".into(),
            }),
            SelectionOperator::Unspecified,
            FusionMode::Cascade,
            "hybrid_search:cascade",
        ),
    ] {
        let public = query(
            &coordinator,
            QueryRequest {
                k: 6,
                selection: Some(composite(operator, two_leaves(), Some(strategy))),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let direct = coordinator
            .hybrid_search(Request::new(HybridSearchRequest {
                text: "zebra".into(),
                vector: qvec.clone(),
                k: 6,
                legs: Some(HybridLegOptions {
                    fusion_mode: mode as i32,
                    leg_k: 6,
                    ..Default::default()
                }),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(public.executed, executed);
        if mode == FusionMode::Cascade {
            let got: Vec<(u64, u32)> = public
                .hits
                .iter()
                .map(|h| (h.doc_id, h.score.to_bits()))
                .collect();
            let want: Vec<(u64, u32)> = direct
                .cascade_hits
                .iter()
                .map(|h| (h.doc_id, h.bm25_score.to_bits()))
                .collect();
            assert_eq!(got, want, "{executed}");
        } else {
            let got: Vec<(u64, u32)> = public
                .hits
                .iter()
                .map(|h| (h.doc_id, h.score.to_bits()))
                .collect();
            let want: Vec<(u64, u32)> = direct
                .hits
                .iter()
                .map(|h| (h.doc_id, h.fused_score.to_bits()))
                .collect();
            assert_eq!(got, want, "{executed}");
        }
        // Provenance: doc 0 is in both legs (its own vector tops the
        // dense leg; "zebra document 0" matches the lexical leg), so
        // its hit names both signals.
        let zero = public.hits.iter().find(|h| h.doc_id == 0).unwrap();
        let mut signal_ids: Vec<&str> = zero.signals.iter().map(|s| s.id.as_str()).collect();
        signal_ids.sort_unstable();
        assert_eq!(signal_ids, vec!["lex", "vec"], "{executed}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_boost_rescoring_draws_from_selection_k_and_returns_k() {
    let (coordinator, qvec, _handles) = start_fleet().await;

    let public = query(
        &coordinator,
        QueryRequest {
            k: 3,
            selection_k: 8,
            selection: Some(composite(
                SelectionOperator::Or,
                vec![dense_leaf("vec", &qvec), lexical_leaf("lex", "zebra")],
                Some(selection_score_strategy::Strategy::Rrf(RrfScore::default())),
            )),
            boosts: vec![BoostQuery {
                query: Some(SearchQuery {
                    id: "boost".into(),
                    query: Some(search_query::Query::Lexical(LexicalQuery {
                        text: "plain".into(),
                        ..Default::default()
                    })),
                }),
                ..Default::default()
            }],
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let direct = coordinator
        .hybrid_search(Request::new(HybridSearchRequest {
            text: "zebra".into(),
            vector: qvec.clone(),
            k: 8,
            legs: Some(HybridLegOptions {
                fusion_mode: FusionMode::GlobalRank as i32,
                leg_k: 8,
                ..Default::default()
            }),
            boost: Some(BoostRescore {
                text: "plain".into(),
                ..Default::default()
            }),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(public.hits.len(), 3, "trimmed to k");
    for (p, d) in public.hits.iter().zip(&direct.hits) {
        assert_eq!(p.doc_id, d.doc_id);
        assert_eq!(p.score.to_bits(), d.fused_score.to_bits());
    }
    // A boosted hit ("plain" matches odd docs) names the boost signal.
    let boosted = public
        .hits
        .iter()
        .find(|h| h.doc_id % 2 == 1)
        .expect("the boost promoted a plain doc into the top 3");
    assert!(boosted.signals.iter().any(|s| s.id == "boost"));
    assert!(boosted.matched.contains(&"boost".to_string()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unsupported_shapes_refuse_by_name() {
    let (coordinator, qvec, _handles) = start_fleet().await;

    let or_two = |strategy| {
        composite(
            SelectionOperator::Or,
            vec![dense_leaf("vec", &qvec), lexical_leaf("lex", "zebra")],
            strategy,
        )
    };
    let cases: Vec<(QueryRequest, &str)> = vec![
        (
            QueryRequest {
                k: 5,
                selection: Some(composite(SelectionOperator::And, Vec::new(), None)),
                ..Default::default()
            },
            "empty browse",
        ),
        (
            QueryRequest {
                k: 5,
                selection: Some(composite(
                    SelectionOperator::And,
                    vec![dense_leaf("vec", &qvec), lexical_leaf("lex", "zebra")],
                    None,
                )),
                ..Default::default()
            },
            "AND over 2 scoring structures",
        ),
        (
            QueryRequest {
                k: 5,
                selection: Some(composite(
                    SelectionOperator::Or,
                    vec![
                        dense_leaf("vec", &qvec),
                        lexical_leaf("lex", "zebra"),
                        cel_filter("f", "year >= 2"),
                    ],
                    Some(selection_score_strategy::Strategy::Rrf(RrfScore::default())),
                )),
                ..Default::default()
            },
            "AND wrapper AROUND",
        ),
        (
            QueryRequest {
                k: 5,
                selection: Some(or_two(None)),
                ..Default::default()
            },
            "explicit strategy",
        ),
        (
            QueryRequest {
                k: 5,
                selection: Some(or_two(Some(selection_score_strategy::Strategy::Cascade(
                    CascadeScore {
                        gate_id: "vec".into(),
                    },
                )))),
                ..Default::default()
            },
            "operator must be left unspecified",
        ),
        (
            QueryRequest {
                k: 5,
                selection: Some(composite(
                    SelectionOperator::Unspecified,
                    vec![dense_leaf("vec", &qvec), lexical_leaf("lex", "zebra")],
                    Some(selection_score_strategy::Strategy::Cascade(CascadeScore {
                        gate_id: "lex".into(),
                    })),
                )),
                ..Default::default()
            },
            "vector-gate",
        ),
        (
            QueryRequest {
                k: 5,
                selection: Some(lexical_leaf("lex", "zebra")),
                scorer: Some(CompositeScorer {
                    operation: 1,
                    dimensions: Vec::new(),
                }),
                ..Default::default()
            },
            "no dimensions",
        ),
        (
            QueryRequest {
                k: 5,
                selection: Some(composite(
                    SelectionOperator::And,
                    vec![lexical_leaf("dup", "zebra"), cel_filter("dup", "year >= 0")],
                    None,
                )),
                ..Default::default()
            },
            "duplicate query id",
        ),
        (
            QueryRequest {
                k: 5,
                selection: Some(lexical_leaf("", "zebra")),
                ..Default::default()
            },
            "non-empty id",
        ),
        (
            QueryRequest {
                k: 5,
                selection_k: 3,
                selection: Some(lexical_leaf("lex", "zebra")),
                ..Default::default()
            },
            "must not exceed selection_k",
        ),
        (
            QueryRequest {
                k: 3,
                selection_k: 8,
                selection: Some(lexical_leaf("lex", "zebra")),
                ..Default::default()
            },
            "silent no-op",
        ),
        (
            // Two boosts with no scorer: nothing defines how their
            // signals combine.
            QueryRequest {
                k: 5,
                selection: Some(lexical_leaf("lex", "zebra")),
                boosts: ["b1", "b2"]
                    .iter()
                    .map(|id| BoostQuery {
                        query: Some(SearchQuery {
                            id: (*id).into(),
                            query: Some(search_query::Query::Lexical(LexicalQuery {
                                text: "plain".into(),
                                ..Default::default()
                            })),
                        }),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            },
            "no composite scorer",
        ),
        (
            QueryRequest {
                k: 5,
                selection: Some(boolean(
                    Vec::new(),
                    Vec::new(),
                    vec![lexical_leaf("excluded", "zebra")],
                    0,
                    None,
                )),
                boosts: vec![BoostQuery {
                    query: Some(SearchQuery {
                        id: "boost".into(),
                        query: Some(search_query::Query::Lexical(LexicalQuery {
                            text: "plain".into(),
                            ..Default::default()
                        })),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            },
            "positive scoring clause",
        ),
    ];
    for (req, needle) in cases {
        let err = query(&coordinator, req).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument, "{needle}");
        assert!(
            err.message().contains(needle),
            "expected {needle:?} in: {}",
            err.message()
        );
    }
}

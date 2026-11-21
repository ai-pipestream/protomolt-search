//! Recursive boolean exactness, candidate score-stage preservation, and empty match sets.

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recursive_boolean_is_exact_across_hybrid_signals_filters_aggregation_and_pages() {
    let (coordinator, qvec, _handles) = start_fleet().await;
    let selection = || {
        boolean(
            vec![
                lexical_leaf("body", "document"),
                cel_filter("adult", "year >= 1"),
            ],
            vec![
                lexical_leaf("zebra", "zebra"),
                dense_leaf("vector", &qvec),
                boolean(
                    vec![cel_filter("early", "year < 5")],
                    Vec::new(),
                    vec![cel_filter("not_three", "year == 3")],
                    0,
                    None,
                ),
            ],
            vec![cel_filter("not_six", "year == 6")],
            2,
            Some(AggregateRequest {
                aggregations: vec![Aggregation {
                    name: "year_sum".into(),
                    expression: "year".into(),
                    op: AggregateOp::Sum as i32,
                    max_distinct: 0,
                }],
                ..Default::default()
            }),
        )
    };

    let full = query(
        &coordinator,
        QueryRequest {
            request_id: "boolean-full".into(),
            k: 8,
            selection: Some(selection()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(full.executed, "boolean:bitmap");
    let mut matched: Vec<u64> = full.hits.iter().map(|hit| hit.doc_id).collect();
    matched.sort_unstable();
    assert_eq!(matched, vec![1, 2, 4]);
    let two = full.hits.iter().find(|hit| hit.doc_id == 2).unwrap();
    assert!(two.signals.iter().any(|signal| signal.id == "body"));
    assert!(two.signals.iter().any(|signal| signal.id == "zebra"));
    assert!(two.signals.iter().any(|signal| signal.id == "vector"));
    assert!(two.matched.contains(&"adult".to_string()));
    assert!(two.matched.contains(&"early".to_string()));
    assert!(!two.matched.contains(&"not_six".to_string()));
    let aggregate = full.aggregate.as_ref().expect("root boolean aggregate");
    assert_eq!(aggregate.matched, 3);
    assert_eq!(aggregate.results[0].present, 3);
    assert_eq!(
        aggregate.results[0].value,
        Some(aggregate_result::Value::IntValue(7))
    );

    let first = query(
        &coordinator,
        QueryRequest {
            k: 2,
            selection: Some(selection()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(first.hits.len(), 2);
    assert!(!first.next_cursor.is_empty());
    assert_eq!(first.aggregate.as_ref().unwrap().matched, 3);
    let second = query(
        &coordinator,
        QueryRequest {
            k: 2,
            selection: Some(selection()),
            cursor: first.next_cursor.clone(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(second.hits.len(), 1);
    assert!(second.next_cursor.is_empty());
    assert_eq!(second.aggregate.as_ref().unwrap().matched, 3);
    let stitched: Vec<(u64, u32)> = first
        .hits
        .iter()
        .chain(&second.hits)
        .map(|hit| (hit.doc_id, hit.score.to_bits()))
        .collect();
    assert_eq!(
        stitched,
        full.hits
            .iter()
            .map(|hit| (hit.doc_id, hit.score.to_bits()))
            .collect::<Vec<_>>()
    );

    let boosted = query(
        &coordinator,
        QueryRequest {
            k: 3,
            selection: Some(selection()),
            boosts: vec![BoostQuery {
                query: Some(SearchQuery {
                    id: "plain_boost".into(),
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
    let one = boosted.hits.iter().find(|hit| hit.doc_id == 1).unwrap();
    assert!(one.signals.iter().any(|signal| signal.id == "plain_boost"));

    let events = streamed_query(
        &coordinator,
        Some(QueryRequest {
            k: 8,
            selection: Some(selection()),
            ..Default::default()
        }),
        0,
    )
    .await;
    let (_, completion) = stream_parts(&events);
    assert!(completion.completed, "{completion:?}");
    assert_eq!(
        completion
            .response
            .as_ref()
            .unwrap()
            .hits
            .iter()
            .map(|hit| (hit.doc_id, hit.score.to_bits()))
            .collect::<Vec<_>>(),
        full.hits
            .iter()
            .map(|hit| (hit.doc_id, hit.score.to_bits()))
            .collect::<Vec<_>>()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn boolean_candidate_rescore_preserves_the_ordinary_score_stage_bits() {
    let (coordinator, _qvec, _handles) = start_fleet().await;
    let lexical = || SelectionQuery {
        node: Some(selection_query::Node::Search(SearchQuery {
            id: "body".into(),
            query: Some(search_query::Query::Lexical(LexicalQuery {
                text: "document".into(),
                score_stages: vec![ScoreStage {
                    operation: Some(pipestream_search::pb::score_stage::Operation::Op(
                        ScoreOp::AddLinear as i32,
                    )),
                    column: "year".into(),
                    weight: 0.125,
                    ..Default::default()
                }],
                ..Default::default()
            })),
        })),
    };
    let ordinary = query(
        &coordinator,
        QueryRequest {
            k: N_DOCS as u32,
            selection: Some(lexical()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let planned = query(
        &coordinator,
        QueryRequest {
            k: N_DOCS as u32,
            selection: Some(boolean(vec![lexical()], Vec::new(), Vec::new(), 0, None)),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(planned.executed, "boolean:bitmap");
    assert_eq!(planned.hits.len(), ordinary.hits.len());
    assert_eq!(
        planned
            .hits
            .iter()
            .map(|hit| (hit.doc_id, hit.score.to_bits()))
            .collect::<Vec<_>>(),
        ordinary
            .hits
            .iter()
            .map(|hit| (hit.doc_id, hit.score.to_bits()))
            .collect::<Vec<_>>()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_empty_boolean_match_set_has_an_empty_aggregate() {
    let (coordinator, _qvec, _handles) = start_fleet().await;
    let response = query(
        &coordinator,
        QueryRequest {
            k: 8,
            selection: Some(boolean(
                vec![cel_filter("none", "year > 100")],
                Vec::new(),
                Vec::new(),
                0,
                Some(AggregateRequest {
                    aggregations: vec![Aggregation {
                        name: "count".into(),
                        expression: "year".into(),
                        op: AggregateOp::Count as i32,
                        max_distinct: 0,
                    }],
                    ..Default::default()
                }),
            )),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(response.hits.is_empty());
    let aggregate = response.aggregate.unwrap();
    assert_eq!(aggregate.matched, 0);
    assert_eq!(aggregate.results[0].present, 0);
    assert_eq!(
        aggregate.results[0].value,
        Some(aggregate_result::Value::IntValue(0))
    );
}

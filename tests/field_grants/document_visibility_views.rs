//! Document visibility composition with streaming revisions, redaction, and pagination.

use super::*;

#[tokio::test]
async fn document_queries_and_every_stream_revision_match_the_visible_corpus() {
    use tokio_stream::StreamExt;
    let owner = cluster_subset(true, false, true).await;
    let reference = cluster_subset(true, true, true).await;
    let filter = |expression: &str| SelectionQuery {
        node: Some(selection_query::Node::Filter(FilterQuery {
            id: "filter".into(),
            predicate: Some(filter_query::Predicate::Cel(expression.into())),
        })),
    };
    let aggregate = AggregateRequest {
        aggregations: vec![Aggregation {
            name: "sum".into(),
            expression: "boost".into(),
            op: AggregateOp::Sum as i32,
            ..Default::default()
        }],
        ..Default::default()
    };
    let cases = vec![
        public_query(),
        stored_query(),
        QueryRequest {
            highlight: Some(HighlightSpec {
                fields: vec!["body".into()],
                mode: HighlightMode::Window as i32,
                ..Default::default()
            }),
            ..public_query()
        },
        QueryRequest {
            selection: Some(SelectionQuery {
                node: Some(selection_query::Node::Boolean(BooleanQuery {
                    must: vec![public_query().selection.unwrap(), filter("boost > 0")],
                    aggregate: Some(aggregate.clone()),
                    ..Default::default()
                })),
            }),
            ..public_query()
        },
        QueryRequest {
            selection: Some(SelectionQuery {
                node: Some(selection_query::Node::Boolean(BooleanQuery {
                    must_not: vec![filter("color == 'blue'")],
                    ..Default::default()
                })),
            }),
            ..public_query()
        },
        QueryRequest {
            selection: Some(filter("boost > 0")),
            sort: vec![QuerySort {
                map: None,
                column: "boost".into(),
                descending: true,
            }],
            projections: vec![NamedProjection {
                name: "value".into(),
                expression: "boost".into(),
            }],
            aggregate: Some(aggregate),
            ..public_query()
        },
        QueryRequest {
            collapse: Some(CollapseSpec {
                map: None,
                column: "color".into(),
                inner_hits: 4,
            }),
            ..public_query()
        },
        QueryRequest {
            boosts: vec![BoostQuery {
                query: Some(SearchQuery {
                    id: "boost".into(),
                    query: Some(search_query::Query::Lexical(LexicalQuery {
                        text: "beta".into(),
                        ..Default::default()
                    })),
                }),
                ..Default::default()
            }],
            ..public_query()
        },
    ];
    for restrict_fields in [false, true] {
        let fields = restrict_fields.then(|| {
            permissions(
                &[
                    ("body", &[FieldAction::Use, FieldAction::Disclose]),
                    ("color", &[FieldAction::Use, FieldAction::Disclose]),
                    ("boost", &[FieldAction::Use, FieldAction::Disclose]),
                ],
                true,
            )
        });
        // The caller cannot use the authority's audience column under field grants.
        let reader = service(
            owner.clone(),
            Arc::new(PolicyAuthority::new(policy(fields, true)).unwrap()),
        );
        for (case, mut query) in cases.clone().into_iter().enumerate() {
            query.profile = true;
            let expected = SearchService::query(&reference, Request::new(query.clone()))
                .await
                .unwrap()
                .into_inner();
            let actual = SearchService::query(&reader, request(query.clone()))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(actual.hits, expected.hits, "case {case}");
            assert_eq!(actual.aggregate, expected.aggregate, "case {case}");
            assert_eq!(actual.groups, expected.groups, "case {case}");
            if case == 0 {
                let explained = QueryRequest {
                    explain: true,
                    ..query.clone()
                };
                let expected = SearchService::query(&reference, Request::new(explained.clone()))
                    .await
                    .unwrap()
                    .into_inner();
                let actual = SearchService::query(&reader, request(explained))
                    .await
                    .unwrap()
                    .into_inner();
                assert!(actual.hits.iter().all(|h| h.explain.is_some()));
                assert_eq!(actual.hits, expected.hits);
            }
            assert!(actual.execution_details_redacted);
            let mut stream = SearchService::query_stream(
                &reader,
                request(QueryStreamRequest {
                    query: Some(query),
                    ..Default::default()
                }),
            )
            .await
            .unwrap()
            .into_inner();
            let mut completed = false;
            let mut revision = 0;
            let mut final_hits = None;
            let mut provisional = false;
            while let Some(event) = stream.next().await {
                assert!(!completed, "event after completion");
                match event.unwrap().payload.unwrap() {
                    query_stream_response::Payload::Revision(snapshot) => {
                        assert!(snapshot.revision > revision);
                        revision = snapshot.revision;
                        for hit in &snapshot.hits {
                            assert!(
                                matches!(hit.doc_id, 0 | 100),
                                "case {case}: private provisional hit {hit:?}"
                            );
                            assert!(hit.score.is_finite());
                        }
                        if snapshot.phase == QueryStreamPhase::Final as i32 {
                            final_hits = Some(
                                snapshot
                                    .hits
                                    .iter()
                                    .map(|h| (h.doc_id, h.score.to_bits()))
                                    .collect::<Vec<_>>(),
                            );
                        } else if !snapshot.hits.is_empty() {
                            provisional = true;
                        }
                    }
                    query_stream_response::Payload::Completion(end) => {
                        assert!(end.completed, "case {case}: {}", end.error_message);
                        assert_eq!(end.final_revision, revision);
                        let response = end.response.unwrap();
                        assert_eq!(response.hits, expected.hits, "stream case {case}");
                        assert_eq!(response.aggregate, expected.aggregate, "stream case {case}");
                        assert_eq!(response.groups, expected.groups, "stream case {case}");
                        assert_eq!(final_hits.as_ref(), Some(&signature(&response)));
                        assert!(response.execution_details_redacted);
                        assert_eq!(response.profile.unwrap().segments_total, 0);
                        completed = true;
                    }
                }
            }
            assert!(completed);
            if case == 0 {
                assert!(
                    provisional,
                    "lexical test must observe real provisional hits"
                );
            }
        }
    }
}

#[tokio::test]
async fn document_revocation_after_provisional_hits_discards_buffered_results() {
    use tokio_stream::StreamExt;
    let owner = cluster(true).await;
    let mut current = policy(None, true);
    let authority = Arc::new(PolicyAuthority::new(current.clone()).unwrap());
    let reader = service(owner, authority.clone());
    let mut stream = SearchService::query_stream(
        &reader,
        request(QueryStreamRequest {
            query: Some(public_query()),
            ..Default::default()
        }),
    )
    .await
    .unwrap()
    .into_inner();
    loop {
        let event = stream.next().await.expect("provisional event").unwrap();
        match event.payload.unwrap() {
            query_stream_response::Payload::Revision(revision) => {
                assert!(revision
                    .hits
                    .iter()
                    .all(|hit| matches!(hit.doc_id, 0 | 100)));
                if !revision.hits.is_empty() {
                    assert_eq!(
                        revision.identity_state,
                        QueryStreamIdentityState::Resolved as i32
                    );
                    assert!(revision.hits.iter().all(|hit| hit.identity.is_some()));
                    assert_ne!(revision.phase, QueryStreamPhase::Final as i32);
                    break;
                }
            }
            _ => panic!("completion arrived before provisional hits"),
        }
    }
    current.revision += 1;
    current.grants[0]
        .document_visibility
        .as_mut()
        .unwrap()
        .filter = pipestream_search::cel::compile_filter("audience == 'private'").unwrap();
    authority.replace(current).unwrap();
    let error = stream.next().await.unwrap().unwrap_err();
    assert_eq!(error.code(), Code::PermissionDenied);
    let detail = pipestream_search::error_disclosure::status_detail(&error).unwrap();
    assert_eq!(detail.reason, SearchErrorReason::AccessPolicyChanged as i32);
    assert!(detail.details_redacted);
    assert!(stream.next().await.is_none());
    // A new operation uses the new view, not a cached old grant or candidate set.
    let reply = SearchService::query(&reader, request(public_query()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(reply.hits.len(), 2);
    assert!(reply.hits.iter().all(|hit| matches!(hit.doc_id, 1 | 101)));
}

#[tokio::test]
async fn document_views_compose_with_field_redaction_and_pagination() {
    let owner = cluster_subset(true, false, true).await;
    let reference = cluster_subset(true, true, true).await;
    let reader = service(
        owner,
        Arc::new(
            PolicyAuthority::new(policy(
                Some(permissions(
                    &[
                        ("body", &[FieldAction::Use]),
                        ("boost", &[FieldAction::Use]),
                        ("color", &[FieldAction::Use, FieldAction::Disclose]),
                    ],
                    false,
                )),
                true,
            ))
            .unwrap(),
        ),
    );
    let query = QueryRequest {
        collapse: Some(CollapseSpec {
            map: None,
            column: "color".into(),
            inner_hits: 4,
        }),
        ..stored_query()
    };
    let expected = SearchService::query(&reference, Request::new(query.clone()))
        .await
        .unwrap()
        .into_inner();
    let actual = SearchService::query(&reader, request(query))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(signature(&actual), signature(&expected));
    assert!(actual.field_details_redacted && actual.execution_details_redacted);
    assert_eq!(actual.groups.len(), expected.groups.len());
    for (group, reference) in actual.groups.iter().zip(&expected.groups) {
        assert_eq!(
            group
                .hits
                .iter()
                .map(|h| (h.doc_id, h.score.to_bits()))
                .collect::<Vec<_>>(),
            reference
                .hits
                .iter()
                .map(|h| (h.doc_id, h.score.to_bits()))
                .collect::<Vec<_>>()
        );
    }
    for hit in actual
        .hits
        .iter()
        .chain(actual.groups.iter().flat_map(|g| &g.hits))
    {
        assert!(matches!(hit.doc_id, 0 | 100));
        assert!(hit.identity.is_none() && hit.dimensions.is_empty());
    }
    let mut cursor = String::new();
    let mut ids = Vec::new();
    for page in 0..3 {
        let reply = SearchService::query(
            &reader,
            request(QueryRequest {
                k: 1,
                cursor,
                ..public_query()
            }),
        )
        .await
        .unwrap()
        .into_inner();
        ids.extend(reply.hits.iter().map(|h| h.doc_id));
        cursor = reply.next_cursor;
        if cursor.is_empty() {
            break;
        }
        assert!(page < 2, "cursor must terminate over the authorized corpus");
    }
    assert_eq!(ids, vec![0, 100]);
}

#[tokio::test]
async fn unresolved_document_views_never_emit_query_hits() {
    use tokio_stream::StreamExt;
    let mut invalid = policy(None, true);
    invalid.grants[0]
        .document_visibility
        .as_mut()
        .unwrap()
        .filter =
        pipestream_search::cel::compile_filter("policy_internal_column == 'public'").unwrap();
    let reader = service(
        cluster(true).await,
        Arc::new(PolicyAuthority::new(invalid).unwrap()),
    );
    let filter = SelectionQuery {
        node: Some(selection_query::Node::Filter(FilterQuery {
            id: "filter".into(),
            predicate: Some(filter_query::Predicate::Cel("boost > 0".into())),
        })),
    };
    for selection in [
        public_query().selection.unwrap(),
        filter.clone(),
        SelectionQuery {
            node: Some(selection_query::Node::Boolean(BooleanQuery {
                must_not: vec![filter],
                ..Default::default()
            })),
        },
    ] {
        let query = QueryRequest {
            selection: Some(selection),
            ..public_query()
        };
        let error = SearchService::query(&reader, request(query.clone()))
            .await
            .unwrap_err();
        assert_eq!(error.code(), Code::FailedPrecondition);
        let mut stream = SearchService::query_stream(
            &reader,
            request(QueryStreamRequest {
                query: Some(query),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_inner();
        let mut refused = false;
        while let Some(event) = stream.next().await {
            match event.unwrap().payload.unwrap() {
                query_stream_response::Payload::Revision(revision) => {
                    assert!(revision.hits.is_empty())
                }
                query_stream_response::Payload::Completion(end) => {
                    assert!(!end.completed);
                    assert_eq!(end.error_code, Code::FailedPrecondition as u32);
                    assert!(end.error_disclosure.unwrap().details_redacted);
                    assert!(!end.error_message.contains("policy_internal_column"));
                    assert!(end.response.is_none());
                    refused = true;
                }
            }
        }
        assert!(refused);
    }
}

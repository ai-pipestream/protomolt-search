//! Streaming completion redaction, boolean/browse/boost grants, and cursor revocation.

use super::*;

#[tokio::test]
async fn streaming_field_grants_redact_completion_and_deny_before_provisional_hits() {
    use tokio_stream::StreamExt;
    let owner = cluster(true).await;
    let reader = field_reader(
        owner,
        permissions(
            &[
                ("body", &[FieldAction::Use]),
                ("boost", &[FieldAction::Use]),
            ],
            false,
        ),
    );
    let mut stream = SearchService::query_stream(
        &reader,
        request(QueryStreamRequest {
            query: Some(stored_query()),
            ..Default::default()
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let mut completed = false;
    while let Some(event) = stream.next().await {
        if let Some(query_stream_response::Payload::Completion(end)) = event.unwrap().payload {
            assert!(end.completed, "{}", end.error_message);
            let response = end.response.unwrap();
            assert!(response.field_details_redacted);
            assert!(response
                .hits
                .iter()
                .all(|h| h.identity.is_none() && h.dimensions.is_empty()));
            completed = true;
        }
    }
    assert!(completed);
    let mut denied = public_query();
    denied.projections.push(NamedProjection {
        name: "leak".into(),
        expression: "secret".into(),
    });
    let mut stream = SearchService::query_stream(
        &reader,
        request(QueryStreamRequest {
            query: Some(denied),
            ..Default::default()
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let mut refused = false;
    while let Some(event) = stream.next().await {
        match event.unwrap().payload.unwrap() {
            query_stream_response::Payload::Revision(revision) => assert!(revision.hits.is_empty()),
            query_stream_response::Payload::Completion(end) => {
                assert!(!end.completed);
                assert_eq!(end.error_code, Code::PermissionDenied as u32);
                assert!(end.response.is_none());
                assert!(end.error_disclosure.unwrap().details_redacted);
                refused = true;
            }
        }
    }
    assert!(refused);
}

#[tokio::test]
async fn field_granted_boolean_browse_and_boost_queries_match_unrestricted_execution() {
    let owner = cluster(false).await;
    let reader = field_reader(
        owner.clone(),
        permissions(
            &[
                ("body", &[FieldAction::Use, FieldAction::Disclose]),
                ("color", &[FieldAction::Use, FieldAction::Disclose]),
                ("boost", &[FieldAction::Use, FieldAction::Disclose]),
            ],
            true,
        ),
    );
    let filter = SelectionQuery {
        node: Some(selection_query::Node::Filter(FilterQuery {
            id: "color".into(),
            predicate: Some(filter_query::Predicate::Cel("color == 'red'".into())),
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
    let cases = [
        QueryRequest {
            selection: Some(SelectionQuery {
                node: Some(selection_query::Node::Boolean(BooleanQuery {
                    must: vec![public_query().selection.unwrap(), filter.clone()],
                    aggregate: Some(aggregate.clone()),
                    ..Default::default()
                })),
            }),
            ..public_query()
        },
        QueryRequest {
            selection: Some(filter),
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
    for query in cases {
        let expected = SearchService::query(&owner, Request::new(query.clone()))
            .await
            .unwrap()
            .into_inner();
        let actual = SearchService::query(&reader, request(query))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(actual.hits, expected.hits);
        assert_eq!(actual.aggregate, expected.aggregate);
        assert!(!actual.field_details_redacted);
    }
}

#[tokio::test]
async fn query_field_revocation_invalidates_cursors_and_pending_streams() {
    use tokio_stream::StreamExt;
    for documents in [false, true] {
        let owner = cluster(true).await;
        let fields = permissions(&[("body", &[FieldAction::Use])], false);
        let authority =
            Arc::new(PolicyAuthority::new(policy(Some(fields.clone()), documents)).unwrap());
        let reader = service(owner.clone(), authority.clone());
        let query = QueryRequest {
            k: 1,
            ..public_query()
        };
        let first = SearchService::query(&reader, request(query.clone()))
            .await
            .unwrap()
            .into_inner();
        assert!(!first.next_cursor.is_empty());
        let mut hidden = SearchService::query_stream(
            &reader,
            request(QueryStreamRequest {
                query: Some(query.clone()),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_inner();
        let mut observed = false;
        while let Some(event) = hidden.next().await {
            if let Some(query_stream_response::Payload::Revision(revision)) = event.unwrap().payload
            {
                if !revision.hits.is_empty() {
                    observed = true;
                    assert_eq!(
                        revision.identity_state,
                        QueryStreamIdentityState::Withheld as i32
                    );
                    assert!(revision.hits.iter().all(|hit| hit.identity.is_none()));
                }
            }
        }
        assert!(observed);
        let mut stream = SearchService::query_stream(
            &reader,
            request(QueryStreamRequest {
                query: Some(query.clone()),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_inner();
        let mut next = policy(
            Some(permissions(
                &[("body", &[FieldAction::Use, FieldAction::Disclose])],
                true,
            )),
            documents,
        );
        next.revision = 2;
        authority.replace(next).unwrap();
        assert_eq!(
            stream.next().await.unwrap().unwrap_err().code(),
            Code::PermissionDenied
        );
        assert!(stream.next().await.is_none());
        assert_eq!(
            SearchService::query(
                &reader,
                request(QueryRequest {
                    cursor: first.next_cursor,
                    ..query
                })
            )
            .await
            .unwrap_err()
            .code(),
            Code::FailedPrecondition
        );
        // Decisions are compared, even if a provider changes fields without a revision bump.
        let reader = service(
            owner,
            Arc::new(MovingFields {
                authority: PolicyAuthority::new(policy(Some(fields), documents)).unwrap(),
                calls: AtomicUsize::new(0),
            }),
        );
        assert_eq!(
            SearchService::query(&reader, request(public_query()))
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
    }
}

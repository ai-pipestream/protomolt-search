//! Scoped aggregation grants, stored-dimension redaction, and public query admission.

use super::*;

#[tokio::test]
async fn aggregates_match_a_physically_restricted_corpus_and_cannot_widen_the_view() {
    let coordinator = cluster_subset(false, false, true).await;
    let reference = cluster_subset(false, true, true).await;
    let fields = permissions(
        &[
            ("boost", &[FieldAction::Use, FieldAction::Disclose]),
            ("color", &[FieldAction::Use, FieldAction::Disclose]),
        ],
        false,
    );
    // A field-only grant retains all documents, with the same result as the owner.
    let field_only = service(
        coordinator.clone(),
        Arc::new(PolicyAuthority::new(policy(Some(fields.clone()), false)).unwrap()),
    );
    let expected = SearchService::aggregate(&coordinator, Request::new(aggregate_request()))
        .await
        .unwrap()
        .into_inner();
    let actual = SearchService::aggregate(&field_only, request(aggregate_request()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(actual.results, expected.results);
    assert_eq!(actual.percentiles, expected.percentiles);
    assert_eq!(actual.matched, 4);
    for field_policy in [None, Some(fields)] {
        let reader = service(
            coordinator.clone(),
            Arc::new(PolicyAuthority::new(policy(field_policy, true)).unwrap()),
        );
        for filter in [
            "",
            "color == 'red'",
            "color == 'blue'",
            "color == 'red' || color == 'blue'",
        ] {
            let mut input = aggregate_request();
            input.filter = filter.into();
            let expected = SearchService::aggregate(&reference, Request::new(input.clone()))
                .await
                .unwrap()
                .into_inner();
            let actual = SearchService::aggregate(&reader, request(input))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(actual.matched, expected.matched);
            assert_eq!(actual.results, expected.results);
            assert_eq!(actual.groups, expected.groups);
            assert_eq!(actual.histograms, expected.histograms);
            assert_eq!(actual.percentiles, expected.percentiles);
            assert_eq!(actual.ungrouped, expected.ungrouped);
        }
    }
    let reader = service(
        coordinator,
        Arc::new(PolicyAuthority::new(policy(Some(FieldPermissions::default()), true)).unwrap()),
    );
    let result = SearchService::aggregate(&reader, request(count_request()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        result.matched, 2,
        "constant count needs no user field grant; authority still filters"
    );
}
#[tokio::test]
async fn aggregate_field_inputs_require_use_and_disclose_before_reading() {
    let coordinator = cluster(false).await;
    // Exercise a populated statistics cache before denying inputs.
    SearchService::bm25_search(&coordinator, Request::new(query()))
        .await
        .unwrap();
    let mut inputs = vec![aggregate_request()];
    for expression in ["secret", "true ? boost : secret", "metrics['hidden']"] {
        let mut input = count_request();
        input.aggregations[0].expression = expression.into();
        inputs.push(input);
    }
    let mut input = count_request();
    input.group_by = "secret".into();
    inputs.push(input);
    let mut input = count_request();
    input.histograms = vec![HistogramSpec {
        name: "hist".into(),
        expression: "boost".into(),
        interval: 1.,
        ..Default::default()
    }];
    inputs.push(input);
    let mut input = count_request();
    input.percentiles = vec![PercentileSpec {
        name: "pct".into(),
        expression: "boost".into(),
        percentiles: vec![50.],
    }];
    inputs.push(input);
    let mut input = count_request();
    input.filter = "audience == 'public'".into();
    inputs.push(input);
    let mut input = count_request();
    input.geo_filters = vec![GeoFilter {
        column: "location".into(),
        region: Some(geo_filter::Region::Bbox(GeoBbox {
            min_lat: -1.,
            max_lat: 1.,
            min_lon: -1.,
            max_lon: 1.,
        })),
    }];
    inputs.push(input);
    for actions in [vec![], vec![FieldAction::Use], vec![FieldAction::Disclose]] {
        let fields = if actions.is_empty() {
            FieldPermissions::default()
        } else {
            permissions(&[("boost", &actions)], false)
        };
        let reader = service(
            coordinator.clone(),
            Arc::new(PolicyAuthority::new(policy(Some(fields), true)).unwrap()),
        );
        for input in &inputs {
            let error = SearchService::aggregate(&reader, request(input.clone()))
                .await
                .unwrap_err();
            assert_eq!(error.code(), Code::PermissionDenied, "{input:?}: {error}");
            assert!(!error.message().contains("secret"));
            assert!(!error.message().contains("audience"));
        }
    }
    let network = CoordinatorServiceImpl::new(vec!["http://must-not-resolve.invalid:50051".into()]);
    let reader = service(
        network,
        Arc::new(PolicyAuthority::new(policy(None, true)).unwrap()),
    );
    assert_eq!(
        SearchService::aggregate(&reader, request(count_request()))
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
}

#[tokio::test]
async fn public_query_redacts_stored_dimensions_and_inner_hit_identity_without_changing_scores() {
    let owner = cluster_subset(true, false, true).await;
    let request_body = QueryRequest {
        collapse: Some(CollapseSpec {
            map: None,
            column: "color".into(),
            inner_hits: 2,
        }),
        k: 2,
        selection_k: 4,
        ..stored_query()
    };
    let expected = SearchService::query(&owner, Request::new(request_body.clone()))
        .await
        .unwrap()
        .into_inner();
    assert!(expected
        .hits
        .iter()
        .all(|h| h.identity.is_some() && h.dimensions[0].raw.is_some()));
    let reader = field_reader(
        owner.clone(),
        permissions(
            &[
                ("body", &[FieldAction::Use]),
                ("boost", &[FieldAction::Use]),
                ("color", &[FieldAction::Use, FieldAction::Disclose]),
            ],
            false,
        ),
    );
    let explained = field_reader(
        owner.clone(),
        permissions(
            &[
                ("body", &[FieldAction::Use, FieldAction::Disclose]),
                ("boost", &[FieldAction::Use]),
                ("color", &[FieldAction::Use, FieldAction::Disclose]),
            ],
            false,
        ),
    );
    assert_eq!(
        SearchService::query(
            &explained,
            request(QueryRequest {
                explain: true,
                ..request_body.clone()
            })
        )
        .await
        .unwrap_err()
        .code(),
        Code::PermissionDenied
    );
    let actual = SearchService::query(&reader, request(request_body.clone()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(signature(&actual), signature(&expected));
    assert!(actual.field_details_redacted);
    assert_eq!(actual.groups.len(), expected.groups.len());
    for hit in actual
        .hits
        .iter()
        .chain(actual.groups.iter().flat_map(|g| &g.hits))
    {
        assert!(hit.identity.is_none());
        assert!(hit.dimensions.is_empty());
    }
    let reader = field_reader(
        owner,
        permissions(
            &[
                ("body", &[FieldAction::Use, FieldAction::Disclose]),
                ("boost", &[FieldAction::Use, FieldAction::Disclose]),
                ("color", &[FieldAction::Use, FieldAction::Disclose]),
            ],
            true,
        ),
    );
    let actual = SearchService::query(&reader, request(request_body))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(actual.hits, expected.hits);
    assert_eq!(actual.groups, expected.groups);
    assert!(!actual.field_details_redacted);
}

#[tokio::test]
async fn public_query_admits_every_field_before_statistics_or_selection() {
    let owner = cluster(false).await;
    let cache = owner.stats_cache();
    let reader = field_reader(
        owner.clone(),
        permissions(&[("body", &[FieldAction::Use])], false),
    );
    let filter = SelectionQuery {
        node: Some(selection_query::Node::Filter(FilterQuery {
            id: "secret".into(),
            predicate: Some(filter_query::Predicate::Cel(
                "secret == 'private_value'".into(),
            )),
        })),
    };
    let mut denied = vec![stored_query()];
    // MUST_NOT, nested children and disabled scorer dimensions all require Use.
    denied.push(QueryRequest {
        selection: Some(SelectionQuery {
            node: Some(selection_query::Node::Boolean(BooleanQuery {
                must: vec![public_query().selection.unwrap()],
                must_not: vec![filter.clone()],
                ..Default::default()
            })),
        }),
        ..public_query()
    });
    denied.push(QueryRequest {
        selection: Some(SelectionQuery {
            node: Some(selection_query::Node::Composite(CompositeSearchStrategy {
                operator: SelectionOperator::And as i32,
                clauses: vec![public_query().selection.unwrap(), filter],
                scoring: None,
            })),
        }),
        ..public_query()
    });
    for spec in [
        QueryRequest {
            projections: vec![NamedProjection {
                name: "leak".into(),
                expression: "secret".into(),
            }],
            ..public_query()
        },
        QueryRequest {
            sort: vec![QuerySort {
                map: None,
                column: "secret".into(),
                descending: false,
            }],
            ..public_query()
        },
        QueryRequest {
            collapse: Some(CollapseSpec {
                map: None,
                column: "secret".into(),
                inner_hits: 2,
            }),
            ..public_query()
        },
        QueryRequest {
            explain: true,
            ..public_query()
        },
        QueryRequest {
            highlight: Some(HighlightSpec::default()),
            ..public_query()
        },
        QueryRequest {
            aggregate: Some(AggregateRequest {
                aggregations: vec![Aggregation {
                    name: "leak".into(),
                    op: AggregateOp::Sum as i32,
                    expression: "boost".into(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..public_query()
        },
    ] {
        denied.push(spec);
    }
    let mut disabled = stored_query();
    disabled.scorer.as_mut().unwrap().dimensions[0].weight = Some(0.);
    denied.push(disabled);
    let mut boost = public_query();
    boost.boosts.push(BoostQuery {
        query: Some(SearchQuery {
            id: "dense".into(),
            query: Some(search_query::Query::Dense(DenseQuery {
                field: "secret".into(),
                vector: vec![0.25; 16],
                ..Default::default()
            })),
        }),
        ..Default::default()
    });
    denied.push(boost);
    for spec in denied {
        let error = SearchService::query(&reader, request(spec))
            .await
            .unwrap_err();
        assert_eq!(error.code(), Code::PermissionDenied, "{error}");
    }
    assert_eq!(cache.fetch_count(), 0);
}

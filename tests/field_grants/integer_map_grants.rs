//! Separate use and disclosure grant enforcement over exact signed/unsigned integer maps.

use super::*;

#[tokio::test]
async fn exact_integer_maps_require_separate_use_and_disclosure_grants() {
    use tokio_stream::StreamExt;
    let node = Arc::new(NodeServiceImpl::new(
        None,
        NodeConfig {
            analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
            map_integer_fields: vec!["signed".into()],
            map_unsigned_integer_fields: vec!["unsigned".into()],
            ..Default::default()
        },
    ));
    NodeLink::local(node.clone())
        .add_documents(tokio_stream::iter([AddDocumentsRequest {
            text: "alpha".into(),
            analysis: Some(body_spec()),
            map_integers: vec![MapIntegerEntry {
                field: "signed".into(),
                key: "".into(),
                value: i64::MIN,
            }],
            map_unsigned_integers: vec![MapUnsignedIntegerEntry {
                field: "unsigned".into(),
                key: "".into(),
                value: u64::MAX,
            }],
            ..Default::default()
        }]))
        .await
        .unwrap();
    let owner = CoordinatorServiceImpl::with_local_nodes(vec![node])
        .with_bm25(Some(NATIVE_ANALYSIS_BACKEND.into()), Default::default());
    let reader = field_reader(
        owner.clone(),
        permissions(&[("body", &[FieldAction::Use])], false),
    );
    let before = owner.stats_cache().fetch_count();
    for filter in ["signed[''] < 0", "unsigned[''] > 0u", "'' in unsigned"] {
        let mut denied = query();
        denied.filter = filter.into();
        assert_eq!(
            reader
                .bm25_search(request(denied))
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
    }
    assert_eq!(
        owner.stats_cache().fetch_count(),
        before,
        "denied fields must not read statistics"
    );
    let use_only = field_reader(
        owner.clone(),
        permissions(
            &[
                ("body", &[FieldAction::Use, FieldAction::Disclose]),
                ("signed", &[FieldAction::Use]),
                ("unsigned", &[FieldAction::Use]),
            ],
            false,
        ),
    );
    let mut filtered = query();
    filtered.filter = "unsigned[''] == 18446744073709551615u && signed[''] < 0".into();
    assert_eq!(
        use_only
            .bm25_search(request(filtered))
            .await
            .unwrap()
            .into_inner()
            .hits
            .len(),
        1
    );
    let mut scored = query();
    scored.score_stages = vec![ScoreStage {
        column: "unsigned".into(),
        op: ScoreOp::AddLinear as i32,
        map_key: Some(String::new()),
        weight: 2e-19,
        ..Default::default()
    }];
    let before = owner.stats_cache().fetch_count();
    assert_eq!(
        reader
            .bm25_search(request(scored.clone()))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    assert_eq!(owner.stats_cache().fetch_count(), before);
    assert_eq!(
        use_only
            .bm25_search(request(scored.clone()))
            .await
            .unwrap()
            .into_inner()
            .hits
            .len(),
        1
    );
    scored.explain = true;
    let before = owner.stats_cache().fetch_count();
    assert_eq!(
        use_only
            .bm25_search(request(scored.clone()))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    assert_eq!(owner.stats_cache().fetch_count(), before);
    let mut projected = public_query();
    projected.projections = vec![NamedProjection {
        name: "alias".into(),
        expression: "unsigned[''] + 0u".into(),
    }];
    assert_eq!(
        use_only
            .query(request(projected.clone()))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    let allowed = field_reader(
        owner.clone(),
        permissions(
            &[
                ("body", &[FieldAction::Use, FieldAction::Disclose]),
                ("unsigned", &[FieldAction::Use, FieldAction::Disclose]),
            ],
            false,
        ),
    );
    let mut bucketed = query();
    bucketed.range_facet_fields = vec![RangeFacetField {
        column: "unsigned".into(),
        map_key: Some(String::new()),
        edges: vec![
            FilterBound::uint(u64::MAX),
            FilterBound::num(18_446_744_073_709_551_616.0),
        ],
    }];
    let before = owner.stats_cache().fetch_count();
    for denied in [&reader, &use_only] {
        assert_eq!(
            denied
                .bm25_search(request(bucketed.clone()))
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
    }
    assert_eq!(owner.stats_cache().fetch_count(), before);
    let counts = allowed
        .bm25_search(request(bucketed.clone()))
        .await
        .unwrap()
        .into_inner()
        .range_facets
        .remove(0);
    assert_eq!(counts.buckets[0].count, 1);
    assert_eq!(
        counts.buckets[0].from,
        bucketed.range_facet_fields[0].edges.first().copied()
    );
    let explanation = allowed
        .bm25_search(request(scored))
        .await
        .unwrap()
        .into_inner()
        .hits
        .remove(0)
        .explain
        .unwrap();
    assert_eq!(
        explanation.stages[0].input,
        u64::MAX.to_string().parse::<f64>().unwrap()
    );
    for collapse in [false, true] {
        let mut ordered = public_query();
        let map = MapRead {
            column: "unsigned".into(),
            key: "".into(),
        };
        if collapse {
            ordered.collapse = Some(CollapseSpec {
                map: Some(map),
                ..Default::default()
            });
        } else {
            ordered.sort = vec![QuerySort {
                map: Some(map),
                ..Default::default()
            }];
        }
        let before = owner.stats_cache().fetch_count();
        for denied in [&reader, &use_only] {
            assert_eq!(
                denied
                    .query(request(ordered.clone()))
                    .await
                    .unwrap_err()
                    .code(),
                Code::PermissionDenied
            );
        }
        assert_eq!(
            owner.stats_cache().fetch_count(),
            before,
            "map order admission must precede reads"
        );
        let result = allowed
            .query(request(ordered.clone()))
            .await
            .unwrap()
            .into_inner();
        let value = if collapse {
            result.groups[0].key.as_ref().unwrap()
        } else {
            &result.hits[0].sort_values[0]
        };
        assert_eq!(
            value.value,
            Some(sort_value::Value::UnsignedInteger(u64::MAX))
        );
        let mut stream = use_only
            .query_stream(request(QueryStreamRequest {
                query: Some(ordered),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        let mut completed = false;
        while let Some(event) = stream.next().await {
            match event.unwrap().payload.unwrap() {
                query_stream_response::Payload::Revision(revision) => {
                    assert!(revision.hits.is_empty())
                }
                query_stream_response::Payload::Completion(end) => {
                    assert!(!end.completed);
                    assert_eq!(end.error_code, Code::PermissionDenied as u32);
                    assert!(end.response.is_none());
                    completed = true;
                }
            }
        }
        assert!(completed);
    }
    let expected = allowed
        .query(request(projected.clone()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        expected.hits[0].projected[0].value,
        Some(projected_value::Value::UintValue(u64::MAX))
    );
    for (filter, count) in [
        ("unsigned[''] == 18446744073709551615u", 1),
        ("unsigned[''] == 18446744073709551614u", 0),
    ] {
        let mut access = policy(
            Some(permissions(
                &[
                    ("body", &[FieldAction::Use]),
                    ("unsigned", &[FieldAction::Use, FieldAction::Disclose]),
                ],
                false,
            )),
            false,
        );
        access.grants[0].document_visibility = Some(DocumentVisibility {
            filter: pipestream_search::cel::compile_filter(filter).unwrap(),
        });
        let visible = service(
            owner.clone(),
            Arc::new(PolicyAuthority::new(access).unwrap()),
        );
        assert_eq!(
            visible
                .query(request(projected.clone()))
                .await
                .unwrap()
                .into_inner()
                .hits
                .len(),
            count
        );
    }
    for (reader, succeeds) in [(&allowed, true), (&use_only, false)] {
        let mut stream = reader
            .query_stream(request(QueryStreamRequest {
                query: Some(projected.clone()),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        let mut complete = false;
        while let Some(event) = stream.next().await {
            match event.unwrap().payload.unwrap() {
                query_stream_response::Payload::Revision(revision) => {
                    if !succeeds {
                        assert!(revision.hits.is_empty());
                    }
                }
                query_stream_response::Payload::Completion(end) => {
                    assert_eq!(end.completed, succeeds);
                    if succeeds {
                        assert_eq!(
                            end.response.unwrap().hits[0].projected[0].value,
                            Some(projected_value::Value::UintValue(u64::MAX))
                        );
                    } else {
                        assert_eq!(end.error_code, Code::PermissionDenied as u32);
                        assert!(end.response.is_none());
                    }
                    complete = true;
                }
            }
        }
        assert!(complete);
    }
}

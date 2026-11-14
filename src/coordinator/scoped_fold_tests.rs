//! Aggregate authorization and response metadata checks.

use super::*;
use crate::pb::*;

#[tokio::test]
async fn browse_checks_fields_before_io_and_uses_the_authority_view() {
    let node = Arc::new(crate::node::NodeServiceImpl::new(
        None,
        crate::node::NodeConfig {
            analysis_addr: Some(crate::analyzer::NATIVE_ANALYSIS_BACKEND.into()),
            facet_fields: vec!["audience".into(), "color".into()],
            ..Default::default()
        },
    ));
    let mut link = crate::link::NodeLink::local(node.clone());
    for (audience, color) in [("public", "red"), ("private", "blue")] {
        link.add_documents(tokio_stream::iter([AddDocumentsRequest {
            text: "alpha".into(),
            analysis: Some(crate::analyzer::body_spec()),
            facets: vec![
                FacetValue {
                    field: "audience".into(),
                    value: audience.into(),
                },
                FacetValue {
                    field: "color".into(),
                    value: color.into(),
                },
            ],
            ..Default::default()
        }]))
        .await
        .unwrap();
    }
    let owner = CoordinatorServiceImpl::with_local_nodes(vec![node]);
    let access = AccessDecision {
        action: AccessAction::Search as i32,
        document_visibility: Some(DocumentVisibility {
            filter: crate::cel::compile_filter("audience == 'public'").unwrap(),
        }),
        field_permissions: Some(FieldPermissions {
            grants: vec![
                FieldGrant {
                    field: "body".into(),
                    actions: vec![FieldAction::Use as i32],
                },
                FieldGrant {
                    field: "color".into(),
                    actions: vec![FieldAction::Use as i32, FieldAction::Disclose as i32],
                },
            ],
            ..Default::default()
        }),
        ..Default::default()
    };
    let reader = owner.for_access(Some(&access), "aggregate").unwrap();
    let sort = [BrowseSort {
        map: None,
        column: "color".into(),
        descending: false,
    }];
    let rows = reader
        .fanout_browse(10, None, &sort, &[], 0, &RequestFilters::default())
        .await
        .unwrap();
    assert_eq!(rows.ids, vec![0]);
    assert_eq!(
        rows.values,
        vec![vec![crate::sortkeys::Value::Text("red".into())]]
    );
    assert!(reader
        .fanout_browse(
            10,
            None,
            &sort,
            &[],
            0,
            &RequestFilters::compile(&[], "color == 'blue'").unwrap()
        )
        .await
        .unwrap()
        .ids
        .is_empty());
    let (pinned, _) = reader.pin_read_versions().await.unwrap();
    link.add_documents(tokio_stream::iter([AddDocumentsRequest {
        text: "changed".into(),
        analysis: Some(crate::analyzer::body_spec()),
        ..Default::default()
    }]))
    .await
    .unwrap();
    assert_eq!(
        pinned
            .fanout_browse(10, None, &[], &[], 0, &RequestFilters::default())
            .await
            .err()
            .unwrap()
            .code(),
        tonic::Code::FailedPrecondition
    );
    // Even a broken transport must not be touched for a forbidden input.
    let mut denied = reader;
    denied.node_addrs = vec!["http://must-not-resolve.invalid:50051".into()];
    for (sort, terms, filter) in [
        (
            vec![BrowseSort {
                map: None,
                column: "body".into(),
                descending: false,
            }],
            vec![],
            "",
        ),
        (vec![], vec![], "audience == 'public'"),
        (
            vec![BrowseSort {
                map: None,
                column: "parent_id".into(),
                descending: false,
            }],
            vec![],
            "",
        ),
    ] {
        assert_eq!(
            denied
                .fanout_browse(
                    10,
                    None,
                    &sort,
                    &terms,
                    0,
                    &RequestFilters::compile(&[], filter).unwrap()
                )
                .await
                .err()
                .unwrap()
                .code(),
            tonic::Code::PermissionDenied
        );
    }
    denied.field_permissions =
        Some(crate::field_permissions::FieldScope::new(&FieldPermissions::default()).unwrap());
    assert_eq!(
        denied
            .fanout_browse(
                10,
                None,
                &[],
                &["alpha".into()],
                0,
                &RequestFilters::default()
            )
            .await
            .err()
            .unwrap()
            .code(),
        tonic::Code::PermissionDenied
    );
}

#[test]
fn fold_response_metadata_is_required_before_any_partial_is_merged() {
    let view = DocumentVisibility {
        filter: crate::cel::compile_filter("audience == 'public'").unwrap(),
    };
    let scope = crate::visibility::VisibilityScope::new(Some(&view)).unwrap();
    let mut owner = CoordinatorServiceImpl::with_local_nodes(vec![]);
    owner.query_read_versions = Some(Arc::new(vec![StatsClaim::required(4, &[1; 32]).unwrap()]));
    macro_rules! verify {
        ($ty:ident) => {{
            let valid = $ty {
                stats_epoch: 4,
                stats_incarnation: vec![1; 32],
                visibility_fingerprint: scope.fingerprint().to_vec(),
                visibility_columns_known: vec![true],
                ..Default::default()
            };
            for case in 0..7 {
                let mut response = valid.clone();
                match case {
                    0 => response.visibility_fingerprint.clear(),
                    1 => response.visibility_fingerprint = vec![5; 32],
                    2 => response.visibility_columns_known.clear(),
                    3 => response.stats_epoch = 0,
                    4 => response.stats_incarnation.clear(),
                    5 => response.stats_epoch += 1,
                    _ => response.stats_incarnation = vec![2; 32],
                }
                let mut known = vec![false];
                assert_eq!(
                    owner
                        .check_read_view(0, &scope, &response, &mut known)
                        .unwrap_err()
                        .code(),
                    tonic::Code::FailedPrecondition
                );
                assert_eq!(known, vec![false]);
            }
            let mut known = vec![false];
            owner
                .check_read_view(0, &scope, &valid, &mut known)
                .unwrap();
            assert_eq!(known, vec![true]);
        }};
    }
    verify!(BrowseShardResponse);
    verify!(AggregateShardResponse);
    verify!(QuantileCountsResponse);
}

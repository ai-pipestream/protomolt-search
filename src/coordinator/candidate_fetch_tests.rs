//! Authorization checks for membership and candidate retrieval.

use super::*;
use crate::pb::*;

/// Two shards, each holding a public red row with text "alpha" and a private
/// blue row with text "secret".
async fn two_audience_shards() -> Vec<Arc<crate::node::NodeServiceImpl>> {
    let mut nodes = Vec::new();
    for shard in 0..2 {
        let node = Arc::new(crate::node::NodeServiceImpl::new(
            None,
            crate::node::NodeConfig {
                slot_offset: shard * 100,
                analysis_addr: Some(crate::analyzer::NATIVE_ANALYSIS_BACKEND.into()),
                facet_fields: vec!["audience".into(), "color".into()],
                ..Default::default()
            },
        ));
        crate::link::NodeLink::local(node.clone())
            .add_documents(tokio_stream::iter(
                [("public", "red", "alpha"), ("private", "blue", "secret")]
                    .into_iter()
                    .map(|(audience, color, text)| AddDocumentsRequest {
                        text: text.into(),
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
                    }),
            ))
            .await
            .unwrap();
        nodes.push(node);
    }
    nodes
}

/// Public rows only, with `body` and `color` usable.
fn public_color_access() -> AccessDecision {
    AccessDecision {
        action: AccessAction::Search as i32,
        document_visibility: Some(DocumentVisibility {
            filter: crate::cel::compile_filter("audience == 'public'").unwrap(),
        }),
        field_permissions: Some(FieldPermissions {
            grants: ["body", "color"]
                .into_iter()
                .map(|field| FieldGrant {
                    field: field.into(),
                    actions: vec![FieldAction::Use as i32],
                })
                .collect(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

async fn pin_fresh_read_versions(reader: &mut CoordinatorServiceImpl) {
    reader.query_read_versions = Some(Arc::new(
        reader
            .read_query_versions(false)
            .await
            .unwrap()
            .into_iter()
            .map(|(_, claim)| claim)
            .collect(),
    ));
}

async fn assert_scoped_memberships(reader: &CoordinatorServiceImpl, universe: &MembershipSet) {
    let selected = reader
        .lexical_membership("alpha", Some(&crate::analyzer::body_spec()))
        .await
        .unwrap();
    assert_eq!(selected.ids, universe.ids);
    assert_eq!(selected.epochs, universe.epochs);
    assert!(reader
        .lexical_membership("secret", Some(&crate::analyzer::body_spec()))
        .await
        .unwrap()
        .ids
        .is_empty());
    assert!(reader
        .filter_membership(&RequestFilters::compile(&[], "color == 'blue'").unwrap())
        .await
        .unwrap()
        .ids
        .is_empty());
    let denied = reader
        .filter_membership(&RequestFilters::compile(&[], "audience == 'public'").unwrap())
        .await
        .unwrap_err();
    assert_eq!(denied.code(), tonic::Code::PermissionDenied);
    assert_eq!(denied.message(), "field access is not granted");
    assert_eq!(
        reader.vector_membership("").await.unwrap_err().code(),
        tonic::Code::PermissionDenied
    );
}

/// Exercise the actual negative-only planner. Its starting universe must
/// exclude private rows even when the negative clause matches no visible row.
async fn assert_negative_only_plan_stays_in_universe(
    reader: &CoordinatorServiceImpl,
    universe: &MembershipSet,
) {
    let query = QueryRequest {
        k: 4,
        selection: Some(SelectionQuery {
            node: Some(selection_query::Node::Boolean(BooleanQuery {
                must_not: vec![SelectionQuery {
                    node: Some(selection_query::Node::Search(SearchQuery {
                        id: "excluded".into(),
                        query: Some(search_query::Query::Lexical(LexicalQuery {
                            text: "secret".into(),
                            analysis: Some(crate::analyzer::body_spec()),
                            ..Default::default()
                        })),
                    })),
                }],
                ..Default::default()
            })),
        }),
        ..Default::default()
    };
    let planned = crate::query::execute(reader, query, 0).await.unwrap();
    assert_eq!(
        planned
            .hits
            .iter()
            .map(|hit| hit.doc_id)
            .collect::<BTreeSet<_>>(),
        universe.ids
    );
}

/// The refusal of one membership kind: filter, lexical, then vector.
async fn membership_error(
    reader: &CoordinatorServiceImpl,
    all: &RequestFilters,
    kind: usize,
) -> Status {
    match kind {
        0 => reader.filter_membership(all).await.unwrap_err(),
        1 => reader
            .lexical_membership("alpha", Some(&crate::analyzer::body_spec()))
            .await
            .unwrap_err(),
        _ => reader.vector_membership("").await.unwrap_err(),
    }
}

#[tokio::test]
async fn membership_planning_keeps_authority_separate_from_user_fields_and_versions() {
    let nodes = two_audience_shards().await;
    let owner = CoordinatorServiceImpl::with_local_nodes(nodes.clone()).with_bm25(
        Some(crate::analyzer::NATIVE_ANALYSIS_BACKEND.into()),
        Default::default(),
    );
    let access = public_color_access();
    let mut reader = owner.for_access(Some(&access), "bm25_search").unwrap();
    pin_fresh_read_versions(&mut reader).await;
    let all = RequestFilters::default();
    let universe = reader.filter_membership(&all).await.unwrap();
    assert_eq!(universe.ids, [0, 100].into_iter().collect());
    assert_eq!(universe.epochs.len(), 2);
    assert_scoped_memberships(&reader, &universe).await;
    assert_negative_only_plan_stays_in_universe(&reader, &universe).await;
    let mut doc_only = access.clone();
    doc_only.field_permissions = None;
    let mut document_reader = owner.for_access(Some(&doc_only), "bm25_search").unwrap();
    document_reader.query_read_versions = reader.query_read_versions.clone();
    assert_eq!(
        document_reader.vector_membership("").await.unwrap().epochs,
        universe.epochs
    );
    crate::link::NodeLink::local(nodes[0].clone())
        .add_documents(tokio_stream::iter([AddDocumentsRequest {
            text: "new".into(),
            analysis: Some(crate::analyzer::body_spec()),
            ..Default::default()
        }]))
        .await
        .unwrap();
    for kind in 0..3 {
        let error = membership_error(&document_reader, &all, kind).await;
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(error
            .message()
            .contains("query data changed during scoped read"));
    }
    let mut absent = doc_only;
    absent.document_visibility = Some(DocumentVisibility {
        filter: crate::cel::compile_filter("private_column == 'yes'").unwrap(),
    });
    let unknown = owner.for_access(Some(&absent), "bm25_search").unwrap();
    for kind in 0..3 {
        let error = membership_error(&unknown, &all, kind).await;
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(!error.message().contains("private_column"));
    }
}

#[test]
fn membership_view_validation_refuses_legacy_wrong_scope_and_incomplete_claims() {
    let owner = CoordinatorServiceImpl::with_local_nodes(Vec::new());
    let view = DocumentVisibility {
        filter: crate::cel::compile_filter("audience == 'public'").unwrap(),
    };
    let scope = crate::visibility::VisibilityScope::new(Some(&view)).unwrap();
    let valid = MembershipBitmapResponse {
        stats_epoch: 4,
        stats_incarnation: vec![1; 32],
        visibility_fingerprint: scope.fingerprint().to_vec(),
        visibility_columns_known: vec![true],
        ..Default::default()
    };
    for case in 0..5 {
        let mut malformed = valid.clone();
        match case {
            0 => malformed.visibility_fingerprint.clear(),
            1 => malformed.visibility_columns_known.clear(),
            2 => malformed.stats_epoch = 0,
            3 => malformed.stats_incarnation.clear(),
            _ => malformed.visibility_fingerprint[0] ^= 1,
        }
        let mut known = vec![false];
        assert_eq!(
            owner
                .check_read_view(0, &scope, &malformed, &mut known)
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
        assert_eq!(known, vec![false]);
    }
    let mut known = vec![false];
    assert_eq!(
        owner
            .check_read_view(0, &scope, &valid, &mut known)
            .unwrap(),
        StatsClaim::required(4, &[1; 32]).unwrap()
    );
    assert_eq!(known, vec![true]);
}

/// One shard holding a public red row and a private row, both boosted.
async fn audience_boost_node() -> Arc<crate::node::NodeServiceImpl> {
    let node = Arc::new(crate::node::NodeServiceImpl::new(
        None,
        crate::node::NodeConfig {
            analysis_addr: Some(crate::analyzer::NATIVE_ANALYSIS_BACKEND.into()),
            facet_fields: vec!["audience".into(), "color".into()],
            numeric_fields: vec!["boost".into()],
            ..Default::default()
        },
    ));
    crate::link::NodeLink::local(node.clone())
        .add_documents(tokio_stream::iter(
            [("public", "red"), ("private", "secret")]
                .into_iter()
                .map(|(audience, color)| AddDocumentsRequest {
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
                    numerics: vec![NumericValue {
                        field: "boost".into(),
                        value: 2.0,
                    }],
                    ..Default::default()
                }),
        ))
        .await
        .unwrap();
    node
}

/// Public rows only; `color` usable and disclosable, `boost` usable.
fn color_disclosure_access() -> AccessDecision {
    AccessDecision {
        action: AccessAction::Search as i32,
        document_visibility: Some(DocumentVisibility {
            filter: crate::cel::compile_filter("audience == 'public'").unwrap(),
        }),
        field_permissions: Some(FieldPermissions {
            grants: vec![
                FieldGrant {
                    field: "color".into(),
                    actions: vec![FieldAction::Use as i32, FieldAction::Disclose as i32],
                },
                FieldGrant {
                    field: "boost".into(),
                    actions: vec![FieldAction::Use as i32],
                },
            ],
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[tokio::test]
async fn candidate_fetch_enforces_authority_fields_and_documents_before_disclosure() {
    let node = audience_boost_node().await;
    let owner = CoordinatorServiceImpl::with_local_nodes(vec![node]);
    let access = color_disclosure_access();
    // The same authority-bound clone used by certified public routes.
    // Query remains refused until all of its phases consume this context.
    let reader = owner.for_access(Some(&access), "bm25_search").unwrap();
    let projection = |expr: &str| CompiledProjection {
        name: "color".into(),
        expr: Some(crate::cel::compile_value(expr).unwrap()),
    };
    let stages = vec![ScoreStage {
        column: "boost".into(),
        operation: Some(crate::pb::score_stage::Operation::Op(
            ScoreOp::AddLinear as i32,
        )),
        weight: 1.0,
        ..Default::default()
    }];
    let rows = reader
        .fetch_values(&[0, 1], &[projection("color")], &stages)
        .await
        .unwrap();
    assert_eq!(rows.rows.len(), 1);
    assert!(rows.rows.contains_key(&0));
    assert_eq!(rows.stage_rows[0].len(), 1);
    assert!(rows.stage_rows[0].contains_key(&0));
    for forbidden in ["audience", "boost", "true ? color : audience"] {
        let error = reader
            .fetch_values(&[], &[projection(forbidden)], &[])
            .await
            .err()
            .unwrap();
        assert_eq!(error.code(), tonic::Code::PermissionDenied);
        assert_eq!(error.message(), "field access is not granted");
    }
    let mut unknown = access;
    unknown.document_visibility.as_mut().unwrap().filter =
        crate::cel::compile_filter("absent == 'public'").unwrap();
    let reader = owner.for_access(Some(&unknown), "bm25_search").unwrap();
    let error = reader
        .fetch_values(&[], &[projection("color")], &[])
        .await
        .err()
        .unwrap();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(!error.message().contains("absent"));
}

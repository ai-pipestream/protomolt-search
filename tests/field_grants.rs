mod common;
use pipestream_search::{
    analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND},
    authorization::{Authorizer, PolicyAuthority},
    collections::CollectionSet,
    coordinator::CoordinatorServiceImpl,
    link::NodeLink,
    node::{NodeConfig, NodeServiceImpl},
    pb::{search_service_server::SearchService, *},
    security::{PrincipalConfig, Principals},
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tonic::{Code, Request, Status};

fn permissions(fields: &[(&str, &[FieldAction])], identity: bool) -> FieldPermissions {
    FieldPermissions {
        grants: fields
            .iter()
            .map(|(field, actions)| FieldGrant {
                field: (*field).into(),
                actions: actions.iter().map(|a| *a as i32).collect(),
            })
            .collect(),
        disclose_document_identity: identity,
    }
}
fn policy(fields: Option<FieldPermissions>, documents: bool) -> AccessPolicy {
    AccessPolicy {
        format_version: 3,
        revision: 1,
        resources: vec![CollectionResource {
            workspace: "work".into(),
            collection: "".into(),
        }],
        grants: vec![CollectionGrant {
            principal: "reader".into(),
            workspace: "work".into(),
            collection: "".into(),
            actions: vec![AccessAction::Search as i32],
            field_permissions: fields,
            document_visibility: documents.then(|| DocumentVisibility {
                filter: Some(FilterExpr {
                    expr: Some(filter_expr::Expr::Facet(FacetPredicate {
                        column: "audience".into(),
                        values: vec!["public".into()],
                    })),
                }),
            }),
        }],
    }
}
fn service(coordinator: CoordinatorServiceImpl, authority: Arc<dyn Authorizer>) -> CollectionSet {
    CollectionSet::single(coordinator).with_principals(Arc::new(
        Principals::from_configs(&[PrincipalConfig {
            name: "reader".into(),
            token: "reader-token-0123456789012345".into(),
            ..Default::default()
        }])
        .unwrap()
        .with_authorizer(authority),
    ))
}
fn request<T>(body: T) -> Request<T> {
    let mut request = Request::new(body);
    request.metadata_mut().insert(
        "authorization",
        "Bearer reader-token-0123456789012345".parse().unwrap(),
    );
    request
}
fn query() -> Bm25SearchRequest {
    Bm25SearchRequest {
        text: "alpha".into(),
        k: 10,
        analysis: Some(body_spec()),
        ..Default::default()
    }
}
async fn cluster(streaming: bool) -> CoordinatorServiceImpl {
    let mut nodes = Vec::new();
    for offset in [0, 100] {
        let node = Arc::new(NodeServiceImpl::new(
            None,
            NodeConfig {
                slot_offset: offset,
                analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
                bm25_fields: vec!["body".into(), "body.bigrams".into(), "title".into()],
                position_fields: vec!["body".into()],
                bigram_fields: vec!["body".into()],
                facet_fields: vec!["audience".into(), "color".into(), "secret".into()],
                numeric_fields: vec!["boost".into()],
                ..Default::default()
            },
        ));
        for (i, (text, audience, color)) in [
            ("alpha beta", "public", "red"),
            ("alpha alpha", "private", "blue"),
        ]
        .into_iter()
        .enumerate()
        {
            NodeLink::local(node.clone())
                .add_documents(tokio_stream::iter([AddDocumentsRequest {
                    text: text.into(),
                    analysis: Some(body_spec()),
                    fields: vec![DocumentField {
                        field: "title".into(),
                        text: "alpha title".into(),
                        analysis: Some(body_spec()),
                    }],
                    facets: vec![
                        FacetValue {
                            field: "audience".into(),
                            value: audience.into(),
                        },
                        FacetValue {
                            field: "color".into(),
                            value: color.into(),
                        },
                        FacetValue {
                            field: "secret".into(),
                            value: "private_value".into(),
                        },
                    ],
                    numerics: vec![NumericValue {
                        field: "boost".into(),
                        value: (i + 1) as f64,
                    }],
                    original_source: Some(common::protobuf_source(text, "source")),
                    identity: Some(DocumentIdentity {
                        document_key: format!("private-key-{offset}-{i}").into_bytes(),
                        version: 1,
                        chunk_ordinal: None,
                    }),
                    ..Default::default()
                }]))
                .await
                .unwrap();
        }
        nodes.push(node);
    }
    CoordinatorServiceImpl::with_local_nodes(nodes)
        .with_bm25(Some(NATIVE_ANALYSIS_BACKEND.into()), Default::default())
        .with_bm25_stream(streaming)
}

#[test]
fn policy_versions_and_explicit_field_actions_refuse_ambiguous_grants() {
    let fields = permissions(
        &[("body", &[FieldAction::Use, FieldAction::Disclose])],
        false,
    );
    for version in [1, 2] {
        let mut p = policy(Some(fields.clone()), false);
        p.format_version = version;
        assert!(PolicyAuthority::new(p).unwrap_err().contains("format 3"));
    }
    for actions in [vec![], vec![0], vec![100], vec![1, 1]] {
        let mut fields = fields.clone();
        fields.grants[0].actions = actions;
        assert!(PolicyAuthority::new(policy(Some(fields), false)).is_err());
    }
    let mut duplicate = fields.clone();
    duplicate.grants.push(duplicate.grants[0].clone());
    assert!(PolicyAuthority::new(policy(Some(duplicate), false)).is_err());
    let mut p = policy(Some(fields), true);
    p.grants[0].actions.push(AccessAction::Admin as i32);
    let authority = PolicyAuthority::new(p).unwrap();
    assert!(authority
        .authorize("reader", "", AccessAction::Admin)
        .unwrap()
        .field_permissions
        .is_none());
    assert!(authority
        .authorize("reader", "", AccessAction::Search)
        .unwrap()
        .field_permissions
        .is_some());
    PolicyAuthority::new(policy(Some(FieldPermissions::default()), false)).unwrap();
}

#[tokio::test]
async fn use_only_fields_keep_ranking_but_omit_automatic_details_and_raw_keys() {
    for streaming in [false, true] {
        let coordinator = cluster(streaming).await;
        let expected = SearchService::bm25_search(&coordinator, Request::new(query()))
            .await
            .unwrap()
            .into_inner();
        assert!(expected
            .hits
            .iter()
            .all(|h| h.identity.is_some() && !h.terms.is_empty()));
        let cache = coordinator.stats_cache();
        let fetched = cache.fetch_count();
        let reader = service(
            coordinator.clone(),
            Arc::new(
                PolicyAuthority::new(policy(
                    Some(permissions(&[("body", &[FieldAction::Use])], false)),
                    false,
                ))
                .unwrap(),
            ),
        );
        let actual = SearchService::bm25_search(&reader, request(query()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(cache.fetch_count(), fetched);
        assert_eq!(
            actual
                .hits
                .iter()
                .map(|h| (h.doc_id, h.score.to_bits()))
                .collect::<Vec<_>>(),
            expected
                .hits
                .iter()
                .map(|h| (h.doc_id, h.score.to_bits()))
                .collect::<Vec<_>>()
        );
        assert!(actual.field_details_redacted);
        assert!(actual
            .hits
            .iter()
            .all(|h| h.terms.is_empty() && h.identity.is_none()));
        let reader = service(
            coordinator,
            Arc::new(
                PolicyAuthority::new(policy(
                    Some(permissions(
                        &[("body", &[FieldAction::Use, FieldAction::Disclose])],
                        true,
                    )),
                    false,
                ))
                .unwrap(),
            ),
        );
        let actual = SearchService::bm25_search(&reader, request(query()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(actual.hits, expected.hits);
        assert!(!actual.field_details_redacted);
    }
}

#[tokio::test]
async fn every_field_read_is_checked_before_statistics_and_user_aliases_cannot_bypass_it() {
    let coordinator = cluster(false).await;
    let cache = coordinator.stats_cache();
    let fields = permissions(
        &[
            ("body", &[FieldAction::Use]),
            ("color", &[FieldAction::Use, FieldAction::Disclose]),
        ],
        false,
    );
    let reader = service(
        coordinator.clone(),
        Arc::new(PolicyAuthority::new(policy(Some(fields), false)).unwrap()),
    );
    let mut requests = Vec::new();
    for filter in [
        "secret == 'x'",
        "has(secret)",
        "secret.startsWith('x')",
        "secret < 'x'",
        "boost > 0",
        "tags['k'] == 'x'",
        "'k' in tags",
        "metrics['k'] > 0",
        "color == 'red' && secret == 'x'",
    ] {
        let mut q = query();
        q.filter = filter.into();
        requests.push(q);
    }
    for expression in [
        "secret",
        "tags['k']",
        "boost + 1.0",
        "true ? color : secret",
    ] {
        let mut q = query();
        q.projections = vec![NamedProjection {
            name: "color".into(),
            expression: expression.into(),
        }];
        requests.push(q);
    }
    let mut q = query();
    q.facet_fields = vec!["secret".into()];
    requests.push(q);
    let mut q = query();
    q.stats_fields = vec!["boost".into()];
    requests.push(q);
    let mut q = query();
    q.cardinality_fields = vec!["secret".into()];
    requests.push(q);
    let mut q = query();
    q.map_facet_fields = vec![MapFacetField {
        column: "tags".into(),
        key: "k".into(),
    }];
    requests.push(q);
    let mut q = query();
    q.range_facet_fields = vec![RangeFacetField {
        column: "boost".into(),
        edges: vec![0., 2.],
        ..Default::default()
    }];
    requests.push(q);
    let mut q = query();
    q.score_stages = vec![ScoreStage {
        column: "boost".into(),
        op: ScoreOp::AddLinear as i32,
        weight: 1.,
        ..Default::default()
    }];
    requests.push(q);
    let mut q = query();
    q.geo_filters = vec![GeoFilter {
        column: "location".into(),
        ..Default::default()
    }];
    requests.push(q);
    let mut q = query();
    q.analysis = None;
    q.fields = vec![QueryField {
        field: "title".into(),
        analysis: Some(body_spec()),
        ..Default::default()
    }];
    requests.push(q);
    let mut q = query();
    q.highlight = Some(HighlightSpec::default());
    requests.push(q);
    let mut q = query();
    q.explain = true;
    requests.push(q);
    let mut q = query();
    q.prefixes = vec![TermPrefix {
        prefix: "al".into(),
        max_expansions: 10,
    }];
    requests.push(q);
    for (i, q) in requests.into_iter().enumerate() {
        let error = SearchService::bm25_search(&reader, request(q))
            .await
            .unwrap_err();
        assert_eq!(error.code(), Code::PermissionDenied, "case {i}: {error}");
    }
    assert_eq!(cache.fetch_count(), 0);
    let mut q = query();
    q.projections = vec![NamedProjection {
        name: "out".into(),
        expression: "color".into(),
    }];
    let out = SearchService::bm25_search(&reader, request(q))
        .await
        .unwrap()
        .into_inner();
    assert!(out.hits.iter().all(|h| h.projected.len() == 1));
}

#[tokio::test]
async fn document_and_field_grants_compose_and_dictionary_disclosure_is_explicit() {
    let coordinator = cluster(false).await;
    for fields in [
        FieldPermissions::default(),
        permissions(&[("*", &[FieldAction::Use, FieldAction::Disclose])], false),
        permissions(&[("body", &[FieldAction::Disclose])], false),
    ] {
        let reader = service(
            coordinator.clone(),
            Arc::new(PolicyAuthority::new(policy(Some(fields), false)).unwrap()),
        );
        assert_eq!(
            SearchService::bm25_search(&reader, request(query()))
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
    }
    let fields = permissions(
        &[("body", &[FieldAction::Use, FieldAction::Disclose])],
        false,
    );
    let reader = service(
        coordinator.clone(),
        Arc::new(PolicyAuthority::new(policy(Some(fields), true)).unwrap()),
    );
    let out = SearchService::bm25_search(&reader, request(query()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(out.hits.len(), 2); // The private audience predicate does not grant its field to the caller.
    let mut q = query();
    q.filter = "audience == 'public'".into();
    let mut forged = request(q);
    forged.extensions_mut().insert(AccessDecision::default());
    assert_eq!(
        SearchService::bm25_search(&reader, forged)
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    for field in ["body", "title"] {
        let suggestion = SuggestRequest {
            field: field.into(),
            prefix: "al".into(),
            analysis: Some(body_spec()),
            ..Default::default()
        };
        let correction = TermSuggestRequest {
            field: field.into(),
            text: "alpa".into(),
            analysis: Some(body_spec()),
            ..Default::default()
        };
        if field == "body" {
            let out = SearchService::suggest(&reader, request(suggestion))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(out.suggestions.len(), 1);
            assert_eq!(out.suggestions[0].df, 2);
            SearchService::term_suggest(&reader, request(correction))
                .await
                .unwrap();
        } else {
            assert_eq!(
                SearchService::suggest(&reader, request(suggestion))
                    .await
                    .unwrap_err()
                    .code(),
                Code::PermissionDenied
            );
            assert_eq!(
                SearchService::term_suggest(&reader, request(correction))
                    .await
                    .unwrap_err()
                    .code(),
                Code::PermissionDenied
            );
        }
    }
    let reader = service(
        coordinator,
        Arc::new(
            PolicyAuthority::new(policy(
                Some(permissions(&[("body", &[FieldAction::Use])], false)),
                false,
            ))
            .unwrap(),
        ),
    );
    assert_eq!(
        SearchService::suggest(
            &reader,
            request(SuggestRequest {
                field: "body".into(),
                prefix: "al".into(),
                analysis: Some(body_spec()),
                ..Default::default()
            })
        )
        .await
        .unwrap_err()
        .code(),
        Code::PermissionDenied
    );
    assert_eq!(
        SearchService::query(&reader, request(QueryRequest::default()))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
}

#[tokio::test]
async fn fused_details_and_auxiliary_phrase_fields_follow_their_own_grants() {
    let coordinator = cluster(true).await;
    let fields = permissions(
        &[
            ("body", &[FieldAction::Use, FieldAction::Disclose]),
            ("title", &[FieldAction::Use]),
        ],
        false,
    );
    let reader = service(
        coordinator.clone(),
        Arc::new(PolicyAuthority::new(policy(Some(fields), false)).unwrap()),
    );
    let mut q = query();
    q.analysis = None;
    q.fields = ["body", "title"]
        .into_iter()
        .map(|field| QueryField {
            field: field.into(),
            analysis: Some(body_spec()),
            ..Default::default()
        })
        .collect();
    let expected = SearchService::bm25_search(&coordinator, Request::new(q.clone()))
        .await
        .unwrap()
        .into_inner();
    let out = SearchService::bm25_search(&reader, request(q))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        out.hits
            .iter()
            .map(|h| (h.doc_id, h.score.to_bits()))
            .collect::<Vec<_>>(),
        expected
            .hits
            .iter()
            .map(|h| (h.doc_id, h.score.to_bits()))
            .collect::<Vec<_>>()
    );
    assert!(out.field_details_redacted);
    assert!(out
        .hits
        .iter()
        .flat_map(|h| &h.terms)
        .all(|term| term.field == "body"));
    let mut phrase = query();
    phrase.text = "alpha beta".into();
    phrase.phrase = Some(PhraseMatch::default());
    phrase.explain = true;
    let owner = SearchService::bm25_search(&coordinator, Request::new(phrase.clone()))
        .await
        .unwrap()
        .into_inner();
    assert!(owner.phrase_routing[0].bigram_column);
    let out = SearchService::bm25_search(&reader, request(phrase.clone()))
        .await
        .unwrap()
        .into_inner();
    assert!(!out.phrase_routing[0].bigram_column);
    assert_eq!(out.phrase_routing[0].served_field, "body");
    assert_eq!(
        out.hits.iter().map(|h| h.doc_id).collect::<Vec<_>>(),
        vec![0, 100]
    );
    let fields = permissions(
        &[
            ("body", &[FieldAction::Use, FieldAction::Disclose]),
            ("body.bigrams", &[FieldAction::Use]),
        ],
        false,
    );
    let reader = service(
        coordinator,
        Arc::new(PolicyAuthority::new(policy(Some(fields), false)).unwrap()),
    );
    // An explanation must not expose the auxiliary column's tf/df/lengths.
    let out = SearchService::bm25_search(&reader, request(phrase.clone()))
        .await
        .unwrap()
        .into_inner();
    assert!(!out.phrase_routing[0].bigram_column);
    phrase.explain = false;
    let out = SearchService::bm25_search(&reader, request(phrase))
        .await
        .unwrap()
        .into_inner();
    assert!(out.hits.iter().all(|h| h.terms.is_empty()));
    assert!(out.phrase_routing.is_empty());
    assert!(out.field_details_redacted);
}

#[derive(Debug)]
struct MovingFields {
    authority: PolicyAuthority,
    calls: AtomicUsize,
}
impl Authorizer for MovingFields {
    fn authorize(
        &self,
        principal: &str,
        collection: &str,
        action: AccessAction,
    ) -> Result<AccessDecision, Status> {
        let mut decision = self.authority.authorize(principal, collection, action)?;
        if self.calls.fetch_add(1, Ordering::SeqCst) >= 2 {
            decision.field_permissions = Some(FieldPermissions::default());
        }
        Ok(decision)
    }
    fn subscribe(&self) -> tokio::sync::watch::Receiver<u64> {
        self.authority.subscribe()
    }
}
#[tokio::test]
async fn field_revocation_invalidates_computed_results_without_a_revision_bump() {
    let coordinator = cluster(false).await;
    for route in 0..3 {
        let fields = permissions(
            &[("body", &[FieldAction::Use, FieldAction::Disclose])],
            true,
        );
        let reader = service(
            coordinator.clone(),
            Arc::new(MovingFields {
                authority: PolicyAuthority::new(policy(Some(fields), false)).unwrap(),
                calls: AtomicUsize::new(0),
            }),
        );
        let error = match route {
            0 => SearchService::bm25_search(&reader, request(query()))
                .await
                .unwrap_err(),
            1 => SearchService::suggest(
                &reader,
                request(SuggestRequest {
                    field: "body".into(),
                    prefix: "al".into(),
                    analysis: Some(body_spec()),
                    ..Default::default()
                }),
            )
            .await
            .unwrap_err(),
            _ => SearchService::term_suggest(
                &reader,
                request(TermSuggestRequest {
                    field: "body".into(),
                    text: "alpa".into(),
                    analysis: Some(body_spec()),
                    ..Default::default()
                }),
            )
            .await
            .unwrap_err(),
        };
        assert_eq!(error.code(), Code::PermissionDenied);
        assert!(error.message().contains("changed"));
    }
}

#[tokio::test]
async fn numeric_use_does_not_grant_explanation_or_projection_and_network_bypasses_refuse() {
    let coordinator = cluster(false).await;
    let fields = permissions(
        &[
            ("body", &[FieldAction::Use, FieldAction::Disclose]),
            ("boost", &[FieldAction::Use]),
            ("color", &[FieldAction::Use, FieldAction::Disclose]),
        ],
        false,
    );
    let authority = Arc::new(PolicyAuthority::new(policy(Some(fields), false)).unwrap());
    let reader = service(coordinator.clone(), authority.clone());
    let mut q = query();
    q.score_stages = vec![ScoreStage {
        column: "boost".into(),
        op: ScoreOp::AddLinear as i32,
        weight: 1.,
        ..Default::default()
    }];
    q.facet_fields = vec!["color".into()];
    let expected = SearchService::bm25_search(&coordinator, Request::new(q.clone()))
        .await
        .unwrap()
        .into_inner();
    let out = SearchService::bm25_search(&reader, request(q.clone()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(out.facets, expected.facets);
    assert_eq!(
        out.hits
            .iter()
            .map(|h| (h.doc_id, h.score.to_bits()))
            .collect::<Vec<_>>(),
        expected
            .hits
            .iter()
            .map(|h| (h.doc_id, h.score.to_bits()))
            .collect::<Vec<_>>()
    );
    q.explain = true;
    assert_eq!(
        SearchService::bm25_search(&reader, request(q.clone()))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    q.explain = false;
    q.projections = vec![NamedProjection {
        name: "out".into(),
        expression: "boost".into(),
    }];
    assert_eq!(
        SearchService::bm25_search(&reader, request(q))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    let network = service(
        CoordinatorServiceImpl::new(vec!["http://must-not-resolve.invalid:50051".into()]),
        authority,
    );
    assert_eq!(
        SearchService::bm25_search(&network, request(query()))
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
}

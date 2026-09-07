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

#[derive(Debug)]
struct DiagnosticFailureAfterAdmission {
    inner: PolicyAuthority,
    calls: AtomicUsize,
    fail_at: usize,
}
impl Authorizer for DiagnosticFailureAfterAdmission {
    fn authorize(
        &self,
        principal: &str,
        collection: &str,
        action: AccessAction,
    ) -> Result<AccessDecision, Status> {
        if self.calls.fetch_add(1, Ordering::SeqCst) >= self.fail_at {
            let mut error = Status::with_details(
                Code::Internal,
                "PRIVATE policy backend diagnostic",
                b"PRIVATE detail bytes".to_vec().into(),
            );
            error
                .metadata_mut()
                .insert("x-policy-debug", "PRIVATE-header".parse().unwrap());
            return Err(error);
        }
        self.inner.authorize(principal, collection, action)
    }
    fn subscribe(&self) -> tokio::sync::watch::Receiver<u64> {
        self.inner.subscribe()
    }
}

#[tokio::test]
async fn restricted_collection_errors_do_not_disclose_late_diagnostics() {
    use prost::Message;
    let owner = cluster(false).await;
    for (field_grant, document_grant) in
        [(false, false), (true, false), (false, true), (true, true)]
    {
        let fields = field_grant.then(|| {
            permissions(
                &[("body", &[FieldAction::Use, FieldAction::Disclose])],
                false,
            )
        });
        let authority = Arc::new(DiagnosticFailureAfterAdmission {
            inner: PolicyAuthority::new(policy(fields, document_grant)).unwrap(),
            calls: AtomicUsize::new(0),
            fail_at: 2,
        });
        let reader = service(owner.clone(), authority.clone());
        let error = SearchService::bm25_search(&reader, request(query()))
            .await
            .unwrap_err();
        assert_eq!(
            authority.calls.load(Ordering::SeqCst),
            3,
            "failure must occur after handler execution"
        );
        assert_eq!(error.code(), Code::Internal);
        if field_grant || document_grant {
            assert!(!error.message().contains("PRIVATE"));
            assert!(error.metadata().is_empty());
            let envelope = google_rpc::Status::decode(error.details()).unwrap();
            assert_eq!(envelope.code, Code::Internal as i32);
            assert_eq!(envelope.message, error.message());
            assert_eq!(envelope.details.len(), 1);
            assert_eq!(
                envelope.details[0].type_url,
                "type.googleapis.com/ai.protomolt.search.v1.ErrorDisclosure"
            );
            let detail = ErrorDisclosure::decode(envelope.details[0].value.as_slice()).unwrap();
            assert!(detail.details_redacted);
        } else {
            assert!(error.message().contains("PRIVATE"));
            assert_eq!(error.details(), b"PRIVATE detail bytes");
            assert!(error.metadata().contains_key("x-policy-debug"));
        }
    }
}

#[tokio::test]
async fn authority_errors_before_a_grant_never_disclose_backend_details() {
    use prost::Message;
    for fail_at in [0, 1] {
        let authority = Arc::new(DiagnosticFailureAfterAdmission {
            inner: PolicyAuthority::new(policy(None, false)).unwrap(),
            calls: AtomicUsize::new(0),
            fail_at,
        });
        let reader = service(
            CoordinatorServiceImpl::with_local_nodes(vec![]),
            authority.clone(),
        );
        let error = SearchService::bm25_search(&reader, request(query()))
            .await
            .unwrap_err();
        assert_eq!(error.code(), Code::Internal);
        assert_eq!(authority.calls.load(Ordering::SeqCst), fail_at + 1);
        assert!(!error.message().contains("PRIVATE"));
        assert!(error.metadata().is_empty());
        let envelope = google_rpc::Status::decode(error.details()).unwrap();
        let detail = ErrorDisclosure::decode(envelope.details[0].value.as_slice()).unwrap();
        assert!(detail.details_redacted);
    }
}
async fn cluster(streaming: bool) -> CoordinatorServiceImpl {
    cluster_subset(streaming, false, false).await
}
async fn cluster_subset(
    streaming: bool,
    public_only: bool,
    varying: bool,
) -> CoordinatorServiceImpl {
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
            if public_only && audience != "public" {
                continue;
            }
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
                        value: (i + 1) as f64 + if varying { offset as f64 } else { 0. },
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
        "tags[''] >= 'x'",
        "tags[''].startsWith('x')",
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
        Code::InvalidArgument
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
    for route in 0..4 {
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
            2 => SearchService::term_suggest(
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
            _ => SearchService::aggregate(&reader, request(count_request()))
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

fn count_request() -> AggregateRequest {
    AggregateRequest {
        aggregations: vec![Aggregation {
            name: "count".into(),
            expression: "1".into(),
            op: AggregateOp::Count as i32,
            ..Default::default()
        }],
        ..Default::default()
    }
}
fn aggregate_request() -> AggregateRequest {
    AggregateRequest {
        aggregations: [
            ("count", "1", AggregateOp::Count),
            ("sum", "boost", AggregateOp::Sum),
            ("mean", "boost", AggregateOp::Mean),
            ("colors", "color", AggregateOp::Cardinality),
        ]
        .into_iter()
        .map(|(name, expression, op)| Aggregation {
            name: name.into(),
            expression: expression.into(),
            op: op as i32,
            ..Default::default()
        })
        .collect(),
        group_by: "color".into(),
        histograms: vec![HistogramSpec {
            name: "buckets".into(),
            expression: "boost".into(),
            interval: 1.,
            ..Default::default()
        }],
        percentiles: vec![PercentileSpec {
            name: "percentiles".into(),
            expression: "boost".into(),
            percentiles: vec![0., 50., 75., 100.],
        }],
        ..Default::default()
    }
}
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

fn public_query() -> QueryRequest {
    QueryRequest {
        k: 4,
        selection: Some(SelectionQuery {
            node: Some(selection_query::Node::Search(SearchQuery {
                id: "lex".into(),
                query: Some(search_query::Query::Lexical(LexicalQuery {
                    text: "alpha".into(),
                    analysis: Some(body_spec()),
                    ..Default::default()
                })),
            })),
        }),
        ..Default::default()
    }
}
fn stored_query() -> QueryRequest {
    QueryRequest {
        scorer: Some(CompositeScorer {
            operation: CompositeScoreOperation::WeightedSum as i32,
            dimensions: vec![ScoreDimension {
                id: "stored".into(),
                normalization: ScoreNormalization::None as i32,
                source: Some(ScoreSignal {
                    source: Some(score_signal::Source::BoundedValue(ScoreStage {
                        column: "boost".into(),
                        op: ScoreOp::AddLinear as i32,
                        weight: 1.,
                        ..Default::default()
                    })),
                }),
                ..Default::default()
            }],
        }),
        ..public_query()
    }
}
fn field_reader(coordinator: CoordinatorServiceImpl, fields: FieldPermissions) -> CollectionSet {
    service(
        coordinator,
        Arc::new(PolicyAuthority::new(policy(Some(fields), false)).unwrap()),
    )
}
fn signature(response: &QueryResponse) -> Vec<(u64, u32)> {
    response
        .hits
        .iter()
        .map(|h| (h.doc_id, h.score.to_bits()))
        .collect()
}

#[tokio::test]
async fn public_query_redacts_stored_dimensions_and_inner_hit_identity_without_changing_scores() {
    let owner = cluster_subset(true, false, true).await;
    let request_body = QueryRequest {
        collapse: Some(CollapseSpec {
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
                column: "secret".into(),
                descending: false,
            }],
            ..public_query()
        },
        QueryRequest {
            collapse: Some(CollapseSpec {
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

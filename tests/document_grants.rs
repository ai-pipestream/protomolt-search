use pipestream_search::{
    analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND},
    authorization::{AccessPermit, Authorizer, PolicyAuthority},
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
use tonic::{Code, Request};

fn view(value: &str) -> DocumentVisibility {
    DocumentVisibility {
        filter: Some(FilterExpr {
            expr: Some(filter_expr::Expr::Facet(FacetPredicate {
                column: "audience".into(),
                values: vec![value.into()],
            })),
        }),
    }
}

fn policy() -> AccessPolicy {
    AccessPolicy {
        format_version: 2,
        revision: 1,
        resources: vec![CollectionResource {
            workspace: "work".into(),
            collection: "".into(),
        }],
        grants: ["public", "private", "owner"]
            .into_iter()
            .map(|name| CollectionGrant {
                field_permissions: None,
                principal: name.into(),
                workspace: "work".into(),
                collection: "".into(),
                actions: vec![AccessAction::Search as i32],
                document_visibility: (name != "owner").then(|| view(name)),
            })
            .collect(),
    }
}

fn principals(authority: Arc<dyn Authorizer>) -> Arc<Principals> {
    Arc::new(
        Principals::from_configs(&["public", "private", "owner"].map(|name| PrincipalConfig {
            name: name.into(),
            token: format!("{name}-token-0123456789012345"),
            ..Default::default()
        }))
        .unwrap()
        .with_authorizer(authority),
    )
}
fn request<T>(body: T, name: &str) -> Request<T> {
    let mut request = Request::new(body);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {name}-token-0123456789012345")
            .parse()
            .unwrap(),
    );
    request
}

async fn ingest(node: Arc<NodeServiceImpl>, text: &str, audience: &str, color: &str) {
    NodeLink::local(node)
        .add_documents(tokio_stream::iter([AddDocumentsRequest {
            text: text.into(),
            analysis: Some(body_spec()),
            fields: vec![DocumentField {
                field: "title".into(),
                text: format!("alpha {color}"),
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
            ],
            ..Default::default()
        }]))
        .await
        .unwrap();
}

async fn cluster(
    restricted: bool,
    streaming: bool,
) -> (CoordinatorServiceImpl, Vec<Arc<NodeServiceImpl>>) {
    let mut nodes = Vec::new();
    for offset in [0, 100] {
        let node = Arc::new(NodeServiceImpl::new(
            None,
            NodeConfig {
                slot_offset: offset,
                analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
                bm25_fields: vec!["body".into(), "title".into()],
                facet_fields: vec!["audience".into(), "color".into()],
                ..Default::default()
            },
        ));
        ingest(node.clone(), "alpha alpha", "public", "red").await;
        ingest(node.clone(), "alpha beta", "public", "blue").await;
        if !restricted {
            ingest(
                node.clone(),
                "alpha private secret secret",
                "private",
                "hidden",
            )
            .await;
        }
        nodes.push(node);
    }
    let coordinator = CoordinatorServiceImpl::with_local_nodes(nodes.clone())
        .with_bm25(Some(NATIVE_ANALYSIS_BACKEND.into()), Default::default())
        .with_bm25_stream(streaming);
    (coordinator, nodes)
}

fn query(fused: bool) -> Bm25SearchRequest {
    Bm25SearchRequest {
        text: "alpha".into(),
        k: 10,
        analysis: (!fused).then(body_spec),
        fields: if fused {
            ["body", "title"]
                .into_iter()
                .map(|field| QueryField {
                    field: field.into(),
                    analysis: Some(body_spec()),
                    ..Default::default()
                })
                .collect()
        } else {
            vec![]
        },
        facet_fields: vec!["audience".into(), "color".into()],
        projections: if fused {
            vec![]
        } else {
            vec![NamedProjection {
                name: "color".into(),
                expression: "color".into(),
            }]
        },
        highlight: Some(HighlightSpec {
            mode: HighlightMode::Window as i32,
            ..Default::default()
        }),
        explain: true,
        ..Default::default()
    }
}

fn scores(response: &Bm25SearchResponse) -> Vec<(u64, u32)> {
    response
        .hits
        .iter()
        .map(|hit| (hit.doc_id, hit.score.to_bits()))
        .collect()
}

#[test]
fn policy_version_and_scope_semantics_are_explicit() {
    let mut legacy = policy();
    legacy.format_version = 1;
    assert!(PolicyAuthority::new(legacy)
        .unwrap_err()
        .contains("format 2"));
    let mut malformed = policy();
    malformed.grants[0].document_visibility = Some(DocumentVisibility::default());
    assert!(PolicyAuthority::new(malformed).is_err());
    let mut writer = policy();
    writer.grants[0].actions = vec![AccessAction::Ingest as i32];
    assert!(PolicyAuthority::new(writer).is_err());
    let mut mixed = policy();
    mixed.grants[0].actions.push(AccessAction::Admin as i32);
    let authority = Arc::new(PolicyAuthority::new(mixed.clone()).unwrap());
    assert_eq!(
        authority
            .authorize("public", "", AccessAction::Search)
            .unwrap()
            .document_visibility,
        Some(view("public"))
    );
    assert!(authority
        .authorize("public", "", AccessAction::Admin)
        .unwrap()
        .document_visibility
        .is_none());
    let permit =
        AccessPermit::acquire(authority.clone(), "public", "", AccessAction::Search).unwrap();
    mixed.revision = 2;
    mixed.grants[0].document_visibility = Some(view("private"));
    authority.replace(mixed).unwrap();
    assert_eq!(permit.check().unwrap_err().code(), Code::PermissionDenied);
}

#[tokio::test]
async fn document_grants_match_physical_corpus_scores_and_disclosures() {
    for streaming in [false, true] {
        for fused in [false, true] {
            let (coordinator, nodes) = cluster(false, streaming).await;
            let cache = coordinator.stats_cache();
            let (reference, _) = cluster(true, streaming).await;
            let authority = Arc::new(PolicyAuthority::new(policy()).unwrap());
            let service = CollectionSet::single(coordinator.clone())
                .with_principals(principals(authority.clone()));
            let input = query(fused);
            let expected = SearchService::bm25_search(&reference, Request::new(input.clone()))
                .await
                .unwrap()
                .into_inner();
            // Populate the unrestricted cache before querying a restricted view.
            let all = SearchService::bm25_search(&service, request(input.clone(), "owner"))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(all.hits.len(), 6);
            let actual = SearchService::bm25_search(&service, request(input.clone(), "public"))
                .await
                .unwrap()
                .into_inner();
            assert!(actual.execution_details_redacted);
            assert_eq!((actual.segments_total, actual.segments_skipped), (0, 0));
            assert!(!all.execution_details_redacted);
            assert_eq!(scores(&actual), scores(&expected));
            assert_eq!(actual.facets, expected.facets);
            assert_eq!(
                actual.hits, expected.hits,
                "projections, highlights and explains must also agree"
            );
            let fetched = cache.fetch_count();
            let repeated = SearchService::bm25_search(&service, request(input.clone(), "public"))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(actual.hits, repeated.hits);
            assert_eq!(cache.fetch_count(), fetched);
            let hidden = SearchService::bm25_search(&service, request(input.clone(), "private"))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(
                hidden.hits.iter().map(|hit| hit.doc_id).collect::<Vec<_>>(),
                vec![2, 102]
            );
            // A different unauthorized corpus cannot influence public statistics.
            ingest(
                nodes[0].clone(),
                "alpha alpha alpha more secret",
                "private",
                "hidden2",
            )
            .await;
            let after = SearchService::bm25_search(&service, request(input.clone(), "public"))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(after.hits, expected.hits);
            let mut revoked = policy();
            revoked.revision = 2;
            revoked.grants.retain(|grant| grant.principal != "public");
            authority.replace(revoked).unwrap();
            let before = cache.fetch_count();
            assert_eq!(
                SearchService::bm25_search(&service, request(input, "public"))
                    .await
                    .unwrap_err()
                    .code(),
                Code::PermissionDenied
            );
            assert_eq!(
                cache.fetch_count(),
                before,
                "revocation precedes cached execution"
            );
        }
    }
}

#[tokio::test]
async fn caller_filters_and_context_cannot_replace_the_grant() {
    let (coordinator, _) = cluster(false, false).await;
    let service = CollectionSet::single(coordinator).with_principals(principals(Arc::new(
        PolicyAuthority::new(policy()).unwrap(),
    )));
    let mut q = query(false);
    q.filter = "audience == 'public' || audience == 'private'".into();
    let mut input = request(q.clone(), "public");
    input.extensions_mut().insert(AccessDecision {
        field_permissions: None,
        principal: "owner".into(),
        ..Default::default()
    });
    input
        .metadata_mut()
        .insert("x-document-visibility", "unrestricted".parse().unwrap());
    assert_eq!(
        SearchService::bm25_search(&service, input)
            .await
            .unwrap()
            .get_ref()
            .hits
            .len(),
        4
    );
    q.filter = "audience == 'private'".into();
    assert!(SearchService::bm25_search(&service, request(q, "public"))
        .await
        .unwrap()
        .get_ref()
        .hits
        .is_empty());
}

#[tokio::test]
async fn uncertified_routes_and_network_nodes_refuse_before_execution() {
    let (coordinator, _) = cluster(false, false).await;
    let cache = coordinator.stats_cache();
    let authority = Arc::new(PolicyAuthority::new(policy()).unwrap());
    let service =
        CollectionSet::single(coordinator.clone()).with_principals(principals(authority.clone()));
    macro_rules! denied {
        ($method:ident, $message:expr) => {
            assert_eq!(
                SearchService::$method(&service, request($message, "public"))
                    .await
                    .unwrap_err()
                    .code(),
                Code::PermissionDenied,
                stringify!($method)
            );
        };
    }
    denied!(search, SearchRequest::default());
    denied!(phrase_search, PhraseSearchRequest::default());
    denied!(hybrid_search, HybridSearchRequest::default());
    denied!(variant_search, VariantSearchRequest::default());
    denied!(query, QueryRequest::default());
    assert_eq!(
        SearchService::query_stream(&service, request(QueryStreamRequest::default(), "public"))
            .await
            .err()
            .unwrap()
            .code(),
        Code::PermissionDenied
    );
    assert_eq!(cache.fetch_count(), 0);
    let network = CollectionSet::single(CoordinatorServiceImpl::new(vec![
        "http://must-not-resolve.invalid:50051".into(),
    ]))
    .with_principals(principals(authority));
    let error = SearchService::bm25_search(&network, request(query(false), "public"))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("in-process"));
}

#[derive(Debug)]
struct MovingView {
    authority: PolicyAuthority,
    calls: AtomicUsize,
}
impl Authorizer for MovingView {
    fn authorize(
        &self,
        principal: &str,
        collection: &str,
        action: AccessAction,
    ) -> Result<AccessDecision, tonic::Status> {
        let mut decision = self.authority.authorize(principal, collection, action)?;
        if self.calls.fetch_add(1, Ordering::SeqCst) >= 2 {
            decision.document_visibility = Some(view("private"));
        }
        Ok(decision)
    }
    fn subscribe(&self) -> tokio::sync::watch::Receiver<u64> {
        self.authority.subscribe()
    }
}

#[tokio::test]
async fn a_changed_view_cannot_disclose_an_already_computed_response() {
    let (coordinator, _) = cluster(false, false).await;
    for route in 0..4 {
        let authority = Arc::new(MovingView {
            authority: PolicyAuthority::new(policy()).unwrap(),
            calls: AtomicUsize::new(0),
        });
        let service =
            CollectionSet::single(coordinator.clone()).with_principals(principals(authority));
        let error = match route {
            0 => SearchService::bm25_search(&service, request(query(false), "public"))
                .await
                .unwrap_err(),
            1 => SearchService::suggest(
                &service,
                request(
                    SuggestRequest {
                        field: "body".into(),
                        prefix: "al".into(),
                        analysis: Some(body_spec()),
                        ..Default::default()
                    },
                    "public",
                ),
            )
            .await
            .unwrap_err(),
            2 => SearchService::term_suggest(
                &service,
                request(
                    TermSuggestRequest {
                        field: "body".into(),
                        text: "alpa".into(),
                        analysis: Some(body_spec()),
                        ..Default::default()
                    },
                    "public",
                ),
            )
            .await
            .unwrap_err(),
            _ => SearchService::aggregate(
                &service,
                request(
                    AggregateRequest {
                        aggregations: vec![Aggregation {
                            name: "count".into(),
                            expression: "1".into(),
                            op: AggregateOp::Count as i32,
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                    "public",
                ),
            )
            .await
            .unwrap_err(),
        };
        assert_eq!(error.code(), Code::PermissionDenied);
        assert!(error.message().contains("changed"));
    }
}

#[tokio::test]
async fn an_unbound_grant_column_refuses_even_empty_queries_without_naming_the_column() {
    let (coordinator, _) = cluster(false, false).await;
    let mut policy = policy();
    let filter = policy.grants[0]
        .document_visibility
        .as_mut()
        .unwrap()
        .filter
        .as_mut()
        .unwrap();
    let Some(filter_expr::Expr::Facet(facet)) = filter.expr.as_mut() else {
        unreachable!()
    };
    facet.column = "policy_internal_column".into();
    let service = CollectionSet::single(coordinator)
        .with_principals(principals(Arc::new(PolicyAuthority::new(policy).unwrap())));
    for fused in [false, true] {
        for text in ["alpha", " "] {
            let mut q = query(fused);
            q.text = text.into();
            q.projections.clear();
            let error = SearchService::bm25_search(&service, request(q, "public"))
                .await
                .unwrap_err();
            assert_eq!(
                error.code(),
                Code::FailedPrecondition,
                "fused={fused} text={text:?}: {error}"
            );
            assert!(!error.message().contains("policy_internal_column"));
            assert!(error.message().contains("document grant"));
        }
    }
    let suggestion = SuggestRequest {
        field: "body".into(),
        prefix: "al".into(),
        analysis: Some(body_spec()),
        ..Default::default()
    };
    let correction = TermSuggestRequest {
        field: "body".into(),
        text: "alpa".into(),
        analysis: Some(body_spec()),
        ..Default::default()
    };
    for error in [
        SearchService::suggest(&service, request(suggestion, "public"))
            .await
            .unwrap_err(),
        SearchService::term_suggest(&service, request(correction, "public"))
            .await
            .unwrap_err(),
    ] {
        assert_eq!(error.code(), Code::FailedPrecondition);
        assert!(!error.message().contains("policy_internal_column"));
    }
}

#[tokio::test]
async fn dictionaries_and_expansions_disclose_only_the_authorized_corpus() {
    for streaming in [false, true] {
        let (coordinator, nodes) = cluster(false, streaming).await;
        let (reference, reference_nodes) = cluster(true, streaming).await;
        let authority = Arc::new(PolicyAuthority::new(policy()).unwrap());
        let service =
            CollectionSet::single(coordinator).with_principals(principals(authority.clone()));
        // A dense hidden dictionary must not consume a reader's visible cap.
        for node in &nodes {
            ingest(
                node.clone(),
                "albatross alchemy algebra algorithm alien secret",
                "private",
                "hidden",
            )
            .await;
        }
        for prefix in ["al", "se", "be"] {
            let input = SuggestRequest {
                field: "body".into(),
                prefix: prefix.into(),
                max_scan: 1,
                analysis: Some(body_spec()),
                ..Default::default()
            };
            let expected = SearchService::suggest(&reference, Request::new(input.clone()))
                .await
                .unwrap()
                .into_inner();
            let actual = SearchService::suggest(&service, request(input, "public"))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(actual, expected);
            for fused in [false, true] {
                let mut q = query(fused);
                q.text.clear();
                let prefixes = vec![TermPrefix {
                    prefix: prefix.into(),
                    max_expansions: 1,
                }];
                if fused {
                    q.fields.truncate(1);
                    q.fields[0].prefixes = prefixes;
                } else {
                    q.prefixes = prefixes;
                }
                let expected = SearchService::bm25_search(&reference, Request::new(q.clone()))
                    .await
                    .unwrap()
                    .into_inner();
                let actual = SearchService::bm25_search(&service, request(q, "public"))
                    .await
                    .unwrap()
                    .into_inner();
                assert_eq!(actual.hits, expected.hits);
                assert_eq!(actual.prefix_expansions, expected.prefix_expansions);
            }
        }
        for text in ["secret", "secreu", "alpa", "alpha"] {
            for mode in [TermSuggestMode::Missing, TermSuggestMode::Always] {
                let input = TermSuggestRequest {
                    field: "body".into(),
                    text: text.into(),
                    max_scan: 1,
                    mode: mode as i32,
                    analysis: Some(body_spec()),
                    ..Default::default()
                };
                let expected = SearchService::term_suggest(&reference, Request::new(input.clone()))
                    .await
                    .unwrap()
                    .into_inner();
                let actual = SearchService::term_suggest(&service, request(input, "public"))
                    .await
                    .unwrap()
                    .into_inner();
                assert_eq!(actual, expected);
            }
        }
        for node in nodes.iter().chain(reference_nodes.iter()) {
            ingest(node.clone(), "alpine", "public", "green").await;
        }
        let over = SuggestRequest {
            field: "body".into(),
            prefix: "al".into(),
            max_scan: 1,
            analysis: Some(body_spec()),
            ..Default::default()
        };
        let expected = SearchService::suggest(&reference, Request::new(over.clone()))
            .await
            .unwrap_err();
        let actual = SearchService::suggest(&service, request(over.clone(), "public"))
            .await
            .unwrap_err();
        assert_eq!(actual.code(), expected.code());
        assert_eq!(actual.message(), expected.message());
        let mut revoked = policy();
        revoked.revision += 1;
        revoked.grants.remove(0);
        authority.replace(revoked).unwrap();
        assert_eq!(
            SearchService::suggest(&service, request(over, "public"))
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
    }
}

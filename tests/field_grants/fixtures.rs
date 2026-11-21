//! Shared field-permission policies, revocable authorizers, clusters, and query builders.

use super::*;

pub(crate) fn permissions(fields: &[(&str, &[FieldAction])], identity: bool) -> FieldPermissions {
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
pub(crate) fn policy(fields: Option<FieldPermissions>, documents: bool) -> AccessPolicy {
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
pub(crate) fn service(
    coordinator: CoordinatorServiceImpl,
    authority: Arc<dyn Authorizer>,
) -> CollectionSet {
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
pub(crate) fn request<T>(body: T) -> Request<T> {
    let mut request = Request::new(body);
    request.metadata_mut().insert(
        "authorization",
        "Bearer reader-token-0123456789012345".parse().unwrap(),
    );
    request
}
pub(crate) fn query() -> Bm25SearchRequest {
    Bm25SearchRequest {
        text: "alpha".into(),
        k: 10,
        analysis: Some(body_spec()),
        ..Default::default()
    }
}

#[derive(Debug)]
pub(crate) struct DiagnosticFailureAfterAdmission {
    pub(crate) inner: PolicyAuthority,
    pub(crate) calls: AtomicUsize,
    pub(crate) fail_at: usize,
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

pub(crate) async fn cluster(streaming: bool) -> CoordinatorServiceImpl {
    cluster_subset(streaming, false, false).await
}
pub(crate) async fn cluster_subset(
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

pub(crate) fn count_request() -> AggregateRequest {
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
pub(crate) fn aggregate_request() -> AggregateRequest {
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

pub(crate) fn public_query() -> QueryRequest {
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
pub(crate) fn stored_query() -> QueryRequest {
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
pub(crate) fn field_reader(
    coordinator: CoordinatorServiceImpl,
    fields: FieldPermissions,
) -> CollectionSet {
    service(
        coordinator,
        Arc::new(PolicyAuthority::new(policy(Some(fields), false)).unwrap()),
    )
}
pub(crate) fn signature(response: &QueryResponse) -> Vec<(u64, u32)> {
    response
        .hits
        .iter()
        .map(|h| (h.doc_id, h.score.to_bits()))
        .collect()
}

#[derive(Debug)]
pub(crate) struct MovingFields {
    pub(crate) authority: PolicyAuthority,
    pub(crate) calls: AtomicUsize,
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

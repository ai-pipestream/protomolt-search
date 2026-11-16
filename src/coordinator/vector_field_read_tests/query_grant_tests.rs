//! Field grants and execution-detail redaction on the public query routes.

use super::*;
use tokio_stream::StreamExt;

#[tokio::test]
async fn public_field_granted_dense_queries_require_the_indexed_name() {
    use crate::pb::{
        search_query, selection_query, DenseQuery, QueryRequest, SearchQuery, SelectionQuery,
    };
    let owner = CoordinatorServiceImpl::with_local_nodes(vec![node(0, false), node(100, false)]);
    let mut decision = access("semantic", FieldAction::Use);
    decision.document_visibility = None;
    let reader = owner.for_access(Some(&decision), "query").unwrap();
    for streaming in [false, true] {
        let reader = reader.clone().with_stream_search(streaming);
        for mode in [
            crate::pb::DenseScoreMode::Native,
            crate::pb::DenseScoreMode::Fp32Rerank,
        ] {
            let make_query = |field: &str| QueryRequest {
                k: 4,
                selection: Some(SelectionQuery {
                    node: Some(selection_query::Node::Search(SearchQuery {
                        id: "dense".into(),
                        query: Some(search_query::Query::Dense(DenseQuery {
                            field: field.into(),
                            vector: vec![0.25; 16],
                            score_mode: mode as i32,
                            ..Default::default()
                        })),
                    })),
                }),
                ..Default::default()
            };
            let expected = SearchService::query(&owner, Request::new(make_query("semantic")))
                .await
                .unwrap()
                .into_inner();
            let actual = SearchService::query(&reader, Request::new(make_query("semantic")))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(actual.hits, expected.hits);
            for field in ["", "signal", "body"] {
                assert_eq!(
                    SearchService::query(&reader, Request::new(make_query(field)))
                        .await
                        .unwrap_err()
                        .code(),
                    tonic::Code::PermissionDenied
                );
            }
        }
    }
}

/// A semantic-use grant that also allows using and disclosing `body`.
fn body_disclosing_decision() -> AccessDecision {
    let mut decision = access("semantic", FieldAction::Use);
    decision
        .field_permissions
        .as_mut()
        .unwrap()
        .grants
        .push(FieldGrant {
            field: "body".into(),
            actions: vec![FieldAction::Use as i32, FieldAction::Disclose as i32],
        });
    decision
}

/// One request per selection shape: both dense score modes, every hybrid
/// fusion strategy, a boolean over the dense leaf, and a lexical leaf.
fn selection_shape_requests() -> Vec<QueryRequest> {
    use selection_score_strategy::Strategy;
    let mut requests = vec![];
    for mode in [DenseScoreMode::Unspecified, DenseScoreMode::Fp32Rerank] {
        requests.push(QueryRequest {
            k: 4,
            selection_k: 4,
            selection: Some(SelectionQuery {
                node: Some(selection_query::Node::Search(SearchQuery {
                    id: "dense".into(),
                    query: Some(search_query::Query::Dense(DenseQuery {
                        field: "semantic".into(),
                        vector: vec![0.25; 16],
                        score_mode: mode as i32,
                        ..Default::default()
                    })),
                })),
            }),
            ..Default::default()
        });
    }
    for strategy in [
        Strategy::Rrf(RrfScore::default()),
        Strategy::ScoreBlend(BlendScore::default()),
        Strategy::Decomposed(DecomposedScore::default()),
        Strategy::Cascade(CascadeScore {
            gate_id: "dense".into(),
        }),
    ] {
        requests.push(hybrid_query(strategy, "semantic"));
    }
    let dense = requests[0].selection.clone().unwrap();
    requests.push(QueryRequest {
        k: 4,
        selection: Some(SelectionQuery {
            node: Some(selection_query::Node::Boolean(BooleanQuery {
                must: vec![dense],
                ..Default::default()
            })),
        }),
        ..Default::default()
    });
    requests.push(QueryRequest {
        k: 4,
        selection: Some(SelectionQuery {
            node: Some(selection_query::Node::Search(SearchQuery {
                id: "lexical".into(),
                query: Some(search_query::Query::Lexical(LexicalQuery {
                    text: "word".into(),
                    analysis: Some(crate::analyzer::body_spec()),
                    ..Default::default()
                })),
            })),
        }),
        ..Default::default()
    });
    requests
}

/// The redacted reply reports no physical work and no planner detail.
fn assert_execution_details_redacted(reply: &QueryResponse) {
    assert!(reply.execution_details_redacted);
    let profile = reply.profile.as_ref().unwrap();
    assert_eq!(
        (
            profile.rerank_rows,
            profile.rerank_logical_bytes,
            profile.rerank_pages
        ),
        (0, 0, 0)
    );
    assert_eq!(
        (
            profile.rerank_tasks,
            profile.segments_total,
            profile.segments_skipped,
            profile.shards_total,
            profile.shards_skipped
        ),
        (0, 0, 0, 0, 0)
    );
    if let Some(execution) = &reply.dense_execution {
        assert!(execution.planner_reason.is_empty());
        assert!(execution.exhaustive_completion);
        assert_eq!(
            execution.evidence_scope,
            DenseEvidenceScope::NotApplicable as i32
        );
    }
}

/// The stream route discloses only public hits and ends on the same
/// redacted response the unary route returned.
async fn assert_stream_redacted(
    reader: &CoordinatorServiceImpl,
    decision: &AccessDecision,
    request: &QueryRequest,
    raw: &QueryResponse,
) {
    let mut stream_request = Request::new(QueryStreamRequest {
        query: Some(request.clone()),
        ..Default::default()
    });
    stream_request.extensions_mut().insert(decision.clone());
    let mut stream = SearchService::query_stream(reader, stream_request)
        .await
        .unwrap()
        .into_inner();
    let mut completed = false;
    let mut provisional = false;
    while let Some(event) = stream.next().await {
        assert!(!completed);
        match event.unwrap().payload.unwrap() {
            query_stream_response::Payload::Revision(revision) => {
                assert!(
                    revision
                        .hits
                        .iter()
                        .all(|hit| matches!(hit.doc_id, 0 | 100)),
                    "{}: private provisional hit",
                    raw.executed
                );
                provisional |=
                    !revision.hits.is_empty() && revision.phase != QueryStreamPhase::Final as i32;
            }
            query_stream_response::Payload::Completion(end) => {
                assert!(end.completed, "{}: {}", raw.executed, end.error_message);
                let response = end.response.unwrap();
                assert_eq!(response.hits, raw.hits);
                assert!(response.execution_details_redacted);
                completed = true;
            }
        }
    }
    assert!(completed);
    // Single scored leaves have a streaming collector; composite
    // membership can execute without publishing a provisional heap.
    if matches!(
        request.selection.as_ref().and_then(|s| s.node.as_ref()),
        Some(selection_query::Node::Search(_))
    ) {
        assert!(
            provisional,
            "{} must exercise provisional results",
            raw.executed
        );
    }
}

#[tokio::test]
async fn document_query_executor_redacts_physical_details_across_selection_shapes() {
    let decision = body_disclosing_decision();
    let owner = CoordinatorServiceImpl::with_local_nodes(vec![node(0, false), node(100, false)])
        .with_bm25(
            Some(crate::analyzer::NATIVE_ANALYSIS_BACKEND.into()),
            Default::default(),
        );
    let reader = owner.for_access(Some(&decision), "query").unwrap();
    let mut saw_physical_work = false;
    for mut request in selection_shape_requests() {
        request.profile = true;
        let (pinned, _) = reader.pin_read_versions().await.unwrap();
        let raw = crate::query::execute(&pinned, request.clone(), 0)
            .await
            .unwrap();
        let reply = reader
            .execute_query(request.clone(), Some(&decision), 0)
            .await
            .unwrap();
        assert_eq!(reply.hits, raw.hits, "{}", reply.executed);
        assert!(reply
            .hits
            .iter()
            .all(|hit| hit.doc_id == 0 || hit.doc_id == 100));
        saw_physical_work |= raw.profile.as_ref().unwrap().rerank_rows > 0;
        assert_execution_details_redacted(&reply);
        assert_stream_redacted(&reader, &decision, &request, &raw).await;
        let unrestricted = owner.execute_query(request, None, 0).await.unwrap();
        assert!(!unrestricted.execution_details_redacted);
    }
    assert!(
        saw_physical_work,
        "the test must exercise actual FP32 row reads"
    );
}

/// The stream route refuses the incompatible binding before any hit.
async fn assert_stream_refuses_binding(
    incompatible: &CoordinatorServiceImpl,
    query: &QueryRequest,
) {
    let mut stream = SearchService::query_stream(
        incompatible,
        Request::new(QueryStreamRequest {
            query: Some(query.clone()),
            timeout_ms: 5_000,
            ..Default::default()
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let mut failed = false;
    while let Some(frame) = stream.next().await {
        match frame.unwrap().payload.unwrap() {
            query_stream_response::Payload::Revision(revision) => {
                assert!(revision.hits.is_empty())
            }
            query_stream_response::Payload::Completion(completion) => {
                assert!(!completion.completed);
                assert!(completion.response.is_none());
                assert_eq!(
                    completion.error_code,
                    tonic::Code::FailedPrecondition as u32
                );
                failed = true;
            }
        }
    }
    assert!(failed);
}

/// The hybrid query with its dense leaf pointed at another field.
fn with_dense_field(query: QueryRequest, field: &str) -> QueryRequest {
    let mut alias = query;
    let Some(selection_query::Node::Composite(composite)) =
        alias.selection.as_mut().unwrap().node.as_mut()
    else {
        unreachable!()
    };
    let Some(selection_query::Node::Search(leaf)) = composite.clauses[0].node.as_mut() else {
        unreachable!()
    };
    let Some(search_query::Query::Dense(dense)) = leaf.query.as_mut() else {
        unreachable!()
    };
    dense.field = field.into();
    alias
}

#[tokio::test]
async fn public_hybrid_field_grants_validate_actual_bindings_in_every_fusion_mode() {
    use selection_score_strategy::Strategy;
    let mut decision = access("semantic", FieldAction::Use);
    decision.document_visibility = None;
    decision.field_permissions.as_mut().unwrap().grants.extend([
        FieldGrant {
            field: "body".into(),
            actions: vec![FieldAction::Use as i32],
        },
        // A permitted name is still not proof the shard indexed that field.
        FieldGrant {
            field: "signal".into(),
            actions: vec![FieldAction::Use as i32],
        },
    ]);
    let cluster = |different| {
        CoordinatorServiceImpl::with_local_nodes(vec![node(0, false), node(100, different)])
            .with_bm25(
                Some(crate::analyzer::NATIVE_ANALYSIS_BACKEND.into()),
                Default::default(),
            )
    };
    let owner = cluster(false);
    let reader = owner.for_access(Some(&decision), "query").unwrap();
    let incompatible = cluster(true).for_access(Some(&decision), "query").unwrap();
    for strategy in [
        Strategy::Rrf(RrfScore::default()),
        Strategy::Rrf(RrfScore {
            dense_weight: Some(0.0),
            ..Default::default()
        }),
        Strategy::ScoreBlend(BlendScore::default()),
        Strategy::Decomposed(DecomposedScore::default()),
        Strategy::Cascade(CascadeScore {
            gate_id: "dense".into(),
        }),
    ] {
        let query = hybrid_query(strategy, "semantic");
        let expected = SearchService::query(&owner, Request::new(query.clone()))
            .await
            .unwrap()
            .into_inner();
        let actual = SearchService::query(&reader, Request::new(query.clone()))
            .await
            .unwrap()
            .into_inner();
        assert!(!actual.hits.is_empty());
        assert_eq!(actual.hits, expected.hits);
        let error = SearchService::query(&incompatible, Request::new(query.clone()))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
        assert!(error.message().contains("binding"), "{error}");
        assert_stream_refuses_binding(&incompatible, &query).await;
        let alias = with_dense_field(query, "signal");
        let error = SearchService::query(&reader, Request::new(alias))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
    }
}

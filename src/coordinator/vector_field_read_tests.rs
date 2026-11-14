//! Vector field authorization and pinned read-set checks.

use super::*;
use crate::pb::node_service_server::NodeService;
use crate::pb::*;
use prost::Message;

fn node(offset: u64, fingerprint_override: bool) -> Arc<crate::node::NodeServiceImpl> {
    node_rows(offset, fingerprint_override, 2, 2)
}
fn node_rows(
    offset: u64,
    fingerprint_override: bool,
    docs: u32,
    rows: usize,
) -> Arc<crate::node::NodeServiceImpl> {
    node_rows_with_private_terms(offset, fingerprint_override, docs, rows, 1)
}
fn node_rows_with_private_terms(
    offset: u64,
    fingerprint_override: bool,
    docs: u32,
    rows: usize,
    private_terms: u32,
) -> Arc<crate::node::NodeServiceImpl> {
    let plan = crate::mapping::derive_plan(
        include_bytes!("../../tests/fixtures/vector-binding/descriptor.bin"),
        "vector_binding.Named",
    )
    .unwrap();
    let mut binding = plan.vector_binding.unwrap();
    if fingerprint_override {
        binding.plan_fingerprint = "b".repeat(64);
    }
    let mut store = crate::postings::Bm25Store::new().with_facets(&["audience"]);
    store.set_binding(Some(crate::postings::StoredBinding {
        plan_fingerprint: binding.plan_fingerprint.clone(),
        body_path: "body".into(),
        vector_binding: binding.encode_to_vec(),
        ..Default::default()
    }));
    for row in 0..docs {
        let count = if row == 0 { 1 } else { private_terms };
        store.add_document(
            row,
            vec!["word"; count as usize].join(" "),
            crate::postings::AnalyzedDoc::body(
                vec![(
                    "word".into(),
                    count,
                    (0..count).map(|i| (i * 5, i * 5 + 4)).collect(),
                )],
                count,
            ),
        );
        store.set_facet(0, row, if row == 0 { "public" } else { "private" });
    }
    let vectors = vec![0.25; rows * 16];
    let config = crate::vector::embedded_turbovec_config(4, &[0.0; 16], &[1.0; 16]).unwrap();
    let mut index = crate::vector::VectorIndex::from_backend_config(16, &config).unwrap();
    index.add(&vectors, 16).unwrap();
    Arc::new(
        crate::node::NodeServiceImpl::new(
            Some(index),
            crate::node::NodeConfig {
                slot_offset: offset,
                facet_fields: vec!["audience".into()],
                ..Default::default()
            },
        )
        .with_bm25(Some(crate::node::Bm25Shard::Building(store)))
        .with_exact_vectors(Some(
            crate::exact_vectors::ExactVectorStore::from_values(16, vectors).unwrap(),
        ))
        .unwrap(),
    )
}
fn access(field: &str, action: FieldAction) -> AccessDecision {
    AccessDecision {
        action: AccessAction::Search as i32,
        document_visibility: Some(DocumentVisibility {
            filter: crate::cel::compile_filter("audience == 'public'").unwrap(),
        }),
        field_permissions: Some(FieldPermissions {
            grants: vec![FieldGrant {
                field: field.into(),
                actions: vec![action as i32],
            }],
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[tokio::test]
async fn auto_ann_qualifies_the_document_view_instead_of_the_unfiltered_point() {
    let node = node(0, false);
    let fingerprint = {
        let mut guard = crate::node::write_shard(&node.state);
        let index = guard.index.take().unwrap();
        let fingerprint = crate::harness::fake_ann::fingerprint_of(&index);
        guard.index = Some(crate::harness::fake_ann::fake_ann_index(index));
        fingerprint
    };
    let policy = crate::dense_policy::DenseExecutionPolicy::parse(&format!(
        r#"
format_version = 1
policy_id = "document-view"
embedding_model = "test"
corpus_generation = 0
corpus_rows = 2
dimensions = 16
provider_backend = "fake-ann"
scoring_fingerprint = "{fingerprint}"
measured_queries = 20
[[points]]
k = 1
filter_selectivity_ppm_min = 1000000
filter_selectivity_ppm_max = 1000000
candidates = 1
measured_recall_ppm = 990000
[[points]]
k = 1
filter_selectivity_ppm_min = 500000
filter_selectivity_ppm_max = 500000
candidates = 2
measured_recall_ppm = 980000
"#
    ))
    .unwrap();
    let owner =
        CoordinatorServiceImpl::with_local_nodes(vec![node]).with_dense_execution_policy(policy);
    let key = DenseRequestKey {
        k: 1,
        candidate_depth: 0,
        filters: None,
    };
    let unrestricted = owner
        .resolve_dense_execution(DenseExecutionMode::Auto, 16, key)
        .await
        .unwrap();
    assert_eq!(unrestricted.candidate_depth, 1);
    let reader = owner
        .for_access(Some(&access("semantic", FieldAction::Use)), "bm25_search")
        .unwrap()
        .for_vector_field("semantic")
        .unwrap();
    let restricted = reader
        .resolve_dense_execution(DenseExecutionMode::Auto, 16, key)
        .await
        .unwrap();
    assert_eq!(restricted.resolved_mode, DenseExecutionMode::Ann as i32);
    assert_eq!(restricted.filter_selectivity_ppm, 500_000);
    assert_eq!(restricted.candidate_depth, 2);
    assert_eq!(
        restricted.policy_point.unwrap().measured_recall_ppm,
        980_000
    );
    // An explicit depth from the unfiltered point cannot borrow its evidence.
    assert_eq!(
        reader
            .resolve_dense_execution(
                DenseExecutionMode::Auto,
                16,
                DenseRequestKey {
                    candidate_depth: 1,
                    ..key
                }
            )
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
}

#[tokio::test]
async fn dense_policy_selectivity_uses_authorized_vector_membership() {
    let nodes = vec![node_rows(0, false, 4, 2)];
    let owner = CoordinatorServiceImpl::with_local_nodes(nodes.clone());
    let reader = owner
        .for_access(Some(&access("semantic", FieldAction::Use)), "bm25_search")
        .unwrap()
        .for_vector_field("semantic")
        .unwrap();
    assert_eq!(
        reader.dense_filter_selectivity(None, 2).await.unwrap(),
        500_000
    );
    assert_eq!(
        reader
            .dense_filter_selectivity(Some(&RequestFilters::default()), 2)
            .await
            .unwrap(),
        500_000
    );
    let private = RequestFilters {
        tree: crate::cel::compile_filter("audience == 'private'").unwrap(),
        ..Default::default()
    };
    // Three matching documents, but only one owns an indexed vector.
    assert_eq!(
        owner
            .dense_filter_selectivity(Some(&private), 2)
            .await
            .unwrap(),
        500_000
    );
    assert_eq!(
        owner.dense_filter_selectivity(None, 2).await.unwrap(),
        1_000_000
    );
    assert_eq!(
        reader
            .dense_filter_selectivity(Some(&private), 2)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    let mut decision = access("semantic", FieldAction::Use);
    decision
        .field_permissions
        .as_mut()
        .unwrap()
        .grants
        .push(FieldGrant {
            field: "audience".into(),
            actions: vec![FieldAction::Use as i32],
        });
    let reader = owner
        .for_access(Some(&decision), "bm25_search")
        .unwrap()
        .for_vector_field("semantic")
        .unwrap();
    assert_eq!(
        reader
            .dense_filter_selectivity(Some(&private), 2)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        owner
            .for_vector_field("missing")
            .unwrap()
            .dense_filter_selectivity(Some(&private), 2)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    let (pinned, _) = reader.pin_read_versions().await.unwrap();
    nodes[0]
        .delete_documents(Request::new(DeleteDocumentsRequest {
            doc_ids: vec![0],
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(
        pinned
            .dense_filter_selectivity(None, 2)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(reader.dense_filter_selectivity(None, 2).await.unwrap(), 0);
}

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

fn hybrid_query(strategy: selection_score_strategy::Strategy, field: &str) -> QueryRequest {
    let operator = if matches!(strategy, selection_score_strategy::Strategy::Cascade(_)) {
        SelectionOperator::Unspecified
    } else {
        SelectionOperator::Or
    };
    QueryRequest {
        k: 4,
        selection: Some(SelectionQuery {
            node: Some(selection_query::Node::Composite(CompositeSearchStrategy {
                operator: operator as i32,
                clauses: vec![
                    SelectionQuery {
                        node: Some(selection_query::Node::Search(SearchQuery {
                            id: "dense".into(),
                            query: Some(search_query::Query::Dense(DenseQuery {
                                field: field.into(),
                                vector: vec![0.25; 16],
                                ..Default::default()
                            })),
                        })),
                    },
                    SelectionQuery {
                        node: Some(selection_query::Node::Search(SearchQuery {
                            id: "lexical".into(),
                            query: Some(search_query::Query::Lexical(LexicalQuery {
                                text: "word".into(),
                                analysis: Some(crate::analyzer::body_spec()),
                                ..Default::default()
                            })),
                        })),
                    },
                ],
                scoring: Some(SelectionScoreStrategy {
                    strategy: Some(strategy),
                }),
            })),
        }),
        ..Default::default()
    }
}

#[tokio::test]
async fn document_query_executor_redacts_physical_details_across_selection_shapes() {
    use selection_score_strategy::Strategy;
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
    let owner = CoordinatorServiceImpl::with_local_nodes(vec![node(0, false), node(100, false)])
        .with_bm25(
            Some(crate::analyzer::NATIVE_ANALYSIS_BACKEND.into()),
            Default::default(),
        );
    let reader = owner.for_access(Some(&decision), "query").unwrap();
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
    let mut saw_physical_work = false;
    for mut request in requests {
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
        assert!(reply.execution_details_redacted);
        let profile = reply.profile.as_ref().unwrap();
        saw_physical_work |= raw.profile.as_ref().unwrap().rerank_rows > 0;
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
        if let Some(execution) = reply.dense_execution {
            assert!(execution.planner_reason.is_empty());
            assert!(execution.exhaustive_completion);
            assert_eq!(
                execution.evidence_scope,
                DenseEvidenceScope::NotApplicable as i32
            );
        }
        use tokio_stream::StreamExt;
        let mut stream_request = Request::new(QueryStreamRequest {
            query: Some(request.clone()),
            ..Default::default()
        });
        stream_request.extensions_mut().insert(decision.clone());
        let mut stream = SearchService::query_stream(&reader, stream_request)
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
                    provisional |= !revision.hits.is_empty()
                        && revision.phase != QueryStreamPhase::Final as i32;
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
        let unrestricted = owner.execute_query(request, None, 0).await.unwrap();
        assert!(!unrestricted.execution_details_redacted);
    }
    assert!(
        saw_physical_work,
        "the test must exercise actual FP32 row reads"
    );
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
        use tokio_stream::StreamExt;
        let mut stream = SearchService::query_stream(
            &incompatible,
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
        dense.field = "signal".into();
        let error = SearchService::query(&reader, Request::new(alias))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
    }
}

#[tokio::test]
async fn fused_legs_admit_empty_shards_and_apply_the_document_view_to_both_lists() {
    let mut decision = access("semantic", FieldAction::Use);
    decision
        .field_permissions
        .as_mut()
        .unwrap()
        .grants
        .push(FieldGrant {
            field: "body".into(),
            actions: vec![FieldAction::Use as i32],
        });
    for mode in [FusionMode::GlobalRank, FusionMode::TwoLevel] {
        for empty_incompatible in [false, true] {
            let nodes = vec![node(0, false), node_rows(100, empty_incompatible, 0, 0)];
            let reader = CoordinatorServiceImpl::with_local_nodes(nodes)
                .with_bm25(
                    Some(crate::analyzer::NATIVE_ANALYSIS_BACKEND.into()),
                    Default::default(),
                )
                .for_access(Some(&decision), "bm25_search")
                .unwrap()
                .for_vector_field("semantic")
                .unwrap();
            let result = reader
                .fanout_hybrid(
                    "scoped-legs",
                    "word",
                    &[0.25; 16],
                    4,
                    Some(&crate::analyzer::body_spec()),
                    HybridLegs {
                        leg_k: 4,
                        vector_weight: 1.0,
                        bm25_weight: 1.0,
                        rrf_k: 60.0,
                        fusion_mode: mode,
                        normalization: fusion::Normalization::MinMax,
                        combination: fusion::Combination::Arithmetic,
                        min_vector_score: 0.0,
                    },
                    false,
                    &RequestFilters::default(),
                )
                .await;
            if empty_incompatible {
                let error = result.unwrap_err();
                assert_eq!(error.code(), tonic::Code::FailedPrecondition);
                assert!(error.message().contains("binding"), "{error}");
            } else {
                let hits = result.unwrap().0;
                assert_eq!(
                    hits.iter().map(|hit| hit.doc_id).collect::<Vec<_>>(),
                    vec![0]
                );
                assert!(hits[0].vector_rank.is_some());
                assert!(hits[0].bm25_rank.is_some());
            }
        }
    }
}

#[tokio::test]
async fn decomposed_lexical_admission_matches_a_physically_restricted_corpus() {
    let reference = CoordinatorServiceImpl::with_local_nodes(vec![
        node_rows(0, false, 1, 1),
        node_rows(100, false, 1, 1),
    ])
    .with_bm25(
        Some(crate::analyzer::NATIVE_ANALYSIS_BACKEND.into()),
        Default::default(),
    )
    .for_vector_field("semantic")
    .unwrap();
    let mut decision = access("semantic", FieldAction::Use);
    decision
        .field_permissions
        .as_mut()
        .unwrap()
        .grants
        .push(FieldGrant {
            field: "body".into(),
            actions: vec![FieldAction::Use as i32],
        });
    let scoped = CoordinatorServiceImpl::with_local_nodes(vec![
        node_rows_with_private_terms(0, false, 2, 2, 100),
        node_rows_with_private_terms(100, false, 2, 2, 100),
    ])
    .with_bm25(
        Some(crate::analyzer::NATIVE_ANALYSIS_BACKEND.into()),
        Default::default(),
    )
    .for_access(Some(&decision), "bm25_search")
    .unwrap()
    .for_vector_field("semantic")
    .unwrap();
    let legs = HybridLegs {
        leg_k: 1,
        vector_weight: 1.0,
        bm25_weight: 1.0,
        rrf_k: 60.0,
        fusion_mode: FusionMode::Decomposed,
        normalization: fusion::Normalization::MinMax,
        combination: fusion::Combination::Arithmetic,
        min_vector_score: 0.0,
    };
    let expected = reference
        .fanout_hybrid(
            "restricted",
            "word",
            &[0.25; 16],
            1,
            Some(&crate::analyzer::body_spec()),
            legs,
            false,
            &RequestFilters::default(),
        )
        .await
        .unwrap()
        .0;
    let actual = scoped
        .fanout_hybrid(
            "scoped",
            "word",
            &[0.25; 16],
            1,
            Some(&crate::analyzer::body_spec()),
            legs,
            false,
            &RequestFilters::default(),
        )
        .await
        .unwrap()
        .0;
    assert_eq!(expected[0].bm25_rank, Some(1));
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn lexical_candidate_round_enforces_views_fields_and_empty_read_versions() {
    let nodes = vec![node(0, false), node(100, false)];
    let owner = CoordinatorServiceImpl::with_local_nodes(nodes.clone());
    let reader = owner
        .for_access(Some(&access("body", FieldAction::Use)), "bm25_search")
        .unwrap();
    let terms = vec!["word".into()];
    let (global, claims) = reader.body_stats(&terms, false).await.unwrap();
    let (scores, debug) = reader
        .bm25_rescore_round(
            &terms,
            0,
            &global,
            &claims,
            &HashMap::from([(0, vec![0, 1, 0]), (1, vec![100, 101])]),
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        scores
            .keys()
            .copied()
            .collect::<std::collections::BTreeSet<_>>(),
        [0, 100].into_iter().collect()
    );
    assert_eq!(debug.len(), 2);
    assert!(debug.values().all(|(_, hits)| *hits == 1));
    let (scores, debug) = reader
        .bm25_rescore_round(&terms, 0, &global, &claims, &HashMap::new(), &[])
        .await
        .unwrap();
    assert!(scores.is_empty());
    assert_eq!(
        debug.len(),
        2,
        "empty owners must acknowledge the authority view"
    );
    let denied = owner
        .for_access(Some(&access("body", FieldAction::Disclose)), "bm25_search")
        .unwrap();
    assert_eq!(
        denied
            .bm25_rescore_round(&terms, 0, &global, &claims, &HashMap::new(), &[])
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    assert_eq!(
        reader
            .bm25_rescore_round(
                &terms,
                0,
                &global,
                &claims,
                &HashMap::new(),
                &[ScoreStage {
                    column: "audience".into(),
                    operation: Some(crate::pb::score_stage::Operation::Op(
                        ScoreOp::AddLinear as i32
                    )),
                    ..Default::default()
                },]
            )
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    nodes[0]
        .delete_documents(Request::new(DeleteDocumentsRequest {
            doc_ids: vec![0],
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(
        reader
            .bm25_rescore_round(&terms, 0, &global, &claims, &HashMap::new(), &[])
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
}

#[tokio::test]
async fn vector_candidate_reads_enforce_actual_fields_views_and_versions() {
    let nodes = vec![node(0, false), node(100, false)];
    let owner = CoordinatorServiceImpl::with_local_nodes(nodes.clone());
    let reader = owner
        .for_access(Some(&access("semantic", FieldAction::Use)), "bm25_search")
        .unwrap();
    let vector = vec![0.25; 16];
    let members = reader.vector_membership("semantic").await.unwrap();
    assert_eq!(members.ids, [0, 100].into_iter().collect());
    let scores = reader
        .dense_signal(&vector, &[0, 1, 100, 101], "semantic")
        .await
        .unwrap();
    assert_eq!(
        scores
            .keys()
            .copied()
            .collect::<std::collections::BTreeSet<_>>(),
        members.ids
    );
    let reference = owner
        .dense_signal(&vector, &[0, 100], "semantic")
        .await
        .unwrap();
    assert_eq!(scores, reference);
    let exact = reader
        .exact_vector_scores(&vector, &[0, 100], "semantic")
        .await
        .unwrap();
    assert_eq!(exact.scores.len(), 2);
    assert_eq!(exact.logical_bytes, 128);
    assert!(reader
        .dense_signal(&vector, &[], "semantic")
        .await
        .unwrap()
        .is_empty());
    assert!(reader
        .exact_vector_scores(&vector, &[], "semantic")
        .await
        .unwrap()
        .scores
        .is_empty());
    for decision in [
        access("body", FieldAction::Use),
        access("semantic", FieldAction::Disclose),
    ] {
        let denied = owner.for_access(Some(&decision), "bm25_search").unwrap();
        assert_eq!(
            denied
                .vector_membership("semantic")
                .await
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
        assert_eq!(
            denied
                .dense_signal(&[], &[], "semantic")
                .await
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
        assert_eq!(
            denied
                .exact_vector_scores(&[], &[], "semantic")
                .await
                .err()
                .unwrap()
                .code(),
            tonic::Code::PermissionDenied
        );
    }
    let alias = owner
        .for_access(Some(&access("signal", FieldAction::Use)), "bm25_search")
        .unwrap();
    assert_eq!(
        alias
            .dense_signal(&vector, &[], "signal")
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(
        reader
            .dense_signal(&vector, &[], "")
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    let (pinned, _) = reader.pin_read_versions().await.unwrap();
    nodes[0]
        .delete_documents(Request::new(DeleteDocumentsRequest {
            doc_ids: vec![0],
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(
        pinned
            .dense_signal(&vector, &[], "semantic")
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(
        pinned
            .exact_vector_scores(&vector, &[], "semantic")
            .await
            .err()
            .unwrap()
            .code(),
        tonic::Code::FailedPrecondition
    );
    let decision = access("semantic", FieldAction::Use);
    for route in ["query", "query_stream"] {
        let admitted = owner.for_access(Some(&decision), route).unwrap();
        assert_eq!(admitted.document_visibility, decision.document_visibility);
        assert!(admitted
            .field_permissions
            .as_ref()
            .unwrap()
            .vector("signal")
            .is_err());
    }
}

#[tokio::test]
async fn empty_vector_reads_refuse_mixed_generations_and_missing_binding_receipts() {
    let owner = CoordinatorServiceImpl::with_local_nodes(vec![node(0, false), node(100, true)]);
    let vector = vec![0.25; 16];
    assert_eq!(
        owner
            .vector_membership("semantic")
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(
        owner
            .dense_signal(&vector, &[], "semantic")
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(
        owner
            .exact_vector_scores(&vector, &[], "semantic")
            .await
            .err()
            .unwrap()
            .code(),
        tonic::Code::FailedPrecondition
    );
    let binding = crate::mapping::derive_plan(
        include_bytes!("../../tests/fixtures/vector-binding/descriptor.bin"),
        "vector_binding.Named",
    )
    .unwrap()
    .vector_binding
    .unwrap();
    assert!(CoordinatorServiceImpl::check_vector_binding("semantic", None, &mut None).is_err());
    let mut bad = binding.clone();
    bad.field = "signal".into();
    assert!(
        CoordinatorServiceImpl::check_vector_binding("semantic", Some(&bad), &mut None).is_err()
    );
    let response = VectorRescoreResponse {
        vector_binding: Some(binding),
        ..Default::default()
    };
    assert!(owner
        .check_read_view(
            0,
            &crate::visibility::VisibilityScope::default(),
            &response,
            &mut []
        )
        .is_err());
    let unbound = CoordinatorServiceImpl::with_local_nodes(vec![Arc::new(
        crate::node::NodeServiceImpl::new(None, Default::default()),
    )]);
    assert!(unbound
        .dense_signal(&vector, &[], "semantic")
        .await
        .is_err());
    assert!(unbound
        .exact_vector_scores(&vector, &[], "semantic")
        .await
        .is_err());
}

#[tokio::test]
async fn every_scoped_scan_mode_uses_the_same_authorized_read_set() {
    let owner = CoordinatorServiceImpl::with_local_nodes(vec![node(0, false), node(100, false)]);
    let mut decision = access("semantic", FieldAction::Use);
    decision
        .field_permissions
        .as_mut()
        .unwrap()
        .grants
        .push(FieldGrant {
            field: "parent_id".into(),
            actions: vec![FieldAction::Use as i32, FieldAction::Disclose as i32],
        });
    let reader = owner
        .for_access(Some(&decision), "bm25_search")
        .unwrap()
        .for_vector_field("semantic")
        .unwrap();
    let vector = vec![0.25; 16];
    let filters = RequestFilters::compile(&[], "").unwrap();
    let classic = reader
        .fanout_search("scoped-classic", &vector, 4, false, &filters)
        .await
        .unwrap();
    let streaming = reader
        .fanout_stream_search("scoped-stream", &vector, 4, None, &filters)
        .await
        .unwrap();
    let collapsed = reader
        .fanout_search_collapse("scoped-collapse", &vector, 4, &filters)
        .await
        .unwrap();
    let stream_collapsed = reader
        .fanout_stream_search_collapse("scoped-stream-collapse", &vector, 4, &filters)
        .await
        .unwrap();
    let reference = owner
        .dense_signal(&vector, &[0, 100], "semantic")
        .await
        .unwrap();
    for hits in [
        &classic.hits,
        &streaming.hits,
        &collapsed.hits,
        &stream_collapsed.hits,
    ] {
        assert_eq!(hits.len(), 2);
        for hit in hits {
            assert_eq!(hit.score.to_bits(), reference[&hit.vector_id].to_bits());
        }
    }
    let forbidden = RequestFilters::compile(&[], "audience == 'private'").unwrap();
    assert_eq!(
        reader
            .fanout_search("denied-filter", &vector, 4, false, &forbidden)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    let no_parent = owner
        .for_access(Some(&access("semantic", FieldAction::Use)), "bm25_search")
        .unwrap()
        .for_vector_field("semantic")
        .unwrap();
    assert_eq!(
        no_parent
            .fanout_search_collapse("denied-parent", &vector, 4, &filters)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
}

#[tokio::test]
async fn incompatible_scan_receipts_refuse_before_provisional_disclosure() {
    let (progress, observed) = watch::channel(None);
    let reader = CoordinatorServiceImpl::with_local_nodes(vec![node(0, false), node(100, true)])
        .for_vector_field("semantic")
        .unwrap()
        .with_query_progress(progress);
    let filters = RequestFilters::compile(&[], "").unwrap();
    let error = reader
        .fanout_stream_search("mixed-bindings", &[0.25; 16], 4, None, &filters)
        .await
        .err()
        .unwrap();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(observed.borrow().is_none());
    let nodes = vec![node(0, false), node(100, false)];
    let reader = CoordinatorServiceImpl::with_local_nodes(nodes.clone())
        .for_vector_field("semantic")
        .unwrap();
    let (pinned, _) = reader.pin_read_versions().await.unwrap();
    nodes[0]
        .delete_documents(Request::new(DeleteDocumentsRequest {
            doc_ids: vec![0],
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(
        pinned
            .fanout_search("stale-read", &[0.25; 16], 4, false, &filters)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
}

#[tokio::test]
async fn pushed_boolean_dense_domains_preserve_optional_negative_and_vector_only_members() {
    use crate::pb::search_service_server::SearchService;
    let coordinator = CoordinatorServiceImpl::with_local_nodes(vec![
        node_rows(0, false, 2, 1),
        node_rows(100, false, 1, 2),
    ]);
    let docs = || SelectionQuery {
        node: Some(selection_query::Node::Filter(FilterQuery {
            id: "docs".into(),
            predicate: Some(filter_query::Predicate::Cel("audience != 'missing'".into())),
        })),
    };
    for exact in [false, true] {
        let dense = || SelectionQuery {
            node: Some(selection_query::Node::Search(SearchQuery {
                id: "dense".into(),
                query: Some(search_query::Query::Dense(DenseQuery {
                    field: "semantic".into(),
                    vector: vec![0.25; 16],
                    score_mode: if exact {
                        DenseScoreMode::Fp32Rerank as i32
                    } else {
                        DenseScoreMode::Native as i32
                    },
                    ..Default::default()
                })),
            })),
        };
        for (must, should, must_not, want) in [
            (vec![dense()], vec![], vec![], vec![0, 100, 101]),
            (vec![docs()], vec![dense()], vec![], vec![0, 1, 100]),
            (vec![], vec![docs(), dense()], vec![], vec![0, 1, 100, 101]),
            (vec![docs()], vec![], vec![dense()], vec![1]),
            (vec![], vec![], vec![dense()], vec![1]),
            (vec![dense()], vec![], vec![docs()], vec![101]),
        ] {
            let response = coordinator
                .query(Request::new(QueryRequest {
                    k: 10,
                    selection: Some(SelectionQuery {
                        node: Some(selection_query::Node::Boolean(BooleanQuery {
                            must,
                            should,
                            must_not,
                            ..Default::default()
                        })),
                    }),
                    ..Default::default()
                }))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(
                response
                    .hits
                    .iter()
                    .map(|hit| hit.doc_id)
                    .collect::<std::collections::BTreeSet<_>>(),
                want.into_iter().collect(),
                "exact={exact}"
            );
            for hit in response.hits.iter().filter(|hit| hit.doc_id == 1) {
                assert!(!hit.matched.iter().any(|id| id == "dense"));
                assert!(!hit.signals.iter().any(|signal| signal.id == "dense"));
                assert_eq!(hit.score, 0.0);
            }
        }
    }
}

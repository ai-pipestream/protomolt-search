use super::*;
use pipestream_search::{
    authorization::PolicyAuthority,
    collections::CollectionSet,
    coordinator::CoordinatorServiceImpl,
    pb::search_service_server::SearchService,
    security::{PrincipalConfig, Principals},
};
use std::collections::{BTreeMap, BTreeSet};
use tokio_stream::StreamExt;

fn service(node: Arc<NodeServiceImpl>, disclose: bool, visible: bool) -> CollectionSet {
    let authority = PolicyAuthority::new(AccessPolicy {
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
            field_permissions: Some(FieldPermissions {
                grants: ["body", "embedding", "chunk_id"]
                    .into_iter()
                    .map(|field| FieldGrant {
                        field: field.into(),
                        actions: vec![FieldAction::Use as i32, FieldAction::Disclose as i32],
                    })
                    .collect(),
                disclose_document_identity: disclose,
            }),
            document_visibility: Some(DocumentVisibility {
                filter: pipestream_search::cel::compile_filter(if visible {
                    "chunk_id != 1u"
                } else {
                    "chunk_id == 99u"
                })
                .unwrap(),
            }),
        }],
    })
    .unwrap();
    let coordinator = CoordinatorServiceImpl::with_local_nodes(vec![node])
        .with_bm25(Some(NATIVE_ANALYSIS_BACKEND.into()), Default::default());
    CollectionSet::single(coordinator).with_principals(Arc::new(
        Principals::from_configs(&[PrincipalConfig {
            name: "reader".into(),
            token: "reader-token-0123456789012345".into(),
            ..Default::default()
        }])
        .unwrap()
        .with_authorizer(Arc::new(authority)),
    ))
}
fn authorized<T>(payload: T) -> Request<T> {
    let mut request = Request::new(payload);
    request.metadata_mut().insert(
        "authorization",
        "Bearer reader-token-0123456789012345".parse().unwrap(),
    );
    request
}
fn lexical() -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Search(SearchQuery {
            id: "lex".into(),
            query: Some(search_query::Query::Lexical(LexicalQuery {
                text: "accepted".into(),
                analysis: Some(body_spec()),
                ..Default::default()
            })),
        })),
    }
}
fn dense(mode: DenseScoreMode) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Search(SearchQuery {
            id: "vec".into(),
            query: Some(search_query::Query::Dense(DenseQuery {
                field: "embedding".into(),
                vector: vec![0.25; 8],
                score_mode: mode as i32,
                ..Default::default()
            })),
        })),
    }
}
fn requests() -> Vec<QueryRequest> {
    let mut selections = vec![(
        SelectionQuery {
            node: Some(selection_query::Node::Filter(FilterQuery {
                id: "all".into(),
                predicate: Some(filter_query::Predicate::Cel("chunk_id >= 0u".into())),
            })),
        },
        false,
    )];
    for leaf in [lexical(), dense(DenseScoreMode::Unspecified)] {
        selections.push((
            SelectionQuery {
                node: Some(selection_query::Node::Boolean(BooleanQuery {
                    must: vec![leaf],
                    ..Default::default()
                })),
            },
            false,
        ));
    }
    selections.extend([
        (lexical(), true),
        (dense(DenseScoreMode::Unspecified), true),
        (dense(DenseScoreMode::Fp32Rerank), true),
    ]);
    for strategy in [
        selection_score_strategy::Strategy::Rrf(RrfScore::default()),
        selection_score_strategy::Strategy::ScoreBlend(BlendScore::default()),
        selection_score_strategy::Strategy::Decomposed(DecomposedScore::default()),
        selection_score_strategy::Strategy::Cascade(CascadeScore {
            gate_id: "vec".into(),
        }),
    ] {
        let cascade = matches!(strategy, selection_score_strategy::Strategy::Cascade(_));
        selections.push((
            SelectionQuery {
                node: Some(selection_query::Node::Composite(CompositeSearchStrategy {
                    operator: if cascade {
                        SelectionOperator::Unspecified
                    } else {
                        SelectionOperator::Or
                    } as i32,
                    clauses: vec![dense(DenseScoreMode::Unspecified), lexical()],
                    scoring: Some(SelectionScoreStrategy {
                        strategy: Some(strategy),
                    }),
                })),
            },
            true,
        ));
    }
    let mut requests = Vec::new();
    for (selection, collapse) in selections {
        requests.push(QueryRequest {
            k: 10,
            selection: Some(selection.clone()),
            ..Default::default()
        });
        if collapse {
            requests.push(QueryRequest {
                k: 3,
                selection_k: 10,
                selection: Some(selection),
                collapse: Some(CollapseSpec {
                    column: "chunk_id".into(),
                    inner_hits: 4,
                    map: None,
                }),
                ..Default::default()
            });
        }
    }
    for selection in [requests[0].selection.clone(), Some(lexical())] {
        requests.push(QueryRequest {
            k: 10,
            selection,
            sort: vec![QuerySort {
                column: "chunk_id".into(),
                descending: true,
                map: None,
            }],
            ..Default::default()
        });
    }
    requests
}
fn physical_identities(f: &Fixture) -> BTreeMap<u64, DocumentIdentity> {
    let set = rewrite::opened(f);
    let mut identities = BTreeMap::new();
    for i in 0..set.len() {
        for row in 0..set.metadata(i).rows {
            if !set.live_docs(i).is_deleted(row as usize) {
                identities.insert(
                    set.metadata(i).base_label + row,
                    set.bm25(i).document_identity(row as u32).unwrap(),
                );
            }
        }
    }
    identities
}
fn assert_hit(
    id: u64,
    identity: &Option<DocumentIdentity>,
    physical: &BTreeMap<u64, DocumentIdentity>,
    disclose: bool,
) -> Vec<u8> {
    let expected = &physical[&id];
    assert_ne!(
        expected.chunk_ordinal,
        Some(1),
        "document grant excluded this chunk"
    );
    assert_eq!(identity.as_ref(), disclose.then_some(expected));
    expected.encode_to_vec()
}
fn check_response(
    response: &QueryResponse,
    physical: &BTreeMap<u64, DocumentIdentity>,
    disclose: bool,
    visible: bool,
) -> BTreeMap<Vec<u8>, u32> {
    let mut scores = BTreeMap::new();
    for hit in response
        .hits
        .iter()
        .chain(response.groups.iter().flat_map(|group| &group.hits))
    {
        assert!(visible, "a denied document view returned hits");
        scores.insert(
            assert_hit(hit.doc_id, &hit.identity, physical, disclose),
            hit.score.to_bits(),
        );
    }
    let expected: BTreeSet<_> = if visible {
        [(&b"a"[..], 2), (&b"b"[..], 1)]
            .into_iter()
            .flat_map(|(key, version)| {
                [0, 2].map(|ordinal| {
                    DocumentIdentity {
                        document_key: key.to_vec(),
                        version,
                        chunk_ordinal: Some(ordinal),
                    }
                    .encode_to_vec()
                })
            })
            .collect()
    } else {
        BTreeSet::new()
    };
    assert_eq!(scores.keys().cloned().collect::<BTreeSet<_>>(), expected);
    scores
}

#[tokio::test]
async fn supported_query_shapes_keep_authorized_identity_through_source_compaction() {
    let f = Fixture::new();
    let node = Arc::new(NodeServiceImpl::open(f.config.clone(), None, false).unwrap());
    rewrite::publish_key(&f, node.clone(), b"a", 1, Some(3)).await;
    rewrite::publish_key(&f, node.clone(), b"b", 1, Some(3)).await;
    rewrite::publish_key(&f, node.clone(), b"a", 2, Some(3)).await;
    let services: Vec<_> = [(true, true), (false, true), (true, false)]
        .into_iter()
        .map(|(disclose, visible)| (service(node.clone(), disclose, visible), disclose, visible))
        .collect();
    let requests = requests();
    let mut before_scores = BTreeMap::new();
    let before_physical = physical_identities(&f);
    assert_eq!(
        before_physical.keys().copied().collect::<Vec<_>>(),
        (3..9).collect::<Vec<_>>()
    );
    for phase in 0..2 {
        if phase == 1 {
            rewrite::compact(&f, node.clone(), rewrite::request(2))
                .await
                .unwrap();
            assert_ne!(physical_identities(&f), before_physical);
        }
        let physical = physical_identities(&f);
        if phase == 1 {
            assert_eq!(
                physical.keys().copied().collect::<Vec<_>>(),
                (0..6).collect::<Vec<_>>()
            );
        }
        let mut revised_hits = [0; 3];
        for (shape, request) in requests.iter().enumerate() {
            for (policy, (service, disclose, visible)) in services.iter().enumerate() {
                let response = service
                    .query(authorized(request.clone()))
                    .await
                    .unwrap_or_else(|e| {
                        panic!("phase {phase}, shape {shape}, policy {policy}: {e}")
                    })
                    .into_inner();
                let scores = check_response(&response, &physical, *disclose, *visible);
                if phase == 0 {
                    before_scores.insert((shape, policy), scores.clone());
                } else {
                    assert_eq!(
                        scores,
                        before_scores[&(shape, policy)],
                        "shape {shape}, policy {policy}"
                    );
                }
                let mut stream = service
                    .query_stream(authorized(QueryStreamRequest {
                        query: Some(request.clone()),
                        ..Default::default()
                    }))
                    .await
                    .unwrap()
                    .into_inner();
                let mut completed = false;
                while let Some(event) = stream.next().await {
                    assert!(!completed);
                    match event
                        .unwrap_or_else(|e| {
                            panic!("stream phase {phase}, shape {shape}, policy {policy}: {e}")
                        })
                        .payload
                        .unwrap()
                    {
                        query_stream_response::Payload::Revision(revision) => {
                            revised_hits[policy] += revision.hits.len();
                            for hit in revision.hits {
                                assert!(*visible);
                                assert_eq!(
                                    revision.identity_state,
                                    if *disclose {
                                        QueryStreamIdentityState::Resolved
                                    } else {
                                        QueryStreamIdentityState::Withheld
                                    } as i32
                                );
                                assert_hit(hit.doc_id, &hit.identity, &physical, *disclose);
                            }
                        }
                        query_stream_response::Payload::Completion(completion) => {
                            completed = true;
                            assert_eq!(
                                check_response(
                                    completion.response.as_ref().unwrap(),
                                    &physical,
                                    *disclose,
                                    *visible
                                ),
                                scores
                            );
                        }
                    }
                }
                assert!(completed);
            }
        }
        assert!(revised_hits[0] > 0 && revised_hits[1] > 0);
        assert_eq!(revised_hits[2], 0);
        // Compaction must not broaden the grant to another stored column.
        for (service, _, _) in &services {
            let denied = QueryRequest {
                k: 10,
                selection: Some(lexical()),
                sort: vec![QuerySort {
                    column: "value".into(),
                    ..Default::default()
                }],
                ..Default::default()
            };
            assert_eq!(
                service.query(authorized(denied)).await.unwrap_err().code(),
                Code::PermissionDenied
            );
        }
    }
}

//! Scoped scan modes, scan receipts and pushed boolean dense domains.

use super::*;

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

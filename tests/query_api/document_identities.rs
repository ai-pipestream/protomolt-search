//! Imported document identity preservation across dense, lexical, composite, and collapse.

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dense_results_preserve_identity_in_classic_and_streaming_scans() {
    let ((coordinator, vector, handles), _) = start_fleet_with_identities(true).await;
    let request = QueryRequest {
        k: N_DOCS as u32,
        selection: Some(dense_leaf("vec", &vector)),
        ..Default::default()
    };
    let mut expected = None;
    for stream in [false, true] {
        let coordinator = coordinator.clone().with_stream_search(stream);
        let response = query(&coordinator, request.clone()).await.unwrap();
        assert_eq!(response.hits.len(), N_DOCS);
        for hit in &response.hits {
            assert_eq!(hit.identity, fixture_identity(hit.doc_id));
        }
        let signature: Vec<_> = response
            .hits
            .iter()
            .map(|hit| (hit.doc_id, hit.score.to_bits(), hit.identity.clone()))
            .collect();
        if let Some(expected) = expected.as_ref() {
            assert_eq!(&signature, expected);
        }
        expected = Some(signature);
        let events = streamed_query(&coordinator, Some(request.clone()), 0).await;
        let (_, completion) = stream_parts(&events);
        let terminal = completion.response.as_ref().unwrap();
        assert_eq!(terminal.hits.len(), N_DOCS);
        for hit in &terminal.hits {
            assert_eq!(hit.identity, fixture_identity(hit.doc_id));
        }
    }
    for handle in handles {
        handle.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lexical_results_preserve_imported_identity_through_public_and_streamed_queries() {
    let ((coordinator, _, handles), addrs) = start_fleet_with_identities(true).await;
    let request = QueryRequest {
        explain: true,
        k: N_DOCS as u32,
        selection: Some(lexical_leaf("lex", "document")),
        ..Default::default()
    };
    for stream in [false, true] {
        let coordinator = coordinator.clone().with_bm25_stream(stream);
        for fused in [false, true] {
            let lexical = coordinator
                .bm25_search(Request::new(Bm25SearchRequest {
                    explain: true,
                    text: "document".into(),
                    k: N_DOCS as u32,
                    fields: if fused {
                        vec![pipestream_search::pb::QueryField {
                            field: "body".into(),
                            ..Default::default()
                        }]
                    } else {
                        Vec::new()
                    },
                    ..Default::default()
                }))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(lexical.hits.len(), N_DOCS);
            for hit in &lexical.hits {
                assert_eq!(hit.identity, fixture_identity(hit.doc_id));
                assert!(hit.explain.is_some());
            }
        }
        let response = query(&coordinator, request.clone()).await.unwrap();
        assert_eq!(response.hits.len(), N_DOCS);
        for hit in &response.hits {
            assert_eq!(hit.identity, fixture_identity(hit.doc_id));
            assert!(hit.explain.is_some());
        }
    }
    // Explain remains unary-only; the stream still carries terminal identity.
    let events = streamed_query(
        &coordinator,
        Some(QueryRequest {
            explain: false,
            ..request
        }),
        0,
    )
    .await;
    let (_, completion) = stream_parts(&events);
    let response = completion.response.as_ref().unwrap();
    assert_eq!(response.hits.len(), N_DOCS);
    for hit in &response.hits {
        assert_eq!(hit.identity, fixture_identity(hit.doc_id));
        assert!(hit.explain.is_none());
    }
    for addr in addrs {
        let rescored = NodeServiceClient::connect(addr)
            .await
            .unwrap()
            .bm25_rescore(pipestream_search::pb::Bm25RescoreRequest {
                terms: vec!["document".into()],
                global_doc_count: N_DOCS as u64,
                global_total_doc_length: (N_DOCS * 3) as u64,
                global_doc_frequencies: vec![N_DOCS as u32],
                candidate_ids: (0..N_DOCS as u64).collect(),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(rescored.hits.len(), SHARD_DOCS);
        for hit in rescored.hits {
            assert_eq!(hit.identity, fixture_identity(hit.doc_id));
        }
    }
    for handle in handles {
        handle.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_query_shape_and_collapse_carries_imported_identity() {
    let ((coordinator, vector, handles), _) = start_fleet_with_identities(true).await;
    let selections = vec![
        cel_filter("all", "year >= 0"),
        boolean(
            vec![lexical_leaf("lex", "document")],
            vec![],
            vec![],
            0,
            None,
        ),
        boolean(vec![dense_leaf("vec", &vector)], vec![], vec![], 0, None),
        composite(
            SelectionOperator::Or,
            vec![dense_leaf("vec", &vector), lexical_leaf("lex", "document")],
            Some(selection_score_strategy::Strategy::Rrf(RrfScore::default())),
        ),
        lexical_leaf("lex", "document"),
        dense_leaf("vec", &vector),
        composite(
            SelectionOperator::Or,
            vec![dense_leaf("vec", &vector), lexical_leaf("lex", "document")],
            Some(selection_score_strategy::Strategy::Decomposed(
                DecomposedScore::default(),
            )),
        ),
        composite(
            SelectionOperator::Unspecified,
            vec![dense_leaf("vec", &vector), lexical_leaf("lex", "document")],
            Some(selection_score_strategy::Strategy::Cascade(CascadeScore {
                gate_id: "vec".into(),
            })),
        ),
    ];
    for (shape, selection) in selections.into_iter().enumerate() {
        for collapse in [
            None,
            Some(CollapseSpec {
                map: None,
                column: "court".into(),
                inner_hits: 4,
            }),
        ] {
            if shape < 3 && collapse.is_some() {
                continue;
            }
            let request = QueryRequest {
                k: if collapse.is_some() { 3 } else { N_DOCS as u32 },
                selection_k: if collapse.is_some() { N_DOCS as u32 } else { 0 },
                selection: Some(selection.clone()),
                collapse,
                ..Default::default()
            };
            let response = query(&coordinator, request.clone()).await.unwrap();
            assert!(!response.hits.is_empty());
            for hit in response
                .hits
                .iter()
                .chain(response.groups.iter().flat_map(|g| &g.hits))
            {
                assert_eq!(
                    hit.identity,
                    fixture_identity(hit.doc_id),
                    "shape {shape}, row {}",
                    hit.doc_id
                );
            }
            let events = streamed_query(&coordinator, Some(request), 0).await;
            let (revisions, completion) = stream_parts(&events);
            for revision in revisions {
                assert_eq!(revision.content_fingerprint_version, 2);
                if revision.hits.is_empty() {
                    continue;
                }
                assert_eq!(
                    revision.identity_state,
                    pipestream_search::pb::QueryStreamIdentityState::Resolved as i32
                );
                for hit in &revision.hits {
                    assert_eq!(
                        hit.identity,
                        fixture_identity(hit.doc_id),
                        "revision shape {shape}, phase {}",
                        revision.phase
                    );
                }
            }
            let terminal = completion.response.as_ref().unwrap();
            for hit in terminal
                .hits
                .iter()
                .chain(terminal.groups.iter().flat_map(|g| &g.hits))
            {
                assert_eq!(
                    hit.identity,
                    fixture_identity(hit.doc_id),
                    "stream shape {shape}"
                );
            }
        }
    }
    for handle in handles {
        handle.abort();
    }
}

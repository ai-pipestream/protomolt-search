//! Stage-2 calibrated reranking and semantic highlighting over dense and
//! composite shortlists.

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stage2_rerank_and_highlight_reorders_dense_and_composite_and_composes_lineage_offsets() {
    let (mock_addr, mock_handle) = start_mock_analysis().await;

    // Build 4 vectors where Doc 0 has the closest vector to our query (`corpus_vecs[0]`),
    // while Doc 1 has a blended vector (`0.82 * v0 + 0.18 * v1`, normalized) so Doc 1 enters
    // the Stage-1 top-4 shortlist behind Doc 0, then wins Stage-2 cross-encoder reranking!
    let mut corpus_vecs = unit_vectors(4, DIM, 0x1050_CAFE);
    let mut blended = vec![0.0_f32; DIM];
    let mut sq = 0.0_f32;
    for d in 0..DIM {
        let v = 0.82 * corpus_vecs[d] + 0.18 * corpus_vecs[DIM + d];
        blended[d] = v;
        sq += v * v;
    }
    let inv = 1.0 / sq.sqrt();
    for (dst, v) in corpus_vecs[DIM..2 * DIM].iter_mut().zip(blended) {
        *dst = v * inv;
    }
    let (shift, scale) = fit_calibration(DIM, 4, &corpus_vecs);

    // Doc 1 carries `DocLineage { span_start: 1000 }` and two sentences; sentence 2 has exact
    // semantic overlap with `"constitutional appellate jurisdiction maritime appeals"`.
    let docs = [
        ("Industrial boiler valve pressure inspection log.", 0u32),
        (
            "Introductory docket header. The appellate court affirmed constitutional jurisdiction over maritime appeals.",
            1000u32,
        ),
        ("Municipal street lighting maintenance schedule.", 0u32),
        ("Agricultural grain elevator storage inventory.", 0u32),
    ];

    let mut nodes = Vec::new();
    for shard in 0..2usize {
        let node = Arc::new(NodeServiceImpl::new(
            None,
            NodeConfig {
                slot_offset: (shard * 2) as u64,
                analysis_addr: Some(TURBO_ANALYSIS_BACKEND.to_string()),
                sentence_fields: vec!["body".into()],
                ..Default::default()
            },
        ));
        let mut link = NodeLink::local(Arc::clone(&node));
        link.configure_vector_backend(pipestream_search::harness::embedded_backend_request(
            DIM, 4, &shift, &scale,
        ))
        .await
        .unwrap();

        let doc_reqs: Vec<AddDocumentsRequest> = (0..2usize)
            .map(|local| {
                let id = shard * 2 + local;
                let (text, span_start) = docs[id];
                AddDocumentsRequest {
                    text: text.to_string(),
                    analysis: Some(body_spec()),
                    lineage: Some(DocLineage {
                        parent_id: 77,
                        group_id: 1,
                        span_start,
                        span_end: span_start + text.len() as u32,
                    }),
                    ..Default::default()
                }
            })
            .collect();
        link.add_documents(tokio_stream::iter(doc_reqs))
            .await
            .unwrap();

        let start = shard * 2 * DIM;
        link.add_vectors(tokio_stream::iter(vec![AddVectorsRequest {
            materialized_rows: Vec::new(),
            vectors: corpus_vecs[start..start + 2 * DIM].to_vec(),
            dim: DIM as u32,
        }]))
        .await
        .unwrap();

        nodes.push(node);
    }

    let base_coord = CoordinatorServiceImpl::with_local_nodes(nodes)
        .with_bm25(Some(TURBO_ANALYSIS_BACKEND.to_string()), Default::default());
    let q_vec = corpus_vecs[..DIM].to_vec();

    // Baseline WITHOUT Stage-2 rerank: Doc 0 wins pure dense search (exact vector match) and has empty snippets!
    let baseline = SearchService::query(
        &base_coord,
        Request::new(QueryRequest {
            k: 4,
            selection: Some(SelectionQuery {
                node: Some(selection_query::Node::Search(SearchQuery {
                    id: "vec".into(),
                    query: Some(search_query::Query::Dense(DenseQuery {
                        vector: q_vec.clone(),
                        ..Default::default()
                    })),
                })),
            }),
            ..Default::default()
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(baseline.hits[0].doc_id, 0);
    assert!(baseline.hits[0].snippets.is_empty());

    // Test BOTH `DeterministicCalibrated` AND `Remote` (`turbo-kserve` HTTP/2 `RerankAndHighlight`)!
    let remote_client = RemoteClient::new(
        RemoteEndpointConfig::new(mock_addr).with_priority(RemotePriority::Interactive),
    );
    let backends = vec![
        (
            "deterministic",
            RerankBackend::DeterministicCalibrated {
                platt_slope: 2.2,
                platt_intercept: -0.4,
            },
        ),
        (
            "remote_kserve",
            RerankBackend::Remote {
                client: Box::new(remote_client),
                rerank_model: "bge-reranker-v2-m3".into(),
                embed_model: String::new(),
                qa_model: String::new(),
                temperature: 1.0,
            },
        ),
    ];

    for (label, backend) in backends {
        let mut spec = RerankSpec::new(backend);
        spec.shortlist_size = 4;
        spec.highlight_top_k = 4;
        spec.max_snippets_per_candidate = 2;
        spec.compose_parent_lineage_offsets = true;

        let rerank_coord = base_coord.clone().with_rerank_spec(spec);

        // 1. Pure `DenseQuery` (zero lexical postings matched!):
        //    - Doc 1 is promoted to Rank 0 above Doc 0!
        //    - Calibrated scores lie strictly in `[0.0, 1.0]`.
        //    - `QueryHit.snippets` is populated on pure dense hits with `DocLineage.span_start` (`1000`) composed!
        let dense_reranked = SearchService::query(
            &rerank_coord,
            Request::new(QueryRequest {
                k: 4,
                selection_k: 4,
                highlight: Some(HighlightSpec::default()),
                selection: Some(SelectionQuery {
                    node: Some(selection_query::Node::Search(SearchQuery {
                        id: "vec".into(),
                        query: Some(search_query::Query::Dense(DenseQuery {
                            vector: q_vec.clone(),
                            query_text: "constitutional appellate jurisdiction maritime appeals"
                                .into(),
                            ..Default::default()
                        })),
                    })),
                }),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_inner();

        assert!(
            dense_reranked.executed.contains("turbo_rerank"),
            "{label}: executed={}",
            dense_reranked.executed
        );
        assert_eq!(
            dense_reranked.hits[0].doc_id, 1,
            "{label}: expected Doc 1 promoted above Doc 0"
        );
        for hit in &dense_reranked.hits {
            assert!(
                (0.0..=1.0).contains(&hit.score),
                "{label}: uncalibrated score {}",
                hit.score
            );
        }
        let top_snips = &dense_reranked.hits[0].snippets;
        assert!(
            !top_snips.is_empty(),
            "{label}: pure dense hit must carry semantic snippets"
        );
        let snip = &top_snips[0];
        assert!(
            snip.start >= 1000 && snip.end > snip.start,
            "{label}: expected parent lineage offset >= 1000, got {}..{}",
            snip.start,
            snip.end
        );
        let local_start = (snip.start - 1000) as usize;
        let local_end = (snip.end - 1000) as usize;
        assert_eq!(&docs[1].0[local_start..local_end], snip.text.as_str());
        assert!(snip.text.contains("jurisdiction") || snip.text.contains("appellate"));
        assert!(!snip.highlights.is_empty());
        assert!(snip.highlights[0].start >= snip.start && snip.highlights[0].end <= snip.end);

        // 2. Hybrid `CompositeSearchStrategy` (`RrfScore` + `SelectionOperator::Or`):
        let hybrid_reranked = SearchService::query(
            &rerank_coord,
            Request::new(QueryRequest {
                k: 4,
                selection_k: 4,
                highlight: Some(HighlightSpec::default()),
                selection: Some(SelectionQuery {
                    node: Some(selection_query::Node::Composite(CompositeSearchStrategy {
                        operator: SelectionOperator::Or as i32,
                        clauses: vec![
                            SelectionQuery {
                                node: Some(selection_query::Node::Search(SearchQuery {
                                    id: "vec".into(),
                                    query: Some(search_query::Query::Dense(DenseQuery {
                                        vector: q_vec.clone(),
                                        query_text:
                                            "constitutional appellate jurisdiction maritime appeals"
                                                .into(),
                                        ..Default::default()
                                    })),
                                })),
                            },
                            SelectionQuery {
                                node: Some(selection_query::Node::Search(SearchQuery {
                                    id: "lex".into(),
                                    query: Some(search_query::Query::Lexical(LexicalQuery {
                                        text: "boiler inspection".into(),
                                        analysis: Some(body_spec()),
                                        ..Default::default()
                                    })),
                                })),
                            },
                        ],
                        scoring: Some(SelectionScoreStrategy {
                            strategy: Some(selection_score_strategy::Strategy::Rrf(
                                RrfScore::default(),
                            )),
                        }),
                    })),
                }),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_inner();

        assert!(hybrid_reranked.executed.contains("turbo_rerank"));
        assert_eq!(hybrid_reranked.hits[0].doc_id, 1);
        assert!(!hybrid_reranked.hits[0].snippets.is_empty());
    }

    mock_handle.abort();
}

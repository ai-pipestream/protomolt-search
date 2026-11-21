//! Single-leaf equivalence, explicit dense execution modes, and persisted FP32 reranking.

use super::*;

fn assert_exhaustive_native_receipt(execution: &DenseExecutionOutcome) {
    assert_eq!(
        DenseExecutionMode::try_from(execution.requested_mode).unwrap(),
        DenseExecutionMode::Exhaustive
    );
    assert_eq!(
        DenseExecutionMode::try_from(execution.resolved_mode).unwrap(),
        DenseExecutionMode::Exhaustive
    );
    assert_eq!(execution.provider_backend, "embedded-turbovec");
    assert_eq!(
        VectorQualityContract::try_from(execution.quality_contract).unwrap(),
        VectorQualityContract::ExhaustiveNativeScore
    );
    assert_eq!(
        execution.coverage,
        DenseCoverage::ExhaustiveCertified as i32
    );
    assert!(!execution.scoring_fingerprint.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_leaves_execute_their_ordinary_routes() {
    let (coordinator, qvec, _handles) = start_fleet().await;

    // Lexical leaf == Bm25Search, bitwise, with named provenance.
    let public = query(
        &coordinator,
        QueryRequest {
            k: 5,
            selection: Some(lexical_leaf("lex", "zebra")),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let direct = coordinator
        .bm25_search(Request::new(Bm25SearchRequest {
            text: "zebra".into(),
            k: 5,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(public.executed, "bm25_search");
    assert_eq!(public.hits.len(), direct.hits.len());
    for (p, d) in public.hits.iter().zip(&direct.hits) {
        assert_eq!(p.doc_id, d.doc_id);
        assert_eq!(p.score.to_bits(), d.score.to_bits());
        assert_eq!(p.signals.len(), 1);
        assert_eq!(p.signals[0].id, "lex");
        assert_eq!(p.matched, vec!["lex"]);
    }

    // Dense leaf == Search, bitwise.
    let public = query(
        &coordinator,
        QueryRequest {
            k: 5,
            selection: Some(dense_leaf("vec", &qvec)),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let direct = coordinator
        .search(Request::new(SearchRequest {
            k: 5,
            vector: qvec.clone(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(public.executed, "search");
    let got: Vec<(u64, u32)> = public
        .hits
        .iter()
        .map(|h| (h.doc_id, h.score.to_bits()))
        .collect();
    let want: Vec<(u64, u32)> = direct
        .hits
        .iter()
        .map(|h| (h.vector_id, h.score.to_bits()))
        .collect();
    assert_eq!(got, want);
    assert_eq!(public.hits[0].doc_id, 0, "the query IS doc 0's vector");
    let execution = public
        .dense_execution
        .expect("every dense result reports its execution contract");
    assert_exhaustive_native_receipt(&execution);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dense_execution_modes_are_explicit_and_fail_closed() {
    let (coordinator, qvec, _handles) = start_fleet().await;
    let run = |mode: DenseExecutionMode| QueryRequest {
        k: 5,
        selection: Some(SelectionQuery {
            node: Some(selection_query::Node::Search(SearchQuery {
                id: "vec".into(),
                query: Some(search_query::Query::Dense(DenseQuery {
                    vector: qvec.clone(),
                    execution_mode: mode as i32,
                    ..Default::default()
                })),
            })),
        }),
        ..Default::default()
    };

    let exact = query(&coordinator, run(DenseExecutionMode::Exhaustive))
        .await
        .unwrap();
    let auto = query(&coordinator, run(DenseExecutionMode::Auto))
        .await
        .unwrap();
    let bits = |r: &QueryResponse| {
        r.hits
            .iter()
            .map(|h| (h.doc_id, h.score.to_bits()))
            .collect::<Vec<_>>()
    };
    assert_eq!(bits(&exact), bits(&auto));
    for (response, requested) in [
        (exact, DenseExecutionMode::Exhaustive),
        (auto, DenseExecutionMode::Auto),
    ] {
        let outcome = response.dense_execution.unwrap();
        assert_eq!(outcome.requested_mode, requested as i32);
        assert_eq!(outcome.resolved_mode, DenseExecutionMode::Exhaustive as i32);
        assert_eq!(outcome.coverage, DenseCoverage::ExhaustiveCertified as i32);
        assert!(!outcome.planner_reason.is_empty());
    }

    let fast = query(&coordinator, run(DenseExecutionMode::Fast))
        .await
        .unwrap_err();
    assert_eq!(fast.code(), tonic::Code::FailedPrecondition);
    assert!(fast.message().contains("FAST") && fast.message().contains("never substituted"));

    let mut unknown = run(DenseExecutionMode::Exhaustive);
    let Some(selection_query::Node::Search(leaf)) =
        unknown.selection.as_mut().unwrap().node.as_mut()
    else {
        unreachable!()
    };
    let Some(search_query::Query::Dense(dense)) = leaf.query.as_mut() else {
        unreachable!()
    };
    dense.execution_mode = i32::MAX;
    let unknown = query(&coordinator, unknown).await.unwrap_err();
    assert_eq!(unknown.code(), tonic::Code::InvalidArgument);
    assert!(unknown.message().contains("execution_mode"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fp32_mode_reranks_a_persisted_mmap_candidate_pool() {
    const ROWS: usize = 64;
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("protomolt_query_exact_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let index_path = dir.join("shard.vector");
    let corpus = unit_vectors(ROWS, DIM, 0xE7AC_7001);
    let query_vector = unit_vectors(1, DIM, 0xE7AC_7002);
    let (shift, scale) = fit_calibration(DIM, 4, &corpus);
    let (addr, handle) = start_empty_node(NodeConfig {
        index_path: Some(index_path.clone()),
        ..Default::default()
    })
    .await;
    let mut node = NodeServiceClient::connect(addr.clone()).await.unwrap();
    node.configure_vector_backend(pipestream_search::harness::embedded_backend_request(
        DIM, 4, &shift, &scale,
    ))
    .await
    .unwrap();
    node.add_vectors(tokio_stream::iter(vec![AddVectorsRequest {
        materialized_rows: Vec::new(),
        vectors: corpus.clone(),
        dim: DIM as u32,
    }]))
    .await
    .unwrap();
    node.flush(FlushRequest {}).await.unwrap();
    let health = node.health(HealthRequest {}).await.unwrap().into_inner();
    assert!(health.exact_vectors_available);
    assert!(health.exact_vectors_mmap);
    assert_eq!(health.exact_vector_rows, ROWS as u64);
    let root = pipestream_search::node::segments_root(&index_path);
    let set = pipestream_search::segments::OpenedSegmentSet::open(root).unwrap();
    assert!(set.exact_vectors(0).is_some(), "sealed FP32 rows kept");
    let profile_path = dir.join("dense-quality.toml");
    std::fs::write(
        &profile_path,
        format!(
            r#"format_version = 1
profile_id = "held-out-64"
embedding_model = "test-unit-vectors"
corpus_generation = 0
corpus_rows = {ROWS}
dimensions = {DIM}
provider_backend = "{}"
scoring_fingerprint = "{}"
measured_queries = 32
[[points]]
k = 10
target_recall_ppm = 1000000
candidates = {ROWS}
"#,
            health.vector_backend, health.scoring_fingerprint
        ),
    )
    .unwrap();
    let quality_profile =
        pipestream_search::quality::DenseQualityProfile::load(&profile_path).unwrap();
    let coordinator = CoordinatorServiceImpl::new(vec![addr.clone()])
        .with_topology_generation(0)
        .with_dense_quality_profile(quality_profile);
    let request = QueryRequest {
        request_id: "fp32-public".into(),
        k: 10,
        selection_k: ROWS as u32,
        selection: Some(fp32_dense_leaf("vec", &query_vector)),
        profile: true,
        ..Default::default()
    };
    let response = query(&coordinator, request.clone()).await.unwrap();
    assert_eq!(response.executed, "search:fp32_pool");
    let physical = response.profile.as_ref().unwrap();
    assert!(physical.rerank_ms >= 0.0);
    assert_eq!(physical.rerank_rows, ROWS as u64);
    assert_eq!(physical.rerank_logical_bytes, (ROWS * DIM * 4) as u64);
    assert!(physical.rerank_pages > 0);
    assert!(physical.rerank_tasks > 0);

    let expected_bits = expected_fp32_bits(&corpus, &query_vector, 10);
    let actual: Vec<(u64, u32)> = response
        .hits
        .iter()
        .map(|hit| {
            assert_eq!(hit.signals[0].score.to_bits(), hit.score.to_bits());
            (hit.doc_id, hit.score.to_bits())
        })
        .collect();
    assert_eq!(actual, expected_bits);

    let measured_request = QueryRequest {
        request_id: "fp32-measured".into(),
        k: 10,
        selection: Some(SelectionQuery {
            node: Some(selection_query::Node::Search(SearchQuery {
                id: "vec".into(),
                query: Some(search_query::Query::Dense(DenseQuery {
                    vector: query_vector.clone(),
                    quality: Some(DenseQualityPolicy {
                        target_recall_ppm: 1_000_000,
                        max_candidates: ROWS as u32,
                        required_profile_fingerprint: String::new(),
                    }),
                    execution_mode: DenseExecutionMode::Balanced as i32,
                    precision: DensePrecisionRequest::Fp32 as i32,
                    ..Default::default()
                })),
            })),
        }),
        ..Default::default()
    };
    let measured = query(&coordinator, measured_request.clone()).await.unwrap();
    assert_eq!(
        measured
            .hits
            .iter()
            .map(|hit| (hit.doc_id, hit.score.to_bits()))
            .collect::<Vec<_>>(),
        expected_bits
    );
    let outcome = measured.dense_quality.expect("measured policy outcome");
    assert_eq!(outcome.target_recall_ppm, 1_000_000);
    assert_eq!(outcome.selection_k, ROWS as u32);
    assert_eq!(outcome.profile_id, "held-out-64");
    assert_eq!(outcome.corpus_rows, ROWS as u64);

    let mut conflicting = measured_request.clone();
    conflicting.selection_k = ROWS as u32;
    let err = query(&coordinator, conflicting).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err.message().contains("selection_k"));

    let stale_path = dir.join("dense-quality-stale.toml");
    let stale = std::fs::read_to_string(&profile_path)
        .unwrap()
        .replace("corpus_rows = 64", "corpus_rows = 65");
    std::fs::write(&stale_path, stale).unwrap();
    let stale_coordinator = CoordinatorServiceImpl::new(vec![addr.clone()])
        .with_topology_generation(0)
        .with_dense_quality_profile(
            pipestream_search::quality::DenseQualityProfile::load(&stale_path).unwrap(),
        );
    let err = query(&stale_coordinator, measured_request.clone())
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("row"), "{}", err.message());

    let byte_limited =
        CoordinatorServiceImpl::new(vec![addr]).with_max_rerank_bytes((ROWS * DIM * 4 - 1) as u64);
    let err = query(&byte_limited, request.clone()).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::ResourceExhausted);
    assert!(err.message().contains("bytes"), "{}", err.message());

    let mut stream_request = request;
    stream_request.profile = false;
    let stream_expected = query(&coordinator, stream_request.clone()).await.unwrap();
    let events = streamed_query(&coordinator, Some(stream_request), 0).await;
    let (revisions, completion) = stream_parts(&events);
    assert!(completion.completed, "{completion:?}");
    assert_eq!(
        completion.response.as_ref().unwrap().encode_to_vec(),
        stream_expected.encode_to_vec()
    );
    assert!(revisions.iter().any(|revision| {
        QueryStreamPhase::try_from(revision.phase).unwrap() == QueryStreamPhase::Dense
    }));
    assert_eq!(
        QueryStreamPhase::try_from(revisions.last().unwrap().phase).unwrap(),
        QueryStreamPhase::Final
    );

    node.delete_documents(DeleteDocumentsRequest {
        expected_wal_generation: None,
        doc_ids: vec![63],
    })
    .await
    .unwrap();
    let err = query(&coordinator, measured_request).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(
        err.message().contains("compact and remeasure"),
        "{}",
        err.message()
    );

    handle.abort();
    let _ = std::fs::remove_dir_all(dir);
}

fn expected_fp32_bits(corpus: &[f32], query_vector: &[f32], k: usize) -> Vec<(u64, u32)> {
    let mut expected: Vec<(u64, f32)> = corpus
        .as_chunks::<DIM>()
        .0
        .iter()
        .enumerate()
        .map(|(id, row)| {
            let score: f32 = row.iter().zip(query_vector).map(|(a, b)| a * b).sum();
            (id as u64, score)
        })
        .collect();
    expected.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    expected.truncate(k);
    expected
        .into_iter()
        .map(|(id, score)| (id, score.to_bits()))
        .collect()
}

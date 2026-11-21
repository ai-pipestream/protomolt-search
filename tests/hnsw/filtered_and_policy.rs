//! Filtered ANN regime reporting, boolean dense enumeration, and policy thresholds.

use super::*;

/// A filtered ANN query on shards larger than one
/// scan chunk answers from one search of each shard's whole graph, on the
/// unary route and the streaming one alike. A mask of half the rows is
/// walked filter-aware and holds its recall against the exact filtered top-k; a
/// mask of twenty rows a shard is enumerated, which is exact. Every hit
/// passes the filter and carries its exact FP32 score, and the response says
/// which regime each shard answered in.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_filtered_ann_query_reports_its_regime_and_keeps_recall() {
    const K: usize = 20;
    let (fixture, addrs) = start_filtered_fixture().await;
    let queries = unit_vectors(8, DIM, 0x4A5E_0056);
    let truth = |query: &[f32], court: &str| -> Vec<u64> {
        let mut all: Vec<(u64, f32)> = fixture
            .corpus
            .as_chunks::<DIM>()
            .0
            .iter()
            .enumerate()
            .filter(|(id, _)| court_of(*id) == court)
            .map(|(id, row)| (id as u64, pipestream_search::exact_vectors::dot(row, query)))
            .collect();
        all.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
        all.truncate(K);
        all.into_iter().map(|hit| hit.0).collect()
    };
    for streaming in [false, true] {
        let coordinator = CoordinatorServiceImpl::new(addrs.clone())
            .with_bm25(
                Some(NATIVE_ANALYSIS_BACKEND.to_string()),
                Default::default(),
            )
            .with_stream_search(streaming);
        for (court, regimes, floor) in [
            (
                "wide",
                // Half the rows allowed: the far probe routes it plain.
                DenseFilterRegimes {
                    plain: SHARDS as u32,
                    ..Default::default()
                },
                // A beam of 20 over uniform random vectors: measured 0.88 to
                // 0.94 across builds; a walk that lost the mask or the
                // graph falls far below this.
                0.75,
            ),
            (
                "narrow",
                DenseFilterRegimes {
                    enumerate: SHARDS as u32,
                    ..Default::default()
                },
                1.0,
            ),
        ] {
            let mut found = 0;
            for query_vector in queries.as_chunks::<DIM>().0 {
                let response = query(
                    &coordinator,
                    filtered_dense_request(query_vector, K as u32, court, DenseExecutionMode::Ann),
                )
                .await
                .unwrap();
                assert_eq!(response.hits.len(), K, "streaming={streaming} {court}");
                let expected = truth(query_vector, court);
                for hit in &response.hits {
                    assert_eq!(court_of(hit.doc_id as usize), court);
                    let row = &fixture.corpus[hit.doc_id as usize * DIM..][..DIM];
                    let exact = pipestream_search::exact_vectors::dot(row, query_vector);
                    assert_eq!(hit.score.to_bits(), exact.to_bits(), "row {}", hit.doc_id);
                    found += expected.contains(&hit.doc_id) as usize;
                }
                let outcome = response.dense_execution.unwrap();
                assert_eq!(outcome.provider_backend, HNSW);
                assert_eq!(
                    outcome.filter_regimes,
                    Some(regimes),
                    "streaming={streaming} {court}"
                );
            }
            let recall = found as f64 / (queries.len() / DIM * K) as f64;
            assert!(
                recall >= floor,
                "streaming={streaming} {court}: recall@{K} {recall}"
            );
        }
    }
    // An unfiltered query has no regime to report.
    let response = query(
        &CoordinatorServiceImpl::new(addrs).with_bm25(
            Some(NATIVE_ANALYSIS_BACKEND.to_string()),
            Default::default(),
        ),
        dense_request(
            queries.as_chunks::<DIM>().0[0].as_slice(),
            K as u32,
            DenseExecutionMode::Ann,
        ),
    )
    .await
    .unwrap();
    assert_eq!(response.dense_execution.unwrap().filter_regimes, None);
}

/// Recursive Boolean evaluation needs every member's dense score, and a
/// graph walk reaches only part of a mask. On graph shards the dense pass
/// runs in the enumerate regime instead, which scores every member exactly:
/// a Boolean dense leaf returns the exact FP32 order of its match set, alone
/// and under a filter clause, while ANN stays refused inside a Boolean as on
/// every provider.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn boolean_dense_leaves_are_answered_exactly_by_enumerate() {
    const K: usize = 10;
    let (fixture, addrs) = start_filtered_fixture().await;
    let coordinator = CoordinatorServiceImpl::new(addrs).with_bm25(
        Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        Default::default(),
    );
    let queries = unit_vectors(4, DIM, 0x4A5E_0057);
    let boolean = |vector: &[f32], mode: DenseExecutionMode, court: Option<&str>| {
        let mut must = vec![dense_request(vector, K as u32, mode).selection.unwrap()];
        if let Some(court) = court {
            must.push(SelectionQuery {
                node: Some(selection_query::Node::Filter(FilterQuery {
                    id: "court".into(),
                    predicate: Some(filter_query::Predicate::Cel(format!(
                        r#"court == "{court}""#
                    ))),
                })),
            });
        }
        QueryRequest {
            k: K as u32,
            selection: Some(SelectionQuery {
                node: Some(selection_query::Node::Boolean(BooleanQuery {
                    must,
                    ..Default::default()
                })),
            }),
            ..Default::default()
        }
    };
    for query_vector in queries.as_chunks::<DIM>().0 {
        for mode in [DenseExecutionMode::Exact, DenseExecutionMode::Unspecified] {
            for court in [None, Some("wide"), Some("narrow")] {
                let mut expected: Vec<(u64, f32)> = fixture
                    .corpus
                    .as_chunks::<DIM>()
                    .0
                    .iter()
                    .enumerate()
                    .filter(|(id, _)| court.is_none_or(|court| court_of(*id) == court))
                    .map(|(id, row)| {
                        (
                            id as u64,
                            pipestream_search::exact_vectors::dot(row, query_vector),
                        )
                    })
                    .collect();
                expected.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
                expected.truncate(K);
                let response = query(&coordinator, boolean(query_vector, mode, court))
                    .await
                    .unwrap();
                let got: Vec<u64> = response.hits.iter().map(|hit| hit.doc_id).collect();
                let want: Vec<u64> = expected.iter().map(|hit| hit.0).collect();
                assert_eq!(got, want, "{mode:?} {court:?}");
            }
        }
        let error = query(
            &coordinator,
            boolean(query_vector, DenseExecutionMode::Ann, None),
        )
        .await
        .unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert!(error.message().contains("ANN"), "{}", error.message());
    }
}

/// AUTO serves a filtered query under the enumerate threshold its policy
/// point was measured with, not the provider's own. The same half-corpus
/// mask is walked filter-aware under a threshold below its 3900 rows a shard
/// and enumerated, exactly, under one above; the outcome names the
/// threshold; and a filtered point that names none is refused, because the
/// regime that would serve it is not the one anything measured.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_policy_point_serves_a_filter_under_its_measured_threshold() {
    const K: usize = 20;
    let (fixture, addrs) = start_filtered_fixture().await;
    let fingerprint = graph_index().descriptor().scoring_fingerprint;
    let rows = FILTERED_SHARD_DOCS * SHARDS;
    let policy = |tag: &str, threshold: Option<u32>| {
        let threshold =
            threshold.map_or_else(String::new, |rows| format!("enumerate_max_rows = {rows}\n"));
        load_policy(
            tag,
            &format!(
                "format_version = 1\npolicy_id = \"hnsw-filtered\"\n\
                 embedding_model = \"test-embed\"\ncorpus_generation = {GENERATION}\n\
                 corpus_rows = {rows}\ndimensions = {DIM}\nprovider_backend = \"{HNSW}\"\n\
                 scoring_fingerprint = \"{fingerprint}\"\nmeasured_queries = 16\n\n\
                 [[points]]\nk = {K}\nfilter_selectivity_ppm_min = 400000\n\
                 filter_selectivity_ppm_max = 600000\ncandidates = {K}\n\
                 measured_recall_ppm = 900000\n{threshold}"
            ),
        )
    };
    let query_vector = unit_vectors(1, DIM, 0x4A5E_0058);
    let mut exact: Vec<(u64, f32)> = fixture
        .corpus
        .as_chunks::<DIM>()
        .0
        .iter()
        .enumerate()
        .filter(|(id, _)| court_of(*id) == "wide")
        .map(|(id, row)| {
            (
                id as u64,
                pipestream_search::exact_vectors::dot(row, &query_vector),
            )
        })
        .collect();
    exact.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
    exact.truncate(K);
    let exact: Vec<u64> = exact.into_iter().map(|hit| hit.0).collect();
    let request =
        || filtered_dense_request(&query_vector, K as u32, "wide", DenseExecutionMode::Auto);
    for streaming in [false, true] {
        let coordinator = CoordinatorServiceImpl::new(addrs.clone())
            .with_bm25(
                Some(NATIVE_ANALYSIS_BACKEND.to_string()),
                Default::default(),
            )
            .with_topology_generation(GENERATION)
            .with_stream_search(streaming);

        let walked = coordinator
            .clone()
            .with_dense_execution_policy(policy("walked", Some(100)));
        let response = query(&walked, request()).await.unwrap();
        assert_eq!(response.hits.len(), K);
        let outcome = response.dense_execution.unwrap();
        assert_eq!(outcome.resolved_mode, DenseExecutionMode::Ann as i32);
        assert_eq!(outcome.filter_selectivity_ppm, 500_000);
        assert_eq!(outcome.policy_point.unwrap().enumerate_max_rows, Some(100));
        assert!(
            outcome
                .planner_reason
                .contains("enumerate threshold 100 rows"),
            "{}",
            outcome.planner_reason
        );
        assert_eq!(
            outcome.filter_regimes,
            Some(DenseFilterRegimes {
                plain: SHARDS as u32,
                ..Default::default()
            }),
            "streaming={streaming}"
        );

        let enumerated = coordinator
            .clone()
            .with_dense_execution_policy(policy("enumerated", Some(4000)));
        let response = query(&enumerated, request()).await.unwrap();
        let outcome = response.dense_execution.unwrap();
        assert_eq!(
            outcome.filter_regimes,
            Some(DenseFilterRegimes {
                enumerate: SHARDS as u32,
                ..Default::default()
            }),
            "streaming={streaming}"
        );
        let got: Vec<u64> = response.hits.iter().map(|hit| hit.doc_id).collect();
        assert_eq!(got, exact, "streaming={streaming}");

        let unmeasured = coordinator
            .clone()
            .with_dense_execution_policy(policy("unmeasured", None));
        let error = query(&unmeasured, request()).await.unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(
            error.message().contains("names no enumerate_max_rows"),
            "{}",
            error.message()
        );

        // A bridge-heavy sparse filter just above the policy's enumerate
        // threshold starts filter-aware, hits its visit budget mid-walk,
        // finishes by enumerating the remaining allowed rows without
        // duplicates, and reports both `enumerate` and `budget_handoffs`.
        let budget_policy = load_policy(
            "budget-handoff",
            &format!(
                "format_version = 1\npolicy_id = \"hnsw-budget\"\n\
                 embedding_model = \"test-embed\"\ncorpus_generation = {GENERATION}\n\
                 corpus_rows = {rows}\ndimensions = {DIM}\nprovider_backend = \"{HNSW}\"\n\
                 scoring_fingerprint = \"{fingerprint}\"\nmeasured_queries = 16\n\n\
                 [[points]]\nk = {K}\nfilter_selectivity_ppm_min = 20000\n\
                 filter_selectivity_ppm_max = 50000\ncandidates = {K}\n\
                 measured_recall_ppm = 900000\nenumerate_max_rows = 150\n"
            ),
        );
        let mut exact_sparse: Vec<(u64, f32)> = fixture
            .corpus
            .as_chunks::<DIM>()
            .0
            .iter()
            .enumerate()
            .filter(|(id, _)| court_of(*id) == "sparse")
            .map(|(id, row)| {
                (
                    id as u64,
                    pipestream_search::exact_vectors::dot(row, &query_vector),
                )
            })
            .collect();
        exact_sparse.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
        exact_sparse.truncate(K);
        let exact_sparse: Vec<u64> = exact_sparse.into_iter().map(|hit| hit.0).collect();

        let budgeted = coordinator
            .clone()
            .with_dense_execution_policy(budget_policy);
        let response = query(
            &budgeted,
            filtered_dense_request(&query_vector, K as u32, "sparse", DenseExecutionMode::Auto),
        )
        .await
        .unwrap();
        let outcome = response.dense_execution.unwrap();
        assert_eq!(
            outcome.filter_regimes,
            Some(DenseFilterRegimes {
                enumerate: SHARDS as u32,
                budget_handoffs: SHARDS as u32,
                ..Default::default()
            }),
            "streaming={streaming}"
        );
        let got_sparse: Vec<u64> = response.hits.iter().map(|hit| hit.doc_id).collect();
        assert_eq!(got_sparse, exact_sparse, "streaming={streaming}");
    }
}

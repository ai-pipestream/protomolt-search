//! Explicit ANN recall, exact-mode refusal, policy qualification, floors, and depth.

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn explicit_ann_returns_the_fp32_top_k_and_reports_the_graph() {
    let fixture = start_fixture().await;
    let queries = unit_vectors(8, DIM, 0x4A5E_0003);
    let mut found = 0;
    let mut response = None;
    for query_vector in queries.as_chunks::<DIM>().0 {
        let answer = query(
            &fixture.coordinator,
            dense_request(query_vector, 5, DenseExecutionMode::Ann),
        )
        .await
        .unwrap();
        assert_eq!(answer.hits.len(), 5);
        let truth = fp32_top(&fixture.corpus, query_vector, 5);
        for hit in &answer.hits {
            let row = &fixture.corpus[hit.doc_id as usize * DIM..][..DIM];
            let exact: f32 = row.iter().zip(query_vector).map(|(a, b)| a * b).sum();
            assert!(
                (hit.score - exact).abs() < 1.0e-5,
                "{} vs {exact}",
                hit.score
            );
            found += truth.iter().any(|t| t.0 == hit.doc_id) as usize;
        }
        response = Some(answer);
    }
    assert!(found >= 36, "recall@5 {found}/40");
    let response = response.unwrap();
    let outcome = response.dense_execution.unwrap();
    assert_eq!(outcome.provider_backend, HNSW);
    assert_eq!(outcome.resolved_mode, DenseExecutionMode::Ann as i32);
    assert_eq!(
        outcome.quality_contract,
        VectorQualityContract::ConfiguredAnn as i32
    );
    assert!(!outcome.exhaustive_completion);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_graph_is_never_taken_for_exact() {
    let fixture = start_fixture().await;
    for mode in [
        DenseExecutionMode::Exact,
        DenseExecutionMode::Unspecified,
        DenseExecutionMode::Auto,
    ] {
        let error = query(
            &fixture.coordinator,
            dense_request(&fixture.query_vector, 5, mode),
        )
        .await
        .unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{mode:?}");
        assert!(
            error.message().contains(HNSW),
            "{mode:?}: {}",
            error.message()
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_qualifies_the_graph_only_through_its_own_fingerprint() {
    let fixture = start_fixture().await;
    let policy_for = |fingerprint: &str, tag: &str| {
        let text = format!(
            "format_version = 1\npolicy_id = \"hnsw-v1\"\nembedding_model = \"test-embed\"\n\
             corpus_generation = {GENERATION}\ncorpus_rows = {N_DOCS}\ndimensions = {DIM}\n\
             provider_backend = \"{HNSW}\"\nscoring_fingerprint = \"{fingerprint}\"\n\
             measured_queries = 16\n\n[[points]]\nk = 5\n\
             filter_selectivity_ppm_min = 1000000\nfilter_selectivity_ppm_max = 1000000\n\
             candidates = 10\nmeasured_recall_ppm = 990000\n"
        );
        load_policy(tag, &text)
    };

    // A policy measured at another graph configuration does not qualify.
    let other = VectorIndex::from_backend_config(
        DIM,
        &HnswConfig {
            ef_construction: 300,
            ..Default::default()
        }
        .backend_config()
        .unwrap(),
    )
    .unwrap()
    .descriptor()
    .scoring_fingerprint;
    assert_ne!(other, fixture.fingerprint);
    let coordinator = fixture
        .coordinator
        .clone()
        .with_dense_execution_policy(policy_for(&other, "other"));
    let error = query(
        &coordinator,
        dense_request(&fixture.query_vector, 5, DenseExecutionMode::Auto),
    )
    .await
    .unwrap_err();
    assert!(
        error.message().contains("scoring_fingerprint"),
        "{}",
        error.message()
    );

    let coordinator = fixture
        .coordinator
        .clone()
        .with_dense_execution_policy(policy_for(&fixture.fingerprint, "own"));
    let response = query(
        &coordinator,
        dense_request(&fixture.query_vector, 5, DenseExecutionMode::Auto),
    )
    .await
    .unwrap();
    assert_eq!(response.hits.len(), 5);
    let outcome = response.dense_execution.unwrap();
    assert_eq!(outcome.resolved_mode, DenseExecutionMode::Ann as i32);
    assert_eq!(outcome.policy_id, "hnsw-v1");
    assert_eq!(outcome.candidate_depth, 10);
}

/// The streaming scan reaches each node's graph walk with the request's
/// depth, and a seeded floor only filters what the walk returns: seeding it
/// with the unfloored k-th best must keep the identical top-k rather than
/// stopping the walk at the entry point.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_seeded_floor_filters_the_walk_without_cutting_it_short() {
    let fixture = start_fixture().await;
    let queries = unit_vectors(4, DIM, 0x4A5E_0004);
    for (q, query_vector) in queries.as_chunks::<DIM>().0.iter().enumerate() {
        let unfloored = fixture
            .coordinator
            .fanout_stream_search(
                &format!("hnsw-base-{q}"),
                query_vector,
                5,
                None,
                &Default::default(),
            )
            .await
            .unwrap();
        assert_eq!(unfloored.hits.len(), 5);
        let truth = fp32_top(&fixture.corpus, query_vector, 5);
        let found = unfloored
            .hits
            .iter()
            .filter(|h| truth.iter().any(|t| t.0 == h.vector_id))
            .count();
        assert!(found >= 4, "query {q}: streamed recall@5 {found}/5");
        let kth = unfloored.hits.last().unwrap().score;
        let floored = fixture
            .coordinator
            .fanout_stream_search(
                &format!("hnsw-floor-{q}"),
                query_vector,
                5,
                Some(kth),
                &Default::default(),
            )
            .await
            .unwrap();
        let ids = |hits: &[pipestream_search::pb::ScoredHit]| {
            hits.iter()
                .map(|h| (h.vector_id, h.score.to_bits()))
                .collect::<Vec<_>>()
        };
        assert_eq!(ids(&floored.hits), ids(&unfloored.hits), "query {q}");
    }
}

/// One node's streaming scan, driven directly: the rows it emitted as
/// `(global id, exact score)` and the terminal summary.
async fn node_stream(
    addr: &str,
    vector: &[f32],
    candidate_depth: Option<u32>,
) -> Result<(Vec<(u64, f32)>, pipestream_search::pb::StreamSearchSummary), tonic::Status> {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let (tx, rx) = mpsc::channel(2);
    tx.send(StreamSearchRequest {
        payload: Some(stream_search_request::Payload::Start(StartStreamSearch {
            request_id: format!("depth-{candidate_depth:?}"),
            vector: vector.to_vec(),
            candidate_depth,
            ..Default::default()
        })),
    })
    .await
    .unwrap();
    drop(tx);
    let mut inbound = client
        .stream_search(ReceiverStream::new(rx))
        .await?
        .into_inner();
    let mut hits = Vec::new();
    loop {
        let message = inbound.message().await?.expect("terminal summary");
        match message.payload {
            Some(stream_search_response::Payload::Batch(batch)) => {
                for record in batch.hits.as_chunks::<12>().0 {
                    let id = u64::from_le_bytes(record[..8].try_into().unwrap());
                    let score = f32::from_le_bytes(record[8..].try_into().unwrap());
                    hits.push((id, score));
                }
            }
            Some(stream_search_response::Payload::Summary(summary)) => return Ok((hits, summary)),
            _ => {}
        }
    }
}

/// The candidate depth reaches the provider on a real
/// node's streaming path and sizes the walk — a deeper request scores and
/// emits more rows and a different set — a stream without a depth is refused
/// by the provider's name, every emitted score is the exact FP32 product,
/// and the dense execution outcome reports the depth each request executed
/// at.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn candidate_depth_sizes_the_walk_and_is_reported() {
    let fixture = start_fixture().await;
    let node = &fixture.addrs[0];
    let query_vector = &fixture.query_vector;

    let refused = node_stream(node, query_vector, None).await.unwrap_err();
    assert!(
        refused.message().contains(HNSW) && refused.message().contains("candidate depth"),
        "{}",
        refused.message()
    );

    let (shallow, shallow_summary) = node_stream(node, query_vector, Some(16)).await.unwrap();
    let (deep, deep_summary) = node_stream(node, query_vector, Some(400)).await.unwrap();
    assert!(shallow_summary.completed && deep_summary.completed);
    assert!(
        deep.len() > shallow.len() && deep_summary.blocks_scanned > shallow_summary.blocks_scanned,
        "depth 16 emitted {} rows over {} scored, depth 400 emitted {} over {}",
        shallow.len(),
        shallow_summary.blocks_scanned,
        deep.len(),
        deep_summary.blocks_scanned
    );
    let shallow_ids: std::collections::BTreeSet<u64> = shallow.iter().map(|h| h.0).collect();
    let deep_ids: std::collections::BTreeSet<u64> = deep.iter().map(|h| h.0).collect();
    assert_ne!(shallow_ids, deep_ids);
    for (id, score) in shallow.iter().chain(&deep) {
        let row = &fixture.corpus[*id as usize * DIM..][..DIM];
        let exact = pipestream_search::exact_vectors::dot(row, query_vector);
        assert_eq!(score.to_bits(), exact.to_bits(), "row {id}");
    }

    // The outcome reports the depth the shards were asked for: k for a plain
    // ANN selection, selection_k when the FP32 rerank names a deeper pool.
    for k in [5u32, 40] {
        let response = query(
            &fixture.coordinator,
            dense_request(query_vector, k, DenseExecutionMode::Ann),
        )
        .await
        .unwrap();
        assert_eq!(response.hits.len(), k as usize);
        assert_eq!(response.dense_execution.unwrap().candidate_depth, k);
    }
    let mut pooled = dense_request(query_vector, 5, DenseExecutionMode::Ann);
    pooled.selection_k = 64;
    if let Some(selection_query::Node::Search(search)) =
        pooled.selection.as_mut().and_then(|s| s.node.as_mut())
    {
        if let Some(search_query::Query::Dense(dense)) = search.query.as_mut() {
            dense.score_mode = DenseScoreMode::Fp32Rerank as i32;
        }
    }
    let response = query(&fixture.coordinator, pooled).await.unwrap();
    assert_eq!(response.hits.len(), 5);
    assert_eq!(response.dense_execution.unwrap().candidate_depth, 64);
}

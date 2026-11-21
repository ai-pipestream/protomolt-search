//! Unsealed tail enumeration before flush and patience-stopped graph walks.

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unsealed_tail_enumerates_until_flushed() {
    let per_shard = 400usize;
    let total = per_shard * SHARDS;
    let corpus = unit_vectors(total, DIM, 0x4A5E_0097);
    let query_vector = unit_vectors(1, DIM, 0x4A5E_9700);
    let mut addrs = Vec::new();
    let mut handles = Vec::new();
    for shard in 0..SHARDS {
        let start = shard * per_shard;
        let (addr, handle) = start_node(
            graph_index(),
            NodeConfig {
                slot_offset: start as u64,
                analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
                ..Default::default()
            },
        )
        .await;
        let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
        let (tx, rx) = mpsc::channel(per_shard);
        for i in 0..per_shard {
            tx.send(AddDocumentsRequest {
                text: format!("unsealed doc {}", start + i),
                analysis: Some(body_spec()),
                ..Default::default()
            })
            .await
            .unwrap();
        }
        drop(tx);
        client.add_documents(ReceiverStream::new(rx)).await.unwrap();
        client
            .add_vectors(tokio_stream::iter(vec![AddVectorsRequest {
                materialized_rows: Vec::new(),
                vectors: corpus[start * DIM..(start + per_shard) * DIM].to_vec(),
                dim: DIM as u32,
            }]))
            .await
            .unwrap();
        addrs.push(addr);
        handles.push(handle);
    }

    for streaming in [false, true] {
        let coordinator = CoordinatorServiceImpl::new(addrs.clone())
            .with_bm25(
                Some(NATIVE_ANALYSIS_BACKEND.to_string()),
                Default::default(),
            )
            .with_stream_search(streaming);
        let unsealed = query(
            &coordinator,
            dense_request(&query_vector, 5, DenseExecutionMode::Ann),
        )
        .await
        .unwrap();
        assert_eq!(
            unsealed.dense_execution.unwrap().tail_enumerated_rows,
            total as u64,
            "streaming={streaming}: unsealed shards must report tail_enumerated_rows"
        );
    }

    for addr in &addrs {
        let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
        client.flush(FlushRequest {}).await.unwrap();
    }

    for streaming in [false, true] {
        let coordinator = CoordinatorServiceImpl::new(addrs.clone())
            .with_bm25(
                Some(NATIVE_ANALYSIS_BACKEND.to_string()),
                Default::default(),
            )
            .with_stream_search(streaming);
        let sealed = query(
            &coordinator,
            dense_request(&query_vector, 5, DenseExecutionMode::Ann),
        )
        .await
        .unwrap();
        assert_eq!(
            sealed.dense_execution.unwrap().tail_enumerated_rows,
            0,
            "streaming={streaming}: flushed shards (>= MIN_GRAPH_ROWS) must build graphs"
        );
    }

    for handle in handles {
        handle.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_policy_point_serves_patience_and_reports_stopped_walks() {
    const K: u32 = 10;
    const DEPTH: u32 = 256;
    let (_fixture, addrs) = start_filtered_fixture().await;
    let fingerprint = graph_index().descriptor().scoring_fingerprint;
    let rows = FILTERED_SHARD_DOCS * SHARDS;
    let policy = load_policy(
        "patience-auto",
        &format!(
            "format_version = 1\npolicy_id = \"hnsw-patience\"\n\
             embedding_model = \"test-embed\"\ncorpus_generation = {GENERATION}\n\
             corpus_rows = {rows}\ndimensions = {DIM}\nprovider_backend = \"{HNSW}\"\n\
             scoring_fingerprint = \"{fingerprint}\"\nmeasured_queries = 16\n\n\
             [[points]]\nk = {K}\nfilter_selectivity_ppm_min = 1000000\n\
             filter_selectivity_ppm_max = 1000000\ncandidates = {DEPTH}\n\
             measured_recall_ppm = 990000\npatience_saturation_ppm = 900000\n\
             patience_hops = 3\n"
        ),
    );
    let query_vector = unit_vectors(1, DIM, 0x4A5E_0095);
    for streaming in [false, true] {
        let coordinator = CoordinatorServiceImpl::new(addrs.clone())
            .with_bm25(
                Some(NATIVE_ANALYSIS_BACKEND.to_string()),
                Default::default(),
            )
            .with_topology_generation(GENERATION)
            .with_stream_search(streaming)
            .with_dense_execution_policy(policy.clone());
        let response = query(
            &coordinator,
            dense_request(&query_vector, K, DenseExecutionMode::Auto),
        )
        .await
        .unwrap();
        assert_eq!(response.hits.len(), K as usize, "streaming={streaming}");
        let outcome = response.dense_execution.unwrap();
        assert_eq!(outcome.resolved_mode, DenseExecutionMode::Ann as i32);
        assert_eq!(outcome.candidate_depth, DEPTH);
        let point = outcome.policy_point.unwrap();
        assert_eq!(point.patience_saturation_ppm, Some(900_000));
        assert_eq!(point.patience_hops, Some(3));
        assert!(
            outcome
                .planner_reason
                .contains("patience 900000 ppm over 3 hops"),
            "streaming={streaming}: {}",
            outcome.planner_reason
        );
        assert!(
            outcome.patience_stopped > 0,
            "streaming={streaming}: wide ef={DEPTH} walk with (s=0.90, p=3) must stop early on >=1 shard"
        );
    }

    // Direct node validation: half-specified patience is InvalidArgument.
    let mut node_client = NodeServiceClient::connect(addrs[0].clone()).await.unwrap();
    let half_err = node_client
        .stream_search(tokio_stream::iter(vec![
            pipestream_search::pb::StreamSearchRequest {
                payload: Some(
                    pipestream_search::pb::stream_search_request::Payload::Start(
                        pipestream_search::pb::StartStreamSearch {
                            request_id: "half-patience".into(),
                            vector: query_vector.clone(),
                            candidate_depth: Some(64),
                            patience_saturation_ppm: Some(990_000),
                            patience_hops: None,
                            ..Default::default()
                        },
                    ),
                ),
            },
        ]))
        .await;
    // Stream error arrives either immediately or as first item on the stream.
    let status = match half_err {
        Err(status) => status,
        Ok(resp) => resp.into_inner().message().await.unwrap_err(),
    };
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert!(
        status
            .message()
            .contains("patience_saturation_ppm and patience_hops must be set together"),
        "{}",
        status.message()
    );
}

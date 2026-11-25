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
            dense_request(&query_vector, 5, DenseExecutionMode::Balanced),
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
            dense_request(&query_vector, 5, DenseExecutionMode::Balanced),
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
async fn fast_walks_under_the_speed_preset_and_reports_patience() {
    // FAST's beam is k; at k = 100 on these 1,100-row graphs every walk
    // saturates for 15 hops before it converges.
    const K: u32 = 100;
    let fixture = start_fixture().await;
    let queries = unit_vectors(8, DIM, 0x4A5E_00FA);
    for streaming in [false, true] {
        let coordinator = fixture.coordinator.clone().with_stream_search(streaming);
        for query_vector in queries.as_chunks::<DIM>().0 {
            let answer = query(
                &coordinator,
                dense_request(query_vector, K, DenseExecutionMode::Fast),
            )
            .await
            .unwrap();
            assert_eq!(answer.hits.len(), K as usize, "streaming={streaming}");
            let outcome = answer.dense_execution.unwrap();
            assert_eq!(outcome.requested_mode, DenseExecutionMode::Fast as i32);
            assert_eq!(outcome.resolved_mode, DenseExecutionMode::Fast as i32);
            assert_eq!(outcome.preset, "hnsw-fast-v1");
            assert_eq!(outcome.filter_strategy, DenseFilterStrategy::Speed as i32);
            assert_eq!(outcome.coverage, DenseCoverage::CandidateSet as i32);
            assert!(!outcome.policy_qualified && outcome.policy_id.is_empty());
            assert_eq!(
                techniques_of(&outcome),
                vec![DenseTechnique::HnswWalk as i32]
            );
            assert_eq!(
                outcome.patience_stopped, SHARDS as u32,
                "streaming={streaming}: patience 0.99 over 15 hops must stop every walk"
            );
            assert_eq!(
                outcome.termination,
                DenseTermination::BudgetStopped as i32,
                "streaming={streaming}"
            );
        }
    }
}

fn techniques_of(outcome: &pipestream_search::pb::DenseExecutionOutcome) -> Vec<i32> {
    outcome.techniques.iter().map(|t| t.technique).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn half_specified_patience_is_refused_on_the_node() {
    let fixture = start_fixture().await;
    for (saturation, hops) in [(Some(990_000), None), (None, Some(15))] {
        let mut client = NodeServiceClient::connect(fixture.addrs[0].clone())
            .await
            .unwrap();
        let started = client
            .stream_search(tokio_stream::iter(vec![StreamSearchRequest {
                payload: Some(stream_search_request::Payload::Start(StartStreamSearch {
                    request_id: "half-patience".into(),
                    vector: fixture.query_vector.clone(),
                    candidate_depth: Some(64),
                    patience_saturation_ppm: saturation,
                    patience_hops: hops,
                    ..Default::default()
                })),
            }]))
            .await;
        // The refusal arrives either on the call or as the stream's first item.
        let status = match started {
            Err(status) => status,
            Ok(response) => response.into_inner().message().await.unwrap_err(),
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
}

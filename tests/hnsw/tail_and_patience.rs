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

//! Streaming search straggler flush, identity resolution, and exhaustive protection.

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn finish_stream_search_flushes_straggler_resolves_identities_and_protects_exhaustive() {
    use pipestream_search::coordinator::RequestFilters;
    use pipestream_search::harness::serve_node;
    use pipestream_search::node::NodeServiceImpl;
    use pipestream_search::pb::{
        DocumentIdentity, FinishStreamSearch, StopStreamSearch, StreamEndReason,
    };
    use pipestream_search::vector::EMBEDDED_TURBOVEC;
    use std::time::Duration;

    const PER_SHARD: usize = 1200;
    let query_vector = unit_vectors(1, DIM, 0x4A5E_0100);
    let mut corpus = unit_vectors(PER_SHARD * 2, DIM, 0x4A5E_0101);
    // Plant the unique global rank-1 hit (exact match = score 1.0) at row 0 of
    // shard 1 (global ID = PER_SHARD), so shard 1 discovers it in its very
    // first hops and flushes it via early first-flush before settle fires.
    corpus[PER_SHARD * DIM..(PER_SHARD + 1) * DIM].copy_from_slice(&query_vector);
    let planted_id = PER_SHARD as u64;
    let planted_identity = DocumentIdentity {
        document_key: vec![0xAB; 32],
        version: 11,
        chunk_ordinal: None,
    };

    let mut addrs = Vec::new();
    let mut handles = Vec::new();
    for shard in 0..2usize {
        let mut node = NodeServiceImpl::new(
            Some(graph_index()),
            NodeConfig {
                slot_offset: (shard * PER_SHARD) as u64,
                analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
                ..Default::default()
            },
        );
        if shard == 1 {
            // 12 ms per hop: at candidate_depth = 200 (~40+ hops), a full walk
            // on shard 1 takes ~500 ms unless FinishStreamSearch ends it early.
            node = node.with_stream_poll_delay(Duration::from_millis(12));
        }
        let (addr, handle) = serve_node(node).await;
        let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
        let (tx, rx) = mpsc::channel(PER_SHARD);
        for i in 0..PER_SHARD {
            let gid = shard * PER_SHARD + i;
            let text = format!("straggler doc {gid}");
            let identity = if gid as u64 == planted_id {
                Some(planted_identity.clone())
            } else {
                Some(DocumentIdentity {
                    document_key: format!("doc-{gid:06}").into_bytes(),
                    version: 1,
                    chunk_ordinal: None,
                })
            };
            tx.send(AddDocumentsRequest {
                text: text.clone(),
                analysis: Some(body_spec()),
                original_source: Some(common::protobuf_source(&text, "src")),
                identity,
                ..Default::default()
            })
            .await
            .unwrap();
        }
        drop(tx);
        client.add_documents(ReceiverStream::new(rx)).await.unwrap();

        let start = shard * PER_SHARD * DIM;
        let end = (shard + 1) * PER_SHARD * DIM;
        client
            .add_vectors(tokio_stream::iter(vec![AddVectorsRequest {
                materialized_rows: Vec::new(),
                vectors: corpus[start..end].to_vec(),
                dim: DIM as u32,
            }]))
            .await
            .unwrap();
        client.flush(FlushRequest {}).await.unwrap();
        addrs.push(addr);
        handles.push(handle);
    }

    let coordinator = CoordinatorServiceImpl::new(addrs.clone())
        .with_bm25(
            Some(NATIVE_ANALYSIS_BACKEND.to_string()),
            Default::default(),
        )
        .with_topology_generation(GENERATION)
        .with_stream_search(true);

    // 1. Coordinator settle finish (`finish_settle_us = 45_000` = 45 ms):
    //    shard 0 finishes quickly (< 10 ms), shard 1 emits its early first
    //    flush at hop 2 (~24 ms) containing `planted_id` (resetting settle),
    //    and once top-k stops improving for 45 ms, `FinishStreamSearch` closes
    //    shard 1 cleanly with `IdentityReady` intact.
    let t0 = std::time::Instant::now();
    let streamed = coordinator
        .fanout_stream_search(
            "settle-test",
            &query_vector,
            200,
            None,
            &RequestFilters {
                finish_settle_us: Some(45_000),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let elapsed = t0.elapsed();
    assert!(
        elapsed < Duration::from_millis(320),
        "graceful settle should close straggler well before full ~500ms walk, took {elapsed:?}"
    );
    assert_eq!(streamed.shards_finished_early, 1);
    assert!(streamed.summaries[0].completed);
    assert!(streamed.summaries[1].completed);
    assert_eq!(
        streamed.summaries[0].end_reason,
        StreamEndReason::WalkConverged as i32
    );
    assert_eq!(
        streamed.summaries[1].end_reason,
        StreamEndReason::FinishRequested as i32
    );
    assert_eq!(streamed.hits[0].vector_id, planted_id);
    assert_eq!(
        streamed.hits[0].identity.as_ref(),
        Some(&planted_identity),
        "IdentityReady + ResolveStreamIdentities must resolve straggler's flushed #1 hit"
    );

    // 3. Coordinator deadline finish (`finish_deadline_us = 35_000`) sets
    //    `StreamEndReason::Deadline`.
    let deadlined = coordinator
        .fanout_stream_search(
            "deadline-test",
            &query_vector,
            200,
            None,
            &RequestFilters {
                finish_deadline_us: Some(35_000),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(deadlined.shards_finished_early, 1);
    assert!(deadlined.summaries[1].completed);
    assert_eq!(
        deadlined.summaries[1].end_reason,
        StreamEndReason::Deadline as i32
    );

    // 4. `StopStreamSearch` still means abandon (`completed = false`,
    //    `StreamEndReason::Abandoned`).
    let mut straggler = NodeServiceClient::connect(addrs[1].clone()).await.unwrap();
    let (stx, srx) = mpsc::channel(4);
    stx.send(StreamSearchRequest {
        payload: Some(stream_search_request::Payload::Start(StartStreamSearch {
            request_id: "abandon-check".into(),
            vector: query_vector.clone(),
            candidate_depth: Some(200),
            ..Default::default()
        })),
    })
    .await
    .unwrap();
    let mut sresp = straggler
        .stream_search(ReceiverStream::new(srx))
        .await
        .unwrap()
        .into_inner();
    stx.send(StreamSearchRequest {
        payload: Some(stream_search_request::Payload::Stop(StopStreamSearch {})),
    })
    .await
    .unwrap();
    let mut stop_summary = None;
    while let Some(frame) = sresp.message().await.unwrap() {
        if let Some(stream_search_response::Payload::Summary(s)) = frame.payload {
            stop_summary = Some(s);
        }
    }
    let stop_summary = stop_summary.expect("StopStreamSearch must yield terminal Summary");
    assert!(!stop_summary.completed);
    assert_eq!(stop_summary.end_reason, StreamEndReason::Abandoned as i32);

    // 5. Exhaustive `EmbeddedTurboVec` ignores `FinishStreamSearch` and scans
    //    every block to `StreamEndReason::Exhausted`.
    let tv_vectors = unit_vectors(256, DIM, 0x4A5E_0102);
    let tv_cfg = VectorIndex::fit_backend_config(EMBEDDED_TURBOVEC, DIM, 4, &tv_vectors).unwrap();
    let mut tv_index = VectorIndex::from_backend_config(DIM, &tv_cfg).unwrap();
    tv_index.add(&tv_vectors, DIM).unwrap();
    tv_index.prepare().unwrap();
    let tv_node = NodeServiceImpl::new(
        Some(tv_index),
        NodeConfig {
            chunk_blocks: 1,
            ..Default::default()
        },
    )
    .with_stream_poll_delay(Duration::from_millis(5));
    let (tv_addr, tv_handle) = serve_node(tv_node).await;
    handles.push(tv_handle);

    let mut tv_client = NodeServiceClient::connect(tv_addr).await.unwrap();
    let (ftx, frx) = mpsc::channel(4);
    ftx.send(StreamSearchRequest {
        payload: Some(stream_search_request::Payload::Start(StartStreamSearch {
            request_id: "exhaustive-ignores-finish".into(),
            vector: query_vector.clone(),
            ..Default::default()
        })),
    })
    .await
    .unwrap();
    let mut fresp = tv_client
        .stream_search(ReceiverStream::new(frx))
        .await
        .unwrap()
        .into_inner();
    ftx.send(StreamSearchRequest {
        payload: Some(stream_search_request::Payload::Finish(FinishStreamSearch {
            deadline: false,
        })),
    })
    .await
    .unwrap();
    let mut tv_summary = None;
    while let Some(frame) = fresp.message().await.unwrap() {
        if let Some(stream_search_response::Payload::Summary(s)) = frame.payload {
            tv_summary = Some(s);
        }
    }
    let tv_summary = tv_summary.expect("exhaustive stream must emit Summary");
    assert!(tv_summary.completed);
    assert_eq!(tv_summary.emitted, 256);
    assert_eq!(tv_summary.end_reason, StreamEndReason::Exhausted as i32);

    for handle in handles {
        handle.abort();
    }
}

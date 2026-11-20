//! Broadcast vector backend setup, resident BM25 parity, and fusion debug profiling.

use super::*;

/// BroadcastVectorBackend fans one calibration out to every shard;
/// GetVectorBackend agrees afterwards, re-broadcast is idempotent, and a
/// non-empty shard refuses without breaking the fan-out.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn broadcast_vector_backend_reaches_all_shards() {
    let corpus = unit_vectors(2_000, DIM, 0x5555_0001);
    let (shift, scale) = fit_calibration(DIM, 4, &corpus);

    let (addr_a, handle_a) = start_empty_node(NodeConfig::default()).await;
    let (addr_b, handle_b) = start_empty_node(NodeConfig::default()).await;
    // A third node that already holds vectors: must refuse.
    let (addr_c, handle_c) = start_empty_node(NodeConfig::default()).await;
    add_vectors(&addr_c, corpus[..100 * DIM].to_vec()).await;

    let coordinator =
        CoordinatorServiceImpl::new(vec![addr_a.clone(), addr_b.clone(), addr_c.clone()]);
    let request = pipestream_search::harness::embedded_broadcast_request(
        DIM,
        4,
        &shift,
        &scale,
        String::new(),
    );
    let results = coordinator.fanout_vector_backend(&request).await;
    assert_eq!(results.len(), 3);
    assert!(
        results[0].ok && !results[0].already_configured,
        "node A: {:?}",
        results[0].error
    );
    assert!(
        results[1].ok && !results[1].already_configured,
        "node B: {:?}",
        results[1].error
    );
    assert!(!results[2].ok, "non-empty node must refuse");
    assert!(!results[2].error.is_empty());

    // Every seeded node reports the broadcast calibration.
    for addr in [&addr_a, &addr_b] {
        let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
        let cal = client
            .get_vector_backend(pipestream_search::pb::GetVectorBackendRequest {})
            .await
            .unwrap()
            .into_inner();
        assert_eq!(cal.descriptor.as_ref().unwrap().dim as usize, DIM);
        assert_eq!(
            pipestream_search::harness::reported_calibration(&cal),
            (shift.clone(), scale.clone())
        );
    }

    // Re-broadcast: idempotent no-op on the seeded nodes, still refused
    // on the non-empty one.
    let again = coordinator.fanout_vector_backend(&request).await;
    assert!(again[0].ok && again[0].already_configured);
    assert!(again[1].ok && again[1].already_configured);
    assert!(!again[2].ok);

    handle_a.abort();
    handle_b.abort();
    handle_c.abort();
}

/// The HybridSearch lexical leg routes through block-max
/// (`top_k_pruned`) on a v5-resident shard: the fused result must be
/// bit-identical to the heap-backed fallback path (which keeps top_k).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hybrid_lexical_leg_matches_between_heap_and_v5_resident() {
    let (analysis, mock) = start_mock_analysis().await;
    let corpus = unit_vectors(SHARD_DOCS, DIM, 0x1111_0002);
    let (shift, scale) = fit_calibration(DIM, 4, &corpus);
    let texts: Vec<String> = (0..SHARD_DOCS)
        .map(|i| format!("plain document number {i} about nothing special"))
        .collect();
    let mut zebra = texts.clone();
    zebra[0] = "zebra stripes everywhere".to_string();
    zebra[3] = "another zebra crossing".to_string();

    // Heap-backed shard (Building store → top_k fallback).
    let (addr_heap, handle_heap) =
        start_hybrid_shard(&analysis, 0, &zebra, corpus.clone(), &shift, &scale).await;
    // v5-resident shard (index path → Flush → Bm25Reader → pruned leg).
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("hybrid_v5_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let (addr_v5, handle_v5) = start_empty_node(NodeConfig {
        analysis_addr: Some(analysis.clone()),
        index_path: Some(dir.join("shard.tv")),
        ..Default::default()
    })
    .await;
    configure_backend(&addr_v5, &shift, &scale).await;
    add_documents(&addr_v5, &zebra).await;
    add_vectors(&addr_v5, corpus.clone()).await;
    {
        let mut client = NodeServiceClient::connect(addr_v5.clone()).await.unwrap();
        let flushed = client
            .flush(pipestream_search::pb::FlushRequest {})
            .await
            .unwrap()
            .into_inner();
        assert!(flushed.written);
    }

    let query = corpus[..DIM].to_vec();
    let mut runs = Vec::new();
    for addr in [addr_heap, addr_v5] {
        let coordinator = CoordinatorServiceImpl::new(vec![addr])
            .with_bm25(Some(analysis.clone()), Default::default());
        runs.push(
            common::fanout::hybrid(&coordinator, "h", "zebra", &query, 8, legs_default())
                .await
                .unwrap()
                .0,
        );
    }
    let sig = |hits: &[pipestream_search::pb::HybridHit]| {
        hits.iter()
            .map(|h| {
                (
                    h.doc_id,
                    h.fused_score.to_bits(),
                    h.vector_rank,
                    h.bm25_rank,
                    h.vector_score.to_bits(),
                    h.bm25_score.to_bits(),
                )
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(
        sig(&runs[0]),
        sig(&runs[1]),
        "heap and v5-resident hybrid results diverged"
    );
    // Sanity: the zebra docs really came through the lexical leg.
    assert!(runs[1].iter().any(|h| h.bm25_rank.is_some()));

    handle_heap.abort();
    handle_v5.abort();
    mock.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// The debug/profile block: absent unless requested, mode and depth
/// echoed, one entry per shard with real timings, terms carried, cascade
/// rich with the vector scan's stats — and enabling it never changes
/// results.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn debug_block_profiles_every_fusion_mode() {
    let (analysis, mock) = start_mock_analysis().await;
    let corpus = unit_vectors(2 * SHARD_DOCS, DIM, 0x1111_0009);
    let (shift, scale) = fit_calibration(DIM, 4, &corpus);
    let mut texts: Vec<String> = (0..2 * SHARD_DOCS)
        .map(|i| format!("plain document number {i} about nothing special"))
        .collect();
    texts[0] = "zebra stripes everywhere".to_string();
    texts[5] = "another zebra crossing".to_string();

    let mut addrs = Vec::new();
    let mut handles = Vec::new();
    for shard in 0..2usize {
        let start = shard * SHARD_DOCS;
        let vecs = corpus[start * DIM..(start + SHARD_DOCS) * DIM].to_vec();
        let (addr, handle) = start_hybrid_shard(
            &analysis,
            (shard * SHARD_DOCS) as u64,
            &texts[start..start + SHARD_DOCS],
            vecs,
            &shift,
            &scale,
        )
        .await;
        addrs.push(addr);
        handles.push(handle);
    }
    let coordinator =
        CoordinatorServiceImpl::new(addrs).with_bm25(Some(analysis), Default::default());
    let query = corpus[..DIM].to_vec();

    for legs in [
        legs_default(),
        legs_two_level(),
        legs_blend(Normalization::MinMax, Combination::Arithmetic),
    ] {
        let (plain, no_debug) = common::fanout::hybrid(&coordinator, "p", "zebra", &query, 8, legs)
            .await
            .unwrap();
        assert!(no_debug.is_none(), "debug=false must not build a profile");
        let (hits, debug) =
            common::fanout::hybrid_debug(&coordinator, "d", "zebra", &query, 8, legs)
                .await
                .unwrap();
        assert_eq!(ids(&plain), ids(&hits), "debug changed the results");
        let debug = debug.unwrap();
        assert_eq!(debug.fusion_mode, legs.fusion_mode as i32);
        assert_eq!(debug.leg_k, legs.leg_k);
        assert_eq!(debug.terms, vec!["zebra".to_string()]);
        assert_eq!(debug.shards.len(), 2, "one entry per shard");
        for (i, shard) in debug.shards.iter().enumerate() {
            assert_eq!(shard.shard, i as u32, "shards sorted by index");
            assert!(shard.rpc_ms > 0.0);
        }
        assert!(
            debug.shards.iter().any(|s| s.bm25_hits > 0),
            "zebra must reach some shard's lexical leg"
        );
        assert!(debug.analysis_ms > 0.0);
        assert!(debug.stats_ms > 0.0);
        assert!(debug.legs_ms > 0.0);
        assert!(debug.total_ms >= debug.legs_ms);
    }

    let (plain, no_debug) = common::fanout::cascade(
        &coordinator,
        "cp",
        "zebra",
        &query,
        4,
        0.0,
        &Default::default(),
    )
    .await
    .unwrap();
    assert!(no_debug.is_none());
    let (hits, debug) = common::fanout::cascade_debug(&coordinator, "cd", "zebra", &query, 4, 0.0)
        .await
        .unwrap();
    let cascade_sig = |hits: &[pipestream_search::pb::CascadeHit]| {
        hits.iter().map(|h| (h.doc_id, h.rank)).collect::<Vec<_>>()
    };
    assert_eq!(cascade_sig(&plain), cascade_sig(&hits));
    let debug = debug.unwrap();
    assert_eq!(
        debug.fusion_mode,
        pipestream_search::pb::FusionMode::Cascade as i32
    );
    assert_eq!(debug.terms, vec!["zebra".to_string()]);
    assert_eq!(debug.shards.len(), 2);
    for shard in &debug.shards {
        assert!(
            shard.scan.is_some(),
            "cascade carries the vector scan stats"
        );
        assert!(shard.vector_hits > 0, "phase-1 candidates counted");
    }
    assert!(debug.total_ms > 0.0);

    mock.abort();
    for handle in handles {
        handle.abort();
    }
}

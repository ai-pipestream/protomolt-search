//! Topology map-move fencing, child stream errors, route refusals, and health merging.

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_map_move_refuses_the_decisions_pinned_under_the_older_map() {
    let leaves = leaves().await;
    let l0 = leaves.addrs[0].clone();

    // The token is pinned to the map revision it was issued under.
    let hot = CoordinatorServiceImpl::new(vec![l0.clone()])
        .with_hot_topology(vec![None])
        .unwrap();
    let (relay_addr, relay, _handle) = start_relay_over(hot).await;
    let mut client = NodeServiceClient::connect(relay_addr).await.unwrap();
    let token = client
        .term_stats(stats_request())
        .await
        .unwrap()
        .into_inner()
        .stats_epoch;
    assert_eq!(relay.map().control_revision, 0);
    assert!(relay.token_tuple(token).is_ok());
    relay
        .base()
        .reload_topology(
            1,
            vec![TopologyRoute {
                addr: l0.clone(),
                replica: None,
                hash_range: None,
                placement: None,
            }],
            None,
        )
        .unwrap();
    assert_eq!(relay.map().control_revision, 1);
    let refused = relay.token_tuple(token).unwrap_err();
    assert_eq!(refused.code(), tonic::Code::FailedPrecondition);
    assert!(
        refused.message().starts_with("stale stats epoch")
            && refused.message().contains("revision"),
        "{}",
        refused.message()
    );
    let fresh = client
        .term_stats(stats_request())
        .await
        .unwrap()
        .into_inner()
        .stats_epoch;
    assert_ne!(fresh, token);
    assert_eq!(relay.token_tuple(fresh).unwrap().control_revision, 1);

    // A stream opened under one map and still waiting on a child when
    // the map moves is refused by name, not completed under the old map.
    let hold = capability_probe::Peer::start("StreamSearch").await;
    let hot = CoordinatorServiceImpl::new(vec![l0.clone(), hold.addr.clone()])
        .with_hot_topology(vec![None, None])
        .unwrap();
    let (relay_addr, relay, _handle) = start_relay_over(hot).await;
    let mut client = NodeServiceClient::connect(relay_addr).await.unwrap();
    let (tx, rx) = mpsc::channel(4);
    tx.send(StreamSearchRequest {
        payload: Some(stream_search_request::Payload::Start(StartStreamSearch {
            request_id: "moved".into(),
            vector: unit_vectors(1, DIM, 0x0C0D_E001),
            ..Default::default()
        })),
    })
    .await
    .unwrap();
    let mut inbound = client
        .stream_search(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
    // The live leaf's first batch arrives through the relay while the
    // holding child keeps the attempt open.
    let first = tokio::time::timeout(std::time::Duration::from_secs(5), inbound.message())
        .await
        .expect("live child batch must arrive after capability discovery")
        .unwrap()
        .expect("a batch");
    assert!(matches!(
        first.payload,
        Some(stream_search_response::Payload::Batch(_))
    ));
    relay
        .base()
        .reload_topology(
            1,
            vec![TopologyRoute {
                addr: l0.clone(),
                replica: None,
                hash_range: None,
                placement: None,
            }],
            None,
        )
        .unwrap();
    let refused = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        read_until_summary_or_error(&mut inbound),
    )
    .await
    .expect("map movement must interrupt established fanout")
    .unwrap_err();
    assert_eq!(refused.code(), tonic::Code::FailedPrecondition);
    assert!(
        refused.message().contains("moved from revision 0"),
        "{}",
        refused.message()
    );

    // A transport-hung sibling (accepts TCP, never speaks HTTP/2) must also
    // time out best-effort capability discovery so the healthy leaf streams its
    // first batch before a mid-stream map move refuses the attempt.
    let raw_hold = holding_child().await;
    let hot = CoordinatorServiceImpl::new(vec![l0.clone(), raw_hold])
        .with_hot_topology(vec![None, None])
        .unwrap();
    let (relay_addr, relay, server) = start_relay_over(hot).await;
    let mut client = NodeServiceClient::connect(relay_addr).await.unwrap();
    let (tx, rx) = mpsc::channel(4);
    tx.send(StreamSearchRequest {
        payload: Some(stream_search_request::Payload::Start(StartStreamSearch {
            request_id: "moved-raw-tcp".into(),
            vector: unit_vectors(1, DIM, 0x0C0D_E001),
            ..Default::default()
        })),
    })
    .await
    .unwrap();
    let mut inbound = client
        .stream_search(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
    let first = tokio::time::timeout(std::time::Duration::from_secs(5), inbound.message())
        .await
        .expect("transport-hung sibling must not block live child stream past discovery bound")
        .unwrap()
        .expect("a batch");
    assert!(matches!(
        first.payload,
        Some(stream_search_response::Payload::Batch(_))
    ));
    relay
        .base()
        .reload_topology(
            1,
            vec![TopologyRoute {
                addr: l0.clone(),
                replica: None,
                hash_range: None,
                placement: None,
            }],
            None,
        )
        .unwrap();
    let refused = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        read_until_summary_or_error(&mut inbound),
    )
    .await
    .expect("map movement must interrupt established fanout with a transport-hung sibling")
    .unwrap_err();
    assert_eq!(refused.code(), tonic::Code::FailedPrecondition);
    assert!(
        refused.message().contains("moved from revision 0"),
        "{}",
        refused.message()
    );
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_child_error_fails_the_attempt_and_a_stop_yields_an_incomplete_summary() {
    let leaves = leaves().await;
    let l0 = leaves.addrs[0].clone();
    let relay_addr = relay_over(&[&l0, "http://127.0.0.1:1"]).await;
    let root = CoordinatorServiceImpl::new(vec![relay_addr.clone()]);
    let query = unit_vectors(1, DIM, 0x0C0D_E002);
    let err = match root
        .fanout_stream_search("dead-child", &query, K, None, &Default::default())
        .await
    {
        Ok(_) => panic!("a dead child completed"),
        Err(status) => status,
    };
    assert!(
        err.message().contains("failed") || err.message().contains("child"),
        "{}",
        err.message()
    );

    let relay_addr = relay_over(&[&l0]).await;
    let mut client = NodeServiceClient::connect(relay_addr).await.unwrap();
    let (tx, rx) = mpsc::channel(4);
    tx.send(StreamSearchRequest {
        payload: Some(stream_search_request::Payload::Start(StartStreamSearch {
            request_id: "stopped".into(),
            vector: query.clone(),
            ..Default::default()
        })),
    })
    .await
    .unwrap();
    tx.send(StreamSearchRequest {
        payload: Some(stream_search_request::Payload::Stop(StopStreamSearch {})),
    })
    .await
    .unwrap();
    let mut inbound = client
        .stream_search(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
    let summary = read_until_summary_or_error(&mut inbound).await.unwrap();
    assert!(!summary.completed, "a parent's Stop is never a completion");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unsupported_routes_refuse_by_name() {
    let leaves = leaves().await;
    let relay_addr = relay_over(&[&leaves.addrs[0]]).await;
    let mut client = NodeServiceClient::connect(relay_addr).await.unwrap();
    // Two-level fusion is partition dependent by design, so a relay
    // standing as one shard refuses it by name and says why.
    let err = client
        .hybrid_shard(pipestream_search::pb::HybridShardRequest::default())
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unimplemented);
    assert!(
        err.message().contains("relay")
            && err.message().contains("HybridShard")
            && err.message().contains("partition"),
        "{}",
        err.message()
    );
    // Aggregates the relay cannot merge without changing bits refuse by
    // name on the routes it does serve.
    let err = client
        .bm25_query(Bm25QueryRequest {
            terms: vec!["court".into()],
            k: 3,
            stats_fields: vec!["score".into()],
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unimplemented);
    assert!(
        err.message().contains("stats_fields") && err.message().contains("relay"),
        "{}",
        err.message()
    );
    let (tx, rx) = mpsc::channel::<AddDocumentsRequest>(1);
    drop(tx);
    let err = client
        .add_documents(ReceiverStream::new(rx))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unimplemented);
    assert!(err.message().contains("AddDocuments"), "{}", err.message());
    let err = client
        .compact_shard(pipestream_search::pb::CompactShardRequest::default())
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unimplemented);
    assert!(err.message().contains("CompactShard"), "{}", err.message());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vector_backend_merges_the_children_and_refuses_a_mismatch() {
    let leaves = leaves().await;
    let (relay_addr, _relay, _handle) =
        start_relay(vec![leaves.addrs[0].clone(), leaves.addrs[1].clone()]).await;
    let leaf = NodeServiceClient::connect(leaves.addrs[0].clone())
        .await
        .unwrap()
        .get_vector_backend(pipestream_search::pb::GetVectorBackendRequest {})
        .await
        .unwrap()
        .into_inner();
    let merged = NodeServiceClient::connect(relay_addr)
        .await
        .unwrap()
        .get_vector_backend(pipestream_search::pb::GetVectorBackendRequest {})
        .await
        .unwrap()
        .into_inner();
    assert!(leaf.descriptor.is_some());
    assert_eq!(merged.descriptor, leaf.descriptor);
    assert_eq!(merged.config, leaf.config);
    assert_eq!(merged.num_vectors, (2 * LEAF_ROWS) as u64);

    // A child of another dimension is contiguous in slots and healthy,
    // yet a different provider identity: the relay refuses by name.
    let wide = DIM * 2;
    let mut index = VectorIndex::create(EMBEDDED_TURBOVEC, wide, BIT_WIDTH).unwrap();
    index.add(&unit_vectors(100, wide, 0x6A9), wide).unwrap();
    index.prepare().unwrap();
    let (other, _handle) = start_node(
        index,
        NodeConfig {
            slot_offset: LEAF_ROWS as u64,
            ..Default::default()
        },
    )
    .await;
    let (relay_addr, _relay, _handle) = start_relay(vec![leaves.addrs[0].clone(), other]).await;
    let err = NodeServiceClient::connect(relay_addr)
        .await
        .unwrap()
        .get_vector_backend(pipestream_search::pb::GetVectorBackendRequest {})
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(
        err.message().contains("child 1") && err.message().contains("descriptor"),
        "{}",
        err.message()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn health_merges_contiguous_children_and_refuses_a_gap() {
    let leaves = leaves().await;
    let (relay_addr, relay, _handle) =
        start_relay(vec![leaves.addrs[1].clone(), leaves.addrs[0].clone()]).await;
    let merged = relay.check_children().await.expect("contiguous children");
    assert_eq!(merged.slot_offset, 0);
    assert_eq!(merged.num_vectors, (2 * LEAF_ROWS) as u64);
    assert!(!merged.scoring_fingerprint.is_empty());
    assert!(!merged.wal_clocked && merged.wal_generation == 0);
    let over_the_wire = NodeServiceClient::connect(relay_addr)
        .await
        .unwrap()
        .health(HealthRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(over_the_wire.num_vectors, merged.num_vectors);
    assert_eq!(over_the_wire.slot_offset, 0);

    // A leaf placed past a gap.
    let mut index = VectorIndex::create(EMBEDDED_TURBOVEC, DIM, BIT_WIDTH).unwrap();
    index.add(&unit_vectors(100, DIM, 0x6A9), DIM).unwrap();
    index.prepare().unwrap();
    let (gapped, _handle) = start_node(
        index,
        NodeConfig {
            slot_offset: (LEAF_ROWS + 7) as u64,
            ..Default::default()
        },
    )
    .await;
    let (relay_addr, relay, _handle) = start_relay(vec![leaves.addrs[0].clone(), gapped]).await;
    let err = relay.check_children().await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("gap"), "{}", err.message());
    let err = NodeServiceClient::connect(relay_addr)
        .await
        .unwrap()
        .health(HealthRequest {})
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("contiguous"), "{}", err.message());
}

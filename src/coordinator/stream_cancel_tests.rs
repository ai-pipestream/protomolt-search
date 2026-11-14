//! Stream cancel tests.

use super::*;

#[allow(clippy::type_complexity)]
fn identity_fanout() -> (
    StreamFanout,
    mpsc::Sender<(usize, Result<Option<StreamSearchResponse>, Status>)>,
    mpsc::Receiver<StreamSearchRequest>,
) {
    let (tx, rx) = mpsc::channel(8);
    let (request_tx, request_rx) = mpsc::channel(1);
    (
        StreamFanout {
            scoped: false,
            readers: tokio::task::JoinSet::new(),
            merged_rx: rx,
            floor_txs: vec![Some(request_tx)],
            udp_lanes: vec![None],
            udp_socket: None,
            udp_key: None,
            udp_seq: Arc::new(std::sync::atomic::AtomicU32::new(0)),
        },
        tx,
        request_rx,
    )
}

#[tokio::test]
async fn identity_budget_applies_to_the_combined_reply_from_multiple_children() {
    let (mut fanout, tx, first) = identity_fanout();
    let (second_tx, second) = mpsc::channel(1);
    fanout.floor_txs.push(Some(second_tx));
    fanout.udp_lanes.push(None);
    let scan = StreamSearchSummary {
        completed: true,
        emitted: 1,
        ..Default::default()
    };
    let mut children = tokio::task::JoinSet::new();
    for (shard, mut requests) in [first, second].into_iter().enumerate() {
        let tx = tx.clone();
        let scan = scan.clone();
        children.spawn(async move {
            requests.recv().await.unwrap();
            let row = crate::pb::StreamIdentity {
                vector_id: 100 + shard as u64,
                identity: Some(crate::pb::DocumentIdentity {
                    document_key: vec![shard as u8; 32],
                    version: 7,
                    chunk_ordinal: None,
                }),
                rerank_candidate: None,
            };
            let response = StreamSearchResponse {
                payload: Some(stream_search_response::Payload::Identities(
                    crate::pb::StreamIdentities { rows: vec![row] },
                )),
            };
            assert!(prost::Message::encoded_len(&response) < 64);
            tx.send((shard, Ok(Some(response)))).await.unwrap();
            tx.send((
                shard,
                Ok(Some(StreamSearchResponse {
                    payload: Some(stream_search_response::Payload::Summary(scan)),
                })),
            ))
            .await
            .unwrap();
            while let Some(request) = requests.recv().await {
                if matches!(
                    request.payload,
                    Some(stream_search_request::Payload::Stop(_))
                ) {
                    break;
                }
            }
        });
    }
    let hits = [
        MergedHit {
            shard: 0,
            vector_id: 100,
            score: 1.0,
        },
        MergedHit {
            shard: 1,
            vector_id: 101,
            score: 1.0,
        },
    ];
    let limits = crate::pb::StreamIdentityLimits {
        max_rows: 2,
        max_response_bytes: 64,
        timeout_ms: 1000,
    };
    let error = fanout
        .resolve_identities(
            &hits,
            &[Some(scan.clone()), Some(scan)],
            &mut [None, None],
            &limits,
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::ResourceExhausted);
    assert!(error.message().contains("combined"));
    while let Some(done) = children.join_next().await {
        done.unwrap();
    }
}

#[tokio::test]
async fn completed_fanout_releases_a_peer_that_never_closes_its_response() {
    let (mut fanout, _tx, _requests) = identity_fanout();
    let (started, ready) = tokio::sync::oneshot::channel();
    let (retained, released) = tokio::sync::oneshot::channel::<()>();
    fanout.readers.spawn(async move {
        let _retained = retained;
        let _ = started.send(());
        std::future::pending::<()>().await;
    });
    ready.await.unwrap();
    fanout.mark_completed(0);
    drop(fanout);
    assert!(tokio::time::timeout(Duration::from_secs(1), released)
        .await
        .expect("terminal completion must release response readers")
        .is_err());
}

#[tokio::test]
async fn identity_timeout_is_bounded_even_when_the_request_lane_is_full() {
    let (mut fanout, _tx, mut requests) = identity_fanout();
    let limits = crate::pb::StreamIdentityLimits {
        max_rows: 1,
        max_response_bytes: 1024,
        timeout_ms: 10,
    };
    let scan = StreamSearchSummary {
        completed: true,
        ..Default::default()
    };
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        fanout.resolve_identities(&[], &[Some(scan)], &mut [None], &limits),
    )
    .await
    .expect("error cleanup must not wait forever behind the unanswered selection");
    assert_eq!(result.unwrap_err().code(), tonic::Code::DeadlineExceeded);
    assert!(matches!(
        requests.recv().await.unwrap().payload,
        Some(stream_search_request::Payload::ResolveIdentities(_))
    ));
    assert!(requests.recv().await.is_none());
}

#[tokio::test]
async fn identity_exchange_rejects_wrong_ids_missing_or_changed_certificates_and_early_close() {
    let scan = StreamSearchSummary {
        completed: true,
        emitted: 1,
        ..Default::default()
    };
    let rows = |id| StreamSearchResponse {
        payload: Some(stream_search_response::Payload::Identities(
            crate::pb::StreamIdentities {
                rows: vec![crate::pb::StreamIdentity {
                    vector_id: id,
                    identity: None,
                    rerank_candidate: None,
                }],
            },
        )),
    };
    let summary = |scan| StreamSearchResponse {
        payload: Some(stream_search_response::Payload::Summary(scan)),
    };
    let cases = [
        (vec![Some(rows(999))], tonic::Code::Internal),
        (
            vec![Some(summary(scan.clone()))],
            tonic::Code::FailedPrecondition,
        ),
        (
            vec![
                Some(rows(100)),
                Some(summary(StreamSearchSummary {
                    emitted: 2,
                    ..scan.clone()
                })),
            ],
            tonic::Code::FailedPrecondition,
        ),
        (vec![Some(rows(100)), None], tonic::Code::Internal),
        (
            vec![Some(rows(100)), Some(rows(100))],
            tonic::Code::Internal,
        ),
    ];
    for (messages, expected) in cases {
        let (mut fanout, tx, mut requests) = identity_fanout();
        let child = tokio::spawn(async move {
            let request = requests.recv().await.unwrap();
            assert!(matches!(
                request.payload,
                Some(stream_search_request::Payload::ResolveIdentities(_))
            ));
            for message in messages {
                tx.send((0, Ok(message))).await.unwrap();
            }
            assert!(matches!(
                requests.recv().await.unwrap().payload,
                Some(stream_search_request::Payload::Stop(_))
            ));
        });
        let limits = crate::pb::StreamIdentityLimits {
            max_rows: 1,
            max_response_bytes: 1024,
            timeout_ms: 1000,
        };
        let hit = MergedHit {
            shard: 0,
            vector_id: 100,
            score: 1.0,
        };
        assert_eq!(
            fanout
                .resolve_identities(&[hit], &[Some(scan.clone())], &mut [None], &limits)
                .await
                .unwrap_err()
                .code(),
            expected
        );
        child.await.unwrap();
    }
}

fn route(addr: &str, lo: u64, hi: u64) -> TopologyRoute {
    TopologyRoute {
        addr: addr.to_string(),
        replica: None,
        hash_range: Some((lo, hi)),
        placement: None,
    }
}

#[test]
fn topology_refuses_ragged_or_incomplete_hash_space() {
    assert!(
        build_topology(1, vec![route("a", 0, 9), route("b", 11, u64::MAX)], None)
            .err()
            .expect("gap must be refused")
            .contains("gap or overlap")
    );
    assert!(
        build_topology(1, vec![route("a", 0, 10), route("b", 10, u64::MAX)], None)
            .err()
            .expect("overlap must be refused")
            .contains("gap or overlap")
    );
    assert!(build_topology(
        1,
        vec![
            route("a", 0, u64::MAX),
            TopologyRoute {
                addr: "b".to_string(),
                replica: None,
                hash_range: None,
                placement: None,
            }
        ],
        None
    )
    .err()
    .expect("ragged ranges must be refused")
    .contains("every shard or none"));
    assert!(build_topology(
        1,
        vec![
            TopologyRoute {
                addr: "a".into(),
                replica: Some("b".into()),
                hash_range: Some((0, 9)),
                placement: None,
            },
            route("b", 10, u64::MAX),
        ],
        None,
    )
    .err()
    .expect("a replica cannot also serve another logical shard")
    .contains("duplicate topology endpoint"));
}

#[test]
fn hot_topology_snapshots_one_generation_per_request() {
    let coordinator = CoordinatorServiceImpl::new(vec!["old".to_string()])
        .with_topology_generation(4)
        .with_hot_topology(vec![Some((0, u64::MAX))])
        .unwrap();
    let old = coordinator.request_snapshot().unwrap();

    coordinator
        .reload_topology(5, vec![route("new", 0, u64::MAX)], None)
        .unwrap();
    let new = coordinator.request_snapshot().unwrap();

    assert_eq!(old.topology_generation, 4);
    assert_eq!(old.node_addrs, vec!["old"]);
    assert_eq!(new.topology_generation, 5);
    assert_eq!(new.node_addrs, vec!["new"]);
    assert_eq!(coordinator.current_topology_generation(), 5);
    assert!(coordinator
        .reload_topology(5, vec![route("newer", 0, u64::MAX)], None)
        .unwrap_err()
        .contains("must increase"));
}

#[tokio::test]
async fn cutover_barrier_publishes_a_new_shard_count_atomically() {
    let coordinator = CoordinatorServiceImpl::new(vec!["old".to_string()])
        .with_topology_generation(4)
        .with_hot_topology(vec![Some((0, u64::MAX))])
        .unwrap();
    let frozen = SearchService::freeze_topology_writes(
        &coordinator,
        Request::new(FreezeTopologyWritesRequest {
            collection: String::new(),
            required_topology_generation: 4,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(coordinator
        .reload_topology(5, vec![route("other", 0, u64::MAX)], None)
        .unwrap_err()
        .contains("frozen writes"));

    let split = u64::MAX / 2;
    SearchService::publish_topology(
        &coordinator,
        Request::new(PublishTopologyRequest {
            collection: String::new(),
            placement: None,
            cutover_token: frozen.cutover_token,
            generation: 5,
            shards: vec![
                crate::pb::PublishedTopologyShard {
                    addr: "a:50051".into(),
                    replica: String::new(),
                    hash_lo: 0,
                    hash_hi: split,
                    has_placement: false,
                    placement: 0,
                },
                crate::pb::PublishedTopologyShard {
                    addr: "b:50051".into(),
                    replica: String::new(),
                    hash_lo: split + 1,
                    hash_hi: u64::MAX,
                    has_placement: false,
                    placement: 0,
                },
            ],
        }),
    )
    .await
    .unwrap();
    assert_eq!(coordinator.current_topology_generation(), 5);
    assert_eq!(coordinator.current_topology_routes().len(), 2);
    let (_, routed) = coordinator.route_stable_key(b"stable-product-id").unwrap();
    assert!(routed < 2);

    let frozen = SearchService::freeze_topology_writes(
        &coordinator,
        Request::new(FreezeTopologyWritesRequest {
            collection: String::new(),
            required_topology_generation: 5,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let aborted = SearchService::abort_topology_cutover(
        &coordinator,
        Request::new(AbortTopologyCutoverRequest {
            collection: String::new(),
            cutover_token: frozen.cutover_token,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(aborted.topology_generation, 5);
}

#[test]
fn stable_product_keys_route_deterministically() {
    let split = u64::MAX / 2;
    let coordinator = CoordinatorServiceImpl::new(vec!["a".to_string(), "b".to_string()])
        .with_topology_generation(9)
        .with_hot_topology(vec![Some((0, split)), Some((split + 1, u64::MAX))])
        .unwrap();
    let key = b"courtlistener:opinion:123/chunk:7";
    let expected = usize::from(stable_routing_hash(key) > split);
    assert_eq!(coordinator.route_stable_key(key), Ok((9, expected)));
    assert_eq!(coordinator.route_stable_key(key), Ok((9, expected)));
    assert!(coordinator
        .route_stable_key(b"")
        .unwrap_err()
        .contains("empty"));
}

#[test]
fn in_process_mode_has_no_network_fallback_or_udp_lane() {
    let coordinator = CoordinatorServiceImpl::with_local_nodes(Vec::new());
    assert!(!coordinator.allows_network());
    let error = coordinator
        .node_client("http://must-not-resolve.invalid:50051")
        .expect_err("a missing in-process link must not dial");
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(coordinator.floor_socket().is_none());
    assert!(coordinator
        .floor_target("must-not-resolve.invalid:50051")
        .is_none());
}

#[tokio::test]
async fn cancellation_uses_typed_udp_then_authoritative_grpc_stop() {
    let udp_rx = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let target = udp_rx.local_addr().unwrap();
    let udp_tx = Arc::new(std::net::UdpSocket::bind("127.0.0.1:0").unwrap());
    udp_tx.set_nonblocking(true).unwrap();

    let (request_tx, mut request_rx) = mpsc::channel(1);
    let (_merged_tx, merged_rx) = mpsc::channel(1);
    let token = 0x0A11_CE11_u64;
    let mut fanout = StreamFanout {
        scoped: false,
        readers: tokio::task::JoinSet::new(),
        merged_rx,
        floor_txs: vec![Some(request_tx)],
        udp_lanes: vec![Some((token, target))],
        udp_socket: Some(udp_tx),
        udp_key: None,
        udp_seq: Arc::new(std::sync::atomic::AtomicU32::new(0)),
    };

    fanout.cancel().await;

    let request = request_rx.recv().await.expect("authoritative Stop");
    assert!(matches!(
        request.payload,
        Some(stream_search_request::Payload::Stop(_))
    ));
    assert!(
        request_rx.recv().await.is_none(),
        "request stream must close"
    );

    let mut frame = [0u8; crate::stream_signal::FRAME_LEN];
    let (len, _) = tokio::time::timeout(Duration::from_secs(1), udp_rx.recv_from(&mut frame))
        .await
        .expect("UDP cancel timed out")
        .unwrap();
    assert_eq!(len, crate::stream_signal::FRAME_LEN);
    assert_eq!(
        crate::stream_signal::decode(&frame),
        Some(crate::stream_signal::StreamSignal::Cancel { token })
    );
}

#[tokio::test]
async fn dropping_an_unfinished_fanout_still_stops_both_lanes() {
    let udp_rx = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let target = udp_rx.local_addr().unwrap();
    let udp_tx = Arc::new(std::net::UdpSocket::bind("127.0.0.1:0").unwrap());
    udp_tx.set_nonblocking(true).unwrap();

    let (request_tx, mut request_rx) = mpsc::channel(1);
    let (_merged_tx, merged_rx) = mpsc::channel(1);
    let token = 0x0D09_CE11_u64;
    drop(StreamFanout {
        scoped: false,
        readers: tokio::task::JoinSet::new(),
        merged_rx,
        floor_txs: vec![Some(request_tx)],
        udp_lanes: vec![Some((token, target))],
        udp_socket: Some(udp_tx),
        udp_key: None,
        udp_seq: Arc::new(std::sync::atomic::AtomicU32::new(0)),
    });

    let request = tokio::time::timeout(Duration::from_secs(1), request_rx.recv())
        .await
        .expect("gRPC Stop task timed out")
        .expect("authoritative Stop");
    assert!(matches!(
        request.payload,
        Some(stream_search_request::Payload::Stop(_))
    ));

    let mut frame = [0u8; crate::stream_signal::FRAME_LEN];
    let (len, _) = tokio::time::timeout(Duration::from_secs(1), udp_rx.recv_from(&mut frame))
        .await
        .expect("UDP cancel timed out")
        .unwrap();
    assert_eq!(len, crate::stream_signal::FRAME_LEN);
    assert_eq!(
        crate::stream_signal::decode(&frame),
        Some(crate::stream_signal::StreamSignal::Cancel { token })
    );
}

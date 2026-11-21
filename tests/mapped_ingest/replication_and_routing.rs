//! Clocked WAL replica catch-up and stable-key routed mapped ingest.

use super::*;

#[tokio::test]
async fn clocked_wal_catches_a_replica_up_idempotently() {
    replicated_binding(false).await;
}
#[tokio::test]
async fn explicit_analysis_replication_preserves_the_binding_and_is_idempotent() {
    replicated_binding(true).await;
}
async fn replicated_binding(explicit: bool) {
    let (analysis, _mock) = start_mock_analysis().await;
    let root = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "tvmapped_replication_{explicit}_{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let (primary, primary_handle) = start_empty_node(NodeConfig {
        index_path: Some(root.join("primary.tv")),
        wal: true,
        ..case_node_config(analysis.clone())
    })
    .await;
    let (replica, replica_handle) = start_empty_node(NodeConfig {
        index_path: Some(root.join("replica.tv")),
        wal: true,
        ..case_node_config(analysis)
    })
    .await;
    seed_calibration(&primary).await;
    seed_calibration(&replica).await;

    let key = b"law.v1.Case/case-0".to_vec();
    let mut client = NodeServiceClient::connect(primary.clone()).await.unwrap();
    let response = client
        .ingest_mapped(tokio_stream::iter([
            IngestMappedRequest {
                payload: Some(ingest_mapped_request::Payload::Bind(if explicit {
                    explicit_bind()
                } else {
                    bind()
                })),
            },
            IngestMappedRequest {
                payload: Some(ingest_mapped_request::Payload::RoutedDocument(
                    RoutedMappedDocument {
                        stable_key: key.clone(),
                        document: doc(0).encode(),
                    },
                )),
            },
        ]))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.added, 1);

    let first =
        pipestream_search::replication::sync_once(&pipestream_search::replication::ReplicaCursor {
            primary: primary.clone(),
            replica: replica.clone(),
            ..Default::default()
        })
        .await
        .expect("first catch-up");
    assert!(first.clock > 0);
    let replica_clock_after_first = NodeServiceClient::connect(replica.clone())
        .await
        .unwrap()
        .health(HealthRequest {})
        .await
        .unwrap()
        .into_inner()
        .wal_high_watermark;
    let second = pipestream_search::replication::sync_once(&first)
        .await
        .expect("idempotent empty catch-up");
    assert!(second.clock >= first.clock);

    if explicit {
        let mut replica_client = NodeServiceClient::connect(replica.clone()).await.unwrap();
        for extra in [false, true] {
            let error = replica_client
                .add_documents(tokio_stream::iter([
                    pipestream_search::pb::AddDocumentsRequest {
                        text: "rogue".into(),
                        analysis: extra.then(pipestream_search::analyzer::body_spec),
                        fields: if extra {
                            vec![pipestream_search::pb::DocumentField {
                                field: "meta_author".into(),
                                text: "rogue".into(),
                                analysis: None,
                            }]
                        } else {
                            Vec::new()
                        },
                        ..Default::default()
                    },
                ]))
                .await
                .unwrap_err();
            assert_eq!(error.code(), tonic::Code::FailedPrecondition);
            assert!(
                error.message().contains("explicit mapped binding"),
                "{error}"
            );
        }
    }

    let source = NodeServiceClient::connect(primary)
        .await
        .unwrap()
        .health(HealthRequest {})
        .await
        .unwrap()
        .into_inner();
    let target = NodeServiceClient::connect(replica)
        .await
        .unwrap()
        .health(HealthRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(source.num_vectors, target.num_vectors);
    assert_eq!(source.document_slots, target.document_slots);
    assert_eq!(source.scoring_fingerprint, target.scoring_fingerprint);
    assert_eq!(target.wal_high_watermark, replica_clock_after_first);
    let replica_store = pipestream_search::postings::Bm25Store::load(
        &common::catalog::segment_bm25(&root.join("replica.tv"), 0),
    )
    .unwrap();
    if explicit {
        let generation = pipestream_search::reshard::resolve_gen(&pipestream_search::wal::wal_dir(
            &root.join("primary.tv"),
        ))
        .unwrap();
        let source_binding = pipestream_search::reshard::read_generation_binding(&generation)
            .unwrap()
            .unwrap();
        assert_eq!(source_binding.analysis_sha.len(), 64);
        assert_eq!(replica_store.binding(), Some(&source_binding));
    } else {
        assert_eq!(replica_store.binding(), Some(&expected_binding()));
    }
    assert_eq!(
        replica_store.protobuf_source(0).unwrap(),
        Some((
            ProtobufSource {
                descriptor_set: case_set(),
                message_type: "law.v1.Case".into(),
                payload: doc(0).encode(),
            },
            None
        ))
    );

    primary_handle.abort();
    replica_handle.abort();
    std::fs::remove_dir_all(&root).ok();
}

#[tokio::test]
async fn routed_mapped_ingest_uses_stable_keys_and_requires_one_generation() {
    let (analysis, _mock) = start_mock_analysis().await;
    let split = u64::MAX / 2;
    let mut nodes = Vec::new();
    let mut handles = Vec::new();
    for offset in [0, 1_000] {
        let (addr, handle) = start_empty_node(NodeConfig {
            slot_offset: offset,
            ..case_node_config(analysis.clone())
        })
        .await;
        seed_calibration(&addr).await;
        nodes.push(addr);
        handles.push(handle);
    }
    let coordinator = CoordinatorServiceImpl::new(nodes.clone())
        .with_topology_generation(10)
        .with_bm25(Some(analysis), Default::default())
        .with_hot_topology(vec![Some((0, split)), Some((split + 1, u64::MAX))])
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let coordinator_addr = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(CoordinatorServiceImpl::into_server(
                coordinator.clone(),
                pipestream_search::MAX_MESSAGE_BYTES,
            ))
            .serve_with_incoming(pipestream_search::harness::nodelay_incoming(listener)),
    );

    let mut keys: [Option<Vec<u8>>; 2] = [None, None];
    for candidate in 0..10_000u64 {
        let key = format!("law.v1.Case/case-{candidate}").into_bytes();
        let shard = usize::from(pipestream_search::coordinator::stable_routing_hash(&key) > split);
        keys[shard].get_or_insert(key);
        if keys.iter().all(Option::is_some) {
            break;
        }
    }
    let mut client = SearchServiceClient::connect(coordinator_addr)
        .await
        .unwrap();
    let stream = vec![
        RoutedIngestMappedRequest {
            payload: Some(routed_ingest_mapped_request::Payload::Bind(
                RoutedMappedBind {
                    collection: String::new(),
                    required_topology_generation: 10,
                    bind: Some(bind()),
                },
            )),
        },
        RoutedIngestMappedRequest {
            payload: Some(routed_ingest_mapped_request::Payload::Document(
                RoutedMappedDocument {
                    stable_key: keys[0].clone().unwrap(),
                    document: doc(0).encode(),
                },
            )),
        },
        RoutedIngestMappedRequest {
            payload: Some(routed_ingest_mapped_request::Payload::Document(
                RoutedMappedDocument {
                    stable_key: keys[1].clone().unwrap(),
                    document: doc(1).encode(),
                },
            )),
        },
    ];
    let response = client
        .routed_ingest_mapped(tokio_stream::iter(stream))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.served_topology_generation, 10);
    assert_eq!(response.added, 2);
    assert_eq!(response.shards.len(), 2);
    assert!(response.shards.iter().all(|shard| shard.added == 1));
    for addr in &nodes {
        let health = NodeServiceClient::connect(addr.clone())
            .await
            .unwrap()
            .health(HealthRequest {})
            .await
            .unwrap()
            .into_inner();
        assert_eq!(health.num_vectors, 1);
        assert_eq!(health.document_slots, 1);
    }

    let error = client
        .routed_ingest_mapped(tokio_stream::iter([RoutedIngestMappedRequest {
            payload: Some(routed_ingest_mapped_request::Payload::Bind(
                RoutedMappedBind {
                    collection: String::new(),
                    required_topology_generation: 9,
                    bind: Some(bind()),
                },
            )),
        }]))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);

    let frozen = SearchService::freeze_topology_writes(
        &coordinator,
        Request::new(FreezeTopologyWritesRequest {
            collection: String::new(),
            required_topology_generation: 10,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let mut waiting_client = client.clone();
    let waiting = tokio::spawn(async move {
        waiting_client
            .routed_ingest_mapped(tokio_stream::iter([RoutedIngestMappedRequest {
                payload: Some(routed_ingest_mapped_request::Payload::Bind(
                    RoutedMappedBind {
                        collection: String::new(),
                        required_topology_generation: 10,
                        bind: Some(bind()),
                    },
                )),
            }]))
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(!waiting.is_finished(), "routed write crossed the freeze");
    SearchService::publish_topology(
        &coordinator,
        Request::new(PublishTopologyRequest {
            collection: String::new(),
            placement: None,
            cutover_token: frozen.cutover_token,
            generation: 11,
            shards: vec![
                PublishedTopologyShard {
                    addr: nodes[0].clone(),
                    replica: String::new(),
                    hash_lo: 0,
                    hash_hi: split,
                    has_placement: false,
                    placement: 0,
                },
                PublishedTopologyShard {
                    addr: nodes[1].clone(),
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
    let error = waiting.await.unwrap().unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(error.message().contains("generation 11"));

    server.abort();
    for handle in handles {
        handle.abort();
    }
}

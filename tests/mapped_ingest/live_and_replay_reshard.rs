//! Online reshard tail catch-up with live cutover and offline WAL binding replay.

use super::*;

fn routed_mapped_range(start: usize, end: usize) -> Vec<IngestMappedRequest> {
    std::iter::once(IngestMappedRequest {
        payload: Some(ingest_mapped_request::Payload::Bind(bind())),
    })
    .chain((start..end).map(|i| IngestMappedRequest {
        payload: Some(ingest_mapped_request::Payload::RoutedDocument(
            RoutedMappedDocument {
                stable_key: format!("law.v1.Case/case-{i}").into_bytes(),
                document: doc(i).encode(),
            },
        )),
    }))
    .collect()
}

type NodeTask = tokio::task::JoinHandle<Result<(), tonic::transport::Error>>;

async fn spawn_live_reshard_children(
    children_images: &[ChildImage],
    analysis: &str,
) -> (Vec<LiveChild>, Vec<NodeTask>) {
    let (mut children, mut handles) = (Vec::new(), Vec::new());
    for c in children_images {
        let cfg = NodeConfig {
            index_path: Some(c.vector_path.clone()),
            slot_offset: c.slot_offset,
            wal: true,
            ..case_node_config(analysis.to_string())
        };
        let svc = pipestream_search::node::NodeServiceImpl::open(cfg, None).unwrap();
        let lis = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = format!("http://{}", lis.local_addr().unwrap());
        let incoming = pipestream_search::harness::nodelay_incoming(lis);
        let srv = svc.into_server(pipestream_search::MAX_MESSAGE_BYTES);
        handles.push(tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(srv)
                .serve_with_incoming(incoming),
        ));
        children.push(LiveChild {
            addr,
            replica: None,
            hash_lo: c.hash_lo,
            hash_hi: c.hash_hi,
            slot_offset: c.slot_offset,
            base_vectors: 0,
            base_document_slots: 0,
            applied_vectors: 0,
            applied_documents: 0,
        });
    }
    (children, handles)
}

fn split_mapped_baseline(
    source_path: &std::path::Path,
    out_dir: &std::path::Path,
    analysis: &str,
) -> StableReshardOutput {
    let g = resolve_gen(&pipestream_search::wal::wal_dir(source_path)).unwrap();
    let rt = tokio::runtime::Handle::current();
    let addr = analysis.to_string();
    let mut analyze = move |docs: &[(
        &str,
        Option<&pipestream_search::pb::AnalysisSpec>,
        pipestream_search::analyzer::SessionLayers,
    )]| {
        tokio::task::block_in_place(|| {
            rt.block_on(pipestream_search::analyzer::analyze_batch_streams(
                &addr, docs, 1,
            ))
            .map_err(|e| e.to_string())
        })
    };
    let fields = ["body".to_string(), "meta_author".to_string()];
    let req = SplitRequest::new(std::slice::from_ref(&g), 2, out_dir)
        .slot_base(10_000)
        .slot_stride(10_000)
        .bm25_fields(Some(&fields));
    split_stable_logs_with(&req, &mut analyze).unwrap()
}

async fn verify_live_cutover_and_routed_bind(
    root: &std::path::Path,
    source: &str,
    analysis: String,
    source_client: &mut NodeServiceClient<tonic::transport::Channel>,
    state: &LiveReshardState,
) -> NodeTask {
    let coord = CoordinatorServiceImpl::new(vec![source.to_string()])
        .with_topology_generation(1)
        .with_bm25(Some(analysis), Default::default())
        .with_hot_topology(vec![Some((0, u64::MAX))])
        .unwrap();
    let lis = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let coord_addr = format!("http://{}", lis.local_addr().unwrap());
    let srv = CoordinatorServiceImpl::into_server(coord, pipestream_search::MAX_MESSAGE_BYTES);
    let incoming = pipestream_search::harness::nodelay_incoming(lis);
    let coord_handle = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(srv)
            .serve_with_incoming(incoming),
    );
    source_client
        .ingest_mapped(tokio_stream::iter(routed_mapped_range(5, 6)))
        .await
        .unwrap();
    let (state_path, map_path) = (root.join("live-state.toml"), root.join("shard-map.toml"));
    let final_st = atomic_live_cutover(&coord_addr, state, &state_path, &map_path)
        .await
        .unwrap();
    assert!(final_st.source_clock > state.source_clock);
    assert_eq!(load_shard_map(&map_path).unwrap().generation, 2);
    let rows: u64 = final_st
        .children
        .iter()
        .map(|c| c.base_vectors + c.applied_vectors)
        .sum();
    assert_eq!(rows, 6);
    let mut client = SearchServiceClient::connect(coord_addr).await.unwrap();
    let bind_msg = RoutedMappedBind {
        collection: String::new(),
        required_topology_generation: 2,
        bind: Some(bind()),
    };
    let req = RoutedIngestMappedRequest {
        payload: Some(routed_ingest_mapped_request::Payload::Bind(bind_msg)),
    };
    let resp = client
        .routed_ingest_mapped(tokio_stream::iter([req]))
        .await
        .unwrap();
    assert_eq!(resp.into_inner().served_topology_generation, 2);
    coord_handle
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_reshard_catches_the_tail_and_cuts_over_without_stopping_queries() {
    let (analysis, _mock) = start_mock_analysis().await;
    let root = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("tvmapped_live_reshard_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let source_path = root.join("source.tv");
    let (source, source_handle) = start_empty_node(NodeConfig {
        index_path: Some(source_path.clone()),
        wal: true,
        wal_buckets: 8,
        ..case_node_config(analysis.clone())
    })
    .await;
    seed_calibration(&source).await;
    let mut source_client = NodeServiceClient::connect(source.clone()).await.unwrap();
    source_client
        .ingest_mapped(tokio_stream::iter(routed_mapped_range(0, 4)))
        .await
        .unwrap();
    source_client.flush(FlushRequest {}).await.unwrap();

    let baseline = split_mapped_baseline(&source_path, &root.join("children"), &analysis);
    assert_eq!(baseline.source_cutoffs.len(), 1);
    let (children, child_handles) =
        spawn_live_reshard_children(&baseline.images.children, &analysis).await;
    let state = initialize_live_reshard(source.clone(), baseline.source_cutoffs[0], 1, 2, children)
        .await
        .unwrap();
    source_client
        .ingest_mapped(tokio_stream::iter(routed_mapped_range(4, 5)))
        .await
        .unwrap();
    let state = catch_up_children_once(&state).await.unwrap();
    assert!(state.source_clock > baseline.source_cutoffs[0].high_watermark);

    let coord_handle =
        verify_live_cutover_and_routed_bind(&root, &source, analysis, &mut source_client, &state)
            .await;
    for h in [coord_handle, source_handle]
        .into_iter()
        .chain(child_handles)
    {
        h.abort();
    }
    std::fs::remove_dir_all(root).ok();
}

/// Replay must not launder a binding away: the bind rides the WAL
/// (markers), and resharded children come out bound to the same plan
/// their parent was written under.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reshard_replay_carries_the_binding() {
    reshard_binding(false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_analysis_reshard_replay_keeps_the_complete_binding() {
    reshard_binding(true).await;
}
async fn reshard_binding(explicit: bool) {
    let (analysis, _mock) = start_mock_analysis().await;
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "tvmapped_reshard_{explicit}_{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let index_path = dir.join("shard.tv");

    let (addr, _node) = start_empty_node(NodeConfig {
        index_path: Some(index_path.clone()),
        wal: true,
        wal_buckets: 8,
        ..case_node_config(analysis.clone())
    })
    .await;
    seed_calibration(&addr).await;
    send(
        &addr,
        if explicit { explicit_bind() } else { bind() },
        (0..4).map(|i| doc(i).encode()).collect(),
    )
    .await
    .expect("mapped ingest succeeds");
    let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
    client
        .flush(pipestream_search::pb::FlushRequest {})
        .await
        .unwrap();

    let handle = tokio::runtime::Handle::current();
    let analysis_addr = analysis.clone();
    let mut analyze = move |docs: &[(
        &str,
        Option<&pipestream_search::pb::AnalysisSpec>,
        pipestream_search::analyzer::SessionLayers,
    )]| {
        tokio::task::block_in_place(|| {
            handle.block_on(async {
                let mut out = Vec::with_capacity(docs.len());
                for (text, spec, layers) in docs {
                    let analyzed = if *layers != Default::default() {
                        pipestream_search::analyzer::analyze_batch(
                            &analysis_addr,
                            &[(*text, *spec, *layers)],
                        )
                        .await
                        .map(|mut batch| batch.remove(0))
                    } else {
                        pipestream_search::analyzer::analyze_document(&analysis_addr, text, *spec)
                            .await
                    };
                    out.push(analyzed.map_err(|e| e.to_string())?);
                }
                Ok(out)
            })
        })
    };
    let fields: Vec<String> = vec!["body".into(), "meta_author".into()];
    let gen =
        pipestream_search::reshard::resolve_gen(&pipestream_search::wal::wal_dir(&index_path))
            .unwrap();
    let source_binding = pipestream_search::reshard::read_generation_binding(&gen)
        .unwrap()
        .unwrap();
    assert_eq!(
        source_binding.analysis_sha.len(),
        if explicit { 64 } else { 0 }
    );
    let out_dir = dir.join("out");
    let output = pipestream_search::reshard::merge(
        &[gen],
        &out_dir,
        None,
        false,
        Some(&fields),
        &mut analyze,
    )
    .unwrap();
    assert_eq!(output.children.len(), 1);
    let child_bm25 = common::catalog::segment_bm25(&output.children[0].vector_path, 0);
    let child = pipestream_search::postings::Bm25Store::load(&child_bm25).unwrap();
    assert_eq!(child.doc_count(), 4);
    assert_eq!(child.binding(), Some(&source_binding));
    for row in 0..4 {
        let original = (0..4)
            .find(|i| child.text(row) == doc(*i).title.as_deref())
            .unwrap();
        assert_eq!(
            child.protobuf_source(row).unwrap(),
            Some((
                ProtobufSource {
                    descriptor_set: case_set(),
                    message_type: "law.v1.Case".into(),
                    payload: doc(original).encode(),
                },
                None
            ))
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}

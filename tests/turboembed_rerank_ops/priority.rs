//! Interactive Stage-2 reranking against concurrent bulk ingest on one
//! priority-enforcing analysis server: every streamed document and every
//! rerank call takes a unit of the server's single accelerator slot, and
//! the interactive lane is served first. Reranked queries over real serving
//! nodes and a coordinator finish while bulk ingest into other nodes on the
//! same analysis server is still running, and bulk ingest still completes.
//! Bulk ingest goes to nodes the coordinator does not query: a query over a
//! shard whose data changes mid-query is refused by design.

use super::*;
use pipestream_search::harness::mock_analysis::start_mock_analysis_prioritized;

const SERVING: usize = 2;
const BULK_NODES: usize = 12;
const BULK_DOCS: usize = 150;
const QUERIES: usize = 8;
const QUEUED_BULK: usize = 8;
const UNIT: Duration = Duration::from_millis(3);

/// A node holding two searchable documents and their vectors, analysed by
/// the server at `addr`.
async fn node(
    shard: usize,
    addr: &str,
    vectors: &[f32],
    calibration: &(Vec<f32>, Vec<f32>),
) -> Arc<NodeServiceImpl> {
    let node = Arc::new(NodeServiceImpl::new(
        None,
        NodeConfig {
            slot_offset: (shard * 10_000) as u64,
            analysis_addr: Some(addr.to_string()),
            sentence_fields: vec!["body".into()],
            ..Default::default()
        },
    ));
    let mut link = NodeLink::local(Arc::clone(&node));
    link.configure_vector_backend(pipestream_search::harness::embedded_backend_request(
        DIM,
        4,
        &calibration.0,
        &calibration.1,
    ))
    .await
    .unwrap();
    let documents = (0..2).map(|local| AddDocumentsRequest {
        text: format!("Shard {shard} record {local}. The appellate court affirmed jurisdiction."),
        analysis: Some(body_spec()),
        ..Default::default()
    });
    link.add_documents(tokio_stream::iter(documents.collect::<Vec<_>>()))
        .await
        .unwrap();
    let start = shard * 2 * DIM;
    link.add_vectors(tokio_stream::iter([AddVectorsRequest {
        materialized_rows: Vec::new(),
        vectors: vectors[start..start + 2 * DIM].to_vec(),
        dim: DIM as u32,
    }]))
    .await
    .unwrap();
    node
}

/// An empty node, outside the coordinator, analysed by the server at `addr`.
fn bulk_node(shard: usize, addr: &str) -> Arc<NodeServiceImpl> {
    Arc::new(NodeServiceImpl::new(
        None,
        NodeConfig {
            slot_offset: (shard * 10_000) as u64,
            analysis_addr: Some(addr.to_string()),
            ..Default::default()
        },
    ))
}

/// One bulk ingest stream of `BULK_DOCS` documents into `node`.
fn bulk_ingest(node: Arc<NodeServiceImpl>, shard: usize) -> tokio::task::JoinHandle<u64> {
    tokio::spawn(async move {
        let documents: Vec<_> = (0..BULK_DOCS)
            .map(|i| AddDocumentsRequest {
                text: format!("Bulk shard {shard} filing {i}. Maritime appeals docket entry."),
                analysis: Some(body_spec()),
                ..Default::default()
            })
            .collect();
        NodeLink::local(node)
            .add_documents(tokio_stream::iter(documents))
            .await
            .unwrap()
            .into_inner()
            .added
    })
}

/// A reranking coordinator over `nodes`, scoring on the interactive lane.
fn reranking(nodes: Vec<Arc<NodeServiceImpl>>, addr: &str) -> CoordinatorServiceImpl {
    let client = RemoteClient::new(
        RemoteEndpointConfig::new(addr.to_string()).with_priority(RemotePriority::Bulk),
    );
    let mut spec = RerankSpec::new(RerankBackend::Remote {
        client: Box::new(client),
        rerank_model: "bge-reranker-v2-m3".into(),
        embed_model: String::new(),
        qa_model: String::new(),
        temperature: 1.0,
    });
    spec.shortlist_size = 4;
    spec.highlight_top_k = 4;
    spec.query_text_override = Some("appellate jurisdiction".into());
    CoordinatorServiceImpl::with_local_nodes(nodes)
        .with_bm25(Some(addr.to_string()), Default::default())
        .with_rerank_spec(spec)
}

/// One reranked dense query: its hits and how long it took.
async fn reranked_query(coordinator: &CoordinatorServiceImpl, vector: &[f32]) -> Duration {
    let started = std::time::Instant::now();
    let response = SearchService::query(
        coordinator,
        Request::new(QueryRequest {
            k: 4,
            selection_k: 4,
            highlight: Some(HighlightSpec::default()),
            selection: Some(common::dense_selection(
                "vec",
                vector.to_vec(),
                DenseExecutionMode::Exhaustive as i32,
                0,
            )),
            ..Default::default()
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(
        response.executed.contains("turbo_rerank"),
        "{}",
        response.executed
    );
    assert!(!response.hits.is_empty());
    started.elapsed()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interactive_rerank_stays_bounded_under_concurrent_bulk_ingest() {
    let (addr, server, lanes) = start_mock_analysis_prioritized(UNIT).await;
    let vectors = unit_vectors(SERVING * 2, DIM, 0x207_0001);
    let calibration = fit_calibration(DIM, 4, &vectors);
    let mut serving = Vec::new();
    for shard in 0..SERVING {
        serving.push(node(shard, &addr, &vectors, &calibration).await);
    }
    let preloaded = lanes.stats().bulk_units;
    let coordinator = reranking(serving, &addr);
    let bulk: Vec<_> = (0..BULK_NODES)
        .map(|shard| bulk_ingest(bulk_node(shard, &addr), shard))
        .collect();
    lanes.wait_for_waiters(QUEUED_BULK, 0).await;

    for _ in 0..QUERIES {
        reranked_query(&coordinator, &vectors[..DIM]).await;
    }
    let during = lanes.stats();
    let bulk_total = preloaded + (BULK_NODES * BULK_DOCS) as u64;
    assert!(
        during.bulk_units < bulk_total,
        "every query finished while bulk ingest was still running: {during:?}"
    );
    assert!(during.interactive_units >= QUERIES as u64, "{during:?}");
    for task in bulk {
        assert_eq!(
            task.await.unwrap(),
            BULK_DOCS as u64,
            "bulk ingest completed"
        );
    }
    let after = lanes.stats();
    assert_eq!(after.bulk_units, bulk_total);
    assert!(during.max_units_before_interactive <= 1, "{during:?}");
    assert!(
        during.overtaken_bulk_waiters >= (QUERIES * QUEUED_BULK) as u64,
        "{during:?}"
    );
    server.abort();
}

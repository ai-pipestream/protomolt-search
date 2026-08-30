//! Exact conformance for the product's distributed TurboVec transports.

mod common;

use pipestream_search::clustered_turbovec::{ClusteredLabelFilter, ClusteredTurboVecBackend};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::harness;
use pipestream_search::node::NodeConfig;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    AddDocumentsRequest, ClusterHealthRequest, FacetValue, HybridSearchRequest, SearchRequest,
};
use pipestream_search::vector::{VectorIndex, VectorSearchOptions, EMBEDDED_TURBOVEC};
use tokio::sync::mpsc;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::transport::Server;
use tonic::Request;
use turbovec_grpc::proto::LabelBitmap;
use turbovec_grpc::{
    CoordinatorService, Index, IndexStore, NodeTable, ShardConfig, TurboVecService,
};

const DIM: usize = 64;
const BITS: usize = 4;
const ROWS: usize = 768;
const K: usize = 17;

async fn serve_node(index: turbovec::TurboQuantIndex, labels: Vec<u64>) -> (String, String) {
    let store = IndexStore::new();
    let index_id = store.insert_labelled(Index::Positional(index), labels);
    let service = TurboVecService::new(store);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        Server::builder()
            .add_service(service.into_query_server())
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    (address, index_id)
}

async fn serve_coordinator(service: CoordinatorService) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        Server::builder()
            .add_service(service.into_server())
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    address
}

async fn serve_product_filter_shard(
    analysis: &str,
    start: usize,
    end: usize,
) -> (
    String,
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
) {
    let (address, handle) = common::start_empty_node(NodeConfig {
        slot_offset: start as u64,
        analysis_addr: Some(analysis.to_string()),
        facet_fields: vec!["court".to_string()],
        ..Default::default()
    })
    .await;
    let mut client = NodeServiceClient::connect(address.clone()).await.unwrap();
    let (tx, rx) = mpsc::channel(16);
    tokio::spawn(async move {
        for id in start..end {
            tx.send(AddDocumentsRequest {
                text: format!("opinion {id}"),
                facets: vec![FacetValue {
                    field: "court".to_string(),
                    value: if id.is_multiple_of(2) {
                        "scotus".to_string()
                    } else {
                        "ca5".to_string()
                    },
                }],
                ..Default::default()
            })
            .await
            .unwrap();
        }
    });
    client.add_documents(ReceiverStream::new(rx)).await.unwrap();
    (address, handle)
}

fn bitmap(start: usize, end: usize, admitted: impl Fn(usize) -> bool) -> LabelBitmap {
    let label_count = end - start;
    let mut bits = vec![0; label_count.div_ceil(8)];
    for label in start..end {
        if admitted(label) {
            let local = label - start;
            bits[local / 8] |= 1 << (local % 8);
        }
    }
    LabelBitmap {
        base_label: start as u64,
        label_count: label_count as u64,
        bits,
    }
}

fn ranking(
    index: &VectorIndex,
    queries: &[f32],
    k: usize,
    allow: Option<&[bool]>,
) -> Vec<Vec<(u64, u32)>> {
    let mut options = VectorSearchOptions::new();
    if let Some(allow) = allow {
        options = options.with_allowlist(allow);
    }
    let result = index.search(queries, k, options);
    (0..result.query_count)
        .map(|query| {
            let mut hits: Vec<(u64, u32)> = result
                .slots_for_query(query)
                .iter()
                .zip(result.scores_for_query(query))
                .filter_map(|(&slot, &score)| (slot >= 0).then_some((slot as u64, score.to_bits())))
                .collect();
            hits.sort_by(|a, b| {
                f32::from_bits(b.1)
                    .total_cmp(&f32::from_bits(a.1))
                    .then_with(|| a.0.cmp(&b.0))
            });
            hits
        })
        .collect()
}

fn clustered_ranking(
    response: turbovec_grpc::proto::CollectionSearchResponse,
) -> Vec<Vec<(u64, u32)>> {
    response
        .results
        .into_iter()
        .map(|result| {
            result
                .neighbours
                .into_iter()
                .map(|hit| {
                    (
                        hit.label
                            .expect("product collections require stable labels"),
                        hit.score.to_bits(),
                    )
                })
                .collect()
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn embedded_in_process_and_external_transports_are_bit_exact() {
    let corpus = harness::unit_vectors(ROWS, DIM, 0xC105_7EED);
    let sample_rows = 512;
    let config =
        VectorIndex::fit_backend_config(EMBEDDED_TURBOVEC, DIM, BITS, &corpus[..sample_rows * DIM])
            .unwrap();
    let calibration = pipestream_search::vector::legacy_calibration_config(&config)
        .unwrap()
        .unwrap();

    let mut embedded = VectorIndex::from_backend_config(DIM, &config).unwrap();
    embedded.add(&corpus, DIM).unwrap();
    embedded.prepare().unwrap();

    let cuts = [0, 173, 491, ROWS];
    let mut shards = Vec::new();
    for pair in cuts.windows(2) {
        let start = pair[0];
        let end = pair[1];
        let mut index = turbovec::TurboQuantIndex::from_parts(
            Some(DIM),
            BITS,
            0,
            Vec::new(),
            Vec::new(),
            calibration.shift.clone(),
            calibration.scale.clone(),
        )
        .unwrap();
        index.add_2d(&corpus[start * DIM..end * DIM], DIM).unwrap();
        index.prepare();
        let labels = (start as u64..end as u64).collect();
        let (address, index_id) = serve_node(index, labels).await;
        shards.push(ShardConfig::with_index(address, index_id));
    }
    let table = NodeTable::new(shards);
    let in_process = ClusteredTurboVecBackend::in_process(CoordinatorService::new(table.clone()));
    let external_address = serve_coordinator(CoordinatorService::new(table)).await;
    let external =
        ClusteredTurboVecBackend::external(&external_address, pipestream_search::MAX_MESSAGE_BYTES)
            .unwrap();
    let local_health = in_process.health().await.unwrap();
    let remote_health = external.health().await.unwrap();
    assert!(local_health.servable, "{}", local_health.error);
    assert!(remote_health.servable, "{}", remote_health.error);
    assert_eq!(local_health.rows, ROWS as u64);
    assert_eq!(remote_health.rows, ROWS as u64);

    let queries = harness::unit_vectors(4, DIM, 0xC105_7E02);
    let expected = ranking(&embedded, &queries, K, None);
    let local = clustered_ranking(
        in_process
            .search(queries.clone(), K as u32, None, None, false)
            .await
            .unwrap(),
    );
    let remote = clustered_ranking(
        external
            .search(queries.clone(), K as u32, None, None, false)
            .await
            .unwrap(),
    );
    assert_eq!(local, expected);
    assert_eq!(remote, expected);

    let admitted = vec![3, 172, 173, 490, 491, 767];
    let mut mask = vec![false; ROWS];
    for &id in &admitted {
        mask[id as usize] = true;
    }
    let expected_filtered = ranking(&embedded, &queries, K, Some(&mask));
    let local_filtered = clustered_ranking(
        in_process
            .search(
                queries.clone(),
                K as u32,
                Some(ClusteredLabelFilter::Labels(admitted.clone())),
                None,
                false,
            )
            .await
            .unwrap(),
    );
    let remote_filtered = clustered_ranking(
        external
            .search(
                queries.clone(),
                K as u32,
                Some(ClusteredLabelFilter::Labels(admitted)),
                None,
                false,
            )
            .await
            .unwrap(),
    );
    assert_eq!(local_filtered, expected_filtered);
    assert_eq!(remote_filtered, expected_filtered);

    // Product shards need not share boundaries with vector shards. Their
    // packed filter segments still identify the same stable product rows.
    let product_cuts = [0, 129, 520, ROWS];
    let bitmaps: Vec<LabelBitmap> = product_cuts
        .windows(2)
        .map(|pair| bitmap(pair[0], pair[1], |label| label.is_multiple_of(2)))
        .collect();
    let mut even_mask = vec![false; ROWS];
    even_mask
        .iter_mut()
        .step_by(2)
        .for_each(|value| *value = true);
    let expected_bitmap = ranking(&embedded, &queries, K, Some(&even_mask));
    let local_bitmap = clustered_ranking(
        in_process
            .search(
                queries.clone(),
                K as u32,
                Some(ClusteredLabelFilter::Bitmaps(bitmaps.clone())),
                None,
                false,
            )
            .await
            .unwrap(),
    );
    let remote_bitmap = clustered_ranking(
        external
            .search(
                queries,
                K as u32,
                Some(ClusteredLabelFilter::Bitmaps(bitmaps)),
                None,
                false,
            )
            .await
            .unwrap(),
    );
    assert_eq!(local_bitmap, expected_bitmap);
    assert_eq!(remote_bitmap, expected_bitmap);

    let ties = clustered_ranking(
        in_process
            .search(vec![0.0; DIM], K as u32, None, None, false)
            .await
            .unwrap(),
    );
    assert_eq!(
        ties[0].iter().map(|hit| hit.0).collect::<Vec<_>>(),
        (0..K as u64).collect::<Vec<_>>()
    );

    let none = clustered_ranking(
        external
            .search(
                vec![0.0; DIM],
                K as u32,
                Some(ClusteredLabelFilter::Labels(Vec::new())),
                None,
                false,
            )
            .await
            .unwrap(),
    );
    assert!(none[0].is_empty());

    // The public product route resolves its own CEL columns into bitmaps. The
    // product and vector shard cuts deliberately differ above.
    let (analysis, _analysis_handle) = common::mock::start_mock_analysis().await;
    let mut product_nodes = Vec::new();
    let mut _product_handles = Vec::new();
    for pair in product_cuts.windows(2) {
        let (address, handle) = serve_product_filter_shard(&analysis, pair[0], pair[1]).await;
        product_nodes.push(address);
        _product_handles.push(handle);
    }
    let product = CoordinatorServiceImpl::new(product_nodes.clone())
        .with_max_k(K as u32)
        .with_clustered_turbovec(in_process.clone());
    let product_external = CoordinatorServiceImpl::new(product_nodes)
        .with_max_k(K as u32)
        .with_clustered_turbovec(external);
    let public_query = harness::unit_vectors(1, DIM, 0xC105_7E03);
    let expected_public = ranking(&embedded, &public_query, K, Some(&even_mask))[0].clone();
    let routed = SearchService::search(
        &product,
        Request::new(SearchRequest {
            request_id: "clustered-route".to_string(),
            k: K as u32,
            vector: public_query.clone(),
            collapse_parents: false,
            geo_filters: Vec::new(),
            filter: r#"court == "scotus""#.to_string(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(routed.request_id, "clustered-route");
    assert_eq!(
        routed
            .hits
            .iter()
            .map(|hit| (hit.vector_id, hit.score.to_bits()))
            .collect::<Vec<_>>(),
        expected_public
    );
    let routed_external = SearchService::search(
        &product_external,
        Request::new(SearchRequest {
            request_id: "clustered-route-external".to_string(),
            k: K as u32,
            vector: public_query.clone(),
            collapse_parents: false,
            geo_filters: Vec::new(),
            filter: r#"court == "scotus""#.to_string(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(
        routed_external
            .hits
            .iter()
            .map(|hit| (hit.vector_id, hit.score.to_bits()))
            .collect::<Vec<_>>(),
        expected_public
    );

    let no_matches = SearchService::search(
        &product,
        Request::new(SearchRequest {
            k: K as u32,
            vector: public_query.clone(),
            filter: r#"court == "nowhere""#.to_string(),
            ..Default::default()
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(no_matches.hits.is_empty());

    let unknown = SearchService::search(
        &product_external,
        Request::new(SearchRequest {
            k: K as u32,
            vector: public_query,
            filter: r#"kourt == "scotus""#.to_string(),
            ..Default::default()
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(unknown.code(), tonic::Code::InvalidArgument);
    assert!(unknown.message().contains("kourt"));

    let health = SearchService::cluster_health(&product, Request::new(ClusterHealthRequest {}))
        .await
        .unwrap()
        .into_inner()
        .clustered_vector
        .expect("cluster health reports the selected vector backend");
    assert_eq!(health.transport, "in-process");
    assert!(health.reachable && health.servable);
    assert_eq!(health.rows, ROWS as u64);

    let unsupported = SearchService::hybrid_search(
        &product,
        Request::new(HybridSearchRequest {
            text: "term".to_string(),
            vector: vec![0.0; DIM],
            k: K as u32,
            ..Default::default()
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(unsupported.code(), tonic::Code::Unimplemented);
    assert!(unsupported.message().contains("candidate-stream"));
}

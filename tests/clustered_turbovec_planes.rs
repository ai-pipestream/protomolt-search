//! Exact bit-plane conformance across embedded TurboVec, single-node `turbovec-grpc`,
//! and sharded `ClusteredTurboVecBackend` transports.
//!
//! Each shard holds at least 32,768 rows so hosts with bit-plane kernels exercise
//! multi-bit plane streaming on the coordinator and exhaustive chunked search on
//! both embedded and single-node surfaces.

use pipestream_search::clustered_turbovec::{ClusteredLabelFilter, ClusteredTurboVecBackend};
use pipestream_search::harness;
use pipestream_search::vector::{
    embedded_turbovec_calibration, VectorIndex, VectorSearchOptions, EMBEDDED_TURBOVEC,
};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use turbovec_grpc::proto::turbo_vec_query_client::TurboVecQueryClient;
use turbovec_grpc::proto::{CollectionSearchResponse, LabelBitmap, SearchRequest};
use turbovec_grpc::{
    CoordinatorService, Index, IndexStore, NodeTable, ShardConfig, TurboVecService,
};

const DIM: usize = 64;
const K: usize = 100;
const QUERY_COUNT: usize = 3;
const CALIBRATION_SAMPLE_ROWS: usize = 512;
const SHARD_ZERO_ROWS: usize = 32_800;
const TOTAL_ROWS: usize = 65_605;

type Ranking = Vec<Vec<(u64, u32)>>;

struct PlanesCluster {
    embedded: VectorIndex,
    shard_zero_embedded: VectorIndex,
    shard_zero_address: String,
    shard_zero_id: String,
    in_process: ClusteredTurboVecBackend,
    external: ClusteredTurboVecBackend,
}

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

fn embedded_ranking(
    index: &VectorIndex,
    queries: &[f32],
    k: usize,
    allow: Option<&[bool]>,
) -> Ranking {
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

fn clustered_ranking(response: CollectionSearchResponse) -> Ranking {
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

async fn node_unary_ranking(address: &str, index_id: &str, queries: &[f32], k: usize) -> Ranking {
    let mut client = TurboVecQueryClient::connect(address.to_string())
        .await
        .unwrap();
    let response = client
        .search(SearchRequest {
            index_id: index_id.to_string(),
            queries: queries.to_vec(),
            k: k as u32,
            allowlist: Vec::new(),
        })
        .await
        .unwrap()
        .into_inner();
    response
        .results
        .into_iter()
        .map(|row| {
            let mut hits: Vec<(u64, u32)> = row
                .ids
                .into_iter()
                .zip(row.scores)
                .map(|(slot, score)| (slot, score.to_bits()))
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

fn build_bitmap(start: usize, end: usize, admitted: impl Fn(usize) -> bool) -> LabelBitmap {
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

async fn build_planes_cluster(corpus: &[f32], bit_width: usize) -> PlanesCluster {
    let config = VectorIndex::fit_backend_config(
        EMBEDDED_TURBOVEC,
        DIM,
        bit_width,
        &corpus[..CALIBRATION_SAMPLE_ROWS * DIM],
    )
    .unwrap();
    let calibration = embedded_turbovec_calibration(&config).unwrap().unwrap();

    let mut embedded = VectorIndex::from_backend_config(DIM, &config).unwrap();
    embedded.add(corpus, DIM).unwrap();
    embedded.prepare().unwrap();

    let mut shard_zero_embedded = VectorIndex::from_backend_config(DIM, &config).unwrap();
    shard_zero_embedded
        .add(&corpus[..SHARD_ZERO_ROWS * DIM], DIM)
        .unwrap();
    shard_zero_embedded.prepare().unwrap();

    let cuts = [0, SHARD_ZERO_ROWS, TOTAL_ROWS];
    let mut shards = Vec::new();
    let mut shard_zero_target = None;
    for pair in cuts.windows(2) {
        let (start, end) = (pair[0], pair[1]);
        let mut index = turbovec::TurboQuantIndex::from_parts(
            Some(DIM),
            bit_width,
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
        if shard_zero_target.is_none() {
            shard_zero_target = Some((address.clone(), index_id.clone()));
        }
        shards.push(ShardConfig::with_index(address, index_id));
    }

    let table = NodeTable::new(shards);
    let in_process = ClusteredTurboVecBackend::in_process(CoordinatorService::new(table.clone()));
    let external_address = serve_coordinator(CoordinatorService::new(table)).await;
    let external =
        ClusteredTurboVecBackend::external(&external_address, pipestream_search::MAX_MESSAGE_BYTES)
            .unwrap();
    let (shard_zero_address, shard_zero_id) = shard_zero_target.unwrap();

    PlanesCluster {
        embedded,
        shard_zero_embedded,
        shard_zero_address,
        shard_zero_id,
        in_process,
        external,
    }
}

async fn assert_unfiltered_and_node(cluster: &PlanesCluster, queries: &[f32], bit_width: usize) {
    let shard_expected = embedded_ranking(&cluster.shard_zero_embedded, queries, K, None);
    let shard_actual = node_unary_ranking(
        &cluster.shard_zero_address,
        &cluster.shard_zero_id,
        queries,
        K,
    )
    .await;
    assert_eq!(
        shard_actual, shard_expected,
        "bit_width {bit_width} node unary"
    );

    let expected = embedded_ranking(&cluster.embedded, queries, K, None);
    let local = clustered_ranking(
        cluster
            .in_process
            .search(queries.to_vec(), K as u32, None, None, false)
            .await
            .unwrap(),
    );
    let remote = clustered_ranking(
        cluster
            .external
            .search(queries.to_vec(), K as u32, None, None, false)
            .await
            .unwrap(),
    );
    assert_eq!(
        local, expected,
        "bit_width {bit_width} in-process unfiltered"
    );
    assert_eq!(
        remote, expected,
        "bit_width {bit_width} external unfiltered"
    );
}

async fn assert_filtered_surfaces(cluster: &PlanesCluster, queries: &[f32], bit_width: usize) {
    let admitted: Vec<u64> = (0..240).map(|step| (step * 273) as u64).collect();
    let mut allow_mask = vec![false; TOTAL_ROWS];
    for &label in &admitted {
        allow_mask[label as usize] = true;
    }
    let expected_labels = embedded_ranking(&cluster.embedded, queries, K, Some(&allow_mask));
    let local_labels = clustered_ranking(
        cluster
            .in_process
            .search(
                queries.to_vec(),
                K as u32,
                Some(ClusteredLabelFilter::Labels(admitted.clone())),
                None,
                false,
            )
            .await
            .unwrap(),
    );
    let remote_labels = clustered_ranking(
        cluster
            .external
            .search(
                queries.to_vec(),
                K as u32,
                Some(ClusteredLabelFilter::Labels(admitted)),
                None,
                false,
            )
            .await
            .unwrap(),
    );
    assert_eq!(
        local_labels, expected_labels,
        "bit_width {bit_width} label list"
    );
    assert_eq!(
        remote_labels, expected_labels,
        "bit_width {bit_width} remote label list"
    );

    let product_cuts = [0, 20_113, 47_999, TOTAL_ROWS];
    let bitmaps: Vec<LabelBitmap> = product_cuts
        .windows(2)
        .map(|pair| build_bitmap(pair[0], pair[1], |label| label % 3 == 0))
        .collect();
    let mut bitmap_mask = vec![false; TOTAL_ROWS];
    bitmap_mask
        .iter_mut()
        .step_by(3)
        .for_each(|admitted| *admitted = true);
    let expected_bitmaps = embedded_ranking(&cluster.embedded, queries, K, Some(&bitmap_mask));
    let local_bitmaps = clustered_ranking(
        cluster
            .in_process
            .search(
                queries.to_vec(),
                K as u32,
                Some(ClusteredLabelFilter::Bitmaps(bitmaps.clone())),
                None,
                false,
            )
            .await
            .unwrap(),
    );
    let remote_bitmaps = clustered_ranking(
        cluster
            .external
            .search(
                queries.to_vec(),
                K as u32,
                Some(ClusteredLabelFilter::Bitmaps(bitmaps)),
                None,
                false,
            )
            .await
            .unwrap(),
    );
    assert_eq!(
        local_bitmaps, expected_bitmaps,
        "bit_width {bit_width} bitmaps"
    );
    assert_eq!(
        remote_bitmaps, expected_bitmaps,
        "bit_width {bit_width} remote bitmaps"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bit_plane_shards_agree_across_embedded_node_and_clustered_transports() {
    let corpus = harness::unit_vectors(TOTAL_ROWS, DIM, 0x5EED_CAFE);
    let queries = harness::unit_vectors(QUERY_COUNT, DIM, 0x5EED_BEEF);
    for bit_width in [4, 2] {
        let cluster = build_planes_cluster(&corpus, bit_width).await;
        assert_unfiltered_and_node(&cluster, &queries, bit_width).await;
        assert_filtered_surfaces(&cluster, &queries, bit_width).await;
    }
}

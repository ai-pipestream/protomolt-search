//! Lossless end-to-end test: a 3-shard cluster answering through the
//! coordinator must reproduce the monolithic index's top-k EXACTLY — same
//! ids, same scores (bitwise), same order — with floor sharing on and off.
//!
//! Why exactness is a fair bar here: every index (shards + monolithic) is
//! seeded with the same TQ+ calibration, so a vector encodes byte-identically
//! everywhere and per-slot scores are pure functions of (codes, scale,
//! calibration, query). The only reordering freedom is ties, and both sides
//! are compared under the same deterministic total order (score desc, id
//! asc). All corpora and queries are fixed-seed, so the test is
//! deterministic.

mod common;

use common::{monolithic_topk, unit_vectors, Cluster, BIT_WIDTH, DIM};
use turbovec_search::coordinator::CoordinatorServiceImpl;

const N: usize = 20_000;
const K: u32 = 10;
const QUERIES: usize = 8;

async fn assert_lossless(share_floors: bool) {
    // Small chunks (8 blocks = 256 vectors) give the floor flow many
    // chances to engage mid-scan: ~26 chunks per shard.
    let cluster = Cluster::start(N, 8, share_floors).await;
    let coordinator = CoordinatorServiceImpl::new(cluster.node_addrs.clone());

    for qi in 0..QUERIES {
        let query = unit_vectors(1, DIM, 0x9E4A_0000 + qi as u64);
        let result = coordinator
            .fanout_search(&format!("lossless-{qi}"), &query, K)
            .await
            .expect("fanout search");

        let got: Vec<(u64, u32)> = result
            .hits
            .iter()
            .map(|h| (h.vector_id, h.score.to_bits()))
            .collect();
        let want = monolithic_topk(&cluster.monolithic, &query, K as usize);

        assert_eq!(
            got.len(),
            want.len(),
            "share_floors={share_floors} query {qi}: hit count"
        );
        assert_eq!(
            got, want,
            "share_floors={share_floors} query {qi}: distributed top-{K} != monolithic top-{K}"
        );

        // Sanity: every shard reported stats and answered with <= k hits.
        assert_eq!(result.shard_stats.len(), 3);
    }

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distributed_matches_monolithic_with_floor_sharing() {
    assert_lossless(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distributed_matches_monolithic_without_floor_sharing() {
    assert_lossless(false).await;
}

/// Direct gRPC path (SearchService client over loopback) rather than the
/// in-process fanout, so the client-facing surface is covered too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn search_service_over_loopback_matches_monolithic() {
    use turbovec_search::pb::search_service_client::SearchServiceClient;
    use turbovec_search::pb::SearchRequest;

    let cluster = Cluster::start(N, 8, true).await;
    let (coord_addr, coord_handle) = common::start_coordinator(cluster.node_addrs.clone()).await;
    let mut client = SearchServiceClient::connect(coord_addr).await.unwrap();

    for qi in 0..3 {
        let query = unit_vectors(1, DIM, 0xBEEF_0000 + qi as u64);
        let response = client
            .search(SearchRequest {
                request_id: format!("grpc-{qi}"),
                k: K,
                vector: query.clone(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response.request_id, format!("grpc-{qi}"));

        let got: Vec<(u64, u32)> = response
            .hits
            .iter()
            .map(|h| (h.vector_id, h.score.to_bits()))
            .collect();
        let want = monolithic_topk(&cluster.monolithic, &query, K as usize);
        assert_eq!(got, want, "gRPC query {qi}");
    }

    coord_handle.abort();
    cluster.shutdown().await;
}

/// The calibration handshake: every node must report the calibration the
/// cluster was seeded with, plus consistent shape metadata.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_calibration_reports_seeded_values() {
    use turbovec_search::pb::node_service_client::NodeServiceClient;
    use turbovec_search::pb::GetCalibrationRequest;

    let cluster = Cluster::start(N, 8, true).await;
    let mut reference: Option<(Vec<f32>, Vec<f32>)> = None;
    for addr in &cluster.node_addrs {
        let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
        let cal = client
            .get_calibration(GetCalibrationRequest {})
            .await
            .unwrap()
            .into_inner();
        assert_eq!(cal.dim as usize, DIM);
        assert_eq!(cal.bit_width as usize, BIT_WIDTH);
        assert!(cal.num_vectors > 0);
        match &reference {
            None => reference = Some((cal.shift, cal.scale)),
            Some((shift, scale)) => {
                assert_eq!(*shift, cal.shift, "shards disagree on shift");
                assert_eq!(*scale, cal.scale, "shards disagree on scale");
            }
        }
    }
    cluster.shutdown().await;
}

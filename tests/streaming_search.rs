//! Streaming-search lossless gate: `fanout_stream_search` — shards hold
//! no top-k and emit at or above the coordinator's relayed floor — must
//! reproduce the monolithic index's top-k EXACTLY (bitwise scores, same
//! total order) and agree with the collaborative fan-out on the same
//! cluster, with every shard's terminal summary certifying a completed
//! scan.
//!
//! Two cluster shapes: the seeded default cluster (one 8192-row
//! calibration block per shard — a single batch per shard, floors moot)
//! and an unseeded multi-block cluster (1024-row blocks, whole blocks
//! everywhere, so per-block refits are content-identical across the
//! sharded and monolithic builds and bitwise equality holds with no
//! seed at all). Floor RAISES mid-scan are timing-dependent (a fast
//! shard can finish before the first raise lands), so their volume
//! effect is asserted only where it is deterministic: the initial
//! floor, which binds from block 0.

mod common;

use common::{monolithic_topk, start_node, unit_vectors, Cluster, BIT_WIDTH, DIM};
use turbovec::TurboQuantIndex;
use turbovec_search::coordinator::CoordinatorServiceImpl;
use turbovec_search::node::NodeConfig;

/// 1024-row calibration blocks: big enough for honest per-block fits
/// (>= ~1000 samples), small enough that every shard holds 8 blocks and
/// the stream carries real per-block batches.
const BLOCK: usize = 1024;
const SHARD_ROWS: usize = 8192;
const N_SHARDS: usize = 3;
const N: usize = N_SHARDS * SHARD_ROWS;
const K: u32 = 10;

/// Three unseeded shard nodes over whole 1024-row blocks, plus the
/// monolithic reference built the same way. Whole blocks mean every
/// sealed block's fit is a pure function of exactly the rows the
/// monolithic build seals together, so encodings agree bitwise without
/// any calibration seed.
async fn multi_block_cluster() -> (Vec<String>, TurboQuantIndex) {
    let corpus = unit_vectors(N, DIM, 0x5EED_B10C);
    let mut addrs = Vec::new();
    for shard in 0..N_SHARDS {
        let mut index = TurboQuantIndex::with_block_size(DIM, BIT_WIDTH, BLOCK).unwrap();
        index.add(&corpus[shard * SHARD_ROWS * DIM..(shard + 1) * SHARD_ROWS * DIM]);
        index.prepare();
        let (addr, _handle) = start_node(
            index,
            NodeConfig {
                slot_offset: (shard * SHARD_ROWS) as u64,
                ..Default::default()
            },
        )
        .await;
        addrs.push(addr);
    }
    let mut monolithic = TurboQuantIndex::with_block_size(DIM, BIT_WIDTH, BLOCK).unwrap();
    monolithic.add(&corpus);
    monolithic.prepare();
    (addrs, monolithic)
}

fn bits(hits: &[turbovec_search::pb::ScoredHit]) -> Vec<(u64, u32)> {
    hits.iter()
        .map(|h| (h.vector_id, h.score.to_bits()))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn streaming_matches_monolithic_and_fanout_exactly() {
    let (addrs, monolithic) = multi_block_cluster().await;
    let coordinator = CoordinatorServiceImpl::new(addrs);

    for qi in 0..6u64 {
        let query = unit_vectors(1, DIM, 0x57AE_0000 + qi);
        let streamed = coordinator
            .fanout_stream_search(&format!("stream-{qi}"), &query, K, None)
            .await
            .expect("stream fanout");
        let got = bits(&streamed.hits);

        let want = monolithic_topk(&monolithic, &query, K as usize);
        assert_eq!(got, want, "query {qi}: streaming != monolithic");

        let fanout = coordinator
            .fanout_search(&format!("classic-{qi}"), &query, K, false)
            .await
            .expect("classic fanout");
        assert_eq!(
            got,
            bits(&fanout.hits),
            "query {qi}: streaming != collaborative fan-out"
        );

        assert_eq!(streamed.summaries.len(), N_SHARDS);
        for (shard, summary) in streamed.summaries.iter().enumerate() {
            assert!(summary.completed, "query {qi}: shard {shard} not completed");
            assert_eq!(
                summary.blocks_scanned,
                (SHARD_ROWS / BLOCK) as u64,
                "query {qi}: shard {shard} block count"
            );
        }
        // The heap fills immediately (the first batch alone holds far
        // more than k candidates), so a floor is always broadcast.
        assert!(streamed.floors_sent > 0, "query {qi}: no floor broadcast");
    }
}

/// The seeded default cluster (one whole 8192-row block per shard):
/// streaming answers bitwise-identically to the collaborative fan-out
/// and the monolithic reference there too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn streaming_matches_on_seeded_default_cluster() {
    let cluster = Cluster::start(24_576, 8, true).await;
    let coordinator = CoordinatorServiceImpl::new(cluster.node_addrs.clone());

    for qi in 0..3u64 {
        let query = unit_vectors(1, DIM, 0x5EED_57AE + qi);
        let streamed = coordinator
            .fanout_stream_search(&format!("dstream-{qi}"), &query, K, None)
            .await
            .expect("stream fanout");
        let got = bits(&streamed.hits);
        assert_eq!(
            got,
            monolithic_topk(&cluster.monolithic, &query, K as usize),
            "query {qi}: streaming != monolithic"
        );
        let fanout = coordinator
            .fanout_search(&format!("dclassic-{qi}"), &query, K, false)
            .await
            .expect("classic fanout");
        assert_eq!(got, bits(&fanout.hits), "query {qi}: streaming != fan-out");
        assert!(streamed.summaries.iter().all(|s| s.completed));
    }
    cluster.shutdown().await;
}

/// The initial floor binds from block 0 on every shard, so its effect
/// is deterministic: seeding with the k-th best score of an unfloored
/// run returns the identical top-k (ties at the floor survive) while
/// emitting a fraction of the corpus.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn initial_floor_is_deterministic_and_lossless() {
    let (addrs, _monolithic) = multi_block_cluster().await;
    let coordinator = CoordinatorServiceImpl::new(addrs);
    let query = unit_vectors(1, DIM, 0xF100_0001);

    let unfloored = coordinator
        .fanout_stream_search("floor-base", &query, K, None)
        .await
        .expect("unfloored stream");
    let kth = unfloored
        .hits
        .last()
        .expect("k results on a dense corpus")
        .score;

    let floored = coordinator
        .fanout_stream_search("floor-seeded", &query, K, Some(kth))
        .await
        .expect("floored stream");
    assert_eq!(
        bits(&unfloored.hits),
        bits(&floored.hits),
        "a floor at the true k-th best must not change the result"
    );
    let emitted: u64 = floored.summaries.iter().map(|s| s.emitted).sum();
    let baseline: u64 = unfloored.summaries.iter().map(|s| s.emitted).sum();
    assert!(
        emitted < baseline / 4,
        "floor at k-th best barely filtered: {emitted} of {baseline}"
    );
    // Every emission respected the floor.
    assert!(emitted >= u64::from(K));
}

/// Protocol errors: a stream that does not open with Start is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stream_without_start_is_refused() {
    use turbovec_search::pb::node_service_client::NodeServiceClient;
    use turbovec_search::pb::{stream_search_request, FloorUpdate, StreamSearchRequest};

    let (addrs, _monolithic) = multi_block_cluster().await;
    let mut client = NodeServiceClient::connect(addrs[0].clone()).await.unwrap();
    let outbound = tokio_stream::iter(vec![StreamSearchRequest {
        payload: Some(stream_search_request::Payload::FloorUpdate(FloorUpdate {
            floor: 0.5,
        })),
    }]);
    let mut inbound = client.stream_search(outbound).await.unwrap().into_inner();
    let first = inbound.message().await;
    assert!(
        first.is_err(),
        "a stream opened without Start must error, got {first:?}"
    );
}

//! NodeService over loopback: a mid-scan floor update injected by the
//! client must leave the final shard top-k identical to an uninjected scan
//! (lossless under reactive updates), while provably pruning candidates.

mod common;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use turbovec_search::node::NodeConfig;
use turbovec_search::pb::node_service_client::NodeServiceClient;
use turbovec_search::pb::{
    search_shard_request, search_shard_response, FloorUpdate, SearchShardDone, SearchShardRequest,
    StartShardSearch,
};

use common::{fit_calibration, unit_vectors, DIM};

const N: usize = 10_000;
const K: u32 = 10;

struct ScanOutcome {
    done: SearchShardDone,
    floor_updates_seen: usize,
}

/// Drive one SearchShard stream manually: send Start, optionally send one
/// FloorUpdate immediately after (it lands mid-scan because the node scans
/// in chunks), then collect responses until Done.
async fn drive_scan(addr: &str, query: &[f32], inject_floor: Option<f32>) -> ScanOutcome {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let (tx, rx) = mpsc::channel::<SearchShardRequest>(8);
    tx.send(SearchShardRequest {
        payload: Some(search_shard_request::Payload::Start(StartShardSearch {
            request_id: "loopback".to_string(),
            k: K,
            vector: query.to_vec(),
            tie_complete: false,
        })),
    })
    .await
    .unwrap();
    if let Some(floor) = inject_floor {
        tx.send(SearchShardRequest {
            payload: Some(search_shard_request::Payload::FloorUpdate(FloorUpdate {
                floor,
            })),
        })
        .await
        .unwrap();
    }

    let mut responses = client
        .search_shard(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();

    let mut floor_updates_seen = 0;
    loop {
        match responses.message().await.unwrap() {
            Some(search_shard_response) => match search_shard_response.payload {
                Some(search_shard_response::Payload::FloorUpdate(_)) => {
                    floor_updates_seen += 1;
                }
                Some(search_shard_response::Payload::Done(done)) => {
                    return ScanOutcome {
                        done,
                        floor_updates_seen,
                    };
                }
                None => panic!("empty response payload"),
            },
            None => panic!("stream closed before Done"),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mid_scan_floor_update_is_lossless() {
    let corpus = unit_vectors(N, DIM, 0x10AD_0001);
    let (shift, scale) = fit_calibration(DIM, 4, &corpus[..2_000 * DIM]);
    let mut index =
        turbovec::TurboQuantIndex::new_with_calibration(DIM, 4, &shift, &scale).unwrap();
    index.add(&corpus);
    index.prepare();

    let query = unit_vectors(1, DIM, 0x10AD_0002);
    // The shard's true k-th best is a lossless floor.
    let true_kth = index.search(&query, K as usize).scores_for_query(0)[(K - 1) as usize];

    // chunk_blocks=2 → 64 vectors per chunk → ~157 chunks: the injected
    // floor is guaranteed to land before most chunks run.
    let (addr, handle) = common::start_node(
        index,
        NodeConfig {
            slot_offset: 0,
            chunk_blocks: 2,
            share_floors: true,
            ..Default::default()
        },
    )
    .await;

    let baseline = drive_scan(&addr, &query, None).await;
    let injected = drive_scan(&addr, &query, Some(true_kth)).await;

    let hits_of = |done: &SearchShardDone| {
        done.hits
            .iter()
            .map(|h| (h.vector_id, h.score.to_bits()))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        hits_of(&baseline.done),
        hits_of(&injected.done),
        "mid-scan floor changed the shard top-k"
    );

    let base_stats = baseline.done.stats.unwrap();
    let inj_stats = injected.done.stats.unwrap();
    assert!(
        inj_stats.candidates_collected < base_stats.candidates_collected,
        "injected floor should prune: {} vs {}",
        inj_stats.candidates_collected,
        base_stats.candidates_collected
    );
    assert!(
        inj_stats.floor_updates_applied > 0,
        "injected floor was never applied to a chunk"
    );
    assert!(
        baseline.floor_updates_seen > 0,
        "node never published its own floor"
    );

    handle.abort();
}

/// A floor ABOVE the true k-th best is out of contract (lossy by design):
/// the node must not fabricate candidates — hits are the floor-filtered
/// set, possibly fewer than k.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overhigh_floor_yields_fewer_hits() {
    let corpus = unit_vectors(4_096, DIM, 0x10AD_0003);
    let (shift, scale) = fit_calibration(DIM, 4, &corpus[..1_024 * DIM]);
    let mut index =
        turbovec::TurboQuantIndex::new_with_calibration(DIM, 4, &shift, &scale).unwrap();
    index.add(&corpus);

    let query = unit_vectors(1, DIM, 0x10AD_0004);
    let best = index.search(&query, 1).scores_for_query(0)[0];

    let (addr, handle) = common::start_node(
        index,
        NodeConfig {
            slot_offset: 0,
            chunk_blocks: 2,
            share_floors: true,
            ..Default::default()
        },
    )
    .await;

    let outcome = drive_scan(&addr, &query, Some(best + 1.0)).await;
    assert!(
        outcome.done.hits.is_empty(),
        "floor above every score must yield no hits"
    );

    handle.abort();
}

/// Sixteen concurrent searches through the coalescing scheduler
/// (scan_parallel=1, so every query after the first must share batches
/// with its neighbors) return exactly the hits a non-coalescing node
/// returns for the same queries — and the batch counters prove
/// multi-query batches actually formed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_coalesced_scans_match_solo() {
    let corpus = unit_vectors(N, DIM, 0x10AD_0007);
    let (shift, scale) = fit_calibration(DIM, 4, &corpus[..2_000 * DIM]);
    let build = || {
        let mut index =
            turbovec::TurboQuantIndex::new_with_calibration(DIM, 4, &shift, &scale).unwrap();
        index.add(&corpus);
        index.prepare();
        index
    };
    let (solo_addr, solo_handle) = common::start_node(
        build(),
        NodeConfig {
            coalesce: false,
            ..Default::default()
        },
    )
    .await;
    // chunk_blocks=2 keeps each scan slow enough that concurrent arrivals
    // genuinely queue behind the single scan slot.
    let (batched_addr, batched_handle) = common::start_node(
        build(),
        NodeConfig {
            coalesce: true,
            scan_parallel: 1,
            chunk_blocks: 2,
            ..Default::default()
        },
    )
    .await;

    let queries: Vec<Vec<f32>> = (0..16)
        .map(|i| unit_vectors(1, DIM, 0xC0A1_0000 + i))
        .collect();
    let mut solo = Vec::new();
    for query in &queries {
        solo.push(drive_scan(&solo_addr, query, None).await.done);
    }

    let (batches_before, jobs_before) = turbovec_search::node::scan_batch_counters();
    let tasks: Vec<_> = queries
        .iter()
        .map(|query| {
            let addr = batched_addr.clone();
            let query = query.clone();
            tokio::spawn(async move { drive_scan(&addr, &query, None).await.done })
        })
        .collect();
    let mut batched = Vec::new();
    for task in tasks {
        batched.push(task.await.unwrap());
    }
    let (batches_after, jobs_after) = turbovec_search::node::scan_batch_counters();

    let hits_of = |done: &SearchShardDone| {
        done.hits
            .iter()
            .map(|h| (h.vector_id, h.score.to_bits()))
            .collect::<Vec<_>>()
    };
    for (i, (s, b)) in solo.iter().zip(&batched).enumerate() {
        assert_eq!(hits_of(s), hits_of(b), "query {i} differs under coalescing");
    }
    let jobs = jobs_after - jobs_before;
    let batches = batches_after - batches_before;
    assert!(
        jobs > batches,
        "no multi-query batch formed ({jobs} jobs in {batches} batches)"
    );

    solo_handle.abort();
    batched_handle.abort();
}

//! Skipped-work benchmark: measures what floor sharing saves.
//!
//! Metric: `candidates_collected` — every real (non-padding) candidate the
//! per-chunk searches returned, summed over shards and queries. turbovec's
//! scan kernel does not expose a block-skip counter (that would take a fork
//! patch), so the candidate count is the cleanest kernel-visible proxy for
//! skipped work: a raised floor both prefilter-skips whole blocks AND
//! shrinks the collected-candidate set, and the latter is exactly
//! countable through the existing API. Wall-time medians over 50 queries
//! are reported alongside as a sanity signal, not asserted (loopback
//! in-process timing is noisy at this corpus size).

mod common;

use std::time::Instant;

use turbovec_search::coordinator::CoordinatorServiceImpl;

use common::{unit_vectors, Cluster, DIM};

const N: usize = 60_000;
const K: u32 = 10;
const QUERIES: usize = 50;
const CHUNK_BLOCKS: usize = 4; // 128 vectors/chunk → ~157 chunks per shard

struct BenchRun {
    wall_times_ms: Vec<f64>,
    candidates: u64,
    floors_published: u64,
    floor_updates_applied: u64,
    hits: Vec<Vec<(u64, u32)>>,
}

async fn bench(share_floors: bool) -> BenchRun {
    let cluster = Cluster::start(N, CHUNK_BLOCKS, share_floors).await;
    let coordinator = CoordinatorServiceImpl::new(cluster.node_addrs.clone());

    let mut run = BenchRun {
        wall_times_ms: Vec::with_capacity(QUERIES),
        candidates: 0,
        floors_published: 0,
        floor_updates_applied: 0,
        hits: Vec::with_capacity(QUERIES),
    };
    for qi in 0..QUERIES {
        let query = unit_vectors(1, DIM, 0xB300_0000 + qi as u64);
        let start = Instant::now();
        let result = coordinator
            .fanout_search(&format!("bench-{qi}"), &query, K)
            .await
            .expect("fanout search");
        run.wall_times_ms.push(start.elapsed().as_secs_f64() * 1e3);
        for stats in result.shard_stats.iter().flatten() {
            run.candidates += stats.candidates_collected;
            run.floors_published += stats.floors_published;
            run.floor_updates_applied += stats.floor_updates_applied;
        }
        run.hits.push(
            result
                .hits
                .iter()
                .map(|h| (h.vector_id, h.score.to_bits()))
                .collect(),
        );
    }

    cluster.shutdown().await;
    run
}

fn median(values: &mut [f64]) -> f64 {
    values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    values[values.len() / 2]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn floor_sharing_skips_work() {
    let off = bench(false).await;
    let on = bench(true).await;

    // Lossless under sharing: identical hit sequences for every query.
    assert_eq!(off.hits, on.hits, "floor sharing changed results");

    let off_median = median(&mut off.wall_times_ms.clone());
    let on_median = median(&mut on.wall_times_ms.clone());
    eprintln!("=== floor-sharing benchmark (N={N}, 3 shards, k={K}, {QUERIES} queries, chunk={CHUNK_BLOCKS} blocks) ===");
    eprintln!(
        "sharing OFF: candidates={} median wall={off_median:.3}ms",
        off.candidates
    );
    eprintln!(
        "sharing ON:  candidates={} median wall={on_median:.3}ms",
        on.candidates
    );
    eprintln!(
        "candidate reduction: {:.1}%",
        100.0 * (1.0 - on.candidates as f64 / off.candidates as f64)
    );
    eprintln!(
        "floors published={} applied-to-chunks={}",
        on.floors_published, on.floor_updates_applied
    );

    // Sharing can only prune, never add.
    assert!(
        on.candidates <= off.candidates,
        "sharing collected more candidates: {} > {}",
        on.candidates,
        off.candidates
    );
    // The mechanism actually engaged: floors flowed and pruned.
    assert!(on.floors_published > 0, "no floors were published");
    assert!(
        on.floor_updates_applied > 0,
        "no chunk ran with a shared floor"
    );
    assert!(
        on.candidates < off.candidates,
        "sharing collected no fewer candidates ({} vs {})",
        on.candidates,
        off.candidates
    );
}

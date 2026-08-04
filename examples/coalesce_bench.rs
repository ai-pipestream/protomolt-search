//! A/B: concurrent shard searches with and without request coalescing.
//!
//! Builds an n-vector 4-bit index twice with one calibration (identical
//! codes), serves it from an in-process node per config, and hammers each
//! with C concurrent clients running Q sequential queries apiece. The
//! scan is memory-bandwidth-bound, so the coalesced node's win is queries
//! sharing each pass over the packed codes (up to 4 per kernel call);
//! results are bitwise identical either way (gated by the test suite).
//!
//! ```text
//! cargo run --release --example coalesce_bench -- --n=1000000 --clients=1,4,16,32
//! ```

use std::time::Instant;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use turbovec_search::harness::{fit_calibration, start_node, unit_vectors};
use turbovec_search::node::{scan_batch_counters, NodeConfig};
use turbovec_search::pb::node_service_client::NodeServiceClient;
use turbovec_search::pb::{
    search_shard_request, search_shard_response, SearchShardRequest, StartShardSearch,
};

fn opt(args: &[String], name: &str) -> Option<String> {
    let prefix = format!("--{name}=");
    args.iter()
        .find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
}

async fn one_query(addr: &str, vector: Vec<f32>, k: u32) -> usize {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let (tx, rx) = mpsc::channel::<SearchShardRequest>(2);
    tx.send(SearchShardRequest {
        payload: Some(search_shard_request::Payload::Start(StartShardSearch {
            request_id: String::new(),
            k,
            vector,
            tie_complete: false,
            collapse_parents: false,
        })),
    })
    .await
    .unwrap();
    drop(tx);
    let mut responses = client
        .search_shard(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
    loop {
        match responses.message().await.unwrap() {
            Some(r) => {
                if let Some(search_shard_response::Payload::Done(done)) = r.payload {
                    return done.hits.len();
                }
            }
            None => panic!("stream closed before Done"),
        }
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let n: usize = opt(&args, "n").map_or(1_000_000, |s| s.parse().unwrap());
    let dim: usize = opt(&args, "dim").map_or(256, |s| s.parse().unwrap());
    let k: u32 = opt(&args, "k").map_or(10, |s| s.parse().unwrap());
    let per_client: usize = opt(&args, "queries").map_or(8, |s| s.parse().unwrap());
    let client_counts: Vec<usize> = opt(&args, "clients")
        .unwrap_or_else(|| "1,4,16,32".to_string())
        .split(',')
        .map(|s| s.trim().parse().unwrap())
        .collect();
    let scan_parallel: usize = opt(&args, "scan-parallel").map_or(0, |s| s.parse().unwrap());
    let chunk_blocks: usize = opt(&args, "chunk-blocks")
        .map_or(turbovec_search::chunked::DEFAULT_CHUNK_BLOCKS, |s| {
            s.parse().unwrap()
        });

    eprintln!(
        "corpus: {n} x dim {dim}, 4-bit ({} MB packed), k={k}, {} cores",
        n * dim / 2 / (1024 * 1024),
        std::thread::available_parallelism()
            .map(|c| c.get())
            .unwrap_or(0),
    );
    let corpus = unit_vectors(n, dim, 0xC0A1_E5CE);
    let (shift, scale) = fit_calibration(dim, 4, &corpus[..100_000.min(n) * dim]);

    println!("| coalesce | clients | total queries | wall s | QPS | mean latency ms |");
    println!("|---|---:|---:|---:|---:|---:|");
    for coalesce in [false, true] {
        let mut index = turbovec_search::harness::seeded_index(dim, 4, &shift, &scale);
        index.add(&corpus);
        index.prepare();
        let (addr, server) = start_node(
            index,
            NodeConfig {
                coalesce,
                scan_parallel,
                chunk_blocks,
                ..Default::default()
            },
        )
        .await;

        // Warm the page cache and the connection path.
        one_query(&addr, unit_vectors(1, dim, 0xFEED_0001), k).await;

        for &clients in &client_counts {
            let (b0, j0) = scan_batch_counters();
            let t0 = Instant::now();
            let tasks: Vec<_> = (0..clients)
                .map(|c| {
                    let addr = addr.clone();
                    tokio::spawn(async move {
                        let mut latency = 0.0f64;
                        for q in 0..per_client {
                            let vector =
                                unit_vectors(1, dim, 0xBE9C_0000 + (c * per_client + q) as u64);
                            let t = Instant::now();
                            let hits = one_query(&addr, vector, k).await;
                            latency += t.elapsed().as_secs_f64();
                            assert_eq!(hits, k as usize);
                        }
                        latency / per_client as f64
                    })
                })
                .collect();
            let mut mean_latency = 0.0;
            for task in tasks {
                mean_latency += task.await.unwrap();
            }
            mean_latency /= clients as f64;
            let wall = t0.elapsed().as_secs_f64();
            let total = clients * per_client;
            let (b1, j1) = scan_batch_counters();
            let batching = if coalesce {
                format!(" ({} jobs / {} batches)", j1 - j0, b1 - b0)
            } else {
                String::new()
            };
            println!(
                "| {coalesce} | {clients} | {total} | {wall:.2} | {:.1} | {:.1}{batching} |",
                total as f64 / wall,
                mean_latency * 1e3,
            );
        }
        server.abort();
    }
}

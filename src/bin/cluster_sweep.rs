//! k-sweep against a PRE-EXISTING cluster (e.g. the two-machine wiki
//! shakedown deployment), measuring the floor-sharing payoff on real
//! data.
//!
//! Unlike `sweep` (which builds shards in-process), this binary drives
//! running nodes over the network. Floor sharing is a node-side config
//! flag, so it takes TWO node lists — one cluster with sharing on, one
//! with sharing off (same shard files, different ports) — and for each k
//! runs `--queries` probe vectors against both, reporting candidates
//! collected and wall median/p90 per mode, with the sharing on/off
//! correctness gate (identical hit signatures) asserted per k.
//!
//! Probe vectors come from the corpus on disk (`--data-dir`,
//! `read_embedding_at`), so queries live in the real bge-m3 space.
//!
//! ```text
//! cluster_sweep \
//!   --nodes-sharing=krick:50061,krick:50062,krick-1:50063,krick-1:50064 \
//!   --nodes-nosharing=krick:50071,krick:50072,krick-1:50073,krick-1:50074 \
//!   --k=10,100,1000,10000 --queries=20
//! ```

use std::time::{Duration, Instant};

use turbovec_search::coordinator::CoordinatorServiceImpl;
use turbovec_search::pb::node_service_client::NodeServiceClient;

const DEFAULT_DATA_DIR: &str = "/work/opensearch-grpc-knn/distributed_test_data/wikipedia";
const PART_COUNT: usize = 61_077;

fn arg(key: &str) -> Option<String> {
    let prefix = format!("--{key}=");
    std::env::args().find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
}

fn arg_or(key: &str, default: &str) -> String {
    arg(key).unwrap_or_else(|| default.to_string())
}

/// One probe: optional (opinion_id, ordinal) lineage plus the vector.
type Probe = (Option<(u64, u32)>, Vec<f32>);

/// Probe vectors: either from the court embeddings file (--probes-from)
/// or from the wiki corpus parts (--data-dir).
fn load_probes(
    probes_from: &str,
    data_dir: &str,
    n_queries: usize,
) -> Vec<Probe> {
    if probes_from.is_empty() {
        (0..n_queries)
            .map(|qi| {
                let (part, index) = (qi % 4, 1_000 + (qi / 4) * 9_000 % PART_COUNT);
                let (vector, _) = turbovec_search::dataset::read_embedding_at(
                    &std::path::PathBuf::from(format!("{data_dir}/embeddings_part_{part}.bin")),
                    index,
                )
                .expect("read probe vector");
                (None, vector)
            })
            .collect()
    } else {
        let (_, reader) = turbovec_search::court::EmbeddingReader::open(probes_from.as_ref())
            .expect("open probes file");
        let mut probes = Vec::new();
        for record in reader {
            if probes.len() >= n_queries {
                break;
            }
            let record = record.expect("read probe record");
            probes.push((Some((record.opinion_id, record.ordinal)), record.vector));
        }
        probes
    }
}

fn node_list(key: &str) -> Vec<String> {
    arg(key)
        .unwrap_or_else(|| panic!("--{key} is required"))
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| {
            if s.starts_with("http://") || s.starts_with("https://") {
                s.to_string()
            } else {
                format!("http://{s}")
            }
        })
        .collect()
}

async fn wait_ready(addr: &str) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if NodeServiceClient::connect(addr.to_string()).await.is_ok() {
            return;
        }
        assert!(Instant::now() < deadline, "node at {addr} never came up");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    let idx = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[idx]
}

struct Cell {
    candidates: u64,
    wall_median_ms: f64,
    wall_p90_ms: f64,
    signature: Vec<(u64, u32)>,
}

async fn run_cell(nodes: &[String], probes: &[Probe], k: u32) -> Cell {
    let coordinator = CoordinatorServiceImpl::new(nodes.to_vec());
    let mut walls = Vec::with_capacity(probes.len());
    let mut candidates = 0u64;
    let mut signature = Vec::new();
    for (qi, (_, vector)) in probes.iter().enumerate() {
        let start = Instant::now();
        let result = coordinator
            .fanout_search(&format!("sweep-{k}-{qi}"), vector, k, false)
            .await
            .expect("fanout search");
        walls.push(start.elapsed().as_secs_f64() * 1e3);
        for stats in result.shard_stats.iter().flatten() {
            candidates += stats.candidates_collected;
        }
        if qi == 0 {
            signature = result
                .hits
                .iter()
                .map(|h| (h.vector_id, h.score.to_bits()))
                .collect();
        }
    }
    walls.sort_by(|a, b| a.partial_cmp(b).unwrap());
    Cell {
        candidates,
        wall_median_ms: percentile(&walls, 0.5),
        wall_p90_ms: percentile(&walls, 0.9),
        signature,
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let nodes_sharing = node_list("nodes-sharing");
    let nodes_nosharing = node_list("nodes-nosharing");
    let data_dir = arg_or("data-dir", DEFAULT_DATA_DIR);
    let ks: Vec<u32> = arg_or("k", "10,100,1000,10000")
        .split(',')
        .map(|s| s.trim().parse().expect("--k"))
        .collect();
    let n_queries: usize = arg_or("queries", "20").parse()?;

    for addr in nodes_sharing.iter().chain(nodes_nosharing.iter()) {
        wait_ready(addr).await;
    }
    eprintln!(
        "cluster_sweep: {} + {} nodes, {} queries x k={:?}",
        nodes_sharing.len(),
        nodes_nosharing.len(),
        n_queries,
        ks
    );

    let probes = load_probes(&arg_or("probes-from", ""), &data_dir, n_queries);

    println!();
    println!(
        "{:>8} {:>8} {:>14} {:>14} {:>12}",
        "k", "sharing", "candidates", "wall_med_ms", "wall_p90_ms"
    );
    let mut gate_failures = 0;
    for &k in &ks {
        let on = run_cell(&nodes_sharing, &probes, k).await;
        let off = run_cell(&nodes_nosharing, &probes, k).await;
        println!(
            "{:>8} {:>8} {:>14} {:>14.3} {:>12.3}",
            k, "on", on.candidates, on.wall_median_ms, on.wall_p90_ms
        );
        println!(
            "{:>8} {:>8} {:>14} {:>14.3} {:>12.3}",
            k, "off", off.candidates, off.wall_median_ms, off.wall_p90_ms
        );
        if on.signature != off.signature {
            gate_failures += 1;
            eprintln!("CORRECTNESS GATE FAILURE at k={k}: sharing changed results");
        }
    }
    if gate_failures == 0 {
        eprintln!("correctness gate: sharing on/off results identical at every k");
    }
    std::process::exit(if gate_failures == 0 { 0 } else { 1 });
}

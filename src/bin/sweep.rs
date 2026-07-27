//! k-sweep benchmark harness: measures how floor sharing's payoff varies
//! with k.
//!
//! Builds a deterministic corpus in-process, partitions it across shard
//! node servers on loopback (real gRPC, same code path as production), and
//! for each k in the sweep runs `--queries` queries with floor sharing on
//! and off, reporting candidates collected and wall-time statistics per
//! mode. It also verifies (and asserts) that sharing never changes results.
//!
//! Usage:
//!
//! ```text
//! cargo run --release --bin sweep -- \
//!     --vectors 60000 --dim 128 --shards 3 \
//!     --k 10,100,1000,10000 --queries 20 \
//!     [--chunk-blocks 64] [--modes on,off] [--write-indexes /data/turbovec]
//! ```
//!
//! `--write-indexes DIR` persists the shards as `.tv` files and prints
//! ready-to-paste `[[shards]]` cluster-config entries — this is how the
//! indexes for a real multi-machine deployment are produced.
//!
//! The full k=10000 multi-machine experiment is a manual step (see the
//! README runbook); this binary is the harness for it, not something CI
//! runs end-to-end.

use std::path::PathBuf;
use std::time::Instant;

use turbovec_search::coordinator::CoordinatorServiceImpl;
use turbovec_search::harness::{
    build_shards, fit_calibration, start_node, unit_vectors, write_shards,
};
use turbovec_search::node::NodeConfig;

struct Args {
    vectors: usize,
    dim: usize,
    bit_width: usize,
    shards: usize,
    ks: Vec<u32>,
    queries: usize,
    chunk_blocks: usize,
    modes: Vec<bool>,
    write_indexes: Option<PathBuf>,
}

fn parse_args() -> Result<Args, String> {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let get = |key: &str| {
        let prefix = format!("--{key}=");
        argv.iter()
            .find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
    };
    let num = |key: &str, default: usize| {
        get(key)
            .map(|s| s.parse::<usize>().map_err(|e| format!("--{key}: {e}")))
            .transpose()
            .map(|v| v.unwrap_or(default))
    };
    let ks = get("k")
        .unwrap_or_else(|| "10,100,1000,10000".to_string())
        .split(',')
        .map(|s| s.trim().parse::<u32>().map_err(|e| format!("--k: {e}")))
        .collect::<Result<Vec<_>, _>>()?;
    let modes = get("modes")
        .unwrap_or_else(|| "on,off".to_string())
        .split(',')
        .map(|s| match s.trim() {
            "on" => Ok(true),
            "off" => Ok(false),
            other => Err(format!("--modes: unknown mode {other:?}")),
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Args {
        vectors: num("vectors", 60_000)?,
        dim: num("dim", 128)?,
        bit_width: num("bit-width", 4)?,
        shards: num("shards", 3)?,
        ks,
        queries: num("queries", 20)?,
        chunk_blocks: num("chunk-blocks", 64)?,
        modes,
        write_indexes: get("write-indexes").map(PathBuf::from),
    })
}

struct SweepRow {
    k: u32,
    sharing: bool,
    candidates: u64,
    wall_median_ms: f64,
    wall_p90_ms: f64,
    /// Hits of the first query, kept for the on/off correctness gate.
    first_query_hits: Vec<(u64, u32)>,
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    let idx = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[idx]
}

async fn run_mode(node_addrs: &[String], queries: &[Vec<f32>], k: u32, sharing: bool) -> SweepRow {
    let coordinator = CoordinatorServiceImpl::new(node_addrs.to_vec());
    let mut walls = Vec::with_capacity(queries.len());
    let mut candidates = 0u64;
    let mut first_query_hits = Vec::new();
    for (qi, query) in queries.iter().enumerate() {
        let start = Instant::now();
        let result = coordinator
            .fanout_search(&format!("sweep-{sharing}-{k}-{qi}"), query, k)
            .await
            .expect("fanout search");
        walls.push(start.elapsed().as_secs_f64() * 1e3);
        for stats in result.shard_stats.iter().flatten() {
            candidates += stats.candidates_collected;
        }
        assert_eq!(result.hits.len(), k as usize, "short result at k={k}");
        if qi == 0 {
            first_query_hits = result
                .hits
                .iter()
                .map(|h| (h.vector_id, h.score.to_bits()))
                .collect();
        }
    }
    walls.sort_by(|a, b| a.partial_cmp(b).unwrap());
    SweepRow {
        k,
        sharing,
        candidates,
        wall_median_ms: percentile(&walls, 0.5),
        wall_p90_ms: percentile(&walls, 0.9),
        first_query_hits,
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args()?;
    eprintln!(
        "sweep: {} vectors x dim {} @ {} bits, {} shards, k={:?}, {} queries, chunk_blocks={}, modes={:?}",
        args.vectors,
        args.dim,
        args.bit_width,
        args.shards,
        args.ks,
        args.queries,
        args.chunk_blocks,
        args.modes
    );

    // Deterministic corpus, calibration fitted on a sample, shards seeded
    // with it — the same construction the lossless tests use.
    let corpus = unit_vectors(args.vectors, args.dim, 0x5EED_CA11);
    let sample_n = 2_000.min(args.vectors);
    let (shift, scale) = fit_calibration(args.dim, args.bit_width, &corpus[..sample_n * args.dim]);

    let mut all_rows: Vec<SweepRow> = Vec::new();
    for sharing in args.modes.clone() {
        let shards = build_shards(
            &corpus,
            args.dim,
            args.bit_width,
            args.shards,
            &shift,
            &scale,
        );
        if sharing == args.modes[0] {
            if let Some(dir) = &args.write_indexes {
                write_shards(&shards, dir, 50051)?;
            }
        }
        let mut node_addrs = Vec::new();
        let mut handles = Vec::new();
        for shard in shards {
            let (addr, handle) = start_node(
                shard.index,
                NodeConfig {
                    slot_offset: shard.slot_offset,
                    chunk_blocks: args.chunk_blocks,
                    share_floors: sharing,
                },
            )
            .await;
            node_addrs.push(addr);
            handles.push(handle);
        }

        let queries: Vec<Vec<f32>> = (0..args.queries)
            .map(|qi| unit_vectors(1, args.dim, 0xB300_0000 + qi as u64))
            .collect();

        for &k in &args.ks {
            all_rows.push(run_mode(&node_addrs, &queries, k, sharing).await);
        }

        for handle in handles {
            handle.abort();
        }
    }

    // Correctness gate: for every k, sharing must not change results.
    for &k in &args.ks {
        let mut signatures = all_rows
            .iter()
            .filter(|r| r.k == k)
            .map(|r| (&r.first_query_hits, r.sharing));
        if let Some((first, _)) = signatures.next() {
            for (other, sharing) in signatures {
                assert_eq!(first, other, "sharing={sharing} changed results at k={k}");
            }
        }
    }
    if args.modes.len() > 1 {
        eprintln!("correctness: sharing on/off results identical at every k");
    }

    println!();
    println!(
        "{:>8} {:>8} {:>14} {:>14} {:>12}",
        "k", "sharing", "candidates", "wall_med_ms", "wall_p90_ms"
    );
    for row in &all_rows {
        println!(
            "{:>8} {:>8} {:>14} {:>14.3} {:>12.3}",
            row.k,
            if row.sharing { "on" } else { "off" },
            row.candidates,
            row.wall_median_ms,
            row.wall_p90_ms
        );
    }
    Ok(())
}

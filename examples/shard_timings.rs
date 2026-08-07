//! Per-shard leg timings for hybrid queries, against a live cluster.
//!
//! `HybridDebug.shards` carries the coordinator-measured wall time of
//! each shard's leg RPC (cascade: phase-1 SearchShard stream + phase-2
//! Bm25Rescore), so a slow fan-out can be attributed to the shard that
//! caused it instead of hiding inside `legs_ms`. This runs a handful of
//! queries with `debug: true` and prints:
//!
//!   1. one row per query — total_ms plus r1..rN rpc_ms in shard order,
//!      so the straggler is visible per query
//!   2. one row per shard — p50 / p90 / max of rpc_ms across queries,
//!      plus mean candidates_collected / floor_updates_applied from the
//!      cascade scan stats, and a verdict naming the straggler
//!
//! ```text
//! shard_timings --coord=127.0.0.1:59291 --analysis-addr=http://127.0.0.1:59202 \
//!               --queries=deploy/v7-rebuild/queries-case-folding.txt --k=10000 --n=8
//! ```

use turbovec_search::analyzer;
use turbovec_search::pb::search_service_client::SearchServiceClient;
use turbovec_search::pb::{AnalysisSpec, FusionMode, HybridLegOptions, HybridSearchRequest};

fn arg(key: &str, default: &str) -> String {
    let prefix = format!("--{key}=");
    std::env::args()
        .find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
        .unwrap_or_else(|| default.to_string())
}

fn body_spec() -> AnalysisSpec {
    turbovec_search::analyzer::body_spec()
}

#[derive(Default, Clone)]
struct ShardRow {
    shard: u32,
    rpc_ms: f32,
    vector_hits: u32,
    bm25_hits: u32,
    candidates_collected: u64,
    floor_updates_applied: u64,
}

/// Nearest-rank percentile of `values` (ms), sorted internally.
fn percentile(values: &[f32], p: f64) -> f32 {
    if values.is_empty() {
        return 0.0;
    }
    let mut v = values.to_vec();
    v.sort_by(|a, b| a.total_cmp(b));
    let idx = ((p / 100.0) * (v.len() as f64 - 1.0)).round() as usize;
    v[idx.min(v.len() - 1)]
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let coord = arg("coord", "127.0.0.1:59291");
    let analysis_addr = arg("analysis-addr", "http://127.0.0.1:59202");
    let k: u32 = arg("k", "10000").parse()?;
    let n: usize = arg("n", "8").parse()?;
    let queries: Vec<String> = match arg("queries", "").as_str() {
        "" => vec![arg("query", "qualified immunity clearly established right")],
        path => std::fs::read_to_string(path)?
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
            .map(str::to_string)
            .collect(),
    };
    let queries: Vec<String> = queries.into_iter().take(n).collect();
    if queries.is_empty() {
        eprintln!("no queries to run");
        std::process::exit(1);
    }

    let mut client = SearchServiceClient::connect(format!("http://{coord}")).await?;

    // Per-query rows, in run order; each row's shards are in shard order.
    let mut rows: Vec<(f32, f32, f32, Vec<ShardRow>)> = Vec::new(); // total, legs, fusion, shards
    for q in &queries {
        let vector = analyzer::embed_text(&analysis_addr, q).await?;
        let r = client
            .hybrid_search(HybridSearchRequest {
                request_id: String::new(),
                text: q.clone(),
                vector,
                k,
                analysis: Some(body_spec()),
                legs: Some(HybridLegOptions {
                    // Cascade is the only mode whose debug block carries
                    // the phase-1 scan stats; the counters are a property
                    // of the streaming vector path, which every mode that
                    // uses it shares.
                    fusion_mode: FusionMode::Cascade as i32,
                    ..Default::default()
                }),
                debug: true,
                boost: None,
            })
            .await?
            .into_inner();
        let Some(d) = r.debug else {
            eprintln!("no debug block returned; the server ignored `debug`");
            std::process::exit(1);
        };
        let mut shards: Vec<ShardRow> = d
            .shards
            .iter()
            .map(|s| {
                let (cands, applied) = match &s.scan {
                    Some(sc) => (sc.candidates_collected, sc.floor_updates_applied),
                    None => (0, 0),
                };
                ShardRow {
                    shard: s.shard,
                    rpc_ms: s.rpc_ms,
                    vector_hits: s.vector_hits,
                    bm25_hits: s.bm25_hits,
                    candidates_collected: cands,
                    floor_updates_applied: applied,
                }
            })
            .collect();
        shards.sort_by_key(|s| s.shard);
        rows.push((d.total_ms, d.legs_ms, d.fusion_ms, shards));
    }

    let n_shards = rows.first().map(|r| r.3.len()).unwrap_or(0);

    // --- Section 1: per-query table -------------------------------------
    println!("per-query (ms), k={k}, {} queries", rows.len());
    print!("  {:>3} {:>9} {:>9} {:>9}", "#", "total", "legs", "fusion");
    for i in 1..=n_shards {
        print!(" {:>9}", format!("r{i}"));
    }
    println!();
    for (qi, (total, legs, fusion, shards)) in rows.iter().enumerate() {
        print!("  {:>3} {:>9.1} {:>9.1} {:>9.1}", qi + 1, total, legs, fusion);
        for s in shards {
            print!(" {:>9.1}", s.rpc_ms);
        }
        println!();
    }

    // --- Section 2: per-shard summary ------------------------------------
    println!();
    println!("per-shard rpc_ms across {} queries", rows.len());
    println!(
        "  {:>5} {:>9} {:>9} {:>9} {:>10} {:>10} {:>9} {:>9}",
        "shard", "p50", "p90", "max", "cand_mean", "floor_mean", "vec_hits", "bm25_hits"
    );
    let mut p50s: Vec<(u32, f32)> = Vec::new();
    for si in 0..n_shards {
        let mut rpcs: Vec<f32> = Vec::new();
        let (mut cands, mut applied, mut vh, mut bh) = (0u64, 0u64, 0u64, 0u64);
        let mut shard_idx = 0u32;
        for (_, _, _, shards) in &rows {
            if let Some(s) = shards.get(si) {
                shard_idx = s.shard;
                rpcs.push(s.rpc_ms);
                cands += s.candidates_collected;
                applied += s.floor_updates_applied;
                vh += u64::from(s.vector_hits);
                bh += u64::from(s.bm25_hits);
            }
        }
        let nq = rpcs.len().max(1) as f64;
        let p50 = percentile(&rpcs, 50.0);
        let p90 = percentile(&rpcs, 90.0);
        let max = rpcs.iter().copied().fold(0.0f32, f32::max);
        p50s.push((shard_idx, p50));
        println!(
            "  {:>5} {:>9.1} {:>9.1} {:>9.1} {:>10.1} {:>10.1} {:>9.1} {:>9.1}",
            shard_idx,
            p50,
            p90,
            max,
            cands as f64 / nq,
            applied as f64 / nq,
            vh as f64 / nq,
            bh as f64 / nq,
        );
    }
    if let (Some(&(slow, slow_p50)), Some((fast, fast_p50))) = (
        p50s.iter().max_by(|a, b| a.1.total_cmp(&b.1)),
        p50s.iter().min_by(|a, b| a.1.total_cmp(&b.1)).copied(),
    ) {
        let ratio = if fast_p50 > 0.0 {
            slow_p50 / fast_p50
        } else {
            f32::INFINITY
        };
        println!(
            "straggler: shard {slow} (p50 {slow_p50:.1} ms, {ratio:.2}x fastest shard {fast} p50 {fast_p50:.1} ms)"
        );
    }
    Ok(())
}

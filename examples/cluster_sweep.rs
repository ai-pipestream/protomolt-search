//! Latency / pruning benchmark against a PRE-EXISTING cluster (e.g. the
//! two-machine wiki shakedown deployment or the court e2e stack).
//!
//! Unlike `sweep` (which builds shards in-process), this binary drives
//! running nodes over the network. Two modes:
//!
//! * Single-cluster (default): point `--nodes-sharing` at the cluster
//!   under test. For each k it runs `--warmup` discarded probes, then
//!   `--queries` timed probes, reporting per-query wall percentiles
//!   (p50/p90/p99), pruning counters (candidates collected, floors
//!   published/applied), and QPS when `--concurrency` > 1.
//! * A/B: also pass `--nodes-nosharing` (same shard files, different
//!   ports — floor sharing is a node-side startup flag) and each k runs
//!   against both clusters, with the sharing on/off correctness gate
//!   (identical hit signatures) asserted per k.
//!
//! Probe vectors come from the corpus on disk (`--data-dir`,
//! `read_embedding_at`) or from a court embeddings file
//! (`--probes-from`), so queries live in the real embedding space.
//!
//! With `--replicas` (one entry per shard, empty slots allowed) the
//! sharing cluster can also be swept over hedge delays: `--hedge-ms`
//! takes a comma list where `off` disables hedging, and every hedged
//! cell's hit signature is gated against the un-hedged cell — hedging
//! races two copies of an exact search, so it must never move a result.
//!
//! ```text
//! # single cluster, k sweep, 8 concurrent clients
//! cluster_sweep \
//!   --nodes-sharing=node1:50051,node2:50051,node3:50051,node4:50051 \
//!   --k=10,100,1000 --queries=100 --warmup=5 --concurrency=8 \
//!   --probes-from=/corpus/embeddings.bin --label=4shard-200gb \
//!   --json=bench.jsonl
//!
//! # A/B floor-sharing gate (two clusters over the same shard files)
//! cluster_sweep \
//!   --nodes-sharing=host-a:50061,host-a:50062,host-b:50063,host-b:50064 \
//!   --nodes-nosharing=host-a:50071,host-a:50072,host-b:50073,host-b:50074 \
//!   --k=10,100,1000,10000 --queries=20
//!
//! # hedge sweep against replicas of the same shards
//! cluster_sweep \
//!   --nodes-sharing=host-a:50061,host-b:50063 \
//!   --replicas=host-a:50071,host-b:50073 \
//!   --hedge-ms=off,400,800 --k=10 --queries=200 --concurrency=8
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use pipestream_search::coordinator::{CoordinatorServiceImpl, FanoutLimits};
use pipestream_search::pb::node_service_client::NodeServiceClient;
use tokio::sync::Semaphore;

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
fn load_probes(probes_from: &str, data_dir: &str, n_queries: usize) -> Vec<Probe> {
    if probes_from.is_empty() {
        (0..n_queries)
            .map(|qi| {
                let (part, index) = (qi % 4, 1_000 + (qi / 4) * 9_000 % PART_COUNT);
                let (vector, _) = pipestream_search::demo::dataset::read_embedding_at(
                    &std::path::PathBuf::from(format!("{data_dir}/embeddings_part_{part}.bin")),
                    index,
                )
                .expect("read probe vector");
                (None, vector)
            })
            .collect()
    } else {
        let (_, reader) =
            pipestream_search::demo::court::EmbeddingReader::open(probes_from.as_ref())
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

fn node_list_required(key: &str) -> Vec<String> {
    node_list(key).unwrap_or_else(|| panic!("--{key} is required"))
}

fn node_list(key: &str) -> Option<Vec<String>> {
    arg(key).map(|raw| {
        raw.split(',')
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
    })
}

/// Replica addresses, positionally aligned with the sharing node list.
/// An empty slot (`a,,c`) means "this shard has no replica".
fn replica_list(key: &str, shards: usize) -> Vec<Option<String>> {
    let Some(raw) = arg(key) else {
        return vec![None; shards];
    };
    let mut replicas: Vec<Option<String>> = raw
        .split(',')
        .map(str::trim)
        .map(|s| match s {
            "" => None,
            s if s.starts_with("http://") || s.starts_with("https://") => Some(s.to_string()),
            s => Some(format!("http://{s}")),
        })
        .collect();
    assert!(
        replicas.len() <= shards,
        "--{key} has {} entries for {shards} shard(s)",
        replicas.len()
    );
    replicas.resize(shards, None);
    replicas
}

/// `--hedge-ms=off,400,800` -> [None, 400ms, 800ms]. `off` and `0` both
/// disable hedging for that cell.
fn hedge_list() -> Vec<Option<Duration>> {
    arg_or("hedge-ms", "off")
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| match s {
            "off" | "0" => None,
            ms => Some(Duration::from_millis(ms.parse().expect("--hedge-ms"))),
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

/// One query's outcome: wall time, aggregated shard stats, hit signature.
struct QueryOutcome {
    wall_ms: f64,
    candidates: u64,
    floors_published: u64,
    floor_updates_applied: u64,
    hedges_fired: u64,
    hedge_wins: u64,
    signature: Vec<(u64, u32)>,
}

async fn run_query(
    coordinator: &CoordinatorServiceImpl,
    request_id: &str,
    vector: &[f32],
    k: u32,
) -> QueryOutcome {
    let start = Instant::now();
    let result = coordinator
        .fanout_search(request_id, vector, k, false, &Default::default())
        .await
        .expect("fanout search");
    let wall_ms = start.elapsed().as_secs_f64() * 1e3;
    let mut candidates = 0u64;
    let mut floors_published = 0u64;
    let mut floor_updates_applied = 0u64;
    for stats in result.shard_stats.iter().flatten() {
        candidates += stats.candidates_collected;
        floors_published += stats.floors_published;
        floor_updates_applied += stats.floor_updates_applied;
    }
    QueryOutcome {
        wall_ms,
        candidates,
        floors_published,
        floor_updates_applied,
        hedges_fired: result.hedges_fired,
        hedge_wins: result.hedge_wins,
        signature: result
            .hits
            .iter()
            .map(|h| (h.vector_id, h.score.to_bits()))
            .collect(),
    }
}

struct Cell {
    n_shards: usize,
    candidates: u64,
    floors_published: u64,
    floor_updates_applied: u64,
    hedges_fired: u64,
    hedge_wins: u64,
    walls: Vec<f64>,
    /// Total timed-phase elapsed (for QPS under concurrency).
    elapsed: Duration,
    signature: Vec<(u64, u32)>,
}

impl Cell {
    fn p(&self, p: f64) -> f64 {
        percentile(&self.walls, p)
    }
    fn qps(&self) -> f64 {
        self.walls.len() as f64 / self.elapsed.as_secs_f64()
    }
}

/// Warm up with the head of the probe set, then time every probe. The
/// warmup probes are timed too (they run twice total) — harmless for
/// latency distribution and keeps the probe count exactly `--queries`.
async fn run_cell(
    nodes: &[String],
    replicas: &[Option<String>],
    limits: FanoutLimits,
    probes: &[Probe],
    k: u32,
    warmup: usize,
    concurrency: usize,
) -> Cell {
    let coordinator = Arc::new(
        CoordinatorServiceImpl::new(nodes.to_vec())
            .with_replicas(replicas.to_vec())
            .with_limits(limits),
    );

    for (qi, (_, vector)) in probes.iter().take(warmup).enumerate() {
        run_query(&coordinator, &format!("sweep-{k}-warm-{qi}"), vector, k).await;
    }

    let start = Instant::now();
    let mut outcomes: Vec<QueryOutcome> = if concurrency <= 1 {
        let mut out = Vec::with_capacity(probes.len());
        for (qi, (_, vector)) in probes.iter().enumerate() {
            out.push(run_query(&coordinator, &format!("sweep-{k}-{qi}"), vector, k).await);
        }
        out
    } else {
        let semaphore = Arc::new(Semaphore::new(concurrency));
        let mut tasks = tokio::task::JoinSet::new();
        for (qi, (_, vector)) in probes.iter().enumerate() {
            let coordinator = Arc::clone(&coordinator);
            let permit = Arc::clone(&semaphore)
                .acquire_owned()
                .await
                .expect("semaphore");
            let vector = vector.clone();
            let request_id = format!("sweep-{k}-{qi}");
            tasks.spawn(async move {
                let _permit = permit;
                (qi, run_query(&coordinator, &request_id, &vector, k).await)
            });
        }
        let mut indexed = Vec::with_capacity(probes.len());
        while let Some(joined) = tasks.join_next().await {
            indexed.push(joined.expect("query task panicked"));
        }
        indexed.sort_by_key(|(qi, _)| *qi);
        indexed.into_iter().map(|(_, outcome)| outcome).collect()
    };
    let elapsed = start.elapsed();

    let signature = outcomes
        .first_mut()
        .map(|o| std::mem::take(&mut o.signature))
        .unwrap_or_default();
    let mut walls: Vec<f64> = outcomes.iter().map(|o| o.wall_ms).collect();
    walls.sort_by(|a, b| a.partial_cmp(b).unwrap());
    Cell {
        n_shards: nodes.len(),
        candidates: outcomes.iter().map(|o| o.candidates).sum(),
        floors_published: outcomes.iter().map(|o| o.floors_published).sum(),
        floor_updates_applied: outcomes.iter().map(|o| o.floor_updates_applied).sum(),
        hedges_fired: outcomes.iter().map(|o| o.hedges_fired).sum(),
        hedge_wins: outcomes.iter().map(|o| o.hedge_wins).sum(),
        walls,
        elapsed,
        signature,
    }
}

fn hedge_label(hedge: Option<Duration>) -> String {
    match hedge {
        None => "off".to_string(),
        Some(d) => format!("{}ms", d.as_millis()),
    }
}

fn print_row(k: u32, mode: &str, hedge: &str, cell: &Cell, n_queries: usize) {
    println!(
        "{:>8} {:>8} {:>8} {:>7} {:>14} {:>12} {:>10} {:>10} {:>8} {:>6} {:>9.3} {:>9.3} {:>9.3} {:>8.1}",
        k,
        mode,
        hedge,
        cell.n_shards,
        cell.candidates,
        cell.candidates / n_queries.max(1) as u64,
        cell.floors_published,
        cell.floor_updates_applied,
        cell.hedges_fired,
        cell.hedge_wins,
        cell.p(0.5),
        cell.p(0.9),
        cell.p(0.99),
        cell.qps(),
    );
}

fn json_line(
    label: &str,
    k: u32,
    mode: &str,
    hedge: &str,
    cell: &Cell,
    n_queries: usize,
) -> String {
    serde_json::json!({
        "label": label,
        "k": k,
        "floor_sharing": mode,
        "hedge": hedge,
        "shards": cell.n_shards,
        "queries": n_queries,
        "candidates_collected": cell.candidates,
        "candidates_per_query": cell.candidates as f64 / n_queries.max(1) as f64,
        "floors_published": cell.floors_published,
        "floor_updates_applied": cell.floor_updates_applied,
        "hedges_fired": cell.hedges_fired,
        "hedge_wins": cell.hedge_wins,
        "wall_p50_ms": cell.p(0.5),
        "wall_p90_ms": cell.p(0.9),
        "wall_p99_ms": cell.p(0.99),
        "wall_min_ms": cell.walls.first().copied().unwrap_or(0.0),
        "wall_max_ms": cell.walls.last().copied().unwrap_or(0.0),
        "qps": cell.qps(),
    })
    .to_string()
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Lexical mode: --bm25-terms runs the block-max factorial instead of
    // the vector sweep.
    if arg("bm25-terms").is_some() {
        return run_lexical_factorial().await;
    }
    let nodes_sharing = node_list_required("nodes-sharing");
    let nodes_nosharing = node_list("nodes-nosharing");
    let replicas = replica_list("replicas", nodes_sharing.len());
    let hedges = hedge_list();
    let shard_deadline: u64 = arg_or("shard-deadline-ms", "0").parse()?;
    let shard_deadline = (shard_deadline > 0).then(|| Duration::from_millis(shard_deadline));
    let data_dir = arg_or("data-dir", DEFAULT_DATA_DIR);
    let ks: Vec<u32> = arg_or("k", "10,100,1000,10000")
        .split(',')
        .map(|s| s.trim().parse().expect("--k"))
        .collect();
    let n_queries: usize = arg_or("queries", "20").parse()?;
    let warmup: usize = arg_or("warmup", "2").parse()?;
    let concurrency: usize = arg_or("concurrency", "1").parse()?;
    let label = arg_or("label", "");
    let json_path = arg("json");

    for addr in nodes_sharing
        .iter()
        .chain(nodes_nosharing.iter().flatten())
        .chain(replicas.iter().flatten())
    {
        wait_ready(addr).await;
    }
    eprintln!(
        "cluster_sweep: {} shard(s){}, {} queries (+{} warmup) x k={:?}, concurrency={}",
        nodes_sharing.len(),
        nodes_nosharing
            .as_ref()
            .map(|n| format!(" + {} no-sharing", n.len()))
            .unwrap_or_default(),
        n_queries,
        warmup,
        ks,
        concurrency,
    );

    let probes = load_probes(&arg_or("probes-from", ""), &data_dir, n_queries);

    println!();
    println!(
        "{:>8} {:>8} {:>8} {:>7} {:>14} {:>12} {:>10} {:>10} {:>8} {:>6} {:>9} {:>9} {:>9} {:>8}",
        "k",
        "sharing",
        "hedge",
        "shards",
        "candidates",
        "cand/query",
        "floors_pub",
        "floors_app",
        "hedged",
        "won",
        "p50_ms",
        "p90_ms",
        "p99_ms",
        "qps"
    );

    let mut json_lines = Vec::new();
    let mut gate_failures = 0;
    for &k in &ks {
        // The first hedge value is the reference cell for this k: every
        // other cell's hit signature is gated against it. Hedging races
        // two copies of the same exact search, so it must not move a hit.
        let mut reference: Option<Vec<(u64, u32)>> = None;
        for &hedge in &hedges {
            let limits = FanoutLimits {
                shard_deadline,
                hedge_delay: hedge,
            };
            let cell = run_cell(
                &nodes_sharing,
                &replicas,
                limits,
                &probes,
                k,
                warmup,
                concurrency,
            )
            .await;
            let hedge = hedge_label(hedge);
            print_row(k, "on", &hedge, &cell, n_queries);
            json_lines.push(json_line(&label, k, "on", &hedge, &cell, n_queries));
            match &reference {
                None => reference = Some(cell.signature.clone()),
                Some(expected) if *expected != cell.signature => {
                    gate_failures += 1;
                    eprintln!("CORRECTNESS GATE FAILURE at k={k}: hedge={hedge} changed results");
                }
                Some(_) => {}
            }
        }

        if let Some(off_nodes) = &nodes_nosharing {
            let off = run_cell(
                off_nodes,
                &vec![None; off_nodes.len()],
                FanoutLimits::default(),
                &probes,
                k,
                warmup,
                concurrency,
            )
            .await;
            print_row(k, "off", "off", &off, n_queries);
            json_lines.push(json_line(&label, k, "off", "off", &off, n_queries));
            if reference.as_ref().is_some_and(|r| *r != off.signature) {
                gate_failures += 1;
                eprintln!("CORRECTNESS GATE FAILURE at k={k}: sharing changed results");
            }
        }
    }

    if let Some(path) = json_path {
        use std::io::Write as _;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        for line in &json_lines {
            writeln!(file, "{line}")?;
        }
        eprintln!("wrote {} JSONL record(s) to {path}", json_lines.len());
    }

    let gated = nodes_nosharing.is_some() || hedges.len() > 1;
    if gated {
        if gate_failures == 0 {
            eprintln!("correctness gate: results identical across every cell at every k");
        }
        std::process::exit(if gate_failures == 0 { 0 } else { 1 });
    }
    Ok(())
}

// --- lexical mode: the block-max factorial -------------------------------
//
// `--bm25-terms="court,state,appeal"` switches to the lexical leg
// (docs/block-max.md, stage 4): the terms form ONE multi-term
// Bm25Search (the realistic query shape), repeated --queries times per
// cell. The 2x2 factorial is {block-max on, off} x {unseeded, seeded}:
//
// * block-max on is `--nodes-sharing` (nodes started normally); off is
//   `--nodes-nosharing` (same shard files, nodes started with
//   --block-max=false — a node startup flag, exactly like the sharing
//   A/B). With no second cluster only the "on" column runs.
// * seeded re-issues the same query with min_score set to one f32 ULP
//   below the merged k-th best captured from that cluster's unseeded
//   cell (`seed_from_kth`): the ULP-down recipe guarantees every doc at
//   or above the true k-th best survives, so seeded must equal unseeded
//   EXACTLY.
//
// Every cell's hit signature is gated against the on/unseeded
// reference; any mismatch exits non-zero.
//
// ```text
// cluster_sweep --bm25-terms="court,state,appeal" \
//   --analysis=localhost:50052 \
//   --nodes-sharing=host-a:50061,host-b:50063 \
//   --nodes-nosharing=host-a:50071,host-b:50073 \
//   --k=10,1000 --queries=20
// ```

/// The one-ULP-down seed: the largest f32 strictly below `kth`
/// (`f32::next_down`, stable since Rust 1.86). Every doc whose true f64
/// score is >= the true k-th best survives it. 0 stays 0 (no floor).
fn seed_from_kth(kth: f32) -> f32 {
    if kth > 0.0 {
        kth.next_down()
    } else {
        0.0
    }
}

struct LexCell {
    walls: Vec<f64>,
    signature: Vec<(u64, u32)>,
    kth_best: f32,
}

/// One factorial cell: the same multi-term query, --queries times
/// (plus warmup), wall percentiles and the hit signature.
async fn run_lexical_cell(
    addrs: &[String],
    analysis: &str,
    text: &str,
    k: u32,
    n_queries: usize,
    warmup: usize,
    seed: f32,
) -> LexCell {
    let coordinator = CoordinatorServiceImpl::new(addrs.to_vec())
        .with_bm25(Some(analysis.to_string()), Default::default());
    let mut walls = Vec::with_capacity(n_queries);
    let mut signature = Vec::new();
    let mut kth_best = 0.0f32;
    for qi in 0..warmup + n_queries {
        let start = Instant::now();
        let hits = coordinator
            .fanout_bm25_seeded(text, k, None, seed)
            .await
            .expect("bm25 fanout");
        if qi >= warmup {
            walls.push(start.elapsed().as_secs_f64() * 1e3);
            if signature.is_empty() {
                signature = hits.iter().map(|h| (h.doc_id, h.score.to_bits())).collect();
                kth_best = if hits.len() == k as usize {
                    hits.last().map(|h| h.score).unwrap_or(0.0)
                } else {
                    0.0
                };
            }
        }
    }
    walls.sort_by(|a, b| a.partial_cmp(b).unwrap());
    LexCell {
        walls,
        signature,
        kth_best,
    }
}

async fn run_lexical_factorial() -> Result<(), Box<dyn std::error::Error>> {
    let terms = arg("bm25-terms").expect("lexical mode checked");
    let text = terms
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join(" ");
    assert!(!text.is_empty(), "--bm25-terms: no usable terms");
    let analysis = arg("analysis").expect("--analysis is required in lexical mode");
    let analysis = if analysis.starts_with("http://") || analysis.starts_with("https://") {
        analysis
    } else {
        format!("http://{analysis}")
    };
    let nodes_on = node_list_required("nodes-sharing");
    let nodes_off = node_list("nodes-nosharing");
    let ks: Vec<u32> = arg_or("k", "10,1000")
        .split(',')
        .map(|s| s.trim().parse().expect("--k"))
        .collect();
    let n_queries: usize = arg_or("queries", "20").parse()?;
    let warmup: usize = arg_or("warmup", "2").parse()?;

    for addr in nodes_on.iter().chain(nodes_off.iter().flatten()) {
        wait_ready(addr).await;
    }
    eprintln!(
        "cluster_sweep lexical: {text:?} over {} block-max shard(s){}, {} queries x k={ks:?}",
        nodes_on.len(),
        nodes_off
            .as_ref()
            .map(|n| format!(" + {} block-max-off", n.len()))
            .unwrap_or_default(),
        n_queries,
    );
    println!();
    println!(
        "{:>8} {:>9} {:>8} {:>9} {:>9} {:>9}",
        "k", "blockmax", "seeded", "p50_ms", "p90_ms", "p99_ms"
    );

    let mut gate_failures = 0u64;
    for &k in &ks {
        // The on/unseeded cell is the reference signature for this k.
        let reference =
            run_lexical_cell(&nodes_on, &analysis, &text, k, n_queries, warmup, 0.0).await;
        println!(
            "{:>8} {:>9} {:>8} {:>9.3} {:>9.3} {:>9.3}",
            k,
            "on",
            "no",
            percentile(&reference.walls, 0.5),
            percentile(&reference.walls, 0.9),
            percentile(&reference.walls, 0.99)
        );
        let clusters: Vec<(&str, &[String])> = match &nodes_off {
            Some(off) => vec![("on", &nodes_on[..]), ("off", &off[..])],
            None => vec![("on", &nodes_on[..])],
        };
        for (label, addrs) in clusters {
            // Each cluster seeds from its own unseeded k-th best.
            let base = if label == "on" {
                seed_from_kth(reference.kth_best)
            } else {
                let unseeded =
                    run_lexical_cell(addrs, &analysis, &text, k, n_queries, warmup, 0.0).await;
                println!(
                    "{:>8} {:>9} {:>8} {:>9.3} {:>9.3} {:>9.3}",
                    k,
                    label,
                    "no",
                    percentile(&unseeded.walls, 0.5),
                    percentile(&unseeded.walls, 0.9),
                    percentile(&unseeded.walls, 0.99)
                );
                if unseeded.signature != reference.signature {
                    gate_failures += 1;
                    eprintln!(
                        "CORRECTNESS GATE FAILURE at k={k}: block-max {label} unseeded changed results"
                    );
                }
                seed_from_kth(unseeded.kth_best)
            };
            let seeded =
                run_lexical_cell(addrs, &analysis, &text, k, n_queries, warmup, base).await;
            println!(
                "{:>8} {:>9} {:>8} {:>9.3} {:>9.3} {:>9.3}",
                k,
                label,
                "yes",
                percentile(&seeded.walls, 0.5),
                percentile(&seeded.walls, 0.9),
                percentile(&seeded.walls, 0.99)
            );
            if seeded.signature != reference.signature {
                gate_failures += 1;
                eprintln!(
                    "CORRECTNESS GATE FAILURE at k={k}: block-max {label} seeded changed results"
                );
            }
        }
    }
    if gate_failures == 0 {
        eprintln!("correctness gate: results identical across every cell at every k");
    }
    std::process::exit(if gate_failures == 0 { 0 } else { 1 });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seed_from_kth_is_one_ulp_down() {
        let x = 0.5f32;
        let seed = seed_from_kth(x);
        assert!(seed < x, "seed must be strictly below the k-th best");
        assert_eq!(seed_from_kth(seed), f32::from_bits(seed.to_bits() - 1));
        assert_eq!(seed.to_bits(), x.to_bits() - 1, "exactly one f32 ULP down");
        // Anything that rounds to x (i.e. the true k-th best, within
        // half a ULP) is strictly above the seed.
        assert!(f64::from(seed) < f64::from(x));
    }

    #[test]
    fn seed_from_kth_zero_stays_zero() {
        assert_eq!(seed_from_kth(0.0), 0.0, "no floor available: stay unseeded");
        assert_eq!(seed_from_kth(-1.0), 0.0);
    }

    #[test]
    fn signature_comparison_is_exact() {
        let a: Vec<(u64, u32)> = vec![(1, 0x3f80_0000), (5, 0x3f00_0000)];
        let b = a.clone();
        assert_eq!(a, b);
        let mut c = a.clone();
        c[1].1 ^= 1;
        assert_ne!(a, c, "one flipped score bit must fail the gate");
        let d = a.clone().into_iter().rev().collect::<Vec<_>>();
        assert_ne!(a, d, "order matters in the signature");
    }
}

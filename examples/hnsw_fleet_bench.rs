//! Multi-shard HNSW streaming fleet benchmark (`#100`, `#72`).
//!
//! Two subcommands:
//! - `build`: partitions an `.f32` fixture (`manifest.json`, `base.f32`,
//!   `queries.f32`) into `S` `PMHNSW01` shard images under `hash` (`row % S`)
//!   or spherical `kmeans` Voronoi placement, writes `centroids.f32` (`S x D`),
//!   copies `queries.f32`, and computes exact global top-`max_k` ground-truth
//!   `vector_id`s (`s * slot_stride + local_slot`) in `gt_u64.bin`.
//! - `sweep`: connects an in-process streaming `CoordinatorServiceImpl` (with
//!   UDP `TVS1` floor/finish signaling + gRPC `StreamSearch`) to running
//!   `pipestream-search --role=node --vector-backend=hnsw` shard processes and
//!   sweeps `k`, floor-pruning `modes`, `settle_us`, and `deadline_us` across
//!   staggered repeats, reporting exact `recall@k`, `p50/p90/p99` ms, QPS,
//!   `units_scanned`, per-shard `min/med/max` scans, `emitted`, `floors_sent`,
//!   `shards_finished_early`, and `shards_floor_pruned`.

use std::cmp::Ordering;
use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use pipestream_search::coordinator::{CoordinatorServiceImpl, RequestFilters};
use pipestream_search::hnsw::{BuildMode, HnswConfig, LocalityOrder, HNSW};
use pipestream_search::security::ToolClient;
use pipestream_search::vector::VectorIndex;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};

const DEFAULT_SLOT_STRIDE: u64 = 1_000_000;
const DEFAULT_SHARDS: usize = 16;
const DEFAULT_MAX_K: usize = 10_000;
const DEFAULT_KMEANS_ITERS: usize = 14;
const DEFAULT_SEED: u64 = 0x4857_4E53_5700_0001;

#[derive(Debug, Serialize, Deserialize)]
struct FixtureManifest {
    dim: usize,
}

#[derive(Debug, Serialize, Deserialize)]
struct FleetManifest {
    dim: usize,
    rows: usize,
    queries: usize,
    shards: usize,
    slot_stride: u64,
    max_k: usize,
    placement: String,
    shard_rows: Vec<usize>,
}

#[derive(Debug, Serialize)]
struct SweepRecord {
    label: String,
    mode: String,
    placement: String,
    shards: usize,
    k: u32,
    settle_us: Option<u32>,
    deadline_us: Option<u32>,
    repeats: usize,
    queries_per_repeat: usize,
    recall: f64,
    recall_min: f64,
    recall_p05: f64,
    recall_std: f64,
    p50_ms: f64,
    p90_ms: f64,
    p99_ms: f64,
    mean_ms: f64,
    qps: f64,
    units_scanned_per_query: f64,
    shard_min_scanned_per_query: f64,
    shard_median_scanned_per_query: f64,
    shard_max_scanned_per_query: f64,
    emitted_per_query: f64,
    floors_sent_per_query: f64,
    shards_finished_early_per_query: f64,
    shards_floor_pruned_per_query: f64,
    shards_patience_stopped_per_query: f64,
}

fn arg(key: &str) -> Option<String> {
    let prefix = format!("--{key}=");
    std::env::args().find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
}

fn arg_or(key: &str, default: &str) -> String {
    arg(key).unwrap_or_else(|| default.to_string())
}

fn security() -> &'static ToolClient {
    static SECURITY: OnceLock<ToolClient> = OnceLock::new();
    SECURITY.get_or_init(|| {
        let security = ToolClient::from_env_args().unwrap_or_else(|e| panic!("{e}"));
        security.install();
        security
    })
}

fn read_f32_vec(path: &Path) -> Result<Vec<f32>, String> {
    let meta = fs::metadata(path).map_err(|e| format!("stat {}: {e}", path.display()))?;
    let bytes = usize::try_from(meta.len()).map_err(|e| format!("size {}: {e}", path.display()))?;
    if !bytes.is_multiple_of(4) {
        return Err(format!(
            "{} length {bytes} is not a multiple of 4",
            path.display()
        ));
    }
    let count = bytes / 4;
    let mut reader =
        BufReader::new(File::open(path).map_err(|e| format!("open {}: {e}", path.display()))?);
    let mut raw = vec![0u8; bytes];
    reader
        .read_exact(&mut raw)
        .map_err(|e| format!("read {}: {e}", path.display()))?;
    let mut out = Vec::with_capacity(count);
    for chunk in raw.as_chunks::<4>().0 {
        out.push(f32::from_le_bytes(*chunk));
    }
    Ok(out)
}

fn write_f32_vec(path: &Path, values: &[f32]) -> Result<(), String> {
    let mut writer =
        BufWriter::new(File::create(path).map_err(|e| format!("create {}: {e}", path.display()))?);
    for &v in values {
        writer
            .write_all(&v.to_le_bytes())
            .map_err(|e| format!("write {}: {e}", path.display()))?;
    }
    writer
        .flush()
        .map_err(|e| format!("flush {}: {e}", path.display()))
}

fn write_u64_vec(path: &Path, values: &[u64]) -> Result<(), String> {
    let mut writer =
        BufWriter::new(File::create(path).map_err(|e| format!("create {}: {e}", path.display()))?);
    for &v in values {
        writer
            .write_all(&v.to_le_bytes())
            .map_err(|e| format!("write {}: {e}", path.display()))?;
    }
    writer
        .flush()
        .map_err(|e| format!("flush {}: {e}", path.display()))
}

fn read_u64_vec(path: &Path) -> Result<Vec<u64>, String> {
    let meta = fs::metadata(path).map_err(|e| format!("stat {}: {e}", path.display()))?;
    let bytes = usize::try_from(meta.len()).map_err(|e| format!("size {}: {e}", path.display()))?;
    if !bytes.is_multiple_of(8) {
        return Err(format!(
            "{} length {bytes} is not a multiple of 8",
            path.display()
        ));
    }
    let count = bytes / 8;
    let mut reader =
        BufReader::new(File::open(path).map_err(|e| format!("open {}: {e}", path.display()))?);
    let mut raw = vec![0u8; bytes];
    reader
        .read_exact(&mut raw)
        .map_err(|e| format!("read {}: {e}", path.display()))?;
    let mut out = Vec::with_capacity(count);
    for chunk in raw.as_chunks::<8>().0 {
        out.push(u64::from_le_bytes(*chunk));
    }
    Ok(out)
}

fn dot_f32(a: &[f32], b: &[f32]) -> f32 {
    let mut s0 = 0.0f32;
    let mut s1 = 0.0f32;
    let mut s2 = 0.0f32;
    let mut s3 = 0.0f32;
    let chunks = a.len() / 4;
    for i in 0..chunks {
        let base = i * 4;
        s0 += a[base] * b[base];
        s1 += a[base + 1] * b[base + 1];
        s2 += a[base + 2] * b[base + 2];
        s3 += a[base + 3] * b[base + 3];
    }
    let mut tail = 0.0f32;
    for i in (chunks * 4)..a.len() {
        tail += a[i] * b[i];
    }
    (s0 + s1) + (s2 + s3) + tail
}

fn normalize_in_place(v: &mut [f32]) {
    let norm_sq: f64 = v.iter().map(|&x| f64::from(x) * f64::from(x)).sum();
    let norm = norm_sq.sqrt();
    if norm > 1e-12 {
        let inv = (1.0 / norm) as f32;
        for x in v.iter_mut() {
            *x *= inv;
        }
    }
}

#[derive(Clone, Copy)]
struct SplitMix64(u64);

impl SplitMix64 {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn next_f64(&mut self) -> f64 {
        ((self.next_u64() >> 11) as f64) * (1.0 / ((1u64 << 53) as f64))
    }
}

/// Spherical k-means++ initialization followed by `iters` Lloyd steps.
fn spherical_kmeans(
    base: &[f32],
    rows: usize,
    dim: usize,
    shards: usize,
    iters: usize,
    seed: u64,
) -> (Vec<usize>, Vec<f32>) {
    let mut rng = SplitMix64(seed);
    let mut centroids = vec![0.0f32; shards * dim];
    let first = (rng.next_u64() as usize) % rows;
    centroids[..dim].copy_from_slice(&base[first * dim..(first + 1) * dim]);
    normalize_in_place(&mut centroids[..dim]);

    let mut min_dist = vec![f64::INFINITY; rows];
    for c in 1..shards {
        let prev = &centroids[(c - 1) * dim..c * dim];
        min_dist
            .par_iter_mut()
            .enumerate()
            .for_each(|(r, best_dist)| {
                let row = &base[r * dim..(r + 1) * dim];
                let sim = f64::from(dot_f32(row, prev)).clamp(-1.0, 1.0);
                let dist = (1.0 - sim).max(0.0);
                if dist < *best_dist {
                    *best_dist = dist;
                }
            });
        let total: f64 = min_dist.iter().sum();
        let chosen = if total <= 1e-12 {
            (rng.next_u64() as usize) % rows
        } else {
            let target = rng.next_f64() * total;
            let mut acc = 0.0;
            let mut idx = rows - 1;
            for (r, &d) in min_dist.iter().enumerate() {
                acc += d;
                if acc >= target {
                    idx = r;
                    break;
                }
            }
            idx
        };
        centroids[c * dim..(c + 1) * dim].copy_from_slice(&base[chosen * dim..(chosen + 1) * dim]);
        normalize_in_place(&mut centroids[c * dim..(c + 1) * dim]);
    }

    let mut assign = vec![0usize; rows];
    for _ in 0..iters {
        assign.par_iter_mut().enumerate().for_each(|(r, slot)| {
            let row = &base[r * dim..(r + 1) * dim];
            let mut best_c = 0usize;
            let mut best_s = f32::NEG_INFINITY;
            for c in 0..shards {
                let s = dot_f32(row, &centroids[c * dim..(c + 1) * dim]);
                if s > best_s {
                    best_s = s;
                    best_c = c;
                }
            }
            *slot = best_c;
        });

        let mut sums = vec![0.0f64; shards * dim];
        let mut counts = vec![0usize; shards];
        for (r, &c) in assign.iter().enumerate() {
            counts[c] += 1;
            let row = &base[r * dim..(r + 1) * dim];
            let dst = &mut sums[c * dim..(c + 1) * dim];
            for d in 0..dim {
                dst[d] += f64::from(row[d]);
            }
        }
        for c in 0..shards {
            if counts[c] == 0 {
                let pick = (rng.next_u64() as usize) % rows;
                centroids[c * dim..(c + 1) * dim]
                    .copy_from_slice(&base[pick * dim..(pick + 1) * dim]);
            } else {
                for d in 0..dim {
                    centroids[c * dim + d] = sums[c * dim + d] as f32;
                }
            }
            normalize_in_place(&mut centroids[c * dim..(c + 1) * dim]);
        }
    }

    (assign, centroids)
}

fn cmp_score_id_desc(a: &(f32, u64), b: &(f32, u64)) -> Ordering {
    b.0.total_cmp(&a.0).then_with(|| a.1.cmp(&b.1))
}

fn run_build() -> Result<(), String> {
    let fixture_dir = PathBuf::from(arg("fixture").ok_or("--fixture=<dir> is required")?);
    let out_dir = PathBuf::from(arg("out").ok_or("--out=<dir> is required")?);
    let placement = arg_or("placement", "hash");
    let shards: usize = arg_or("shards", &DEFAULT_SHARDS.to_string())
        .parse()
        .map_err(|e| format!("--shards: {e}"))?;
    let slot_stride: u64 = arg_or("slot-stride", &DEFAULT_SLOT_STRIDE.to_string())
        .parse()
        .map_err(|e| format!("--slot-stride: {e}"))?;
    let max_k: usize = arg_or("max-k", &DEFAULT_MAX_K.to_string())
        .parse()
        .map_err(|e| format!("--max-k: {e}"))?;
    let m: u32 = arg_or("m", "16").parse().map_err(|e| format!("--m: {e}"))?;
    let ef_construction: u32 = arg_or("ef-construction", "200")
        .parse()
        .map_err(|e| format!("--ef-construction: {e}"))?;
    let max_queries: Option<usize> = arg("max-queries")
        .map(|s| s.parse().map_err(|e| format!("--max-queries: {e}")))
        .transpose()?;

    fs::create_dir_all(&out_dir).map_err(|e| format!("mkdir {}: {e}", out_dir.display()))?;
    let fx_manifest: FixtureManifest = serde_json::from_str(
        &fs::read_to_string(fixture_dir.join("manifest.json"))
            .map_err(|e| format!("read fixture manifest: {e}"))?,
    )
    .map_err(|e| format!("parse fixture manifest: {e}"))?;
    let dim = fx_manifest.dim;

    let base = read_f32_vec(&fixture_dir.join("base.f32"))?;
    let rows = base.len() / dim;
    let mut queries = read_f32_vec(&fixture_dir.join("queries.f32"))?;
    let total_queries = queries.len() / dim;
    let query_count = max_queries.map_or(total_queries, |mq| mq.min(total_queries));
    queries.truncate(query_count * dim);

    eprintln!(
        "building {shards}-shard {placement} fleet from {} ({rows} rows x {dim}d, {query_count} queries)",
        fixture_dir.display()
    );
    let t0 = Instant::now();
    let (assign, centroids) = match placement.as_str() {
        "hash" => {
            let assign: Vec<usize> = (0..rows).map(|r| r % shards).collect();
            let mut sums = vec![0.0f64; shards * dim];
            for (r, &s) in assign.iter().enumerate() {
                let row = &base[r * dim..(r + 1) * dim];
                for d in 0..dim {
                    sums[s * dim + d] += f64::from(row[d]);
                }
            }
            let mut centroids = vec![0.0f32; shards * dim];
            for s in 0..shards {
                for d in 0..dim {
                    centroids[s * dim + d] = sums[s * dim + d] as f32;
                }
                normalize_in_place(&mut centroids[s * dim..(s + 1) * dim]);
            }
            (assign, centroids)
        }
        "kmeans" => spherical_kmeans(
            &base,
            rows,
            dim,
            shards,
            DEFAULT_KMEANS_ITERS,
            DEFAULT_SEED ^ 0x517C_C1B7_2722_0A95,
        ),
        other => {
            return Err(format!(
                "unknown --placement={other:?}; expected hash or kmeans"
            ))
        }
    };

    let mut shard_vecs: Vec<Vec<f32>> = (0..shards).map(|_| Vec::new()).collect();
    let mut global_ids: Vec<u64> = vec![0; rows];
    let mut shard_rows = vec![0usize; shards];
    for (r, &s) in assign.iter().enumerate() {
        let local_slot = shard_rows[s] as u64;
        if local_slot >= slot_stride {
            return Err(format!(
                "shard {s} row count exceeds --slot-stride={slot_stride}"
            ));
        }
        shard_rows[s] += 1;
        global_ids[r] = (s as u64) * slot_stride + local_slot;
        shard_vecs[s].extend_from_slice(&base[r * dim..(r + 1) * dim]);
    }
    eprintln!(
        "placement ready in {:.2}s; shard sizes min={} max={}",
        t0.elapsed().as_secs_f64(),
        shard_rows.iter().min().copied().unwrap_or(0),
        shard_rows.iter().max().copied().unwrap_or(0),
    );

    let backend_cfg = HnswConfig {
        m,
        ef_construction,
        build_mode: BuildMode::Parallel,
        seed: DEFAULT_SEED,
        locality_order: LocalityOrder::None,
    }
    .backend_config()
    .map_err(|e| format!("hnsw backend_config: {e}"))?;

    for (s, vecs) in shard_vecs.iter().enumerate() {
        let st = Instant::now();
        let mut idx = VectorIndex::from_backend_config(dim, &backend_cfg)
            .map_err(|e| format!("construct shard {s}: {e}"))?;
        if !vecs.is_empty() {
            idx.add(vecs, dim)
                .map_err(|e| format!("add shard {s}: {e}"))?;
        }
        idx.prepare()
            .map_err(|e| format!("prepare shard {s}: {e}"))?;
        let shard_path = out_dir.join(format!("shard-{s:02}.hnsw"));
        idx.write(&shard_path)
            .map_err(|e| format!("write {}: {e}", shard_path.display()))?;
        eprintln!(
            "  built shard-{s:02}.hnsw ({} rows) in {:.2}s",
            shard_rows[s],
            st.elapsed().as_secs_f64()
        );
    }

    let gt_k = max_k.min(rows);
    let gt_t0 = Instant::now();
    let gt_flat: Vec<u64> = (0..query_count)
        .into_par_iter()
        .flat_map_iter(|qi| {
            let q = &queries[qi * dim..(qi + 1) * dim];
            let mut scored: Vec<(f32, u64)> = (0..rows)
                .map(|r| (dot_f32(q, &base[r * dim..(r + 1) * dim]), global_ids[r]))
                .collect();
            if gt_k < scored.len() {
                scored.select_nth_unstable_by(gt_k - 1, cmp_score_id_desc);
                scored.truncate(gt_k);
            }
            scored.sort_unstable_by(cmp_score_id_desc);
            scored.into_iter().map(|(_, id)| id)
        })
        .collect();
    eprintln!(
        "computed exact global top-{gt_k} ground truth for {query_count} queries in {:.2}s",
        gt_t0.elapsed().as_secs_f64()
    );

    write_f32_vec(&out_dir.join("queries.f32"), &queries)?;
    write_f32_vec(&out_dir.join("centroids.f32"), &centroids)?;
    write_u64_vec(&out_dir.join("gt_u64.bin"), &gt_flat)?;
    let manifest = FleetManifest {
        dim,
        rows,
        queries: query_count,
        shards,
        slot_stride,
        max_k: gt_k,
        placement,
        shard_rows,
    };
    fs::write(
        out_dir.join("manifest.json"),
        serde_json::to_string_pretty(&manifest).map_err(|e| format!("serialize manifest: {e}"))?,
    )
    .map_err(|e| format!("write manifest.json: {e}"))?;
    eprintln!("fleet dataset ready at {} ({HNSW})", out_dir.display());
    Ok(())
}

#[derive(Clone)]
struct ModeSpec {
    name: String,
    floor_prune: Option<bool>,
    floor_greediness_ppm: Option<u32>,
    floor_clamp_min: Option<u32>,
}

fn parse_modes(raw: Option<String>) -> Result<Vec<ModeSpec>, String> {
    let Some(raw) = raw else {
        return Ok(vec![ModeSpec {
            name: "default".to_string(),
            floor_prune: None,
            floor_greediness_ppm: None,
            floor_clamp_min: None,
        }]);
    };
    let mut out = Vec::new();
    for item in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let spec = match item {
            "no_floor" | "emit_only" | "off" => ModeSpec {
                name: item.to_string(),
                floor_prune: Some(false),
                floor_greediness_ppm: None,
                floor_clamp_min: None,
            },
            "gate_k" => ModeSpec {
                name: item.to_string(),
                floor_prune: Some(true),
                floor_greediness_ppm: Some(0),
                floor_clamp_min: Some(32),
            },
            "clamp_050" => ModeSpec {
                name: item.to_string(),
                floor_prune: Some(true),
                floor_greediness_ppm: Some(500_000),
                floor_clamp_min: Some(32),
            },
            "hop0" => ModeSpec {
                name: item.to_string(),
                floor_prune: Some(true),
                floor_greediness_ppm: Some(1_000_000),
                floor_clamp_min: Some(1),
            },
            other => return Err(format!("unknown --modes entry {other:?}")),
        };
        out.push(spec);
    }
    Ok(out)
}

fn parse_opt_u32_list(raw: &str) -> Result<Vec<Option<u32>>, String> {
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| match s {
            "off" | "none" | "0" => Ok(None),
            v => v
                .parse::<u32>()
                .map(Some)
                .map_err(|e| format!("invalid u32 window {v:?}: {e}")),
        })
        .collect()
}

fn parse_u32_list(raw: &str) -> Result<Vec<u32>, String> {
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| {
            s.parse::<u32>()
                .map_err(|e| format!("invalid u32 {s:?}: {e}"))
        })
        .collect()
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[idx]
}

async fn wait_nodes_ready(nodes: &[String]) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(60);
    for addr in nodes {
        loop {
            if security().connect(addr).await.is_ok() {
                break;
            }
            if Instant::now() >= deadline {
                return Err(format!("node at {addr} never became reachable within 60s"));
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
    Ok(())
}

async fn run_sweep() -> Result<(), String> {
    let fleet_dir = PathBuf::from(arg("fleet").ok_or("--fleet=<dir> is required")?);
    let nodes_raw = arg("nodes").ok_or("--nodes=host:port,... is required")?;
    let nodes: Vec<String> = nodes_raw
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| security().url(s))
        .collect();
    let label = arg_or("label", "fleet");
    let k_list = parse_u32_list(&arg_or("k", "100,1000,10000"))?;
    let settle_default = if arg("modes").is_some() {
        "off"
    } else {
        "off,250,500,1000,2000"
    };
    let settle_list = parse_opt_u32_list(&arg_or("settle-us", settle_default))?;
    let deadline_list = parse_opt_u32_list(&arg_or("deadline-us", "off"))?;
    let modes = parse_modes(arg("modes"))?;
    let repeats: usize = arg_or("repeats", "5")
        .parse()
        .map_err(|e| format!("--repeats: {e}"))?;
    let warmup: usize = arg_or("warmup", "10")
        .parse()
        .map_err(|e| format!("--warmup: {e}"))?;
    let query_limit: Option<usize> = arg("queries")
        .map(|s| s.parse().map_err(|e| format!("--queries: {e}")))
        .transpose()?;
    let json_out = arg("json").map(PathBuf::from);

    let manifest: FleetManifest = serde_json::from_str(
        &fs::read_to_string(fleet_dir.join("manifest.json"))
            .map_err(|e| format!("read manifest.json: {e}"))?,
    )
    .map_err(|e| format!("parse manifest.json: {e}"))?;
    if nodes.len() != manifest.shards {
        return Err(format!(
            "--nodes lists {} addresses, but manifest declares {} shards",
            nodes.len(),
            manifest.shards
        ));
    }
    let queries_all = read_f32_vec(&fleet_dir.join("queries.f32"))?;
    let gt_all = read_u64_vec(&fleet_dir.join("gt_u64.bin"))?;
    let n_queries = query_limit
        .map_or(manifest.queries, |ql| ql.min(manifest.queries))
        .max(1);

    wait_nodes_ready(&nodes).await?;
    let coordinator = CoordinatorServiceImpl::new(nodes.clone()).with_stream_search(true);

    #[derive(Clone)]
    struct CellSpec {
        k: u32,
        settle_us: Option<u32>,
        deadline_us: Option<u32>,
        mode: ModeSpec,
    }

    #[derive(Default)]
    struct CellAccum {
        walls_ms: Vec<f64>,
        recalls: Vec<f64>,
        elapsed_s: f64,
        units_scanned: u64,
        shard_min_scanned: u64,
        shard_median_scanned: u64,
        shard_max_scanned: u64,
        emitted: u64,
        floors_sent: u64,
        shards_finished_early: u64,
        shards_floor_pruned: u64,
        shards_patience_stopped: u64,
    }

    let mut specs = Vec::new();
    for &k in &k_list {
        if k as usize > manifest.max_k {
            return Err(format!("k={k} exceeds manifest max_k={}", manifest.max_k));
        }
        for mode in &modes {
            for &deadline_us in &deadline_list {
                for &settle_us in &settle_list {
                    specs.push(CellSpec {
                        k,
                        settle_us,
                        deadline_us,
                        mode: mode.clone(),
                    });
                }
            }
        }
    }
    let mut accums: Vec<CellAccum> = (0..specs.len()).map(|_| CellAccum::default()).collect();

    // Warm up every node connection and page in mapped HNSW pages.
    let warmup_filters = RequestFilters::default();
    for qi in 0..warmup.min(n_queries) {
        let q = &queries_all[qi * manifest.dim..(qi + 1) * manifest.dim];
        coordinator
            .fanout_stream_search(&format!("warmup-{qi}"), q, k_list[0], None, &warmup_filters)
            .await
            .map_err(|e| format!("warmup query {qi}: {e}"))?;
    }

    // Staggered repeats across all CellSpecs to cancel thermal/network drift.
    for rep in 0..repeats {
        for idx in 0..specs.len() {
            let cell_idx = (idx + rep) % specs.len();
            let spec = &specs[cell_idx];
            let filters = RequestFilters {
                finish_settle_us: spec.settle_us,
                finish_deadline_us: spec.deadline_us,
                floor_prune: spec.mode.floor_prune,
                floor_k: Some(spec.k),
                floor_greediness_ppm: spec.mode.floor_greediness_ppm,
                floor_clamp_min: spec.mode.floor_clamp_min,
                ..Default::default()
            };
            let cell_t0 = Instant::now();
            for qi in 0..n_queries {
                let q = &queries_all[qi * manifest.dim..(qi + 1) * manifest.dim];
                let q_t0 = Instant::now();
                let res = coordinator
                    .fanout_stream_search(
                        &format!("r{rep}-c{cell_idx}-q{qi}"),
                        q,
                        spec.k,
                        None,
                        &filters,
                    )
                    .await
                    .map_err(|e| {
                        format!("search mode={} k={} q={qi}: {e}", spec.mode.name, spec.k)
                    })?;
                let wall_ms = q_t0.elapsed().as_secs_f64() * 1e3;

                let gt_slice =
                    &gt_all[qi * manifest.max_k..qi * manifest.max_k + (spec.k as usize)];
                let truth: HashSet<u64> = gt_slice.iter().copied().collect();
                let matched = res
                    .hits
                    .iter()
                    .take(spec.k as usize)
                    .filter(|h| truth.contains(&h.vector_id))
                    .count();
                let acc = &mut accums[cell_idx];
                acc.walls_ms.push(wall_ms);
                acc.recalls.push((matched as f64) / f64::from(spec.k));
                acc.floors_sent += res.floors_sent;
                acc.shards_finished_early += u64::from(res.shards_finished_early);
                acc.shards_floor_pruned += u64::from(res.shards_floor_pruned);
                let mut shard_scans: Vec<u64> = Vec::with_capacity(res.summaries.len());
                for s in &res.summaries {
                    acc.units_scanned += s.blocks_scanned;
                    acc.emitted += s.emitted;
                    acc.shards_patience_stopped += u64::from(s.patience_stopped);
                    shard_scans.push(s.blocks_scanned);
                }
                if !shard_scans.is_empty() {
                    shard_scans.sort_unstable();
                    acc.shard_min_scanned += shard_scans[0];
                    acc.shard_median_scanned += shard_scans[shard_scans.len() / 2];
                    acc.shard_max_scanned += *shard_scans.last().unwrap_or(&0);
                }
            }
            accums[cell_idx].elapsed_s += cell_t0.elapsed().as_secs_f64();
        }
    }

    println!(
        "| label | mode | placement | k | settle | recall | min_rec | p50 ms | p99 ms | QPS | scanned/q | shard min/med/max | emitted/q | floors/q | pruned_sh/q |"
    );
    println!("|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|---:|---:|");

    let mut json_file = match &json_out {
        Some(path) => {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)
                    .map_err(|e| format!("mkdir {}: {e}", parent.display()))?;
            }
            Some(File::create(path).map_err(|e| format!("create jsonl {}: {e}", path.display()))?)
        }
        None => None,
    };

    for (spec, acc) in specs.iter().zip(accums.iter_mut()) {
        acc.walls_ms.sort_by(|a, b| a.total_cmp(b));
        acc.recalls.sort_by(|a, b| a.total_cmp(b));
        let total_q = acc.walls_ms.len().max(1) as f64;
        let mean_ms = acc.walls_ms.iter().sum::<f64>() / total_q;
        let mean_recall = acc.recalls.iter().sum::<f64>() / total_q;
        let recall_var = acc
            .recalls
            .iter()
            .map(|&r| (r - mean_recall) * (r - mean_recall))
            .sum::<f64>()
            / total_q;
        let rec = SweepRecord {
            label: label.clone(),
            mode: spec.mode.name.clone(),
            placement: manifest.placement.clone(),
            shards: manifest.shards,
            k: spec.k,
            settle_us: spec.settle_us,
            deadline_us: spec.deadline_us,
            repeats,
            queries_per_repeat: n_queries,
            recall: mean_recall,
            recall_min: acc.recalls.first().copied().unwrap_or(0.0),
            recall_p05: percentile(&acc.recalls, 0.05),
            recall_std: recall_var.sqrt(),
            p50_ms: percentile(&acc.walls_ms, 0.50),
            p90_ms: percentile(&acc.walls_ms, 0.90),
            p99_ms: percentile(&acc.walls_ms, 0.99),
            mean_ms,
            qps: total_q / acc.elapsed_s.max(1e-9),
            units_scanned_per_query: (acc.units_scanned as f64) / total_q,
            shard_min_scanned_per_query: (acc.shard_min_scanned as f64) / total_q,
            shard_median_scanned_per_query: (acc.shard_median_scanned as f64) / total_q,
            shard_max_scanned_per_query: (acc.shard_max_scanned as f64) / total_q,
            emitted_per_query: (acc.emitted as f64) / total_q,
            floors_sent_per_query: (acc.floors_sent as f64) / total_q,
            shards_finished_early_per_query: (acc.shards_finished_early as f64) / total_q,
            shards_floor_pruned_per_query: (acc.shards_floor_pruned as f64) / total_q,
            shards_patience_stopped_per_query: (acc.shards_patience_stopped as f64) / total_q,
        };
        let settle_str = rec
            .settle_us
            .map_or_else(|| "off".to_string(), |v| v.to_string());
        println!(
            "| {} | {} | {} | {} | {} | {:.4} | {:.4} | {:.2} | {:.2} | {:.1} | {:.0} | {:.0}/{:.0}/{:.0} | {:.0} | {:.1} | {:.2} |",
            rec.label,
            rec.mode,
            rec.placement,
            rec.k,
            settle_str,
            rec.recall,
            rec.recall_min,
            rec.p50_ms,
            rec.p99_ms,
            rec.qps,
            rec.units_scanned_per_query,
            rec.shard_min_scanned_per_query,
            rec.shard_median_scanned_per_query,
            rec.shard_max_scanned_per_query,
            rec.emitted_per_query,
            rec.floors_sent_per_query,
            rec.shards_floor_pruned_per_query,
        );
        if let Some(f) = json_file.as_mut() {
            writeln!(
                f,
                "{}",
                serde_json::to_string(&rec).map_err(|e| format!("json: {e}"))?
            )
            .map_err(|e| format!("write jsonl: {e}"))?;
        }
    }
    Ok(())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let mode = std::env::args().nth(1).unwrap_or_default();
    let res = match mode.as_str() {
        "build" => run_build(),
        "sweep" => run_sweep().await,
        _ => Err("usage: hnsw_fleet_bench <build|sweep> [--flags...]\n  \
             build --fixture=<dir> --out=<dir> [--placement=hash|kmeans] [--shards=16]\n  \
             sweep --fleet=<dir> --nodes=h1:p1,... [--k=100,1000,10000] [--modes=...]"
            .to_string()),
    };
    if let Err(err) = res {
        eprintln!("hnsw_fleet_bench error: {err}");
        std::process::exit(1);
    }
}

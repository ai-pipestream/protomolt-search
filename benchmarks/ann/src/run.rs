//! `ann_bench run`: build every engine at every sweep point, over the whole
//! fixture, over sealed segments plus a tail, or over shards, and measure
//! every filter cell at every query breadth. Writes one results file.
//!
//! The sharded shape (`--placement=hash|skewed --shards=N`) splits the rows
//! over N shards by a placement the fixture wrote, builds one index per
//! shard, and answers a query by asking every shard for its local top k and
//! merging those to the global top k. It runs in one process: no
//! coordinator, no transport, and the fan-out is sequential within a query.
//! There is no shared floor between shards yet (#72), so every sharded
//! result is the floor-off shape.

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::engines::{self, Allow, BuildParams, Index, QueryStats};
use crate::fixture::{self, Fixture, PLACEMENTS};
use crate::gate::sharded_shape_name;
use crate::util::{
    arg, by_score_desc, harness, host, host_threads, list_str, list_usize, rss_bytes, write_json,
    Error,
};

pub const FORMAT: &str = "protomolt-ann-bench-results-v1";

/// The shard count of the sharded shape when `--shards` is not given.
const DEFAULT_SHARDS: &str = "4";

/// Sweep defaults of the single and segmented shapes.
const K: &str = "10,100";
const EF: &str = "16,32,48,64,96,128,192,256,384,512";
const RERANK: &str = "10,20,50,100,200,500,1000,2000,5000";

/// Sweep defaults of the sharded shape: k of 100, 1,000 and 10,000, and
/// breadths that reach past the largest k (a breadth below k is skipped).
const SHARDED_K: &str = "100,1000,10000";
const SHARDED_EF: &str =
    "100,128,192,256,384,512,768,1000,1536,2048,3072,4096,6144,8192,10000,12288,16384,24576,32768";
const SHARDED_RERANK: &str = "100,200,500,1000,2000,5000,10000,20000,50000";

/// Where a part's local rows sit among the fixture's rows.
#[derive(Clone)]
pub enum Global {
    /// A segment: contiguous rows from this global row on.
    Offset(usize),
    /// A shard: the global row of every local row, ascending.
    Rows(Arc<Vec<u32>>),
}

impl Global {
    #[inline]
    fn row(&self, local: u32) -> u32 {
        match self {
            Global::Offset(offset) => *offset as u32 + local,
            Global::Rows(rows) => rows[local as usize],
        }
    }
}

/// One index of a built shape and where its rows sit.
pub struct Part {
    pub index: Box<dyn Index>,
    pub global: Global,
}

/// One built shape: its parts (one, segments in row order, or shards in
/// shard order).
pub struct Shape {
    pub parts: Vec<Part>,
}

/// The rows one part indexes, prepared once per run so every build reads
/// the same copy and the copy is not counted in a build's retained memory.
struct Source {
    rows: engines::Rows,
    global: Global,
}

/// The shape a run builds.
enum Layout {
    Single,
    Segmented(usize),
    Sharded { placement: String, shards: usize },
}

/// The shape from `--segment-rows`, or `--placement` and `--shards`.
fn layout() -> Result<Layout, Error> {
    let segment_rows: usize = arg("segment-rows", "0").parse()?;
    let placement = arg("placement", "");
    let shards = arg("shards", "");
    if placement.is_empty() {
        if !shards.is_empty() {
            return Err("--shards needs --placement=hash|skewed".into());
        }
        return Ok(if segment_rows == 0 {
            Layout::Single
        } else {
            Layout::Segmented(segment_rows)
        });
    }
    if !PLACEMENTS.contains(&placement.as_str()) {
        return Err(format!("--placement={placement:?} must be one of {PLACEMENTS:?}").into());
    }
    if segment_rows != 0 {
        return Err(
            "--segment-rows and --placement name different shapes; give one of them".into(),
        );
    }
    let raw = if shards.is_empty() {
        DEFAULT_SHARDS
    } else {
        &shards
    };
    let shards: usize = raw
        .parse()
        .map_err(|e| format!("--shards={raw:?} is not a shard count: {e}"))?;
    if shards < 2 {
        return Err(format!("--shards={shards}: the sharded shape needs at least 2 shards").into());
    }
    Ok(Layout::Sharded { placement, shards })
}

pub struct Settings {
    pub ks: Vec<usize>,
    pub efs: Vec<usize>,
    pub rerank_depths: Vec<usize>,
    pub min_time: Duration,
    pub threads: usize,
    pub recall_stop: f64,
    pub min_qps: f64,
    pub filters: Vec<usize>,
}

/// The global top `k` of every part's local top `k`: score descending, then
/// global row ascending, exactly (a full selection, not a bounded heap, so
/// ties at the k-th score resolve by row).
pub fn merge_top(mut hits: Vec<(u32, f32)>, k: usize) -> Vec<(u32, f32)> {
    if hits.len() > k {
        hits.select_nth_unstable_by(k - 1, by_score_desc);
        hits.truncate(k);
    }
    hits.sort_unstable_by(by_score_desc);
    hits
}

/// One query: the mask from the column index (timed, reported as the
/// predicate cost), every part searched, results merged. `stats` holds one
/// entry per part, to which each part's search adds its work.
#[allow(clippy::too_many_arguments)]
fn one_query(
    fx: &Fixture,
    shape: &Shape,
    wants_bools: bool,
    filter: usize,
    q: usize,
    k: usize,
    ef: usize,
    stats: &mut [QueryStats],
) -> (Vec<u32>, Duration) {
    let started = Instant::now();
    let allow = (filter > 0).then(|| {
        let f = &fx.filters[filter - 1];
        let bits = fx.index.mask(f.column, f.values(q));
        // The product's dense mask today is a Vec<bool> built per query
        // (resolve_shard_filters); engines on that seam pay for it here.
        let bools = wants_bools.then(|| (0..bits.len()).map(|i| bits.get(i)).collect());
        Allow { bits, bools }
    });
    let predicate = started.elapsed();
    let query = fx.rows.query(q);
    let ids = if shape.parts.len() == 1 {
        let part = &shape.parts[0];
        part.index
            .search(query, k, ef, allow.as_ref(), &mut stats[0])
            .into_iter()
            .map(|(r, _)| part.global.row(r))
            .collect()
    } else {
        let mut hits = Vec::with_capacity(k * shape.parts.len());
        for (part, part_stats) in shape.parts.iter().zip(stats.iter_mut()) {
            for (r, s) in part.index.search(query, k, ef, allow.as_ref(), part_stats) {
                hits.push((part.global.row(r), s));
            }
        }
        merge_top(hits, k).into_iter().map(|(r, _)| r).collect()
    };
    (ids, predicate)
}

fn recall(fx: &Fixture, filter: usize, q: usize, k: usize, got: &[u32]) -> f64 {
    let truth = fx.truth_for(filter, q);
    debug_assert!(k <= truth.len(), "k is checked against the truth depth");
    let truth: Vec<u32> = truth
        .iter()
        .take(k)
        .copied()
        .filter(|r| *r != u32::MAX)
        .collect();
    if truth.is_empty() {
        return 1.0;
    }
    let got: std::collections::HashSet<u32> = got.iter().take(k).copied().collect();
    truth.iter().filter(|r| got.contains(r)).count() as f64 / truth.len() as f64
}

fn percentile(sorted: &[f64], f: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    sorted[((sorted.len() - 1) as f64 * f).round() as usize]
}

/// The median of sorted values: the middle one, or the mean of the two
/// middle ones for an even count.
fn median(sorted: &[f64]) -> f64 {
    let n = sorted.len();
    if n % 2 == 1 {
        sorted[n / 2]
    } else {
        (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0
    }
}

/// Per-part work over the recall pass: the sum over queries of each
/// query's least, median and most working part, for hops and exact scores.
#[derive(Default)]
struct PartWork {
    hops: [f64; 3],
    exact: [f64; 3],
}

impl PartWork {
    fn add(&mut self, stats: &[QueryStats]) {
        let hops: Vec<f64> = stats.iter().map(|s| s.hops as f64).collect();
        let exact: Vec<f64> = stats.iter().map(|s| s.exact as f64).collect();
        for (sums, mut v) in [(&mut self.hops, hops), (&mut self.exact, exact)] {
            v.sort_by(f64::total_cmp);
            sums[0] += v[0];
            sums[1] += median(&v);
            sums[2] += v[v.len() - 1];
        }
    }

    /// Means over `queries`; null where the engine reported no such work.
    fn to_json(&self, parts: usize, queries: usize, total: &QueryStats) -> Value {
        let means = |sums: &[f64; 3], reported: bool| {
            if !reported {
                return Value::Null;
            }
            let q = queries as f64;
            json!({ "min_mean": sums[0] / q, "median_mean": sums[1] / q, "max_mean": sums[2] / q })
        };
        json!({
            "parts": parts,
            "hops": means(&self.hops, total.hops > 0),
            "exact_scores": means(&self.exact, total.exact > 0),
        })
    }
}

/// Measure one (filter, k, ef) point.
fn measure(
    fx: &Fixture,
    shape: &Shape,
    wants_bools: bool,
    filter: usize,
    k: usize,
    ef: usize,
    s: &Settings,
) -> Value {
    let qn = fx.rows.query_count();
    let parts = shape.parts.len();
    // Recall pass: every query once, timed. It is also the hop and
    // exact-score pass, and the warm-up for the timed passes below.
    let mut recalls = Vec::with_capacity(qn);
    let mut stats = QueryStats::default();
    let mut per_part = vec![QueryStats::default(); parts];
    let mut part_work = PartWork::default();
    let mut latencies = Vec::with_capacity(qn);
    let mut predicate = Duration::ZERO;
    let started = Instant::now();
    for q in 0..qn {
        per_part.fill(QueryStats::default());
        let t = Instant::now();
        let (ids, p) = one_query(fx, shape, wants_bools, filter, q, k, ef, &mut per_part);
        latencies.push(t.elapsed().as_secs_f64() * 1e3);
        predicate += p;
        recalls.push(recall(fx, filter, q, k, &ids));
        for part in &per_part {
            stats.add(part);
        }
        part_work.add(&per_part);
        // A point slower than --min-qps on one core is abandoned after 50
        // queries; the gate ignores it and the breadth sweep stops.
        if s.min_qps > 0.0 && q + 1 >= 50 && q + 1 < qn {
            let rate = (q + 1) as f64 / started.elapsed().as_secs_f64();
            if rate < s.min_qps {
                return json!({
                    "k": k, "ef": ef, "below_qps_floor": true, "queries_measured": q + 1,
                    "qps_one_core": rate,
                    "recall_mean_partial": recalls.iter().sum::<f64>() / recalls.len() as f64,
                });
            }
        }
    }
    let mut one_core_s = started.elapsed().as_secs_f64();
    let mut done = qn;
    // One core: when the recall pass was shorter than the minimum time (a
    // fast point, where the first queries' cold caches would weigh), time
    // separate warm passes of at least min(queries, 200) queries instead.
    if one_core_s < s.min_time.as_secs_f64() || qn < 200 {
        latencies.clear();
        predicate = Duration::ZERO;
        done = 0;
        let min_queries = qn.min(200);
        let started = Instant::now();
        while done < min_queries || started.elapsed() < s.min_time {
            let q = done % qn;
            let t = Instant::now();
            let (_, p) = one_query(fx, shape, wants_bools, filter, q, k, ef, &mut per_part);
            latencies.push(t.elapsed().as_secs_f64() * 1e3);
            predicate += p;
            done += 1;
        }
        one_core_s = started.elapsed().as_secs_f64();
    }
    // All cores: threads pull query indices until the minimum time.
    let next = AtomicUsize::new(0);
    let completed = AtomicUsize::new(0);
    let started = Instant::now();
    let deadline = s.min_time.max(Duration::from_millis(1));
    std::thread::scope(|scope| {
        for _ in 0..s.threads {
            scope.spawn(|| {
                let mut scratch = vec![QueryStats::default(); parts];
                while started.elapsed() < deadline {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    one_query(fx, shape, wants_bools, filter, i % qn, k, ef, &mut scratch);
                    completed.fetch_add(1, Ordering::Relaxed);
                }
            });
        }
    });
    let all_s = started.elapsed().as_secs_f64();
    let all_n = completed.load(Ordering::Relaxed);
    latencies.sort_by(f64::total_cmp);
    let mean_recall = recalls.iter().sum::<f64>() / qn as f64;
    let mut point = json!({
        "k": k,
        "ef": ef,
        "recall_mean": mean_recall,
        "recall_worst": recalls.iter().copied().fold(1.0, f64::min),
        "qps_one_core": done as f64 / one_core_s,
        "qps_all_cores": all_n as f64 / all_s,
        "threads": s.threads,
        "p50_ms": percentile(&latencies, 0.5),
        "p99_ms": percentile(&latencies, 0.99),
        "predicate_ms_mean": predicate.as_secs_f64() * 1e3 / done as f64,
        "hops_mean": stats.hops as f64 / qn as f64,
        "exact_scores_mean": stats.exact as f64 / qn as f64,
        "bridges_mean": stats.bridges as f64 / qn as f64,
        "budget_handoffs": stats.budget_handoffs,
        "regimes": {
            "enumerate": stats.regimes[0],
            "filter_aware": stats.regimes[1],
            "plain": stats.regimes[2],
        },
    });
    if parts > 1 {
        point["per_part"] = part_work.to_json(parts, qn, &stats);
    }
    point
}

/// Segment boundaries: sealed segments of `segment_rows`, the rest a tail.
pub fn segment_ranges(rows: usize, segment_rows: usize) -> Vec<(usize, usize)> {
    if segment_rows == 0 || segment_rows >= rows {
        return vec![(0, rows)];
    }
    let mut out = Vec::new();
    let mut at = 0;
    while at < rows {
        let len = segment_rows.min(rows - at);
        out.push((at, len));
        at += len;
    }
    out
}

/// The global rows of every shard, ascending, from a row-to-shard placement.
pub fn shard_rows(placement: &[u16], shards: usize) -> Vec<Vec<u32>> {
    let mut rows = vec![Vec::new(); shards];
    for (row, shard) in placement.iter().enumerate() {
        rows[*shard as usize].push(row as u32);
    }
    rows
}

/// For each k: the mean over queries of the largest share of the query's
/// true top k that one shard holds. Near 1/shards under hash placement;
/// near 1 when a placement keeps a query's neighbors together.
fn truth_top_share(fx: &Fixture, placement: &[u16], shards: usize, ks: &[usize]) -> Value {
    let qn = fx.rows.query_count();
    let mut out = serde_json::Map::new();
    for &k in ks {
        let mut sum = 0.0;
        for q in 0..qn {
            let mut held = vec![0usize; shards];
            for r in fx.truth_for(0, q).iter().take(k) {
                held[placement[*r as usize] as usize] += 1;
            }
            sum += *held.iter().max().unwrap_or(&0) as f64 / k as f64;
        }
        out.insert(k.to_string(), json!(sum / qn as f64));
    }
    Value::Object(out)
}

/// The query breadths an engine sweeps for `k`: none to sweep (one point
/// at k), rerank depths for the exhaustive path, beams otherwise. Breadths
/// below k are skipped, so a beam is always at least k and may exceed it.
fn breadths(name: &str, info: &engines::EngineInfo, k: usize, s: &Settings) -> Vec<usize> {
    if !info.sweeps_ef {
        vec![k]
    } else if name == "exhaustive-tq" {
        s.rerank_depths
            .iter()
            .copied()
            .filter(|d| *d >= k)
            .collect()
    } else {
        s.efs.iter().copied().filter(|e| *e >= k).collect()
    }
}

pub fn command() -> Result<(), Error> {
    let dir = PathBuf::from(arg("fixture", ""));
    if dir.as_os_str().is_empty() {
        return Err("--fixture=DIR is required".into());
    }
    let out = PathBuf::from(arg("out", "ann-results.json"));
    let fx = fixture::load(&dir)?;
    let layout = layout()?;
    let sharded = matches!(layout, Layout::Sharded { .. });
    let threads: usize = arg("threads", &host_threads().to_string()).parse()?;
    // Every engine by default; the sharded shape leaves out rust-cv-hnsw,
    // which cannot run it (see below).
    let default_engines: Vec<&str> = engines::ENGINES
        .iter()
        .copied()
        .filter(|e| !sharded || *e != "rust-cv-hnsw")
        .collect();
    let names = list_str("engines", &default_engines.join(","));
    let ms = list_usize("m", "16,32,48,64")?;
    let efcs = list_usize("ef-construction", "100,200,400")?;
    let bits = list_usize("bits", "4")?;
    let filter_names = fx.filter_names();
    let only = list_str("filters", "");
    if let Some(unknown) = only.iter().find(|o| !filter_names.contains(o)) {
        return Err(format!("--filters: {unknown:?} is not a filter of this fixture").into());
    }
    // The sharded shape has no per-shard mask yet: engines read the global
    // allow bitset at their own row offset, which a shard's rows do not
    // follow, so it measures unfiltered cells only.
    if sharded && only.iter().any(|o| o != "none") {
        return Err("the sharded shape measures the unfiltered cell only (--filters=none)".into());
    }
    let filters: Vec<usize> = if sharded {
        vec![0]
    } else if only.is_empty() {
        (0..filter_names.len()).collect()
    } else {
        (0..filter_names.len())
            .filter(|i| only.contains(&filter_names[*i]))
            .collect()
    };
    let (k_default, ef_default, rerank_default) = if sharded {
        (SHARDED_K, SHARDED_EF, SHARDED_RERANK)
    } else {
        (K, EF, RERANK)
    };
    let s = Settings {
        ks: list_usize("k", k_default)?,
        efs: list_usize("ef", ef_default)?,
        rerank_depths: list_usize("rerank", rerank_default)?,
        min_time: Duration::from_millis(arg("min-time-ms", "300").parse()?),
        threads,
        recall_stop: arg("recall-stop", "0.99").parse()?,
        min_qps: arg("min-qps", "0").parse()?,
        filters,
    };
    if s.ks.is_empty() || s.ks.contains(&0) {
        return Err("--k must list values of at least 1".into());
    }
    for &filter in &s.filters {
        let depth = fx.truth_depth(filter);
        if let Some(k) = s.ks.iter().find(|k| **k > depth) {
            return Err(format!(
                "k={k} exceeds the fixture's truth depth {depth} for {}; rebuild the fixture with a \
                 larger {}",
                filter_names[filter],
                if filter == 0 {
                    "--k-truth-none"
                } else {
                    "--k-truth"
                }
            )
            .into());
        }
    }
    // rust-cv's hnsw 0.11 panics when a walk finds fewer rows than the k it
    // was asked for (it copies its candidates into a k-long slice), which a
    // shard's local top 1,000 or 10,000 reaches.
    if sharded && names.iter().any(|n| n == "rust-cv-hnsw") {
        return Err(
            "rust-cv-hnsw cannot run the sharded shape: the library panics when a walk finds fewer \
             than k rows"
                .into(),
        );
    }
    for name in &names {
        let info = engines::info(name)?;
        if let Some(k) =
            s.ks.iter()
                .find(|k| breadths(name, &info, **k, &s).is_empty())
        {
            return Err(format!(
                "{name}: no query breadth of at least k={k} in --ef or --rerank; list one"
            )
            .into());
        }
    }

    let dim = fx.rows.dim;
    let mut report = json!({
        "format": FORMAT,
        "fixture": {
            "name": fx.name(),
            "dir": fx.dir.display().to_string(),
            "rows": fx.rows.rows(),
            "dim": dim,
            "queries": fx.rows.query_count(),
            "base_sha256": fx.manifest["base_sha256"],
            "filters": fx.manifest["filters"],
        },
        "host": host(),
        "harness": harness(),
        "settings": {
            "k": s.ks, "ef": s.efs, "rerank": s.rerank_depths, "m": ms, "ef_construction": efcs,
            "min_time_ms": s.min_time.as_millis() as u64, "threads": s.threads, "recall_stop": s.recall_stop, "min_qps": s.min_qps,
        },
        "engines": [],
    });
    let sources: Vec<Source> = match &layout {
        Layout::Single | Layout::Segmented(_) => {
            let segment_rows = match layout {
                Layout::Segmented(rows) => rows,
                _ => 0,
            };
            let ranges = segment_ranges(fx.rows.rows(), segment_rows);
            report["shape"] = json!(if ranges.len() == 1 {
                "single".to_string()
            } else {
                format!(
                    "segmented:{}x{}+tail{}",
                    ranges.iter().filter(|r| r.1 == segment_rows).count(),
                    segment_rows,
                    ranges
                        .iter()
                        .filter(|r| r.1 != segment_rows)
                        .map(|r| r.1)
                        .sum::<usize>()
                )
            });
            report["segments"] = ranges.iter().map(|r| json!([r.0, r.1])).collect();
            let base = Arc::new(fx.rows.base.clone());
            ranges
                .iter()
                .map(|&(offset, len)| Source {
                    rows: engines::Rows {
                        base: base.clone(),
                        dim,
                        offset,
                        len,
                    },
                    global: Global::Offset(offset),
                })
                .collect()
        }
        Layout::Sharded { placement, shards } => {
            let shard_of = fx.placement(placement, *shards)?;
            let rows = shard_rows(&shard_of, *shards);
            if let Some(empty) = rows.iter().position(Vec::is_empty) {
                return Err(
                    format!("placement {placement}-{shards} leaves shard {empty} empty").into(),
                );
            }
            report["shape"] = json!(sharded_shape_name(*shards, placement, false));
            report["shards"] = json!({
                "count": shards,
                "placement": placement,
                "floor": "off",
                "rows_per_shard": rows.iter().map(Vec::len).collect::<Vec<_>>(),
                "truth_top_share": truth_top_share(&fx, &shard_of, *shards, &s.ks),
                "fan_out": "in process, sequential within a query; each shard returns its local top k \
                            and the merge keeps the global top k by score descending, then global row \
                            ascending",
            });
            rows.into_iter()
                .map(|ids| {
                    let mut base = Vec::with_capacity(ids.len() * dim);
                    for r in &ids {
                        base.extend_from_slice(fx.rows.row(*r as usize));
                    }
                    Source {
                        rows: engines::Rows {
                            base: Arc::new(base),
                            dim,
                            offset: 0,
                            len: ids.len(),
                        },
                        global: Global::Rows(Arc::new(ids)),
                    }
                })
                .collect()
        }
    };

    for name in &names {
        let info = engines::info(name)?;
        let mut builds = Vec::new();
        for p in engines::build_sweep(name, &ms, &efcs, &bits) {
            match build_and_measure(&fx, &sources, name, &info, &p, &s, &filter_names) {
                Ok(b) => builds.push(b),
                Err(e) => {
                    eprintln!("run: {name} {p:?} failed: {e}");
                    builds.push(json!({ "params": p.to_json(), "error": e.to_string() }));
                }
            }
            write_partial(&mut report, &info, &builds, &out)?;
        }
        finish_engine(&mut report, &info, builds);
        write_json(&out, &report)?;
    }
    eprintln!("run: wrote {}", out.display());
    Ok(())
}

fn engine_json(info: &engines::EngineInfo, builds: Vec<Value>) -> Value {
    json!({
        "engine": info.name,
        "library": info.library,
        "version": info.version,
        "license": info.license,
        "filter": info.filter,
        "builds": builds,
    })
}

fn write_partial(
    report: &mut Value,
    info: &engines::EngineInfo,
    builds: &[Value],
    out: &std::path::Path,
) -> Result<(), Error> {
    let mut copy = report.clone();
    copy["engines"]
        .as_array_mut()
        .unwrap()
        .push(engine_json(info, builds.to_vec()));
    write_json(out, &copy)
}

fn finish_engine(report: &mut Value, info: &engines::EngineInfo, builds: Vec<Value>) {
    report["engines"]
        .as_array_mut()
        .unwrap()
        .push(engine_json(info, builds));
}

fn build_and_measure(
    fx: &Fixture,
    sources: &[Source],
    name: &str,
    info: &engines::EngineInfo,
    p: &BuildParams,
    s: &Settings,
    filter_names: &[String],
) -> Result<Value, Error> {
    let rss_before = rss_bytes();
    let started = Instant::now();
    let mut parts = Vec::new();
    for source in sources {
        parts.push(Part {
            index: engines::build(name, source.rows.clone(), p, s.threads)?,
            global: source.global.clone(),
        });
    }
    let build_s = started.elapsed().as_secs_f64();
    let retained = rss_bytes().saturating_sub(rss_before);
    let disk: Option<u64> = parts.iter().map(|part| part.index.disk_bytes()).sum();
    let mut shape = Shape { parts };
    eprintln!("run: {name} {p:?} built in {build_s:.1}s");
    let n = fx.rows.rows() as f64;
    let mut cells = Vec::new();
    for &filter in &s.filters {
        if filter > 0 && !info.supports_filter {
            continue;
        }
        for &k in &s.ks {
            let mut points = Vec::new();
            for ef in breadths(name, info, k, s) {
                for part in shape.parts.iter_mut() {
                    part.index.set_ef(ef);
                }
                let point = measure(fx, &shape, info.wants_bools, filter, k, ef, s);
                let below_floor = point["below_qps_floor"].as_bool().unwrap_or(false);
                let r = point["recall_mean"].as_f64().unwrap_or(0.0);
                points.push(point);
                // Past the highest recall target a larger breadth is only
                // slower, so the gate can never pick it.
                if below_floor || r >= s.recall_stop {
                    break;
                }
                // A plateau: three more breadths added less than 0.005
                // recall, so a larger one will not reach a target either.
                let n = points.len();
                if n >= 4 {
                    let back = points[n - 4]["recall_mean"].as_f64().unwrap_or(0.0);
                    if r - back < 0.005 {
                        break;
                    }
                }
            }
            eprintln!(
                "run: {name} m={} efc={} {} k={k}: {} points",
                p.m,
                p.ef_construction,
                filter_names[filter],
                points.len()
            );
            cells.push(json!({ "filter": filter_names[filter], "k": k, "points": points }));
        }
    }
    Ok(json!({
        "params": p.to_json(),
        "build_s": build_s,
        "retained_rss_bytes": retained,
        "retained_bytes_per_row": retained as f64 / n,
        "disk_bytes": disk,
        "disk_bytes_per_row": disk.map(|d| d as f64 / n),
        "cells": cells,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_merge_keeps_the_global_top_k_and_orders_ties_by_row() {
        // Two shards' local results; rows 7 and 3 tie at 0.5 across shards.
        let hits = vec![(7, 0.5), (1, 0.9), (3, 0.5), (4, 0.1), (2, 0.7)];
        assert_eq!(
            merge_top(hits.clone(), 3),
            vec![(1, 0.9), (2, 0.7), (3, 0.5)]
        );
        assert_eq!(
            merge_top(hits.clone(), 4),
            vec![(1, 0.9), (2, 0.7), (3, 0.5), (7, 0.5)]
        );
        // Fewer hits than k keeps them all, best first.
        assert_eq!(merge_top(hits, 10).len(), 5);
    }

    #[test]
    fn shard_rows_are_ascending_global_rows() {
        let rows = shard_rows(&[1, 0, 1, 1, 0], 2);
        assert_eq!(rows, vec![vec![1, 4], vec![0, 2, 3]]);
        assert_eq!(Global::Rows(Arc::new(rows[1].clone())).row(2), 3);
        assert_eq!(Global::Offset(10).row(2), 12);
    }

    /// Reports fixed work per search, so every counter's path from
    /// `Index::search` to the measured point can be checked.
    struct Counting(QueryStats);

    impl Index for Counting {
        fn search(
            &self,
            _query: &[f32],
            _k: usize,
            _ef: usize,
            _allow: Option<&Allow>,
            stats: &mut QueryStats,
        ) -> Vec<(u32, f32)> {
            stats.add(&self.0);
            vec![(0, 1.0)]
        }
    }

    fn one_row_fixture() -> Fixture {
        Fixture {
            dir: std::path::PathBuf::new(),
            manifest: json!({}),
            rows: crate::fixture::synthetic(4, 4, 1),
            k_truth: 1,
            k_truth_none: 1,
            filters: Vec::new(),
            truth: vec![vec![0]],
            index: crate::fixture::ColumnIndex::new(&[0; 4], &[0; 4], 1),
        }
    }

    #[test]
    fn every_counter_reaches_the_point_whatever_the_part_count() {
        let work = QueryStats {
            hops: 2,
            exact: 3,
            bridges: 7,
            budget_handoffs: 1,
            regimes: [1, 2, 3],
        };
        let fx = one_row_fixture();
        let settings = Settings {
            ks: vec![1],
            efs: vec![1],
            rerank_depths: Vec::new(),
            min_time: Duration::from_millis(1),
            threads: 1,
            recall_stop: 1.0,
            min_qps: 0.0,
            filters: Vec::new(),
        };
        for parts in [1usize, 3] {
            let shape = Shape {
                parts: (0..parts)
                    .map(|_| Part {
                        index: Box::new(Counting(work)),
                        global: Global::Offset(0),
                    })
                    .collect(),
            };
            let point = measure(&fx, &shape, false, 0, 1, 1, &settings);
            let n = parts as f64;
            assert_eq!(point["hops_mean"].as_f64(), Some(2.0 * n), "{parts} parts");
            assert_eq!(
                point["exact_scores_mean"].as_f64(),
                Some(3.0 * n),
                "{parts} parts"
            );
            assert_eq!(
                point["bridges_mean"].as_f64(),
                Some(7.0 * n),
                "{parts} parts"
            );
            assert_eq!(
                point["budget_handoffs"].as_u64(),
                Some(parts as u64),
                "{parts} parts"
            );
            for (name, n) in [("enumerate", 1), ("filter_aware", 2), ("plain", 3)] {
                assert_eq!(
                    point["regimes"][name].as_u64(),
                    Some(n * parts as u64),
                    "{name}, {parts} parts"
                );
            }
        }
    }

    #[test]
    fn the_median_of_an_even_count_is_the_mean_of_the_middle_two() {
        assert_eq!(median(&[1.0, 2.0, 4.0, 8.0]), 3.0);
        assert_eq!(median(&[1.0, 2.0, 4.0]), 2.0);
    }
}

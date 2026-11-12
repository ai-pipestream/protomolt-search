//! `ann_bench run`: build every engine at every sweep point, over the whole
//! fixture or over sealed segments plus a tail, and measure every filter
//! cell at every query breadth. Writes one results file.

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::engines::{self, Allow, BuildParams, Index, QueryStats};
use crate::fixture::{self, Fixture};
use crate::util::{
    arg, harness, host, host_threads, list_str, list_usize, rss_bytes, write_json, Error, TopK,
};

pub const FORMAT: &str = "protomolt-ann-bench-results-v1";

/// One built shape: segments in row order, each with its global offset.
pub struct Shape {
    pub segments: Vec<(usize, Box<dyn Index>)>,
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

/// One query: the mask from the column index (timed, reported as the
/// predicate cost), every segment searched, results merged.
#[allow(clippy::too_many_arguments)]
fn one_query(
    fx: &Fixture,
    shape: &Shape,
    wants_bools: bool,
    filter: usize,
    q: usize,
    k: usize,
    ef: usize,
    stats: &mut QueryStats,
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
    let ids = if shape.segments.len() == 1 {
        shape.segments[0]
            .1
            .search(query, k, ef, allow.as_ref(), stats)
            .into_iter()
            .map(|(r, _)| r)
            .collect()
    } else {
        let mut top = TopK::new(k);
        for (offset, seg) in &shape.segments {
            for (r, s) in seg.search(query, k, ef, allow.as_ref(), stats) {
                top.push((offset + r as usize) as u32, s);
            }
        }
        top.into_sorted().into_iter().map(|(r, _)| r).collect()
    };
    (ids, predicate)
}

fn recall(fx: &Fixture, filter: usize, q: usize, k: usize, got: &[u32]) -> f64 {
    let truth: Vec<u32> = fx
        .truth_for(filter, q)
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
    // Recall pass: every query once, timed. It is also the hop and
    // exact-score pass, and the warm-up for the timed passes below.
    let mut recalls = Vec::with_capacity(qn);
    let mut stats = QueryStats::default();
    let mut latencies = Vec::with_capacity(qn);
    let mut predicate = Duration::ZERO;
    let started = Instant::now();
    for q in 0..qn {
        let t = Instant::now();
        let (ids, p) = one_query(fx, shape, wants_bools, filter, q, k, ef, &mut stats);
        latencies.push(t.elapsed().as_secs_f64() * 1e3);
        predicate += p;
        recalls.push(recall(fx, filter, q, k, &ids));
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
            let (_, p) = one_query(
                fx,
                shape,
                wants_bools,
                filter,
                q,
                k,
                ef,
                &mut QueryStats::default(),
            );
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
                while started.elapsed() < deadline {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    one_query(
                        fx,
                        shape,
                        wants_bools,
                        filter,
                        i % qn,
                        k,
                        ef,
                        &mut QueryStats::default(),
                    );
                    completed.fetch_add(1, Ordering::Relaxed);
                }
            });
        }
    });
    let all_s = started.elapsed().as_secs_f64();
    let all_n = completed.load(Ordering::Relaxed);
    latencies.sort_by(f64::total_cmp);
    let mean_recall = recalls.iter().sum::<f64>() / qn as f64;
    json!({
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
    })
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

pub fn command() -> Result<(), Error> {
    let dir = PathBuf::from(arg("fixture", ""));
    if dir.as_os_str().is_empty() {
        return Err("--fixture=DIR is required".into());
    }
    let out = PathBuf::from(arg("out", "ann-results.json"));
    let fx = fixture::load(&dir)?;
    let threads: usize = arg("threads", &host_threads().to_string()).parse()?;
    let names = list_str("engines", &engines::ENGINES.join(","));
    let ms = list_usize("m", "16,32,48,64")?;
    let efcs = list_usize("ef-construction", "100,200,400")?;
    let bits = list_usize("bits", "4")?;
    let segment_rows: usize = arg("segment-rows", "0").parse()?;
    let all_filters: Vec<usize> = (0..=fx.filters.len()).collect();
    let filter_names = fx.filter_names();
    let only = list_str("filters", "");
    let filters: Vec<usize> = if only.is_empty() {
        all_filters
    } else {
        all_filters
            .into_iter()
            .filter(|i| only.iter().any(|o| *o == filter_names[*i]))
            .collect()
    };
    let s = Settings {
        ks: list_usize("k", "10,100")?,
        efs: list_usize("ef", "16,32,48,64,96,128,192,256,384,512")?,
        rerank_depths: list_usize("rerank", "10,20,50,100,200,500,1000,2000,5000")?,
        min_time: Duration::from_millis(arg("min-time-ms", "300").parse()?),
        threads,
        recall_stop: arg("recall-stop", "0.99").parse()?,
        min_qps: arg("min-qps", "0").parse()?,
        filters,
    };
    let ranges = segment_ranges(fx.rows.rows(), segment_rows);
    let shape_name = if ranges.len() == 1 {
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
    };
    let base = Arc::new(fx.rows.base.clone());

    let mut report = json!({
        "format": FORMAT,
        "fixture": {
            "name": fx.name(),
            "dir": fx.dir.display().to_string(),
            "rows": fx.rows.rows(),
            "dim": fx.rows.dim,
            "queries": fx.rows.query_count(),
            "base_sha256": fx.manifest["base_sha256"],
            "filters": fx.manifest["filters"],
        },
        "shape": shape_name,
        "segments": ranges.iter().map(|r| json!([r.0, r.1])).collect::<Vec<_>>(),
        "host": host(),
        "harness": harness(),
        "settings": {
            "k": s.ks, "ef": s.efs, "rerank": s.rerank_depths, "m": ms, "ef_construction": efcs,
            "min_time_ms": s.min_time.as_millis() as u64, "threads": s.threads, "recall_stop": s.recall_stop, "min_qps": s.min_qps,
        },
        "engines": [],
    });

    for name in &names {
        let info = engines::info(name)?;
        let mut builds = Vec::new();
        for p in engines::build_sweep(name, &ms, &efcs, &bits) {
            match build_and_measure(&fx, &base, &ranges, name, &info, &p, &s, &filter_names) {
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

#[allow(clippy::too_many_arguments)]
fn build_and_measure(
    fx: &Fixture,
    base: &Arc<Vec<f32>>,
    ranges: &[(usize, usize)],
    name: &str,
    info: &engines::EngineInfo,
    p: &BuildParams,
    s: &Settings,
    filter_names: &[String],
) -> Result<Value, Error> {
    let rss_before = rss_bytes();
    let started = Instant::now();
    let mut segments = Vec::new();
    for &(offset, len) in ranges {
        let rows = engines::Rows {
            base: base.clone(),
            dim: fx.rows.dim,
            offset,
            len,
        };
        segments.push((offset, engines::build(name, rows, p, s.threads)?));
    }
    let build_s = started.elapsed().as_secs_f64();
    let retained = rss_bytes().saturating_sub(rss_before);
    let disk: Option<u64> = segments.iter().map(|(_, i)| i.disk_bytes()).sum();
    let mut shape = Shape { segments };
    eprintln!("run: {name} {p:?} built in {build_s:.1}s");
    let n = fx.rows.rows() as f64;
    let mut cells = Vec::new();
    for &filter in &s.filters {
        if filter > 0 && !info.supports_filter {
            continue;
        }
        for &k in &s.ks {
            let breadths: Vec<usize> = if !info.sweeps_ef {
                vec![k]
            } else if name == "exhaustive-tq" {
                s.rerank_depths
                    .iter()
                    .copied()
                    .filter(|d| *d >= k)
                    .collect()
            } else {
                s.efs.iter().copied().filter(|e| *e >= k).collect()
            };
            let mut points = Vec::new();
            for ef in breadths {
                for (_, seg) in shape.segments.iter_mut() {
                    seg.set_ef(ef);
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

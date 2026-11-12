//! `ann_bench patience`: evaluate hop-to-hop saturation early stopping (#95)
//! across `{0.90, 0.95, 0.99} x {7, 15, 30}` for both `beam` (`ef`) and
//! `top_k` (`min(k, ef)`) windows against a fine-grained plain-`ef` Pareto
//! frontier on `challenge` and `court` fixtures (`k = 10, 100`, unfiltered
//! and filtered).

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use pipestream_search::boolean_bits::Bits;
use pipestream_search::hnsw::{BuildMode, HnswConfig, HnswIndex};
use pipestream_search::vector::{
    FilterRegime, PatienceSpec, PatienceWindow, VectorProvider, VectorSearchOptions,
};
use serde_json::{json, Value};

use crate::fixture::{self, Fixture};
use crate::util::{
    arg, harness, host, host_threads, list_str, list_usize, rss_bytes, write_json, Error,
};

pub const FORMAT: &str = "protomolt-ann-patience-v1";

const SATURATIONS: [f32; 3] = [0.90, 0.95, 0.99];
const PATIENCES: [u32; 3] = [7, 15, 30];
const TARGETS: [f64; 3] = [0.95, 0.98, 0.99];

#[derive(Clone, Debug)]
struct Point {
    ef: usize,
    recall_mean: f64,
    recall_worst: f64,
    qps_one_core: f64,
    qps_all_cores: f64,
    hops_mean: f64,
    scored_mean: f64,
    bridges_mean: f64,
    budget_handoffs: u64,
    patience_stopped: u64,
    hops_total: u64,
}

fn recall_at_k(fx: &Fixture, filter: usize, q: usize, k: usize, got: &[(u32, f32)]) -> f64 {
    let truth: Vec<u32> = fx
        .truth_for(filter, q)
        .iter()
        .take(k)
        .copied()
        .filter(|&r| r != u32::MAX)
        .collect();
    if truth.is_empty() {
        return 1.0;
    }
    let found = got
        .iter()
        .take(k)
        .filter(|&&(id, _)| truth.contains(&id))
        .count();
    found as f64 / truth.len() as f64
}

/// Upper Pareto hull of `(recall_mean, qps)`: monotone non-increasing QPS as
/// recall increases.
fn pareto_hull(points: &[Point], all_cores: bool) -> Vec<(f64, f64, usize)> {
    let mut raw: Vec<(f64, f64, usize)> = points
        .iter()
        .map(|p| {
            let qps = if all_cores {
                p.qps_all_cores
            } else {
                p.qps_one_core
            };
            (p.recall_mean, qps, p.ef)
        })
        .collect();
    // Sort by recall descending, then QPS descending, and keep running max QPS.
    raw.sort_by(|a, b| b.0.total_cmp(&a.0).then(b.1.total_cmp(&a.1)));
    let mut hull = Vec::new();
    let mut best_qps = -1.0f64;
    for (r, qps, ef) in raw {
        if qps > best_qps {
            hull.push((r, qps, ef));
            best_qps = qps;
        }
    }
    hull.reverse(); // ascending recall, strictly descending QPS
    hull
}

/// Highest plain-`ef` QPS whose recall is `>= recall - 1e-9`.
fn pareto_best_at_or_above(hull: &[(f64, f64, usize)], recall: f64) -> Option<(f64, f64, usize)> {
    hull.iter().copied().find(|&(r, _, _)| r + 1.0e-9 >= recall)
}

/// Piecewise-linear interpolated QPS on the upper Pareto hull at `recall`.
fn pareto_interp(hull: &[(f64, f64, usize)], recall: f64) -> Option<f64> {
    if hull.is_empty() {
        return None;
    }
    if recall <= hull[0].0 {
        return Some(hull[0].1);
    }
    for pair in hull.windows(2) {
        let (r0, q0, _) = pair[0];
        let (r1, q1, _) = pair[1];
        if recall <= r1 {
            let span = r1 - r0;
            if span <= 1.0e-12 {
                return Some(q0.max(q1));
            }
            let t = ((recall - r0) / span).clamp(0.0, 1.0);
            return Some(q0 + t * (q1 - q0));
        }
    }
    None
}

#[allow(clippy::too_many_arguments)]
fn measure_point(
    fx: &Fixture,
    index: &HnswIndex,
    masks: Option<&[Bits]>,
    pinned_regime: Option<FilterRegime>,
    filter: usize,
    k: usize,
    ef: usize,
    patience: Option<PatienceSpec>,
    min_time: Duration,
    threads: usize,
) -> Point {
    let qn = fx.rows.query_count();
    let make_opts = |q: usize| -> VectorSearchOptions<'_> {
        let mut opts = match masks {
            Some(m) => VectorSearchOptions::new().with_bits(&m[q]),
            None => VectorSearchOptions::new(),
        };
        if let Some(regime) = pinned_regime {
            if masks.is_some() {
                opts = opts.with_filter_regime(regime);
            }
        }
        if let Some(spec) = patience {
            opts = opts.with_patience(spec);
        }
        opts
    };

    let mut recalls = Vec::with_capacity(qn);
    let mut hops_total = 0u64;
    let mut scored_total = 0u64;
    let mut bridges_total = 0u64;
    let mut budget_handoffs = 0u64;
    let mut patience_stopped = 0u64;

    let started = Instant::now();
    for q in 0..qn {
        let query = fx.rows.query(q);
        let (hits, ans) = index
            .search_with_stats(query, k, ef, make_opts(q))
            .expect("hnsw patience search");
        hops_total += ans.stats.hops;
        scored_total += ans.stats.scored;
        bridges_total += ans.stats.bridges;
        budget_handoffs += u64::from(ans.budget_handoff);
        patience_stopped += u64::from(ans.patience_stopped);
        recalls.push(recall_at_k(fx, filter, q, k, &hits));
    }
    let mut one_core_s = started.elapsed().as_secs_f64();
    let mut done = qn;
    if one_core_s < min_time.as_secs_f64() {
        done = 0;
        let min_queries = qn.min(200);
        let started = Instant::now();
        while done < min_queries || started.elapsed() < min_time {
            let q = done % qn;
            let _ = index
                .search_with_stats(fx.rows.query(q), k, ef, make_opts(q))
                .expect("hnsw warm search");
            done += 1;
        }
        one_core_s = started.elapsed().as_secs_f64();
    }

    let qps_all_cores = if threads <= 1 {
        done as f64 / one_core_s
    } else {
        let next = AtomicUsize::new(0);
        let completed = AtomicUsize::new(0);
        let deadline = min_time.max(Duration::from_millis(1));
        let started = Instant::now();
        std::thread::scope(|scope| {
            for _ in 0..threads {
                scope.spawn(|| {
                    while started.elapsed() < deadline {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        let q = i % qn;
                        let _ = index
                            .search_with_stats(fx.rows.query(q), k, ef, make_opts(q))
                            .expect("hnsw mt search");
                        completed.fetch_add(1, Ordering::Relaxed);
                    }
                });
            }
        });
        completed.load(Ordering::Relaxed) as f64 / started.elapsed().as_secs_f64()
    };

    Point {
        ef,
        recall_mean: recalls.iter().sum::<f64>() / qn as f64,
        recall_worst: recalls.iter().copied().fold(1.0, f64::min),
        qps_one_core: done as f64 / one_core_s,
        qps_all_cores,
        hops_mean: hops_total as f64 / qn as f64,
        scored_mean: scored_total as f64 / qn as f64,
        bridges_mean: bridges_total as f64 / qn as f64,
        budget_handoffs,
        patience_stopped,
        hops_total,
    }
}

pub fn command() -> Result<(), Error> {
    let dir = PathBuf::from(arg("fixture", ""));
    if dir.as_os_str().is_empty() {
        return Err("--fixture=DIR is required".into());
    }
    let out = PathBuf::from(arg("out", "ann-patience.json"));
    let fx = fixture::load(&dir)?;
    let threads: usize = arg("threads", &host_threads().to_string()).parse()?;
    let m: u32 = arg("m", "16").parse()?;
    let ef_construction: u32 = arg("ef-construction", "200").parse()?;
    let min_time = Duration::from_millis(arg("min-time-ms", "100").parse()?);
    let ks = list_usize("k", "10,100")?;
    let plain_efs = list_usize(
        "plain-ef",
        "16,20,24,28,32,40,48,56,64,80,96,112,128,160,192,224,256,320,384,512",
    )?;
    let patience_efs = list_usize("patience-ef", "32,48,64,96,128,192,256,384,512")?;
    let pinned_regime = match arg("regime", "adaptive").as_str() {
        "adaptive" | "" => None,
        "filter_aware" => Some(FilterRegime::FilterAware),
        "plain" => Some(FilterRegime::Plain),
        other => {
            return Err(
                format!("--regime={other:?} must be adaptive, filter_aware, or plain").into(),
            )
        }
    };

    let filter_names = fx.filter_names();
    let requested = list_str(
        "filters",
        "none,uncorrelated-50,uncorrelated-10,near-50,near-10",
    );
    let filters: Vec<usize> = if requested.len() == 1 && requested[0] == "all" {
        (0..=fx.filters.len()).collect()
    } else {
        (0..=fx.filters.len())
            .filter(|&i| requested.iter().any(|want| want == &filter_names[i]))
            .collect()
    };
    if filters.is_empty() {
        return Err(format!("no filters matched --filters={requested:?}").into());
    }

    let cfg = HnswConfig {
        m,
        ef_construction,
        build_mode: BuildMode::Parallel,
        seed: 0x5EED_0A11,
    };
    eprintln!(
        "building hnsw (m={m}, ef_construction={ef_construction}) over {}x{}...",
        fx.rows.rows(),
        fx.rows.dim
    );
    let build_started = Instant::now();
    let mut index = HnswIndex::new(fx.rows.dim, cfg)?;
    index.add(&fx.rows.base, fx.rows.dim)?;
    index.prepare()?;
    let build_s = build_started.elapsed().as_secs_f64();
    eprintln!("built in {build_s:.2}s; precomputing filter bitmaps...");

    let qn = fx.rows.query_count();
    let base_n = fx.rows.rows();
    let mut precomputed_masks: Vec<Option<Vec<Bits>>> = vec![None; fx.filters.len() + 1];
    for &f_idx in &filters {
        if f_idx > 0 {
            let f = &fx.filters[f_idx - 1];
            let mut q_masks = Vec::with_capacity(qn);
            for q in 0..qn {
                let bs = fx.index.mask(f.column, f.values(q));
                q_masks.push(Bits::from_words(bs.words().to_vec(), base_n));
            }
            precomputed_masks[f_idx] = Some(q_masks);
        }
    }

    let mut cells_json = Vec::new();
    for &f_idx in &filters {
        let fname = &filter_names[f_idx];
        let masks = precomputed_masks[f_idx].as_deref();
        for &k in &ks {
            eprintln!("measuring cell filter={fname} k={k}...");
            let mut plain_points = Vec::with_capacity(plain_efs.len());
            for &ef in &plain_efs {
                if ef < k {
                    continue;
                }
                let pt = measure_point(
                    &fx,
                    &index,
                    masks,
                    pinned_regime,
                    f_idx,
                    k,
                    ef,
                    None,
                    min_time,
                    threads,
                );
                plain_points.push(pt);
            }
            let hull_1c = pareto_hull(&plain_points, false);
            let hull_mt = pareto_hull(&plain_points, true);

            let plain_json: Vec<Value> = plain_points
                .iter()
                .map(|p| {
                    json!({
                        "ef": p.ef,
                        "recall_mean": p.recall_mean,
                        "recall_worst": p.recall_worst,
                        "qps_one_core": p.qps_one_core,
                        "qps_all_cores": p.qps_all_cores,
                        "hops_mean": p.hops_mean,
                        "scored_mean": p.scored_mean,
                        "bridges_mean": p.bridges_mean,
                        "budget_handoffs": p.budget_handoffs,
                    })
                })
                .collect();

            let mut sweeps_json = Vec::new();
            for window in [PatienceWindow::Beam, PatienceWindow::TopK] {
                for &sat in &SATURATIONS {
                    for &pat in &PATIENCES {
                        let spec = PatienceSpec {
                            saturation: sat,
                            saturation_ppm: Some((sat as f64 * 1_000_000.0).round() as u32),
                            patience: pat,
                            ascent_gate: 0,
                            window,
                        };
                        let mut pts = Vec::new();
                        let mut pts_json = Vec::new();
                        for &ef in &patience_efs {
                            if ef < k {
                                continue;
                            }
                            // When adaptive planning routes every query at this `ef`
                            // straight to exact `Enumerate` (`hops_total == 0`), no
                            // graph walk runs so patience is a guaranteed no-op.
                            let plain_at_ef = plain_points.iter().find(|p| p.ef == ef);
                            let (pt, skipped_enumerate) =
                                if let Some(base_pt) = plain_at_ef.filter(|p| p.hops_total == 0) {
                                    (base_pt.clone(), true)
                                } else {
                                    (
                                        measure_point(
                                            &fx,
                                            &index,
                                            masks,
                                            pinned_regime,
                                            f_idx,
                                            k,
                                            ef,
                                            Some(spec),
                                            min_time,
                                            threads,
                                        ),
                                        false,
                                    )
                                };
                            let pareto_1c = pareto_best_at_or_above(&hull_1c, pt.recall_mean);
                            let interp_1c = pareto_interp(&hull_1c, pt.recall_mean);
                            let pareto_mt = pareto_best_at_or_above(&hull_mt, pt.recall_mean);
                            let interp_mt = pareto_interp(&hull_mt, pt.recall_mean);

                            pts_json.push(json!({
                                "ef": pt.ef,
                                "skipped_all_enumerate": skipped_enumerate,
                                "recall_mean": pt.recall_mean,
                                "recall_worst": pt.recall_worst,
                                "qps_one_core": pt.qps_one_core,
                                "qps_all_cores": pt.qps_all_cores,
                                "hops_mean": pt.hops_mean,
                                "scored_mean": pt.scored_mean,
                                "bridges_mean": pt.bridges_mean,
                                "budget_handoffs": pt.budget_handoffs,
                                "patience_stopped": pt.patience_stopped,
                                "plain_pareto_1c": pareto_1c.map(|(r, q, e)| json!({
                                    "ef": e, "recall_mean": r, "qps_one_core": q,
                                    "speedup": pt.qps_one_core / q,
                                })),
                                "plain_interp_speedup_1c": interp_1c.map(|q| pt.qps_one_core / q),
                                "plain_pareto_mt": pareto_mt.map(|(r, q, e)| json!({
                                    "ef": e, "recall_mean": r, "qps_all_cores": q,
                                    "speedup": pt.qps_all_cores / q,
                                })),
                                "plain_interp_speedup_mt": interp_mt.map(|q| pt.qps_all_cores / q),
                            }));
                            pts.push(pt);
                        }

                        let pat_hull_1c = pareto_hull(&pts, false);
                        let pat_hull_mt = pareto_hull(&pts, true);
                        let mut targets_json = Vec::new();
                        for &target in &TARGETS {
                            let base_1c = pareto_best_at_or_above(&hull_1c, target);
                            let cand_1c = pareto_best_at_or_above(&pat_hull_1c, target);
                            let base_mt = pareto_best_at_or_above(&hull_mt, target);
                            let cand_mt = pareto_best_at_or_above(&pat_hull_mt, target);
                            targets_json.push(json!({
                                "recall_target": target,
                                "one_core": {
                                    "plain": base_1c.map(|(r, q, e)| json!({"ef": e, "recall": r, "qps": q})),
                                    "patience": cand_1c.map(|(r, q, e)| json!({"ef": e, "recall": r, "qps": q})),
                                    "speedup": match (base_1c, cand_1c) {
                                        (Some((_, bq, _)), Some((_, cq, _))) => Some(cq / bq),
                                        _ => None,
                                    },
                                },
                                "all_cores": {
                                    "plain": base_mt.map(|(r, q, e)| json!({"ef": e, "recall": r, "qps": q})),
                                    "patience": cand_mt.map(|(r, q, e)| json!({"ef": e, "recall": r, "qps": q})),
                                    "speedup": match (base_mt, cand_mt) {
                                        (Some((_, bq, _)), Some((_, cq, _))) => Some(cq / bq),
                                        _ => None,
                                    },
                                },
                            }));
                        }

                        sweeps_json.push(json!({
                            "window": window.name(),
                            "saturation": sat,
                            "patience": pat,
                            "targets": targets_json,
                            "points": pts_json,
                        }));
                    }
                }
            }

            cells_json.push(json!({
                "filter": fname,
                "k": k,
                "plain": plain_json,
                "sweeps": sweeps_json,
            }));
        }
    }

    let report = json!({
        "format": FORMAT,
        "host": host(),
        "harness": harness(),
        "rss_bytes": rss_bytes(),
        "fixture": {
            "name": fx.name(),
            "dir": fx.dir.display().to_string(),
            "rows": fx.rows.rows(),
            "dim": fx.rows.dim,
            "queries": fx.rows.query_count(),
            "base_sha256": fx.manifest["base_sha256"],
            "filters": fx.manifest["filters"],
        },
        "build": {
            "m": m,
            "ef_construction": ef_construction,
            "build_s": build_s,
            "kernel": index.kernel().name(),
        },
        "settings": {
            "threads": threads,
            "min_time_ms": min_time.as_millis() as u64,
            "regime": pinned_regime.map_or("adaptive", FilterRegime::name),
            "ks": ks,
            "plain_efs": plain_efs,
            "patience_efs": patience_efs,
        },
        "cells": cells_json,
    });
    write_json(&out, &report)?;
    eprintln!("wrote {}", out.display());
    Ok(())
}

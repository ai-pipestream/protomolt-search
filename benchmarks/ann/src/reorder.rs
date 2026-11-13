//! `ann_bench reorder`: measure `#98` (locality row reordering) across
//! `LocalityOrder::{None, Rcm, RecursiveBisection}` in both:
//! - compaction-global layout (`attach_node_map = false`, `0 B/row` overhead),
//! - seal-local layout (`attach_node_map = true`, `8 B/row` `NodeMap` section).
//!
//! Modes (`--mode=full|prep-images|cold-cgroup`):
//! - `full`: builds one base linked HNSW graph (`m=16, ef_construction=200`),
//!   times `permute_from_linked` for `Rcm` and `RecursiveBisection`, writes and
//!   memory-maps all 5 `.hnsw` variants, records structural locality metrics
//!   (`mean_id_gap`, `median_id_gap`, `same_4k_row_page_frac`, unique 4 KB row
//!   pages per query), runs in-RAM mapped QPS/latency/recall sweeps (`1t` + `mt`,
//!   unfiltered + `10%` & `1%` filtered), plus cold-PTE (`MADV_DONTNEED`) and
//!   optional cold-disk (`POSIX_FADV_DONTNEED`, `--cold-disk=true`) page-fault
//!   sweeps.
//! - `prep-images`: builds and writes the 5 `.hnsw` images + permutation files
//!   into `--work-dir` for subsequent `cold-cgroup` runs without keeping
//!   `base.f32` in RAM during constrained-cgroup execution.
//! - `cold-cgroup`: loads only `manifest.json`, `queries.f32`, and
//!   `truth/none.u32` (~1.5 MB heap) and maps one pre-built `.hnsw` image at a
//!   time inside a memory-capped cgroup (`mapped > page cache`), recording
//!   `/proc/self/stat` `minflt` + `majflt` page faults per query, p50/p99
//!   latency, and QPS.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use pipestream_search::hnsw::{BuildMode, HnswConfig, HnswIndex, LocalityOrder};
use pipestream_search::vector::{VectorProvider, VectorSearchOptions};
use serde_json::json;

use crate::fixture;
use crate::util::{
    arg, harness, host, host_threads, list_usize, read_f32, read_json, read_u32, rss_bytes,
    write_json, write_u32, Error,
};

#[derive(Clone, Copy)]
struct VariantSpec {
    name: &'static str,
    order: LocalityOrder,
    attach_node_map: bool,
    file_name: &'static str,
    perm_file: Option<&'static str>,
}

const VARIANTS: [VariantSpec; 5] = [
    VariantSpec {
        name: "none",
        order: LocalityOrder::None,
        attach_node_map: false,
        file_name: "none.hnsw",
        perm_file: None,
    },
    VariantSpec {
        name: "rcm_global",
        order: LocalityOrder::Rcm,
        attach_node_map: false,
        file_name: "rcm-global.hnsw",
        perm_file: Some("rcm-perm.u32"),
    },
    VariantSpec {
        name: "rcm_local",
        order: LocalityOrder::Rcm,
        attach_node_map: true,
        file_name: "rcm-local.hnsw",
        perm_file: None,
    },
    VariantSpec {
        name: "bisect_global",
        order: LocalityOrder::RecursiveBisection,
        attach_node_map: false,
        file_name: "bisect-global.hnsw",
        perm_file: Some("bisect-perm.u32"),
    },
    VariantSpec {
        name: "bisect_local",
        order: LocalityOrder::RecursiveBisection,
        attach_node_map: true,
        file_name: "bisect-local.hnsw",
        perm_file: None,
    },
];

/// Read `(minflt, majflt)` for the current process from `/proc/self/stat`.
fn proc_faults() -> (u64, u64) {
    let Ok(stat) = std::fs::read_to_string("/proc/self/stat") else {
        return (0, 0);
    };
    let Some(after_comm) = stat.rfind(')').map(|idx| &stat[idx + 1..]) else {
        return (0, 0);
    };
    let mut toks = after_comm.split_whitespace();
    // After `(comm)`: 0=state, 1=ppid, 2=pgrp, 3=session, 4=tty_nr, 5=tpgid,
    // 6=flags, 7=minflt, 8=cminflt, 9=majflt.
    let minflt = toks.nth(7).and_then(|s| s.parse::<u64>().ok()).unwrap_or(0);
    let majflt = toks.nth(1).and_then(|s| s.parse::<u64>().ok()).unwrap_or(0);
    (minflt, majflt)
}

fn percentile_us(sorted_us: &[f64], p: f64) -> f64 {
    if sorted_us.is_empty() {
        return 0.0;
    }
    let idx = ((sorted_us.len() - 1) as f64 * p).round() as usize;
    sorted_us[idx.min(sorted_us.len() - 1)]
}

fn throughput<F: Fn(usize) + Sync>(qn: usize, threads: usize, min_time: Duration, f: F) -> f64 {
    if threads <= 1 {
        let mut done = 0usize;
        let started = Instant::now();
        while started.elapsed() < min_time || done < qn.min(64) {
            f(done % qn);
            done += 1;
        }
        return done as f64 / started.elapsed().as_secs_f64();
    }
    let next = AtomicUsize::new(0);
    let started = Instant::now();
    std::thread::scope(|scope| {
        for _ in 0..threads {
            scope.spawn(|| loop {
                let i = next.fetch_add(1, Ordering::Relaxed);
                if i >= qn && started.elapsed() >= min_time {
                    break;
                }
                f(i % qn);
            });
        }
    });
    let done = next.load(Ordering::Relaxed);
    done as f64 / started.elapsed().as_secs_f64()
}

fn recall_at(slots: &[i64], truth_row: &[u32], k: usize, perm_new_to_old: Option<&[u32]>) -> f64 {
    let want = &truth_row[..k.min(truth_row.len())];
    if want.is_empty() {
        return 1.0;
    }
    let hits = slots
        .iter()
        .take(k)
        .filter(|&&slot| {
            if slot < 0 {
                return false;
            }
            let ext = match perm_new_to_old {
                Some(map) => map[slot as usize],
                None => slot as u32,
            };
            want.contains(&ext)
        })
        .count();
    hits as f64 / want.len() as f64
}

struct PreparedVariant {
    spec: VariantSpec,
    reorder_ms: f64,
    write_ms: f64,
    image_bytes: u64,
    bytes_per_row: f64,
    path: PathBuf,
    perm: Option<Vec<u32>>,
}

fn build_and_stage_variants(
    base_vecs: &[f32],
    rows: usize,
    dim: usize,
    m: u32,
    efc: u32,
    work_dir: &Path,
) -> Result<(f64, Vec<PreparedVariant>), Error> {
    std::fs::create_dir_all(work_dir)?;
    let cfg = HnswConfig {
        m,
        ef_construction: efc,
        seed: 42,
        build_mode: BuildMode::Parallel,
        locality_order: LocalityOrder::None,
    };
    let t_build = Instant::now();
    let mut base = HnswIndex::new(dim, cfg)?;
    base.add(base_vecs, dim)?;
    base.prepare()?;
    let base_build_ms = t_build.elapsed().as_secs_f64() * 1_000.0;

    let mut out = Vec::with_capacity(VARIANTS.len());
    for spec in VARIANTS {
        let t_reord = Instant::now();
        let (permuted, perm) = base.permute_from_linked(spec.order, spec.attach_node_map)?;
        let reorder_ms = if spec.order == LocalityOrder::None {
            0.0
        } else {
            t_reord.elapsed().as_secs_f64() * 1_000.0
        };
        let path = work_dir.join(spec.file_name);
        let t_write = Instant::now();
        permuted.write(&path)?;
        let write_ms = t_write.elapsed().as_secs_f64() * 1_000.0;
        let image_bytes = std::fs::metadata(&path)?.len();
        let bytes_per_row = image_bytes as f64 / rows as f64;
        if let Some(pf) = spec.perm_file {
            write_u32(&work_dir.join(pf), &perm)?;
        }
        out.push(PreparedVariant {
            spec,
            reorder_ms,
            write_ms,
            image_bytes,
            bytes_per_row,
            path,
            perm: spec.perm_file.map(|_| perm),
        });
    }
    write_json(
        &work_dir.join("prep-meta.json"),
        &json!({
            "base_build_ms": base_build_ms,
            "m": m,
            "ef_construction": efc,
            "rows": rows,
            "dim": dim,
        }),
    )?;
    Ok((base_build_ms, out))
}

fn run_cold_cgroup(
    fixture_dir: &Path,
    work_dir: &Path,
    out_path: &Path,
    beams: &[usize],
    cold_queries: usize,
) -> Result<(), Error> {
    let manifest = read_json(&fixture_dir.join("manifest.json"))?;
    let dim = manifest["dim"].as_u64().ok_or("manifest dim")? as usize;
    let rows = manifest["rows"].as_u64().ok_or("manifest rows")? as usize;
    let k_truth = manifest["k_truth"].as_u64().ok_or("manifest k_truth")? as usize;
    let source = manifest["source"].as_str().unwrap_or("").to_string();
    let queries = read_f32(&fixture_dir.join("queries.f32"))?;
    let total_q = queries.len() / dim;
    let qn = cold_queries.min(total_q).max(1);
    let truth_flat = read_u32(&fixture_dir.join("truth/none.u32"))?;
    let prep_meta = read_json(&work_dir.join("prep-meta.json")).unwrap_or_else(|_| json!({}));

    let threads = host_threads().max(1);
    let mut variant_reports = Vec::with_capacity(VARIANTS.len());

    for spec in VARIANTS {
        let path = work_dir.join(spec.file_name);
        let perm = match spec.perm_file {
            Some(pf) => Some(read_u32(&work_dir.join(pf))?),
            None => None,
        };
        let image_bytes = std::fs::metadata(&path)?.len();
        let mapped = HnswIndex::load_mapped(&path)?;
        let metrics = mapped.locality_metrics();

        let mut sweep = Vec::new();
        for &ef in beams {
            // Evict clean page cache + PTEs and disable OS readahead before the continuous run.
            let _ = HnswIndex::drop_file_page_cache(&path);
            mapped.advise_cold_mmap()?;

            let mut lat_us = Vec::with_capacity(qn);
            let mut sum_r10 = 0.0;
            let (min0, maj0) = proc_faults();
            let t_all = Instant::now();
            for qi in 0..qn {
                let qv = &queries[qi * dim..(qi + 1) * dim];
                let t0 = Instant::now();
                let res =
                    mapped.search(qv, 10, VectorSearchOptions::new().with_candidate_depth(ef))?;
                lat_us.push(t0.elapsed().as_secs_f64() * 1.0e6);
                let truth_row = &truth_flat[qi * k_truth..(qi + 1) * k_truth];
                sum_r10 += recall_at(res.slots_for_query(0), truth_row, 10, perm.as_deref());
            }
            let elapsed_s = t_all.elapsed().as_secs_f64();
            let (min1, maj1) = proc_faults();
            lat_us.sort_by(|a, b| a.total_cmp(b));

            // Multi-threaded continuous cgroup-constrained QPS (`threads` workers).
            let _ = HnswIndex::drop_file_page_cache(&path);
            mapped.advise_cold_mmap()?;
            let mt_next = AtomicUsize::new(0);
            let t_mt = Instant::now();
            std::thread::scope(|scope| {
                for _ in 0..threads {
                    scope.spawn(|| loop {
                        let i = mt_next.fetch_add(1, Ordering::Relaxed);
                        if i >= qn {
                            break;
                        }
                        let qv = &queries[(i % total_q) * dim..((i % total_q) + 1) * dim];
                        let _ = mapped.search(
                            qv,
                            10,
                            VectorSearchOptions::new().with_candidate_depth(ef),
                        );
                    });
                }
            });
            let qps_mt = qn as f64 / t_mt.elapsed().as_secs_f64();

            let min_delta = min1.saturating_sub(min0);
            let maj_delta = maj1.saturating_sub(maj0);
            sweep.push(json!({
                "ef": ef,
                "queries": qn,
                "recall_at_10": sum_r10 / qn as f64,
                "qps_1t": qn as f64 / elapsed_s,
                "qps_mt": qps_mt,
                "p50_us": percentile_us(&lat_us, 0.50),
                "p99_us": percentile_us(&lat_us, 0.99),
                "minflt_per_query": min_delta as f64 / qn as f64,
                "majflt_per_query": maj_delta as f64 / qn as f64,
                "total_flt_per_query": (min_delta + maj_delta) as f64 / qn as f64,
                "rss_mib": rss_bytes() as f64 / (1024.0 * 1024.0),
            }));
        }

        drop(mapped);
        let _ = HnswIndex::drop_file_page_cache(&path);
        variant_reports.push(json!({
            "variant": spec.name,
            "has_node_map": spec.attach_node_map,
            "image_bytes": image_bytes,
            "image_mib": image_bytes as f64 / (1024.0 * 1024.0),
            "bytes_per_row": image_bytes as f64 / rows as f64,
            "locality": {
                "edges": metrics.edges,
                "mean_id_gap": metrics.mean_id_gap,
                "median_id_gap": metrics.median_id_gap,
                "same_4k_row_page_frac": metrics.same_4k_row_page_frac,
                "same_cacheline_inv_norm_frac": metrics.same_cacheline_inv_norm_frac,
            },
            "cold_cgroup_sweep": sweep,
        }));
    }

    write_json(
        out_path,
        &json!({
            "benchmark": "hnsw_locality_reorder_98",
            "mode": "cold-cgroup",
            "harness": harness(),
            "host": host(),
            "fixture": {
                "source": source,
                "rows": rows,
                "dim": dim,
                "queries_evaluated": qn,
            },
            "prep": prep_meta,
            "variants": variant_reports,
        }),
    )?;
    Ok(())
}

pub fn command() -> Result<(), Error> {
    let fixture_dir = PathBuf::from(arg("fixture", ""));
    let out_path = PathBuf::from(arg("out", ""));
    let mode = arg("mode", "full");
    let work_dir = PathBuf::from(arg("work-dir", "/tmp/ann-bench-reorder"));
    if fixture_dir.as_os_str().is_empty() {
        return Err("--fixture=DIR is required".into());
    }
    if mode != "prep-images" && out_path.as_os_str().is_empty() {
        return Err("--out=FILE.json is required".into());
    }
    let m: u32 = arg("m", "16").parse()?;
    let efc: u32 = arg("ef-construction", "200").parse()?;
    let beams = list_usize("beams", "40,80,160,320")?;
    let cold_queries: usize = arg("cold-queries", "200").parse()?;
    let cold_disk: bool = arg("cold-disk", "false").parse()?;
    let reuse_images: bool = arg("reuse-images", "false").parse()?;

    if mode == "cold-cgroup" {
        return run_cold_cgroup(&fixture_dir, &work_dir, &out_path, &beams, cold_queries);
    }

    let fx = fixture::load(&fixture_dir)?;
    let rows = fx.rows.rows();
    let dim = fx.rows.dim;
    let qn = fx.rows.query_count();
    let k_truth = fx.k_truth;
    let threads = host_threads().max(1);

    let (base_build_ms, staged) = if reuse_images && work_dir.join("prep-meta.json").exists() {
        let meta = read_json(&work_dir.join("prep-meta.json"))?;
        let build_ms = meta["base_build_ms"].as_f64().unwrap_or(0.0);
        let mut v = Vec::with_capacity(VARIANTS.len());
        for spec in VARIANTS {
            let path = work_dir.join(spec.file_name);
            let image_bytes = std::fs::metadata(&path)?.len();
            let perm = match spec.perm_file {
                Some(pf) => Some(read_u32(&work_dir.join(pf))?),
                None => None,
            };
            v.push(PreparedVariant {
                spec,
                reorder_ms: 0.0,
                write_ms: 0.0,
                image_bytes,
                bytes_per_row: image_bytes as f64 / rows as f64,
                path,
                perm,
            });
        }
        (build_ms, v)
    } else {
        build_and_stage_variants(&fx.rows.base, rows, dim, m, efc, &work_dir)?
    };

    if mode == "prep-images" {
        if !out_path.as_os_str().is_empty() {
            write_json(
                &out_path,
                &json!({
                    "mode": "prep-images",
                    "base_build_ms": base_build_ms,
                    "work_dir": work_dir.display().to_string(),
                }),
            )?;
        }
        return Ok(());
    }

    // Prepare 10% and 1% uncorrelated filter masks (plus node-permuted equivalents
    // for `*_global` variants where compaction permuted all segment columns together).
    let mask_10_ext: Vec<bool> = (0..rows).map(|r| r % 10 == 0).collect();
    let mask_01_ext: Vec<bool> = (0..rows).map(|r| r % 100 == 0).collect();
    let compute_masked_truth = |mask: &[bool], k: usize| -> Vec<Vec<u32>> {
        crate::util::par_map(qn, threads, |qi| {
            let qv = fx.rows.query(qi);
            let mut top = crate::util::TopK::new(k);
            for (r, &ok) in mask.iter().enumerate() {
                if !ok {
                    continue;
                }
                let s = crate::util::dot(qv, fx.rows.row(r));
                if s > top.threshold() {
                    top.push(r as u32, s);
                }
            }
            top.into_sorted().into_iter().map(|(id, _)| id).collect()
        })
    };
    let truth_10 = compute_masked_truth(&mask_10_ext, 10);
    let truth_01 = compute_masked_truth(&mask_01_ext, 10);

    let mut variant_reports = Vec::with_capacity(staged.len());
    for pv in &staged {
        let mapped = HnswIndex::load_mapped(&pv.path)?;
        let metrics = mapped.locality_metrics();

        // Warm all mmap pages into RAM for the in-RAM sweep.
        for qi in 0..qn.min(64) {
            let _ = mapped.search(
                fx.rows.query(qi),
                10,
                VectorSearchOptions::new().with_candidate_depth(80),
            )?;
        }

        let mut in_ram = Vec::new();
        for &ef in &beams {
            let mut sum_pages = 0usize;
            let page_q = qn.min(200);
            for qi in 0..page_q {
                sum_pages += mapped.walk_row_4k_pages(fx.rows.query(qi), ef);
            }
            let avg_row_4k_pages = sum_pages as f64 / page_q as f64;

            // k=10 single-thread latency + recall + 1t/mt QPS.
            let mut lat10 = Vec::with_capacity(qn);
            let mut r10_sum = 0.0;
            for qi in 0..qn {
                let qv = fx.rows.query(qi);
                let t0 = Instant::now();
                let res =
                    mapped.search(qv, 10, VectorSearchOptions::new().with_candidate_depth(ef))?;
                lat10.push(t0.elapsed().as_secs_f64() * 1.0e6);
                let tr = &fx.truth[0][qi * k_truth..(qi + 1) * k_truth];
                r10_sum += recall_at(res.slots_for_query(0), tr, 10, pv.perm.as_deref());
            }
            lat10.sort_by(|a, b| a.total_cmp(b));
            let qps10_1t = throughput(qn, 1, Duration::from_millis(1200), |qi| {
                let _ = mapped.search(
                    fx.rows.query(qi),
                    10,
                    VectorSearchOptions::new().with_candidate_depth(ef),
                );
            });
            let qps10_mt = throughput(qn, threads, Duration::from_millis(1200), |qi| {
                let _ = mapped.search(
                    fx.rows.query(qi),
                    10,
                    VectorSearchOptions::new().with_candidate_depth(ef),
                );
            });

            // k=100 when ef >= 100.
            let k100_json = if ef >= 100 {
                let ef100 = ef;
                let mut r100_sum = 0.0;
                for qi in 0..qn {
                    let res = mapped.search(
                        fx.rows.query(qi),
                        100,
                        VectorSearchOptions::new().with_candidate_depth(ef100),
                    )?;
                    let tr = &fx.truth[0][qi * k_truth..(qi + 1) * k_truth];
                    r100_sum += recall_at(res.slots_for_query(0), tr, 100, pv.perm.as_deref());
                }
                let qps100_1t = throughput(qn, 1, Duration::from_millis(1000), |qi| {
                    let _ = mapped.search(
                        fx.rows.query(qi),
                        100,
                        VectorSearchOptions::new().with_candidate_depth(ef100),
                    );
                });
                json!({
                    "recall_at_100": r100_sum / qn as f64,
                    "qps_1t": qps100_1t,
                })
            } else {
                json!(null)
            };

            in_ram.push(json!({
                "ef": ef,
                "avg_row_4k_pages": avg_row_4k_pages,
                "recall_at_10": r10_sum / qn as f64,
                "qps_1t": qps10_1t,
                "qps_mt": qps10_mt,
                "p50_us": percentile_us(&lat10, 0.50),
                "p99_us": percentile_us(&lat10, 0.99),
                "k100": k100_json,
            }));
        }

        // Filtered 10% and 1% at ef=160, k=10.
        let mut filtered = Vec::new();
        for (fname, ext_mask, f_truth) in [
            ("uncorr_10pct", &mask_10_ext, &truth_10),
            ("uncorr_01pct", &mask_01_ext, &truth_01),
        ] {
            let active_mask: Vec<bool> = match &pv.perm {
                Some(new_to_old) => new_to_old
                    .iter()
                    .map(|&old| ext_mask[old as usize])
                    .collect(),
                None => ext_mask.clone(),
            };
            let mut r_sum = 0.0;
            let mut lat = Vec::with_capacity(qn);
            for (qi, truth_row) in f_truth.iter().enumerate().take(qn) {
                let t0 = Instant::now();
                let res = mapped.search(
                    fx.rows.query(qi),
                    10,
                    VectorSearchOptions::new()
                        .with_allowlist(&active_mask)
                        .with_candidate_depth(160),
                )?;
                lat.push(t0.elapsed().as_secs_f64() * 1.0e6);
                r_sum += recall_at(res.slots_for_query(0), truth_row, 10, pv.perm.as_deref());
            }
            lat.sort_by(|a, b| a.total_cmp(b));
            let qps_1t = throughput(qn, 1, Duration::from_millis(1000), |qi| {
                let _ = mapped.search(
                    fx.rows.query(qi),
                    10,
                    VectorSearchOptions::new()
                        .with_allowlist(&active_mask)
                        .with_candidate_depth(160),
                );
            });
            filtered.push(json!({
                "filter": fname,
                "ef": 160,
                "recall_at_10": r_sum / qn as f64,
                "qps_1t": qps_1t,
                "p50_us": percentile_us(&lat, 0.50),
                "p99_us": percentile_us(&lat, 0.99),
            }));
        }

        // Cold-PTE (`MADV_RANDOM | MADV_DONTNEED` per query) hardware 4 KB fault sweep.
        let cq = cold_queries.min(qn).max(1);
        let mut cold_pte = Vec::new();
        for &ef in &[40usize, 80, 160] {
            if !beams.contains(&ef) {
                continue;
            }
            let mut minflts = 0u64;
            let mut lat = Vec::with_capacity(cq);
            for qi in 0..cq {
                mapped.advise_cold_mmap()?;
                let (m0, _) = proc_faults();
                let t0 = Instant::now();
                let _ = mapped.search(
                    fx.rows.query(qi),
                    10,
                    VectorSearchOptions::new().with_candidate_depth(ef),
                )?;
                lat.push(t0.elapsed().as_secs_f64() * 1.0e6);
                let (m1, _) = proc_faults();
                minflts += m1.saturating_sub(m0);
            }
            lat.sort_by(|a, b| a.total_cmp(b));
            cold_pte.push(json!({
                "ef": ef,
                "queries": cq,
                "minflt_per_query": minflts as f64 / cq as f64,
                "p50_us": percentile_us(&lat, 0.50),
                "p99_us": percentile_us(&lat, 0.99),
            }));
        }

        // Optional Cold-Disk (`POSIX_FADV_DONTNEED` + `MADV_DONTNEED` per query) NVMe sweep.
        let mut cold_nvme = Vec::new();
        if cold_disk {
            let dq = cq.min(100);
            for &ef in &[40usize, 80, 160] {
                if !beams.contains(&ef) {
                    continue;
                }
                let mut minflts = 0u64;
                let mut majflts = 0u64;
                let mut lat = Vec::with_capacity(dq);
                for qi in 0..dq {
                    mapped.advise_cold_mmap()?;
                    let _ = HnswIndex::drop_file_page_cache(&pv.path);
                    let (min0, maj0) = proc_faults();
                    let t0 = Instant::now();
                    let _ = mapped.search(
                        fx.rows.query(qi),
                        10,
                        VectorSearchOptions::new().with_candidate_depth(ef),
                    )?;
                    lat.push(t0.elapsed().as_secs_f64() * 1.0e6);
                    let (min1, maj1) = proc_faults();
                    minflts += min1.saturating_sub(min0);
                    majflts += maj1.saturating_sub(maj0);
                }
                lat.sort_by(|a, b| a.total_cmp(b));
                let total_s: f64 = lat.iter().sum::<f64>() * 1.0e-6;
                cold_nvme.push(json!({
                    "ef": ef,
                    "queries": dq,
                    "minflt_per_query": minflts as f64 / dq as f64,
                    "majflt_per_query": majflts as f64 / dq as f64,
                    "qps_1t": dq as f64 / total_s.max(1.0e-9),
                    "p50_us": percentile_us(&lat, 0.50),
                    "p99_us": percentile_us(&lat, 0.99),
                }));
            }
        }

        variant_reports.push(json!({
            "variant": pv.spec.name,
            "has_node_map": pv.spec.attach_node_map,
            "reorder_ms": pv.reorder_ms,
            "reorder_pct_of_build": if base_build_ms > 0.0 { pv.reorder_ms / base_build_ms * 100.0 } else { 0.0 },
            "write_ms": pv.write_ms,
            "image_bytes": pv.image_bytes,
            "bytes_per_row": pv.bytes_per_row,
            "locality": {
                "edges": metrics.edges,
                "mean_id_gap": metrics.mean_id_gap,
                "median_id_gap": metrics.median_id_gap,
                "same_4k_row_page_frac": metrics.same_4k_row_page_frac,
                "same_cacheline_inv_norm_frac": metrics.same_cacheline_inv_norm_frac,
            },
            "in_ram": in_ram,
            "filtered": filtered,
            "cold_pte": cold_pte,
            "cold_disk": cold_nvme,
        }));
    }

    if !reuse_images {
        let _ = std::fs::remove_dir_all(&work_dir);
    }

    write_json(
        &out_path,
        &json!({
            "benchmark": "hnsw_locality_reorder_98",
            "mode": "full",
            "harness": harness(),
            "host": host(),
            "fixture": {
                "source": fx.rows.source,
                "rows": rows,
                "dim": dim,
                "queries": qn,
            },
            "base_build_ms": base_build_ms,
            "m": m,
            "ef_construction": efc,
            "variants": variant_reports,
        }),
    )?;
    Ok(())
}

//! `ann_bench merge`: measure `#76` (donor remap & Qdrant-style tombstone link
//! healing) and `#99` (greedy `K`-dominating join-set merge + directed
//! forward-BFS connectivity check):
//! 1. Merge wall time (`ms`), join-set fraction, reachability (`unreachable == 0`),
//!    `recall@10` / `recall@100`, and QPS across **2, 4, and 8 segments** of
//!    equal and uneven (skewed) sizes, comparing:
//!    - `FullRebuild` (`Store::build_links`),
//!    - `DonorReinsert` (`#76` donor remap + `LinkBuilder` reinsert),
//!    - `JoinSet` (`#99` donor remap + greedy `K`-dominating join set + graft +
//!      connectivity check).
//! 2. No-donor rebuild baseline (`used_no_donor_rebuild = true`).
//! 3. Tombstone live-only recall & QPS at `10%` and `30%` random + entry/hub
//!    deletions (online masked search vs `heal_tombstones` vs fresh rebuild).

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use pipestream_search::hnsw::{BuildMode, HnswConfig, HnswIndex, MergeStrategy, SegmentMergeInput};
use pipestream_search::vector::{Mask, VectorProvider, VectorSearchOptions};
use serde_json::json;

use crate::fixture::{self, Fixture};
use crate::util::{arg, dot, harness, host, host_threads, par_map, write_json, Error, TopK};

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

fn exact_truth_slice(
    base: &[f32],
    dim: usize,
    rows: usize,
    queries: &[f32],
    qn: usize,
    k: usize,
) -> Vec<Vec<u32>> {
    let k = k.min(rows);
    let threads = host_threads().max(1);
    par_map(qn, threads, |q| {
        let qv = &queries[q * dim..(q + 1) * dim];
        let mut top = TopK::new(k);
        for r in 0..rows {
            let s = dot(qv, &base[r * dim..(r + 1) * dim]);
            if s > top.threshold() {
                top.push(r as u32, s);
            }
        }
        top.into_sorted().into_iter().map(|(id, _)| id).collect()
    })
}

fn exact_truth_masked(
    base: &[f32],
    dim: usize,
    live: &[bool],
    queries: &[f32],
    qn: usize,
    k: usize,
) -> Vec<Vec<u32>> {
    let threads = host_threads().max(1);
    par_map(qn, threads, |q| {
        let qv = &queries[q * dim..(q + 1) * dim];
        let mut top = TopK::new(k);
        for (r, &ok) in live.iter().enumerate() {
            if !ok {
                continue;
            }
            let s = dot(qv, &base[r * dim..(r + 1) * dim]);
            if s > top.threshold() {
                top.push(r as u32, s);
            }
        }
        top.into_sorted().into_iter().map(|(id, _)| id).collect()
    })
}

fn eval_recall(
    index: &HnswIndex,
    fx: &Fixture,
    qn: usize,
    truth: (&[Vec<u32>], &[Vec<u32>]),
    beams: (usize, usize),
    allow: Option<&[bool]>,
) -> (f64, f64) {
    let (truth10, truth100) = truth;
    let (ef10, ef100) = beams;
    let mut r10 = 0.0;
    let mut r100 = 0.0;
    for q in 0..qn {
        let qv = fx.rows.query(q);
        let mut opts10 = VectorSearchOptions::new().with_candidate_depth(ef10);
        let mut opts100 = VectorSearchOptions::new().with_candidate_depth(ef100);
        if let Some(mask) = allow {
            opts10 = opts10.with_allowlist(mask);
            opts100 = opts100.with_allowlist(mask);
        }
        let res10 = index.search(qv, 10, opts10).unwrap();
        let got10 = res10.slots_for_query(0);
        let want10 = &truth10[q];
        let take10 = 10.min(want10.len());
        if take10 > 0 {
            let hits = got10
                .iter()
                .take(take10)
                .filter(|&&s| {
                    u32::try_from(s)
                        .ok()
                        .is_some_and(|id| want10[..take10].contains(&id))
                })
                .count();
            r10 += hits as f64 / take10 as f64;
        }

        let res100 = index.search(qv, 100, opts100).unwrap();
        let got100 = res100.slots_for_query(0);
        let want100 = &truth100[q];
        let take100 = 100.min(want100.len());
        if take100 > 0 {
            let hits = got100
                .iter()
                .take(take100)
                .filter(|&&s| {
                    u32::try_from(s)
                        .ok()
                        .is_some_and(|id| want100[..take100].contains(&id))
                })
                .count();
            r100 += hits as f64 / take100 as f64;
        }
    }
    (r10 / qn as f64, r100 / qn as f64)
}

fn partition_counts(total: usize, weights: &[usize]) -> Vec<usize> {
    let sum: usize = weights.iter().sum();
    let mut out = Vec::with_capacity(weights.len());
    let mut acc = 0usize;
    for (i, &w) in weights.iter().enumerate() {
        if i + 1 == weights.len() {
            out.push(total - acc);
        } else {
            let share = total * w / sum;
            out.push(share);
            acc += share;
        }
    }
    out
}

pub fn command() -> Result<(), Error> {
    let fixture_raw = arg("fixture", "");
    let out_raw = arg("out", "");
    if fixture_raw.is_empty() || out_raw.is_empty() {
        return Err("ann_bench merge requires --fixture=DIR and --out=FILE.json".into());
    }
    let fixture_dir = PathBuf::from(fixture_raw);
    let out_path = PathBuf::from(out_raw);
    let rows_raw = arg("rows", "");
    let rows_cap = if rows_raw.is_empty() {
        usize::MAX
    } else {
        rows_raw.parse::<usize>()?
    };
    let qn_cap = arg("queries", "200").parse::<usize>()?;
    let ef10 = arg("ef10", "80").parse::<usize>()?;
    let ef100 = arg("ef100", "160").parse::<usize>()?;
    let min_ms = arg("min-ms", "500").parse::<u64>()?;

    let fx = fixture::load(&fixture_dir)?;
    let fixture_name = fx.manifest["name"]
        .as_str()
        .unwrap_or("fixture")
        .to_string();
    let dim = fx.rows.dim;
    let n = fx.rows.rows().min(rows_cap);
    let qn = fx.rows.query_count().min(qn_cap);
    let threads = host_threads().max(1);
    let min_time = Duration::from_millis(min_ms);

    let config = HnswConfig {
        m: 16,
        ef_construction: 200,
        seed: 0x7699_2026,
        build_mode: BuildMode::Parallel,
        ..HnswConfig::default()
    };

    eprintln!(
        "ann_bench merge: fixture={} n={} dim={} qn={} threads={} kernel={}",
        fixture_name,
        n,
        dim,
        qn,
        threads,
        pipestream_search::hnsw::Kernel::detect().name()
    );

    let truth10 = exact_truth_slice(&fx.rows.base, dim, n, &fx.rows.queries, qn, 10);
    let truth100 = exact_truth_slice(&fx.rows.base, dim, n, &fx.rows.queries, qn, 100);

    // FullRebuild reference once for the whole `n` corpus (same output rows for every layout).
    let t0 = Instant::now();
    let (rebuild_idx, _) = HnswIndex::merge_segments(
        dim,
        config,
        &[],
        &fx.rows.base[..n * dim],
        MergeStrategy::FullRebuild,
    )?;
    let full_rebuild_ms = t0.elapsed().as_secs_f64() * 1000.0;
    let (rebuild_r10, rebuild_r100) = eval_recall(
        &rebuild_idx,
        &fx,
        qn,
        (&truth10, &truth100),
        (ef10, ef100),
        None,
    );
    let rebuild_qps10 = throughput(qn, threads, min_time, |q| {
        let qv = fx.rows.query(q);
        let _ = rebuild_idx
            .search(
                qv,
                10,
                VectorSearchOptions::new().with_candidate_depth(ef10),
            )
            .unwrap();
    });
    eprintln!(
        "  FullRebuild: {:.1} ms | r@10={:.4} r@100={:.4} qps@10={:.0}",
        full_rebuild_ms, rebuild_r10, rebuild_r100, rebuild_qps10
    );

    let specs: &[(&str, &[usize])] = &[
        ("2-equal", &[1, 1]),
        ("2-skewed-70-30", &[70, 30]),
        ("4-equal", &[1, 1, 1, 1]),
        ("4-skewed-55-20-15-10", &[55, 20, 15, 10]),
        ("8-equal", &[1, 1, 1, 1, 1, 1, 1, 1]),
        ("8-skewed-44-14-10-8-7-6-6-5", &[44, 14, 10, 8, 7, 6, 6, 5]),
    ];

    let mut merge_cases = Vec::new();
    for &(layout_name, weights) in specs {
        let counts = partition_counts(n, weights);
        let mut segs = Vec::with_capacity(counts.len());
        let mut cursor = 0usize;
        let seg_build_start = Instant::now();
        for &cnt in &counts {
            let mut seg = HnswIndex::new(dim, config)?;
            seg.add(&fx.rows.base[cursor * dim..(cursor + cnt) * dim], dim)?;
            seg.prepare()?;
            segs.push(seg);
            cursor += cnt;
        }
        let seg_build_ms = seg_build_start.elapsed().as_secs_f64() * 1000.0;
        let inputs: Vec<SegmentMergeInput<'_>> = segs
            .iter()
            .map(|index| SegmentMergeInput { index, live: None })
            .collect();

        let td = Instant::now();
        let (donor_reinsert_idx, dr_stats) =
            HnswIndex::merge_segments(dim, config, &inputs, &[], MergeStrategy::DonorReinsert)?;
        let donor_reinsert_ms = td.elapsed().as_secs_f64() * 1000.0;
        assert_eq!(donor_reinsert_idx.unreachable_from_entry(), 0);
        let (dr_r10, dr_r100) = eval_recall(
            &donor_reinsert_idx,
            &fx,
            qn,
            (&truth10, &truth100),
            (ef10, ef100),
            None,
        );
        let dr_qps10 = throughput(qn, threads, min_time, |q| {
            let qv = fx.rows.query(q);
            let _ = donor_reinsert_idx
                .search(
                    qv,
                    10,
                    VectorSearchOptions::new().with_candidate_depth(ef10),
                )
                .unwrap();
        });
        drop(donor_reinsert_idx);

        let tj = Instant::now();
        let (join_set_idx, js_stats) =
            HnswIndex::merge_segments(dim, config, &inputs, &[], MergeStrategy::JoinSet)?;
        let join_set_ms = tj.elapsed().as_secs_f64() * 1000.0;
        let unreachable = join_set_idx.unreachable_from_entry();
        assert_eq!(unreachable, 0);
        let (js_r10, js_r100) = eval_recall(
            &join_set_idx,
            &fx,
            qn,
            (&truth10, &truth100),
            (ef10, ef100),
            None,
        );
        let js_qps10 = throughput(qn, threads, min_time, |q| {
            let qv = fx.rows.query(q);
            let _ = join_set_idx
                .search(
                    qv,
                    10,
                    VectorSearchOptions::new().with_candidate_depth(ef10),
                )
                .unwrap();
        });

        let non_donor = n - js_stats.donor_rows;
        let join_pct = if non_donor > 0 {
            100.0 * js_stats.join_set_rows as f64 / non_donor as f64
        } else {
            0.0
        };
        eprintln!(
            "  {layout_name}: rebuild={:.1}ms | #76_donor_reinsert={:.1}ms ({:.2}x, r@10={:.4}, r@100={:.4}) | #99_join_set={:.1}ms ({:.2}x vs rebuild, {:.2}x vs #76, join={:.1}%, islands={}, r@10={:.4}, r@100={:.4})",
            full_rebuild_ms,
            donor_reinsert_ms,
            full_rebuild_ms / donor_reinsert_ms,
            dr_r10,
            dr_r100,
            join_set_ms,
            full_rebuild_ms / join_set_ms,
            donor_reinsert_ms / join_set_ms,
            join_pct,
            js_stats.islands_repaired,
            js_r10,
            js_r100
        );

        merge_cases.push(json!({
            "layout": layout_name,
            "segment_counts": counts,
            "source_build_ms": seg_build_ms,
            "full_rebuild": {
                "merge_ms": full_rebuild_ms,
                "recall_at_10": rebuild_r10,
                "recall_at_100": rebuild_r100,
                "qps_at_10": rebuild_qps10,
            },
            "donor_reinsert_76": {
                "merge_ms": donor_reinsert_ms,
                "speedup_vs_rebuild": full_rebuild_ms / donor_reinsert_ms,
                "donor_rows": dr_stats.donor_rows,
                "reinserted_rows": dr_stats.reinserted_rows,
                "islands_repaired": dr_stats.islands_repaired,
                "recall_at_10": dr_r10,
                "recall_at_100": dr_r100,
                "delta_r10_vs_rebuild": dr_r10 - rebuild_r10,
                "delta_r100_vs_rebuild": dr_r100 - rebuild_r100,
                "qps_at_10": dr_qps10,
            },
            "join_set_99": {
                "merge_ms": join_set_ms,
                "speedup_vs_rebuild": full_rebuild_ms / join_set_ms,
                "speedup_vs_donor_reinsert": donor_reinsert_ms / join_set_ms,
                "donor_rows": js_stats.donor_rows,
                "join_set_rows": js_stats.join_set_rows,
                "join_set_pct_of_non_donor": join_pct,
                "grafted_rows": js_stats.grafted_rows,
                "islands_repaired": js_stats.islands_repaired,
                "unreachable_nodes": unreachable,
                "recall_at_10": js_r10,
                "recall_at_100": js_r100,
                "delta_r10_vs_rebuild": js_r10 - rebuild_r10,
                "delta_r100_vs_rebuild": js_r100 - rebuild_r100,
                "qps_at_10": js_qps10,
            }
        }));
    }

    // No-donor rebuild time (donor 100% deleted, remaining half from raw vectors).
    let half = n / 2;
    let mut dead_donor = HnswIndex::new(dim, config)?;
    dead_donor.add(&fx.rows.base[..half * dim], dim)?;
    dead_donor.prepare()?;
    let dead_mask = vec![false; half];
    let t_nd = Instant::now();
    let (nd_idx, nd_stats) = HnswIndex::merge_segments(
        dim,
        config,
        &[SegmentMergeInput {
            index: &dead_donor,
            live: Some(Mask::Bools(&dead_mask)),
        }],
        &fx.rows.base[half * dim..n * dim],
        MergeStrategy::JoinSet,
    )?;
    let no_donor_ms = t_nd.elapsed().as_secs_f64() * 1000.0;
    assert!(nd_stats.used_no_donor_rebuild);
    assert_eq!(nd_idx.unreachable_from_entry(), 0);
    drop(nd_idx);
    drop(dead_donor);

    // Tombstone live-only recall & healing (#76): 10% and 30% deletions (including node 0..8).
    let mut tombstone_cases = Vec::new();
    for (tag, drop_pct) in [("10pct", 10usize), ("30pct", 30usize)] {
        let mut live: Vec<bool> = (0..n)
            .map(|i| ((i.wrapping_mul(2654435761)) % 100) >= drop_pct)
            .collect();
        for slot in live.iter_mut().take(8.min(n)) {
            *slot = false;
        }
        let live_t10 = exact_truth_masked(&fx.rows.base, dim, &live, &fx.rows.queries, qn, 10);
        let live_t100 = exact_truth_masked(&fx.rows.base, dim, &live, &fx.rows.queries, qn, 100);
        let (online_r10, online_r100) = eval_recall(
            &rebuild_idx,
            &fx,
            qn,
            (&live_t10, &live_t100),
            (ef10, ef100),
            Some(&live),
        );
        let online_qps10 = throughput(qn, threads, min_time, |q| {
            let qv = fx.rows.query(q);
            let _ = rebuild_idx
                .search(
                    qv,
                    10,
                    VectorSearchOptions::new()
                        .with_allowlist(&live)
                        .with_candidate_depth(ef10),
                )
                .unwrap();
        });

        let th = Instant::now();
        let (healed_idx, survivors, heal_stats) =
            rebuild_idx.heal_tombstones(Mask::Bools(&live))?;
        let heal_ms = th.elapsed().as_secs_f64() * 1000.0;
        assert_eq!(healed_idx.unreachable_from_entry(), 0);

        let mut surv_flat = Vec::with_capacity(survivors.len() * dim);
        for &old in &survivors {
            let at = old as usize * dim;
            surv_flat.extend_from_slice(&fx.rows.base[at..at + dim]);
        }
        let surv_t10 =
            exact_truth_slice(&surv_flat, dim, survivors.len(), &fx.rows.queries, qn, 10);
        let surv_t100 =
            exact_truth_slice(&surv_flat, dim, survivors.len(), &fx.rows.queries, qn, 100);

        let tr = Instant::now();
        let mut surv_rebuild = HnswIndex::new(dim, config)?;
        surv_rebuild.add(&surv_flat, dim)?;
        surv_rebuild.prepare()?;
        let surv_rebuild_ms = tr.elapsed().as_secs_f64() * 1000.0;

        let (healed_r10, healed_r100) = eval_recall(
            &healed_idx,
            &fx,
            qn,
            (&surv_t10, &surv_t100),
            (ef10, ef100),
            None,
        );
        let (sreb_r10, sreb_r100) = eval_recall(
            &surv_rebuild,
            &fx,
            qn,
            (&surv_t10, &surv_t100),
            (ef10, ef100),
            None,
        );
        let healed_qps10 = throughput(qn, threads, min_time, |q| {
            let qv = fx.rows.query(q);
            let _ = healed_idx
                .search(
                    qv,
                    10,
                    VectorSearchOptions::new().with_candidate_depth(ef10),
                )
                .unwrap();
        });
        eprintln!(
            "  tombstones {tag}: online r@10={:.4} r@100={:.4} qps={:.0} | heal={:.1}ms (vs {:.1}ms rebuild, {:.2}x) healed r@10={:.4} (rebuild {:.4}) r@100={:.4} (rebuild {:.4}) qps={:.0}",
            online_r10,
            online_r100,
            online_qps10,
            heal_ms,
            surv_rebuild_ms,
            surv_rebuild_ms / heal_ms,
            healed_r10,
            sreb_r10,
            healed_r100,
            sreb_r100,
            healed_qps10
        );
        tombstone_cases.push(json!({
            "tombstone_rate": tag,
            "surviving_rows": survivors.len(),
            "online_masked": {
                "recall_at_10": online_r10,
                "recall_at_100": online_r100,
                "qps_at_10": online_qps10,
            },
            "healed_76": {
                "heal_ms": heal_ms,
                "survivor_rebuild_ms": surv_rebuild_ms,
                "speedup_vs_rebuild": surv_rebuild_ms / heal_ms,
                "healed_lists": heal_stats.donor_healed_lists,
                "islands_repaired": heal_stats.islands_repaired,
                "recall_at_10": healed_r10,
                "recall_at_100": healed_r100,
                "rebuild_recall_at_10": sreb_r10,
                "rebuild_recall_at_100": sreb_r100,
                "delta_r10_vs_rebuild": healed_r10 - sreb_r10,
                "delta_r100_vs_rebuild": healed_r100 - sreb_r100,
                "qps_at_10": healed_qps10,
            }
        }));
    }

    let report = json!({
        "harness": harness(),
        "host": host(),
        "kernel": pipestream_search::hnsw::Kernel::detect().name(),
        "fixture": {
            "name": fixture_name,
            "rows": n,
            "dim": dim,
            "queries": qn,
            "ef10": ef10,
            "ef100": ef100,
        },
        "full_rebuild_baseline": {
            "merge_ms": full_rebuild_ms,
            "recall_at_10": rebuild_r10,
            "recall_at_100": rebuild_r100,
            "qps_at_10": rebuild_qps10,
        },
        "no_donor_rebuild": {
            "rows": n - half,
            "merge_ms": no_donor_ms,
        },
        "merge_cases": merge_cases,
        "tombstone_cases": tombstone_cases,
    });
    write_json(&out_path, &report)?;
    Ok(())
}

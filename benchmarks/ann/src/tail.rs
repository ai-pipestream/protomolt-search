//! `ann_bench tail`: measure (#97):
//! 1. Small-segment crossover (`View::enumerate` vs HNSW graph walk across
//!    segment sizes `n` at the fixture's dimension).
//! 2. Segmented lifecycle ingest throughput (`add` in streaming batches into
//!    the heap tail) and seal time (`freeze_tail` + `write` + `catalog.append`
//!    + `republish`).
//! 3. Query recall and QPS at matched recall over sealed mapped HNSW segments
//!    plus an unsealed heap tail of `0`, `10_000`, and `100_000` rows (both
//!    unary `VectorProvider::search` and controlled streaming).

use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use pipestream_search::boolean_bits::Bits;
use pipestream_search::exact_vectors::ExactVectorStore;
use pipestream_search::hnsw::{BuildMode, HnswConfig, HnswIndex, HNSW};
use pipestream_search::live_docs::LiveDocs;
use pipestream_search::postings::{AnalyzedDoc, Bm25Store};
use pipestream_search::segmented_vectors::SegmentedProvider;
use pipestream_search::segments::{SegmentCatalog, SegmentSource, VectorLoad};
use pipestream_search::vector::{
    FilterRegime, VectorProvider, VectorSearchOptions, VectorStreamControl,
};
use serde_json::{json, Value};

use crate::fixture::{self, Fixture};
use crate::util::{
    arg, dot, harness, host, host_threads, list_usize, par_map, write_json, Error, TopK,
};

static NEXT_DIR: AtomicU32 = AtomicU32::new(0);

fn scratch_dir(tag: &str) -> Result<PathBuf, Error> {
    let dir = std::env::temp_dir().join(format!(
        "ann-tail-{tag}-{}-{}",
        std::process::id(),
        NEXT_DIR.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
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

fn exact_truth(
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

fn mean_recall(truth: &[Vec<u32>], q: usize, k: usize, got: &[i64]) -> f64 {
    let want = &truth[q];
    let take = k.min(want.len());
    if take == 0 {
        return 1.0;
    }
    let want_slice = &want[..take];
    let found = got
        .iter()
        .take(take)
        .filter(|&&slot| {
            u32::try_from(slot)
                .ok()
                .is_some_and(|id| want_slice.contains(&id))
        })
        .count();
    found as f64 / take as f64
}

/// 1. Small-segment crossover: `HnswIndex` exact `enumerate` vs graph walk.
fn measure_crossover(
    fx: &Fixture,
    config: HnswConfig,
    sizes: &[usize],
    efs: &[usize],
    threads: usize,
    min_time: Duration,
) -> Result<Vec<Value>, Error> {
    let dim = fx.rows.dim;
    let qn = fx.rows.query_count().min(200);
    let mut out = Vec::new();
    for &n in sizes {
        if n == 0 || n > fx.rows.rows() {
            continue;
        }
        let slice = &fx.rows.base[..n * dim];
        let mut index = HnswIndex::new(dim, config)?;
        index.add(slice, dim)?;
        index.prepare()?;

        let all_ones = Bits::from_words(vec![!0u64; n.div_ceil(64)], n);
        let truth10 = exact_truth(&fx.rows.base, dim, n, &fx.rows.queries, qn, 10);

        let enum_qps_1c = throughput(qn, 1, min_time, |q| {
            let qv = fx.rows.query(q);
            let opts = VectorSearchOptions::new()
                .with_bits(&all_ones)
                .with_filter_regime(FilterRegime::Enumerate);
            let _ = index.search(qv, 10.min(n), opts).unwrap();
        });
        let enum_qps_all = throughput(qn, threads, min_time, |q| {
            let qv = fx.rows.query(q);
            let opts = VectorSearchOptions::new()
                .with_bits(&all_ones)
                .with_filter_regime(FilterRegime::Enumerate);
            let _ = index.search(qv, 10.min(n), opts).unwrap();
        });

        let mut walk_points = Vec::new();
        for &ef in efs {
            if ef < 10 {
                continue;
            }
            let k = 10.min(n);
            let mut rec_sum = 0.0;
            for q in 0..qn {
                let qv = fx.rows.query(q);
                let opts = VectorSearchOptions::new().with_candidate_depth(ef);
                let res = index.search(qv, k, opts).unwrap();
                rec_sum += mean_recall(&truth10, q, k, res.slots_for_query(0));
            }
            let recall = rec_sum / qn as f64;
            let qps_1c = throughput(qn, 1, min_time, |q| {
                let qv = fx.rows.query(q);
                let opts = VectorSearchOptions::new().with_candidate_depth(ef);
                let _ = index.search(qv, k, opts).unwrap();
            });
            let qps_all = throughput(qn, threads, min_time, |q| {
                let qv = fx.rows.query(q);
                let opts = VectorSearchOptions::new().with_candidate_depth(ef);
                let _ = index.search(qv, k, opts).unwrap();
            });
            walk_points.push(json!({
                "ef": ef,
                "recall_mean": recall,
                "qps_one_core": qps_1c,
                "qps_all_cores": qps_all,
                "walk_over_enum_1c": qps_1c / enum_qps_1c,
                "walk_over_enum_all": qps_all / enum_qps_all,
            }));
        }
        eprintln!(
            "tail crossover: n={n:<5} enum_1c={enum_qps_1c:8.0} enum_{threads}c={enum_qps_all:8.0}"
        );
        out.push(json!({
            "rows": n,
            "dim": dim,
            "enumerate_qps_one_core": enum_qps_1c,
            "enumerate_qps_all_cores": enum_qps_all,
            "walk": walk_points,
        }));
    }
    Ok(out)
}

fn write_minimal_bm25_and_live(
    stage: &std::path::Path,
    rows: usize,
) -> Result<(PathBuf, PathBuf), Error> {
    let mut store = Bm25Store::new();
    for doc_id in 0..rows as u32 {
        store.add_document_with_lineage(
            doc_id,
            String::new(),
            AnalyzedDoc::body(Vec::new(), 0),
            None,
        );
    }
    let bm25_path = stage.join("documents.bm25");
    store.save(&bm25_path)?;
    let live_path = stage.join("live-docs.bin");
    LiveDocs::default().write(&live_path, rows as u64)?;
    Ok((bm25_path, live_path))
}

/// 2. Segmented lifecycle ingest (`add` in streaming batches) + seal time.
fn measure_ingest_and_seal(
    fx: &Fixture,
    config: HnswConfig,
    ingest_rows: usize,
    batch_rows: usize,
) -> Result<Value, Error> {
    let dim = fx.rows.dim;
    let rows = ingest_rows.min(fx.rows.rows());
    let dir = scratch_dir("ingest-seal")?;
    let catalog_root = dir.join("catalog");
    let catalog = SegmentCatalog::open_with(&catalog_root, VectorLoad::Mapped)?;
    let tail = HnswIndex::new(dim, config)?.into_index();
    let mut provider = SegmentedProvider::open(catalog.snapshot(), tail)?;
    let mut exact = ExactVectorStore::empty(Some(dim));

    let ingest_start = Instant::now();
    let mut offset = 0usize;
    while offset < rows {
        let take = batch_rows.min(rows - offset);
        let batch = &fx.rows.base[offset * dim..(offset + take) * dim];
        provider.add(batch, dim)?;
        exact.append(batch, dim)?;
        offset += take;
    }
    let ingest_s = ingest_start.elapsed().as_secs_f64();

    // Prepare BM25 & live-docs outside the vector seal timer so vector seal
    // time isolates freeze_tail + HNSW image build/write + exact write +
    // catalog append + republish, plus a pure vector_write_s timer.
    let stage = dir.join("stage");
    std::fs::create_dir_all(&stage)?;
    let (bm25_path, live_path) = write_minimal_bm25_and_live(&stage, rows)?;

    let seal_start = Instant::now();
    let fresh = HnswIndex::new(dim, config)?.into_index();
    let frozen = provider.freeze_tail(fresh, rows)?;
    let vector_path = stage.join("vector.index");
    let vec_write_start = Instant::now();
    frozen.write(&vector_path)?;
    let vector_write_s = vec_write_start.elapsed().as_secs_f64();
    let exact_path = stage.join("vectors.f32");
    let values = exact.row_values(0, rows)?;
    ExactVectorStore::from_values(dim, values)?.write(&exact_path)?;
    let published = catalog.append(SegmentSource {
        segment_id: "seg-00000001",
        generation: 1,
        base_label: 0,
        backend_kind: HNSW,
        vector_path: Some(&vector_path),
        exact_vector_path: Some(&exact_path),
        bm25_path: &bm25_path,
        live_docs_path: &live_path,
        partition_column: None,
    })?;
    provider.republish(published)?;
    let seal_s = seal_start.elapsed().as_secs_f64();
    let total_s = ingest_s + seal_s;
    let _ = std::fs::remove_dir_all(&dir);

    eprintln!(
        "tail lifecycle ({rows} rows, batch={batch_rows}, d={dim}): ingest={:.1}ms ({:.0} rows/s), seal={:.1}ms (vector_write={:.1}ms), total={:.1}ms ({:.0} rows/s)",
        ingest_s * 1e3,
        rows as f64 / ingest_s,
        seal_s * 1e3,
        vector_write_s * 1e3,
        total_s * 1e3,
        rows as f64 / total_s,
    );
    Ok(json!({
        "rows": rows,
        "dim": dim,
        "batch_rows": batch_rows,
        "ingest_ms": ingest_s * 1e3,
        "ingest_rows_per_s": rows as f64 / ingest_s,
        "seal_ms": seal_s * 1e3,
        "vector_write_ms": vector_write_s * 1e3,
        "total_ms": total_s * 1e3,
        "total_rows_per_s": rows as f64 / total_s,
    }))
}

/// 3. Sealed segments + unsealed heap tail (`0`, `10k`, `100k` rows) QPS & recall.
#[allow(clippy::too_many_arguments)]
fn measure_tail_queries(
    fx: &Fixture,
    config: HnswConfig,
    sealed_rows: usize,
    segment_rows: usize,
    tail_sizes: &[usize],
    ks: &[usize],
    efs: &[usize],
    threads: usize,
    min_time: Duration,
) -> Result<Vec<Value>, Error> {
    let dim = fx.rows.dim;
    if sealed_rows > fx.rows.rows() {
        return Err(format!(
            "sealed_rows ({sealed_rows}) exceeds fixture rows ({})",
            fx.rows.rows()
        )
        .into());
    }
    let dir = scratch_dir("tail-queries")?;
    let catalog_root = dir.join("catalog");
    let catalog = SegmentCatalog::open_with(&catalog_root, VectorLoad::Mapped)?;

    // Build and seal the mapped base segments once.
    let mut base_offset = 0usize;
    let mut seg_idx = 0u64;
    while base_offset < sealed_rows {
        let len = segment_rows.min(sealed_rows - base_offset);
        seg_idx += 1;
        let stage = dir.join(format!("stage-{seg_idx}"));
        std::fs::create_dir_all(&stage)?;
        let (bm25_path, live_path) = write_minimal_bm25_and_live(&stage, len)?;
        let slice = &fx.rows.base[base_offset * dim..(base_offset + len) * dim];
        let mut idx = HnswIndex::new(dim, config)?;
        idx.add(slice, dim)?;
        idx.prepare()?;
        let vector_path = stage.join("vector.index");
        idx.write(&vector_path)?;
        let exact_path = stage.join("vectors.f32");
        ExactVectorStore::from_values(dim, slice.to_vec())?.write(&exact_path)?;
        let seg_id = format!("seg-{seg_idx:08}");
        catalog.append(SegmentSource {
            segment_id: &seg_id,
            generation: seg_idx,
            base_label: base_offset as u64,
            backend_kind: HNSW,
            vector_path: Some(&vector_path),
            exact_vector_path: Some(&exact_path),
            bm25_path: &bm25_path,
            live_docs_path: &live_path,
            partition_column: None,
        })?;
        let _ = std::fs::remove_dir_all(&stage);
        base_offset += len;
    }

    let qn = fx.rows.query_count();
    let mut results = Vec::new();
    for &tail_rows in tail_sizes {
        let total_rows = sealed_rows + tail_rows;
        if total_rows > fx.rows.rows() || total_rows == 0 {
            eprintln!(
                "tail queries: skipping tail_rows={tail_rows} (sealed={sealed_rows} + tail={tail_rows} > fixture={})",
                fx.rows.rows()
            );
            continue;
        }
        let tail = HnswIndex::new(dim, config)?.into_index();
        let mut provider = SegmentedProvider::open(catalog.snapshot(), tail)?;
        if tail_rows > 0 {
            let tail_slice = &fx.rows.base[sealed_rows * dim..total_rows * dim];
            provider.add(tail_slice, dim)?;
        }

        let mut cells = Vec::new();
        for &k in ks {
            let truth = exact_truth(&fx.rows.base, dim, total_rows, &fx.rows.queries, qn, k);
            let mut unary_points = Vec::new();
            for &ef in efs {
                if ef < k {
                    continue;
                }
                let mut rec_sum = 0.0;
                let mut tail_enum_sum = 0u64;
                for q in 0..qn {
                    let qv = fx.rows.query(q);
                    let opts = VectorSearchOptions::new().with_candidate_depth(ef);
                    let res = provider.search(qv, k, opts)?;
                    rec_sum += mean_recall(&truth, q, k, res.slots_for_query(0));
                    tail_enum_sum += res.tail_enumerated_rows;
                }
                let recall = rec_sum / qn as f64;
                let qps_1c = throughput(qn, 1, min_time, |q| {
                    let qv = fx.rows.query(q);
                    let opts = VectorSearchOptions::new().with_candidate_depth(ef);
                    let _ = provider.search(qv, k, opts).unwrap();
                });
                let qps_all = throughput(qn, threads, min_time, |q| {
                    let qv = fx.rows.query(q);
                    let opts = VectorSearchOptions::new().with_candidate_depth(ef);
                    let _ = provider.search(qv, k, opts).unwrap();
                });
                unary_points.push(json!({
                    "ef": ef,
                    "recall_mean": recall,
                    "qps_one_core": qps_1c,
                    "qps_all_cores": qps_all,
                    "tail_enumerated_rows_mean": tail_enum_sum as f64 / qn as f64,
                }));
            }

            // Also measure controlled streaming at ef = max(k, 64) to verify
            // shared-floor pruning across sealed segments -> unlinked tail.
            let stream_ef = k.max(64);
            let mut stream_rec_sum = 0.0;
            let mut emitted_sum = 0usize;
            let mut stream_tail_enum_sum = 0u64;
            for q in 0..qn {
                let qv = fx.rows.query(q);
                let opts = VectorSearchOptions::new().with_candidate_depth(stream_ef);
                let mut top = TopK::new(k);
                let summary = provider.search_streaming_controlled(
                    qv,
                    opts,
                    &mut |batch| {
                        for (&slot, &score) in batch.slots.iter().zip(batch.scores) {
                            if let Ok(id) = u32::try_from(slot) {
                                top.push(id, score);
                            }
                        }
                        if top.threshold().is_finite() {
                            VectorStreamControl::RaiseFloor(top.threshold())
                        } else {
                            VectorStreamControl::Continue
                        }
                    },
                    &mut || VectorStreamControl::Continue,
                )?;
                emitted_sum += summary.emitted;
                stream_tail_enum_sum += summary.tail_enumerated_rows;
                let slots: Vec<i64> = top
                    .into_sorted()
                    .into_iter()
                    .map(|(id, _)| i64::from(id))
                    .collect();
                stream_rec_sum += mean_recall(&truth, q, k, &slots);
            }
            let stream_qps_all = throughput(qn, threads, min_time, |q| {
                let qv = fx.rows.query(q);
                let opts = VectorSearchOptions::new().with_candidate_depth(stream_ef);
                let mut top = TopK::new(k);
                let _ = provider
                    .search_streaming_controlled(
                        qv,
                        opts,
                        &mut |batch| {
                            for (&slot, &score) in batch.slots.iter().zip(batch.scores) {
                                if let Ok(id) = u32::try_from(slot) {
                                    top.push(id, score);
                                }
                            }
                            if top.threshold().is_finite() {
                                VectorStreamControl::RaiseFloor(top.threshold())
                            } else {
                                VectorStreamControl::Continue
                            }
                        },
                        &mut || VectorStreamControl::Continue,
                    )
                    .unwrap();
            });

            eprintln!(
                "tail queries: sealed={sealed_rows} tail={tail_rows:<6} k={k:<3} stream_ef={stream_ef} stream_qps_{threads}c={stream_qps_all:7.0} emitted_mean={:.1}",
                emitted_sum as f64 / qn as f64
            );
            cells.push(json!({
                "k": k,
                "unary": unary_points,
                "streaming": {
                    "ef": stream_ef,
                    "recall_mean": stream_rec_sum / qn as f64,
                    "qps_all_cores": stream_qps_all,
                    "emitted_mean": emitted_sum as f64 / qn as f64,
                    "tail_enumerated_rows_mean": stream_tail_enum_sum as f64 / qn as f64,
                }
            }));
        }
        results.push(json!({
            "sealed_rows": sealed_rows,
            "segment_rows": segment_rows,
            "tail_rows": tail_rows,
            "cells": cells,
        }));
    }

    let _ = std::fs::remove_dir_all(&dir);
    Ok(results)
}

pub fn command() -> Result<(), Error> {
    let fixture_dir: PathBuf = arg("fixture", "").into();
    let out: PathBuf = arg("out", "").into();
    if fixture_dir.as_os_str().is_empty() || out.as_os_str().is_empty() {
        return Err("tail needs --fixture=DIR --out=FILE".into());
    }
    let fx = fixture::load(&fixture_dir)?;
    let threads: usize = arg("threads", &host_threads().to_string()).parse()?;
    let m: u32 = arg("m", "16").parse()?;
    let efc: u32 = arg("ef-construction", "200").parse()?;
    let config = HnswConfig {
        m,
        ef_construction: efc,
        build_mode: BuildMode::Parallel,
        seed: 0x5EED_0A97,
        ..HnswConfig::default()
    };
    let min_time = Duration::from_millis(arg("min-time-ms", "200").parse()?);
    let crossover_raw = arg(
        "crossover-rows",
        "64,128,256,384,512,768,1024,1536,2048,4096",
    );
    let crossover_sizes = if crossover_raw == "none" || crossover_raw.is_empty() {
        Vec::new()
    } else {
        list_usize(
            "crossover-rows",
            "64,128,256,384,512,768,1024,1536,2048,4096",
        )?
    };
    let crossover_efs = list_usize("crossover-ef", "16,32,64")?;
    let ingest_rows: usize = arg("ingest-rows", "20000").parse()?;
    let batch_rows: usize = arg("batch-rows", "500").parse()?;
    let sealed_rows: usize = arg("sealed-rows", "100000").parse()?;
    let segment_rows: usize = arg("segment-rows", "50000").parse()?;
    let tail_sizes = list_usize("tail-rows", "0,10000,100000")?;
    let ks = list_usize("k", "10,100")?;
    let efs = list_usize("ef", "16,32,48,64,96,128,192")?;

    let crossover = measure_crossover(
        &fx,
        config,
        &crossover_sizes,
        &crossover_efs,
        threads,
        min_time,
    )?;
    let lifecycle = measure_ingest_and_seal(&fx, config, ingest_rows, batch_rows)?;
    let tail_queries = measure_tail_queries(
        &fx,
        config,
        sealed_rows,
        segment_rows,
        &tail_sizes,
        &ks,
        &efs,
        threads,
        min_time,
    )?;

    let report = json!({
        "format": "protomolt-ann-tail-bench-v1",
        "fixture": {
            "name": fx.name(),
            "rows": fx.rows.rows(),
            "dim": fx.rows.dim,
            "queries": fx.rows.query_count(),
        },
        "host": host(),
        "harness": harness(),
        "params": { "m": m, "ef_construction": efc, "threads": threads },
        "crossover": crossover,
        "lifecycle": lifecycle,
        "tail_queries": tail_queries,
    });
    write_json(&out, &report)?;
    eprintln!("tail: wrote {}", out.display());
    Ok(())
}

//! Recall, latency and build cost of the `leann-graph` provider against
//! the default embedded TurboVec scan, on a prefix of the CourtListener
//! embeddings (or a synthetic corpus for a smoke run). This is the
//! measurement `docs/leann-graph.md` says AUTO waits for.
//!
//! Every cell is judged against exact FP32 brute-force truth over the same
//! rows. The flat engine is reported raw (quantized scores) and after an
//! FP32 rerank of its top `--rerank-depth`, the product's quality ladder.
//! The graph's reported scores are already FP32; its query beam is
//! `max(k, search_beam)`, so the harness builds with `search_beam = 1` and
//! sweeps the beam by asking for `max(k, beam)` rows and keeping the top k.
//!
//! ```text
//! cargo run --release --example leann_eval -- \
//!     --input=/work/court-corpus/embeddings-full.bin --vectors=1000000 \
//!     --queries=64 --k=10,100 --beams=10,32,64,128,256,512 \
//!     --configs=32:8:64,64:16:128 --out=/work/court-corpus/bench/leann/1m.json
//! cargo run --release --example leann_eval -- \
//!     --synthetic=50000:64 --queries=32 --out=/tmp/leann-smoke.json
//! ```

use std::cmp::Ordering;
use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Instant;

use pipestream_search::demo::court::EmbeddingReader;
use pipestream_search::harness::{fit_calibration, seeded_index};
use pipestream_search::leann_graph::{LeannGraph, LeannGraphConfig};
use pipestream_search::vector::{VectorIndex, VectorSearchOptions, VectorSearchResults};
use serde_json::{json, Value};

type Error = Box<dyn std::error::Error + Send + Sync>;

fn arg(key: &str, default: &str) -> String {
    let prefix = format!("--{key}=");
    std::env::args()
        .find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
        .unwrap_or_else(|| default.to_string())
}

fn list(key: &str, default: &str) -> Result<Vec<usize>, Error> {
    let mut values = arg(key, default)
        .split(',')
        .map(|part| part.trim().parse::<usize>())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("--{key}: {e}"))?;
    values.sort_unstable();
    values.dedup();
    if values.is_empty() || values[0] == 0 {
        return Err(format!("--{key} needs positive values").into());
    }
    Ok(values)
}

struct Corpus {
    values: Vec<f32>,
    queries: Vec<f32>,
    dim: usize,
    provenance: String,
}

fn normalize(row: &mut [f32]) {
    let norm = row.iter().map(|v| v * v).sum::<f32>().sqrt();
    if norm > 0.0 {
        row.iter_mut().for_each(|v| *v /= norm);
    }
}

/// Topic-shaped unit vectors from a fixed LCG: deterministic, no file needed.
fn synthetic(rows: usize, dim: usize, queries: usize) -> Corpus {
    let mut state = 0xC0DE_51A7_u64;
    let mut next = move || {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        ((state >> 40) as f32 / (1u64 << 24) as f32) * 2.0 - 1.0
    };
    let topics: Vec<Vec<f32>> = (0..64)
        .map(|_| {
            let mut t: Vec<f32> = (0..dim).map(|_| next()).collect();
            normalize(&mut t);
            t
        })
        .collect();
    let mut draw = |n: usize| {
        let mut out = Vec::with_capacity(n * dim);
        for _ in 0..n {
            let topic = &topics[((next() + 1.0) * 32.0) as usize % topics.len()];
            let mut row: Vec<f32> = topic.iter().map(|t| t + 0.35 * next()).collect();
            normalize(&mut row);
            out.extend_from_slice(&row);
        }
        out
    };
    Corpus {
        values: draw(rows),
        queries: draw(queries),
        dim,
        provenance: format!("synthetic {rows}x{dim}, 64 topics"),
    }
}

fn court(path: &str, rows: usize, queries: usize) -> Result<Corpus, Error> {
    let (dim, reader) = EmbeddingReader::open(&PathBuf::from(path))?;
    let dim = dim as usize;
    let mut values = Vec::with_capacity(rows * dim);
    let mut held_out = Vec::with_capacity(queries * dim);
    for (row, record) in reader.take(rows + queries).enumerate() {
        let record = record?;
        if row < rows {
            values.extend_from_slice(&record.vector);
        } else {
            held_out.extend_from_slice(&record.vector);
        }
    }
    if values.len() != rows * dim || held_out.len() != queries * dim {
        return Err(format!("{path} ended before {rows} rows plus {queries} queries").into());
    }
    Ok(Corpus {
        values,
        queries: held_out,
        dim,
        provenance: format!(
            "CourtListener rows 0..{rows}, queries {rows}..{}",
            rows + queries
        ),
    })
}

fn dot(left: &[f32], right: &[f32]) -> f32 {
    left.iter().zip(right).map(|(a, b)| a * b).sum()
}

fn by_score_desc(left: &(usize, f32), right: &(usize, f32)) -> Ordering {
    right.1.total_cmp(&left.1).then(left.0.cmp(&right.0))
}

/// Exact FP32 top-`k` ids per query, one query per thread at a time.
fn truth(corpus: &Corpus, k: usize) -> Vec<Vec<usize>> {
    let one = |query: &[f32]| {
        let mut scored: Vec<(usize, f32)> = corpus
            .values
            .chunks(corpus.dim)
            .enumerate()
            .map(|(id, row)| (id, dot(query, row)))
            .collect();
        let k = k.min(scored.len());
        scored.select_nth_unstable_by(k - 1, by_score_desc);
        scored.truncate(k);
        scored.sort_unstable_by(by_score_desc);
        scored.into_iter().map(|(id, _)| id).collect::<Vec<_>>()
    };
    let queries: Vec<&[f32]> = corpus.queries.chunks(corpus.dim).collect();
    let threads = std::thread::available_parallelism().map_or(1, |n| n.get());
    let per_thread = queries.len().div_ceil(threads).max(1);
    std::thread::scope(|scope| {
        let handles: Vec<_> = queries
            .chunks(per_thread)
            .map(|batch| scope.spawn(move || batch.iter().map(|q| one(q)).collect::<Vec<_>>()))
            .collect();
        handles
            .into_iter()
            .flat_map(|h| h.join().expect("truth thread"))
            .collect()
    })
}

/// Per-query result ids, best first.
fn ranked(results: &VectorSearchResults, query: usize) -> Vec<(usize, f32)> {
    let mut hits: Vec<(usize, f32)> = results
        .slots_for_query(query)
        .iter()
        .zip(results.scores_for_query(query))
        .filter_map(|(slot, score)| usize::try_from(*slot).ok().map(|id| (id, *score)))
        .collect();
    hits.sort_unstable_by(by_score_desc);
    hits
}

fn recall(got: &[Vec<usize>], wanted: &[Vec<usize>], k: usize) -> Value {
    let per_query: Vec<f64> = got
        .iter()
        .zip(wanted)
        .map(|(got, wanted)| {
            let got: HashSet<usize> = got.iter().take(k).copied().collect();
            wanted.iter().take(k).filter(|id| got.contains(id)).count() as f64 / k as f64
        })
        .collect();
    json!({
        "mean": per_query.iter().sum::<f64>() / per_query.len() as f64,
        "worst": per_query.iter().copied().fold(1.0, f64::min),
    })
}

fn percentiles(mut ms: Vec<f64>) -> Value {
    ms.sort_by(f64::total_cmp);
    let at = |f: f64| ms[((ms.len() - 1) as f64 * f).round() as usize];
    json!({ "p50_ms": at(0.5), "p95_ms": at(0.95), "p99_ms": at(0.99), "samples": ms.len() })
}

/// Single-query searches at depth `depth`; returns per-query ranked hits
/// from the last iteration and every latency sample.
fn timed(
    index: &VectorIndex,
    corpus: &Corpus,
    depth: usize,
    iterations: usize,
) -> Result<(Vec<Vec<(usize, f32)>>, Vec<f64>), Error> {
    let mut samples = Vec::new();
    let mut hits = Vec::new();
    for iteration in 0..iterations + 1 {
        hits.clear();
        for query in corpus.queries.chunks_exact(corpus.dim) {
            let started = Instant::now();
            let result = index.try_search(query, depth, VectorSearchOptions::new())?;
            let elapsed = started.elapsed().as_secs_f64() * 1e3;
            // Iteration 0 is warmup.
            if iteration > 0 {
                samples.push(elapsed);
            }
            hits.push(ranked(&result, 0));
        }
    }
    Ok((hits, samples))
}

fn rss_bytes() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status.lines().find_map(|line| {
                let kib = line.strip_prefix("VmRSS:")?.trim().strip_suffix(" kB")?;
                kib.trim().parse::<u64>().ok()
            })
        })
        .map_or(0, |kib| kib * 1024)
}

fn parse_config(spec: &str, bits: u32) -> Result<LeannGraphConfig, Error> {
    let parts = spec
        .split(':')
        .map(str::parse::<u32>)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("--configs entry {spec:?}: {e}"))?;
    let [hub_degree, base_degree, build_beam] = parts[..] else {
        return Err(format!("--configs entry {spec:?} is not M:m:build_beam").into());
    };
    let config = LeannGraphConfig {
        hub_degree,
        base_degree,
        build_beam,
        search_beam: 1,
        ..LeannGraphConfig::with_bits(bits as usize)
    };
    config.validate()?;
    Ok(config)
}

fn write(out: &PathBuf, value: &Value) -> Result<(), Error> {
    if let Some(parent) = out.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(out, serde_json::to_vec_pretty(value)?)?;
    Ok(())
}

fn main() -> Result<(), Error> {
    let queries: usize = arg("queries", "64").parse()?;
    let iterations: usize = arg("iterations", "3").parse()?;
    let rerank_depth: usize = arg("rerank-depth", "1000").parse()?;
    let bits: u32 = arg("bits", "8").parse()?;
    let flat_bits: usize = arg("flat-bits", "4").parse()?;
    let ks = list("k", "10,100")?;
    let beams = list("beams", "10,32,64,128,256,512")?;
    let out = PathBuf::from(arg("out", "leann-eval.json"));
    let configs = arg("configs", "32:8:64,64:16:128")
        .split(',')
        .map(|spec| parse_config(spec, bits).map(|c| (spec.to_string(), c)))
        .collect::<Result<Vec<_>, _>>()?;

    let synthetic_spec = arg("synthetic", "");
    let corpus = if synthetic_spec.is_empty() {
        let rows: usize = arg("vectors", "1000000").parse()?;
        court(
            &arg("input", "/work/court-corpus/embeddings-full.bin"),
            rows,
            queries,
        )?
    } else {
        let (rows, dim) = synthetic_spec
            .split_once(':')
            .ok_or("--synthetic is ROWS:DIM")?;
        synthetic(rows.parse()?, dim.parse()?, queries)
    };
    let rows = corpus.values.len() / corpus.dim;
    let max_k = *ks.last().expect("non-empty");
    eprintln!("leann-eval: {} ({} queries)", corpus.provenance, queries);

    let started = Instant::now();
    let truth = truth(&corpus, max_k);
    eprintln!(
        "leann-eval: FP32 truth in {:.1}s",
        started.elapsed().as_secs_f64()
    );

    let mut report = json!({
        "format": "protomolt-leann-eval-v1",
        "corpus": corpus.provenance,
        "rows": rows,
        "dimension": corpus.dim,
        "queries": queries,
        "iterations": iterations,
        "ks": ks,
        "beams": beams,
        "host_threads": std::thread::available_parallelism().map_or(0, |n| n.get()),
        "turbovec_4bit_planes": std::env::var("TURBOVEC_4BIT_PLANES").ok(),
        "flat": Value::Null,
        "graphs": [],
    });

    // Flat reference: the production engine, raw and after an FP32 rerank.
    let rss_before = rss_bytes();
    let started = Instant::now();
    let sample = rows.min(8192) * corpus.dim;
    let (shift, scale) = fit_calibration(corpus.dim, flat_bits, &corpus.values[..sample]);
    let mut flat = seeded_index(corpus.dim, flat_bits, &shift, &scale);
    flat.add(&corpus.values, corpus.dim)?;
    flat.prepare()?;
    let flat_build_s = started.elapsed().as_secs_f64();
    let flat_rss = rss_bytes().saturating_sub(rss_before);
    eprintln!("leann-eval: flat built in {flat_build_s:.1}s");
    let mut flat_cells = Vec::new();
    for &k in &ks {
        let (hits, samples) = timed(&flat, &corpus, k, iterations)?;
        let ids: Vec<Vec<usize>> = hits
            .iter()
            .map(|h| h.iter().map(|x| x.0).collect())
            .collect();
        flat_cells.push(json!({
            "k": k, "rerank": 0, "recall": recall(&ids, &truth, k), "latency": percentiles(samples),
        }));
        let depth = rerank_depth.max(k).min(rows);
        let (hits, samples) = timed(&flat, &corpus, depth, iterations)?;
        let reranked: Vec<Vec<usize>> = hits
            .iter()
            .zip(corpus.queries.chunks_exact(corpus.dim))
            .map(|(hits, query)| {
                let mut scored: Vec<(usize, f32)> = hits
                    .iter()
                    .map(|(id, _)| {
                        (
                            *id,
                            dot(query, &corpus.values[id * corpus.dim..][..corpus.dim]),
                        )
                    })
                    .collect();
                scored.sort_unstable_by(by_score_desc);
                scored.into_iter().map(|(id, _)| id).collect()
            })
            .collect();
        flat_cells.push(json!({
            "k": k, "rerank": depth, "recall": recall(&reranked, &truth, k),
            "latency_scan_only": percentiles(samples),
        }));
        eprintln!("leann-eval: flat k={k} done");
    }
    report["flat"] = json!({
        "bits": flat_bits, "build_s": flat_build_s, "retained_rss_bytes": flat_rss, "cells": flat_cells,
    });
    drop(flat);
    write(&out, &report)?;

    for (spec, config) in configs {
        let rss_before = rss_bytes();
        let started = Instant::now();
        let mut graph = LeannGraph::new(Some(corpus.dim), config)?.into_index();
        graph.add(&corpus.values, corpus.dim)?;
        graph.prepare()?;
        let build_s = started.elapsed().as_secs_f64();
        let retained = rss_bytes().saturating_sub(rss_before);
        eprintln!("leann-eval: graph {spec} built in {build_s:.1}s");
        let mut cells = Vec::new();
        for &k in &ks {
            for &beam in beams.iter().filter(|b| **b >= k) {
                let (hits, samples) = timed(&graph, &corpus, beam.max(k), iterations)?;
                let ids: Vec<Vec<usize>> = hits
                    .iter()
                    .map(|h| h.iter().map(|x| x.0).collect())
                    .collect();
                cells.push(json!({
                    "k": k, "beam": beam, "recall": recall(&ids, &truth, k), "latency": percentiles(samples),
                }));
            }
            eprintln!("leann-eval: graph {spec} k={k} done");
        }
        report["graphs"]
            .as_array_mut()
            .expect("array")
            .push(json!({
                "config": spec, "bits": bits, "build_s": build_s, "retained_rss_bytes": retained, "cells": cells,
            }));
        // Write after every graph so a killed run keeps what finished.
        write(&out, &report)?;
    }
    eprintln!("leann-eval: wrote {}", out.display());
    Ok(())
}

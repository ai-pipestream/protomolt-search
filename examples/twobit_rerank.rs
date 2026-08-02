//! The 2-bit + rerank experiment: can 2-bit codes (half the scan bytes of
//! 4-bit, same SIMD path — the LUT build takes `bits` and packing is
//! `8/bits`) plus an exact f32 rerank of the top k' reach 4-bit quality?
//!
//! Reads real corpus embeddings (`embeddings-full.bin`, records of a
//! 12-byte header + dim f32), holds out the records after the sample as
//! queries, computes exact f32 ground truth, and reports recall@10 and
//! median scan latency for raw 4-bit, raw 2-bit, and 2-bit + rerank at
//! several k'.
//!
//! ```text
//! cargo run --release --example twobit_rerank -- \
//!     --embeddings=/work/court-corpus/embeddings-full.bin --n=1000000
//! ```

use std::io::Read;
use std::time::Instant;

use turbovec::TurboQuantIndex;

const DIM: usize = 256;
const HEADER: usize = 12;
const K: usize = 10;

fn opt(args: &[String], name: &str) -> Option<String> {
    let prefix = format!("--{name}=");
    args.iter()
        .find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
}

/// Read `count` embedding rows (skipping the per-record header) into a
/// flat f32 buffer.
fn read_rows(reader: &mut impl Read, count: usize) -> Vec<f32> {
    let mut out = vec![0.0f32; count * DIM];
    let mut record = vec![0u8; HEADER + DIM * 4];
    for row in 0..count {
        reader
            .read_exact(&mut record)
            .expect("embeddings truncated");
        for d in 0..DIM {
            let base = HEADER + d * 4;
            out[row * DIM + d] =
                f32::from_le_bytes(record[base..base + 4].try_into().expect("4 bytes"));
        }
    }
    out
}

fn dot(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| x * y).sum()
}

/// Exact top-k row indices of `query` against `corpus` by f32 dot.
fn exact_topk(corpus: &[f32], query: &[f32], k: usize) -> Vec<i64> {
    let mut scored: Vec<(f32, i64)> = corpus
        .chunks(DIM)
        .enumerate()
        .map(|(i, row)| (dot(row, query), i as i64))
        .collect();
    scored.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)));
    scored.truncate(k);
    scored.into_iter().map(|(_, i)| i).collect()
}

fn recall_at_k(got: &[i64], truth: &[i64]) -> f64 {
    let hits = got.iter().filter(|g| truth.contains(g)).count();
    hits as f64 / truth.len() as f64
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let path = opt(&args, "embeddings")
        .unwrap_or_else(|| "/work/court-corpus/embeddings-full.bin".to_string());
    let n: usize = opt(&args, "n").map_or(1_000_000, |s| s.parse().unwrap());
    let n_queries: usize = opt(&args, "queries").map_or(64, |s| s.parse().unwrap());

    eprintln!("reading {n} corpus rows + {n_queries} held-out queries from {path}...");
    let file = std::fs::File::open(&path).expect("open embeddings");
    let mut reader = std::io::BufReader::with_capacity(1 << 22, file);
    // Skip the 12-byte file header (8-byte magic + u32 dim). Earlier
    // runs read from byte 0, shifting every parsed vector by 12 bytes;
    // the codec comparison stayed internally consistent (all codecs and
    // the ground truth saw identical vectors), but absolute vectors were
    // not the true corpus rows.
    let mut header = [0u8; 12];
    reader.read_exact(&mut header).expect("embeddings header");
    let corpus = read_rows(&mut reader, n);
    let queries = read_rows(&mut reader, n_queries);

    eprintln!("computing exact f32 ground truth ({n} x {n_queries} dots)...");
    let t0 = Instant::now();
    let truths: Vec<Vec<i64>> = std::thread::scope(|scope| {
        let workers: Vec<_> = queries
            .chunks(DIM)
            .map(|query| scope.spawn(|| exact_topk(&corpus, query, K)))
            .collect();
        workers.into_iter().map(|w| w.join().unwrap()).collect()
    });
    eprintln!("ground truth in {:?}", t0.elapsed());

    let mut indexes = Vec::new();
    for bits in [4usize, 2] {
        let t0 = Instant::now();
        let mut index = TurboQuantIndex::new(DIM, bits).expect("index");
        index.add(&corpus);
        index.prepare();
        eprintln!(
            "{bits}-bit index built in {:?} ({} MB packed)",
            t0.elapsed(),
            n * DIM * bits / 8 / (1024 * 1024)
        );
        indexes.push((bits, index));
    }

    // Median scan latency at k'=100, single query, unchunked kernel: the
    // isolated cost of a full sweep at each bit width.
    for (bits, index) in &indexes {
        let mut times: Vec<f64> = queries
            .chunks(DIM)
            .map(|q| {
                let t = Instant::now();
                std::hint::black_box(index.search(q, 100));
                t.elapsed().as_secs_f64()
            })
            .collect();
        times.sort_by(f64::total_cmp);
        println!(
            "{bits}-bit full sweep, k'=100: median {:.2} ms",
            times[times.len() / 2] * 1e3
        );
    }

    println!();
    println!("| config | recall@10 (mean over {n_queries} queries) |");
    println!("|---|---:|");
    for (bits, index) in &indexes {
        let mut recall = 0.0;
        for (query, truth) in queries.chunks(DIM).zip(&truths) {
            let got = index.search(query, K);
            recall += recall_at_k(got.indices_for_query(0), truth);
        }
        println!("| raw {bits}-bit | {:.4} |", recall / n_queries as f64);
    }
    for kp in [20usize, 50, 100, 200, 500, 1000] {
        for (bits, index) in &indexes {
            if *bits != 2 && kp != 100 {
                continue; // full rerank sweep for 2-bit; 4-bit once as reference
            }
            let mut recall = 0.0;
            for (query, truth) in queries.chunks(DIM).zip(&truths) {
                let got = index.search(query, kp);
                // Exact f32 rerank of the candidates, then top-10.
                let mut reranked: Vec<(f32, i64)> = got
                    .indices_for_query(0)
                    .iter()
                    .filter(|&&i| i >= 0)
                    .map(|&i| {
                        let row = &corpus[i as usize * DIM..(i as usize + 1) * DIM];
                        (dot(row, query), i)
                    })
                    .collect();
                reranked.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)));
                reranked.truncate(K);
                let top: Vec<i64> = reranked.into_iter().map(|(_, i)| i).collect();
                recall += recall_at_k(&top, truth);
            }
            println!(
                "| {bits}-bit + f32 rerank of top {kp} | {:.4} |",
                recall / n_queries as f64
            );
        }
    }
}

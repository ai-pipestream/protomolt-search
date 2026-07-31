//! v4-vs-v5 BM25 scorer benchmark (stage 1 of `docs/block-max.md`: the
//! occurrence split, no skipping).
//!
//! Builds a synthetic corpus with a court-like shape (zipfian-ish term df
//! distribution, one very-high-df term in ~45% of docs, avgdl ~200),
//! writes BOTH a v4 file (via the bench-only v4 writers) and a v5 file,
//! then times `bm25::top_k` for a few query shapes against the v4 reader
//! and the v5 reader, printing per-query wall medians and — via a
//! counting global allocator — allocations per query. The allocation
//! number is the falsifiable proof: the v4 scored path allocates one
//! offsets `Vec` per posting; the v5 path must not (expect ~1 per posting
//! in v4 vs ~zero per posting in v5).
//!
//! Usage:
//!
//! ```text
//! cargo run --release --example bm25_bench -- \
//!     [--docs 1000000] [--queries 20] [--k 10] [--vocab 20000] [--seed 45347]
//! ```
//!
//! Files land in `target/bm25-bench/` and are reused across runs with the
//! same docs/vocab/seed; delete the directory to force a rebuild.

use std::alloc::{GlobalAlloc, Layout, System};
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use turbovec_search::bm25::{self, Bm25Params, CorpusStats};
use turbovec_search::postings::{
    AnalyzedDoc, Bm25Index, Bm25Reader, DocTerms, SpillBuilder,
};

// --- counting allocator -------------------------------------------------

static ALLOCS: AtomicU64 = AtomicU64::new(0);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);

/// Counts allocations and allocated bytes process-wide. Wrapping the
/// system allocator keeps jemalloc-style arenas out of the measurement.
struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

fn alloc_snapshot() -> (u64, u64) {
    (
        ALLOCS.load(Ordering::Relaxed),
        ALLOC_BYTES.load(Ordering::Relaxed),
    )
}

// --- deterministic RNG (LCG, same style as harness::unit_vectors) -------

struct Lcg(u64);

impl Lcg {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_add(0x9E3779B97F4A7C15))
    }
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 33
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    /// [0, 1)
    fn unit(&mut self) -> f64 {
        self.next() as f64 / (1u64 << 31) as f64
    }
}

// --- corpus generation ---------------------------------------------------

const HIGH_DF_TERM: &str = "court";

/// Stream `docs` synthetic documents into `builder`. Deterministic in
/// `seed`, so the v4 and v5 builds see byte-identical input. Court-like
/// shape: `court` in ~45% of docs, remaining terms drawn u^3-skewed over
/// the vocabulary (df decays like a power law), avgdl ~200, one
/// occurrence span per occurrence.
fn generate(docs: usize, vocab: usize, seed: u64, builder: &mut SpillBuilder) -> io::Result<()> {
    let mut rng = Lcg::new(seed);
    let mut term_ids: Vec<u32> = Vec::with_capacity(512);
    for doc_id in 0..docs as u32 {
        let len = 100 + rng.below(200) as usize;
        term_ids.clear();
        // u32::MAX is the sentinel for the high-df term.
        if rng.below(100) < 45 {
            term_ids.push(u32::MAX);
        }
        for _ in 0..len {
            let u = rng.unit();
            term_ids.push((vocab as f64 * u * u * u) as u32);
        }
        term_ids.sort_unstable();
        let mut terms: DocTerms = Vec::new();
        let mut length = 0u32;
        let mut i = 0;
        while i < term_ids.len() {
            let t = term_ids[i];
            let mut tf = 1u32;
            i += 1;
            while i < term_ids.len() && term_ids[i] == t {
                tf += 1;
                i += 1;
            }
            let name = if t == u32::MAX {
                HIGH_DF_TERM.to_string()
            } else {
                format!("t{t}")
            };
            let offsets = (0..tf).map(|o| (o * 10, o * 10 + 5)).collect();
            length += tf;
            terms.push((name, tf, offsets));
        }
        builder.add_document_with_lineage(
            doc_id,
            String::new(),
            AnalyzedDoc { terms, length },
            None,
        )?;
        if doc_id % 100_000 == 0 {
            eprintln!("  ... {doc_id}/{docs} docs");
        }
    }
    Ok(())
}

// --- args -----------------------------------------------------------------

struct Args {
    docs: usize,
    queries: usize,
    ks: Vec<usize>,
    vocab: usize,
    seed: u64,
}

fn parse_args() -> Result<Args, String> {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let get = |key: &str| {
        let prefix = format!("--{key}=");
        argv.iter()
            .find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
    };
    let num = |key: &str, default: usize| {
        get(key)
            .map(|s| s.parse::<usize>().map_err(|e| format!("--{key}: {e}")))
            .transpose()
            .map(|v| v.unwrap_or(default))
    };
    Ok(Args {
        docs: num("docs", 1_000_000)?,
        queries: num("queries", 20)?,
        ks: get("k")
            .unwrap_or_else(|| "10,1000".to_string())
            .split(',')
            .map(|s| s.trim().parse::<usize>().map_err(|e| format!("--k: {e}")))
            .collect::<Result<Vec<_>, _>>()?,
        vocab: num("vocab", 20_000)?,
        seed: get("seed")
            .map(|s| s.parse::<u64>().map_err(|e| format!("--seed: {e}")))
            .transpose()?
            .unwrap_or(45347),
    })
}

fn median(mut xs: Vec<f64>) -> f64 {
    xs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    xs[xs.len() / 2]
}

fn main() -> Result<(), String> {
    let args = parse_args().map_err(|e| format!("bad args: {e}"))?;
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/bm25-bench");
    std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
    let stem = format!("d{}_v{}_s{}", args.docs, args.vocab, args.seed);
    let v4_path = dir.join(format!("{stem}.v4.bm25"));
    let v5_path = dir.join(format!("{stem}.v5.bm25"));

    // Build (or reuse) both formats from the same deterministic corpus.
    for (path, v4) in [(&v4_path, true), (&v5_path, false)] {
        if path.exists() {
            eprintln!("reusing {}", path.display());
            continue;
        }
        let t = Instant::now();
        eprintln!("building {} ...", path.display());
        let spill_dir = dir.join(format!("{stem}.{}.spill", if v4 { "v4" } else { "v5" }));
        let _ = std::fs::remove_dir_all(&spill_dir);
        let mut builder = if v4 {
            SpillBuilder::create_v4_for_bench(&spill_dir)
        } else {
            SpillBuilder::create(&spill_dir)
        }
        .map_err(|e| e.to_string())?;
        generate(args.docs, args.vocab, args.seed, &mut builder).map_err(|e| e.to_string())?;
        builder.finish(path).map_err(|e| e.to_string())?;
        eprintln!(
            "  done in {:.1}s, {:.1} MiB",
            t.elapsed().as_secs_f64(),
            path.metadata().map_err(|e| e.to_string())?.len() as f64 / 1e6
        );
    }

    let v4r = Bm25Reader::open(&v4_path).map_err(|e| e.to_string())?;
    let v5r = Bm25Reader::open(&v5_path).map_err(|e| e.to_string())?;

    // Query shapes: the 45%-df term alone, a multi-term mix, a mid-df
    // term, and a rare term.
    let rare = format!("t{}", args.vocab / 2);
    let rarest = format!("t{}", args.vocab - 1);
    let shapes: Vec<(&str, Vec<String>)> = vec![
        ("high-df [court]", vec![HIGH_DF_TERM.to_string()]),
        (
            "mixed [court t0 t10]",
            vec![HIGH_DF_TERM.to_string(), "t0".to_string(), "t10".to_string()],
        ),
        ("mid [t0 t10 t100]", vec!["t0".to_string(), "t10".to_string(), "t100".to_string()]),
        ("rare", vec![rare]),
        ("rarest", vec![rarest]),
    ];

    // One table per k. Configurations:
    //   old-v4:      the pre-v5 exhaustive scorer on the v4 reader (the
    //                "before" baseline: one offsets Vec per posting);
    //   v4:          the occurrence-split scorer on the v4 reader;
    //   v5:          the occurrence-split scorer on the v5 reader
    //                (stage 1: doc run only, offsets for survivors);
    //   v5-pruned:   block-max top_k_pruned, unseeded (stage 2+3:
    //                level skips + MaxScore partition);
    //   v5-seeded:   top_k_pruned seeded with the true k-th best — the
    //                realistic coordinator case.
    // Skip columns: level-0 blocks skipped (128 postings each) and
    // level-1 groups leapt (4096 postings each).
    for &k in &args.ks {
        println!("\n== k={k}");
        println!(
            "{:<24} {:>10} {:<12} {:>10} {:>10} {:>8} {:>8} {:>12}",
            "query", "postings", "config", "wall ms", "allocs/q", "l0 skip%", "l1 leaps", "evaluated"
        );
        for (label, terms) in &shapes {
            let dfs: Vec<u32> = terms.iter().map(|t| v5r.df(t)).collect();
            let stats = CorpusStats {
                doc_count: v5r.doc_count(),
                total_doc_length: v5r.total_doc_length(),
                dfs: dfs.clone(),
            };
            let postings: u64 = dfs.iter().map(|&d| u64::from(d)).sum();
            if postings == 0 {
                println!("{label:<24} (no postings; skipped)");
                continue;
            }
            let params = Bm25Params::default();
            // The seed for the floor variant: the true k-th best of the
            // unseeded pruned run (exact f64, so ties survive).
            let seed = bm25::top_k_pruned(
                &v5r,
                terms,
                &stats,
                params,
                k,
                f64::NEG_INFINITY,
            )
            .last()
            .map(|h| h.score)
            .unwrap_or(f64::NEG_INFINITY);

            #[derive(Clone, Copy)]
            enum Mode {
                OldV4,
                Plain(&'static str),
                Pruned(&'static str, f64),
            }
            let modes = [
                (Mode::OldV4, &v4r),
                (Mode::Plain("v4"), &v4r),
                (Mode::Plain("v5"), &v5r),
                (Mode::Pruned("v5-pruned", f64::NEG_INFINITY), &v5r),
                (Mode::Pruned("v5-seeded", seed), &v5r),
            ];
            let mut reference: Option<Vec<(u32, u64)>> = None;
            for (mode, reader) in &modes {
                let index: &dyn Bm25Index = *reader;
                let (name, mut last_stats) = match mode {
                    Mode::OldV4 => ("old-v4", bm25::PruneStats::default()),
                    Mode::Plain(name) => (*name, bm25::PruneStats::default()),
                    Mode::Pruned(name, _) => (*name, bm25::PruneStats::default()),
                };
                let run = |ps: &mut bm25::PruneStats| match mode {
                    Mode::OldV4 => bm25::top_k_exhaustive(index, terms, &stats, params, k),
                    Mode::Plain(_) => bm25::top_k(index, terms, &stats, params, k),
                    Mode::Pruned(_, floor) => {
                        bm25::top_k_pruned_stats(index, terms, &stats, params, k, *floor, ps)
                    }
                };
                // Warm up (also faults the pages in for all alike).
                for _ in 0..2 {
                    let _ = run(&mut last_stats);
                }
                let mut walls = Vec::new();
                let mut allocs = Vec::new();
                for _ in 0..args.queries.max(3) {
                    let mut ps = bm25::PruneStats::default();
                    let (a0, _) = alloc_snapshot();
                    let t = Instant::now();
                    let hits = run(&mut ps);
                    let wall = t.elapsed().as_secs_f64() * 1e3;
                    let (a1, _) = alloc_snapshot();
                    std::hint::black_box(&hits);
                    walls.push(wall);
                    allocs.push((a1 - a0) as f64);
                    last_stats = ps;
                }
                // Correctness: identical hit signatures across all
                // configurations (the seed is the exact k-th best, so
                // the seeded result equals the unseeded one).
                let sig: Vec<(u32, u64)> = run(&mut bm25::PruneStats::default())
                    .iter()
                    .map(|h| (h.doc_id, h.score.to_bits()))
                    .collect();
                match &reference {
                    None => reference = Some(sig),
                    Some(want) => assert_eq!(want, &sig, "{label}/{name} diverged at k={k}"),
                }
                let skip_pct = if last_stats.blocks_total > 0 {
                    100.0 * last_stats.blocks_skipped as f64 / last_stats.blocks_total as f64
                } else {
                    0.0
                };
                let evaluated = if matches!(mode, Mode::Pruned(..)) {
                    format!("{}", last_stats.candidates_evaluated)
                } else {
                    "-".to_string()
                };
                println!(
                    "{:<24} {:>10} {:<12} {:>10.2} {:>10.0} {:>8.1} {:>8} {:>12}",
                    label,
                    postings,
                    name,
                    median(walls),
                    median(allocs),
                    skip_pct,
                    last_stats.l1_groups_skipped,
                    evaluated
                );
            }
        }
    }
    Ok(())
}

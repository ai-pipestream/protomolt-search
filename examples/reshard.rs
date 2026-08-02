//! Offline resharding tool: replay a shard's write-ahead log to split it
//! into N child images or merge several shards' logs into one image —
//! split/merge as replay-from-log, with no re-embedding and no live
//! cluster. The logic lives in `turbovec_search::reshard`; this is the
//! thin CLI.
//!
//! ```text
//! # Split one shard 1 -> N (N a power of two):
//! reshard --log=/data/shard-0.tv.wal --split=2 --out-dir=/data/split \
//!     --slot-base=0 --slot-stride=25000000 --analysis-addr=http://localhost:50051
//!
//! # Merge several shards -> 1 (identical calibration required):
//! reshard --logs=/data/shard-0.tv.wal,/data/shard-1.tv.wal --out-dir=/data/merged \
//!     --analysis-addr=http://localhost:50051
//! ```
//!
//! `--log` accepts a WAL directory (the newest generation is replayed — a
//! snapshot install supersedes earlier generations) or one generation
//! directory. Documents are re-analyzed with the SAME analysis options
//! they were ingested with, so point `--analysis-addr` at the same
//! sidecar configuration the cluster ingests through. Writes
//! `<out>/shard-<i>.tv` (+ `.bm25`), `<out>/shard-map.toml`, and prints
//! the matching `[[shards]]` node config blocks.

use std::path::{Path, PathBuf};

use turbovec_search::pb::AnalysisSpec;
use turbovec_search::postings::AnalyzedDoc;
use turbovec_search::{analyzer, reshard};

fn arg(key: &str, default: &str) -> String {
    let prefix = format!("--{key}=");
    std::env::args()
        .find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
        .unwrap_or_else(|| default.to_string())
}

fn opt(key: &str) -> Option<String> {
    let prefix = format!("--{key}=");
    std::env::args().find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = arg("out-dir", "");
    if out_dir.is_empty() {
        return Err(
            "usage: reshard (--log=<wal dir|generation dir> --split=N | --logs=a,b,c) \
             --out-dir=<dir> [--slot-base=B] [--slot-stride=S] [--analysis-addr=ADDR]"
                .into(),
        );
    }
    let out_dir = PathBuf::from(out_dir);
    let analysis_addr = arg("analysis-addr", "http://127.0.0.1:50051");
    // Vector-only replay skips document analysis and the BM25 sidecars
    // entirely: shard-count and routing experiments only search the
    // vector leg, and children shrink from tens of GB to the .tv files.
    let vectors_only = std::env::args().any(|a| a == "--vectors-only");
    // Child BM25 field table override (docs/multi-field.md): comma list
    // starting with "body". Absent = derive from the replayed records.
    let bm25_fields: Option<Vec<String>> = opt("bm25-fields").map(|s| {
        s.split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect()
    });

    // The reshard core is synchronous; the sidecar client is async. Bridge
    // with block_in_place on the multi-thread runtime (same idiom the
    // court examples use for sync work under tokio). Each batch rides
    // `--analysis-streams` AnalyzeStreams, paced by the sidecar's flow
    // control (a sidecar predating the RPC is refused outright, not
    // quietly downgraded to per-document unary calls that would die deep
    // into the replay).
    //
    // One stream is a pipeline, not a parallel: analysis is the ceiling
    // on this replay, so raising the count lets the sidecar work on
    // several documents at once. It cannot change the result -- results
    // are keyed by sequence and analysis is a pure function of (text,
    // spec) -- so it is safe to tune against a live sidecar.
    let streams: usize = arg("analysis-streams", "1").parse()?;
    let addr = analysis_addr.clone();
    let handle = tokio::runtime::Handle::current();
    let mut analyze =
        move |docs: &[(&str, Option<&AnalysisSpec>)]| -> Result<Vec<AnalyzedDoc>, String> {
            tokio::task::block_in_place(|| {
                handle.block_on(analyzer::analyze_batch_streams(&addr, docs, streams))
            })
            .map_err(|e| format!("analysis sidecar at {addr}: {e}"))
        };

    let output = match opt("logs") {
        Some(logs) => {
            let generations = logs
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(|s| reshard::resolve_gen(Path::new(s)))
                .collect::<Result<Vec<_>, _>>()?;
            // --logs + --split is the general N -> M reshard: the inputs'
            // union redistributed across M children. --logs alone merges
            // to one image.
            match opt("split") {
                Some(n) => {
                    let n: usize = n.parse()?;
                    let slot_base: u64 = arg("slot-base", "0").parse()?;
                    let slot_stride: u64 = arg("slot-stride", "25000000").parse()?;
                    reshard::split_logs(
                        &generations,
                        n,
                        &out_dir,
                        slot_base,
                        slot_stride,
                        vectors_only,
                        bm25_fields.as_deref(),
                        &mut analyze,
                    )?
                }
                None => {
                    let slot_base = opt("slot-base").map(|s| s.parse::<u64>()).transpose()?;
                    reshard::merge(
                        &generations,
                        &out_dir,
                        slot_base,
                        vectors_only,
                        bm25_fields.as_deref(),
                        &mut analyze,
                    )?
                }
            }
        }
        None => {
            let log = opt("log").ok_or("split requires --log=<wal dir|generation dir>")?;
            let gen = reshard::resolve_gen(Path::new(&log))?;
            eprintln!("replaying {}", gen.display());
            let n: usize = arg("split", "2").parse()?;
            let slot_base: u64 = arg("slot-base", "0").parse()?;
            let slot_stride: u64 = arg("slot-stride", "25000000").parse()?;
            reshard::split(
                &gen,
                n,
                &out_dir,
                slot_base,
                slot_stride,
                vectors_only,
                bm25_fields.as_deref(),
                &mut analyze,
            )?
        }
    };

    let map = reshard::shard_map_toml(&output);
    let map_path = out_dir.join("shard-map.toml");
    std::fs::write(&map_path, &map)?;
    eprintln!("wrote {}", map_path.display());
    for child in &output.children {
        eprintln!(
            "child {}: {} vectors, {} documents, slot_offset {}, hash [{}, {}]",
            child.tv_path.display(),
            child.num_vectors,
            child.num_documents,
            child.slot_offset,
            child.hash_lo,
            child.hash_hi
        );
    }
    println!("# node config — one [[shards]] block per child\n");
    print!("{}", reshard::shards_toml(&output));
    println!(
        "\n# coordinator shard map (also written to {})\n",
        map_path.display()
    );
    print!("{map}");
    Ok(())
}

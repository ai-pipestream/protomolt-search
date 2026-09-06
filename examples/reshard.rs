//! Offline resharding tool: replay a shard's write-ahead log to split it
//! into N child images or merge several shards' logs into one image —
//! split/merge as replay-from-log, with no re-embedding and no live
//! cluster. The logic lives in `pipestream_search::reshard`; this is the
//! thin CLI.
//!
//! ```text
//! # Split one shard 1 -> N (N a power of two):
//! reshard --log=/data/shard-0.vector.wal --split=2 --out-dir=/data/split \
//!     --slot-base=0 --slot-stride=25000000 --analysis-addr=http://localhost:50051
//!     --stable-routing
//!
//! # Split one shard by the placement code its rows carry (docs/placement.md):
//! reshard --log=/data/shard-0.vector.wal --placement-column=placement \
//!     --placement-ranges=0..=0,18014398509481984..=36028797018963967,default \
//!     --out-dir=/data/split --slot-base=0 --slot-stride=25000000 \
//!     --analysis-addr=http://localhost:50051
//!
//! # Re-place one shard (or the union of several) under a NEW tree: the tree
//! # is evaluated on each document, its code rewritten, one child per leaf
//! # shard (docs/placement.md, "Changing the tree"). The file is the
//! # coordinator's shard map (its [placement] table) or just that table.
//! reshard --log=/data/shard-0.vector.wal --placement-tree=/data/root-map-v11.toml \
//!     --out-dir=/data/bands --slot-base=0 --slot-stride=25000000 \
//!     --analysis-addr=http://localhost:50051
//!
//! A re-placement split writes each child as a segment catalog
//! (`<out>/shard-<i>.tv.segments`, served with `--index=<out>/shard-<i>.tv`),
//! one sealed segment per spill bucket, so memory is one bucket of one child;
//! `--single-image=<max child rows>` writes one image per child instead and
//! refuses a child above the bound.
//!
//! # Merge several shards -> 1 (identical provider configuration required):
//! reshard --logs=/data/shard-0.vector.wal,/data/shard-1.vector.wal --out-dir=/data/merged \
//!     --analysis-addr=http://localhost:50051
//! ```
//!
//! `--log` accepts a WAL directory (the newest generation is replayed — a
//! snapshot install supersedes earlier generations) or one generation
//! directory. Documents are re-analyzed with the SAME analysis options
//! they were ingested with, so point `--analysis-addr` at the same
//! sidecar configuration the cluster ingests through. Writes
//! `<out>/shard-<i>.vector` (+ `.bm25`), `<out>/shard-map.toml`, and prints
//! the matching `[[shards]]` node config blocks.

use std::path::{Path, PathBuf};

use pipestream_search::pb::AnalysisSpec;
use pipestream_search::postings::AnalyzedDoc;
use pipestream_search::{analyzer, reshard};

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

#[allow(clippy::result_large_err)]
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = arg("out-dir", "");
    if out_dir.is_empty() {
        return Err(
            "usage: reshard (--log=<wal dir|generation dir> --split=N | --logs=a,b,c) \
             --out-dir=<dir> [--slot-base=B] [--slot-stride=S] [--analysis-addr=ADDR] \
             [--stable-routing] [--placement-tree=<file> [--single-image=<max child rows>] \
             [--spill-bucket-bits=<bits>]]"
                .into(),
        );
    }
    let out_dir = PathBuf::from(out_dir);
    let analysis_addr = arg("analysis-addr", "http://127.0.0.1:50051");
    // Vector-only replay skips document analysis and the BM25 sidecars
    // entirely: shard-count and routing experiments only search the
    // vector leg, and children shrink from tens of GB to provider images.
    let vectors_only = std::env::args().any(|a| a == "--vectors-only");
    let stable_routing = std::env::args().any(|a| a == "--stable-routing");
    if stable_routing && opt("logs").is_some() {
        return Err(
            "--stable-routing currently serves one live source; use --log, not --logs".into(),
        );
    }
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
    let mut analyze = move |docs: &[(
        &str,
        Option<&AnalysisSpec>,
        pipestream_search::analyzer::SessionLayers,
    )]|
          -> Result<Vec<AnalyzedDoc>, String> {
        tokio::task::block_in_place(|| {
            handle.block_on(analyzer::analyze_batch_streams(&addr, docs, streams))
        })
        .map_err(|e| format!("analysis sidecar at {addr}: {e}"))
    };

    if let Some(tree_path) = opt("placement-tree") {
        let generations = match (opt("logs"), opt("log")) {
            (Some(logs), _) => logs
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(|s| reshard::resolve_gen(Path::new(s)))
                .collect::<Result<Vec<_>, _>>()?,
            (None, Some(log)) => vec![reshard::resolve_gen(Path::new(&log))?],
            (None, None) => return Err("a re-placement split needs --log or --logs".into()),
        };
        if vectors_only {
            return Err(
                "a re-placement split evaluates the tree on documents; drop --vectors-only".into(),
            );
        }
        let tree = pipestream_search::config::load_placement_tree(Path::new(&tree_path))?;
        let placement = pipestream_search::placement::Placement::validate(&tree)?;
        let children = reshard::tree_children(&placement)?;
        let slot_base: u64 = arg("slot-base", "0").parse()?;
        let slot_stride: u64 = arg("slot-stride", "25000000").parse()?;
        let slot_offsets: Vec<u64> = (0..children.len() as u64)
            .map(|i| slot_base + i * slot_stride)
            .collect();
        for gen in &generations {
            eprintln!("replaying {}", gen.display());
        }
        let layout = match opt("single-image") {
            Some(bound) => reshard::TreeChildLayout::SingleImage {
                max_child_rows: bound.parse().map_err(|error| {
                    format!("--single-image takes the most rows a child may have: {error}")
                })?,
            },
            None => reshard::TreeChildLayout::Segmented,
        };
        let spill_bucket_bits = opt("spill-bucket-bits")
            .map(|bits| bits.parse::<u32>())
            .transpose()?;
        let placed = reshard::split_placement_tree_logs(
            &generations,
            &tree,
            &out_dir,
            &slot_offsets,
            bm25_fields.as_deref(),
            reshard::TreeSplitOptions {
                layout,
                spill_bucket_bits,
            },
            &mut analyze,
        )?;
        let map = reshard::tree_shard_map_toml(&placed, &tree)?;
        let map_path = out_dir.join("shard-map.toml");
        std::fs::write(&map_path, &map)?;
        eprintln!("wrote {}", map_path.display());
        eprintln!("{} documents changed code under the new tree", placed.moved);
        eprintln!(
            "spill logs of {} buckets; the largest replay held {} rows ({:?})",
            placed.spill_bucket_count, placed.peak_replay_rows, placed.layout
        );
        for (((image, child), rows), segments) in placed
            .images
            .children
            .iter()
            .zip(&placed.children)
            .zip(&placed.placed)
            .zip(&placed.segments)
        {
            eprintln!(
                "child {}: leaf {} (placement {}), {rows} documents, {} vectors, {segments} \
                 segments, slot_offset {}, hash [{}, {}]",
                image.vector_path.display(),
                child.leaf,
                child.code,
                image.num_vectors,
                image.slot_offset,
                image.hash_lo,
                image.hash_hi
            );
        }
        println!(
            "# node config: one [[shards]] block per child; add --placement-column={} \
                  --placement-leaf=<placement> --placement-tree={} to each\n",
            tree.column, tree_path
        );
        print!("{}", reshard::shards_toml(&placed.images));
        println!(
            "\n# coordinator shard map (also written to {})\n",
            map_path.display()
        );
        print!("{map}");
        return Ok(());
    }

    let (output, live_cutoff) = match opt("logs") {
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
                    (
                        reshard::split_logs(
                            &generations,
                            n,
                            &out_dir,
                            slot_base,
                            slot_stride,
                            vectors_only,
                            bm25_fields.as_deref(),
                            &mut analyze,
                        )?,
                        None,
                    )
                }
                None => {
                    let slot_base = opt("slot-base").map(|s| s.parse::<u64>()).transpose()?;
                    (
                        reshard::merge(
                            &generations,
                            &out_dir,
                            slot_base,
                            vectors_only,
                            bm25_fields.as_deref(),
                            &mut analyze,
                        )?,
                        None,
                    )
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
            if let Some(column) = opt("placement-column") {
                // --placement-ranges=lo..=hi,lo..=hi[,default]: one child per
                // entry, in order; `default` is the child that takes rows with
                // no code or a code outside every range.
                let spec = opt("placement-ranges")
                    .ok_or("a placement split needs --placement-ranges=lo..=hi,...[,default]")?;
                let mut children = Vec::new();
                let mut default_child = None;
                for (i, entry) in spec.split(',').map(str::trim).enumerate() {
                    if entry == "default" {
                        default_child = Some(i);
                        children.push(reshard::PlacementChild::NONE);
                        continue;
                    }
                    let (lo, hi) = entry
                        .split_once("..=")
                        .ok_or_else(|| format!("placement range {entry:?} is not lo..=hi"))?;
                    children.push(reshard::PlacementChild {
                        lo: lo.trim().parse()?,
                        hi: hi.trim().parse()?,
                    });
                }
                let slot_offsets: Vec<u64> = (0..children.len() as u64)
                    .map(|i| slot_base + i * slot_stride)
                    .collect();
                let placed = reshard::split_placement_logs(
                    &[gen],
                    &column,
                    &children,
                    default_child,
                    &out_dir,
                    &slot_offsets,
                    vectors_only,
                    bm25_fields.as_deref(),
                    &mut analyze,
                )?;
                let map = reshard::placement_shard_map_toml(&placed);
                let map_path = out_dir.join("shard-map.toml");
                std::fs::write(&map_path, &map)?;
                eprintln!("wrote {}", map_path.display());
                for (child, range) in placed.images.children.iter().zip(&placed.ranges) {
                    eprintln!(
                        "child {}: {} vectors, {} documents, slot_offset {}, placement {}..={}",
                        child.vector_path.display(),
                        child.num_vectors,
                        child.num_documents,
                        child.slot_offset,
                        range.lo,
                        range.hi
                    );
                }
                print!("{}", reshard::shards_toml(&placed.images));
                return Ok(());
            }
            if stable_routing {
                let stable = reshard::split_stable_logs(
                    &[gen],
                    n,
                    &out_dir,
                    slot_base,
                    slot_stride,
                    vectors_only,
                    bm25_fields.as_deref(),
                    &mut analyze,
                )?;
                let cutoff = stable.source_cutoffs[0];
                (stable.images, Some(cutoff))
            } else {
                (
                    reshard::split(
                        &gen,
                        n,
                        &out_dir,
                        slot_base,
                        slot_stride,
                        vectors_only,
                        bm25_fields.as_deref(),
                        &mut analyze,
                    )?,
                    None,
                )
            }
        }
    };

    let map = reshard::shard_map_toml(&output);
    let map_path = out_dir.join("shard-map.toml");
    std::fs::write(&map_path, &map)?;
    eprintln!("wrote {}", map_path.display());
    if let Some(cutoff) = live_cutoff {
        let cutoff_path = out_dir.join("live-cutoff.toml");
        std::fs::write(
            &cutoff_path,
            format!(
                "generation = {}\nhigh_watermark = {}\n",
                cutoff.generation, cutoff.high_watermark
            ),
        )?;
        eprintln!("wrote {}", cutoff_path.display());
    }
    for child in &output.children {
        eprintln!(
            "child {}: {} vectors, {} documents, slot_offset {}, hash [{}, {}]",
            child.vector_path.display(),
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

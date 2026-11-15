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
             [--spill-bucket-bits=<bits>] [--from-segments] [--build-threads=<n>] [--build-queue=<n>] \
             [--build-memory=<MiB>] [--only-child=<index>] [--cut-column=<col> \
             [--cut-rows=<n>]] [--derived-columns=<file> [--derive=a,b]]]"
                .into(),
        );
    }
    let out_dir = PathBuf::from(out_dir);
    match reshard::raise_open_files_limit() {
        Some(limit) => eprintln!("reshard: open-files limit {limit}"),
        None => eprintln!("reshard: open-files limit left as is"),
    }
    let analysis_addr = arg("analysis-addr", "http://127.0.0.1:50051");
    let vectors_only = std::env::args().any(|a| a == "--vectors-only");
    let stable_routing = std::env::args().any(|a| a == "--stable-routing");
    if stable_routing && opt("logs").is_some() {
        return Err(
            "--stable-routing currently serves one live source; use --log, not --logs".into(),
        );
    }
    let bm25_fields: Option<Vec<String>> = opt("bm25-fields").map(|s| {
        s.split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect()
    });
    let streams: usize = arg("analysis-streams", "1").parse()?;
    let handle = tokio::runtime::Handle::current();
    let mut analyze = move |docs: &[(
        &str,
        Option<&AnalysisSpec>,
        pipestream_search::analyzer::SessionLayers,
    )]|
          -> Result<Vec<AnalyzedDoc>, String> {
        tokio::task::block_in_place(|| {
            handle.block_on(analyzer::analyze_batch_streams(
                &analysis_addr,
                docs,
                streams,
            ))
        })
        .map_err(|e| format!("analysis sidecar at {analysis_addr}: {e}"))
    };

    let ctx = CliArgs {
        out_dir: &out_dir,
        vectors_only,
        stable_routing,
        bm25_fields: bm25_fields.as_deref(),
    };
    if let Some(tree_path) = opt("placement-tree") {
        return run_placement_tree_reshard(&ctx, &tree_path, &mut analyze);
    }
    if let Some((output, cutoff)) = run_logs_or_single_reshard(&ctx, &mut analyze)? {
        write_reshard_outputs(&out_dir, &output, cutoff)?;
    }
    Ok(())
}

type ReshardErr = Box<dyn std::error::Error>;

struct CliArgs<'a> {
    out_dir: &'a Path,
    vectors_only: bool,
    stable_routing: bool,
    bm25_fields: Option<&'a [String]>,
}

fn parse_generations_from_flags() -> Result<Vec<PathBuf>, ReshardErr> {
    match (opt("logs"), opt("log")) {
        (Some(logs), _) => Ok(logs
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| reshard::resolve_gen(Path::new(s)))
            .collect::<Result<Vec<_>, _>>()?),
        (None, Some(log)) => Ok(vec![reshard::resolve_gen(Path::new(&log))?]),
        (None, None) => Err("a re-placement split needs --log or --logs".into()),
    }
}

fn parse_derived_declaration(
) -> Result<Option<std::sync::Arc<pipestream_search::derived::Declaration>>, String> {
    opt("derived-columns")
        .map(|path| {
            pipestream_search::derived::load_declaration(Path::new(&path))
                .and_then(|spec| pipestream_search::derived::Declaration::compile(&spec))
                .map(std::sync::Arc::new)
                .map_err(|error| format!("--derived-columns={path}: {error}"))
        })
        .transpose()
}

fn parse_tree_split_options() -> Result<reshard::TreeSplitOptions, ReshardErr> {
    let layout = match opt("single-image") {
        Some(bound) => reshard::TreeChildLayout::SingleImage {
            max_child_rows: bound
                .parse()
                .map_err(|e| format!("--single-image takes the most rows a child may have: {e}"))?,
        },
        None => reshard::TreeChildLayout::Segmented,
    };
    let spill_bucket_bits = opt("spill-bucket-bits")
        .map(|b| b.parse::<u32>())
        .transpose()?;
    let source = if std::env::args().any(|a| a == "--from-segments") {
        reshard::TreeRowSource::Segments
    } else {
        reshard::TreeRowSource::Logs
    };
    let cut = match opt("cut-column") {
        Some(column) => reshard::SpillCut::Column {
            column,
            rows_per_cut: arg("cut-rows", "1000000").parse()?,
        },
        None => reshard::SpillCut::Hash,
    };
    let only_child = opt("only-child")
        .map(|i| {
            i.parse::<usize>()
                .map_err(|e| format!("--only-child takes the child's index in leaf order: {e}"))
        })
        .transpose()?;
    let build_threads = arg("build-threads", "1")
        .parse::<usize>()
        .map_err(|e| format!("--build-threads takes a thread count: {e}"))?;
    if build_threads == 0 {
        return Err("--build-threads takes at least one thread".into());
    }
    let build_queue = opt("build-queue")
        .map(|n| {
            n.parse::<usize>()
                .map_err(|e| format!("--build-queue takes a depth: {e}"))
        })
        .transpose()?;
    let build_memory = opt("build-memory")
        .map(|mb| {
            mb.parse::<u64>()
                .ok()
                .and_then(|mb| mb.checked_mul(1 << 20))
                .ok_or_else(|| "--build-memory takes a MiB count".to_string())
        })
        .transpose()?;
    let derived = parse_derived_declaration()?;
    let derive: Vec<String> = opt("derive")
        .map(|names| {
            names
                .split(',')
                .map(str::trim)
                .filter(|n| !n.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    Ok(reshard::TreeSplitOptions {
        layout,
        spill_bucket_bits,
        source,
        cut,
        only_child,
        open_files_limit: None,
        derived,
        derive,
        build_threads,
        build_queue,
        build_memory,
    })
}

fn run_placement_tree_reshard(
    ctx: &CliArgs<'_>,
    tree_path: &str,
    analyze: &mut reshard::Analyzer,
) -> Result<(), ReshardErr> {
    let generations = parse_generations_from_flags()?;
    if ctx.vectors_only {
        return Err(
            "a re-placement split evaluates the tree on documents; drop --vectors-only".into(),
        );
    }
    let tree = pipestream_search::config::load_placement_tree(Path::new(tree_path))?;
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
    let options = parse_tree_split_options()?;
    let placed = reshard::split_placement_tree_logs(
        &generations,
        &tree,
        ctx.out_dir,
        &slot_offsets,
        ctx.bm25_fields,
        options,
        analyze,
    )?;
    let map = reshard::tree_shard_map_toml(&placed, &tree)?;
    let map_path = ctx.out_dir.join("shard-map.toml");
    std::fs::write(&map_path, &map)?;
    report_tree_reshard(&placed, &tree.column, tree_path, &map_path, &map);
    Ok(())
}

fn report_tree_reshard(
    placed: &reshard::TreeReshardOutput,
    column: &str,
    tree_path: &str,
    map_path: &Path,
    map: &str,
) {
    eprintln!("wrote {}", map_path.display());
    eprintln!("{} documents changed code under the new tree", placed.moved);
    eprintln!(
        "spill logs of {} buckets; the largest replay held {} rows ({:?})",
        placed.spill_bucket_count, placed.peak_replay_rows, placed.layout
    );
    eprintln!(
        "analysis: {:?}, {} documents transplanted, largest transpose {} bytes; spill cut {:?}",
        placed.source, placed.transplanted_rows, placed.peak_transpose_bytes, placed.cut
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
        "# node config: one [[shards]] block per child; add --placement-column={column} \
         --placement-leaf=<placement> --placement-tree={tree_path} to each\n"
    );
    print!("{}", reshard::shards_toml(&placed.images));
    println!(
        "\n# coordinator shard map (also written to {})\n",
        map_path.display()
    );
    print!("{map}");
}

fn run_placement_column_reshard(
    ctx: &CliArgs<'_>,
    gen: &PathBuf,
    column: &str,
    slot_base: u64,
    slot_stride: u64,
    analyze: &mut reshard::Analyzer,
) -> Result<(), ReshardErr> {
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
    let req = reshard::PlacementSplitRequest::new(
        std::slice::from_ref(gen),
        column,
        &children,
        ctx.out_dir,
        &slot_offsets,
    )
    .default_child(default_child)
    .vectors_only(ctx.vectors_only)
    .bm25_fields(ctx.bm25_fields);
    let placed = reshard::split_placement_logs_with(&req, analyze)?;
    let map = reshard::placement_shard_map_toml(&placed);
    let map_path = ctx.out_dir.join("shard-map.toml");
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
    Ok(())
}

fn run_logs_or_single_reshard(
    ctx: &CliArgs<'_>,
    analyze: &mut reshard::Analyzer,
) -> Result<Option<(reshard::ReshardOutput, Option<reshard::WalCutoff>)>, ReshardErr> {
    if let Some(logs) = opt("logs") {
        let generations = logs
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| reshard::resolve_gen(Path::new(s)))
            .collect::<Result<Vec<_>, _>>()?;
        let output = match opt("split") {
            Some(n) => {
                let req = reshard::SplitRequest::new(&generations, n.parse()?, ctx.out_dir)
                    .slot_base(arg("slot-base", "0").parse()?)
                    .slot_stride(arg("slot-stride", "25000000").parse()?)
                    .vectors_only(ctx.vectors_only)
                    .bm25_fields(ctx.bm25_fields);
                reshard::split_logs_with(&req, analyze)?
            }
            None => {
                let slot_base = opt("slot-base").map(|s| s.parse::<u64>()).transpose()?;
                reshard::merge(
                    &generations,
                    ctx.out_dir,
                    slot_base,
                    ctx.vectors_only,
                    ctx.bm25_fields,
                    analyze,
                )?
            }
        };
        return Ok(Some((output, None)));
    }
    let log = opt("log").ok_or("split requires --log=<wal dir|generation dir>")?;
    let gen = reshard::resolve_gen(Path::new(&log))?;
    eprintln!("replaying {}", gen.display());
    let n: usize = arg("split", "2").parse()?;
    let slot_base: u64 = arg("slot-base", "0").parse()?;
    let slot_stride: u64 = arg("slot-stride", "25000000").parse()?;
    if let Some(column) = opt("placement-column") {
        run_placement_column_reshard(ctx, &gen, &column, slot_base, slot_stride, analyze)?;
        return Ok(None);
    }
    let req = reshard::SplitRequest::new(std::slice::from_ref(&gen), n, ctx.out_dir)
        .slot_base(slot_base)
        .slot_stride(slot_stride)
        .vectors_only(ctx.vectors_only)
        .bm25_fields(ctx.bm25_fields);
    if ctx.stable_routing {
        let stable = reshard::split_stable_logs_with(&req, analyze)?;
        Ok(Some((stable.images, Some(stable.source_cutoffs[0]))))
    } else {
        Ok(Some((reshard::split_logs_with(&req, analyze)?, None)))
    }
}

fn write_reshard_outputs(
    out_dir: &Path,
    output: &reshard::ReshardOutput,
    live_cutoff: Option<reshard::WalCutoff>,
) -> Result<(), ReshardErr> {
    let map = reshard::shard_map_toml(output);
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
    print!("{}", reshard::shards_toml(output));
    println!(
        "\n# coordinator shard map (also written to {})\n",
        map_path.display()
    );
    print!("{map}");
    Ok(())
}

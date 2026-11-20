//! The placement-tree split itself: route, spill, and build every leaf child.

mod build;
mod cut;
mod plan;
mod segmented;
mod spill;

use std::path::{Path, PathBuf};

use super::analysis_sidecar::drop_segment_cache;
use super::child_build::child_order;
use super::reconstruct::{transpose_segment, transposed_fields};
use super::replay::{read_gen_manifest, replay_buckets_through, Replay};
use super::segment_sources::open_segment_exact;
use super::tree_split::{TreeReshardOutput, TreeRowSource, TreeSplitOptions};
use super::{Analyzer, ReshardOutput};

use self::build::{check_build_memory, PinnedTables, TreeBuild};
use self::plan::{TreePlan, TreeTarget};
use self::spill::{RoutedRow, SegmentRows, TreeSpill};

/// Split one or more full-history generations under a NEW placement
/// tree: the tree is
/// evaluated on each live document's stored values, the way the
/// coordinator routes at ingest, the placement column is rewritten to
/// the code the new tree assigns, and the document goes to the child
/// serving that leaf (inside a leaf with several shards, by its stable
/// key's hash, which each such row must then carry). A vector row with
/// no document cannot be evaluated and refuses the split by id. Memory
/// is bounded by one WAL bucket during routing and by one spill bucket
/// while each child is built: rows are spilled per child under
/// `out_dir/spill` first, as a partitioned compaction does, and each
/// child is a catalog sealed one spill bucket at a time. Offline
/// only: no live cutoff is recorded, so the sources must be quiescent.
pub fn split_placement_tree_logs(
    gens: &[PathBuf],
    tree: &crate::placement::PlacementTreeConfig,
    out_dir: &Path,
    slot_offsets: &[u64],
    bm25_fields: Option<&[String]>,
    options: TreeSplitOptions,
    analyze: &mut Analyzer,
) -> Result<TreeReshardOutput, String> {
    let target = TreeTarget {
        out_dir,
        slot_offsets,
    };
    let plan = TreePlan::open(gens, tree, target, &options)?;
    let cut_plans = cut::cut_plans(&plan)?;
    let bits = cut::spill_bucket_bits(&plan, cut_plans.as_deref())?;
    cut::check_open_files(&plan, cut_plans.as_deref(), 1u32 << bits)?;
    let mut spill = TreeSpill::create(&plan, cut_plans, bits)?;
    match &plan.sources {
        None => spill_logs(&mut spill)?,
        Some(_) => spill_segments(&mut spill)?,
    }
    let spill = spill.finish()?;
    let pinned = PinnedTables::of(&plan);
    check_build_memory(&plan, &spill)?;
    let build = TreeBuild::new(&plan, &spill, &pinned, bm25_fields)?;
    let built = segmented::build_segmented(&build, analyze)?;
    let generation = build.generation;
    let spill_root = out_dir.join("spill");
    std::fs::remove_dir_all(&spill_root)
        .map_err(|error| format!("remove spill {}: {error}", spill_root.display()))?;
    let stats = spill.stats;
    Ok(TreeReshardOutput {
        derived: plan
            .declaration
            .as_deref()
            .map(|declaration| declaration.config())
            .unwrap_or_default(),
        images: ReshardOutput {
            generation,
            children: built.images,
        },
        children: plan.children.clone(),
        moved: stats.moved,
        placed: stats.placed,
        segments: built.segments,
        peak_replay_rows: built.peak_replay_rows,
        spill_bucket_count: spill.spill_bucket_count,
        source: options.source,
        cut: options.cut.clone(),
        peak_transpose_bytes: stats.peak_transpose_bytes,
        transplanted_rows: stats.transplanted_rows,
    })
}

/// The refusals that depend only on the options and the child count.
fn check_options(options: &TreeSplitOptions, children: usize) -> Result<(), String> {
    if let Some(only) = options.only_child {
        if only >= children {
            return Err(format!(
                "--only-child={only} names no child; the tree has {children} leaf shards \
                 (0..{})",
                children.saturating_sub(1)
            ));
        }
    }
    if options.build_threads > 1 {
        let threads = options.build_threads;
        if options.source != TreeRowSource::Segments {
            return Err(format!(
                "--build-threads={threads} builds buckets at once from transplanted fields; the \
                 log replay analyzes through one sidecar session, split with --from-segments or \
                 one thread"
            ));
        }
    }
    if options.build_queue == Some(0) {
        return Err("--build-queue=0 holds no finished bucket; give it one or more".to_string());
    }
    Ok(())
}

/// Route every live row of the logs, one WAL bucket at a time.
fn spill_logs(spill: &mut TreeSpill<'_>) -> Result<(), String> {
    for gen in spill.plan.gens {
        let current = read_gen_manifest(gen)?;
        let (count, dim) = (current.bucket_count as usize, current.dim as usize);
        for bucket in 0..current.bucket_count {
            let mut replay = Replay::default();
            replay_buckets_through(
                gen,
                bucket..bucket + 1,
                count,
                dim,
                false,
                u64::MAX,
                &mut replay,
            )?;
            replay.compact();
            for id in child_order(&replay) {
                spill_log_row(spill, &replay, id, current.dim)?;
            }
        }
    }
    Ok(())
}

fn spill_log_row(
    spill: &mut TreeSpill<'_>,
    replay: &Replay,
    id: u64,
    dim: u32,
) -> Result<(), String> {
    spill.first_sight(id, "input generation")?;
    let Some(document) = replay.documents.get(&id) else {
        return Err(format!(
            "live source id {id} has a vector and no document; a re-placement split evaluates \
             the tree on documents (a code-keyed split, --placement-ranges, routes such a row \
             to its default child)"
        ));
    };
    let key = replay.stable_keys.get(&id);
    let (index, document) = spill.route_row(id, document.clone(), key)?;
    let vector = replay
        .vectors
        .get(&id)
        .map(|vector| (vector.clone(), dim, replay.retained.get(&id).cloned()));
    spill.file_row(RoutedRow {
        index,
        id,
        document,
        vector,
        keys: key.cloned().into_iter().collect(),
        fields: None,
    })
}

/// Route every live row of the sealed segments, one segment in memory at
/// a time; each segment's pages leave the resident set and the page cache
/// once it is read, so the run's footprint is one segment's, not the
/// catalog's.
fn spill_segments(spill: &mut TreeSpill<'_>) -> Result<(), String> {
    let plan = spill.plan;
    let (shards, _) = plan.sources.as_ref().expect("segment sources");
    for shard in shards {
        let dim = read_gen_manifest(&shard.gen)?.dim;
        for i in 0..shard.set.len() {
            let meta = shard.set.metadata(i);
            let context = || format!("{} segment {}", shard.root.display(), meta.segment_id);
            let bm25 = shard.set.bm25(i);
            let exact = open_segment_exact(shard, i)?;
            if exact.is_none() && !shard.set.retained_segment(i).is_empty() {
                return Err(format!(
                    "{}: retained rows have no active vectors",
                    context()
                ));
            }
            let rows = bm25.next_doc_id();
            if u64::from(rows) != meta.rows {
                return Err(format!(
                    "{}: the store has {rows} rows, the metadata {}",
                    context(),
                    meta.rows
                ));
            }
            let transposes = transpose_segment(bm25, &context)?;
            let bytes = transposes.iter().map(|t| t.bytes() as u64).sum::<u64>();
            let stats = &mut spill.stats;
            stats.peak_transpose_bytes = stats.peak_transpose_bytes.max(bytes);
            let segment = SegmentRows {
                shard,
                segment: i,
                exact: exact.as_ref(),
                transposes: &transposes,
                dim,
            };
            for row in 0..rows {
                spill_segment_row(spill, &segment, row)?;
            }
            drop(transposes);
            if let Some(store) = exact.as_ref() {
                store
                    .release_pages()
                    .map_err(|e| format!("{}: {e}", context()))?;
            }
            drop(exact);
            bm25.release_pages()
                .map_err(|e| format!("{}: {e}", context()))?;
            drop_segment_cache(&shard.root, &meta.segment_id);
        }
    }
    Ok(())
}

fn spill_segment_row(
    spill: &mut TreeSpill<'_>,
    segment: &SegmentRows<'_>,
    row: u32,
) -> Result<(), String> {
    let shard = segment.shard;
    let label = shard.set.metadata(segment.segment).base_label + u64::from(row);
    if !shard.is_live(segment.segment, row, label) {
        return Ok(());
    }
    let id = shard.slot_offset + label;
    spill.first_sight(id, "source")?;
    let (_, tables) = spill.plan.sources.as_ref().expect("segment sources");
    let bm25 = shard.set.bm25(segment.segment);
    let document = super::reconstruct::reconstruct_document(bm25, row, tables, true)
        .map_err(|e| format!("{}: {e}", segment.context()))?;
    let (index, document) = spill.route_row(id, document, None)?;
    let vector = segment.vector(row)?;
    let fields = transposed_fields(segment.transposes, row, &|| segment.context())?;
    spill.file_row(RoutedRow {
        index,
        id,
        document,
        vector,
        keys: Vec::new(),
        fields: Some(&fields),
    })?;
    spill.stats.transplanted_rows += 1;
    Ok(())
}

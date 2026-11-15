//! Partitioned compaction straight from sealed segments
//! (docs/replay-from-segments.md, "Partitioned compaction of a catalog
//! without a log").

mod build;
mod shape;
mod spill;

use std::collections::BTreeMap;
use std::path::Path;

use super::hnsw_sources::HnswCompactionSources;
use super::tree_split::BUILD_BYTES_PER_ROW;
use super::{ChildImage, PartitionSpec};

use self::build::CutBuild;
use self::shape::CatalogShape;
use self::spill::{count_keys, spill_manifest, CatalogRows, CutSpill};

/// The build bounds a compaction from segments accepts: the same knobs a
/// re-placement split's segmented build takes (`--build-threads`,
/// `--build-queue`, `--build-memory`, docs/replay-from-segments.md),
/// with the same refusal rules.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentCompactionOptions {
    /// Cuts built at once: each thread replays one cut's spill into
    /// memory and seals it, the images append in cut order, so the
    /// catalog is the one-thread build's, byte for byte.
    pub build_threads: usize,
    /// Sealed-but-unappended cut images held at once: a worker waits for
    /// backlog space before claiming its next cut. `None` takes the
    /// thread count; zero is refused.
    pub build_queue: Option<usize>,
    /// A fixed 70 KiB/row planning admission estimate in bytes
    /// ([`BUILD_BYTES_PER_ROW`]): the largest cut alone, and the thread
    /// count times it, must fit. A plan that cannot fit is refused by
    /// name; nothing is lowered to fit. `None` enforces no check.
    pub build_memory: Option<u64>,
}

impl Default for SegmentCompactionOptions {
    fn default() -> Self {
        Self {
            build_threads: 1,
            build_queue: None,
            build_memory: None,
        }
    }
}

/// What [`compact_segments_partitioned`] built of a catalog's sealed set.
#[derive(Debug)]
pub struct SegmentCompactionBuild {
    /// One image per cut, in cut order (ascending keys, the unkeyed cut
    /// last), each `slot_offset` its dense local base.
    pub images: Vec<ChildImage>,
    /// Source global id -> dense LOCAL slot, for every live row.
    pub id_map: BTreeMap<u64, u64>,
    /// Rows the sealed set held, tombstoned or not.
    pub rows_before: u64,
    /// Rows tombstoned at the cutoff, which the outputs drop.
    pub tombstones: u64,
    /// The catalog's mapped-plan binding, pinned on every output.
    pub binding: Option<crate::postings::StoredBinding>,
}

/// Build the dense partitioned images of one catalog's sealed set without
/// its log (`docs/replay-from-segments.md`): the row source is the
/// segments themselves, each document's analyzed fields transposed from
/// the postings (`FieldView::transpose`), never the analyzer; the live
/// rows are keyed by `spec.column` and cut into partitions of at most
/// `spec.bound` rows by the split's cut-point selection (`ChildCutPlan`,
/// the `--cut-column/--cut-rows` logic); rows without the column seal in
/// one final unkeyed cut.
///
/// `set` is the cutoff snapshot of the live catalog and `overlay` the
/// shard-wide live-document overlay at the cutoff: a row either names is
/// tombstoned and does not move. Every segment must share one field
/// table, one fingerprint per field, one column table, one derived-column
/// declaration, and one vector provider state; a difference is refused
/// naming the segment. The outputs pin those tables and the declaration
/// (the kind-15 entry) on every image, and `binding` is the catalog's own
/// generation binding.
///
/// Three passes, none holding more than one segment's transposes or one
/// cut's replay in memory: a key count over the column tables, a spill of
/// each live row into its cut's single-bucket log plus analysis sidecar,
/// then one image per cut — single-threaded, or `options.build_threads`
/// cuts at once under the same backlog and budget rules as the split.
/// Slots come from the spill's own row counts before any cut is replayed,
/// so the threaded build appends in cut order and is byte for byte the
/// serial build's.
pub fn compact_segments_partitioned(
    set: &crate::segments::OpenedSegmentSet,
    overlay: &crate::live_docs::LiveDocs,
    slot_offset: u64,
    out_dir: &Path,
    spec: PartitionSpec<'_>,
    options: &SegmentCompactionOptions,
) -> Result<SegmentCompactionBuild, String> {
    let binding = check_request(set, spec, options)?;
    let shape = CatalogShape::check(set)?;
    let rows = CatalogRows {
        set,
        overlay,
        slot_offset,
    };
    let mut count = count_keys(rows, &shape, spec)?;
    std::fs::create_dir_all(out_dir)
        .map_err(|error| format!("mkdir {}: {error}", out_dir.display()))?;
    let spill_root = out_dir.join("spill");
    let mut spill = CutSpill::create(rows, &shape, spec, &spill_root, count.plan.buckets)?;
    for i in 0..set.len() {
        spill.spill_segment(i, &mut count.plan)?;
    }
    let (spill_dirs, bucket_rows) = spill.finish()?;
    check_build_memory(&bucket_rows, options)?;
    let hnsw_sources = HnswCompactionSources::from_segment_set(slot_offset, set);
    let manifest = spill_manifest(rows, &shape)?;
    let cuts = CutBuild {
        manifest: &manifest,
        schema: super::child_build::ChildSchema {
            bm25_fields: Some(&shape.tables.fields),
            pinned_fingerprints: Some(&shape.tables.fingerprints),
            binding: Some(&binding),
            derived: shape.derived.as_ref(),
            columns: Some(&shape.tables.columns),
            hnsw_sources: hnsw_sources.as_ref(),
        },
        column: spec.column,
        out_dir,
        spill_dirs: &spill_dirs,
        bucket_rows: &bucket_rows,
    };
    let placed = if options.build_threads <= 1 {
        cuts.build_serial()?
    } else {
        cuts.build_threaded(options)?
    };
    let _ = std::fs::remove_dir_all(&spill_root);
    Ok(SegmentCompactionBuild {
        images: placed.images,
        id_map: placed.id_map,
        rows_before: count.rows_before,
        tombstones: count.tombstones,
        binding: Some(binding),
    })
}

/// The refusals that need nothing read: an empty catalog, one without a
/// generation binding (returned when present), a zero bound or queue.
fn check_request(
    set: &crate::segments::OpenedSegmentSet,
    spec: PartitionSpec<'_>,
    options: &SegmentCompactionOptions,
) -> Result<crate::postings::StoredBinding, String> {
    if set.is_empty() {
        return Err(
            "a compaction from segments needs a catalog with at least one sealed \
                    segment; an empty or never-flushed catalog has no rows to transplant"
                .to_string(),
        );
    }
    let binding = set.binding().cloned().ok_or_else(|| {
        format!(
            "{}: the catalog carries no generation binding; a compaction from segments needs \
             the bound generation it rebuilds",
            set.root().display()
        )
    })?;
    if spec.bound == 0 {
        return Err("a partitioned compaction needs a positive row bound".to_string());
    }
    if options.build_queue == Some(0) {
        return Err("build queue 0 holds no finished cut; give it one or more".to_string());
    }
    Ok(binding)
}

/// The build-memory budget against the spill's own per-cut row counts:
/// the largest cut must fit alone (one thread still replays one cut),
/// and the thread count times it must fit together. A plan that cannot
/// fit is refused by name; nothing is lowered to fit.
fn check_build_memory(
    bucket_rows: &[u64],
    options: &SegmentCompactionOptions,
) -> Result<(), String> {
    let Some(budget) = options.build_memory else {
        return Ok(());
    };
    let threads = options.build_threads.max(1) as u64;
    let Some((bucket, &rows)) = bucket_rows
        .iter()
        .enumerate()
        .filter(|(_, &n)| n > 0)
        .max_by_key(|(_, &n)| n)
    else {
        return Ok(());
    };
    let estimate = rows
        .checked_mul(BUILD_BYTES_PER_ROW)
        .ok_or_else(|| "build memory arithmetic overflow estimating a cut".to_string())?;
    if estimate > budget {
        return Err(format!(
            "cut {bucket}: {rows} documents at {} KiB a row (the conservative end of the 40-70 \
             KB band) is about {} MiB, over the build-memory budget of {} MiB for a single cut; \
             raise the budget or cut finer",
            BUILD_BYTES_PER_ROW / 1024,
            estimate.div_ceil(1 << 20),
            budget / (1 << 20)
        ));
    }
    let need = estimate.checked_mul(threads).ok_or_else(|| {
        "build memory arithmetic overflow multiplying the cut estimate by build threads".to_string()
    })?;
    if need > budget {
        return Err(format!(
            "cut {bucket}: {rows} documents at {} KiB a row is about {} MiB, and {threads} build \
             threads need {} MiB at once, over the build-memory budget of {} MiB; lower the \
             thread count, raise the budget, or cut finer -- the thread count is not lowered \
             for you",
            BUILD_BYTES_PER_ROW / 1024,
            estimate.div_ceil(1 << 20),
            need.div_ceil(1 << 20),
            budget / (1 << 20)
        ));
    }
    Ok(())
}

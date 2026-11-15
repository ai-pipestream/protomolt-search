//! The positional entry points the crate's callers outside this module
//! still use, each a thin adapter onto its request-struct form.

use std::path::{Path, PathBuf};

use super::compact_log::{compact_log, CompactionBuild, CompactionRequest, RowSink};
use super::hash_split::{split_logs_with, split_stable_logs_with, SplitRequest};
use super::hnsw_sources::HnswCompactionSources;
use super::partitioned::compact_log_partitioned;
use super::placement_split::{
    split_placement_logs_with, PlacementChild, PlacementReshardOutput, PlacementSplitRequest,
};
use super::segmented_split::split_logs_segmented_with;
use super::{
    Analyzer, ColumnTables, PartitionSpec, ReshardOutput, SegmentedReshardOutput,
    StableReshardOutput,
};

/// [`split_logs_with`] over one generation.
#[allow(clippy::too_many_arguments)]
pub fn split(
    gen: &Path,
    n: usize,
    out_dir: &Path,
    slot_base: u64,
    slot_stride: u64,
    vectors_only: bool,
    bm25_fields: Option<&[String]>,
    analyze: &mut Analyzer,
) -> Result<ReshardOutput, String> {
    let gens = [gen.to_path_buf()];
    let request = SplitRequest {
        gens: &gens,
        n,
        out_dir,
        slot_base,
        slot_stride,
        vectors_only,
        bm25_fields,
    };
    split_logs_with(&request, analyze)
}

/// [`split_logs_with`] with positional arguments.
#[allow(clippy::too_many_arguments)]
pub fn split_logs(
    gens: &[PathBuf],
    n: usize,
    out_dir: &Path,
    slot_base: u64,
    slot_stride: u64,
    vectors_only: bool,
    bm25_fields: Option<&[String]>,
    analyze: &mut Analyzer,
) -> Result<ReshardOutput, String> {
    let request = SplitRequest {
        gens,
        n,
        out_dir,
        slot_base,
        slot_stride,
        vectors_only,
        bm25_fields,
    };
    split_logs_with(&request, analyze)
}

/// [`split_logs_segmented_with`] with positional arguments.
#[allow(clippy::too_many_arguments)]
pub fn split_logs_segmented(
    gens: &[PathBuf],
    n: usize,
    out_dir: &Path,
    slot_base: u64,
    slot_stride: u64,
    vectors_only: bool,
    bm25_fields: Option<&[String]>,
    analyze: &mut Analyzer,
) -> Result<SegmentedReshardOutput, String> {
    let request = SplitRequest {
        gens,
        n,
        out_dir,
        slot_base,
        slot_stride,
        vectors_only,
        bm25_fields,
    };
    split_logs_segmented_with(&request, analyze)
}

/// [`split_stable_logs_with`] with positional arguments.
#[allow(clippy::too_many_arguments)]
pub fn split_stable_logs(
    gens: &[PathBuf],
    n: usize,
    out_dir: &Path,
    slot_base: u64,
    slot_stride: u64,
    vectors_only: bool,
    bm25_fields: Option<&[String]>,
    analyze: &mut Analyzer,
) -> Result<StableReshardOutput, String> {
    let request = SplitRequest {
        gens,
        n,
        out_dir,
        slot_base,
        slot_stride,
        vectors_only,
        bm25_fields,
    };
    split_stable_logs_with(&request, analyze)
}

/// [`split_placement_logs_with`] with positional arguments.
#[allow(clippy::too_many_arguments)]
pub fn split_placement_logs(
    gens: &[PathBuf],
    column: &str,
    children: &[PlacementChild],
    default_child: Option<usize>,
    out_dir: &Path,
    slot_offsets: &[u64],
    vectors_only: bool,
    bm25_fields: Option<&[String]>,
    analyze: &mut Analyzer,
) -> Result<PlacementReshardOutput, String> {
    let request = PlacementSplitRequest {
        gens,
        column,
        children,
        default_child,
        out_dir,
        slot_offsets,
        vectors_only,
        bm25_fields,
    };
    split_placement_logs_with(&request, analyze)
}

/// The crate's compaction entry with the sources' HNSW graphs; the
/// shadow compaction calls it with the shard's tables.
#[allow(clippy::too_many_arguments)]
pub(crate) fn compact_log_with_sources(
    gen: &Path,
    cutoff_clock: u64,
    out_dir: &Path,
    segmented: bool,
    bm25_fields: Option<&[String]>,
    pinned_fingerprints: Option<&[u64]>,
    columns: Option<&ColumnTables>,
    derived: Option<&crate::postings::StoredDerived>,
    hnsw_sources: Option<&HnswCompactionSources>,
    analyze: &mut Analyzer,
    sink: &mut RowSink,
) -> Result<CompactionBuild, String> {
    let request = CompactionRequest {
        bm25_fields,
        pinned_fingerprints,
        columns,
        derived,
        hnsw_sources,
        ..CompactionRequest::new(gen, cutoff_clock, out_dir)
    };
    compact_log(&request, segmented, analyze, sink)
}

/// The crate's partitioned compaction entry with the sources' HNSW
/// graphs; the shadow compaction and the segment catalog call it.
#[allow(clippy::too_many_arguments)]
pub(crate) fn compact_log_partitioned_with_sources(
    gen: &Path,
    cutoff_clock: u64,
    out_dir: &Path,
    spec: PartitionSpec<'_>,
    bm25_fields: Option<&[String]>,
    pinned_fingerprints: Option<&[u64]>,
    columns: Option<&ColumnTables>,
    derived: Option<&crate::postings::StoredDerived>,
    hnsw_sources: Option<&HnswCompactionSources>,
    analyze: &mut Analyzer,
    sink: &mut RowSink,
) -> Result<CompactionBuild, String> {
    let request = CompactionRequest {
        bm25_fields,
        pinned_fingerprints,
        columns,
        derived,
        hnsw_sources,
        ..CompactionRequest::new(gen, cutoff_clock, out_dir)
    };
    compact_log_partitioned(&request, spec, analyze, sink)
}

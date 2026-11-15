//! Offline resharding: replay a shard's WAL into N child images (split) or
//! several shards' WALs into one image (merge). This is the library half of
//! `examples/reshard.rs`; the integration test drives the same code.
//!
//! The WAL is pre-partitioned: every record was routed to
//! `bucket_of(id, bucket_count)` at write time, so a split with
//! `N <= bucket_count` (and `bucket_count % N == 0`) hands each child a
//! contiguous range of bucket files and never re-hashes a record. A
//! finer split (`N > bucket_count`) still works but re-partitions every
//! record — the bucket count caps CHEAP split granularity.
//!
//! Invariants (enforced here, documented in the README):
//!
//! - ONE VECTOR CONFIGURATION per split/merge: the WAL manifest must carry
//!   locked provider state, and merge requires byte-identical backend
//!   configurations AND identical bucket counts across all
//!   inputs. Provider state that cannot reproduce byte-comparable scores
//!   cannot be resharded. This is a hard error, as is installing an image
//!   whose scoring fingerprint differs from the active generation.
//! - Ids are GENERATION-SCOPED: records carry the global ids the server
//!   assigned; children re-assign dense local slots in original id order
//!   and get their slot base from the new shard map, so parent ids never
//!   leak into a child by accident. [`ChildImage::parent_ids`] is the
//!   explicit remap (child local slot -> parent global id).
//! - The split partition is hash-based and stable: id `v` goes to bucket
//!   `fnv1a64(v) >> (64 - log2(N))`, so every id lands in exactly one
//!   child and a re-split of a child bisects its range again.

mod analysis_sidecar;
mod child_build;
mod compact_log;
mod hash_split;
mod hnsw_sources;
mod merge_inputs;
mod partitioned;
mod placement_split;
mod replay;
mod segment_compaction;
mod segment_sources;
mod source_agreement;
mod tree_logs;
mod tree_split;

pub use self::child_build::manifest_derived;
pub(crate) use self::compact_log::compact_log_with_sources;
pub use self::compact_log::{compact_log, CompactedRow, CompactionBuild, RowSink};
pub use self::hash_split::{
    merge, split, split_logs, split_logs_segmented, split_stable_logs, split_stable_logs_ranged,
};
pub(crate) use self::hnsw_sources::HnswCompactionSources;
pub use self::partitioned::compact_log_partitioned;
pub(crate) use self::partitioned::compact_log_partitioned_with_sources;
pub use self::placement_split::{
    placement_shard_map_toml, split_placement_logs, validate_placement_children, PlacementChild,
    PlacementReshardOutput,
};
pub use self::replay::{read_generation_binding, resolve_gen};
pub use self::segment_compaction::{
    compact_segments_partitioned, SegmentCompactionBuild, SegmentCompactionOptions,
};
pub(crate) use self::segment_sources::{
    open_segment_exact, open_segment_sources, reconstruct_document, segment_tables,
    SegmentSourceShard, SourceTables,
};
pub use self::tree_logs::split_placement_tree_logs;
pub use self::tree_split::{
    tree_children, tree_shard_map_toml, SpillCut, TreeChild, TreeChildLayout, TreeReshardOutput,
    TreeRowSource, TreeSplitOptions, BUILD_BYTES_PER_ROW,
};

use std::path::PathBuf;

use crate::analyzer::SessionLayers;
use crate::pb::AnalysisSpec;
use crate::postings::AnalyzedDoc;

/// Text analyzer for rebuilding BM25 stores: a batch of (raw text, the
/// analysis spec it was ingested with) -> analyzed terms, one result
/// per input in order. The example wires this to the analysis sidecar
/// (the same one ingest used) with the batch's requests in flight
/// concurrently — replay throughput is bounded by sidecar round-trips,
/// exactly like ingest. Tests wire it to the mock sidecar. Split/merge
/// only re-partitions — term identity must be reproduced EXACTLY as at
/// ingest, hence the spec round-trip. Multi-field documents
/// (docs/multi-field.md) flatten into the batch as one entry per field
/// (body first, extras in record order); the caller reassembles.
pub type Analyzer<'a> = dyn FnMut(&[(&str, Option<&AnalysisSpec>, SessionLayers)]) -> Result<Vec<AnalyzedDoc>, String>
    + 'a;

/// Batch size handed to the analyzer (the analyzer's concurrency window).
const ANALYZE_BATCH: usize = 32;

/// FNV-1a 64 over the 8 little-endian bytes of an id (hand-rolled; no
/// dependency). Stable across platforms and runs — it is the split
/// partition function (and the WAL routing function), so it must never
/// change.
pub fn fnv1a64(id: u64) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in id.to_le_bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// The bucket (child index) of a vector or document id for an N-way
/// partition. `n` must be a power of two; the top log2(n) bits of the
/// id's hash select the bucket. This is ALSO the WAL routing function:
/// bucket file `bucket-NNN.wal` holds exactly the records with
/// `bucket_of(id, bucket_count) == NNN`.
pub fn bucket_of(id: u64, n: usize) -> usize {
    assert!(n.is_power_of_two(), "split factor must be a power of two");
    // Top log2(1) = 0 bits select bucket 0. Spelled out because the
    // general expression would shift by 64, which panics in debug
    // builds and wraps to a shift of 0 in release: every id would
    // route to its own hash-numbered bucket, silently.
    if n == 1 {
        return 0;
    }
    let shift = 64 - n.trailing_zeros();
    (fnv1a64(id) >> shift) as usize
}

/// One built shard image: the opaque vector index, its optional `.bm25` sidecar,
/// and the shard-map metadata the coordinator needs to route to it.
#[derive(Debug)]
pub struct ChildImage {
    /// The written index file.
    pub vector_path: PathBuf,
    /// Product-owned original FP32 rows aligned with `vector_path`.
    pub exact_vector_path: PathBuf,
    /// Materialized multi-resolution rows and their authoritative declaration.
    pub multi_res_path: Option<PathBuf>,
    /// The written BM25 sidecar (absent when the child received no
    /// documents).
    pub bm25_path: Option<PathBuf>,
    /// Global id base for this child in the NEW generation.
    pub slot_offset: u64,
    /// Inclusive hash-range bounds this child covers (see [`bucket_of`]).
    pub hash_lo: u64,
    pub hash_hi: u64,
    pub num_vectors: u64,
    pub num_documents: u64,
    /// Parent global id per child local vector slot, ascending (the id
    /// remap).
    pub parent_ids: Vec<u64>,
    /// Parent global id per child local ROW: `parent_ids` followed by the
    /// document-only rows (documents whose id named no vector), in
    /// parent-id order — the complete old-to-new id map of the child,
    /// which compaction extends as the tail applies.
    pub row_parent_ids: Vec<u64>,
}

/// The result of one split or merge: the images plus the NEW generation
/// number (one past the highest input generation) for the shard map.
#[derive(Debug)]
pub struct ReshardOutput {
    pub generation: u64,
    pub children: Vec<ChildImage>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalCutoff {
    pub generation: u64,
    pub high_watermark: u64,
}

#[derive(Debug)]
pub struct StableReshardOutput {
    pub images: ReshardOutput,
    /// Durable source prefixes included in `images`, parallel to the input
    /// generation list. Child catch-up resumes strictly after these clocks.
    pub source_cutoffs: Vec<WalCutoff>,
}

/// One immutable segment emitted by bucket-bounded resharding. Several
/// segments may belong to one logical hash shard; [`crate::segments`] opens
/// and searches them as one snapshot with global BM25 statistics.
#[derive(Debug)]
pub struct SegmentedChildImage {
    pub logical_shard: usize,
    pub segment_ordinal: usize,
    pub physical_rows: u64,
    pub image: ChildImage,
}

/// Bucket-bounded reshard result. Memory holds at most one WAL bucket's live
/// replay plus one segment build, never a whole child or corpus.
#[derive(Debug)]
pub struct SegmentedReshardOutput {
    pub generation: u64,
    pub logical_shards: usize,
    pub segments: Vec<SegmentedChildImage>,
    pub peak_replay_rows: u64,
}

impl SegmentedReshardOutput {
    /// The images of one logical shard as a segmented output, in order.
    pub fn from_images(generation: u64, images: Vec<ChildImage>) -> Self {
        let peak_replay_rows = images
            .iter()
            .map(|image| image.num_vectors.max(image.num_documents))
            .max()
            .unwrap_or(0);
        Self {
            generation,
            logical_shards: 1,
            segments: images
                .into_iter()
                .enumerate()
                .map(|(ordinal, image)| SegmentedChildImage {
                    logical_shard: 0,
                    segment_ordinal: ordinal,
                    physical_rows: image.num_vectors.max(image.num_documents),
                    image,
                })
                .collect(),
            peak_replay_rows,
        }
    }
}

/// The layout a partitioned compaction builds (docs/immutable-segments.md
/// "Partitioned layout"): live rows ordered by an integer column and cut
/// into segments of at most `bound` rows.
#[derive(Debug, Clone, Copy)]
pub struct PartitionSpec<'a> {
    /// An integer column of the shard (timestamps included).
    pub column: &'a str,
    /// The most rows one output segment may hold.
    pub bound: usize,
}

/// The column tables a compacted image must declare, in the shard's
/// order: the live shard's own, so an output that holds no record of a
/// column still declares it and opens under the same node configuration
/// (a segment's tables must equal the tail's). Without them an image
/// derives its tables from the records it holds.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ColumnTables {
    pub facets: Vec<String>,
    pub numerics: Vec<String>,
    pub map_facets: Vec<String>,
    pub map_numerics: Vec<String>,
    pub map_integers: Vec<String>,
    pub map_unsigned_integers: Vec<String>,
    pub integers: Vec<String>,
    pub unsigned_integers: Vec<String>,
    pub geo: Vec<String>,
}

/// Render the coordinator shard map for a reshard result (see
/// `config::ShardMap`). `addr` is a TODO placeholder — the operator fills
/// in where each child will listen.
pub fn shard_map_toml(out: &ReshardOutput) -> String {
    let mut s = format!("generation = {}\n", out.generation);
    for child in &out.children {
        s.push_str(&format!(
            "\n[[shards]]\naddr = \"TODO\"\nslot_offset = {}\nhash_lo = {}\nhash_hi = {}\n",
            child.slot_offset, child.hash_lo, child.hash_hi
        ));
    }
    s
}

/// Render the node-side `[[shards]]` config blocks for a reshard result
/// (same print idiom as `harness::write_shards`).
pub fn shards_toml(out: &ReshardOutput) -> String {
    let mut s = String::new();
    for child in &out.children {
        s.push_str(&format!(
            "[[shards]]\nlisten = \"0.0.0.0:50051\"  # TODO: one listener per child\nindex = \
             \"{}\"\nslot_offset = {}\n",
            child.vector_path.display(),
            child.slot_offset
        ));
    }
    s
}

/// The process's soft open-files limit, from `/proc/self/limits` on
/// Linux; `None` where that file is absent or unreadable.
pub fn open_files_soft_limit() -> Option<u64> {
    let text = std::fs::read_to_string("/proc/self/limits").ok()?;
    let line = text
        .lines()
        .find(|line| line.starts_with("Max open files"))?;
    let mut fields = line["Max open files".len()..].split_whitespace();
    let soft = fields.next()?;
    if soft == "unlimited" {
        return Some(u64::MAX);
    }
    soft.parse().ok()
}

/// Raise the soft open-files limit to the hard limit, so a spill plan
/// sized for the machine is not stopped by the shell's default of
/// 1024. Returns the limit in force afterwards; `None` when the build
/// cannot set limits (no `net` feature) or the call failed.
#[cfg(feature = "net")]
pub fn raise_open_files_limit() -> Option<u64> {
    // SAFETY: getrlimit/setrlimit read and write a plain struct the
    // caller owns; no memory is shared past the calls.
    unsafe {
        let mut limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) != 0 {
            return None;
        }
        if limit.rlim_cur < limit.rlim_max {
            limit.rlim_cur = limit.rlim_max;
            if libc::setrlimit(libc::RLIMIT_NOFILE, &limit) != 0 {
                return None;
            }
        }
        // rlim_t is u64 on the fleet's targets and wider elsewhere.
        #[allow(clippy::unnecessary_cast)]
        Some(limit.rlim_cur as u64)
    }
}

#[cfg(not(feature = "net"))]
pub fn raise_open_files_limit() -> Option<u64> {
    None
}

#[cfg(test)]
mod retained_vector_tests;

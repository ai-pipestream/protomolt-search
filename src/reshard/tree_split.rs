//! Placement-tree split types, options, and the shard-map rendering of a tree.

use std::collections::{BTreeMap, BTreeSet};

use super::replay::Replay;
use super::{ChildImage, ReshardOutput};

/// One child of a re-placement split (`docs/placement.md`, "Changing
/// the tree"): the leaf it serves under the NEW tree and the stable-key
/// hash range it takes inside that leaf (the full range on a one-shard
/// leaf).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeChild {
    /// Dotted leaf name under the new tree.
    pub leaf: String,
    /// The leaf's path code, which the child's rows carry after the
    /// split and the shard map names as `placement`.
    pub code: i64,
    pub hash_lo: u64,
    pub hash_hi: u64,
}

/// Built bucket images by plan position, handed from the sealing
/// threads to the appending one.
pub(super) type BuiltBuckets = BTreeMap<usize, Result<(ChildImage, u64), String>>;

/// The bytes a spilled row costs the child build while its bucket is
/// replayed: the conservative end of the documented 40-70 KB band
/// (docs/replay-from-segments.md): the document text, the transplanted
/// spans and ordinals, the vector and the FP32 row. The build-memory
/// budget is checked against it.
pub const BUILD_BYTES_PER_ROW: u64 = 70 * 1024;

/// The result of a re-placement split: the images, the child list
/// (parallel to `images.children`), and what moved.
#[derive(Debug)]
pub struct TreeReshardOutput {
    /// One entry per child. Under [`TreeChildLayout::Segmented`] a child's
    /// `vector_path` is the index path a node serves (`<out>/shard-<i>.tv`,
    /// its catalog beside it under `.segments`), `bm25_path` is `None`, and
    /// the id maps are empty: the segments' own metadata carry the rows.
    /// Under [`TreeChildLayout::SingleImage`] the entries are the images
    /// as any split writes them.
    pub images: ReshardOutput,
    pub children: Vec<TreeChild>,
    /// Live documents whose code under the new tree differs from the
    /// code they carried (or that carried none).
    pub moved: u64,
    /// Live documents per child.
    pub placed: Vec<u64>,
    /// How the children were written.
    pub layout: TreeChildLayout,
    /// Sealed segments per child (1 per child under a single image).
    pub segments: Vec<usize>,
    /// The most live source rows any one replay held: one spill bucket
    /// under the segmented layout, one child under a single image.
    pub peak_replay_rows: u64,
    /// The bucket count the spill logs were written with.
    pub spill_bucket_count: u32,
    /// Where the documents' analyzed fields came from.
    pub source: TreeRowSource,
    /// How the spill was cut.
    pub cut: SpillCut,
    /// The largest transpose held at once, in bytes (0 under the log
    /// replay): one field of one source segment at a time is the bound.
    pub peak_transpose_bytes: u64,
    /// The derived-column declaration the children were written under
    /// (`docs/derived-columns.md`), as the shard map's `[[derived]]`
    /// table; empty without one.
    pub derived: Vec<crate::derived::DerivedColumnConfig>,
    /// Documents whose fields were transplanted (0 under the log replay).
    pub transplanted_rows: u64,
}

/// How a re-placement split writes each child.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TreeChildLayout {
    /// A segment catalog per child (`<out>/shard-<i>.tv.segments`,
    /// `docs/immutable-segments.md`): the child's spill log replays one
    /// WAL bucket at a time and each non-empty bucket seals as one
    /// segment, so memory is one bucket's rows plus one segment build,
    /// and a node starts on the child with `--index=<out>/shard-<i>.tv`.
    Segmented,
    /// One image per child, the way the other splits write: the child's
    /// rows are held together while the image is built. The caller states
    /// the most rows a child may have; a larger child is refused by name
    /// before any image is written.
    SingleImage { max_child_rows: u64 },
}

/// The knobs of a re-placement split.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeSplitOptions {
    pub layout: TreeChildLayout,
    /// log2 of the spill logs' bucket count; `None` takes the largest
    /// bucket count among the inputs, so a replay of one spill bucket is
    /// bounded the way a replay of one source bucket is. Ignored under
    /// a column cut, whose bucket count the cut plan sets.
    pub spill_bucket_bits: Option<u32>,
    /// Where each document's analyzed fields come from
    /// (`docs/replay-from-segments.md`).
    pub source: TreeRowSource,
    /// How each child's spill is cut into buckets, and so into segments.
    pub cut: SpillCut,
    /// Build only this child (by index in leaf order) and leave the
    /// others' catalogs unwritten: the routing pass still spills every
    /// child, so the rows and the map are the full split's, but a
    /// child whose build failed (an out-of-memory kill on the archive,
    /// 2026-09-06) is redone without the five that finished. Segmented
    /// layout only; a single-image split is refused by name.
    pub only_child: Option<usize>,
    /// The open-files limit the spill pass is checked against; `None`
    /// reads the process's own soft limit. A test sets a small number
    /// to see the refusal.
    pub open_files_limit: Option<u64>,
    /// The derived-column declaration the children are written under
    /// (`--derived-columns`, docs/derived-columns.md); `None` keeps the
    /// sources' own, from the log's manifest. With a declaration the
    /// sources' column tables must declare every input.
    pub derived: Option<std::sync::Arc<crate::derived::Declaration>>,
    /// Derived columns to compute on every row (`--derive=<name>`): a
    /// column added to the declaration, or one whose expression changed.
    /// Rows already derived under `derived` refuse; rows never derived
    /// need every declared column named (`Declaration::rederive`).
    pub derive: Vec<String>,
    /// Spill buckets built at once in a segmented child under
    /// `--from-segments` (`--build-threads`): each thread replays one
    /// bucket into memory and seals it, so the build's memory is this
    /// many buckets' replays; the segments append in bucket order and
    /// the catalog is the same as the one-thread build's, byte for
    /// byte. The log replay analyzes through one sidecar session and
    /// refuses more than one.
    pub build_threads: usize,
    /// Sealed-but-unappended bucket images a threaded build holds at
    /// once (`--build-queue`): a worker waits for backlog space before
    /// claiming its next bucket, so the build holds the threads'
    /// in-flight replays plus a backlog of at most this many finished
    /// images' id maps (plus one per thread when all finish at once).
    /// `None` takes the thread count; zero is refused.
    pub build_queue: Option<usize>,
    /// A fixed 70 KiB/row planning admission estimate in bytes
    /// (`--build-memory`, segmented layout only): after the routing
    /// pass, each child's spill counts at [`BUILD_BYTES_PER_ROW`] a row
    /// must fit it — the largest bucket alone, and the thread count
    /// times it. A plan that cannot fit is refused by name; neither
    /// the thread count nor the queue depth is lowered to make it fit.
    /// This is not an actual process-memory limit: wide values, queued id maps,
    /// and page cache are outside the estimate. `None` enforces no admission check.
    pub build_memory: Option<u64>,
}

impl Default for TreeSplitOptions {
    fn default() -> Self {
        Self {
            layout: TreeChildLayout::Segmented,
            spill_bucket_bits: None,
            source: TreeRowSource::Logs,
            cut: SpillCut::Hash,
            only_child: None,
            open_files_limit: None,
            derived: None,
            derive: Vec::new(),
            build_threads: 1,
            build_queue: None,
            build_memory: None,
        }
    }
}

/// The children a tree implies: `shards` per leaf, in leaf (code) order,
/// each leaf's shards tiling the stable-key hash space the way the
/// coordinator routes inside a leaf (`route_stable_key_in`). A leaf's
/// shard count must be a power of two for the tiles to be exact.
pub fn tree_children(placement: &crate::placement::Placement) -> Result<Vec<TreeChild>, String> {
    let mut out = Vec::new();
    for leaf in placement.leaves() {
        let n = leaf.shards.max(1) as usize;
        if !n.is_power_of_two() {
            return Err(format!(
                "placement leaf {:?} has {n} shards; a re-placement split tiles a leaf's hash \
                 space in powers of two",
                leaf.name
            ));
        }
        let shift = if n == 1 { 0 } else { 64 - n.trailing_zeros() };
        for shard in 0..n {
            let hash_lo = if n == 1 { 0 } else { (shard as u64) << shift };
            let hash_hi = if shard + 1 == n {
                u64::MAX
            } else {
                ((shard as u64 + 1) << shift) - 1
            };
            out.push(TreeChild {
                leaf: leaf.name.clone(),
                code: leaf.code,
                hash_lo,
                hash_hi,
            });
        }
    }
    Ok(out)
}

/// The live source rows a replay holds (vectors and documents by id).
pub(super) fn live_rows(replay: &Replay) -> u64 {
    replay
        .vectors
        .keys()
        .chain(replay.documents.keys())
        .collect::<BTreeSet<_>>()
        .len() as u64
}

/// The slots an image takes.
pub(super) fn image_rows(image: &ChildImage) -> u64 {
    image.num_vectors.max(image.num_documents)
}

/// A child's slots must end at or before the next child's offset.
pub(super) fn check_slot_bound(
    index: usize,
    leaf: &str,
    base: u64,
    rows: u64,
    bound: Option<u64>,
) -> Result<(), String> {
    let end = base
        .checked_add(rows)
        .ok_or_else(|| "re-placement split slot overflow".to_string())?;
    if let Some(bound) = bound {
        if end > bound {
            return Err(format!(
                "child {index} ({leaf}) needs slots through {end}, past the next child's offset \
                 {bound}; widen the slot stride"
            ));
        }
    }
    Ok(())
}

/// The shard map for a re-placement split: the new tree under
/// `[placement]`, one `[[shards]]` per child with its code and hash
/// range, addresses left for the operator.
pub fn tree_shard_map_toml(
    out: &TreeReshardOutput,
    tree: &crate::placement::PlacementTreeConfig,
) -> Result<String, String> {
    let map = crate::config::ShardMap {
        generation: out.images.generation,
        shards: out
            .images
            .children
            .iter()
            .zip(&out.children)
            .map(|(image, child)| crate::config::ShardMapShard {
                addr: "TODO".to_string(),
                replica: None,
                slot_offset: image.slot_offset,
                hash_lo: Some(image.hash_lo),
                hash_hi: Some(image.hash_hi),
                placement: Some(child.code as u64),
            })
            .collect(),
        placement: Some(tree.clone()),
        derived: out.derived.clone(),
    };
    toml::to_string(&map).map_err(|error| format!("render shard map: {error}"))
}

// ---------------------------------------------------------------------
// Replay from segments (docs/replay-from-segments.md)
// ---------------------------------------------------------------------

/// Where a re-placement split takes each document's analyzed fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TreeRowSource {
    /// Replay the logs and analyze each document's text again.
    #[default]
    Logs,
    /// Read the sealed segments beside each log: the postings transposed
    /// per document, the columns, text, vectors and identities copied,
    /// the analyzer not called.
    Segments,
}

/// How a child's spill is cut into buckets, and so into segments.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum SpillCut {
    /// By id hash, one bucket per source bucket: each segment then
    /// spans the column's range and a partitioned compaction follows.
    #[default]
    Hash,
    /// By the integer column's value in ascending order, at most
    /// `rows_per_cut` rows per cut, the rows without the column last:
    /// the child comes out partitioned by the column.
    Column { column: String, rows_per_cut: u64 },
}

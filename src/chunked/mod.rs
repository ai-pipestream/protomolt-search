//! Chunked shard scan with mid-scan floor adoption.
//!
//! A provider's bounded search is a single synchronous scan whose floor
//! is fixed at call time, so reactive floor sharing is built *around* it:
//! the shard is scanned in chunks of `chunk_blocks` SIMD blocks, each chunk
//! a masked `search_with_options` call (an allowlist over a contiguous slot
//! range) seeded with the best floor known at that moment. Between chunks
//! the scan polls `external_floor` for coordinator-pushed floors and reports
//! its own k-th best through `publish_floor` once its heap is full.
//!
//! Losslessness: the union of the masked chunk ranges is the whole shard,
//! and every floor ever applied is a lower bound on the shard's final k-th
//! best — the local heap floor because it can only rise toward the true
//! k-th best, a coordinator floor because any shard's k-th best lower-bounds
//! the global k-th best, which lower-bounds... which is itself bounded below
//! by every shard's k-th best. turbovec keeps candidates scoring exactly at
//! the floor, so boundary ties survive. The merged heap therefore ends with
//! exactly the shard's unchunked top-k.

use crate::vector::{Mask, VectorCapability, VectorIndex, VectorSearchOptions};

/// Vectors per block in the current embedded scan provider. Chunk ranges are
/// block-aligned so a chunk is always a whole number of kernel blocks.
pub const BLOCK_VECTORS: usize = 32;

/// Default chunk size in SIMD blocks (64 blocks = 2048 vectors per chunk).
pub const DEFAULT_CHUNK_BLOCKS: usize = 64;

/// One candidate collected during a chunked scan.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ChunkHit {
    /// Local slot within the shard index.
    pub slot: u32,
    /// turbovec score; higher is better.
    pub score: f32,
}

/// Instrumentation for one chunked scan.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ScanStats {
    /// Per-chunk `search_with_options` calls made.
    pub chunk_calls: u32,
    /// Real (non-sentinel) candidates collected across all chunks. This is
    /// exactly the set of vectors that survived every floor in effect when
    /// their chunk ran — the metric that shows floor sharing saving work.
    pub candidates_collected: u64,
    /// Floors OFFERED to `publish_floor`: one per chunk whose heap held
    /// `k`. This is the scan's own behaviour and does not depend on what
    /// the caller does with them.
    pub floors_offered: u64,
    /// Floors the caller actually put on the wire, which is what
    /// `--floor-warmup-chunks` and `--floor-min-interval-ms` move. It was
    /// once the same counter as `floors_offered`, so a knob that
    /// suppressed nine tenths of the broadcasts read as having done
    /// nothing at all.
    pub floors_published: u64,
    /// Chunks that ran with an external (coordinator-pushed) floor in effect.
    pub floor_updates_applied: u64,
    /// Sealed segments in the shard's snapshot, and how many the
    /// request's filter ruled out before the scan opened them. Set by the node after the scan;
    /// the kernel does not know about segments.
    pub segments_total: u32,
    pub segments_skipped: u32,
    /// Encoded index bytes the kernel streamed for this query: the rows
    /// of every chunk it scanned (whole chunk unfiltered; the blocks the
    /// allowlist left non-empty when filtered) times [`encoded_row_bytes`].
    /// A batched query is charged the pass it rode, so summing this over
    /// the queries of one batch counts the pass once per query; the
    /// process-wide counters take the pass once through the observer.
    pub bytes_scanned: u64,
    /// Wall time inside the kernel calls this query rode, in nanoseconds.
    pub scan_nanos: u64,
    /// How a provider that steers its search by the filter answered this
    /// query; all zero on a scan provider.
    pub regimes: crate::vector::FilterRegimeCounts,
    /// Rows scored by enumeration across unlinked HNSW parts (tail, frozen
    /// tail, or sealed segments below `MIN_GRAPH_ROWS`); zero on a scan
    /// provider.
    pub tail_enumerated_rows: u64,
    /// Layer-0 graph walks that ended early via patience saturation;
    /// zero on a scan provider.
    pub patience_stopped: u32,
    /// Layer-0 graph walks that pruned candidates against the active score
    /// floor; zero on a scan provider.
    pub floor_pruned: u32,
}

/// One kernel call's cost: encoded bytes streamed and wall time inside
/// the call. Reported once per call, however many queries rode it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ScanPass {
    pub bytes: u64,
    pub nanos: u64,
}

/// Bytes of the image the kernel reads per row: the packed codes
/// (`dim / (8 / bits)` bytes, the format's own integer geometry) plus the
/// row's 4-byte scale. The id column is not read by the scan and is not
/// counted; neither are calibration tables, headers, or the FP32 sidecar.
/// Zero for a lazy index whose geometry is not committed yet, which
/// counts nothing rather than a guess.
pub fn encoded_row_bytes(dim: usize, bits_per_dimension: usize) -> u64 {
    if dim == 0 || bits_per_dimension == 0 || bits_per_dimension > 8 {
        return 0;
    }
    let codes_per_byte = 8 / bits_per_dimension;
    (dim / codes_per_byte) as u64 + 4
}

/// [`encoded_row_bytes`] of an index from its own geometry.
pub fn index_row_bytes(index: &VectorIndex) -> u64 {
    match (index.dim_opt(), index.bits_per_dimension()) {
        (Some(dim), Some(bits)) => encoded_row_bytes(dim, bits),
        _ => 0,
    }
}

/// Rows a masked chunk streams: the kernel skips a block whose 32 slots
/// are all masked out, so the count is the rows of the blocks that keep
/// at least one allowed slot, clipped to the chunk.
fn masked_rows(allow: &[bool], start: usize, end: usize) -> usize {
    let mut rows = 0;
    let mut block_start = start;
    while block_start < end {
        let block_end = (block_start + BLOCK_VECTORS).min(end);
        if allow[block_start..block_end].iter().any(|&ok| ok) {
            rows += block_end - block_start;
        }
        block_start = block_end;
    }
    rows
}

/// `true` when `a` ranks ahead of `b` in top-k order: score descending,
/// ties broken by ascending slot (deterministic, matches the coordinator's
/// merge tie-break).
fn ranks_before(a: ChunkHit, b: ChunkHit) -> bool {
    a.score > b.score || (a.score == b.score && a.slot < b.slot)
}

/// Merge one chunk's hits into the running top-k `heap` (score desc, slot
/// asc, capped at `k` — or at `k` plus the boundary tie group when
/// `keep_ties` is set).
///
/// turbovec returns each row score-descending, so instead of inserting hit
/// by hit — O(k) per insert, which hurts at k=10000 — this sorts the (small)
/// chunk batch into the heap's total order and does a linear two-pointer
/// merge: O(chunk log chunk + k) per chunk.
///
/// With `keep_ties`, entries beyond `k` are kept while their score equals
/// the current k-th best: the cascade fusion mode's cutoff is
/// score-defined, so every doc tied at the boundary must survive on every
/// shard. Eviction still happens whenever the k-th best score RISES (the
/// old boundary group is then provably below any future cutoff), so the
/// retention is bounded by k plus the largest tie group at the CURRENT
/// boundary — worst case a shard of identical scores, same as any
/// tie-complete contract.
fn merge_chunk_hits(heap: &mut Vec<ChunkHit>, chunk: &mut [ChunkHit], k: usize, keep_ties: bool) {
    if chunk.is_empty() {
        return;
    }
    chunk.sort_by(|a, b| {
        if ranks_before(*a, *b) {
            std::cmp::Ordering::Less
        } else if ranks_before(*b, *a) {
            std::cmp::Ordering::Greater
        } else {
            std::cmp::Ordering::Equal
        }
    });
    if heap.len() >= k {
        // The chunk's best hit cannot displace the heap's k-th. With
        // keep_ties, a hit TIED at the boundary must still merge in.
        let skip = if keep_ties {
            chunk[0].score < heap[k - 1].score
        } else {
            heap.len() == k && !ranks_before(chunk[0], heap[k - 1])
        };
        if skip {
            return;
        }
    }
    let mut merged = Vec::with_capacity(heap.len() + chunk.len());
    let (mut i, mut j) = (0, 0);
    while merged.len() < k && (i < heap.len() || j < chunk.len()) {
        let take_heap = match (heap.get(i), chunk.get(j)) {
            (Some(&h), Some(&c)) => ranks_before(h, c),
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (None, None) => break,
        };
        merged.push(if take_heap {
            i += 1;
            heap[i - 1]
        } else {
            j += 1;
            chunk[j - 1]
        });
    }
    if keep_ties && merged.len() == k {
        // Boundary tie group: every remaining entry scoring exactly the
        // k-th best rides along. Both sides are (score desc, slot asc), so
        // ties are contiguous.
        let boundary = merged[k - 1].score;
        while i < heap.len() && heap[i].score == boundary {
            merged.push(heap[i]);
            i += 1;
        }
        while j < chunk.len() && chunk[j].score == boundary {
            merged.push(chunk[j]);
            j += 1;
        }
        // Re-sort the tie tail into (score desc, slot asc) order: heap
        // ties and chunk ties interleave by slot.
        merged[k..].sort_by_key(|h| h.slot);
    }
    *heap = merged;
}

/// One query of a batched chunked scan.
#[derive(Debug, Clone, Copy)]
pub struct BatchQuery<'a> {
    /// The query vector, of the index's dimension.
    pub vector: &'a [f32],
    /// Result count for this query.
    pub k: usize,
    /// Tie-complete collection for this query (see [`chunked_topk`]).
    pub keep_ties: bool,
    /// This query's slot allowlist: `allow[slot]` is false for a
    /// document the request's filters removed, and the slice covers
    /// every slot of the index. `None` is an unfiltered query, which
    /// is not the same thing as an all-true allowlist: `None` batches
    /// with every other unfiltered query, an allowlist batches only
    /// with itself (see [`chunked_topk_batch`]). The chunk scan masks
    /// with a byte per row and converts a bitmap once per query; a
    /// provider searched whole takes the mask as it is.
    pub allow: Option<Mask<'a>>,
    /// The enumerate threshold for a provider that chooses a filter
    /// regime (`VectorSearchOptions::enumerate_max_rows`); the chunk scan
    /// has no regimes and ignores it.
    pub enumerate_max_rows: Option<usize>,
    /// Optional hop-to-hop beam saturation early-stop policy for layer-0
    /// walks (`VectorSearchOptions::patience`); ignored by chunk scans.
    pub patience: Option<crate::vector::PatienceSpec>,
    /// Optional shared-floor candidate pruning policy for layer-0 walks
    /// (`VectorSearchOptions::floor_prune`); ignored by chunk scans.
    pub floor_prune: Option<crate::vector::FloorPruneSpec>,
}

mod batch;
mod collapsed;
mod groups;
#[cfg(test)]
mod tests;

pub use batch::{chunked_topk, chunked_topk_batch, chunked_topk_batch_observed};
pub use collapsed::{chunked_topk_collapsed, CollapsedHit};

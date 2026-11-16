//! Donor-seeded compaction remap, Qdrant-style tombstone link healing,
//! greedy `K`-dominating join-set segment merge, and directed forward-BFS
//! layer-0 connectivity repair.
//!
//! When sealed HNSW segments compact into a dense output image (removing
//! tombstones, merging multiple segments, or both):
//!
//! 1. **Donor transplant & tombstone link healing:** The largest
//!    qualifying linked source segment becomes the donor. Surviving donor rows
//!    keep their levels and surviving edges under the output's dense `0..N`
//!    node numbering. Any adjacency list that held a deleted neighbor is
//!    repaired locally by gathering surviving 1-hop neighbors plus surviving
//!    2-hop bridge neighbors across deleted nodes, scoring them in bulk, and
//!    pruning with the direction heuristic (`View::select`).
//! 2. **Greedy `K`-dominating join-set merge:** Each non-donor linked
//!    source segment selects a join set consisting of all upper-layer nodes
//!    (`level >= 1`) plus a greedy `K`-dominating layer-0 cover
//!    (`JOIN_DOMINATION_K = 2`, ordered by surviving degree descending). Only
//!    the join set (and any unlinked rows from tiny segments or the tail) runs
//!    full HNSW beam insertion (`LinkBuilder::insert`). Every remaining layer-0
//!    node in the segment runs a bounded local layer-0 beam walk (`ef_graft`,
//!    at most `GRAFT_MAX_EXPANSIONS` hops, zero upper-layer descent) seeded from
//!    its surviving intra-segment neighborhood plus the merged layer-0
//!    neighborhoods of its join-set neighbors, runs `select`, and wires
//!    reciprocal back-links (`connect_back`).
//! 3. **Directed forward-BFS connectivity check & repair:** A linear
//!    `O(V + E)` forward BFS from `entry` on layer 0 verifies that every node
//!    is reachable via directed outgoing edges, and bridges any stranded
//!    component into the main component while preserving existing in-degrees.

mod assemble;
mod donor;
mod donor_heal;
mod graft;
mod islands;
mod join_set;

use std::ops::ControlFlow;

use crate::vector::{Mask, VectorError, VectorProvider};

use super::config::HnswConfig;
pub(super) use super::repair::repair_connectivity;
use super::rows::RowSource;
use super::{Backing, HnswIndex, HNSW, MAX_DIM};

use assemble::merge_ordered_inner;
pub(super) use islands::{
    force_incoming_from_visited, try_connect_visited_to_island, unreachable_from_entry,
};

/// Number of join-set neighbors each non-join layer-0 node aims to be covered
/// by in [`MergeStrategy::JoinSet`].
pub const JOIN_DOMINATION_K: u8 = 2;

/// Graph construction / merge strategy when assembling a compacted [`HnswIndex`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MergeStrategy {
    /// Ignore source edges and construct a fresh HNSW graph over the output
    /// rows (`Store::build_links`).
    FullRebuild,
    /// Transplant and heal the largest qualifying donor segment, then insert
    /// every non-donor row via standard `LinkBuilder` beam search.
    DonorReinsert,
    /// Transplant and heal the largest qualifying donor segment, insert a
    /// greedy `K`-dominating join set per non-donor linked segment, graft the
    /// remaining layer-0 nodes from their 1-hop intra-segment + 2-hop join
    /// neighborhoods, and verify/repair directed layer-0 reachability.
    #[default]
    JoinSet,
}

/// Telemetry from [`HnswIndex::heal_tombstones`] and [`HnswIndex::merge_segments`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MergeStats {
    /// Total dense rows in the output index (`0..output_rows`).
    pub output_rows: usize,
    /// Index of the chosen donor segment among `sources`, if any qualified.
    pub donor_segment: Option<usize>,
    /// Surviving rows transplanted directly from the donor segment.
    pub donor_rows: usize,
    /// Donor adjacency lists repaired because at least one neighbor was deleted.
    pub donor_healed_lists: usize,
    /// Non-donor rows inserted into the merged graph via `LinkBuilder` as part
    /// of the upper-layer + `K`-dominating join set.
    pub join_set_rows: usize,
    /// Non-donor layer-0 rows grafted locally via join-set neighborhoods.
    pub grafted_rows: usize,
    /// Non-donor / unlinked rows inserted via full `LinkBuilder` search.
    pub reinserted_rows: usize,
    /// Disconnected layer-0 islands bridged into `entry`'s forward-reachable
    /// component during the post-merge connectivity pass.
    pub islands_repaired: usize,
    /// True when no qualifying linked donor segment contributed surviving rows,
    /// causing merge to fall back to a full rebuild over the output rows.
    pub used_no_donor_rebuild: bool,
}

/// One input segment and optional live-row mask for [`HnswIndex::merge_segments`].
#[derive(Clone, Copy)]
pub struct SegmentMergeInput<'a> {
    pub index: &'a HnswIndex,
    /// Live rows in `0..index.len()`; `None` treats every row in `index` as live.
    pub live: Option<Mask<'a>>,
}

/// Provenance of one dense output slot `0..N` in [`merge_ordered`].
#[derive(Clone, Copy)]
pub(crate) enum OrderedMergeRow<'a> {
    /// Surviving row `old_node` from `sources[segment]`, paired with its exact
    /// FP32 coordinates.
    FromSegment {
        segment: usize,
        old_node: u32,
        vector: &'a [f32],
    },
    /// Unlinked / tail / rebuilt row with no reusable source graph edges.
    Raw(&'a [f32]),
}

impl OrderedMergeRow<'_> {
    #[inline]
    fn vector(&self) -> &[f32] {
        match self {
            Self::FromSegment { vector, .. } => vector,
            Self::Raw(vector) => vector,
        }
    }
}

/// Compact a single [`HnswIndex`] under `live`, healing tombstoned links via
/// 2-hop surviving bridges and repairing any disconnected layer-0 islands.
/// Returns `(compacted_index, survivor_old_ids, stats)`.
pub(super) fn heal_tombstones(
    source: &HnswIndex,
    live: Mask<'_>,
) -> Result<(HnswIndex, Vec<u32>, MergeStats), VectorError> {
    if live.len() != source.len() {
        return Err(VectorError::new(format!(
            "live mask covers {} rows, the {HNSW} index holds {}",
            live.len(),
            source.len()
        )));
    }
    let mut survivor_old_ids = Vec::with_capacity(live.count());
    let _ = live.try_for_each(|row| {
        survivor_old_ids.push(row as u32);
        ControlFlow::<()>::Continue(())
    });
    let (mut merged, stats) = source.with_row_source(|rows_src| {
        let mut ordered = Vec::with_capacity(survivor_old_ids.len());
        for &old_node in &survivor_old_ids {
            ordered.push(OrderedMergeRow::FromSegment {
                segment: 0,
                old_node,
                vector: rows_src.row(source.row_to_node(old_node)),
            });
        }
        // Single-source tombstone healing always reuses the linked graph even at
        // heavy deletion rates (> 65%) as long as output_rows >= MIN_GRAPH_ROWS.
        merge_ordered_inner(
            source.dim,
            source.config,
            &[source],
            &ordered,
            MergeStrategy::JoinSet,
            true,
        )
    })?;
    if let Some(perm) = merged.reorder_global_in_place()? {
        survivor_old_ids = perm
            .into_iter()
            .map(|idx| survivor_old_ids[idx as usize])
            .collect();
    }
    Ok((merged, survivor_old_ids, stats))
}

/// The flat node-ordered FP32 rows of `index`, heap or mapped.
fn flat_rows(index: &HnswIndex) -> &[f32] {
    match &index.backing {
        Backing::Heap(store) => store.rows.values(),
        Backing::Mapped(mapped) => mapped.view().rows,
    }
}

/// Check that every segment matches `dim` and its live mask covers it.
fn check_segments(dim: usize, segments: &[SegmentMergeInput<'_>]) -> Result<(), VectorError> {
    for (seg_idx, seg) in segments.iter().enumerate() {
        if seg.index.dim != dim {
            return Err(VectorError::new(format!(
                "segment {seg_idx} has dimension {}, merge expects {dim}",
                seg.index.dim
            )));
        }
        if let Some(mask) = seg.live {
            if mask.len() != seg.index.len() {
                return Err(VectorError::new(format!(
                    "segment {seg_idx} live mask covers {} rows, segment holds {}",
                    mask.len(),
                    seg.index.len()
                )));
            }
        }
    }
    Ok(())
}

/// Merge `segments` (in segment order, filtered by each segment's `live` mask)
/// plus optional `unlinked_vectors` into one dense [`HnswIndex`].
pub(super) fn merge_segments(
    dim: usize,
    config: HnswConfig,
    segments: &[SegmentMergeInput<'_>],
    unlinked_vectors: &[f32],
    strategy: MergeStrategy,
) -> Result<(HnswIndex, MergeStats), VectorError> {
    config.validate()?;
    if dim == 0 || dim > MAX_DIM {
        return Err(VectorError::new(format!(
            "{HNSW} dimension {dim} is outside 1..={MAX_DIM}"
        )));
    }
    if !unlinked_vectors.len().is_multiple_of(dim) {
        return Err(VectorError::new(format!(
            "{} unlinked values do not form rows of dimension {dim}",
            unlinked_vectors.len()
        )));
    }
    check_segments(dim, segments)?;

    let source_refs: Vec<&HnswIndex> = segments.iter().map(|s| s.index).collect();
    let mut total_rows = unlinked_vectors.len() / dim;
    for seg in segments {
        let count = seg.live.map_or_else(|| seg.index.len(), |m| m.count());
        total_rows = total_rows
            .checked_add(count)
            .ok_or_else(|| VectorError::new("merged row count overflow"))?;
    }
    let mut ordered = Vec::with_capacity(total_rows);
    for (seg_idx, seg) in segments.iter().enumerate() {
        let flat = flat_rows(seg.index);
        let mask = seg.live.unwrap_or(Mask::All(seg.index.len()));
        let _ = mask.try_for_each(|row| {
            let donor_node = seg.index.row_to_node(row as u32);
            let at = donor_node as usize * dim;
            ordered.push(OrderedMergeRow::FromSegment {
                segment: seg_idx,
                old_node: row as u32,
                vector: &flat[at..at + dim],
            });
            ControlFlow::<()>::Continue(())
        });
    }
    for raw in unlinked_vectors.chunks_exact(dim) {
        ordered.push(OrderedMergeRow::Raw(raw));
    }
    let (mut merged, stats) = merge_ordered(dim, config, &source_refs, &ordered, strategy)?;
    merged.reorder_local_in_place();
    Ok((merged, stats))
}

/// Build a dense [`HnswIndex`] over `rows` in exact caller-specified slot order,
/// reusing graph structure from `sources` according to `strategy`.
pub(crate) fn merge_ordered(
    dim: usize,
    config: HnswConfig,
    sources: &[&HnswIndex],
    rows: &[OrderedMergeRow<'_>],
    strategy: MergeStrategy,
) -> Result<(HnswIndex, MergeStats), VectorError> {
    merge_ordered_inner(dim, config, sources, rows, strategy, false)
}

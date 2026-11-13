//! Donor-seeded compaction remap, Qdrant-style tombstone link healing (`#76`),
//! greedy `K`-dominating join-set segment merge, and directed forward-BFS
//! layer-0 connectivity repair (`#99`).
//!
//! When sealed HNSW segments compact into a dense output image (removing
//! tombstones, merging multiple segments, or both):
//!
//! 1. **Donor transplant & tombstone link healing (`#76`):** The largest
//!    qualifying linked source segment becomes the donor. Surviving donor rows
//!    keep their levels and surviving edges under the output's dense `0..N`
//!    node numbering. Any adjacency list that held a deleted neighbor is
//!    repaired locally by gathering surviving 1-hop neighbors plus surviving
//!    2-hop bridge neighbors across deleted nodes, scoring them in bulk, and
//!    pruning with the direction heuristic (`View::select`).
//! 2. **Greedy `K`-dominating join-set merge (`#99`):** Each non-donor linked
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
//! 3. **Directed forward-BFS connectivity check & repair (`#99`):** A linear
//!    `O(V + E)` forward BFS from `entry` on layer 0 verifies that every node
//!    is reachable via directed outgoing edges, and bridges any stranded
//!    component into the main component while preserving existing in-degrees.

use std::cmp::Reverse;
use std::ops::ControlFlow;
use std::sync::atomic::{AtomicU32, Ordering as Atomic};

use crate::boolean_bits::Bits;
use crate::vector::{first_invalid_coordinate, Mask, VectorError, VectorProvider};

use super::config::{BuildMode, HnswConfig};
use super::distance::Kernel;
use super::graph::{
    level_for, with_scratch, FloorPrune, GraphLinks, LinkBuilder, LinkCells, Locks, NoHook,
    Patience, Scored, Scratch, SearchStats, Store, View, MAX_LEVEL, MAX_ROWS, NO_NODE,
};
use super::rows::RowSource;
use super::{Backing, HnswIndex, HNSW, MAX_DIM, MIN_GRAPH_ROWS, PARALLEL_MIN_BATCH};

/// Number of join-set neighbors each non-join layer-0 node aims to be covered
/// by in [`MergeStrategy::JoinSet`] (`#99`).
pub const JOIN_DOMINATION_K: u8 = 2;

/// Maximum local layer-0 expansions per grafted non-join node once seeded in
/// its target Voronoi neighborhood (`#99`).
const GRAFT_MAX_EXPANSIONS: usize = 40;

/// Minimum initial expansions per grafted node so 1-hop cross-segment bridge
/// seeds always step into sibling segments even when intra-segment cluster
/// neighbors raise the initial beam floor (`#99`).
const GRAFT_MIN_EXPANSIONS: usize = 10;

/// Minimum fraction of a source segment's physical rows that must survive into
/// an output image for that segment's graph edges to be reused as a donor or
/// join-set source (`0.35` covers up to `65%` tombstones while routing sparse
/// multi-bucket hash sub-samples to a clean build).
const MIN_SEGMENT_SURVIVAL_RATIO: f64 = 0.35;

/// Minimum surviving rows required from a linked source segment before reusing
/// its graph structure.
const MIN_QUALIFYING_SEGMENT_ROWS: usize = 64;

/// Maximum deleted-node hops traversed locally when healing an adjacency list
/// whose 1-hop neighborhood lost members to tombstones (`#76`).
const MAX_DELETED_BRIDGE_EXPANDS: usize = 64;

/// Graph construction / merge strategy when assembling a compacted [`HnswIndex`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MergeStrategy {
    /// Ignore source edges and construct a fresh HNSW graph over the output
    /// rows (`Store::build_links`).
    FullRebuild,
    /// Transplant and heal the largest qualifying donor segment, then insert
    /// every non-donor row via standard `LinkBuilder` beam search (`#76`).
    DonorReinsert,
    /// Transplant and heal the largest qualifying donor segment, insert a
    /// greedy `K`-dominating join set per non-donor linked segment, graft the
    /// remaining layer-0 nodes from their 1-hop intra-segment + 2-hop join
    /// neighborhoods, and verify/repair directed layer-0 reachability (`#99`).
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
    /// of `#99`'s upper-layer + `K`-dominating join set.
    pub join_set_rows: usize,
    /// Non-donor layer-0 rows grafted locally via join-set neighborhoods (`#99`).
    pub grafted_rows: usize,
    /// Non-donor / unlinked rows inserted via full `LinkBuilder` search.
    pub reinserted_rows: usize,
    /// Disconnected layer-0 islands bridged into `entry`'s forward-reachable
    /// component during the post-merge connectivity pass (`#99`).
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

#[derive(Clone, Copy)]
struct ValidatedRow {
    source: Option<(usize, u32)>,
}

struct NonDonorPlan {
    join_nodes: Vec<u32>,
    graft_tasks: Vec<(u32, Vec<u32>)>,
    fallback_insert_nodes: Vec<u32>,
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

    let source_refs: Vec<&HnswIndex> = segments.iter().map(|s| s.index).collect();
    // Mapped and Heap segments both expose slices through `with_row_source`;
    // collect flat row slices cleanly by recursing or borrowing raw slices.
    let mut row_slices: Vec<&[f32]> = Vec::with_capacity(segments.len());
    for seg in segments {
        let slice = match &seg.index.backing {
            Backing::Heap(store) => store.rows.values(),
            Backing::Mapped(mapped) => mapped.view().rows,
        };
        row_slices.push(slice);
    }

    let mut total_rows = unlinked_vectors.len() / dim;
    for seg in segments {
        let count = seg.live.map_or_else(|| seg.index.len(), |m| m.count());
        total_rows = total_rows
            .checked_add(count)
            .ok_or_else(|| VectorError::new("merged row count overflow"))?;
    }
    let mut ordered = Vec::with_capacity(total_rows);
    for (seg_idx, seg) in segments.iter().enumerate() {
        let flat = row_slices[seg_idx];
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

fn merge_ordered_inner(
    dim: usize,
    config: HnswConfig,
    sources: &[&HnswIndex],
    rows: &[OrderedMergeRow<'_>],
    strategy: MergeStrategy,
    allow_any_donor_survival: bool,
) -> Result<(HnswIndex, MergeStats), VectorError> {
    config.validate()?;
    if dim == 0 || dim > MAX_DIM {
        return Err(VectorError::new(format!(
            "{HNSW} dimension {dim} is outside 1..={MAX_DIM}"
        )));
    }
    let n = rows.len();
    if n > MAX_ROWS {
        return Err(VectorError::new(format!(
            "{HNSW} graph cannot address {n} rows; the limit is {MAX_ROWS}"
        )));
    }

    let kernel = Kernel::detect();
    let mut store = Store::new(
        dim,
        config.m as usize,
        config.ef_construction as usize,
        config.seed,
        kernel,
    );
    let mut stats = MergeStats {
        output_rows: n,
        ..Default::default()
    };

    if n == 0 {
        return Ok((
            HnswIndex {
                dim,
                config,
                kernel,
                backing: Backing::Heap(store),
            },
            stats,
        ));
    }

    // Copy dense output rows and validate finite coordinates + provenance.
    store.rows.reserve(n * dim);
    store.inv_norm.reserve(n);
    let source_slices: Vec<Option<&[f32]>> = sources
        .iter()
        .map(|src| {
            if src.dim == dim && src.config.m == config.m && src.is_graph_linked() {
                Some(match &src.backing {
                    Backing::Heap(s) => s.rows.values(),
                    Backing::Mapped(m) => m.view().rows,
                })
            } else {
                None
            }
        })
        .collect();

    let mut validated = Vec::with_capacity(n);
    let mut seg_surviving = vec![0usize; sources.len()];
    for (row_idx, item) in rows.iter().enumerate() {
        let vec = item.vector();
        if vec.len() != dim {
            return Err(VectorError::new(format!(
                "row {row_idx} has {} values, expected {dim}",
                vec.len()
            )));
        }
        if let Some((_, coord, value)) = first_invalid_coordinate(vec, dim) {
            return Err(VectorError::new(format!(
                "vector {row_idx} coordinate {coord} is invalid: {value}"
            )));
        }
        let norm = kernel.dot(vec, vec).sqrt();
        store.rows.push(vec);
        store.inv_norm.push(if norm > 0.0 && norm.is_finite() {
            1.0 / norm
        } else {
            0.0
        });

        let provenance = match *item {
            OrderedMergeRow::FromSegment {
                segment,
                old_node,
                vector,
            } => {
                if let Some(Some(src_flat)) = source_slices.get(segment) {
                    let old_idx = old_node as usize;
                    let src_len = sources[segment].len();
                    if old_idx < src_len {
                        let donor_node = sources[segment].row_to_node(old_node);
                        let start = donor_node as usize * dim;
                        if &src_flat[start..start + dim] == vector {
                            seg_surviving[segment] += 1;
                            Some((segment, donor_node))
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            OrderedMergeRow::Raw(_) => None,
        };
        validated.push(ValidatedRow { source: provenance });
    }

    // Small output invariant (#97): keep unlinked when below MIN_GRAPH_ROWS.
    if n < MIN_GRAPH_ROWS {
        return Ok((
            HnswIndex {
                dim,
                config,
                kernel,
                backing: Backing::Heap(store),
            },
            stats,
        ));
    }

    let parallel = config.build_mode == BuildMode::Parallel && n >= PARALLEL_MIN_BATCH;
    if strategy == MergeStrategy::FullRebuild {
        let links = store.build_links(parallel)?;
        stats.islands_repaired = repair_connectivity(&LinkBuilder::new(&store, &links));
        store.install_links(links);
        stats.reinserted_rows = n;
        return Ok((
            HnswIndex {
                dim,
                config,
                kernel,
                backing: Backing::Heap(store),
            },
            stats,
        ));
    }

    // Identify qualifying linked source segments and pick the largest donor.
    let seg_qualifies: Vec<bool> = (0..sources.len())
        .map(|s| {
            source_slices[s].is_some()
                && seg_surviving[s] > 0
                && (allow_any_donor_survival
                    || (seg_surviving[s] >= MIN_QUALIFYING_SEGMENT_ROWS
                        && (seg_surviving[s] as f64)
                            >= (sources[s].len() as f64) * MIN_SEGMENT_SURVIVAL_RATIO))
        })
        .collect();

    let donor = (0..sources.len())
        .filter(|&s| seg_qualifies[s])
        .max_by_key(|&s| (seg_surviving[s], Reverse(s)));

    let Some(donor_seg) = donor else {
        let links = store.build_links(parallel)?;
        stats.islands_repaired = repair_connectivity(&LinkBuilder::new(&store, &links));
        store.install_links(links);
        stats.reinserted_rows = n;
        stats.used_no_donor_rebuild = true;
        return Ok((
            HnswIndex {
                dim,
                config,
                kernel,
                backing: Backing::Heap(store),
            },
            stats,
        ));
    };

    stats.donor_segment = Some(donor_seg);
    stats.donor_rows = seg_surviving[donor_seg];

    // Assign node levels in 0..n: preserve source levels for qualifying linked
    // segments so upper-layer edge invariants (`level[nbr] >= layer`) hold on
    // the donor and upper-layer hubs carry over into non-donor join sets.
    let source_levels: Vec<Option<&[u8]>> = (0..sources.len())
        .map(|s| {
            if seg_qualifies[s] {
                Some(match &sources[s].backing {
                    Backing::Heap(st) => st.links().expect("qualified linked").level.as_slice(),
                    Backing::Mapped(mp) => mp.view().level,
                })
            } else {
                None
            }
        })
        .collect();

    let stride0 = store.m0 + 1;
    let stride_up = store.m + 1;
    let mut level = Vec::with_capacity(n);
    let mut upper_slot = Vec::with_capacity(n);
    let mut upper_base = Vec::new();
    let mut upper_cells = 0usize;
    for (new_u, row_meta) in validated.iter().enumerate() {
        let lvl = match row_meta.source {
            Some((seg, old_u)) if seg_qualifies[seg] => {
                source_levels[seg].expect("qualified")[old_u as usize].min(MAX_LEVEL)
            }
            _ => level_for(store.seed, new_u as u32, store.level_mult),
        };
        level.push(lvl);
        if lvl == 0 {
            upper_slot.push(NO_NODE);
        } else {
            let cells = usize::from(lvl) * stride_up;
            if upper_cells + cells > u32::MAX as usize || upper_base.len() >= MAX_ROWS {
                return Err(VectorError::new(format!(
                    "{HNSW} upper layers exceed the addressable {} cells",
                    u32::MAX
                )));
            }
            upper_slot.push(upper_base.len() as u32);
            upper_base.push(upper_cells as u32);
            upper_cells += cells;
        }
    }

    let links0: Vec<AtomicU32> = std::iter::repeat_with(|| AtomicU32::new(0))
        .take(n * stride0)
        .collect();
    let upper: Vec<AtomicU32> = std::iter::repeat_with(|| AtomicU32::new(0))
        .take(upper_cells)
        .collect();
    let links = GraphLinks::from_parts(level, upper_slot, upper_base, links0, upper, NO_NODE, 0);
    let builder = LinkBuilder::new(&store, &links);

    // Build donor remap table (`old_u -> new_u`) and transplant + heal donor edges.
    let donor_index = sources[donor_seg];
    let mut donor_to_new = vec![NO_NODE; donor_index.len()];
    let mut donor_pairs: Vec<(u32, u32)> = Vec::with_capacity(seg_surviving[donor_seg]);
    for (new_u, row_meta) in validated.iter().enumerate() {
        if let Some((seg, old_u)) = row_meta.source {
            if seg == donor_seg {
                donor_to_new[old_u as usize] = new_u as u32;
                donor_pairs.push((old_u, new_u as u32));
            }
        }
    }

    let donor_parallel =
        config.build_mode == BuildMode::Parallel && donor_pairs.len() >= PARALLEL_MIN_BATCH;
    stats.donor_healed_lists = match &donor_index.backing {
        Backing::Heap(st) => {
            let dv = st.view();
            transplant_and_heal_donor(&dv, &builder, &donor_to_new, &donor_pairs, donor_parallel)
        }
        Backing::Mapped(mp) => {
            let dv = mp.view();
            transplant_and_heal_donor(&dv, &builder, &donor_to_new, &donor_pairs, donor_parallel)
        }
    };

    // Publish initial `(entry, top)` over the transplanted donor subgraph so
    // subsequent `LinkBuilder` insertions descend a valid hierarchy and raise
    // `top` naturally whenever a non-donor node has `level > donor_top`.
    let out_view = builder.view();
    let initial_entry = select_donor_entry(&out_view, donor_index, &donor_to_new, &donor_pairs);
    let initial_top = links.level[initial_entry as usize];
    links.publish_entry(initial_entry, initial_top);
    match &donor_index.backing {
        Backing::Heap(st) => {
            inherit_deleted_entry_highways(&st.view(), &builder, &donor_to_new, initial_entry);
        }
        Backing::Mapped(mp) => {
            inherit_deleted_entry_highways(&mp.view(), &builder, &donor_to_new, initial_entry);
        }
    }

    match strategy {
        MergeStrategy::FullRebuild => unreachable!("handled above"),
        MergeStrategy::DonorReinsert => {
            let mut non_donor = Vec::with_capacity(n - donor_pairs.len());
            for (new_u, row_meta) in validated.iter().enumerate() {
                if !matches!(row_meta.source, Some((seg, _)) if seg == donor_seg) {
                    non_donor.push(new_u as u32);
                }
            }
            let par_insert =
                config.build_mode == BuildMode::Parallel && non_donor.len() >= PARALLEL_MIN_BATCH;
            builder.insert_slice(&non_donor, par_insert);
            stats.reinserted_rows = non_donor.len();
        }
        MergeStrategy::JoinSet => {
            // Partition non-donor rows by qualifying segment vs unlinked rows.
            let mut per_seg_pairs: Vec<Vec<(u32, u32)>> = vec![Vec::new(); sources.len()];
            let mut unlinked_nodes: Vec<u32> = Vec::new();
            for (new_u, row_meta) in validated.iter().enumerate() {
                match row_meta.source {
                    Some((seg, _)) if seg == donor_seg => {}
                    Some((seg, old_u)) if seg_qualifies[seg] => {
                        per_seg_pairs[seg].push((old_u, new_u as u32));
                    }
                    _ => unlinked_nodes.push(new_u as u32),
                }
            }

            let mut seg_join_lists: Vec<Vec<u32>> = Vec::new();
            let mut seg_graft_lists: Vec<Vec<(u32, Vec<u32>)>> = Vec::new();
            let mut total_graft_rows = 0usize;
            let mut fallback_inserts: Vec<u32> = Vec::new();

            // Order non-donor qualifying segments largest-first for deterministic
            // round-robin interleaving.
            let mut non_donor_segs: Vec<usize> = (0..sources.len())
                .filter(|&s| s != donor_seg && !per_seg_pairs[s].is_empty())
                .collect();
            non_donor_segs.sort_by_key(|&s| (Reverse(per_seg_pairs[s].len()), s));

            for &seg in &non_donor_segs {
                let pairs = &per_seg_pairs[seg];
                let seg_len = sources[seg].len();
                let plan = match &sources[seg].backing {
                    Backing::Heap(st) => {
                        let sv = st.view();
                        plan_non_donor_segment(&sv, seg_len, pairs, &links.level)
                    }
                    Backing::Mapped(mp) => {
                        let sv = mp.view();
                        plan_non_donor_segment(&sv, seg_len, pairs, &links.level)
                    }
                };
                stats.join_set_rows += plan.join_nodes.len();
                total_graft_rows += plan.graft_tasks.len();
                seg_join_lists.push(plan.join_nodes);
                seg_graft_lists.push(plan.graft_tasks);
                fallback_inserts.extend(plan.fallback_insert_nodes);
            }

            // Interleave join-set nodes round-robin across segments so early
            // insertions span every segment uniformly, followed by unlinked and
            // zero-degree fallback nodes.
            let total_inserts = stats.join_set_rows + unlinked_nodes.len() + fallback_inserts.len();
            let mut all_inserts = Vec::with_capacity(total_inserts);
            let max_join_len = seg_join_lists.iter().map(Vec::len).max().unwrap_or(0);
            for idx in 0..max_join_len {
                for list in &seg_join_lists {
                    if let Some(&node) = list.get(idx) {
                        all_inserts.push(node);
                    }
                }
            }
            let join_only_count = all_inserts.len();
            all_inserts.extend_from_slice(&unlinked_nodes);
            all_inserts.extend_from_slice(&fallback_inserts);
            stats.reinserted_rows = unlinked_nodes.len() + fallback_inserts.len();

            let par_insert =
                config.build_mode == BuildMode::Parallel && all_inserts.len() >= PARALLEL_MIN_BATCH;
            builder.insert_slice(&all_inserts, par_insert);

            if total_graft_rows > 0 {
                let mut graft_tasks = Vec::with_capacity(total_graft_rows);
                let max_graft_len = seg_graft_lists.iter().map(Vec::len).max().unwrap_or(0);
                for idx in 0..max_graft_len {
                    for list in &seg_graft_lists {
                        if let Some(task) = list.get(idx) {
                            graft_tasks.push(task.clone());
                        }
                    }
                }

                // Snapshot the merged layer-0 adjacency of every join-set node
                // into a flat read-only CSR buffer for race-free 2-hop seeding.
                let join_nodes_slice = &all_inserts[..join_only_count];
                let (join_slot, join_offsets, join_edges) =
                    snapshot_join_layer0(&builder.view(), n, join_nodes_slice);

                let par_graft = config.build_mode == BuildMode::Parallel
                    && graft_tasks.len() >= PARALLEL_MIN_BATCH;
                graft_non_join_nodes(
                    &builder,
                    n,
                    &graft_tasks,
                    &join_slot,
                    &join_offsets,
                    &join_edges,
                    par_graft,
                );
                stats.grafted_rows = graft_tasks.len();
            }
        }
    }

    // Verify `entry` / `top` and run directed forward-BFS layer-0 connectivity
    // check & repair (#99).
    stats.islands_repaired = repair_connectivity(&builder);
    store.install_links(links);

    Ok((
        HnswIndex {
            dim,
            config,
            kernel,
            backing: Backing::Heap(store),
        },
        stats,
    ))
}

/// Count nodes in `0..index.len()` that are NOT forward-reachable from `entry`
/// via directed layer-0 outgoing edges (`0` when `index` is unlinked or fully
/// connected).
pub(super) fn unreachable_from_entry(index: &HnswIndex) -> usize {
    if !index.is_graph_linked() || index.is_empty() {
        return 0;
    }
    match &index.backing {
        Backing::Heap(store) => count_unreachable(&store.view()),
        Backing::Mapped(mapped) => count_unreachable(&mapped.view()),
    }
}

fn count_unreachable<L: LinkCells + ?Sized>(view: &View<'_, L>) -> usize {
    let n = view.n();
    if n == 0 || view.entry == NO_NODE || view.entry as usize >= n {
        return n;
    }
    let mut visited = Bits::empty(n);
    let mut queue = Vec::with_capacity(n);
    let mut nbrs = Vec::with_capacity(view.m0);
    visited.set(view.entry as usize);
    queue.push(view.entry);
    let mut head = 0usize;
    while head < queue.len() {
        let u = queue[head];
        head += 1;
        view.copy_links(u, 0, None, &mut nbrs);
        for &v in &nbrs {
            let vi = v as usize;
            if vi < n && !visited.test(vi) {
                visited.set(vi);
                queue.push(v);
            }
        }
    }
    n.saturating_sub(queue.len())
}

fn select_donor_entry<L: LinkCells + ?Sized>(
    out_view: &View<'_, L>,
    donor_index: &HnswIndex,
    donor_to_new: &[u32],
    donor_pairs: &[(u32, u32)],
) -> u32 {
    let max_donor_lvl = donor_pairs
        .iter()
        .map(|&(_, new_u)| out_view.level[new_u as usize])
        .max()
        .unwrap_or(0);

    // Keep the donor's original entry when it survived and still sits at the
    // highest surviving donor level.
    let orig_donor_entry = match &donor_index.backing {
        Backing::Heap(st) => st.entry_state().0,
        Backing::Mapped(mp) => mp.header().entry,
    };
    if (orig_donor_entry as usize) < donor_to_new.len() {
        let mapped = donor_to_new[orig_donor_entry as usize];
        if mapped != NO_NODE && out_view.level[mapped as usize] == max_donor_lvl {
            return mapped;
        }
    }

    // Otherwise promote the surviving donor node at `max_donor_lvl` with the
    // highest surviving layer-0 degree (ties broken by lowest `new_u`).
    donor_pairs
        .iter()
        .map(|&(_, new_u)| new_u)
        .filter(|&new_u| out_view.level[new_u as usize] == max_donor_lvl)
        .max_by_key(|&new_u| {
            let start = out_view.list_start(new_u, 0);
            let deg = out_view.links0.get(start);
            (deg, Reverse(new_u))
        })
        .expect("donor_pairs is non-empty")
}

/// When the donor's original `entry` was deleted, wire reciprocal highways
/// between the newly promoted `initial_entry` and the deleted entry's surviving
/// neighbors across layers `0..=min(initial_top, orig_top)`.
fn inherit_deleted_entry_highways<L: LinkCells + ?Sized>(
    donor_view: &View<'_, L>,
    builder: &LinkBuilder<'_>,
    donor_to_new: &[u32],
    initial_entry: u32,
) {
    let orig_entry = donor_view.entry;
    if orig_entry == NO_NODE || (orig_entry as usize) >= donor_to_new.len() {
        return;
    }
    if donor_to_new[orig_entry as usize] != NO_NODE {
        return;
    }
    let initial_top = usize::from(builder.links.level[initial_entry as usize]);
    let max_layer = initial_top.min(usize::from(donor_view.top));
    with_scratch(|scratch| {
        let mut raw = Vec::new();
        let mut hop2 = Vec::new();
        let mut pool = Vec::new();
        for layer in 0..=max_layer {
            let cap = builder.view().cap(layer);
            donor_view.copy_links(orig_entry, layer, None, &mut raw);
            let mut surv = Vec::new();
            for &old_v in &raw {
                let mapped = donor_to_new[old_v as usize];
                if mapped != NO_NODE && mapped != initial_entry {
                    if usize::from(builder.links.level[mapped as usize]) >= layer
                        && !surv.contains(&mapped)
                    {
                        surv.push(mapped);
                    }
                } else if mapped == NO_NODE {
                    donor_view.copy_links(old_v, layer, None, &mut hop2);
                    for &old_w in &hop2 {
                        let m2 = donor_to_new[old_w as usize];
                        if m2 != NO_NODE
                            && m2 != initial_entry
                            && usize::from(builder.links.level[m2 as usize]) >= layer
                            && !surv.contains(&m2)
                        {
                            surv.push(m2);
                        }
                    }
                }
            }
            for &v in &surv {
                builder.connect_back(initial_entry, v, layer, cap, None, &mut pool, scratch);
                builder.connect_back(v, initial_entry, layer, cap, None, &mut pool, scratch);
            }
        }
    });
}

#[derive(Clone, Copy)]
struct BridgeLink {
    target: u32,
    source: u32,
    layer: u8,
}

struct HealNodeOut {
    healed_lists: usize,
    bridges: Vec<BridgeLink>,
}

/// Remap surviving donor edges across all layers `0..=level[new_u]`, preserving
/// all surviving 1-hop donor edges while refilling vacated tombstone slots with
/// heuristic-diversified 2-hop bridges and reciprocal back-links (`#76`).
fn transplant_and_heal_donor<L: LinkCells + ?Sized>(
    donor_view: &View<'_, L>,
    builder: &LinkBuilder<'_>,
    donor_to_new: &[u32],
    donor_pairs: &[(u32, u32)],
    parallel: bool,
) -> usize {
    let out_n = builder.inv_norm.len();
    let heal_one = |&(old_u, new_u): &(u32, u32), scratch: &mut Scratch| -> HealNodeOut {
        let out_view = builder.view();
        let lvl = usize::from(builder.links.level[new_u as usize]);
        let mut raw_nbrs = std::mem::take(&mut scratch.nbrs);
        let mut bridge_nbrs = std::mem::take(&mut scratch.routes);
        let mut cands = std::mem::take(&mut scratch.fresh);
        let mut pool = std::mem::take(&mut scratch.pool);
        let mut sel = std::mem::take(&mut scratch.sel);
        let mut deleted_queue: Vec<u32> = Vec::new();
        let mut surv_1hop: Vec<u32> = Vec::new();
        let mut healed: Vec<u32> = Vec::new();
        let mut bridges: Vec<BridgeLink> = Vec::new();
        let mut healed_lists = 0usize;

        for layer in 0..=lvl {
            let cap = out_view.cap(layer);
            donor_view.copy_links(old_u, layer, None, &mut raw_nbrs);
            let any_deleted = raw_nbrs
                .iter()
                .any(|&old_v| donor_to_new[old_v as usize] == NO_NODE);
            if !any_deleted {
                cands.clear();
                for &old_v in &raw_nbrs {
                    let mapped = donor_to_new[old_v as usize];
                    if mapped != new_u {
                        cands.push(mapped);
                    }
                }
                builder.write_links(new_u, layer, &cands, None);
                continue;
            }

            healed_lists += 1;
            let orig_deg = raw_nbrs.len().min(cap);
            surv_1hop.clear();
            cands.clear();
            deleted_queue.clear();
            scratch.begin(out_n);
            scratch.visit(new_u);

            for &old_v in &raw_nbrs {
                let mapped = donor_to_new[old_v as usize];
                if mapped != NO_NODE {
                    if scratch.visit(mapped) {
                        surv_1hop.push(mapped);
                        cands.push(mapped);
                    }
                } else if deleted_queue.len() < MAX_DELETED_BRIDGE_EXPANDS {
                    deleted_queue.push(old_v);
                }
            }

            // Expand deleted 1-hop neighbors (and a bounded 2nd deleted hop
            // when heavy deletions leave fewer than `cap` surviving candidates).
            let first_wave = deleted_queue.len();
            let mut d_idx = 0usize;
            while d_idx < deleted_queue.len() {
                if d_idx >= first_wave && cands.len() >= cap * 2 {
                    break;
                }
                let del_node = deleted_queue[d_idx];
                d_idx += 1;
                donor_view.copy_links(del_node, layer, None, &mut bridge_nbrs);
                for &old_w in &bridge_nbrs {
                    if old_w == old_u {
                        continue;
                    }
                    let mapped = donor_to_new[old_w as usize];
                    if mapped != NO_NODE {
                        if usize::from(builder.links.level[mapped as usize]) >= layer
                            && scratch.visit(mapped)
                        {
                            cands.push(mapped);
                        }
                    } else if d_idx <= first_wave
                        && deleted_queue.len() < MAX_DELETED_BRIDGE_EXPANDS
                        && !deleted_queue.contains(&old_w)
                    {
                        deleted_queue.push(old_w);
                    }
                }
            }

            if cands.len() <= surv_1hop.len() {
                builder.write_links(new_u, layer, &surv_1hop, None);
            } else {
                let row_u = out_view.row(new_u);
                pool.clear();
                pool.reserve(cands.len());
                out_view.for_each_scored(row_u, &cands, |v, score| {
                    pool.push(Scored { score, node: v });
                });
                pool.sort_unstable_by(|a, b| b.cmp(a));
                out_view.select(out_view.inv_norm[new_u as usize], &pool, cap, &mut sel);

                // Keep 100% of surviving 1-hop donor edges (preserving prior
                // `connect_back` cluster & duplicate links), then fill vacated
                // slots first from direction-diversified `sel` bridges and
                // finally from top-scoring remaining 2-hop bridges up to `orig_deg`.
                healed.clear();
                healed.extend_from_slice(&surv_1hop);
                for &b in &sel {
                    if healed.len() >= cap {
                        break;
                    }
                    if !healed.contains(&b) {
                        healed.push(b);
                        bridges.push(BridgeLink {
                            target: b,
                            source: new_u,
                            layer: layer as u8,
                        });
                    }
                }
                if healed.len() < orig_deg {
                    for item in &pool {
                        if healed.len() >= orig_deg {
                            break;
                        }
                        if !healed.contains(&item.node) {
                            healed.push(item.node);
                            bridges.push(BridgeLink {
                                target: item.node,
                                source: new_u,
                                layer: layer as u8,
                            });
                        }
                    }
                }
                builder.write_links(new_u, layer, &healed, None);
            }
        }

        scratch.nbrs = raw_nbrs;
        scratch.routes = bridge_nbrs;
        scratch.fresh = cands;
        scratch.pool = pool;
        scratch.sel = sel;
        HealNodeOut {
            healed_lists,
            bridges,
        }
    };

    let mut total_healed = 0usize;
    let mut bridge_batches: Vec<Vec<BridgeLink>> = Vec::new();
    if parallel {
        use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
        let parts: Vec<HealNodeOut> = donor_pairs
            .par_iter()
            .map(|pair| with_scratch(|scratch| heal_one(pair, scratch)))
            .collect();
        for part in parts {
            total_healed += part.healed_lists;
            if !part.bridges.is_empty() {
                bridge_batches.push(part.bridges);
            }
        }
    } else {
        with_scratch(|scratch| {
            for pair in donor_pairs {
                let part = heal_one(pair, scratch);
                total_healed += part.healed_lists;
                if !part.bridges.is_empty() {
                    bridge_batches.push(part.bridges);
                }
            }
        });
    }

    if !bridge_batches.is_empty() {
        let out_view = builder.view();
        if parallel {
            use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
            let locks = Locks::default();
            bridge_batches.par_iter().for_each(|batch| {
                with_scratch(|scratch| {
                    let mut pool = std::mem::take(&mut scratch.pool);
                    for &edge in batch {
                        let layer = usize::from(edge.layer);
                        let cap = out_view.cap(layer);
                        builder.connect_back(
                            edge.target,
                            edge.source,
                            layer,
                            cap,
                            Some(&locks),
                            &mut pool,
                            scratch,
                        );
                    }
                    scratch.pool = pool;
                });
            });
        } else {
            with_scratch(|scratch| {
                let mut pool = std::mem::take(&mut scratch.pool);
                for batch in &bridge_batches {
                    for &edge in batch {
                        let layer = usize::from(edge.layer);
                        let cap = out_view.cap(layer);
                        builder.connect_back(
                            edge.target,
                            edge.source,
                            layer,
                            cap,
                            None,
                            &mut pool,
                            scratch,
                        );
                    }
                }
                scratch.pool = pool;
            });
        }
    }

    total_healed
}

/// Extract surviving layer-0 neighborhoods and choose `#99`'s greedy
/// `K`-dominating join set (`level >= 1` plus greedy highest-degree cover) for
/// one non-donor linked segment.
fn plan_non_donor_segment<L: LinkCells + ?Sized>(
    seg_view: &View<'_, L>,
    seg_len: usize,
    pairs: &[(u32, u32)],
    out_levels: &[u8],
) -> NonDonorPlan {
    let surv_len = pairs.len();
    let mut old_to_local = vec![NO_NODE; seg_len];
    for (local_idx, &(old_u, _)) in pairs.iter().enumerate() {
        old_to_local[old_u as usize] = local_idx as u32;
    }

    let cap_cands = seg_view.m0 * 2;
    let mut local_l0: Vec<Vec<u32>> = Vec::with_capacity(surv_len);
    let mut raw_nbrs = Vec::with_capacity(seg_view.m0);
    let mut bridge_nbrs = Vec::with_capacity(seg_view.m0);
    let mut seen_local = vec![0u32; surv_len];
    let mut epoch = 0u32;

    for (local_idx, &(old_u, _)) in pairs.iter().enumerate() {
        epoch = epoch.wrapping_add(1);
        if epoch == 0 {
            seen_local.fill(0);
            epoch = 1;
        }
        seen_local[local_idx] = epoch;

        seg_view.copy_links(old_u, 0, None, &mut raw_nbrs);
        let mut nbr_locals = Vec::with_capacity(raw_nbrs.len());
        let mut deleted_1hop = Vec::new();
        for &old_v in &raw_nbrs {
            let loc = old_to_local[old_v as usize];
            if loc != NO_NODE {
                if seen_local[loc as usize] != epoch {
                    seen_local[loc as usize] = epoch;
                    nbr_locals.push(loc);
                }
            } else {
                deleted_1hop.push(old_v);
            }
        }
        for del_v in deleted_1hop {
            if nbr_locals.len() >= cap_cands {
                break;
            }
            seg_view.copy_links(del_v, 0, None, &mut bridge_nbrs);
            for &old_w in &bridge_nbrs {
                if nbr_locals.len() >= cap_cands {
                    break;
                }
                let loc = old_to_local[old_w as usize];
                if loc != NO_NODE && seen_local[loc as usize] != epoch {
                    seen_local[loc as usize] = epoch;
                    nbr_locals.push(loc);
                }
            }
        }
        local_l0.push(nbr_locals);
    }

    // Greedy K-dominating join set:
    // 1. Every node with `level >= 1` enters `in_join` unconditionally.
    // 2. Remaining `level == 0` nodes are ordered by `(surviving_deg desc, new_u asc)`
    //    and added whenever `dom_count[idx] < JOIN_DOMINATION_K`.
    let mut in_join = vec![false; surv_len];
    let mut dom_count = vec![0u8; surv_len];
    let mut l0_order = Vec::with_capacity(surv_len);

    for (idx, &(_, new_u)) in pairs.iter().enumerate() {
        if out_levels[new_u as usize] >= 1 {
            in_join[idx] = true;
            for &nbr_loc in &local_l0[idx] {
                let slot = &mut dom_count[nbr_loc as usize];
                *slot = slot.saturating_add(1);
            }
        } else {
            l0_order.push(idx as u32);
        }
    }

    l0_order.sort_unstable_by_key(|&idx| {
        let i = idx as usize;
        (Reverse(local_l0[i].len()), pairs[i].1)
    });

    for idx_u32 in l0_order {
        let idx = idx_u32 as usize;
        if dom_count[idx] < JOIN_DOMINATION_K && !local_l0[idx].is_empty() {
            in_join[idx] = true;
            dom_count[idx] = JOIN_DOMINATION_K;
            for &nbr_loc in &local_l0[idx] {
                let slot = &mut dom_count[nbr_loc as usize];
                *slot = slot.saturating_add(1);
            }
        }
    }

    let mut join_nodes = Vec::new();
    let mut graft_tasks = Vec::new();
    let mut fallback_insert_nodes = Vec::new();

    for (idx, &(_, new_u)) in pairs.iter().enumerate() {
        if in_join[idx] {
            join_nodes.push(new_u);
        } else if local_l0[idx].is_empty() || dom_count[idx] == 0 {
            fallback_insert_nodes.push(new_u);
        } else {
            let mapped_nbrs: Vec<u32> = local_l0[idx]
                .iter()
                .map(|&loc| pairs[loc as usize].1)
                .collect();
            graft_tasks.push((new_u, mapped_nbrs));
        }
    }

    NonDonorPlan {
        join_nodes,
        graft_tasks,
        fallback_insert_nodes,
    }
}

/// Snapshot layer-0 neighbor lists of all `join_nodes` into a compact CSR table.
fn snapshot_join_layer0<L: LinkCells + ?Sized>(
    view: &View<'_, L>,
    n: usize,
    join_nodes: &[u32],
) -> (Vec<u32>, Vec<u32>, Vec<u32>) {
    let mut join_slot = vec![NO_NODE; n];
    let mut offsets = Vec::with_capacity(join_nodes.len() + 1);
    let mut edges = Vec::with_capacity(join_nodes.len() * view.m0);
    let mut nbrs = Vec::with_capacity(view.m0);
    offsets.push(0);
    for (slot_idx, &node) in join_nodes.iter().enumerate() {
        join_slot[node as usize] = slot_idx as u32;
        view.copy_links(node, 0, None, &mut nbrs);
        edges.extend_from_slice(&nbrs);
        offsets.push(edges.len() as u32);
    }
    (join_slot, offsets, edges)
}

/// Graft non-join layer-0 nodes by seeding a bounded local layer-0 beam walk
/// (`ef_graft`, capped at [`GRAFT_MAX_EXPANSIONS`] hops, with zero upper-layer
/// descent) from `{surviving intra-segment neighbors} ∪ {merged layer-0
/// neighbors of join-set neighbors} ∪ {already-placed back-links}`, running
/// `select`, union-publishing with concurrent back-links, and reciprocating via
/// `connect_back`.
fn graft_non_join_nodes(
    builder: &LinkBuilder<'_>,
    n: usize,
    graft_tasks: &[(u32, Vec<u32>)],
    join_slot: &[u32],
    join_offsets: &[u32],
    join_edges: &[u32],
    parallel: bool,
) {
    let ef_graft = (builder.ef_construction * 2 / 3).clamp(builder.m0 + 24, 128);

    let graft_one =
        |(node, seg_nbrs): &(u32, Vec<u32>), locks: Option<&Locks>, scratch: &mut Scratch| {
            let view = builder.view();
            let node_u = *node;
            let mut cands = std::mem::take(&mut scratch.fresh);
            let mut step_nbrs = std::mem::take(&mut scratch.routes);
            let mut pool = std::mem::take(&mut scratch.pool);
            let mut sel = std::mem::take(&mut scratch.sel);

            scratch.begin(n);
            scratch.visit(node_u);

            // 1. Score surviving intra-segment layer-0 neighbors into `res` so no
            //    intra-segment quality is lost, keeping them ranked so spare slots
            //    after direction pruning retain intra-cluster links, and seeding
            //    `cand` with the top 6 intra-segment bridges.
            cands.clear();
            for &u in seg_nbrs {
                if scratch.visit(u) {
                    cands.push(u);
                }
            }
            let row_v = view.row(node_u);
            pool.clear();
            let mut intra_sorted: Vec<u32> = Vec::with_capacity(cands.len());
            if !cands.is_empty() {
                view.for_each_scored(row_v, &cands, |w, score| {
                    let item = Scored { score, node: w };
                    pool.push(item);
                    if scratch.res.len() < ef_graft {
                        scratch.res.push(Reverse(item));
                    } else if item > scratch.res.peek().expect("ef_graft > 0").0 {
                        scratch.res.pop();
                        scratch.res.push(Reverse(item));
                    }
                });
                pool.sort_unstable_by(|a, b| b.cmp(a));
                for &top_intra in pool.iter().take(6) {
                    scratch.cand.push(top_intra);
                }
                for item in &pool {
                    intra_sorted.push(item.node);
                }
            }

            // 2. Gather cross-segment seeds: already-placed reciprocal back-links
            //    on `node_u` plus the merged layer-0 neighborhoods of `node_u`'s
            //    join-set neighbors. Push top cross-segment seeds unconditionally
            //    into `cand` so 1-hop cross-segment bridges always expand.
            cands.clear();
            view.copy_links(node_u, 0, locks, &mut step_nbrs);
            for &w in &step_nbrs {
                if scratch.visit(w) {
                    cands.push(w);
                }
            }
            for &u in seg_nbrs {
                let slot = join_slot[u as usize];
                if slot != NO_NODE {
                    let s = slot as usize;
                    let start = join_offsets[s] as usize;
                    let end = join_offsets[s + 1] as usize;
                    for &w in &join_edges[start..end] {
                        if scratch.visit(w) {
                            cands.push(w);
                        }
                    }
                }
            }

            if scratch.res.is_empty() && cands.is_empty() {
                scratch.fresh = cands;
                scratch.routes = step_nbrs;
                scratch.pool = pool;
                scratch.sel = sel;
                builder.insert(node_u, locks, scratch);
                return;
            }

            if !cands.is_empty() {
                pool.clear();
                view.for_each_scored(row_v, &cands, |w, score| {
                    let item = Scored { score, node: w };
                    pool.push(item);
                    if scratch.res.len() < ef_graft {
                        scratch.res.push(Reverse(item));
                    } else if item > scratch.res.peek().expect("ef_graft > 0").0 {
                        scratch.res.pop();
                        scratch.res.push(Reverse(item));
                    }
                });
                pool.sort_unstable_by(|a, b| b.cmp(a));
                let worst_res = scratch.res.peek().map_or(f32::NEG_INFINITY, |r| r.0.score);
                for (idx, &item) in pool.iter().enumerate() {
                    if idx < 16 || item.score >= worst_res {
                        scratch.cand.push(item);
                    }
                }
            }

            let mut expansions = 0usize;
            while let Some(c) = scratch.cand.pop() {
                if expansions >= GRAFT_MIN_EXPANSIONS
                    && scratch.res.len() >= ef_graft
                    && c < scratch.res.peek().expect("ef_graft > 0").0
                {
                    break;
                }
                if expansions >= GRAFT_MAX_EXPANSIONS {
                    break;
                }
                expansions += 1;
                view.copy_links(c.node, 0, locks, &mut step_nbrs);
                cands.clear();
                for &w in &step_nbrs {
                    if scratch.visit(w) {
                        cands.push(w);
                    }
                }
                if !cands.is_empty() {
                    view.for_each_scored(row_v, &cands, |w, score| {
                        let item = Scored { score, node: w };
                        if scratch.res.len() < ef_graft {
                            scratch.cand.push(item);
                            scratch.res.push(Reverse(item));
                        } else if item > scratch.res.peek().expect("ef_graft > 0").0 {
                            scratch.cand.push(item);
                            scratch.res.pop();
                            scratch.res.push(Reverse(item));
                        }
                    });
                }
            }

            pool.clear();
            pool.reserve(scratch.res.len());
            for Reverse(item) in scratch.res.drain() {
                pool.push(item);
            }
            pool.sort_unstable_by(|a, b| b.cmp(a));
            scratch.cand.clear();
            view.select(view.inv_norm[node_u as usize], &pool, view.m0, &mut sel);
            let sel_recip_len = sel.len();
            let target_own = intra_sorted.len().min(view.m0);
            if sel.len() < target_own {
                for &intra_u in &intra_sorted {
                    if sel.len() >= target_own {
                        break;
                    }
                    if !sel.contains(&intra_u) {
                        sel.push(intra_u);
                    }
                }
            }

            // Always merge with any back-links already added by earlier/concurrent
            // grafts (in both sequential and parallel modes), reciprocating on the
            // direction-diversified `select` neighbors.
            merge_publish_layer0(builder, node_u, locks, &sel, &mut pool, scratch);
            for &e in &sel[..sel_recip_len] {
                builder.connect_back(e, node_u, 0, view.m0, locks, &mut pool, scratch);
            }

            scratch.fresh = cands;
            scratch.routes = step_nbrs;
            scratch.pool = pool;
            scratch.sel = sel;
        };

    if parallel {
        use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
        let locks = Locks::default();
        graft_tasks
            .par_iter()
            .for_each(|task| with_scratch(|scratch| graft_one(task, Some(&locks), scratch)));
    } else {
        with_scratch(|scratch| {
            for task in graft_tasks {
                graft_one(task, None, scratch);
            }
        });
    }
}

fn merge_publish_layer0(
    builder: &LinkBuilder<'_>,
    node: u32,
    locks: Option<&Locks>,
    sel: &[u32],
    pool: &mut Vec<Scored>,
    scratch: &mut Scratch,
) {
    let view = builder.view();
    let cap = view.m0;
    let start = view.list_start(node, 0);
    let cells = &builder.links.links0;
    let _guard = locks.map(|l| l.lock(node));
    let count = (cells[start].load(Atomic::Relaxed) as usize).min(cap);
    if count == 0 {
        cells[start].store(sel.len() as u32, Atomic::Relaxed);
        for (i, &v) in sel.iter().enumerate() {
            cells[start + 1 + i].store(v, Atomic::Relaxed);
        }
        return;
    }
    let mut merged = std::mem::take(&mut scratch.nbrs);
    merged.clear();
    merged.extend_from_slice(sel);
    for i in 0..count {
        let v = cells[start + 1 + i].load(Atomic::Relaxed);
        if v != node && !merged.contains(&v) {
            merged.push(v);
        }
    }
    if merged.len() > cap {
        let row = view.row(node);
        pool.clear();
        pool.reserve(merged.len());
        view.for_each_scored(row, &merged, |v, score| {
            pool.push(Scored { score, node: v });
        });
        pool.sort_unstable_by(|a, b| b.cmp(a));
        merged.clear();
        view.select(view.inv_norm[node as usize], pool, cap, &mut merged);
    }
    cells[start].store(merged.len() as u32, Atomic::Relaxed);
    for (i, &v) in merged.iter().enumerate() {
        cells[start + 1 + i].store(v, Atomic::Relaxed);
    }
    scratch.nbrs = merged;
}

/// Verify `PMHNSW01` entry/top invariants and run a directed forward BFS from
/// `entry` on layer 0, bridging any unreachable components into the main
/// component without orphaning already-reachable nodes (`#99`).
pub(super) fn repair_connectivity(builder: &LinkBuilder<'_>) -> usize {
    let n = builder.inv_norm.len();
    if n == 0 {
        return 0;
    }
    let max_lvl = builder.links.level.iter().copied().max().unwrap_or(0);
    let (mut entry, mut top) = builder.links.entry_state();
    if entry == NO_NODE
        || entry as usize >= n
        || builder.links.level[entry as usize] != max_lvl
        || top != max_lvl
    {
        let view = builder.view();
        let best_entry = (0..n as u32)
            .filter(|&u| builder.links.level[u as usize] == max_lvl)
            .max_by_key(|&u| {
                let deg = view.links0[view.list_start(u, 0)].load(Atomic::Relaxed);
                (deg, Reverse(u))
            })
            .expect("n > 0");
        builder.links.publish_entry(best_entry, max_lvl);
        entry = best_entry;
        top = max_lvl;
    }

    let mut visited = Bits::empty(n);
    // Spanning-tree parent on layer 0 (`bfs_parent[entry] = entry`). Protecting
    // tree edges `bfs_parent[v] -> v` from eviction prevents detaching mutual
    // cycles (`a <-> b`) that raw in-degree counting would miss.
    let mut bfs_parent = vec![NO_NODE; n];
    bfs_parent[entry as usize] = entry;
    visited.set(entry as usize);
    let mut visited_count = 1usize;
    let mut queue = Vec::with_capacity(n);
    queue.push(entry);
    let mut head = 0usize;
    let mut bfs_nbrs = Vec::with_capacity(builder.m0);

    let drain_bfs = |view: &View<'_, [AtomicU32]>,
                     visited: &mut Bits,
                     visited_count: &mut usize,
                     bfs_parent: &mut [u32],
                     queue: &mut Vec<u32>,
                     head: &mut usize,
                     bfs_nbrs: &mut Vec<u32>| {
        while *head < queue.len() {
            let u = queue[*head];
            *head += 1;
            view.copy_links(u, 0, None, bfs_nbrs);
            for &v in bfs_nbrs.iter() {
                let vi = v as usize;
                if vi < n && !visited.test(vi) {
                    visited.set(vi);
                    bfs_parent[vi] = u;
                    *visited_count += 1;
                    queue.push(v);
                }
            }
        }
    };

    {
        let view = builder.view();
        drain_bfs(
            &view,
            &mut visited,
            &mut visited_count,
            &mut bfs_parent,
            &mut queue,
            &mut head,
            &mut bfs_nbrs,
        );
    }

    if visited_count == n {
        return 0;
    }

    let mut islands_repaired = 0usize;
    let mut scan_cursor = 0usize;

    with_scratch(|scratch| {
        while visited_count < n {
            islands_repaired += 1;
            let view = builder.view();

            // Advance scan_cursor to the first unvisited node, then pick the
            // unvisited hub maximizing `(level[u], deg0(u), Reverse(u))` within
            // a bounded window (or whole scan when small) so repair stays fast.
            while scan_cursor < n && visited.test(scan_cursor) {
                scan_cursor += 1;
            }
            debug_assert!(scan_cursor < n);
            let window_end = n.min(scan_cursor.saturating_add(4096));
            let w = (scan_cursor..window_end)
                .filter(|&u| !visited.test(u))
                .map(|u| u as u32)
                .max_by_key(|&u| {
                    let deg = view.links0[view.list_start(u, 0)].load(Atomic::Relaxed);
                    (builder.links.level[u as usize], deg, Reverse(u))
                })
                .expect("scan_cursor is unvisited");

            let q = view.row(w);
            let mut step_nbrs = Vec::with_capacity(view.m0);
            let mut vis_nbrs = Vec::with_capacity(view.m0 * 2);
            view.copy_links(w, 0, None, &mut step_nbrs);

            // Fast path: when `w` is a leaf / duplicate-vector node whose own
            // layer-0 list already points into `visited`, gather its immediate
            // 1-to-2-hop `visited` neighborhood in O(m0) instead of running a
            // full corpus search from `entry`.
            let has_direct_visited = step_nbrs.iter().any(|&u| visited.test(u as usize));
            let pool_visited = if has_direct_visited {
                scratch.begin(n);
                scratch.visit(w);
                vis_nbrs.clear();
                let mut seeds = Vec::with_capacity(4);
                for &u in &step_nbrs {
                    if visited.test(u as usize) && scratch.visit(u) {
                        vis_nbrs.push(u);
                        if seeds.len() < 4 {
                            seeds.push(u);
                        }
                    }
                }
                let mut hop_buf = Vec::with_capacity(view.m0);
                for s in seeds {
                    if vis_nbrs.len() >= view.m0 * 2 {
                        break;
                    }
                    view.copy_links(s, 0, None, &mut hop_buf);
                    for &u in &hop_buf {
                        if vis_nbrs.len() >= view.m0 * 2 {
                            break;
                        }
                        if visited.test(u as usize) && scratch.visit(u) {
                            vis_nbrs.push(u);
                        }
                    }
                }
                let mut pv = Vec::with_capacity(vis_nbrs.len());
                view.for_each_scored(q, &vis_nbrs, |u, score| {
                    pv.push(Scored { score, node: u });
                });
                pv.sort_unstable_by(|a, b| b.cmp(a));
                let mut sel_w = step_nbrs.clone();
                for cand in pv.iter().take(4) {
                    try_connect_visited_to_island(builder, cand.node, w, &mut bfs_parent, scratch);
                }
                if bfs_parent[w as usize] == NO_NODE {
                    force_incoming_from_visited(builder, &pv, w, &mut sel_w, &mut bfs_parent);
                    if sel_w != step_nbrs {
                        builder.write_links(w, 0, &sel_w, None);
                    }
                }
                pv
            } else {
                // Full path for multi-node islands with zero outgoing edges into `visited`:
                // descend from `entry` over `visited` nodes to find the closest anchor.
                let mut ep = Scored {
                    score: view.kernel.dot(q, view.row(entry)),
                    node: entry,
                };
                for layer in (1..=usize::from(top)).rev() {
                    loop {
                        view.copy_links(ep.node, layer, None, &mut step_nbrs);
                        vis_nbrs.clear();
                        for &cand in &step_nbrs {
                            if visited.test(cand as usize) {
                                vis_nbrs.push(cand);
                            }
                        }
                        let mut improved = false;
                        view.for_each_scored(q, &vis_nbrs, |cand, s| {
                            if s > ep.score {
                                ep = Scored {
                                    score: s,
                                    node: cand,
                                };
                                improved = true;
                            }
                        });
                        if !improved {
                            break;
                        }
                    }
                }

                let mut stats = SearchStats::default();
                view.search_layer(
                    q,
                    &[ep],
                    builder.ef_construction.max(view.m0),
                    0,
                    Some(Mask::Bits(&visited)),
                    Patience::OFF,
                    FloorPrune::OFF,
                    None,
                    scratch,
                    &mut NoHook,
                    &mut stats,
                );
                let mut pv = scratch.drain_results();
                if pv.is_empty() {
                    pv.push(Scored {
                        score: view.kernel.dot(q, view.row(entry)),
                        node: entry,
                    });
                }

                view.copy_links(w, 0, None, &mut step_nbrs);
                let mut cands_w = Vec::with_capacity(step_nbrs.len() + pv.len());
                scratch.begin(n);
                scratch.visit(w);
                for &u in &step_nbrs {
                    if scratch.visit(u) {
                        cands_w.push(u);
                    }
                }
                for &item in &pv {
                    if scratch.visit(item.node) {
                        cands_w.push(item.node);
                    }
                }

                let mut pool_w = Vec::with_capacity(cands_w.len());
                view.for_each_scored(q, &cands_w, |u, score| {
                    pool_w.push(Scored { score, node: u });
                });
                pool_w.sort_unstable_by(|a, b| b.cmp(a));
                let mut sel_w = Vec::with_capacity(view.m0);
                view.select(view.inv_norm[w as usize], &pool_w, view.m0, &mut sel_w);
                if !sel_w.iter().any(|&u| visited.test(u as usize)) {
                    let best_vis = pv[0].node;
                    if sel_w.len() < view.m0 {
                        sel_w.push(best_vis);
                    } else if let Some(last) = sel_w.last_mut() {
                        *last = best_vis;
                    }
                }

                for &r in &sel_w {
                    if visited.test(r as usize) {
                        try_connect_visited_to_island(builder, r, w, &mut bfs_parent, scratch);
                    }
                }
                if bfs_parent[w as usize] == NO_NODE {
                    force_incoming_from_visited(builder, &pv, w, &mut sel_w, &mut bfs_parent);
                }
                builder.write_links(w, 0, &sel_w, None);
                pv
            };

            // Bridge upper layers `1..=level[w]` into `visited` upper-layer
            // nodes as well so greedy upper-layer descent routes directly into
            // the repaired component.
            let w_lvl = usize::from(builder.links.level[w as usize]);
            for layer in 1..=w_lvl {
                view.copy_links(w, layer, None, &mut step_nbrs);
                let mut upper_cands = step_nbrs.clone();
                for pv in &pool_visited {
                    let r = pv.node;
                    if r != w
                        && usize::from(builder.links.level[r as usize]) >= layer
                        && !upper_cands.contains(&r)
                    {
                        upper_cands.push(r);
                    }
                }
                if !upper_cands.iter().any(|&u| visited.test(u as usize))
                    && entry != w
                    && usize::from(top) >= layer
                {
                    upper_cands.push(entry);
                }
                if !upper_cands.is_empty() {
                    let mut up_pool = Vec::with_capacity(upper_cands.len());
                    view.for_each_scored(q, &upper_cands, |u, score| {
                        up_pool.push(Scored { score, node: u });
                    });
                    up_pool.sort_unstable_by(|a, b| b.cmp(a));
                    let mut up_sel = Vec::with_capacity(view.m);
                    view.select(view.inv_norm[w as usize], &up_pool, view.m, &mut up_sel);
                    builder.write_links(w, layer, &up_sel, None);
                    let mut cb_pool = Vec::new();
                    for &r in &up_sel {
                        if visited.test(r as usize) {
                            builder.connect_back(r, w, layer, view.m, None, &mut cb_pool, scratch);
                        }
                    }
                }
            }

            debug_assert_ne!(bfs_parent[w as usize], NO_NODE);
            if !visited.test(w as usize) {
                visited.set(w as usize);
                visited_count += 1;
                queue.push(w);
            }
            let refreshed = builder.view();
            drain_bfs(
                &refreshed,
                &mut visited,
                &mut visited_count,
                &mut bfs_parent,
                &mut queue,
                &mut head,
                &mut bfs_nbrs,
            );
        }
    });

    islands_repaired
}

/// Offer `w` to already-visited node `r`'s layer-0 list via `select`, committing
/// the update only if no spanning-tree child (`bfs_parent[old_u] == r`) is evicted.
fn try_connect_visited_to_island(
    builder: &LinkBuilder<'_>,
    r: u32,
    w: u32,
    bfs_parent: &mut [u32],
    scratch: &mut Scratch,
) {
    if r == w {
        return;
    }
    let view = builder.view();
    let cap = view.m0;
    let start = view.list_start(r, 0);
    let cells = &builder.links.links0;
    let count = (cells[start].load(Atomic::Relaxed) as usize).min(cap);
    if (0..count).any(|i| cells[start + 1 + i].load(Atomic::Relaxed) == w) {
        if bfs_parent[w as usize] == NO_NODE {
            bfs_parent[w as usize] = r;
        }
        return;
    }
    if count < cap {
        cells[start + 1 + count].store(w, Atomic::Relaxed);
        cells[start].store((count + 1) as u32, Atomic::Relaxed);
        if bfs_parent[w as usize] == NO_NODE {
            bfs_parent[w as usize] = r;
        }
        return;
    }

    let mut old_list = Vec::with_capacity(count + 1);
    for i in 0..count {
        old_list.push(cells[start + 1 + i].load(Atomic::Relaxed));
    }
    let mut cands = old_list.clone();
    cands.push(w);

    let row_r = view.row(r);
    let mut pool = std::mem::take(&mut scratch.pool);
    let mut sel = std::mem::take(&mut scratch.sel);
    pool.clear();
    pool.reserve(cands.len());
    view.for_each_scored(row_r, &cands, |u, score| {
        pool.push(Scored { score, node: u });
    });
    pool.sort_unstable_by(|a, b| b.cmp(a));
    view.select(view.inv_norm[r as usize], &pool, cap, &mut sel);

    let keeps_w = sel.contains(&w);
    let orphans_tree = old_list
        .iter()
        .any(|&old_u| !sel.contains(&old_u) && bfs_parent[old_u as usize] == r);
    if keeps_w && !orphans_tree {
        cells[start].store(sel.len() as u32, Atomic::Relaxed);
        for (i, &u) in sel.iter().enumerate() {
            cells[start + 1 + i].store(u, Atomic::Relaxed);
        }
        if bfs_parent[w as usize] == NO_NODE {
            bfs_parent[w as usize] = r;
        }
    }
    scratch.pool = pool;
    scratch.sel = sel;
}

/// Deterministically establish a spanning-tree edge `r -> w` from `pool_visited`
/// without detaching any spanning-tree subtree rooted under `entry`.
fn force_incoming_from_visited(
    builder: &LinkBuilder<'_>,
    pool_visited: &[Scored],
    w: u32,
    sel_w: &mut Vec<u32>,
    bfs_parent: &mut [u32],
) {
    let view = builder.view();
    let cap = view.m0;
    let cells = &builder.links.links0;

    // 1. Prefer the highest-scoring visited node with spare capacity (`< m0`).
    for cand in pool_visited {
        let r = cand.node;
        if r == w {
            continue;
        }
        let start = view.list_start(r, 0);
        let count = (cells[start].load(Atomic::Relaxed) as usize).min(cap);
        if (0..count).any(|i| cells[start + 1 + i].load(Atomic::Relaxed) == w) {
            bfs_parent[w as usize] = r;
            return;
        }
        if count < cap {
            cells[start + 1 + count].store(w, Atomic::Relaxed);
            cells[start].store((count + 1) as u32, Atomic::Relaxed);
            bfs_parent[w as usize] = r;
            return;
        }
    }

    // 2. Otherwise replace a non-tree edge (`bfs_parent[old_u] != r`) on the
    //    closest visited node, scanning from its lowest-ranked slot backwards.
    for cand in pool_visited {
        let r = cand.node;
        if r == w {
            continue;
        }
        let start = view.list_start(r, 0);
        let count = (cells[start].load(Atomic::Relaxed) as usize).min(cap);
        for slot in (0..count).rev() {
            let old_u = cells[start + 1 + slot].load(Atomic::Relaxed);
            if bfs_parent[old_u as usize] != r {
                cells[start + 1 + slot].store(w, Atomic::Relaxed);
                bfs_parent[w as usize] = r;
                return;
            }
        }
    }

    // 3. Fallback when every candidate is full and all `m0` slots are spanning-tree
    //    children of `r`: splice `r -> w -> old_u` and update `bfs_parent` so the
    //    spanning tree remains intact.
    let r = pool_visited[0].node;
    let start = view.list_start(r, 0);
    let count = (cells[start].load(Atomic::Relaxed) as usize).min(cap);
    if count > 0 {
        let slot = count - 1;
        let old_u = cells[start + 1 + slot].load(Atomic::Relaxed);
        cells[start + 1 + slot].store(w, Atomic::Relaxed);
        bfs_parent[w as usize] = r;
        if old_u != w {
            bfs_parent[old_u as usize] = w;
            if !sel_w.contains(&old_u) {
                if sel_w.len() < cap {
                    sel_w.push(old_u);
                } else if let Some(pos) = sel_w.iter().rposition(|&x| x != r) {
                    sel_w[pos] = old_u;
                }
            }
        }
    }
}

//! Assembly of one dense output index from caller-ordered rows: row copy and
//! provenance, donor choice, link allocation, and the strategy's linking.

use std::cmp::Reverse;
use std::sync::atomic::AtomicU32;

use crate::vector::{first_invalid_coordinate, VectorError, VectorProvider};

use super::super::config::{BuildMode, HnswConfig};
use super::super::distance::Kernel;
use super::super::graph::{
    level_for, GraphLinks, LinkBuilder, Store, MAX_LEVEL, MAX_ROWS, NO_NODE,
};
use super::super::{Backing, HnswIndex, HNSW, MAX_DIM, MIN_GRAPH_ROWS, PARALLEL_MIN_BATCH};
use super::donor::{inherit_deleted_entry_highways, select_donor_entry, DonorMap};
use super::donor_heal::transplant_and_heal_donor;
use super::join_set::join_set_merge;
use super::{flat_rows, repair_connectivity, MergeStats, MergeStrategy, OrderedMergeRow};

/// Minimum fraction of a source segment's physical rows that must survive into
/// an output image for that segment's graph edges to be reused as a donor or
/// join-set source (`0.35` covers up to `65%` tombstones while routing sparse
/// multi-bucket hash sub-samples to a clean build).
const MIN_SEGMENT_SURVIVAL_RATIO: f64 = 0.35;

/// Minimum surviving rows required from a linked source segment before reusing
/// its graph structure.
const MIN_QUALIFYING_SEGMENT_ROWS: usize = 64;

/// Where each output row came from and what each source contributed.
struct Provenance<'a> {
    sources: &'a [&'a HnswIndex],
    /// The flat rows of every source whose graph the output may reuse.
    reusable_rows: Vec<Option<&'a [f32]>>,
    /// Per output slot: `(segment, source node)` when the row is bit for bit
    /// a row of a reusable source.
    row_sources: Vec<Option<(usize, u32)>>,
    /// Per source: output rows it contributed.
    surviving: Vec<usize>,
}

/// The sources of one merge once a donor is chosen.
pub(super) struct MergePlan<'a> {
    pub(super) sources: &'a [&'a HnswIndex],
    pub(super) row_sources: &'a [Option<(usize, u32)>],
    /// Per source: its graph structure is reused.
    pub(super) qualifies: &'a [bool],
    pub(super) donor_seg: usize,
    pub(super) build_mode: BuildMode,
    pub(super) strategy: MergeStrategy,
}

impl MergePlan<'_> {
    /// Whether a batch of `len` rows is worth forking under this build mode.
    pub(super) fn parallel(&self, len: usize) -> bool {
        self.build_mode == BuildMode::Parallel && len >= PARALLEL_MIN_BATCH
    }
}

/// Build a dense [`HnswIndex`] over `rows` in exact caller-specified slot
/// order, reusing graph structure from `sources` according to `strategy`.
/// With `allow_any_donor_survival` a linked source qualifies as donor
/// however few of its rows survive.
pub(super) fn merge_ordered_inner(
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
    let m = config.m as usize;
    let mut store = Store::new(dim, m, config.ef_construction as usize, config.seed, kernel);
    let mut stats = MergeStats {
        output_rows: n,
        ..Default::default()
    };
    if n > 0 {
        let provenance = copy_rows(&mut store, &config, sources, rows)?;
        // An output below `MIN_GRAPH_ROWS` stays unlinked.
        if n >= MIN_GRAPH_ROWS {
            let donor = (strategy != MergeStrategy::FullRebuild)
                .then(|| choose_donor(&provenance, allow_any_donor_survival))
                .flatten();
            link_output(
                &mut store,
                &config,
                strategy,
                &provenance,
                donor,
                &mut stats,
            )?;
        }
    }
    let index = HnswIndex {
        dim,
        config,
        kernel,
        backing: Backing::Heap(store),
    };
    Ok((index, stats))
}

/// Copy the output rows into `store`, validating their shape and
/// coordinates, and record which of them a reusable source graph holds.
fn copy_rows<'a>(
    store: &mut Store,
    config: &HnswConfig,
    sources: &'a [&'a HnswIndex],
    rows: &[OrderedMergeRow<'_>],
) -> Result<Provenance<'a>, VectorError> {
    let (dim, n) = (store.dim, rows.len());
    store.rows.reserve(n * dim);
    store.inv_norm.reserve(n);
    let reusable_rows: Vec<Option<&[f32]>> = sources
        .iter()
        .map(|src| {
            let reusable = src.dim == dim && src.config.m == config.m && src.is_graph_linked();
            reusable.then(|| flat_rows(src))
        })
        .collect();
    let mut provenance = Provenance {
        sources,
        reusable_rows,
        row_sources: Vec::with_capacity(n),
        surviving: vec![0usize; sources.len()],
    };
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
        let norm = store.kernel.dot(vec, vec).sqrt();
        store.rows.push(vec);
        store.inv_norm.push(if norm > 0.0 && norm.is_finite() {
            1.0 / norm
        } else {
            0.0
        });
        let source = row_source(item, sources, &provenance.reusable_rows, dim);
        if let Some((segment, _)) = source {
            provenance.surviving[segment] += 1;
        }
        provenance.row_sources.push(source);
    }
    Ok(provenance)
}

/// The `(segment, source node)` of `item` when a reusable source holds
/// exactly its coordinates at the node its row maps to.
fn row_source(
    item: &OrderedMergeRow<'_>,
    sources: &[&HnswIndex],
    reusable_rows: &[Option<&[f32]>],
    dim: usize,
) -> Option<(usize, u32)> {
    let OrderedMergeRow::FromSegment {
        segment,
        old_node,
        vector,
    } = *item
    else {
        return None;
    };
    let src_flat = (*reusable_rows.get(segment)?)?;
    if old_node as usize >= sources[segment].len() {
        return None;
    }
    let donor_node = sources[segment].row_to_node(old_node);
    let start = donor_node as usize * dim;
    (&src_flat[start..start + dim] == vector).then_some((segment, donor_node))
}

/// Which linked sources contribute enough surviving rows for their graph
/// structure to be reused, and the donor among them: the one with the most
/// surviving rows, the earliest on a tie. `None` when no source qualifies.
fn choose_donor(
    provenance: &Provenance<'_>,
    allow_any_donor_survival: bool,
) -> Option<(Vec<bool>, usize)> {
    let (sources, surviving) = (provenance.sources, &provenance.surviving);
    let qualifies: Vec<bool> = (0..sources.len())
        .map(|s| {
            provenance.reusable_rows[s].is_some()
                && surviving[s] > 0
                && (allow_any_donor_survival
                    || (surviving[s] >= MIN_QUALIFYING_SEGMENT_ROWS
                        && (surviving[s] as f64)
                            >= (sources[s].len() as f64) * MIN_SEGMENT_SURVIVAL_RATIO))
        })
        .collect();
    let donor = (0..sources.len())
        .filter(|&s| qualifies[s])
        .max_by_key(|&s| (surviving[s], Reverse(s)))?;
    Some((qualifies, donor))
}

/// Link the copied rows of `store`. Without a `donor` the graph is built
/// fresh over the output rows; with one, the donor is transplanted and
/// healed and the remaining rows are linked by `strategy`.
fn link_output(
    store: &mut Store,
    config: &HnswConfig,
    strategy: MergeStrategy,
    provenance: &Provenance<'_>,
    donor: Option<(Vec<bool>, usize)>,
    stats: &mut MergeStats,
) -> Result<(), VectorError> {
    let n = store.n();
    let Some((qualifies, donor_seg)) = donor else {
        let parallel = config.build_mode == BuildMode::Parallel && n >= PARALLEL_MIN_BATCH;
        let links = store.build_links(parallel)?;
        stats.islands_repaired = repair_connectivity(&LinkBuilder::new(store, &links));
        store.install_links(links);
        stats.reinserted_rows = n;
        stats.used_no_donor_rebuild = strategy != MergeStrategy::FullRebuild;
        return Ok(());
    };
    stats.donor_segment = Some(donor_seg);
    stats.donor_rows = provenance.surviving[donor_seg];
    let plan = MergePlan {
        sources: provenance.sources,
        row_sources: &provenance.row_sources,
        qualifies: &qualifies,
        donor_seg,
        build_mode: config.build_mode,
        strategy,
    };
    let links = allocate_links(store, &plan)?;
    link_from_donor(&LinkBuilder::new(store, &links), &plan, stats);
    store.install_links(links);
    Ok(())
}

/// The level array of a source whose graph is reused.
fn source_levels(index: &HnswIndex) -> &[u8] {
    match &index.backing {
        Backing::Heap(store) => store.links().expect("qualified linked").level.as_slice(),
        Backing::Mapped(mapped) => mapped.view().level,
    }
}

/// Allocate the output's empty link arrays. A row from a qualifying source
/// keeps its source level, so the upper-layer edge invariant
/// (`level[nbr] >= layer`) holds on the donor and upper-layer hubs carry
/// over into non-donor join sets; every other row draws its level.
fn allocate_links(store: &Store, plan: &MergePlan<'_>) -> Result<GraphLinks, VectorError> {
    let n = store.n();
    let levels: Vec<Option<&[u8]>> = (0..plan.sources.len())
        .map(|s| plan.qualifies[s].then(|| source_levels(plan.sources[s])))
        .collect();
    let stride0 = store.m0 + 1;
    let stride_up = store.m + 1;
    let mut level = Vec::with_capacity(n);
    let mut upper_slot = Vec::with_capacity(n);
    let mut upper_base = Vec::new();
    let mut upper_cells = 0usize;
    for (new_u, source) in plan.row_sources.iter().enumerate() {
        let lvl = match *source {
            Some((seg, old_u)) if plan.qualifies[seg] => {
                levels[seg].expect("qualified")[old_u as usize].min(MAX_LEVEL)
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
    Ok(GraphLinks::from_parts(
        level, upper_slot, upper_base, links0, upper, NO_NODE, 0,
    ))
}

/// Transplant and heal the donor's edges, publish an entry over them, link
/// every non-donor row by the plan's strategy, and repair layer-0
/// reachability from the entry.
fn link_from_donor(builder: &LinkBuilder<'_>, plan: &MergePlan<'_>, stats: &mut MergeStats) {
    let donor_index = plan.sources[plan.donor_seg];
    let mut to_new = vec![NO_NODE; donor_index.len()];
    let mut pairs: Vec<(u32, u32)> = Vec::with_capacity(stats.donor_rows);
    for (new_u, source) in plan.row_sources.iter().enumerate() {
        if let Some((seg, old_u)) = *source {
            if seg == plan.donor_seg {
                to_new[old_u as usize] = new_u as u32;
                pairs.push((old_u, new_u as u32));
            }
        }
    }
    let donor = DonorMap {
        to_new: &to_new,
        pairs: &pairs,
    };
    let parallel = plan.parallel(pairs.len());
    stats.donor_healed_lists = with_view!(donor_index, |donor_view| {
        transplant_and_heal_donor(&donor_view, builder, &donor, parallel)
    });

    // Publish `(entry, top)` over the transplanted donor subgraph so later
    // `LinkBuilder` insertions descend a valid hierarchy and raise `top`
    // whenever a non-donor node has `level > donor_top`.
    let initial_entry = select_donor_entry(&builder.view(), donor_index, &donor);
    let initial_top = builder.links.level[initial_entry as usize];
    builder.links.publish_entry(initial_entry, initial_top);
    with_view!(donor_index, |donor_view| {
        inherit_deleted_entry_highways(&donor_view, builder, donor.to_new, initial_entry);
    });

    match plan.strategy {
        MergeStrategy::FullRebuild => unreachable!("a full rebuild picks no donor"),
        MergeStrategy::DonorReinsert => {
            let non_donor: Vec<u32> = plan
                .row_sources
                .iter()
                .enumerate()
                .filter(|(_, source)| !matches!(source, Some((seg, _)) if *seg == plan.donor_seg))
                .map(|(new_u, _)| new_u as u32)
                .collect();
            builder.insert_slice(&non_donor, plan.parallel(non_donor.len()));
            stats.reinserted_rows = non_donor.len();
        }
        MergeStrategy::JoinSet => join_set_merge(builder, plan, stats),
    }
    stats.islands_repaired = repair_connectivity(builder);
}

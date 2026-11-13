//! The `hnsw` vector provider: a from-scratch Hierarchical Navigable Small
//! World graph over FP32 rows, scored by inner product.
//!
//! The algorithms are those of Malkov & Yashunin, "Efficient and robust
//! approximate nearest neighbor search using Hierarchical Navigable Small
//! World graphs" (arXiv:1603.09320); see `NOTICE`. Nothing is ported from
//! another implementation. `docs/hnsw.md` is the operator-facing
//! description; this module's submodules are:
//!
//! - [`config`]: the protobuf-carried build parameters and the fingerprint
//!   contract.
//! - [`distance`]: single-row and multi-row inner-product kernels with
//!   runtime dispatch.
//! - [`graph`]: the flat layered graph, insertion and the walk.
//! - [`image`]: the persisted format, loaded onto the heap or served from
//!   a memory map.
//! - [`rows`]: the FP32 rows behind the [`RowSource`] trait, with the
//!   identity a shared row store must match (#71).
//!
//! The provider advertises [`QualityContract::ConfiguredAnn`] without
//! exhaustive completion: `EXACT` refuses it, `ANN` runs it, and `AUTO`
//! admits it only through a measured dense execution policy bound to its
//! scoring fingerprint.
//!
//! Numerical contract (`docs/hnsw.md`, "Scores"): the SIMD kernels steer the
//! traversal only. Every row the provider reports, in a unary result or a
//! streamed batch, is rescored with [`crate::exact_vectors::dot`], the
//! scalar row-order accumulation, before it leaves the provider, so reported
//! scores are bit for bit the product's exact scores on every kernel and
//! host. Ties order by score descending, then row ascending.

pub mod config;
pub mod distance;
pub mod graph;
pub mod image;
pub mod merge;
pub mod reorder;
pub mod rows;
#[cfg(test)]
mod tests;

use std::ops::{ControlFlow, Range};
use std::path::Path;

pub use config::{BuildMode, HnswConfig, LocalityOrder, CONFIG_FORMAT};
pub use distance::Kernel;
pub use graph::SearchStats;
pub use merge::{MergeStats, MergeStrategy, SegmentMergeInput, JOIN_DOMINATION_K};
pub use reorder::LocalityMetrics;
pub use reorder::NodeMap;
pub use rows::{RowSource, RowSourceIdentity};

use crate::vector::{
    first_invalid_coordinate, FilterRegime, FloorPruneSpec, Mask, PatienceSpec, PatienceWindow,
    QualityContract, ScoreDirection, VectorBackendConfig, VectorBackendDescriptor,
    VectorCapability, VectorError, VectorIndex, VectorProvider, VectorSearchOptions,
    VectorSearchResults, VectorStreamBatch, VectorStreamControl, VectorStreamEndReason,
    VectorStreamSummary,
};

use distance::prefetch;
use graph::{
    with_scratch, Filter, FloorHook, FloorPrune, Hook, Patience, Route, Scored, Store, Walk,
};

/// Stable identifier of the backend, as `vector_backend` names it.
pub const HNSW: &str = "hnsw";
const BACKEND_VERSION: &str = "1.0.0";
/// Largest dimension an image may declare.
pub const MAX_DIM: usize = 1 << 16;
/// The smallest beam a query walks with. The harness's breadth sweep
/// starts here, so a product query never runs a beam narrower than one the
/// gate measured; below it the walk degenerates toward a greedy descent.
pub const EF_FLOOR: usize = 16;
/// Minimum row count at which `prepare` / `write` builds a graph
/// (`docs/hnsw.md`, "Unlinked tail and tiny segments"). Below 256 rows,
/// sequential SIMD enumeration achieves 100% exact recall faster than a
/// realistic beam walk (`ef >= 16..32` crosses `enumerate` at `256..768`
/// rows on AVX-512F and NEON) while avoiding graph construction and saving
/// `141` bytes/row in `PMHNSW01` images (`entry = NO_NODE`).
pub const MIN_GRAPH_ROWS: usize = 256;
/// A parallel-mode batch at least this large is inserted from every
/// thread; smaller batches are not worth the fork.
const PARALLEL_MIN_BATCH: usize = 256;
/// Streaming emission batch: rows scored before the sink is called.
const STREAM_BATCH_ROWS: usize = 64;
/// Hops before `StreamHook` emits its early first-flush batch so the
/// coordinator establishes a global floor without waiting for 64 rows.
const STREAM_FIRST_FLUSH_HOPS: usize = 2;
/// Minimum buffered rows required for the early first-flush after
/// [`STREAM_FIRST_FLUSH_HOPS`] layer-0 hops.
const STREAM_FIRST_FLUSH_ROWS: usize = 8;
/// Rows the enumerate regime streams between polls of the control.
const STREAM_POLL_ROWS: usize = 16;
/// Enumerate wins while the allowed rows cost less to score in row order
/// than a walk costs. A filter-aware walk scores on the order of `ef · m0`
/// rows, each behind a cache miss, and bridges through the disallowed ones;
/// a row scored in mask order costs a fraction of that. The factor is the
/// measured ratio on the challenge fixture (`docs/hnsw.md`, "Filters"); a
/// dense execution policy will carry the thresholds measured per host
/// class (#55).
const ENUMERATE_ROWS_PER_WALK_ROW: usize = 4;
/// A plain walk does not bridge: on the challenge fixture (M = 16, d = 64)
/// it costs about `1.2 · ef · m0` mask-ordered rows (near-10 at ef 96:
/// 106 µs walking plain, 295 µs enumerating its 10,252 allowed rows). Up to
/// `ef · m0` allowed rows enumerate before the far probe runs; above that
/// the probe decides (`ENUMERATE_ROWS_PER_WALK_ROW`, `PLAIN_LOCAL_SHARE`).
const ENUMERATE_ROWS_PER_PLAIN_ROW: usize = 1;
/// The far probe walks plain when at least this share of the rows around
/// the query is allowed. Measured on the challenge fixture with pinned
/// regimes (uncorrelated masks, M = 16): the filter-aware walk wins at a
/// 10% share (1.4x at k = 10, 1.08x at k = 100), plain wins from 20% at
/// k = 100 (1.4x at 20%, 1.8x at 30%, 2.15x at 50%) and is level at k = 10.
/// A mask gathered around the query (near-10, near-50) is mostly allowed
/// locally whatever its global share, and plain wins there too.
const PLAIN_LOCAL_SHARE: f64 = 0.2;
/// Measured mask-order `enumerate` rows per layer-0 walk work unit
/// (`layer0_scored + bridges`, `docs/hnsw.md`, "Filters"): across
/// AVX2/AVX-512F and NEON (`d = 64` and `d = 256`), one random walk or bridge
/// expansion costs ~3.0–3.6× one sequential mask-ordered row in `enumerate`.
/// At `allowed = 4 · ef · m0` this sets a layer-0 budget of `1.14 · ef · m0`
/// work units (above a healthy walk's layer-0 work), and scales
/// proportionally with a policy point's `enumerate_max_rows`.
const WALK_WORK_ENUMERATE_ROWS: f64 = 3.5;

/// How one query was answered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Answer {
    /// The regime that answered; `None` for an unfiltered walk. A
    /// filter-aware walk that gave the query up reports enumerate.
    pub regime: Option<FilterRegime>,
    pub stats: SearchStats,
    /// True when a filter-aware walk spent its visit budget and handed the
    /// remaining allowed rows to enumerate (`regime` is `Enumerate`).
    pub budget_handoff: bool,
    /// True when the layer-0 walk stopped early after its tracked window
    /// stayed saturated for the configured patience hops.
    pub patience_stopped: bool,
    /// True when the layer-0 walk pruned candidates against an active score
    /// floor (`#72`).
    pub floor_pruned: bool,
    /// Rows scored by enumeration because this index was unlinked (heap tail,
    /// frozen tail, or sealed segment below [`MIN_GRAPH_ROWS`]).
    pub tail_enumerated_rows: u64,
}

/// The mask of one search with what the regimes need of it.
#[derive(Clone, Copy)]
struct Plan<'a> {
    regime: Option<FilterRegime>,
    allow: Option<Mask<'a>>,
    /// Rows the mask allows.
    allowed: usize,
    /// The provider chose the regime, so the far probe routes a filter-aware
    /// plan to plain or enumerate; a pinned regime runs as pinned (`None`).
    route: Option<Route>,
    /// Work-unit cap before a filter-aware walk hands the remaining allowed
    /// rows to enumerate (`None` when pinned).
    budget: Option<u64>,
    patience: PatienceSpec,
    /// Clamped local allowed-row gate when `#72` floor pruning is enabled.
    floor_prune_gate: Option<usize>,
}

impl Plan<'_> {
    const UNFILTERED: Plan<'static> = Plan {
        regime: None,
        allow: None,
        allowed: 0,
        route: None,
        budget: None,
        patience: PatienceSpec::OFF,
        floor_prune_gate: None,
    };

    fn filter(&self) -> Filter<'_> {
        match (self.regime, self.allow) {
            (Some(FilterRegime::FilterAware), Some(allow)) => Filter::Aware {
                allow,
                allowed: self.allowed,
                route: self.route,
                budget: self.budget,
            },
            (Some(FilterRegime::Plain), Some(allow)) => Filter::Plain(allow),
            _ => Filter::None,
        }
    }

    fn patience(&self, k: usize, floor: f32) -> Patience {
        if self.patience.patience == 0 {
            return Patience::OFF;
        }
        let top_k = match self.patience.window {
            PatienceWindow::Beam => 0,
            PatienceWindow::TopK => k.max(1),
        };
        Patience {
            saturation: self.patience.saturation,
            saturation_ppm: self.patience.saturation_ppm,
            hops: self.patience.patience,
            top_k,
            ascent_gate: self
                .patience
                .ascent_gate
                .max(self.floor_prune_gate.unwrap_or(0)),
            floor_active: floor.is_finite(),
        }
    }

    fn floor_prune(&self, slack_per_norm: f64) -> FloorPrune {
        match self.floor_prune_gate {
            Some(gate) => FloorPrune {
                enabled: true,
                gate,
                slack_per_norm,
            },
            None => FloorPrune::OFF,
        }
    }
}

enum Backing {
    Heap(Store),
    Mapped(image::Mapped),
}

/// Dispatch over the two link storages: `$v` is a `graph::View`.
macro_rules! with_view {
    ($self:expr, |$v:ident| $body:expr) => {
        match &$self.backing {
            Backing::Heap(store) => {
                let $v = store.view();
                $body
            }
            Backing::Mapped(mapped) => {
                let $v = mapped.view();
                $body
            }
        }
    };
}

/// The provider.
pub struct HnswIndex {
    dim: usize,
    config: HnswConfig,
    kernel: Kernel,
    backing: Backing,
}

impl std::fmt::Debug for HnswIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HnswIndex")
            .field("dim", &self.dim)
            .field("config", &self.config)
            .field("kernel", &self.kernel.name())
            .field("rows", &self.len())
            .field("mapped", &self.is_mapped())
            .finish()
    }
}

impl HnswIndex {
    /// An empty graph for rows of `dim` values.
    pub fn new(dim: usize, config: HnswConfig) -> Result<Self, VectorError> {
        config.validate()?;
        if dim == 0 || dim > MAX_DIM {
            return Err(VectorError::new(format!(
                "{HNSW} dimension {dim} is outside 1..={MAX_DIM}"
            )));
        }
        let kernel = Kernel::detect();
        Ok(Self {
            dim,
            config,
            kernel,
            backing: Backing::Heap(Store::new(
                dim,
                config.m as usize,
                config.ef_construction as usize,
                config.seed,
                kernel,
            )),
        })
    }

    /// Read an image onto the heap.
    pub fn load(path: &Path) -> Result<Self, VectorError> {
        let kernel = Kernel::detect();
        let (store, config) = image::load_heap(path, kernel)?;
        Ok(Self {
            dim: store.dim,
            config,
            kernel,
            backing: Backing::Heap(store),
        })
    }

    /// Serve an image from a memory map; the graph and its rows are read
    /// in place. The index is read-only.
    pub fn load_mapped(path: &Path) -> Result<Self, VectorError> {
        let kernel = Kernel::detect();
        let mapped = image::load_mapped(path, kernel)?;
        Ok(Self {
            dim: mapped.header().dim,
            config: mapped.header().config,
            kernel,
            backing: Backing::Mapped(mapped),
        })
    }

    pub fn into_index(self) -> VectorIndex {
        VectorIndex::from_provider(self)
    }

    pub fn config(&self) -> &HnswConfig {
        &self.config
    }

    pub fn kernel(&self) -> Kernel {
        self.kernel
    }

    /// Bytes the image of this graph takes, per row; `None` while empty.
    pub fn image_bytes_per_row(&self) -> Option<f64> {
        let n = self.len();
        if n == 0 {
            return None;
        }
        let bytes = match &self.backing {
            Backing::Heap(store) => store.heap_bytes(),
            Backing::Mapped(mapped) => mapped.byte_len(),
        };
        Some(bytes as f64 / n as f64)
    }

    /// Run `f` over the internal node-ordered rows the graph traverses.
    pub fn with_row_source<R>(&self, f: impl FnOnce(&dyn RowSource) -> R) -> R {
        match &self.backing {
            Backing::Heap(store) => f(&store.rows),
            Backing::Mapped(mapped) => f(&mapped.rows()),
        }
    }

    /// True when this index carries a seal-time `node <-> row` permutation.
    pub fn has_node_map(&self) -> bool {
        self.node_to_row_slice().is_some()
    }

    /// Structural layer-0 locality summary (`#98`).
    pub fn locality_metrics(&self) -> LocalityMetrics {
        with_view!(self, |v| reorder::compute_locality_metrics(&v))
    }

    /// Count distinct 4 KB vector-row pages touched during an unfiltered
    /// layer-0 walk at beam `ef` (`#98`).
    pub fn walk_row_4k_pages(&self, query: &[f32], ef: usize) -> usize {
        with_view!(self, |v| reorder::walk_row_4k_pages(&v, query, ef))
    }

    /// Disable OS readahead and unmap active PTEs on a memory-mapped `.hnsw`
    /// image (`MADV_RANDOM` + `MADV_DONTNEED`) so subsequent queries measure
    /// cold page-table / page-cache fault behavior (`#98`).
    pub fn advise_cold_mmap(&self) -> Result<(), VectorError> {
        match &self.backing {
            Backing::Mapped(mapped) => mapped.advise_cold(),
            Backing::Heap(_) => Ok(()),
        }
    }

    /// Evict clean kernel page-cache pages backing `path` (`POSIX_FADV_DONTNEED`).
    pub fn drop_file_page_cache(path: &Path) -> Result<(), VectorError> {
        image::drop_file_page_cache(path)
    }

    /// Optional `node_to_row[node] = ext_row` permutation slice.
    pub fn node_to_row_slice(&self) -> Option<&[u32]> {
        match &self.backing {
            Backing::Heap(store) => store.node_map().map(|m| m.node_to_row.as_slice()),
            Backing::Mapped(mapped) => mapped.node_to_row(),
        }
    }

    /// Optional `row_to_node[ext_row] = node` permutation slice.
    pub fn row_to_node_slice(&self) -> Option<&[u32]> {
        match &self.backing {
            Backing::Heap(store) => store.node_map().map(|m| m.row_to_node.as_slice()),
            Backing::Mapped(mapped) => mapped.row_to_node(),
        }
    }

    #[inline]
    pub(crate) fn row_to_node(&self, ext_row: u32) -> u32 {
        self.row_to_node_slice()
            .map_or(ext_row, |m| m[ext_row as usize])
    }

    /// The identity a shared row store must match to replace the
    /// provider's own rows (#71): dimension, row count, content digest in
    /// external row order `0..self.len()`.
    pub fn row_identity(&self) -> RowSourceIdentity {
        let Some(r2n) = self.row_to_node_slice() else {
            return self.with_row_source(|rows| rows.identity());
        };
        self.with_row_source(|rows| {
            let mut sha = crate::sha256::Sha256::new();
            let mut bytes = Vec::with_capacity(4 * self.dim.max(256));
            for &node in r2n {
                bytes.clear();
                for &v in rows.row(node) {
                    bytes.extend_from_slice(&v.to_le_bytes());
                }
                sha.update(&bytes);
            }
            RowSourceIdentity {
                dim: rows.dim(),
                rows: rows.row_count(),
                content_digest: sha.finalize(),
            }
        })
    }

    /// Permute this heap index in-place for a global segment compaction remap
    /// (`#98`), leaving `node_map` absent (`0 B/row` overhead) and returning
    /// `new_to_old` (`new_slot -> old_slot`) when reordering ran.
    pub(crate) fn reorder_global_in_place(&mut self) -> Result<Option<Vec<u32>>, VectorError> {
        let order = self.config.locality_order;
        if order == LocalityOrder::None {
            return Ok(None);
        }
        let Backing::Heap(store) = &mut self.backing else {
            return Err(VectorError::new(format!(
                "{HNSW} mapped image cannot be reordered in place"
            )));
        };
        if store.n() < MIN_GRAPH_ROWS {
            return Ok(None);
        }
        if !store.is_linked() {
            let parallel =
                self.config.build_mode == BuildMode::Parallel && store.n() >= PARALLEL_MIN_BATCH;
            let links = store.build_links(parallel)?;
            store.install_links(links);
        }
        if store.is_reordered() && store.node_map().is_none() {
            return Ok(None);
        }
        // If `store` already held a local `node_map`, drop its map and return
        // its existing `node_to_row` directly without permuting twice.
        if store.is_reordered() {
            if let Some(prev) = store.take_node_map() {
                store.set_reorder_state(true, None);
                return Ok(Some(prev.node_to_row));
            }
        }
        let new_to_old = reorder::compute_locality_order(&store.view(), order);
        let links = store.links().expect("linked above");
        let permuted = reorder::permute_store_with_links(store, links, &new_to_old, false);
        *store = permuted;
        Ok(Some(new_to_old))
    }

    /// Apply a vector-local `NodeMap` permutation in-place on a linked heap
    /// store when `config.locality_order != None` and not yet reordered.
    pub(crate) fn reorder_local_in_place(&mut self) {
        let order = self.config.locality_order;
        if order == LocalityOrder::None {
            return;
        }
        if let Backing::Heap(store) = &mut self.backing {
            if store.is_linked() && !store.is_reordered() && store.n() >= MIN_GRAPH_ROWS {
                let new_to_old = reorder::compute_locality_order(&store.view(), order);
                let links = store.links().expect("linked");
                *store = reorder::permute_store_with_links(store, links, &new_to_old, true);
            }
        }
    }

    /// Produce a permuted copy of a linked heap index with the exact same
    /// graph topology under `locality_order` (for controlled A/B benchmarking
    /// and equivalence verification).
    pub fn permute_from_linked(
        &self,
        locality_order: LocalityOrder,
        attach_node_map: bool,
    ) -> Result<(Self, Vec<u32>), VectorError> {
        let Backing::Heap(store) = &self.backing else {
            return Err(VectorError::new(format!(
                "permute_from_linked requires a heap-backed {HNSW} index"
            )));
        };
        let Some(links) = store.links() else {
            return Err(VectorError::new(format!(
                "permute_from_linked requires a linked {HNSW} index"
            )));
        };
        let mut config = self.config;
        config.locality_order = locality_order;
        let new_to_old = reorder::compute_locality_order(&store.view(), locality_order);
        let out_store = if locality_order == LocalityOrder::None {
            let identity: Vec<u32> = (0..store.n() as u32).collect();
            let mut cloned = reorder::permute_store_with_links(store, links, &identity, false);
            cloned.set_reorder_state(false, None);
            cloned
        } else {
            reorder::permute_store_with_links(store, links, &new_to_old, attach_node_map)
        };
        Ok((
            Self {
                dim: self.dim,
                config,
                kernel: self.kernel,
                backing: Backing::Heap(out_store),
            },
            new_to_old,
        ))
    }

    /// Compact this index to the rows allowed by `live`, healing broken edges
    /// across deleted neighbors via 2-hop surviving bridges (`#76`) and
    /// verifying/repairing directed layer-0 reachability from `entry` (`#99`).
    /// Returns `(compacted_index, survivor_old_ids, stats)`.
    pub fn heal_tombstones(
        &self,
        live: Mask<'_>,
    ) -> Result<(Self, Vec<u32>, MergeStats), VectorError> {
        merge::heal_tombstones(self, live)
    }

    /// Merge `segments` (each filtered by its optional `live` mask) plus
    /// optional `unlinked_vectors` into one dense [`HnswIndex`] using
    /// `strategy` (`#76`, `#99`).
    pub fn merge_segments(
        dim: usize,
        config: HnswConfig,
        segments: &[SegmentMergeInput<'_>],
        unlinked_vectors: &[f32],
        strategy: MergeStrategy,
    ) -> Result<(Self, MergeStats), VectorError> {
        merge::merge_segments(dim, config, segments, unlinked_vectors, strategy)
    }

    /// Count nodes in `0..self.len()` that are not forward-reachable from
    /// `entry` via directed layer-0 edges (`0` when unlinked or fully
    /// connected).
    pub fn unreachable_from_entry(&self) -> usize {
        merge::unreachable_from_entry(self)
    }

    pub(crate) fn merge_ordered(
        dim: usize,
        config: HnswConfig,
        sources: &[&Self],
        rows: &[merge::OrderedMergeRow<'_>],
        strategy: MergeStrategy,
    ) -> Result<(Self, MergeStats), VectorError> {
        merge::merge_ordered(dim, config, sources, rows, strategy)
    }

    /// Rescore the walk's results with the scalar scorer, drop those below
    /// the floor, map internal nodes back to external row IDs when a seal-time
    /// node map is present, and order them score descending then row ascending.
    /// This is the only path a unary result takes out of the provider.
    fn report(
        hits: Vec<Scored>,
        query: &[f32],
        rows: &dyn RowSource,
        floor: f32,
        k: usize,
        node_to_row: Option<&[u32]>,
    ) -> Vec<(u32, f32)> {
        let mut exact: Vec<(u32, f32)> = hits
            .into_iter()
            .map(|h| {
                let ext_row = node_to_row.map_or(h.node, |m| m[h.node as usize]);
                (ext_row, crate::exact_vectors::dot(rows.row(h.node), query))
            })
            .filter(|(_, score)| *score >= floor)
            .collect();
        exact.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
        exact.truncate(k);
        exact
    }

    /// When this index carries a seal-time `row_to_node` permutation and the
    /// query specifies a non-trivial `allow` mask in external row coordinates,
    /// translate `allow` once into internal node coordinates (`Bits`).
    fn node_allow_bits(
        &self,
        allow: Option<Mask<'_>>,
    ) -> Result<Option<crate::boolean_bits::Bits>, VectorError> {
        self.check_allow(allow)?;
        let (Some(ext_allow), Some(row_to_node)) = (allow, self.row_to_node_slice()) else {
            return Ok(None);
        };
        if matches!(ext_allow, Mask::All(_)) {
            return Ok(None);
        }
        let mut bits = crate::boolean_bits::Bits::empty(self.len());
        let _ = ext_allow.try_for_each(|ext_row| {
            bits.set(row_to_node[ext_row] as usize);
            ControlFlow::<()>::Continue(())
        });
        Ok(Some(bits))
    }

    fn check_queries(&self, queries: &[f32]) -> Result<usize, VectorError> {
        if !queries.len().is_multiple_of(self.dim) {
            return Err(VectorError::new(format!(
                "{} query values do not form rows of dimension {}",
                queries.len(),
                self.dim
            )));
        }
        if let Some((row, coord, value)) = first_invalid_coordinate(queries, self.dim) {
            return Err(VectorError::new(format!(
                "query {row} coordinate {coord} is invalid: {value}"
            )));
        }
        Ok(queries.len() / self.dim)
    }

    fn check_allow(&self, allow: Option<Mask<'_>>) -> Result<(), VectorError> {
        if let Some(allow) = allow {
            if allow.len() != self.len() {
                return Err(VectorError::new(format!(
                    "allow mask covers {} rows, the index holds {}",
                    allow.len(),
                    self.len()
                )));
            }
        }
        Ok(())
    }

    fn m0(&self) -> usize {
        2 * self.config.m as usize
    }

    /// Choose how a search at beam `ef` answers under `options`: the regime
    /// the caller pinned, or the one the mask's cardinality calls for.
    ///
    /// - no mask, or a mask that allows every row: an unfiltered walk;
    /// - at most the request's `enumerate_max_rows` (which a dense execution
    ///   policy point carries from its measurement) or, without one,
    ///   `ENUMERATE_ROWS_PER_PLAIN_ROW · ef · m0` allowed rows: enumerate,
    ///   cheaper than any walk. The enumerate threshold is the request's or,
    ///   without one, `ENUMERATE_ROWS_PER_WALK_ROW · ef · m0`;
    /// - otherwise a routed walk: the far probe samples the rows around
    ///   where the descent landed and walks plain when at least
    ///   `PLAIN_LOCAL_SHARE` of them are allowed, enumerates when the
    ///   neighborhood is all but empty of allowed rows or the mask allows at
    ///   most the enumerate threshold, and otherwise walks filter-aware,
    ///   which may still hand the query to enumerate (`graph::Walk`).
    fn check_patience(patience: Option<PatienceSpec>) -> Result<PatienceSpec, VectorError> {
        let Some(spec) = patience else {
            return Ok(PatienceSpec::OFF);
        };
        if spec.patience == 0 {
            return Ok(PatienceSpec::OFF);
        }
        if let Some(ppm) = spec.saturation_ppm {
            if !(1..=1_000_000).contains(&ppm) {
                return Err(VectorError::new(format!(
                    "{HNSW} patience saturation_ppm {ppm} must be in 1..=1000000"
                )));
            }
        }
        if !spec.saturation.is_finite() || spec.saturation <= 0.0 || spec.saturation > 1.0 {
            return Err(VectorError::new(format!(
                "{HNSW} patience saturation {} must be finite and in (0.0, 1.0]",
                spec.saturation
            )));
        }
        Ok(spec)
    }

    fn check_floor_prune(
        floor_prune: Option<FloorPruneSpec>,
    ) -> Result<Option<FloorPruneSpec>, VectorError> {
        let Some(spec) = floor_prune else {
            return Ok(None);
        };
        if spec.share_ppm > 1_000_000 {
            return Err(VectorError::new(format!(
                "{HNSW} floor_prune share_ppm {} must be in 0..=1000000",
                spec.share_ppm
            )));
        }
        if spec.greediness_ppm > 1_000_000 {
            return Err(VectorError::new(format!(
                "{HNSW} floor_prune greediness_ppm {} must be in 0..=1000000",
                spec.greediness_ppm
            )));
        }
        Ok(Some(spec))
    }

    fn plan<'a>(
        &self,
        ef: usize,
        options: &VectorSearchOptions<'a>,
    ) -> Result<Plan<'a>, VectorError> {
        self.check_allow(options.allow)?;
        let mut patience = Self::check_patience(options.patience)?;
        let floor_prune = Self::check_floor_prune(options.floor_prune)?;
        let Some(allow) = options.allow else {
            if let Some(regime) = options.filter_regime {
                return Err(VectorError::new(format!(
                    "{HNSW} was asked for the {} filter regime on a search with no mask",
                    regime.name()
                )));
            }
            let local_eligible = self.len();
            if let Some(fp) = floor_prune {
                patience.ascent_gate = patience.ascent_gate.max(fp.ascent_gate(local_eligible));
            }
            let floor_prune_gate = floor_prune
                .filter(|fp| fp.enabled)
                .map(|fp| fp.effective_gate(local_eligible));
            return Ok(Plan {
                patience,
                floor_prune_gate,
                ..Plan::UNFILTERED
            });
        };
        if let (Some(regime), Some(rows)) = (options.filter_regime, options.enumerate_max_rows) {
            return Err(VectorError::new(format!(
                "{HNSW} was asked for the {} filter regime and an enumerate threshold of \
                 {rows} rows; a pinned regime has no threshold to apply",
                regime.name()
            )));
        }
        let allowed = allow.count();
        let walk_units = ef * self.m0();
        let default_threshold = ENUMERATE_ROWS_PER_WALK_ROW * walk_units;
        let mut budget = None;
        let mut route = None;
        let regime = match options.filter_regime {
            Some(regime) => regime,
            None if allowed == allow.len() => {
                let local_eligible = self.len();
                if let Some(fp) = floor_prune {
                    patience.ascent_gate = patience.ascent_gate.max(fp.ascent_gate(local_eligible));
                }
                let floor_prune_gate = floor_prune
                    .filter(|fp| fp.enabled)
                    .map(|fp| fp.effective_gate(local_eligible));
                return Ok(Plan {
                    patience,
                    floor_prune_gate,
                    ..Plan::UNFILTERED
                });
            }
            None => {
                let threshold = options.enumerate_max_rows.unwrap_or(default_threshold);
                // A policy point's threshold was measured as the line below
                // which it enumerates, and is served as measured; without
                // one only a mask cheaper than a plain walk enumerates
                // before the probe looks.
                let outright = options
                    .enumerate_max_rows
                    .unwrap_or(ENUMERATE_ROWS_PER_PLAIN_ROW * walk_units);
                if allowed <= outright {
                    FilterRegime::Enumerate
                } else {
                    route = Some(Route {
                        plain_share: PLAIN_LOCAL_SHARE,
                        enumerate_rows: threshold,
                    });
                    if threshold > 0 && default_threshold > 0 {
                        // Scale the measured enumerate-rows-per-walk-unit
                        // ratio by the policy's `enumerate_max_rows`, bounded
                        // below by 1.
                        let cost_ratio = (WALK_WORK_ENUMERATE_ROWS
                            * (threshold as f64 / default_threshold as f64))
                            .max(1.0);
                        budget = Some(((allowed as f64) / cost_ratio).ceil() as u64);
                    }
                    FilterRegime::FilterAware
                }
            }
        };
        if let Some(fp) = floor_prune {
            patience.ascent_gate = patience.ascent_gate.max(fp.ascent_gate(allowed));
        }
        let floor_prune_gate = floor_prune
            .filter(|fp| fp.enabled)
            .map(|fp| fp.effective_gate(allowed));
        Ok(Plan {
            regime: Some(regime),
            allow: Some(allow),
            allowed,
            route,
            budget,
            patience,
            floor_prune_gate,
        })
    }

    fn slack_per_norm(&self, query: &[f32]) -> f64 {
        let du = self.dim as f64 / (1u64 << 24) as f64;
        let gamma = du / (1.0 - du);
        let query_norm = query
            .iter()
            .map(|&v| f64::from(v) * f64::from(v))
            .sum::<f64>()
            .sqrt();
        4.0 * gamma * query_norm
    }

    /// The enumerate regime: the exact top `k` of the allowed rows at or
    /// above the floor. Every allowed row is scored with the traversal
    /// kernel, and the scalar scorer settles the rows the kernel cannot
    /// rule out (`graph::View::enumerate`).
    fn enumerate(
        &self,
        query: &[f32],
        k: usize,
        allow: Mask<'_>,
        floor: f32,
    ) -> (Vec<(u32, f32)>, SearchStats) {
        self.enumerate_skipping(query, k, allow, floor, &[], |_| false)
    }

    fn enumerate_skipping(
        &self,
        query: &[f32],
        k: usize,
        allow: Mask<'_>,
        floor: f32,
        already_scored: &[Scored],
        skip: impl Fn(u32) -> bool,
    ) -> (Vec<(u32, f32)>, SearchStats) {
        let slack_per_norm = self.slack_per_norm(query);
        let mut survivors = Vec::new();
        let scored = with_view!(self, |view| view.enumerate_skipping(
            query,
            allow,
            k,
            floor,
            slack_per_norm,
            already_scored,
            skip,
            &mut survivors
        ));
        let stats = SearchStats {
            scored,
            ..SearchStats::default()
        };
        let n2r = self.node_to_row_slice();
        let hits = self.with_row_source(|rows| Self::report(survivors, query, rows, floor, k, n2r));
        (hits, stats)
    }

    /// One query under a plan: up to `k` `(row, score)` best first, and
    /// how it was answered.
    fn answer(
        &self,
        query: &[f32],
        k: usize,
        ef: usize,
        floor: f32,
        plan: &Plan<'_>,
        scratch: &mut graph::Scratch,
    ) -> (Vec<(u32, f32)>, Answer) {
        if !self.is_graph_linked() {
            let allow = plan.allow.unwrap_or(Mask::All(self.len()));
            let (hits, stats) = self.enumerate(query, k, allow, floor);
            let tail_enumerated_rows = stats.scored;
            return (
                hits,
                Answer {
                    regime: plan.regime.map(|_| FilterRegime::Enumerate),
                    stats,
                    budget_handoff: false,
                    patience_stopped: false,
                    floor_pruned: false,
                    tail_enumerated_rows,
                },
            );
        }
        if let (Some(FilterRegime::Enumerate), Some(allow)) = (plan.regime, plan.allow) {
            let (hits, stats) = self.enumerate(query, k, allow, floor);
            return (
                hits,
                Answer {
                    regime: plan.regime,
                    stats,
                    budget_handoff: false,
                    patience_stopped: false,
                    floor_pruned: false,
                    tail_enumerated_rows: 0,
                },
            );
        }
        let patience = plan.patience(k, floor);
        let floor_prune = plan.floor_prune(if plan.floor_prune_gate.is_some() {
            self.slack_per_norm(query)
        } else {
            0.0
        });
        let mut hook = FloorHook(floor);
        let (hits, mut stats, walk) = with_view!(self, |view| view.walk(
            query,
            ef,
            plan.filter(),
            patience,
            floor_prune,
            scratch,
            &mut hook
        ));
        match (walk, plan.allow) {
            (Walk::Far, Some(allow)) => {
                // The graph cannot serve this mask from where the query lands:
                // the answer is the exact top k of the mask.
                let (hits, exact) = self.enumerate(query, k, allow, floor);
                stats.scored += exact.scored;
                return (
                    hits,
                    Answer {
                        regime: Some(FilterRegime::Enumerate),
                        stats,
                        budget_handoff: false,
                        patience_stopped: false,
                        floor_pruned: false,
                        tail_enumerated_rows: 0,
                    },
                );
            }
            (Walk::Short | Walk::Budget, Some(allow)) => {
                // Keep the allowed rows the walk already scored and score
                // only the remaining allowed rows in mask order.
                let (hits, exact) = self.enumerate_skipping(
                    query,
                    k,
                    allow,
                    floor,
                    scratch.scored_allowed(),
                    |row| scratch.seen(row),
                );
                stats.scored += exact.scored;
                return (
                    hits,
                    Answer {
                        regime: Some(FilterRegime::Enumerate),
                        stats,
                        budget_handoff: walk == Walk::Budget,
                        patience_stopped: false,
                        floor_pruned: false,
                        tail_enumerated_rows: 0,
                    },
                );
            }
            _ => {}
        }
        let n2r = self.node_to_row_slice();
        let hits = self.with_row_source(|rows| Self::report(hits, query, rows, floor, k, n2r));
        let regime = if scratch.walked_plain() {
            Some(FilterRegime::Plain)
        } else {
            plan.regime
        };
        (
            hits,
            Answer {
                regime,
                stats,
                budget_handoff: false,
                patience_stopped: walk == Walk::Patience,
                floor_pruned: walk == Walk::FloorPruned,
                tail_enumerated_rows: 0,
            },
        )
    }

    /// One query at an explicit beam: up to `k` `(row, score)` best first
    /// among the allowed rows at or above the floor, with the regime that
    /// answered and its work. The beam is `max(beam, k, EF_FLOOR)`.
    pub fn search_with_stats(
        &self,
        query: &[f32],
        k: usize,
        beam: usize,
        options: VectorSearchOptions<'_>,
    ) -> Result<(Vec<(u32, f32)>, Answer), VectorError> {
        if self.check_queries(query)? != 1 {
            return Err(VectorError::new(
                "search_with_stats takes exactly one query",
            ));
        }
        let ef = beam.max(k).max(EF_FLOOR);
        let node_bits = self.node_allow_bits(options.allow)?;
        let effective_options = match &node_bits {
            Some(bits) => VectorSearchOptions {
                allow: Some(Mask::Bits(bits)),
                ..options
            },
            None => options,
        };
        let plan = self.plan(ef, &effective_options)?;
        let floor = effective_options.minimum_score.unwrap_or(f32::NEG_INFINITY);
        Ok(with_scratch(|scratch| {
            self.answer(query, k, ef, floor, &plan, scratch)
        }))
    }

    /// The enumerate regime of a stream: every allowed row `skip` does not
    /// name, in row order. When a finite floor is active (from a prior
    /// segment's `minimum_score` or a mid-stream `RaiseFloor`), chunks of
    /// `STREAM_POLL_ROWS` rows are pre-filtered with the SIMD kernel using
    /// the exact error bound before invoking the scalar scorer; when no
    /// floor is active yet, rows go directly to the scalar scorer to avoid
    /// double-scoring. The control is polled every `STREAM_POLL_ROWS` rows.
    /// Returns the rows scored and whether the pass finished.
    fn stream_rows(
        &self,
        allow: Mask<'_>,
        skip: impl Fn(u32) -> bool,
        hook: &mut StreamHook<'_>,
    ) -> (usize, bool) {
        let slack_per_norm = self.slack_per_norm(hook.query);
        with_view!(self, |view| {
            let lines = view.kernel.prefetch_lines(view.dim);
            let batch = view.kernel.batch_width();
            let mut cur = [0u32; STREAM_POLL_ROWS];
            let mut cur_len = 0usize;
            let mut next = [0u32; STREAM_POLL_ROWS];
            let mut next_len = 0usize;
            let mut simd_scores = [0.0f32; STREAM_POLL_ROWS];
            let mut total_scored = 0usize;

            let mut run_chunk = |chunk: &[u32],
                                 ahead: &[u32],
                                 hook: &mut StreamHook<'_>,
                                 total: &mut usize|
             -> bool {
                for &node in &ahead[..ahead.len().min(batch)] {
                    prefetch(view.row(node), lines);
                }
                if (*hook.floor).is_finite() {
                    view.kernel.score_nodes(
                        hook.query,
                        view.rows,
                        view.dim,
                        chunk,
                        lines,
                        &mut simd_scores[..chunk.len()],
                    );
                    for (&node, &simd_score) in chunk.iter().zip(&simd_scores[..chunk.len()]) {
                        if hook.stopped {
                            return false;
                        }
                        *total += 1;
                        let inv = view.inv_norm[node as usize];
                        let keep = !(inv > 0.0 && inv.is_finite())
                            || !(*hook.floor).is_finite()
                            || f64::from(simd_score) + slack_per_norm / f64::from(inv)
                                >= f64::from(*hook.floor);
                        if keep {
                            hook.scored(node, simd_score, view.row(node));
                        }
                    }
                } else {
                    for &node in chunk {
                        if hook.stopped {
                            return false;
                        }
                        *total += 1;
                        hook.scored(node, 0.0, view.row(node));
                    }
                }
                if hook.stopped || hook.finish_reason.is_some() {
                    return false;
                }
                if chunk.len() == STREAM_POLL_ROWS && !hook.hop() {
                    return false;
                }
                true
            };

            let flow = allow.try_for_each(|row| {
                let node = row as u32;
                if skip(node) {
                    return ControlFlow::Continue(());
                }
                if cur_len < STREAM_POLL_ROWS {
                    cur[cur_len] = node;
                    cur_len += 1;
                } else {
                    next[next_len] = node;
                    next_len += 1;
                    if next_len == STREAM_POLL_ROWS {
                        if !run_chunk(&cur[..cur_len], &next[..next_len], hook, &mut total_scored) {
                            cur_len = 0;
                            next_len = 0;
                            return ControlFlow::Break(());
                        }
                        cur = next;
                        cur_len = next_len;
                        next_len = 0;
                    }
                }
                ControlFlow::Continue(())
            });
            if flow.is_break() {
                return (total_scored, !hook.stopped);
            }
            if cur_len > 0
                && !run_chunk(&cur[..cur_len], &next[..next_len], hook, &mut total_scored)
            {
                return (total_scored, !hook.stopped);
            }
            if next_len > 0 && !run_chunk(&next[..next_len], &[], hook, &mut total_scored) {
                return (total_scored, !hook.stopped);
            }
            (total_scored, !hook.stopped)
        })
    }

    fn row_bytes_into(&self, row: usize, out: &mut Vec<u8>) {
        match &self.backing {
            Backing::Heap(store) => {
                let node = self.row_to_node(row as u32);
                for v in store.rows.row(node) {
                    out.extend_from_slice(&v.to_le_bytes());
                }
            }
            Backing::Mapped(mapped) => out.extend_from_slice(mapped.row_bytes(row)),
        }
    }
}

/// The streaming walk's hook: rescores every allowed row the walk scores
/// with the scalar scorer, buffers those at or above the floor, hands
/// batches to the sink ordered score descending then row ascending, polls
/// the control after each expansion, and lets either raise the floor,
/// finish gracefully at a hop boundary (`#100`), or abandon (`Stop`).
struct StreamHook<'a> {
    query_index: usize,
    query: &'a [f32],
    node_to_row: Option<&'a [u32]>,
    floor: &'a mut f32,
    allow_early_finish: bool,
    hops: usize,
    flushed_once: bool,
    scores: Vec<f32>,
    slots: Vec<i64>,
    sink: &'a mut dyn FnMut(&VectorStreamBatch<'_>) -> VectorStreamControl,
    control: &'a mut dyn FnMut() -> VectorStreamControl,
    stopped: bool,
    finish_reason: Option<VectorStreamEndReason>,
    emitted: usize,
}

impl StreamHook<'_> {
    fn apply(&mut self, decision: VectorStreamControl) {
        match decision {
            VectorStreamControl::Continue => {}
            VectorStreamControl::RaiseFloor(raised) => {
                if raised > *self.floor {
                    *self.floor = raised;
                }
            }
            VectorStreamControl::Finish { deadline } => {
                if self.allow_early_finish && self.finish_reason.is_none() {
                    self.finish_reason = Some(if deadline {
                        VectorStreamEndReason::Deadline
                    } else {
                        VectorStreamEndReason::FinishRequested
                    });
                }
            }
            VectorStreamControl::Stop => self.stopped = true,
        }
    }

    fn flush(&mut self) {
        if self.scores.is_empty() || self.stopped {
            self.scores.clear();
            self.slots.clear();
            return;
        }
        let floor = *self.floor;
        // Rows that were above the floor when scored may have fallen below
        // a floor raised since; the sink only sees rows at or above it now.
        let mut kept = 0;
        for i in 0..self.scores.len() {
            if self.scores[i] >= floor {
                self.scores[kept] = self.scores[i];
                self.slots[kept] = self.slots[i];
                kept += 1;
            }
        }
        self.scores.truncate(kept);
        self.slots.truncate(kept);
        if kept == 0 {
            return;
        }
        // Score descending, then row ascending, within the batch.
        let mut order: Vec<usize> = (0..kept).collect();
        order.sort_by(|&a, &b| {
            self.scores[b]
                .total_cmp(&self.scores[a])
                .then(self.slots[a].cmp(&self.slots[b]))
        });
        let scores: Vec<f32> = order.iter().map(|&i| self.scores[i]).collect();
        let slots: Vec<i64> = order.iter().map(|&i| self.slots[i]).collect();
        self.scores = scores;
        self.slots = slots;
        self.flushed_once = true;
        self.emitted += kept;
        let decision = (self.sink)(&VectorStreamBatch {
            query_index: self.query_index,
            block_base: 0,
            scores: &self.scores,
            slots: &self.slots,
        });
        self.scores.clear();
        self.slots.clear();
        self.apply(decision);
    }
}

impl Hook for StreamHook<'_> {
    #[inline]
    fn scored(&mut self, node: u32, _traversal_score: f32, row: &[f32]) {
        if self.stopped {
            return;
        }
        let score = crate::exact_vectors::dot(row, self.query);
        if score >= *self.floor {
            let ext_row = self.node_to_row.map_or(node, |m| m[node as usize]);
            self.scores.push(score);
            self.slots.push(i64::from(ext_row));
            if self.scores.len() >= STREAM_BATCH_ROWS {
                self.flush();
            }
        }
    }

    fn hop(&mut self) -> bool {
        self.hops += 1;
        if !self.stopped && self.finish_reason.is_none() {
            let decision = (self.control)();
            self.apply(decision);
        }
        if !self.stopped
            && self.finish_reason.is_none()
            && !self.flushed_once
            && self.hops >= STREAM_FIRST_FLUSH_HOPS
            && self.scores.len() >= STREAM_FIRST_FLUSH_ROWS
        {
            self.flush();
        }
        !self.stopped && self.finish_reason.is_none()
    }

    #[inline]
    fn current_floor(&self) -> f32 {
        *self.floor
    }
}

impl VectorProvider for HnswIndex {
    fn descriptor(&self) -> VectorBackendDescriptor {
        let dimension = (self.dim as u64).to_le_bytes();
        let payload = self.config.encode();
        VectorBackendDescriptor {
            backend_kind: HNSW.to_string(),
            backend_version: BACKEND_VERSION.to_string(),
            dimension: Some(self.dim),
            bits_per_dimension: Some(32),
            metric: "inner_product".to_string(),
            score_direction: ScoreDirection::HigherIsBetter,
            scoring_fingerprint: crate::sha256::hex_digest(
                [
                    HNSW.as_bytes(),
                    BACKEND_VERSION.as_bytes(),
                    dimension.as_slice(),
                    CONFIG_FORMAT.as_bytes(),
                    payload.as_slice(),
                ]
                .concat()
                .as_slice(),
            ),
            quality_contract: QualityContract::ConfiguredAnn,
            capabilities: vec![
                VectorCapability::BatchQuery,
                VectorCapability::CandidateStream,
                VectorCapability::LiveBoundInput,
                VectorCapability::DenseMask,
                VectorCapability::Append,
                VectorCapability::Flush,
                VectorCapability::SnapshotInstall,
                VectorCapability::RawVectorRebuild,
                VectorCapability::MaskEnumeration,
                VectorCapability::PatienceEarlyStop,
                VectorCapability::FloorPruning,
            ],
        }
    }

    fn backend_config(&self) -> Result<VectorBackendConfig, VectorError> {
        self.config.backend_config()
    }

    fn len(&self) -> usize {
        match &self.backing {
            Backing::Heap(store) => store.n(),
            Backing::Mapped(mapped) => mapped.n(),
        }
    }

    fn dimension(&self) -> Option<usize> {
        Some(self.dim)
    }

    fn is_mapped(&self) -> bool {
        matches!(self.backing, Backing::Mapped(_))
    }

    fn is_graph_linked(&self) -> bool {
        match &self.backing {
            Backing::Heap(store) => store.is_linked(),
            Backing::Mapped(mapped) => mapped.header().entry != graph::NO_NODE,
        }
    }

    fn as_hnsw(&self) -> Option<&HnswIndex> {
        Some(self)
    }

    /// Append rows and their reciprocal norms to the unlinked heap store
    /// without building graph edges (`docs/hnsw.md`, "Unlinked tail and tiny
    /// segments"). Queries over an unlinked store answer by exact SIMD
    /// enumeration until [`Self::prepare`] or [`Self::write`] builds the
    /// graph in bulk at seal time.
    fn add(&mut self, vectors: &[f32], dimension: usize) -> Result<(), VectorError> {
        if dimension != self.dim {
            return Err(VectorError::new(format!(
                "vectors have dimension {dimension}, the {HNSW} index expects {}",
                self.dim
            )));
        }
        if !vectors.len().is_multiple_of(self.dim) {
            return Err(VectorError::new(format!(
                "{} values do not form rows of dimension {}",
                vectors.len(),
                self.dim
            )));
        }
        if let Some((row, coord, value)) = first_invalid_coordinate(vectors, self.dim) {
            return Err(VectorError::new(format!(
                "vector {row} coordinate {coord} is invalid: {value}"
            )));
        }
        let store = match &mut self.backing {
            Backing::Heap(store) => store,
            Backing::Mapped(_) => {
                return Err(VectorError::new(format!(
                    "{HNSW} image is served from a map and is sealed; rows cannot be added"
                )))
            }
        };
        store.append_unlinked(vectors)
    }

    /// Build the graph in bulk when the heap store is unlinked and holds at
    /// least [`MIN_GRAPH_ROWS`] rows; stores below [`MIN_GRAPH_ROWS`] remain
    /// unlinked (`entry = NO_NODE`) and answer by exact SIMD enumeration.
    fn prepare(&mut self) -> Result<(), VectorError> {
        let mode = self.config.build_mode;
        if let Backing::Heap(store) = &mut self.backing {
            if !store.is_linked() && store.n() >= MIN_GRAPH_ROWS {
                let parallel = mode == BuildMode::Parallel && store.n() >= PARALLEL_MIN_BATCH;
                let links = store.build_links(parallel)?;
                store.install_links(links);
            }
        }
        self.reorder_local_in_place();
        Ok(())
    }

    fn write(&self, path: &Path) -> Result<(), VectorError> {
        match &self.backing {
            Backing::Heap(store) => {
                if !store.is_linked() && store.n() >= MIN_GRAPH_ROWS {
                    let parallel = self.config.build_mode == BuildMode::Parallel
                        && store.n() >= PARALLEL_MIN_BATCH;
                    let links = store.build_links(parallel)?;
                    if self.config.locality_order != LocalityOrder::None {
                        let (entry, top) = links.entry_state();
                        let temp_view = graph::View {
                            dim: store.dim,
                            m: store.m,
                            m0: store.m0,
                            rows: store.rows.values(),
                            inv_norm: &store.inv_norm,
                            level: &links.level,
                            upper_slot: &links.upper_slot,
                            upper_base: &links.upper_base,
                            links0: links.links0.as_slice(),
                            upper: links.upper.as_slice(),
                            entry,
                            top,
                            kernel: store.kernel,
                        };
                        let new_to_old =
                            reorder::compute_locality_order(&temp_view, self.config.locality_order);
                        let reordered =
                            reorder::permute_store_with_links(store, &links, &new_to_old, true);
                        image::write(&reordered, &self.config, path)
                    } else {
                        image::write_with_links(store, Some(&links), &self.config, path)
                    }
                } else if store.is_linked()
                    && !store.is_reordered()
                    && self.config.locality_order != LocalityOrder::None
                {
                    let links = store.links().expect("linked");
                    let new_to_old =
                        reorder::compute_locality_order(&store.view(), self.config.locality_order);
                    let reordered =
                        reorder::permute_store_with_links(store, links, &new_to_old, true);
                    image::write(&reordered, &self.config, path)
                } else {
                    image::write(store, &self.config, path)
                }
            }
            Backing::Mapped(_) => Err(VectorError::new(format!(
                "{HNSW} image is served from a map; copy the file instead of rewriting it"
            ))),
        }
    }

    /// Top `k` per query at a beam of `max(k, candidate_depth, EF_FLOOR)`.
    /// Disallowed rows route but are never returned; `minimum_score`
    /// filters the results; short results are padded with `-1` and
    /// negative infinity.
    fn search(
        &self,
        queries: &[f32],
        k: usize,
        options: VectorSearchOptions<'_>,
    ) -> Result<VectorSearchResults, VectorError> {
        let query_count = self.check_queries(queries)?;
        let ef = k.max(EF_FLOOR).max(options.candidate_depth.unwrap_or(0));
        let node_bits = self.node_allow_bits(options.allow)?;
        let effective_options = match &node_bits {
            Some(bits) => VectorSearchOptions {
                allow: Some(Mask::Bits(bits)),
                ..options
            },
            None => options,
        };
        let plan = self.plan(ef, &effective_options)?;
        let floor = effective_options.minimum_score.unwrap_or(f32::NEG_INFINITY);
        let mut scores = Vec::with_capacity(query_count * k);
        let mut slots = Vec::with_capacity(query_count * k);
        let mut regimes = crate::vector::FilterRegimeCounts::default();
        let mut tail_enumerated_rows = 0u64;
        let mut patience_stopped = 0u32;
        let mut floor_pruned = 0u32;
        with_scratch(|scratch| {
            for query in queries.chunks_exact(self.dim) {
                let (hits, answer) = self.answer(query, k, ef, floor, &plan, scratch);
                tail_enumerated_rows =
                    tail_enumerated_rows.saturating_add(answer.tail_enumerated_rows);
                if answer.patience_stopped {
                    patience_stopped = patience_stopped.saturating_add(1);
                }
                if answer.floor_pruned {
                    floor_pruned = floor_pruned.saturating_add(1);
                }
                if answer.budget_handoff {
                    regimes.count_budget_handoff();
                } else if let Some(regime) = answer.regime {
                    regimes.count(regime);
                }
                for (node, score) in &hits {
                    scores.push(*score);
                    slots.push(i64::from(*node));
                }
                for _ in hits.len()..k {
                    scores.push(f32::NEG_INFINITY);
                    slots.push(-1);
                }
            }
        });
        Ok(VectorSearchResults {
            scores,
            slots,
            query_count,
            result_count: k,
            regimes,
            tail_enumerated_rows,
            patience_stopped,
            floor_pruned,
        })
    }

    /// One walk per query at the request's candidate depth, emitting every
    /// allowed row the walk scores exactly at or above the floor, in
    /// batches, as it proceeds. `control` is polled after every expansion;
    /// `Stop` from either callback ends the stream uncompleted, and a floor
    /// raised by either gates the rest of the query and every later one.
    /// A filtered query follows the regimes of `plan`: enumerate emits
    /// every allowed row, and a filter-aware walk that gives the query up
    /// is finished by enumerating the allowed rows it did not reach.
    /// `completed` means every query finished, never that every row was
    /// evaluated. Without a depth the provider refuses: the beam is not
    /// configuration, and a graph walk cannot guess how far to go.
    fn search_streaming_controlled(
        &self,
        queries: &[f32],
        options: VectorSearchOptions<'_>,
        sink: &mut dyn FnMut(&VectorStreamBatch<'_>) -> VectorStreamControl,
        control: &mut dyn FnMut() -> VectorStreamControl,
    ) -> Result<VectorStreamSummary, VectorError> {
        let query_count = self.check_queries(queries)?;
        let depth = options.candidate_depth.ok_or_else(|| {
            VectorError::new(format!(
                "{HNSW} streaming search needs the request's candidate depth; the beam is not \
                 part of the backend config"
            ))
        })?;
        if depth == 0 {
            return Err(VectorError::new(format!(
                "{HNSW} streaming search was given candidate depth 0"
            )));
        }
        let ef = depth.max(EF_FLOOR);
        let allow_early_finish = options.filter_regime != Some(FilterRegime::Enumerate);
        let node_bits = self.node_allow_bits(options.allow)?;
        let effective_options = match &node_bits {
            Some(bits) => VectorSearchOptions {
                allow: Some(Mask::Bits(bits)),
                ..options
            },
            None => options,
        };
        let plan = self.plan(ef, &effective_options)?;
        let linked = self.is_graph_linked();
        let node_to_row = self.node_to_row_slice();
        let mut floor = effective_options.minimum_score.unwrap_or(f32::NEG_INFINITY);
        let mut emitted = 0usize;
        let mut units = 0usize;
        let mut stopped = false;
        let mut end_reason = VectorStreamEndReason::Unspecified;
        let mut regimes = crate::vector::FilterRegimeCounts::default();
        let mut tail_enumerated_rows = 0u64;
        let mut patience_stopped = 0u32;
        let mut floor_pruned = 0u32;
        with_scratch(|scratch| {
            for (query_index, query) in queries.chunks_exact(self.dim).enumerate() {
                let mut hook = StreamHook {
                    query_index,
                    query,
                    node_to_row,
                    floor: &mut floor,
                    allow_early_finish,
                    hops: 0,
                    flushed_once: false,
                    scores: Vec::with_capacity(STREAM_BATCH_ROWS),
                    slots: Vec::with_capacity(STREAM_BATCH_ROWS),
                    sink,
                    control,
                    stopped: false,
                    finish_reason: None,
                    emitted: 0,
                };
                let mut answered = plan.regime;
                let mut budget_handoff = false;
                let (scored, completed, query_reason) = if !linked {
                    answered = plan.regime.map(|_| FilterRegime::Enumerate);
                    let allow = plan.allow.unwrap_or(Mask::All(self.len()));
                    let (rows, done) = self.stream_rows(allow, |_| false, &mut hook);
                    tail_enumerated_rows = tail_enumerated_rows.saturating_add(rows as u64);
                    (rows, done, VectorStreamEndReason::Exhausted)
                } else {
                    match (plan.regime, plan.allow) {
                        (Some(FilterRegime::Enumerate), Some(allow)) => {
                            let (rows, done) = self.stream_rows(allow, |_| false, &mut hook);
                            (rows, done, VectorStreamEndReason::Exhausted)
                        }
                        _ => {
                            let patience = plan.patience(ef, *hook.floor);
                            let floor_prune_cfg =
                                plan.floor_prune(if plan.floor_prune_gate.is_some() {
                                    self.slack_per_norm(query)
                                } else {
                                    0.0
                                });
                            let (_, stats, walk) = with_view!(self, |view| view.walk(
                                query,
                                ef,
                                plan.filter(),
                                patience,
                                floor_prune_cfg,
                                scratch,
                                &mut hook
                            ));
                            let walked = stats.scored as usize;
                            match (walk, plan.allow) {
                                (Walk::Far, Some(allow)) => {
                                    answered = Some(FilterRegime::Enumerate);
                                    let (rows, done) =
                                        self.stream_rows(allow, |_| false, &mut hook);
                                    (walked + rows, done, VectorStreamEndReason::Exhausted)
                                }
                                // The walk already emitted the rows it reached.
                                (Walk::Short | Walk::Budget, Some(allow)) => {
                                    answered = Some(FilterRegime::Enumerate);
                                    budget_handoff = walk == Walk::Budget;
                                    let (rows, done) =
                                        self.stream_rows(allow, |row| scratch.seen(row), &mut hook);
                                    (walked + rows, done, VectorStreamEndReason::Exhausted)
                                }
                                (walk, _) => {
                                    if scratch.walked_plain() {
                                        answered = Some(FilterRegime::Plain);
                                    }
                                    if walk == Walk::Patience {
                                        patience_stopped = patience_stopped.saturating_add(1);
                                    }
                                    if walk == Walk::FloorPruned {
                                        floor_pruned = floor_pruned.saturating_add(1);
                                    }
                                    let done =
                                        walk != Walk::Stopped || hook.finish_reason.is_some();
                                    let reason = if walk == Walk::FloorPruned {
                                        VectorStreamEndReason::FloorPruned
                                    } else {
                                        VectorStreamEndReason::WalkConverged
                                    };
                                    (walked, done, reason)
                                }
                            }
                        }
                    }
                };
                hook.flush();
                if budget_handoff {
                    regimes.count_budget_handoff();
                } else if let Some(regime) = answered {
                    regimes.count(regime);
                }
                units += scored;
                emitted += hook.emitted;
                if !completed || hook.stopped {
                    stopped = true;
                    end_reason = VectorStreamEndReason::Abandoned;
                    break;
                }
                if let Some(reason) = hook.finish_reason {
                    end_reason = end_reason.merge(reason);
                    break;
                }
                end_reason = end_reason.merge(query_reason);
            }
        });
        if !stopped && end_reason == VectorStreamEndReason::Unspecified {
            end_reason = VectorStreamEndReason::Exhausted;
        }
        Ok(VectorStreamSummary {
            query_count,
            emitted,
            units_scanned: units,
            completed: !stopped,
            end_reason,
            regimes,
            tail_enumerated_rows,
            patience_stopped,
            floor_pruned,
        })
    }

    /// A row's transcript is the bytes its score is computed from: the
    /// dimension, then the stored FP32 row little-endian. Nothing else in
    /// the image (norms, links) reaches a reported score.
    fn row_transcripts(
        &self,
        rows: Range<usize>,
        sink: &mut dyn FnMut(usize, &[u8]),
    ) -> Result<(), VectorError> {
        if rows.start > rows.end || rows.end > self.len() {
            return Err(VectorError::new(format!(
                "rows {rows:?} are outside the {} stored vectors",
                self.len()
            )));
        }
        let mut out = Vec::with_capacity(12 + 4 * self.dim);
        for row in rows {
            out.clear();
            out.extend_from_slice(b"HNSW");
            out.extend_from_slice(&(self.dim as u64).to_le_bytes());
            self.row_bytes_into(row, &mut out);
            sink(row, &out);
        }
        Ok(())
    }
}

/// `VectorIndex::create`: the default config at `dimension`. The FP32 rows
/// have no code width, so `bits_per_dimension` plays no part; the
/// descriptor reports 32.
pub(crate) fn create(dimension: usize) -> Result<VectorIndex, VectorError> {
    HnswIndex::new(dimension, HnswConfig::default()).map(HnswIndex::into_index)
}

pub(crate) fn from_config(
    dimension: usize,
    config: &VectorBackendConfig,
) -> Result<VectorIndex, VectorError> {
    let config = HnswConfig::from_backend_config(config)?;
    HnswIndex::new(dimension, config).map(HnswIndex::into_index)
}

pub(crate) fn load(path: &Path) -> Result<VectorIndex, VectorError> {
    HnswIndex::load(path).map(HnswIndex::into_index)
}

pub(crate) fn load_mapped(path: &Path) -> Result<VectorIndex, VectorError> {
    HnswIndex::load_mapped(path).map(HnswIndex::into_index)
}

#[allow(dead_code)]
fn _assert_scored_is_small() {
    const _: () = assert!(std::mem::size_of::<Scored>() == 8);
}

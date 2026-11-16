//! The `hnsw` vector provider: a from-scratch Hierarchical Navigable Small
//! World graph over FP32 rows, scored by inner product.
//!
//! The algorithms are those of Malkov & Yashunin, "Efficient and robust
//! approximate nearest neighbor search using Hierarchical Navigable Small
//! World graphs" (arXiv:1603.09320); see `NOTICE`. Nothing is ported from
//! another implementation. This module's submodules are:
//!
//! - [`config`]: the protobuf-carried build parameters and the fingerprint
//!   contract.
//! - [`distance`]: single-row and multi-row inner-product kernels with
//!   runtime dispatch.
//! - [`graph`]: the flat layered graph, insertion and the walk.
//! - [`image`]: the persisted format, loaded onto the heap or served from
//!   a memory map.
//! - [`rows`]: the FP32 rows behind the [`RowSource`] trait, with the
//!   identity a shared row store must match.
//!
//! The provider advertises
//! [`QualityContract::ConfiguredAnn`](crate::vector::QualityContract::ConfiguredAnn) without
//! exhaustive completion: `EXACT` refuses it, `ANN` runs it, and `AUTO`
//! admits it only through a measured dense execution policy bound to its
//! scoring fingerprint.
//!
//! Numerical contract: the SIMD kernels steer the traversal only. Every row
//! the provider reports, in a unary result or a streamed batch, is rescored
//! with [`crate::exact_vectors::dot`], the scalar row-order accumulation,
//! before it leaves the provider, so reported scores are bit for bit the
//! product's exact scores on every kernel and host. Ties order by score
//! descending, then row ascending.

/// Dispatch over the two link storages: `$v` is a `graph::View`.
macro_rules! with_view {
    ($self:expr, |$v:ident| $body:expr) => {
        match &$self.backing {
            $crate::hnsw::Backing::Heap(store) => {
                let $v = store.view();
                $body
            }
            $crate::hnsw::Backing::Mapped(mapped) => {
                let $v = mapped.view();
                $body
            }
        }
    };
}

mod answer;
pub mod config;
pub mod distance;
pub mod graph;
pub mod image;
mod index;
pub mod merge;
mod plan;
mod provider;
pub mod reorder;
mod repair;
pub mod rows;
mod seal;
mod stream;
mod stream_hook;
#[cfg(test)]
mod tests;

use std::path::Path;

pub use config::{BuildMode, HnswConfig, LocalityOrder, CONFIG_FORMAT};
pub use distance::Kernel;
pub use graph::SearchStats;
pub use merge::{MergeStats, MergeStrategy, SegmentMergeInput, JOIN_DOMINATION_K};
pub use reorder::LocalityMetrics;
pub use reorder::NodeMap;
pub use rows::{RowSource, RowSourceIdentity};

use crate::vector::{FilterRegime, VectorBackendConfig, VectorError, VectorIndex, VectorProvider};

use graph::{Scored, Store};

/// Stable identifier of the backend, as `vector_backend` names it.
pub const HNSW: &str = "hnsw";
/// Largest dimension an image may declare.
pub const MAX_DIM: usize = 1 << 16;
/// The smallest beam a query walks with. The harness's breadth sweep
/// starts here, so a product query never runs a beam narrower than one the
/// gate measured; below it the walk degenerates toward a greedy descent.
pub const EF_FLOOR: usize = 16;
/// Minimum row count at which `prepare` / `write` builds a graph. Below 256
/// rows, sequential SIMD enumeration achieves 100% exact recall faster than
/// a realistic beam walk (`ef >= 16..32` crosses `enumerate` at `256..768`
/// rows on AVX-512F and NEON) while avoiding graph construction and saving
/// `141` bytes/row in `PMHNSW01` images (`entry = NO_NODE`).
pub const MIN_GRAPH_ROWS: usize = 256;
/// A parallel-mode batch at least this large is inserted from every
/// thread; smaller batches are not worth the fork.
const PARALLEL_MIN_BATCH: usize = 256;
/// Enumerate wins while the allowed rows cost less to score in row order
/// than a walk costs. A filter-aware walk scores on the order of `ef · m0`
/// rows, each behind a cache miss, and bridges through the disallowed ones;
/// a row scored in mask order costs a fraction of that. The factor is the
/// measured ratio on the challenge fixture; a dense execution policy point
/// carries its own measured threshold (`enumerate_max_rows`).
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
/// (`layer0_scored + bridges`): across AVX2/AVX-512F and NEON (`d = 64` and
/// `d = 256`), one random walk or bridge expansion costs ~3.0–3.6× one
/// sequential mask-ordered row in `enumerate`.
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
    /// floor.
    pub floor_pruned: bool,
    /// Rows scored by enumeration because this index was unlinked (heap tail,
    /// frozen tail, or sealed segment below [`MIN_GRAPH_ROWS`]).
    pub tail_enumerated_rows: u64,
}

impl Answer {
    /// An answer by `regime` with none of the handoff or early-stop flags.
    fn of(regime: Option<FilterRegime>, stats: SearchStats) -> Self {
        Self {
            regime,
            stats,
            budget_handoff: false,
            patience_stopped: false,
            floor_pruned: false,
            tail_enumerated_rows: 0,
        }
    }
}

enum Backing {
    Heap(Store),
    Mapped(image::Mapped),
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

const _: () = assert!(std::mem::size_of::<Scored>() == 8);

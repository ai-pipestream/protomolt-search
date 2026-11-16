//! The layered graph: flat storage, insertion, and the walk.
//!
//! Layout (one contiguous array per kind, no `Vec<Vec>`):
//!
//! - `rows`: FP32 rows, `dim` values each, in node order.
//! - `inv_norm`: one `1/|row|` per node (0 for a zero row), for the
//!   direction-based neighbor heuristic.
//! - `level`: each node's top layer.
//! - `links0`: layer 0 for every node, `2M + 1` cells per node: the count,
//!   then the neighbors.
//! - `upper_slot`, `upper_base`, `upper`: the upper layers of the nodes
//!   that have them. `upper_slot[node]` is the node's compact index `u`
//!   (`NO_NODE` for a level-0 node); the node's layers `1..=level` are
//!   `level` consecutive blocks of `M + 1` cells starting at
//!   `upper[upper_base[u]]`.
//!
//! The heap graph keeps its link cells as `AtomicU32` so a parallel build
//! can write them from several threads under striped per-node locks with no
//! `unsafe`; a mapped image serves plain `u32` words. Both implement
//! [`LinkCells`] and the walk is generic over it, so a search over a loaded
//! and a mapped image runs the same code and produces the same scores.
//!
//! Algorithms follow Malkov & Yashunin, "Efficient and robust approximate
//! nearest neighbor search using Hierarchical Navigable Small World graphs"
//! (arXiv:1603.09320): insertion (Alg. 1), the layer search (Alg. 2), the
//! neighbor heuristic (Alg. 4, without `extendCandidates` and without
//! `keepPrunedConnections`), and the query (Alg. 5). The heuristic
//! compares directions rather than inner products: see `select`.

use std::cmp::Ordering;
use std::sync::atomic::{AtomicU32, Ordering as Atomic};

mod beam;
mod build;
#[cfg(test)]
mod build_tests;
mod enumerate;
mod hooks;
mod policy;
mod probe;
mod scratch;
mod search_layer;
mod select;
mod store;
mod view;
mod walk;

pub(in crate::hnsw) use beam::LayerSearch;
pub(super) use build::LinkBuilder;
pub(in crate::hnsw) use enumerate::EnumerateQuery;
pub use hooks::{FloorHook, Hook, NoHook};
pub use policy::{Filter, FloorPrune, Patience, Route, Steering, Walk};
pub use scratch::{with_scratch, Scratch};
pub use store::{level_for, GraphLinks, Store};
pub use view::{Locks, View};

/// The absent node: an empty graph's entry, a level-0 node's upper slot.
pub const NO_NODE: u32 = u32::MAX;
/// Levels are capped so a hostile image cannot claim an absurd skeleton;
/// the geometric draw reaches 13 at `M = 16` with probability 2^-53.
pub const MAX_LEVEL: u8 = 31;
/// Rows beyond this cannot be addressed by a `u32` node id with the sentinel.
pub const MAX_ROWS: usize = (u32::MAX - 1) as usize;
/// Stack chunk size for [`View::for_each_scored`] and [`View::enumerate`]
/// (`64` covers a full layer-0 neighbor list at `M <= 32` in one call).
const SCORE_CHUNK_NODES: usize = 64;
/// Striped per-node locks for the parallel build.
const LOCK_STRIPES: usize = 1 << 14;

/// Link cells the walk reads.
pub trait LinkCells: Sync {
    fn get(&self, i: usize) -> u32;
    fn cell_count(&self) -> usize;
    /// Hint the cache that cells `start..start + cells` are about to be read.
    /// A hint only: the default does nothing, so implementors may skip it.
    fn prefetch(&self, start: usize, cells: usize) {
        let _ = (start, cells);
    }
}

/// Prefetch the first `cells` link cells of `tail`. The span is counted
/// from its 64-byte line, so a list that straddles one more line than its
/// length suggests still has its last line fetched.
#[inline]
fn prefetch_cells<T>(tail: &[T], cells: usize) {
    let bytes = cells.min(tail.len()) * std::mem::size_of::<T>();
    if bytes == 0 {
        return;
    }
    let start = tail.as_ptr() as *const u8;
    let skew = start as usize % 64;
    let span = skew + bytes;
    super::distance::prefetch_bytes(start.wrapping_sub(skew), span, span.div_ceil(64));
}

impl LinkCells for [AtomicU32] {
    #[inline]
    fn get(&self, i: usize) -> u32 {
        self[i].load(Atomic::Relaxed)
    }
    #[inline]
    fn prefetch(&self, start: usize, cells: usize) {
        prefetch_cells(&self[start.min(self.len())..], cells);
    }
    fn cell_count(&self) -> usize {
        <[AtomicU32]>::len(self)
    }
}

impl LinkCells for [u32] {
    #[inline]
    fn get(&self, i: usize) -> u32 {
        self[i]
    }
    #[inline]
    fn prefetch(&self, start: usize, cells: usize) {
        prefetch_cells(&self[start.min(self.len())..], cells);
    }
    fn cell_count(&self) -> usize {
        <[u32]>::len(self)
    }
}

/// A scored node. The order is total (score, then the smaller node first
/// among ties), so heaps behave the same on every run.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Scored {
    pub score: f32,
    pub node: u32,
}

impl Eq for Scored {}

impl Ord for Scored {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| other.node.cmp(&self.node))
    }
}

impl PartialOrd for Scored {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Work counters of one walk.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SearchStats {
    /// Nodes expanded (neighbor lists read).
    pub hops: u64,
    /// Rows scored by the traversal kernel.
    pub scored: u64,
    /// Disallowed rows a filter-aware walk looked through without scoring.
    pub bridges: u64,
    /// Rows the far probe looked at (links and mask only, never scored).
    pub probed: u64,
}

#[cfg(test)]
mod prefetch_tests {
    use super::LinkCells;
    use std::sync::atomic::AtomicU32;

    #[test]
    fn link_prefetch_clamps_out_of_range_starts_and_lengths() {
        let plain: Vec<u32> = (0..37).collect();
        let atomic: Vec<AtomicU32> = (0..37).map(AtomicU32::new).collect();
        for start in [0, 1, 15, 36, 37, 1_000] {
            for cells in [0, 1, 16, 33, 10_000] {
                plain[..].prefetch(start, cells);
                atomic[..].prefetch(start, cells);
            }
        }
        // Every sub-slice start, so each 64-byte skew is covered.
        for start in 0..plain.len() {
            plain[start..].prefetch(0, 33);
        }
        assert_eq!(LinkCells::get(&plain[..], 36), 36);
    }

    /// An implementor that skips the optional hook still compiles and runs.
    struct Bare(Vec<u32>);

    impl LinkCells for Bare {
        fn get(&self, i: usize) -> u32 {
            self.0[i]
        }
        fn cell_count(&self) -> usize {
            self.0.len()
        }
    }

    #[test]
    fn link_prefetch_defaults_to_a_no_op() {
        let bare = Bare(vec![7; 4]);
        bare.prefetch(0, 4);
        assert_eq!(bare.get(3), 7);
    }
}

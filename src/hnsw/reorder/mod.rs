//! Graph-locality node permutations for sealed and compacted HNSW segments.
//!
//! In ingest order, adjacent neighbors in the layer-0 graph sit at unrelated row
//! indices, so each expansion touches distinct cache lines (`visited`, `inv_norm`,
//! `links0`) and distinct 4 KiB pages (`rows`). Reordering nodes so layer-0
//! neighbors receive nearby indices packs neighborhoods onto shared cache lines
//! and OS pages:
//!
//! - [`LocalityOrder::Rcm`]: symmetrized `O(V + E)` Reverse Cuthill–McKee pass
//!   seeded from 2-sweep pseudo-peripheral vertices per connected component.
//! - [`LocalityOrder::RecursiveBisection`]: bounded top-down balanced graph
//!   bisection over the symmetrized layer-0 graph down to page-sized leaves
//!   (`max(8, min(64, 4096 / (4 · dim)))`), refined at each cut by linear-time
//!   bucket-sorted Fiduccia–Mattheyses swap passes.
//!
//! At non-partitioned compaction, the caller applies the permutation globally
//! across the segment (`.hnsw`, `.exact`, `Bm25Store`, `parent_ids`, WAL, and
//! fresh lazy column filter structures), so `NodeMap` is absent (`0 B/row`). At
//! seal (`Flush`) or partitioned compaction, external slot IDs remain fixed and
//! [`NodeMap`] persists the `node <-> row` bijection inside `PMHNSW01`.

mod bisect;
mod csr;
mod metrics;
mod permute;
mod rcm;

pub use metrics::LocalityMetrics;
pub(super) use metrics::{compute_locality_metrics, walk_row_4k_pages};
pub(super) use permute::{permute_store_with_links, unpermute_rows_in_place};

use super::config::LocalityOrder;
use super::graph::{LinkCells, View, NO_NODE};
use bisect::recursive_bisection_from_csr;
use csr::UndirectedCsr;
use rcm::rcm_from_csr;

/// Bijection between internal locality-ordered HNSW node IDs and external
/// segment row IDs (`0..n`), carried only when a segment is reordered locally
/// without a segment-wide compaction remap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeMap {
    /// `node_to_row[node]` = external segment row ID.
    pub node_to_row: Vec<u32>,
    /// `row_to_node[ext_row]` = internal HNSW node ID.
    pub row_to_node: Vec<u32>,
}

impl NodeMap {
    /// Construct both directions from a `new_to_old` (`node_to_row`) permutation.
    pub fn from_new_to_old(node_to_row: Vec<u32>) -> Self {
        let n = node_to_row.len();
        let mut row_to_node = vec![NO_NODE; n];
        for (node, &ext_row) in node_to_row.iter().enumerate() {
            debug_assert!((ext_row as usize) < n);
            row_to_node[ext_row as usize] = node as u32;
        }
        debug_assert!(!row_to_node.contains(&NO_NODE));
        Self {
            node_to_row,
            row_to_node,
        }
    }

    #[inline]
    pub fn heap_bytes(&self) -> usize {
        (self.node_to_row.len() + self.row_to_node.len()) * 4
    }
}

/// Compute `new_to_old` (`node -> old_node`) for `view` under `order`.
pub(super) fn compute_locality_order<L: LinkCells + ?Sized>(
    view: &View<'_, L>,
    order: LocalityOrder,
) -> Vec<u32> {
    let n = view.n();
    if n <= 2 || !view.is_linked() || order == LocalityOrder::None {
        return (0..n as u32).collect();
    }
    let csr = UndirectedCsr::from_view(view);
    match order {
        LocalityOrder::None => (0..n as u32).collect(),
        LocalityOrder::Rcm => rcm_from_csr(&csr),
        LocalityOrder::RecursiveBisection => {
            let leaf_cap = (4096 / (4 * view.dim.max(1))).clamp(8, 64);
            recursive_bisection_from_csr(&csr, leaf_cap)
        }
    }
}

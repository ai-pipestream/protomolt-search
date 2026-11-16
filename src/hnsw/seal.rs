//! Sealing a heap index: the bulk link build, the locality reorder, the
//! image write, and the compaction and merge entry points.

use std::path::Path;

use crate::vector::{Mask, VectorError};

use super::graph::{self, Store};
use super::rows::RowSource;
use super::{
    image, merge, reorder, Backing, BuildMode, HnswConfig, HnswIndex, LocalityOrder, MergeStats,
    MergeStrategy, SegmentMergeInput, HNSW, MIN_GRAPH_ROWS, PARALLEL_MIN_BATCH,
};

impl HnswIndex {
    /// Permute this heap index in-place for a global segment compaction remap,
    /// leaving `node_map` absent (`0 B/row` overhead) and returning
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
            let parallel = parallel_build(&self.config, store);
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
    /// across deleted neighbors via 2-hop surviving bridges and
    /// verifying/repairing directed layer-0 reachability from `entry`.
    /// Returns `(compacted_index, survivor_old_ids, stats)`.
    pub fn heal_tombstones(
        &self,
        live: Mask<'_>,
    ) -> Result<(Self, Vec<u32>, MergeStats), VectorError> {
        merge::heal_tombstones(self, live)
    }

    /// Merge `segments` (each filtered by its optional `live` mask) plus
    /// optional `unlinked_vectors` into one dense [`HnswIndex`] using
    /// `strategy`.
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

    /// Build the graph in bulk when the heap store is unlinked and holds at
    /// least [`MIN_GRAPH_ROWS`] rows, then apply the configured locality
    /// order. A smaller store stays unlinked.
    pub(super) fn seal_in_place(&mut self) -> Result<(), VectorError> {
        if let Backing::Heap(store) = &mut self.backing {
            if !store.is_linked() && store.n() >= MIN_GRAPH_ROWS {
                let parallel = parallel_build(&self.config, store);
                let links = store.build_links(parallel)?;
                store.install_links(links);
            }
        }
        self.reorder_local_in_place();
        Ok(())
    }

    /// Write the image of this heap index to `path` as [`Self::seal_in_place`]
    /// would leave it, without mutating the index: an unlinked store of at
    /// least [`MIN_GRAPH_ROWS`] rows is linked for the write, and a linked
    /// store not yet reordered is written in the configured locality order.
    pub(super) fn write_image(&self, path: &Path) -> Result<(), VectorError> {
        let Backing::Heap(store) = &self.backing else {
            return Err(VectorError::new(format!(
                "{HNSW} image is served from a map; copy the file instead of rewriting it"
            )));
        };
        let order = self.config.locality_order;
        if !store.is_linked() && store.n() >= MIN_GRAPH_ROWS {
            let links = store.build_links(parallel_build(&self.config, store))?;
            if order == LocalityOrder::None {
                return image::write_with_links(store, Some(&links), &self.config, path);
            }
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
            let new_to_old = reorder::compute_locality_order(&temp_view, order);
            let reordered = reorder::permute_store_with_links(store, &links, &new_to_old, true);
            image::write(&reordered, &self.config, path)
        } else if store.is_linked() && !store.is_reordered() && order != LocalityOrder::None {
            let links = store.links().expect("linked");
            let new_to_old = reorder::compute_locality_order(&store.view(), order);
            let reordered = reorder::permute_store_with_links(store, links, &new_to_old, true);
            image::write(&reordered, &self.config, path)
        } else {
            image::write(store, &self.config, path)
        }
    }
}

/// Whether a bulk build over `store` forks: the parallel build mode and a
/// batch worth the fork.
fn parallel_build(config: &HnswConfig, store: &Store) -> bool {
    config.build_mode == BuildMode::Parallel && store.n() >= PARALLEL_MIN_BATCH
}

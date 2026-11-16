//! Construction, loading, and the row and permutation accessors of
//! [`HnswIndex`].

use std::path::Path;

use crate::vector::{VectorError, VectorIndex, VectorProvider};

use super::graph::Store;
use super::{
    image, reorder, Backing, HnswConfig, HnswIndex, Kernel, LocalityMetrics, RowSource,
    RowSourceIdentity, HNSW, MAX_DIM,
};

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

    /// Structural layer-0 locality summary.
    pub fn locality_metrics(&self) -> LocalityMetrics {
        with_view!(self, |v| reorder::compute_locality_metrics(&v))
    }

    /// Count distinct 4 KB vector-row pages touched during an unfiltered
    /// layer-0 walk at beam `ef`.
    pub fn walk_row_4k_pages(&self, query: &[f32], ef: usize) -> usize {
        with_view!(self, |v| reorder::walk_row_4k_pages(&v, query, ef))
    }

    /// Disable OS readahead and unmap active PTEs on a memory-mapped `.hnsw`
    /// image (`MADV_RANDOM` + `MADV_DONTNEED`) so subsequent queries measure
    /// cold page-table / page-cache fault behavior.
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
    /// provider's own rows: dimension, row count, content digest in
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

    /// Append the stored FP32 bytes of external row `row`, little-endian.
    pub(super) fn row_bytes_into(&self, row: usize, out: &mut Vec<u8>) {
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

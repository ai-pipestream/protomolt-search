//! The heap row store and the link arrays a build fills in.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering as Atomic};
use std::sync::Mutex;

use crate::vector::VectorError;

use super::super::distance::Kernel;
use super::super::reorder::{unpermute_rows_in_place, NodeMap};
use super::super::rows::{OwnedRows, RowSource};
#[cfg(doc)]
use super::super::HnswIndex;
use super::super::HNSW;
use super::{LinkBuilder, View, MAX_LEVEL, MAX_ROWS, NO_NODE};

static EMPTY_CELLS: [AtomicU32; 0] = [];

/// Built adjacency and skeleton arrays over a fixed `(rows, inv_norm)` slice.
///
/// Kept `None` on an unsealed heap tail or a tiny segment below
/// `MIN_GRAPH_ROWS`, materialized in place by [`Store::build_links`] when
/// sealing (`HnswIndex::write`) or preparing (`HnswIndex::prepare`), and
/// populated on [`HnswIndex::load`] when the persisted image carries links.
pub struct GraphLinks {
    pub level: Vec<u8>,
    pub upper_slot: Vec<u32>,
    pub upper_base: Vec<u32>,
    pub links0: Vec<AtomicU32>,
    pub upper: Vec<AtomicU32>,
    /// `(top << 32) | entry`, `entry == NO_NODE` while empty.
    entry: AtomicU64,
    /// Serializes entry publication; held through the insertion of a node
    /// that raises the top level, so no other insertion starts from an
    /// entry whose upper lists are not yet written.
    pub(super) entry_lock: Mutex<()>,
}

impl GraphLinks {
    pub fn from_parts(
        level: Vec<u8>,
        upper_slot: Vec<u32>,
        upper_base: Vec<u32>,
        links0: Vec<AtomicU32>,
        upper: Vec<AtomicU32>,
        entry: u32,
        top: u8,
    ) -> Self {
        Self {
            level,
            upper_slot,
            upper_base,
            links0,
            upper,
            entry: AtomicU64::new(pack_entry(entry, top)),
            entry_lock: Mutex::new(()),
        }
    }

    pub fn entry_state(&self) -> (u32, u8) {
        unpack_entry(self.entry.load(Atomic::Acquire))
    }

    pub(in crate::hnsw) fn publish_entry(&self, node: u32, top: u8) {
        self.entry.store(pack_entry(node, top), Atomic::Release);
    }

    fn heap_bytes(&self) -> usize {
        self.level.len()
            + (self.upper_slot.len() + self.upper_base.len() + self.links0.len() + self.upper.len())
                * 4
    }
}

/// The heap store: owns FP32 rows and reciprocal norms during online tail
/// ingestion, and holds [`GraphLinks`] only once built at seal/prepare or
/// loaded from a linked image.
pub struct Store {
    pub dim: usize,
    pub m: usize,
    pub m0: usize,
    pub seed: u64,
    pub ef_construction: usize,
    pub kernel: Kernel,
    pub(in crate::hnsw) level_mult: f64,
    pub rows: OwnedRows,
    pub inv_norm: Vec<f32>,
    links: Option<GraphLinks>,
    reordered: bool,
    node_map: Option<NodeMap>,
}

/// The level of `node` under `seed`: a geometric draw with the paper's
/// multiplier `1/ln(M)`, from a splitmix64 hash of the node id, so levels
/// depend on neither insertion order nor thread scheduling.
pub fn level_for(seed: u64, node: u32, level_mult: f64) -> u8 {
    let mut z = seed ^ u64::from(node).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^= z >> 31;
    // (0, 1]: never zero, so the logarithm is finite.
    let u = ((z >> 11) as f64 + 1.0) / (1u64 << 53) as f64;
    let level = (-u.ln() * level_mult).floor();
    if level >= f64::from(MAX_LEVEL) {
        MAX_LEVEL
    } else {
        level as u8
    }
}

impl Store {
    pub fn new(dim: usize, m: usize, ef_construction: usize, seed: u64, kernel: Kernel) -> Self {
        Self {
            dim,
            m,
            m0: 2 * m,
            seed,
            ef_construction,
            kernel,
            level_mult: 1.0 / (m as f64).ln(),
            rows: OwnedRows::new(dim),
            inv_norm: Vec::new(),
            links: None,
            reordered: false,
            node_map: None,
        }
    }

    /// Reassemble a store from decoded arrays (an image loaded onto the
    /// heap). The caller has validated them; an unlinked image (`entry ==
    /// NO_NODE`) leaves `links` absent.
    #[allow(clippy::too_many_arguments)]
    pub fn from_parts(
        dim: usize,
        m: usize,
        ef_construction: usize,
        seed: u64,
        kernel: Kernel,
        rows: Vec<f32>,
        inv_norm: Vec<f32>,
        level: Vec<u8>,
        upper_slot: Vec<u32>,
        upper_base: Vec<u32>,
        links0: Vec<AtomicU32>,
        upper: Vec<AtomicU32>,
        entry: u32,
        top: u8,
    ) -> Self {
        let mut store = Self::new(dim, m, ef_construction, seed, kernel);
        store.rows = OwnedRows::from_values(dim, rows);
        store.inv_norm = inv_norm;
        if entry != NO_NODE {
            store.links = Some(GraphLinks::from_parts(
                level, upper_slot, upper_base, links0, upper, entry, top,
            ));
        }
        store
    }

    #[inline]
    pub fn n(&self) -> usize {
        self.inv_norm.len()
    }

    #[inline]
    pub fn is_linked(&self) -> bool {
        self.links
            .as_ref()
            .is_some_and(|l| l.entry_state().0 != NO_NODE)
    }

    #[inline]
    pub fn links(&self) -> Option<&GraphLinks> {
        self.links.as_ref()
    }

    #[inline]
    pub fn is_reordered(&self) -> bool {
        self.reordered
    }

    #[inline]
    pub fn node_map(&self) -> Option<&NodeMap> {
        self.node_map.as_ref()
    }

    pub(in crate::hnsw) fn take_node_map(&mut self) -> Option<NodeMap> {
        self.node_map.take()
    }

    pub(in crate::hnsw) fn set_reorder_state(
        &mut self,
        reordered: bool,
        node_map: Option<NodeMap>,
    ) {
        self.reordered = reordered;
        self.node_map = node_map;
    }

    pub fn entry_state(&self) -> (u32, u8) {
        self.links
            .as_ref()
            .map_or((NO_NODE, 0), GraphLinks::entry_state)
    }

    pub fn heap_bytes(&self) -> usize {
        (self.rows.values().len() + self.inv_norm.len()) * 4
            + self.links.as_ref().map_or(0, GraphLinks::heap_bytes)
            + self.node_map.as_ref().map_or(0, NodeMap::heap_bytes)
    }

    pub fn view(&self) -> View<'_, [AtomicU32]> {
        let (level, upper_slot, upper_base, links0, upper, (entry, top)) = match &self.links {
            Some(links) => (
                links.level.as_slice(),
                links.upper_slot.as_slice(),
                links.upper_base.as_slice(),
                links.links0.as_slice(),
                links.upper.as_slice(),
                links.entry_state(),
            ),
            None => (
                &[][..],
                &[][..],
                &[][..],
                &EMPTY_CELLS[..],
                &EMPTY_CELLS[..],
                (NO_NODE, 0),
            ),
        };
        View {
            dim: self.dim,
            m: self.m,
            m0: self.m0,
            rows: self.rows.values(),
            inv_norm: &self.inv_norm,
            level,
            upper_slot,
            upper_base,
            links0,
            upper,
            entry,
            top,
            kernel: self.kernel,
        }
    }

    /// Append validated rows to the unlinked store (`rows` + `inv_norm`
    /// only). A heap store that holds built links (for example a
    /// non-segmented single-image node that flushed and then accepts new
    /// writes) drops them, so queries enumerate cleanly until the next bulk
    /// build at `prepare` / `write`.
    pub fn append_unlinked(&mut self, vectors: &[f32]) -> Result<(), VectorError> {
        let count = vectors.len() / self.dim;
        let first = self.n();
        if first + count > MAX_ROWS {
            return Err(VectorError::new(format!(
                "{HNSW} graph cannot address {} rows; the limit is {MAX_ROWS}",
                first + count
            )));
        }
        if count == 0 {
            return Ok(());
        }
        self.links = None;
        unpermute_rows_in_place(self);
        self.rows.reserve(vectors.len());
        self.inv_norm.reserve(count);
        for row in vectors.chunks_exact(self.dim) {
            let norm = self.kernel.dot(row, row).sqrt();
            self.rows.push(row);
            self.inv_norm.push(if norm > 0.0 && norm.is_finite() {
                1.0 / norm
            } else {
                0.0
            });
        }
        Ok(())
    }

    /// Build a full [`GraphLinks`] over `0..self.n()` without mutating or
    /// copying `self.rows` / `self.inv_norm`, so sealing a frozen tail can
    /// run lock-free alongside concurrent readers.
    pub fn build_links(&self, parallel: bool) -> Result<GraphLinks, VectorError> {
        let n = self.n();
        if n == 0 {
            return Ok(GraphLinks::from_parts(
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                NO_NODE,
                0,
            ));
        }
        let stride0 = self.m0 + 1;
        let stride_up = self.m + 1;
        let mut level = Vec::with_capacity(n);
        let mut upper_slot = Vec::with_capacity(n);
        let mut upper_base = Vec::new();
        let mut upper_cells = 0usize;
        for i in 0..n {
            let lvl = level_for(self.seed, i as u32, self.level_mult);
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
        let links0: Vec<AtomicU32> = (0..n * stride0).map(|_| AtomicU32::new(0)).collect();
        let upper: Vec<AtomicU32> = (0..upper_cells).map(|_| AtomicU32::new(0)).collect();
        let links =
            GraphLinks::from_parts(level, upper_slot, upper_base, links0, upper, NO_NODE, 0);
        let builder = LinkBuilder::new(self, &links);
        let range = 0..n as u32;
        if parallel {
            builder.insert_parallel(range);
            super::super::repair::repair_connectivity(&builder);
        } else {
            builder.insert_sequential(range);
        }
        Ok(links)
    }

    pub fn install_links(&mut self, links: GraphLinks) {
        debug_assert_eq!(links.level.len(), self.n());
        self.links = Some(links);
    }
}

fn pack_entry(entry: u32, top: u8) -> u64 {
    (u64::from(top) << 32) | u64::from(entry)
}

fn unpack_entry(packed: u64) -> (u32, u8) {
    (packed as u32, (packed >> 32) as u8)
}

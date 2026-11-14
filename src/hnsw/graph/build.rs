//! Heap row store and sequential/parallel HNSW link construction.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering as Atomic};
use std::sync::Mutex;

use crate::vector::VectorError;

use super::super::distance::Kernel;
use super::super::reorder::{unpermute_rows_in_place, NodeMap};
use super::super::rows::{OwnedRows, RowSource};
#[cfg(doc)]
use super::super::HnswIndex;
use super::super::HNSW;
use super::{
    with_scratch, FloorPrune, Locks, NoHook, Patience, Scored, Scratch, SearchStats, View,
    MAX_LEVEL, MAX_ROWS, NO_NODE,
};

/// Minimum nodes per Rayon task during parallel HNSW insertion so small
/// segments are not shattered into single-row work-stealing tasks.
const PARALLEL_MIN_TASK_LEN: usize = 64;

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
    entry_lock: Mutex<()>,
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
        match &self.links {
            Some(links) => {
                let (entry, top) = links.entry_state();
                View {
                    dim: self.dim,
                    m: self.m,
                    m0: self.m0,
                    rows: self.rows.values(),
                    inv_norm: &self.inv_norm,
                    level: &links.level,
                    upper_slot: &links.upper_slot,
                    upper_base: &links.upper_base,
                    links0: links.links0.as_slice(),
                    upper: links.upper.as_slice(),
                    entry,
                    top,
                    kernel: self.kernel,
                }
            }
            None => View {
                dim: self.dim,
                m: self.m,
                m0: self.m0,
                rows: self.rows.values(),
                inv_norm: &self.inv_norm,
                level: &[],
                upper_slot: &[],
                upper_base: &[],
                links0: &EMPTY_CELLS,
                upper: &EMPTY_CELLS,
                entry: NO_NODE,
                top: 0,
                kernel: self.kernel,
            },
        }
    }

    /// Append validated rows to the unlinked store (`rows` + `inv_norm`
    /// only). If this heap store previously held built links (for example
    /// a non-segmented single-image node that flushed and then accepted
    /// new writes), those links are dropped so queries enumerate cleanly
    /// until the next bulk build at `prepare` / `write`.
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
        let links0: Vec<AtomicU32> = std::iter::repeat_with(|| AtomicU32::new(0))
            .take(n * stride0)
            .collect();
        let upper: Vec<AtomicU32> = std::iter::repeat_with(|| AtomicU32::new(0))
            .take(upper_cells)
            .collect();
        let links =
            GraphLinks::from_parts(level, upper_slot, upper_base, links0, upper, NO_NODE, 0);
        let builder = LinkBuilder {
            dim: self.dim,
            m: self.m,
            m0: self.m0,
            ef_construction: self.ef_construction,
            kernel: self.kernel,
            rows: self.rows.values(),
            inv_norm: &self.inv_norm,
            links: &links,
        };
        let range = 0..n as u32;
        if parallel {
            builder.insert_parallel(range);
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

pub(in crate::hnsw) struct LinkBuilder<'a> {
    pub(in crate::hnsw) dim: usize,
    pub(in crate::hnsw) m: usize,
    pub(in crate::hnsw) m0: usize,
    pub(in crate::hnsw) ef_construction: usize,
    pub(in crate::hnsw) kernel: Kernel,
    pub(in crate::hnsw) rows: &'a [f32],
    pub(in crate::hnsw) inv_norm: &'a [f32],
    pub(in crate::hnsw) links: &'a GraphLinks,
}

impl<'a> LinkBuilder<'a> {
    pub(in crate::hnsw) fn new(store: &'a Store, links: &'a GraphLinks) -> Self {
        Self {
            dim: store.dim,
            m: store.m,
            m0: store.m0,
            ef_construction: store.ef_construction,
            kernel: store.kernel,
            rows: store.rows.values(),
            inv_norm: &store.inv_norm,
            links,
        }
    }

    pub(in crate::hnsw) fn view(&self) -> View<'_, [AtomicU32]> {
        let (entry, top) = self.links.entry_state();
        View {
            dim: self.dim,
            m: self.m,
            m0: self.m0,
            rows: self.rows,
            inv_norm: self.inv_norm,
            level: &self.links.level,
            upper_slot: &self.links.upper_slot,
            upper_base: &self.links.upper_base,
            links0: self.links.links0.as_slice(),
            upper: self.links.upper.as_slice(),
            entry,
            top,
            kernel: self.kernel,
        }
    }

    /// Seed an initial navigable skeleton sequentially when `self.links` is
    /// empty (`entry == NO_NODE`), returning the prefix count to insert on one
    /// thread before forking across Rayon workers. Without a connected entry
    /// neighborhood and upper-layer skeleton, dozens of worker threads racing
    /// at cold start search a near-empty graph and contend on `entry_lock`.
    pub(super) fn bootstrap_prefix_len(&self, total: usize) -> usize {
        if self.links.entry_state().0 != NO_NODE {
            return 0;
        }
        self.ef_construction.max(2 * self.m0).min(total)
    }

    fn insert_sequential(&self, range: std::ops::Range<u32>) {
        with_scratch(|scratch| {
            for node in range {
                self.insert(node, None, scratch);
            }
        });
    }

    fn insert_parallel(&self, range: std::ops::Range<u32>) {
        use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator};
        let boot = self.bootstrap_prefix_len(range.len()) as u32;
        if boot > 0 {
            self.insert_sequential(range.start..range.start + boot);
        }
        let rest = range.start + boot..range.end;
        if rest.is_empty() {
            return;
        }
        if rest.len() < PARALLEL_MIN_TASK_LEN {
            self.insert_sequential(rest);
            return;
        }
        let locks = Locks::default();
        rest.into_par_iter()
            .with_min_len(PARALLEL_MIN_TASK_LEN)
            .for_each(|node| with_scratch(|scratch| self.insert(node, Some(&locks), scratch)));
    }

    pub(in crate::hnsw) fn insert_slice(&self, nodes: &[u32], parallel: bool) {
        if nodes.is_empty() {
            return;
        }
        if !parallel {
            with_scratch(|scratch| {
                for &node in nodes {
                    self.insert(node, None, scratch);
                }
            });
            return;
        }
        let boot = self.bootstrap_prefix_len(nodes.len());
        if boot > 0 {
            with_scratch(|scratch| {
                for &node in &nodes[..boot] {
                    self.insert(node, None, scratch);
                }
            });
        }
        let rest = &nodes[boot..];
        if rest.is_empty() {
            return;
        }
        if rest.len() < PARALLEL_MIN_TASK_LEN {
            with_scratch(|scratch| {
                for &node in rest {
                    self.insert(node, None, scratch);
                }
            });
            return;
        }
        use rayon::iter::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator};
        let locks = Locks::default();
        rest.par_iter()
            .with_min_len(PARALLEL_MIN_TASK_LEN)
            .for_each(|&node| with_scratch(|scratch| self.insert(node, Some(&locks), scratch)));
    }

    pub(in crate::hnsw) fn write_links(
        &self,
        node: u32,
        layer: usize,
        list: &[u32],
        locks: Option<&Locks>,
    ) {
        let view = self.view();
        let start = view.list_start(node, layer);
        let cells = if layer == 0 {
            &self.links.links0
        } else {
            &self.links.upper
        };
        debug_assert!(list.len() <= view.cap(layer));
        let _guard = locks.map(|l| l.lock(node));
        cells[start].store(list.len() as u32, Atomic::Relaxed);
        for (i, &v) in list.iter().enumerate() {
            cells[start + 1 + i].store(v, Atomic::Relaxed);
        }
    }

    /// Write the inserting node's own list at `layer`. Under a parallel
    /// build other insertions may already have linked back into this list
    /// (the node's row was visible to their searches); those links are kept,
    /// and when the union exceeds the cap the heuristic chooses among all of
    /// them, so no concurrent back-link is lost to an overwrite.
    #[allow(clippy::too_many_arguments)]
    fn publish_links(
        &self,
        node: u32,
        layer: usize,
        cap: usize,
        locks: Option<&Locks>,
        sel: &[u32],
        pool: &mut Vec<Scored>,
        scratch: &mut Scratch,
    ) {
        let Some(locks) = locks else {
            self.write_links(node, layer, sel, None);
            return;
        };
        let view = self.view();
        let start = view.list_start(node, layer);
        let cells = if layer == 0 {
            &self.links.links0
        } else {
            &self.links.upper
        };
        let _guard = locks.lock(node);
        let count = (cells[start].load(Atomic::Relaxed) as usize).min(cap);
        let mut merged = std::mem::take(&mut scratch.nbrs);
        merged.clear();
        merged.extend_from_slice(sel);
        for i in 0..count {
            let v = cells[start + 1 + i].load(Atomic::Relaxed);
            if !merged.contains(&v) {
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

    /// Paper Algorithm 1 for one node.
    pub(in crate::hnsw) fn insert(&self, node: u32, locks: Option<&Locks>, scratch: &mut Scratch) {
        let level = self.links.level[node as usize];
        let (mut entry, mut top) = self.links.entry_state();
        let mut raising = None;
        if entry == NO_NODE || level > top {
            let guard = self
                .links
                .entry_lock
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            (entry, top) = self.links.entry_state();
            if entry == NO_NODE || level > top {
                raising = Some(guard);
            }
        }
        if entry == NO_NODE {
            // The first node: nothing to link to.
            self.links.publish_entry(node, level);
            return;
        }
        let view = self.view();
        let q = view.row(node);
        let mut stats = SearchStats::default();
        let mut ep = Scored {
            score: view.kernel.dot(q, view.row(entry)),
            node: entry,
        };
        let mut nbrs = std::mem::take(&mut scratch.nbrs);
        for layer in (usize::from(level) + 1..=usize::from(top)).rev() {
            ep = view.greedy(q, ep, layer, locks, &mut nbrs, &mut stats);
        }
        scratch.nbrs = nbrs;
        let node_inv_norm = view.inv_norm[node as usize];
        for layer in (0..=usize::from(level.min(top))).rev() {
            let cap = view.cap(layer);
            view.search_layer(
                q,
                &[ep],
                self.ef_construction,
                layer,
                None,
                Patience::OFF,
                FloorPrune::OFF,
                locks,
                scratch,
                &mut NoHook,
                &mut stats,
            );
            let mut pool = std::mem::take(&mut scratch.pool);
            pool.clear();
            pool.extend(scratch.res.drain().map(|r| r.0));
            pool.sort_unstable_by(|a, b| b.cmp(a));
            if let Some(best) = pool.first() {
                ep = *best;
            }
            let mut sel = std::mem::take(&mut scratch.sel);
            view.select(node_inv_norm, &pool, cap, &mut sel);
            self.publish_links(node, layer, cap, locks, &sel, &mut pool, scratch);
            for &e in &sel {
                self.connect_back(e, node, layer, cap, locks, &mut pool, scratch);
            }
            scratch.sel = sel;
            scratch.pool = pool;
        }
        if raising.is_some() {
            self.links.publish_entry(node, level);
        }
    }

    /// Add `node` to `e`'s list at `layer`; when the list is full, keep
    /// the heuristic's choice among the old neighbors and the newcomer.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::hnsw) fn connect_back(
        &self,
        e: u32,
        node: u32,
        layer: usize,
        cap: usize,
        locks: Option<&Locks>,
        pool: &mut Vec<Scored>,
        scratch: &mut Scratch,
    ) {
        let view = self.view();
        let start = view.list_start(e, layer);
        let cells = if layer == 0 {
            &self.links.links0
        } else {
            &self.links.upper
        };
        let _guard = locks.map(|l| l.lock(e));
        let count = (cells[start].load(Atomic::Relaxed) as usize).min(cap);
        if count < cap {
            // Already linked by this pair's other insertion: nothing to add.
            if (0..count).any(|i| cells[start + 1 + i].load(Atomic::Relaxed) == node) {
                return;
            }
            cells[start + 1 + count].store(node, Atomic::Relaxed);
            cells[start].store((count + 1) as u32, Atomic::Relaxed);
            return;
        }
        let mut candidates = std::mem::take(&mut scratch.nbrs);
        candidates.clear();
        for i in 0..count {
            let v = cells[start + 1 + i].load(Atomic::Relaxed);
            if v == node {
                scratch.nbrs = candidates;
                return; // already linked by another insertion of this pair
            }
            candidates.push(v);
        }
        candidates.push(node);
        let row_e = view.row(e);
        pool.clear();
        pool.reserve(candidates.len());
        view.for_each_scored(row_e, &candidates, |v, score| {
            pool.push(Scored { score, node: v });
        });
        scratch.nbrs = candidates;
        pool.sort_unstable_by(|a, b| b.cmp(a));
        let mut sel = std::mem::take(&mut scratch.sel);
        view.select(view.inv_norm[e as usize], pool, cap, &mut sel);
        cells[start].store(sel.len() as u32, Atomic::Relaxed);
        for (i, &v) in sel.iter().enumerate() {
            cells[start + 1 + i].store(v, Atomic::Relaxed);
        }
        scratch.sel = sel;
    }
}

fn pack_entry(entry: u32, top: u8) -> u64 {
    (u64::from(top) << 32) | u64::from(entry)
}

fn unpack_entry(packed: u64) -> (u32, u8) {
    (packed as u32, (packed >> 32) as u8)
}

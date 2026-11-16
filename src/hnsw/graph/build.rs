//! Sequential and parallel HNSW link construction over a fixed row store.

use std::sync::atomic::{AtomicU32, Ordering as Atomic};

use super::super::distance::Kernel;
use super::super::rows::RowSource;
use super::{
    with_scratch, FloorPrune, GraphLinks, LayerSearch, Locks, NoHook, Patience, Scored, Scratch,
    SearchStats, Store, View, NO_NODE,
};

/// Minimum nodes per Rayon task during parallel HNSW insertion so small
/// segments are not shattered into single-row work-stealing tasks.
const PARALLEL_MIN_TASK_LEN: usize = 64;

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

    /// Prefix length to insert sequentially on cold start (`entry == NO_NODE`)
    /// before forking across Rayon workers.
    pub(super) fn bootstrap_prefix_len(&self, total: usize) -> usize {
        if self.links.entry_state().0 != NO_NODE {
            return 0;
        }
        self.ef_construction.max(2 * self.m0).min(total)
    }

    pub(super) fn insert_sequential(&self, range: std::ops::Range<u32>) {
        with_scratch(|scratch| {
            for node in range {
                self.insert(node, None, scratch);
            }
        });
    }

    /// Insert `all` in doubling waves: each wave links layer-0 nodes in parallel
    /// over immutable upper layers, then links `level > 0` hubs sequentially so
    /// upper layers and `(entry, top)` stay deterministic with full layer-0 reach.
    fn insert_waves(&self, all: &[u32]) {
        use rayon::iter::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator};
        let boot = self.bootstrap_prefix_len(all.len());
        if boot > 0 {
            with_scratch(|s| all[..boot].iter().for_each(|&n| self.insert(n, None, s)));
        }
        if boot >= all.len() {
            return;
        }
        let locks = Locks::default();
        let mut wave = if boot > 0 {
            boot
        } else {
            super::super::repair::linked_node_count(self).max(1)
        };
        let (mut at, mut zero, mut upper) = (boot, Vec::new(), Vec::new());
        while at < all.len() {
            let end = all.len().min(at + wave);
            zero.clear();
            upper.clear();
            for &n in &all[at..end] {
                if self.links.level[n as usize] > 0 {
                    upper.push(n);
                } else {
                    zero.push(n);
                }
            }
            if zero.len() < PARALLEL_MIN_TASK_LEN {
                with_scratch(|s| zero.iter().for_each(|&n| self.insert(n, None, s)));
            } else {
                zero.par_iter()
                    .with_min_len(PARALLEL_MIN_TASK_LEN)
                    .for_each(|&n| with_scratch(|s| self.insert(n, Some(&locks), s)));
            }
            if !upper.is_empty() {
                with_scratch(|s| upper.iter().for_each(|&n| self.insert(n, None, s)));
            }
            at = end;
            wave = wave.saturating_mul(2);
        }
    }

    pub(super) fn insert_parallel(&self, range: std::ops::Range<u32>) {
        let nodes: Vec<u32> = range.collect();
        self.insert_waves(&nodes);
    }

    pub(in crate::hnsw) fn insert_slice(&self, nodes: &[u32], parallel: bool) {
        if nodes.is_empty() {
            return;
        }
        if !parallel {
            with_scratch(|s| nodes.iter().for_each(|&n| self.insert(n, None, s)));
            return;
        }
        self.insert_waves(nodes);
        super::super::repair::repair_slice_connectivity(self, nodes);
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
    fn publish_links(
        &self,
        node: u32,
        layer: usize,
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
        let cap = view.cap(layer);
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
            let search = LayerSearch {
                q,
                ef: self.ef_construction,
                layer,
                allow: None,
                patience: Patience::OFF,
                floor_prune: FloorPrune::OFF,
                locks,
            };
            view.search_layer(&search, &[ep], scratch, &mut NoHook, &mut stats);
            let mut pool = std::mem::take(&mut scratch.pool);
            pool.clear();
            pool.extend(scratch.res.drain().map(|r| r.0));
            pool.sort_unstable_by(|a, b| b.cmp(a));
            if let Some(best) = pool.first() {
                ep = *best;
            }
            let mut sel = std::mem::take(&mut scratch.sel);
            view.select(node_inv_norm, &pool, cap, &mut sel);
            self.publish_links(node, layer, locks, &sel, &mut pool, scratch);
            for &e in &sel {
                self.connect_back(e, node, layer, locks, &mut pool, scratch);
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
    pub(in crate::hnsw) fn connect_back(
        &self,
        e: u32,
        node: u32,
        layer: usize,
        locks: Option<&Locks>,
        pool: &mut Vec<Scored>,
        scratch: &mut Scratch,
    ) {
        let view = self.view();
        let cap = view.cap(layer);
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

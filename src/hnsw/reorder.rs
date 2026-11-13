//! Graph-locality node permutations for sealed and compacted HNSW segments (`#98`).
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

use std::cell::RefCell;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU32, Ordering as Atomic};

use super::config::LocalityOrder;
use super::graph::{GraphLinks, LinkCells, Store, View, NO_NODE};
use super::rows::{OwnedRows, RowSource};

/// Maximum balanced swap-refinement passes per recursive bisection cut.
const MAX_BISECT_PASSES: usize = 10;
/// Minimum subgraph vertex count to fork left/right halves across Rayon.
const PARALLEL_BISECT_MIN_NODES: usize = 2048;
/// Clamped gain magnitude for `O(V)` counting-bucket sort (`[-GAIN_CAP, +GAIN_CAP]`).
const GAIN_CAP: i32 = 128;
const GAIN_BUCKETS: usize = (2 * GAIN_CAP + 1) as usize;
/// Consecutive non-positive candidate pairs before ending a swap pass early.
const MAX_STALE_PAIRS: usize = 48;

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

/// Symmetrized, deduplicated undirected layer-0 CSR graph.
struct UndirectedCsr {
    offsets: Vec<u32>,
    edges: Vec<u32>,
}

impl UndirectedCsr {
    fn from_view<L: LinkCells + ?Sized>(view: &View<'_, L>) -> Self {
        let n = view.n();
        if n == 0 || !view.is_linked() {
            return Self {
                offsets: vec![0; n + 1],
                edges: Vec::new(),
            };
        }
        let stride0 = view.m0 + 1;
        let mut counts = vec![0u32; n];
        for u in 0..n {
            let start = u * stride0;
            let count = (view.links0.get(start) as usize).min(view.m0);
            for i in 0..count {
                let v = view.links0.get(start + 1 + i) as usize;
                if v < n && v != u {
                    counts[u] += 1;
                    counts[v] += 1;
                }
            }
        }
        let mut raw_offsets = Vec::with_capacity(n + 1);
        let mut total = 0u32;
        raw_offsets.push(0);
        for &c in &counts {
            total = total.saturating_add(c);
            raw_offsets.push(total);
        }
        let mut raw_edges = vec![0u32; total as usize];
        let mut cursor = raw_offsets[..n].to_vec();
        for u in 0..n {
            let start = u * stride0;
            let count = (view.links0.get(start) as usize).min(view.m0);
            for i in 0..count {
                let v = view.links0.get(start + 1 + i) as usize;
                if v < n && v != u {
                    let pos_u = cursor[u] as usize;
                    raw_edges[pos_u] = v as u32;
                    cursor[u] += 1;
                    let pos_v = cursor[v] as usize;
                    raw_edges[pos_v] = u as u32;
                    cursor[v] += 1;
                }
            }
        }
        // Sort and deduplicate each vertex's undirected adjacency list into a
        // compact CSR.
        let mut offsets = Vec::with_capacity(n + 1);
        let mut edges = Vec::with_capacity(raw_edges.len());
        offsets.push(0);
        for u in 0..n {
            let lo = raw_offsets[u] as usize;
            let hi = raw_offsets[u + 1] as usize;
            let slice = &mut raw_edges[lo..hi];
            slice.sort_unstable();
            let mut prev = NO_NODE;
            for &v in slice.iter() {
                if v != prev {
                    edges.push(v);
                    prev = v;
                }
            }
            offsets.push(edges.len() as u32);
        }
        Self { offsets, edges }
    }

    #[inline]
    fn n(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    #[inline]
    fn degree(&self, u: u32) -> u32 {
        let idx = u as usize;
        self.offsets[idx + 1] - self.offsets[idx]
    }

    #[inline]
    fn neighbors(&self, u: u32) -> &[u32] {
        let idx = u as usize;
        &self.edges[self.offsets[idx] as usize..self.offsets[idx + 1] as usize]
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

/// Symmetrized `O(V + E)` Reverse Cuthill–McKee ordering seeded from 2-sweep
/// pseudo-peripheral vertices per connected component.
fn rcm_from_csr(csr: &UndirectedCsr) -> Vec<u32> {
    let n = csr.n();
    if n == 0 {
        return Vec::new();
    }
    // Pre-sort each vertex's neighbor list by ascending `(degree, vertex_id)`
    // for Cuthill-McKee expansion.
    let mut cm_edges = csr.edges.clone();
    for u in 0..n {
        let lo = csr.offsets[u] as usize;
        let hi = csr.offsets[u + 1] as usize;
        cm_edges[lo..hi].sort_unstable_by_key(|&v| (csr.degree(v), v));
    }
    let mut by_deg: Vec<u32> = (0..n as u32).collect();
    by_deg.sort_unstable_by_key(|&u| (csr.degree(u), u));

    let mut placed = vec![false; n];
    let mut bfs_stamp = vec![0u32; n];
    let mut bfs_epoch = 0u32;
    let mut queue: VecDeque<u32> = VecDeque::new();
    let mut order = Vec::with_capacity(n);

    for &candidate in &by_deg {
        if placed[candidate as usize] {
            continue;
        }
        if csr.degree(candidate) == 0 {
            placed[candidate as usize] = true;
            order.push(candidate);
            continue;
        }
        // 2-sweep pseudo-peripheral refinement inside this unplaced component.
        let mut start = candidate;
        for _ in 0..2 {
            bfs_epoch = bfs_epoch.wrapping_add(1);
            if bfs_epoch == 0 {
                bfs_stamp.fill(0);
                bfs_epoch = 1;
            }
            queue.clear();
            bfs_stamp[start as usize] = bfs_epoch;
            queue.push_back(start);
            let mut last_level_best = (u32::MAX, start);
            let mut level_rem = 1usize;
            while let Some(u) = queue.pop_front() {
                let d = csr.degree(u);
                if (d, u) < last_level_best {
                    last_level_best = (d, u);
                }
                for &v in csr.neighbors(u) {
                    if !placed[v as usize] && bfs_stamp[v as usize] != bfs_epoch {
                        bfs_stamp[v as usize] = bfs_epoch;
                        queue.push_back(v);
                    }
                }
                level_rem -= 1;
                if level_rem == 0 && !queue.is_empty() {
                    level_rem = queue.len();
                    last_level_best = (u32::MAX, *queue.front().expect("non-empty"));
                }
            }
            if last_level_best.1 == start {
                break;
            }
            start = last_level_best.1;
        }

        // Standard Cuthill-McKee BFS from `start`.
        placed[start as usize] = true;
        order.push(start);
        let mut head = order.len() - 1;
        while head < order.len() {
            let u = order[head] as usize;
            head += 1;
            let lo = csr.offsets[u] as usize;
            let hi = csr.offsets[u + 1] as usize;
            for &v in &cm_edges[lo..hi] {
                if !placed[v as usize] {
                    placed[v as usize] = true;
                    order.push(v);
                }
            }
        }
    }
    debug_assert_eq!(order.len(), n);
    order.reverse();
    order
}

/// Per-worker epoch-stamped global-to-local vertex mapper for induced subgraphs.
#[derive(Default)]
struct BisectScratch {
    stamp: Vec<u32>,
    local_idx: Vec<u32>,
    epoch: u32,
}

impl BisectScratch {
    fn prepare(&mut self, global_n: usize) -> u32 {
        if self.stamp.len() < global_n {
            self.stamp.resize(global_n, 0);
            self.local_idx.resize(global_n, 0);
        }
        self.epoch = self.epoch.wrapping_add(1);
        if self.epoch == 0 {
            self.stamp.fill(0);
            self.epoch = 1;
        }
        self.epoch
    }
}

thread_local! {
    static BISECT_SCRATCH: RefCell<Option<Box<BisectScratch>>> = const { RefCell::new(None) };
}

fn with_bisect_scratch<R>(global_n: usize, f: impl FnOnce(&mut BisectScratch, u32) -> R) -> R {
    let mut scratch = BISECT_SCRATCH
        .with(|cell| cell.borrow_mut().take())
        .unwrap_or_default();
    let epoch = scratch.prepare(global_n);
    let out = f(&mut scratch, epoch);
    BISECT_SCRATCH.with(|cell| {
        let mut slot = cell.borrow_mut();
        if slot.is_none() {
            *slot = Some(scratch);
        }
    });
    out
}

/// Compact induced subgraph over `0..k` for one bisection subproblem.
struct LocalCsr {
    offsets: Vec<u32>,
    edges: Vec<u32>,
}

impl LocalCsr {
    fn extract(csr: &UndirectedCsr, nodes: &[u32]) -> Self {
        let k = nodes.len();
        with_bisect_scratch(csr.n(), |bs, epoch| {
            for (local, &u) in nodes.iter().enumerate() {
                let gu = u as usize;
                bs.stamp[gu] = epoch;
                bs.local_idx[gu] = local as u32;
            }
            let mut offsets = Vec::with_capacity(k + 1);
            let mut edges = Vec::with_capacity(k * 16);
            offsets.push(0);
            for &u in nodes {
                let start = edges.len();
                for &v in csr.neighbors(u) {
                    let gv = v as usize;
                    if bs.stamp[gv] == epoch {
                        edges.push(bs.local_idx[gv]);
                    }
                }
                edges[start..].sort_unstable();
                offsets.push(edges.len() as u32);
            }
            Self { offsets, edges }
        })
    }

    #[inline]
    fn k(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    #[inline]
    fn degree(&self, i: usize) -> usize {
        (self.offsets[i + 1] - self.offsets[i]) as usize
    }

    #[inline]
    fn neighbors(&self, i: usize) -> &[u32] {
        &self.edges[self.offsets[i] as usize..self.offsets[i + 1] as usize]
    }

    #[inline]
    fn has_edge(&self, u: usize, v: usize) -> bool {
        self.neighbors(u).binary_search(&(v as u32)).is_ok()
    }
}

fn recursive_bisection_from_csr(csr: &UndirectedCsr, leaf_cap: usize) -> Vec<u32> {
    let mut nodes = rcm_from_csr(csr);
    bisect_slice(csr, &mut nodes, leaf_cap);
    nodes
}

fn bisect_slice(csr: &UndirectedCsr, nodes: &mut [u32], leaf_cap: usize) {
    let k = nodes.len();
    if k <= leaf_cap {
        if k > 2 {
            order_leaf_locally(csr, nodes);
        }
        return;
    }
    let local = LocalCsr::extract(csr, nodes);
    if local.edges.is_empty() {
        return;
    }
    let half = k / 2;
    let mut side = initial_overlap_bisection(&local, half);
    refine_bisection(&local, &mut side, half);

    let mut left = Vec::with_capacity(half);
    let mut right = Vec::with_capacity(k - half);
    for (i, &u) in nodes.iter().enumerate() {
        if side[i] == 0 {
            left.push(u);
        } else {
            right.push(u);
        }
    }
    debug_assert_eq!(left.len(), half);
    debug_assert_eq!(right.len(), k - half);
    nodes[..half].copy_from_slice(&left);
    nodes[half..].copy_from_slice(&right);
    drop(left);
    drop(right);
    drop(side);
    drop(local);

    let (left_slice, right_slice) = nodes.split_at_mut(half);
    if k >= PARALLEL_BISECT_MIN_NODES {
        rayon::join(
            || bisect_slice(csr, left_slice, leaf_cap),
            || bisect_slice(csr, right_slice, leaf_cap),
        );
    } else {
        bisect_slice(csr, left_slice, leaf_cap);
        bisect_slice(csr, right_slice, leaf_cap);
    }
}

/// Order a page-sized leaf (`k <= 64`) by local RCM so intra-page 64 B cache
/// lines also cluster immediate neighbors.
fn order_leaf_locally(csr: &UndirectedCsr, nodes: &mut [u32]) {
    let local = LocalCsr::extract(csr, nodes);
    if local.edges.is_empty() {
        return;
    }
    let k = local.k();
    let mut placed = [false; 64];
    let mut order = Vec::with_capacity(k);
    for start in 0..k {
        if placed[start] {
            continue;
        }
        placed[start] = true;
        order.push(start);
        let mut head = order.len() - 1;
        while head < order.len() {
            let u = order[head];
            head += 1;
            for &v in local.neighbors(u) {
                let vi = v as usize;
                if !placed[vi] {
                    placed[vi] = true;
                    order.push(vi);
                }
            }
        }
    }
    order.reverse();
    let orig = nodes.to_vec();
    for (dst, &src_idx) in nodes.iter_mut().zip(&order) {
        *dst = orig[src_idx];
    }
}

/// Seed a balanced `(half, k - half)` split by growing `side = 0` via an
/// `O(V + E)` bucketed max-overlap queue from a peripheral vertex so dense
/// intra-cluster neighborhoods fill completely before crossing sparse
/// small-world HNSW shortcut edges.
fn initial_overlap_bisection(local: &LocalCsr, half: usize) -> Vec<u8> {
    const MAX_OVERLAP_BUCKET: usize = 64;

    let k = local.k();
    if half == 0 || half >= k {
        return vec![1u8; k];
    }
    let mut seed = 0usize;
    let mut best_deg = usize::MAX;
    for i in 0..k {
        let d = local.degree(i);
        if d > 0 && d < best_deg {
            best_deg = d;
            seed = i;
        }
    }
    // One peripheral BFS hop to move `seed` away from any central hub.
    let mut seen = vec![false; k];
    let mut queue = Vec::with_capacity(k);
    seen[seed] = true;
    queue.push(seed as u32);
    let mut q_head = 0usize;
    while q_head < queue.len() {
        let u = queue[q_head] as usize;
        q_head += 1;
        for &v in local.neighbors(u) {
            let vi = v as usize;
            if !seen[vi] {
                seen[vi] = true;
                queue.push(v);
            }
        }
    }
    if let Some(&farthest) = queue.last() {
        seed = farthest as usize;
    }

    let mut side = vec![1u8; k];
    let mut overlap = vec![0u16; k];
    let mut buckets: Vec<Vec<u32>> = (0..=MAX_OVERLAP_BUCKET).map(|_| Vec::new()).collect();
    let mut heads = [0usize; MAX_OVERLAP_BUCKET + 1];
    let mut top_b = 0usize;
    let mut assigned = 0usize;
    let mut fallback_scan = 0usize;

    let place = |u: usize,
                 side: &mut [u8],
                 overlap: &mut [u16],
                 buckets: &mut [Vec<u32>],
                 top_b: &mut usize| {
        side[u] = 0;
        for &v in local.neighbors(u) {
            let vi = v as usize;
            if side[vi] == 0 {
                continue;
            }
            let next_c = (usize::from(overlap[vi]) + 1).min(MAX_OVERLAP_BUCKET);
            overlap[vi] = next_c as u16;
            buckets[next_c].push(v);
            if next_c > *top_b {
                *top_b = next_c;
            }
        }
    };

    place(seed, &mut side, &mut overlap, &mut buckets, &mut top_b);
    assigned += 1;

    while assigned < half {
        let mut chosen = None;
        while top_b > 0 {
            let b_vec = &buckets[top_b];
            let mut h = heads[top_b];
            while h < b_vec.len() {
                let cand = b_vec[h] as usize;
                h += 1;
                if side[cand] != 0 && (usize::from(overlap[cand]).min(MAX_OVERLAP_BUCKET) == top_b)
                {
                    chosen = Some(cand);
                    break;
                }
            }
            heads[top_b] = h;
            if chosen.is_some() {
                break;
            }
            top_b -= 1;
        }
        let u = match chosen {
            Some(c) => c,
            None => {
                while fallback_scan < k && side[fallback_scan] == 0 {
                    fallback_scan += 1;
                }
                fallback_scan
            }
        };
        place(u, &mut side, &mut overlap, &mut buckets, &mut top_b);
        assigned += 1;
    }
    side
}

#[inline]
fn live_gain(local: &LocalCsr, side: &[u8], u: usize) -> i32 {
    let my_side = side[u];
    let mut gain = 0i32;
    for &v in local.neighbors(u) {
        if side[v as usize] == my_side {
            gain -= 1;
        } else {
            gain += 1;
        }
    }
    gain
}

#[inline]
fn gain_bucket(gain: i32) -> usize {
    let clamped = gain.clamp(-GAIN_CAP, GAIN_CAP);
    (GAIN_CAP - clamped) as usize
}

/// Refine `side` (`half` zeros, `k - half` ones) using `O(k)` bucket-sorted
/// balanced vertex-pair swaps that strictly reduce undirected cut edges.
fn refine_bisection(local: &LocalCsr, side: &mut [u8], half: usize) {
    let k = local.k();
    if half == 0 || half >= k {
        return;
    }
    let mut gains = vec![0i32; k];
    let mut left_sorted = vec![0u32; half];
    let mut right_sorted = vec![0u32; k - half];

    for _ in 0..MAX_BISECT_PASSES {
        let mut counts_l = [0usize; GAIN_BUCKETS];
        let mut counts_r = [0usize; GAIN_BUCKETS];
        for (u, gain_slot) in gains.iter_mut().enumerate() {
            let g = live_gain(local, side, u);
            *gain_slot = g;
            let b = gain_bucket(g);
            if side[u] == 0 {
                counts_l[b] += 1;
            } else {
                counts_r[b] += 1;
            }
        }
        let mut off_l = [0usize; GAIN_BUCKETS];
        let mut off_r = [0usize; GAIN_BUCKETS];
        let mut acc_l = 0usize;
        let mut acc_r = 0usize;
        for b in 0..GAIN_BUCKETS {
            off_l[b] = acc_l;
            acc_l += counts_l[b];
            off_r[b] = acc_r;
            acc_r += counts_r[b];
        }
        for (u, &g) in gains.iter().enumerate() {
            let b = gain_bucket(g);
            if side[u] == 0 {
                left_sorted[off_l[b]] = u as u32;
                off_l[b] += 1;
            } else {
                right_sorted[off_r[b]] = u as u32;
                off_r[b] += 1;
            }
        }

        let pairs = left_sorted.len().min(right_sorted.len());
        let mut swaps = 0usize;
        let mut stale = 0usize;
        for idx in 0..pairs {
            let u = left_sorted[idx] as usize;
            let v = right_sorted[idx] as usize;
            let gu = live_gain(local, side, u);
            let gv = live_gain(local, side, v);
            let cross = if local.has_edge(u, v) { 2 } else { 0 };
            if gu + gv - cross > 0 {
                side[u] = 1;
                side[v] = 0;
                swaps += 1;
                stale = 0;
            } else {
                stale += 1;
                if gains[u] + gains[v] <= 0 && stale >= MAX_STALE_PAIRS {
                    break;
                }
            }
        }
        if swaps == 0 {
            break;
        }
    }
}

/// Build a new [`Store`] whose `rows`, `inv_norm`, `level`, `upper_slot`,
/// `upper_base`, `links0`, `upper`, and `entry` are permuted according to
/// `new_to_old` (`new_node -> old_node`), preserving each neighbor list's
/// relative `select()` similarity order.
pub(super) fn permute_store_with_links(
    store: &Store,
    links: &GraphLinks,
    new_to_old: &[u32],
    attach_node_map: bool,
) -> Store {
    let n = store.n();
    debug_assert_eq!(new_to_old.len(), n);
    let dim = store.dim;
    let m = store.m;
    let m0 = store.m0;
    let stride0 = m0 + 1;
    let stride_up = m + 1;

    let mut old_to_new = vec![NO_NODE; n];
    for (new_node, &old_node) in new_to_old.iter().enumerate() {
        old_to_new[old_node as usize] = new_node as u32;
    }
    debug_assert!(!old_to_new.contains(&NO_NODE));

    let src_rows = store.rows.values();
    let mut rows = vec![0.0f32; n * dim];
    let mut inv_norm = Vec::with_capacity(n);
    let mut level = Vec::with_capacity(n);
    let mut upper_slot = Vec::with_capacity(n);
    let mut upper_base = Vec::with_capacity(links.upper_base.len());
    let mut upper_cells = 0usize;

    for (new_node, &old_u32) in new_to_old.iter().enumerate() {
        let old = old_u32 as usize;
        rows[new_node * dim..(new_node + 1) * dim]
            .copy_from_slice(&src_rows[old * dim..(old + 1) * dim]);
        inv_norm.push(store.inv_norm[old]);
        let lvl = links.level[old];
        level.push(lvl);
        if lvl == 0 {
            upper_slot.push(NO_NODE);
        } else {
            upper_slot.push(upper_base.len() as u32);
            upper_base.push(upper_cells as u32);
            upper_cells += usize::from(lvl) * stride_up;
        }
    }
    debug_assert_eq!(upper_cells, links.upper.len());

    let mut links0: Vec<AtomicU32> = Vec::with_capacity(n * stride0);
    for &old_u32 in new_to_old {
        let old_start = old_u32 as usize * stride0;
        let count = (links.links0[old_start].load(Atomic::Relaxed) as usize).min(m0);
        links0.push(AtomicU32::new(count as u32));
        for i in 0..count {
            let old_nbr = links.links0[old_start + 1 + i].load(Atomic::Relaxed) as usize;
            links0.push(AtomicU32::new(old_to_new[old_nbr]));
        }
        for _ in count..m0 {
            links0.push(AtomicU32::new(0));
        }
    }

    let mut upper: Vec<AtomicU32> = Vec::with_capacity(upper_cells);
    for (new_node, &old_u32) in new_to_old.iter().enumerate() {
        let lvl = usize::from(level[new_node]);
        if lvl == 0 {
            continue;
        }
        let old_u = links.upper_slot[old_u32 as usize] as usize;
        let old_base = links.upper_base[old_u] as usize;
        for layer_idx in 0..lvl {
            let old_start = old_base + layer_idx * stride_up;
            let count = (links.upper[old_start].load(Atomic::Relaxed) as usize).min(m);
            upper.push(AtomicU32::new(count as u32));
            for i in 0..count {
                let old_nbr = links.upper[old_start + 1 + i].load(Atomic::Relaxed) as usize;
                upper.push(AtomicU32::new(old_to_new[old_nbr]));
            }
            for _ in count..m {
                upper.push(AtomicU32::new(0));
            }
        }
    }
    debug_assert_eq!(upper.len(), upper_cells);

    let (old_entry, top) = links.entry_state();
    let new_entry = if old_entry == NO_NODE {
        NO_NODE
    } else {
        old_to_new[old_entry as usize]
    };
    let mut out = Store::from_parts(
        dim,
        m,
        store.ef_construction,
        store.seed,
        store.kernel,
        rows,
        inv_norm,
        level,
        upper_slot,
        upper_base,
        links0,
        upper,
        new_entry,
        top,
    );
    let map = if attach_node_map {
        // Compose with an existing `node_map` if `store` was already locally
        // mapped before this permutation.
        let composed = match store.node_map() {
            Some(prev) => new_to_old
                .iter()
                .map(|&mid| prev.node_to_row[mid as usize])
                .collect(),
            None => new_to_old.to_vec(),
        };
        Some(NodeMap::from_new_to_old(composed))
    } else {
        None
    };
    out.set_reorder_state(true, map);
    out
}

/// Restore `store.rows` and `store.inv_norm` from internal node order back to
/// external row order `0..n` before appending new unlinked tail rows.
pub(super) fn unpermute_rows_in_place(store: &mut Store) {
    let Some(map) = store.take_node_map() else {
        store.set_reorder_state(false, None);
        return;
    };
    let n = store.n();
    let dim = store.dim;
    let src_rows = store.rows.values();
    let mut ext_rows = vec![0.0f32; n * dim];
    let mut ext_inv = Vec::with_capacity(n);
    for ext_row in 0..n {
        let node = map.row_to_node[ext_row] as usize;
        ext_rows[ext_row * dim..(ext_row + 1) * dim]
            .copy_from_slice(&src_rows[node * dim..(node + 1) * dim]);
        ext_inv.push(store.inv_norm[node]);
    }
    store.rows = OwnedRows::from_values(dim, ext_rows);
    store.inv_norm = ext_inv;
    store.set_reorder_state(false, None);
}

/// Structural locality summary of a linked layer-0 HNSW graph (`#98`).
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct LocalityMetrics {
    /// Total directed layer-0 edges `(u -> v)`.
    pub edges: u64,
    /// Mean `|u - v|` across all directed layer-0 edges.
    pub mean_id_gap: f64,
    /// Median `|u - v|` across all directed layer-0 edges.
    pub median_id_gap: u32,
    /// Fraction of directed layer-0 edges whose endpoints start on the same
    /// 4 KB vector-row page (`(u * dim * 4) / 4096 == (v * dim * 4) / 4096`).
    pub same_4k_row_page_frac: f64,
    /// Fraction of directed layer-0 edges whose endpoints share the same
    /// 64-byte `inv_norm` cache line (`u / 16 == v / 16`).
    pub same_cacheline_inv_norm_frac: f64,
}

const OS_PAGE_BYTES: usize = 4_096;

/// Compute [`LocalityMetrics`] over `view`'s layer-0 adjacency.
pub(super) fn compute_locality_metrics<L: LinkCells + ?Sized>(
    view: &View<'_, L>,
) -> LocalityMetrics {
    let n = view.n();
    if n == 0 || !view.is_linked() {
        return LocalityMetrics::default();
    }
    let row_bytes = view.dim * std::mem::size_of::<f32>();
    let stride0 = view.m0 + 1;
    let mut gaps: Vec<u32> = Vec::with_capacity(n * view.m0);
    let mut sum_gap = 0u64;
    let mut same_page = 0u64;
    let mut same_line = 0u64;
    for u in 0..n {
        let start = u * stride0;
        let count = (LinkCells::get(view.links0, start) as usize).min(view.m0);
        let u_page = (u * row_bytes) / OS_PAGE_BYTES;
        let u_line = u / 16;
        for i in 0..count {
            let v = LinkCells::get(view.links0, start + 1 + i) as usize;
            let gap = u.abs_diff(v) as u32;
            gaps.push(gap);
            sum_gap += u64::from(gap);
            if (v * row_bytes) / OS_PAGE_BYTES == u_page {
                same_page += 1;
            }
            if v / 16 == u_line {
                same_line += 1;
            }
        }
    }
    if gaps.is_empty() {
        return LocalityMetrics::default();
    }
    gaps.sort_unstable();
    let edges = gaps.len() as u64;
    LocalityMetrics {
        edges,
        mean_id_gap: sum_gap as f64 / edges as f64,
        median_id_gap: gaps[gaps.len() / 2],
        same_4k_row_page_frac: same_page as f64 / edges as f64,
        same_cacheline_inv_norm_frac: same_line as f64 / edges as f64,
    }
}

/// Count distinct 4 KB vector-row pages touched during a layer-0 walk at `ef`.
pub(super) fn walk_row_4k_pages<L: LinkCells + ?Sized>(
    view: &View<'_, L>,
    query: &[f32],
    ef: usize,
) -> usize {
    use super::graph::{with_scratch, Filter, FloorPrune, Hook, Patience};

    if view.n() == 0 || !view.is_linked() {
        return 0;
    }
    let row_bytes = view.dim * std::mem::size_of::<f32>();
    let total_pages = (view.n() * row_bytes).div_ceil(OS_PAGE_BYTES);
    struct PageHook {
        row_bytes: usize,
        touched: Vec<u64>,
    }
    impl Hook for PageHook {
        #[inline]
        fn scored(&mut self, node: u32, _score: f32, _row: &[f32]) {
            let start_byte = node as usize * self.row_bytes;
            let end_byte = start_byte + self.row_bytes.saturating_sub(1);
            for p in (start_byte / OS_PAGE_BYTES)..=(end_byte / OS_PAGE_BYTES) {
                self.touched[p / 64] |= 1u64 << (p % 64);
            }
        }
        #[inline]
        fn hop(&mut self) -> bool {
            true
        }
    }
    let mut hook = PageHook {
        row_bytes,
        touched: vec![0u64; total_pages.div_ceil(64)],
    };
    with_scratch(|scratch| {
        let _ = view.walk(
            query,
            ef.max(1),
            Filter::None,
            Patience::OFF,
            FloorPrune::OFF,
            scratch,
            &mut hook,
        );
    });
    hook.touched.iter().map(|w| w.count_ones() as usize).sum()
}

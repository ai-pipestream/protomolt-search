//! Recursive balanced bisection of the symmetrized layer-0 graph down to
//! page-sized leaves, refined at each cut by bucket-sorted swap passes.

use std::cell::RefCell;

use super::csr::UndirectedCsr;
use super::rcm::rcm_from_csr;

/// Maximum balanced swap-refinement passes per recursive bisection cut.
const MAX_BISECT_PASSES: usize = 10;
/// Minimum subgraph vertex count to fork left/right halves across Rayon.
const PARALLEL_BISECT_MIN_NODES: usize = 2048;
/// Clamped gain magnitude for `O(V)` counting-bucket sort (`[-GAIN_CAP, +GAIN_CAP]`).
const GAIN_CAP: i32 = 128;
const GAIN_BUCKETS: usize = (2 * GAIN_CAP + 1) as usize;
/// Consecutive non-positive candidate pairs before ending a swap pass early.
const MAX_STALE_PAIRS: usize = 48;
/// Largest overlap count the seeding queue tracks per vertex.
const MAX_OVERLAP_BUCKET: usize = 64;

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

pub(super) fn recursive_bisection_from_csr(csr: &UndirectedCsr, leaf_cap: usize) -> Vec<u32> {
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

/// A peripheral seed for the overlap growth: the lowest-degree connected
/// vertex, moved one breadth-first sweep away from any central hub.
fn peripheral_seed(local: &LocalCsr) -> usize {
    let k = local.k();
    let mut seed = 0usize;
    let mut best_deg = usize::MAX;
    for i in 0..k {
        let d = local.degree(i);
        if d > 0 && d < best_deg {
            best_deg = d;
            seed = i;
        }
    }
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
    seed
}

/// The bucketed max-overlap queue that grows `side = 0`: every unassigned
/// vertex sits in the bucket of its (clamped) count of assigned neighbors.
struct OverlapGrowth<'a> {
    local: &'a LocalCsr,
    side: Vec<u8>,
    overlap: Vec<u16>,
    buckets: Vec<Vec<u32>>,
    heads: [usize; MAX_OVERLAP_BUCKET + 1],
    top_b: usize,
}

impl<'a> OverlapGrowth<'a> {
    fn new(local: &'a LocalCsr) -> Self {
        let k = local.k();
        Self {
            local,
            side: vec![1u8; k],
            overlap: vec![0u16; k],
            buckets: (0..=MAX_OVERLAP_BUCKET).map(|_| Vec::new()).collect(),
            heads: [0usize; MAX_OVERLAP_BUCKET + 1],
            top_b: 0,
        }
    }

    /// Assign `u` to side 0 and raise each unassigned neighbor's overlap.
    fn place(&mut self, u: usize) {
        self.side[u] = 0;
        for &v in self.local.neighbors(u) {
            let vi = v as usize;
            if self.side[vi] == 0 {
                continue;
            }
            let next_c = (usize::from(self.overlap[vi]) + 1).min(MAX_OVERLAP_BUCKET);
            self.overlap[vi] = next_c as u16;
            self.buckets[next_c].push(v);
            if next_c > self.top_b {
                self.top_b = next_c;
            }
        }
    }

    /// The next unassigned vertex from the highest non-empty bucket, if
    /// any vertex still overlaps the assigned side.
    fn pop_best(&mut self) -> Option<usize> {
        let mut chosen = None;
        while self.top_b > 0 {
            let b_vec = &self.buckets[self.top_b];
            let mut h = self.heads[self.top_b];
            while h < b_vec.len() {
                let cand = b_vec[h] as usize;
                h += 1;
                if self.side[cand] != 0
                    && (usize::from(self.overlap[cand]).min(MAX_OVERLAP_BUCKET) == self.top_b)
                {
                    chosen = Some(cand);
                    break;
                }
            }
            self.heads[self.top_b] = h;
            if chosen.is_some() {
                break;
            }
            self.top_b -= 1;
        }
        chosen
    }
}

/// Seed a balanced `(half, k - half)` split by growing `side = 0` via an
/// `O(V + E)` bucketed max-overlap queue from a peripheral vertex so dense
/// intra-cluster neighborhoods fill completely before crossing sparse
/// small-world HNSW shortcut edges.
fn initial_overlap_bisection(local: &LocalCsr, half: usize) -> Vec<u8> {
    let k = local.k();
    if half == 0 || half >= k {
        return vec![1u8; k];
    }
    let mut growth = OverlapGrowth::new(local);
    let mut assigned = 0usize;
    let mut fallback_scan = 0usize;

    growth.place(peripheral_seed(local));
    assigned += 1;

    while assigned < half {
        let u = match growth.pop_best() {
            Some(c) => c,
            None => {
                while fallback_scan < k && growth.side[fallback_scan] == 0 {
                    fallback_scan += 1;
                }
                fallback_scan
            }
        };
        growth.place(u);
        assigned += 1;
    }
    growth.side
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

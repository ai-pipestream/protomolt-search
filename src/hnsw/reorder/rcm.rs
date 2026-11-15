//! Reverse Cuthill–McKee ordering over the symmetrized layer-0 graph.

use std::collections::VecDeque;

use super::csr::UndirectedCsr;

/// Epoch-stamped breadth-first marks reused across pseudo-peripheral sweeps.
struct SweepScratch {
    stamp: Vec<u32>,
    epoch: u32,
    queue: VecDeque<u32>,
}

impl SweepScratch {
    fn new(n: usize) -> Self {
        Self {
            stamp: vec![0u32; n],
            epoch: 0,
            queue: VecDeque::new(),
        }
    }

    fn next_epoch(&mut self) -> u32 {
        self.epoch = self.epoch.wrapping_add(1);
        if self.epoch == 0 {
            self.stamp.fill(0);
            self.epoch = 1;
        }
        self.epoch
    }
}

/// A copy of the edge array with each vertex's neighbors sorted by
/// ascending `(degree, vertex_id)` for Cuthill-McKee expansion.
fn degree_sorted_edges(csr: &UndirectedCsr) -> Vec<u32> {
    let mut cm_edges = csr.edges.clone();
    for u in 0..csr.n() {
        let lo = csr.offsets[u] as usize;
        let hi = csr.offsets[u + 1] as usize;
        cm_edges[lo..hi].sort_unstable_by_key(|&v| (csr.degree(v), v));
    }
    cm_edges
}

/// 2-sweep pseudo-peripheral refinement inside `candidate`'s unplaced
/// component: the lowest `(degree, vertex_id)` vertex of the last
/// breadth-first level, taken twice.
fn pseudo_peripheral_start(
    csr: &UndirectedCsr,
    placed: &[bool],
    candidate: u32,
    scratch: &mut SweepScratch,
) -> u32 {
    let mut start = candidate;
    for _ in 0..2 {
        let epoch = scratch.next_epoch();
        scratch.queue.clear();
        scratch.stamp[start as usize] = epoch;
        scratch.queue.push_back(start);
        let mut last_level_best = (u32::MAX, start);
        let mut level_rem = 1usize;
        while let Some(u) = scratch.queue.pop_front() {
            let d = csr.degree(u);
            if (d, u) < last_level_best {
                last_level_best = (d, u);
            }
            for &v in csr.neighbors(u) {
                if !placed[v as usize] && scratch.stamp[v as usize] != epoch {
                    scratch.stamp[v as usize] = epoch;
                    scratch.queue.push_back(v);
                }
            }
            level_rem -= 1;
            if level_rem == 0 && !scratch.queue.is_empty() {
                level_rem = scratch.queue.len();
                last_level_best = (u32::MAX, *scratch.queue.front().expect("non-empty"));
            }
        }
        if last_level_best.1 == start {
            break;
        }
        start = last_level_best.1;
    }
    start
}

/// Standard Cuthill-McKee breadth-first expansion from `start`, appending
/// its component to `order`.
fn expand_component(
    csr: &UndirectedCsr,
    cm_edges: &[u32],
    start: u32,
    placed: &mut [bool],
    order: &mut Vec<u32>,
) {
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

/// Symmetrized `O(V + E)` Reverse Cuthill–McKee ordering seeded from 2-sweep
/// pseudo-peripheral vertices per connected component.
pub(super) fn rcm_from_csr(csr: &UndirectedCsr) -> Vec<u32> {
    let n = csr.n();
    if n == 0 {
        return Vec::new();
    }
    let cm_edges = degree_sorted_edges(csr);
    let mut by_deg: Vec<u32> = (0..n as u32).collect();
    by_deg.sort_unstable_by_key(|&u| (csr.degree(u), u));

    let mut placed = vec![false; n];
    let mut scratch = SweepScratch::new(n);
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
        let start = pseudo_peripheral_start(csr, &placed, candidate, &mut scratch);
        expand_component(csr, &cm_edges, start, &mut placed, &mut order);
    }
    debug_assert_eq!(order.len(), n);
    order.reverse();
    order
}

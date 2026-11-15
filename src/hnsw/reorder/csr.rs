//! The symmetrized layer-0 graph every ordering pass reads.

use super::super::graph::{LinkCells, View, NO_NODE};

/// Symmetrized, deduplicated undirected layer-0 CSR graph.
pub(super) struct UndirectedCsr {
    pub(super) offsets: Vec<u32>,
    pub(super) edges: Vec<u32>,
}

impl UndirectedCsr {
    pub(super) fn from_view<L: LinkCells + ?Sized>(view: &View<'_, L>) -> Self {
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
    pub(super) fn n(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    #[inline]
    pub(super) fn degree(&self, u: u32) -> u32 {
        let idx = u as usize;
        self.offsets[idx + 1] - self.offsets[idx]
    }

    #[inline]
    pub(super) fn neighbors(&self, u: u32) -> &[u32] {
        let idx = u as usize;
        &self.edges[self.offsets[idx] as usize..self.offsets[idx + 1] as usize]
    }
}

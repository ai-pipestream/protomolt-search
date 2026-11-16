//! A read-only view of one graph and the striped locks a parallel build
//! holds while it reads or writes a node's links.

use std::sync::{Mutex, MutexGuard};

use super::super::distance::{kernel_row_slack, Kernel};
use super::{LinkCells, Scored, SearchStats, LOCK_STRIPES, NO_NODE, SCORE_CHUNK_NODES};

/// Striped locks over node ids. Insertion holds at most one node lock at
/// a time (and takes the entry lock only while holding none), so striping
/// cannot deadlock.
pub struct Locks(Vec<Mutex<()>>);

impl Default for Locks {
    fn default() -> Self {
        Self((0..LOCK_STRIPES).map(|_| Mutex::new(())).collect())
    }
}

impl Locks {
    pub(in crate::hnsw) fn lock(&self, node: u32) -> MutexGuard<'_, ()> {
        let guard = self.0[node as usize % LOCK_STRIPES].lock();
        guard.unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// A read-only view of one graph, generic over where the link cells live.
pub struct View<'a, L: LinkCells + ?Sized> {
    pub dim: usize,
    pub m: usize,
    pub m0: usize,
    pub rows: &'a [f32],
    pub inv_norm: &'a [f32],
    pub level: &'a [u8],
    pub upper_slot: &'a [u32],
    pub upper_base: &'a [u32],
    pub links0: &'a L,
    pub upper: &'a L,
    pub entry: u32,
    pub top: u8,
    pub kernel: Kernel,
}

impl<'a, L: LinkCells + ?Sized> View<'a, L> {
    #[inline]
    pub fn n(&self) -> usize {
        self.inv_norm.len()
    }

    #[inline]
    pub fn is_linked(&self) -> bool {
        self.entry != NO_NODE
    }

    #[inline]
    pub fn row(&self, node: u32) -> &'a [f32] {
        let at = node as usize * self.dim;
        &self.rows[at..at + self.dim]
    }

    #[inline]
    pub(in crate::hnsw) fn cap(&self, layer: usize) -> usize {
        if layer == 0 {
            self.m0
        } else {
            self.m
        }
    }

    #[inline]
    fn cells(&self, layer: usize) -> &'a L {
        if layer == 0 {
            self.links0
        } else {
            self.upper
        }
    }

    /// Where `node`'s list at `layer` starts (its count cell).
    #[inline]
    pub(in crate::hnsw) fn list_start(&self, node: u32, layer: usize) -> usize {
        if layer == 0 {
            node as usize * (self.m0 + 1)
        } else {
            let u = self.upper_slot[node as usize] as usize;
            self.upper_base[u] as usize + (layer - 1) * (self.m + 1)
        }
    }

    /// Copy `node`'s neighbors at `layer` into `out`, under the node's lock
    /// when a build is running.
    #[inline]
    pub(in crate::hnsw) fn copy_links(
        &self,
        node: u32,
        layer: usize,
        locks: Option<&Locks>,
        out: &mut Vec<u32>,
    ) {
        let start = self.list_start(node, layer);
        let cells = self.cells(layer);
        let _guard = locks.map(|l| l.lock(node));
        let count = (cells.get(start) as usize).min(self.cap(layer));
        out.clear();
        out.extend((0..count).map(|i| cells.get(start + 1 + i)));
    }

    /// Cache lines of one row to prefetch ahead of scoring (`16` floats per
    /// 64-byte line, bounded by [`Kernel::prefetch_lines`]).
    #[inline]
    pub(super) fn prefetch_lines(&self) -> usize {
        self.kernel.prefetch_lines(self.dim)
    }

    /// Score every node in `nodes` against `q` inside a single target-feature
    /// call per [`SCORE_CHUNK_NODES`] slice, and deliver `(node, score)` in
    /// `nodes` order.
    #[inline]
    pub(in crate::hnsw) fn for_each_scored(
        &self,
        q: &[f32],
        nodes: &[u32],
        mut f: impl FnMut(u32, f32),
    ) {
        let lines = self.prefetch_lines();
        let mut scores = [0.0f32; SCORE_CHUNK_NODES];
        for chunk in nodes.chunks(SCORE_CHUNK_NODES) {
            self.kernel.score_nodes(
                q,
                self.rows,
                self.dim,
                chunk,
                lines,
                &mut scores[..chunk.len()],
            );
            for (&node, &score) in chunk.iter().zip(&scores[..chunk.len()]) {
                f(node, score);
            }
        }
    }

    /// Greedy descent at one layer: move to the best neighbor until none
    /// improves.
    pub(super) fn greedy(
        &self,
        q: &[f32],
        mut best: Scored,
        layer: usize,
        locks: Option<&Locks>,
        nbrs: &mut Vec<u32>,
        stats: &mut SearchStats,
    ) -> Scored {
        loop {
            self.copy_links(best.node, layer, locks, nbrs);
            stats.hops += 1;
            stats.scored += nbrs.len() as u64;
            let mut improved = false;
            self.for_each_scored(q, nbrs, |v, s| {
                if s > best.score {
                    best = Scored { score: s, node: v };
                    improved = true;
                }
            });
            if !improved {
                return best;
            }
        }
    }

    /// True when `node`'s exact scalar score is provably strictly below `floor`
    /// under Higham proportional error plus subnormal gradual-underflow slack.
    #[inline]
    pub(super) fn below_floor(
        &self,
        node: u32,
        kernel_score: f32,
        floor: f32,
        slack_per_norm: f64,
    ) -> bool {
        if !floor.is_finite() {
            return false;
        }
        let inv = self.inv_norm[node as usize];
        if !(inv > 0.0 && inv.is_finite()) {
            return false;
        }
        f64::from(kernel_score) + kernel_row_slack(self.dim, slack_per_norm, inv) < f64::from(floor)
    }
}

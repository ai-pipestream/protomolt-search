//! Exact SIMD candidate pre-filtering for unlinked stores and filtered enumeration.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::ops::ControlFlow;

use crate::vector::Mask;

use super::super::distance::{kernel_row_slack, prefetch};
use super::{LinkCells, Scored, View, SCORE_CHUNK_NODES};

/// Parameters for [`View::enumerate_skipping`].
pub(in crate::hnsw) struct EnumerateQuery<'a> {
    pub(in crate::hnsw) q: &'a [f32],
    pub(in crate::hnsw) allow: Mask<'a>,
    pub(in crate::hnsw) k: usize,
    pub(in crate::hnsw) floor: f32,
    pub(in crate::hnsw) slack_per_norm: f64,
    pub(in crate::hnsw) already_scored: &'a [Scored],
}

struct EnumerateWindow {
    dim: usize,
    k: usize,
    finite_floor: Option<f64>,
    slack_per_norm: f64,
    top: BinaryHeap<Reverse<Scored>>,
    shortest_inv: f32,
}

impl EnumerateWindow {
    fn new(dim: usize, k: usize, floor: f32, slack_per_norm: f64) -> Self {
        Self {
            dim,
            k,
            finite_floor: floor.is_finite().then(|| f64::from(floor)),
            slack_per_norm,
            top: BinaryHeap::with_capacity(k.saturating_add(1)),
            shortest_inv: f32::INFINITY,
        }
    }

    #[inline]
    fn consider(&mut self, scored: Scored, inv: f32) {
        if self.k == 0 || !(inv > 0.0 && inv.is_finite()) {
            return;
        }
        self.shortest_inv = self.shortest_inv.min(inv);
        if self.top.len() < self.k {
            self.top.push(Reverse(scored));
        } else if self.top.peek().is_some_and(|worst| scored > worst.0) {
            self.top.pop();
            self.top.push(Reverse(scored));
        }
    }

    #[inline]
    fn current_bar(&self) -> Option<f64> {
        let top_bar = if self.k > 0 && self.top.len() == self.k {
            self.top.peek().map(|Reverse(kth)| {
                f64::from(kth.score)
                    - kernel_row_slack(self.dim, self.slack_per_norm, self.shortest_inv)
            })
        } else {
            None
        };
        match (self.finite_floor, top_bar) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        }
    }

    #[inline]
    fn survives(&self, s: Scored, inv: f32, bar: Option<f64>) -> bool {
        if self.k == 0 {
            return false;
        }
        let Some(bar) = bar else {
            return true;
        };
        if !(inv > 0.0 && inv.is_finite()) {
            return true;
        }
        f64::from(s.score) + kernel_row_slack(self.dim, self.slack_per_norm, inv) >= bar
    }
}

impl<'a, L: LinkCells + ?Sized> View<'a, L> {
    /// Score every row `allow` names with the traversal kernel, and leave in
    /// `out` only those that can make the exact top `k` for the caller to
    /// settle with the scalar scorer. Returns the rows scored.
    pub fn enumerate(
        &self,
        q: &[f32],
        allow: Mask<'_>,
        k: usize,
        slack_per_norm: f64,
        out: &mut Vec<Scored>,
    ) -> u64 {
        self.enumerate_skipping(
            EnumerateQuery {
                q,
                allow,
                k,
                floor: f32::NEG_INFINITY,
                slack_per_norm,
                already_scored: &[],
            },
            |_| false,
            out,
        )
    }

    /// [`View::enumerate`] seeded with `already_scored` allowed rows from an
    /// aborted or budget-capped walk and an optional exact score `floor`,
    /// scoring only allowed rows where `!skip(row)` in stack-sized SIMD chunks.
    pub(in crate::hnsw) fn enumerate_skipping(
        &self,
        query: EnumerateQuery<'_>,
        skip: impl Fn(u32) -> bool,
        out: &mut Vec<Scored>,
    ) -> u64 {
        out.clear();
        let mut win = EnumerateWindow::new(self.dim, query.k, query.floor, query.slack_per_norm);
        for &prior in query.already_scored {
            win.consider(prior, self.inv_norm[prior.node as usize]);
        }

        let mut cur = [0u32; SCORE_CHUNK_NODES];
        let mut cur_len = 0usize;
        let mut next = [0u32; SCORE_CHUNK_NODES];
        let mut next_len = 0usize;
        let mut scored = 0u64;

        let _ = query.allow.try_for_each(|row| {
            let node = row as u32;
            if skip(node) {
                return ControlFlow::<()>::Continue(());
            }
            scored += 1;
            if cur_len < SCORE_CHUNK_NODES {
                cur[cur_len] = node;
                cur_len += 1;
            } else {
                next[next_len] = node;
                next_len += 1;
                if next_len == SCORE_CHUNK_NODES {
                    self.score_enumerate_chunk(
                        query.q,
                        &cur[..cur_len],
                        &next[..next_len],
                        &mut win,
                        out,
                    );
                    cur = next;
                    cur_len = next_len;
                    next_len = 0;
                }
            }
            ControlFlow::Continue(())
        });
        if cur_len > 0 {
            self.score_enumerate_chunk(query.q, &cur[..cur_len], &next[..next_len], &mut win, out);
        }
        if next_len > 0 {
            self.score_enumerate_chunk(query.q, &next[..next_len], &[], &mut win, out);
        }

        let final_bar = win.current_bar();
        out.retain(|&s| win.survives(s, self.inv_norm[s.node as usize], final_bar));
        for &prior in query.already_scored {
            if win.survives(prior, self.inv_norm[prior.node as usize], final_bar) {
                out.push(prior);
            }
        }
        scored
    }

    fn score_enumerate_chunk(
        &self,
        q: &[f32],
        chunk: &[u32],
        ahead: &[u32],
        win: &mut EnumerateWindow,
        out: &mut Vec<Scored>,
    ) {
        let lines = self.prefetch_lines();
        let batch = self.kernel.batch_width();
        for &node in &ahead[..ahead.len().min(batch)] {
            prefetch(self.row(node), lines);
        }
        let mut scores = [0.0f32; SCORE_CHUNK_NODES];
        self.kernel.score_nodes(
            q,
            self.rows,
            self.dim,
            chunk,
            lines,
            &mut scores[..chunk.len()],
        );
        for (&node, &score) in chunk.iter().zip(&scores[..chunk.len()]) {
            let s = Scored { score, node };
            let inv = self.inv_norm[node as usize];
            win.consider(s, inv);
            if win.survives(s, inv, win.current_bar()) {
                out.push(s);
            }
        }
    }
}

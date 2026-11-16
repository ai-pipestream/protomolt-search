//! Per-thread working memory of a walk or an insertion.

use std::cell::RefCell;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

use super::Scored;

/// Per-thread working memory: the epoch-stamped visited set (reset in
/// O(1) by bumping the epoch), the candidate and result heaps, and the
/// buffers insertion and the heuristic reuse.
#[derive(Default)]
pub struct Scratch {
    visited: Vec<u32>,
    epoch: u32,
    pub(in crate::hnsw) cand: BinaryHeap<Scored>,
    pub(in crate::hnsw) res: BinaryHeap<Reverse<Scored>>,
    pub(in crate::hnsw) nbrs: Vec<u32>,
    pub(in crate::hnsw) pool: Vec<Scored>,
    pub(in crate::hnsw) sel: Vec<u32>,
    /// Filter-aware walk: the disallowed neighbors of the node being
    /// expanded, the allowed rows about to be scored, and every allowed row
    /// scored at layer 0 (so a handoff to enumerate never scores them twice).
    pub(in crate::hnsw) routes: Vec<u32>,
    pub(in crate::hnsw) fresh: Vec<u32>,
    pub(super) scored_allowed: Vec<Scored>,
    /// The last walk's far probe routed it plain (`Route::plain_share`).
    pub(super) walked_plain: bool,
    /// Patience early-stop scratch: nodes inserted into the tracked window
    /// during the current hop, and the optional `top_k` (`< ef`) min-heap.
    pub(super) patience_added: Vec<u32>,
    pub(super) patience_top: BinaryHeap<Reverse<Scored>>,
}

impl Scratch {
    pub(in crate::hnsw) fn begin(&mut self, n: usize) {
        if self.visited.len() < n {
            self.visited.resize(n, 0);
        }
        self.epoch = self.epoch.wrapping_add(1);
        if self.epoch == 0 {
            self.visited.fill(0);
            self.epoch = 1;
        }
        self.cand.clear();
        self.res.clear();
        self.scored_allowed.clear();
        self.patience_added.clear();
        self.patience_top.clear();
    }

    /// Mark `node` visited; true when it was not.
    #[inline]
    pub(in crate::hnsw) fn visit(&mut self, node: u32) -> bool {
        let slot = &mut self.visited[node as usize];
        if *slot == self.epoch {
            false
        } else {
            *slot = self.epoch;
            true
        }
    }

    /// Whether the last walk's far probe routed it plain.
    #[inline]
    pub fn walked_plain(&self) -> bool {
        self.walked_plain
    }

    /// Whether the last walk reached `node`. An allowed row a filter-aware
    /// walk reached was scored and handed to its hook.
    pub fn seen(&self, node: u32) -> bool {
        self.visited[node as usize] == self.epoch
    }

    /// Allowed rows scored during the last filter-aware walk, with their
    /// traversal-kernel scores.
    #[inline]
    pub fn scored_allowed(&self) -> &[Scored] {
        &self.scored_allowed
    }

    /// The results, best first.
    pub(in crate::hnsw) fn drain_results(&mut self) -> Vec<Scored> {
        let mut out: Vec<Scored> = self.res.drain().map(|r| r.0).collect();
        out.sort_unstable_by(|a, b| b.cmp(a));
        out
    }
}

thread_local! {
    static SCRATCH: RefCell<Option<Box<Scratch>>> = const { RefCell::new(None) };
}

/// Run `f` with this thread's scratch. The scratch is taken out for the
/// duration, so a callback that searches again (a streaming sink, say)
/// gets a fresh one instead of a borrow panic.
pub fn with_scratch<R>(f: impl FnOnce(&mut Scratch) -> R) -> R {
    let mut scratch = SCRATCH
        .with(|cell| cell.borrow_mut().take())
        .unwrap_or_default();
    let out = f(&mut scratch);
    SCRATCH.with(|cell| {
        let mut slot = cell.borrow_mut();
        if slot.is_none() {
            *slot = Some(scratch);
        }
    });
    out
}

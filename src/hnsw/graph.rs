//! The layered graph: flat storage, insertion, and the walk.
//!
//! Layout (one contiguous array per kind, no `Vec<Vec>`):
//!
//! - `rows`: FP32 rows, `dim` values each, in node order.
//! - `inv_norm`: one `1/|row|` per node (0 for a zero row), for the
//!   direction-based neighbor heuristic.
//! - `level`: each node's top layer.
//! - `links0`: layer 0 for every node, `2M + 1` cells per node: the count,
//!   then the neighbors.
//! - `upper_slot`, `upper_base`, `upper`: the upper layers of the nodes
//!   that have them. `upper_slot[node]` is the node's compact index `u`
//!   (`NO_NODE` for a level-0 node); the node's layers `1..=level` are
//!   `level` consecutive blocks of `M + 1` cells starting at
//!   `upper[upper_base[u]]`.
//!
//! The heap graph keeps its link cells as `AtomicU32` so a parallel build
//! can write them from several threads under striped per-node locks with no
//! `unsafe`; a mapped image serves plain `u32` words. Both implement
//! [`LinkCells`] and the walk is generic over it, so a search over a loaded
//! and a mapped image runs the same code and produces the same scores.
//!
//! Algorithms follow Malkov & Yashunin, "Efficient and robust approximate
//! nearest neighbor search using Hierarchical Navigable Small World graphs"
//! (arXiv:1603.09320): insertion (Alg. 1), the layer search (Alg. 2), the
//! neighbor heuristic (Alg. 4, without `extendCandidates` and without
//! `keepPrunedConnections`), and the query (Alg. 5). The heuristic
//! compares directions rather than inner products: see `select`.

use std::cell::RefCell;
use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::ops::ControlFlow;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering as Atomic};
use std::sync::{Mutex, MutexGuard};

use crate::vector::{Mask, VectorError};

use super::distance::{prefetch, Kernel};
use super::rows::{OwnedRows, RowSource};
use super::HNSW;

/// The absent node: an empty graph's entry, a level-0 node's upper slot.
pub const NO_NODE: u32 = u32::MAX;
/// Levels are capped so a hostile image cannot claim an absurd skeleton;
/// the geometric draw reaches 13 at `M = 16` with probability 2^-53.
pub const MAX_LEVEL: u8 = 31;
/// Rows beyond this cannot be addressed by a `u32` node id with the sentinel.
pub const MAX_ROWS: usize = (u32::MAX - 1) as usize;
/// Stack chunk size for [`View::for_each_scored`] and [`View::enumerate`]
/// (`64` covers a full layer-0 neighbor list at `M <= 32` in one call).
const SCORE_CHUNK_NODES: usize = 64;
/// Striped per-node locks for the parallel build.
const LOCK_STRIPES: usize = 1 << 14;

/// Link cells the walk reads.
pub trait LinkCells: Sync {
    fn get(&self, i: usize) -> u32;
    fn cell_count(&self) -> usize;
}

impl LinkCells for [AtomicU32] {
    #[inline]
    fn get(&self, i: usize) -> u32 {
        self[i].load(Atomic::Relaxed)
    }
    fn cell_count(&self) -> usize {
        <[AtomicU32]>::len(self)
    }
}

impl LinkCells for [u32] {
    #[inline]
    fn get(&self, i: usize) -> u32 {
        self[i]
    }
    fn cell_count(&self) -> usize {
        <[u32]>::len(self)
    }
}

/// A scored node. The order is total (score, then the smaller node first
/// among ties), so heaps behave the same on every run.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Scored {
    pub score: f32,
    pub node: u32,
}

impl Eq for Scored {}

impl Ord for Scored {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| other.node.cmp(&self.node))
    }
}

impl PartialOrd for Scored {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Work counters of one walk.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SearchStats {
    /// Nodes expanded (neighbor lists read).
    pub hops: u64,
    /// Rows scored by the traversal kernel.
    pub scored: u64,
    /// Disallowed rows a filter-aware walk looked through without scoring.
    pub bridges: u64,
    /// Rows the far probe looked at (links and mask only, never scored).
    pub probed: u64,
}

/// Per-thread working memory: the epoch-stamped visited set (reset in
/// O(1) by bumping the epoch), the candidate and result heaps, and the
/// buffers insertion and the heuristic reuse.
#[derive(Default)]
pub struct Scratch {
    visited: Vec<u32>,
    epoch: u32,
    cand: BinaryHeap<Scored>,
    res: BinaryHeap<Reverse<Scored>>,
    nbrs: Vec<u32>,
    pool: Vec<Scored>,
    sel: Vec<u32>,
    /// Filter-aware walk: the disallowed neighbors of the node being
    /// expanded, the allowed rows about to be scored, and every allowed row
    /// scored at layer 0 (so a handoff to enumerate never scores them twice).
    routes: Vec<u32>,
    fresh: Vec<u32>,
    scored_allowed: Vec<Scored>,
    /// Patience early-stop scratch: nodes inserted into the tracked window
    /// during the current hop, and the optional `top_k` (`< ef`) min-heap.
    patience_added: Vec<u32>,
    patience_top: BinaryHeap<Reverse<Scored>>,
}

impl Scratch {
    fn begin(&mut self, n: usize) {
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
    fn visit(&mut self, node: u32) -> bool {
        let slot = &mut self.visited[node as usize];
        if *slot == self.epoch {
            false
        } else {
            *slot = self.epoch;
            true
        }
    }

    /// Whether the last walk reached `node`. An allowed row a filter-aware
    /// walk reached was scored and handed to its hook.
    #[inline]
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
    fn drain_results(&mut self) -> Vec<Scored> {
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

/// What a walk reports as it goes: every exactly scored allowed node at
/// layer 0 and each expansion. A plain search uses [`NoHook`].
pub trait Hook {
    /// An allowed node was scored at layer 0: its id, the traversal
    /// kernel's score, and its row (for the scalar rescoring of anything
    /// that leaves the provider).
    fn scored(&mut self, node: u32, score: f32, row: &[f32]);
    /// After each expansion; `false` stops the walk.
    fn hop(&mut self) -> bool;
    /// Current exact score floor for layer-0 floor pruning (`#72`).
    #[inline]
    fn current_floor(&self) -> f32 {
        f32::NEG_INFINITY
    }
}

pub struct NoHook;

impl Hook for NoHook {
    #[inline]
    fn scored(&mut self, _node: u32, _score: f32, _row: &[f32]) {}
    #[inline]
    fn hop(&mut self) -> bool {
        true
    }
}

/// Unary walk hook carrying a static score floor (`#72`).
pub struct FloorHook(pub f32);

impl Hook for FloorHook {
    #[inline]
    fn scored(&mut self, _node: u32, _score: f32, _row: &[f32]) {}
    #[inline]
    fn hop(&mut self) -> bool {
        true
    }
    #[inline]
    fn current_floor(&self) -> f32 {
        self.0
    }
}

/// How a mask steers the layer-0 walk (`docs/hnsw.md`, "Filters").
#[derive(Clone, Copy)]
pub enum Filter<'a> {
    /// Every row may be returned.
    None,
    /// The mask gates results only: disallowed rows are scored and routed
    /// through like any other.
    Plain(Mask<'a>),
    /// The walk moves through the allowed rows: a disallowed neighbor is
    /// never scored, its own neighbors are looked through instead.
    /// `allowed` is the mask's count. With `adaptive` the walk gives the
    /// query up to the caller ([`Walk::Far`], [`Walk::Short`],
    /// [`Walk::Budget`]) where the graph cannot serve this mask or `budget`
    /// work units have been spent; without it the walk runs regardless
    /// (a pinned regime).
    Aware {
        allow: Mask<'a>,
        allowed: usize,
        adaptive: bool,
        budget: Option<u64>,
    },
}

/// Hop-to-hop saturation early-stop policy for a layer-0 walk (`#95`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Patience {
    /// Required fraction of the tracked window that survives an expansion.
    pub saturation: f32,
    /// Optional exact parts-per-million saturation threshold (`1..=1_000_000`).
    pub saturation_ppm: Option<u32>,
    /// Consecutive saturated expansions required before ending the walk
    /// (`0` disables patience).
    pub hops: u32,
    /// Optional `top_k` window size (`0` tracks the full `ef` beam).
    pub top_k: usize,
    /// Minimum allowed rows scored at layer 0 before patience may count when
    /// `floor_active` is true (`#72` ascent gate).
    pub ascent_gate: usize,
    /// True when a finite score floor is active on this search.
    pub floor_active: bool,
}

impl Default for Patience {
    fn default() -> Self {
        Self::OFF
    }
}

impl Patience {
    pub const OFF: Self = Self {
        saturation: 1.0,
        saturation_ppm: None,
        hops: 0,
        top_k: 0,
        ascent_gate: 0,
        floor_active: false,
    };

    #[inline]
    pub fn enabled(self) -> bool {
        self.hops > 0
    }

    #[inline]
    fn min_survived(self, window: usize) -> usize {
        if !self.enabled() || window == 0 {
            return usize::MAX;
        }
        if let Some(ppm) = self.saturation_ppm {
            let need = (u64::from(ppm) * window as u64).div_ceil(1_000_000);
            return usize::try_from(need).unwrap_or(window).clamp(1, window);
        }
        // Subtract one f32 ULP so binary-rounded thresholds such as `0.99f32`
        // (`0.9900000095...`) do not round `0.99 * 100` up to `100`.
        let raw = (f64::from(self.saturation) - f64::from(f32::EPSILON)) * (window as f64);
        raw.ceil().clamp(1.0, window as f64) as usize
    }
}

/// Layer-0 shared floor pruning gate and Higham kernel-vs-scalar error slack (`#72`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FloorPrune {
    pub enabled: bool,
    /// Minimum allowed rows scored at layer 0 before candidates below the
    /// live floor may be pruned.
    pub gate: usize,
    /// `4 * gamma(d) * |query|`: per-unit-norm kernel-vs-scalar dot-product
    /// error slack.
    pub slack_per_norm: f64,
}

impl Default for FloorPrune {
    fn default() -> Self {
        Self::OFF
    }
}

impl FloorPrune {
    pub const OFF: Self = Self {
        enabled: false,
        gate: 0,
        slack_per_norm: 0.0,
    };
}

/// How a walk ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Walk {
    Completed,
    /// The hook stopped it.
    Stopped,
    /// Ended early after `Patience::hops` consecutive saturated expansions
    /// once the beam (and ascent gate when a floor is active) filled.
    Patience,
    /// Pruned at least one candidate or expansion against an active score floor
    /// once the `#72` ascent gate was satisfied; never triggers a starvation
    /// fallback to `enumerate`.
    FloorPruned,
    /// Not started: the far probe found the query's neighborhood all but
    /// empty of allowed rows. Nothing was scored at layer 0 or reported.
    Far,
    /// Ran dry with the beam unfilled although the mask allows more rows:
    /// the allowed rows it reached are an island. `Scratch::seen` tells
    /// which rows it scored.
    Short,
    /// Spent its layer-0 work-unit budget (`layer0_scored + bridges`) before
    /// the beam converged: enumerating the remaining allowed rows is cheaper
    /// than continuing the walk. `Scratch::seen` and `Scratch::scored_allowed`
    /// name the allowed rows already scored.
    Budget,
}

#[inline]
fn walk_work_units(stats: &SearchStats, base_scored: u64) -> u64 {
    stats.scored.saturating_sub(base_scored) + stats.bridges
}

/// The far probe trips when the mask's share of the rows predicts at least
/// this many allowed rows in the sample, and fewer than one in
/// `FAR_PROBE_RATIO` of them are there. Below the minimum the sample says
/// nothing (a Poisson count of mean 8 is under 2 three times in a
/// thousand).
const FAR_PROBE_MIN_EXPECTED: f64 = 8.0;
const FAR_PROBE_RATIO: f64 = 4.0;
/// The probe stops reading neighbor lists once the sample predicts this
/// many allowed rows: a wide mask is judged from a few lists.
const FAR_PROBE_ENOUGH_EXPECTED: f64 = 32.0;

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
    fn lock(&self, node: u32) -> MutexGuard<'_, ()> {
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
    fn cap(&self, layer: usize) -> usize {
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
    fn list_start(&self, node: u32, layer: usize) -> usize {
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
    fn copy_links(&self, node: u32, layer: usize, locks: Option<&Locks>, out: &mut Vec<u32>) {
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
    fn prefetch_lines(&self) -> usize {
        self.kernel.prefetch_lines(self.dim)
    }

    /// Score every node in `nodes` against `q` inside a single target-feature
    /// call per [`SCORE_CHUNK_NODES`] slice, and deliver `(node, score)` in
    /// `nodes` order.
    #[inline]
    fn for_each_scored(&self, q: &[f32], nodes: &[u32], mut f: impl FnMut(u32, f32)) {
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
    fn greedy(
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
    /// under `#70`'s Higham `4 · γ(d) · |q| · |x|` error bound (`#72`).
    #[inline]
    fn below_floor(&self, node: u32, kernel_score: f32, floor: f32, slack_per_norm: f64) -> bool {
        if !floor.is_finite() {
            return false;
        }
        let inv = self.inv_norm[node as usize];
        if !(inv > 0.0 && inv.is_finite()) {
            return false;
        }
        f64::from(kernel_score) + slack_per_norm / f64::from(inv) < f64::from(floor)
    }

    /// Paper Algorithm 2 at one layer. Leaves up to `ef` results (allowed
    /// nodes only) in `scratch.res`. Disallowed nodes route: they enter the
    /// candidate heap and are expanded, but never the results.
    #[allow(clippy::too_many_arguments)]
    fn search_layer<H: Hook>(
        &self,
        q: &[f32],
        entries: &[Scored],
        ef: usize,
        layer: usize,
        allow: Option<Mask<'_>>,
        patience: Patience,
        floor_prune: FloorPrune,
        locks: Option<&Locks>,
        scratch: &mut Scratch,
        hook: &mut H,
        stats: &mut SearchStats,
    ) -> Walk {
        let ef = ef.max(1);
        scratch.begin(self.n());
        let prune_active = layer == 0 && floor_prune.enabled;
        let mut pruned_by_floor = false;
        let use_top_k = patience.enabled() && patience.top_k > 0 && patience.top_k < ef;
        let window = if use_top_k { patience.top_k } else { ef };
        let min_survived = patience.min_survived(window);
        let mut patience_added = std::mem::take(&mut scratch.patience_added);
        let mut patience_top = std::mem::take(&mut scratch.patience_top);
        patience_added.clear();
        patience_top.clear();
        let mut allowed_scored = 0usize;
        let allowed = |v: u32| allow.is_none_or(|a| a.test(v as usize));
        for &e in entries {
            if scratch.visit(e.node) {
                scratch.cand.push(e);
                if allowed(e.node) {
                    allowed_scored += 1;
                    hook.scored(e.node, e.score, self.row(e.node));
                    scratch.res.push(Reverse(e));
                    if use_top_k {
                        if patience_top.len() < window {
                            patience_top.push(Reverse(e));
                        } else if patience_top.peek().is_some_and(|worst| e > worst.0) {
                            patience_top.pop();
                            patience_top.push(Reverse(e));
                        }
                    }
                }
            }
        }
        while scratch.res.len() > ef {
            scratch.res.pop();
        }
        let mut nbrs = std::mem::take(&mut scratch.nbrs);
        let mut consecutive_saturated = 0u32;
        let mut outcome = Walk::Completed;
        while let Some(c) = scratch.cand.pop() {
            if scratch.res.len() >= ef {
                let worst = scratch.res.peek().map_or(f32::NEG_INFINITY, |r| r.0.score);
                if c.score < worst {
                    break;
                }
            }
            if prune_active
                && allowed_scored >= floor_prune.gate
                && self.below_floor(
                    c.node,
                    c.score,
                    hook.current_floor(),
                    floor_prune.slack_per_norm,
                )
            {
                pruned_by_floor = true;
                continue;
            }
            if patience.enabled() && consecutive_saturated >= patience.hops {
                outcome = Walk::Patience;
                break;
            }
            let floor_live = patience.floor_active || hook.current_floor().is_finite();
            let gate_ok = patience.enabled()
                && scratch.res.len() >= ef
                && (!floor_live || allowed_scored >= patience.ascent_gate)
                && (!use_top_k || patience_top.len() >= window);
            if gate_ok {
                patience_added.clear();
            }
            let mut lost = 0usize;
            stats.hops += 1;
            self.copy_links(c.node, layer, locks, &mut nbrs);
            nbrs.retain(|&v| scratch.visit(v));
            stats.scored += nbrs.len() as u64;
            self.for_each_scored(q, &nbrs, |v, s| {
                let ok = allowed(v);
                if ok {
                    allowed_scored += 1;
                    hook.scored(v, s, self.row(v));
                }
                let full = scratch.res.len() >= ef;
                let worst = scratch.res.peek().map_or(f32::NEG_INFINITY, |r| r.0.score);
                if !full || s > worst {
                    if prune_active
                        && allowed_scored >= floor_prune.gate
                        && self.below_floor(v, s, hook.current_floor(), floor_prune.slack_per_norm)
                    {
                        pruned_by_floor = true;
                        return;
                    }
                    let scored = Scored { score: s, node: v };
                    scratch.cand.push(scored);
                    if ok {
                        scratch.res.push(Reverse(scored));
                        if scratch.res.len() > ef {
                            if let Some(Reverse(evicted)) = scratch.res.pop() {
                                if gate_ok && !use_top_k && !patience_added.contains(&evicted.node)
                                {
                                    lost += 1;
                                }
                            }
                            if gate_ok && !use_top_k {
                                patience_added.push(scored.node);
                            }
                        }
                        if use_top_k {
                            if patience_top.len() < window {
                                patience_top.push(Reverse(scored));
                            } else if patience_top.peek().is_some_and(|w| scored > w.0) {
                                if let Some(Reverse(evicted)) = patience_top.pop() {
                                    if gate_ok && !patience_added.contains(&evicted.node) {
                                        lost += 1;
                                    }
                                }
                                patience_top.push(Reverse(scored));
                                if gate_ok {
                                    patience_added.push(scored.node);
                                }
                            }
                        }
                    }
                }
            });
            if patience.enabled() {
                if gate_ok && window.saturating_sub(lost) >= min_survived {
                    consecutive_saturated += 1;
                } else {
                    consecutive_saturated = 0;
                }
            }
            if !hook.hop() {
                outcome = Walk::Stopped;
                break;
            }
        }
        scratch.nbrs = nbrs;
        scratch.patience_added = patience_added;
        scratch.patience_top = patience_top;
        if outcome == Walk::Completed && pruned_by_floor {
            outcome = Walk::FloorPruned;
        }
        outcome
    }

    /// The filter-aware beam at layer 0 (the predicate-subgraph idea of
    /// ACORN-1, Patel et al., arXiv:2403.04871). Only allowed rows are
    /// scored and only they enter the candidate and result heaps, so the
    /// walk's cost follows the rows it can return. Expanding a node scores
    /// its unvisited allowed neighbors; each disallowed neighbor is a
    /// bridge, and its own allowed neighbors are scored in its place, until
    /// the expansion has scored one full neighbor list (`m0` rows). A
    /// bridge is marked visited only when it is looked through, so one cut
    /// off by the budget can still be used from another node.
    ///
    /// `entries` are already scored and may be disallowed (the descent's
    /// landing point): they are expanded but not returned.
    #[allow(clippy::too_many_arguments)]
    fn search_layer_aware<H: Hook>(
        &self,
        q: &[f32],
        entries: &[Scored],
        ef: usize,
        allow: Mask<'_>,
        visit_budget: Option<u64>,
        patience: Patience,
        floor_prune: FloorPrune,
        scratch: &mut Scratch,
        hook: &mut H,
        stats: &mut SearchStats,
    ) -> Walk {
        let ef = ef.max(1);
        scratch.begin(self.n());
        let mut pruned_by_floor = false;
        let use_top_k = patience.enabled() && patience.top_k > 0 && patience.top_k < ef;
        let window = if use_top_k { patience.top_k } else { ef };
        let min_survived = patience.min_survived(window);
        let mut patience_added = std::mem::take(&mut scratch.patience_added);
        let mut patience_top = std::mem::take(&mut scratch.patience_top);
        patience_added.clear();
        patience_top.clear();
        let mut allowed_scored = 0usize;
        for &e in entries {
            if scratch.visit(e.node) {
                scratch.cand.push(e);
                if allow.test(e.node as usize) {
                    allowed_scored += 1;
                    hook.scored(e.node, e.score, self.row(e.node));
                    scratch.scored_allowed.push(e);
                    scratch.res.push(Reverse(e));
                    if use_top_k {
                        if patience_top.len() < window {
                            patience_top.push(Reverse(e));
                        } else if patience_top.peek().is_some_and(|worst| e > worst.0) {
                            patience_top.pop();
                            patience_top.push(Reverse(e));
                        }
                    }
                }
            }
        }
        while scratch.res.len() > ef {
            scratch.res.pop();
        }
        let mut nbrs = std::mem::take(&mut scratch.nbrs);
        let mut routes = std::mem::take(&mut scratch.routes);
        let mut fresh = std::mem::take(&mut scratch.fresh);
        let hop_budget = self.m0;
        let base_scored = stats.scored;
        let mut consecutive_saturated = 0u32;
        let mut outcome = Walk::Completed;
        while let Some(c) = scratch.cand.pop() {
            if scratch.res.len() >= ef {
                let worst = scratch.res.peek().map_or(f32::NEG_INFINITY, |r| r.0.score);
                if c.score < worst {
                    break;
                }
            }
            if floor_prune.enabled
                && allowed_scored >= floor_prune.gate
                && self.below_floor(
                    c.node,
                    c.score,
                    hook.current_floor(),
                    floor_prune.slack_per_norm,
                )
            {
                pruned_by_floor = true;
                continue;
            }
            if patience.enabled() && consecutive_saturated >= patience.hops {
                outcome = Walk::Patience;
                break;
            }
            if visit_budget.is_some_and(|limit| walk_work_units(stats, base_scored) >= limit) {
                outcome = Walk::Budget;
                break;
            }
            let floor_live = patience.floor_active || hook.current_floor().is_finite();
            let gate_ok = patience.enabled()
                && scratch.res.len() >= ef
                && (!floor_live || allowed_scored >= patience.ascent_gate)
                && (!use_top_k || patience_top.len() >= window);
            if gate_ok {
                patience_added.clear();
            }
            let mut lost = 0usize;
            stats.hops += 1;
            self.copy_links(c.node, 0, None, &mut nbrs);
            routes.clear();
            fresh.clear();
            for &v in nbrs.iter() {
                if allow.test(v as usize) {
                    if scratch.visit(v) {
                        fresh.push(v);
                    }
                } else if !scratch.seen(v) {
                    routes.push(v);
                }
            }
            for &bridge in routes.iter() {
                if fresh.len() >= hop_budget {
                    break;
                }
                if !scratch.visit(bridge) {
                    continue;
                }
                stats.bridges += 1;
                self.copy_links(bridge, 0, None, &mut nbrs);
                for &w in nbrs.iter() {
                    if fresh.len() >= hop_budget {
                        break;
                    }
                    if allow.test(w as usize) && scratch.visit(w) {
                        fresh.push(w);
                    }
                }
            }
            stats.scored += fresh.len() as u64;
            self.for_each_scored(q, &fresh, |v, s| {
                allowed_scored += 1;
                hook.scored(v, s, self.row(v));
                let scored = Scored { score: s, node: v };
                scratch.scored_allowed.push(scored);
                let full = scratch.res.len() >= ef;
                let worst = scratch.res.peek().map_or(f32::NEG_INFINITY, |r| r.0.score);
                if !full || s > worst {
                    if floor_prune.enabled
                        && allowed_scored >= floor_prune.gate
                        && self.below_floor(v, s, hook.current_floor(), floor_prune.slack_per_norm)
                    {
                        pruned_by_floor = true;
                        return;
                    }
                    scratch.cand.push(scored);
                    scratch.res.push(Reverse(scored));
                    if scratch.res.len() > ef {
                        if let Some(Reverse(evicted)) = scratch.res.pop() {
                            if gate_ok && !use_top_k && !patience_added.contains(&evicted.node) {
                                lost += 1;
                            }
                        }
                        if gate_ok && !use_top_k {
                            patience_added.push(scored.node);
                        }
                    }
                    if use_top_k {
                        if patience_top.len() < window {
                            patience_top.push(Reverse(scored));
                        } else if patience_top.peek().is_some_and(|w| scored > w.0) {
                            if let Some(Reverse(evicted)) = patience_top.pop() {
                                if gate_ok && !patience_added.contains(&evicted.node) {
                                    lost += 1;
                                }
                            }
                            patience_top.push(Reverse(scored));
                            if gate_ok {
                                patience_added.push(scored.node);
                            }
                        }
                    }
                }
            });
            if patience.enabled() {
                if gate_ok && window.saturating_sub(lost) >= min_survived {
                    consecutive_saturated += 1;
                } else {
                    consecutive_saturated = 0;
                }
            }
            if !hook.hop() {
                outcome = Walk::Stopped;
                break;
            }
        }
        scratch.nbrs = nbrs;
        scratch.routes = routes;
        scratch.fresh = fresh;
        scratch.patience_added = patience_added;
        scratch.patience_top = patience_top;
        if outcome == Walk::Completed && pruned_by_floor {
            outcome = Walk::FloorPruned;
        }
        outcome
    }

    /// The enumerate regime's pass over the mask: score every allowed row
    /// with the traversal kernel, in row order, and leave in `out` the rows
    /// that can still be among the exact top `k`, for the scalar scorer to
    /// settle. Returns the rows scored.
    ///
    /// A kernel score differs from the scalar score by at most
    /// `2·γ(d)·Σ|qᵢxᵢ| ≤ 2·γ(d)·|q|·|x|` (`distance.rs`). `slack_per_norm`
    /// is `4·γ(d)·|q|` (the factor two covers the stored norm, itself a
    /// kernel result), so `b = slack_per_norm·|x|` bounds the error of row
    /// `x`, and `B`, the same with the longest allowed row, bounds every
    /// row's. With `t` the k-th largest kernel score, k rows have an exact
    /// score of at least `t − B`; a row whose kernel score is below
    /// `t − 2B` has an exact score below that, so k rows beat it and it is
    /// dropped. A row with no stored direction (a zero row, or one whose
    /// norm overflowed) has no bound: it takes no part in `t` and is always
    /// kept.
    pub fn enumerate(
        &self,
        q: &[f32],
        allow: Mask<'_>,
        k: usize,
        slack_per_norm: f64,
        out: &mut Vec<Scored>,
    ) -> u64 {
        self.enumerate_skipping(
            q,
            allow,
            k,
            f32::NEG_INFINITY,
            slack_per_norm,
            &[],
            |_| false,
            out,
        )
    }

    /// [`View::enumerate`] seeded with `already_scored` allowed rows from an
    /// aborted or stranded filter-aware walk and an optional exact score
    /// `floor`, scoring only allowed rows where `!skip(row)` in stack-sized
    /// SIMD chunks and pruning candidates online against both `floor` and the
    /// running `k`-th kernel score before pushing to `out`.
    #[allow(clippy::too_many_arguments)]
    pub fn enumerate_skipping(
        &self,
        q: &[f32],
        allow: Mask<'_>,
        k: usize,
        floor: f32,
        slack_per_norm: f64,
        already_scored: &[Scored],
        skip: impl Fn(u32) -> bool,
        out: &mut Vec<Scored>,
    ) -> u64 {
        out.clear();
        let mut top: BinaryHeap<Reverse<Scored>> = BinaryHeap::with_capacity(k.saturating_add(1));
        let mut shortest_inv = f32::INFINITY;
        let consider =
            |scored: Scored, top: &mut BinaryHeap<Reverse<Scored>>, shortest_inv: &mut f32| {
                if k == 0 {
                    return;
                }
                let inv = self.inv_norm[scored.node as usize];
                if inv > 0.0 && inv.is_finite() {
                    *shortest_inv = (*shortest_inv).min(inv);
                    if top.len() < k {
                        top.push(Reverse(scored));
                    } else if top.peek().is_some_and(|worst| scored > worst.0) {
                        top.pop();
                        top.push(Reverse(scored));
                    }
                }
            };
        for &prior in already_scored {
            consider(prior, &mut top, &mut shortest_inv);
        }
        let finite_floor = floor.is_finite().then(|| f64::from(floor));
        let current_bar = |top: &BinaryHeap<Reverse<Scored>>, shortest_inv: f32| -> Option<f64> {
            let top_bar = if k > 0 && top.len() == k {
                top.peek().map(|Reverse(kth)| {
                    f64::from(kth.score) - slack_per_norm / f64::from(shortest_inv)
                })
            } else {
                None
            };
            match (finite_floor, top_bar) {
                (Some(a), Some(b)) => Some(a.max(b)),
                (Some(a), None) => Some(a),
                (None, Some(b)) => Some(b),
                (None, None) => None,
            }
        };
        let survives = |s: Scored, bar: Option<f64>| -> bool {
            if k == 0 {
                return false;
            }
            let Some(bar) = bar else {
                return true;
            };
            let inv = self.inv_norm[s.node as usize];
            if !(inv > 0.0 && inv.is_finite()) {
                return true;
            }
            f64::from(s.score) + slack_per_norm / f64::from(inv) >= bar
        };

        let lines = self.prefetch_lines();
        let batch = self.kernel.batch_width();
        let mut cur = [0u32; SCORE_CHUNK_NODES];
        let mut cur_len = 0usize;
        let mut next = [0u32; SCORE_CHUNK_NODES];
        let mut next_len = 0usize;
        let mut scores = [0.0f32; SCORE_CHUNK_NODES];
        let mut scored = 0u64;

        // Allowed rows lie apart in filtered masks and contiguous in unlinked
        // tails: double-buffering two stack chunks keeps seam prefetches hot
        // without allocating a candidate vector proportional to mask size.
        let mut flush_chunk = |chunk: &[u32], ahead: &[u32]| {
            for &node in &ahead[..ahead.len().min(batch)] {
                prefetch(self.row(node), lines);
            }
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
                consider(s, &mut top, &mut shortest_inv);
                let bar = current_bar(&top, shortest_inv);
                if survives(s, bar) {
                    out.push(s);
                }
            }
        };

        let _ = allow.try_for_each(|row| {
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
                    flush_chunk(&cur[..cur_len], &next[..next_len]);
                    cur = next;
                    cur_len = next_len;
                    next_len = 0;
                }
            }
            ControlFlow::Continue(())
        });
        if cur_len > 0 {
            flush_chunk(&cur[..cur_len], &next[..next_len]);
        }
        if next_len > 0 {
            flush_chunk(&next[..next_len], &[]);
        }

        let final_bar = current_bar(&top, shortest_inv);
        out.retain(|&s| survives(s, final_bar));
        for &prior in already_scored {
            if survives(prior, final_bar) {
                out.push(prior);
            }
        }
        scored
    }

    /// The far probe: is the neighborhood the descent landed in all but
    /// empty of allowed rows? The sample is `entry`'s layer-0 neighbors and
    /// theirs, read from the links alone (no row is touched), and no larger
    /// than the mask's share needs to be judged
    /// (`FAR_PROBE_ENOUGH_EXPECTED`). A mask spread
    /// through the graph, or gathered around the query, holds its share of
    /// the sample or more; a mask on a region away from the query (one
    /// court, one tenant) holds almost none of it, and a walk from here
    /// would have to cross rows it may not score to rows it cannot rank.
    fn far(
        &self,
        entry: u32,
        allow: Mask<'_>,
        allowed: usize,
        scratch: &mut Scratch,
        stats: &mut SearchStats,
    ) -> bool {
        scratch.begin(self.n());
        scratch.visit(entry);
        let mut near = std::mem::take(&mut scratch.routes);
        let mut nbrs = std::mem::take(&mut scratch.nbrs);
        self.copy_links(entry, 0, None, &mut near);
        near.retain(|&v| scratch.visit(v));
        let mut sample = 1 + near.len();
        let mut hits = usize::from(allow.test(entry as usize))
            + near.iter().filter(|&&v| allow.test(v as usize)).count();
        let share = allowed as f64 / self.n() as f64;
        let enough = (FAR_PROBE_ENOUGH_EXPECTED / share).ceil();
        for &v in near.iter() {
            if sample as f64 >= enough {
                break;
            }
            self.copy_links(v, 0, None, &mut nbrs);
            for &w in nbrs.iter() {
                if scratch.visit(w) {
                    sample += 1;
                    hits += usize::from(allow.test(w as usize));
                }
            }
        }
        scratch.routes = near;
        scratch.nbrs = nbrs;
        stats.probed += sample as u64;
        let expected = sample as f64 * share;
        expected >= FAR_PROBE_MIN_EXPECTED && (hits as f64) * FAR_PROBE_RATIO < expected
    }

    /// Paper Algorithm 5: descend greedily from the entry to layer 1, then
    /// a beam of `ef` at layer 0, steered by `filter`, `patience`, and
    /// `floor_prune`. Results best first.
    #[allow(clippy::too_many_arguments)]
    pub fn walk<H: Hook>(
        &self,
        q: &[f32],
        ef: usize,
        filter: Filter<'_>,
        patience: Patience,
        floor_prune: FloorPrune,
        scratch: &mut Scratch,
        hook: &mut H,
    ) -> (Vec<Scored>, SearchStats, Walk) {
        let mut stats = SearchStats::default();
        if self.entry == NO_NODE {
            return (Vec::new(), stats, Walk::Completed);
        }
        let mut ep = Scored {
            score: self.kernel.dot(q, self.row(self.entry)),
            node: self.entry,
        };
        stats.scored += 1;
        let mut nbrs = std::mem::take(&mut scratch.nbrs);
        for layer in (1..=self.top as usize).rev() {
            ep = self.greedy(q, ep, layer, None, &mut nbrs, &mut stats);
        }
        scratch.nbrs = nbrs;
        let walk = match filter {
            Filter::None => self.search_layer(
                q,
                &[ep],
                ef,
                0,
                None,
                patience,
                floor_prune,
                None,
                scratch,
                hook,
                &mut stats,
            ),
            Filter::Plain(allow) => self.search_layer(
                q,
                &[ep],
                ef,
                0,
                Some(allow),
                patience,
                floor_prune,
                None,
                scratch,
                hook,
                &mut stats,
            ),
            Filter::Aware {
                allow,
                allowed,
                adaptive,
                budget,
            } => {
                if adaptive && self.far(ep.node, allow, allowed, scratch, &mut stats) {
                    return (Vec::new(), stats, Walk::Far);
                }
                let outcome = self.search_layer_aware(
                    q,
                    &[ep],
                    ef,
                    allow,
                    budget,
                    patience,
                    floor_prune,
                    scratch,
                    hook,
                    &mut stats,
                );
                if outcome == Walk::Completed
                    && adaptive
                    && scratch.res.len() < ef.max(1).min(allowed)
                {
                    return (scratch.drain_results(), stats, Walk::Short);
                }
                return (scratch.drain_results(), stats, outcome);
            }
        };
        (scratch.drain_results(), stats, walk)
    }

    /// Paper Algorithm 4 on directions. Candidates arrive best first by
    /// inner product with the base row (the ranking the metric gives);
    /// a candidate is kept when its direction is closer to the base than
    /// to every neighbor already kept: `cos(c, base) > cos(c, s)`. Both
    /// sides share the factor `1/|c|`, so the test multiplies the known
    /// inner products by the other row's `1/|row|` only. With unit rows this
    /// is the paper's test on inner products; with rows of different
    /// lengths a long row cannot crowd out its neighbors just by being long.
    fn select(&self, base_inv_norm: f32, pool: &[Scored], cap: usize, sel: &mut Vec<u32>) {
        sel.clear();
        let lines = self.prefetch_lines();
        for (idx, c) in pool.iter().enumerate() {
            if sel.len() >= cap {
                break;
            }
            if let Some(ahead) = pool.get(idx + 2) {
                prefetch(self.row(ahead.node), lines);
            }
            let to_base = c.score * base_inv_norm;
            let row_c = self.row(c.node);
            let mut keep = true;
            let mut at = 0usize;
            let mut dots = [0.0f32; 4];
            // Short-circuit in 4-neighbor batches on targets with software
            // prefetch: rejected candidates fail against an early,
            // higher-scoring neighbor without scoring the rest of `sel`, while
            // kept candidates score four neighbors per pass over `row_c`.
            // `sel` stays cache-resident across pool iterations, so `lines = 0`
            // avoids redundant prefetches; on 128-bit NEON (`lines == 0`)
            // per-neighbor short-circuiting avoids extra FMAs on early
            // rejections.
            while lines > 0 && at + 4 <= sel.len() {
                self.kernel
                    .score_nodes(row_c, self.rows, self.dim, &sel[at..at + 4], 0, &mut dots);
                let s0 = sel[at] as usize;
                let s1 = sel[at + 1] as usize;
                let s2 = sel[at + 2] as usize;
                let s3 = sel[at + 3] as usize;
                let closer = [
                    dots[0] * self.inv_norm[s0] < to_base,
                    dots[1] * self.inv_norm[s1] < to_base,
                    dots[2] * self.inv_norm[s2] < to_base,
                    dots[3] * self.inv_norm[s3] < to_base,
                ];
                if !closer.into_iter().all(|ok| ok) {
                    keep = false;
                    break;
                }
                at += 4;
            }
            if keep {
                keep = sel[at..].iter().all(|&s| {
                    let to_s = self.kernel.dot(row_c, self.row(s)) * self.inv_norm[s as usize];
                    to_s < to_base
                });
            }
            if keep {
                sel.push(c.node);
            }
        }
    }
}

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

    fn publish_entry(&self, node: u32, top: u8) {
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
    level_mult: f64,
    pub rows: OwnedRows,
    pub inv_norm: Vec<f32>,
    links: Option<GraphLinks>,
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

    pub fn entry_state(&self) -> (u32, u8) {
        self.links
            .as_ref()
            .map_or((NO_NODE, 0), GraphLinks::entry_state)
    }

    pub fn heap_bytes(&self) -> usize {
        (self.rows.values().len() + self.inv_norm.len()) * 4
            + self.links.as_ref().map_or(0, GraphLinks::heap_bytes)
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

struct LinkBuilder<'a> {
    dim: usize,
    m: usize,
    m0: usize,
    ef_construction: usize,
    kernel: Kernel,
    rows: &'a [f32],
    inv_norm: &'a [f32],
    links: &'a GraphLinks,
}

impl LinkBuilder<'_> {
    fn view(&self) -> View<'_, [AtomicU32]> {
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

    fn insert_sequential(&self, range: std::ops::Range<u32>) {
        with_scratch(|scratch| {
            for node in range {
                self.insert(node, None, scratch);
            }
        });
    }

    fn insert_parallel(&self, range: std::ops::Range<u32>) {
        use rayon::iter::{IntoParallelIterator, ParallelIterator};
        let locks = Locks::default();
        range
            .into_par_iter()
            .for_each(|node| with_scratch(|scratch| self.insert(node, Some(&locks), scratch)));
    }

    fn write_links(&self, node: u32, layer: usize, list: &[u32], locks: Option<&Locks>) {
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
    fn insert(&self, node: u32, locks: Option<&Locks>, scratch: &mut Scratch) {
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
    fn connect_back(
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

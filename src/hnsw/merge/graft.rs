//! Grafting of non-join layer-0 rows: a bounded local beam walk seeded from
//! a row's surviving neighborhood and its join-set neighbors' merged lists.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicU32, Ordering as Atomic};

use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

use super::super::graph::{
    with_scratch, LinkBuilder, LinkCells, Locks, Scored, Scratch, View, NO_NODE,
};

/// Maximum local layer-0 expansions per grafted non-join node once seeded in
/// its target Voronoi neighborhood.
const GRAFT_MAX_EXPANSIONS: usize = 40;

/// Minimum initial expansions per grafted node so 1-hop cross-segment bridge
/// seeds always step into sibling segments even when intra-segment cluster
/// neighbors raise the initial beam floor.
const GRAFT_MIN_EXPANSIONS: usize = 10;

/// Intra-segment neighbors that always seed a graft's candidate heap.
const GRAFT_INTRA_SEEDS: usize = 6;

/// Cross-segment seeds that always enter a graft's candidate heap.
const GRAFT_CROSS_SEEDS: usize = 16;

/// The merged layer-0 neighbor lists of the join-set nodes, frozen in a
/// compact CSR table.
pub(super) struct JoinSnapshot {
    /// `slot[node]` indexes `offsets`, `NO_NODE` for a node outside the set.
    slot: Vec<u32>,
    offsets: Vec<u32>,
    edges: Vec<u32>,
}

impl JoinSnapshot {
    /// Snapshot the layer-0 neighbor lists of all `join_nodes`.
    pub(super) fn of<L: LinkCells + ?Sized>(view: &View<'_, L>, join_nodes: &[u32]) -> Self {
        let mut slot = vec![NO_NODE; view.n()];
        let mut offsets = Vec::with_capacity(join_nodes.len() + 1);
        let mut edges = Vec::with_capacity(join_nodes.len() * view.m0);
        let mut nbrs = Vec::with_capacity(view.m0);
        offsets.push(0);
        for (slot_idx, &node) in join_nodes.iter().enumerate() {
            slot[node as usize] = slot_idx as u32;
            view.copy_links(node, 0, None, &mut nbrs);
            edges.extend_from_slice(&nbrs);
            offsets.push(edges.len() as u32);
        }
        Self {
            slot,
            offsets,
            edges,
        }
    }

    /// The frozen list of `node`, empty for a node outside the set.
    fn edges_of(&self, node: u32) -> &[u32] {
        let slot = self.slot[node as usize];
        if slot == NO_NODE {
            return &[];
        }
        let s = slot as usize;
        &self.edges[self.offsets[s] as usize..self.offsets[s + 1] as usize]
    }
}

/// Buffers a graft borrows from the thread's scratch.
struct GraftBuffers {
    cands: Vec<u32>,
    step_nbrs: Vec<u32>,
    pool: Vec<Scored>,
    sel: Vec<u32>,
}

impl GraftBuffers {
    fn take(scratch: &mut Scratch) -> Self {
        Self {
            cands: std::mem::take(&mut scratch.fresh),
            step_nbrs: std::mem::take(&mut scratch.routes),
            pool: std::mem::take(&mut scratch.pool),
            sel: std::mem::take(&mut scratch.sel),
        }
    }

    fn restore(self, scratch: &mut Scratch) {
        scratch.fresh = self.cands;
        scratch.routes = self.step_nbrs;
        scratch.pool = self.pool;
        scratch.sel = self.sel;
    }
}

/// Keep `item` in the result heap `res` of at most `ef` entries; true when it
/// entered.
#[inline]
fn keep_best(res: &mut BinaryHeap<Reverse<Scored>>, ef: usize, item: Scored) -> bool {
    if res.len() < ef {
        res.push(Reverse(item));
        true
    } else if item > res.peek().expect("ef_graft > 0").0 {
        res.pop();
        res.push(Reverse(item));
        true
    } else {
        false
    }
}

struct Grafter<'a> {
    builder: &'a LinkBuilder<'a>,
    view: View<'a, [AtomicU32]>,
    join: &'a JoinSnapshot,
    ef_graft: usize,
}

impl Grafter<'_> {
    /// Graft one node with its surviving intra-segment neighbors `seg_nbrs`.
    fn graft(&self, node: u32, seg_nbrs: &[u32], locks: Option<&Locks>, scratch: &mut Scratch) {
        let mut bufs = GraftBuffers::take(scratch);
        scratch.begin(self.view.n());
        scratch.visit(node);
        let intra_sorted = self.seed_intra(node, seg_nbrs, &mut bufs, scratch);
        self.gather_cross(node, seg_nbrs, locks, &mut bufs, scratch);
        if scratch.res.is_empty() && bufs.cands.is_empty() {
            bufs.restore(scratch);
            self.builder.insert(node, locks, scratch);
            return;
        }
        self.seed_cross(node, &mut bufs, scratch);
        self.expand(node, locks, &mut bufs, scratch);
        self.publish(node, &intra_sorted, locks, &mut bufs, scratch);
        bufs.restore(scratch);
    }

    /// Score the surviving intra-segment layer-0 neighbors into the result
    /// heap so no intra-segment quality is lost, and seed the candidate heap
    /// with the best [`GRAFT_INTRA_SEEDS`] of them. Returns them best first,
    /// so spare slots after direction pruning keep intra-cluster links.
    fn seed_intra(
        &self,
        node: u32,
        seg_nbrs: &[u32],
        bufs: &mut GraftBuffers,
        scratch: &mut Scratch,
    ) -> Vec<u32> {
        let GraftBuffers { cands, pool, .. } = bufs;
        cands.clear();
        for &u in seg_nbrs {
            if scratch.visit(u) {
                cands.push(u);
            }
        }
        pool.clear();
        let mut intra_sorted: Vec<u32> = Vec::with_capacity(cands.len());
        if cands.is_empty() {
            return intra_sorted;
        }
        self.view
            .for_each_scored(self.view.row(node), cands, |w, score| {
                let item = Scored { score, node: w };
                pool.push(item);
                keep_best(&mut scratch.res, self.ef_graft, item);
            });
        pool.sort_unstable_by(|a, b| b.cmp(a));
        for &top_intra in pool.iter().take(GRAFT_INTRA_SEEDS) {
            scratch.cand.push(top_intra);
        }
        intra_sorted.extend(pool.iter().map(|item| item.node));
        intra_sorted
    }

    /// Gather the cross-segment seeds into `bufs.cands`: the reciprocal
    /// back-links already placed on `node`, plus the merged layer-0
    /// neighborhoods of its join-set neighbors.
    fn gather_cross(
        &self,
        node: u32,
        seg_nbrs: &[u32],
        locks: Option<&Locks>,
        bufs: &mut GraftBuffers,
        scratch: &mut Scratch,
    ) {
        let GraftBuffers {
            cands, step_nbrs, ..
        } = bufs;
        cands.clear();
        self.view.copy_links(node, 0, locks, step_nbrs);
        for &w in step_nbrs.iter() {
            if scratch.visit(w) {
                cands.push(w);
            }
        }
        for &u in seg_nbrs {
            for &w in self.join.edges_of(u) {
                if scratch.visit(w) {
                    cands.push(w);
                }
            }
        }
    }

    /// Score the cross-segment seeds into the result heap. The best
    /// [`GRAFT_CROSS_SEEDS`] enter the candidate heap unconditionally, so
    /// 1-hop cross-segment bridges always expand; the rest enter when they
    /// reach the beam.
    fn seed_cross(&self, node: u32, bufs: &mut GraftBuffers, scratch: &mut Scratch) {
        let GraftBuffers { cands, pool, .. } = bufs;
        if cands.is_empty() {
            return;
        }
        pool.clear();
        self.view
            .for_each_scored(self.view.row(node), cands, |w, score| {
                let item = Scored { score, node: w };
                pool.push(item);
                keep_best(&mut scratch.res, self.ef_graft, item);
            });
        pool.sort_unstable_by(|a, b| b.cmp(a));
        let worst_res = scratch.res.peek().map_or(f32::NEG_INFINITY, |r| r.0.score);
        for (idx, &item) in pool.iter().enumerate() {
            if idx < GRAFT_CROSS_SEEDS || item.score >= worst_res {
                scratch.cand.push(item);
            }
        }
    }

    /// The bounded layer-0 beam walk: at least [`GRAFT_MIN_EXPANSIONS`]
    /// expansions before the beam may converge, at most
    /// [`GRAFT_MAX_EXPANSIONS`] in all.
    fn expand(
        &self,
        node: u32,
        locks: Option<&Locks>,
        bufs: &mut GraftBuffers,
        scratch: &mut Scratch,
    ) {
        let GraftBuffers {
            cands, step_nbrs, ..
        } = bufs;
        let ef_graft = self.ef_graft;
        let row_v = self.view.row(node);
        let mut expansions = 0usize;
        while let Some(c) = scratch.cand.pop() {
            if expansions >= GRAFT_MIN_EXPANSIONS
                && scratch.res.len() >= ef_graft
                && c < scratch.res.peek().expect("ef_graft > 0").0
            {
                break;
            }
            if expansions >= GRAFT_MAX_EXPANSIONS {
                break;
            }
            expansions += 1;
            self.view.copy_links(c.node, 0, locks, step_nbrs);
            cands.clear();
            for &w in step_nbrs.iter() {
                if scratch.visit(w) {
                    cands.push(w);
                }
            }
            if !cands.is_empty() {
                self.view.for_each_scored(row_v, cands, |w, score| {
                    let item = Scored { score, node: w };
                    if keep_best(&mut scratch.res, ef_graft, item) {
                        scratch.cand.push(item);
                    }
                });
            }
        }
    }

    /// Select the node's neighbors from the beam, top the list up with its
    /// best intra-segment neighbors, publish it merged with any back-links
    /// earlier or concurrent grafts placed, and reciprocate on the
    /// direction-diversified `select` neighbors.
    fn publish(
        &self,
        node: u32,
        intra_sorted: &[u32],
        locks: Option<&Locks>,
        bufs: &mut GraftBuffers,
        scratch: &mut Scratch,
    ) {
        let GraftBuffers { pool, sel, .. } = bufs;
        let view = &self.view;
        pool.clear();
        pool.reserve(scratch.res.len());
        for Reverse(item) in scratch.res.drain() {
            pool.push(item);
        }
        pool.sort_unstable_by(|a, b| b.cmp(a));
        scratch.cand.clear();
        view.select(view.inv_norm[node as usize], pool, view.m0, sel);
        let sel_recip_len = sel.len();
        let target_own = intra_sorted.len().min(view.m0);
        for &intra_u in intra_sorted {
            if sel.len() >= target_own {
                break;
            }
            if !sel.contains(&intra_u) {
                sel.push(intra_u);
            }
        }
        merge_publish_layer0(self.builder, node, locks, sel, pool, scratch);
        for &e in &sel[..sel_recip_len] {
            self.builder.connect_back(e, node, 0, locks, pool, scratch);
        }
    }
}

/// Graft non-join layer-0 nodes by seeding a bounded local layer-0 beam walk
/// (`ef_graft`, capped at [`GRAFT_MAX_EXPANSIONS`] hops, with zero upper-layer
/// descent) from `{surviving intra-segment neighbors} ∪ {merged layer-0
/// neighbors of join-set neighbors} ∪ {already-placed back-links}`, running
/// `select`, union-publishing with concurrent back-links, and reciprocating via
/// `connect_back`.
pub(super) fn graft_non_join_nodes(
    builder: &LinkBuilder<'_>,
    graft_tasks: &[(u32, Vec<u32>)],
    join: &JoinSnapshot,
    parallel: bool,
) {
    let grafter = Grafter {
        builder,
        view: builder.view(),
        join,
        ef_graft: (builder.ef_construction * 2 / 3).clamp(builder.m0 + 24, 128),
    };
    if parallel {
        let locks = Locks::default();
        graft_tasks.par_iter().for_each(|(node, seg_nbrs)| {
            with_scratch(|scratch| grafter.graft(*node, seg_nbrs, Some(&locks), scratch));
        });
    } else {
        with_scratch(|scratch| {
            for (node, seg_nbrs) in graft_tasks {
                grafter.graft(*node, seg_nbrs, None, scratch);
            }
        });
    }
}

/// Write `sel` as `node`'s layer-0 list, merged with the back-links other
/// grafts already placed there; when the union exceeds the cap the heuristic
/// chooses among all of them.
fn merge_publish_layer0(
    builder: &LinkBuilder<'_>,
    node: u32,
    locks: Option<&Locks>,
    sel: &[u32],
    pool: &mut Vec<Scored>,
    scratch: &mut Scratch,
) {
    let view = builder.view();
    let cap = view.m0;
    let start = view.list_start(node, 0);
    let cells = &builder.links.links0;
    let _guard = locks.map(|l| l.lock(node));
    let count = (cells[start].load(Atomic::Relaxed) as usize).min(cap);
    if count == 0 {
        cells[start].store(sel.len() as u32, Atomic::Relaxed);
        for (i, &v) in sel.iter().enumerate() {
            cells[start + 1 + i].store(v, Atomic::Relaxed);
        }
        return;
    }
    let mut merged = std::mem::take(&mut scratch.nbrs);
    merged.clear();
    merged.extend_from_slice(sel);
    for i in 0..count {
        let v = cells[start + 1 + i].load(Atomic::Relaxed);
        if v != node && !merged.contains(&v) {
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

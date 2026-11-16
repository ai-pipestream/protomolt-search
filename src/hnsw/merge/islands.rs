//! Layer-0 reachability from the entry and the edge surgery that bridges a
//! stranded island into the reachable component.

use std::sync::atomic::Ordering as Atomic;

use crate::boolean_bits::Bits;
use crate::vector::VectorProvider;

use super::super::graph::{LinkBuilder, LinkCells, Scored, Scratch, View, NO_NODE};
use super::super::HnswIndex;

/// Count nodes in `0..index.len()` that are NOT forward-reachable from `entry`
/// via directed layer-0 outgoing edges (`0` when `index` is unlinked or fully
/// connected).
pub(in crate::hnsw) fn unreachable_from_entry(index: &HnswIndex) -> usize {
    if !index.is_graph_linked() || index.is_empty() {
        return 0;
    }
    with_view!(index, |view| count_unreachable(&view))
}

fn count_unreachable<L: LinkCells + ?Sized>(view: &View<'_, L>) -> usize {
    let n = view.n();
    if n == 0 || view.entry == NO_NODE || view.entry as usize >= n {
        return n;
    }
    let mut visited = Bits::empty(n);
    let mut queue = Vec::with_capacity(n);
    let mut nbrs = Vec::with_capacity(view.m0);
    visited.set(view.entry as usize);
    queue.push(view.entry);
    let mut head = 0usize;
    while head < queue.len() {
        let u = queue[head];
        head += 1;
        view.copy_links(u, 0, None, &mut nbrs);
        for &v in &nbrs {
            let vi = v as usize;
            if vi < n && !visited.test(vi) {
                visited.set(vi);
                queue.push(v);
            }
        }
    }
    n.saturating_sub(queue.len())
}

/// Offer `w` to already-visited node `r`'s layer-0 list via `select`, committing
/// the update only if no spanning-tree child (`bfs_parent[old_u] == r`) is evicted.
pub(in crate::hnsw) fn try_connect_visited_to_island(
    builder: &LinkBuilder<'_>,
    r: u32,
    w: u32,
    bfs_parent: &mut [u32],
    scratch: &mut Scratch,
) {
    if r == w {
        return;
    }
    let view = builder.view();
    let cap = view.m0;
    let start = view.list_start(r, 0);
    let cells = &builder.links.links0;
    let count = (cells[start].load(Atomic::Relaxed) as usize).min(cap);
    if (0..count).any(|i| cells[start + 1 + i].load(Atomic::Relaxed) == w) {
        if bfs_parent[w as usize] == NO_NODE {
            bfs_parent[w as usize] = r;
        }
        return;
    }
    if count < cap {
        cells[start + 1 + count].store(w, Atomic::Relaxed);
        cells[start].store((count + 1) as u32, Atomic::Relaxed);
        if bfs_parent[w as usize] == NO_NODE {
            bfs_parent[w as usize] = r;
        }
        return;
    }

    let mut old_list = Vec::with_capacity(count + 1);
    for i in 0..count {
        old_list.push(cells[start + 1 + i].load(Atomic::Relaxed));
    }
    let mut cands = old_list.clone();
    cands.push(w);

    let row_r = view.row(r);
    let mut pool = std::mem::take(&mut scratch.pool);
    let mut sel = std::mem::take(&mut scratch.sel);
    pool.clear();
    pool.reserve(cands.len());
    view.for_each_scored(row_r, &cands, |u, score| {
        pool.push(Scored { score, node: u });
    });
    pool.sort_unstable_by(|a, b| b.cmp(a));
    view.select(view.inv_norm[r as usize], &pool, cap, &mut sel);

    let keeps_w = sel.contains(&w);
    let orphans_tree = old_list
        .iter()
        .any(|&old_u| !sel.contains(&old_u) && bfs_parent[old_u as usize] == r);
    if keeps_w && !orphans_tree {
        cells[start].store(sel.len() as u32, Atomic::Relaxed);
        for (i, &u) in sel.iter().enumerate() {
            cells[start + 1 + i].store(u, Atomic::Relaxed);
        }
        if bfs_parent[w as usize] == NO_NODE {
            bfs_parent[w as usize] = r;
        }
    }
    scratch.pool = pool;
    scratch.sel = sel;
}

/// Deterministically establish a spanning-tree edge `r -> w` from `pool_visited`
/// without detaching any spanning-tree subtree rooted under `entry`.
pub(in crate::hnsw) fn force_incoming_from_visited(
    builder: &LinkBuilder<'_>,
    pool_visited: &[Scored],
    w: u32,
    sel_w: &mut Vec<u32>,
    bfs_parent: &mut [u32],
) {
    let view = builder.view();
    let cap = view.m0;
    let cells = &builder.links.links0;

    // 1. Prefer the highest-scoring visited node with spare capacity (`< m0`).
    for cand in pool_visited {
        let r = cand.node;
        if r == w {
            continue;
        }
        let start = view.list_start(r, 0);
        let count = (cells[start].load(Atomic::Relaxed) as usize).min(cap);
        if (0..count).any(|i| cells[start + 1 + i].load(Atomic::Relaxed) == w) {
            bfs_parent[w as usize] = r;
            return;
        }
        if count < cap {
            cells[start + 1 + count].store(w, Atomic::Relaxed);
            cells[start].store((count + 1) as u32, Atomic::Relaxed);
            bfs_parent[w as usize] = r;
            return;
        }
    }

    // 2. Otherwise replace a non-tree edge (`bfs_parent[old_u] != r`) on the
    //    closest visited node, scanning from its lowest-ranked slot backwards.
    for cand in pool_visited {
        let r = cand.node;
        if r == w {
            continue;
        }
        let start = view.list_start(r, 0);
        let count = (cells[start].load(Atomic::Relaxed) as usize).min(cap);
        for slot in (0..count).rev() {
            let old_u = cells[start + 1 + slot].load(Atomic::Relaxed);
            if bfs_parent[old_u as usize] != r {
                cells[start + 1 + slot].store(w, Atomic::Relaxed);
                bfs_parent[w as usize] = r;
                return;
            }
        }
    }

    // 3. Fallback when every candidate is full and all `m0` slots are spanning-tree
    //    children of `r`: splice `r -> w -> old_u` and update `bfs_parent` so the
    //    spanning tree remains intact.
    let r = pool_visited[0].node;
    let start = view.list_start(r, 0);
    let count = (cells[start].load(Atomic::Relaxed) as usize).min(cap);
    if count > 0 {
        let slot = count - 1;
        let old_u = cells[start + 1 + slot].load(Atomic::Relaxed);
        cells[start + 1 + slot].store(w, Atomic::Relaxed);
        bfs_parent[w as usize] = r;
        if old_u != w {
            bfs_parent[old_u as usize] = w;
            if !sel_w.contains(&old_u) {
                if sel_w.len() < cap {
                    sel_w.push(old_u);
                } else if let Some(pos) = sel_w.iter().rposition(|&x| x != r) {
                    sel_w[pos] = old_u;
                }
            }
        }
    }
}

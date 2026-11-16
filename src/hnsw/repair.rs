//! Directed forward-BFS layer-0 connectivity verification and island repair.

use std::cmp::Reverse;
use std::sync::atomic::{AtomicU32, Ordering as Atomic};

use crate::boolean_bits::Bits;
use crate::vector::Mask;

use super::graph::{
    with_scratch, FloorPrune, LayerSearch, LinkBuilder, NoHook, Patience, Scored, Scratch,
    SearchStats, View, NO_NODE,
};
use super::merge::{force_incoming_from_visited, try_connect_visited_to_island};

struct BfsState {
    visited: Bits,
    visited_count: usize,
    bfs_parent: Vec<u32>,
    queue: Vec<u32>,
    head: usize,
    nbrs: Vec<u32>,
}

/// Count nodes already linked on layer 0 (including `entry` when present).
pub(in crate::hnsw) fn linked_node_count(builder: &LinkBuilder<'_>) -> usize {
    let (entry, _) = builder.links.entry_state();
    if entry == NO_NODE {
        return 0;
    }
    let stride0 = builder.m0 + 1;
    (0..builder.inv_norm.len())
        .filter(|&u| {
            u as u32 == entry || builder.links.links0[u * stride0].load(Atomic::Relaxed) > 0
        })
        .count()
}

/// Verify entry/top invariants and run a directed forward BFS from `entry` on
/// layer 0 across all `0..N` nodes, bridging any unreachable components into
/// the main component without orphaning already-reachable nodes.
pub(in crate::hnsw) fn repair_connectivity(builder: &LinkBuilder<'_>) -> usize {
    repair_populated_connectivity(builder, None, builder.inv_norm.len())
}

/// Repair layer-0 connectivity across the populated subset of `builder` after a
/// parallel `insert_slice` batch, leaving unpopulated slots untouched.
pub(in crate::hnsw) fn repair_slice_connectivity(
    builder: &LinkBuilder<'_>,
    nodes: &[u32],
) -> usize {
    let n = builder.inv_norm.len();
    if n == 0 {
        return 0;
    }
    let (entry, _) = builder.links.entry_state();
    if entry == NO_NODE {
        return 0;
    }
    let mut populated = Bits::empty(n);
    let mut count = 0usize;
    for &u in nodes {
        let ui = u as usize;
        if ui < n && !populated.test(ui) {
            populated.set(ui);
            count += 1;
        }
    }
    if count == n {
        return repair_connectivity(builder);
    }
    let stride0 = builder.m0 + 1;
    let cap0 = builder.m0;
    for u in 0..n {
        let deg = (builder.links.links0[u * stride0].load(Atomic::Relaxed) as usize).min(cap0);
        if u as u32 == entry || deg > 0 {
            if !populated.test(u) {
                populated.set(u);
                count += 1;
            }
            for i in 0..deg {
                let v = builder.links.links0[u * stride0 + 1 + i].load(Atomic::Relaxed) as usize;
                if v < n && !populated.test(v) {
                    populated.set(v);
                    count += 1;
                }
            }
        }
    }
    if count == 0 {
        0
    } else if count == n {
        repair_connectivity(builder)
    } else {
        repair_populated_connectivity(builder, Some(&populated), count)
    }
}

pub(in crate::hnsw) fn repair_populated_connectivity(
    builder: &LinkBuilder<'_>,
    populated: Option<&Bits>,
    target_count: usize,
) -> usize {
    let n = builder.inv_norm.len();
    if n == 0 || target_count == 0 {
        return 0;
    }
    let (entry, top) = ensure_entry(builder, populated);
    let mut state = BfsState {
        visited: Bits::empty(n),
        visited_count: 1,
        bfs_parent: vec![NO_NODE; n],
        queue: Vec::with_capacity(target_count),
        head: 0,
        nbrs: Vec::with_capacity(builder.m0),
    };
    state.bfs_parent[entry as usize] = entry;
    state.visited.set(entry as usize);
    state.queue.push(entry);

    drain_bfs(&builder.view(), populated, &mut state);
    if state.visited_count >= target_count {
        return 0;
    }

    let mut islands_repaired = 0usize;
    let mut scan_cursor = 0usize;
    with_scratch(|scratch| {
        while state.visited_count < target_count {
            islands_repaired += 1;
            let view = builder.view();
            let w = next_island_hub(builder, &view, populated, &state.visited, &mut scan_cursor);
            let mut step_nbrs = Vec::with_capacity(view.m0);
            view.copy_links(w, 0, None, &mut step_nbrs);

            let has_direct_visited = step_nbrs.iter().any(|&u| state.visited.test(u as usize));
            let pool_visited = if has_direct_visited {
                bridge_leaf_island(builder, &view, w, &step_nbrs, &mut state, scratch)
            } else {
                bridge_isolated_island(builder, &view, w, entry, top, &mut state, scratch)
            };
            bridge_upper_layers(
                builder,
                &view,
                w,
                (entry, top),
                &pool_visited,
                &state.visited,
                scratch,
            );
            debug_assert_ne!(state.bfs_parent[w as usize], NO_NODE);
            if !state.visited.test(w as usize) {
                state.visited.set(w as usize);
                state.visited_count += 1;
                state.queue.push(w);
            }
            drain_bfs(&builder.view(), populated, &mut state);
        }
    });
    islands_repaired
}

fn ensure_entry(builder: &LinkBuilder<'_>, populated: Option<&Bits>) -> (u32, u8) {
    let n = builder.inv_norm.len();
    let is_pop = |u: usize| populated.is_none_or(|p| p.test(u));
    let max_lvl = (0..n)
        .filter(|&u| is_pop(u))
        .map(|u| builder.links.level[u])
        .max()
        .unwrap_or(0);
    let (entry, top) = builder.links.entry_state();
    if entry != NO_NODE
        && (entry as usize) < n
        && is_pop(entry as usize)
        && builder.links.level[entry as usize] == max_lvl
        && top == max_lvl
    {
        return (entry, top);
    }
    let view = builder.view();
    let best_entry = (0..n as u32)
        .filter(|&u| is_pop(u as usize) && builder.links.level[u as usize] == max_lvl)
        .max_by_key(|&u| {
            let deg = view.links0[view.list_start(u, 0)].load(Atomic::Relaxed);
            (deg, Reverse(u))
        })
        .expect("at least one populated node");
    builder.links.publish_entry(best_entry, max_lvl);
    (best_entry, max_lvl)
}

fn drain_bfs(view: &View<'_, [AtomicU32]>, populated: Option<&Bits>, state: &mut BfsState) {
    let n = view.n();
    while state.head < state.queue.len() {
        let u = state.queue[state.head];
        state.head += 1;
        view.copy_links(u, 0, None, &mut state.nbrs);
        for &v in &state.nbrs {
            let vi = v as usize;
            if vi < n && populated.is_none_or(|p| p.test(vi)) && !state.visited.test(vi) {
                state.visited.set(vi);
                state.bfs_parent[vi] = u;
                state.visited_count += 1;
                state.queue.push(v);
            }
        }
    }
}

fn next_island_hub(
    builder: &LinkBuilder<'_>,
    view: &View<'_, [AtomicU32]>,
    populated: Option<&Bits>,
    visited: &Bits,
    scan_cursor: &mut usize,
) -> u32 {
    let n = view.n();
    let unreached = |u: usize| populated.is_none_or(|p| p.test(u)) && !visited.test(u);
    while *scan_cursor < n && !unreached(*scan_cursor) {
        *scan_cursor += 1;
    }
    debug_assert!(*scan_cursor < n);
    let window_end = n.min(scan_cursor.saturating_add(4096));
    (*scan_cursor..window_end)
        .filter(|&u| unreached(u))
        .map(|u| u as u32)
        .max_by_key(|&u| {
            let deg = view.links0[view.list_start(u, 0)].load(Atomic::Relaxed);
            (builder.links.level[u as usize], deg, Reverse(u))
        })
        .expect("scan_cursor points to an unreached node")
}

fn score_sorted(view: &View<'_, [AtomicU32]>, q: &[f32], nodes: &[u32]) -> Vec<Scored> {
    let mut pool = Vec::with_capacity(nodes.len());
    view.for_each_scored(q, nodes, |node, score| pool.push(Scored { score, node }));
    pool.sort_unstable_by(|a, b| b.cmp(a));
    pool
}

fn bridge_leaf_island(
    builder: &LinkBuilder<'_>,
    view: &View<'_, [AtomicU32]>,
    w: u32,
    step_nbrs: &[u32],
    state: &mut BfsState,
    scratch: &mut Scratch,
) -> Vec<Scored> {
    scratch.begin(view.n());
    scratch.visit(w);
    let mut vis_nbrs = Vec::with_capacity(view.m0 * 2);
    let mut seeds = Vec::with_capacity(4);
    for &u in step_nbrs {
        if state.visited.test(u as usize) && scratch.visit(u) {
            vis_nbrs.push(u);
            if seeds.len() < 4 {
                seeds.push(u);
            }
        }
    }
    let mut hop_buf = Vec::with_capacity(view.m0);
    for s in seeds {
        if vis_nbrs.len() >= view.m0 * 2 {
            break;
        }
        view.copy_links(s, 0, None, &mut hop_buf);
        for &u in &hop_buf {
            if vis_nbrs.len() >= view.m0 * 2 {
                break;
            }
            if state.visited.test(u as usize) && scratch.visit(u) {
                vis_nbrs.push(u);
            }
        }
    }
    let pv = score_sorted(view, view.row(w), &vis_nbrs);
    let mut sel_w = step_nbrs.to_vec();
    for cand in pv.iter().take(4) {
        try_connect_visited_to_island(builder, cand.node, w, &mut state.bfs_parent, scratch);
    }
    if state.bfs_parent[w as usize] == NO_NODE {
        force_incoming_from_visited(builder, &pv, w, &mut sel_w, &mut state.bfs_parent);
        if sel_w != step_nbrs {
            builder.write_links(w, 0, &sel_w, None);
        }
    }
    pv
}

fn descend_visited_anchor(
    view: &View<'_, [AtomicU32]>,
    q: &[f32],
    entry: u32,
    top: u8,
    visited: &Bits,
) -> Scored {
    let mut ep = Scored {
        score: view.kernel.dot(q, view.row(entry)),
        node: entry,
    };
    let mut step_nbrs = Vec::with_capacity(view.m);
    let mut vis_nbrs = Vec::with_capacity(view.m);
    for layer in (1..=usize::from(top)).rev() {
        loop {
            view.copy_links(ep.node, layer, None, &mut step_nbrs);
            vis_nbrs.clear();
            vis_nbrs.extend(
                step_nbrs
                    .iter()
                    .copied()
                    .filter(|&c| visited.test(c as usize)),
            );
            let mut improved = false;
            view.for_each_scored(q, &vis_nbrs, |cand, s| {
                if s > ep.score {
                    ep = Scored {
                        score: s,
                        node: cand,
                    };
                    improved = true;
                }
            });
            if !improved {
                break;
            }
        }
    }
    ep
}

fn bridge_isolated_island(
    builder: &LinkBuilder<'_>,
    view: &View<'_, [AtomicU32]>,
    w: u32,
    entry: u32,
    top: u8,
    state: &mut BfsState,
    scratch: &mut Scratch,
) -> Vec<Scored> {
    let q = view.row(w);
    let ep = descend_visited_anchor(view, q, entry, top, &state.visited);
    let mut stats = SearchStats::default();
    let search = LayerSearch {
        q,
        ef: builder.ef_construction.max(view.m0),
        layer: 0,
        allow: Some(Mask::Bits(&state.visited)),
        patience: Patience::OFF,
        floor_prune: FloorPrune::OFF,
        locks: None,
    };
    view.search_layer(&search, &[ep], scratch, &mut NoHook, &mut stats);
    let mut pv = scratch.drain_results();
    if pv.is_empty() {
        pv.push(Scored {
            score: view.kernel.dot(q, view.row(entry)),
            node: entry,
        });
    }
    let mut step_nbrs = Vec::with_capacity(view.m0);
    view.copy_links(w, 0, None, &mut step_nbrs);
    let mut cands_w = Vec::with_capacity(step_nbrs.len() + pv.len());
    scratch.begin(view.n());
    scratch.visit(w);
    for &u in &step_nbrs {
        if scratch.visit(u) {
            cands_w.push(u);
        }
    }
    for &item in &pv {
        if scratch.visit(item.node) {
            cands_w.push(item.node);
        }
    }
    let pool_w = score_sorted(view, q, &cands_w);
    let mut sel_w = Vec::with_capacity(view.m0);
    view.select(view.inv_norm[w as usize], &pool_w, view.m0, &mut sel_w);
    if !sel_w.iter().any(|&u| state.visited.test(u as usize)) {
        let best_vis = pv[0].node;
        if sel_w.len() < view.m0 {
            sel_w.push(best_vis);
        } else if let Some(last) = sel_w.last_mut() {
            *last = best_vis;
        }
    }
    for &r in &sel_w {
        if state.visited.test(r as usize) {
            try_connect_visited_to_island(builder, r, w, &mut state.bfs_parent, scratch);
        }
    }
    if state.bfs_parent[w as usize] == NO_NODE {
        force_incoming_from_visited(builder, &pv, w, &mut sel_w, &mut state.bfs_parent);
    }
    builder.write_links(w, 0, &sel_w, None);
    pv
}

fn bridge_upper_layers(
    builder: &LinkBuilder<'_>,
    view: &View<'_, [AtomicU32]>,
    w: u32,
    (entry, top): (u32, u8),
    pool_visited: &[Scored],
    visited: &Bits,
    scratch: &mut Scratch,
) {
    let q = view.row(w);
    let w_lvl = usize::from(builder.links.level[w as usize]);
    let mut step_nbrs = Vec::with_capacity(view.m);
    for layer in 1..=w_lvl {
        view.copy_links(w, layer, None, &mut step_nbrs);
        let mut upper_cands = step_nbrs.clone();
        for pv in pool_visited {
            let r = pv.node;
            if r != w
                && usize::from(builder.links.level[r as usize]) >= layer
                && !upper_cands.contains(&r)
            {
                upper_cands.push(r);
            }
        }
        if !upper_cands.iter().any(|&u| visited.test(u as usize))
            && entry != w
            && usize::from(top) >= layer
        {
            upper_cands.push(entry);
        }
        if !upper_cands.is_empty() {
            let up_pool = score_sorted(view, q, &upper_cands);
            let mut up_sel = Vec::with_capacity(view.m);
            view.select(view.inv_norm[w as usize], &up_pool, view.m, &mut up_sel);
            builder.write_links(w, layer, &up_sel, None);
            let mut cb_pool = Vec::new();
            for &r in &up_sel {
                if visited.test(r as usize) {
                    builder.connect_back(r, w, layer, None, &mut cb_pool, scratch);
                }
            }
        }
    }
}

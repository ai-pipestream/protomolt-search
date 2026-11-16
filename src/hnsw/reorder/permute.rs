//! Applying a node permutation to a linked store, and undoing a local one.

use std::sync::atomic::{AtomicU32, Ordering as Atomic};

use super::super::graph::{GraphLinks, Store, NO_NODE};
use super::super::rows::{OwnedRows, RowSource};
use super::NodeMap;

/// The per-node arrays of a permuted store.
struct PermutedNodes {
    rows: Vec<f32>,
    inv_norm: Vec<f32>,
    level: Vec<u8>,
    upper_slot: Vec<u32>,
    upper_base: Vec<u32>,
    upper_cells: usize,
}

fn permute_nodes(store: &Store, links: &GraphLinks, new_to_old: &[u32]) -> PermutedNodes {
    let n = store.n();
    let dim = store.dim;
    let stride_up = store.m + 1;
    let src_rows = store.rows.values();
    let mut rows = vec![0.0f32; n * dim];
    let mut inv_norm = Vec::with_capacity(n);
    let mut level = Vec::with_capacity(n);
    let mut upper_slot = Vec::with_capacity(n);
    let mut upper_base = Vec::with_capacity(links.upper_base.len());
    let mut upper_cells = 0usize;

    for (new_node, &old_u32) in new_to_old.iter().enumerate() {
        let old = old_u32 as usize;
        rows[new_node * dim..(new_node + 1) * dim]
            .copy_from_slice(&src_rows[old * dim..(old + 1) * dim]);
        inv_norm.push(store.inv_norm[old]);
        let lvl = links.level[old];
        level.push(lvl);
        if lvl == 0 {
            upper_slot.push(NO_NODE);
        } else {
            upper_slot.push(upper_base.len() as u32);
            upper_base.push(upper_cells as u32);
            upper_cells += usize::from(lvl) * stride_up;
        }
    }
    debug_assert_eq!(upper_cells, links.upper.len());
    PermutedNodes {
        rows,
        inv_norm,
        level,
        upper_slot,
        upper_base,
        upper_cells,
    }
}

/// Append the neighbor list at `old_start` in `src` to `out`, renumbered
/// through `old_to_new` and zero-padded to `cap` neighbors.
fn push_renumbered_list(
    out: &mut Vec<AtomicU32>,
    src: &[AtomicU32],
    old_start: usize,
    cap: usize,
    old_to_new: &[u32],
) {
    let count = (src[old_start].load(Atomic::Relaxed) as usize).min(cap);
    out.push(AtomicU32::new(count as u32));
    for i in 0..count {
        let old_nbr = src[old_start + 1 + i].load(Atomic::Relaxed) as usize;
        out.push(AtomicU32::new(old_to_new[old_nbr]));
    }
    for _ in count..cap {
        out.push(AtomicU32::new(0));
    }
}

fn permute_upper(
    store: &Store,
    links: &GraphLinks,
    new_to_old: &[u32],
    old_to_new: &[u32],
    nodes: &PermutedNodes,
) -> Vec<AtomicU32> {
    let m = store.m;
    let stride_up = m + 1;
    let mut upper: Vec<AtomicU32> = Vec::with_capacity(nodes.upper_cells);
    for (new_node, &old_u32) in new_to_old.iter().enumerate() {
        let lvl = usize::from(nodes.level[new_node]);
        if lvl == 0 {
            continue;
        }
        let old_u = links.upper_slot[old_u32 as usize] as usize;
        let old_base = links.upper_base[old_u] as usize;
        for layer_idx in 0..lvl {
            let old_start = old_base + layer_idx * stride_up;
            push_renumbered_list(&mut upper, &links.upper, old_start, m, old_to_new);
        }
    }
    debug_assert_eq!(upper.len(), nodes.upper_cells);
    upper
}

/// The node map a locally reordered store carries, composed with the map
/// `store` already had if it was locally mapped before this permutation.
fn composed_node_map(store: &Store, new_to_old: &[u32]) -> NodeMap {
    let composed = match store.node_map() {
        Some(prev) => new_to_old
            .iter()
            .map(|&mid| prev.node_to_row[mid as usize])
            .collect(),
        None => new_to_old.to_vec(),
    };
    NodeMap::from_new_to_old(composed)
}

/// Build a new [`Store`] whose `rows`, `inv_norm`, `level`, `upper_slot`,
/// `upper_base`, `links0`, `upper`, and `entry` are permuted according to
/// `new_to_old` (`new_node -> old_node`), preserving each neighbor list's
/// relative `select()` similarity order.
pub(in crate::hnsw) fn permute_store_with_links(
    store: &Store,
    links: &GraphLinks,
    new_to_old: &[u32],
    attach_node_map: bool,
) -> Store {
    let n = store.n();
    debug_assert_eq!(new_to_old.len(), n);
    let m0 = store.m0;
    let stride0 = m0 + 1;

    let mut old_to_new = vec![NO_NODE; n];
    for (new_node, &old_node) in new_to_old.iter().enumerate() {
        old_to_new[old_node as usize] = new_node as u32;
    }
    debug_assert!(!old_to_new.contains(&NO_NODE));

    let nodes = permute_nodes(store, links, new_to_old);
    let mut links0: Vec<AtomicU32> = Vec::with_capacity(n * stride0);
    for &old_u32 in new_to_old {
        let old_start = old_u32 as usize * stride0;
        push_renumbered_list(&mut links0, &links.links0, old_start, m0, &old_to_new);
    }
    let upper = permute_upper(store, links, new_to_old, &old_to_new, &nodes);

    let (old_entry, top) = links.entry_state();
    let new_entry = if old_entry == NO_NODE {
        NO_NODE
    } else {
        old_to_new[old_entry as usize]
    };
    let links = GraphLinks::from_parts(
        nodes.level,
        nodes.upper_slot,
        nodes.upper_base,
        links0,
        upper,
        new_entry,
        top,
    );
    let mut out = Store::new(
        store.dim,
        store.m,
        store.ef_construction,
        store.seed,
        store.kernel,
    )
    .with_parts(nodes.rows, nodes.inv_norm, links);
    let map = attach_node_map.then(|| composed_node_map(store, new_to_old));
    out.set_reorder_state(true, map);
    out
}

/// Restore `store.rows` and `store.inv_norm` from internal node order back to
/// external row order `0..n` before appending new unlinked tail rows.
pub(in crate::hnsw) fn unpermute_rows_in_place(store: &mut Store) {
    let Some(map) = store.take_node_map() else {
        store.set_reorder_state(false, None);
        return;
    };
    let n = store.n();
    let dim = store.dim;
    let src_rows = store.rows.values();
    let mut ext_rows = vec![0.0f32; n * dim];
    let mut ext_inv = Vec::with_capacity(n);
    for ext_row in 0..n {
        let node = map.row_to_node[ext_row] as usize;
        ext_rows[ext_row * dim..(ext_row + 1) * dim]
            .copy_from_slice(&src_rows[node * dim..(node + 1) * dim]);
        ext_inv.push(store.inv_norm[node]);
    }
    store.rows = OwnedRows::from_values(dim, ext_rows);
    store.inv_norm = ext_inv;
    store.set_reorder_state(false, None);
}

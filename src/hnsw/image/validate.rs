//! Structural validation of a decoded image: after it, no index into the
//! graph or the node map can go out of bounds.

use super::super::graph::{LinkCells, View, MAX_LEVEL, NO_NODE};
use super::super::HNSW;
use super::Header;
use crate::vector::VectorError;

fn bad(what: String) -> VectorError {
    VectorError::new(format!("{HNSW} image {what}"))
}

pub(super) fn validate_node_map(
    node_to_row: &[u32],
    row_to_node: &[u32],
    n: usize,
) -> Result<(), VectorError> {
    if node_to_row.len() != n || row_to_node.len() != n {
        return Err(bad("node-map sections disagree with the row count".into()));
    }
    for (node, &row_u32) in node_to_row.iter().enumerate() {
        let row = row_u32 as usize;
        if row >= n {
            return Err(bad(format!(
                "node_to_row[{node}] names row {row} over {n} rows"
            )));
        }
        if row_to_node[row] != node as u32 {
            return Err(bad(format!(
                "node_to_row[{node}]={row} disagrees with row_to_node[{row}]={}",
                row_to_node[row]
            )));
        }
    }
    Ok(())
}

/// An unlinked image carries rows only: every link section is empty.
fn validate_unlinked<L: LinkCells + ?Sized>(view: &View<'_, L>) -> Result<(), VectorError> {
    if !view.level.is_empty()
        || !view.upper_slot.is_empty()
        || !view.upper_base.is_empty()
        || view.links0.cell_count() != 0
        || view.upper.cell_count() != 0
    {
        return Err(bad("unlinked image carries non-empty link sections".into()));
    }
    Ok(())
}

/// Every row's level, and the compact upper index the levels imply: slots
/// in row order, each base following its predecessors, and totals and top
/// level that match the header.
fn validate_levels<L: LinkCells + ?Sized>(
    view: &View<'_, L>,
    header: &Header,
) -> Result<(), VectorError> {
    let n = header.n;
    let stride_up = view.m + 1;
    let mut next_slot = 0usize;
    let mut cells = 0usize;
    let mut top = 0u8;
    for v in 0..n {
        let level = view.level[v];
        if level > MAX_LEVEL {
            return Err(bad(format!("row {v} claims level {level}")));
        }
        top = top.max(level);
        let slot = view.upper_slot[v];
        if level == 0 {
            if slot != NO_NODE {
                return Err(bad(format!("level-0 row {v} has an upper slot")));
            }
            continue;
        }
        if slot as usize != next_slot {
            return Err(bad(format!(
                "row {v} has upper slot {slot}, expected {next_slot}"
            )));
        }
        if view.upper_base.get(next_slot).copied() != Some(cells as u32) {
            return Err(bad(format!(
                "upper node {next_slot} has a base that does not follow its predecessors"
            )));
        }
        next_slot += 1;
        cells += usize::from(level) * stride_up;
    }
    if next_slot != header.upper_nodes || cells != header.upper_cells {
        return Err(bad(format!(
            "has {next_slot} upper nodes over {cells} cells; the header says {} over {}",
            header.upper_nodes, header.upper_cells
        )));
    }
    if n > 0 && top != header.top {
        return Err(bad(format!(
            "tops out at level {top}; the header says {}",
            header.top
        )));
    }
    Ok(())
}

/// One adjacency list: its count within the layer's cap, and every
/// neighbor a different in-range row that reaches the layer.
fn validate_list<L: LinkCells + ?Sized>(
    view: &View<'_, L>,
    n: usize,
    node: u32,
    layer: usize,
) -> Result<(), VectorError> {
    let m = view.m;
    let start = view_list_start(view, node, layer);
    let cap = if layer == 0 { 2 * m } else { m };
    let cells = if layer == 0 { view.links0 } else { view.upper };
    let count = cells.get(start) as usize;
    if count > cap {
        return Err(bad(format!(
            "row {node} lists {count} neighbors at layer {layer}, over {cap}"
        )));
    }
    for i in 0..count {
        let e = cells.get(start + 1 + i);
        if e as usize >= n || e == node {
            return Err(bad(format!(
                "row {node} names neighbor {e} at layer {layer}"
            )));
        }
        if usize::from(view.level[e as usize]) < layer {
            return Err(bad(format!(
                "row {node} links to {e} at layer {layer}, above its level"
            )));
        }
    }
    Ok(())
}

/// Check the decoded arrays against each other: levels, the compact upper
/// index, every list's count and every neighbor id. After this, no index
/// into the graph can go out of bounds.
pub(super) fn validate_structure<L: LinkCells + ?Sized>(
    view: &View<'_, L>,
    header: &Header,
) -> Result<(), VectorError> {
    let n = header.n;
    let m = view.m;
    if view.rows.len() != n * view.dim || view.inv_norm.len() != n {
        return Err(bad("row sections disagree with the row count".into()));
    }
    if header.entry == NO_NODE {
        return validate_unlinked(view);
    }
    if view.level.len() != n {
        return Err(bad("row sections disagree with the row count".into()));
    }
    if view.links0.cell_count() != n * (2 * m + 1) || view.upper.cell_count() != header.upper_cells
    {
        return Err(bad("link sections disagree with their counts".into()));
    }
    validate_levels(view, header)?;
    for v in 0..n {
        for layer in 0..=usize::from(view.level[v]) {
            validate_list(view, n, v as u32, layer)?;
        }
    }
    Ok(())
}

fn view_list_start<L: LinkCells + ?Sized>(view: &View<'_, L>, node: u32, layer: usize) -> usize {
    if layer == 0 {
        node as usize * (view.m0 + 1)
    } else {
        let u = view.upper_slot[node as usize] as usize;
        view.upper_base[u] as usize + (layer - 1) * (view.m + 1)
    }
}

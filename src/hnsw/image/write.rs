//! Writing an image: sections in layout order through a hashing writer,
//! then the SHA-256 trailer.

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::atomic::AtomicU32;

use super::super::config::{HnswConfig, LocalityOrder};
use super::super::graph::{GraphLinks, Store, NO_NODE};
use super::super::reorder::NodeMap;
use super::super::rows::RowSource;
use super::super::HNSW;
use super::{Header, Layout, ALIGN, TRAILER_LEN};
use crate::vector::VectorError;

/// A writer that hashes what passes through it, so the trailer never
/// needs the whole image in memory.
struct Hashing<W: Write> {
    inner: W,
    sha: turbo_sha256::Sha256,
    written: u64,
}

impl<W: Write> Write for Hashing<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.sha.update(&buf[..n]);
        self.written += n as u64;
        Ok(n)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// Write the store's image to `path`.
pub fn write(store: &Store, config: &HnswConfig, path: &Path) -> Result<(), VectorError> {
    write_with_links(store, None, config, path)
}

type ImageOut = Hashing<BufWriter<File>>;

fn write_error(path: &Path) -> impl Fn(std::io::Error) -> VectorError + '_ {
    move |e| VectorError::new(format!("write {}: {e}", path.display()))
}

/// Zero-fill from the bytes written so far up to the section at `off`.
fn pad_to(out: &mut ImageOut, off: u64, path: &Path) -> Result<(), VectorError> {
    let gap = off
        .checked_sub(out.written)
        .ok_or_else(|| VectorError::new(format!("{HNSW} image sections written out of order")))?;
    let zeros = [0u8; ALIGN as usize];
    let mut left = gap as usize;
    while left > 0 {
        let take = left.min(zeros.len());
        out.write_all(&zeros[..take]).map_err(write_error(path))?;
        left -= take;
    }
    Ok(())
}

/// The header for `store` with the graph `active` (none for an unlinked
/// image) and the node map that travels with it.
fn image_header(
    store: &Store,
    active: Option<&GraphLinks>,
    node_map: Option<&NodeMap>,
    config: &HnswConfig,
) -> Result<Header, VectorError> {
    let (entry, top, upper_nodes, upper_cells) = match active {
        Some(links) => {
            let (entry, top) = links.entry_state();
            (entry, top, links.upper_base.len(), links.upper.len())
        }
        None => (NO_NODE, 0, 0, 0),
    };
    let n = store.n();
    let layout = Layout::compute(
        n as u64,
        store.dim as u64,
        store.m as u64,
        upper_nodes as u64,
        upper_cells as u64,
        active.is_some(),
        node_map.is_some(),
    )?;
    Ok(Header {
        dim: store.dim,
        config: *config,
        n,
        upper_nodes,
        upper_cells,
        entry,
        top,
        has_node_map: node_map.is_some(),
        layout,
    })
}

fn write_link_sections(
    out: &mut ImageOut,
    links: &GraphLinks,
    layout: &Layout,
    path: &Path,
) -> Result<(), VectorError> {
    let io = write_error(path);
    pad_to(out, layout.level_off, path)?;
    out.write_all(&links.level).map_err(&io)?;
    pad_to(out, layout.upper_slot_off, path)?;
    write_u32s(out, &links.upper_slot).map_err(&io)?;
    pad_to(out, layout.upper_base_off, path)?;
    write_u32s(out, &links.upper_base).map_err(&io)?;
    pad_to(out, layout.links0_off, path)?;
    write_cells(out, &links.links0).map_err(&io)?;
    pad_to(out, layout.upper_off, path)?;
    write_cells(out, &links.upper).map_err(&io)?;
    Ok(())
}

fn write_node_map_sections(
    out: &mut ImageOut,
    map: &NodeMap,
    layout: &Layout,
    path: &Path,
) -> Result<(), VectorError> {
    let io = write_error(path);
    pad_to(out, layout.node_to_row_off, path)?;
    write_u32s(out, &map.node_to_row).map_err(&io)?;
    pad_to(out, layout.row_to_node_off, path)?;
    write_u32s(out, &map.row_to_node).map_err(&io)?;
    Ok(())
}

/// Write `store` to `path` using `built` (when sealing an unlinked frozen
/// store) or `store.links()` (when already linked); when neither carries an
/// entry point (empty or below `MIN_GRAPH_ROWS`), writes a valid unlinked
/// `PMHNSW01` image (`entry == NO_NODE`) with 0-byte link sections.
pub fn write_with_links(
    store: &Store,
    built: Option<&GraphLinks>,
    config: &HnswConfig,
    path: &Path,
) -> Result<(), VectorError> {
    let io = write_error(path);
    let active = built
        .or_else(|| store.links())
        .filter(|l| l.entry_state().0 != NO_NODE);
    let node_map = if active.is_some() && config.locality_order != LocalityOrder::None {
        store.node_map()
    } else {
        None
    };
    let header = image_header(store, active, node_map, config)?;
    let layout = header.layout;
    let file = File::create(path).map_err(&io)?;
    let mut out = Hashing {
        inner: BufWriter::with_capacity(1 << 20, file),
        sha: turbo_sha256::Sha256::new(),
        written: 0,
    };
    out.write_all(&header.encode()).map_err(&io)?;
    pad_to(&mut out, layout.rows_off, path)?;
    write_f32s(&mut out, store.rows.values()).map_err(&io)?;
    pad_to(&mut out, layout.inv_norm_off, path)?;
    write_f32s(&mut out, &store.inv_norm).map_err(&io)?;
    if let Some(links) = active {
        write_link_sections(&mut out, links, &layout, path)?;
    }
    if let Some(map) = node_map {
        write_node_map_sections(&mut out, map, &layout, path)?;
    }
    pad_to(&mut out, layout.total_len - TRAILER_LEN as u64, path)?;
    let digest = out.sha.clone().finish().0;
    out.inner.write_all(&digest).map_err(&io)?;
    out.inner.flush().map_err(&io)?;
    out.inner.get_ref().sync_all().map_err(&io)?;
    Ok(())
}

fn write_f32s<W: Write>(out: &mut W, values: &[f32]) -> std::io::Result<()> {
    let mut buf = Vec::with_capacity(4 * 4096);
    for chunk in values.chunks(4096) {
        buf.clear();
        for v in chunk {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        out.write_all(&buf)?;
    }
    Ok(())
}

fn write_u32s<W: Write>(out: &mut W, values: &[u32]) -> std::io::Result<()> {
    let mut buf = Vec::with_capacity(4 * 4096);
    for chunk in values.chunks(4096) {
        buf.clear();
        for v in chunk {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        out.write_all(&buf)?;
    }
    Ok(())
}

fn write_cells<W: Write>(out: &mut W, cells: &[AtomicU32]) -> std::io::Result<()> {
    let mut buf = Vec::with_capacity(4 * 4096);
    for chunk in cells.chunks(4096) {
        buf.clear();
        for v in chunk {
            buf.extend_from_slice(&v.load(std::sync::atomic::Ordering::Relaxed).to_le_bytes());
        }
        out.write_all(&buf)?;
    }
    Ok(())
}

//! The image header and the section layout its counts imply.

use super::super::config::{BuildMode, HnswConfig, LocalityOrder};
use super::super::graph::{MAX_LEVEL, MAX_ROWS, NO_NODE};
use super::super::{HNSW, MAX_DIM};
use super::{
    align, get_u32, get_u64, put_u32, put_u64, FORMAT, HEADER_CRC_AT, HEADER_LEN, MAGIC,
    TRAILER_LEN,
};
use crate::vector::VectorError;

/// Where every section lives, derived from the counts alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Layout {
    pub rows_off: u64,
    pub inv_norm_off: u64,
    pub level_off: u64,
    pub upper_slot_off: u64,
    pub upper_base_off: u64,
    pub links0_off: u64,
    pub upper_off: u64,
    pub node_to_row_off: u64,
    pub row_to_node_off: u64,
    pub total_len: u64,
}

impl Layout {
    pub(super) fn compute(
        n: u64,
        dim: u64,
        m: u64,
        upper_nodes: u64,
        upper_cells: u64,
        linked: bool,
        has_node_map: bool,
    ) -> Result<Self, VectorError> {
        let overflow = || VectorError::new(format!("{HNSW} image layout overflows"));
        let mut at = HEADER_LEN as u64;
        let mut place = |bytes: u64| -> Result<u64, VectorError> {
            let off = align(at).ok_or_else(overflow)?;
            at = off.checked_add(bytes).ok_or_else(overflow)?;
            Ok(off)
        };
        let graph_n = if linked { n } else { 0 };
        let rows_off = place(
            n.checked_mul(dim)
                .and_then(|x| x.checked_mul(4))
                .ok_or_else(overflow)?,
        )?;
        let inv_norm_off = place(n.checked_mul(4).ok_or_else(overflow)?)?;
        let level_off = place(graph_n)?;
        let upper_slot_off = place(graph_n.checked_mul(4).ok_or_else(overflow)?)?;
        let upper_base_off = place(upper_nodes.checked_mul(4).ok_or_else(overflow)?)?;
        let links0_cells = graph_n
            .checked_mul(
                m.checked_mul(2)
                    .and_then(|x| x.checked_add(1))
                    .ok_or_else(overflow)?,
            )
            .ok_or_else(overflow)?;
        let links0_off = place(links0_cells.checked_mul(4).ok_or_else(overflow)?)?;
        let upper_off = place(upper_cells.checked_mul(4).ok_or_else(overflow)?)?;
        let map_n = if has_node_map { n } else { 0 };
        let node_to_row_off = place(map_n.checked_mul(4).ok_or_else(overflow)?)?;
        let row_to_node_off = place(map_n.checked_mul(4).ok_or_else(overflow)?)?;
        let total_len = align(at)
            .and_then(|x| x.checked_add(TRAILER_LEN as u64))
            .ok_or_else(overflow)?;
        Ok(Self {
            rows_off,
            inv_norm_off,
            level_off,
            upper_slot_off,
            upper_base_off,
            links0_off,
            upper_off,
            node_to_row_off,
            row_to_node_off,
            total_len,
        })
    }
}

/// The parsed, checked header.
#[derive(Debug, Clone)]
pub struct Header {
    pub dim: usize,
    pub config: HnswConfig,
    pub n: usize,
    pub upper_nodes: usize,
    pub upper_cells: usize,
    pub entry: u32,
    pub top: u8,
    pub has_node_map: bool,
    pub layout: Layout,
}

impl Header {
    pub(super) fn encode(&self) -> [u8; HEADER_LEN] {
        let mut h = [0u8; HEADER_LEN];
        h[..8].copy_from_slice(MAGIC);
        put_u32(&mut h, 8, FORMAT);
        put_u32(&mut h, 12, HEADER_LEN as u32);
        put_u32(&mut h, 16, self.dim as u32);
        put_u32(&mut h, 20, self.config.m);
        put_u32(&mut h, 24, self.config.ef_construction);
        put_u32(&mut h, 28, self.config.build_mode.as_u32());
        put_u64(&mut h, 32, self.config.seed);
        put_u64(&mut h, 40, self.n as u64);
        put_u64(&mut h, 48, self.upper_nodes as u64);
        put_u64(&mut h, 56, self.upper_cells as u64);
        put_u32(&mut h, 64, self.entry);
        let packed_top = u32::from(self.top)
            | (u32::from(self.config.locality_order.as_u8()) << 8)
            | (u32::from(self.has_node_map) << 16);
        put_u32(&mut h, 68, packed_top);
        let l = &self.layout;
        for (i, off) in [
            l.rows_off,
            l.inv_norm_off,
            l.level_off,
            l.upper_slot_off,
            l.upper_base_off,
            l.links0_off,
            l.upper_off,
            l.total_len,
        ]
        .iter()
        .enumerate()
        {
            put_u64(&mut h, 72 + 8 * i, *off);
        }
        let crc = crate::wal::crc32(&h[..HEADER_CRC_AT]);
        put_u32(&mut h, HEADER_CRC_AT, crc);
        h
    }

    /// Parse and check a header. Nothing here depends on the body.
    pub(super) fn parse(h: &[u8]) -> Result<Self, VectorError> {
        check_preamble(h)?;
        let dim = get_u32(h, 16) as usize;
        if dim == 0 || dim > MAX_DIM {
            return Err(bad(format!(
                "names dimension {dim}; the limit is {MAX_DIM}"
            )));
        }
        let packed = parse_packed_top(h)?;
        let config = HnswConfig {
            m: get_u32(h, 20),
            ef_construction: get_u32(h, 24),
            build_mode: BuildMode::from_u32(get_u32(h, 28))?,
            seed: get_u64(h, 32),
            locality_order: packed.locality_order,
        };
        config.validate()?;
        let (n, upper_nodes, upper_cells) = parse_counts(h, &config)?;
        let entry = parse_entry(h, &packed, (n, upper_nodes, upper_cells))?;
        let layout = Layout::compute(
            n,
            dim as u64,
            u64::from(config.m),
            upper_nodes,
            upper_cells,
            entry != NO_NODE,
            packed.has_node_map,
        )?;
        check_stored_offsets(h, &layout)?;
        Ok(Self {
            dim,
            config,
            n: n as usize,
            upper_nodes: upper_nodes as usize,
            upper_cells: upper_cells as usize,
            entry,
            top: packed.top as u8,
            has_node_map: packed.has_node_map,
            layout,
        })
    }
}

fn bad(what: String) -> VectorError {
    VectorError::new(format!("{HNSW} image {what}"))
}

/// The fixed prefix: length, magic, format, declared header length, and
/// the header checksum.
fn check_preamble(h: &[u8]) -> Result<(), VectorError> {
    if h.len() < HEADER_LEN {
        return Err(bad(format!(
            "is truncated: {} bytes, the header alone is {HEADER_LEN}",
            h.len()
        )));
    }
    if &h[..8] != MAGIC {
        return Err(bad("has an unknown magic".into()));
    }
    let format = get_u32(h, 8);
    if format != FORMAT {
        return Err(bad(format!(
            "has format {format}; this build reads {FORMAT}"
        )));
    }
    let header_len = get_u32(h, 12);
    if header_len as usize != HEADER_LEN {
        return Err(bad(format!(
            "declares a {header_len}-byte header, expected {HEADER_LEN}"
        )));
    }
    let crc = get_u32(h, HEADER_CRC_AT);
    let actual = crate::wal::crc32(&h[..HEADER_CRC_AT]);
    if crc != actual {
        return Err(bad(format!(
            "header checksum {crc:#010x} does not match {actual:#010x}"
        )));
    }
    Ok(())
}

/// The packed word at offset 68: the top level, the locality order, and
/// the node-map flag.
struct PackedTop {
    top: u32,
    locality_order: LocalityOrder,
    has_node_map: bool,
}

fn parse_packed_top(h: &[u8]) -> Result<PackedTop, VectorError> {
    let raw_top = get_u32(h, 68);
    if (raw_top >> 24) != 0 {
        return Err(bad(format!(
            "declares unknown header flags {raw_top:#010x} at offset 68"
        )));
    }
    let top = raw_top & 0xFF;
    let locality_order = LocalityOrder::from_u8(((raw_top >> 8) & 0xFF) as u8)?;
    let map_flag = ((raw_top >> 16) & 0xFF) as u8;
    if map_flag > 1 {
        return Err(bad(format!("declares invalid node-map flag {map_flag}")));
    }
    let has_node_map = map_flag == 1;
    if has_node_map && locality_order == LocalityOrder::None {
        return Err(bad(
            "declares a node map while locality_order is none".into()
        ));
    }
    Ok(PackedTop {
        top,
        locality_order,
        has_node_map,
    })
}

/// The row count, upper node count, and upper cell count.
fn parse_counts(h: &[u8], config: &HnswConfig) -> Result<(u64, u64, u64), VectorError> {
    let n = get_u64(h, 40);
    if n > MAX_ROWS as u64 {
        return Err(bad(format!("names {n} rows; the limit is {MAX_ROWS}")));
    }
    let upper_nodes = get_u64(h, 48);
    if upper_nodes > n {
        return Err(bad(format!(
            "names {upper_nodes} upper nodes over {n} rows"
        )));
    }
    let upper_cells = get_u64(h, 56);
    let stride_up = u64::from(config.m) + 1;
    let most_cells = n
        .checked_mul(u64::from(MAX_LEVEL))
        .and_then(|x| x.checked_mul(stride_up))
        .ok_or_else(|| bad("overflows its upper cell bound".into()))?;
    if upper_cells > most_cells || !upper_cells.is_multiple_of(stride_up) {
        return Err(bad(format!(
            "names {upper_cells} upper cells, which no {n}-row graph at m={} has",
            config.m
        )));
    }
    Ok((n, upper_nodes, upper_cells))
}

/// The entry point, checked against the counts and the packed word.
fn parse_entry(
    h: &[u8],
    packed: &PackedTop,
    (n, upper_nodes, upper_cells): (u64, u64, u64),
) -> Result<u32, VectorError> {
    let top = packed.top;
    let has_node_map = packed.has_node_map;
    let entry = get_u32(h, 64);
    let linked = entry != NO_NODE;
    if n == 0 && (linked || top != 0 || has_node_map) {
        return Err(bad("is empty but names an entry point".into()));
    }
    if !linked {
        if top != 0 || upper_nodes != 0 || upper_cells != 0 || has_node_map {
            return Err(bad(format!(
                "is unlinked (entry = NO_NODE) but declares top={top}, upper_nodes={upper_nodes}, upper_cells={upper_cells}, has_node_map={has_node_map}"
            )));
        }
    } else if u64::from(entry) >= n || top > u32::from(MAX_LEVEL) {
        return Err(bad(format!(
            "names entry {entry} at level {top} over {n} rows"
        )));
    }
    Ok(entry)
}

/// The stored section offsets must be the ones the counts lay out to.
fn check_stored_offsets(h: &[u8], layout: &Layout) -> Result<(), VectorError> {
    let stored = (
        get_u64(h, 72),
        get_u64(h, 80),
        get_u64(h, 88),
        get_u64(h, 96),
        get_u64(h, 104),
        get_u64(h, 112),
        get_u64(h, 120),
        get_u64(h, 128),
    );
    let expected = (
        layout.rows_off,
        layout.inv_norm_off,
        layout.level_off,
        layout.upper_slot_off,
        layout.upper_base_off,
        layout.links0_off,
        layout.upper_off,
        layout.total_len,
    );
    if stored != expected {
        return Err(bad(
            "stores section offsets that do not follow from its counts".into(),
        ));
    }
    if usize::try_from(layout.total_len).is_err() {
        return Err(bad("does not fit this platform's address space".into()));
    }
    Ok(())
}

//! The persisted image (`PMHNSW01`, format 1).
//!
//! ```text
//! header   192 bytes, little-endian, CRC32 over its first 136 bytes
//! sections rows f32 | inv_norm f32 | level u8 | upper_slot u32 | upper_base u32
//!          | links0 u32 | upper u32, each starting on a 64-byte boundary
//! trailer  SHA-256 of everything before it
//! ```
//!
//! Loading parses and checks the header first (magic, format, CRC, every
//! count, and that the counts lay out to exactly the file's length) before
//! any section is allocated or touched, then verifies the trailer, then
//! walks the structure (levels, compact upper index, every list's count and
//! neighbor ids). `load_mapped` serves rows and links straight from the
//! map: the 64-byte section alignment makes the typed views aligned.

use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::atomic::AtomicU32;

use super::config::{BuildMode, HnswConfig};
use super::distance::Kernel;
use super::graph::{GraphLinks, LinkCells, Store, View, MAX_LEVEL, MAX_ROWS, NO_NODE};
use super::rows::{MappedRows, RowSource};
use super::{HNSW, MAX_DIM};
use crate::vector::VectorError;

const MAGIC: &[u8; 8] = b"PMHNSW01";
const FORMAT: u32 = 1;
const HEADER_LEN: usize = 192;
const HEADER_CRC_AT: usize = 136;
const ALIGN: u64 = 64;
const TRAILER_LEN: usize = 32;

fn align(x: u64) -> Option<u64> {
    x.checked_add(ALIGN - 1).map(|y| y / ALIGN * ALIGN)
}

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
    pub total_len: u64,
}

impl Layout {
    fn compute(
        n: u64,
        dim: u64,
        m: u64,
        upper_nodes: u64,
        upper_cells: u64,
        linked: bool,
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
    pub layout: Layout,
}

impl Header {
    fn encode(&self) -> [u8; HEADER_LEN] {
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
        put_u32(&mut h, 68, u32::from(self.top));
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
    fn parse(h: &[u8]) -> Result<Self, VectorError> {
        let bad = |what: String| VectorError::new(format!("{HNSW} image {what}"));
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
        let dim = get_u32(h, 16) as usize;
        if dim == 0 || dim > MAX_DIM {
            return Err(bad(format!(
                "names dimension {dim}; the limit is {MAX_DIM}"
            )));
        }
        let config = HnswConfig {
            m: get_u32(h, 20),
            ef_construction: get_u32(h, 24),
            build_mode: BuildMode::from_u32(get_u32(h, 28))?,
            seed: get_u64(h, 32),
        };
        config.validate()?;
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
        let entry = get_u32(h, 64);
        let top = get_u32(h, 68);
        let linked = entry != NO_NODE;
        if n == 0 && (linked || top != 0) {
            return Err(bad("is empty but names an entry point".into()));
        }
        if !linked {
            if top != 0 || upper_nodes != 0 || upper_cells != 0 {
                return Err(bad(format!(
                    "is unlinked (entry = NO_NODE) but declares top={top}, upper_nodes={upper_nodes}, upper_cells={upper_cells}"
                )));
            }
        } else if u64::from(entry) >= n || top > u32::from(MAX_LEVEL) {
            return Err(bad(format!(
                "names entry {entry} at level {top} over {n} rows"
            )));
        }
        let layout = Layout::compute(
            n,
            dim as u64,
            u64::from(config.m),
            upper_nodes,
            upper_cells,
            linked,
        )?;
        let stored = Layout {
            rows_off: get_u64(h, 72),
            inv_norm_off: get_u64(h, 80),
            level_off: get_u64(h, 88),
            upper_slot_off: get_u64(h, 96),
            upper_base_off: get_u64(h, 104),
            links0_off: get_u64(h, 112),
            upper_off: get_u64(h, 120),
            total_len: get_u64(h, 128),
        };
        if stored != layout {
            return Err(bad(
                "stores section offsets that do not follow from its counts".into(),
            ));
        }
        if usize::try_from(layout.total_len).is_err() {
            return Err(bad("does not fit this platform's address space".into()));
        }
        Ok(Self {
            dim,
            config,
            n: n as usize,
            upper_nodes: upper_nodes as usize,
            upper_cells: upper_cells as usize,
            entry,
            top: top as u8,
            layout,
        })
    }
}

/// A writer that hashes what passes through it, so the trailer never
/// needs the whole image in memory.
struct Hashing<W: Write> {
    inner: W,
    sha: crate::sha256::Sha256,
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
    let io = |e: std::io::Error| VectorError::new(format!("write {}: {e}", path.display()));
    let active = built
        .or_else(|| store.links())
        .filter(|l| l.entry_state().0 != NO_NODE);
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
    )?;
    let header = Header {
        dim: store.dim,
        config: *config,
        n,
        upper_nodes,
        upper_cells,
        entry,
        top,
        layout,
    };
    let file = File::create(path).map_err(io)?;
    let mut out = Hashing {
        inner: BufWriter::with_capacity(1 << 20, file),
        sha: crate::sha256::Sha256::new(),
        written: 0,
    };
    out.write_all(&header.encode()).map_err(io)?;
    let pad_to = |out: &mut Hashing<BufWriter<File>>, off: u64| -> Result<(), VectorError> {
        let gap = off.checked_sub(out.written).ok_or_else(|| {
            VectorError::new(format!("{HNSW} image sections written out of order"))
        })?;
        let zeros = [0u8; ALIGN as usize];
        let mut left = gap as usize;
        while left > 0 {
            let take = left.min(zeros.len());
            out.write_all(&zeros[..take]).map_err(io)?;
            left -= take;
        }
        Ok(())
    };
    pad_to(&mut out, layout.rows_off)?;
    write_f32s(&mut out, store.rows.values()).map_err(io)?;
    pad_to(&mut out, layout.inv_norm_off)?;
    write_f32s(&mut out, &store.inv_norm).map_err(io)?;
    if let Some(links) = active {
        pad_to(&mut out, layout.level_off)?;
        out.write_all(&links.level).map_err(io)?;
        pad_to(&mut out, layout.upper_slot_off)?;
        write_u32s(&mut out, &links.upper_slot).map_err(io)?;
        pad_to(&mut out, layout.upper_base_off)?;
        write_u32s(&mut out, &links.upper_base).map_err(io)?;
        pad_to(&mut out, layout.links0_off)?;
        write_cells(&mut out, &links.links0).map_err(io)?;
        pad_to(&mut out, layout.upper_off)?;
        write_cells(&mut out, &links.upper).map_err(io)?;
    }
    pad_to(&mut out, layout.total_len - TRAILER_LEN as u64)?;
    let digest = out.sha.clone().finalize();
    out.inner.write_all(&digest).map_err(io)?;
    out.inner.flush().map_err(io)?;
    out.inner.get_ref().sync_all().map_err(io)?;
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

/// Open the file, check the header against the file's length, and return
/// the header with the open file positioned after it.
fn open_checked(path: &Path) -> Result<(File, Header), VectorError> {
    let io = |e: std::io::Error| VectorError::new(format!("load {}: {e}", path.display()));
    let mut file = File::open(path).map_err(io)?;
    let len = file.metadata().map_err(io)?.len();
    let mut head = [0u8; HEADER_LEN];
    if len < HEADER_LEN as u64 {
        return Err(VectorError::new(format!(
            "load {}: {HNSW} image is truncated: {len} bytes, the header alone is {HEADER_LEN}",
            path.display()
        )));
    }
    file.read_exact(&mut head).map_err(io)?;
    let header = Header::parse(&head)
        .map_err(|e| VectorError::new(format!("load {}: {e}", path.display())))?;
    if header.layout.total_len != len {
        return Err(VectorError::new(format!(
            "load {}: {HNSW} image declares {} bytes but the file holds {len}",
            path.display(),
            header.layout.total_len
        )));
    }
    Ok((file, header))
}

fn check_trailer(bytes: &[u8], path: &Path) -> Result<(), VectorError> {
    let body = bytes.len() - TRAILER_LEN;
    let digest = crate::sha256::digest(&bytes[..body]);
    if digest[..] != bytes[body..] {
        return Err(VectorError::new(format!(
            "load {}: {HNSW} image trailer does not match its contents",
            path.display()
        )));
    }
    Ok(())
}

fn section(bytes: &[u8], off: u64, len: usize) -> &[u8] {
    let off = off as usize;
    &bytes[off..off + len]
}

/// Read an image onto the heap.
pub fn load_heap(path: &Path, kernel: Kernel) -> Result<(Store, HnswConfig), VectorError> {
    let io = |e: std::io::Error| VectorError::new(format!("load {}: {e}", path.display()));
    let (mut file, header) = open_checked(path)?;
    let total = header.layout.total_len as usize;
    let mut bytes = vec![0u8; total];
    bytes[..HEADER_LEN].copy_from_slice(&header.encode());
    file.read_exact(&mut bytes[HEADER_LEN..]).map_err(io)?;
    check_trailer(&bytes, path)?;
    let l = &header.layout;
    let n = header.n;
    let graph_n = if header.entry == NO_NODE { 0 } else { n };
    let m = header.config.m as usize;
    let rows = f32s_from(section(&bytes, l.rows_off, n * header.dim * 4));
    let inv_norm = f32s_from(section(&bytes, l.inv_norm_off, n * 4));
    let level = section(&bytes, l.level_off, graph_n).to_vec();
    let upper_slot = u32s_from(section(&bytes, l.upper_slot_off, graph_n * 4));
    let upper_base = u32s_from(section(&bytes, l.upper_base_off, header.upper_nodes * 4));
    let links0 = u32s_from(section(&bytes, l.links0_off, graph_n * (2 * m + 1) * 4));
    let upper = u32s_from(section(&bytes, l.upper_off, header.upper_cells * 4));
    drop(bytes);
    let view = View {
        dim: header.dim,
        m,
        m0: 2 * m,
        rows: &rows,
        inv_norm: &inv_norm,
        level: &level,
        upper_slot: &upper_slot,
        upper_base: &upper_base,
        links0: links0.as_slice(),
        upper: upper.as_slice(),
        entry: header.entry,
        top: header.top,
        kernel,
    };
    validate_structure(&view, &header)
        .map_err(|e| VectorError::new(format!("load {}: {e}", path.display())))?;
    let store = Store::from_parts(
        header.dim,
        m,
        header.config.ef_construction as usize,
        header.config.seed,
        kernel,
        rows,
        inv_norm,
        level,
        upper_slot,
        upper_base,
        links0.into_iter().map(AtomicU32::new).collect(),
        upper.into_iter().map(AtomicU32::new).collect(),
        header.entry,
        header.top,
    );
    Ok((store, header.config))
}

/// An image served from its file through a memory map.
pub struct Mapped {
    map: memmap2::Mmap,
    header: Header,
    kernel: Kernel,
}

impl Mapped {
    pub fn header(&self) -> &Header {
        &self.header
    }

    pub fn n(&self) -> usize {
        self.header.n
    }

    /// Bytes of the whole image.
    pub fn byte_len(&self) -> usize {
        self.map.len()
    }

    /// The row bytes of `row`, as stored.
    pub fn row_bytes(&self, row: usize) -> &[u8] {
        let stride = self.header.dim * 4;
        let at = self.header.layout.rows_off as usize + row * stride;
        &self.map[at..at + stride]
    }

    /// The rows, read in place from the map.
    pub fn rows(&self) -> MappedRows<'_> {
        let h = &self.header;
        MappedRows::new(
            h.dim,
            f32s_view(section(&self.map, h.layout.rows_off, h.n * h.dim * 4)),
        )
    }

    pub fn view(&self) -> View<'_, [u32]> {
        let h = &self.header;
        let l = &h.layout;
        let n = h.n;
        let graph_n = if h.entry == NO_NODE { 0 } else { n };
        let m = h.config.m as usize;
        View {
            dim: h.dim,
            m,
            m0: 2 * m,
            rows: self.rows().into_values(),
            inv_norm: f32s_view(section(&self.map, l.inv_norm_off, n * 4)),
            level: section(&self.map, l.level_off, graph_n),
            upper_slot: u32s_view(section(&self.map, l.upper_slot_off, graph_n * 4)),
            upper_base: u32s_view(section(&self.map, l.upper_base_off, h.upper_nodes * 4)),
            links0: u32s_view(section(&self.map, l.links0_off, graph_n * (2 * m + 1) * 4)),
            upper: u32s_view(section(&self.map, l.upper_off, h.upper_cells * 4)),
            entry: h.entry,
            top: h.top,
            kernel: self.kernel,
        }
    }
}

/// Map an image and check it; rows and links are then read from the map.
pub fn load_mapped(path: &Path, kernel: Kernel) -> Result<Mapped, VectorError> {
    let io = |e: std::io::Error| VectorError::new(format!("map {}: {e}", path.display()));
    let (file, header) = open_checked(path)?;
    // SAFETY: the image is an immutable artifact of a sealed segment
    // generation; writers publish a new file and never modify or truncate
    // this one while it is open, which is the condition a shared file map
    // relies on.
    let map = unsafe { memmap2::MmapOptions::new().map(&file) }.map_err(io)?;
    if map.len() != header.layout.total_len as usize {
        return Err(VectorError::new(format!(
            "map {}: mapped {} bytes of a {}-byte image",
            path.display(),
            map.len(),
            header.layout.total_len
        )));
    }
    if !(map.as_ptr() as usize).is_multiple_of(ALIGN as usize) {
        return Err(VectorError::new(format!(
            "map {}: the map is not {ALIGN}-byte aligned, so its sections cannot be read in place",
            path.display()
        )));
    }
    check_trailer(&map, path)?;
    let mapped = Mapped {
        map,
        header,
        kernel,
    };
    validate_structure(&mapped.view(), &mapped.header)
        .map_err(|e| VectorError::new(format!("map {}: {e}", path.display())))?;
    Ok(mapped)
}

/// Check the decoded arrays against each other: levels, the compact upper
/// index, every list's count and every neighbor id. After this, no index
/// into the graph can go out of bounds.
fn validate_structure<L: LinkCells + ?Sized>(
    view: &View<'_, L>,
    header: &Header,
) -> Result<(), VectorError> {
    let bad = |what: String| VectorError::new(format!("{HNSW} image {what}"));
    let n = header.n;
    let m = view.m;
    let stride_up = m + 1;
    if view.rows.len() != n * view.dim || view.inv_norm.len() != n {
        return Err(bad("row sections disagree with the row count".into()));
    }
    if header.entry == NO_NODE {
        if !view.level.is_empty()
            || !view.upper_slot.is_empty()
            || !view.upper_base.is_empty()
            || view.links0.cell_count() != 0
            || view.upper.cell_count() != 0
        {
            return Err(bad("unlinked image carries non-empty link sections".into()));
        }
        return Ok(());
    }
    if view.level.len() != n {
        return Err(bad("row sections disagree with the row count".into()));
    }
    if view.links0.cell_count() != n * (2 * m + 1) || view.upper.cell_count() != header.upper_cells
    {
        return Err(bad("link sections disagree with their counts".into()));
    }
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
    let check_list = |node: u32, layer: usize| -> Result<(), VectorError> {
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
    };
    for v in 0..n {
        for layer in 0..=usize::from(view.level[v]) {
            check_list(v as u32, layer)?;
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

fn f32s_from(bytes: &[u8]) -> Vec<f32> {
    bytes
        .as_chunks::<4>()
        .0
        .iter()
        .map(|b| f32::from_le_bytes(*b))
        .collect()
}

fn u32s_from(bytes: &[u8]) -> Vec<u32> {
    bytes
        .as_chunks::<4>()
        .0
        .iter()
        .map(|b| u32::from_le_bytes(*b))
        .collect()
}

/// An in-place `f32` view of a mapped section. The layout aligns every
/// section to 64 bytes and `load_mapped` checks the map's alignment, which
/// the assertion restates; the image is little-endian, as every target this
/// crate builds for is.
fn f32s_view(bytes: &[u8]) -> &[f32] {
    assert!(bytes.len().is_multiple_of(4) && (bytes.as_ptr() as usize).is_multiple_of(4));
    // SAFETY: the pointer is 4-aligned and the length is a multiple of 4
    // (asserted above); the bytes are a borrowed, initialized part of the
    // map that lives as long as the returned slice; every bit pattern is a
    // valid f32; the map is never written through, so no aliasing write
    // exists.
    unsafe { std::slice::from_raw_parts(bytes.as_ptr().cast::<f32>(), bytes.len() / 4) }
}

/// An in-place `u32` view of a mapped section; see [`f32s_view`].
fn u32s_view(bytes: &[u8]) -> &[u32] {
    assert!(bytes.len().is_multiple_of(4) && (bytes.as_ptr() as usize).is_multiple_of(4));
    // SAFETY: as for `f32s_view`; every bit pattern is a valid u32.
    unsafe { std::slice::from_raw_parts(bytes.as_ptr().cast::<u32>(), bytes.len() / 4) }
}

fn put_u32(h: &mut [u8], at: usize, v: u32) {
    h[at..at + 4].copy_from_slice(&v.to_le_bytes());
}

fn put_u64(h: &mut [u8], at: usize, v: u64) {
    h[at..at + 8].copy_from_slice(&v.to_le_bytes());
}

fn get_u32(h: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(h[at..at + 4].try_into().expect("4 bytes"))
}

fn get_u64(h: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(h[at..at + 8].try_into().expect("8 bytes"))
}

#[cfg(test)]
pub(super) fn header_len() -> usize {
    HEADER_LEN
}

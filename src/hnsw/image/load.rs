//! Loading an image: onto the heap, or served in place from a memory map.

use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::atomic::AtomicU32;

use super::super::config::{HnswConfig, LocalityOrder};
use super::super::distance::Kernel;
use super::super::graph::{Store, View, NO_NODE};
use super::super::reorder::NodeMap;
use super::super::rows::MappedRows;
use super::super::HNSW;
use super::validate::{validate_node_map, validate_structure};
use super::{f32s_from, f32s_view, u32s_from, u32s_view, Header, ALIGN, HEADER_LEN, TRAILER_LEN};
use crate::vector::VectorError;

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
    let node_map = if header.has_node_map {
        let node_to_row = u32s_from(section(&bytes, l.node_to_row_off, n * 4));
        let row_to_node = u32s_from(section(&bytes, l.row_to_node_off, n * 4));
        validate_node_map(&node_to_row, &row_to_node, n)
            .map_err(|e| VectorError::new(format!("load {}: {e}", path.display())))?;
        Some(NodeMap {
            node_to_row,
            row_to_node,
        })
    } else {
        None
    };
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
    let mut store = Store::from_parts(
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
    store.set_reorder_state(
        header.entry != NO_NODE && header.config.locality_order != LocalityOrder::None,
        node_map,
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

    #[inline]
    pub fn node_to_row(&self) -> Option<&[u32]> {
        let h = &self.header;
        h.has_node_map
            .then(|| u32s_view(section(&self.map, h.layout.node_to_row_off, h.n * 4)))
    }

    #[inline]
    pub fn row_to_node(&self) -> Option<&[u32]> {
        let h = &self.header;
        h.has_node_map
            .then(|| u32s_view(section(&self.map, h.layout.row_to_node_off, h.n * 4)))
    }

    /// The row bytes of external `row`, as stored.
    pub fn row_bytes(&self, row: usize) -> &[u8] {
        let node = self.row_to_node().map_or(row, |m| m[row] as usize);
        let stride = self.header.dim * 4;
        let at = self.header.layout.rows_off as usize + node * stride;
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

    #[cfg(unix)]
    pub fn advise_cold(&self) -> Result<(), VectorError> {
        if self.map.is_empty() {
            return Ok(());
        }
        // SAFETY: `self.map` is a valid immutable file-backed memory map of
        // length `self.map.len()` aligned to the OS page size.
        let ptr = self.map.as_ptr() as *mut libc::c_void;
        let len = self.map.len();
        for (name, advice) in [
            ("MADV_RANDOM", libc::MADV_RANDOM),
            ("MADV_DONTNEED", libc::MADV_DONTNEED),
        ] {
            let rc = unsafe { libc::madvise(ptr, len, advice) };
            if rc != 0 {
                return Err(VectorError::new(format!(
                    "{HNSW} madvise({name}): {}",
                    std::io::Error::last_os_error()
                )));
            }
        }
        Ok(())
    }

    #[cfg(not(unix))]
    pub fn advise_cold(&self) -> Result<(), VectorError> {
        Ok(())
    }
}

/// Evict clean kernel page-cache pages for `path` (`POSIX_FADV_DONTNEED`).
#[cfg(target_os = "linux")]
pub fn drop_file_page_cache(path: &Path) -> Result<(), VectorError> {
    use std::os::fd::AsRawFd;
    let file = File::open(path)
        .map_err(|e| VectorError::new(format!("open {} for fadvise: {e}", path.display())))?;
    // SAFETY: `file` is an open file descriptor owned for the duration of the call.
    let rc = unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
    if rc != 0 {
        return Err(VectorError::new(format!(
            "posix_fadvise(DONTNEED) on {} returned {rc}",
            path.display()
        )));
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn drop_file_page_cache(_path: &Path) -> Result<(), VectorError> {
    Ok(())
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
    if let (Some(n2r), Some(r2n)) = (mapped.node_to_row(), mapped.row_to_node()) {
        validate_node_map(n2r, r2n, mapped.header.n)
            .map_err(|e| VectorError::new(format!("map {}: {e}", path.display())))?;
    }
    validate_structure(&mapped.view(), &mapped.header)
        .map_err(|e| VectorError::new(format!("map {}: {e}", path.display())))?;
    Ok(mapped)
}

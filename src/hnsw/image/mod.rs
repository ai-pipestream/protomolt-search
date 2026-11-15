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

mod header;
mod load;
mod validate;
mod write;

pub use header::{Header, Layout};
pub use load::{drop_file_page_cache, load_heap, load_mapped, Mapped};
pub use write::{write, write_with_links};

const MAGIC: &[u8; 8] = b"PMHNSW01";
const FORMAT: u32 = 1;
const HEADER_LEN: usize = 192;
const HEADER_CRC_AT: usize = 136;
const ALIGN: u64 = 64;
const TRAILER_LEN: usize = 32;

fn align(x: u64) -> Option<u64> {
    x.checked_add(ALIGN - 1).map(|y| y / ALIGN * ALIGN)
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

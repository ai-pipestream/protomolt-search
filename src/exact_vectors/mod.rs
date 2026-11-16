//! Product-owned FP32 vectors for candidate reranking.
//!
//! The vector provider owns its native index and score. This sidecar keeps the
//! original row-major vectors in the product generation so a public dense
//! query can select candidates with that provider and rescore the fixed pool
//! with an ordinary FP32 dot product. Persisted stores are memory-mapped. A
//! persisted shard builds new rows on disk ([`ExactVectorStore::spilling`]).
//! Appends to a mapped store retain the immutable base and spill only the new
//! rows. A full `write` streams both into one compatible snapshot file. An
//! in-memory shard keeps a heap builder until `write` persists it.

use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

mod append;
mod open;
mod persist;
mod score;
mod segmented;
#[cfg(test)]
mod tests;

const PAGE_BYTES: usize = 4096;
const MIN_ROWS_PER_TASK: usize = 256;

pub(crate) fn rerank_task_count(rows: usize, parallelism: usize) -> usize {
    parallelism
        .max(1)
        .min(rows.div_ceil(MIN_ROWS_PER_TASK).max(1))
}

const MAGIC: &[u8; 8] = b"PMEXACT1";
const VERSION: u32 = 1;
const HEADER_BYTES: usize = 80;
const FLAG_LITTLE_ENDIAN_F32: u32 = 1;
const HASH_START: usize = 40;
const HASH_END: usize = 72;
const HEADER_CRC_START: usize = 72;

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
thread_local! {
    static FAIL_FINALIZE_SYNC: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[derive(Debug)]
enum Storage {
    Building {
        dim: Option<usize>,
        values: Vec<f32>,
    },
    /// An append-only builder on disk: the rows go straight to the file
    /// that [`ExactVectorStore::write`] finalizes in place (header, then
    /// a rename onto `target`). Reads serve rows by offset. The payload
    /// digest runs as rows arrive, so finalizing costs one header write.
    Spilled {
        target: PathBuf,
        path: PathBuf,
        file: File,
        dim: Option<usize>,
        rows: usize,
        digest: turbo_sha256::Sha256,
        finalized: AtomicBool,
    },
    /// The immutable mapped prefix plus only the new rows in a private spill.
    /// A full write remains an explicit snapshot/export operation.
    Appended {
        base: Box<ExactVectorStore>,
        delta: Box<ExactVectorStore>,
    },
    Segmented(segmented::SegmentedExact),
    Mapped {
        path: PathBuf,
        map: memmap2::Mmap,
        dim: usize,
        rows: usize,
        payload_sha256: [u8; 32],
    },
}

/// Original vectors aligned one-for-one with a shard's provider slots.
#[derive(Debug)]
pub struct ExactVectorStore {
    storage: Storage,
}

/// Exact scores plus observable storage work. Rows remain in caller request
/// order even though mmap reads are scheduled in page order.
#[derive(Debug, Clone, PartialEq)]
pub struct ScoredSlots {
    pub rows: Vec<(usize, f32)>,
    pub logical_bytes: u64,
    pub pages_touched: u64,
    pub tasks: u32,
}

impl ExactVectorStore {
    /// Drop a mapped store's resident pages from the process; the
    /// mapping stays valid and a later read faults them back in. A
    /// building or spilling store has no mapping and is unchanged.
    pub fn release_pages(&self) -> io::Result<()> {
        match &self.storage {
            // A read-only file mapping: DONTNEED drops the pages, the
            // next access reads the file again.
            Storage::Mapped { map, .. } => unsafe {
                map.unchecked_advise(memmap2::UncheckedAdvice::DontNeed)
            },
            Storage::Appended { base, delta } => {
                base.release_pages()?;
                delta.release_pages()
            }
            Storage::Segmented(view) => view.release_pages(),
            _ => Ok(()),
        }
    }

    /// An appendable empty store. `dim` may remain unknown until the first
    /// vector batch arrives.
    pub fn empty(dim: Option<usize>) -> Self {
        Self {
            storage: Storage::Building {
                dim,
                values: Vec::new(),
            },
        }
    }

    /// An appendable empty store that builds on disk next to `target`
    /// (`<target>.building`; a stale one from an interrupted run is
    /// truncated). [`Self::write`] to `target` finalizes it in place.
    pub fn spilling(target: &Path, dim: Option<usize>) -> io::Result<Self> {
        let parent = target.parent().unwrap_or_else(|| Path::new("."));
        std::fs::create_dir_all(parent)?;
        let path = spill_path(target);
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&path)?;
        file.write_all_at(&[0u8; HEADER_BYTES], 0)?;
        Ok(Self {
            storage: Storage::Spilled {
                target: target.to_path_buf(),
                path,
                file,
                dim,
                rows: 0,
                digest: turbo_sha256::Sha256::new(),
                finalized: AtomicBool::new(false),
            },
        })
    }

    /// Build an in-memory store from row-major FP32 values.
    pub fn from_values(dim: usize, values: Vec<f32>) -> io::Result<Self> {
        validate_shape(dim, values.len())?;
        if let Some((index, value)) = values
            .iter()
            .copied()
            .enumerate()
            .find(|(_, v)| !v.is_finite())
        {
            return Err(invalid(format!(
                "exact vector coordinate {index} is not finite: {value}"
            )));
        }
        Ok(Self {
            storage: Storage::Building {
                dim: Some(dim),
                values,
            },
        })
    }

    pub fn dim(&self) -> Option<usize> {
        match &self.storage {
            Storage::Building { dim, .. } => *dim,
            Storage::Spilled { dim, .. } => *dim,
            Storage::Mapped { dim, .. } => Some(*dim),
            Storage::Appended { base, .. } => base.dim(),
            Storage::Segmented(view) => Some(view.dim),
        }
    }

    pub fn len(&self) -> usize {
        match &self.storage {
            Storage::Building { dim, values } => dim.map_or(0, |d| values.len() / d),
            Storage::Spilled { rows, .. } => *rows,
            Storage::Mapped { rows, .. } => *rows,
            Storage::Appended { base, delta } => base.len() + delta.len(),
            Storage::Segmented(view) => view.rows,
        }
    }

    /// Whether new rows build on disk, including a mapped-base/delta view.
    pub fn is_spilled(&self) -> bool {
        matches!(
            self.storage,
            Storage::Spilled { .. } | Storage::Appended { .. }
        )
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_mapped(&self) -> bool {
        matches!(self.storage, Storage::Mapped { .. } | Storage::Segmented(_))
    }

    /// The SHA-256 the persisted store keeps over its payload: every row's
    /// little-endian `f32` bytes in row order. `None` while the store is
    /// still being built or is a view over several files.
    pub fn payload_sha256(&self) -> Option<[u8; 32]> {
        match &self.storage {
            Storage::Mapped { payload_sha256, .. } => Some(*payload_sha256),
            _ => None,
        }
    }

    pub fn path(&self) -> Option<&Path> {
        match &self.storage {
            Storage::Mapped { path, .. } => Some(path),
            Storage::Building { .. }
            | Storage::Spilled { .. }
            | Storage::Appended { .. }
            | Storage::Segmented(_) => None,
        }
    }
}

impl Drop for ExactVectorStore {
    fn drop(&mut self) {
        if let Storage::Spilled {
            path, finalized, ..
        } = &self.storage
        {
            if !finalized.load(Ordering::Acquire) {
                let _ = std::fs::remove_file(path);
            }
        }
    }
}

/// The disk builder's file: beside its target, never the target itself,
/// so an interrupted build cannot pass for a finished store.
fn spill_path(target: &Path) -> PathBuf {
    let mut name = target.as_os_str().to_owned();
    name.push(".building");
    PathBuf::from(name)
}

/// [`dot`] over little-endian bytes of unknown alignment: the same
/// scalar accumulation in row order, so the bits match the heap path.
fn dot_bytes(row: &[u8], query: &[f32]) -> f32 {
    row.as_chunks::<4>()
        .0
        .iter()
        .zip(query)
        .map(|(bytes, q)| f32::from_le_bytes(*bytes) * q)
        .sum()
}

/// The rerank's dot product: scalar accumulation in row order, so every
/// consumer that needs the same bits (the quality profile's brute ground
/// truth) reuses it rather than restating it.
pub fn dot(row: &[f32], query: &[f32]) -> f32 {
    row.iter().zip(query).map(|(a, b)| a * b).sum()
}

#[cfg(target_endian = "little")]
fn dot_mapped(row: &[u8], query: &[f32]) -> f32 {
    // SAFETY: the mmap base is page-aligned, HEADER_BYTES is divisible by
    // f32 alignment, every row begins at an f32 multiple, and every bit
    // pattern is a valid f32. The format flag and target cfg both require LE.
    let (prefix, values, suffix) = unsafe { row.align_to::<f32>() };
    debug_assert!(prefix.is_empty() && suffix.is_empty());
    dot(values, query)
}

#[cfg(target_endian = "big")]
fn dot_mapped(row: &[u8], query: &[f32]) -> f32 {
    row.as_chunks::<4>()
        .0
        .iter()
        .zip(query)
        .map(|(bytes, q)| f32::from_le_bytes(*bytes) * q)
        .sum()
}

fn validate_shape(dim: usize, value_count: usize) -> io::Result<()> {
    if dim == 0 {
        return Err(invalid("exact-vector dimension must be positive"));
    }
    if !value_count.is_multiple_of(dim) {
        return Err(invalid(format!(
            "{value_count} exact-vector values are not a multiple of dim {dim}"
        )));
    }
    Ok(())
}

fn make_header(
    dim: usize,
    rows: usize,
    payload_bytes: usize,
    payload_sha256: [u8; 32],
) -> io::Result<[u8; HEADER_BYTES]> {
    let mut header = [0u8; HEADER_BYTES];
    header[..8].copy_from_slice(MAGIC);
    header[8..12].copy_from_slice(&VERSION.to_le_bytes());
    header[12..16].copy_from_slice(&(HEADER_BYTES as u32).to_le_bytes());
    header[16..20].copy_from_slice(
        &u32::try_from(dim)
            .map_err(|_| invalid("exact-vector dimension exceeds u32"))?
            .to_le_bytes(),
    );
    header[20..24].copy_from_slice(&FLAG_LITTLE_ENDIAN_F32.to_le_bytes());
    header[24..32].copy_from_slice(
        &u64::try_from(rows)
            .map_err(|_| invalid("exact-vector row count exceeds u64"))?
            .to_le_bytes(),
    );
    header[32..40].copy_from_slice(
        &u64::try_from(payload_bytes)
            .map_err(|_| invalid("exact-vector payload exceeds u64"))?
            .to_le_bytes(),
    );
    header[HASH_START..HASH_END].copy_from_slice(&payload_sha256);
    let crc = crate::wal::crc32(&header[..HEADER_CRC_START]);
    header[HEADER_CRC_START..HEADER_CRC_START + 4].copy_from_slice(&crc.to_le_bytes());
    Ok(header)
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().expect("fixed header"))
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().expect("fixed header"))
}

fn unique_temp_path(path: &Path) -> PathBuf {
    let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let mut name = path.as_os_str().to_owned();
    name.push(format!(".tmp-{}-{sequence}", std::process::id()));
    PathBuf::from(name)
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

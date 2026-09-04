//! Product-owned FP32 vectors for candidate reranking.
//!
//! The vector provider owns its native index and score. This sidecar keeps the
//! original row-major vectors in the product generation so a public dense
//! query can select candidates with that provider and rescore the fixed pool
//! with an ordinary FP32 dot product. Persisted stores are memory-mapped. A
//! persisted shard builds its store on disk ([`ExactVectorStore::spilling`]):
//! every appended row goes to the file [`ExactVectorStore::write`] later
//! finalizes in place, so an ingest never holds its FP32 rows in heap. An
//! in-memory shard keeps a heap builder until `write` persists it.

use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

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
        digest: crate::sha256::Sha256,
        finalized: AtomicBool,
    },
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
                digest: crate::sha256::Sha256::new(),
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

    /// Open and structurally validate a persisted store without faulting its
    /// complete payload into memory. [`Self::verify_payload`] performs the
    /// explicit full SHA-256 integrity pass.
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = File::open(path)?;
        let map = unsafe { memmap2::MmapOptions::new().map(&file)? };
        if map.len() < HEADER_BYTES {
            return Err(invalid(format!(
                "{} is truncated: {} bytes, header requires {HEADER_BYTES}",
                path.display(),
                map.len()
            )));
        }
        let header = &map[..HEADER_BYTES];
        if &header[..8] != MAGIC {
            return Err(invalid(format!(
                "{} has unknown exact-vector magic",
                path.display()
            )));
        }
        let version = read_u32(header, 8);
        if version != VERSION {
            return Err(invalid(format!(
                "{} has exact-vector version {version}, expected {VERSION}",
                path.display()
            )));
        }
        if read_u32(header, 12) as usize != HEADER_BYTES {
            return Err(invalid(format!(
                "{} has an unsupported exact-vector header size",
                path.display()
            )));
        }
        if read_u32(header, 20) != FLAG_LITTLE_ENDIAN_F32 {
            return Err(invalid(format!(
                "{} has unsupported exact-vector encoding flags",
                path.display()
            )));
        }
        let expected_header_crc = read_u32(header, HEADER_CRC_START);
        let actual_header_crc = crate::wal::crc32(&header[..HEADER_CRC_START]);
        if actual_header_crc != expected_header_crc {
            return Err(invalid(format!(
                "{} exact-vector header CRC mismatch",
                path.display()
            )));
        }
        let dim = read_u32(header, 16) as usize;
        let rows = usize::try_from(read_u64(header, 24))
            .map_err(|_| invalid("exact-vector row count does not fit this platform"))?;
        let payload_bytes = usize::try_from(read_u64(header, 32))
            .map_err(|_| invalid("exact-vector payload size does not fit this platform"))?;
        let expected_payload = rows
            .checked_mul(dim)
            .and_then(|n| n.checked_mul(std::mem::size_of::<f32>()))
            .ok_or_else(|| invalid("exact-vector dimensions overflow the file size"))?;
        if dim == 0 || payload_bytes != expected_payload {
            return Err(invalid(format!(
                "{} exact-vector shape is inconsistent: {rows}x{dim} but {payload_bytes} payload bytes",
                path.display()
            )));
        }
        let expected_len = HEADER_BYTES
            .checked_add(payload_bytes)
            .ok_or_else(|| invalid("exact-vector file size overflow"))?;
        if map.len() != expected_len {
            return Err(invalid(format!(
                "{} exact-vector length is {}, expected {expected_len}",
                path.display(),
                map.len()
            )));
        }
        let mut payload_sha256 = [0u8; 32];
        payload_sha256.copy_from_slice(&header[HASH_START..HASH_END]);
        Ok(Self {
            storage: Storage::Mapped {
                path: path.to_path_buf(),
                map,
                dim,
                rows,
                payload_sha256,
            },
        })
    }

    pub fn dim(&self) -> Option<usize> {
        match &self.storage {
            Storage::Building { dim, .. } => *dim,
            Storage::Spilled { dim, .. } => *dim,
            Storage::Mapped { dim, .. } => Some(*dim),
        }
    }

    pub fn len(&self) -> usize {
        match &self.storage {
            Storage::Building { dim, values } => dim.map_or(0, |d| values.len() / d),
            Storage::Spilled { rows, .. } => *rows,
            Storage::Mapped { rows, .. } => *rows,
        }
    }

    /// Whether the store builds on disk ([`Self::spilling`]).
    pub fn is_spilled(&self) -> bool {
        matches!(self.storage, Storage::Spilled { .. })
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_mapped(&self) -> bool {
        matches!(self.storage, Storage::Mapped { .. })
    }

    pub fn path(&self) -> Option<&Path> {
        match &self.storage {
            Storage::Mapped { path, .. } => Some(path),
            Storage::Building { .. } | Storage::Spilled { .. } => None,
        }
    }

    /// Append complete rows. A mapped store becomes a disk builder beside
    /// its own file (its payload copied, never decoded into heap) only
    /// when a real append arrives; read-only serving stays mmap-backed.
    pub fn append(&mut self, vectors: &[f32], dim: usize) -> io::Result<()> {
        if vectors.is_empty() {
            return Ok(());
        }
        validate_shape(dim, vectors.len())?;
        if let Some((index, value)) = vectors
            .iter()
            .copied()
            .enumerate()
            .find(|(_, v)| !v.is_finite())
        {
            return Err(invalid(format!(
                "exact vector coordinate {index} is not finite: {value}"
            )));
        }
        if self.dim().is_some_and(|known| known != dim) {
            return Err(invalid(format!(
                "exact-vector append dim {dim} does not match store dim {}",
                self.dim().expect("checked Some")
            )));
        }
        if let Storage::Mapped { path, .. } = &self.storage {
            let target = path.clone();
            let mut spilled = Self::spilling(&target, Some(dim))?;
            let Storage::Mapped { map, rows, .. } = &self.storage else {
                unreachable!("matched above")
            };
            let Storage::Spilled {
                file,
                rows: spilled_rows,
                digest,
                ..
            } = &mut spilled.storage
            else {
                unreachable!("spilling() builds a Spilled store")
            };
            let payload = &map[HEADER_BYTES..];
            for (i, chunk) in payload.chunks(1 << 20).enumerate() {
                digest.update(chunk);
                file.write_all_at(chunk, (HEADER_BYTES + (i << 20)) as u64)?;
            }
            *spilled_rows = *rows;
            self.storage = std::mem::replace(
                &mut spilled.storage,
                Storage::Building {
                    dim: None,
                    values: Vec::new(),
                },
            );
        }
        match &mut self.storage {
            Storage::Building { dim: known, values } => {
                *known = Some(dim);
                values.extend_from_slice(vectors);
            }
            Storage::Spilled {
                file,
                dim: known,
                rows,
                digest,
                ..
            } => {
                let mut bytes = Vec::with_capacity(vectors.len() * 4);
                for value in vectors {
                    bytes.extend_from_slice(&value.to_le_bytes());
                }
                let offset = HEADER_BYTES as u64 + (*rows as u64) * (dim as u64) * 4;
                file.write_all_at(&bytes, offset)?;
                digest.update(&bytes);
                *known = Some(dim);
                *rows += vectors.len() / dim;
            }
            Storage::Mapped { .. } => unreachable!("mapped store converted above"),
        }
        Ok(())
    }

    /// Atomically persist and reopen the store. The returned instance is
    /// always mmap-backed.
    pub fn write(&self, path: &Path) -> io::Result<Self> {
        if self.path() == Some(path) {
            return Self::open(path);
        }
        let dim = self
            .dim()
            .ok_or_else(|| invalid("cannot persist an exact-vector store before dim is known"))?;
        if let Storage::Spilled {
            target,
            path: spill,
            file,
            rows,
            digest,
            finalized,
            ..
        } = &self.storage
        {
            if target == path {
                let payload_bytes = rows
                    .checked_mul(dim)
                    .and_then(|n| n.checked_mul(std::mem::size_of::<f32>()))
                    .ok_or_else(|| invalid("exact-vector payload size overflow"))?;
                let header = make_header(dim, *rows, payload_bytes, digest.clone().finalize())?;
                file.write_all_at(&header, 0)?;
                file.sync_all()?;
                std::fs::rename(spill, path)?;
                crate::postings::fsync_parent(path)?;
                finalized.store(true, Ordering::Release);
                return Self::open(path);
            }
        }
        let rows = self.len();
        let payload_bytes = rows
            .checked_mul(dim)
            .and_then(|n| n.checked_mul(std::mem::size_of::<f32>()))
            .ok_or_else(|| invalid("exact-vector payload size overflow"))?;
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        std::fs::create_dir_all(parent)?;
        let tmp = unique_temp_path(path);
        let result = (|| -> io::Result<()> {
            let file = OpenOptions::new().create_new(true).write(true).open(&tmp)?;
            let mut out = BufWriter::new(file);
            out.write_all(&[0u8; HEADER_BYTES])?;
            let mut digest = crate::sha256::Sha256::new();
            match &self.storage {
                Storage::Building { values, .. } => {
                    let mut bytes = Vec::with_capacity(64 * 1024);
                    for chunk in values.chunks(16 * 1024) {
                        bytes.clear();
                        for value in chunk {
                            bytes.extend_from_slice(&value.to_le_bytes());
                        }
                        digest.update(&bytes);
                        out.write_all(&bytes)?;
                    }
                }
                Storage::Spilled { file, rows, .. } => {
                    let payload_bytes = rows * dim * 4;
                    let mut buf = vec![0u8; 1 << 20];
                    let mut done = 0usize;
                    while done < payload_bytes {
                        let take = buf.len().min(payload_bytes - done);
                        file.read_exact_at(&mut buf[..take], (HEADER_BYTES + done) as u64)?;
                        digest.update(&buf[..take]);
                        out.write_all(&buf[..take])?;
                        done += take;
                    }
                }
                Storage::Mapped { map, .. } => {
                    let payload = &map[HEADER_BYTES..];
                    digest.update(payload);
                    out.write_all(payload)?;
                }
            }
            out.flush()?;
            let mut file = out
                .into_inner()
                .map_err(|e| io::Error::new(e.error().kind(), e.to_string()))?;
            let header = make_header(dim, rows, payload_bytes, digest.finalize())?;
            file.seek(SeekFrom::Start(0))?;
            file.write_all(&header)?;
            file.sync_all()?;
            std::fs::rename(&tmp, path)?;
            crate::postings::fsync_parent(path)?;
            Ok(())
        })();
        if result.is_err() {
            let _ = std::fs::remove_file(&tmp);
        }
        result?;
        Self::open(path)
    }

    /// Write one store whose rows are `parts` back to back, streaming each
    /// part's payload with a bounded buffer — the dense whole-shard sidecar
    /// a compaction assembles from its sealed segments' FP32 files
    /// (`docs/mutations.md`) without decoding them into heap. Every part
    /// must be a persisted store of dimension `dim`. Returns the new store
    /// opened mapped.
    pub fn write_concatenated(dim: usize, parts: &[&Path], path: &Path) -> io::Result<Self> {
        if dim == 0 {
            return Err(invalid(
                "cannot concatenate exact-vector stores of dimension 0",
            ));
        }
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        std::fs::create_dir_all(parent)?;
        let tmp = unique_temp_path(path);
        let result = (|| -> io::Result<()> {
            let file = OpenOptions::new().create_new(true).write(true).open(&tmp)?;
            let mut out = BufWriter::new(file);
            out.write_all(&[0u8; HEADER_BYTES])?;
            let mut digest = crate::sha256::Sha256::new();
            let mut rows = 0usize;
            let mut payload_bytes = 0usize;
            for part in parts {
                let opened = Self::open(part)?;
                if opened.dim() != Some(dim) {
                    return Err(invalid(format!(
                        "{} has dimension {:?}, expected {dim}",
                        part.display(),
                        opened.dim()
                    )));
                }
                let Storage::Mapped { map, .. } = &opened.storage else {
                    unreachable!("open() maps the file")
                };
                for chunk in map[HEADER_BYTES..].chunks(1 << 20) {
                    digest.update(chunk);
                    out.write_all(chunk)?;
                }
                rows = rows
                    .checked_add(opened.len())
                    .ok_or_else(|| invalid("exact-vector row count overflow"))?;
                payload_bytes = payload_bytes
                    .checked_add(map.len() - HEADER_BYTES)
                    .ok_or_else(|| invalid("exact-vector payload size overflow"))?;
            }
            out.flush()?;
            let mut file = out
                .into_inner()
                .map_err(|e| io::Error::new(e.error().kind(), e.to_string()))?;
            let header = make_header(dim, rows, payload_bytes, digest.finalize())?;
            file.seek(SeekFrom::Start(0))?;
            file.write_all(&header)?;
            file.sync_all()?;
            std::fs::rename(&tmp, path)?;
            crate::postings::fsync_parent(path)?;
            Ok(())
        })();
        if result.is_err() {
            let _ = std::fs::remove_file(&tmp);
        }
        result?;
        Self::open(path)
    }

    /// Verify the payload against the SHA-256 committed in the header.
    /// This intentionally scans the complete file and is therefore an
    /// explicit integrity operation rather than part of ordinary mmap open.
    pub fn verify_payload(&self) -> io::Result<()> {
        let Storage::Mapped {
            map,
            payload_sha256,
            path,
            ..
        } = &self.storage
        else {
            return Ok(());
        };
        let actual = crate::sha256::digest(&map[HEADER_BYTES..]);
        if &actual != payload_sha256 {
            return Err(invalid(format!(
                "{} exact-vector payload SHA-256 mismatch",
                path.display()
            )));
        }
        Ok(())
    }

    /// Score local slots by FP32 dot product, returning `(slot, score)` in
    /// request order. Callers own global-id routing and final ordering.
    pub fn score_slots(&self, query: &[f32], slots: &[usize]) -> io::Result<Vec<(usize, f32)>> {
        self.score_slots_profiled(query, slots, 1)
            .map(|result| result.rows)
    }

    /// Score local slots after ordering reads by mmap page, using at most
    /// `parallelism` bounded worker lanes. Every individual dot product keeps
    /// the original scalar accumulation order, so scheduling does not change
    /// score bits. The result is restored to request order before return.
    pub fn score_slots_profiled(
        &self,
        query: &[f32],
        slots: &[usize],
        parallelism: usize,
    ) -> io::Result<ScoredSlots> {
        let dim = self
            .dim()
            .ok_or_else(|| invalid("exact-vector store has no dimension"))?;
        if query.len() != dim {
            return Err(invalid(format!(
                "query dim {} does not match exact-vector dim {dim}",
                query.len()
            )));
        }
        if let Some((coordinate, value)) = query
            .iter()
            .copied()
            .enumerate()
            .find(|(_, v)| !v.is_finite())
        {
            return Err(invalid(format!(
                "query coordinate {coordinate} is not finite: {value}"
            )));
        }
        let row_bytes = dim
            .checked_mul(std::mem::size_of::<f32>())
            .ok_or_else(|| invalid("exact-vector row size overflow"))?;
        let mut scheduled: Vec<(usize, usize)> = slots
            .iter()
            .copied()
            .enumerate()
            .filter_map(|(ordinal, slot)| (slot < self.len()).then_some((ordinal, slot)))
            .collect();
        scheduled.sort_unstable_by_key(|&(ordinal, slot)| {
            let byte = HEADER_BYTES.saturating_add(slot.saturating_mul(row_bytes));
            (byte / PAGE_BYTES, slot, ordinal)
        });
        let tasks = rerank_task_count(scheduled.len(), parallelism);
        let scored_page_order: Vec<(usize, usize, f32)> = if tasks <= 1 {
            scheduled
                .iter()
                .map(|&(ordinal, slot)| (ordinal, slot, self.score_one(query, slot, dim)))
                .collect()
        } else {
            let chunk = scheduled.len().div_ceil(tasks);
            std::thread::scope(|scope| {
                let handles: Vec<_> = scheduled
                    .chunks(chunk)
                    .map(|part| {
                        scope.spawn(move || {
                            part.iter()
                                .map(|&(ordinal, slot)| {
                                    (ordinal, slot, self.score_one(query, slot, dim))
                                })
                                .collect::<Vec<_>>()
                        })
                    })
                    .collect();
                handles
                    .into_iter()
                    .flat_map(|handle| handle.join().expect("exact rerank worker panicked"))
                    .collect()
            })
        };
        let mut restored = scored_page_order;
        restored.sort_unstable_by_key(|&(ordinal, _, _)| ordinal);
        let rows = restored
            .into_iter()
            .map(|(_, slot, score)| (slot, score))
            .collect::<Vec<_>>();
        let pages_touched = if self.is_mapped() {
            let mut pages = std::collections::BTreeSet::new();
            for &(_, slot) in &scheduled {
                let start = HEADER_BYTES + slot * row_bytes;
                let end = start + row_bytes - 1;
                for page in start / PAGE_BYTES..=end / PAGE_BYTES {
                    pages.insert(page);
                }
            }
            pages.len() as u64
        } else {
            0
        };
        let logical_bytes = u64::try_from(rows.len())
            .ok()
            .and_then(|count| count.checked_mul(row_bytes as u64))
            .ok_or_else(|| invalid("exact-vector logical byte count overflow"))?;
        Ok(ScoredSlots {
            rows,
            logical_bytes,
            pages_touched,
            tasks: tasks as u32,
        })
    }

    fn score_one(&self, query: &[f32], slot: usize, dim: usize) -> f32 {
        match &self.storage {
            Storage::Building { values, .. } => {
                let row = &values[slot * dim..(slot + 1) * dim];
                dot(row, query)
            }
            Storage::Spilled { file, .. } => {
                let mut row = vec![0u8; dim * 4];
                // A short read here is the file vanishing under the
                // store (an operator removing the spill); zeros score
                // zero rather than tearing the query down.
                let _ = file.read_exact_at(&mut row, (HEADER_BYTES + slot * dim * 4) as u64);
                dot_bytes(&row, query)
            }
            Storage::Mapped { map, .. } => dot_mapped(
                &map[HEADER_BYTES + slot * dim * 4..HEADER_BYTES + (slot + 1) * dim * 4],
                query,
            ),
        }
    }

    /// The FP32 rows `[from, to)`, flat, for sealing a segment
    /// (`docs/immutable-segments.md`); empty when the store has no
    /// dimension yet.
    pub fn row_values(&self, from: usize, to: usize) -> Vec<f32> {
        let Some(dim) = self.dim() else {
            return Vec::new();
        };
        let (from, to) = (from.min(self.len()), to.min(self.len()));
        if from >= to {
            return Vec::new();
        }
        match &self.storage {
            Storage::Building { values, .. } => values[from * dim..to * dim].to_vec(),
            Storage::Spilled { file, .. } => {
                let mut bytes = vec![0u8; (to - from) * dim * 4];
                let _ = file.read_exact_at(&mut bytes, (HEADER_BYTES + from * dim * 4) as u64);
                bytes
                    .as_chunks::<4>()
                    .0
                    .iter()
                    .map(|b| f32::from_le_bytes(*b))
                    .collect()
            }
            Storage::Mapped { map, .. } => map
                [HEADER_BYTES + from * dim * 4..HEADER_BYTES + to * dim * 4]
                .as_chunks::<4>()
                .0
                .iter()
                .map(|bytes| f32::from_le_bytes(*bytes))
                .collect(),
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The disk builder appends straight to its file, reads rows back
    /// bit for bit, scores like the heap builder, finalizes in place
    /// (one header write, one rename) with the payload digest it kept
    /// while appending, and leaves nothing behind when dropped
    /// unfinalized.
    #[test]
    fn spilled_builder_matches_the_heap_builder_and_finalizes_in_place() {
        let dir = std::env::temp_dir().join(format!("pm-exact-spill-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let target = dir.join("gen").join("vectors.f32");
        let dim = 5;
        let rows: Vec<f32> = (0..dim * 37).map(|i| (i as f32 * 0.37).sin()).collect();

        let mut heap = ExactVectorStore::empty(Some(dim));
        let mut spilled = ExactVectorStore::spilling(&target, None).unwrap();
        assert!(spilled.is_spilled());
        assert_eq!(spilled.len(), 0);
        for batch in rows.chunks(dim * 7) {
            heap.append(batch, dim).unwrap();
            spilled.append(batch, dim).unwrap();
        }
        assert_eq!(spilled.len(), 37);
        assert_eq!(spilled.dim(), Some(dim));
        assert_eq!(spilled.row_values(3, 11), heap.row_values(3, 11));
        assert_eq!(spilled.row_values(0, 37), rows);
        let query: Vec<f32> = (0..dim).map(|i| 0.5 - i as f32 * 0.1).collect();
        let slots = [36usize, 0, 17, 5];
        assert_eq!(
            spilled.score_slots(&query, &slots).unwrap(),
            heap.score_slots(&query, &slots).unwrap()
        );
        let spill_file = spill_path(&target);
        assert!(spill_file.exists());
        assert!(!target.exists());

        let mapped = spilled.write(&target).unwrap();
        assert!(mapped.is_mapped());
        assert!(target.exists(), "finalized in place");
        assert!(!spill_file.exists(), "renamed onto the target");
        mapped.verify_payload().unwrap();
        assert_eq!(mapped.row_values(0, 37), rows);
        let expected = heap.write(&dir.join("heap.f32")).unwrap();
        assert_eq!(
            std::fs::read(&target).unwrap(),
            std::fs::read(dir.join("heap.f32")).unwrap(),
            "the same bytes either way"
        );
        assert_eq!(expected.len(), mapped.len());
        drop(spilled);
        assert!(
            target.exists(),
            "dropping a finalized builder keeps the store"
        );

        // Appending to the mapped store builds on disk again beside it.
        let mut reopened = ExactVectorStore::open(&target).unwrap();
        reopened.append(&rows[..dim], dim).unwrap();
        assert!(reopened.is_spilled());
        assert_eq!(reopened.len(), 38);
        assert_eq!(reopened.row_values(37, 38), &rows[..dim]);
        assert_eq!(reopened.row_values(0, 37), rows);
        let grown = reopened.write(&target).unwrap();
        assert_eq!(grown.len(), 38);
        grown.verify_payload().unwrap();

        // Writing a spilled store to ANOTHER path copies it there and
        // leaves the builder in place.
        let mut other = ExactVectorStore::spilling(&dir.join("other.f32"), Some(dim)).unwrap();
        other.append(&rows, dim).unwrap();
        let elsewhere = other.write(&dir.join("copy.f32")).unwrap();
        elsewhere.verify_payload().unwrap();
        assert_eq!(elsewhere.row_values(0, 37), rows);
        assert!(spill_path(&dir.join("other.f32")).exists());
        drop(other);
        assert!(
            !spill_path(&dir.join("other.f32")).exists(),
            "an unfinalized builder removes its file"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn round_trip_is_mapped_and_scores_fp32() {
        let dir = std::env::temp_dir().join(format!(
            "protomolt-exact-vectors-{}-{}",
            std::process::id(),
            TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("vectors.exact");
        let store = ExactVectorStore::from_values(3, vec![1.0, 2.0, 3.0, -1.0, 0.5, 2.0])
            .unwrap()
            .write(&path)
            .unwrap();
        assert!(store.is_mapped());
        assert_eq!(store.dim(), Some(3));
        assert_eq!(store.len(), 2);
        store.verify_payload().unwrap();
        assert_eq!(
            store.score_slots(&[0.5, 1.0, -1.0], &[1, 0]).unwrap(),
            vec![(1, -2.0), (0, -0.5)]
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn header_and_payload_corruption_are_distinct() {
        let dir = std::env::temp_dir().join(format!(
            "protomolt-exact-vectors-corrupt-{}-{}",
            std::process::id(),
            TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("vectors.exact");
        ExactVectorStore::from_values(2, vec![1.0, 2.0])
            .unwrap()
            .write(&path)
            .unwrap();

        let mut bytes = std::fs::read(&path).unwrap();
        bytes[HEADER_BYTES] ^= 1;
        std::fs::write(&path, &bytes).unwrap();
        let store = ExactVectorStore::open(&path).unwrap();
        assert!(store.verify_payload().is_err());

        bytes[16] ^= 1;
        std::fs::write(&path, &bytes).unwrap();
        assert!(ExactVectorStore::open(&path).is_err());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn page_ordered_parallel_scoring_is_bit_identical_and_restores_order() {
        let dir = std::env::temp_dir().join(format!(
            "protomolt-exact-vectors-parallel-{}-{}",
            std::process::id(),
            TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("vectors.exact");
        let dim = 32usize;
        let rows = 1_024usize;
        let values: Vec<f32> = (0..rows * dim)
            .map(|i| ((i * 17 % 251) as f32 - 125.0) / 127.0)
            .collect();
        let query: Vec<f32> = (0..dim).map(|i| (i as f32 - 15.0) / 31.0).collect();
        let store = ExactVectorStore::from_values(dim, values)
            .unwrap()
            .write(&path)
            .unwrap();
        let slots: Vec<usize> = (0..rows).rev().step_by(3).collect();
        let serial = store.score_slots_profiled(&query, &slots, 1).unwrap();
        let parallel = store.score_slots_profiled(&query, &slots, 4).unwrap();
        assert_eq!(parallel.rows, serial.rows);
        assert_eq!(
            parallel.rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            slots
        );
        assert_eq!(parallel.logical_bytes, (slots.len() * dim * 4) as u64);
        assert!(parallel.pages_touched > 1);
        assert!(parallel.tasks > 1 && parallel.tasks <= 4);
        std::fs::remove_dir_all(dir).unwrap();
    }
}

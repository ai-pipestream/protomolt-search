//! Product-owned FP32 vectors for candidate reranking.
//!
//! The vector provider owns its native index and score. This sidecar keeps the
//! original row-major vectors in the product generation so a public dense
//! query can select candidates with that provider and rescore the fixed pool
//! with an ordinary FP32 dot product. Persisted stores are memory-mapped; a
//! fresh or appended store remains a heap builder until [`ExactVectorStore::write`]
//! atomically replaces the file and reopens it.

use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

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
            Storage::Mapped { dim, .. } => Some(*dim),
        }
    }

    pub fn len(&self) -> usize {
        match &self.storage {
            Storage::Building { dim, values } => dim.map_or(0, |d| values.len() / d),
            Storage::Mapped { rows, .. } => *rows,
        }
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
            Storage::Building { .. } => None,
        }
    }

    /// Append complete rows. A mapped store is materialized into the builder
    /// only when a real append arrives; read-only serving stays mmap-backed.
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
        if matches!(self.storage, Storage::Mapped { .. }) {
            let values = self.decode_all();
            self.storage = Storage::Building {
                dim: Some(dim),
                values,
            };
        }
        let Storage::Building { dim: known, values } = &mut self.storage else {
            unreachable!("mapped store converted above")
        };
        *known = Some(dim);
        values.extend_from_slice(vectors);
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
        let mut scored = Vec::with_capacity(slots.len());
        for &slot in slots {
            if slot >= self.len() {
                continue;
            }
            let score = match &self.storage {
                Storage::Building { values, .. } => {
                    let row = &values[slot * dim..(slot + 1) * dim];
                    dot(row, query)
                }
                Storage::Mapped { map, .. } => dot_mapped(
                    &map[HEADER_BYTES + slot * dim * 4..HEADER_BYTES + (slot + 1) * dim * 4],
                    query,
                ),
            };
            scored.push((slot, score));
        }
        Ok(scored)
    }

    fn decode_all(&self) -> Vec<f32> {
        match &self.storage {
            Storage::Building { values, .. } => values.clone(),
            Storage::Mapped { map, .. } => map[HEADER_BYTES..]
                .chunks_exact(4)
                .map(|bytes| f32::from_le_bytes(bytes.try_into().expect("four-byte chunk")))
                .collect(),
        }
    }
}

fn dot(row: &[f32], query: &[f32]) -> f32 {
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
    row.chunks_exact(4)
        .zip(query)
        .map(|(bytes, q)| f32::from_le_bytes(bytes.try_into().expect("four-byte chunk")) * q)
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
}

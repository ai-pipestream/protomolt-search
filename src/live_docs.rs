//! Generation overlay for removals and append-then-replace commits.
//!
//! Provider vectors, postings, columns, and exact rows remain immutable. A
//! compact tombstone bitmap is shared by every read path and persisted beside
//! those artifacts. Offline WAL compaction rewrites a dense generation and
//! drops the deleted rows.

use std::fs::OpenOptions;
use std::io::{self, BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

const MAGIC: &[u8; 8] = b"PMLIVE01";
const VERSION: u32 = 1;
const HEADER_BYTES: usize = 80;
const HASH_START: usize = 40;
const HASH_END: usize = 72;
const HEADER_CRC_START: usize = 72;
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, Default)]
pub struct LiveDocs {
    deleted: Arc<Vec<u64>>,
    revision: u64,
    persisted_rows: u64,
}

impl LiveDocs {
    pub fn open(path: &Path) -> io::Result<Self> {
        let bytes = std::fs::read(path)?;
        if bytes.len() < HEADER_BYTES || &bytes[..8] != MAGIC {
            return Err(invalid(format!(
                "{} has invalid live-doc header",
                path.display()
            )));
        }
        if read_u32(&bytes, 8) != VERSION || read_u32(&bytes, 12) as usize != HEADER_BYTES {
            return Err(invalid(format!(
                "{} has unsupported live-doc version",
                path.display()
            )));
        }
        let expected_crc = read_u32(&bytes, HEADER_CRC_START);
        if crate::wal::crc32(&bytes[..HEADER_CRC_START]) != expected_crc {
            return Err(invalid(format!(
                "{} live-doc header CRC mismatch",
                path.display()
            )));
        }
        let rows = read_u64(&bytes, 16);
        let revision = read_u64(&bytes, 24);
        let words = usize::try_from(read_u64(&bytes, 32))
            .map_err(|_| invalid("live-doc word count does not fit this platform"))?;
        let expected_len = HEADER_BYTES
            .checked_add(
                words
                    .checked_mul(8)
                    .ok_or_else(|| invalid("live-doc size overflow"))?,
            )
            .ok_or_else(|| invalid("live-doc size overflow"))?;
        let platform_rows = usize::try_from(rows)
            .map_err(|_| invalid("live-doc row count does not fit this platform"))?;
        if bytes.len() != expected_len || words != platform_rows.div_ceil(64) {
            return Err(invalid(format!(
                "{} live-doc shape is inconsistent",
                path.display()
            )));
        }
        let payload = &bytes[HEADER_BYTES..];
        let mut expected_hash = [0u8; 32];
        expected_hash.copy_from_slice(&bytes[HASH_START..HASH_END]);
        if crate::sha256::digest(payload) != expected_hash {
            return Err(invalid(format!(
                "{} live-doc payload SHA-256 mismatch",
                path.display()
            )));
        }
        let deleted = payload
            .as_chunks::<8>()
            .0
            .iter()
            .map(|word| u64::from_le_bytes(*word))
            .collect::<Vec<_>>();
        if !rows.is_multiple_of(64) && deleted.last().is_some_and(|word| *word >> (rows % 64) != 0)
        {
            return Err(invalid(format!(
                "{} sets tombstone bits beyond row count",
                path.display()
            )));
        }
        Ok(Self {
            deleted: Arc::new(deleted),
            revision,
            persisted_rows: rows,
        })
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub fn persisted_rows(&self) -> u64 {
        self.persisted_rows
    }

    pub fn deleted_count(&self) -> u64 {
        self.deleted
            .iter()
            .map(|word| u64::from(word.count_ones()))
            .sum()
    }

    pub fn is_deleted(&self, slot: usize) -> bool {
        self.deleted
            .get(slot / 64)
            .is_some_and(|word| word & (1u64 << (slot % 64)) != 0)
    }

    pub fn has_deletes(&self) -> bool {
        self.deleted.iter().any(|word| *word != 0)
    }

    pub fn words(&self) -> Option<Arc<Vec<u64>>> {
        self.has_deletes().then(|| Arc::clone(&self.deleted))
    }

    /// Idempotently tombstone one local slot. Returns true only on the first
    /// transition from live to deleted.
    pub fn delete(&mut self, slot: usize) -> bool {
        let words = Arc::make_mut(&mut self.deleted);
        words.resize((slot + 1).div_ceil(64), 0);
        let mask = 1u64 << (slot % 64);
        let word = &mut words[slot / 64];
        if *word & mask != 0 {
            return false;
        }
        *word |= mask;
        self.revision = self.revision.saturating_add(1);
        true
    }

    pub fn write(&self, path: &Path, rows: u64) -> io::Result<Self> {
        if self.persisted_rows > rows {
            return Err(invalid(format!(
                "live-doc overlay was persisted for {} rows but generation now has only {rows}",
                self.persisted_rows
            )));
        }
        let words_len = usize::try_from(rows)
            .map_err(|_| invalid("live-doc row count does not fit this platform"))?
            .div_ceil(64);
        let mut words = self.deleted.as_ref().clone();
        words.resize(words_len, 0);
        if !rows.is_multiple_of(64) {
            if let Some(last) = words.last_mut() {
                *last &= (1u64 << (rows % 64)) - 1;
            }
        }
        let mut payload = Vec::with_capacity(words.len() * 8);
        for word in &words {
            payload.extend_from_slice(&word.to_le_bytes());
        }
        let hash = crate::sha256::digest(&payload);
        let mut header = [0u8; HEADER_BYTES];
        header[..8].copy_from_slice(MAGIC);
        header[8..12].copy_from_slice(&VERSION.to_le_bytes());
        header[12..16].copy_from_slice(&(HEADER_BYTES as u32).to_le_bytes());
        header[16..24].copy_from_slice(&rows.to_le_bytes());
        header[24..32].copy_from_slice(&self.revision.to_le_bytes());
        header[32..40].copy_from_slice(&(words.len() as u64).to_le_bytes());
        header[HASH_START..HASH_END].copy_from_slice(&hash);
        let crc = crate::wal::crc32(&header[..HEADER_CRC_START]);
        header[HEADER_CRC_START..HEADER_CRC_START + 4].copy_from_slice(&crc.to_le_bytes());

        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        std::fs::create_dir_all(parent)?;
        let tmp = unique_temp_path(path);
        let result = (|| -> io::Result<()> {
            let file = OpenOptions::new().create_new(true).write(true).open(&tmp)?;
            let mut out = BufWriter::new(file);
            out.write_all(&header)?;
            out.write_all(&payload)?;
            out.flush()?;
            let mut file = out
                .into_inner()
                .map_err(|error| io::Error::new(error.error().kind(), error.to_string()))?;
            file.seek(SeekFrom::End(0))?;
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
    fn round_trip_and_idempotent_delete() {
        let dir = std::env::temp_dir().join(format!(
            "protomolt-live-docs-{}-{}",
            std::process::id(),
            TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("rows.live");
        let mut live = LiveDocs::default();
        assert!(live.delete(1));
        assert!(!live.delete(1));
        assert!(live.delete(129));
        let reopened = live.write(&path, 130).unwrap();
        assert_eq!(reopened.deleted_count(), 2);
        assert!(reopened.is_deleted(1));
        assert!(reopened.is_deleted(129));
        assert!(!reopened.is_deleted(128));
        std::fs::remove_dir_all(dir).unwrap();
    }
}

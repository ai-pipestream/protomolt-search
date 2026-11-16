//! The generation sidecar file: a header, one length-prefixed record per
//! retained row, and a SHA-256 footer over both. Opening verifies the
//! artifact against the digest its parent metadata recorded; writing
//! publishes through an owned temporary file and a rename.

use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

use prost::encoding::decode_key;
use prost::Message;

use super::codec::{
    delimited, embedding, materialized, scan_embedding, skip, validate_retained, Row,
};
use super::{invalid, Limits, MultiResVectors, LIMITS, MAGIC, MAX_RECORD_BYTES, NEXT_TEMP};
use turbo_sha256::Sha256;

const HEADER_LEN: usize = 24;

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

struct OwnedTemp(Option<PathBuf>);
impl Drop for OwnedTemp {
    fn drop(&mut self) {
        if let Some(path) = &self.0 {
            let _ = std::fs::remove_file(path);
        }
    }
}

fn is_lower_hex_sha256(digest: &str) -> bool {
    digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// Hash the whole artifact through the open descriptor, refuse a digest
/// other than `expected_sha256`, and rewind for parsing.
fn verify_artifact_digest(file: &mut File, expected_sha256: &str) -> io::Result<()> {
    let mut hash = Sha256::new();
    let mut buffer = [0; 64 * 1024];
    loop {
        let size = file.read(&mut buffer)?;
        if size == 0 {
            break;
        }
        hash.update(&buffer[..size]);
    }
    if hex(&hash.finish().0) != expected_sha256 {
        return Err(invalid("multi-resolution artifact digest mismatch"));
    }
    file.seek(SeekFrom::Start(0))?;
    Ok(())
}

/// The header bytes, the physical row span, and the record count.
fn read_header(
    reader: &mut impl Read,
    physical_rows: u64,
) -> io::Result<([u8; HEADER_LEN], u64, u64)> {
    let mut header = [0u8; HEADER_LEN];
    reader.read_exact(&mut header)?;
    if &header[..8] != MAGIC {
        return Err(invalid("unrecognized multi-resolution vector format"));
    }
    let span = u64::from_le_bytes(header[8..16].try_into().unwrap());
    let count = u64::from_le_bytes(header[16..].try_into().unwrap());
    if span != physical_rows || count > span || span > u64::from(u32::MAX) + 1 {
        return Err(invalid("multi-resolution vector row count mismatch"));
    }
    Ok((header, span, count))
}

/// One length-prefixed record, folded into `digest` as it is read.
fn read_record(reader: &mut impl Read, digest: &mut Sha256) -> io::Result<Vec<u8>> {
    let mut size = [0u8; 4];
    reader.read_exact(&mut size)?;
    digest.update(&size);
    let size = u32::from_le_bytes(size) as usize;
    if size > MAX_RECORD_BYTES {
        return Err(invalid("multi-resolution vector record exceeds size bound"));
    }
    let mut bytes = vec![0; size];
    reader.read_exact(&mut bytes)?;
    digest.update(&bytes);
    Ok(bytes)
}

/// The embedding payload of a `Row` record, found without allocating
/// its nested message.
fn embedding_payload(record: &[u8]) -> io::Result<&[u8]> {
    let mut fields = record;
    let mut payload = None;
    while !fields.is_empty() {
        let (tag, wire) = decode_key(&mut fields).map_err(|e| invalid(e.to_string()))?;
        if tag == 2 {
            if payload.is_some() {
                return Err(invalid("duplicate multi-resolution embedding"));
            }
            payload = Some(delimited(wire, &mut fields)?);
        } else {
            skip(wire, tag, &mut fields)?;
        }
    }
    payload.ok_or_else(|| invalid("missing vector row"))
}

impl MultiResVectors {
    pub(crate) fn open(path: &Path, physical_rows: u64, expected_sha256: &str) -> io::Result<Self> {
        if !is_lower_hex_sha256(expected_sha256) {
            return Err(invalid("invalid expected multi-resolution artifact digest"));
        }
        // Verify the authoritative artifact before decoding any owned rows.
        // Keep this descriptor open, and hash again during parsing so neither
        // path replacement nor in-place modification can bypass the binding.
        let mut file = File::open(path)?;
        verify_artifact_digest(&mut file, expected_sha256)?;
        let mut reader = BufReader::new(file);
        let (header, span, count) = read_header(&mut reader, physical_rows)?;
        let mut digest = Sha256::new();
        digest.update(&header);
        let mut result = Self::default();
        let mut previous = None;
        for _ in 0..count {
            let bytes = read_record(&mut reader, &mut digest)?;
            scan_embedding(embedding_payload(&bytes)?, LIMITS)?;
            let row = Row::decode(bytes.as_slice()).map_err(|e| invalid(e.to_string()))?;
            if u64::from(row.slot) >= span || previous.is_some_and(|slot| slot >= row.slot) {
                return Err(invalid(
                    "unordered or out-of-range multi-resolution vector slot",
                ));
            }
            previous = Some(row.slot);
            let value = materialized(row.embedding.ok_or_else(|| invalid("missing vector row"))?)?;
            result.insert(row.slot, value);
        }
        let mut stored_digest = [0; 32];
        reader.read_exact(&mut stored_digest)?;
        let expected_footer = digest.clone().finish().0;
        digest.update(&stored_digest);
        if expected_footer != stored_digest
            || hex(&digest.finish().0) != expected_sha256
            || reader.read(&mut [0])? != 0
        {
            return Err(invalid("multi-resolution vector file integrity failure"));
        }
        Ok(result)
    }

    /// Publish into an existing durable directory. A successful result binds
    /// the entire file, including its integrity footer, for parent metadata.
    pub(crate) fn write(&self, path: &Path, physical_rows: u64) -> io::Result<(u64, String)> {
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let tmp = parent.join(format!(
            ".multi-res-{}-{}.tmp",
            std::process::id(),
            NEXT_TEMP.fetch_add(1, Ordering::Relaxed)
        ));
        self.write_at_temp(path, physical_rows, &tmp, LIMITS, |directory| {
            directory.sync_all()
        })
    }

    pub(super) fn write_at_temp(
        &self,
        path: &Path,
        physical_rows: u64,
        tmp: &Path,
        limits: Limits,
        sync_parent: impl FnOnce(&File) -> io::Result<()>,
    ) -> io::Result<(u64, String)> {
        self.validate_rows(physical_rows)?;
        // Complete admission validation before creating a file or cloning rows.
        for row in self.rows.values() {
            validate_retained(row, limits)?;
        }
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let directory = File::open(parent)?;
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(tmp)?;
        // The cleanup guard exists only after create_new proved ownership.
        let mut owned = OwnedTemp(Some(tmp.to_path_buf()));
        let mut writer = BufWriter::new(file);
        let mut digest = Sha256::new();
        let mut header = Vec::with_capacity(HEADER_LEN);
        header.extend_from_slice(MAGIC);
        header.extend_from_slice(&physical_rows.to_le_bytes());
        header.extend_from_slice(&(self.rows.len() as u64).to_le_bytes());
        writer.write_all(&header)?;
        digest.update(&header);
        for (slot, value) in &self.rows {
            let row = Row {
                slot: *slot,
                embedding: Some(embedding(value)),
            };
            let bytes = row.encode_to_vec();
            debug_assert!(bytes.len() <= limits.record);
            let size = (bytes.len() as u32).to_le_bytes();
            writer.write_all(&size)?;
            writer.write_all(&bytes)?;
            digest.update(&size);
            digest.update(&bytes);
        }
        let footer = digest.clone().finish().0;
        writer.write_all(&footer)?;
        digest.update(&footer);
        writer.flush()?;
        writer.get_ref().sync_all()?;
        let bytes = writer.get_ref().metadata()?.len();
        drop(writer);
        std::fs::rename(tmp, path)?;
        owned.0 = None; // The rename gave the temporary name away.
        sync_parent(&directory).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("multi-resolution publication uncertain after rename: {error}"),
            )
        })?;
        Ok((bytes, hex(&digest.finish().0)))
    }
}

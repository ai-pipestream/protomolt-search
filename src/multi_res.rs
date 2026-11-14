//! Generation-owned multi-resolution vector rows.
//!
//! Store the already-materialized values, including their original float bits.
//! Recovery must neither run an embedder nor normalize an already-normalized
//! vector again. The sidecar has one bounded protobuf record per retained row,
//! with a SHA-256 digest covering the header and all records.

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use prost::encoding::{
    decode_key, decode_varint, encoded_len_varint, skip_field, DecodeContext, WireType,
};
use prost::Message;

use crate::sha256::Sha256;
use crate::turbo_bridge::MultiResVectorOutput;

const MAGIC: &[u8; 8] = b"PMMRES01";
const MAX_RECORD_BYTES: usize = 64 * 1024 * 1024;
// Bound additional decoded allocations per record, not the retained generation.
// Four times each vector payload covers Vec's geometric/minimum capacity;
// message vectors also reserve their temporary and materialized descriptors.
const MAX_DECODED_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Copy)]
struct Limits {
    record: usize,
    decoded: usize,
}
const LIMITS: Limits = Limits {
    record: MAX_RECORD_BYTES,
    decoded: MAX_DECODED_BYTES,
};
static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

pub(crate) const FILE_NAME: &str = "vectors.multi";

pub(crate) fn sidecar_path(index_path: &Path) -> PathBuf {
    let mut path = index_path.as_os_str().to_owned();
    path.push(".multi");
    PathBuf::from(path)
}

pub(crate) fn storage_path(index_path: &Path, generation: Option<&PathBuf>) -> PathBuf {
    generation.map_or_else(|| sidecar_path(index_path), |dir| dir.join(FILE_NAME))
}

#[derive(Clone, Default)]
pub(crate) struct MultiResVectors {
    // Immutable row payloads are shared by sealed snapshots, frozen ranges and
    // the serving map; copying a view must not duplicate the corpus floats.
    rows: BTreeMap<u32, Arc<RetainedVector>>,
}

#[derive(Clone)]
pub(crate) struct RetainedVector {
    pub(crate) output: MultiResVectorOutput,
}

#[derive(Clone, PartialEq, Message)]
struct Tier {
    #[prost(uint32, tag = "1")]
    dim: u32,
    #[prost(float, repeated, packed = "true", tag = "2")]
    values: Vec<f32>,
}

#[derive(Clone, PartialEq, Message)]
struct Embedding {
    #[prost(float, repeated, packed = "true", tag = "1")]
    full: Vec<f32>,
    #[prost(message, repeated, tag = "2")]
    prefixes: Vec<Tier>,
    #[prost(float, optional, tag = "3")]
    inv_norm: Option<f32>,
    #[prost(bytes = "vec", optional, tag = "4")]
    quantized_codes: Option<Vec<u8>>,
    #[prost(message, repeated, tag = "5")]
    sentences: Vec<Tier>,
    #[prost(bytes = "vec", tag = "6")]
    sha256: Vec<u8>,
    // Tag 7 is reserved; replay carries materialized tiers, never raw inputs.
}

#[derive(Clone, PartialEq, Message)]
struct Row {
    #[prost(uint32, tag = "1")]
    slot: u32,
    #[prost(message, optional, tag = "2")]
    embedding: Option<Embedding>,
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn embedding(retained: &RetainedVector) -> Embedding {
    let row = &retained.output;
    Embedding {
        full: row.full.clone(),
        prefixes: row
            .prefixes
            .iter()
            .map(|(dim, values)| Tier {
                dim: *dim,
                values: values.clone(),
            })
            .collect(),
        inv_norm: Some(row.inv_norm),
        quantized_codes: row.quantized_codes.clone(),
        sentences: row
            .sentence_vectors
            .iter()
            .map(|values| Tier {
                dim: values.len() as u32,
                values: values.clone(),
            })
            .collect(),
        sha256: row.sha256.to_vec(),
    }
}

fn materialized(row: Embedding) -> io::Result<RetainedVector> {
    if row.full.is_empty()
        || row.full.iter().any(|v| !v.is_finite())
        || row
            .inv_norm
            .is_none_or(|norm| !norm.is_finite() || norm < 0.0)
    {
        return Err(invalid("invalid full-resolution vector or norm"));
    }
    for tier in row.prefixes.iter().chain(&row.sentences) {
        if tier.dim == 0
            || tier.dim as usize != tier.values.len()
            || tier.values.iter().any(|v| !v.is_finite())
        {
            return Err(invalid("invalid multi-resolution vector tier"));
        }
    }
    if row
        .prefixes
        .iter()
        .any(|tier| tier.values.len() > row.full.len())
    {
        return Err(invalid("vector prefix exceeds full dimension"));
    }
    let mut digest = Sha256::new();
    for value in &row.full {
        digest.update(&value.to_le_bytes());
    }
    let sha256 = digest.finalize();
    if row.sha256.as_slice() != sha256 {
        return Err(invalid("multi-resolution vector digest mismatch"));
    }
    Ok(RetainedVector {
        output: MultiResVectorOutput {
            full: row.full,
            prefixes: row
                .prefixes
                .into_iter()
                .map(|tier| (tier.dim, tier.values))
                .collect(),
            inv_norm: row.inv_norm.expect("validated norm presence"),
            quantized_codes: row.quantized_codes,
            sentence_vectors: row.sentences.into_iter().map(|tier| tier.values).collect(),
            sha256,
        },
    })
}

// All tags in this private codec fit in one key byte. Reserve the largest
// slot varint even for WAL payloads, whose slot is carried by their envelope.
fn envelope_size(payload: usize) -> io::Result<usize> {
    payload
        .checked_add(1 + 5 + 1 + encoded_len_varint(payload as u64))
        .ok_or_else(|| invalid("multi-resolution vector size overflow"))
}

fn bounded_payload(payload: usize, limits: Limits) -> io::Result<()> {
    if envelope_size(payload)? > limits.record {
        return Err(invalid("multi-resolution vector record exceeds size bound"));
    }
    Ok(())
}

fn delimited_size(bytes: usize) -> usize {
    1 + encoded_len_varint(bytes as u64) + bytes
}

struct Budget {
    used: usize,
    limit: usize,
}
impl Budget {
    fn new(limit: usize) -> Self {
        Self {
            used: std::mem::size_of::<Embedding>() + std::mem::size_of::<RetainedVector>(),
            limit,
        }
    }
    fn add(&mut self, bytes: usize) -> io::Result<()> {
        self.used = self
            .used
            .checked_add(bytes)
            .ok_or_else(|| invalid("multi-resolution allocation overflow"))?;
        if self.used > self.limit {
            return Err(invalid(
                "multi-resolution decoded allocation exceeds size bound",
            ));
        }
        Ok(())
    }
    fn vector(&mut self, bytes: usize) -> io::Result<()> {
        self.add(
            bytes
                .checked_mul(4)
                .ok_or_else(|| invalid("multi-resolution allocation overflow"))?,
        )
    }
    fn tier(&mut self, prefix: bool) -> io::Result<()> {
        self.add(
            4 * std::mem::size_of::<Tier>()
                + if prefix {
                    std::mem::size_of::<(u32, Vec<f32>)>()
                } else {
                    std::mem::size_of::<Vec<f32>>()
                },
        )
    }
}

fn finite(values: &[f32]) -> io::Result<()> {
    if values.iter().any(|v| !v.is_finite()) {
        return Err(invalid("nonfinite multi-resolution vector value"));
    }
    Ok(())
}

/// Borrow first: rejection cannot clone an oversized input or encode a row
/// which the sidecar's enclosing Row would subsequently reject.
fn validate_retained(row: &RetainedVector, limits: Limits) -> io::Result<usize> {
    let out = &row.output;
    if out.full.is_empty() || !out.inv_norm.is_finite() || out.inv_norm < 0.0 {
        return Err(invalid("invalid full-resolution vector or norm"));
    }
    let mut budget = Budget::new(limits.decoded);
    let mut size = 5 + delimited_size(32); // present inv_norm, full-vector digest
    budget.vector(32)?;
    {
        let bytes = out
            .full
            .len()
            .checked_mul(4)
            .ok_or_else(|| invalid("multi-resolution vector size overflow"))?;
        budget.vector(bytes)?;
        size = size
            .checked_add(delimited_size(bytes))
            .ok_or_else(|| invalid("multi-resolution vector size overflow"))?;
    }
    for (prefix, dim, values) in out
        .prefixes
        .iter()
        .map(|(dim, v)| (true, *dim as usize, v))
        .chain(out.sentence_vectors.iter().map(|v| (false, v.len(), v)))
    {
        if dim == 0
            || dim != values.len()
            || dim > u32::MAX as usize
            || (prefix && dim > out.full.len())
        {
            return Err(invalid("invalid multi-resolution vector tier"));
        }
        budget.tier(prefix)?;
        let bytes = values
            .len()
            .checked_mul(4)
            .ok_or_else(|| invalid("multi-resolution vector size overflow"))?;
        budget.vector(bytes)?;
        let tier_size = 1 + encoded_len_varint(dim as u64) + delimited_size(bytes);
        size = size
            .checked_add(delimited_size(tier_size))
            .ok_or_else(|| invalid("multi-resolution vector size overflow"))?;
    }
    if let Some(codes) = &out.quantized_codes {
        budget.vector(codes.len())?;
        size = size
            .checked_add(delimited_size(codes.len()))
            .ok_or_else(|| invalid("multi-resolution vector size overflow"))?;
    }
    bounded_payload(size, limits)?;
    finite(&out.full)?;
    for values in out
        .prefixes
        .iter()
        .map(|(_, v)| v)
        .chain(&out.sentence_vectors)
    {
        finite(values)?;
    }
    let mut digest = Sha256::new();
    for value in &out.full {
        digest.update(&value.to_le_bytes());
    }
    if digest.finalize() != out.sha256 {
        return Err(invalid("multi-resolution vector digest mismatch"));
    }
    Ok(size)
}

fn delimited<'a>(wire: WireType, bytes: &mut &'a [u8]) -> io::Result<&'a [u8]> {
    if wire != WireType::LengthDelimited {
        return Err(invalid("invalid multi-resolution protobuf wire type"));
    }
    let len = decode_varint(bytes).map_err(|e| invalid(e.to_string()))?;
    let len = usize::try_from(len).map_err(|_| invalid("multi-resolution vector size overflow"))?;
    if len > bytes.len() {
        return Err(invalid("truncated multi-resolution protobuf field"));
    }
    let (field, rest) = bytes.split_at(len);
    *bytes = rest;
    Ok(field)
}

fn floats(wire: WireType, bytes: &mut &[u8], budget: &mut Budget) -> io::Result<usize> {
    let size = if wire == WireType::ThirtyTwoBit {
        if bytes.len() < 4 {
            return Err(invalid("truncated multi-resolution float"));
        }
        *bytes = &bytes[4..];
        4
    } else {
        delimited(wire, bytes)?.len()
    };
    if !size.is_multiple_of(4) {
        return Err(invalid("invalid packed multi-resolution float length"));
    }
    budget.vector(size)?;
    Ok(size / 4)
}

fn skip(wire: WireType, tag: u32, bytes: &mut &[u8]) -> io::Result<()> {
    skip_field(wire, tag, bytes, DecodeContext::default()).map_err(|e| invalid(e.to_string()))
}

fn scan_tier(mut bytes: &[u8], budget: &mut Budget) -> io::Result<usize> {
    let mut dim = 0;
    let mut values = 0usize;
    while !bytes.is_empty() {
        let (tag, wire) = decode_key(&mut bytes).map_err(|e| invalid(e.to_string()))?;
        match tag {
            1 if wire == WireType::Varint => {
                dim = decode_varint(&mut bytes).map_err(|e| invalid(e.to_string()))?;
            }
            2 => {
                values = values
                    .checked_add(floats(wire, &mut bytes, budget)?)
                    .ok_or_else(|| invalid("multi-resolution vector size overflow"))?;
            }
            _ => skip(wire, tag, &mut bytes)?,
        }
    }
    if dim == 0 || dim > u32::MAX as u64 || dim != values as u64 {
        return Err(invalid("invalid multi-resolution vector tier"));
    }
    Ok(values)
}

/// Allocation-free inspection precedes prost's owned message construction.
/// It accounts for repeated packed/unpacked fields and message descriptors,
/// including fields that a later scalar occurrence would overwrite.
fn scan_embedding(mut bytes: &[u8], limits: Limits) -> io::Result<()> {
    bounded_payload(bytes.len(), limits)?;
    let mut budget = Budget::new(limits.decoded);
    let mut full = 0usize;
    let mut largest_prefix = 0usize;
    let mut norm = false;
    while !bytes.is_empty() {
        let (tag, wire) = decode_key(&mut bytes).map_err(|e| invalid(e.to_string()))?;
        match tag {
            1 => {
                full = full
                    .checked_add(floats(wire, &mut bytes, &mut budget)?)
                    .ok_or_else(|| invalid("multi-resolution vector size overflow"))?;
            }
            2 | 5 => {
                budget.tier(tag == 2)?;
                let count = scan_tier(delimited(wire, &mut bytes)?, &mut budget)?;
                if tag == 2 {
                    largest_prefix = largest_prefix.max(count);
                }
            }
            3 if wire == WireType::ThirtyTwoBit => {
                if bytes.len() < 4 {
                    return Err(invalid("truncated multi-resolution norm"));
                }
                bytes = &bytes[4..];
                norm = true;
            }
            4 | 6 => {
                let field = delimited(wire, &mut bytes)?;
                budget.vector(field.len())?;
                if tag == 6 && field.len() != 32 {
                    return Err(invalid("invalid multi-resolution digest length"));
                }
            }
            _ => skip(wire, tag, &mut bytes)?,
        }
    }
    if full == 0 || largest_prefix > full || !norm {
        return Err(invalid("invalid full-resolution vector or norm"));
    }
    Ok(())
}

/// The same materialized payload travels in the WAL and generation files.
pub(crate) fn encoded_len(row: &RetainedVector) -> io::Result<usize> {
    validate_retained(row, LIMITS)
}

pub(crate) fn encode(row: &RetainedVector) -> io::Result<Vec<u8>> {
    encode_with_limits(row, LIMITS)
}
fn encode_with_limits(row: &RetainedVector, limits: Limits) -> io::Result<Vec<u8>> {
    let size = validate_retained(row, limits)?;
    let stored = embedding(row);
    debug_assert_eq!(stored.encoded_len(), size);
    Ok(stored.encode_to_vec())
}

pub(crate) fn decode(bytes: &[u8]) -> io::Result<RetainedVector> {
    scan_embedding(bytes, LIMITS)?;
    materialized(Embedding::decode(bytes).map_err(|e| invalid(e.to_string()))?)
}

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

impl MultiResVectors {
    pub(crate) fn get(&self, slot: u32) -> Option<&MultiResVectorOutput> {
        self.rows.get(&slot).map(|row| &row.output)
    }

    pub(crate) fn retained(&self, slot: u32) -> Option<&RetainedVector> {
        self.rows.get(&slot).map(Arc::as_ref)
    }

    pub(crate) fn insert(&mut self, slot: u32, row: RetainedVector) {
        self.rows.insert(slot, Arc::new(row));
    }

    pub(crate) fn row_count(&self) -> usize {
        self.rows.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Copy a positional range into a segment-local row space.
    pub(crate) fn range(&self, base: u32, rows: u32) -> io::Result<Self> {
        let end = u64::from(base) + u64::from(rows);
        if end > u64::from(u32::MAX) + 1 {
            return Err(invalid("vector row range overflow"));
        }
        Ok(Self {
            rows: self
                .rows
                .range(base..)
                .take_while(|(slot, _)| u64::from(**slot) < end)
                .map(|(slot, row)| (slot - base, row.clone()))
                .collect(),
        })
    }

    pub(crate) fn append_at(&mut self, base: u32, other: &Self) -> io::Result<()> {
        for slot in other.rows.keys() {
            let target = base
                .checked_add(*slot)
                .ok_or_else(|| invalid("vector slot overflow"))?;
            if self.rows.contains_key(&target) {
                return Err(invalid("overlapping multi-resolution vector rows"));
            }
        }
        for (slot, row) in &other.rows {
            self.rows.insert(base + slot, row.clone());
        }
        Ok(())
    }

    pub(crate) fn validate_rows(&self, physical_rows: u64) -> io::Result<()> {
        if physical_rows > u64::from(u32::MAX) + 1
            || self
                .rows
                .last_key_value()
                .is_some_and(|(slot, _)| u64::from(*slot) >= physical_rows)
        {
            return Err(invalid(
                "multi-resolution vector lies outside its generation",
            ));
        }
        Ok(())
    }

    pub(crate) fn open(path: &Path, physical_rows: u64, expected_sha256: &str) -> io::Result<Self> {
        if expected_sha256.len() != 64
            || !expected_sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(invalid("invalid expected multi-resolution artifact digest"));
        }
        // Verify the authoritative artifact before decoding any owned rows.
        // Keep this descriptor open, and hash again during parsing so neither
        // path replacement nor in-place modification can bypass the binding.
        let mut file = File::open(path)?;
        let mut hash = Sha256::new();
        let mut buffer = [0; 64 * 1024];
        loop {
            let size = file.read(&mut buffer)?;
            if size == 0 {
                break;
            }
            hash.update(&buffer[..size]);
        }
        if hex(&hash.finalize()) != expected_sha256 {
            return Err(invalid("multi-resolution artifact digest mismatch"));
        }
        file.seek(SeekFrom::Start(0))?;
        let mut reader = BufReader::new(file);
        let mut header = [0u8; 24];
        reader.read_exact(&mut header)?;
        if &header[..8] != MAGIC {
            return Err(invalid("unrecognized multi-resolution vector format"));
        }
        let span = u64::from_le_bytes(header[8..16].try_into().unwrap());
        let count = u64::from_le_bytes(header[16..].try_into().unwrap());
        if span != physical_rows || count > span || span > u64::from(u32::MAX) + 1 {
            return Err(invalid("multi-resolution vector row count mismatch"));
        }
        let mut digest = Sha256::new();
        digest.update(&header);
        let mut result = Self::default();
        let mut previous = None;
        for _ in 0..count {
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
            // Inspect the Row envelope without allocating its nested message.
            let mut fields = bytes.as_slice();
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
            scan_embedding(
                payload.ok_or_else(|| invalid("missing vector row"))?,
                LIMITS,
            )?;
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
        let expected_footer = digest.clone().finalize();
        digest.update(&stored_digest);
        if expected_footer != stored_digest
            || hex(&digest.finalize()) != expected_sha256
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

    fn write_at_temp(
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
        let mut header = Vec::with_capacity(24);
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
        let footer = digest.clone().finalize();
        writer.write_all(&footer)?;
        digest.update(&footer);
        writer.flush()?;
        writer.get_ref().sync_all()?;
        let bytes = writer.get_ref().metadata()?.len();
        drop(writer);
        std::fs::rename(tmp, path)?;
        owned.0 = None; // The temporary name is no longer ours after rename.
        sync_parent(&directory).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("multi-resolution publication uncertain after rename: {error}"),
            )
        })?;
        Ok((bytes, hex(&digest.finalize())))
    }
}

#[cfg(test)]
mod tests;

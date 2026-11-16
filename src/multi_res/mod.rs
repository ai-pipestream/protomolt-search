//! Generation-owned multi-resolution vector rows.
//!
//! Store the already-materialized values, including their original float bits.
//! Recovery must neither run an embedder nor normalize an already-normalized
//! vector again. The sidecar has one bounded protobuf record per retained row,
//! with a SHA-256 digest covering the header and all records.

use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use crate::turbo_bridge::MultiResVectorOutput;

mod artifact;
mod codec;
#[cfg(test)]
mod tests;

pub(crate) use codec::{decode, encode, encoded_len};

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

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
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
}

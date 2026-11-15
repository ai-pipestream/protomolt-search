//! Per-shard write-ahead log (WAL): bucketed folder layout, writer, and
//! reader.
//!
//! One directory per shard generation at `<index path>.wal/gen-<NNNNNN>/`
//! holding:
//!
//! - `manifest.toml` — the shard shape (dimension, provider configuration,
//!   compatibility bit width, slot offset, generation) plus
//!   `bucket_bits`/`bucket_count` and a format version. Provider state starts
//!   incomplete on a from-scratch shard and the manifest is rewritten
//!   atomically when that state locks.
//! - `bucket-<NNN>.wal` — one append-only file per hash bucket. Every
//!   vector/document record routes to `bucket_of(id, bucket_count)` — the
//!   exact partition function the reshard tool splits by, so each bucket
//!   file is a pre-partitioned log slice a cheap split can consume
//!   without re-hashing a single record.
//! - `markers.wal` — FlushMarker / SnapshotMarker / LoggedBinding records (the first two carry no
//!   id, so they get their own small file instead of fanning out to every
//!   bucket).
//! - `sources.wal` — descriptor and original-source blobs shared
//!   by the generation's document records. Readers resolve their addresses
//!   before replay or replication; Flush syncs sources before row logs.
//!
//! Frames keep the `[u32 len LE][u32 crc32 IEEE of payload LE][prost
//! bytes]` format; `seq` is 1-based and gapless PER FILE. Durability:
//! appends are buffered (no fsync per batch — the WAL is on the
//! add/flush/snapshot path, never the search path); [`WalWriter::flush`]
//! fsyncs every open file. A crash can leave a TORN TAIL frame in any
//! file; readers ignore it with a warning, and [`scan_records`] reports
//! the valid prefix so a reopening node truncates the tail before
//! appending again. Corruption INSIDE a complete frame (CRC mismatch,
//! seq gap) is a hard error with the byte offset — and is scoped to the
//! one bucket file it happened in.

mod append;
mod crc;
mod frames;
mod layout;
mod manifest;
mod sources;
mod tail;
mod writer;

#[cfg(test)]
mod tests;

pub use crc::crc32;
pub use frames::{scan_records, RecordReader, RecordScan};
pub use layout::{
    bucket_path, gen_dir, latest_gen, manifest_path, markers_path, wal_dir, FORMAT_VERSION,
};
pub use manifest::{
    check_manifest_tables, read_manifest, write_manifest, ColumnTable, WalManifest,
};
pub use tail::{read_clocked_records, truncate_records_at_or_above, ClockedTail};
pub use writer::{open_or_create, WalWriter};

//! Whole-generation reads in clock order, the incremental tail, and
//! crash-recovery truncation.

use std::fs::OpenOptions;
use std::io;
use std::path::{Path, PathBuf};

use super::frames::RecordReader;
use super::layout::{markers_path, parse_bucket_name};
use crate::pb::wal::{wal_record, WalRecord};

/// Read one fsynced generation as a single logical stream. Per-file sequence
/// validation still happens in [`RecordReader`]; the generation clock then
/// proves that merging the marker and bucket files lost, duplicated, or
/// reordered nothing.
pub fn read_clocked_records(gen_dir: &Path, after_clock: u64) -> io::Result<Vec<WalRecord>> {
    let mut paths = Vec::new();
    let markers = markers_path(gen_dir);
    if markers.exists() {
        paths.push(markers);
    }
    for entry in std::fs::read_dir(gen_dir)? {
        let entry = entry?;
        if parse_bucket_name(&entry.file_name().to_string_lossy()).is_some() {
            paths.push(entry.path());
        }
    }
    let mut records = Vec::new();
    for path in paths {
        let mut reader = RecordReader::open(&path)?;
        while let Some(record) = reader.next_record()? {
            records.push(record);
        }
    }
    records.sort_by_key(|record| record.clock);
    for (index, record) in records.iter().enumerate() {
        let expected = index as u64 + 1;
        if record.clock != expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "WAL generation clock gap: expected {expected}, got {}",
                    record.clock
                ),
            ));
        }
    }
    Ok(records
        .into_iter()
        .filter(|record| record.clock > after_clock)
        .collect())
}

/// One file's position in a [`ClockedTail`]: the byte offset of the
/// first frame not yet consumed and the seq that frame must carry.
struct TailPosition {
    path: PathBuf,
    offset: u64,
    next_seq: u64,
}

/// Incremental reader over one generation in clock order, for a tail loop
/// that must not re-read the whole log on every pass (mutation
/// tailing, compaction). Each [`Self::read_through`] scans only the frames appended
/// since the last call, across every bucket file and the markers file
/// (bucket files appear lazily, so the directory is re-listed each pass),
/// and returns the records with clocks in `(applied, upto]` in clock order.
///
/// A record past `upto` that was already complete on disk is held for the
/// next pass; a torn frame at a file's end (an append in flight, or a
/// buffer that flushed mid-frame) is left for the next pass too, at the
/// offset it began. The caller fixes `upto` at a point it has fsynced, so
/// every clock in the window is on disk and a gap is corruption, refused.
pub struct ClockedTail {
    gen_dir: PathBuf,
    files: Vec<TailPosition>,
    pending: std::collections::BTreeMap<u64, WalRecord>,
    applied: u64,
}

impl ClockedTail {
    /// Start tailing `gen_dir` strictly after clock `applied`. Frames with
    /// clocks at or below it are read once and dropped; the first pass is
    /// therefore a full read of the generation, later passes are
    /// incremental.
    pub fn start(gen_dir: &Path, applied: u64) -> Self {
        Self {
            gen_dir: gen_dir.to_path_buf(),
            files: Vec::new(),
            pending: std::collections::BTreeMap::new(),
            applied,
        }
    }

    /// The clock the caller has consumed through.
    pub fn applied(&self) -> u64 {
        self.applied
    }

    fn refresh_files(&mut self) -> io::Result<()> {
        let mut paths = Vec::new();
        let markers = markers_path(&self.gen_dir);
        if markers.exists() {
            paths.push(markers);
        }
        for entry in std::fs::read_dir(&self.gen_dir)? {
            let entry = entry?;
            if parse_bucket_name(&entry.file_name().to_string_lossy()).is_some() {
                paths.push(entry.path());
            }
        }
        for path in paths {
            if !self.files.iter().any(|held| held.path == path) {
                self.files.push(TailPosition {
                    path,
                    offset: 0,
                    next_seq: 1,
                });
            }
        }
        Ok(())
    }

    /// Records with clocks in `(applied, upto]`, in clock order, and
    /// advance `applied` to `upto`. Refuses a clock the window should hold but the files do not.
    pub fn read_through(&mut self, upto: u64) -> io::Result<Vec<WalRecord>> {
        if upto < self.applied {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "WAL tail asked to read through clock {upto}, below the applied clock {}",
                    self.applied
                ),
            ));
        }
        self.refresh_files()?;
        for held in &mut self.files {
            let mut reader = RecordReader::open_at(&held.path, held.offset, held.next_seq)?;
            loop {
                let frame_start = reader.offset();
                match reader.next_record()? {
                    Some(record) => {
                        held.offset = reader.offset();
                        held.next_seq = record.seq + 1;
                        if record.clock > self.applied {
                            self.pending.insert(record.clock, record);
                        }
                    }
                    None => {
                        // A torn frame begins at `frame_start`; the next
                        // pass retries from there. A clean end leaves the
                        // offset where the reader stopped.
                        if reader.offset() > frame_start {
                            held.offset = frame_start;
                        }
                        break;
                    }
                }
            }
        }
        let mut out = Vec::new();
        let mut expected = self.applied + 1;
        while expected <= upto {
            match self.pending.remove(&expected) {
                Some(record) => out.push(record),
                None => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                        "WAL generation clock gap while tailing {}: clock {expected} is missing \
                             below the flushed watermark {upto}",
                        self.gen_dir.display()
                    ),
                    ))
                }
            }
            expected += 1;
        }
        self.applied = upto;
        Ok(out)
    }
}

/// Truncate every bucket file in `gen_dir` at its first record whose id
/// is `>= cutoff_id`; returns how many records were dropped.
///
/// This is the crash-recovery reconciliation: appends are buffered, so a
/// crash between Flushes can leave the on-disk log AHEAD of the on-disk
/// index (kernel-cached pages survive a process crash without any
/// fsync). A reopening node would then re-assign ids the log already
/// holds and poison it with duplicates. Cutting the log back to the
/// applied tip restores the invariant that the log and the index agree
/// at every durability point — the dropped records were never
/// durable-acked (Flush is the durability point).
///
/// Ids are assigned monotonically per shard and appended in assignment
/// order, so within each bucket file ids ascend and the cut is a prefix
/// cut. Markers carry no id and are untouched. A torn tail is left for
/// [`WalWriter::resume`](super::WalWriter::resume), which truncates it anyway.
pub fn truncate_records_at_or_above(gen_dir: &Path, cutoff_id: u64) -> io::Result<u64> {
    let mut dropped = 0u64;
    for entry in std::fs::read_dir(gen_dir)? {
        let entry = entry?;
        if parse_bucket_name(&entry.file_name().to_string_lossy()).is_none() {
            continue;
        }
        let path = entry.path();
        let mut reader = RecordReader::open(&path)?;
        let mut cut_at: Option<u64> = None;
        loop {
            let frame_start = reader.offset;
            match reader.next_record()? {
                Some(record) => {
                    let id = match &record.op {
                        Some(wal_record::Op::AddVectors(a)) => Some(a.first_id),
                        Some(wal_record::Op::AddDocuments(a)) => Some(a.first_id),
                        _ => None,
                    };
                    if cut_at.is_some() || id.is_some_and(|id| id >= cutoff_id) {
                        cut_at.get_or_insert(frame_start);
                        dropped += 1;
                    }
                }
                None => break,
            }
        }
        if let Some(offset) = cut_at {
            OpenOptions::new()
                .write(true)
                .open(&path)?
                .set_len(offset)?;
        }
    }
    Ok(dropped)
}

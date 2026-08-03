//! Per-shard write-ahead log (WAL): bucketed folder layout, writer, and
//! reader.
//!
//! One directory per shard generation at `<index path>.wal/gen-<NNNNNN>/`
//! holding:
//!
//! - `manifest.toml` — the shard shape (dim, bit width, calibration, slot
//!   offset, generation) plus `bucket_bits`/`bucket_count` and a format
//!   version. Calibration starts empty on a from-scratch shard and the
//!   manifest is REWRITTEN (atomically: tmp + rename) when calibration
//!   locks — the same lazy-completion semantics the frame header had.
//! - `bucket-<NNN>.wal` — one append-only file per hash bucket. Every
//!   vector/document record routes to `bucket_of(id, bucket_count)` — the
//!   exact partition function the reshard tool splits by, so each bucket
//!   file is a pre-partitioned log slice a cheap split can consume
//!   without re-hashing a single record.
//! - `markers.wal` — FlushMarker / SnapshotMarker records (they carry no
//!   id, so they get their own small file instead of fanning out to every
//!   bucket).
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

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use prost::Message;
use serde::{Deserialize, Serialize};

use crate::pb::wal::{wal_record, WalRecord};
use crate::reshard::bucket_of;

// ---------------------------------------------------------------------------
// CRC32 (IEEE, table-based; hand-rolled to avoid a crate dependency)
// ---------------------------------------------------------------------------

const CRC_TABLE: [u32; 256] = {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut c = i as u32;
        let mut k = 0;
        while k < 8 {
            c = if c & 1 != 0 {
                0xEDB8_8320 ^ (c >> 1)
            } else {
                c >> 1
            };
            k += 1;
        }
        table[i] = c;
        i += 1;
    }
    table
};

/// CRC32 (IEEE 802.3 polynomial, reflected) of `data`.
pub fn crc32(data: &[u8]) -> u32 {
    let mut c = !0u32;
    for &b in data {
        c = CRC_TABLE[((c ^ u32::from(b)) & 0xFF) as usize] ^ (c >> 8);
    }
    !c
}

// ---------------------------------------------------------------------------
// Layout
// ---------------------------------------------------------------------------

/// Current on-disk format version (manifest `format_version`).
pub const FORMAT_VERSION: u32 = 1;

/// The WAL directory of a shard: `<index path>.wal/`.
pub fn wal_dir(index_path: &Path) -> PathBuf {
    let mut p = index_path.as_os_str().to_owned();
    p.push(".wal");
    PathBuf::from(p)
}

/// The directory of one generation inside a WAL directory.
pub fn gen_dir(wal_dir: &Path, generation: u64) -> PathBuf {
    wal_dir.join(format!("gen-{generation:06}"))
}

/// The bucket file for `bucket` inside a generation directory.
pub fn bucket_path(gen_dir: &Path, bucket: u32) -> PathBuf {
    gen_dir.join(format!("bucket-{bucket:03}.wal"))
}

/// The marker log inside a generation directory.
pub fn markers_path(gen_dir: &Path) -> PathBuf {
    gen_dir.join("markers.wal")
}

/// The manifest path inside a generation directory.
pub fn manifest_path(gen_dir: &Path) -> PathBuf {
    gen_dir.join("manifest.toml")
}

/// Parse a generation directory name (`gen-000042`) back into its number.
fn parse_gen_name(name: &str) -> Option<u64> {
    name.strip_prefix("gen-")?.parse().ok()
}

/// Parse a bucket filename (`bucket-007.wal`) back into its index.
fn parse_bucket_name(name: &str) -> Option<u32> {
    name.strip_prefix("bucket-")?
        .strip_suffix(".wal")?
        .parse()
        .ok()
}

/// The newest generation directory in a WAL directory, if any.
pub fn latest_gen(wal_dir: &Path) -> io::Result<Option<(u64, PathBuf)>> {
    let mut latest: Option<(u64, PathBuf)> = None;
    if !wal_dir.exists() {
        return Ok(None);
    }
    for entry in std::fs::read_dir(wal_dir)? {
        let entry = entry?;
        if let Some(gen) = parse_gen_name(&entry.file_name().to_string_lossy()) {
            if latest.as_ref().is_none_or(|(g, _)| gen > *g) {
                latest = Some((gen, entry.path()));
            }
        }
    }
    Ok(latest)
}

// ---------------------------------------------------------------------------
// Manifest
// ---------------------------------------------------------------------------

/// The shard shape of one WAL generation (`manifest.toml`). Replaying a
/// generation requires a locked calibration (`calibration_shift`/`scale`
/// present, length `dim`); a generation logged by an UNSEEDED shard
/// carries an empty calibration and cannot be resharded (its scores are
/// not comparable across shards anyway — see `NodeService.AddVectors`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WalManifest {
    /// Vector dimensionality. 0 while the shard has not committed to one.
    pub dim: u32,
    /// Quantization bit width (2, 3, or 4).
    pub bit_width: u32,
    /// Per-coordinate calibration shift (length `dim`), matching how
    /// search.proto carries calibration (`GetCalibrationResponse`).
    #[serde(default)]
    pub calibration_shift: Vec<f32>,
    /// Per-coordinate calibration scale (length `dim`).
    #[serde(default)]
    pub calibration_scale: Vec<f32>,
    /// The shard's global id base for this generation.
    pub slot_offset: u64,
    /// Generation number (matches the directory name; 0 for the initial
    /// generation, +1 per snapshot rotation).
    pub generation: u64,
    /// log2 of `bucket_count`; the top `bucket_bits` of `fnv1a64(id)`
    /// select the bucket file.
    pub bucket_bits: u32,
    /// Number of bucket files (a power of two), fixed at WAL creation.
    pub bucket_count: u32,
    /// Vectors the shard already held when this generation was created —
    /// state the log does NOT contain. Nonzero after an InstallSnapshot
    /// rotation (the image's contents) or when logging was enabled on an
    /// already-populated shard. A generation with preexisting state is
    /// not full history: replaying it alone reproduces only the records
    /// logged since, so the reshard tool refuses it.
    #[serde(default)]
    pub preexisting_vectors: u64,
    /// Documents the shard already held when this generation was created;
    /// see [`Self::preexisting_vectors`].
    #[serde(default)]
    pub preexisting_documents: u64,
    /// On-disk format version; [`FORMAT_VERSION`].
    pub format_version: u32,
}

/// Write the manifest atomically (tmp file + rename).
pub fn write_manifest(gen_dir: &Path, manifest: &WalManifest) -> io::Result<()> {
    let tmp = gen_dir.join("manifest.toml.tmp");
    std::fs::write(&tmp, toml::to_string(manifest).map_err(io::Error::other)?)?;
    std::fs::rename(&tmp, manifest_path(gen_dir))
}

/// Read and validate a generation's manifest.
pub fn read_manifest(gen_dir: &Path) -> io::Result<WalManifest> {
    let text = std::fs::read_to_string(manifest_path(gen_dir))?;
    let manifest: WalManifest = toml::from_str(&text)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("bad manifest: {e}")))?;
    if manifest.format_version != FORMAT_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "unsupported WAL format version {} (this build understands {FORMAT_VERSION})",
                manifest.format_version
            ),
        ));
    }
    if !manifest.bucket_count.is_power_of_two()
        || manifest.bucket_count == 0
        || 1u64 << manifest.bucket_bits != u64::from(manifest.bucket_count)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "bad bucket geometry: bucket_bits={} bucket_count={}",
                manifest.bucket_bits, manifest.bucket_count
            ),
        ));
    }
    Ok(manifest)
}

// ---------------------------------------------------------------------------
// Frames
// ---------------------------------------------------------------------------

/// The three ways a frame read can end: a complete frame, a clean
/// end-of-log, or a torn tail (crash mid-append) at `offset`.
enum FrameRead {
    Frame(Vec<u8>),
    End,
    Torn { offset: u64 },
}

/// Read until `buf` is full or EOF; returns how many bytes were read.
fn read_up_to(reader: &mut impl Read, buf: &mut [u8]) -> io::Result<usize> {
    let mut n = 0;
    while n < buf.len() {
        match reader.read(&mut buf[n..]) {
            Ok(0) => break,
            Ok(m) => n += m,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(n)
}

fn read_frame(reader: &mut impl Read, offset: &mut u64) -> io::Result<FrameRead> {
    let start = *offset;
    let mut len_buf = [0u8; 4];
    match read_up_to(reader, &mut len_buf)? {
        0 => return Ok(FrameRead::End), // clean end-of-log
        4 => {}
        _ => return Ok(FrameRead::Torn { offset: start }),
    }
    *offset += 4;
    let len = u32::from_le_bytes(len_buf) as usize;
    let mut crc_buf = [0u8; 4];
    if read_up_to(reader, &mut crc_buf)? < 4 {
        return Ok(FrameRead::Torn { offset: start });
    }
    *offset += 4;
    let mut payload = vec![0u8; len];
    if read_up_to(reader, &mut payload)? < len {
        return Ok(FrameRead::Torn { offset: start });
    }
    *offset += len as u64;
    let expected = u32::from_le_bytes(crc_buf);
    if crc32(&payload) != expected {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("wal: crc mismatch in frame at byte offset {start}"),
        ));
    }
    Ok(FrameRead::Frame(payload))
}

fn write_frame(file: &mut impl Write, payload: &[u8]) -> io::Result<()> {
    file.write_all(&(payload.len() as u32).to_le_bytes())?;
    file.write_all(&crc32(payload).to_le_bytes())?;
    file.write_all(payload)
}

// ---------------------------------------------------------------------------
// Reader / recovery scan (one file)
// ---------------------------------------------------------------------------

/// Sequential replay over one bucket or markers file. Validates per-frame
/// CRC and seq continuity; a torn tail frame ends the file with a warning
/// (see the module docs).
pub struct RecordReader {
    reader: BufReader<File>,
    path: PathBuf,
    offset: u64,
    next_seq: u64,
    done: bool,
}

impl RecordReader {
    pub fn open(path: &Path) -> io::Result<Self> {
        Ok(Self {
            reader: BufReader::new(File::open(path)?),
            path: path.to_path_buf(),
            offset: 0,
            next_seq: 1,
            done: false,
        })
    }

    /// Byte offset one past the last frame consumed — the length of the
    /// valid prefix so far. Inspection tooling uses it to detect a torn
    /// tail (`offset() < file length` after the reader ends).
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// The next record, or `None` at the end of the file (including a
    /// torn tail, which is reported on stderr and treated as the end).
    pub fn next_record(&mut self) -> io::Result<Option<WalRecord>> {
        if self.done {
            return Ok(None);
        }
        let frame_start = self.offset;
        let frame = match read_frame(&mut self.reader, &mut self.offset)? {
            FrameRead::Frame(payload) => payload,
            FrameRead::End => {
                self.done = true;
                return Ok(None);
            }
            FrameRead::Torn { offset } => {
                eprintln!(
                    "wal: ignoring torn tail frame at byte offset {offset} in {}",
                    self.path.display()
                );
                self.done = true;
                return Ok(None);
            }
        };
        let record = WalRecord::decode(frame.as_slice()).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "wal: undecodable record at byte offset {frame_start} in {}: {e}",
                    self.path.display()
                ),
            )
        })?;
        if record.seq != self.next_seq {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "wal: seq gap at byte offset {frame_start} in {}: expected {}, got {}",
                    self.path.display(),
                    self.next_seq,
                    record.seq
                ),
            ));
        }
        self.next_seq += 1;
        Ok(Some(record))
    }
}

/// The recovery summary of one file after a crash: the seq of the last
/// intact record and the byte length of the valid prefix (append resumes
/// there; the torn tail is truncated).
pub struct RecordScan {
    pub last_seq: u64,
    pub valid_len: u64,
}

/// Scan one file tolerantly for reopen-after-crash. Like [`RecordReader`]
/// but never fails on an empty file or torn tail; CRC/seq errors inside
/// complete frames are still hard errors.
pub fn scan_records(path: &Path) -> io::Result<RecordScan> {
    let mut reader = RecordReader::open(path)?;
    let mut scan = RecordScan {
        last_seq: 0,
        valid_len: 0,
    };
    loop {
        let frame_start = reader.offset;
        match reader.next_record()? {
            Some(record) => {
                scan.last_seq = record.seq;
                scan.valid_len = reader.offset;
            }
            None => {
                if reader.offset > frame_start {
                    // Torn tail: the valid prefix ends where it began.
                    scan.valid_len = frame_start;
                }
                return Ok(scan);
            }
        }
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
/// [`WalWriter::resume`], which truncates it anyway.
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

// ---------------------------------------------------------------------------
// Writer
// ---------------------------------------------------------------------------

/// One open bucket file: buffered appends plus its own seq counter.
struct BucketWriter {
    file: BufWriter<File>,
    next_seq: u64,
}

impl BucketWriter {
    fn create(path: &Path) -> io::Result<Self> {
        Ok(Self {
            file: BufWriter::new(File::create(path)?),
            next_seq: 1,
        })
    }

    fn open_append(path: &Path, scan: &RecordScan) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        file.set_len(scan.valid_len)?;
        let mut file = file;
        file.seek(SeekFrom::Start(scan.valid_len))?;
        Ok(Self {
            file: BufWriter::new(file),
            next_seq: scan.last_seq + 1,
        })
    }

    fn append(&mut self, op: &wal_record::Op) -> io::Result<()> {
        let record = WalRecord {
            seq: self.next_seq,
            op: Some(op.clone()),
        };
        write_frame(&mut self.file, &record.encode_to_vec())?;
        self.next_seq += 1;
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()?;
        self.file.get_ref().sync_all()
    }
}

/// Buffered writer for one generation directory. Bucket files open lazily
/// on the first record that routes to them; the manifest is on disk from
/// creation and atomically rewritten by [`WalWriter::update_manifest`]
/// until the first record lands.
pub struct WalWriter {
    dir: PathBuf,
    manifest: WalManifest,
    buckets: HashMap<u32, BucketWriter>,
    markers: BucketWriter,
    records_appended: u64,
}

impl WalWriter {
    /// Open a NEW generation (`manifest.generation`) inside `wal_dir`:
    /// fresh directory, manifest on disk, empty markers file. Any
    /// existing files in the directory are truncated — a create means
    /// "this generation starts now" (initial WAL, or rotation after a
    /// snapshot superseded the old log).
    pub fn create(wal_dir: &Path, manifest: WalManifest) -> io::Result<Self> {
        // The same geometry check read_manifest applies — a log that can
        // be written but never read back must not come into existence.
        if !manifest.bucket_count.is_power_of_two()
            || manifest.bucket_count == 0
            || 1u64 << manifest.bucket_bits != u64::from(manifest.bucket_count)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "bad bucket geometry: bucket_bits={} bucket_count={}",
                    manifest.bucket_bits, manifest.bucket_count
                ),
            ));
        }
        let dir = gen_dir(wal_dir, manifest.generation);
        std::fs::create_dir_all(&dir)?;
        write_manifest(&dir, &manifest)?;
        Ok(Self {
            markers: BucketWriter::create(&markers_path(&dir))?,
            dir,
            manifest,
            buckets: HashMap::new(),
            records_appended: 0,
        })
    }

    /// Resume the generation in `gen_dir` after a restart: adopt its
    /// manifest, and for every existing bucket file and the markers file
    /// truncate any torn tail and continue its sequence.
    pub fn resume(gen_dir: &Path, manifest: WalManifest) -> io::Result<Self> {
        let mut buckets = HashMap::new();
        for entry in std::fs::read_dir(gen_dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if let Some(bucket) = parse_bucket_name(&name) {
                let path = entry.path();
                let scan = scan_records(&path)?;
                buckets.insert(bucket, BucketWriter::open_append(&path, &scan)?);
            }
        }
        let markers = match scan_records(&markers_path(gen_dir)) {
            Ok(scan) => BucketWriter::open_append(&markers_path(gen_dir), &scan)?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                BucketWriter::create(&markers_path(gen_dir))?
            }
            Err(e) => return Err(e),
        };
        let records_appended = buckets.values().map(|b| b.next_seq - 1).sum();
        Ok(Self {
            dir: gen_dir.to_path_buf(),
            manifest,
            buckets,
            markers,
            records_appended,
        })
    }

    /// Complete the manifest (calibration on `SetCalibration`, dim on the
    /// first batch); rewrites `manifest.toml` atomically when anything
    /// changed. Returns false once any vector/document record has been
    /// logged — the manifest records describe the shard as the log
    /// started and must never change under it.
    pub fn update_manifest(&mut self, update: impl FnOnce(&mut WalManifest)) -> bool {
        if self.records_appended > 0 {
            return false;
        }
        let mut updated = self.manifest.clone();
        update(&mut updated);
        if updated == self.manifest {
            return true;
        }
        match write_manifest(&self.dir, &updated) {
            Ok(()) => {
                self.manifest = updated;
                true
            }
            Err(e) => {
                eprintln!(
                    "wal: manifest rewrite in {} failed: {e}",
                    self.dir.display()
                );
                false
            }
        }
    }

    pub fn manifest(&self) -> &WalManifest {
        &self.manifest
    }

    pub fn generation(&self) -> u64 {
        self.manifest.generation
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    fn bucket(&mut self, bucket: u32) -> io::Result<&mut BucketWriter> {
        if let std::collections::hash_map::Entry::Vacant(e) = self.buckets.entry(bucket) {
            e.insert(BucketWriter::create(&bucket_path(&self.dir, bucket))?);
        }
        Ok(self.buckets.get_mut(&bucket).expect("just inserted"))
    }

    /// Append one record. The node applies the mutation to the in-memory
    /// index FIRST and logs after, under one lock: both sides are
    /// volatile until Flush, and Flush fsyncs the log before it writes
    /// the index images, so every durable index state has its records
    /// durable too — while an apply that fails never reaches the log
    /// (a logged-but-unapplied record would poison the id sequence).
    /// Vector/document records route to their id's bucket file; markers
    /// go to `markers.wal`. Buffered only — no fsync.
    ///
    /// NOTE a `LoggedAddVectors` batch must not straddle buckets: the
    /// caller (the node) splits batches into per-vector records, so
    /// `first_id` routes the whole record.
    pub fn append(&mut self, op: wal_record::Op) -> io::Result<()> {
        let bucket = match &op {
            wal_record::Op::AddVectors(a) => {
                Some(bucket_of(a.first_id, self.manifest.bucket_count as usize) as u32)
            }
            wal_record::Op::AddDocuments(a) => {
                Some(bucket_of(a.first_id, self.manifest.bucket_count as usize) as u32)
            }
            wal_record::Op::Flush(_) | wal_record::Op::Snapshot(_) => None,
        };
        match bucket {
            Some(b) => {
                self.records_appended += 1;
                self.bucket(b)?.append(&op)
            }
            None => self.markers.append(&op),
        }
    }

    /// Flush all buffers and fsync every open file. Called on Flush and
    /// on generation rotation.
    pub fn flush(&mut self) -> io::Result<()> {
        for (bucket, writer) in &mut self.buckets {
            writer
                .flush()
                .map_err(|e| io::Error::new(e.kind(), format!("bucket {bucket}: {e}")))?;
        }
        self.markers.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pb::wal::{FlushMarker, LoggedAddDocuments, LoggedAddVectors, SnapshotMarker};
    use crate::pb::{AddDocumentsRequest, AddVectorsRequest};

    fn tempdir(tag: &str) -> PathBuf {
        // Under target/ (a real disk), not the tmpfs /tmp — same
        // convention as the integration tests' CARGO_TARGET_TMPDIR,
        // which cargo does not set for unit tests.
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!(
            "target/test-tmp/wal_{tag}_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn manifest(bucket_count: u32) -> WalManifest {
        WalManifest {
            dim: 8,
            bit_width: 4,
            calibration_shift: vec![0.0; 8],
            calibration_scale: vec![1.0; 8],
            slot_offset: 25_000_000,
            generation: 0,
            bucket_bits: bucket_count.trailing_zeros(),
            bucket_count,
            preexisting_vectors: 0,
            preexisting_documents: 0,
            format_version: FORMAT_VERSION,
        }
    }

    fn add_op(id: u64) -> wal_record::Op {
        wal_record::Op::AddVectors(LoggedAddVectors {
            first_id: id,
            batch: Some(AddVectorsRequest {
                vectors: vec![1.0; 8],
                dim: 8,
            }),
        })
    }

    fn doc_op(id: u64) -> wal_record::Op {
        wal_record::Op::AddDocuments(LoggedAddDocuments {
            first_id: id,
            documents: vec![AddDocumentsRequest {
                map_numerics: Vec::new(),
                map_facets: Vec::new(),
                numerics: Vec::new(),
                facets: Vec::new(),
                text: format!("doc {id}"),
                analysis: None,
                lineage: None,
                fields: Vec::new(),
            }],
        })
    }

    /// All records in one file, in log order.
    fn read_all(path: &Path) -> io::Result<Vec<WalRecord>> {
        let mut reader = RecordReader::open(path)?;
        let mut out = Vec::new();
        while let Some(record) = reader.next_record()? {
            out.push(record);
        }
        Ok(out)
    }

    #[test]
    fn crc32_known_vector() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
        assert_eq!(crc32(b""), 0);
    }

    #[test]
    fn manifest_roundtrip() {
        let dir = tempdir("manifest");
        let gen = gen_dir(&dir, 3);
        std::fs::create_dir_all(&gen).unwrap();
        let m = manifest(64);
        write_manifest(&gen, &m).unwrap();
        assert_eq!(read_manifest(&gen).unwrap(), m);
        // Bad geometry is rejected.
        let mut bad = m.clone();
        bad.bucket_bits = 5;
        write_manifest(&gen, &bad).unwrap();
        assert!(read_manifest(&gen).is_err());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn roundtrip_buckets_and_resume() {
        let dir = tempdir("roundtrip");
        let mut writer = WalWriter::create(&dir, manifest(4)).unwrap();
        // Manifest completion is still possible while uncalibrated.
        assert!(writer.update_manifest(|m| m.slot_offset = 42));
        for id in 0..20u64 {
            writer.append(add_op(id)).unwrap();
        }
        writer.append(doc_op(3)).unwrap();
        writer
            .append(wal_record::Op::Flush(FlushMarker {}))
            .unwrap();
        writer.flush().unwrap();
        drop(writer);

        // Records route to their id's bucket with per-file seq 1..m.
        let gen = gen_dir(&dir, 0);
        let mut total = 0;
        for bucket in 0..4u32 {
            let path = bucket_path(&gen, bucket);
            if !path.exists() {
                continue;
            }
            let records = read_all(&path).unwrap();
            for (i, record) in records.iter().enumerate() {
                assert_eq!(record.seq, i as u64 + 1);
                let first_id = match &record.op {
                    Some(wal_record::Op::AddVectors(a)) => a.first_id,
                    Some(wal_record::Op::AddDocuments(a)) => a.first_id,
                    other => panic!("unexpected op in bucket file: {other:?}"),
                };
                assert_eq!(bucket_of(first_id, 4) as u32, bucket);
            }
            total += records.len();
        }
        assert_eq!(total, 21);
        // The flush marker is in markers.wal, seq 1.
        let markers = read_all(&markers_path(&gen)).unwrap();
        assert_eq!(markers.len(), 1);
        assert!(matches!(markers[0].op, Some(wal_record::Op::Flush(_))));

        // Resume: sequences continue, manifest is adopted (and locked).
        let m = read_manifest(&gen).unwrap();
        assert_eq!(m.slot_offset, 42);
        let mut writer = WalWriter::resume(&gen, m).unwrap();
        assert!(!writer.update_manifest(|m| m.slot_offset = 1));
        writer.append(add_op(100)).unwrap();
        writer.flush().unwrap();
        let bucket = bucket_of(100, 4) as u32;
        let records = read_all(&bucket_path(&gen, bucket)).unwrap();
        let bucket_records_before = (0..20u64)
            .filter(|&id| bucket_of(id, 4) as u32 == bucket)
            .count()
            // The document (id 3) rides its bucket too.
            + usize::from(bucket_of(3, 4) as u32 == bucket);
        assert_eq!(
            records.last().unwrap().seq,
            bucket_records_before as u64 + 1
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn torn_tail_on_one_bucket_leaves_others_intact() {
        let dir = tempdir("torn");
        let mut writer = WalWriter::create(&dir, manifest(4)).unwrap();
        for id in 0..20u64 {
            writer.append(add_op(id)).unwrap();
        }
        writer
            .append(wal_record::Op::Snapshot(SnapshotMarker {
                source_generation: 0,
            }))
            .unwrap();
        writer.flush().unwrap();
        drop(writer);

        let gen = gen_dir(&dir, 0);
        // Tear the tail of bucket 0 (append garbage bytes, then cut mid-frame).
        let torn = bucket_path(&gen, 0);
        let len = std::fs::metadata(&torn).unwrap().len();
        let file = OpenOptions::new().write(true).open(&torn).unwrap();
        file.set_len(len - 3).unwrap();
        drop(file);

        // The torn bucket loses its tail frame but scans cleanly to the
        // valid prefix; resume continues its sequence.
        let scan = scan_records(&torn).unwrap();
        let intact: Vec<u64> = (0..20u64).filter(|&id| bucket_of(id, 4) == 0).collect();
        assert!(scan.last_seq <= intact.len() as u64);
        let m = read_manifest(&gen).unwrap();
        let mut writer = WalWriter::resume(&gen, m).unwrap();
        let extra_id = (20..).find(|&id| bucket_of(id, 4) == 0).unwrap();
        writer.append(add_op(extra_id)).unwrap();
        writer.flush().unwrap();
        drop(writer);
        let records = read_all(&torn).unwrap();
        assert_eq!(
            records.last().unwrap().seq,
            scan.last_seq + 1,
            "resume continues the truncated bucket's sequence"
        );

        // Every other bucket still replays fully.
        for bucket in 1..4u32 {
            let path = bucket_path(&gen, bucket);
            if !path.exists() {
                continue;
            }
            let records = read_all(&path).unwrap();
            let expected = (0..20u64)
                .filter(|&id| bucket_of(id, 4) as u32 == bucket)
                .count();
            assert_eq!(records.len(), expected);
        }
        // Markers file intact.
        assert_eq!(read_all(&markers_path(&gen)).unwrap().len(), 1);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn corruption_is_an_error_with_offset() {
        let dir = tempdir("corrupt");
        let mut writer = WalWriter::create(&dir, manifest(2)).unwrap();
        for id in 0..10u64 {
            writer.append(add_op(id)).unwrap();
        }
        writer.flush().unwrap();
        drop(writer);

        let gen = gen_dir(&dir, 0);
        let path = bucket_path(&gen, 1);
        let len = std::fs::metadata(&path).unwrap().len();
        let mut bytes = std::fs::read(&path).unwrap();
        // Flip a byte inside the LAST frame's payload (payloads here are
        // ~40 bytes, so the tail is payload, not a length prefix — a
        // corrupted length would read as a torn tail instead).
        bytes[len as usize - 3] ^= 0xFF;
        std::fs::write(&path, &bytes).unwrap();

        let mut reader = RecordReader::open(&path).unwrap();
        let mut err = None;
        loop {
            match reader.next_record() {
                Ok(Some(_)) => {}
                Ok(None) => break,
                Err(e) => {
                    err = Some(e);
                    break;
                }
            }
        }
        let err = err.expect("corruption must be detected");
        assert!(err.to_string().contains("byte offset"), "{err}");
        // Bucket 0 is unaffected.
        let other = read_all(&bucket_path(&gen, 0)).unwrap();
        assert_eq!(
            other.len(),
            (0..10u64).filter(|&id| bucket_of(id, 2) == 0).count()
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn truncate_cuts_records_at_or_above_the_applied_tip() {
        let dir = tempdir("truncate");
        let mut writer = WalWriter::create(&dir, manifest(4)).unwrap();
        // Ids 0..12 applied and flushed; 12..20 are the crash-surviving
        // tail the index never persisted.
        for id in 0..20u64 {
            writer.append(add_op(id)).unwrap();
        }
        writer.append(doc_op(20)).unwrap();
        writer
            .append(wal_record::Op::Flush(FlushMarker {}))
            .unwrap();
        writer.flush().unwrap();
        drop(writer);

        let gen = gen_dir(&dir, 0);
        let dropped = truncate_records_at_or_above(&gen, 12).unwrap();
        assert_eq!(dropped, 9, "ids 12..20 plus the id-20 document");

        // Survivors are exactly ids < 12, each in its bucket, sequences
        // still gapless; the marker file is untouched; resume appends
        // continue cleanly at the cut.
        let mut survivors = 0;
        for bucket in 0..4u32 {
            let path = bucket_path(&gen, bucket);
            if !path.exists() {
                continue;
            }
            for record in read_all(&path).unwrap() {
                match record.op {
                    Some(wal_record::Op::AddVectors(a)) => assert!(a.first_id < 12),
                    other => panic!("unexpected survivor: {other:?}"),
                }
                survivors += 1;
            }
        }
        assert_eq!(survivors, 12);
        assert_eq!(read_all(&markers_path(&gen)).unwrap().len(), 1);
        let m = read_manifest(&gen).unwrap();
        let mut writer = WalWriter::resume(&gen, m).unwrap();
        writer.append(add_op(12)).unwrap();
        writer.flush().unwrap();
        assert_eq!(truncate_records_at_or_above(&gen, 13).unwrap(), 0);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn seq_gap_is_an_error() {
        let dir = tempdir("seqgap");
        let path = bucket_path(&dir, 0);
        let mut bytes = Vec::new();
        let mut push = |payload: Vec<u8>| {
            bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            bytes.extend_from_slice(&crc32(&payload).to_le_bytes());
            bytes.extend_from_slice(&payload);
        };
        push(
            WalRecord {
                seq: 1,
                op: Some(wal_record::Op::Flush(FlushMarker {})),
            }
            .encode_to_vec(),
        );
        push(
            WalRecord {
                seq: 3,
                op: Some(wal_record::Op::Flush(FlushMarker {})),
            }
            .encode_to_vec(),
        );
        std::fs::write(&path, &bytes).unwrap();

        let mut reader = RecordReader::open(&path).unwrap();
        assert_eq!(reader.next_record().unwrap().unwrap().seq, 1);
        let err = reader.next_record().unwrap_err();
        assert!(err.to_string().contains("seq gap"), "{err}");
        std::fs::remove_dir_all(&dir).ok();
    }
}

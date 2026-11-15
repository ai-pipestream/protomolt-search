//! The generation writer: creation, resume, and manifest maintenance.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use prost::Message;

use super::frames::{scan_records, write_frame, RecordScan};
use super::layout::{
    bucket_path, gen_dir, latest_gen, markers_path, parse_bucket_name, FORMAT_VERSION,
};
use super::manifest::{
    check_manifest_tables, check_version, read_manifest, validate_derived, write_manifest,
    WalManifest,
};
use super::sources;
use super::tail::truncate_records_at_or_above;
use crate::pb::wal::{wal_record, WalRecord};

/// How long a first-open waits for a concurrent creator's manifest to
/// appear before giving up. The winning create is a directory create
/// plus one fsynced small-file rename — milliseconds on disk, still
/// well under a second on a loaded SD card — so 10 s of slack absorbs
/// a busy machine without masking a genuinely abandoned generation
/// directory (which still fails the open, just after the wait).
pub(super) const PEER_CREATE_TIMEOUT: Duration = Duration::from_secs(10);
pub(super) const PEER_CREATE_POLL: Duration = Duration::from_millis(5);

/// The geometry check `read_manifest` applies — a log that can be
/// written but never read back must not come into existence.
pub(super) fn check_geometry(manifest: &WalManifest) -> io::Result<()> {
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
    Ok(())
}

/// Open a shard's WAL the way a starting node does: resume the newest
/// generation (truncating records at or above `cutoff`, the applied
/// tip — buffered appends that outlived a crash) or, when no log
/// exists, create generation 0 from `fresh`.
///
/// Concurrent first-open is idempotent: twin nodes sharing the shard
/// files can cold-start together. A peer's create is visible (the
/// generation directory) before it is complete (the manifest rename),
/// so a NotFound while reading a just-appeared generation is retried
/// until [`PEER_CREATE_TIMEOUT`] rather than failing the open, and the
/// create side is claimed atomically — every opener returns a valid
/// writer onto the SAME well-formed generation.
pub fn open_or_create(wal_dir: &Path, cutoff: u64, fresh: WalManifest) -> io::Result<WalWriter> {
    let deadline = Instant::now() + PEER_CREATE_TIMEOUT;
    loop {
        let attempt = match latest_gen(wal_dir)? {
            Some((_, gen)) => match read_manifest(&gen) {
                Ok(m) => {
                    check_manifest_tables(&m, &fresh)
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                    let dropped = truncate_records_at_or_above(&gen, cutoff)?;
                    if dropped > 0 {
                        eprintln!(
                            "wal: truncated {dropped} record(s) at or above applied tip {cutoff} \
                             in {} (buffered appends that outlived a crash; never durable-acked)",
                            gen.display()
                        );
                    }
                    WalWriter::resume(&gen, m)
                }
                Err(e) => Err(e),
            },
            None => WalWriter::create_or_resume(wal_dir, fresh.clone()),
        };
        match attempt {
            Err(e) if e.kind() == io::ErrorKind::NotFound && Instant::now() < deadline => {
                std::thread::sleep(PEER_CREATE_POLL);
            }
            result => return result,
        }
    }
}

/// One open bucket file: buffered appends plus its own seq counter.
pub(super) struct BucketWriter {
    file: BufWriter<File>,
    next_seq: u64,
}

impl BucketWriter {
    pub(super) fn create(path: &Path) -> io::Result<Self> {
        Ok(Self {
            file: BufWriter::new(File::create(path)?),
            next_seq: 1,
        })
    }

    pub(super) fn open_append(path: &Path, scan: &RecordScan) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        file.set_len(scan.valid_len)?;
        let mut file = file;
        file.seek(SeekFrom::Start(scan.valid_len))?;
        Ok(Self {
            file: BufWriter::new(file),
            next_seq: scan.last_seq + 1,
        })
    }

    pub(super) fn append(&mut self, clock: u64, op: &wal_record::Op) -> io::Result<()> {
        let record = WalRecord {
            seq: self.next_seq,
            clock,
            op: Some(op.clone()),
        };
        write_frame(&mut self.file, &record.encode_to_vec())?;
        self.next_seq += 1;
        Ok(())
    }

    pub(super) fn flush(&mut self) -> io::Result<()> {
        self.file.flush()?;
        self.file.get_ref().sync_all()
    }
}

/// Buffered writer for one generation directory. Bucket files open lazily
/// on the first record that routes to them; the manifest is on disk from
/// creation and atomically rewritten by [`WalWriter::update_manifest`]
/// until the first record lands.
pub struct WalWriter {
    pub(super) dir: PathBuf,
    pub(super) manifest: WalManifest,
    pub(super) buckets: HashMap<u32, BucketWriter>,
    pub(super) markers: BucketWriter,
    pub(super) records_appended: u64,
    pub(super) next_clock: u64,
    pub(super) dirty: bool,
    pub(super) sources: Option<sources::Writer>,
}

impl WalWriter {
    /// Open a NEW generation (`manifest.generation`) inside `wal_dir`:
    /// fresh directory, manifest on disk, empty markers file. Any
    /// existing files in the directory are truncated — a create means
    /// "this generation starts now" (initial WAL, or rotation after a
    /// snapshot superseded the old log).
    ///
    /// Node startup must NOT use this directly: two nodes cold-starting
    /// the same shard would both create and clobber each other. Startup
    /// goes through [`open_or_create`], which converges concurrent
    /// first-openers on one generation.
    pub fn create(wal_dir: &Path, mut manifest: WalManifest) -> io::Result<Self> {
        check_geometry(&manifest)?;
        validate_derived(&manifest)?;
        manifest.format_version = FORMAT_VERSION;
        let dir = gen_dir(wal_dir, manifest.generation);
        std::fs::create_dir_all(&dir)?;
        write_manifest(&dir, &manifest)?;
        Ok(Self {
            markers: BucketWriter::create(&markers_path(&dir))?,
            dir,
            manifest,
            buckets: HashMap::new(),
            records_appended: 0,
            next_clock: 1,
            dirty: false,
            sources: None,
        })
    }

    /// Open generation `manifest.generation` for a node that does not
    /// know whether a peer is creating it at this moment: claim the
    /// generation directory atomically (`create_dir` fails
    /// `AlreadyExists` for every opener but the first) and populate it,
    /// or — once the peer's manifest is visible — resume the peer's
    /// generation instead. Every concurrent opener ends up with a valid
    /// writer onto ONE well-formed generation, the cold-start contract
    /// for twin nodes sharing shard files.
    ///
    /// A claimed-but-manifestless directory is a create in mid-flight;
    /// it is polled until [`PEER_CREATE_TIMEOUT`], not mistaken for a
    /// resumable generation (an abandoned one still errors, just
    /// slower). Rotation keeps [`Self::create`]: there a pre-existing
    /// directory is stale state to truncate, not a peer to join.
    pub fn create_or_resume(wal_dir: &Path, mut manifest: WalManifest) -> io::Result<Self> {
        check_geometry(&manifest)?;
        validate_derived(&manifest)?;
        manifest.format_version = FORMAT_VERSION;
        std::fs::create_dir_all(wal_dir)?;
        let dir = gen_dir(wal_dir, manifest.generation);
        let deadline = Instant::now() + PEER_CREATE_TIMEOUT;
        loop {
            match std::fs::create_dir(&dir) {
                Ok(()) => {
                    // Claimed: this caller materializes the generation.
                    if manifest.preexisting_vectors > 0 || manifest.preexisting_documents > 0 {
                        eprintln!(
                            "wal: shard already holds {} vectors / {} documents; the new \
                             log records them as preexisting — this shard can serve but cannot be \
                             resharded from this log (rebuild via InstallSnapshot for full history)",
                            manifest.preexisting_vectors,
                            manifest.preexisting_documents
                        );
                    }
                    write_manifest(&dir, &manifest)?;
                    return Ok(Self {
                        markers: BucketWriter::create(&markers_path(&dir))?,
                        dir,
                        manifest,
                        buckets: HashMap::new(),
                        records_appended: 0,
                        next_clock: 1,
                        dirty: false,
                        sources: None,
                    });
                }
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => match read_manifest(&dir) {
                    Ok(m) => return Self::resume(&dir, m),
                    Err(e) if e.kind() == io::ErrorKind::NotFound && Instant::now() < deadline => {
                        std::thread::sleep(PEER_CREATE_POLL);
                    }
                    Err(e) => return Err(e),
                },
                Err(e) => return Err(e),
            }
        }
    }

    /// Resume the generation in `gen_dir` after a restart: adopt its
    /// manifest, and for every existing bucket file and the markers file
    /// truncate any torn tail and continue its sequence.
    pub fn resume(gen_dir: &Path, manifest: WalManifest) -> io::Result<Self> {
        check_version(&manifest)?;
        validate_derived(&manifest)?;
        let mut buckets = HashMap::new();
        let mut max_clock = 0u64;
        for entry in std::fs::read_dir(gen_dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if let Some(bucket) = parse_bucket_name(&name) {
                let path = entry.path();
                let scan = scan_records(&path)?;
                max_clock = max_clock.max(scan.max_clock);
                buckets.insert(bucket, BucketWriter::open_append(&path, &scan)?);
            }
        }
        let markers = match scan_records(&markers_path(gen_dir)) {
            Ok(scan) => {
                max_clock = max_clock.max(scan.max_clock);
                BucketWriter::open_append(&markers_path(gen_dir), &scan)?
            }
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
            next_clock: max_clock.saturating_add(1).max(1),
            dirty: false,
            sources: None,
        })
    }

    /// Complete the manifest (provider state on configuration, dimension on
    /// the first batch); rewrites `manifest.toml` atomically when anything
    /// changed. Returns false once any vector/document record has been
    /// logged — the manifest records describe the shard as the log
    /// started and must never change under it.
    pub fn update_manifest(&mut self, update: impl FnOnce(&mut WalManifest)) -> bool {
        if self.records_appended > 0 {
            return false;
        }
        let mut updated = self.manifest.clone();
        update(&mut updated);
        updated.format_version = FORMAT_VERSION;
        if let Err(error) = validate_derived(&updated) {
            eprintln!(
                "wal: refusing manifest update in {}: {error}",
                self.dir.display()
            );
            return false;
        }
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

    pub fn high_watermark(&self) -> u64 {
        self.next_clock.saturating_sub(1)
    }

    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Adopt `dir` as this generation's directory after the caller
    /// renamed it there (compaction builds the rewritten generation in a
    /// work directory and moves it into the shard's WAL directory at
    /// cutover). The open file handles follow the rename; only the path
    /// new bucket files are created under changes. Refuses a directory
    /// whose manifest is not this writer's, or one for another
    /// generation number.
    pub fn relocate(&mut self, dir: PathBuf) -> io::Result<()> {
        let held = read_manifest(&dir)?;
        if held != self.manifest {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "{} holds a manifest that is not this writer's generation {}",
                    dir.display(),
                    self.manifest.generation
                ),
            ));
        }
        if dir
            != gen_dir(
                dir.parent().unwrap_or(Path::new(".")),
                self.manifest.generation,
            )
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "{} is not the directory of generation {}",
                    dir.display(),
                    self.manifest.generation
                ),
            ));
        }
        self.dir = dir;
        Ok(())
    }

    pub(super) fn bucket(&mut self, bucket: u32) -> io::Result<&mut BucketWriter> {
        if let std::collections::hash_map::Entry::Vacant(e) = self.buckets.entry(bucket) {
            e.insert(BucketWriter::create(&bucket_path(&self.dir, bucket))?);
        }
        Ok(self.buckets.get_mut(&bucket).expect("just inserted"))
    }
}

//! Pin journal-matching index files before copying the coherent source bundle.
use super::*;
use crate::pb::{
    storage::{SourceBackupIndex, SourceBackupLimits, SourceBackupManifest},
    SnapshotArtifact,
};
use crate::segments::{OpenedSegmentSet, SegmentArtifact, SegmentCatalog};
use std::{
    collections::BTreeSet,
    io::{Read, Seek, SeekFrom, Write},
    path::{Component, PathBuf},
};

mod write;

pub(super) const SOURCE: &str = "sources.redb";
pub(super) const COMPLETION: &str = "source-backup.pb";

/// A coherent capture of source history and every journaled index. Open file
/// descriptions survive path retirement; no catalog mutation fence is retained
/// during the copy. Dropping this value releases its pins and source read view.
pub struct CapturedBackup<'a> {
    source: CatalogCheckpoint<'a>,
    limits: SourceBackupLimits,
    indexes: Vec<SourceBackupIndex>,
    files: Vec<PinnedFile>,
    paths: BTreeSet<String>,
    source_roots: BTreeSet<PathBuf>,
    bytes: u64,
    metadata_bytes: usize,
}
struct PinnedFile {
    metadata: SnapshotArtifact,
    data: Data,
}
enum Data {
    Bytes(Vec<u8>),
    File(File),
}

fn fail(message: impl Into<String>) -> Status {
    Status::failed_precondition(message.into())
}
pub(super) fn exhausted() -> Status {
    Status::resource_exhausted("source backup budget exceeded")
}
pub(super) fn charge(total: &mut u64, amount: u64, maximum: u64) -> Result<(), Status> {
    *total = total.checked_add(amount).ok_or_else(exhausted)?;
    if *total > maximum {
        return Err(exhausted());
    }
    Ok(())
}
fn file_name(name: &str) -> Result<(), Status> {
    let mut components = Path::new(name).components();
    if name.is_empty()
        || name.contains('\\')
        || !matches!(components.next(), Some(Component::Normal(_)))
        || components.next().is_some()
    {
        return Err(fail("backup artifact needs one portable relative filename"));
    }
    Ok(())
}
fn parent_directory(path: &Path) -> &Path {
    path.parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."))
}
pub(super) fn new_file(path: &Path) -> Result<File, Status> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path).map_err(storage)
}

pub(super) fn validate_limits(limits: &SourceBackupLimits) -> Result<(), Status> {
    if limits.metadata_bytes == 0
        || limits.metadata_bytes > 64 << 20
        || limits.max_files == 0
        || limits.max_files > 65_536
        || limits.max_bytes == 0
        || limits.source_batch_bytes == 0
        || limits.source_batch_bytes > 64 << 20
    {
        return Err(Status::invalid_argument("backup needs metadata_bytes and source_batch_bytes 1..64 MiB, max_files 1..65536 and positive max_bytes"));
    }
    Ok(())
}

impl DocumentCatalog {
    /// Capture exactly the indexes in the source journal. Call with the live
    /// catalogs owned by this authority, not newly opened lookalikes. The source
    /// read is pinned first; a concurrently advanced physical manifest refuses
    /// capture rather than silently selecting a newer source transaction.
    pub fn capture_backup<'a>(
        &'a self,
        indexes: &[(&[u8], &SegmentCatalog)],
        limits: &SourceBackupLimits,
    ) -> Result<CapturedBackup<'a>, Status> {
        validate_limits(limits)?;
        let source = self.capture_checkpoint(limits.metadata_bytes as usize)?;
        let states = &source.metadata().indexes;
        let mut supplied = indexes.to_vec();
        supplied.sort_by(|a, b| a.0.cmp(b.0));
        if supplied.len() != states.len()
            || supplied
                .iter()
                .zip(states)
                .any(|((key, _), state)| *key != state.index_key.as_slice())
        {
            return Err(fail(
                "backup must supply every journaled index exactly once",
            ));
        }
        let metadata_bytes = source.metadata().encoded_len();
        let mut roots = BTreeSet::new();
        for (_, catalog) in &supplied {
            if !roots.insert(std::fs::canonicalize(catalog.snapshot().root()).map_err(storage)?) {
                return Err(fail("backup indexes must have distinct catalog roots"));
            }
        }
        let mut capture = CapturedBackup {
            source,
            limits: *limits,
            indexes: Vec::new(),
            files: Vec::new(),
            paths: BTreeSet::new(),
            source_roots: roots,
            bytes: 0,
            metadata_bytes,
        };
        for (ordinal, (key, catalog)) in supplied.into_iter().enumerate() {
            let state = capture.source.metadata().indexes[ordinal].clone();
            let header = capture
                .source
                .metadata()
                .header
                .as_ref()
                .expect("captured header")
                .clone();
            catalog.with_durable_snapshot(|set| {
                capture.pin_index(ordinal, key, set, &state, &header)
            })?;
        }
        // Include the final protobuf inventory in metadata accounting before any
        // output is created. The source file fields need at most 256 extra bytes.
        let manifest = capture.manifest(capture.source.metadata().clone());
        capture.metadata_charge(
            manifest
                .encoded_len()
                .checked_add(256)
                .ok_or_else(exhausted)?,
        )?;
        Ok(capture)
    }
}

impl CapturedBackup<'_> {
    /// Pin one journaled index: its manifest must be the captured journal
    /// state's and owned by this source; every segment file it names is
    /// held open under the bundle path it will be copied to.
    fn pin_index(
        &mut self,
        ordinal: usize,
        key: &[u8],
        set: &OpenedSegmentSet,
        state: &crate::pb::storage::ProjectionJournalState,
        header: &DocumentCatalogHeader,
    ) -> Result<(), Status> {
        let directory = format!("indexes/{ordinal:08}/index.segments");
        let encoded = self.json(set.manifest())?;
        if turbo_sha256::sha256(&encoded).0.as_slice() != state.committed_manifest_sha256 {
            return Err(fail(
                "backup index manifest differs from its captured journal state",
            ));
        }
        let expected_owner = crate::pb::storage::SourceIndexOwner {
            format_version: 1,
            history_id: header.history_id.clone(),
            index_key: key.to_vec(),
            collection: header.collection.clone(),
        };
        match &set.manifest().source_owner {
            Some(owner) if owner.decode().map_err(Status::data_loss)? == expected_owner => {}
            None if state.committed_sequence == 0 && set.is_empty() => {}
            _ => return Err(fail("backup index belongs to another source owner")),
        }
        self.indexes.push(SourceBackupIndex {
            index_key: key.to_vec(),
            directory: directory.clone(),
            catalog_epoch: set.epoch(),
        });
        self.bytes_file(format!("{directory}/segments.json"), encoded)?;
        for segment in &set.manifest().segments {
            file_name(&segment.segment_id)?;
            let prefix = format!("{directory}/segments/{}", segment.segment_id);
            let bytes = self.json(segment)?;
            self.bytes_file(format!("{prefix}/segment.json"), bytes)?;
            let original = SegmentCatalog::segment_dir(set.root(), &segment.segment_id);
            for artifact in [
                Some(&segment.vector),
                Some(&segment.exact_vectors),
                Some(&segment.bm25),
                Some(&segment.live_docs),
                segment.multi_res.as_ref(),
            ]
            .into_iter()
            .flatten()
            {
                if artifact.file.is_empty() {
                    continue;
                }
                self.pin_file(&original, &prefix, artifact)?;
            }
        }
        Ok(())
    }
    fn metadata_charge(&mut self, bytes: usize) -> Result<(), Status> {
        self.metadata_bytes = self
            .metadata_bytes
            .checked_add(bytes)
            .ok_or_else(exhausted)?;
        if self.metadata_bytes > self.limits.metadata_bytes as usize {
            return Err(exhausted());
        }
        Ok(())
    }
    fn json(&self, value: &impl serde::Serialize) -> Result<Vec<u8>, Status> {
        struct Bounded {
            bytes: Vec<u8>,
            limit: usize,
        }
        impl Write for Bounded {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
                    return Err(std::io::Error::other("backup metadata budget exceeded"));
                }
                self.bytes.extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let mut output = Bounded {
            bytes: Vec::new(),
            limit: (self.limits.metadata_bytes as usize).saturating_sub(self.metadata_bytes),
        };
        serde_json::to_writer(&mut output, value).map_err(|_| exhausted())?;
        Ok(output.bytes)
    }
    fn add(&mut self, metadata: SnapshotArtifact, data: Data) -> Result<(), Status> {
        // The source database occupies one additional inventory slot.
        if self.files.len() as u32 + 1 >= self.limits.max_files {
            return Err(exhausted());
        }
        if !self.paths.insert(metadata.file.clone()) {
            return Err(fail("backup artifact path is duplicated"));
        }
        self.metadata_charge(metadata.encoded_len())?;
        charge(&mut self.bytes, metadata.bytes, self.limits.max_bytes)?;
        self.files.push(PinnedFile { metadata, data });
        Ok(())
    }
    fn bytes_file(&mut self, name: String, bytes: Vec<u8>) -> Result<(), Status> {
        self.metadata_charge(bytes.len())?;
        self.add(
            SnapshotArtifact {
                file: name,
                bytes: bytes.len() as u64,
                sha256: turbo_sha256::sha256(&bytes).to_hex(),
            },
            Data::Bytes(bytes),
        )
    }
    fn pin_file(
        &mut self,
        directory: &Path,
        prefix: &str,
        artifact: &SegmentArtifact,
    ) -> Result<(), Status> {
        file_name(&artifact.file)?;
        if self.files.len() as u32 + 1 >= self.limits.max_files {
            return Err(exhausted());
        }
        let path = directory.join(&artifact.file);
        if !std::fs::symlink_metadata(&path).map_err(storage)?.is_file() {
            return Err(fail("backup artifact must be a regular file"));
        }
        let file = File::open(&path).map_err(|error| {
            if matches!(error.raw_os_error(), Some(23 | 24)) {
                Status::resource_exhausted(format!("backup file pin limit: {error}"))
            } else {
                storage(error)
            }
        })?;
        let stat = file.metadata().map_err(storage)?;
        if !stat.is_file() || stat.len() != artifact.bytes {
            return Err(Status::data_loss(
                "backup artifact length or type differs from its manifest",
            ));
        }
        self.add(
            SnapshotArtifact {
                file: format!("{prefix}/{}", artifact.file),
                bytes: artifact.bytes,
                sha256: artifact.sha256.clone(),
            },
            Data::File(file),
        )
    }
    fn manifest(
        &self,
        source: crate::pb::storage::DocumentCatalogCheckpoint,
    ) -> SourceBackupManifest {
        let mut artifacts: Vec<_> = self.files.iter().map(|f| f.metadata.clone()).collect();
        artifacts.push(SnapshotArtifact {
            file: SOURCE.into(),
            bytes: source.bytes,
            sha256: source.sha256.iter().map(|b| format!("{b:02x}")).collect(),
        });
        artifacts.sort_by(|a, b| a.file.cmp(&b.file));
        SourceBackupManifest {
            format_version: 1,
            source: Some(source),
            indexes: self.indexes.clone(),
            artifacts,
            manifest_sha256: Vec::new(),
        }
    }
}

/// Create `path` as a new directory private to this process's user. The
/// returned guard removes it again unless the caller keeps it.
pub(super) fn create_private_directory(
    path: &Path,
    occupied: &'static str,
) -> Result<OwnedDirectory, Status> {
    let mut builder = std::fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::AlreadyExists {
            Status::already_exists(occupied)
        } else {
            storage(error)
        }
    })?;
    Ok(OwnedDirectory {
        path: path.to_path_buf(),
        keep: false,
    })
}

/// Make a fully written bundle durable: sync every directory created
/// under it, then write the completion marker and sync the bundle and its
/// parent.
pub(super) fn commit_bundle(
    destination: &Path,
    directories: &BTreeSet<PathBuf>,
    completion: &[u8],
    parent: &File,
) -> Result<(), Status> {
    for directory in directories.iter().rev() {
        File::open(directory)
            .and_then(|f| f.sync_all())
            .map_err(storage)?;
    }
    // Commit marker is last. Its presence alone is not proof: readers must
    // validate its digest and every referenced artifact before restoration.
    {
        let mut file = new_file(&destination.join(COMPLETION))?;
        file.write_all(completion).map_err(storage)?;
        file.sync_all().map_err(storage)?;
    }
    File::open(destination)
        .and_then(|f| f.sync_all())
        .map_err(storage)?;
    parent.sync_all().map_err(storage)
}

pub(super) struct OwnedDirectory {
    pub(super) path: PathBuf,
    pub(super) keep: bool,
}
impl Drop for OwnedDirectory {
    fn drop(&mut self) {
        if !self.keep {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}

//! Verify a complete incoming bundle in private local storage before activation.
use super::backup::{
    charge, exhausted, new_file, validate_limits, OwnedDirectory, COMPLETION, SOURCE,
};
use super::*;
use crate::pb::{
    storage::{SourceBackupManifest, SourceRestoreRequest},
    SnapshotArtifact,
};
use crate::segments::{OpenedSegmentSet, SegmentSetManifest};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::{Read, Write},
};

/// Verified private staging, with no writer or serving activation. Dropping the
/// result closes the read-only source and removes only its staging directory.
/// The caller must retain this owner until a separate authority operation can
/// consume it. A process crash can leave a bundle, never an active writer.
pub struct VerifiedSourceRestore {
    manifest: SourceBackupManifest,
    // Close database handles before the directory guard performs cleanup.
    _source: redb::ReadOnlyDatabase,
    _source_lock: File,
    owned: OwnedDirectory,
}
impl VerifiedSourceRestore {
    pub fn manifest(&self) -> &SourceBackupManifest {
        &self.manifest
    }
    pub fn directory(&self) -> &Path {
        &self.owned.path
    }
}

fn damaged(message: impl Into<String>) -> Status {
    Status::data_loss(format!("source restore: {}", message.into()))
}
fn relative(name: &str) -> Result<(), Status> {
    if name.is_empty()
        || name.len() > 4096
        || name.contains(['\\', '\0', ':'])
        || name
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err(damaged("artifact path is not a canonical relative path"));
    }
    Ok(())
}
fn hash_valid(hash: &str) -> bool {
    hash.len() == 64
        && hash
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Each component is opened relative to a held directory, with symlink
/// following disabled. Nonblocking open prevents a replaced FIFO from hanging
/// before the regular-file check. The initial root is owner-selected.
struct Directory(File);
#[cfg(unix)]
impl Directory {
    fn open(path: &Path) -> Result<Self, Status> {
        use std::os::unix::fs::OpenOptionsExt;
        OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path)
            .map(Self)
            .map_err(|e| damaged(format!("open bundle directory: {e}")))
    }
    fn file(&self, name: &str) -> Result<File, Status> {
        use std::os::fd::{AsRawFd, FromRawFd};
        relative(name)?;
        let mut held = self.0.try_clone().map_err(storage)?;
        let mut parts = name.split('/').peekable();
        while let Some(part) = parts.next() {
            let component =
                std::ffi::CString::new(part).map_err(|_| damaged("NUL in artifact path"))?;
            let directory = parts.peek().is_some();
            let flags = libc::O_RDONLY
                | libc::O_NOFOLLOW
                | libc::O_CLOEXEC
                | libc::O_NONBLOCK
                | if directory { libc::O_DIRECTORY } else { 0 };
            // SAFETY: held remains open, component is NUL-terminated, and flags
            // do not request creation. A successful descriptor transfers once.
            let fd = unsafe { libc::openat(held.as_raw_fd(), component.as_ptr(), flags) };
            if fd < 0 {
                return Err(damaged(format!(
                    "open artifact {name:?}: {}",
                    std::io::Error::last_os_error()
                )));
            }
            // SAFETY: openat returned a new owned descriptor above.
            held = unsafe { File::from_raw_fd(fd) };
            let metadata = held.metadata().map_err(storage)?;
            if (directory && !metadata.is_dir()) || (!directory && !metadata.is_file()) {
                return Err(damaged(
                    "artifact must be a regular file below real directories",
                ));
            }
        }
        Ok(held)
    }
}
#[cfg(not(unix))]
impl Directory {
    fn open(_: &Path) -> Result<Self, Status> {
        Err(Status::unimplemented(
            "local restore requires descriptor-relative file opens on this platform",
        ))
    }
    fn file(&self, _: &str) -> Result<File, Status> {
        Err(Status::unimplemented(
            "local restore requires descriptor-relative file opens on this platform",
        ))
    }
}

fn read_bounded(root: &Directory, name: &str, limit: u64) -> Result<Vec<u8>, Status> {
    let mut file = root.file(name)?;
    let length = file.metadata().map_err(storage)?.len();
    if length > limit {
        return Err(exhausted());
    }
    let mut bytes = Vec::new();
    (&mut file)
        .take(limit + 1)
        .read_to_end(&mut bytes)
        .map_err(storage)?;
    if bytes.len() as u64 > limit {
        return Err(exhausted());
    }
    if bytes.len() as u64 != length {
        return Err(damaged("metadata changed during its read"));
    }
    Ok(bytes)
}

// Count the repeated envelope records before prost allocates them. Unknown
// fields or noncanonical encodings are refused by the full roundtrip below.
fn envelope_budget(bytes: &[u8], maximum: u32) -> Result<(), Status> {
    fn varint(bytes: &mut &[u8]) -> Result<u64, Status> {
        let mut value = 0u64;
        for shift in (0..70).step_by(7) {
            let (first, rest) = bytes
                .split_first()
                .ok_or_else(|| damaged("truncated manifest varint"))?;
            *bytes = rest;
            if shift == 63 && *first > 1 {
                return Err(damaged("manifest varint overflow"));
            }
            value |= u64::from(first & 0x7f) << shift;
            if first & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(damaged("manifest varint overflow"))
    }
    fn walk(
        mut bytes: &[u8],
        maximum: u32,
        envelope: bool,
        counts: &mut [u32; 3],
    ) -> Result<(), Status> {
        while !bytes.is_empty() {
            let tag = varint(&mut bytes)?;
            if tag >> 3 == 0 {
                return Err(damaged("zero manifest field number"));
            }
            match tag & 7 {
                0 => {
                    varint(&mut bytes)?;
                }
                1 | 5 => {
                    let length = if tag & 7 == 1 { 8 } else { 4 };
                    bytes = bytes
                        .get(length..)
                        .ok_or_else(|| damaged("truncated manifest field"))?;
                }
                2 => {
                    let length = usize::try_from(varint(&mut bytes)?).map_err(|_| exhausted())?;
                    let payload = bytes
                        .get(..length)
                        .ok_or_else(|| damaged("truncated manifest field"))?;
                    bytes = &bytes[length..];
                    if envelope && tag >> 3 == 2 {
                        // Repeated encodings of the singular source field merge
                        // in protobuf. Count their combined checkpoint indexes.
                        walk(payload, maximum, false, counts)?;
                    }
                    let slot = match (envelope, tag >> 3) {
                        (true, 3) => Some(0),
                        (true, 4) => Some(1),
                        (false, 3) => Some(2),
                        _ => None,
                    };
                    if let Some(slot) = slot {
                        counts[slot] = counts[slot].checked_add(1).ok_or_else(exhausted)?;
                        if counts[slot] > maximum {
                            return Err(exhausted());
                        }
                    }
                }
                _ => return Err(damaged("unsupported manifest wire type")),
            }
        }
        Ok(())
    }
    walk(bytes, maximum, true, &mut [0; 3])
}

fn manifest(
    root: &Directory,
    request: &SourceRestoreRequest,
) -> Result<(SourceBackupManifest, Vec<u8>), Status> {
    let limits = request.limits.as_ref().expect("validated limits");
    let bytes = read_bounded(root, COMPLETION, limits.metadata_bytes)?;
    envelope_budget(&bytes, limits.max_files)?;
    let manifest: SourceBackupManifest = decode(&bytes)?;
    if manifest.format_version != 1 || manifest.encode_to_vec() != bytes {
        return Err(damaged("unsupported or noncanonical completion manifest"));
    }
    let mut unsigned = manifest.clone();
    let digest = std::mem::take(&mut unsigned.manifest_sha256);
    if digest != request.expected_manifest_sha256
        || sha256::digest(&unsigned.encode_to_vec()).as_slice() != digest
    {
        return Err(damaged(
            "completion digest differs from the owner's expectation",
        ));
    }
    let source = manifest
        .source
        .as_ref()
        .ok_or_else(|| damaged("completion has no source checkpoint"))?;
    let header = source
        .header
        .as_ref()
        .ok_or_else(|| damaged("checkpoint has no header"))?;
    validate_current_header(header)?;
    if source.format_version != 1
        || header.collection != request.collection
        || header.history_id != request.history_id
    {
        return Err(damaged(
            "checkpoint differs from the expected collection or history",
        ));
    }
    if source.sha256.len() != 32
        || source.bytes == 0
        || source.records == 0
        || manifest.indexes.len() != source.indexes.len()
        || manifest.artifacts.is_empty()
    {
        return Err(damaged("invalid source checkpoint or index inventory"));
    }
    Ok((manifest, bytes))
}

fn metadata(
    root: &Directory,
    artifact: &SnapshotArtifact,
    total: &mut u64,
    maximum: u64,
) -> Result<Vec<u8>, Status> {
    charge(total, artifact.bytes, maximum)?;
    let bytes = read_bounded(root, &artifact.file, artifact.bytes.min(maximum))?;
    if bytes.len() as u64 != artifact.bytes || sha256::hex_digest(&bytes) != artifact.sha256 {
        return Err(damaged("index metadata differs from its inventory"));
    }
    Ok(bytes)
}

fn inventory(
    root: &Directory,
    manifest: &SourceBackupManifest,
    completion_bytes: u64,
    limits: &crate::pb::storage::SourceBackupLimits,
) -> Result<(), Status> {
    let source = manifest.source.as_ref().expect("validated source");
    let mut total = completion_bytes;
    let mut metadata_bytes = completion_bytes;
    let mut entries = BTreeMap::new();
    let mut last = "";
    for artifact in &manifest.artifacts {
        relative(&artifact.file)?;
        if artifact.file.as_str() <= last
            || !hash_valid(&artifact.sha256)
            || artifact.file == COMPLETION
        {
            return Err(damaged(
                "artifact inventory is not sorted, unique and checksummed",
            ));
        }
        last = &artifact.file;
        charge(&mut total, artifact.bytes, limits.max_bytes)?;
        entries.insert(artifact.file.as_str(), artifact);
    }
    if total > limits.max_bytes || entries.len() > limits.max_files as usize {
        return Err(exhausted());
    }
    let source_file = entries
        .get(SOURCE)
        .ok_or_else(|| damaged("inventory omits sources.redb"))?;
    let source_digest: &[u8; 32] = source
        .sha256
        .as_slice()
        .try_into()
        .map_err(|_| damaged("source checkpoint checksum has an invalid length"))?;
    if source_file.bytes != source.bytes || source_file.sha256 != sha256::to_hex(source_digest) {
        return Err(damaged(
            "source checkpoint checksum or length differs from its inventory",
        ));
    }
    let mut required = BTreeSet::from([SOURCE.to_string()]);
    let mut prior_key: &[u8] = &[];
    for (ordinal, (index, state)) in manifest.indexes.iter().zip(&source.indexes).enumerate() {
        let directory = format!("indexes/{ordinal:08}/index.segments");
        if index.index_key.is_empty()
            || index.index_key.as_slice() <= prior_key
            || index.index_key != state.index_key
            || index.directory != directory
        {
            return Err(damaged(
                "index inventory differs from the ordered source journal",
            ));
        }
        prior_key = &index.index_key;
        let path = format!("{directory}/segments.json");
        let entry = entries
            .get(path.as_str())
            .ok_or_else(|| damaged("index manifest is missing"))?;
        let bytes = metadata(root, entry, &mut metadata_bytes, limits.metadata_bytes)?;
        let set: SegmentSetManifest =
            serde_json::from_slice(&bytes).map_err(|e| damaged(format!("index manifest: {e}")))?;
        if serde_json::to_vec(&set).map_err(storage)? != bytes
            || sha256::digest(&bytes).as_slice() != state.committed_manifest_sha256
            || set.epoch != index.catalog_epoch
        {
            return Err(damaged("index manifest differs from its journal or epoch"));
        }
        let owner = crate::pb::storage::SourceIndexOwner {
            format_version: 1,
            history_id: source.header.as_ref().unwrap().history_id.clone(),
            index_key: index.index_key.clone(),
            collection: source.header.as_ref().unwrap().collection.clone(),
        };
        match &set.source_owner {
            Some(held) if held.decode().map_err(damaged)? == owner => {}
            None if state.committed_sequence == 0 && set.segments.is_empty() => {}
            _ => return Err(damaged("index manifest belongs to another source owner")),
        }
        required.insert(path);
        for segment in &set.segments {
            relative(&segment.segment_id)?;
            if segment.segment_id.contains('/') {
                return Err(damaged("segment id is not one component"));
            }
            let prefix = format!("{directory}/segments/{}", segment.segment_id);
            let path = format!("{prefix}/segment.json");
            if !required.insert(path.clone()) {
                return Err(damaged("duplicate segment metadata"));
            }
            let entry = entries
                .get(path.as_str())
                .ok_or_else(|| damaged("segment metadata is missing"))?;
            let bytes = metadata(root, entry, &mut metadata_bytes, limits.metadata_bytes)?;
            if serde_json::to_vec(segment).map_err(storage)? != bytes {
                return Err(damaged("segment metadata differs from index manifest"));
            }
            for artifact in [
                &segment.vector,
                &segment.exact_vectors,
                &segment.bm25,
                &segment.live_docs,
            ] {
                if artifact.file.is_empty() {
                    continue;
                }
                relative(&artifact.file)?;
                if artifact.file.contains('/') {
                    return Err(damaged("segment artifact is not one component"));
                }
                let path = format!("{prefix}/{}", artifact.file);
                let entry = entries
                    .get(path.as_str())
                    .ok_or_else(|| damaged("segment artifact is missing"))?;
                if entry.bytes != artifact.bytes
                    || entry.sha256 != artifact.sha256
                    || !required.insert(path)
                {
                    return Err(damaged(
                        "segment artifact has conflicting metadata or roles",
                    ));
                }
            }
        }
    }
    if required.len() != entries.len()
        || required
            .iter()
            .any(|name| !entries.contains_key(name.as_str()))
    {
        return Err(damaged("inventory contains unreferenced artifacts"));
    }
    Ok(())
}

impl DocumentCatalog {
    /// Stage and verify one local bundle against independently supplied identity
    /// and digest. This grants no network export, permission restoration or
    /// active writer. The caller supplies a trusted, private destination parent.
    pub fn stage_backup_restore(
        bundle: &Path,
        destination: &Path,
        request: &SourceRestoreRequest,
    ) -> Result<VerifiedSourceRestore, Status> {
        let limits = request
            .limits
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("restore needs explicit budgets"))?;
        validate_limits(limits)?;
        if request.expected_manifest_sha256.len() != 32 || !valid_history_id(&request.history_id) {
            return Err(Status::invalid_argument(
                "restore needs an expected digest, collection and history identity",
            ));
        }
        let input = Directory::open(bundle)?;
        let source_root = std::fs::canonicalize(bundle).map_err(storage)?;
        let parent = destination
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let parent = std::fs::canonicalize(parent).map_err(storage)?;
        let name = destination.file_name().ok_or_else(|| {
            Status::invalid_argument("restore destination needs a directory name")
        })?;
        let destination = parent.join(name);
        if destination.starts_with(&source_root) {
            return Err(Status::failed_precondition(
                "restore destination must be outside the incoming bundle",
            ));
        }
        let (manifest, completion) = manifest(&input, request)?;
        inventory(&input, &manifest, completion.len() as u64, limits)?;
        let parent_file = File::open(&parent).map_err(storage)?;
        let mut builder = std::fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(&destination).map_err(|e| {
            if e.kind() == std::io::ErrorKind::AlreadyExists {
                Status::already_exists("restore destination already exists")
            } else {
                storage(e)
            }
        })?;
        let owned = OwnedDirectory {
            path: destination.clone(),
            keep: false,
        };
        let mut directories = BTreeSet::from([destination.clone()]);
        let mut buffer = [0u8; 64 << 10];
        for artifact in &manifest.artifacts {
            let mut input = input.file(&artifact.file)?;
            if input.metadata().map_err(storage)?.len() != artifact.bytes {
                return Err(damaged("artifact length differs from its inventory"));
            }
            let path = destination.join(&artifact.file);
            let parent = path.parent().expect("validated relative artifact");
            std::fs::create_dir_all(parent).map_err(storage)?;
            let mut ancestor = parent;
            while ancestor != destination {
                directories.insert(ancestor.to_path_buf());
                ancestor = ancestor.parent().unwrap();
            }
            let mut output = new_file(&path)?;
            let mut count = 0u64;
            let mut hash = sha256::Sha256::new();
            loop {
                let n = input.read(&mut buffer).map_err(storage)?;
                if n == 0 {
                    break;
                }
                count = count.checked_add(n as u64).ok_or_else(exhausted)?;
                if count > artifact.bytes {
                    return Err(damaged("artifact grew while being copied"));
                }
                hash.update(&buffer[..n]);
                output.write_all(&buffer[..n]).map_err(storage)?;
            }
            if count != artifact.bytes || sha256::to_hex(&hash.finalize()) != artifact.sha256 {
                return Err(damaged("artifact failed its length or checksum"));
            }
            output.sync_all().map_err(storage)?;
        }
        let source_lock = File::open(destination.join(SOURCE)).map_err(storage)?;
        source_lock.try_lock_shared().map_err(|e| {
            Status::failed_precondition(format!("restore source read lock unavailable: {e}"))
        })?;
        let mut builder = Database::builder();
        builder.set_cache_size(CACHE_BYTES);
        let source = builder
            .open_read_only(destination.join(SOURCE))
            .map_err(storage)?;
        let checkpoint = CatalogCheckpoint::from_read(
            source.begin_read().map_err(storage)?,
            limits.metadata_bytes as usize,
            None,
        )?;
        let expected = manifest.source.as_ref().expect("validated checkpoint");
        if checkpoint.metadata().header != expected.header
            || checkpoint.metadata().indexes != expected.indexes
            || checkpoint.record_count()? != expected.records
        {
            return Err(damaged(
                "copied source differs from its checkpoint metadata",
            ));
        }
        checkpoint.verify_source_history(
            &destination.join(".source-audit.redb"),
            limits.source_batch_bytes as usize,
            limits.max_bytes,
        )?;
        drop(checkpoint);
        for index in &manifest.indexes {
            let set =
                OpenedSegmentSet::open(destination.join(&index.directory)).map_err(damaged)?;
            for i in 0..set.len() {
                set.bm25(i)
                    .verify_integrity()
                    .map_err(|e| damaged(format!("BM25 integrity: {e}")))?;
                if let Some(exact) = set.exact_vectors(i) {
                    exact
                        .verify_payload()
                        .map_err(|e| damaged(format!("exact-vector integrity: {e}")))?;
                }
            }
        }
        for directory in directories.iter().rev() {
            File::open(directory)
                .and_then(|f| f.sync_all())
                .map_err(storage)?;
        }
        let mut marker = new_file(&destination.join(COMPLETION))?;
        marker.write_all(&completion).map_err(storage)?;
        marker.sync_all().map_err(storage)?;
        File::open(&destination)
            .and_then(|f| f.sync_all())
            .map_err(storage)?;
        parent_file.sync_all().map_err(storage)?;
        Ok(VerifiedSourceRestore {
            manifest,
            _source: source,
            _source_lock: source_lock,
            owned,
        })
    }
}

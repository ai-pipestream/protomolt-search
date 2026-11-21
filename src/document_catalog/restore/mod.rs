//! Verify a complete incoming bundle in private local storage before activation.
use super::backup::{
    charge, commit_bundle, create_private_directory, exhausted, new_file, validate_limits,
    OwnedDirectory, COMPLETION, SOURCE,
};
use super::*;
use crate::pb::storage::SourceBackupLimits;
use crate::pb::{
    storage::{SourceBackupManifest, SourceRestoreRequest},
    SnapshotArtifact,
};
use crate::segments::{OpenedSegmentSet, SegmentSetManifest};
use std::path::PathBuf;
use std::{
    collections::{BTreeMap, BTreeSet},
    io::{Read, Write},
};

mod input;
mod inventory;
mod manifest;

use input::Directory;
use inventory::inventory;
use manifest::manifest;

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
        let (parent, destination) = resolve_destination(bundle, destination)?;
        let (manifest, completion) = manifest(&input, request)?;
        inventory(&input, &manifest, completion.len() as u64, limits)?;
        let parent_file = File::open(&parent).map_err(storage)?;
        let owned = create_private_directory(&destination, "restore destination already exists")?;
        let directories = copy_artifacts(&input, &manifest, &destination)?;
        let (source_lock, source) = open_verified_source(&destination, &manifest, limits)?;
        verify_indexes(&destination, &manifest)?;
        commit_bundle(&destination, &directories, &completion, &parent_file)?;
        Ok(VerifiedSourceRestore {
            manifest,
            _source: source,
            _source_lock: source_lock,
            owned,
        })
    }
}

/// The canonical parent and path of the staging directory, which must lie
/// outside the incoming bundle.
fn resolve_destination(bundle: &Path, destination: &Path) -> Result<(PathBuf, PathBuf), Status> {
    let source_root = std::fs::canonicalize(bundle).map_err(storage)?;
    let parent = destination
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let parent = std::fs::canonicalize(parent).map_err(storage)?;
    let name = destination
        .file_name()
        .ok_or_else(|| Status::invalid_argument("restore destination needs a directory name"))?;
    let destination = parent.join(name);
    if destination.starts_with(&source_root) {
        return Err(Status::failed_precondition(
            "restore destination must be outside the incoming bundle",
        ));
    }
    Ok((parent, destination))
}

/// Copy every listed artifact into the staging directory, checking its
/// length and digest as it is written. Returns the directories created.
fn copy_artifacts(
    input: &Directory,
    manifest: &SourceBackupManifest,
    destination: &Path,
) -> Result<BTreeSet<PathBuf>, Status> {
    let mut directories = BTreeSet::from([destination.to_path_buf()]);
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
        let mut hash = turbo_sha256::Sha256::new();
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
        if count != artifact.bytes || hash.finish().to_hex() != artifact.sha256 {
            return Err(damaged("artifact failed its length or checksum"));
        }
        output.sync_all().map_err(storage)?;
    }
    Ok(directories)
}

/// Open the staged source read-only under a shared lock and check it
/// against the completion manifest's checkpoint, including the full audit
/// of its history. Returns the lock file and the database it guards.
fn open_verified_source(
    destination: &Path,
    manifest: &SourceBackupManifest,
    limits: &SourceBackupLimits,
) -> Result<(File, redb::ReadOnlyDatabase), Status> {
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
    Ok((source_lock, source))
}

/// Open every staged index and verify its BM25 and exact-vector payloads.
fn verify_indexes(destination: &Path, manifest: &SourceBackupManifest) -> Result<(), Status> {
    for index in &manifest.indexes {
        let set = OpenedSegmentSet::open(destination.join(&index.directory)).map_err(damaged)?;
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
    Ok(())
}

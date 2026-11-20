//! Inbound repository snapshot staging, validation, and segment catalog cutover.

use crate::node::*;

#[path = "segment_cutover.rs"]
mod segment_cutover;
#[path = "segment_prepare.rs"]
mod segment_prepare;

use segment_prepare::{PreparedSegments, SegmentPaths};

fn check_pending_compaction(guard: &ShardState) -> Result<(), Status> {
    if guard.pending_compaction.is_some() {
        return Err(Status::failed_precondition(
            "a compaction cutover is pending its closing flush on this shard; call Flush \
             before installing a snapshot",
        ));
    }
    Ok(())
}

impl NodeServiceImpl {
    /// The snapshot supersedes the log: fsync and retire the current
    /// generation, open gen-(g+1) with the installed image's provider
    /// state (same bucket geometry), and mark where it came from.
    /// Records before this point describe the OLD shard contents.
    pub(in crate::node) fn rotate_wal_after_install(
        guard: &mut ShardState,
        config: &NodeConfig,
        path: &Path,
        counts: (u64, u64),
    ) -> Result<(), Status> {
        if guard.wal.is_none() {
            return Ok(());
        }
        let source_generation = guard.wal.as_ref().map_or(0, WalWriter::generation);
        // The installed image is state this fresh log does NOT
        // contain: record it as preexisting so the reshard tool
        // refuses a log-only replay that would drop the image.
        let mut manifest =
            wal_manifest(guard.index.as_ref(), config, source_generation + 1, counts);
        let previous = guard.wal.as_ref().expect("checked above").manifest();
        manifest.bucket_bits = previous.bucket_bits;
        manifest.bucket_count = previous.bucket_count;
        let wal_err = |e: std::io::Error| Status::internal(format!("wal rotate: {e}"));
        let wal = guard.wal.as_mut().expect("checked above");
        wal.flush().map_err(wal_err)?;
        *wal = WalWriter::create(&wal::wal_dir(path), manifest).map_err(wal_err)?;
        wal.append(wal_record::Op::Snapshot(SnapshotMarker {
            source_generation,
        }))
        .map_err(wal_err)?;
        wal.flush().map_err(wal_err)?;
        Ok(())
    }

    /// Stage a repository directory into the staging generation dir:
    /// the manifest and every artifact it names, copied as they are.
    /// Verification happens in [`Self::install_staged_repository`].
    pub(in crate::node) fn stage_from_directory(
        source: &Path,
        tmp_dir: &Path,
    ) -> Result<(RepositoryManifest, String), Status> {
        let (manifest, sha) = repo::read_manifest(source).map_err(|e| {
            Status::invalid_argument(format!("repository {}: {e}", source.display()))
        })?;
        let io_err = |what: &Path, e: std::io::Error| {
            Status::internal(format!("stage {}: {e}", what.display()))
        };
        std::fs::create_dir_all(tmp_dir).map_err(|e| io_err(tmp_dir, e))?;
        std::fs::copy(
            source.join(repo::MANIFEST_FILE),
            tmp_dir.join(repo::MANIFEST_FILE),
        )
        .map_err(|e| io_err(&source.join(repo::MANIFEST_FILE), e))?;
        for artifact in &manifest.artifacts {
            let from = source.join(&artifact.file);
            let to = tmp_dir.join(&artifact.file);
            if let Some(parent) = to.parent() {
                std::fs::create_dir_all(parent).map_err(|e| io_err(parent, e))?;
            }
            std::fs::copy(&from, &to).map_err(|e| {
                Status::invalid_argument(format!(
                    "artifact {:?} is unreadable at {}: {e}",
                    artifact.file,
                    from.display()
                ))
            })?;
        }
        Ok((manifest, sha))
    }

    /// Verify a staged repository against its manifest and install its
    /// segment catalog. Refuses, by name and before touching the live
    /// shard: a size or digest mismatch, another shard's slot offset or
    /// collection.
    pub fn install_staged_repository(
        &self,
        tmp_dir: &Path,
        manifest: &RepositoryManifest,
    ) -> Result<InstallSnapshotResponse, Status> {
        read_shard(&self.state).check_index_owned_write()?;
        repo::verify_artifacts(tmp_dir, manifest).map_err(Status::invalid_argument)?;
        if manifest.slot_offset != self.config.slot_offset {
            return Err(Status::failed_precondition(format!(
                "snapshot was exported from a shard at slot offset {}, this shard serves {}; \
                 a repository image installs only on the shard it describes",
                manifest.slot_offset, self.config.slot_offset
            )));
        }
        if manifest.collection != self.config.collection {
            return Err(Status::failed_precondition(format!(
                "snapshot belongs to collection {:?}, this shard serves {:?}",
                manifest.collection, self.config.collection
            )));
        }
        self.apply_segment_snapshot(tmp_dir, manifest)
    }

    /// Validate the incoming generation before taking the catalog publication fence.
    pub(in crate::node) fn apply_segment_snapshot(
        &self,
        tmp_dir: &Path,
        manifest: &RepositoryManifest,
    ) -> Result<InstallSnapshotResponse, Status> {
        let _seal = self.seal_lock.lock().expect("seal lock poisoned");
        {
            let guard = read_shard(&self.state);
            guard.check_index_owned_write()?;
        }
        let path = self
            .config
            .index_path
            .as_ref()
            .expect("handler requires index_path");
        let staged = SegmentPaths::staged(tmp_dir);
        let prepared = PreparedSegments::open(&self.config, &staged)?;
        prepared.check_claims(manifest)?;
        let mut guard = write_shard(&self.state);
        guard.check_index_owned_write()?;
        check_pending_compaction(&guard)?;
        prepared.check_provider(&guard)?;
        drop(prepared);
        let result =
            crate::segments::SegmentCatalog::with_generation_lock(&segments_root(path), || {
                segment_cutover::install(self, &mut guard, &staged, path, manifest)
            })
            .map_err(Status::failed_precondition)??;
        segment_cutover::cleanup_retired(tmp_dir);
        Ok(result)
    }
}

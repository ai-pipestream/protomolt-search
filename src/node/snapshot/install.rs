//! Inbound snapshot image staging, validation, and generation cutover.

use crate::node::*;

#[path = "image_prepare.rs"]
mod image_prepare;
#[path = "segment_cutover.rs"]
mod segment_cutover;
#[path = "segment_prepare.rs"]
mod segment_prepare;

use image_prepare::{check_pending_compaction, PreparedImage};
use segment_prepare::{PreparedSegments, SegmentPaths};

impl NodeServiceImpl {
    /// Receive one snapshot image into the staging generation directory
    /// (`vector.index`, `vectors.f32`, `documents.bm25`, and `live-docs.bin`
    /// when declared).
    /// The byte counts in the manifest split the stream; every file is synced
    /// before the caller swaps anything. Returns
    /// with the staging dir complete or not at all — on error the caller
    /// removes it.
    pub(in crate::node) async fn receive_image(
        inbound: &mut Streaming<SnapshotChunk>,
        manifest: &SnapshotManifest,
        tmp_dir: &Path,
    ) -> Result<(), Status> {
        use tokio::io::AsyncWriteExt;
        if (manifest.multi_res_vector_bytes == 0) != manifest.multi_res_declaration.is_empty()
            || manifest.multi_res_declaration.len() > 4096
        {
            return Err(Status::invalid_argument(
                "invalid retained vector snapshot declaration",
            ));
        }
        let io_err = |e: std::io::Error| Status::internal(format!("snapshot receive: {e}"));
        tokio::fs::create_dir_all(tmp_dir).await.map_err(io_err)?;
        let paths = [
            generation_vector(tmp_dir),
            generation_exact_vectors(tmp_dir),
            generation_bm25(tmp_dir),
            generation_live_docs(tmp_dir),
            tmp_dir.join(crate::multi_res::FILE_NAME),
        ];
        let lengths = [
            manifest.vector_bytes,
            manifest.exact_vector_bytes,
            manifest.bm25_bytes,
            manifest.live_docs_bytes,
            manifest.multi_res_vector_bytes,
        ];
        let mut files = Vec::with_capacity(paths.len());
        for (path, length) in paths.iter().zip(lengths) {
            files.push(if length > 0 {
                Some(tokio::fs::File::create(path).await.map_err(io_err)?)
            } else {
                None
            });
        }
        let mut written = [0u64; 5];
        while let Some(chunk) = inbound.message().await? {
            let Some(snapshot_chunk::Payload::Data(data)) = chunk.payload else {
                return Err(Status::invalid_argument(
                    "SnapshotChunk after the manifest must carry data",
                ));
            };
            let mut remaining = data.as_slice();
            for (i, file) in files.iter_mut().enumerate() {
                let take = (lengths[i] - written[i]).min(remaining.len() as u64) as usize;
                if take > 0 {
                    file.as_mut()
                        .expect("positive length opened its file")
                        .write_all(&remaining[..take])
                        .await
                        .map_err(io_err)?;
                    written[i] += take as u64;
                    remaining = &remaining[take..];
                }
            }
            if !remaining.is_empty() {
                return Err(Status::invalid_argument(
                    "snapshot carries more data than declared",
                ));
            }
        }
        if written != lengths {
            return Err(Status::invalid_argument(format!(
                "truncated snapshot: received {written:?}, declared {lengths:?}"
            )));
        }
        for file in files.iter_mut().flatten() {
            file.sync_all().await.map_err(io_err)?;
        }
        if manifest.multi_res_vector_bytes > 0 {
            let path = crate::multi_res_storage::declaration_path(&paths[4]);
            let mut file = tokio::fs::File::create(path).await.map_err(io_err)?;
            file.write_all(&manifest.multi_res_declaration)
                .await
                .map_err(io_err)?;
            file.sync_all().await.map_err(io_err)?;
        }
        Ok(())
    }

    /// Validate every image artifact before replacing the serving generation.
    pub(in crate::node) fn apply_snapshot(
        &self,
        tmp_dir: &Path,
        with_exact_vectors: bool,
        with_bm25: bool,
        with_live_docs: bool,
    ) -> Result<InstallSnapshotResponse, Status> {
        read_shard(&self.state).check_legacy_mutation()?;
        let path = self
            .config
            .index_path
            .as_ref()
            .expect("handler requires index_path");
        let prepared =
            PreparedImage::open(self, tmp_dir, with_exact_vectors, with_bm25, with_live_docs)?;
        let mut guard = write_shard(&self.state);
        guard.check_legacy_mutation()?;
        prepared.check_provider(&guard)?;
        check_pending_compaction(&guard)?;
        let snap = Self::adopt_generation(path, tmp_dir, true)?;
        let counts = prepared.activate(&mut guard, &snap, with_exact_vectors, with_bm25)?;
        Self::rotate_wal_after_install(&mut guard, &self.config, path, counts)?;
        Ok(InstallSnapshotResponse {
            path: generation_vector(&snap).display().to_string(),
            num_vectors: counts.0,
            num_documents: counts.1,
            manifest: None,
        })
    }

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

    /// Verify a staged repository against its manifest and install it
    /// through the layout's install path (the single-image path is the
    /// same [`Self::apply_snapshot`] the client stream takes). Refuses,
    /// by name and before touching the live shard: a size or digest
    /// mismatch, another shard's slot offset or collection, a layout
    /// this shard cannot adopt.
    pub fn install_staged_repository(
        &self,
        tmp_dir: &Path,
        manifest: &RepositoryManifest,
    ) -> Result<InstallSnapshotResponse, Status> {
        read_shard(&self.state).check_legacy_mutation()?;
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
        if manifest.layout == LAYOUT_SEGMENTS {
            return self.apply_segment_snapshot(tmp_dir, manifest);
        }
        if manifest.artifact("vector.index").is_none() {
            if manifest.vector_rows != 0
                || manifest.document_rows != 0
                || manifest.live_rows != 0
                || manifest.dim != 0
                || !manifest.scoring_fingerprint.is_empty()
                || manifest.artifact("vectors.f32").is_some()
                || manifest.artifact("documents.bm25").is_none()
                || generation_vector(tmp_dir).exists()
                || generation_exact_vectors(tmp_dir).exists()
            {
                return Err(Status::failed_precondition(
                    "a snapshot without a provider image must be an empty binding image",
                ));
            }
            let image = Bm25Reader::open(&generation_bm25(tmp_dir)).map_err(|e| {
                Status::invalid_argument(format!("invalid empty binding image: {e}"))
            })?;
            if image.next_doc_id() != 0 || image.binding().is_none() {
                return Err(Status::failed_precondition(
                    "a snapshot without a provider image requires a zero-row mapped binding",
                ));
            }
            crate::segments::SegmentBinding::encode(image.binding().expect("checked above"))
                .map_err(Status::invalid_argument)?;
        }
        {
            let guard = read_shard(&self.state);
            if Self::served_layout(&guard) == LAYOUT_SEGMENTS && physical_rows(&guard) > 0 {
                return Err(Status::failed_precondition(format!(
                    "this shard serves a segment catalog with {} rows; a single-image snapshot \
                     installs only on a single-image shard or an empty one (layouts do not mix)",
                    physical_rows(&guard)
                )));
            }
        }
        for name in ["vectors.multi", "vectors.multi.json"] {
            if manifest.artifact(name).is_some()
                != tmp_dir
                    .join(name)
                    .try_exists()
                    .map_err(|error| Status::internal(error.to_string()))?
            {
                return Err(Status::invalid_argument(
                    "retained snapshot files disagree with repository inventory",
                ));
            }
        }
        let mut response = self.apply_snapshot(
            tmp_dir,
            manifest.artifact("vectors.f32").is_some(),
            manifest.artifact("documents.bm25").is_some(),
            manifest.artifact("live-docs.bin").is_some(),
        )?;
        response.manifest = Some(manifest.to_pb());
        Ok(response)
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
            guard.check_legacy_mutation()?;
            if self.config.layout != Layout::Segments || guard.generation.is_some() {
                return Err(Status::failed_precondition(
                    "segment-catalog snapshots require a segment-layout shard, not --layout=single-image",
                ));
            }
        }
        let path = self
            .config
            .index_path
            .as_ref()
            .expect("handler requires index_path");
        let staged = SegmentPaths::staged(tmp_dir);
        let prepared = PreparedSegments::open(&self.config, &staged)?;
        let mut guard = write_shard(&self.state);
        guard.check_legacy_mutation()?;
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

    /// The atomic generation swap shared by snapshot install and
    /// compaction cutover (`docs/mutations.md`): the previous generation
    /// aside (if any), the staged directory into place, both inside ONE
    /// directory rename each. The staged dir's own entries are fsynced
    /// first (the files' sync_all covered bytes and inodes, not the names
    /// pointing at them), and the parent is fsynced after so the swap
    /// itself survives a crash; the window between the two renames is
    /// covered by [`recover_generation`]. With `retire_old` the previous
    /// generation is removed at once (a snapshot install); a compaction
    /// keeps it until its closing flush and removes it then. Returns the
    /// active generation directory.
    pub(crate) fn adopt_generation(
        index_path: &Path,
        staged: &Path,
        retire_old: bool,
    ) -> Result<PathBuf, Status> {
        let snap = generation_dir(index_path);
        let old = generation_old_dir(index_path);
        crate::postings::fsync_parent(&generation_vector(staged))
            .map_err(|e| Status::internal(format!("fsync staging {}: {e}", staged.display())))?;
        if snap.exists() {
            std::fs::rename(&snap, &old)
                .map_err(|e| Status::internal(format!("retire {}: {e}", old.display())))?;
        }
        if let Err(e) = std::fs::rename(staged, &snap) {
            // Best-effort rollback so startup recovery sees a clean state.
            if old.exists() && !snap.exists() {
                let _ = std::fs::rename(&old, &snap);
            }
            return Err(Status::internal(format!("install {}: {e}", snap.display())));
        }
        if retire_old {
            let _ = std::fs::remove_dir_all(&old);
        }
        crate::postings::fsync_parent(&snap)
            .map_err(|e| Status::internal(format!("fsync {}: {e}", snap.display())))?;
        Ok(snap)
    }
}

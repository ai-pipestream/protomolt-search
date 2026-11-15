//! Snapshot repository export and chunked file streaming.

use crate::node::*;

/// Send `path` onto a snapshot stream in [`SNAPSHOT_STREAM_CHUNK`]
/// pieces. False when the receiver hung up or the file could not be read
/// (the receiver then sees a truncated artifact and refuses it by hash).
pub(in crate::node) async fn stream_file(
    tx: &mpsc::Sender<Result<SnapshotChunk, Status>>,
    path: &Path,
) -> bool {
    use tokio::io::AsyncReadExt;
    let mut file = match tokio::fs::File::open(path).await {
        Ok(file) => file,
        Err(e) => {
            let _ = tx
                .send(Err(Status::internal(format!(
                    "read {}: {e}",
                    path.display()
                ))))
                .await;
            return false;
        }
    };
    let mut buf = vec![0u8; SNAPSHOT_STREAM_CHUNK];
    loop {
        match file.read(&mut buf).await {
            Ok(0) => return true,
            Ok(n) => {
                let chunk = SnapshotChunk {
                    payload: Some(snapshot_chunk::Payload::Data(buf[..n].to_vec())),
                };
                if tx.send(Ok(chunk)).await.is_err() {
                    return false;
                }
            }
            Err(e) => {
                let _ = tx
                    .send(Err(Status::internal(format!(
                        "read {}: {e}",
                        path.display()
                    ))))
                    .await;
                return false;
            }
        }
    }
}

/// Snapshot stream chunk: 1 MiB keeps messages far under
/// [`crate::MAX_MESSAGE_BYTES`] while amortizing per-message overhead.
pub const SNAPSHOT_STREAM_CHUNK: usize = 1024 * 1024;

impl NodeServiceImpl {
    /// The layout the shard's files form: the segment catalog, or one
    /// image plus sidecars (an installed generation is always the latter).
    pub(in crate::node) fn served_layout(guard: &ShardState) -> &'static str {
        if guard.generation.is_none()
            && (matches!(guard.bm25, Some(Bm25Shard::Segmented(_)))
                || guard
                    .index
                    .as_ref()
                    .is_some_and(|index| index.as_segmented().is_some()))
        {
            LAYOUT_SEGMENTS
        } else {
            LAYOUT_SINGLE_IMAGE
        }
    }

    /// Per-field analysis fingerprints of the BM25 store, in field-table
    /// order (empty without a store).
    pub fn analysis_fingerprints(&self) -> Vec<u64> {
        let guard = read_shard(&self.state);
        Self::analysis_fingerprints_of(&guard)
    }

    pub(in crate::node) fn analysis_fingerprints_of(guard: &ShardState) -> Vec<u64> {
        guard.bm25.as_ref().map_or_else(Vec::new, |bm25| {
            (0..bm25.field_count())
                .map(|f| bm25.analysis_fingerprint(f))
                .collect()
        })
    }

    /// Sealed immutable segments of the shard: the catalog's parts for
    /// the segment layout, one for a disk-resident single image, zero
    /// for a heap builder or an empty shard.
    pub fn immutable_segments(&self) -> u32 {
        let guard = read_shard(&self.state);
        match guard.bm25.as_ref() {
            Some(Bm25Shard::Segmented(shard)) => shard.sealed_parts() as u32,
            Some(Bm25Shard::Resident(_)) => 1,
            _ => 0,
        }
    }

    /// Export a consistent snapshot: flush, copy the current generation into
    /// `directory` under the seal mutex and shard read lock, and write the
    /// snapshot manifest, retrying bounded flushes on intervening mutations.
    pub fn export_snapshot_blocking(
        &self,
        directory: &Path,
    ) -> Result<ExportSnapshotResponse, Status> {
        read_shard(&self.state).check_legacy_mutation()?;
        let index_path = self.config.index_path.clone().ok_or_else(|| {
            Status::failed_precondition(
                "shard has no persistence path (index_path); a snapshot export needs one",
            )
        })?;
        if directory.as_os_str().is_empty() {
            return Err(Status::invalid_argument("ExportSnapshot needs a directory"));
        }
        std::fs::create_dir_all(directory).map_err(|e| {
            Status::internal(format!("create repository {}: {e}", directory.display()))
        })?;
        let entries = std::fs::read_dir(directory)
            .map_err(|e| Status::internal(format!("read {}: {e}", directory.display())))?
            .count();
        if entries > 0 {
            return Err(Status::invalid_argument(format!(
                "repository directory {} is not empty ({entries} entries); a snapshot is \
                 exported into an empty directory only",
                directory.display()
            )));
        }
        self.flush_index()?;
        let started = std::time::Instant::now();
        let mut attempts = 0u32;
        let (manifest, bytes) = loop {
            attempts += 1;
            if let Some(copy) = self.export_if_flushed(&index_path, directory)? {
                break copy;
            }
            if attempts >= 8 {
                return Err(Status::aborted(format!(
                    "shard kept writing through {attempts} flushes; a snapshot copies a flushed generation, retry when ingest pauses"
                )));
            }
            self.flush_index()?;
        };
        let copy_millis = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
        let (manifest_path, manifest_sha256) = repo::write_manifest(directory, &manifest)
            .map_err(|e| Status::internal(format!("write manifest: {e}")))?;
        crate::postings::fsync_parent(&manifest_path)
            .map_err(|e| Status::internal(format!("fsync {}: {e}", directory.display())))?;
        Ok(ExportSnapshotResponse {
            manifest: Some(manifest.to_pb()),
            manifest_path: manifest_path.display().to_string(),
            manifest_sha256,
            copy_millis,
            bytes,
        })
    }

    /// Check after acquiring the seal fence and state guard. A mutation
    /// between Flush and copy requires another flush, including without WAL.
    pub(in crate::node) fn export_if_flushed(
        &self,
        index_path: &Path,
        directory: &Path,
    ) -> Result<Option<(RepositoryManifest, u64)>, Status> {
        let _sealing = self.seal_lock.lock().expect("seal lock poisoned");
        let guard = read_shard(&self.state);
        guard.check_legacy_mutation()?;
        guard.check_catalog_publication()?;
        if !guard.files_current || guard.wal.as_ref().is_some_and(|wal| wal.is_dirty()) {
            return Ok(None);
        }
        self.export_locked(&guard, index_path, directory).map(Some)
    }

    /// The copy itself, under whichever lock the caller holds.
    pub(in crate::node) fn export_locked(
        &self,
        guard: &ShardState,
        index_path: &Path,
        directory: &Path,
    ) -> Result<(RepositoryManifest, u64), Status> {
        guard.check_legacy_mutation()?;
        let layout = Self::served_layout(guard);
        let mut sources: Vec<(String, PathBuf)> = Vec::new();
        if layout == LAYOUT_SEGMENTS {
            let root = segments_root(index_path);
            let Some(Bm25Shard::Segmented(shard)) = guard.bm25.as_ref() else {
                return Err(Status::internal("segment snapshot has no serving catalog"));
            };
            sources.push((
                format!("{CATALOG_DIR}/segments.json"),
                root.join("segments.json"),
            ));
            // Compaction can stage new segments while the serving catalog is
            // pinned. Copy only its published artifacts, never staging files or
            // retired segments discovered by walking the directory tree.
            for metadata in &shard.snapshot().manifest().segments {
                let directory = format!("segments/{}", metadata.segment_id);
                sources.push((
                    format!("{CATALOG_DIR}/{directory}/segment.json"),
                    root.join(&directory).join("segment.json"),
                ));
                for artifact in [
                    Some(&metadata.vector),
                    Some(&metadata.exact_vectors),
                    Some(&metadata.bm25),
                    Some(&metadata.live_docs),
                    metadata.multi_res.as_ref(),
                ]
                .into_iter()
                .flatten()
                {
                    if !artifact.file.is_empty() {
                        let name = format!("{directory}/{}", artifact.file);
                        sources.push((format!("{CATALOG_DIR}/{name}"), root.join(name)));
                    }
                }
            }
            for (name, path) in [
                ("vector.index", index_path.to_path_buf()),
                ("vectors.f32", exact_vector_sidecar_path(index_path)),
                ("live-docs.bin", live_docs_sidecar_path(index_path)),
            ] {
                if name == "vectors.f32" && guard.index.as_ref().is_some_and(VectorIndex::is_empty)
                {
                    continue;
                }
                // A standalone image from before the first seal is obsolete
                // once sealed vector images define the serving generation.
                if (name == "vector.index" || name == "vectors.f32")
                    && matches!(guard.bm25.as_ref(), Some(Bm25Shard::Segmented(shard))
                        if (0..shard.snapshot().len()).any(|i| shard.snapshot().vector(i).is_some()))
                {
                    continue;
                }
                if path.is_file() {
                    sources.push((name.to_string(), path));
                }
            }
        } else {
            let (vector, exact, bm25) = storage_paths(index_path, guard.generation.as_ref());
            let live = live_docs_storage_path(index_path, guard.generation.as_ref());
            let retained = crate::multi_res::storage_path(index_path, guard.generation.as_ref());
            for (name, path) in [
                ("vector.index", vector),
                ("vectors.f32", exact),
                ("documents.bm25", bm25),
                ("live-docs.bin", live),
                (
                    "vectors.multi.json",
                    crate::multi_res_storage::declaration_path(&retained),
                ),
                ("vectors.multi", retained),
            ] {
                if path.is_file() {
                    sources.push((name.to_string(), path));
                }
            }
        }
        let mut artifacts = Vec::with_capacity(sources.len());
        let mut bytes = 0u64;
        for (name, source) in &sources {
            let artifact = repo::copy_and_hash(source, &directory.join(name), name)
                .map_err(|e| Status::internal(format!("copy {}: {e}", source.display())))?;
            bytes += artifact.bytes;
            artifacts.push(artifact);
        }
        let mut synced = std::collections::BTreeSet::new();
        for artifact in &artifacts {
            let path = directory.join(&artifact.file);
            if let Some(parent) = path.parent() {
                if synced.insert(parent.to_path_buf()) {
                    crate::postings::fsync_parent(&path).map_err(|e| {
                        Status::internal(format!("fsync {}: {e}", parent.display()))
                    })?;
                }
            }
        }
        let (backend_kind, scoring_fingerprint, dim) = match guard.index.as_ref() {
            Some(index) => {
                let descriptor = index.descriptor();
                (
                    descriptor.backend_kind,
                    descriptor.scoring_fingerprint,
                    index.dim_opt().unwrap_or(0) as u32,
                )
            }
            None => (self.config.vector_backend.clone(), String::new(), 0),
        };
        let physical = physical_rows(guard);
        let deleted = guard.live_docs.deleted_count().min(physical);
        let (wal_generation, wal_high_watermark, wal_clocked) =
            guard.wal.as_ref().map_or((0, 0, false), |wal| {
                (wal.generation(), wal.high_watermark(), true)
            });
        Ok((
            RepositoryManifest {
                format_version: if artifacts.iter().any(|artifact: &repo::Artifact| {
                    artifact.file == "vectors.multi" || artifact.file.ends_with("/vectors.multi")
                }) {
                    repo::RETAINED_FORMAT_VERSION
                } else {
                    repo::FORMAT_VERSION
                },
                layout: layout.to_string(),
                backend_kind,
                scoring_fingerprint,
                dim,
                slot_offset: self.config.slot_offset,
                collection: self.config.collection.clone(),
                vector_rows: guard.index.as_ref().map_or(0, |i| i.len() as u64),
                document_rows: guard
                    .bm25
                    .as_ref()
                    .map_or(0, |b| u64::from(b.next_doc_id())),
                live_rows: physical - deleted,
                analysis_fingerprints: Self::analysis_fingerprints_of(guard),
                wal_generation,
                wal_high_watermark,
                wal_clocked,
                artifacts,
            },
            bytes,
        ))
    }
}

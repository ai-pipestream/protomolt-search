//! The durable shard flush: log before data, then each image in turn.

use super::*;

/// Refuse an exact-vector sidecar whose shape disagrees with the rows it rescores.
fn check_exact_sidecar_shape(guard: &ShardState) -> Result<(), Status> {
    if let Some(exact) = guard.exact_vectors.as_ref() {
        if let Some(index) = guard.index.as_ref() {
            if exact.len() != index.len() || exact.dim() != index.dim_opt() {
                return Err(Status::failed_precondition(format!(
                    "exact-vector sidecar shape {:?}x{} does not match provider shape {:?}x{}",
                    exact.dim(),
                    exact.len(),
                    index.dim_opt(),
                    index.len()
                )));
            }
        } else if guard
            .bm25
            .as_ref()
            .is_some_and(|bm25| exact.len() != bm25.next_doc_id() as usize)
        {
            return Err(Status::failed_precondition(
                "exact-vector sidecar row count does not match product document rows",
            ));
        }
    }
    Ok(())
}

/// Publish the old-reader refusal before the first retained image can
/// become durable, including when this shard deliberately has no WAL.
fn publish_retained_reader_gate(
    guard: &mut ShardState,
    live_docs_path: &Path,
) -> Result<(), Status> {
    if !guard.multi_res_vectors.is_empty()
        && !guard.live_docs.requires_retained_vectors()
        && guard
            .index
            .as_ref()
            .and_then(VectorIndex::as_segmented)
            .is_none()
    {
        guard.live_docs = guard
            .live_docs
            .clone()
            .with_retained_vectors()
            .write(live_docs_path, physical_rows(guard))
            .map_err(|error| Status::internal(format!("publish retained reader gate: {error}")))?;
    }
    Ok(())
}

/// Write the single-image vector index, or a segmented provider's empty tail.
fn write_vector_image(guard: &mut ShardState, vector_path: &Path) -> Result<(), Status> {
    if let Some(index) = guard.index.as_mut() {
        if let Some(provider) = index.as_segmented() {
            // With no rows, no sealed image carries the provider state.
            // Persist the empty tail so reopening without WAL retains its
            // dimension and calibration, including after full compaction.
            if index.is_empty() {
                provider.tail().write(vector_path).map_err(|e| {
                    Status::internal(format!("write {}: {e}", vector_path.display()))
                })?;
            }
        } else {
            index
                .prepare()
                .map_err(|e| Status::internal(format!("prepare {}: {e}", vector_path.display())))?;
            index
                .write(vector_path)
                .map_err(|e| Status::internal(format!("write {}: {e}", vector_path.display())))?;
        }
    }
    Ok(())
}

/// Adopt the sealed exact-vector view, or write the single-image sidecar.
fn persist_exact_vectors(guard: &mut ShardState, exact_path: &Path) -> Result<(), Status> {
    let sealed_exact = guard
        .index
        .as_ref()
        .and_then(VectorIndex::as_segmented)
        .filter(|provider| provider.tail().is_empty() && provider.frozen().is_none())
        .map(|provider| {
            let dim = provider
                .tail()
                .dim_opt()
                .ok_or_else(|| Status::internal("segmented provider has no dimension"))?;
            ExactVectorStore::from_segments(provider.snapshot(), dim)
                .map_err(|error| Status::internal(format!("sealed exact-vector view: {error}")))
        })
        .transpose()?;
    if let Some(exact) = sealed_exact {
        guard.exact_vectors = Some(exact);
    } else if guard
        .index
        .as_ref()
        .and_then(VectorIndex::as_segmented)
        .is_some()
    {
        // Writes accepted during the seal remain in the private tail. The
        // catalog already durably owns the sealed prefix; no full copy is
        // needed. files_current below prevents exporting this newer tail.
    } else if let Some(exact) = guard.exact_vectors.as_ref() {
        let mapped = exact
            .write(exact_path)
            .map_err(|e| Status::internal(format!("write {}: {e}", exact_path.display())))?;
        guard.exact_vectors = Some(mapped);
    }
    Ok(())
}

/// Write the retained vectors of a single-image shard, then the tombstones.
fn persist_retained_and_live_docs(
    guard: &mut ShardState,
    config_path: &Path,
    live_docs_path: &Path,
) -> Result<(), Status> {
    let physical_rows = physical_rows(guard);
    if guard
        .index
        .as_ref()
        .and_then(VectorIndex::as_segmented)
        .is_none()
    {
        let payload = crate::multi_res::storage_path(config_path, guard.generation.as_ref());
        crate::multi_res_storage::write(
            &guard.multi_res_vectors,
            &payload,
            physical_rows,
            guard.live_docs.requires_retained_vectors(),
        )
        .map_err(|error| Status::internal(format!("persist retained vectors: {error}")))?;
    }
    if !guard.multi_res_vectors.is_empty()
        && guard
            .index
            .as_ref()
            .and_then(VectorIndex::as_segmented)
            .is_none()
    {
        guard.live_docs = guard.live_docs.clone().with_retained_vectors();
    }
    guard.live_docs = guard
        .live_docs
        .write(live_docs_path, physical_rows)
        .map_err(|e| Status::internal(format!("write {}: {e}", live_docs_path.display())))?;
    Ok(())
}

impl NodeServiceImpl {
    /// Persist the index to its configured path, if any. Shared by `Flush` and shutdown.
    pub fn flush_index(&self) -> Result<FlushResponse, Status> {
        let Some(config_path) = self.config.index_path.clone() else {
            return self.flush_unpersisted();
        };
        self.sync_wal_before_images()?;
        // A segmented shard seals its tail with the lock free (see
        // `seal_tail`); the single-image writes below hold it.
        let sealed = self.seal_tail()?;
        let mut guard = write_shard(&self.state);
        guard.check_catalog_publication()?;
        let num_vectors = guard.index.as_ref().map_or(0, |i| i.len() as u64);
        let num_documents = guard.bm25.as_ref().map_or(0, |b| b.doc_count());
        let (vector_path, exact_path, bm25_path) =
            storage_paths(&config_path, guard.generation.as_ref());
        let live_docs_path = live_docs_storage_path(&config_path, guard.generation.as_ref());
        check_exact_sidecar_shape(&guard)?;
        publish_retained_reader_gate(&mut guard, &live_docs_path)?;
        write_vector_image(&mut guard, &vector_path)?;
        persist_exact_vectors(&mut guard, &exact_path)?;
        persist_retained_and_live_docs(&mut guard, &config_path, &live_docs_path)?;
        self.persist_documents(&mut guard, &bm25_path)?;
        let written = sealed
            || guard.index.is_some()
            || guard.exact_vectors.is_some()
            || guard.bm25.is_some();
        self.complete_flush(&mut guard)?;
        Ok(FlushResponse {
            path: vector_path.display().to_string(),
            num_vectors,
            num_documents,
            written,
        })
    }

    /// A shard with no index path prepares its in-memory image and writes nothing.
    fn flush_unpersisted(&self) -> Result<FlushResponse, Status> {
        let mut guard = write_shard(&self.state);
        if let Some(index) = guard.index.as_mut() {
            if index.as_segmented().is_none() {
                index
                    .prepare()
                    .map_err(|e| Status::internal(format!("prepare index: {e}")))?;
            }
        }
        Ok(FlushResponse {
            path: String::new(),
            num_vectors: guard.index.as_ref().map_or(0, |i| i.len() as u64),
            num_documents: guard.bm25.as_ref().map_or(0, |b| b.doc_count()),
            written: false,
        })
    }

    /// Log before data: fsync the WAL BEFORE the index images are
    /// written, so a crash between the two leaves the log a superset
    /// of the on-disk indexes — never the reverse. An index image
    /// whose records the log lost would silently drop those records
    /// from every future replay (reshard, recovery).
    fn sync_wal_before_images(&self) -> Result<(), Status> {
        let mut guard = write_shard(&self.state);
        guard.check_catalog_publication()?;
        if guard.mapped_binding.is_some() && guard.bm25.is_none() {
            guard.bm25 = Some(self.new_builder(guard.generation.as_ref())?);
        }
        if let Some(wal) = guard.wal.as_mut() {
            wal.flush()
                .map_err(|e| Status::internal(format!("wal fsync {}: {e}", wal.dir().display())))?;
        }
        Ok(())
    }

    /// Save the builder as v3 and immediately reopen it disk-resident:
    /// after Flush a shard holds no postings or texts in heap.
    /// Already-resident shards have nothing to write.
    /// The binding persists with the store bytes (the kinded binding
    /// entry), so the flushed file carries exactly what the shard is
    /// bound to.
    fn persist_documents(&self, guard: &mut ShardState, bm25_path: &Path) -> Result<(), Status> {
        let binding = guard.mapped_binding.clone();
        let derived = stored_derived(&self.config);
        let built = match guard.bm25.as_mut() {
            Some(Bm25Shard::Building(store)) => {
                store.set_binding(binding);
                store.set_derived(derived);
                store
                    .save(bm25_path)
                    .map_err(|e| Status::internal(format!("write {}: {e}", bm25_path.display())))?;
                true
            }
            Some(Bm25Shard::Spilling(builder)) => {
                builder.set_binding(binding);
                builder.set_derived(derived);
                builder
                    .finish(bm25_path)
                    .map_err(|e| Status::internal(format!("write {}: {e}", bm25_path.display())))?;
                true
            }
            _ => false,
        };
        if built {
            guard.bm25 = Some(
                Bm25Reader::open(bm25_path)
                    .map(Bm25Shard::Resident)
                    .map_err(|e| {
                        Status::internal(format!("reopen {}: {e}", bm25_path.display()))
                    })?,
            );
            // The stats are the same numbers after a flush, but a
            // Spilling shard was refusing TermStats until now — the
            // bump is what tells a cache to come look again.
            guard.advance_stats_epoch();
        }
        Ok(())
    }

    /// Complete a pending compaction cutover, log the flush marker, and
    /// record whether the files now hold everything the shard serves.
    fn complete_flush(&self, guard: &mut ShardState) -> Result<(), Status> {
        // Finalize any pending compaction cutover now that its images are durable.
        if let Some(pending) = guard.pending_compaction.take() {
            pending
                .complete()
                .map_err(|e| Status::internal(format!("complete compaction cutover: {e}")))?;
        }
        // Durability point reached: the log was fsynced above, then the
        // indexes hit disk. The marker records that a flush happened;
        // its own fsync failing degrades the log rather than un-flushing
        // the indexes (they are already durable and consistent).
        wal_append_or_degrade(&mut guard.wal, wal_record::Op::Flush(FlushMarker {}));
        if let Some(wal) = guard.wal.as_mut() {
            if let Err(e) = wal.flush() {
                eprintln!("wal: post-flush marker fsync failed: {e}");
            }
        }
        if let Some(notify) = &self.flush_notify {
            notify.notify_one();
        }
        // Writes arriving while the frozen segment was being written may
        // occupy a new tail. They require another flush before snapshot copy.
        guard.files_current = !matches!(guard.bm25.as_ref(), Some(Bm25Shard::Segmented(shard))
            if shard.tail().next_doc_id() != 0 || shard.frozen().is_some())
            && !guard
                .index
                .as_ref()
                .and_then(VectorIndex::as_segmented)
                .is_some_and(|provider| !provider.tail().is_empty() || provider.frozen().is_some());
        Ok(())
    }
}

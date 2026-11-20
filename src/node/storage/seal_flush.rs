//! The durable shard flush: log before data, then each image in turn.

use super::*;

#[cfg(test)]
#[path = "seal_flush_tests.rs"]
mod tests;

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

/// The segmented provider of a persisted shard; its vectors live only in
/// the catalog and its tail.
fn persisted_provider(guard: &ShardState) -> Result<Option<&SegmentedProvider>, Status> {
    match guard.index.as_ref() {
        None => Ok(None),
        Some(index) => index.as_segmented().map(Some).ok_or_else(|| {
            Status::internal("a persisted shard serves a vector index outside its segment catalog")
        }),
    }
}

/// With no rows, no sealed image carries the provider state: persist the
/// empty tail so reopening without WAL retains its dimension and
/// calibration, including after full compaction.
fn write_empty_tail(guard: &ShardState, vector_path: &Path) -> Result<(), Status> {
    if let Some(provider) = persisted_provider(guard)? {
        if provider.tail().is_empty() && guard.index.as_ref().is_some_and(VectorIndex::is_empty) {
            provider
                .tail()
                .write(vector_path)
                .map_err(|e| Status::internal(format!("write {}: {e}", vector_path.display())))?;
        }
    }
    Ok(())
}

/// Adopt the sealed exact-vector view once the tail is sealed. Writes
/// accepted during the seal remain in the private tail: the catalog
/// durably owns the sealed prefix, and `files_current` keeps a snapshot
/// from exporting the newer tail.
fn adopt_sealed_exact_vectors(guard: &mut ShardState) -> Result<(), Status> {
    let Some(provider) = persisted_provider(guard)? else {
        return Ok(());
    };
    if !provider.tail().is_empty() || provider.frozen().is_some() {
        return Ok(());
    }
    let dim = provider
        .tail()
        .dim_opt()
        .ok_or_else(|| Status::internal("segmented provider has no dimension"))?;
    let exact = ExactVectorStore::from_segments(provider.snapshot(), dim)
        .map_err(|error| Status::internal(format!("sealed exact-vector view: {error}")))?;
    guard.exact_vectors = Some(exact);
    Ok(())
}

/// The durable row prefix: rows the sealed catalog owns, never the
/// volatile heap tail. A frozen-but-unpublished part is not durable
/// either, so its base bounds the prefix while one exists.
fn sealed_rows(guard: &ShardState) -> u64 {
    match guard.bm25.as_ref() {
        Some(Bm25Shard::Segmented(shard)) => u64::from(
            shard
                .frozen()
                .map_or_else(|| shard.tail_base(), |(base, _, _)| base),
        ),
        _ => match guard.index.as_ref().and_then(VectorIndex::as_segmented) {
            Some(provider) => provider
                .frozen()
                .map_or_else(|| provider.tail_base(), |(base, _, _)| base)
                as u64,
            None => physical_rows(guard),
        },
    }
}

/// Write the tombstones over the sealed prefix only. Sizing the file to
/// every physical row would claim unsealed tail rows the catalog and the
/// truncated WAL cannot recreate after a crash, and reopen would then
/// refuse the overlay forever. Tail tombstones stay in memory (and in
/// the WAL) until their rows seal.
fn persist_live_docs(guard: &mut ShardState, live_docs_path: &Path) -> Result<(), Status> {
    let sealed = sealed_rows(guard);
    // `write` masks and bounds the bitmap to `sealed` words itself
    // without mutating the in-memory overlay, so no sliced copy.
    let _ = guard
        .live_docs
        .write(live_docs_path, sealed)
        .map_err(|e| Status::internal(format!("write {}: {e}", live_docs_path.display())))?;
    Ok(())
}

impl NodeServiceImpl {
    /// Persist the index to its configured path, if any. Shared by `Flush` and shutdown.
    pub fn flush_index(&self) -> Result<FlushResponse, Status> {
        self.flush_index_with(TailSeal::Any)
    }

    /// The flush that closes a compaction cutover: a tail caught between a
    /// row's documents and its vectors stays in place for the next flush
    /// instead of refusing the cutover's completion.
    pub(crate) fn flush_closing_cutover(&self) -> Result<FlushResponse, Status> {
        self.flush_index_with(TailSeal::Deferred)
    }

    fn flush_index_with(&self, mode: TailSeal) -> Result<FlushResponse, Status> {
        let Some(config_path) = self.config.index_path.clone() else {
            return self.flush_unpersisted();
        };
        self.sync_wal_before_images()?;
        // The seal writes its segment with the lock free (see `seal_tail`);
        // the sidecar writes below hold it.
        let sealed = self.seal_tail_with(mode)?;
        let mut guard = write_shard(&self.state);
        guard.check_catalog_publication()?;
        let num_vectors = guard.index.as_ref().map_or(0, |i| i.len() as u64);
        let num_documents = guard.bm25.as_ref().map_or(0, |b| b.doc_count());
        // The sealed view comes first: a documents-only segment holds
        // provider rows the tail's exact store never saw, and the view
        // covers them; the check then sees the store the shard serves.
        adopt_sealed_exact_vectors(&mut guard)?;
        check_exact_sidecar_shape(&guard)?;
        write_empty_tail(&guard, &config_path)?;
        persist_live_docs(&mut guard, &live_docs_sidecar_path(&config_path))?;
        let written = sealed
            || guard.index.is_some()
            || guard.exact_vectors.is_some()
            || guard.bm25.is_some();
        self.complete_flush(&mut guard)?;
        Ok(FlushResponse {
            path: config_path.display().to_string(),
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
        // A shard built with `NodeServiceImpl::new` has no catalog until its
        // first document; a flush opens it so vectors seal into segments.
        self.ensure_bm25_builder(&mut guard)?;
        if let Some(wal) = guard.wal.as_mut() {
            wal.flush()
                .map_err(|e| Status::internal(format!("wal fsync {}: {e}", wal.dir().display())))?;
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

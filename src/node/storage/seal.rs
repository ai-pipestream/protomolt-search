//! Segment tail freezing, segment writing, and durable shard flush.

use crate::node::*;

/// The shard-owner gRPC service. Cheap to clone (state is shared).
/// What a seal in flight writes out (`NodeServiceImpl::seal_tail`):
/// the frozen part's rows and shared artifacts, gathered under the
/// write lock, written with no lock held.
pub(in crate::node) struct SealPlan {
    pub(in crate::node) retained: crate::multi_res::MultiResVectors,
    pub(in crate::node) base: usize,
    pub(in crate::node) rows: usize,
    pub(in crate::node) documents: Arc<Bm25Store>,
    pub(in crate::node) live: LiveDocs,
    pub(in crate::node) image: Option<Arc<VectorIndex>>,
    pub(in crate::node) backend_kind: String,
    pub(in crate::node) catalog: crate::segments::SegmentCatalog,
    pub(in crate::node) stage: PathBuf,
    pub(in crate::node) segment_id: String,
    pub(in crate::node) generation: u64,
}

impl NodeServiceImpl {
    /// A new, empty vector index in the shard's layout: the tail image of
    /// a segmented provider over the catalog's sealed images when the
    /// documents are segmented, else the index as created. Every path
    /// that creates the shard's first index goes through here, so a
    /// calibrated-then-ingested segmented shard seals its vectors like
    /// an ingested-then-calibrated one.
    pub(crate) fn adopt_layout(
        bm25: Option<&Bm25Shard>,
        created: VectorIndex,
    ) -> Result<VectorIndex, Status> {
        match bm25 {
            Some(Bm25Shard::Segmented(g)) => {
                let provider = SegmentedProvider::open(g.snapshot().clone(), created)
                    .map_err(|e| Status::failed_precondition(format!("{e}")))?;
                Ok(VectorIndex::from_provider(provider))
            }
            _ => Ok(created),
        }
    }

    /// Seal a segmented shard's tail into a new catalog segment
    /// (`docs/immutable-segments.md`) in three steps. Under the shard's
    /// write lock the tail freezes into a read-only part and a fresh
    /// tail starts after it. With no lock held, the frozen part's
    /// documents, vectors, FP32 rows, and live bitmap are written,
    /// hashed, fsynced, and appended to the catalog with one manifest
    /// swap. Under the write lock again the shard adopts the published
    /// snapshot. Queries serve the frozen part throughout and ingest
    /// continues into the new tail. Seals on one shard run one at a
    /// time, and a seal that failed after freezing is finished by the
    /// next attempt. The WAL is untouched: sealing changes the on-disk
    /// layout, not the log's history. Returns whether a segment was
    /// written.
    pub(crate) fn seal_tail(&self) -> Result<bool, Status> {
        self.seal_tail_with(false)
    }

    /// [`Self::seal_tail`] for a sealer that runs beside live ingest
    /// (compaction): with a vector provider configured it refuses, under
    /// the same write lock that would freeze the tail, a tail whose
    /// documents outnumber their vectors. Such a tail is a two-call
    /// append between its calls; freezing it would seal the documents
    /// alone and the vectors that follow would be refused. The refusal
    /// carries the misaligned-tail message, which the caller retries.
    pub(crate) fn seal_tail_paired(&self) -> Result<bool, Status> {
        self.seal_tail_with(true)
    }

    pub(in crate::node) fn seal_tail_with(&self, paired: bool) -> Result<bool, Status> {
        let _one_at_a_time = self.seal_lock.lock().expect("seal lock poisoned");
        let Some(plan) = self.freeze_tail(paired)? else {
            return Ok(false);
        };
        let outcome = self.write_segment(&plan);
        let _ = std::fs::remove_dir_all(&plan.stage);
        let published = outcome?;
        let mut guard = write_shard(&self.state);
        let Some(Bm25Shard::Segmented(shard)) = guard.bm25.as_mut() else {
            return Err(Status::internal(
                "the segmented shard changed layout while a seal was in flight",
            ));
        };
        shard
            .republish(published.clone())
            .map_err(|e| Status::internal(format!("republish segments: {e}")))?;
        if let Some(provider) = guard.index.as_mut().and_then(VectorIndex::as_segmented_mut) {
            provider
                .republish(published)
                .map_err(|e| Status::internal(format!("republish vectors: {e}")))?;
        }
        guard.advance_stats_epoch();
        drop(guard);
        // The frozen tail just went out of scope: hand its freed pages
        // back to the kernel. glibc keeps freed small chunks in the
        // arena that allocated them, and a long ingest touches a
        // different arena per stream, so without this a node's resident
        // set grows by a tail's worth per seal (measured 4 GB per
        // million rows; MALLOC_ARENA_MAX=2 in the launch environment
        // bounds the spread, this returns the rest).
        #[cfg(all(feature = "net", target_os = "linux", target_env = "gnu"))]
        // SAFETY: malloc_trim takes an integer pad and touches only the
        // allocator's own free lists; it has no preconditions.
        unsafe {
            libc::malloc_trim(0);
        }
        Ok(true)
    }

    /// Step one of a seal, under the write lock: freeze the tail (or
    /// pick up the frozen part a failed seal left) and gather what the
    /// writer needs. `None` when there is nothing to seal. `paired`
    /// refuses a document-only tail when a vector provider is configured
    /// (see [`Self::seal_tail_paired`]).
    pub(in crate::node) fn freeze_tail(&self, paired: bool) -> Result<Option<SealPlan>, Status> {
        let mut guard = write_shard(&self.state);
        guard.check_catalog_publication()?;
        if let Some(binding) = guard.mapped_binding.clone() {
            if let Some(Bm25Shard::Segmented(shard)) = guard.bm25.as_mut() {
                shard
                    .publish_binding(&binding)
                    .map_err(Status::failed_precondition)?;
            }
        }
        let ShardState {
            bm25,
            index,
            live_docs,
            multi_res_vectors,
            mapped_binding,
            ..
        } = &mut *guard;
        let Some(Bm25Shard::Segmented(shard)) = bm25.as_mut() else {
            return Ok(None);
        };
        let provider = index.as_mut().and_then(VectorIndex::as_segmented_mut);
        let (base, rows, documents) = match shard.frozen() {
            Some((base, rows, store)) => (base as usize, rows as usize, Arc::clone(store)),
            None => {
                let base = shard.tail_base() as usize;
                let docs = shard.tail().next_doc_id() as usize;
                let vectors = provider.as_deref().map_or(0, |p| p.tail().len());
                if docs == 0 && vectors == 0 {
                    return Ok(None);
                }
                let awaits_vectors = paired && provider.is_some() && vectors == 0;
                if docs != 0 && (vectors != 0 || awaits_vectors) && docs != vectors {
                    return Err(Status::failed_precondition(format!(
                        "the tail has {docs} documents and {vectors} vectors; a segment's \
                         artifacts cover the same rows, so ingest through the mapped path, \
                         which keeps them aligned, or run this shard with --layout=single-image"
                    )));
                }
                let fresh = heap_store(&self.config).map_err(Status::failed_precondition)?;
                shard.tail_mut().set_binding(mapped_binding.clone());
                let rows = docs.max(vectors);
                let store = shard
                    .freeze_tail(fresh, rows as u32)
                    .map_err(Status::internal)?;
                (base, rows, store)
            }
        };
        let image = match provider {
            Some(provider) => match provider.frozen() {
                Some((_, frozen_rows, image)) => (frozen_rows > 0).then(|| Arc::clone(image)),
                None => {
                    let held = provider.tail().len();
                    let backend = provider
                        .tail()
                        .backend_config()
                        .map_err(|e| Status::internal(format!("tail backend: {e}")))?;
                    let dim = provider
                        .tail()
                        .dim_opt()
                        .ok_or_else(|| Status::internal("segmented tail image has no dimension"))?;
                    let fresh = VectorIndex::from_backend_config(dim, &backend)
                        .map_err(|e| Status::internal(format!("fresh tail image: {e}")))?;
                    let image = provider
                        .freeze_tail(fresh, rows)
                        .map_err(|e| Status::internal(format!("freeze vectors: {e}")))?;
                    (held > 0).then_some(image)
                }
            },
            None => None,
        };
        let backend_kind = image
            .as_ref()
            .map(|i| i.descriptor().backend_kind)
            .unwrap_or_default();
        // The segment's id is its generation, the published epoch plus
        // one: monotone across seals AND across a compaction cutover,
        // which renumbers the catalog and would otherwise let a fresh
        // `seg-<count>` collide with a replaced directory the closing
        // flush has not retired yet (docs/mutations.md).
        let generation = shard.snapshot().epoch() + 1;
        Ok(Some(SealPlan {
            retained: multi_res_vectors
                .range(base as u32, rows as u32)
                .map_err(|error| Status::internal(error.to_string()))?,
            base,
            rows,
            documents,
            live: live_docs.slice(base, rows),
            image,
            backend_kind,
            catalog: shard.catalog().clone(),
            stage: shard.stage_dir(&format!("{generation:08}")),
            segment_id: format!("seg-{generation:08}"),
            generation,
        }))
    }

    /// Step two of a seal, with no lock held: write the frozen part's
    /// artifacts into the stage directory and append them to the
    /// catalog, which hashes, fsyncs, and publishes them. The FP32 rows
    /// are copied out of the exact-vector sidecar under a read lock (a
    /// sequential copy); their file is written, hashed, and fsynced
    /// after that lock is released.
    pub(in crate::node) fn write_segment(
        &self,
        plan: &SealPlan,
    ) -> Result<Arc<crate::segments::OpenedSegmentSet>, Status> {
        let _ = std::fs::remove_dir_all(&plan.stage);
        std::fs::create_dir_all(&plan.stage)
            .map_err(|e| Status::internal(format!("stage {}: {e}", plan.stage.display())))?;
        let io = |what: &str, e: std::io::Error| Status::internal(format!("seal {what}: {e}"));
        let bm25_path = plan.stage.join("documents.bm25");
        plan.documents
            .save(&bm25_path)
            .map_err(|e| io("documents", e))?;
        let live_path = plan.stage.join("live-docs.bin");
        plan.live
            .write(&live_path, plan.rows as u64)
            .map_err(|e| io("live docs", e))?;
        let (vector_path, exact_path) = match plan.image.as_deref() {
            Some(image) => {
                let vector_path = plan.stage.join("vector.index");
                image
                    .write(&vector_path)
                    .map_err(|e| Status::internal(format!("seal vectors: {e}")))?;
                let (dim, values) = {
                    let guard = read_shard(&self.state);
                    let exact = guard.exact_vectors.as_ref().ok_or_else(|| {
                        Status::failed_precondition(
                            "sealing a segment with vectors needs the exact-vector sidecar for \
                             its FP32 rows; the node has none",
                        )
                    })?;
                    if exact.len() < plan.base + plan.rows {
                        return Err(Status::failed_precondition(format!(
                            "the exact-vector sidecar has {} rows, fewer than the {} the seal \
                             covers",
                            exact.len(),
                            plan.base + plan.rows
                        )));
                    }
                    let dim = exact.dim().ok_or_else(|| {
                        Status::failed_precondition("exact-vector sidecar has no dim")
                    })?;
                    (
                        dim,
                        exact
                            .row_values(plan.base, plan.base + plan.rows)
                            .map_err(|e| io("exact rows", e))?,
                    )
                };
                let exact_path = plan.stage.join("vectors.f32");
                ExactVectorStore::from_values(dim, values)
                    .and_then(|store| store.write(&exact_path))
                    .map_err(|e| io("exact rows", e))?;
                (Some(vector_path), Some(exact_path))
            }
            None => (None, None),
        };
        let multi_res_path = if plan.retained.is_empty() {
            None
        } else {
            let path = plan.stage.join(crate::multi_res::FILE_NAME);
            plan.retained
                .write(&path, plan.rows as u64)
                .map_err(|error| io("retained vectors", error))?;
            Some(path)
        };
        let source = crate::segments::SegmentSource {
            multi_res_path: multi_res_path.as_deref(),
            segment_id: &plan.segment_id,
            generation: plan.generation,
            base_label: plan.base as u64,
            backend_kind: &plan.backend_kind,
            vector_path: vector_path.as_deref(),
            exact_vector_path: exact_path.as_deref(),
            bm25_path: &bm25_path,
            live_docs_path: &live_path,
            partition_column: None,
        };
        plan.catalog
            .append(source)
            .map_err(|e| Status::internal(format!("seal segment: {e}")))
    }

    /// Whether the tail reached `--seal-tail-docs` (documents or
    /// vectors; the mapped path grows them together).
    pub(in crate::node) fn tail_full(&self) -> bool {
        if self.config.seal_tail_docs == 0 || self.config.index_path.is_none() {
            return false;
        }
        let bound = self.config.seal_tail_docs as usize;
        let guard = read_shard(&self.state);
        let Some(Bm25Shard::Segmented(shard)) = guard.bm25.as_ref() else {
            return false;
        };
        let docs = shard.tail().next_doc_id() as usize;
        let vectors = guard
            .index
            .as_ref()
            .and_then(VectorIndex::as_segmented)
            .map_or(0, |p| p.tail().len());
        // A segment's artifacts cover the same rows, so a tail that holds
        // both documents and vectors seals only at a moment when the two
        // agree: the legacy two-call append (AddDocuments, then
        // AddVectors for the same rows) is between its calls otherwise,
        // and the seal waits for the vectors rather than sealing a
        // document-only segment the vectors could never join.
        if docs == 0 {
            vectors >= bound
        } else if guard.index.is_some() {
            // A provider is configured, so the documents' vectors are
            // coming; a document-only segment would leave them nowhere
            // to go (their call is refused by name), so the seal waits.
            docs == vectors && docs >= bound
        } else {
            docs >= bound
        }
    }

    /// Seal the tail when it reached the configured size, so a long
    /// ingest stays bounded without a flush. Blocking; the ingest loops
    /// call it through [`Self::seal_if_due`].
    pub(in crate::node) fn seal_if_due_blocking(&self) -> Result<bool, Status> {
        if self.tail_full() {
            self.seal_tail()
        } else {
            Ok(false)
        }
    }

    /// The async face of the due-check for the ingest loops: the seal
    /// itself runs on a blocking thread, with the shard lock free.
    pub(in crate::node) async fn seal_if_due(&self) -> Result<bool, Status> {
        if !self.tail_full() {
            return Ok(false);
        }
        let service = self.clone();
        tokio::task::spawn_blocking(move || service.seal_if_due_blocking())
            .await
            .map_err(|e| Status::internal(format!("seal task failed: {e}")))?
    }

    pub fn flush_index(&self) -> Result<FlushResponse, Status> {
        let Some(config_path) = self.config.index_path.clone() else {
            let mut guard = write_shard(&self.state);
            if let Some(index) = guard.index.as_mut() {
                if index.as_segmented().is_none() {
                    index
                        .prepare()
                        .map_err(|e| Status::internal(format!("prepare index: {e}")))?;
                }
            }
            return Ok(FlushResponse {
                path: String::new(),
                num_vectors: guard.index.as_ref().map_or(0, |i| i.len() as u64),
                num_documents: guard.bm25.as_ref().map_or(0, |b| b.doc_count()),
                written: false,
            });
        };
        // Log before data: fsync the WAL BEFORE the index images are
        // written, so a crash between the two leaves the log a superset
        // of the on-disk indexes — never the reverse. An index image
        // whose records the log lost would silently drop those records
        // from every future replay (reshard, recovery).
        {
            let mut guard = write_shard(&self.state);
            guard.check_catalog_publication()?;
            if guard.mapped_binding.is_some() && guard.bm25.is_none() {
                guard.bm25 = Some(self.new_builder(guard.generation.as_ref())?);
            }
            if let Some(wal) = guard.wal.as_mut() {
                wal.flush().map_err(|e| {
                    Status::internal(format!("wal fsync {}: {e}", wal.dir().display()))
                })?;
            }
        }
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
        // Publish the old-reader refusal before the first retained image can
        // become durable, including when this shard deliberately has no WAL.
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
                .write(&live_docs_path, physical_rows(&guard))
                .map_err(|error| {
                    Status::internal(format!("publish retained reader gate: {error}"))
                })?;
        }
        if let Some(index) = guard.index.as_mut() {
            if let Some(provider) = index.as_segmented() {
                // With no rows, no sealed image carries the provider state.
                // Persist the empty tail so reopening without WAL retains its
                // dimension and calibration, including after full compaction.
                if index.is_empty() {
                    provider.tail().write(&vector_path).map_err(|e| {
                        Status::internal(format!("write {}: {e}", vector_path.display()))
                    })?;
                }
            } else {
                index.prepare().map_err(|e| {
                    Status::internal(format!("prepare {}: {e}", vector_path.display()))
                })?;
                index.write(&vector_path).map_err(|e| {
                    Status::internal(format!("write {}: {e}", vector_path.display()))
                })?;
            }
        }
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
                .write(&exact_path)
                .map_err(|e| Status::internal(format!("write {}: {e}", exact_path.display())))?;
            guard.exact_vectors = Some(mapped);
        }
        let physical_rows = physical_rows(&guard);
        if guard
            .index
            .as_ref()
            .and_then(VectorIndex::as_segmented)
            .is_none()
        {
            let payload = crate::multi_res::storage_path(&config_path, guard.generation.as_ref());
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
            .write(&live_docs_path, physical_rows)
            .map_err(|e| Status::internal(format!("write {}: {e}", live_docs_path.display())))?;
        // Save the builder as v3 and immediately reopen it disk-resident:
        // after Flush a shard holds no postings or texts in heap.
        // Already-resident shards have nothing to write.
        // The binding persists with the store bytes (the kinded binding
        // entry), so the flushed file carries exactly what the shard is
        // bound to.
        let binding = guard.mapped_binding.clone();
        let derived = stored_derived(&self.config);
        let built = match guard.bm25.as_mut() {
            Some(Bm25Shard::Building(store)) => {
                store.set_binding(binding);
                store.set_derived(derived);
                store
                    .save(&bm25_path)
                    .map_err(|e| Status::internal(format!("write {}: {e}", bm25_path.display())))?;
                true
            }
            Some(Bm25Shard::Spilling(builder)) => {
                builder.set_binding(binding);
                builder.set_derived(derived);
                builder
                    .finish(&bm25_path)
                    .map_err(|e| Status::internal(format!("write {}: {e}", bm25_path.display())))?;
                true
            }
            _ => false,
        };
        if built {
            guard.bm25 = Some(
                Bm25Reader::open(&bm25_path)
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
        let written = sealed
            || guard.index.is_some()
            || guard.exact_vectors.is_some()
            || guard.bm25.is_some();
        // A compaction cutover commits here (docs/mutations.md): its
        // images are on disk now, so the marker that would roll it back
        // on restart goes, and the generation it replaced with it.
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
        Ok(FlushResponse {
            path: vector_path.display().to_string(),
            num_vectors,
            num_documents,
            written,
        })
    }
}

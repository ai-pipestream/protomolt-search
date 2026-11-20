//! Segment tail freezing, segment writing, and durable shard flush.

use crate::node::*;

#[path = "seal_flush.rs"]
mod seal_flush;
#[path = "seal_write.rs"]
mod seal_write;

/// What a seal does with a tail whose documents and vectors disagree.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::node) enum TailSeal {
    /// Seal a documents-only tail; refuse documents and vectors that
    /// disagree (a flush).
    Any,
    /// Also refuse documents whose vectors have not arrived (a sealer
    /// beside live ingest: compaction).
    Aligned,
    /// Leave a tail whose documents and vectors disagree in place for the
    /// next flush (a compaction's closing flush).
    Deferred,
}

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

/// Freeze the vector tail over the same `rows` as the documents (or pick
/// up the frozen image a failed seal left). `None` when it holds no vector.
fn freeze_vector_image(
    provider: Option<&mut SegmentedProvider>,
    rows: usize,
) -> Result<Option<Arc<VectorIndex>>, Status> {
    match provider {
        Some(provider) => match provider.frozen() {
            Some((_, frozen_rows, image)) => Ok((frozen_rows > 0).then(|| Arc::clone(image))),
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
                Ok((held > 0).then_some(image))
            }
        },
        None => Ok(None),
    }
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

    /// Seal a segmented shard's heap tail into a new catalog segment: freeze
    /// the tail under the write lock, write/hash/fsync segment artifacts and
    /// publish the manifest unlocked, then republish the new snapshot under the
    /// write lock without altering WAL history.
    pub(crate) fn seal_tail(&self) -> Result<bool, Status> {
        self.seal_tail_with(TailSeal::Any)
    }

    /// [`Self::seal_tail`] for a sealer that runs beside live ingest
    /// (compaction): with a vector provider configured it refuses, under
    /// the same write lock that would freeze the tail, a tail whose
    /// documents have no vectors yet. Freezing it would seal the documents
    /// alone and the vectors that follow would be refused.
    pub(crate) fn seal_tail_aligned(&self) -> Result<bool, Status> {
        self.seal_tail_with(TailSeal::Aligned)
    }

    pub(in crate::node) fn seal_tail_with(&self, mode: TailSeal) -> Result<bool, Status> {
        let _one_at_a_time = self.seal_lock.lock().expect("seal lock poisoned");
        let Some(plan) = self.freeze_tail(mode)? else {
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
    /// writer needs. `None` when there is nothing to seal, or when `mode`
    /// defers a tail whose documents and vectors disagree (see
    /// [`TailSeal`]).
    pub(in crate::node) fn freeze_tail(&self, mode: TailSeal) -> Result<Option<SealPlan>, Status> {
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
                let awaits_vectors = matches!(mode, TailSeal::Aligned | TailSeal::Deferred)
                    && provider.is_some()
                    && vectors == 0;
                if docs != 0 && (vectors != 0 || awaits_vectors) && docs != vectors {
                    if mode == TailSeal::Deferred {
                        return Ok(None);
                    }
                    return Err(Status::failed_precondition(format!(
                        "the tail has {docs} documents and {vectors} vectors; a segment's \
                         artifacts cover the same rows, so send AddVectors for each \
                         AddDocuments batch before flushing, or ingest through the mapped \
                         path, which keeps them aligned"
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
        let image = freeze_vector_image(provider, rows)?;
        let backend_kind = image
            .as_ref()
            .map(|i| i.descriptor().backend_kind)
            .unwrap_or_default();
        // Assign `epoch + 1` as a monotonic segment ID so post-compaction seals
        // cannot collide with unretired pre-compaction segment directories.
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
        // agree: AddVectors has not yet caught up with AddDocuments
        // otherwise, and the seal waits for the vectors rather than
        // sealing a document-only segment the vectors could never join.
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
}

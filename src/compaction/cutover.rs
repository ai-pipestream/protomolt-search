//! The WAL-path cutover: the final tail, the install, and the seal wait.

use super::*;

impl NodeServiceImpl {
    /// The tail loop and the cutover. Returns `(records applied under the
    /// lock, write-lock hold in ms, unlocked passes)`.
    pub(super) fn tail_and_cut_over(
        &self,
        pre: &Preflight,
        shadow: &mut Shadow,
        tail_bound: usize,
        analyze: &mut Analyze<'_>,
    ) -> Result<(u64, u64, u64), Status> {
        let mut tail = ClockedTail::start(&pre.gen_dir, pre.cutoff_clock);
        let mut passes = 0u64;
        let mut smallest = usize::MAX;
        let mut stalled = 0u64;
        loop {
            passes += 1;
            if passes > MAX_TAIL_PASSES {
                return Err(Status::resource_exhausted(format!(
                    "writes outpace compaction: {MAX_TAIL_PASSES} tail passes never left fewer \
                     than {tail_bound} records to apply"
                )));
            }
            let watermark = self.flush_live_wal(pre)?;
            let records = tail
                .read_through(watermark)
                .map_err(|e| Status::failed_precondition(format!("tail the live WAL: {e}")))?;
            let count = records.len();
            self.apply_pass(shadow, records, analyze)?;
            if count < tail_bound {
                break;
            }
            if count < smallest {
                smallest = count;
                stalled = 0;
            } else {
                stalled += 1;
                if stalled >= STALLED_TAIL_PASSES {
                    return Err(Status::resource_exhausted(format!(
                        "writes outpace compaction: {STALLED_TAIL_PASSES} consecutive tail \
                         passes read no fewer than {smallest} records (last {count}, \
                         tail_bound {tail_bound}) after {passes} passes; pause writes or \
                         raise tail_bound"
                    )));
                }
            }
        }

        // Reserve commits before taking the seal lock, matching Flush's
        // order. Writers wait asynchronously while analysis and reads can
        // still run. The state lock is held only for the final fence/swap.
        let _mutation = self.mutation_gate.blocking_write();
        let _seal = self.seal_lock.lock().expect("seal lock poisoned");
        let mut attempt = 0usize;
        loop {
            attempt += 1;
            let watermark = self.flush_live_wal(pre)?;
            let records = tail
                .read_through(watermark)
                .map_err(|e| Status::failed_precondition(format!("tail the live WAL: {e}")))?;
            // Analysis may use asynchronous I/O. A live-state lock here
            // can block the runtime workers needed to complete that I/O.
            self.apply_pass(shadow, records, analyze)?;
            passes += 1;
            let mut guard = crate::node::write_shard(&self.state);
            let started = Instant::now();
            if Self::flush_live_wal_locked(&mut guard, pre)? != watermark {
                drop(guard);
                if attempt >= CUTOVER_RETRIES {
                    return Err(Status::resource_exhausted(format!(
                        "writes outpace compaction: the live WAL advanced during all \
                         {CUTOVER_RETRIES} cutover preparations"
                    )));
                }
                continue;
            }
            shadow
                .state
                .wal
                .as_mut()
                .expect("checked after every apply")
                .flush()
                .map_err(|e| Status::internal(format!("fsync the rewritten generation: {e}")))?;
            self.install(pre, shadow, &guard)?;
            shadow.state.stats_epoch = guard.stats_epoch;
            shadow.state.advance_stats_epoch();
            shadow.state.parents = None;
            let previous = std::mem::replace(&mut *guard, std::mem::take(&mut shadow.state));
            let held = started.elapsed().as_millis() as u64;
            drop(guard);
            drop(previous);
            return Ok((0, held, passes));
        }
    }

    /// The on-disk cutover, under the write lock the caller holds:
    /// marker first, then the rewritten WAL generation into the shard's
    /// WAL directory, then the layout's own swap.
    pub(super) fn install(
        &self,
        pre: &Preflight,
        shadow: &mut Shadow,
        live: &ShardState,
    ) -> Result<(), Status> {
        let wal_dir = wal::wal_dir(&pre.index_path);
        let new_generation = pre.cutoff_generation + 1;
        let root = crate::node::segments_root(&pre.index_path);
        let previous_snapshot = live.generation.is_some();
        let legacy_files: Vec<PathBuf> = if pre.segmented || previous_snapshot {
            Vec::new()
        } else {
            [
                pre.index_path.clone(),
                crate::node::exact_vector_sidecar_path(&pre.index_path),
                crate::node::bm25_sidecar_path(&pre.index_path),
                crate::node::live_docs_sidecar_path(&pre.index_path),
            ]
            .into_iter()
            .filter(|p| p.exists())
            .collect()
        };
        if pre.segmented {
            let current = SegmentCatalog::read_manifest(&root)
                .map_err(Status::internal)?
                .unwrap_or_default();
            crate::segments::write_manifest_file(&manifest_backup_path(&root), &current)
                .map_err(Status::internal)?;
        }
        let marker = CommitMarker {
            format: MARKER_FORMAT,
            layout: layout_name(pre.segmented).to_string(),
            old_wal_generation: pre.cutoff_generation,
            new_wal_generation: new_generation,
            walless: false,
            work_dir: pre.work_dir.clone(),
            previous_snapshot,
            legacy_files,
            staged_segments: shadow.staged.iter().map(|s| s.segment_id.clone()).collect(),
            replaced_segments: shadow.replaced.clone(),
        };
        write_marker(&pre.index_path, &marker)
            .map_err(|e| Status::internal(format!("write the compaction marker: {e}")))?;
        // From here every failure leaves the marker, and a restart rolls
        // back; the live state is not swapped until everything is in
        // place, so a failure returned here also rolls back at once.
        let result = (|| -> Result<(), Status> {
            let target = wal::gen_dir(&wal_dir, new_generation);
            let staged_gen = wal::gen_dir(&pre.work_dir.join("wal"), new_generation);
            std::fs::rename(&staged_gen, &target).map_err(|e| {
                Status::internal(format!(
                    "move the rewritten generation {} -> {}: {e}",
                    staged_gen.display(),
                    target.display()
                ))
            })?;
            crate::postings::fsync_parent(&target)
                .map_err(|e| Status::internal(format!("fsync {}: {e}", wal_dir.display())))?;
            shadow
                .state
                .wal
                .as_mut()
                .expect("the shadow keeps its writer")
                .relocate(target)
                .map_err(|e| Status::internal(format!("relocate the rewritten generation: {e}")))?;
            if pre.segmented {
                let Some(Bm25Shard::Segmented(shard)) = shadow.state.bm25.as_ref() else {
                    return Err(Status::internal("the segmented shadow lost its catalog"));
                };
                let live_epoch = match live.bm25.as_ref() {
                    Some(Bm25Shard::Segmented(live)) => live.snapshot().epoch(),
                    _ => shadow.epoch_at_open,
                };
                let epoch = live_epoch
                    .max(shadow.epoch_at_open)
                    .checked_add(1)
                    .ok_or_else(|| {
                        Status::out_of_range("compaction publication epoch exhausted")
                    })?;
                let published = shard
                    .catalog()
                    .commit_current(epoch)
                    .map_err(|e| Status::internal(format!("publish the compacted set: {e}")))?;
                // Publication returns a new immutable view. Adopt it in both
                // legs; previously an in-place epoch mutation hid this step.
                if let Some(Bm25Shard::Segmented(shard)) = shadow.state.bm25.as_mut() {
                    shard
                        .republish(published.clone())
                        .map_err(|e| Status::internal(format!("adopt compacted documents: {e}")))?;
                }
                if let Some(provider) = shadow
                    .state
                    .index
                    .as_mut()
                    .and_then(VectorIndex::as_segmented_mut)
                {
                    provider
                        .republish(published)
                        .map_err(|e| Status::internal(format!("adopt compacted vectors: {e}")))?;
                }
            } else {
                let staged = shadow
                    .state
                    .generation
                    .clone()
                    .expect("the single-image shadow serves a generation directory");
                let snap = Self::adopt_generation(&pre.index_path, &staged, false)?;
                shadow.state.generation = Some(snap);
            }
            Ok(())
        })();
        match result {
            Ok(()) => {
                shadow.state.pending_compaction = Some(PendingCommit {
                    index_path: pre.index_path.clone(),
                    marker,
                });
                Ok(())
            }
            Err(status) => {
                recover_interrupted(&pre.index_path);
                Err(status)
            }
        }
    }

    // ----- The from-segments path: a catalog without a WAL -----
    //
    // docs/replay-from-segments.md, "Partitioned compaction of a catalog
    // without a log". The cutoff is the sealed catalog once the current
    // tail is sealed; the build transplants its live rows through
    // `reshard::compact_segments_partitioned` (the analyzer never runs);
    // writes that arrive after the cutoff are caught up by the same seal
    // ingest uses (`seal_tail`), landing in the live catalog as unordered
    // segments the cutover carries over; the cutover keeps the
    // shadow/marker/closing-flush contract of docs/mutations.md.

    /// Seal the tail when it holds rows (or a frozen part is waiting on
    /// its publication), waiting out a legacy two-RPC append caught
    /// mid-row the way the closing flush waits it out. Returns the rows
    /// the seal covered, 0 when there was nothing to seal.
    pub(super) fn seal_tail_wait(&self) -> Result<u64, Status> {
        let deadline = Instant::now() + PAIRED_APPEND_WAIT;
        loop {
            let (pending, frozen) = {
                let guard = crate::node::read_shard(&self.state);
                let (documents, vectors) = match guard.bm25.as_ref() {
                    Some(Bm25Shard::Segmented(shard)) => (
                        shard.tail().next_doc_id() as usize,
                        guard
                            .index
                            .as_ref()
                            .and_then(VectorIndex::as_segmented)
                            .map_or(0, |p| p.tail().len()),
                    ),
                    _ => (0, 0),
                };
                (
                    documents.max(vectors) as u64,
                    matches!(guard.bm25.as_ref(), Some(Bm25Shard::Segmented(shard)) if shard.frozen().is_some()),
                )
            };
            if pending == 0 && !frozen {
                return Ok(0);
            }
            // Paired: a fresh tail holding a document whose vectors have
            // not arrived yet is that same mid-row append; sealing it
            // would strand the document without its vectors.
            match self.seal_tail_paired() {
                Ok(_) => return Ok(pending),
                Err(status)
                    if Instant::now() < deadline
                        && status.code() == tonic::Code::FailedPrecondition
                        && status
                            .message()
                            .contains("a segment's artifacts cover the same rows") =>
                {
                    std::thread::sleep(PAIRED_APPEND_PAUSE);
                }
                Err(status) => return Err(status),
            }
        }
    }

    /// The tail holds documents whose vectors have not arrived: a
    /// two-call append between its calls, which only the writer can
    /// finish (see `ShardService::seal_tail_paired`).
    pub(super) fn tail_awaits_vectors(guard: &ShardState) -> bool {
        let Some(Bm25Shard::Segmented(shard)) = guard.bm25.as_ref() else {
            return false;
        };
        let Some(provider) = guard.index.as_ref().and_then(VectorIndex::as_segmented) else {
            return false;
        };
        shard.frozen().is_none() && shard.tail().next_doc_id() as usize > provider.tail().len()
    }

    /// The tail is empty on both legs and no seal is in flight.
    pub(super) fn segment_tail_is_clear(guard: &ShardState) -> bool {
        let Some(Bm25Shard::Segmented(shard)) = guard.bm25.as_ref() else {
            return false;
        };
        if shard.tail().next_doc_id() != 0 || shard.frozen().is_some() {
            return false;
        }
        guard
            .index
            .as_ref()
            .and_then(VectorIndex::as_segmented)
            .is_none_or(|p| p.tail().is_empty() && p.frozen().is_none())
    }
}

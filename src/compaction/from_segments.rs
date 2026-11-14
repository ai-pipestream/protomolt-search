//! Compaction from sealed segments: staging the outputs and installing
//! the new catalog.

use super::*;

impl NodeServiceImpl {
    /// The from-segments compaction of a catalog without a WAL, start to
    /// closing flush. On any failure before the cutover commits, the live
    /// shard is untouched and the staged outputs are removed.
    pub(super) fn compact_from_segments(
        &self,
        request: &CompactShardRequest,
        mut pre: SegmentPreflight,
        tail_bound: usize,
    ) -> Result<CompactShardResponse, Status> {
        let options = crate::reshard::SegmentCompactionOptions {
            build_threads: request.build_threads.max(1) as usize,
            build_queue: (request.build_queue != 0).then_some(request.build_queue as usize),
            build_memory: (request.build_memory != 0).then_some(request.build_memory),
        };
        if request.dry_run {
            return Ok(CompactShardResponse {
                rows_before: pre.rows_now,
                rows_after: pre.rows_now,
                tombstones_reclaimed: pre.tombstones_now,
                layout: layout_name(true).to_string(),
                dry_run: true,
                stats_epoch: pre.stats_epoch,
                partition_column: pre.partition.clone(),
                ..Default::default()
            });
        }
        std::fs::create_dir_all(&pre.work_dir)
            .map_err(|e| Status::internal(format!("mkdir {}: {e}", pre.work_dir.display())))?;
        let outcome = self.build_stage_and_cut_over_segments(&mut pre, tail_bound, &options);
        if outcome.is_ok() {
            // Everything the work directory held was copied into place at
            // staging (the catalog hashes its own copies).
            if let Err(error) = std::fs::remove_dir_all(&pre.work_dir) {
                eprintln!(
                    "compaction: removing the work directory {} failed: {error}",
                    pre.work_dir.display()
                );
            }
        }
        outcome
    }

    fn build_stage_and_cut_over_segments(
        &self,
        pre: &mut SegmentPreflight,
        tail_bound: usize,
        options: &crate::reshard::SegmentCompactionOptions,
    ) -> Result<CompactShardResponse, Status> {
        // The cutoff seal: the current tail joins the sealed set, so the
        // cutoff is the sealed catalog and only later writes are caught
        // up. Writes continue; nothing here locks them out, so the tail
        // may already hold rows again when the set is read: those are
        // later writes, which the catch-up seals and the cutover carries
        // as segments past the cutoff set. The seal lock keeps a seal
        // from publishing between the set and the overlay read.
        self.seal_tail_wait()?;
        {
            let _seal = self.seal_lock.lock().expect("seal lock poisoned");
            let guard = crate::node::read_shard(&self.state);
            let Some(Bm25Shard::Segmented(shard)) = guard.bm25.as_ref() else {
                return Err(Status::internal(
                    "the shard changed layout while a compaction was starting",
                ));
            };
            let vectors_frozen = guard
                .index
                .as_ref()
                .and_then(VectorIndex::as_segmented)
                .is_some_and(|p| p.frozen().is_some());
            if shard.frozen().is_some() || vectors_frozen {
                return Err(Status::internal(
                    "a frozen tail part is unpublished at the cutoff",
                ));
            }
            pre.set = shard.snapshot().clone();
            pre.overlay = guard.live_docs.clone();
            pre.epoch = shard.snapshot().epoch();
            pre.stats_epoch = guard.stats_epoch;
        }
        // A column no sealed row carries cannot order the outputs (the
        // build refuses a column no LIVE row carries; this refuses before
        // any work when even the physical rows lack it).
        let sealed: u64 = pre
            .set
            .manifest()
            .segments
            .iter()
            .filter_map(|segment| segment.summary.as_ref())
            .flat_map(|summary| summary.int_columns.iter())
            .filter(|c| c.name == pre.partition)
            .map(|c| c.present)
            .sum();
        if sealed == 0 {
            return Err(Status::failed_precondition(format!(
                "partition_column {:?}: no document of this shard carries it, so it cannot \
                 order the rows",
                pre.partition
            )));
        }
        let build_dir = pre.work_dir.join("build");
        let build = crate::reshard::compact_segments_partitioned(
            &pre.set,
            &pre.overlay,
            self.config.slot_offset,
            &build_dir,
            crate::reshard::PartitionSpec {
                column: &pre.partition,
                bound: tail_bound,
            },
            options,
        )
        .map_err(|e| Status::failed_precondition(format!("compaction build: {e}")))?;
        let staged = self.stage_segment_outputs(pre, &build)?;
        let root = crate::node::segments_root(&pre.index_path);
        #[cfg(test)]
        if let Err(error) = segment_staging_hook::run(self, &pre.index_path) {
            crate::segments::remove_segment_dirs(&root, &staged);
            return Err(Status::internal(format!(
                "segment compaction staging hook: {error}"
            )));
        }

        // The tail catch-up: whatever arrived after the cutoff seals into
        // the live catalog as unordered segments through the same seal
        // ingest uses. Each pass seals the whole tail; the loop ends when
        // one finds nothing.
        let mut tail_passes = 0u64;
        let mut smallest = u64::MAX;
        let mut stalled = 0u64;
        loop {
            tail_passes += 1;
            if tail_passes > MAX_TAIL_PASSES {
                crate::segments::remove_segment_dirs(&root, &staged);
                return Err(Status::resource_exhausted(format!(
                    "writes outpace compaction: {MAX_TAIL_PASSES} tail seals never left the \
                     tail empty"
                )));
            }
            let sealed = self.seal_tail_wait()?;
            if sealed == 0 {
                break;
            }
            if sealed < smallest {
                smallest = sealed;
                stalled = 0;
            } else {
                stalled += 1;
                if stalled >= STALLED_TAIL_PASSES {
                    crate::segments::remove_segment_dirs(&root, &staged);
                    return Err(Status::resource_exhausted(format!(
                        "writes outpace compaction: {STALLED_TAIL_PASSES} consecutive tail \
                         seals each sealed at least {smallest} rows; pause writes, then retry"
                    )));
                }
            }
        }

        // The cutover. Reserve commits first: with the gate held, no
        // write but a gate-bypassing one can land, and the fence below
        // catches those. The seal lock keeps a seal from racing the
        // manifest read. The gate is taken per attempt: a two-call append
        // caught between its calls cannot land its vectors while the gate
        // is held, so the attempt releases the gate until it has.
        let mut attempt = 0usize;
        let pair_deadline = Instant::now() + PAIRED_APPEND_WAIT;
        let (write_lock_ms, carried_rows, _mutation) = loop {
            let mutation = self.mutation_gate.blocking_write();
            if Self::tail_awaits_vectors(&crate::node::read_shard(&self.state)) {
                drop(mutation);
                if Instant::now() >= pair_deadline {
                    crate::segments::remove_segment_dirs(&root, &staged);
                    return Err(Status::failed_precondition(format!(
                        "the tail holds documents whose vectors did not arrive within {}s; \
                         send AddVectors for each AddDocuments batch, then retry the compaction",
                        PAIRED_APPEND_WAIT.as_secs()
                    )));
                }
                std::thread::sleep(PAIRED_APPEND_PAUSE);
                continue;
            }
            attempt += 1;
            self.seal_tail_wait()?;
            let _seal = self.seal_lock.lock().expect("seal lock poisoned");
            {
                let guard = crate::node::read_shard(&self.state);
                if !Self::segment_tail_is_clear(&guard) {
                    if attempt >= CUTOVER_RETRIES {
                        crate::segments::remove_segment_dirs(&root, &staged);
                        return Err(Status::resource_exhausted(format!(
                            "writes outpace compaction: the tail was not empty after \
                             {CUTOVER_RETRIES} cutover preparations"
                        )));
                    }
                    continue;
                }
            }
            let prepared = match self.prepare_segment_cutover(pre, &build, &staged) {
                Ok(prepared) => prepared,
                Err(status) => {
                    crate::segments::remove_segment_dirs(&root, &staged);
                    return Err(status);
                }
            };
            let mut guard = crate::node::write_shard(&self.state);
            let started = Instant::now();
            if !self.segment_cutover_fence(&guard, &prepared) {
                drop(guard);
                if attempt >= CUTOVER_RETRIES {
                    crate::segments::remove_segment_dirs(&root, &staged);
                    return Err(Status::resource_exhausted(format!(
                        "writes outpace compaction: the live catalog or its overlay advanced \
                         during all {CUTOVER_RETRIES} cutover preparations"
                    )));
                }
                continue;
            }
            let carried_rows = prepared.carried_rows;
            match self.install_segments(pre, prepared, &mut guard) {
                Ok(()) => break (started.elapsed().as_millis() as u64, carried_rows, mutation),
                Err(status) => return Err(status),
            }
        };
        // rows_after is the cutover's answer: the dense partitions plus
        // the carried rows. Read it before the closing flush, whose seal
        // would fold in writes that landed after the swap.
        let (rows_after, stats_epoch) = {
            let guard = crate::node::read_shard(&self.state);
            (crate::node::physical_rows(&guard), guard.stats_epoch)
        };
        let closing = Instant::now();
        self.closing_flush()?;
        let closing_flush_ms = closing.elapsed().as_millis() as u64;
        let stats_epoch = {
            let guard = crate::node::read_shard(&self.state);
            guard.stats_epoch.max(stats_epoch)
        };
        Ok(CompactShardResponse {
            rows_before: build.rows_before,
            rows_after,
            tombstones_reclaimed: build.tombstones,
            tail_records_applied: carried_rows,
            locked_tail_records: 0,
            write_lock_ms,
            wal_generation: 0,
            cutoff_clock: 0,
            layout: layout_name(true).to_string(),
            dry_run: false,
            closing_flush_ms,
            tail_passes,
            stats_epoch,
            partition_column: pre.partition.clone(),
        })
    }

    /// Stage the build's images under the live catalog root (hashed,
    /// fsynced, unpublished, `cmp-<epoch>-NNNN` ids), then open them once
    /// as a set for validation: the field table and fingerprints are the
    /// shard's, the declaration the segments', the provider state the
    /// live shard's.
    fn stage_segment_outputs(
        &self,
        pre: &SegmentPreflight,
        build: &crate::reshard::SegmentCompactionBuild,
    ) -> Result<Vec<SegmentMetadata>, Status> {
        let root = crate::node::segments_root(&pre.index_path);
        let generation = pre
            .epoch
            .checked_add(1)
            .ok_or_else(|| Status::out_of_range("compaction staging epoch exhausted"))?;
        let mut live_paths = Vec::with_capacity(build.images.len());
        for image in &build.images {
            if image.num_vectors != 0 && image.num_vectors as usize != image.row_parent_ids.len() {
                return Err(Status::failed_precondition(format!(
                    "compaction output {} has {} vectors over {} rows; a segment's artifacts \
                     cover the same rows",
                    image.vector_path.display(),
                    image.num_vectors,
                    image.row_parent_ids.len()
                )));
            }
            if image.bm25_path.is_none() {
                return Err(Status::failed_precondition(format!(
                    "compaction output {} has no BM25 image; the segment layout seals documents",
                    image.vector_path.display()
                )));
            }
            let path = crate::node::live_docs_sidecar_path(&image.vector_path);
            LiveDocs::default()
                .write(&path, image.row_parent_ids.len() as u64)
                .map_err(|e| Status::internal(format!("write {}: {e}", path.display())))?;
            live_paths.push(path);
        }
        let ids: Vec<String> = (0..build.images.len())
            .map(|i| format!("cmp-{generation:06}-{i:04}"))
            .collect();
        let sources: Vec<SegmentSource<'_>> = build
            .images
            .iter()
            .zip(&ids)
            .zip(&live_paths)
            .map(|((image, id), live)| SegmentSource {
                segment_id: id,
                generation,
                base_label: image.slot_offset,
                backend_kind: &pre.backend_kind,
                vector_path: (image.num_vectors > 0).then_some(image.vector_path.as_path()),
                exact_vector_path: (image.num_vectors > 0)
                    .then_some(image.exact_vector_path.as_path()),
                bm25_path: image.bm25_path.as_deref().expect("checked above"),
                live_docs_path: live,
                partition_column: Some(&pre.partition),
            })
            .collect();
        let staged = crate::segments::stage_segments(&root, sources)
            .map_err(|e| Status::internal(format!("stage compacted segments: {e}")))?;
        let opened = (|| -> Result<(), Status> {
            let manifest = SegmentSetManifest {
                epoch: pre.epoch,
                segments: staged.clone(),
                partition_key: Some(pre.partition.clone()),
                generation_declaration: pre.set.manifest().generation_declaration.clone(),
                ..Default::default()
            }
            .with_binding(build.binding.as_ref())
            .map_err(Status::failed_precondition)?;
            let catalog = SegmentCatalog::open_staged(&root, manifest, self.config.vector_load())
                .map_err(|e| Status::internal(format!("open the compacted set: {e}")))?;
            let tail =
                crate::node::heap_store(&self.config).map_err(Status::failed_precondition)?;
            let shard =
                crate::segmented::SegmentedShard::open_catalog(catalog, tail).map_err(|e| {
                    Status::failed_precondition(format!("the compacted set does not open: {e}"))
                })?;
            let set = shard.snapshot().clone();
            self.check_shadow_fingerprints(pre.fields.as_ref(), &Bm25Shard::Segmented(shard))?;
            if let Some(first) = (0..set.len()).find_map(|i| set.vector(i)) {
                let d = first.descriptor();
                if d.backend_kind != pre.backend_kind
                    || d.scoring_fingerprint != pre.scoring_fingerprint
                {
                    return Err(Status::failed_precondition(format!(
                        "the compacted segments score under {}/{} but the shard serves {}/{}; \
                         the catalog's provider state does not reproduce the live generation",
                        d.backend_kind,
                        d.scoring_fingerprint,
                        pre.backend_kind,
                        pre.scoring_fingerprint
                    )));
                }
            }
            Ok(())
        })();
        if opened.is_err() {
            crate::segments::remove_segment_dirs(&root, &staged);
        }
        opened?;
        Ok(staged)
    }

    /// Prepare the cutover with no lock held past a read: the final
    /// manifest (the rebuilt partitions, then every segment sealed after
    /// the cutoff, re-based onto the dense row count), the live overlay
    /// translated onto the new labels, and the shadow serving state
    /// opened over the prepared catalog. The fence at install re-checks
    /// the epoch, the overlay revision, and the tail.
    fn prepare_segment_cutover(
        &self,
        pre: &SegmentPreflight,
        build: &crate::reshard::SegmentCompactionBuild,
        staged: &[SegmentMetadata],
    ) -> Result<PreparedSegmentCutover, Status> {
        let slot_offset = self.config.slot_offset;
        let root = crate::node::segments_root(&pre.index_path);
        let (manifest_now, overlay_now) = {
            let guard = crate::node::read_shard(&self.state);
            let Some(Bm25Shard::Segmented(shard)) = guard.bm25.as_ref() else {
                return Err(Status::internal(
                    "the shard changed layout while a compaction was running",
                ));
            };
            (shard.snapshot().manifest().clone(), guard.live_docs.clone())
        };
        let source = &pre.set.manifest().segments;
        let s_rows = source
            .iter()
            .try_fold(0u64, |rows, s| rows.checked_add(s.rows))
            .ok_or_else(|| Status::internal("cutoff set row count overflow"))?;
        let prefix_holds = manifest_now.segments.len() >= source.len()
            && manifest_now.segments[..source.len()]
                .iter()
                .zip(source)
                .all(|(now, at_cutoff)| {
                    now.segment_id == at_cutoff.segment_id && now.base_label == at_cutoff.base_label
                });
        if !prefix_holds {
            return Err(Status::aborted(
                "the catalog's sealed set changed under the compaction; the build transplanted \
                 the set at its cutoff, so this run cannot cut over — retry",
            ));
        }
        let carried = &manifest_now.segments[source.len()..];
        let mut total = s_rows;
        for segment in carried {
            if segment.base_label != total {
                return Err(Status::internal(format!(
                    "carried segment {} starts at label {} but the segments before it end at \
                     {total}; the live catalog is not one contiguous id space",
                    segment.segment_id, segment.base_label
                )));
            }
            total += segment.rows;
        }
        let dense = build
            .images
            .iter()
            .try_fold(0u64, |rows, image| {
                rows.checked_add(image.row_parent_ids.len() as u64)
            })
            .ok_or_else(|| Status::internal("compacted row count overflow"))?;
        // The outputs, then the carried segments re-based onto them.
        let mut segments: Vec<SegmentMetadata> = staged.to_vec();
        let mut base = dense;
        for segment in carried {
            let mut rebased = segment.clone();
            rebased.base_label = base;
            base += segment.rows;
            segments.push(rebased);
        }
        // The live overlay is the tombstone authority. A bit under the
        // cutoff's rows names a rebuilt row through the id map (absent =
        // already dead at the cutoff, dropped by the build); a bit above
        // them names a carried row, shifted by the reclaimed tombstones.
        let mut overlay = LiveDocs::default();
        if let Some(words) = overlay_now.words() {
            for (wi, &word) in words.iter().enumerate() {
                let mut bits = word;
                while bits != 0 {
                    let bit = bits.trailing_zeros() as usize;
                    bits &= bits - 1;
                    let label = (wi * 64 + bit) as u64;
                    if label < s_rows {
                        let global = slot_offset
                            .checked_add(label)
                            .ok_or_else(|| Status::internal("compaction id overflow"))?;
                        if let Some(&new_local) = build.id_map.get(&global) {
                            overlay.delete(new_local as usize);
                        }
                    } else if label < total {
                        overlay.delete((dense + (label - s_rows)) as usize);
                    } else {
                        return Err(Status::internal(format!(
                            "the live overlay names row {label}, past the catalog's {total} rows"
                        )));
                    }
                }
            }
        }
        let final_manifest = SegmentSetManifest {
            epoch: manifest_now.epoch,
            segments,
            partition_key: Some(pre.partition.clone()),
            generation_declaration: manifest_now.generation_declaration.clone(),
            ..Default::default()
        }
        .with_binding(build.binding.as_ref())
        .map_err(Status::failed_precondition)?;
        let catalog = SegmentCatalog::open_staged(&root, final_manifest, self.config.vector_load())
            .map_err(|e| Status::internal(format!("open the compacted set: {e}")))?;
        let tail = crate::node::heap_store(&self.config).map_err(Status::failed_precondition)?;
        let shard =
            crate::segmented::SegmentedShard::open_catalog(catalog.clone(), tail).map_err(|e| {
                Status::failed_precondition(format!("the compacted set does not open: {e}"))
            })?;
        let set = shard.snapshot().clone();
        let mut index = None;
        let mut exact_vectors = None;
        if let Some(first) = (0..set.len()).find_map(|i| set.vector(i)) {
            let backend = first
                .backend_config()
                .map_err(|e| Status::internal(format!("segment vector backend: {e}")))?;
            let dim = first
                .dim_opt()
                .ok_or_else(|| Status::internal("segment vector image has no dimension"))?;
            let tail_image = VectorIndex::from_backend_config(dim, &backend)
                .map_err(|e| Status::internal(format!("segment tail image: {e}")))?;
            let provider =
                crate::segmented_vectors::SegmentedProvider::open(set.clone(), tail_image)
                    .map_err(|e| Status::internal(format!("segment vectors: {e}")))?;
            index = Some(VectorIndex::from_provider(provider));
            exact_vectors = Some(ExactVectorStore::from_segments(&set, dim).map_err(|error| {
                Status::internal(format!("compacted exact-vector view: {error}"))
            })?);
        }
        Ok(PreparedSegmentCutover {
            catalog,
            shard: Some(shard),
            index,
            exact_vectors,
            overlay,
            epoch: manifest_now.epoch,
            revision: overlay_now.revision(),
            staged_ids: staged.iter().map(|s| s.segment_id.clone()).collect(),
            replaced_ids: source.iter().map(|s| s.segment_id.clone()).collect(),
            carried_rows: total - s_rows,
        })
    }

    /// The fence under the write lock: the catalog, the overlay, and the
    /// tail are exactly what the preparation read. Anything else means a
    /// gate-bypassing write landed; release and prepare again.
    fn segment_cutover_fence(&self, guard: &ShardState, prepared: &PreparedSegmentCutover) -> bool {
        let Some(Bm25Shard::Segmented(shard)) = guard.bm25.as_ref() else {
            return false;
        };
        shard.snapshot().epoch() == prepared.epoch
            && guard.live_docs.revision() == prepared.revision
            && Self::segment_tail_is_clear(guard)
    }

    /// The on-disk cutover, under the write lock the caller holds: the
    /// manifest backup, the commit marker, one manifest publish through
    /// the catalog's own path, the state swap. No WAL moves: this path
    /// writes none.
    fn install_segments(
        &self,
        pre: &SegmentPreflight,
        mut prepared: PreparedSegmentCutover,
        guard: &mut ShardState,
    ) -> Result<(), Status> {
        let root = crate::node::segments_root(&pre.index_path);
        let current = SegmentCatalog::read_manifest(&root)
            .map_err(Status::internal)?
            .unwrap_or_default();
        crate::segments::write_manifest_file(&manifest_backup_path(&root), &current)
            .map_err(Status::internal)?;
        let marker = CommitMarker {
            format: MARKER_FORMAT,
            layout: layout_name(true).to_string(),
            old_wal_generation: 0,
            new_wal_generation: 0,
            walless: true,
            work_dir: pre.work_dir.clone(),
            previous_snapshot: false,
            legacy_files: Vec::new(),
            staged_segments: prepared.staged_ids.clone(),
            replaced_segments: prepared.replaced_ids.clone(),
        };
        write_marker(&pre.index_path, &marker)
            .map_err(|e| Status::internal(format!("write the compaction marker: {e}")))?;
        // From here every failure leaves the marker, and a restart rolls
        // back; a failure returned here also rolls back at once.
        let result = (|| -> Result<(), Status> {
            let epoch = prepared
                .epoch
                .checked_add(1)
                .ok_or_else(|| Status::out_of_range("compaction publication epoch exhausted"))?;
            let published = prepared
                .catalog
                .commit_current(epoch)
                .map_err(|e| Status::internal(format!("publish the compacted set: {e}")))?;
            let shard = prepared.shard.as_mut().expect("prepared once");
            shard
                .republish(published.clone())
                .map_err(|e| Status::internal(format!("adopt compacted documents: {e}")))?;
            if let Some(provider) = prepared
                .index
                .as_mut()
                .and_then(VectorIndex::as_segmented_mut)
            {
                provider
                    .republish(published)
                    .map_err(|e| Status::internal(format!("adopt compacted vectors: {e}")))?;
            }
            let shard = prepared.shard.take().expect("checked above");
            let mapped_binding = shard.binding().cloned();
            let mut state = ShardState {
                index: prepared.index.take(),
                exact_vectors: prepared.exact_vectors.take(),
                bm25: Some(Bm25Shard::Segmented(shard)),
                live_docs: prepared.overlay.clone(),
                generation: None,
                wal: None,
                parents: None,
                mapped_binding,
                stats_epoch: guard.stats_epoch,
                files_current: false,
                stats_incarnation: Default::default(),
                pending_compaction: Some(PendingCommit {
                    index_path: pre.index_path.clone(),
                    marker,
                }),
            };
            state.advance_stats_epoch();
            let previous = std::mem::replace(guard, state);
            drop(previous);
            Ok(())
        })();
        match result {
            Ok(()) => Ok(()),
            Err(status) => {
                recover_interrupted(&pre.index_path);
                Err(status)
            }
        }
    }
}

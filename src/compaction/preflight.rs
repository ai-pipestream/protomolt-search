//! The entry point and the preflight: what a compaction reads under the
//! shard's lock before it builds anything.

use super::*;

impl NodeServiceImpl {
    /// Compact this shard online (`docs/mutations.md`): the blocking
    /// entry point behind `NodeService.CompactShard`, also for an
    /// in-process control-plane worker. The WAL path needs a Tokio
    /// runtime on the calling thread's context for the analysis sessions
    /// (`spawn_blocking` threads have one); the from-segments path of a
    /// catalog without a WAL analyzes nothing and needs none.
    pub fn compact_shard(
        &self,
        request: &CompactShardRequest,
    ) -> Result<CompactShardResponse, Status> {
        let tail_bound = if request.tail_bound == 0 {
            DEFAULT_TAIL_BOUND
        } else {
            request.tail_bound
        } as usize;
        if self
            .compacting
            .compare_exchange(
                false,
                true,
                std::sync::atomic::Ordering::Acquire,
                std::sync::atomic::Ordering::Relaxed,
            )
            .is_err()
        {
            return Err(Status::failed_precondition(
                "a compaction is already running on this shard",
            ));
        }
        let _gate = CompactingGuard(std::sync::Arc::clone(&self.compacting));
        let preflight = self.preflight_at_row_boundary(request)?;
        let preflight = match preflight {
            PreflightKind::Wal(preflight) => preflight,
            PreflightKind::Segments(preflight) => {
                return self.compact_from_segments(request, *preflight, tail_bound);
            }
        };
        let handle = tokio::runtime::Handle::try_current().map_err(|_| {
            Status::failed_precondition(
                "compaction analyzes documents through the node's analysis backend and needs a \
                 Tokio runtime context",
            )
        })?;
        if request.dry_run {
            return Ok(CompactShardResponse {
                rows_before: preflight.rows_now,
                rows_after: preflight.rows_now,
                tombstones_reclaimed: preflight.tombstones_now,
                wal_generation: preflight.cutoff_generation + 1,
                cutoff_clock: preflight.cutoff_clock,
                layout: layout_name(preflight.segmented).to_string(),
                dry_run: true,
                stats_epoch: preflight.stats_epoch,
                partition_column: preflight.partition.clone().unwrap_or_default(),
                ..Default::default()
            });
        }
        // The prefix through the cutoff goes to disk before the replay
        // reads it; writes keep landing on the live shard meanwhile.
        {
            let mut guard = crate::node::write_shard(&self.state);
            if let Some(wal) = guard.wal.as_mut() {
                wal.flush()
                    .map_err(|e| Status::internal(format!("wal fsync before compaction: {e}")))?;
            }
        }
        std::fs::create_dir_all(&preflight.work_dir).map_err(|e| {
            Status::internal(format!("mkdir {}: {e}", preflight.work_dir.display()))
        })?;
        probe_same_filesystem(&preflight.work_dir, &wal::wal_dir(&preflight.index_path))?;

        let mut analyze = self.analyzer(&handle);
        let outcome = self.build_and_cut_over(&preflight, tail_bound, &mut analyze);
        match outcome {
            Ok(response) => {
                // Everything the work directory held was moved or copied
                // into place; the closing flush wrote the images it
                // still mapped from there.
                if let Err(error) = std::fs::remove_dir_all(&preflight.work_dir) {
                    eprintln!(
                        "compaction: removing the work directory {} failed: {error}",
                        preflight.work_dir.display()
                    );
                }
                Ok(response)
            }
            Err(status) => Err(status),
        }
    }

    /// Preflight at a row boundary. The cut is the log's high-water mark
    /// (or, for a catalog without a WAL, the sealed set once the cutoff
    /// seal lands), read under the shard lock; a legacy two-RPC append
    /// (AddDocuments, then AddVectors) can be halfway through at that
    /// instant, and on the segment layout the replay through such a cut
    /// builds a bucket with one document more than vectors, which the
    /// layout refuses to seal. The row completes with the client's next
    /// call, so this waits that out for a bounded time, then refuses
    /// naming the counts.
    pub(super) fn preflight_at_row_boundary(
        &self,
        request: &CompactShardRequest,
    ) -> Result<PreflightKind, Status> {
        const ATTEMPTS: usize = 200;
        const PAUSE: std::time::Duration = std::time::Duration::from_millis(10);
        for attempt in 1..=ATTEMPTS {
            match self.preflight(request)? {
                Ok(preflight) => return Ok(preflight),
                Err(_) if attempt < ATTEMPTS => std::thread::sleep(PAUSE),
                Err(MidRow { documents, vectors }) => {
                    return Err(Status::failed_precondition(format!(
                        "the tail has {documents} documents and {vectors} vectors after {:?}; \
                         compaction cuts at a row boundary, so finish the append (AddVectors \
                         after AddDocuments) or ingest through the mapped path, then retry",
                        PAUSE * ATTEMPTS as u32
                    )))
                }
            }
        }
        unreachable!("the last attempt returns")
    }

    /// One preflight under the read lock: `Ok(Err(_))` when the segment
    /// layout's tail is mid-row (see [`Self::preflight_at_row_boundary`]).
    /// A shard without a WAL qualifies for the from-segments path here or
    /// refuses by name.
    fn preflight(
        &self,
        request: &CompactShardRequest,
    ) -> Result<Result<PreflightKind, MidRow>, Status> {
        let index_path = self.config.index_path.clone().ok_or_else(|| {
            Status::failed_precondition(
                "compaction needs a persisted shard (index_path); an in-memory shard has no log \
                 to compact",
            )
        })?;
        let guard = crate::node::read_shard(&self.state);
        guard.check_legacy_mutation()?;
        if guard.pending_compaction.is_some() {
            return Err(Status::failed_precondition(
                "a compaction cutover is pending its closing flush on this shard; call Flush",
            ));
        }
        let Some(wal) = guard.wal.as_ref() else {
            return self
                .preflight_segments(request, &guard, &index_path)
                .map(|r| r.map(|p| PreflightKind::Segments(Box::new(p))));
        };
        if request.build_threads != 0 || request.build_queue != 0 || request.build_memory != 0 {
            return Err(Status::invalid_argument(
                "build_threads, build_queue and build_memory bound the from-segments build of a \
                 catalog without a WAL; this shard compacts from its log",
            ));
        }
        if wal.has_legacy_clock_records() {
            return Err(Status::failed_precondition(format!(
                "WAL generation {} carries legacy unclocked records; compaction needs a fully \
                 clocked generation (install a snapshot to rotate the log)",
                wal.generation()
            )));
        }
        let manifest = wal.manifest().clone();
        if manifest.preexisting_vectors > 0 || manifest.preexisting_documents > 0 {
            return Err(Status::failed_precondition(format!(
                "WAL generation {} began with {} preexisting vector(s) and {} preexisting \
                 document(s) its log does not contain; compaction replays the log and would \
                 drop them — rebuild the shard from source",
                manifest.generation, manifest.preexisting_vectors, manifest.preexisting_documents
            )));
        }
        if matches!(guard.bm25, Some(Bm25Shard::Spilling(_))) {
            return Err(Status::failed_precondition(
                "a bulk BM25 build is in progress on this shard; Flush it before compacting",
            ));
        }
        let segmented = matches!(guard.bm25, Some(Bm25Shard::Segmented(_)));
        let fields = guard.bm25.as_ref().map(|shard| {
            (0..shard.field_count())
                .map(|f| {
                    (
                        shard.field_name(f).to_string(),
                        shard.analysis_fingerprint(f),
                    )
                })
                .unzip()
        });
        let partition = if request.partition_column.is_empty() {
            None
        } else {
            let column = request.partition_column.as_str();
            if !segmented {
                return Err(Status::failed_precondition(format!(
                    "partition_column {column:?} needs the segment layout; this shard is a \
                     single image"
                )));
            }
            let Some(shard) = guard.bm25.as_ref() else {
                return Err(Status::failed_precondition(format!(
                    "partition_column {column:?}: this shard holds no documents, so no column \
                     can order it"
                )));
            };
            let Some(ii) = shard.integer_index(column) else {
                let kind = if shard.numeric_index(column).is_some() {
                    "a double column"
                } else if shard.facet_index(column).is_some() {
                    "a facet column"
                } else {
                    "not a column of this shard"
                };
                return Err(Status::invalid_argument(format!(
                    "partition_column {column:?} is {kind}; a partitioned compaction orders \
                     by an integer column (--integer-fields, timestamps included)"
                )));
            };
            let Bm25Shard::Segmented(segmented) = shard else {
                return Err(Status::internal(
                    "the segment layout serves a segmented shard",
                ));
            };
            // Before any work: a column no document of the shard carries
            // cannot order it. The sealed summaries count the segments'
            // rows; the tail is scanned.
            let sealed: u64 = segmented
                .snapshot()
                .manifest()
                .segments
                .iter()
                .filter_map(|segment| segment.summary.as_ref())
                .flat_map(|summary| summary.int_columns.iter())
                .filter(|c| c.name == column)
                .map(|c| c.present)
                .sum();
            let tail = segmented.tail();
            let in_tail = (0..tail.next_doc_id()).any(|doc| tail.integer_value(ii, doc).is_some());
            if sealed == 0 && !in_tail {
                return Err(Status::failed_precondition(format!(
                    "partition_column {column:?}: no document of this shard carries it, so it \
                     cannot order the rows"
                )));
            }
            Some(column.to_string())
        };
        // Retain every declared column, including columns with no surviving
        // values. Both storage layouts must reopen with the same schema.
        macro_rules! column_tables {
            ($store:expr) => {{
                let s = $store;
                crate::reshard::ColumnTables {
                    facets: (0..s.facet_count())
                        .map(|i| s.facet_name(i).to_string())
                        .collect(),
                    numerics: (0..s.numeric_count())
                        .map(|i| s.numeric_name(i).to_string())
                        .collect(),
                    map_facets: (0..s.map_facet_count())
                        .map(|i| s.map_facet_name(i).to_string())
                        .collect(),
                    map_numerics: (0..s.map_numeric_count())
                        .map(|i| s.map_numeric_name(i).to_string())
                        .collect(),
                    map_integers: (0..s.map_integer_count())
                        .map(|i| s.map_integer_name(i).to_string())
                        .collect(),
                    map_unsigned_integers: (0..s.map_unsigned_integer_count())
                        .map(|i| s.map_unsigned_integer_name(i).to_string())
                        .collect(),
                    integers: (0..s.integer_count())
                        .map(|i| s.integer_name(i).to_string())
                        .collect(),
                    unsigned_integers: (0..s.unsigned_integer_count())
                        .map(|i| s.unsigned_integer_name(i).to_string())
                        .collect(),
                    geo: (0..s.geo_count())
                        .map(|i| s.geo_name(i).to_string())
                        .collect(),
                }
            }};
        }
        let columns = match guard.bm25.as_ref() {
            Some(Bm25Shard::Building(s)) => Some(column_tables!(s)),
            Some(Bm25Shard::Resident(s)) => Some(column_tables!(s)),
            Some(Bm25Shard::Segmented(s)) => Some(column_tables!(s)),
            Some(Bm25Shard::Spilling(_)) => unreachable!("bulk build refused above"),
            None => None,
        };
        if fields.is_some() && self.config.analysis_addr.is_none() {
            return Err(Status::unavailable(
                "no analysis backend configured for this shard (analysis_addr); compaction \
                 re-analyzes every live document",
            ));
        }
        let (backend_kind, scoring_fingerprint) = guard
            .index
            .as_ref()
            .map(|index| {
                let d = index.descriptor();
                (d.backend_kind, d.scoring_fingerprint)
            })
            .unwrap_or_default();
        let work_dir = if request.work_dir.is_empty() {
            default_work_dir(&index_path)
        } else {
            PathBuf::from(&request.work_dir)
        };
        if work_dir.exists()
            && std::fs::read_dir(&work_dir)
                .map_err(|e| Status::internal(format!("read {}: {e}", work_dir.display())))?
                .next()
                .is_some()
        {
            return Err(Status::failed_precondition(format!(
                "compaction work directory {} is not empty; a previous compaction left it — \
                 inspect and remove it",
                work_dir.display()
            )));
        }
        let cutoff_generation = wal.generation();
        if segmented {
            let root = crate::node::segments_root(&index_path);
            let prefix = format!("cmp-{:06}-", cutoff_generation + 1);
            if let Ok(entries) = std::fs::read_dir(root.join("segments")) {
                for entry in entries.flatten() {
                    let name = entry.file_name().to_string_lossy().into_owned();
                    if name.starts_with(&prefix) {
                        return Err(Status::failed_precondition(format!(
                            "staged segment directory {} exists; a previous compaction left it \
                             — remove it before retrying",
                            entry.path().display()
                        )));
                    }
                }
            }
        }
        if let Some(Bm25Shard::Segmented(shard)) = guard.bm25.as_ref() {
            if let Some(provider) = guard.index.as_ref().and_then(VectorIndex::as_segmented) {
                let documents = shard.tail().next_doc_id() as usize;
                let vectors = provider.tail().len();
                if documents != vectors {
                    return Ok(Err(MidRow { documents, vectors }));
                }
            }
        }
        let rows_now = crate::node::physical_rows(&guard);
        Ok(Ok(PreflightKind::Wal(Box::new(Preflight {
            index_path,
            work_dir,
            segmented,
            gen_dir: wal.dir().to_path_buf(),
            cutoff_generation,
            cutoff_clock: wal.high_watermark(),
            manifest,
            rows_now,
            tombstones_now: guard.live_docs.deleted_count().min(rows_now),
            fields,
            backend_kind,
            scoring_fingerprint,
            stats_epoch: guard.stats_epoch,
            partition,
            columns,
        }))))
    }

    /// The preflight of a catalog WITHOUT a WAL: the from-segments path
    /// (docs/replay-from-segments.md, "Partitioned compaction of a
    /// catalog without a log"). Only a catalog that qualifies enters it:
    /// the segmented layout with at least one sealed segment and the
    /// generation binding the outputs must carry. Anything less refuses
    /// by name, as does a missing partition column: with no log there is
    /// no bucket layout to keep, so this path builds the partitioned
    /// layout only.
    fn preflight_segments(
        &self,
        request: &CompactShardRequest,
        guard: &ShardState,
        index_path: &Path,
    ) -> Result<Result<SegmentPreflight, MidRow>, Status> {
        let no_wal = "this shard has no WAL; compaction replays the log, so a shard without one \
                      can only be rebuilt from source";
        let Some(Bm25Shard::Segmented(shard)) = guard.bm25.as_ref() else {
            return Err(Status::failed_precondition(no_wal));
        };
        let set = shard.snapshot().clone();
        if set.is_empty() {
            return Err(Status::failed_precondition(format!(
                "{no_wal} — and this catalog has no sealed segments to compact from; flush it \
                 first"
            )));
        }
        if set.binding().is_none() {
            return Err(Status::failed_precondition(format!(
                "{no_wal} — and this catalog carries no generation binding for the outputs to \
                 keep"
            )));
        }
        if request.partition_column.is_empty() {
            return Err(Status::failed_precondition(
                "compaction of a catalog without a WAL replays its sealed segments and needs \
                 partition_column to order the outputs; without one there is no layout to build",
            ));
        }
        let column = request.partition_column.as_str();
        if shard.integer_index(column).is_none() {
            let kind = if shard.numeric_index(column).is_some() {
                "a double column"
            } else if shard.facet_index(column).is_some() {
                "a facet column"
            } else {
                "not a column of this shard"
            };
            return Err(Status::invalid_argument(format!(
                "partition_column {column:?} is {kind}; a partitioned compaction orders by an \
                 integer column (--integer-fields, timestamps included)"
            )));
        }
        crate::node::check_attached_derived(
            &self.config,
            shard.derived(),
            shard.doc_count(),
            "the catalog",
        )
        .map_err(Status::failed_precondition)?;
        let fields = Some(
            (0..shard.field_count())
                .map(|f| {
                    (
                        shard.field_name(f).to_string(),
                        shard.analysis_fingerprint(f),
                    )
                })
                .unzip(),
        );
        let (backend_kind, scoring_fingerprint) = guard
            .index
            .as_ref()
            .map(|index| {
                let d = index.descriptor();
                (d.backend_kind, d.scoring_fingerprint)
            })
            .unwrap_or_default();
        let work_dir = if request.work_dir.is_empty() {
            default_work_dir(index_path)
        } else {
            PathBuf::from(&request.work_dir)
        };
        if work_dir.exists()
            && std::fs::read_dir(&work_dir)
                .map_err(|e| Status::internal(format!("read {}: {e}", work_dir.display())))?
                .next()
                .is_some()
        {
            return Err(Status::failed_precondition(format!(
                "compaction work directory {} is not empty; a previous compaction left it — \
                 inspect and remove it",
                work_dir.display()
            )));
        }
        // A staged leftover of an interrupted run is never adopted: a
        // retry refuses it by name until an operator removes it.
        let root = crate::node::segments_root(index_path);
        if let Ok(entries) = std::fs::read_dir(root.join("segments")) {
            let published: std::collections::BTreeSet<&str> = set
                .manifest()
                .segments
                .iter()
                .map(|s| s.segment_id.as_str())
                .collect();
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().into_owned();
                if name.starts_with("cmp-") && !published.contains(name.as_str()) {
                    return Err(Status::failed_precondition(format!(
                        "staged segment directory {} exists; a previous compaction left it — \
                         remove it before retrying",
                        entry.path().display()
                    )));
                }
            }
        }
        if let Some(provider) = guard.index.as_ref().and_then(VectorIndex::as_segmented) {
            let documents = shard.tail().next_doc_id() as usize;
            let vectors = provider.tail().len();
            if documents != vectors {
                return Ok(Err(MidRow { documents, vectors }));
            }
        }
        let rows_now = crate::node::physical_rows(guard);
        Ok(Ok(SegmentPreflight {
            index_path: index_path.to_path_buf(),
            work_dir,
            set,
            overlay: guard.live_docs.clone(),
            epoch: shard.snapshot().epoch(),
            rows_now,
            tombstones_now: guard.live_docs.deleted_count().min(rows_now),
            partition: column.to_string(),
            fields,
            backend_kind,
            scoring_fingerprint,
            stats_epoch: guard.stats_epoch,
        }))
    }

    /// The analyzer for the replay and the tail: the node's own analysis
    /// backend, sidecar or native, through the same batch sessions the
    /// offline reshard uses.
    pub(super) fn analyzer<'a>(&'a self, handle: &'a tokio::runtime::Handle) -> Box<Analyze<'a>> {
        Box::new(move |docs| {
            let addr = self.config.analysis_addr.as_deref().ok_or_else(|| {
                "no analysis backend configured for this shard (analysis_addr)".to_string()
            })?;
            handle
                .block_on(crate::analyzer::analyze_batch_streams(
                    addr,
                    docs,
                    ANALYSIS_STREAMS,
                ))
                .map_err(|status| status.message().to_string())
        })
    }
}

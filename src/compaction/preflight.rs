//! Preflight: what a compaction checks and pins before it builds anything.

use std::path::{Path, PathBuf};

use tonic::Status;

use crate::node::{Bm25Shard, NodeServiceImpl, ShardState};
use crate::pb::CompactShardRequest;
use crate::vector::VectorIndex;

use super::{default_work_dir, MidRow, Preflight, PreflightKind, SegmentPreflight};

fn shard_field_fingerprints(shard: &Bm25Shard) -> (Vec<String>, Vec<u64>) {
    (0..shard.field_count())
        .map(|f| {
            (
                shard.field_name(f).to_string(),
                shard.analysis_fingerprint(f),
            )
        })
        .unzip()
}

fn validate_partition_integer_column(shard: &Bm25Shard, column: &str) -> Result<usize, Status> {
    if let Some(ii) = shard.integer_index(column) {
        return Ok(ii);
    }
    let kind = if shard.numeric_index(column).is_some() {
        "a double column"
    } else if shard.facet_index(column).is_some() {
        "a facet column"
    } else {
        "not a column of this shard"
    };
    Err(Status::invalid_argument(format!(
        "partition_column {column:?} is {kind}; a partitioned compaction orders \
         by an integer column (--integer-fields, timestamps included)"
    )))
}

fn resolve_wal_partition_column(
    segmented: bool,
    bm25: Option<&Bm25Shard>,
    requested: &str,
) -> Result<Option<String>, Status> {
    if requested.is_empty() {
        return Ok(None);
    }
    if !segmented {
        return Err(Status::failed_precondition(format!(
            "partition_column {requested:?} needs the segment layout; this shard is a \
             single image"
        )));
    }
    let Some(shard) = bm25 else {
        return Err(Status::failed_precondition(format!(
            "partition_column {requested:?}: this shard holds no documents, so no column \
             can order it"
        )));
    };
    let ii = validate_partition_integer_column(shard, requested)?;
    let Bm25Shard::Segmented(seg) = shard else {
        return Err(Status::internal(
            "the segment layout serves a segmented shard",
        ));
    };
    let sealed: u64 = seg
        .snapshot()
        .manifest()
        .segments
        .iter()
        .filter_map(|s| s.summary.as_ref())
        .flat_map(|summary| summary.int_columns.iter())
        .filter(|c| c.name == requested)
        .map(|c| c.present)
        .sum();
    let tail = seg.tail();
    let in_tail = (0..tail.next_doc_id()).any(|doc| tail.integer_value(ii, doc).is_some());
    if sealed == 0 && !in_tail {
        return Err(Status::failed_precondition(format!(
            "partition_column {requested:?}: no document of this shard carries it, so it \
             cannot order the rows"
        )));
    }
    Ok(Some(requested.to_string()))
}

fn extract_column_tables(bm25: Option<&Bm25Shard>) -> Option<crate::reshard::ColumnTables> {
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
    match bm25 {
        Some(Bm25Shard::Building(s)) => Some(column_tables!(s)),
        Some(Bm25Shard::Resident(s)) => Some(column_tables!(s)),
        Some(Bm25Shard::Segmented(s)) => Some(column_tables!(s)),
        Some(Bm25Shard::Spilling(_)) => unreachable!("bulk build refused above"),
        None => None,
    }
}

fn provider_descriptor(guard: &ShardState) -> (String, String) {
    guard
        .index
        .as_ref()
        .map(|index| {
            let d = index.descriptor();
            (d.backend_kind, d.scoring_fingerprint)
        })
        .unwrap_or_default()
}

fn resolve_empty_work_dir(index_path: &Path, requested: &str) -> Result<PathBuf, Status> {
    let work_dir = if requested.is_empty() {
        default_work_dir(index_path)
    } else {
        PathBuf::from(requested)
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
    Ok(work_dir)
}

fn check_unclaimed_staged_segments<F>(index_path: &Path, mut is_leftover: F) -> Result<(), Status>
where
    F: FnMut(&str) -> bool,
{
    let root = crate::node::segments_root(index_path);
    if let Ok(entries) = std::fs::read_dir(root.join("segments")) {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if is_leftover(&name) {
                return Err(Status::failed_precondition(format!(
                    "staged segment directory {} exists; a previous compaction left it \
                     — remove it before retrying",
                    entry.path().display()
                )));
            }
        }
    }
    Ok(())
}

fn check_tail_row_alignment(
    shard: &crate::segmented::SegmentedShard,
    guard: &ShardState,
) -> Option<MidRow> {
    let provider = guard.index.as_ref().and_then(VectorIndex::as_segmented)?;
    let documents = shard.tail().next_doc_id() as usize;
    let vectors = provider.tail().len();
    (documents != vectors).then_some(MidRow { documents, vectors })
}

fn resolve_hnsw_sources(
    slot_offset: u64,
    backend_kind: &str,
    segmented: bool,
    guard: &ShardState,
    index_path: &Path,
) -> Option<crate::reshard::HnswCompactionSources> {
    if backend_kind != crate::hnsw::HNSW {
        return None;
    }
    if segmented {
        guard
            .index
            .as_ref()
            .and_then(VectorIndex::as_segmented)
            .and_then(|provider| {
                crate::reshard::HnswCompactionSources::from_segment_set(
                    slot_offset,
                    provider.snapshot(),
                )
            })
    } else {
        let persisted = guard
            .generation
            .as_ref()
            .map(|g| crate::node::generation_vector(g))
            .unwrap_or_else(|| index_path.to_path_buf());
        crate::reshard::HnswCompactionSources::from_single_image_path(slot_offset, &persisted)
    }
}

fn validate_wal_preconditions(
    request: &CompactShardRequest,
    manifest: &crate::wal::WalManifest,
    guard: &ShardState,
    has_fields: bool,
    analysis_addr: Option<&str>,
) -> Result<(), Status> {
    if request.build_threads != 0 || request.build_queue != 0 || request.build_memory != 0 {
        return Err(Status::invalid_argument(
            "build_threads, build_queue and build_memory bound the from-segments build of a \
             catalog without a WAL; this shard compacts from its log",
        ));
    }
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
    if has_fields && analysis_addr.is_none() {
        return Err(Status::unavailable(
            "no analysis backend configured for this shard (analysis_addr); compaction \
             re-analyzes every live document",
        ));
    }
    Ok(())
}

impl NodeServiceImpl {
    /// Preflight at a row boundary. Legacy two-RPC appends (`AddDocuments`
    /// followed by `AddVectors`) can momentarily leave documents ahead of
    /// vectors; wait up to a bounded window before refusing by count.
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
        let manifest = wal.manifest().clone();
        let segmented = matches!(guard.bm25, Some(Bm25Shard::Segmented(_)));
        let fields = guard.bm25.as_ref().map(shard_field_fingerprints);
        validate_wal_preconditions(
            request,
            &manifest,
            &guard,
            fields.is_some(),
            self.config.analysis_addr.as_deref(),
        )?;
        let partition = resolve_wal_partition_column(
            segmented,
            guard.bm25.as_ref(),
            &request.partition_column,
        )?;
        let columns = extract_column_tables(guard.bm25.as_ref());
        let (backend_kind, scoring_fingerprint) = provider_descriptor(&guard);
        let work_dir = resolve_empty_work_dir(&index_path, &request.work_dir)?;
        let cutoff_generation = wal.generation();
        if segmented {
            let prefix = format!("cmp-{:06}-", cutoff_generation + 1);
            check_unclaimed_staged_segments(&index_path, |name| name.starts_with(&prefix))?;
        }
        if let Some(Bm25Shard::Segmented(shard)) = guard.bm25.as_ref() {
            if let Some(mid) = check_tail_row_alignment(shard, &guard) {
                return Ok(Err(mid));
            }
        }
        let hnsw_sources = resolve_hnsw_sources(
            self.config.slot_offset,
            &backend_kind,
            segmented,
            &guard,
            &index_path,
        );
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
            hnsw_sources,
        }))))
    }

    fn preflight_segments(
        &self,
        request: &CompactShardRequest,
        guard: &ShardState,
        index_path: &Path,
    ) -> Result<Result<SegmentPreflight, MidRow>, Status> {
        let no_wal = "this shard has no WAL; compaction replays the log, so a shard without one \
                      can only be rebuilt from source";
        let Some(Bm25Shard::Segmented(bm25)) = guard.bm25.as_ref() else {
            return Err(Status::failed_precondition(no_wal));
        };
        let set = bm25.snapshot().clone();
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
        validate_partition_integer_column(guard.bm25.as_ref().expect("checked above"), column)?;
        crate::node::check_attached_derived(
            &self.config,
            bm25.derived(),
            bm25.doc_count(),
            "the catalog",
        )
        .map_err(Status::failed_precondition)?;
        let fields = guard.bm25.as_ref().map(shard_field_fingerprints);
        let (backend_kind, scoring_fingerprint) = provider_descriptor(guard);
        let work_dir = resolve_empty_work_dir(index_path, &request.work_dir)?;
        let published: std::collections::BTreeSet<&str> = set
            .manifest()
            .segments
            .iter()
            .map(|s| s.segment_id.as_str())
            .collect();
        check_unclaimed_staged_segments(index_path, |name| {
            name.starts_with("cmp-") && !published.contains(name)
        })?;
        if let Some(mid) = check_tail_row_alignment(bm25, guard) {
            return Ok(Err(mid));
        }
        let rows_now = crate::node::physical_rows(guard);
        Ok(Ok(SegmentPreflight {
            index_path: index_path.to_path_buf(),
            work_dir,
            set,
            overlay: guard.live_docs.clone(),
            epoch: bm25.snapshot().epoch(),
            rows_now,
            tombstones_now: guard.live_docs.deleted_count().min(rows_now),
            partition: column.to_string(),
            fields,
            backend_kind,
            scoring_fingerprint,
            stats_epoch: guard.stats_epoch,
        }))
    }
}

/// Probe atomic rename compatibility between `work_dir` and `wal_dir` up front
/// rather than discovering a cross-filesystem work directory at cutover.
pub(super) fn probe_same_filesystem(work_dir: &Path, wal_dir: &Path) -> Result<(), Status> {
    std::fs::create_dir_all(wal_dir)
        .map_err(|e| Status::internal(format!("mkdir {}: {e}", wal_dir.display())))?;
    let probe = work_dir.join(".fs-probe");
    let target = wal_dir.join(format!(".compact-probe-{}", std::process::id()));
    std::fs::write(&probe, b"probe")
        .map_err(|e| Status::internal(format!("write {}: {e}", probe.display())))?;
    let moved = std::fs::rename(&probe, &target);
    let _ = std::fs::remove_file(&probe);
    let _ = std::fs::remove_file(&target);
    moved.map_err(|e| {
        Status::failed_precondition(format!(
            "compaction work directory {} must be on the same filesystem as the WAL at {} (the \
             rewritten generation is renamed into place): {e}",
            work_dir.display(),
            wal_dir.display()
        ))
    })
}

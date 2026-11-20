//! Preflight: what a compaction checks and pins before it builds anything.

mod checks;

pub(super) use self::checks::probe_same_filesystem;

use std::path::Path;

use tonic::Status;

use crate::node::{Bm25Shard, NodeServiceImpl, ShardState};
use crate::pb::CompactShardRequest;

use self::checks::{
    check_tail_row_alignment, check_unclaimed_staged_segments, extract_column_tables,
    provider_descriptor, resolve_empty_work_dir, resolve_hnsw_sources,
    resolve_wal_partition_column, shard_field_fingerprints, validate_partition_integer_column,
    validate_wal_preconditions,
};
use super::{MidRow, Preflight, PreflightKind, SegmentPreflight};

impl NodeServiceImpl {
    /// Preflight at a row boundary: a tail whose documents and vectors
    /// counts differ refuses by count.
    pub(super) fn preflight_at_row_boundary(
        &self,
        request: &CompactShardRequest,
    ) -> Result<PreflightKind, Status> {
        self.preflight(request)?
            .map_err(|MidRow { documents, vectors }| {
                Status::failed_precondition(format!(
                    "the tail has {documents} documents and {vectors} vectors; compaction cuts at \
                 a row boundary, so send AddVectors for each AddDocuments batch or ingest \
                 through the mapped path, then retry"
                ))
            })
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
        guard.check_index_owned_write()?;
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
        let Some(bm25 @ Bm25Shard::Segmented(shard)) = guard.bm25.as_ref() else {
            return Err(Status::internal(
                "a persisted shard serves its segment catalog",
            ));
        };
        let fields = Some(shard_field_fingerprints(bm25));
        validate_wal_preconditions(request, &manifest, self.config.analysis_addr.as_deref())?;
        let partition = resolve_wal_partition_column(bm25, shard, &request.partition_column)?;
        let columns = extract_column_tables(guard.bm25.as_ref());
        let (backend_kind, scoring_fingerprint) = provider_descriptor(&guard);
        let work_dir = resolve_empty_work_dir(&index_path, &request.work_dir)?;
        let cutoff_generation = wal.generation();
        let prefix = format!("cmp-{:06}-", cutoff_generation + 1);
        check_unclaimed_staged_segments(&index_path, |name| name.starts_with(&prefix))?;
        if let Some(mid) = check_tail_row_alignment(shard, &guard) {
            return Ok(Err(mid));
        }
        let hnsw_sources = resolve_hnsw_sources(self.config.slot_offset, &backend_kind, &guard);
        let rows_now = crate::node::physical_rows(&guard);
        Ok(Ok(PreflightKind::Wal(Box::new(Preflight {
            index_path,
            work_dir,
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

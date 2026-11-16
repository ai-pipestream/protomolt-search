//! Preflight checks and probes over the live shard state.

use std::path::{Path, PathBuf};

use tonic::Status;

use crate::node::{Bm25Shard, ShardState};
use crate::pb::CompactShardRequest;
use crate::vector::VectorIndex;

use super::super::{default_work_dir, MidRow};

pub(super) fn shard_field_fingerprints(shard: &Bm25Shard) -> (Vec<String>, Vec<u64>) {
    (0..shard.field_count())
        .map(|f| {
            (
                shard.field_name(f).to_string(),
                shard.analysis_fingerprint(f),
            )
        })
        .unzip()
}

pub(super) fn validate_partition_integer_column(
    shard: &Bm25Shard,
    column: &str,
) -> Result<usize, Status> {
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

pub(super) fn resolve_wal_partition_column(
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

pub(super) fn extract_column_tables(
    bm25: Option<&Bm25Shard>,
) -> Option<crate::reshard::ColumnTables> {
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

pub(super) fn provider_descriptor(guard: &ShardState) -> (String, String) {
    guard
        .index
        .as_ref()
        .map(|index| {
            let d = index.descriptor();
            (d.backend_kind, d.scoring_fingerprint)
        })
        .unwrap_or_default()
}

pub(super) fn resolve_empty_work_dir(
    index_path: &Path,
    requested: &str,
) -> Result<PathBuf, Status> {
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

pub(super) fn check_unclaimed_staged_segments<F>(
    index_path: &Path,
    mut is_leftover: F,
) -> Result<(), Status>
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

pub(super) fn check_tail_row_alignment(
    shard: &crate::segmented::SegmentedShard,
    guard: &ShardState,
) -> Option<MidRow> {
    let provider = guard.index.as_ref().and_then(VectorIndex::as_segmented)?;
    let documents = shard.tail().next_doc_id() as usize;
    let vectors = provider.tail().len();
    (documents != vectors).then_some(MidRow { documents, vectors })
}

pub(super) fn resolve_hnsw_sources(
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

pub(super) fn validate_wal_preconditions(
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

/// Probe atomic rename compatibility between `work_dir` and `wal_dir` up front
/// rather than discovering a cross-filesystem work directory at cutover.
pub(in crate::compaction) fn probe_same_filesystem(
    work_dir: &Path,
    wal_dir: &Path,
) -> Result<(), Status> {
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

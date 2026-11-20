//! Keep WAL-less publication, serving adoption, and rollback under one fence.

use super::super::recovery::{recover_interrupted, write_marker, CommitMarker, PendingCommit};
use super::super::{manifest_backup_path, MARKER_FORMAT};
use super::*;
use crate::segments::CatalogCutover;

impl NodeServiceImpl {
    pub(super) fn install_segments(
        &self,
        pre: &SegmentPreflight,
        prepared: PreparedSegmentCutover,
        guard: &mut ShardState,
    ) -> Result<(), Status> {
        let Some(Bm25Shard::Segmented(serving)) = guard.bm25.as_ref() else {
            return Err(Status::aborted(
                "serving shard changed layout before compaction cutover",
            ));
        };
        let serving_snapshot = serving.snapshot().clone();
        let serving_catalog = serving.catalog().clone();
        let shadow_catalog = prepared.catalog.clone();
        shadow_catalog
            .with_caught_up_shadow(&serving_catalog, &serving_snapshot, |cutover| {
                install_fenced(pre, prepared, guard, cutover)
            })
            .map_err(Status::aborted)?
    }
}

fn install_fenced(
    pre: &SegmentPreflight,
    mut prepared: PreparedSegmentCutover,
    guard: &mut ShardState,
    cutover: &CatalogCutover<'_>,
) -> Result<(), Status> {
    let marker = prepare_marker(pre, &prepared)?;
    write_marker(&pre.index_path, &marker)
        .map_err(|e| Status::internal(format!("write the compaction marker: {e}")))?;
    if let Err(status) = publish_prepared(&mut prepared, cutover) {
        recover_interrupted(&pre.index_path);
        return Err(status);
    }
    let shard = prepared.shard.take().expect("prepared once");
    let mapped_binding = shard.binding().cloned();
    let mut state = ShardState {
        multi_res_vectors: std::mem::take(&mut prepared.multi_res_vectors),
        index: prepared.index.take(),
        exact_vectors: prepared.exact_vectors.take(),
        bm25: Some(Bm25Shard::Segmented(shard)),
        live_docs: prepared.overlay.clone(),
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
}

fn prepare_marker(
    pre: &SegmentPreflight,
    prepared: &PreparedSegmentCutover,
) -> Result<CommitMarker, Status> {
    let root = crate::node::segments_root(&pre.index_path);
    let current = SegmentCatalog::read_manifest(&root)
        .map_err(Status::internal)?
        .unwrap_or_default();
    crate::segments::write_manifest_file(&manifest_backup_path(&root), &current)
        .map_err(Status::internal)?;
    Ok(CommitMarker {
        format: MARKER_FORMAT,
        old_wal_generation: 0,
        new_wal_generation: 0,
        walless: true,
        work_dir: pre.work_dir.clone(),
        staged_segments: prepared.staged_ids.clone(),
        replaced_segments: prepared.replaced_ids.clone(),
    })
}

fn publish_prepared(
    prepared: &mut PreparedSegmentCutover,
    cutover: &CatalogCutover<'_>,
) -> Result<(), Status> {
    let epoch = prepared
        .epoch
        .checked_add(1)
        .ok_or_else(|| Status::out_of_range("compaction publication epoch exhausted"))?;
    let published = cutover
        .commit_current(epoch)
        .map_err(|e| Status::internal(format!("publish the compacted set: {e}")))?;
    prepared
        .shard
        .as_mut()
        .expect("prepared once")
        .republish_cutover(published.clone(), cutover)
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
    Ok(())
}

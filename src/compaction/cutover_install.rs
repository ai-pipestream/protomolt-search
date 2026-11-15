//! Commit a caught-up shadow while holding the serving and catalog fences.

use super::super::recovery::{recover_interrupted, write_marker, CommitMarker, PendingCommit};
use super::super::{layout_name, manifest_backup_path, MARKER_FORMAT};
use super::*;
use crate::segments::{CatalogCutover, SegmentCatalog};
use crate::vector::VectorIndex;
use crate::wal;

impl NodeServiceImpl {
    pub(super) fn install(
        &self,
        pre: &Preflight,
        shadow: &mut Shadow,
        live: &mut ShardState,
    ) -> Result<ShardState, Status> {
        if !pre.segmented {
            return self.install_and_activate(pre, shadow, live, None);
        }
        let Some(Bm25Shard::Segmented(serving)) = live.bm25.as_ref() else {
            return Err(Status::aborted(
                "serving shard changed layout before compaction cutover",
            ));
        };
        let Some(Bm25Shard::Segmented(prepared)) = shadow.state.bm25.as_ref() else {
            return Err(Status::internal("the segmented shadow lost its catalog"));
        };
        let serving_snapshot = serving.snapshot().clone();
        let serving = serving.catalog().clone();
        let prepared = prepared.catalog().clone();
        prepared
            .with_caught_up_shadow(&serving, &serving_snapshot, |cutover| {
                self.install_and_activate(pre, shadow, live, Some(cutover))
            })
            .map_err(Status::aborted)?
    }

    fn install_and_activate(
        &self,
        pre: &Preflight,
        shadow: &mut Shadow,
        live: &mut ShardState,
        cutover: Option<&CatalogCutover<'_>>,
    ) -> Result<ShardState, Status> {
        let marker = prepare_marker(pre, shadow, live)?;
        write_marker(&pre.index_path, &marker)
            .map_err(|e| Status::internal(format!("write the compaction marker: {e}")))?;
        let result = move_shadow_generation(pre, shadow).and_then(|()| {
            if let Some(cutover) = cutover {
                publish_segment_shadow(shadow, live, cutover)
            } else {
                let staged = shadow
                    .state
                    .generation
                    .clone()
                    .expect("the single-image shadow serves a generation directory");
                let snap = Self::adopt_generation(&pre.index_path, &staged, false)?;
                shadow.state.generation = Some(snap);
                Ok(())
            }
        });
        if let Err(status) = result {
            recover_interrupted(&pre.index_path);
            return Err(status);
        }
        shadow.state.pending_compaction = Some(PendingCommit {
            index_path: pre.index_path.clone(),
            marker,
        });
        shadow.state.stats_epoch = live.stats_epoch;
        shadow.state.advance_stats_epoch();
        shadow.state.parents = None;
        Ok(std::mem::replace(live, std::mem::take(&mut shadow.state)))
    }
}

fn prepare_marker(
    pre: &Preflight,
    shadow: &Shadow,
    live: &ShardState,
) -> Result<CommitMarker, Status> {
    let previous_snapshot = live.generation.is_some();
    let legacy_files = if pre.segmented || previous_snapshot {
        Vec::new()
    } else {
        [
            pre.index_path.clone(),
            crate::node::exact_vector_sidecar_path(&pre.index_path),
            crate::node::bm25_sidecar_path(&pre.index_path),
            crate::node::live_docs_sidecar_path(&pre.index_path),
            crate::multi_res::sidecar_path(&pre.index_path),
            crate::multi_res_storage::declaration_path(&crate::multi_res::sidecar_path(
                &pre.index_path,
            )),
        ]
        .into_iter()
        .filter(|p| p.exists())
        .collect()
    };
    if pre.segmented {
        let root = crate::node::segments_root(&pre.index_path);
        let current = SegmentCatalog::read_manifest(&root)
            .map_err(Status::internal)?
            .unwrap_or_default();
        crate::segments::write_manifest_file(&manifest_backup_path(&root), &current)
            .map_err(Status::internal)?;
    }
    Ok(CommitMarker {
        format: MARKER_FORMAT,
        layout: layout_name(pre.segmented).to_string(),
        old_wal_generation: pre.cutoff_generation,
        new_wal_generation: pre.cutoff_generation + 1,
        walless: false,
        work_dir: pre.work_dir.clone(),
        previous_snapshot,
        legacy_files,
        staged_segments: shadow.staged.iter().map(|s| s.segment_id.clone()).collect(),
        replaced_segments: shadow.replaced.clone(),
    })
}

fn move_shadow_generation(pre: &Preflight, shadow: &mut Shadow) -> Result<(), Status> {
    let wal_dir = wal::wal_dir(&pre.index_path);
    let new_generation = pre.cutoff_generation + 1;
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
        .map_err(|e| Status::internal(format!("relocate the rewritten generation: {e}")))
}

fn publish_segment_shadow(
    shadow: &mut Shadow,
    live: &ShardState,
    cutover: &CatalogCutover<'_>,
) -> Result<(), Status> {
    let live_epoch = match live.bm25.as_ref() {
        Some(Bm25Shard::Segmented(live)) => live.snapshot().epoch(),
        _ => shadow.epoch_at_open,
    };
    let epoch = live_epoch
        .max(shadow.epoch_at_open)
        .checked_add(1)
        .ok_or_else(|| Status::out_of_range("compaction publication epoch exhausted"))?;
    let published = cutover
        .commit_current(epoch)
        .map_err(|e| Status::internal(format!("publish the compacted set: {e}")))?;
    if let Some(Bm25Shard::Segmented(shard)) = shadow.state.bm25.as_mut() {
        shard
            .republish_cutover(published.clone(), cutover)
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
    Ok(())
}

#[cfg(all(test, feature = "net"))]
#[path = "cutover_fence_tests.rs"]
mod tests;

//! The baseline half of a split: the children's ranges and slots, their
//! images built from the source's WAL, and their placement here.

use std::path::Path;
use std::sync::atomic::Ordering;

use crate::pb::{ClusterPlan, PlacementAction};
use crate::replication::{LiveChild, LiveReshardState};
use crate::reshard::{ChildImage, StableReshardOutput};

use super::super::state::move_image_file;
use super::super::{NodeAgent, PlacedShard, SplitChild, SplitState};
use super::{Split, SplitSource};

impl NodeAgent {
    /// Two children halving the source's hash range, on fresh,
    /// non-overlapping slot ranges above every range the plan and this
    /// node know, spaced by the source's row count rounded up to a mebi:
    /// a child's ids never reuse another shard's.
    pub(super) fn plan_split_children(
        &self,
        action: &PlacementAction,
        plan: &ClusterPlan,
        rows: u64,
    ) -> Result<Vec<SplitChild>, String> {
        let span = rows.saturating_add(1).div_ceil(1 << 20).max(1) << 20;
        let served_tops: Vec<u64> = self
            .shards()
            .values()
            .map(|shard| shard.node.config().slot_offset.saturating_add(rows))
            .collect();
        let known_top = plan
            .replicas
            .iter()
            .map(|record| record.slot_offset.saturating_add(record.rows))
            .chain(served_tops)
            .max()
            .unwrap_or(0);
        let base = known_top.div_ceil(span).saturating_mul(span);
        let mid = action.hash_lo + (action.hash_hi - action.hash_lo) / 2;
        let children = vec![
            SplitChild {
                shard_id: format!("{}-0", action.shard_id),
                hash_lo: action.hash_lo,
                hash_hi: mid,
                slot_offset: base,
            },
            SplitChild {
                shard_id: format!("{}-1", action.shard_id),
                hash_lo: mid + 1,
                hash_hi: action.hash_hi,
                slot_offset: base.saturating_add(span),
            },
        ];
        for child in &children {
            let served = self.shards().contains_key(&child.shard_id);
            if served || plan.replicas.iter().any(|r| r.shard_id == child.shard_id) {
                return Err(format!(
                    "split child shard id {:?} already exists",
                    child.shard_id
                ));
            }
        }
        Ok(children)
    }

    /// Build the children's baseline images from the source's WAL, place
    /// them here, and record the live cursor at the build's cutoff.
    pub(super) async fn build_split_children(&self, split: &mut Split<'_>) -> Result<(), String> {
        let work = split.dir.join("build");
        let _ = std::fs::remove_dir_all(&work);
        let built = self
            .build_split_images(&split.state, &split.source, &work)
            .await?;
        let cutoff = *built
            .source_cutoffs
            .first()
            .ok_or_else(|| "the split reported no source cutoff".to_string())?;
        let mut live_children = Vec::with_capacity(split.state.children.len());
        for (child, image) in split.state.children.iter().zip(&built.images.children) {
            let target_generation = split.action.target_generation;
            live_children.push(
                self.place_split_child(child, image, target_generation)
                    .await?,
            );
        }
        let _ = std::fs::remove_dir_all(&work);
        let topology = split.plan.topology_generation;
        split.state.live = Some(LiveReshardState {
            source: split.source.addr.clone(),
            source_wal_generation: cutoff.generation,
            source_clock: cutoff.high_watermark,
            old_topology_generation: topology,
            new_topology_generation: topology.saturating_add(1),
            children: live_children,
        });
        split.state.built = true;
        split.save()?;
        self.inner.stats.installs.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// The ranged stable split of the source's newest generation, on a
    /// blocking thread with the node's analysis backend.
    async fn build_split_images(
        &self,
        state: &SplitState,
        source: &SplitSource,
        work: &Path,
    ) -> Result<StableReshardOutput, String> {
        let ranges: Vec<(u64, u64)> = state
            .children
            .iter()
            .map(|child| (child.hash_lo, child.hash_hi))
            .collect();
        let offsets: Vec<u64> = state
            .children
            .iter()
            .map(|child| child.slot_offset)
            .collect();
        let generation = crate::reshard::resolve_gen(&crate::wal::wal_dir(&source.index))?;
        let template = &self.inner.config.template;
        let analysis_addr = template.analysis_addr.clone().ok_or_else(|| {
            "the node has no analysis backend; a split rebuilds the children's postings".to_string()
        })?;
        let bm25_fields = template.bm25_fields.clone();
        let handle = tokio::runtime::Handle::current();
        let build_dir = work.to_path_buf();
        tokio::task::spawn_blocking(move || {
            let mut analyze = move |docs: &[(
                &str,
                Option<&crate::pb::AnalysisSpec>,
                crate::analyzer::SessionLayers,
            )]| {
                handle
                    .block_on(crate::analyzer::analyze_batch_streams(
                        &analysis_addr,
                        docs,
                        1,
                    ))
                    .map_err(|error| error.to_string())
            };
            crate::reshard::split_stable_logs_ranged(
                &[generation],
                &ranges,
                &build_dir,
                &offsets,
                false,
                (!bm25_fields.is_empty()).then_some(bm25_fields.as_slice()),
                &mut analyze,
            )
        })
        .await
        .map_err(|e| format!("split build task: {e}"))?
    }

    /// Move one child's built image into its shard directory and serve it.
    async fn place_split_child(
        &self,
        child: &SplitChild,
        image: &ChildImage,
        target_generation: u64,
    ) -> Result<LiveChild, String> {
        let dir = self.shard_dir(&child.shard_id);
        std::fs::create_dir_all(&dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
        let target = dir.join("shard");
        move_image_file(&image.vector_path, &target)?;
        move_image_file(
            &image.exact_vector_path,
            &crate::node::exact_vector_sidecar_path(&target),
        )?;
        if let Some(bm25) = &image.bm25_path {
            move_image_file(bm25, &crate::node::bm25_sidecar_path(&target))?;
        }
        let placed = PlacedShard {
            shard_id: child.shard_id.clone(),
            collection: self.inner.config.collection.clone(),
            slot_offset: child.slot_offset,
            hash_lo: child.hash_lo,
            hash_hi: child.hash_hi,
            source_generation: target_generation,
            installed: true,
            ..Default::default()
        };
        let addr = self.serve_placed(placed).await?;
        Ok(LiveChild {
            addr,
            replica: None,
            hash_lo: child.hash_lo,
            hash_hi: child.hash_hi,
            slot_offset: child.slot_offset,
            base_vectors: image.num_vectors,
            base_document_slots: image.num_documents,
            applied_vectors: 0,
            applied_documents: 0,
        })
    }
}

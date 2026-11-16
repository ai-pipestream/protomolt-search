//! Prepare a from-segments cutover and fence it against concurrent writes.

use super::*;

impl NodeServiceImpl {
    fn rebase_carried_segments(
        source: &[SegmentMetadata],
        manifest_now: &SegmentSetManifest,
        build: &crate::reshard::SegmentCompactionBuild,
        staged: &[SegmentMetadata],
    ) -> Result<(Vec<SegmentMetadata>, u64, u64, u64), Status> {
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
        let mut segments: Vec<SegmentMetadata> = staged.to_vec();
        let mut base = dense;
        for segment in carried {
            let mut rebased = segment.clone();
            rebased.base_label = base;
            base += segment.rows;
            segments.push(rebased);
        }
        Ok((segments, s_rows, total, dense))
    }

    fn translate_live_overlay(
        overlay_now: &LiveDocs,
        id_map: &std::collections::BTreeMap<u64, u64>,
        slot_offset: u64,
        s_rows: u64,
        total: u64,
        dense: u64,
    ) -> Result<LiveDocs, Status> {
        let mut overlay = LiveDocs::default();
        let Some(words) = overlay_now.words() else {
            return Ok(overlay);
        };
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
                    if let Some(&new_local) = id_map.get(&global) {
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
        Ok(overlay)
    }

    /// Prepare the cutover with no lock held past a read: the final
    /// manifest (the rebuilt partitions, then every segment sealed after
    /// the cutoff, re-based onto the dense row count), the live overlay
    /// translated onto the new labels, and the shadow serving state
    /// opened over the prepared catalog. The fence at install re-checks
    /// the epoch, the overlay revision, and the tail.
    pub(super) fn prepare_segment_cutover(
        &self,
        pre: &SegmentPreflight,
        build: &crate::reshard::SegmentCompactionBuild,
        staged: &[SegmentMetadata],
    ) -> Result<PreparedSegmentCutover, Status> {
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
        let (segments, s_rows, total, dense) =
            Self::rebase_carried_segments(source, &manifest_now, build, staged)?;
        let overlay = Self::translate_live_overlay(
            &overlay_now,
            &build.id_map,
            self.config.slot_offset,
            s_rows,
            total,
            dense,
        )?;
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
        let (index, exact_vectors) = match open_segmented_vectors(&set)? {
            Some((vec_index, exact)) => (Some(vec_index), Some(exact)),
            None => (None, None),
        };
        Ok(PreparedSegmentCutover {
            multi_res_vectors: set.retained_vectors().map_err(Status::internal)?,
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
    pub(super) fn segment_cutover_fence(
        &self,
        guard: &ShardState,
        prepared: &PreparedSegmentCutover,
    ) -> bool {
        let Some(Bm25Shard::Segmented(shard)) = guard.bm25.as_ref() else {
            return false;
        };
        shard.snapshot().epoch() == prepared.epoch
            && guard.live_docs.revision() == prepared.revision
            && Self::segment_tail_is_clear(guard)
    }
}

//! Source HNSW graphs a compaction may reuse instead of rebuilding.

use crate::vector::{VectorIndex, VectorProvider};

/// Sealed HNSW segment images available for donor transplant and join-set
/// graph merge during shard compaction.
pub(crate) struct HnswCompactionSources {
    slot_offset: u64,
    ranges: Vec<HnswSourceRange>,
}

struct HnswSourceRange {
    base_label: u64,
    rows: u64,
    index: std::sync::Arc<VectorIndex>,
}

impl HnswCompactionSources {
    pub(crate) fn from_segment_set(
        slot_offset: u64,
        set: &crate::segments::OpenedSegmentSet,
    ) -> Option<Self> {
        let mut ranges = Vec::new();
        for i in 0..set.len() {
            if let Some(arc) = set.vector_arc(i) {
                if let Some(hnsw) = arc.as_hnsw() {
                    if hnsw.is_graph_linked() {
                        let meta = set.metadata(i);
                        ranges.push(HnswSourceRange {
                            base_label: meta.base_label,
                            rows: meta.rows,
                            index: std::sync::Arc::clone(arc),
                        });
                    }
                }
            }
        }
        (!ranges.is_empty()).then(|| {
            ranges.sort_by_key(|r| r.base_label);
            Self {
                slot_offset,
                ranges,
            }
        })
    }

    pub(super) fn build_merged_hnsw(
        &self,
        dim: usize,
        backend_config: &crate::vector::VectorBackendConfig,
        vectors: &[(u64, Vec<f32>)],
    ) -> Result<Option<crate::hnsw::HnswIndex>, String> {
        if backend_config.backend_kind != crate::hnsw::HNSW || self.ranges.is_empty() {
            return Ok(None);
        }
        let config = crate::hnsw::HnswConfig::from_backend_config(backend_config)
            .map_err(|e| format!("decode child hnsw config: {e}"))?;
        let sources: Vec<&crate::hnsw::HnswIndex> = self
            .ranges
            .iter()
            .map(|r| r.index.as_hnsw().expect("checked in constructor"))
            .collect();
        let mut ordered = Vec::with_capacity(vectors.len());
        for (global_id, vec) in vectors {
            let mapped = global_id.checked_sub(self.slot_offset).and_then(|rel| {
                let pos = self
                    .ranges
                    .partition_point(|r| r.base_label.saturating_add(r.rows) <= rel);
                let range = self.ranges.get(pos)?;
                if rel >= range.base_label && rel < range.base_label.saturating_add(range.rows) {
                    let old_node = u32::try_from(rel - range.base_label).ok()?;
                    Some((pos, old_node))
                } else {
                    None
                }
            });
            match mapped {
                Some((seg_idx, old_node)) => {
                    ordered.push(crate::hnsw::merge::OrderedMergeRow::FromSegment {
                        segment: seg_idx,
                        old_node,
                        vector: vec.as_slice(),
                    })
                }
                None => ordered.push(crate::hnsw::merge::OrderedMergeRow::Raw(vec.as_slice())),
            }
        }
        let (merged, _) = crate::hnsw::HnswIndex::merge_ordered(
            dim,
            config,
            &sources,
            &ordered,
            crate::hnsw::MergeStrategy::JoinSet,
        )
        .map_err(|e| format!("merge child hnsw segments: {e}"))?;
        Ok(Some(merged))
    }

    pub(super) fn build_merged_index(
        &self,
        dim: usize,
        backend_config: &crate::vector::VectorBackendConfig,
        vectors: &[(u64, Vec<f32>)],
    ) -> Result<Option<VectorIndex>, String> {
        Ok(self
            .build_merged_hnsw(dim, backend_config, vectors)?
            .map(crate::hnsw::HnswIndex::into_index))
    }
}

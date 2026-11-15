//! Per-shard lexical/columnar read surfaces and synchronized shard state.

mod bm25_shard;
mod columns;
mod facets;
mod filters;
mod state;

pub use self::bm25_shard::Bm25Shard;
pub(crate) use self::columns::{
    f64_from_order_bits, f64_order_bits, i64_from_order_bits, i64_order_bits, parse_score_stages,
    ScoreStageSpec,
};
pub(in crate::node) use self::columns::{
    int_min_max_as_f64, projected_value, uint_min_max_as_f64, EmptyColumns, ShardNumericRead,
};
pub(crate) use self::facets::{
    timestamp_to_epoch_micros, validate_geo_filter, validate_geo_filters,
    validate_range_facet_fields,
};
pub(in crate::node) use self::facets::{unknown_range_counts, validate_lat_lon};
pub(in crate::node) use self::filters::{
    filter_known_flags, prune_segments, resolve_shard_filters, ResolvedShardFilters, SegmentPrune,
};
pub(in crate::node) use self::state::{
    active_artifact_rows, live_document_count, live_field_stats, IngestGuard,
};
pub(crate) use self::state::{
    physical_rows, read_shard, write_shard, ParentMap, ShardState, STALE_STATS_EPOCH,
};

pub(in crate::node) struct DocPassSpec<'a> {
    pub geo_filters: &'a [crate::pb::GeoFilter],
    pub geo_regions: &'a [crate::geo::GeoRegion],
    pub filter: Option<&'a crate::pb::FilterExpr>,
    pub doc_ids: Option<&'a [u64]>,
}

pub(in crate::node) struct AdmittedDocPass<'a> {
    pub doc_filter: crate::filter::DocFilter<'a>,
    pub cols: ShardNumericRead<'a>,
    pub prune: SegmentPrune,
    pub rows: u32,
    pub id_allowlist: Option<std::collections::HashSet<u32>>,
}

impl<'a> AdmittedDocPass<'a> {
    pub(in crate::node) fn admits(
        &self,
        doc: u32,
        membership: Option<&crate::boolean_bits::Bits>,
    ) -> bool {
        if self
            .id_allowlist
            .as_ref()
            .is_some_and(|allowlist| !allowlist.contains(&doc))
        {
            return false;
        }
        if membership.is_some_and(|members| !members.test(doc as usize)) {
            return false;
        }
        self.doc_filter.passes(doc, &self.cols)
    }
}

impl super::NodeServiceImpl {
    pub(in crate::node) fn admitted_doc_pass<'a>(
        &self,
        guard: &'a ShardState,
        store: &'a Bm25Shard,
        spec: DocPassSpec<'a>,
    ) -> Result<AdmittedDocPass<'a>, tonic::Status> {
        let doc_filter = crate::filter::DocFilter {
            deleted: guard.live_docs.words(),
            geo: store.resolve_geo_filters(spec.geo_filters, spec.geo_regions),
            pred: spec
                .filter
                .map(|expr| store.resolve_filter(expr))
                .transpose()?,
            phrase: Vec::new(),
        };
        let cols = ShardNumericRead(store);
        let rows = store.next_doc_id();
        let prune = prune_segments(
            store,
            doc_filter.pred.as_ref(),
            self.knobs.segment_pruning(),
        );
        let id_allowlist = spec.doc_ids.map(|ids| {
            ids.iter()
                .filter_map(|id| id.checked_sub(self.config.slot_offset))
                .filter_map(|local| u32::try_from(local).ok())
                .collect()
        });
        Ok(AdmittedDocPass {
            doc_filter,
            cols,
            prune,
            rows,
            id_allowlist,
        })
    }

    pub(in crate::node) fn prepare_exact_sidecar(
        guard: &ShardState,
        slot_offset: u64,
        slot_span: Option<usize>,
    ) -> Result<(&crate::exact_vectors::ExactVectorStore, usize), tonic::Status> {
        let exact = guard.exact_vectors.as_ref().ok_or_else(|| {
            let range_end = slot_span.map_or_else(String::new, |n| (slot_offset + n as u64).to_string());
            tonic::Status::failed_precondition(format!(
                "FP32 rerank requested for shard slots {slot_offset}..{range_end} but this generation \
                 has no exact-vector sidecar; rebuild or backfill it"
            ))
        })?;
        if let Some(index) = guard.index.as_ref() {
            if exact.len() != index.len() || exact.dim() != index.dim_opt() {
                return Err(tonic::Status::failed_precondition(format!(
                    "exact-vector sidecar shape {:?}x{} does not match provider shape {:?}x{}",
                    exact.dim(),
                    exact.len(),
                    index.dim_opt(),
                    index.len()
                )));
            }
        }
        let dim = exact.dim().ok_or_else(|| {
            tonic::Status::failed_precondition("exact-vector sidecar has no dimension")
        })?;
        Ok((exact, dim))
    }
}

pub(in crate::node) fn score_exact_slots_checked(
    exact: &crate::exact_vectors::ExactVectorStore,
    dim: usize,
    vector: &[f32],
    slots: &[usize],
    max_logical_bytes: u64,
    parallel: usize,
) -> Result<crate::exact_vectors::ScoredSlots, tonic::Status> {
    let predicted = (slots.len() as u64)
        .checked_mul(dim as u64)
        .and_then(|bytes| bytes.checked_mul(4))
        .ok_or_else(|| tonic::Status::resource_exhausted("FP32 rerank byte count overflow"))?;
    if max_logical_bytes != 0 && predicted > max_logical_bytes {
        return Err(tonic::Status::resource_exhausted(format!(
            "FP32 rerank needs {predicted} logical row bytes on this shard, above \
             max_logical_bytes={max_logical_bytes}"
        )));
    }
    exact
        .score_slots_profiled(vector, slots, parallel)
        .map_err(|e| tonic::Status::invalid_argument(e.to_string()))
}

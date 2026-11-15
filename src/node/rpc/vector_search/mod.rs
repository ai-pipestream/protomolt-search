//! Streaming and rescored dense vector search RPC handlers.

mod search_shard;
mod stream_scan;
mod stream_search;

use crate::node::*;

pub(super) fn validate_scan_filters(
    geo_filters: &[crate::pb::GeoFilter],
    filter: Option<&crate::pb::FilterExpr>,
) -> Result<Vec<crate::geo::GeoRegion>, Status> {
    let geo_regions = validate_geo_filters(geo_filters)?;
    if let Some(expr) = filter {
        crate::filter::validate_filter(expr)?;
    }
    Ok(geo_regions)
}

pub(super) fn validate_query_dim(vector: &[f32], dim: usize) -> Result<(), Status> {
    if vector.len() != dim {
        return Err(Status::invalid_argument(format!(
            "query vector has dim {}, index expects {dim}",
            vector.len()
        )));
    }
    if let Some((_, coord, value)) = first_invalid_coordinate(vector, dim) {
        return Err(Status::invalid_argument(format!(
            "query coordinate {coord} is invalid: {value}"
        )));
    }
    Ok(())
}

pub(super) fn collect_rerank_candidates(
    guard: &ShardState,
    include: bool,
    slots: impl IntoIterator<Item = u32>,
) -> HashMap<u32, crate::pb::RerankCandidatePayload> {
    if !include {
        return HashMap::new();
    }
    slots
        .into_iter()
        .filter_map(|slot| guard.rerank_candidate_for(slot).map(|c| (slot, c)))
        .collect()
}

pub(super) struct DonePayload {
    pub hits: Vec<(u32, f32, u64)>,
    pub stats: ScanStats,
    pub identities: crate::source_archive::IdentitySnapshot,
    pub rerank_candidates: HashMap<u32, crate::pb::RerankCandidatePayload>,
    pub geo_columns_known: Vec<bool>,
    pub filter_columns_known: Vec<bool>,
}

pub(super) fn build_search_shard_done(
    slot_offset: u64,
    mut payload: DonePayload,
) -> SearchShardDone {
    let stats = payload.stats;
    SearchShardDone {
        hits: payload
            .hits
            .into_iter()
            .map(|(slot, score, parent_id)| ScoredHit {
                vector_id: slot_offset + u64::from(slot),
                score,
                parent_id,
                identity: payload.identities.identity(slot),
                rerank_candidate: payload.rerank_candidates.remove(&slot),
            })
            .collect(),
        stats: Some(ShardScanStats {
            chunk_calls: stats.chunk_calls,
            candidates_collected: stats.candidates_collected,
            floors_published: stats.floors_published,
            floor_updates_applied: stats.floor_updates_applied,
            floors_offered: stats.floors_offered,
            segments_total: stats.segments_total,
            segments_skipped: stats.segments_skipped,
            filter_regimes: stats.regimes.to_proto(),
            tail_enumerated_rows: stats.tail_enumerated_rows,
            patience_stopped: stats.patience_stopped,
            floor_pruned: stats.floor_pruned,
        }),
        geo_columns_known: payload.geo_columns_known,
        filter_columns_known: payload.filter_columns_known,
    }
}

fn run_vector_rescore(
    state: &Arc<RwLock<ShardState>>,
    offset: u64,
    req: VectorRescoreRequest,
) -> Result<VectorRescoreResponse, Status> {
    let scope = crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
    let guard = read_shard(state);
    guard.check_stats_epoch(req.expected_stats_epoch, &req.expected_stats_incarnation)?;
    let (_, visibility_columns_known) = filter_known_flags(
        guard.bm25.as_ref(),
        &[],
        req.visibility
            .as_ref()
            .and_then(|view| view.filter.as_ref()),
    );
    let mut response = VectorRescoreResponse {
        vector_binding: guard.vector_binding(&req.field)?,
        stats_epoch: guard.stats_epoch,
        stats_incarnation: guard.stats_incarnation.bytes()?,
        visibility_fingerprint: scope.fingerprint().to_vec(),
        visibility_columns_known,
        ..Default::default()
    };
    let Some(index) = guard.index.as_ref() else {
        return Ok(response);
    };
    let Some(dim) = index.dim_opt() else {
        return Ok(response);
    };
    validate_query_dim(&req.vector, dim)?;
    let n = index.len();
    let mut mask = vec![false; n];
    let slots =
        guard.visible_candidate_slots(req.visibility.as_ref(), &req.candidate_ids, offset, n)?;
    let allowed = slots.len();
    for slot in slots {
        mask[slot] = true;
    }
    if allowed == 0 {
        return Ok(response);
    }
    let results = index
        .try_search_with_mask(&req.vector, allowed, Some(&mask))
        .map_err(|e| Status::invalid_argument(e.to_string()))?;
    response.hits = results
        .indices_for_query(0)
        .iter()
        .zip(results.scores_for_query(0))
        .filter(|&(&slot, _)| slot >= 0)
        .map(|(&slot, &score)| RawLegHit {
            doc_id: offset + slot as u64,
            score,
            rerank_candidate: None,
        })
        .collect();
    Ok(response)
}

fn run_exact_vector_rescore(
    state: &Arc<RwLock<ShardState>>,
    offset: u64,
    parallel: usize,
    req: ExactVectorRescoreRequest,
) -> Result<ExactVectorRescoreResponse, Status> {
    let scope = crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
    let guard = read_shard(state);
    guard.check_stats_epoch(req.expected_stats_epoch, &req.expected_stats_incarnation)?;
    let (_, visibility_columns_known) = filter_known_flags(
        guard.bm25.as_ref(),
        &[],
        req.visibility
            .as_ref()
            .and_then(|view| view.filter.as_ref()),
    );
    let response = ExactVectorRescoreResponse {
        vector_binding: guard.vector_binding(&req.field)?,
        stats_epoch: guard.stats_epoch,
        stats_incarnation: guard.stats_incarnation.bytes()?,
        visibility_fingerprint: scope.fingerprint().to_vec(),
        visibility_columns_known,
        ..Default::default()
    };
    // Product shards may own exact rows for a clustered vector collection
    // without serving a local provider image; take the maximum aligned span.
    let n = guard
        .index
        .as_ref()
        .map_or(0, VectorIndex::len)
        .max(
            guard
                .exact_vectors
                .as_ref()
                .map_or(0, ExactVectorStore::len),
        )
        .max(guard.bm25.as_ref().map_or(0, |s| s.next_doc_id() as usize));
    let slots =
        guard.visible_candidate_slots(req.visibility.as_ref(), &req.candidate_ids, offset, n)?;
    if slots.is_empty() {
        return Ok(response);
    }
    let (exact, dim) = NodeServiceImpl::prepare_exact_sidecar(&guard, offset, Some(n))?;
    let slots: Vec<usize> = slots
        .into_iter()
        .filter(|&slot| exact.contains_row(slot))
        .collect();
    let scored = score_exact_slots_checked(
        exact,
        dim,
        &req.vector,
        &slots,
        req.max_logical_bytes,
        parallel,
    )?;
    Ok(ExactVectorRescoreResponse {
        hits: scored
            .rows
            .into_iter()
            .map(|(slot, score)| RawLegHit {
                doc_id: offset + slot as u64,
                score,
                rerank_candidate: None,
            })
            .collect(),
        logical_bytes: scored.logical_bytes,
        pages_touched: scored.pages_touched,
        tasks: scored.tasks,
        ..response
    })
}

impl NodeServiceImpl {
    pub(in crate::node::rpc) async fn handle_vector_rescore(
        &self,
        request: Request<VectorRescoreRequest>,
    ) -> Result<Response<VectorRescoreResponse>, Status> {
        crate::metrics::timed(Route::VectorRescore, request, |request| async move {
            let req = request.into_inner();
            let offset = self.config.slot_offset;
            let state = self.state.clone();
            let response =
                tokio::task::spawn_blocking(move || run_vector_rescore(&state, offset, req))
                    .await
                    .map_err(|e| Status::internal(format!("vector rescore task failed: {e}")))??;
            Ok(Response::new(response))
        })
        .await
    }

    pub(in crate::node::rpc) async fn handle_exact_vector_rescore(
        &self,
        request: Request<ExactVectorRescoreRequest>,
    ) -> Result<Response<ExactVectorRescoreResponse>, Status> {
        crate::metrics::timed(Route::ExactVectorRescore, request, |request| async move {
            let req = request.into_inner();
            let offset = self.config.slot_offset;
            let state = self.state.clone();
            let parallel = resolved_rerank_parallel(self.config.rerank_parallel);
            let lanes = crate::exact_vectors::rerank_task_count(req.candidate_ids.len(), parallel);
            let permits = self
                .rerank_slots
                .clone()
                .acquire_many_owned(lanes as u32)
                .await
                .map_err(|_| Status::unavailable("exact rerank worker budget closed"))?;
            let result = tokio::task::spawn_blocking(move || {
                let _permits = permits;
                run_exact_vector_rescore(&state, offset, parallel, req)
            })
            .await
            .map_err(|e| Status::internal(format!("exact vector rescore task failed: {e}")))??;
            Ok(Response::new(result))
        })
        .await
    }
}

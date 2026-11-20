//! Dictionary scans, BM25 query, hybrid leg, and SPLADE RPC handlers.

mod query_stream;
mod term_stats;

use crate::node::*;

impl NodeServiceImpl {
    pub(super) async fn handle_expand_term_prefix(
        &self,
        request: Request<crate::pb::ExpandTermPrefixRequest>,
    ) -> Result<Response<crate::pb::ExpandTermPrefixResponse>, Status> {
        crate::metrics::timed(Route::ExpandTermPrefix, request, |request| async move {
            let req = request.into_inner();
            let result = self.scan_dictionary(
                &req.field,
                &req.prefix,
                req.cap as usize,
                req.visibility.as_ref(),
            )?;
            Ok(Response::new(crate::pb::ExpandTermPrefixResponse {
                terms: result.entries.into_iter().map(|entry| entry.term).collect(),
                count: result.count,
                known: result.known,
                visibility_fingerprint: result.visibility_fingerprint,
                visibility_columns_known: result.visibility_columns_known,
            }))
        })
        .await
    }

    pub(super) async fn handle_suggest_terms(
        &self,
        request: Request<crate::pb::SuggestTermsRequest>,
    ) -> Result<Response<crate::pb::SuggestTermsResponse>, Status> {
        crate::metrics::timed(Route::SuggestTerms, request, |request| async move {
            let req = request.into_inner();
            let cap = usize::try_from(req.max_scan)
                .map_err(|_| Status::invalid_argument("max_scan is out of range"))?;
            if cap == 0 {
                return Err(Status::invalid_argument(
                    "max_scan must be positive: it bounds the dictionary scan",
                ));
            }
            self.scan_dictionary(&req.field, &req.prefix, cap, req.visibility.as_ref())
                .map(Response::new)
        })
        .await
    }

    pub(super) async fn handle_bm25_query(
        &self,
        request: Request<Bm25QueryRequest>,
    ) -> Result<Response<Bm25QueryResponse>, Status> {
        crate::metrics::timed(Route::Bm25Query, request, |request| async move {
            let service = self.clone();
            let req = request.into_inner();
            tokio::task::spawn_blocking(move || service.run_bm25_query(req))
                .await
                .map_err(|e| Status::internal(format!("bm25 query task failed: {e}")))?
                .map(Response::new)
        })
        .await
    }

    pub(super) async fn handle_bm25_phrase_query(
        &self,
        request: Request<crate::pb::Bm25PhraseQueryRequest>,
    ) -> Result<Response<Bm25QueryResponse>, Status> {
        crate::metrics::timed(Route::Bm25PhraseQuery, request, |request| async move {
            let service = self.clone();
            let req = request.into_inner();
            tokio::task::spawn_blocking(move || service.run_bm25_phrase_query(req))
                .await
                .map_err(|error| {
                    Status::internal(format!("bm25 phrase query task failed: {error}"))
                })?
                .map(Response::new)
        })
        .await
    }

    pub(super) async fn handle_bm25_rescore(
        &self,
        request: Request<Bm25RescoreRequest>,
    ) -> Result<Response<Bm25RescoreResponse>, Status> {
        crate::metrics::timed(Route::Bm25Rescore, request, |request| async move {
            let service = self.clone();
            let req = request.into_inner();
            tokio::task::spawn_blocking(move || service.run_bm25_rescore(req))
                .await
                .map_err(|e| Status::internal(format!("bm25 rescore task failed: {e}")))?
                .map(Response::new)
        })
        .await
    }

    pub(super) async fn handle_hybrid_shard(
        &self,
        request: Request<HybridShardRequest>,
    ) -> Result<Response<HybridShardResponse>, Status> {
        crate::metrics::timed(Route::HybridShard, request, |request| async move {
            let service = self.clone();
            tokio::task::spawn_blocking(move || service.run_hybrid(request.into_inner()))
                .await
                .map_err(|e| Status::internal(format!("hybrid task failed: {e}")))?
                .map(Response::new)
        })
        .await
    }

    pub(super) async fn handle_shard_legs(
        &self,
        request: Request<ShardLegsRequest>,
    ) -> Result<Response<ShardLegsResponse>, Status> {
        crate::metrics::timed(Route::ShardLegs, request, |request| async move {
            let req = request.into_inner();
            if req.terms.len() != req.global_doc_frequencies.len() {
                return Err(Status::invalid_argument(
                    "terms and global_doc_frequencies must have the same length",
                ));
            }
            let geo_regions = validate_geo_filters(&req.geo_filters)?;
            if let Some(f) = req.filter.as_ref() {
                crate::filter::validate_filter(f)?;
            }
            let service = self.clone();
            tokio::task::spawn_blocking(move || {
                let mut legs = service.compute_legs(LegCompute {
                    vector: &req.vector,
                    terms: &req.terms,
                    analysis_fingerprint: req.analysis_fingerprint,
                    global_doc_count: req.global_doc_count,
                    global_total_doc_length: req.global_total_doc_length,
                    global_doc_frequencies: &req.global_doc_frequencies,
                    params: params_from(req.k1, req.b)?,
                    k: req.k as usize,
                    expected_stats_epoch: req.expected_stats_epoch,
                    expected_stats_incarnation: &req.expected_stats_incarnation,
                    filters: &LegFilters {
                        geo: &req.geo_filters,
                        regions: geo_regions,
                        tree: req.filter.as_ref(),
                    },
                    read_context: req.read_context.as_ref(),
                    include_rerank_candidate: req.include_rerank_candidate,
                    vector_controls: VectorLegControls {
                        enumerate_max_rows: req.enumerate_max_rows,
                        filter_strategy: req.filter_strategy,
                        enumerate_all: req.enumerate_all,
                        patience_saturation_ppm: req.patience_saturation_ppm,
                        patience_hops: req.patience_hops,
                        floor_prune: req.floor_prune,
                        floor_greediness_ppm: req.floor_greediness_ppm,
                        floor_clamp_min: req.floor_clamp_min,
                    },
                })?;
                Ok(ShardLegsResponse {
                    vector_hits: legs
                        .vector
                        .into_iter()
                        .map(|(doc_id, score)| RawLegHit {
                            doc_id,
                            score: score as f32,
                            rerank_candidate: legs.rerank_candidates.get(&doc_id).cloned(),
                        })
                        .collect(),
                    bm25_hits: legs
                        .bm25
                        .into_iter()
                        .map(|(doc_id, score)| RawLegHit {
                            doc_id,
                            score: score as f32,
                            rerank_candidate: legs.rerank_candidates.remove(&doc_id),
                        })
                        .collect(),
                    geo_columns_known: legs.geo_columns_known,
                    filter_columns_known: legs.filter_columns_known,
                    read_receipt: legs.read_receipt,
                    vector_scan: legs.vector_scan,
                })
            })
            .await
            .map_err(|e| Status::internal(format!("shard legs task failed: {e}")))?
            .map(Response::new)
        })
        .await
    }

    pub(super) async fn handle_score_splade(
        &self,
        request: Request<crate::pb::ScoreSpladeRequest>,
    ) -> Result<Response<crate::pb::ScoreSpladeResponse>, Status> {
        crate::metrics::timed(Route::ScoreSplade, request, |request| async move {
            let service = self.clone();
            tokio::task::spawn_blocking(move || service.run_score_splade(request.into_inner()))
                .await
                .map_err(|e| Status::internal(format!("score splade task failed: {e}")))?
                .map(Response::new)
        })
        .await
    }
}

//! Query RPC admission, execution, and response assembly.

use super::*;

impl CoordinatorServiceImpl {
    /// Public unified query adapter delegating to route handlers (`src/query.rs`).
    pub(super) async fn rpc_query(
        &self,
        request: Request<crate::pb::QueryRequest>,
    ) -> Result<Response<crate::pb::QueryResponse>, Status> {
        crate::metrics::timed(Route::Query, request, |request| async move {
            self.admit(&request.get_ref().collection)?;
            if let Some(snapshot) = self.request_snapshot() {
                return Box::pin(SearchService::query(
                    &snapshot,
                    crate::metrics::nested(request),
                ))
                .await;
            }
            let grpc_budget = crate::relay::grpc_timeout(request.metadata());
            let finish_deadline_us = grpc_budget.map_or(0, |dur| {
                let now_us = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_micros() as u64;
                now_us
                    .saturating_add(dur.as_micros().min(u128::from(u64::MAX)) as u64)
                    .max(1)
            });
            let access = request
                .extensions()
                .get::<crate::pb::AccessDecision>()
                .cloned();
            let request = request.into_inner();
            self.require_topology_generation(request.required_topology_generation)?;
            let response = self
                .execute_query(request, access.as_ref(), finish_deadline_us)
                .await?;
            Ok(Response::new(response))
        })
        .await
    }
}

impl CoordinatorServiceImpl {
    pub(super) async fn rpc_search(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        crate::metrics::timed(Route::Search, request, |request| async move {
            self.admit(&request.get_ref().collection)?;
            if let Some(snapshot) = self.request_snapshot() {
                return Box::pin(SearchService::search(
                    &snapshot,
                    crate::metrics::nested(request),
                ))
                .await;
            }
            let mut req = request.into_inner();
            if !req.field.is_empty() {
                let scoped = self.for_vector_field(&req.field)?;
                req.field.clear();
                return Box::pin(SearchService::search(
                    &scoped,
                    crate::metrics::nested(Request::new(req)),
                ))
                .await;
            }
            if self.scoped_vector_scan() && self.query_read_versions.is_none() {
                let mut filters = RequestFilters::compile(&req.geo_filters, &req.filter)?;
                filters.floor_prune = req.floor_prune;
                filters.floor_greediness_ppm = req.floor_greediness_ppm;
                filters.floor_clamp_min = req.floor_clamp_min;
                self.check_vector_scan(&filters, req.collapse_parents)?;
                let (pinned, reads) = self.pin_read_versions().await?;
                let result = Box::pin(SearchService::search(
                    &pinned,
                    crate::metrics::nested(Request::new(req)),
                ))
                .await;
                pinned.validate_read_versions(&reads).await?;
                return result;
            }
            let k = self.resolve_k(req.k)?;
            if req.vector.is_empty() {
                return Err(Status::invalid_argument("empty query vector"));
            }
            if req.vector.iter().any(|x| !x.is_finite()) {
                return Err(Status::invalid_argument(
                    "query vector has non-finite coordinates",
                ));
            }
            let request_id = if req.request_id.is_empty() {
                format!(
                    "req-{}-{}",
                    std::process::id(),
                    REQUEST_COUNTER.fetch_add(1, AtomicOrdering::Relaxed)
                )
            } else {
                req.request_id.clone()
            };
            // CEL text compiles ONCE, here, into the predicate IR the
            // shards execute; no shard ever sees CEL text.
            let mut filters = RequestFilters::compile(&req.geo_filters, &req.filter)?;
            filters.enumerate_max_rows = req.enumerate_max_rows;
            filters.patience_saturation_ppm = req.patience_saturation_ppm;
            filters.patience_hops = req.patience_hops;
            filters.finish_settle_us = req.finish_settle_us;
            filters.finish_deadline_us = req.finish_deadline_us;
            filters.floor_prune = req.floor_prune;
            filters.floor_k = Some(k);
            filters.floor_greediness_ppm = req.floor_greediness_ppm;
            filters.floor_clamp_min = req.floor_clamp_min;
            filters.include_rerank_candidate = req.include_rerank_candidate;
            let _ = self.resolve_finish_windows(&filters)?;
            self.check_vector_scan(&filters, req.collapse_parents)?;
            // Refuse mixed provider kinds or scoring fingerprints before fan-out.
            if !self.has_clustered_vectors() {
                self.fleet_vector_identity(true).await?;
            }

            #[cfg(feature = "net")]
            if self.clustered_vectors.is_some() {
                if req.collapse_parents {
                    let collapsed = self
                        .clustered_parent_collapse(&request_id, &req.vector, k, &filters)
                        .await?;
                    return Ok(Response::new(SearchResponse {
                        filter_regimes: None,
                        tail_enumerated_rows: 0,
                        patience_stopped: 0,
                        shards_finished_early: 0,
                        shards_floor_pruned: 0,
                        segments_total: 0,
                        segments_skipped: 0,
                        request_id,
                        hits: collapsed.hits,
                        groups: collapsed.groups,
                        chunk_floor: collapsed.chunk_floor,
                    }));
                }
                let result = self
                    .clustered_vector_candidates(&request_id, &req.vector, k, None, false, &filters)
                    .await?;
                let hits = result
                    .hits
                    .into_iter()
                    .map(|(vector_id, score)| ScoredHit {
                        identity: None,
                        vector_id,
                        score,
                        parent_id: 0,
                        rerank_candidate: None,
                    })
                    .collect();
                return Ok(Response::new(SearchResponse {
                    filter_regimes: None,
                    tail_enumerated_rows: 0,
                    patience_stopped: 0,
                    shards_finished_early: 0,
                    shards_floor_pruned: 0,
                    segments_total: 0,
                    segments_skipped: 0,
                    request_id,
                    hits,
                    groups: Vec::new(),
                    chunk_floor: 0.0,
                }));
            }

            let use_streaming = self.stream_search
                || filters.finish_settle_us.is_some()
                || filters.finish_deadline_us.is_some();
            let result = if req.collapse_parents {
                // Document mode on a streaming coordinator: parents
                // aggregate here from tagged chunk emissions, and the
                // response carries the per-parent chunk groups. The bidi
                // path collapses shard-side and returns representatives
                // only.
                if use_streaming {
                    let doc = self
                        .fanout_stream_search_collapse(&request_id, &req.vector, k, &filters)
                        .await?;
                    return Ok(Response::new(SearchResponse {
                        filter_regimes: summed_filter_regimes(&doc.summaries),
                        tail_enumerated_rows: summed_tail_enumerated_rows(&doc.summaries),
                        patience_stopped: summed_patience_stopped(&doc.summaries),
                        shards_finished_early: doc.shards_finished_early,
                        shards_floor_pruned: doc.shards_floor_pruned,
                        segments_total: 0,
                        segments_skipped: 0,
                        request_id,
                        hits: doc.hits,
                        groups: doc.groups,
                        chunk_floor: doc.chunk_floor,
                    }));
                }
                self.fanout_search_collapse(&request_id, &req.vector, k, &filters)
                    .await?
            } else if use_streaming {
                let streamed = self
                    .fanout_stream_search(&request_id, &req.vector, k, None, &filters)
                    .await?;
                return Ok(Response::new(SearchResponse {
                    filter_regimes: summed_filter_regimes(&streamed.summaries),
                    tail_enumerated_rows: summed_tail_enumerated_rows(&streamed.summaries),
                    patience_stopped: summed_patience_stopped(&streamed.summaries),
                    shards_finished_early: streamed.shards_finished_early,
                    shards_floor_pruned: streamed.shards_floor_pruned,
                    segments_total: 0,
                    segments_skipped: 0,
                    request_id,
                    hits: streamed.hits,
                    groups: Vec::new(),
                    chunk_floor: 0.0,
                }));
            } else {
                self.fanout_search(&request_id, &req.vector, k, false, &filters)
                    .await?
            };
            let (segments_total, segments_skipped) = result.shard_stats.iter().flatten().fold(
                (0u32, 0u32),
                |(total, skipped), stats| {
                    (
                        total.saturating_add(stats.segments_total),
                        skipped.saturating_add(stats.segments_skipped),
                    )
                },
            );
            let mut regimes = crate::vector::FilterRegimeCounts::default();
            let mut tail_enumerated_rows = 0u64;
            let mut patience_stopped = 0u32;
            let mut shards_floor_pruned = 0u32;
            for stats in result.shard_stats.iter().flatten() {
                regimes.add(crate::vector::FilterRegimeCounts::from_proto(
                    stats.filter_regimes.as_ref(),
                ));
                tail_enumerated_rows =
                    tail_enumerated_rows.saturating_add(stats.tail_enumerated_rows);
                patience_stopped = patience_stopped.saturating_add(stats.patience_stopped);
                if stats.floor_pruned > 0 {
                    shards_floor_pruned = shards_floor_pruned.saturating_add(1);
                }
            }
            Ok(Response::new(SearchResponse {
                filter_regimes: regimes.to_proto(),
                tail_enumerated_rows,
                patience_stopped,
                shards_finished_early: 0,
                shards_floor_pruned,
                segments_total,
                segments_skipped,
                request_id,
                hits: result.hits,
                groups: Vec::new(),
                chunk_floor: 0.0,
            }))
        })
        .await
    }
}

impl CoordinatorServiceImpl {
    pub(super) async fn rpc_phrase_search(
        &self,
        request: Request<crate::pb::PhraseSearchRequest>,
    ) -> Result<Response<Bm25SearchResponse>, Status> {
        crate::metrics::timed(Route::PhraseSearch, request, |request| async move {
        self.admit(&request.get_ref().collection)?;
        if let Some(snapshot) = self.request_snapshot() {
                return Box::pin(SearchService::phrase_search(&snapshot, crate::metrics::nested(request))).await;
        }
        let request = request.into_inner();
        let base = request
            .base
            .ok_or_else(|| Status::invalid_argument("PhraseSearchRequest.base is required"))?;
        let k = self.resolve_k(base.k)?;
        if base.min_score.is_nan() || base.min_score == f32::NEG_INFINITY {
            return Err(Status::invalid_argument(
                "min_score must be finite (NaN and -inf are not valid floors)",
            ));
        }
        if !base.score_stages.is_empty()
            || !base.stats_fields.is_empty()
            || !base.cardinality_fields.is_empty()
            || !base.projections.is_empty()
        {
            return Err(Status::invalid_argument(
                "phrase max-group scoring is not yet certified with score stages, stats, cardinality, or projections; use filters and facets, or issue an ordinary Bm25Search",
            ));
        }
        let options = request.options.unwrap_or_default();
        let weight_per_token = if options.weight_per_token == 0.0 {
            1.0
        } else {
            options.weight_per_token
        };
        let max_weight = if options.max_weight == 0.0 {
            3.0
        } else {
            options.max_weight
        };
        let filter = crate::cel::compile_filter(&base.filter)?;
        let (hits, facets, range_facets) = self
            .fanout_phrase(&base, k, weight_per_token, max_weight, filter.as_ref())
            .await?;
        let kth_best = if hits.len() == k as usize {
            hits.last()
                .map(|hit| crate::bm25::floor_seed(hit.score))
                .unwrap_or(0.0)
        } else {
            0.0
        };
        Ok(Response::new(Bm25SearchResponse {
            execution_details_redacted: false,
            field_details_redacted: false,
            segments_total: 0,
            segments_skipped: 0,
            hits,
            kth_best,
            facets,
            range_facets,
            stats: Vec::new(),
            cardinality: Vec::new(),
            phrase_routing: Vec::new(),
            prefix_expansions: Vec::new(),
            synonym_expansions: Vec::new(),
        }))
        })
        .await
    }
}

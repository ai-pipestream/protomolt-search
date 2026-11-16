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
            self.search_with_evidence(request.into_inner(), false)
                .await
                .map(|(resp, _)| Response::new(resp))
        })
        .await
    }

    pub(crate) async fn search_with_evidence(
        &self,
        mut req: SearchRequest,
        exact_fp32_scan: bool,
    ) -> Result<(SearchResponse, DenseScanExecution), Status> {
        self.admit(&req.collection)?;
        if let Some(snapshot) = self.request_snapshot() {
            return Box::pin(snapshot.search_with_evidence(req, exact_fp32_scan)).await;
        }
        if !req.field.is_empty() {
            let scoped = self.for_vector_field(&req.field)?;
            req.field.clear();
            return Box::pin(scoped.search_with_evidence(req, exact_fp32_scan)).await;
        }
        if self.scoped_vector_scan() && self.query_read_versions.is_none() {
            let mut filters = RequestFilters::compile(&req.geo_filters, &req.filter)?;
            filters.floor_prune = req.floor_prune;
            filters.floor_greediness_ppm = req.floor_greediness_ppm;
            filters.floor_clamp_min = req.floor_clamp_min;
            filters.exact_fp32_scan = exact_fp32_scan;
            self.check_vector_scan(&filters, req.collapse_parents)?;
            let (pinned, reads) = self.pin_read_versions().await?;
            let result = Box::pin(pinned.search_with_evidence(req, exact_fp32_scan)).await;
            pinned.validate_read_versions(&reads).await?;
            return result;
        }
        self.search_vectors(req, exact_fp32_scan).await
    }

    /// The validated vector search past admission, scoping, and read pinning.
    async fn search_vectors(
        &self,
        req: SearchRequest,
        exact_fp32_scan: bool,
    ) -> Result<(SearchResponse, DenseScanExecution), Status> {
        let k = self.resolve_k(req.k)?;
        if req.vector.is_empty() {
            return Err(Status::invalid_argument("empty query vector"));
        }
        if req.vector.iter().any(|x| !x.is_finite()) {
            return Err(Status::invalid_argument(
                "query vector has non-finite coordinates",
            ));
        }
        let raw = raw_search_controls(&req);
        self.validate_raw_dense_controls(&raw).await?;
        let request_id = if req.request_id.is_empty() {
            format!(
                "req-{}-{}",
                std::process::id(),
                REQUEST_COUNTER.fetch_add(1, AtomicOrdering::Relaxed)
            )
        } else {
            req.request_id.clone()
        };
        let mut filters = search_filters(&req, k)?;
        filters.exact_fp32_scan = exact_fp32_scan;
        let _ = self.resolve_finish_windows(&filters)?;
        self.check_vector_scan(&filters, req.collapse_parents)?;
        let rows = if !self.has_clustered_vectors() {
            self.fleet_vector_identity(true).await?.rows
        } else if exact_fp32_scan {
            self.ensure_shard_vector_rows().await;
            self.clustered_quality_identity()
                .await?
                .map_or(0, |id| id.rows)
        } else {
            0
        };

        #[cfg(feature = "net")]
        let (response, exec) = if self.clustered_vectors.is_some() && !exact_fp32_scan {
            self.search_clustered(request_id, &req, k, &filters).await?
        } else {
            self.search_shards(request_id, &req, k, &filters, rows)
                .await?
        };
        #[cfg(not(feature = "net"))]
        let (response, exec) = self
            .search_shards(request_id, &req, k, &filters, rows)
            .await?;

        if req.enumerate_all {
            crate::dense_execution::verify_exhaustive_scan(
                crate::dense_execution::Route::EnumerateAll,
                &exec.evidence,
            )
            .map_err(dense_status)?;
        }
        Ok((response, exec))
    }

    #[cfg(feature = "net")]
    async fn search_clustered(
        &self,
        request_id: String,
        req: &SearchRequest,
        k: u32,
        filters: &RequestFilters,
    ) -> Result<(SearchResponse, DenseScanExecution), Status> {
        let (hits, groups, chunk_floor) = if req.collapse_parents {
            let collapsed = self
                .clustered_parent_collapse(&request_id, &req.vector, k, filters)
                .await?;
            (collapsed.hits, collapsed.groups, collapsed.chunk_floor)
        } else {
            let result = self
                .clustered_vector_candidates(&request_id, &req.vector, k, None, false, filters)
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
            (hits, Vec::new(), 0.0)
        };
        let rows = self
            .clustered_quality_identity()
            .await?
            .map_or(0, |id| id.rows);
        Ok((
            SearchResponse {
                filter_regimes: None,
                tail_enumerated_rows: 0,
                patience_stopped: 0,
                shards_finished_early: 0,
                shards_floor_pruned: 0,
                segments_total: 0,
                segments_skipped: 0,
                request_id,
                hits,
                groups,
                chunk_floor,
            },
            clustered_scan_execution(rows),
        ))
    }

    async fn search_shards(
        &self,
        request_id: String,
        req: &SearchRequest,
        k: u32,
        filters: &RequestFilters,
        rows: u64,
    ) -> Result<(SearchResponse, DenseScanExecution), Status> {
        let use_streaming = self.stream_search
            || filters.finish_settle_us.is_some()
            || filters.finish_deadline_us.is_some();
        if use_streaming {
            return self
                .search_shards_streaming(request_id, req, k, filters, rows)
                .await;
        }
        let result = if req.collapse_parents {
            self.fanout_search_collapse(&request_id, &req.vector, k, filters)
                .await?
        } else {
            self.fanout_search(
                &request_id,
                &req.vector,
                k,
                filters.exact_fp32_scan,
                filters,
            )
            .await?
        };
        let refs: Vec<(usize, Option<&ShardScanStats>)> = result
            .shard_hits
            .iter()
            .zip(&result.shard_stats)
            .map(|((shard, _), stats)| (*shard as usize, stats.as_ref()))
            .collect();
        let exec = self.evidence_from_shard_stats(&refs, rows);
        Ok((unary_search_response(request_id, result), exec))
    }

    async fn search_shards_streaming(
        &self,
        request_id: String,
        req: &SearchRequest,
        k: u32,
        filters: &RequestFilters,
        rows: u64,
    ) -> Result<(SearchResponse, DenseScanExecution), Status> {
        let mask = self.vector_scan_mask(filters.tree.as_ref());
        let skipped = mask.as_ref().map(|m| m.skipped.as_slice());
        if req.collapse_parents {
            let doc = self
                .fanout_stream_search_collapse(&request_id, &req.vector, k, filters)
                .await?;
            let exec = self.evidence_from_summaries(
                &doc.summaries,
                skipped,
                rows,
                doc.shards_finished_early,
                doc.shards_floor_pruned,
            );
            return Ok((
                stream_search_response(
                    request_id,
                    &doc.summaries,
                    doc.hits,
                    doc.groups,
                    doc.chunk_floor,
                    doc.shards_finished_early,
                    doc.shards_floor_pruned,
                ),
                exec,
            ));
        }
        let streamed = self
            .fanout_stream_search(&request_id, &req.vector, k, None, filters)
            .await?;
        let exec = self.evidence_from_summaries(
            &streamed.summaries,
            skipped,
            rows,
            streamed.shards_finished_early,
            streamed.shards_floor_pruned,
        );
        Ok((
            stream_search_response(
                request_id,
                &streamed.summaries,
                streamed.hits,
                Vec::new(),
                0.0,
                streamed.shards_finished_early,
                streamed.shards_floor_pruned,
            ),
            exec,
        ))
    }
}

fn stream_search_response(
    request_id: String,
    summaries: &[StreamSearchSummary],
    hits: Vec<ScoredHit>,
    groups: Vec<crate::pb::ParentGroup>,
    chunk_floor: f32,
    shards_finished_early: u32,
    shards_floor_pruned: u32,
) -> SearchResponse {
    SearchResponse {
        filter_regimes: summed_filter_regimes(summaries),
        tail_enumerated_rows: summed_tail_enumerated_rows(summaries),
        patience_stopped: summed_patience_stopped(summaries),
        shards_finished_early,
        shards_floor_pruned,
        segments_total: 0,
        segments_skipped: 0,
        request_id,
        hits,
        groups,
        chunk_floor,
    }
}

fn raw_search_controls(req: &SearchRequest) -> crate::dense_execution::RawDenseControls {
    crate::dense_execution::RawDenseControls {
        filter_strategy: req.filter_strategy,
        enumerate_all: req.enumerate_all,
        staged_selection: req.staged_selection,
        enumerate_max_rows: req.enumerate_max_rows,
        patience_saturation_ppm: req.patience_saturation_ppm,
        patience_hops: req.patience_hops,
        finish_settle_us: req.finish_settle_us,
        finish_deadline_us: req.finish_deadline_us,
        floor_prune: req.floor_prune,
        floor_greediness_ppm: req.floor_greediness_ppm,
        floor_clamp_min: req.floor_clamp_min,
    }
}

/// CEL text compiles ONCE, here, into the predicate IR the shards execute;
/// no shard ever sees CEL text.
fn search_filters(req: &SearchRequest, k: u32) -> Result<RequestFilters, Status> {
    let mut filters = RequestFilters::compile(&req.geo_filters, &req.filter)?;
    filters.enumerate_max_rows = req.enumerate_max_rows;
    filters.filter_strategy = req.filter_strategy;
    filters.enumerate_all = req.enumerate_all;
    filters.staged_selection = req.staged_selection;
    filters.patience_saturation_ppm = req.patience_saturation_ppm;
    filters.patience_hops = req.patience_hops;
    filters.finish_settle_us = req.finish_settle_us;
    filters.finish_deadline_us = req.finish_deadline_us;
    filters.floor_prune = req.floor_prune;
    filters.floor_k = Some(k);
    filters.floor_greediness_ppm = req.floor_greediness_ppm;
    filters.floor_clamp_min = req.floor_clamp_min;
    filters.include_rerank_candidate = req.include_rerank_candidate;
    Ok(filters)
}

/// The unary fan-out's response: hits plus the shards' summed scan stats.
fn unary_search_response(request_id: String, result: FanoutResult) -> SearchResponse {
    let (segments_total, segments_skipped) =
        result
            .shard_stats
            .iter()
            .flatten()
            .fold((0u32, 0u32), |(total, skipped), stats| {
                (
                    total.saturating_add(stats.segments_total),
                    skipped.saturating_add(stats.segments_skipped),
                )
            });
    let mut regimes = crate::vector::FilterRegimeCounts::default();
    let mut tail_enumerated_rows = 0u64;
    let mut patience_stopped = 0u32;
    let mut shards_floor_pruned = 0u32;
    for stats in result.shard_stats.iter().flatten() {
        regimes.add(crate::vector::FilterRegimeCounts::from_proto(
            stats.filter_regimes.as_ref(),
        ));
        tail_enumerated_rows = tail_enumerated_rows.saturating_add(stats.tail_enumerated_rows);
        patience_stopped = patience_stopped.saturating_add(stats.patience_stopped);
        if stats.floor_pruned > 0 {
            shards_floor_pruned = shards_floor_pruned.saturating_add(1);
        }
    }
    SearchResponse {
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

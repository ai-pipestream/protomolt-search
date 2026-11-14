//! Rpc query.

use super::*;

impl CoordinatorServiceImpl {
    /// The public query surface: an adapter over the routes above
    /// (`docs/query-api.md`, `src/query.rs`). Delegation, never a fork.
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

    pub(super) async fn rpc_query_stream(
        &self,
        request: Request<crate::pb::QueryStreamRequest>,
    ) -> Result<Response<<Self as SearchService>::QueryStreamStream>, Status> {
        crate::metrics::timed_stream(Route::QueryStream, request, |request| async move {
        self.admit(&request.get_ref().collection)?;
        if let Some(snapshot) = self.request_snapshot() {
                return Box::pin(SearchService::query_stream(
                    &snapshot,
                    crate::metrics::nested(request),
                ))
                .await
                .map(|response| response.map(crate::metrics::Timed::into_inner));
        }
        let grpc_budget = crate::relay::grpc_timeout(request.metadata());
        let access = request.extensions().get::<crate::pb::AccessDecision>().cloned();
        let mut request = request.into_inner();
        let request_fingerprint = request.query.as_ref().map(|query| crate::sha256::hex_digest(&prost::Message::encode_to_vec(query))).unwrap_or_default();
        if let Some(query) = request.query.as_mut() {
            self.require_topology_generation(query.required_topology_generation)?;
            // Reject collection, query, authority and topology mismatches before
            // opening the stream. Keep the original token for the execution's
            // data-version binding, which runs under the stream deadline.
            self.bind_query_cursor(&mut query.clone(), access.as_ref())?;
            if query.explain {
                return Err(Status::invalid_argument(
                    "explain is served on the unary Query route: a stream's revisions carry                      candidate hits without their trees, and a tree over a revision that a                      later one replaces would explain a score that was never served",
                ));
            }
        }
        let (tx, rx) = mpsc::channel::<Result<crate::pb::QueryStreamResponse, Status>>(8);
        let service = self.clone();
        tokio::spawn(async move {
            let mut revision = 1u64;
            let accepted = query_stream_revision(
                revision,
                crate::pb::QueryStreamPhase::Accepted,
                Vec::new(),
                String::new(),
                crate::pb::QueryStreamIdentityState::Unspecified,
            );
            if tx
                .send(Ok(crate::pb::QueryStreamResponse {
                    payload: Some(crate::pb::query_stream_response::Payload::Revision(
                        accepted,
                    )),
                }))
                .await
                .is_err()
            {
                return;
            }

            let Some(query) = request.query else {
                let status = Status::invalid_argument("QueryStream requires query");
                let _ = tx
                    .send(Ok(crate::pb::QueryStreamResponse {
                        payload: Some(crate::pb::query_stream_response::Payload::Completion(
                            crate::pb::QueryStreamCompletion {
                                error_disclosure: crate::error_disclosure::status_detail(&status),
                                completed: false,
                                response: None,
                                final_revision: revision,
                                scoring_fingerprints: Vec::new(),
                                error_code: status.code() as u32,
                                error_message: status.message().to_string(),
                            },
                        )),
                    }))
                    .await;
                return;
            };
            let (progress_tx, progress_rx) = watch::channel(None);
            let runner = service.with_stream_search(true).with_bm25_stream(true)
                .with_query_progress(progress_tx);
            let req_timeout = (request.timeout_ms > 0).then(|| Duration::from_millis(request.timeout_ms));
            let timeout = match (req_timeout, grpc_budget) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (Some(a), None) => Some(a),
                (None, Some(b)) => Some(b),
                (None, None) => None,
            };
            let finish_deadline_us = timeout.map_or(0, |dur| {
                let now_us = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_micros() as u64;
                now_us
                    .saturating_add(dur.as_micros().min(u128::from(u64::MAX)) as u64)
                    .max(1)
            });
            let deadline = async move {
                match timeout {
                    Some(duration) => tokio::time::sleep(duration).await,
                    None => std::future::pending::<()>().await,
                }
            };
            tokio::pin!(deadline);
            let mut scoring_fingerprints = Vec::new();
            let result = {
                let attempt = runner.query_stream_attempt(query, access, finish_deadline_us, progress_rx, &tx,
                    &mut revision, &mut scoring_fingerprints, &request_fingerprint);
                tokio::pin!(attempt);
                tokio::select! {
                    biased;
                    _ = tx.closed() => return,
                    _ = &mut deadline => Err(Status::deadline_exceeded(format!(
                        "QueryStream exceeded its {}ms deadline", request.timeout_ms))),
                    result = &mut attempt => result,
                }
            };
            // Dropping the attempt aborts its JoinSet on deadline, cancellation
            // or identity refusal. No collector is left running behind a
            // terminal stream. The completion itself may await client capacity.
            scoring_fingerprints.sort(); scoring_fingerprints.dedup();
            let completion = match result {
                Ok(response) => crate::pb::QueryStreamCompletion {
                    completed: true, response: Some(response), final_revision: revision,
                    scoring_fingerprints, error_code: 0, error_message: String::new(), error_disclosure: None,
                },
                Err(status) => crate::pb::QueryStreamCompletion {
                    completed: false, response: None, final_revision: revision,
                    scoring_fingerprints, error_code: status.code() as u32,
                    error_message: status.message().to_string(),
                    error_disclosure: crate::error_disclosure::status_detail(&status),
                },
            };
            let _ = tx.send(Ok(crate::pb::QueryStreamResponse {
                payload: Some(crate::pb::query_stream_response::Payload::Completion(completion)),
            })).await;
        });
        Ok(Response::new(ReceiverStream::new(rx)))
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
            // The fleet scores in one space or not at all: mixed provider
            // kinds or fingerprints are refused before any shard is asked
            // (docs/mmap-vectors.md).
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

    pub(super) async fn rpc_hybrid_search(
        &self,
        request: Request<HybridSearchRequest>,
    ) -> Result<Response<HybridSearchResponse>, Status> {
        crate::metrics::timed(Route::HybridSearch, request, |request| async move {
            self.admit(&request.get_ref().collection)?;
            if let Some(snapshot) = self.request_snapshot() {
                return Box::pin(SearchService::hybrid_search(
                    &snapshot,
                    crate::metrics::nested(request),
                ))
                .await;
            }
            let req = request.into_inner();
            let k = self.resolve_k(req.k)?;
            if req.text.is_empty() {
                return Err(Status::invalid_argument(
                    "hybrid search requires query text",
                ));
            }
            if req.vector.is_empty() {
                return Err(Status::invalid_argument(
                    "hybrid search requires a query vector",
                ));
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
            // One compilation, both legs. The hybrid route used to refuse
            // filters outright because the vector leg had no filter
            // machinery and filtering only the lexical half would have
            // misdescribed the result set (docs/vector-filters.md).
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
            // leg_k: default max(k, rrf_k) so the RRF constant never exceeds a
            // leg's depth; explicit values below k are clamped to k.
            let options = req.legs.unwrap_or_default();
            let rrf_k = if options.rrf_k == 0.0 {
                fusion::DEFAULT_RRF_K
            } else {
                f64::from(options.rrf_k)
            };
            // Weights: absent = 1.0; an explicit 0 disables the leg (the
            // proto documents which modes support that).
            let vector_weight = options.vector_weight.unwrap_or(1.0);
            let bm25_weight = options.bm25_weight.unwrap_or(1.0);
            if vector_weight == 0.0 && bm25_weight == 0.0 {
                return Err(Status::invalid_argument(
                    "both legs disabled: at least one of vector_weight/bm25_weight must be nonzero",
                ));
            }
            if options.fusion_mode() == FusionMode::TwoLevel
                && (vector_weight == 0.0 || bm25_weight == 0.0)
            {
                return Err(Status::invalid_argument(
                    "TWO_LEVEL cannot disable a leg (its node wire format cannot distinguish \
                 0 from unset); use GLOBAL_RANK or SCORE_BLEND",
                ));
            }
            // The decomposed floor algebra divides by vector_weight and
            // scales bounds by bm25_weight; both must be strictly positive
            // (a single-leg query belongs to GLOBAL_RANK or SCORE_BLEND).
            if options.fusion_mode() == FusionMode::Decomposed
                && !(vector_weight > 0.0
                    && vector_weight.is_finite()
                    && bm25_weight > 0.0
                    && bm25_weight.is_finite())
            {
                return Err(Status::invalid_argument(
                    "DECOMPOSED requires finite leg weights > 0 for both legs",
                ));
            }
            if options.min_vector_score.is_nan() {
                return Err(Status::invalid_argument("min_vector_score must not be NaN"));
            }
            // A floor on the vector leg's score cannot be met by a query that
            // does not run the vector leg. Fusion would drop every hit and
            // return an empty result set, which reads as "nothing matched"
            // rather than "you asked for two contradictory things".
            if options.min_vector_score > 0.0 && vector_weight == 0.0 {
                return Err(Status::invalid_argument(
                    "min_vector_score is set but the vector leg is disabled (vector_weight 0); \
                 no hit can carry a qualifying vector score",
                ));
            }
            let legs = HybridLegs {
                leg_k: if options.leg_k == 0 {
                    k.max(rrf_k as u32)
                } else {
                    options.leg_k.max(k)
                },
                vector_weight,
                bm25_weight,
                rrf_k,
                fusion_mode: options.fusion_mode(),
                normalization: match options.normalization() {
                    crate::pb::ScoreNormalization::ZScore => fusion::Normalization::ZScore,
                    crate::pb::ScoreNormalization::None => fusion::Normalization::None,
                    _ => fusion::Normalization::MinMax,
                },
                combination: match options.combination() {
                    crate::pb::ScoreCombination::Geometric => fusion::Combination::Geometric,
                    crate::pb::ScoreCombination::Harmonic => fusion::Combination::Harmonic,
                    _ => fusion::Combination::Arithmetic,
                },
                min_vector_score: options.min_vector_score,
            };
            let (mut hits, mut cascade_hits, mut debug) = match legs.fusion_mode {
                FusionMode::Cascade | FusionMode::Unspecified => {
                    let (cascade_hits, debug) = self
                        .fanout_cascade_with_terms(
                            &request_id,
                            &req.text,
                            &req.pre_analyzed_terms,
                            &req.vector,
                            k,
                            req.analysis.as_ref(),
                            legs.min_vector_score,
                            req.debug,
                            &filters,
                        )
                        .await?;
                    (Vec::new(), cascade_hits, debug)
                }
                _ => {
                    let (hits, debug) = self
                        .fanout_hybrid_with_terms(
                            &request_id,
                            &req.text,
                            &req.pre_analyzed_terms,
                            &req.vector,
                            k,
                            req.analysis.as_ref(),
                            legs,
                            req.debug,
                            &filters,
                        )
                        .await?;
                    (hits, Vec::new(), debug)
                }
            };
            if let Some(boost) = &req.boost {
                self.apply_boost(
                    boost,
                    req.analysis.as_ref(),
                    &mut hits,
                    &mut cascade_hits,
                    &mut debug,
                )
                .await?;
            }
            Ok(Response::new(HybridSearchResponse {
                request_id,
                hits,
                cascade_hits,
                debug,
            }))
        })
        .await
    }
}

impl CoordinatorServiceImpl {
    pub(super) async fn rpc_bm25_search(
        &self,
        request: Request<Bm25SearchRequest>,
    ) -> Result<Response<Bm25SearchResponse>, Status> {
        crate::metrics::timed(Route::Bm25Search, request, |request| async move {
            self.admit(&request.get_ref().collection)?;
            if let Some(snapshot) = self.request_snapshot() {
                return Box::pin(SearchService::bm25_search(
                    &snapshot,
                    crate::metrics::nested(request),
                ))
                .await;
            }
            let req = request.into_inner();
            let k = self.resolve_k(req.k)?;
            // A malformed HighlightSpec refuses here, before any shard is
            // asked, so an empty fleet answers the same as a full one
            // (docs/highlighting.md).
            if let Some(spec) = req.highlight.as_ref() {
                crate::highlight::Plan::from_spec(spec)?;
            }
            if req.min_score.is_nan() || req.min_score == f32::NEG_INFINITY {
                return Err(Status::invalid_argument(
                    "min_score must be finite (NaN and -inf are not valid floors)",
                ));
            }
            // CEL text compiles ONCE, here, into the predicate IR the
            // shards execute (docs/cel-filters.md): every shard sees the
            // same tree, and none ever sees CEL text.
            let user_filter = crate::cel::compile_filter(&req.filter)?;
            // Projection text compiles ONCE, here, into the ValueExpr IR
            // the shards resolve and evaluate (docs/cel-values.md).
            let projections = compile_projections(&req.projections)?;
            if let Some(fields) = &self.field_permissions {
                fields.bm25(&req, user_filter.as_ref(), &projections)?;
            }
            let filter = self.visible_filter(user_filter)?;
            let mut phrase_routing = Vec::new();
            let mut prefix_expansions = Vec::new();
            let mut synonym_expansions = Vec::new();
            let (hits, facets, range_facets, stats, cardinality, prune) =
                if req.fields.is_empty() && req.phrase.is_some() {
                    // A phrase on the flat route is the body field's phrase on
                    // the fused route (docs/phrase-proximity.md); the fused
                    // route's uncertified combinations refuse by name here too.
                    if !req.score_stages.is_empty() {
                        return Err(Status::invalid_argument(
                    "score stages are not yet certified with a phrase constraint; drop `phrase` \
                     or drop `score_stages`",
                ));
                    }
                    if !req.stats_fields.is_empty() || !req.cardinality_fields.is_empty() {
                        return Err(Status::invalid_argument(
                        "stats/cardinality are not yet certified with a phrase constraint; drop \
                     `phrase` or drop the aggregations",
                    ));
                    }
                    if !req.projections.is_empty() {
                        return Err(Status::invalid_argument(
                    "projections are not yet certified with a phrase constraint; drop `phrase` \
                     or drop the projections",
                ));
                    }
                    let body = vec![crate::pb::QueryField {
                        field: "body".to_string(),
                        analysis: req.analysis.clone(),
                        weight: 1.0,
                        k1: 0.0,
                        b: 0.0,
                        phrase: req.phrase,
                        prefixes: req.prefixes.clone(),
                        synonyms: req.synonyms.clone(),
                        synonyms_off: req.synonyms_off,
                    }];
                    let (((hits, facets, ranges), fused_prune), routing) = self
                        .fanout_bm25_fused_routed(
                            &req.text,
                            k,
                            &body,
                            req.min_score,
                            &req.facet_fields,
                            &req.map_facet_fields,
                            &req.range_facet_fields,
                            &req.geo_filters,
                            filter.as_ref(),
                            &mut prefix_expansions,
                            req.highlight.as_ref(),
                            &mut synonym_expansions,
                            req.explain,
                        )
                        .await?;
                    phrase_routing = routing;
                    (hits, facets, ranges, Vec::new(), Vec::new(), fused_prune)
                } else if req.fields.is_empty() {
                    self.fanout_bm25_aggregated_with_terms(
                        &req.text,
                        &req.pre_analyzed_terms,
                        k,
                        req.analysis.as_ref(),
                        req.min_score,
                        &req.facet_fields,
                        &req.map_facet_fields,
                        &req.range_facet_fields,
                        &req.score_stages,
                        &req.geo_filters,
                        filter.as_ref(),
                        &req.stats_fields,
                        &req.cardinality_fields,
                        &projections,
                        &req.prefixes,
                        &mut prefix_expansions,
                        req.highlight.as_ref(),
                        &req.synonyms,
                        req.synonyms_off,
                        &mut synonym_expansions,
                        req.explain,
                    )
                    .await?
                } else {
                    if !req.score_stages.is_empty() {
                        return Err(Status::invalid_argument(
                            "score stages are not yet supported on the fused multi-field route; \
                     drop `fields` to use the flat route, or drop `score_stages`",
                        ));
                    }
                    if !req.stats_fields.is_empty() || !req.cardinality_fields.is_empty() {
                        return Err(Status::invalid_argument(
                            "stats/cardinality are not yet supported on the fused multi-field \
                     route; drop `fields` to use the flat route, or drop the aggregations",
                        ));
                    }
                    if !req.projections.is_empty() {
                        return Err(Status::invalid_argument(
                            "projections are not yet supported on the fused multi-field route; \
                     drop `fields` to use the flat route, or drop the projections",
                        ));
                    }
                    // `analysis` is documented as ignored once `fields` is set,
                    // because term identity is per field. Ignoring it QUIETLY is
                    // the trap: the caller believes it asked for the ingest
                    // analysis, every field falls back to the sidecar default,
                    // and the query runs against terms that do not exist in the
                    // index. That returns a confident ranking over whichever
                    // tokens happened to survive, so it does not look like a
                    // failure -- it looks like bad relevance.
                    if req.analysis.is_some() {
                        return Err(Status::invalid_argument(
                    "Bm25SearchRequest.analysis is ignored when `fields` is set (term identity \
                     is per field). Move the spec onto each QueryField.analysis, or drop \
                     `fields` to use the single-field route.",
                ));
                    }
                    if req.phrase.is_some() {
                        return Err(Status::invalid_argument(
                    "Bm25SearchRequest.phrase is the flat route's constraint; with `fields` set, \
                     put the PhraseMatch on the QueryField it constrains",
                ));
                    }
                    if !req.prefixes.is_empty() {
                        return Err(Status::invalid_argument(
                        "Bm25SearchRequest.prefixes expand in the body; with `fields` set, put \
                         the prefixes on the QueryField whose dictionary they expand in",
                    ));
                    }
                    let (((hits, facets, ranges), fused_prune), routing) = self
                        .fanout_bm25_fused_routed(
                            &req.text,
                            k,
                            &req.fields,
                            req.min_score,
                            &req.facet_fields,
                            &req.map_facet_fields,
                            &req.range_facet_fields,
                            &req.geo_filters,
                            filter.as_ref(),
                            &mut prefix_expansions,
                            req.highlight.as_ref(),
                            &mut synonym_expansions,
                            req.explain,
                        )
                        .await?;
                    phrase_routing = routing;
                    (hits, facets, ranges, Vec::new(), Vec::new(), fused_prune)
                };
            // The merged k-th best: one f32 ULP below the last hit's score
            // when k hits were returned (see `bm25::floor_seed` — a later
            // seed can never exceed the true k-th best), 0 otherwise.
            let kth_best = if hits.len() == k as usize {
                hits.last()
                    .map(|h| crate::bm25::floor_seed(h.score))
                    .unwrap_or(0.0)
            } else {
                0.0
            };
            let mut response = Bm25SearchResponse {
                field_details_redacted: false,
                execution_details_redacted: self.document_visibility.is_some(),
                segments_total: if self.document_visibility.is_some() {
                    0
                } else {
                    prune.segments_total
                },
                segments_skipped: if self.document_visibility.is_some() {
                    0
                } else {
                    prune.segments_skipped
                },
                hits,
                kth_best,
                facets,
                range_facets,
                stats,
                cardinality,
                phrase_routing,
                prefix_expansions,
                synonym_expansions,
            };
            if let Some(fields) = &self.field_permissions {
                fields.disclose(&mut response)?;
            }
            Ok(Response::new(response))
        })
        .await
    }

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

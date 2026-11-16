//! Hybrid RPC admission, option defaults, and response assembly.

use super::*;

impl CoordinatorServiceImpl {
    pub(super) async fn rpc_hybrid_search(
        &self,
        request: Request<HybridSearchRequest>,
    ) -> Result<Response<HybridSearchResponse>, Status> {
        crate::metrics::timed(Route::HybridSearch, request, |request| async move {
            self.execute_hybrid_with_evidence(request.into_inner())
                .await
                .map(|(resp, _)| Response::new(resp))
        })
        .await
    }

    pub(crate) async fn execute_hybrid_with_evidence(
        &self,
        req: HybridSearchRequest,
    ) -> Result<(HybridSearchResponse, DenseScanExecution), Status> {
        self.admit(&req.collection)?;
        if let Some(snapshot) = self.request_snapshot() {
            return Box::pin(snapshot.execute_hybrid_with_evidence(req)).await;
        }
        let k = self.resolve_k(req.k)?;
        validate_hybrid_rpc_query(&req)?;
        let request_id = Self::next_hybrid_request_id(&req.request_id);
        let filters = self.hybrid_rpc_filters(&req, k)?;
        let legs = hybrid_rpc_legs(&req, k)?;
        let runs_vector = matches!(
            legs.fusion_mode,
            FusionMode::Cascade | FusionMode::Unspecified
        ) || legs.vector_weight != 0.0;
        let rows = self.check_hybrid_raw_controls(&req, runs_vector).await?;
        let (mut hits, mut cascade_hits, mut debug, mut exec) = match legs.fusion_mode {
            FusionMode::Cascade | FusionMode::Unspecified => {
                let query = CascadeQuery::from_request(
                    &request_id,
                    &req,
                    k,
                    legs.min_vector_score,
                    &filters,
                )?;
                let (cascade_hits, debug, exec) = self.fanout_cascade_with_terms(query).await?;
                (Vec::new(), cascade_hits, debug, exec)
            }
            _ => {
                let query = HybridQuery::from_request(&request_id, &req, k, legs, &filters)?;
                let (hits, debug, exec) = self.fanout_hybrid_with_terms(query).await?;
                (hits, Vec::new(), debug, exec)
            }
        };
        if exec.evidence.rows == 0 {
            exec.evidence.rows = rows;
        }
        if runs_vector && req.enumerate_all {
            crate::dense_execution::verify_exhaustive_scan(
                crate::dense_execution::Route::EnumerateAll,
                &exec.evidence,
            )
            .map_err(dense_status)?;
        }
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
        Ok((
            HybridSearchResponse {
                request_id,
                hits,
                cascade_hits,
                debug,
            },
            exec,
        ))
    }

    async fn check_hybrid_raw_controls(
        &self,
        req: &HybridSearchRequest,
        runs_vector: bool,
    ) -> Result<u64, Status> {
        let raw = raw_hybrid_controls(req);
        if runs_vector {
            self.validate_raw_dense_controls(&raw).await
        } else {
            crate::dense_execution::validate_raw_controls(&raw, None).map_err(dense_status)?;
            Ok(0)
        }
    }

    fn next_hybrid_request_id(request_id: &str) -> String {
        if request_id.is_empty() {
            format!(
                "req-{}-{}",
                std::process::id(),
                REQUEST_COUNTER.fetch_add(1, AtomicOrdering::Relaxed)
            )
        } else {
            request_id.to_string()
        }
    }

    fn hybrid_rpc_filters(
        &self,
        req: &HybridSearchRequest,
        k: u32,
    ) -> Result<RequestFilters, Status> {
        // Compile one filter tree for both retrieval legs.
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
        let _ = self.resolve_finish_windows(&filters)?;
        Ok(filters)
    }
}

fn raw_hybrid_controls(req: &HybridSearchRequest) -> crate::dense_execution::RawDenseControls {
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

fn validate_hybrid_rpc_query(req: &HybridSearchRequest) -> Result<(), Status> {
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
    Ok(())
}

fn hybrid_rpc_legs(req: &HybridSearchRequest, k: u32) -> Result<HybridLegs, Status> {
    let options = req.legs.as_ref().cloned().unwrap_or_default();
    let rrf_k = if options.rrf_k == 0.0 {
        fusion::DEFAULT_RRF_K
    } else {
        f64::from(options.rrf_k)
    };
    // Weights: absent = 1.0; an explicit 0 disables the leg (the
    // proto documents which modes support that).
    let vector_weight = options.vector_weight.unwrap_or(1.0);
    let bm25_weight = options.bm25_weight.unwrap_or(1.0);
    validate_hybrid_rpc_options(&options, vector_weight, bm25_weight)?;
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
    Ok(legs)
}

fn validate_hybrid_rpc_options(
    options: &crate::pb::HybridLegOptions,
    vector_weight: f32,
    bm25_weight: f32,
) -> Result<(), Status> {
    if vector_weight == 0.0 && bm25_weight == 0.0 {
        return Err(Status::invalid_argument(
            "both legs disabled: at least one of vector_weight/bm25_weight must be nonzero",
        ));
    }
    if options.fusion_mode() == FusionMode::TwoLevel && (vector_weight == 0.0 || bm25_weight == 0.0)
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
    Ok(())
}

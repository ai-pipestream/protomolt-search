//! Fused lexical/vector leg execution and hybrid shard scoring.

use crate::node::*;

/// Raw leg hits as `(global_doc_id, raw_score)`, score-descending.
type RawLeg = Vec<(u64, f64)>;

/// Wire filters and geo regions validated at the RPC boundary.
pub(in crate::node) struct LegFilters<'a> {
    pub(in crate::node) geo: &'a [crate::pb::GeoFilter],
    pub(in crate::node) regions: Vec<crate::geo::GeoRegion>,
    pub(in crate::node) tree: Option<&'a crate::pb::FilterExpr>,
}

/// Dense execution controls carried on a hybrid vector leg.
#[derive(Clone, Copy, Default)]
pub(in crate::node) struct VectorLegControls {
    pub(in crate::node) enumerate_max_rows: Option<u32>,
    pub(in crate::node) filter_strategy: i32,
    pub(in crate::node) enumerate_all: bool,
    pub(in crate::node) staged_selection: bool,
    pub(in crate::node) patience_saturation_ppm: Option<u32>,
    pub(in crate::node) patience_hops: Option<u32>,
    pub(in crate::node) floor_prune: Option<bool>,
    pub(in crate::node) floor_greediness_ppm: Option<u32>,
    pub(in crate::node) floor_clamp_min: Option<u32>,
}

/// Grouped inputs for computing both retrieval legs under one shard read guard.
pub(in crate::node) struct LegCompute<'a> {
    pub(in crate::node) vector: &'a [f32],
    pub(in crate::node) terms: &'a [String],
    pub(in crate::node) analysis_fingerprint: u64,
    pub(in crate::node) global_doc_count: u64,
    pub(in crate::node) global_total_doc_length: u64,
    pub(in crate::node) global_doc_frequencies: &'a [u32],
    pub(in crate::node) params: Bm25Params,
    pub(in crate::node) k: usize,
    pub(in crate::node) expected_stats_epoch: u64,
    pub(in crate::node) expected_stats_incarnation: &'a [u8],
    pub(in crate::node) filters: &'a LegFilters<'a>,
    pub(in crate::node) read_context: Option<&'a crate::pb::VectorReadContext>,
    pub(in crate::node) include_rerank_candidate: bool,
    pub(in crate::node) vector_controls: VectorLegControls,
}

/// Raw legs, known-column handshakes, and generation-bound candidate details.
pub(in crate::node) struct LegResults {
    pub(in crate::node) vector: RawLeg,
    pub(in crate::node) vector_scan: Option<ShardScanStats>,
    pub(in crate::node) bm25: RawLeg,
    pub(in crate::node) geo_columns_known: Vec<bool>,
    pub(in crate::node) filter_columns_known: Vec<bool>,
    pub(in crate::node) read_receipt: Option<crate::pb::VectorReadReceipt>,
    pub(in crate::node) rerank_candidates: HashMap<u64, crate::pb::RerankCandidatePayload>,
}

impl NodeServiceImpl {
    pub(in crate::node) fn compute_legs(&self, req: LegCompute<'_>) -> Result<LegResults, Status> {
        let guard = read_shard(&self.state);
        if !req.terms.is_empty() {
            guard.check_query_analysis("body", req.analysis_fingerprint)?;
        }
        guard.check_stats_epoch(req.expected_stats_epoch, req.expected_stats_incarnation)?;
        let (geo_columns_known, filter_columns_known) =
            filter_known_flags(guard.bm25.as_ref(), req.filters.geo, req.filters.tree);
        let slots = if req.k > 0 && !req.vector.is_empty() {
            guard.index.as_ref().map_or(0, VectorIndex::len)
        } else {
            0
        };
        let ((doc_filter, allow, prune), read_receipt) = guard.vector_scan_filters(
            slots,
            req.filters.geo,
            &req.filters.regions,
            req.filters.tree,
            req.read_context,
            self.knobs.segment_pruning(),
        )?;

        let (vector_leg, vector_scan) = if req.k > 0 && !req.vector.is_empty() {
            self.vector_leg(
                &guard,
                req.vector,
                req.k,
                allow.as_ref(),
                prune,
                req.vector_controls,
            )?
        } else {
            (Vec::new(), None)
        };
        let bm25_leg = match guard.bm25.as_ref() {
            Some(store) if req.k > 0 && !req.terms.is_empty() => {
                let stats = bm25::CorpusStats {
                    doc_count: req.global_doc_count,
                    total_doc_length: req.global_total_doc_length,
                    dfs: req.global_doc_frequencies.to_vec(),
                };
                self.bm25_leg(
                    store,
                    req.terms,
                    &stats,
                    req.params,
                    doc_filter.as_ref(),
                    req.k,
                )?
            }
            _ => Vec::new(),
        };
        let rerank_candidates = if req.include_rerank_candidate {
            self.rerank_candidates(&guard, vector_leg.iter().chain(&bm25_leg))
        } else {
            HashMap::new()
        };
        Ok(LegResults {
            vector: vector_leg,
            vector_scan,
            bm25: bm25_leg,
            geo_columns_known,
            filter_columns_known,
            read_receipt,
            rerank_candidates,
        })
    }

    /// The vector leg: top-k over the shard's index under `allow` and `ctl`.
    fn vector_leg(
        &self,
        guard: &ShardState,
        vector: &[f32],
        k: usize,
        allow: Option<&crate::boolean_bits::Bits>,
        prune: crate::segment_prune::PruneStats,
        ctl: VectorLegControls,
    ) -> Result<(RawLeg, Option<ShardScanStats>), Status> {
        let Some(index) = guard.index.as_ref() else {
            return Ok((Vec::new(), None));
        };
        let dim = index.dim_opt().unwrap_or(0);
        if vector.len() != dim {
            return Err(Status::invalid_argument(format!(
                "hybrid vector has dim {}, index expects {dim}",
                vector.len()
            )));
        }
        if let Some((_, coord, value)) = first_invalid_coordinate(vector, dim) {
            return Err(Status::invalid_argument(format!(
                "hybrid vector coordinate {coord} is invalid: {value}"
            )));
        }
        check_enumerate_threshold(index, ctl.enumerate_max_rows)?;
        let controls = selection_controls(
            index,
            ctl.filter_strategy,
            ctl.enumerate_all,
            ctl.staged_selection,
        )?;
        let patience =
            check_patience_options(index, ctl.patience_saturation_ppm, ctl.patience_hops)?;
        let floor_prune = check_floor_prune_options(
            index,
            ctl.floor_prune,
            Some(k as u32),
            k as u32,
            None,
            ctl.floor_greediness_ppm,
            ctl.floor_clamp_min,
        )?;
        let queries = [BatchQuery {
            vector,
            k,
            keep_ties: false,
            allow: allow.map(crate::vector::Mask::Bits),
            enumerate_max_rows: ctl.enumerate_max_rows.map(|r| r as usize),
            controls,
            patience,
            floor_prune,
        }];
        let (hits, mut scan_stats) = crate::chunked::chunked_topk_batch(
            index,
            &queries,
            self.config.chunk_blocks,
            &mut |_| None,
            &mut |_, _| false,
        )
        .pop()
        .expect("one query in, one result out");
        scan_stats.segments_total = prune.segments_total;
        scan_stats.segments_skipped = prune.segments_skipped;
        observe_scan(scan_stats.bytes_scanned, scan_stats.scan_nanos);
        let leg = hits
            .into_iter()
            .map(|h| {
                (
                    self.config.slot_offset + u64::from(h.slot),
                    f64::from(h.score),
                )
            })
            .collect();
        Ok((leg, Some(proto_scan_stats(scan_stats))))
    }

    /// The lexical leg: BM25 top-k gated by the same filter as the vector leg.
    fn bm25_leg(
        &self,
        store: &Bm25Shard,
        terms: &[String],
        stats: &bm25::CorpusStats,
        params: Bm25Params,
        doc_filter: Option<&crate::filter::DocFilter<'_>>,
        k: usize,
    ) -> Result<RawLeg, Status> {
        let index = store.as_index();
        let numeric_read = ShardNumericRead(store);
        let filter = doc_filter.map(|f| (f, &numeric_read as &dyn crate::scorefn::NumericRead));
        let request = bm25::TopK::new(k)
            .filter(filter)
            .block_max(self.config.block_max);
        let docs = bm25::top_k(index, terms, stats, params, request);
        Ok(docs
            .into_iter()
            .map(|d| (self.config.slot_offset + u64::from(d.doc_id), d.score))
            .collect())
    }

    /// Generation-bound rerank payloads for every distinct local hit.
    fn rerank_candidates<'a>(
        &self,
        guard: &ShardState,
        hits: impl Iterator<Item = &'a (u64, f64)>,
    ) -> HashMap<u64, crate::pb::RerankCandidatePayload> {
        let offset = self.config.slot_offset;
        let mut candidates = HashMap::new();
        for &(doc_id, _) in hits {
            if candidates.contains_key(&doc_id) || doc_id < offset {
                continue;
            }
            if let Ok(local) = u32::try_from(doc_id - offset) {
                if let Some(payload) = guard.rerank_candidate_for(local) {
                    candidates.insert(doc_id, payload);
                }
            }
        }
        candidates
    }

    /// Level one of the two-level hybrid fusion: run both legs locally
    /// and RRF-fuse them (see `SearchService.HybridSearch`).
    pub(in crate::node) fn run_hybrid(
        &self,
        req: HybridShardRequest,
    ) -> Result<HybridShardResponse, Status> {
        let k = req.k as usize;
        if req.terms.len() != req.global_doc_frequencies.len() {
            return Err(Status::invalid_argument(
                "terms and global_doc_frequencies must have the same length",
            ));
        }
        let vector_weight = weight_or_default(req.vector_weight, "vector_weight")?;
        let bm25_weight = weight_or_default(req.bm25_weight, "bm25_weight")?;
        let rrf_k = if req.rrf_k == 0.0 {
            fusion::DEFAULT_RRF_K
        } else {
            f64::from(req.rrf_k)
        };
        if rrf_k.is_nan() || rrf_k <= 0.0 {
            return Err(Status::invalid_argument("rrf_k must be positive"));
        }

        let geo_regions = validate_geo_filters(&req.geo_filters)?;
        if let Some(f) = req.filter.as_ref() {
            crate::filter::validate_filter(f)?;
        }
        let mut legs = self.compute_legs(LegCompute {
            vector: &req.vector,
            terms: &req.terms,
            analysis_fingerprint: req.analysis_fingerprint,
            global_doc_count: req.global_doc_count,
            global_total_doc_length: req.global_total_doc_length,
            global_doc_frequencies: &req.global_doc_frequencies,
            params: params_from(req.k1, req.b)?,
            k,
            expected_stats_epoch: req.expected_stats_epoch,
            expected_stats_incarnation: &req.expected_stats_incarnation,
            filters: &LegFilters {
                geo: &req.geo_filters,
                regions: geo_regions,
                tree: req.filter.as_ref(),
            },
            read_context: req.read_context.as_ref(),
            include_rerank_candidate: req.include_rerank_candidate,
            vector_controls: hybrid_vector_controls(&req),
        })?;
        let fused = fusion::rrf_fuse(
            &[
                Leg {
                    hits: legs.vector,
                    weight: vector_weight,
                },
                Leg {
                    hits: legs.bm25,
                    weight: bm25_weight,
                },
            ],
            rrf_k,
            k,
        );
        Ok(HybridShardResponse {
            hits: pack_hybrid_hits(fused, &mut legs.rerank_candidates),
            geo_columns_known: legs.geo_columns_known,
            filter_columns_known: legs.filter_columns_known,
            read_receipt: legs.read_receipt,
            vector_scan: legs.vector_scan,
        })
    }
}

fn hybrid_vector_controls(req: &HybridShardRequest) -> VectorLegControls {
    VectorLegControls {
        enumerate_max_rows: req.enumerate_max_rows,
        filter_strategy: req.filter_strategy,
        enumerate_all: req.enumerate_all,
        staged_selection: req.staged_selection,
        patience_saturation_ppm: req.patience_saturation_ppm,
        patience_hops: req.patience_hops,
        floor_prune: req.floor_prune,
        floor_greediness_ppm: req.floor_greediness_ppm,
        floor_clamp_min: req.floor_clamp_min,
    }
}

fn pack_hybrid_hits(
    fused: Vec<fusion::FusedHit>,
    rerank_candidates: &mut HashMap<u64, crate::pb::RerankCandidatePayload>,
) -> Vec<HybridLegHit> {
    fused
        .into_iter()
        .map(|h| HybridLegHit {
            doc_id: h.doc_id,
            fused_score: h.fused_score as f32,
            vector_rank: h.leg_ranks[0],
            vector_score: h.leg_scores[0].unwrap_or(0.0) as f32,
            bm25_rank: h.leg_ranks[1],
            bm25_score: h.leg_scores[1].unwrap_or(0.0) as f32,
            rerank_candidate: rerank_candidates.remove(&h.doc_id),
        })
        .collect()
}

/// Request weights default to 1.0 (0 means "unset" in the proto);
/// negatives are rejected.
fn weight_or_default(value: f32, name: &str) -> Result<f64, Status> {
    if value == 0.0 {
        return Ok(1.0);
    }
    if value < 0.0 || value.is_nan() {
        return Err(Status::invalid_argument(format!("{name} must be >= 0")));
    }
    Ok(f64::from(value))
}

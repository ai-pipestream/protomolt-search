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

/// Raw legs, known-column handshakes, and generation-bound candidate details.
pub(in crate::node) struct LegResults {
    pub(in crate::node) vector: RawLeg,
    pub(in crate::node) bm25: RawLeg,
    pub(in crate::node) geo_columns_known: Vec<bool>,
    pub(in crate::node) filter_columns_known: Vec<bool>,
    pub(in crate::node) read_receipt: Option<crate::pb::VectorReadReceipt>,
    pub(in crate::node) rerank_candidates: HashMap<u64, crate::pb::RerankCandidatePayload>,
}

impl NodeServiceImpl {
    #[allow(clippy::too_many_arguments)]
    pub(in crate::node) fn compute_legs(
        &self,
        vector: &[f32],
        terms: &[String],
        analysis_fingerprint: u64,
        global_doc_count: u64,
        global_total_doc_length: u64,
        global_doc_frequencies: &[u32],
        params: Bm25Params,
        k: usize,
        expected_stats_epoch: u64,
        expected_stats_incarnation: &[u8],
        filters: &LegFilters<'_>,
        read_context: Option<&crate::pb::VectorReadContext>,
        include_rerank_candidate: bool,
    ) -> Result<LegResults, Status> {
        let guard = read_shard(&self.state);
        if !terms.is_empty() {
            guard.check_query_analysis("body", analysis_fingerprint)?;
        }
        guard.check_stats_epoch(expected_stats_epoch, expected_stats_incarnation)?;
        // One resolution for both legs: the
        // allowlist the vector kernel scans under and the predicate the
        // lexical heap gate applies are the same DocFilter, so the two
        // halves of a fused list cannot disagree about what matched.
        let (geo_columns_known, filter_columns_known) =
            filter_known_flags(guard.bm25.as_ref(), filters.geo, filters.tree);
        // A lexical-only or disabled leg needs the predicate and receipt,
        // but must not allocate a vector mask proportional to the corpus.
        let slots = if k > 0 && !vector.is_empty() {
            guard.index.as_ref().map_or(0, |index| index.len())
        } else {
            0
        };
        let ((doc_filter, allow, _prune), read_receipt) = guard.vector_scan_filters(
            slots,
            filters.geo,
            &filters.regions,
            filters.tree,
            read_context,
            self.knobs.segment_pruning(),
        )?;

        let vector_leg = if k > 0 && !vector.is_empty() {
            self.vector_leg(&guard, vector, k, allow.as_ref())?
        } else {
            Vec::new()
        };
        let bm25_leg = match guard.bm25.as_ref() {
            Some(store) if k > 0 && !terms.is_empty() => {
                let stats = bm25::CorpusStats {
                    doc_count: global_doc_count,
                    total_doc_length: global_total_doc_length,
                    dfs: global_doc_frequencies.to_vec(),
                };
                self.bm25_leg(store, terms, &stats, params, doc_filter.as_ref(), k)?
            }
            _ => Vec::new(),
        };
        let rerank_candidates = if include_rerank_candidate {
            self.rerank_candidates(&guard, vector_leg.iter().chain(&bm25_leg))
        } else {
            HashMap::new()
        };
        Ok(LegResults {
            vector: vector_leg,
            bm25: bm25_leg,
            geo_columns_known,
            filter_columns_known,
            read_receipt,
            rerank_candidates,
        })
    }

    /// The vector leg: exact top-k over the shard's index under `allow`.
    fn vector_leg(
        &self,
        guard: &ShardState,
        vector: &[f32],
        k: usize,
        allow: Option<&crate::boolean_bits::Bits>,
    ) -> Result<RawLeg, Status> {
        let Some(index) = guard.index.as_ref() else {
            return Ok(Vec::new());
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
        let (hits, scan_stats) = chunked_topk(
            crate::chunked::ChunkedScan {
                index,
                query: vector,
                k,
                chunk_blocks: self.config.chunk_blocks,
                allow: allow.map(crate::vector::Mask::Bits),
            },
            false,
            &mut || None,
            &mut |_| false,
        );
        observe_scan(scan_stats.bytes_scanned, scan_stats.scan_nanos);
        Ok(hits
            .into_iter()
            .map(|h| {
                (
                    self.config.slot_offset + u64::from(h.slot),
                    f64::from(h.score),
                )
            })
            .collect())
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
        let mut legs = self.compute_legs(
            &req.vector,
            &req.terms,
            req.analysis_fingerprint,
            req.global_doc_count,
            req.global_total_doc_length,
            &req.global_doc_frequencies,
            params_from(req.k1, req.b)?,
            k,
            req.expected_stats_epoch,
            &req.expected_stats_incarnation,
            &LegFilters {
                geo: &req.geo_filters,
                regions: geo_regions,
                tree: req.filter.as_ref(),
            },
            req.read_context.as_ref(),
            req.include_rerank_candidate,
        )?;
        let geo_columns_known = legs.geo_columns_known;
        let filter_columns_known = legs.filter_columns_known;

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
            hits: fused
                .into_iter()
                .map(|h| HybridLegHit {
                    doc_id: h.doc_id,
                    fused_score: h.fused_score as f32,
                    vector_rank: h.leg_ranks[0],
                    vector_score: h.leg_scores[0].unwrap_or(0.0) as f32,
                    bm25_rank: h.leg_ranks[1],
                    bm25_score: h.leg_scores[1].unwrap_or(0.0) as f32,
                    rerank_candidate: legs.rerank_candidates.remove(&h.doc_id),
                })
                .collect(),
            geo_columns_known,
            filter_columns_known,
            read_receipt: legs.read_receipt,
        })
    }
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

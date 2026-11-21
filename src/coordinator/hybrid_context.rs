//! Validated hybrid inputs and statistics bound to each fan-out round.

use super::*;

#[derive(Clone, Copy)]
pub(super) struct HybridInput<'a> {
    request_id: &'a str,
    text: &'a str,
    pre_analyzed_terms: &'a [String],
    vector: &'a [f32],
    k: u32,
    spec: Option<&'a crate::pb::AnalysisSpec>,
    filters: &'a RequestFilters,
}

impl<'a> HybridInput<'a> {
    pub(super) fn new(
        request_id: &'a str,
        text: &'a str,
        vector: &'a [f32],
        k: u32,
        spec: Option<&'a crate::pb::AnalysisSpec>,
        filters: &'a RequestFilters,
    ) -> Result<Self, Status> {
        if vector.iter().any(|value| !value.is_finite()) {
            return Err(Status::invalid_argument(
                "query vector has non-finite coordinates",
            ));
        }
        crate::node::validate_geo_filters(&filters.geo)?;
        if let Some(filter) = &filters.tree {
            crate::filter::validate_filter(filter)?;
        }
        Ok(Self {
            request_id,
            text,
            pre_analyzed_terms: &[],
            vector,
            k,
            spec,
            filters,
        })
    }
    pub(super) fn request_id(self) -> &'a str {
        self.request_id
    }
    pub(super) fn text(self) -> &'a str {
        self.text
    }
    pub(super) fn pre_analyzed_terms(self) -> &'a [String] {
        self.pre_analyzed_terms
    }
    pub(super) fn vector(self) -> &'a [f32] {
        self.vector
    }
    pub(super) fn k(self) -> u32 {
        self.k
    }
    pub(super) fn spec(self) -> Option<&'a crate::pb::AnalysisSpec> {
        self.spec
    }
    pub(super) fn filters(self) -> &'a RequestFilters {
        self.filters
    }
}

/// A validated hybrid search: input, fusion legs and debug flag.
#[derive(Clone, Copy)]
pub struct HybridQuery<'a> {
    input: HybridInput<'a>,
    legs: HybridLegs,
    debug: bool,
}
impl<'a> HybridQuery<'a> {
    pub(super) fn new(
        input: HybridInput<'a>,
        legs: HybridLegs,
        debug: bool,
    ) -> Result<Self, Status> {
        validate_legs(legs)?;
        Ok(Self { input, legs, debug })
    }
    pub fn from_request(
        request_id: &'a str,
        request: &'a HybridSearchRequest,
        k: u32,
        legs: HybridLegs,
        filters: &'a RequestFilters,
    ) -> Result<Self, Status> {
        let mut input = HybridInput::new(
            request_id,
            &request.text,
            &request.vector,
            k,
            request.analysis.as_ref(),
            filters,
        )?;
        input.pre_analyzed_terms = &request.pre_analyzed_terms;
        Self::new(input, legs, request.debug)
    }
    pub(super) fn input(self) -> HybridInput<'a> {
        self.input
    }
    pub(super) fn legs(self) -> HybridLegs {
        self.legs
    }
    pub(super) fn debug(self) -> bool {
        self.debug
    }
}

/// A validated cascade search: input, vector floor and debug flag.
#[derive(Clone, Copy)]
pub struct CascadeQuery<'a> {
    input: HybridInput<'a>,
    min_vector_score: f32,
    debug: bool,
}
impl<'a> CascadeQuery<'a> {
    pub(super) fn new(
        input: HybridInput<'a>,
        min_vector_score: f32,
        debug: bool,
    ) -> Result<Self, Status> {
        validate_vector_floor(min_vector_score)?;
        Ok(Self {
            input,
            min_vector_score,
            debug,
        })
    }
    pub fn from_request(
        request_id: &'a str,
        request: &'a HybridSearchRequest,
        k: u32,
        min_vector_score: f32,
        filters: &'a RequestFilters,
    ) -> Result<Self, Status> {
        let mut input = HybridInput::new(
            request_id,
            &request.text,
            &request.vector,
            k,
            request.analysis.as_ref(),
            filters,
        )?;
        input.pre_analyzed_terms = &request.pre_analyzed_terms;
        Self::new(input, min_vector_score, request.debug)
    }
    pub(super) fn input(self) -> HybridInput<'a> {
        self.input
    }
    pub(super) fn min_vector_score(self) -> f32 {
        self.min_vector_score
    }
    pub(super) fn debug(self) -> bool {
        self.debug
    }
}

fn validate_vector_floor(floor: f32) -> Result<(), Status> {
    if floor.is_nan() {
        return Err(Status::invalid_argument("min_vector_score must not be NaN"));
    }
    Ok(())
}

fn validate_legs(legs: HybridLegs) -> Result<(), Status> {
    validate_vector_floor(legs.min_vector_score)?;
    if legs.rrf_k <= 0.0 || legs.rrf_k.is_nan() {
        return Err(Status::invalid_argument("rrf_k must be > 0"));
    }
    if legs.vector_weight < 0.0
        || legs.vector_weight.is_nan()
        || legs.bm25_weight < 0.0
        || legs.bm25_weight.is_nan()
    {
        return Err(Status::invalid_argument("hybrid leg weights must be >= 0"));
    }
    if legs.vector_weight == 0.0 && legs.bm25_weight == 0.0 {
        return Err(Status::invalid_argument(
            "both legs disabled: at least one of vector_weight/bm25_weight must be nonzero",
        ));
    }
    if legs.fusion_mode == FusionMode::TwoLevel
        && (legs.vector_weight == 0.0 || legs.bm25_weight == 0.0)
    {
        return Err(Status::invalid_argument("TWO_LEVEL cannot disable a leg (its node wire format cannot distinguish 0 from unset); use GLOBAL_RANK or SCORE_BLEND"));
    }
    if legs.fusion_mode == FusionMode::Decomposed
        && !(legs.vector_weight > 0.0
            && legs.vector_weight.is_finite()
            && legs.bm25_weight > 0.0
            && legs.bm25_weight.is_finite())
    {
        return Err(Status::invalid_argument(
            "DECOMPOSED requires finite leg weights > 0 for both legs",
        ));
    }
    if legs.min_vector_score > 0.0 && legs.vector_weight == 0.0 {
        return Err(Status::invalid_argument("min_vector_score is set but the vector leg is disabled (vector_weight 0); no hit can carry a qualifying vector score"));
    }
    Ok(())
}

#[derive(Clone, Copy)]
pub(super) struct HybridRound<'a> {
    query: HybridQuery<'a>,
    terms: &'a [String],
    analysis_fingerprint: u64,
    global: &'a CorpusStats,
    claims: &'a [StatsClaim],
}

impl<'a> HybridRound<'a> {
    pub(super) fn query(self) -> HybridQuery<'a> {
        self.query
    }
    pub(super) fn terms(self) -> &'a [String] {
        self.terms
    }
    pub(super) fn analysis_fingerprint(self) -> u64 {
        self.analysis_fingerprint
    }
    pub(super) fn global(self) -> &'a CorpusStats {
        self.global
    }
    pub(super) fn claims(self) -> &'a [StatsClaim] {
        self.claims
    }

    pub(super) fn new(
        query: HybridQuery<'a>,
        terms: &'a [String],
        analysis_fingerprint: u64,
        global: &'a CorpusStats,
        claims: &'a [StatsClaim],
        shards: usize,
    ) -> Result<Self, Status> {
        if claims.len() != shards || global.dfs.len() != terms.len() {
            return Err(Status::internal(
                "hybrid statistics do not match the query round",
            ));
        }
        Ok(Self {
            query,
            terms,
            analysis_fingerprint,
            global,
            claims,
        })
    }

    pub(super) fn shard_request(
        &self,
        params: Bm25Params,
        shard: usize,
        read_context: Option<crate::pb::VectorReadContext>,
        filter: Option<crate::pb::FilterExpr>,
        clustered: bool,
        two_level: bool,
    ) -> ShardLegsRequest {
        let (vector, terms, dfs) = leg_payloads(
            self.query.input.vector,
            self.terms,
            self.global,
            self.query.legs,
        );
        ShardLegsRequest {
            read_context,
            analysis_fingerprint: self.analysis_fingerprint,
            request_id: if clustered || two_level {
                self.query.input.request_id.to_string()
            } else {
                String::new()
            },
            k: self.query.legs.leg_k,
            vector: if clustered { Vec::new() } else { vector },
            terms,
            global_doc_count: self.global.doc_count,
            global_total_doc_length: self.global.total_doc_length,
            global_doc_frequencies: dfs,
            k1: params.k1 as f32,
            b: params.b as f32,
            expected_stats_epoch: self.claims[shard].epoch,
            expected_stats_incarnation: self.claims[shard].incarnation(),
            geo_filters: self.query.input.filters.geo.clone(),
            filter,
            include_rerank_candidate: self.query.input.filters.include_rerank_candidate,
            enumerate_max_rows: self.query.input.filters.enumerate_max_rows,
            filter_strategy: self.query.input.filters.filter_strategy,
            enumerate_all: self.query.input.filters.enumerate_all,
            patience_saturation_ppm: self.query.input.filters.patience_saturation_ppm,
            patience_hops: self.query.input.filters.patience_hops,
            floor_prune: self.query.input.filters.floor_prune,
            floor_greediness_ppm: self.query.input.filters.floor_greediness_ppm,
            floor_clamp_min: self.query.input.filters.floor_clamp_min,
        }
    }
}

pub(super) fn local_fusion_request(
    request: ShardLegsRequest,
    legs: HybridLegs,
) -> HybridShardRequest {
    HybridShardRequest {
        read_context: request.read_context,
        analysis_fingerprint: request.analysis_fingerprint,
        request_id: request.request_id,
        k: request.k,
        vector: request.vector,
        terms: request.terms,
        global_doc_count: request.global_doc_count,
        global_total_doc_length: request.global_total_doc_length,
        global_doc_frequencies: request.global_doc_frequencies,
        vector_weight: legs.vector_weight,
        bm25_weight: legs.bm25_weight,
        rrf_k: legs.rrf_k as f32,
        k1: request.k1,
        b: request.b,
        expected_stats_epoch: request.expected_stats_epoch,
        expected_stats_incarnation: request.expected_stats_incarnation,
        geo_filters: request.geo_filters,
        filter: request.filter,
        include_rerank_candidate: request.include_rerank_candidate,
        enumerate_max_rows: request.enumerate_max_rows,
        filter_strategy: request.filter_strategy,
        enumerate_all: request.enumerate_all,
        patience_saturation_ppm: request.patience_saturation_ppm,
        patience_hops: request.patience_hops,
        floor_prune: request.floor_prune,
        floor_greediness_ppm: request.floor_greediness_ppm,
        floor_clamp_min: request.floor_clamp_min,
    }
}

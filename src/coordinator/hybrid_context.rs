//! Hybrid query inputs and validated statistics rounds.

use super::*;

/// Inputs shared by the hybrid fusion routes.
#[derive(Clone, Copy)]
pub struct HybridQuery<'a> {
    pub request_id: &'a str,
    pub text: &'a str,
    pub pre_analyzed_terms: &'a [String],
    pub vector: &'a [f32],
    pub k: u32,
    pub spec: Option<&'a crate::pb::AnalysisSpec>,
    pub legs: HybridLegs,
    pub debug: bool,
    pub filters: &'a RequestFilters,
}

/// Inputs for vector candidate generation followed by lexical reranking.
#[derive(Clone, Copy)]
pub struct CascadeQuery<'a> {
    pub request_id: &'a str,
    pub text: &'a str,
    pub pre_analyzed_terms: &'a [String],
    pub vector: &'a [f32],
    pub k: u32,
    pub spec: Option<&'a crate::pb::AnalysisSpec>,
    pub min_vector_score: f32,
    pub debug: bool,
    pub filters: &'a RequestFilters,
}

#[derive(Clone, Copy)]
pub(super) struct HybridRound<'a> {
    pub(super) query: HybridQuery<'a>,
    pub(super) terms: &'a [String],
    pub(super) analysis_fingerprint: u64,
    pub(super) global: &'a CorpusStats,
    pub(super) claims: &'a [StatsClaim],
}

impl<'a> HybridRound<'a> {
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
        let (vector, terms, dfs) =
            leg_payloads(self.query.vector, self.terms, self.global, self.query.legs);
        ShardLegsRequest {
            read_context,
            analysis_fingerprint: self.analysis_fingerprint,
            request_id: if clustered || two_level {
                self.query.request_id.to_string()
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
            geo_filters: self.query.filters.geo.clone(),
            filter,
            include_rerank_candidate: self.query.filters.include_rerank_candidate,
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
    }
}

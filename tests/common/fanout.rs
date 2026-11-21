//! Concise coordinator fan-out helpers for integration tests exercising
//! lexical, hybrid, and cascade query contexts.

use pipestream_search::coordinator::{
    CascadeQuery, CoordinatorServiceImpl, FlatLexicalQuery, FusedLexicalQuery, HybridLegs,
    HybridQuery, RequestFilters,
};
use pipestream_search::pb::{
    Bm25Hit, Bm25SearchRequest, CascadeHit, FacetFieldCounts, FilterExpr, GeoFilter, HybridDebug,
    HybridHit, HybridSearchRequest, MapFacetField, RangeFacetCounts, RangeFacetField, ScoreStage,
};
use tonic::Status;

pub type LexicalHits = (Vec<Bm25Hit>, Vec<FacetFieldCounts>, Vec<RangeFacetCounts>);

pub async fn bm25_req(
    coord: &CoordinatorServiceImpl,
    req: Bm25SearchRequest,
    k: u32,
    filter: Option<&FilterExpr>,
) -> Result<LexicalHits, Status> {
    let query = FlatLexicalQuery::new(&req, k, &[], filter)?;
    coord.fanout_bm25_context(query).await
}

pub async fn bm25_fused_req(
    coord: &CoordinatorServiceImpl,
    req: Bm25SearchRequest,
    k: u32,
    filter: Option<&FilterExpr>,
) -> Result<LexicalHits, Status> {
    let query = FusedLexicalQuery::new(&req, k, filter)?;
    coord.fanout_bm25_fused_context(query).await
}

pub async fn bm25_facets(
    coord: &CoordinatorServiceImpl,
    text: &str,
    k: u32,
    min_score: f32,
    facets: &[String],
    filter: Option<&FilterExpr>,
) -> Result<LexicalHits, Status> {
    bm25_req(
        coord,
        Bm25SearchRequest {
            text: text.into(),
            min_score,
            facet_fields: facets.to_vec(),
            ..Default::default()
        },
        k,
        filter,
    )
    .await
}

pub async fn bm25_map_facets(
    coord: &CoordinatorServiceImpl,
    text: &str,
    k: u32,
    map_facets: &[MapFacetField],
    filter: Option<&FilterExpr>,
) -> Result<LexicalHits, Status> {
    bm25_req(
        coord,
        Bm25SearchRequest {
            text: text.into(),
            map_facet_fields: map_facets.to_vec(),
            ..Default::default()
        },
        k,
        filter,
    )
    .await
}

pub async fn bm25_range_facets(
    coord: &CoordinatorServiceImpl,
    text: &str,
    k: u32,
    ranges: &[RangeFacetField],
    filter: Option<&FilterExpr>,
) -> Result<LexicalHits, Status> {
    bm25_req(
        coord,
        Bm25SearchRequest {
            text: text.into(),
            range_facet_fields: ranges.to_vec(),
            ..Default::default()
        },
        k,
        filter,
    )
    .await
}

pub async fn bm25_geo(
    coord: &CoordinatorServiceImpl,
    text: &str,
    k: u32,
    geo: &[GeoFilter],
    filter: Option<&FilterExpr>,
) -> Result<LexicalHits, Status> {
    bm25_req(
        coord,
        Bm25SearchRequest {
            text: text.into(),
            geo_filters: geo.to_vec(),
            ..Default::default()
        },
        k,
        filter,
    )
    .await
}

pub async fn bm25_stages(
    coord: &CoordinatorServiceImpl,
    text: &str,
    k: u32,
    stages: &[ScoreStage],
    filter: Option<&FilterExpr>,
) -> Result<LexicalHits, Status> {
    bm25_req(
        coord,
        Bm25SearchRequest {
            text: text.into(),
            score_stages: stages.to_vec(),
            ..Default::default()
        },
        k,
        filter,
    )
    .await
}

pub async fn hybrid_filtered(
    coord: &CoordinatorServiceImpl,
    id: &str,
    text: &str,
    vector: &[f32],
    k: u32,
    legs: HybridLegs,
    filters: &RequestFilters,
) -> Result<(Vec<HybridHit>, Option<HybridDebug>), Status> {
    let req = HybridSearchRequest {
        text: text.into(),
        vector: vector.to_vec(),
        ..Default::default()
    };
    let query = HybridQuery::from_request(id, &req, k, legs, filters)?;
    coord.fanout_hybrid_context(query).await
}

pub async fn hybrid(
    coord: &CoordinatorServiceImpl,
    id: &str,
    text: &str,
    vector: &[f32],
    k: u32,
    legs: HybridLegs,
) -> Result<(Vec<HybridHit>, Option<HybridDebug>), Status> {
    hybrid_filtered(coord, id, text, vector, k, legs, &RequestFilters::default()).await
}

pub async fn hybrid_debug(
    coord: &CoordinatorServiceImpl,
    id: &str,
    text: &str,
    vector: &[f32],
    k: u32,
    legs: HybridLegs,
) -> Result<(Vec<HybridHit>, Option<HybridDebug>), Status> {
    let req = HybridSearchRequest {
        text: text.into(),
        vector: vector.to_vec(),
        debug: true,
        ..Default::default()
    };
    let filters = RequestFilters::default();
    let query = HybridQuery::from_request(id, &req, k, legs, &filters)?;
    coord.fanout_hybrid_context(query).await
}

pub async fn cascade(
    coord: &CoordinatorServiceImpl,
    id: &str,
    text: &str,
    vector: &[f32],
    k: u32,
    min_vector_score: f32,
    filters: &RequestFilters,
) -> Result<(Vec<CascadeHit>, Option<HybridDebug>), Status> {
    let req = HybridSearchRequest {
        text: text.into(),
        vector: vector.to_vec(),
        ..Default::default()
    };
    let query = CascadeQuery::from_request(id, &req, k, min_vector_score, filters)?;
    coord.fanout_cascade_context(query).await
}

pub async fn cascade_debug(
    coord: &CoordinatorServiceImpl,
    id: &str,
    text: &str,
    vector: &[f32],
    k: u32,
    min_vector_score: f32,
) -> Result<(Vec<CascadeHit>, Option<HybridDebug>), Status> {
    let req = HybridSearchRequest {
        text: text.into(),
        vector: vector.to_vec(),
        debug: true,
        ..Default::default()
    };
    let filters = RequestFilters::default();
    let query = CascadeQuery::from_request(id, &req, k, min_vector_score, &filters)?;
    coord.fanout_cascade_context(query).await
}

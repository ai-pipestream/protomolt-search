//! The two-leaf composite: the `HybridSearch` route under the fusion
//! mode its strategy maps onto.

use tonic::{Request, Status};

use super::boosts::{weight_or_one, BoostPlan};
use super::composite_shape::leg_options;
use super::pool::{answer_pool, Answer};
use super::rerank::{turbo_rerank, RerankSources};
use super::response::{matched, ms, scored_hit};
use super::run::Run;
use super::selection::Strategy;
use crate::pb::search_service_server::SearchService;
use crate::pb::{
    BoostRescore, DenseQuery, FusionMode, HybridLegOptions, HybridSearchRequest,
    HybridSearchResponse, LexicalQuery, QueryHit, QueryResponse, QuerySignal,
};

/// The composite's two scoring leaves with their request ids, and the
/// strategy that fuses them.
pub(super) struct Legs<'a> {
    pub(super) dense_id: &'a str,
    pub(super) dense: &'a DenseQuery,
    pub(super) lexical_id: &'a str,
    pub(super) lexical: &'a LexicalQuery,
    pub(super) strategy: &'a Strategy<'a>,
}

/// A hit's provenance as it accumulates: the signals that scored it and
/// the ids it matched, in the same order.
#[derive(Default)]
struct Provenance {
    signals: Vec<QuerySignal>,
    matched: Vec<String>,
}

impl Provenance {
    fn push(&mut self, id: &str, score: f32) {
        self.signals.push(QuerySignal {
            id: id.to_string(),
            score,
        });
        self.matched.push(id.to_string());
    }

    /// The legacy hybrid boost's signal, present only where it scored.
    fn push_boost(&mut self, boost_id: Option<&str>, score: f32) {
        if let Some(id) = boost_id {
            if score != 0.0 {
                self.push(id, score);
            }
        }
    }

    fn into_hit(self, doc_id: u64, score: f32, filter_ids: &[&str]) -> QueryHit {
        scored_hit(
            doc_id,
            score,
            self.signals,
            matched(self.matched, filter_ids),
        )
    }
}

pub(super) async fn execute_composite(
    mut run: Run<'_>,
    legs: Legs<'_>,
) -> Result<QueryResponse, Status> {
    let base = run.base;
    let (mode, leg_opts) = leg_options(legs.strategy, run.depths.selection_k);
    let legacy = match &run.boosts {
        BoostPlan::LegacyHybrid(id, b) => Some((id.clone(), b.clone())),
        _ => None,
    };
    let t_sel = std::time::Instant::now();
    let request = hybrid_request(&mut run, &legs, &leg_opts, legacy.as_ref().map(|(_, b)| b));
    let response = base
        .coordinator
        .hybrid_search(crate::metrics::nested(Request::new(request)))
        .await?
        .into_inner();
    if let Some(p) = run.prof.as_mut() {
        p.selection_ms = ms(t_sel);
    }
    let boost_id = legacy.as_ref().map(|(id, _)| id.as_str());
    let filter_ids = &base.plan.filter_ids;
    let cascade = matches!(legs.strategy, Strategy::Cascade);
    let mut hits = if cascade {
        cascade_hits(&response, &legs, boost_id, filter_ids)
    } else {
        fused_hits(&response, &legs, mode, boost_id, filter_ids)
    };
    let mut route = match mode {
        FusionMode::GlobalRank => "hybrid_search:global_rank",
        FusionMode::ScoreBlend => "hybrid_search:score_blend",
        FusionMode::Decomposed => "hybrid_search:decomposed",
        _ => "hybrid_search:cascade",
    };
    if base.req.explain {
        let fusion = crate::explain::Fusion::resolve(mode, &leg_opts);
        explain_hits(&mut hits, &response, &legs, &fusion, legacy.as_ref())?;
    }
    let sources = rerank_sources(base.coordinator.rerank_spec(), &hits, &response);
    if turbo_rerank(&base, &legs.lexical.text, &sources, &mut hits).await? {
        route = "hybrid_search:turbo_rerank";
    }
    let mut answer = match answer_pool(&mut run, hits, route, response.request_id).await? {
        Answer::Deepened(response) => return Ok(response),
        Answer::Paged(response) => response,
    };
    answer.dense_execution = run.dense.execution;
    if let Some(bound) = &run.one_pass {
        answer
            .synonym_expansions
            .extend(bound.synonym_expansions.iter().cloned());
    }
    Ok(answer)
}

/// The route request. The selection phase runs selection_k deep; the
/// final page trims to k afterwards. For the exact modes the trimmed
/// prefix IS the top-k (prefix property of a total order).
fn hybrid_request(
    run: &mut Run<'_>,
    legs: &Legs<'_>,
    leg_opts: &HybridLegOptions,
    legacy_boost: Option<&BoostRescore>,
) -> HybridSearchRequest {
    let knobs = run.dense.knobs;
    HybridSearchRequest {
        request_id: run.base.req.request_id.clone(),
        text: run.effective_text(legs.lexical),
        vector: legs.dense.vector.clone(),
        k: run.depths.selection_k,
        analysis: legs.lexical.analysis.clone(),
        legs: Some(*leg_opts),
        boost: legacy_boost.cloned(),
        geo_filters: run.base.plan.geo_filters.clone(),
        filter: std::mem::take(&mut run.filter),
        enumerate_max_rows: knobs.enumerate_max_rows,
        patience_saturation_ppm: knobs.patience_saturation_ppm,
        patience_hops: knobs.patience_hops,
        finish_settle_us: knobs.finish_settle_us,
        floor_prune: knobs.floor_prune,
        floor_greediness_ppm: knobs.floor_greediness_ppm,
        floor_clamp_min: knobs.floor_clamp_min,
        include_rerank_candidate: run.base.coordinator.rerank_spec().is_some(),
        pre_analyzed_terms: run.residual_terms(legs.lexical),
        ..Default::default()
    }
}

/// The owning shard and inline candidate payload of every shortlisted
/// hit, the cascade hits' report taking precedence.
fn rerank_sources(
    spec: Option<&crate::turbo_bridge::rerank::RerankSpec>,
    hits: &[QueryHit],
    response: &HybridSearchResponse,
) -> RerankSources {
    let mut sources = RerankSources::for_shortlist(spec, hits);
    if !sources.is_enabled() {
        return sources;
    }
    for h in &response.cascade_hits {
        sources.note_owner(h.doc_id, h.shard as usize);
        sources.note_candidate(h.doc_id, h.rerank_candidate.as_ref());
    }
    for h in &response.hits {
        sources.note_owner(h.doc_id, h.shard as usize);
        sources.note_candidate(h.doc_id, h.rerank_candidate.as_ref());
    }
    sources
}

/// Cascade hits: both legs scored every admitted hit. Cascade's final
/// order is the rerank leg's, so the score is the rerank leg's raw
/// relevance.
fn cascade_hits(
    response: &HybridSearchResponse,
    legs: &Legs<'_>,
    boost_id: Option<&str>,
    filter_ids: &[&str],
) -> Vec<QueryHit> {
    response
        .cascade_hits
        .iter()
        .map(|h| {
            let mut provenance = Provenance::default();
            provenance.push(legs.dense_id, h.vector_score);
            provenance.push(legs.lexical_id, h.bm25_score);
            provenance.push_boost(boost_id, h.boost_score);
            provenance.into_hit(h.doc_id, h.bm25_score, filter_ids)
        })
        .collect()
}

/// Fused hits: a leg's signal is present where the leg retained the
/// hit. DECOMPOSED is rank-free (an exact full-corpus weighted sum):
/// leg ranks are never set, but every hit's vector score is real, so
/// the dense signal is always present under it.
fn fused_hits(
    response: &HybridSearchResponse,
    legs: &Legs<'_>,
    mode: FusionMode,
    boost_id: Option<&str>,
    filter_ids: &[&str],
) -> Vec<QueryHit> {
    response
        .hits
        .iter()
        .map(|h| {
            let mut provenance = Provenance::default();
            if h.vector_rank.is_some() || mode == FusionMode::Decomposed {
                provenance.push(legs.dense_id, h.vector_score);
            }
            if h.bm25_rank.is_some() {
                provenance.push(legs.lexical_id, h.bm25_score);
            }
            provenance.push_boost(boost_id, h.boost_score);
            provenance.into_hit(h.doc_id, h.fused_score, filter_ids)
        })
        .collect()
}

fn explain_hits(
    hits: &mut [QueryHit],
    response: &HybridSearchResponse,
    legs: &Legs<'_>,
    fusion: &crate::explain::Fusion,
    legacy: Option<&(String, BoostRescore)>,
) -> Result<(), Status> {
    let legacy_boost = legacy.map(|(id, b)| crate::explain::LegacyBoost {
        id: id.clone(),
        base_weight: weight_or_one(b.base_weight),
        boost_weight: weight_or_one(b.boost_weight),
    });
    if matches!(legs.strategy, Strategy::Cascade) {
        for (hit, source) in hits.iter_mut().zip(&response.cascade_hits) {
            hit.explain = Some(crate::explain::cascade(
                source,
                legs.dense_id,
                legs.lexical_id,
                legacy_boost.as_ref(),
            ));
        }
    } else {
        for (hit, source) in hits.iter_mut().zip(&response.hits) {
            hit.explain = Some(crate::explain::composite(
                source,
                fusion,
                legs.dense_id,
                legs.lexical_id,
                legacy_boost.as_ref(),
            )?);
        }
    }
    Ok(())
}

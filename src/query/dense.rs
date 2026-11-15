//! The single dense leaf: the `Search` route, with the optional exact
//! FP32 rescore of its candidate pool.

use tonic::{Request, Status};

use super::pool::{answer_pool, Answer};
use super::rerank::{turbo_rerank, RerankSources};
use super::response::{leaf_signal, matched, ms, scored_hit};
use super::run::Run;
use super::selection::dense_score_mode;
use crate::pb::search_service_server::SearchService;
use crate::pb::{
    DenseExecutionMode, DenseQuery, DenseScoreMode, QueryHit, QueryResponse, SearchRequest,
    SearchResponse,
};

pub(super) async fn execute_dense(
    mut run: Run<'_>,
    id: &str,
    query: &DenseQuery,
) -> Result<QueryResponse, Status> {
    let base = run.base;
    let t_sel = std::time::Instant::now();
    let request = search_request(&mut run, query);
    let response = base
        .coordinator
        .search(crate::metrics::nested(Request::new(request)))
        .await?
        .into_inner();
    if let Some(p) = run.prof.as_mut() {
        p.selection_ms = ms(t_sel);
        p.segments_total = response.segments_total;
        p.segments_skipped = response.segments_skipped;
    }
    report_shard_regimes(&mut run, &response);
    let mut sources = RerankSources::default();
    for h in &response.hits {
        sources.note_candidate(h.vector_id, h.rerank_candidate.as_ref());
    }
    let matched = matched(vec![id.to_string()], &base.plan.filter_ids);
    let mut hits: Vec<QueryHit> = response
        .hits
        .iter()
        .map(|h| QueryHit {
            identity: h.identity.clone(),
            ..scored_hit(
                h.vector_id,
                h.score,
                leaf_signal(id, h.score),
                matched.clone(),
            )
        })
        .collect();
    let dense_mode = dense_score_mode(query)?;
    let mut route = if run.dense.fp32_rerank {
        fp32_rescore(&mut run, id, query, dense_mode, &mut hits).await?;
        "search:fp32_rerank"
    } else {
        if base.req.explain {
            for hit in &mut hits {
                hit.explain = Some(crate::explain::dense(id, hit.score, dense_mode, None));
            }
        }
        "search"
    };
    if turbo_rerank(&base, &base.req.request_id, &sources, &mut hits).await? {
        route = "search:turbo_rerank";
    }
    let mut answer = match answer_pool(&mut run, hits, route, response.request_id).await? {
        Answer::Deepened(response) => return Ok(response),
        Answer::Paged(response) => response,
    };
    report_dense(run, &mut answer);
    Ok(answer)
}

fn search_request(run: &mut Run<'_>, query: &DenseQuery) -> SearchRequest {
    let knobs = run.dense.knobs;
    SearchRequest {
        request_id: run.base.req.request_id.clone(),
        k: run.depths.fetch_k,
        vector: query.vector.clone(),
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
        ..Default::default()
    }
}

/// The regime the shards answered the filter in is theirs to report.
fn report_shard_regimes(run: &mut Run<'_>, response: &SearchResponse) {
    if let Some(outcome) = run.dense.execution.as_mut() {
        outcome.filter_regimes = response.filter_regimes;
        outcome.tail_enumerated_rows = response.tail_enumerated_rows;
        outcome.patience_stopped = response.patience_stopped;
        outcome.shards_finished_early = response.shards_finished_early;
        outcome.shards_floor_pruned = response.shards_floor_pruned;
    }
}

/// Rescore the candidate pool against the stored FP32 vectors and
/// reorder it by the exact score, ties by doc id. Every candidate must
/// resolve an exact score.
async fn fp32_rescore(
    run: &mut Run<'_>,
    id: &str,
    query: &DenseQuery,
    dense_mode: DenseScoreMode,
    hits: &mut [QueryHit],
) -> Result<(), Status> {
    let t0 = std::time::Instant::now();
    let ids: Vec<u64> = hits.iter().map(|hit| hit.doc_id).collect();
    let reranked = run
        .base
        .coordinator
        .exact_vector_scores(&query.vector, &ids, &query.field)
        .await?;
    for hit in hits.iter_mut() {
        let score = *reranked.scores.get(&hit.doc_id).ok_or_else(|| {
            Status::failed_precondition(format!(
                "FP32 rerank candidate {} has no exact score",
                hit.doc_id
            ))
        })?;
        if run.base.req.explain {
            hit.explain = Some(crate::explain::dense(
                id,
                score,
                dense_mode,
                Some(hit.score),
            ));
        }
        hit.score = score;
        hit.signals[0].score = score;
    }
    hits.sort_by(|a, b| {
        b.score
            .total_cmp(&a.score)
            .then_with(|| a.doc_id.cmp(&b.doc_id))
    });
    if let Some(p) = run.prof.as_mut() {
        p.rerank_ms = ms(t0);
        p.rerank_rows = reranked.rows;
        p.rerank_logical_bytes = reranked.logical_bytes;
        p.rerank_pages = reranked.pages_touched;
        p.rerank_tasks = reranked.tasks;
    }
    Ok(())
}

/// What only the dense route reports: the quality resolution that fixed
/// the depth, and the execution outcome with the planner's reasons.
fn report_dense(run: Run<'_>, response: &mut QueryResponse) {
    let mut dense = run.dense;
    if dense.auto_default_depth {
        if let (Some(outcome), Some(resolution)) =
            (dense.execution.as_mut(), dense.quality.as_ref())
        {
            outcome.planner_reason.push_str(&format!(
                "; FP32 rerank depth selection_k={} resolved through quality profile \
                 {:?} default_target_recall_ppm={} (as an explicit DenseQualityPolicy would)",
                resolution.selection_k, resolution.profile_id, resolution.target_recall_ppm
            ));
        }
    }
    response.dense_quality = dense
        .quality
        .map(|resolution| crate::pb::DenseQualityOutcome {
            evidence_scope: crate::pb::DenseEvidenceScope::CorpusBenchmark as i32,
            target_recall_ppm: resolution.target_recall_ppm,
            selection_k: resolution.selection_k,
            profile_fingerprint: resolution.profile_fingerprint,
            profile_id: resolution.profile_id,
            embedding_model: resolution.embedding_model,
            corpus_generation: resolution.corpus_generation,
            corpus_rows: resolution.corpus_rows,
            provider_backend: resolution.provider_backend,
            scoring_fingerprint: resolution.scoring_fingerprint,
        });
    if dense.fp32_rerank {
        if let Some(outcome) = dense.execution.as_mut() {
            if outcome.resolved_mode == DenseExecutionMode::Ann as i32 {
                outcome.planner_reason.push_str(
                    "; FP32 rerank rescored that candidate pool without widening it, so \
                     the traversal stays approximate",
                );
            }
        }
    }
    response.dense_execution = dense.execution;
}

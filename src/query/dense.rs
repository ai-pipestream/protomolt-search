//! The single dense leaf: the `Search` route, with the optional exact
//! FP32 rescore of its candidate pool or shard-local exact FP32 scan.

use tonic::Status;

use super::pool::{answer_pool, Answer, Routed};
use super::rerank::{turbo_rerank, RerankSources};
use super::response::{leaf_signal, matched, ms, scored_hit};
use super::run::Run;
use crate::coordinator::{dense_status, DenseScanExecution};
use crate::dense_execution::Route;
use crate::pb::{DenseQuery, QueryHit, QueryResponse, SearchRequest, SearchResponse};

pub(super) async fn execute_dense(
    mut run: Run<'_>,
    id: &str,
    query: &DenseQuery,
) -> Result<Routed, Status> {
    let base = run.base;
    let exact_fp32 = run.dense.route() == Some(Route::Fp32AllRows);
    let t_sel = std::time::Instant::now();
    let request = search_request(&mut run, query);
    let (response, exec) = base
        .coordinator
        .search_with_evidence(request, exact_fp32)
        .await?;
    record_selection_profile(&mut run, t_sel, &response, &exec, exact_fp32);
    stamp_receipt(&mut run, exec.evidence)?;
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
    let mut route = match run.dense.route() {
        Some(Route::Fp32Pool) => {
            fp32_rescore(&mut run, id, query, &mut hits).await?;
            "search:fp32_pool"
        }
        Some(Route::Fp32AllRows) => {
            explain_native_hits(base.req.explain, id, &mut hits);
            "search:fp32_scan"
        }
        _ => {
            explain_native_hits(base.req.explain, id, &mut hits);
            "search"
        }
    };
    let sources = rerank_sources(base.coordinator.rerank_spec(), &hits, &response);
    if turbo_rerank(&base, &base.req.request_id, &sources, &mut hits).await? {
        route = "search:turbo_rerank";
    }
    let mut answer = match answer_pool(&mut run, hits, route, response.request_id).await? {
        Answer::Deepen(depth) => return Ok(Routed::Deepen(depth)),
        Answer::Paged(response) => *response,
    };
    report_dense(run, &mut answer);
    Ok(Routed::Done(Box::new(answer)))
}

fn explain_native_hits(explain: bool, id: &str, hits: &mut [QueryHit]) {
    if explain {
        for hit in hits {
            hit.explain = Some(crate::explain::dense(id, hit.score, None));
        }
    }
}

fn record_selection_profile(
    run: &mut Run<'_>,
    started: std::time::Instant,
    response: &SearchResponse,
    exec: &DenseScanExecution,
    exact_fp32: bool,
) {
    let Some(p) = run.prof.as_mut() else {
        return;
    };
    p.selection_ms = ms(started);
    p.segments_total = response.segments_total;
    p.segments_skipped = response.segments_skipped;
    if exact_fp32 {
        p.rerank_ms = p.selection_ms;
        p.rerank_rows = exec.evidence.fp32_rows_scored;
        p.rerank_logical_bytes = exec.fp32_logical_bytes;
        p.rerank_pages = exec.fp32_pages_touched;
        p.rerank_tasks = exec.fp32_tasks;
    }
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
        filter_strategy: knobs.filter_strategy,
        enumerate_all: knobs.enumerate_all,
        staged_selection: knobs.staged_selection,
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

/// Validate what the shards reported against the plan and stamp it.
fn stamp_receipt(
    run: &mut Run<'_>,
    mut evidence: crate::dense_execution::Evidence,
) -> Result<(), Status> {
    let (Some(outcome), Some(plan)) = (run.dense.execution.as_mut(), run.dense.plan.as_ref())
    else {
        return Ok(());
    };
    evidence.rows = run.dense.rows;
    crate::dense_execution::stamp(plan, &evidence, outcome).map_err(dense_status)
}

fn rerank_sources(
    spec: Option<&crate::turbo_bridge::rerank::RerankSpec>,
    hits: &[QueryHit],
    response: &SearchResponse,
) -> RerankSources {
    let mut sources = RerankSources::for_shortlist(spec, hits);
    if !sources.is_enabled() {
        return sources;
    }
    for h in &response.hits {
        sources.note_candidate(h.vector_id, h.rerank_candidate.as_ref());
        if sources.has_all_inline() {
            break;
        }
    }
    sources
}

/// Rescore the candidate pool against the stored FP32 vectors and
/// reorder it by the exact score, ties by doc id. Every candidate must
/// resolve an exact score.
async fn fp32_rescore(
    run: &mut Run<'_>,
    id: &str,
    query: &DenseQuery,
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
                "FP32 candidate {} has no score from its original row",
                hit.doc_id
            ))
        })?;
        if run.base.req.explain {
            hit.explain = Some(crate::explain::dense(id, score, Some(hit.score)));
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
/// the FP32 pool depth, and the execution outcome.
fn report_dense(run: Run<'_>, response: &mut QueryResponse) {
    let mut dense = run.dense;
    if dense.quality_default {
        if let (Some(outcome), Some(resolution)) =
            (dense.execution.as_mut(), dense.quality.as_ref())
        {
            outcome.planner_reason.push_str(&format!(
                "; FP32 pool depth selection_k={} resolved through quality profile {:?} \
                 default_target_recall_ppm={}",
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
    response.dense_execution = dense.execution;
}

//! Execution of a recursive `BooleanQuery` root: the stats round, the
//! shard fan-out, and the pool phases above the exact match set.

use tonic::Status;

use super::boolean_compile::{BooleanLeafMeta, CompiledBoolean};
use super::boolean_request::{validate, BooleanRequest};
use super::boosts::{apply_boosts, window_boosts};
use super::cursor::page;
use super::response::{apply_scorer, done, fill_projected, finish_prof, ms, scored_hit};
use crate::coordinator::{BooleanFanout, BooleanFanoutPlan, CoordinatorServiceImpl};
use crate::pb::{
    BooleanCandidate, BooleanPlanLeaf, BooleanQuery, QueryHit, QueryRequest, QueryResponse,
    QuerySignal,
};
use crate::stats_identity::StatsClaim;

pub(super) async fn execute_recursive_boolean(
    coordinator: &CoordinatorServiceImpl,
    req: &QueryRequest,
    boolean: &BooleanQuery,
) -> Result<QueryResponse, Status> {
    let t_total = std::time::Instant::now();
    let request = validate(coordinator, req, boolean)?;
    let t_selection = std::time::Instant::now();
    let compiled = CompiledBoolean::compile(coordinator, boolean).await?;
    let fanout = evaluate(coordinator, &compiled, &request).await?;
    let mut hits = boolean_hits(fanout.candidates, &compiled.meta);
    if req.explain {
        for hit in &mut hits {
            hit.explain = Some(crate::explain::boolean(hit));
        }
    }
    let mut profile: Option<crate::pb::QueryProfile> = req.profile.then(Default::default);
    if let Some(profile) = profile.as_mut() {
        profile.selection_ms = ms(t_selection);
        profile.segments_total = fanout.prune.segments_total;
        profile.segments_skipped = fanout.prune.segments_skipped;
        profile.shards_total = fanout.shards_total;
        profile.shards_skipped = fanout.shards_skipped;
    }
    let BooleanRequest {
        k,
        cursor,
        scorer,
        boosts,
        ..
    } = request;
    let scored = scorer.is_some();
    apply_boosts(coordinator, &boosts, &mut hits, scored, &mut profile).await?;
    let executed = apply_scorer(
        coordinator,
        &scorer,
        &mut hits,
        "boolean:bitmap",
        &mut profile,
    )
    .await?;
    if req.explain {
        let scorer_name: Option<String> = scorer.as_ref().map(|s| s.executed_suffix());
        crate::explain::finish(&mut hits, &window_boosts(&boosts), scorer_name.as_deref())?;
    }
    let (mut hits, next_cursor) = page(hits, k, cursor.as_ref())?;
    let compiled_projections = crate::coordinator::compile_projections(&req.projections)?;
    fill_projected(coordinator, &compiled_projections, &mut hits, &mut profile).await?;
    let mut response = done(
        req.request_id.clone(),
        hits,
        &executed,
        next_cursor,
        finish_prof(profile, t_total),
    );
    response.aggregate = fanout.aggregate;
    Ok(response)
}

/// Stats and evaluation run as a round: a stale-stats refusal reruns
/// them once with fresh stats and their complete physical claim.
async fn evaluate(
    coordinator: &CoordinatorServiceImpl,
    compiled: &CompiledBoolean,
    request: &BooleanRequest<'_>,
) -> Result<BooleanFanout, Status> {
    let positive_lexical: Vec<usize> = compiled
        .meta
        .iter()
        .enumerate()
        .filter(|(_, m)| m.positive && m.terms.is_some())
        .map(|(i, _)| i)
        .collect();
    let mut fresh = false;
    loop {
        let (leaves, claims) = boolean_stats_round(coordinator, compiled, &mut fresh).await?;
        let plan = BooleanFanoutPlan {
            root: compiled.root.clone(),
            leaves,
            filters: compiled.meta.iter().map(|m| m.filters.clone()).collect(),
            root_must_filters: compiled.root_must_filters.clone(),
            positive_lexical: positive_lexical.clone(),
            depth: request.depth,
            aggregate: request.aggregate.clone(),
        };
        match coordinator.evaluate_boolean_fanout(&plan, &claims).await {
            Err(status) if !fresh && crate::coordinator::is_stale_stats(&status) => {
                coordinator.invalidate_stats();
                fresh = true;
            }
            Err(status) => return Err(status),
            Ok(fanout) => return Ok(fanout),
        }
    }
}

/// The stats round: every lexical leaf's global statistics under one
/// physical reading, and the complete per-shard claims the shards enforce
/// on every attempt, including a fresh refetch. Two leaves that read different epochs for
/// one shard saw a store move between them; the round refetches fresh
/// once, the retry contract every stats consumer follows.
async fn boolean_stats_round(
    coordinator: &CoordinatorServiceImpl,
    compiled: &CompiledBoolean,
    fresh: &mut bool,
) -> Result<(Vec<BooleanPlanLeaf>, Vec<StatsClaim>), Status> {
    loop {
        let mut leaves = compiled.leaves.clone();
        let mut claims: Option<Vec<StatsClaim>> = None;
        let mut moved = false;
        for (leaf, meta) in leaves.iter_mut().zip(&compiled.meta) {
            let Some(terms) = meta.terms.as_ref() else {
                continue;
            };
            let Some(crate::pb::boolean_plan_leaf::Leaf::Lexical(lexical)) = leaf.leaf.as_mut()
            else {
                continue;
            };
            if terms.is_empty() {
                continue;
            }
            let (global, epochs) = coordinator.body_stats(terms, *fresh).await?;
            lexical.global_doc_count = global.doc_count;
            lexical.global_total_doc_length = global.total_doc_length;
            lexical.global_doc_frequencies = global.dfs.clone();
            match claims.as_ref() {
                None => claims = Some(epochs),
                Some(seen) if *seen != epochs => moved = true,
                Some(_) => {}
            }
        }
        if moved && !*fresh {
            coordinator.invalidate_stats();
            *fresh = true;
            continue;
        }
        if moved {
            return Err(tonic::Status::failed_precondition(
                "Boolean statistics changed during the retry",
            ));
        }
        let claims = claims.unwrap_or(coordinator.admitted_read_claims()?);
        return Ok((leaves, claims));
    }
}

/// The shards' candidates as hits: each leaf's score under the request
/// id of the clause it compiled from, and the positive clauses matched.
fn boolean_hits(candidates: Vec<BooleanCandidate>, meta: &[BooleanLeafMeta]) -> Vec<QueryHit> {
    candidates
        .into_iter()
        .map(|candidate| {
            let signals = candidate
                .signals
                .iter()
                .map(|signal| QuerySignal {
                    id: meta[signal.leaf as usize].id.clone(),
                    score: signal.score,
                })
                .collect();
            let matched = candidate
                .matched
                .iter()
                .map(|&leaf| &meta[leaf as usize])
                .filter(|meta| meta.positive)
                .map(|meta| meta.id.clone())
                .collect();
            scored_hit(candidate.doc_id, candidate.score, signals, matched)
        })
        .collect()
}

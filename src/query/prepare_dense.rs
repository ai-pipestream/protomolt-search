//! The dense leaf's execution plan: the mode AUTO resolves to, the
//! policy point's knobs, and the quality profile that fixes the depth
//! of an FP32 rerank.

use tonic::Status;

use super::refuse;
use super::run::Base;
use super::selection::{dense_execution_mode, dense_score_mode, Shape};
use crate::coordinator::{DenseRequestKey, RequestFilters};
use crate::pb::{
    DenseExecutionMode, DenseExecutionOutcome, DensePolicyPoint, DenseQuery, DenseScoreMode,
};
use crate::turbo_bridge::query_plan::{
    select_filtered_ann_strategy, BoundOnePassQuery, FilteredAnnStrategy,
};

/// The shard-side knobs a policy point fixes for the traversal; all
/// unset without one.
#[derive(Clone, Copy, Default)]
pub(super) struct PolicyKnobs {
    /// The enumerate threshold the shards answer the filter under.
    pub(super) enumerate_max_rows: Option<u32>,
    pub(super) patience_saturation_ppm: Option<u32>,
    pub(super) patience_hops: Option<u32>,
    pub(super) finish_settle_us: Option<u32>,
    pub(super) floor_prune: Option<bool>,
    pub(super) floor_greediness_ppm: Option<u32>,
    pub(super) floor_clamp_min: Option<u32>,
}

impl PolicyKnobs {
    fn from_point(point: Option<&DensePolicyPoint>) -> Self {
        let Some(point) = point else {
            return Self::default();
        };
        PolicyKnobs {
            enumerate_max_rows: point.enumerate_max_rows,
            patience_saturation_ppm: point.patience_saturation_ppm,
            patience_hops: point.patience_hops,
            finish_settle_us: point.finish_settle_us,
            floor_prune: point.floor_greediness_ppm.map(|_| true),
            floor_greediness_ppm: point.floor_greediness_ppm,
            floor_clamp_min: point.floor_clamp_min,
        }
    }
}

/// How the selection's dense leaf executes. Everything is absent or
/// false on a selection without one.
pub(super) struct DensePlan {
    pub(super) execution: Option<DenseExecutionOutcome>,
    /// A policy point fixes the candidate depth AUTO runs at; it is the
    /// depth that was measured, reported back, and never widened.
    pub(super) policy_depth: Option<u32>,
    pub(super) knobs: PolicyKnobs,
    /// Whether the single dense leaf rescores its pool against the
    /// stored FP32 vectors.
    pub(super) fp32_rerank: bool,
    /// Whether AUTO resolved the FP32 rerank depth through the quality
    /// profile's default target.
    pub(super) auto_default_depth: bool,
    pub(super) quality: Option<crate::quality::DenseQualityResolution>,
}

/// Resolve the dense leaf's execution mode and the policy knobs it runs
/// under. With a one-pass filter of known selectivity and no policy
/// point, an ANN traversal picks its filtered strategy here.
pub(super) async fn resolve_dense(
    base: &Base<'_>,
    filter: &str,
    one_pass: Option<&BoundOnePassQuery>,
) -> Result<DensePlan, Status> {
    let req = base.req;
    let mut execution = match base.plan.dense_query() {
        Some(query) => Some(resolve_execution(base, query, filter).await?),
        None => None,
    };
    let mut policy_depth: Option<u32> = execution
        .as_ref()
        .filter(|o| o.resolved_mode == DenseExecutionMode::Ann as i32 && o.policy_point.is_some())
        .map(|o| o.candidate_depth);
    let mut knobs =
        PolicyKnobs::from_point(execution.as_ref().and_then(|o| o.policy_point.as_ref()));
    if let (Some(outcome), Some(bound)) = (execution.as_mut(), one_pass) {
        if outcome.resolved_mode == DenseExecutionMode::Ann as i32
            && outcome.policy_point.is_none()
            && bound.estimated_selectivity < 1.0
        {
            match select_filtered_ann_strategy(
                outcome.candidate_depth.max(req.k),
                req.k,
                50_000,
                bound.estimated_selectivity,
            ) {
                FilteredAnnStrategy::PrefilteredExact => {
                    knobs.enumerate_max_rows = Some(req.k.saturating_mul(8).max(256));
                }
                FilteredAnnStrategy::Graph { ef_search } => {
                    policy_depth = Some(ef_search);
                    outcome.candidate_depth = ef_search;
                }
            }
        }
    }
    Ok(DensePlan {
        execution,
        policy_depth,
        knobs,
        fp32_rerank: false,
        auto_default_depth: false,
        quality: None,
    })
}

/// The policy key AUTO is judged on: k as sent, the named candidate
/// depth, and the request's filters (their live selectivity is measured
/// only when a policy is consulted).
async fn resolve_execution(
    base: &Base<'_>,
    query: &DenseQuery,
    filter: &str,
) -> Result<DenseExecutionOutcome, Status> {
    let requested = dense_execution_mode(query)?;
    let geo_filters = &base.plan.geo_filters;
    let filters = (!geo_filters.is_empty() || !filter.is_empty())
        .then(|| RequestFilters::compile(geo_filters, filter))
        .transpose()?;
    base.coordinator
        .resolve_dense_execution(
            requested,
            query.vector.len(),
            DenseRequestKey {
                k: base.req.k,
                candidate_depth: base.req.selection_k,
                filters: filters.as_ref(),
            },
        )
        .await
}

/// Whether the selection is a single dense leaf under FP32 rerank; the
/// quality policy and the rerank mode are refused everywhere else.
pub(super) fn fp32_rerank(shape: &Shape<'_>) -> Result<bool, Status> {
    match shape {
        Shape::Dense { query, .. } => {
            let mode = dense_score_mode(query)?;
            if query.quality.is_some() && mode != DenseScoreMode::Fp32Rerank {
                return Err(refuse(
                    "DenseQualityPolicy is served only with DENSE_SCORE_MODE_FP32_RERANK",
                ));
            }
            Ok(mode == DenseScoreMode::Fp32Rerank)
        }
        Shape::Composite { dense, .. } => {
            if dense.quality.is_some() {
                return Err(refuse(
                    "DenseQualityPolicy is currently served on a single dense selection only",
                ));
            }
            if dense_score_mode(dense)? == DenseScoreMode::Fp32Rerank {
                return Err(refuse(
                    "FP32 rerank is currently served on a single dense selection only; \
                     composite fusion still consumes provider-native dense scores",
                ));
            }
            Ok(false)
        }
        _ => Ok(false),
    }
}

/// Resolve the depth authority of a single dense leaf's FP32 rerank.
///
/// AUTO + FP32 rerank with neither a policy nor a selection_k names no
/// depth; running at selection_k = k would silently serve the raw
/// quantized top-k (0.544 recall@10 on the challenge fixture). AUTO on
/// an exhaustive provider therefore resolves the depth through the
/// installed profile's default target, exactly as an explicit policy
/// would; AUTO through an ANN policy already fixed its depth and is
/// left alone. EXACT/UNSPECIFIED keep the pool at k: that is the
/// caller's explicit choice.
pub(super) async fn resolve_quality(base: &Base<'_>, dense: &mut DensePlan) -> Result<(), Status> {
    let Shape::Dense { query, .. } = &base.plan.shape else {
        return Ok(());
    };
    let (coordinator, req) = (base.coordinator, base.req);
    dense.auto_default_depth = dense.fp32_rerank
        && query.quality.is_none()
        && req.selection_k == 0
        && dense_execution_mode(query)? == DenseExecutionMode::Auto
        && dense
            .execution
            .as_ref()
            .is_some_and(|o| o.resolved_mode == DenseExecutionMode::Exact as i32);
    dense.quality = match &query.quality {
        Some(policy) => {
            if req.selection_k != 0 {
                return Err(refuse(
                    "DenseQualityPolicy and selection_k are competing depth authorities; leave selection_k zero",
                ));
            }
            Some(
                coordinator
                    .resolve_dense_quality(req.k, query.vector.len(), policy)
                    .await?,
            )
        }
        None if dense.auto_default_depth => Some(
            coordinator
                .resolve_dense_quality_default(req.k, query.vector.len())
                .await?,
        ),
        None => None,
    };
    Ok(())
}

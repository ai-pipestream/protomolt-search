//! The dense leaf's execution plan: the validated contract the request
//! resolved to, the shard controls its preset fixes, and the quality
//! profile that fixes the depth of an FP32 candidate pool.

use tonic::Status;

use super::refuse;
use super::run::Base;
use super::selection::Shape;
use crate::dense_execution::{ResolvedDensePlan, Route};
use crate::pb::DenseExecutionOutcome;

/// The shard-side knobs a plan fixes for the traversal; all unset for an
/// exhaustive plan.
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
    /// `DenseFilterStrategy` and whole-set enumeration for the shards.
    pub(super) filter_strategy: i32,
    pub(super) enumerate_all: bool,
}

impl PolicyKnobs {
    fn from_plan(plan: &ResolvedDensePlan) -> Self {
        let patience = plan.preset.and_then(|preset| preset.patience);
        PolicyKnobs {
            patience_saturation_ppm: patience.map(|(ppm, _)| ppm),
            patience_hops: patience.map(|(_, hops)| hops),
            filter_strategy: plan.filter_strategy as i32,
            enumerate_all: plan.route == Route::EnumerateAll,
            ..Default::default()
        }
    }
}

/// How the selection's dense leaf executes. Everything is absent or
/// false on a selection without one.
pub(super) struct DensePlan {
    pub(super) execution: Option<DenseExecutionOutcome>,
    pub(super) plan: Option<ResolvedDensePlan>,
    /// Physical rows across the fleet, the depth an FP32 row scan fetches.
    pub(super) rows: u64,
    pub(super) knobs: PolicyKnobs,
    /// Whether the single dense leaf rescores a quantized candidate pool
    /// in FP32.
    pub(super) fp32_pool: bool,
    pub(super) quality: Option<crate::quality::DenseQualityResolution>,
    /// Whether the profile's default target, not a request policy, sized
    /// the FP32 pool.
    pub(super) quality_default: bool,
}

impl DensePlan {
    pub(super) fn route(&self) -> Option<Route> {
        self.plan.as_ref().map(|plan| plan.route)
    }
}

/// Resolve the dense leaf's execution plan against the live provider.
pub(super) async fn resolve_dense(base: &Base<'_>) -> Result<DensePlan, Status> {
    let Some(query) = base.plan.dense_query() else {
        return Ok(DensePlan {
            execution: None,
            plan: None,
            rows: 0,
            knobs: PolicyKnobs::default(),
            fp32_pool: false,
            quality: None,
            quality_default: false,
        });
    };
    let request =
        crate::dense_execution::parse_request(query).map_err(crate::coordinator::dense_status)?;
    let planned = base
        .coordinator
        .resolve_dense_execution(request, query.vector.len())
        .await?;
    Ok(DensePlan {
        knobs: PolicyKnobs::from_plan(&planned.plan),
        execution: Some(planned.outcome),
        rows: planned.rows,
        fp32_pool: false,
        plan: Some(planned.plan),
        quality: None,
        quality_default: false,
    })
}

/// Whether the selection is a single dense leaf rescoring an FP32 pool;
/// FP32 precision on a quantized provider and the quality policy are
/// refused everywhere else.
pub(super) fn fp32_pool(shape: &Shape<'_>, dense: &DensePlan) -> Result<bool, Status> {
    let route = dense.route();
    match shape {
        Shape::Dense { query, .. } => {
            let pooled = route == Some(Route::Fp32Pool);
            if query.quality.is_some() && !pooled {
                return Err(refuse(
                    "DenseQualityPolicy sizes the FP32 candidate pool of a BALANCED \
                     selection on a quantized provider; this plan has no such pool",
                ));
            }
            Ok(pooled)
        }
        Shape::Composite { dense: query, .. } => {
            if query.quality.is_some() {
                return Err(refuse(
                    "DenseQualityPolicy is currently served on a single dense selection only",
                ));
            }
            if matches!(route, Some(Route::Fp32Pool | Route::Fp32AllRows)) {
                return Err(refuse(
                    "FP32 precision on a quantized provider is served on a single dense \
                     selection only; composite fusion consumes provider-native dense scores",
                ));
            }
            Ok(false)
        }
        _ => Ok(false),
    }
}

/// Resolve the depth authority of a single dense leaf's FP32 pool: an
/// explicit quality policy, the caller's selection_k, or the installed
/// profile's default target. Without any of them the pool stays at k.
pub(super) async fn resolve_quality(base: &Base<'_>, dense: &mut DensePlan) -> Result<(), Status> {
    let Shape::Dense { query, .. } = &base.plan.shape else {
        return Ok(());
    };
    let (coordinator, req) = (base.coordinator, base.req);
    dense.quality = match &query.quality {
        Some(_) if req.selection_k != 0 => {
            return Err(refuse(
                "DenseQualityPolicy and selection_k are competing depth authorities; leave selection_k zero",
            ))
        }
        Some(policy) => Some(
            coordinator
                .resolve_dense_quality(req.k, query.vector.len(), policy)
                .await?,
        ),
        None if dense.fp32_pool && req.selection_k == 0 => {
            let resolved = coordinator
                .resolve_dense_quality_default(req.k, query.vector.len())
                .await?;
            dense.quality_default = resolved.is_some();
            resolved
        }
        None => None,
    };
    Ok(())
}

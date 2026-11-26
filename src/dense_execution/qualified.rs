//! AUTO through measured evidence: the technique a qualifying policy point
//! measured, served under the controls it was measured with.

use super::plan::{
    bind, Choice, DenseExecutionError, DenseRequest, ProviderCaps, ResolvedDensePlan, Route,
};
use crate::pb::{
    DenseExecutionMode as Mode, DenseFilterStrategy as Strategy, DensePolicyPoint,
    DenseScorePrecision as Precision, DenseTechnique,
};

/// The live provider must serve every regime the point was measured under;
/// evidence never transfers to a technique it did not measure.
fn check_capabilities(
    point: &DensePolicyPoint,
    caps: &ProviderCaps,
) -> Result<(), DenseExecutionError> {
    let refuse = |what: &str| {
        Err(DenseExecutionError::FailedPrecondition(format!(
            "the qualifying policy point k={} depth {} was measured with {what}, which \
             provider {} cannot serve",
            point.k, point.candidates, caps.provider
        )))
    };
    match DenseTechnique::try_from(point.technique) {
        Ok(DenseTechnique::HnswWalk) if !caps.walks => return refuse("a graph walk"),
        Ok(DenseTechnique::BitplaneStaged) if !caps.staged => {
            return refuse("staged bit-plane selection")
        }
        _ => {}
    }
    if point.enumerate_max_rows.is_some() && !caps.enumerates {
        return refuse("an enumerate threshold");
    }
    if point.patience_hops.is_some() && !caps.patience {
        return refuse("patience early stopping");
    }
    if point.finish_settle_us.is_some() && !caps.candidate_stream {
        return refuse("candidate streaming");
    }
    if (point.floor_greediness_ppm.is_some() || point.floor_clamp_min.is_some())
        && !caps.floor_pruning
    {
        return refuse("shared-floor pruning");
    }
    if point.selection_precision != caps.native as i32 {
        return refuse("another selection precision");
    }
    if point.output_precision == Precision::Fp32 as i32
        && caps.native == Precision::Quantized
        && !caps.walks
        && !caps.fp32_sidecar
    {
        return refuse("shard-local FP32 sidecar rescoring");
    }
    Ok(())
}

/// The plan a qualifying point admits for an AUTO request.
pub(crate) fn qualified(
    request: DenseRequest,
    caps: &ProviderCaps,
    point: &DensePolicyPoint,
) -> Result<ResolvedDensePlan, DenseExecutionError> {
    let staged = point.technique == DenseTechnique::BitplaneStaged as i32;
    let strategy = Strategy::try_from(point.filter_strategy).unwrap_or(Strategy::Default);
    if staged
        && (strategy != Strategy::Default
            || point.enumerate_max_rows.is_some()
            || point.patience_hops.is_some()
            || point.finish_settle_us.is_some()
            || point.floor_greediness_ppm.is_some()
            || point.floor_clamp_min.is_some())
    {
        return Err(DenseExecutionError::InvalidRequest(
            "BITPLANE_STAGED policy point must use DEFAULT filter_strategy without graph controls"
                .to_string(),
        ));
    }
    if point.shape == crate::pb::DenseExecutionShape::Unary as i32
        && point.finish_settle_us.is_some()
    {
        return Err(DenseExecutionError::InvalidRequest(
            "UNARY policy point must not set finish_settle_us".to_string(),
        ));
    }
    check_capabilities(point, caps)?;
    let mode = Mode::try_from(point.mode).unwrap_or(Mode::Balanced);
    let output = Precision::try_from(point.output_precision).unwrap_or(caps.native);
    let selection = caps.native;
    let route = match (output == selection, staged) {
        (false, _) => Route::Fp32Pool,
        (true, true) => Route::Staged,
        (true, false) => Route::Walk,
    };
    let mut choice = Choice::new(
        mode,
        route,
        selection,
        &format!(
            "AUTO selected {} through a measured policy point (k={} depth {}, recall {} \
             ppm)",
            mode.as_str_name(),
            point.k,
            point.candidates,
            point.measured_recall_ppm
        ),
    );
    choice.pool = if staged {
        DenseTechnique::BitplaneStaged
    } else {
        DenseTechnique::HnswWalk
    };
    Ok(bind(request, output, strategy, choice, true))
}

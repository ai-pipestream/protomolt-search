//! Validation of raw dense controls on direct `Search` and `HybridSearch` RPCs.

use super::plan::{DenseExecutionError, ProviderCaps};
use crate::pb::DenseFilterStrategy as Strategy;

/// Raw dense execution controls carried on internal/direct `Search` and
/// `HybridSearch` requests.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct RawDenseControls {
    pub(crate) filter_strategy: i32,
    pub(crate) enumerate_all: bool,
    pub(crate) staged_selection: bool,
    pub(crate) enumerate_max_rows: Option<u32>,
    pub(crate) patience_saturation_ppm: Option<u32>,
    pub(crate) patience_hops: Option<u32>,
    pub(crate) finish_settle_us: Option<u32>,
    pub(crate) finish_deadline_us: Option<u32>,
    pub(crate) floor_prune: Option<bool>,
    pub(crate) floor_greediness_ppm: Option<u32>,
    pub(crate) floor_clamp_min: Option<u32>,
}

impl RawDenseControls {
    pub(crate) fn has_any(&self) -> bool {
        self.filter_strategy != 0
            || self.enumerate_all
            || self.staged_selection
            || self.enumerate_max_rows.is_some()
            || self.patience_saturation_ppm.is_some()
            || self.patience_hops.is_some()
            || self.finish_settle_us.is_some()
            || self.finish_deadline_us.is_some()
            || self.floor_prune.is_some()
            || self.floor_greediness_ppm.is_some()
            || self.floor_clamp_min.is_some()
    }
}

/// Validate raw dense controls on direct `Search` / `HybridSearch` calls and
/// check peer capabilities when `caps` is supplied.
pub(crate) fn validate_raw_controls(
    raw: &RawDenseControls,
    caps: Option<&ProviderCaps>,
) -> Result<(), DenseExecutionError> {
    let strategy = Strategy::try_from(raw.filter_strategy).map_err(|_| {
        DenseExecutionError::InvalidRequest(format!(
            "unknown dense filter_strategy {}",
            raw.filter_strategy
        ))
    })?;
    if raw.patience_saturation_ppm.is_some() != raw.patience_hops.is_some() {
        return Err(DenseExecutionError::InvalidRequest(
            "patience_saturation_ppm and patience_hops must be set together".to_string(),
        ));
    }
    if raw.floor_prune == Some(false)
        && (raw.floor_greediness_ppm.is_some() || raw.floor_clamp_min.is_some())
    {
        return Err(DenseExecutionError::InvalidRequest(
            "floor_greediness_ppm and floor_clamp_min require floor_prune = true".to_string(),
        ));
    }
    let approx_controls = strategy != Strategy::Default
        || raw.enumerate_max_rows.is_some()
        || raw.patience_saturation_ppm.is_some()
        || raw.finish_settle_us.is_some()
        || raw.finish_deadline_us.is_some()
        || raw.floor_prune == Some(true)
        || raw.floor_greediness_ppm.is_some()
        || raw.floor_clamp_min.is_some();
    if raw.enumerate_all && (approx_controls || raw.staged_selection) {
        return Err(DenseExecutionError::InvalidRequest(
            "enumerate_all pins exhaustive enumeration and cannot be combined with \
             staged_selection, filter_strategy, enumerate_max_rows, patience, floor pruning, \
             or early-finish controls"
                .to_string(),
        ));
    }
    if raw.staged_selection
        && (strategy != Strategy::Default
            || raw.enumerate_max_rows.is_some()
            || raw.patience_saturation_ppm.is_some()
            || raw.finish_settle_us.is_some()
            || raw.finish_deadline_us.is_some())
    {
        return Err(DenseExecutionError::InvalidRequest(
            "staged_selection is unary bit-plane selection and cannot be combined with \
             filter_strategy, enumerate_max_rows, patience, or early-finish controls"
                .to_string(),
        ));
    }
    if let Some(caps) = caps {
        check_raw_caps(raw, strategy, caps)?;
    }
    Ok(())
}

fn check_raw_caps(
    raw: &RawDenseControls,
    strategy: Strategy,
    caps: &ProviderCaps,
) -> Result<(), DenseExecutionError> {
    if raw.staged_selection && !caps.staged {
        return Err(DenseExecutionError::FailedPrecondition(format!(
            "provider {} cannot serve staged_selection (staged_top_k not available on unary path)",
            caps.provider
        )));
    }
    if raw.enumerate_all && !caps.enumerates {
        return Err(DenseExecutionError::FailedPrecondition(format!(
            "provider {} cannot pin enumeration of every admitted row (mask_enumeration not advertised)",
            caps.provider
        )));
    }
    if raw.enumerate_max_rows.is_some() && !caps.enumerates {
        return Err(DenseExecutionError::FailedPrecondition(format!(
            "provider {} has no enumerate regime for enumerate_max_rows (mask_enumeration not advertised)",
            caps.provider
        )));
    }
    if strategy != Strategy::Default && !caps.walks {
        return Err(DenseExecutionError::FailedPrecondition(format!(
            "filter_strategy {} requires an approximate graph provider; provider {} does not walk",
            strategy.as_str_name(),
            caps.provider
        )));
    }
    if strategy == Strategy::EnumerateSmall && !caps.enumerates {
        return Err(DenseExecutionError::FailedPrecondition(format!(
            "filter_strategy ENUMERATE_SMALL requires mask_enumeration; provider {} does not advertise it",
            caps.provider
        )));
    }
    if raw.patience_saturation_ppm.is_some() && !caps.patience {
        return Err(DenseExecutionError::FailedPrecondition(format!(
            "provider {} does not advertise patience_early_stop",
            caps.provider
        )));
    }
    let floor_requested = raw.floor_prune == Some(true)
        || raw.floor_greediness_ppm.is_some()
        || raw.floor_clamp_min.is_some();
    if floor_requested && !caps.floor_pruning {
        return Err(DenseExecutionError::FailedPrecondition(format!(
            "provider {} does not advertise floor_pruning",
            caps.provider
        )));
    }
    if (raw.finish_settle_us.is_some() || raw.finish_deadline_us.is_some())
        && !(caps.walks && caps.candidate_stream)
    {
        return Err(DenseExecutionError::FailedPrecondition(format!(
            "early stream finish controls require an approximate provider with candidate_stream; \
             provider {} cannot honor them",
            caps.provider
        )));
    }
    Ok(())
}

//! Validation of the vector search options a request may carry.

use super::*;

/// An enumerate threshold names a filter regime, so only a provider that
/// has one may be handed it: anything else would serve the request under a
/// rule its policy point was not measured with.
pub(in crate::node) fn check_enumerate_threshold(
    index: &VectorIndex,
    rows: Option<u32>,
) -> Result<(), Status> {
    let Some(rows) = rows else {
        return Ok(());
    };
    let descriptor = index.descriptor();
    if descriptor
        .capabilities
        .contains(&crate::vector::VectorCapability::MaskEnumeration)
    {
        return Ok(());
    }
    Err(Status::failed_precondition(format!(
        "vector backend {:?} has no enumerate regime for the request's threshold of {rows} \
         rows (it does not advertise mask_enumeration)",
        descriptor.backend_kind
    )))
}

/// The plan's selection controls. Whole-set enumeration on a provider
/// without an enumerate regime is refused: its own scan already scores
/// every row, and only a regime provider can be pinned to.
pub(in crate::node) fn selection_controls(
    index: &VectorIndex,
    strategy: i32,
    enumerate_all: bool,
    staged: bool,
) -> Result<crate::vector::SelectionControls, Status> {
    use crate::pb::DenseFilterStrategy as Strategy;
    let filter_routing = match Strategy::try_from(strategy) {
        Ok(Strategy::Default | Strategy::Speed) => crate::vector::FilterRouting::Speed,
        Ok(Strategy::EnumerateSmall) => crate::vector::FilterRouting::Exact,
        Err(_) => {
            return Err(Status::invalid_argument(format!(
                "unknown dense filter_strategy {strategy}"
            )))
        }
    };
    if enumerate_all
        && !index
            .descriptor()
            .capabilities
            .contains(&crate::vector::VectorCapability::MaskEnumeration)
    {
        return Err(Status::failed_precondition(format!(
            "vector backend {:?} cannot pin enumeration of every admitted row (it does \
             not advertise mask_enumeration)",
            index.descriptor().backend_kind
        )));
    }
    let capabilities = index.descriptor().capabilities;
    if staged && !capabilities.contains(&crate::vector::VectorCapability::StagedTopK) {
        return Err(Status::failed_precondition(format!(
            "vector backend {:?} cannot select by staged top-k on this generation (it does \
             not advertise staged_top_k)",
            index.descriptor().backend_kind
        )));
    }
    Ok(crate::vector::SelectionControls {
        filter_routing,
        enumerate_all,
        staged,
    })
}

pub(in crate::node) fn check_patience_options(
    index: &VectorIndex,
    saturation_ppm: Option<u32>,
    hops: Option<u32>,
) -> Result<Option<crate::vector::PatienceSpec>, Status> {
    let (ppm, hops) = match (saturation_ppm, hops) {
        (None, None) => return Ok(None),
        (Some(ppm), Some(hops)) => (ppm, hops),
        _ => {
            return Err(Status::invalid_argument(
                "patience_saturation_ppm and patience_hops must be set together",
            ));
        }
    };
    let descriptor = index.descriptor();
    if !descriptor
        .capabilities
        .contains(&crate::vector::VectorCapability::PatienceEarlyStop)
    {
        return Err(Status::failed_precondition(format!(
            "vector backend {:?} does not support patience early stopping (it does not advertise \
             patience_early_stop)",
            descriptor.backend_kind
        )));
    }
    if !(1..=1_000_000).contains(&ppm) {
        return Err(Status::invalid_argument(format!(
            "patience_saturation_ppm {ppm} must be in 1..=1000000"
        )));
    }
    if hops == 0 {
        return Err(Status::invalid_argument(
            "patience_hops must be positive when patience_saturation_ppm is set",
        ));
    }
    Ok(Some(crate::vector::PatienceSpec::from_ppm(ppm, hops)))
}

pub(in crate::node) fn check_floor_prune_options(
    index: &VectorIndex,
    floor_prune: Option<bool>,
    floor_k: Option<u32>,
    default_k: u32,
    floor_share_ppm: Option<u32>,
    floor_greediness_ppm: Option<u32>,
    floor_clamp_min: Option<u32>,
) -> Result<Option<crate::vector::FloorPruneSpec>, Status> {
    if floor_prune == Some(false) && (floor_greediness_ppm.is_some() || floor_clamp_min.is_some()) {
        return Err(Status::invalid_argument(
            "floor_greediness_ppm and floor_clamp_min require floor_prune = true",
        ));
    }
    let requested = floor_prune == Some(true)
        || (floor_prune.is_none() && (floor_greediness_ppm.is_some() || floor_clamp_min.is_some()));
    if let Some(share) = floor_share_ppm {
        if share > 1_000_000 {
            return Err(Status::invalid_argument(format!(
                "floor_share_ppm {share} must be in 0..=1000000"
            )));
        }
    }
    if !requested {
        // When `floor_k` and `floor_share_ppm` are supplied without enabling
        // floor pruning, carry them with `enabled = false` on graph providers
        // so patience early-stop still enforces the shared-floor ascent gate.
        if let Some(k) = floor_k.filter(|&k| k > 0) {
            let descriptor = index.descriptor();
            if descriptor
                .capabilities
                .contains(&crate::vector::VectorCapability::PatienceEarlyStop)
            {
                return Ok(Some(crate::vector::FloorPruneSpec {
                    enabled: false,
                    floor_k: k as usize,
                    share_ppm: floor_share_ppm.unwrap_or(1_000_000),
                    greediness_ppm: 0,
                    clamp_min: crate::vector::DEFAULT_FLOOR_CLAMP_MIN as usize,
                }));
            }
        }
        return Ok(None);
    }
    let descriptor = index.descriptor();
    if !descriptor
        .capabilities
        .contains(&crate::vector::VectorCapability::FloorPruning)
    {
        return Err(Status::failed_precondition(format!(
            "vector backend {:?} does not support graph floor pruning (it does not advertise \
             floor_pruning)",
            descriptor.backend_kind
        )));
    }
    let greediness_ppm = floor_greediness_ppm.unwrap_or(0);
    if greediness_ppm > 1_000_000 {
        return Err(Status::invalid_argument(format!(
            "floor_greediness_ppm {greediness_ppm} must be in 0..=1000000"
        )));
    }
    let clamp_min = floor_clamp_min.unwrap_or(crate::vector::DEFAULT_FLOOR_CLAMP_MIN) as usize;
    let share_ppm = floor_share_ppm.unwrap_or(1_000_000);
    let resolved_k = floor_k.unwrap_or(default_k) as usize;
    Ok(Some(crate::vector::FloorPruneSpec {
        enabled: true,
        floor_k: resolved_k,
        share_ppm,
        greediness_ppm,
        clamp_min,
    }))
}

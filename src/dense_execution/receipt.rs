//! The execution receipt: what the searched parts proved, validated against
//! the plan that asked for it.

use super::plan::{DenseExecutionError, ResolvedDensePlan, Route};
use crate::pb::{
    DenseCoverage as Coverage, DenseExecutionOutcome, DenseFilterRegimes, DenseTechnique,
    DenseTechniqueCount, DenseTermination as Termination,
};

/// What the shards reported for one dense selection.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Evidence {
    pub(crate) regimes: Option<DenseFilterRegimes>,
    /// Consulted non-empty shards and how many reported explicit per-part counts.
    pub(crate) consulted_shards: u32,
    pub(crate) reported_shards: u32,
    /// Per-part execution counts summed across consulted shards.
    pub(crate) parts_total: u32,
    pub(crate) parts_skipped: u32,
    pub(crate) parts_walked: u32,
    pub(crate) parts_enumerated: u32,
    pub(crate) parts_scanned: u32,
    pub(crate) parts_staged: u32,
    pub(crate) tail_enumerated_rows: u64,
    pub(crate) patience_stopped: u32,
    pub(crate) shards_finished_early: u32,
    pub(crate) shards_floor_pruned: u32,
    pub(crate) fp32_rows_scored: u64,
    /// Physical rows the fleet holds.
    pub(crate) rows: u64,
}

impl Evidence {
    fn regime_walks(&self) -> u32 {
        self.regimes.map_or(0, |r| r.filter_aware + r.plain)
    }

    fn early_stops(&self) -> u32 {
        self.patience_stopped + self.shards_finished_early + self.shards_floor_pruned
    }

    fn active_parts(&self) -> u32 {
        self.parts_total.saturating_sub(self.parts_skipped)
    }
}

fn contradiction(route: Route, why: &str) -> DenseExecutionError {
    DenseExecutionError::ReceiptContradiction(format!(
        "dense execution receipt contradicts its {route:?} plan: {why}"
    ))
}

fn counts(entries: &[(DenseTechnique, u32)]) -> Vec<DenseTechniqueCount> {
    entries
        .iter()
        .filter(|(_, parts)| *parts > 0)
        .map(|&(technique, parts)| DenseTechniqueCount {
            technique: technique as i32,
            parts,
        })
        .collect()
}

/// Exhaustive routes certify only when every consulted non-empty shard reported
/// per-part accounting and every active part executed exhaustively without a
/// graph walk, staged approximation, or early stop.
fn exhaustive_receipt(
    route: Route,
    evidence: &Evidence,
) -> Result<(Vec<DenseTechniqueCount>, Coverage, Termination), DenseExecutionError> {
    if evidence.reported_shards < evidence.consulted_shards {
        return Err(contradiction(
            route,
            "not every consulted shard reported per-part execution counts",
        ));
    }
    if evidence.rows > 0 && evidence.consulted_shards > 0 && evidence.parts_total == 0 {
        return Err(contradiction(
            route,
            "non-empty consulted shards reported zero parts",
        ));
    }
    if evidence.parts_walked > 0
        || evidence.regime_walks() > 0
        || evidence.parts_staged > 0
        || evidence.early_stops() > 0
    {
        return Err(contradiction(
            route,
            "a part walked a graph, ran staged selection, or stopped early",
        ));
    }
    let active = evidence.active_parts();
    let techniques = match route {
        Route::NativeScan => {
            if evidence.parts_scanned != active || evidence.parts_enumerated > 0 {
                return Err(contradiction(
                    route,
                    "active parts did not all complete an exhaustive quantized scan",
                ));
            }
            counts(&[(DenseTechnique::QuantizedScan, evidence.parts_scanned)])
        }
        Route::EnumerateAll => {
            if evidence.parts_enumerated != active || evidence.parts_scanned > 0 {
                return Err(contradiction(
                    route,
                    "active parts did not all complete pinned FP32 enumeration",
                ));
            }
            counts(&[(DenseTechnique::Fp32Scan, evidence.parts_enumerated)])
        }
        Route::Fp32AllRows => {
            if evidence.parts_enumerated != active || evidence.parts_scanned > 0 {
                return Err(contradiction(
                    route,
                    "active parts did not all complete shard-local FP32 scanning",
                ));
            }
            if active > 0 && evidence.fp32_rows_scored == 0 {
                return Err(contradiction(
                    route,
                    "active parts were admitted, but zero FP32 rows were scored",
                ));
            }
            counts(&[(DenseTechnique::Fp32Scan, evidence.parts_enumerated)])
        }
        Route::Walk | Route::Fp32Pool => {
            return Err(contradiction(
                route,
                "approximate route reached exhaustive receipt validation",
            ))
        }
    };
    Ok((
        techniques,
        Coverage::ExhaustiveCertified,
        Termination::Exhausted,
    ))
}

/// Verify an exhaustive execution receipt on direct `Search` / `HybridSearch`
/// calls when `enumerate_all` or an exhaustive scan contract is active.
pub(crate) fn verify_exhaustive_scan(
    route: Route,
    evidence: &Evidence,
) -> Result<(), DenseExecutionError> {
    exhaustive_receipt(route, evidence).map(|_| ())
}

/// Approximate routes report actual per-part walk vs enumeration counts and
/// honest termination (`EXHAUSTED` when every active part enumerated without
/// walking, `BUDGET_STOPPED` when patience or early finish stopped a walk,
/// `CONVERGED` when at least one graph walk ran to completion).
fn approximate_receipt(
    route: Route,
    pool_selection: DenseTechnique,
    evidence: &Evidence,
) -> Result<(Vec<DenseTechniqueCount>, Coverage, Termination), DenseExecutionError> {
    if evidence.reported_shards < evidence.consulted_shards {
        return Err(contradiction(
            route,
            "not every consulted shard reported per-part execution counts",
        ));
    }
    let active = evidence.active_parts();
    if route == Route::Fp32Pool && pool_selection == DenseTechnique::QuantizedScan {
        if evidence.parts_scanned != active || evidence.parts_walked > 0 {
            return Err(contradiction(
                route,
                "quantized candidate pool did not scan every active part",
            ));
        }
        let techniques = counts(&[(DenseTechnique::QuantizedScan, evidence.parts_scanned)]);
        return Ok((techniques, Coverage::CandidateSet, Termination::Exhausted));
    }
    let executed = evidence
        .parts_walked
        .saturating_add(evidence.parts_enumerated);
    if evidence.shards_finished_early == 0 && executed != active {
        return Err(contradiction(
            route,
            "walk and enumeration part counts do not match active parts",
        ));
    }
    let techniques = counts(&[
        (DenseTechnique::HnswWalk, evidence.parts_walked),
        (DenseTechnique::Fp32Scan, evidence.parts_enumerated),
    ]);
    let termination = if evidence.patience_stopped + evidence.shards_finished_early > 0 {
        Termination::BudgetStopped
    } else if evidence.parts_walked == 0 {
        Termination::Exhausted
    } else {
        Termination::Converged
    };
    Ok((techniques, Coverage::CandidateSet, termination))
}

/// Stamp the plan and the validated receipt onto the outcome. Contradictory
/// evidence is an error, never a weaker certificate.
pub(crate) fn stamp(
    plan: &ResolvedDensePlan,
    evidence: &Evidence,
    outcome: &mut DenseExecutionOutcome,
) -> Result<(), DenseExecutionError> {
    let (techniques, coverage, termination) = if plan.approximate() {
        approximate_receipt(plan.route, plan.pool_selection, evidence)?
    } else {
        exhaustive_receipt(plan.route, evidence)?
    };
    outcome.techniques = techniques;
    outcome.coverage = coverage as i32;
    outcome.termination = termination as i32;
    outcome.filter_regimes = evidence.regimes;
    outcome.tail_enumerated_rows = evidence.tail_enumerated_rows;
    outcome.patience_stopped = evidence.patience_stopped;
    outcome.shards_finished_early = evidence.shards_finished_early;
    outcome.shards_floor_pruned = evidence.shards_floor_pruned;
    Ok(())
}

/// The plan's request-side facts on a fresh outcome.
pub(crate) fn planned_outcome(plan: &ResolvedDensePlan) -> DenseExecutionOutcome {
    DenseExecutionOutcome {
        requested_mode: plan.request.mode as i32,
        resolved_mode: plan.resolved_mode as i32,
        requested_precision: plan.request.precision as i32,
        selection_precision: plan.selection_precision as i32,
        output_precision: plan.output_precision as i32,
        filter_strategy: plan.filter_strategy as i32,
        preset: plan.preset.map(|p| p.name.to_string()).unwrap_or_default(),
        policy_qualified: false,
        fallback_reason: plan.fallback_reason.clone(),
        planner_reason: plan.planner_reason.clone(),
        evidence_scope: crate::pb::DenseEvidenceScope::NotApplicable as i32,
        ..Default::default()
    }
}

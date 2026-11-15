//! The tier plan: placements and moves derived from one snapshot.

mod classify;
mod encode;
mod repair;
mod reports;

use super::observations::CommittedCopy;
use super::snapshot::{FragmentRecord, TierSnapshot};
use super::MAX_REPORTS_PER_FRAGMENT;

use self::classify::classify;
use self::repair::Repair;
use self::reports::{aggregate, check_coverage, index_reports, FragmentReports};

/// §3/§4: one reporter's figures inside a placement, unmerged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReporterFigures {
    pub node_id: String,
    pub shard: String,
    pub scans_observed: u64,
    pub queue_wait_p50_us: Option<u64>,
    pub queue_wait_p99_us: Option<u64>,
    pub samples: u32,
    pub last_scanned_unix_ms: u64,
}

/// §4: the tier a fragment's observations classify it into, before any
/// move, with the figures the plan saw.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FragmentPlacement {
    pub fragment: FragmentRecord,
    pub classified_tier: Option<String>,
    pub unclassified_reason: Option<String>,
    pub rate_nanos: u64,
    pub rows: u64,
    pub resident_bytes: u64,
    pub last_scanned_unix_ms: u64,
    pub complete_replicas: Vec<CommittedCopy>,
    pub reporters: Vec<ReporterFigures>,
}

/// §4/§6: one advisory move. The destination is an advisory node
/// description; no storage incarnation is minted by a dry run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TierMove {
    pub fragment: FragmentRecord,
    pub source: CommittedCopy,
    pub destination_node: String,
    pub from_tier: String,
    pub to_tier: String,
    pub bytes: u64,
    pub plan_digest: [u8; 32],
    pub control_revision: u64,
    pub observation_epoch: u64,
    pub policy_fingerprint: [u8; 32],
}

/// §4: the advisory plan. `moves` is the repair sequence; `refusals`
/// holds the per-violation capacity reports (§4 step 4, E13).
#[derive(Debug, Clone)]
pub struct PlanTiersResponse {
    pub plan_digest: [u8; 32],
    pub policy_fingerprint: [u8; 32],
    pub observation_epoch: u64,
    pub control_revision: u64,
    pub placements: Vec<FragmentPlacement>,
    pub moves: Vec<TierMove>,
    pub refusals: Vec<String>,
}

/// §5's exact rate: `floor(S · 10^12 / (W · B))` in `u128`. With §3's
/// bounds the arithmetic cannot overflow; the caller guarantees `B > 0`.
pub(super) fn rate_nanos(scans: u128, window_ms: u64, resident_bytes: u128) -> u64 {
    let numerator = scans * 1_000_000_000_000u128;
    let denominator = (window_ms as u128) * resident_bytes;
    u64::try_from(numerator / denominator).unwrap_or(u64::MAX)
}

/// §3/§5's fan-in bound.
pub(super) fn ensure_fan_in(count: usize) -> Result<(), String> {
    if count > MAX_REPORTS_PER_FRAGMENT {
        return Err(format!(
            "plan_tiers: fragment aggregates {count} reports, past the 2^20 fan-in bound"
        ));
    }
    Ok(())
}

/// §4: the deterministic dry run. Pure function of the validated
/// snapshot; the same snapshot yields the same response, bitwise.
pub fn plan_tiers(snapshot: &TierSnapshot) -> Result<PlanTiersResponse, String> {
    snapshot.policy.validate()?;
    let reports = index_reports(snapshot)?;
    let policy_fingerprint = snapshot.policy.fingerprint();
    let plan_digest = snapshot.plan_digest();
    let mut placements = Vec::new();
    let mut repair = Repair::new(snapshot, plan_digest, policy_fingerprint);
    let empty = FragmentReports::default();

    for fragment in &snapshot.fragments {
        let frag_reports = reports
            .get(&(
                fragment.partition.clone(),
                fragment.topology_generation,
                fragment.leaf.clone(),
            ))
            .unwrap_or(&empty);
        check_coverage(snapshot, fragment, frag_reports)?;
        let figures = aggregate(snapshot, fragment, frag_reports)?;
        let bytes = figures.bytes;
        let classification = classify(snapshot, figures.rate_nanos, figures.warmth)?;
        let tier = classification.tier;
        placements.push(placement(fragment, figures, classification)?);
        if let Some(tier) = tier {
            repair.plan_fragment(fragment, tier, bytes);
        }
    }

    Ok(PlanTiersResponse {
        plan_digest,
        policy_fingerprint,
        observation_epoch: snapshot.observation_epoch,
        control_revision: snapshot.control_revision,
        placements,
        moves: repair.moves,
        refusals: repair.refusals,
    })
}

/// The placement record of one fragment: its classification and the
/// figures the plan saw, with the aggregates narrowed to `u64`.
fn placement(
    fragment: &FragmentRecord,
    figures: reports::FragmentFigures,
    classification: classify::Classification<'_>,
) -> Result<FragmentPlacement, String> {
    Ok(FragmentPlacement {
        fragment: fragment.clone(),
        classified_tier: classification.tier.map(|tier| tier.name.clone()),
        unclassified_reason: classification.unclassified_reason,
        rate_nanos: figures.rate_nanos,
        rows: u64::try_from(figures.rows).map_err(|_| {
            format!(
                "plan_tiers: fragment {} row count overflows u64",
                fragment.label()
            )
        })?,
        resident_bytes: u64::try_from(figures.bytes).map_err(|_| {
            format!(
                "plan_tiers: fragment {} byte count overflows u64",
                fragment.label()
            )
        })?,
        last_scanned_unix_ms: figures.warmth,
        complete_replicas: fragment.complete_copies().cloned().collect(),
        reporters: figures.reporters,
    })
}

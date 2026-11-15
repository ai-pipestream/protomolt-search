//! Classifying a fragment into the first tier its figures satisfy.

use crate::capacity_tiers::snapshot::TierSnapshot;
use crate::capacity_tiers::CapacityTier;

/// The tier a fragment classifies into, or why no tier took it. A reason
/// can stand beside a tier: it names the last higher-precedence tier
/// whose band matched and whose warmth bound the fragment missed.
pub(super) struct Classification<'a> {
    pub(super) tier: Option<&'a CapacityTier>,
    pub(super) unclassified_reason: Option<String>,
}

/// Classify (§5): first tier in precedence order whose band and warmth
/// bound the fragment satisfies.
pub(super) fn classify(
    snapshot: &TierSnapshot,
    rate: u64,
    warmth: u64,
) -> Result<Classification<'_>, String> {
    let t = snapshot.planning_instant_unix_ms;
    let mut unclassified_reason = None;
    for tier in &snapshot.policy.tiers {
        if rate < tier.scans_per_byte_nanos_lo || rate >= tier.scans_per_byte_nanos_hi {
            continue;
        }
        if tier.max_seconds_since_scan > 0 {
            let bound_ms = tier
                .max_seconds_since_scan
                .checked_mul(1000)
                .ok_or_else(|| {
                    format!(
                        "plan_tiers: tier {:?} warmth bound {} s overflows milliseconds",
                        tier.name, tier.max_seconds_since_scan
                    )
                })?;
            if warmth == 0 {
                unclassified_reason = Some(format!(
                    "tier {:?} needs a warmth bound of {} s and the fragment was never scanned",
                    tier.name, tier.max_seconds_since_scan
                ));
                continue;
            }
            let age = t.saturating_sub(warmth);
            if age > bound_ms {
                unclassified_reason = Some(format!(
                    "tier {:?} needs a scan within {} s; the last was {} ms ago",
                    tier.name, tier.max_seconds_since_scan, age
                ));
                continue;
            }
        }
        return Ok(Classification {
            tier: Some(tier),
            unclassified_reason,
        });
    }
    if unclassified_reason.is_none() {
        unclassified_reason = Some(format!("rate {rate} nanos is outside every band"));
    }
    Ok(Classification {
        tier: None,
        unclassified_reason,
    })
}

//! Current-cohort reports per fragment: indexing, coverage, aggregation.

use std::collections::{BTreeMap, BTreeSet};

use crate::capacity_tiers::observations::{PartitionIdentity, PartitionObservation};
use crate::capacity_tiers::snapshot::{FragmentRecord, TierSnapshot};

use super::{ensure_fan_in, rate_nanos, ReporterFigures};

pub(super) type FragmentKey = (PartitionIdentity, u64, String);

#[derive(Default)]
pub(super) struct FragmentReports<'a> {
    /// Current-cohort reports by (shard, node).
    current: BTreeMap<(String, String), &'a PartitionObservation>,
}

/// §3's aggregate of one fragment: owner-only rows and bytes, scans over
/// every reporter, warmth as the maximum, and each reporter's own figures.
pub(super) struct FragmentFigures {
    pub(super) rows: u128,
    pub(super) bytes: u128,
    pub(super) rate_nanos: u64,
    pub(super) warmth: u64,
    pub(super) reporters: Vec<ReporterFigures>,
}

/// Index the current-cohort reports of covered fragments by their full
/// identity — resource, declaration, generation, leaf, shard, node —
/// and apply the skew rule to every stored report the plan could read.
pub(super) fn index_reports(
    snapshot: &TierSnapshot,
) -> Result<BTreeMap<FragmentKey, FragmentReports<'_>>, String> {
    let t = snapshot.planning_instant_unix_ms;
    let (w0, w1) = snapshot.current_cohort()?;
    let skew_horizon = t
        .checked_add(snapshot.clock_skew_bound_ms)
        .ok_or_else(|| "plan_tiers: planning instant plus the skew bound overflows".to_string())?;
    let covered: BTreeSet<FragmentKey> = snapshot
        .fragments
        .iter()
        .map(|f| (f.partition.clone(), f.topology_generation, f.leaf.clone()))
        .collect();
    let mut reports: BTreeMap<FragmentKey, FragmentReports> = BTreeMap::new();
    for obs in &snapshot.observations {
        if obs.window_end_unix_ms > skew_horizon {
            return Err(format!(
                "plan_tiers: observation from node {} ends {} ms in the future, past the {} ms skew bound",
                obs.reporter.node_id,
                obs.window_end_unix_ms - t,
                snapshot.clock_skew_bound_ms
            ));
        }
        let key = (
            obs.partition.clone(),
            obs.topology_generation,
            obs.leaf.clone(),
        );
        if !covered.contains(&key) {
            continue;
        }
        if obs.window_start_unix_ms != w0 || obs.window_end_unix_ms != w1 {
            continue;
        }
        reports
            .entry(key)
            .or_default()
            .current
            .insert((obs.shard.shard.clone(), obs.reporter.node_id.clone()), obs);
    }
    Ok(reports)
}

/// Coverage: every shard's committed owner must have a current report
/// (unknown is not idle, E3), and so must every complete copy (§3's
/// freshness rule).
pub(super) fn check_coverage(
    snapshot: &TierSnapshot,
    fragment: &FragmentRecord,
    frag_reports: &FragmentReports<'_>,
) -> Result<(), String> {
    let t = snapshot.planning_instant_unix_ms;
    let mut required: Vec<(&str, &str)> = fragment
        .shard_owners
        .iter()
        .map(|(shard, owner)| (shard.as_str(), owner.as_str()))
        .collect();
    for copy in fragment.complete_copies() {
        for shard in fragment.shard_owners.keys() {
            required.push((shard.as_str(), copy.node_id.as_str()));
        }
    }
    required.sort();
    required.dedup();
    for (shard, node) in required {
        let Some(obs) = frag_reports
            .current
            .get(&(shard.to_string(), node.to_string()))
        else {
            return Err(format!(
                "plan_tiers: partition {} on node {} has no current observation; unknown is not idle",
                fragment.partition.label(),
                node
            ));
        };
        let age = t.saturating_sub(obs.window_end_unix_ms);
        if age > snapshot.max_observation_age_ms {
            return Err(format!(
                "plan_tiers: partition {} on node {} was last observed {} ms ago, past the {} ms bound",
                fragment.partition.label(),
                node,
                age,
                snapshot.max_observation_age_ms
            ));
        }
    }
    Ok(())
}

/// Aggregate (§3): owner-only bytes, scans over every reporter, warmth
/// as the maximum, checked fan-in. The caller has checked coverage, so
/// every committed owner has a current report.
pub(super) fn aggregate(
    snapshot: &TierSnapshot,
    fragment: &FragmentRecord,
    frag_reports: &FragmentReports<'_>,
) -> Result<FragmentFigures, String> {
    ensure_fan_in(frag_reports.current.len())?;
    let mut rows: u128 = 0;
    let mut bytes: u128 = 0;
    for (shard, owner) in &fragment.shard_owners {
        let obs = frag_reports
            .current
            .get(&(shard.clone(), owner.clone()))
            .expect("owner coverage checked above");
        rows += obs.rows as u128;
        bytes += obs.resident_bytes as u128;
    }
    let mut scans: u128 = 0;
    let mut warmth: u64 = 0;
    let mut reporters = Vec::new();
    for ((shard, node), obs) in &frag_reports.current {
        scans += obs.scans_observed as u128;
        warmth = warmth.max(obs.last_scanned_unix_ms);
        reporters.push(ReporterFigures {
            node_id: node.clone(),
            shard: shard.clone(),
            scans_observed: obs.scans_observed,
            queue_wait_p50_us: (obs.samples > 0).then_some(obs.queue_wait_p50_us),
            queue_wait_p99_us: (obs.samples > 0).then_some(obs.queue_wait_p99_us),
            samples: obs.samples,
            last_scanned_unix_ms: obs.last_scanned_unix_ms,
        });
    }
    if bytes == 0 {
        return Err(format!(
            "plan_tiers: fragment {} has 0 resident bytes from its committed owners",
            fragment.label()
        ));
    }
    Ok(FragmentFigures {
        rows,
        bytes,
        rate_nanos: rate_nanos(scans, snapshot.cohort_length_ms, bytes),
        warmth,
        reporters,
    })
}

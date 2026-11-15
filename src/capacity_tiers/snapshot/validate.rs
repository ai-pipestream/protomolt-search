//! The snapshot's single validated constructor and its admission checks.

use std::collections::BTreeSet;

use crate::capacity_tiers::observations::{
    check_observation_identity, PartitionIdentity, PartitionObservation,
};
use crate::capacity_tiers::{
    hex16, MAX_COHORT_LENGTH_MS, MAX_SNAPSHOT_FRAGMENTS, MAX_SNAPSHOT_NODES,
    MAX_SNAPSHOT_OBSERVATIONS,
};

use super::fragment::derive_fragment;
use super::{FragmentRecord, TierSnapshot, TierSnapshotInput};

impl TierSnapshot {
    /// The single constructor. Enforces: policy validity, cohort bounds,
    /// resource binding of every observation and fragment to the context
    /// and the committed view, committed-identity equality in both
    /// directions, incarnation currency, duplicate rejection (nothing is
    /// silently overwritten), source residency and node eligibility for
    /// every `complete` claim, one coverage digest per fragment's
    /// complete copies, canonical ordering of fragments, pools, owners,
    /// and copies, and the admission caps.
    pub fn validated(input: TierSnapshotInput) -> Result<Self, String> {
        input.policy.validate()?;
        check_admission(&input)?;
        check_committed_binding(&input)?;
        let fragments = derive_fragments(&input)?;
        check_observations(&input)?;

        Ok(Self {
            planning_instant_unix_ms: input.planning_instant_unix_ms,
            max_moves: input.max_moves,
            max_observation_age_ms: input.max_observation_age_ms,
            clock_skew_bound_ms: input.clock_skew_bound_ms,
            authority_id: input.authority_id,
            authority_incarnation: input.authority_incarnation,
            control_revision: input.control_revision,
            policy_revision: input.policy_revision,
            policy: input.policy,
            observation_epoch: input.observation_epoch,
            topology_generation: input.topology_generation,
            placement_tree_digest: input.placement_tree_digest,
            workspace: input.workspace,
            collection: input.collection,
            derived_fingerprint: input.derived_fingerprint,
            provider_geometry_digest: input.provider_geometry_digest,
            cohort_length_ms: input.cohort_length_ms,
            cohort_phase_unix_ms: input.cohort_phase_unix_ms,
            nodes: input.nodes,
            observations: input.observations,
            fragments,
        })
    }
}

/// The cohort bound and the admission caps.
fn check_admission(input: &TierSnapshotInput) -> Result<(), String> {
    if input.cohort_length_ms == 0 || input.cohort_length_ms > MAX_COHORT_LENGTH_MS {
        return Err(format!(
            "plan_tiers: cohort length {} ms is outside (0, 2^40]",
            input.cohort_length_ms
        ));
    }
    if input.observations.len() > MAX_SNAPSHOT_OBSERVATIONS {
        return Err(format!(
            "plan_tiers: {} observations is past the admission cap of {MAX_SNAPSHOT_OBSERVATIONS}",
            input.observations.len()
        ));
    }
    if input.fragment_requests.len() > MAX_SNAPSHOT_FRAGMENTS {
        return Err(format!(
            "plan_tiers: {} fragment requests is past the admission cap of {MAX_SNAPSHOT_FRAGMENTS}",
            input.fragment_requests.len()
        ));
    }
    if input.nodes.len() > MAX_SNAPSHOT_NODES {
        return Err(format!(
            "plan_tiers: {} nodes is past the admission cap of {MAX_SNAPSHOT_NODES}",
            input.nodes.len()
        ));
    }
    Ok(())
}

/// The committed view must serve the snapshot's resource at the
/// snapshot's topology generation.
fn check_committed_binding(input: &TierSnapshotInput) -> Result<(), String> {
    if input.committed.workspace != input.workspace
        || input.committed.collection != input.collection
        || input.committed.derived_fingerprint != input.derived_fingerprint
    {
        return Err(format!(
            "plan_tiers: the committed view's resource {}/{}/{} is not the snapshot's {}/{}/{}",
            input.committed.workspace,
            input.committed.collection,
            hex16(
                &input.committed.derived_fingerprint[..16]
                    .try_into()
                    .unwrap()
            ),
            input.workspace,
            input.collection,
            hex16(&input.derived_fingerprint[..16].try_into().unwrap())
        ));
    }
    if input.topology_generation != input.committed.topology_generation {
        return Err(format!(
            "plan_tiers: snapshot topology_generation {} is not the committed {}",
            input.topology_generation, input.committed.topology_generation
        ));
    }
    Ok(())
}

/// Whether a partition belongs to the snapshot's resource.
fn in_resource(input: &TierSnapshotInput, partition: &PartitionIdentity) -> bool {
    partition.workspace == input.workspace
        && partition.collection == input.collection
        && partition.derived_fingerprint == input.derived_fingerprint
}

/// Derive fragments from the committed view in canonical order; reject
/// duplicate requests rather than letting one shadow another.
fn derive_fragments(input: &TierSnapshotInput) -> Result<Vec<FragmentRecord>, String> {
    let mut requests = input.fragment_requests.clone();
    requests.sort_by(|a, b| {
        (&a.partition, a.topology_generation, &a.leaf).cmp(&(
            &b.partition,
            b.topology_generation,
            &b.leaf,
        ))
    });
    for pair in requests.windows(2) {
        if pair[0].partition == pair[1].partition
            && pair[0].topology_generation == pair[1].topology_generation
            && pair[0].leaf == pair[1].leaf
        {
            return Err(format!(
                "plan_tiers: duplicate fragment request ({}, gen {}, {})",
                pair[0].partition.label(),
                pair[0].topology_generation,
                pair[0].leaf
            ));
        }
    }
    let mut fragments = Vec::with_capacity(requests.len());
    for request in requests {
        if !in_resource(input, &request.partition) {
            return Err(format!(
                "plan_tiers: fragment request {} is outside the snapshot's resource",
                request.partition.label()
            ));
        }
        if request.topology_generation != input.committed.topology_generation {
            return Err(format!(
                "plan_tiers: fragment request {} names topology generation {}, the committed is {}",
                request.partition.label(),
                request.topology_generation,
                input.committed.topology_generation
            ));
        }
        fragments.push(derive_fragment(&request, input)?);
    }
    Ok(fragments)
}

/// Observations: the store's ingest checks, re-verified, plus incarnation
/// currency and duplicate rejection.
fn check_observations(input: &TierSnapshotInput) -> Result<(), String> {
    let mut seen_keys = BTreeSet::new();
    for obs in &input.observations {
        if !in_resource(input, &obs.partition) {
            return Err(format!(
                "plan_tiers: observation of partition {} is outside the snapshot's resource",
                obs.partition.label()
            ));
        }
        check_observation_identity(&input.committed, obs)?;
        check_incarnation(input, obs)?;
        check_cohort_window(input, obs)?;
        if !seen_keys.insert(obs.key()) {
            return Err(format!(
                "plan_tiers: duplicate observation for ({}, gen {}, {}, {}, gen {}) from {}",
                obs.partition.label(),
                obs.topology_generation,
                obs.leaf,
                obs.shard.shard,
                obs.shard.source_generation,
                obs.reporter.node_id
            ));
        }
    }
    Ok(())
}

/// Only the reporter's current process incarnation may feed a snapshot.
fn check_incarnation(input: &TierSnapshotInput, obs: &PartitionObservation) -> Result<(), String> {
    match input.current_incarnations.get(&obs.reporter.node_id) {
        Some(current) if *current == obs.reporter.process_incarnation => Ok(()),
        Some(current) => Err(format!(
            "plan_tiers: node {} incarnation {} is superseded by {}",
            obs.reporter.node_id,
            hex16(&obs.reporter.process_incarnation),
            hex16(current)
        )),
        None => Err(format!(
            "plan_tiers: node {} has no registered incarnation",
            obs.reporter.node_id
        )),
    }
}

/// Every observation window is one cohort window of the snapshot's grid.
fn check_cohort_window(
    input: &TierSnapshotInput,
    obs: &PartitionObservation,
) -> Result<(), String> {
    let window_len = obs
        .window_end_unix_ms
        .checked_sub(obs.window_start_unix_ms)
        .ok_or_else(|| {
            format!(
                "plan_tiers: window [{}, {}) is inverted",
                obs.window_start_unix_ms, obs.window_end_unix_ms
            )
        })?;
    if window_len != input.cohort_length_ms
        || obs.window_start_unix_ms < input.cohort_phase_unix_ms
        || !(obs.window_start_unix_ms - input.cohort_phase_unix_ms)
            .is_multiple_of(input.cohort_length_ms)
    {
        return Err(format!(
            "plan_tiers: window [{}, {}) is not a cohort window (length {} ms, phase {})",
            obs.window_start_unix_ms,
            obs.window_end_unix_ms,
            input.cohort_length_ms,
            input.cohort_phase_unix_ms
        ));
    }
    Ok(())
}

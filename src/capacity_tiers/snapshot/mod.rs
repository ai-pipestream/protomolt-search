//! The validated planning snapshot: fragments, nodes, and their digests.

mod digest;
mod fragment;
mod validate;

use std::collections::BTreeMap;

use super::observations::{CommittedCopy, CommittedView, PartitionIdentity, PartitionObservation};
use super::{put_str, put_u32, put_u64, NodeResidency, TierPolicy};

/// §2: a fragment as the planner needs it: the committed coverage, the
/// shard owners whose reports count bytes, and the verified copies the
/// replica floor counts. Constructed only by [`TierSnapshot::validated`]
/// from the committed view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FragmentRecord {
    pub partition: PartitionIdentity,
    pub topology_generation: u64,
    pub leaf: String,
    pub pool: Vec<String>,
    /// shard → committed owner node; owner reports are the only byte
    /// sources (§3's deduplication rule).
    pub shard_owners: BTreeMap<String, String>,
    /// Every committed copy, complete and partial, in canonical
    /// (node id, storage incarnation) order; the floor counts complete
    /// copies in distinct failure domains.
    pub copies: Vec<CommittedCopy>,
}

impl FragmentRecord {
    pub(super) fn label(&self) -> String {
        format!(
            "({}, gen {}, {})",
            self.partition.label(),
            self.topology_generation,
            self.leaf
        )
    }

    pub(super) fn complete_copies(&self) -> impl Iterator<Item = &CommittedCopy> {
        self.copies.iter().filter(|copy| copy.complete)
    }

    pub(super) fn put_canonical(&self, out: &mut Vec<u8>) {
        self.partition.put_canonical(out);
        put_u64(out, self.topology_generation);
        put_str(out, &self.leaf);
        put_u32(out, self.pool.len() as u32);
        for node in &self.pool {
            put_str(out, node);
        }
        put_u32(out, self.shard_owners.len() as u32);
        for (shard, owner) in &self.shard_owners {
            put_str(out, shard);
            put_str(out, owner);
        }
        put_u32(out, self.copies.len() as u32);
        for copy in &self.copies {
            put_copy(out, copy);
        }
    }
}

pub(super) fn put_copy(out: &mut Vec<u8>, copy: &CommittedCopy) {
    put_str(out, &copy.node_id);
    out.extend_from_slice(&copy.storage_incarnation);
    put_str(out, &copy.failure_domain);
    out.push(copy.complete as u8);
    out.extend_from_slice(&copy.coverage_digest);
}

/// A node's committed capacity report from the provider geometry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeCapacity {
    pub total_bytes: u64,
    pub resident_bytes: u64,
}

impl NodeCapacity {
    pub(super) fn free_bytes(&self) -> u64 {
        self.total_bytes.saturating_sub(self.resident_bytes)
    }
}

/// §7: a node as the authority committed it: self-declared residency,
/// eligibility, failure domain, and capacity. Pool membership is not
/// proof of residency or movability; this record is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeRecord {
    pub residency: NodeResidency,
    pub eligible: bool,
    pub failure_domain: String,
    pub capacity: NodeCapacity,
}

/// A fragment the operator wants planned, by identity. The validated
/// constructor derives its pool, owners, and copies from the committed
/// view — they are never accepted from the caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FragmentRequest {
    pub partition: PartitionIdentity,
    pub topology_generation: u64,
    pub leaf: String,
}

/// The public input to [`TierSnapshot::validated`]. Everything the
/// constructor checks comes in here; nothing else is an input.
#[derive(Debug, Clone)]
pub struct TierSnapshotInput {
    pub planning_instant_unix_ms: u64,
    pub max_moves: u64,
    pub max_observation_age_ms: u64,
    pub clock_skew_bound_ms: u64,
    pub authority_id: String,
    pub authority_incarnation: [u8; 16],
    pub control_revision: u64,
    pub policy_revision: u64,
    pub policy: TierPolicy,
    pub observation_epoch: u64,
    pub topology_generation: u64,
    pub placement_tree_digest: [u8; 32],
    pub workspace: String,
    pub collection: String,
    pub derived_fingerprint: [u8; 32],
    pub provider_geometry_digest: [u8; 32],
    pub cohort_length_ms: u64,
    pub cohort_phase_unix_ms: u64,
    pub nodes: BTreeMap<String, NodeRecord>,
    pub observations: Vec<PartitionObservation>,
    pub fragment_requests: Vec<FragmentRequest>,
    pub committed: CommittedView,
    pub current_incarnations: BTreeMap<String, [u8; 16]>,
}

/// §4: the frozen input snapshot the plan is a pure function of. Opaque:
/// built only by [`TierSnapshot::validated`], so identity, residency,
/// canonical ordering, and admission caps are enforced exactly once, for
/// the store-fed path and for any hand-built input alike. It performs
/// no I/O and no live lookups.
#[derive(Debug, Clone)]
pub struct TierSnapshot {
    pub(super) planning_instant_unix_ms: u64,
    pub(super) max_moves: u64,
    pub(super) max_observation_age_ms: u64,
    pub(super) clock_skew_bound_ms: u64,
    authority_id: String,
    authority_incarnation: [u8; 16],
    pub(super) control_revision: u64,
    policy_revision: u64,
    pub(super) policy: TierPolicy,
    pub(super) observation_epoch: u64,
    topology_generation: u64,
    placement_tree_digest: [u8; 32],
    workspace: String,
    collection: String,
    derived_fingerprint: [u8; 32],
    provider_geometry_digest: [u8; 32],
    pub(super) cohort_length_ms: u64,
    cohort_phase_unix_ms: u64,
    pub(super) nodes: BTreeMap<String, NodeRecord>,
    pub(super) observations: Vec<PartitionObservation>,
    pub(super) fragments: Vec<FragmentRecord>,
}

impl TierSnapshot {
    /// The cohort window the plan aggregates: the latest cohort window
    /// fully closed at the frozen instant (§3).
    pub fn current_cohort(&self) -> Result<(u64, u64), String> {
        if self.cohort_length_ms == 0 {
            return Err("plan_tiers: cohort length 0 ms is invalid".to_string());
        }
        if self.planning_instant_unix_ms < self.cohort_phase_unix_ms {
            return Err(format!(
                "plan_tiers: planning instant {} is before the cohort phase {}",
                self.planning_instant_unix_ms, self.cohort_phase_unix_ms
            ));
        }
        let elapsed = self.planning_instant_unix_ms - self.cohort_phase_unix_ms;
        let closed = elapsed / self.cohort_length_ms;
        if closed == 0 {
            return Err(format!(
                "plan_tiers: no cohort window has closed by planning instant {}",
                self.planning_instant_unix_ms
            ));
        }
        let end = self.cohort_phase_unix_ms + closed * self.cohort_length_ms;
        Ok((end - self.cohort_length_ms, end))
    }
}

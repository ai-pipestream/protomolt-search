//! The authority's committed view and the identity checks against it.

use std::collections::BTreeMap;

use crate::capacity_tiers::hex16;

use super::identity::{PartitionIdentity, PartitionObservation};

/// §2: one copy of a fragment's bytes as the authority committed it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedCopy {
    pub node_id: String,
    pub storage_incarnation: [u8; 16],
    pub failure_domain: String,
    /// Whether this copy's coverage evidence over the fragment's full
    /// row set at the committed source version verified (§2's complete
    /// replica test). The validated constructor binds a `complete` claim
    /// to a Server-resident, eligible node and the fragment's single
    /// coverage digest; anything less is partial evidence.
    pub complete: bool,
    pub coverage_digest: [u8; 32],
}

/// The committed coverage of one (shard, leaf) pair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeafCoverage {
    pub pool: Vec<String>,
    pub owner_node: String,
    pub copies: Vec<CommittedCopy>,
}

/// The committed record a shard's reports are checked against.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedShard {
    pub source_generation: u64,
    pub ownership_epoch: u64,
    pub leaves: BTreeMap<String, LeafCoverage>,
}

/// §3: the authority's committed view, consulted at report ingest and at
/// snapshot validation. All comparisons are equality checks in both
/// directions: a behind-or-ahead value names bytes other than the
/// committed ones and is refused. The resource triple binds the view to
/// one (workspace, collection, declaration): a report or fragment of any
/// other resource is refused, never matched by shard and leaf alone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedView {
    pub workspace: String,
    pub collection: String,
    pub derived_fingerprint: [u8; 32],
    pub topology_generation: u64,
    pub shards: BTreeMap<String, CommittedShard>,
}

impl CommittedView {
    pub(super) fn resource_matches(&self, partition: &PartitionIdentity) -> bool {
        self.workspace == partition.workspace
            && self.collection == partition.collection
            && self.derived_fingerprint == partition.derived_fingerprint
    }
}

/// §3's committed-identity equality checks, shared by the store's ingest
/// and the snapshot's validated constructor: a behind-or-ahead value in
/// either direction names bytes other than the committed ones.
pub(in crate::capacity_tiers) fn check_observation_identity(
    committed: &CommittedView,
    obs: &PartitionObservation,
) -> Result<(), String> {
    if obs.topology_generation != committed.topology_generation {
        return Err(format!(
            "report_shard: topology_generation {} is not the committed {}",
            obs.topology_generation, committed.topology_generation
        ));
    }
    let committed_shard = committed.shards.get(&obs.shard.shard).ok_or_else(|| {
        format!(
            "report_shard: shard {} is not in the committed topology",
            obs.shard.shard
        )
    })?;
    if obs.shard.source_generation != committed_shard.source_generation {
        return Err(format!(
            "report_shard: shard {} source generation {} is not the committed {}",
            obs.shard.shard, obs.shard.source_generation, committed_shard.source_generation
        ));
    }
    if obs.shard.ownership_epoch != committed_shard.ownership_epoch {
        return Err(format!(
            "report_shard: shard {} ownership epoch {} is not the committed {}",
            obs.shard.shard, obs.shard.ownership_epoch, committed_shard.ownership_epoch
        ));
    }
    let coverage = committed_shard.leaves.get(&obs.leaf).ok_or_else(|| {
        format!(
            "report_shard: shard {} covers no rows in leaf {} at generation {}",
            obs.shard.shard, obs.leaf, committed.topology_generation
        )
    })?;
    let copy = coverage
        .copies
        .iter()
        .find(|copy| copy.node_id == obs.reporter.node_id)
        .ok_or_else(|| {
            format!(
                "report_shard: node {} holds no committed copy of shard {} in leaf {}",
                obs.reporter.node_id, obs.shard.shard, obs.leaf
            )
        })?;
    if copy.storage_incarnation != obs.shard.storage_incarnation {
        return Err(format!(
            "report_shard: node {} storage incarnation {} is not the committed {} for shard {}",
            obs.reporter.node_id,
            hex16(&obs.shard.storage_incarnation),
            hex16(&copy.storage_incarnation),
            obs.shard.shard
        ));
    }
    Ok(())
}

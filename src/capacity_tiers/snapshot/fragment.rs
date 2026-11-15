//! Deriving one fragment's committed facts from the committed view.

use std::collections::BTreeMap;

use crate::capacity_tiers::observations::{CommittedCopy, LeafCoverage};
use crate::capacity_tiers::NodeResidency;

use super::{FragmentRecord, FragmentRequest, TierSnapshotInput};

/// The committed coverage of one leaf gathered across its shards.
#[derive(Default)]
struct GatheredCoverage {
    pool: Option<Vec<String>>,
    shard_owners: BTreeMap<String, String>,
    copies: Vec<CommittedCopy>,
}

impl GatheredCoverage {
    /// Fold one shard's coverage of the leaf in: the pools must agree,
    /// the shard contributes its owner, and its copies join the union
    /// unless they contradict a copy already gathered.
    fn absorb(
        &mut self,
        request: &FragmentRequest,
        shard_id: &str,
        coverage: &LeafCoverage,
    ) -> Result<(), String> {
        let mut this_pool = coverage.pool.clone();
        this_pool.sort();
        match &self.pool {
            None => self.pool = Some(this_pool),
            Some(existing) if *existing != this_pool => {
                return Err(format!(
                    "plan_tiers: shard {} names a different pool for leaf {} than its siblings",
                    shard_id, request.leaf
                ));
            }
            _ => {}
        }
        self.shard_owners
            .insert(shard_id.to_string(), coverage.owner_node.clone());
        for copy in &coverage.copies {
            if let Some(existing) = self.copies.iter().find(|existing| {
                existing.node_id == copy.node_id
                    && existing.storage_incarnation == copy.storage_incarnation
            }) {
                if existing != copy {
                    return Err(format!(
                        "plan_tiers: shard {} reports a conflicting copy of {} on {}",
                        shard_id,
                        request.partition.label(),
                        copy.node_id
                    ));
                }
            } else {
                self.copies.push(copy.clone());
            }
        }
        Ok(())
    }
}

/// Derive one fragment's committed facts from the committed view:
/// every shard with coverage of the leaf contributes its owner, the
/// pools must agree, and copies are the union in canonical order.
/// `complete` claims are then verified against the node registry:
/// Server-resident, eligible, and sharing one coverage digest.
pub(super) fn derive_fragment(
    request: &FragmentRequest,
    input: &TierSnapshotInput,
) -> Result<FragmentRecord, String> {
    let mut gathered = GatheredCoverage::default();
    for (shard_id, shard) in &input.committed.shards {
        let Some(coverage) = shard.leaves.get(&request.leaf) else {
            continue;
        };
        gathered.absorb(request, shard_id, coverage)?;
    }
    let Some(pool) = gathered.pool else {
        return Err(format!(
            "plan_tiers: fragment ({}, gen {}, {}) has no committed coverage",
            request.partition.label(),
            request.topology_generation,
            request.leaf
        ));
    };
    let mut copies = gathered.copies;
    copies.sort_by(|a, b| {
        (&a.node_id, &a.storage_incarnation).cmp(&(&b.node_id, &b.storage_incarnation))
    });
    for member in &pool {
        if !input.nodes.contains_key(member) {
            return Err(format!(
                "plan_tiers: pool member {member} of leaf {} has no committed node record",
                request.leaf
            ));
        }
    }
    verify_complete_copies(request, input, &copies)?;
    Ok(FragmentRecord {
        partition: request.partition.clone(),
        topology_generation: request.topology_generation,
        leaf: request.leaf.clone(),
        pool,
        shard_owners: gathered.shard_owners,
        copies,
    })
}

/// Every `complete` claim sits on a Server-resident, eligible node, and
/// all of a fragment's complete copies share one coverage digest.
fn verify_complete_copies(
    request: &FragmentRequest,
    input: &TierSnapshotInput,
    copies: &[CommittedCopy],
) -> Result<(), String> {
    let mut coverage_digest: Option<[u8; 32]> = None;
    for copy in copies.iter().filter(|copy| copy.complete) {
        let record = input.nodes.get(&copy.node_id).ok_or_else(|| {
            format!(
                "plan_tiers: complete copy on {} has no committed node record",
                copy.node_id
            )
        })?;
        if record.residency != NodeResidency::Server {
            return Err(format!(
                "plan_tiers: complete copy on {} is not server-resident; device-local and unspecified sources are never movable",
                copy.node_id
            ));
        }
        if !record.eligible {
            return Err(format!(
                "plan_tiers: complete copy on {} sits on an ineligible node",
                copy.node_id
            ));
        }
        match coverage_digest {
            None => coverage_digest = Some(copy.coverage_digest),
            Some(existing) if existing != copy.coverage_digest => {
                return Err(format!(
                    "plan_tiers: fragment {} has complete copies under different coverage digests; they cannot be the same source version",
                    request.partition.label()
                ));
            }
            _ => {}
        }
    }
    Ok(())
}

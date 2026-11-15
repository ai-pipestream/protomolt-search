//! Floor and residency violations and the advisory moves that repair them.

use std::collections::{BTreeMap, BTreeSet};

use crate::capacity_tiers::observations::CommittedCopy;
use crate::capacity_tiers::snapshot::{FragmentRecord, TierSnapshot};
use crate::capacity_tiers::{CapacityTier, NodeResidency};

use super::TierMove;

/// The repair state carried across fragments: the moves and refusals so
/// far and the free bytes each node is projected to keep after them.
pub(super) struct Repair<'a> {
    snapshot: &'a TierSnapshot,
    plan_digest: [u8; 32],
    policy_fingerprint: [u8; 32],
    pub(super) moves: Vec<TierMove>,
    pub(super) refusals: Vec<String>,
    projected_free: BTreeMap<String, u64>,
    planned_onto: BTreeSet<String>,
}

/// The nodes and failure domains that hold one fragment's complete
/// copies, the planned destinations included.
struct Held {
    nodes: BTreeSet<String>,
    domains: BTreeSet<String>,
}

impl<'a> Repair<'a> {
    pub(super) fn new(
        snapshot: &'a TierSnapshot,
        plan_digest: [u8; 32],
        policy_fingerprint: [u8; 32],
    ) -> Self {
        Self {
            snapshot,
            plan_digest,
            policy_fingerprint,
            moves: Vec::new(),
            refusals: Vec::new(),
            projected_free: snapshot
                .nodes
                .iter()
                .map(|(node, record)| (node.clone(), record.capacity.free_bytes()))
                .collect(),
            planned_onto: BTreeSet::new(),
        }
    }

    fn free(&self, node: &str) -> u64 {
        self.projected_free.get(node).copied().unwrap_or(0)
    }

    /// Repair (§4 steps 3-5): candidates inside the pool, server-
    /// resident and eligible per the committed node record, domain-
    /// distinct at every intermediate step (§7), capacity-checked
    /// against projected free bytes.
    pub(super) fn plan_fragment(
        &mut self,
        fragment: &FragmentRecord,
        tier: &CapacityTier,
        bytes: u128,
    ) {
        let Some(violation_sources) = self.violation_sources(fragment, tier) else {
            return;
        };
        let mut held = Held {
            nodes: fragment
                .complete_copies()
                .map(|copy| copy.node_id.clone())
                .collect(),
            domains: fragment
                .complete_copies()
                .map(|copy| copy.failure_domain.clone())
                .collect(),
        };
        let fragment_bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        for source in violation_sources {
            if self.moves.len() as u64 >= self.snapshot.max_moves {
                self.refusals.push(format!(
                    "plan_tiers: max_moves {} reached; a violation of fragment {} is left unplanned",
                    self.snapshot.max_moves,
                    fragment.label()
                ));
                return;
            }
            let candidates = self.candidates(fragment, &held);
            let destination = candidates
                .iter()
                .find(|node| self.free(node) >= fragment_bytes)
                .map(|node| (*node).clone());
            let Some(destination) = destination else {
                self.refuse_capacity(fragment, tier, &candidates, fragment_bytes);
                continue;
            };
            *self.projected_free.entry(destination.clone()).or_insert(0) -= fragment_bytes;
            self.planned_onto.insert(destination.clone());
            held.nodes.insert(destination.clone());
            if let Some(record) = self.snapshot.nodes.get(&destination) {
                held.domains.insert(record.failure_domain.clone());
            }
            self.moves.push(TierMove {
                fragment: fragment.clone(),
                source: source.clone(),
                destination_node: destination,
                from_tier: tier.name.clone(),
                to_tier: tier.name.clone(),
                bytes: fragment_bytes,
                plan_digest: self.plan_digest,
                control_revision: self.snapshot.control_revision,
                observation_epoch: self.snapshot.observation_epoch,
                policy_fingerprint: self.policy_fingerprint,
            });
        }
    }

    /// Violations (§4 step 2): floor first, then residency. The floor
    /// counts complete copies in distinct failure domains (§2). The
    /// deficit is a count with one source, never a cloned vector of
    /// work orders. `None` when the fragment has no complete replica to
    /// copy from; the refusal is recorded.
    fn violation_sources(
        &mut self,
        fragment: &FragmentRecord,
        tier: &CapacityTier,
    ) -> Option<Vec<CommittedCopy>> {
        let mut seen_domains = BTreeSet::new();
        let effective = fragment
            .complete_copies()
            .filter(|copy| seen_domains.insert(copy.failure_domain.as_str()))
            .count() as u32;
        let mut violation_sources: Vec<CommittedCopy> = Vec::new();
        if effective < tier.min_replicas {
            let Some(source) = fragment.complete_copies().next() else {
                self.refusals.push(format!(
                    "plan_tiers: fragment {} has 0 complete replicas ({} partial); tier {:?} floor is {}",
                    fragment.label(),
                    fragment.copies.iter().filter(|c| !c.complete).count(),
                    tier.name,
                    tier.min_replicas
                ));
                return None;
            };
            for _ in effective..tier.min_replicas {
                violation_sources.push(source.clone());
            }
        }
        let mut out_of_pool: Vec<CommittedCopy> = fragment
            .complete_copies()
            .filter(|copy| !fragment.pool.contains(&copy.node_id))
            .cloned()
            .collect();
        out_of_pool.sort_by(|a, b| {
            (&a.node_id, &a.storage_incarnation).cmp(&(&b.node_id, &b.storage_incarnation))
        });
        violation_sources.extend(out_of_pool);
        Some(violation_sources)
    }

    /// The pool members that could take a copy, most projected free
    /// bytes first, ties by node id.
    fn candidates<'f>(&self, fragment: &'f FragmentRecord, held: &Held) -> Vec<&'f String> {
        let mut candidates: Vec<&String> = fragment
            .pool
            .iter()
            .filter(|node| !held.nodes.contains(node.as_str()))
            .filter(|node| {
                self.snapshot
                    .nodes
                    .get(node.as_str())
                    .map(|record| {
                        record.residency == NodeResidency::Server
                            && record.eligible
                            && !held.domains.contains(&record.failure_domain)
                    })
                    .unwrap_or(false)
            })
            .collect();
        candidates.sort_by(|a, b| self.free(b).cmp(&self.free(a)).then_with(|| a.cmp(b)));
        candidates
    }

    /// The per-violation capacity report (§4 step 4, E13): every
    /// candidate in node-id order with its projected free bytes.
    fn refuse_capacity(
        &mut self,
        fragment: &FragmentRecord,
        tier: &CapacityTier,
        candidates: &[&String],
        fragment_bytes: u64,
    ) {
        let mut detail_nodes: Vec<&String> = candidates.to_vec();
        detail_nodes.sort();
        let mut detail = String::new();
        for (i, node) in detail_nodes.iter().enumerate() {
            let free = self.free(node);
            if i > 0 {
                detail.push_str(", ");
            }
            if self.planned_onto.contains(node.as_str()) {
                detail.push_str(&format!(
                    "{node} has {free} free after earlier planned moves"
                ));
            } else {
                detail.push_str(&format!("{node} has {free} free"));
            }
        }
        self.refusals.push(format!(
            "plan_tiers: tier {:?} floor is {} for partition {}, but no eligible destination in leaf {} has capacity: {}, the fragment needs {}",
            tier.name,
            tier.min_replicas,
            fragment.partition.label(),
            fragment.leaf,
            detail,
            fragment_bytes
        ));
    }
}

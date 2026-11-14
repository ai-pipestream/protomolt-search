//! The validated planning snapshot: fragments, nodes, and their digests.

use std::collections::BTreeMap;

use crate::sha256;

use super::observations::{
    check_observation_identity, observation_set_digest, CommittedCopy, CommittedView,
    PartitionIdentity, PartitionObservation,
};
use super::{
    hex16, put_str, put_u32, put_u64, NodeResidency, TierPolicy, ENCODING_VERSION,
    FRAGMENTS_DOMAIN, MAX_COHORT_LENGTH_MS, MAX_SNAPSHOT_FRAGMENTS, MAX_SNAPSHOT_NODES,
    MAX_SNAPSHOT_OBSERVATIONS, NODES_DOMAIN, PLAN_DOMAIN, PLAN_ENCODING_VERSION,
};

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
        let resource = (
            input.workspace.as_str(),
            input.collection.as_str(),
            input.derived_fingerprint,
        );
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

        // Derive fragments from the committed view; reject duplicate
        // requests rather than letting one shadow another.
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
            if (
                request.partition.workspace.as_str(),
                request.partition.collection.as_str(),
                request.partition.derived_fingerprint,
            ) != resource
            {
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
            fragments.push(derive_fragment(&request, &input)?);
        }

        // Observations: the store's ingest checks, re-verified, plus
        // incarnation currency and duplicate rejection.
        let mut seen_keys = std::collections::BTreeSet::new();
        for obs in &input.observations {
            if (
                obs.partition.workspace.as_str(),
                obs.partition.collection.as_str(),
                obs.partition.derived_fingerprint,
            ) != resource
            {
                return Err(format!(
                    "plan_tiers: observation of partition {} is outside the snapshot's resource",
                    obs.partition.label()
                ));
            }
            check_observation_identity(&input.committed, obs)?;
            match input.current_incarnations.get(&obs.reporter.node_id) {
                Some(current) if *current == obs.reporter.process_incarnation => {}
                Some(current) => {
                    return Err(format!(
                        "plan_tiers: node {} incarnation {} is superseded by {}",
                        obs.reporter.node_id,
                        hex16(&obs.reporter.process_incarnation),
                        hex16(current)
                    ));
                }
                None => {
                    return Err(format!(
                        "plan_tiers: node {} has no registered incarnation",
                        obs.reporter.node_id
                    ));
                }
            }
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

    /// The canonical digest of the node registry (domain
    /// `protomolt.capacity-plan-nodes.v1`), nodes in id order.
    fn nodes_digest(&self) -> [u8; 32] {
        let mut out = Vec::new();
        out.extend_from_slice(NODES_DOMAIN);
        put_u32(&mut out, ENCODING_VERSION);
        put_u32(&mut out, self.nodes.len() as u32);
        for (node_id, record) in &self.nodes {
            put_str(&mut out, node_id);
            out.push(record.residency.code());
            out.push(record.eligible as u8);
            put_str(&mut out, &record.failure_domain);
            put_u64(&mut out, record.capacity.total_bytes);
            put_u64(&mut out, record.capacity.resident_bytes);
        }
        sha256::digest(&out)
    }

    /// The canonical digest of the fragment records (domain
    /// `protomolt.capacity-plan-fragments.v1`), fragments in the
    /// constructor's canonical order.
    fn fragments_digest(&self) -> [u8; 32] {
        let mut out = Vec::new();
        out.extend_from_slice(FRAGMENTS_DOMAIN);
        put_u32(&mut out, ENCODING_VERSION);
        put_u32(&mut out, self.fragments.len() as u32);
        for fragment in &self.fragments {
            fragment.put_canonical(&mut out);
        }
        sha256::digest(&out)
    }

    /// §4, encoding version 2: `SHA-256(domain ‖ u32le(2) ‖ context fields
    /// ‖ cohort ‖ nodes subdigest ‖ fragments subdigest)`. Every effective
    /// input — the resolved limits, the node registry, and the fragment
    /// records included — changes this digest.
    pub fn plan_digest(&self) -> [u8; 32] {
        let mut out = Vec::new();
        out.extend_from_slice(PLAN_DOMAIN);
        put_u32(&mut out, PLAN_ENCODING_VERSION);
        put_u64(&mut out, self.planning_instant_unix_ms);
        put_u64(&mut out, self.max_moves);
        put_u64(&mut out, self.max_observation_age_ms);
        put_u64(&mut out, self.clock_skew_bound_ms);
        put_str(&mut out, &self.authority_id);
        out.extend_from_slice(&self.authority_incarnation);
        put_u64(&mut out, self.control_revision);
        put_u64(&mut out, self.policy_revision);
        out.extend_from_slice(&self.policy.fingerprint());
        put_u64(&mut out, self.observation_epoch);
        out.extend_from_slice(&observation_set_digest(&self.observations));
        put_u64(&mut out, self.topology_generation);
        out.extend_from_slice(&self.placement_tree_digest);
        put_str(&mut out, &self.workspace);
        put_str(&mut out, &self.collection);
        out.extend_from_slice(&self.derived_fingerprint);
        out.extend_from_slice(&self.provider_geometry_digest);
        put_u64(&mut out, self.cohort_length_ms);
        put_u64(&mut out, self.cohort_phase_unix_ms);
        out.extend_from_slice(&self.nodes_digest());
        out.extend_from_slice(&self.fragments_digest());
        sha256::digest(&out)
    }

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

/// Derive one fragment's committed facts from the committed view:
/// every shard with coverage of the leaf contributes its owner, the
/// pools must agree, and copies are the union in canonical order.
/// `complete` claims are then verified against the node registry:
/// Server-resident, eligible, and sharing one coverage digest.
fn derive_fragment(
    request: &FragmentRequest,
    input: &TierSnapshotInput,
) -> Result<FragmentRecord, String> {
    let mut pool: Option<Vec<String>> = None;
    let mut shard_owners = BTreeMap::new();
    let mut copies: Vec<CommittedCopy> = Vec::new();
    for (shard_id, shard) in &input.committed.shards {
        let Some(coverage) = shard.leaves.get(&request.leaf) else {
            continue;
        };
        let mut this_pool = coverage.pool.clone();
        this_pool.sort();
        match &pool {
            None => pool = Some(this_pool),
            Some(existing) if *existing != this_pool => {
                return Err(format!(
                    "plan_tiers: shard {} names a different pool for leaf {} than its siblings",
                    shard_id, request.leaf
                ));
            }
            _ => {}
        }
        shard_owners.insert(shard_id.clone(), coverage.owner_node.clone());
        for copy in &coverage.copies {
            if let Some(existing) = copies.iter().find(|existing| {
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
                copies.push(copy.clone());
            }
        }
    }
    let Some(pool) = pool else {
        return Err(format!(
            "plan_tiers: fragment ({}, gen {}, {}) has no committed coverage",
            request.partition.label(),
            request.topology_generation,
            request.leaf
        ));
    };
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
    Ok(FragmentRecord {
        partition: request.partition.clone(),
        topology_generation: request.topology_generation,
        leaf: request.leaf.clone(),
        pool,
        shard_owners,
        copies,
    })
}

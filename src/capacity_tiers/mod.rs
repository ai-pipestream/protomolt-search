//! Capacity tiers over declared hash partitions (`docs/capacity-tiers.md`):
//! bounded capacity observations and the deterministic tier dry run.
//!
//! This module is a pure library. It allocates no protocol fields, opens
//! no sockets, and performs no live coordinator lookups: the observation
//! store validates and retains per-(partition, shard, leaf) reports under
//! the cohort and identity rules of §3, and `plan_tiers` computes the
//! advisory plan of §4 from an immutable [`TierSnapshot`].
//!
//! The snapshot is the coordination seam with the control-plane track
//! (Fable). [`TierSnapshot`] is opaque: the only constructor is
//! [`TierSnapshot::validated`], which enforces once — for the store-fed
//! path and for any hand-built input — the full resource and owner/history
//! identity checks, source residency and node eligibility, canonical
//! ordering of every unordered input, duplicate rejection, and admission
//! caps. Fable owns how the committed topology, capacity reports,
//! revisions, and ownership epochs are persisted and allocated; node and
//! shard identity are the control plane's existing logical identities, and
//! the ownership epoch is a counter the authority advances on every
//! committed ownership transition, never a replacement identity.
//!
//! Every hashed value uses the canonical encoding of §5 (little-endian
//! integers, length-prefixed strings, fields in specification order), so
//! the digests here agree byte-for-byte with the independent Python oracle
//! `scripts/verify_capacity_tier_fixtures.py` and with the literals pinned
//! in §10 of the design document. The plan-input encoding is version 2:
//! it binds the cohort and canonical subdigests of the node registry and
//! the fragment records, so every effective input changes the digest.

use crate::sha256;
use std::collections::BTreeMap;

mod observations;
mod plan;
mod snapshot;
#[cfg(test)]
mod tests;

use observations::check_observation_identity;
pub use plan::plan_tiers;
pub use snapshot::observation_set_digest;

const POLICY_DOMAIN: &[u8] = b"protomolt.capacity-tier-policy.v1\0";
const OBSERVATIONS_DOMAIN: &[u8] = b"protomolt.capacity-observations.v1\0";
const PLAN_DOMAIN: &[u8] = b"protomolt.capacity-plan-input.v1\0";
const NODES_DOMAIN: &[u8] = b"protomolt.capacity-plan-nodes.v1\0";
const FRAGMENTS_DOMAIN: &[u8] = b"protomolt.capacity-plan-fragments.v1\0";
const ENCODING_VERSION: u32 = 1;
/// Plan-input encoding version 2: binds the cohort and the node/fragment
/// subdigests (the review of v1 found the gap).
const PLAN_ENCODING_VERSION: u32 = 2;

/// §5: a cohort window longer than 2^40 ms is refused.
const MAX_COHORT_LENGTH_MS: u64 = 1 << 40;
/// §3/§5: at most this many reports may feed one fragment's aggregate.
const MAX_REPORTS_PER_FRAGMENT: usize = 1 << 20;
/// Admission caps, enforced by the validated constructor.
const MAX_SNAPSHOT_OBSERVATIONS: usize = 1 << 20;
const MAX_SNAPSHOT_FRAGMENTS: usize = 1 << 16;
const MAX_SNAPSHOT_NODES: usize = 4096;
const MAX_POLICY_TIERS: usize = 64;
/// A replica floor is a small policy constant; a giant floor is a policy
/// error, not a work order (policy validation refuses it).
const MAX_MIN_REPLICAS: u32 = 1024;

fn put_u32(out: &mut Vec<u8>, n: u32) {
    out.extend_from_slice(&n.to_le_bytes());
}

fn put_u64(out: &mut Vec<u8>, n: u64) {
    out.extend_from_slice(&n.to_le_bytes());
}

fn put_str(out: &mut Vec<u8>, s: &str) {
    put_u32(out, s.len() as u32);
    out.extend_from_slice(s.as_bytes());
}

fn hex16(bytes: &[u8; 16]) -> String {
    let mut out = String::with_capacity(32);
    for b in bytes {
        use std::fmt::Write as _;
        write!(out, "{b:02x}").expect("writing to a String cannot fail");
    }
    out
}

/// The residency kind a node declares about itself. Only `Server` may
/// hold a verified complete copy, be a move candidate, or be named by a
/// tier. `Device` and `Unspecified` sources are never movable, and no
/// pool membership, Admin grant, or cluster trust overrides that (§7).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeResidency {
    Server,
    Device,
    Unspecified,
}

impl NodeResidency {
    fn code(self) -> u8 {
        match self {
            NodeResidency::Unspecified => 0,
            NodeResidency::Server => 1,
            NodeResidency::Device => 2,
        }
    }
}

/// One tier of a policy: residency kind, replica floor, workload band in
/// nanos (scans per second per resident byte, times 10^9), warmth bound.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapacityTier {
    pub name: String,
    pub residency: NodeResidency,
    pub min_replicas: u32,
    pub scans_per_byte_nanos_lo: u64,
    pub scans_per_byte_nanos_hi: u64,
    pub max_seconds_since_scan: u64,
}

/// A tier policy: tiers in precedence order. The list order is semantic
/// and is part of the policy's identity (§5).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TierPolicy {
    pub tiers: Vec<CapacityTier>,
}

impl TierPolicy {
    /// Validation at commit time (§5): unique names, `lo < hi`,
    /// `1 <= min_replicas <= 1024`, Server residency, bounded tier count.
    /// Refusals name the failing tier.
    pub fn validate(&self) -> Result<(), String> {
        if self.tiers.len() > MAX_POLICY_TIERS {
            return Err(format!(
                "tier policy: {} tiers is past the {MAX_POLICY_TIERS} bound",
                self.tiers.len()
            ));
        }
        let mut seen = std::collections::BTreeSet::new();
        for tier in &self.tiers {
            if !seen.insert(tier.name.as_str()) {
                return Err(format!(
                    "tier policy: duplicate tier name {:?}; names compare bytewise",
                    tier.name
                ));
            }
            if tier.residency != NodeResidency::Server {
                return Err(format!(
                    "tier policy: tier {:?} names a non-SERVER residency",
                    tier.name
                ));
            }
            if tier.min_replicas == 0 || tier.min_replicas > MAX_MIN_REPLICAS {
                return Err(format!(
                    "tier policy: tier {:?} floor {} is outside [1, {MAX_MIN_REPLICAS}]",
                    tier.name, tier.min_replicas
                ));
            }
            if tier.scans_per_byte_nanos_lo >= tier.scans_per_byte_nanos_hi {
                return Err(format!(
                    "tier policy: tier {:?} band [{}, {}) is empty or inverted",
                    tier.name, tier.scans_per_byte_nanos_lo, tier.scans_per_byte_nanos_hi
                ));
            }
        }
        Ok(())
    }

    /// The canonical bytes the fingerprint hashes (§5): the versioned
    /// definition with the computed fingerprint field excluded, tiers in
    /// declared (precedence) order.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(POLICY_DOMAIN);
        put_u32(&mut out, ENCODING_VERSION);
        put_u32(&mut out, self.tiers.len() as u32);
        for tier in &self.tiers {
            put_str(&mut out, &tier.name);
            out.push(tier.residency.code());
            put_u32(&mut out, tier.min_replicas);
            put_u64(&mut out, tier.scans_per_byte_nanos_lo);
            put_u64(&mut out, tier.scans_per_byte_nanos_hi);
            put_u64(&mut out, tier.max_seconds_since_scan);
        }
        out
    }

    pub fn fingerprint(&self) -> [u8; 32] {
        sha256::digest(&self.canonical_bytes())
    }
}

/// §2: a declared hash partition inside an explicit resource.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PartitionIdentity {
    pub workspace: String,
    pub collection: String,
    pub derived_fingerprint: [u8; 32],
    pub column: String,
    pub bucket: u64,
}

impl PartitionIdentity {
    fn label(&self) -> String {
        format!("{}={}", self.column, self.bucket)
    }

    fn put_canonical(&self, out: &mut Vec<u8>) {
        put_str(out, &self.workspace);
        put_str(out, &self.collection);
        out.extend_from_slice(&self.derived_fingerprint);
        put_str(out, &self.column);
        put_u64(out, self.bucket);
    }
}

/// §2/§3: a node process, pinned to one run of it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReporterIdentity {
    pub node_id: String,
    pub process_incarnation: [u8; 16],
}

/// §3: a shard's bytes as one committed installation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardRef {
    pub shard: String,
    pub source_generation: u64,
    pub storage_incarnation: [u8; 16],
    pub ownership_epoch: u64,
}

/// §3: one node's measured statement about one partition in one shard,
/// attributed to one placement leaf.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionObservation {
    pub partition: PartitionIdentity,
    pub reporter: ReporterIdentity,
    pub shard: ShardRef,
    pub topology_generation: u64,
    pub leaf: String,
    pub rows: u64,
    pub resident_bytes: u64,
    pub scans_observed: u64,
    pub scan_bytes: u64,
    pub queue_wait_p50_us: u64,
    pub queue_wait_p99_us: u64,
    pub samples: u32,
    pub last_scanned_unix_ms: u64,
    pub window_start_unix_ms: u64,
    pub window_end_unix_ms: u64,
}

/// §3: the stored observation key. Field declaration order is the sort
/// order, matching the canonical tuple in the design document.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ObsKey {
    pub workspace: String,
    pub collection: String,
    pub derived_fingerprint: [u8; 32],
    pub column: String,
    pub bucket: u64,
    pub topology_generation: u64,
    pub leaf: String,
    pub shard: String,
    pub source_generation: u64,
    pub ownership_epoch: u64,
    pub node_id: String,
    pub process_incarnation: [u8; 16],
    pub storage_incarnation: [u8; 16],
}

impl PartitionObservation {
    pub fn key(&self) -> ObsKey {
        ObsKey {
            workspace: self.partition.workspace.clone(),
            collection: self.partition.collection.clone(),
            derived_fingerprint: self.partition.derived_fingerprint,
            column: self.partition.column.clone(),
            bucket: self.partition.bucket,
            topology_generation: self.topology_generation,
            leaf: self.leaf.clone(),
            shard: self.shard.shard.clone(),
            source_generation: self.shard.source_generation,
            ownership_epoch: self.shard.ownership_epoch,
            node_id: self.reporter.node_id.clone(),
            process_incarnation: self.reporter.process_incarnation,
            storage_incarnation: self.shard.storage_incarnation,
        }
    }

    /// The canonical bytes of §3/§5, fields in specification order.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.partition.put_canonical(&mut out);
        put_u64(&mut out, self.topology_generation);
        put_str(&mut out, &self.leaf);
        put_str(&mut out, &self.shard.shard);
        put_u64(&mut out, self.shard.source_generation);
        put_u64(&mut out, self.shard.ownership_epoch);
        put_str(&mut out, &self.reporter.node_id);
        out.extend_from_slice(&self.reporter.process_incarnation);
        out.extend_from_slice(&self.shard.storage_incarnation);
        put_u64(&mut out, self.rows);
        put_u64(&mut out, self.resident_bytes);
        put_u64(&mut out, self.scans_observed);
        put_u64(&mut out, self.scan_bytes);
        put_u64(&mut out, self.queue_wait_p50_us);
        put_u64(&mut out, self.queue_wait_p99_us);
        put_u64(&mut out, self.last_scanned_unix_ms);
        put_u64(&mut out, self.window_start_unix_ms);
        put_u64(&mut out, self.window_end_unix_ms);
        put_u32(&mut out, self.samples);
        out
    }

    /// The measured fields after the key and window: what a conflicting
    /// retry would change (§3, E6).
    fn payload_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        put_u64(&mut out, self.rows);
        put_u64(&mut out, self.resident_bytes);
        put_u64(&mut out, self.scans_observed);
        put_u64(&mut out, self.scan_bytes);
        put_u64(&mut out, self.queue_wait_p50_us);
        put_u64(&mut out, self.queue_wait_p99_us);
        put_u64(&mut out, self.last_scanned_unix_ms);
        put_u32(&mut out, self.samples);
        out
    }

    /// The retained-byte charge: the store keeps both the cloned key and
    /// the observation, so every string is charged twice, by allocation
    /// capacity rather than length — a short string with a large capacity
    /// must not defeat the bound.
    fn heap_bytes(&self) -> usize {
        let twice = self.partition.workspace.capacity()
            + self.partition.collection.capacity()
            + self.partition.column.capacity()
            + self.leaf.capacity()
            + self.shard.shard.capacity()
            + self.reporter.node_id.capacity();
        336 + 2 * twice
    }
}

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
    fn resource_matches(&self, partition: &PartitionIdentity) -> bool {
        self.workspace == partition.workspace
            && self.collection == partition.collection
            && self.derived_fingerprint == partition.derived_fingerprint
    }
}

/// The result of an accepted report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestOutcome {
    /// The report changed the stored set; the observation epoch advanced.
    Landed,
    /// Byte-identical retry of the stored report; the set and the epoch
    /// are unchanged (§3, E6).
    Unchanged,
    /// A report for an older cohort window than the stored one; discarded
    /// without changing the set (§3).
    Superseded,
}

struct StoredObservation {
    obs: PartitionObservation,
    heap_bytes: usize,
}

/// §3: the bounded store of capacity observations. It enforces resource
/// binding and cohort alignment, identity equality against the committed
/// view, incarnation supersession, exact-retry idempotence,
/// conflicting-report refusals, and total retained-record, retained-byte,
/// and registered-node bounds. The observation epoch advances only on
/// transitions that change the stored set, with the increment checked
/// before any mutation.
pub struct ObservationStore {
    cohort_length_ms: u64,
    cohort_phase_unix_ms: u64,
    max_total_records: usize,
    max_total_bytes: usize,
    max_registered_nodes: usize,
    committed: CommittedView,
    current_incarnation: BTreeMap<String, [u8; 16]>,
    records: BTreeMap<ObsKey, StoredObservation>,
    epoch: u64,
    retained_bytes: usize,
}

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

fn put_copy(out: &mut Vec<u8>, copy: &CommittedCopy) {
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
    fn free_bytes(&self) -> u64 {
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
    planning_instant_unix_ms: u64,
    max_moves: u64,
    max_observation_age_ms: u64,
    clock_skew_bound_ms: u64,
    authority_id: String,
    authority_incarnation: [u8; 16],
    control_revision: u64,
    policy_revision: u64,
    policy: TierPolicy,
    observation_epoch: u64,
    topology_generation: u64,
    placement_tree_digest: [u8; 32],
    workspace: String,
    collection: String,
    derived_fingerprint: [u8; 32],
    provider_geometry_digest: [u8; 32],
    cohort_length_ms: u64,
    cohort_phase_unix_ms: u64,
    nodes: BTreeMap<String, NodeRecord>,
    observations: Vec<PartitionObservation>,
    fragments: Vec<FragmentRecord>,
}

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

type FragmentKey = (PartitionIdentity, u64, String);

struct FragmentReports<'a> {
    /// Current-cohort reports by (shard, node).
    current: BTreeMap<(String, String), &'a PartitionObservation>,
}

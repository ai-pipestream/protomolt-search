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
//! (Fable): this module defines the typed input it plans from, and Fable
//! owns how the committed topology, capacity reports, revisions, and
//! ownership epochs are persisted and allocated. Node and shard identity
//! are the control plane's existing logical identities; the ownership
//! epoch is a counter the authority advances on every committed ownership
//! transition, never a replacement identity.
//!
//! Every hashed value uses the canonical encoding of §5 (little-endian
//! integers, length-prefixed strings, fields in specification order), so
//! the digests here agree byte-for-byte with the independent Python oracle
//! `scripts/verify_capacity_tier_fixtures.py` and with the literals pinned
//! in §10 of the design document.

use crate::sha256;
use std::collections::BTreeMap;

const POLICY_DOMAIN: &[u8] = b"protomolt.capacity-tier-policy.v1\0";
const OBSERVATIONS_DOMAIN: &[u8] = b"protomolt.capacity-observations.v1\0";
const PLAN_DOMAIN: &[u8] = b"protomolt.capacity-plan-input.v1\0";
const ENCODING_VERSION: u32 = 1;

/// §5: a cohort window longer than 2^40 ms is refused.
const MAX_COHORT_LENGTH_MS: u64 = 1 << 40;
/// §3/§5: at most this many reports may feed one fragment's aggregate.
const MAX_REPORTS_PER_FRAGMENT: usize = 1 << 20;

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

/// The residency kind a tier may require. Only `Server` is a valid tier
/// residency; device-local sources are excluded from every plan, and an
/// unspecified residency is never assumed movable (§7).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeResidency {
    Server,
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
    /// `min_replicas >= 1`. Refusals name the failing tier.
    pub fn validate(&self) -> Result<(), String> {
        let mut seen = std::collections::BTreeSet::new();
        for tier in &self.tiers {
            if !seen.insert(tier.name.as_str()) {
                return Err(format!(
                    "tier policy: duplicate tier name {:?}; names compare bytewise",
                    tier.name
                ));
            }
            if tier.min_replicas == 0 {
                return Err(format!(
                    "tier policy: tier {:?} floor is 0; min_replicas is >= 1",
                    tier.name
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
            let NodeResidency::Server = tier.residency;
            out.push(1u8);
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
        put_str(&mut out, &self.partition.workspace);
        put_str(&mut out, &self.partition.collection);
        out.extend_from_slice(&self.partition.derived_fingerprint);
        put_str(&mut out, &self.partition.column);
        put_u64(&mut out, self.partition.bucket);
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

    /// The retained-byte estimate the store's total bound charges.
    fn heap_bytes(&self) -> usize {
        160 + self.partition.workspace.len()
            + self.partition.collection.len()
            + self.partition.column.len()
            + self.leaf.len()
            + self.shard.shard.len()
            + self.reporter.node_id.len()
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
    /// replica test). A partial copy is evidence of nothing but rows.
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

/// §3: the authority's committed view, consulted at report ingest. All
/// comparisons are equality checks in both directions: a behind-or-ahead
/// value names bytes other than the committed ones and is refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedView {
    pub topology_generation: u64,
    pub shards: BTreeMap<String, CommittedShard>,
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

/// §3: the bounded store of capacity observations. It enforces cohort
/// alignment, identity equality against the committed view, incarnation
/// supersession, exact-retry idempotence, conflicting-report refusals,
/// and total retained-record and retained-byte bounds. The observation
/// epoch advances only on transitions that change the stored set.
pub struct ObservationStore {
    cohort_length_ms: u64,
    cohort_phase_unix_ms: u64,
    max_total_records: usize,
    max_total_bytes: usize,
    committed: CommittedView,
    current_incarnation: BTreeMap<String, [u8; 16]>,
    records: BTreeMap<ObsKey, StoredObservation>,
    epoch: u64,
    retained_bytes: usize,
}

impl ObservationStore {
    pub fn new(
        cohort_length_ms: u64,
        cohort_phase_unix_ms: u64,
        max_total_records: usize,
        max_total_bytes: usize,
        committed: CommittedView,
    ) -> Result<Self, String> {
        if cohort_length_ms == 0 || cohort_length_ms > MAX_COHORT_LENGTH_MS {
            return Err(format!(
                "report_shard: cohort length {cohort_length_ms} ms is outside (0, 2^40]"
            ));
        }
        Ok(Self {
            cohort_length_ms,
            cohort_phase_unix_ms,
            max_total_records,
            max_total_bytes,
            committed,
            current_incarnation: BTreeMap::new(),
            records: BTreeMap::new(),
            epoch: 0,
            retained_bytes: 0,
        })
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn cohort_length_ms(&self) -> u64 {
        self.cohort_length_ms
    }

    pub fn cohort_phase_unix_ms(&self) -> u64 {
        self.cohort_phase_unix_ms
    }

    /// Register the node's current process incarnation. A new incarnation
    /// supersedes the old one: every stored observation from the old
    /// incarnation is dropped in the same committed transition, and the
    /// epoch advances once if anything was dropped (§3, E5).
    pub fn register_incarnation(&mut self, node_id: &str, incarnation: [u8; 16]) {
        let previous = self
            .current_incarnation
            .insert(node_id.to_string(), incarnation);
        if previous == Some(incarnation) {
            return;
        }
        let dropped =
            self.drop_where(|key| key.node_id == node_id && key.process_incarnation != incarnation);
        if dropped > 0 {
            self.epoch += 1;
        }
    }

    /// The current-cohort rule applied to the wall clock by the authority's
    /// scheduler: remove every observation whose window end is more than
    /// `max_age_ms` before `now`, advancing the epoch once if the set
    /// changed. Expiry is a committed transition, never a side effect of
    /// planning (§3/§4).
    pub fn commit_expiry(&mut self, now_unix_ms: u64, max_age_ms: u64) -> usize {
        let doomed: Vec<ObsKey> = self
            .records
            .iter()
            .filter(|(_, stored)| {
                now_unix_ms.saturating_sub(stored.obs.window_end_unix_ms) > max_age_ms
            })
            .map(|(key, _)| key.clone())
            .collect();
        let dropped = doomed.len();
        for key in doomed {
            if let Some(stored) = self.records.remove(&key) {
                self.retained_bytes -= stored.heap_bytes;
            }
        }
        if dropped > 0 {
            self.epoch += 1;
        }
        dropped
    }

    fn drop_where(&mut self, keep_dropping: impl Fn(&ObsKey) -> bool) -> usize {
        let doomed: Vec<ObsKey> = self
            .records
            .keys()
            .filter(|key| keep_dropping(key))
            .cloned()
            .collect();
        let dropped = doomed.len();
        for key in doomed {
            if let Some(stored) = self.records.remove(&key) {
                self.retained_bytes -= stored.heap_bytes;
            }
        }
        dropped
    }

    /// Validate and store one report (§3). Refusals name the route, the
    /// field, the carried value, and the committed value.
    pub fn ingest(&mut self, obs: PartitionObservation) -> Result<IngestOutcome, String> {
        let key = obs.key();

        // Cohort alignment, before any identity work (E10).
        let window_len = obs
            .window_end_unix_ms
            .checked_sub(obs.window_start_unix_ms)
            .ok_or_else(|| {
                format!(
                    "report_shard: window [{}, {}) is inverted",
                    obs.window_start_unix_ms, obs.window_end_unix_ms
                )
            })?;
        if window_len != self.cohort_length_ms
            || obs.window_start_unix_ms < self.cohort_phase_unix_ms
            || !(obs.window_start_unix_ms - self.cohort_phase_unix_ms)
                .is_multiple_of(self.cohort_length_ms)
        {
            return Err(format!(
                "report_shard: window [{}, {}) is not a cohort window (length {} ms, phase {})",
                obs.window_start_unix_ms,
                obs.window_end_unix_ms,
                self.cohort_length_ms,
                self.cohort_phase_unix_ms
            ));
        }

        // Impossible states are refusals, never zeroes (§3).
        if obs.rows > 0 && obs.resident_bytes == 0 {
            return Err(format!(
                "report_shard: {} on {} reports {} rows and 0 resident bytes",
                obs.partition.label(),
                obs.reporter.node_id,
                obs.rows
            ));
        }
        if obs.scans_observed > 0 && obs.resident_bytes == 0 {
            return Err(format!(
                "report_shard: {} on {} reports {} scans of 0 resident bytes",
                obs.partition.label(),
                obs.reporter.node_id,
                obs.scans_observed
            ));
        }

        // Identity equality against the committed view, both directions.
        if obs.topology_generation != self.committed.topology_generation {
            return Err(format!(
                "report_shard: topology_generation {} is not the committed {}",
                obs.topology_generation, self.committed.topology_generation
            ));
        }
        let committed_shard = self.committed.shards.get(&obs.shard.shard).ok_or_else(|| {
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
                obs.shard.shard, obs.leaf, self.committed.topology_generation
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

        // Incarnation: only the node's current process may report (E5).
        match self.current_incarnation.get(&obs.reporter.node_id) {
            Some(current) if *current == obs.reporter.process_incarnation => {}
            Some(current) => {
                return Err(format!(
                    "report_shard: node {} incarnation {} is superseded by {}",
                    obs.reporter.node_id,
                    hex16(&obs.reporter.process_incarnation),
                    hex16(current)
                ));
            }
            None => {
                return Err(format!(
                    "report_shard: node {} has not registered an incarnation",
                    obs.reporter.node_id
                ));
            }
        }

        // Retry and supersession rules against the stored record.
        if let Some(stored) = self.records.get(&key) {
            if stored.obs.window_end_unix_ms == obs.window_end_unix_ms {
                if stored.obs.payload_bytes() == obs.payload_bytes() {
                    return Ok(IngestOutcome::Unchanged);
                }
                return Err(format!(
                    "report_shard: observation for ({}, gen {}, {}, {}, gen {}) window [{}, {}) from {}/{} conflicts with the stored report",
                    obs.partition.label(),
                    obs.topology_generation,
                    obs.leaf,
                    obs.shard.shard,
                    obs.shard.source_generation,
                    obs.window_start_unix_ms,
                    obs.window_end_unix_ms,
                    obs.reporter.node_id,
                    hex16(&obs.reporter.process_incarnation)
                ));
            }
            if obs.window_end_unix_ms < stored.obs.window_end_unix_ms {
                return Ok(IngestOutcome::Superseded);
            }
        }

        // Bounds: total retained records and bytes, not only per-fragment
        // fan-in. Replacing an existing key first frees its charge.
        let heap_bytes = obs.heap_bytes();
        let replacing = self.records.get(&key).map(|s| s.heap_bytes).unwrap_or(0);
        if replacing == 0 && self.records.len() >= self.max_total_records {
            return Err(format!(
                "report_shard: the observation store holds the bound of {} records",
                self.max_total_records
            ));
        }
        if self.retained_bytes - replacing + heap_bytes > self.max_total_bytes {
            return Err(format!(
                "report_shard: the observation store would exceed its bound of {} retained bytes",
                self.max_total_bytes
            ));
        }

        self.retained_bytes = self.retained_bytes - replacing + heap_bytes;
        self.records
            .insert(key, StoredObservation { obs, heap_bytes });
        self.epoch += 1;
        Ok(IngestOutcome::Landed)
    }

    /// The stored set in canonical key order, all cohorts.
    pub fn observations(&self) -> Vec<&PartitionObservation> {
        self.records.values().map(|stored| &stored.obs).collect()
    }

    /// §4: the observation-set digest over the stored set in canonical
    /// key order (domain `protomolt.capacity-observations.v1`).
    pub fn observation_set_digest(&self) -> [u8; 32] {
        let observations: Vec<PartitionObservation> = self
            .records
            .values()
            .map(|stored| stored.obs.clone())
            .collect();
        observation_set_digest(&observations)
    }
}

/// §2: a fragment as the planner needs it: the committed coverage, the
/// shard owners whose reports count bytes, and the verified copies the
/// replica floor counts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FragmentRecord {
    pub partition: PartitionIdentity,
    pub topology_generation: u64,
    pub leaf: String,
    pub pool: Vec<String>,
    /// shard → committed owner node; owner reports are the only byte
    /// sources (§3's deduplication rule).
    pub shard_owners: BTreeMap<String, String>,
    /// Every committed copy, complete and partial; the floor counts
    /// complete copies in distinct failure domains.
    pub copies: Vec<CommittedCopy>,
}

impl FragmentRecord {
    fn label(&self) -> String {
        format!(
            "({}, gen {}, {})",
            self.partition.label(),
            self.topology_generation,
            self.leaf
        )
    }

    fn complete_copies(&self) -> impl Iterator<Item = &CommittedCopy> {
        self.copies.iter().filter(|copy| copy.complete)
    }
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

/// §4: the frozen input snapshot the plan is a pure function of. This is
/// the typed coordination seam with the control-plane track: whoever
/// persists committed state builds this value; `plan_tiers` consumes it
/// and nothing else. It performs no I/O and no live lookups.
#[derive(Debug, Clone)]
pub struct TierSnapshot {
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
    /// Committed node → failure-domain map from the provider geometry;
    /// destination selection needs it for the §7 failure-domain rule.
    pub node_domains: BTreeMap<String, String>,
    /// The stored observation set in canonical key order, all cohorts.
    pub observations: Vec<PartitionObservation>,
    /// Every fragment the policy covers, in canonical partition order.
    pub fragments: Vec<FragmentRecord>,
    pub capacity: BTreeMap<String, NodeCapacity>,
}

impl TierSnapshot {
    /// §4: `SHA-256(domain ‖ version ‖ context fields in order)`. The
    /// digest binds every effective input, the resolved limits included.
    pub fn plan_digest(&self) -> [u8; 32] {
        let mut out = Vec::new();
        out.extend_from_slice(PLAN_DOMAIN);
        put_u32(&mut out, ENCODING_VERSION);
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
        sha256::digest(&out)
    }

    /// The cohort window the plan aggregates: the latest cohort window
    /// fully closed at the frozen instant (§3). A planning instant
    /// before the phase is a refusal.
    pub fn current_cohort(&self) -> Result<(u64, u64), String> {
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

/// The observation-set digest over an explicit set in canonical key
/// order — the free-function form used by the snapshot, which does not
/// hold the store.
pub fn observation_set_digest(observations: &[PartitionObservation]) -> [u8; 32] {
    let mut keyed: Vec<(ObsKey, &PartitionObservation)> =
        observations.iter().map(|obs| (obs.key(), obs)).collect();
    keyed.sort_by(|a, b| a.0.cmp(&b.0));
    let mut out = Vec::new();
    out.extend_from_slice(OBSERVATIONS_DOMAIN);
    put_u32(&mut out, ENCODING_VERSION);
    put_u32(&mut out, keyed.len() as u32);
    for (_, obs) in keyed {
        out.extend_from_slice(&obs.canonical_bytes());
    }
    sha256::digest(&out)
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

/// §5's exact rate: `floor(S · 10^12 / (W · B))` in `u128`. With §3's
/// bounds the arithmetic cannot overflow; the caller guarantees `B > 0`.
fn rate_nanos(scans: u128, window_ms: u64, resident_bytes: u128) -> u64 {
    let numerator = scans * 1_000_000_000_000u128;
    let denominator = (window_ms as u128) * resident_bytes;
    u64::try_from(numerator / denominator).unwrap_or(u64::MAX)
}

/// §3/§5's fan-in bound.
fn ensure_fan_in(count: usize) -> Result<(), String> {
    if count > MAX_REPORTS_PER_FRAGMENT {
        return Err(format!(
            "plan_tiers: fragment aggregates {count} reports, past the 2^20 fan-in bound"
        ));
    }
    Ok(())
}

struct FragmentReports<'a> {
    /// Current-cohort reports by (shard, node).
    current: BTreeMap<(String, String), &'a PartitionObservation>,
}

/// §4: the deterministic dry run. Pure function of the snapshot; the
/// same snapshot yields the same response, bitwise.
pub fn plan_tiers(snapshot: &TierSnapshot) -> Result<PlanTiersResponse, String> {
    snapshot.policy.validate()?;
    let t = snapshot.planning_instant_unix_ms;
    let (w0, w1) = snapshot.current_cohort()?;

    // Index the reports of covered fragments and apply the skew rule to
    // every stored report the plan could read (§4, E4).
    let covered: std::collections::BTreeSet<(&str, u64, &str)> = snapshot
        .fragments
        .iter()
        .map(|f| {
            (
                f.partition.column.as_str(),
                f.partition.bucket,
                f.leaf.as_str(),
            )
        })
        .collect();
    let mut reports: BTreeMap<(String, u64, String), FragmentReports> = BTreeMap::new();
    for obs in &snapshot.observations {
        if obs.window_end_unix_ms > t + snapshot.clock_skew_bound_ms {
            return Err(format!(
                "plan_tiers: observation from node {} ends {} ms in the future, past the {} ms skew bound",
                obs.reporter.node_id,
                obs.window_end_unix_ms - t,
                snapshot.clock_skew_bound_ms
            ));
        }
        if !covered.contains(&(
            obs.partition.column.as_str(),
            obs.partition.bucket,
            obs.leaf.as_str(),
        )) {
            continue;
        }
        if obs.window_start_unix_ms != w0 || obs.window_end_unix_ms != w1 {
            continue;
        }
        reports
            .entry((
                obs.partition.column.clone(),
                obs.partition.bucket,
                obs.leaf.clone(),
            ))
            .or_insert_with(|| FragmentReports {
                current: BTreeMap::new(),
            })
            .current
            .insert((obs.shard.shard.clone(), obs.reporter.node_id.clone()), obs);
    }

    let policy_fingerprint = snapshot.policy.fingerprint();
    let plan_digest = snapshot.plan_digest();
    let mut placements = Vec::new();
    let mut moves: Vec<TierMove> = Vec::new();
    let mut refusals = Vec::new();
    let mut projected_free: BTreeMap<String, u64> = snapshot
        .capacity
        .iter()
        .map(|(node, cap)| (node.clone(), cap.free_bytes()))
        .collect();
    let mut planned_onto: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();

    'fragments: for fragment in &snapshot.fragments {
        let empty = FragmentReports {
            current: BTreeMap::new(),
        };
        let frag_reports = reports
            .get(&(
                fragment.partition.column.clone(),
                fragment.partition.bucket,
                fragment.leaf.clone(),
            ))
            .unwrap_or(&empty);

        // Coverage: every shard's committed owner must have a current
        // report (unknown is not idle, E3), and so must every complete
        // copy (§3's freshness rule).
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
            match frag_reports
                .current
                .get(&(shard.to_string(), node.to_string()))
            {
                None => {
                    return Err(format!(
                        "plan_tiers: partition {} on node {} has no current observation; unknown is not idle",
                        fragment.partition.label(),
                        node
                    ));
                }
                Some(obs) => {
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
            }
        }

        // Aggregate (§3): owner-only bytes, scans over every reporter,
        // warmth as the maximum, checked fan-in.
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
        let mut figures = Vec::new();
        for ((shard, node), obs) in &frag_reports.current {
            scans += obs.scans_observed as u128;
            warmth = warmth.max(obs.last_scanned_unix_ms);
            figures.push(ReporterFigures {
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
        let rate = rate_nanos(scans, snapshot.cohort_length_ms, bytes);

        // Classify (§5): first tier in precedence order whose band and
        // warmth bound the fragment satisfies.
        let mut classified: Option<&CapacityTier> = None;
        let mut unclassified_reason = None;
        'tiers: for tier in &snapshot.policy.tiers {
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
                    continue 'tiers;
                }
                let age = t.saturating_sub(warmth);
                if age > bound_ms {
                    unclassified_reason = Some(format!(
                        "tier {:?} needs a scan within {} s; the last was {} ms ago",
                        tier.name, tier.max_seconds_since_scan, age
                    ));
                    continue 'tiers;
                }
            }
            classified = Some(tier);
            break;
        }
        let tier_name = classified.map(|tier| tier.name.clone());
        if tier_name.is_none() && unclassified_reason.is_none() {
            unclassified_reason = Some(format!("rate {rate} nanos is outside every band"));
        }

        let mut complete_replicas: Vec<CommittedCopy> =
            fragment.complete_copies().cloned().collect();
        complete_replicas.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        placements.push(FragmentPlacement {
            fragment: fragment.clone(),
            classified_tier: tier_name.clone(),
            unclassified_reason,
            rate_nanos: rate,
            rows: u64::try_from(rows).map_err(|_| {
                format!(
                    "plan_tiers: fragment {} row count overflows u64",
                    fragment.label()
                )
            })?,
            resident_bytes: u64::try_from(bytes).map_err(|_| {
                format!(
                    "plan_tiers: fragment {} byte count overflows u64",
                    fragment.label()
                )
            })?,
            last_scanned_unix_ms: warmth,
            complete_replicas: complete_replicas.clone(),
            reporters: figures,
        });

        let Some(tier) = classified else {
            continue 'fragments;
        };

        // Violations (§4 step 2): floor first, then residency. The floor
        // counts complete copies in distinct failure domains (§2).
        let mut seen_domains = std::collections::BTreeSet::new();
        let effective = fragment
            .complete_copies()
            .filter(|copy| seen_domains.insert(copy.failure_domain.as_str()))
            .count() as u32;
        let mut violation_sources: Vec<CommittedCopy> = Vec::new();
        if effective < tier.min_replicas {
            if fragment.complete_copies().next().is_none() {
                refusals.push(format!(
                    "plan_tiers: fragment {} has 0 complete replicas ({} partial); tier {:?} floor is {}",
                    fragment.label(),
                    fragment.copies.iter().filter(|c| !c.complete).count(),
                    tier.name,
                    tier.min_replicas
                ));
                continue 'fragments;
            }
            let source = fragment
                .complete_copies()
                .min_by(|a, b| a.node_id.cmp(&b.node_id))
                .expect("a complete copy exists")
                .clone();
            for _ in effective..tier.min_replicas {
                violation_sources.push(source.clone());
            }
        }
        for copy in fragment
            .complete_copies()
            .filter(|copy| !fragment.pool.contains(&copy.node_id))
        {
            violation_sources.push(copy.clone());
        }

        // Repair (§4 steps 3-5): candidates inside the pool, domain-
        // distinct from every complete copy at each intermediate step
        // (§7), capacity-checked against projected free bytes.
        let mut held_nodes: std::collections::BTreeSet<String> = fragment
            .complete_copies()
            .map(|copy| copy.node_id.clone())
            .collect();
        let mut held_domains: std::collections::BTreeSet<String> = fragment
            .complete_copies()
            .map(|copy| copy.failure_domain.clone())
            .collect();
        let fragment_bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        for source in violation_sources {
            if moves.len() as u64 >= snapshot.max_moves {
                refusals.push(format!(
                    "plan_tiers: max_moves {} reached; a violation of fragment {} is left unplanned",
                    snapshot.max_moves,
                    fragment.label()
                ));
                continue 'fragments;
            }
            let mut candidates: Vec<&String> = fragment
                .pool
                .iter()
                .filter(|node| !held_nodes.contains(node.as_str()))
                .filter(|node| {
                    snapshot
                        .node_domains
                        .get(node.as_str())
                        .map(|domain| !held_domains.contains(domain))
                        .unwrap_or(false)
                })
                .collect();
            candidates.sort_by(|a, b| {
                let free_a = projected_free.get(a.as_str()).copied().unwrap_or(0);
                let free_b = projected_free.get(b.as_str()).copied().unwrap_or(0);
                free_b.cmp(&free_a).then_with(|| a.cmp(b))
            });
            let destination = candidates
                .iter()
                .find(|node| {
                    projected_free.get(node.as_str()).copied().unwrap_or(0) >= fragment_bytes
                })
                .map(|node| (*node).clone());
            let Some(destination) = destination else {
                let mut detail_nodes: Vec<&String> = candidates.clone();
                detail_nodes.sort();
                let mut detail = String::new();
                for (i, node) in detail_nodes.iter().enumerate() {
                    let free = projected_free.get(node.as_str()).copied().unwrap_or(0);
                    if i > 0 {
                        detail.push_str(", ");
                    }
                    if planned_onto.contains(node.as_str()) {
                        detail.push_str(&format!(
                            "{node} has {free} free after earlier planned moves"
                        ));
                    } else {
                        detail.push_str(&format!("{node} has {free} free"));
                    }
                }
                refusals.push(format!(
                    "plan_tiers: tier {:?} floor is {} for partition {}, but no eligible destination in leaf {} has capacity: {}, the fragment needs {}",
                    tier.name,
                    tier.min_replicas,
                    fragment.partition.label(),
                    fragment.leaf,
                    detail,
                    fragment_bytes
                ));
                continue;
            };
            *projected_free.entry(destination.clone()).or_insert(0) -= fragment_bytes;
            planned_onto.insert(destination.clone());
            held_nodes.insert(destination.clone());
            if let Some(domain) = snapshot.node_domains.get(&destination) {
                held_domains.insert(domain.clone());
            }
            moves.push(TierMove {
                fragment: fragment.clone(),
                source: source.clone(),
                destination_node: destination,
                from_tier: tier.name.clone(),
                to_tier: tier.name.clone(),
                bytes: fragment_bytes,
                plan_digest,
                control_revision: snapshot.control_revision,
                observation_epoch: snapshot.observation_epoch,
                policy_fingerprint,
            });
        }
    }

    Ok(PlanTiersResponse {
        plan_digest,
        policy_fingerprint,
        observation_epoch: snapshot.observation_epoch,
        control_revision: snapshot.control_revision,
        placements,
        moves,
        refusals,
    })
}

impl PlanTiersResponse {
    /// Deterministic serialized form for the byte-identity obligation of
    /// §4's proof test: two planner instances given the same snapshot
    /// must emit identical bytes. This is the implementation's own
    /// canonical form (not a §5-hashed value).
    pub fn canonical_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&self.plan_digest);
        out.extend_from_slice(&self.policy_fingerprint);
        put_u64(&mut out, self.observation_epoch);
        put_u64(&mut out, self.control_revision);
        put_u32(&mut out, self.placements.len() as u32);
        for placement in &self.placements {
            put_fragment(&mut out, &placement.fragment);
            match &placement.classified_tier {
                Some(tier) => {
                    out.push(1);
                    put_str(&mut out, tier);
                }
                None => out.push(0),
            }
            match &placement.unclassified_reason {
                Some(reason) => {
                    out.push(1);
                    put_str(&mut out, reason);
                }
                None => out.push(0),
            }
            put_u64(&mut out, placement.rate_nanos);
            put_u64(&mut out, placement.rows);
            put_u64(&mut out, placement.resident_bytes);
            put_u64(&mut out, placement.last_scanned_unix_ms);
            put_u32(&mut out, placement.complete_replicas.len() as u32);
            for copy in &placement.complete_replicas {
                put_copy(&mut out, copy);
            }
            put_u32(&mut out, placement.reporters.len() as u32);
            for reporter in &placement.reporters {
                put_str(&mut out, &reporter.node_id);
                put_str(&mut out, &reporter.shard);
                put_u64(&mut out, reporter.scans_observed);
                match reporter.queue_wait_p50_us {
                    Some(v) => {
                        out.push(1);
                        put_u64(&mut out, v);
                    }
                    None => out.push(0),
                }
                match reporter.queue_wait_p99_us {
                    Some(v) => {
                        out.push(1);
                        put_u64(&mut out, v);
                    }
                    None => out.push(0),
                }
                put_u32(&mut out, reporter.samples);
                put_u64(&mut out, reporter.last_scanned_unix_ms);
            }
        }
        put_u32(&mut out, self.moves.len() as u32);
        for mv in &self.moves {
            put_fragment(&mut out, &mv.fragment);
            put_copy(&mut out, &mv.source);
            put_str(&mut out, &mv.destination_node);
            put_str(&mut out, &mv.from_tier);
            put_str(&mut out, &mv.to_tier);
            put_u64(&mut out, mv.bytes);
            out.extend_from_slice(&mv.plan_digest);
            put_u64(&mut out, mv.control_revision);
            put_u64(&mut out, mv.observation_epoch);
            out.extend_from_slice(&mv.policy_fingerprint);
        }
        put_u32(&mut out, self.refusals.len() as u32);
        for refusal in &self.refusals {
            put_str(&mut out, refusal);
        }
        out
    }
}

fn put_fragment(out: &mut Vec<u8>, fragment: &FragmentRecord) {
    put_str(out, &fragment.partition.workspace);
    put_str(out, &fragment.partition.collection);
    out.extend_from_slice(&fragment.partition.derived_fingerprint);
    put_str(out, &fragment.partition.column);
    put_u64(out, fragment.partition.bucket);
    put_u64(out, fragment.topology_generation);
    put_str(out, &fragment.leaf);
    put_u32(out, fragment.pool.len() as u32);
    for node in &fragment.pool {
        put_str(out, node);
    }
    put_u32(out, fragment.shard_owners.len() as u32);
    for (shard, owner) in &fragment.shard_owners {
        put_str(out, shard);
        put_str(out, owner);
    }
    put_u32(out, fragment.copies.len() as u32);
    for copy in &fragment.copies {
        put_copy(out, copy);
    }
}

fn put_copy(out: &mut Vec<u8>, copy: &CommittedCopy) {
    put_str(out, &copy.node_id);
    out.extend_from_slice(&copy.storage_incarnation);
    put_str(out, &copy.failure_domain);
    out.push(copy.complete as u8);
    out.extend_from_slice(&copy.coverage_digest);
}

#[cfg(test)]
mod tests {
    use super::*;

    const T: u64 = 1_788_868_800_000; // 2026-09-08T12:00:00Z
    const COHORT: u64 = 600_000;
    const W0: u64 = T - COHORT;
    const W1: u64 = T;

    const DECL_FP_HEX: &str = "34386b557959dd356964202d4a1fd062671e5ffac646dca4fe48421df09826d0";
    const POLICY_FP_HEX: &str = "5858b135aed48c2bbc1cc5bd0accec509eee415ea29929c2dd29908e76e708eb";
    const POLICY_SWAPPED_FP_HEX: &str =
        "449751c3e2d50ec0177958554e92c6ea3403a82139b6e6aecc3223f45973a2e7";
    const OBS_A_HEX: &str = "33d9878d3941dbc85ffddd22d4b739162bae935dd5476521a5f0a1439ab965e3";
    const PLAN_A_HEX: &str = "2a4ba9094445438e21bbf888a5e22569c5cfbbada571cfd856823edb20102a4f";
    const OBS_M_HEX: &str = "236d6740a5d5eaec435bb2b6c4b85644ed437ff87b40fd6b9150579ead21f6e1";
    const PLAN_M_HEX: &str = "96f514b75b2c29dcf708ddcdb558d69628f01e1280191bae947aa9182e81d48e";
    const COV_L4_HEX: &str = "0dbf795457d54641dc05c4e7d679f8a1988de734d6f45594079ed463e02ca681";
    const COV_L7_HEX: &str = "c06e53ec1642080f0acda4e03305040364fd96cbbeb37cd3106f2adc99ac18e0";

    fn inc(byte: u8) -> [u8; 16] {
        let mut out = [0u8; 16];
        out[15] = byte;
        out
    }

    fn hex32(bytes: &[u8; 32]) -> String {
        sha256::to_hex(bytes)
    }

    fn decl_fp() -> [u8; 32] {
        sha256::digest(b"key_bucket = hash.fnv64(stable_key()) % 64u")
    }

    fn tier(name: &str, min_replicas: u32, lo: u64, hi: u64, warmth: u64) -> CapacityTier {
        CapacityTier {
            name: name.to_string(),
            residency: NodeResidency::Server,
            min_replicas,
            scans_per_byte_nanos_lo: lo,
            scans_per_byte_nanos_hi: hi,
            max_seconds_since_scan: warmth,
        }
    }

    fn policy_p() -> TierPolicy {
        TierPolicy {
            tiers: vec![
                tier("hot", 3, 100, 1_000_000_000_000, 0),
                tier("warm", 2, 1, 100, 86_400),
                tier("archive", 2, 0, 1, 0),
            ],
        }
    }

    fn partition(bucket: u64) -> PartitionIdentity {
        PartitionIdentity {
            workspace: "ws-court".to_string(),
            collection: "cases".to_string(),
            derived_fingerprint: decl_fp(),
            column: "key_bucket".to_string(),
            bucket,
        }
    }

    fn cov_l4() -> [u8; 32] {
        sha256::digest(
            b"example manifest: fragment (key_bucket=7, gen 9, L4), source generation 3, rows 1000000, bytes 268435456",
        )
    }

    fn cov_l7() -> [u8; 32] {
        sha256::digest(
            b"example manifest: fragment (key_bucket=7, gen 9, L7), source generation 3, rows 500000, bytes 134217728",
        )
    }

    fn tree_digest() -> [u8; 32] {
        sha256::digest(
            b"example placement tree: gen 9, leaf L4 = {krick-1, pi5v1, pi5v3}, leaf L7 = {krick-1, pi5v1}",
        )
    }

    fn prov_digest() -> [u8; 32] {
        sha256::digest(
            b"example provider geometry: turbovec 64-shard mapped image; committed free bytes: krick-1=26843545600, pi5v1=16106127360, pi5v3=0",
        )
    }

    struct CopySpec {
        node: &'static str,
        proc_inc: u8,
        stor_inc: u8,
        domain: &'static str,
    }

    const K1_S6: CopySpec = CopySpec {
        node: "krick-1",
        proc_inc: 0x0a,
        stor_inc: 0xa1,
        domain: "krick",
    };
    const P1_S6: CopySpec = CopySpec {
        node: "pi5v1",
        proc_inc: 0x0b,
        stor_inc: 0xb1,
        domain: "pi5-west",
    };
    const P3_S6: CopySpec = CopySpec {
        node: "pi5v3",
        proc_inc: 0x0c,
        stor_inc: 0xc1,
        domain: "pi5-east",
    };
    const P1_S7: CopySpec = CopySpec {
        node: "pi5v1",
        proc_inc: 0x0b,
        stor_inc: 0xb2,
        domain: "pi5-west",
    };
    const K1_S7: CopySpec = CopySpec {
        node: "krick-1",
        proc_inc: 0x0a,
        stor_inc: 0xa2,
        domain: "krick",
    };

    fn committed_copy(spec: &CopySpec, coverage: [u8; 32]) -> CommittedCopy {
        CommittedCopy {
            node_id: spec.node.to_string(),
            storage_incarnation: inc(spec.stor_inc),
            failure_domain: spec.domain.to_string(),
            complete: true,
            coverage_digest: coverage,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn obs(
        bucket: u64,
        spec: &CopySpec,
        leaf: &str,
        shard: &str,
        sgen: u64,
        epoch: u64,
        rows: u64,
        bytes: u64,
        scans: u64,
        last: u64,
    ) -> PartitionObservation {
        PartitionObservation {
            partition: partition(bucket),
            reporter: ReporterIdentity {
                node_id: spec.node.to_string(),
                process_incarnation: inc(spec.proc_inc),
            },
            shard: ShardRef {
                shard: shard.to_string(),
                source_generation: sgen,
                storage_incarnation: inc(spec.stor_inc),
                ownership_epoch: epoch,
            },
            topology_generation: 9,
            leaf: leaf.to_string(),
            rows,
            resident_bytes: bytes,
            scans_observed: scans,
            scan_bytes: if scans > 0 { 786_432_000_000 } else { 0 },
            queue_wait_p50_us: if scans > 0 { 1200 } else { 0 },
            queue_wait_p99_us: if scans > 0 { 9400 } else { 0 },
            samples: if scans > 0 { 3000 } else { 0 },
            last_scanned_unix_ms: last,
            window_start_unix_ms: W0,
            window_end_unix_ms: W1,
        }
    }

    fn fixture_a_reports() -> Vec<PartitionObservation> {
        vec![
            obs(
                7,
                &K1_S6,
                "L4",
                "s6",
                3,
                5,
                1_000_000,
                268_435_456,
                3_000_000,
                T - 5000,
            ),
            obs(7, &P1_S6, "L4", "s6", 3, 5, 1_000_000, 268_435_456, 0, 0),
            obs(7, &P3_S6, "L4", "s6", 3, 5, 1_000_000, 268_435_456, 0, 0),
            obs(
                7,
                &P1_S7,
                "L7",
                "s7",
                3,
                2,
                500_000,
                134_217_728,
                0,
                T - 3_600_000,
            ),
            obs(7, &K1_S7, "L7", "s7", 3, 2, 500_000, 134_217_728, 0, 0),
        ]
    }

    fn committed_view_a() -> CommittedView {
        let mut shards = BTreeMap::new();
        shards.insert(
            "s6".to_string(),
            CommittedShard {
                source_generation: 3,
                ownership_epoch: 5,
                leaves: BTreeMap::from([(
                    "L4".to_string(),
                    LeafCoverage {
                        pool: vec![
                            "krick-1".to_string(),
                            "pi5v1".to_string(),
                            "pi5v3".to_string(),
                        ],
                        owner_node: "krick-1".to_string(),
                        copies: vec![
                            committed_copy(&K1_S6, cov_l4()),
                            committed_copy(&P1_S6, cov_l4()),
                            committed_copy(&P3_S6, cov_l4()),
                        ],
                    },
                )]),
            },
        );
        shards.insert(
            "s7".to_string(),
            CommittedShard {
                source_generation: 3,
                ownership_epoch: 2,
                leaves: BTreeMap::from([(
                    "L7".to_string(),
                    LeafCoverage {
                        pool: vec!["krick-1".to_string(), "pi5v1".to_string()],
                        owner_node: "pi5v1".to_string(),
                        copies: vec![
                            committed_copy(&P1_S7, cov_l7()),
                            committed_copy(&K1_S7, cov_l7()),
                        ],
                    },
                )]),
            },
        );
        CommittedView {
            topology_generation: 9,
            shards,
        }
    }

    fn store_a() -> ObservationStore {
        let mut store = ObservationStore::new(COHORT, 0, 1000, 1 << 20, committed_view_a())
            .expect("valid cohort config");
        store.register_incarnation("krick-1", inc(0x0a));
        store.register_incarnation("pi5v1", inc(0x0b));
        store.register_incarnation("pi5v3", inc(0x0c));
        for report in fixture_a_reports() {
            assert_eq!(store.ingest(report).expect("ingest"), IngestOutcome::Landed);
        }
        store
    }

    fn fragment_a_l4() -> FragmentRecord {
        FragmentRecord {
            partition: partition(7),
            topology_generation: 9,
            leaf: "L4".to_string(),
            pool: vec![
                "krick-1".to_string(),
                "pi5v1".to_string(),
                "pi5v3".to_string(),
            ],
            shard_owners: BTreeMap::from([("s6".to_string(), "krick-1".to_string())]),
            copies: vec![
                committed_copy(&K1_S6, cov_l4()),
                committed_copy(&P1_S6, cov_l4()),
                committed_copy(&P3_S6, cov_l4()),
            ],
        }
    }

    fn fragment_a_l7() -> FragmentRecord {
        FragmentRecord {
            partition: partition(7),
            topology_generation: 9,
            leaf: "L7".to_string(),
            pool: vec!["krick-1".to_string(), "pi5v1".to_string()],
            shard_owners: BTreeMap::from([("s7".to_string(), "pi5v1".to_string())]),
            copies: vec![
                committed_copy(&P1_S7, cov_l7()),
                committed_copy(&K1_S7, cov_l7()),
            ],
        }
    }

    fn capacity_map() -> BTreeMap<String, NodeCapacity> {
        BTreeMap::from([
            (
                "krick-1".to_string(),
                NodeCapacity {
                    total_bytes: 26_843_545_600,
                    resident_bytes: 0,
                },
            ),
            (
                "pi5v1".to_string(),
                NodeCapacity {
                    total_bytes: 16_106_127_360,
                    resident_bytes: 0,
                },
            ),
            (
                "pi5v3".to_string(),
                NodeCapacity {
                    total_bytes: 0,
                    resident_bytes: 0,
                },
            ),
        ])
    }

    fn domain_map() -> BTreeMap<String, String> {
        BTreeMap::from([
            ("krick-1".to_string(), "krick".to_string()),
            ("pi5v1".to_string(), "pi5-west".to_string()),
            ("pi5v3".to_string(), "pi5-east".to_string()),
        ])
    }

    fn snapshot_a(store: &ObservationStore, epoch: u64) -> TierSnapshot {
        TierSnapshot {
            planning_instant_unix_ms: T,
            max_moves: 16,
            max_observation_age_ms: 600_000,
            clock_skew_bound_ms: 5_000,
            authority_id: "control-0".to_string(),
            authority_incarnation: inc(0x01),
            control_revision: 41,
            policy_revision: 7,
            policy: policy_p(),
            observation_epoch: epoch,
            topology_generation: 9,
            placement_tree_digest: tree_digest(),
            workspace: "ws-court".to_string(),
            collection: "cases".to_string(),
            derived_fingerprint: decl_fp(),
            provider_geometry_digest: prov_digest(),
            cohort_length_ms: COHORT,
            cohort_phase_unix_ms: 0,
            node_domains: domain_map(),
            observations: store.observations().into_iter().cloned().collect(),
            fragments: vec![fragment_a_l4(), fragment_a_l7()],
            capacity: capacity_map(),
        }
    }

    #[test]
    fn pinned_policy_and_declaration_fixtures() {
        assert_eq!(hex32(&decl_fp()), DECL_FP_HEX);
        let policy = policy_p();
        assert_eq!(policy.canonical_bytes().len(), 155);
        assert_eq!(hex32(&policy.fingerprint()), POLICY_FP_HEX);
        let mut swapped = policy_p();
        swapped.tiers.swap(0, 1);
        assert_eq!(hex32(&swapped.fingerprint()), POLICY_SWAPPED_FP_HEX);
        assert_eq!(hex32(&cov_l4()), COV_L4_HEX);
        assert_eq!(hex32(&cov_l7()), COV_L7_HEX);

        let mut dup = policy_p();
        dup.tiers[1].name = "hot".to_string();
        assert!(dup.validate().unwrap_err().contains("duplicate tier name"));
        let bad_band = TierPolicy {
            tiers: vec![tier("warm", 2, 100, 100, 0)],
        };
        assert!(
            bad_band
                .validate()
                .unwrap_err()
                .contains("empty or inverted")
        );
        let bad_floor = TierPolicy {
            tiers: vec![tier("warm", 0, 1, 100, 0)],
        };
        assert!(bad_floor.validate().unwrap_err().contains("min_replicas"));
    }

    #[test]
    fn fixture_a_observation_set_digest_matches_section_10() {
        let store = store_a();
        assert_eq!(store.epoch(), 5);
        assert_eq!(hex32(&store.observation_set_digest()), OBS_A_HEX);
    }

    #[test]
    fn fixture_a_plan_matches_section_10() {
        let store = store_a();
        let snapshot = snapshot_a(&store, 118);
        assert_eq!(hex32(&snapshot.plan_digest()), PLAN_A_HEX);
        let response = plan_tiers(&snapshot).expect("plan");
        assert_eq!(hex32(&response.plan_digest), PLAN_A_HEX);
        assert_eq!(response.moves.len(), 0);
        assert_eq!(response.refusals.len(), 0);
        assert_eq!(response.placements.len(), 2);

        let l4 = &response.placements[0];
        assert_eq!(l4.fragment.leaf, "L4");
        assert_eq!(l4.classified_tier.as_deref(), Some("hot"));
        assert_eq!(l4.rate_nanos, 18_626);
        assert_eq!(l4.rows, 1_000_000);
        assert_eq!(l4.resident_bytes, 268_435_456);
        assert_eq!(l4.last_scanned_unix_ms, T - 5000);
        assert_eq!(l4.complete_replicas.len(), 3);
        assert_eq!(l4.reporters.len(), 3);
        assert_eq!(l4.reporters[0].node_id, "krick-1");
        assert_eq!(l4.reporters[0].queue_wait_p50_us, Some(1200));
        assert_eq!(l4.reporters[1].queue_wait_p50_us, None);

        let l7 = &response.placements[1];
        assert_eq!(l7.fragment.leaf, "L7");
        assert_eq!(l7.classified_tier.as_deref(), Some("archive"));
        assert_eq!(l7.rate_nanos, 0);
        assert_eq!(l7.last_scanned_unix_ms, T - 3_600_000);
        assert_eq!(l7.complete_replicas.len(), 2);
    }

    #[test]
    fn warm_literal_12000_scans_classifies_warm() {
        let store = store_a();
        let mut snapshot = snapshot_a(&store, 118);
        for obs in &mut snapshot.observations {
            if obs.reporter.node_id == "krick-1" && obs.leaf == "L4" {
                obs.scans_observed = 12_000;
            }
        }
        let response = plan_tiers(&snapshot).expect("plan");
        let l4 = &response.placements[0];
        assert_eq!(l4.rate_nanos, 74);
        assert_eq!(l4.classified_tier.as_deref(), Some("warm"));
    }

    #[test]
    fn permuted_ingest_order_yields_identical_plan_bytes() {
        let store = store_a();
        let snapshot = snapshot_a(&store, 118);
        let response = plan_tiers(&snapshot).expect("plan");

        let mut store2 = ObservationStore::new(COHORT, 0, 1000, 1 << 20, committed_view_a())
            .expect("valid cohort config");
        store2.register_incarnation("krick-1", inc(0x0a));
        store2.register_incarnation("pi5v1", inc(0x0b));
        store2.register_incarnation("pi5v3", inc(0x0c));
        let mut reports = fixture_a_reports();
        reports.reverse();
        for report in reports {
            assert_eq!(
                store2.ingest(report).expect("ingest"),
                IngestOutcome::Landed
            );
        }
        assert_eq!(
            store2.observation_set_digest(),
            store.observation_set_digest()
        );
        let snapshot2 = snapshot_a(&store2, 118);
        let response2 = plan_tiers(&snapshot2).expect("plan");
        assert_eq!(response.canonical_bytes(), response2.canonical_bytes());
    }

    #[test]
    fn cohort_alignment_rules() {
        let mut store = ObservationStore::new(COHORT, 0, 1000, 1 << 20, committed_view_a())
            .expect("valid cohort config");
        store.register_incarnation("krick-1", inc(0x0a));

        // E10: a 660-second, off-grid window refuses by name.
        let mut misaligned = obs(7, &K1_S6, "L4", "s6", 3, 5, 1_000_000, 268_435_456, 0, 0);
        misaligned.window_start_unix_ms = T - 660_000;
        misaligned.window_end_unix_ms = T - 60_000;
        assert_eq!(
            store.ingest(misaligned).unwrap_err(),
            "report_shard: window [1788868140000, 1788868740000) is not a cohort window (length 600000 ms, phase 0)"
        );

        // An inverted window refuses.
        let mut inverted = obs(7, &K1_S6, "L4", "s6", 3, 5, 1_000_000, 268_435_456, 0, 0);
        inverted.window_start_unix_ms = W1;
        inverted.window_end_unix_ms = W0;
        assert!(store.ingest(inverted).unwrap_err().contains("inverted"));

        // A window before the phase refuses as unaligned.
        let mut store_phased = ObservationStore::new(COHORT, W0, 1000, 1 << 20, committed_view_a())
            .expect("valid cohort config");
        store_phased.register_incarnation("krick-1", inc(0x0a));
        let mut early = obs(7, &K1_S6, "L4", "s6", 3, 5, 1_000_000, 268_435_456, 0, 0);
        early.window_start_unix_ms = W0 - COHORT;
        early.window_end_unix_ms = W0;
        assert!(
            store_phased
                .ingest(early)
                .unwrap_err()
                .contains("not a cohort window")
        );

        // A cohort length past 2^40 ms refuses at configuration.
        assert!(ObservationStore::new((1 << 40) + 1, 0, 10, 1000, committed_view_a()).is_err());

        // The current cohort: exact boundary selects [T-L, T); mid-window
        // selects the previously closed cohort.
        let store = store_a();
        let snapshot = snapshot_a(&store, 118);
        assert_eq!(snapshot.current_cohort().unwrap(), (W0, W1));
        let mut mid = snapshot_a(&store, 118);
        mid.planning_instant_unix_ms = W0 + 100_000;
        assert_eq!(mid.current_cohort().unwrap(), (W0 - COHORT, W0));
    }

    #[test]
    fn missing_and_stale_reports_refuse_the_plan() {
        // E3: an uncovered complete copy's missing report refuses.
        let mut store = ObservationStore::new(COHORT, 0, 1000, 1 << 20, committed_view_a())
            .expect("valid cohort config");
        store.register_incarnation("krick-1", inc(0x0a));
        store.register_incarnation("pi5v1", inc(0x0b));
        store.register_incarnation("pi5v3", inc(0x0c));
        for report in fixture_a_reports().into_iter().skip(1) {
            store.ingest(report).expect("ingest");
        }
        let snapshot = snapshot_a(&store, 118);
        assert_eq!(
            plan_tiers(&snapshot).unwrap_err(),
            "plan_tiers: partition key_bucket=7 on node krick-1 has no current observation; unknown is not idle"
        );

        // E4's boundary arithmetic, with cohort-aligned literals: the age
        // equals T - w1; strictly greater than the bound is stale.
        let store = store_a();
        let mut snapshot = snapshot_a(&store, 118);
        snapshot.planning_instant_unix_ms = W1 + 1000;
        snapshot.max_observation_age_ms = 1000;
        assert!(plan_tiers(&snapshot).is_ok(), "age == bound is fresh");
        snapshot.max_observation_age_ms = 999;
        assert_eq!(
            plan_tiers(&snapshot).unwrap_err(),
            "plan_tiers: partition key_bucket=7 on node krick-1 was last observed 1000 ms ago, past the 999 ms bound"
        );
    }

    #[test]
    fn skewed_future_report_refuses_the_plan() {
        let mut store = store_a();
        // A report for the next cohort window (future at T) is stored,
        // then refused by the plan's skew rule.
        let mut future = obs(7, &P3_S6, "L4", "s6", 3, 5, 1_000_000, 268_435_456, 0, 0);
        future.window_start_unix_ms = W1;
        future.window_end_unix_ms = W1 + COHORT;
        assert_eq!(store.ingest(future).expect("ingest"), IngestOutcome::Landed);
        let snapshot = snapshot_a(&store, 118);
        assert_eq!(
            plan_tiers(&snapshot).unwrap_err(),
            "plan_tiers: observation from node pi5v3 ends 600000 ms in the future, past the 5000 ms skew bound"
        );
    }

    #[test]
    fn restart_supersedes_the_old_incarnation() {
        let mut store = store_a();
        let epoch_before = store.epoch();
        // krick-1 restarts: new process incarnation, same storage.
        store.register_incarnation("krick-1", inc(0x1a));
        assert_eq!(
            store.epoch(),
            epoch_before + 1,
            "the drop advanced the epoch once"
        );
        assert_eq!(
            store.observations().len(),
            3,
            "both krick-1 reports dropped"
        );

        // A retry from the dead process refuses by name.
        let ghost = obs(
            7,
            &K1_S6,
            "L4",
            "s6",
            3,
            5,
            1_000_000,
            268_435_456,
            3_000_000,
            T - 5000,
        );
        let err = store.ingest(ghost).unwrap_err();
        assert!(
            err.contains("report_shard: node krick-1 incarnation"),
            "{err}"
        );
        assert!(err.contains("is superseded by"), "{err}");

        // The new process reports; classification uses its figures.
        let mut fresh = obs(7, &K1_S6, "L4", "s6", 3, 5, 1_000_000, 268_435_456, 0, 0);
        fresh.reporter.process_incarnation = inc(0x1a);
        assert_eq!(store.ingest(fresh).expect("ingest"), IngestOutcome::Landed);
        let mut s7_owner = obs(7, &K1_S7, "L7", "s7", 3, 2, 500_000, 134_217_728, 0, 0);
        s7_owner.reporter.process_incarnation = inc(0x1a);
        assert_eq!(
            store.ingest(s7_owner).expect("ingest"),
            IngestOutcome::Landed
        );
        let snapshot = snapshot_a(&store, 118);
        let response = plan_tiers(&snapshot).expect("plan");
        assert_eq!(response.placements[0].rate_nanos, 0);
        assert_eq!(
            response.placements[0].classified_tier.as_deref(),
            Some("archive")
        );
    }

    #[test]
    fn exact_and_conflicting_retries() {
        let mut store = store_a();
        let digest_before = store.observation_set_digest();
        let epoch_before = store.epoch();

        // E6: byte-identical retry is a no-op.
        let retry = obs(
            7,
            &K1_S6,
            "L4",
            "s6",
            3,
            5,
            1_000_000,
            268_435_456,
            3_000_000,
            T - 5000,
        );
        assert_eq!(
            store.ingest(retry).expect("ingest"),
            IngestOutcome::Unchanged
        );
        assert_eq!(store.epoch(), epoch_before);
        assert_eq!(store.observation_set_digest(), digest_before);

        // E6: same key and window, different payload refuses.
        let mut conflict = obs(
            7,
            &K1_S6,
            "L4",
            "s6",
            3,
            5,
            1_000_000,
            268_435_456,
            2_999_999,
            T - 5000,
        );
        conflict.scan_bytes = 0;
        conflict.queue_wait_p50_us = 0;
        conflict.queue_wait_p99_us = 0;
        conflict.samples = 0;
        assert_eq!(
            store.ingest(conflict).unwrap_err(),
            "report_shard: observation for (key_bucket=7, gen 9, L4, s6, gen 3) window [1788868200000, 1788868800000) from krick-1/0000000000000000000000000000000a conflicts with the stored report"
        );

        // A late report for the previous cohort is discarded.
        let mut old = obs(7, &K1_S6, "L4", "s6", 3, 5, 1_000_000, 268_435_456, 0, 0);
        old.window_start_unix_ms = W0 - COHORT;
        old.window_end_unix_ms = W0;
        assert_eq!(
            store.ingest(old).expect("ingest"),
            IngestOutcome::Superseded
        );
        assert_eq!(store.epoch(), epoch_before);
    }

    #[test]
    fn committed_identity_is_equality_checked_both_directions() {
        let mut store = ObservationStore::new(COHORT, 0, 1000, 1 << 20, committed_view_a())
            .expect("valid cohort config");
        store.register_incarnation("krick-1", inc(0x0a));
        let base = || obs(7, &K1_S6, "L4", "s6", 3, 5, 1_000_000, 268_435_456, 0, 0);

        let mut ahead = base();
        ahead.shard.source_generation = 4;
        assert_eq!(
            store.ingest(ahead).unwrap_err(),
            "report_shard: shard s6 source generation 4 is not the committed 3"
        );
        let mut behind_epoch = base();
        behind_epoch.shard.ownership_epoch = 4;
        assert!(
            store
                .ingest(behind_epoch)
                .unwrap_err()
                .contains("ownership epoch 4 is not the committed 5")
        );
        let mut replaced = base();
        replaced.shard.storage_incarnation = inc(0xaa);
        assert!(
            store
                .ingest(replaced)
                .unwrap_err()
                .contains("is not the committed")
        );
        let mut wrong_leaf = base();
        wrong_leaf.leaf = "L7".to_string();
        assert!(
            store
                .ingest(wrong_leaf)
                .unwrap_err()
                .contains("covers no rows in leaf L7")
        );
        let mut wrong_shard = base();
        wrong_shard.shard.shard = "s9".to_string();
        assert!(
            store
                .ingest(wrong_shard)
                .unwrap_err()
                .contains("not in the committed topology")
        );
        let mut wrong_gen = base();
        wrong_gen.topology_generation = 10;
        assert!(
            store
                .ingest(wrong_gen)
                .unwrap_err()
                .contains("topology_generation 10 is not the committed 9")
        );

        let mut store2 = ObservationStore::new(COHORT, 0, 1000, 1 << 20, committed_view_a())
            .expect("valid cohort config");
        assert!(
            store2
                .ingest(base())
                .unwrap_err()
                .contains("has not registered an incarnation")
        );
    }

    #[test]
    fn store_bounds_and_impossible_states() {
        // Impossible states refuse at ingest.
        let mut store = ObservationStore::new(COHORT, 0, 1000, 1 << 20, committed_view_a())
            .expect("valid cohort config");
        store.register_incarnation("krick-1", inc(0x0a));
        let mut no_bytes = obs(7, &K1_S6, "L4", "s6", 3, 5, 1_000_000, 0, 0, 0);
        assert!(
            store
                .ingest(no_bytes.clone())
                .unwrap_err()
                .contains("0 resident bytes")
        );
        no_bytes.rows = 0;
        no_bytes.scans_observed = 5;
        assert!(
            store
                .ingest(no_bytes)
                .unwrap_err()
                .contains("scans of 0 resident bytes")
        );

        // Total record bound: new keys refuse, supersession still works.
        let mut tight = ObservationStore::new(COHORT, 0, 1, 1 << 20, committed_view_a())
            .expect("valid cohort config");
        tight.register_incarnation("krick-1", inc(0x0a));
        tight.register_incarnation("pi5v1", inc(0x0b));
        tight
            .ingest(obs(
                7,
                &K1_S6,
                "L4",
                "s6",
                3,
                5,
                1_000_000,
                268_435_456,
                0,
                0,
            ))
            .expect("first record");
        assert_eq!(
            tight
                .ingest(obs(
                    7,
                    &P1_S6,
                    "L4",
                    "s6",
                    3,
                    5,
                    1_000_000,
                    268_435_456,
                    0,
                    0
                ))
                .unwrap_err(),
            "report_shard: the observation store holds the bound of 1 records"
        );
        let mut newer = obs(7, &K1_S6, "L4", "s6", 3, 5, 1_000_000, 268_435_456, 1, 0);
        newer.last_scanned_unix_ms = W1 - 1000;
        newer.window_start_unix_ms = W1;
        newer.window_end_unix_ms = W1 + COHORT;
        assert_eq!(
            tight.ingest(newer).expect("supersede at the record bound"),
            IngestOutcome::Landed
        );

        // Total byte bound.
        let mut tight_bytes = ObservationStore::new(COHORT, 0, 1000, 100, committed_view_a())
            .expect("valid cohort config");
        tight_bytes.register_incarnation("krick-1", inc(0x0a));
        assert!(
            tight_bytes
                .ingest(obs(
                    7,
                    &K1_S6,
                    "L4",
                    "s6",
                    3,
                    5,
                    1_000_000,
                    268_435_456,
                    0,
                    0
                ))
                .unwrap_err()
                .contains("bound of 100 retained bytes")
        );

        // Expiry is a committed transition that advances the epoch once.
        let mut store = store_a();
        let dropped = store.commit_expiry(W1 + COHORT, 1000);
        assert_eq!(dropped, 5);
        assert_eq!(store.observations().len(), 0);
        assert_eq!(
            store.commit_expiry(W1 + COHORT, 1000),
            0,
            "second pass changes nothing"
        );
    }

    fn fixture_m_reports() -> Vec<PartitionObservation> {
        vec![
            obs(
                5,
                &P3_S6,
                "L4",
                "s6",
                3,
                5,
                80_000,
                21_474_836_480,
                0,
                T - 7_200_000,
            ),
            obs(
                6,
                &P3_S6,
                "L4",
                "s6",
                3,
                5,
                72_000,
                19_327_352_832,
                0,
                T - 10_800_000,
            ),
        ]
    }

    fn committed_view_m() -> CommittedView {
        let mut shards = BTreeMap::new();
        shards.insert(
            "s6".to_string(),
            CommittedShard {
                source_generation: 3,
                ownership_epoch: 5,
                leaves: BTreeMap::from([(
                    "L4".to_string(),
                    LeafCoverage {
                        pool: vec![
                            "krick-1".to_string(),
                            "pi5v1".to_string(),
                            "pi5v3".to_string(),
                        ],
                        owner_node: "pi5v3".to_string(),
                        copies: vec![committed_copy(&P3_S6, cov_l4())],
                    },
                )]),
            },
        );
        CommittedView {
            topology_generation: 9,
            shards,
        }
    }

    fn store_m() -> ObservationStore {
        let mut store = ObservationStore::new(COHORT, 0, 1000, 1 << 20, committed_view_m())
            .expect("valid cohort config");
        store.register_incarnation("pi5v3", inc(0x0c));
        for report in fixture_m_reports() {
            assert_eq!(store.ingest(report).expect("ingest"), IngestOutcome::Landed);
        }
        store
    }

    fn fragment_m(bucket: u64) -> FragmentRecord {
        FragmentRecord {
            partition: partition(bucket),
            topology_generation: 9,
            leaf: "L4".to_string(),
            pool: vec![
                "krick-1".to_string(),
                "pi5v1".to_string(),
                "pi5v3".to_string(),
            ],
            shard_owners: BTreeMap::from([("s6".to_string(), "pi5v3".to_string())]),
            copies: vec![committed_copy(&P3_S6, cov_l4())],
        }
    }

    fn snapshot_m(store: &ObservationStore) -> TierSnapshot {
        TierSnapshot {
            planning_instant_unix_ms: T,
            max_moves: 16,
            max_observation_age_ms: 600_000,
            clock_skew_bound_ms: 5_000,
            authority_id: "control-0".to_string(),
            authority_incarnation: inc(0x01),
            control_revision: 41,
            policy_revision: 7,
            policy: policy_p(),
            observation_epoch: 119,
            topology_generation: 9,
            placement_tree_digest: tree_digest(),
            workspace: "ws-court".to_string(),
            collection: "cases".to_string(),
            derived_fingerprint: decl_fp(),
            provider_geometry_digest: prov_digest(),
            cohort_length_ms: COHORT,
            cohort_phase_unix_ms: 0,
            node_domains: domain_map(),
            observations: store.observations().into_iter().cloned().collect(),
            fragments: vec![fragment_m(5), fragment_m(6)],
            capacity: capacity_map(),
        }
    }

    #[test]
    fn fixture_m_move_plan_and_capacity_refusal_match_section_10() {
        let store = store_m();
        assert_eq!(hex32(&store.observation_set_digest()), OBS_M_HEX);
        let snapshot = snapshot_m(&store);
        assert_eq!(hex32(&snapshot.plan_digest()), PLAN_M_HEX);
        let response = plan_tiers(&snapshot).expect("plan");

        assert_eq!(response.placements.len(), 2);
        assert_eq!(
            response.placements[0].classified_tier.as_deref(),
            Some("archive")
        );
        assert_eq!(response.placements[0].rate_nanos, 0);
        assert_eq!(
            response.placements[1].classified_tier.as_deref(),
            Some("archive")
        );

        // E12: exactly one move, fully specified.
        assert_eq!(response.moves.len(), 1);
        let mv = &response.moves[0];
        assert_eq!(mv.fragment.partition.bucket, 5);
        assert_eq!(mv.source.node_id, "pi5v3");
        assert_eq!(mv.source.storage_incarnation, inc(0xc1));
        assert_eq!(mv.destination_node, "krick-1");
        assert_eq!(mv.from_tier, "archive");
        assert_eq!(mv.to_tier, "archive");
        assert_eq!(mv.bytes, 21_474_836_480);
        assert_eq!(hex32(&mv.plan_digest), PLAN_M_HEX);
        assert_eq!(mv.control_revision, 41);
        assert_eq!(mv.observation_epoch, 119);
        assert_eq!(hex32(&mv.policy_fingerprint), POLICY_FP_HEX);

        // E13: bucket 6 has no eligible destination, refusal verbatim.
        assert_eq!(response.refusals.len(), 1);
        assert_eq!(
            response.refusals[0],
            "plan_tiers: tier \"archive\" floor is 2 for partition key_bucket=6, but no eligible destination in leaf L4 has capacity: krick-1 has 5368709120 free after earlier planned moves, pi5v1 has 16106127360 free, the fragment needs 19327352832"
        );
    }

    #[test]
    fn partial_copies_are_not_replicas_and_domains_count_once() {
        let store = store_m();
        let mut snapshot = snapshot_m(&store);
        // E2: bucket 9's rows sit in s6 under L4 with an owner report,
        // but its copies are two disjoint partials — zero complete
        // replicas, so the floor refuses instead of planning a move.
        snapshot.observations.push(obs(
            9,
            &P3_S6,
            "L4",
            "s6",
            3,
            5,
            60_000,
            10_000_000_000,
            0,
            T - 1000,
        ));
        let mut partial_a = committed_copy(&P1_S6, cov_l4());
        partial_a.complete = false;
        let mut partial_b = committed_copy(&P3_S6, cov_l4());
        partial_b.complete = false;
        let fragment9 = FragmentRecord {
            partition: partition(9),
            topology_generation: 9,
            leaf: "L4".to_string(),
            pool: vec![
                "krick-1".to_string(),
                "pi5v1".to_string(),
                "pi5v3".to_string(),
            ],
            shard_owners: BTreeMap::from([("s6".to_string(), "pi5v3".to_string())]),
            copies: vec![partial_a, partial_b],
        };
        snapshot.fragments = vec![fragment9];
        let response = plan_tiers(&snapshot).expect("plan");
        assert_eq!(response.moves.len(), 0);
        assert_eq!(
            response.refusals,
            vec![
                "plan_tiers: fragment (key_bucket=9, gen 9, L4) has 0 complete replicas (2 partial); tier \"archive\" floor is 2"
                .to_string()
            ]
        );

        // Three complete copies over two failure domains satisfy a floor
        // of 2 but not 3 (§2's distinct-domain rule), so hot plans one
        // copy to a fourth node in a fresh domain.
        let store = store_a();
        let mut snapshot = snapshot_a(&store, 118);
        let mut same_domain = committed_copy(&P3_S6, cov_l4());
        same_domain.failure_domain = "pi5-west".to_string();
        snapshot.fragments[0].copies[2] = same_domain;
        snapshot.fragments[0].pool.push("pi5v4".to_string());
        snapshot
            .node_domains
            .insert("pi5v4".to_string(), "pi5-north".to_string());
        snapshot.capacity.insert(
            "pi5v4".to_string(),
            NodeCapacity {
                total_bytes: 1_000_000_000_000,
                resident_bytes: 0,
            },
        );
        let response = plan_tiers(&snapshot).expect("plan");
        assert_eq!(
            response.placements[0].classified_tier.as_deref(),
            Some("hot")
        );
        assert_eq!(
            response.moves.len(),
            1,
            "hot's floor of 3 sees only 2 domains"
        );
        assert_eq!(response.moves[0].destination_node, "pi5v4");
        assert_eq!(response.refusals.len(), 0);
    }

    #[test]
    fn cross_collection_identity_is_bound_in_the_context() {
        let store = store_a();
        let mut briefs = snapshot_a(&store, 118);
        briefs.collection = "briefs".to_string();
        assert_ne!(briefs.plan_digest(), snapshot_a(&store, 118).plan_digest());
    }

    #[test]
    fn numeric_bounds_are_checked_not_wrapping() {
        // The u128 rate path is exact at u64 extremes.
        let s = u64::MAX as u128;
        let b = u64::MAX as u128;
        let expected = (s * 1_000_000_000_000u128) / (600_000u128 * b);
        assert_eq!(rate_nanos(s, 600_000, b), u64::try_from(expected).unwrap());

        // Fan-in beyond 2^20 refuses by name.
        assert!(ensure_fan_in(1 << 20).is_ok());
        assert!(ensure_fan_in((1 << 20) + 1).unwrap_err().contains("fan-in"));

        // A warmth bound that overflows milliseconds refuses at plan time.
        let store = store_a();
        let mut snapshot = snapshot_a(&store, 118);
        snapshot.policy.tiers[0].max_seconds_since_scan = u64::MAX;
        snapshot.policy.tiers[0].scans_per_byte_nanos_lo = 0;
        assert!(
            plan_tiers(&snapshot)
                .unwrap_err()
                .contains("warmth bound 18446744073709551615 s overflows milliseconds")
        );
    }
}

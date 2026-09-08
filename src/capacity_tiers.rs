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

impl ObservationStore {
    pub fn new(
        cohort_length_ms: u64,
        cohort_phase_unix_ms: u64,
        max_total_records: usize,
        max_total_bytes: usize,
        max_registered_nodes: usize,
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
            max_registered_nodes,
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

    pub fn committed(&self) -> &CommittedView {
        &self.committed
    }

    pub fn current_incarnations(&self) -> &BTreeMap<String, [u8; 16]> {
        &self.current_incarnation
    }

    fn next_epoch(&self) -> Result<u64, String> {
        self.epoch
            .checked_add(1)
            .ok_or_else(|| "report_shard: the observation epoch is exhausted".to_string())
    }

    /// Register the node's current process incarnation. A new incarnation
    /// supersedes the old one: every stored observation from the old
    /// incarnation is dropped in the same committed transition, and the
    /// epoch advances once if anything was dropped (§3, E5). The registry
    /// itself is bounded; a new node past the bound refuses.
    pub fn register_incarnation(
        &mut self,
        node_id: &str,
        incarnation: [u8; 16],
    ) -> Result<(), String> {
        if self.current_incarnation.get(node_id) == Some(&incarnation) {
            return Ok(());
        }
        if !self.current_incarnation.contains_key(node_id)
            && self.current_incarnation.len() >= self.max_registered_nodes
        {
            return Err(format!(
                "report_shard: the incarnation registry holds the bound of {} nodes",
                self.max_registered_nodes
            ));
        }
        let doomed: Vec<ObsKey> = self
            .records
            .keys()
            .filter(|key| key.node_id == node_id && key.process_incarnation != incarnation)
            .cloned()
            .collect();
        let next = if doomed.is_empty() {
            None
        } else {
            Some(self.next_epoch()?)
        };
        for key in doomed {
            if let Some(stored) = self.records.remove(&key) {
                self.retained_bytes -= stored.heap_bytes;
            }
        }
        if let Some(next) = next {
            self.epoch = next;
        }
        self.current_incarnation
            .insert(node_id.to_string(), incarnation);
        Ok(())
    }

    /// The current-cohort rule applied to the wall clock by the authority's
    /// scheduler: remove every observation whose window end is more than
    /// `max_age_ms` before `now`, advancing the epoch once if the set
    /// changed. Expiry is a committed transition, never a side effect of
    /// planning (§3/§4).
    pub fn commit_expiry(&mut self, now_unix_ms: u64, max_age_ms: u64) -> Result<usize, String> {
        let doomed: Vec<ObsKey> = self
            .records
            .iter()
            .filter(|(_, stored)| {
                now_unix_ms.saturating_sub(stored.obs.window_end_unix_ms) > max_age_ms
            })
            .map(|(key, _)| key.clone())
            .collect();
        if doomed.is_empty() {
            return Ok(0);
        }
        let next = self.next_epoch()?;
        let dropped = doomed.len();
        for key in doomed {
            if let Some(stored) = self.records.remove(&key) {
                self.retained_bytes -= stored.heap_bytes;
            }
        }
        self.epoch = next;
        Ok(dropped)
    }

    /// Validate and store one report (§3). Refusals name the route, the
    /// field, the carried value, and the committed value.
    pub fn ingest(&mut self, obs: PartitionObservation) -> Result<IngestOutcome, String> {
        let key = obs.key();

        // Resource binding, before anything else: the committed view
        // serves one (workspace, collection, declaration).
        if !self.committed.resource_matches(&obs.partition) {
            return Err(format!(
                "report_shard: partition {}/{}/{} is not this resource {}/{}/{}",
                obs.partition.workspace,
                obs.partition.collection,
                hex16(&obs.partition.derived_fingerprint[..16].try_into().unwrap()),
                self.committed.workspace,
                self.committed.collection,
                hex16(&self.committed.derived_fingerprint[..16].try_into().unwrap())
            ));
        }

        // Cohort alignment (E10).
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

        check_observation_identity(&self.committed, &obs)?;

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
        let new_total = (self.retained_bytes - replacing)
            .checked_add(heap_bytes)
            .ok_or_else(|| "report_shard: the retained-byte accounting overflowed".to_string())?;
        if new_total > self.max_total_bytes {
            return Err(format!(
                "report_shard: the observation store would exceed its bound of {} retained bytes",
                self.max_total_bytes
            ));
        }
        let next = self.next_epoch()?;

        self.retained_bytes = new_total;
        self.records
            .insert(key, StoredObservation { obs, heap_bytes });
        self.epoch = next;
        Ok(IngestOutcome::Landed)
    }

    /// The stored set in canonical key order, all cohorts.
    pub fn observations(&self) -> Vec<&PartitionObservation> {
        self.records.values().map(|stored| &stored.obs).collect()
    }

    /// §4: the observation-set digest over the stored set in canonical
    /// key order (domain `protomolt.capacity-observations.v1`), computed
    /// by streaming the ordered records without cloning the set.
    pub fn observation_set_digest(&self) -> [u8; 32] {
        let mut out = Vec::new();
        out.extend_from_slice(OBSERVATIONS_DOMAIN);
        put_u32(&mut out, ENCODING_VERSION);
        put_u32(&mut out, self.records.len() as u32);
        for stored in self.records.values() {
            out.extend_from_slice(&stored.obs.canonical_bytes());
        }
        sha256::digest(&out)
    }
}

/// §3's committed-identity equality checks, shared by the store's ingest
/// and the snapshot's validated constructor: a behind-or-ahead value in
/// either direction names bytes other than the committed ones.
fn check_observation_identity(
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

    fn put_canonical(&self, out: &mut Vec<u8>) {
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
            placement.fragment.put_canonical(&mut out);
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
            mv.fragment.put_canonical(&mut out);
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

type FragmentKey = (PartitionIdentity, u64, String);

struct FragmentReports<'a> {
    /// Current-cohort reports by (shard, node).
    current: BTreeMap<(String, String), &'a PartitionObservation>,
}

/// §4: the deterministic dry run. Pure function of the validated
/// snapshot; the same snapshot yields the same response, bitwise.
pub fn plan_tiers(snapshot: &TierSnapshot) -> Result<PlanTiersResponse, String> {
    snapshot.policy.validate()?;
    let t = snapshot.planning_instant_unix_ms;
    let (w0, w1) = snapshot.current_cohort()?;
    let skew_horizon = t
        .checked_add(snapshot.clock_skew_bound_ms)
        .ok_or_else(|| "plan_tiers: planning instant plus the skew bound overflows".to_string())?;

    // Index the current-cohort reports of covered fragments by their full
    // identity — resource, declaration, generation, leaf, shard, node —
    // and apply the skew rule to every stored report the plan could read.
    let covered: std::collections::BTreeSet<FragmentKey> = snapshot
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
        .nodes
        .iter()
        .map(|(node, record)| (node.clone(), record.capacity.free_bytes()))
        .collect();
    let mut planned_onto: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();

    'fragments: for fragment in &snapshot.fragments {
        let empty = FragmentReports {
            current: BTreeMap::new(),
        };
        let frag_reports = reports
            .get(&(
                fragment.partition.clone(),
                fragment.topology_generation,
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

        let complete_replicas: Vec<CommittedCopy> = fragment.complete_copies().cloned().collect();
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
        // counts complete copies in distinct failure domains (§2). The
        // deficit is a count with one source, never a cloned vector of
        // work orders.
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
                .next()
                .expect("a complete copy exists")
                .clone();
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

        // Repair (§4 steps 3-5): candidates inside the pool, server-
        // resident and eligible per the committed node record, domain-
        // distinct at every intermediate step (§7), capacity-checked
        // against projected free bytes.
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
                        .nodes
                        .get(node.as_str())
                        .map(|record| {
                            record.residency == NodeResidency::Server
                                && record.eligible
                                && !held_domains.contains(&record.failure_domain)
                        })
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
            if let Some(record) = snapshot.nodes.get(&destination) {
                held_domains.insert(record.failure_domain.clone());
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
    const OBS_M_HEX: &str = "236d6740a5d5eaec435bb2b6c4b85644ed437ff87b40fd6b9150579ead21f6e1";
    // Plan-input encoding v2 digests (2026-09-08): bind the cohort and the
    // node/fragment subdigests; recomputed with the Python oracle.
    const PLAN_A_HEX: &str = "363d0d80d42d9d9e1e3deb256792dab6c088d3aa82eada126e7a038c7460ce27";
    const PLAN_M_HEX: &str = "69725b5f19f17c5f42e5fbd45074c11ed941ac5ad1962a9de993dac6cb2400c1";
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

    fn req(bucket: u64, leaf: &str) -> FragmentRequest {
        FragmentRequest {
            partition: partition(bucket),
            topology_generation: 9,
            leaf: leaf.to_string(),
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
            workspace: "ws-court".to_string(),
            collection: "cases".to_string(),
            derived_fingerprint: decl_fp(),
            topology_generation: 9,
            shards,
        }
    }

    fn nodes_map() -> BTreeMap<String, NodeRecord> {
        let server = |domain: &str, total: u64| NodeRecord {
            residency: NodeResidency::Server,
            eligible: true,
            failure_domain: domain.to_string(),
            capacity: NodeCapacity {
                total_bytes: total,
                resident_bytes: 0,
            },
        };
        BTreeMap::from([
            ("krick-1".to_string(), server("krick", 26_843_545_600)),
            ("pi5v1".to_string(), server("pi5-west", 16_106_127_360)),
            ("pi5v3".to_string(), server("pi5-east", 0)),
        ])
    }

    fn store_a() -> ObservationStore {
        let mut store = ObservationStore::new(COHORT, 0, 1000, 1 << 20, 64, committed_view_a())
            .expect("valid cohort config");
        store.register_incarnation("krick-1", inc(0x0a)).unwrap();
        store.register_incarnation("pi5v1", inc(0x0b)).unwrap();
        store.register_incarnation("pi5v3", inc(0x0c)).unwrap();
        for report in fixture_a_reports() {
            assert_eq!(store.ingest(report).expect("ingest"), IngestOutcome::Landed);
        }
        store
    }

    fn input_a(store: &ObservationStore, epoch: u64) -> TierSnapshotInput {
        TierSnapshotInput {
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
            nodes: nodes_map(),
            observations: store.observations().into_iter().cloned().collect(),
            fragment_requests: vec![req(7, "L4"), req(7, "L7")],
            committed: committed_view_a(),
            current_incarnations: store.current_incarnations().clone(),
        }
    }

    fn snapshot_a(store: &ObservationStore, epoch: u64) -> TierSnapshot {
        TierSnapshot::validated(input_a(store, epoch)).expect("fixture A validates")
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
            workspace: "ws-court".to_string(),
            collection: "cases".to_string(),
            derived_fingerprint: decl_fp(),
            topology_generation: 9,
            shards,
        }
    }

    fn store_m() -> ObservationStore {
        let mut store = ObservationStore::new(COHORT, 0, 1000, 1 << 20, 64, committed_view_m())
            .expect("valid cohort config");
        store.register_incarnation("pi5v3", inc(0x0c)).unwrap();
        for report in fixture_m_reports() {
            assert_eq!(store.ingest(report).expect("ingest"), IngestOutcome::Landed);
        }
        store
    }

    fn input_m(store: &ObservationStore) -> TierSnapshotInput {
        TierSnapshotInput {
            observation_epoch: 119,
            fragment_requests: vec![req(5, "L4"), req(6, "L4")],
            committed: committed_view_m(),
            observations: store.observations().into_iter().cloned().collect(),
            current_incarnations: store.current_incarnations().clone(),
            ..input_a(&store_a(), 119)
        }
    }

    fn snapshot_m(store: &ObservationStore) -> TierSnapshot {
        TierSnapshot::validated(input_m(store)).expect("fixture M validates")
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
        assert!(
            bad_floor
                .validate()
                .unwrap_err()
                .contains("outside [1, 1024]")
        );
        // A giant floor is a policy error, not a work order.
        let giant_floor = TierPolicy {
            tiers: vec![tier("hot", 1_000_000, 100, 1_000, 0)],
        };
        assert_eq!(
            giant_floor.validate().unwrap_err(),
            "tier policy: tier \"hot\" floor 1000000 is outside [1, 1024]"
        );
        let device_tier = TierPolicy {
            tiers: vec![CapacityTier {
                residency: NodeResidency::Device,
                ..tier("cold", 1, 0, 1, 0)
            }],
        };
        assert!(
            device_tier
                .validate()
                .unwrap_err()
                .contains("non-SERVER residency")
        );
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
    fn permuted_inputs_yield_identical_validated_plan_bytes() {
        // Reverse the ingest order.
        let store = store_a();
        let mut store2 = ObservationStore::new(COHORT, 0, 1000, 1 << 20, 64, committed_view_a())
            .expect("valid cohort config");
        store2.register_incarnation("krick-1", inc(0x0a)).unwrap();
        store2.register_incarnation("pi5v1", inc(0x0b)).unwrap();
        store2.register_incarnation("pi5v3", inc(0x0c)).unwrap();
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
        let forward = plan_tiers(&snapshot_a(&store, 118))
            .expect("plan")
            .canonical_bytes();
        let reversed = plan_tiers(&snapshot_a(&store2, 118))
            .expect("plan")
            .canonical_bytes();
        assert_eq!(forward, reversed);

        // Reverse the fragment requests, the pools, and the committed
        // copies: the constructor canonicalizes, so the plans agree.
        let store = store_a();
        let mut input = input_a(&store, 118);
        input.fragment_requests.reverse();
        for shard in input.committed.shards.values_mut() {
            for coverage in shard.leaves.values_mut() {
                coverage.pool.reverse();
                coverage.copies.reverse();
            }
        }
        let canonical = TierSnapshot::validated(input).expect("valid");
        assert_eq!(
            plan_tiers(&canonical).expect("plan").canonical_bytes(),
            forward
        );
    }

    #[test]
    fn two_fragments_competing_for_one_destination_plan_canonically() {
        // Pre-fix, fragment order decided which violation claimed the
        // capacity. The canonical order — bucket 5 before bucket 6 —
        // now decides, identically for both input orders.
        let store = store_m();
        let mut input = input_m(&store);
        input.nodes.get_mut("krick-1").unwrap().capacity.total_bytes = 22_000_000_000;
        input.nodes.get_mut("pi5v1").unwrap().capacity.total_bytes = 0;
        let mut reversed = input.clone();
        reversed.fragment_requests.reverse();
        let a = plan_tiers(&TierSnapshot::validated(input).expect("valid")).expect("plan");
        let b = plan_tiers(&TierSnapshot::validated(reversed).expect("valid")).expect("plan");
        assert_eq!(a.canonical_bytes(), b.canonical_bytes());
        assert_eq!(a.moves.len(), 1);
        assert_eq!(a.moves[0].fragment.partition.bucket, 5);
        assert_eq!(a.moves[0].destination_node, "krick-1");
        assert_eq!(a.refusals.len(), 1);
        assert!(a.refusals[0].contains("key_bucket=6"));
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

        assert_eq!(response.refusals.len(), 1);
        assert_eq!(
            response.refusals[0],
            "plan_tiers: tier \"archive\" floor is 2 for partition key_bucket=6, but no eligible destination in leaf L4 has capacity: krick-1 has 5368709120 free after earlier planned moves, pi5v1 has 16106127360 free, the fragment needs 19327352832"
        );
    }

    #[test]
    fn validated_constructor_enforces_resource_and_identity_binding() {
        let store = store_a();

        // A report from another resource refuses; order does not matter.
        let mut forged = input_a(&store, 118);
        let mut alien = forged.observations[0].clone();
        alien.partition.collection = "briefs".to_string();
        forged.observations.push(alien.clone());
        assert!(
            TierSnapshot::validated(forged)
                .unwrap_err()
                .contains("outside the snapshot's resource")
        );
        let mut reversed = input_a(&store, 118);
        reversed.observations.insert(0, alien);
        assert!(
            TierSnapshot::validated(reversed)
                .unwrap_err()
                .contains("outside the snapshot's resource")
        );

        // A committed view of another resource refuses.
        let mut wrong = input_a(&store, 118);
        wrong.committed.collection = "briefs".to_string();
        assert!(
            TierSnapshot::validated(wrong)
                .unwrap_err()
                .contains("is not the snapshot's")
        );

        // Duplicate full-identity observations refuse; nothing overwrites.
        let mut dup = input_a(&store, 118);
        let copy = dup.observations[0].clone();
        dup.observations.push(copy);
        assert!(
            TierSnapshot::validated(dup)
                .unwrap_err()
                .contains("duplicate observation")
        );

        // A duplicate fragment request refuses.
        let mut dup_req = input_a(&store, 118);
        dup_req.fragment_requests.push(req(7, "L4"));
        assert!(
            TierSnapshot::validated(dup_req)
                .unwrap_err()
                .contains("duplicate fragment request")
        );

        // A report from a superseded incarnation refuses even when the
        // store's own ingest was bypassed.
        let mut stale = input_a(&store, 118);
        stale
            .current_incarnations
            .insert("krick-1".to_string(), inc(0x1a));
        assert!(
            TierSnapshot::validated(stale)
                .unwrap_err()
                .contains("is superseded by")
        );

        // Store ingest binds the resource too.
        let mut store = ObservationStore::new(COHORT, 0, 1000, 1 << 20, 64, committed_view_a())
            .expect("valid cohort config");
        store.register_incarnation("krick-1", inc(0x0a)).unwrap();
        let mut alien = obs(7, &K1_S6, "L4", "s6", 3, 5, 1_000_000, 268_435_456, 0, 0);
        alien.partition.collection = "briefs".to_string();
        assert!(
            store
                .ingest(alien)
                .unwrap_err()
                .contains("is not this resource")
        );
    }

    #[test]
    fn plan_digest_binds_every_effective_input() {
        let store = store_a();
        let base = snapshot_a(&store, 118).plan_digest();
        assert_eq!(hex32(&base), PLAN_A_HEX);

        let mutators: Vec<(&str, fn(&mut TierSnapshotInput))> = vec![
            ("capacity", |i| {
                i.nodes.get_mut("krick-1").unwrap().capacity.total_bytes = 1;
            }),
            ("failure domain", |i| {
                i.nodes.get_mut("krick-1").unwrap().failure_domain = "krick-2".to_string();
            }),
            ("resident bytes", |i| {
                i.nodes.get_mut("pi5v1").unwrap().capacity.resident_bytes = 4096;
            }),
            ("node set", |i| {
                i.nodes.insert(
                    "pi5v4".to_string(),
                    NodeRecord {
                        residency: NodeResidency::Server,
                        eligible: true,
                        failure_domain: "pi5-north".to_string(),
                        capacity: NodeCapacity {
                            total_bytes: 0,
                            resident_bytes: 0,
                        },
                    },
                );
            }),
            ("fragment set", |i| {
                i.fragment_requests.pop();
            }),
            ("observation epoch", |i| i.observation_epoch += 1),
            ("max moves", |i| i.max_moves = 8),
            ("provider digest", |i| i.provider_geometry_digest[0] ^= 1),
        ];
        for (name, mutate) in mutators {
            let mut input = input_a(&store, 118);
            mutate(&mut input);
            let snapshot = TierSnapshot::validated(input)
                .unwrap_or_else(|e| panic!("mutation {name} must stay valid: {e}"));
            assert_ne!(
                snapshot.plan_digest(),
                base,
                "mutating {name} must change the plan digest"
            );
        }

        // The cohort is an input too; changing it invalidates the stored
        // windows, which the constructor refuses by name.
        let mut cohort = input_a(&store, 118);
        cohort.cohort_length_ms = 700_000;
        assert!(
            TierSnapshot::validated(cohort)
                .unwrap_err()
                .contains("not a cohort window")
        );
        let mut zero = input_a(&store, 118);
        zero.cohort_length_ms = 0;
        assert!(
            TierSnapshot::validated(zero)
                .unwrap_err()
                .contains("outside (0, 2^40]")
        );
    }

    #[test]
    fn residency_and_eligibility_are_verified_not_assumed() {
        let store = store_a();

        // A complete copy on a device-local source refuses validation.
        let mut device = input_a(&store, 118);
        device.nodes.get_mut("pi5v3").unwrap().residency = NodeResidency::Device;
        assert!(
            TierSnapshot::validated(device)
                .unwrap_err()
                .contains("not server-resident")
        );

        // Unspecified residency refuses the same way.
        let mut unknown = input_a(&store, 118);
        unknown.nodes.get_mut("pi5v3").unwrap().residency = NodeResidency::Unspecified;
        assert!(
            TierSnapshot::validated(unknown)
                .unwrap_err()
                .contains("not server-resident")
        );

        // An ineligible node cannot hold a verified copy.
        let mut ineligible = input_a(&store, 118);
        ineligible.nodes.get_mut("pi5v3").unwrap().eligible = false;
        assert!(
            TierSnapshot::validated(ineligible)
                .unwrap_err()
                .contains("ineligible node")
        );

        // Complete copies under different coverage digests cannot be one
        // source version.
        let mut forged = input_a(&store, 118);
        forged
            .committed
            .shards
            .get_mut("s6")
            .unwrap()
            .leaves
            .get_mut("L4")
            .unwrap()
            .copies[2]
            .coverage_digest = cov_l7();
        assert!(
            TierSnapshot::validated(forged)
                .unwrap_err()
                .contains("different coverage digests")
        );

        // A device node inside a server pool is never a destination.
        let mut store = ObservationStore::new(COHORT, 0, 1000, 1 << 20, 64, committed_view_m())
            .expect("valid cohort config");
        store.register_incarnation("pi5v3", inc(0x0c)).unwrap();
        for report in fixture_m_reports() {
            store.ingest(report).expect("ingest");
        }
        let mut input = input_m(&store);
        input
            .committed
            .shards
            .get_mut("s6")
            .unwrap()
            .leaves
            .get_mut("L4")
            .unwrap()
            .pool = vec!["pi5v3".to_string(), "pi5v2".to_string()];
        input.nodes.insert(
            "pi5v2".to_string(),
            NodeRecord {
                residency: NodeResidency::Device,
                eligible: true,
                failure_domain: "pi5-south".to_string(),
                capacity: NodeCapacity {
                    total_bytes: 1_000_000_000_000,
                    resident_bytes: 0,
                },
            },
        );
        input.nodes.remove("krick-1");
        input.nodes.remove("pi5v1");
        let response = plan_tiers(&TierSnapshot::validated(input).expect("valid")).expect("plan");
        assert_eq!(
            response.moves.len(),
            0,
            "the device pool member is excluded"
        );
        assert!(response.refusals.iter().all(|r| !r.contains("pi5v2 free")));
    }

    #[test]
    fn checked_arithmetic_and_store_bounds_hold_state_intact() {
        // Epoch exhaustion refuses before mutating: the report is not
        // inserted, and a later exact retry of a stored report still
        // works.
        let mut store = store_a();
        store.epoch = u64::MAX;
        let fresh = obs(
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
        );
        let count_before = store.observations().len();
        assert_eq!(
            store.ingest(fresh).unwrap_err(),
            "report_shard: the observation epoch is exhausted"
        );
        assert_eq!(store.observations().len(), count_before);
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
            store.ingest(retry).expect("retry"),
            IngestOutcome::Unchanged
        );
        assert_eq!(
            store.commit_expiry(W1 + COHORT, 1000).unwrap_err(),
            "report_shard: the observation epoch is exhausted"
        );
        assert_eq!(
            store.observations().len(),
            count_before,
            "expiry changed nothing"
        );

        // The incarnation registry is bounded.
        let mut tight = ObservationStore::new(COHORT, 0, 1000, 1 << 20, 1, committed_view_a())
            .expect("valid cohort config");
        tight.register_incarnation("krick-1", inc(0x0a)).unwrap();
        assert_eq!(
            tight.register_incarnation("pi5v1", inc(0x0b)).unwrap_err(),
            "report_shard: the incarnation registry holds the bound of 1 nodes"
        );

        // Retained accounting charges allocation capacity, not length:
        // a short string with a large capacity defeats a tight bound.
        let mut over = obs(7, &K1_S6, "L4", "s6", 3, 5, 1_000_000, 268_435_456, 0, 0);
        over.partition.column = {
            let mut s = String::with_capacity(4096);
            s.push_str("key_bucket");
            s
        };
        assert!(over.heap_bytes() >= 2 * 4096);
        let mut tight_bytes = ObservationStore::new(COHORT, 0, 1000, 100, 64, committed_view_a())
            .expect("valid cohort config");
        tight_bytes
            .register_incarnation("krick-1", inc(0x0a))
            .unwrap();
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
    }

    #[test]
    fn cohort_alignment_rules() {
        let mut store = ObservationStore::new(COHORT, 0, 1000, 1 << 20, 64, committed_view_a())
            .expect("valid cohort config");
        store.register_incarnation("krick-1", inc(0x0a)).unwrap();

        // E10: a 660-second, off-grid window refuses by name.
        let mut misaligned = obs(7, &K1_S6, "L4", "s6", 3, 5, 1_000_000, 268_435_456, 0, 0);
        misaligned.window_start_unix_ms = T - 660_000;
        misaligned.window_end_unix_ms = T - 60_000;
        assert_eq!(
            store.ingest(misaligned).unwrap_err(),
            "report_shard: window [1788868140000, 1788868740000) is not a cohort window (length 600000 ms, phase 0)"
        );

        let mut inverted = obs(7, &K1_S6, "L4", "s6", 3, 5, 1_000_000, 268_435_456, 0, 0);
        inverted.window_start_unix_ms = W1;
        inverted.window_end_unix_ms = W0;
        assert!(store.ingest(inverted).unwrap_err().contains("inverted"));

        assert!(ObservationStore::new((1 << 40) + 1, 0, 10, 1000, 64, committed_view_a()).is_err());

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
        let mut store = ObservationStore::new(COHORT, 0, 1000, 1 << 20, 64, committed_view_a())
            .expect("valid cohort config");
        store.register_incarnation("krick-1", inc(0x0a)).unwrap();
        store.register_incarnation("pi5v1", inc(0x0b)).unwrap();
        store.register_incarnation("pi5v3", inc(0x0c)).unwrap();
        for report in fixture_a_reports().into_iter().skip(1) {
            store.ingest(report).expect("ingest");
        }
        let snapshot = snapshot_a(&store, 118);
        assert_eq!(
            plan_tiers(&snapshot).unwrap_err(),
            "plan_tiers: partition key_bucket=7 on node krick-1 has no current observation; unknown is not idle"
        );

        // E4's boundary arithmetic, with cohort-aligned literals.
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
        store.register_incarnation("krick-1", inc(0x1a)).unwrap();
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
        let mut store = ObservationStore::new(COHORT, 0, 1000, 1 << 20, 64, committed_view_a())
            .expect("valid cohort config");
        store.register_incarnation("krick-1", inc(0x0a)).unwrap();
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

        let mut store2 = ObservationStore::new(COHORT, 0, 1000, 1 << 20, 64, committed_view_a())
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
        let mut store = ObservationStore::new(COHORT, 0, 1000, 1 << 20, 64, committed_view_a())
            .expect("valid cohort config");
        store.register_incarnation("krick-1", inc(0x0a)).unwrap();
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

        let mut tight = ObservationStore::new(COHORT, 0, 1, 1 << 20, 64, committed_view_a())
            .expect("valid cohort config");
        tight.register_incarnation("krick-1", inc(0x0a)).unwrap();
        tight.register_incarnation("pi5v1", inc(0x0b)).unwrap();
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

        let mut store = store_a();
        let dropped = store.commit_expiry(W1 + COHORT, 1000).expect("expiry");
        assert_eq!(dropped, 5);
        assert_eq!(store.observations().len(), 0);
        assert_eq!(store.commit_expiry(W1 + COHORT, 1000).expect("expiry"), 0);
    }

    #[test]
    fn partial_copies_are_not_replicas_and_domains_count_once() {
        // E2: bucket 9's copies are two disjoint partials — zero complete
        // replicas, so the floor refuses instead of planning a move.
        let mut partial_a = committed_copy(&P1_S6, cov_l4());
        partial_a.complete = false;
        let mut partial_b = committed_copy(&P3_S6, cov_l4());
        partial_b.complete = false;
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
                        copies: vec![partial_a, partial_b],
                    },
                )]),
            },
        );
        let committed = CommittedView {
            workspace: "ws-court".to_string(),
            collection: "cases".to_string(),
            derived_fingerprint: decl_fp(),
            topology_generation: 9,
            shards,
        };
        let mut store = ObservationStore::new(COHORT, 0, 1000, 1 << 20, 64, committed.clone())
            .expect("valid cohort config");
        store.register_incarnation("pi5v3", inc(0x0c)).unwrap();
        store
            .ingest(obs(
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
            ))
            .expect("ingest");
        let mut input = input_m(&store_m());
        input.committed = committed;
        input.observations = store.observations().into_iter().cloned().collect();
        input.current_incarnations = store.current_incarnations().clone();
        input.fragment_requests = vec![req(9, "L4")];
        let response = plan_tiers(&TierSnapshot::validated(input).expect("valid")).expect("plan");
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
        let mut committed = committed_view_a();
        {
            let coverage = committed
                .shards
                .get_mut("s6")
                .unwrap()
                .leaves
                .get_mut("L4")
                .unwrap();
            coverage.copies[2].failure_domain = "pi5-west".to_string();
            coverage.pool.push("pi5v4".to_string());
        }
        let mut store = ObservationStore::new(COHORT, 0, 1000, 1 << 20, 64, committed.clone())
            .expect("valid cohort config");
        store.register_incarnation("krick-1", inc(0x0a)).unwrap();
        store.register_incarnation("pi5v1", inc(0x0b)).unwrap();
        store.register_incarnation("pi5v3", inc(0x0c)).unwrap();
        for report in fixture_a_reports() {
            store.ingest(report).expect("ingest");
        }
        let mut input = input_a(&store, 118);
        input.committed = committed;
        input.nodes.insert(
            "pi5v4".to_string(),
            NodeRecord {
                residency: NodeResidency::Server,
                eligible: true,
                failure_domain: "pi5-north".to_string(),
                capacity: NodeCapacity {
                    total_bytes: 1_000_000_000_000,
                    resident_bytes: 0,
                },
            },
        );
        let response = plan_tiers(&TierSnapshot::validated(input).expect("valid")).expect("plan");
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
    fn numeric_bounds_are_checked_not_wrapping() {
        let s = u64::MAX as u128;
        let b = u64::MAX as u128;
        let expected = (s * 1_000_000_000_000u128) / (600_000u128 * b);
        assert_eq!(rate_nanos(s, 600_000, b), u64::try_from(expected).unwrap());

        assert!(ensure_fan_in(1 << 20).is_ok());
        assert!(ensure_fan_in((1 << 20) + 1).unwrap_err().contains("fan-in"));

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

//! Partition observations and the store that admits them cohort by cohort.

use std::collections::BTreeMap;

use crate::sha256;

use super::{
    hex16, put_str, put_u32, put_u64, ENCODING_VERSION, MAX_COHORT_LENGTH_MS, OBSERVATIONS_DOMAIN,
};

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
    pub(super) fn label(&self) -> String {
        format!("{}={}", self.column, self.bucket)
    }

    pub(super) fn put_canonical(&self, out: &mut Vec<u8>) {
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
    pub(super) fn heap_bytes(&self) -> usize {
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
    pub(super) epoch: u64,
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
pub(super) fn check_observation_identity(
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

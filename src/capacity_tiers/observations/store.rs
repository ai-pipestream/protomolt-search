//! The bounded observation store: registration, expiry, and digests.

use std::collections::BTreeMap;

use crate::capacity_tiers::{put_u32, ENCODING_VERSION, MAX_COHORT_LENGTH_MS, OBSERVATIONS_DOMAIN};

use super::committed::CommittedView;
use super::identity::{ObsKey, PartitionObservation};

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

pub(super) struct StoredObservation {
    pub(super) obs: PartitionObservation,
    pub(super) heap_bytes: usize,
}

/// §3: the bounded store of capacity observations. It enforces resource
/// binding and cohort alignment, identity equality against the committed
/// view, incarnation supersession, exact-retry idempotence,
/// conflicting-report refusals, and total retained-record, retained-byte,
/// and registered-node bounds. The observation epoch advances only on
/// transitions that change the stored set, with the increment checked
/// before any mutation.
pub struct ObservationStore {
    pub(super) cohort_length_ms: u64,
    pub(super) cohort_phase_unix_ms: u64,
    pub(super) max_total_records: usize,
    pub(super) max_total_bytes: usize,
    pub(super) max_registered_nodes: usize,
    pub(super) committed: CommittedView,
    pub(super) current_incarnation: BTreeMap<String, [u8; 16]>,
    pub(super) records: BTreeMap<ObsKey, StoredObservation>,
    pub(in crate::capacity_tiers) epoch: u64,
    pub(super) retained_bytes: usize,
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

    pub(super) fn next_epoch(&self) -> Result<u64, String> {
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
        turbo_sha256::sha256(&out).0
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
    turbo_sha256::sha256(&out).0
}

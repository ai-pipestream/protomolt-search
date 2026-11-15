//! Report ingest: the checks one report passes before it is stored.

use crate::capacity_tiers::hex16;

use super::committed::check_observation_identity;
use super::identity::{ObsKey, PartitionObservation};
use super::store::{IngestOutcome, ObservationStore, StoredObservation};

impl ObservationStore {
    /// Validate and store one report (§3). Refusals name the route, the
    /// field, the carried value, and the committed value.
    pub fn ingest(&mut self, obs: PartitionObservation) -> Result<IngestOutcome, String> {
        let key = obs.key();
        self.check_resource(&obs)?;
        self.check_cohort_window(&obs)?;
        check_possible_state(&obs)?;
        check_observation_identity(&self.committed, &obs)?;
        self.check_incarnation(&obs)?;
        if let Some(outcome) = self.stored_outcome(&key, &obs)? {
            return Ok(outcome);
        }
        self.admit(key, obs)
    }

    /// Resource binding, before anything else: the committed view serves
    /// one (workspace, collection, declaration).
    fn check_resource(&self, obs: &PartitionObservation) -> Result<(), String> {
        if self.committed.resource_matches(&obs.partition) {
            return Ok(());
        }
        Err(format!(
            "report_shard: partition {}/{}/{} is not this resource {}/{}/{}",
            obs.partition.workspace,
            obs.partition.collection,
            hex16(&obs.partition.derived_fingerprint[..16].try_into().unwrap()),
            self.committed.workspace,
            self.committed.collection,
            hex16(&self.committed.derived_fingerprint[..16].try_into().unwrap())
        ))
    }

    /// Cohort alignment (E10).
    fn check_cohort_window(&self, obs: &PartitionObservation) -> Result<(), String> {
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
        Ok(())
    }

    /// Incarnation: only the node's current process may report (E5).
    fn check_incarnation(&self, obs: &PartitionObservation) -> Result<(), String> {
        match self.current_incarnation.get(&obs.reporter.node_id) {
            Some(current) if *current == obs.reporter.process_incarnation => Ok(()),
            Some(current) => Err(format!(
                "report_shard: node {} incarnation {} is superseded by {}",
                obs.reporter.node_id,
                hex16(&obs.reporter.process_incarnation),
                hex16(current)
            )),
            None => Err(format!(
                "report_shard: node {} has not registered an incarnation",
                obs.reporter.node_id
            )),
        }
    }

    /// Retry and supersession rules against the stored record: an exact
    /// retry and an older cohort window both leave the set unchanged, and
    /// a same-window report with other figures refuses (§3, E6).
    fn stored_outcome(
        &self,
        key: &ObsKey,
        obs: &PartitionObservation,
    ) -> Result<Option<IngestOutcome>, String> {
        let Some(stored) = self.records.get(key) else {
            return Ok(None);
        };
        if stored.obs.window_end_unix_ms == obs.window_end_unix_ms {
            if stored.obs.payload_bytes() == obs.payload_bytes() {
                return Ok(Some(IngestOutcome::Unchanged));
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
            return Ok(Some(IngestOutcome::Superseded));
        }
        Ok(None)
    }

    /// Bounds: total retained records and bytes, not only per-fragment
    /// fan-in. Replacing an existing key first frees its charge. Every
    /// bound and the epoch increment are checked before any mutation.
    fn admit(&mut self, key: ObsKey, obs: PartitionObservation) -> Result<IngestOutcome, String> {
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
}

/// Impossible states are refusals, never zeroes (§3).
fn check_possible_state(obs: &PartitionObservation) -> Result<(), String> {
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
    Ok(())
}

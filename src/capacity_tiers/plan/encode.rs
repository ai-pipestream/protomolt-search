//! The plan response's deterministic serialized form.

use crate::capacity_tiers::snapshot::put_copy;
use crate::capacity_tiers::{put_str, put_u32, put_u64};

use super::PlanTiersResponse;

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

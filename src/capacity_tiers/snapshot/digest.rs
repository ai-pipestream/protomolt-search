//! The snapshot's canonical digests: nodes, fragments, and the plan input.

use crate::capacity_tiers::observations::observation_set_digest;
use crate::capacity_tiers::{
    put_str, put_u32, put_u64, ENCODING_VERSION, FRAGMENTS_DOMAIN, NODES_DOMAIN, PLAN_DOMAIN,
    PLAN_ENCODING_VERSION,
};

use super::TierSnapshot;

impl TierSnapshot {
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
        turbo_sha256::sha256(&out).0
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
        turbo_sha256::sha256(&out).0
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
        turbo_sha256::sha256(&out).0
    }
}

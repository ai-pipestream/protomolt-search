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

mod observations;
mod plan;
mod snapshot;
#[cfg(test)]
mod tests;

pub use self::observations::{
    observation_set_digest, CommittedCopy, CommittedShard, CommittedView, IngestOutcome,
    LeafCoverage, ObsKey, ObservationStore, PartitionIdentity, PartitionObservation,
    ReporterIdentity, ShardRef,
};
pub use self::plan::{plan_tiers, FragmentPlacement, PlanTiersResponse, ReporterFigures, TierMove};
pub use self::snapshot::{
    FragmentRecord, FragmentRequest, NodeCapacity, NodeRecord, TierSnapshot, TierSnapshotInput,
};

use crate::sha256;

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

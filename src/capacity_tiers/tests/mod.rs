//! Capacity-tier tests over the pinned example fixtures.

mod fixtures;
mod plans;
mod store;
mod validation;

use std::collections::BTreeMap;

use super::plan::{ensure_fan_in, rate_nanos};
use super::*;

use self::fixtures::*;

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
// Plan-input encoding v2 digests: they bind the cohort and the
// node/fragment subdigests, and match the independent fixture verifier.
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
    turbo_sha256::to_hex(bytes)
}

fn decl_fp() -> [u8; 32] {
    turbo_sha256::sha256(b"key_bucket = hash.fnv64(stable_key()) % 64u").0
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
    turbo_sha256::sha256(
        b"example manifest: fragment (key_bucket=7, gen 9, L4), source generation 3, rows 1000000, bytes 268435456",
    ).0
}

fn cov_l7() -> [u8; 32] {
    turbo_sha256::sha256(
        b"example manifest: fragment (key_bucket=7, gen 9, L7), source generation 3, rows 500000, bytes 134217728",
    ).0
}

fn tree_digest() -> [u8; 32] {
    turbo_sha256::sha256(
        b"example placement tree: gen 9, leaf L4 = {krick-1, pi5v1, pi5v3}, leaf L7 = {krick-1, pi5v1}",
    ).0
}

fn prov_digest() -> [u8; 32] {
    turbo_sha256::sha256(
        b"example provider geometry: turbovec 64-shard mapped image; committed free bytes: krick-1=26843545600, pi5v1=16106127360, pi5v3=0",
    ).0
}

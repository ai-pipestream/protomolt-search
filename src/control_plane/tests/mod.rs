//! Control-plane tests: balance planning, reconciliation, and topology.

mod balance;
mod reconcile;
mod topology;

use std::ops::RangeInclusive;

use crate::pb::{
    CompletePlacementActionRequest, DrainNodeRequest, NodeCapacity, NodeLease, NodeResidency,
    PlacementActionKind, RegisterNodeRequest, RenewNodeLeaseRequest, ReportShardRequest,
    ShardReplicaRole, ShardReplicaState,
};

use super::*;

fn register(
    plane: &DurableControlPlane,
    id: &str,
    addr: &str,
    domain: &str,
    now: u64,
) -> NodeLease {
    plane
        .register(
            RegisterNodeRequest {
                collection: String::new(),
                node_id: id.into(),
                addr: addr.into(),
                capacity: Some(NodeCapacity {
                    disk_bytes: 1_000,
                    failure_domain: domain.into(),
                    ..Default::default()
                }),
                lease_ms: 10_000,
            },
            now,
        )
        .unwrap()
}

/// A ready replica of `shard` at generation 7 over `range`, with the
/// fixture fingerprints.
fn replica_state(
    shard: &str,
    role: ShardReplicaRole,
    rows: u64,
    segments: u32,
    range: RangeInclusive<u64>,
) -> ShardReplicaState {
    ShardReplicaState {
        shard_id: shard.into(),
        generation: 7,
        hash_lo: *range.start(),
        hash_hi: *range.end(),
        rows,
        role: role as i32,
        ready: true,
        scoring_fingerprint: "score-v1".into(),
        analysis_fingerprint: "analysis-v1".into(),
        immutable_segments: segments,
        ..Default::default()
    }
}

/// Report `replica` as held by `lease`'s node.
fn report_replica(
    plane: &DurableControlPlane,
    lease: &NodeLease,
    replica: ShardReplicaState,
    now: u64,
) {
    plane
        .report(
            ReportShardRequest {
                collection: String::new(),
                node_id: lease.node_id.clone(),
                lease_token: lease.lease_token,
                replica: Some(replica),
            },
            now,
        )
        .unwrap();
}

/// Report a replica of `shard` that covers the whole hash space.
fn report(
    plane: &DurableControlPlane,
    lease: &NodeLease,
    shard: &str,
    role: ShardReplicaRole,
    rows: u64,
    segments: u32,
    now: u64,
) {
    let replica = replica_state(shard, role, rows, segments, 0..=u64::MAX);
    report_replica(plane, lease, replica, now);
}

/// A node with an observed rate and a residency, renewed so the
/// capacity is stored.
fn measured(
    plane: &DurableControlPlane,
    lease: &NodeLease,
    domain: &str,
    rate: u64,
    observed: u64,
    residency: NodeResidency,
    now: u64,
) {
    plane
        .renew(
            RenewNodeLeaseRequest {
                collection: String::new(),
                node_id: lease.node_id.clone(),
                lease_token: lease.lease_token,
                capacity: Some(NodeCapacity {
                    disk_bytes: 1_000,
                    failure_domain: domain.into(),
                    scan_bytes_per_second: rate,
                    scan_rate_observed_unix_ms: observed,
                    scan_rate_samples: 8,
                    scan_rate_window_ms: 600_000,
                    residency: residency as i32,
                    ..Default::default()
                }),
                lease_ms: 10_000,
            },
            now,
        )
        .unwrap();
}

/// A primary shard on `lease`'s node at its own listener address.
fn primary_at(
    plane: &DurableControlPlane,
    lease: &NodeLease,
    shard: &str,
    addr: &str,
    rows: u64,
    now: u64,
) {
    plane
        .report(
            ReportShardRequest {
                collection: String::new(),
                node_id: lease.node_id.clone(),
                lease_token: lease.lease_token,
                replica: Some(ShardReplicaState {
                    shard_id: shard.into(),
                    addr: addr.into(),
                    generation: 7,
                    hash_lo: 0,
                    hash_hi: u64::MAX,
                    rows,
                    role: ShardReplicaRole::Primary as i32,
                    ready: true,
                    scoring_fingerprint: "score-v1".into(),
                    analysis_fingerprint: "analysis-v1".into(),
                    immutable_segments: 1,
                    ..Default::default()
                }),
            },
            now,
        )
        .unwrap();
}

fn action_output(
    shard_id: &str,
    generation: u64,
    hash_lo: u64,
    hash_hi: u64,
    rows: u64,
) -> ShardReplicaState {
    ShardReplicaState {
        shard_id: shard_id.into(),
        generation,
        hash_lo,
        hash_hi,
        rows,
        role: ShardReplicaRole::Primary as i32,
        ready: true,
        scoring_fingerprint: "score-v1".into(),
        analysis_fingerprint: "analysis-v1".into(),
        immutable_segments: 1,
        ..Default::default()
    }
}

/// Complete `action_id` as `lease`'s node with `outputs`.
fn complete(
    plane: &DurableControlPlane,
    lease: &NodeLease,
    action_id: u64,
    outputs: Vec<ShardReplicaState>,
    now: u64,
) -> Result<(), tonic::Status> {
    plane.complete_action(
        CompletePlacementActionRequest {
            collection: String::new(),
            node_id: lease.node_id.clone(),
            lease_token: lease.lease_token,
            action_id,
            outputs,
        },
        now,
    )
}

//! Topology bootstrap and the split and merge completions that replace it.

use crate::coordinator::{CoordinatorServiceImpl, TopologyRoute};

use super::*;

#[test]
fn pristine_state_bootstraps_the_live_topology_without_renumbering_it() {
    let plane = DurableControlPlane::in_memory(ControlPolicy {
        replication_factor: 1,
        ..Default::default()
    });
    let routes = vec![TopologyRoute {
        addr: "http://a".into(),
        replica: None,
        hash_range: Some((0, u64::MAX)),
        placement: None,
    }];
    plane.bootstrap_topology(4, &routes).unwrap();
    let node = register(&plane, "a", "http://a", "az-a", 100);
    report(&plane, &node, "s0", ShardReplicaRole::Primary, 10, 1, 100);
    let (plan, changed) = plane.reconcile(false, 100).unwrap();
    assert!(!changed);
    assert_eq!(plan.topology_generation, 4);

    let coordinator = CoordinatorServiceImpl::new(vec!["http://a".into()])
        .with_topology_generation(4)
        .with_hot_topology(vec![Some((0, u64::MAX))])
        .unwrap();
    ClusterControlService::new(plane)
        .with_coordinator(coordinator)
        .publish_current_topology()
        .unwrap();
}

#[test]
fn bootstrap_requires_ranges_and_never_overwrites_durable_authority() {
    let plane = DurableControlPlane::in_memory(ControlPolicy::default());
    let error = plane
        .bootstrap_topology(
            1,
            &[TopologyRoute {
                addr: "http://a".into(),
                replica: None,
                hash_range: None,
                placement: None,
            }],
        )
        .unwrap_err();
    assert!(error.contains("missing its hash range"));

    plane
        .bootstrap_topology(
            4,
            &[TopologyRoute {
                addr: "http://a".into(),
                replica: None,
                hash_range: Some((0, u64::MAX)),
                placement: None,
            }],
        )
        .unwrap();
    plane
        .bootstrap_topology(
            5,
            &[TopologyRoute {
                addr: "http://b".into(),
                replica: None,
                hash_range: Some((0, u64::MAX)),
                placement: None,
            }],
        )
        .unwrap();
    let (generation, routes) = plane.topology_routes().unwrap();
    assert_eq!(generation, 4);
    assert_eq!(routes[0].addr, "http://a");
}

#[test]
fn split_completion_conserves_rows_and_atomically_replaces_topology() {
    let plane = DurableControlPlane::in_memory(ControlPolicy {
        replication_factor: 1,
        split_rows: 100,
        merge_rows: 0,
        compact_segments: u32::MAX,
        compact_tombstone_ppm: 1_000_000,
        ..Default::default()
    });
    let node = register(&plane, "a", "http://a", "az-a", 100);
    report(
        &plane,
        &node,
        "parent",
        ShardReplicaRole::Primary,
        101,
        1,
        100,
    );
    let (initial, _) = plane.reconcile(false, 100).unwrap();
    let action = initial
        .actions
        .iter()
        .find(|action| action.kind == PlacementActionKind::SplitShard as i32)
        .unwrap()
        .clone();
    let midpoint = u64::MAX / 2;
    let invalid = CompletePlacementActionRequest {
        collection: String::new(),
        node_id: node.node_id.clone(),
        lease_token: node.lease_token,
        action_id: action.action_id,
        outputs: vec![
            action_output("left", 8, 0, midpoint, 50),
            action_output("right", 8, midpoint + 1, u64::MAX, 50),
        ],
    };
    let error = plane.complete_action(invalid, 100).unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(plane
        .plan()
        .unwrap()
        .actions
        .iter()
        .any(|held| held.action_id == action.action_id));

    let completion = CompletePlacementActionRequest {
        collection: String::new(),
        node_id: node.node_id.clone(),
        lease_token: node.lease_token,
        action_id: action.action_id,
        outputs: vec![
            action_output("left", 8, 0, midpoint, 50),
            action_output("right", 8, midpoint + 1, u64::MAX, 51),
        ],
    };
    plane.complete_action(completion.clone(), 100).unwrap();
    // Crash-window retries are acknowledgements, not duplicate mutations.
    plane.complete_action(completion, 100).unwrap();
    let (published, changed) = plane.reconcile(false, 100).unwrap();
    assert!(changed);
    assert_eq!(published.topology_generation, 2);
    assert_eq!(published.replicas.len(), 2);
    assert!(published
        .replicas
        .iter()
        .all(|replica| replica.shard_id != "parent"));
}

#[test]
fn merge_completion_replaces_both_adjacent_inputs() {
    let plane = DurableControlPlane::in_memory(ControlPolicy {
        replication_factor: 1,
        split_rows: u64::MAX,
        merge_rows: 100,
        compact_segments: u32::MAX,
        compact_tombstone_ppm: 1_000_000,
        ..Default::default()
    });
    let node = register(&plane, "a", "http://a", "az-a", 100);
    let midpoint = u64::MAX / 2;
    let left = replica_state("left", ShardReplicaRole::Primary, 10, 1, 0..=midpoint);
    report_replica(&plane, &node, left, 100);
    let right = replica_state(
        "right",
        ShardReplicaRole::Primary,
        20,
        1,
        midpoint + 1..=u64::MAX,
    );
    report_replica(&plane, &node, right, 100);
    let (initial, _) = plane.reconcile(false, 100).unwrap();
    let action = initial
        .actions
        .iter()
        .find(|action| action.kind == PlacementActionKind::MergeShards as i32)
        .unwrap();
    plane
        .complete_action(
            CompletePlacementActionRequest {
                collection: String::new(),
                node_id: node.node_id.clone(),
                lease_token: node.lease_token,
                action_id: action.action_id,
                outputs: vec![action_output("merged", 8, 0, u64::MAX, 30)],
            },
            100,
        )
        .unwrap();
    let (published, changed) = plane.reconcile(false, 100).unwrap();
    assert!(changed);
    assert_eq!(published.replicas.len(), 1);
    assert_eq!(published.replicas[0].shard_id, "merged");
    assert_eq!(published.topology_generation, 2);
}

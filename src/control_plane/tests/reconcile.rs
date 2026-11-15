//! Reconciliation: promotion, drains, listener addresses, and durability.

use crate::control_plane::state::now_ms;

use super::*;

#[test]
fn lease_expiry_promotes_ready_replica_and_preserves_history() {
    let plane = DurableControlPlane::in_memory(ControlPolicy {
        lease_ms: 10_000,
        replication_factor: 2,
        ..Default::default()
    });
    let primary = register(&plane, "a", "http://a", "az-a", 100);
    let replica = register(&plane, "b", "http://b", "az-b", 100);
    report(
        &plane,
        &primary,
        "s0",
        ShardReplicaRole::Primary,
        10,
        1,
        100,
    );
    report(
        &plane,
        &replica,
        "s0",
        ShardReplicaRole::Replica,
        10,
        1,
        100,
    );
    let (first, changed) = plane.reconcile(false, 100).unwrap();
    assert!(changed);
    assert_eq!(first.topology_generation, 1);
    plane
        .renew(
            RenewNodeLeaseRequest {
                node_id: replica.node_id.clone(),
                lease_token: replica.lease_token,
                lease_ms: 10_000,
                ..Default::default()
            },
            5_000,
        )
        .unwrap();

    let (promoted, changed) = plane.reconcile(false, 10_101).unwrap();
    assert!(changed);
    assert_eq!(promoted.topology_generation, 2);
    assert_eq!(promoted.topology_history, vec![0, 1]);
    assert_eq!(
        promoted
            .replicas
            .iter()
            .find(|held| held.node_id == "b")
            .unwrap()
            .role,
        ShardReplicaRole::Primary as i32
    );
}

#[test]
fn drain_places_a_copy_across_failure_domains_without_a_competing_rewrite() {
    let plane = DurableControlPlane::in_memory(ControlPolicy {
        replication_factor: 2,
        compact_segments: 4,
        ..Default::default()
    });
    let primary = register(&plane, "a", "http://a", "az-a", 100);
    let target = register(&plane, "b", "http://b", "az-b", 100);
    report(
        &plane,
        &primary,
        "s0",
        ShardReplicaRole::Primary,
        10,
        5,
        100,
    );
    plane
        .drain(
            DrainNodeRequest {
                collection: String::new(),
                node_id: primary.node_id.clone(),
                lease_token: primary.lease_token,
            },
            100,
        )
        .unwrap();
    let (plan, _) = plane.reconcile(false, 100).unwrap();
    assert!(plan.actions.iter().any(|action| {
        action.kind == PlacementActionKind::CopyReplica as i32
            && action.target_node_id == target.node_id
    }));
    assert!(!plan
        .actions
        .iter()
        .any(|action| { action.kind == PlacementActionKind::CompactShard as i32 }));
    let count = plan.actions.len();
    let (again, _) = plane.reconcile(false, 100).unwrap();
    assert_eq!(again.actions.len(), count);

    let copy = plan
        .actions
        .iter()
        .find(|action| action.kind == PlacementActionKind::CopyReplica as i32)
        .unwrap();
    let mut replica = action_output("s0", 7, 0, u64::MAX, 10);
    replica.role = ShardReplicaRole::Replica as i32;
    complete(&plane, &target, copy.action_id, vec![replica], 100).unwrap();
    let drop_action = plane
        .plan()
        .unwrap()
        .actions
        .into_iter()
        .find(|action| action.kind == PlacementActionKind::DropReplica as i32)
        .unwrap();
    complete(&plane, &primary, drop_action.action_id, Vec::new(), 100).unwrap();
    let (published, changed) = plane.reconcile(false, 100).unwrap();
    assert!(changed);
    assert_eq!(published.topology_generation, 1);
    assert_eq!(published.replicas.len(), 1);
    assert_eq!(published.replicas[0].node_id, target.node_id);
    assert_eq!(published.replicas[0].role, ShardReplicaRole::Primary as i32);
}

#[test]
fn a_report_names_its_own_listener_but_not_another_nodes_address() {
    let plane = DurableControlPlane::in_memory(ControlPolicy {
        replication_factor: 1,
        ..Default::default()
    });
    let a = register(&plane, "a", "http://a:1", "az-a", 100);
    let _b = register(&plane, "b", "http://b:1", "az-b", 100);
    let report = |addr: &str| ReportShardRequest {
        collection: String::new(),
        node_id: a.node_id.clone(),
        lease_token: a.lease_token,
        replica: Some(ShardReplicaState {
            shard_id: "s0".into(),
            addr: addr.into(),
            hash_hi: u64::MAX,
            rows: 10,
            role: ShardReplicaRole::Primary as i32,
            ready: true,
            scoring_fingerprint: "score-v1".into(),
            analysis_fingerprint: "analysis-v1".into(),
            ..Default::default()
        }),
    };
    // A second listener of the same node is the shard's address.
    plane.report(report("http://a:2"), 100).unwrap();
    let (plan, _) = plane.reconcile(false, 100).unwrap();
    assert_eq!(plan.replicas[0].addr, "http://a:2");
    assert_eq!(plan.topology.len(), 1);
    assert_eq!(plan.topology[0].addr, "http://a:2");
    assert_eq!(
        (plan.topology[0].hash_lo, plan.topology[0].hash_hi),
        (0, u64::MAX)
    );
    // Empty falls back to the registered address.
    plane.report(report(""), 100).unwrap();
    assert_eq!(plane.plan().unwrap().replicas[0].addr, "http://a:1");
    // Another node's registered address is not a listener of a.
    let error = plane.report(report("http://b:1"), 100).unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(
        error.message().contains("registered address of node \"b\""),
        "{}",
        error.message()
    );
}

#[test]
fn active_compaction_action_is_idempotent() {
    let plane = DurableControlPlane::in_memory(ControlPolicy {
        replication_factor: 1,
        compact_segments: 4,
        split_rows: u64::MAX,
        ..Default::default()
    });
    let primary = register(&plane, "a", "http://a", "az-a", 100);
    report(
        &plane,
        &primary,
        "s0",
        ShardReplicaRole::Primary,
        10,
        5,
        100,
    );
    let (plan, _) = plane.reconcile(false, 100).unwrap();
    assert_eq!(
        plan.actions
            .iter()
            .filter(|action| action.kind == PlacementActionKind::CompactShard as i32)
            .count(),
        1
    );
    let (again, _) = plane.reconcile(false, 100).unwrap();
    assert_eq!(again.actions.len(), plan.actions.len());
}

#[test]
fn durable_state_reopens_and_dry_run_does_not_mutate() {
    let dir = std::env::temp_dir().join(format!(
        "protomolt-control-{}-{}",
        std::process::id(),
        now_ms()
    ));
    let path = dir.join("state.json");
    let plane = DurableControlPlane::open(&path, ControlPolicy::default()).unwrap();
    register(&plane, "a", "http://a", "az-a", 100);
    let revision = plane.plan().unwrap().control_revision;
    let _ = plane.reconcile(true, 20_000).unwrap();
    assert_eq!(plane.plan().unwrap().control_revision, revision);
    drop(plane);
    let reopened = DurableControlPlane::open(&path, ControlPolicy::default()).unwrap();
    assert_eq!(reopened.plan().unwrap().nodes.len(), 1);
    std::fs::remove_dir_all(dir).unwrap();
}

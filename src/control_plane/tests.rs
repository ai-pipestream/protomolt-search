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

fn report(
    plane: &DurableControlPlane,
    lease: &NodeLease,
    shard: &str,
    role: ShardReplicaRole,
    rows: u64,
    segments: u32,
    now: u64,
) {
    report_range(plane, lease, shard, role, rows, segments, 0, u64::MAX, now);
}

#[allow(clippy::too_many_arguments)]
fn report_range(
    plane: &DurableControlPlane,
    lease: &NodeLease,
    shard: &str,
    role: ShardReplicaRole,
    rows: u64,
    segments: u32,
    hash_lo: u64,
    hash_hi: u64,
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
                    generation: 7,
                    hash_lo,
                    hash_hi,
                    rows,
                    role: role as i32,
                    ready: true,
                    scoring_fingerprint: "score-v1".into(),
                    analysis_fingerprint: "analysis-v1".into(),
                    immutable_segments: segments,
                    ..Default::default()
                }),
            },
            now,
        )
        .unwrap();
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

fn any_pool(_: &str) -> BalancePool {
    BalancePool::default()
}

/// Three measured servers with rates 100, 50 and 25 (bytes per
/// second, with row bytes of 1 so rows are bytes): the slow node
/// holds the most, the plan moves whole shards off it toward the
/// node that ends up lowest, deterministically, until a move would
/// gain less than `min_gain`.
#[test]
fn a_balance_plan_is_deterministic_and_bounded() {
    let plane = DurableControlPlane::in_memory(ControlPolicy::default());
    let now = 1_000_000;
    let a = register(&plane, "a", "10.0.0.1:1", "z1", now);
    let b = register(&plane, "b", "10.0.0.2:1", "z2", now);
    let c = register(&plane, "c", "10.0.0.3:1", "z3", now);
    measured(&plane, &a, "z1", 100, now, NodeResidency::Server, now);
    measured(&plane, &b, "z2", 50, now, NodeResidency::Server, now);
    measured(&plane, &c, "z3", 25, now, NodeResidency::Server, now);
    // c: three shards of 1000 rows (120 s); a: one of 1000 (10 s);
    // b: one of 1000 (20 s).
    primary_at(&plane, &c, "s0", "10.0.0.3:100", 1_000, now);
    primary_at(&plane, &c, "s1", "10.0.0.3:101", 1_000, now);
    primary_at(&plane, &c, "s2", "10.0.0.3:102", 1_000, now);
    primary_at(&plane, &a, "s3", "10.0.0.1:100", 1_000, now);
    primary_at(&plane, &b, "s4", "10.0.0.2:100", 1_000, now);
    let request = PlanBalanceRequest {
        collection: String::new(),
        min_gain: 0.0,
        max_moves: 0,
        max_rate_age_ms: 0,
    };
    let plan = plane.plan_balance(&request, 1, &any_pool, now).unwrap();
    assert_eq!(plan.min_gain, BALANCE_DEFAULT_MIN_GAIN);
    assert_eq!(plan.max_moves, BALANCE_DEFAULT_MAX_MOVES);
    assert_eq!(plan.control_revision, plane.state.lock().unwrap().revision);
    assert!(plan.excluded.is_empty(), "{:?}", plan.excluded);
    assert_eq!(plan.seconds_before, 120.0);
    // Greedy: s0 leaves c (120 s) for a (a 20 s; the maximum is c at
    // 80 s), then s1 leaves c for a (a 30 s, c 40 s: maximum 40 s).
    // Moving s2 too would leave a at 40 s, no lower than c's 40 s,
    // so the plan stops there.
    let moved: Vec<(String, String, String)> = plan
        .moves
        .iter()
        .map(|m| (m.from_node.clone(), m.to_node.clone(), m.leaf.clone()))
        .collect();
    assert_eq!(
        moved,
        vec![
            ("c".to_string(), "a".to_string(), String::new()),
            ("c".to_string(), "a".to_string(), String::new()),
        ],
        "{:?}",
        plan.moves
    );
    assert_eq!(plan.moves[0].bytes, 1_000);
    assert_eq!(plan.moves[0].seconds_after, 80.0);
    assert_eq!(plan.moves[1].seconds_after, 40.0);
    assert_eq!(plan.seconds_after, 40.0);
    // Loads describe the state the plan saw, not the state after.
    let c_load = plan.loads.iter().find(|l| l.node_id == "c").unwrap();
    assert_eq!(c_load.bytes, 3_000);
    assert_eq!(c_load.seconds, 120.0);
    assert_eq!(c_load.residency, NodeResidency::Server as i32);
    // The same state plans the same moves again.
    let again = plane.plan_balance(&request, 1, &any_pool, now).unwrap();
    assert_eq!(again.moves, plan.moves);
    // A move budget of one keeps the first move only.
    let one = plane
        .plan_balance(
            &PlanBalanceRequest {
                max_moves: 1,
                ..request.clone()
            },
            1,
            &any_pool,
            now,
        )
        .unwrap();
    assert_eq!(one.moves.len(), 1);
    assert_eq!(one.seconds_after, 80.0);
    // A gain threshold above what any move earns plans nothing.
    let strict = plane
        .plan_balance(
            &PlanBalanceRequest {
                min_gain: 0.9,
                ..request.clone()
            },
            1,
            &any_pool,
            now,
        )
        .unwrap();
    assert!(strict.moves.is_empty());
    assert_eq!(strict.seconds_after, strict.seconds_before);
    // Row bytes scale every load and every estimate.
    let scaled = plane.plan_balance(&request, 36, &any_pool, now).unwrap();
    assert_eq!(scaled.seconds_before, 120.0 * 36.0);
    assert_eq!(scaled.moves[0].bytes, 36_000);
}

#[test]
fn balance_refusals_and_exclusions_name_their_reason() {
    let plane = DurableControlPlane::in_memory(ControlPolicy::default());
    let now = 10_000_000;
    let request = PlanBalanceRequest::default();
    for bad in [-0.1, 1.5, f64::NAN, f64::INFINITY] {
        let err = plane
            .plan_balance(
                &PlanBalanceRequest {
                    min_gain: bad,
                    ..request.clone()
                },
                1,
                &any_pool,
                now,
            )
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument, "{bad}: {err}");
        assert!(err.message().contains("min_gain"), "{err}");
    }
    let err = plane.plan_balance(&request, 0, &any_pool, now).unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("row bytes"), "{err}");

    let fast = register(&plane, "fast", "10.0.0.1:1", "z1", now);
    let unmeasured = register(&plane, "unmeasured", "10.0.0.2:1", "z2", now);
    let stale = register(&plane, "stale", "10.0.0.3:1", "z3", now);
    let phone = register(&plane, "phone", "10.0.0.4:1", "z4", now);
    let unspecified = register(&plane, "unspecified", "10.0.0.5:1", "z5", now);
    let draining = register(&plane, "draining", "10.0.0.6:1", "z6", now);
    let lapsed = register(&plane, "lapsed", "10.0.0.7:1", "z7", now);
    measured(&plane, &fast, "z1", 1_000, now, NodeResidency::Server, now);
    measured(&plane, &unmeasured, "z2", 0, 0, NodeResidency::Server, now);
    measured(
        &plane,
        &stale,
        "z3",
        1_000,
        now - 3_600_000,
        NodeResidency::Server,
        now,
    );
    measured(
        &plane,
        &phone,
        "z4",
        1_000_000,
        now,
        NodeResidency::Device,
        now,
    );
    measured(
        &plane,
        &unspecified,
        "z5",
        1_000,
        now,
        NodeResidency::Unspecified,
        now,
    );
    measured(
        &plane,
        &draining,
        "z6",
        1_000,
        now,
        NodeResidency::Server,
        now,
    );
    measured(
        &plane,
        &lapsed,
        "z7",
        1_000,
        now,
        NodeResidency::Server,
        now,
    );
    plane
        .drain(
            DrainNodeRequest {
                collection: String::new(),
                node_id: "draining".into(),
                lease_token: draining.lease_token,
            },
            now,
        )
        .unwrap();
    // The phone is the fastest node and holds the heaviest shard; the
    // slow server holds one too. Neither the phone's shard nor any
    // shard may involve the phone.
    primary_at(&plane, &phone, "p0", "10.0.0.4:100", 100_000, now);
    primary_at(&plane, &fast, "f0", "10.0.0.1:100", 10_000, now);
    primary_at(&plane, &fast, "f1", "10.0.0.1:101", 10_000, now);
    primary_at(&plane, &unmeasured, "u0", "10.0.0.2:100", 10_000, now);
    let late = now + 30_000; // past every 10 s lease
    let plan = plane.plan_balance(&request, 1, &any_pool, now).unwrap();
    let reasons: BTreeMap<String, String> = plan
        .excluded
        .iter()
        .map(|e| (e.node_id.clone(), e.reason.clone()))
        .collect();
    assert_eq!(
        reasons.get("unmeasured").map(String::as_str),
        Some("unmeasured")
    );
    assert_eq!(reasons.get("stale").map(String::as_str), Some("stale"));
    assert_eq!(reasons.get("phone").map(String::as_str), Some("device"));
    assert_eq!(
        reasons.get("unspecified").map(String::as_str),
        Some("residency-unspecified")
    );
    assert_eq!(
        reasons.get("draining").map(String::as_str),
        Some("draining")
    );
    assert!(!reasons.contains_key("fast"));
    assert!(!reasons.contains_key("lapsed"), "still leased at {now}");
    for m in &plan.moves {
        assert_ne!(m.from_node, "phone");
        assert_ne!(m.to_node, "phone");
        assert_ne!(m.to_node, "unmeasured");
        assert_ne!(m.to_node, "draining");
    }
    // The only eligible peers are fast and lapsed; fast's two shards
    // (20 s) can spread to lapsed (0 s): one move leaves both at 10 s.
    assert_eq!(plan.moves.len(), 1, "{:?}", plan.moves);
    assert_eq!(plan.moves[0].from_node, "fast");
    assert_eq!(plan.moves[0].to_node, "lapsed");
    assert_eq!(
        plan.moves[0].shard,
        u32::MAX,
        "no topology names the shard's index"
    );
    let phone_load = plan.loads.iter().find(|l| l.node_id == "phone").unwrap();
    assert_eq!(phone_load.bytes, 100_000);
    assert_eq!(phone_load.seconds, 0.0, "no estimate for an excluded node");
    // Later, lapsed's lease has run out: it is excluded and nothing moves.
    let later = plane.plan_balance(&request, 1, &any_pool, late).unwrap();
    assert!(later
        .excluded
        .iter()
        .any(|e| e.node_id == "lapsed" && e.reason == "no-lease"));
    assert!(later.moves.is_empty(), "{:?}", later.moves);
}

/// A pool confines a move to the leaf's node set, and a ready copy's
/// failure domain is never the destination.
#[test]
fn balance_moves_stay_inside_the_pool_and_off_the_replicas_domain() {
    let plane = DurableControlPlane::in_memory(ControlPolicy::default());
    let now = 1_000_000;
    let a = register(&plane, "a", "10.0.0.1:1", "z1", now);
    let b = register(&plane, "b", "10.0.0.2:1", "z2", now);
    let c = register(&plane, "c", "10.0.0.3:1", "z3", now);
    for (lease, domain) in [(&a, "z1"), (&b, "z2"), (&c, "z3")] {
        measured(&plane, lease, domain, 100, now, NodeResidency::Server, now);
    }
    primary_at(&plane, &a, "s0", "10.0.0.1:100", 1_000, now);
    primary_at(&plane, &a, "s1", "10.0.0.1:101", 1_000, now);
    // Without a pool the plan spreads s0 (or s1) to b or c; the pool
    // "leaf-x" allows c only.
    let pool = |_: &str| BalancePool {
        leaf: "leaf-x".into(),
        node_ids: Some(["c".to_string()].into_iter().collect()),
    };
    let plan = plane
        .plan_balance(&PlanBalanceRequest::default(), 1, &pool, now)
        .unwrap();
    assert_eq!(plan.moves.len(), 1, "{:?}", plan.moves);
    assert_eq!(plan.moves[0].to_node, "c");
    assert_eq!(plan.moves[0].leaf, "leaf-x");
    // A pool that names no eligible node plans nothing.
    let nowhere = |_: &str| BalancePool {
        leaf: "leaf-y".into(),
        node_ids: Some(["zz".to_string()].into_iter().collect()),
    };
    let plan = plane
        .plan_balance(&PlanBalanceRequest::default(), 1, &nowhere, now)
        .unwrap();
    assert!(plan.moves.is_empty());
    // A ready replica of s0 on c puts c's domain off limits for s0;
    // s1 has no replica, so s1 goes to c and s0 stays.
    plane
        .report(
            ReportShardRequest {
                collection: String::new(),
                node_id: "c".into(),
                lease_token: c.lease_token,
                replica: Some(ShardReplicaState {
                    shard_id: "s0".into(),
                    addr: "10.0.0.3:100".into(),
                    generation: 7,
                    hash_lo: 0,
                    hash_hi: u64::MAX,
                    rows: 1_000,
                    role: ShardReplicaRole::Replica as i32,
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
    let plan = plane
        .plan_balance(&PlanBalanceRequest::default(), 1, &pool, now)
        .unwrap();
    assert_eq!(plan.moves.len(), 1, "{:?}", plan.moves);
    assert_eq!(plan.moves[0].to_node, "c");
    let moved_addr_is_s1 = plan.moves[0].bytes == 1_000;
    assert!(moved_addr_is_s1);
    let c_after: Vec<&BalanceMove> = plan.moves.iter().collect();
    assert!(c_after.iter().all(|m| m.from_node == "a"));
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
    plane
        .complete_action(
            CompletePlacementActionRequest {
                collection: String::new(),
                node_id: target.node_id.clone(),
                lease_token: target.lease_token,
                action_id: copy.action_id,
                outputs: vec![replica],
            },
            100,
        )
        .unwrap();
    let drop_action = plane
        .plan()
        .unwrap()
        .actions
        .into_iter()
        .find(|action| action.kind == PlacementActionKind::DropReplica as i32)
        .unwrap();
    plane
        .complete_action(
            CompletePlacementActionRequest {
                collection: String::new(),
                node_id: primary.node_id.clone(),
                lease_token: primary.lease_token,
                action_id: drop_action.action_id,
                outputs: Vec::new(),
            },
            100,
        )
        .unwrap();
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
    report_range(
        &plane,
        &node,
        "left",
        ShardReplicaRole::Primary,
        10,
        1,
        0,
        midpoint,
        100,
    );
    report_range(
        &plane,
        &node,
        "right",
        ShardReplicaRole::Primary,
        20,
        1,
        midpoint + 1,
        u64::MAX,
        100,
    );
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

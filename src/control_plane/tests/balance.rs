//! The balance dry run: determinism, bounds, refusals, and pools.

use std::collections::BTreeMap;

use crate::pb::{BalanceMove, PlanBalanceRequest};

use super::*;

fn any_pool(_: &str) -> BalancePool {
    BalancePool::default()
}

/// Three measured servers with rates 100, 50 and 25 (bytes per
/// second, with row bytes of 1 so rows are bytes): the slow node
/// holds the most.
fn three_measured_servers(now: u64) -> DurableControlPlane {
    let plane = DurableControlPlane::in_memory(ControlPolicy::default());
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
    plane
}

fn default_balance_request() -> PlanBalanceRequest {
    PlanBalanceRequest {
        collection: String::new(),
        min_gain: 0.0,
        max_moves: 0,
        max_rate_age_ms: 0,
    }
}

/// The plan moves whole shards off the slow node toward the node that
/// ends up lowest, deterministically, until a move would gain less than
/// `min_gain`.
#[test]
fn a_balance_plan_is_deterministic() {
    let now = 1_000_000;
    let plane = three_measured_servers(now);
    let request = default_balance_request();
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
}

/// The move budget, the gain threshold and the row bytes bound the plan
/// over the same three measured servers.
#[test]
fn a_balance_plan_is_bounded() {
    let now = 1_000_000;
    let plane = three_measured_servers(now);
    let request = default_balance_request();
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
fn balance_refusals_name_their_reason() {
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
}

/// Seven registered nodes, one per exclusion reason plus the eligible
/// `fast` and `lapsed`, with the phone holding the heaviest shard.
fn nodes_with_exclusions(now: u64) -> DurableControlPlane {
    let plane = DurableControlPlane::in_memory(ControlPolicy::default());
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
    plane
}

#[test]
fn balance_exclusions_name_their_reason() {
    let now = 10_000_000;
    let plane = nodes_with_exclusions(now);
    let request = PlanBalanceRequest::default();
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

//! The dry run over the pinned fixtures: placements, moves, and refusals.

use super::*;

#[test]
fn fixture_a_observation_set_digest_matches_section_10() {
    let store = store_a();
    assert_eq!(store.epoch(), 5);
    assert_eq!(hex32(&store.observation_set_digest()), OBS_A_HEX);
}

#[test]
fn fixture_a_plan_matches_section_10() {
    let store = store_a();
    let snapshot = snapshot_a(&store, 118);
    assert_eq!(hex32(&snapshot.plan_digest()), PLAN_A_HEX);
    let response = plan_tiers(&snapshot).expect("plan");
    assert_eq!(hex32(&response.plan_digest), PLAN_A_HEX);
    assert_eq!(response.moves.len(), 0);
    assert_eq!(response.refusals.len(), 0);
    assert_eq!(response.placements.len(), 2);

    let l4 = &response.placements[0];
    assert_eq!(l4.fragment.leaf, "L4");
    assert_eq!(l4.classified_tier.as_deref(), Some("hot"));
    assert_eq!(l4.rate_nanos, 18_626);
    assert_eq!(l4.rows, 1_000_000);
    assert_eq!(l4.resident_bytes, 268_435_456);
    assert_eq!(l4.last_scanned_unix_ms, T - 5000);
    assert_eq!(l4.complete_replicas.len(), 3);
    assert_eq!(l4.reporters.len(), 3);
    assert_eq!(l4.reporters[0].node_id, "krick-1");
    assert_eq!(l4.reporters[0].queue_wait_p50_us, Some(1200));
    assert_eq!(l4.reporters[1].queue_wait_p50_us, None);

    let l7 = &response.placements[1];
    assert_eq!(l7.fragment.leaf, "L7");
    assert_eq!(l7.classified_tier.as_deref(), Some("archive"));
    assert_eq!(l7.rate_nanos, 0);
    assert_eq!(l7.last_scanned_unix_ms, T - 3_600_000);
    assert_eq!(l7.complete_replicas.len(), 2);
}

#[test]
fn warm_literal_12000_scans_classifies_warm() {
    let store = store_a();
    let mut snapshot = snapshot_a(&store, 118);
    for obs in &mut snapshot.observations {
        if obs.reporter.node_id == "krick-1" && obs.leaf == "L4" {
            obs.scans_observed = 12_000;
        }
    }
    let response = plan_tiers(&snapshot).expect("plan");
    let l4 = &response.placements[0];
    assert_eq!(l4.rate_nanos, 74);
    assert_eq!(l4.classified_tier.as_deref(), Some("warm"));
}

#[test]
fn permuted_inputs_yield_identical_validated_plan_bytes() {
    // Reverse the ingest order.
    let store = store_a();
    let mut store2 = ObservationStore::new(COHORT, 0, 1000, 1 << 20, 64, committed_view_a())
        .expect("valid cohort config");
    store2.register_incarnation("krick-1", inc(0x0a)).unwrap();
    store2.register_incarnation("pi5v1", inc(0x0b)).unwrap();
    store2.register_incarnation("pi5v3", inc(0x0c)).unwrap();
    let mut reports = fixture_a_reports();
    reports.reverse();
    for report in reports {
        assert_eq!(
            store2.ingest(report).expect("ingest"),
            IngestOutcome::Landed
        );
    }
    assert_eq!(
        store2.observation_set_digest(),
        store.observation_set_digest()
    );
    let forward = plan_tiers(&snapshot_a(&store, 118))
        .expect("plan")
        .canonical_bytes();
    let reversed = plan_tiers(&snapshot_a(&store2, 118))
        .expect("plan")
        .canonical_bytes();
    assert_eq!(forward, reversed);

    // Reverse the fragment requests, the pools, and the committed
    // copies: the constructor canonicalizes, so the plans agree.
    let store = store_a();
    let mut input = input_a(&store, 118);
    input.fragment_requests.reverse();
    for shard in input.committed.shards.values_mut() {
        for coverage in shard.leaves.values_mut() {
            coverage.pool.reverse();
            coverage.copies.reverse();
        }
    }
    let canonical = TierSnapshot::validated(input).expect("valid");
    assert_eq!(
        plan_tiers(&canonical).expect("plan").canonical_bytes(),
        forward
    );
}

#[test]
fn two_fragments_competing_for_one_destination_plan_canonically() {
    // The canonical order — bucket 5 before bucket 6 — decides which
    // violation claims the capacity, identically for both input orders.
    let store = store_m();
    let mut input = input_m(&store);
    input.nodes.get_mut("krick-1").unwrap().capacity.total_bytes = 22_000_000_000;
    input.nodes.get_mut("pi5v1").unwrap().capacity.total_bytes = 0;
    let mut reversed = input.clone();
    reversed.fragment_requests.reverse();
    let a = plan_tiers(&TierSnapshot::validated(input).expect("valid")).expect("plan");
    let b = plan_tiers(&TierSnapshot::validated(reversed).expect("valid")).expect("plan");
    assert_eq!(a.canonical_bytes(), b.canonical_bytes());
    assert_eq!(a.moves.len(), 1);
    assert_eq!(a.moves[0].fragment.partition.bucket, 5);
    assert_eq!(a.moves[0].destination_node, "krick-1");
    assert_eq!(a.refusals.len(), 1);
    assert!(a.refusals[0].contains("key_bucket=6"));
}

#[test]
fn fixture_m_move_plan_and_capacity_refusal_match_section_10() {
    let store = store_m();
    assert_eq!(hex32(&store.observation_set_digest()), OBS_M_HEX);
    let snapshot = snapshot_m(&store);
    assert_eq!(hex32(&snapshot.plan_digest()), PLAN_M_HEX);
    let response = plan_tiers(&snapshot).expect("plan");

    assert_eq!(response.placements.len(), 2);
    assert_eq!(
        response.placements[0].classified_tier.as_deref(),
        Some("archive")
    );
    assert_eq!(response.placements[0].rate_nanos, 0);
    assert_eq!(
        response.placements[1].classified_tier.as_deref(),
        Some("archive")
    );

    assert_eq!(response.moves.len(), 1);
    let mv = &response.moves[0];
    assert_eq!(mv.fragment.partition.bucket, 5);
    assert_eq!(mv.source.node_id, "pi5v3");
    assert_eq!(mv.source.storage_incarnation, inc(0xc1));
    assert_eq!(mv.destination_node, "krick-1");
    assert_eq!(mv.from_tier, "archive");
    assert_eq!(mv.to_tier, "archive");
    assert_eq!(mv.bytes, 21_474_836_480);
    assert_eq!(hex32(&mv.plan_digest), PLAN_M_HEX);
    assert_eq!(mv.control_revision, 41);
    assert_eq!(mv.observation_epoch, 119);
    assert_eq!(hex32(&mv.policy_fingerprint), POLICY_FP_HEX);

    assert_eq!(response.refusals.len(), 1);
    assert_eq!(
        response.refusals[0],
        "plan_tiers: tier \"archive\" floor is 2 for partition key_bucket=6, but no eligible destination in leaf L4 has capacity: krick-1 has 5368709120 free after earlier planned moves, pi5v1 has 16106127360 free, the fragment needs 19327352832"
    );
}

#[test]
fn missing_and_stale_reports_refuse_the_plan() {
    // E3: an uncovered complete copy's missing report refuses.
    let mut store = ObservationStore::new(COHORT, 0, 1000, 1 << 20, 64, committed_view_a())
        .expect("valid cohort config");
    store.register_incarnation("krick-1", inc(0x0a)).unwrap();
    store.register_incarnation("pi5v1", inc(0x0b)).unwrap();
    store.register_incarnation("pi5v3", inc(0x0c)).unwrap();
    for report in fixture_a_reports().into_iter().skip(1) {
        store.ingest(report).expect("ingest");
    }
    let snapshot = snapshot_a(&store, 118);
    assert_eq!(
        plan_tiers(&snapshot).unwrap_err(),
        "plan_tiers: partition key_bucket=7 on node krick-1 has no current observation; unknown is not idle"
    );

    // E4's boundary arithmetic, with cohort-aligned literals.
    let store = store_a();
    let mut snapshot = snapshot_a(&store, 118);
    snapshot.planning_instant_unix_ms = W1 + 1000;
    snapshot.max_observation_age_ms = 1000;
    assert!(plan_tiers(&snapshot).is_ok(), "age == bound is fresh");
    snapshot.max_observation_age_ms = 999;
    assert_eq!(
        plan_tiers(&snapshot).unwrap_err(),
        "plan_tiers: partition key_bucket=7 on node krick-1 was last observed 1000 ms ago, past the 999 ms bound"
    );
}

#[test]
fn skewed_future_report_refuses_the_plan() {
    let mut store = store_a();
    let mut future = obs(7, &P3_S6, 1_000_000, 268_435_456, 0, 0);
    future.window_start_unix_ms = W1;
    future.window_end_unix_ms = W1 + COHORT;
    assert_eq!(store.ingest(future).expect("ingest"), IngestOutcome::Landed);
    let snapshot = snapshot_a(&store, 118);
    assert_eq!(
        plan_tiers(&snapshot).unwrap_err(),
        "plan_tiers: observation from node pi5v3 ends 600000 ms in the future, past the 5000 ms skew bound"
    );
}

#[test]
fn partial_copies_are_not_replicas() {
    // E2: bucket 9's copies are two disjoint partials — zero complete
    // replicas, so the floor refuses instead of planning a move.
    let mut partial_a = committed_copy(&P1_S6, cov_l4());
    partial_a.complete = false;
    let mut partial_b = committed_copy(&P3_S6, cov_l4());
    partial_b.complete = false;
    let mut shards = BTreeMap::new();
    shards.insert(
        "s6".to_string(),
        CommittedShard {
            source_generation: 3,
            ownership_epoch: 5,
            leaves: BTreeMap::from([(
                "L4".to_string(),
                LeafCoverage {
                    pool: vec![
                        "krick-1".to_string(),
                        "pi5v1".to_string(),
                        "pi5v3".to_string(),
                    ],
                    owner_node: "pi5v3".to_string(),
                    copies: vec![partial_a, partial_b],
                },
            )]),
        },
    );
    let committed = CommittedView {
        workspace: "ws-court".to_string(),
        collection: "cases".to_string(),
        derived_fingerprint: decl_fp(),
        topology_generation: 9,
        shards,
    };
    let mut store = ObservationStore::new(COHORT, 0, 1000, 1 << 20, 64, committed.clone())
        .expect("valid cohort config");
    store.register_incarnation("pi5v3", inc(0x0c)).unwrap();
    store
        .ingest(obs(9, &P3_S6, 60_000, 10_000_000_000, 0, T - 1000))
        .expect("ingest");
    let mut input = input_m(&store_m());
    input.committed = committed;
    input.observations = store.observations().into_iter().cloned().collect();
    input.current_incarnations = store.current_incarnations().clone();
    input.fragment_requests = vec![req(9, "L4")];
    let response = plan_tiers(&TierSnapshot::validated(input).expect("valid")).expect("plan");
    assert_eq!(response.moves.len(), 0);
    assert_eq!(
        response.refusals,
        vec![
            "plan_tiers: fragment (key_bucket=9, gen 9, L4) has 0 complete replicas (2 partial); tier \"archive\" floor is 2"
            .to_string()
        ]
    );
}

#[test]
fn failure_domains_count_once_toward_the_floor() {
    // Three complete copies over two failure domains satisfy a floor
    // of 2 but not 3 (§2's distinct-domain rule), so hot plans one
    // copy to a fourth node in a fresh domain.
    let mut committed = committed_view_a();
    {
        let coverage = committed
            .shards
            .get_mut("s6")
            .unwrap()
            .leaves
            .get_mut("L4")
            .unwrap();
        coverage.copies[2].failure_domain = "pi5-west".to_string();
        coverage.pool.push("pi5v4".to_string());
    }
    let mut store = ObservationStore::new(COHORT, 0, 1000, 1 << 20, 64, committed.clone())
        .expect("valid cohort config");
    store.register_incarnation("krick-1", inc(0x0a)).unwrap();
    store.register_incarnation("pi5v1", inc(0x0b)).unwrap();
    store.register_incarnation("pi5v3", inc(0x0c)).unwrap();
    for report in fixture_a_reports() {
        store.ingest(report).expect("ingest");
    }
    let mut input = input_a(&store, 118);
    input.committed = committed;
    input.nodes.insert(
        "pi5v4".to_string(),
        NodeRecord {
            residency: NodeResidency::Server,
            eligible: true,
            failure_domain: "pi5-north".to_string(),
            capacity: NodeCapacity {
                total_bytes: 1_000_000_000_000,
                resident_bytes: 0,
            },
        },
    );
    let response = plan_tiers(&TierSnapshot::validated(input).expect("valid")).expect("plan");
    assert_eq!(
        response.placements[0].classified_tier.as_deref(),
        Some("hot")
    );
    assert_eq!(
        response.moves.len(),
        1,
        "hot's floor of 3 sees only 2 domains"
    );
    assert_eq!(response.moves[0].destination_node, "pi5v4");
    assert_eq!(response.refusals.len(), 0);
}

#[test]
fn numeric_bounds_are_checked_not_wrapping() {
    let s = u64::MAX as u128;
    let b = u64::MAX as u128;
    let expected = (s * 1_000_000_000_000u128) / (600_000u128 * b);
    assert_eq!(rate_nanos(s, 600_000, b), u64::try_from(expected).unwrap());

    assert!(ensure_fan_in(1 << 20).is_ok());
    assert!(ensure_fan_in((1 << 20) + 1).unwrap_err().contains("fan-in"));

    let store = store_a();
    let mut snapshot = snapshot_a(&store, 118);
    snapshot.policy.tiers[0].max_seconds_since_scan = u64::MAX;
    snapshot.policy.tiers[0].scans_per_byte_nanos_lo = 0;
    assert!(plan_tiers(&snapshot)
        .unwrap_err()
        .contains("warmth bound 18446744073709551615 s overflows milliseconds"));
}

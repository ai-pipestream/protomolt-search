use super::plan::{ensure_fan_in, rate_nanos};
use super::*;

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
// Plan-input encoding v2 digests (2026-09-08): bind the cohort and the
// node/fragment subdigests; recomputed with the Python oracle.
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
    sha256::to_hex(bytes)
}

fn decl_fp() -> [u8; 32] {
    sha256::digest(b"key_bucket = hash.fnv64(stable_key()) % 64u")
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
    sha256::digest(
            b"example manifest: fragment (key_bucket=7, gen 9, L4), source generation 3, rows 1000000, bytes 268435456",
        )
}

fn cov_l7() -> [u8; 32] {
    sha256::digest(
            b"example manifest: fragment (key_bucket=7, gen 9, L7), source generation 3, rows 500000, bytes 134217728",
        )
}

fn tree_digest() -> [u8; 32] {
    sha256::digest(
            b"example placement tree: gen 9, leaf L4 = {krick-1, pi5v1, pi5v3}, leaf L7 = {krick-1, pi5v1}",
        )
}

fn prov_digest() -> [u8; 32] {
    sha256::digest(
            b"example provider geometry: turbovec 64-shard mapped image; committed free bytes: krick-1=26843545600, pi5v1=16106127360, pi5v3=0",
        )
}

struct CopySpec {
    node: &'static str,
    proc_inc: u8,
    stor_inc: u8,
    domain: &'static str,
}

const K1_S6: CopySpec = CopySpec {
    node: "krick-1",
    proc_inc: 0x0a,
    stor_inc: 0xa1,
    domain: "krick",
};
const P1_S6: CopySpec = CopySpec {
    node: "pi5v1",
    proc_inc: 0x0b,
    stor_inc: 0xb1,
    domain: "pi5-west",
};
const P3_S6: CopySpec = CopySpec {
    node: "pi5v3",
    proc_inc: 0x0c,
    stor_inc: 0xc1,
    domain: "pi5-east",
};
const P1_S7: CopySpec = CopySpec {
    node: "pi5v1",
    proc_inc: 0x0b,
    stor_inc: 0xb2,
    domain: "pi5-west",
};
const K1_S7: CopySpec = CopySpec {
    node: "krick-1",
    proc_inc: 0x0a,
    stor_inc: 0xa2,
    domain: "krick",
};

fn committed_copy(spec: &CopySpec, coverage: [u8; 32]) -> CommittedCopy {
    CommittedCopy {
        node_id: spec.node.to_string(),
        storage_incarnation: inc(spec.stor_inc),
        failure_domain: spec.domain.to_string(),
        complete: true,
        coverage_digest: coverage,
    }
}

#[allow(clippy::too_many_arguments)]
fn obs(
    bucket: u64,
    spec: &CopySpec,
    leaf: &str,
    shard: &str,
    sgen: u64,
    epoch: u64,
    rows: u64,
    bytes: u64,
    scans: u64,
    last: u64,
) -> PartitionObservation {
    PartitionObservation {
        partition: partition(bucket),
        reporter: ReporterIdentity {
            node_id: spec.node.to_string(),
            process_incarnation: inc(spec.proc_inc),
        },
        shard: ShardRef {
            shard: shard.to_string(),
            source_generation: sgen,
            storage_incarnation: inc(spec.stor_inc),
            ownership_epoch: epoch,
        },
        topology_generation: 9,
        leaf: leaf.to_string(),
        rows,
        resident_bytes: bytes,
        scans_observed: scans,
        scan_bytes: if scans > 0 { 786_432_000_000 } else { 0 },
        queue_wait_p50_us: if scans > 0 { 1200 } else { 0 },
        queue_wait_p99_us: if scans > 0 { 9400 } else { 0 },
        samples: if scans > 0 { 3000 } else { 0 },
        last_scanned_unix_ms: last,
        window_start_unix_ms: W0,
        window_end_unix_ms: W1,
    }
}

fn fixture_a_reports() -> Vec<PartitionObservation> {
    vec![
        obs(
            7,
            &K1_S6,
            "L4",
            "s6",
            3,
            5,
            1_000_000,
            268_435_456,
            3_000_000,
            T - 5000,
        ),
        obs(7, &P1_S6, "L4", "s6", 3, 5, 1_000_000, 268_435_456, 0, 0),
        obs(7, &P3_S6, "L4", "s6", 3, 5, 1_000_000, 268_435_456, 0, 0),
        obs(
            7,
            &P1_S7,
            "L7",
            "s7",
            3,
            2,
            500_000,
            134_217_728,
            0,
            T - 3_600_000,
        ),
        obs(7, &K1_S7, "L7", "s7", 3, 2, 500_000, 134_217_728, 0, 0),
    ]
}

fn committed_view_a() -> CommittedView {
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
                    owner_node: "krick-1".to_string(),
                    copies: vec![
                        committed_copy(&K1_S6, cov_l4()),
                        committed_copy(&P1_S6, cov_l4()),
                        committed_copy(&P3_S6, cov_l4()),
                    ],
                },
            )]),
        },
    );
    shards.insert(
        "s7".to_string(),
        CommittedShard {
            source_generation: 3,
            ownership_epoch: 2,
            leaves: BTreeMap::from([(
                "L7".to_string(),
                LeafCoverage {
                    pool: vec!["krick-1".to_string(), "pi5v1".to_string()],
                    owner_node: "pi5v1".to_string(),
                    copies: vec![
                        committed_copy(&P1_S7, cov_l7()),
                        committed_copy(&K1_S7, cov_l7()),
                    ],
                },
            )]),
        },
    );
    CommittedView {
        workspace: "ws-court".to_string(),
        collection: "cases".to_string(),
        derived_fingerprint: decl_fp(),
        topology_generation: 9,
        shards,
    }
}

fn nodes_map() -> BTreeMap<String, NodeRecord> {
    let server = |domain: &str, total: u64| NodeRecord {
        residency: NodeResidency::Server,
        eligible: true,
        failure_domain: domain.to_string(),
        capacity: NodeCapacity {
            total_bytes: total,
            resident_bytes: 0,
        },
    };
    BTreeMap::from([
        ("krick-1".to_string(), server("krick", 26_843_545_600)),
        ("pi5v1".to_string(), server("pi5-west", 16_106_127_360)),
        ("pi5v3".to_string(), server("pi5-east", 0)),
    ])
}

fn store_a() -> ObservationStore {
    let mut store = ObservationStore::new(COHORT, 0, 1000, 1 << 20, 64, committed_view_a())
        .expect("valid cohort config");
    store.register_incarnation("krick-1", inc(0x0a)).unwrap();
    store.register_incarnation("pi5v1", inc(0x0b)).unwrap();
    store.register_incarnation("pi5v3", inc(0x0c)).unwrap();
    for report in fixture_a_reports() {
        assert_eq!(store.ingest(report).expect("ingest"), IngestOutcome::Landed);
    }
    store
}

fn input_a(store: &ObservationStore, epoch: u64) -> TierSnapshotInput {
    TierSnapshotInput {
        planning_instant_unix_ms: T,
        max_moves: 16,
        max_observation_age_ms: 600_000,
        clock_skew_bound_ms: 5_000,
        authority_id: "control-0".to_string(),
        authority_incarnation: inc(0x01),
        control_revision: 41,
        policy_revision: 7,
        policy: policy_p(),
        observation_epoch: epoch,
        topology_generation: 9,
        placement_tree_digest: tree_digest(),
        workspace: "ws-court".to_string(),
        collection: "cases".to_string(),
        derived_fingerprint: decl_fp(),
        provider_geometry_digest: prov_digest(),
        cohort_length_ms: COHORT,
        cohort_phase_unix_ms: 0,
        nodes: nodes_map(),
        observations: store.observations().into_iter().cloned().collect(),
        fragment_requests: vec![req(7, "L4"), req(7, "L7")],
        committed: committed_view_a(),
        current_incarnations: store.current_incarnations().clone(),
    }
}

fn snapshot_a(store: &ObservationStore, epoch: u64) -> TierSnapshot {
    TierSnapshot::validated(input_a(store, epoch)).expect("fixture A validates")
}

fn fixture_m_reports() -> Vec<PartitionObservation> {
    vec![
        obs(
            5,
            &P3_S6,
            "L4",
            "s6",
            3,
            5,
            80_000,
            21_474_836_480,
            0,
            T - 7_200_000,
        ),
        obs(
            6,
            &P3_S6,
            "L4",
            "s6",
            3,
            5,
            72_000,
            19_327_352_832,
            0,
            T - 10_800_000,
        ),
    ]
}

fn committed_view_m() -> CommittedView {
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
                    copies: vec![committed_copy(&P3_S6, cov_l4())],
                },
            )]),
        },
    );
    CommittedView {
        workspace: "ws-court".to_string(),
        collection: "cases".to_string(),
        derived_fingerprint: decl_fp(),
        topology_generation: 9,
        shards,
    }
}

fn store_m() -> ObservationStore {
    let mut store = ObservationStore::new(COHORT, 0, 1000, 1 << 20, 64, committed_view_m())
        .expect("valid cohort config");
    store.register_incarnation("pi5v3", inc(0x0c)).unwrap();
    for report in fixture_m_reports() {
        assert_eq!(store.ingest(report).expect("ingest"), IngestOutcome::Landed);
    }
    store
}

fn input_m(store: &ObservationStore) -> TierSnapshotInput {
    TierSnapshotInput {
        observation_epoch: 119,
        fragment_requests: vec![req(5, "L4"), req(6, "L4")],
        committed: committed_view_m(),
        observations: store.observations().into_iter().cloned().collect(),
        current_incarnations: store.current_incarnations().clone(),
        ..input_a(&store_a(), 119)
    }
}

fn snapshot_m(store: &ObservationStore) -> TierSnapshot {
    TierSnapshot::validated(input_m(store)).expect("fixture M validates")
}

#[test]
fn pinned_policy_and_declaration_fixtures() {
    assert_eq!(hex32(&decl_fp()), DECL_FP_HEX);
    let policy = policy_p();
    assert_eq!(policy.canonical_bytes().len(), 155);
    assert_eq!(hex32(&policy.fingerprint()), POLICY_FP_HEX);
    let mut swapped = policy_p();
    swapped.tiers.swap(0, 1);
    assert_eq!(hex32(&swapped.fingerprint()), POLICY_SWAPPED_FP_HEX);
    assert_eq!(hex32(&cov_l4()), COV_L4_HEX);
    assert_eq!(hex32(&cov_l7()), COV_L7_HEX);

    let mut dup = policy_p();
    dup.tiers[1].name = "hot".to_string();
    assert!(dup.validate().unwrap_err().contains("duplicate tier name"));
    let bad_band = TierPolicy {
        tiers: vec![tier("warm", 2, 100, 100, 0)],
    };
    assert!(bad_band
        .validate()
        .unwrap_err()
        .contains("empty or inverted"));
    let bad_floor = TierPolicy {
        tiers: vec![tier("warm", 0, 1, 100, 0)],
    };
    assert!(bad_floor
        .validate()
        .unwrap_err()
        .contains("outside [1, 1024]"));
    // A giant floor is a policy error, not a work order.
    let giant_floor = TierPolicy {
        tiers: vec![tier("hot", 1_000_000, 100, 1_000, 0)],
    };
    assert_eq!(
        giant_floor.validate().unwrap_err(),
        "tier policy: tier \"hot\" floor 1000000 is outside [1, 1024]"
    );
    let device_tier = TierPolicy {
        tiers: vec![CapacityTier {
            residency: NodeResidency::Device,
            ..tier("cold", 1, 0, 1, 0)
        }],
    };
    assert!(device_tier
        .validate()
        .unwrap_err()
        .contains("non-SERVER residency"));
}

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
    // Pre-fix, fragment order decided which violation claimed the
    // capacity. The canonical order — bucket 5 before bucket 6 —
    // now decides, identically for both input orders.
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
fn validated_constructor_enforces_resource_and_identity_binding() {
    let store = store_a();

    // A report from another resource refuses; order does not matter.
    let mut forged = input_a(&store, 118);
    let mut alien = forged.observations[0].clone();
    alien.partition.collection = "briefs".to_string();
    forged.observations.push(alien.clone());
    assert!(TierSnapshot::validated(forged)
        .unwrap_err()
        .contains("outside the snapshot's resource"));
    let mut reversed = input_a(&store, 118);
    reversed.observations.insert(0, alien);
    assert!(TierSnapshot::validated(reversed)
        .unwrap_err()
        .contains("outside the snapshot's resource"));

    // A committed view of another resource refuses.
    let mut wrong = input_a(&store, 118);
    wrong.committed.collection = "briefs".to_string();
    assert!(TierSnapshot::validated(wrong)
        .unwrap_err()
        .contains("is not the snapshot's"));

    // Duplicate full-identity observations refuse; nothing overwrites.
    let mut dup = input_a(&store, 118);
    let copy = dup.observations[0].clone();
    dup.observations.push(copy);
    assert!(TierSnapshot::validated(dup)
        .unwrap_err()
        .contains("duplicate observation"));

    // A duplicate fragment request refuses.
    let mut dup_req = input_a(&store, 118);
    dup_req.fragment_requests.push(req(7, "L4"));
    assert!(TierSnapshot::validated(dup_req)
        .unwrap_err()
        .contains("duplicate fragment request"));

    // A report from a superseded incarnation refuses even when the
    // store's own ingest was bypassed.
    let mut stale = input_a(&store, 118);
    stale
        .current_incarnations
        .insert("krick-1".to_string(), inc(0x1a));
    assert!(TierSnapshot::validated(stale)
        .unwrap_err()
        .contains("is superseded by"));

    // Store ingest binds the resource too.
    let mut store = ObservationStore::new(COHORT, 0, 1000, 1 << 20, 64, committed_view_a())
        .expect("valid cohort config");
    store.register_incarnation("krick-1", inc(0x0a)).unwrap();
    let mut alien = obs(7, &K1_S6, "L4", "s6", 3, 5, 1_000_000, 268_435_456, 0, 0);
    alien.partition.collection = "briefs".to_string();
    assert!(store
        .ingest(alien)
        .unwrap_err()
        .contains("is not this resource"));
}

#[test]
fn plan_digest_binds_every_effective_input() {
    let store = store_a();
    let base = snapshot_a(&store, 118).plan_digest();
    assert_eq!(hex32(&base), PLAN_A_HEX);

    #[allow(clippy::type_complexity)]
    let mutators: Vec<(&str, fn(&mut TierSnapshotInput))> = vec![
        ("capacity", |i| {
            i.nodes.get_mut("krick-1").unwrap().capacity.total_bytes = 1;
        }),
        ("failure domain", |i| {
            i.nodes.get_mut("krick-1").unwrap().failure_domain = "krick-2".to_string();
        }),
        ("resident bytes", |i| {
            i.nodes.get_mut("pi5v1").unwrap().capacity.resident_bytes = 4096;
        }),
        ("node set", |i| {
            i.nodes.insert(
                "pi5v4".to_string(),
                NodeRecord {
                    residency: NodeResidency::Server,
                    eligible: true,
                    failure_domain: "pi5-north".to_string(),
                    capacity: NodeCapacity {
                        total_bytes: 0,
                        resident_bytes: 0,
                    },
                },
            );
        }),
        ("fragment set", |i| {
            i.fragment_requests.pop();
        }),
        ("observation epoch", |i| i.observation_epoch += 1),
        ("max moves", |i| i.max_moves = 8),
        ("provider digest", |i| i.provider_geometry_digest[0] ^= 1),
    ];
    for (name, mutate) in mutators {
        let mut input = input_a(&store, 118);
        mutate(&mut input);
        let snapshot = TierSnapshot::validated(input)
            .unwrap_or_else(|e| panic!("mutation {name} must stay valid: {e}"));
        assert_ne!(
            snapshot.plan_digest(),
            base,
            "mutating {name} must change the plan digest"
        );
    }

    // The cohort is an input too; changing it invalidates the stored
    // windows, which the constructor refuses by name.
    let mut cohort = input_a(&store, 118);
    cohort.cohort_length_ms = 700_000;
    assert!(TierSnapshot::validated(cohort)
        .unwrap_err()
        .contains("not a cohort window"));
    let mut zero = input_a(&store, 118);
    zero.cohort_length_ms = 0;
    assert!(TierSnapshot::validated(zero)
        .unwrap_err()
        .contains("outside (0, 2^40]"));
}

#[test]
fn residency_and_eligibility_are_verified_not_assumed() {
    let store = store_a();

    // A complete copy on a device-local source refuses validation.
    let mut device = input_a(&store, 118);
    device.nodes.get_mut("pi5v3").unwrap().residency = NodeResidency::Device;
    assert!(TierSnapshot::validated(device)
        .unwrap_err()
        .contains("not server-resident"));

    // Unspecified residency refuses the same way.
    let mut unknown = input_a(&store, 118);
    unknown.nodes.get_mut("pi5v3").unwrap().residency = NodeResidency::Unspecified;
    assert!(TierSnapshot::validated(unknown)
        .unwrap_err()
        .contains("not server-resident"));

    // An ineligible node cannot hold a verified copy.
    let mut ineligible = input_a(&store, 118);
    ineligible.nodes.get_mut("pi5v3").unwrap().eligible = false;
    assert!(TierSnapshot::validated(ineligible)
        .unwrap_err()
        .contains("ineligible node"));

    // Complete copies under different coverage digests cannot be one
    // source version.
    let mut forged = input_a(&store, 118);
    forged
        .committed
        .shards
        .get_mut("s6")
        .unwrap()
        .leaves
        .get_mut("L4")
        .unwrap()
        .copies[2]
        .coverage_digest = cov_l7();
    assert!(TierSnapshot::validated(forged)
        .unwrap_err()
        .contains("different coverage digests"));

    // A device node inside a server pool is never a destination.
    let mut store = ObservationStore::new(COHORT, 0, 1000, 1 << 20, 64, committed_view_m())
        .expect("valid cohort config");
    store.register_incarnation("pi5v3", inc(0x0c)).unwrap();
    for report in fixture_m_reports() {
        store.ingest(report).expect("ingest");
    }
    let mut input = input_m(&store);
    input
        .committed
        .shards
        .get_mut("s6")
        .unwrap()
        .leaves
        .get_mut("L4")
        .unwrap()
        .pool = vec!["pi5v3".to_string(), "pi5v2".to_string()];
    input.nodes.insert(
        "pi5v2".to_string(),
        NodeRecord {
            residency: NodeResidency::Device,
            eligible: true,
            failure_domain: "pi5-south".to_string(),
            capacity: NodeCapacity {
                total_bytes: 1_000_000_000_000,
                resident_bytes: 0,
            },
        },
    );
    input.nodes.remove("krick-1");
    input.nodes.remove("pi5v1");
    let response = plan_tiers(&TierSnapshot::validated(input).expect("valid")).expect("plan");
    assert_eq!(
        response.moves.len(),
        0,
        "the device pool member is excluded"
    );
    assert!(response.refusals.iter().all(|r| !r.contains("pi5v2 free")));
}

#[test]
fn checked_arithmetic_and_store_bounds_hold_state_intact() {
    // Epoch exhaustion refuses before mutating: the report is not
    // inserted, and a later exact retry of a stored report still
    // works.
    let mut store = store_a();
    store.epoch = u64::MAX;
    let fresh = obs(
        9,
        &P3_S6,
        "L4",
        "s6",
        3,
        5,
        60_000,
        10_000_000_000,
        0,
        T - 1000,
    );
    let count_before = store.observations().len();
    assert_eq!(
        store.ingest(fresh).unwrap_err(),
        "report_shard: the observation epoch is exhausted"
    );
    assert_eq!(store.observations().len(), count_before);
    let retry = obs(
        7,
        &K1_S6,
        "L4",
        "s6",
        3,
        5,
        1_000_000,
        268_435_456,
        3_000_000,
        T - 5000,
    );
    assert_eq!(
        store.ingest(retry).expect("retry"),
        IngestOutcome::Unchanged
    );
    assert_eq!(
        store.commit_expiry(W1 + COHORT, 1000).unwrap_err(),
        "report_shard: the observation epoch is exhausted"
    );
    assert_eq!(
        store.observations().len(),
        count_before,
        "expiry changed nothing"
    );

    // The incarnation registry is bounded.
    let mut tight = ObservationStore::new(COHORT, 0, 1000, 1 << 20, 1, committed_view_a())
        .expect("valid cohort config");
    tight.register_incarnation("krick-1", inc(0x0a)).unwrap();
    assert_eq!(
        tight.register_incarnation("pi5v1", inc(0x0b)).unwrap_err(),
        "report_shard: the incarnation registry holds the bound of 1 nodes"
    );

    // Retained accounting charges allocation capacity, not length:
    // a short string with a large capacity defeats a tight bound.
    let mut over = obs(7, &K1_S6, "L4", "s6", 3, 5, 1_000_000, 268_435_456, 0, 0);
    over.partition.column = {
        let mut s = String::with_capacity(4096);
        s.push_str("key_bucket");
        s
    };
    assert!(over.heap_bytes() >= 2 * 4096);
    let mut tight_bytes = ObservationStore::new(COHORT, 0, 1000, 100, 64, committed_view_a())
        .expect("valid cohort config");
    tight_bytes
        .register_incarnation("krick-1", inc(0x0a))
        .unwrap();
    assert!(tight_bytes
        .ingest(obs(
            7,
            &K1_S6,
            "L4",
            "s6",
            3,
            5,
            1_000_000,
            268_435_456,
            0,
            0
        ))
        .unwrap_err()
        .contains("bound of 100 retained bytes"));
}

#[test]
fn cohort_alignment_rules() {
    let mut store = ObservationStore::new(COHORT, 0, 1000, 1 << 20, 64, committed_view_a())
        .expect("valid cohort config");
    store.register_incarnation("krick-1", inc(0x0a)).unwrap();

    // E10: a 660-second, off-grid window refuses by name.
    let mut misaligned = obs(7, &K1_S6, "L4", "s6", 3, 5, 1_000_000, 268_435_456, 0, 0);
    misaligned.window_start_unix_ms = T - 660_000;
    misaligned.window_end_unix_ms = T - 60_000;
    assert_eq!(
            store.ingest(misaligned).unwrap_err(),
            "report_shard: window [1788868140000, 1788868740000) is not a cohort window (length 600000 ms, phase 0)"
        );

    let mut inverted = obs(7, &K1_S6, "L4", "s6", 3, 5, 1_000_000, 268_435_456, 0, 0);
    inverted.window_start_unix_ms = W1;
    inverted.window_end_unix_ms = W0;
    assert!(store.ingest(inverted).unwrap_err().contains("inverted"));

    assert!(ObservationStore::new((1 << 40) + 1, 0, 10, 1000, 64, committed_view_a()).is_err());

    let store = store_a();
    let snapshot = snapshot_a(&store, 118);
    assert_eq!(snapshot.current_cohort().unwrap(), (W0, W1));
    let mut mid = snapshot_a(&store, 118);
    mid.planning_instant_unix_ms = W0 + 100_000;
    assert_eq!(mid.current_cohort().unwrap(), (W0 - COHORT, W0));
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
    let mut future = obs(7, &P3_S6, "L4", "s6", 3, 5, 1_000_000, 268_435_456, 0, 0);
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
fn restart_supersedes_the_old_incarnation() {
    let mut store = store_a();
    let epoch_before = store.epoch();
    store.register_incarnation("krick-1", inc(0x1a)).unwrap();
    assert_eq!(
        store.epoch(),
        epoch_before + 1,
        "the drop advanced the epoch once"
    );
    assert_eq!(
        store.observations().len(),
        3,
        "both krick-1 reports dropped"
    );

    let ghost = obs(
        7,
        &K1_S6,
        "L4",
        "s6",
        3,
        5,
        1_000_000,
        268_435_456,
        3_000_000,
        T - 5000,
    );
    let err = store.ingest(ghost).unwrap_err();
    assert!(
        err.contains("report_shard: node krick-1 incarnation"),
        "{err}"
    );
    assert!(err.contains("is superseded by"), "{err}");

    let mut fresh = obs(7, &K1_S6, "L4", "s6", 3, 5, 1_000_000, 268_435_456, 0, 0);
    fresh.reporter.process_incarnation = inc(0x1a);
    assert_eq!(store.ingest(fresh).expect("ingest"), IngestOutcome::Landed);
    let mut s7_owner = obs(7, &K1_S7, "L7", "s7", 3, 2, 500_000, 134_217_728, 0, 0);
    s7_owner.reporter.process_incarnation = inc(0x1a);
    assert_eq!(
        store.ingest(s7_owner).expect("ingest"),
        IngestOutcome::Landed
    );
    let snapshot = snapshot_a(&store, 118);
    let response = plan_tiers(&snapshot).expect("plan");
    assert_eq!(response.placements[0].rate_nanos, 0);
    assert_eq!(
        response.placements[0].classified_tier.as_deref(),
        Some("archive")
    );
}

#[test]
fn exact_and_conflicting_retries() {
    let mut store = store_a();
    let digest_before = store.observation_set_digest();
    let epoch_before = store.epoch();

    let retry = obs(
        7,
        &K1_S6,
        "L4",
        "s6",
        3,
        5,
        1_000_000,
        268_435_456,
        3_000_000,
        T - 5000,
    );
    assert_eq!(
        store.ingest(retry).expect("ingest"),
        IngestOutcome::Unchanged
    );
    assert_eq!(store.epoch(), epoch_before);
    assert_eq!(store.observation_set_digest(), digest_before);

    let mut conflict = obs(
        7,
        &K1_S6,
        "L4",
        "s6",
        3,
        5,
        1_000_000,
        268_435_456,
        2_999_999,
        T - 5000,
    );
    conflict.scan_bytes = 0;
    conflict.queue_wait_p50_us = 0;
    conflict.queue_wait_p99_us = 0;
    conflict.samples = 0;
    assert_eq!(
            store.ingest(conflict).unwrap_err(),
            "report_shard: observation for (key_bucket=7, gen 9, L4, s6, gen 3) window [1788868200000, 1788868800000) from krick-1/0000000000000000000000000000000a conflicts with the stored report"
        );

    let mut old = obs(7, &K1_S6, "L4", "s6", 3, 5, 1_000_000, 268_435_456, 0, 0);
    old.window_start_unix_ms = W0 - COHORT;
    old.window_end_unix_ms = W0;
    assert_eq!(
        store.ingest(old).expect("ingest"),
        IngestOutcome::Superseded
    );
    assert_eq!(store.epoch(), epoch_before);
}

#[test]
fn committed_identity_is_equality_checked_both_directions() {
    let mut store = ObservationStore::new(COHORT, 0, 1000, 1 << 20, 64, committed_view_a())
        .expect("valid cohort config");
    store.register_incarnation("krick-1", inc(0x0a)).unwrap();
    let base = || obs(7, &K1_S6, "L4", "s6", 3, 5, 1_000_000, 268_435_456, 0, 0);

    let mut ahead = base();
    ahead.shard.source_generation = 4;
    assert_eq!(
        store.ingest(ahead).unwrap_err(),
        "report_shard: shard s6 source generation 4 is not the committed 3"
    );
    let mut behind_epoch = base();
    behind_epoch.shard.ownership_epoch = 4;
    assert!(store
        .ingest(behind_epoch)
        .unwrap_err()
        .contains("ownership epoch 4 is not the committed 5"));
    let mut replaced = base();
    replaced.shard.storage_incarnation = inc(0xaa);
    assert!(store
        .ingest(replaced)
        .unwrap_err()
        .contains("is not the committed"));
    let mut wrong_leaf = base();
    wrong_leaf.leaf = "L7".to_string();
    assert!(store
        .ingest(wrong_leaf)
        .unwrap_err()
        .contains("covers no rows in leaf L7"));
    let mut wrong_shard = base();
    wrong_shard.shard.shard = "s9".to_string();
    assert!(store
        .ingest(wrong_shard)
        .unwrap_err()
        .contains("not in the committed topology"));
    let mut wrong_gen = base();
    wrong_gen.topology_generation = 10;
    assert!(store
        .ingest(wrong_gen)
        .unwrap_err()
        .contains("topology_generation 10 is not the committed 9"));

    let mut store2 = ObservationStore::new(COHORT, 0, 1000, 1 << 20, 64, committed_view_a())
        .expect("valid cohort config");
    assert!(store2
        .ingest(base())
        .unwrap_err()
        .contains("has not registered an incarnation"));
}

#[test]
fn store_bounds_and_impossible_states() {
    let mut store = ObservationStore::new(COHORT, 0, 1000, 1 << 20, 64, committed_view_a())
        .expect("valid cohort config");
    store.register_incarnation("krick-1", inc(0x0a)).unwrap();
    let mut no_bytes = obs(7, &K1_S6, "L4", "s6", 3, 5, 1_000_000, 0, 0, 0);
    assert!(store
        .ingest(no_bytes.clone())
        .unwrap_err()
        .contains("0 resident bytes"));
    no_bytes.rows = 0;
    no_bytes.scans_observed = 5;
    assert!(store
        .ingest(no_bytes)
        .unwrap_err()
        .contains("scans of 0 resident bytes"));

    let mut tight = ObservationStore::new(COHORT, 0, 1, 1 << 20, 64, committed_view_a())
        .expect("valid cohort config");
    tight.register_incarnation("krick-1", inc(0x0a)).unwrap();
    tight.register_incarnation("pi5v1", inc(0x0b)).unwrap();
    tight
        .ingest(obs(
            7,
            &K1_S6,
            "L4",
            "s6",
            3,
            5,
            1_000_000,
            268_435_456,
            0,
            0,
        ))
        .expect("first record");
    assert_eq!(
        tight
            .ingest(obs(
                7,
                &P1_S6,
                "L4",
                "s6",
                3,
                5,
                1_000_000,
                268_435_456,
                0,
                0
            ))
            .unwrap_err(),
        "report_shard: the observation store holds the bound of 1 records"
    );
    let mut newer = obs(7, &K1_S6, "L4", "s6", 3, 5, 1_000_000, 268_435_456, 1, 0);
    newer.last_scanned_unix_ms = W1 - 1000;
    newer.window_start_unix_ms = W1;
    newer.window_end_unix_ms = W1 + COHORT;
    assert_eq!(
        tight.ingest(newer).expect("supersede at the record bound"),
        IngestOutcome::Landed
    );

    let mut store = store_a();
    let dropped = store.commit_expiry(W1 + COHORT, 1000).expect("expiry");
    assert_eq!(dropped, 5);
    assert_eq!(store.observations().len(), 0);
    assert_eq!(store.commit_expiry(W1 + COHORT, 1000).expect("expiry"), 0);
}

#[test]
fn partial_copies_are_not_replicas_and_domains_count_once() {
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
        .ingest(obs(
            9,
            &P3_S6,
            "L4",
            "s6",
            3,
            5,
            60_000,
            10_000_000_000,
            0,
            T - 1000,
        ))
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

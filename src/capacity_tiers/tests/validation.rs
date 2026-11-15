//! Policy validation and the snapshot's validated constructor.

use super::*;

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
    let mut alien = obs(7, &K1_S6, 1_000_000, 268_435_456, 0, 0);
    alien.partition.collection = "briefs".to_string();
    assert!(store
        .ingest(alien)
        .unwrap_err()
        .contains("is not this resource"));
}

/// One named change to a valid snapshot input.
type InputMutation = (&'static str, fn(&mut TierSnapshotInput));

#[test]
fn plan_digest_binds_every_effective_input() {
    let store = store_a();
    let base = snapshot_a(&store, 118).plan_digest();
    assert_eq!(hex32(&base), PLAN_A_HEX);

    let mutators: Vec<InputMutation> = vec![
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
}

#[test]
fn a_device_node_inside_a_server_pool_is_never_a_destination() {
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

use super::capacity::*;
use super::import_tests::{
    checkpoint_of, command as import_command, identity, key, legacy, limits, payload, policy,
    retire, run_import, store, supplement, Directory,
};
use super::*;
use crate::capacity_tiers::{plan_tiers, FragmentRequest, PartitionIdentity, TierSnapshot};
use crate::pb::storage::capacity_transition::Action as TransitionAction;
use crate::pb::storage::control_import_command::Action as ImportAction;
use crate::pb::storage::legacy_control_import_supplement::Placement;
use crate::pb::{AccessAction, CollectionGrant, PlacementNode, PlacementTree};
use crate::test_support::ForkGuarded;
use std::path::PathBuf;
use tonic::Code;

const COHORT_MS: u64 = 60_000;

fn tree() -> PlacementTree {
    PlacementTree {
        column: "placement".into(),
        level_bits: 0,
        nodes: vec![
            PlacementNode {
                name: "old".into(),
                cel: "year < 2020".into(),
                ..Default::default()
            },
            PlacementNode {
                name: "rest".into(),
                ..Default::default()
            },
        ],
    }
}

/// A store holding the two-route legacy import with a placement tree, so
/// shard-a (routes 0..99, primary node/α, generation 7) sits in leaf "old".
struct Imported {
    dir: Directory,
    authority: SourceAuthorityIdentity,
    store: SourceAuthorityStore,
    /// The control revision after the import.
    revision: u64,
}

fn imported(name: &str) -> Imported {
    let dir = Directory::new(name);
    let limits = limits(64 << 10, 256);
    let authority = identity(7);
    let store = store(&dir, 7, &limits);
    let legacy = legacy(&dir, 2);
    let (retired, _) = retire(&store, &legacy, 1);
    let checkpoint = checkpoint_of(&retired);
    let placement = crate::placement::Placement::validate(
        &crate::placement::PlacementTreeConfig::from_proto(&tree()),
    )
    .unwrap();
    let leaf = |name: &str| placement.leaf_by_name(name).unwrap().code as u64;
    let mut with_tree = supplement(&checkpoint);
    with_tree.placement = Some(Placement::Tree(tree()));
    with_tree.route_codes = vec![
        PlacementRouteCode {
            has_placement: true,
            placement: leaf("old"),
        },
        PlacementRouteCode {
            has_placement: true,
            placement: leaf("rest"),
        },
    ];
    let payload = payload(&retired, with_tree);
    let (_, revision, _) = run_import(&store, &authority, &limits, &retired, &payload, 1);
    Imported {
        dir,
        authority,
        store,
        revision,
    }
}

fn configuration(policy_name: &str) -> CapacityConfiguration {
    CapacityConfiguration {
        format_version: 1,
        cohort_length_ms: COHORT_MS,
        cohort_phase_unix_ms: 0,
        max_total_records: 64,
        max_total_bytes: 1 << 20,
        max_registered_nodes: 8,
        policy: Some(CapacityTierPolicy {
            format_version: 1,
            tiers: vec![CapacityTierSpec {
                name: policy_name.into(),
                residency: CapacityResidency::Server as i32,
                min_replicas: 1,
                scans_per_byte_nanos_lo: 0,
                scans_per_byte_nanos_hi: 1_000_000_000,
                max_seconds_since_scan: 3600,
            }],
        }),
    }
}

fn configure(
    authority: &SourceAuthorityIdentity,
    id: &str,
    revision: u64,
    configuration: CapacityConfiguration,
) -> CapacityConfigureCommand {
    CapacityConfigureCommand {
        format_version: 1,
        authority: Some(authority.clone()),
        key: Some(key("books")),
        command_id: id.as_bytes().to_vec(),
        expected_control_revision: revision,
        expected_policy_revision: 1,
        configuration: Some(configuration),
    }
}

fn transition(authority: &SourceAuthorityIdentity, action: TransitionAction) -> CapacityTransition {
    CapacityTransition {
        format_version: 1,
        authority: Some(authority.clone()),
        key: Some(key("books")),
        action: Some(action),
    }
}

fn register(
    authority: &SourceAuthorityIdentity,
    node: &str,
    incarnation: u8,
) -> CapacityTransition {
    transition(
        authority,
        TransitionAction::Register(RegisterCapacityReporter {
            node_id: node.into(),
            process_incarnation: vec![incarnation; 16],
        }),
    )
}

/// A report from node/α about shard-a in leaf "old", cohort window `window`.
fn observation(incarnation: u8, window: u64, scans: u64) -> CapacityObservation {
    CapacityObservation {
        format_version: 1,
        partition: Some(CapacityPartition {
            workspace: "workspace-a".into(),
            collection: "books".into(),
            derived_fingerprint: vec![0; 32],
            column: "year_bucket".into(),
            bucket: 7,
        }),
        node_id: "node/α".into(),
        process_incarnation: vec![incarnation; 16],
        shard: Some(CapacityShardRef {
            shard: "shard-a".into(),
            source_generation: 7,
            storage_incarnation: vec![0; 16],
            ownership_epoch: 0,
        }),
        topology_generation: 3,
        leaf: "old".into(),
        rows: 1000,
        resident_bytes: 4096,
        scans_observed: scans,
        scan_bytes: scans * 4096,
        queue_wait_p50_us: 10,
        queue_wait_p99_us: 100,
        samples: 5,
        last_scanned_unix_ms: window * COHORT_MS + 100,
        window_start_unix_ms: window * COHORT_MS,
        window_end_unix_ms: (window + 1) * COHORT_MS,
    }
}

fn report(
    authority: &SourceAuthorityIdentity,
    observation: CapacityObservation,
) -> CapacityTransition {
    transition(authority, TransitionAction::Report(observation))
}

fn expire(authority: &SourceAuthorityIdentity, now: u64, max_age: u64) -> CapacityTransition {
    transition(
        authority,
        TransitionAction::Expire(CommitCapacityExpiry {
            now_unix_ms: now,
            max_age_ms: max_age,
        }),
    )
}

fn context(instant: u64) -> PlanningContext {
    PlanningContext {
        planning_instant_unix_ms: instant,
        max_moves: 4,
        max_observation_age_ms: 10 * COHORT_MS,
        clock_skew_bound_ms: 1_000,
        fragment_requests: vec![FragmentRequest {
            partition: PartitionIdentity {
                workspace: "workspace-a".into(),
                collection: "books".into(),
                derived_fingerprint: [0; 32],
                column: "year_bucket".into(),
                bucket: 7,
            },
            topology_generation: 3,
            leaf: "old".into(),
        }],
    }
}

/// The planner's view of the store: the input's debug form, the validated
/// snapshot's plan digest and the plan's canonical bytes.
fn planner_view(store: &SourceAuthorityStore, instant: u64) -> (String, [u8; 32], Vec<u8>) {
    let input = store
        .planner_input("alice", &key("books"), &context(instant))
        .unwrap();
    let rendered = format!("{input:?}");
    let snapshot = TierSnapshot::validated(input).unwrap();
    let plan = plan_tiers(&snapshot).unwrap();
    (rendered, snapshot.plan_digest(), plan.canonical_bytes())
}

#[test]
fn configure_is_a_retained_control_command_over_applied_state() {
    let dir = Directory::new("configure-unapplied");
    let limits = limits(64 << 10, 64);
    let authority = identity(7);
    let store = store(&dir, 7, &limits);
    // Without applied control state there is nothing to plan over.
    let refused = store
        .configure_capacity(
            "alice",
            &configure(&authority, "c1", 1, configuration("hot")),
        )
        .unwrap();
    assert_eq!(
        refused.code,
        Code::FailedPrecondition as u32,
        "{}",
        refused.message
    );
    assert!(
        refused.message.contains("no applied control state"),
        "{}",
        refused.message
    );
    drop(store);

    let fixture = imported("configure");
    let store = &fixture.store;
    let authority = &fixture.authority;
    let revision = fixture.revision;
    // Invalid configurations are envelope refusals, never decisions.
    let mut device = configuration("hot");
    device.policy.as_mut().unwrap().tiers[0].residency = CapacityResidency::Device as i32;
    assert_eq!(
        store
            .configure_capacity("alice", &configure(authority, "bad-tier", revision, device))
            .err()
            .unwrap()
            .code(),
        Code::InvalidArgument
    );
    let mut wide = configuration("hot");
    wide.max_total_records = MAX_CAPACITY_RECORDS + 1;
    assert_eq!(
        store
            .configure_capacity("alice", &configure(authority, "bad-bound", revision, wide))
            .err()
            .unwrap()
            .code(),
        Code::InvalidArgument
    );
    assert_eq!(
        store
            .configure_capacity(
                "bob",
                &configure(authority, "bob", revision, configuration("hot"))
            )
            .unwrap()
            .code,
        0
    );
    // bob's configuration landed; alice's exact retry of her own id is hers.
    let stale = store
        .configure_capacity(
            "alice",
            &configure(authority, "c1", revision, configuration("hot")),
        )
        .unwrap();
    assert_eq!(stale.code, Code::FailedPrecondition as u32);
    let accepted = store
        .configure_capacity(
            "alice",
            &configure(authority, "c2", revision + 1, configuration("warm")),
        )
        .unwrap();
    assert_eq!(accepted.code, 0, "{}", accepted.message);
    assert_eq!(accepted.control_revision, revision + 2);
    assert_eq!(
        store
            .configure_capacity(
                "alice",
                &configure(authority, "c2", revision + 1, configuration("warm"))
            )
            .unwrap(),
        accepted
    );
    assert_eq!(
        store
            .configure_capacity(
                "alice",
                &configure(authority, "c2", revision + 1, configuration("cold"))
            )
            .err()
            .unwrap()
            .code(),
        Code::FailedPrecondition
    );
    // The shared command_id namespace: an import id cannot be reused here
    // and vice versa.
    assert_eq!(
        store
            .configure_capacity(
                "alice",
                &configure(authority, "commit", revision + 2, configuration("x"))
            )
            .err()
            .unwrap()
            .code(),
        Code::FailedPrecondition
    );
    assert_eq!(
        store
            .execute_control_import(
                "alice",
                &import_command(
                    authority,
                    "c2",
                    revision + 2,
                    ImportAction::Abort(AbortControlImport {})
                ),
            )
            .err()
            .unwrap()
            .code(),
        Code::FailedPrecondition
    );
    let state = store.capacity_state("alice", &key("books")).unwrap();
    assert_eq!(state.configured_control_revision, revision + 2);
    assert_eq!(
        state.configuration.unwrap().policy.unwrap().tiers[0].name,
        "warm"
    );
    assert_eq!(
        store
            .capacity_configure_decision("alice", &key("books"), b"c2")
            .unwrap(),
        accepted
    );
    drop(fixture);
}

#[test]
fn transitions_register_report_expire_and_recover_identically() {
    let fixture = imported("transitions");
    let store = &fixture.store;
    let authority = &fixture.authority;
    let revision = fixture.revision;
    assert_eq!(
        store
            .capacity_transition("alice", &register(authority, "node/α", 1))
            .err()
            .unwrap()
            .code(),
        Code::FailedPrecondition,
        "no configuration yet"
    );
    assert_eq!(
        store
            .configure_capacity(
                "alice",
                &configure(authority, "cfg", revision, configuration("hot"))
            )
            .unwrap()
            .code,
        0
    );
    // A report before registration refuses with Kimi's refusal text.
    let error = store
        .capacity_transition("alice", &report(authority, observation(1, 5, 10)))
        .err()
        .unwrap();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("has not registered"), "{error}");
    let registered = store
        .capacity_transition("alice", &register(authority, "node/α", 1))
        .unwrap();
    assert_eq!(registered.observation_epoch, 0);
    assert_eq!(
        store
            .capacity_transition("alice", &register(authority, "node/α", 1))
            .unwrap(),
        registered
    );
    // Land, exact retry, conflict, supersession, newer window.
    let landed = store
        .capacity_transition("alice", &report(authority, observation(1, 5, 10)))
        .unwrap();
    assert_eq!(landed.outcome, CapacityTransitionOutcome::Landed as i32);
    assert_eq!(landed.observation_epoch, 1);
    assert_eq!(landed.control_revision, revision + 1);
    let again = store
        .capacity_transition("alice", &report(authority, observation(1, 5, 10)))
        .unwrap();
    assert_eq!(again.outcome, CapacityTransitionOutcome::Unchanged as i32);
    assert_eq!(again.observation_epoch, 1);
    let conflict = store
        .capacity_transition("alice", &report(authority, observation(1, 5, 11)))
        .err()
        .unwrap();
    assert_eq!(conflict.code(), Code::FailedPrecondition);
    assert!(conflict.message().contains("conflicts"), "{conflict}");
    let older = store
        .capacity_transition("alice", &report(authority, observation(1, 4, 3)))
        .unwrap();
    assert_eq!(older.outcome, CapacityTransitionOutcome::Superseded as i32);
    assert_eq!(older.observation_epoch, 1);
    let newer = store
        .capacity_transition("alice", &report(authority, observation(1, 6, 12)))
        .unwrap();
    assert_eq!(newer.outcome, CapacityTransitionOutcome::Landed as i32);
    assert_eq!(newer.observation_epoch, 2);
    let state = store.capacity_state("alice", &key("books")).unwrap();
    assert_eq!((state.reporter_count, state.observation_count), (1, 1));
    // Reports against uncommitted identity are refused by name.
    let mut wrong_leaf = observation(1, 6, 12);
    wrong_leaf.leaf = "rest".into();
    let error = store
        .capacity_transition("alice", &report(authority, wrong_leaf))
        .err()
        .unwrap();
    assert!(
        error.message().contains("covers no rows in leaf rest"),
        "{error}"
    );
    let mut wrong_generation = observation(1, 6, 12);
    wrong_generation.shard.as_mut().unwrap().source_generation = 8;
    let error = store
        .capacity_transition("alice", &report(authority, wrong_generation))
        .err()
        .unwrap();
    assert!(
        error
            .message()
            .contains("source generation 8 is not the committed 7"),
        "{error}"
    );
    let mut misaligned = observation(1, 6, 12);
    misaligned.window_start_unix_ms += 1;
    assert_eq!(
        store
            .capacity_transition("alice", &report(authority, misaligned))
            .err()
            .unwrap()
            .code(),
        Code::FailedPrecondition
    );
    // Planner input is served from the committed rows; the planning instant
    // sits just after the observed window closes.
    let (rendered, digest, plan) = planner_view(store, 7 * COHORT_MS);
    assert!(rendered.contains("observation_epoch: 2"), "{rendered}");

    // A restart of node/α: the new incarnation supersedes the old one and
    // its observations are dropped in the same committed transition.
    let restarted = store
        .capacity_transition("alice", &register(authority, "node/α", 2))
        .unwrap();
    assert_eq!(restarted.dropped, 1);
    assert_eq!(restarted.observation_epoch, 3);
    let error = store
        .capacity_transition("alice", &report(authority, observation(1, 7, 1)))
        .err()
        .unwrap();
    assert!(error.message().contains("is superseded by"), "{error}");
    assert_eq!(
        store
            .capacity_transition("alice", &report(authority, observation(2, 7, 1)))
            .unwrap()
            .observation_epoch,
        4
    );
    // The cohort cannot change with observations retained; expiry first.
    let mut shifted = configuration("hot");
    shifted.cohort_phase_unix_ms = 5;
    let refused = store
        .configure_capacity(
            "alice",
            &configure(authority, "shift", revision + 1, shifted),
        )
        .unwrap();
    assert_eq!(
        refused.code,
        Code::FailedPrecondition as u32,
        "{}",
        refused.message
    );
    let nothing = store
        .capacity_transition("alice", &expire(authority, 8 * COHORT_MS, 10 * COHORT_MS))
        .unwrap();
    assert_eq!((nothing.dropped, nothing.observation_epoch), (0, 4));
    let expired = store
        .capacity_transition("alice", &expire(authority, 20 * COHORT_MS, COHORT_MS))
        .unwrap();
    assert_eq!((expired.dropped, expired.observation_epoch), (1, 5));
    let state = store.capacity_state("alice", &key("books")).unwrap();
    assert_eq!(
        (
            state.reporter_count,
            state.observation_count,
            state.observation_epoch
        ),
        (1, 0, 5)
    );
    assert_eq!(
        store
            .capacity_transition("alice", &report(authority, observation(2, 9, 2)))
            .unwrap()
            .observation_epoch,
        6
    );
    let before = planner_view(store, 10 * COHORT_MS);
    assert_ne!(before.1, digest);
    assert_ne!(before.2, plan);

    // Reopen recomputes every count and re-lands every row; the planner
    // sees identical inputs and produces identical outputs.
    drop(fixture.store.clone());
    let Imported { dir, store, .. } = fixture;
    drop(store);
    let reopened = SourceAuthorityStore::open(&dir.authority(), authority).unwrap();
    let after = planner_view(&reopened, 10 * COHORT_MS);
    assert_eq!(after, before);
    let state = reopened.capacity_state("alice", &key("books")).unwrap();
    assert_eq!(
        (
            state.reporter_count,
            state.observation_count,
            state.observation_epoch
        ),
        (1, 1, 6)
    );
    assert_eq!(
        reopened
            .capacity_transition("alice", &report(authority, observation(2, 9, 2)))
            .unwrap()
            .outcome,
        CapacityTransitionOutcome::Unchanged as i32
    );
}

#[test]
fn policy_and_configuration_changes_are_bound_into_the_plan_digest() {
    let fixture = imported("policy-changes");
    let store = &fixture.store;
    let authority = &fixture.authority;
    let revision = fixture.revision;
    assert_eq!(
        store
            .configure_capacity(
                "alice",
                &configure(authority, "cfg", revision, configuration("hot"))
            )
            .unwrap()
            .code,
        0
    );
    store
        .capacity_transition("alice", &register(authority, "node/α", 1))
        .unwrap();
    store
        .capacity_transition("alice", &report(authority, observation(1, 5, 10)))
        .unwrap();
    let base = planner_view(store, 6 * COHORT_MS);
    // An access-policy change advances the policy revision the plan binds.
    let grants = SourceAuthorityCommand {
        format_version: 1,
        authority: Some(authority.clone()),
        key: Some(key("books")),
        command_id: b"grants".to_vec(),
        expected_control_revision: revision + 1,
        expected_policy_revision: 1,
        expected_ownership_generation: 0,
        action: Some(Action::ReplaceGrants(ReplaceSourceCollectionGrants {
            grants: vec![
                CollectionGrant {
                    principal: "alice".into(),
                    workspace: "workspace-a".into(),
                    collection: "books".into(),
                    actions: vec![AccessAction::Admin as i32, AccessAction::Search as i32],
                    ..Default::default()
                },
                CollectionGrant {
                    principal: "bob".into(),
                    workspace: "workspace-a".into(),
                    collection: "books".into(),
                    actions: vec![AccessAction::Admin as i32],
                    ..Default::default()
                },
            ],
        })),
    };
    assert_eq!(store.execute("alice", &grants).unwrap().code, 0);
    let after_grants = planner_view(store, 6 * COHORT_MS);
    assert_ne!(after_grants.1, base.1);
    assert!(
        after_grants.0.contains("policy_revision: 2"),
        "{}",
        after_grants.0
    );
    // A tier-policy change is a control command and changes the digest too.
    let mut cfg = configure(authority, "cfg2", revision + 2, configuration("warm"));
    cfg.expected_policy_revision = 2;
    assert_eq!(store.configure_capacity("alice", &cfg).unwrap().code, 0);
    let after_policy = planner_view(store, 6 * COHORT_MS);
    assert_ne!(after_policy.1, after_grants.1);
    // Observations survive both; a revoked actor cannot read the input.
    assert!(
        after_policy.0.contains("observation_epoch: 1"),
        "{}",
        after_policy.0
    );
    assert_eq!(
        store
            .planner_input("carol", &key("books"), &context(6 * COHORT_MS))
            .err()
            .unwrap()
            .code(),
        Code::PermissionDenied
    );
    // Reopen reproduces the final view exactly.
    let Imported { dir, store, .. } = fixture;
    drop(store);
    let reopened = SourceAuthorityStore::open(&dir.authority(), authority).unwrap();
    assert_eq!(planner_view(&reopened, 6 * COHORT_MS), after_policy);
}

#[test]
fn capacity_exit_worker() {
    let Some(mode) = std::env::var_os("PSEARCH_CAPACITY_EXIT_FAULT") else {
        return;
    };
    let root = PathBuf::from(std::env::var_os("PSEARCH_CAPACITY_DIR").unwrap());
    let mode = mode.to_str().unwrap().to_string();
    let (phase, fault) = mode.split_once(':').unwrap();
    let dir = Directory(root);
    let limits = limits(64 << 10, 256);
    let authority = identity(7);
    let store =
        SourceAuthorityStore::create(&dir.authority(), &authority, &policy(), &limits).unwrap();
    let legacy = crate::control_plane::DurableControlPlane::open_existing(
        dir.legacy(),
        crate::control_plane::test_fixtures::populated_policy(),
    )
    .unwrap()
    .with_collection("books")
    .unwrap();
    let (retired, _) = retire(&store, &legacy, 1);
    let checkpoint = checkpoint_of(&retired);
    let placement = crate::placement::Placement::validate(
        &crate::placement::PlacementTreeConfig::from_proto(&tree()),
    )
    .unwrap();
    let mut with_tree = supplement(&checkpoint);
    with_tree.placement = Some(Placement::Tree(tree()));
    with_tree.route_codes = ["old", "rest"]
        .map(|name| PlacementRouteCode {
            has_placement: true,
            placement: placement.leaf_by_name(name).unwrap().code as u64,
        })
        .to_vec();
    let payload = payload(&retired, with_tree);
    let (_, revision, _) = run_import(&store, &authority, &limits, &retired, &payload, 1);
    let arm = |store: &SourceAuthorityStore| {
        *store.inner.fault.lock().unwrap() = Some(match fault {
            "before" => Fault::ExitBeforeCommit,
            "after" => Fault::ExitAfterCommit,
            other => panic!("unknown exit fault {other}"),
        });
    };
    if phase == "configure" {
        arm(&store);
    }
    store
        .configure_capacity(
            "alice",
            &configure(&authority, "cfg", revision, configuration("hot")),
        )
        .unwrap();
    store
        .capacity_transition("alice", &register(&authority, "node/α", 1))
        .unwrap();
    if phase == "report" {
        arm(&store);
    }
    store
        .capacity_transition("alice", &report(&authority, observation(1, 5, 10)))
        .unwrap();
    std::mem::forget(dir);
    panic!("exit fault did not terminate the worker");
}

#[test]
fn abrupt_exit_around_capacity_commits_recovers_and_replays() {
    for phase in ["configure", "report"] {
        for fault in ["before", "after"] {
            let dir = Directory::new(&format!("capacity-exit-{phase}-{fault}"));
            std::fs::write(
                dir.legacy(),
                crate::control_plane::test_fixtures::populated_state_json("books", 2),
            )
            .unwrap();
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("source_authority::capacity_tests::capacity_exit_worker")
                .arg("--nocapture")
                .env("PSEARCH_CAPACITY_EXIT_FAULT", format!("{phase}:{fault}"))
                .env("PSEARCH_CAPACITY_DIR", &dir.0)
                .status_guarded()
                .unwrap();
            assert_eq!(status.code(), Some(87), "{phase}:{fault}");
            let authority = identity(7);
            let store = SourceAuthorityStore::open(&dir.authority(), &authority).unwrap();
            let revision = store
                .control_snapshot("alice", &key("books"))
                .unwrap()
                .control_revision;
            let configured = store.capacity_state("alice", &key("books"));
            match (phase, fault) {
                ("configure", "before") => {
                    assert_eq!(configured.err().unwrap().code(), Code::NotFound);
                    assert_eq!(
                        store
                            .configure_capacity(
                                "alice",
                                &configure(&authority, "cfg", revision, configuration("hot"))
                            )
                            .unwrap()
                            .code,
                        0
                    );
                }
                ("configure", "after") => {
                    assert_eq!(configured.unwrap().configured_control_revision, revision);
                    let retry = store
                        .configure_capacity(
                            "alice",
                            &configure(&authority, "cfg", revision - 1, configuration("hot")),
                        )
                        .unwrap();
                    assert_eq!(retry.code, 0);
                    assert_eq!(retry.control_revision, revision);
                }
                ("report", "before") => {
                    assert_eq!(configured.unwrap().observation_count, 0);
                    assert_eq!(
                        store
                            .capacity_transition(
                                "alice",
                                &report(&authority, observation(1, 5, 10))
                            )
                            .unwrap()
                            .outcome,
                        CapacityTransitionOutcome::Landed as i32
                    );
                }
                _ => {
                    let state = configured.unwrap();
                    assert_eq!((state.observation_count, state.observation_epoch), (1, 1));
                    assert_eq!(
                        store
                            .capacity_transition(
                                "alice",
                                &report(&authority, observation(1, 5, 10))
                            )
                            .unwrap()
                            .outcome,
                        CapacityTransitionOutcome::Unchanged as i32
                    );
                }
            }
            if phase == "configure" {
                // Configure-phase workers exit before any report; land one so
                // the planner view exists on both sides of the reopen.
                store
                    .capacity_transition("alice", &register(&authority, "node/α", 1))
                    .unwrap();
                store
                    .capacity_transition("alice", &report(&authority, observation(1, 5, 10)))
                    .unwrap();
            }
            let view = planner_view(&store, 6 * COHORT_MS);
            drop(store);
            let reopened = SourceAuthorityStore::open(&dir.authority(), &authority).unwrap();
            assert_eq!(planner_view(&reopened, 6 * COHORT_MS), view);
        }
    }
}

#[test]
fn rows_that_disagree_with_the_committed_view_refuse_the_open() {
    let fixture = imported("tamper");
    let store = &fixture.store;
    let authority = &fixture.authority;
    let revision = fixture.revision;
    store
        .configure_capacity(
            "alice",
            &configure(authority, "cfg", revision, configuration("hot")),
        )
        .unwrap();
    store
        .capacity_transition("alice", &register(authority, "node/α", 1))
        .unwrap();
    store
        .capacity_transition("alice", &report(authority, observation(1, 5, 10)))
        .unwrap();
    let key_bytes = observation_key(&key("books"), &observation(1, 5, 10));
    let Imported { dir, store, .. } = fixture;
    // A value whose identity differs from its key.
    {
        let tx = store.inner.database().begin_write().unwrap();
        {
            let mut observations = tx.open_table(OBSERVATIONS).unwrap();
            let mut forged = observation(1, 5, 10);
            forged.node_id = "node z".into();
            observations
                .insert(key_bytes.as_slice(), forged.encode_to_vec().as_slice())
                .unwrap();
        }
        tx.commit().unwrap();
    }
    drop(store);
    let error = SourceAuthorityStore::open(&dir.authority(), authority)
        .err()
        .unwrap();
    assert_eq!(error.code(), Code::DataLoss);
    assert!(error.message().contains("key differs"), "{error}");
}

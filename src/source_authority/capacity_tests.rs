use super::capacity::*;
use super::control_tests::{
    command as install_command, identity, install, key, limits, policy, state, state_with_tree,
    store, Directory,
};
use super::*;
use crate::capacity_tiers::{plan_tiers, FragmentRequest, PartitionIdentity, TierSnapshot};
use crate::pb::storage::capacity_transition::Action as TransitionAction;
use crate::pb::{AccessAction, CollectionGrant};
use crate::test_support::ForkGuarded;
use std::path::PathBuf;
use tonic::Code;

mod configure;
mod transitions;

const COHORT_MS: u64 = 60_000;

/// A store holding the installed `state_with_tree`.
struct Installed {
    dir: Directory,
    authority: SourceAuthorityIdentity,
    store: SourceAuthorityStore,
    /// The control revision after the install.
    revision: u64,
}

fn installed(name: &str) -> Installed {
    let dir = Directory::new(name);
    let authority = identity(7);
    let store = store(&dir, 7, &limits(64 << 10, 256));
    let revision = install(&store, &authority, state_with_tree(), 1).control_revision;
    Installed {
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
    let revision = install(&store, &authority, state_with_tree(), 1).control_revision;
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
            the_interrupted_step_recovers(&store, &authority, revision, (phase, fault));
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

/// The step the worker exited around either never committed and applies
/// now, or committed and answers its exact retry from the record.
fn the_interrupted_step_recovers(
    store: &SourceAuthorityStore,
    authority: &SourceAuthorityIdentity,
    revision: u64,
    (phase, fault): (&str, &str),
) {
    let configured = store.capacity_state("alice", &key("books"));
    match (phase, fault) {
        ("configure", "before") => {
            assert_eq!(configured.err().unwrap().code(), Code::NotFound);
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
        }
        ("configure", "after") => {
            assert_eq!(configured.unwrap().configured_control_revision, revision);
            let retry = store
                .configure_capacity(
                    "alice",
                    &configure(authority, "cfg", revision - 1, configuration("hot")),
                )
                .unwrap();
            assert_eq!(retry.code, 0);
            assert_eq!(retry.control_revision, revision);
        }
        ("report", "before") => {
            assert_eq!(configured.unwrap().observation_count, 0);
            assert_eq!(
                store
                    .capacity_transition("alice", &report(authority, observation(1, 5, 10)))
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
                    .capacity_transition("alice", &report(authority, observation(1, 5, 10)))
                    .unwrap()
                    .outcome,
                CapacityTransitionOutcome::Unchanged as i32
            );
        }
    }
}

#[test]
fn rows_that_disagree_with_the_committed_view_refuse_the_open() {
    let fixture = installed("tamper");
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
    let Installed { dir, store, .. } = fixture;
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

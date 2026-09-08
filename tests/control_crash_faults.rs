//! Crash-matrix target (slice 3d;
//! `docs/control-authority-test-harness.md`).
//!
//! Process-exit fault injection around the durable commit boundaries the
//! review calls out: capacity configure/report, import administrative
//! recovery, and the managed-catalog bind/activate fences. Every case runs a
//! worker process that arms one exit fault (`ExitFault::{BeforeCommit,
//! AfterCommit}`, process exit code 87) immediately before the commit under
//! test; the parent first computes a NO-CRASH baseline in a twin directory,
//! then asserts the documented recovery window and that the state converges
//! to the baseline through the public reopen/retry API only.
//!
//! These are the behavioral contract pinned by
//! `docs/source-owner-admission.md`'s crash windows and by the in-crate
//! reference tests (`src/source_authority/capacity_tests.rs`,
//! `src/source_authority/import_tests.rs`,
//! `src/document_catalog/managed/tests.rs`), re-driven through the public
//! adversarial-kit surface. At the frozen checkpoint `331c18f` every case
//! PASSES — they are the control group for the R3/R5 reproductions, proving
//! the crash machinery itself is sound before Fable changes the snapshot
//! path.
//!
//! Run: `cargo test --test control_crash_faults --features
//! raft,fault-injection -- --test-threads=2`.
#![cfg(feature = "fault-injection")]

mod control_adversarial;

use std::sync::Arc;

use control_adversarial::kit;
use pipestream_search::authorization::{AccessPermit, Authorizer, PolicyAuthority};
use pipestream_search::document_catalog::{
    AccessControlledCatalog, ActiveManagedCatalog, PreparedManagedCatalog,
};
use pipestream_search::pb::storage::source_authority_command::Action;
use pipestream_search::pb::storage::{CapacityTransitionOutcome, ControlImportPhase};
use pipestream_search::pb::storage::{
    ConfirmSourceOwnerReady, LogicalSourceOwner, PrepareSourceOwner, PreparedSourceOwner,
    PreparedSourceOwnerPhase, SourceAuthorityCommand, SourceAuthorityIdentity,
    SourceAuthorityLimits, SourceManagedBinding, SourceOwnerCompletion, SourceResidency,
    SourceResourceBinding, SourceStorageTarget,
};
use pipestream_search::pb::AcceptDocumentRequest;
use pipestream_search::pb::{
    accept_document_request::Mutation, AccessAction, AccessPolicy, CollectionGrant,
    CollectionResource, ProtobufSource,
};
use pipestream_search::source_authority::{ExitFault, SourceAuthorityStore};
use tonic::Code;

const CRASH_ENV: &str = "PSEARCH_CRASH_WORKER";
const CRASH_FAMILY_ENV: &str = "PSEARCH_CRASH_FAMILY";
const CRASH_FAULT_ENV: &str = "PSEARCH_CRASH_FAULT";

fn parse_fault() -> ExitFault {
    match std::env::var(CRASH_FAULT_ENV).unwrap().as_str() {
        "before" => ExitFault::BeforeCommit,
        "after" => ExitFault::AfterCommit,
        other => panic!("unknown crash fault {other}"),
    }
}

/// A worker body returns (and its #[test] panics) when the armed fault did
/// not terminate the process.
fn fault_must_fire() -> ! {
    panic!("exit fault did not terminate the worker");
}

// ---- capacity family --------------------------------------------------------

/// Build the deterministic capacity state the configure crash point needs:
/// cluster legacy plane, retirement, placement import — everything through
/// the import commit (control revision 6), no capacity commands.
fn capacity_import_only(dir: &kit::TestDir) -> (SourceAuthorityStore, SourceAuthorityIdentity) {
    let (store, authority) = kit::create_store(dir);
    let legacy = kit::legacy_plane_with_cluster(dir);
    let (retired, _request) = kit::retire(&store, &legacy, 1);
    let payload = kit::import_payload_placement(&retired);
    kit::run_import_staged(&store, &authority, &retired, &payload, false);
    (store, authority)
}

/// Land the two registers and the two observations so the planner view
/// exists; identical schedule to `kit::adapter_fixture`'s tail.
fn land_capacity_tail(store: &SourceAuthorityStore, authority: &SourceAuthorityIdentity) {
    let generation = kit::committed_generation(store);
    store
        .capacity_transition(
            "alice",
            &kit::register_transition(authority, kit::NODE_A, kit::INC_A),
        )
        .unwrap();
    store
        .capacity_transition(
            "alice",
            &kit::register_transition(authority, kit::NODE_B, kit::INC_B),
        )
        .unwrap();
    store
        .capacity_transition(
            "alice",
            &kit::report_transition(
                authority,
                kit::scanned_observation(kit::NODE_A, kit::INC_A, generation),
            ),
        )
        .unwrap();
    store
        .capacity_transition(
            "alice",
            &kit::report_transition(
                authority,
                kit::silent_observation(kit::NODE_B, kit::INC_B, generation),
            ),
        )
        .unwrap();
}

fn spawn_crash_worker(
    test_name: &str,
    dir: &kit::TestDir,
    family: &str,
    fault: &str,
) -> kit::KillOnDrop {
    kit::KillOnDrop(kit::spawn_worker(
        test_name,
        &[
            (CRASH_ENV, "1"),
            (kit::WORKER_DIR_ENV, dir.path().to_str().unwrap()),
            (CRASH_FAMILY_ENV, family),
            (CRASH_FAULT_ENV, fault),
        ],
    ))
}

/// Worker: capacity configure/report crash body, env-selected.
#[test]
fn crash_worker_capacity() {
    if std::env::var_os(CRASH_ENV).is_none() {
        return;
    }
    let dir = kit::TestDir::from_env();
    let family = std::env::var(CRASH_FAMILY_ENV).unwrap();
    let fault = parse_fault();
    let (store, authority) = capacity_import_only(&dir);
    let generation = kit::committed_generation(&store);
    match family.as_str() {
        "configure" => {
            store.arm_exit_fault(fault);
            let decision = store
                .configure_capacity(
                    "alice",
                    &kit::configure_command(
                        &authority,
                        "cfg",
                        6,
                        1,
                        kit::capacity_configuration(1_000_000_000_000),
                    ),
                )
                .unwrap();
            assert_eq!(decision.code, 0, "{}", decision.message);
            fault_must_fire();
        }
        "report" => {
            store
                .configure_capacity(
                    "alice",
                    &kit::configure_command(
                        &authority,
                        "cfg",
                        6,
                        1,
                        kit::capacity_configuration(1_000_000_000_000),
                    ),
                )
                .unwrap();
            store
                .capacity_transition(
                    "alice",
                    &kit::register_transition(&authority, kit::NODE_A, kit::INC_A),
                )
                .unwrap();
            store
                .capacity_transition(
                    "alice",
                    &kit::register_transition(&authority, kit::NODE_B, kit::INC_B),
                )
                .unwrap();
            store.arm_exit_fault(fault);
            store
                .capacity_transition(
                    "alice",
                    &kit::report_transition(
                        &authority,
                        kit::scanned_observation(kit::NODE_A, kit::INC_A, generation),
                    ),
                )
                .unwrap();
            fault_must_fire();
        }
        other => panic!("unknown capacity crash family {other}"),
    }
}

/// Crash matrix: exit immediately before/after the capacity-configuration
/// commit. The window contract (mirrors
/// `capacity_tests::abrupt_exit_around_capacity_commits_recovers_and_replays`):
/// before -> no configuration committed, a fresh configure lands; after ->
/// the configuration is committed and an exact retry returns the stored
/// decision. Both converge to the no-crash baseline digest and revision.
#[test]
fn crash_capacity_configure_before_and_after_commit() {
    let baseline = kit::adapter_fixture("crash-cap-cfg-baseline");
    let baseline_view = kit::planner_view(&baseline.store, kit::PLANNING_INSTANT_UNIX_MS);
    let baseline_revision = baseline
        .store
        .control_snapshot("alice", &kit::key())
        .unwrap()
        .control_revision;
    drop(baseline);

    for fault in ["before", "after"] {
        let dir = kit::TestDir::new(&format!("crash-cap-cfg-{fault}"));
        let mut worker = spawn_crash_worker("crash_worker_capacity", &dir, "configure", fault);
        let status = worker.child().wait().unwrap();
        assert_eq!(status.code(), Some(87), "configure:{fault} exit code");

        let store = kit::open_store(&dir, &kit::identity(kit::SEED));
        let authority = kit::identity(kit::SEED);
        let revision = store
            .control_snapshot("alice", &kit::key())
            .unwrap()
            .control_revision;
        let configured = store.capacity_state("alice", &kit::key());
        if fault == "before" {
            assert_eq!(
                configured.err().unwrap().code(),
                Code::NotFound,
                "configure:before must commit no configuration"
            );
            let decision = store
                .configure_capacity(
                    "alice",
                    &kit::configure_command(
                        &authority,
                        "cfg",
                        revision,
                        1,
                        kit::capacity_configuration(1_000_000_000_000),
                    ),
                )
                .unwrap();
            assert_eq!(decision.code, 0, "{}", decision.message);
        } else {
            assert_eq!(
                configured.unwrap().configured_control_revision,
                revision,
                "configure:after must leave the committed configuration durable"
            );
            let retry = store
                .configure_capacity(
                    "alice",
                    &kit::configure_command(
                        &authority,
                        "cfg",
                        revision - 1,
                        1,
                        kit::capacity_configuration(1_000_000_000_000),
                    ),
                )
                .unwrap();
            assert_eq!(retry.code, 0, "{}", retry.message);
            assert_eq!(
                retry.control_revision, revision,
                "the exact retry must return the stored decision"
            );
        }
        land_capacity_tail(&store, &authority);
        let view = kit::planner_view(&store, kit::PLANNING_INSTANT_UNIX_MS);
        assert_eq!(view, baseline_view, "configure:{fault} planner view");
        let final_revision = store
            .control_snapshot("alice", &kit::key())
            .unwrap()
            .control_revision;
        assert_eq!(
            final_revision, baseline_revision,
            "configure:{fault} final control revision"
        );
        drop(store);
        drop(dir);
    }
}

/// Crash matrix: exit immediately before/after the first capacity report
/// commit. Before -> the observation never lands and re-reporting reports
/// `Landed`; after -> `(observation_count, epoch) == (1, 1)` and the repeat
/// reports `Unchanged`. Both converge to the no-crash baseline.
#[test]
fn crash_capacity_report_before_and_after_commit() {
    let baseline = kit::adapter_fixture("crash-cap-report-baseline");
    let baseline_view = kit::planner_view(&baseline.store, kit::PLANNING_INSTANT_UNIX_MS);
    let baseline_revision = baseline
        .store
        .control_snapshot("alice", &kit::key())
        .unwrap()
        .control_revision;
    drop(baseline);

    for fault in ["before", "after"] {
        let dir = kit::TestDir::new(&format!("crash-cap-report-{fault}"));
        let mut worker = spawn_crash_worker("crash_worker_capacity", &dir, "report", fault);
        let status = worker.child().wait().unwrap();
        assert_eq!(status.code(), Some(87), "report:{fault} exit code");

        let store = kit::open_store(&dir, &kit::identity(kit::SEED));
        let authority = kit::identity(kit::SEED);
        let generation = kit::committed_generation(&store);
        let state = store.capacity_state("alice", &kit::key()).unwrap();
        if fault == "before" {
            assert_eq!(
                state.observation_count, 0,
                "report:before must commit no observation"
            );
            let outcome = store
                .capacity_transition(
                    "alice",
                    &kit::report_transition(
                        &authority,
                        kit::scanned_observation(kit::NODE_A, kit::INC_A, generation),
                    ),
                )
                .unwrap()
                .outcome;
            assert_eq!(
                outcome,
                CapacityTransitionOutcome::Landed as i32,
                "the re-issued report must land"
            );
        } else {
            assert_eq!(
                (state.observation_count, state.observation_epoch),
                (1, 1),
                "report:after must leave the observation durable"
            );
            let outcome = store
                .capacity_transition(
                    "alice",
                    &kit::report_transition(
                        &authority,
                        kit::scanned_observation(kit::NODE_A, kit::INC_A, generation),
                    ),
                )
                .unwrap()
                .outcome;
            assert_eq!(
                outcome,
                CapacityTransitionOutcome::Unchanged as i32,
                "the exact repeat must report Unchanged"
            );
        }
        store
            .capacity_transition(
                "alice",
                &kit::report_transition(
                    &authority,
                    kit::silent_observation(kit::NODE_B, kit::INC_B, generation),
                ),
            )
            .unwrap();
        let view = kit::planner_view(&store, kit::PLANNING_INSTANT_UNIX_MS);
        assert_eq!(view, baseline_view, "report:{fault} planner view");
        let final_revision = store
            .control_snapshot("alice", &kit::key())
            .unwrap()
            .control_revision;
        assert_eq!(
            final_revision, baseline_revision,
            "report:{fault} final control revision"
        );
        drop(store);
        drop(dir);
    }
}

// ---- import-recovery family -------------------------------------------------

/// Worker: stage a begin + two chunks, arm, then Recover as bob.
#[test]
fn crash_worker_recover() {
    if std::env::var_os(CRASH_ENV).is_none() {
        return;
    }
    let dir = kit::TestDir::from_env();
    let fault = parse_fault();
    let (store, authority) = kit::create_store(&dir);
    let legacy = kit::legacy_plane_with_cluster(&dir);
    let (retired, _request) = kit::retire(&store, &legacy, 1);
    let payload = kit::import_payload(&retired);
    let (chunk_bytes, chunk_count) = kit::plan_three_chunks(payload.len());
    let begin = store
        .begin_control_import(
            "alice",
            &kit::import_command(
                &authority,
                "begin",
                1,
                kit::WORKFLOW,
                kit::begin_action(&retired, &payload, chunk_bytes, chunk_count),
            ),
            &retired,
        )
        .unwrap();
    assert_eq!(begin.code, 0, "{}", begin.message);
    for (ordinal, revision) in [(0u32, 2u64), (1, 3)] {
        let decision = store
            .execute_control_import(
                "alice",
                &kit::import_command(
                    &authority,
                    &format!("chunk-{ordinal}"),
                    revision,
                    kit::WORKFLOW,
                    kit::chunk_action(&payload, chunk_bytes, ordinal),
                ),
            )
            .unwrap();
        assert_eq!(decision.code, 0, "{}", decision.message);
    }
    store.arm_exit_fault(fault);
    store
        .execute_control_import(
            "bob",
            &kit::import_command(
                &authority,
                "bob-recover",
                4,
                kit::WORKFLOW,
                kit::recover_action(),
            ),
        )
        .unwrap();
    fault_must_fire();
}

/// Crash matrix: exit immediately before/after the administrative recovery
/// commit. Before -> the workflow is still `Staging` with its reservation
/// held and no recovery decision recorded; after -> `Recovered` with the
/// reservation released. Both accept the exact recovery retry (stored
/// decision, control revision 5) and record a refusal for a later Commit
/// attempt. (A fresh import after recovery is covered by the slice-3b
/// SIGKILL recovery matrix, which re-runs the whole import.)
#[test]
fn crash_recover_before_and_after_commit() {
    for fault in ["before", "after"] {
        let dir = kit::TestDir::new(&format!("crash-recover-{fault}"));
        let mut worker = spawn_crash_worker("crash_worker_recover", &dir, "recover", fault);
        let status = worker.child().wait().unwrap();
        assert_eq!(status.code(), Some(87), "recover:{fault} exit code");

        let store = kit::open_store(&dir, &kit::identity(kit::SEED));
        let authority = kit::identity(kit::SEED);
        let workflow = store
            .control_import_workflow("bob", &kit::key(), kit::WORKFLOW)
            .unwrap();
        if fault == "before" {
            assert_eq!(
                workflow.phase,
                ControlImportPhase::Staging as i32,
                "recover:before keeps the workflow staging"
            );
            assert!(
                workflow.reserved_bytes > 0,
                "recover:before keeps the reservation held"
            );
            assert_eq!(
                store
                    .control_import_decision("bob", &kit::key(), b"bob-recover")
                    .err()
                    .unwrap()
                    .code(),
                Code::NotFound,
                "recover:before records no recovery decision"
            );
        } else {
            assert_eq!(
                workflow.phase,
                ControlImportPhase::Recovered as i32,
                "recover:after leaves the workflow recovered"
            );
            assert_eq!(
                workflow.reserved_bytes, 0,
                "recover:after releases the reservation"
            );
        }
        // The exact recovery retry is a normal idempotent command: it lands
        // (before) or replays the stored decision (after).
        let decision = store
            .execute_control_import(
                "bob",
                &kit::import_command(
                    &authority,
                    "bob-recover",
                    4,
                    kit::WORKFLOW,
                    kit::recover_action(),
                ),
            )
            .unwrap();
        assert_eq!(decision.code, 0, "recover:{fault}: {}", decision.message);
        assert_eq!(
            decision.control_revision, 5,
            "recover:{fault} committed recovery revision"
        );
        assert_eq!(
            store
                .control_import_workflow("bob", &kit::key(), kit::WORKFLOW)
                .unwrap()
                .phase,
            ControlImportPhase::Recovered as i32
        );
        // A Commit attempt against the recovered workflow records a refusal
        // and cannot resurrect the staging workflow.
        let commit = store
            .execute_control_import(
                "alice",
                &kit::import_command(&authority, "commit", 5, kit::WORKFLOW, kit::commit_action()),
            )
            .unwrap();
        assert_ne!(
            commit.code, 0,
            "recover:{fault}: a commit after recovery must record a refusal"
        );
        assert_eq!(
            store
                .control_import_workflow("bob", &kit::key(), kit::WORKFLOW)
                .unwrap()
                .phase,
            ControlImportPhase::Recovered as i32,
            "recover:{fault}: the refusal must not change the terminal workflow"
        );
        drop(store);
        // Reopen proves the recorded state is durable, not in-memory.
        let reopened = kit::open_store(&dir, &kit::identity(kit::SEED));
        assert_eq!(
            reopened
                .control_import_workflow("bob", &kit::key(), kit::WORKFLOW)
                .unwrap()
                .phase,
            ControlImportPhase::Recovered as i32
        );
        drop(reopened);
        drop(dir);
    }
}

// ---- managed-catalog family -------------------------------------------------

fn managed_policy() -> AccessPolicy {
    let alice = CollectionGrant {
        principal: "alice".into(),
        workspace: "workspace-a".into(),
        collection: "books".into(),
        actions: vec![AccessAction::Admin as i32, AccessAction::Ingest as i32],
        ..Default::default()
    };
    AccessPolicy {
        format_version: 1,
        revision: 1,
        resources: vec![CollectionResource {
            workspace: "workspace-a".into(),
            collection: "books".into(),
        }],
        grants: vec![alice],
    }
}

fn managed_limits() -> SourceAuthorityLimits {
    SourceAuthorityLimits {
        max_owners: 16,
        max_decisions: 32,
        max_payload_bytes: 1 << 20,
        max_command_bytes: 64 << 10,
    }
}

fn managed_resource() -> SourceResourceBinding {
    SourceResourceBinding {
        format_version: 1,
        workspace: "workspace-a".into(),
        collection: "books".into(),
    }
}

fn managed_owner_key() -> LogicalSourceOwner {
    LogicalSourceOwner {
        workspace: "workspace-a".into(),
        collection: "books".into(),
        owner_id: b"phone-owner".to_vec(),
    }
}

fn managed_write(document_key: &[u8], operation_id: &[u8]) -> AcceptDocumentRequest {
    AcceptDocumentRequest {
        contract_version: 1,
        document_key: document_key.to_vec(),
        operation_id: operation_id.to_vec(),
        expected_version: Some(0),
        mutation: Some(Mutation::Source(ProtobufSource {
            descriptor_set: include_bytes!("fixtures/protobuf-semantics/descriptor.bin").to_vec(),
            message_type: "semantics.Doc".into(),
            payload: vec![8, 7],
        })),
        ..Default::default()
    }
}

fn managed_prepare_command(
    identity: &SourceAuthorityIdentity,
    history_id: Vec<u8>,
) -> SourceAuthorityCommand {
    SourceAuthorityCommand {
        format_version: 1,
        authority: Some(identity.clone()),
        key: Some(managed_owner_key()),
        command_id: b"prepare-owner".to_vec(),
        expected_control_revision: 1,
        expected_policy_revision: 1,
        expected_ownership_generation: 0,
        action: Some(Action::Prepare(PrepareSourceOwner {
            workflow_id: b"installation-one".to_vec(),
            target: Some(SourceStorageTarget {
                node_id: "server-a".into(),
                storage_incarnation: vec![41; 16],
                history_id,
                residency: SourceResidency::Server as i32,
                resident_device_id: String::new(),
            }),
        })),
    }
}

fn managed_confirm_command(
    identity: &SourceAuthorityIdentity,
    control_revision: u64,
    completion: SourceOwnerCompletion,
) -> SourceAuthorityCommand {
    SourceAuthorityCommand {
        format_version: 1,
        authority: Some(identity.clone()),
        key: Some(managed_owner_key()),
        command_id: b"confirm-ready".to_vec(),
        expected_control_revision: control_revision,
        expected_policy_revision: 1,
        expected_ownership_generation: 1,
        action: Some(Action::ConfirmReady(ConfirmSourceOwnerReady {
            workflow_id: b"installation-one".to_vec(),
            completion: Some(completion),
        })),
    }
}

fn managed_activate_command(
    identity: &SourceAuthorityIdentity,
    control_revision: u64,
) -> SourceAuthorityCommand {
    SourceAuthorityCommand {
        format_version: 1,
        authority: Some(identity.clone()),
        key: Some(managed_owner_key()),
        command_id: b"activate".to_vec(),
        expected_control_revision: control_revision,
        expected_policy_revision: 1,
        expected_ownership_generation: 1,
        action: Some(Action::Activate(
            pipestream_search::pb::storage::ActivateSourceOwner {
                workflow_id: b"installation-one".to_vec(),
            },
        )),
    }
}

/// Worker: catalog create/accept + authority create/prepare, then either an
/// armed bind or the full ready+activate sequence with an armed activate.
#[test]
fn crash_worker_managed() {
    if std::env::var_os(CRASH_ENV).is_none() {
        return;
    }
    let dir = kit::TestDir::from_env();
    let family = std::env::var(CRASH_FAMILY_ENV).unwrap();
    let fault = parse_fault();
    let local: Arc<dyn Authorizer> = Arc::new(PolicyAuthority::new(managed_policy()).unwrap());
    let admin =
        AccessPermit::acquire(local.clone(), "alice", "books", AccessAction::Admin).unwrap();
    let ingest = AccessPermit::acquire(local, "alice", "books", AccessAction::Ingest).unwrap();
    let catalog_path = dir.path().join("source.redb");
    let mut catalog =
        AccessControlledCatalog::create(&catalog_path, &managed_resource(), &admin).unwrap();
    let receipt = catalog
        .accept(&ingest, &managed_write(b"document-one", b"accept-one"))
        .unwrap();
    let identity = kit::identity(kit::SEED);
    let authority = SourceAuthorityStore::create(
        &dir.path().join("authority.redb"),
        &identity,
        &managed_policy(),
        &managed_limits(),
    )
    .unwrap();
    let preparation = authority
        .execute(
            "alice",
            &managed_prepare_command(&identity, receipt.history_id),
        )
        .unwrap()
        .owner
        .unwrap();
    match family.as_str() {
        "bind" => {
            let admission = authority.admission("alice").unwrap();
            catalog.arm_bind_exit_fault(fault);
            catalog
                .bind_prepared_owner(&admission, &authority, &preparation, 1 << 20)
                .unwrap();
            fault_must_fire();
        }
        "activate" => {
            let mut managed = catalog
                .bind_prepared_owner(
                    &authority.admission("alice").unwrap(),
                    &authority,
                    &preparation,
                    1 << 20,
                )
                .unwrap();
            let completion = managed.completion().unwrap().completion().clone();
            managed
                .confirm_ready("alice", &managed_confirm_command(&identity, 2, completion))
                .unwrap();
            // No admission guard may span this control command.
            authority
                .execute("alice", &managed_activate_command(&identity, 3))
                .unwrap();
            managed.arm_activate_exit_fault(fault);
            let admission = authority.admission("alice").unwrap();
            managed.activate(&admission).unwrap();
            fault_must_fire();
        }
        other => panic!("unknown managed crash family {other}"),
    }
}

/// The committed binding the managed window asserts, format 1, bound at the
/// catalog's first accepted sequence.
fn expected_binding(
    identity: &SourceAuthorityIdentity,
    preparation: PreparedSourceOwner,
) -> SourceManagedBinding {
    SourceManagedBinding {
        format_version: 1,
        authority: Some(identity.clone()),
        preparation: Some(preparation),
        bound_at_sequence: 1,
    }
}

/// Crash matrix: the managed-catalog bind and activate fences. Windows
/// (mirrors `managed/tests.rs`'s abrupt exit tests and
/// `docs/source-owner-admission.md`):
/// - bind before -> the catalog is still format 8, opened through
///   `AccessControlledCatalog::open`;
/// - bind after -> a committed format-9 binding recovered through
///   `PreparedManagedCatalog::recover`;
/// - activate before -> still format 9 prepared (the authority owner is
///   ACTIVE on the control side), recovered and activatable;
/// - activate after -> format 10, recovered directly through
///   `ActiveManagedCatalog::recover`.
/// All four recovered catalogs serve the next write at sequence 2.
#[test]
fn crash_managed_bind_and_activate_boundaries() {
    for family in ["bind", "activate"] {
        for fault in ["before", "after"] {
            let dir = kit::TestDir::new(&format!("crash-managed-{family}-{fault}"));
            let mut worker = spawn_crash_worker("crash_worker_managed", &dir, family, fault);
            let status = worker.child().wait().unwrap();
            assert_eq!(status.code(), Some(87), "{family}:{fault} exit code");

            let identity = kit::identity(kit::SEED);
            let authority = kit::open_store(&dir, &identity);
            let catalog_path = dir.path().join("source.redb");
            let local: Arc<dyn Authorizer> =
                Arc::new(PolicyAuthority::new(managed_policy()).unwrap());
            let admin =
                AccessPermit::acquire(local, "alice", "books", AccessAction::Admin).unwrap();

            if family == "bind" && fault == "before" {
                // Format 8: untouched access-controlled catalog.
                let owner = authority.owner("alice", &managed_owner_key()).unwrap();
                assert_eq!(
                    owner.phase,
                    PreparedSourceOwnerPhase::Prepared as i32,
                    "bind:before leaves the owner unbound on the authority"
                );
                let controlled =
                    AccessControlledCatalog::open(&catalog_path, &managed_resource(), &admin)
                        .unwrap();
                assert_eq!(controlled.resource_binding(), &managed_resource());
            } else if family == "bind" {
                // Format 9: committed binding recovered from the prepared
                // owner recorded on the authority.
                let preparation = authority.owner("alice", &managed_owner_key()).unwrap();
                let managed = PreparedManagedCatalog::recover(
                    &catalog_path,
                    &authority,
                    "alice",
                    &identity,
                    &preparation,
                )
                .unwrap();
                assert_eq!(
                    managed.binding("alice").unwrap(),
                    expected_binding(&identity, preparation)
                );
            } else {
                // The worker executed the activation command on the authority
                // before arming the catalog-side fault: the owner is ACTIVE
                // in both activate windows.
                let owner = authority.owner("alice", &managed_owner_key()).unwrap();
                assert_eq!(
                    owner.phase,
                    PreparedSourceOwnerPhase::Active as i32,
                    "activate:{fault} owner phase"
                );
                // The binding's preparation view is the owner at prepare time
                // (control revision 2, no readiness/activation yet).
                let preparation = PreparedSourceOwner {
                    phase: PreparedSourceOwnerPhase::Prepared as i32,
                    control_revision: 2,
                    last_command: Some(
                        pipestream_search::pb::storage::SourceAuthorityOperationKey {
                            command_id: b"prepare-owner".to_vec(),
                            ..owner.last_command.clone().unwrap()
                        },
                    ),
                    readiness: None,
                    activation: None,
                    ..owner.clone()
                };
                let binding = expected_binding(&identity, preparation);
                let admission = authority.admission("alice").unwrap();
                let active = if fault == "before" {
                    // Format 9: not yet activated. The active recovery
                    // refuses; the prepared recovery works and activates.
                    assert_eq!(
                        ActiveManagedCatalog::recover(
                            &catalog_path,
                            &authority,
                            &admission,
                            &binding
                        )
                        .err()
                        .unwrap()
                        .code(),
                        Code::FailedPrecondition,
                        "activate:before is not the activated format"
                    );
                    let managed = PreparedManagedCatalog::recover(
                        &catalog_path,
                        &authority,
                        "alice",
                        &identity,
                        binding.preparation.as_ref().unwrap(),
                    )
                    .unwrap();
                    managed.activate(&admission).unwrap()
                } else {
                    // Format 10: the activated source recovers directly.
                    ActiveManagedCatalog::recover(&catalog_path, &authority, &admission, &binding)
                        .unwrap()
                };
                let receipt = active
                    .accept(&admission, &managed_write(b"document-two", b"accept-two"))
                    .unwrap();
                assert_eq!(
                    receipt.accepted_sequence, 2,
                    "{family}:{fault} serves the next write"
                );
            }
            drop(authority);
            drop(dir);
        }
    }
}

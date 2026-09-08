//! Raft-host regression target (slice 3c;
//! `docs/control-authority-test-harness.md`).
//!
//! Independent reproductions of Astra's checkpoint review findings R1, R2
//! and R4 (host level), plus the ACTIVE and Recover happy paths through the
//! Raft host. Each test pins the REQUIRED behavior quoted in the review; at
//! the frozen checkpoint `331c18f` several FAIL — a failing `rN_...` test is
//! a minimized reproduction to hand to Fable, not a test bug. Assertions are
//! never weakened to make a finding pass.
//!
//! Run: `cargo test --test control_raft_regressions --features
//! raft,fault-injection -- --test-threads=1` (the target holds a
//! target-wide serial lock, so any `--test-threads` value is safe).
#![cfg(feature = "raft")]

mod control_adversarial;

use std::sync::Arc;
use std::time::Duration;

use control_adversarial::raft_kit::HostGuard;
use control_adversarial::{kit, raft_kit};
use pipestream_search::authorization::{AccessPermit, Authorizer, PolicyAuthority};
use pipestream_search::document_catalog::AccessControlledCatalog;
use pipestream_search::pb::storage::raft_proposal::Command;
use pipestream_search::pb::storage::{CapacityTransitionOutcome, SourceOwnerCompletion};
use pipestream_search::pb::storage::{
    ConfirmSourceOwnerReady, ControlImportPhase, PreparedSourceOwnerPhase, SourceResourceBinding,
};
use pipestream_search::pb::{
    accept_document_request::Mutation, AcceptDocumentRequest, AccessAction, ProtobufSource,
};
use pipestream_search::raft::{ControlRaft, ControlStateMachine};
use pipestream_search::source_authority::SourceAuthorityStore;
use tonic::Code;

const OWNER_WORKFLOW: &[u8] = b"installation-one";

/// Target-wide serialization for the parallel release gate. The tests share
/// one piece of process-global state: the raft kit's fixed `NODE_ADDR` —
/// every host in this target bootstraps on `127.0.0.1:9917`, so two
/// concurrent tests would collide on the bind. Every test holds this lock
/// for its whole body (the suite is small; the serialized wall time is
/// unchanged in practice). The guard is taken before the first `.await` and
/// is `Send`, so it is safe inside `#[tokio::test(flavor = "multi_thread")]`.
/// Panics are expected here (the failing `rN` tests are the reproductions),
/// so the lock is re-acquired across poisoning; mutual exclusion is what
/// matters, not mutex state.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// One accepted document, mirroring `src/document_catalog/managed/tests.rs`:
/// the history id it returns is what the managed bridge binds to.
fn catalog_write() -> AcceptDocumentRequest {
    AcceptDocumentRequest {
        contract_version: 1,
        document_key: b"document-one".to_vec(),
        operation_id: b"accept-one".to_vec(),
        expected_version: Some(0),
        mutation: Some(Mutation::Source(ProtobufSource {
            descriptor_set: include_bytes!("fixtures/protobuf-semantics/descriptor.bin").to_vec(),
            message_type: "semantics.Doc".into(),
            payload: vec![8, 7],
        })),
        ..Default::default()
    }
}

fn catalog_fixture(dir: &kit::TestDir) -> (AccessControlledCatalog, Vec<u8>) {
    // The catalog needs an Ingest grant in addition to the kit policy's
    // Admin-only grant (mirrors `src/document_catalog/managed/tests.rs`,
    // which pushes Ingest into alice's grant list).
    let mut policy = kit::policy();
    for grant in &mut policy.grants {
        if grant.principal == "alice" {
            grant.actions.push(AccessAction::Ingest as i32);
        }
    }
    let authorizer: Arc<dyn Authorizer> = Arc::new(PolicyAuthority::new(policy).unwrap());
    let admin = AccessPermit::acquire(
        authorizer.clone(),
        "alice",
        kit::COLLECTION,
        AccessAction::Admin,
    )
    .unwrap();
    let ingest =
        AccessPermit::acquire(authorizer, "alice", kit::COLLECTION, AccessAction::Ingest).unwrap();
    let catalog = AccessControlledCatalog::create(
        &dir.path().join("catalog.redb"),
        &SourceResourceBinding {
            format_version: 1,
            workspace: kit::WORKSPACE.into(),
            collection: kit::COLLECTION.into(),
        },
        &admin,
    )
    .unwrap();
    let receipt = catalog.accept(&ingest, &catalog_write()).unwrap();
    (catalog, receipt.history_id)
}

fn confirm_command(
    authority: &pipestream_search::pb::storage::SourceAuthorityIdentity,
    control_revision: u64,
    completion: SourceOwnerCompletion,
) -> pipestream_search::pb::storage::SourceAuthorityCommand {
    kit::source_command(
        authority,
        &kit::owner_key(),
        "confirm-ready",
        control_revision,
        1,
        1,
        pipestream_search::pb::storage::source_authority_command::Action::ConfirmReady(
            ConfirmSourceOwnerReady {
                workflow_id: OWNER_WORKFLOW.to_vec(),
                completion: Some(completion),
            },
        ),
    )
}

/// R1 (review): "propose is public and calls only validate_proposal ...
/// accepts raw Begin and ConfirmReady envelopes without the
/// retirement/binding checks." REQUIRED: every public proposal entry point
/// refuses a Begin without the retired holder. At 331c18f the raw paths
/// apply the workflow: both workflow ids below exist with no holder ever
/// presented. That is the reproduction.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r1_raw_propose_begin_without_holder_is_refused() {
    let _serial = serial();
    let dir = kit::TestDir::new("r1-raw-begin");
    let guard = HostGuard::new(raft_kit::bootstrap_host(&dir).await);
    let store = guard.store().unwrap();
    let legacy = kit::legacy_plane(&dir, 2);
    let (retired, _request) = kit::retire(&store, &legacy, 1);
    let payload = kit::import_payload(&retired);
    let (chunk_bytes, chunk_count) = kit::plan_three_chunks(payload.len());
    let begin = |id: &str, workflow: &[u8]| {
        kit::import_command(
            &kit::identity(kit::SEED),
            id,
            1,
            workflow,
            kit::begin_action(&retired, &payload, chunk_bytes, chunk_count),
        )
    };

    // Positive control: the admitted path with the holder works.
    let admitted = guard
        .propose_import("alice", &begin("begin-held", kit::WORKFLOW), Some(&retired))
        .await
        .unwrap();
    assert_eq!(admitted.code, 0, "{}", admitted.message);
    // The positive control staged a workflow; recover it so the raw leg has
    // clean ids.
    let recover = kit::import_command(
        &kit::identity(kit::SEED),
        "recover-held",
        2,
        kit::WORKFLOW,
        kit::recover_action(),
    );
    let recovered = guard.propose_import("bob", &recover, None).await.unwrap();
    assert_eq!(recovered.code, 0, "{}", recovered.message);

    let mut findings: Vec<String> = Vec::new();
    // host.propose: raw envelope, no holder.
    let raw_propose = begin("begin-raw-propose", b"import-raw-a");
    let outcome = guard
        .propose(raft_kit::raw_proposal(Command::Import(raw_propose)))
        .await;
    if outcome.is_ok() {
        findings.push("host.propose applied a raw Begin without the retired holder".to_string());
    }
    // raft().client_write: the exposed raft client-write surface.
    let raw_client = begin("begin-raw-client", b"import-raw-b");
    let client_outcome = guard
        .raft()
        .client_write(raft_kit::raw_proposal(Command::Import(raw_client)))
        .await;
    if client_outcome.is_ok() {
        findings
            .push("raft().client_write applied a raw Begin without the retired holder".to_string());
    }

    // Evidence: the workflows exist with no holder ever presented.
    for workflow in [b"import-raw-a".as_slice(), b"import-raw-b".as_slice()] {
        match store.control_import_workflow("alice", &kit::key(), workflow) {
            Ok(view) => findings.push(format!(
                "workflow {workflow:?} staged without a holder (phase {})",
                view.phase
            )),
            Err(_) => {}
        }
    }
    assert!(
        findings.is_empty(),
        "R1 REQUIRED refusal of holder-free Begin through every public \
         proposal entry point; observed:\n  - {}",
        findings.join("\n  - ")
    );
    guard.shutdown().await;
}

/// R1: raw ConfirmReady without the managed binding must refuse. REQUIRED
/// per the review; at 331c18f the replay path admits the envelope and the
/// owner turns READY without any binding proof.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r1_raw_propose_confirm_ready_without_binding_is_refused() {
    let _serial = serial();
    let dir = kit::TestDir::new("r1-raw-confirm");
    let guard = HostGuard::new(raft_kit::bootstrap_host(&dir).await);
    let authority = kit::identity(kit::SEED);
    let prepare = kit::source_command(
        &authority,
        &kit::owner_key(),
        "prepare-owner",
        1,
        1,
        0,
        kit::prepare_action(OWNER_WORKFLOW),
    );
    let decision = guard.propose_command("alice", &prepare).await.unwrap();
    assert_eq!(decision.code, 0, "{}", decision.message);

    let confirm = kit::source_command(
        &authority,
        &kit::owner_key(),
        "confirm-raw",
        2,
        1,
        1,
        kit::confirm_ready_action(OWNER_WORKFLOW),
    );
    let mut findings: Vec<String> = Vec::new();
    let outcome = guard
        .propose(raft_kit::raw_proposal(Command::Control(confirm.clone())))
        .await;
    if outcome.is_ok() {
        findings.push("host.propose accepted a raw ConfirmReady without the binding".to_string());
    }
    let client_outcome = guard
        .raft()
        .client_write(raft_kit::raw_proposal(Command::Control(confirm)))
        .await;
    if client_outcome.is_ok() {
        findings.push(
            "raft().client_write accepted a raw ConfirmReady without the binding".to_string(),
        );
    }
    // Evidence: the owner is READY although no binding was ever presented.
    let store = guard.store().unwrap();
    if let Ok(owner) = store.owner("alice", &kit::owner_key()) {
        findings.push(format!(
            "owner phase is {:?} without any managed binding",
            owner.phase
        ));
    }
    assert!(
        findings.is_empty(),
        "R1 REQUIRED refusal of ConfirmReady without the managed binding; \
         observed:\n  - {}",
        findings.join("\n  - ")
    );
    guard.shutdown().await;
}

/// R1: "A handle advertised for reads by host.store() therefore exposes
/// mutation APIs that do not require a committed log entry." REQUIRED:
/// replay/direct mutation surfaces on a hosted read handle refuse (or are
/// compile-time inaccessible). At 331c18f the replay surfaces are public and
/// apply, so this test collects every surface it can reach and reports all
/// of them in one run.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r1_read_handle_cannot_mutate() {
    let _serial = serial();
    let dir = kit::TestDir::new("r1-read-handle");
    let guard = HostGuard::new(raft_kit::bootstrap_host(&dir).await);
    let authority = kit::identity(kit::SEED);
    let store = guard.store().unwrap();

    // The direct paths already refuse (a positive guard, expected to pass).
    let prepare_guard = kit::source_command(
        &authority,
        &kit::owner_key(),
        "prepare-owner",
        1,
        1,
        0,
        kit::prepare_action(OWNER_WORKFLOW),
    );
    assert_eq!(
        store.execute("alice", &prepare_guard).err().unwrap().code(),
        Code::FailedPrecondition,
        "the direct execute guard changed"
    );
    // The refused execute must not have moved the control revision.
    let mut findings: Vec<String> = Vec::new();

    // Retire and build applied state through the replay import surface so
    // the capacity replay surface has something to bind to. (Retirement
    // comes first: it pins revision 1 and consumes none itself.)
    let legacy = kit::legacy_plane(&dir, 2);
    let (retired, _request) = kit::retire(&store, &legacy, 1);
    let payload = kit::import_payload(&retired);
    let (chunk_bytes, chunk_count) = kit::plan_three_chunks(payload.len());
    let begin = kit::import_command(
        &authority,
        "replay-begin",
        1,
        kit::WORKFLOW,
        kit::begin_action(&retired, &payload, chunk_bytes, chunk_count),
    );
    match store.replay_control_import("alice", &begin) {
        Ok(decision) if decision.code == 0 => {
            findings.push("replay_control_import began a workflow without a log entry".to_string())
        }
        Ok(_) => {}
        Err(_) => {}
    }
    for ordinal in 0..chunk_count {
        let chunk = kit::import_command(
            &authority,
            &format!("replay-chunk-{ordinal}"),
            2 + ordinal as u64,
            kit::WORKFLOW,
            kit::chunk_action(&payload, chunk_bytes, ordinal),
        );
        if let Ok(decision) = store.replay_control_import("alice", &chunk) {
            if decision.code == 0 {
                findings
                    .push("replay_control_import staged a chunk without a log entry".to_string());
            }
        }
    }
    let commit = kit::import_command(
        &authority,
        "replay-commit",
        2 + chunk_count as u64,
        kit::WORKFLOW,
        kit::commit_action(),
    );
    if let Ok(decision) = store.replay_control_import("alice", &commit) {
        if decision.code == 0 {
            findings
                .push("replay_control_import committed an import without a log entry".to_string());
        }
    }
    // The imported resource makes the capacity replay surface meaningful.
    let configure = kit::configure_command(
        &authority,
        "replay-cfg",
        6,
        1,
        kit::capacity_configuration(1_000_000_000_000),
    );
    match store.replay_capacity_configure("alice", &configure) {
        Ok(decision) if decision.code == 0 => findings
            .push("replay_capacity_configure configured capacity without a log entry".to_string()),
        Ok(_) => {}
        Err(_) => {}
    }
    let register = kit::register_transition(&authority, kit::NODE_A, kit::INC_A);
    match store.replay_capacity_transition("alice", &register) {
        Ok(_) => findings.push(
            "replay_capacity_transition registered a reporter without a log entry".to_string(),
        ),
        Err(_) => {}
    }

    // R1 REQUIRED: replay_command on a hosted read handle refuses. The import
    // leg above consumed revisions 1..=6, so the next command is revision 7.
    let replay_prepare = kit::source_command(
        &authority,
        &kit::owner_key(),
        "replay-prepare",
        7,
        7,
        0,
        kit::prepare_action(OWNER_WORKFLOW),
    );
    match store.replay_command("alice", &replay_prepare) {
        Ok(_) => findings.push("replay_command applied a Prepare without a log entry".to_string()),
        Err(_) => {}
    }

    // Evidence that the mutations landed on the advertised read handle.
    if store.owner("alice", &kit::owner_key()).is_ok() {
        findings.push(
            "the read handle now shows a PREPARED owner written with no log entry".to_string(),
        );
    }
    if store
        .control_import_workflow("alice", &kit::key(), kit::WORKFLOW)
        .is_ok()
    {
        findings.push(
            "the read handle now shows a staged workflow written with no log entry".to_string(),
        );
    }
    if store.capacity_state("alice", &kit::key()).is_ok() {
        findings
            .push("the read handle now shows capacity state written with no log entry".to_string());
    }
    assert!(
        findings.is_empty(),
        "R1 REQUIRED that a hosted read handle cannot mutate; observed:\n  - {}",
        findings.join("\n  - ")
    );
    guard.shutdown().await;
}

/// R1 positive coverage: the trusted host entry points keep working — the
/// same command families that the raw paths accept holder-free succeed with
/// their holders here, so the R1 refusals cannot be read as "raft is off".
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r1_positive_replay_through_trusted_host() {
    let _serial = serial();
    let dir = kit::TestDir::new("r1-positive");
    let guard = HostGuard::new(raft_kit::bootstrap_host(&dir).await);
    let authority = kit::identity(kit::SEED);
    let prepare = kit::source_command(
        &authority,
        &kit::owner_key(),
        "prepare-owner",
        1,
        1,
        0,
        kit::prepare_action(OWNER_WORKFLOW),
    );
    assert_eq!(
        guard.propose_command("alice", &prepare).await.unwrap().code,
        0
    );

    // The prepare consumed control revision 1 and retirement consumes none,
    // so the store sits at revision 2 and both the retirement request and
    // the Begin must expect that.
    let store = guard.store().unwrap();
    let legacy = kit::legacy_plane(&dir, 2);
    let (retired, _request) = kit::retire(&store, &legacy, 2);
    let payload = kit::import_payload(&retired);
    let (chunk_bytes, chunk_count) = kit::plan_three_chunks(payload.len());
    let begin = kit::import_command(
        &authority,
        "begin",
        2,
        kit::WORKFLOW,
        kit::begin_action(&retired, &payload, chunk_bytes, chunk_count),
    );
    assert_eq!(
        guard
            .propose_import("alice", &begin, Some(&retired))
            .await
            .unwrap()
            .code,
        0
    );
    let _ = chunk_count;
    guard.shutdown().await;
}

/// R2 (review): "each consumed committed entry advances the durable applied
/// position, including exact retries ... Make the position update
/// transactional with the retry lookup." REQUIRED: the snapshot meta's
/// applied position equals the latest consumed index even when the latest
/// entry is an exact retry. At 331c18f the retry path returns the stored
/// decision before `write_pending_applied`, so a snapshot taken right after
/// an exact retry binds the PREVIOUS entry's position — this test fails
/// there, and that failure is the reproduction. (A fresh entry after the
/// retry would overwrite the lagging position, which is why the retry is
/// last; and raft's `initialize` commits a membership entry at index 1, so
/// after N proposals the applied index is N+1.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r2_exact_retry_advances_durable_applied_position() {
    let _serial = serial();
    let dir = kit::TestDir::new("r2-control-retry");
    let guard = HostGuard::new(raft_kit::bootstrap_host(&dir).await);
    let authority = kit::identity(kit::SEED);
    let prepare = kit::source_command(
        &authority,
        &kit::owner_key(),
        "prepare-owner",
        1,
        1,
        0,
        kit::prepare_action(OWNER_WORKFLOW),
    );
    let first = guard.propose_command("alice", &prepare).await.unwrap();
    assert_eq!(
        first.control_revision, 2,
        "the decision reports the post-commit revision"
    );
    raft_kit::wait_applied(&guard, 2).await;

    // Changed-content retry of the same id: a consumed refusal entry.
    let changed = kit::source_command(
        &authority,
        &kit::owner_key(),
        "prepare-owner",
        2,
        2,
        0,
        kit::prepare_action(b"another-workflow"),
    );
    let refusal = guard
        .propose_command("alice", &changed)
        .await
        .err()
        .expect("a changed-content retry must refuse");
    assert_eq!(refusal.code(), Code::FailedPrecondition);
    raft_kit::wait_applied(&guard, 3).await;

    // The exact retry comes LAST: identical bytes, a new consumed index, the
    // stored decision verbatim. R2 REQUIRED the snapshot to bind index 4.
    let retry = guard.propose_command("alice", &prepare).await.unwrap();
    assert_eq!(
        first, retry,
        "an exact retry must return the stored decision verbatim"
    );
    raft_kit::wait_applied(&guard, 4).await;

    let (_image, meta) = raft_kit::snapshot_image(&dir, &guard).await;
    let durable = meta
        .last_log_id
        .unwrap_or_else(|| panic!("snapshot meta carries no applied position"));
    assert_eq!(
        durable.index, 4,
        "R2 REQUIRED the durable applied position to include the exact retry \
         at index 4; the snapshot bound {durable:?} instead — the retry path \
         skipped write_pending_applied"
    );

    // Restart: the group recovers from the log (not the lagging snapshot)
    // and keeps serving at the same position.
    let dir_path = dir.path().to_path_buf();
    guard.shutdown().await;
    let guard = HostGuard::new(
        pipestream_search::raft::RaftHost::start(
            &dir_path,
            &authority,
            raft_kit::NODE_ID,
            &raft_kit::host_config(),
        )
        .await
        .unwrap(),
    );
    raft_kit::wait_leader(&guard).await;
    raft_kit::wait_applied(&guard, 4).await;
    let store = guard.store().unwrap();
    let owner = store.owner("alice", &kit::owner_key()).unwrap();
    assert_eq!(owner.phase, PreparedSourceOwnerPhase::Prepared as i32);
    drop(store);
    guard.shutdown().await;
}

/// R2, import family: an exact Chunk retry and a changed-content Chunk
/// retry both consume committed entries; the durable position must include
/// them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r2_import_chunk_retry_advances_durable_applied_position() {
    let _serial = serial();
    let dir = kit::TestDir::new("r2-import-retry");
    let guard = HostGuard::new(raft_kit::bootstrap_host(&dir).await);
    let authority = kit::identity(kit::SEED);
    let store = guard.store().unwrap();
    let legacy = kit::legacy_plane(&dir, 2);
    let (retired, _request) = kit::retire(&store, &legacy, 1);
    let payload = kit::import_payload(&retired);
    let (chunk_bytes, chunk_count) = kit::plan_three_chunks(payload.len());
    let begin = kit::import_command(
        &authority,
        "begin",
        1,
        kit::WORKFLOW,
        kit::begin_action(&retired, &payload, chunk_bytes, chunk_count),
    );
    assert_eq!(
        guard
            .propose_import("alice", &begin, Some(&retired))
            .await
            .unwrap()
            .code,
        0
    );
    let chunk = |id: &str, ecr: u64, ordinal: u32| {
        kit::import_command(
            &authority,
            id,
            ecr,
            kit::WORKFLOW,
            kit::chunk_action(&payload, chunk_bytes, ordinal),
        )
    };
    let staged = guard
        .propose_import("alice", &chunk("chunk-0", 2, 0), None)
        .await
        .unwrap();
    assert_eq!(staged.code, 0, "{}", staged.message);
    raft_kit::wait_applied(&guard, 3).await;

    // Changed-content retry of the same id: the idempotency refusal. The
    // entry is still consumed.
    let conflict = guard
        .propose_import("alice", &chunk("chunk-0", 2, 1), None)
        .await
        .expect_err("a changed-content retry must refuse");
    assert_eq!(conflict.code(), Code::FailedPrecondition);
    raft_kit::wait_applied(&guard, 4).await;

    // The exact retry comes LAST: stored decision verbatim, a new consumed
    // index. R2 REQUIRED the snapshot to bind index 5.
    let retried = guard
        .propose_import("alice", &chunk("chunk-0", 2, 0), None)
        .await
        .unwrap();
    assert_eq!(staged, retried, "the exact retry changed the decision");
    raft_kit::wait_applied(&guard, 5).await;

    let (_image, meta) = raft_kit::snapshot_image(&dir, &guard).await;
    let durable = meta.last_log_id.unwrap();
    assert_eq!(
        durable.index, 5,
        "R2 REQUIRED the durable applied position to include the exact \
         chunk retry at index 5; the snapshot bound {durable:?} instead — \
         the retry path skipped write_pending_applied"
    );
    guard.shutdown().await;
    let _ = chunk_count;
}

/// R2, capacity family: an exact configure retry, a changed-content
/// configure refusal, and an unchanged observation transition (the review's
/// positive case: a no-op transition must still commit the position). All
/// three consume entries; the durable position must include them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r2_capacity_retries_and_observation_transition() {
    let _serial = serial();
    let dir = kit::TestDir::new("r2-capacity-retry");
    let guard = HostGuard::new(raft_kit::bootstrap_host(&dir).await);
    let authority = kit::identity(kit::SEED);
    let store = guard.store().unwrap();
    // `legacy_plane_with_cluster` drives cluster routes through its own
    // short-lived runtime, which panics inside an async runtime — build it
    // on a plain thread.
    let legacy = std::thread::scope(|scope| {
        scope
            .spawn(|| kit::legacy_plane_with_cluster(&dir))
            .join()
            .unwrap()
    });
    let (retired, _request) = kit::retire(&store, &legacy, 1);
    let payload = kit::import_payload_placement(&retired);
    let (chunk_bytes, chunk_count) = kit::plan_three_chunks(payload.len());
    let begin = kit::import_command(
        &authority,
        "begin",
        1,
        kit::WORKFLOW,
        kit::begin_action(&retired, &payload, chunk_bytes, chunk_count),
    );
    assert_eq!(
        guard
            .propose_import("alice", &begin, Some(&retired))
            .await
            .unwrap()
            .code,
        0
    );
    for ordinal in 0..chunk_count {
        let chunk = kit::import_command(
            &authority,
            &format!("chunk-{ordinal}"),
            2 + ordinal as u64,
            kit::WORKFLOW,
            kit::chunk_action(&payload, chunk_bytes, ordinal),
        );
        assert_eq!(
            guard
                .propose_import("alice", &chunk, None)
                .await
                .unwrap()
                .code,
            0
        );
    }
    let commit = kit::import_command(
        &authority,
        "commit",
        2 + chunk_count as u64,
        kit::WORKFLOW,
        kit::commit_action(),
    );
    assert_eq!(
        guard
            .propose_import("alice", &commit, None)
            .await
            .unwrap()
            .code,
        0
    );

    let configure = kit::configure_command(
        &authority,
        "cfg",
        6,
        1,
        kit::capacity_configuration(1_000_000_000_000),
    );
    let first = guard
        .propose_capacity_configure("alice", &configure)
        .await
        .unwrap();
    assert_eq!(first.code, 0, "{}", first.message);
    raft_kit::wait_applied(&guard, 7).await;

    // Changed-content retry of the same configure id: the idempotency
    // refusal, still a consumed entry.
    let conflict = guard
        .propose_capacity_configure(
            "alice",
            &kit::configure_command(
                &authority,
                "cfg",
                6,
                1,
                kit::capacity_configuration(100_000_000_000),
            ),
        )
        .await
        .expect_err("a changed-content configure retry must refuse");
    assert_eq!(conflict.code(), Code::FailedPrecondition);
    raft_kit::wait_applied(&guard, 8).await;

    for (node, incarnation) in [(kit::NODE_A, kit::INC_A), (kit::NODE_B, kit::INC_B)] {
        guard
            .propose_capacity_transition(
                "alice",
                &kit::register_transition(&authority, node, incarnation),
            )
            .await
            .unwrap();
    }
    let generation = kit::committed_generation(&store);
    let report = kit::report_transition(
        &authority,
        kit::scanned_observation(kit::NODE_A, kit::INC_A, generation),
    );
    let landed = guard
        .propose_capacity_transition("alice", &report)
        .await
        .unwrap();
    assert_eq!(landed.outcome, CapacityTransitionOutcome::Landed as i32);
    // The review's positive case: an unchanged transition is a committed
    // no-op that must still advance the durable position.
    let again = guard
        .propose_capacity_transition("alice", &report)
        .await
        .unwrap();
    assert_eq!(again.outcome, CapacityTransitionOutcome::Unchanged as i32);
    assert_eq!(again.observation_epoch, 1);
    raft_kit::wait_applied(&guard, 12).await;

    // The exact configure retry comes LAST: stored decision verbatim, a new
    // consumed index. R2 REQUIRED the snapshot to bind index 13.
    let retried = guard
        .propose_capacity_configure("alice", &configure)
        .await
        .unwrap();
    assert_eq!(
        first, retried,
        "the exact configure retry changed the decision"
    );
    raft_kit::wait_applied(&guard, 13).await;

    let (_image, meta) = raft_kit::snapshot_image(&dir, &guard).await;
    let durable = meta.last_log_id.unwrap();
    assert_eq!(
        durable.index, 13,
        "R2 REQUIRED the durable applied position to include the configure \
         retry at index 13 (after the refusal, both observation transitions \
         and the unchanged report); the snapshot bound {durable:?} instead — \
         the retry path skipped write_pending_applied"
    );
    guard.shutdown().await;
}

/// Build a real snapshot image + meta from a throwaway host that proposed
/// one Prepare, for the standalone-machine R4 tests.
async fn snapshot_with_prepare(
    name: &str,
) -> (
    kit::TestDir,
    Vec<u8>,
    pipestream_search::pb::storage::RaftSnapshotMeta,
) {
    let dir = kit::TestDir::new(name);
    let guard = HostGuard::new(raft_kit::bootstrap_host(&dir).await);
    let authority = kit::identity(kit::SEED);
    let prepare = kit::source_command(
        &authority,
        &kit::owner_key(),
        "prepare-owner",
        1,
        1,
        0,
        kit::prepare_action(OWNER_WORKFLOW),
    );
    assert_eq!(
        guard.propose_command("alice", &prepare).await.unwrap().code,
        0
    );
    let (image, meta) = raft_kit::snapshot_image(&dir, &guard).await;
    guard.shutdown().await;
    (dir, image, meta)
}

fn standalone_machine(name: &str) -> (kit::TestDir, ControlStateMachine, SourceAuthorityStore) {
    let dir = kit::TestDir::new(name);
    let store = SourceAuthorityStore::create(
        &dir.path().join("authority.redb"),
        &kit::identity(kit::SEED),
        &kit::policy(),
        &kit::limits(),
    )
    .unwrap();
    let machine =
        ControlStateMachine::new(store.clone(), &dir.path().join("raft-snapshots")).unwrap();
    (dir, machine, store)
}

/// R4 (review): "make refusal preserve the old usable handle ... At 331c18f
/// state_machine.rs:307-314 takes the live store out of the shared Option
/// before replace_from; on refusal the slot stays None." REQUIRED: a refused
/// install leaves the machine's handle slot serving reads (and the held
/// reader keeps working). At 331c18f the slot is None after the refusal, so
/// `shared_store()` reports unavailable — the reproduction.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r4_install_refusal_preserves_the_live_handle() {
    let _serial = serial();
    let (_image_dir, image, meta) = snapshot_with_prepare("r4-image-refusal").await;
    let (_dir, mut machine, store) = standalone_machine("r4-refusal");
    let reader = machine
        .shared_store()
        .read()
        .unwrap()
        .clone()
        .expect("fresh machine serves handles");
    let _ = store;

    let refused = raft_kit::install_image(&mut machine, &image, &meta).await;
    assert!(
        refused.is_err(),
        "the install must refuse: a reader is outstanding ({refused:?})"
    );

    // REQUIRED (R4): the refusal preserves the old usable handle.
    let slot = machine.shared_store().read().unwrap().clone();
    assert!(
        slot.is_some(),
        "R4 REQUIRED the refused install to preserve the live handle; the \
         shared slot is None — state_machine took the store out before \
         replace_from and the refusal path never put it back"
    );
    slot.unwrap()
        .control_snapshot("alice", &kit::key())
        .expect("the preserved handle must still serve reads");

    // The reader held across the refused install must still work (this part
    // should pass even at 331c18f; it pins the half of R4 that already holds).
    reader
        .control_snapshot("alice", &kit::key())
        .expect("the held reader must survive the refused install");
    drop(reader);

    // With no outstanding handles the same image installs cleanly.
    raft_kit::install_image(&mut machine, &image, &meta)
        .await
        .expect("a clean install must succeed");
}

/// R4: "define a stable host-owned indirection/read-guard API for snapshot
/// swaps ... Test subscription rebinding and managed owner access after a
/// successful install." REQUIRED: a `subscribe_applied` receiver taken
/// before the install observes revisions published after it (rebound to the
/// new database). At 331c18f the receiver stays attached to the retired
/// database and never fires again — the reproduction. Managed owner access
/// on the installed store must reflect the image.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r4_subscription_rebinding_after_install() {
    let _serial = serial();
    let (_image_dir, image, meta) = snapshot_with_prepare("r4-image-rebind").await;
    let (_dir, mut machine, store) = standalone_machine("r4-rebind");
    let mut rx = store.subscribe_applied();
    let applied_before = *rx.borrow();
    // The install needs exclusive ownership: keep the receiver (its watch
    // channel outlives the handle) but drop every store clone.
    drop(store);

    raft_kit::install_image(&mut machine, &image, &meta)
        .await
        .expect("clean install must succeed");

    // Managed owner access after the install reflects the image.
    let installed = machine
        .shared_store()
        .read()
        .unwrap()
        .clone()
        .expect("installed machine serves handles");
    let owner = installed
        .owner("alice", &kit::owner_key())
        .expect("the installed store must serve the image's owner");
    assert_eq!(owner.phase, PreparedSourceOwnerPhase::Prepared as i32);

    // Publish one more committed entry through the machine; the image's
    // applied position is the meta's index, so the next entry is index+1.
    use openraft::storage::RaftStateMachine;
    let authority = kit::identity(kit::SEED);
    let next_prepare = kit::source_command(
        &authority,
        &kit::owner_key(),
        "prepare-second",
        2,
        1,
        0,
        kit::prepare_action(b"installation-two"),
    );
    let base = meta.last_log_id.unwrap();
    let entry = openraft::Entry::<ControlRaft> {
        log_id: openraft::LogId::new(
            openraft::LeaderId::new(base.term, base.node_id),
            base.index + 1,
        ),
        payload: openraft::EntryPayload::Normal(raft_kit::raw_proposal(Command::Control(
            next_prepare,
        ))),
    };
    RaftStateMachine::apply(&mut machine, vec![entry])
        .await
        .unwrap();
    let after = *installed.subscribe_applied().borrow();
    assert!(
        after > applied_before,
        "the new entry did not advance the applied revision"
    );

    // REQUIRED (R4): the pre-install receiver must observe the post-install
    // revision — it must be rebound, not left on the retired database.
    let rebound = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if *rx.borrow() >= after {
                return true;
            }
            if rx.changed().await.is_err() {
                return false;
            }
        }
    })
    .await;
    assert!(
        rebound == Ok(true),
        "R4 REQUIRED subscription rebinding across a snapshot install; the \
         pre-install receiver never observed the new applied revision \
         {after} (it stays attached to the retired database)"
    );
}

/// ACTIVE happy path through the host: Prepare -> bind -> ConfirmReady ->
/// Activate, the write fence, and the fence's survival across a host
/// restart. Positive coverage; expected to pass at 331c18f.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn active_fence_through_the_host() {
    let _serial = serial();
    let dir = kit::TestDir::new("active-through-host");
    // The fence admission checks the store's policy for the write action, so
    // the hosted authority needs the Ingest grant the kit policy omits.
    let mut hosted_policy = kit::policy();
    for grant in &mut hosted_policy.grants {
        if grant.principal == "alice" {
            grant.actions.push(AccessAction::Ingest as i32);
        }
    }
    let guard = HostGuard::new(raft_kit::bootstrap_host_with(&dir, &hosted_policy).await);
    let authority = kit::identity(kit::SEED);
    let (catalog, history_id) = catalog_fixture(&dir);

    let prepare = kit::source_command(
        &authority,
        &kit::owner_key(),
        "prepare-owner",
        1,
        1,
        0,
        kit::prepare_action_history(OWNER_WORKFLOW, history_id),
    );
    let decision = guard.propose_command("alice", &prepare).await.unwrap();
    assert_eq!(decision.code, 0, "{}", decision.message);
    let preparation = decision.owner.clone().unwrap();

    let store = guard.store().unwrap();
    // The admission is a read guard on the store's admission lock; the raft
    // apply path takes that lock exclusively, so it must not span a propose.
    let managed = {
        let admission = store.admission("alice").unwrap();
        catalog
            .bind_prepared_owner(&admission, &store, &preparation, 1 << 20)
            .unwrap()
    };
    let verified = managed.completion().unwrap();
    assert_eq!(verified.completion().bound_at_sequence, 1);

    let confirm = confirm_command(&authority, 2, verified.completion().clone());
    // The completion borrow ends with this call; the guard is long gone.
    let confirmed = guard
        .propose_confirm_ready("alice", &confirm, &managed.completion().unwrap())
        .await
        .unwrap();
    assert_eq!(confirmed.code, 0, "{}", confirmed.message);

    let activate = kit::source_command(
        &authority,
        &kit::owner_key(),
        "activate-owner",
        3,
        1,
        1,
        kit::activate_action(OWNER_WORKFLOW),
    );
    let activated = guard.propose_command("alice", &activate).await.unwrap();
    assert_eq!(activated.code, 0, "{}", activated.message);

    // The committed fence.
    let owner = store.owner("alice", &kit::owner_key()).unwrap();
    assert_eq!(owner.phase, PreparedSourceOwnerPhase::Active as i32);
    assert_eq!(owner.ownership_generation, 1);
    {
        let admission = store.admission("alice").unwrap();
        admission
            .admit_write(&kit::owner_key(), 1, AccessAction::Ingest)
            .expect("the committed epoch admits");
        assert_eq!(
            admission
                .admit_write(&kit::owner_key(), 2, AccessAction::Ingest)
                .err()
                .unwrap()
                .code(),
            Code::FailedPrecondition,
            "an epoch past the committed fence must refuse"
        );
    }

    // The catalog side opens the activated source (format 10).
    {
        let admission = store.admission("alice").unwrap();
        let active = managed.activate(&admission).unwrap();
        let _ = active;
    }

    // The fence survives a host restart. Every clone of the hosted store
    // holds the redb file lock, so drop them all before reopening the group.
    guard.shutdown().await;
    // `managed.activate` consumed `managed` (and its store clone) above.
    drop(store);
    let guard = HostGuard::new(raft_kit::start_host(&dir).await);
    raft_kit::wait_leader(&guard).await;
    let store = guard.store().unwrap();
    let owner = store.owner("alice", &kit::owner_key()).unwrap();
    assert_eq!(owner.phase, PreparedSourceOwnerPhase::Active as i32);
    store
        .admission("alice")
        .unwrap()
        .admit_write(&kit::owner_key(), 1, AccessAction::Ingest)
        .expect("the fence survives a restart");
    guard.shutdown().await;
}

/// Recover happy path through the host: bob Recovers alice's staged import;
/// the row keeps alice as initiator while the terminal step names bob; a
/// later Commit is a recorded refusal. Positive coverage.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recover_through_the_host() {
    let _serial = serial();
    let dir = kit::TestDir::new("recover-through-host");
    let guard = HostGuard::new(raft_kit::bootstrap_host(&dir).await);
    let authority = kit::identity(kit::SEED);
    let store = guard.store().unwrap();
    let legacy = kit::legacy_plane(&dir, 2);
    let (retired, _request) = kit::retire(&store, &legacy, 1);
    let payload = kit::import_payload(&retired);
    let (chunk_bytes, chunk_count) = kit::plan_three_chunks(payload.len());
    let begin = kit::import_command(
        &authority,
        "begin",
        1,
        kit::WORKFLOW,
        kit::begin_action(&retired, &payload, chunk_bytes, chunk_count),
    );
    assert_eq!(
        guard
            .propose_import("alice", &begin, Some(&retired))
            .await
            .unwrap()
            .code,
        0
    );
    for ordinal in 0..2 {
        let chunk = kit::import_command(
            &authority,
            &format!("chunk-{ordinal}"),
            2 + ordinal as u64,
            kit::WORKFLOW,
            kit::chunk_action(&payload, chunk_bytes, ordinal),
        );
        assert_eq!(
            guard
                .propose_import("alice", &chunk, None)
                .await
                .unwrap()
                .code,
            0
        );
    }
    let recover = kit::import_command(
        &authority,
        "recover-1",
        4,
        kit::WORKFLOW,
        kit::recover_action(),
    );
    let recovered = guard.propose_import("bob", &recover, None).await.unwrap();
    assert_eq!(recovered.code, 0, "{}", recovered.message);

    let store = guard.store().unwrap();
    let view = store
        .control_import_workflow("alice", &kit::key(), kit::WORKFLOW)
        .unwrap();
    assert_eq!(view.phase, ControlImportPhase::Recovered as i32);
    assert_eq!(view.principal, "alice", "the row keeps the initiator");
    assert_eq!(
        view.terminal.unwrap().principal,
        "bob",
        "the terminal step names the recovering Admin"
    );

    let commit = kit::import_command(&authority, "commit", 5, kit::WORKFLOW, kit::commit_action());
    let refused = guard.propose_import("alice", &commit, None).await.unwrap();
    assert_eq!(
        refused.code,
        Code::FailedPrecondition as u32,
        "a Commit after Recover must be a recorded refusal: {}",
        refused.message
    );
    let _ = chunk_count;
    guard.shutdown().await;
}

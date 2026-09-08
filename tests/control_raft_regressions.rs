//! Raft-host regression target (slice 3c;
//! `docs/control-authority-test-harness.md`), re-expressed on the supported
//! surface after the R1 repair.
//!
//! Independent reproductions of Astra's checkpoint review findings R1, R2
//! and R4 (host level), plus the ACTIVE and Recover happy paths through the
//! Raft host. Each test pins the REQUIRED behavior quoted in the review. The
//! original runtime reproductions of R1 reached `RaftHost::propose`,
//! `RaftHost::raft` and the store's replay paths; those are private since
//! 1f4ea13, so the accessibility half of R1 is a set of `compile_fail`
//! doctests on the `pipestream_search::raft` module and the runtime half
//! here pins what the supported entry points refuse. R4 is exercised through
//! the transport (a two-member group and a raw registered peer), the only
//! install path a product caller can reach.
//!
//! Run: `cargo test --test control_raft_regressions --features
//! raft,fault-injection -- --test-threads=1` (the target holds a
//! target-wide serial lock, so any `--test-threads` value is safe).
#![cfg(all(feature = "raft", feature = "tls"))]

mod control_adversarial;

use std::sync::Arc;
use std::time::Duration;

use control_adversarial::raft_kit::{Cluster, HostGuard};
use control_adversarial::{kit, raft_kit};
use pipestream_search::authorization::{AccessPermit, Authorizer, PolicyAuthority};
use pipestream_search::document_catalog::AccessControlledCatalog;
use pipestream_search::pb::storage::{CapacityTransitionOutcome, SourceOwnerCompletion};
use pipestream_search::pb::storage::{
    ConfirmSourceOwnerReady, ControlImportPhase, PreparedSourceOwnerPhase, SourceResourceBinding,
};
use pipestream_search::pb::{
    accept_document_request::Mutation, AcceptDocumentRequest, AccessAction, ProtobufSource,
};
use tonic::Code;

const OWNER_WORKFLOW: &[u8] = b"installation-one";

/// Target-wide serialization for the parallel release gate. The single-node
/// tests share the raft kit's fixed `NODE_ADDR` — every `bootstrap_single`
/// host in this target binds `127.0.0.1:9917`, so two concurrent tests would
/// collide on the bind. Every test holds this lock for its whole body. The
/// guard is taken before the first `.await` and is `Send`, so it is safe
/// inside `#[tokio::test(flavor = "multi_thread")]`. Panics are possible
/// here, so the lock is re-acquired across poisoning; mutual exclusion is
/// what matters, not mutex state.
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
/// refuses a Begin without the retired holder and a ConfirmReady without
/// the binding. The raw entry points (`propose`, `raft().client_write`)
/// are private since 1f4ea13 — the `compile_fail` doctests on
/// `pipestream_search::raft` pin that — and the supported entry points
/// admit before they submit: this test pins those refusals and the absence
/// of any staged workflow or READY owner afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r1_holder_free_begin_and_binding_free_confirm_refuse_at_proposal() {
    let _serial = serial();
    let dir = kit::TestDir::new("r1-raw-begin");
    let guard = HostGuard::new(raft_kit::bootstrap_host(&dir).await);
    let store = guard.store().unwrap();
    let authority = kit::identity(kit::SEED);
    let legacy = kit::legacy_plane(&dir, 2);
    let (retired, _request) = kit::retire(&store, &legacy, 1);
    let payload = kit::import_payload(&retired);
    let (chunk_bytes, chunk_count) = kit::plan_three_chunks(payload.len());
    let begin = |id: &str, workflow: &[u8]| {
        kit::import_command(
            &authority,
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
    let recover = kit::import_command(
        &authority,
        "recover-held",
        2,
        kit::WORKFLOW,
        kit::recover_action(),
    );
    let recovered = guard.propose_import("bob", &recover, None).await.unwrap();
    assert_eq!(recovered.code, 0, "{}", recovered.message);

    let mut findings: Vec<String> = Vec::new();
    // The only public import entry: a Begin without the holder is refused
    // at proposal, before anything reaches the log.
    let position_before = raft_kit::durable_position(&guard);
    let entries_before = guard.log_entries().unwrap().len();
    let raw = begin("begin-raw", b"import-raw-a");
    match guard.propose_import("alice", &raw, None).await {
        Ok(decision) => findings.push(format!(
            "propose_import applied a holder-free Begin (code {})",
            decision.code
        )),
        Err(error) => {
            if !matches!(
                error.code(),
                Code::PermissionDenied | Code::FailedPrecondition
            ) {
                findings.push(format!(
                    "holder-free Begin refused with an unnamed code: {error}"
                ));
            }
        }
    }
    if store
        .control_import_workflow("alice", &kit::key(), b"import-raw-a")
        .is_ok()
    {
        findings.push("workflow import-raw-a was staged without a holder".to_string());
    }
    if raft_kit::durable_position(&guard) != position_before
        || guard.log_entries().unwrap().len() != entries_before
    {
        findings.push("a refused holder-free Begin consumed a log entry".to_string());
    }

    // ConfirmReady through the general path is refused at proposal; the
    // owner stays PREPARED with no binding ever presented.
    let prepare = kit::source_command(
        &authority,
        &kit::owner_key(),
        "prepare-owner",
        3,
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
        4,
        1,
        1,
        kit::confirm_ready_action(OWNER_WORKFLOW),
    );
    match guard.propose_command("alice", &confirm).await {
        Ok(decision) => findings.push(format!(
            "propose_command accepted a ConfirmReady without the binding (code {})",
            decision.code
        )),
        Err(error) => {
            if error.code() != Code::PermissionDenied {
                findings.push(format!(
                    "ConfirmReady refused with an unnamed code: {error}"
                ));
            }
        }
    }
    let owner = store.owner("alice", &kit::owner_key()).unwrap();
    if owner.phase != PreparedSourceOwnerPhase::Prepared as i32 {
        findings.push(format!(
            "owner phase is {} without any managed binding",
            owner.phase
        ));
    }
    assert!(
        findings.is_empty(),
        "R1 REQUIRED refusal of holder-free Begin and binding-free ConfirmReady \
         through every public proposal entry point; observed:\n  - {}",
        findings.join("\n  - ")
    );
    drop(store);
    guard.shutdown().await;
}

/// R1: "A handle advertised for reads by host.store() therefore exposes
/// mutation APIs that do not require a committed log entry." REQUIRED:
/// replay/direct mutation surfaces on a hosted read handle refuse (or are
/// compile-time inaccessible). The replay surfaces are crate-private since
/// 1f4ea13 (`compile_fail` doctests); every direct command path that is
/// still public refuses on a hosted store by name, and this test collects
/// every surface it can reach and reports all of them in one run.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r1_hosted_read_handle_refuses_every_direct_mutation() {
    let _serial = serial();
    let dir = kit::TestDir::new("r1-read-handle");
    let guard = HostGuard::new(raft_kit::bootstrap_host(&dir).await);
    let authority = kit::identity(kit::SEED);
    let store = guard.store().unwrap();
    // The bootstrap membership entry applies a tick after leadership.
    raft_kit::wait_applied(&guard, 1).await;
    let position_before = raft_kit::durable_position(&guard);
    let entries_before = guard.log_entries().unwrap().len();
    let mut findings: Vec<String> = Vec::new();
    let hosted = |name: &str, outcome: Result<(), tonic::Status>, findings: &mut Vec<String>| {
        match outcome {
            Ok(()) => findings.push(format!("{name} applied on the hosted read handle")),
            Err(error) if error.code() != Code::FailedPrecondition => {
                findings.push(format!("{name} refused with an unnamed code: {error}"))
            }
            Err(error) if !error.message().contains("propose through the host") => {
                findings.push(format!("{name} refused without naming the host: {error}"))
            }
            Err(_) => {}
        }
    };

    let prepare = kit::source_command(
        &authority,
        &kit::owner_key(),
        "prepare-owner",
        1,
        1,
        0,
        kit::prepare_action(OWNER_WORKFLOW),
    );
    hosted(
        "execute",
        store.execute("alice", &prepare).map(|_| ()),
        &mut findings,
    );

    // Retirement is a legacy-plane act; the Begin it enables is the hosted
    // refusal under test.
    let legacy = kit::legacy_plane(&dir, 2);
    let (retired, _request) = kit::retire(&store, &legacy, 1);
    let payload = kit::import_payload(&retired);
    let (chunk_bytes, chunk_count) = kit::plan_three_chunks(payload.len());
    let begin = kit::import_command(
        &authority,
        "direct-begin",
        1,
        kit::WORKFLOW,
        kit::begin_action(&retired, &payload, chunk_bytes, chunk_count),
    );
    hosted(
        "begin_control_import",
        store
            .begin_control_import("alice", &begin, &retired)
            .map(|_| ()),
        &mut findings,
    );
    let chunk = kit::import_command(
        &authority,
        "direct-chunk",
        2,
        kit::WORKFLOW,
        kit::chunk_action(&payload, chunk_bytes, 0),
    );
    hosted(
        "execute_control_import",
        store.execute_control_import("alice", &chunk).map(|_| ()),
        &mut findings,
    );
    let configure = kit::configure_command(
        &authority,
        "direct-cfg",
        1,
        1,
        kit::capacity_configuration(1_000_000_000_000),
    );
    hosted(
        "configure_capacity",
        store.configure_capacity("alice", &configure).map(|_| ()),
        &mut findings,
    );
    let register = kit::register_transition(&authority, kit::NODE_A, kit::INC_A);
    hosted(
        "capacity_transition",
        store.capacity_transition("alice", &register).map(|_| ()),
        &mut findings,
    );
    match store.admission("alice") {
        Ok(_) => findings.push("admission() granted a local admission on a hosted store".into()),
        Err(error) if error.code() != Code::FailedPrecondition => {
            findings.push(format!("admission() refused with an unnamed code: {error}"))
        }
        Err(_) => {}
    }

    // Evidence: nothing landed on the advertised read handle.
    if store.owner("alice", &kit::owner_key()).is_ok() {
        findings.push("the read handle shows a PREPARED owner written with no log entry".into());
    }
    if store
        .control_import_workflow("alice", &kit::key(), kit::WORKFLOW)
        .is_ok()
    {
        findings.push("the read handle shows a staged workflow written with no log entry".into());
    }
    if store.capacity_state("alice", &kit::key()).is_ok() {
        findings.push("the read handle shows capacity state written with no log entry".into());
    }
    if raft_kit::durable_position(&guard) != position_before
        || guard.log_entries().unwrap().len() != entries_before
    {
        findings.push("the durable applied position moved with no committed entry".into());
    }
    assert!(
        findings.is_empty(),
        "R1 REQUIRED that a hosted read handle cannot mutate; observed:\n  - {}",
        findings.join("\n  - ")
    );
    drop(store);
    guard.shutdown().await;
}

/// R1 positive coverage: the trusted host entry points keep working — the
/// same command families the closed raw paths accepted holder-free succeed
/// with their holders here, so the R1 refusals cannot be read as "raft is
/// off".
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
/// transactional with the retry lookup." REQUIRED: the durable applied
/// position (the store's recorded position, read through the host, and
/// the position a snapshot binds) equals the latest consumed index even
/// when the latest entry is an exact retry. At 331c18f the retry path
/// returned the stored decision before `write_pending_applied`, so a
/// snapshot taken right after an exact retry bound the PREVIOUS entry's
/// position. (A fresh entry after the retry would overwrite the lagging
/// position, which is why the retry is last; raft's `initialize` commits a
/// membership entry at index 1, so after N proposals the applied index is
/// N+1.)
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
    // stored decision verbatim. R2 REQUIRED the durable position to be 4.
    let retry = guard.propose_command("alice", &prepare).await.unwrap();
    assert_eq!(
        first, retry,
        "an exact retry must return the stored decision verbatim"
    );
    raft_kit::wait_applied(&guard, 4).await;
    assert_eq!(
        raft_kit::durable_position(&guard),
        4,
        "R2 REQUIRED the durable applied position to include the exact retry \
         at index 4 — the retry path skipped write_pending_applied"
    );
    let (_image, meta) = raft_kit::snapshot_image(&dir, &guard).await;
    let bound = meta
        .last_log_id
        .unwrap_or_else(|| panic!("snapshot meta carries no applied position"));
    assert_eq!(
        bound.index, 4,
        "R2 REQUIRED the snapshot to bind the exact retry at index 4; it bound {bound:?}"
    );

    // Restart: the group recovers from durable state and keeps serving at
    // the same position.
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
    assert!(raft_kit::durable_position(&guard) >= 4);
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
    // index. R2 REQUIRED the durable position to be 5.
    let retried = guard
        .propose_import("alice", &chunk("chunk-0", 2, 0), None)
        .await
        .unwrap();
    assert_eq!(staged, retried, "the exact retry changed the decision");
    raft_kit::wait_applied(&guard, 5).await;
    assert_eq!(
        raft_kit::durable_position(&guard),
        5,
        "R2 REQUIRED the durable applied position to include the exact chunk \
         retry at index 5 — the retry path skipped write_pending_applied"
    );
    let (_image, meta) = raft_kit::snapshot_image(&dir, &guard).await;
    assert_eq!(meta.last_log_id.unwrap().index, 5);
    drop(store);
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
    assert_eq!(
        raft_kit::durable_position(&guard),
        12,
        "the unchanged observation transition must advance the durable position"
    );

    // The exact configure retry comes LAST: stored decision verbatim, a new
    // consumed index. R2 REQUIRED the durable position to be 13.
    let retried = guard
        .propose_capacity_configure("alice", &configure)
        .await
        .unwrap();
    assert_eq!(
        first, retried,
        "the exact configure retry changed the decision"
    );
    raft_kit::wait_applied(&guard, 13).await;
    assert_eq!(
        raft_kit::durable_position(&guard),
        13,
        "R2 REQUIRED the durable applied position to include the configure \
         retry at index 13 (after the refusal, both observation transitions \
         and the unchanged report) — the retry path skipped write_pending_applied"
    );
    let (_image, meta) = raft_kit::snapshot_image(&dir, &guard).await;
    assert_eq!(meta.last_log_id.unwrap().index, 13);
    drop(store);
    guard.shutdown().await;
}

/// A two-member fixture whose leader has one PREPARED owner and a published
/// snapshot binding it, and whose member is started but not yet seeded.
async fn leader_with_image(
    name: &str,
) -> (
    Cluster,
    Vec<u8>,
    pipestream_search::pb::storage::RaftSnapshotMeta,
) {
    let group = kit::identity(kit::SEED);
    let mut cluster = Cluster::bootstrap(name, &group, &kit::policy(), &kit::limits()).await;
    let prepare = kit::source_command(
        &group,
        &kit::owner_key(),
        "prepare-owner",
        1,
        1,
        0,
        kit::prepare_action(OWNER_WORKFLOW),
    );
    let decision = cluster
        .leader()
        .propose_command("alice", &prepare)
        .await
        .unwrap();
    assert_eq!(decision.code, 0, "{}", decision.message);
    let (image, meta) = raft_kit::snapshot_image(&cluster.leader_dir, cluster.leader()).await;
    cluster.start_member().await;
    assert!(cluster.member().awaiting_snapshot().unwrap());
    (cluster, image, meta)
}

/// R4 (review): "make refusal preserve the old usable handle ... At 331c18f
/// state_machine.rs:307-314 takes the live store out of the shared Option
/// before replace_from; on refusal the slot stays None." REQUIRED: a
/// refused install leaves the host's handle slot serving reads (and the
/// held reader keeps working). Through the supported path the install
/// arrives from a registered peer over the transport; the refusal is
/// returned to that peer by name. The library treats a refused install as a
/// fatal storage error and may stop the receiving core; that liveness
/// observation is recorded, and the safety assertions do not depend on it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r4_install_refusal_preserves_the_live_handle() {
    let _serial = serial();
    let (mut cluster, image, meta) = leader_with_image("r4-refusal").await;
    let group = cluster.group.clone();
    let held = cluster.member().store().unwrap();
    let refused = cluster.push(&meta, &image).await;
    let error = match refused {
        Ok(_) => panic!("the install must refuse: a reader is outstanding"),
        Err(error) => error,
    };
    assert!(
        error.message().contains("outstanding handles"),
        "the refusal must name the outstanding handle: {error}"
    );
    let core_running = raft_kit::core_running(cluster.member());
    eprintln!("r4: member core running after the refused install: {core_running}");

    // REQUIRED (R4): the refusal preserves the old usable handle.
    let slot = cluster
        .member()
        .store()
        .expect("R4 REQUIRED the refused install to preserve the live handle");
    slot.policy("alice", kit::WORKSPACE, kit::COLLECTION)
        .expect("the preserved handle must still serve reads");
    assert!(
        cluster.member().awaiting_snapshot().unwrap(),
        "nothing was installed under the refused swap"
    );
    // The reader held across the refused install must still work.
    held.policy("alice", kit::WORKSPACE, kit::COLLECTION)
        .expect("the held reader must survive the refused install");
    drop(held);
    drop(slot);

    // With no outstanding handles the same image installs cleanly (on a
    // restarted member if the library stopped the core).
    if !core_running {
        cluster.restart_member().await;
    }
    cluster
        .push(&meta, &image)
        .await
        .expect("a clean install must succeed");
    let _ = group;
    assert!(!cluster.member().awaiting_snapshot().unwrap());
    let owner = cluster
        .member()
        .store()
        .unwrap()
        .owner("alice", &kit::owner_key())
        .expect("the installed store serves the image's owner");
    assert_eq!(owner.phase, PreparedSourceOwnerPhase::Prepared as i32);
    cluster.shutdown().await;
}

/// R4: "define a stable host-owned indirection/read-guard API for snapshot
/// swaps ... Test subscription rebinding and managed owner access after a
/// successful install." REQUIRED: a `subscribe_applied` receiver taken
/// before the install observes revisions published after it (rebound to the
/// new database). At 331c18f the receiver stayed attached to the retired
/// database and never fired again. Managed owner access on the installed
/// store must reflect the image, and entries replicated after the install
/// keep waking the same receiver.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r4_subscription_rebinding_after_install() {
    let _serial = serial();
    let (cluster, image, meta) = leader_with_image("r4-rebind").await;
    let group = cluster.group.clone();
    let mut rx = cluster.member().store().unwrap().subscribe_applied();
    let applied_before = *rx.borrow_and_update();

    cluster
        .push(&meta, &image)
        .await
        .expect("clean install must succeed");

    // Managed owner access after the install reflects the image.
    let installed = cluster.member().store().unwrap();
    let owner = installed
        .owner("alice", &kit::owner_key())
        .expect("the installed store must serve the image's owner");
    assert_eq!(owner.phase, PreparedSourceOwnerPhase::Prepared as i32);
    let after_install = *installed.subscribe_applied().borrow();
    assert!(
        after_install > applied_before,
        "the installed image did not advance the applied revision"
    );
    drop(installed);

    // REQUIRED (R4): the pre-install receiver observes the post-install
    // revision — rebound, not left on the retired database.
    let rebound = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if *rx.borrow_and_update() >= after_install {
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
         {after_install} (it stays attached to the retired database)"
    );

    // Entries replicated after the install wake the same receiver: add the
    // member to the group (it is at the leader's snapshot position, so the
    // log continues from there) and propose one more command.
    cluster.add_member().await;
    let next = kit::source_command(
        &group,
        &kit::owner_key(),
        "prepare-second",
        2,
        1,
        0,
        kit::prepare_action(b"installation-two"),
    );
    let decision = cluster
        .leader()
        .propose_command("alice", &next)
        .await
        .unwrap();
    assert_eq!(
        decision.code,
        Code::FailedPrecondition as u32,
        "a second preparation of the same owner is a recorded refusal: {}",
        decision.message
    );
    // A recorded refusal commits a decision, not a new revision.
    let revoke = kit::source_command(
        &group,
        &kit::key(),
        "grant-bob",
        2,
        1,
        0,
        kit::replace_grants_action(vec![
            kit::grant("alice", kit::COLLECTION),
            kit::grant("bob", kit::COLLECTION),
        ]),
    );
    let decision = cluster
        .leader()
        .propose_command("alice", &revoke)
        .await
        .unwrap();
    assert_eq!(decision.code, 0, "{}", decision.message);
    let leader_position = raft_kit::durable_position(cluster.leader());
    raft_kit::wait_applied(cluster.member(), leader_position).await;
    let replicated = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if *rx.borrow_and_update() > after_install {
                return true;
            }
            if rx.changed().await.is_err() {
                return false;
            }
        }
    })
    .await;
    assert!(
        replicated == Ok(true),
        "the pre-install receiver must keep waking on entries replicated after the install"
    );
    cluster.shutdown().await;
}

/// ACTIVE happy path through the host: Prepare -> bind -> ConfirmReady ->
/// Activate, the write fence, and the fence's survival across a host
/// restart. Positive coverage. A hosted store grants no local admission
/// since 1f4ea13; the owner-side work runs under the host's leased
/// admission.
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
    // A hosted store refuses local admission; the host grants a leased one
    // after a linearizable read. The admission is a read guard on the
    // store's admission lock, and the raft apply path takes that lock
    // exclusively, so it must not span a propose.
    assert_eq!(
        store.admission("alice").err().map(|e| e.code()),
        Some(Code::FailedPrecondition)
    );
    let managed = guard
        .with_admission("alice", |admission| {
            catalog.bind_prepared_owner(admission, &store, &preparation, 1 << 20)
        })
        .await
        .unwrap();
    let verified = managed.completion().unwrap();
    assert_eq!(verified.completion().bound_at_sequence, 1);

    let confirm = confirm_command(&authority, 2, verified.completion().clone());
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
    guard
        .with_admission("alice", |admission| {
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
            Ok(())
        })
        .await
        .unwrap();

    // The catalog side opens the activated source (format 10).
    guard
        .with_admission("alice", |admission| managed.activate(admission).map(|_| ()))
        .await
        .unwrap();

    // The fence survives a host restart. Every clone of the hosted store
    // holds the redb file lock, so drop them all before reopening the group.
    guard.shutdown().await;
    drop(store);
    let guard = HostGuard::new(raft_kit::start_host(&dir).await);
    raft_kit::wait_leader(&guard).await;
    let store = guard.store().unwrap();
    let owner = store.owner("alice", &kit::owner_key()).unwrap();
    assert_eq!(owner.phase, PreparedSourceOwnerPhase::Active as i32);
    guard
        .with_admission("alice", |admission| {
            admission
                .admit_write(&kit::owner_key(), 1, AccessAction::Ingest)
                .map(|_| ())
        })
        .await
        .expect("the fence survives a restart");
    drop(store);
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
    drop(store);
    guard.shutdown().await;
}

//! Raft-host verification target (slice 4a, reconciled onto the fixed
//! checkpoint `0fe7081`; `docs/control-authority-test-harness.md`).
//!
//! At `331c18f` this target pinned review findings R1 (unadmitted mutation
//! surfaces), R2 (retry position) and R4 (install handle lifecycle) as
//! minimized reproductions. Fable's fixes (1f4ea13) closed them, and the
//! fixes ALSO moved the offending surfaces behind compile-time boundaries:
//! `RaftHost::raft()` is gone, `RaftHost::propose` is private (host.rs:697),
//! and every replay surface on `SourceAuthorityStore` is pub(crate). The R1
//! reproductions are therefore restructured: what a test binary can still
//! reach is asserted at runtime (every public mutation path on a hosted
//! read handle refuses by name), and what it cannot reach is documented as
//! the compile-time boundary itself. Every test here is expected to PASS at
//! `0fe7081`; a failure is a finding against the fix.
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
use pipestream_search::pb::storage::{
    ConfirmSourceOwnerReady, PreparedSourceOwnerPhase, SourceOwnerCompletion, SourceResourceBinding,
};
use pipestream_search::pb::{
    accept_document_request::Mutation, AcceptDocumentRequest, AccessAction, ProtobufSource,
};
use tonic::Code;

const OWNER_WORKFLOW: &[u8] = b"installation-one";

/// Target-wide serialization for the parallel release gate: the raft kit
/// uses fixed node ids/certificates and loopback listeners, and the cluster
/// tests share timing-sensitive election windows, so tests hold this lock
/// for their whole body (the suite is small; serialized wall time is
/// unchanged in practice). Panics are not expected here anymore, but the
/// lock is re-acquired across poisoning anyway; mutual exclusion is what
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

/// The control revision a fresh raft store commits at before any proposal:
/// `initialize` commits the membership entry at log index 1, so after N
/// proposals the applied index is N + 1 (docs/raft-hosting.md).
const INITIALIZED_INDEX: u64 = 1;

/// Assert a hosted read handle refuses one mutation path by name, and
/// return the refusal message for evidence.
fn assert_hosted_refusal<T>(result: Result<T, tonic::Status>, path: &str) -> String {
    let status = match result {
        Ok(_) => panic!("{path} unexpectedly succeeded on a hosted store"),
        Err(status) => status,
    };
    assert_eq!(
        status.code(),
        Code::FailedPrecondition,
        "{path} refused with the wrong code"
    );
    status.message().to_string()
}

// ---------------------------------------------------------------------------
// R1: unadmitted mutation surfaces.
//
// Compile-time boundary at 0fe7081 (this is the fix; a test binary cannot
// even name these):
//   * `RaftHost::raft()` — REMOVED. There is no accessor to the raft core;
//     `RaftHost::wait`/`metrics`/`applied_position` are the only observes.
//   * `RaftHost::propose` — PRIVATE (host.rs:697, "Submit an admitted
//     proposal... Private: every public entry admits its command first").
//   * `SourceAuthorityStore::replay_command`, `replay_control_import`,
//     `replay_capacity_configure`, `replay_capacity_transition` — all
//     pub(crate) (source_authority.rs), so the 331c18f read-handle mutation
//     surfaces no longer exist outside the crate.
//   * `VerifiedOwnerCompletion` — private fields with a pub(crate)
//     constructor `from_binding` (source_authority.rs:411-435): "Only that
//     adapter can construct it; readiness is never confirmed from caller
//     bytes." A raw ConfirmReady envelope cannot even be assembled with a
//     verification token.
// What remains reachable at runtime is asserted below, by name.
// ---------------------------------------------------------------------------

/// R1 (restructured): every PUBLIC mutation path on the hosted read handle
/// refuses by name; reads still work. Fails loudly if any path starts
/// succeeding.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r1_hosted_read_handle_refuses_every_public_mutation_path() {
    let _serial = serial();
    let dir = kit::TestDir::new("r1-hosted-refusals");
    let guard = HostGuard::new(raft_kit::bootstrap_host(&dir).await);
    let authority = kit::identity(kit::SEED);
    let store = guard.store().unwrap();

    // Reads are the handle's advertised purpose: a fresh owner fails loud
    // (NotFound), not with a hosted-refusal or a fake success.
    let missing = store
        .owner("alice", &kit::owner_key())
        .expect_err("a fresh store has no owner to read");
    assert_eq!(missing.code(), Code::NotFound, "{missing}");

    // Direct control command.
    let prepare = kit::source_command(
        &authority,
        &kit::owner_key(),
        "prepare-direct",
        1,
        1,
        0,
        kit::prepare_action(b"installation-direct"),
    );
    let message = assert_hosted_refusal(store.execute("alice", &prepare), "store.execute");
    assert!(
        message.contains("propose through the host"),
        "execute refusal must name the host path: {message}"
    );

    // Direct import step (Begin without the holder: the raft path refuses
    // before any holder question is reached).
    let legacy = kit::legacy_plane(&dir, 2);
    let (retired, _request) = kit::retire(&store, &legacy, 1);
    let payload = kit::import_payload(&retired);
    let (chunk_bytes, chunk_count) = kit::plan_three_chunks(payload.len());
    let begin = kit::import_command(
        &authority,
        "begin-direct",
        1,
        kit::WORKFLOW,
        kit::begin_action(&retired, &payload, chunk_bytes, chunk_count),
    );
    let message = assert_hosted_refusal(
        store.begin_control_import("alice", &begin, &retired),
        "store.begin_control_import",
    );
    assert!(
        message.contains("propose through the host"),
        "import refusal must name the host path: {message}"
    );

    // Capacity configure and observation transition.
    let configure = kit::configure_command(
        &authority,
        "cfg-direct",
        1,
        1,
        kit::capacity_configuration(1_000_000_000_000),
    );
    let message = assert_hosted_refusal(
        store.configure_capacity("alice", &configure),
        "store.configure_capacity",
    );
    assert!(
        message.contains("propose through the host"),
        "configure refusal must name the host path: {message}"
    );
    let register = kit::register_transition(&authority, kit::NODE_A, kit::INC_A);
    let message = assert_hosted_refusal(
        store.capacity_transition("alice", &register),
        "store.capacity_transition",
    );
    assert!(
        message.contains("propose through the host"),
        "transition refusal must name the host path: {message}"
    );

    // The local admission grant is exactly what a hosted store must not hand
    // out (docs/raft-admission.md, "Why a local lock is not enough").
    let message = assert_hosted_refusal(store.admission("alice"), "store.admission");
    assert!(
        message.contains("obtain a leased admission through the host"),
        "admission refusal must name the leased host path: {message}"
    );

    // Nothing leaked: the refused paths applied nothing.
    assert!(
        store.owner("alice", &kit::owner_key()).is_err(),
        "a refused path must not apply"
    );

    // The trusted path still admits the same command end to end, and only
    // then does the read handle show it: a refused path must not apply.
    drop(store);
    let decision = guard.propose_command("alice", &prepare).await.unwrap();
    assert_eq!(decision.code, 0, "{}", decision.message);
    let store = guard.store().unwrap();
    let owner = store.owner("alice", &kit::owner_key()).unwrap();
    assert_eq!(
        owner.phase,
        pipestream_search::pb::storage::PreparedSourceOwnerPhase::Prepared as i32,
        "exactly the trusted-path command may have applied"
    );
    drop(store);
    guard.shutdown().await;
}

/// R1 (restructured): ConfirmReady through the general control path refuses
/// at proposal; without the managed binding's `VerifiedOwnerCompletion`
/// (private, pub(crate)-constructed — source_authority.rs:411) there is no
/// verification token to present at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r1_confirm_ready_through_the_general_path_is_refused() {
    let _serial = serial();
    let dir = kit::TestDir::new("r1-confirm-refused");
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

    // A forged completion: any caller bytes. At 331c18f this turned the
    // owner READY with no binding; the general path must now refuse it.
    let forged = pipestream_search::pb::storage::SourceOwnerCompletion {
        format_version: 1,
        binding_sha256: vec![7; 32],
        bound_at_sequence: 1,
        history_id: vec![42; 16],
        node_id: "server-a".into(),
        storage_incarnation: vec![41; 16],
    };
    let confirm = confirm_command(&authority, 2, forged);
    let refused = guard.propose_command("alice", &confirm).await;
    let status = refused.expect_err("ConfirmReady through the general path applied");
    assert_eq!(
        status.code(),
        Code::PermissionDenied,
        "wrong refusal code: {status}"
    );
    assert!(
        status.message().to_lowercase().contains("binding"),
        "the refusal must name the managed-binding boundary: {}",
        status.message()
    );

    // The owner did not move.
    let store = guard.store().unwrap();
    let owner = store.owner("alice", &kit::owner_key()).unwrap();
    assert_eq!(
        owner.phase,
        PreparedSourceOwnerPhase::Prepared as i32,
        "a refused ConfirmReady must not advance the owner"
    );
    drop(store);
    guard.shutdown().await;
}

/// R1 positive control: the trusted entry points admit the same command
/// families the raw paths used to mutate, and the effects are durable and
/// visible through the read handle.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r1_positive_replay_through_the_host() {
    let _serial = serial();
    let dir = kit::TestDir::new("r1-positive");
    let guard = HostGuard::new(raft_kit::bootstrap_host(&dir).await);
    let authority = kit::identity(kit::SEED);
    let store = guard.store().unwrap();

    // Control family: Prepare.
    let prepare = kit::source_command(
        &authority,
        &kit::owner_key(),
        "prepare-trusted",
        1,
        1,
        0,
        kit::prepare_action(OWNER_WORKFLOW),
    );
    let decision = guard.propose_command("alice", &prepare).await.unwrap();
    assert_eq!(decision.code, 0, "{}", decision.message);

    // Import family: Begin needs the retired holder, chunks do not. The
    // prepare above applied at control revision 2, which retirement binds.
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
    for ordinal in 0..chunk_count {
        let chunk = kit::import_command(
            &authority,
            &format!("chunk-{ordinal}"),
            3 + ordinal as u64,
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

    // Durable and visible through the read handle: the owner the trusted
    // path prepared (the capacity family is exercised end to end by the r2
    // capacity test, which commits the import first).
    let owner = store.owner("alice", &kit::owner_key()).unwrap();
    assert_eq!(
        owner.phase,
        pipestream_search::pb::storage::PreparedSourceOwnerPhase::Prepared as i32
    );
    let decision = store
        .decision("alice", &kit::owner_key(), prepare.command_id.as_slice())
        .unwrap();
    assert_eq!(decision.code, 0, "{}", decision.message);
    drop(store);
    guard.shutdown().await;
}

// ---------------------------------------------------------------------------
// R2: an exact retry must advance the durable applied position (write the
// stored decision and the position in the same transaction — 1f4ea13).
// The primary observable is now the public `RaftHost::applied_position()`
// (host.rs:584); one test keeps the snapshot-bound check (the position must
// survive into a snapshot's meta).
// ---------------------------------------------------------------------------

/// Propose a Prepare and return the applied index it reached.
async fn propose_prepare(
    host: &pipestream_search::raft::RaftHost,
    owner_id: &str,
    ecr: u64,
) -> u64 {
    let authority = kit::identity(kit::SEED);
    let command = kit::source_command(
        &authority,
        &kit::owner_key(),
        owner_id,
        ecr,
        1,
        0,
        kit::prepare_action(OWNER_WORKFLOW),
    );
    let decision = host.propose_command("alice", &command).await.unwrap();
    assert_eq!(decision.code, 0, "{}", decision.message);
    host.applied_position().unwrap().unwrap().index
}

/// R2 (control family): changed-content retry consumes a refusal entry, the
/// exact retry returns the stored decision, and the durable applied
/// position is the retry's index — including after a snapshot binds it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r2_exact_retry_advances_durable_applied_position() {
    let _serial = serial();
    let dir = kit::TestDir::new("r2-exact-retry");
    let guard = HostGuard::new(raft_kit::bootstrap_host(&dir).await);

    let first = propose_prepare(&guard, "retry-owner", 1).await;
    assert_eq!(first, INITIALIZED_INDEX + 1);

    // Changed-content retry: same command id, different bytes — a consumed
    // refusal that must still advance the position.
    let authority = kit::identity(kit::SEED);
    let changed = kit::source_command(
        &authority,
        &kit::owner_key(),
        "retry-owner",
        2,
        1,
        0,
        kit::prepare_action(b"installation-changed"),
    );
    let refused = guard.propose_command("alice", &changed).await;
    let status = refused.expect_err("changed-content retry must refuse");
    assert_eq!(status.code(), Code::FailedPrecondition, "{status}");
    assert_eq!(guard.applied_position().unwrap().unwrap().index, first + 1);

    // Exact retry, deliberately last: the byte-identical first command. The
    // replay ledger returns the stored decision; the applied position must
    // still advance (1f4ea13 runs write_pending_applied in the retry txn).
    let original = kit::source_command(
        &authority,
        &kit::owner_key(),
        "retry-owner",
        1,
        1,
        0,
        kit::prepare_action(OWNER_WORKFLOW),
    );
    let decision = guard.propose_command("alice", &original).await.unwrap();
    assert_eq!(decision.code, 0, "{}", decision.message);
    let retried = guard.applied_position().unwrap().unwrap().index;
    assert_eq!(
        retried,
        first + 2,
        "R2: the exact retry must advance the durable applied position \
         (1f4ea13 runs write_pending_applied in the retry transaction)"
    );

    // The position survives into a snapshot's meta.
    let (_image, meta) = raft_kit::snapshot_image(&dir, &guard).await;
    assert_eq!(
        meta.last_log_id.map(|id| id.index),
        Some(retried),
        "a snapshot built after the retry must bind the retry's position, \
         not the position before it"
    );
    guard.shutdown().await;
}

/// R2 (import family): chunk retries advance the durable applied position
/// the same way.
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
    drop(store);

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
    let chunk0 = kit::import_command(
        &authority,
        "chunk-0",
        2,
        kit::WORKFLOW,
        kit::chunk_action(&payload, chunk_bytes, 0),
    );
    assert_eq!(
        guard
            .propose_import("alice", &chunk0, None)
            .await
            .unwrap()
            .code,
        0
    );
    assert_eq!(
        guard.applied_position().unwrap().unwrap().index,
        INITIALIZED_INDEX + 2
    );

    // Changed-content retry of chunk-0: different bytes under the same id.
    let mut altered = payload.clone();
    let last = altered.len() - 1;
    altered[last] ^= 0xFF;
    let changed = kit::import_command(
        &authority,
        "chunk-0",
        3,
        kit::WORKFLOW,
        kit::chunk_action(&altered, chunk_bytes, 0),
    );
    let refused = guard.propose_import("alice", &changed, None).await;
    let status = refused.expect_err("changed-content chunk retry must refuse");
    assert_eq!(status.code(), Code::FailedPrecondition, "{status}");
    assert_eq!(
        guard.applied_position().unwrap().unwrap().index,
        INITIALIZED_INDEX + 3
    );

    // Exact retry, deliberately last: the byte-identical chunk-0 command.
    let exact = kit::import_command(
        &authority,
        "chunk-0",
        2,
        kit::WORKFLOW,
        kit::chunk_action(&payload, chunk_bytes, 0),
    );
    assert_eq!(
        guard
            .propose_import("alice", &exact, None)
            .await
            .unwrap()
            .code,
        0
    );
    assert_eq!(
        guard.applied_position().unwrap().unwrap().index,
        INITIALIZED_INDEX + 4,
        "R2: the exact chunk retry must advance the durable applied position"
    );
    guard.shutdown().await;
}

/// R2 (capacity family): the full placement flow, ending with the exact
/// configure retry, binds the retry's position durably.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r2_capacity_retries_and_observation_transition() {
    let _serial = serial();
    let dir = kit::TestDir::new("r2-capacity-retry");
    let guard = HostGuard::new(raft_kit::bootstrap_host(&dir).await);
    let authority = kit::identity(kit::SEED);
    let store = guard.store().unwrap();
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
    // Commit at revision 6: begin 1, three chunks 2..4, commit 5.
    let commit = kit::import_command(&authority, "commit", 5, kit::WORKFLOW, kit::commit_action());
    assert_eq!(
        guard
            .propose_import("alice", &commit, None)
            .await
            .unwrap()
            .code,
        0
    );
    let generation = kit::committed_generation(&store);
    drop(store);

    // Configure (index 7), changed-content configure refusal (8).
    let configure = || {
        kit::configure_command(
            &authority,
            "cfg",
            6,
            1,
            kit::capacity_configuration(1_000_000_000_000),
        )
    };
    assert_eq!(
        guard
            .propose_capacity_configure("alice", &configure())
            .await
            .unwrap()
            .code,
        0
    );
    let mut changed_cfg = kit::capacity_configuration(500_000_000_000);
    changed_cfg.max_registered_nodes = 4;
    let changed = kit::configure_command(&authority, "cfg", 7, 1, changed_cfg);
    let refused = guard.propose_capacity_configure("alice", &changed).await;
    let status = refused.expect_err("changed-content configure retry must refuse");
    assert_eq!(status.code(), Code::FailedPrecondition, "{status}");

    // Both registrations, a landed report and the positive unchanged report.
    for (node, inc) in [(kit::NODE_A, kit::INC_A), (kit::NODE_B, kit::INC_B)] {
        guard
            .propose_capacity_transition("alice", &kit::register_transition(&authority, node, inc))
            .await
            .unwrap();
    }
    let hot = kit::scanned_observation(kit::NODE_A, kit::INC_A, generation);
    let landed = guard
        .propose_capacity_transition("alice", &kit::report_transition(&authority, hot.clone()))
        .await
        .unwrap();
    assert_eq!(
        landed.outcome,
        pipestream_search::pb::storage::CapacityTransitionOutcome::Landed as i32
    );
    let unchanged = guard
        .propose_capacity_transition("alice", &kit::report_transition(&authority, hot))
        .await
        .unwrap();
    assert_eq!(
        unchanged.outcome,
        pipestream_search::pb::storage::CapacityTransitionOutcome::Unchanged as i32
    );
    assert_eq!(unchanged.observation_epoch, 1);

    // Exact configure retry, deliberately last (indices 2..=12 committed, the
    // retry is 13).
    let retry = guard
        .propose_capacity_configure("alice", &configure())
        .await
        .unwrap();
    assert_eq!(retry.code, 0, "{}", retry.message);
    assert_eq!(
        guard.applied_position().unwrap().unwrap().index,
        INITIALIZED_INDEX + 12,
        "R2: the exact configure retry must advance the durable applied \
         position (indices 2..=12 before it, the retry last)"
    );
    guard.shutdown().await;
}

// ---------------------------------------------------------------------------
// R4: snapshot install lifecycle, through the real transport (0fe7081 made
// the standalone machine constructor pub(crate), so a follower seeded by
// the leader's snapshot is the external install path).
// ---------------------------------------------------------------------------

/// Poll a predicate with a deadline.
async fn poll_until(deadline: Duration, what: &str, mut probe: impl FnMut() -> bool) {
    let start = std::time::Instant::now();
    loop {
        if probe() {
            return;
        }
        assert!(
            start.elapsed() < deadline,
            "{what} did not happen within {deadline:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// R4 (through the transport): an install attempted while a store clone is
/// outstanding refuses by name — "outstanding handles" — and nothing on disk
/// is replaced: no generation is published on the member and the pending
/// genesis store awaits its snapshot still. The refusal reaches the member
/// as a state-machine storage error, which the raft core treats as fatal
/// (`running_state` is `Err`, the core is `Shutdown`): the retry-after-
/// release half of the swap contract is machine-level only (the standalone
/// constructor is pub(crate); the in-crate half is src/raft/tests.rs:623).
/// After the clone is released, a freshly prepared member joins and seeds
/// cleanly; restarting the refused member itself refuses ("no applied
/// position; nothing to snapshot") and is recorded as a residual.
#[cfg(feature = "tls")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r4_install_refusal_preserves_the_live_handle() {
    let _serial = serial();
    let authority = kit::identity(kit::SEED);
    let directory = raft_kit::peer_directory(&authority, &[1, 2, 3]);
    let dir_leader = kit::TestDir::new("r4-refuse-leader");
    let leader = raft_kit::bootstrap_cluster_node(&dir_leader, 1, &directory).await;
    let applied = propose_prepare(&leader, "r4-owner", 1).await;
    assert_eq!(applied, INITIALIZED_INDEX + 1);

    let dir_member = kit::TestDir::new("r4-refuse-member");
    let member = raft_kit::start_member_node(&dir_member, 2, &directory).await;
    // Hold a clone of the pending member's store across the install attempt:
    // the swap must refuse rather than close the handle out from under its
    // reader (docs/raft-hosting.md: "on refusal the old handle is kept, the
    // unpublished generation removed and the error named").
    let held = member.store().unwrap();
    let join = leader
        .add_learner(2, member.advertised_addr().unwrap())
        .await;
    let status = join.expect_err("a refused install must not report a caught-up learner");
    assert_eq!(
        status.code(),
        Code::Unavailable,
        "the leader must refuse loudly, not seed a half-installed member: {status}"
    );

    // The refusal is named on the member: the core recorded the storage
    // error and stopped.
    let metrics = member.metrics().borrow().clone();
    assert!(
        format!("{:?}", metrics.running_state).contains("outstanding handles"),
        "R4: the refused install must name the outstanding-handles boundary: \
         {metrics:?}"
    );
    assert_eq!(
        metrics.state,
        openraft::ServerState::Shutdown,
        "R4: a refused install stops the member core (openraft treats a \
         state-machine storage error as fatal); the member does not serve a \
         half-installed image"
    );
    // The held clone still reads: the old handle is kept, nothing replaced.
    let missing = held
        .owner("alice", &kit::owner_key())
        .expect_err("the genesis member store must not have served the owner");
    assert_eq!(missing.code(), Code::NotFound, "{missing}");
    drop(held);

    // Nothing landed on disk: no published generation, no receive left.
    let snapshots = raft_kit::snapshots_dir(&dir_member);
    assert!(
        raft_kit::read_pointer(&dir_member).is_none(),
        "a refused install must not publish a generation"
    );
    let leftovers = std::fs::read_dir(&snapshots)
        .unwrap()
        .filter(|entry| {
            entry
                .as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("incoming-")
        })
        .count();
    assert_eq!(leftovers, 0, "a refused install leaves no receive behind");

    // Released, a freshly prepared member joins and seeds cleanly: the group
    // continues. (Restarting the refused member itself does not work at
    // 0fe7081: its log holds entries its state machine never applied, and
    // `RaftHost::start_member` refuses with "no applied position; nothing to
    // snapshot" — recorded as a residual for Fable, loud at least.)
    member.shutdown().await.unwrap();
    let dir_member3 = kit::TestDir::new("r4-refuse-member-3");
    let member3 = raft_kit::start_member_node(&dir_member3, 3, &directory).await;
    leader
        .add_learner(3, member3.advertised_addr().unwrap())
        .await
        .unwrap();
    poll_until(Duration::from_secs(30), "member 3 seeded", || {
        !member3.awaiting_snapshot().unwrap()
    })
    .await;
    let store = member3.store().unwrap();
    let owner = store.owner("alice", &kit::owner_key()).unwrap();
    assert_eq!(
        owner.phase,
        PreparedSourceOwnerPhase::Prepared as i32,
        "the freshly seeded member must serve the installed image"
    );
    drop(store);
    leader.shutdown().await.unwrap();
    member3.shutdown().await.unwrap();
}

/// R4: a subscriber taken before the install stays attached across it — the
/// applied watch channels move to the reopened store (docs/raft-hosting.md:
/// "On success the pointer moves and subscribers stay attached").
#[cfg(feature = "tls")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r4_subscription_rebinding_after_install() {
    let _serial = serial();
    let authority = kit::identity(kit::SEED);
    let directory = raft_kit::peer_directory(&authority, &[1, 2]);
    let dir_leader = kit::TestDir::new("r4-rebind-leader");
    let leader = raft_kit::bootstrap_cluster_node(&dir_leader, 1, &directory).await;
    propose_prepare(&leader, "r4-rebind-owner", 1).await;

    let dir_member = kit::TestDir::new("r4-rebind-member");
    let member = raft_kit::start_member_node(&dir_member, 2, &directory).await;
    // Subscribe on the pending store BEFORE the install, then drop the store
    // handle: the R4 swap refuses while any handle clone is outstanding
    // (docs/raft-hosting.md, "The swap needs exclusive ownership of the
    // store handle"), but the applied watch channel is not a handle — it
    // moves to the reopened store, so subscribers stay attached.
    let mut rx = {
        let pre_install = member.store().unwrap();
        pre_install.subscribe_applied()
    };
    let before = *rx.borrow();
    leader
        .add_learner(2, member.advertised_addr().unwrap())
        .await
        .unwrap();
    poll_until(Duration::from_secs(30), "member seeded", || {
        !member.awaiting_snapshot().unwrap()
    })
    .await;

    // A fresh command after the install; the pre-install receiver must see
    // its applied revision. `subscribe_applied` carries the source control
    // revision (src/source_authority.rs `publish_applied`), so the command
    // must genuinely advance it: a fresh Prepare on a second owner key (the
    // fixture owner key already holds a preparation).
    let second_key = pipestream_search::pb::storage::LogicalSourceOwner {
        owner_id: b"r4-rebind-second".to_vec(),
        ..kit::key()
    };
    let second = kit::source_command(
        &authority,
        &second_key,
        "r4-rebind-second",
        2,
        1,
        0,
        kit::prepare_action(OWNER_WORKFLOW),
    );
    let decision = leader.propose_command("alice", &second).await.unwrap();
    assert_eq!(decision.code, 0, "{}", decision.message);
    let expected = decision.control_revision;
    let observed = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let now = *rx.borrow();
            if now >= expected {
                return true;
            }
            if rx.changed().await.is_err() {
                return false;
            }
        }
    })
    .await;
    assert!(
        observed == Ok(true),
        "R4 REQUIRED subscription rebinding across a snapshot install; the \
         pre-install receiver never observed the post-install control \
         revision {expected} (before the install it was {before})"
    );
    leader.shutdown().await.unwrap();
    member.shutdown().await.unwrap();
}

/// ACTIVE happy path through the host: Prepare -> bind -> ConfirmReady ->
/// Activate, the write fence, and the fence's survival across a host
/// restart. Positive coverage.
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
    // A hosted store refuses local admissions (R1); the leased admission
    // entry is `with_admission`. The binding consumes the catalog and must
    // hold the admission through the source commit, so it runs inside the
    // lease window.
    let managed = guard
        .with_admission("alice", |admission| {
            catalog.bind_prepared_owner(admission, &store, &preparation, 1 << 20)
        })
        .await
        .unwrap();
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
    let active = guard
        .with_admission("alice", |admission| managed.activate(admission))
        .await
        .unwrap();

    // The fence survives a host restart. Every clone of the hosted store
    // holds the redb file lock, so drop them all — `activate` consumes the
    // managed binding, whose store clone lives on in the activated source —
    // before reopening the group. The lock release can lag the shutdown
    // teardown, so reopen with a short bounded retry instead of racing it.
    drop(active);
    drop(store);
    guard.shutdown().await;
    let mut reopened = None;
    for _ in 0..40 {
        match pipestream_search::raft::RaftHost::start(
            dir.path(),
            &kit::identity(kit::SEED),
            raft_kit::NODE_ID,
            &raft_kit::host_config(),
        )
        .await
        {
            Ok(host) => {
                reopened = Some(host);
                break;
            }
            Err(status) => {
                assert!(
                    status.message().contains("would block"),
                    "the restart refused for an unexpected reason: {status}"
                );
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }
    let guard = HostGuard::new(reopened.expect("the restarted host never opened its store"));
    raft_kit::wait_leader(&guard).await;
    let store = guard.store().unwrap();
    let owner = store.owner("alice", &kit::owner_key()).unwrap();
    assert_eq!(owner.phase, PreparedSourceOwnerPhase::Active as i32);
    guard
        .with_admission("alice", |admission| {
            admission.admit_write(&kit::owner_key(), 1, AccessAction::Ingest)
        })
        .await
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
    let decision = guard.propose_import("bob", &recover, None).await.unwrap();
    assert_eq!(decision.code, 0, "{}", decision.message);

    // The workflow is recovered with the reservation released; the terminal
    // step names bob while alice stays the initiator; a later Commit is a
    // recorded refusal decision (the refusal applies and stays durable —
    // it does not resurrect the workflow).
    let workflow = store
        .control_import_workflow("bob", &kit::key(), kit::WORKFLOW)
        .unwrap();
    assert_eq!(
        workflow.phase,
        pipestream_search::pb::storage::ControlImportPhase::Recovered as i32
    );
    assert_eq!(workflow.reserved_bytes, 0);
    let commit = kit::import_command(&authority, "commit", 5, kit::WORKFLOW, kit::commit_action());
    let decision = guard.propose_import("alice", &commit, None).await.unwrap();
    assert_eq!(
        decision.code,
        Code::FailedPrecondition as u32,
        "a commit after recovery must be a recorded refusal: {}",
        decision.message
    );
    assert!(
        decision.message.contains("terminal"),
        "the refusal must name the terminal workflow: {}",
        decision.message
    );
    let workflow = store
        .control_import_workflow("bob", &kit::key(), kit::WORKFLOW)
        .unwrap();
    assert_eq!(
        workflow.phase,
        pipestream_search::pb::storage::ControlImportPhase::Recovered as i32
    );
    drop(store);
    guard.shutdown().await;
}

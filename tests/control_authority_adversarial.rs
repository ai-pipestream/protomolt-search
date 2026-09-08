//! Adversarial integration suite for the source-authority control plane
//! (slice 1; `docs/control-authority-test-harness.md`).
//!
//! Public API only: staged import of a retired legacy control authority,
//! exact-retry/revocation semantics, READY refusal, and SIGKILL recovery at
//! every durable import boundary. State equivalence is asserted through the
//! 32-byte `ControlCollectionSnapshot.digest`.

mod control_adversarial;

use std::time::Duration;

use control_adversarial::kit;
use pipestream_search::control_plane::DurableControlPlane;
use pipestream_search::pb::storage::control_import_command::Action as ImportAction;
use pipestream_search::pb::storage::LegacyControlRetirementRequest;
use prost::Message;
use tonic::Code;

const MARKER_TIMEOUT: Duration = Duration::from_secs(60);

/// Full Begin -> three Chunks -> Commit on a 2-route legacy plane; reopen is
/// digest-stable and a second import for the resource is a recorded refusal.
#[test]
fn import_commit_reopen_snapshot_digest_stable() {
    let dir = kit::TestDir::new("commit-reopen");
    let (store, authority) = kit::create_store(&dir);
    let legacy = kit::legacy_plane(&dir, 2);
    let (retired, _request) = kit::retire(&store, &legacy, 1);
    let payload = kit::import_payload(&retired);

    let (receipt, chunk_count) = kit::run_import(&store, &authority, &retired, &payload);
    assert_eq!(chunk_count, kit::CHUNKS);
    assert_eq!(receipt.routes, 2);
    // One control revision per accepted step: begin(1) + 3 chunks + commit.
    let expected_revision = 1 + 1 + u64::from(kit::CHUNKS) + 1;
    assert_eq!(receipt.control_revision, expected_revision);

    let before = kit::snapshot_digest(&store, "alice", &kit::key());
    let snapshot = store.control_snapshot("alice", &kit::key()).unwrap();
    assert_eq!(snapshot.control_revision, expected_revision);
    drop(store);

    let reopened = kit::open_store(&dir, &authority);
    let after = kit::snapshot_digest(&reopened, "alice", &kit::key());
    assert_eq!(
        before, after,
        "reopen changed the committed snapshot digest"
    );

    // Nothing else may import into this resource again.
    let (chunk_bytes, chunk_count) = kit::plan_three_chunks(payload.len());
    let again = reopened
        .begin_control_import(
            "alice",
            &kit::import_command(
                &authority,
                "begin-2",
                expected_revision,
                b"import-2",
                kit::begin_action(&retired, &payload, chunk_bytes, chunk_count),
            ),
            &retired,
        )
        .unwrap();
    assert_eq!(again.code, Code::AlreadyExists as u32, "{}", again.message);
}

/// Begin + one Chunk + Abort seals the workflow: later steps are recorded
/// refusals, the workflow id is a single-use tombstone, and a fresh workflow
/// can still import the resource and commit.
#[test]
fn import_abort_releases_and_seals_workflow() {
    let dir = kit::TestDir::new("abort-seal");
    let (store, authority) = kit::create_store(&dir);
    let legacy = kit::legacy_plane(&dir, 2);
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
    let staged = store
        .execute_control_import(
            "alice",
            &kit::import_command(
                &authority,
                "chunk-0",
                2,
                kit::WORKFLOW,
                kit::chunk_action(&payload, chunk_bytes, 0),
            ),
        )
        .unwrap();
    assert_eq!(staged.code, 0, "{}", staged.message);
    let abort = store
        .execute_control_import(
            "alice",
            &kit::import_command(&authority, "abort", 3, kit::WORKFLOW, kit::abort_action()),
        )
        .unwrap();
    assert_eq!(abort.code, 0, "{}", abort.message);
    let workflow = store
        .control_import_workflow("alice", &kit::key(), kit::WORKFLOW)
        .unwrap();
    assert_eq!(
        workflow.phase,
        pipestream_search::pb::storage::ControlImportPhase::Aborted as i32
    );

    // Terminal: a late chunk and a commit refuse, durably recorded.
    for (id, action) in [
        ("chunk-1", kit::chunk_action(&payload, chunk_bytes, 1)),
        ("commit", kit::commit_action()),
    ] {
        let decision = store
            .execute_control_import(
                "alice",
                &kit::import_command(&authority, id, 4, kit::WORKFLOW, action),
            )
            .unwrap();
        assert_eq!(
            decision.code,
            Code::FailedPrecondition as u32,
            "{id}: {}",
            decision.message
        );
        let stored = store
            .control_import_decision("alice", &kit::key(), id.as_bytes())
            .unwrap();
        assert_eq!(stored.code, Code::FailedPrecondition as u32);
    }

    // The same workflow id can never be reused, even after the abort.
    let reuse = store
        .begin_control_import(
            "alice",
            &kit::import_command(
                &authority,
                "begin-again",
                4,
                kit::WORKFLOW,
                kit::begin_action(&retired, &payload, chunk_bytes, chunk_count),
            ),
            &retired,
        )
        .unwrap();
    assert_eq!(reuse.code, Code::AlreadyExists as u32, "{}", reuse.message);

    // A fresh workflow id imports the resource and commits.
    let fresh = store
        .begin_control_import(
            "alice",
            &kit::import_command(
                &authority,
                "fresh-begin",
                4,
                b"import-2",
                kit::begin_action(&retired, &payload, chunk_bytes, chunk_count),
            ),
            &retired,
        )
        .unwrap();
    assert_eq!(fresh.code, 0, "{}", fresh.message);
    let mut revision = 5u64;
    for ordinal in 0..chunk_count {
        let decision = store
            .execute_control_import(
                "alice",
                &kit::import_command(
                    &authority,
                    &format!("fresh-chunk-{ordinal}"),
                    revision,
                    b"import-2",
                    kit::chunk_action(&payload, chunk_bytes, ordinal),
                ),
            )
            .unwrap();
        assert_eq!(decision.code, 0, "{}", decision.message);
        revision += 1;
    }
    let commit = store
        .execute_control_import(
            "alice",
            &kit::import_command(
                &authority,
                "fresh-commit",
                revision,
                b"import-2",
                kit::commit_action(),
            ),
        )
        .unwrap();
    assert_eq!(commit.code, 0, "{}", commit.message);
    assert!(commit.receipt.is_some());
}

/// Two fresh stores with identical inputs; store A stages chunks in ordinal
/// order, store B in reverse order. The committed snapshot digests must be
/// identical: staging order is not part of the applied state.
#[test]
fn permuted_chunk_order_yields_identical_snapshot() {
    let build = |name: &str, order: [u32; 3]| {
        let dir = kit::TestDir::new(name);
        let (store, authority) = kit::create_store(&dir);
        let legacy = kit::legacy_plane(&dir, 2);
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
        for (step, ordinal) in order.iter().enumerate() {
            let decision = store
                .execute_control_import(
                    "alice",
                    &kit::import_command(
                        &authority,
                        &format!("chunk-{ordinal}"),
                        2 + step as u64,
                        kit::WORKFLOW,
                        kit::chunk_action(&payload, chunk_bytes, *ordinal),
                    ),
                )
                .unwrap();
            assert_eq!(decision.code, 0, "{}", decision.message);
        }
        let commit = store
            .execute_control_import(
                "alice",
                &kit::import_command(
                    &authority,
                    "commit",
                    2 + u64::from(chunk_count),
                    kit::WORKFLOW,
                    kit::commit_action(),
                ),
            )
            .unwrap();
        assert_eq!(commit.code, 0, "{}", commit.message);
        kit::snapshot_digest(&store, "alice", &kit::key())
    };
    let forward = build("permute-forward", [0, 1, 2]);
    let reverse = build("permute-reverse", [2, 1, 0]);
    assert_eq!(
        forward, reverse,
        "chunk staging order leaked into the committed snapshot"
    );
}

/// Exact retry returns the stored decision without reapplying; the same id
/// with changed content refuses; a new id restaging identical content under a
/// staged ordinal is a recorded AlreadyExists, differing content a recorded
/// FailedPrecondition.
#[test]
fn exact_retry_and_changed_content() {
    let dir = kit::TestDir::new("exact-retry");
    let (store, authority) = kit::create_store(&dir);
    let legacy = kit::legacy_plane(&dir, 2);
    let (retired, _request) = kit::retire(&store, &legacy, 1);
    let payload = kit::import_payload(&retired);
    let (chunk_bytes, chunk_count) = kit::plan_three_chunks(payload.len());

    // General command path: Prepare on a logical owner key.
    let prepare = kit::source_command(
        &authority,
        &kit::owner_key(),
        "prepare-1",
        1,
        1,
        0,
        kit::prepare_action(b"installation-one"),
    );
    let first = store.execute("alice", &prepare).unwrap();
    assert_eq!(first.code, 0, "{}", first.message);
    assert_eq!(first.control_revision, 2);
    let retry = store.execute("alice", &prepare).unwrap();
    assert_eq!(retry, first, "exact retry must return the stored decision");
    let mut changed = prepare.clone();
    if let Some(pipestream_search::pb::storage::source_authority_command::Action::Prepare(
        request,
    )) = changed.action.as_mut()
    {
        request.workflow_id = b"installation-two".to_vec();
    }
    assert_eq!(
        store.execute("alice", &changed).unwrap_err().code(),
        Code::FailedPrecondition,
        "same command_id with different content must refuse"
    );

    // Import path on the collection key: the store revision is now 2.
    let begin = store
        .begin_control_import(
            "alice",
            &kit::import_command(
                &authority,
                "begin",
                2,
                kit::WORKFLOW,
                kit::begin_action(&retired, &payload, chunk_bytes, chunk_count),
            ),
            &retired,
        )
        .unwrap();
    assert_eq!(begin.code, 0, "{}", begin.message);
    let chunk = |id: &str, revision: u64, action: ImportAction| {
        kit::import_command(&authority, id, revision, kit::WORKFLOW, action)
    };
    let staged = store
        .execute_control_import(
            "alice",
            &chunk("chunk-0", 3, kit::chunk_action(&payload, chunk_bytes, 0)),
        )
        .unwrap();
    assert_eq!(staged.code, 0, "{}", staged.message);
    let restaged = store
        .execute_control_import(
            "alice",
            &chunk("chunk-0", 3, kit::chunk_action(&payload, chunk_bytes, 0)),
        )
        .unwrap();
    assert_eq!(
        restaged, staged,
        "identical restage under the same ordinal and id returns the stored decision"
    );
    // A new id with identical content under the staged ordinal is recorded.
    let same = store
        .execute_control_import(
            "alice",
            &chunk(
                "chunk-0-again",
                4,
                kit::chunk_action(&payload, chunk_bytes, 0),
            ),
        )
        .unwrap();
    assert_eq!(same.code, Code::AlreadyExists as u32, "{}", same.message);
    let recorded = store
        .control_import_decision("alice", &kit::key(), b"chunk-0-again")
        .unwrap();
    assert_eq!(recorded.code, Code::AlreadyExists as u32);
    // Differing content under the staged ordinal is a recorded refusal.
    let mut different = kit::chunk_action(&payload, chunk_bytes, 0);
    if let ImportAction::Chunk(bytes) = &mut different {
        bytes.bytes[0] ^= 1;
        bytes.sha256 = pipestream_search::source_authority::chunk_digest(&bytes.bytes);
    }
    let conflict = store
        .execute_control_import("alice", &chunk("chunk-0-other", 4, different))
        .unwrap();
    assert_eq!(conflict.code, Code::FailedPrecondition as u32);
    let recorded = store
        .control_import_decision("alice", &kit::key(), b"chunk-0-other")
        .unwrap();
    assert_eq!(recorded.code, Code::FailedPrecondition as u32);
}

/// Current permission gates retry disclosure: bob's stored Prepare decision
/// is unreachable while revoked, and regranting returns it without reapplying.
#[test]
fn revocation_blocks_retry_then_regrant_restores() {
    let dir = kit::TestDir::new("revocation");
    let (store, authority) = kit::create_store(&dir);
    let prepare = kit::source_command(
        &authority,
        &kit::owner_key(),
        "prepare-1",
        1,
        1,
        0,
        kit::prepare_action(b"installation-one"),
    );
    let original = store.execute("bob", &prepare).unwrap();
    assert_eq!(original.code, 0, "{}", original.message);
    assert_eq!(original.control_revision, 2);

    // alice revokes bob from test/books.
    let revoke = kit::source_command(
        &authority,
        &kit::key(),
        "revoke-bob",
        2,
        1,
        0,
        kit::replace_grants_action(vec![kit::grant("alice", kit::COLLECTION)]),
    );
    let revoked = store.execute("alice", &revoke).unwrap();
    assert_eq!(revoked.code, 0, "{}", revoked.message);
    assert_eq!(revoked.policy_revision, 2);

    // Bob's exact retry is PermissionDenied, and so is decision lookup: no
    // disclosure to a revoked actor even though the decision is stored.
    assert_eq!(
        store.execute("bob", &prepare).unwrap_err().code(),
        Code::PermissionDenied
    );
    assert_eq!(
        store
            .decision("bob", &kit::owner_key(), b"prepare-1")
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );

    // alice regrants bob.
    let regrant = kit::source_command(
        &authority,
        &kit::key(),
        "regrant-bob",
        3,
        2,
        0,
        kit::replace_grants_action(vec![
            kit::grant("alice", kit::COLLECTION),
            kit::grant("bob", kit::COLLECTION),
        ]),
    );
    let restored = store.execute("alice", &regrant).unwrap();
    assert_eq!(restored.code, 0, "{}", restored.message);
    assert_eq!(restored.policy_revision, 3);

    // The retry returns the ORIGINAL stored decision; nothing re-applies.
    let retry = store.execute("bob", &prepare).unwrap();
    assert_eq!(retry, original);
    assert_eq!(
        retry.control_revision, 2,
        "a stored-decision retry must not advance the control revision"
    );
    // The owner still reflects exactly one accepted Prepare.
    let owner = store.owner("alice", &kit::owner_key()).unwrap();
    assert_eq!(owner.control_revision, 2);
    assert_eq!(owner.ownership_generation, 1);
}

/// A ConfirmReady action never gains write authority through the general
/// command path, and a committed import admits no one the policy denies.
#[test]
fn ready_never_grants_write_authority() {
    let dir = kit::TestDir::new("ready-refusal");
    let (store, authority) = kit::create_store(&dir);
    let legacy = kit::legacy_plane(&dir, 2);
    let (retired, _request) = kit::retire(&store, &legacy, 1);
    let payload = kit::import_payload(&retired);

    // Commit a real import first so the revision arithmetic below is exact.
    kit::run_import(&store, &authority, &retired, &payload);

    // A real pending owner under a current Admin ...
    let prepare = kit::source_command(
        &authority,
        &kit::owner_key(),
        "prepare-1",
        6,
        1,
        0,
        kit::prepare_action(b"installation-one"),
    );
    let prepared = store.execute("alice", &prepare).unwrap();
    assert_eq!(prepared.code, 0, "{}", prepared.message);
    // ... still cannot confirm readiness through `execute`, even with a
    // syntactically complete command. `confirm_owner_ready` requires a
    // VerifiedOwnerCompletion, which only the hosting adapter constructs; the
    // fields are private, so this boundary is compile-time for this harness.
    let confirm = kit::source_command(
        &authority,
        &kit::owner_key(),
        "confirm-1",
        7,
        1,
        1,
        kit::confirm_ready_action(b"installation-one"),
    );
    assert_eq!(
        store.execute("alice", &confirm).unwrap_err().code(),
        Code::PermissionDenied
    );

    // Imported node state admits nothing by itself.
    for action in [
        pipestream_search::pb::AccessAction::Search,
        pipestream_search::pb::AccessAction::Admin,
    ] {
        assert_eq!(
            store
                .authorize("mallory", kit::COLLECTION, action)
                .unwrap_err()
                .code(),
            Code::PermissionDenied,
            "imported state must not authorize an ungranted principal"
        );
    }
    // The general path keeps refusing ConfirmReady after the import too.
    assert_eq!(
        store.execute("alice", &confirm).unwrap_err().code(),
        Code::PermissionDenied
    );
}

/// SIGKILL the worker after every durable import step (retire, begin, each of
/// three chunks, commit), then recover through the public API. The committed
/// snapshot digest must equal a no-crash baseline for every kill point.
#[test]
fn sigkill_recovery_at_every_import_boundary() {
    // No-crash baseline: same inputs, no kill.
    let baseline_dir = kit::TestDir::new("sigkill-baseline");
    let (baseline_store, baseline_authority) = kit::create_store(&baseline_dir);
    let baseline_legacy = kit::legacy_plane(&baseline_dir, 2);
    let (baseline_retired, _request) = kit::retire(&baseline_store, &baseline_legacy, 1);
    let baseline_payload = kit::import_payload(&baseline_retired);
    let (_receipt, chunk_count) = kit::run_import(
        &baseline_store,
        &baseline_authority,
        &baseline_retired,
        &baseline_payload,
    );
    let baseline_digest = kit::snapshot_digest(&baseline_store, "alice", &kit::key());
    eprintln!(
        "sigkill baseline: {} chunks, baseline digest recorded",
        chunk_count
    );
    drop(baseline_store);

    for k in 0..=5u32 {
        let dir = kit::TestDir::new(&format!("sigkill-k{k}"));
        let mut worker = kit::KillOnDrop(kit::spawn_worker(
            "sigkill_import_worker",
            &[
                (kit::WORKER_ENV, "1"),
                (kit::WORKER_DIR_ENV, dir.path().to_str().unwrap()),
                (kit::WORKER_ARM_ENV, &k.to_string()),
            ],
        ));
        kit::wait_marker(&dir, k, MARKER_TIMEOUT);
        kit::kill9(worker.child());

        // Recovery through the public API only.
        let authority = kit::identity(kit::SEED);
        let store = kit::open_store(&dir, &authority);
        let request = LegacyControlRetirementRequest::decode(
            std::fs::read(dir.request()).unwrap().as_slice(),
        )
        .unwrap();
        let retired = store
            .recover_legacy_retirement("alice", &request, &dir.legacy())
            .unwrap();
        let payload = kit::import_payload(&retired);
        let (chunk_bytes, chunk_count) = kit::plan_three_chunks(payload.len());

        // Re-issue the whole sequence: exact retries return stored decisions,
        // missing steps apply fresh. `begin` first for k = 0 (killed after
        // retire, before begin).
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
        assert_eq!(begin.code, 0, "k={k}: {}", begin.message);
        let mut revision = 2u64;
        for ordinal in 0..chunk_count {
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
            assert_eq!(
                decision.code, 0,
                "k={k} chunk {ordinal}: {}",
                decision.message
            );
            revision += 1;
        }
        let commit = store
            .execute_control_import(
                "alice",
                &kit::import_command(
                    &authority,
                    "commit",
                    revision,
                    kit::WORKFLOW,
                    kit::commit_action(),
                ),
            )
            .unwrap();
        assert_eq!(commit.code, 0, "k={k}: {}", commit.message);
        let receipt = commit.receipt.clone().unwrap();

        // The recovered state equals the no-crash baseline exactly.
        let snapshot = store.control_snapshot("alice", &kit::key()).unwrap();
        assert_eq!(
            snapshot.digest, baseline_digest,
            "k={k}: crash recovery changed the committed snapshot digest"
        );
        assert_eq!(receipt.control_revision, snapshot.control_revision);

        // The exact retry of the final step returns the stored receipt
        // decision (for k = 5 the commit was already durable at the kill).
        let retry = store
            .execute_control_import(
                "alice",
                &kit::import_command(
                    &authority,
                    "commit",
                    revision,
                    kit::WORKFLOW,
                    kit::commit_action(),
                ),
            )
            .unwrap();
        assert_eq!(retry.code, 0, "k={k}: {}", retry.message);
        let retry_receipt = retry.receipt.unwrap();
        assert_eq!(retry_receipt.control_revision, receipt.control_revision);
        assert_eq!(
            retry_receipt.payload_sha256, receipt.payload_sha256,
            "k={k}: commit retry did not return the stored receipt"
        );

        // The retired legacy plane can never be restarted as a writer. The
        // recovered holder keeps the legacy lock; release it first.
        drop(retired);
        let error = match DurableControlPlane::open_existing(dir.legacy(), kit::control_policy()) {
            Ok(_) => panic!("k={k}: the retired legacy plane must refuse to reopen"),
            Err(error) => error,
        };
        assert!(
            error.contains("durably retired"),
            "k={k}: unexpected legacy reopen error: {error}"
        );
        eprintln!("sigkill k={k}: recovered, digest matches baseline");
    }
}

/// Worker for `sigkill_recovery_at_every_import_boundary`; the body lives
/// in `kit::sigkill_import_worker_body` so the planner leg can drive the
/// same crash schedule. Early-returns unless `PSEARCH_ADV_WORKER` is set.
#[test]
fn sigkill_import_worker() {
    kit::sigkill_import_worker_body();
}

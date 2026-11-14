//! Adversarial integration suite for the source-authority control plane
//! (slice 1; `docs/control-authority-test-harness.md`).
//!
//! Public API only: the collection control-state install, exact-retry and
//! revocation semantics, READY refusal and activation refusals. State
//! equivalence is asserted through the 32-byte
//! `ControlCollectionSnapshot.digest`.

mod control_adversarial;

use control_adversarial::kit;
use pipestream_search::pb::storage::{LogicalSourceOwner, PreparedSourceOwnerPhase};
use tonic::Code;

/// One install on a 2-route collection; reopen is digest-stable and a
/// second install for the resource is a recorded refusal.
#[test]
fn install_commit_reopen_snapshot_digest_stable() {
    let dir = kit::TestDir::new("commit-reopen");
    let (store, authority) = kit::create_store(&dir);
    let receipt = kit::run_install(&store, &authority, kit::install_state(2));
    assert_eq!(receipt.routes, 2);
    assert_eq!(receipt.control_revision, 2);

    let before = kit::snapshot_digest(&store, "alice", &kit::key());
    let snapshot = store.control_snapshot("alice", &kit::key()).unwrap();
    assert_eq!(snapshot.control_revision, 2);
    drop(store);

    let reopened = kit::open_store(&dir, &authority);
    let after = kit::snapshot_digest(&reopened, "alice", &kit::key());
    assert_eq!(
        before, after,
        "reopen changed the committed snapshot digest"
    );

    // Nothing else may install into this resource again.
    let again = reopened
        .install_control(
            "alice",
            &kit::install_command(&authority, "install-2", 2, kit::install_state(2)),
        )
        .unwrap();
    assert_eq!(again.code, Code::AlreadyExists as u32, "{}", again.message);
}

/// Exact retry returns the stored decision without reapplying; the same id
/// with changed content refuses; on both the source and the install path.
#[test]
fn exact_retry_and_changed_content() {
    let dir = kit::TestDir::new("exact-retry");
    let (store, authority) = kit::create_store(&dir);

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

    // Install path on the collection key: the store revision is now 2.
    let install = kit::install_command(&authority, "install", 2, kit::install_state(2));
    let installed = store.install_control("alice", &install).unwrap();
    assert_eq!(installed.code, 0, "{}", installed.message);
    assert_eq!(store.install_control("alice", &install).unwrap(), installed);
    let changed = kit::install_command(&authority, "install", 2, kit::install_state(3));
    assert_eq!(
        store.install_control("alice", &changed).unwrap_err().code(),
        Code::FailedPrecondition
    );
    // A stale install under a new id is a recorded refusal.
    let stale = store
        .install_control(
            "alice",
            &kit::install_command(&authority, "install-stale", 2, kit::install_state(2)),
        )
        .unwrap();
    assert_eq!(stale.code, Code::FailedPrecondition as u32);
    assert_eq!(
        store
            .control_install_decision("alice", &kit::key(), b"install-stale")
            .unwrap(),
        stale
    );
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
/// command path, and installed control state admits no one the policy denies.
#[test]
fn ready_never_grants_write_authority() {
    let dir = kit::TestDir::new("ready-refusal");
    let (store, authority) = kit::create_store(&dir);
    // Install real control state first so the revision arithmetic below is exact.
    kit::run_install(&store, &authority, kit::install_state(2));

    // A real pending owner under a current Admin ...
    let prepare = kit::source_command(
        &authority,
        &kit::owner_key(),
        "prepare-1",
        2,
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
        3,
        1,
        1,
        kit::confirm_ready_action(b"installation-one"),
    );
    assert_eq!(
        store.execute("alice", &confirm).unwrap_err().code(),
        Code::PermissionDenied
    );

    // Installed node state admits nothing by itself.
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
            "installed state must not authorize an ungranted principal"
        );
    }
    // The general path keeps refusing ConfirmReady after the install too.
    assert_eq!(
        store.execute("alice", &confirm).unwrap_err().code(),
        Code::PermissionDenied
    );
}

/// Activation is the committed fence and only follows READY
/// (docs/source-owner-admission.md). From tests READY is unreachable (the
/// readiness proof is pub(crate)), so every Activate here is a refusal: on
/// a PREPARED owner, on a CANCELLED owner, with the wrong workflow id, and
/// on a missing owner — and none of them records an activation row.
#[test]
fn activate_refusals_before_ready() {
    let dir = kit::TestDir::new("activate-refusals");
    let (store, authority) = kit::create_store(&dir);
    let owner_key = kit::owner_key();
    let workflow = b"owner-wf-1";

    // Prepare the owner: generation 1, control revision 2.
    let prepare = store
        .execute(
            "alice",
            &kit::source_command(
                &authority,
                &owner_key,
                "prepare",
                1,
                1,
                0,
                kit::prepare_action(workflow),
            ),
        )
        .unwrap();
    assert_eq!(prepare.code, 0, "{}", prepare.message);

    // A non-admin is refused before anything is validated or recorded.
    let error = store
        .execute(
            "mallory",
            &kit::source_command(
                &authority,
                &owner_key,
                "mallory-activate",
                2,
                1,
                1,
                kit::activate_action(workflow),
            ),
        )
        .unwrap_err();
    assert_eq!(error.code(), Code::PermissionDenied);

    // PREPARED is not READY: recorded refusal, no fence.
    let early = store
        .execute(
            "alice",
            &kit::source_command(
                &authority,
                &owner_key,
                "activate-prepared",
                2,
                1,
                1,
                kit::activate_action(workflow),
            ),
        )
        .unwrap();
    assert_eq!(
        early.code,
        Code::FailedPrecondition as u32,
        "{}",
        early.message
    );
    // The wrong workflow on the prepared owner is the same recorded refusal.
    let wrong = store
        .execute(
            "alice",
            &kit::source_command(
                &authority,
                &owner_key,
                "activate-wrong-wf",
                2,
                1,
                1,
                kit::activate_action(b"owner-wf-2"),
            ),
        )
        .unwrap();
    assert_eq!(
        wrong.code,
        Code::FailedPrecondition as u32,
        "{}",
        wrong.message
    );
    let owner = store.owner("alice", &owner_key).unwrap();
    assert_eq!(owner.phase, PreparedSourceOwnerPhase::Prepared as i32);
    assert!(
        owner.activation.is_none(),
        "a refused activation must not record a fence"
    );
    assert_eq!(owner.ownership_generation, 1);

    // Cancel; activation on the CANCELLED owner refuses the same way.
    let cancel = store
        .execute(
            "alice",
            &kit::source_command(
                &authority,
                &owner_key,
                "cancel",
                2,
                1,
                1,
                kit::cancel_action(workflow),
            ),
        )
        .unwrap();
    assert_eq!(cancel.code, 0, "{}", cancel.message);
    let late = store
        .execute(
            "alice",
            &kit::source_command(
                &authority,
                &owner_key,
                "activate-cancelled",
                3,
                1,
                1,
                kit::activate_action(workflow),
            ),
        )
        .unwrap();
    assert_eq!(
        late.code,
        Code::FailedPrecondition as u32,
        "{}",
        late.message
    );
    let owner = store.owner("alice", &owner_key).unwrap();
    assert_eq!(owner.phase, PreparedSourceOwnerPhase::Cancelled as i32);
    assert!(
        owner.activation.is_none(),
        "no activation on a cancelled owner"
    );

    // A missing owner is a recorded NotFound.
    let ghost = LogicalSourceOwner {
        owner_id: b"ghost-owner".to_vec(),
        ..kit::key()
    };
    let missing = store
        .execute(
            "alice",
            &kit::source_command(
                &authority,
                &ghost,
                "activate-ghost",
                3,
                1,
                0,
                kit::activate_action(workflow),
            ),
        )
        .unwrap();
    assert_eq!(missing.code, Code::NotFound as u32, "{}", missing.message);
    eprintln!("activate: prepared/cancelled/missing/wrong-workflow/non-admin all refuse, no fence recorded");
}

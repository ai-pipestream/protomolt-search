use super::*;
use crate::authorization::AccessPermit;
use crate::pb::{AccessAction, AccessPolicy, CollectionGrant, CollectionResource};
use std::path::PathBuf;
use std::sync::mpsc;
use std::time::Duration;
use tonic::Code;

struct Directory(PathBuf);

impl Directory {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "source-admission-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn authority(&self) -> PathBuf {
        self.0.join("authority.redb")
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn identity(seed: u8) -> SourceAuthorityIdentity {
    SourceAuthorityIdentity {
        format_version: 1,
        group_id: vec![seed; 16],
        authority_incarnation: vec![seed.wrapping_add(1); 16],
    }
}

fn grant(principal: &str, actions: &[AccessAction]) -> CollectionGrant {
    CollectionGrant {
        principal: principal.into(),
        workspace: "workspace-a".into(),
        collection: "books".into(),
        actions: actions.iter().map(|a| *a as i32).collect(),
        ..Default::default()
    }
}

fn policy(alice_ingests: bool) -> AccessPolicy {
    let mut alice = vec![AccessAction::Admin];
    if alice_ingests {
        alice.push(AccessAction::Ingest);
    }
    AccessPolicy {
        format_version: 1,
        revision: 1,
        resources: vec![CollectionResource {
            workspace: "workspace-a".into(),
            collection: "books".into(),
        }],
        grants: vec![grant("alice", &alice), grant("bob", &[AccessAction::Admin])],
    }
}

fn limits() -> SourceAuthorityLimits {
    SourceAuthorityLimits {
        max_owners: 16,
        max_decisions: 64,
        max_payload_bytes: 16 << 20,
        max_command_bytes: 64 << 10,
    }
}

fn key() -> LogicalSourceOwner {
    LogicalSourceOwner {
        workspace: "workspace-a".into(),
        collection: "books".into(),
        owner_id: b"phone-owner".to_vec(),
    }
}

fn target() -> SourceStorageTarget {
    SourceStorageTarget {
        node_id: "server-a".into(),
        storage_incarnation: vec![41; 16],
        history_id: vec![42; 16],
        residency: SourceResidency::Server as i32,
        resident_device_id: String::new(),
    }
}

fn command(
    authority: &SourceAuthorityIdentity,
    id: &str,
    control: u64,
    generation: u64,
    action: Action,
) -> SourceAuthorityCommand {
    SourceAuthorityCommand {
        format_version: 1,
        authority: Some(authority.clone()),
        key: Some(key()),
        command_id: id.as_bytes().to_vec(),
        expected_control_revision: control,
        expected_policy_revision: 1,
        expected_ownership_generation: generation,
        action: Some(action),
    }
}

fn prepare(authority: &SourceAuthorityIdentity) -> SourceAuthorityCommand {
    command(
        authority,
        "prepare",
        1,
        0,
        Action::Prepare(PrepareSourceOwner {
            workflow_id: b"installation-one".to_vec(),
            target: Some(target()),
        }),
    )
}

fn grants_command(
    authority: &SourceAuthorityIdentity,
    id: &str,
    control: u64,
    policy_revision: u64,
    grants: Vec<CollectionGrant>,
) -> SourceAuthorityCommand {
    SourceAuthorityCommand {
        format_version: 1,
        authority: Some(authority.clone()),
        key: Some(LogicalSourceOwner {
            workspace: "workspace-a".into(),
            collection: "books".into(),
            owner_id: Vec::new(),
        }),
        command_id: id.as_bytes().to_vec(),
        expected_control_revision: control,
        expected_policy_revision: policy_revision,
        expected_ownership_generation: 0,
        action: Some(Action::ReplaceGrants(ReplaceSourceCollectionGrants {
            grants,
        })),
    }
}

fn binding(
    authority: &SourceAuthorityIdentity,
    preparation: &PreparedSourceOwner,
    sequence: u64,
) -> SourceManagedBinding {
    SourceManagedBinding {
        format_version: 1,
        authority: Some(authority.clone()),
        preparation: Some(preparation.clone()),
        bound_at_sequence: sequence,
    }
}

fn confirm(
    authority: &SourceAuthorityIdentity,
    id: &str,
    control: u64,
    completion: SourceOwnerCompletion,
) -> SourceAuthorityCommand {
    command(
        authority,
        id,
        control,
        1,
        Action::ConfirmReady(ConfirmSourceOwnerReady {
            workflow_id: b"installation-one".to_vec(),
            completion: Some(completion),
        }),
    )
}

#[test]
fn the_store_is_a_workspace_authorizer_with_one_ordered_policy_history() {
    let dir = Directory::new("authorizer");
    let authority = identity(7);
    let store =
        SourceAuthorityStore::create(&dir.authority(), &authority, &policy(true), &limits())
            .unwrap();
    let shared: Arc<dyn Authorizer> = Arc::new(store.clone());
    assert_eq!(*shared.subscribe().borrow(), 1);
    let permit =
        AccessPermit::acquire(shared.clone(), "alice", "books", AccessAction::Ingest).unwrap();
    assert_eq!(permit.decision().policy_revision, 1);
    assert_eq!(permit.decision().workspace, "workspace-a");
    assert_eq!(
        AccessPermit::acquire(shared.clone(), "carol", "books", AccessAction::Search)
            .err()
            .unwrap()
            .code(),
        Code::PermissionDenied
    );
    assert_eq!(
        AccessPermit::acquire(shared.clone(), "alice", "music", AccessAction::Search)
            .err()
            .unwrap()
            .code(),
        Code::PermissionDenied
    );

    // An admitted operation holds the fence: a revocation committed by another
    // administrator waits for it and publishes the new revision afterwards.
    let pin = permit.pin().unwrap();
    assert_eq!(pin.decision(), permit.decision());
    let (result_tx, result_rx) = mpsc::channel();
    let revoking = {
        let store = store.clone();
        let authority = authority.clone();
        std::thread::spawn(move || {
            let revoke = grants_command(
                &authority,
                "bob-revokes-ingest",
                1,
                1,
                vec![
                    grant("alice", &[AccessAction::Admin]),
                    grant("bob", &[AccessAction::Admin]),
                ],
            );
            result_tx.send(store.execute("bob", &revoke)).unwrap();
        })
    };
    std::thread::sleep(Duration::from_millis(200));
    assert!(
        matches!(result_rx.try_recv(), Err(mpsc::TryRecvError::Empty)),
        "revocation completed while an operation remained admitted"
    );
    assert_eq!(*shared.subscribe().borrow(), 1);
    drop(pin);
    let decision = result_rx
        .recv_timeout(Duration::from_secs(10))
        .unwrap()
        .unwrap();
    revoking.join().unwrap();
    assert_eq!(decision.code, 0, "{}", decision.message);
    assert_eq!(decision.policy_revision, 2);
    assert_eq!(*shared.subscribe().borrow(), 2);
    assert_eq!(permit.check().err().unwrap().code(), Code::PermissionDenied);
    assert_eq!(permit.pin().err().unwrap().code(), Code::PermissionDenied);
    assert_eq!(
        AccessPermit::acquire(shared.clone(), "alice", "books", AccessAction::Ingest)
            .err()
            .unwrap()
            .code(),
        Code::PermissionDenied
    );
    let admin =
        AccessPermit::acquire(shared.clone(), "alice", "books", AccessAction::Admin).unwrap();
    assert_eq!(admin.decision().policy_revision, 2);
    drop(admin.pin().unwrap());

    // Reopen publishes the committed revision, not a default.
    drop(permit);
    drop(admin);
    drop(shared);
    drop(store);
    let reopened = SourceAuthorityStore::open(&dir.authority(), &authority).unwrap();
    assert_eq!(*reopened.subscribe().borrow(), 2);
    assert_eq!(
        reopened
            .authorize("alice", "books", AccessAction::Admin)
            .unwrap()
            .policy_revision,
        2
    );
    assert_eq!(
        reopened.admission("").err().unwrap().code(),
        Code::InvalidArgument
    );
}

#[test]
fn readiness_needs_the_held_binding_and_is_terminal_for_the_generation() {
    let dir = Directory::new("readiness");
    let authority = identity(7);
    let store =
        SourceAuthorityStore::create(&dir.authority(), &authority, &policy(false), &limits())
            .unwrap();
    let preparation = store
        .execute("alice", &prepare(&authority))
        .unwrap()
        .owner
        .unwrap();
    let verified =
        VerifiedOwnerCompletion::from_binding(&binding(&authority, &preparation, 5)).unwrap();
    let completion = verified.completion().clone();
    assert_eq!(completion.bound_at_sequence, 5);
    assert_eq!(completion.history_id, target().history_id);

    // The general path never confirms readiness, whatever the bytes say.
    assert_eq!(
        store
            .execute("alice", &confirm(&authority, "c", 2, completion.clone()))
            .err()
            .unwrap()
            .code(),
        Code::PermissionDenied
    );
    // Bytes that differ from the held binding's completion are not admitted.
    let mut altered = completion.clone();
    altered.bound_at_sequence = 6;
    assert_eq!(
        store
            .confirm_owner_ready("alice", &confirm(&authority, "c", 2, altered), &verified)
            .err()
            .unwrap()
            .code(),
        Code::PermissionDenied
    );
    // A binding of another preparation or another authority admits nothing.
    let mut other_preparation = preparation.clone();
    other_preparation.workflow_id = b"installation-two".to_vec();
    let foreign =
        VerifiedOwnerCompletion::from_binding(&binding(&authority, &other_preparation, 5)).unwrap();
    assert_eq!(
        store
            .confirm_owner_ready(
                "alice",
                &confirm(&authority, "c", 2, foreign.completion().clone()),
                &foreign
            )
            .err()
            .unwrap()
            .code(),
        Code::FailedPrecondition
    );
    let elsewhere =
        VerifiedOwnerCompletion::from_binding(&binding(&identity(9), &preparation, 5)).unwrap();
    assert_eq!(
        store
            .confirm_owner_ready(
                "alice",
                &confirm(&authority, "c", 2, completion.clone()),
                &elsewhere
            )
            .err()
            .unwrap()
            .code(),
        Code::FailedPrecondition
    );
    assert_eq!(
        VerifiedOwnerCompletion::from_binding(&SourceManagedBinding::default())
            .err()
            .unwrap()
            .code(),
        Code::InvalidArgument
    );
    // Another administrator cannot confirm with the actor's binding either:
    // the command is his, the decision is scoped to him, and it still needs
    // the same held binding — which is fine; the binding is the proof, not
    // the actor. The stale revision refuses first.
    let stale = store
        .confirm_owner_ready(
            "bob",
            &confirm(&authority, "bob-stale", 1, completion.clone()),
            &verified,
        )
        .unwrap();
    assert_eq!(stale.code, Code::FailedPrecondition as u32);
    assert_eq!(
        store.owner("alice", &key()).unwrap().phase,
        PreparedSourceOwnerPhase::Prepared as i32
    );

    let ready = store
        .confirm_owner_ready(
            "alice",
            &confirm(&authority, "c", 2, completion.clone()),
            &verified,
        )
        .unwrap();
    assert_eq!(ready.code, 0, "{}", ready.message);
    let owner = ready.owner.clone().unwrap();
    assert_eq!(owner.phase, PreparedSourceOwnerPhase::Ready as i32);
    assert_eq!(owner.ownership_generation, 1);
    assert_eq!(owner.control_revision, 3);
    assert_eq!(
        owner.readiness,
        Some(SourceOwnerReadiness {
            format_version: 1,
            completion: Some(completion.clone()),
        })
    );
    assert_eq!(store.owner("alice", &key()).unwrap(), owner);
    // Exact retry, with or without the holder, answers from the record.
    assert_eq!(
        store
            .confirm_owner_ready(
                "alice",
                &confirm(&authority, "c", 2, completion.clone()),
                &verified
            )
            .unwrap(),
        ready
    );
    assert_eq!(store.decision("alice", &key(), b"c").unwrap(), ready);
    // READY is terminal for this generation.
    let cancel = store
        .execute(
            "alice",
            &command(
                &authority,
                "cancel",
                3,
                1,
                Action::Cancel(CancelPreparedSourceOwner {
                    workflow_id: b"installation-one".to_vec(),
                }),
            ),
        )
        .unwrap();
    assert_eq!(cancel.code, Code::FailedPrecondition as u32);
    let mut again = prepare(&authority);
    again.command_id = b"prepare-again".to_vec();
    again.expected_control_revision = 3;
    again.expected_ownership_generation = 1;
    if let Some(Action::Prepare(request)) = again.action.as_mut() {
        request.workflow_id = b"installation-two".to_vec();
    }
    let again = store.execute("alice", &again).unwrap();
    assert_eq!(again.code, Code::FailedPrecondition as u32);
    // A new confirmation id under the same binding is refused at admission:
    // the binding's preparation is no longer the committed owner. Nothing is
    // recorded for it.
    assert_eq!(
        store
            .confirm_owner_ready(
                "alice",
                &confirm(&authority, "c2", 3, completion.clone()),
                &verified
            )
            .err()
            .unwrap()
            .code(),
        Code::FailedPrecondition
    );
    assert_eq!(
        store.decision("alice", &key(), b"c2").err().unwrap().code(),
        Code::NotFound
    );
    drop(store);
    let reopened = SourceAuthorityStore::open(&dir.authority(), &authority).unwrap();
    assert_eq!(reopened.owner("alice", &key()).unwrap(), owner);

    // A READY row without its completion is corruption, not a default.
    {
        let tx = reopened.inner.database.begin_write().unwrap();
        {
            let mut owners = tx.open_table(OWNERS).unwrap();
            let mut stripped = owner.clone();
            stripped.readiness = None;
            owners
                .insert(
                    key().encode_to_vec().as_slice(),
                    stripped.encode_to_vec().as_slice(),
                )
                .unwrap();
        }
        tx.commit().unwrap();
    }
    drop(reopened);
    assert_eq!(
        SourceAuthorityStore::open(&dir.authority(), &authority)
            .err()
            .unwrap()
            .code(),
        Code::DataLoss
    );
}

#[test]
fn revocation_between_preparation_and_readiness_is_enforced() {
    let dir = Directory::new("revoked");
    let authority = identity(7);
    let store =
        SourceAuthorityStore::create(&dir.authority(), &authority, &policy(false), &limits())
            .unwrap();
    let preparation = store
        .execute("alice", &prepare(&authority))
        .unwrap()
        .owner
        .unwrap();
    let verified =
        VerifiedOwnerCompletion::from_binding(&binding(&authority, &preparation, 1)).unwrap();
    let revoke = grants_command(
        &authority,
        "bob-revokes",
        2,
        1,
        vec![grant("bob", &[AccessAction::Admin])],
    );
    assert_eq!(store.execute("bob", &revoke).unwrap().code, 0);
    let mut confirm = confirm(&authority, "c", 3, verified.completion().clone());
    confirm.expected_policy_revision = 2;
    assert_eq!(
        store
            .confirm_owner_ready("alice", &confirm, &verified)
            .err()
            .unwrap()
            .code(),
        Code::PermissionDenied
    );
    // The admission of a revoked actor is a fence that admits nothing.
    let admission = store.admission("alice").unwrap();
    assert_eq!(
        admission.prepared_owner(&preparation).err().unwrap().code(),
        Code::PermissionDenied
    );
    assert_eq!(
        admission
            .authorize("books", AccessAction::Admin)
            .err()
            .unwrap()
            .code(),
        Code::PermissionDenied
    );
    drop(admission);
    let restore = grants_command(
        &authority,
        "bob-restores",
        3,
        2,
        vec![
            grant("alice", &[AccessAction::Admin]),
            grant("bob", &[AccessAction::Admin]),
        ],
    );
    assert_eq!(store.execute("bob", &restore).unwrap().code, 0);
    let admission = store.admission("alice").unwrap();
    admission.prepared_owner(&preparation).unwrap();
    drop(admission);
    confirm.expected_control_revision = 4;
    confirm.expected_policy_revision = 3;
    let ready = store
        .confirm_owner_ready("alice", &confirm, &verified)
        .unwrap();
    assert_eq!(ready.code, 0, "{}", ready.message);
    assert_eq!(
        ready.owner.unwrap().phase,
        PreparedSourceOwnerPhase::Ready as i32
    );
}

#[test]
fn readiness_exit_worker() {
    let Some(mode) = std::env::var_os("PSEARCH_READINESS_EXIT_FAULT") else {
        return;
    };
    let path = PathBuf::from(std::env::var_os("PSEARCH_READINESS_PATH").unwrap());
    let authority = identity(7);
    let store = SourceAuthorityStore::create(&path, &authority, &policy(false), &limits()).unwrap();
    let preparation = store
        .execute("alice", &prepare(&authority))
        .unwrap()
        .owner
        .unwrap();
    let verified =
        VerifiedOwnerCompletion::from_binding(&binding(&authority, &preparation, 1)).unwrap();
    *store.inner.fault.lock().unwrap() = Some(match mode.to_str().unwrap() {
        "before" => Fault::ExitBeforeCommit,
        "after" => Fault::ExitAfterCommit,
        other => panic!("unknown exit fault {other}"),
    });
    store
        .confirm_owner_ready(
            "alice",
            &confirm(&authority, "c", 2, verified.completion().clone()),
            &verified,
        )
        .unwrap();
    panic!("exit fault did not terminate the worker");
}

#[test]
fn abrupt_exit_around_the_readiness_commit_recovers_prepared_or_ready() {
    for mode in ["before", "after"] {
        let dir = Directory::new(&format!("readiness-exit-{mode}"));
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("source_authority::admission_tests::readiness_exit_worker")
            .arg("--nocapture")
            .env("PSEARCH_READINESS_EXIT_FAULT", mode)
            .env("PSEARCH_READINESS_PATH", dir.authority())
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(87));
        let authority = identity(7);
        let store = SourceAuthorityStore::open(&dir.authority(), &authority).unwrap();
        let held = store.owner("alice", &key()).unwrap();
        let mut preparation = held.clone();
        if mode == "before" {
            assert_eq!(held.phase, PreparedSourceOwnerPhase::Prepared as i32);
            assert_eq!(
                store.decision("alice", &key(), b"c").err().unwrap().code(),
                Code::NotFound
            );
        } else {
            assert_eq!(held.phase, PreparedSourceOwnerPhase::Ready as i32);
            preparation.phase = PreparedSourceOwnerPhase::Prepared as i32;
            preparation.readiness = None;
            preparation.control_revision = 2;
            preparation.last_command.as_mut().unwrap().command_id = b"prepare".to_vec();
        }
        // The owner's binding is the same in both outcomes; the confirmation is
        // issued again and either commits or answers from the record.
        let verified =
            VerifiedOwnerCompletion::from_binding(&binding(&authority, &preparation, 1)).unwrap();
        let decision = store
            .confirm_owner_ready(
                "alice",
                &confirm(&authority, "c", 2, verified.completion().clone()),
                &verified,
            )
            .unwrap();
        assert_eq!(decision.code, 0, "{mode}: {}", decision.message);
        assert_eq!(decision.control_revision, 3);
        let ready = store.owner("alice", &key()).unwrap();
        assert_eq!(ready.phase, PreparedSourceOwnerPhase::Ready as i32);
        assert_eq!(
            ready.readiness.unwrap().completion.unwrap(),
            *verified.completion()
        );
        drop(store);
        SourceAuthorityStore::open(&dir.authority(), &authority).unwrap();
    }
}

#[test]
fn a_committed_readiness_command_replays_without_the_binding() {
    let dir = Directory::new("readiness-replay");
    let authority = identity(7);
    let store =
        SourceAuthorityStore::create(&dir.authority(), &authority, &policy(false), &limits())
            .unwrap();
    let preparation = store
        .execute("alice", &prepare(&authority))
        .unwrap()
        .owner
        .unwrap();
    let verified =
        VerifiedOwnerCompletion::from_binding(&binding(&authority, &preparation, 3)).unwrap();
    let confirmation = confirm(&authority, "c", 2, verified.completion().clone());
    let ready = store
        .confirm_owner_ready("alice", &confirmation, &verified)
        .unwrap();
    assert_eq!(ready.code, 0);

    // The same two committed commands applied to a fresh replica of the same
    // identity, with no managed binding in reach, yield the same owner row.
    let replica_dir = Directory::new("readiness-replica");
    let replica = SourceAuthorityStore::create(
        &replica_dir.authority(),
        &authority,
        &policy(false),
        &limits(),
    )
    .unwrap();
    assert_eq!(
        replica
            .replay_command("alice", &prepare(&authority))
            .unwrap()
            .owner
            .unwrap(),
        preparation
    );
    assert_eq!(
        replica.replay_command("alice", &confirmation).unwrap(),
        ready
    );
    assert_eq!(
        replica.owner("alice", &key()).unwrap(),
        store.owner("alice", &key()).unwrap()
    );
    // The general path still refuses the same bytes.
    assert_eq!(
        replica
            .execute("alice", &confirmation)
            .err()
            .unwrap()
            .code(),
        Code::PermissionDenied
    );
}

fn activate(authority: &SourceAuthorityIdentity, id: &str, control: u64) -> SourceAuthorityCommand {
    command(
        authority,
        id,
        control,
        1,
        Action::Activate(ActivateSourceOwner {
            workflow_id: b"installation-one".to_vec(),
        }),
    )
}

#[test]
fn activation_commits_the_fence_only_from_ready_and_never_from_a_lease() {
    let dir = Directory::new("activation");
    let authority = identity(7);
    let store =
        SourceAuthorityStore::create(&dir.authority(), &authority, &policy(false), &limits())
            .unwrap();
    let preparation = store
        .execute("alice", &prepare(&authority))
        .unwrap()
        .owner
        .unwrap();
    // PREPARED is not READY: recorded refusal, no fence.
    let early = store
        .execute("alice", &activate(&authority, "early", 2))
        .unwrap();
    assert_eq!(early.code, Code::FailedPrecondition as u32);
    assert!(store.owner("alice", &key()).unwrap().activation.is_none());
    let verified =
        VerifiedOwnerCompletion::from_binding(&binding(&authority, &preparation, 1)).unwrap();
    let ready = store
        .confirm_owner_ready(
            "alice",
            &confirm(&authority, "c", 2, verified.completion().clone()),
            &verified,
        )
        .unwrap();
    assert_eq!(ready.code, 0);
    // The target carries no lease; nothing but the committed READY fact and
    // a current Admin's activation allocates the epoch.
    let active = store
        .execute("alice", &activate(&authority, "activate", 3))
        .unwrap();
    assert_eq!(active.code, 0, "{}", active.message);
    let owner = active.owner.clone().unwrap();
    assert_eq!(owner.phase, PreparedSourceOwnerPhase::Active as i32);
    assert_eq!(
        owner.activation,
        Some(SourceOwnerActivation {
            format_version: 1,
            write_epoch: 1,
            activated_control_revision: 4,
        })
    );
    assert_eq!(
        store
            .execute("alice", &activate(&authority, "activate", 3))
            .unwrap(),
        active
    );
    let twice = store
        .execute("alice", &activate(&authority, "activate-2", 4))
        .unwrap();
    assert_eq!(twice.code, Code::FailedPrecondition as u32);
    // The admission sees the fence; a stale epoch or a wrong action refuses.
    let admission = store.admission("alice").unwrap();
    assert_eq!(
        admission
            .admit_write(&key(), 1, AccessAction::Admin)
            .unwrap()
            .principal,
        "alice"
    );
    assert_eq!(
        admission
            .admit_write(&key(), 2, AccessAction::Admin)
            .err()
            .unwrap()
            .code(),
        Code::FailedPrecondition
    );
    assert_eq!(
        admission
            .admit_write(&key(), 1, AccessAction::Ingest)
            .err()
            .unwrap()
            .code(),
        Code::PermissionDenied
    );
    drop(admission);
    // Replay on a fresh replica reproduces the fence; reopen validates it and
    // a tampered epoch refuses the open.
    let replica_dir = Directory::new("activation-replica");
    let replica = SourceAuthorityStore::create(
        &replica_dir.authority(),
        &authority,
        &policy(false),
        &limits(),
    )
    .unwrap();
    replica
        .replay_command("alice", &prepare(&authority))
        .unwrap();
    replica
        .replay_command("alice", &activate(&authority, "early", 2))
        .unwrap();
    replica
        .replay_command(
            "alice",
            &confirm(&authority, "c", 2, verified.completion().clone()),
        )
        .unwrap();
    assert_eq!(
        replica
            .replay_command("alice", &activate(&authority, "activate", 3))
            .unwrap(),
        active
    );
    drop(store);
    let reopened = SourceAuthorityStore::open(&dir.authority(), &authority).unwrap();
    assert_eq!(reopened.owner("alice", &key()).unwrap(), owner);
    {
        let tx = reopened.inner.database.begin_write().unwrap();
        {
            let mut owners = tx.open_table(OWNERS).unwrap();
            let mut forged = owner.clone();
            forged.activation.as_mut().unwrap().write_epoch = 2;
            owners
                .insert(
                    key().encode_to_vec().as_slice(),
                    forged.encode_to_vec().as_slice(),
                )
                .unwrap();
        }
        tx.commit().unwrap();
    }
    drop(reopened);
    assert_eq!(
        SourceAuthorityStore::open(&dir.authority(), &authority)
            .err()
            .unwrap()
            .code(),
        Code::DataLoss
    );
}

use super::*;
use crate::authorization::AccessPermit;
use crate::pb::{AccessAction, AccessPolicy, CollectionGrant, CollectionResource};
use crate::test_support::ForkGuarded;
use std::path::PathBuf;
use std::sync::mpsc;
use std::time::Duration;
use tonic::Code;

mod activation;
mod authorizer;
mod readiness;

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
        format_version: 3,
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
            .status_guarded()
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

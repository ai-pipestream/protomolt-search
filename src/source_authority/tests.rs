use super::*;
use crate::pb::{AccessAction, AccessPolicy, CollectionGrant, CollectionResource};
use crate::test_support::ForkGuarded;
use prost::Message;
use redb::{ReadableTable, ReadableTableMetadata};
use std::{path::PathBuf, sync::Arc};
use tonic::Code;

mod faults;
mod lifecycle;
mod policy;

struct Directory(PathBuf);
impl Directory {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "source-authority-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
    fn store(&self) -> PathBuf {
        self.0.join("authority.redb")
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

fn identity(seed: u8) -> SourceAuthorityIdentity {
    SourceAuthorityIdentity {
        format_version: 1,
        group_id: vec![seed; 16],
        authority_incarnation: vec![seed.wrapping_add(1); 16],
    }
}

fn limits() -> SourceAuthorityLimits {
    SourceAuthorityLimits {
        max_owners: 32,
        max_decisions: 64,
        max_payload_bytes: 1024 * 1024,
        max_command_bytes: 64 * 1024,
    }
}

fn grant(principal: &str, collection: &str, action: AccessAction) -> CollectionGrant {
    CollectionGrant {
        principal: principal.into(),
        workspace: "workspace-a".into(),
        collection: collection.into(),
        actions: vec![action as i32],
        ..Default::default()
    }
}

fn policy(revision: u64) -> AccessPolicy {
    AccessPolicy {
        format_version: 3,
        revision,
        resources: ["books", "music"]
            .map(|collection| CollectionResource {
                workspace: "workspace-a".into(),
                collection: collection.into(),
            })
            .to_vec(),
        grants: vec![
            grant("alice", "books", AccessAction::Admin),
            grant("bob", "books", AccessAction::Admin),
            grant("bob", "music", AccessAction::Admin),
        ],
    }
}

fn key(owner: &[u8]) -> LogicalSourceOwner {
    LogicalSourceOwner {
        workspace: "workspace-a".into(),
        collection: "books".into(),
        owner_id: owner.to_vec(),
    }
}

fn target(node: &str, seed: u8, residency: SourceResidency) -> SourceStorageTarget {
    SourceStorageTarget {
        node_id: node.into(),
        storage_incarnation: vec![seed; 16],
        history_id: vec![seed.wrapping_add(1); 16],
        residency: residency as i32,
        resident_device_id: if residency == SourceResidency::DeviceLocal {
            node.into()
        } else {
            String::new()
        },
    }
}

fn prepare(
    authority: &SourceAuthorityIdentity,
    owner: LogicalSourceOwner,
    command_id: &[u8],
    workflow_id: &[u8],
    (control, policy, generation): (u64, u64, u64),
    target: SourceStorageTarget,
) -> SourceAuthorityCommand {
    SourceAuthorityCommand {
        format_version: 1,
        authority: Some(authority.clone()),
        key: Some(owner),
        command_id: command_id.to_vec(),
        expected_control_revision: control,
        expected_policy_revision: policy,
        expected_ownership_generation: generation,
        action: Some(source_authority_command::Action::Prepare(
            PrepareSourceOwner {
                workflow_id: workflow_id.to_vec(),
                target: Some(target),
            },
        )),
    }
}

fn cancel(
    authority: &SourceAuthorityIdentity,
    owner: LogicalSourceOwner,
    command_id: &[u8],
    workflow_id: &[u8],
    control: u64,
    policy: u64,
    generation: u64,
) -> SourceAuthorityCommand {
    SourceAuthorityCommand {
        format_version: 1,
        authority: Some(authority.clone()),
        key: Some(owner),
        command_id: command_id.to_vec(),
        expected_control_revision: control,
        expected_policy_revision: policy,
        expected_ownership_generation: generation,
        action: Some(source_authority_command::Action::Cancel(
            CancelPreparedSourceOwner {
                workflow_id: workflow_id.to_vec(),
            },
        )),
    }
}

fn replace_grants(
    authority: &SourceAuthorityIdentity,
    collection: &str,
    command_id: &[u8],
    control: u64,
    policy_revision: u64,
    grants: Vec<CollectionGrant>,
) -> SourceAuthorityCommand {
    SourceAuthorityCommand {
        format_version: 1,
        authority: Some(authority.clone()),
        key: Some(LogicalSourceOwner {
            workspace: "workspace-a".into(),
            collection: collection.into(),
            owner_id: Vec::new(),
        }),
        command_id: command_id.to_vec(),
        expected_control_revision: control,
        expected_policy_revision: policy_revision,
        expected_ownership_generation: 0,
        action: Some(source_authority_command::Action::ReplaceGrants(
            ReplaceSourceCollectionGrants { grants },
        )),
    }
}

fn create(
    dir: &Directory,
    limits: SourceAuthorityLimits,
) -> (SourceAuthorityStore, SourceAuthorityIdentity) {
    let identity = identity(7);
    let store = SourceAuthorityStore::create(&dir.store(), &identity, &policy(1), &limits).unwrap();
    (store, identity)
}

#[test]
fn strict_open_refuses_missing_wrong_identity_and_corrupt_state_without_creating() {
    let dir = Directory::new("open");
    let missing = dir.0.join("missing.redb");
    assert!(SourceAuthorityStore::open(&missing, &identity(1)).is_err());
    assert!(!missing.exists());
    let (store, authority) = create(&dir, limits());
    drop(store);
    assert_eq!(
        SourceAuthorityStore::open(&dir.store(), &identity(2))
            .err()
            .unwrap()
            .code(),
        Code::FailedPrecondition
    );
    let original = std::fs::read(dir.store()).unwrap();
    assert!(!original.is_empty());
    let corrupt = dir.0.join("corrupt.redb");
    std::fs::write(&corrupt, b"not a redb database").unwrap();
    assert!(SourceAuthorityStore::open(&corrupt, &authority).is_err());
    assert_eq!(std::fs::read(corrupt).unwrap(), b"not a redb database");
}

#[test]
fn concurrent_exact_commands_converge_on_one_durable_decision() {
    let dir = Directory::new("concurrent");
    let (store, authority) = create(&dir, limits());
    let command = prepare(
        &authority,
        key(b"owner"),
        b"race",
        b"race-flow",
        (1, 1, 0),
        target("server", 61, SourceResidency::Server),
    );
    let store = Arc::new(store);
    let left = {
        let store = store.clone();
        let command = command.clone();
        std::thread::spawn(move || store.execute("alice", &command))
    };
    let right = {
        let store = store.clone();
        let command = command.clone();
        std::thread::spawn(move || store.execute("alice", &command))
    };
    let left = left.join().unwrap().unwrap();
    let right = right.join().unwrap().unwrap();
    assert_eq!(left, right);
    assert_eq!(left.code, 0);
    assert_eq!(
        store
            .owner("alice", &key(b"owner"))
            .unwrap()
            .ownership_generation,
        1
    );
}

#[test]
fn abrupt_exit_worker() {
    let Some(mode) = std::env::var_os("PSEARCH_SOURCE_AUTHORITY_EXIT_FAULT") else {
        return;
    };
    let path = PathBuf::from(std::env::var_os("PSEARCH_SOURCE_AUTHORITY_PATH").unwrap());
    let authority = identity(7);
    let store = SourceAuthorityStore::create(&path, &authority, &policy(1), &limits()).unwrap();
    *store.inner.fault.lock().unwrap() = Some(match mode.to_str().unwrap() {
        "before" => Fault::ExitBeforeCommit,
        "after" => Fault::ExitAfterCommit,
        other => panic!("unknown exit fault {other}"),
    });
    let command = prepare(
        &authority,
        key(b"crash-owner"),
        b"crash-command",
        b"crash-workflow",
        (1, 1, 0),
        target("server", 111, SourceResidency::Server),
    );
    store.execute("alice", &command).unwrap();
    panic!("exit fault did not terminate the worker");
}

#[test]
fn abrupt_exit_distinguishes_uncommitted_and_committed_commands() {
    for mode in ["before", "after"] {
        let dir = Directory::new(mode);
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("source_authority::tests::abrupt_exit_worker")
            .arg("--nocapture")
            .env("PSEARCH_SOURCE_AUTHORITY_EXIT_FAULT", mode)
            .env("PSEARCH_SOURCE_AUTHORITY_PATH", dir.store())
            .status_guarded()
            .unwrap();
        assert_eq!(status.code(), Some(87));

        let authority = identity(7);
        let reopened = SourceAuthorityStore::open(&dir.store(), &authority).unwrap();
        let owner = key(b"crash-owner");
        let command = prepare(
            &authority,
            owner.clone(),
            b"crash-command",
            b"crash-workflow",
            (1, 1, 0),
            target("server", 111, SourceResidency::Server),
        );
        if mode == "before" {
            assert_eq!(
                reopened
                    .decision("alice", &owner, b"crash-command")
                    .unwrap_err()
                    .code(),
                Code::NotFound
            );
            assert_eq!(
                reopened.owner("alice", &owner).unwrap_err().code(),
                Code::NotFound
            );
            assert_eq!(reopened.execute("alice", &command).unwrap().code, 0);
        } else {
            let saved = reopened
                .decision("alice", &owner, b"crash-command")
                .unwrap();
            assert_eq!(saved.code, 0);
            assert_eq!(reopened.execute("alice", &command).unwrap(), saved);
            assert_eq!(
                reopened.owner("alice", &owner).unwrap(),
                saved.owner.unwrap()
            );
        }
    }
}

#[test]
fn persisted_owner_and_operation_keys_keep_their_versioned_wire_layout() {
    let owner_bytes = b"\x0a\x01w\x12\x01c\x1a\x02\x00\xff";
    let owner = LogicalSourceOwner {
        workspace: "w".into(),
        collection: "c".into(),
        owner_id: vec![0, 255],
    };
    assert_eq!(owner.encode_to_vec(), owner_bytes);
    assert_eq!(
        LogicalSourceOwner::decode(owner_bytes.as_slice()).unwrap(),
        owner
    );

    let operation_bytes =
        b"\x08\x01\x12\x0a\x0a\x01w\x12\x01c\x1a\x02\x00\xff\x1a\x01a\x22\x02\x00\xff";
    let operation = SourceAuthorityOperationKey {
        format_version: 1,
        key: Some(owner),
        principal: "a".into(),
        command_id: vec![0, 255],
    };
    assert_eq!(operation.encode_to_vec(), operation_bytes);
    assert_eq!(
        SourceAuthorityOperationKey::decode(operation_bytes.as_slice()).unwrap(),
        operation
    );
}

use super::*;
use crate::pb::{AccessAction, AccessPolicy, CollectionGrant, CollectionResource};
use crate::test_support::ForkGuarded;
use prost::Message;
use redb::{ReadableTable, ReadableTableMetadata};
use std::{path::PathBuf, sync::Arc};
use tonic::Code;

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
        format_version: 1,
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
    control: u64,
    policy: u64,
    generation: u64,
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
fn preparation_cancel_reprepare_and_workflow_reservation_persist() {
    let dir = Directory::new("lifecycle");
    let (store, authority) = create(&dir, limits());
    let owner = key(b"phone-owner");
    let local = target("phone-17", 31, SourceResidency::DeviceLocal);
    let first_command = prepare(
        &authority,
        owner.clone(),
        b"prepare-1",
        b"workflow-1",
        1,
        1,
        0,
        local.clone(),
    );
    let first = store.execute("alice", &first_command).unwrap();
    assert_eq!(first.code, 0);
    let prepared = first.owner.clone().unwrap();
    assert_eq!(prepared.key, Some(owner.clone()));
    assert_eq!(prepared.target, Some(local.clone()));
    assert_eq!(prepared.ownership_generation, 1);
    assert_eq!(prepared.phase, PreparedSourceOwnerPhase::Prepared as i32);
    let cancelled = store
        .execute(
            "alice",
            &cancel(
                &authority,
                owner.clone(),
                b"cancel-1",
                b"workflow-1",
                2,
                1,
                1,
            ),
        )
        .unwrap();
    assert_eq!(cancelled.code, 0);
    assert_eq!(
        cancelled.owner.as_ref().unwrap().phase,
        PreparedSourceOwnerPhase::Cancelled as i32
    );

    let mut changed_residency = local.clone();
    changed_residency.residency = SourceResidency::Server as i32;
    changed_residency.resident_device_id.clear();
    let mut changed_device = local.clone();
    changed_device.node_id = "phone-18".into();
    changed_device.resident_device_id = "phone-18".into();
    let mut changed_history = local.clone();
    changed_history.history_id = vec![53; 16];
    for (command_id, invalid) in [
        (b"change-residency".as_slice(), changed_residency),
        (b"change-device".as_slice(), changed_device),
        (b"change-history".as_slice(), changed_history),
    ] {
        let rejected = store
            .execute(
                "alice",
                &prepare(
                    &authority,
                    owner.clone(),
                    command_id,
                    b"invalid-flow",
                    3,
                    1,
                    1,
                    invalid,
                ),
            )
            .unwrap();
        assert_ne!(rejected.code, 0);
        assert_eq!(rejected.control_revision, 3);
    }

    let mut replacement = local.clone();
    replacement.storage_incarnation = vec![51; 16];
    let second = store
        .execute(
            "alice",
            &prepare(
                &authority,
                owner.clone(),
                b"prepare-2",
                b"workflow-2",
                3,
                1,
                1,
                replacement.clone(),
            ),
        )
        .unwrap();
    assert_eq!(second.code, 0);
    assert_eq!(second.owner.as_ref().unwrap().ownership_generation, 2);
    assert_eq!(
        second.owner.as_ref().unwrap().target,
        Some(replacement.clone())
    );
    let cancelled_again = store
        .execute(
            "alice",
            &cancel(
                &authority,
                owner.clone(),
                b"cancel-2",
                b"workflow-2",
                4,
                1,
                2,
            ),
        )
        .unwrap();
    assert_eq!(cancelled_again.code, 0);
    let mut another_incarnation = replacement;
    another_incarnation.storage_incarnation = vec![71; 16];
    let reused_command = prepare(
        &authority,
        owner.clone(),
        b"reuse",
        b"workflow-1",
        5,
        1,
        2,
        another_incarnation,
    );
    let reused = store.execute("alice", &reused_command).unwrap();
    assert_ne!(reused.code, 0);
    assert_eq!(store.execute("alice", &reused_command).unwrap(), reused);

    drop(store);
    let reopened = SourceAuthorityStore::open(&dir.store(), &authority).unwrap();
    assert_eq!(
        reopened.owner("alice", &owner).unwrap(),
        cancelled_again.owner.unwrap()
    );
    assert_eq!(
        reopened.decision("alice", &owner, b"reuse").unwrap(),
        reused
    );
}

#[test]
fn complete_owner_actor_and_authority_scopes_do_not_alias() {
    let dir = Directory::new("scopes");
    let (store, authority) = create(&dir, limits());
    let phone = key(b"phone");
    let tablet = key(b"tablet");
    let command_id = b"same-command";
    let a = store
        .execute(
            "alice",
            &prepare(
                &authority,
                phone.clone(),
                command_id,
                b"phone-flow",
                1,
                1,
                0,
                target("phone", 11, SourceResidency::DeviceLocal),
            ),
        )
        .unwrap();
    let b = store
        .execute(
            "alice",
            &prepare(
                &authority,
                tablet.clone(),
                command_id,
                b"tablet-flow",
                2,
                1,
                0,
                target("tablet", 21, SourceResidency::DeviceLocal),
            ),
        )
        .unwrap();
    assert_eq!(a.code, 0);
    assert_eq!(b.code, 0);
    let actor_rejection = store
        .execute(
            "bob",
            &prepare(
                &authority,
                phone.clone(),
                command_id,
                b"bob-flow",
                3,
                1,
                1,
                target("server", 31, SourceResidency::Server),
            ),
        )
        .unwrap();
    assert_ne!(actor_rejection.code, 0);
    assert_eq!(store.decision("alice", &phone, command_id).unwrap(), a);
    assert_eq!(store.decision("alice", &tablet, command_id).unwrap(), b);
    assert_eq!(
        store.decision("bob", &phone, command_id).unwrap(),
        actor_rejection
    );

    let mut wrong_authority = prepare(
        &authority,
        key(b"other"),
        b"wrong-authority",
        b"flow",
        3,
        1,
        0,
        target("server", 41, SourceResidency::Server),
    );
    wrong_authority.authority = Some(identity(99));
    assert_eq!(
        store.execute("alice", &wrong_authority).unwrap_err().code(),
        Code::FailedPrecondition
    );
    let corrected = prepare(
        &authority,
        key(b"other"),
        b"wrong-authority",
        b"flow",
        3,
        1,
        0,
        target("server", 41, SourceResidency::Server),
    );
    assert_eq!(store.execute("alice", &corrected).unwrap().code, 0);
}

#[test]
fn durable_rejections_consume_capacity_and_changed_retries_never_replace_them() {
    let dir = Directory::new("decisions");
    let constrained = SourceAuthorityLimits {
        max_decisions: 1,
        ..limits()
    };
    let (store, authority) = create(&dir, constrained);
    let owner = key(b"owner");
    let rejected_command = prepare(
        &authority,
        owner.clone(),
        b"stale",
        b"flow",
        99,
        1,
        0,
        target("server", 9, SourceResidency::Server),
    );
    let rejected = store.execute("alice", &rejected_command).unwrap();
    assert_ne!(rejected.code, 0);
    assert_eq!(rejected.control_revision, 1);
    assert_eq!(store.execute("alice", &rejected_command).unwrap(), rejected);

    let mut changed = rejected_command.clone();
    changed.expected_control_revision = 1;
    assert_eq!(
        store.execute("alice", &changed).unwrap_err().code(),
        Code::FailedPrecondition
    );
    let full = store
        .execute(
            "alice",
            &prepare(
                &authority,
                owner,
                b"fresh",
                b"fresh-flow",
                1,
                1,
                0,
                target("server", 10, SourceResidency::Server),
            ),
        )
        .unwrap_err();
    assert_eq!(full.code(), Code::ResourceExhausted);
    assert!(store.decision("alice", &key(b"owner"), b"fresh").is_err());
}

#[test]
fn policy_replacement_is_resource_scoped_and_replay_requires_current_admin() {
    let dir = Directory::new("policy");
    let (store, authority) = create(&dir, limits());
    let revoke_alice = replace_grants(
        &authority,
        "books",
        b"revoke",
        1,
        1,
        vec![grant("bob", "books", AccessAction::Admin)],
    );
    let suppressed = store.execute("alice", &revoke_alice).unwrap_err();
    assert_eq!(suppressed.code(), Code::PermissionDenied);
    assert_eq!(
        store.execute("alice", &revoke_alice).unwrap_err().code(),
        Code::PermissionDenied
    );
    let visible = store.policy("bob", "workspace-a", "books").unwrap();
    assert_eq!(visible.revision, 2);
    assert_eq!(visible.resources.len(), 1);
    assert!(visible
        .grants
        .iter()
        .all(|grant| grant.collection == "books"));
    assert_eq!(
        store
            .policy("alice", "workspace-a", "books")
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );

    let restore = replace_grants(
        &authority,
        "books",
        b"restore",
        2,
        2,
        vec![
            grant("alice", "books", AccessAction::Admin),
            grant("bob", "books", AccessAction::Admin),
        ],
    );
    assert_eq!(store.execute("bob", &restore).unwrap().code, 0);
    let replay = store.execute("alice", &revoke_alice).unwrap();
    assert_eq!(
        (replay.code, replay.control_revision, replay.policy_revision),
        (0, 2, 2)
    );

    let cross = replace_grants(
        &authority,
        "books",
        b"cross",
        3,
        3,
        vec![grant("bob", "music", AccessAction::Admin)],
    );
    let cross_rejection = store.execute("alice", &cross).unwrap();
    assert_eq!(cross_rejection.code, Code::PermissionDenied as u32);
    assert_eq!(store.execute("alice", &cross).unwrap(), cross_rejection);
    let corrected = replace_grants(
        &authority,
        "books",
        b"cross",
        3,
        3,
        vec![
            grant("alice", "books", AccessAction::Admin),
            grant("bob", "books", AccessAction::Admin),
        ],
    );
    assert_eq!(
        store.execute("alice", &corrected).unwrap_err().code(),
        Code::FailedPrecondition
    );
    let corrected = replace_grants(
        &authority,
        "books",
        b"cross-corrected",
        3,
        3,
        vec![
            grant("alice", "books", AccessAction::Admin),
            grant("bob", "books", AccessAction::Admin),
        ],
    );
    assert_eq!(store.execute("alice", &corrected).unwrap().code, 0);
    let music = store.policy("bob", "workspace-a", "music").unwrap();
    assert!(music
        .grants
        .iter()
        .any(|grant| grant.principal == "bob" && grant.collection == "music"));
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
        1,
        1,
        0,
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
fn commit_faults_distinguish_known_abort_from_ambiguous_shared_failure() {
    let dir = Directory::new("faults");
    let (store, authority) = create(&dir, limits());
    let before = prepare(
        &authority,
        key(b"before"),
        b"before",
        b"before-flow",
        1,
        1,
        0,
        target("server", 81, SourceResidency::Server),
    );
    *store.inner.fault.lock().unwrap() = Some(Fault::BeforeCommit);
    let aborted = store.execute("alice", &before).unwrap_err();
    assert_eq!(aborted.code(), Code::Aborted);
    assert!(store.owner("alice", &key(b"before")).is_err());
    let accepted = store.execute("alice", &before).unwrap();
    assert_eq!(accepted.code, 0);

    let after = prepare(
        &authority,
        key(b"after"),
        b"after",
        b"after-flow",
        2,
        1,
        0,
        target("server", 91, SourceResidency::Server),
    );
    let clone = store.clone();
    *store.inner.fault.lock().unwrap() = Some(Fault::AfterCommit);
    let ambiguous = store.execute("alice", &after).unwrap_err();
    assert_eq!(ambiguous.code(), Code::Internal);
    for error in [
        store.owner("alice", &key(b"after")).unwrap_err(),
        clone.execute("alice", &after).unwrap_err(),
        clone
            .decision("alice", &key(b"after"), b"after")
            .unwrap_err(),
    ] {
        assert_eq!(error.code(), Code::FailedPrecondition);
    }

    drop(clone);
    drop(store);
    let reopened = SourceAuthorityStore::open(&dir.store(), &authority).unwrap();
    let committed = reopened
        .decision("alice", &key(b"after"), b"after")
        .unwrap();
    assert_eq!(committed.code, 0);
    assert_eq!(committed.control_revision, 3);
    assert_eq!(
        reopened.owner("alice", &key(b"after")).unwrap(),
        committed.owner.unwrap()
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
        1,
        1,
        0,
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
            1,
            1,
            0,
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

#[derive(Clone, Copy)]
enum Damage {
    HeaderCount,
    OwnerLink,
    WorkflowLink,
}

#[test]
fn reopen_audits_header_counts_and_owner_workflow_links() {
    for damage in [Damage::HeaderCount, Damage::OwnerLink, Damage::WorkflowLink] {
        let dir = Directory::new(match damage {
            Damage::HeaderCount => "damage-count",
            Damage::OwnerLink => "damage-owner",
            Damage::WorkflowLink => "damage-workflow",
        });
        let (store, authority) = create(&dir, limits());
        let command = prepare(
            &authority,
            key(b"linked-owner"),
            b"linked-command",
            b"linked-workflow",
            1,
            1,
            0,
            target("server", 121, SourceResidency::Server),
        );
        assert_eq!(store.execute("alice", &command).unwrap().code, 0);
        drop(store);

        let database = redb::Database::open(dir.store()).unwrap();
        let write = database.begin_write().unwrap();
        match damage {
            Damage::HeaderCount => {
                let mut meta = write.open_table(META).unwrap();
                let bytes = meta.get("header").unwrap().unwrap().value().to_vec();
                let mut header = SourceAuthorityHeader::decode(bytes.as_slice()).unwrap();
                header.owner_count += 1;
                meta.insert("header", header.encode_to_vec().as_slice())
                    .unwrap();
            }
            Damage::OwnerLink => {
                let mut owners = write.open_table(OWNERS).unwrap();
                let (key_bytes, mut owner) = {
                    let (key_bytes, value_bytes) = owners.iter().unwrap().next().unwrap().unwrap();
                    (
                        key_bytes.value().to_vec(),
                        PreparedSourceOwner::decode(value_bytes.value()).unwrap(),
                    )
                };
                owner.last_command.as_mut().unwrap().command_id = b"absent-command".to_vec();
                owners
                    .insert(key_bytes.as_slice(), owner.encode_to_vec().as_slice())
                    .unwrap();
            }
            Damage::WorkflowLink => {
                let mut workflows = write.open_table(WORKFLOWS).unwrap();
                let (key_bytes, mut workflow) = {
                    let (key_bytes, value_bytes) =
                        workflows.iter().unwrap().next().unwrap().unwrap();
                    (
                        key_bytes.value().to_vec(),
                        SourceAuthorityWorkflow::decode(value_bytes.value()).unwrap(),
                    )
                };
                workflow.preparation_command.as_mut().unwrap().command_id =
                    b"absent-command".to_vec();
                workflows
                    .insert(key_bytes.as_slice(), workflow.encode_to_vec().as_slice())
                    .unwrap();
            }
        }
        write.commit().unwrap();
        drop(database);

        let error = SourceAuthorityStore::open(&dir.store(), &authority)
            .err()
            .unwrap();
        assert_eq!(error.code(), Code::DataLoss);
    }
}

#[test]
fn transition_refuses_owner_state_from_another_logical_key() {
    let dir = Directory::new("transition-owner-key");
    let (store, authority) = create(&dir, limits());
    let owner_b = key(b"owner-b");
    let workflow = b"owner-b-workflow";
    let prepared = store
        .execute(
            "alice",
            &prepare(
                &authority,
                owner_b,
                b"prepare-b",
                workflow,
                1,
                1,
                0,
                target("server-b", 131, SourceResidency::Server),
            ),
        )
        .unwrap()
        .owner
        .unwrap();
    let owner_a = key(b"owner-a");
    let command = cancel(
        &authority,
        owner_a.clone(),
        b"cancel-a",
        workflow,
        2,
        1,
        prepared.ownership_generation,
    );
    let operation = contract::operation_key("alice", &owner_a, &command.command_id);
    let read = store.inner.database().begin_read().unwrap();
    let meta = read.open_table(META).unwrap();
    let header =
        SourceAuthorityHeader::decode(meta.get("header").unwrap().unwrap().value()).unwrap();
    let committed_policy =
        AccessPolicy::decode(meta.get("policy").unwrap().unwrap().value()).unwrap();

    let error = transition::apply(
        &header,
        &committed_policy,
        Some(&prepared),
        &command,
        &operation,
        false,
    )
    .err()
    .expect("an owner from another logical key must be refused");
    assert_eq!(error.code(), Code::DataLoss);
}

fn committed_state(
    store: &SourceAuthorityStore,
) -> (SourceAuthorityHeader, AccessPolicy, u64, u64, u64) {
    let read = store.inner.database().begin_read().unwrap();
    let meta = read.open_table(META).unwrap();
    let header =
        SourceAuthorityHeader::decode(meta.get("header").unwrap().unwrap().value()).unwrap();
    let policy = AccessPolicy::decode(meta.get("policy").unwrap().unwrap().value()).unwrap();
    let owners = read.open_table(OWNERS).unwrap().len().unwrap();
    let decisions = read.open_table(DECISIONS).unwrap().len().unwrap();
    let workflows = read.open_table(WORKFLOWS).unwrap().len().unwrap();
    (header, policy, owners, decisions, workflows)
}

#[test]
fn capacity_refusals_abort_the_whole_command_transaction() {
    let authority = identity(7);
    let first = prepare(
        &authority,
        key(b"first-owner"),
        b"first-command",
        b"first-workflow",
        1,
        1,
        0,
        target("server-a", 141, SourceResidency::Server),
    );
    let second = prepare(
        &authority,
        key(b"second-owner"),
        b"second-command",
        b"second-workflow",
        2,
        1,
        0,
        target("server-b", 151, SourceResidency::Server),
    );

    let measurement = Directory::new("capacity-measurement");
    let command_limit = first.encoded_len().max(second.encoded_len()) as u32;
    let measurement_limits = SourceAuthorityLimits {
        max_command_bytes: command_limit,
        ..limits()
    };
    let measured = SourceAuthorityStore::create(
        &measurement.store(),
        &authority,
        &policy(1),
        &measurement_limits,
    )
    .unwrap();
    assert_eq!(measured.execute("alice", &first).unwrap().code, 0);
    let first_payload = committed_state(&measured).0.payload_bytes;
    drop(measured);

    for (name, constrained) in [
        (
            "owner-capacity",
            SourceAuthorityLimits {
                max_owners: 1,
                max_command_bytes: command_limit,
                ..limits()
            },
        ),
        (
            "payload-capacity",
            SourceAuthorityLimits {
                max_payload_bytes: first_payload,
                max_command_bytes: command_limit,
                ..limits()
            },
        ),
    ] {
        let dir = Directory::new(name);
        let store =
            SourceAuthorityStore::create(&dir.store(), &authority, &policy(1), &constrained)
                .unwrap();
        let original = store.execute("alice", &first).unwrap();
        assert_eq!(original.code, 0);
        let before = committed_state(&store);

        let error = store.execute("alice", &second).unwrap_err();
        assert_eq!(error.code(), Code::ResourceExhausted);
        assert_eq!(committed_state(&store), before);
        assert_eq!(
            store
                .owner("alice", second.key.as_ref().unwrap())
                .unwrap_err()
                .code(),
            Code::NotFound
        );
        assert_eq!(
            store
                .decision("alice", second.key.as_ref().unwrap(), &second.command_id)
                .unwrap_err()
                .code(),
            Code::NotFound
        );
        assert_eq!(store.execute("alice", &first).unwrap(), original);
    }
}

#[test]
fn non_admin_and_cross_workspace_attempts_are_denied_without_reserving_command_ids() {
    let dir = Directory::new("authorization");
    let authority = identity(7);
    let mut initial_policy = policy(1);
    initial_policy.grants.extend([
        grant("reader", "books", AccessAction::Search),
        grant("ingester", "books", AccessAction::Ingest),
    ]);
    let store =
        SourceAuthorityStore::create(&dir.store(), &authority, &initial_policy, &limits()).unwrap();
    let owner = key(b"restricted-owner");

    for principal in ["reader", "ingester"] {
        let command = prepare(
            &authority,
            owner.clone(),
            b"denied-command",
            b"denied-workflow",
            1,
            1,
            0,
            target("server", 161, SourceResidency::Server),
        );
        assert_eq!(
            store.execute(principal, &command).unwrap_err().code(),
            Code::PermissionDenied
        );
        assert_eq!(
            store.owner(principal, &owner).unwrap_err().code(),
            Code::PermissionDenied
        );
        assert_eq!(
            store
                .decision(principal, &owner, b"denied-command")
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
        assert_eq!(
            store
                .policy(principal, "workspace-a", "books")
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
    }

    let mut other_workspace = owner.clone();
    other_workspace.workspace = "workspace-b".into();
    let wrong_workspace = prepare(
        &authority,
        other_workspace,
        b"wrong-workspace",
        b"wrong-workspace-flow",
        1,
        1,
        0,
        target("server", 171, SourceResidency::Server),
    );
    assert_eq!(
        store.execute("alice", &wrong_workspace).unwrap_err().code(),
        Code::PermissionDenied
    );

    let admit_reader = replace_grants(
        &authority,
        "books",
        b"admit-reader",
        1,
        1,
        vec![
            grant("alice", "books", AccessAction::Admin),
            grant("reader", "books", AccessAction::Admin),
        ],
    );
    assert_eq!(store.execute("alice", &admit_reader).unwrap().code, 0);
    let admitted = prepare(
        &authority,
        owner,
        b"denied-command",
        b"denied-workflow",
        2,
        2,
        0,
        target("server", 161, SourceResidency::Server),
    );
    assert_eq!(store.execute("reader", &admitted).unwrap().code, 0);
}

#[test]
fn policy_revision_exhaustion_is_a_durable_unchanged_state_decision() {
    let dir = Directory::new("policy-revision-exhaustion");
    let authority = identity(7);
    let initial_policy = policy(u64::MAX);
    let store =
        SourceAuthorityStore::create(&dir.store(), &authority, &initial_policy, &limits()).unwrap();
    let command = replace_grants(
        &authority,
        "books",
        b"exhaust-policy-revision",
        1,
        u64::MAX,
        vec![grant("alice", "books", AccessAction::Admin)],
    );
    let rejected = store.execute("alice", &command).unwrap();
    assert_eq!(rejected.code, Code::ResourceExhausted as u32);
    assert!(rejected.message.contains("policy revision exhausted"));
    assert_eq!(rejected.control_revision, 1);
    assert_eq!(rejected.policy_revision, u64::MAX);
    assert_eq!(store.execute("alice", &command).unwrap(), rejected);
    assert_eq!(
        store
            .policy("alice", "workspace-a", "books")
            .unwrap()
            .revision,
        u64::MAX
    );

    drop(store);
    let reopened = SourceAuthorityStore::open(&dir.store(), &authority).unwrap();
    assert_eq!(
        reopened
            .decision("alice", command.key.as_ref().unwrap(), &command.command_id)
            .unwrap(),
        rejected
    );
    assert_eq!(
        reopened
            .policy("alice", "workspace-a", "books")
            .unwrap()
            .revision,
        u64::MAX
    );
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

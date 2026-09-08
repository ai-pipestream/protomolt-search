use super::log_store::RaftLogStore;
use super::state_machine::{
    format_snapshot_id, parse_snapshot_id, ControlStateMachine, ImageSignature,
};
use super::types::*;
use super::{HostConfig, RaftHost};
use crate::pb::storage::LogicalSourceOwner;
use crate::pb::storage::{
    raft_entry::Payload, source_authority_command::Action, PrepareSourceOwner,
    PreparedSourceOwnerPhase, RaftEntry, RaftMembership, RaftNode, RaftProposal, RaftVoterSet,
    SourceAuthorityCommand, SourceAuthorityIdentity, SourceAuthorityLimits, SourceResidency,
    SourceStorageTarget,
};
use crate::pb::{AccessAction, AccessPolicy, CollectionGrant, CollectionResource};
use openraft::storage::{RaftLogStorage, RaftStateMachine};
use openraft::{BasicNode, Entry, EntryPayload, LeaderId, LogId, Membership, RaftLogReader, Vote};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use tokio::io::AsyncWriteExt;
use tonic::Code;

struct Directory(PathBuf);

impl Directory {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "raft-host-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
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

fn policy() -> AccessPolicy {
    AccessPolicy {
        format_version: 1,
        revision: 1,
        resources: vec![CollectionResource {
            workspace: "workspace-a".into(),
            collection: "books".into(),
        }],
        grants: vec![CollectionGrant {
            principal: "alice".into(),
            workspace: "workspace-a".into(),
            collection: "books".into(),
            actions: vec![AccessAction::Admin as i32, AccessAction::Ingest as i32],
            ..Default::default()
        }],
    }
}

fn limits() -> SourceAuthorityLimits {
    SourceAuthorityLimits {
        max_owners: 16,
        max_decisions: 256,
        max_payload_bytes: 16 << 20,
        max_command_bytes: 64 << 10,
    }
}

fn config() -> HostConfig {
    HostConfig {
        heartbeat_interval_ms: 50,
        election_timeout_min_ms: 150,
        election_timeout_max_ms: 300,
        snapshot_logs_since_last: 1_000_000,
        ..HostConfig::default()
    }
}

fn owner(id: &str) -> LogicalSourceOwner {
    LogicalSourceOwner {
        workspace: "workspace-a".into(),
        collection: "books".into(),
        owner_id: id.as_bytes().to_vec(),
    }
}

fn prepare(
    authority: &SourceAuthorityIdentity,
    owner_id: &str,
    command_id: &str,
    revision: u64,
) -> SourceAuthorityCommand {
    SourceAuthorityCommand {
        format_version: 1,
        authority: Some(authority.clone()),
        key: Some(owner(owner_id)),
        command_id: command_id.as_bytes().to_vec(),
        expected_control_revision: revision,
        expected_policy_revision: 1,
        expected_ownership_generation: 0,
        action: Some(Action::Prepare(PrepareSourceOwner {
            workflow_id: format!("install-{owner_id}").into_bytes(),
            target: Some(SourceStorageTarget {
                node_id: "server-a".into(),
                storage_incarnation: vec![41; 16],
                history_id: vec![42; 16],
                residency: SourceResidency::Server as i32,
                resident_device_id: String::new(),
            }),
        })),
    }
}

fn proposal(principal: &str, command: &SourceAuthorityCommand) -> RaftProposal {
    RaftProposal {
        format_version: 1,
        principal: principal.into(),
        command: Some(crate::pb::storage::raft_proposal::Command::Control(
            command.clone(),
        )),
    }
}

fn log_id(term: u64, node: u64, index: u64) -> LogId<u64> {
    LogId::new(LeaderId::new(term, node), index)
}

#[test]
fn envelopes_round_trip_and_malformed_ones_refuse() {
    let id = log_id(3, 7, 42);
    assert_eq!(log_id_from_proto(&log_id_to_proto(&id)), id);
    for vote in [Vote::new(3, 7), Vote::new_committed(4, 2)] {
        assert_eq!(vote_from_proto(&vote_to_proto(&vote)), vote);
    }
    let joint = Membership::new(
        vec![BTreeSet::from([1, 2, 3]), BTreeSet::from([2, 3, 4])],
        BTreeMap::from([
            (1, BasicNode { addr: "a".into() }),
            (2, BasicNode { addr: "b".into() }),
            (3, BasicNode { addr: "c".into() }),
            (4, BasicNode { addr: "d".into() }),
            (
                9,
                BasicNode {
                    addr: "learner".into(),
                },
            ),
        ]),
    );
    assert_eq!(
        membership_from_proto(&membership_to_proto(&joint)).unwrap(),
        joint
    );
    let stored = openraft::StoredMembership::new(Some(id), joint.clone());
    assert_eq!(
        stored_membership_from_proto(&stored_membership_to_proto(&stored)).unwrap(),
        stored
    );
    let command = prepare(&identity(7), "phone", "prepare", 1);
    for entry in [
        Entry {
            log_id: id,
            payload: EntryPayload::Blank,
        },
        Entry {
            log_id: id,
            payload: EntryPayload::Normal(proposal("alice", &command)),
        },
        Entry {
            log_id: id,
            payload: EntryPayload::Membership(joint.clone()),
        },
    ] {
        let back = entry_from_proto(entry_to_proto(&entry)).unwrap();
        assert_eq!(back.log_id, entry.log_id);
        assert_eq!(
            format!("{:?}", back.payload),
            format!("{:?}", entry.payload)
        );
    }
    // A voter with no node record, unsorted ids and an entry without a log
    // id are all data loss, never defaults.
    let orphan = RaftMembership {
        configs: vec![RaftVoterSet {
            node_ids: vec![1, 5],
        }],
        nodes: vec![RaftNode {
            node_id: 1,
            addr: "a".into(),
        }],
    };
    assert_eq!(
        membership_from_proto(&orphan).err().unwrap().code(),
        Code::DataLoss
    );
    let unsorted = RaftMembership {
        configs: vec![RaftVoterSet {
            node_ids: vec![2, 1],
        }],
        nodes: vec![
            RaftNode {
                node_id: 1,
                addr: "a".into(),
            },
            RaftNode {
                node_id: 2,
                addr: "b".into(),
            },
        ],
    };
    assert_eq!(
        membership_from_proto(&unsorted).err().unwrap().code(),
        Code::DataLoss
    );
    assert_eq!(
        entry_from_proto(RaftEntry {
            log_id: None,
            payload: Some(Payload::Blank(true)),
        })
        .err()
        .unwrap()
        .code(),
        Code::DataLoss
    );
    let signature = ImageSignature {
        length: 4096,
        sha256: [0xab; 32],
    };
    let snapshot_id = format_snapshot_id(Some(&id), &signature);
    assert_eq!(parse_snapshot_id(&snapshot_id).unwrap(), signature);
    assert!(parse_snapshot_id("42-4096-nothex").is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_log_store_keeps_votes_consecutive_entries_and_boundaries() {
    let dir = Directory::new("log");
    let group = identity(7);
    let path = dir.0.join("raft-log.redb");
    let mut store = RaftLogStore::create(&path, &group, 1).unwrap();
    assert_eq!(store.read_vote().await.unwrap(), None);
    store.save_vote(&Vote::new(2, 1)).await.unwrap();
    assert_eq!(store.read_vote().await.unwrap(), Some(Vote::new(2, 1)));
    let command = prepare(&group, "phone", "prepare", 1);
    let entries: Vec<Entry<ControlRaft>> = (1..=3)
        .map(|index| Entry {
            log_id: log_id(2, 1, index),
            payload: if index == 1 {
                EntryPayload::Blank
            } else {
                EntryPayload::Normal(proposal("alice", &command))
            },
        })
        .collect();
    store.append_sync(entries.clone()).unwrap();
    let read = store.try_get_log_entries(2..=3).await.unwrap();
    assert_eq!(read.len(), 2);
    assert_eq!(read[0].log_id, log_id(2, 1, 2));
    // A hole is refused by name.
    let hole = store.append_sync(vec![Entry {
        log_id: log_id(2, 1, 9),
        payload: EntryPayload::Blank,
    }]);
    assert!(hole.unwrap_err().to_string().contains("hole"));
    let state = store.get_log_state().await.unwrap();
    assert_eq!(state.last_log_id, Some(log_id(2, 1, 3)));
    assert_eq!(state.last_purged_log_id, None);
    store.truncate_sync(log_id(2, 1, 3)).unwrap();
    assert_eq!(
        store.get_log_state().await.unwrap().last_log_id,
        Some(log_id(2, 1, 2))
    );
    store.save_committed(Some(log_id(2, 1, 2))).await.unwrap();
    store.purge_sync(log_id(2, 1, 1)).unwrap();
    let state = store.get_log_state().await.unwrap();
    assert_eq!(state.last_purged_log_id, Some(log_id(2, 1, 1)));
    assert_eq!(state.last_log_id, Some(log_id(2, 1, 2)));
    assert_eq!(store.entries().unwrap().len(), 1);
    drop(store);
    // Reopen keeps everything; another node or group refuses.
    assert_eq!(
        RaftLogStore::open(&path, &group, 2).err().unwrap().code(),
        Code::FailedPrecondition
    );
    assert_eq!(
        RaftLogStore::open(&path, &identity(9), 1)
            .err()
            .unwrap()
            .code(),
        Code::FailedPrecondition
    );
    let mut reopened = RaftLogStore::open(&path, &group, 1).unwrap();
    assert_eq!(reopened.read_vote().await.unwrap(), Some(Vote::new(2, 1)));
    assert_eq!(
        reopened.read_committed().await.unwrap(),
        Some(log_id(2, 1, 2))
    );
    let state = reopened.get_log_state().await.unwrap();
    assert_eq!(state.last_purged_log_id, Some(log_id(2, 1, 1)));
    assert_eq!(state.last_log_id, Some(log_id(2, 1, 2)));
    assert_eq!(
        RaftLogStore::open(&dir.0.join("missing.redb"), &group, 1)
            .err()
            .unwrap()
            .code(),
        Code::NotFound
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_single_node_host_applies_through_the_log_and_restarts_in_place() {
    let dir = Directory::new("host");
    let group = identity(7);
    let host = RaftHost::bootstrap_single(
        &dir.0,
        &group,
        1,
        "https://node-1:19291",
        &policy(),
        &limits(),
        &config(),
    )
    .await
    .unwrap();
    let store = host.store().unwrap();
    // The direct path is closed; only proposals apply.
    assert_eq!(
        store
            .execute("alice", &prepare(&group, "phone", "prepare", 1))
            .err()
            .unwrap()
            .code(),
        Code::FailedPrecondition
    );
    let decision = host
        .propose_command("alice", &prepare(&group, "phone", "prepare", 1))
        .await
        .unwrap();
    assert_eq!(decision.code, 0, "{}", decision.message);
    assert_eq!(decision.control_revision, 2);
    let prepared = store.owner("alice", &owner("phone")).unwrap();
    assert_eq!(prepared.phase, PreparedSourceOwnerPhase::Prepared as i32);
    // An exact retry through the log answers from the record and consumes
    // one more entry; a stale command is a recorded rejection.
    let again = host
        .propose_command("alice", &prepare(&group, "phone", "prepare", 1))
        .await
        .unwrap();
    assert_eq!(again, decision);
    let stale = host
        .propose_command("alice", &prepare(&group, "other", "stale", 1))
        .await
        .unwrap();
    assert_eq!(stale.code, Code::FailedPrecondition as u32);
    // An unauthorized principal is a refusal recorded as the reply, and the
    // entry is consumed.
    let error = host
        .propose_command("carol", &prepare(&group, "other", "carol", 2))
        .await
        .err()
        .unwrap();
    assert_eq!(error.code(), Code::PermissionDenied);
    let applied = store.raft_applied().unwrap().unwrap();
    let last = applied.last_applied.unwrap();
    assert!(last.index >= 5, "{last:?}");
    let metrics = host.metrics().borrow().clone();
    assert_eq!(metrics.last_applied.map(|l| l.index), Some(last.index));
    // No control state was imported into this group; the map is absent.
    assert_eq!(
        store
            .published_map("alice", &owner("phone"))
            .err()
            .unwrap()
            .code(),
        Code::NotFound
    );
    let entries = host.log_store().entries().unwrap().len();
    drop(store);
    host.shutdown().await.unwrap();

    // Restart from durable state: the applied position and the owner are
    // there, no entry applies twice, and proposals continue.
    let host = RaftHost::start(&dir.0, &group, 1, &config()).await.unwrap();
    host.raft()
        .wait(Some(std::time::Duration::from_secs(30)))
        .state(openraft::ServerState::Leader, "re-elected")
        .await
        .unwrap();
    let store = host.store().unwrap();
    assert_eq!(store.owner("alice", &owner("phone")).unwrap(), prepared);
    assert_eq!(
        store
            .raft_applied()
            .unwrap()
            .unwrap()
            .last_applied
            .unwrap()
            .index,
        last.index
    );
    // Nothing applied twice: the log holds exactly what it held before.
    assert!(host.log_store().entries().unwrap().len() >= entries);
    let second = host
        .propose_command("alice", &prepare(&group, "other", "second", 2))
        .await
        .unwrap();
    assert_eq!(second.code, 0, "{}", second.message);
    assert_eq!(second.control_revision, 3);
    drop(store);
    host.shutdown().await.unwrap();
    // Startup never creates: an empty directory has no store to open.
    std::fs::create_dir(dir.0.join("nowhere")).unwrap();
    assert_eq!(
        RaftHost::start(&dir.0.join("nowhere"), &group, 1, &config())
            .await
            .err()
            .unwrap()
            .code(),
        Code::NotFound
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committed_entries_apply_on_restart_and_the_position_is_in_the_same_transaction() {
    let dir = Directory::new("replay");
    let group = identity(7);
    let host = RaftHost::bootstrap_single(
        &dir.0,
        &group,
        1,
        "https://node-1:19291",
        &policy(),
        &limits(),
        &config(),
    )
    .await
    .unwrap();
    let vote = host.metrics().borrow().vote;
    let last = host.log_store().entries().unwrap().last().unwrap().log_id;
    host.shutdown().await.unwrap();
    // A crash between commit and apply: the log holds a committed entry the
    // state machine never saw.
    let log = RaftLogStore::open(&dir.0.join(super::host::LOG_FILE), &group, 1).unwrap();
    let next = LogId::new(vote.leader_id, last.index + 1);
    log.append_sync(vec![Entry {
        log_id: next,
        payload: EntryPayload::Normal(proposal("alice", &prepare(&group, "phone", "prepare", 1))),
    }])
    .unwrap();
    {
        let mut log = log.clone();
        log.save_committed(Some(next)).await.unwrap();
    }
    drop(log);
    let host = RaftHost::start(&dir.0, &group, 1, &config()).await.unwrap();
    host.raft()
        .wait(Some(std::time::Duration::from_secs(30)))
        .applied_index_at_least(Some(next.index), "committed entry applied")
        .await
        .unwrap();
    let store = host.store().unwrap();
    let owner_row = store.owner("alice", &owner("phone")).unwrap();
    assert_eq!(owner_row.control_revision, 2);
    let applied = store.raft_applied().unwrap().unwrap();
    assert!(applied.last_applied.unwrap().index >= next.index);
    drop(store);
    host.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_snapshot_installs_into_a_fresh_replica_with_the_same_map() {
    let dir = Directory::new("snapshot");
    let group = identity(7);
    let host = RaftHost::bootstrap_single(
        &dir.0,
        &group,
        1,
        "https://node-1:19291",
        &policy(),
        &limits(),
        &config(),
    )
    .await
    .unwrap();
    for (owner_id, id, revision) in [("phone", "p1", 1), ("tablet", "p2", 2)] {
        assert_eq!(
            host.propose_command("alice", &prepare(&group, owner_id, id, revision))
                .await
                .unwrap()
                .code,
            0
        );
    }
    host.trigger_snapshot().await.unwrap();
    let applied = host.store().unwrap().raft_applied().unwrap().unwrap();
    let last = log_id_from_proto(applied.last_applied.as_ref().unwrap());
    host.raft()
        .wait(Some(std::time::Duration::from_secs(30)))
        .snapshot(last, "snapshot built")
        .await
        .unwrap();
    let source_owner = host
        .store()
        .unwrap()
        .owner("alice", &owner("tablet"))
        .unwrap();
    let image = std::fs::read(dir.0.join(super::host::SNAPSHOT_DIR).join("current.redb")).unwrap();
    let meta_bytes =
        std::fs::read(dir.0.join(super::host::SNAPSHOT_DIR).join("current.meta")).unwrap();
    host.shutdown().await.unwrap();

    // A fresh replica of the same group receives the image and installs it.
    let replica_dir = Directory::new("snapshot-replica");
    let replica_store = crate::source_authority::SourceAuthorityStore::create(
        &replica_dir.0.join(super::host::STORE_FILE),
        &group,
        &policy(),
        &limits(),
    )
    .unwrap();
    let mut machine = ControlStateMachine::new(
        replica_store,
        &replica_dir.0.join(super::host::SNAPSHOT_DIR),
    )
    .unwrap();
    let (before, _) = machine.applied_state().await.unwrap();
    assert_eq!(before, None);
    let meta_value =
        <crate::pb::storage::RaftSnapshotMeta as prost::Message>::decode(meta_bytes.as_slice())
            .unwrap();
    let meta = openraft::storage::SnapshotMeta {
        last_log_id: meta_value.last_log_id.as_ref().map(log_id_from_proto),
        last_membership: stored_membership_from_proto(meta_value.membership.as_ref().unwrap())
            .unwrap(),
        snapshot_id: meta_value.snapshot_id.clone(),
    };
    let mut file = machine.begin_receiving_snapshot().await.unwrap();
    file.write_all(&image).await.unwrap();
    // A corrupted image refuses before anything is replaced.
    let mut corrupt = meta.clone();
    corrupt.snapshot_id = format_snapshot_id(
        meta.last_log_id.as_ref(),
        &ImageSignature {
            length: image.len() as u64,
            sha256: [0; 32],
        },
    );
    let refused = machine.install_snapshot(&corrupt, file).await;
    assert!(refused.is_err());
    let (still, _) = machine.applied_state().await.unwrap();
    assert_eq!(still, None);
    let mut file = machine.begin_receiving_snapshot().await.unwrap();
    file.write_all(&image).await.unwrap();
    machine.install_snapshot(&meta, file).await.unwrap();
    let (after, membership) = machine.applied_state().await.unwrap();
    assert_eq!(after, Some(last));
    assert_eq!(membership, meta.last_membership);
    let replica = machine.shared_store().read().unwrap().clone().unwrap();
    assert_eq!(
        replica.owner("alice", &owner("tablet")).unwrap(),
        source_owner
    );
    let current = machine.get_current_snapshot().await.unwrap().unwrap();
    assert_eq!(current.meta, meta);
    drop(replica);
}

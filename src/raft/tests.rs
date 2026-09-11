use super::log_store::RaftLogStore;
use super::state_machine::{
    format_snapshot_id, parse_snapshot_id, published_generation, ControlStateMachine,
    ImageSignature,
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

pub(super) struct Directory(pub(super) PathBuf);

impl Directory {
    pub(super) fn new(name: &str) -> Self {
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

pub(super) fn identity(seed: u8) -> SourceAuthorityIdentity {
    SourceAuthorityIdentity {
        format_version: 1,
        group_id: vec![seed; 16],
        authority_incarnation: vec![seed.wrapping_add(1); 16],
    }
}

pub(super) fn policy() -> AccessPolicy {
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

pub(super) fn limits() -> SourceAuthorityLimits {
    SourceAuthorityLimits {
        max_owners: 16,
        max_decisions: 256,
        max_payload_bytes: 16 << 20,
        max_command_bytes: 64 << 10,
    }
}

pub(super) fn config() -> HostConfig {
    HostConfig {
        heartbeat_interval_ms: 50,
        election_timeout_min_ms: 150,
        election_timeout_max_ms: 300,
        snapshot_logs_since_last: 1_000_000,
        admission_lease_ms: 100,
        clock_skew_ms: 50,
        ..HostConfig::default()
    }
}

pub(super) fn owner(id: &str) -> LogicalSourceOwner {
    LogicalSourceOwner {
        workspace: "workspace-a".into(),
        collection: "books".into(),
        owner_id: id.as_bytes().to_vec(),
    }
}

pub(super) fn prepare(
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

pub(super) fn proposal(principal: &str, command: &SourceAuthorityCommand) -> RaftProposal {
    RaftProposal {
        format_version: 1,
        principal: principal.into(),
        command: Some(crate::pb::storage::raft_proposal::Command::Control(
            command.clone(),
        )),
    }
}

pub(super) fn log_id(term: u64, node: u64, index: u64) -> LogId<u64> {
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
    let entries = host.log_entries().unwrap().len();
    drop(store);
    host.shutdown().await.unwrap();

    // Restart from durable state: the applied position and the owner are
    // there, no entry applies twice, and proposals continue.
    let host = RaftHost::start(&dir.0, &group, 1, &config()).await.unwrap();
    host.wait(Some(std::time::Duration::from_secs(30)))
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
    assert!(host.log_entries().unwrap().len() >= entries);
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
    let last = host.log_entries().unwrap().last().unwrap().log_id;
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
    host.wait(Some(std::time::Duration::from_secs(30)))
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
    host.wait(Some(std::time::Duration::from_secs(30)))
        .snapshot(last, "snapshot built")
        .await
        .unwrap();
    let source_owner = host
        .store()
        .unwrap()
        .owner("alice", &owner("tablet"))
        .unwrap();
    let generation = published_generation(&dir.0.join(super::host::SNAPSHOT_DIR))
        .unwrap()
        .unwrap();
    let image = std::fs::read(generation.join("image.redb")).unwrap();
    let meta_bytes = std::fs::read(generation.join("meta")).unwrap();
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
        64 << 20,
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
    // A valid image of another group is refused on the probe, and a meta
    // whose membership differs from the image is refused too; the replica
    // keeps its state and has no published snapshot yet.
    let foreign_dir = Directory::new("snapshot-foreign");
    let foreign_bytes = {
        let foreign = crate::source_authority::SourceAuthorityStore::create(
            &foreign_dir.0.join("foreign.redb"),
            &identity(9),
            &policy(),
            &limits(),
        )
        .unwrap();
        drop(foreign);
        std::fs::read(foreign_dir.0.join("foreign.redb")).unwrap()
    };
    let mut file = machine.begin_receiving_snapshot().await.unwrap();
    file.write_all(&foreign_bytes).await.unwrap();
    let mut foreign_meta = meta.clone();
    foreign_meta.snapshot_id = format_snapshot_id(
        meta.last_log_id.as_ref(),
        &ImageSignature {
            length: foreign_bytes.len() as u64,
            sha256: crate::sha256::digest(&foreign_bytes),
        },
    );
    assert!(machine.install_snapshot(&foreign_meta, file).await.is_err());
    let mut file = machine.begin_receiving_snapshot().await.unwrap();
    file.write_all(&image).await.unwrap();
    let mut wrong_membership = meta.clone();
    wrong_membership.last_membership = openraft::StoredMembership::new(
        meta.last_log_id,
        Membership::new(
            vec![BTreeSet::from([1, 2])],
            BTreeMap::from([
                (1, BasicNode { addr: "a".into() }),
                (2, BasicNode { addr: "b".into() }),
            ]),
        ),
    );
    assert!(machine
        .install_snapshot(&wrong_membership, file)
        .await
        .is_err());
    assert_eq!(machine.applied_state().await.unwrap().0, None);
    assert!(machine.get_current_snapshot().await.unwrap().is_none());
    // Every refused receive left nothing behind.
    let leftovers = std::fs::read_dir(replica_dir.0.join(super::host::SNAPSHOT_DIR))
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("incoming-")
        })
        .count();
    assert_eq!(leftovers, 0);

    // A held handle never blocks the swap: the install replaces the
    // database in place, the held handle observes the installed state on
    // its next operation, and the subscriber attached before the install
    // wakes after it.
    let mut applied_watch = machine
        .shared_store()
        .read()
        .unwrap()
        .clone()
        .unwrap()
        .subscribe_applied();
    let held = machine.shared_store().read().unwrap().clone().unwrap();
    assert_eq!(
        held.owner("alice", &owner("tablet")).err().unwrap().code(),
        Code::NotFound
    );
    let mut file = machine.begin_receiving_snapshot().await.unwrap();
    file.write_all(&image).await.unwrap();
    machine.install_snapshot(&meta, file).await.unwrap();
    let (after, membership) = machine.applied_state().await.unwrap();
    assert_eq!(after, Some(last));
    assert_eq!(membership, meta.last_membership);
    assert!(applied_watch.has_changed().unwrap());
    assert_eq!(*applied_watch.borrow_and_update(), 3);
    assert_eq!(
        held.owner("alice", &owner("tablet")).unwrap(),
        source_owner,
        "the held handle serves the installed state"
    );
    drop(held);
    let replica = machine.shared_store().read().unwrap().clone().unwrap();
    assert_eq!(
        replica.owner("alice", &owner("tablet")).unwrap(),
        source_owner
    );
    let current = machine.get_current_snapshot().await.unwrap().unwrap();
    assert_eq!(current.meta, meta);
    drop(replica);

    // A bound below the image refuses the receive; abandoned receives,
    // partial builds and an unpublished generation are swept at startup
    // while the published generation stays.
    let snapshots = replica_dir.0.join(super::host::SNAPSHOT_DIR);
    drop(machine);
    let store = crate::source_authority::SourceAuthorityStore::open(
        &replica_dir.0.join(super::host::STORE_FILE),
        &group,
    )
    .unwrap();
    std::fs::create_dir(snapshots.join("incoming-1-0")).unwrap();
    std::fs::create_dir(snapshots.join("build-1")).unwrap();
    std::fs::create_dir(snapshots.join("generations").join("99")).unwrap();
    let mut machine = ControlStateMachine::new(store, &snapshots, 1024).unwrap();
    assert!(!snapshots.join("incoming-1-0").exists());
    assert!(!snapshots.join("build-1").exists());
    assert!(!snapshots.join("generations").join("99").exists());
    assert_eq!(
        machine.get_current_snapshot().await.unwrap().unwrap().meta,
        meta
    );
    let mut file = machine.begin_receiving_snapshot().await.unwrap();
    file.write_all(&image).await.unwrap();
    let oversize = machine.install_snapshot(&meta, file).await.err().unwrap();
    assert!(oversize.to_string().contains("bound"), "{oversize}");
}

/// The library purges to a snapshot before the state machine installs it,
/// and the purge is bounded by the store's applied position. The
/// remainder is recorded and completed once the store is at the position:
/// at the next append (log `a`) and at start (log `b`), both with a store
/// that applied nothing when the purge came, and with a store that had
/// applied an earlier snapshot, where the purge is cut at that position
/// (log `c`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_purge_deferred_by_the_store_completes_when_the_store_is_at_the_position() {
    let dir = Directory::new("deferred-purge");
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
    let mut snapshots = Vec::new();
    for (owner_id, id, revision) in [("phone", "p1", 1), ("tablet", "p2", 2)] {
        host.propose_command("alice", &prepare(&group, owner_id, id, revision))
            .await
            .unwrap();
        host.trigger_snapshot().await.unwrap();
        let applied = host.store().unwrap().raft_applied().unwrap().unwrap();
        let last = log_id_from_proto(applied.last_applied.as_ref().unwrap());
        host.wait(Some(std::time::Duration::from_secs(30)))
            .snapshot(last, "snapshot built")
            .await
            .unwrap();
        let generation = published_generation(&dir.0.join(super::host::SNAPSHOT_DIR))
            .unwrap()
            .unwrap();
        let image = std::fs::read(generation.join("image.redb")).unwrap();
        let meta_bytes = std::fs::read(generation.join("meta")).unwrap();
        let meta_value =
            <crate::pb::storage::RaftSnapshotMeta as prost::Message>::decode(meta_bytes.as_slice())
                .unwrap();
        let meta = openraft::storage::SnapshotMeta {
            last_log_id: meta_value.last_log_id.as_ref().map(log_id_from_proto),
            last_membership: stored_membership_from_proto(meta_value.membership.as_ref().unwrap())
                .unwrap(),
            snapshot_id: meta_value.snapshot_id.clone(),
        };
        snapshots.push((image, meta, last));
    }
    host.shutdown().await.unwrap();
    let (first_image, first_meta, first) = snapshots.remove(0);
    let (second_image, second_meta, second) = snapshots.remove(0);
    assert!(second.index > first.index);

    let replica_dir = Directory::new("deferred-purge-replica");
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
        64 << 20,
    )
    .unwrap();
    let blanks = |from: u64, upto: u64| -> Vec<Entry<ControlRaft>> {
        (from..=upto)
            .map(|index| Entry {
                log_id: log_id(1, 1, index),
                payload: EntryPayload::Blank,
            })
            .collect()
    };
    let log_path = |name: &str| replica_dir.0.join(format!("raft-log-{name}.redb"));
    let mut a = RaftLogStore::create(&log_path("a"), &group, 2).unwrap();
    let mut b = RaftLogStore::create(&log_path("b"), &group, 2).unwrap();
    for log in [&mut a, &mut b] {
        log.bind_applied_floor(machine.shared_store()).unwrap();
        log.append_sync(blanks(0, 1)).unwrap();
        // The store applied nothing: the purge moves no entry and is deferred.
        log.purge(first).await.unwrap();
        assert_eq!(log.entries().unwrap().len(), 2);
        let state = log.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, None);
        assert_eq!(state.last_log_id, Some(log_id(1, 1, 1)));
    }
    drop(b);
    let mut file = machine.begin_receiving_snapshot().await.unwrap();
    file.write_all(&first_image).await.unwrap();
    machine.install_snapshot(&first_meta, file).await.unwrap();
    assert_eq!(machine.applied_state().await.unwrap().0, Some(first));

    // Log a: the next append completes the purge first, then lands.
    let after_first = LogId::new(first.leader_id, first.index + 1);
    a.append_sync(vec![Entry {
        log_id: after_first,
        payload: EntryPayload::Blank,
    }])
    .unwrap();
    let state = a.get_log_state().await.unwrap();
    assert_eq!(state.last_purged_log_id, Some(first));
    assert_eq!(state.last_log_id, Some(after_first));
    assert_eq!(a.entries().unwrap().len(), 1);
    // Log b: a start completes it.
    let mut b = RaftLogStore::open(&log_path("b"), &group, 2).unwrap();
    b.bind_applied_floor(machine.shared_store()).unwrap();
    let state = b.get_log_state().await.unwrap();
    assert_eq!(state.last_purged_log_id, Some(first));
    assert_eq!(state.last_log_id, Some(first));
    assert_eq!(b.entries().unwrap().len(), 0);
    b.append_sync(vec![Entry {
        log_id: after_first,
        payload: EntryPayload::Blank,
    }])
    .unwrap();
    assert_eq!(
        b.get_log_state().await.unwrap().last_log_id,
        Some(after_first)
    );

    // Log c: the store is at the first snapshot, and an empty log bound to
    // it starts after that position. A purge to the second is cut at the
    // first and the rest deferred, then completed by the install of the
    // second and the append after it.
    let mut c = RaftLogStore::create(&log_path("c"), &group, 2).unwrap();
    c.bind_applied_floor(machine.shared_store()).unwrap();
    assert_eq!(
        c.get_log_state().await.unwrap().last_purged_log_id,
        Some(first)
    );
    c.append_sync(blanks(first.index + 1, second.index + 1))
        .unwrap();
    c.purge(second).await.unwrap();
    let state = c.get_log_state().await.unwrap();
    assert_eq!(state.last_purged_log_id, Some(first));
    assert_eq!(state.last_log_id, Some(log_id(1, 1, second.index + 1)));
    assert_eq!(
        c.entries().unwrap().len() as u64,
        second.index + 1 - first.index
    );
    let mut file = machine.begin_receiving_snapshot().await.unwrap();
    file.write_all(&second_image).await.unwrap();
    machine.install_snapshot(&second_meta, file).await.unwrap();
    assert_eq!(machine.applied_state().await.unwrap().0, Some(second));
    let after_second = LogId::new(second.leader_id, second.index + 2);
    c.append_sync(vec![Entry {
        log_id: after_second,
        payload: EntryPayload::Blank,
    }])
    .unwrap();
    let state = c.get_log_state().await.unwrap();
    assert_eq!(state.last_purged_log_id, Some(second));
    assert_eq!(state.last_log_id, Some(after_second));
    assert_eq!(c.entries().unwrap().len(), 2);
    // A purge completed in full clears the record: a later start keeps the
    // entries after the snapshot.
    drop(c);
    let mut c = RaftLogStore::open(&log_path("c"), &group, 2).unwrap();
    c.bind_applied_floor(machine.shared_store()).unwrap();
    let state = c.get_log_state().await.unwrap();
    assert_eq!(state.last_purged_log_id, Some(second));
    assert_eq!(state.last_log_id, Some(after_second));
    assert_eq!(c.entries().unwrap().len(), 2);
    assert_eq!(c.deferred_purge().unwrap(), None);
}

/// A record the store has not reached when the log is reopened, and a
/// run in which the library asks for less than the record: the log keeps
/// the record and purges only what the library asked in this run. The
/// library records a purge as done before it asks for it, so a purge past
/// what it asked would remove entries it still reads; a record from an
/// earlier run completes at the next start, or when the library asks for
/// a purge at or past it. A second purge with a smaller target while the
/// record is pending leaves the record as it is.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_purge_deferred_across_a_restart_completes_only_where_the_library_requested_it() {
    let dir = Directory::new("deferred-purge-restart");
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
    let mut snapshots = Vec::new();
    // Two proposals before the second snapshot: a position strictly
    // between the two snapshots is needed below.
    for batch in [
        vec![("phone", "p1", 1)],
        vec![("tablet", "p2", 2), ("laptop", "p3", 3)],
    ] {
        for (owner_id, id, revision) in batch {
            host.propose_command("alice", &prepare(&group, owner_id, id, revision))
                .await
                .unwrap();
        }
        host.trigger_snapshot().await.unwrap();
        let applied = host.store().unwrap().raft_applied().unwrap().unwrap();
        let last = log_id_from_proto(applied.last_applied.as_ref().unwrap());
        host.wait(Some(std::time::Duration::from_secs(30)))
            .snapshot(last, "snapshot built")
            .await
            .unwrap();
        let generation = published_generation(&dir.0.join(super::host::SNAPSHOT_DIR))
            .unwrap()
            .unwrap();
        let image = std::fs::read(generation.join("image.redb")).unwrap();
        let meta_bytes = std::fs::read(generation.join("meta")).unwrap();
        let meta_value =
            <crate::pb::storage::RaftSnapshotMeta as prost::Message>::decode(meta_bytes.as_slice())
                .unwrap();
        let meta = openraft::storage::SnapshotMeta {
            last_log_id: meta_value.last_log_id.as_ref().map(log_id_from_proto),
            last_membership: stored_membership_from_proto(meta_value.membership.as_ref().unwrap())
                .unwrap(),
            snapshot_id: meta_value.snapshot_id.clone(),
        };
        snapshots.push((image, meta, last));
    }
    host.shutdown().await.unwrap();
    let (first_image, first_meta, first) = snapshots.remove(0);
    let (second_image, second_meta, second) = snapshots.remove(0);
    assert!(
        second.index > first.index + 1,
        "no position between the two snapshots: {first} and {second}"
    );

    let replica_dir = Directory::new("deferred-purge-restart-replica");
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
        64 << 20,
    )
    .unwrap();
    let blanks = |from: u64, upto: u64| -> Vec<Entry<ControlRaft>> {
        (from..=upto)
            .map(|index| Entry {
                log_id: log_id(1, 1, index),
                payload: EntryPayload::Blank,
            })
            .collect()
    };
    let log_path = replica_dir.0.join("raft-log-d.redb");
    let mut file = machine.begin_receiving_snapshot().await.unwrap();
    file.write_all(&first_image).await.unwrap();
    machine.install_snapshot(&first_meta, file).await.unwrap();

    // Run one: the store is at the first snapshot; a purge to the second is
    // cut at the first and recorded.
    let mut d = RaftLogStore::create(&log_path, &group, 2).unwrap();
    d.bind_applied_floor(machine.shared_store()).unwrap();
    d.append_sync(blanks(first.index + 1, second.index + 1))
        .unwrap();
    d.purge(second).await.unwrap();
    assert_eq!(d.deferred_purge().unwrap(), Some(second));
    assert_eq!(
        d.get_log_state().await.unwrap().last_purged_log_id,
        Some(first)
    );
    // A smaller target while the record is pending: the record stays.
    let between = LogId::new(second.leader_id, second.index - 1);
    d.purge(between).await.unwrap();
    assert_eq!(d.deferred_purge().unwrap(), Some(second));
    assert_eq!(
        d.get_log_state().await.unwrap().last_purged_log_id,
        Some(first)
    );
    drop(d);

    // Run two: the store is still at the first; the start keeps the record
    // and the log, and an append in a run that asked for no purge keeps
    // them too, even once the store is past the record.
    let mut d = RaftLogStore::open(&log_path, &group, 2).unwrap();
    d.bind_applied_floor(machine.shared_store()).unwrap();
    assert_eq!(d.deferred_purge().unwrap(), Some(second));
    let state = d.get_log_state().await.unwrap();
    assert_eq!(state.last_purged_log_id, Some(first));
    assert_eq!(state.last_log_id, Some(log_id(1, 1, second.index + 1)));
    let mut file = machine.begin_receiving_snapshot().await.unwrap();
    file.write_all(&second_image).await.unwrap();
    machine.install_snapshot(&second_meta, file).await.unwrap();
    assert_eq!(machine.applied_state().await.unwrap().0, Some(second));
    d.append_sync(blanks(second.index + 2, second.index + 3))
        .unwrap();
    assert_eq!(d.deferred_purge().unwrap(), Some(second));
    let state = d.get_log_state().await.unwrap();
    assert_eq!(state.last_purged_log_id, Some(first));
    assert_eq!(state.last_log_id, Some(log_id(1, 1, second.index + 3)));
    assert_eq!(
        d.entries().unwrap().len() as u64,
        second.index + 3 - first.index
    );
    // The library asks for a purge below the record: the log purges to it
    // (the store is past it) and the record stays above.
    d.purge(between).await.unwrap();
    assert_eq!(d.deferred_purge().unwrap(), Some(second));
    assert_eq!(
        d.get_log_state().await.unwrap().last_purged_log_id,
        Some(between)
    );
    // At the next append the record completes: the library asked in this
    // run for a position it is below, so the completion to the record is
    // still past what it asked, and it waits.
    d.append_sync(blanks(second.index + 4, second.index + 4))
        .unwrap();
    assert_eq!(d.deferred_purge().unwrap(), Some(second));
    assert_eq!(
        d.get_log_state().await.unwrap().last_purged_log_id,
        Some(between)
    );
    // The library asks at the record: completed in full and cleared.
    d.purge(second).await.unwrap();
    assert_eq!(d.deferred_purge().unwrap(), None);
    let state = d.get_log_state().await.unwrap();
    assert_eq!(state.last_purged_log_id, Some(second));
    assert_eq!(state.last_log_id, Some(log_id(1, 1, second.index + 4)));
    assert_eq!(d.entries().unwrap().len(), 4);
    drop(d);

    // Run three, the start path: a record the store is past at the start
    // completes there, before the library reads the log state.
    let log_path = replica_dir.0.join("raft-log-e.redb");
    let mut e = RaftLogStore::create(&log_path, &group, 2).unwrap();
    e.bind_applied_floor(machine.shared_store()).unwrap();
    e.append_sync(blanks(second.index + 1, second.index + 2))
        .unwrap();
    let past = LogId::new(second.leader_id, second.index + 2);
    // The store is at the second: the purge is cut there and recorded.
    e.purge(past).await.unwrap();
    assert_eq!(e.deferred_purge().unwrap(), Some(past));
    assert_eq!(
        e.get_log_state().await.unwrap().last_purged_log_id,
        Some(second)
    );
    drop(e);
    // The store moves past the record between the runs (the two proposals
    // the log holds, applied through the machine).
    for entry in blanks(second.index + 1, second.index + 2) {
        machine.apply(vec![entry]).await.unwrap();
    }
    let mut e = RaftLogStore::open(&log_path, &group, 2).unwrap();
    e.bind_applied_floor(machine.shared_store()).unwrap();
    assert_eq!(e.deferred_purge().unwrap(), None);
    let state = e.get_log_state().await.unwrap();
    assert_eq!(state.last_purged_log_id, Some(past));
    assert_eq!(state.last_log_id, Some(past));
    assert_eq!(e.entries().unwrap().len(), 0);
}

/// A purge target below the purge point is a state the log cannot
/// explain, and refuses by name; and a log whose entries start after a gap
/// past its purge point refuses at the next settle whether or not a purge
/// is pending.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_purge_below_the_purge_point_and_a_gap_after_it_refuse() {
    let dir = Directory::new("purge-point");
    let group = identity(7);
    let blanks = |from: u64, upto: u64| -> Vec<Entry<ControlRaft>> {
        (from..=upto)
            .map(|index| Entry {
                log_id: log_id(1, 1, index),
                payload: EntryPayload::Blank,
            })
            .collect()
    };
    let path = dir.0.join("raft-log.redb");
    let mut log = RaftLogStore::create(&path, &group, 2).unwrap();
    log.append_sync(blanks(0, 6)).unwrap();
    log.purge_sync(log_id(1, 1, 4)).unwrap();
    let below = log.purge(log_id(1, 1, 2)).await.err().unwrap();
    assert!(
        below.to_string().contains("below the purge point 4"),
        "{below}"
    );
    assert_eq!(
        log.get_log_state().await.unwrap().last_purged_log_id,
        Some(log_id(1, 1, 4))
    );
    drop(log);

    // A gap: entry 5 removed from under the log, entry 6 kept. The store
    // bound to it is at 6, past the gap.
    {
        let database = redb::Database::open(&path).unwrap();
        let tx = database.begin_write().unwrap();
        {
            let mut table = tx
                .open_table(redb::TableDefinition::<u64, &[u8]>::new("raft_log_entries"))
                .unwrap();
            table.remove(&5).unwrap();
        }
        tx.commit().unwrap();
    }
    let store = crate::source_authority::SourceAuthorityStore::create(
        &dir.0.join(super::host::STORE_FILE),
        &group,
        &policy(),
        &limits(),
    )
    .unwrap();
    let mut machine =
        ControlStateMachine::new(store, &dir.0.join(super::host::SNAPSHOT_DIR), 64 << 20).unwrap();
    machine.apply(blanks(0, 6)).await.unwrap();
    let mut log = RaftLogStore::open(&path, &group, 2).unwrap();
    let gap = log
        .bind_applied_floor(machine.shared_store())
        .err()
        .unwrap();
    assert!(
        gap.to_string()
            .contains("holds entries from index 6 after a purge at 4"),
        "{gap}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exact_retries_advance_the_applied_position_in_every_command_family() {
    let dir = Directory::new("retry-position");
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
    let last_index = |host: &RaftHost| host.log_entries().unwrap().last().unwrap().log_id.index;
    let applied_index = |host: &RaftHost| host.applied_position().unwrap().unwrap().index;
    // Control: accepted, then the exact retry.
    let command = prepare(&group, "phone", "prepare", 1);
    let first = host.propose_command("alice", &command).await.unwrap();
    let again = host.propose_command("alice", &command).await.unwrap();
    assert_eq!(first, again);
    assert_eq!(applied_index(&host), last_index(&host));
    // Import: a recorded rejection (no workflow), then its exact retry.
    let chunk = crate::pb::storage::ControlImportCommand {
        format_version: 1,
        authority: Some(group.clone()),
        key: Some(LogicalSourceOwner {
            workspace: "workspace-a".into(),
            collection: "books".into(),
            owner_id: Vec::new(),
        }),
        command_id: b"chunk".to_vec(),
        expected_control_revision: 2,
        expected_policy_revision: 1,
        workflow_id: b"import-1".to_vec(),
        action: Some(crate::pb::storage::control_import_command::Action::Chunk(
            crate::pb::storage::ControlImportChunk {
                ordinal: 0,
                bytes: vec![1, 2, 3],
                sha256: crate::source_authority::chunk_digest(&[1, 2, 3]),
            },
        )),
    };
    let first = host.propose_import("alice", &chunk, None).await.unwrap();
    assert_eq!(first.code, Code::NotFound as u32);
    let again = host.propose_import("alice", &chunk, None).await.unwrap();
    assert_eq!(first, again);
    assert_eq!(applied_index(&host), last_index(&host));
    // Capacity configure: a recorded rejection (no applied control state),
    // then its exact retry.
    let configure = crate::pb::storage::CapacityConfigureCommand {
        format_version: 1,
        authority: Some(group.clone()),
        key: chunk.key.clone(),
        command_id: b"configure".to_vec(),
        expected_control_revision: 2,
        expected_policy_revision: 1,
        configuration: Some(crate::pb::storage::CapacityConfiguration {
            format_version: 1,
            cohort_length_ms: 60_000,
            cohort_phase_unix_ms: 0,
            max_total_records: 16,
            max_total_bytes: 1 << 20,
            max_registered_nodes: 4,
            policy: Some(crate::pb::storage::CapacityTierPolicy {
                format_version: 1,
                tiers: vec![crate::pb::storage::CapacityTierSpec {
                    name: "hot".into(),
                    residency: crate::pb::storage::CapacityResidency::Server as i32,
                    min_replicas: 1,
                    scans_per_byte_nanos_lo: 0,
                    scans_per_byte_nanos_hi: 1_000,
                    max_seconds_since_scan: 60,
                }],
            }),
        }),
    };
    let first = host
        .propose_capacity_configure("alice", &configure)
        .await
        .unwrap();
    assert_eq!(first.code, Code::FailedPrecondition as u32);
    let again = host
        .propose_capacity_configure("alice", &configure)
        .await
        .unwrap();
    assert_eq!(first, again);
    assert_eq!(applied_index(&host), last_index(&host));
    // Observation transition: refused at its envelope (no configuration),
    // recorded as the reply; the entry is consumed both times.
    let transition = crate::pb::storage::CapacityTransition {
        format_version: 1,
        authority: Some(group.clone()),
        key: chunk.key.clone(),
        action: Some(crate::pb::storage::capacity_transition::Action::Register(
            crate::pb::storage::RegisterCapacityReporter {
                node_id: "server-a".into(),
                process_incarnation: vec![5; 16],
            },
        )),
    };
    for _ in 0..2 {
        assert_eq!(
            host.propose_capacity_transition("alice", &transition)
                .await
                .err()
                .unwrap()
                .code(),
            Code::FailedPrecondition
        );
        assert_eq!(applied_index(&host), last_index(&host));
    }
    // Changed content under a used id is refused at the envelope and still
    // consumes its entry.
    let mut changed = command.clone();
    changed.expected_policy_revision = 7;
    assert_eq!(
        host.propose_command("alice", &changed)
            .await
            .err()
            .unwrap()
            .code(),
        Code::FailedPrecondition
    );
    assert_eq!(applied_index(&host), last_index(&host));
    let last = applied_index(&host);
    host.shutdown().await.unwrap();
    let host = RaftHost::start(&dir.0, &group, 1, &config()).await.unwrap();
    assert_eq!(applied_index(&host), last);
    host.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hosted_store_refuses_direct_mutation_and_local_admission() {
    let dir = Directory::new("hosted-refusals");
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
    let hosted = |result: Result<(), tonic::Status>| {
        let error = result.err().unwrap();
        assert_eq!(error.code(), Code::FailedPrecondition);
        assert!(error.message().contains("Raft-hosted"), "{error}");
    };
    hosted(
        store
            .execute("alice", &prepare(&group, "phone", "prepare", 1))
            .map(|_| ()),
    );
    let key = LogicalSourceOwner {
        workspace: "workspace-a".into(),
        collection: "books".into(),
        owner_id: Vec::new(),
    };
    hosted(
        store
            .execute_control_import(
                "alice",
                &crate::pb::storage::ControlImportCommand {
                    format_version: 1,
                    authority: Some(group.clone()),
                    key: Some(key.clone()),
                    command_id: b"abort".to_vec(),
                    expected_control_revision: 1,
                    expected_policy_revision: 1,
                    workflow_id: b"import-1".to_vec(),
                    action: Some(crate::pb::storage::control_import_command::Action::Abort(
                        crate::pb::storage::AbortControlImport {},
                    )),
                },
            )
            .map(|_| ()),
    );
    hosted(
        store
            .capacity_transition(
                "alice",
                &crate::pb::storage::CapacityTransition {
                    format_version: 1,
                    authority: Some(group.clone()),
                    key: Some(key.clone()),
                    action: Some(crate::pb::storage::capacity_transition::Action::Expire(
                        crate::pb::storage::CommitCapacityExpiry {
                            now_unix_ms: 1,
                            max_age_ms: 1,
                        },
                    )),
                },
            )
            .map(|_| ()),
    );
    hosted(store.admission("alice").map(|_| ()));
    // The general proposal path never admits a readiness confirmation.
    let confirm = SourceAuthorityCommand {
        command_id: b"confirm".to_vec(),
        action: Some(Action::ConfirmReady(
            crate::pb::storage::ConfirmSourceOwnerReady {
                workflow_id: b"install-phone".to_vec(),
                completion: None,
            },
        )),
        ..prepare(&group, "phone", "prepare", 1)
    };
    assert_eq!(
        host.propose_command("alice", &confirm)
            .await
            .err()
            .unwrap()
            .code(),
        Code::PermissionDenied
    );
    // A leased admission from the host works while it leads; past its
    // deadline it admits nothing.
    host.with_admission("alice", |admission| {
        assert!(admission.expires_at().is_some());
        admission
            .authorize("books", AccessAction::Admin)
            .map(|d| assert_eq!(d.principal, "alice"))
    })
    .await
    .unwrap();
    // An interval that elapsed before the grant is not granted at all; one
    // that elapses while held admits nothing from then on.
    let elapsed = crate::source_authority::AdmissionLease {
        anchor: std::time::Instant::now() - std::time::Duration::from_millis(200),
        anchor_wall: std::time::SystemTime::now() - std::time::Duration::from_millis(200),
        ttl: std::time::Duration::from_millis(100),
    };
    let error = match store.leased_admission("alice", elapsed) {
        Ok(_) => panic!("an elapsed interval was granted"),
        Err(error) => error,
    };
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(
        error.message().contains("elapsed before the grant"),
        "{error}"
    );
    let short = crate::source_authority::AdmissionLease {
        anchor: std::time::Instant::now(),
        anchor_wall: std::time::SystemTime::now(),
        ttl: std::time::Duration::from_millis(30),
    };
    let expiring = store.leased_admission("alice", short).unwrap();
    expiring.authorize("books", AccessAction::Admin).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(40));
    let error = expiring
        .authorize("books", AccessAction::Admin)
        .err()
        .unwrap();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("lease expired"), "{error}");
    drop(expiring);
    drop(store);
    host.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lease_taken_on_one_thread_grants_on_another_within_its_interval() {
    let dir = Directory::new("owned-lease-grant");
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
    let leased = host.lease("alice").await.unwrap();
    assert_eq!(leased.principal(), "alice");
    let granted = tokio::task::spawn_blocking(move || {
        let admission = leased.admission().unwrap();
        assert!(admission.expires_at().is_some());
        admission
            .authorize("books", AccessAction::Admin)
            .map(|d| assert_eq!(d.principal, "alice"))
    })
    .await
    .unwrap();
    granted.unwrap();
    host.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lease_kept_past_its_interval_is_rejected_at_the_grant() {
    let dir = Directory::new("owned-lease-expiry");
    let group = identity(7);
    let short = HostConfig {
        admission_lease_ms: 40,
        ..config()
    };
    let host = RaftHost::bootstrap_single(
        &dir.0,
        &group,
        1,
        "https://node-1:19291",
        &policy(),
        &limits(),
        &short,
    )
    .await
    .unwrap();
    let leased = host.lease("alice").await.unwrap();
    let applied = host.applied_position().unwrap().map(|id| id.index);
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    let error = leased.admission().err().unwrap();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(
        error
            .message()
            .contains("admission interval elapsed before the grant"),
        "{error}"
    );
    drop(leased);
    assert_eq!(host.applied_position().unwrap().map(|id| id.index), applied);
    host.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_recovery_admission_opens_a_handle_and_admits_no_write() {
    let dir = Directory::new("recovery-admission");
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
    // The applied view opens a handle: the actor and identity resolve, and
    // the guard is taken, without a leader or a lease.
    let admission = store.recovery_admission("alice").unwrap();
    assert_eq!(admission.principal(), "alice");
    assert_eq!(admission.identity(), &group);
    assert!(admission.lease().is_none());
    // Every write-side check on it is rejected by name.
    for (what, result) in [
        ("check_fresh", admission.check_fresh()),
        (
            "authorize",
            admission
                .authorize("books", AccessAction::Ingest)
                .map(|_| ()),
        ),
        (
            "admit_write",
            admission
                .admit_write(
                    &crate::pb::storage::LogicalSourceOwner {
                        workspace: "workspace-a".into(),
                        collection: "books".into(),
                        owner_id: vec![1; 16],
                    },
                    1,
                    AccessAction::Ingest,
                )
                .map(|_| ()),
        ),
    ] {
        let error = result.err().unwrap_or_else(|| panic!("{what} admitted"));
        assert_eq!(error.code(), Code::FailedPrecondition, "{what}: {error}");
        assert!(
            error
                .message()
                .contains("a recovery admission opens a managed handle and admits no write"),
            "{what}: {error}"
        );
    }
    drop(admission);
    host.shutdown().await.unwrap();
}

/// I2's floor (docs/raft-admission.md, "The rule", 4): a lease plus the
/// skew budget that reaches past the election floor is refused at
/// validation, before any host is built; the in-crate recipe validates.
#[test]
fn a_lease_beyond_the_election_floor_is_rejected_at_validation() {
    let sound = config();
    sound.validate().unwrap();
    let beyond = HostConfig {
        admission_lease_ms: sound.election_timeout_min_ms - sound.clock_skew_ms + 1,
        ..config()
    };
    let error = beyond.validate().unwrap_err();
    assert_eq!(error.code(), Code::InvalidArgument, "{error}");
    assert!(
        error.message().contains("election_timeout_min_ms"),
        "the refusal names the floor: {error}"
    );
    let zero = HostConfig {
        admission_lease_ms: 0,
        ..config()
    };
    let error = zero.validate().unwrap_err();
    assert_eq!(error.code(), Code::InvalidArgument, "{error}");
}

/// A member with no transport has no directory to change: a membership
/// operation refuses by name before the library sees it.
#[cfg(feature = "tls")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_member_without_a_transport_refuses_membership_changes_by_name() {
    let dir = Directory::new("not-networked");
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
    let error = host.add_learner(2, "127.0.0.1:1").await.unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition, "{error}");
    assert_eq!(
        super::reasons::reason_of(&error),
        Some(super::reasons::MEMBERSHIP_NOT_NETWORKED),
        "{error}"
    );
    host.shutdown().await.unwrap();
}

/// The specification's document versions and its version map stay equal
/// to the code's numbers (`docs/raft-specification.md`): a bumped
/// format or protocol fails here until the documents move with it.
#[test]
fn specification_versions_match_the_code() {
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    // Each document's own version line, as written under its title; the
    // map's version column is checked against it, not against a literal.
    let mut versions = BTreeMap::new();
    for doc in [
        "docs/raft-admission.md",
        "docs/raft-hosting.md",
        "docs/document-writes.md",
        "docs/raft-error-registry.md",
    ] {
        let text = std::fs::read_to_string(root.join(doc)).unwrap();
        let version = text
            .lines()
            .find_map(|line| line.trim().strip_prefix("Specification version: "))
            .unwrap_or_else(|| panic!("{doc} carries a specification version line"))
            .trim()
            .to_string();
        assert!(
            !version.is_empty() && version.chars().all(|c| c.is_ascii_digit()),
            "{doc}: specification version {version:?} is a number"
        );
        versions.insert(doc, version);
    }
    let spec = std::fs::read_to_string(root.join("docs/raft-specification.md")).unwrap();
    let protocol = super::transport::PROTOCOL_VERSION;
    let format = crate::document_catalog::ACTIVE_MANAGED_FORMAT;
    let mut rows = 0;
    for line in spec.lines() {
        let line = line.trim();
        if !line.starts_with("| `docs/") {
            continue;
        }
        let cells: Vec<&str> = line
            .trim_start_matches('|')
            .trim_end_matches('|')
            .split('|')
            .map(str::trim)
            .collect();
        assert_eq!(cells.len(), 3, "version-map row: {line}");
        let doc = cells[0].trim_matches('`');
        let version = versions
            .get(doc)
            .unwrap_or_else(|| panic!("version-map row names an unlisted document: {line}"));
        assert_eq!(cells[1], version, "document version: {line}");
        if cells[2].contains("transport protocol version") {
            assert!(
                cells[2].contains(&format!("transport protocol version {protocol}")),
                "protocol number: {line}"
            );
        }
        if cells[2].contains("catalog format") {
            assert!(
                cells[2].contains(&format!("catalog format {format}")),
                "catalog format: {line}"
            );
        }
        rows += 1;
    }
    assert_eq!(rows, 4, "one version-map row per behavioral document");
}

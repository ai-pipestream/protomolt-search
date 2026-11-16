//! Fixtures shared by the Raft host tests and the transport tests.
use super::*;

/// The snapshot metadata the library carries, over this group's types.
pub(super) type SnapshotMeta = openraft::storage::SnapshotMeta<NodeId, BasicNode>;

pub(in crate::raft) struct Directory(pub(in crate::raft) PathBuf);

impl Directory {
    pub(in crate::raft) fn new(name: &str) -> Self {
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

pub(in crate::raft) fn identity(seed: u8) -> SourceAuthorityIdentity {
    SourceAuthorityIdentity {
        format_version: 1,
        group_id: vec![seed; 16],
        authority_incarnation: vec![seed.wrapping_add(1); 16],
    }
}

pub(in crate::raft) fn policy() -> AccessPolicy {
    AccessPolicy {
        format_version: 3,
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

pub(in crate::raft) fn limits() -> SourceAuthorityLimits {
    SourceAuthorityLimits {
        max_owners: 16,
        max_decisions: 256,
        max_payload_bytes: 16 << 20,
        max_command_bytes: 64 << 10,
    }
}

pub(in crate::raft) fn config() -> HostConfig {
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

pub(super) async fn bootstrap_application_is_ready(host: &RaftHost) {
    // Bootstrap leadership precedes application of membership at index zero
    // and the leader's blank at index one. These fixtures measure a lease,
    // not the startup fsyncs its read barrier would otherwise have to await.
    host.wait(Some(std::time::Duration::from_secs(30)))
        .applied_index_at_least(Some(1), "bootstrap membership and leader blank applied")
        .await
        .unwrap();
}

pub(in crate::raft) fn owner(id: &str) -> LogicalSourceOwner {
    LogicalSourceOwner {
        workspace: "workspace-a".into(),
        collection: "books".into(),
        owner_id: id.as_bytes().to_vec(),
    }
}

pub(in crate::raft) fn prepare(
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

pub(in crate::raft) fn proposal(principal: &str, command: &SourceAuthorityCommand) -> RaftProposal {
    RaftProposal {
        format_version: 1,
        principal: principal.into(),
        command: Some(crate::pb::storage::raft_proposal::Command::Control(
            command.clone(),
        )),
    }
}

pub(in crate::raft) fn log_id(term: u64, node: u64, index: u64) -> LogId<u64> {
    LogId::new(LeaderId::new(term, node), index)
}

/// Blank entries of term 1 from node 1 over `from..=upto`.
pub(super) fn blanks(from: u64, upto: u64) -> Vec<Entry<ControlRaft>> {
    (from..=upto)
        .map(|index| Entry {
            log_id: log_id(1, 1, index),
            payload: EntryPayload::Blank,
        })
        .collect()
}

/// One published snapshot of a host: its image, its metadata and the
/// position it covers.
pub(super) struct Published {
    pub(super) image: Vec<u8>,
    pub(super) meta: SnapshotMeta,
    pub(super) last: LogId<u64>,
}

/// Build a snapshot at the host's applied position and read the
/// published generation back from `dir`.
pub(super) async fn snapshot_now(host: &RaftHost, dir: &Directory) -> Published {
    host.trigger_snapshot().await.unwrap();
    let applied = host.store().unwrap().raft_applied().unwrap().unwrap();
    let last = log_id_from_proto(applied.last_applied.as_ref().unwrap());
    host.wait(Some(std::time::Duration::from_secs(30)))
        .snapshot(last, "snapshot built")
        .await
        .unwrap();
    let generation = published_generation(&dir.0.join(crate::raft::host::SNAPSHOT_DIR))
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
    Published { image, meta, last }
}

/// A fresh replica of `group` in `replica_dir`: a created store under a
/// state machine that has applied nothing.
pub(super) fn replica_machine(
    replica_dir: &Directory,
    group: &SourceAuthorityIdentity,
) -> ControlStateMachine {
    let replica_store = crate::source_authority::SourceAuthorityStore::create(
        &replica_dir.0.join(crate::raft::host::STORE_FILE),
        group,
        &policy(),
        &limits(),
    )
    .unwrap();
    ControlStateMachine::new(
        replica_store,
        &replica_dir.0.join(crate::raft::host::SNAPSHOT_DIR),
        64 << 20,
    )
    .unwrap()
}

/// A single-node host that proposes each batch of `(owner, command id,
/// revision)` and snapshots after it, then shuts down; returns the group
/// and the snapshots in order.
pub(super) async fn snapshots_after(
    name: &str,
    batches: &[&[(&str, &str, u64)]],
) -> (SourceAuthorityIdentity, Vec<Published>) {
    let dir = Directory::new(name);
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
    for batch in batches {
        for (owner_id, id, revision) in batch.iter().copied() {
            host.propose_command("alice", &prepare(&group, owner_id, id, revision))
                .await
                .unwrap();
        }
        snapshots.push(snapshot_now(&host, &dir).await);
    }
    host.shutdown().await.unwrap();
    (group, snapshots)
}

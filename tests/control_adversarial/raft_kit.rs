//! Raft-host kit for the slice-3c regression target
//! (`docs/control-authority-test-harness.md`). Single-node hosts on the
//! public `RaftHost` surface only; every helper mirrors the in-crate
//! `src/raft/tests.rs` recipes.
//!
//! Shared between the `control_raft_regressions` target and the other
//! adversarial targets, which compile it under the `raft` feature without
//! using every helper.
#![cfg(feature = "raft")]
#![allow(dead_code)]

use std::ops::Deref;
use std::time::Duration;

use pipestream_search::pb::storage::{RaftProposal, RaftSnapshotMeta};
use pipestream_search::raft::{ControlStateMachine, HostConfig, RaftHost};
use prost::Message;
use tokio::io::AsyncWriteExt;

use super::kit::{identity, limits, policy, TestDir, SEED};

pub const NODE_ID: u64 = 1;
pub const NODE_ADDR: &str = "127.0.0.1:9917";

/// The in-crate single-node recipe: fast heartbeats, wide snapshots only on
/// demand.
pub fn host_config() -> HostConfig {
    HostConfig {
        heartbeat_interval_ms: 50,
        election_timeout_min_ms: 150,
        election_timeout_max_ms: 300,
        snapshot_logs_since_last: 1_000_000,
        ..HostConfig::default()
    }
}

/// Bootstrap a one-voter group in `dir`, waiting for leadership.
pub async fn bootstrap_host(dir: &TestDir) -> RaftHost {
    bootstrap_host_with(dir, &policy()).await
}

/// [`bootstrap_host`] with an explicit access policy (e.g. one that also
/// grants Ingest for fence-admission checks).
pub async fn bootstrap_host_with(
    dir: &TestDir,
    policy: &pipestream_search::pb::AccessPolicy,
) -> RaftHost {
    RaftHost::bootstrap_single(
        dir.path(),
        &identity(SEED),
        NODE_ID,
        NODE_ADDR,
        policy,
        &limits(),
        &host_config(),
    )
    .await
    .unwrap()
}

/// Reopen a bootstrapped group.
pub async fn start_host(dir: &TestDir) -> RaftHost {
    RaftHost::start(dir.path(), &identity(SEED), NODE_ID, &host_config())
        .await
        .unwrap()
}

/// Block until the node leads again after a restart.
pub async fn wait_leader(host: &RaftHost) {
    host.raft()
        .wait(Some(Duration::from_secs(30)))
        .state(openraft::ServerState::Leader, "regression host leader")
        .await
        .unwrap();
}

/// Poll raft metrics until the state machine has applied at least `index`.
/// `RaftHost::propose` resolves on commit; the apply (and the metrics watch)
/// land a tick later, so a bare read races the state machine.
pub async fn wait_applied(host: &RaftHost, index: u64) {
    let mut metrics = host.metrics();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if metrics
            .borrow()
            .last_applied
            .is_some_and(|applied| applied.index >= index)
        {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "applied position never reached index {index}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
        let _ = metrics.changed().await;
    }
}

/// The committed index the state machine has applied, from raft metrics.
pub fn last_applied(host: &RaftHost) -> u64 {
    host.metrics()
        .borrow()
        .clone()
        .last_applied
        .unwrap_or_else(|| panic!("host has applied nothing yet"))
        .index
}

// ---- slice 3d: standalone snapshot-protocol helpers ------------------------
//
// These drive a `ControlStateMachine` built over a kit store (never a
// `RaftHost`) so installs can be exercised directly, exactly as a follower
// receiver would see them.

use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::{LeaderId, LogId, Membership, SnapshotMeta, StoredMembership};

/// One committed control entry at `(term 1, node 1, index)`, the shape every
/// standalone fixture in this suite uses.
pub fn control_entry(
    index: u64,
    command: pipestream_search::pb::storage::SourceAuthorityCommand,
) -> openraft::Entry<pipestream_search::raft::ControlRaft> {
    openraft::Entry {
        log_id: LogId::new(LeaderId::new(1, NODE_ID), index),
        payload: openraft::EntryPayload::Normal(raw_proposal(
            pipestream_search::pb::storage::raft_proposal::Command::Control(command),
        )),
    }
}

/// Build the current generation through the real snapshot builder and return
/// the published image bytes together with the openraft meta.
pub async fn build_current(
    machine: &mut ControlStateMachine,
) -> (Vec<u8>, SnapshotMeta<u64, openraft::BasicNode>) {
    let mut builder = RaftStateMachine::get_snapshot_builder(machine).await;
    let snapshot = builder.build_snapshot().await.unwrap();
    let mut bytes = Vec::new();
    use tokio::io::AsyncReadExt;
    let mut file = snapshot.snapshot;
    file.read_to_end(&mut bytes).await.unwrap();
    (bytes, snapshot.meta)
}

/// A `SnapshotMeta` naming `index` for exactly `image` bytes, with the given
/// voter set — the snapshot id format the receiver verifies.
pub fn meta_for(
    index: u64,
    image: &[u8],
    voters: &[u64],
) -> SnapshotMeta<u64, openraft::BasicNode> {
    let digest = pipestream_search::sha256::digest(image);
    let log_id = LogId::new(LeaderId::new(1, NODE_ID), index);
    let config: Vec<std::collections::BTreeSet<u64>> = voters
        .iter()
        .map(|v| std::collections::BTreeSet::from([*v]))
        .collect();
    SnapshotMeta {
        last_log_id: Some(log_id),
        last_membership: StoredMembership::new(Some(log_id), Membership::new(config, ())),
        snapshot_id: format!(
            "{}-{}-{}",
            index,
            image.len(),
            pipestream_search::sha256::to_hex(&digest)
        ),
    }
}

/// Receive `image` through the machine's own receive path and install it
/// under `meta`. The receive file identity is preserved end to end.
pub async fn install_received(
    machine: &mut ControlStateMachine,
    image: &[u8],
    meta: &SnapshotMeta<u64, openraft::BasicNode>,
) -> Result<(), String> {
    use openraft::storage::RaftStateMachine;
    let mut file = RaftStateMachine::begin_receiving_snapshot(machine)
        .await
        .map_err(|e| e.to_string())?;
    file.write_all(image).await.map_err(|e| e.to_string())?;
    RaftStateMachine::install_snapshot(machine, meta, file)
        .await
        .map_err(|e| e.to_string())
}

/// Copy the store file out of `dir` as an image. Every clone of the store
/// (including the state machine that owns it) must be dropped first: redb
/// holds an exclusive file lock and the copy must see quiesced bytes.
pub fn image_of(dir: &TestDir) -> Vec<u8> {
    std::fs::read(dir.authority()).unwrap()
}

/// A raw envelope as `host.propose` and `raft().client_write` accept it —
/// no admission, holder or binding checks beyond envelope shape.
pub fn raw_proposal(
    command: pipestream_search::pb::storage::raft_proposal::Command,
) -> RaftProposal {
    RaftProposal {
        format_version: 1,
        principal: "alice".into(),
        command: Some(command),
    }
}

/// Wait for `trigger_snapshot`'s builder to publish the image and meta,
/// then return both. The meta on disk is the prost `RaftSnapshotMeta`; its
/// `last_log_id` is the durable applied position the snapshot bound.
pub async fn snapshot_image(dir: &TestDir, host: &RaftHost) -> (Vec<u8>, RaftSnapshotMeta) {
    host.trigger_snapshot().await.unwrap();
    let dir_path = dir.path().join("raft-snapshots");
    let meta_path = dir_path.join("current.meta");
    let mut last_error = None;
    for _ in 0..200 {
        if meta_path.exists() {
            let bytes = std::fs::read(&meta_path).unwrap();
            match RaftSnapshotMeta::decode(bytes.as_slice()) {
                Ok(meta) => {
                    let image = std::fs::read(dir_path.join("current.redb")).unwrap();
                    return (image, meta);
                }
                Err(error) => last_error = Some(error.to_string()),
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("snapshot meta never became readable: {last_error:?}");
}

/// Install `image` into a standalone state machine through the openraft
/// trait surface, exactly as a follower receiver would: the received file
/// is the one `begin_receiving_snapshot` opened. Returns the storage error
/// rendered (the R4 refusal path renders the replace failure).
pub async fn install_image(
    machine: &mut ControlStateMachine,
    image: &[u8],
    meta_pb: &RaftSnapshotMeta,
) -> Result<(), String> {
    use openraft::storage::RaftStateMachine;
    let mut file = RaftStateMachine::begin_receiving_snapshot(machine)
        .await
        .map_err(|e| e.to_string())?;
    file.write_all(image).await.map_err(|e| e.to_string())?;
    let last_log_id = meta_pb
        .last_log_id
        .map(|id| openraft::LogId::new(openraft::LeaderId::new(id.term, id.node_id), id.index));
    let membership = openraft::Membership::<u64, openraft::BasicNode>::new(
        vec![std::collections::BTreeSet::from([NODE_ID])],
        (),
    );
    let meta = openraft::SnapshotMeta {
        last_log_id,
        last_membership: openraft::StoredMembership::new(last_log_id, membership),
        snapshot_id: meta_pb.snapshot_id.clone(),
    };
    RaftStateMachine::install_snapshot(machine, &meta, file)
        .await
        .map_err(|e| e.to_string())
}

/// Guard so a panicking test still shuts its host down: call
/// `shutdown().await` on the happy path; the drop path spawns the shutdown
/// on the running runtime, which the test runtime reaps.
pub struct HostGuard(Option<RaftHost>);

impl HostGuard {
    pub fn new(host: RaftHost) -> Self {
        Self(Some(host))
    }
    pub async fn shutdown(mut self) {
        self.0.take().unwrap().shutdown().await.unwrap();
    }
}

impl Deref for HostGuard {
    type Target = RaftHost;
    fn deref(&self) -> &RaftHost {
        self.0.as_ref().unwrap()
    }
}

impl Drop for HostGuard {
    fn drop(&mut self) {
        if let (Some(host), Ok(handle)) = (self.0.take(), tokio::runtime::Handle::try_current()) {
            let _ = handle.spawn(async move {
                let _ = host.shutdown().await;
            });
        }
    }
}

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
use pipestream_search::raft::{HostConfig, RaftHost};
use prost::Message;

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
        // 0fe7081: lease + skew must end before the election floor
        // (HostConfig::validate); the in-crate tests use 100/50.
        admission_lease_ms: 100,
        clock_skew_ms: 50,
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
    host.wait(Some(Duration::from_secs(30)))
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

/// The snapshot state dir: `raft-snapshots/` (host.rs `SNAPSHOT_DIR`).
pub fn snapshots_dir(dir: &TestDir) -> std::path::PathBuf {
    dir.path().join("raft-snapshots")
}

/// Read the published generation number from the pointer file, if any.
pub fn read_pointer(dir: &TestDir) -> Option<u64> {
    let text = std::fs::read_to_string(snapshots_dir(dir).join("current")).ok()?;
    text.trim().parse::<u64>().ok()
}

/// The directory of generation `n`: `generations/<n>/` holding
/// `image.redb` + `meta` (docs/raft-hosting.md, "Snapshots are immutable
/// generations").
pub fn generation_dir(dir: &TestDir, generation: u64) -> std::path::PathBuf {
    snapshots_dir(dir)
        .join("generations")
        .join(generation.to_string())
}

/// Wait for `trigger_snapshot`'s builder to publish, then read the
/// published generation's image bytes and prost meta (the meta's
/// `last_log_id` is the durable applied position the snapshot bound).
pub async fn snapshot_image(dir: &TestDir, host: &RaftHost) -> (Vec<u8>, RaftSnapshotMeta) {
    host.trigger_snapshot().await.unwrap();
    let mut last_error = None;
    for _ in 0..200 {
        if let Some(generation) = read_pointer(dir) {
            let gen_dir = generation_dir(dir, generation);
            let meta_path = gen_dir.join("meta");
            if meta_path.exists() {
                let bytes = std::fs::read(&meta_path).unwrap();
                match RaftSnapshotMeta::decode(bytes.as_slice()) {
                    Ok(meta) => {
                        let image = std::fs::read(gen_dir.join("image.redb")).unwrap();
                        return (image, meta);
                    }
                    Err(error) => last_error = Some(error.to_string()),
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("snapshot meta never became readable: {last_error:?}");
}

// ---- slice 4a: three-voter cluster kit --------------------------------------
//
// 0fe7081 removed the standalone `ControlStateMachine` constructor from the
// public surface (it is pub(crate) now), so snapshot installs can only be
// driven externally through the real tonic transport: a prepared member
// joins and the leader seeds it by snapshot. These helpers mirror
// `src/raft/transport_tests.rs` and the checked-in loopback mTLS fixtures
// under `tests/certs/raft` (docs/raft-hosting.md, "Three voters over the
// transport").
#[cfg(feature = "tls")]
mod cluster {
    use super::*;
    use pipestream_search::raft::transport::{certificate_sha256, PeerDirectory, TransportLimits};
    use pipestream_search::raft::ClusterTransport;
    use pipestream_search::security::{ClientTls, ServerTls};
    use std::sync::Arc;

    pub fn pem(name: &str) -> Vec<u8> {
        std::fs::read(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/certs/raft")
                .join(name),
        )
        .unwrap()
    }

    pub fn peer_directory(
        group: &pipestream_search::pb::storage::SourceAuthorityIdentity,
        nodes: &[u64],
    ) -> Arc<PeerDirectory> {
        let directory = PeerDirectory::new(group);
        for node in nodes {
            directory
                .register(
                    *node,
                    certificate_sha256(&pem(&format!("node-{node}.pem"))).unwrap(),
                )
                .unwrap();
        }
        Arc::new(directory)
    }

    fn client_tls(cert: &str) -> ClientTls {
        ClientTls {
            ca_pem: pem("ca.pem"),
            identity_pem: Some((pem(&format!("{cert}.pem")), pem(&format!("{cert}.key.pem")))),
            domain: Some("localhost".into()),
        }
    }

    pub fn node_transport(node: u64, directory: &Arc<PeerDirectory>) -> ClusterTransport {
        ClusterTransport {
            directory: Arc::clone(directory),
            server_tls: ServerTls {
                cert_pem: pem(&format!("node-{node}.pem")),
                key_pem: pem(&format!("node-{node}.key.pem")),
                client_ca_pem: Some(pem("ca.pem")),
            },
            client_tls: client_tls(&format!("node-{node}")),
            listen: "127.0.0.1:0".parse().unwrap(),
            advertise: None,
            limits: TransportLimits {
                max_message_bytes: 1 << 20,
                connect_timeout_ms: 1_000,
            },
        }
    }

    /// The networked twin of [`super::host_config`]: small snapshot chunks
    /// so an image crosses several RPCs; lease/skew under the 150 ms floor.
    pub fn cluster_host_config() -> HostConfig {
        HostConfig {
            snapshot_chunk_bytes: 128 << 10,
            install_snapshot_timeout_ms: 5_000,
            admission_lease_ms: 100,
            clock_skew_ms: 50,
            ..host_config()
        }
    }

    /// Bootstrap a networked group of one voter over loopback mTLS.
    pub async fn bootstrap_cluster_node(
        dir: &TestDir,
        node: u64,
        directory: &Arc<PeerDirectory>,
    ) -> RaftHost {
        RaftHost::bootstrap_cluster(
            dir.path(),
            &identity(SEED),
            node,
            &policy(),
            &limits(),
            &cluster_host_config(),
            node_transport(node, directory),
        )
        .await
        .unwrap()
    }

    /// Prepare and start a member that will be seeded by snapshot; assert it
    /// awaits the group's first snapshot before returning.
    pub async fn start_member_node(
        dir: &TestDir,
        node: u64,
        directory: &Arc<PeerDirectory>,
    ) -> RaftHost {
        RaftHost::prepare_member(dir.path(), &identity(SEED), node, &policy(), &limits()).unwrap();
        let host = RaftHost::start_member(
            dir.path(),
            &identity(SEED),
            node,
            &cluster_host_config(),
            node_transport(node, directory),
        )
        .await
        .unwrap();
        assert!(host.awaiting_snapshot().unwrap());
        host
    }
}
// Re-exported for the raft targets; the non-raft targets link this kit
// without using the cluster helpers.
#[cfg(feature = "tls")]
#[allow(unused_imports)]
pub use cluster::*;

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

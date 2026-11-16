//! The Raft host: owns the store, the log store, the Raft instance and,
//! for a networked member, the transport listener; admits proposals the
//! way the direct paths did and turns the committed reply back into the
//! caller's decision.

mod config;
mod lease;
mod membership;
mod observe;
mod propose;
mod start;

pub use self::config::{HostConfig, NoNetwork};
#[cfg(any(test, feature = "fault-injection"))]
pub use self::lease::GrantGate;
pub use self::lease::LeaseHold;

use super::log_store::RaftLogStore;
use super::reasons;
use super::state_machine::{ControlStateMachine, SharedStore};
use super::types::{validate_proposal, ControlRaft, NodeId};
use crate::pb::storage::{
    raft_proposal::Command, raft_reply::Reply, CapacityConfigureCommand, CapacityTransition,
    ControlInstallCommand, RaftProposal, RaftReply, SourceAuthorityCommand,
    SourceAuthorityIdentity, SourceAuthorityLimits,
};
use crate::pb::AccessPolicy;
use crate::source_authority::{AdmissionLease, LeasedAdmission, SourceAdmission};
use crate::source_authority::{SourceAuthorityStore, VerifiedOwnerCompletion};
use openraft::error::{CheckIsLeaderError, ClientWriteError, RaftError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
#[cfg(feature = "tls")]
use openraft::ChangeMembers;
use openraft::{BasicNode, Config, Raft, RaftMetrics, SnapshotPolicy};
use std::collections::BTreeMap;
#[cfg(feature = "tls")]
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tonic::Status;

#[cfg(feature = "tls")]
use super::transport::{
    Isolation, ListenerHost, PeerDirectory, PeerRejection, PeerRejections, RaftTransportService,
    SnapshotRejections, TonicNetworkFactory, TransportLimits,
};
#[cfg(feature = "tls")]
use crate::security::{ClientTls, ServerTls};

pub const STORE_FILE: &str = "authority.redb";
pub const LOG_FILE: &str = "raft-log.redb";
pub const SNAPSHOT_DIR: &str = "raft-snapshots";

/// The transport material of a networked member.
#[cfg(feature = "tls")]
pub struct ClusterTransport {
    /// Which certificate speaks for which node; this node's own
    /// certificate is registered too.
    pub directory: Arc<PeerDirectory>,
    /// The listener's identity; the cluster CA is required of every peer.
    pub server_tls: ServerTls,
    /// What this node presents when it dials a peer.
    pub client_tls: ClientTls,
    pub listen: std::net::SocketAddr,
    /// The address peers dial, when it is not the bound one.
    pub advertise: Option<String>,
    pub limits: TransportLimits,
}

#[cfg_attr(not(feature = "tls"), allow(dead_code))]
struct Listener {
    addr: std::net::SocketAddr,
    advertised: String,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

/// Every rise of a peer's rejection count writes one log line naming the
/// peer, the RPC, the code, the count and the message: one line per peer
/// per change of count, never per poll.
///
/// A peer that rejects this node's image by digest names bytes that
/// differ from what the meta announces. The image was served by length,
/// so the difference is on this node's disk or on the way: this reads the
/// published image by digest, and when it fails, triggers a build, which
/// images the store afresh into a new generation and publishes it, so the
/// next serve is sound. When the image passes, the bytes changed on the
/// way or at the peer, and the peer's rejection stands as recorded. One
/// check per rejection recorded (each fresh serve the peer rejects is
/// one); a trigger the library drops for a build in flight is repeated
/// at the next rejection, so a corrupt generation is replaced within a few
/// serve cycles and the cycle ends.
#[cfg(feature = "tls")]
async fn rebuild_on_digest_rejection(
    node_id: NodeId,
    mut rejections: watch::Receiver<BTreeMap<NodeId, PeerRejection>>,
    published: Arc<super::state_machine::PublishedImage>,
    raft: Raft<ControlRaft>,
) {
    let mut seen: BTreeMap<NodeId, u64> = BTreeMap::new();
    loop {
        let fresh: Vec<(NodeId, PeerRejection)> = rejections
            .borrow_and_update()
            .iter()
            .filter(|(node, rejection)| seen.get(node).is_none_or(|count| rejection.count > *count))
            .map(|(node, rejection)| (*node, rejection.clone()))
            .collect();
        for (node, rejection) in fresh {
            seen.insert(node, rejection.count);
            super::rejection_log::record(format!(
                "raft member {node_id} : peer {node} rejected {} #{} ({:?}): {}",
                rejection.action, rejection.count, rejection.code, rejection.message
            ));
            if rejection.action != "snapshot" || rejection.code != tonic::Code::DataLoss {
                continue;
            }
            let checked = {
                let published = Arc::clone(&published);
                tokio::task::spawn_blocking(move || published.verify()).await
            };
            if let Ok(Err(_)) = checked {
                let _ = raft.trigger().snapshot().await;
            }
        }
        if rejections.changed().await.is_err() {
            return;
        }
    }
}

pub struct RaftHost {
    raft: Raft<ControlRaft>,
    store: SharedStore,
    log_store: RaftLogStore,
    machine_released: watch::Receiver<()>,
    node_id: NodeId,
    dir: PathBuf,
    lease: Duration,
    read_timeout: Duration,
    /// The anchor of the latest barrier that granted an admission; the
    /// transport withholds this node's vote while an interval that started
    /// there may still be open.
    hold: LeaseHold,
    staging: Arc<super::state_machine::SnapshotStaging>,
    last_build: super::state_machine::LastBuild,
    published: Arc<super::state_machine::PublishedImage>,
    listener: Option<Listener>,
    /// Reacts to a peer's digest rejection of this node's image (see
    /// `rebuild_on_digest_rejection`); aborted at shutdown.
    #[cfg(feature = "tls")]
    digest_watch: Option<tokio::task::JoinHandle<()>>,
    #[cfg(feature = "tls")]
    directory: Option<Arc<PeerDirectory>>,
    #[cfg(feature = "tls")]
    peer_rejections: Option<PeerRejections>,
    #[cfg(feature = "tls")]
    snapshot_rejections: Arc<SnapshotRejections>,
    #[cfg(feature = "tls")]
    isolation: Isolation,
    /// The caller side of a forwarded lease; `None` without a transport.
    #[cfg(feature = "tls")]
    forwarder: Option<super::transport::LeaseForwarder>,
    #[cfg(any(test, feature = "fault-injection"))]
    grant_gate: std::sync::Mutex<Option<Arc<GrantGate>>>,
    #[cfg(any(test, feature = "fault-injection"))]
    snapshot_gates: Arc<super::state_machine::SnapshotGates>,
    #[cfg(any(test, feature = "fault-injection"))]
    install_fault: super::state_machine::InstallFault,
}

/// The on-disk state of a node that is not running: its log's purge state
/// and its store's position. For crash-recovery harnesses.
#[cfg(any(test, feature = "fault-injection"))]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeInspection {
    pub log: super::log_store::RaftLogInspection,
    /// The applied index the store recorded with its last entry.
    pub applied: Option<u64>,
    /// Whether the store is a prepared member's, awaiting its first image.
    pub awaiting_snapshot: bool,
}

/// Open the log and the store in `dir` on their own, read their state and
/// close them. Refuses while a host holds either.
#[cfg(any(test, feature = "fault-injection"))]
pub fn inspect_node(
    dir: &Path,
    identity: &SourceAuthorityIdentity,
    node_id: NodeId,
) -> Result<NodeInspection, Status> {
    let log = RaftLogStore::inspect(&dir.join(LOG_FILE), identity, node_id)?;
    let store = SourceAuthorityStore::open(&dir.join(STORE_FILE), identity)?;
    let applied = store
        .raft_applied()?
        .and_then(|a| a.last_applied)
        .map(|id| id.index);
    let awaiting_snapshot = store.awaiting_snapshot()?;
    Ok(NodeInspection {
        log,
        applied,
        awaiting_snapshot,
    })
}

fn refusal_status(refusal: crate::pb::storage::RaftRefusal) -> Status {
    Status::new(tonic::Code::from_i32(refusal.code as i32), refusal.message)
}

fn control_reply(reply: RaftReply) -> Result<crate::pb::storage::SourceAuthorityDecision, Status> {
    match reply.reply {
        Some(Reply::Control(decision)) => Ok(decision),
        Some(Reply::Refusal(refusal)) => Err(refusal_status(refusal)),
        _ => Err(Status::internal(
            "raft reply kind differs from the proposal",
        )),
    }
}

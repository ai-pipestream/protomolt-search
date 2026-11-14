//! The Raft host: owns the store, the log store, the Raft instance and,
//! for a networked member, the transport listener; admits proposals the
//! way the direct paths did and turns the committed reply back into the
//! caller's decision.
use super::log_store::RaftLogStore;
use super::reasons;
use super::state_machine::{ControlStateMachine, SharedStore};
use super::types::{validate_proposal, ControlRaft, NodeId};
use crate::control_plane::RetiredLegacyControl;
use crate::pb::storage::{
    raft_proposal::Command, raft_reply::Reply, CapacityConfigureCommand, CapacityTransition,
    ControlImportCommand, RaftProposal, RaftReply, SourceAuthorityCommand, SourceAuthorityIdentity,
    SourceAuthorityLimits,
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
    Isolation, PeerDirectory, PeerRejection, PeerRejections, RaftTransportService,
    SnapshotRejections, TonicNetworkFactory, TransportLimits,
};
#[cfg(feature = "tls")]
use crate::security::{ClientTls, ServerTls};

mod admission;
mod lifecycle;
mod membership;

pub const STORE_FILE: &str = "authority.redb";
pub const LOG_FILE: &str = "raft-log.redb";
pub const SNAPSHOT_DIR: &str = "raft-snapshots";

/// A network factory for a group of one: every RPC is unreachable. The
/// tonic transport replaces it for multi-node groups.
#[derive(Clone, Default)]
pub struct NoNetwork;

impl RaftNetworkFactory<ControlRaft> for NoNetwork {
    type Network = NoNetwork;

    async fn new_client(&mut self, _target: NodeId, _node: &BasicNode) -> Self::Network {
        NoNetwork
    }
}

fn unreachable<E: std::error::Error>() -> openraft::error::RPCError<NodeId, BasicNode, E> {
    openraft::error::RPCError::Unreachable(Unreachable::new(&std::io::Error::other(
        "no transport is configured for this node",
    )))
}

impl RaftNetwork<ControlRaft> for NoNetwork {
    async fn append_entries(
        &mut self,
        _rpc: AppendEntriesRequest<ControlRaft>,
        _option: RPCOption,
    ) -> Result<
        AppendEntriesResponse<NodeId>,
        openraft::error::RPCError<NodeId, BasicNode, RaftError<NodeId>>,
    > {
        Err(unreachable())
    }

    async fn install_snapshot(
        &mut self,
        _rpc: InstallSnapshotRequest<ControlRaft>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        openraft::error::RPCError<
            NodeId,
            BasicNode,
            RaftError<NodeId, openraft::error::InstallSnapshotError>,
        >,
    > {
        Err(unreachable())
    }

    async fn vote(
        &mut self,
        _rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, openraft::error::RPCError<NodeId, BasicNode, RaftError<NodeId>>>
    {
        Err(unreachable())
    }
}

/// Runtime knobs of the host; all durations in milliseconds.
#[derive(Debug, Clone)]
pub struct HostConfig {
    pub cluster_name: String,
    pub heartbeat_interval_ms: u64,
    pub election_timeout_min_ms: u64,
    pub election_timeout_max_ms: u64,
    /// Build a snapshot after this many applied entries since the last.
    pub snapshot_logs_since_last: u64,
    /// Largest store image this node accepts from a peer, in bytes. A
    /// build of this node's own store is not bounded by it: a store over
    /// a peer's bound is rejected at that peer, by name, and the leader
    /// records the rejection (`RaftHost::peer_rejections`). The bound must be
    /// at least the group's store image for this node to be seeded after
    /// a wipe or caught up by a snapshot; the build report
    /// (`RaftHost::last_snapshot_build`) compares the two.
    pub max_snapshot_bytes: u64,
    /// One snapshot chunk on the wire.
    pub snapshot_chunk_bytes: u64,
    /// Sending and installing one snapshot chunk must finish within this.
    pub install_snapshot_timeout_ms: u64,
    /// Entries per append; the transport halves it when a batch exceeds
    /// its message bound.
    pub max_payload_entries: u64,
    /// Entries kept behind a snapshot before the log is purged; adding a
    /// member purges to the snapshot regardless, so the member is seeded
    /// by the verified image.
    pub max_in_snapshot_log_to_keep: u64,
    /// How long a leased admission stays authoritative after the
    /// linearizable read that granted it (docs/raft-admission.md).
    pub admission_lease_ms: u64,
    /// Clock skew the lease budget tolerates between members.
    pub clock_skew_ms: u64,
}

impl Default for HostConfig {
    fn default() -> Self {
        Self {
            cluster_name: "source-authority".into(),
            heartbeat_interval_ms: 250,
            election_timeout_min_ms: 1_000,
            election_timeout_max_ms: 2_000,
            snapshot_logs_since_last: 1_024,
            max_snapshot_bytes: 4 << 30,
            snapshot_chunk_bytes: 1 << 20,
            install_snapshot_timeout_ms: 10_000,
            max_payload_entries: 64,
            max_in_snapshot_log_to_keep: 1_000,
            admission_lease_ms: 500,
            clock_skew_ms: 250,
        }
    }
}

impl HostConfig {
    /// A lease plus the skew budget must end before any other member can
    /// win an election; otherwise an isolated leader could admit work a
    /// surviving quorum already revoked.
    pub fn validate(&self) -> Result<(), Status> {
        if self.admission_lease_ms == 0 {
            return Err(Status::invalid_argument(
                "admission_lease_ms must be positive",
            ));
        }
        if self.admission_lease_ms.saturating_add(self.clock_skew_ms) > self.election_timeout_min_ms
        {
            return Err(Status::invalid_argument(format!(
                "admission_lease_ms {} plus clock_skew_ms {} must not exceed election_timeout_min_ms {}",
                self.admission_lease_ms, self.clock_skew_ms, self.election_timeout_min_ms
            )));
        }
        if self.max_snapshot_bytes == 0 {
            return Err(Status::invalid_argument(
                "max_snapshot_bytes must be positive",
            ));
        }
        if self.snapshot_chunk_bytes == 0 || self.snapshot_chunk_bytes > self.max_snapshot_bytes {
            return Err(Status::invalid_argument(
                "snapshot_chunk_bytes must be positive and within max_snapshot_bytes",
            ));
        }
        if self.install_snapshot_timeout_ms == 0 || self.max_payload_entries == 0 {
            return Err(Status::invalid_argument(
                "install_snapshot_timeout_ms and max_payload_entries must be positive",
            ));
        }
        Ok(())
    }

    /// The bound on a linearizable read: an isolated leader cannot collect
    /// a quorum, and past the longest election it is no longer the leader
    /// anyone else believes in.
    fn read_timeout(&self) -> Duration {
        Duration::from_millis(self.election_timeout_max_ms)
    }

    /// The timing values the lease argument depends on, as every peer must
    /// hold them; the transport refuses a peer whose values differ.
    pub fn timing_agreement(&self) -> String {
        format!(
            "v1:heartbeat={}:election={}-{}:lease={}:skew={}",
            self.heartbeat_interval_ms,
            self.election_timeout_min_ms,
            self.election_timeout_max_ms,
            self.admission_lease_ms,
            self.clock_skew_ms
        )
    }
}

/// While an admission interval anchored at a read barrier may still be
/// open, this node withholds its vote from every candidate: the pinned
/// library refreshes a leader's own vote timer only when its vote changes,
/// so an established leader would otherwise grant a vote to an up-to-date
/// candidate at once, and a vote quorum could then avoid every follower
/// that acknowledged the barrier (docs/raft-admission.md).
#[derive(Clone)]
pub struct LeaseHold {
    anchor: Arc<std::sync::RwLock<Option<std::time::Instant>>>,
    window: Duration,
}

impl LeaseHold {
    pub(crate) fn new(window: Duration) -> Self {
        Self {
            anchor: Arc::new(std::sync::RwLock::new(None)),
            window,
        }
    }

    pub(crate) fn extend(&self, anchor: std::time::Instant) {
        let mut held = self.anchor.write().unwrap_or_else(|e| e.into_inner());
        if held.is_none_or(|current| anchor > current) {
            *held = Some(anchor);
        }
    }

    /// Whether an interval anchored at the latest barrier is still open.
    pub(crate) fn holds(&self, now: std::time::Instant) -> bool {
        self.anchor
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .is_some_and(|anchor| now < anchor + self.window)
    }
}

/// A test and fault-injection hook: the next `with_admission` pauses after
/// its read barrier and before its grant until `release`.
#[cfg(any(test, feature = "fault-injection"))]
pub struct GrantGate {
    reached: tokio::sync::watch::Sender<bool>,
    release: tokio::sync::Notify,
}

#[cfg(any(test, feature = "fault-injection"))]
impl GrantGate {
    fn new() -> Self {
        Self {
            reached: tokio::sync::watch::Sender::new(false),
            release: tokio::sync::Notify::new(),
        }
    }

    /// Resolves once an admission has passed its barrier and is paused.
    pub async fn reached(&self) {
        let mut receiver = self.reached.subscribe();
        let _ = receiver.wait_for(|reached| *reached).await;
    }

    pub fn release(&self) {
        self.release.notify_one();
    }
}

/// The transport material of a networked member (docs/raft-hosting.md).
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
/// per change of count, never per poll (docs/raft-hosting.md, "Operator
/// surface").
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
    /// The caller side of a forwarded lease (docs/raft-admission.md, "A
    /// lease forwarded from the leader"); `None` without a transport.
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

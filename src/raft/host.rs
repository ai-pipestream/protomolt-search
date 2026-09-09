//! The Raft host: owns the store, the log store, the Raft instance and,
//! for a networked member, the transport listener; admits proposals the
//! way the direct paths did and turns the committed reply back into the
//! caller's decision.
use super::log_store::RaftLogStore;
use super::state_machine::{ControlStateMachine, SharedStore};
use super::types::{validate_proposal, ControlRaft, NodeId};
use crate::control_plane::RetiredLegacyControl;
use crate::pb::storage::{
    raft_proposal::Command, raft_reply::Reply, CapacityConfigureCommand, CapacityTransition,
    ControlImportCommand, RaftProposal, RaftReply, SourceAuthorityCommand, SourceAuthorityIdentity,
    SourceAuthorityLimits,
};
use crate::pb::AccessPolicy;
use crate::source_authority::{AdmissionLease, SourceAdmission};
use crate::source_authority::{SourceAuthorityStore, VerifiedOwnerCompletion};
use openraft::error::{ClientWriteError, RaftError, Unreachable};
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
    Isolation, PeerDirectory, RaftTransportService, TonicNetworkFactory, TransportLimits,
};
#[cfg(feature = "tls")]
use crate::security::{ClientTls, ServerTls};

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
    /// Largest store image a snapshot may carry, in bytes.
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
    listener: Option<Listener>,
    #[cfg(feature = "tls")]
    directory: Option<Arc<PeerDirectory>>,
    #[cfg(feature = "tls")]
    isolation: Isolation,
    #[cfg(any(test, feature = "fault-injection"))]
    grant_gate: std::sync::Mutex<Option<Arc<GrantGate>>>,
    #[cfg(any(test, feature = "fault-injection"))]
    snapshot_gates: Arc<super::state_machine::SnapshotGates>,
}

impl RaftHost {
    fn raft_config(config: &HostConfig) -> Result<Arc<Config>, Status> {
        config.validate()?;
        let raft = Config {
            cluster_name: config.cluster_name.clone(),
            heartbeat_interval: config.heartbeat_interval_ms,
            election_timeout_min: config.election_timeout_min_ms,
            election_timeout_max: config.election_timeout_max_ms,
            snapshot_policy: SnapshotPolicy::LogsSinceLast(config.snapshot_logs_since_last),
            snapshot_max_chunk_size: config.snapshot_chunk_bytes,
            install_snapshot_timeout: config.install_snapshot_timeout_ms,
            max_payload_entries: config.max_payload_entries,
            max_in_snapshot_log_to_keep: config.max_in_snapshot_log_to_keep,
            ..Default::default()
        }
        .validate()
        .map_err(|e| Status::invalid_argument(format!("raft config: {e}")))?;
        Ok(Arc::new(raft))
    }

    /// Explicit one-time bootstrap of a group of one voter: creates the
    /// store and the log in `dir` and commits the initial membership.
    pub async fn bootstrap_single(
        dir: &Path,
        identity: &SourceAuthorityIdentity,
        node_id: NodeId,
        addr: &str,
        policy: &AccessPolicy,
        limits: &SourceAuthorityLimits,
        config: &HostConfig,
    ) -> Result<Self, Status> {
        let store = SourceAuthorityStore::create(&dir.join(STORE_FILE), identity, policy, limits)?;
        let log_store = RaftLogStore::create(&dir.join(LOG_FILE), identity, node_id)?;
        let host =
            Self::start_with(dir, store, log_store, node_id, config, NoNetwork, None).await?;
        host.initialize(addr).await?;
        Ok(host)
    }

    /// Start from existing durable state. A missing store or log refuses;
    /// nothing is created on the normal startup path.
    pub async fn start(
        dir: &Path,
        identity: &SourceAuthorityIdentity,
        node_id: NodeId,
        config: &HostConfig,
    ) -> Result<Self, Status> {
        let store = SourceAuthorityStore::open(&dir.join(STORE_FILE), identity)?;
        let log_store = RaftLogStore::open(&dir.join(LOG_FILE), identity, node_id)?;
        Self::start_with(dir, store, log_store, node_id, config, NoNetwork, None).await
    }

    /// Bootstrap a networked group of one voter: the first member, from
    /// which every later member is seeded by snapshot.
    #[cfg(feature = "tls")]
    pub async fn bootstrap_cluster(
        dir: &Path,
        identity: &SourceAuthorityIdentity,
        node_id: NodeId,
        policy: &AccessPolicy,
        limits: &SourceAuthorityLimits,
        config: &HostConfig,
        transport: ClusterTransport,
    ) -> Result<Self, Status> {
        let store = SourceAuthorityStore::create(&dir.join(STORE_FILE), identity, policy, limits)?;
        let log_store = RaftLogStore::create(&dir.join(LOG_FILE), identity, node_id)?;
        let host =
            Self::start_networked(dir, identity, store, log_store, node_id, config, transport)
                .await?;
        let advertised = host.advertised_addr().expect("networked host").to_string();
        host.initialize(&advertised).await?;
        Ok(host)
    }

    /// Create the durable state of a member that will join by snapshot:
    /// a store marked pending, on which no log entry applies until the
    /// group's image replaces it, and an empty log. No host is started.
    pub fn prepare_member(
        dir: &Path,
        identity: &SourceAuthorityIdentity,
        node_id: NodeId,
        policy: &AccessPolicy,
        limits: &SourceAuthorityLimits,
    ) -> Result<(), Status> {
        std::fs::create_dir_all(dir)
            .map_err(|e| Status::internal(format!("member directory: {e}")))?;
        SourceAuthorityStore::create_member(&dir.join(STORE_FILE), identity, policy, limits)?;
        RaftLogStore::create(&dir.join(LOG_FILE), identity, node_id)?;
        Ok(())
    }

    /// Start a networked member from existing durable state (a prepared
    /// member, or any member restarting). Nothing is created.
    #[cfg(feature = "tls")]
    pub async fn start_member(
        dir: &Path,
        identity: &SourceAuthorityIdentity,
        node_id: NodeId,
        config: &HostConfig,
        transport: ClusterTransport,
    ) -> Result<Self, Status> {
        let store = SourceAuthorityStore::open(&dir.join(STORE_FILE), identity)?;
        let log_store = RaftLogStore::open(&dir.join(LOG_FILE), identity, node_id)?;
        Self::start_networked(dir, identity, store, log_store, node_id, config, transport).await
    }

    #[cfg(feature = "tls")]
    async fn start_networked(
        dir: &Path,
        identity: &SourceAuthorityIdentity,
        store: SourceAuthorityStore,
        log_store: RaftLogStore,
        node_id: NodeId,
        config: &HostConfig,
        transport: ClusterTransport,
    ) -> Result<Self, Status> {
        transport.limits.validate(config.snapshot_chunk_bytes)?;
        if transport.directory.group() != identity {
            return Err(Status::invalid_argument(
                "peer directory is for another group",
            ));
        }
        if !transport.directory.knows(node_id) {
            return Err(Status::failed_precondition(format!(
                "peer directory does not register this node ({node_id}); its own certificate must be bound before it serves"
            )));
        }
        let socket = tokio::net::TcpListener::bind(transport.listen)
            .await
            .map_err(|e| Status::unavailable(format!("raft listener {}: {e}", transport.listen)))?;
        let addr = socket
            .local_addr()
            .map_err(|e| Status::internal(format!("raft listener address: {e}")))?;
        let isolation = Isolation::default();
        let timing = config.timing_agreement();
        let factory = TonicNetworkFactory::new(
            node_id,
            Arc::clone(&transport.directory),
            transport.client_tls,
            transport.limits.clone(),
            isolation.clone(),
            timing.clone(),
        );
        let mut host =
            Self::start_with(dir, store, log_store, node_id, config, factory, None).await?;
        let service = RaftTransportService::new(
            host.raft.clone(),
            node_id,
            Arc::clone(&transport.directory),
            isolation.clone(),
            timing,
            host.hold.clone(),
            Arc::clone(&host.staging),
            &transport.limits,
        );
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let server = tonic::transport::Server::builder()
            .tls_config(transport.server_tls.server_config(true))
            .map_err(|e| Status::internal(format!("raft listener TLS: {e}")))?
            .add_service(service.into_server(&transport.limits))
            .serve_with_incoming_shutdown(crate::harness::nodelay_incoming(socket), async {
                let _ = stopped.await;
            });
        let task = tokio::spawn(server);
        host.listener = Some(Listener {
            addr,
            advertised: transport.advertise.unwrap_or_else(|| addr.to_string()),
            stop: Some(stop),
            task,
        });
        host.directory = Some(transport.directory);
        host.isolation = isolation;
        Ok(host)
    }

    async fn start_with<N: RaftNetworkFactory<ControlRaft>>(
        dir: &Path,
        store: SourceAuthorityStore,
        log_store: RaftLogStore,
        node_id: NodeId,
        config: &HostConfig,
        network: N,
        listener: Option<Listener>,
    ) -> Result<Self, Status> {
        let machine =
            ControlStateMachine::new(store, &dir.join(SNAPSHOT_DIR), config.max_snapshot_bytes)?;
        let shared = machine.shared_store();
        let staging = machine.staging();
        #[cfg(any(test, feature = "fault-injection"))]
        let snapshot_gates = machine.gates();
        let mut log_store = log_store;
        log_store
            .bind_applied_floor(Arc::clone(&shared))
            .map_err(|e| Status::data_loss(format!("raft log and store disagree: {e}")))?;
        let raft = Raft::new(
            node_id,
            Self::raft_config(config)?,
            network,
            log_store.clone(),
            machine,
        )
        .await
        .map_err(|e| Status::internal(format!("raft start: {e}")))?;
        Ok(Self {
            raft,
            store: shared,
            log_store,
            node_id,
            dir: dir.to_path_buf(),
            lease: Duration::from_millis(config.admission_lease_ms),
            read_timeout: config.read_timeout(),
            hold: LeaseHold::new(Duration::from_millis(config.election_timeout_max_ms)),
            staging,
            listener,
            #[cfg(feature = "tls")]
            directory: None,
            #[cfg(feature = "tls")]
            isolation: Isolation::default(),
            #[cfg(any(test, feature = "fault-injection"))]
            grant_gate: std::sync::Mutex::new(None),
            #[cfg(any(test, feature = "fault-injection"))]
            snapshot_gates,
        })
    }

    async fn initialize(&self, addr: &str) -> Result<(), Status> {
        let mut members = BTreeMap::new();
        members.insert(
            self.node_id,
            BasicNode {
                addr: addr.to_string(),
            },
        );
        self.raft
            .initialize(members)
            .await
            .map_err(|e| Status::internal(format!("raft initialize: {e}")))?;
        self.raft
            .wait(Some(Duration::from_secs(30)))
            .state(openraft::ServerState::Leader, "bootstrap leader")
            .await
            .map_err(|e| Status::internal(format!("raft bootstrap: {e}")))?;
        Ok(())
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Where the transport listens, for a networked member.
    pub fn listen_addr(&self) -> Option<std::net::SocketAddr> {
        self.listener.as_ref().map(|l| l.addr)
    }

    /// The address peers dial, for a networked member.
    pub fn advertised_addr(&self) -> Option<&str> {
        self.listener.as_ref().map(|l| l.advertised.as_str())
    }

    /// Every entry the log holds, in index order: a read for audits.
    pub fn log_entries(&self) -> Result<Vec<openraft::Entry<ControlRaft>>, Status> {
        self.log_store.entries()
    }

    pub fn metrics(&self) -> watch::Receiver<RaftMetrics<NodeId, BasicNode>> {
        self.raft.metrics()
    }

    /// Wait for a metrics condition (state, applied index, snapshot).
    pub fn wait(
        &self,
        timeout: Option<Duration>,
    ) -> openraft::metrics::Wait<NodeId, BasicNode, openraft::TokioRuntime> {
        self.raft.wait(timeout)
    }

    /// The applied position the store recorded, as a read.
    pub fn applied_position(&self) -> Result<Option<crate::pb::storage::RaftLogId>, Status> {
        Ok(self.store()?.raft_applied()?.and_then(|a| a.last_applied))
    }

    /// Whether this member's store is still the prepared genesis image
    /// waiting for the group's first snapshot.
    pub fn awaiting_snapshot(&self) -> Result<bool, Status> {
        self.store()?.awaiting_snapshot()
    }

    /// The leader this node currently believes in, if any: an applied
    /// view, never an admission.
    pub fn believed_leader(&self) -> Option<NodeId> {
        self.raft.metrics().borrow().current_leader
    }

    /// Run owner-side work under a leased admission (docs/raft-admission.md).
    /// The lease is anchored before the read barrier is invoked: the
    /// barrier's heartbeats are sent after that instant, every follower
    /// that acknowledges them refreshes its own leader lease later still,
    /// and none of them grants a vote before that plus the election
    /// ceiling. The interval therefore includes every response, catch-up
    /// and scheduling delay between the barrier and the grant, and a grant
    /// whose interval has already elapsed is refused. The read itself is
    /// bounded: an isolated leader collects no quorum and refuses.
    pub async fn with_admission<T>(
        &self,
        principal: &str,
        run: impl FnOnce(&SourceAdmission<'_>) -> Result<T, Status>,
    ) -> Result<T, Status> {
        let lease = AdmissionLease {
            anchor: std::time::Instant::now(),
            anchor_wall: std::time::SystemTime::now(),
            ttl: self.lease,
        };
        match tokio::time::timeout(self.read_timeout, self.raft.ensure_linearizable()).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                return Err(Status::unavailable(format!(
                    "admission needs a linearizable read: {e}"
                )))
            }
            Err(_) => {
                return Err(Status::unavailable(format!(
                    "admission needs a linearizable read: no quorum acknowledged within {} ms",
                    self.read_timeout.as_millis()
                )))
            }
        }
        self.hold.extend(lease.anchor);
        #[cfg(any(test, feature = "fault-injection"))]
        {
            let gate = self.grant_gate.lock().unwrap().take();
            if let Some(gate) = gate {
                gate.reached.send_replace(true);
                gate.release.notified().await;
            }
        }
        let store = self.store()?;
        let admission = store.leased_admission(principal, lease)?;
        run(&admission)
    }

    /// Arm a pause between the next admission's read barrier and its grant.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn arm_grant_gate(&self) -> Arc<GrantGate> {
        let gate = Arc::new(GrantGate::new());
        *self.grant_gate.lock().unwrap() = Some(Arc::clone(&gate));
        gate
    }

    /// Arm a pause in the next snapshot build, once its generation
    /// directory is in place and before it publishes.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn arm_snapshot_build_gate(&self) -> Arc<super::state_machine::SnapshotGate> {
        self.snapshot_gates.arm_build()
    }

    /// Arm a pause in the next snapshot serve, after it read the pointer
    /// and before it opens the generation.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn arm_snapshot_serve_gate(&self) -> Arc<super::state_machine::SnapshotGate> {
        self.snapshot_gates.arm_serve()
    }

    /// The snapshot the state machine serves to a peer right now, read in
    /// full on the state machine worker's path: its stored meta and the
    /// image bytes. `None` when no generation is published.
    #[cfg(any(test, feature = "fault-injection"))]
    pub async fn serve_snapshot(
        &self,
    ) -> Result<Option<(crate::pb::storage::RaftSnapshotMeta, Vec<u8>)>, Status> {
        let Some(snapshot) = self
            .raft
            .get_snapshot()
            .await
            .map_err(|e| Status::internal(format!("raft snapshot read: {e}")))?
        else {
            return Ok(None);
        };
        let signature = super::state_machine::parse_snapshot_id(&snapshot.meta.snapshot_id)?;
        let group = self.store()?.identity().clone();
        let meta = super::state_machine::meta_to_proto(&group, &snapshot.meta, &signature);
        let mut file = snapshot.snapshot;
        let mut bytes = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut *file, &mut bytes)
            .await
            .map_err(|e| Status::internal(format!("raft snapshot read: {e}")))?;
        Ok(Some((meta, bytes)))
    }

    /// A handle that yields the current store on every call, for consumers
    /// that outlive one snapshot install (the relay's map source).
    pub fn store_handle(
        &self,
    ) -> impl Fn() -> Result<SourceAuthorityStore, Status> + Send + Sync + 'static {
        let shared = Arc::clone(&self.store);
        move || {
            shared
                .read()
                .map_err(|_| Status::internal("host store lock poisoned"))?
                .clone()
                .ok_or_else(|| Status::unavailable("store is being replaced by a snapshot install"))
        }
    }

    /// The current store handle, for reads (maps, snapshots, decisions).
    /// Direct commands on it refuse; propose through the host.
    pub fn store(&self) -> Result<SourceAuthorityStore, Status> {
        self.store
            .read()
            .map_err(|_| Status::internal("host store lock poisoned"))?
            .clone()
            .ok_or_else(|| Status::unavailable("store is being replaced by a snapshot install"))
    }

    fn write_error<E: std::fmt::Display>(
        error: RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>,
        what: E,
    ) -> Status {
        match error {
            RaftError::APIError(ClientWriteError::ForwardToLeader(forward)) => {
                Status::unavailable(format!("not the leader; leader is {:?}", forward.leader_id))
            }
            RaftError::APIError(ClientWriteError::ChangeMembershipError(change)) => {
                Status::failed_precondition(format!("{what}: {change}"))
            }
            other => Status::unavailable(format!("{what}: {other}")),
        }
    }

    /// Submit an admitted proposal and wait for its committed reply. Private:
    /// every public entry admits its command first.
    async fn propose(&self, proposal: RaftProposal) -> Result<RaftReply, Status> {
        validate_proposal(&proposal)?;
        let response = self
            .raft
            .client_write(proposal)
            .await
            .map_err(|error| Self::write_error(error, "raft proposal"))?;
        Ok(response.data)
    }

    /// A control command through the log. ConfirmReady needs the holder
    /// path below; the general path refuses it at proposal.
    pub async fn propose_command(
        &self,
        principal: &str,
        command: &SourceAuthorityCommand,
    ) -> Result<crate::pb::storage::SourceAuthorityDecision, Status> {
        if matches!(
            command.action,
            Some(crate::pb::storage::source_authority_command::Action::ConfirmReady(_))
        ) {
            return Err(Status::permission_denied(
                "readiness confirmation requires the hosting adapter with the managed binding",
            ));
        }
        let reply = self
            .propose(RaftProposal {
                format_version: 1,
                principal: principal.into(),
                command: Some(Command::Control(command.clone())),
            })
            .await?;
        control_reply(reply)
    }

    /// Readiness confirmation admitted against the held binding at proposal.
    pub async fn propose_confirm_ready(
        &self,
        principal: &str,
        command: &SourceAuthorityCommand,
        verified: &VerifiedOwnerCompletion,
    ) -> Result<crate::pb::storage::SourceAuthorityDecision, Status> {
        self.store()?
            .proposal_admits_readiness(principal, command, verified)?;
        let reply = self
            .propose(RaftProposal {
                format_version: 1,
                principal: principal.into(),
                command: Some(Command::Control(command.clone())),
            })
            .await?;
        control_reply(reply)
    }

    /// Import steps through the log; a Begin needs the retirement holder.
    pub async fn propose_import(
        &self,
        principal: &str,
        command: &ControlImportCommand,
        retired: Option<&RetiredLegacyControl>,
    ) -> Result<crate::pb::storage::ControlImportDecision, Status> {
        self.store()?
            .proposal_admits_import(principal, command, retired)?;
        let reply = self
            .propose(RaftProposal {
                format_version: 1,
                principal: principal.into(),
                command: Some(Command::Import(command.clone())),
            })
            .await?;
        match reply.reply {
            Some(Reply::Import(decision)) => Ok(decision),
            Some(Reply::Refusal(refusal)) => Err(refusal_status(refusal)),
            _ => Err(Status::internal(
                "raft reply kind differs from the proposal",
            )),
        }
    }

    pub async fn propose_capacity_configure(
        &self,
        principal: &str,
        command: &CapacityConfigureCommand,
    ) -> Result<crate::pb::storage::CapacityConfigureDecision, Status> {
        let reply = self
            .propose(RaftProposal {
                format_version: 1,
                principal: principal.into(),
                command: Some(Command::Capacity(command.clone())),
            })
            .await?;
        match reply.reply {
            Some(Reply::Capacity(decision)) => Ok(decision),
            Some(Reply::Refusal(refusal)) => Err(refusal_status(refusal)),
            _ => Err(Status::internal(
                "raft reply kind differs from the proposal",
            )),
        }
    }

    pub async fn propose_capacity_transition(
        &self,
        principal: &str,
        transition: &CapacityTransition,
    ) -> Result<crate::pb::storage::CapacityTransitionReceipt, Status> {
        let reply = self
            .propose(RaftProposal {
                format_version: 1,
                principal: principal.into(),
                command: Some(Command::Observation(transition.clone())),
            })
            .await?;
        match reply.reply {
            Some(Reply::Observation(receipt)) => Ok(receipt),
            Some(Reply::Refusal(refusal)) => Err(refusal_status(refusal)),
            _ => Err(Status::internal(
                "raft reply kind differs from the proposal",
            )),
        }
    }

    /// Ask the state machine for a snapshot now (tests and operators). A
    /// store that has applied nothing yet (a prepared member before its
    /// first install) has nothing to snapshot; the library would send the
    /// build regardless and treat the builder's refusal as fatal, so the
    /// trigger is refused here by name.
    pub async fn trigger_snapshot(&self) -> Result<(), Status> {
        if self.applied_position()?.is_none() {
            return Err(Status::failed_precondition(
                "no applied position; nothing to snapshot",
            ));
        }
        self.raft
            .trigger()
            .snapshot()
            .await
            .map_err(|e| Status::internal(format!("raft snapshot trigger: {e}")))
    }

    // -----------------------------------------------------------------
    // Membership (docs/raft-hosting.md): learner, promotion, removal
    // -----------------------------------------------------------------

    /// Add a prepared member as a learner and wait until it is caught up.
    /// Leader only. The member's certificate must be registered in this
    /// node's directory. A snapshot is built and the log purged to it
    /// first, so the learner is seeded by the verified image rather than
    /// by replaying onto its own genesis.
    #[cfg(feature = "tls")]
    pub async fn add_learner(&self, node_id: NodeId, addr: &str) -> Result<(), Status> {
        let directory = self.directory.as_ref().ok_or_else(|| {
            Status::failed_precondition("membership changes need a networked member")
        })?;
        if !directory.knows(node_id) {
            return Err(Status::failed_precondition(format!(
                "node {node_id} has no registered certificate; bind it before it joins"
            )));
        }
        if addr.is_empty() || addr.len() > 1024 {
            return Err(Status::invalid_argument(
                "member address must be 1..1024 bytes",
            ));
        }
        self.seed_boundary().await?;
        // The library's blocking mode returns once the learner is within
        // its lag threshold; a member is caught up when it holds the
        // membership entry that added it.
        let added = self
            .raft
            .add_learner(
                node_id,
                BasicNode {
                    addr: addr.to_string(),
                },
                false,
            )
            .await
            .map_err(|e| Self::write_error(e, format!("add learner {node_id}")))?
            .log_id;
        self.raft
            .wait(Some(Duration::from_secs(60)))
            .metrics(
                |metrics| {
                    metrics
                        .replication
                        .as_ref()
                        .and_then(|replication| replication.get(&node_id).copied().flatten())
                        .is_some_and(|matching| matching >= added)
                },
                "learner caught up",
            )
            .await
            .map_err(|e| Status::unavailable(format!("learner {node_id} did not catch up: {e}")))?;
        Ok(())
    }

    /// A snapshot at the current applied position with the log purged up
    /// to it: the boundary every new member is seeded from.
    async fn seed_boundary(&self) -> Result<(), Status> {
        let metrics = self.raft.metrics().borrow().clone();
        if metrics.state != openraft::ServerState::Leader {
            return Err(Status::unavailable(format!(
                "not the leader; leader is {:?}",
                metrics.current_leader
            )));
        }
        let applied = metrics
            .last_applied
            .ok_or_else(|| Status::failed_precondition("nothing is applied yet"))?;
        if metrics.snapshot != Some(applied) {
            self.trigger_snapshot().await?;
            self.raft
                .wait(Some(Duration::from_secs(60)))
                .snapshot(applied, "seed snapshot")
                .await
                .map_err(|e| Status::unavailable(format!("seed snapshot: {e}")))?;
        }
        if metrics.purged != Some(applied) {
            self.raft
                .trigger()
                .purge_log(applied.index)
                .await
                .map_err(|e| Status::internal(format!("purge log: {e}")))?;
            self.raft
                .wait(Some(Duration::from_secs(60)))
                .purged(Some(applied), "seed purge")
                .await
                .map_err(|e| Status::unavailable(format!("seed purge: {e}")))?;
        }
        Ok(())
    }

    /// Promote caught-up learners to voters through the library's joint
    /// consensus. Leader only.
    #[cfg(feature = "tls")]
    pub async fn promote(&self, learners: BTreeSet<NodeId>) -> Result<(), Status> {
        if learners.is_empty() {
            return Err(Status::invalid_argument("no learners to promote"));
        }
        self.raft
            .change_membership(ChangeMembers::AddVoterIds(learners), false)
            .await
            .map_err(|e| Self::write_error(e, "promote"))?;
        Ok(())
    }

    /// Remove a member (voter or learner) from the group through joint
    /// consensus. Leader only; a removed member's proposals and admissions
    /// refuse from the committed change on.
    #[cfg(feature = "tls")]
    pub async fn remove_member(&self, node_id: NodeId) -> Result<(), Status> {
        let membership = self.raft.metrics().borrow().membership_config.clone();
        let voter = membership.membership().voter_ids().any(|id| id == node_id);
        if voter {
            self.raft
                .change_membership(
                    ChangeMembers::RemoveVoters(BTreeSet::from([node_id])),
                    false,
                )
                .await
                .map_err(|e| Self::write_error(e, format!("remove voter {node_id}")))?;
        } else {
            self.raft
                .change_membership(ChangeMembers::RemoveNodes(BTreeSet::from([node_id])), false)
                .await
                .map_err(|e| Self::write_error(e, format!("remove learner {node_id}")))?;
        }
        Ok(())
    }

    /// Fault injection: refuse every RPC to and from these peers.
    #[cfg(all(feature = "tls", any(test, feature = "fault-injection")))]
    pub fn isolate(&self, peers: impl IntoIterator<Item = NodeId>) {
        self.isolation.isolate(peers);
    }

    #[cfg(all(feature = "tls", any(test, feature = "fault-injection")))]
    pub fn heal(&self) {
        self.isolation.heal();
    }

    pub async fn shutdown(mut self) -> Result<(), Status> {
        let stopped = self
            .raft
            .shutdown()
            .await
            .map_err(|e| Status::internal(format!("raft shutdown: {e}")));
        if let Some(mut listener) = self.listener.take() {
            if let Some(stop) = listener.stop.take() {
                let _ = stop.send(());
            }
            match tokio::time::timeout(Duration::from_secs(10), &mut listener.task).await {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(e))) => {
                    return Err(Status::internal(format!("raft listener: {e}")));
                }
                Ok(Err(e)) => return Err(Status::internal(format!("raft listener task: {e}"))),
                Err(_) => {
                    listener.task.abort();
                    return Err(Status::internal(
                        "raft listener did not stop within 10 s; aborted",
                    ));
                }
            }
        }
        stopped
    }
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

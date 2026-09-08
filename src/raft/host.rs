//! The in-process Raft host: owns the store, the log store and the Raft
//! instance; admits proposals the way the direct paths did and turns the
//! committed reply back into the caller's decision.
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
use crate::source_authority::SourceAdmission;
use crate::source_authority::{SourceAuthorityStore, VerifiedOwnerCompletion};
use openraft::error::{ClientWriteError, RaftError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, Config, Raft, RaftMetrics, SnapshotPolicy};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::watch;
use tonic::Status;

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
        Ok(())
    }
}

pub struct RaftHost {
    raft: Raft<ControlRaft>,
    store: SharedStore,
    log_store: RaftLogStore,
    node_id: NodeId,
    dir: PathBuf,
    lease: std::time::Duration,
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
        let host = Self::start_with(dir, store, log_store, node_id, config).await?;
        let mut members = BTreeMap::new();
        members.insert(
            node_id,
            BasicNode {
                addr: addr.to_string(),
            },
        );
        host.raft
            .initialize(members)
            .await
            .map_err(|e| Status::internal(format!("raft initialize: {e}")))?;
        host.raft
            .wait(Some(std::time::Duration::from_secs(30)))
            .state(openraft::ServerState::Leader, "bootstrap leader")
            .await
            .map_err(|e| Status::internal(format!("raft bootstrap: {e}")))?;
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
        Self::start_with(dir, store, log_store, node_id, config).await
    }

    async fn start_with(
        dir: &Path,
        store: SourceAuthorityStore,
        log_store: RaftLogStore,
        node_id: NodeId,
        config: &HostConfig,
    ) -> Result<Self, Status> {
        let machine =
            ControlStateMachine::new(store, &dir.join(SNAPSHOT_DIR), config.max_snapshot_bytes)?;
        let shared = machine.shared_store();
        let raft = Raft::new(
            node_id,
            Self::raft_config(config)?,
            NoNetwork,
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
            lease: std::time::Duration::from_millis(config.admission_lease_ms),
        })
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub fn dir(&self) -> &Path {
        &self.dir
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
        timeout: Option<std::time::Duration>,
    ) -> openraft::metrics::Wait<NodeId, BasicNode, openraft::TokioRuntime> {
        self.raft.wait(timeout)
    }

    /// The applied position the store recorded, as a read.
    pub fn applied_position(&self) -> Result<Option<crate::pb::storage::RaftLogId>, Status> {
        Ok(self.store()?.raft_applied()?.and_then(|a| a.last_applied))
    }

    /// Run owner-side work under a leased admission: a linearizable read
    /// proves this node is the leader with a quorum now, and the lease is
    /// the bound under which that stays true. Past it the admission admits
    /// nothing, so an isolated former leader cannot admit work a surviving
    /// quorum has revoked (docs/raft-admission.md).
    pub async fn with_admission<T>(
        &self,
        principal: &str,
        run: impl FnOnce(&SourceAdmission<'_>) -> Result<T, Status>,
    ) -> Result<T, Status> {
        self.raft.ensure_linearizable().await.map_err(|e| {
            Status::unavailable(format!("admission needs a linearizable read: {e}"))
        })?;
        let granted = std::time::Instant::now();
        let store = self.store()?;
        let admission = store.leased_admission(principal, granted + self.lease)?;
        run(&admission)
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

    /// Submit an admitted proposal and wait for its committed reply. Private:
    /// every public entry admits its command first.
    async fn propose(&self, proposal: RaftProposal) -> Result<RaftReply, Status> {
        validate_proposal(&proposal)?;
        let response = self
            .raft
            .client_write(proposal)
            .await
            .map_err(|error| match error {
                RaftError::APIError(ClientWriteError::ForwardToLeader(forward)) => {
                    Status::unavailable(format!(
                        "not the leader; leader is {:?}",
                        forward.leader_id
                    ))
                }
                other => Status::unavailable(format!("raft proposal: {other}")),
            })?;
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

    /// Ask the state machine for a snapshot now (tests and operators).
    pub async fn trigger_snapshot(&self) -> Result<(), Status> {
        self.raft
            .trigger()
            .snapshot()
            .await
            .map_err(|e| Status::internal(format!("raft snapshot trigger: {e}")))
    }

    pub async fn shutdown(self) -> Result<(), Status> {
        self.raft
            .shutdown()
            .await
            .map_err(|e| Status::internal(format!("raft shutdown: {e}")))
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

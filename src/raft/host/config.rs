//! The host's timing and limits, and the network of a member that has none.

use super::*;

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

pub(super) fn unreachable<E: std::error::Error>() -> openraft::error::RPCError<NodeId, BasicNode, E>
{
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
    pub(super) fn read_timeout(&self) -> Duration {
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

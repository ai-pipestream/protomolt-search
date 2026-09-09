//! The tonic Raft transport (docs/raft-hosting.md): the library's vote,
//! append and snapshot operations as typed RPCs (`raft_transport.proto`)
//! over the cluster's mTLS channels.
//!
//! Identity is bound on both sides. A listener authenticates the peer by
//! the client certificate the cluster CA issued and looks that certificate
//! up in the `PeerDirectory`: the request's `from_node_id` must be the node
//! the certificate is registered to, its group must be this group and its
//! `to_node_id` must be this node. A caller names the target in every
//! request and checks the responder named in every reply. An address is
//! where a node is dialed, never who it is.
use super::state_machine::SnapshotStaging;
use super::types::{
    entry_from_proto, entry_to_proto, log_id_from_proto, log_id_to_proto,
    stored_membership_from_proto, stored_membership_to_proto, vote_from_proto, vote_to_proto,
    ControlRaft, NodeId,
};
use crate::pb::storage::{
    raft_append_entries_response::Outcome as AppendOutcome,
    raft_install_snapshot_response::Outcome as InstallOutcome,
    raft_transport_client::RaftTransportClient,
    raft_transport_server::{RaftTransport, RaftTransportServer},
    RaftAppendEntriesRequest, RaftAppendEntriesResponse, RaftInstallSnapshotRequest,
    RaftInstallSnapshotResponse, RaftPartialSuccess, RaftRpcHeader, RaftSnapshotMeta,
    RaftSnapshotMismatch, RaftVoteRequest, RaftVoteResponse, SourceAuthorityIdentity,
};
use crate::security::{apply_client_tls, secure_url, ClientTls};
use openraft::error::{
    InstallSnapshotError, NetworkError, PayloadTooLarge, RPCError, RaftError, RemoteError,
    SnapshotMismatch, Timeout, Unreachable,
};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::storage::{Snapshot, SnapshotMeta};
use openraft::{BasicNode, RPCTypes, Raft, SnapshotSegmentId};
use prost::Message;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status};

pub const PROTOCOL_VERSION: u32 = 1;

/// Which certificate speaks for which node of one group. Registered before
/// a node is dialed or admitted; a certificate is bound to one node and a
/// node to one certificate.
pub struct PeerDirectory {
    group: SourceAuthorityIdentity,
    peers: RwLock<BTreeMap<NodeId, [u8; 32]>>,
}

impl PeerDirectory {
    pub fn new(group: &SourceAuthorityIdentity) -> Self {
        Self {
            group: group.clone(),
            peers: RwLock::new(BTreeMap::new()),
        }
    }

    pub fn group(&self) -> &SourceAuthorityIdentity {
        &self.group
    }

    /// Bind a node id to the SHA-256 of its certificate's DER. A second
    /// certificate for a known node, or a certificate already bound to
    /// another node, refuses.
    pub fn register(&self, node_id: NodeId, certificate_sha256: [u8; 32]) -> Result<(), Status> {
        let mut peers = self
            .peers
            .write()
            .map_err(|_| Status::internal("peer directory lock poisoned"))?;
        if let Some((other, _)) = peers
            .iter()
            .find(|(id, fingerprint)| **fingerprint == certificate_sha256 && **id != node_id)
        {
            return Err(Status::already_exists(format!(
                "certificate is already registered to node {other}"
            )));
        }
        match peers.get(&node_id) {
            Some(existing) if *existing != certificate_sha256 => Err(Status::already_exists(
                format!("node {node_id} is already registered with another certificate"),
            )),
            _ => {
                peers.insert(node_id, certificate_sha256);
                Ok(())
            }
        }
    }

    pub fn knows(&self, node_id: NodeId) -> bool {
        self.peers
            .read()
            .map(|peers| peers.contains_key(&node_id))
            .unwrap_or(false)
    }

    fn node_of(&self, certificate_sha256: &[u8; 32]) -> Option<NodeId> {
        self.peers.read().ok().and_then(|peers| {
            peers
                .iter()
                .find(|(_, fingerprint)| *fingerprint == certificate_sha256)
                .map(|(id, _)| *id)
        })
    }
}

/// The SHA-256 of the first certificate in a PEM file: the value the
/// directory binds a node to. The file must hold exactly one certificate.
pub fn certificate_sha256(pem: &[u8]) -> Result<[u8; 32], Status> {
    use rustls_pki_types::pem::PemObject;
    let mut certificates = rustls_pki_types::CertificateDer::pem_slice_iter(pem);
    let first = certificates
        .next()
        .ok_or_else(|| Status::invalid_argument("PEM holds no certificate"))?
        .map_err(|e| Status::invalid_argument(format!("PEM certificate: {e}")))?;
    if certificates.next().is_some() {
        return Err(Status::invalid_argument(
            "PEM holds more than one certificate; a member identity is one leaf",
        ));
    }
    Ok(crate::sha256::digest(first.as_ref()))
}

/// Bounds on one RPC and one dial.
#[derive(Clone, Debug)]
pub struct TransportLimits {
    /// Largest encoded request or reply either side accepts.
    pub max_message_bytes: usize,
    pub connect_timeout_ms: u64,
    /// A snapshot transfer with no chunk for this long is dropped with its
    /// bytes; the next transfer may then take its place.
    pub snapshot_idle_timeout_ms: u64,
}

impl Default for TransportLimits {
    fn default() -> Self {
        Self {
            max_message_bytes: 16 << 20,
            connect_timeout_ms: 2_000,
            snapshot_idle_timeout_ms: 30_000,
        }
    }
}

impl TransportLimits {
    /// The bound must hold one snapshot chunk with its envelope.
    pub fn validate(&self, snapshot_chunk_bytes: u64) -> Result<(), Status> {
        if self.max_message_bytes < 64 << 10 {
            return Err(Status::invalid_argument(
                "max_message_bytes must be at least 64 KiB",
            ));
        }
        if (self.max_message_bytes as u64) < snapshot_chunk_bytes.saturating_add(64 << 10) {
            return Err(Status::invalid_argument(format!(
                "max_message_bytes {} cannot carry a {} byte snapshot chunk with its envelope",
                self.max_message_bytes, snapshot_chunk_bytes
            )));
        }
        if self.connect_timeout_ms == 0 {
            return Err(Status::invalid_argument(
                "connect_timeout_ms must be positive",
            ));
        }
        if self.snapshot_idle_timeout_ms == 0 {
            return Err(Status::invalid_argument(
                "snapshot_idle_timeout_ms must be positive",
            ));
        }
        Ok(())
    }
}

pub use super::host::LeaseHold;

/// The metadata key that carries the timing agreement on every RPC.
pub const TIMING_METADATA: &str = "protomolt-raft-timing";

/// A fault-injection switch: peers this node refuses to speak to or hear
/// from. Empty outside tests and the `fault-injection` feature.
#[derive(Clone, Default)]
pub struct Isolation {
    blocked: Arc<RwLock<BTreeSet<NodeId>>>,
}

impl Isolation {
    fn blocks(&self, peer: NodeId) -> bool {
        self.blocked
            .read()
            .map(|blocked| blocked.contains(&peer))
            .unwrap_or(true)
    }

    /// Refuse every RPC to and from these peers until `heal`.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn isolate(&self, peers: impl IntoIterator<Item = NodeId>) {
        self.blocked.write().unwrap().extend(peers);
    }

    #[cfg(any(test, feature = "fault-injection"))]
    pub fn heal(&self) {
        self.blocked.write().unwrap().clear();
    }
}

// ---------------------------------------------------------------------
// Listener side
// ---------------------------------------------------------------------

/// The RPC service in front of one node's Raft instance.
pub struct RaftTransportService {
    raft: Raft<ControlRaft>,
    node_id: NodeId,
    directory: Arc<PeerDirectory>,
    isolation: Isolation,
    timing: String,
    hold: LeaseHold,
    staging: Arc<SnapshotStaging>,
    transfer: Mutex<Option<Transfer>>,
    idle: Duration,
}

/// One snapshot transfer in progress: bound to the authenticated peer that
/// began it, the vote it came under, the snapshot id and meta, and the
/// receive directory holding its bytes. Chunks are appended in order to
/// the file; nothing is buffered beyond one chunk.
struct Transfer {
    peer: NodeId,
    vote: openraft::Vote<NodeId>,
    meta: SnapshotMeta<NodeId, BasicNode>,
    announced: u64,
    dir: PathBuf,
    file: std::fs::File,
    received: u64,
    last_chunk: std::time::Instant,
}

/// What staging one chunk led to.
enum Staged {
    /// The chunk was appended; more to come.
    Partial,
    /// The chunk is out of order: the sender restarts from `expect_offset`.
    Mismatch { expect_offset: u64 },
    /// The transfer is complete and the image validated; hand it to the
    /// library.
    Complete {
        meta: SnapshotMeta<NodeId, BasicNode>,
        vote: openraft::Vote<NodeId>,
        dir: PathBuf,
    },
}

impl RaftTransportService {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        raft: Raft<ControlRaft>,
        node_id: NodeId,
        directory: Arc<PeerDirectory>,
        isolation: Isolation,
        timing: String,
        hold: LeaseHold,
        staging: Arc<SnapshotStaging>,
        limits: &TransportLimits,
    ) -> Self {
        Self {
            raft,
            node_id,
            directory,
            isolation,
            timing,
            hold,
            staging,
            transfer: Mutex::new(None),
            idle: Duration::from_millis(limits.snapshot_idle_timeout_ms),
        }
    }

    /// Stage one chunk of a snapshot transfer from `from`. The transfer is
    /// bound to the peer, the vote, the snapshot id and the meta of its
    /// first chunk; bytes go straight to the receive file; the announced
    /// length bounds the disk it may take and the image bound caps that.
    /// One transfer at a time: a chunk of another id from the same peer or
    /// under a higher vote supersedes the transfer in progress, an idle
    /// transfer is dropped, and anything else waits. On the final chunk
    /// the complete image is validated (length, digest, store identity,
    /// applied position, membership on a probe copy); an invalid image is
    /// refused here, by name, and never reaches the library.
    fn stage(
        &self,
        from: NodeId,
        rpc: InstallSnapshotRequest<ControlRaft>,
    ) -> Result<Staged, Status> {
        let announced = super::state_machine::parse_snapshot_id(&rpc.meta.snapshot_id)
            .map_err(wire)?
            .length;
        if announced > self.staging.max_image_bytes() {
            return Err(Status::resource_exhausted(format!(
                "snapshot announces {announced} bytes, beyond the {} byte image bound",
                self.staging.max_image_bytes()
            )));
        }
        let now = std::time::Instant::now();
        let mut guard = self
            .transfer
            .lock()
            .map_err(|_| Status::internal("snapshot transfer lock poisoned"))?;
        if guard
            .as_ref()
            .is_some_and(|active| now.duration_since(active.last_chunk) > self.idle)
        {
            let stale = guard.take().expect("checked");
            self.staging.discard(&stale.dir);
        }
        let same_id = guard
            .as_ref()
            .is_some_and(|active| active.meta.snapshot_id == rpc.meta.snapshot_id);
        if !same_id {
            if let Some(active) = guard.as_ref() {
                let supersedes = active.peer == from || rpc.vote > active.vote;
                if !supersedes {
                    return Err(Status::unavailable(format!(
                        "a snapshot transfer from node {} is in progress",
                        active.peer
                    )));
                }
                let superseded = guard.take().expect("checked");
                self.staging.discard(&superseded.dir);
            }
            if rpc.offset != 0 {
                return Ok(Staged::Mismatch { expect_offset: 0 });
            }
            let (dir, file) = self.staging.begin()?;
            *guard = Some(Transfer {
                peer: from,
                vote: rpc.vote,
                meta: rpc.meta.clone(),
                announced,
                dir,
                file,
                received: 0,
                last_chunk: now,
            });
        }
        let active = guard.as_mut().expect("a transfer is in progress");
        if active.peer != from {
            return Err(Status::failed_precondition(format!(
                "snapshot transfer {} is bound to node {}",
                active.meta.snapshot_id, active.peer
            )));
        }
        if active.meta != rpc.meta || active.vote != rpc.vote {
            let broken = guard.take().expect("checked");
            self.staging.discard(&broken.dir);
            return Err(Status::invalid_argument(
                "snapshot meta or vote changed within one transfer; the transfer is dropped",
            ));
        }
        if rpc.offset == 0 && active.received != 0 {
            // The sender starts this transfer over (the library's own
            // response to a mismatch): the bytes so far are discarded.
            use std::io::Seek;
            if let Err(error) = active
                .file
                .set_len(0)
                .and_then(|()| active.file.seek(std::io::SeekFrom::Start(0)))
            {
                let broken = guard.take().expect("checked");
                self.staging.discard(&broken.dir);
                return Err(Status::internal(format!("snapshot receive: {error}")));
            }
            active.received = 0;
        } else if rpc.offset != active.received {
            return Ok(Staged::Mismatch {
                expect_offset: active.received,
            });
        }
        if rpc.offset.saturating_add(rpc.data.len() as u64) > active.announced {
            let announced = active.announced;
            let broken = guard.take().expect("checked");
            self.staging.discard(&broken.dir);
            return Err(Status::invalid_argument(format!(
                "snapshot chunk ends past the announced length {announced}; the transfer is dropped"
            )));
        }
        {
            use std::io::Write;
            if let Err(error) = active.file.write_all(&rpc.data) {
                let broken = guard.take().expect("checked");
                self.staging.discard(&broken.dir);
                return Err(Status::internal(format!("snapshot receive: {error}")));
            }
        }
        active.received += rpc.data.len() as u64;
        active.last_chunk = now;
        if !rpc.done {
            return Ok(Staged::Partial);
        }
        let complete = guard.take().expect("checked");
        drop(guard);
        if complete.received != complete.announced {
            self.staging.discard(&complete.dir);
            return Err(Status::invalid_argument(format!(
                "snapshot transfer ended at {} bytes of the announced length {}",
                complete.received, complete.announced
            )));
        }
        if let Err(error) = complete.file.sync_all() {
            self.staging.discard(&complete.dir);
            return Err(Status::internal(format!("snapshot receive: {error}")));
        }
        drop(complete.file);
        if let Err(error) = self.staging.verify(&complete.dir, &complete.meta) {
            self.staging.discard(&complete.dir);
            return Err(error);
        }
        Ok(Staged::Complete {
            meta: complete.meta,
            vote: complete.vote,
            dir: complete.dir,
        })
    }

    pub(crate) fn into_server(self, limits: &TransportLimits) -> RaftTransportServer<Self> {
        RaftTransportServer::new(self)
            .max_decoding_message_size(limits.max_message_bytes)
            .max_encoding_message_size(limits.max_message_bytes)
    }

    /// The peer behind a request: the verified client certificate names a
    /// registered node, and the header agrees with it, this group and this
    /// node. Returns the sender's id.
    fn authenticate<T>(
        &self,
        request: &Request<T>,
        header: Option<&RaftRpcHeader>,
    ) -> Result<NodeId, Status> {
        let certificates = request.peer_certs().ok_or_else(|| {
            Status::unauthenticated(
                "raft transport requires a client certificate from the cluster CA",
            )
        })?;
        let leaf = certificates.first().ok_or_else(|| {
            Status::unauthenticated(
                "raft transport requires a client certificate from the cluster CA",
            )
        })?;
        let fingerprint = crate::sha256::digest(leaf.as_ref());
        let registered = self.directory.node_of(&fingerprint).ok_or_else(|| {
            Status::unauthenticated(
                "client certificate is not registered to any member of this group",
            )
        })?;
        let header =
            header.ok_or_else(|| Status::invalid_argument("raft rpc carries no header"))?;
        if header.protocol_version != PROTOCOL_VERSION {
            return Err(Status::invalid_argument(format!(
                "raft rpc protocol version {} is not {PROTOCOL_VERSION}",
                header.protocol_version
            )));
        }
        if header.group.as_ref() != Some(self.directory.group()) {
            return Err(Status::permission_denied("raft rpc names another group"));
        }
        if header.to_node_id != self.node_id {
            return Err(Status::permission_denied(format!(
                "raft rpc addressed to node {} reached node {}",
                header.to_node_id, self.node_id
            )));
        }
        if header.from_node_id != registered {
            return Err(Status::permission_denied(format!(
                "raft rpc claims node {} but the certificate is registered to node {registered}",
                header.from_node_id
            )));
        }
        if self.isolation.blocks(registered) {
            return Err(Status::unavailable(format!(
                "node {registered} is isolated from this node (fault injection)"
            )));
        }
        let timing = request.metadata().get(TIMING_METADATA).ok_or_else(|| {
            Status::failed_precondition(
                "raft rpc carries no timing agreement; the peer runs another build",
            )
        })?;
        if timing.as_bytes() != self.timing.as_bytes() {
            return Err(Status::failed_precondition(format!(
                "peer timing configuration {:?} differs from this node's {}; election and lease values must agree across the group",
                timing, self.timing
            )));
        }
        Ok(registered)
    }

    fn header(&self, to: NodeId) -> RaftRpcHeader {
        RaftRpcHeader {
            protocol_version: PROTOCOL_VERSION,
            group: Some(self.directory.group().clone()),
            from_node_id: self.node_id,
            to_node_id: to,
        }
    }
}

fn core_stopped<E: std::error::Error>(error: RaftError<NodeId, E>) -> Status {
    Status::unavailable(format!("raft core: {error}"))
}

#[tonic::async_trait]
impl RaftTransport for RaftTransportService {
    async fn append_entries(
        &self,
        request: Request<RaftAppendEntriesRequest>,
    ) -> Result<Response<RaftAppendEntriesResponse>, Status> {
        let from = self.authenticate(&request, request.get_ref().header.as_ref())?;
        let rpc = append_request_from_proto(request.into_inner())?;
        let response = match self.raft.append_entries(rpc).await {
            Ok(response) => response,
            Err(error) => return Err(core_stopped(error)),
        };
        Ok(Response::new(append_response_to_proto(
            self.header(from),
            response,
        )))
    }

    async fn vote(
        &self,
        request: Request<RaftVoteRequest>,
    ) -> Result<Response<RaftVoteResponse>, Status> {
        let from = self.authenticate(&request, request.get_ref().header.as_ref())?;
        let rpc = vote_request_from_proto(request.into_inner())?;
        // A leader with an admission interval possibly open withholds its
        // vote; the library would grant it (see `LeaseHold`).
        let held = {
            let metrics = self.raft.metrics().borrow().clone();
            if metrics.state == openraft::ServerState::Leader
                && self.hold.holds(std::time::Instant::now())
            {
                Some(metrics.vote)
            } else {
                None
            }
        };
        let response = match held {
            Some(vote) => VoteResponse {
                vote,
                vote_granted: false,
                last_log_id: None,
            },
            None => match self.raft.vote(rpc).await {
                Ok(response) => response,
                Err(error) => return Err(core_stopped(error)),
            },
        };
        Ok(Response::new(vote_response_to_proto(
            self.header(from),
            response,
        )))
    }

    async fn install_snapshot(
        &self,
        request: Request<RaftInstallSnapshotRequest>,
    ) -> Result<Response<RaftInstallSnapshotResponse>, Status> {
        let from = self.authenticate(&request, request.get_ref().header.as_ref())?;
        let rpc = install_request_from_proto(self.directory.group(), request.into_inner())?;
        let snapshot_id = rpc.meta.snapshot_id.clone();
        let offset = rpc.offset;
        let outcome = match self.stage(from, rpc)? {
            Staged::Partial => {
                InstallOutcome::Vote(vote_to_proto(&self.raft.metrics().borrow().vote))
            }
            Staged::Mismatch { expect_offset } => InstallOutcome::Mismatch(RaftSnapshotMismatch {
                expect_id: snapshot_id.clone(),
                expect_offset,
                got_id: snapshot_id,
                got_offset: offset,
            }),
            Staged::Complete { meta, vote, dir } => {
                // The image is validated; the library applies its own term
                // and position rules and may decline it (an older vote, a
                // position it already holds). Only a storage failure in the
                // swap is fatal, as before.
                let file = match tokio::fs::File::open(SnapshotStaging::image_path(&dir)).await {
                    Ok(file) => file,
                    Err(error) => {
                        self.staging.discard(&dir);
                        return Err(Status::internal(format!("snapshot receive: {error}")));
                    }
                };
                let response = self
                    .raft
                    .install_full_snapshot(
                        vote,
                        Snapshot {
                            meta,
                            snapshot: Box::new(file),
                        },
                    )
                    .await
                    .map_err(|e| Status::unavailable(format!("raft core: {e}")))?;
                if self.staging.is_current(&dir) {
                    // Declined by the library: the validated bytes are of no
                    // further use.
                    self.staging.discard(&dir);
                }
                InstallOutcome::Vote(vote_to_proto(&response.vote))
            }
        };
        Ok(Response::new(RaftInstallSnapshotResponse {
            header: Some(self.header(from)),
            outcome: Some(outcome),
        }))
    }
}

// ---------------------------------------------------------------------
// Envelope mappings
// ---------------------------------------------------------------------

fn wire(error: Status) -> Status {
    Status::invalid_argument(error.message().to_string())
}

fn required_vote(
    vote: Option<&crate::pb::storage::RaftVote>,
) -> Result<openraft::Vote<NodeId>, Status> {
    vote.map(vote_from_proto)
        .ok_or_else(|| Status::invalid_argument("raft rpc carries no vote"))
}

/// Entries and snapshots come from a leader, and a leader's vote is
/// committed by definition; the library asserts as much when it accepts
/// them, so a request under an uncommitted vote is refused before it.
fn leader_vote(
    vote: Option<&crate::pb::storage::RaftVote>,
) -> Result<openraft::Vote<NodeId>, Status> {
    let vote = required_vote(vote)?;
    if !vote.is_committed() {
        return Err(Status::invalid_argument(
            "entries and snapshots come from a leader; this vote is not committed",
        ));
    }
    Ok(vote)
}

fn append_request_from_proto(
    value: RaftAppendEntriesRequest,
) -> Result<AppendEntriesRequest<ControlRaft>, Status> {
    let vote = leader_vote(value.vote.as_ref())?;
    let entries = value
        .entries
        .into_iter()
        .map(|entry| entry_from_proto(entry).map_err(wire))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(AppendEntriesRequest {
        vote,
        prev_log_id: value.prev_log_id.as_ref().map(log_id_from_proto),
        entries,
        leader_commit: value.leader_commit.as_ref().map(log_id_from_proto),
    })
}

fn append_request_to_proto(
    header: RaftRpcHeader,
    rpc: &AppendEntriesRequest<ControlRaft>,
) -> RaftAppendEntriesRequest {
    RaftAppendEntriesRequest {
        header: Some(header),
        vote: Some(vote_to_proto(&rpc.vote)),
        prev_log_id: rpc.prev_log_id.as_ref().map(log_id_to_proto),
        entries: rpc.entries.iter().map(entry_to_proto).collect(),
        leader_commit: rpc.leader_commit.as_ref().map(log_id_to_proto),
    }
}

fn append_response_to_proto(
    header: RaftRpcHeader,
    response: AppendEntriesResponse<NodeId>,
) -> RaftAppendEntriesResponse {
    let outcome = match response {
        AppendEntriesResponse::Success => AppendOutcome::Success(true),
        AppendEntriesResponse::PartialSuccess(matching) => {
            AppendOutcome::PartialSuccess(RaftPartialSuccess {
                matching: matching.as_ref().map(log_id_to_proto),
            })
        }
        AppendEntriesResponse::Conflict => AppendOutcome::Conflict(true),
        AppendEntriesResponse::HigherVote(vote) => AppendOutcome::HigherVote(vote_to_proto(&vote)),
    };
    RaftAppendEntriesResponse {
        header: Some(header),
        outcome: Some(outcome),
    }
}

fn append_response_from_proto(
    value: RaftAppendEntriesResponse,
) -> Result<AppendEntriesResponse<NodeId>, Status> {
    match value.outcome {
        Some(AppendOutcome::Success(true)) => Ok(AppendEntriesResponse::Success),
        Some(AppendOutcome::PartialSuccess(partial)) => Ok(AppendEntriesResponse::PartialSuccess(
            partial.matching.as_ref().map(log_id_from_proto),
        )),
        Some(AppendOutcome::Conflict(true)) => Ok(AppendEntriesResponse::Conflict),
        Some(AppendOutcome::HigherVote(vote)) => {
            Ok(AppendEntriesResponse::HigherVote(vote_from_proto(&vote)))
        }
        _ => Err(Status::invalid_argument(
            "append entries reply carries no recognized outcome",
        )),
    }
}

fn vote_request_from_proto(value: RaftVoteRequest) -> Result<VoteRequest<NodeId>, Status> {
    Ok(VoteRequest {
        vote: required_vote(value.vote.as_ref())?,
        last_log_id: value.last_log_id.as_ref().map(log_id_from_proto),
    })
}

fn vote_request_to_proto(header: RaftRpcHeader, rpc: &VoteRequest<NodeId>) -> RaftVoteRequest {
    RaftVoteRequest {
        header: Some(header),
        vote: Some(vote_to_proto(&rpc.vote)),
        last_log_id: rpc.last_log_id.as_ref().map(log_id_to_proto),
    }
}

fn vote_response_to_proto(
    header: RaftRpcHeader,
    response: VoteResponse<NodeId>,
) -> RaftVoteResponse {
    RaftVoteResponse {
        header: Some(header),
        vote: Some(vote_to_proto(&response.vote)),
        vote_granted: response.vote_granted,
        last_log_id: response.last_log_id.as_ref().map(log_id_to_proto),
    }
}

fn vote_response_from_proto(value: RaftVoteResponse) -> Result<VoteResponse<NodeId>, Status> {
    Ok(VoteResponse {
        vote: required_vote(value.vote.as_ref())?,
        vote_granted: value.vote_granted,
        last_log_id: value.last_log_id.as_ref().map(log_id_from_proto),
    })
}

/// The snapshot meta on the wire carries the group and the checksum the id
/// names, so a receiver checks both before the library sees the chunk.
fn snapshot_meta_to_proto(
    group: &SourceAuthorityIdentity,
    meta: &SnapshotMeta<NodeId, BasicNode>,
) -> Result<RaftSnapshotMeta, Status> {
    let signature = super::state_machine::parse_snapshot_id(&meta.snapshot_id)?;
    Ok(RaftSnapshotMeta {
        format_version: 1,
        group: Some(group.clone()),
        last_log_id: meta.last_log_id.as_ref().map(log_id_to_proto),
        membership: Some(stored_membership_to_proto(&meta.last_membership)),
        snapshot_id: meta.snapshot_id.clone(),
        length: signature.length,
        sha256: signature.sha256.to_vec(),
    })
}

fn snapshot_meta_from_proto(
    group: &SourceAuthorityIdentity,
    value: &RaftSnapshotMeta,
) -> Result<SnapshotMeta<NodeId, BasicNode>, Status> {
    if value.format_version != 1 {
        return Err(Status::invalid_argument("snapshot meta requires format 1"));
    }
    if value.group.as_ref() != Some(group) {
        return Err(Status::permission_denied(
            "snapshot meta names another group",
        ));
    }
    let signature = super::state_machine::parse_snapshot_id(&value.snapshot_id).map_err(wire)?;
    if signature.length != value.length || signature.sha256[..] != value.sha256[..] {
        return Err(Status::invalid_argument(
            "snapshot meta length or checksum differs from its id",
        ));
    }
    let membership = value
        .membership
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("snapshot meta carries no membership"))?;
    Ok(SnapshotMeta {
        last_log_id: value.last_log_id.as_ref().map(log_id_from_proto),
        last_membership: stored_membership_from_proto(membership).map_err(wire)?,
        snapshot_id: value.snapshot_id.clone(),
    })
}

fn install_request_from_proto(
    group: &SourceAuthorityIdentity,
    value: RaftInstallSnapshotRequest,
) -> Result<InstallSnapshotRequest<ControlRaft>, Status> {
    let meta = value
        .meta
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("snapshot chunk carries no meta"))?;
    let meta = snapshot_meta_from_proto(group, meta)?;
    // The announced length bounds the receive while it happens, not only
    // at install: a chunk past it is refused before the library sees it.
    let announced = super::state_machine::parse_snapshot_id(&meta.snapshot_id)
        .map_err(wire)?
        .length;
    let end = value.offset.saturating_add(value.data.len() as u64);
    if end > announced {
        return Err(Status::invalid_argument(format!(
            "snapshot chunk ends at byte {end}, beyond the announced length {announced}"
        )));
    }
    Ok(InstallSnapshotRequest {
        vote: leader_vote(value.vote.as_ref())?,
        meta,
        offset: value.offset,
        data: value.data,
        done: value.done,
    })
}

fn install_request_to_proto(
    header: RaftRpcHeader,
    group: &SourceAuthorityIdentity,
    rpc: InstallSnapshotRequest<ControlRaft>,
) -> Result<RaftInstallSnapshotRequest, Status> {
    Ok(RaftInstallSnapshotRequest {
        header: Some(header),
        vote: Some(vote_to_proto(&rpc.vote)),
        meta: Some(snapshot_meta_to_proto(group, &rpc.meta)?),
        offset: rpc.offset,
        data: rpc.data,
        done: rpc.done,
    })
}

// ---------------------------------------------------------------------
// Caller side
// ---------------------------------------------------------------------

struct Shared {
    node_id: NodeId,
    directory: Arc<PeerDirectory>,
    client_tls: ClientTls,
    limits: TransportLimits,
    isolation: Isolation,
    timing: tonic::metadata::MetadataValue<tonic::metadata::Ascii>,
}

/// Builds one `TonicNetwork` per target for the library.
#[derive(Clone)]
pub struct TonicNetworkFactory {
    shared: Arc<Shared>,
}

impl TonicNetworkFactory {
    pub(crate) fn new(
        node_id: NodeId,
        directory: Arc<PeerDirectory>,
        client_tls: ClientTls,
        limits: TransportLimits,
        isolation: Isolation,
        timing: String,
    ) -> Self {
        Self {
            shared: Arc::new(Shared {
                node_id,
                directory,
                client_tls,
                limits,
                isolation,
                timing: timing
                    .parse()
                    .expect("timing agreement is ASCII digits and punctuation"),
            }),
        }
    }
}

impl RaftNetworkFactory<ControlRaft> for TonicNetworkFactory {
    type Network = TonicNetwork;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        TonicNetwork {
            shared: Arc::clone(&self.shared),
            target,
            addr: node.addr.clone(),
            client: None,
        }
    }
}

/// One caller-side channel to one peer, dialed lazily and redialed by the
/// channel after a failure.
pub struct TonicNetwork {
    shared: Arc<Shared>,
    target: NodeId,
    addr: String,
    client: Option<RaftTransportClient<Channel>>,
}

type Rpc<E> = RPCError<NodeId, BasicNode, E>;

impl TonicNetwork {
    fn header(&self) -> RaftRpcHeader {
        RaftRpcHeader {
            protocol_version: PROTOCOL_VERSION,
            group: Some(self.shared.directory.group().clone()),
            from_node_id: self.shared.node_id,
            to_node_id: self.target,
        }
    }

    fn unreachable<E: std::error::Error>(message: String) -> Rpc<E> {
        RPCError::Unreachable(Unreachable::new(&std::io::Error::other(message)))
    }

    fn network<E: std::error::Error>(message: String) -> Rpc<E> {
        RPCError::Network(NetworkError::new(&std::io::Error::other(message)))
    }

    /// The target must be a registered member and not isolated, and the
    /// channel is built once from the cluster client TLS material.
    fn client<E: std::error::Error>(
        &mut self,
    ) -> Result<&mut RaftTransportClient<Channel>, Rpc<E>> {
        if !self.shared.directory.knows(self.target) {
            return Err(Self::unreachable(format!(
                "node {} is not registered in the peer directory",
                self.target
            )));
        }
        if self.shared.isolation.blocks(self.target) {
            return Err(Self::unreachable(format!(
                "node {} is isolated from this node (fault injection)",
                self.target
            )));
        }
        if self.client.is_none() {
            let url = secure_url(&self.addr, Some(&self.shared.client_tls));
            let endpoint = Endpoint::from_shared(url)
                .map_err(|e| Self::unreachable::<E>(format!("raft peer address: {e}")))?
                .connect_timeout(Duration::from_millis(self.shared.limits.connect_timeout_ms))
                .tcp_nodelay(true);
            let endpoint = apply_client_tls(endpoint, Some(&self.shared.client_tls))
                .map_err(Self::unreachable::<E>)?;
            let client = RaftTransportClient::new(endpoint.connect_lazy())
                .max_decoding_message_size(self.shared.limits.max_message_bytes)
                .max_encoding_message_size(self.shared.limits.max_message_bytes);
            self.client = Some(client);
        }
        Ok(self.client.as_mut().expect("client was just built"))
    }

    fn request<T>(&self, message: T, option: &RPCOption) -> Request<T> {
        let mut request = Request::new(message);
        request.set_timeout(option.hard_ttl());
        request
            .metadata_mut()
            .insert(TIMING_METADATA, self.shared.timing.clone());
        request
    }

    /// A reply must come from the node that was dialed, for this group.
    fn check_reply(&self, header: Option<&RaftRpcHeader>) -> Result<(), Status> {
        let header =
            header.ok_or_else(|| Status::invalid_argument("raft reply carries no header"))?;
        if header.protocol_version != PROTOCOL_VERSION
            || header.group.as_ref() != Some(self.shared.directory.group())
            || header.to_node_id != self.shared.node_id
        {
            return Err(Status::permission_denied(
                "raft reply names another group, node or protocol",
            ));
        }
        if header.from_node_id != self.target {
            return Err(Status::permission_denied(format!(
                "raft reply from node {} answered a request to node {}",
                header.from_node_id, self.target
            )));
        }
        Ok(())
    }

    fn failure<E: std::error::Error>(
        &self,
        action: RPCTypes,
        ttl: Duration,
        status: Status,
    ) -> Rpc<E> {
        match status.code() {
            tonic::Code::Unavailable | tonic::Code::Unknown => Self::unreachable(format!(
                "node {} at {}: {}",
                self.target,
                self.addr,
                status.message()
            )),
            tonic::Code::DeadlineExceeded | tonic::Code::Cancelled => RPCError::Timeout(Timeout {
                action,
                id: self.shared.node_id,
                target: self.target,
                timeout: ttl,
            }),
            _ => Self::network(format!(
                "node {}: {} ({:?})",
                self.target,
                status.message(),
                status.code()
            )),
        }
    }
}

impl RaftNetwork<ControlRaft> for TonicNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<ControlRaft>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, Rpc<RaftError<NodeId>>> {
        let message = append_request_to_proto(self.header(), &rpc);
        let bound = self.shared.limits.max_message_bytes;
        if message.encoded_len() > bound {
            if rpc.entries.len() > 1 {
                return Err(RPCError::PayloadTooLarge(
                    PayloadTooLarge::new_entries_hint((rpc.entries.len() / 2).max(1) as u64),
                ));
            }
            return Err(Self::network(format!(
                "one log entry of {} bytes exceeds the {bound} byte transport bound",
                message.encoded_len()
            )));
        }
        let ttl = option.hard_ttl();
        let request = self.request(message, &option);
        let client = self.client()?;
        let reply = client
            .append_entries(request)
            .await
            .map_err(|status| self.failure(RPCTypes::AppendEntries, ttl, status))?
            .into_inner();
        self.check_reply(reply.header.as_ref())
            .and_then(|()| append_response_from_proto(reply))
            .map_err(|status| Self::network(status.message().to_string()))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<ControlRaft>,
        option: RPCOption,
    ) -> Result<InstallSnapshotResponse<NodeId>, Rpc<RaftError<NodeId, InstallSnapshotError>>> {
        let group = self.shared.directory.group().clone();
        let message = install_request_to_proto(self.header(), &group, rpc)
            .map_err(|status| Self::network(status.message().to_string()))?;
        if message.encoded_len() > self.shared.limits.max_message_bytes {
            return Err(Self::network(format!(
                "snapshot chunk of {} bytes exceeds the {} byte transport bound",
                message.encoded_len(),
                self.shared.limits.max_message_bytes
            )));
        }
        let ttl = option.hard_ttl();
        let request = self.request(message, &option);
        let client = self.client()?;
        let reply = client
            .install_snapshot(request)
            .await
            .map_err(|status| self.failure(RPCTypes::InstallSnapshot, ttl, status))?
            .into_inner();
        self.check_reply(reply.header.as_ref())
            .map_err(|status| Self::network(status.message().to_string()))?;
        match reply.outcome {
            Some(InstallOutcome::Vote(vote)) => Ok(InstallSnapshotResponse {
                vote: vote_from_proto(&vote),
            }),
            Some(InstallOutcome::Mismatch(mismatch)) => {
                Err(RPCError::RemoteError(RemoteError::new_with_node(
                    self.target,
                    BasicNode {
                        addr: self.addr.clone(),
                    },
                    RaftError::APIError(InstallSnapshotError::SnapshotMismatch(SnapshotMismatch {
                        expect: SnapshotSegmentId {
                            id: mismatch.expect_id,
                            offset: mismatch.expect_offset,
                        },
                        got: SnapshotSegmentId {
                            id: mismatch.got_id,
                            offset: mismatch.got_offset,
                        },
                    })),
                )))
            }
            None => Err(Self::network(
                "install snapshot reply carries no outcome".to_string(),
            )),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, Rpc<RaftError<NodeId>>> {
        let message = vote_request_to_proto(self.header(), &rpc);
        let ttl = option.hard_ttl();
        let request = self.request(message, &option);
        let client = self.client()?;
        let reply = client
            .vote(request)
            .await
            .map_err(|status| self.failure(RPCTypes::Vote, ttl, status))?
            .into_inner();
        self.check_reply(reply.header.as_ref())
            .and_then(|()| vote_response_from_proto(reply))
            .map_err(|status| Self::network(status.message().to_string()))
    }
}

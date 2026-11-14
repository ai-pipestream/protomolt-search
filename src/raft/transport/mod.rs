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
use super::reasons;
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
    RaftAppendEntriesRequest, RaftAppendEntriesResponse, RaftGrantLeaseRequest,
    RaftGrantLeaseResponse, RaftInstallSnapshotRequest, RaftInstallSnapshotResponse,
    RaftPartialSuccess, RaftRpcHeader, RaftSnapshotMeta, RaftSnapshotMismatch, RaftVoteRequest,
    RaftVoteResponse, SourceAuthorityIdentity,
};
use crate::security::{apply_client_tls, secure_url, ClientTls};
use openraft::error::{
    CheckIsLeaderError, InstallSnapshotError, NetworkError, PayloadTooLarge, RPCError, RaftError,
    RemoteError, SnapshotMismatch, Timeout, Unreachable,
};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::storage::{Snapshot, SnapshotMeta};
use openraft::{BasicNode, LogId, RPCTypes, Raft, SnapshotSegmentId};
use prost::Message;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status};

mod envelope;
mod network;
mod service;

use envelope::{
    append_request_from_proto, append_request_to_proto, append_response_from_proto,
    append_response_to_proto, install_request_from_proto, install_request_to_proto,
    vote_request_from_proto, vote_request_to_proto, vote_response_from_proto,
    vote_response_to_proto, wire,
};
#[cfg(test)]
use service::answered;
pub(crate) use service::ANSWERED;

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
/// from, and peers it refuses to hear from while still speaking to them.
/// Empty outside tests and the `fault-injection` feature.
#[derive(Clone, Default)]
pub struct Isolation {
    blocked: Arc<RwLock<BTreeSet<NodeId>>>,
    deaf: Arc<RwLock<BTreeSet<NodeId>>>,
}

impl Isolation {
    fn blocks(&self, peer: NodeId) -> bool {
        self.blocked
            .read()
            .map(|blocked| blocked.contains(&peer))
            .unwrap_or(true)
    }

    /// Whether this node refuses what `peer` sends it, at the listener.
    fn deaf_to(&self, peer: NodeId) -> bool {
        self.deaf
            .read()
            .map(|deaf| deaf.contains(&peer))
            .unwrap_or(true)
    }

    /// Refuse every RPC to and from these peers until `heal`.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn isolate(&self, peers: impl IntoIterator<Item = NodeId>) {
        self.blocked.write().unwrap().extend(peers);
    }

    /// Refuse every RPC from these peers at the listener while this node
    /// still calls them: the node falls behind and can still ask.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn isolate_inbound(&self, peers: impl IntoIterator<Item = NodeId>) {
        self.deaf.write().unwrap().extend(peers);
    }

    #[cfg(any(test, feature = "fault-injection"))]
    pub fn heal(&self) {
        self.blocked.write().unwrap().clear();
        self.deaf.write().unwrap().clear();
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
    rejections: Arc<SnapshotRejections>,
    transfer: Mutex<Option<Transfer>>,
    idle: Duration,
    /// The bound on the read barrier a forwarded lease runs here.
    read_timeout: Duration,
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
    rejections: PeerRejections,
}

/// A rejection a peer answered to one of this node's RPCs: a definite
/// answer from the peer (a bound, an identity, an invalid image, a timing
/// disagreement), not a transport failure. Kept per target for the
/// operator surface and for `RaftHost::add_learner`, which names the
/// rejection instead of waiting out its timeout.
#[derive(Clone, Debug)]
pub struct PeerRejection {
    /// The RPC rejected: "append", "vote" or "snapshot".
    pub action: &'static str,
    pub code: tonic::Code,
    pub message: String,
    pub at: std::time::Instant,
    /// Rejections answered by this peer since this node started.
    pub count: u64,
}

/// The latest rejection from each peer, shared by the network clients and
/// the host.
#[derive(Clone)]
pub struct PeerRejections {
    sender: Arc<tokio::sync::watch::Sender<BTreeMap<NodeId, PeerRejection>>>,
}

/// Installs this node rejected at the announce, before any byte was
/// received: what an operator reads at a peer that is not being seeded.
#[derive(Default)]
pub struct SnapshotRejections {
    count: std::sync::atomic::AtomicU64,
    last: Mutex<Option<SnapshotRejection>>,
}

/// One install rejected at the announce.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotRejection {
    pub from: NodeId,
    pub announced: u64,
    pub bound: u64,
}

/// Builds one `TonicNetwork` per target for the library.
#[derive(Clone)]
pub struct TonicNetworkFactory {
    shared: Arc<Shared>,
}

/// The caller side of a forwarded lease: one channel per ask, to the
/// leader the library named, under the same identity, timing and
/// isolation rules as the library's own calls.
#[derive(Clone)]
pub(crate) struct LeaseForwarder {
    shared: Arc<Shared>,
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

/// What a status from a peer's transport is to this node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Answer {
    /// The peer's transport is not reachable, or it answers that its core
    /// is stopped or the node isolated: retried with the library's backoff.
    Unreachable,
    /// The call ran out its deadline or was cancelled.
    Timeout,
    /// A status without the service's mark: made by the transport between
    /// the two nodes (a connection closed, a frame refused, a message past
    /// this client's decode bound), not by the peer's service. Retried
    /// with the library's backoff, and not recorded as the peer's answer.
    Transport,
    /// The peer's service answered with a refusal, by name: recorded.
    Rejection,
}

/// Unavailable and Unknown first, whoever made them (the service answers
/// Unavailable for a stopped core, an isolated node and a transfer in
/// progress); then the deadline codes; then the mark decides.
pub(crate) fn classify(status: &Status) -> Answer {
    match status.code() {
        tonic::Code::Unavailable | tonic::Code::Unknown => Answer::Unreachable,
        tonic::Code::DeadlineExceeded | tonic::Code::Cancelled => Answer::Timeout,
        _ if status.metadata().contains_key(ANSWERED) => Answer::Rejection,
        _ => Answer::Transport,
    }
}

#[cfg(test)]
mod answer_tests {
    use super::{answered, classify, Answer};
    use tonic::{Code, Status};

    /// The codes tonic uses for transport events (a connection closed
    /// with NO_ERROR is Internal, ENHANCE_YOUR_CALM is ResourceExhausted,
    /// an unexpected end of stream and this client's own decode bound are
    /// OutOfRange) are the codes the service answers with too; only the
    /// service's mark tells them apart.
    #[test]
    fn a_status_is_a_rejection_only_with_the_services_mark() {
        for code in [
            Code::Internal,
            Code::ResourceExhausted,
            Code::OutOfRange,
            Code::DataLoss,
            Code::PermissionDenied,
            Code::FailedPrecondition,
            Code::InvalidArgument,
            Code::Unauthenticated,
            Code::Aborted,
        ] {
            assert_eq!(
                classify(&Status::new(code, "transport")),
                Answer::Transport,
                "{code:?}"
            );
            assert_eq!(
                classify(&answered(Status::new(code, "answer"))),
                Answer::Rejection,
                "{code:?}"
            );
        }
        for code in [Code::Unavailable, Code::Unknown] {
            assert_eq!(classify(&Status::new(code, "down")), Answer::Unreachable);
            assert_eq!(
                classify(&answered(Status::new(code, "down"))),
                Answer::Unreachable
            );
        }
        for code in [Code::DeadlineExceeded, Code::Cancelled] {
            assert_eq!(classify(&Status::new(code, "late")), Answer::Timeout);
            assert_eq!(
                classify(&answered(Status::new(code, "late"))),
                Answer::Timeout
            );
        }
    }
}

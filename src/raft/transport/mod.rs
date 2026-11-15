//! The tonic Raft transport: the library's vote,
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
mod answer;
mod authenticate;
mod client;
mod directory;
mod network;
mod rejections;
mod stage;
mod wire;

pub use self::directory::certificate_sha256;
pub(crate) use self::network::classify;
use self::wire::*;

use super::reasons;
use super::state_machine::{SnapshotCompletion, SnapshotFile, SnapshotStaging};
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

pub const PROTOCOL_VERSION: u32 = 1;

/// Which certificate speaks for which node of one group. Registered before
/// a node is dialed or admitted; a certificate is bound to one node and a
/// node to one certificate.
pub struct PeerDirectory {
    group: SourceAuthorityIdentity,
    peers: RwLock<BTreeMap<NodeId, [u8; 32]>>,
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
    transfer: tokio::sync::Mutex<Option<Transfer>>,
    final_receipt: tokio::sync::Mutex<Option<FinalReceipt>>,
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

#[derive(PartialEq, Eq)]
struct FinalChunk {
    peer: NodeId,
    vote: openraft::Vote<NodeId>,
    meta: SnapshotMeta<NodeId, BasicNode>,
    offset: u64,
    length: usize,
    sha256: [u8; 32],
}

impl FinalChunk {
    fn new(peer: NodeId, rpc: &InstallSnapshotRequest<ControlRaft>) -> Self {
        Self {
            peer,
            vote: rpc.vote,
            meta: rpc.meta.clone(),
            offset: rpc.offset,
            length: rpc.data.len(),
            sha256: crate::sha256::digest(&rpc.data),
        }
    }
}

struct FinalReceipt {
    chunk: FinalChunk,
    completion: Arc<SnapshotCompletion>,
}

/// What staging one chunk led to.
enum Staged {
    /// The chunk was appended; more to come.
    Partial,
    /// The chunk is out of order: the sender restarts from `expect_offset`.
    Mismatch {
        expect_offset: u64,
    },
    /// The transfer is complete and the image validated; hand it to the
    /// library.
    Complete {
        meta: SnapshotMeta<NodeId, BasicNode>,
        vote: openraft::Vote<NodeId>,
        snapshot: Box<SnapshotFile>,
        completion: Arc<SnapshotCompletion>,
    },
    Replay(Arc<SnapshotCompletion>),
}

/// Metadata key on every status this service answers with. The client
/// records a rejection only for a status that carries it: a status
/// without it was made by the transport (a connection closed, a frame
/// refused, a message past the client's own decode bound) and is a
/// transport failure to retry, not a peer's answer.
pub(crate) const ANSWERED: &str = "psearch-raft-answer";

// ---------------------------------------------------------------------
// Envelope mappings
// ---------------------------------------------------------------------

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

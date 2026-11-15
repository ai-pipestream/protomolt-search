//! The state machine: the source authority store applied through its
//! committed-replay paths, with the applied position written in the same
//! transaction as each command. Snapshots are immutable generations of the
//! store file, checksummed by length and SHA-256, published through one
//! small pointer; install verifies group, applied position and membership
//! on a separate probe copy before the live store is swapped.
mod builder;
mod image;
mod machine;
mod receive;
mod staging;
mod storage;

pub(crate) use self::image::meta_to_proto;
#[cfg(test)]
pub(crate) use self::image::published_generation;
use self::image::*;
pub use self::image::{format_snapshot_id, parse_snapshot_id};

use super::types::{
    log_id_from_proto, log_id_to_proto, stored_membership_from_proto, stored_membership_to_proto,
    ControlRaft, NodeId,
};
use crate::pb::storage::{
    raft_proposal::Command, raft_reply::Reply, RaftApplied, RaftProposal, RaftRefusal, RaftReply,
    RaftSnapshotMeta, SourceAuthorityIdentity,
};
use crate::source_authority::SourceAuthorityStore;
use openraft::storage::{RaftStateMachine, Snapshot, SnapshotMeta};
use openraft::{
    AnyError, BasicNode, Entry, EntryPayload, ErrorSubject, ErrorVerb, LogId, Membership,
    RaftSnapshotBuilder, StorageError, StorageIOError, StoredMembership,
};
use prost::Message;
use std::collections::BTreeMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::task::{Context, Poll};
use tokio::io::AsyncWriteExt;
use tonic::{Code, Status};

const GENERATIONS: &str = "generations";
const POINTER: &str = "current";
const IMAGE: &str = "image.redb";
const META: &str = "meta";
const PROBE: &str = "probe.redb";
const STAGED: &str = "staged.redb";
const STREAM_BUFFER: usize = 1024 * 1024;
// One install and one successor may be owned by the core. A separate
// partial transfer may be staged, but it cannot hand off a third image.
const MAX_OWNED_RECEIVES: usize = 2;

/// The store behind the state machine. Set once at construction; a
/// snapshot install swaps the database in place under the store's own
/// locks (`SourceAuthorityStore::replace_from`), so the slot is never
/// emptied. The `None` refusal names that impossible state.
pub type SharedStore = Arc<RwLock<Option<SourceAuthorityStore>>>;

fn sm_error(verb: ErrorVerb, error: impl std::fmt::Display) -> StorageError<NodeId> {
    StorageIOError::new(
        ErrorSubject::StateMachine,
        verb,
        AnyError::error(error.to_string()),
    )
    .into()
}

fn snapshot_error(verb: ErrorVerb, error: impl std::fmt::Display) -> StorageError<NodeId> {
    StorageIOError::new(
        ErrorSubject::Snapshot(None),
        verb,
        AnyError::error(error.to_string()),
    )
    .into()
}

/// A store failure that must stop the node rather than become a reply:
/// storage, corruption and the unknown.
fn fatal(status: &Status) -> bool {
    matches!(
        status.code(),
        Code::Internal | Code::DataLoss | Code::Unknown | Code::Unavailable
    )
}

fn io(error: impl std::fmt::Display) -> Status {
    Status::internal(format!("raft snapshot storage: {error}"))
}

/// One received image, identified by the exact directory the receive was
/// started in; install consumes only this receive's artifacts. `verified`
/// names the snapshot id the complete image was validated against.
struct Receive {
    dir: PathBuf,
    verified: Option<String>,
}

/// A snapshot streams through a file. Incoming files additionally own their
/// exact directory until installation or discard; a later receive cannot
/// delete or select it. Published files own no removable directory.
pub struct SnapshotFile {
    file: tokio::fs::File,
    receive: Option<OwnedReceive>,
}

struct OwnedReceive {
    receive: Receive,
    _permit: tokio::sync::OwnedSemaphorePermit,
    completion: Option<Arc<SnapshotCompletion>>,
}

/// The one retained final-RPC receipt can observe installation even when its
/// caller cancelled. The core-owned file signals a real install or discard;
/// the ordinary core reply also handles snapshots the core declines safely.
pub(crate) struct SnapshotCompletion {
    outcome: tokio::sync::watch::Sender<Option<Result<(), Status>>>,
}

/// An incoming receive claimed from `SnapshotStaging::receiving` before
/// blocking sync and redb verification run, so dropping the caller or starting
/// another transfer can neither leak nor delete its directory.
pub(crate) struct ClaimedReceive {
    file: std::fs::File,
    receive: OwnedReceive,
}

/// Staging of incoming snapshot bytes, shared by the transport (which
/// receives and validates the complete image before the library sees it)
/// and the state machine (which swaps a validated image in). One receive
/// at a time; a receive is its own directory, and only that receive's
/// artifacts are ever consumed.
pub(crate) struct SnapshotStaging {
    snapshots: PathBuf,
    identity: SourceAuthorityIdentity,
    max_image_bytes: u64,
    receiving: Mutex<Option<Receive>>,
    counter: AtomicU64,
    owned: Arc<tokio::sync::Semaphore>,
    #[cfg(any(test, feature = "fault-injection"))]
    gates: Arc<SnapshotGates>,
}

#[cfg(any(test, feature = "fault-injection"))]
pub(crate) use super::snapshot_gates::SnapshotGates;
#[cfg(any(test, feature = "fault-injection"))]
pub use super::snapshot_gates::{AsyncSnapshotGate, SnapshotGate};

/// What the latest build of this node's own store came to, against the
/// bound this node applies to images it receives. A node whose image is
/// over its own bound cannot be seeded from a peer's image of the same
/// store until the bound is raised.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotBuildReport {
    pub generation: u64,
    pub bytes: u64,
    pub receiver_bound: u64,
    pub last_log_id: Option<LogId<NodeId>>,
}

impl SnapshotBuildReport {
    pub fn over_receiver_bound(&self) -> bool {
        self.bytes > self.receiver_bound
    }
}

pub(crate) type LastBuild = Arc<Mutex<Option<SnapshotBuildReport>>>;

pub struct ControlStateMachine {
    store: SharedStore,
    identity: SourceAuthorityIdentity,
    snapshots: PathBuf,
    membership: Mutex<StoredMembership<NodeId, BasicNode>>,
    staging: Arc<SnapshotStaging>,
    last_build: LastBuild,
    // Build and install never overlap; the builder shares this lock.
    snapshot_lock: Arc<Mutex<()>>,
    // A serve reads the pointer, the generation's meta and its image under
    // this lock; a publish (which moves the pointer and removes the other
    // generations) takes it too, so no generation goes away between a
    // serve reading the pointer and opening the image.
    pointer_lock: Arc<Mutex<()>>,
    #[cfg(any(test, feature = "fault-injection"))]
    gates: Arc<SnapshotGates>,
    // A process-exit fault armed for the next install's store replacement.
    #[cfg(any(test, feature = "fault-injection"))]
    install_fault: InstallFault,
}

#[cfg(any(test, feature = "fault-injection"))]
pub(crate) type InstallFault = Arc<Mutex<Option<crate::source_authority::ExitFault>>>;

/// Images the node's own store. The image is not bounded here:
/// `max_snapshot_bytes` is each receiver's bound, applied where an image
/// arrives, so a store over a peer's bound is rejected at that peer by name
/// and the building node keeps running.
pub struct ControlSnapshotBuilder {
    store: SharedStore,
    identity: SourceAuthorityIdentity,
    snapshots: PathBuf,
    /// The bound this node applies to images it receives; the build is not
    /// bounded, the report compares.
    receiver_bound: u64,
    last_build: LastBuild,
    snapshot_lock: Arc<Mutex<()>>,
    pointer_lock: Arc<Mutex<()>>,
    #[cfg(any(test, feature = "fault-injection"))]
    gates: Arc<SnapshotGates>,
}

/// Length and SHA-256 of an image: an integrity checksum bound into the
/// snapshot id and meta, not an authenticated signature.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageSignature {
    pub length: u64,
    pub sha256: [u8; 32],
}

/// The published generation, read by digest on request. A serve reads the
/// image by length and the receiver verifies the digest; when a receiver
/// rejects the image by digest, this is how the node tells its own disk
/// from the wire.
pub(crate) struct PublishedImage {
    identity: SourceAuthorityIdentity,
    snapshots: PathBuf,
    pointer_lock: Arc<Mutex<()>>,
}

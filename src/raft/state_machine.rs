//! The state machine: the source authority store applied through its
//! committed-replay paths, with the applied position written in the same
//! transaction as each command. Snapshots are immutable generations of the
//! store file, checksummed by length and SHA-256, published through one
//! small pointer; install verifies group, applied position and membership
//! on a separate probe copy before the live store is swapped.
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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use tokio::io::AsyncWriteExt;
use tonic::{Code, Status};

const GENERATIONS: &str = "generations";
const POINTER: &str = "current";
const IMAGE: &str = "image.redb";
const META: &str = "meta";
const PROBE: &str = "probe.redb";
const STAGED: &str = "staged.redb";
const STREAM_BUFFER: usize = 1024 * 1024;

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
}

impl SnapshotStaging {
    pub(crate) fn max_image_bytes(&self) -> u64 {
        self.max_image_bytes
    }

    pub(crate) fn image_path(dir: &Path) -> PathBuf {
        dir.join(IMAGE)
    }

    /// Begin a receive: a fresh directory with an empty image file. A
    /// previous unfinished receive is dropped with its bytes.
    pub(crate) fn begin(&self) -> Result<(PathBuf, std::fs::File), Status> {
        if let Some(previous) = self.receiving.lock().unwrap().take() {
            let _ = std::fs::remove_dir_all(&previous.dir);
        }
        let token = format!(
            "incoming-{}-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0),
            self.counter.fetch_add(1, Ordering::Relaxed)
        );
        let dir = self.snapshots.join(token);
        std::fs::create_dir(&dir).map_err(io)?;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(dir.join(IMAGE))
            .map_err(io)?;
        *self.receiving.lock().unwrap() = Some(Receive {
            dir: dir.clone(),
            verified: None,
        });
        Ok((dir, file))
    }

    /// Whether `dir` is the current receive.
    pub(crate) fn is_current(&self, dir: &Path) -> bool {
        self.receiving
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|r| r.dir == dir)
    }

    /// Validate the complete image in `dir` against `meta`: the announced
    /// bound, its length and digest, then a separate probe copy opened as a
    /// store of this group whose applied position and membership must
    /// equal the meta. The received bytes stay untouched and are marked
    /// verified for this meta.
    pub(crate) fn verify(
        &self,
        dir: &Path,
        meta: &SnapshotMeta<NodeId, BasicNode>,
    ) -> Result<ImageSignature, Status> {
        let image = dir.join(IMAGE);
        let signature = parse_snapshot_id(&meta.snapshot_id)?;
        if signature.length > self.max_image_bytes {
            return Err(Status::resource_exhausted(format!(
                "snapshot image of {} bytes exceeds the {} byte bound",
                signature.length, self.max_image_bytes
            )));
        }
        verify_image(&image, &signature, self.max_image_bytes)?;
        // Probe a separate copy: a redb open may rewrite recovery state, and
        // the received bytes must stay identical to their checksum.
        let probe = dir.join(PROBE);
        std::fs::copy(&image, &probe).map_err(io)?;
        let checked =
            (|| -> Result<(), Status> {
                let opened = SourceAuthorityStore::open(&probe, &self.identity)
                    .map_err(|e| Status::data_loss(format!("snapshot image: {}", e.message())))?;
                let applied = opened.raft_applied()?.ok_or_else(|| {
                    Status::data_loss("snapshot image carries no applied position")
                })?;
                if applied.last_applied.as_ref().map(log_id_from_proto) != meta.last_log_id {
                    return Err(Status::data_loss(
                        "snapshot image applied position differs from the snapshot meta",
                    ));
                }
                let membership =
                    stored_membership_from_proto(applied.membership.as_ref().ok_or_else(
                        || Status::data_loss("snapshot image carries no membership"),
                    )?)?;
                if membership != meta.last_membership {
                    return Err(Status::data_loss(
                        "snapshot image membership differs from the snapshot meta",
                    ));
                }
                Ok(())
            })();
        let _ = std::fs::remove_file(&probe);
        checked?;
        if let Some(receive) = self.receiving.lock().unwrap().as_mut() {
            if receive.dir == dir {
                receive.verified = Some(meta.snapshot_id.clone());
            }
        }
        Ok(signature)
    }

    /// Drop a receive and its bytes.
    pub(crate) fn discard(&self, dir: &Path) {
        let mut receiving = self.receiving.lock().unwrap();
        if receiving.as_ref().is_some_and(|r| r.dir == dir) {
            *receiving = None;
        }
        let _ = std::fs::remove_dir_all(dir);
    }

    fn take(&self) -> Option<Receive> {
        self.receiving.lock().unwrap().take()
    }
}

/// A test and fault-injection hook on the snapshot paths: the next build
/// pauses once its generation directory is in place and before it
/// publishes; the next serve pauses after it read the pointer and before
/// it opens the generation. Each pauses on its own thread, under the locks
/// it holds there, so the pause shows what those locks exclude. An armed
/// gate that is reached must be released.
#[cfg(any(test, feature = "fault-injection"))]
pub struct SnapshotGate {
    reached: tokio::sync::watch::Sender<bool>,
    release: Mutex<Option<std::sync::mpsc::Sender<()>>>,
    wait: Mutex<Option<std::sync::mpsc::Receiver<()>>>,
}

#[cfg(any(test, feature = "fault-injection"))]
impl SnapshotGate {
    fn new() -> Self {
        let (release, wait) = std::sync::mpsc::channel();
        Self {
            reached: tokio::sync::watch::Sender::new(false),
            release: Mutex::new(Some(release)),
            wait: Mutex::new(Some(wait)),
        }
    }

    /// Resolves once the paused path is at the gate.
    pub async fn reached(&self) {
        let mut receiver = self.reached.subscribe();
        let _ = receiver.wait_for(|reached| *reached).await;
    }

    pub fn release(&self) {
        if let Some(release) = self.release.lock().unwrap().take() {
            let _ = release.send(());
        }
    }

    /// Block the calling thread at the gate until released.
    fn pause(&self) {
        let wait = self.wait.lock().unwrap().take();
        self.reached.send_replace(true);
        if let Some(wait) = wait {
            let _ = wait.recv();
        }
    }
}

#[cfg(any(test, feature = "fault-injection"))]
#[derive(Default)]
pub(crate) struct SnapshotGates {
    build: Mutex<Option<Arc<SnapshotGate>>>,
    serve: Mutex<Option<Arc<SnapshotGate>>>,
}

#[cfg(any(test, feature = "fault-injection"))]
impl SnapshotGates {
    pub(crate) fn arm_build(&self) -> Arc<SnapshotGate> {
        Self::arm(&self.build)
    }

    pub(crate) fn arm_serve(&self) -> Arc<SnapshotGate> {
        Self::arm(&self.serve)
    }

    fn pause_build(&self) {
        Self::pause(&self.build)
    }

    fn pause_serve(&self) {
        Self::pause(&self.serve)
    }

    fn arm(slot: &Mutex<Option<Arc<SnapshotGate>>>) -> Arc<SnapshotGate> {
        let gate = Arc::new(SnapshotGate::new());
        *slot.lock().unwrap() = Some(Arc::clone(&gate));
        gate
    }

    fn pause(slot: &Mutex<Option<Arc<SnapshotGate>>>) {
        let gate = slot.lock().unwrap().take();
        if let Some(gate) = gate {
            gate.pause();
        }
    }
}

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
}

impl ControlStateMachine {
    /// Wrap an opened store. The store is marked Raft-hosted: its direct
    /// command paths refuse from here on. Abandoned receives and builds and
    /// unpublished generations are removed; the published one is kept.
    pub(crate) fn new(
        store: SourceAuthorityStore,
        snapshots: &Path,
        max_image_bytes: u64,
    ) -> Result<Self, Status> {
        store.set_raft_hosted();
        std::fs::create_dir_all(snapshots.join(GENERATIONS)).map_err(io)?;
        let membership = match store.raft_applied()? {
            Some(applied) => stored_membership_from_proto(
                applied
                    .membership
                    .as_ref()
                    .ok_or_else(|| Status::data_loss("raft applied position has no membership"))?,
            )?,
            None => StoredMembership::new(None, Membership::new(Vec::new(), BTreeMap::new())),
        };
        let store_identity = store.identity().clone();
        let machine = Self {
            identity: store.identity().clone(),
            store: Arc::new(RwLock::new(Some(store))),
            snapshots: snapshots.to_path_buf(),
            membership: Mutex::new(membership),
            staging: Arc::new(SnapshotStaging {
                snapshots: snapshots.to_path_buf(),
                identity: store_identity,
                max_image_bytes,
                receiving: Mutex::new(None),
                counter: AtomicU64::new(0),
            }),
            last_build: Arc::new(Mutex::new(None)),
            snapshot_lock: Arc::new(Mutex::new(())),
            pointer_lock: Arc::new(Mutex::new(())),
            #[cfg(any(test, feature = "fault-injection"))]
            gates: Arc::new(SnapshotGates::default()),
        };
        machine.sweep()?;
        Ok(machine)
    }

    /// The staging area the transport receives and validates images in.
    pub(crate) fn staging(&self) -> Arc<SnapshotStaging> {
        Arc::clone(&self.staging)
    }

    /// The latest build's report, shared with the host.
    pub(crate) fn last_build(&self) -> LastBuild {
        Arc::clone(&self.last_build)
    }

    /// The pause hooks on the snapshot build and serve paths.
    #[cfg(any(test, feature = "fault-injection"))]
    pub(crate) fn gates(&self) -> Arc<SnapshotGates> {
        Arc::clone(&self.gates)
    }

    pub(crate) fn shared_store(&self) -> SharedStore {
        Arc::clone(&self.store)
    }

    fn store(&self) -> Result<SourceAuthorityStore, Status> {
        self.store
            .read()
            .map_err(|_| Status::internal("state machine store lock poisoned"))?
            .clone()
            .ok_or_else(|| Status::unavailable("state machine store is being replaced"))
    }

    /// Remove everything but the published generation: partial builds,
    /// abandoned receives and generations whose pointer never landed.
    fn sweep(&self) -> Result<(), Status> {
        let published = read_pointer(&self.snapshots)?;
        for entry in std::fs::read_dir(&self.snapshots).map_err(io)? {
            let entry = entry.map_err(io)?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with("incoming-") || name.starts_with("build-") || name == STAGED {
                if entry.path().is_dir() {
                    std::fs::remove_dir_all(entry.path()).map_err(io)?;
                } else {
                    std::fs::remove_file(entry.path()).map_err(io)?;
                }
            }
        }
        for entry in std::fs::read_dir(self.snapshots.join(GENERATIONS)).map_err(io)? {
            let entry = entry.map_err(io)?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.parse::<u64>().ok() != published {
                std::fs::remove_dir_all(entry.path()).map_err(io)?;
            }
        }
        if let Some(generation) = published {
            let dir = self
                .snapshots
                .join(GENERATIONS)
                .join(generation.to_string());
            // A pointer without a complete generation is data loss, never a
            // reason to start over.
            let (meta, _) = read_generation(&dir, &self.identity)?;
            // A generation is an image of this store at its applied position;
            // one claiming a position the store never applied is data loss
            // too (a forged pointer, or a store restored from an older copy),
            // and the library's own invariants would fail on it.
            let claimed = meta.last_log_id.map(|id| id.index);
            let applied = self
                .store()?
                .raft_applied()?
                .and_then(|a| a.last_applied)
                .map(|id| id.index);
            if claimed > applied {
                return Err(Status::data_loss(format!(
                    "published snapshot generation {generation} claims position {} past the store's applied position {}",
                    claimed.map_or_else(|| "none".to_string(), |i| i.to_string()),
                    applied.map_or_else(|| "none".to_string(), |i| i.to_string())
                )));
            }
        }
        Ok(())
    }

    fn dispatch(store: &SourceAuthorityStore, proposal: &RaftProposal) -> Result<Reply, Status> {
        let principal = proposal.principal.as_str();
        match proposal
            .command
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("raft proposal carries no command"))?
        {
            Command::Control(command) => {
                Ok(Reply::Control(store.replay_command(principal, command)?))
            }
            Command::Import(command) => Ok(Reply::Import(
                store.replay_control_import(principal, command)?,
            )),
            Command::Capacity(command) => Ok(Reply::Capacity(
                store.replay_capacity_configure(principal, command)?,
            )),
            Command::Observation(transition) => Ok(Reply::Observation(
                store.replay_capacity_transition(principal, transition)?,
            )),
        }
    }

    /// The applied position the store recorded must be the entry just
    /// applied; anything else means an application path skipped it.
    fn confirm_position(store: &SourceAuthorityStore, entry: &LogId<NodeId>) -> Result<(), Status> {
        let applied = store
            .raft_applied()?
            .and_then(|a| a.last_applied)
            .map(|id| log_id_from_proto(&id));
        if applied.as_ref() != Some(entry) {
            return Err(Status::internal(format!(
                "applied position {applied:?} differs from the entry {entry} just applied"
            )));
        }
        Ok(())
    }
}

impl RaftStateMachine<ControlRaft> for ControlStateMachine {
    type SnapshotBuilder = ControlSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>>
    {
        let store = self.store().map_err(|e| sm_error(ErrorVerb::Read, e))?;
        match store
            .raft_applied()
            .map_err(|e| sm_error(ErrorVerb::Read, e))?
        {
            Some(applied) => {
                let membership =
                    stored_membership_from_proto(applied.membership.as_ref().ok_or_else(|| {
                        sm_error(ErrorVerb::Read, "raft applied position has no membership")
                    })?)
                    .map_err(|e| sm_error(ErrorVerb::Read, e))?;
                *self.membership.lock().unwrap() = membership.clone();
                Ok((
                    applied.last_applied.as_ref().map(log_id_from_proto),
                    membership,
                ))
            }
            None => Ok((None, self.membership.lock().unwrap().clone())),
        }
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<RaftReply>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<ControlRaft>> + Send,
        I::IntoIter: Send,
    {
        let store = self.store().map_err(|e| sm_error(ErrorVerb::Write, e))?;
        let mut replies = Vec::new();
        for entry in entries {
            let mut membership = self.membership.lock().unwrap().clone();
            if let EntryPayload::Membership(m) = &entry.payload {
                membership = StoredMembership::new(Some(entry.log_id), m.clone());
            }
            let applied = RaftApplied {
                format_version: 1,
                last_applied: Some(log_id_to_proto(&entry.log_id)),
                membership: Some(stored_membership_to_proto(&membership)),
            };
            let reply = match &entry.payload {
                EntryPayload::Blank | EntryPayload::Membership(_) => {
                    store
                        .apply_raft_position(&applied)
                        .map_err(|e| sm_error(ErrorVerb::Write, e))?;
                    RaftReply {
                        format_version: 1,
                        reply: None,
                    }
                }
                EntryPayload::Normal(proposal) => {
                    match store.raft_apply(applied.clone(), || Self::dispatch(&store, proposal)) {
                        Ok(reply) => RaftReply {
                            format_version: 1,
                            reply: Some(reply),
                        },
                        Err(status) if fatal(&status) => {
                            return Err(sm_error(ErrorVerb::Write, status));
                        }
                        Err(status) => {
                            // An envelope or admission refusal: the entry is
                            // consumed and the position advances; the refusal
                            // is the recorded outcome.
                            store
                                .apply_raft_position(&applied)
                                .map_err(|e| sm_error(ErrorVerb::Write, e))?;
                            RaftReply {
                                format_version: 1,
                                reply: Some(Reply::Refusal(RaftRefusal {
                                    code: status.code() as u32,
                                    message: status.message().to_string(),
                                })),
                            }
                        }
                    }
                }
            };
            Self::confirm_position(&store, &entry.log_id)
                .map_err(|e| sm_error(ErrorVerb::Write, e))?;
            *self.membership.lock().unwrap() = membership;
            replies.push(reply);
        }
        Ok(replies)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        ControlSnapshotBuilder {
            store: Arc::clone(&self.store),
            identity: self.identity.clone(),
            snapshots: self.snapshots.clone(),
            receiver_bound: self.staging.max_image_bytes,
            last_build: Arc::clone(&self.last_build),
            snapshot_lock: Arc::clone(&self.snapshot_lock),
            pointer_lock: Arc::clone(&self.pointer_lock),
            #[cfg(any(test, feature = "fault-injection"))]
            gates: Arc::clone(&self.gates),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<tokio::fs::File>, StorageError<NodeId>> {
        let (_, file) = self
            .staging
            .begin()
            .map_err(|e| snapshot_error(ErrorVerb::Write, e))?;
        Ok(Box::new(tokio::fs::File::from_std(file)))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        mut snapshot: Box<tokio::fs::File>,
    ) -> Result<(), StorageError<NodeId>> {
        snapshot
            .flush()
            .await
            .map_err(|e| snapshot_error(ErrorVerb::Write, e))?;
        snapshot
            .sync_all()
            .await
            .map_err(|e| snapshot_error(ErrorVerb::Write, e))?;
        drop(snapshot);
        let receive = self
            .staging
            .take()
            .ok_or_else(|| snapshot_error(ErrorVerb::Read, "no receive is in progress"))?;
        let result = self.install_received(meta, &receive);
        if result.is_err() {
            let _ = std::fs::remove_dir_all(&receive.dir);
        }
        result.map_err(|e| snapshot_error(ErrorVerb::Write, e))
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<ControlRaft>>, StorageError<NodeId>> {
        // One step under the pointer lock, from reading the pointer to
        // opening the image: a build publishing in its own task meanwhile
        // waits, so the generation this serve is on is not removed under
        // it. Once open, the file outlives the directory's removal. A read
        // that fails here is a real storage failure, and fatal by the
        // library's contract. The image's length is checked; its digest
        // is not read here (a serve is repeated for as long as a peer
        // rejects the image): the receiver verifies the digest and rejects
        // by name, and this node's start verifies it in the sweep.
        let _pointer = self.pointer_lock.lock().unwrap();
        let Some(generation) =
            read_pointer(&self.snapshots).map_err(|e| snapshot_error(ErrorVerb::Read, e))?
        else {
            return Ok(None);
        };
        #[cfg(any(test, feature = "fault-injection"))]
        self.gates.pause_serve();
        let dir = self
            .snapshots
            .join(GENERATIONS)
            .join(generation.to_string());
        let (meta, _) = read_generation_by_length(&dir, &self.identity)
            .map_err(|e| snapshot_error(ErrorVerb::Read, e))?;
        let file =
            std::fs::File::open(dir.join(IMAGE)).map_err(|e| snapshot_error(ErrorVerb::Read, e))?;
        Ok(Some(Snapshot {
            meta,
            snapshot: Box::new(tokio::fs::File::from_std(file)),
        }))
    }
}

impl ControlStateMachine {
    fn install_received(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        receive: &Receive,
    ) -> Result<(), Status> {
        let _serial = self.snapshot_lock.lock().unwrap();
        // The transport validates the complete image before the library's
        // install runs; a receive that arrived another way is validated
        // here, on the same rules.
        let signature = if receive.verified.as_deref() == Some(meta.snapshot_id.as_str()) {
            parse_snapshot_id(&meta.snapshot_id)?
        } else {
            self.staging.verify(&receive.dir, meta)?
        };
        write_meta(&receive.dir, &self.identity, meta, &signature)?;
        let generation = next_generation(&self.snapshots)?;
        let target = self
            .snapshots
            .join(GENERATIONS)
            .join(generation.to_string());
        std::fs::rename(&receive.dir, &target).map_err(io)?;
        sync_dir(&self.snapshots.join(GENERATIONS))?;
        // Swap the live store to a fresh copy of the verified image in
        // place; the generation's image stays pristine. Every handle sees
        // the installed state on its next operation; on refusal nothing
        // changed and the unpublished generation is removed.
        let staged = self.snapshots.join(STAGED);
        std::fs::copy(target.join(IMAGE), &staged).map_err(io)?;
        std::fs::File::open(&staged)
            .and_then(|f| f.sync_all())
            .map_err(io)?;
        let store = self.store()?;
        if let Err(status) = store.replace_from(&staged) {
            let _ = std::fs::remove_file(&staged);
            let _ = std::fs::remove_dir_all(&target);
            return Err(status);
        }
        drop(store);
        publish(&self.pointer_lock, &self.snapshots, generation)?;
        *self.membership.lock().unwrap() = meta.last_membership.clone();
        Ok(())
    }
}

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

impl RaftSnapshotBuilder<ControlRaft> for ControlSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<ControlRaft>, StorageError<NodeId>> {
        self.build()
            .map_err(|e| snapshot_error(ErrorVerb::Write, e))
    }
}

impl ControlSnapshotBuilder {
    fn build(&self) -> Result<Snapshot<ControlRaft>, Status> {
        let _serial = self.snapshot_lock.lock().unwrap();
        let store = self
            .store
            .read()
            .map_err(|_| Status::internal("state machine store lock poisoned"))?
            .clone()
            .ok_or_else(|| Status::unavailable("state machine store is being replaced"))?;
        let build_dir = self.snapshots.join(format!(
            "build-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir(&build_dir).map_err(io)?;
        let image = build_dir.join(IMAGE);
        let outcome = (|| -> Result<Snapshot<ControlRaft>, Status> {
            let applied = store.quiesced(|path, applied| {
                std::fs::copy(path, &image).map_err(io)?;
                std::fs::File::open(&image)
                    .and_then(|f| f.sync_all())
                    .map_err(io)?;
                Ok(applied)
            })?;
            let Some(applied) = applied else {
                return Err(Status::failed_precondition(
                    "no applied position; nothing to snapshot",
                ));
            };
            let last_log_id = applied.last_applied.as_ref().map(log_id_from_proto);
            let membership = stored_membership_from_proto(
                applied
                    .membership
                    .as_ref()
                    .ok_or_else(|| Status::data_loss("applied position has no membership"))?,
            )?;
            let signature = image_signature(&image, u64::MAX)?;
            let meta = SnapshotMeta {
                last_log_id,
                last_membership: membership,
                snapshot_id: format_snapshot_id(last_log_id.as_ref(), &signature),
            };
            write_meta(&build_dir, &self.identity, &meta, &signature)?;
            let generation = next_generation(&self.snapshots)?;
            let target = self
                .snapshots
                .join(GENERATIONS)
                .join(generation.to_string());
            std::fs::rename(&build_dir, &target).map_err(io)?;
            sync_dir(&self.snapshots.join(GENERATIONS))?;
            #[cfg(any(test, feature = "fault-injection"))]
            self.gates.pause_build();
            publish(&self.pointer_lock, &self.snapshots, generation)?;
            if let Ok(mut last) = self.last_build.lock() {
                *last = Some(SnapshotBuildReport {
                    generation,
                    bytes: signature.length,
                    receiver_bound: self.receiver_bound,
                    last_log_id,
                });
            }
            let file = std::fs::File::open(target.join(IMAGE)).map_err(io)?;
            Ok(Snapshot {
                meta,
                snapshot: Box::new(tokio::fs::File::from_std(file)),
            })
        })();
        if outcome.is_err() {
            let _ = std::fs::remove_dir_all(&build_dir);
        }
        outcome
    }
}

fn next_generation(snapshots: &Path) -> Result<u64, Status> {
    let mut highest = read_pointer(snapshots)?.unwrap_or(0);
    for entry in std::fs::read_dir(snapshots.join(GENERATIONS)).map_err(io)? {
        let entry = entry.map_err(io)?;
        if let Ok(n) = entry.file_name().to_string_lossy().parse::<u64>() {
            highest = highest.max(n);
        }
    }
    highest
        .checked_add(1)
        .ok_or_else(|| Status::resource_exhausted("snapshot generations exhausted"))
}

/// Publish one generation through the pointer, then drop the other
/// generations, under the pointer lock a serve holds from reading the
/// pointer to opening its image. The previous generation stays usable
/// until the pointer moved.
fn publish(lock: &Mutex<()>, snapshots: &Path, generation: u64) -> Result<(), Status> {
    let _pointer = lock.lock().unwrap();
    write_pointer(snapshots, generation)?;
    for entry in std::fs::read_dir(snapshots.join(GENERATIONS)).map_err(io)? {
        let entry = entry.map_err(io)?;
        if entry.file_name().to_string_lossy().parse::<u64>().ok() != Some(generation) {
            std::fs::remove_dir_all(entry.path()).map_err(io)?;
        }
    }
    Ok(())
}

/// Length and SHA-256 of an image: an integrity checksum bound into the
/// snapshot id and meta, not an authenticated signature.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageSignature {
    pub length: u64,
    pub sha256: [u8; 32],
}

/// Streams the file through a bounded buffer; refuses past `max_bytes`.
fn image_signature(path: &Path, max_bytes: u64) -> Result<ImageSignature, Status> {
    let mut file = std::fs::File::open(path).map_err(io)?;
    let mut hasher = crate::sha256::Sha256::new();
    let mut buffer = vec![0u8; STREAM_BUFFER];
    let mut length = 0u64;
    loop {
        let read = file.read(&mut buffer).map_err(io)?;
        if read == 0 {
            break;
        }
        length += read as u64;
        if length > max_bytes {
            return Err(Status::resource_exhausted(format!(
                "snapshot image exceeds the {max_bytes} byte bound"
            )));
        }
        hasher.update(&buffer[..read]);
    }
    Ok(ImageSignature {
        length,
        sha256: hasher.finalize(),
    })
}

fn verify_image(path: &Path, expected: &ImageSignature, max_bytes: u64) -> Result<(), Status> {
    let length = std::fs::metadata(path).map_err(io)?.len();
    if length != expected.length {
        return Err(Status::data_loss(format!(
            "snapshot image length {length} differs from the announced {}",
            expected.length
        )));
    }
    let actual = image_signature(path, max_bytes)?;
    if actual != *expected {
        return Err(Status::data_loss(
            "snapshot image digest differs from the announced digest",
        ));
    }
    Ok(())
}

/// `"<index>-<length>-<sha256 hex>"`: the id openraft carries names the
/// exact bytes, so a receiver verifies before it installs.
pub fn format_snapshot_id(
    last_log_id: Option<&LogId<NodeId>>,
    signature: &ImageSignature,
) -> String {
    format!(
        "{}-{}-{}",
        last_log_id.map_or(0, |id| id.index),
        signature.length,
        crate::sha256::to_hex(&signature.sha256)
    )
}

pub fn parse_snapshot_id(id: &str) -> Result<ImageSignature, Status> {
    let mut parts = id.splitn(3, '-');
    let _index = parts.next();
    let length = parts
        .next()
        .and_then(|s| s.parse::<u64>().ok())
        .ok_or_else(|| Status::data_loss("snapshot id carries no length"))?;
    let hex = parts
        .next()
        .ok_or_else(|| Status::data_loss("snapshot id carries no digest"))?;
    let bytes = hex.as_bytes();
    if bytes.len() != 64 {
        return Err(Status::data_loss("snapshot id digest is not 64 hex digits"));
    }
    let nibble = |c: u8| match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        _ => Err(Status::data_loss("snapshot id digest is not lowercase hex")),
    };
    let mut sha256 = [0u8; 32];
    for (i, pair) in bytes.chunks(2).enumerate() {
        sha256[i] = (nibble(pair[0])? << 4) | nibble(pair[1])?;
    }
    Ok(ImageSignature { length, sha256 })
}

fn sync_dir(dir: &Path) -> Result<(), Status> {
    std::fs::File::open(dir)
        .and_then(|d| d.sync_all())
        .map_err(io)
}

/// The stored form of a snapshot meta: the group, the position, the
/// membership, the id and the image's length and digest.
pub(crate) fn meta_to_proto(
    group: &SourceAuthorityIdentity,
    meta: &SnapshotMeta<NodeId, BasicNode>,
    signature: &ImageSignature,
) -> RaftSnapshotMeta {
    RaftSnapshotMeta {
        format_version: 1,
        group: Some(group.clone()),
        last_log_id: meta.last_log_id.as_ref().map(log_id_to_proto),
        membership: Some(stored_membership_to_proto(&meta.last_membership)),
        snapshot_id: meta.snapshot_id.clone(),
        length: signature.length,
        sha256: signature.sha256.to_vec(),
    }
}

fn write_meta(
    dir: &Path,
    group: &SourceAuthorityIdentity,
    meta: &SnapshotMeta<NodeId, BasicNode>,
    signature: &ImageSignature,
) -> Result<(), Status> {
    let value = meta_to_proto(group, meta, signature);
    let path = dir.join(META);
    std::fs::write(&path, value.encode_to_vec()).map_err(io)?;
    std::fs::File::open(&path)
        .and_then(|f| f.sync_all())
        .map_err(io)?;
    sync_dir(dir)
}

/// The published generation, read from the pointer file.
fn read_pointer(snapshots: &Path) -> Result<Option<u64>, Status> {
    match std::fs::read_to_string(snapshots.join(POINTER)) {
        Ok(text) => text
            .trim()
            .parse::<u64>()
            .map(Some)
            .map_err(|_| Status::data_loss("snapshot pointer is not a generation number")),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(io(error)),
    }
}

/// Publish a generation: write the pointer to a temporary file, sync it,
/// rename it into place and sync the directory. Readers see the old or the
/// new pointer, never a partial one.
fn write_pointer(snapshots: &Path, generation: u64) -> Result<(), Status> {
    let tmp = snapshots.join("current.tmp");
    std::fs::write(&tmp, generation.to_string()).map_err(io)?;
    std::fs::File::open(&tmp)
        .and_then(|f| f.sync_all())
        .map_err(io)?;
    std::fs::rename(&tmp, snapshots.join(POINTER)).map_err(io)?;
    sync_dir(snapshots)
}

/// Read one generation: the meta must name this group, its id must be the
/// image's length and digest, and the image must verify against them.
fn read_generation(
    dir: &Path,
    group: &SourceAuthorityIdentity,
) -> Result<(SnapshotMeta<NodeId, BasicNode>, ImageSignature), Status> {
    read_generation_with(dir, group, |image, signature| {
        verify_image(image, signature, u64::MAX)
    })
}

/// [`read_generation`] with the image checked by length only: the serve
/// path, where the receiver verifies the digest.
fn read_generation_by_length(
    dir: &Path,
    group: &SourceAuthorityIdentity,
) -> Result<(SnapshotMeta<NodeId, BasicNode>, ImageSignature), Status> {
    read_generation_with(dir, group, |image, signature| {
        let length = std::fs::metadata(image).map_err(io)?.len();
        if length != signature.length {
            return Err(Status::data_loss(format!(
                "snapshot image length {length} differs from the announced {}",
                signature.length
            )));
        }
        Ok(())
    })
}

fn read_generation_with(
    dir: &Path,
    group: &SourceAuthorityIdentity,
    check: impl FnOnce(&Path, &ImageSignature) -> Result<(), Status>,
) -> Result<(SnapshotMeta<NodeId, BasicNode>, ImageSignature), Status> {
    let bytes = std::fs::read(dir.join(META)).map_err(io)?;
    let value = RaftSnapshotMeta::decode(bytes.as_slice())
        .map_err(|e| Status::data_loss(format!("snapshot meta: {e}")))?;
    if value.format_version != 1 || value.sha256.len() != 32 {
        return Err(Status::data_loss(
            "snapshot meta format or digest is invalid",
        ));
    }
    if value.group.as_ref() != Some(group) {
        return Err(Status::data_loss("snapshot meta names another group"));
    }
    let signature = ImageSignature {
        length: value.length,
        sha256: value.sha256.as_slice().try_into().expect("checked length"),
    };
    check(&dir.join(IMAGE), &signature)?;
    let last_log_id = value.last_log_id.as_ref().map(log_id_from_proto);
    if value.snapshot_id != format_snapshot_id(last_log_id.as_ref(), &signature) {
        return Err(Status::data_loss("snapshot meta id differs from its image"));
    }
    Ok((
        SnapshotMeta {
            last_log_id,
            last_membership: stored_membership_from_proto(
                value
                    .membership
                    .as_ref()
                    .ok_or_else(|| Status::data_loss("snapshot meta has no membership"))?,
            )?,
            snapshot_id: value.snapshot_id,
        },
        signature,
    ))
}

/// The published generation's directory, for tests.
#[cfg(test)]
pub(crate) fn published_generation(snapshots: &Path) -> Result<Option<PathBuf>, Status> {
    Ok(read_pointer(snapshots)?.map(|g| snapshots.join(GENERATIONS).join(g.to_string())))
}

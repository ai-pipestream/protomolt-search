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
use crate::source_authority::{ReplaceFailure, SourceAuthorityStore};
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

/// The store behind the state machine. `None` only while a snapshot swap
/// is between closing the old file and opening the new one.
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
/// started in; install consumes only this receive's artifacts.
struct Receive {
    dir: PathBuf,
}

pub struct ControlStateMachine {
    store: SharedStore,
    identity: SourceAuthorityIdentity,
    live_path: PathBuf,
    snapshots: PathBuf,
    max_image_bytes: u64,
    membership: Mutex<StoredMembership<NodeId, BasicNode>>,
    receiving: Mutex<Option<Receive>>,
    // Build and install never overlap; the builder shares this lock.
    snapshot_lock: Arc<Mutex<()>>,
    receive_counter: AtomicU64,
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
        let machine = Self {
            identity: store.identity().clone(),
            live_path: store.path().to_path_buf(),
            store: Arc::new(RwLock::new(Some(store))),
            snapshots: snapshots.to_path_buf(),
            max_image_bytes,
            membership: Mutex::new(membership),
            receiving: Mutex::new(None),
            snapshot_lock: Arc::new(Mutex::new(())),
            receive_counter: AtomicU64::new(0),
        };
        machine.sweep()?;
        Ok(machine)
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
            max_image_bytes: self.max_image_bytes,
            snapshot_lock: Arc::clone(&self.snapshot_lock),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<tokio::fs::File>, StorageError<NodeId>> {
        // One receive at a time; a previous unfinished receive is dropped
        // with its bytes.
        if let Some(previous) = self.receiving.lock().unwrap().take() {
            let _ = std::fs::remove_dir_all(&previous.dir);
        }
        let token = format!(
            "incoming-{}-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0),
            self.receive_counter.fetch_add(1, Ordering::Relaxed)
        );
        let dir = self.snapshots.join(token);
        std::fs::create_dir(&dir).map_err(|e| snapshot_error(ErrorVerb::Write, e))?;
        let file = tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(dir.join(IMAGE))
            .await
            .map_err(|e| snapshot_error(ErrorVerb::Write, e))?;
        *self.receiving.lock().unwrap() = Some(Receive { dir });
        Ok(Box::new(file))
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
            .receiving
            .lock()
            .unwrap()
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
        let Some(generation) =
            read_pointer(&self.snapshots).map_err(|e| snapshot_error(ErrorVerb::Read, e))?
        else {
            return Ok(None);
        };
        let dir = self
            .snapshots
            .join(GENERATIONS)
            .join(generation.to_string());
        let (meta, _) = read_generation(&dir, &self.identity)
            .map_err(|e| snapshot_error(ErrorVerb::Read, e))?;
        let file = tokio::fs::File::open(dir.join(IMAGE))
            .await
            .map_err(|e| snapshot_error(ErrorVerb::Read, e))?;
        Ok(Some(Snapshot {
            meta,
            snapshot: Box::new(file),
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
        let image = receive.dir.join(IMAGE);
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
        let probe = receive.dir.join(PROBE);
        std::fs::copy(&image, &probe).map_err(io)?;
        {
            let opened = SourceAuthorityStore::open(&probe, &self.identity)
                .map_err(|e| Status::data_loss(format!("snapshot image: {}", e.message())))?;
            let applied = opened
                .raft_applied()?
                .ok_or_else(|| Status::data_loss("snapshot image carries no applied position"))?;
            if applied.last_applied.as_ref().map(log_id_from_proto) != meta.last_log_id {
                return Err(Status::data_loss(
                    "snapshot image applied position differs from the snapshot meta",
                ));
            }
            let membership = stored_membership_from_proto(
                applied
                    .membership
                    .as_ref()
                    .ok_or_else(|| Status::data_loss("snapshot image carries no membership"))?,
            )?;
            if membership != meta.last_membership {
                return Err(Status::data_loss(
                    "snapshot image membership differs from the snapshot meta",
                ));
            }
        }
        std::fs::remove_file(&probe).map_err(io)?;
        write_meta(&receive.dir, &self.identity, meta, &signature)?;
        let generation = next_generation(&self.snapshots)?;
        let target = self
            .snapshots
            .join(GENERATIONS)
            .join(generation.to_string());
        std::fs::rename(&receive.dir, &target).map_err(io)?;
        sync_dir(&self.snapshots.join(GENERATIONS))?;
        // Swap the live store for a fresh copy of the verified image; the
        // generation's image stays pristine. On refusal the old handle is
        // kept and the unpublished generation removed.
        let staged = self.snapshots.join(STAGED);
        std::fs::copy(target.join(IMAGE), &staged).map_err(io)?;
        std::fs::File::open(&staged)
            .and_then(|f| f.sync_all())
            .map_err(io)?;
        let swap = {
            let mut guard = self
                .store
                .write()
                .map_err(|_| Status::internal("state machine store lock poisoned"))?;
            let store = guard
                .take()
                .ok_or_else(|| Status::internal("state machine store is absent"))?;
            match store.replace_from(&staged) {
                Ok(replaced) => {
                    *guard = Some(replaced);
                    Ok(())
                }
                Err(ReplaceFailure::Refused(old, status)) => {
                    *guard = Some(old);
                    Err(status)
                }
                Err(ReplaceFailure::Lost(status)) => {
                    // The old handle is closed; reopen whatever is durable.
                    match SourceAuthorityStore::open(&self.live_path, &self.identity) {
                        Ok(reopened) => {
                            reopened.set_raft_hosted();
                            *guard = Some(reopened);
                            Err(status)
                        }
                        Err(reopen) => Err(Status::internal(format!(
                            "snapshot swap failed ({}) and the live store did not reopen ({})",
                            status.message(),
                            reopen.message()
                        ))),
                    }
                }
            }
        };
        if let Err(status) = swap {
            let _ = std::fs::remove_file(&staged);
            let _ = std::fs::remove_dir_all(&target);
            return Err(status);
        }
        publish(&self.snapshots, generation)?;
        *self.membership.lock().unwrap() = meta.last_membership.clone();
        Ok(())
    }
}

pub struct ControlSnapshotBuilder {
    store: SharedStore,
    identity: SourceAuthorityIdentity,
    snapshots: PathBuf,
    max_image_bytes: u64,
    snapshot_lock: Arc<Mutex<()>>,
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
        let max = self.max_image_bytes;
        let outcome = (|| -> Result<Snapshot<ControlRaft>, Status> {
            let applied = store.quiesced(|path, applied| {
                let length = std::fs::metadata(path).map_err(io)?.len();
                if length > max {
                    return Err(Status::resource_exhausted(format!(
                        "store image of {length} bytes exceeds the {max} byte snapshot bound"
                    )));
                }
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
            let signature = image_signature(&image, max)?;
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
            publish(&self.snapshots, generation)?;
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

/// Publish one generation through the pointer, then drop every other
/// generation. The previous generation stays usable until the pointer moved.
fn publish(snapshots: &Path, generation: u64) -> Result<(), Status> {
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

fn write_meta(
    dir: &Path,
    group: &SourceAuthorityIdentity,
    meta: &SnapshotMeta<NodeId, BasicNode>,
    signature: &ImageSignature,
) -> Result<(), Status> {
    let value = RaftSnapshotMeta {
        format_version: 1,
        group: Some(group.clone()),
        last_log_id: meta.last_log_id.as_ref().map(log_id_to_proto),
        membership: Some(stored_membership_to_proto(&meta.last_membership)),
        snapshot_id: meta.snapshot_id.clone(),
        length: signature.length,
        sha256: signature.sha256.to_vec(),
    };
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
    verify_image(&dir.join(IMAGE), &signature, u64::MAX)?;
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

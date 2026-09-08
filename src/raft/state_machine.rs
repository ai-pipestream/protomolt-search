//! The state machine: the source authority store applied through its
//! committed-replay paths, with the applied position written in the same
//! transaction as each command. Snapshots are consistent copies of the store
//! file taken while every store transaction is held off; install verifies
//! group, length, digest and applied position before the swap.
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
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use tokio::io::AsyncWriteExt;
use tonic::{Code, Status};

const CURRENT_SNAPSHOT: &str = "current.redb";
const CURRENT_META: &str = "current.meta";
const BUILDING_SNAPSHOT: &str = "building.redb";

/// The store behind the state machine. `None` only for the instant of a
/// snapshot install, when the old file is closed and the new one opened.
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

pub struct ControlStateMachine {
    store: SharedStore,
    snapshots: PathBuf,
    membership: Mutex<StoredMembership<NodeId, BasicNode>>,
}

impl ControlStateMachine {
    /// Wrap an opened store. The store is marked Raft-hosted: its direct
    /// command paths refuse from here on.
    pub fn new(store: SourceAuthorityStore, snapshots: &Path) -> Result<Self, Status> {
        store.set_raft_hosted();
        std::fs::create_dir_all(snapshots)
            .map_err(|e| Status::internal(format!("raft snapshot directory: {e}")))?;
        let membership = match store.raft_applied()? {
            Some(applied) => stored_membership_from_proto(
                applied
                    .membership
                    .as_ref()
                    .ok_or_else(|| Status::data_loss("raft applied position has no membership"))?,
            )?,
            None => StoredMembership::new(None, Membership::new(Vec::new(), BTreeMap::new())),
        };
        Ok(Self {
            store: Arc::new(RwLock::new(Some(store))),
            snapshots: snapshots.to_path_buf(),
            membership: Mutex::new(membership),
        })
    }

    pub fn shared_store(&self) -> SharedStore {
        Arc::clone(&self.store)
    }

    fn store(&self) -> Result<SourceAuthorityStore, Status> {
        self.store
            .read()
            .map_err(|_| Status::internal("state machine store lock poisoned"))?
            .clone()
            .ok_or_else(|| Status::unavailable("state machine store is being replaced"))
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
            *self.membership.lock().unwrap() = membership;
            replies.push(reply);
        }
        Ok(replies)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        ControlSnapshotBuilder {
            store: Arc::clone(&self.store),
            snapshots: self.snapshots.clone(),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<tokio::fs::File>, StorageError<NodeId>> {
        let path = self.snapshots.join(format!(
            "incoming-{}.redb",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        let file = tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .await
            .map_err(|e| snapshot_error(ErrorVerb::Write, e))?;
        // The path travels with the meta on install: openraft hands back the
        // same File, whose path we recover through /proc-free means by
        // recording it beside the directory listing.
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
        // The received file is the newest incoming-*.redb; there is exactly
        // one receive in flight per node.
        let incoming =
            newest_incoming(&self.snapshots).map_err(|e| snapshot_error(ErrorVerb::Read, e))?;
        let signature =
            parse_snapshot_id(&meta.snapshot_id).map_err(|e| snapshot_error(ErrorVerb::Read, e))?;
        verify_image(&incoming, &signature).map_err(|e| snapshot_error(ErrorVerb::Read, e))?;
        let (identity, path) = {
            let store = self
                .store()
                .map_err(|e| snapshot_error(ErrorVerb::Read, e))?;
            (store.identity().clone(), store.path().to_path_buf())
        };
        // Keep the verified bytes as the current snapshot before anything
        // opens the image: a redb open may rewrite recovery state in place,
        // and the kept snapshot must stay byte-identical to its signature.
        let current = self.snapshots.join(CURRENT_SNAPSHOT);
        std::fs::copy(&incoming, &current).map_err(|e| snapshot_error(ErrorVerb::Write, e))?;
        write_current_meta(&self.snapshots, &identity, meta, &signature)
            .map_err(|e| snapshot_error(ErrorVerb::Write, e))?;
        let staged = self.snapshots.join("staged.redb");
        std::fs::rename(&incoming, &staged).map_err(|e| snapshot_error(ErrorVerb::Write, e))?;
        // Verify the image as a store of this group with the expected applied
        // position before it replaces anything.
        {
            let probe = SourceAuthorityStore::open(&staged, &identity).map_err(|e| {
                snapshot_error(ErrorVerb::Read, format!("snapshot image: {}", e.message()))
            })?;
            let applied = probe
                .raft_applied()
                .map_err(|e| snapshot_error(ErrorVerb::Read, e))?
                .ok_or_else(|| {
                    snapshot_error(
                        ErrorVerb::Read,
                        "snapshot image carries no applied position",
                    )
                })?;
            if applied.last_applied.as_ref().map(log_id_from_proto) != meta.last_log_id {
                return Err(snapshot_error(
                    ErrorVerb::Read,
                    "snapshot image applied position differs from the snapshot meta",
                ));
            }
        }
        // Swap the live store for the verified image.
        {
            let mut guard = self.store.write().map_err(|_| {
                snapshot_error(ErrorVerb::Write, "state machine store lock poisoned")
            })?;
            let store = guard
                .take()
                .ok_or_else(|| snapshot_error(ErrorVerb::Write, "state machine store is absent"))?;
            let replaced = store
                .replace_from(&staged)
                .map_err(|e| snapshot_error(ErrorVerb::Write, e))?;
            debug_assert_eq!(replaced.path(), path.as_path());
            *guard = Some(replaced);
        }
        *self.membership.lock().unwrap() = meta.last_membership.clone();
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<ControlRaft>>, StorageError<NodeId>> {
        let meta_path = self.snapshots.join(CURRENT_META);
        if !meta_path.exists() {
            return Ok(None);
        }
        let meta =
            read_current_meta(&self.snapshots).map_err(|e| snapshot_error(ErrorVerb::Read, e))?;
        let file = tokio::fs::File::open(self.snapshots.join(CURRENT_SNAPSHOT))
            .await
            .map_err(|e| snapshot_error(ErrorVerb::Read, e))?;
        Ok(Some(Snapshot {
            meta,
            snapshot: Box::new(file),
        }))
    }
}

pub struct ControlSnapshotBuilder {
    store: SharedStore,
    snapshots: PathBuf,
}

impl RaftSnapshotBuilder<ControlRaft> for ControlSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<ControlRaft>, StorageError<NodeId>> {
        let store = self
            .store
            .read()
            .map_err(|_| snapshot_error(ErrorVerb::Read, "state machine store lock poisoned"))?
            .clone()
            .ok_or_else(|| {
                snapshot_error(ErrorVerb::Read, "state machine store is being replaced")
            })?;
        let building = self.snapshots.join(BUILDING_SNAPSHOT);
        let applied = store
            .quiesced(|path, applied| {
                std::fs::copy(path, &building)
                    .map_err(|e| Status::internal(format!("snapshot copy: {e}")))?;
                let file = std::fs::File::open(&building)
                    .map_err(|e| Status::internal(format!("snapshot open: {e}")))?;
                file.sync_all()
                    .map_err(|e| Status::internal(format!("snapshot sync: {e}")))?;
                Ok(applied)
            })
            .map_err(|e| snapshot_error(ErrorVerb::Read, e))?;
        let Some(applied) = applied else {
            return Err(snapshot_error(
                ErrorVerb::Read,
                "no applied position; nothing to snapshot",
            ));
        };
        let last_log_id = applied.last_applied.as_ref().map(log_id_from_proto);
        let membership =
            stored_membership_from_proto(applied.membership.as_ref().ok_or_else(|| {
                snapshot_error(ErrorVerb::Read, "applied position has no membership")
            })?)
            .map_err(|e| snapshot_error(ErrorVerb::Read, e))?;
        let signature =
            image_signature(&building).map_err(|e| snapshot_error(ErrorVerb::Read, e))?;
        let meta = SnapshotMeta {
            last_log_id,
            last_membership: membership,
            snapshot_id: format_snapshot_id(last_log_id.as_ref(), &signature),
        };
        let current = self.snapshots.join(CURRENT_SNAPSHOT);
        std::fs::rename(&building, &current).map_err(|e| snapshot_error(ErrorVerb::Write, e))?;
        write_current_meta(&self.snapshots, store.identity(), &meta, &signature)
            .map_err(|e| snapshot_error(ErrorVerb::Write, e))?;
        let file = tokio::fs::File::open(&current)
            .await
            .map_err(|e| snapshot_error(ErrorVerb::Read, e))?;
        Ok(Snapshot {
            meta,
            snapshot: Box::new(file),
        })
    }
}

/// Length and SHA-256 of an image, bound into the snapshot id and meta.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageSignature {
    pub length: u64,
    pub sha256: [u8; 32],
}

fn image_signature(path: &Path) -> Result<ImageSignature, Status> {
    let bytes = std::fs::read(path).map_err(|e| Status::internal(format!("snapshot read: {e}")))?;
    Ok(ImageSignature {
        length: bytes.len() as u64,
        sha256: crate::sha256::digest(&bytes),
    })
}

fn verify_image(path: &Path, expected: &ImageSignature) -> Result<(), Status> {
    let actual = image_signature(path)?;
    if actual != *expected {
        return Err(Status::data_loss(format!(
            "snapshot image length {} and digest differ from the announced {}",
            actual.length, expected.length
        )));
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

fn write_current_meta(
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
    let tmp = dir.join("current.meta.tmp");
    std::fs::write(&tmp, value.encode_to_vec())
        .map_err(|e| Status::internal(format!("snapshot meta: {e}")))?;
    std::fs::File::open(&tmp)
        .and_then(|f| f.sync_all())
        .map_err(|e| Status::internal(format!("snapshot meta sync: {e}")))?;
    std::fs::rename(&tmp, dir.join(CURRENT_META))
        .map_err(|e| Status::internal(format!("snapshot meta: {e}")))?;
    std::fs::File::open(dir)
        .and_then(|d| d.sync_all())
        .map_err(|e| Status::internal(format!("snapshot directory sync: {e}")))?;
    Ok(())
}

fn read_current_meta(dir: &Path) -> Result<SnapshotMeta<NodeId, BasicNode>, Status> {
    let bytes = std::fs::read(dir.join(CURRENT_META))
        .map_err(|e| Status::internal(format!("snapshot meta: {e}")))?;
    let value = RaftSnapshotMeta::decode(bytes.as_slice())
        .map_err(|e| Status::data_loss(format!("snapshot meta: {e}")))?;
    if value.format_version != 1 || value.sha256.len() != 32 {
        return Err(Status::data_loss(
            "snapshot meta format or digest is invalid",
        ));
    }
    let signature = ImageSignature {
        length: value.length,
        sha256: value.sha256.as_slice().try_into().expect("checked length"),
    };
    verify_image(&dir.join(CURRENT_SNAPSHOT), &signature)?;
    let last_log_id = value.last_log_id.as_ref().map(log_id_from_proto);
    if value.snapshot_id != format_snapshot_id(last_log_id.as_ref(), &signature) {
        return Err(Status::data_loss("snapshot meta id differs from its image"));
    }
    Ok(SnapshotMeta {
        last_log_id,
        last_membership: stored_membership_from_proto(
            value
                .membership
                .as_ref()
                .ok_or_else(|| Status::data_loss("snapshot meta has no membership"))?,
        )?,
        snapshot_id: value.snapshot_id,
    })
}

fn newest_incoming(dir: &Path) -> Result<PathBuf, Status> {
    let mut newest: Option<PathBuf> = None;
    for entry in
        std::fs::read_dir(dir).map_err(|e| Status::internal(format!("snapshot directory: {e}")))?
    {
        let entry = entry.map_err(|e| Status::internal(format!("snapshot directory: {e}")))?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("incoming-") && name.ends_with(".redb") {
            let path = entry.path();
            if newest
                .as_ref()
                .is_none_or(|n| n.file_name() < path.file_name())
            {
                newest = Some(path);
            }
        }
    }
    newest.ok_or_else(|| Status::failed_precondition("no received snapshot image to install"))
}

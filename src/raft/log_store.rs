//! The Raft log store: a dedicated redb database beside the source authority
//! store, holding the group header (identity, node id, vote, committed and
//! purged positions) and the consecutive log entries as typed envelopes.
//! Every write is one immediate transaction; the flush callback fires only
//! after the commit. Truncate and purge are protocol boundaries, never file
//! cleanup guessed from an index.
use super::types::{
    entry_from_proto, entry_to_proto, log_id_from_proto, log_id_to_proto, vote_from_proto,
    vote_to_proto, ControlRaft, NodeId,
};
use crate::pb::storage::{RaftEntry, RaftLogHeader, SourceAuthorityIdentity};
use openraft::storage::{LogFlushed, LogState, RaftLogStorage};
use openraft::{
    AnyError, Entry, ErrorSubject, ErrorVerb, LogId, RaftLogReader, StorageError, StorageIOError,
    Vote,
};
use prost::Message;
use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition};
use std::fs::{File, OpenOptions};
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tonic::Status;

const LOG_META: TableDefinition<&str, &[u8]> = TableDefinition::new("raft_log_meta");
const LOG: TableDefinition<u64, &[u8]> = TableDefinition::new("raft_log_entries");
const HEADER: &str = "header";
/// One entry may not exceed the command bound plus its envelope.
const MAX_ENTRY_BYTES: usize = 2 * 1024 * 1024;

struct Inner {
    database: Database,
    group: SourceAuthorityIdentity,
    node_id: NodeId,
    // Serializes every write: votes and entries share one order.
    write: Mutex<()>,
    _file_lock: File,
}

/// Cloneable handle; clones share the store and the write order.
#[derive(Clone)]
pub struct RaftLogStore {
    inner: Arc<Inner>,
}

fn io<E: std::fmt::Display>(
    subject: ErrorSubject<NodeId>,
    verb: ErrorVerb,
    error: E,
) -> StorageError<NodeId> {
    StorageIOError::new(subject, verb, AnyError::error(error.to_string())).into()
}

fn storage_status(error: impl std::fmt::Display) -> Status {
    Status::internal(format!("raft log storage: {error}"))
}

impl RaftLogStore {
    /// Create the log store for one group and node. Refuses an existing file.
    pub fn create(
        path: &Path,
        group: &SourceAuthorityIdentity,
        node_id: NodeId,
    ) -> Result<Self, Status> {
        crate::source_owner::identity(group)?;
        let store = Self::open_file(path, group, node_id, true)?;
        let mut tx = store.inner.database.begin_write().map_err(storage_status)?;
        tx.set_durability(Durability::Immediate)
            .map_err(storage_status)?;
        {
            let mut meta = tx.open_table(LOG_META).map_err(storage_status)?;
            let header = RaftLogHeader {
                format_version: 1,
                group: Some(group.clone()),
                node_id,
                vote: None,
                committed: None,
                last_purged: None,
            };
            meta.insert(HEADER, header.encode_to_vec().as_slice())
                .map_err(storage_status)?;
            tx.open_table(LOG).map_err(storage_status)?;
        }
        tx.commit().map_err(storage_status)?;
        Ok(store)
    }

    /// Open an existing log store; the recorded group and node id must equal
    /// the expected ones. A missing or empty file never becomes a new log.
    pub fn open(
        path: &Path,
        group: &SourceAuthorityIdentity,
        node_id: NodeId,
    ) -> Result<Self, Status> {
        crate::source_owner::identity(group)?;
        let store = Self::open_file(path, group, node_id, false)?;
        let header = store.header()?;
        if header.format_version != 1
            || header.group.as_ref() != Some(group)
            || header.node_id != node_id
        {
            return Err(Status::failed_precondition(
                "raft log store belongs to another group, node or format",
            ));
        }
        Ok(store)
    }

    fn open_file(
        path: &Path,
        group: &SourceAuthorityIdentity,
        node_id: NodeId,
        create: bool,
    ) -> Result<Self, Status> {
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(create);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(path).map_err(|error| match error.kind() {
            std::io::ErrorKind::AlreadyExists => {
                Status::already_exists("raft log store already exists")
            }
            std::io::ErrorKind::NotFound => {
                Status::not_found("raft log store is missing; startup never creates a new log")
            }
            _ => storage_status(error),
        })?;
        file.try_lock().map_err(|error| {
            Status::failed_precondition(format!("raft log store exclusive lock: {error}"))
        })?;
        if !create && file.metadata().map_err(storage_status)?.len() == 0 {
            return Err(Status::data_loss("raft log store file is empty"));
        }
        let mut builder = Database::builder();
        builder.set_cache_size(8 * 1024 * 1024);
        let database = builder
            .create_file(file.try_clone().map_err(storage_status)?)
            .map_err(storage_status)?;
        if create {
            let parent = path
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new("."));
            File::open(parent)
                .and_then(|d| d.sync_all())
                .map_err(storage_status)?;
        }
        Ok(Self {
            inner: Arc::new(Inner {
                database,
                group: group.clone(),
                node_id,
                write: Mutex::new(()),
                _file_lock: file,
            }),
        })
    }

    pub fn node_id(&self) -> NodeId {
        self.inner.node_id
    }

    pub fn group(&self) -> &SourceAuthorityIdentity {
        &self.inner.group
    }

    fn header(&self) -> Result<RaftLogHeader, Status> {
        let tx = self.inner.database.begin_read().map_err(storage_status)?;
        let meta = tx.open_table(LOG_META).map_err(storage_status)?;
        let bytes = meta
            .get(HEADER)
            .map_err(storage_status)?
            .ok_or_else(|| Status::data_loss("raft log header is missing"))?;
        let header = RaftLogHeader::decode(bytes.value())
            .map_err(|error| Status::data_loss(format!("raft log header: {error}")))?;
        if header.encode_to_vec() != bytes.value() {
            return Err(Status::data_loss(
                "raft log header has unknown or noncanonical fields",
            ));
        }
        Ok(header)
    }

    fn update_header(
        &self,
        verb: ErrorVerb,
        update: impl FnOnce(&mut RaftLogHeader),
    ) -> Result<(), StorageError<NodeId>> {
        let _order = self.inner.write.lock().unwrap();
        let mut header = self.header().map_err(|e| io(ErrorSubject::Vote, verb, e))?;
        update(&mut header);
        let mut tx = self
            .inner
            .database
            .begin_write()
            .map_err(|e| io(ErrorSubject::Vote, verb, e))?;
        tx.set_durability(Durability::Immediate)
            .map_err(|e| io(ErrorSubject::Vote, verb, e))?;
        {
            let mut meta = tx
                .open_table(LOG_META)
                .map_err(|e| io(ErrorSubject::Vote, verb, e))?;
            meta.insert(HEADER, header.encode_to_vec().as_slice())
                .map_err(|e| io(ErrorSubject::Vote, verb, e))?;
        }
        tx.commit().map_err(|e| io(ErrorSubject::Vote, verb, e))
    }

    fn last_log_id(&self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        let tx = self
            .inner
            .database
            .begin_read()
            .map_err(|e| io(ErrorSubject::Logs, ErrorVerb::Read, e))?;
        let log = tx
            .open_table(LOG)
            .map_err(|e| io(ErrorSubject::Logs, ErrorVerb::Read, e))?;
        let Some(last) = log
            .last()
            .map_err(|e| io(ErrorSubject::Logs, ErrorVerb::Read, e))?
        else {
            return Ok(None);
        };
        let entry = RaftEntry::decode(last.1.value())
            .map_err(|e| io(ErrorSubject::Logs, ErrorVerb::Read, e))?;
        Ok(entry.log_id.as_ref().map(log_id_from_proto))
    }

    /// Every stored entry in index order, for tests and audits.
    pub fn entries(&self) -> Result<Vec<Entry<ControlRaft>>, Status> {
        let tx = self.inner.database.begin_read().map_err(storage_status)?;
        let log = tx.open_table(LOG).map_err(storage_status)?;
        let mut out = Vec::new();
        for item in log.iter().map_err(storage_status)? {
            let (_, value) = item.map_err(storage_status)?;
            let entry = RaftEntry::decode(value.value())
                .map_err(|e| Status::data_loss(format!("raft entry: {e}")))?;
            out.push(entry_from_proto(entry)?);
        }
        Ok(out)
    }
}

impl RaftLogReader<ControlRaft> for RaftLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + std::fmt::Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<ControlRaft>>, StorageError<NodeId>> {
        let tx = self
            .inner
            .database
            .begin_read()
            .map_err(|e| io(ErrorSubject::Logs, ErrorVerb::Read, e))?;
        let log = tx
            .open_table(LOG)
            .map_err(|e| io(ErrorSubject::Logs, ErrorVerb::Read, e))?;
        let mut out = Vec::new();
        for item in log
            .range(range)
            .map_err(|e| io(ErrorSubject::Logs, ErrorVerb::Read, e))?
        {
            let (index, value) = item.map_err(|e| io(ErrorSubject::Logs, ErrorVerb::Read, e))?;
            let entry = RaftEntry::decode(value.value())
                .map_err(|e| io(ErrorSubject::LogIndex(index.value()), ErrorVerb::Read, e))?;
            let entry = entry_from_proto(entry).map_err(|e| {
                io(
                    ErrorSubject::LogIndex(index.value()),
                    ErrorVerb::Read,
                    e.message(),
                )
            })?;
            if entry.log_id.index != index.value() {
                return Err(io(
                    ErrorSubject::LogIndex(index.value()),
                    ErrorVerb::Read,
                    "stored entry index differs from its key",
                ));
            }
            out.push(entry);
        }
        Ok(out)
    }
}

impl RaftLogStorage<ControlRaft> for RaftLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<ControlRaft>, StorageError<NodeId>> {
        let header = self
            .header()
            .map_err(|e| io(ErrorSubject::Logs, ErrorVerb::Read, e))?;
        let last_purged_log_id = header.last_purged.as_ref().map(log_id_from_proto);
        let last_log_id = self.last_log_id()?.or(last_purged_log_id);
        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        let vote = vote_to_proto(vote);
        self.update_header(ErrorVerb::Write, |header| header.vote = Some(vote))
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        let header = self
            .header()
            .map_err(|e| io(ErrorSubject::Vote, ErrorVerb::Read, e))?;
        Ok(header.vote.as_ref().map(vote_from_proto))
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        let committed = committed.as_ref().map(log_id_to_proto);
        self.update_header(ErrorVerb::Write, |header| header.committed = committed)
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        let header = self
            .header()
            .map_err(|e| io(ErrorSubject::Logs, ErrorVerb::Read, e))?;
        Ok(header.committed.as_ref().map(log_id_from_proto))
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<ControlRaft>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<ControlRaft>> + Send,
        I::IntoIter: Send,
    {
        let result = self.append_sync(entries);
        match result {
            Ok(()) => {
                callback.log_io_completed(Ok(()));
                Ok(())
            }
            Err(error) => {
                callback.log_io_completed(Err(std::io::Error::other(error.to_string())));
                Err(error)
            }
        }
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.truncate_sync(log_id)
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.purge_sync(log_id)
    }
}

impl RaftLogStore {
    /// Append consecutive entries in one immediate transaction; the entries
    /// are durable when this returns.
    pub fn append_sync<I>(&self, entries: I) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<ControlRaft>>,
    {
        {
            let _order = self.inner.write.lock().unwrap();
            let mut expected = self.last_log_id()?.map(|id| id.index + 1).or_else(|| {
                self.header()
                    .ok()
                    .and_then(|h| h.last_purged.map(|p| p.index + 1))
            });
            let mut tx = self
                .inner
                .database
                .begin_write()
                .map_err(|e| io(ErrorSubject::Logs, ErrorVerb::Write, e))?;
            tx.set_durability(Durability::Immediate)
                .map_err(|e| io(ErrorSubject::Logs, ErrorVerb::Write, e))?;
            {
                let mut log = tx
                    .open_table(LOG)
                    .map_err(|e| io(ErrorSubject::Logs, ErrorVerb::Write, e))?;
                for entry in entries {
                    let index = entry.log_id.index;
                    if expected.is_some_and(|next| next != index) {
                        return Err(io(
                            ErrorSubject::LogIndex(index),
                            ErrorVerb::Write,
                            format!(
                                "appending index {index} would leave a hole; the next index is {}",
                                expected.unwrap_or(0)
                            ),
                        ));
                    }
                    let bytes = entry_to_proto(&entry).encode_to_vec();
                    if bytes.len() > MAX_ENTRY_BYTES {
                        return Err(io(
                            ErrorSubject::LogIndex(index),
                            ErrorVerb::Write,
                            "raft entry exceeds the 2 MiB bound",
                        ));
                    }
                    log.insert(index, bytes.as_slice())
                        .map_err(|e| io(ErrorSubject::LogIndex(index), ErrorVerb::Write, e))?;
                    expected = Some(index + 1);
                }
            }
            tx.commit()
                .map_err(|e| io(ErrorSubject::Logs, ErrorVerb::Write, e))?;
        }
        Ok(())
    }

    /// Delete every entry at or after `log_id`.
    pub fn truncate_sync(&self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let _order = self.inner.write.lock().unwrap();
        let mut tx = self
            .inner
            .database
            .begin_write()
            .map_err(|e| io(ErrorSubject::Logs, ErrorVerb::Delete, e))?;
        tx.set_durability(Durability::Immediate)
            .map_err(|e| io(ErrorSubject::Logs, ErrorVerb::Delete, e))?;
        {
            let mut log = tx
                .open_table(LOG)
                .map_err(|e| io(ErrorSubject::Logs, ErrorVerb::Delete, e))?;
            log.retain(|index, _| index < log_id.index)
                .map_err(|e| io(ErrorSubject::Logs, ErrorVerb::Delete, e))?;
        }
        tx.commit()
            .map_err(|e| io(ErrorSubject::Logs, ErrorVerb::Delete, e))
    }

    /// Delete every entry at or before `log_id` and record it as purged.
    pub fn purge_sync(&self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        {
            let _order = self.inner.write.lock().unwrap();
            let mut tx = self
                .inner
                .database
                .begin_write()
                .map_err(|e| io(ErrorSubject::Logs, ErrorVerb::Delete, e))?;
            tx.set_durability(Durability::Immediate)
                .map_err(|e| io(ErrorSubject::Logs, ErrorVerb::Delete, e))?;
            {
                let mut log = tx
                    .open_table(LOG)
                    .map_err(|e| io(ErrorSubject::Logs, ErrorVerb::Delete, e))?;
                log.retain(|index, _| index > log_id.index)
                    .map_err(|e| io(ErrorSubject::Logs, ErrorVerb::Delete, e))?;
                let mut meta = tx
                    .open_table(LOG_META)
                    .map_err(|e| io(ErrorSubject::Logs, ErrorVerb::Delete, e))?;
                let mut header = {
                    let bytes = meta
                        .get(HEADER)
                        .map_err(|e| io(ErrorSubject::Logs, ErrorVerb::Delete, e))?
                        .ok_or_else(|| {
                            io(
                                ErrorSubject::Logs,
                                ErrorVerb::Delete,
                                "raft log header is missing",
                            )
                        })?;
                    RaftLogHeader::decode(bytes.value())
                        .map_err(|e| io(ErrorSubject::Logs, ErrorVerb::Delete, e))?
                };
                header.last_purged = Some(log_id_to_proto(&log_id));
                meta.insert(HEADER, header.encode_to_vec().as_slice())
                    .map_err(|e| io(ErrorSubject::Logs, ErrorVerb::Delete, e))?;
            }
            tx.commit()
                .map_err(|e| io(ErrorSubject::Logs, ErrorVerb::Delete, e))?;
        }
        Ok(())
    }
}

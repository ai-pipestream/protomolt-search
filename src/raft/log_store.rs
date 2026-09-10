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
/// The library's purge position a bounded purge has not reached yet,
/// as a `RaftLogId`; absent when no purge is deferred.
const DEFERRED_PURGE: &str = "purge_deferred";
/// One entry may not exceed the command bound plus its envelope.
const MAX_ENTRY_BYTES: usize = 2 * 1024 * 1024;

struct Inner {
    database: Database,
    group: SourceAuthorityIdentity,
    node_id: NodeId,
    // Serializes every write: votes and entries share one order.
    write: Mutex<()>,
    /// The highest index the library asked to purge to in this run. A
    /// purge deferred by the store's position is completed at an append
    /// only up to this: the library records a purge as done before it asks
    /// for it, so a completion up to here removes entries it has given up,
    /// and a completion past here would remove entries it still reads
    /// (across a restart it reads the log state afresh, so a record from
    /// an earlier run completes at start or when it is asked again).
    requested_purge: Mutex<Option<u64>>,
    _file_lock: File,
}

/// Where a settle runs: at start, before the library reads the log state,
/// or at an append, in a run whose purge point the library holds.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Settle {
    Start,
    Append,
}

/// Cloneable handle; clones share the store and the write order.
#[derive(Clone)]
pub struct RaftLogStore {
    inner: Arc<Inner>,
    // The state machine's store, once hosted: a purge never passes its
    // applied position, so a snapshot install the state machine refuses
    // leaves the log consistent with the store it kept (the library purges
    // for an incoming snapshot before the state machine has installed it).
    floor: Option<super::state_machine::SharedStore>,
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
    pub(crate) fn create(
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
    pub(crate) fn open(
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
        #[cfg(any(test, feature = "fork-guard"))]
        let _handoff = crate::test_support::lock_handoff();
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
            floor: None,
            inner: Arc::new(Inner {
                database,
                group: group.clone(),
                node_id,
                write: Mutex::new(()),
                requested_purge: Mutex::new(None),
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
        // The library purges for an incoming snapshot before the state
        // machine has installed it; a purge never passes what the store has
        // applied, so a refused install leaves the log consistent with the
        // store it kept. The rest is recorded as deferred and purged when
        // the store catches up (`settle_locked`: at the next append in this
        // run, and at start). The floor is read under the write order, so
        // the position it cuts at is the one the purge is written against.
        let _order = self.inner.write.lock().unwrap();
        let upto = match self.applied_floor()? {
            Floor::Unbound => Some(log_id),
            Floor::Applied(applied) if applied.index >= log_id.index => Some(log_id),
            Floor::Applied(applied) => Some(applied),
            Floor::Nothing => None,
        };
        // The purge point moves forward only. The library asks in order and
        // the store's position is at or past the purge point (the log is
        // purged up to what the store applied, no further), so a target
        // below it is a state this log cannot explain.
        if let (Some(upto), Some(purged)) = (upto, self.purged_index()?) {
            if upto.index < purged {
                return Err(io(
                    ErrorSubject::Logs,
                    ErrorVerb::Delete,
                    format!(
                        "raft log purge to index {} is below the purge point {purged} (the library asked {}, the store's position bounds it)",
                        upto.index, log_id.index
                    ),
                ));
            }
        }
        {
            let mut requested = self.inner.requested_purge.lock().unwrap();
            *requested = Some(requested.map_or(log_id.index, |r| r.max(log_id.index)));
        }
        let recorded = self.deferred_purge()?;
        let deferred = match upto {
            Some(upto) if upto.index >= log_id.index => recorded.filter(|d| d.index > upto.index),
            _ => Some(
                recorded
                    .filter(|d| d.index > log_id.index)
                    .unwrap_or(log_id),
            ),
        };
        match upto {
            Some(upto) => self.purge_locked(upto, deferred),
            None => self.record_deferred_purge_locked(deferred),
        }
    }
}

/// What the hosted store has applied, as a bound on purges.
enum Floor {
    /// No store is bound (the log alone, in tests).
    Unbound,
    /// The store is between swaps or has applied nothing.
    Nothing,
    Applied(LogId<NodeId>),
}

impl RaftLogStore {
    /// Bind the hosted store whose applied position bounds every purge,
    /// and complete a purge the store has since caught up with: a deferred
    /// purge is completed to its position once the store is at or past it
    /// and as far as the store's position until then, and an empty log
    /// behind the store's position is purged to it. This runs before the
    /// library reads the log state (`RaftHost::start_with` binds before
    /// `Raft::new`), which is what makes a purge here one the library
    /// never has to know about; moving the bind after the start would
    /// change that. A log with entries after a gap is data loss and
    /// refuses.
    pub(crate) fn bind_applied_floor(
        &mut self,
        store: super::state_machine::SharedStore,
    ) -> Result<(), StorageError<NodeId>> {
        self.floor = Some(store);
        let _order = self.inner.write.lock().unwrap();
        self.settle_locked(Settle::Start)
    }

    /// The purge point, as an index.
    fn purged_index(&self) -> Result<Option<u64>, StorageError<NodeId>> {
        Ok(self
            .header()
            .map_err(|e| io(ErrorSubject::Logs, ErrorVerb::Read, e))?
            .last_purged
            .map(|p| p.index))
    }

    fn applied_floor(&self) -> Result<Floor, StorageError<NodeId>> {
        let Some(floor) = &self.floor else {
            return Ok(Floor::Unbound);
        };
        let store = floor
            .read()
            .map_err(|_| {
                io(
                    ErrorSubject::Logs,
                    ErrorVerb::Delete,
                    "host store lock poisoned",
                )
            })?
            .clone();
        let Some(store) = store else {
            return Ok(Floor::Nothing);
        };
        Ok(store
            .raft_applied()
            .map_err(|e| io(ErrorSubject::Logs, ErrorVerb::Delete, e))?
            .and_then(|a| a.last_applied)
            .map_or(Floor::Nothing, |id| Floor::Applied(log_id_from_proto(&id))))
    }

    /// Under the write order: when the store has applied past the purged
    /// position, complete a purge bounded earlier. A deferred purge is
    /// completed to its own position once the store is at or past it; the
    /// entries it removes are behind what the store applied. With no purge
    /// deferred, an empty log behind the store's position is purged to it.
    /// At start (before the library reads the log state) a pending record
    /// is also carried as far as the store's position. At an append, a
    /// purge here goes only as far as the library asked in this run
    /// (`requested_purge`): the library holds its own purge point and
    /// reads entries above it, so a purge it did not ask for would remove
    /// entries it still sends, and it guards that by assertion only. A
    /// record from an earlier run that the store reaches in this one waits
    /// for the next purge the library asks or the next start; the log
    /// keeps entries meanwhile, contiguous from its purge point. Entries
    /// contiguous from the purged position are kept in every case (the
    /// library keeps entries behind a snapshot on purpose); entries after
    /// a gap are a hole the log cannot explain, whatever is pending.
    fn settle_locked(&self, at: Settle) -> Result<(), StorageError<NodeId>> {
        let Floor::Applied(applied) = self.applied_floor()? else {
            return Ok(());
        };
        let mut purged = self.purged_index()?;
        let deferred = self.deferred_purge()?;
        let (target, keep) = match (at, deferred) {
            (_, Some(deferred)) if applied.index >= deferred.index => (Some(deferred), None),
            (Settle::Start, Some(deferred)) => (Some(applied), Some(deferred)),
            (Settle::Append, Some(deferred)) => (None, Some(deferred)),
            (_, None) if self.first_log_index()?.is_none() => (Some(applied), None),
            (_, None) => (None, None),
        };
        if let Some(target) = target {
            let asked = match at {
                Settle::Start => true,
                Settle::Append => self
                    .inner
                    .requested_purge
                    .lock()
                    .unwrap()
                    .is_some_and(|requested| requested >= target.index),
            };
            if purged.is_some_and(|p| p >= target.index) {
                // The purge point is at or past a record: the record is
                // done and stale (a purge clears what it completes, so this
                // is not reached in practice).
                if deferred.is_some() && keep.is_none() {
                    self.record_deferred_purge_locked(None)?;
                }
            } else if asked {
                self.purge_locked(target, keep)?;
                purged = Some(target.index);
            }
        }
        match self.first_log_index()? {
            None => Ok(()),
            // The log starts at index 0; entries are contiguous from the
            // purged position.
            Some(first) if first == purged.map_or(0, |p| p + 1) => Ok(()),
            Some(first) => Err(io(
                ErrorSubject::Logs,
                ErrorVerb::Read,
                format!(
                    "raft log holds entries from index {first} after a purge at {}; the store applied {}",
                    purged.unwrap_or(0),
                    applied.index
                ),
            )),
        }
    }

    /// The purge position recorded as deferred, if any.
    pub(crate) fn deferred_purge(&self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        let tx = self
            .inner
            .database
            .begin_read()
            .map_err(|e| io(ErrorSubject::Logs, ErrorVerb::Read, e))?;
        let meta = tx
            .open_table(LOG_META)
            .map_err(|e| io(ErrorSubject::Logs, ErrorVerb::Read, e))?;
        let Some(bytes) = meta
            .get(DEFERRED_PURGE)
            .map_err(|e| io(ErrorSubject::Logs, ErrorVerb::Read, e))?
        else {
            return Ok(None);
        };
        let value = crate::pb::storage::RaftLogId::decode(bytes.value())
            .map_err(|e| io(ErrorSubject::Logs, ErrorVerb::Read, e))?;
        if value.encode_to_vec() != bytes.value() {
            return Err(io(
                ErrorSubject::Logs,
                ErrorVerb::Read,
                "raft log deferred purge record has unknown or noncanonical fields",
            ));
        }
        Ok(Some(log_id_from_proto(&value)))
    }

    /// Write or clear the deferred purge record in its own transaction.
    fn record_deferred_purge_locked(
        &self,
        deferred: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        let mut tx = self
            .inner
            .database
            .begin_write()
            .map_err(|e| io(ErrorSubject::Logs, ErrorVerb::Write, e))?;
        tx.set_durability(Durability::Immediate)
            .map_err(|e| io(ErrorSubject::Logs, ErrorVerb::Write, e))?;
        {
            let mut meta = tx
                .open_table(LOG_META)
                .map_err(|e| io(ErrorSubject::Logs, ErrorVerb::Write, e))?;
            Self::write_deferred(&mut meta, deferred, ErrorVerb::Write)?;
        }
        tx.commit()
            .map_err(|e| io(ErrorSubject::Logs, ErrorVerb::Write, e))
    }

    fn write_deferred(
        meta: &mut redb::Table<'_, &str, &[u8]>,
        deferred: Option<LogId<NodeId>>,
        verb: ErrorVerb,
    ) -> Result<(), StorageError<NodeId>> {
        match deferred {
            Some(deferred) => {
                let bytes = log_id_to_proto(&deferred).encode_to_vec();
                meta.insert(DEFERRED_PURGE, bytes.as_slice())
                    .map_err(|e| io(ErrorSubject::Logs, verb, e))?;
            }
            None => {
                meta.remove(DEFERRED_PURGE)
                    .map_err(|e| io(ErrorSubject::Logs, verb, e))?;
            }
        }
        Ok(())
    }

    fn first_log_index(&self) -> Result<Option<u64>, StorageError<NodeId>> {
        let tx = self
            .inner
            .database
            .begin_read()
            .map_err(|e| io(ErrorSubject::Logs, ErrorVerb::Read, e))?;
        let log = tx
            .open_table(LOG)
            .map_err(|e| io(ErrorSubject::Logs, ErrorVerb::Read, e))?;
        let first = log
            .first()
            .map_err(|e| io(ErrorSubject::Logs, ErrorVerb::Read, e))?
            .map(|(index, _)| index.value());
        Ok(first)
    }

    /// Append consecutive entries in one immediate transaction; the entries
    /// are durable when this returns.
    pub(crate) fn append_sync<I>(&self, entries: I) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<ControlRaft>>,
    {
        {
            let _order = self.inner.write.lock().unwrap();
            self.settle_locked(Settle::Append)?;
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
    pub(crate) fn truncate_sync(&self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let _order = self.inner.write.lock().unwrap();
        // The library truncates at or above its committed position, and a
        // purge is deferred at or below the snapshot it was asked for,
        // which is committed. A truncation reaching into a deferred purge
        // would leave a record naming entries the log no longer holds.
        if let Some(deferred) = self.deferred_purge()? {
            if log_id.index <= deferred.index {
                return Err(io(
                    ErrorSubject::Logs,
                    ErrorVerb::Delete,
                    format!(
                        "raft log truncate from index {} reaches into a purge deferred to {}",
                        log_id.index, deferred.index
                    ),
                ));
            }
        }
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

    /// Delete every entry at or before `log_id` and record it as purged,
    /// with no bound.
    #[cfg(test)]
    pub(crate) fn purge_sync(&self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let _order = self.inner.write.lock().unwrap();
        let deferred = self.deferred_purge()?.filter(|d| d.index > log_id.index);
        self.purge_locked(log_id, deferred)
    }

    /// Delete every entry at or before `log_id`, record it as purged and
    /// set or clear the deferred purge record, in one transaction.
    fn purge_locked(
        &self,
        log_id: LogId<NodeId>,
        deferred: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        {
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
                Self::write_deferred(&mut meta, deferred, ErrorVerb::Delete)?;
            }
            tx.commit()
                .map_err(|e| io(ErrorSubject::Logs, ErrorVerb::Delete, e))?;
        }
        Ok(())
    }
}

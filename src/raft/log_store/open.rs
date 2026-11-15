//! Creating and opening the log file, its header, and inspection.

use super::*;

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

    pub(super) fn open_file(
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
        let backend = redb::backends::FileBackend::new(file.try_clone().map_err(storage_status)?)
            .map_err(storage_status)?;
        let database = builder
            .create_with_backend(backend)
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
                #[cfg(any(test, feature = "fault-injection"))]
                fault: Mutex::new(None),
            }),
        })
    }

    /// Arm one process-exit fault for the next purge transaction of this
    /// log: the process exits with code 87 either before that transaction
    /// commits or right after it. A purge transaction is the library's
    /// purge, the record of a purge the store bounds, or the completion of
    /// a recorded purge at an append or at start.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn arm_purge_exit_fault(&self, fault: crate::source_authority::ExitFault) {
        *self.inner.fault.lock().unwrap() = Some(fault);
    }

    #[cfg(any(test, feature = "fault-injection"))]
    pub(super) fn inject(&self, after: bool) {
        use crate::source_authority::ExitFault;
        let fault = self.inner.fault.lock().unwrap();
        match (*fault, after) {
            (Some(ExitFault::BeforeCommit), false) | (Some(ExitFault::AfterCommit), true) => {
                std::process::exit(87)
            }
            _ => {}
        }
    }

    /// The purge state of this log.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inspection(&self) -> Result<RaftLogInspection, Status> {
        let _order = self.inner.write.lock().unwrap();
        let header = self.header()?;
        let deferred = self.deferred_purge().map_err(storage_status)?;
        let first = self.first_log_index().map_err(storage_status)?;
        let last = self.last_log_id().map_err(storage_status)?;
        Ok(RaftLogInspection {
            purged: header.last_purged.map(|p| p.index),
            deferred: deferred.map(|d| d.index),
            first,
            last: last.map(|l| l.index),
        })
    }

    /// Open the log at `path` on its own, read its purge state and close
    /// it: the on-disk state of a node that is not running.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inspect(
        path: &Path,
        group: &SourceAuthorityIdentity,
        node_id: NodeId,
    ) -> Result<RaftLogInspection, Status> {
        Self::open(path, group, node_id)?.inspection()
    }

    pub fn node_id(&self) -> NodeId {
        self.inner.node_id
    }

    pub fn group(&self) -> &SourceAuthorityIdentity {
        &self.inner.group
    }

    pub(super) fn header(&self) -> Result<RaftLogHeader, Status> {
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

    pub(super) fn update_header(
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

    pub(super) fn last_log_id(&self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
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

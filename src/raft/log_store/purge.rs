//! Appends, truncation, and purges held behind the applied floor.

use super::*;

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
        store: super::super::state_machine::SharedStore,
    ) -> Result<(), StorageError<NodeId>> {
        self.floor = Some(store);
        let _order = self.inner.write.lock().unwrap();
        self.settle_locked(Settle::Start)
    }

    /// The purge point, as an index.
    pub(super) fn purged_index(&self) -> Result<Option<u64>, StorageError<NodeId>> {
        Ok(self
            .header()
            .map_err(|e| io(ErrorSubject::Logs, ErrorVerb::Read, e))?
            .last_purged
            .map(|p| p.index))
    }

    pub(super) fn applied_floor(&self) -> Result<Floor, StorageError<NodeId>> {
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
    pub(super) fn settle_locked(&self, at: Settle) -> Result<(), StorageError<NodeId>> {
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
    pub(super) fn record_deferred_purge_locked(
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
        #[cfg(any(test, feature = "fault-injection"))]
        self.inject(false);
        tx.commit()
            .map_err(|e| io(ErrorSubject::Logs, ErrorVerb::Write, e))?;
        #[cfg(any(test, feature = "fault-injection"))]
        self.inject(true);
        Ok(())
    }

    pub(super) fn write_deferred(
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

    pub(super) fn first_log_index(&self) -> Result<Option<u64>, StorageError<NodeId>> {
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
    pub(super) fn purge_locked(
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
            #[cfg(any(test, feature = "fault-injection"))]
            self.inject(false);
            tx.commit()
                .map_err(|e| io(ErrorSubject::Logs, ErrorVerb::Delete, e))?;
            #[cfg(any(test, feature = "fault-injection"))]
            self.inject(true);
        }
        Ok(())
    }
}

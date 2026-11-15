//! The library's log reader and log storage interfaces.

use super::*;

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

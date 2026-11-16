//! Raft hosting: the applied position written with each committed entry,
//! and the holds a snapshot builder and installer take.
use super::*;

impl SourceAuthorityStore {
    /// Write the pending Raft applied position, if any, into the transaction
    /// that applies the command it belongs to.
    pub(super) fn write_pending_applied(
        &self,
        meta: &mut redb::Table<&'static str, &'static [u8]>,
    ) -> Result<(), Status> {
        match self.inner.raft_pending.lock().unwrap().as_ref() {
            Some(applied) => {
                Self::refuse_unseeded(meta)?;
                meta.insert(RAFT_META, applied.encode_to_vec().as_slice())
                    .map_err(storage)?;
                Ok(())
            }
            None if self.raft_hosted() => Err(Status::failed_precondition(
                "Raft-hosted store applied a command outside the state machine; no applied position is pending",
            )),
            None => Ok(()),
        }
    }

    /// A prepared member applies nothing before the group's snapshot has
    /// replaced its genesis image; replaying onto that image would diverge
    /// from the group silently.
    fn refuse_unseeded(meta: &redb::Table<&'static str, &'static [u8]>) -> Result<(), Status> {
        if meta.get(MEMBER_META).map_err(storage)?.is_some() {
            return Err(crate::raft::reasons::reason(
                Status::failed_precondition(
                    "prepared member store awaits the group's first snapshot; log entries do not apply to its genesis image",
                ),
                crate::raft::reasons::MEMBERSHIP_AWAITING_SNAPSHOT,
            ));
        }
        Ok(())
    }

    /// An exact retry changes no application state; it commits only when a
    /// Raft applied position was written for the consumed entry.
    pub(super) fn finish_retry(&self, tx: redb::WriteTransaction) -> Result<(), Status> {
        if self.inner.raft_pending.lock().unwrap().is_some() {
            tx.commit().map_err(storage)
        } else {
            tx.abort().map_err(storage)
        }
    }

    /// Mark the store as Raft-hosted; the direct command paths refuse from
    /// here on. The host owns proposal admission.
    #[cfg_attr(not(feature = "raft"), allow(dead_code))]
    pub(crate) fn set_raft_hosted(&self) {
        self.inner.hosted.store(true, Ordering::Release);
    }

    pub(crate) fn raft_hosted(&self) -> bool {
        self.inner.hosted.load(Ordering::Acquire)
    }

    /// Run one committed-replay application with `applied` recorded in the
    /// same transaction. The state machine applies entries one at a time.
    #[cfg_attr(not(feature = "raft"), allow(dead_code))]
    pub(crate) fn raft_apply<T>(
        &self,
        applied: RaftApplied,
        run: impl FnOnce() -> Result<T, Status>,
    ) -> Result<T, Status> {
        *self.inner.raft_pending.lock().unwrap() = Some(applied);
        let result = run();
        *self.inner.raft_pending.lock().unwrap() = None;
        result
    }

    /// Record an applied position with no application change: blank and
    /// membership entries, and entries refused at their envelope.
    #[cfg_attr(not(feature = "raft"), allow(dead_code))]
    pub(crate) fn apply_raft_position(&self, applied: &RaftApplied) -> Result<(), Status> {
        let _exclusive = self.exclusive()?;
        self.guarded(|| {
            let mut tx = self.inner.database().begin_write().map_err(storage)?;
            tx.set_durability(Durability::Immediate).map_err(storage)?;
            {
                let mut meta = tx.open_table(META).map_err(storage)?;
                Self::refuse_unseeded(&meta)?;
                meta.insert(RAFT_META, applied.encode_to_vec().as_slice())
                    .map_err(storage)?;
            }
            tx.commit().map_err(storage)
        })
    }

    /// The applied Raft position recorded with the last applied entry.
    #[cfg_attr(not(feature = "raft"), allow(dead_code))]
    pub(crate) fn raft_applied(&self) -> Result<Option<RaftApplied>, Status> {
        self.guarded(|| {
            let tx = self.inner.database().begin_read().map_err(storage)?;
            let meta = tx.open_table(META).map_err(storage)?;
            meta.get(RAFT_META)
                .map_err(storage)?
                .map(|v| contract::decode(v.value()))
                .transpose()
        })
    }

    /// Hold every store transaction off while `run` reads the file: the
    /// snapshot builder copies a consistent image. The applied position is
    /// read under the same hold, so image and position agree.
    #[cfg_attr(not(feature = "raft"), allow(dead_code))]
    pub(crate) fn quiesced<T>(
        &self,
        run: impl FnOnce(&Path, Option<RaftApplied>) -> Result<T, Status>,
    ) -> Result<T, Status> {
        let _exclusive = self.exclusive()?;
        self.guarded(|| {
            let applied = {
                let tx = self.inner.database().begin_read().map_err(storage)?;
                let meta = tx.open_table(META).map_err(storage)?;
                meta.get(RAFT_META)
                    .map_err(storage)?
                    .map(|v| contract::decode(v.value()))
                    .transpose()?
            };
            run(&self.inner.path, applied)
        })
    }
}

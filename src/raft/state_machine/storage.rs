//! The library's state-machine interface.

use super::*;

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
    ) -> Result<Box<SnapshotFile>, StorageError<NodeId>> {
        let permit = self
            .staging
            .reserve()
            .map_err(|e| snapshot_error(ErrorVerb::Write, e))?;
        let (dir, file) = self
            .staging
            .begin()
            .map_err(|e| snapshot_error(ErrorVerb::Write, e))?;
        self.staging
            .own_reserved(&dir, file, permit)
            .map(Box::new)
            .map_err(|e| snapshot_error(ErrorVerb::Write, e))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        mut snapshot: Box<SnapshotFile>,
    ) -> Result<(), StorageError<NodeId>> {
        snapshot
            .file
            .flush()
            .await
            .map_err(|e| snapshot_error(ErrorVerb::Write, e))?;
        snapshot
            .file
            .sync_all()
            .await
            .map_err(|e| snapshot_error(ErrorVerb::Write, e))?;
        let receive = snapshot
            .receive
            .take()
            .ok_or_else(|| snapshot_error(ErrorVerb::Read, "no receive is in progress"))?;
        drop(snapshot);
        let result = self.install_received(meta, &receive.receive);
        if let Some(completion) = &receive.completion {
            completion.finish(result.clone());
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
            snapshot: Box::new(SnapshotFile::published(file)),
        }))
    }
}

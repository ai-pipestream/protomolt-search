//! The listener's snapshot staging: chunks received, checked, and held
//! until the transfer completes.

use super::*;

impl RaftTransportService {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        raft: Raft<ControlRaft>,
        node_id: NodeId,
        directory: Arc<PeerDirectory>,
        isolation: Isolation,
        timing: String,
        hold: LeaseHold,
        staging: Arc<SnapshotStaging>,
        rejections: Arc<SnapshotRejections>,
        limits: &TransportLimits,
        read_timeout: Duration,
    ) -> Self {
        Self {
            raft,
            node_id,
            directory,
            isolation,
            timing,
            hold,
            staging,
            rejections,
            transfer: tokio::sync::Mutex::new(None),
            final_receipt: tokio::sync::Mutex::new(None),
            idle: Duration::from_millis(limits.snapshot_idle_timeout_ms),
            read_timeout,
        }
    }

    /// Stage one chunk of a snapshot transfer from `from`. The transfer is
    /// bound to the peer, the vote, the snapshot id and the meta of its
    /// first chunk; bytes go straight to the receive file; the announced
    /// length bounds the disk it may take and the image bound caps that.
    /// One transfer at a time: a chunk of another id from the same peer or
    /// under a higher vote supersedes the transfer in progress, an idle
    /// transfer is dropped, and anything else waits. On the final chunk
    /// the complete image is validated (length, digest, store identity,
    /// applied position, membership on a probe copy); an invalid image is
    /// refused here, by name, and never reaches the library.
    pub(super) async fn stage(
        &self,
        from: NodeId,
        rpc: InstallSnapshotRequest<ControlRaft>,
    ) -> Result<Staged, Status> {
        let announced = self.check_announced(from, &rpc)?;
        let now = std::time::Instant::now();
        let mut guard = self.transfer.lock().await;
        self.open_transfer(&mut guard, from, &rpc, announced, now)?;
        if let Some(staged) = self.write_chunk(&mut guard, from, &rpc, now)? {
            return Ok(staged);
        }
        let complete = guard.take().expect("checked");
        if complete.received != complete.announced {
            self.staging.discard(&complete.dir);
            return Err(reasons::reason(
                Status::invalid_argument(format!(
                    "snapshot transfer ended at {} bytes of the announced length {}",
                    complete.received, complete.announced
                )),
                reasons::TRANSPORT_SNAPSHOT_OVER_ANNOUNCED,
            ));
        }
        // Claim ownership of the incoming directory before yielding to the
        // blocking pool so caller cancellation or a later transfer start can
        // neither delete nor leak this image while disk verification runs, and
        // release the transfer slot lock so higher-vote transfers never wait on
        // blocking image verification.
        let claimed = self.staging.claim(&complete.dir, complete.file)?;
        drop(guard);
        let staging = Arc::clone(&self.staging);
        let verify_meta = complete.meta.clone();
        let mut snapshot = tokio::task::spawn_blocking(move || {
            claimed.verify_and_into_snapshot(&staging, &verify_meta)
        })
        .await
        .map_err(|error| {
            Status::internal(format!("snapshot verification task failed: {error}"))
        })??;
        let completion = Arc::new(SnapshotCompletion::new());
        snapshot.set_completion(Arc::clone(&completion));
        Ok(Staged::Complete {
            meta: complete.meta,
            vote: complete.vote,
            snapshot: Box::new(snapshot),
            completion,
        })
    }

    /// The length the snapshot id announces, refused by name past the
    /// image bound.
    fn check_announced(
        &self,
        from: NodeId,
        rpc: &InstallSnapshotRequest<ControlRaft>,
    ) -> Result<u64, Status> {
        let announced = super::super::state_machine::parse_snapshot_id(&rpc.meta.snapshot_id)
            .map_err(wire)?
            .length;
        let bound = self.staging.max_image_bytes();
        if announced <= bound {
            return Ok(announced);
        }
        self.rejections.record(from, announced, bound);
        super::super::rejection_log::record(format!(
            "raft member {} : snapshot from {from} announced {announced} over bound {bound}",
            self.node_id
        ));
        Err(reasons::reason(
            Status::resource_exhausted(format!(
                "snapshot announces {announced} bytes, beyond the {bound} byte image bound of node {}",
                self.node_id
            )),
            reasons::TRANSPORT_SNAPSHOT_OVER_BOUND,
        ))
    }

    /// Drop the transfer in `slot`, and its received bytes.
    fn drop_transfer(&self, slot: &mut Option<Transfer>) {
        if let Some(transfer) = slot.take() {
            self.staging.discard(&transfer.dir);
        }
    }

    /// The transfer this chunk belongs to: the one in progress for its
    /// id, or a new one begun at offset zero once an idle or superseded
    /// transfer is dropped.
    fn open_transfer(
        &self,
        slot: &mut Option<Transfer>,
        from: NodeId,
        rpc: &InstallSnapshotRequest<ControlRaft>,
        announced: u64,
        now: std::time::Instant,
    ) -> Result<(), Status> {
        if slot
            .as_ref()
            .is_some_and(|active| now.duration_since(active.last_chunk) > self.idle)
        {
            self.drop_transfer(slot);
        }
        if slot
            .as_ref()
            .is_some_and(|active| active.meta.snapshot_id == rpc.meta.snapshot_id)
        {
            return Ok(());
        }
        if let Some(active) = slot.as_ref() {
            let supersedes = active.peer == from || rpc.vote > active.vote;
            if !supersedes {
                return Err(Status::unavailable(format!(
                    "a snapshot transfer from node {} is in progress",
                    active.peer
                )));
            }
            self.drop_transfer(slot);
        }
        if rpc.offset != 0 {
            // A chunk of a transfer this node has no record of. The
            // library's sender begins every transfer at offset zero and
            // returns to zero on a mismatch, so a nonzero offset with no
            // transfer in progress is a continuation of one this node
            // dropped (a rejection on its final chunk, or the idle
            // timeout). A mismatch answer here made the sender start the
            // same bytes over from zero on the same opened image, without
            // end and without a fresh serve; a definite refusal spends the
            // sender's retry budget, so it returns to the library's core,
            // backs off and serves its snapshot afresh.
            return Err(reasons::reason(
                Status::failed_precondition(format!(
                    "snapshot chunk at offset {} of {} continues no transfer at node {}: the transfer was dropped, and a transfer begins at offset zero",
                    rpc.offset, rpc.meta.snapshot_id, self.node_id
                )),
                reasons::TRANSPORT_SNAPSHOT_STALE_CONTINUATION,
            ));
        }
        let (dir, file) = self.staging.begin()?;
        *slot = Some(Transfer {
            peer: from,
            vote: rpc.vote,
            meta: rpc.meta.clone(),
            announced,
            dir,
            file,
            received: 0,
            last_chunk: now,
        });
        Ok(())
    }

    /// Write the chunk into the transfer in `slot`. `Some` answers the
    /// chunk (a partial transfer, or an offset mismatch); `None` means the
    /// final chunk landed and the transfer is complete.
    fn write_chunk(
        &self,
        slot: &mut Option<Transfer>,
        from: NodeId,
        rpc: &InstallSnapshotRequest<ControlRaft>,
        now: std::time::Instant,
    ) -> Result<Option<Staged>, Status> {
        let active = slot.as_mut().expect("a transfer is in progress");
        if active.peer != from {
            return Err(reasons::reason(
                Status::failed_precondition(format!(
                    "snapshot transfer {} is bound to node {}",
                    active.meta.snapshot_id, active.peer
                )),
                reasons::TRANSPORT_SNAPSHOT_TRANSFER_BOUND,
            ));
        }
        if active.meta != rpc.meta || active.vote != rpc.vote {
            self.drop_transfer(slot);
            return Err(Status::invalid_argument(
                "snapshot meta or vote changed within one transfer; the transfer is dropped",
            ));
        }
        if let Some(mismatch) = self.rewind(slot, rpc.offset)? {
            return Ok(Some(mismatch));
        }
        let active = slot.as_mut().expect("a transfer is in progress");
        let end = rpc.offset.saturating_add(rpc.data.len() as u64);
        if end > active.announced {
            let announced = active.announced;
            self.drop_transfer(slot);
            return Err(reasons::reason(
                Status::invalid_argument(format!(
                    "snapshot chunk ends at byte {end}, beyond the announced length {announced}; the transfer is dropped"
                )),
                reasons::TRANSPORT_SNAPSHOT_OVER_ANNOUNCED,
            ));
        }
        if let Err(error) = std::io::Write::write_all(&mut active.file, &rpc.data) {
            self.drop_transfer(slot);
            return Err(Status::internal(format!("snapshot receive: {error}")));
        }
        active.received += rpc.data.len() as u64;
        active.last_chunk = now;
        Ok((!rpc.done).then_some(Staged::Partial))
    }

    /// A chunk at offset zero of a transfer under way starts it over (the
    /// library's own response to a mismatch): the bytes so far are
    /// discarded. Any other offset must be where the transfer stands;
    /// `Some` answers a mismatch.
    fn rewind(&self, slot: &mut Option<Transfer>, offset: u64) -> Result<Option<Staged>, Status> {
        let active = slot.as_mut().expect("a transfer is in progress");
        if offset == 0 && active.received != 0 {
            use std::io::Seek;
            if let Err(error) = active
                .file
                .set_len(0)
                .and_then(|()| active.file.seek(std::io::SeekFrom::Start(0)))
            {
                self.drop_transfer(slot);
                return Err(Status::internal(format!("snapshot receive: {error}")));
            }
            active.received = 0;
        } else if offset != active.received {
            return Ok(Some(Staged::Mismatch {
                expect_offset: active.received,
            }));
        }
        Ok(None)
    }

    pub(super) async fn stage_or_replay(
        &self,
        from: NodeId,
        rpc: InstallSnapshotRequest<ControlRaft>,
    ) -> Result<Staged, Status> {
        let Some(chunk) = rpc.done.then(|| FinalChunk::new(from, &rpc)) else {
            return self.stage(from, rpc).await;
        };
        let mut receipt = self.final_receipt.lock().await;
        if receipt
            .as_ref()
            .is_some_and(|previous| previous.completion.failed())
        {
            *receipt = None;
        }
        if let Some(previous) = receipt.as_ref() {
            let restarted = self.transfer.lock().await.as_ref().is_some_and(|active| {
                active.peer == from && active.vote == rpc.vote && active.meta == rpc.meta
            });
            if !restarted && previous.chunk == chunk {
                return Ok(Staged::Replay(Arc::clone(&previous.completion)));
            }
        }
        let staged = self.stage(from, rpc).await?;
        if let Staged::Complete { completion, .. } = &staged {
            *receipt = Some(FinalReceipt {
                chunk,
                completion: Arc::clone(completion),
            });
        }
        Ok(staged)
    }

    pub(crate) fn into_server(self, limits: &TransportLimits) -> RaftTransportServer<Self> {
        RaftTransportServer::new(self)
            .max_decoding_message_size(limits.max_message_bytes)
            .max_encoding_message_size(limits.max_message_bytes)
    }

    pub(super) fn header(&self, to: NodeId) -> RaftRpcHeader {
        RaftRpcHeader {
            protocol_version: PROTOCOL_VERSION,
            group: Some(self.directory.group().clone()),
            from_node_id: self.node_id,
            to_node_id: to,
        }
    }
}

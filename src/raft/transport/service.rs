//! The callee side: `RaftTransportService` and its `RaftTransport` impl.

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
            transfer: Mutex::new(None),
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
    fn stage(
        &self,
        from: NodeId,
        rpc: InstallSnapshotRequest<ControlRaft>,
    ) -> Result<Staged, Status> {
        let announced = super::super::state_machine::parse_snapshot_id(&rpc.meta.snapshot_id)
            .map_err(wire)?
            .length;
        if announced > self.staging.max_image_bytes() {
            let bound = self.staging.max_image_bytes();
            self.rejections.record(from, announced, bound);
            super::super::rejection_log::record(format!(
                "raft member {} : snapshot from {from} announced {announced} over bound {bound}",
                self.node_id
            ));
            return Err(reasons::reason(
                Status::resource_exhausted(format!(
                    "snapshot announces {announced} bytes, beyond the {bound} byte image bound of node {}",
                    self.node_id
                )),
                reasons::TRANSPORT_SNAPSHOT_OVER_BOUND,
            ));
        }
        let now = std::time::Instant::now();
        let mut guard = self
            .transfer
            .lock()
            .map_err(|_| Status::internal("snapshot transfer lock poisoned"))?;
        if guard
            .as_ref()
            .is_some_and(|active| now.duration_since(active.last_chunk) > self.idle)
        {
            let stale = guard.take().expect("checked");
            self.staging.discard(&stale.dir);
        }
        let same_id = guard
            .as_ref()
            .is_some_and(|active| active.meta.snapshot_id == rpc.meta.snapshot_id);
        if !same_id {
            if let Some(active) = guard.as_ref() {
                let supersedes = active.peer == from || rpc.vote > active.vote;
                if !supersedes {
                    return Err(Status::unavailable(format!(
                        "a snapshot transfer from node {} is in progress",
                        active.peer
                    )));
                }
                let superseded = guard.take().expect("checked");
                self.staging.discard(&superseded.dir);
            }
            if rpc.offset != 0 {
                // A chunk of a transfer this node has no record of. The
                // library's sender begins every transfer at offset zero and
                // returns to zero on a mismatch, so a nonzero offset with no
                // transfer in progress is a continuation of one this node
                // dropped (a rejection on its final chunk, or the idle
                // timeout). A mismatch answer here made the sender start the
                // same bytes over from zero on the same opened image,
                // without end and without a fresh serve; a definite refusal
                // spends the sender's retry budget, so it returns to the
                // library's core, backs off and serves its snapshot afresh.
                return Err(reasons::reason(
                    Status::failed_precondition(format!(
                        "snapshot chunk at offset {} of {} continues no transfer at node {}: the transfer was dropped, and a transfer begins at offset zero",
                        rpc.offset, rpc.meta.snapshot_id, self.node_id
                    )),
                    reasons::TRANSPORT_SNAPSHOT_STALE_CONTINUATION,
                ));
            }
            let (dir, file) = self.staging.begin()?;
            *guard = Some(Transfer {
                peer: from,
                vote: rpc.vote,
                meta: rpc.meta.clone(),
                announced,
                dir,
                file,
                received: 0,
                last_chunk: now,
            });
        }
        let active = guard.as_mut().expect("a transfer is in progress");
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
            let broken = guard.take().expect("checked");
            self.staging.discard(&broken.dir);
            return Err(Status::invalid_argument(
                "snapshot meta or vote changed within one transfer; the transfer is dropped",
            ));
        }
        if rpc.offset == 0 && active.received != 0 {
            // The sender starts this transfer over (the library's own
            // response to a mismatch): the bytes so far are discarded.
            use std::io::Seek;
            if let Err(error) = active
                .file
                .set_len(0)
                .and_then(|()| active.file.seek(std::io::SeekFrom::Start(0)))
            {
                let broken = guard.take().expect("checked");
                self.staging.discard(&broken.dir);
                return Err(Status::internal(format!("snapshot receive: {error}")));
            }
            active.received = 0;
        } else if rpc.offset != active.received {
            return Ok(Staged::Mismatch {
                expect_offset: active.received,
            });
        }
        let end = rpc.offset.saturating_add(rpc.data.len() as u64);
        if end > active.announced {
            let announced = active.announced;
            let broken = guard.take().expect("checked");
            self.staging.discard(&broken.dir);
            return Err(reasons::reason(
                Status::invalid_argument(format!(
                    "snapshot chunk ends at byte {end}, beyond the announced length {announced}; the transfer is dropped"
                )),
                reasons::TRANSPORT_SNAPSHOT_OVER_ANNOUNCED,
            ));
        }
        {
            use std::io::Write;
            if let Err(error) = active.file.write_all(&rpc.data) {
                let broken = guard.take().expect("checked");
                self.staging.discard(&broken.dir);
                return Err(Status::internal(format!("snapshot receive: {error}")));
            }
        }
        active.received += rpc.data.len() as u64;
        active.last_chunk = now;
        if !rpc.done {
            return Ok(Staged::Partial);
        }
        let complete = guard.take().expect("checked");
        drop(guard);
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
        if let Err(error) = complete.file.sync_all() {
            self.staging.discard(&complete.dir);
            return Err(Status::internal(format!("snapshot receive: {error}")));
        }
        drop(complete.file);
        if let Err(error) = self.staging.verify(&complete.dir, &complete.meta) {
            self.staging.discard(&complete.dir);
            return Err(error);
        }
        Ok(Staged::Complete {
            meta: complete.meta,
            vote: complete.vote,
            dir: complete.dir,
        })
    }

    pub(crate) fn into_server(self, limits: &TransportLimits) -> RaftTransportServer<Self> {
        RaftTransportServer::new(self)
            .max_decoding_message_size(limits.max_message_bytes)
            .max_encoding_message_size(limits.max_message_bytes)
    }

    /// The peer behind a request: the verified client certificate names a
    /// registered node, and the header agrees with it, this group and this
    /// node. Returns the sender's id.
    fn authenticate<T>(
        &self,
        request: &Request<T>,
        header: Option<&RaftRpcHeader>,
    ) -> Result<NodeId, Status> {
        let certificates = request.peer_certs().ok_or_else(|| {
            reasons::reason(
                Status::unauthenticated(
                    "raft transport requires a client certificate from the cluster CA",
                ),
                reasons::TRANSPORT_NO_CLIENT_CERTIFICATE,
            )
        })?;
        let leaf = certificates.first().ok_or_else(|| {
            reasons::reason(
                Status::unauthenticated(
                    "raft transport requires a client certificate from the cluster CA",
                ),
                reasons::TRANSPORT_NO_CLIENT_CERTIFICATE,
            )
        })?;
        let fingerprint = crate::sha256::digest(leaf.as_ref());
        let registered = self.directory.node_of(&fingerprint).ok_or_else(|| {
            reasons::reason(
                Status::unauthenticated(
                    "client certificate is not registered to any member of this group",
                ),
                reasons::TRANSPORT_UNREGISTERED_CERTIFICATE,
            )
        })?;
        let header = header.ok_or_else(|| {
            reasons::reason(
                Status::invalid_argument("raft rpc carries no header"),
                reasons::TRANSPORT_BAD_HEADER,
            )
        })?;
        if header.protocol_version != PROTOCOL_VERSION {
            return Err(reasons::reason(
                Status::invalid_argument(format!(
                    "raft rpc protocol version {} is not {PROTOCOL_VERSION}",
                    header.protocol_version
                )),
                reasons::TRANSPORT_BAD_HEADER,
            ));
        }
        if header.group.as_ref() != Some(self.directory.group()) {
            return Err(reasons::reason(
                Status::permission_denied("raft rpc names another group"),
                reasons::TRANSPORT_WRONG_GROUP,
            ));
        }
        if header.to_node_id != self.node_id {
            return Err(reasons::reason(
                Status::permission_denied(format!(
                    "raft rpc addressed to node {} reached node {}",
                    header.to_node_id, self.node_id
                )),
                reasons::TRANSPORT_MISADDRESSED,
            ));
        }
        if header.from_node_id != registered {
            return Err(reasons::reason(
                Status::permission_denied(format!(
                    "raft rpc claims node {} but the certificate is registered to node {registered}",
                    header.from_node_id
                )),
                reasons::TRANSPORT_IDENTITY_MISMATCH,
            ));
        }
        if self.isolation.blocks(registered) {
            return Err(reasons::reason(
                Status::unavailable(format!(
                    "node {registered} is isolated from this node (fault injection)"
                )),
                reasons::TRANSPORT_ISOLATED,
            ));
        }
        if self.isolation.deaf_to(registered) {
            return Err(reasons::reason(
                Status::unavailable(format!(
                    "node {registered} is not heard at this node (fault injection)"
                )),
                reasons::TRANSPORT_NOT_HEARD,
            ));
        }
        let timing = request.metadata().get(TIMING_METADATA).ok_or_else(|| {
            reasons::reason(
                Status::failed_precondition(
                    "raft rpc carries no timing agreement; the peer runs another build",
                ),
                reasons::TRANSPORT_TIMING_MISMATCH,
            )
        })?;
        if timing.as_bytes() != self.timing.as_bytes() {
            return Err(reasons::reason(
                Status::failed_precondition(format!(
                    "peer timing configuration {:?} differs from this node's {}; election and lease values must agree across the group",
                    timing, self.timing
                )),
                reasons::TRANSPORT_TIMING_MISMATCH,
            ));
        }
        Ok(registered)
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

pub(super) fn core_stopped<E: std::error::Error>(error: RaftError<NodeId, E>) -> Status {
    Status::unavailable(format!("raft core: {error}"))
}

impl RaftTransportService {
    async fn answer_append_entries(
        &self,
        request: Request<RaftAppendEntriesRequest>,
    ) -> Result<Response<RaftAppendEntriesResponse>, Status> {
        let from = self.authenticate(&request, request.get_ref().header.as_ref())?;
        let rpc = append_request_from_proto(request.into_inner())?;
        let response = match self.raft.append_entries(rpc).await {
            Ok(response) => response,
            Err(error) => return Err(core_stopped(error)),
        };
        Ok(Response::new(append_response_to_proto(
            self.header(from),
            response,
        )))
    }

    async fn answer_vote(
        &self,
        request: Request<RaftVoteRequest>,
    ) -> Result<Response<RaftVoteResponse>, Status> {
        let from = self.authenticate(&request, request.get_ref().header.as_ref())?;
        let rpc = vote_request_from_proto(request.into_inner())?;
        // A leader with an admission interval possibly open withholds its
        // vote; the library would grant it (see `LeaseHold`).
        let held = {
            let metrics = self.raft.metrics().borrow().clone();
            if metrics.state == openraft::ServerState::Leader
                && self.hold.holds(std::time::Instant::now())
            {
                Some(metrics.vote)
            } else {
                None
            }
        };
        let response = match held {
            Some(vote) => VoteResponse {
                vote,
                vote_granted: false,
                last_log_id: None,
            },
            None => match self.raft.vote(rpc).await {
                Ok(response) => response,
                Err(error) => return Err(core_stopped(error)),
            },
        };
        Ok(Response::new(vote_response_to_proto(
            self.header(from),
            response,
        )))
    }

    async fn answer_install_snapshot(
        &self,
        request: Request<RaftInstallSnapshotRequest>,
    ) -> Result<Response<RaftInstallSnapshotResponse>, Status> {
        let from = self.authenticate(&request, request.get_ref().header.as_ref())?;
        let rpc = install_request_from_proto(self.directory.group(), request.into_inner())?;
        let snapshot_id = rpc.meta.snapshot_id.clone();
        let offset = rpc.offset;
        let outcome = match self.stage(from, rpc)? {
            Staged::Partial => {
                InstallOutcome::Vote(vote_to_proto(&self.raft.metrics().borrow().vote))
            }
            Staged::Mismatch { expect_offset } => InstallOutcome::Mismatch(RaftSnapshotMismatch {
                expect_id: snapshot_id.clone(),
                expect_offset,
                got_id: snapshot_id,
                got_offset: offset,
            }),
            Staged::Complete { meta, vote, dir } => {
                // The image is validated; the library applies its own term
                // and position rules and may decline it (an older vote, a
                // position it already holds). Only a storage failure in the
                // swap is fatal, as before.
                let file = match tokio::fs::File::open(SnapshotStaging::image_path(&dir)).await {
                    Ok(file) => file,
                    Err(error) => {
                        self.staging.discard(&dir);
                        return Err(Status::internal(format!("snapshot receive: {error}")));
                    }
                };
                let response = self
                    .raft
                    .install_full_snapshot(
                        vote,
                        Snapshot {
                            meta,
                            snapshot: Box::new(file),
                        },
                    )
                    .await
                    .map_err(|e| Status::unavailable(format!("raft core: {e}")))?;
                if self.staging.is_current(&dir) {
                    // Declined by the library: the validated bytes are of no
                    // further use.
                    self.staging.discard(&dir);
                }
                InstallOutcome::Vote(vote_to_proto(&response.vote))
            }
        };
        Ok(Response::new(RaftInstallSnapshotResponse {
            header: Some(self.header(from)),
            outcome: Some(outcome),
        }))
    }
}

impl RaftTransportService {
    /// A member that does not lead asks this node for a lease
    /// (docs/raft-admission.md, "A lease forwarded from the leader"): the
    /// read barrier bounded as for a local lease, the vote hold extended
    /// from the instant the request was received, and the position the
    /// barrier read returned for the asker to apply before it grants. A
    /// node that does not lead answers by naming the leader, and the
    /// asker retries where the library points it.
    async fn answer_grant_lease(
        &self,
        request: Request<RaftGrantLeaseRequest>,
    ) -> Result<Response<RaftGrantLeaseResponse>, Status> {
        let from = self.authenticate(&request, request.get_ref().header.as_ref())?;
        let anchor = std::time::Instant::now();
        let read = match tokio::time::timeout(self.read_timeout, self.raft.ensure_linearizable())
            .await
        {
            Ok(Ok(read)) => read,
            Ok(Err(RaftError::APIError(CheckIsLeaderError::ForwardToLeader(forward)))) => {
                return Err(reasons::reason(
                    Status::unavailable(format!(
                        "not the leader; leader is {:?}",
                        forward.leader_id
                    )),
                    reasons::LEASE_NOT_LEADER,
                ))
            }
            Ok(Err(e)) => {
                return Err(reasons::reason(
                    Status::unavailable(format!(
                        "a forwarded lease needs a linearizable read: {e}"
                    )),
                    reasons::TRANSPORT_NO_LINEARIZABLE_READ,
                ))
            }
            Err(_) => {
                return Err(reasons::reason(
                    Status::unavailable(format!(
                        "a forwarded lease needs a linearizable read: no quorum acknowledged within {} ms",
                        self.read_timeout.as_millis()
                    )),
                    reasons::TRANSPORT_NO_LINEARIZABLE_READ,
                ))
            }
        };
        self.hold.extend(anchor);
        Ok(Response::new(RaftGrantLeaseResponse {
            header: Some(self.header(from)),
            read_log_id: read.as_ref().map(log_id_to_proto),
        }))
    }
}

/// Metadata key on every status this service answers with. The client
/// records a rejection only for a status that carries it: a status
/// without it was made by the transport (a connection closed, a frame
/// refused, a message past the client's own decode bound) and is a
/// transport failure to retry, not a peer's answer.
pub(crate) const ANSWERED: &str = "psearch-raft-answer";

pub(super) fn answered(mut status: Status) -> Status {
    status
        .metadata_mut()
        .insert(ANSWERED, tonic::metadata::MetadataValue::from_static("1"));
    status
}

#[tonic::async_trait]
impl RaftTransport for RaftTransportService {
    async fn append_entries(
        &self,
        request: Request<RaftAppendEntriesRequest>,
    ) -> Result<Response<RaftAppendEntriesResponse>, Status> {
        self.answer_append_entries(request).await.map_err(answered)
    }

    async fn vote(
        &self,
        request: Request<RaftVoteRequest>,
    ) -> Result<Response<RaftVoteResponse>, Status> {
        self.answer_vote(request).await.map_err(answered)
    }

    async fn install_snapshot(
        &self,
        request: Request<RaftInstallSnapshotRequest>,
    ) -> Result<Response<RaftInstallSnapshotResponse>, Status> {
        self.answer_install_snapshot(request)
            .await
            .map_err(answered)
    }

    async fn grant_lease(
        &self,
        request: Request<RaftGrantLeaseRequest>,
    ) -> Result<Response<RaftGrantLeaseResponse>, Status> {
        self.answer_grant_lease(request).await.map_err(answered)
    }
}

//! The listener's answers to vote, append, snapshot and lease requests.

use super::*;

pub(super) fn core_stopped<E: std::error::Error>(error: RaftError<NodeId, E>) -> Status {
    Status::unavailable(format!("raft core: {error}"))
}

impl RaftTransportService {
    pub(super) async fn answer_append_entries(
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

    pub(super) async fn answer_vote(
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

    pub(super) async fn answer_install_snapshot(
        &self,
        request: Request<RaftInstallSnapshotRequest>,
    ) -> Result<Response<RaftInstallSnapshotResponse>, Status> {
        let from = self.authenticate(&request, request.get_ref().header.as_ref())?;
        let rpc = install_request_from_proto(self.directory.group(), request.into_inner())?;
        let snapshot_id = rpc.meta.snapshot_id.clone();
        let offset = rpc.offset;
        let outcome = match self.stage_or_replay(from, rpc).await? {
            Staged::Partial => {
                InstallOutcome::Vote(vote_to_proto(&self.raft.metrics().borrow().vote))
            }
            Staged::Mismatch { expect_offset } => InstallOutcome::Mismatch(RaftSnapshotMismatch {
                expect_id: snapshot_id.clone(),
                expect_offset,
                got_id: snapshot_id,
                got_offset: offset,
            }),
            Staged::Replay(completion) => {
                InstallOutcome::Vote(vote_to_proto(&self.replay_vote(&completion).await?))
            }
            Staged::Complete {
                meta,
                vote,
                snapshot,
                completion,
            } => {
                let installed = self
                    .install_complete(meta, vote, snapshot, completion)
                    .await?;
                InstallOutcome::Vote(vote_to_proto(&installed))
            }
        };
        Ok(Response::new(RaftInstallSnapshotResponse {
            header: Some(self.header(from)),
            outcome: Some(outcome),
        }))
    }

    /// The vote a replayed final chunk answers with, once the original
    /// finalization completed. A replay observes the current term; an old
    /// cached vote or a lagging metrics watch must never hide a later vote.
    async fn replay_vote(
        &self,
        completion: &SnapshotCompletion,
    ) -> Result<openraft::Vote<NodeId>, Status> {
        tokio::time::timeout(self.idle, async {
            completion.wait().await?;
            self.raft
                .with_raft_state(|state| *state.vote_ref())
                .await
                .map_err(|error| Status::unavailable(format!("raft core: {error}")))
        })
        .await
        .map_err(|_| {
            Status::unavailable(
                "snapshot finalization has not completed within the receive idle interval",
            )
        })?
    }

    /// Hand a validated image to the library. It applies its own term and
    /// position rules and may decline it (an older vote, a position it
    /// already holds); only a storage failure in the swap is fatal. The
    /// completion records the outcome for a replayed final chunk.
    async fn install_complete(
        &self,
        meta: SnapshotMeta<NodeId, BasicNode>,
        vote: openraft::Vote<NodeId>,
        snapshot: Box<SnapshotFile>,
        completion: Arc<SnapshotCompletion>,
    ) -> Result<openraft::Vote<NodeId>, Status> {
        #[cfg(any(test, feature = "fault-injection"))]
        let mut drop_guard = FinalHandlerDropGuard {
            staging: &self.staging,
            snapshot: Some(snapshot),
            armed: true,
        };
        #[cfg(any(test, feature = "fault-injection"))]
        let snapshot = {
            self.staging.gates().pause_handoff().await;
            drop_guard.snapshot.take().expect("snapshot still held")
        };
        let response = self
            .raft
            .install_full_snapshot(vote, Snapshot { meta, snapshot })
            .await
            .map_err(|e| Status::unavailable(format!("raft core: {e}")));
        #[cfg(any(test, feature = "fault-injection"))]
        {
            drop_guard.armed = false;
        }
        completion.finish(response.as_ref().map(|_| ()).map_err(Clone::clone));
        Ok(response?.vote)
    }
}

/// Counts a final-chunk handler dropped while it still held the image,
/// for the fault-injection tests.
#[cfg(any(test, feature = "fault-injection"))]
struct FinalHandlerDropGuard<'a> {
    staging: &'a SnapshotStaging,
    snapshot: Option<Box<SnapshotFile>>,
    armed: bool,
}

#[cfg(any(test, feature = "fault-injection"))]
impl Drop for FinalHandlerDropGuard<'_> {
    fn drop(&mut self) {
        drop(self.snapshot.take());
        if self.armed {
            self.staging.gates().record_final_handler_drop();
        }
    }
}

impl RaftTransportService {
    /// A member that does not lead asks this node for a lease: the
    /// read barrier bounded as for a local lease, the vote hold extended
    /// from the instant the request was received, and the position the
    /// barrier read returned for the asker to apply before it grants. A
    /// node that does not lead answers by naming the leader, and the
    /// asker retries where the library points it.
    pub(super) async fn answer_grant_lease(
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

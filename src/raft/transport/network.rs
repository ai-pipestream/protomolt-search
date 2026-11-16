//! One peer's channel and how its failures are classified.

use super::*;

/// Unavailable and Unknown first, whoever made them (the service answers
/// Unavailable for a stopped core, an isolated node and a transfer in
/// progress); then the deadline codes; then the mark decides.
pub(crate) fn classify(status: &Status) -> Answer {
    match status.code() {
        tonic::Code::Unavailable | tonic::Code::Unknown => Answer::Unreachable,
        tonic::Code::DeadlineExceeded | tonic::Code::Cancelled => Answer::Timeout,
        _ if status.metadata().contains_key(ANSWERED) => Answer::Rejection,
        _ => Answer::Transport,
    }
}

impl TonicNetwork {
    pub(super) fn header(&self) -> RaftRpcHeader {
        RaftRpcHeader {
            protocol_version: PROTOCOL_VERSION,
            group: Some(self.shared.directory.group().clone()),
            from_node_id: self.shared.node_id,
            to_node_id: self.target,
        }
    }

    pub(super) fn unreachable<E: std::error::Error>(message: String) -> Rpc<E> {
        RPCError::Unreachable(Unreachable::new(&std::io::Error::other(message)))
    }

    pub(super) fn network<E: std::error::Error>(message: String) -> Rpc<E> {
        RPCError::Network(NetworkError::new(&std::io::Error::other(message)))
    }

    /// The target must be a registered member and not isolated, and the
    /// channel is built once from the cluster client TLS material.
    pub(super) fn client<E: std::error::Error>(
        &mut self,
    ) -> Result<&mut RaftTransportClient<Channel>, Rpc<E>> {
        if !self.shared.directory.knows(self.target) {
            return Err(Self::unreachable(format!(
                "node {} is not registered in the peer directory",
                self.target
            )));
        }
        if self.shared.isolation.blocks(self.target) {
            return Err(Self::unreachable(format!(
                "node {} is isolated from this node (fault injection)",
                self.target
            )));
        }
        if self.client.is_none() {
            let url = secure_url(&self.addr, Some(&self.shared.client_tls));
            let endpoint = Endpoint::from_shared(url)
                .map_err(|e| Self::unreachable::<E>(format!("raft peer address: {e}")))?
                .connect_timeout(Duration::from_millis(self.shared.limits.connect_timeout_ms))
                .tcp_nodelay(true);
            let endpoint = apply_client_tls(endpoint, Some(&self.shared.client_tls))
                .map_err(Self::unreachable::<E>)?;
            let client = RaftTransportClient::new(endpoint.connect_lazy())
                .max_decoding_message_size(self.shared.limits.max_message_bytes)
                .max_encoding_message_size(self.shared.limits.max_message_bytes);
            self.client = Some(client);
        }
        Ok(self.client.as_mut().expect("client was just built"))
    }

    pub(super) fn request<T>(&self, message: T, option: &RPCOption) -> Request<T> {
        let mut request = Request::new(message);
        request.set_timeout(option.hard_ttl());
        request
            .metadata_mut()
            .insert(TIMING_METADATA, self.shared.timing.clone());
        request
    }

    /// A reply must come from the node that was dialed, for this group.
    pub(super) fn check_reply(&self, header: Option<&RaftRpcHeader>) -> Result<(), Status> {
        let header =
            header.ok_or_else(|| Status::invalid_argument("raft reply carries no header"))?;
        if header.protocol_version != PROTOCOL_VERSION
            || header.group.as_ref() != Some(self.shared.directory.group())
            || header.to_node_id != self.shared.node_id
        {
            return Err(Status::permission_denied(
                "raft reply names another group, node or protocol",
            ));
        }
        if header.from_node_id != self.target {
            return Err(Status::permission_denied(format!(
                "raft reply from node {} answered a request to node {}",
                header.from_node_id, self.target
            )));
        }
        Ok(())
    }

    pub(super) fn failure<E: std::error::Error>(
        &self,
        action: RPCTypes,
        ttl: Duration,
        status: Status,
    ) -> Rpc<E> {
        match classify(&status) {
            Answer::Unreachable => Self::unreachable(format!(
                "node {} at {}: {}",
                self.target,
                self.addr,
                status.message()
            )),
            Answer::Timeout => RPCError::Timeout(Timeout {
                action,
                id: self.shared.node_id,
                target: self.target,
                timeout: ttl,
            }),
            Answer::Transport => Self::unreachable(format!(
                "node {} at {}: transport failure ({:?}): {}",
                self.target,
                self.addr,
                status.code(),
                status.message()
            )),
            Answer::Rejection => {
                // The peer answered: a rejection, recorded for the host's
                // accessors and for `add_learner`. A rejected snapshot is
                // returned as unreachable so the library backs off before
                // it serves the image again, rather than retrying at once.
                let (name, rejected_snapshot) = match action {
                    RPCTypes::AppendEntries => ("append", false),
                    RPCTypes::Vote => ("vote", false),
                    RPCTypes::InstallSnapshot => ("snapshot", true),
                };
                self.shared.rejections.record(self.target, name, &status);
                let message = format!(
                    "node {} rejected the {name}: {} ({:?})",
                    self.target,
                    status.message(),
                    status.code()
                );
                if rejected_snapshot {
                    Self::unreachable(message)
                } else {
                    Self::network(message)
                }
            }
        }
    }
}

async fn race_shutdown<T>(
    mut closed: tokio::sync::watch::Receiver<bool>,
    call: impl std::future::Future<Output = Result<Response<T>, Status>>,
) -> Result<T, Result<Status, &'static str>> {
    if *closed.borrow_and_update() {
        return Err(Err("raft host is shutting down"));
    }
    tokio::select! {
        reply = call => reply.map(Response::into_inner).map_err(Ok),
        _ = closed.wait_for(|stopped| *stopped) => Err(Err("raft host is shutting down")),
    }
}

impl RaftNetwork<ControlRaft> for TonicNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<ControlRaft>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, Rpc<RaftError<NodeId>>> {
        let message = append_request_to_proto(self.header(), &rpc);
        let bound = self.shared.limits.max_message_bytes;
        if message.encoded_len() > bound {
            if rpc.entries.len() > 1 {
                return Err(RPCError::PayloadTooLarge(
                    PayloadTooLarge::new_entries_hint((rpc.entries.len() / 2).max(1) as u64),
                ));
            }
            return Err(Self::network(format!(
                "one log entry of {} bytes exceeds the {bound} byte transport bound",
                message.encoded_len()
            )));
        }
        let ttl = option.hard_ttl();
        let request = self.request(message, &option);
        let closed = self.shared.closed.subscribe();
        let client = self.client()?;
        let reply = race_shutdown(closed, client.append_entries(request))
            .await
            .map_err(|err| match err {
                Ok(status) => self.failure(RPCTypes::AppendEntries, ttl, status),
                Err(msg) => Self::unreachable(msg.to_string()),
            })?;
        self.check_reply(reply.header.as_ref())
            .and_then(|()| append_response_from_proto(reply))
            .map_err(|status| Self::network(status.message().to_string()))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<ControlRaft>,
        option: RPCOption,
    ) -> Result<InstallSnapshotResponse<NodeId>, Rpc<RaftError<NodeId, InstallSnapshotError>>> {
        let group = self.shared.directory.group().clone();
        let message = install_request_to_proto(self.header(), &group, rpc)
            .map_err(|status| Self::network(status.message().to_string()))?;
        if message.encoded_len() > self.shared.limits.max_message_bytes {
            return Err(Self::network(format!(
                "snapshot chunk of {} bytes exceeds the {} byte transport bound",
                message.encoded_len(),
                self.shared.limits.max_message_bytes
            )));
        }
        let ttl = option.hard_ttl();
        let request = self.request(message, &option);
        let closed = self.shared.closed.subscribe();
        let client = self.client()?;
        let reply = race_shutdown(closed, client.install_snapshot(request))
            .await
            .map_err(|err| match err {
                Ok(status) => self.failure(RPCTypes::InstallSnapshot, ttl, status),
                Err(msg) => Self::unreachable(msg.to_string()),
            })?;
        self.check_reply(reply.header.as_ref())
            .map_err(|status| Self::network(status.message().to_string()))?;
        match reply.outcome {
            Some(InstallOutcome::Vote(vote)) => Ok(InstallSnapshotResponse {
                vote: vote_from_proto(&vote),
            }),
            Some(InstallOutcome::Mismatch(mismatch)) => {
                Err(RPCError::RemoteError(RemoteError::new_with_node(
                    self.target,
                    BasicNode {
                        addr: self.addr.clone(),
                    },
                    RaftError::APIError(InstallSnapshotError::SnapshotMismatch(SnapshotMismatch {
                        expect: SnapshotSegmentId {
                            id: mismatch.expect_id,
                            offset: mismatch.expect_offset,
                        },
                        got: SnapshotSegmentId {
                            id: mismatch.got_id,
                            offset: mismatch.got_offset,
                        },
                    })),
                )))
            }
            None => Err(Self::network(
                "install snapshot reply carries no outcome".to_string(),
            )),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, Rpc<RaftError<NodeId>>> {
        let message = vote_request_to_proto(self.header(), &rpc);
        let ttl = option.hard_ttl();
        let request = self.request(message, &option);
        let closed = self.shared.closed.subscribe();
        let client = self.client()?;
        let reply = race_shutdown(closed, client.vote(request))
            .await
            .map_err(|err| match err {
                Ok(status) => self.failure(RPCTypes::Vote, ttl, status),
                Err(msg) => Self::unreachable(msg.to_string()),
            })?;
        self.check_reply(reply.header.as_ref())
            .and_then(|()| vote_response_from_proto(reply))
            .map_err(|status| Self::network(status.message().to_string()))
    }
}

#[cfg(test)]
mod answer_tests {
    use super::super::answer::answered;
    use super::{classify, Answer};
    use tonic::{Code, Status};

    /// The codes tonic uses for transport events (a connection closed
    /// with NO_ERROR is Internal, ENHANCE_YOUR_CALM is ResourceExhausted,
    /// an unexpected end of stream and this client's own decode bound are
    /// OutOfRange) are the codes the service answers with too; only the
    /// service's mark tells them apart.
    #[test]
    fn a_status_is_a_rejection_only_with_the_services_mark() {
        for code in [
            Code::Internal,
            Code::ResourceExhausted,
            Code::OutOfRange,
            Code::DataLoss,
            Code::PermissionDenied,
            Code::FailedPrecondition,
            Code::InvalidArgument,
            Code::Unauthenticated,
            Code::Aborted,
        ] {
            assert_eq!(
                classify(&Status::new(code, "transport")),
                Answer::Transport,
                "{code:?}"
            );
            assert_eq!(
                classify(&answered(Status::new(code, "answer"))),
                Answer::Rejection,
                "{code:?}"
            );
        }
        for code in [Code::Unavailable, Code::Unknown] {
            assert_eq!(classify(&Status::new(code, "down")), Answer::Unreachable);
            assert_eq!(
                classify(&answered(Status::new(code, "down"))),
                Answer::Unreachable
            );
        }
        for code in [Code::DeadlineExceeded, Code::Cancelled] {
            assert_eq!(classify(&Status::new(code, "late")), Answer::Timeout);
            assert_eq!(
                classify(&answered(Status::new(code, "late"))),
                Answer::Timeout
            );
        }
    }
}

//! The caller side: peer rejection records, the network factory, the
//! lease forwarder, and `TonicNetwork`.

use super::*;

impl PeerRejections {
    pub(super) fn new() -> Self {
        let (sender, _) = tokio::sync::watch::channel(BTreeMap::new());
        Self {
            sender: Arc::new(sender),
        }
    }

    pub(super) fn record(&self, target: NodeId, action: &'static str, status: &Status) {
        self.sender.send_modify(|rejections| {
            let count = rejections.get(&target).map_or(0, |r| r.count) + 1;
            rejections.insert(
                target,
                PeerRejection {
                    action,
                    code: status.code(),
                    message: status.message().to_string(),
                    at: std::time::Instant::now(),
                    count,
                },
            );
        });
    }

    /// The latest rejection from each peer.
    pub fn latest(&self) -> BTreeMap<NodeId, PeerRejection> {
        self.sender.borrow().clone()
    }

    pub(crate) fn subscribe(
        &self,
    ) -> tokio::sync::watch::Receiver<BTreeMap<NodeId, PeerRejection>> {
        self.sender.subscribe()
    }
}

impl SnapshotRejections {
    pub(super) fn record(&self, from: NodeId, announced: u64, bound: u64) {
        self.count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if let Ok(mut last) = self.last.lock() {
            *last = Some(SnapshotRejection {
                from,
                announced,
                bound,
            });
        }
    }

    /// Installs rejected at the announce since this node started.
    pub fn count(&self) -> u64 {
        self.count.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// The latest rejection.
    pub fn last(&self) -> Option<SnapshotRejection> {
        self.last.lock().ok().and_then(|last| last.clone())
    }
}

impl TonicNetworkFactory {
    pub(crate) fn new(
        node_id: NodeId,
        directory: Arc<PeerDirectory>,
        client_tls: ClientTls,
        limits: TransportLimits,
        isolation: Isolation,
        timing: String,
    ) -> Self {
        Self {
            shared: Arc::new(Shared {
                node_id,
                directory,
                client_tls,
                limits,
                isolation,
                timing: timing
                    .parse()
                    .expect("timing agreement is ASCII digits and punctuation"),
                rejections: PeerRejections::new(),
            }),
        }
    }

    /// The rejections peers answer to this node's RPCs.
    pub(crate) fn rejections(&self) -> PeerRejections {
        self.shared.rejections.clone()
    }
}

impl LeaseForwarder {
    /// Ask `leader` at `addr` for a lease: the leader's barrier position,
    /// or the failure by name. A transport failure or an unreachable
    /// leader is `Unavailable`; a rejection the leader's service answered
    /// keeps its code and names the leader.
    pub(crate) async fn grant_lease(
        &self,
        leader: NodeId,
        addr: &str,
        timeout: Duration,
    ) -> Result<Option<LogId<NodeId>>, Status> {
        let mut network = TonicNetwork {
            shared: Arc::clone(&self.shared),
            target: leader,
            addr: addr.to_string(),
            client: None,
        };
        let header = network.header();
        let client = network.client::<RaftError<NodeId>>().map_err(|e| {
            reasons::reason(
                Status::unavailable(format!("forwarded lease: {e}")),
                reasons::LEASE_LEADER_UNREACHABLE,
            )
        })?;
        let mut request = Request::new(RaftGrantLeaseRequest {
            header: Some(header),
        });
        request.set_timeout(timeout);
        request
            .metadata_mut()
            .insert(TIMING_METADATA, self.shared.timing.clone());
        let response = match client.grant_lease(request).await {
            Ok(response) => response.into_inner(),
            Err(status) => {
                return Err(match classify(&status) {
                    // The leader's answer keeps its code and its reason;
                    // only the message names the forward.
                    Answer::Rejection => reasons::carry(
                        &status,
                        Status::new(
                            status.code(),
                            format!(
                                "forwarded lease: leader {leader} rejected it: {}",
                                status.message()
                            ),
                        ),
                    ),
                    Answer::Timeout => reasons::reason(
                        Status::unavailable(format!(
                            "forwarded lease: leader {leader} at {addr} did not answer within {} ms",
                            timeout.as_millis()
                        )),
                        reasons::LEASE_LEADER_UNREACHABLE,
                    ),
                    Answer::Unreachable | Answer::Transport => reasons::reason(
                        Status::unavailable(format!(
                            "forwarded lease: leader {leader} at {addr}: {}",
                            status.message()
                        )),
                        reasons::LEASE_LEADER_UNREACHABLE,
                    ),
                });
            }
        };
        network
            .check_reply(response.header.as_ref())
            .map_err(|e| Status::unavailable(format!("forwarded lease: {}", e.message())))?;
        Ok(response.read_log_id.as_ref().map(log_id_from_proto))
    }
}

impl TonicNetworkFactory {
    pub(crate) fn forwarder(&self) -> LeaseForwarder {
        LeaseForwarder {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl RaftNetworkFactory<ControlRaft> for TonicNetworkFactory {
    type Network = TonicNetwork;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        TonicNetwork {
            shared: Arc::clone(&self.shared),
            target,
            addr: node.addr.clone(),
            client: None,
        }
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

    fn unreachable<E: std::error::Error>(message: String) -> Rpc<E> {
        RPCError::Unreachable(Unreachable::new(&std::io::Error::other(message)))
    }

    pub(super) fn network<E: std::error::Error>(message: String) -> Rpc<E> {
        RPCError::Network(NetworkError::new(&std::io::Error::other(message)))
    }

    /// The target must be a registered member and not isolated, and the
    /// channel is built once from the cluster client TLS material.
    fn client<E: std::error::Error>(
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

    fn request<T>(&self, message: T, option: &RPCOption) -> Request<T> {
        let mut request = Request::new(message);
        request.set_timeout(option.hard_ttl());
        request
            .metadata_mut()
            .insert(TIMING_METADATA, self.shared.timing.clone());
        request
    }

    /// A reply must come from the node that was dialed, for this group.
    fn check_reply(&self, header: Option<&RaftRpcHeader>) -> Result<(), Status> {
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

    fn failure<E: std::error::Error>(
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
        let client = self.client()?;
        let reply = client
            .append_entries(request)
            .await
            .map_err(|status| self.failure(RPCTypes::AppendEntries, ttl, status))?
            .into_inner();
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
        let client = self.client()?;
        let reply = client
            .install_snapshot(request)
            .await
            .map_err(|status| self.failure(RPCTypes::InstallSnapshot, ttl, status))?
            .into_inner();
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
        let client = self.client()?;
        let reply = client
            .vote(request)
            .await
            .map_err(|status| self.failure(RPCTypes::Vote, ttl, status))?
            .into_inner();
        self.check_reply(reply.header.as_ref())
            .and_then(|()| vote_response_from_proto(reply))
            .map_err(|status| Self::network(status.message().to_string()))
    }
}

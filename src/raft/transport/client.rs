//! The caller side: the network factory and the lease forwarder.

use super::*;

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

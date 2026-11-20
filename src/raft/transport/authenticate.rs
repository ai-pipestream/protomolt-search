//! The listener's check of who is calling.

use super::*;

impl RaftTransportService {
    /// The peer behind a request: the verified client certificate names a
    /// registered node, and the header agrees with it, this group and this
    /// node. Returns the sender's id.
    pub(super) fn authenticate<T>(
        &self,
        request: &Request<T>,
        header: Option<&RaftRpcHeader>,
    ) -> Result<NodeId, Status> {
        let registered = self.certified_node(request)?;
        self.check_header(header, registered)?;
        self.check_heard(registered)?;
        self.check_timing(request)?;
        Ok(registered)
    }

    /// The registered node the verified client certificate names.
    fn certified_node<T>(&self, request: &Request<T>) -> Result<NodeId, Status> {
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
        let fingerprint = turbo_sha256::sha256(leaf.as_ref()).0;
        let registered = self.directory.node_of(&fingerprint).ok_or_else(|| {
            reasons::reason(
                Status::unauthenticated(
                    "client certificate is not registered to any member of this group",
                ),
                reasons::TRANSPORT_UNREGISTERED_CERTIFICATE,
            )
        })?;
        Ok(registered)
    }

    /// The header agrees with this protocol, this group, this node, and
    /// the certificate's node.
    fn check_header(
        &self,
        header: Option<&RaftRpcHeader>,
        registered: NodeId,
    ) -> Result<(), Status> {
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
        Ok(())
    }

    /// A test partition can cut the peer off in either direction.
    fn check_heard(&self, registered: NodeId) -> Result<(), Status> {
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
        Ok(())
    }

    /// Election and lease timing must agree across the group.
    fn check_timing<T>(&self, request: &Request<T>) -> Result<(), Status> {
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
        Ok(())
    }
}

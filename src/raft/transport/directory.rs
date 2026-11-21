//! Who a peer is: the certificate directory, the transport limits, and
//! the test partitions.

use super::*;

impl PeerDirectory {
    pub fn new(group: &SourceAuthorityIdentity) -> Self {
        Self {
            group: group.clone(),
            peers: RwLock::new(BTreeMap::new()),
        }
    }

    pub fn group(&self) -> &SourceAuthorityIdentity {
        &self.group
    }

    /// Bind a node id to the SHA-256 of its certificate's DER. A second
    /// certificate for a known node, or a certificate already bound to
    /// another node, refuses.
    pub fn register(&self, node_id: NodeId, certificate_sha256: [u8; 32]) -> Result<(), Status> {
        let mut peers = self
            .peers
            .write()
            .map_err(|_| Status::internal("peer directory lock poisoned"))?;
        if let Some((other, _)) = peers
            .iter()
            .find(|(id, fingerprint)| **fingerprint == certificate_sha256 && **id != node_id)
        {
            return Err(Status::already_exists(format!(
                "certificate is already registered to node {other}"
            )));
        }
        match peers.get(&node_id) {
            Some(existing) if *existing != certificate_sha256 => Err(Status::already_exists(
                format!("node {node_id} is already registered with another certificate"),
            )),
            _ => {
                peers.insert(node_id, certificate_sha256);
                Ok(())
            }
        }
    }

    pub fn knows(&self, node_id: NodeId) -> bool {
        self.peers
            .read()
            .map(|peers| peers.contains_key(&node_id))
            .unwrap_or(false)
    }

    pub(super) fn node_of(&self, certificate_sha256: &[u8; 32]) -> Option<NodeId> {
        self.peers.read().ok().and_then(|peers| {
            peers
                .iter()
                .find(|(_, fingerprint)| *fingerprint == certificate_sha256)
                .map(|(id, _)| *id)
        })
    }
}

/// The SHA-256 of the first certificate in a PEM file: the value the
/// directory binds a node to. The file must hold exactly one certificate.
pub fn certificate_sha256(pem: &[u8]) -> Result<[u8; 32], Status> {
    use rustls_pki_types::pem::PemObject;
    let mut certificates = rustls_pki_types::CertificateDer::pem_slice_iter(pem);
    let first = certificates
        .next()
        .ok_or_else(|| Status::invalid_argument("PEM holds no certificate"))?
        .map_err(|e| Status::invalid_argument(format!("PEM certificate: {e}")))?;
    if certificates.next().is_some() {
        return Err(Status::invalid_argument(
            "PEM holds more than one certificate; a member identity is one leaf",
        ));
    }
    Ok(turbo_sha256::sha256(first.as_ref()).0)
}

impl Default for TransportLimits {
    fn default() -> Self {
        Self {
            max_message_bytes: 16 << 20,
            connect_timeout_ms: 2_000,
            snapshot_idle_timeout_ms: 30_000,
        }
    }
}

impl TransportLimits {
    /// The bound must hold one snapshot chunk with its envelope.
    pub fn validate(&self, snapshot_chunk_bytes: u64) -> Result<(), Status> {
        if self.max_message_bytes < 64 << 10 {
            return Err(Status::invalid_argument(
                "max_message_bytes must be at least 64 KiB",
            ));
        }
        if (self.max_message_bytes as u64) < snapshot_chunk_bytes.saturating_add(64 << 10) {
            return Err(Status::invalid_argument(format!(
                "max_message_bytes {} cannot carry a {} byte snapshot chunk with its envelope",
                self.max_message_bytes, snapshot_chunk_bytes
            )));
        }
        if self.connect_timeout_ms == 0 {
            return Err(Status::invalid_argument(
                "connect_timeout_ms must be positive",
            ));
        }
        if self.snapshot_idle_timeout_ms == 0 {
            return Err(Status::invalid_argument(
                "snapshot_idle_timeout_ms must be positive",
            ));
        }
        Ok(())
    }
}

impl Isolation {
    pub(super) fn blocks(&self, peer: NodeId) -> bool {
        self.blocked
            .read()
            .map(|blocked| blocked.contains(&peer))
            .unwrap_or(true)
    }

    /// Whether this node refuses what `peer` sends it, at the listener.
    pub(super) fn deaf_to(&self, peer: NodeId) -> bool {
        self.deaf
            .read()
            .map(|deaf| deaf.contains(&peer))
            .unwrap_or(true)
    }

    /// Refuse every RPC to and from these peers until `heal`.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn isolate(&self, peers: impl IntoIterator<Item = NodeId>) {
        self.blocked.write().unwrap().extend(peers);
    }

    /// Refuse every RPC from these peers at the listener while this node
    /// still calls them: the node falls behind and can still ask.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn isolate_inbound(&self, peers: impl IntoIterator<Item = NodeId>) {
        self.deaf.write().unwrap().extend(peers);
    }

    #[cfg(any(test, feature = "fault-injection"))]
    pub fn heal(&self) {
        self.blocked.write().unwrap().clear();
        self.deaf.write().unwrap().clear();
    }
}

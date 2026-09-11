//! Operator surfaces of a networked member (docs/raft-hosting.md,
//! "Operator configuration"): the peers file that binds node ids to
//! certificates, and the host and transport built from `--raft-*`
//! configuration. Nothing here creates a group; `raft-prepare` creates the
//! durable state of one member and serving opens existing state only.
use super::transport::{certificate_sha256, PeerDirectory, TransportLimits};
use super::{ClusterTransport, HostConfig};
use crate::config::RaftMemberConfig;
use crate::pb::storage::SourceAuthorityIdentity;
use crate::security::{ClientTls, ServerTls};
use std::path::Path;
use std::sync::Arc;

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PeersFile {
    #[serde(default)]
    peers: Vec<PeerEntry>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PeerEntry {
    node_id: u64,
    /// PEM with exactly one certificate; relative to the peers file.
    certificate: String,
}

/// The peers file: `[[peers]]` entries of `node_id` and `certificate`.
/// Every member of the group is listed, this node included; a duplicate
/// node id or a certificate bound twice refuses.
pub fn load_peer_directory(
    path: &Path,
    group: &SourceAuthorityIdentity,
) -> Result<Arc<PeerDirectory>, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("read raft peers {}: {e}", path.display()))?;
    let file: PeersFile =
        toml::from_str(&text).map_err(|e| format!("parse raft peers {}: {e}", path.display()))?;
    if file.peers.is_empty() {
        return Err(format!("raft peers {} lists no members", path.display()));
    }
    let base = path.parent().unwrap_or(Path::new("."));
    let directory = PeerDirectory::new(group);
    for entry in &file.peers {
        let certificate = base.join(&entry.certificate);
        let pem = std::fs::read(&certificate).map_err(|e| {
            format!(
                "read certificate {} of raft node {}: {e}",
                certificate.display(),
                entry.node_id
            )
        })?;
        let fingerprint = certificate_sha256(&pem).map_err(|e| {
            format!(
                "certificate {} of raft node {}: {}",
                certificate.display(),
                entry.node_id,
                e.message()
            )
        })?;
        if directory.knows(entry.node_id) {
            return Err(format!(
                "raft peers {} lists node {} twice",
                path.display(),
                entry.node_id
            ));
        }
        directory
            .register(entry.node_id, fingerprint)
            .map_err(|e| format!("raft peers {}: {}", path.display(), e.message()))?;
    }
    Ok(Arc::new(directory))
}

pub fn identity(cfg: &RaftMemberConfig) -> SourceAuthorityIdentity {
    SourceAuthorityIdentity {
        format_version: 1,
        group_id: cfg.group_id.clone(),
        authority_incarnation: cfg.authority_incarnation.clone(),
    }
}

pub fn host_config(cfg: &RaftMemberConfig) -> Result<HostConfig, String> {
    let config = HostConfig {
        heartbeat_interval_ms: cfg.heartbeat_ms,
        election_timeout_min_ms: cfg.election_min_ms,
        election_timeout_max_ms: cfg.election_max_ms,
        admission_lease_ms: cfg.lease_ms,
        clock_skew_ms: cfg.skew_ms,
        max_snapshot_bytes: cfg.max_snapshot_bytes,
        ..HostConfig::default()
    };
    config
        .validate()
        .map_err(|e| format!("raft timing: {}", e.message()))?;
    Ok(config)
}

/// The transport of this member: its listener identity with the cluster
/// CA required of every peer, its own client identity, and the directory.
/// A listener without a client CA or a client without an identity cannot
/// bind peer identity and refuses.
pub fn cluster_transport(
    cfg: &RaftMemberConfig,
    server_tls: Option<&ServerTls>,
    client_tls: Option<&ClientTls>,
) -> Result<ClusterTransport, String> {
    let server_tls = server_tls
        .ok_or("a raft member needs --tls-cert and --tls-key for its listener")?
        .clone();
    if server_tls.client_ca_pem.is_none() {
        return Err(
            "a raft member needs --tls-client-ca: peers are authenticated by certificate"
                .to_string(),
        );
    }
    let client_tls = client_tls
        .ok_or("a raft member needs --tls-ca with --tls-client-cert and --tls-client-key")?
        .clone();
    if client_tls.identity_pem.is_none() {
        return Err(
            "a raft member needs --tls-client-cert and --tls-client-key: its own certificate is its identity"
                .to_string(),
        );
    }
    let directory = load_peer_directory(&cfg.peers, &identity(cfg))?;
    if !directory.knows(cfg.node_id) {
        return Err(format!(
            "raft peers {} does not list this node ({})",
            cfg.peers.display(),
            cfg.node_id
        ));
    }
    Ok(ClusterTransport {
        directory,
        server_tls,
        client_tls,
        listen: cfg.listen,
        advertise: cfg.advertise.clone(),
        limits: TransportLimits::default(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixtures() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/certs/raft")
    }

    fn group() -> SourceAuthorityIdentity {
        SourceAuthorityIdentity {
            format_version: 1,
            group_id: vec![9; 16],
            authority_incarnation: vec![10; 16],
        }
    }

    fn peers_file(name: &str, body: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "raft-peers-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&dir).unwrap();
        let path = dir.join("peers.toml");
        std::fs::write(&path, body).unwrap();
        path
    }

    #[test]
    fn a_peers_file_binds_node_ids_to_certificates_and_refuses_duplicates() {
        let certs = fixtures();
        let body = format!(
            "[[peers]]\nnode_id = 1\ncertificate = \"{c}/node-1.pem\"\n\n[[peers]]\nnode_id = 2\ncertificate = \"{c}/node-2.pem\"\n",
            c = certs.display()
        );
        let directory = load_peer_directory(&peers_file("ok", &body), &group()).unwrap();
        assert!(directory.knows(1) && directory.knows(2) && !directory.knows(3));
        assert_eq!(directory.group(), &group());

        let twice = format!(
            "[[peers]]\nnode_id = 1\ncertificate = \"{c}/node-1.pem\"\n\n[[peers]]\nnode_id = 1\ncertificate = \"{c}/node-2.pem\"\n",
            c = certs.display()
        );
        let error = load_peer_directory(&peers_file("twice", &twice), &group())
            .err()
            .unwrap();
        assert!(error.contains("twice"), "{error}");

        let rebound = format!(
            "[[peers]]\nnode_id = 1\ncertificate = \"{c}/node-1.pem\"\n\n[[peers]]\nnode_id = 2\ncertificate = \"{c}/node-1.pem\"\n",
            c = certs.display()
        );
        let error = load_peer_directory(&peers_file("rebound", &rebound), &group())
            .err()
            .unwrap();
        assert!(error.contains("already registered"), "{error}");

        let error = load_peer_directory(&peers_file("empty", ""), &group())
            .err()
            .unwrap();
        assert!(error.contains("no members"), "{error}");

        let unknown = "[[peers]]\nnode_id = 1\ncertificate = \"x.pem\"\naddr = \"nowhere\"\n";
        let error = load_peer_directory(&peers_file("unknown", unknown), &group())
            .err()
            .unwrap();
        assert!(error.contains("parse raft peers"), "{error}");

        let missing = "[[peers]]\nnode_id = 1\ncertificate = \"missing.pem\"\n";
        let error = load_peer_directory(&peers_file("missing", missing), &group())
            .err()
            .unwrap();
        assert!(error.contains("read certificate"), "{error}");
    }

    #[test]
    fn the_transport_needs_a_client_ca_and_an_own_identity() {
        let certs = fixtures();
        let pem = |name: &str| std::fs::read(certs.join(name)).unwrap();
        let body = format!(
            "[[peers]]\nnode_id = 1\ncertificate = \"{c}/node-1.pem\"\n",
            c = certs.display()
        );
        let cfg = RaftMemberConfig {
            dir: std::path::PathBuf::from("/nonexistent"),
            node_id: 1,
            group_id: vec![9; 16],
            authority_incarnation: vec![10; 16],
            listen: "127.0.0.1:0".parse().unwrap(),
            advertise: None,
            peers: peers_file("transport", &body),
            heartbeat_ms: 250,
            election_min_ms: 1_000,
            election_max_ms: 2_000,
            lease_ms: 500,
            skew_ms: 250,
            max_snapshot_bytes: 4 << 30,
            map: None,
            managed_catalogs: Vec::new(),
            managed_principal: None,
        };
        let server = ServerTls {
            cert_pem: pem("node-1.pem"),
            key_pem: pem("node-1.key.pem"),
            client_ca_pem: Some(pem("ca.pem")),
        };
        let client = ClientTls {
            ca_pem: pem("ca.pem"),
            identity_pem: Some((pem("node-1.pem"), pem("node-1.key.pem"))),
            domain: None,
        };
        assert!(host_config(&cfg).is_ok());
        let error = cluster_transport(&cfg, None, Some(&client)).err().unwrap();
        assert!(error.contains("--tls-cert"), "{error}");
        let no_ca = ServerTls {
            client_ca_pem: None,
            ..server.clone()
        };
        let error = cluster_transport(&cfg, Some(&no_ca), Some(&client))
            .err()
            .unwrap();
        assert!(error.contains("--tls-client-ca"), "{error}");
        let anonymous = ClientTls {
            identity_pem: None,
            ..client.clone()
        };
        let error = cluster_transport(&cfg, Some(&server), Some(&anonymous))
            .err()
            .unwrap();
        assert!(error.contains("--tls-client-cert"), "{error}");
        let transport = cluster_transport(&cfg, Some(&server), Some(&client)).unwrap();
        assert!(transport.directory.knows(1));
        let mut stranger = cfg.clone();
        stranger.node_id = 2;
        let error = cluster_transport(&stranger, Some(&server), Some(&client))
            .err()
            .unwrap();
        assert!(error.contains("does not list this node"), "{error}");
        let mut bad_timing = cfg.clone();
        bad_timing.lease_ms = 2_000;
        let error = host_config(&bad_timing).err().unwrap();
        assert!(error.contains("raft timing"), "{error}");
    }
}

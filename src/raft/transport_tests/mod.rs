//! Multi-node evidence over the tonic transport: peer identity binding,
//! three voters seeded by snapshot, leader isolation with revocation on
//! the surviving quorum, member restart and voter replacement. Every node
//! runs in this process on loopback with the Raft fixtures under
//! `tests/certs/raft`.
mod identity;
mod leases;
mod quorum;
mod snapshot;

use super::tests::{config, identity, limits, owner, prepare, Directory};
use super::transport::{certificate_sha256, PeerDirectory, TransportLimits};
use super::types::NodeId;
use super::{ClusterTransport, HostConfig, RaftHost};
use crate::pb::storage::{
    raft_transport_client::RaftTransportClient, source_authority_command::Action, RaftRpcHeader,
    RaftVote, RaftVoteRequest, ReplaceSourceCollectionGrants, SourceAuthorityCommand,
    SourceAuthorityIdentity,
};
use crate::pb::{AccessAction, AccessPolicy, CollectionGrant};
use crate::security::{apply_client_tls, ClientTls, ServerTls};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tonic::Code;

/// The refusal carries the code and the registry reason it should.
fn assert_reason(error: &tonic::Status, code: Code, reason: &str) {
    assert_eq!(error.code(), code, "{error}");
    assert_eq!(super::reasons::reason_of(error), Some(reason), "{error}");
}

fn pem(name: &str) -> Vec<u8> {
    std::fs::read(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/certs/raft")
            .join(name),
    )
    .unwrap()
}

fn directory(group: &SourceAuthorityIdentity, nodes: &[NodeId]) -> Arc<PeerDirectory> {
    let directory = PeerDirectory::new(group);
    for node in nodes {
        directory
            .register(
                *node,
                certificate_sha256(&pem(&format!("node-{node}.pem"))).unwrap(),
            )
            .unwrap();
    }
    Arc::new(directory)
}

fn client_tls(cert: &str) -> ClientTls {
    ClientTls {
        ca_pem: pem("ca.pem"),
        identity_pem: Some((pem(&format!("{cert}.pem")), pem(&format!("{cert}.key.pem")))),
        domain: Some("localhost".into()),
    }
}

fn transport(node: NodeId, directory: &Arc<PeerDirectory>) -> ClusterTransport {
    ClusterTransport {
        directory: Arc::clone(directory),
        server_tls: ServerTls {
            cert_pem: pem(&format!("node-{node}.pem")),
            key_pem: pem(&format!("node-{node}.key.pem")),
            client_ca_pem: Some(pem("ca.pem")),
        },
        client_tls: client_tls(&format!("node-{node}")),
        listen: "127.0.0.1:0".parse().unwrap(),
        advertise: None,
        limits: TransportLimits {
            max_message_bytes: 1 << 20,
            connect_timeout_ms: 1_000,
            snapshot_idle_timeout_ms: 300,
        },
    }
}

/// The bootstrap policy: alice ingests and administers, bob administers,
/// so bob can revoke alice and still read the decision.
fn policy() -> AccessPolicy {
    let mut policy = super::tests::policy();
    policy.grants.push(CollectionGrant {
        principal: "bob".into(),
        workspace: "workspace-a".into(),
        collection: "books".into(),
        actions: vec![AccessAction::Admin as i32],
        ..Default::default()
    });
    policy
}

fn cluster_config() -> HostConfig {
    HostConfig {
        // Small chunks so a store image crosses several RPCs.
        snapshot_chunk_bytes: 128 << 10,
        install_snapshot_timeout_ms: 5_000,
        ..config()
    }
}

struct Cluster {
    group: SourceAuthorityIdentity,
    directory: Arc<PeerDirectory>,
    dirs: BTreeMap<NodeId, Directory>,
    hosts: BTreeMap<NodeId, RaftHost>,
    /// Control revision the next command must expect.
    revision: u64,
}

impl Cluster {
    fn host(&self, node: NodeId) -> &RaftHost {
        &self.hosts[&node]
    }

    async fn leader(&self) -> NodeId {
        self.leader_excluding(None).await
    }

    async fn leader_excluding(&self, excluded: Option<NodeId>) -> NodeId {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                for host in self.hosts.values() {
                    if Some(host.node_id()) == excluded
                        || host.metrics().borrow().state != openraft::ServerState::Leader
                    {
                        continue;
                    }
                    match host.test_leader_readiness().await {
                        Ok(())
                            if host.metrics().borrow().state == openraft::ServerState::Leader =>
                        {
                            return host.node_id()
                        }
                        Ok(()) => {}
                        Err(error) if error.code() == Code::Unavailable => {}
                        Err(error) => panic!("leader readiness failed: {error}"),
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("no quorum-confirmed leader within 10 s")
    }

    /// Find a quorum-confirmed leader among the surviving hosts, without
    /// granting any application lease on it.
    async fn leader_other_than(&self, not: NodeId) -> NodeId {
        self.leader_excluding(Some(not)).await
    }

    /// Propose a Prepare on the leader; the command expects the current
    /// control revision and the owner id doubles as the command id.
    async fn propose_prepare(&mut self, owner_id: &str) -> u64 {
        let leader = self.leader().await;
        let command = prepare(&self.group, owner_id, owner_id, self.revision);
        let decision = self
            .host(leader)
            .propose_command("alice", &command)
            .await
            .unwrap();
        assert_eq!(decision.code, 0, "{}", decision.message);
        self.revision += 1;
        let applied = self.host(leader).applied_position().unwrap().unwrap();
        applied.index
    }

    async fn wait_applied(&self, node: NodeId, index: u64) {
        self.host(node)
            .wait(Some(Duration::from_secs(30)))
            .applied_index_at_least(Some(index), "applied")
            .await
            .unwrap();
    }

    async fn join(&mut self, node: NodeId) {
        let dir = Directory::new(&format!("member-{node}"));
        RaftHost::prepare_member(&dir.0, &self.group, node, &policy(), &limits()).unwrap();
        let host = RaftHost::start_member(
            &dir.0,
            &self.group,
            node,
            &cluster_config(),
            transport(node, &self.directory),
        )
        .await
        .unwrap();
        assert!(host.awaiting_snapshot().unwrap());
        let addr = host.advertised_addr().unwrap().to_string();
        self.dirs.insert(node, dir);
        self.hosts.insert(node, host);
        // Preparing and starting a member may outlast an election interval.
        let leader = self.leader().await;
        self.host(leader).add_learner(node, &addr).await.unwrap();
    }

    /// Stop a member and start it again on the address the membership
    /// records for it.
    async fn restart(&mut self, node: NodeId) {
        let host = self.hosts.remove(&node).unwrap();
        let listen = host.listen_addr().unwrap();
        host.shutdown().await.unwrap();
        self.start(node, listen).await;
    }

    async fn start(&mut self, node: NodeId, listen: std::net::SocketAddr) {
        let mut transport = transport(node, &self.directory);
        transport.listen = listen;
        let host = RaftHost::start_member(
            &self.dirs[&node].0,
            &self.group,
            node,
            &cluster_config(),
            transport,
        )
        .await
        .unwrap();
        self.hosts.insert(node, host);
    }

    async fn shutdown(mut self) {
        for (_, host) in std::mem::take(&mut self.hosts) {
            host.shutdown().await.unwrap();
        }
    }
}

/// Bootstrap node 1 with one owner prepared, seed 2 and 3 from its
/// snapshot and promote them: a group of three voters.
async fn three_voters(name: &str) -> Cluster {
    let group = identity(9);
    let directory = directory(&group, &[1, 2, 3, 4]);
    let dir = Directory::new(&format!("{name}-1"));
    let host = RaftHost::bootstrap_cluster(
        &dir.0,
        &group,
        1,
        &policy(),
        &limits(),
        &cluster_config(),
        transport(1, &directory),
    )
    .await
    .unwrap();
    let mut cluster = Cluster {
        group,
        directory,
        dirs: BTreeMap::from([(1, dir)]),
        hosts: BTreeMap::from([(1, host)]),
        revision: 1,
    };
    cluster.propose_prepare("phone-a").await;
    cluster.join(2).await;
    cluster.join(3).await;
    cluster
        .host(1)
        .promote(BTreeSet::from([2, 3]))
        .await
        .unwrap();
    cluster
        .host(1)
        .wait(Some(Duration::from_secs(30)))
        .voter_ids([1, 2, 3], "three voters")
        .await
        .unwrap();
    cluster
}

fn header(group: &SourceAuthorityIdentity, from: NodeId, to: NodeId) -> RaftRpcHeader {
    RaftRpcHeader {
        protocol_version: 1,
        group: Some(group.clone()),
        from_node_id: from,
        to_node_id: to,
    }
}

fn vote_request(header: Option<RaftRpcHeader>) -> RaftVoteRequest {
    RaftVoteRequest {
        header,
        vote: Some(RaftVote {
            term: 1,
            node_id: 2,
            committed: false,
        }),
        last_log_id: None,
    }
}

/// A raw request with the timing agreement every peer must present.
fn timed<T>(message: T) -> tonic::Request<T> {
    let mut request = tonic::Request::new(message);
    request.metadata_mut().insert(
        super::transport::TIMING_METADATA,
        cluster_config().timing_agreement().parse().unwrap(),
    );
    request
}

async fn raw_client(
    addr: std::net::SocketAddr,
    cert: &str,
) -> Result<RaftTransportClient<tonic::transport::Channel>, tonic::transport::Error> {
    let endpoint = apply_client_tls(
        tonic::transport::Endpoint::from_shared(format!("https://{addr}"))
            .unwrap()
            .timeout(Duration::from_secs(5)),
        Some(&client_tls(cert)),
    )
    .unwrap();
    Ok(RaftTransportClient::new(endpoint.connect().await?))
}

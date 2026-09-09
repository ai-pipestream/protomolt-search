//! Multi-node evidence over the tonic transport: peer identity binding,
//! three voters seeded by snapshot, leader isolation with revocation on
//! the surviving quorum, member restart and voter replacement. Every node
//! runs in this process on loopback with the Raft fixtures under
//! `tests/certs/raft`.
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
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            for host in self.hosts.values() {
                if host.metrics().borrow().state == openraft::ServerState::Leader {
                    return host.node_id();
                }
            }
            assert!(Instant::now() < deadline, "no leader within 10 s");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Wait until some running host other than `not` believes in a leader
    /// that is not `not`; returns that leader.
    async fn leader_other_than(&self, not: NodeId) -> NodeId {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            for (id, host) in &self.hosts {
                if *id == not {
                    continue;
                }
                if let Some(leader) = host.believed_leader() {
                    if leader != not
                        && self.hosts[&leader].metrics().borrow().state
                            == openraft::ServerState::Leader
                    {
                        return leader;
                    }
                }
            }
            assert!(Instant::now() < deadline, "no other leader within 10 s");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Propose a Prepare on the leader; the command expects the current
    /// control revision and the owner id doubles as the command id.
    async fn propose_prepare(&mut self, leader: NodeId, owner_id: &str) -> u64 {
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

    async fn join(&mut self, node: NodeId, leader: NodeId) {
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
    cluster.propose_prepare(1, "phone-a").await;
    cluster.join(2, 1).await;
    cluster.join(3, 1).await;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn peer_identity_binds_certificate_group_and_node() {
    let group = identity(9);
    let directory = directory(&group, &[1, 2, 3]);
    let dir = Directory::new("identity-1");
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
    let addr = host.listen_addr().unwrap();

    // A registered member speaking as itself is answered by node 1.
    let mut node2 = raw_client(addr, "node-2").await.unwrap();
    let reply = node2
        .vote(timed(vote_request(Some(header(&group, 2, 1)))))
        .await
        .unwrap()
        .into_inner();
    let responder = reply.header.unwrap();
    assert_eq!((responder.from_node_id, responder.to_node_id), (1, 2));
    assert_eq!(responder.group.as_ref(), Some(&group));

    // The same certificate cannot speak for node 3, reach node 9 or name
    // another group; an unversioned or missing header is malformed.
    let refused = |request: RaftVoteRequest| {
        let mut client = node2.clone();
        async move { client.vote(timed(request)).await.unwrap_err() }
    };
    let error = refused(vote_request(Some(header(&group, 3, 1)))).await;
    assert_eq!(error.code(), Code::PermissionDenied, "{error}");
    assert!(error.message().contains("registered to node 2"), "{error}");
    let error = refused(vote_request(Some(header(&group, 2, 9)))).await;
    assert_eq!(error.code(), Code::PermissionDenied, "{error}");
    assert!(error.message().contains("addressed to node 9"), "{error}");
    let error = refused(vote_request(Some(header(&identity(8), 2, 1)))).await;
    assert_eq!(error.code(), Code::PermissionDenied, "{error}");
    assert!(error.message().contains("another group"), "{error}");
    let mut stale = header(&group, 2, 1);
    stale.protocol_version = 2;
    let error = refused(vote_request(Some(stale))).await;
    assert_eq!(error.code(), Code::InvalidArgument, "{error}");
    let error = refused(vote_request(None)).await;
    assert_eq!(error.code(), Code::InvalidArgument, "{error}");

    // A peer whose election or lease values differ, or that presents none,
    // is refused: the lease argument depends on every member holding the
    // same values.
    let error = node2
        .vote(vote_request(Some(header(&group, 2, 1))))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition, "{error}");
    assert!(error.message().contains("timing agreement"), "{error}");
    let mut other_timing = tonic::Request::new(vote_request(Some(header(&group, 2, 1))));
    other_timing.metadata_mut().insert(
        super::transport::TIMING_METADATA,
        "v1:heartbeat=50:election=150-300:lease=120:skew=50"
            .parse()
            .unwrap(),
    );
    let error = node2.vote(other_timing).await.unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition, "{error}");
    assert!(error.message().contains("differs"), "{error}");

    // A certificate the cluster CA issued but nobody registered is not a
    // member, whatever node id it claims.
    let mut node4 = raw_client(addr, "node-4").await.unwrap();
    let error = node4
        .vote(timed(vote_request(Some(header(&group, 4, 1)))))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::Unauthenticated, "{error}");
    let error = node4
        .vote(timed(vote_request(Some(header(&group, 2, 1)))))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::Unauthenticated, "{error}");

    // A certificate of another CA does not complete the handshake.
    match raw_client(addr, "stranger").await {
        Err(_) => {}
        Ok(mut stranger) => {
            let error = stranger
                .vote(timed(vote_request(Some(header(&group, 2, 1)))))
                .await
                .unwrap_err();
            // The handshake fails after the lazy connect: the stream is
            // cut before any service sees the call.
            assert!(
                matches!(
                    error.code(),
                    Code::Unavailable | Code::Unknown | Code::Cancelled | Code::Internal
                ),
                "{error}"
            );
        }
    }

    // A node without a registered certificate cannot be added, and the
    // directory refuses to rebind a certificate or a node.
    let error = host.add_learner(4, "127.0.0.1:1").await.unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition, "{error}");
    let error = directory
        .register(5, certificate_sha256(&pem("node-2.pem")).unwrap())
        .unwrap_err();
    assert_eq!(error.code(), Code::AlreadyExists, "{error}");
    let error = directory
        .register(2, certificate_sha256(&pem("node-4.pem")).unwrap())
        .unwrap_err();
    assert_eq!(error.code(), Code::AlreadyExists, "{error}");
    host.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_voters_replicate_and_learners_are_seeded_by_snapshot() {
    let mut cluster = three_voters("three").await;
    // Each member was seeded by the leader's verified image: the prepared
    // marker is gone, the owner row is there, and the generation is
    // published beside the member's store.
    for node in [2, 3] {
        let host = cluster.host(node);
        assert!(!host.awaiting_snapshot().unwrap());
        let store = host.store().unwrap();
        let row = store.owner("alice", &owner("phone-a")).unwrap();
        assert_eq!(row.control_revision, 2);
        assert!(super::state_machine::published_generation(
            &cluster.dirs[&node].0.join(super::host::SNAPSHOT_DIR)
        )
        .unwrap()
        .is_some());
    }
    // Entries proposed on the leader apply on every voter with the same
    // position and rows.
    let leader = cluster.leader().await;
    let index = cluster.propose_prepare(leader, "phone-b").await;
    for node in [1, 2, 3] {
        cluster.wait_applied(node, index).await;
        let store = cluster.host(node).store().unwrap();
        assert_eq!(
            store
                .owner("alice", &owner("phone-b"))
                .unwrap()
                .control_revision,
            3
        );
        assert_eq!(
            store
                .raft_applied()
                .unwrap()
                .unwrap()
                .last_applied
                .unwrap()
                .index,
            index
        );
    }
    // A follower does not propose; it names the leader.
    let follower = if leader == 1 { 2 } else { 1 };
    let error = cluster
        .host(follower)
        .propose_command(
            "alice",
            &prepare(&cluster.group, "phone-c", "phone-c", cluster.revision),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::Unavailable, "{error}");
    assert!(error.message().contains("not the leader"), "{error}");
    // A prepared member store refuses to apply anything before its seed.
    let pending = Directory::new("pending");
    RaftHost::prepare_member(&pending.0, &cluster.group, 4, &policy(), &limits()).unwrap();
    let store = crate::source_authority::SourceAuthorityStore::open(
        &pending.0.join(super::host::STORE_FILE),
        &cluster.group,
    )
    .unwrap();
    store.set_raft_hosted();
    let error = store
        .apply_raft_position(&crate::pb::storage::RaftApplied {
            format_version: 1,
            last_applied: Some(crate::pb::storage::RaftLogId {
                term: 1,
                node_id: 1,
                index: 1,
            }),
            membership: Some(crate::pb::storage::RaftStoredMembership {
                log_id: None,
                membership: Some(crate::pb::storage::RaftMembership::default()),
            }),
        })
        .unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition, "{error}");
    assert!(error.message().contains("awaits"), "{error}");
    cluster.shutdown().await;
}

fn revoke_alice(group: &SourceAuthorityIdentity, revision: u64) -> SourceAuthorityCommand {
    SourceAuthorityCommand {
        format_version: 1,
        authority: Some(group.clone()),
        key: Some(owner("")),
        command_id: b"revoke-alice".to_vec(),
        expected_control_revision: revision,
        expected_policy_revision: 1,
        expected_ownership_generation: 0,
        action: Some(Action::ReplaceGrants(ReplaceSourceCollectionGrants {
            grants: vec![CollectionGrant {
                principal: "bob".into(),
                workspace: "workspace-a".into(),
                collection: "books".into(),
                actions: vec![AccessAction::Admin as i32],
                ..Default::default()
            }],
        })),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_isolated_leader_admits_nothing_and_the_surviving_quorum_revokes() {
    let mut cluster = three_voters("isolation").await;
    let leader = cluster.leader().await;
    // A lease on the leader with a quorum admits alice's ingest.
    let granted = Instant::now();
    cluster
        .host(leader)
        .with_admission("alice", |admission| {
            admission.authorize("books", AccessAction::Ingest)
        })
        .await
        .unwrap();
    // Isolate the leader from both peers, both directions.
    let others: Vec<NodeId> = [1, 2, 3].into_iter().filter(|n| *n != leader).collect();
    cluster.host(leader).isolate(others.iter().copied());
    for other in &others {
        cluster.host(*other).isolate([leader]);
    }
    let successor = cluster.leader_other_than(leader).await;
    // No election finishes before the election floor, and the floor is
    // where every lease the old leader issued has expired.
    assert!(
        granted.elapsed() >= Duration::from_millis(config().election_timeout_min_ms),
        "successor elected after {:?}",
        granted.elapsed()
    );
    // The old leader collects no quorum: its admission refuses within the
    // read bound rather than admitting on its stale view.
    let error = cluster
        .host(leader)
        .with_admission("alice", |admission| {
            admission.authorize("books", AccessAction::Ingest)
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::Unavailable, "{error}");
    assert!(error.message().contains("linearizable"), "{error}");
    // The surviving quorum revokes alice.
    let decision = cluster
        .host(successor)
        .propose_command("bob", &revoke_alice(&cluster.group, cluster.revision))
        .await
        .unwrap();
    assert_eq!(decision.code, 0, "{}", decision.message);
    cluster.revision += 1;
    let revoked_at = cluster
        .host(successor)
        .applied_position()
        .unwrap()
        .unwrap()
        .index;
    // The old side still shows the stale grant, but grants nothing on it:
    // the local admission is closed and the leased one cannot be renewed.
    let stale = cluster.host(leader).store().unwrap();
    assert_eq!(
        stale
            .policy("alice", "workspace-a", "books")
            .unwrap()
            .revision,
        1
    );
    let error = match stale.admission("alice") {
        Ok(_) => panic!("a hosted store granted a local admission"),
        Err(error) => error,
    };
    assert_eq!(error.code(), Code::FailedPrecondition, "{error}");
    drop(stale);
    let error = cluster
        .host(leader)
        .with_admission("alice", |admission| {
            admission.authorize("books", AccessAction::Ingest)
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::Unavailable, "{error}");
    // Heal: the old leader catches up and is a follower of the successor.
    for host in cluster.hosts.values() {
        host.heal();
    }
    cluster.wait_applied(leader, revoked_at).await;
    let healed = cluster.host(leader).store().unwrap();
    assert_eq!(
        healed
            .policy("bob", "workspace-a", "books")
            .unwrap()
            .revision,
        2
    );
    drop(healed);
    let error = cluster
        .host(leader)
        .with_admission("alice", |admission| {
            admission.authorize("books", AccessAction::Ingest)
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::Unavailable, "{error}");
    // On the current leader the revocation is what a fresh admission sees.
    let error = cluster
        .host(successor)
        .with_admission("alice", |admission| {
            admission.authorize("books", AccessAction::Ingest)
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::PermissionDenied, "{error}");
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_restarts_and_replays_the_entries_it_missed() {
    let mut cluster = three_voters("restart").await;
    let leader = cluster.leader().await;
    let absent = if leader == 3 { 2 } else { 3 };
    let host = cluster.hosts.remove(&absent).unwrap();
    let listen = host.listen_addr().unwrap();
    host.shutdown().await.unwrap();
    cluster.propose_prepare(leader, "phone-b").await;
    let index = cluster.propose_prepare(leader, "phone-c").await;
    cluster.start(absent, listen).await;
    cluster.wait_applied(absent, index).await;
    let store = cluster.host(absent).store().unwrap();
    for (id, revision) in [("phone-a", 2), ("phone-b", 3), ("phone-c", 4)] {
        assert_eq!(
            store.owner("alice", &owner(id)).unwrap().control_revision,
            revision
        );
    }
    drop(store);
    // The restarted member takes part again: a leader restart elects a
    // successor among the three and proposals continue.
    cluster.restart(leader).await;
    let leader = cluster.leader().await;
    let index = cluster.propose_prepare(leader, "phone-d").await;
    for node in [1, 2, 3] {
        cluster.wait_applied(node, index).await;
    }
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_voter_is_replaced_through_membership() {
    let mut cluster = three_voters("replace").await;
    let leader = cluster.leader().await;
    let retiring = if leader == 3 { 2 } else { 3 };
    // Node 4 joins as a learner seeded by snapshot, becomes a voter, and
    // the retiring voter leaves through the same joint procedure.
    cluster.join(4, leader).await;
    assert!(!cluster.host(4).awaiting_snapshot().unwrap());
    cluster
        .host(leader)
        .promote(BTreeSet::from([4]))
        .await
        .unwrap();
    cluster.host(leader).remove_member(retiring).await.unwrap();
    let voters: BTreeSet<NodeId> = [1, 2, 3, 4]
        .into_iter()
        .filter(|n| *n != retiring)
        .collect();
    cluster
        .host(leader)
        .wait(Some(Duration::from_secs(30)))
        .voter_ids(voters.iter().copied(), "replaced voter")
        .await
        .unwrap();
    let index = cluster.propose_prepare(leader, "phone-b").await;
    for node in &voters {
        cluster.wait_applied(*node, index).await;
    }
    // The removed member is outside the group: it proposes nothing.
    let removed = cluster.hosts.remove(&retiring).unwrap();
    let error = removed
        .propose_command(
            "alice",
            &prepare(&cluster.group, "phone-x", "phone-x", cluster.revision),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::Unavailable, "{error}");
    removed.shutdown().await.unwrap();
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_grant_paused_after_the_barrier_is_refused_once_its_interval_elapsed() {
    let mut cluster = three_voters("paused-grant").await;
    let leader = cluster.leader().await;
    // An admission that pauses after its barrier and resumes within the
    // interval is granted, anchored at or before the barrier.
    let gate = cluster.host(leader).arm_grant_gate();
    let before = Instant::now();
    let host = cluster.hosts.remove(&leader).unwrap();
    let host = Arc::new(host);
    let task = {
        let host = Arc::clone(&host);
        tokio::spawn(async move {
            host.with_admission("alice", |admission| {
                let lease = *admission.lease().unwrap();
                admission
                    .authorize("books", AccessAction::Ingest)
                    .map(|_| lease)
            })
            .await
        })
    };
    gate.reached().await;
    let paused_at = Instant::now();
    tokio::time::sleep(Duration::from_millis(20)).await;
    gate.release();
    let lease = task.await.unwrap().unwrap();
    assert!(lease.anchor >= before && lease.anchor <= paused_at);
    assert!(lease.deadline() <= paused_at + Duration::from_millis(config().admission_lease_ms));

    // The same pause, held while the survivors elect a successor and
    // commit a revocation: resuming grants nothing, because the interval
    // anchored before the barrier has elapsed.
    let gate = host.arm_grant_gate();
    let task = {
        let host = Arc::clone(&host);
        tokio::spawn(async move {
            host.with_admission("alice", |admission| {
                admission.authorize("books", AccessAction::Ingest)
            })
            .await
        })
    };
    gate.reached().await;
    let others: Vec<NodeId> = [1, 2, 3].into_iter().filter(|n| *n != leader).collect();
    host.isolate(others.iter().copied());
    for other in &others {
        cluster.host(*other).isolate([leader]);
    }
    let successor = cluster.leader_other_than(leader).await;
    let decision = cluster
        .host(successor)
        .propose_command("bob", &revoke_alice(&cluster.group, cluster.revision))
        .await
        .unwrap();
    assert_eq!(decision.code, 0, "{}", decision.message);
    cluster.revision += 1;
    gate.release();
    let error = task.await.unwrap().unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition, "{error}");
    assert!(
        error.message().contains("elapsed before the grant"),
        "{error}"
    );
    host.heal();
    for other in &others {
        cluster.host(*other).heal();
    }
    Arc::try_unwrap(host)
        .ok()
        .expect("task finished")
        .shutdown()
        .await
        .unwrap();
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leader_withholds_its_vote_while_an_admission_interval_may_be_open() {
    let cluster = three_voters("vote-hold").await;
    let leader = cluster.leader().await;
    cluster
        .host(leader)
        .with_admission("alice", |admission| {
            admission.authorize("books", AccessAction::Ingest)
        })
        .await
        .unwrap();
    let held_until = Instant::now() + Duration::from_millis(config().election_timeout_max_ms);
    // A registered peer asks the leader for a vote in a higher term with a
    // log at least as long: the pinned library would grant it, the leader
    // withholds it while the interval may be open and stays leader.
    let candidate = if leader == 2 { 3 } else { 2 };
    let mut client = raw_client(
        cluster.host(leader).listen_addr().unwrap(),
        &format!("node-{candidate}"),
    )
    .await
    .unwrap();
    let metrics = cluster.host(leader).metrics().borrow().clone();
    let request = RaftVoteRequest {
        header: Some(header(&cluster.group, candidate, leader)),
        vote: Some(RaftVote {
            term: metrics.current_term + 1,
            node_id: candidate,
            committed: false,
        }),
        last_log_id: metrics
            .last_applied
            .as_ref()
            .map(super::types::log_id_to_proto),
    };
    let reply = client
        .vote(timed(request.clone()))
        .await
        .unwrap()
        .into_inner();
    assert!(!reply.vote_granted);
    assert_eq!(reply.vote.unwrap().term, metrics.current_term);
    assert_eq!(
        cluster.host(leader).metrics().borrow().state,
        openraft::ServerState::Leader
    );
    // Past the ceiling the request reaches the library, which answers for
    // itself; the leader's own answer is no longer withheld.
    tokio::time::sleep(
        held_until.saturating_duration_since(Instant::now()) + Duration::from_millis(20),
    )
    .await;
    let reply = client.vote(timed(request)).await.unwrap().into_inner();
    assert!(reply.vote.is_some());
    cluster.shutdown().await;
}

/// A snapshot transfer with no chunk for the idle timeout is dropped with
/// its bytes and its place is free: another peer under the same vote is
/// refused while the transfer is live and accepted once it has gone idle,
/// and its complete transfer installs on the running core.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_idle_snapshot_transfer_is_dropped_and_its_place_freed() {
    use crate::pb::storage::{
        raft_install_snapshot_response::Outcome as InstallOutcome, RaftInstallSnapshotRequest,
        RaftSnapshotMeta,
    };
    use prost::Message as _;
    let group = identity(9);
    let directory = directory(&group, &[1, 2, 3, 4]);
    let leader_dir = Directory::new("idle-leader");
    let leader = RaftHost::bootstrap_cluster(
        &leader_dir.0,
        &group,
        1,
        &policy(),
        &limits(),
        &cluster_config(),
        transport(1, &directory),
    )
    .await
    .unwrap();
    let decision = leader
        .propose_command("alice", &prepare(&group, "phone-a", "phone-a", 1))
        .await
        .unwrap();
    assert_eq!(decision.code, 0, "{}", decision.message);
    let applied = leader.applied_position().unwrap().unwrap().index;
    leader.trigger_snapshot().await.unwrap();
    leader
        .wait(Some(Duration::from_secs(30)))
        .metrics(
            |m| m.snapshot.is_some_and(|s| s.index >= applied),
            "leader snapshot",
        )
        .await
        .unwrap();
    let generation =
        super::state_machine::published_generation(&leader_dir.0.join(super::host::SNAPSHOT_DIR))
            .unwrap()
            .unwrap();
    let image = std::fs::read(generation.join("image.redb")).unwrap();
    let meta = RaftSnapshotMeta::decode(std::fs::read(generation.join("meta")).unwrap().as_slice())
        .unwrap();
    let member_dir = Directory::new("idle-member");
    RaftHost::prepare_member(&member_dir.0, &group, 2, &policy(), &limits()).unwrap();
    let member = RaftHost::start_member(
        &member_dir.0,
        &group,
        2,
        &cluster_config(),
        transport(2, &directory),
    )
    .await
    .unwrap();
    let addr = member.listen_addr().unwrap();
    let vote = {
        let v = leader.metrics().borrow().vote;
        crate::pb::storage::RaftVote {
            term: v.leader_id.term,
            node_id: v.leader_id.node_id,
            committed: v.committed,
        }
    };
    let chunk = |from: NodeId, offset: usize, data: &[u8], done: bool| {
        timed(RaftInstallSnapshotRequest {
            header: Some(header(&group, from, 2)),
            vote: Some(vote.clone()),
            meta: Some(meta.clone()),
            offset: offset as u64,
            data: data.to_vec(),
            done,
        })
    };
    // Node 4 begins and goes quiet after one chunk.
    let mut quiet = raw_client(addr, "node-4").await.unwrap();
    quiet
        .install_snapshot(chunk(4, 0, &image[..4096], false))
        .await
        .unwrap();
    // Node 1 is refused while that transfer is live: the same snapshot id
    // is bound to node 4.
    let mut sender = raw_client(addr, "node-1").await.unwrap();
    let error = sender
        .install_snapshot(chunk(1, 0, &image[..4096], false))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition, "{error}");
    assert!(error.message().contains("bound to node 4"), "{error}");
    // Past the idle timeout the quiet transfer is dropped and node 1's
    // transfer takes its place and completes; the core ran throughout.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut offset = 0;
    let chunks: Vec<&[u8]> = image.chunks(128 << 10).collect();
    for (i, piece) in chunks.iter().enumerate() {
        let reply = sender
            .install_snapshot(chunk(1, offset, piece, i + 1 == chunks.len()))
            .await
            .unwrap()
            .into_inner();
        assert!(matches!(reply.outcome, Some(InstallOutcome::Vote(_))));
        offset += piece.len();
    }
    assert!(member.metrics().borrow().running_state.is_ok());
    assert!(!member.awaiting_snapshot().unwrap());
    assert_eq!(member.applied_position().unwrap().unwrap().index, applied);
    // The quiet peer's stale continuation is out of order for nothing: no
    // transfer is open, so it is a mismatch, not a refusal of the core.
    let reply = quiet
        .install_snapshot(chunk(4, 4096, &image[4096..8192], false))
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(reply.outcome, Some(InstallOutcome::Mismatch(_))));
    let incoming = std::fs::read_dir(member_dir.0.join(super::host::SNAPSHOT_DIR))
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("incoming-")
        })
        .count();
    assert_eq!(incoming, 0);
    member.shutdown().await.unwrap();
    leader.shutdown().await.unwrap();
}

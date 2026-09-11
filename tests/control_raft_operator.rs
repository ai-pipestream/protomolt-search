//! The Raft operator surface (`docs/raft-hosting.md`, "Operator
//! surface"): member status, membership changes and image verification
//! over each member's listener under cluster trust, the rejection
//! numbers on the status route, the metrics snapshot and the log line,
//! and the local `raft-bootstrap` as a child process.
//!
//! The oracle is `MUSE-BRIEF-RAFT-OPERATOR-SURFACE-2026-09-11.md` A.5:
//! every test below is one of its bullets. Compiled only with `raft`,
//! `tls` and `fault-injection` together (the `control_raft_hosted_writes`
//! convention: the group needs the mTLS transport, and the log-line
//! assertion needs the rejection ring).
#![cfg(all(feature = "raft", feature = "tls", feature = "fault-injection"))]

mod control_adversarial;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use control_adversarial::{kit, raft_kit};
use pipestream_search::diagnostics::{CoordinatorDiagnostics, RecentRing};
use pipestream_search::pb::diagnostics_service_client::DiagnosticsServiceClient;
use pipestream_search::pb::raft_operator_service_client::RaftOperatorServiceClient;
use pipestream_search::pb::{
    AddLearnerRequest, GetMemberStatusRequest, MemberStatus, MetricsSnapshotRequest,
    PromoteLearnersRequest, RemoveMemberRequest, VerifyPublishedImageRequest,
};
use pipestream_search::raft::operator_service::RaftOperatorServiceImpl;
use pipestream_search::raft::transport::certificate_sha256;
use pipestream_search::raft::RaftHost;
use pipestream_search::security::{apply_client_tls, ClientTls, ServerTls};
use pipestream_search::test_support::ForkGuarded;
use tokio::net::TcpListener;
use tonic::transport::{Channel, Endpoint, Server};
use tonic::Code;

use raft_kit::Voters;

/// Target-wide serialization: the tests share timing-sensitive election
/// windows and loopback listeners, so each holds this lock whole-body.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn fixtures() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/certs/raft")
}

fn pem(name: &str) -> Vec<u8> {
    let path = fixtures().join(name);
    std::fs::read(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from(b"0123456789abcdef"[usize::from(byte >> 4)]));
        out.push(char::from(b"0123456789abcdef"[usize::from(byte & 15)]));
    }
    out
}

/// The operator listener's TLS: this member's certificate, trusting the
/// cluster CA for client certificates without demanding one at the
/// handshake (the service demands it per call, the way the coordinator
/// listener lets cluster control demand it).
fn server_tls(cert: &str) -> ServerTls {
    ServerTls {
        cert_pem: pem(&format!("{cert}.pem")),
        key_pem: pem(&format!("{cert}.key.pem")),
        client_ca_pem: Some(pem("ca.pem")),
    }
}

/// The operator client: the cluster CA with `identity`'s certificate,
/// or the CA alone for the call without one.
fn operator_client(addr: &str, identity: Option<&str>) -> RaftOperatorServiceClient<Channel> {
    let tls = ClientTls {
        ca_pem: pem("ca.pem"),
        identity_pem: identity
            .map(|name| (pem(&format!("{name}.pem")), pem(&format!("{name}.key.pem")))),
        domain: Some("localhost".to_string()),
    };
    let endpoint =
        apply_client_tls(Endpoint::from_shared(addr.to_string()).unwrap(), Some(&tls)).unwrap();
    RaftOperatorServiceClient::new(endpoint.connect_lazy())
        .max_decoding_message_size(pipestream_search::MAX_MESSAGE_BYTES)
        .max_encoding_message_size(pipestream_search::MAX_MESSAGE_BYTES)
}

struct Served {
    addr: String,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

impl Served {
    async fn stop(mut self) {
        self.shutdown.take().unwrap().send(()).unwrap();
        match tokio::time::timeout(Duration::from_secs(10), &mut self.task).await {
            Ok(result) => result.unwrap().unwrap(),
            Err(_) => {
                self.task.abort();
                let _ = self.task.await;
                panic!("operator server did not release its connections");
            }
        }
    }
}

/// Serve `host`'s operator service on loopback mTLS as `cert`,
/// demanding a client certificate on every call.
async fn serve_operator(host: Arc<RaftHost>, cert: &str) -> Served {
    let service = RaftOperatorServiceImpl::new(host).with_client_cert_required(true);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("https://{}", listener.local_addr().unwrap());
    let (shutdown, stopping) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(
        Server::builder()
            .tls_config(server_tls(cert).server_config(false))
            .unwrap()
            .add_service(service.into_server())
            .serve_with_incoming_shutdown(
                pipestream_search::harness::nodelay_incoming(listener),
                async move {
                    let _ = stopping.await;
                },
            ),
    );
    Served {
        addr,
        shutdown: Some(shutdown),
        task,
    }
}

/// Serve the leader's diagnostics with its member gauges, so the test
/// reads the same snapshot the dashboard does.
async fn serve_diagnostics(host: Arc<RaftHost>, cert: &str) -> Served {
    let service = CoordinatorDiagnostics::new(Vec::new(), None, Arc::new(RecentRing::default()))
        .with_member_gauges(vec![RaftHost::member_gauge_provider(host)]);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("https://{}", listener.local_addr().unwrap());
    let (shutdown, stopping) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(
        Server::builder()
            .tls_config(server_tls(cert).server_config(false))
            .unwrap()
            .add_service(service.into_server(pipestream_search::MAX_MESSAGE_BYTES))
            .serve_with_incoming_shutdown(
                pipestream_search::harness::nodelay_incoming(listener),
                async move {
                    let _ = stopping.await;
                },
            ),
    );
    Served {
        addr,
        shutdown: Some(shutdown),
        task,
    }
}

fn diagnostics_client(addr: &str, identity: &str) -> DiagnosticsServiceClient<Channel> {
    let tls = ClientTls {
        ca_pem: pem("ca.pem"),
        identity_pem: Some((
            pem(&format!("{identity}.pem")),
            pem(&format!("{identity}.key.pem")),
        )),
        domain: Some("localhost".to_string()),
    };
    let endpoint =
        apply_client_tls(Endpoint::from_shared(addr.to_string()).unwrap(), Some(&tls)).unwrap();
    DiagnosticsServiceClient::new(endpoint.connect_lazy())
}

/// Take `node` out of the voters to wrap in an `Arc` for serving.
fn take_host(voters: &mut Voters, node: u64) -> Arc<RaftHost> {
    Arc::new(voters.take(node))
}

/// Return a served host: no server still holds it.
fn return_host(voters: &mut Voters, node: u64, host: Arc<RaftHost>) {
    let host = Arc::try_unwrap(host)
        .ok()
        .expect("no server still holds the host");
    voters.insert(node, host);
}

/// The voters of `status`'s membership, in order.
fn voter_ids(status: &MemberStatus) -> Vec<u64> {
    status
        .membership
        .as_ref()
        .unwrap()
        .configs
        .first()
        .unwrap()
        .node_ids
        .clone()
}

/// Every known node of `status`'s membership: id and address.
fn member_nodes(status: &MemberStatus) -> Vec<(u64, String)> {
    status
        .membership
        .as_ref()
        .unwrap()
        .nodes
        .iter()
        .map(|node| (node.node_id, node.addr.clone()))
        .collect()
}

async fn get_status(client: &mut RaftOperatorServiceClient<Channel>) -> MemberStatus {
    client
        .get_member_status(GetMemberStatusRequest {})
        .await
        .unwrap()
        .into_inner()
}

// ---------------------------------------------------------------------------
// The leader and a follower each answer; the leader leads, the follower
// names the leader; the membership lists three voters with addresses;
// the applied position is the store's.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_reports_its_status_over_the_operator_route() {
    let _serial = serial();
    let mut voters = raft_kit::three_voters("operator-status", &kit::policy()).await;
    let leader = voters.leader().await;
    let follower = [1, 2, 3].into_iter().find(|node| *node != leader).unwrap();

    let leader_host = take_host(&mut voters, leader);
    let follower_host = take_host(&mut voters, follower);
    let leader_server = serve_operator(Arc::clone(&leader_host), &format!("node-{leader}")).await;
    let follower_server =
        serve_operator(Arc::clone(&follower_host), &format!("node-{follower}")).await;

    let status = get_status(&mut operator_client(&leader_server.addr, Some("node-1"))).await;
    assert_eq!(status.node_id, leader);
    assert_eq!(status.group_id, hex(&voters.group.group_id));
    assert_eq!(
        status.authority_incarnation,
        hex(&voters.group.authority_incarnation)
    );
    assert!(status.leads);
    assert_eq!(status.applied, leader_host.applied_position().unwrap());
    assert!(status.vote.is_some());
    assert!(status.last_log_index.is_some());
    assert_eq!(voter_ids(&status), vec![1, 2, 3]);
    let nodes = member_nodes(&status);
    assert_eq!(
        nodes.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert!(nodes.iter().all(|(_, addr)| !addr.is_empty()));

    let follower_status =
        get_status(&mut operator_client(&follower_server.addr, Some("node-1"))).await;
    assert_eq!(follower_status.node_id, follower);
    assert!(!follower_status.leads);
    assert_eq!(follower_status.believed_leader, Some(leader));
    assert_eq!(
        follower_status.applied,
        follower_host.applied_position().unwrap()
    );
    assert_eq!(voter_ids(&follower_status), vec![1, 2, 3]);

    let verification = operator_client(&leader_server.addr, Some("node-1"))
        .verify_published_image(VerifyPublishedImageRequest {})
        .await
        .unwrap()
        .into_inner();
    assert!(
        verification.generation.is_some(),
        "three seeded voters publish an image"
    );

    leader_server.stop().await;
    follower_server.stop().await;
    return_host(&mut voters, leader, leader_host);
    return_host(&mut voters, follower, follower_host);
    voters.shutdown().await;
}

/// One accepted write under a fresh owner: the group still serves after
/// each membership step. `revision` is the voter's revision tracker,
/// kept here because the hosts are out of the map while they serve.
async fn write_ok(
    leader: &RaftHost,
    revision: &mut u64,
    group: &pipestream_search::pb::storage::SourceAuthorityIdentity,
    tag: &str,
) {
    let mut key = kit::owner_key();
    key.owner_id = format!("operator-write-{tag}").into_bytes();
    let command = kit::source_command(
        group,
        &key,
        &format!("operator-write-{tag}"),
        *revision,
        1,
        0,
        kit::prepare_action(b"operator-write"),
    );
    let decision = leader.propose_command("alice", &command).await.unwrap();
    assert_eq!(decision.code, 0, "{}", decision.message);
    *revision += 1;
}

/// The voters of a membership change reply, in order.
fn voter_ids_of(change: &pipestream_search::pb::MembershipChange) -> Vec<u64> {
    change
        .membership
        .as_ref()
        .unwrap()
        .configs
        .first()
        .unwrap()
        .node_ids
        .clone()
}

/// Poll the leader's status until the voter set is `voters`.
async fn wait_voters(
    client: &mut RaftOperatorServiceClient<Channel>,
    voters: Vec<u64>,
) -> MemberStatus {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let status = get_status(client).await;
        if voter_ids(&status) == voters {
            return status;
        }
        assert!(Instant::now() < deadline, "voters never became {voters:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

// ---------------------------------------------------------------------------
// Node 4 is prepared, added by RPC (a learner), promoted by RPC (a
// voter), removed by RPC (absent); the group accepts a write after each
// step.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_learner_is_added_promoted_and_removed_over_the_operator_route() {
    let _serial = serial();
    let mut voters = raft_kit::three_voters("operator-learner", &kit::policy()).await;
    let leader = voters.leader().await;
    let mut revision = voters.revision;

    let leader_host = take_host(&mut voters, leader);
    let others: Vec<u64> = [1, 2, 3]
        .into_iter()
        .filter(|node| *node != leader)
        .collect();
    let other_hosts: Vec<Arc<RaftHost>> = others
        .iter()
        .map(|node| take_host(&mut voters, *node))
        .collect();
    let leader_server = serve_operator(Arc::clone(&leader_host), &format!("node-{leader}")).await;
    let mut client = operator_client(&leader_server.addr, Some("node-1"));

    voters
        .directory
        .register(4, certificate_sha256(&pem("node-4.pem")).unwrap())
        .unwrap();
    let dir4 = kit::TestDir::new("operator-learner-4");
    RaftHost::prepare_member(
        dir4.path(),
        &voters.group,
        4,
        &kit::policy(),
        &kit::limits(),
    )
    .unwrap();
    let host4 = RaftHost::start_member(
        dir4.path(),
        &voters.group,
        4,
        &voters.config,
        raft_kit::transport(4, &voters.directory, "127.0.0.1:0".parse().unwrap()),
    )
    .await
    .unwrap();
    assert!(host4.awaiting_snapshot().unwrap());
    let addr4 = host4.advertised_addr().unwrap().to_string();

    let change = client
        .add_learner(AddLearnerRequest {
            node_id: 4,
            addr: addr4,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(voter_ids_of(&change), vec![1, 2, 3]);
    let status = get_status(&mut client).await;
    assert_eq!(voter_ids(&status), vec![1, 2, 3]);
    assert!(
        member_nodes(&status).iter().any(|(id, _)| *id == 4),
        "the learner is known before it is a voter"
    );
    let deadline = Instant::now() + Duration::from_secs(30);
    while host4.awaiting_snapshot().unwrap() {
        assert!(Instant::now() < deadline, "member 4 never seeded");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    write_ok(&leader_host, &mut revision, &voters.group, "added").await;

    client
        .promote_learners(PromoteLearnersRequest {
            learner_ids: vec![4],
        })
        .await
        .unwrap();
    let status = wait_voters(&mut client, vec![1, 2, 3, 4]).await;
    assert!(
        member_nodes(&status).iter().any(|(id, _)| *id == 4),
        "the promoted member keeps its address"
    );
    write_ok(&leader_host, &mut revision, &voters.group, "promoted").await;

    client
        .remove_member(RemoveMemberRequest { node_id: 4 })
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let status = get_status(&mut client).await;
        if member_nodes(&status).iter().all(|(id, _)| *id != 4) {
            break;
        }
        assert!(Instant::now() < deadline, "member 4 never left");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    write_ok(&leader_host, &mut revision, &voters.group, "removed").await;

    host4.shutdown().await.unwrap();
    leader_server.stop().await;
    return_host(&mut voters, leader, leader_host);
    for (node, host) in others.into_iter().zip(other_hosts) {
        return_host(&mut voters, node, host);
    }
    voters.shutdown().await;
}

// ---------------------------------------------------------------------------
// A membership change on a follower is Unavailable naming the leader.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_membership_change_on_a_follower_is_rejected_naming_the_leader() {
    let _serial = serial();
    let mut voters = raft_kit::three_voters("operator-follower", &kit::policy()).await;
    let leader = voters.leader().await;
    let follower = [1, 2, 3].into_iter().find(|node| *node != leader).unwrap();

    let follower_host = take_host(&mut voters, follower);
    let follower_server =
        serve_operator(Arc::clone(&follower_host), &format!("node-{follower}")).await;
    // The learner's certificate is bound before it joins, so the call
    // reaches the leadership check rather than the binding check.
    voters
        .directory
        .register(4, certificate_sha256(&pem("node-4.pem")).unwrap())
        .unwrap();
    let rejected = operator_client(&follower_server.addr, Some("node-1"))
        .add_learner(AddLearnerRequest {
            node_id: 4,
            addr: "127.0.0.1:50151".to_string(),
        })
        .await
        .err()
        .expect("a follower takes no membership change");
    assert_eq!(rejected.code(), Code::Unavailable, "{rejected}");
    assert!(
        rejected.message().contains(&leader.to_string()),
        "the refusal names the leader: {rejected}"
    );

    follower_server.stop().await;
    return_host(&mut voters, follower, follower_host);
    voters.shutdown().await;
}

// ---------------------------------------------------------------------------
// A call without a client certificate is Unauthenticated by name; a
// certificate from the cluster CA answers even when the peer directory
// never bound it (the operator is not a peer).
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_operator_call_without_a_client_certificate_is_rejected_by_name() {
    let _serial = serial();
    let mut voters = raft_kit::three_voters("operator-trust", &kit::policy()).await;
    let leader = voters.leader().await;

    let leader_host = take_host(&mut voters, leader);
    let leader_server = serve_operator(Arc::clone(&leader_host), &format!("node-{leader}")).await;
    let rejected = operator_client(&leader_server.addr, None)
        .get_member_status(GetMemberStatusRequest {})
        .await
        .err()
        .expect("a call without a client certificate is refused");
    assert_eq!(rejected.code(), Code::Unauthenticated, "{rejected}");
    assert!(
        rejected.message().contains("client certificate"),
        "the refusal names what is missing: {rejected}"
    );

    leader_server.stop().await;
    return_host(&mut voters, leader, leader_host);
    voters.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_operator_certificate_outside_the_peer_directory_is_accepted() {
    let _serial = serial();
    let mut voters = raft_kit::three_voters("operator-outside", &kit::policy()).await;
    let leader = voters.leader().await;

    let leader_host = take_host(&mut voters, leader);
    let leader_server = serve_operator(Arc::clone(&leader_host), &format!("node-{leader}")).await;
    // Node 4's certificate chains to the cluster CA but no directory in
    // this test ever binds it: the operator is not a peer.
    let status = get_status(&mut operator_client(&leader_server.addr, Some("node-4"))).await;
    assert_eq!(status.node_id, leader);
    assert!(status.leads);

    leader_server.stop().await;
    return_host(&mut voters, leader, leader_host);
    voters.shutdown().await;
}

// ---------------------------------------------------------------------------
// The digest rejection scenario of `control_raft_snapshots` driven
// through the operator route: the add is refused by the receiver's
// code, and the peer rejection's count and code appear in the status,
// in the metrics snapshot, and on the log line.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_peer_rejection_is_reported_over_the_route_and_on_the_metrics_page() {
    let _serial = serial();
    let group = kit::identity(kit::SEED);
    let directory = raft_kit::directory(&group);
    let leader_dir = kit::TestDir::new("operator-rejection-1");
    let member_dir = kit::TestDir::new("operator-rejection-2");
    let leader = RaftHost::bootstrap_cluster(
        leader_dir.path(),
        &group,
        1,
        &kit::policy(),
        &kit::limits(),
        &raft_kit::host_config(),
        raft_kit::transport(1, &directory, "127.0.0.1:0".parse().unwrap()),
    )
    .await
    .unwrap();
    let command = kit::source_command(
        &group,
        &kit::owner_key(),
        "prepare-1",
        1,
        1,
        0,
        kit::prepare_action(b"operator-rejection"),
    );
    let decision = leader.propose_command("alice", &command).await.unwrap();
    assert_eq!(decision.code, 0, "{}", decision.message);
    let (image, _) = raft_kit::snapshot_image(&leader_dir, &leader).await;
    let generation = raft_kit::published_generation(leader_dir.path()).unwrap();
    let path = raft_kit::generation_dir(leader_dir.path(), generation).join("image.redb");
    let mut bytes = std::fs::read(&path).unwrap();
    assert_eq!(bytes, image);
    let middle = bytes.len() / 2;
    bytes[middle] ^= 0xff;
    std::fs::write(&path, &bytes).unwrap();

    RaftHost::prepare_member(member_dir.path(), &group, 2, &kit::policy(), &kit::limits()).unwrap();
    let member = RaftHost::start_member(
        member_dir.path(),
        &group,
        2,
        &raft_kit::host_config(),
        raft_kit::transport(2, &directory, "127.0.0.1:0".parse().unwrap()),
    )
    .await
    .unwrap();
    let addr = member.advertised_addr().unwrap().to_string();

    let leader = Arc::new(leader);
    let operator = serve_operator(Arc::clone(&leader), "node-1").await;
    let diagnostics = serve_diagnostics(Arc::clone(&leader), "node-1").await;
    let mut client = operator_client(&operator.addr, Some("node-1"));

    let rejected = client
        .add_learner(AddLearnerRequest { node_id: 2, addr })
        .await
        .err()
        .expect("an image whose bytes changed installs nowhere");
    assert_eq!(rejected.code(), Code::DataLoss, "{rejected}");
    assert!(rejected.message().contains("digest"), "{rejected}");

    let status = get_status(&mut client).await;
    let rejection = status
        .peer_rejections
        .iter()
        .find(|rejection| rejection.node_id == 2)
        .expect("the leader reports the peer's rejection");
    assert_eq!(rejection.action, "snapshot");
    assert_eq!(rejection.code, "DataLoss");
    assert!(rejection.count >= 1, "{}", rejection.count);

    let snapshot = diagnostics_client(&diagnostics.addr, "node-1")
        .get_metrics_snapshot(MetricsSnapshotRequest {})
        .await
        .unwrap()
        .into_inner();
    let sample = snapshot
        .samples
        .iter()
        .find(|sample| {
            sample.name == "raft_peer_rejections_total"
                && sample
                    .labels
                    .iter()
                    .any(|label| label.name == "peer" && label.value == "2")
        })
        .expect("the snapshot carries the peer's rejection");
    let labels: Vec<(&str, &str)> = sample
        .labels
        .iter()
        .map(|label| (label.name.as_str(), label.value.as_str()))
        .collect();
    assert!(labels.contains(&("action", "snapshot")), "{labels:?}");
    assert!(labels.contains(&("code", "DataLoss")), "{labels:?}");
    assert!(
        sample.value >= rejection.count as f64,
        "the snapshot is no older than the status: {} < {}",
        sample.value,
        rejection.count
    );

    let deadline = Instant::now() + Duration::from_secs(30);
    let line = loop {
        let lines = pipestream_search::raft::rejection_log::take();
        if let Some(line) = lines
            .iter()
            .find(|line| line.contains("peer 2") && line.contains("snapshot"))
        {
            break line.clone();
        }
        assert!(Instant::now() < deadline, "no rejection line was written");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert!(line.contains("raft member 1"), "{line}");

    operator.stop().await;
    diagnostics.stop().await;
    member.shutdown().await.unwrap();
    Arc::try_unwrap(leader)
        .ok()
        .expect("no server still holds the host")
        .shutdown()
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// `raft-bootstrap` as a child process (the `multiprocess` style: the
// binary under the fork guard): it creates one leading member and
// prints its status JSON; a second run on the directory is refused by
// name; a policy file that does not decode is refused naming the file.
// ---------------------------------------------------------------------------

const BIN: &str = env!("CARGO_BIN_EXE_pipestream-search");

/// One proto message as proto3 JSON, through the rendering the console
/// and the CLI share.
fn message_json(message_name: &str, bytes: &[u8]) -> Vec<u8> {
    let pool = pipestream_search::console::descriptor_pool();
    let descriptor = pool.get_message_by_name(message_name).unwrap();
    pipestream_search::console::message_bytes_to_json(&descriptor, bytes).unwrap()
}

fn bootstrap_args(
    dir: &std::path::Path,
    policy: &std::path::Path,
    limits: &std::path::Path,
) -> Vec<String> {
    let group = kit::identity(kit::SEED);
    let peers = dir.join("peers.toml");
    vec![
        "raft-bootstrap".to_string(),
        format!("--raft-dir={}", dir.join("member").display()),
        "--raft-node-id=1".to_string(),
        format!("--raft-group-id={}", hex(&group.group_id)),
        format!(
            "--raft-authority-incarnation={}",
            hex(&group.authority_incarnation)
        ),
        "--raft-listen=127.0.0.1:0".to_string(),
        format!("--raft-peers={}", peers.display()),
        format!("--tls-cert={}", fixtures().join("node-1.pem").display()),
        format!("--tls-key={}", fixtures().join("node-1.key.pem").display()),
        format!("--tls-client-ca={}", fixtures().join("ca.pem").display()),
        format!("--tls-ca={}", fixtures().join("ca.pem").display()),
        format!(
            "--tls-client-cert={}",
            fixtures().join("node-1.pem").display()
        ),
        format!(
            "--tls-client-key={}",
            fixtures().join("node-1.key.pem").display()
        ),
        "--tls-domain=localhost".to_string(),
        format!("--policy={}", policy.display()),
        format!("--limits={}", limits.display()),
    ]
}

fn run_child(args: Vec<String>) -> std::process::Output {
    let mut command = std::process::Command::new(BIN);
    command.args(&args);
    // The child's config comes from its flags alone: a TURBOVEC_* or
    // PIPESTREAM_SEARCH_* variable of this shell must not leak into it.
    for (key, _) in std::env::vars() {
        if key.starts_with("TURBOVEC_") || key.starts_with("PIPESTREAM_SEARCH_") {
            command.env_remove(key);
        }
    }
    command.output_guarded().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raft_bootstrap_creates_one_leading_member_and_rejects_a_used_directory() {
    use prost::Message as _;
    let _serial = serial();
    let work = kit::TestDir::new("operator-bootstrap");
    std::fs::copy(
        fixtures().join("node-1.pem"),
        work.path().join("node-1.pem"),
    )
    .unwrap();
    std::fs::write(
        work.path().join("peers.toml"),
        "[[peers]]\nnode_id = 1\ncertificate = \"node-1.pem\"\n",
    )
    .unwrap();
    let policy_path = work.path().join("policy.json");
    let limits_path = work.path().join("limits.json");
    std::fs::write(
        &policy_path,
        message_json(
            "ai.protomolt.search.v1.AccessPolicy",
            &kit::policy().encode_to_vec(),
        ),
    )
    .unwrap();
    std::fs::write(
        &limits_path,
        message_json(
            "ai.protomolt.search.storage.v1.SourceAuthorityLimits",
            &kit::limits().encode_to_vec(),
        ),
    )
    .unwrap();
    let args = bootstrap_args(work.path(), &policy_path, &limits_path);

    let out = tokio::task::spawn_blocking({
        let args = args.clone();
        move || run_child(args)
    })
    .await
    .unwrap();
    assert!(
        out.status.success(),
        "raft-bootstrap: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let status: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(status["nodeId"], serde_json::Value::from("1"));
    assert_eq!(status["leads"], serde_json::Value::Bool(true));

    let again = tokio::task::spawn_blocking({
        let args = args.clone();
        move || run_child(args)
    })
    .await
    .unwrap();
    assert!(!again.status.success(), "a used directory bootstraps twice");
    assert_eq!(again.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&again.stderr);
    assert!(
        stderr.contains("already exists"),
        "the refusal names the store: {stderr}"
    );

    let bad_policy = work.path().join("bad-policy.json");
    std::fs::write(&bad_policy, b"{not json").unwrap();
    // A fresh directory, so the run reaches the decoder rather than the
    // used-directory check.
    let fresh = work.path().join("fresh");
    std::fs::create_dir_all(&fresh).unwrap();
    std::fs::copy(work.path().join("peers.toml"), fresh.join("peers.toml")).unwrap();
    std::fs::copy(fixtures().join("node-1.pem"), fresh.join("node-1.pem")).unwrap();
    let mut bad_args = bootstrap_args(&fresh, &bad_policy, &limits_path);
    for arg in &mut bad_args {
        if arg.starts_with("--policy=") {
            *arg = format!("--policy={}", bad_policy.display());
        }
    }
    let bad = tokio::task::spawn_blocking(move || run_child(bad_args))
        .await
        .unwrap();
    assert!(
        !bad.status.success(),
        "a policy that does not decode bootstraps"
    );
    let stderr = String::from_utf8_lossy(&bad.stderr);
    assert!(
        stderr.contains(&bad_policy.display().to_string()),
        "the refusal names the file: {stderr}"
    );
}

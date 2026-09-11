//! Operator evidence for the Raft specification: member certificate
//! rotation (docs/raft-hosting.md, "Security considerations"). The
//! procedure runs through the host API directly, the same calls the
//! operator service makes: remove the member, issue the new
//! certificate, rebind the node id in every peers file (here a rebuilt
//! directory, which running members pick up by restart), re-prepare and
//! re-add it.
#![cfg(all(feature = "raft", feature = "tls"))]

mod control_adversarial;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use control_adversarial::{kit, raft_kit};
use pipestream_search::pb::storage::raft_transport_client::RaftTransportClient;
use pipestream_search::pb::storage::RaftVoteRequest;
use pipestream_search::raft::reasons;
use pipestream_search::raft::transport::{certificate_sha256, PeerDirectory, TransportLimits};
use pipestream_search::raft::{ClusterTransport, RaftHost};
use pipestream_search::security::{apply_client_tls, ClientTls, ServerTls};
use tonic::Code;

fn pem(name: &str) -> Vec<u8> {
    std::fs::read(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/certs/raft")
            .join(name),
    )
    .unwrap()
}

fn fingerprint(cert: &str) -> [u8; 32] {
    certificate_sha256(&pem(&format!("{cert}.pem"))).unwrap()
}

fn client_tls(cert: &str) -> ClientTls {
    ClientTls {
        ca_pem: pem("ca.pem"),
        identity_pem: Some((pem(&format!("{cert}.pem")), pem(&format!("{cert}.key.pem")))),
        domain: Some("localhost".into()),
    }
}

/// A transport presenting `cert`'s files, on `directory`.
fn transport_for(
    cert: &str,
    directory: &Arc<PeerDirectory>,
    listen: std::net::SocketAddr,
) -> ClusterTransport {
    ClusterTransport {
        directory: Arc::clone(directory),
        server_tls: ServerTls {
            cert_pem: pem(&format!("{cert}.pem")),
            key_pem: pem(&format!("{cert}.key.pem")),
            client_ca_pem: Some(pem("ca.pem")),
        },
        client_tls: client_tls(cert),
        listen,
        advertise: None,
        limits: TransportLimits::default(),
    }
}

async fn raw_client(
    addr: std::net::SocketAddr,
    cert: &str,
) -> RaftTransportClient<tonic::transport::Channel> {
    let endpoint = apply_client_tls(
        tonic::transport::Endpoint::from_shared(format!("https://{addr}"))
            .unwrap()
            .timeout(Duration::from_secs(5)),
        Some(&client_tls(cert)),
    )
    .unwrap();
    RaftTransportClient::new(endpoint.connect().await.unwrap())
}

/// Rotation: node 3 is removed, rebound to the node-4 fixture
/// certificate as its new identity, re-prepared and re-added, and serves
/// again; a call under the old certificate is rejected by name at every
/// surviving listener.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_rotated_to_a_new_certificate_serves_again() {
    let mut cluster = raft_kit::three_voters("op-rotate", &kit::policy()).await;
    let leader = cluster.leader().await;
    let rotated = if leader == 3 { 2 } else { 3 };

    // A write commits on the full group before rotation.
    let before = cluster
        .host(leader)
        .applied_position()
        .unwrap()
        .unwrap()
        .index;

    // 1. Remove the member whose certificate is compromised.
    cluster.host(leader).remove_member(rotated).await.unwrap();
    let survivors: BTreeSet<u64> = [1, 2, 3]
        .into_iter()
        .filter(|node| *node != rotated)
        .collect();
    cluster
        .host(leader)
        .wait(Some(Duration::from_secs(30)))
        .voter_ids(survivors.iter().copied(), "rotated member removed")
        .await
        .unwrap();
    let retired = cluster.take(rotated);
    retired.shutdown().await.unwrap();

    // 2-3. Issue the new certificate (the node-4 fixture stands in for a
    // fresh CA issue) and rebind the node id in every peers file: the
    // rotated directory binds the survivors to their old certificates
    // and the rotated node to the new one. Running members pick the
    // rebinding up by restart, the only reload path on this base.
    let rebound = Arc::new({
        let directory = PeerDirectory::new(&cluster.group);
        for node in &survivors {
            directory
                .register(*node, fingerprint(&format!("node-{node}")))
                .unwrap();
        }
        directory.register(rotated, fingerprint("node-4")).unwrap();
        directory
    });
    for node in survivors.clone() {
        let old = cluster.take(node);
        let listen = old.listen_addr().unwrap();
        old.shutdown().await.unwrap();
        let dir = cluster.member_dir(node);
        let host = RaftHost::start_member(
            &dir,
            &cluster.group,
            node,
            &cluster.config,
            raft_kit::transport(node, &rebound, listen),
        )
        .await
        .unwrap();
        cluster.insert(node, host);
    }

    // The old certificate is rejected by name at every listener: the
    // directory no longer binds it to any member.
    for node in &survivors {
        let addr = cluster.host(*node).listen_addr().unwrap();
        let mut client = raw_client(addr, &format!("node-{rotated}")).await;
        let error = client.vote(RaftVoteRequest::default()).await.unwrap_err();
        assert_eq!(error.code(), Code::Unauthenticated, "{error}");
        assert_eq!(
            reasons::reason_of(&error),
            Some(reasons::TRANSPORT_UNREGISTERED_CERTIFICATE),
            "{error}"
        );
    }

    // 4. Re-prepare and re-add the member under its new certificate.
    let dir = kit::TestDir::new("op-rotate-member");
    RaftHost::prepare_member(
        dir.path(),
        &cluster.group,
        rotated,
        &kit::policy(),
        &kit::limits(),
    )
    .unwrap();
    let host = RaftHost::start_member(
        dir.path(),
        &cluster.group,
        rotated,
        &cluster.config,
        transport_for("node-4", &rebound, "127.0.0.1:0".parse().unwrap()),
    )
    .await
    .unwrap();
    assert!(host.awaiting_snapshot().unwrap());
    let addr = host.advertised_addr().unwrap().to_string();
    let leader = cluster.leader().await;
    cluster
        .host(leader)
        .add_learner(rotated, &addr)
        .await
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while host.awaiting_snapshot().unwrap() {
        assert!(
            std::time::Instant::now() < deadline,
            "rotated member never seeded"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    cluster
        .host(leader)
        .promote(BTreeSet::from([rotated]))
        .await
        .unwrap();
    // The re-added host serves its own reads from the same process; the
    // group admits it back as a voter.
    cluster
        .host(leader)
        .wait(Some(Duration::from_secs(30)))
        .voter_ids([1, 2, 3].into_iter(), "rotated member voter again")
        .await
        .unwrap();

    // It serves again: a write committed after the rotation applies on it.
    let decision = cluster
        .host(leader)
        .propose_command(
            "alice",
            &kit::source_command(
                &cluster.group,
                &kit::owner_key(),
                "op-rotate-write",
                cluster.revision,
                1,
                0,
                kit::prepare_action_history(kit::WORKFLOW, vec![9; 16]),
            ),
        )
        .await
        .unwrap();
    assert_eq!(decision.code, 0, "{}", decision.message);
    cluster.revision += 1;
    let index = cluster
        .host(leader)
        .applied_position()
        .unwrap()
        .unwrap()
        .index;
    assert!(index > before, "the rotation write applied");
    host.wait(Some(Duration::from_secs(30)))
        .applied_index_at_least(Some(index), "rotated member caught up")
        .await
        .unwrap();

    // Every listener, the re-added member's included, still rejects the
    // old certificate by name: no directory binds it to any member.
    let mut listeners: Vec<std::net::SocketAddr> = survivors
        .iter()
        .map(|node| cluster.host(*node).listen_addr().unwrap())
        .collect();
    listeners.push(host.listen_addr().unwrap());
    for addr in listeners {
        let mut client = raw_client(addr, &format!("node-{rotated}")).await;
        let error = client.vote(RaftVoteRequest::default()).await.unwrap_err();
        assert_eq!(error.code(), Code::Unauthenticated, "{error}");
        assert_eq!(
            reasons::reason_of(&error),
            Some(reasons::TRANSPORT_UNREGISTERED_CERTIFICATE),
            "{error}"
        );
    }

    host.shutdown().await.unwrap();
    cluster.shutdown().await;
}

//! Leader write admission, follower lease forwarding, and read-index lease barriers.

use super::*;

// A write through the leader's hosted service is admitted and durable.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_through_the_hosted_service_is_admitted_by_the_leader_and_durable() {
    let _serial = serial();
    let mut cluster = raft_kit::three_voters("hosted-write", &hosted_policy()).await;
    let leader = cluster.leader().await;
    let dir = kit::TestDir::new("hosted-write-catalog");
    let active = bridge_active(&mut cluster, leader, &dir, "hosted-prepare").await;

    let catalog = Arc::new(active);
    let host = take_host(&mut cluster, leader);
    let service = HostedDocumentWriteService::new(
        principals(),
        Arc::clone(&host),
        vec![Arc::clone(&catalog)],
        limits(),
    )
    .unwrap();
    let (mut client, server) = serve(service).await;

    let target = client
        .get_write_target(auth(
            GetDocumentWriteTargetRequest {
                collection: kit::COLLECTION.into(),
            },
            "alice",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(target.history_id.len(), 16);

    let request = network_write(
        target.history_id.clone(),
        b"hosted-one",
        b"hosted-op-one",
        0,
    );
    let first = client
        .accept_document(auth(request.clone(), "alice"))
        .await
        .unwrap()
        .into_inner();
    assert!(first.accepted && first.durable && !first.searchable);
    assert_eq!(first.version, 1);
    assert!(!first.replayed);

    // The exact retry answers from the stored decision.
    let replay = client
        .accept_document(auth(request, "alice"))
        .await
        .unwrap()
        .into_inner();
    assert!(replay.replayed);
    assert_eq!(
        (replay.version, replay.accepted_sequence),
        (first.version, first.accepted_sequence)
    );

    // The row is in the catalog itself: the fixture's write was the first
    // accepted row, the network write is the second.
    let inspected = host
        .with_admission("alice", |admission| catalog.inspect(admission, 1 << 20))
        .await
        .unwrap();
    assert_eq!(
        inspected
            .header
            .as_ref()
            .map(|header| header.accepted_sequence),
        Some(2),
        "the network write must be the catalog's second accepted row"
    );

    drop(client);
    server.shutdown().await;
    return_host(&mut cluster, leader, host);
    cluster.shutdown().await;
}

// A write through a follower names the leader and refuses.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_through_a_follower_is_admitted_under_a_lease_forwarded_from_the_leader() {
    let _serial = serial();
    let mut cluster = raft_kit::three_voters("hosted-follower", &hosted_policy()).await;
    let leader = cluster.leader().await;
    let follower = *cluster
        .nodes()
        .iter()
        .find(|n| **n != leader)
        .expect("three voters");
    let dir = kit::TestDir::new("hosted-follower-catalog");
    let catalog = Arc::new(bridge_active(&mut cluster, leader, &dir, "hosted-prepare").await);

    // The follower serves the catalog: its target read and its write each
    // take a lease forwarded from the leader and are admitted on the
    // follower's own applied view.
    let host = take_host(&mut cluster, follower);
    let service = HostedDocumentWriteService::new(
        principals(),
        Arc::clone(&host),
        vec![Arc::clone(&catalog)],
        limits(),
    )
    .unwrap();
    let (mut client, server) = serve(service).await;
    assert_ne!(
        host.metrics().borrow().state,
        openraft::ServerState::Leader,
        "the serving member must not lead"
    );
    let target = client
        .get_write_target(auth(
            GetDocumentWriteTargetRequest {
                collection: kit::COLLECTION.into(),
            },
            "alice",
        ))
        .await
        .unwrap()
        .into_inner();
    let request = network_write(
        target.history_id.clone(),
        b"hosted-one",
        b"hosted-op-one",
        0,
    );
    let receipt = client
        .accept_document(auth(request.clone(), "alice"))
        .await
        .unwrap()
        .into_inner();
    assert!(
        receipt.accepted && receipt.durable && !receipt.replayed,
        "{receipt:?}"
    );
    assert_eq!(receipt.version, 1);
    assert_eq!(receipt.write_epoch, catalog.activation().write_epoch);
    let replay = client
        .accept_document(auth(request, "alice"))
        .await
        .unwrap()
        .into_inner();
    assert!(replay.replayed);
    assert_eq!(replay.accepted_sequence, receipt.accepted_sequence);
    assert_ne!(
        host.metrics().borrow().state,
        openraft::ServerState::Leader,
        "the write did not make the member lead"
    );

    // Cut from the leader, the follower cannot forward and rejects by
    // name; nothing is admitted on a stale view.
    host.isolate([leader]);
    let error = client
        .accept_document(auth(
            network_write(target.history_id, b"hosted-two", b"hosted-op-two", 0),
            "alice",
        ))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::Unavailable, "{error}");
    assert_eq!(
        reasons::reason_of(&error),
        Some(reasons::LEASE_LEADER_UNREACHABLE),
        "the rejection must name the forwarded lease: {error}"
    );
    host.heal();

    drop(client);
    server.shutdown().await;
    return_host(&mut cluster, follower, host);
    cluster.shutdown().await;
}

// A forwarded lease grants only once the member has applied the position
// the leader's barrier read: a member that has fallen behind rejects
// inside its interval, and once it catches up it sees the revocation.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_forwarded_lease_grants_only_once_the_member_applied_the_leaders_read_position() {
    let _serial = serial();
    let config = slow_lease_config();
    let lease = Duration::from_millis(config.admission_lease_ms);
    let mut cluster =
        raft_kit::three_voters_with("hosted-read-position", &hosted_policy(), &config).await;
    let leader = cluster.leader().await;
    let follower = *cluster
        .nodes()
        .iter()
        .find(|n| **n != leader)
        .expect("three voters");
    let dir = kit::TestDir::new("hosted-read-position-catalog");
    let catalog = Arc::new(bridge_active(&mut cluster, leader, &dir, "hosted-prepare").await);
    let host = take_host(&mut cluster, follower);
    let service = HostedDocumentWriteService::new(
        principals(),
        Arc::clone(&host),
        vec![Arc::clone(&catalog)],
        limits(),
    )
    .unwrap();
    let (mut client, server) = serve(service).await;
    let target = client
        .get_write_target(auth(
            GetDocumentWriteTargetRequest {
                collection: kit::COLLECTION.into(),
            },
            "alice",
        ))
        .await
        .unwrap()
        .into_inner();

    // The follower stops hearing the leader but can still ask it. The
    // leader and the third voter commit the revocation of alice; the
    // follower has not applied it.
    host.isolate_inbound([leader]);
    let behind = host.applied_position().unwrap().unwrap().index;
    cluster
        .propose(
            leader,
            "bob",
            "revoke-alice",
            &kit::key(),
            revoke_alice_action(),
        )
        .await;
    let revoked_at = cluster
        .host(leader)
        .applied_position()
        .unwrap()
        .unwrap()
        .index;
    assert!(revoked_at > behind);
    let started = Instant::now();
    let error = client
        .accept_document(auth(
            network_write(target.history_id.clone(), b"hosted-behind", b"behind-op", 0),
            "alice",
        ))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::Unavailable, "{error}");
    assert!(
        error.message().contains(&format!(
            "had not applied the leader's read position {revoked_at}"
        )),
        "the rejection must name the position the member is behind: {error}"
    );
    assert_eq!(
        reasons::reason_of(&error),
        Some(reasons::LEASE_READ_POSITION_BEHIND),
        "{error}"
    );
    assert!(
        started.elapsed() <= lease + Duration::from_millis(200),
        "the wait is bounded by the interval: {:?}",
        started.elapsed()
    );
    assert_eq!(
        host.applied_position().unwrap().unwrap().index,
        behind,
        "nothing was applied while the member was deaf"
    );

    // Healed, the member applies the revocation and the same write is
    // denied on the merits, not on the position.
    host.heal();
    host.wait(Some(Duration::from_secs(30)))
        .applied_index_at_least(Some(revoked_at), "applied")
        .await
        .unwrap();
    let error = client
        .accept_document(auth(
            network_write(target.history_id, b"hosted-behind", b"behind-op", 0),
            "alice",
        ))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::PermissionDenied, "{error}");

    drop(client);
    server.shutdown().await;
    return_host(&mut cluster, follower, host);
    cluster.shutdown().await;
}

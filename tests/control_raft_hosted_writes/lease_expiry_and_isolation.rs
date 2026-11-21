//! Expired lease write rejection and bounded leader-isolation step-down.

use super::*;

// A lease kept past its interval leaves no durable change.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_kept_past_its_lease_leaves_no_durable_change() {
    let _serial = serial();
    let config = raft_kit::host_config();
    let lease = Duration::from_millis(config.admission_lease_ms);
    let mut cluster = raft_kit::three_voters("hosted-kept-lease", &hosted_policy()).await;
    let leader = cluster.leader().await;
    let dir = kit::TestDir::new("hosted-kept-lease-catalog");
    let active = bridge_active(&mut cluster, leader, &dir, "hosted-prepare").await;

    let host = take_host(&mut cluster, leader);
    let service = HostedDocumentWriteService::new(
        principals(),
        Arc::clone(&host),
        vec![Arc::new(active)],
        limits(),
    )
    .unwrap();
    // The target is fetched before the delay is armed: the target read
    // takes a lease of its own and would expire under the same pause.
    // The clone shares the service state, so arming through it arms the
    // served copy.
    let handle = service.clone();
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
    handle.arm_grant_delay(lease * 3);
    let request = network_write(
        target.history_id.clone(),
        b"hosted-one",
        b"hosted-op-one",
        0,
    );
    let error = client
        .accept_document(auth(request.clone(), "alice"))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition, "{error}");
    assert_eq!(
        reasons::reason_of(&error),
        Some(reasons::LEASE_INTERVAL_ELAPSED),
        "the refusal must name the grant-time check: {error}"
    );

    // The same operation retried after a fresh admission is new work, not
    // a replay: the refused grant left no row and no retry record.
    let second = client
        .accept_document(auth(request, "alice"))
        .await
        .unwrap()
        .into_inner();
    assert!(second.accepted && !second.replayed);
    assert_eq!(second.version, 1);

    drop(handle);
    drop(client);
    server.shutdown().await;
    return_host(&mut cluster, leader, host);
    cluster.shutdown().await;
}

// An isolated leader refuses through the service within the election
// ceiling; after healing it forwards its lease to the successor and
// denies the revoked principal on the merits.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_isolated_leader_rejects_hosted_writes_within_election_timeout_max() {
    let _serial = serial();
    let config = raft_kit::host_config();
    let election_max = Duration::from_millis(config.election_timeout_max_ms);
    let mut cluster = raft_kit::three_voters("hosted-isolation", &hosted_policy()).await;
    let leader = cluster.leader().await;
    let dir = kit::TestDir::new("hosted-isolation-catalog");
    let active = bridge_active(&mut cluster, leader, &dir, "hosted-prepare").await;

    let host = take_host(&mut cluster, leader);
    let service = HostedDocumentWriteService::new(
        principals(),
        Arc::clone(&host),
        vec![Arc::new(active)],
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

    // Isolate the old leader from both peers, both directions.
    let others: Vec<u64> = cluster
        .nodes()
        .into_iter()
        .filter(|n| *n != leader)
        .collect();
    host.isolate(others.iter().copied());
    for other in &others {
        cluster.host(*other).isolate([leader]);
    }

    // The barrier is bounded by the election ceiling, so the refusal
    // arrives within it however long the isolation lasts; the lease is
    // slack for the local service path around the barrier.
    let lease = Duration::from_millis(config.admission_lease_ms);
    let request_at = Instant::now();
    let error = client
        .accept_document(auth(
            network_write(
                target.history_id.clone(),
                b"hosted-one",
                b"hosted-op-one",
                0,
            ),
            "alice",
        ))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::Unavailable, "{error}");
    assert!(
        request_at.elapsed() < election_max + lease,
        "the refusal must arrive near the read bound, not after a full election: {:?}",
        request_at.elapsed()
    );

    // The surviving quorum elects a successor and commits the revocation
    // of alice while the old side still shows the stale policy.
    let successor = cluster.leader_other_than(leader).await;
    cluster
        .propose(
            successor,
            "bob",
            "revoke-alice",
            &kit::key(),
            revoke_alice_action(),
        )
        .await;
    let revoked_at = cluster
        .host(successor)
        .applied_position()
        .unwrap()
        .unwrap()
        .index;

    // Heal: the old leader catches up to the revoked policy; its lease is
    // forwarded to the successor and granted on the applied revocation,
    // so alice is denied on the merits.
    host.heal();
    for other in &others {
        cluster.host(*other).heal();
    }
    host.wait(Some(Duration::from_secs(30)))
        .applied_index_at_least(Some(revoked_at), "applied")
        .await
        .unwrap();
    let error = client
        .accept_document(auth(
            network_write(target.history_id, b"hosted-two", b"hosted-op-two", 0),
            "alice",
        ))
        .await
        .unwrap_err();
    assert_eq!(
        error.code(),
        Code::PermissionDenied,
        "the healed old leader must deny the revoked principal under a forwarded lease: {error}"
    );
    let _ = successor;

    drop(client);
    server.shutdown().await;
    return_host(&mut cluster, leader, host);
    cluster.shutdown().await;
}

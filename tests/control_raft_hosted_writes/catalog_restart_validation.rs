//! Managed catalog restart recovery and refusal of inactive or foreign catalogs.

use super::*;

// Restart: every member restarts on its directory, the catalogs recover
// through the same library function the binary uses, and the leader's
// service serves writes again.
//
// Leadership after a restart is not deterministic, so the restart covers
// the whole group (as a fleet rollout would) and the write goes to
// whichever member leads: recovery itself is member-agnostic, running on
// the host's store under a leased admission.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_restarted_with_its_managed_catalogs_serves_writes_again() {
    let _serial = serial();
    let mut cluster = raft_kit::three_voters("hosted-restart", &hosted_policy()).await;
    let leader = cluster.leader().await;
    let dir = kit::TestDir::new("hosted-restart-catalog");
    let active = bridge_active(&mut cluster, leader, &dir, "hosted-prepare").await;
    drop(active);
    let entry = managed_entry(kit::COLLECTION, &dir);

    // Record every member's bound address, then stop the whole group.
    let mut states = Vec::new();
    for node in cluster.nodes() {
        let addr = cluster.host(node).listen_addr().expect("listening");
        states.push((node, cluster.member_dir(node), addr));
    }
    let mut hosts = Vec::new();
    for node in cluster.nodes() {
        hosts.push(cluster.take(node));
    }
    for host in hosts {
        host.shutdown().await.unwrap();
    }

    // Start every member again on its own directory and address. The first
    // one up recovers the catalogs at once: one member of three is running,
    // so no leader can exist, and recovery reads the applied view.
    let mut recovered_without_a_leader = None;
    for (node, dir, addr) in &states {
        let host = RaftHost::start_member(
            dir,
            &cluster.group,
            *node,
            &cluster.config,
            raft_kit::transport(*node, &cluster.directory, *addr),
        )
        .await
        .unwrap();
        assert!(
            !host.awaiting_snapshot().unwrap(),
            "a restarted member must hold its seeded state"
        );
        if recovered_without_a_leader.is_none() {
            // One member of three: the library may resume the old leader's
            // role from its committed vote, but no quorum is in reach, so
            // no lease can be granted here and a leased recovery could not
            // have run.
            let no_lease = host.lease("alice").await.err().expect("no quorum");
            assert_eq!(no_lease.code(), Code::Unavailable, "{no_lease}");
            let recovered = recover_catalogs(&host, "alice", std::slice::from_ref(&entry)).unwrap();
            recovered_without_a_leader = Some(recovered.len());
        }
        cluster.insert(*node, host);
    }
    assert_eq!(recovered_without_a_leader, Some(1));
    let leader = cluster.leader().await;

    // Recover through the same function the binary calls at start, then
    // serve one write on the leader.
    let host = take_host(&mut cluster, leader);
    let recovered = recover_catalogs(&host, "alice", &[entry]).unwrap();
    assert_eq!(recovered.len(), 1);
    let service =
        HostedDocumentWriteService::new(principals(), Arc::clone(&host), recovered, limits())
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
    let receipt = client
        .accept_document(auth(
            network_write(target.history_id, b"hosted-after-restart", b"restart-op", 0),
            "alice",
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(receipt.accepted && receipt.durable && !receipt.replayed);
    assert_eq!(receipt.version, 1);

    drop(client);
    server.shutdown().await;
    return_host(&mut cluster, leader, host);
    cluster.shutdown().await;
}

/// A catalog file bound under a preparation but never activated: recovery
/// names the prepared state and serves nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restart_with_a_prepared_but_inactive_catalog_is_rejected_by_name() {
    let _serial = serial();
    let mut cluster = raft_kit::three_voters("hosted-prepared", &hosted_policy()).await;
    let leader = cluster.leader().await;
    let dir = kit::TestDir::new("hosted-prepared-catalog");
    let authority = cluster.group.clone();
    let (catalog, history_id) = catalog_fixture(&dir);
    let prepare = kit::source_command(
        &authority,
        &kit::owner_key(),
        "hosted-prepare",
        cluster.revision,
        1,
        0,
        kit::prepare_action_history(WORKFLOW, history_id),
    );
    let decision = cluster
        .host(leader)
        .propose_command("alice", &prepare)
        .await
        .unwrap();
    assert_eq!(decision.code, 0, "{}", decision.message);
    let preparation = decision.owner.clone().unwrap();
    let store = cluster.host(leader).store().unwrap();
    let prepared = cluster
        .host(leader)
        .with_admission("alice", |admission| {
            catalog.bind_prepared_owner(admission, &store, &preparation, 1 << 20)
        })
        .await
        .unwrap();
    drop(prepared);
    drop(store);

    let host = take_host(&mut cluster, leader);
    let error = recover_catalogs(&host, "alice", &[managed_entry(kit::COLLECTION, &dir)])
        .err()
        .expect("a prepared catalog must not recover");
    assert_eq!(error.code(), Code::FailedPrecondition, "{error}");
    assert_eq!(
        reasons::reason_of(&error),
        Some(reasons::ADMISSION_NOT_ACTIVE),
        "the rejection must name the prepared state: {error}"
    );

    return_host(&mut cluster, leader, host);
    cluster.shutdown().await;
}

/// A catalog file bound under one collection but configured for another:
/// recovery names the foreign collection and serves nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restart_with_a_foreign_collection_catalog_is_rejected_by_name() {
    let _serial = serial();
    let mut cluster = raft_kit::three_voters("hosted-collection", &hosted_policy()).await;
    let leader = cluster.leader().await;
    let dir = kit::TestDir::new("hosted-collection-catalog");
    // Activated, so recovery passes the prepared state and reaches the
    // collection binding: the file is bound to the prepared collection.
    let active = bridge_active(&mut cluster, leader, &dir, "hosted-prepare").await;
    drop(active);

    let host = take_host(&mut cluster, leader);
    let error = recover_catalogs(&host, "alice", &[managed_entry("another-collection", &dir)])
        .err()
        .expect("a foreign-collection catalog must not recover");
    assert_eq!(error.code(), Code::FailedPrecondition, "{error}");
    assert_eq!(
        reasons::reason_of(&error),
        Some(reasons::ADMISSION_FOREIGN_COLLECTION),
        "the rejection must name the foreign collection: {error}"
    );

    return_host(&mut cluster, leader, host);
    cluster.shutdown().await;
}

/// A catalog file bound under another authority: recovery names the
/// foreign identity and serves nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restart_with_a_foreign_authority_catalog_is_rejected_by_name() {
    let _serial = serial();
    let mut cluster = raft_kit::three_voters("hosted-foreign", &hosted_policy()).await;
    let leader = cluster.leader().await;
    let dir = kit::TestDir::new("hosted-foreign-catalog");
    let active = bridge_active(&mut cluster, leader, &dir, "hosted-prepare").await;
    drop(active);

    // A member of another group: its store carries another identity, so
    // the file's binding names an authority it never served.
    let foreign_dir = kit::TestDir::new("hosted-foreign-member");
    let foreign = RaftHost::bootstrap_single(
        foreign_dir.path(),
        &kit::identity(9),
        1,
        "https://node-1:19291",
        &hosted_policy(),
        &kit::limits(),
        &raft_kit::host_config(),
    )
    .await
    .unwrap();
    let error = recover_catalogs(&foreign, "alice", &[managed_entry(kit::COLLECTION, &dir)])
        .err()
        .expect("a foreign catalog must not recover");
    assert_eq!(error.code(), Code::FailedPrecondition, "{error}");
    assert_eq!(
        reasons::reason_of(&error),
        Some(reasons::ADMISSION_FOREIGN_AUTHORITY),
        "the rejection must name the foreign authority: {error}"
    );
    foreign.shutdown().await.unwrap();

    cluster.shutdown().await;
}

/// A catalog file whose activation the store does not record: recovery
/// names the fence mismatch and serves nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restart_with_an_unrecorded_activation_is_rejected_by_name() {
    let _serial = serial();
    let mut cluster = raft_kit::three_voters("hosted-unrecorded", &hosted_policy()).await;
    let leader = cluster.leader().await;
    let dir = kit::TestDir::new("hosted-unrecorded-catalog");
    let active = bridge_active(&mut cluster, leader, &dir, "hosted-prepare").await;
    drop(active);

    // A fresh store of the same identity holds no owner row at all, so the
    // file's activation is recorded nowhere in it: the fence lookup finds
    // no committed preparation to compare against.
    let fresh_dir = kit::TestDir::new("hosted-unrecorded-member");
    let fresh = raft_kit::bootstrap_host_with(&fresh_dir, &hosted_policy()).await;
    let error = recover_catalogs(&fresh, "alice", &[managed_entry(kit::COLLECTION, &dir)])
        .err()
        .expect("an unrecorded activation must not recover");
    assert_eq!(error.code(), Code::FailedPrecondition, "{error}");
    assert_eq!(
        reasons::reason_of(&error),
        Some(reasons::ADMISSION_UNRECORDED_ACTIVATION),
        "the rejection must name the unrecorded activation: {error}"
    );
    fresh.shutdown().await.unwrap();

    cluster.shutdown().await;
}

// One member restarted while the other two keep running: it recovers its
// catalogs at start, before it has any leader, and serves writes again
// once it leads.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_single_member_restarted_recovers_at_start_and_serves_once_it_leads() {
    let _serial = serial();
    let mut cluster = raft_kit::three_voters("hosted-single-restart", &hosted_policy()).await;
    let leader = cluster.leader().await;
    let others: Vec<u64> = cluster
        .nodes()
        .into_iter()
        .filter(|n| *n != leader)
        .collect();
    let dir = kit::TestDir::new("hosted-single-restart-catalog");
    let active = bridge_active(&mut cluster, leader, &dir, "hosted-prepare").await;
    drop(active);
    let entry = managed_entry(kit::COLLECTION, &dir);
    let addr = cluster.host(leader).listen_addr().expect("listening");
    let member_dir = cluster.member_dir(leader);

    // Cut every link, then leave one entry in the leader's log that no peer
    // holds: a command the state machine refuses (a stale expected
    // revision), appended locally while its commit waits for a quorum that
    // cannot answer. With the longer log, the restarted member is the only
    // one its peers can elect, whichever of them asks first.
    cluster.host(leader).isolate(others.iter().copied());
    for other in &others {
        let rest: Vec<u64> = cluster.nodes().into_iter().filter(|n| n != other).collect();
        cluster.host(*other).isolate(rest);
    }
    let before = cluster.host(leader).log_entries().unwrap().len();
    let host = Arc::new(cluster.take(leader));
    let pending = {
        let host = Arc::clone(&host);
        let stale = kit::source_command(
            &cluster.group,
            &kit::owner_key(),
            "stale-revision",
            1,
            1,
            0,
            kit::prepare_action_history(WORKFLOW, vec![7; 16]),
        );
        tokio::spawn(async move { host.propose_command("alice", &stale).await })
    };
    let deadline = Instant::now() + Duration::from_secs(10);
    while host.log_entries().unwrap().len() == before {
        assert!(
            Instant::now() < deadline,
            "the entry never reached the leader's log"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    pending.abort();
    let _ = pending.await;
    let host = Arc::try_unwrap(host)
        .ok()
        .expect("the proposal task released the host");
    host.shutdown().await.unwrap();

    // Start the member again on its directory. It has no leader, and the
    // catalogs recover all the same, through the function the binary calls.
    let host = RaftHost::start_member(
        &member_dir,
        &cluster.group,
        leader,
        &cluster.config,
        raft_kit::transport(leader, &cluster.directory, addr),
    )
    .await
    .unwrap();
    // The library may resume the old leader's role from the committed
    // vote, but both peers still cut this member off: no quorum is in
    // reach, no lease can be granted, and recovery runs all the same.
    let no_lease = host.lease("alice").await.err().expect("no quorum");
    assert_eq!(no_lease.code(), Code::Unavailable, "{no_lease}");
    let recovered = recover_catalogs(&host, "alice", &[entry]).unwrap();
    assert_eq!(recovered.len(), 1);

    // Open the peers to the restarted member while they stay cut from each
    // other, and serve on it.
    for other in &others {
        cluster.host(*other).heal();
        let rest: Vec<u64> = others.iter().copied().filter(|n| n != other).collect();
        cluster.host(*other).isolate(rest);
    }
    let host = Arc::new(host);
    let service =
        HostedDocumentWriteService::new(principals(), Arc::clone(&host), recovered, limits())
            .unwrap();
    let (mut client, server) = serve(service).await;

    // Until it leads the barrier finds no quorum for it and the service
    // rejects; the library elects it once a peer grants the vote.
    let deadline = Instant::now() + Duration::from_secs(30);
    while host.metrics().borrow().state != openraft::ServerState::Leader {
        assert!(
            Instant::now() < deadline,
            "the restarted member never led with the longest log"
        );
        host.trigger_election().await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    for other in &others {
        cluster.host(*other).heal();
    }

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
    let receipt = client
        .accept_document(auth(
            network_write(
                target.history_id,
                b"hosted-after-single-restart",
                b"single-restart-op",
                0,
            ),
            "alice",
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(receipt.accepted && receipt.durable && !receipt.replayed);
    assert_eq!(receipt.version, 1);

    drop(client);
    server.shutdown().await;
    return_host(&mut cluster, leader, host);
    cluster.shutdown().await;
}

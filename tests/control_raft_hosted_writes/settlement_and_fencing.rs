//! Post-lease durable write settlement, retry convergence, and revoked-actor fencing.

use super::*;

// Write outcomes over the hosted service ("Write
// outcomes"): a write whose commit returns after its lease lapsed is
// settled under a fresh lease on the same call; one the leader cannot
// settle is named unconfirmed and settled by the exact retry; one whose
// actor lost the right meanwhile is fenced by name, and stays fenced.

/// Make `node` lead again after a partition: its peers are cut from each
/// other so that, with a log as long as any, it is the only member either
/// of them can elect; then every link is healed.
async fn lead_again(cluster: &Voters, host: &Arc<RaftHost>, others: &[u64]) {
    for other in others {
        let rest: Vec<u64> = others.iter().copied().filter(|n| n != other).collect();
        cluster.host(*other).isolate(rest);
    }
    let deadline = Instant::now() + Duration::from_secs(30);
    while host.metrics().borrow().state != openraft::ServerState::Leader {
        assert!(
            Instant::now() < deadline,
            "the member never led again after the partition"
        );
        host.trigger_election().await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    for other in others {
        cluster.host(*other).heal();
    }
}

fn history_request(history_id: Vec<u8>) -> ReadAcceptedDocumentsRequest {
    ReadAcceptedDocumentsRequest {
        after_sequence: 0,
        limit: 100,
        through_sequence: None,
        max_bytes: 1 << 20,
        history_id,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_durable_after_its_lease_lapsed_is_settled_under_a_fresh_lease() {
    let _serial = serial();
    let config = slow_lease_config();
    let lease = Duration::from_millis(config.admission_lease_ms);
    let mut cluster =
        raft_kit::three_voters_with("hosted-lapsed-settled", &hosted_policy(), &config).await;
    let leader = cluster.leader().await;
    let dir = kit::TestDir::new("hosted-lapsed-settled-catalog");
    let catalog = Arc::new(bridge_active(&mut cluster, leader, &dir, "hosted-prepare").await);
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

    // The commit returns after the lease lapsed; the same call takes a
    // fresh lease, finds the right current and answers accepted.
    catalog.arm_postcommit_pause(lease + Duration::from_millis(200));
    let request = network_write(target.history_id.clone(), b"hosted-lapsed", b"lapsed-op", 0);
    let started = Instant::now();
    let receipt = client
        .accept_document(auth(request.clone(), "alice"))
        .await
        .unwrap()
        .into_inner();
    assert!(
        started.elapsed() > lease,
        "the pause must outlast the lease"
    );
    assert!(receipt.accepted && receipt.durable && !receipt.replayed);
    assert_eq!(receipt.version, 1);
    assert_eq!(receipt.write_epoch, catalog.activation().write_epoch);

    // The retry replays the settled decision; the history shows the row
    // under its epoch, not fenced.
    let replay = client
        .accept_document(auth(request, "alice"))
        .await
        .unwrap()
        .into_inner();
    assert!(replay.replayed && replay.accepted);
    assert_eq!(replay.accepted_sequence, receipt.accepted_sequence);
    let history = host
        .with_admission("alice", |admission| {
            catalog.read_accepted(admission, &history_request(target.history_id.clone()))
        })
        .await
        .unwrap();
    let row = history
        .documents
        .iter()
        .find(|d| d.accepted_sequence == receipt.accepted_sequence)
        .expect("the row is in the history");
    assert!(!row.fenced);
    assert_eq!(row.write_epoch, receipt.write_epoch);

    drop(client);
    server.shutdown().await;
    return_host(&mut cluster, leader, host);
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unconfirmed_write_the_leader_cannot_settle_is_settled_by_the_retry() {
    let _serial = serial();
    let config = slow_lease_config();
    let lease = Duration::from_millis(config.admission_lease_ms);
    let election_max = Duration::from_millis(config.election_timeout_max_ms);
    let mut cluster =
        raft_kit::three_voters_with("hosted-unconfirmed", &hosted_policy(), &config).await;
    let leader = cluster.leader().await;
    let others: Vec<u64> = cluster
        .nodes()
        .into_iter()
        .filter(|n| *n != leader)
        .collect();
    let dir = kit::TestDir::new("hosted-unconfirmed-catalog");
    let catalog = Arc::new(bridge_active(&mut cluster, leader, &dir, "hosted-prepare").await);
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

    // The commit returns after the lease lapsed while every link is cut:
    // no fresh lease can be had, and the call names the durable row as
    // unconfirmed instead of guessing.
    catalog.arm_postcommit_pause(lease * 3);
    let request = network_write(
        target.history_id.clone(),
        b"hosted-unconfirmed",
        b"unconfirmed-op",
        0,
    );
    let call = {
        let mut client = client.clone();
        let request = request.clone();
        tokio::spawn(async move { client.accept_document(auth(request, "alice")).await })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    host.isolate(others.iter().copied());
    for other in &others {
        let rest: Vec<u64> = cluster.nodes().into_iter().filter(|n| n != other).collect();
        cluster.host(*other).isolate(rest);
    }
    let started = Instant::now();
    let error = call.await.unwrap().unwrap_err();
    assert_eq!(error.code(), Code::Unavailable, "{error}");
    assert_eq!(
        reasons::reason_of(&error),
        Some(reasons::OUTCOME_UNCONFIRMED),
        "the call must name the row as durable and unconfirmed: {error}"
    );
    assert!(
        started.elapsed() < lease * 3 + election_max + lease,
        "the answer arrives within the pause plus the read bound: {:?}",
        started.elapsed()
    );

    // Healed, the member with the catalog leads again and the exact retry
    // settles the record: accepted, replayed, the same row.
    host.heal();
    for other in &others {
        cluster.host(*other).heal();
    }
    lead_again(&cluster, &host, &others).await;
    let replay = client
        .accept_document(auth(request.clone(), "alice"))
        .await
        .unwrap()
        .into_inner();
    assert!(
        replay.replayed && replay.accepted && replay.durable,
        "{replay:?}"
    );
    assert_eq!(replay.version, 1);
    let again = client
        .accept_document(auth(request, "alice"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(again, replay);

    drop(client);
    server.shutdown().await;
    return_host(&mut cluster, leader, host);
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unconfirmed_write_whose_actor_was_revoked_meanwhile_is_fenced() {
    let _serial = serial();
    let config = slow_lease_config();
    let lease = Duration::from_millis(config.admission_lease_ms);
    let mut cluster = raft_kit::three_voters_with("hosted-fenced", &hosted_policy(), &config).await;
    let leader = cluster.leader().await;
    let others: Vec<u64> = cluster
        .nodes()
        .into_iter()
        .filter(|n| *n != leader)
        .collect();
    let dir = kit::TestDir::new("hosted-fenced-catalog");
    let catalog = Arc::new(bridge_active(&mut cluster, leader, &dir, "hosted-prepare").await);
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

    // The commit returns after the lease lapsed with the leader cut off;
    // meanwhile the survivors elect and revoke alice.
    catalog.arm_postcommit_pause(lease * 8);
    let request = network_write(target.history_id.clone(), b"hosted-fenced", b"fenced-op", 0);
    let call = {
        let mut client = client.clone();
        let request = request.clone();
        tokio::spawn(async move { client.accept_document(auth(request, "alice")).await })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    host.isolate(others.iter().copied());
    for other in &others {
        cluster.host(*other).isolate([leader]);
    }
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
    let error = call.await.unwrap().unwrap_err();
    assert_eq!(error.code(), Code::Unavailable, "{error}");
    assert_eq!(
        reasons::reason_of(&error),
        Some(reasons::OUTCOME_UNCONFIRMED),
        "{error}"
    );

    // Healed, the old leader applies the revocation and leads again; the
    // retry settles the record and the decision is the fence, at the
    // revision the old leader has applied.
    host.heal();
    for other in &others {
        cluster.host(*other).heal();
    }
    host.wait(Some(Duration::from_secs(30)))
        .applied_index_at_least(Some(revoked_at), "applied")
        .await
        .unwrap();
    lead_again(&cluster, &host, &others).await;
    let fenced = client
        .accept_document(auth(request.clone(), "alice"))
        .await
        .unwrap_err();
    assert_eq!(fenced.code(), Code::FailedPrecondition, "{fenced}");
    let revision = *host.store().unwrap().subscribe_applied().borrow();
    assert!(
        fenced.message().contains(&format!(
            "version 1 at sequence 2 became durable after its admission lapsed and its right was gone at control revision {revision}"
        )),
        "{fenced}"
    );
    assert_eq!(
        reasons::reason_of(&fenced),
        Some(reasons::OUTCOME_FENCED),
        "{fenced}"
    );
    assert!(
        fenced.message().contains("fenced under write epoch 1"),
        "{fenced}"
    );

    // The fence is final: granted again, alice's retry replays it, and the
    // history shows the row marked at that revision. Her next write of
    // another key is new work at the next sequence.
    let mut alice = kit::grant("alice", kit::COLLECTION);
    alice.actions.push(AccessAction::Ingest as i32);
    let regrant = kit::source_command(
        &cluster.group,
        &kit::key(),
        "grant-alice-again",
        cluster.revision,
        2,
        0,
        kit::replace_grants_action(vec![kit::grant("bob", kit::COLLECTION), alice]),
    );
    let decision = host.propose_command("bob", &regrant).await.unwrap();
    assert_eq!(decision.code, 0, "{}", decision.message);
    cluster.revision += 1;
    let again = client
        .accept_document(auth(request, "alice"))
        .await
        .unwrap_err();
    assert_eq!(again.code(), Code::FailedPrecondition, "{again}");
    assert_eq!(again.message(), fenced.message());
    let history = host
        .with_admission("alice", |admission| {
            catalog.read_accepted(admission, &history_request(target.history_id.clone()))
        })
        .await
        .unwrap();
    let row = history
        .documents
        .iter()
        .find(|d| d.accepted_sequence == 2)
        .expect("the fenced row stays in the history");
    assert!(row.fenced);
    assert_eq!(row.fenced_at_revision, revision);
    assert_eq!(row.write_epoch, 1);
    let next = client
        .accept_document(auth(
            network_write(
                target.history_id.clone(),
                b"hosted-after-fence",
                b"after-fence-op",
                0,
            ),
            "alice",
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(next.accepted && !next.replayed);
    assert_eq!(next.accepted_sequence, 3);

    drop(client);
    server.shutdown().await;
    return_host(&mut cluster, leader, host);
    cluster.shutdown().await;
}

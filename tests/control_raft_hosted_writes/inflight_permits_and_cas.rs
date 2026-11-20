//! Dropped-client permit retention, revoked-access receipt withholding, and CAS.

use super::*;

fn one_in_flight() -> DocumentWriteServiceLimits {
    DocumentWriteServiceLimits {
        max_request_bytes: 64 * 1024,
        max_pending_bytes: 256 * 1024,
        max_in_flight: 1,
    }
}

// A client that drops its call: the worker finishes on its own terms, the
// permits are released only when it returns, and the write is durable.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_that_drops_its_call_leaves_the_worker_to_finish_under_its_permits() {
    let _serial = serial();
    let mut cluster = raft_kit::three_voters_with(
        "hosted-dropped-call",
        &hosted_policy(),
        &slow_lease_config(),
    )
    .await;
    let leader = cluster.leader().await;
    let dir = kit::TestDir::new("hosted-dropped-call-catalog");
    let catalog = Arc::new(bridge_active(&mut cluster, leader, &dir, "hosted-prepare").await);
    let host = take_host(&mut cluster, leader);
    let service = HostedDocumentWriteService::new(
        principals(),
        Arc::clone(&host),
        vec![Arc::clone(&catalog)],
        one_in_flight(),
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

    // Park the next write inside its transaction, after the staging and
    // before the final check, for less than the lease; drop the call while
    // it is parked.
    let pause = Duration::from_millis(300);
    catalog.arm_precommit_pause(pause);
    let request = network_write(
        target.history_id.clone(),
        b"hosted-dropped",
        b"dropped-op",
        0,
    );
    let started = Instant::now();
    let dropped = tokio::time::timeout(
        Duration::from_millis(60),
        client.accept_document(auth(request.clone(), "alice")),
    )
    .await;
    assert!(
        dropped.is_err(),
        "the call must still be parked when the client drops it"
    );

    // The worker holds the only in-flight permit until it returns.
    let refused = client
        .accept_document(auth(
            network_write(target.history_id.clone(), b"hosted-second", b"second-op", 0),
            "alice",
        ))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Code::ResourceExhausted, "{refused}");
    assert!(
        refused
            .message()
            .contains("source execution capacity is full"),
        "{refused}"
    );
    assert!(
        started.elapsed() < pause,
        "the second call must arrive while the first is parked"
    );

    // The worker commits with no client listening: once its permit is
    // free, the exact retry replays the durable row.
    let deadline = Instant::now() + Duration::from_secs(10);
    let replay = loop {
        match client.accept_document(auth(request.clone(), "alice")).await {
            Ok(receipt) => break receipt.into_inner(),
            Err(status) if status.code() == Code::ResourceExhausted => {
                assert!(
                    Instant::now() < deadline,
                    "the permit was never released: {status}"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(status) => panic!("the retry failed for another reason: {status}"),
        }
    };
    assert!(
        replay.replayed && replay.durable,
        "the dropped call's write must be durable and replay"
    );
    assert_eq!(replay.version, 1);
    assert!(
        started.elapsed() >= pause,
        "the retry can only answer after the worker's pause"
    );

    drop(client);
    server.shutdown().await;
    return_host(&mut cluster, leader, host);
    cluster.shutdown().await;
}

// The transport policy changes under a committed write: the row is durable
// and the receipt is withheld by the transport gate, by name.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_receipt_is_kept_back_when_transport_access_changes_under_a_committed_write() {
    let _serial = serial();
    let mut cluster = raft_kit::three_voters_with(
        "hosted-receipt-race",
        &hosted_policy(),
        &slow_lease_config(),
    )
    .await;
    let leader = cluster.leader().await;
    let dir = kit::TestDir::new("hosted-receipt-race-catalog");
    let catalog = Arc::new(bridge_active(&mut cluster, leader, &dir, "hosted-prepare").await);
    let host = take_host(&mut cluster, leader);
    let transport_policy = Arc::new(PolicyAuthority::new(hosted_policy()).unwrap());
    let service = HostedDocumentWriteService::new(
        principals_with(Arc::clone(&transport_policy)),
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

    // Park the write inside its transaction and replace the transport
    // policy without alice while it is parked. The authority policy, which
    // admits the write, is unchanged: the commit goes through, and the
    // transport gate withholds the receipt after the worker returns.
    catalog.arm_precommit_pause(Duration::from_millis(300));
    let request = network_write(
        target.history_id.clone(),
        b"hosted-withheld",
        b"withheld-op",
        0,
    );
    let call = {
        let mut client = client.clone();
        let request = request.clone();
        tokio::spawn(async move { client.accept_document(auth(request, "alice")).await })
    };
    tokio::time::sleep(Duration::from_millis(80)).await;
    let mut without_alice = hosted_policy();
    without_alice.revision = 2;
    without_alice
        .grants
        .retain(|grant| grant.principal != "alice");
    transport_policy.replace(without_alice).unwrap();
    let withheld = call.await.unwrap().unwrap_err();
    assert_eq!(withheld.code(), Code::PermissionDenied, "{withheld}");
    assert_eq!(
        reasons::reason_of(&withheld),
        Some(reasons::RECEIPT_POLICY_CHANGED),
        "the withheld receipt must name the policy change: {withheld}"
    );

    // Durable regardless: with alice granted again, the exact retry replays.
    let mut restored = hosted_policy();
    restored.revision = 3;
    transport_policy.replace(restored).unwrap();
    let replay = client
        .accept_document(auth(request, "alice"))
        .await
        .unwrap()
        .into_inner();
    assert!(replay.replayed && replay.durable);
    assert_eq!(replay.version, 1);

    drop(client);
    server.shutdown().await;
    return_host(&mut cluster, leader, host);
    cluster.shutdown().await;
}

// The conditional write over the hosted service (
// "Identity and retry rules"): two writers name the same expected version
// of one key, one wins, the other is ABORTED by name inside the write
// transaction and leaves no version and no operation record, so its
// corrected request is new work. The rule holds through a member that
// does not lead as well.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_version_precondition_is_decided_in_the_write_transaction_and_a_loser_leaves_no_record() {
    let _serial = serial();
    let mut cluster = raft_kit::three_voters("hosted-version-race", &hosted_policy()).await;
    let leader = cluster.leader().await;
    let follower = *cluster
        .nodes()
        .iter()
        .find(|n| **n != leader)
        .expect("three voters");
    let dir = kit::TestDir::new("hosted-version-race-catalog");
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

    // Two writers, one key, both expecting no prior version: the catalog's
    // writer lock orders them, the first commits version 1, the second is
    // ABORTED by name.
    let first = network_write(target.history_id.clone(), b"raced", b"race-op-one", 0);
    let second = network_write(target.history_id.clone(), b"raced", b"race-op-two", 0);
    let (won, lost) = {
        let mut one = client.clone();
        let mut two = client.clone();
        let (a, b) = tokio::join!(
            one.accept_document(auth(first.clone(), "alice")),
            two.accept_document(auth(second.clone(), "alice"))
        );
        match (a, b) {
            (Ok(receipt), Err(status)) => (receipt.into_inner(), (status, second.clone())),
            (Err(status), Ok(receipt)) => (receipt.into_inner(), (status, first.clone())),
            (Ok(a), Ok(b)) => panic!("both writers won: {a:?} {b:?}"),
            (Err(a), Err(b)) => panic!("neither writer won: {a} {b}"),
        }
    };
    assert_eq!(won.version, 1);
    assert!(won.accepted && won.durable && !won.replayed);
    let (status, loser) = lost;
    assert_eq!(status.code(), Code::Aborted, "{status}");
    assert_eq!(
        reasons::reason_of(&status),
        Some(reasons::OUTCOME_VERSION_MISMATCH),
        "{status}"
    );

    // The loser left no record: its exact retry is not a replay of the
    // rejection but the same precondition failing again, and the corrected
    // request (expecting version 1) is new work at version 2.
    let again = client
        .accept_document(auth(loser.clone(), "alice"))
        .await
        .unwrap_err();
    assert_eq!(again.code(), Code::Aborted, "{again}");
    let mut corrected = loser;
    corrected.document.as_mut().unwrap().expected_version = Some(1);
    let receipt = client
        .accept_document(auth(corrected.clone(), "alice"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(receipt.version, 2);
    assert!(!receipt.replayed);
    assert_eq!(receipt.accepted_sequence, won.accepted_sequence + 1);
    // Sending the corrected request under the loser's operation id is a
    // different request under a used id: ALREADY_EXISTS, no third version.
    let mut reused = corrected;
    reused.document.as_mut().unwrap().expected_version = Some(2);
    let error = client
        .accept_document(auth(reused, "alice"))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::AlreadyExists, "{error}");

    // The precondition is checked on the follower's own view under a
    // forwarded lease too: version 2 is what it sees.
    drop(client);
    server.shutdown().await;
    return_host(&mut cluster, leader, host);
    let host = take_host(&mut cluster, follower);
    let service = HostedDocumentWriteService::new(
        principals(),
        Arc::clone(&host),
        vec![Arc::clone(&catalog)],
        limits(),
    )
    .unwrap();
    let (mut client, server) = serve(service).await;
    let stale = network_write(target.history_id.clone(), b"raced", b"race-op-three", 1);
    let error = client
        .accept_document(auth(stale, "alice"))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::Aborted, "{error}");
    let current = network_write(target.history_id, b"raced", b"race-op-three", 2);
    let receipt = client
        .accept_document(auth(current, "alice"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(receipt.version, 3);
    assert!(!receipt.replayed);

    drop(client);
    server.shutdown().await;
    return_host(&mut cluster, follower, host);
    cluster.shutdown().await;
}

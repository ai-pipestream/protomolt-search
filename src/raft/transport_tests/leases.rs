//! Admission leases across a barrier, an open interval, and a
//! forwarded grant.

use super::quorum::revoke_alice;
use super::*;

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
    assert_reason(
        &error,
        Code::FailedPrecondition,
        super::super::reasons::LEASE_INTERVAL_ELAPSED,
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
            .map(super::super::types::log_id_to_proto),
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

/// A lease the leader forwarded counts toward its hold (I2): the asking
/// member took no lease on the leader itself, and the leader still
/// withholds its vote from a candidate while the forwarded interval may
/// be open, extended from the request's receipt.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leader_withholds_its_vote_for_a_lease_it_forwarded() {
    let cluster = three_voters("vote-hold-forwarded").await;
    let leader = cluster.leader().await;
    assert!(
        !cluster.host(leader).test_application_lease_held(),
        "fixture readiness must not supply the application lease hold being tested"
    );
    let member = if leader == 1 { 2 } else { 1 };
    let candidate = [1, 2, 3]
        .into_iter()
        .find(|n| *n != leader && *n != member)
        .unwrap();
    // The member has applied everything the leader has, so the forwarded
    // grant needs no catch-up inside its interval; the lease then goes
    // through the leader's barrier over GrantLease, and the leader's own
    // hold moves with it.
    let applied = cluster
        .host(leader)
        .applied_position()
        .unwrap()
        .unwrap()
        .index;
    cluster.wait_applied(member, applied).await;
    cluster
        .host(member)
        .with_admission("alice", |admission| {
            admission.authorize("books", AccessAction::Ingest)
        })
        .await
        .unwrap();
    assert_ne!(
        cluster.host(member).metrics().borrow().state,
        openraft::ServerState::Leader,
        "the lease was forwarded, not local"
    );
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
            .map(super::super::types::log_id_to_proto),
    };
    let reply = client.vote(timed(request)).await.unwrap().into_inner();
    assert!(
        !reply.vote_granted,
        "the vote is withheld for the forwarded lease"
    );
    assert_eq!(reply.vote.unwrap().term, metrics.current_term);
    assert_eq!(
        cluster.host(leader).metrics().borrow().state,
        openraft::ServerState::Leader
    );
    cluster.shutdown().await;
}

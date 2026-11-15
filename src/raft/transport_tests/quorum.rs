//! Three voters: seeding by snapshot, an isolated leader and the
//! surviving quorum's revocation, a restart, and a replaced voter.

use super::*;

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
        assert!(super::super::state_machine::published_generation(
            &cluster.dirs[&node].0.join(super::super::host::SNAPSHOT_DIR)
        )
        .unwrap()
        .is_some());
    }
    // Entries proposed on the leader apply on every voter with the same
    // position and rows.
    let index = cluster.propose_prepare("phone-b").await;
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
    let leader = cluster.leader().await;
    let follower = if leader == 1 { 2 } else { 1 };
    let error = cluster
        .host(follower)
        .propose_command(
            "alice",
            &prepare(&cluster.group, "phone-c", "phone-c", cluster.revision),
        )
        .await
        .unwrap_err();
    assert_reason(
        &error,
        Code::Unavailable,
        super::super::reasons::LEASE_NOT_LEADER,
    );
    assert!(error.message().contains("not the leader"), "{error}");
    prepared_store_refuses_before_seed(&cluster.group);
    cluster.shutdown().await;
}

/// A prepared member store refuses to apply anything before its seed.
pub(super) fn prepared_store_refuses_before_seed(group: &SourceAuthorityIdentity) {
    let pending = Directory::new("pending");
    RaftHost::prepare_member(&pending.0, group, 4, &policy(), &limits()).unwrap();
    let store = crate::source_authority::SourceAuthorityStore::open(
        &pending.0.join(super::super::host::STORE_FILE),
        group,
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
    assert_reason(
        &error,
        Code::FailedPrecondition,
        super::super::reasons::MEMBERSHIP_AWAITING_SNAPSHOT,
    );
}

pub(super) fn revoke_alice(
    group: &SourceAuthorityIdentity,
    revision: u64,
) -> SourceAuthorityCommand {
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

/// Alice's ingest admitted through `host`'s lease.
pub(super) async fn admit_alice(host: &RaftHost) -> Result<(), tonic::Status> {
    host.with_admission("alice", |admission| {
        admission.authorize("books", AccessAction::Ingest)
    })
    .await
    .map(|_| ())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_isolated_leader_admits_nothing_and_the_surviving_quorum_revokes() {
    let mut cluster = three_voters("isolation").await;
    let leader = cluster.leader().await;
    // A lease on the leader with a quorum admits alice's ingest.
    let granted = Instant::now();
    admit_alice(cluster.host(leader)).await.unwrap();
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
    let error = admit_alice(cluster.host(leader)).await.unwrap_err();
    assert_reason(
        &error,
        Code::Unavailable,
        super::super::reasons::LEASE_NO_LINEARIZABLE_READ,
    );
    let revoked_at = revoke_on(&mut cluster, successor).await;
    stale_side_grants_nothing(&cluster, leader).await;
    heal_and_deny(&cluster, leader, revoked_at).await;
    cluster.shutdown().await;
}

/// The surviving quorum revokes alice; returns the revocation's index.
pub(super) async fn revoke_on(cluster: &mut Cluster, successor: NodeId) -> u64 {
    let decision = cluster
        .host(successor)
        .propose_command("bob", &revoke_alice(&cluster.group, cluster.revision))
        .await
        .unwrap();
    assert_eq!(decision.code, 0, "{}", decision.message);
    cluster.revision += 1;
    let applied = cluster.host(successor).applied_position().unwrap();
    applied.unwrap().index
}

/// The old side still shows the stale grant, but grants nothing on it:
/// the local admission is closed and the leased one cannot be renewed.
pub(super) async fn stale_side_grants_nothing(cluster: &Cluster, leader: NodeId) {
    let stale = cluster.host(leader).store().unwrap();
    let policy = stale.policy("alice", "workspace-a", "books").unwrap();
    assert_eq!(policy.revision, 1);
    let error = match stale.admission("alice") {
        Ok(_) => panic!("a hosted store granted a local admission"),
        Err(error) => error,
    };
    assert_eq!(error.code(), Code::FailedPrecondition, "{error}");
    drop(stale);
    let error = admit_alice(cluster.host(leader)).await.unwrap_err();
    assert_eq!(error.code(), Code::Unavailable, "{error}");
}

/// Heal: the old leader catches up as a follower of the successor, and
/// alice is denied on the merits there and on the current leader.
pub(super) async fn heal_and_deny(cluster: &Cluster, leader: NodeId, revoked_at: u64) {
    // Confirm the surviving leader before healing. Applied bytes alone do
    // not establish that the healed replica knows where to forward a read.
    let successor = cluster.leader_other_than(leader).await;
    for host in cluster.hosts.values() {
        host.heal();
    }
    cluster
        .host(leader)
        .wait(Some(Duration::from_secs(30)))
        .metrics(
            |metrics| {
                metrics
                    .last_applied
                    .is_some_and(|id| id.index >= revoked_at)
                    && metrics.current_leader == Some(successor)
                    && metrics.state == openraft::ServerState::Follower
            },
            "healed follower applied revocation and knows the confirmed leader",
        )
        .await
        .unwrap();
    let healed = cluster.host(leader).store().unwrap();
    let policy = healed.policy("bob", "workspace-a", "books").unwrap();
    assert_eq!(policy.revision, 2);
    drop(healed);
    // The healed old leader forwards its lease to the successor and grants
    // on the revocation it has applied.
    let error = admit_alice(cluster.host(leader)).await.unwrap_err();
    assert_eq!(error.code(), Code::PermissionDenied, "{error}");
    // On the current leader the revocation is what a fresh admission sees.
    let successor = cluster.leader().await;
    let error = admit_alice(cluster.host(successor)).await.unwrap_err();
    assert_eq!(error.code(), Code::PermissionDenied, "{error}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_restarts_and_replays_the_entries_it_missed() {
    let mut cluster = three_voters("restart").await;
    let leader = cluster.leader().await;
    let absent = if leader == 3 { 2 } else { 3 };
    let host = cluster.hosts.remove(&absent).unwrap();
    let listen = host.listen_addr().unwrap();
    host.shutdown().await.unwrap();
    cluster.propose_prepare("phone-b").await;
    let index = cluster.propose_prepare("phone-c").await;
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
    let leader = cluster.leader().await;
    cluster.restart(leader).await;
    let index = cluster.propose_prepare("phone-d").await;
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
    cluster.join(4).await;
    assert!(!cluster.host(4).awaiting_snapshot().unwrap());
    let leader = cluster.leader().await;
    cluster
        .host(leader)
        .promote(BTreeSet::from([4]))
        .await
        .unwrap();
    let leader = cluster.leader().await;
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
    let index = cluster.propose_prepare("phone-b").await;
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

//! In-flight build learner seeding, join/trigger races, and pending-member log recovery.

use super::*;

/// A join asked while a build is in flight, begun before the position
/// moved: the library drops the join's trigger without a word, and the
/// in-flight build lands below the applied position. The seed repeats its
/// trigger, so the join is seeded from a build at the position and
/// answered, instead of waiting out its window on a leader that is well.
#[cfg(feature = "fault-injection")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_join_issued_during_a_build_in_flight_is_seeded_after_that_build_completes() {
    let _serial = serial();
    let group = kit::identity(kit::SEED);
    let mut cluster =
        Cluster::bootstrap("join-during-build", &group, &kit::policy(), &kit::limits()).await;
    propose_ok(cluster.leader(), &prepare_command(&group, 1)).await;
    // A build parked before it publishes, at the position before the
    // grant; the grant moves the applied position past it.
    let build_gate = cluster.leader().arm_snapshot_build_gate();
    cluster.leader().trigger_snapshot().await.unwrap();
    build_gate.reached().await;
    propose_ok(cluster.leader(), &grant_command(&group, 2, 1, "bob")).await;
    let applied = raft_kit::durable_position(cluster.leader());
    cluster.start_member().await;
    let addr = cluster.member().advertised_addr().unwrap().to_string();
    // The join's own trigger is dropped while the build is parked; it is
    // inside its wait when the build lands below the position.
    let (join, ()) = tokio::join!(
        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            cluster.leader().add_learner(raft_kit::MEMBER_ID, &addr),
        ),
        async {
            tokio::time::sleep(std::time::Duration::from_secs(3)).await;
            build_gate.release();
        }
    );
    join.expect("the join answers inside 30 s")
        .expect("the join was seeded after the in-flight build landed");
    let build = cluster
        .leader()
        .last_snapshot_build()
        .expect("the leader reports its build");
    assert!(
        build.last_log_id.is_some_and(|id| id.index >= applied),
        "the seed's build is below the applied position: {build:?}"
    );
    assert!(!cluster.member().awaiting_snapshot().unwrap());
    raft_kit::wait_applied(cluster.member(), applied).await;
    assert!(raft_kit::core_running(cluster.leader()));
    assert!(raft_kit::core_running(cluster.member()));
    cluster.shutdown().await;
}

/// With no fault-injection gate, a
/// snapshot trigger and an `add_learner` issued at the same instant on a
/// leader that already holds a published generation. `add_learner` runs its
/// own seed boundary (build, then purge) and serves the learner from the
/// pointer, so one build's publish and the other path's read of the
/// generation overlap.
///
/// Under the log-and-store agreement contract,
/// Serve: a serve is one step under the pointer lock, a publish waits for
/// it, and only a local storage failure stops a core. The leader's core
/// keeps running, the learner is seeded, and the group commits afterwards.
///
/// The gated target
/// (`a_serve_under_a_concurrent_publish_returns_the_generation_it_read`) is
/// what pins the interleaving itself; this one pins the outcome in the
/// shape a group produces on its own, `ROUNDS` times over.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_trigger_racing_a_join_seeds_the_learner_with_the_leader_core_running() {
    const ROUNDS: u64 = 16;
    let _serial = serial();
    let group = kit::identity(kit::SEED);
    for round in 0..ROUNDS {
        let mut cluster = Cluster::bootstrap(
            &format!("trigger-join-race-{round}"),
            &group,
            &kit::policy(),
            &kit::limits(),
        )
        .await;
        propose_ok(cluster.leader(), &prepare_command(&group, 1)).await;
        raft_kit::snapshot_image(&cluster.leader_dir, cluster.leader()).await;
        let first = raft_kit::published_generation(cluster.leader_dir.path())
            .expect("the leader publishes its first generation");
        // A committed change past that generation, so the trigger racing
        // the join has a newer position to image.
        propose_ok(cluster.leader(), &grant_command(&group, 2, 1, "bob")).await;
        let applied = raft_kit::durable_position(cluster.leader());
        raft_kit::wait_applied(cluster.leader(), applied).await;
        cluster.start_member().await;
        assert!(cluster.member().awaiting_snapshot().unwrap());
        let addr = cluster.member().advertised_addr().unwrap().to_string();

        let (triggered, joined) = tokio::join!(
            cluster.leader().trigger_snapshot(),
            cluster.leader().add_learner(raft_kit::MEMBER_ID, &addr),
        );
        assert!(
            raft_kit::core_running(cluster.leader()),
            "round {round}: the leader core stopped under a trigger racing the join \
             (trigger {triggered:?}, join {joined:?}, generation at entry {first})"
        );
        triggered.unwrap_or_else(|e| panic!("round {round}: the trigger did not run: {e}"));
        joined.unwrap_or_else(|e| panic!("round {round}: the join did not complete: {e}"));

        assert!(
            !cluster.member().awaiting_snapshot().unwrap(),
            "round {round}: the learner was not seeded"
        );
        assert!(
            raft_kit::published_generation(cluster.member_dir.path()).is_some(),
            "round {round}: the learner holds no generation of its own"
        );
        assert!(raft_kit::core_running(cluster.member()));
        let seeded = raft_kit::durable_position(cluster.member());
        assert!(
            seeded >= applied,
            "round {round}: the learner is at {seeded}, behind the leader's {applied}"
        );

        // The group commits afterwards and the learner applies it.
        propose_ok(cluster.leader(), &grant_command(&group, 3, 2, "carol")).await;
        let after = raft_kit::durable_position(cluster.leader());
        assert!(after > applied, "round {round}: the leader did not advance");
        raft_kit::wait_applied(cluster.member(), after).await;
        assert!(raft_kit::core_running(cluster.leader()));
        cluster.shutdown().await;
    }
}

/// Shared pending-member fixture: a prepared
/// member whose log advanced while its state machine applied nothing. The
/// raw registered peer (node 3's certificate, the R3/R5 adversary) appends
/// two blank entries under the leader's committed vote with nothing
/// committed, so the log takes them and the marked store applies none of
/// them.
async fn pending_member_with_an_advanced_log(
    name: &str,
    group: &SourceAuthorityIdentity,
) -> (Cluster, std::net::SocketAddr) {
    let mut cluster = Cluster::bootstrap(name, group, &kit::policy(), &kit::limits()).await;
    propose_ok(cluster.leader(), &prepare_command(group, 1)).await;
    cluster.start_member().await;
    let listen = cluster.member().listen_addr().unwrap();
    let vote = raft_kit::current_vote(cluster.leader());
    let mut peer = raft_kit::peer_client(listen, raft_kit::PEER_ID).await;
    raft_kit::append_blanks(
        &mut peer,
        group,
        raft_kit::PEER_ID,
        raft_kit::MEMBER_ID,
        &vote,
        2,
    )
    .await
    .expect("the registered peer's appends are taken");
    let metrics = cluster.member().metrics().borrow().clone();
    assert_eq!(
        metrics.last_log_index,
        Some(1),
        "the member's log did not advance: {metrics}"
    );
    assert_eq!(metrics.last_applied, None, "the member applied an entry");
    assert_eq!(cluster.member().applied_position().unwrap(), None);
    assert!(cluster.member().awaiting_snapshot().unwrap());
    assert!(raft_kit::core_running(cluster.member()));
    (cluster, listen)
}

/// Such a member, stopped and started again on the
/// same directory. At 0fe7081 the start ended in the builder's
/// `FailedPrecondition` ("no applied position; nothing to snapshot"),
/// because the library builds a snapshot at startup when the log names a
/// purged position and no generation is published. `RaftLogStore` bounds
/// every purge by the hosted store's applied position (log-and-store agreement), which for this member is nothing, so the log
/// names no purged position and the start has no snapshot to build.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pending_member_with_an_advanced_log_and_no_apply_starts_again() {
    let _serial = serial();
    let group = kit::identity(kit::SEED);
    let (mut cluster, listen) =
        pending_member_with_an_advanced_log("pending-log-restart", &group).await;

    cluster.stop_member().await;
    let started = RaftHost::start_member(
        cluster.member_dir.path(),
        &group,
        raft_kit::MEMBER_ID,
        &raft_kit::host_config(),
        raft_kit::transport(raft_kit::MEMBER_ID, &cluster.directory, listen),
    )
    .await;
    let member = started.unwrap_or_else(|status| {
        panic!(
            "a pending member whose log advanced without an apply did not start again: {:?} {}",
            status.code(),
            status.message()
        )
    });
    assert_eq!(member.metrics().borrow().last_log_index, Some(1));
    assert_eq!(member.applied_position().unwrap(), None);
    assert!(member.awaiting_snapshot().unwrap());
    assert!(raft_kit::core_running(&member));
    assert!(raft_kit::published_generation(cluster.member_dir.path()).is_none());
    cluster.member = Some(member);
    cluster.shutdown().await;
}

/// The group's recovery of that member by seeding, which
/// the contract states as "the member restarts and is seeded
/// again". The library purges the member's log for the incoming snapshot
/// before the state machine has installed it; that purge is bounded by the
/// store's applied position, which is nothing at that moment, so it moves
/// no entry and is recorded as deferred. The install applies the leader's
/// position, and the leader's next append completes the purge to it first,
/// so the entries from before the seed go and the append lands at the
/// position after the snapshot; the member applies it with its core
/// running. Before the record (slice 4c), the log kept those entries and
/// the append opened a gap the log named as data loss, which stopped the
/// member's core (`kimi-recovery-residual2.log`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pending_member_with_an_advanced_log_and_no_apply_is_seeded_and_applies() {
    let _serial = serial();
    let group = kit::identity(kit::SEED);
    let (cluster, _) = pending_member_with_an_advanced_log("pending-log-seed", &group).await;

    let addr = cluster.member().advertised_addr().unwrap().to_string();
    let joined = tokio::time::timeout(
        std::time::Duration::from_secs(20),
        cluster.leader().add_learner(raft_kit::MEMBER_ID, &addr),
    )
    .await;
    assert!(
        raft_kit::core_running(cluster.member()),
        "the member core stopped after its seed: {:?} (join outcome {joined:?}, applied {:?})",
        cluster.member().metrics().borrow().running_state,
        cluster.member().applied_position()
    );
    joined
        .expect("the join answered inside 20 s")
        .expect("the learner was added");
    assert!(!cluster.member().awaiting_snapshot().unwrap());
    assert!(raft_kit::published_generation(cluster.member_dir.path()).is_some());
    propose_ok(cluster.leader(), &grant_command(&group, 2, 1, "bob")).await;
    let after = raft_kit::durable_position(cluster.leader());
    raft_kit::wait_applied(cluster.member(), after).await;
    assert_eq!(raft_kit::durable_position(cluster.member()), after);
    assert!(raft_kit::core_running(cluster.member()));
    assert!(raft_kit::core_running(cluster.leader()));
    cluster.shutdown().await;
}

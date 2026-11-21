//! Concurrent publish serving, peer backoff, bound increases, and oversize peer refusal.

use super::*;

/// The serve path beside the publish path. The leader serves its published
/// generation to a lagging peer on the state machine worker
/// (`get_current_snapshot`) while a build, in its own task, publishes the
/// next generation and removes the rest. Before this checkpoint the serve
/// read the pointer and then the generation with no lock between them; a
/// publish landing between the two removed the generation under the
/// serve, the read failed as a storage error and the core stopped, with
/// no invalid data and no storage fault anywhere. Now a serve is one step
/// under the pointer lock, from reading the pointer to opening the image;
/// a publish waits for it, and an open file outlives the directory's
/// removal. The two gates pin the interleaving: the build is paused just
/// before it publishes, the serve just after it read the pointer, and the
/// build is released first.
#[cfg(feature = "fault-injection")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_serve_under_a_concurrent_publish_returns_the_generation_it_read() {
    let _serial = serial();
    let group = kit::identity(kit::SEED);
    let dir = kit::TestDir::new("serve-under-publish");
    let host = std::sync::Arc::new(raft_kit::bootstrap_host(&dir).await);
    propose_ok(&host, &prepare_command(&group, 1)).await;
    let (first_image, first_meta) = raft_kit::snapshot_image(&dir, &host).await;
    let first = raft_kit::published_generation(dir.path()).unwrap();
    propose_ok(&host, &grant_command(&group, 2, 1, "bob")).await;
    let applied = raft_kit::durable_position(&host);
    raft_kit::wait_applied(&host, applied).await;
    assert!(first_meta.last_log_id.unwrap().index < applied);

    // A build in flight in its own task: image copied, generation
    // directory in place, paused before it publishes.
    let build_gate = host.arm_snapshot_build_gate();
    host.trigger_snapshot().await.unwrap();
    build_gate.reached().await;

    // A serve on the state machine worker, paused after it read the
    // pointer and before it opened the generation.
    let serve_gate = host.arm_snapshot_serve_gate();
    let serving = {
        let host = std::sync::Arc::clone(&host);
        tokio::spawn(async move { host.serve_snapshot().await })
    };
    serve_gate.reached().await;

    // The build may publish now. It has this long to move the pointer and
    // remove the first generation under the paused serve.
    build_gate.release();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let pointer_during_pause = raft_kit::published_generation(dir.path());
    let first_image_present = raft_kit::generation_dir(dir.path(), first)
        .join("image.redb")
        .exists();

    serve_gate.release();
    let served = serving.await.unwrap();
    let outcome = served.as_ref().map(|s| {
        s.as_ref()
            .map(|(meta, image)| (meta.snapshot_id.clone(), image.len()))
    });
    assert!(
        raft_kit::core_running(&host),
        "the core stopped: the serve read generation {first}, the publish moved the pointer \
         to {pointer_during_pause:?} under it (image present: {first_image_present}); \
         serve outcome {outcome:?}"
    );
    let (served_meta, served_image) = served
        .expect("the serve completed")
        .expect("a generation was published");
    assert_eq!(
        served_meta, first_meta,
        "the serve returned another generation's meta"
    );
    assert_eq!(served_image, first_image, "the serve returned other bytes");
    assert_eq!(
        pointer_during_pause,
        Some(first),
        "the pointer moved under a serve that had read it"
    );
    assert!(
        first_image_present,
        "the generation was removed under a serve that had read the pointer to it"
    );

    // The build publishes once the serve is done; the first generation goes
    // with it, and the core keeps committing.
    host.wait(Some(std::time::Duration::from_secs(30)))
        .metrics(
            |metrics| metrics.snapshot.is_some_and(|s| s.index >= applied),
            "the next generation built",
        )
        .await
        .unwrap();
    let second = raft_kit::published_generation(dir.path()).unwrap();
    assert!(second > first, "the build did not publish after the serve");
    assert!(!raft_kit::generation_dir(dir.path(), first).exists());
    let (_, second_meta) = raft_kit::published_pair(dir.path()).unwrap();
    assert_eq!(second_meta.last_log_id.unwrap().index, applied);
    assert!(raft_kit::core_running(&host));
    propose_ok(&host, &grant_command(&group, 3, 2, "carol")).await;
    assert_eq!(raft_kit::durable_position(&host), applied + 1);
    std::sync::Arc::try_unwrap(host)
        .ok()
        .expect("no task holds the host")
        .shutdown()
        .await
        .unwrap();
}

/// The leader keeps serving a peer that rejects its seed, with the
/// library's backoff between attempts: one serve per attempt, none of
/// them closer than the backoff. Each attempt reads the published
/// generation once, and that read crosses the armed serve gate, so one
/// crossing is one attempt.
#[cfg(feature = "fault-injection")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_leader_keeps_serving_a_peer_that_rejects_its_seed_with_backoff() {
    let _serial = serial();
    let group = kit::identity(kit::SEED);
    let bound = raft_kit::CHUNK_BYTES as u64;
    let mut small = raft_kit::host_config();
    small.max_snapshot_bytes = bound;
    let mut cluster = Cluster::bootstrap_with_config(
        "peer-bound-retry",
        &group,
        &kit::policy(),
        &kit::limits(),
        &small,
    )
    .await;
    propose_ok(cluster.leader(), &prepare_command(&group, 1)).await;
    let (image, _) = raft_kit::snapshot_image(&cluster.leader_dir, cluster.leader()).await;
    assert!(image.len() as u64 > bound);
    cluster.start_member_with(&small).await;
    let addr = cluster.member().advertised_addr().unwrap().to_string();
    let rejected = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        cluster.leader().add_learner(raft_kit::MEMBER_ID, &addr),
    )
    .await
    .expect("the leader answers inside 10 s")
    .expect_err("the learner was seeded past its bound");
    assert_eq!(rejected.code(), Code::ResourceExhausted, "{rejected}");

    let mut crossings = Vec::new();
    for _ in 0..3 {
        let gate = cluster.leader().arm_snapshot_serve_gate();
        tokio::time::timeout(std::time::Duration::from_secs(20), gate.reached())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "the leader did not serve again within 20 s after {} crossings",
                    crossings.len()
                )
            });
        crossings.push(std::time::Instant::now());
        gate.release();
    }
    for pair in crossings.windows(2) {
        let gap = pair[1].duration_since(pair[0]);
        assert!(
            gap >= std::time::Duration::from_millis(500),
            "two attempts {gap:?} apart; the library's backoff between attempts is 500 ms"
        );
    }
    assert!(raft_kit::core_running(cluster.leader()));
    assert!(raft_kit::core_running(cluster.member()));
    assert!(cluster.member().awaiting_snapshot().unwrap());
    let recorded = cluster.leader().peer_rejections();
    assert!(
        recorded
            .get(&raft_kit::MEMBER_ID)
            .is_some_and(|r| r.count >= 3),
        "the leader recorded {:?}",
        recorded.get(&raft_kit::MEMBER_ID)
    );
    cluster.shutdown().await;
}

/// The recovery the hosting document states: a peer rejected at its
/// bound is seeded once it is started again under a bound above the
/// image. Its log never advanced while it was rejecting (the seed is the
/// only channel the leader has to it), so the restart is a plain one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_peer_rejected_at_its_bound_is_seeded_after_a_restart_under_a_larger_bound() {
    let _serial = serial();
    let group = kit::identity(kit::SEED);
    let bound = raft_kit::CHUNK_BYTES as u64;
    let mut small = raft_kit::host_config();
    small.max_snapshot_bytes = bound;
    let mut cluster = Cluster::bootstrap_with_config(
        "peer-bound-raised",
        &group,
        &kit::policy(),
        &kit::limits(),
        &small,
    )
    .await;
    propose_ok(cluster.leader(), &prepare_command(&group, 1)).await;
    let (image, _) = raft_kit::snapshot_image(&cluster.leader_dir, cluster.leader()).await;
    assert!(image.len() as u64 > bound);
    cluster.start_member_with(&small).await;
    let addr = cluster.member().advertised_addr().unwrap().to_string();
    let rejected = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        cluster.leader().add_learner(raft_kit::MEMBER_ID, &addr),
    )
    .await
    .expect("the leader answers inside 10 s")
    .expect_err("the learner was seeded past its bound");
    assert_eq!(rejected.code(), Code::ResourceExhausted, "{rejected}");
    assert_eq!(cluster.member().metrics().borrow().last_log_index, None);
    assert!(cluster.member().awaiting_snapshot().unwrap());

    // The bound is raised the one way there is: the member is started
    // again on its directory under the larger bound.
    cluster.stop_member().await;
    cluster.start_member_with(&raft_kit::host_config()).await;
    let addr = cluster.member().advertised_addr().unwrap().to_string();
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        cluster.leader().add_learner(raft_kit::MEMBER_ID, &addr),
    )
    .await
    .expect("the join answers inside 30 s")
    .expect("the learner is seeded under the larger bound");
    assert!(!cluster.member().awaiting_snapshot().unwrap());
    assert!(raft_kit::published_generation(cluster.member_dir.path()).is_some());
    assert!(raft_kit::core_running(cluster.member()));
    assert!(raft_kit::core_running(cluster.leader()));
    propose_ok(cluster.leader(), &grant_command(&group, 2, 1, "bob")).await;
    let after = raft_kit::durable_position(cluster.leader());
    raft_kit::wait_applied(cluster.member(), after).await;
    assert_eq!(raft_kit::durable_position(cluster.member()), after);
    assert!(raft_kit::core_running(cluster.member()));
    cluster.shutdown().await;
}

/// The other non-storage condition on the build path: a snapshot trigger
/// at a prepared member that has applied nothing yet. The library sends
/// the build regardless, the builder has no position to snapshot, and its
/// refusal would stop the core the way a storage failure does. The host
/// refuses the trigger by name before the library sees it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_snapshot_trigger_with_nothing_applied_is_refused_before_the_library() {
    let _serial = serial();
    let group = kit::identity(kit::SEED);
    let mut cluster =
        Cluster::bootstrap("trigger-unseeded", &group, &kit::policy(), &kit::limits()).await;
    propose_ok(cluster.leader(), &prepare_command(&group, 1)).await;
    cluster.start_member().await;
    assert!(cluster.member().awaiting_snapshot().unwrap());

    let refused = cluster.member().trigger_snapshot().await.err();
    // A build the library was sent would fail on its own task shortly
    // after; the core must still be running once it had that time.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(
        raft_kit::core_running(cluster.member()),
        "the member core stopped on a snapshot trigger with nothing applied (trigger outcome: \
         {refused:?})"
    );
    let refused = refused.expect("the trigger is refused by name");
    assert_eq!(refused.code(), Code::FailedPrecondition, "{refused}");
    assert!(refused.message().contains("applied position"), "{refused}");
    assert!(raft_kit::published_generation(cluster.member_dir.path()).is_none());

    // The member is seeded afterwards as usual, and can snapshot then.
    cluster.add_member().await;
    assert!(!cluster.member().awaiting_snapshot().unwrap());
    raft_kit::wait_applied(
        cluster.member(),
        raft_kit::durable_position(cluster.leader()),
    )
    .await;
    let (_, meta) = raft_kit::snapshot_image(&cluster.member_dir, cluster.member()).await;
    assert_eq!(
        meta.last_log_id.unwrap().index,
        raft_kit::durable_position(cluster.member())
    );
    assert!(raft_kit::core_running(cluster.member()));
    cluster.shutdown().await;
}

/// `max_snapshot_bytes` is a receiver's bound. Both nodes run under a
/// bound below the store image: the leader builds and serves its image
/// as usual (its own bound does not apply to its build), and the peer
/// rejects each install by name before any byte is received, with its
/// core and the leader's running, and the leader keeps committing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_store_image_over_a_peers_bound_is_rejected_at_that_peer_with_both_cores_running() {
    let _serial = serial();
    let group = kit::identity(kit::SEED);
    let bound = raft_kit::CHUNK_BYTES as u64;
    let mut small = raft_kit::host_config();
    small.max_snapshot_bytes = bound;
    let mut cluster = Cluster::bootstrap_with_config(
        "peer-bound",
        &group,
        &kit::policy(),
        &kit::limits(),
        &small,
    )
    .await;
    propose_ok(cluster.leader(), &prepare_command(&group, 1)).await;
    // The leader's build is not bounded by its own max_snapshot_bytes.
    let outcome = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        raft_kit::snapshot_image(&cluster.leader_dir, cluster.leader()),
    )
    .await;
    assert!(
        raft_kit::core_running(cluster.leader()),
        "the leader core stopped on a build over its own bound (build outcome: {})",
        outcome.as_ref().map(|(image, _)| image.len()).unwrap_or(0)
    );
    let (image, meta) = outcome.expect("the leader builds its image under its own bound");
    assert!(
        image.len() as u64 > bound,
        "the store image ({} bytes) is not over the bound ({bound})",
        image.len()
    );
    cluster.start_member_with(&small).await;
    assert!(cluster.member().awaiting_snapshot().unwrap());

    // The raw push the leader's transfer reduces to: rejected by name,
    // before any byte is received.
    let rejected = cluster
        .push(&meta, &image)
        .await
        .expect_err("the peer rejects an image over its bound");
    assert_eq!(rejected.code(), Code::ResourceExhausted, "{rejected}");
    assert!(
        rejected
            .message()
            .contains(&format!("beyond the {bound} byte image bound")),
        "{rejected}"
    );
    assert!(raft_kit::incoming_dirs(cluster.member_dir.path()).is_empty());
    assert!(raft_kit::core_running(cluster.member()));

    // Through the leader: the seed transfer is rejected the same way, the
    // leader names the peer's rejection at once with the peer's own code
    // instead of waiting out its timeout, both nodes record it, and both
    // cores keep running.
    let addr = cluster.member().advertised_addr().unwrap().to_string();
    let before = raft_kit::durable_position(cluster.leader());
    let outcome = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        cluster.leader().add_learner(raft_kit::MEMBER_ID, &addr),
    )
    .await;
    let rejected = outcome
        .expect("the leader answers inside 10 s")
        .expect_err("the learner was seeded past its bound");
    assert_eq!(rejected.code(), Code::ResourceExhausted, "{rejected}");
    assert!(
        rejected
            .message()
            .contains("learner 2 rejected the snapshot"),
        "{rejected}"
    );
    assert!(
        rejected
            .message()
            .contains(&format!("beyond the {bound} byte image bound of node 2")),
        "{rejected}"
    );
    let recorded = cluster.leader().peer_rejections();
    let recorded = recorded
        .get(&raft_kit::MEMBER_ID)
        .expect("the leader recorded the peer's rejection");
    assert_eq!(recorded.action, "snapshot");
    assert_eq!(recorded.code, Code::ResourceExhausted);
    assert!(recorded.count >= 1);
    let rejections = cluster.member().snapshot_rejections();
    assert!(
        rejections.count() >= 2,
        "the peer counted {} rejections; the raw push and the leader's seed are two",
        rejections.count()
    );
    let last: SnapshotRejection = rejections
        .last()
        .expect("the peer recorded the last rejection");
    assert_eq!(last.from, raft_kit::NODE_ID);
    assert_eq!(last.bound, bound);
    // The seed may have built afresh since the raw push, so the announced
    // length is the leader's current image, over the bound either way.
    assert!(last.announced > bound, "{last:?}");
    let build = cluster
        .leader()
        .last_snapshot_build()
        .expect("the leader reports its build");
    assert_eq!(build.bytes, image.len() as u64);
    assert_eq!(build.receiver_bound, bound);
    assert!(build.over_receiver_bound());
    assert!(
        raft_kit::core_running(cluster.leader()),
        "the leader core stopped on a peer's rejection"
    );
    assert!(raft_kit::core_running(cluster.member()));
    assert!(cluster.member().awaiting_snapshot().unwrap());
    assert!(raft_kit::published_generation(cluster.member_dir.path()).is_none());
    assert!(raft_kit::incoming_dirs(cluster.member_dir.path()).is_empty());
    propose_ok(cluster.leader(), &grant_command(&group, 2, 1, "bob")).await;
    // Two entries since the join was asked: the membership entry the
    // rejected join wrote (the learner stays in the membership) and the
    // grant.
    assert_eq!(raft_kit::durable_position(cluster.leader()), before + 2);
    assert!(raft_kit::core_running(cluster.leader()));
    cluster.shutdown().await;
}

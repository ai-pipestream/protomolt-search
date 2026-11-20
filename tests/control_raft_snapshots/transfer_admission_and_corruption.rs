//! Snapshot admission refusals, overrun chunk drops, and corrupted byte detection.

use super::*;

/// Snapshot admission : every
/// invalid transfer is refused by the transport's staging, by name, with
/// the core running and its state intact throughout — oversized, foreign,
/// truncated, wrong membership, out of order, interrupted, superseded,
/// bound to another peer, meta changed within a transfer — and a valid
/// snapshot then installs without a restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_admission_refuses_invalid_transfers_without_stopping_the_core() {
    let _serial = serial();
    let (cluster, image, newer) = detached("admission").await;
    let (before, phase_before) = member_state(&cluster);
    let position_before = raft_kit::durable_position(cluster.member());
    let (_dir_b, host_b, image_b, meta_b) = foreign_image("admission-b").await;
    let index = newer.last_log_id.unwrap().index;
    let group = cluster.group.clone();
    let intact = |what: &str| {
        assert!(
            raft_kit::core_running(cluster.member()),
            "{what}: the member core stopped"
        );
        let (now, phase) = member_state(&cluster);
        assert_eq!(
            now.1.snapshot_id, before.1.snapshot_id,
            "{what}: generation replaced"
        );
        assert_eq!(now.0, before.0, "{what}: image bytes changed");
        assert_eq!(phase, phase_before, "{what}: owner row changed");
        assert_eq!(
            raft_kit::durable_position(cluster.member()),
            position_before,
            "{what}: applied position moved"
        );
        assert!(
            raft_kit::incoming_dirs(cluster.member_dir.path()).is_empty(),
            "{what}: a refused transfer left its bytes behind"
        );
    };
    let mut peer =
        raft_kit::peer_client(cluster.member().listen_addr().unwrap(), raft_kit::PEER_ID).await;
    let vote = raft_kit::current_vote(cluster.leader());
    let chunk = |meta: &RaftSnapshotMeta, offset: usize, data: &[u8], done: bool| {
        raft_kit::timed(raft_kit::chunk_request(
            &group,
            raft_kit::PEER_ID,
            raft_kit::MEMBER_ID,
            &vote,
            meta,
            offset,
            data,
            done,
        ))
    };

    // Oversized: an announcement past the image bound is refused before
    // any byte lands.
    let mut huge = raft_kit::renamed(&newer, index, &image);
    huge.length = (4u64 << 30) + 1;
    huge.snapshot_id = format!(
        "{}-{}-{}",
        index,
        (4u64 << 30) + 1,
        pipestream_search::sha256::to_hex(&pipestream_search::sha256::digest(&image))
    );
    let error = peer
        .install_snapshot(chunk(&huge, 0, &image[..1024], false))
        .await
        .expect_err("an oversized announcement must refuse");
    assert_eq!(error.code(), Code::ResourceExhausted, "{error}");
    assert!(error.message().contains("image bound"), "{error}");
    intact("oversized");

    // Foreign bytes under a forged meta of this group: refused at the
    // probe, named.
    let forged = raft_kit::renamed(&raft_kit::with_group(&meta_b, &group), index, &image_b);
    let error = cluster
        .push(&forged, &image_b)
        .await
        .expect_err("foreign image refused");
    assert!(
        error.message().contains("group") || error.message().contains("incarnation"),
        "{error}"
    );
    intact("foreign");

    // Truncated: the final chunk arrives short of the announcement.
    let error = cluster
        .push(&newer, &image[..image.len() / 2])
        .await
        .expect_err("a truncated transfer must refuse");
    assert!(error.message().contains("announced"), "{error}");
    intact("truncated");

    // Wrong membership over the genuine bytes.
    let wrong = raft_kit::meta_pb_for(&group, index, &image, &[9]);
    let error = cluster
        .push(&wrong, &image)
        .await
        .expect_err("wrong membership refused");
    assert!(error.message().contains("membership"), "{error}");
    intact("membership");

    // Out of order: a first chunk at a nonzero offset continues no
    // transfer (the sender begins every transfer at offset zero), so it is
    // rejected by name and nothing is staged. A mismatch answer in its
    // place made the sender start the same bytes over on the same opened
    // image without end after a rejection on the final chunk.
    let error = peer
        .install_snapshot(chunk(&newer, 4096, &image[4096..8192], false))
        .await
        .expect_err("a first chunk at a nonzero offset refused");
    assert_eq!(error.code(), Code::FailedPrecondition, "{error}");
    assert!(
        error.message().contains("continues no transfer at node"),
        "{error}"
    );
    intact("out of order");

    // Interrupted, then superseded by the same peer: the half receive is
    // dropped with its bytes when the next id begins.
    let half = &image[..image.len() / 2];
    let mut offset = 0;
    for piece in half.chunks(raft_kit::CHUNK_BYTES) {
        peer.install_snapshot(chunk(&newer, offset, piece, false))
            .await
            .unwrap();
        offset += piece.len();
    }
    assert_eq!(raft_kit::incoming_dirs(cluster.member_dir.path()).len(), 1);
    let other_id = raft_kit::renamed(&newer, index + 1, &image);
    let error = cluster
        .push(&other_id, &image)
        .await
        .expect_err("a renamed image fails its position check");
    assert!(error.message().contains("applied position"), "{error}");
    intact("superseded");

    // Meta changed within one transfer: dropped, named.
    peer.install_snapshot(chunk(&newer, 0, &image[..4096], false))
        .await
        .unwrap();
    let error = peer
        .install_snapshot(chunk(&wrong, 4096, &image[4096..8192], false))
        .await
        .expect_err("a changed meta must refuse");
    assert_eq!(error.code(), Code::InvalidArgument, "{error}");
    assert!(error.message().contains("changed"), "{error}");
    intact("meta changed");

    // Bound to its peer: while node 3's transfer is open, node 1's own
    // certificate cannot continue it, and cannot start another under the
    // same vote; node 3 superseding its own transfer is allowed.
    peer.install_snapshot(chunk(&newer, 0, &image[..4096], false))
        .await
        .unwrap();
    let mut other =
        raft_kit::peer_client(cluster.member().listen_addr().unwrap(), raft_kit::NODE_ID).await;
    let error = other
        .install_snapshot(raft_kit::timed(raft_kit::chunk_request(
            &group,
            raft_kit::NODE_ID,
            raft_kit::MEMBER_ID,
            &vote,
            &newer,
            4096,
            &image[4096..8192],
            false,
        )))
        .await
        .expect_err("another peer cannot continue a bound transfer");
    assert_eq!(error.code(), Code::FailedPrecondition, "{error}");
    assert!(error.message().contains("bound to node 3"), "{error}");
    let error = other
        .install_snapshot(raft_kit::timed(raft_kit::chunk_request(
            &group,
            raft_kit::NODE_ID,
            raft_kit::MEMBER_ID,
            &vote,
            &other_id,
            0,
            &image[..4096],
            false,
        )))
        .await
        .expect_err("another peer cannot start a transfer under the same vote");
    assert_eq!(error.code(), Code::Unavailable, "{error}");
    assert!(error.message().contains("in progress"), "{error}");
    assert!(raft_kit::core_running(cluster.member()));

    // The genuine newer image installs on the same running core: the
    // interrupted transfer of node 3 is superseded by its own new one.
    cluster.push(&newer, &image).await.unwrap();
    assert!(raft_kit::core_running(cluster.member()));
    assert_eq!(raft_kit::durable_position(cluster.member()), index);
    assert!(raft_kit::incoming_dirs(cluster.member_dir.path()).is_empty());
    let (after, _) = member_state(&cluster);
    assert_eq!(after.1.snapshot_id, newer.snapshot_id);
    assert_eq!(
        cluster
            .member()
            .store()
            .unwrap()
            .policy("alice", kit::WORKSPACE, kit::COLLECTION)
            .unwrap()
            .revision,
        2
    );
    host_b.shutdown().await.unwrap();
    cluster.shutdown().await;
}

/// A chunk that ends past the announced length drops the transfer with
/// its bytes: no receive directory is left and the transfer slot is free,
/// so the next transfer from the peer begins at once and installs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_chunk_past_the_announced_length_drops_the_transfer_and_the_next_one_installs() {
    let _serial = serial();
    let group = kit::identity(kit::SEED);
    let mut cluster =
        Cluster::bootstrap("chunk-past", &group, &kit::policy(), &kit::limits()).await;
    propose_ok(cluster.leader(), &prepare_command(&group, 1)).await;
    let (image, meta) = raft_kit::snapshot_image(&cluster.leader_dir, cluster.leader()).await;
    assert!(image.len() > raft_kit::CHUNK_BYTES);
    cluster.start_member().await;
    let listen = cluster.member().listen_addr().unwrap();
    let vote = raft_kit::current_vote(cluster.leader());
    let mut peer = raft_kit::peer_client(listen, raft_kit::PEER_ID).await;

    // The first chunk is honest and partial: a transfer is in progress.
    peer.install_snapshot(raft_kit::timed(raft_kit::chunk_request(
        &group,
        raft_kit::PEER_ID,
        raft_kit::MEMBER_ID,
        &vote,
        &meta,
        0,
        &image[..raft_kit::CHUNK_BYTES],
        false,
    )))
    .await
    .expect("the first chunk is taken");
    assert_eq!(raft_kit::incoming_dirs(cluster.member_dir.path()).len(), 1);

    // The second ends one byte past the announced length.
    let mut tail = image[raft_kit::CHUNK_BYTES..].to_vec();
    tail.push(0);
    let rejected = peer
        .install_snapshot(raft_kit::timed(raft_kit::chunk_request(
            &group,
            raft_kit::PEER_ID,
            raft_kit::MEMBER_ID,
            &vote,
            &meta,
            raft_kit::CHUNK_BYTES,
            &tail,
            true,
        )))
        .await
        .expect_err("a chunk past the announced length is rejected");
    assert_eq!(rejected.code(), Code::InvalidArgument, "{rejected}");
    assert!(
        rejected.message().contains(&format!(
            "ends at byte {}, beyond the announced length {}; the transfer is dropped",
            image.len() + 1,
            image.len()
        )),
        "{rejected}"
    );
    assert!(
        raft_kit::incoming_dirs(cluster.member_dir.path()).is_empty(),
        "the dropped transfer left its receive directory"
    );
    assert!(raft_kit::core_running(cluster.member()));

    // The slot is free: the honest transfer begins at once and installs.
    cluster
        .push(&meta, &image)
        .await
        .expect("the next transfer from the peer installs");
    assert!(raft_kit::incoming_dirs(cluster.member_dir.path()).is_empty());
    assert!(raft_kit::published_generation(cluster.member_dir.path()).is_some());
    assert!(!cluster.member().awaiting_snapshot().unwrap());
    assert!(raft_kit::core_running(cluster.member()));
    cluster.shutdown().await;
}

/// A serve reads the published image by length and does not read its
/// digest (the serve is repeated for as long as a peer rejects). An image
/// whose bytes changed on the leader's disk is therefore served, rejected
/// at the receiver by its digest, and named at the leader with the
/// receiver's code; the leader's core keeps running, and the next build
/// publishes a sound image the peer is seeded from.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_served_image_with_changed_bytes_is_rejected_at_the_receiver_and_named_at_the_leader() {
    let _serial = serial();
    let group = kit::identity(kit::SEED);
    let mut cluster =
        Cluster::bootstrap("served-changed", &group, &kit::policy(), &kit::limits()).await;
    propose_ok(cluster.leader(), &prepare_command(&group, 1)).await;
    let (image, _) = raft_kit::snapshot_image(&cluster.leader_dir, cluster.leader()).await;
    let generation = raft_kit::published_generation(cluster.leader_dir.path()).unwrap();
    let path = raft_kit::generation_dir(cluster.leader_dir.path(), generation).join("image.redb");
    let mut bytes = std::fs::read(&path).unwrap();
    assert_eq!(bytes, image);
    let middle = bytes.len() / 2;
    bytes[middle] ^= 0xff;
    std::fs::write(&path, &bytes).unwrap();
    cluster.start_member().await;
    let addr = cluster.member().advertised_addr().unwrap().to_string();

    let rejected = tokio::time::timeout(
        std::time::Duration::from_secs(20),
        cluster.leader().add_learner(raft_kit::MEMBER_ID, &addr),
    )
    .await
    .expect("the leader answers inside 20 s")
    .expect_err("an image whose bytes changed was installed");
    assert_eq!(rejected.code(), Code::DataLoss, "{rejected}");
    assert!(rejected.message().contains("digest"), "{rejected}");
    assert!(
        raft_kit::core_running(cluster.leader()),
        "the leader core stopped serving an image whose bytes changed"
    );
    assert!(raft_kit::core_running(cluster.member()));
    assert!(cluster.member().awaiting_snapshot().unwrap());
    assert!(raft_kit::published_generation(cluster.member_dir.path()).is_none());
    assert!(raft_kit::incoming_dirs(cluster.member_dir.path()).is_empty());
    let recorded = cluster.leader().peer_rejections();
    let recorded = recorded
        .get(&raft_kit::MEMBER_ID)
        .expect("the leader recorded the rejection");
    assert_eq!(recorded.action, "snapshot");
    assert_eq!(recorded.code, Code::DataLoss);

    // The rejection names bytes that differ from the meta; the leader reads
    // its published image by digest, finds the difference on its own disk
    // and builds afresh: a new generation is published with no proposal in
    // between, and the leader's own check passes again.
    let rebuilt = tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            if cluster.leader().last_snapshot_build().map(|b| b.generation) > Some(generation)
                && cluster.leader().verify_published_image().is_ok()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    })
    .await;
    assert!(
        rebuilt.is_ok(),
        "the leader did not replace the generation it served with changed bytes: {:?}",
        cluster.leader().last_snapshot_build()
    );
    assert!(raft_kit::core_running(cluster.leader()));
    // The second join is seeded from the fresh image. Its window is the
    // pin for the sender's return to the library's core: a continuation
    // of a dropped transfer is refused, so the sender spends its retry
    // budget (five chunks, about two seconds), returns, backs off and
    // serves the fresh generation, well inside 30 s. A mismatch answer in
    // its place made the sender start the same bytes over from offset
    // zero on the same opened image, without end, and the fresh generation
    // was never served.
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        cluster.leader().add_learner(raft_kit::MEMBER_ID, &addr),
    )
    .await
    .expect("the join answers inside 30 s")
    .expect("the learner is seeded from the fresh image");
    assert!(!cluster.member().awaiting_snapshot().unwrap());
    assert!(raft_kit::published_generation(cluster.member_dir.path()).is_some());
    let after = raft_kit::durable_position(cluster.leader());
    raft_kit::wait_applied(cluster.member(), after).await;
    assert!(raft_kit::core_running(cluster.leader()));
    assert!(raft_kit::core_running(cluster.member()));
    cluster.shutdown().await;
}

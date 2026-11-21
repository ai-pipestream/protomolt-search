//! Selected receive identity, interrupted transfers, size bounds, and bounded hashing.

use super::*;

/// R5: the install must consume the actual receive, not the newest
/// incoming-* name. An abandoned directory with a deliberately later name
/// must not be selected; it is left for the startup sweep.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r5_install_uses_the_actual_receive_not_the_newest_filename() {
    let _serial = serial();
    let group = kit::identity(kit::SEED);
    let mut cluster = Cluster::bootstrap("r5-newest", &group, &kit::policy(), &kit::limits()).await;
    propose_ok(cluster.leader(), &prepare_command(&group, 1)).await;
    let (image, meta) = raft_kit::snapshot_image(&cluster.leader_dir, cluster.leader()).await;
    cluster.start_member().await;
    // Plant an abandoned receive whose name sorts AFTER any real one.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    let planted = raft_kit::snapshots_dir(cluster.member_dir.path())
        .join(format!("incoming-{}-99", now + 1_000_000_000_000_000));
    std::fs::create_dir(&planted).unwrap();
    std::fs::write(planted.join("image.redb"), vec![0xABu8; 4096]).unwrap();

    cluster.push(&meta, &image).await.unwrap_or_else(|error| {
        panic!(
            "R5 REQUIRED the install to consume the actual receive; the newest-name \
             heuristic selected the abandoned file instead: {error}"
        )
    });
    assert!(!cluster.member().awaiting_snapshot().unwrap());
    assert_eq!(
        raft_kit::durable_position(cluster.member()),
        meta.last_log_id.unwrap().index,
        "the installed applied position is not the received image's"
    );
    assert!(
        planted.exists(),
        "the abandoned receive disappeared without its own receive or a restart"
    );
    assert_eq!(
        raft_kit::incoming_dirs(cluster.member_dir.path()),
        vec![planted.clone()]
    );
    // The startup sweep removes it.
    cluster.restart_member().await;
    assert!(raft_kit::incoming_dirs(cluster.member_dir.path()).is_empty());
    assert_eq!(
        cluster
            .member()
            .store()
            .unwrap()
            .owner("alice", &kit::owner_key())
            .unwrap()
            .phase,
        PreparedSourceOwnerPhase::Prepared as i32
    );
    cluster.shutdown().await;
}

/// R5: interrupted receives never participate in a later install. (i) a
/// receive abandoned half way is dropped with its bytes when a receive of
/// another id begins; (ii) a leftover directory with a deliberately LATER
/// name is not selected by a complete receive.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r5_interrupted_receive_leaves_no_selected_artifact() {
    let _serial = serial();
    let group = kit::identity(kit::SEED);
    let mut cluster =
        Cluster::bootstrap("r5-partial", &group, &kit::policy(), &kit::limits()).await;
    propose_ok(cluster.leader(), &prepare_command(&group, 1)).await;
    let (image_1, meta_1) = raft_kit::snapshot_image(&cluster.leader_dir, cluster.leader()).await;
    propose_ok(cluster.leader(), &grant_command(&group, 2, 1, "bob")).await;
    let (image_2, meta_2) = raft_kit::snapshot_image(&cluster.leader_dir, cluster.leader()).await;
    assert_ne!(meta_1.snapshot_id, meta_2.snapshot_id);
    cluster.start_member().await;

    // (i) half of image 1, abandoned; then image 2 in full.
    let mut peer =
        raft_kit::peer_client(cluster.member().listen_addr().unwrap(), raft_kit::PEER_ID).await;
    let vote = raft_kit::current_vote(cluster.leader());
    let half = &image_1[..image_1.len() / 2];
    for (offset, chunk) in half.chunks(raft_kit::CHUNK_BYTES).enumerate() {
        peer.install_snapshot(raft_kit::timed(raft_kit::chunk_request(
            &group,
            raft_kit::PEER_ID,
            raft_kit::MEMBER_ID,
            &vote,
            &meta_1,
            offset * raft_kit::CHUNK_BYTES,
            chunk,
            false,
        )))
        .await
        .unwrap();
    }
    assert_eq!(
        raft_kit::incoming_dirs(cluster.member_dir.path()).len(),
        1,
        "the abandoned receive is on disk until superseded"
    );
    raft_kit::install_chunks(
        &mut peer,
        &group,
        raft_kit::PEER_ID,
        raft_kit::MEMBER_ID,
        &vote,
        &meta_2,
        &image_2,
    )
    .await
    .expect("(i) an abandoned partial broke a later install");
    assert!(
        raft_kit::incoming_dirs(cluster.member_dir.path()).is_empty(),
        "(i) the abandoned receive outlived the install that superseded it"
    );
    assert_eq!(
        raft_kit::durable_position(cluster.member()),
        meta_2.last_log_id.unwrap().index
    );

    // (ii) a leftover directory with a LATER name, then a complete receive.
    cluster.restart_member().await;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    let planted = raft_kit::snapshots_dir(cluster.member_dir.path())
        .join(format!("incoming-{}-7", now + 1_000_000_000_000_000));
    std::fs::create_dir(&planted).unwrap();
    std::fs::write(planted.join("image.redb"), vec![0xCDu8; 8192]).unwrap();
    // A third generation so the id is new to this member.
    propose_ok(cluster.leader(), &grant_command(&group, 3, 2, "carol")).await;
    let (image_3, meta_3) = raft_kit::snapshot_image(&cluster.leader_dir, cluster.leader()).await;
    cluster
        .push(&meta_3, &image_3)
        .await
        .unwrap_or_else(|error| {
            panic!(
                "R5 REQUIRED a later valid receive to win over an abandoned artifact; \
             the newest-name heuristic selected the planted directory instead: {error}"
            )
        });
    assert!(
        planted.exists(),
        "(ii) the planted directory is not this receive's"
    );
    assert_eq!(
        raft_kit::durable_position(cluster.member()),
        meta_3.last_log_id.unwrap().index
    );
    cluster.shutdown().await;
}

/// R5: announced lengths and digests that do not match the received bytes
/// refuse before anything is replaced, leaving the previous generation
/// intact. (The bounded-allocation half of R5 is a separate test.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r5_oversize_and_truncated_images_refuse_before_replacement() {
    let _serial = serial();
    let (mut cluster, image, newer) = detached("r5-lengths").await;
    let (before, _) = member_state(&cluster);
    let index = newer.last_log_id.unwrap().index;

    // (i) an announced length larger than the received image.
    let mut announced = raft_kit::meta_pb_for(&cluster.group, index, &image, &[1]);
    announced.length = image.len() as u64 + 100;
    announced.snapshot_id = format!(
        "{}-{}-{}",
        index,
        image.len() + 100,
        turbo_sha256::to_hex(turbo_sha256::sha256(&image).0)
    );
    let error = match cluster.push(&announced, &image).await {
        Ok(()) => panic!("an oversize announcement must refuse, installed instead"),
        Err(error) => error,
    };
    assert!(
        error.message().contains("length"),
        "the refusal must name the length: {error}"
    );
    recover_if_stopped(&mut cluster, "r5 oversize").await;
    assert_eq!(
        member_state(&cluster).0 .1.snapshot_id,
        before.1.snapshot_id
    );

    // (ii) a truncated receive against a full-image announcement.
    let error = match cluster.push(&newer, &image[..image.len() / 2]).await {
        Ok(()) => panic!("a truncated image must refuse, installed instead"),
        Err(error) => error,
    };
    assert!(
        error.message().contains("length"),
        "the refusal must name the length: {error}"
    );
    recover_if_stopped(&mut cluster, "r5 truncated").await;
    let (after, phase) = member_state(&cluster);
    assert_eq!(after.1.snapshot_id, before.1.snapshot_id);
    assert_eq!(after.0, before.0);
    assert_eq!(phase, PreparedSourceOwnerPhase::Prepared as i32);
    // The genuine newer generation installs afterwards.
    cluster.push(&newer, &image).await.unwrap();
    assert_eq!(raft_kit::durable_position(cluster.member()), index);
    cluster.shutdown().await;
}

/// R5: the announced length is enforced WHILE receiving, before the library
/// sees the chunk (the gap the original reproduction documented). A chunk
/// past the announced length is refused by name and drops the transfer
/// with its bytes (Snapshot admission bounds);
/// the next transfer from the peer begins at once and installs. Until the
/// review of d3b90b3 the check ran before `stage` and the transfer stayed
/// open with its slot and its directory; the assertion here followed the
/// implementation of the time, not the document.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r5_announced_length_is_enforced_while_receiving() {
    let _serial = serial();
    let group = kit::identity(kit::SEED);
    let mut cluster = Cluster::bootstrap("r5-bound", &group, &kit::policy(), &kit::limits()).await;
    propose_ok(cluster.leader(), &prepare_command(&group, 1)).await;
    let (image, meta) = raft_kit::snapshot_image(&cluster.leader_dir, cluster.leader()).await;
    cluster.start_member().await;
    let mut peer =
        raft_kit::peer_client(cluster.member().listen_addr().unwrap(), raft_kit::PEER_ID).await;
    let vote = raft_kit::current_vote(cluster.leader());
    let mut offset = 0;
    for chunk in image.chunks(raft_kit::CHUNK_BYTES) {
        peer.install_snapshot(raft_kit::timed(raft_kit::chunk_request(
            &group,
            raft_kit::PEER_ID,
            raft_kit::MEMBER_ID,
            &vote,
            &meta,
            offset,
            chunk,
            false,
        )))
        .await
        .unwrap();
        offset += chunk.len();
    }
    // Excess bytes past the announcement: refused before the library.
    let error = peer
        .install_snapshot(raft_kit::timed(raft_kit::chunk_request(
            &group,
            raft_kit::PEER_ID,
            raft_kit::MEMBER_ID,
            &vote,
            &meta,
            offset,
            &vec![0u8; 1 << 20],
            true,
        )))
        .await
        .expect_err("R5 REQUIRED the announced length to be enforced while receiving");
    assert_eq!(error.code(), Code::InvalidArgument, "{error}");
    assert!(error.message().contains("announced length"), "{error}");
    assert!(
        error.message().contains("the transfer is dropped"),
        "{error}"
    );
    assert!(raft_kit::core_running(cluster.member()));
    // The transfer is dropped with its bytes; the next one installs.
    assert!(raft_kit::incoming_dirs(cluster.member_dir.path()).is_empty());
    raft_kit::install_chunks(
        &mut peer,
        &group,
        raft_kit::PEER_ID,
        raft_kit::MEMBER_ID,
        &vote,
        &meta,
        &image,
    )
    .await
    .unwrap();
    assert!(!cluster.member().awaiting_snapshot().unwrap());
    assert_eq!(
        raft_kit::durable_position(cluster.member()),
        meta.last_log_id.unwrap().index
    );
    cluster.shutdown().await;
}

/// R5: hashing must use bounded buffers, not a whole-file Vec. Seeds a
/// member from a multi-MiB image through the supported path (`add_learner`)
/// and measures the peak live allocation attributable to the install; at
/// 331c18f `image_signature` read the entire image into one Vec, so the
/// peak reached the image size.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r5_install_hashing_is_bounded() {
    let _serial = serial();
    // A leader whose image is several MiB: each install keeps its command
    // and its topology rows, near 1 MiB apiece, on its own collection.
    // Custom limits admit 1 MiB commands.
    let group = kit::identity(kit::SEED);
    let limits = pipestream_search::pb::storage::SourceAuthorityLimits {
        max_owners: 16,
        max_decisions: 1_000,
        max_payload_bytes: 256 << 20,
        max_command_bytes: 1 << 20,
    };
    const COLLECTIONS: usize = 10;
    let collection = |index: usize| format!("{}-{index}", kit::COLLECTION);
    let mut policy = kit::policy();
    for index in 0..COLLECTIONS {
        policy
            .resources
            .push(pipestream_search::pb::CollectionResource {
                workspace: kit::WORKSPACE.into(),
                collection: collection(index),
            });
        policy.grants.push(kit::grant("alice", &collection(index)));
    }
    let mut cluster = Cluster::bootstrap("r5-bounded", &group, &policy, &limits).await;
    for index in 0..COLLECTIONS {
        let mut install = kit::install_command(
            &group,
            "install",
            1 + index as u64,
            kit::install_state(18_000),
        );
        install.key.as_mut().unwrap().collection = collection(index);
        assert!(install.encoded_len() < 1 << 20, "the install exceeds 1 MiB");
        let decision = cluster
            .leader()
            .propose_install("alice", &install)
            .await
            .unwrap();
        assert_eq!(decision.code, 0, "{}", decision.message);
    }
    let (image, _meta) = raft_kit::snapshot_image(&cluster.leader_dir, cluster.leader()).await;
    assert!(
        image.len() >= 12 << 20,
        "fixture image is {} bytes; raise the installed payload",
        image.len()
    );

    // Seed the member through the supported path, measuring the peak.
    // Baseline re-recorded inside the target-wide SERIAL section: no other
    // test can allocate between this reset and the peak read below.
    cluster.start_member().await;
    let watermark = live_bytes();
    PEAK.store(watermark, Ordering::Relaxed);
    cluster.add_member().await;
    let growth = peak_bytes().saturating_sub(watermark);
    assert!(!cluster.member().awaiting_snapshot().unwrap());
    eprintln!(
        "r5 bounded: image {} bytes, peak live growth across the seeding install {} bytes",
        image.len(),
        growth
    );
    assert!(
        growth < image.len(),
        "R5 REQUIRED bounded-buffer hashing; the install path allocated a \
         peak of {growth} bytes live for a {}-byte image (peak >= image size \
         demonstrates the whole-file Vec read)",
        image.len()
    );
    cluster.shutdown().await;
}

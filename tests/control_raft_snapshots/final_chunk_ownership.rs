//! Authenticated final-chunk replay and staging ownership across cancelled retries.

use super::*;

/// A final RPC can lose its response after the receiver installed its bytes.
/// Retrying that exact RPC must acknowledge the completed operation, while
/// altered bytes, identity, offset, vote or metadata must never use its reply.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r153_a_completed_final_chunk_replays_only_its_exact_authenticated_content() {
    let _serial = serial();
    let group = kit::identity(kit::SEED);
    let mut cluster =
        Cluster::bootstrap("r153-final-replay", &group, &kit::policy(), &kit::limits()).await;
    propose_ok(cluster.leader(), &prepare_command(&group, 1)).await;
    let (image, meta) = raft_kit::snapshot_image(&cluster.leader_dir, cluster.leader()).await;
    cluster.start_member().await;
    let mut peer =
        raft_kit::peer_client(cluster.member().listen_addr().unwrap(), raft_kit::PEER_ID).await;
    let vote = raft_kit::current_vote(cluster.leader());
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
    let offset = (image.len() - 1) / raft_kit::CHUNK_BYTES * raft_kit::CHUNK_BYTES;
    assert!(offset > 0, "the replay must be a nonzero final chunk");
    let final_chunk = raft_kit::chunk_request(
        &group,
        raft_kit::PEER_ID,
        raft_kit::MEMBER_ID,
        &vote,
        &meta,
        offset,
        &image[offset..],
        true,
    );
    let before = raft_kit::published_pair(cluster.member_dir.path()).unwrap();
    let replay = peer
        .install_snapshot(raft_kit::timed(final_chunk.clone()))
        .await
        .expect("the exact completed final RPC is acknowledged")
        .into_inner();
    assert!(matches!(
        replay.outcome,
        Some(pipestream_search::pb::storage::raft_install_snapshot_response::Outcome::Vote(_))
    ));
    let mut conflicts = Vec::new();
    let mut changed = final_chunk.clone();
    changed.data[0] ^= 1;
    conflicts.push(changed);
    let mut changed = final_chunk.clone();
    changed.data.pop();
    conflicts.push(changed);
    let mut changed = final_chunk.clone();
    changed.offset -= 1;
    conflicts.push(changed);
    let mut changed = final_chunk.clone();
    changed.vote.as_mut().unwrap().term += 1;
    conflicts.push(changed);
    let mut changed = final_chunk.clone();
    changed
        .meta
        .as_mut()
        .unwrap()
        .membership
        .as_mut()
        .unwrap()
        .log_id = None;
    conflicts.push(changed);
    let mut changed = final_chunk.clone();
    changed.done = false;
    conflicts.push(changed);
    for changed in conflicts {
        peer.install_snapshot(raft_kit::timed(changed))
            .await
            .expect_err("a conflicting final RPC must not replay a successful reply");
    }
    // Starting again at zero creates a new receive. Even an unchanged final
    // chunk must validate all its earlier bytes instead of using the receipt.
    for (index, bytes) in image[..offset].chunks(raft_kit::CHUNK_BYTES).enumerate() {
        let mut bytes = bytes.to_vec();
        if index == 0 {
            bytes[0] ^= 1;
        }
        peer.install_snapshot(raft_kit::timed(raft_kit::chunk_request(
            &group,
            raft_kit::PEER_ID,
            raft_kit::MEMBER_ID,
            &vote,
            &meta,
            index * raft_kit::CHUNK_BYTES,
            &bytes,
            false,
        )))
        .await
        .unwrap();
    }
    let corrupt_restart = peer
        .install_snapshot(raft_kit::timed(final_chunk.clone()))
        .await
        .expect_err("a matching final chunk must not acknowledge corrupted earlier bytes");
    assert_eq!(corrupt_restart.code(), Code::DataLoss, "{corrupt_restart}");
    let mut other =
        raft_kit::peer_client(cluster.member().listen_addr().unwrap(), raft_kit::NODE_ID).await;
    let mut changed = final_chunk.clone();
    changed.header.as_mut().unwrap().from_node_id = raft_kit::NODE_ID;
    other
        .install_snapshot(raft_kit::timed(changed))
        .await
        .expect_err("a different authenticated peer must not replay the reply");
    peer.install_snapshot(raft_kit::timed(final_chunk))
        .await
        .expect("conflicting requests do not erase the exact completion");
    assert_eq!(
        raft_kit::published_pair(cluster.member_dir.path()).unwrap(),
        before
    );
    assert!(raft_kit::incoming_dirs(cluster.member_dir.path()).is_empty());
    assert!(raft_kit::core_running(cluster.member()));
    cluster.shutdown().await;
}

/// The finalizer must own its receive even after its RPC caller disappears.
/// The existing store-install gate pins the first RPC in flight while an
/// identical retry is issued on a fresh connection.
#[cfg(feature = "fault-injection")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r153_a_cancelled_final_rpc_keeps_ownership_until_its_retry_can_complete() {
    let _serial = serial();
    let group = kit::identity(kit::SEED);
    let mut cluster =
        Cluster::bootstrap("r153-final-cancel", &group, &kit::policy(), &kit::limits()).await;
    propose_ok(cluster.leader(), &prepare_command(&group, 1)).await;
    let (image, meta) = raft_kit::snapshot_image(&cluster.leader_dir, cluster.leader()).await;
    cluster.start_member().await;
    let mut peer =
        raft_kit::peer_client(cluster.member().listen_addr().unwrap(), raft_kit::PEER_ID).await;
    let vote = raft_kit::current_vote(cluster.leader());
    let offset = (image.len() - 1) / raft_kit::CHUNK_BYTES * raft_kit::CHUNK_BYTES;
    for (index, chunk) in image[..offset].chunks(raft_kit::CHUNK_BYTES).enumerate() {
        peer.install_snapshot(raft_kit::timed(raft_kit::chunk_request(
            &group,
            raft_kit::PEER_ID,
            raft_kit::MEMBER_ID,
            &vote,
            &meta,
            index * raft_kit::CHUNK_BYTES,
            chunk,
            false,
        )))
        .await
        .unwrap();
    }
    let final_chunk = raft_kit::chunk_request(
        &group,
        raft_kit::PEER_ID,
        raft_kit::MEMBER_ID,
        &vote,
        &meta,
        offset,
        &image[offset..],
        true,
    );
    let gate = cluster.member().arm_snapshot_install_gate();
    let first = final_chunk.clone();
    let sending = tokio::spawn(async move { peer.install_snapshot(raft_kit::timed(first)).await });
    gate.reached().await;
    sending.abort();
    let _ = sending.await;
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        cluster.member().wait_final_snapshot_handler_drops(1),
    )
    .await
    .expect("aborting the client connection must cancel the server-side handler before install finishes");
    assert_eq!(cluster.member().final_snapshot_handler_drops(), 1);
    let mut retry =
        raft_kit::peer_client(cluster.member().listen_addr().unwrap(), raft_kit::PEER_ID).await;
    let (reply, ()) = tokio::join!(
        retry.install_snapshot(raft_kit::timed(final_chunk)),
        async {
            tokio::task::yield_now().await;
            gate.release();
        },
    );
    reply.expect("a retry joins the owned finalization after caller cancellation");
    assert_eq!(
        raft_kit::durable_position(cluster.member()),
        meta.last_log_id.unwrap().index
    );
    assert!(raft_kit::incoming_dirs(cluster.member_dir.path()).is_empty());
    assert!(raft_kit::core_running(cluster.member()));
    cluster.shutdown().await;
}

#[cfg(feature = "fault-injection")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r153_a_final_rpc_cancelled_before_core_handoff_releases_staging_and_allows_fresh_retry() {
    let _serial = serial();
    let group = kit::identity(kit::SEED);
    let mut cluster = Cluster::bootstrap(
        "r153-handoff-cancel",
        &group,
        &kit::policy(),
        &kit::limits(),
    )
    .await;
    propose_ok(cluster.leader(), &prepare_command(&group, 1)).await;
    let (image, meta) = raft_kit::snapshot_image(&cluster.leader_dir, cluster.leader()).await;
    cluster.start_member().await;
    let addr = cluster.member().listen_addr().unwrap();
    let mut peer = raft_kit::peer_client(addr, raft_kit::PEER_ID).await;
    let vote = raft_kit::current_vote(cluster.leader());
    let offset = (image.len() - 1) / raft_kit::CHUNK_BYTES * raft_kit::CHUNK_BYTES;
    assert!(
        offset > 0,
        "multi-chunk snapshot exercises stale-continuation recovery"
    );
    for (index, chunk) in image[..offset].chunks(raft_kit::CHUNK_BYTES).enumerate() {
        peer.install_snapshot(raft_kit::timed(raft_kit::chunk_request(
            &group,
            raft_kit::PEER_ID,
            raft_kit::MEMBER_ID,
            &vote,
            &meta,
            index * raft_kit::CHUNK_BYTES,
            chunk,
            false,
        )))
        .await
        .unwrap();
    }
    let final_chunk = raft_kit::chunk_request(
        &group,
        raft_kit::PEER_ID,
        raft_kit::MEMBER_ID,
        &vote,
        &meta,
        offset,
        &image[offset..],
        true,
    );
    let handoff = cluster.member().arm_snapshot_handoff_gate();
    let first = final_chunk.clone();
    let sending = tokio::spawn(async move { peer.install_snapshot(raft_kit::timed(first)).await });
    handoff.reached().await;
    sending.abort();
    let _ = sending.await;
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        cluster.member().wait_final_snapshot_handler_drops(1),
    )
    .await
    .expect("server handler must drop before install_full_snapshot is polled");
    assert!(
        raft_kit::incoming_dirs(cluster.member_dir.path()).is_empty(),
        "dropping the verified snapshot before core handoff must remove its incoming directory"
    );
    let mut retry = raft_kit::peer_client(addr, raft_kit::PEER_ID).await;
    let stale = retry
        .install_snapshot(raft_kit::timed(final_chunk))
        .await
        .expect_err("a discarded pre-handoff receive must not replay a stale receipt");
    assert_eq!(stale.code(), Code::FailedPrecondition, "{stale}");
    raft_kit::install_chunks(
        &mut retry,
        &group,
        raft_kit::PEER_ID,
        raft_kit::MEMBER_ID,
        &vote,
        &meta,
        &image,
    )
    .await
    .expect("a fresh transfer from offset zero succeeds after pre-handoff cancellation");
    assert_eq!(
        raft_kit::durable_position(cluster.member()),
        meta.last_log_id.unwrap().index
    );
    assert!(raft_kit::incoming_dirs(cluster.member_dir.path()).is_empty());
    assert!(raft_kit::core_running(cluster.member()));
    cluster.shutdown().await;
}

//! Off-worker blocking verification and higher-vote staging during stalled installs.

use super::*;

#[cfg(feature = "fault-injection")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r153_blocking_image_verification_does_not_starve_tokio_workers_under_concurrent_retries() {
    let _serial = serial();
    let group = kit::identity(kit::SEED);
    let mut cluster = Cluster::bootstrap(
        "r153-verify-nonblocking",
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
    let verify_gate = cluster.member().arm_snapshot_verify_gate();
    let first_req = final_chunk.clone();
    let primary =
        tokio::spawn(async move { peer.install_snapshot(raft_kit::timed(first_req)).await });
    verify_gate.reached().await;

    // Issue three concurrent retries on a 2-worker runtime; synchronous mutex
    // waits would pin every Tokio worker thread here.
    let mut retries = Vec::new();
    for _ in 0..3 {
        let mut client = raft_kit::peer_client(addr, raft_kit::PEER_ID).await;
        let req = final_chunk.clone();
        retries.push(tokio::spawn(async move {
            client.install_snapshot(raft_kit::timed(req)).await
        }));
    }
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    let mut heartbeat_peer = raft_kit::peer_client(addr, raft_kit::PEER_ID).await;
    let heartbeat = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        heartbeat_peer.append_entries(raft_kit::timed(
            pipestream_search::pb::storage::RaftAppendEntriesRequest {
                header: Some(raft_kit::header(
                    &group,
                    raft_kit::PEER_ID,
                    raft_kit::MEMBER_ID,
                )),
                vote: Some(vote),
                prev_log_id: None,
                entries: Vec::new(),
                leader_commit: None,
            },
        )),
    )
    .await
    .expect("Raft heartbeats must not stall while snapshot verification runs on blocking pool");
    assert!(heartbeat.is_ok(), "{heartbeat:?}");

    verify_gate.release();
    primary
        .await
        .unwrap()
        .expect("primary final chunk succeeds after verification releases");
    for retry in retries {
        retry
            .await
            .unwrap()
            .expect("concurrent exact retries replay the verified completion");
    }
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
async fn r153_a_higher_vote_can_stage_while_an_earlier_install_is_stalled() {
    let _serial = serial();
    let group = kit::identity(kit::SEED);
    let mut cluster =
        Cluster::bootstrap("r153-higher-vote", &group, &kit::policy(), &kit::limits()).await;
    propose_ok(cluster.leader(), &prepare_command(&group, 1)).await;
    let (image, meta) = raft_kit::snapshot_image(&cluster.leader_dir, cluster.leader()).await;
    cluster.start_member().await;
    let addr = cluster.member().listen_addr().unwrap();
    let mut peer = raft_kit::peer_client(addr, raft_kit::PEER_ID).await;
    let vote = raft_kit::current_vote(cluster.leader());
    let gate = cluster.member().arm_snapshot_install_gate();
    let (first_image, first_meta, first_group) = (image.clone(), meta.clone(), group.clone());
    let first = tokio::spawn(async move {
        raft_kit::install_chunks(
            &mut peer,
            &first_group,
            raft_kit::PEER_ID,
            raft_kit::MEMBER_ID,
            &vote,
            &first_meta,
            &first_image,
        )
        .await
    });
    gate.reached().await;
    let mut successor = raft_kit::peer_client(addr, raft_kit::NODE_ID).await;
    let newer_vote = pipestream_search::pb::storage::RaftVote {
        term: vote.term + 1,
        ..vote
    };
    // The ordinary chunk deadline bounds this control; no enlarged timeout.
    let result = tokio::time::timeout(
        std::time::Duration::from_millis(raft_kit::host_config().install_snapshot_timeout_ms),
        successor.install_snapshot(raft_kit::timed(raft_kit::chunk_request(
            &group,
            raft_kit::NODE_ID,
            raft_kit::MEMBER_ID,
            &newer_vote,
            &meta,
            0,
            &image[..raft_kit::CHUNK_BYTES],
            false,
        ))),
    )
    .await;
    gate.release();
    let first_result = first.await.unwrap();
    result
        .expect("a higher-vote transfer can stage while the old install is stalled")
        .expect("the superseding transfer begins");
    first_result.expect("the earlier install retained its own bytes");
    let replies = raft_kit::install_chunks(
        &mut successor,
        &group,
        raft_kit::NODE_ID,
        raft_kit::MEMBER_ID,
        &newer_vote,
        &meta,
        &image,
    )
    .await
    .expect("the higher-vote transfer completes once the store worker can advance");
    assert!(replies.last().unwrap().outcome.as_ref().is_some_and(|outcome| matches!(outcome, pipestream_search::pb::storage::raft_install_snapshot_response::Outcome::Vote(v) if v.term == newer_vote.term)));
    // Advance the vote after the completion was cached. An exact replay
    // must read the core's new vote, not the receipt's earlier reply.
    let advanced = pipestream_search::pb::storage::RaftVote {
        term: newer_vote.term + 1,
        node_id: raft_kit::NODE_ID,
        committed: true,
    };
    // A Vote request is refused while the prior leader lease is fresh.
    // A committed heartbeat advances the term without waiting out that lease.
    let advanced_reply = successor
        .append_entries(raft_kit::timed(
            pipestream_search::pb::storage::RaftAppendEntriesRequest {
                header: Some(raft_kit::header(
                    &group,
                    raft_kit::NODE_ID,
                    raft_kit::MEMBER_ID,
                )),
                vote: Some(advanced),
                prev_log_id: meta.last_log_id,
                entries: Vec::new(),
                leader_commit: meta.last_log_id,
            },
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(
        advanced_reply.outcome,
        Some(pipestream_search::pb::storage::raft_append_entries_response::Outcome::Success(true))
    ));
    let offset = (image.len() - 1) / raft_kit::CHUNK_BYTES * raft_kit::CHUNK_BYTES;
    let replay = successor
        .install_snapshot(raft_kit::timed(raft_kit::chunk_request(
            &group,
            raft_kit::NODE_ID,
            raft_kit::MEMBER_ID,
            &newer_vote,
            &meta,
            offset,
            &image[offset..],
            true,
        )))
        .await
        .expect("the latest exact completion is replayable after the vote advanced")
        .into_inner();
    assert!(
        matches!(replay.outcome, Some(pipestream_search::pb::storage::raft_install_snapshot_response::Outcome::Vote(v)) if v == advanced)
    );
    assert!(raft_kit::core_running(cluster.member()));
    assert!(raft_kit::incoming_dirs(cluster.member_dir.path()).is_empty());
    cluster.shutdown().await;
}

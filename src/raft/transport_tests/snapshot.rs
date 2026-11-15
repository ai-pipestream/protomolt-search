//! A snapshot transfer that goes idle.

use super::*;

/// A one-node leader with one prepared owner and a published snapshot:
/// the leader, its applied index, and the image and meta it published.
pub(super) async fn snapshot_leader(
    group: &SourceAuthorityIdentity,
    directory: &Arc<PeerDirectory>,
    leader_dir: &Directory,
) -> (RaftHost, u64, Vec<u8>, crate::pb::storage::RaftSnapshotMeta) {
    use prost::Message as _;
    let leader = RaftHost::bootstrap_cluster(
        &leader_dir.0,
        group,
        1,
        &policy(),
        &limits(),
        &cluster_config(),
        transport(1, directory),
    )
    .await
    .unwrap();
    let decision = leader
        .propose_command("alice", &prepare(group, "phone-a", "phone-a", 1))
        .await
        .unwrap();
    assert_eq!(decision.code, 0, "{}", decision.message);
    let applied = leader.applied_position().unwrap().unwrap().index;
    leader.trigger_snapshot().await.unwrap();
    leader
        .wait(Some(Duration::from_secs(30)))
        .metrics(
            |m| m.snapshot.is_some_and(|s| s.index >= applied),
            "leader snapshot",
        )
        .await
        .unwrap();
    let generation = super::super::state_machine::published_generation(
        &leader_dir.0.join(super::super::host::SNAPSHOT_DIR),
    )
    .unwrap()
    .unwrap();
    let image = std::fs::read(generation.join("image.redb")).unwrap();
    let meta = crate::pb::storage::RaftSnapshotMeta::decode(
        std::fs::read(generation.join("meta")).unwrap().as_slice(),
    )
    .unwrap();
    (leader, applied, image, meta)
}

/// Node 2, prepared and started as a member awaiting its seed.
pub(super) async fn prepared_member(
    group: &SourceAuthorityIdentity,
    directory: &Arc<PeerDirectory>,
    member_dir: &Directory,
) -> RaftHost {
    RaftHost::prepare_member(&member_dir.0, group, 2, &policy(), &limits()).unwrap();
    RaftHost::start_member(
        &member_dir.0,
        group,
        2,
        &cluster_config(),
        transport(2, directory),
    )
    .await
    .unwrap()
}

/// The leader's current vote in the transport's form.
pub(super) fn wire_vote(leader: &RaftHost) -> RaftVote {
    let v = leader.metrics().borrow().vote;
    RaftVote {
        term: v.leader_id.term,
        node_id: v.leader_id.node_id,
        committed: v.committed,
    }
}

/// Receive directories left under a member's snapshot directory.
pub(super) fn incoming_dirs(member_dir: &Directory) -> usize {
    std::fs::read_dir(member_dir.0.join(super::super::host::SNAPSHOT_DIR))
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("incoming-")
        })
        .count()
}

/// A snapshot transfer with no chunk for the idle timeout is dropped with
/// its bytes and its place is free: another peer under the same vote is
/// refused while the transfer is live and accepted once it has gone idle,
/// and its complete transfer installs on the running core.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_idle_snapshot_transfer_is_dropped_and_its_place_freed() {
    use crate::pb::storage::{
        raft_install_snapshot_response::Outcome as InstallOutcome, RaftInstallSnapshotRequest,
    };
    let group = identity(9);
    let directory = directory(&group, &[1, 2, 3, 4]);
    let leader_dir = Directory::new("idle-leader");
    let (leader, applied, image, meta) = snapshot_leader(&group, &directory, &leader_dir).await;
    let member_dir = Directory::new("idle-member");
    let member = prepared_member(&group, &directory, &member_dir).await;
    let addr = member.listen_addr().unwrap();
    let vote = wire_vote(&leader);
    let chunk = |from: NodeId, offset: usize, data: &[u8], done: bool| {
        timed(RaftInstallSnapshotRequest {
            header: Some(header(&group, from, 2)),
            vote: Some(vote),
            meta: Some(meta.clone()),
            offset: offset as u64,
            data: data.to_vec(),
            done,
        })
    };
    // Node 4 begins and goes quiet after one chunk.
    let mut quiet = raw_client(addr, "node-4").await.unwrap();
    quiet
        .install_snapshot(chunk(4, 0, &image[..4096], false))
        .await
        .unwrap();
    // Node 1 is refused while that transfer is live: the same snapshot id
    // is bound to node 4.
    let mut sender = raw_client(addr, "node-1").await.unwrap();
    let error = sender
        .install_snapshot(chunk(1, 0, &image[..4096], false))
        .await
        .unwrap_err();
    assert_reason(
        &error,
        Code::FailedPrecondition,
        super::super::reasons::TRANSPORT_SNAPSHOT_TRANSFER_BOUND,
    );
    assert!(error.message().contains("bound to node 4"), "{error}");
    // Past the idle timeout the quiet transfer is dropped and node 1's
    // transfer takes its place and completes; the core ran throughout.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut offset = 0;
    let chunks: Vec<&[u8]> = image.chunks(128 << 10).collect();
    for (i, piece) in chunks.iter().enumerate() {
        let reply = sender
            .install_snapshot(chunk(1, offset, piece, i + 1 == chunks.len()))
            .await
            .unwrap()
            .into_inner();
        assert!(matches!(reply.outcome, Some(InstallOutcome::Vote(_))));
        offset += piece.len();
    }
    assert!(member.metrics().borrow().running_state.is_ok());
    assert!(!member.awaiting_snapshot().unwrap());
    assert_eq!(member.applied_position().unwrap().unwrap().index, applied);
    // The quiet peer's stale continuation continues no transfer: it is
    // rejected by name (the sender begins a transfer at offset zero), and
    // the core is not involved.
    let error = quiet
        .install_snapshot(chunk(4, 4096, &image[4096..8192], false))
        .await
        .unwrap_err();
    let stale = super::super::reasons::TRANSPORT_SNAPSHOT_STALE_CONTINUATION;
    assert_reason(&error, Code::FailedPrecondition, stale);
    assert!(
        error.message().contains("continues no transfer at node"),
        "{error}"
    );
    assert_eq!(incoming_dirs(&member_dir), 0);
    member.shutdown().await.unwrap();
    leader.shutdown().await.unwrap();
}

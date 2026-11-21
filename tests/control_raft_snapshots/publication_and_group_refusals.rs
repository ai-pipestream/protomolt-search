//! Foreign-group and wrong-membership refusals and atomic generation publication.

use super::*;

/// R3: a valid image of ANOTHER group must refuse, and the previous
/// generation must still be served. At 331c18f the install validated only
/// AFTER replacing current.redb and publishing current.meta, so the previous
/// generation was destroyed even though the install then errored. Two
/// shapes reach the receiver: an honest meta naming the other group (refused
/// by the transport before the library sees it) and a forged meta naming
/// this group at a newer index over the foreign bytes (refused by the
/// receiver's probe of the image as a store of this group).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r3_wrong_group_valid_image_is_refused_and_previous_survives() {
    let _serial = serial();
    let (mut cluster, _image, newer) = detached("r3-group-a").await;
    let (before, phase_before) = member_state(&cluster);
    let (_dir_b, host_b, image_b, meta_b) = foreign_image("r3-group-b").await;
    let newer_index = newer.last_log_id.unwrap().index;
    let mut findings: Vec<String> = Vec::new();

    // (i) the honest shape: the transport refuses a meta of another group.
    match cluster.push(&meta_b, &image_b).await {
        Ok(()) => findings.push("a foreign-group meta was accepted by the transport".into()),
        Err(error) => {
            if error.code() != Code::PermissionDenied || !error.message().contains("group") {
                findings.push(format!(
                    "the transport refusal does not name the group: {error}"
                ));
            }
        }
    }
    assert!(
        raft_kit::core_running(cluster.member()),
        "a transport refusal never reaches the library"
    );

    // (ii) the forged shape: this group's name and a newer index over the
    // foreign bytes, self-consistent by checksum.
    let forged = raft_kit::renamed(
        &raft_kit::with_group(&meta_b, &cluster.group),
        newer_index,
        &image_b,
    );
    match cluster.push(&forged, &image_b).await {
        Ok(()) => findings.push("a foreign-group image installed under a forged meta".into()),
        Err(error) => {
            let message = error.message();
            if !(message.contains("identity")
                || message.contains("authority")
                || message.contains("group")
                || message.contains("incarnation"))
            {
                findings.push(format!(
                    "the refusal does not name the group/identity: {message}"
                ));
            }
        }
    }
    recover_if_stopped(&mut cluster, "r3 wrong group").await;
    // REQUIRED: the previous generation is still served, byte for byte, and
    // the live store keeps serving reads.
    let (after, phase_after) = member_state(&cluster);
    if after.0 != before.0 || after.1.snapshot_id != before.1.snapshot_id {
        findings.push(format!(
            "the previous generation was destroyed: the member publishes {} bytes under {}",
            after.0.len(),
            after.1.snapshot_id
        ));
    }
    if phase_after != phase_before {
        findings.push("the member's committed owner changed under a refused install".into());
    }
    assert!(
        findings.is_empty(),
        "R3 REQUIRED a group-naming refusal that preserves the previous \
         generation; observed:\n  - {}",
        findings.join("\n  - ")
    );
    host_b.shutdown().await.unwrap();
    cluster.shutdown().await;
}

/// R3: the same group's image with a correct applied position but a
/// membership naming different voters must refuse BEFORE publication. At
/// 331c18f only the applied index was compared and the in-memory
/// membership was then set from the meta, so the install succeeded.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r3_correct_index_wrong_membership_is_refused() {
    let _serial = serial();
    let (mut cluster, image, newer) = detached("r3-membership").await;
    let (before, _) = member_state(&cluster);
    // Same bytes, same applied index, voters {9} instead of {1}.
    let forged = raft_kit::meta_pb_for(
        &cluster.group,
        newer.last_log_id.unwrap().index,
        &image,
        &[9],
    );
    assert_eq!(forged.snapshot_id, newer.snapshot_id);
    let error = match cluster.push(&forged, &image).await {
        Ok(()) => panic!(
            "R3 REQUIRED refusal of a correct-index/wrong-membership image before \
             publication; the install succeeded"
        ),
        Err(error) => error,
    };
    assert!(
        error.message().contains("membership"),
        "the refusal must name the membership: {error}"
    );
    recover_if_stopped(&mut cluster, "r3 wrong membership").await;
    let (after, phase) = member_state(&cluster);
    assert_eq!(after.1.snapshot_id, before.1.snapshot_id);
    assert_eq!(phase, PreparedSourceOwnerPhase::Prepared as i32);
    // The genuine newer generation installs on the same member.
    cluster.push(&newer, &image).await.unwrap();
    assert_eq!(
        raft_kit::durable_position(cluster.member()),
        newer.last_log_id.unwrap().index
    );
    cluster.shutdown().await;
}

/// R3: after a rejected install the member recovers — restarting it must
/// find the previous generation and state intact, at the same applied
/// position, and the group takes it in and replicates to it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r3_rejected_install_leaves_previous_usable_after_restart() {
    let _serial = serial();
    let (mut cluster, _image, newer) = detached("r3-restart").await;
    let (before, _) = member_state(&cluster);
    let position_before = raft_kit::durable_position(cluster.member());
    let (_dir_b, host_b, image_b, meta_b) = foreign_image("r3-restart-b").await;
    let forged = raft_kit::renamed(
        &raft_kit::with_group(&meta_b, &cluster.group),
        newer.last_log_id.unwrap().index,
        &image_b,
    );
    assert!(cluster.push(&forged, &image_b).await.is_err());
    cluster.restart_member().await;

    let mut findings: Vec<String> = Vec::new();
    let (after, phase) = member_state(&cluster);
    if after.0 != before.0 {
        findings.push(format!(
            "after restart the published generation is {} bytes, not the \
             preserved {}-byte previous generation",
            after.0.len(),
            before.0.len()
        ));
    }
    if phase != PreparedSourceOwnerPhase::Prepared as i32 {
        findings.push("the reopened store does not serve the committed owner".into());
    }
    if raft_kit::durable_position(cluster.member()) != position_before {
        findings.push("the applied position moved across the rejected install".into());
    }
    assert!(
        findings.is_empty(),
        "R3 REQUIRED the previous generation and store state to survive a \
         rejected install across restart; observed:\n  - {}",
        findings.join("\n  - ")
    );
    // The group takes the member in (seeded by the leader's newer image) and
    // a later change lands on it.
    cluster.add_member().await;
    propose_ok(
        cluster.leader(),
        &grant_command(&cluster.group, 3, 2, "carol"),
    )
    .await;
    raft_kit::wait_applied(
        cluster.member(),
        raft_kit::durable_position(cluster.leader()),
    )
    .await;
    assert_eq!(
        cluster
            .member()
            .store()
            .unwrap()
            .policy("alice", kit::WORKSPACE, kit::COLLECTION)
            .unwrap()
            .revision,
        3
    );
    host_b.shutdown().await.unwrap();
    cluster.shutdown().await;
}

/// R3: an interrupted publication must never pair mismatched image/meta
/// silently. Each case starts from the member's good generation with the
/// member stopped, damages the layout the way an interruption would, and
/// restarts it: the member must either serve the COMPLETE previous
/// generation or refuse to start by name. The forged-pointer case (d2)
/// pins that a self-consistent forgery never becomes anyone's state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r3_interrupted_publication_boundaries() {
    let _serial = serial();
    let (mut cluster, _image, newer) = detached("r3-int").await;
    let (good, _) = member_state(&cluster);
    let generation = raft_kit::published_generation(cluster.member_dir.path()).unwrap();
    let position = raft_kit::durable_position(cluster.member());
    let snapshots = raft_kit::snapshots_dir(cluster.member_dir.path());
    let (_dir_b, host_b, image_b, _meta_b) = foreign_image("r3-int-b").await;

    // (a) a later generation directory whose pointer never landed (a build
    // interrupted between the rename and the publish): swept at start, the
    // published pair untouched.
    cluster.stop_member().await;
    let orphan = raft_kit::generation_dir(cluster.member_dir.path(), generation + 1);
    std::fs::create_dir_all(&orphan).unwrap();
    std::fs::write(orphan.join("image.redb"), &image_b).unwrap();
    std::fs::write(orphan.join("meta"), b"partial").unwrap();
    cluster.start_member().await;
    assert!(
        !orphan.exists(),
        "(a) the unpublished generation survived the sweep"
    );
    assert_eq!(
        member_state(&cluster).0,
        good,
        "(a) served something other than the complete previous generation"
    );

    // (b) a leftover pointer temp file must not disturb the published pair.
    cluster.stop_member().await;
    std::fs::write(snapshots.join("current.tmp"), b"garbage").unwrap();
    cluster.start_member().await;
    assert_eq!(
        member_state(&cluster).0,
        good,
        "(b) the leftover tmp file disturbed the published pair"
    );

    // (c) a leftover staged.redb from an interrupted install is removed.
    cluster.stop_member().await;
    std::fs::write(snapshots.join("staged.redb"), &good.0).unwrap();
    cluster.start_member().await;
    assert!(
        !snapshots.join("staged.redb").exists(),
        "(c) the staged file survived the sweep"
    );
    assert_eq!(
        member_state(&cluster).0,
        good,
        "(c) served something other than the complete previous generation"
    );

    // (d1) a meta claiming a newer index while the image is the old bytes:
    // the id no longer matches the image, so start must refuse loudly.
    cluster.stop_member().await;
    let meta_path = raft_kit::generation_dir(cluster.member_dir.path(), generation).join("meta");
    let original = std::fs::read(&meta_path).unwrap();
    let mut pb = RaftSnapshotMeta::decode(original.as_slice()).unwrap();
    pb.last_log_id.as_mut().unwrap().index += 1;
    std::fs::write(&meta_path, pb.encode_to_vec()).unwrap();
    let refused = RaftHost::start_member(
        cluster.member_dir.path(),
        &cluster.group,
        raft_kit::MEMBER_ID,
        &raft_kit::host_config(),
        raft_kit::transport(
            raft_kit::MEMBER_ID,
            &cluster.directory,
            "127.0.0.1:0".parse().unwrap(),
        ),
    )
    .await;
    let error = match refused {
        Ok(host) => {
            let _ = host.shutdown().await;
            panic!("(d1) started over an image/meta pair whose id disagrees")
        }
        Err(error) => error,
    };
    assert!(
        error.message().contains("differs"),
        "(d1) the refusal must name the mismatch: {error}"
    );
    std::fs::write(&meta_path, &original).unwrap();

    // (d2) a forged generation pointer: the meta renamed to the leader's
    // newer index with its id recomputed over the old bytes. Self-consistent
    // by checksum, so the checksum cannot tell; the position can: a
    // generation claiming a position past the store's applied position is
    // data loss, and start refuses it by name (the library's own invariant
    // would otherwise be violated at startup). REQUIRED: the forged position
    // never becomes state.
    let forged = raft_kit::renamed(&pb, newer.last_log_id.unwrap().index, &good.0);
    std::fs::write(&meta_path, forged.encode_to_vec()).unwrap();
    let refused = RaftHost::start_member(
        cluster.member_dir.path(),
        &cluster.group,
        raft_kit::MEMBER_ID,
        &raft_kit::host_config(),
        raft_kit::transport(
            raft_kit::MEMBER_ID,
            &cluster.directory,
            "127.0.0.1:0".parse().unwrap(),
        ),
    )
    .await;
    let error = match refused {
        Ok(host) => {
            let _ = host.shutdown().await;
            panic!("(d2) started over a generation claiming a position the store never applied")
        }
        Err(error) => error,
    };
    assert!(
        error
            .message()
            .contains("past the store's applied position"),
        "(d2) the refusal must name the position: {error}"
    );
    // With the genuine meta back the member starts, at its own position,
    // and the group takes it in from the leader's real newer image.
    std::fs::write(&meta_path, &original).unwrap();
    cluster.start_member().await;
    assert_eq!(raft_kit::durable_position(cluster.member()), position);
    cluster.add_member().await;
    let leader_position = raft_kit::durable_position(cluster.leader());
    raft_kit::wait_applied(cluster.member(), leader_position).await;
    let (_, published) = raft_kit::published_pair(cluster.member_dir.path()).unwrap();
    assert!(raft_kit::published_generation(cluster.member_dir.path()).unwrap() > generation);
    assert_eq!(published.snapshot_id, newer.snapshot_id);
    assert_eq!(
        cluster
            .member()
            .store()
            .unwrap()
            .policy("alice", kit::WORKSPACE, kit::COLLECTION)
            .unwrap()
            .revision,
        2,
        "the member's state is the leader's real state"
    );
    host_b.shutdown().await.unwrap();
    cluster.shutdown().await;
}

/// R3: a reader held across a successful publication keeps its consistent
/// old view. At 331c18f the install copied the new image over current.redb
/// IN PLACE, so a file opened on the previous generation observed the
/// overwritten bytes. Generations are immutable directories now: the held
/// file keeps its bytes after the pointer moved and its directory was
/// removed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r3_reader_held_across_publication() {
    let _serial = serial();
    let group = kit::identity(kit::SEED);
    let mut cluster = Cluster::bootstrap("r3-reader", &group, &kit::policy(), &kit::limits()).await;
    propose_ok(cluster.leader(), &prepare_command(&group, 1)).await;
    cluster.join_member().await;
    let generation = raft_kit::published_generation(cluster.member_dir.path()).unwrap();
    let mut held = std::fs::File::open(
        raft_kit::generation_dir(cluster.member_dir.path(), generation).join("image.redb"),
    )
    .unwrap();
    let mut before = Vec::new();
    held.read_to_end(&mut before).unwrap();

    // A later generation of the member: one more committed change, then the
    // member builds and publishes its own snapshot.
    propose_ok(cluster.leader(), &grant_command(&group, 2, 1, "bob")).await;
    raft_kit::wait_applied(
        cluster.member(),
        raft_kit::durable_position(cluster.leader()),
    )
    .await;
    let (published, meta) = raft_kit::snapshot_image(&cluster.member_dir, cluster.member()).await;
    assert!(raft_kit::published_generation(cluster.member_dir.path()).unwrap() > generation);
    assert_ne!(
        published, before,
        "the new generation carries the new state"
    );
    assert_eq!(
        meta.last_log_id.unwrap().index,
        raft_kit::durable_position(cluster.member())
    );

    // REQUIRED: the held reader still sees its original bytes.
    held.seek(SeekFrom::Start(0)).unwrap();
    let mut after = Vec::new();
    held.read_to_end(&mut after).unwrap();
    assert_eq!(
        after, before,
        "R3 REQUIRED a reader held across publication to keep its consistent \
         old view; the file now returns different bytes"
    );
    cluster.shutdown().await;
}

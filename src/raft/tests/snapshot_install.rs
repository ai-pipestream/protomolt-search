use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_snapshot_installs_into_a_fresh_replica_with_the_same_map() {
    let dir = Directory::new("snapshot");
    let group = identity(7);
    let host = RaftHost::bootstrap_single(
        &dir.0,
        &group,
        1,
        "https://node-1:19291",
        &policy(),
        &limits(),
        &config(),
    )
    .await
    .unwrap();
    for (owner_id, id, revision) in [("phone", "p1", 1), ("tablet", "p2", 2)] {
        assert_eq!(
            host.propose_command("alice", &prepare(&group, owner_id, id, revision))
                .await
                .unwrap()
                .code,
            0
        );
    }
    let published = snapshot_now(&host, &dir).await;
    let source_owner = host
        .store()
        .unwrap()
        .owner("alice", &owner("tablet"))
        .unwrap();
    host.shutdown().await.unwrap();

    // A fresh replica of the same group receives the image and installs it.
    let replica_dir = Directory::new("snapshot-replica");
    let mut machine = replica_machine(&replica_dir, &group);
    let (before, _) = machine.applied_state().await.unwrap();
    assert_eq!(before, None);
    a_corrupted_image_refuses(&mut machine, &published).await;
    foreign_and_mismatched_images_refuse(&mut machine, &replica_dir, &published).await;
    an_install_swaps_under_a_held_handle(&mut machine, &published, &source_owner).await;
    a_restart_sweeps_and_bounds_the_receive(machine, &replica_dir, &group, &published).await;
}

/// A corrupted image refuses before anything is replaced.
async fn a_corrupted_image_refuses(machine: &mut ControlStateMachine, published: &Published) {
    let (image, meta) = (&published.image, &published.meta);
    let mut file = machine.begin_receiving_snapshot().await.unwrap();
    file.write_all(image).await.unwrap();
    let mut corrupt = meta.clone();
    corrupt.snapshot_id = format_snapshot_id(
        meta.last_log_id.as_ref(),
        &ImageSignature {
            length: image.len() as u64,
            sha256: [0; 32],
        },
    );
    let refused = machine.install_snapshot(&corrupt, file).await;
    assert!(refused.is_err());
    let (still, _) = machine.applied_state().await.unwrap();
    assert_eq!(still, None);
}

/// A valid image of another group is refused on the probe, and a meta
/// whose membership differs from the image is refused too; the replica
/// keeps its state and has no published snapshot yet.
async fn foreign_and_mismatched_images_refuse(
    machine: &mut ControlStateMachine,
    replica_dir: &Directory,
    published: &Published,
) {
    let (image, meta) = (&published.image, &published.meta);
    let foreign_dir = Directory::new("snapshot-foreign");
    let foreign_bytes = {
        let foreign = crate::source_authority::SourceAuthorityStore::create(
            &foreign_dir.0.join("foreign.redb"),
            &identity(9),
            &policy(),
            &limits(),
        )
        .unwrap();
        drop(foreign);
        std::fs::read(foreign_dir.0.join("foreign.redb")).unwrap()
    };
    let mut file = machine.begin_receiving_snapshot().await.unwrap();
    file.write_all(&foreign_bytes).await.unwrap();
    let mut foreign_meta = meta.clone();
    foreign_meta.snapshot_id = format_snapshot_id(
        meta.last_log_id.as_ref(),
        &ImageSignature {
            length: foreign_bytes.len() as u64,
            sha256: turbo_sha256::sha256(&foreign_bytes).0,
        },
    );
    assert!(machine.install_snapshot(&foreign_meta, file).await.is_err());
    let mut file = machine.begin_receiving_snapshot().await.unwrap();
    file.write_all(image).await.unwrap();
    let mut wrong_membership = meta.clone();
    wrong_membership.last_membership = openraft::StoredMembership::new(
        meta.last_log_id,
        Membership::new(
            vec![BTreeSet::from([1, 2])],
            BTreeMap::from([
                (1, BasicNode { addr: "a".into() }),
                (2, BasicNode { addr: "b".into() }),
            ]),
        ),
    );
    assert!(machine
        .install_snapshot(&wrong_membership, file)
        .await
        .is_err());
    assert_eq!(machine.applied_state().await.unwrap().0, None);
    assert!(machine.get_current_snapshot().await.unwrap().is_none());
    // Every refused receive left nothing behind.
    let leftovers = std::fs::read_dir(replica_dir.0.join(crate::raft::host::SNAPSHOT_DIR))
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("incoming-")
        })
        .count();
    assert_eq!(leftovers, 0);
}

/// A held handle never blocks the swap: the install replaces the
/// database in place, the held handle observes the installed state on
/// its next operation, and the subscriber attached before the install
/// wakes after it.
async fn an_install_swaps_under_a_held_handle(
    machine: &mut ControlStateMachine,
    published: &Published,
    source_owner: &crate::pb::storage::PreparedSourceOwner,
) {
    let (image, meta, last) = (&published.image, &published.meta, published.last);
    let mut applied_watch = machine
        .shared_store()
        .read()
        .unwrap()
        .clone()
        .unwrap()
        .subscribe_applied();
    let held = machine.shared_store().read().unwrap().clone().unwrap();
    assert_eq!(
        held.owner("alice", &owner("tablet")).err().unwrap().code(),
        Code::NotFound
    );
    let mut file = machine.begin_receiving_snapshot().await.unwrap();
    file.write_all(image).await.unwrap();
    machine.install_snapshot(meta, file).await.unwrap();
    let (after, membership) = machine.applied_state().await.unwrap();
    assert_eq!(after, Some(last));
    assert_eq!(membership, meta.last_membership);
    assert!(applied_watch.has_changed().unwrap());
    assert_eq!(*applied_watch.borrow_and_update(), 3);
    assert_eq!(
        held.owner("alice", &owner("tablet")).unwrap(),
        *source_owner,
        "the held handle serves the installed state"
    );
    drop(held);
    let replica = machine.shared_store().read().unwrap().clone().unwrap();
    assert_eq!(
        replica.owner("alice", &owner("tablet")).unwrap(),
        *source_owner
    );
    let current = machine.get_current_snapshot().await.unwrap().unwrap();
    assert_eq!(current.meta, *meta);
    drop(replica);
}

/// A bound below the image refuses the receive; abandoned receives,
/// partial builds and an unpublished generation are swept at startup
/// while the published generation stays.
async fn a_restart_sweeps_and_bounds_the_receive(
    machine: ControlStateMachine,
    replica_dir: &Directory,
    group: &SourceAuthorityIdentity,
    published: &Published,
) {
    let (image, meta) = (&published.image, &published.meta);
    let snapshots = replica_dir.0.join(crate::raft::host::SNAPSHOT_DIR);
    drop(machine);
    let store = crate::source_authority::SourceAuthorityStore::open(
        &replica_dir.0.join(crate::raft::host::STORE_FILE),
        group,
    )
    .unwrap();
    std::fs::create_dir(snapshots.join("incoming-1-0")).unwrap();
    std::fs::create_dir(snapshots.join("build-1")).unwrap();
    std::fs::create_dir(snapshots.join("generations").join("99")).unwrap();
    let mut machine = ControlStateMachine::new(store, &snapshots, 1024).unwrap();
    assert!(!snapshots.join("incoming-1-0").exists());
    assert!(!snapshots.join("build-1").exists());
    assert!(!snapshots.join("generations").join("99").exists());
    assert_eq!(
        machine.get_current_snapshot().await.unwrap().unwrap().meta,
        *meta
    );
    let mut file = machine.begin_receiving_snapshot().await.unwrap();
    file.write_all(image).await.unwrap();
    let oversize = machine.install_snapshot(meta, file).await.err().unwrap();
    assert!(oversize.to_string().contains("bound"), "{oversize}");
}

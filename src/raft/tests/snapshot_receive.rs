use super::*;

#[tokio::test]
async fn r153_receive_ownership_follows_the_file_across_a_second_receive() {
    use openraft::RaftSnapshotBuilder;
    let source = Directory::new("r153-source");
    let group = identity(7);
    let store = crate::source_authority::SourceAuthorityStore::create(
        &source.0.join("source.redb"),
        &group,
        &policy(),
        &limits(),
    )
    .unwrap();
    let mut source_machine =
        ControlStateMachine::new(store, &source.0.join("snapshots"), 64 << 20).unwrap();
    source_machine
        .apply([Entry {
            log_id: log_id(1, 1, 1),
            payload: EntryPayload::Blank,
        }])
        .await
        .unwrap();
    let mut snapshot = source_machine
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .unwrap();
    let mut image = Vec::new();
    snapshot.snapshot.read_to_end(&mut image).await.unwrap();
    let replica = Directory::new("r153-replica");
    let store = crate::source_authority::SourceAuthorityStore::create(
        &replica.0.join("replica.redb"),
        &group,
        &policy(),
        &limits(),
    )
    .unwrap();
    let snapshots = replica.0.join("snapshots");
    let mut machine = ControlStateMachine::new(store, &snapshots, 64 << 20).unwrap();
    let mut first = machine.begin_receiving_snapshot().await.unwrap();
    let completed = std::sync::Arc::new(SnapshotCompletion::new());
    first.set_completion(std::sync::Arc::clone(&completed));
    first.write_all(&image).await.unwrap();
    let mut second = machine.begin_receiving_snapshot().await.unwrap();
    let rejected = std::sync::Arc::new(SnapshotCompletion::new());
    second.set_completion(std::sync::Arc::clone(&rejected));
    second.write_all(b"another receive's bytes").await.unwrap();
    machine
        .install_snapshot(&snapshot.meta, first)
        .await
        .expect("the first install consumes its own receive, never the newest receive");
    completed
        .wait()
        .await
        .expect("discard on Drop cannot overwrite durable installation success");
    assert_eq!(
        machine.applied_state().await.unwrap().0,
        snapshot.meta.last_log_id
    );
    assert!(machine
        .install_snapshot(&snapshot.meta, second)
        .await
        .is_err());
    assert!(
        rejected.wait().await.is_err(),
        "a failed owned install cannot record successful completion"
    );
    assert_eq!(
        machine.get_current_snapshot().await.unwrap().unwrap().meta,
        snapshot.meta
    );
    assert!(!std::fs::read_dir(&snapshots).unwrap().any(|entry| entry
        .unwrap()
        .file_name()
        .to_string_lossy()
        .starts_with("incoming-")));
}

#[tokio::test]
async fn r153_owned_receives_have_a_fixed_count_bound_and_release_it_on_drop() {
    let dir = Directory::new("r153-receive-bound");
    let store = crate::source_authority::SourceAuthorityStore::create(
        &dir.0.join("replica.redb"),
        &identity(7),
        &policy(),
        &limits(),
    )
    .unwrap();
    let snapshots = dir.0.join("snapshots");
    let mut machine = ControlStateMachine::new(store, &snapshots, 64 << 20).unwrap();
    let mut first = machine.begin_receiving_snapshot().await.unwrap();
    let cancelled = std::sync::Arc::new(SnapshotCompletion::new());
    first.set_completion(std::sync::Arc::clone(&cancelled));
    let second = machine.begin_receiving_snapshot().await.unwrap();
    assert!(
        machine.begin_receiving_snapshot().await.is_err(),
        "two owned receives exhaust the fixed finalization bound"
    );
    drop(first);
    assert!(
        cancelled.wait().await.is_err(),
        "dropping an uninstalled owned receive cannot record success"
    );
    let successor = machine
        .begin_receiving_snapshot()
        .await
        .expect("dropping an uninstalled receive releases its slot");
    drop(second);
    drop(successor);
    assert!(!std::fs::read_dir(snapshots).unwrap().any(|entry| entry
        .unwrap()
        .file_name()
        .to_string_lossy()
        .starts_with("incoming-")));
}

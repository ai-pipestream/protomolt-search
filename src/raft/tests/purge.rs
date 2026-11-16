use super::*;

/// The library purges to a snapshot before the state machine installs it,
/// and the purge is bounded by the store's applied position. The
/// remainder is recorded and completed once the store is at the position:
/// at the next append (log `a`) and at start (log `b`), both with a store
/// that applied nothing when the purge came, and with a store that had
/// applied an earlier snapshot, where the purge is cut at that position
/// (log `c`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_purge_deferred_by_the_store_completes_when_the_store_is_at_the_position() {
    let (group, mut snapshots) = snapshots_after(
        "deferred-purge",
        &[&[("phone", "p1", 1)], &[("tablet", "p2", 2)]],
    )
    .await;
    let first = snapshots.remove(0);
    let second = snapshots.remove(0);
    assert!(second.last.index > first.last.index);

    let replica_dir = Directory::new("deferred-purge-replica");
    let mut machine = replica_machine(&replica_dir, &group);
    logs_a_and_b_complete_at_the_append_and_the_start(&replica_dir, &group, &mut machine, &first)
        .await;
    log_c_is_cut_at_the_applied_snapshot(&replica_dir, &group, &mut machine, first.last, &second)
        .await;
}

/// Logs `a` and `b` over a store that applied nothing when the purge
/// came: the purge is deferred, then completed at the next append (`a`)
/// and at a start (`b`).
async fn logs_a_and_b_complete_at_the_append_and_the_start(
    replica_dir: &Directory,
    group: &SourceAuthorityIdentity,
    machine: &mut ControlStateMachine,
    snapshot: &Published,
) {
    let (first_image, first_meta, first) = (&snapshot.image, &snapshot.meta, snapshot.last);
    let log_path = |name: &str| replica_dir.0.join(format!("raft-log-{name}.redb"));
    let mut a = RaftLogStore::create(&log_path("a"), group, 2).unwrap();
    let mut b = RaftLogStore::create(&log_path("b"), group, 2).unwrap();
    for log in [&mut a, &mut b] {
        log.bind_applied_floor(machine.shared_store()).unwrap();
        log.append_sync(blanks(0, 1)).unwrap();
        // The store applied nothing: the purge moves no entry and is deferred.
        log.purge(first).await.unwrap();
        assert_eq!(log.entries().unwrap().len(), 2);
        let state = log.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, None);
        assert_eq!(state.last_log_id, Some(log_id(1, 1, 1)));
    }
    drop(b);
    let mut file = machine.begin_receiving_snapshot().await.unwrap();
    file.write_all(first_image).await.unwrap();
    machine.install_snapshot(first_meta, file).await.unwrap();
    assert_eq!(machine.applied_state().await.unwrap().0, Some(first));

    // Log a: the next append completes the purge first, then lands.
    let after_first = LogId::new(first.leader_id, first.index + 1);
    a.append_sync(vec![Entry {
        log_id: after_first,
        payload: EntryPayload::Blank,
    }])
    .unwrap();
    let state = a.get_log_state().await.unwrap();
    assert_eq!(state.last_purged_log_id, Some(first));
    assert_eq!(state.last_log_id, Some(after_first));
    assert_eq!(a.entries().unwrap().len(), 1);
    // Log b: a start completes it.
    let mut b = RaftLogStore::open(&log_path("b"), group, 2).unwrap();
    b.bind_applied_floor(machine.shared_store()).unwrap();
    let state = b.get_log_state().await.unwrap();
    assert_eq!(state.last_purged_log_id, Some(first));
    assert_eq!(state.last_log_id, Some(first));
    assert_eq!(b.entries().unwrap().len(), 0);
    b.append_sync(vec![Entry {
        log_id: after_first,
        payload: EntryPayload::Blank,
    }])
    .unwrap();
    assert_eq!(
        b.get_log_state().await.unwrap().last_log_id,
        Some(after_first)
    );
}

/// Log `c`: the store is at the first snapshot, and an empty log bound to
/// it starts after that position. A purge to the second is cut at the
/// first and the rest deferred, then completed by the install of the
/// second and the append after it.
async fn log_c_is_cut_at_the_applied_snapshot(
    replica_dir: &Directory,
    group: &SourceAuthorityIdentity,
    machine: &mut ControlStateMachine,
    first: LogId<u64>,
    snapshot: &Published,
) {
    let (second_image, second_meta, second) = (&snapshot.image, &snapshot.meta, snapshot.last);
    let log_path = |name: &str| replica_dir.0.join(format!("raft-log-{name}.redb"));
    let mut c = RaftLogStore::create(&log_path("c"), group, 2).unwrap();
    c.bind_applied_floor(machine.shared_store()).unwrap();
    assert_eq!(
        c.get_log_state().await.unwrap().last_purged_log_id,
        Some(first)
    );
    c.append_sync(blanks(first.index + 1, second.index + 1))
        .unwrap();
    c.purge(second).await.unwrap();
    let state = c.get_log_state().await.unwrap();
    assert_eq!(state.last_purged_log_id, Some(first));
    assert_eq!(state.last_log_id, Some(log_id(1, 1, second.index + 1)));
    assert_eq!(
        c.entries().unwrap().len() as u64,
        second.index + 1 - first.index
    );
    let mut file = machine.begin_receiving_snapshot().await.unwrap();
    file.write_all(second_image).await.unwrap();
    machine.install_snapshot(second_meta, file).await.unwrap();
    assert_eq!(machine.applied_state().await.unwrap().0, Some(second));
    let after_second = LogId::new(second.leader_id, second.index + 2);
    c.append_sync(vec![Entry {
        log_id: after_second,
        payload: EntryPayload::Blank,
    }])
    .unwrap();
    let state = c.get_log_state().await.unwrap();
    assert_eq!(state.last_purged_log_id, Some(second));
    assert_eq!(state.last_log_id, Some(after_second));
    assert_eq!(c.entries().unwrap().len(), 2);
    // A purge completed in full clears the record: a later start keeps the
    // entries after the snapshot.
    drop(c);
    let mut c = RaftLogStore::open(&log_path("c"), group, 2).unwrap();
    c.bind_applied_floor(machine.shared_store()).unwrap();
    let state = c.get_log_state().await.unwrap();
    assert_eq!(state.last_purged_log_id, Some(second));
    assert_eq!(state.last_log_id, Some(after_second));
    assert_eq!(c.entries().unwrap().len(), 2);
    assert_eq!(c.deferred_purge().unwrap(), None);
}

/// A purge target below the purge point is a state the log cannot
/// explain, and refuses by name; and a log whose entries start after a gap
/// past its purge point refuses at the next settle whether or not a purge
/// is pending.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_purge_below_the_purge_point_and_a_gap_after_it_refuse() {
    let dir = Directory::new("purge-point");
    let group = identity(7);
    let path = dir.0.join("raft-log.redb");
    let mut log = RaftLogStore::create(&path, &group, 2).unwrap();
    log.append_sync(blanks(0, 6)).unwrap();
    log.purge_sync(log_id(1, 1, 4)).unwrap();
    let below = log.purge(log_id(1, 1, 2)).await.err().unwrap();
    assert!(
        below.to_string().contains("below the purge point 4"),
        "{below}"
    );
    assert_eq!(
        log.get_log_state().await.unwrap().last_purged_log_id,
        Some(log_id(1, 1, 4))
    );
    drop(log);

    // A gap: entry 5 removed from under the log, entry 6 kept. The store
    // bound to it is at 6, past the gap.
    {
        let database = redb::Database::open(&path).unwrap();
        let tx = database.begin_write().unwrap();
        {
            let mut table = tx
                .open_table(redb::TableDefinition::<u64, &[u8]>::new("raft_log_entries"))
                .unwrap();
            table.remove(&5).unwrap();
        }
        tx.commit().unwrap();
    }
    let store = crate::source_authority::SourceAuthorityStore::create(
        &dir.0.join(crate::raft::host::STORE_FILE),
        &group,
        &policy(),
        &limits(),
    )
    .unwrap();
    let mut machine = ControlStateMachine::new(
        store,
        &dir.0.join(crate::raft::host::SNAPSHOT_DIR),
        64 << 20,
    )
    .unwrap();
    machine.apply(blanks(0, 6)).await.unwrap();
    let mut log = RaftLogStore::open(&path, &group, 2).unwrap();
    let gap = log
        .bind_applied_floor(machine.shared_store())
        .err()
        .unwrap();
    assert!(
        gap.to_string()
            .contains("holds entries from index 6 after a purge at 4"),
        "{gap}"
    );
}

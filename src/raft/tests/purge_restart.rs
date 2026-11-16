use super::*;
use std::path::Path;

/// A record the store has not reached when the log is reopened, and a
/// run in which the library asks for less than the record: the log keeps
/// the record and purges only what the library asked in this run. The
/// library records a purge as done before it asks for it, so a purge past
/// what it asked would remove entries it still reads; a record from an
/// earlier run completes at the next start, or when the library asks for
/// a purge at or past it. A second purge with a smaller target while the
/// record is pending leaves the record as it is.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_purge_deferred_across_a_restart_completes_only_where_the_library_requested_it() {
    // Two proposals before the second snapshot: a position strictly
    // between the two snapshots is needed below.
    let (group, mut snapshots) = snapshots_after(
        "deferred-purge-restart",
        &[
            &[("phone", "p1", 1)],
            &[("tablet", "p2", 2), ("laptop", "p3", 3)],
        ],
    )
    .await;
    let first_snapshot = snapshots.remove(0);
    let second_snapshot = snapshots.remove(0);
    let (first, second) = (first_snapshot.last, second_snapshot.last);
    assert!(
        second.index > first.index + 1,
        "no position between the two snapshots: {first} and {second}"
    );

    let replica_dir = Directory::new("deferred-purge-restart-replica");
    let mut machine = replica_machine(&replica_dir, &group);
    let log_path = replica_dir.0.join("raft-log-d.redb");
    let mut file = machine.begin_receiving_snapshot().await.unwrap();
    file.write_all(&first_snapshot.image).await.unwrap();
    machine
        .install_snapshot(&first_snapshot.meta, file)
        .await
        .unwrap();
    run_one_records_the_cut_purge(&log_path, &group, &machine, first, second).await;
    run_two_keeps_the_record_until_asked(&log_path, &group, &mut machine, first, &second_snapshot)
        .await;
    run_three_completes_at_the_start(&replica_dir, &group, &mut machine, second).await;
}

/// Run one: the store is at the first snapshot; a purge to the second is
/// cut at the first and recorded.
async fn run_one_records_the_cut_purge(
    log_path: &Path,
    group: &SourceAuthorityIdentity,
    machine: &ControlStateMachine,
    first: LogId<u64>,
    second: LogId<u64>,
) {
    let mut d = RaftLogStore::create(log_path, group, 2).unwrap();
    d.bind_applied_floor(machine.shared_store()).unwrap();
    d.append_sync(blanks(first.index + 1, second.index + 1))
        .unwrap();
    d.purge(second).await.unwrap();
    assert_eq!(d.deferred_purge().unwrap(), Some(second));
    assert_eq!(
        d.get_log_state().await.unwrap().last_purged_log_id,
        Some(first)
    );
    // A smaller target while the record is pending: the record stays.
    let between = LogId::new(second.leader_id, second.index - 1);
    d.purge(between).await.unwrap();
    assert_eq!(d.deferred_purge().unwrap(), Some(second));
    assert_eq!(
        d.get_log_state().await.unwrap().last_purged_log_id,
        Some(first)
    );
    drop(d);
}

/// Run two: the store is still at the first; the start keeps the record
/// and the log, and an append in a run that asked for no purge keeps
/// them too, even once the store is past the record.
async fn run_two_keeps_the_record_until_asked(
    log_path: &Path,
    group: &SourceAuthorityIdentity,
    machine: &mut ControlStateMachine,
    first: LogId<u64>,
    snapshot: &Published,
) {
    let (second_image, second_meta, second) = (&snapshot.image, &snapshot.meta, snapshot.last);
    let between = LogId::new(second.leader_id, second.index - 1);
    let mut d = RaftLogStore::open(log_path, group, 2).unwrap();
    d.bind_applied_floor(machine.shared_store()).unwrap();
    assert_eq!(d.deferred_purge().unwrap(), Some(second));
    let state = d.get_log_state().await.unwrap();
    assert_eq!(state.last_purged_log_id, Some(first));
    assert_eq!(state.last_log_id, Some(log_id(1, 1, second.index + 1)));
    let mut file = machine.begin_receiving_snapshot().await.unwrap();
    file.write_all(second_image).await.unwrap();
    machine.install_snapshot(second_meta, file).await.unwrap();
    assert_eq!(machine.applied_state().await.unwrap().0, Some(second));
    d.append_sync(blanks(second.index + 2, second.index + 3))
        .unwrap();
    assert_eq!(d.deferred_purge().unwrap(), Some(second));
    let state = d.get_log_state().await.unwrap();
    assert_eq!(state.last_purged_log_id, Some(first));
    assert_eq!(state.last_log_id, Some(log_id(1, 1, second.index + 3)));
    assert_eq!(
        d.entries().unwrap().len() as u64,
        second.index + 3 - first.index
    );
    // The library asks for a purge below the record: the log purges to it
    // (the store is past it) and the record stays above.
    d.purge(between).await.unwrap();
    assert_eq!(d.deferred_purge().unwrap(), Some(second));
    assert_eq!(
        d.get_log_state().await.unwrap().last_purged_log_id,
        Some(between)
    );
    // At the next append the record completes: the library asked in this
    // run for a position it is below, so the completion to the record is
    // still past what it asked, and it waits.
    d.append_sync(blanks(second.index + 4, second.index + 4))
        .unwrap();
    assert_eq!(d.deferred_purge().unwrap(), Some(second));
    assert_eq!(
        d.get_log_state().await.unwrap().last_purged_log_id,
        Some(between)
    );
    // The library asks at the record: completed in full and cleared.
    d.purge(second).await.unwrap();
    assert_eq!(d.deferred_purge().unwrap(), None);
    let state = d.get_log_state().await.unwrap();
    assert_eq!(state.last_purged_log_id, Some(second));
    assert_eq!(state.last_log_id, Some(log_id(1, 1, second.index + 4)));
    assert_eq!(d.entries().unwrap().len(), 4);
    drop(d);
}

/// Run three, the start path: a record the store is past at the start
/// completes there, before the library reads the log state.
async fn run_three_completes_at_the_start(
    replica_dir: &Directory,
    group: &SourceAuthorityIdentity,
    machine: &mut ControlStateMachine,
    second: LogId<u64>,
) {
    let log_path = replica_dir.0.join("raft-log-e.redb");
    let mut e = RaftLogStore::create(&log_path, group, 2).unwrap();
    e.bind_applied_floor(machine.shared_store()).unwrap();
    e.append_sync(blanks(second.index + 1, second.index + 2))
        .unwrap();
    let past = LogId::new(second.leader_id, second.index + 2);
    // The store is at the second: the purge is cut there and recorded.
    e.purge(past).await.unwrap();
    assert_eq!(e.deferred_purge().unwrap(), Some(past));
    assert_eq!(
        e.get_log_state().await.unwrap().last_purged_log_id,
        Some(second)
    );
    drop(e);
    // The store moves past the record between the runs (the two proposals
    // the log holds, applied through the machine).
    for entry in blanks(second.index + 1, second.index + 2) {
        machine.apply(vec![entry]).await.unwrap();
    }
    let mut e = RaftLogStore::open(&log_path, group, 2).unwrap();
    e.bind_applied_floor(machine.shared_store()).unwrap();
    assert_eq!(e.deferred_purge().unwrap(), None);
    let state = e.get_log_state().await.unwrap();
    assert_eq!(state.last_purged_log_id, Some(past));
    assert_eq!(state.last_log_id, Some(past));
    assert_eq!(e.entries().unwrap().len(), 0);
}

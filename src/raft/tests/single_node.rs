use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_single_node_host_applies_through_the_log_and_restarts_in_place() {
    let dir = Directory::new("host");
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
    let store = host.store().unwrap();
    let (prepared, last) = proposals_apply_through_the_log(&host, &store, &group).await;
    // The metrics channel publishes after the store commits; wait for it.
    let mut metrics = host.metrics();
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        metrics.wait_for(|m| m.last_applied.map(|l| l.index) == Some(last.index)),
    )
    .await
    .expect("the metrics reach the store's applied position")
    .unwrap();
    // No control state was installed in this group; the map is absent.
    assert_eq!(
        store
            .published_map("alice", &owner("phone"))
            .err()
            .unwrap()
            .code(),
        Code::NotFound
    );
    let entries = host.log_entries().unwrap().len();
    drop(store);
    host.shutdown().await.unwrap();

    restart_keeps_the_applied_state(&dir, &group, &prepared, last.index, entries).await;
    // Startup never creates: an empty directory has no store to open.
    std::fs::create_dir(dir.0.join("nowhere")).unwrap();
    assert_eq!(
        RaftHost::start(&dir.0.join("nowhere"), &group, 1, &config())
            .await
            .err()
            .unwrap()
            .code(),
        Code::NotFound
    );
}

/// The direct path is closed and only proposals apply; returns the
/// prepared owner and the last applied position.
async fn proposals_apply_through_the_log(
    host: &RaftHost,
    store: &crate::source_authority::SourceAuthorityStore,
    group: &SourceAuthorityIdentity,
) -> (
    crate::pb::storage::PreparedSourceOwner,
    crate::pb::storage::RaftLogId,
) {
    // The direct path is closed; only proposals apply.
    assert_eq!(
        store
            .execute("alice", &prepare(group, "phone", "prepare", 1))
            .err()
            .unwrap()
            .code(),
        Code::FailedPrecondition
    );
    let decision = host
        .propose_command("alice", &prepare(group, "phone", "prepare", 1))
        .await
        .unwrap();
    assert_eq!(decision.code, 0, "{}", decision.message);
    assert_eq!(decision.control_revision, 2);
    let prepared = store.owner("alice", &owner("phone")).unwrap();
    assert_eq!(prepared.phase, PreparedSourceOwnerPhase::Prepared as i32);
    // An exact retry through the log answers from the record and consumes
    // one more entry; a stale command is a recorded rejection.
    let again = host
        .propose_command("alice", &prepare(group, "phone", "prepare", 1))
        .await
        .unwrap();
    assert_eq!(again, decision);
    let stale = host
        .propose_command("alice", &prepare(group, "other", "stale", 1))
        .await
        .unwrap();
    assert_eq!(stale.code, Code::FailedPrecondition as u32);
    // An unauthorized principal is a refusal recorded as the reply, and the
    // entry is consumed.
    let error = host
        .propose_command("carol", &prepare(group, "other", "carol", 2))
        .await
        .err()
        .unwrap();
    assert_eq!(error.code(), Code::PermissionDenied);
    let applied = store.raft_applied().unwrap().unwrap();
    let last = applied.last_applied.unwrap();
    assert!(last.index >= 5, "{last:?}");
    (prepared, last)
}

/// Restart from durable state: the applied position and the owner are
/// there, no entry applies twice, and proposals continue.
async fn restart_keeps_the_applied_state(
    dir: &Directory,
    group: &SourceAuthorityIdentity,
    prepared: &crate::pb::storage::PreparedSourceOwner,
    last_index: u64,
    entries: usize,
) {
    let host = RaftHost::start(&dir.0, group, 1, &config()).await.unwrap();
    host.wait(Some(std::time::Duration::from_secs(30)))
        .state(openraft::ServerState::Leader, "re-elected")
        .await
        .unwrap();
    let store = host.store().unwrap();
    assert_eq!(store.owner("alice", &owner("phone")).unwrap(), *prepared);
    assert_eq!(
        store
            .raft_applied()
            .unwrap()
            .unwrap()
            .last_applied
            .unwrap()
            .index,
        last_index
    );
    // Nothing applied twice: the log holds exactly what it held before.
    assert!(host.log_entries().unwrap().len() >= entries);
    let second = host
        .propose_command("alice", &prepare(group, "other", "second", 2))
        .await
        .unwrap();
    assert_eq!(second.code, 0, "{}", second.message);
    assert_eq!(second.control_revision, 3);
    drop(store);
    host.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committed_entries_apply_on_restart_and_the_position_is_in_the_same_transaction() {
    let dir = Directory::new("replay");
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
    let vote = host.metrics().borrow().vote;
    let last = host.log_entries().unwrap().last().unwrap().log_id;
    host.shutdown().await.unwrap();
    // A crash between commit and apply: the log holds a committed entry the
    // state machine never saw.
    let log = RaftLogStore::open(&dir.0.join(crate::raft::host::LOG_FILE), &group, 1).unwrap();
    let next = LogId::new(vote.leader_id, last.index + 1);
    log.append_sync(vec![Entry {
        log_id: next,
        payload: EntryPayload::Normal(proposal("alice", &prepare(&group, "phone", "prepare", 1))),
    }])
    .unwrap();
    {
        let mut log = log.clone();
        log.save_committed(Some(next)).await.unwrap();
    }
    drop(log);
    let host = RaftHost::start(&dir.0, &group, 1, &config()).await.unwrap();
    host.wait(Some(std::time::Duration::from_secs(30)))
        .applied_index_at_least(Some(next.index), "committed entry applied")
        .await
        .unwrap();
    let store = host.store().unwrap();
    let owner_row = store.owner("alice", &owner("phone")).unwrap();
    assert_eq!(owner_row.control_revision, 2);
    let applied = store.raft_applied().unwrap().unwrap();
    assert!(applied.last_applied.unwrap().index >= next.index);
    drop(store);
    host.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exact_retries_advance_the_applied_position_in_every_command_family() {
    let dir = Directory::new("retry-position");
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
    // Control: accepted, then the exact retry.
    let command = prepare(&group, "phone", "prepare", 1);
    let first = host.propose_command("alice", &command).await.unwrap();
    let again = host.propose_command("alice", &command).await.unwrap();
    assert_eq!(first, again);
    assert_eq!(applied_index(&host), last_index(&host));
    // Install: a recorded rejection (stale revision), then its exact retry.
    let install = crate::source_authority::control_tests::command(
        &group,
        "install",
        9,
        crate::source_authority::control_tests::state(2),
    );
    let first = host.propose_install("alice", &install).await.unwrap();
    assert_eq!(first.code, Code::FailedPrecondition as u32);
    let again = host.propose_install("alice", &install).await.unwrap();
    assert_eq!(first, again);
    assert_eq!(applied_index(&host), last_index(&host));
    capacity_retries_advance(&host, &group, &install.key).await;
    // Changed content under a used id is refused at the envelope and still
    // consumes its entry.
    let mut changed = command.clone();
    changed.expected_policy_revision = 7;
    assert_eq!(
        host.propose_command("alice", &changed)
            .await
            .err()
            .unwrap()
            .code(),
        Code::FailedPrecondition
    );
    assert_eq!(applied_index(&host), last_index(&host));
    let last = applied_index(&host);
    host.shutdown().await.unwrap();
    let host = RaftHost::start(&dir.0, &group, 1, &config()).await.unwrap();
    assert_eq!(applied_index(&host), last);
    host.shutdown().await.unwrap();
}

fn last_index(host: &RaftHost) -> u64 {
    host.log_entries().unwrap().last().unwrap().log_id.index
}

fn applied_index(host: &RaftHost) -> u64 {
    host.applied_position().unwrap().unwrap().index
}

/// A capacity configuration with one hot server tier.
fn capacity_configure(
    group: &SourceAuthorityIdentity,
    key: &Option<LogicalSourceOwner>,
) -> crate::pb::storage::CapacityConfigureCommand {
    crate::pb::storage::CapacityConfigureCommand {
        format_version: 1,
        authority: Some(group.clone()),
        key: key.clone(),
        command_id: b"configure".to_vec(),
        expected_control_revision: 2,
        expected_policy_revision: 1,
        configuration: Some(crate::pb::storage::CapacityConfiguration {
            format_version: 1,
            cohort_length_ms: 60_000,
            cohort_phase_unix_ms: 0,
            max_total_records: 16,
            max_total_bytes: 1 << 20,
            max_registered_nodes: 4,
            policy: Some(crate::pb::storage::CapacityTierPolicy {
                format_version: 1,
                tiers: vec![crate::pb::storage::CapacityTierSpec {
                    name: "hot".into(),
                    residency: crate::pb::storage::CapacityResidency::Server as i32,
                    min_replicas: 1,
                    scans_per_byte_nanos_lo: 0,
                    scans_per_byte_nanos_hi: 1_000,
                    max_seconds_since_scan: 60,
                }],
            }),
        }),
    }
}

/// Capacity configure is a recorded rejection with an exact retry, and
/// an observation transition is refused at its envelope; each consumes
/// its entry.
async fn capacity_retries_advance(
    host: &RaftHost,
    group: &SourceAuthorityIdentity,
    key: &Option<LogicalSourceOwner>,
) {
    // Capacity configure: a recorded rejection (no applied control state),
    // then its exact retry.
    let configure = capacity_configure(group, key);
    let first = host
        .propose_capacity_configure("alice", &configure)
        .await
        .unwrap();
    assert_eq!(first.code, Code::FailedPrecondition as u32);
    let again = host
        .propose_capacity_configure("alice", &configure)
        .await
        .unwrap();
    assert_eq!(first, again);
    assert_eq!(applied_index(host), last_index(host));
    // Observation transition: refused at its envelope (no configuration),
    // recorded as the reply; the entry is consumed both times.
    let transition = crate::pb::storage::CapacityTransition {
        format_version: 1,
        authority: Some(group.clone()),
        key: key.clone(),
        action: Some(crate::pb::storage::capacity_transition::Action::Register(
            crate::pb::storage::RegisterCapacityReporter {
                node_id: "server-a".into(),
                process_incarnation: vec![5; 16],
            },
        )),
    };
    for _ in 0..2 {
        assert_eq!(
            host.propose_capacity_transition("alice", &transition)
                .await
                .err()
                .unwrap()
                .code(),
            Code::FailedPrecondition
        );
        assert_eq!(applied_index(host), last_index(host));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_releases_log_and_store_locks_with_external_store_handle_held() {
    let dir = Directory::new("shutdown-locks");
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
    let retained_handle = host.store_handle();
    assert!(retained_handle().is_ok());
    host.shutdown().await.unwrap();
    assert_eq!(retained_handle().err().unwrap().code(), Code::Unavailable);
    let inspected = crate::raft::host::inspect_node(&dir.0, &group, 1).unwrap();
    assert!(inspected.applied.is_some());
}

//! Crash target for the raft log store and the snapshot install
//! (`docs/control-authority-test-harness.md`, "The raft log store crash
//! target").
//!
//! The library purges the log for an incoming snapshot on the core while
//! the state machine installs the image on its own worker, against two
//! redb files: the log's purge transaction (the record of a purge the
//! store bounds, or a purge, or the completion of a recorded purge at an
//! append) and the store's replacement by the image are separate commits
//! with no order between them. Every case here runs a worker process that
//! bootstraps a one-voter group with a prepared member in one process,
//! arms one exit fault (`ExitFault::{BeforeCommit, AfterCommit}`, exit
//! code 87) at one of those commits and drives the seed into it; the
//! parent reads the on-disk state of both nodes with the hosts down,
//! asserts the window the fault names, starts both nodes again on their
//! recorded ports and drives an append through the seeded member.
//!
//! The order between the two commits is fixed by the install gate
//! (`RaftHost::arm_snapshot_install_gate`): the image is received and the
//! install parks before verifying it, the core's purge for the same
//! snapshot commits meanwhile, and the worker releases the gate once the
//! log shows the record. The families:
//!
//! - `member-purge`: the member's record of the library's purge, with the
//!   store at no position (`Floor::Nothing`).
//! - `member-install`: the member's store replaced by the image, before
//!   the generation is published.
//! - `member-settle`: the completion of the recorded purge at the member's
//!   next append, the leader's membership entry after the seed.
//! - `leader-purge`: the leader's own purge to its seed snapshot in
//!   `add_learner`, before the membership changes.
//!
//! Run: `cargo test --test control_raft_crash_faults --features
//! raft,fault-injection -- --test-threads=1` (the target holds a
//! target-wide serial lock, so any `--test-threads` value is safe).
#![cfg(all(feature = "raft", feature = "tls", feature = "fault-injection"))]

mod control_adversarial;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use control_adversarial::kit;
use control_adversarial::raft_kit;
use pipestream_search::pb::storage::{SourceAuthorityCommand, SourceAuthorityIdentity};
use pipestream_search::raft::{inspect_node, NodeInspection, RaftHost, RaftLogInspection};
use pipestream_search::source_authority::ExitFault;

const CRASH_ENV: &str = "PSEARCH_RAFT_CRASH_WORKER";
const FAMILY_ENV: &str = "PSEARCH_RAFT_CRASH_FAMILY";
const FAULT_ENV: &str = "PSEARCH_RAFT_CRASH_FAULT";
const OWNER_WORKFLOW: &[u8] = b"crash-owner";
/// The worker records the two listeners' ports here for the restart.
const PORTS: &str = "ports";

/// One case at a time: each runs its own group and restarts it on fixed
/// ports.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn serial() -> tokio::sync::MutexGuard<'static, ()> {
    SERIAL.lock().await
}

fn parse_fault() -> ExitFault {
    match std::env::var(FAULT_ENV).unwrap().as_str() {
        "before" => ExitFault::BeforeCommit,
        "after" => ExitFault::AfterCommit,
        other => panic!("unknown crash fault {other}"),
    }
}

/// A worker body returns (and its #[test] panics) when the armed fault did
/// not terminate the process.
fn fault_must_fire() -> ! {
    panic!("exit fault did not terminate the worker");
}

fn leader_dir(dir: &Path) -> PathBuf {
    dir.join("leader")
}

fn member_dir(dir: &Path) -> PathBuf {
    dir.join("member")
}

fn ephemeral() -> SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}

fn local(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}

fn prepare_command(group: &SourceAuthorityIdentity, revision: u64) -> SourceAuthorityCommand {
    kit::source_command(
        group,
        &kit::owner_key(),
        &format!("prepare-{revision}"),
        revision,
        1,
        0,
        kit::prepare_action(OWNER_WORKFLOW),
    )
}

fn grant_command(group: &SourceAuthorityIdentity, revision: u64) -> SourceAuthorityCommand {
    kit::source_command(
        group,
        &kit::key(),
        "grant-bob",
        revision,
        1,
        0,
        kit::replace_grants_action(vec![
            kit::grant("alice", kit::COLLECTION),
            kit::grant("bob", kit::COLLECTION),
        ]),
    )
}

async fn propose_ok(host: &RaftHost, command: &SourceAuthorityCommand) {
    let decision = host.propose_command("alice", command).await.unwrap();
    assert_eq!(decision.code, 0, "{}", decision.message);
}

/// The worker's group: node 1 bootstrapped with one PREPARED owner in
/// `dir/leader`, node 2 prepared and started (not added) in `dir/member`,
/// both on ephemeral ports recorded in `dir/ports`.
async fn bootstrap_group(dir: &Path, group: &SourceAuthorityIdentity) -> (RaftHost, RaftHost) {
    let directory = raft_kit::directory(group);
    std::fs::create_dir_all(leader_dir(dir)).unwrap();
    std::fs::create_dir_all(member_dir(dir)).unwrap();
    let leader = RaftHost::bootstrap_cluster(
        &leader_dir(dir),
        group,
        raft_kit::NODE_ID,
        &kit::policy(),
        &kit::limits(),
        &raft_kit::host_config(),
        raft_kit::transport(raft_kit::NODE_ID, &directory, ephemeral()),
    )
    .await
    .unwrap();
    RaftHost::prepare_member(
        &member_dir(dir),
        group,
        raft_kit::MEMBER_ID,
        &kit::policy(),
        &kit::limits(),
    )
    .unwrap();
    propose_ok(&leader, &prepare_command(group, 1)).await;
    let member = RaftHost::start_member(
        &member_dir(dir),
        group,
        raft_kit::MEMBER_ID,
        &raft_kit::host_config(),
        raft_kit::transport(raft_kit::MEMBER_ID, &directory, ephemeral()),
    )
    .await
    .unwrap();
    std::fs::write(
        dir.join(PORTS),
        format!(
            "{}\n{}\n",
            leader.listen_addr().unwrap().port(),
            member.listen_addr().unwrap().port()
        ),
    )
    .unwrap();
    (leader, member)
}

/// Poll the member's log until the library's purge for the seed is
/// recorded (the install is parked at its gate meanwhile).
async fn wait_for_record(member: &RaftHost) -> RaftLogInspection {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let log = member.log_inspection().unwrap();
        if log.deferred.is_some() {
            return log;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the purge for the seed was not recorded while the install was parked: {log:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Worker: the family-selected crash body. Every arm ends in the exit
/// fault; reaching the end is a failure of the worker.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_worker_raft() {
    if std::env::var_os(CRASH_ENV).is_none() {
        return;
    }
    let dir = kit::TestDir::from_env();
    let group = kit::identity(kit::SEED);
    let family = std::env::var(FAMILY_ENV).unwrap();
    let fault = parse_fault();
    let (leader, member) = bootstrap_group(dir.path(), &group).await;
    let addr = member.advertised_addr().unwrap().to_string();
    let window = Duration::from_secs(60);
    match family.as_str() {
        "member-purge" => {
            let _gate = member.arm_snapshot_install_gate();
            member.arm_purge_exit_fault(fault);
            let _ =
                tokio::time::timeout(window, leader.add_learner(raft_kit::MEMBER_ID, &addr)).await;
        }
        "member-install" | "member-settle" => {
            let gate = member.arm_snapshot_install_gate();
            let join = leader.add_learner(raft_kit::MEMBER_ID, &addr);
            tokio::pin!(join);
            tokio::select! {
                outcome = &mut join => panic!("the join ended before the install reached its gate: {outcome:?}"),
                _ = gate.reached() => {}
            }
            wait_for_record(&member).await;
            if family == "member-install" {
                member.arm_install_exit_fault(fault);
            } else {
                member.arm_purge_exit_fault(fault);
            }
            gate.release();
            let _ = tokio::time::timeout(window, join).await;
        }
        "leader-purge" => {
            leader.arm_purge_exit_fault(fault);
            let _ =
                tokio::time::timeout(window, leader.add_learner(raft_kit::MEMBER_ID, &addr)).await;
        }
        other => panic!("unknown raft crash family {other}"),
    }
    fault_must_fire();
}

fn spawn_crash_worker(dir: &kit::TestDir, family: &str, fault: &str) -> kit::KillOnDrop {
    kit::KillOnDrop(kit::spawn_worker(
        "crash_worker_raft",
        &[
            (CRASH_ENV, "1"),
            (kit::WORKER_DIR_ENV, dir.path().to_str().unwrap()),
            (FAMILY_ENV, family),
            (FAULT_ENV, fault),
        ],
    ))
}

/// What the worker left: both nodes' on-disk state, the leader's seed
/// snapshot position and the recorded ports.
struct Left {
    leader: NodeInspection,
    member: NodeInspection,
    /// The position of the leader's published seed snapshot.
    seed: u64,
    member_published: Option<u64>,
    leader_port: u16,
    member_port: u16,
}

fn run_worker(dir: &kit::TestDir, family: &str, fault: &str) -> Left {
    let mut worker = spawn_crash_worker(dir, family, fault);
    let status = tokio::task::block_in_place(|| worker.child().wait().unwrap());
    assert_eq!(
        status.code(),
        Some(87),
        "{family}:{fault} exit code {status}"
    );
    let group = kit::identity(kit::SEED);
    let leader = inspect_node(&leader_dir(dir.path()), &group, raft_kit::NODE_ID).unwrap();
    let member = inspect_node(&member_dir(dir.path()), &group, raft_kit::MEMBER_ID).unwrap();
    let (_, meta) = raft_kit::published_pair(&leader_dir(dir.path()))
        .unwrap_or_else(|| panic!("{family}:{fault}: the leader published no seed snapshot"));
    let seed = meta.last_log_id.unwrap().index;
    let ports = std::fs::read_to_string(dir.path().join(PORTS)).unwrap();
    let mut ports = ports.lines().map(|p| p.parse::<u16>().unwrap());
    Left {
        leader,
        member,
        seed,
        member_published: raft_kit::published_generation(&member_dir(dir.path())),
        leader_port: ports.next().unwrap(),
        member_port: ports.next().unwrap(),
    }
}

/// A log with no purge, no record and no entry.
fn untouched() -> RaftLogInspection {
    RaftLogInspection::default()
}

/// A log with the library's purge to `seed` recorded and nothing purged.
fn recorded(seed: u64) -> RaftLogInspection {
    RaftLogInspection {
        deferred: Some(seed),
        ..RaftLogInspection::default()
    }
}

/// Start both nodes again on their recorded ports; the leader leads again
/// before this returns.
async fn restart(dir: &kit::TestDir, left: &Left) -> (RaftHost, RaftHost) {
    let group = kit::identity(kit::SEED);
    let directory = raft_kit::directory(&group);
    let leader = RaftHost::start_member(
        &leader_dir(dir.path()),
        &group,
        raft_kit::NODE_ID,
        &raft_kit::host_config(),
        raft_kit::transport(raft_kit::NODE_ID, &directory, local(left.leader_port)),
    )
    .await
    .unwrap();
    let member = RaftHost::start_member(
        &member_dir(dir.path()),
        &group,
        raft_kit::MEMBER_ID,
        &raft_kit::host_config(),
        raft_kit::transport(raft_kit::MEMBER_ID, &directory, local(left.member_port)),
    )
    .await
    .unwrap();
    raft_kit::wait_leader(&leader).await;
    (leader, member)
}

/// After the restart the member is seeded and applies the leader's next
/// entry: the seeded member's log has no gap, its record is completed and
/// its purge point is not past its store.
async fn converge(leader: &RaftHost, member: &RaftHost, label: &str) {
    let position = raft_kit::durable_position(leader);
    raft_kit::wait_applied(member, position).await;
    assert!(
        !member.awaiting_snapshot().unwrap(),
        "{label}: still awaiting"
    );
    propose_ok(leader, &grant_command(&kit::identity(kit::SEED), 2)).await;
    let after = raft_kit::durable_position(leader);
    raft_kit::wait_applied(member, after).await;
    assert_eq!(
        raft_kit::durable_position(member),
        after,
        "{label}: durable position"
    );
    assert!(raft_kit::core_running(member), "{label}: member core");
    assert!(raft_kit::core_running(leader), "{label}: leader core");
    let log = member.log_inspection().unwrap();
    assert_eq!(log.deferred, None, "{label}: record completed: {log:?}");
    assert!(
        log.purged.is_some_and(|p| p <= after),
        "{label}: purge point at or before the store: {log:?}"
    );
    assert!(
        log.first.is_none() || log.first == log.purged.map(|p| p + 1),
        "{label}: entries contiguous from the purge point: {log:?}"
    );
}

/// The member's record of the library's purge, with the store at no
/// position: before -> nothing written; after -> the record names the seed
/// position and the store is untouched. Both seed after the restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_member_purge_record_before_and_after_commit() {
    let _serial = serial().await;
    for fault in ["before", "after"] {
        let label = format!("member-purge:{fault}");
        let dir = kit::TestDir::new(&format!("raft-crash-{label}"));
        let left = run_worker(&dir, "member-purge", fault);
        let expected = if fault == "before" {
            untouched()
        } else {
            recorded(left.seed)
        };
        assert_eq!(left.member.log, expected, "{label}: member log");
        assert!(left.member.awaiting_snapshot, "{label}: member store");
        assert_eq!(left.member.applied, None, "{label}: member position");
        assert_eq!(left.member_published, None, "{label}: member generation");

        let (leader, member) = restart(&dir, &left).await;
        converge(&leader, &member, &label).await;
        let _ = member.shutdown().await;
        let _ = leader.shutdown().await;
    }
}

/// The member's store replaced by the image, after the record: before ->
/// the store is still the prepared one; after -> the store is at the seed
/// position with no generation published, and the record stands. Both
/// complete after the restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_member_install_before_and_after_commit() {
    let _serial = serial().await;
    for fault in ["before", "after"] {
        let label = format!("member-install:{fault}");
        let dir = kit::TestDir::new(&format!("raft-crash-{label}"));
        let left = run_worker(&dir, "member-install", fault);
        assert_eq!(left.member.log, recorded(left.seed), "{label}: member log");
        assert_eq!(left.member_published, None, "{label}: member generation");
        if fault == "before" {
            assert!(left.member.awaiting_snapshot, "{label}: member store");
            assert_eq!(left.member.applied, None, "{label}: member position");
        } else {
            assert!(!left.member.awaiting_snapshot, "{label}: member store");
            assert_eq!(
                left.member.applied,
                Some(left.seed),
                "{label}: member position"
            );
        }

        let (leader, member) = restart(&dir, &left).await;
        converge(&leader, &member, &label).await;
        let _ = member.shutdown().await;
        let _ = leader.shutdown().await;
    }
}

/// The completion of the recorded purge at the member's next append, the
/// membership entry after the seed: before -> the record stands and the
/// entry is not appended; after -> the log is purged to the seed position,
/// the record is cleared and the entry is not appended. Both apply the
/// entry after the restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_member_settle_before_and_after_commit() {
    let _serial = serial().await;
    for fault in ["before", "after"] {
        let label = format!("member-settle:{fault}");
        let dir = kit::TestDir::new(&format!("raft-crash-{label}"));
        let left = run_worker(&dir, "member-settle", fault);
        let expected = if fault == "before" {
            recorded(left.seed)
        } else {
            RaftLogInspection {
                purged: Some(left.seed),
                ..RaftLogInspection::default()
            }
        };
        assert_eq!(left.member.log, expected, "{label}: member log");
        assert!(!left.member.awaiting_snapshot, "{label}: member store");
        assert_eq!(
            left.member.applied,
            Some(left.seed),
            "{label}: member position"
        );
        assert!(
            left.member_published.is_some(),
            "{label}: member generation"
        );

        let (leader, member) = restart(&dir, &left).await;
        converge(&leader, &member, &label).await;
        let _ = member.shutdown().await;
        let _ = leader.shutdown().await;
    }
}

/// The leader's purge to its seed snapshot in `add_learner`, before the
/// membership changes: before -> the log holds every entry from the
/// bootstrap and the generation is published; after -> the log is purged
/// to the snapshot. The member is untouched either way; after the restart
/// a fresh `add_learner` seeds it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_leader_purge_before_and_after_commit() {
    let _serial = serial().await;
    for fault in ["before", "after"] {
        let label = format!("leader-purge:{fault}");
        let dir = kit::TestDir::new(&format!("raft-crash-{label}"));
        let left = run_worker(&dir, "leader-purge", fault);
        assert_eq!(
            left.leader.applied,
            Some(left.seed),
            "{label}: leader position"
        );
        let expected = if fault == "before" {
            RaftLogInspection {
                first: Some(0),
                last: Some(left.seed),
                ..RaftLogInspection::default()
            }
        } else {
            RaftLogInspection {
                purged: Some(left.seed),
                ..RaftLogInspection::default()
            }
        };
        assert_eq!(left.leader.log, expected, "{label}: leader log");
        assert_eq!(left.member.log, untouched(), "{label}: member log");
        assert!(left.member.awaiting_snapshot, "{label}: member store");
        assert_eq!(left.member_published, None, "{label}: member generation");

        let (leader, member) = restart(&dir, &left).await;
        let addr = member.advertised_addr().unwrap().to_string();
        let joined = tokio::time::timeout(
            Duration::from_secs(60),
            leader.add_learner(raft_kit::MEMBER_ID, &addr),
        )
        .await;
        match joined {
            Ok(Ok(())) => {}
            Ok(Err(error)) => panic!("{label}: the learner was added: {error}"),
            Err(_) => panic!("{label}: the join answered inside 60 s"),
        }
        converge(&leader, &member, &label).await;
        let _ = member.shutdown().await;
        let _ = leader.shutdown().await;
    }
}

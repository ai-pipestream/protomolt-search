//! Snapshot-protocol regression target (slice 3d), re-expressed on the supported
//! surface after the R1 repair.
//!
//! Independent reproductions of Astra's checkpoint review findings R3
//! (snapshot generation publication) and R5 (receive identity, bounded
//! buffering). The original reproductions drove a standalone
//! `ControlStateMachine`; its constructor is crate-private since 1f4ea13
//! (`compile_fail` doctests on `pipestream_search::raft`), so every install
//! here arrives the way a receiver sees it in production: over the tonic
//! transport, from the leader (`add_learner`) or from a raw registered peer
//! (`raft_kit::install_chunks`, node 3's certificate — the stronger
//! adversary). Generations are read from the documented layout.
//!
//! Two library facts shape the fixtures. The library drops a snapshot whose
//! position is not newer than the receiver's applied position before the
//! receiver validates anything, so crafted images name a newer index and
//! are pushed at a DETACHED replica (seeded by a raw push, never added to
//! the group, so replication cannot move it). And the library treats a
//! refused install as a fatal storage error and stops the receiving core;
//! each test records that observation and restarts the member where a
//! later step needs it. The safety assertions (nothing replaced, the
//! previous generation intact, the store serving, restart recovering) never
//! depend on it.
//!
//! Run: `cargo test --test control_raft_snapshots --features
//! raft,fault-injection -- --test-threads=1` (the target holds a
//! target-wide serial lock, so any `--test-threads` value is safe).
#![cfg(all(feature = "raft", feature = "tls"))]
// Every test holds the target-wide `serial()` guard for its whole body,
// across awaits, on purpose.
#![allow(clippy::await_holding_lock)]

mod control_adversarial;

use std::io::{Read, Seek, SeekFrom};
use std::sync::atomic::{AtomicUsize, Ordering};

use control_adversarial::kit;
use control_adversarial::raft_kit::{self, Cluster};
use pipestream_search::pb::storage::{
    PreparedSourceOwnerPhase, RaftSnapshotMeta, SourceAuthorityCommand, SourceAuthorityIdentity,
};
use pipestream_search::raft::{RaftHost, SnapshotRejection};
use prost::Message;
use tonic::Code;

//
// A global allocator sees every allocation in the test binary, so the
// measurement takes the growth attributable to one install as peak-after
// minus the watermark before it, inside the target-wide serial section.

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

struct Tracking;

unsafe impl std::alloc::GlobalAlloc for Tracking {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        let ptr = std::alloc::System.alloc(layout);
        if !ptr.is_null() {
            let live = LIVE.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK.fetch_max(live, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: std::alloc::Layout) {
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        std::alloc::System.dealloc(ptr, layout);
    }
}

#[global_allocator]
static ALLOC: Tracking = Tracking;

/// Target-wide serialization: the allocation watermark above is process
/// global, and every test holds this lock for its whole body so the
/// measurement window can never overlap another test's allocations. The
/// lock is re-acquired across poisoning; mutual exclusion is what matters.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn live_bytes() -> usize {
    LIVE.load(Ordering::Relaxed)
}

fn peak_bytes() -> usize {
    PEAK.load(Ordering::Relaxed)
}

const OWNER_WORKFLOW: &[u8] = b"installation-one";

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

/// A grant change at `revision` (policy revision `policy`), the committed
/// change that moves the leader past its previous snapshot.
fn grant_command(
    group: &SourceAuthorityIdentity,
    revision: u64,
    policy: u64,
    principal: &str,
) -> SourceAuthorityCommand {
    kit::source_command(
        group,
        &kit::key(),
        &format!("grant-{principal}"),
        revision,
        policy,
        0,
        kit::replace_grants_action(vec![
            kit::grant("alice", kit::COLLECTION),
            kit::grant(principal, kit::COLLECTION),
        ]),
    )
}

async fn propose_ok(host: &RaftHost, command: &SourceAuthorityCommand) {
    let decision = host.propose_command("alice", command).await.unwrap();
    assert_eq!(decision.code, 0, "{}", decision.message);
}

/// The R3/R5 fixture: the leader has one PREPARED owner and a snapshot at
/// that position (the "previous generation"), the member is a detached
/// replica seeded from it by a raw push, and the leader has since committed
/// one grant change and published a newer generation. Returns the newer
/// image and meta, which name an index the member does not hold.
async fn detached(name: &str) -> (Cluster, Vec<u8>, RaftSnapshotMeta) {
    let group = kit::identity(kit::SEED);
    let mut cluster = Cluster::bootstrap(name, &group, &kit::policy(), &kit::limits()).await;
    propose_ok(cluster.leader(), &prepare_command(&group, 1)).await;
    raft_kit::snapshot_image(&cluster.leader_dir, cluster.leader()).await;
    cluster.seed_detached().await;
    assert!(raft_kit::published_generation(cluster.member_dir.path()).is_some());
    propose_ok(cluster.leader(), &grant_command(&group, 2, 1, "bob")).await;
    let (image, meta) = raft_kit::snapshot_image(&cluster.leader_dir, cluster.leader()).await;
    assert!(meta.last_log_id.unwrap().index > raft_kit::durable_position(cluster.member()));
    (cluster, image, meta)
}

/// A one-voter group of ANOTHER identity with one PREPARED owner and a
/// published generation: the foreign image of the R3 reproductions.
async fn foreign_image(name: &str) -> (kit::TestDir, RaftHost, Vec<u8>, RaftSnapshotMeta) {
    let group = kit::identity(9);
    let dir = kit::TestDir::new(name);
    let host = RaftHost::bootstrap_cluster(
        dir.path(),
        &group,
        raft_kit::NODE_ID,
        &kit::policy(),
        &kit::limits(),
        &raft_kit::host_config(),
        raft_kit::transport(
            raft_kit::NODE_ID,
            &raft_kit::directory(&group),
            "127.0.0.1:0".parse().unwrap(),
        ),
    )
    .await
    .unwrap();
    propose_ok(&host, &prepare_command(&group, 1)).await;
    let (image, meta) = raft_kit::snapshot_image(&dir, &host).await;
    (dir, host, image, meta)
}

/// The member's published pair and the committed owner it serves.
fn member_state(cluster: &Cluster) -> ((Vec<u8>, RaftSnapshotMeta), i32) {
    let pair = raft_kit::published_pair(cluster.member_dir.path()).expect("a published generation");
    let owner = cluster
        .member()
        .store()
        .unwrap()
        .owner("alice", &kit::owner_key())
        .expect("the member serves the committed owner");
    (pair, owner.phase)
}

/// Invalid incoming data is refused by the transport's staging before the
/// library sees it: the receiving core keeps running. (Only a real local
/// storage failure inside the library's own install path is fatal; the
/// in-place swap holds no precondition a caller could fail.)
async fn recover_if_stopped(cluster: &mut Cluster, what: &str) -> bool {
    let running = raft_kit::core_running(cluster.member());
    assert!(
        running,
        "{what}: the member core stopped on invalid incoming data"
    );
    running
}

#[path = "control_raft_snapshots/concurrent_staging_and_verification.rs"]
mod concurrent_staging_and_verification;
#[path = "control_raft_snapshots/final_chunk_ownership.rs"]
mod final_chunk_ownership;
#[path = "control_raft_snapshots/leader_serving_and_peer_bounds.rs"]
mod leader_serving_and_peer_bounds;
#[path = "control_raft_snapshots/learner_joins_and_pending_members.rs"]
mod learner_joins_and_pending_members;
#[path = "control_raft_snapshots/publication_and_group_refusals.rs"]
mod publication_and_group_refusals;
#[path = "control_raft_snapshots/receive_bounds_and_hashing.rs"]
mod receive_bounds_and_hashing;
#[path = "control_raft_snapshots/transfer_admission_and_corruption.rs"]
mod transfer_admission_and_corruption;

//! Snapshot-protocol regression target (slice 3d;
//! `docs/control-authority-test-harness.md`), re-expressed on the supported
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

mod control_adversarial;

use std::io::{Read as _, Seek as _, SeekFrom};
use std::sync::atomic::{AtomicUsize, Ordering};

use control_adversarial::kit;
use control_adversarial::raft_kit::{self, Cluster};
use pipestream_search::pb::storage::{
    PreparedSourceOwnerPhase, RaftSnapshotMeta, SourceAuthorityCommand, SourceAuthorityIdentity,
};
use pipestream_search::raft::RaftHost;
use prost::Message;
use tonic::Code;

// ---- peak live-allocation tracker (R5 bounded-buffer measurement) ----------
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

/// R5: the install must consume the actual receive, not the newest
/// incoming-* name. An abandoned directory with a deliberately later name
/// must not be selected; it is left for the startup sweep.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r5_install_uses_the_actual_receive_not_the_newest_filename() {
    let _serial = serial();
    let group = kit::identity(kit::SEED);
    let mut cluster = Cluster::bootstrap("r5-newest", &group, &kit::policy(), &kit::limits()).await;
    propose_ok(cluster.leader(), &prepare_command(&group, 1)).await;
    let (image, meta) = raft_kit::snapshot_image(&cluster.leader_dir, cluster.leader()).await;
    cluster.start_member().await;
    // Plant an abandoned receive whose name sorts AFTER any real one.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    let planted = raft_kit::snapshots_dir(cluster.member_dir.path())
        .join(format!("incoming-{}-99", now + 1_000_000_000_000_000));
    std::fs::create_dir(&planted).unwrap();
    std::fs::write(planted.join("image.redb"), vec![0xABu8; 4096]).unwrap();

    cluster.push(&meta, &image).await.unwrap_or_else(|error| {
        panic!(
            "R5 REQUIRED the install to consume the actual receive; the newest-name \
             heuristic selected the abandoned file instead: {error}"
        )
    });
    assert!(!cluster.member().awaiting_snapshot().unwrap());
    assert_eq!(
        raft_kit::durable_position(cluster.member()),
        meta.last_log_id.unwrap().index,
        "the installed applied position is not the received image's"
    );
    assert!(
        planted.exists(),
        "the abandoned receive disappeared without its own receive or a restart"
    );
    assert_eq!(
        raft_kit::incoming_dirs(cluster.member_dir.path()),
        vec![planted.clone()]
    );
    // The startup sweep removes it.
    cluster.restart_member().await;
    assert!(raft_kit::incoming_dirs(cluster.member_dir.path()).is_empty());
    assert_eq!(
        cluster
            .member()
            .store()
            .unwrap()
            .owner("alice", &kit::owner_key())
            .unwrap()
            .phase,
        PreparedSourceOwnerPhase::Prepared as i32
    );
    cluster.shutdown().await;
}

/// R5: interrupted receives never participate in a later install. (i) a
/// receive abandoned half way is dropped with its bytes when a receive of
/// another id begins; (ii) a leftover directory with a deliberately LATER
/// name is not selected by a complete receive.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r5_interrupted_receive_leaves_no_selected_artifact() {
    let _serial = serial();
    let group = kit::identity(kit::SEED);
    let mut cluster =
        Cluster::bootstrap("r5-partial", &group, &kit::policy(), &kit::limits()).await;
    propose_ok(cluster.leader(), &prepare_command(&group, 1)).await;
    let (image_1, meta_1) = raft_kit::snapshot_image(&cluster.leader_dir, cluster.leader()).await;
    propose_ok(cluster.leader(), &grant_command(&group, 2, 1, "bob")).await;
    let (image_2, meta_2) = raft_kit::snapshot_image(&cluster.leader_dir, cluster.leader()).await;
    assert_ne!(meta_1.snapshot_id, meta_2.snapshot_id);
    cluster.start_member().await;

    // (i) half of image 1, abandoned; then image 2 in full.
    let mut peer =
        raft_kit::peer_client(cluster.member().listen_addr().unwrap(), raft_kit::PEER_ID).await;
    let vote = raft_kit::current_vote(cluster.leader());
    let half = &image_1[..image_1.len() / 2];
    for (offset, chunk) in half.chunks(raft_kit::CHUNK_BYTES).enumerate() {
        peer.install_snapshot(raft_kit::timed(raft_kit::chunk_request(
            &group,
            raft_kit::PEER_ID,
            raft_kit::MEMBER_ID,
            &vote,
            &meta_1,
            offset * raft_kit::CHUNK_BYTES,
            chunk,
            false,
        )))
        .await
        .unwrap();
    }
    assert_eq!(
        raft_kit::incoming_dirs(cluster.member_dir.path()).len(),
        1,
        "the abandoned receive is on disk until superseded"
    );
    raft_kit::install_chunks(
        &mut peer,
        &group,
        raft_kit::PEER_ID,
        raft_kit::MEMBER_ID,
        &vote,
        &meta_2,
        &image_2,
    )
    .await
    .expect("(i) an abandoned partial broke a later install");
    assert!(
        raft_kit::incoming_dirs(cluster.member_dir.path()).is_empty(),
        "(i) the abandoned receive outlived the install that superseded it"
    );
    assert_eq!(
        raft_kit::durable_position(cluster.member()),
        meta_2.last_log_id.unwrap().index
    );

    // (ii) a leftover directory with a LATER name, then a complete receive.
    cluster.restart_member().await;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    let planted = raft_kit::snapshots_dir(cluster.member_dir.path())
        .join(format!("incoming-{}-7", now + 1_000_000_000_000_000));
    std::fs::create_dir(&planted).unwrap();
    std::fs::write(planted.join("image.redb"), vec![0xCDu8; 8192]).unwrap();
    // A third generation so the id is new to this member.
    propose_ok(cluster.leader(), &grant_command(&group, 3, 2, "carol")).await;
    let (image_3, meta_3) = raft_kit::snapshot_image(&cluster.leader_dir, cluster.leader()).await;
    cluster
        .push(&meta_3, &image_3)
        .await
        .unwrap_or_else(|error| {
            panic!(
                "R5 REQUIRED a later valid receive to win over an abandoned artifact; \
             the newest-name heuristic selected the planted directory instead: {error}"
            )
        });
    assert!(
        planted.exists(),
        "(ii) the planted directory is not this receive's"
    );
    assert_eq!(
        raft_kit::durable_position(cluster.member()),
        meta_3.last_log_id.unwrap().index
    );
    cluster.shutdown().await;
}

/// R5: announced lengths and digests that do not match the received bytes
/// refuse before anything is replaced, leaving the previous generation
/// intact. (The bounded-allocation half of R5 is a separate test.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r5_oversize_and_truncated_images_refuse_before_replacement() {
    let _serial = serial();
    let (mut cluster, image, newer) = detached("r5-lengths").await;
    let (before, _) = member_state(&cluster);
    let index = newer.last_log_id.unwrap().index;

    // (i) an announced length larger than the received image.
    let mut announced = raft_kit::meta_pb_for(&cluster.group, index, &image, &[1]);
    announced.length = image.len() as u64 + 100;
    announced.snapshot_id = format!(
        "{}-{}-{}",
        index,
        image.len() + 100,
        pipestream_search::sha256::to_hex(&pipestream_search::sha256::digest(&image))
    );
    let error = match cluster.push(&announced, &image).await {
        Ok(()) => panic!("an oversize announcement must refuse, installed instead"),
        Err(error) => error,
    };
    assert!(
        error.message().contains("length"),
        "the refusal must name the length: {error}"
    );
    recover_if_stopped(&mut cluster, "r5 oversize").await;
    assert_eq!(
        member_state(&cluster).0 .1.snapshot_id,
        before.1.snapshot_id
    );

    // (ii) a truncated receive against a full-image announcement.
    let error = match cluster.push(&newer, &image[..image.len() / 2]).await {
        Ok(()) => panic!("a truncated image must refuse, installed instead"),
        Err(error) => error,
    };
    assert!(
        error.message().contains("length"),
        "the refusal must name the length: {error}"
    );
    recover_if_stopped(&mut cluster, "r5 truncated").await;
    let (after, phase) = member_state(&cluster);
    assert_eq!(after.1.snapshot_id, before.1.snapshot_id);
    assert_eq!(after.0, before.0);
    assert_eq!(phase, PreparedSourceOwnerPhase::Prepared as i32);
    // The genuine newer generation installs afterwards.
    cluster.push(&newer, &image).await.unwrap();
    assert_eq!(raft_kit::durable_position(cluster.member()), index);
    cluster.shutdown().await;
}

/// R5: the announced length is enforced WHILE receiving, before the library
/// sees the chunk (the gap the original reproduction documented). A chunk
/// past the announced length is refused by name; the receive stays open,
/// and the correct final chunk completes it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r5_announced_length_is_enforced_while_receiving() {
    let _serial = serial();
    let group = kit::identity(kit::SEED);
    let mut cluster = Cluster::bootstrap("r5-bound", &group, &kit::policy(), &kit::limits()).await;
    propose_ok(cluster.leader(), &prepare_command(&group, 1)).await;
    let (image, meta) = raft_kit::snapshot_image(&cluster.leader_dir, cluster.leader()).await;
    cluster.start_member().await;
    let mut peer =
        raft_kit::peer_client(cluster.member().listen_addr().unwrap(), raft_kit::PEER_ID).await;
    let vote = raft_kit::current_vote(cluster.leader());
    let mut offset = 0;
    for chunk in image.chunks(raft_kit::CHUNK_BYTES) {
        peer.install_snapshot(raft_kit::timed(raft_kit::chunk_request(
            &group,
            raft_kit::PEER_ID,
            raft_kit::MEMBER_ID,
            &vote,
            &meta,
            offset,
            chunk,
            false,
        )))
        .await
        .unwrap();
        offset += chunk.len();
    }
    // Excess bytes past the announcement: refused before the library.
    let error = peer
        .install_snapshot(raft_kit::timed(raft_kit::chunk_request(
            &group,
            raft_kit::PEER_ID,
            raft_kit::MEMBER_ID,
            &vote,
            &meta,
            offset,
            &vec![0u8; 1 << 20],
            true,
        )))
        .await
        .err()
        .expect("R5 REQUIRED the announced length to be enforced while receiving");
    assert_eq!(error.code(), Code::InvalidArgument, "{error}");
    assert!(error.message().contains("announced length"), "{error}");
    assert!(raft_kit::core_running(cluster.member()));
    // The receive is intact: the correct final chunk completes it.
    peer.install_snapshot(raft_kit::timed(raft_kit::chunk_request(
        &group,
        raft_kit::PEER_ID,
        raft_kit::MEMBER_ID,
        &vote,
        &meta,
        offset,
        &[],
        true,
    )))
    .await
    .unwrap();
    assert!(!cluster.member().awaiting_snapshot().unwrap());
    assert_eq!(
        raft_kit::durable_position(cluster.member()),
        meta.last_log_id.unwrap().index
    );
    cluster.shutdown().await;
}

/// R5: hashing must use bounded buffers, not a whole-file Vec. Seeds a
/// member from a multi-MiB image through the supported path (`add_learner`)
/// and measures the peak live allocation attributable to the install; at
/// 331c18f `image_signature` read the entire image into one Vec, so the
/// peak reached the image size.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r5_install_hashing_is_bounded() {
    let _serial = serial();
    // A leader whose image is several MiB: staged import chunks are retained
    // in the store. Custom limits admit 1 MiB commands.
    let group = kit::identity(kit::SEED);
    let limits = pipestream_search::pb::storage::SourceAuthorityLimits {
        max_owners: 16,
        max_decisions: 1_000,
        max_payload_bytes: 256 << 20,
        max_command_bytes: 1 << 20,
    };
    let mut cluster = Cluster::bootstrap("r5-bounded", &group, &kit::policy(), &limits).await;
    let store = cluster.leader().store().unwrap();
    let legacy = kit::legacy_plane(&cluster.leader_dir, 2);
    let (retired, _request) = kit::retire(&store, &legacy, 1);
    drop(store);
    let payload: Vec<u8> = (0..12usize << 20).map(|i| (i % 251) as u8).collect();
    let chunk_bytes: u32 = 512 << 10;
    let chunk_count = (payload.len() as u32).div_ceil(chunk_bytes);
    let begin = kit::import_command(
        &group,
        "begin",
        1,
        kit::WORKFLOW,
        kit::begin_action(&retired, &payload, chunk_bytes, chunk_count),
    );
    let decision = cluster
        .leader()
        .propose_import("alice", &begin, Some(&retired))
        .await
        .unwrap();
    assert_eq!(decision.code, 0, "{}", decision.message);
    for ordinal in 0..chunk_count {
        let chunk = kit::import_command(
            &group,
            &format!("chunk-{ordinal}"),
            2 + ordinal as u64,
            kit::WORKFLOW,
            kit::chunk_action(&payload, chunk_bytes, ordinal),
        );
        let decision = cluster
            .leader()
            .propose_import("alice", &chunk, None)
            .await
            .unwrap();
        assert_eq!(decision.code, 0, "{}", decision.message);
    }
    let (image, _meta) = raft_kit::snapshot_image(&cluster.leader_dir, cluster.leader()).await;
    assert!(
        image.len() >= 12 << 20,
        "fixture image is {} bytes; raise the staged payload",
        image.len()
    );

    // Seed the member through the supported path, measuring the peak.
    // Baseline re-recorded inside the target-wide SERIAL section: no other
    // test can allocate between this reset and the peak read below.
    cluster.start_member().await;
    let watermark = live_bytes();
    PEAK.store(watermark, Ordering::Relaxed);
    cluster.add_member().await;
    let growth = peak_bytes().saturating_sub(watermark);
    assert!(!cluster.member().awaiting_snapshot().unwrap());
    eprintln!(
        "r5 bounded: image {} bytes, peak live growth across the seeding install {} bytes",
        image.len(),
        growth
    );
    assert!(
        growth < image.len(),
        "R5 REQUIRED bounded-buffer hashing; the install path allocated a \
         peak of {growth} bytes live for a {}-byte image (peak >= image size \
         demonstrates the whole-file Vec read)",
        image.len()
    );
    cluster.shutdown().await;
}

/// Snapshot admission (docs/raft-hosting.md, "Snapshot admission"): every
/// invalid transfer is refused by the transport's staging, by name, with
/// the core running and its state intact throughout — oversized, foreign,
/// truncated, wrong membership, out of order, interrupted, superseded,
/// bound to another peer, meta changed within a transfer — and a valid
/// snapshot then installs without a restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_admission_refuses_invalid_transfers_without_stopping_the_core() {
    let _serial = serial();
    let (cluster, image, newer) = detached("admission").await;
    let (before, phase_before) = member_state(&cluster);
    let position_before = raft_kit::durable_position(cluster.member());
    let (_dir_b, host_b, image_b, meta_b) = foreign_image("admission-b").await;
    let index = newer.last_log_id.unwrap().index;
    let group = cluster.group.clone();
    let intact = |what: &str| {
        assert!(
            raft_kit::core_running(cluster.member()),
            "{what}: the member core stopped"
        );
        let (now, phase) = member_state(&cluster);
        assert_eq!(
            now.1.snapshot_id, before.1.snapshot_id,
            "{what}: generation replaced"
        );
        assert_eq!(now.0, before.0, "{what}: image bytes changed");
        assert_eq!(phase, phase_before, "{what}: owner row changed");
        assert_eq!(
            raft_kit::durable_position(cluster.member()),
            position_before,
            "{what}: applied position moved"
        );
        assert!(
            raft_kit::incoming_dirs(cluster.member_dir.path()).is_empty(),
            "{what}: a refused transfer left its bytes behind"
        );
    };
    let mut peer =
        raft_kit::peer_client(cluster.member().listen_addr().unwrap(), raft_kit::PEER_ID).await;
    let vote = raft_kit::current_vote(cluster.leader());
    let chunk = |meta: &RaftSnapshotMeta, offset: usize, data: &[u8], done: bool| {
        raft_kit::timed(raft_kit::chunk_request(
            &group,
            raft_kit::PEER_ID,
            raft_kit::MEMBER_ID,
            &vote,
            meta,
            offset,
            data,
            done,
        ))
    };

    // Oversized: an announcement past the image bound is refused before
    // any byte lands.
    let mut huge = raft_kit::renamed(&newer, index, &image);
    huge.length = (4u64 << 30) + 1;
    huge.snapshot_id = format!(
        "{}-{}-{}",
        index,
        (4u64 << 30) + 1,
        pipestream_search::sha256::to_hex(&pipestream_search::sha256::digest(&image))
    );
    let error = peer
        .install_snapshot(chunk(&huge, 0, &image[..1024], false))
        .await
        .err()
        .expect("an oversized announcement must refuse");
    assert_eq!(error.code(), Code::ResourceExhausted, "{error}");
    assert!(error.message().contains("image bound"), "{error}");
    intact("oversized");

    // Foreign bytes under a forged meta of this group: refused at the
    // probe, named.
    let forged = raft_kit::renamed(&raft_kit::with_group(&meta_b, &group), index, &image_b);
    let error = cluster
        .push(&forged, &image_b)
        .await
        .err()
        .expect("foreign image refused");
    assert!(
        error.message().contains("group") || error.message().contains("incarnation"),
        "{error}"
    );
    intact("foreign");

    // Truncated: the final chunk arrives short of the announcement.
    let error = cluster
        .push(&newer, &image[..image.len() / 2])
        .await
        .err()
        .expect("a truncated transfer must refuse");
    assert!(error.message().contains("announced"), "{error}");
    intact("truncated");

    // Wrong membership over the genuine bytes.
    let wrong = raft_kit::meta_pb_for(&group, index, &image, &[9]);
    let error = cluster
        .push(&wrong, &image)
        .await
        .err()
        .expect("wrong membership refused");
    assert!(error.message().contains("membership"), "{error}");
    intact("membership");

    // Out of order: a first chunk at a nonzero offset is a mismatch the
    // sender restarts from; nothing is staged.
    let reply = peer
        .install_snapshot(chunk(&newer, 4096, &image[4096..8192], false))
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(
        reply.outcome,
        Some(pipestream_search::pb::storage::raft_install_snapshot_response::Outcome::Mismatch(_))
    ));
    intact("out of order");

    // Interrupted, then superseded by the same peer: the half receive is
    // dropped with its bytes when the next id begins.
    let half = &image[..image.len() / 2];
    let mut offset = 0;
    for piece in half.chunks(raft_kit::CHUNK_BYTES) {
        peer.install_snapshot(chunk(&newer, offset, piece, false))
            .await
            .unwrap();
        offset += piece.len();
    }
    assert_eq!(raft_kit::incoming_dirs(cluster.member_dir.path()).len(), 1);
    let other_id = raft_kit::renamed(&newer, index + 1, &image);
    let error = cluster
        .push(&other_id, &image)
        .await
        .err()
        .expect("a renamed image fails its position check");
    assert!(error.message().contains("applied position"), "{error}");
    intact("superseded");

    // Meta changed within one transfer: dropped, named.
    peer.install_snapshot(chunk(&newer, 0, &image[..4096], false))
        .await
        .unwrap();
    let error = peer
        .install_snapshot(chunk(&wrong, 4096, &image[4096..8192], false))
        .await
        .err()
        .expect("a changed meta must refuse");
    assert_eq!(error.code(), Code::InvalidArgument, "{error}");
    assert!(error.message().contains("changed"), "{error}");
    intact("meta changed");

    // Bound to its peer: while node 3's transfer is open, node 1's own
    // certificate cannot continue it, and cannot start another under the
    // same vote; node 3 superseding its own transfer is allowed.
    peer.install_snapshot(chunk(&newer, 0, &image[..4096], false))
        .await
        .unwrap();
    let mut other =
        raft_kit::peer_client(cluster.member().listen_addr().unwrap(), raft_kit::NODE_ID).await;
    let error = other
        .install_snapshot(raft_kit::timed(raft_kit::chunk_request(
            &group,
            raft_kit::NODE_ID,
            raft_kit::MEMBER_ID,
            &vote,
            &newer,
            4096,
            &image[4096..8192],
            false,
        )))
        .await
        .err()
        .expect("another peer cannot continue a bound transfer");
    assert_eq!(error.code(), Code::FailedPrecondition, "{error}");
    assert!(error.message().contains("bound to node 3"), "{error}");
    let error = other
        .install_snapshot(raft_kit::timed(raft_kit::chunk_request(
            &group,
            raft_kit::NODE_ID,
            raft_kit::MEMBER_ID,
            &vote,
            &other_id,
            0,
            &image[..4096],
            false,
        )))
        .await
        .err()
        .expect("another peer cannot start a transfer under the same vote");
    assert_eq!(error.code(), Code::Unavailable, "{error}");
    assert!(error.message().contains("in progress"), "{error}");
    assert!(raft_kit::core_running(cluster.member()));

    // The genuine newer image installs on the same running core: the
    // interrupted transfer of node 3 is superseded by its own new one.
    cluster.push(&newer, &image).await.unwrap();
    assert!(raft_kit::core_running(cluster.member()));
    assert_eq!(raft_kit::durable_position(cluster.member()), index);
    assert!(raft_kit::incoming_dirs(cluster.member_dir.path()).is_empty());
    let (after, _) = member_state(&cluster);
    assert_eq!(after.1.snapshot_id, newer.snapshot_id);
    assert_eq!(
        cluster
            .member()
            .store()
            .unwrap()
            .policy("alice", kit::WORKSPACE, kit::COLLECTION)
            .unwrap()
            .revision,
        2
    );
    host_b.shutdown().await.unwrap();
    cluster.shutdown().await;
}

/// The serve path beside the publish path. The leader serves its published
/// generation to a lagging peer on the state machine worker
/// (`get_current_snapshot`) while a build, in its own task, publishes the
/// next generation and removes the rest. Before this checkpoint the serve
/// read the pointer and then the generation with no lock between them; a
/// publish landing between the two removed the generation under the
/// serve, the read failed as a storage error and the core stopped, with
/// no invalid data and no storage fault anywhere. Now a serve is one step
/// under the pointer lock, from reading the pointer to opening the image;
/// a publish waits for it, and an open file outlives the directory's
/// removal. The two gates pin the interleaving: the build is paused just
/// before it publishes, the serve just after it read the pointer, and the
/// build is released first.
#[cfg(feature = "fault-injection")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_serve_under_a_concurrent_publish_returns_the_generation_it_read() {
    let _serial = serial();
    let group = kit::identity(kit::SEED);
    let dir = kit::TestDir::new("serve-under-publish");
    let host = std::sync::Arc::new(raft_kit::bootstrap_host(&dir).await);
    propose_ok(&host, &prepare_command(&group, 1)).await;
    let (first_image, first_meta) = raft_kit::snapshot_image(&dir, &host).await;
    let first = raft_kit::published_generation(dir.path()).unwrap();
    propose_ok(&host, &grant_command(&group, 2, 1, "bob")).await;
    let applied = raft_kit::durable_position(&host);
    raft_kit::wait_applied(&host, applied).await;
    assert!(first_meta.last_log_id.unwrap().index < applied);

    // A build in flight in its own task: image copied, generation
    // directory in place, paused before it publishes.
    let build_gate = host.arm_snapshot_build_gate();
    host.trigger_snapshot().await.unwrap();
    build_gate.reached().await;

    // A serve on the state machine worker, paused after it read the
    // pointer and before it opened the generation.
    let serve_gate = host.arm_snapshot_serve_gate();
    let serving = {
        let host = std::sync::Arc::clone(&host);
        tokio::spawn(async move { host.serve_snapshot().await })
    };
    serve_gate.reached().await;

    // The build may publish now. It has this long to move the pointer and
    // remove the first generation under the paused serve.
    build_gate.release();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let pointer_during_pause = raft_kit::published_generation(dir.path());
    let first_image_present = raft_kit::generation_dir(dir.path(), first)
        .join("image.redb")
        .exists();

    serve_gate.release();
    let served = serving.await.unwrap();
    let outcome = served.as_ref().map(|s| {
        s.as_ref()
            .map(|(meta, image)| (meta.snapshot_id.clone(), image.len()))
    });
    assert!(
        raft_kit::core_running(&host),
        "the core stopped: the serve read generation {first}, the publish moved the pointer \
         to {pointer_during_pause:?} under it (image present: {first_image_present}); \
         serve outcome {outcome:?}"
    );
    let (served_meta, served_image) = served
        .expect("the serve completed")
        .expect("a generation was published");
    assert_eq!(
        served_meta, first_meta,
        "the serve returned another generation's meta"
    );
    assert_eq!(served_image, first_image, "the serve returned other bytes");
    assert_eq!(
        pointer_during_pause,
        Some(first),
        "the pointer moved under a serve that had read it"
    );
    assert!(
        first_image_present,
        "the generation was removed under a serve that had read the pointer to it"
    );

    // The build publishes once the serve is done; the first generation goes
    // with it, and the core keeps committing.
    host.wait(Some(std::time::Duration::from_secs(30)))
        .metrics(
            |metrics| metrics.snapshot.is_some_and(|s| s.index >= applied),
            "the next generation built",
        )
        .await
        .unwrap();
    let second = raft_kit::published_generation(dir.path()).unwrap();
    assert!(second > first, "the build did not publish after the serve");
    assert!(!raft_kit::generation_dir(dir.path(), first).exists());
    let (_, second_meta) = raft_kit::published_pair(dir.path()).unwrap();
    assert_eq!(second_meta.last_log_id.unwrap().index, applied);
    assert!(raft_kit::core_running(&host));
    propose_ok(&host, &grant_command(&group, 3, 2, "carol")).await;
    assert_eq!(raft_kit::durable_position(&host), applied + 1);
    std::sync::Arc::try_unwrap(host)
        .ok()
        .expect("no task holds the host")
        .shutdown()
        .await
        .unwrap();
}

/// The other non-storage condition on the build path: a snapshot trigger
/// at a prepared member that has applied nothing yet. The library sends
/// the build regardless, the builder has no position to snapshot, and its
/// refusal would stop the core the way a storage failure does. The host
/// refuses the trigger by name before the library sees it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_snapshot_trigger_with_nothing_applied_is_refused_before_the_library() {
    let _serial = serial();
    let group = kit::identity(kit::SEED);
    let mut cluster =
        Cluster::bootstrap("trigger-unseeded", &group, &kit::policy(), &kit::limits()).await;
    propose_ok(cluster.leader(), &prepare_command(&group, 1)).await;
    cluster.start_member().await;
    assert!(cluster.member().awaiting_snapshot().unwrap());

    let refused = cluster.member().trigger_snapshot().await.err();
    // A build the library was sent would fail on its own task shortly
    // after; the core must still be running once it had that time.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(
        raft_kit::core_running(cluster.member()),
        "the member core stopped on a snapshot trigger with nothing applied (trigger outcome: \
         {refused:?})"
    );
    let refused = refused.expect("the trigger is refused by name");
    assert_eq!(refused.code(), Code::FailedPrecondition, "{refused}");
    assert!(refused.message().contains("applied position"), "{refused}");
    assert!(raft_kit::published_generation(cluster.member_dir.path()).is_none());

    // The member is seeded afterwards as usual, and can snapshot then.
    cluster.add_member().await;
    assert!(!cluster.member().awaiting_snapshot().unwrap());
    raft_kit::wait_applied(
        cluster.member(),
        raft_kit::durable_position(cluster.leader()),
    )
    .await;
    let (_, meta) = raft_kit::snapshot_image(&cluster.member_dir, cluster.member()).await;
    assert_eq!(
        meta.last_log_id.unwrap().index,
        raft_kit::durable_position(cluster.member())
    );
    assert!(raft_kit::core_running(cluster.member()));
    cluster.shutdown().await;
}

//! Snapshot-protocol target (slice 4a; `docs/control-authority-test-harness.md`).
//!
//! R3 (immutable generations, group/membership binding) and R5 (exact
//! receive identity, bounded install hashing), reconciled onto the frozen
//! 0fe7081 checkpoint. The standalone `ControlStateMachine` constructor is
//! pub(crate) there, so installs are driven through the real cluster: a
//! prepared member joins and the leader seeds it by snapshot over loopback
//! mTLS (the in-crate half of each contract is cited where the pub(crate)
//! seam is the only way to reach it). Every test pins the REQUIRED behavior
//! quoted from docs/raft-hosting.md; at 0fe7081 all are expected to pass.
//!
//! Run: `cargo test --test control_raft_snapshots --features
//! raft,fault-injection --release -- --test-threads=4` (the target holds a
//! target-wide serial lock, so any `--test-threads` value is safe).
#![cfg(feature = "raft")]

mod control_adversarial;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use control_adversarial::{kit, raft_kit};
use pipestream_search::pb::storage::RaftSnapshotMeta;
use pipestream_search::raft::RaftHost;
use prost::Message;

const OWNER_WORKFLOW: &[u8] = b"installation-v1";

// ---- peak live-allocation tracker (R5 bounded-buffer measurement) ----------
//
// A global allocator sees every allocation in the test binary; the
// measurement test resets the watermark inside the target-wide serial lock
// and takes the growth attributable to one install as peak-after minus the
// watermark before it.

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

/// Target-wide serialization: the allocation watermark above is
/// process-global, so every test holds this lock for its whole body. The
/// guard is taken before the first `.await` and is `Send`, so it is safe
/// inside `#[tokio::test(flavor = "multi_thread")]`. Panics poison the
/// mutex; re-acquire across poisoning — mutual exclusion is what matters.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn peak_bytes() -> usize {
    PEAK.load(Ordering::Relaxed)
}

/// Reset the watermark; returns the live-bytes baseline it recorded.
fn reset_watermark() -> usize {
    let live = LIVE.load(Ordering::Relaxed);
    PEAK.store(live, Ordering::Relaxed);
    live
}

// ---- shared helpers ---------------------------------------------------------

/// Poll a predicate with a deadline.
async fn poll_until(deadline: Duration, what: &str, mut probe: impl FnMut() -> bool) {
    let start = std::time::Instant::now();
    loop {
        if probe() {
            return;
        }
        assert!(
            start.elapsed() < deadline,
            "{what} did not happen within {deadline:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// A second fixture owner key: a store serves one preparation per owner
/// key, so any test proposing twice needs a second key.
fn second_owner_key() -> pipestream_search::pb::storage::LogicalSourceOwner {
    pipestream_search::pb::storage::LogicalSourceOwner {
        owner_id: b"snap-second-owner".to_vec(),
        ..kit::key()
    }
}

/// One prepare at `(term 1, node 1, index)`; returns the applied index.
async fn propose_prepare(host: &RaftHost, owner_id: &str, ecr: u64) -> u64 {
    propose_prepare_keyed(host, &kit::owner_key(), owner_id, ecr).await
}

/// One prepare on an explicit owner key. Distinct keys keep every owner row
/// live, which the multi-MiB image fixture relies on (a second prepare on a
/// key that already holds a preparation refuses by name).
async fn propose_prepare_keyed(
    host: &RaftHost,
    key: &pipestream_search::pb::storage::LogicalSourceOwner,
    owner_id: &str,
    ecr: u64,
) -> u64 {
    let authority = kit::identity(kit::SEED);
    let command = kit::source_command(
        &authority,
        key,
        owner_id,
        ecr,
        1,
        0,
        kit::prepare_action(OWNER_WORKFLOW),
    );
    let decision = host.propose_command("alice", &command).await.unwrap();
    assert_eq!(decision.code, 0, "{}", decision.message);
    host.applied_position().unwrap().unwrap().index
}

/// The prost meta of one generation directory.
fn read_meta(gen_dir: &Path) -> RaftSnapshotMeta {
    let bytes = std::fs::read(gen_dir.join("meta")).unwrap();
    RaftSnapshotMeta::decode(bytes.as_slice()).unwrap()
}

/// Trigger a snapshot and wait for a generation binding at least
/// `min_index` to publish; returns its number and meta. A bare pointer read
/// is not enough: the previous generation's pointer still names valid files
/// until the new build publishes.
async fn wait_snapshot_at(
    dir: &kit::TestDir,
    host: &RaftHost,
    min_index: u64,
) -> (u64, RaftSnapshotMeta) {
    host.trigger_snapshot().await.unwrap();
    poll_until(
        Duration::from_secs(30),
        "snapshot generation published",
        || match raft_kit::read_pointer(dir) {
            Some(generation) => {
                let gen_dir = raft_kit::generation_dir(dir, generation);
                gen_dir.join("meta").exists()
                    && read_meta(&gen_dir).last_log_id.map(|id| id.index) >= Some(min_index)
            }
            None => false,
        },
    )
    .await;
    let generation = raft_kit::read_pointer(dir).unwrap();
    let meta = read_meta(&raft_kit::generation_dir(dir, generation));
    (generation, meta)
}

/// Length and SHA-256 of an image, streamed through a bounded buffer —
/// the same bounded pass the snapshot id binds.
fn hash_image(path: &Path) -> (u64, [u8; 32]) {
    use std::io::Read as _;
    let mut file = std::fs::File::open(path).unwrap();
    let mut hasher = pipestream_search::sha256::Sha256::new();
    let mut buffer = vec![0u8; 1024 * 1024];
    let mut length = 0u64;
    loop {
        let read = file.read(&mut buffer).unwrap();
        if read == 0 {
            break;
        }
        length += read as u64;
        hasher.update(&buffer[..read]);
    }
    (length, hasher.finalize())
}

/// Every generation currently on disk under one snapshots root, with its
/// decoded meta.
fn generations(snapshots: &Path) -> Vec<(PathBuf, RaftSnapshotMeta)> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(snapshots.join("generations")).unwrap() {
        let dir = entry.unwrap().path();
        if dir.join("image.redb").exists() {
            out.push((dir.clone(), read_meta(&dir)));
        }
    }
    out
}

/// The disk contract every published generation must satisfy
/// (docs/raft-hosting.md: "The image is a copy of the store file ... the
/// snapshot id names both and the meta carries group, last log id,
/// membership and the checksum"; state_machine.rs `read_generation`).
fn assert_generation_contract(
    gen_dir: &Path,
    meta: &RaftSnapshotMeta,
    group: &pipestream_search::pb::storage::SourceAuthorityIdentity,
) {
    assert_eq!(
        meta.format_version, 1,
        "the meta format version must be the current one"
    );
    assert_eq!(
        meta.group.as_ref(),
        Some(group),
        "every published generation's meta must name this group"
    );
    let (length, sha256) = hash_image(&gen_dir.join("image.redb"));
    assert_eq!(meta.length, length, "the meta length must name the image");
    assert_eq!(
        meta.sha256,
        sha256.to_vec(),
        "the meta digest must be the image's SHA-256"
    );
    let signature = pipestream_search::raft::state_machine::ImageSignature { length, sha256 };
    let log_id = meta
        .last_log_id
        .as_ref()
        .map(pipestream_search::raft::types::log_id_from_proto);
    assert_eq!(
        meta.snapshot_id,
        pipestream_search::raft::state_machine::format_snapshot_id(log_id.as_ref(), &signature),
        "the snapshot id must name the exact bytes"
    );
}

// ---------------------------------------------------------------------------
// R3: snapshot generations — publication, retention, group/membership binding
// ---------------------------------------------------------------------------

/// R3: a learner seeded through the real transport serves the group's owner
/// rows, and the generation the leader published names this group and the
/// membership the leader committed.
#[cfg(feature = "tls")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r3_learner_seeded_by_verified_snapshot() {
    let _serial = serial();
    let authority = kit::identity(kit::SEED);
    let directory = raft_kit::peer_directory(&authority, &[1, 2]);
    let dir_leader = kit::TestDir::new("r3-seeded-leader");
    let leader = raft_kit::bootstrap_cluster_node(&dir_leader, 1, &directory).await;
    propose_prepare(&leader, "r3-seeded-owner", 1).await;

    let dir_member = kit::TestDir::new("r3-seeded-member");
    let member = raft_kit::start_member_node(&dir_member, 2, &directory).await;
    leader
        .add_learner(2, member.advertised_addr().unwrap())
        .await
        .unwrap();
    poll_until(Duration::from_secs(30), "member seeded", || {
        !member.awaiting_snapshot().unwrap()
    })
    .await;

    // The seeded store serves the installed image.
    let store = member.store().unwrap();
    let owner = store.owner("alice", &kit::owner_key()).unwrap();
    assert_eq!(
        owner.phase,
        pipestream_search::pb::storage::PreparedSourceOwnerPhase::Prepared as i32,
        "the seeded member must serve the leader's owner row"
    );
    drop(store);

    // The generation on the member's disk is the one the leader published:
    // same group, same committed membership (docs/raft-hosting.md: "a
    // snapshot built on one node installs into a fresh replica of the group
    // with the same owner rows").
    let leader_meta = {
        let generation = raft_kit::read_pointer(&dir_leader).unwrap();
        read_meta(&raft_kit::generation_dir(&dir_leader, generation))
    };
    let member_meta = {
        let generation = raft_kit::read_pointer(&dir_member).unwrap();
        read_meta(&raft_kit::generation_dir(&dir_member, generation))
    };
    assert_eq!(
        member_meta.group.as_ref(),
        Some(&authority),
        "the seeded generation's meta must name this group"
    );
    assert_eq!(
        member_meta.membership, leader_meta.membership,
        "the seeded generation must carry the membership the leader committed"
    );
    assert_eq!(
        member_meta.last_log_id, leader_meta.last_log_id,
        "the seeded generation must bind the leader's applied position"
    );
    leader.shutdown().await.unwrap();
    member.shutdown().await.unwrap();
}

/// R3: publication moves the pointer and drops the previous generation —
/// "Build writes a `build-*` directory, syncs, renames it into a new
/// generation, publishes the pointer and only then removes older
/// generations" (docs/raft-hosting.md).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r3_publish_drops_previous_generation() {
    let _serial = serial();
    let dir = kit::TestDir::new("r3-publish-drops");
    let guard = raft_kit::bootstrap_host(&dir).await;
    propose_prepare(&guard, "r3-pub-a", 1).await;
    let (_image, meta_a) = raft_kit::snapshot_image(&dir, &guard).await;
    let gen_a = raft_kit::read_pointer(&dir).unwrap();

    propose_prepare_keyed(&guard, &second_owner_key(), "r3-pub-b", 2).await;
    // The second prepare applied at index 3 (membership at 1, prepares 2, 3).
    let (gen_b, meta_b) = wait_snapshot_at(&dir, &guard, 3).await;

    assert_ne!(gen_a, gen_b, "the pointer must move to the new generation");
    assert!(
        !raft_kit::generation_dir(&dir, gen_a).exists(),
        "publication must remove the previous generation directory"
    );
    let gen_b_dir = raft_kit::generation_dir(&dir, gen_b);
    assert!(gen_b_dir.join("image.redb").exists());
    assert!(meta_b.last_log_id.map(|id| id.index) > meta_a.last_log_id.map(|id| id.index));
    assert_generation_contract(&gen_b_dir, &meta_b, &kit::identity(kit::SEED));
    guard.shutdown().await.unwrap();
}

/// R3: a reader holding the previous generation's image across a publication
/// keeps seeing the same bytes — generations are immutable; publication
/// unlinks the old directory but never rewrites it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r3_reader_held_across_publication() {
    let _serial = serial();
    let dir = kit::TestDir::new("r3-reader-held");
    let guard = raft_kit::bootstrap_host(&dir).await;
    propose_prepare(&guard, "r3-reader-a", 1).await;
    let (_image, _meta) = raft_kit::snapshot_image(&dir, &guard).await;
    let gen_a = raft_kit::read_pointer(&dir).unwrap();
    let image_a = raft_kit::generation_dir(&dir, gen_a).join("image.redb");

    // Hold a reader on generation A across the publication of generation B.
    let mut held = std::fs::File::open(&image_a).unwrap();
    let (len_a, sha_a) = hash_image(&image_a);

    propose_prepare_keyed(&guard, &second_owner_key(), "r3-reader-b", 2).await;
    let (gen_b, _meta_b) = wait_snapshot_at(&dir, &guard, 3).await;
    assert_ne!(gen_a, gen_b);
    assert!(
        !raft_kit::generation_dir(&dir, gen_a).exists(),
        "publication removes the previous generation's directory"
    );

    // The held reader still sees generation A's exact bytes (POSIX defers
    // the inode unlink until the last handle closes; the bytes were never
    // rewritten).
    use std::io::Read as _;
    let mut bytes = Vec::new();
    held.read_to_end(&mut bytes).unwrap();
    assert_eq!(bytes.len() as u64, len_a);
    assert_eq!(
        pipestream_search::sha256::digest(&bytes),
        sha_a,
        "a held reader must see immutable bytes across publication"
    );
    guard.shutdown().await.unwrap();
}

/// R3: startup sweeps interrupted artifacts — partial builds, abandoned
/// receives, staged files and unpublished generations — while the published
/// generation stays intact (state_machine.rs `sweep`, at machine
/// construction; docs/raft-hosting.md: "startup sweeps partial builds,
/// abandoned receives and unpublished generations while verifying the
/// published one").
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r3_startup_sweep_removes_interrupted_artifacts() {
    let _serial = serial();
    let dir = kit::TestDir::new("r3-startup-sweep");
    let guard = raft_kit::bootstrap_host(&dir).await;
    propose_prepare(&guard, "r3-sweep-owner", 1).await;
    let (_image, meta) = raft_kit::snapshot_image(&dir, &guard).await;
    let published = raft_kit::read_pointer(&dir).unwrap();
    guard.shutdown().await.unwrap();

    // Plant an interrupted build, an abandoned receive, a staged file and
    // an unpublished generation, exactly where a crash would leave them.
    let snapshots = raft_kit::snapshots_dir(&dir);
    std::fs::create_dir(snapshots.join("incoming-1-0")).unwrap();
    std::fs::write(
        snapshots.join("incoming-1-0").join("image.redb"),
        b"partial",
    )
    .unwrap();
    std::fs::create_dir(snapshots.join("build-1")).unwrap();
    std::fs::write(snapshots.join("staged.redb"), b"staged").unwrap();
    std::fs::create_dir(snapshots.join("generations").join("999")).unwrap();
    std::fs::write(
        snapshots.join("generations").join("999").join("image.redb"),
        b"orphan",
    )
    .unwrap();

    let guard = raft_kit::start_host(&dir).await;
    raft_kit::wait_leader(&guard).await;
    assert!(
        !snapshots.join("incoming-1-0").exists(),
        "abandoned receives must be swept"
    );
    assert!(
        !snapshots.join("build-1").exists(),
        "partial builds must be swept"
    );
    assert!(
        !snapshots.join("staged.redb").exists(),
        "staged files must be swept"
    );
    assert!(
        !snapshots.join("generations").join("999").exists(),
        "unpublished generations must be swept"
    );
    assert_eq!(
        raft_kit::read_pointer(&dir),
        Some(published),
        "the published generation must stay published across a restart"
    );
    let gen_dir = raft_kit::generation_dir(&dir, published);
    assert!(gen_dir.join("image.redb").exists());
    assert_generation_contract(&gen_dir, &meta, &kit::identity(kit::SEED));
    let store = guard.store().unwrap();
    let owner = store.owner("alice", &kit::owner_key()).unwrap();
    assert_eq!(
        owner.phase,
        pipestream_search::pb::storage::PreparedSourceOwnerPhase::Prepared as i32,
        "the swept node must keep serving the published generation's owner"
    );
    drop(store);
    guard.shutdown().await.unwrap();
}

/// R3: the adversarial halves of the group/membership binding — a valid
/// image of ANOTHER group, and a meta whose membership differs from the
/// image — refuse before anything is replaced. A real leader only ever
/// sends images of its own group with its own committed membership, and the
/// machine constructor that could ingest a forged image is pub(crate), so
/// from outside the crate the binding is pinned as the disk contract: every
/// generation a node publishes names its group and verifies against its
/// image. The refusal halves live in-crate: src/raft/tests.rs:590 (foreign
/// image refuses) and src/raft/tests.rs:605 (wrong membership refuses);
/// docs/raft-hosting.md: "a corrupted image, a valid image of another group
/// and a meta with another membership refuse before anything is replaced".
#[cfg(feature = "tls")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r3_disk_generations_name_the_group_and_verify() {
    let _serial = serial();
    let authority = kit::identity(kit::SEED);
    let directory = raft_kit::peer_directory(&authority, &[1, 2]);
    let dir_leader = kit::TestDir::new("r3-disk-leader");
    let leader = raft_kit::bootstrap_cluster_node(&dir_leader, 1, &directory).await;
    propose_prepare(&leader, "r3-disk-owner", 1).await;

    let dir_member = kit::TestDir::new("r3-disk-member");
    let member = raft_kit::start_member_node(&dir_member, 2, &directory).await;
    leader
        .add_learner(2, member.advertised_addr().unwrap())
        .await
        .unwrap();
    poll_until(Duration::from_secs(30), "member seeded", || {
        !member.awaiting_snapshot().unwrap()
    })
    .await;

    for (label, dir) in [("leader", &dir_leader), ("member", &dir_member)] {
        let gens = generations(&raft_kit::snapshots_dir(dir));
        assert!(
            !gens.is_empty(),
            "{label} must publish at least one generation"
        );
        for (gen_dir, meta) in &gens {
            assert_generation_contract(gen_dir, meta, &authority);
        }
    }
    leader.shutdown().await.unwrap();
    member.shutdown().await.unwrap();
}

// ---------------------------------------------------------------------------
// R5: receive identity and bounded install work
// ---------------------------------------------------------------------------

/// R5: a receive is identified by the exact directory it was started in,
/// not by any filename ordering — "Install receives into
/// `incoming-<token>/image.redb`, the exact directory the receive was
/// started in" (docs/raft-hosting.md; `begin_receiving_snapshot`,
/// state_machine.rs). An adversarially later-named directory planted while
/// the member runs is neither selected nor removed: the join succeeds and
/// the planted bytes are untouched.
#[cfg(feature = "tls")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r5_receive_identity_is_exact_and_abandoned_receives_are_left_alone() {
    let _serial = serial();
    let authority = kit::identity(kit::SEED);
    let directory = raft_kit::peer_directory(&authority, &[1, 2]);
    let dir_leader = kit::TestDir::new("r5-receive-leader");
    let leader = raft_kit::bootstrap_cluster_node(&dir_leader, 1, &directory).await;
    propose_prepare(&leader, "r5-receive-owner", 1).await;

    let dir_member = kit::TestDir::new("r5-receive-member");
    let member = raft_kit::start_member_node(&dir_member, 2, &directory).await;
    // Plant a later-named incoming directory AFTER the startup sweep and
    // BEFORE the join. The nanos token outranks any real receive token for
    // the next ~500 years.
    let planted = raft_kit::snapshots_dir(&dir_member).join("incoming-99999999999999999999-0");
    std::fs::create_dir(&planted).unwrap();
    std::fs::write(planted.join("garbage.bin"), b"not a snapshot").unwrap();

    leader
        .add_learner(2, member.advertised_addr().unwrap())
        .await
        .unwrap();
    poll_until(Duration::from_secs(30), "member seeded", || {
        !member.awaiting_snapshot().unwrap()
    })
    .await;

    // The exact-identity receive installed the leader's image...
    let store = member.store().unwrap();
    assert!(store.owner("alice", &kit::owner_key()).is_ok());
    drop(store);
    // ...and the planted directory was never touched: exact receive identity
    // means a superseded receive is the machine's own previous receive,
    // never an arbitrary on-disk name.
    assert!(
        planted.join("garbage.bin").exists(),
        "a planted incoming directory must be left alone"
    );
    assert_eq!(
        std::fs::read(planted.join("garbage.bin")).unwrap(),
        b"not a snapshot",
        "the planted bytes must be untouched"
    );
    leader.shutdown().await.unwrap();
    member.shutdown().await.unwrap();
}

/// R5: install hashing is bounded — "verifies length and digest streaming"
/// (docs/raft-hosting.md; `image_signature` streams a 1 MiB buffer,
/// state_machine.rs). With a multi-MiB image, the peak live-allocation
/// growth across a full seeding must stay below the image length; the
/// pre-fix whole-file `Vec` made growth meet or exceed it.
#[cfg(feature = "tls")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r5_install_hashing_is_bounded() {
    let _serial = serial();
    let authority = kit::identity(kit::SEED);
    let directory = raft_kit::peer_directory(&authority, &[1, 2]);
    let dir_leader = kit::TestDir::new("r5-bounded-leader");
    // Distinct owners keep every row live, so the image genuinely spans
    // multiple MiB; the bounds must admit them all.
    let limits = pipestream_search::pb::storage::SourceAuthorityLimits {
        max_owners: 20_000,
        max_decisions: 1_000_000,
        ..kit::limits()
    };
    let leader = RaftHost::bootstrap_cluster(
        dir_leader.path(),
        &authority,
        1,
        &kit::policy(),
        &limits,
        &raft_kit::cluster_host_config(),
        raft_kit::node_transport(1, &directory),
    )
    .await
    .unwrap();
    for i in 0..12_000u64 {
        let key = pipestream_search::pb::storage::LogicalSourceOwner {
            owner_id: format!("big-{i}").into_bytes(),
            ..kit::key()
        };
        propose_prepare_keyed(&leader, &key, &format!("big-{i}"), i + 1).await;
    }

    let dir_member = kit::TestDir::new("r5-bounded-member");
    let member = raft_kit::start_member_node(&dir_member, 2, &directory).await;
    // The watermark resets just before the join: the window covers the
    // leader's build, the transport, and the member's receive, verify and
    // install — every pass over the image must be bounded.
    let watermark = reset_watermark();
    leader
        .add_learner(2, member.advertised_addr().unwrap())
        .await
        .unwrap();
    poll_until(Duration::from_secs(30), "member seeded", || {
        !member.awaiting_snapshot().unwrap()
    })
    .await;
    let generation = raft_kit::read_pointer(&dir_leader).unwrap();
    let meta = read_meta(&raft_kit::generation_dir(&dir_leader, generation));
    let image_len = meta.length as usize;
    assert!(
        image_len > 2 << 20,
        "the fixture image must span multiple MiB to make the bound meaningful (got {image_len})"
    );
    let growth = peak_bytes().saturating_sub(watermark);
    assert!(
        growth < image_len,
        "R5: the install path must hash and move the image with bounded \
         buffering: peak live-allocation growth {growth} bytes against an \
         image of {image_len} bytes (a whole-file buffer would meet or \
         exceed the image length)"
    );
    leader.shutdown().await.unwrap();
    member.shutdown().await.unwrap();
}

/// R5: the build side refuses by name when the store outgrows the bound —
/// "A store larger than `max_snapshot_bytes` refuses to snapshot by name"
/// (docs/raft-hosting.md; `ControlSnapshotBuilder::build`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r5_oversize_image_refuses() {
    let _serial = serial();
    let dir = kit::TestDir::new("r5-oversize");
    let config = pipestream_search::raft::HostConfig {
        max_snapshot_bytes: 1024,
        snapshot_chunk_bytes: 128,
        ..raft_kit::host_config()
    };
    let guard = RaftHost::bootstrap_single(
        dir.path(),
        &kit::identity(kit::SEED),
        raft_kit::NODE_ID,
        raft_kit::NODE_ADDR,
        &kit::policy(),
        &kit::limits(),
        &config,
    )
    .await
    .unwrap();
    // A single prepare leaves the redb image far above the 1 KiB bound.
    // `trigger_snapshot` only queues the build inside openraft, so the
    // refusal surfaces as an effect, not as the trigger's return value:
    // no generation is ever published and the leader keeps serving.
    propose_prepare(&guard, "r5-oversize-owner", 1).await;
    guard.trigger_snapshot().await.unwrap();
    poll_until(
        Duration::from_secs(5),
        "the oversize build never publishes",
        || raft_kit::read_pointer(&dir).is_none(),
    )
    .await;
    assert!(
        guard.metrics().borrow().running_state.is_ok(),
        "the refused oversize build must not take the leader core down"
    );
    // The build had a full grace window to publish; nothing appeared.
    assert_eq!(
        raft_kit::read_pointer(&dir),
        None,
        "a refused build must not publish a generation"
    );
    guard.shutdown().await.unwrap();
}

/// R5: the snapshot id names the exact bytes — `"<index>-<length>-<sha256
/// hex>"` (`format_snapshot_id`, state_machine.rs). The published meta's id,
/// length and digest must equal a fresh streamed pass over the image.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r5_snapshot_id_names_the_exact_bytes() {
    let _serial = serial();
    let dir = kit::TestDir::new("r5-snapshot-id");
    let guard = raft_kit::bootstrap_host(&dir).await;
    propose_prepare(&guard, "r5-id-a", 1).await;
    propose_prepare_keyed(&guard, &second_owner_key(), "r5-id-b", 2).await;
    let (_image, meta) = raft_kit::snapshot_image(&dir, &guard).await;
    let generation = raft_kit::read_pointer(&dir).unwrap();
    let gen_dir = raft_kit::generation_dir(&dir, generation);
    let (length, sha256) = hash_image(&gen_dir.join("image.redb"));
    assert_eq!(meta.length, length, "the meta length names the image bytes");
    assert_eq!(
        meta.sha256,
        sha256.to_vec(),
        "the meta digest names the image bytes"
    );
    let signature = pipestream_search::raft::state_machine::ImageSignature { length, sha256 };
    let log_id = meta
        .last_log_id
        .as_ref()
        .map(pipestream_search::raft::types::log_id_from_proto);
    let expected =
        pipestream_search::raft::state_machine::format_snapshot_id(log_id.as_ref(), &signature);
    assert_eq!(
        meta.snapshot_id, expected,
        "the snapshot id must be `<index>-<length>-<sha256 hex>` of the image"
    );
    assert_eq!(
        meta.snapshot_id,
        format!(
            "{}-{}-{}",
            log_id.map_or(0, |id| id.index),
            length,
            pipestream_search::sha256::to_hex(&sha256)
        )
    );
    guard.shutdown().await.unwrap();
}

/// R5: `format_snapshot_id`/`parse_snapshot_id` round-trip the length and
/// digest the receiver verifies against (`parse_snapshot_id` feeds
/// `verify_image` at install).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r5_snapshot_id_round_trip() {
    let _serial = serial();
    let signature = pipestream_search::raft::state_machine::ImageSignature {
        length: 42,
        sha256: [7; 32],
    };
    let id = pipestream_search::raft::state_machine::format_snapshot_id(None, &signature);
    assert_eq!(
        pipestream_search::raft::state_machine::parse_snapshot_id(&id).unwrap(),
        signature
    );
    // A corrupted digest refuses loudly rather than parsing loosely.
    let bad = format!("{}-42-{}", 0, "z".repeat(64));
    assert!(pipestream_search::raft::state_machine::parse_snapshot_id(&bad).is_err());
    let short = "0-42-deadbeef";
    assert!(pipestream_search::raft::state_machine::parse_snapshot_id(short).is_err());
}

//! Snapshot-protocol regression target (slice 3d;
//! `docs/control-authority-test-harness.md`).
//!
//! Independent reproductions of Astra's checkpoint review findings R3
//! (snapshot generation publication) and R5 (receive identity, bounded
//! buffering), driven through a standalone `ControlStateMachine` over a kit
//! store — never a `RaftHost` — so installs can be exercised exactly as a
//! follower receiver sees them. Each test pins the REQUIRED behavior quoted
//! in the review; at the frozen checkpoint `331c18f` several FAIL, and each
//! failure is a minimized reproduction to hand to Fable, not a test bug.
//!
//! Run: `cargo test --test control_raft_snapshots --features
//! raft,fault-injection -- --test-threads=1`.
#![cfg(feature = "raft")]

mod control_adversarial;

use std::io::SeekFrom;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use control_adversarial::{kit, raft_kit};
use openraft::storage::RaftStateMachine;
use pipestream_search::pb::storage::RaftSnapshotMeta;
use pipestream_search::raft::ControlStateMachine;
use prost::Message;
use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _};

// ---- peak live-allocation tracker (R5 bounded-buffer measurement) ----------
//
// A global allocator sees every allocation in the test binary, so the
// measurement test runs with `--test-threads=1` (the target gate) and takes
// the growth attributable to one install as peak-after minus the watermark
// before it.

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

fn live_bytes() -> usize {
    LIVE.load(Ordering::Relaxed)
}

fn peak_bytes() -> usize {
    PEAK.load(Ordering::Relaxed)
}

/// One prepare at `(term 1, node 1, index)` for a fresh standalone store
/// sitting at control revision `index` (fresh stores start at revision 1 and
/// each command bumps it by one).
fn prepare_entry(index: u64) -> ControlEntry {
    raft_kit::control_entry(
        index,
        kit::source_command(
            &kit::identity(kit::SEED),
            &kit::owner_key(),
            &format!("prepare-{index}"),
            index,
            index,
            0,
            kit::prepare_action(format!("installation-{index}").as_bytes()),
        ),
    )
}

type ControlEntry = openraft::Entry<pipestream_search::raft::ControlRaft>;

/// A machine over a fresh kit store plus its published first generation.
async fn machine_with_generation(
    name: &str,
) -> (
    kit::TestDir,
    ControlStateMachine,
    Vec<u8>,
    openraft::SnapshotMeta<u64, openraft::BasicNode>,
) {
    let dir = kit::TestDir::new(name);
    let (store, _authority) = kit::create_store(&dir);
    let mut machine = ControlStateMachine::new(store, &dir.path().join("raft-snapshots")).unwrap();
    RaftStateMachine::apply(&mut machine, vec![prepare_entry(1)])
        .await
        .unwrap();
    let (image, meta) = raft_kit::build_current(&mut machine).await;
    (dir, machine, image, meta)
}

/// The prost meta on disk, decoded — for hand-crafting interrupted states.
fn meta_pb(dir: &kit::TestDir) -> RaftSnapshotMeta {
    let bytes = std::fs::read(dir.path().join("raft-snapshots").join("current.meta")).unwrap();
    RaftSnapshotMeta::decode(bytes.as_slice()).unwrap()
}

fn write_meta_pb(dir: &kit::TestDir, meta: &RaftSnapshotMeta) {
    std::fs::write(
        dir.path().join("raft-snapshots").join("current.meta"),
        meta.encode_to_vec(),
    )
    .unwrap();
}

/// The published image/meta pair as served right now; `None` when the state
/// refuses loudly (a loud refusal is an acceptable outcome in the
/// interruption cases, never silent service of a mixed pair).
async fn served(
    machine: &mut ControlStateMachine,
) -> Option<(Vec<u8>, openraft::SnapshotMeta<u64, openraft::BasicNode>)> {
    let snapshot = RaftStateMachine::get_current_snapshot(machine)
        .await
        .ok()
        .flatten()?;
    let mut snapshot = snapshot;
    let mut bytes = Vec::new();
    snapshot
        .snapshot
        .read_to_end(&mut bytes)
        .await
        .expect("read the served snapshot image");
    Some((bytes, snapshot.meta))
}

/// R3: a valid image of ANOTHER group must refuse, and the previous
/// generation must still be served. At 331c18f the install validates only
/// AFTER replacing current.redb and publishing current.meta, so the previous
/// generation is destroyed even though the install then errors.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r3_wrong_group_valid_image_is_refused_and_previous_survives() {
    let (dir_a, mut machine_a, image_a, _meta_a) = machine_with_generation("r3-group-a").await;
    let reader = machine_a
        .shared_store()
        .read()
        .unwrap()
        .clone()
        .expect("machine serves handles");

    // A valid one-entry image of a different group, with a correct id.
    let dir_b = kit::TestDir::new("r3-group-b");
    let store_b = pipestream_search::source_authority::SourceAuthorityStore::create(
        &dir_b.authority(),
        &kit::identity(9),
        &kit::policy(),
        &kit::limits(),
    )
    .unwrap();
    let mut machine_b =
        ControlStateMachine::new(store_b, &dir_b.path().join("raft-snapshots")).unwrap();
    RaftStateMachine::apply(&mut machine_b, vec![prepare_entry(1)])
        .await
        .unwrap();
    let (image_b, meta_b) = raft_kit::build_current(&mut machine_b).await;
    drop(machine_b);

    let mut findings: Vec<String> = Vec::new();
    let installed = raft_kit::install_received(&mut machine_a, &image_b, &meta_b).await;
    match &installed {
        Ok(()) => {
            findings.push("a foreign-group image installed without any group refusal".to_string())
        }
        Err(message) => {
            if !(message.contains("identity")
                || message.contains("authority")
                || message.contains("group"))
            {
                findings.push(format!(
                    "the refusal does not name the group/identity: {message}"
                ));
            }
        }
    }
    // REQUIRED: the previous generation is still served, byte for byte.
    match served(&mut machine_a).await {
        Some((bytes, meta)) => {
            if bytes != image_a {
                findings.push(format!(
                    "the previous generation was destroyed: current.redb now serves {} bytes of the foreign image (snapshot id {})",
                    bytes.len(),
                    meta.snapshot_id
                ));
            }
        }
        None => {
            findings.push("get_current_snapshot refuses after the rejected install".to_string())
        }
    }
    // The same REQUIRED behavior straight off the disk: install_snapshot must
    // not have replaced current.redb before the group check ran.
    let on_disk = std::fs::read(dir_a.path().join("raft-snapshots").join("current.redb"))
        .expect("current.redb exists");
    if on_disk != image_a {
        findings.push(format!(
            "current.redb on disk holds {} bytes after the refused install, not the previous {}-byte generation",
            on_disk.len(),
            image_a.len()
        ));
    }
    // REQUIRED: the live store keeps serving reads.
    if let Err(error) = reader.control_snapshot("alice", &kit::key()) {
        findings.push(format!("the live store stopped serving: {error}"));
    }
    drop(reader);

    assert!(
        findings.is_empty(),
        "R3 REQUIRED a group-naming refusal that preserves the previous \
         generation; observed:\n  - {}",
        findings.join("\n  - ")
    );
    drop(machine_a);
    drop(dir_a);
}

/// R3: the same group's image with a correct applied position but a
/// membership naming different voters must refuse BEFORE publication. At
/// 331c18f only the applied index is compared and the in-memory membership is
/// then set from the meta, so the install succeeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r3_correct_index_wrong_membership_is_refused() {
    let (_dir, mut machine, image, real_meta) = machine_with_generation("r3-membership").await;
    // Same bytes, same applied index, voters {9} instead of {1}.
    let forged = raft_kit::meta_for(real_meta.last_log_id.unwrap().index, &image, &[9]);
    assert_eq!(forged.snapshot_id, real_meta.snapshot_id);

    let outcome = raft_kit::install_received(&mut machine, &image, &forged).await;
    assert!(
        outcome.is_err(),
        "R3 REQUIRED refusal of a correct-index/wrong-membership image before \
         publication; the install succeeded (membership is never compared, \
         and the in-memory membership is then set from the meta)"
    );
    drop(machine);
}

/// R3: after a rejected install the group recovers — reopening the store and
/// rebuilding the state machine must find the previous generation and state
/// intact. At 331c18f the wrong-group attempt above already replaced the
/// snapshot files, so the previous generation is gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r3_rejected_install_leaves_previous_usable_after_restart() {
    let (dir, mut machine, image_a, _meta_a) = machine_with_generation("r3-restart").await;

    // Same wrong-group attempt as r3_wrong_group: refused, but late.
    let dir_b = kit::TestDir::new("r3-restart-b");
    let store_b = pipestream_search::source_authority::SourceAuthorityStore::create(
        &dir_b.authority(),
        &kit::identity(9),
        &kit::policy(),
        &kit::limits(),
    )
    .unwrap();
    let mut machine_b =
        ControlStateMachine::new(store_b, &dir_b.path().join("raft-snapshots")).unwrap();
    RaftStateMachine::apply(&mut machine_b, vec![prepare_entry(1)])
        .await
        .unwrap();
    let (image_b, meta_b) = raft_kit::build_current(&mut machine_b).await;
    drop(machine_b);
    let _ = raft_kit::install_received(&mut machine, &image_b, &meta_b).await;
    drop(machine);

    // Restart: reopen the store and rebuild the machine over the same dir.
    let reopened = kit::open_store(&dir, &kit::identity(kit::SEED));
    let mut rebuilt =
        ControlStateMachine::new(reopened, &dir.path().join("raft-snapshots")).unwrap();
    let mut findings: Vec<String> = Vec::new();
    match served(&mut rebuilt).await {
        Some((bytes, _)) => {
            if bytes != image_a {
                findings.push(format!(
                    "after restart the served snapshot is {} bytes, not the \
                     preserved {}-byte previous generation",
                    bytes.len(),
                    image_a.len()
                ));
            }
        }
        None => findings.push("get_current_snapshot refuses after restart".to_string()),
    }
    let store = rebuilt.shared_store().read().unwrap().clone().unwrap();
    if store
        .owner("alice", &kit::owner_key())
        .err()
        .is_some_and(|e| e.code() != tonic::Code::NotFound)
    {
        findings.push("the reopened store does not serve the committed owner".to_string());
    }
    assert!(
        findings.is_empty(),
        "R3 REQUIRED the previous generation and store state to survive a \
         rejected install across restart; observed:\n  - {}",
        findings.join("\n  - ")
    );
    drop(store);
    drop(rebuilt);
    drop(dir);
}

/// R3: an interrupted publication must never pair mismatched image/meta
/// silently. Each case starts from a good generation; per case the state must
/// either serve the COMPLETE previous generation or refuse loudly by name.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r3_interrupted_publication_boundaries() {
    let snapshots = |dir: &kit::TestDir| dir.path().join("raft-snapshots");

    // (a) current.redb replaced with newer bytes, meta still the old one
    // (builder interruption between the rename and the meta publish).
    {
        let (dir, mut machine, image_a, meta_a) = machine_with_generation("r3-int-a").await;
        let dir_b = kit::TestDir::new("r3-int-a-b");
        let store_b = pipestream_search::source_authority::SourceAuthorityStore::create(
            &dir_b.authority(),
            &kit::identity(9),
            &kit::policy(),
            &kit::limits(),
        )
        .unwrap();
        let mut machine_b =
            ControlStateMachine::new(store_b, &dir_b.path().join("raft-snapshots")).unwrap();
        RaftStateMachine::apply(&mut machine_b, vec![prepare_entry(1)])
            .await
            .unwrap();
        let (image_b, _meta_b) = raft_kit::build_current(&mut machine_b).await;
        drop(machine_b);
        std::fs::write(snapshots(&dir).join("current.redb"), &image_b).unwrap();
        match served(&mut machine).await {
            None => {} // refused loudly: the meta/image signatures disagree
            Some((bytes, meta)) => assert!(
                bytes == image_a && meta.snapshot_id == meta_a.snapshot_id,
                "(a) silently served a mixed generation"
            ),
        }
        drop(machine);
        drop(dir);
    }

    // (b) a leftover current.meta.tmp must not disturb the published pair.
    {
        let (dir, mut machine, image_a, meta_a) = machine_with_generation("r3-int-b").await;
        std::fs::write(snapshots(&dir).join("current.meta.tmp"), b"garbage").unwrap();
        let (bytes, meta) = served(&mut machine)
            .await
            .expect("(b) the leftover tmp file disturbed the published pair");
        assert!(
            bytes == image_a && meta.snapshot_id == meta_a.snapshot_id,
            "(b) served something other than the complete previous generation"
        );
        drop(machine);
        drop(dir);
    }

    // (c) a leftover staged.redb from an interrupted install is inert.
    {
        let (dir, mut machine, image_a, meta_a) = machine_with_generation("r3-int-c").await;
        std::fs::write(snapshots(&dir).join("staged.redb"), &image_a).unwrap();
        let (bytes, meta) = served(&mut machine)
            .await
            .expect("(c) the leftover staged file disturbed the published pair");
        assert!(
            bytes == image_a && meta.snapshot_id == meta_a.snapshot_id,
            "(c) served something other than the complete previous generation"
        );
        drop(machine);
        drop(dir);
    }

    // (d1) meta claiming a newer index while the image is still the old
    // bytes (interrupted meta publish): the id no longer matches the image
    // signature, so this must refuse loudly.
    {
        let (dir, mut machine, _image_a, _meta_a) = machine_with_generation("r3-int-d1").await;
        let mut pb = meta_pb(&dir);
        pb.last_log_id.as_mut().unwrap().index += 1;
        write_meta_pb(&dir, &pb);
        match served(&mut machine).await {
            None => {}
            Some(_) => panic!("(d1) silently served an image/meta pair whose id disagrees"),
        }
        drop(machine);
        drop(dir);
    }

    // (d2) a forged generation pointer: meta claiming a newer index whose id
    // IS recomputed against the old image bytes. The pair is self-consistent
    // by signature but the content is not the announced generation, and the
    // stored group is never checked — R3 requires validation before
    // publication, not name-matching alone.
    {
        let (dir, mut machine, image_a, _meta_a) = machine_with_generation("r3-int-d2").await;
        let mut pb = meta_pb(&dir);
        let index = pb.last_log_id.as_ref().unwrap().index + 1;
        pb.last_log_id.as_mut().unwrap().index = index;
        let digest = pipestream_search::sha256::digest(&image_a);
        pb.snapshot_id = format!(
            "{}-{}-{}",
            index,
            image_a.len(),
            pipestream_search::sha256::to_hex(&digest)
        );
        write_meta_pb(&dir, &pb);
        assert!(
            served(&mut machine).await.is_none(),
            "R3 REQUIRED a forged generation pointer (old image bytes under a \
             recomputed newer-index meta) to be refused; it was served \
             silently — the reader pairs names, not validated content"
        );
        drop(machine);
        drop(dir);
    }
}

/// R3: a reader held across a successful publication must keep its
/// consistent old view (or the install must refuse by name — either is
/// acceptable; corruption is not). At 331c18f the install copies the new
/// image over current.redb IN PLACE, so a file opened on the previous
/// generation observes the overwritten bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r3_reader_held_across_publication() {
    let (_dir_a, mut machine, _image_a1, _meta_a1) = machine_with_generation("r3-reader-a").await;
    // Hold the served generation-1 file open and remember its bytes.
    let mut held = RaftStateMachine::get_current_snapshot(&mut machine)
        .await
        .unwrap()
        .expect("a generation exists")
        .snapshot;
    let mut before = Vec::new();
    held.read_to_end(&mut before).await.unwrap();

    // A valid second generation of the SAME group at applied index 2.
    let dir_b = kit::TestDir::new("r3-reader-b");
    let (store_b, _authority) = kit::create_store(&dir_b);
    let mut machine_b =
        ControlStateMachine::new(store_b, &dir_b.path().join("raft-snapshots")).unwrap();
    RaftStateMachine::apply(&mut machine_b, vec![prepare_entry(1), prepare_entry(2)])
        .await
        .unwrap();
    let (image_b, meta_b) = raft_kit::build_current(&mut machine_b).await;
    drop(machine_b);

    let installed = raft_kit::install_received(&mut machine, &image_b, &meta_b).await;
    if installed.is_ok() {
        // REQUIRED: the held reader still sees its original bytes.
        held.seek(SeekFrom::Start(0)).await.unwrap();
        let mut after = Vec::new();
        held.read_to_end(&mut after).await.unwrap();
        assert_eq!(
            after, before,
            "R3 REQUIRED a reader held across publication to keep its \
             consistent old view; the file now returns different bytes — the \
             install overwrote current.redb in place under an open reader"
        );
    }
    // If the install refused by name (the other acceptable outcome), nothing
    // else to check: the held bytes are untouched by definition.
    drop(held);
    drop(machine);
}

/// R5: the install must consume the actual receive, not the newest
/// incoming-* filename. An abandoned file with a deliberately later name must
/// not be selected. At 331c18f `newest_incoming` picks by directory-name
/// order, so the abandoned garbage is hashed and the install fails (or worse,
/// a valid foreign file would install).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r5_install_uses_the_actual_receive_not_the_newest_filename() {
    let (dir, mut machine, _image, _meta) = machine_with_generation("r5-newest").await;
    // A valid same-group image at applied index 2, to receive for real.
    let dir_b = kit::TestDir::new("r5-newest-b");
    let (store_b, _authority) = kit::create_store(&dir_b);
    let mut machine_b =
        ControlStateMachine::new(store_b, &dir_b.path().join("raft-snapshots")).unwrap();
    RaftStateMachine::apply(&mut machine_b, vec![prepare_entry(1), prepare_entry(2)])
        .await
        .unwrap();
    let (image_b, meta_b) = raft_kit::build_current(&mut machine_b).await;
    drop(machine_b);

    // Begin the real receive and write the real bytes into THAT file.
    let mut receive = RaftStateMachine::begin_receiving_snapshot(&mut machine)
        .await
        .unwrap();
    use tokio::io::AsyncWriteExt;
    receive.write_all(&image_b).await.unwrap();

    // Plant an abandoned incoming file whose name sorts AFTER the real one.
    // The real receive was opened before `now`, so `now + 1e15` (still
    // 16 digits, still lexicographically later) cannot collide with it.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    let planted = dir
        .path()
        .join("raft-snapshots")
        .join(format!("incoming-{}.redb", now + 1_000_000_000_000_000));
    std::fs::write(&planted, vec![0xABu8; 4096]).unwrap();

    let outcome = RaftStateMachine::install_snapshot(&mut machine, &meta_b, receive)
        .await
        .map_err(|e| e.to_string());
    match outcome {
        Ok(()) => {
            // REQUIRED beyond success: the actual receive was installed and
            // the abandoned file was left alone (only its own receive may
            // clean it).
            assert!(
                planted.exists(),
                "the abandoned incoming file disappeared without its own receive"
            );
            let (applied, _) = RaftStateMachine::applied_state(&mut machine).await.unwrap();
            assert_eq!(
                applied.map(|id| id.index),
                meta_b.last_log_id.map(|id| id.index),
                "the installed applied position is not the received image's"
            );
        }
        Err(message) => panic!(
            "R5 REQUIRED the install to consume the actual receive; the \
             newest-filename heuristic selected the abandoned file instead: \
             {message}"
        ),
    }
    drop(machine);
    drop(dir);
}

/// R5: interrupted receives must never participate in a later install.
/// (i) a plain leftover partial from a crashed receive (earlier name) is
/// accidentally harmless at 331c18f; (ii) a leftover partial with a
/// deliberately LATER name is selected and must not be.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r5_interrupted_receive_leaves_no_selected_artifact() {
    let snapshots = |dir: &kit::TestDir| dir.path().join("raft-snapshots");

    // (i) plain leftover: fresh receive wins by name at 331c18f.
    {
        let (dir, mut machine, _image, _meta) = machine_with_generation("r5-partial-i").await;
        let dir_b = kit::TestDir::new("r5-partial-i-b");
        let (store_b, _authority) = kit::create_store(&dir_b);
        let mut machine_b =
            ControlStateMachine::new(store_b, &dir_b.path().join("raft-snapshots")).unwrap();
        RaftStateMachine::apply(&mut machine_b, vec![prepare_entry(1), prepare_entry(2)])
            .await
            .unwrap();
        let (image_b, meta_b) = raft_kit::build_current(&mut machine_b).await;
        drop(machine_b);

        // Interrupted receive: partial bytes, dropped without install.
        let mut partial = RaftStateMachine::begin_receiving_snapshot(&mut machine)
            .await
            .unwrap();
        use tokio::io::AsyncWriteExt;
        partial
            .write_all(&image_b[..image_b.len() / 2])
            .await
            .unwrap();
        drop(partial);
        tokio::time::sleep(Duration::from_millis(5)).await;

        // Fresh receive and install must succeed: the partial is older by name.
        raft_kit::install_received(&mut machine, &image_b, &meta_b)
            .await
            .expect("(i) a plain leftover partial broke a later install");
        drop(machine);
        drop(dir);
    }

    // (ii) leftover partial with a LATER name: at 331c18f it is selected.
    {
        let (dir, mut machine, _image, _meta) = machine_with_generation("r5-partial-ii").await;
        let dir_b = kit::TestDir::new("r5-partial-ii-b");
        let (store_b, _authority) = kit::create_store(&dir_b);
        let mut machine_b =
            ControlStateMachine::new(store_b, &dir_b.path().join("raft-snapshots")).unwrap();
        RaftStateMachine::apply(&mut machine_b, vec![prepare_entry(1), prepare_entry(2)])
            .await
            .unwrap();
        let (image_b, meta_b) = raft_kit::build_current(&mut machine_b).await;
        drop(machine_b);

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        std::fs::write(
            snapshots(&dir).join(format!("incoming-{}.redb", now + 1_000_000_000_000_000)),
            vec![0xCDu8; 8192],
        )
        .unwrap();
        let outcome = raft_kit::install_received(&mut machine, &image_b, &meta_b).await;
        assert!(
            outcome.is_ok(),
            "R5 REQUIRED a later valid receive to win over an abandoned \
             artifact; the newest-filename heuristic selected the planted \
             partial instead: {}",
            outcome.err().unwrap_or_default()
        );
        drop(machine);
        drop(dir);
    }
}

/// R5: announced lengths and digests that do not match the received bytes
/// must refuse before anything is replaced, leaving the previous generation
/// intact. (The bounded-allocation half of R5 is the next test.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r5_oversize_and_truncated_images_refuse_before_replacement() {
    // (i) an announced length larger than the received image.
    {
        let (dir, mut machine, image, meta) = machine_with_generation("r5-oversize").await;
        let digest = pipestream_search::sha256::digest(&image);
        let mut announced = raft_kit::meta_for(1, &image, &[1]);
        announced.snapshot_id = format!(
            "1-{}-{}",
            image.len() + 100,
            pipestream_search::sha256::to_hex(&digest)
        );
        let outcome = raft_kit::install_received(&mut machine, &image, &announced).await;
        assert!(
            outcome.is_err(),
            "an oversize announcement must refuse, installed instead"
        );
        let served_now = served(&mut machine)
            .await
            .expect("previous generation intact");
        assert_eq!(served_now.1.snapshot_id, meta.snapshot_id);
        drop(machine);
        drop(dir);
    }

    // (ii) a truncated receive against a full-image announcement.
    {
        let (dir, mut machine, image, meta) = machine_with_generation("r5-truncated").await;
        let outcome =
            raft_kit::install_received(&mut machine, &image[..image.len() / 2], &meta).await;
        assert!(
            outcome.is_err(),
            "a truncated image must refuse, installed instead"
        );
        let served_now = served(&mut machine)
            .await
            .expect("previous generation intact");
        assert_eq!(served_now.1.snapshot_id, meta.snapshot_id);
        drop(machine);
        drop(dir);
    }
}

/// R5 (API gap): the REQUIRED "enforce announced lengths WHILE receiving"
/// has no surface — the receive sink is an unbounded `File`. This test pins
/// what is reachable: bytes beyond the announcement are accepted by the sink
/// and the refusal happens late, at install. It passes at 331c18f; the gap is
/// the missing bounded receive sink, documented here and in the harness doc.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r5_announced_length_is_checked_only_at_install() {
    let (dir, mut machine, image, meta) = machine_with_generation("r5-late-check").await;
    let mut receive = RaftStateMachine::begin_receiving_snapshot(&mut machine)
        .await
        .unwrap();
    use tokio::io::AsyncWriteExt;
    // The sink accepts arbitrarily more than the announced image.
    receive.write_all(&image).await.unwrap();
    receive.write_all(&vec![0u8; 1 << 20]).await.unwrap();
    let outcome = RaftStateMachine::install_snapshot(&mut machine, &meta, receive)
        .await
        .map_err(|e| e.to_string());
    assert!(
        outcome.is_err(),
        "at minimum the late check must refuse excess bytes; installed instead"
    );
    drop(machine);
    drop(dir);
}

/// R5: hashing must use bounded buffers, not a whole-file Vec. Installs a
/// multi-MiB valid image and measures peak live allocation attributable to
/// the install; at 331c18f `image_signature` reads the entire image into one
/// Vec, so the peak reaches the image size.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r5_install_hashing_is_bounded() {
    // A store whose image is several MiB: distinct prepares accumulate
    // decision records. Custom limits: the kit decision budget is far too
    // small for this many commands.
    let dir_src = kit::TestDir::new("r5-bounded-src");
    let limits = pipestream_search::pb::storage::SourceAuthorityLimits {
        max_owners: 16,
        max_decisions: 1_000_000,
        max_payload_bytes: 64 << 20,
        max_command_bytes: 8 << 10,
    };
    let store = pipestream_search::source_authority::SourceAuthorityStore::create(
        &dir_src.authority(),
        &kit::identity(kit::SEED),
        &kit::policy(),
        &limits,
    )
    .unwrap();
    let mut machine_src =
        ControlStateMachine::new(store, &dir_src.path().join("raft-snapshots")).unwrap();
    let entries: Vec<ControlEntry> = (1..=12_000).map(prepare_entry).collect();
    RaftStateMachine::apply(&mut machine_src, entries)
        .await
        .unwrap();
    let (image, meta) = raft_kit::build_current(&mut machine_src).await;
    assert!(
        image.len() >= 4 << 20,
        "fixture image is {} bytes; raise the prepare count",
        image.len()
    );
    drop(machine_src);

    // Install into a fresh machine of the same group, measuring the peak.
    // Reset the monotonic peak to the current live watermark so growth is
    // attributable to this install alone (the fixture build already pushed
    // the process peak far higher than any single install allocates).
    let dir_dst = kit::TestDir::new("r5-bounded-dst");
    let (store_dst, _authority) = kit::create_store(&dir_dst);
    let mut machine_dst =
        ControlStateMachine::new(store_dst, &dir_dst.path().join("raft-snapshots")).unwrap();
    let watermark = live_bytes();
    PEAK.store(watermark, Ordering::Relaxed);
    let outcome = raft_kit::install_received(&mut machine_dst, &image, &meta).await;
    assert!(outcome.is_ok(), "the valid image must install: {outcome:?}");
    let growth = peak_bytes().saturating_sub(watermark);
    assert!(
        growth < image.len(),
        "R5 REQUIRED bounded-buffer hashing; the install path allocated a \
         peak of {growth} bytes live for a {}-byte image (peak >= image size \
         demonstrates the whole-file Vec read)",
        image.len()
    );
    drop(machine_dst);
    drop(dir_dst);
    drop(dir_src);
}

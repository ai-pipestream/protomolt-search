//! Concurrent first-open of a shard WAL.
//!
//! Production failure this pins down: a floor-sharing node and its twin
//! start near-simultaneously against the same shard files whose
//! `<index>.tv.wal/` directory does not exist yet. Both scanned an absent
//! WAL directory and raced to create generation 0 (or one scanned the
//! gen dir in the middle of the other's create), and the loser panicked
//! with `open WAL at ...: No such file or directory (os error 2)`. Two
//! ENOENT sources, both covered here:
//!
//! - `write_manifest`'s `rename`: both creators shared one tmp name
//!   (`manifest.toml.tmp`); the first rename consumed the path and the
//!   second `rename` failed. (Reproduces on ~every iteration of the
//!   race test below against the pre-fix code.)
//! - `read_manifest`'s `read_to_string`: the loser's generation scan saw
//!   the winner's just-created `gen-000000/` before the winner's
//!   manifest rename landed.
//!
//! The contract after the fix (`wal::open_or_create`, the spine of
//! `node::open_wal`): any number of concurrent first-opens succeed,
//! every opener gets a valid writer onto the SAME generation, and the
//! on-disk result is exactly one well-formed generation.

use std::path::{Path, PathBuf};
use std::sync::Barrier;

use turbovec_search::wal::{self, WalManifest, WalWriter};

const ITERATIONS: usize = 200;
const OPENERS: usize = 2;

fn fresh_dir(tag: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "wal_concurrent_{tag}_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::remove_dir_all(&dir).ok();
    dir
}

fn manifest() -> WalManifest {
    WalManifest {
        dim: 8,
        bit_width: 4,
        calibration_shift: vec![0.0; 8],
        calibration_scale: vec![1.0; 8],
        slot_offset: 25_000_000,
        generation: 0,
        bucket_bits: 2,
        bucket_count: 4,
        preexisting_vectors: 10_829_824,
        preexisting_documents: 10_829_824,
        format_version: wal::FORMAT_VERSION,
    }
}

/// The node WAL-init path (`node::open_wal`'s wal.rs spine), as both
/// racing nodes run it.
fn open_like_node(wal_dir: &Path, fresh: &WalManifest) -> std::io::Result<WalWriter> {
    wal::open_or_create(wal_dir, 0, fresh.clone())
}

/// `openers` threads start barrier-synchronized against `wal_dir`; every
/// one must come back with a usable writer on generation 0.
fn concurrent_open(wal_dir: &Path, openers: usize, manifest: &WalManifest) -> Vec<WalWriter> {
    let barrier = std::sync::Arc::new(Barrier::new(openers));
    let handles: Vec<_> = (0..openers)
        .map(|_| {
            let barrier = barrier.clone();
            let wal_dir = wal_dir.to_path_buf();
            let manifest = manifest.clone();
            std::thread::spawn(move || {
                barrier.wait();
                open_like_node(&wal_dir, &manifest)
            })
        })
        .collect();
    handles
        .into_iter()
        .map(|h| {
            let mut w = h.join().unwrap().expect("concurrent WAL open failed");
            assert_eq!(w.generation(), 0);
            w.flush().expect("flush on the opened writer");
            w
        })
        .collect()
}

/// One fully-formed generation 0 on disk, and nothing half-made.
fn assert_one_wellformed_generation(wal_dir: &Path, expected: &WalManifest) {
    let gens: Vec<_> = std::fs::read_dir(wal_dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(gens, ["gen-000000"], "entries in {}", wal_dir.display());
    let gen = wal_dir.join("gen-000000");
    let on_disk = wal::read_manifest(&gen).unwrap();
    assert_eq!(&on_disk, expected);
    // The markers file exists and scans clean (empty log, no torn tail).
    let scan = wal::scan_records(&wal::markers_path(&gen)).unwrap();
    assert_eq!((scan.last_seq, scan.valid_len), (0, 0));
    // No orphaned manifest tmp files from the rename race.
    let leftovers: Vec<_> = std::fs::read_dir(&gen)
        .unwrap()
        .filter_map(|e| {
            let name = e.unwrap().file_name().to_string_lossy().into_owned();
            name.contains(".tmp").then_some(name)
        })
        .collect();
    assert!(leftovers.is_empty(), "tmp leftovers: {leftovers:?}");
}

/// The fleet failure: two openers race the FIRST creation of the WAL
/// over a shard whose `<index>.wal/` parent chain does not exist at
/// all. Looped over fresh directories: the pre-fix code loses the
/// rename race on the shared manifest tmp name on most iterations.
#[test]
fn concurrent_first_open_converges_on_one_generation() {
    let root = fresh_dir("race");
    for iteration in 0..ITERATIONS {
        let wal_dir = root
            .join(format!("iter{iteration}"))
            .join("shard.tv.wal");
        let writers = concurrent_open(&wal_dir, OPENERS, &manifest());
        drop(writers);
        assert_one_wellformed_generation(&wal_dir, &manifest());
    }
    std::fs::remove_dir_all(&root).ok();
}

/// Same race with the loser arriving late enough to see the winner's
/// generation directory but (sometimes) not yet its manifest: the
/// resume arm of the open must wait out the mid-creation window, not
/// fail on the missing manifest or markers.
#[test]
fn open_racing_a_half_made_generation_waits_for_it() {
    let root = fresh_dir("halfmade");
    for iteration in 0..ITERATIONS {
        let wal_dir = root.join(format!("iter{iteration}")).join("shard.tv.wal");
        // The half-made state: gen dir and manifest exist, markers does
        // not — exactly what a scanner can see mid-create.
        let gen = wal::gen_dir(&wal_dir, 0);
        std::fs::create_dir_all(&gen).unwrap();
        wal::write_manifest(&gen, &manifest()).unwrap();
        let writers = concurrent_open(&wal_dir, OPENERS, &manifest());
        drop(writers);
        assert_one_wellformed_generation(&wal_dir, &manifest());
    }
    std::fs::remove_dir_all(&root).ok();
}

/// Both openers resume an already-complete generation: the steady-state
/// restart of a twin pair, concurrently.
#[test]
fn concurrent_open_of_existing_generation_resumes_for_all() {
    let wal_dir = fresh_dir("existing").join("shard.tv.wal");
    let first = wal::open_or_create(&wal_dir, 0, manifest()).unwrap();
    drop(first);
    let writers = concurrent_open(&wal_dir, OPENERS, &manifest());
    drop(writers);
    assert_one_wellformed_generation(&wal_dir, &manifest());
    std::fs::remove_dir_all(wal_dir.parent().unwrap()).ok();
}

/// The create side alone must build the whole parent chain
/// (`<index>.wal/` included) when none of it exists.
#[test]
fn first_open_creates_a_missing_parent_chain() {
    let wal_dir = fresh_dir("deep")
        .join("missing")
        .join("chain")
        .join("shard.tv.wal");
    let writer = wal::open_or_create(&wal_dir, 0, manifest()).unwrap();
    assert_eq!(writer.generation(), 0);
    drop(writer);
    assert_one_wellformed_generation(&wal_dir, &manifest());
    std::fs::remove_dir_all(wal_dir.parent().unwrap().parent().unwrap().parent().unwrap()).ok();
}

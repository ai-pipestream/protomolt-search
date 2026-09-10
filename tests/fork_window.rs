//! The fork window at an integration test binary, through a production
//! lock path. A child spawned by one test thread keeps duplicates of the
//! parent's descriptors between fork and exec, so a lock a sibling test
//! drops in that window stays in force until the child's close-on-exec
//! sweep, and the lib test binary closes the window with two layers
//! (`pipestream_search::test_support`). This binary links the library the
//! way every integration target does, without `cfg(test)`, so it proves the
//! `fork-guard` feature carries both layers there: the control ownership
//! lock take calls the handoff, and the spawn goes through the guard and
//! the pre-exec sweep. The control ownership lock is the one the crash
//! target saw rejected; the source authority and raft log store locks are
//! taken the same way. The document catalog's lock is not subject to the
//! window: redb unlocks its file explicitly when the database is dropped,
//! which releases the lock for every duplicate of the description.
//!
//! The window is a few tens of microseconds wide (a probe on 2026-09-09:
//! the fork some 4 µs after `Command::status` is entered, the child's sweep
//! some 40 µs after that), so the rounds are arranged to land the re-take
//! inside it with two pipes: the child announces itself from a pre-exec
//! hook, after the fork and before the exec, and waits there; the holder
//! drops its catalog on the announcement, tells the child, and reopens the
//! catalog; the child goes on to its exec. With the guard absent from the
//! spawn the reopen is rejected on every round (the evidence is in the
//! harness document); with it, the reopen waits for the spawn to return, by
//! which time the child has closed the inherited file, and it is never
//! rejected.
use std::io::{Read, Write};
use std::os::fd::FromRawFd;
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use pipestream_search::control_plane::{ControlPolicy, DurableControlPlane};
use pipestream_search::test_support::ForkGuarded;

struct Directory(PathBuf);

impl Directory {
    fn new() -> Self {
        let path = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
            "fork-window-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn state(&self) -> PathBuf {
        self.0.join("legacy.json")
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

fn wait_for(counter: &AtomicU64, value: u64) {
    while counter.load(Ordering::Acquire) < value {
        std::hint::spin_loop();
    }
}

/// A close-on-exec pipe, as raw descriptors: a child uses one end from a
/// pre-exec hook, after the fork and before the exec. Pipes are not
/// regular files, so the guard's sweep leaves them for the hook.
fn pipe() -> (i32, i32) {
    let mut ends = [0i32; 2];
    // SAFETY: `pipe2` writes two descriptors into the array it is given.
    let made = unsafe { libc::pipe2(ends.as_mut_ptr(), libc::O_CLOEXEC) };
    assert_eq!(made, 0, "pipe: {}", std::io::Error::last_os_error());
    (ends[0], ends[1])
}

/// One byte over a pipe, raw: for the hook in the child, which makes
/// system calls only.
fn write_byte(descriptor: i32) -> std::io::Result<()> {
    let byte = [1u8];
    // SAFETY: a write of one byte from a stack buffer to a descriptor this
    // process owns.
    if unsafe { libc::write(descriptor, byte.as_ptr().cast(), 1) } == 1 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

fn read_byte(descriptor: i32) -> std::io::Result<()> {
    let mut byte = [0u8];
    // SAFETY: a read of one byte into a stack buffer from a descriptor this
    // process owns.
    if unsafe { libc::read(descriptor, byte.as_mut_ptr().cast(), 1) } == 1 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

/// Round after round: the holder opens the control plane, which takes the
/// control ownership lock through the library's own lock code; the
/// spawner, told the lock is held, spawns a child under the guard; the
/// child announces itself between fork and exec and waits; the holder, on
/// the announcement, drops the plane, releases the child and reopens the
/// plane at once. A rejected reopen is the window.
#[test]
fn a_control_lock_dropped_while_a_child_is_between_fork_and_exec_is_free() {
    const ROUNDS: u64 = 200;
    let dir = Directory::new();
    drop(DurableControlPlane::open(dir.state(), ControlPolicy::default()).unwrap());
    let (announced, announce) = pipe();
    let (released, release) = pipe();
    // SAFETY: each descriptor is owned by this process and handed over once.
    let mut announcements = unsafe { std::fs::File::from_raw_fd(announced) };
    let mut releases = unsafe { std::fs::File::from_raw_fd(release) };
    // Round numbers, each advanced by one side when its step is done.
    let held = Arc::new(AtomicU64::new(0));
    let spawned = Arc::new(AtomicU64::new(0));
    let spawner = {
        let held = Arc::clone(&held);
        let spawned = Arc::clone(&spawned);
        std::thread::spawn(move || {
            for round in 1..=ROUNDS {
                wait_for(&held, round);
                let mut command = Command::new("/bin/true");
                // SAFETY: raw system calls on pipes and no allocation, which
                // a forked child of a multithreaded process may do before
                // exec.
                unsafe {
                    command.pre_exec(move || {
                        write_byte(announce)?;
                        read_byte(released)
                    });
                }
                let status = command.status_guarded().unwrap();
                assert!(status.success());
                spawned.store(round, Ordering::Release);
            }
        })
    };
    let mut byte = [0u8; 1];
    for round in 1..=ROUNDS {
        let plane = DurableControlPlane::open_existing(dir.state(), ControlPolicy::default())
            .unwrap_or_else(|e| panic!("round {round}: open rejected: {e}"));
        held.store(round, Ordering::Release);
        announcements.read_exact(&mut byte).unwrap();
        drop(plane);
        releases.write_all(&[1]).unwrap();
        let again = DurableControlPlane::open_existing(dir.state(), ControlPolicy::default())
            .unwrap_or_else(|e| panic!("round {round}: reopen rejected: {e}"));
        wait_for(&spawned, round);
        drop(again);
    }
    spawner.join().unwrap();
    // SAFETY: the raw ends are closed once, here, after the last child.
    unsafe {
        libc::close(announce);
        libc::close(released);
    }
}

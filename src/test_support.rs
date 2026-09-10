//! Test-only process hygiene for the exclusive file locks.
//!
//! `flock` locks belong to an open file description. Between `fork` and
//! `exec`, a child process keeps duplicates of every descriptor of the test
//! process, so a lock the parent releases in that window stays in force until
//! the child's close-on-exec sweep. A test that drops its last handle and
//! reopens the file could then see "would block" only because a sibling
//! test was spawning a worker at that instant.
//!
//! Two layers close the window, and both are needed:
//!
//! - Workers spawn under the write side of one process-wide guard, and
//!   every lock acquisition takes the read side, so an open does not overlap
//!   a fork.
//! - The child closes its inherited regular-file descriptors before it
//!   execs (`close_regular_files_after_fork`). `Command::spawn` returning
//!   does not mean the child has finished its close-on-exec sweep: with
//!   the `clone(CLONE_VFORK)` spawn glibc uses, the kernel resumes the
//!   parent when the child gives up the shared address space, which is
//!   before the sweep. On 2026-09-09 (kernel 7.0, glibc 2.43) a probe in
//!   the shape of a test (hold, spawn under the guard, drop, re-take
//!   through a fresh open) saw the re-take rejected 6 times in 120,000
//!   rounds, with the parent's file still in the child's table after
//!   `spawn` returned. With the descriptors closed before exec, a `spawn`
//!   that returned means the descriptions are closed, however the parent
//!   was resumed: the fork path std uses when a pre-exec hook is set
//!   reports over a pipe the hook leaves open. The same probe with the
//!   hook: 0 in 160,000.
//!
//! Regular files only: locks live on regular files, and the pipes std and
//! tokio hold must survive (std reports an exec failure over one). The
//! hook runs in the child of a multithreaded process, so it makes raw
//! system calls and makes no allocation.
//!
//! The lib test binary gets both layers under `cfg(test)`. An integration
//! test binary links the library without `cfg(test)`, so the `fork-guard`
//! feature compiles the handoff into the lock takes and makes this module
//! public; the crate's dev-dependency on itself turns the feature on for
//! every test build and for no serving binary. A test binary that spawns
//! this same executable (the crash harnesses under `tests/`) spawns through
//! [`ForkGuarded`]; `tests/fork_window.rs` pins the window through the
//! document catalog's lock.
use std::process::{Child, Command, ExitStatus, Output, Stdio};
use std::sync::{RwLock, RwLockReadGuard};

static FORK_GUARD: RwLock<()> = RwLock::new(());

/// Held by every lock acquisition while it takes the lock.
pub fn lock_handoff() -> RwLockReadGuard<'static, ()> {
    FORK_GUARD
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// `Command::spawn` / `Command::status` / `Command::output` with the
/// fork-to-exec window under the write guard and the inherited regular
/// files closed before exec.
pub trait ForkGuarded {
    fn spawn_guarded(&mut self) -> std::io::Result<Child>;
    fn status_guarded(&mut self) -> std::io::Result<ExitStatus>;
    fn output_guarded(&mut self) -> std::io::Result<Output>;
}

fn spawn_guarded(command: &mut Command) -> std::io::Result<Child> {
    let _fork = FORK_GUARD
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::process::CommandExt;
        // SAFETY: the hook makes no allocation and makes raw system calls
        // only, which is what a child of a multithreaded process may do
        // between fork and exec.
        unsafe {
            command.pre_exec(close_regular_files_after_fork);
        }
    }
    // The guard covers all of `spawn`: the child's inherited
    // descriptors are closed by the hook before the parent resumes.
    command.spawn()
}

impl ForkGuarded for Command {
    fn spawn_guarded(&mut self) -> std::io::Result<Child> {
        spawn_guarded(self)
    }

    fn status_guarded(&mut self) -> std::io::Result<ExitStatus> {
        spawn_guarded(self)?.wait()
    }

    fn output_guarded(&mut self) -> std::io::Result<Output> {
        self.stdout(Stdio::piped()).stderr(Stdio::piped());
        spawn_guarded(self)?.wait_with_output()
    }
}

/// Descriptors a child may have: past this the hook rejects (`EMFILE`)
/// instead of leaving one open.
#[cfg(target_os = "linux")]
const MAX_CHILD_DESCRIPTORS: usize = 4096;

/// Close every inherited descriptor from 3 up that is a regular file.
/// Runs in the child between fork and exec: `/proc/self/fd` is read with
/// `getdents64` into a fixed buffer, the names are decimal descriptor
/// numbers parsed by hand, and each regular file is closed. Errors are
/// raw `errno` values, since building an error message would allocate.
#[cfg(target_os = "linux")]
fn close_regular_files_after_fork() -> std::io::Result<()> {
    use std::io::Error;
    // SAFETY: raw system calls on descriptors this process owns; the
    // buffer is read only within the byte count `getdents64` returned and
    // each record's name is read up to its terminating zero, which the
    // kernel writes within the record.
    unsafe {
        let dir = libc::open(
            c"/proc/self/fd".as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
        );
        if dir < 0 {
            return Err(Error::last_os_error());
        }
        let mut descriptors = [0i32; MAX_CHILD_DESCRIPTORS];
        let mut count = 0usize;
        let mut buffer = [0u8; 8192];
        loop {
            let read = libc::syscall(libc::SYS_getdents64, dir, buffer.as_mut_ptr(), buffer.len());
            if read < 0 {
                let error = Error::last_os_error();
                libc::close(dir);
                return Err(error);
            }
            if read == 0 {
                break;
            }
            let mut offset = 0usize;
            while offset < read as usize {
                // struct linux_dirent64: d_ino u64, d_off i64, d_reclen u16,
                // d_type u8, then the zero-terminated name.
                let record = buffer.as_ptr().add(offset);
                let record_length = u16::from_ne_bytes([*record.add(16), *record.add(17)]) as usize;
                if record_length == 0 {
                    libc::close(dir);
                    return Err(Error::from_raw_os_error(libc::EIO));
                }
                let mut name = record.add(19);
                let mut number: i32 = 0;
                let mut digits = 0usize;
                let mut decimal = true;
                while *name != 0 {
                    let byte = *name;
                    if !byte.is_ascii_digit() || digits >= 9 {
                        decimal = false;
                        break;
                    }
                    number = number * 10 + i32::from(byte - b'0');
                    digits += 1;
                    name = name.add(1);
                }
                if decimal && digits > 0 && number >= 3 && number != dir {
                    if count == MAX_CHILD_DESCRIPTORS {
                        libc::close(dir);
                        return Err(Error::from_raw_os_error(libc::EMFILE));
                    }
                    descriptors[count] = number;
                    count += 1;
                }
                offset += record_length;
            }
        }
        libc::close(dir);
        for &descriptor in &descriptors[..count] {
            let mut stat: libc::stat = std::mem::zeroed();
            if libc::fstat(descriptor, &mut stat) == 0
                && (stat.st_mode & libc::S_IFMT) == libc::S_IFREG
            {
                libc::close(descriptor);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::ForkGuarded;
    use std::fs::File;
    use std::io::Write;
    use std::process::Command;

    /// The child sees no inherited regular file: it lists its own
    /// descriptors and prints the regular ones among them, and there are
    /// none, while the parent's file is open across the spawn.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_spawned_child_has_no_inherited_regular_file() {
        let dir = std::env::temp_dir().join(format!(
            "fork-guard-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&dir).unwrap();
        let mut kept = File::create(dir.join("kept")).unwrap();
        kept.write_all(b"kept across the spawn").unwrap();
        kept.try_lock().unwrap();
        let output = Command::new("/bin/sh")
            .arg("-c")
            .arg("for f in /proc/self/fd/*; do n=${f##*/}; [ \"$n\" -ge 3 ] && [ -f \"$f\" ] && readlink \"$f\"; done; exit 0")
            .output_guarded()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        let listed = String::from_utf8(output.stdout).unwrap();
        assert!(
            listed.trim().is_empty(),
            "the child inherited regular files:\n{listed}"
        );
        // The lock and the file are unchanged in the parent. The re-take
        // goes through the handoff, as the store and control locks do: a
        // sibling test's spawn may be between fork and the hook.
        let again = File::create(dir.join("kept")).unwrap();
        assert!(again.try_lock().is_err(), "the parent's lock was lost");
        drop(kept);
        {
            let _handoff = super::lock_handoff();
            again.try_lock().unwrap();
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// The path a re-take runs while a sibling test spawns workers: hold
    /// a lock, drop the holder and re-take through a fresh open, again and
    /// again while another thread spawns two hundred children under the
    /// guard. The window closed here was a rare one, so the re-takes
    /// number in the thousands.
    #[test]
    fn a_lock_dropped_during_spawns_is_free_at_once() {
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::sync::Arc;
        const SPAWNS: u64 = 200;
        let path = std::env::temp_dir().join(format!(
            "fork-guard-retake-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let spawned = Arc::new(AtomicU64::new(0));
        let spawner = {
            let spawned = Arc::clone(&spawned);
            std::thread::spawn(move || {
                for _ in 0..SPAWNS {
                    let status = Command::new("/bin/true").status_guarded().unwrap();
                    assert!(status.success());
                    spawned.fetch_add(1, Ordering::Relaxed);
                }
            })
        };
        let mut rounds = 0u64;
        while spawned.load(Ordering::Relaxed) < SPAWNS {
            // Each take under the handoff, as the store and control locks
            // take it; the drops are outside the guard, as theirs are.
            let holder = File::create(&path).unwrap();
            {
                let _handoff = super::lock_handoff();
                holder
                    .try_lock()
                    .unwrap_or_else(|e| panic!("round {rounds}: take rejected: {e}"));
            }
            drop(holder);
            let again = File::create(&path).unwrap();
            {
                let _handoff = super::lock_handoff();
                again
                    .try_lock()
                    .unwrap_or_else(|e| panic!("round {rounds}: re-take rejected: {e}"));
            }
            rounds += 1;
        }
        spawner.join().unwrap();
        assert!(rounds >= SPAWNS, "{rounds} re-takes during {SPAWNS} spawns");
        std::fs::remove_file(&path).unwrap();
    }
}

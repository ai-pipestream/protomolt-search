//! Test-only process hygiene for the exclusive file locks.
//!
//! `flock` locks belong to an open file description. Between `fork` and
//! `exec`, a child process holds duplicates of every descriptor of the test
//! process, so a lock the parent releases in that window stays held until
//! the child execs. A test that drops its last handle and reopens the file
//! could then see "would block" only because a sibling test was spawning a
//! worker at that instant. Workers therefore spawn under the write side of
//! one process-wide guard, and every lock acquisition takes the read side:
//! an open never overlaps a fork, so a released lock is free when the open
//! looks.
use std::process::{Child, Command, ExitStatus, Output, Stdio};
use std::sync::{RwLock, RwLockReadGuard};

static FORK_GUARD: RwLock<()> = RwLock::new(());

/// Held by every lock acquisition while it takes the lock.
pub(crate) fn lock_handoff() -> RwLockReadGuard<'static, ()> {
    FORK_GUARD
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// `Command::status` / `Command::output` with the fork-to-exec window under
/// the write guard.
pub(crate) trait ForkGuarded {
    fn status_guarded(&mut self) -> std::io::Result<ExitStatus>;
    fn output_guarded(&mut self) -> std::io::Result<Output>;
}

fn spawn_guarded(command: &mut Command) -> std::io::Result<Child> {
    let _fork = FORK_GUARD
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    // `spawn` returns after the child has exec'd (or failed to), so the
    // inherited descriptors are closed when the guard drops.
    command.spawn()
}

impl ForkGuarded for Command {
    fn status_guarded(&mut self) -> std::io::Result<ExitStatus> {
        spawn_guarded(self)?.wait()
    }

    fn output_guarded(&mut self) -> std::io::Result<Output> {
        self.stdout(Stdio::piped()).stderr(Stdio::piped());
        spawn_guarded(self)?.wait_with_output()
    }
}

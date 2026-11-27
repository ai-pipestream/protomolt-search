//! A fault-injection gate inside a catalog write: the write parks at a
//! named point of its transaction until the test releases it, so a test
//! orders "the write is in flight" against another event without a
//! wall-clock sleep.
use std::sync::{Condvar, Mutex};
use std::time::Duration;

use tonic::Status;

/// How long a parked write waits for its release before it fails by
/// name. A gate that is never released is a broken test, never a write
/// that may proceed.
const RELEASE_CEILING: Duration = Duration::from_secs(60);

/// Armed with `arm_precommit_gate`: the next write parks inside its
/// transaction, after every table write and before its final admission
/// check, until [`release`](Self::release).
pub struct WriteGate {
    reached: tokio::sync::watch::Sender<bool>,
    released: Mutex<bool>,
    changed: Condvar,
}

impl WriteGate {
    pub(crate) fn new() -> Self {
        Self {
            reached: tokio::sync::watch::Sender::new(false),
            released: Mutex::new(false),
            changed: Condvar::new(),
        }
    }

    /// Resolves once a write is parked at the gate.
    pub async fn reached(&self) {
        let mut receiver = self.reached.subscribe();
        // The sender lives in `self`, so the channel cannot close while
        // this borrow is held.
        let _ = receiver.wait_for(|reached| *reached).await;
    }

    /// Whether a write is parked at the gate now or has passed it.
    pub fn is_reached(&self) -> bool {
        *self.reached.borrow()
    }

    /// Let the parked write continue. Releasing before the write arrives
    /// lets it pass without parking.
    pub fn release(&self) {
        let mut released = self.released.lock().unwrap_or_else(|e| e.into_inner());
        *released = true;
        self.changed.notify_all();
    }

    /// Park the calling write until release. A gate not released within
    /// the ceiling fails the write by name, leaving no durable change.
    pub(crate) fn park(&self) -> Result<(), Status> {
        self.park_within(RELEASE_CEILING)
    }

    fn park_within(&self, ceiling: Duration) -> Result<(), Status> {
        self.reached.send_replace(true);
        let released = self.released.lock().unwrap_or_else(|e| e.into_inner());
        let (released, _) = self
            .changed
            .wait_timeout_while(released, ceiling, |released| !*released)
            .unwrap_or_else(|e| e.into_inner());
        if *released {
            Ok(())
        } else {
            Err(Status::deadline_exceeded(format!(
                "fault injection: the write gate was not released within {} ms",
                ceiling.as_millis()
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_parked_write_waits_for_its_release() {
        let gate = Arc::new(WriteGate::new());
        assert!(!gate.is_reached());
        let parked = {
            let gate = Arc::clone(&gate);
            std::thread::spawn(move || gate.park())
        };
        gate.reached().await;
        assert!(gate.is_reached());
        assert!(!parked.is_finished(), "the write passed an unreleased gate");
        gate.release();
        parked
            .join()
            .unwrap()
            .expect("a released gate lets the write on");
    }

    #[test]
    fn a_gate_released_first_lets_the_write_pass() {
        let gate = WriteGate::new();
        gate.release();
        gate.park_within(Duration::from_millis(1))
            .expect("an already released gate does not park");
        assert!(gate.is_reached());
    }

    #[test]
    fn a_gate_never_released_fails_the_write_by_name() {
        let gate = WriteGate::new();
        let refused = gate
            .park_within(Duration::from_millis(20))
            .expect_err("an unreleased gate let the write on");
        assert_eq!(refused.code(), tonic::Code::DeadlineExceeded);
        assert!(refused.message().contains("not released"), "{refused}");
    }
}

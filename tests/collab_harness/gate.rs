//! Named two-way rendezvous gates for deterministic interleavings.
//!
//! One task calls [`GateHook::enter`] at a protocol boundary (for example
//! after the first data frame of an `InstallSnapshot` stream) and parks
//! there. The controlling task calls [`GateHook::wait_until_entered`] to
//! learn that the boundary was reached, performs the competing operation,
//! and then calls [`GateHook::release`] to let the parked task continue.
//!
//! Every wait is bounded by the hook's timeout and returns a
//! [`GateTimeout`] naming the stage instead of hanging, so a broken
//! schedule fails the test rather than stalling the suite. A released
//! stage stays released: entering it again does not park.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Notify;

/// A bounded wait on a stage that did not reach the expected state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GateTimeout {
    /// The stage waited on.
    pub stage: String,
    /// What the waiter needed: `"enter"` or `"release"`.
    pub waiting_for: &'static str,
    /// How long it waited.
    pub after: Duration,
}

impl fmt::Display for GateTimeout {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "gate stage {:?}: no {} within {:?}",
            self.stage, self.waiting_for, self.after
        )
    }
}

#[derive(Default)]
struct Stage {
    entered: u32,
    released: bool,
}

/// A set of named stages shared by the tasks of one interleaving.
pub struct GateHook {
    stages: Mutex<HashMap<String, Stage>>,
    changed: Notify,
    timeout: Duration,
}

impl GateHook {
    /// A hook whose every wait gives up after `timeout`.
    pub fn new(timeout: Duration) -> Arc<Self> {
        Arc::new(Self {
            stages: Mutex::new(HashMap::new()),
            changed: Notify::new(),
            timeout,
        })
    }

    /// Record arrival at `stage`, then park until it is released.
    pub async fn enter(&self, stage: &str) -> Result<(), GateTimeout> {
        self.update(stage, |s| s.entered += 1);
        self.wait(stage, "release", |s| s.released).await
    }

    /// Wait until some task has entered `stage`.
    pub async fn wait_until_entered(&self, stage: &str) -> Result<(), GateTimeout> {
        self.wait(stage, "enter", |s| s.entered > 0).await
    }

    /// Let every task parked at `stage`, and every later arrival, continue.
    pub fn release(&self, stage: &str) {
        self.update(stage, |s| s.released = true);
    }

    /// How many times `stage` has been entered.
    pub fn entries(&self, stage: &str) -> u32 {
        let stages = self.stages.lock().unwrap();
        stages.get(stage).map_or(0, |s| s.entered)
    }

    fn update(&self, stage: &str, change: impl FnOnce(&mut Stage)) {
        change(
            self.stages
                .lock()
                .unwrap()
                .entry(stage.to_string())
                .or_default(),
        );
        self.changed.notify_waiters();
    }

    async fn wait(
        &self,
        stage: &str,
        waiting_for: &'static str,
        done: impl Fn(&Stage) -> bool,
    ) -> Result<(), GateTimeout> {
        let deadline = tokio::time::Instant::now() + self.timeout;
        loop {
            // Register before checking so a change between the check and
            // the await still wakes this waiter.
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.stages.lock().unwrap().get(stage).is_some_and(&done) {
                return Ok(());
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return Err(GateTimeout {
                    stage: stage.to_string(),
                    waiting_for,
                    after: self.timeout,
                });
            }
        }
    }
}

#[tokio::test]
async fn a_parked_task_resumes_only_after_release() {
    let gate = GateHook::new(Duration::from_secs(5));
    let parked = tokio::spawn({
        let gate = gate.clone();
        async move { gate.enter("boundary").await }
    });
    gate.wait_until_entered("boundary").await.unwrap();
    tokio::task::yield_now().await;
    assert!(!parked.is_finished(), "entered but not yet released");
    gate.release("boundary");
    parked.await.unwrap().unwrap();
    // A released stage stays open for later arrivals.
    gate.enter("boundary").await.unwrap();
    assert_eq!(gate.entries("boundary"), 2);
}

#[tokio::test]
async fn waits_are_bounded_and_name_the_stage() {
    let gate = GateHook::new(Duration::from_millis(50));
    let missing = gate.wait_until_entered("never").await.unwrap_err();
    assert_eq!(missing.stage, "never");
    assert_eq!(missing.waiting_for, "enter");
    let stuck = gate.enter("unreleased").await.unwrap_err();
    assert_eq!(stuck.waiting_for, "release");
    assert_eq!(
        stuck.to_string(),
        "gate stage \"unreleased\": no release within 50ms"
    );
    assert_eq!(gate.entries("unreleased"), 1);
}

//! Fault-injection pause hooks for snapshot build, serve, verify, install, and
//! transport handoff paths.
//!
//! Caller handles (`SnapshotGate` and `AsyncSnapshotGate`) are intentionally
//! decoupled from the worker-side pause receivers (`SnapshotPause` and
//! `AsyncSnapshotPause`) so dropping a caller handle during panic unwind
//! immediately unblocks any parked thread or task instead of wedging runtime
//! shutdown.
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// Synchronous fault-injection gate for snapshot build, serve, verify, and
/// install boundaries.
pub struct SnapshotGate {
    reached: tokio::sync::watch::Receiver<bool>,
    release: Mutex<Option<std::sync::mpsc::Sender<()>>>,
}

struct SnapshotPause {
    reached: tokio::sync::watch::Sender<bool>,
    wait: std::sync::mpsc::Receiver<()>,
}

impl SnapshotGate {
    fn pair() -> (Arc<Self>, SnapshotPause) {
        let (reached_tx, reached_rx) = tokio::sync::watch::channel(false);
        let (release, wait) = std::sync::mpsc::channel();
        (
            Arc::new(Self {
                reached: reached_rx,
                release: Mutex::new(Some(release)),
            }),
            SnapshotPause {
                reached: reached_tx,
                wait,
            },
        )
    }

    /// Resolves once the paused path reaches the gate.
    pub async fn reached(&self) {
        let mut receiver = self.reached.clone();
        let _ = receiver.wait_for(|reached| *reached).await;
    }

    /// Releases the paused worker thread.
    pub fn release(&self) {
        if let Some(release) = self.release.lock().unwrap().take() {
            let _ = release.send(());
        }
    }
}

impl SnapshotPause {
    fn pause(self) {
        self.reached.send_replace(true);
        let _ = self.wait.recv();
    }
}

/// Asynchronous fault-injection gate for non-blocking transport handoff
/// boundaries.
pub struct AsyncSnapshotGate {
    reached: tokio::sync::watch::Receiver<bool>,
    release: Arc<tokio::sync::Notify>,
}

struct AsyncSnapshotPause {
    reached: tokio::sync::watch::Sender<bool>,
    release: Arc<tokio::sync::Notify>,
}

impl AsyncSnapshotGate {
    fn pair() -> (Arc<Self>, AsyncSnapshotPause) {
        let (reached_tx, reached_rx) = tokio::sync::watch::channel(false);
        let release = Arc::new(tokio::sync::Notify::new());
        (
            Arc::new(Self {
                reached: reached_rx,
                release: Arc::clone(&release),
            }),
            AsyncSnapshotPause {
                reached: reached_tx,
                release,
            },
        )
    }

    /// Resolves once the paused async handler reaches the gate.
    pub async fn reached(&self) {
        let mut receiver = self.reached.clone();
        let _ = receiver.wait_for(|reached| *reached).await;
    }

    /// Releases the paused async handler.
    pub fn release(&self) {
        self.release.notify_waiters();
    }
}

impl Drop for AsyncSnapshotGate {
    fn drop(&mut self) {
        self.release.notify_waiters();
    }
}

impl AsyncSnapshotPause {
    async fn pause(self) {
        let notified = self.release.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        self.reached.send_replace(true);
        notified.await;
    }
}

#[derive(Default)]
pub(crate) struct SnapshotGates {
    build: Mutex<Option<SnapshotPause>>,
    serve: Mutex<Option<SnapshotPause>>,
    verify: Mutex<Option<SnapshotPause>>,
    install: Mutex<Option<SnapshotPause>>,
    handoff: Mutex<Option<AsyncSnapshotPause>>,
    final_handler_drops: AtomicUsize,
    final_handler_drop_notify: tokio::sync::Notify,
}

impl SnapshotGates {
    pub(crate) fn arm_build(&self) -> Arc<SnapshotGate> {
        Self::arm(&self.build)
    }

    pub(crate) fn arm_serve(&self) -> Arc<SnapshotGate> {
        Self::arm(&self.serve)
    }

    pub(crate) fn arm_verify(&self) -> Arc<SnapshotGate> {
        Self::arm(&self.verify)
    }

    pub(crate) fn arm_install(&self) -> Arc<SnapshotGate> {
        Self::arm(&self.install)
    }

    pub(crate) fn arm_handoff(&self) -> Arc<AsyncSnapshotGate> {
        let (gate, pause) = AsyncSnapshotGate::pair();
        *self.handoff.lock().unwrap() = Some(pause);
        gate
    }

    pub(crate) fn record_final_handler_drop(&self) {
        self.final_handler_drops.fetch_add(1, Ordering::SeqCst);
        self.final_handler_drop_notify.notify_waiters();
    }

    pub(crate) fn final_handler_drops(&self) -> usize {
        self.final_handler_drops.load(Ordering::SeqCst)
    }

    pub(crate) async fn wait_final_handler_drops(&self, at_least: usize) {
        loop {
            let notified = self.final_handler_drop_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.final_handler_drops.load(Ordering::SeqCst) >= at_least {
                return;
            }
            notified.await;
        }
    }

    pub(crate) async fn pause_handoff(&self) {
        let pause = self.handoff.lock().unwrap().take();
        if let Some(pause) = pause {
            pause.pause().await;
        }
    }

    pub(super) fn pause_build(&self) {
        Self::pause(&self.build)
    }

    pub(super) fn pause_serve(&self) {
        Self::pause(&self.serve)
    }

    pub(super) fn pause_verify(&self) {
        Self::pause(&self.verify)
    }

    pub(super) fn pause_install(&self) {
        Self::pause(&self.install)
    }

    fn arm(slot: &Mutex<Option<SnapshotPause>>) -> Arc<SnapshotGate> {
        let (gate, pause) = SnapshotGate::pair();
        *slot.lock().unwrap() = Some(pause);
        gate
    }

    fn pause(slot: &Mutex<Option<SnapshotPause>>) {
        let pause = slot.lock().unwrap().take();
        if let Some(pause) = pause {
            pause.pause();
        }
    }
}

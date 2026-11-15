//! The membership loops: registration, lease renewal, shard reports, and
//! the worker, each until shutdown.

use std::future::Future;
use std::time::Duration;

use tokio::sync::watch;

use super::host::now_ms;
use super::NodeAgent;

impl NodeAgent {
    /// Start the membership loops: register (retrying until the plane
    /// answers), renew every lease/3, report on the timer and after
    /// every flush, and run the worker every reconcile interval. Each
    /// loop ends when `shutdown` turns true.
    pub fn start(&self, shutdown: watch::Receiver<bool>) -> Vec<tokio::task::JoinHandle<()>> {
        let agent = self.clone();
        vec![tokio::spawn(async move { agent.run(shutdown).await })]
    }

    async fn run(self, mut shutdown: watch::Receiver<bool>) {
        // Registration first; the other loops wait for it.
        if !self.register_until_answered(&mut shutdown).await {
            return;
        }
        let node_id = &self.inner.config.node_id;
        if let Err(error) = self.open_placed().await {
            eprintln!("node {node_id:?}: {error}");
        }
        if let Err(error) = self.report_all().await {
            eprintln!("node {node_id:?}: report: {error}");
        }
        let lease_ms = self
            .lease()
            .map(|lease| lease.expires_unix_ms.saturating_sub(now_ms()))
            .filter(|ms| *ms > 0)
            .unwrap_or(15_000);
        let renew_every = Duration::from_millis((lease_ms / 3).max(200));
        let report_every = Duration::from_millis(self.inner.config.report_ms.max(100));
        let reconcile_every = Duration::from_millis(self.inner.config.reconcile_ms.max(100));
        let lease_loop = self.spawn_lease_loop(renew_every, shutdown.clone());
        let report_loop = self.spawn_report_loop(report_every, shutdown.clone());
        let worker_loop = self.spawn_worker_loop(reconcile_every, shutdown);
        let _ = tokio::join!(lease_loop, report_loop, worker_loop);
        self.stop();
    }

    /// Register, retrying every second; `false` when shutdown came first.
    async fn register_until_answered(&self, shutdown: &mut watch::Receiver<bool>) -> bool {
        let config = &self.inner.config;
        loop {
            if *shutdown.borrow() {
                return false;
            }
            match self.register().await {
                Ok(lease) => {
                    eprintln!(
                        "node {:?}: registered at {} (lease {} ms)",
                        config.node_id,
                        config.node_addr,
                        lease.expires_unix_ms.saturating_sub(now_ms())
                    );
                    return true;
                }
                Err(error) => {
                    eprintln!("node {:?}: {error}; retrying", config.node_id);
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                        _ = shutdown.changed() => return false,
                    }
                }
            }
        }
    }

    fn spawn_lease_loop(
        &self,
        every: Duration,
        shutdown: watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        let agent = self.clone();
        tokio::spawn(every_tick(every, shutdown, None, move || {
            let agent = agent.clone();
            async move {
                if let Err(error) = agent.renew().await {
                    eprintln!("node {:?}: {error}", agent.inner.config.node_id);
                }
            }
        }))
    }

    fn spawn_report_loop(
        &self,
        every: Duration,
        shutdown: watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        let agent = self.clone();
        let notify = self.flush_notify();
        tokio::spawn(every_tick(every, shutdown, Some(notify), move || {
            let agent = agent.clone();
            async move {
                if let Err(error) = agent.report_all().await {
                    eprintln!("node {:?}: report: {error}", agent.inner.config.node_id);
                }
            }
        }))
    }

    fn spawn_worker_loop(
        &self,
        every: Duration,
        shutdown: watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        let agent = self.clone();
        tokio::spawn(every_tick(every, shutdown, None, move || {
            let agent = agent.clone();
            async move {
                if let Err(error) = agent.run_once().await {
                    eprintln!("node {:?}: worker: {error}", agent.inner.config.node_id);
                }
            }
        }))
    }
}

/// Run `tick` on every interval tick after the first (and on every
/// `wake` notification, when given) until `shutdown` changes.
async fn every_tick<F, Fut>(
    every: Duration,
    mut shutdown: watch::Receiver<bool>,
    wake: Option<std::sync::Arc<tokio::sync::Notify>>,
    mut tick: F,
) where
    F: FnMut() -> Fut,
    Fut: Future<Output = ()>,
{
    let mut interval = tokio::time::interval(every);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    interval.tick().await;
    loop {
        match &wake {
            Some(notify) => tokio::select! {
                _ = shutdown.changed() => return,
                _ = interval.tick() => {}
                _ = notify.notified() => {}
            },
            None => tokio::select! {
                _ = shutdown.changed() => return,
                _ = interval.tick() => {}
            },
        }
        tick().await;
    }
}

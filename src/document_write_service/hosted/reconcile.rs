//! Event-driven settlement of durable writes with unresolved outcomes.

use super::*;
use crate::document_catalog::Reconciled;

#[cfg(feature = "fault-injection")]
static RECONCILE_PASSES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

impl HostedDocumentWriteService {
    #[doc(hidden)]
    #[cfg(feature = "fault-injection")]
    pub fn reconcile_pass_count() -> u64 {
        RECONCILE_PASSES.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// List up to `limit` UNCONFIRMED writes under `admin`'s leased Admin
    /// admission, then settle each under a fresh lease of its own actor.
    pub async fn reconcile_unconfirmed(
        &self,
        admin: &str,
        collection: &str,
        limit: usize,
    ) -> Result<Reconciled, Status> {
        let _budget = self.inner.budget.admit(0)?;
        #[cfg(feature = "fault-injection")]
        RECONCILE_PASSES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let catalog = self
            .inner
            .catalogs
            .get(collection)
            .cloned()
            .ok_or_else(|| {
                Status::not_found(format!(
                    "no managed catalog serves collection {collection:?}"
                ))
            })?;
        let lease = self.inner.host.lease(admin).await?;
        let listing = Arc::clone(&catalog);
        let pending = tokio::task::spawn_blocking(move || {
            let admission = lease.admission()?;
            listing.unconfirmed_operations(&admission, limit)
        })
        .await
        .map_err(|_| Status::internal("source reconciliation worker failed"))??;
        let mut reconciled = Reconciled::default();
        for operation in pending {
            let settled = match self.inner.host.lease(&operation.principal).await {
                Ok(lease) => {
                    let catalog = Arc::clone(&catalog);
                    let (actor, id) = (operation.principal.clone(), operation.operation_id.clone());
                    tokio::task::spawn_blocking(move || {
                        let admission = lease.admission()?;
                        catalog.settle(&admission, &actor, &id)
                    })
                    .await
                    .map_err(|_| Status::internal("source settlement worker failed"))?
                }
                Err(error) => Err(error),
            };
            reconciled.record(operation, settled);
        }
        Ok(reconciled)
    }

    /// Reconcile pending writes at startup, on write signals, and after
    /// leadership changes until shutdown. Idle catalogs do no lease or I/O.
    pub fn spawn_reconciler(
        self,
        admin: String,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<Result<(), tonic::transport::Error>> {
        tokio::spawn(async move {
            let mut workers = tokio::task::JoinSet::new();
            for (collection, catalog) in &self.inner.catalogs {
                workers.spawn(reconcile_collection(
                    self.clone(),
                    admin.clone(),
                    collection.clone(),
                    catalog.unconfirmed_updates(),
                    shutdown.clone(),
                ));
            }
            while let Some(result) = workers.join_next().await {
                if let Err(error) = result {
                    eprintln!("publication reconciler worker failed: {error}");
                }
            }
            Ok(())
        })
    }
}

async fn reconcile_collection(
    service: HostedDocumentWriteService,
    admin: String,
    collection: String,
    mut updates: tokio::sync::watch::Receiver<bool>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    const RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(25);
    let mut leadership = service.inner.host.metrics();
    let mut last_leadership = {
        let metrics = leadership.borrow_and_update();
        (metrics.current_term, metrics.current_leader, metrics.state)
    };
    let mut retry_at = None;
    let mut run = true;
    loop {
        if run && *updates.borrow() {
            let limit = service.inner.budget.limits.max_in_flight as usize;
            let _ = service
                .reconcile_unconfirmed(&admin, &collection, limit)
                .await;
            run = updates.has_changed().unwrap_or(false);
            if run || !*updates.borrow() {
                retry_at = None;
            } else if retry_at.is_none() {
                retry_at = Some(tokio::time::Instant::now() + RETRY_DELAY);
            }
        }
        if *shutdown.borrow() {
            break;
        }
        let retry = retry_at.is_some() && *updates.borrow();
        tokio::select! {
            _ = shutdown.wait_for(|raised| *raised) => break,
            changed = updates.changed() => {
                if changed.is_err() {
                    break;
                }
                retry_at = None;
                run = true;
            }
            changed = leadership.changed(), if *updates.borrow() => {
                if changed.is_err() {
                    break;
                }
                let metrics = leadership.borrow_and_update();
                let current = (metrics.current_term, metrics.current_leader, metrics.state);
                if current != last_leadership {
                    last_leadership = current;
                    retry_at = None;
                    run = true;
                }
            }
            _ = wait_retry(retry_at), if retry => {
                retry_at = None;
                run = true;
            }
        }
    }
}

async fn wait_retry(deadline: Option<tokio::time::Instant>) {
    if let Some(deadline) = deadline {
        tokio::time::sleep_until(deadline).await;
    } else {
        std::future::pending::<()>().await;
    }
}

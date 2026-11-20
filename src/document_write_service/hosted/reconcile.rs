//! Event-driven settlement of durable writes with unresolved outcomes.

use super::*;
use crate::document_catalog::Reconciled;

impl HostedDocumentWriteService {
    /// List up to `limit` UNCONFIRMED writes under `admin`'s leased Admin
    /// admission, then settle each under a fresh lease of its own actor.
    pub async fn reconcile_unconfirmed(
        &self,
        admin: &str,
        collection: &str,
        limit: usize,
    ) -> Result<Reconciled, Status> {
        let _budget = self.inner.budget.admit(0)?;
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
                result.unwrap();
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
    let mut leadership = service.inner.host.metrics();
    loop {
        if *updates.borrow() {
            let limit = service.inner.budget.limits.max_in_flight as usize;
            let outcome = service
                .reconcile_unconfirmed(&admin, &collection, limit)
                .await;
            match outcome {
                Ok(result) if !result.accepted.is_empty() || !result.fenced.is_empty() => {
                    if *shutdown.borrow() {
                        break;
                    }
                    continue;
                }
                Err(error) if error.code() == tonic::Code::ResourceExhausted => {
                    // Another catalog or request holds capacity; no lease or
                    // database read occurred, so retry when it can finish.
                    tokio::select! {
                        _ = shutdown.wait_for(|raised| *raised) => break,
                        _ = tokio::time::sleep(std::time::Duration::from_millis(25)) => {}
                    }
                    continue;
                }
                _ => {}
            }
        }
        tokio::select! {
            _ = shutdown.wait_for(|raised| *raised) => break,
            changed = updates.changed() => {
                if changed.is_err() {
                    break;
                }
            }
            changed = leadership.changed() => {
                if changed.is_err() {
                    break;
                }
            }
        }
    }
}

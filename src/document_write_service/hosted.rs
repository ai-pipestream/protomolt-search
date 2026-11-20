//! Raft-hosted `DocumentWriteService` adapter with leased leader admission.
//!
//! Authenticates and authorizes the transport principal first, acquires a
//! `RaftHost::lease` under that actor identity, and runs the catalog grant and
//! commit inside a blocking worker that holds the lease and capacity permits
//! until completion even if the caller drops the RPC future. Lease errors pass
//! through unchanged without retry or leader forwarding.

use super::{
    admit_ingest, admit_publication_read, extract_network_document, publication_targets,
    WorkPermit, WriteBudget,
};
use crate::document_catalog::{Acceptance, ActiveManagedCatalog, Reconciled, Settlement};
use crate::metrics::{self, Route};
use crate::node::ManagedTarget;
use crate::pb::document_write_service_server::{DocumentWriteService, DocumentWriteServiceServer};
use crate::pb::{
    DocumentPublicationStatus, DocumentWriteReceipt, DocumentWriteRequest,
    DocumentWriteServiceLimits, DocumentWriteTarget, GetDocumentPublicationStatusRequest,
    GetDocumentWriteTargetRequest,
};
use crate::raft::RaftHost;
use crate::security::Principals;
use prost::Message;
use std::collections::BTreeMap;
use std::sync::Arc;
use tonic::{Request, Response, Status};

struct Inner {
    principals: Principals,
    host: Arc<RaftHost>,
    catalogs: BTreeMap<String, Arc<ActiveManagedCatalog>>,
    publication: BTreeMap<String, Vec<ManagedTarget>>,
    budget: WriteBudget,
    /// One-shot pause between the lease and the worker's grant, armed by
    /// `arm_grant_delay` so a test can age a lease past its interval.
    #[cfg(any(test, feature = "fault-injection"))]
    grant_delay: std::sync::Mutex<Option<std::time::Duration>>,
}

/// Recover every configured managed catalog against `host`'s applied store view
/// without taking a leader lease. Rejects prepared-only catalogs, mismatched
/// collection bindings, and foreign authority identities; any failure aborts
/// recovery before serving starts.
pub fn recover_catalogs(
    host: &RaftHost,
    principal: &str,
    entries: &[crate::config::RaftManagedCatalog],
) -> Result<Vec<Arc<ActiveManagedCatalog>>, Status> {
    let store = host.store()?;
    let mut catalogs = Vec::with_capacity(entries.len());
    for entry in entries {
        let context = |error: Status| {
            crate::raft::reasons::carry(
                &error,
                Status::new(
                    error.code(),
                    format!(
                        "managed catalog for collection {:?}: {}",
                        entry.collection,
                        error.message()
                    ),
                ),
            )
        };
        let (_, binding, file_activation) =
            crate::document_catalog::header_binding(&entry.path).map_err(context)?;
        if file_activation.is_none() {
            return Err(crate::raft::reasons::reason(
                Status::failed_precondition(format!(
                    "managed catalog for collection {:?} is prepared, not active; only an activated source serves writes",
                    entry.collection
                )),
                crate::raft::reasons::ADMISSION_NOT_ACTIVE,
            ));
        }
        let bound_collection = binding
            .preparation
            .as_ref()
            .and_then(|preparation| preparation.key.as_ref())
            .map(|key| key.collection.as_str());
        if bound_collection != Some(entry.collection.as_str()) {
            return Err(crate::raft::reasons::reason(
                Status::failed_precondition(format!(
                    "managed catalog for collection {:?} names another collection in its binding",
                    entry.collection
                )),
                crate::raft::reasons::ADMISSION_FOREIGN_COLLECTION,
            ));
        }
        if binding.authority.as_ref() != Some(store.identity()) {
            return Err(crate::raft::reasons::reason(
                Status::failed_precondition(format!(
                    "managed catalog for collection {:?} names another source authority",
                    entry.collection
                )),
                crate::raft::reasons::ADMISSION_FOREIGN_AUTHORITY,
            ));
        }
        let admission = store.recovery_admission(principal).map_err(context)?;
        let catalog = ActiveManagedCatalog::recover(&entry.path, &store, &admission, &binding)
            .map_err(context)?;
        catalogs.push(Arc::new(catalog));
    }
    Ok(catalogs)
}

/// The hosted write service: the network adapter over activated managed
/// catalogs whose admissions all come from the member's Raft host.
#[derive(Clone)]
pub struct HostedDocumentWriteService {
    inner: Arc<Inner>,
}

impl HostedDocumentWriteService {
    pub fn new(
        principals: Principals,
        host: Arc<RaftHost>,
        catalogs: Vec<Arc<ActiveManagedCatalog>>,
        limits: DocumentWriteServiceLimits,
    ) -> Result<Self, Status> {
        if catalogs.is_empty() {
            return Err(Status::invalid_argument(
                "hosted write service requires a recovered managed catalog",
            ));
        }
        let mut members = BTreeMap::new();
        for catalog in catalogs {
            let collection = catalog.collection().to_string();
            if members.insert(collection.clone(), catalog).is_some() {
                return Err(Status::invalid_argument(format!(
                    "hosted write service repeats collection {collection:?}"
                )));
            }
        }
        Ok(Self {
            inner: Arc::new(Inner {
                principals,
                host,
                catalogs: members,
                publication: BTreeMap::new(),
                budget: WriteBudget::new(limits)?,
                #[cfg(any(test, feature = "fault-injection"))]
                grant_delay: std::sync::Mutex::new(None),
            }),
        })
    }

    /// Register the serving targets visible to publication-status reads.
    pub fn with_publication_targets(
        mut self,
        collection: &str,
        targets: Vec<ManagedTarget>,
    ) -> Result<Self, Status> {
        if targets.is_empty() {
            return Err(Status::invalid_argument(
                "publication status requires at least one target",
            ));
        }
        if !self.inner.catalogs.contains_key(collection) {
            return Err(Status::not_found("publication source is not configured"));
        }
        let inner = Arc::get_mut(&mut self.inner).ok_or_else(|| {
            Status::failed_precondition("register publication before sharing the write service")
        })?;
        if inner
            .publication
            .insert(collection.to_string(), targets)
            .is_some()
        {
            return Err(Status::invalid_argument(
                "publication status repeats a collection",
            ));
        }
        Ok(self)
    }

    /// Use this builder when registering the adapter so tonic enforces the
    /// request byte limit during decoding as well as at execution admission.
    pub fn into_server(self) -> DocumentWriteServiceServer<Self> {
        let max_bytes = self.inner.budget.max_request_bytes();
        DocumentWriteServiceServer::new(self).max_decoding_message_size(max_bytes)
    }

    /// Arm a one-shot pause between the next call's lease and its grant.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn arm_grant_delay(&self, delay: std::time::Duration) {
        *self.inner.grant_delay.lock().unwrap() = Some(delay);
    }

    fn admit<T: Message>(
        &self,
        request: &Request<T>,
        collection: &str,
        write: bool,
    ) -> Result<
        (
            String,
            Arc<ActiveManagedCatalog>,
            crate::authorization::AccessPermit,
            WorkPermit,
        ),
        Status,
    > {
        let admitted = admit_ingest(
            &self.inner.principals,
            &self.inner.catalogs,
            &self.inner.budget,
            request,
            collection,
            write,
        )?;
        Ok((
            admitted.actor,
            admitted.catalog,
            admitted.access,
            admitted.permit,
        ))
    }

    /// The window between the lease and the worker, on the RPC future. A
    /// client that drops its call here drops the lease ungranted; the
    /// fault-injection delay ages a lease on purpose so a test can see the
    /// worker reject the grant by name.
    async fn handoff(&self) -> Result<(), Status> {
        #[cfg(any(test, feature = "fault-injection"))]
        let delay = self.inner.grant_delay.lock().unwrap().take();
        #[cfg(any(test, feature = "fault-injection"))]
        if let Some(delay) = delay {
            tokio::time::sleep(delay).await;
        }
        Ok(())
    }

    async fn target(
        &self,
        request: Request<GetDocumentWriteTargetRequest>,
    ) -> Result<Response<DocumentWriteTarget>, Status> {
        let (actor, catalog, access, permit) =
            self.admit(&request, &request.get_ref().collection, false)?;
        let lease = self.inner.host.lease(&actor).await?;
        self.handoff().await?;
        let result = tokio::task::spawn_blocking(move || {
            // Pin the lease and the permits in the receiving worker, not
            // in the RPC future: from here a dropped future releases
            // nothing until the worker returns.
            let _permit = permit;
            let admission = lease.admission()?;
            catalog.write_target(&admission)
        })
        .await
        .map_err(|_| Status::internal("source target worker failed"))?;
        // The target discloses the pinned history, so current access still
        // gates the reply after the worker returns.
        access.check()?;
        result.map(Response::new)
    }

    async fn status(
        &self,
        request: Request<GetDocumentPublicationStatusRequest>,
    ) -> Result<Response<DocumentPublicationStatus>, Status> {
        let collection = request.get_ref().collection.clone();
        let sequence = request.get_ref().accepted_sequence;
        if sequence == 0 {
            return Err(Status::invalid_argument(
                "publication status requires a positive accepted_sequence",
            ));
        }
        let admitted = admit_publication_read(
            &self.inner.principals,
            &self.inner.catalogs,
            &self.inner.budget,
            &request,
            &collection,
        )?;
        let configured = self
            .inner
            .publication
            .get(&collection)
            .cloned()
            .ok_or_else(|| {
                Status::failed_precondition(
                    "publication targets are not configured for this collection",
                )
            })?;
        let targets = publication_targets(&configured, &request.get_ref().index_keys)?;
        let access = admitted.access.clone();
        let lease = self.inner.host.lease(&admitted.actor).await?;
        self.handoff().await?;
        let result = tokio::task::spawn_blocking(move || {
            let _permit = admitted.permit;
            let admission = lease.admission()?;
            crate::node::publication_status_blocking(
                &admitted.catalog,
                &admission,
                sequence,
                &targets,
            )
        })
        .await
        .map_err(|_| Status::internal("publication status worker failed"))??;
        access.check()?;
        result
            .map(Response::new)
            .ok_or_else(|| Status::not_found("accepted sequence is not in the source history"))
    }

    async fn accept(
        &self,
        request: Request<DocumentWriteRequest>,
    ) -> Result<Response<DocumentWriteReceipt>, Status> {
        let (actor, catalog, access, permit) =
            self.admit(&request, &request.get_ref().collection, true)?;
        let document = extract_network_document(request)?;
        let lease = self.inner.host.lease(&actor).await?;
        self.handoff().await?;
        let settling = Arc::clone(&catalog);
        let operation_id = document.operation_id.clone();
        let (result, permit) = tokio::task::spawn_blocking(move || {
            // Pin the lease and the permits in the receiving worker, not
            // in the RPC future: from here a dropped future releases
            // nothing until the worker returns, and the commit happens
            // whether or not a client is still listening. The permits come
            // back so a settlement runs under the same ones.
            let result = lease
                .admission()
                .and_then(|admission| catalog.accept(&admission, &document));
            (result, permit)
        })
        .await
        .map_err(|_| Status::internal("source acceptance worker failed"))?;
        let result = match result {
            Ok(Acceptance::Accepted(receipt)) => Ok(receipt),
            Ok(Acceptance::Unconfirmed(receipt)) => {
                self.settle(&actor, settling, operation_id, receipt, permit)
                    .await
            }
            Err(error) => Err(error),
        };
        // Acceptance may have committed under its admitted decision while a
        // replacement published. Current access still gates receipt disclosure.
        access.check().map_err(|error| {
            if crate::error_disclosure::status_detail(&error).is_some_and(|detail| {
                detail.reason == crate::pb::SearchErrorReason::AccessPolicyChanged as i32
                    && error.code() == tonic::Code::PermissionDenied
            }) {
                crate::raft::reasons::reason(error, crate::raft::reasons::RECEIPT_POLICY_CHANGED)
            } else {
                error
            }
        })?;
        result.map(Response::new)
    }

    /// The pending-actor reconciler, run after a restart or a leader
    /// failover: list up to `limit` UNCONFIRMED writes of `collection`
    /// under `admin`'s leased Admin admission, then settle each under a
    /// fresh lease of its own actor, the one whose Ingest right is judged.
    /// A write no fresh lease can decide now stays unconfirmed and is
    /// reported, for the next pass.
    pub async fn reconcile_unconfirmed(
        &self,
        admin: &str,
        collection: &str,
        limit: usize,
    ) -> Result<Reconciled, Status> {
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

    /// Reconcile at startup and under the current leader until shutdown.
    /// Every pass is bounded by the write-service admission budget.
    pub fn spawn_reconciler(
        self,
        admin: String,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<Result<(), tonic::transport::Error>> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                let collections: Vec<String> = self.inner.catalogs.keys().cloned().collect();
                for collection in collections {
                    let limit = self.inner.budget.limits.max_in_flight as usize;
                    let _ = self.reconcile_unconfirmed(&admin, &collection, limit).await;
                }
                tokio::select! {
                    _ = shutdown.wait_for(|raised| *raised) => break,
                    _ = interval.tick() => {}
                }
            }
            Ok(())
        })
    }

    /// Settle a durable write whose original lease lapsed before commit returned,
    /// using a fresh lease under the call's existing permits. Returns `Unavailable`
    /// naming the durable version if no fresh lease can settle it immediately.
    async fn settle(
        &self,
        actor: &str,
        catalog: Arc<ActiveManagedCatalog>,
        operation_id: Vec<u8>,
        receipt: DocumentWriteReceipt,
        permit: WorkPermit,
    ) -> Result<DocumentWriteReceipt, Status> {
        let unsettled = |cause: &Status| {
            crate::raft::reasons::reason(
                Status::unavailable(format!(
                    "write of version {} at sequence {} is durable but unconfirmed: its lease lapsed before the commit returned, and no fresh lease settles it now ({}); retry the operation to settle it",
                    receipt.version,
                    receipt.accepted_sequence,
                    cause.message()
                )),
                crate::raft::reasons::OUTCOME_UNCONFIRMED,
            )
        };
        let lease = match self.inner.host.lease(actor).await {
            Ok(lease) => lease,
            Err(error) => return Err(unsettled(&error)),
        };
        let actor = actor.to_string();
        let settled = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let admission = lease.admission()?;
            catalog.settle(&admission, &actor, &operation_id)
        })
        .await
        .map_err(|_| Status::internal("source settlement worker failed"))?;
        match settled {
            Ok(Settlement::Accepted(receipt)) => Ok(receipt),
            Ok(Settlement::Fenced(rejection)) => Err(rejection),
            Err(error) => Err(unsettled(&error)),
        }
    }
}

impl_document_write_service!(HostedDocumentWriteService);

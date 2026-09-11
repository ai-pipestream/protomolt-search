//! Owner writes served by a Raft member through the host's leased admission.
//!
//! Every admission on this path comes from `RaftHost::lease`. The transport
//! gate (`Principals::authenticate` and `authorize(.., Ingest)`) stays as
//! the first check, exactly as on the single-authority service; the
//! authority admission below is the second and authoritative gate. A
//! process serves the single-authority service or this one, never both:
//! the two share the `DocumentWriteService` proto and its `Route` rows, so
//! clients do not change.
//!
//! The lease actor is the transport principal's name. Policy rows in the
//! authority name actors by that same string, and the decision the source
//! commit records carries it, so the two gates judge the same actor. This
//! equivalence is a contract of the deployment: a principal name must mean
//! the same actor in the Bearer [REDACTED] and in the authority policy.
//!
//! One `AcceptDocument` call, in order: transport gate, request size with
//! pending and byte permits plus ingest admission on the principal,
//! `host.lease` on the RPC future, then a blocking worker holding the
//! lease, the permits and the request, where the grant runs and the
//! catalog commits. A client that drops its call before the worker exists
//! drops the lease ungranted and the permits unused, and no row is
//! written. Once the worker exists it finishes on its own terms: the
//! library does not cancel a blocking task when its handle is dropped, the
//! permits are released only when the worker returns, and the grant and
//! the commit run there whether or not a client is still listening
//! (`a_client_that_drops_its_call_leaves_the_worker_to_finish_under_its_permits`).
//! `GetDocumentWriteTarget` follows the same shape.
//!
//! Every error from `lease` passes through unchanged: `Unavailable`
//! naming the leader off-leader, `Unavailable` when no quorum
//! acknowledged the barrier within the election ceiling, and
//! `FailedPrecondition` when the interval elapsed before the grant. The
//! service does not retry, does not forward to the leader, and never
//! falls back to the local `admission()`, which rejects on a hosted
//! store anyway.
//!
//! No lease check runs after the commit. A write that passed its final
//! check is durable and fenced by the owner's write epoch, and hiding its
//! receipt would not make it less durable. A pause between the final
//! check and durability is the named residual, closed only by a
//! storage-side fencing token the catalog does not have yet.

use super::{BudgetPermit, WriteBudget};
use crate::document_catalog::ActiveManagedCatalog;
use crate::metrics::{self, Route};
use crate::pb::document_write_service_server::{DocumentWriteService, DocumentWriteServiceServer};
use crate::pb::{
    AccessAction, DocumentWriteReceipt, DocumentWriteRequest, DocumentWriteServiceLimits,
    DocumentWriteTarget, GetDocumentWriteTargetRequest,
};
use crate::raft::RaftHost;
use crate::security::{Permit, Principals};
use prost::Message;
use std::collections::BTreeMap;
use std::sync::Arc;
use tonic::{Request, Response, Status};

struct Inner {
    principals: Principals,
    host: Arc<RaftHost>,
    catalogs: BTreeMap<String, Arc<ActiveManagedCatalog>>,
    budget: WriteBudget,
    /// One-shot pause between the lease and the worker's grant, armed by
    /// `arm_grant_delay` so a test can age a lease past its interval.
    #[cfg(any(test, feature = "fault-injection"))]
    grant_delay: std::sync::Mutex<Option<std::time::Duration>>,
}

struct WorkPermit {
    _principal: Permit,
    _budget: BudgetPermit,
}

/// Recover every managed catalog the operator configured, for the serving
/// binary and for tests alike: open each path, recover the managed handle
/// under the store's applied view, and reject by name a catalog that is
/// prepared rather than active, one with an authority identity that is not
/// the member's, or one with an activation the store does not record. The
/// binding comes from the catalog file's own header, presented byte-exact
/// to the committed owner row; nothing is reconstructed, and the row is
/// what admits the handle. A rejection stops the whole recovery with its
/// cause: no partial service starts on the rest.
///
/// Recovery takes no lease. A lease needs a leader, and a member restarts
/// before any leader exists and serves as a follower most of its life; a
/// recovery admission (`SourceAuthorityStore::recovery_admission`) opens
/// the handle on what this replica has applied and admits no write. Every
/// write on the handle takes its own lease, so a handle opened on a view
/// the quorum has since moved past is rejected at its first write, by
/// name, and nothing this function admits reaches a source transaction.
pub fn recover_catalogs(
    host: &RaftHost,
    principal: &str,
    entries: &[crate::config::RaftManagedCatalog],
) -> Result<Vec<Arc<ActiveManagedCatalog>>, Status> {
    let store = host.store()?;
    let mut catalogs = Vec::with_capacity(entries.len());
    for entry in entries {
        let context = |error: Status| {
            Status::new(
                error.code(),
                format!(
                    "managed catalog for collection {:?}: {}",
                    entry.collection,
                    error.message()
                ),
            )
        };
        let (_, binding, file_activation) =
            crate::document_catalog::header_binding(&entry.path).map_err(context)?;
        if file_activation.is_none() {
            return Err(Status::failed_precondition(format!(
                "managed catalog for collection {:?} is prepared, not active; only an activated source serves writes",
                entry.collection
            )));
        }
        let bound_collection = binding
            .preparation
            .as_ref()
            .and_then(|preparation| preparation.key.as_ref())
            .map(|key| key.collection.as_str());
        if bound_collection != Some(entry.collection.as_str()) {
            return Err(Status::failed_precondition(format!(
                "managed catalog for collection {:?} names another collection in its binding",
                entry.collection
            )));
        }
        if binding.authority.as_ref() != Some(store.identity()) {
            return Err(Status::failed_precondition(format!(
                "managed catalog for collection {:?} names another source authority",
                entry.collection
            )));
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
                budget: WriteBudget::new(limits)?,
                #[cfg(any(test, feature = "fault-injection"))]
                grant_delay: std::sync::Mutex::new(None),
            }),
        })
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
        let principal = self.inner.principals.authenticate(request.metadata())?;
        let access =
            self.inner
                .principals
                .authorize(&principal, collection, AccessAction::Ingest)?;
        let catalog = self
            .inner
            .catalogs
            .get(collection)
            .cloned()
            .ok_or_else(|| Status::not_found("source collection is not configured"))?;
        let length = request.get_ref().encoded_len();
        if length > self.inner.budget.max_request_bytes() {
            return Err(Status::resource_exhausted(
                "source request exceeds max_request_bytes",
            ));
        }
        let principal_permit = principal.admit_request()?;
        let budget = self.inner.budget.admit(length)?;
        if write {
            principal.admit_ingest(1)?;
        }
        Ok((
            principal.name.clone(),
            catalog,
            access,
            WorkPermit {
                _principal: principal_permit,
                _budget: budget,
            },
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

    async fn accept(
        &self,
        request: Request<DocumentWriteRequest>,
    ) -> Result<Response<DocumentWriteReceipt>, Status> {
        let (actor, catalog, access, permit) =
            self.admit(&request, &request.get_ref().collection, true)?;
        let document = request
            .into_inner()
            .document
            .ok_or_else(|| Status::invalid_argument("source write is missing document"))?;
        if document.contract_version != 2 || document.history_id.len() != 16 {
            return Err(Status::invalid_argument(
                "network source writes require contract_version 2 and an exact 16-byte history_id",
            ));
        }
        let lease = self.inner.host.lease(&actor).await?;
        self.handoff().await?;
        let result = tokio::task::spawn_blocking(move || {
            // Pin the lease and the permits in the receiving worker, not
            // in the RPC future: from here a dropped future releases
            // nothing until the worker returns, and the commit happens
            // whether or not a client is still listening.
            let _permit = permit;
            let admission = lease.admission()?;
            catalog.accept(&admission, &document)
        })
        .await
        .map_err(|_| Status::internal("source acceptance worker failed"))?;
        // Acceptance may have committed under its admitted decision while a
        // replacement published. Current access still gates receipt disclosure.
        access.check()?;
        result.map(Response::new)
    }
}

#[tonic::async_trait]
impl DocumentWriteService for HostedDocumentWriteService {
    async fn get_write_target(
        &self,
        request: Request<GetDocumentWriteTargetRequest>,
    ) -> Result<Response<DocumentWriteTarget>, Status> {
        metrics::timed(Route::GetDocumentWriteTarget, request, |request| {
            self.target(request)
        })
        .await
    }

    async fn accept_document(
        &self,
        request: Request<DocumentWriteRequest>,
    ) -> Result<Response<DocumentWriteReceipt>, Status> {
        metrics::timed(Route::AcceptSourceDocument, request, |request| {
            self.accept(request)
        })
        .await
    }
}

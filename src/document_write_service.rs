//! Receiving-side source writes for explicitly provisioned local catalogs.
//!
//! The receiver holds its authorization through synchronous source commit.
//! This adapter does not provision or activate a distributed source owner.

use crate::authorization::AccessPermit;
use crate::document_catalog::AccessControlledCatalog;
use crate::metrics::{self, Route};
use crate::pb::document_write_service_server::{DocumentWriteService, DocumentWriteServiceServer};
use crate::pb::{
    AccessAction, DocumentWriteReceipt, DocumentWriteRequest, DocumentWriteServiceLimits,
    DocumentWriteTarget, GetDocumentWriteTargetRequest,
};
use crate::security::{Permit, Principals};
use prost::Message;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tonic::{Request, Response, Status};

struct Inner {
    principals: Principals,
    catalogs: BTreeMap<String, Arc<AccessControlledCatalog>>,
    limits: DocumentWriteServiceLimits,
    pending: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
}

struct WorkPermit {
    _principal: Permit,
    _pending: OwnedSemaphorePermit,
    _bytes: OwnedSemaphorePermit,
}

/// Host adapter over catalogs that the embedding application has explicitly
/// opened under Admin permission. Clones share every receiver capacity limit.
/// Source/history bytes remain in those local catalogs. Exposing this service
/// does not turn their resource binding into distributed ownership authority.
#[derive(Clone)]
pub struct DocumentWriteServiceImpl {
    inner: Arc<Inner>,
}

impl DocumentWriteServiceImpl {
    pub fn new(
        principals: Principals,
        catalogs: Vec<Arc<AccessControlledCatalog>>,
        limits: DocumentWriteServiceLimits,
    ) -> Result<Self, Status> {
        if limits.max_request_bytes == 0 || limits.max_request_bytes > 64 * 1024 * 1024 {
            return Err(Status::invalid_argument(
                "source max_request_bytes must be 1..64 MiB",
            ));
        }
        if limits.max_pending_bytes < limits.max_request_bytes
            || limits.max_pending_bytes > 256 * 1024 * 1024
        {
            return Err(Status::invalid_argument(
                "source max_pending_bytes must cover max_request_bytes and be at most 256 MiB",
            ));
        }
        if limits.max_in_flight == 0 || limits.max_in_flight > 1024 {
            return Err(Status::invalid_argument(
                "source max_in_flight must be 1..1024",
            ));
        }
        if catalogs.is_empty() {
            return Err(Status::invalid_argument(
                "source write service requires an explicitly provisioned catalog",
            ));
        }
        let mut members = BTreeMap::new();
        for catalog in catalogs {
            let collection = catalog.resource_binding().collection.clone();
            if members.insert(collection.clone(), catalog).is_some() {
                return Err(Status::invalid_argument(format!(
                    "source write service repeats collection {collection:?}"
                )));
            }
        }
        Ok(Self {
            inner: Arc::new(Inner {
                principals,
                catalogs: members,
                pending: Arc::new(Semaphore::new(limits.max_in_flight as usize)),
                bytes: Arc::new(Semaphore::new(limits.max_pending_bytes as usize)),
                limits,
            }),
        })
    }

    /// Use this builder when registering the adapter so tonic enforces the
    /// request byte limit during decoding as well as at execution admission.
    pub fn into_server(self) -> DocumentWriteServiceServer<Self> {
        let max_bytes = self.inner.limits.max_request_bytes as usize;
        DocumentWriteServiceServer::new(self).max_decoding_message_size(max_bytes)
    }

    fn admit<T: Message>(
        &self,
        request: &Request<T>,
        collection: &str,
        write: bool,
    ) -> Result<(Arc<AccessControlledCatalog>, AccessPermit, WorkPermit), Status> {
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
        if length > self.inner.limits.max_request_bytes as usize {
            return Err(Status::resource_exhausted(
                "source request exceeds max_request_bytes",
            ));
        }
        let principal_permit = principal.admit_request()?;
        let pending = Arc::clone(&self.inner.pending)
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("source execution capacity is full"))?;
        let bytes = Arc::clone(&self.inner.bytes)
            .try_acquire_many_owned(length as u32)
            .map_err(|_| Status::resource_exhausted("source pending byte budget is full"))?;
        if write {
            principal.admit_ingest(1)?;
        }
        Ok((
            catalog,
            access,
            WorkPermit {
                _principal: principal_permit,
                _pending: pending,
                _bytes: bytes,
            },
        ))
    }

    async fn target(
        &self,
        request: Request<GetDocumentWriteTargetRequest>,
    ) -> Result<Response<DocumentWriteTarget>, Status> {
        let (catalog, access, admission) =
            self.admit(&request, &request.get_ref().collection, false)?;
        let commit_access = access.clone();
        let result = tokio::task::spawn_blocking(move || {
            let _admission = admission;
            catalog.write_target(&commit_access)
        })
        .await
        .map_err(|_| Status::internal("source target worker failed"))?;
        access.check()?;
        result.map(Response::new)
    }

    async fn accept(
        &self,
        request: Request<DocumentWriteRequest>,
    ) -> Result<Response<DocumentWriteReceipt>, Status> {
        let (catalog, access, admission) =
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
        let commit_access = access.clone();
        let result = tokio::task::spawn_blocking(move || {
            // Pin inside the receiving worker, not in the RPC future. Dropping
            // that future cannot release permission or capacity before commit.
            let _admission = admission;
            catalog.accept(&commit_access, &document)
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
impl DocumentWriteService for DocumentWriteServiceImpl {
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

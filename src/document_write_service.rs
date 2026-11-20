//! Receiving-side source writes for explicitly provisioned local catalogs.
//!
//! The receiver holds its authorization through synchronous source commit.
//! This adapter does not provision or activate a distributed source owner.

use crate::authorization::AccessPermit;
use crate::document_catalog::{AccessControlledCatalog, ActiveManagedCatalog};
use crate::metrics::{self, Route};
use crate::node::ManagedTarget;
use crate::pb::document_write_service_server::{DocumentWriteService, DocumentWriteServiceServer};
use crate::pb::{
    AcceptDocumentRequest, AccessAction, DocumentPublicationStatus, DocumentWriteReceipt,
    DocumentWriteRequest, DocumentWriteServiceLimits, DocumentWriteTarget,
    GetDocumentPublicationStatusRequest, GetDocumentWriteTargetRequest,
};
use crate::security::{Permit, Principals};
use prost::Message;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tonic::{Request, Response, Status};

macro_rules! impl_document_write_service {
    ($ty:ty) => {
        #[tonic::async_trait]
        impl DocumentWriteService for $ty {
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

            async fn get_document_publication_status(
                &self,
                request: Request<GetDocumentPublicationStatusRequest>,
            ) -> Result<Response<DocumentPublicationStatus>, Status> {
                metrics::timed(Route::GetDocumentWriteTarget, request, |request| {
                    self.status(request)
                })
                .await
            }
        }
    };
}

#[cfg(feature = "raft")]
pub mod hosted;

/// The admission limits every write service enforces, shared by the
/// single-authority service and the hosted one: one validation, one pair
/// of semaphores, no copied block.
struct WriteBudget {
    limits: DocumentWriteServiceLimits,
    pending: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
}

struct BudgetPermit {
    _pending: OwnedSemaphorePermit,
    _bytes: OwnedSemaphorePermit,
}

impl WriteBudget {
    fn new(limits: DocumentWriteServiceLimits) -> Result<Self, Status> {
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
        Ok(Self {
            pending: Arc::new(Semaphore::new(limits.max_in_flight as usize)),
            bytes: Arc::new(Semaphore::new(limits.max_pending_bytes as usize)),
            limits,
        })
    }

    fn max_request_bytes(&self) -> usize {
        self.limits.max_request_bytes as usize
    }

    fn admit(&self, length: usize) -> Result<BudgetPermit, Status> {
        let pending = Arc::clone(&self.pending)
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("source execution capacity is full"))?;
        let bytes = Arc::clone(&self.bytes)
            .try_acquire_many_owned(length as u32)
            .map_err(|_| Status::resource_exhausted("source pending byte budget is full"))?;
        Ok(BudgetPermit {
            _pending: pending,
            _bytes: bytes,
        })
    }
}

#[derive(Clone)]
struct ManagedPublication {
    source: Arc<ActiveManagedCatalog>,
    targets: Vec<ManagedTarget>,
}

struct Inner {
    principals: Principals,
    catalogs: BTreeMap<String, Arc<AccessControlledCatalog>>,
    publication: BTreeMap<String, ManagedPublication>,
    budget: WriteBudget,
}

struct WorkPermit {
    _principal: Permit,
    _budget: BudgetPermit,
}

struct AdmittedIngest<C> {
    actor: String,
    catalog: Arc<C>,
    access: AccessPermit,
    permit: WorkPermit,
}

fn admit_ingest<T: Message, C>(
    principals: &Principals,
    catalogs: &BTreeMap<String, Arc<C>>,
    budget: &WriteBudget,
    request: &Request<T>,
    collection: &str,
    write: bool,
) -> Result<AdmittedIngest<C>, Status> {
    admit_action(
        principals, catalogs, budget, request, collection, write, false,
    )
}

fn admit_publication_read<T: Message, C>(
    principals: &Principals,
    catalogs: &BTreeMap<String, Arc<C>>,
    budget: &WriteBudget,
    request: &Request<T>,
    collection: &str,
) -> Result<AdmittedIngest<C>, Status> {
    admit_action(
        principals, catalogs, budget, request, collection, false, true,
    )
}

fn admit_action<T: Message, C>(
    principals: &Principals,
    catalogs: &BTreeMap<String, Arc<C>>,
    budget: &WriteBudget,
    request: &Request<T>,
    collection: &str,
    write: bool,
    publication_read: bool,
) -> Result<AdmittedIngest<C>, Status> {
    let principal = principals.authenticate(request.metadata())?;
    let access = if publication_read {
        principals
            .authorize(&principal, collection, AccessAction::Admin)
            .or_else(|error| {
                if error.code() == tonic::Code::PermissionDenied {
                    principals.authorize(&principal, collection, AccessAction::Ingest)
                } else {
                    Err(error)
                }
            })?
    } else {
        principals.authorize(&principal, collection, AccessAction::Ingest)?
    };
    let catalog = catalogs
        .get(collection)
        .cloned()
        .ok_or_else(|| Status::not_found("source collection is not configured"))?;
    let length = request.get_ref().encoded_len();
    if length > budget.max_request_bytes() {
        return Err(Status::resource_exhausted(
            "source request exceeds max_request_bytes",
        ));
    }
    let principal_permit = principal.admit_request()?;
    let budget_permit = budget.admit(length)?;
    if write {
        principal.admit_ingest(1)?;
    }
    Ok(AdmittedIngest {
        actor: principal.name.clone(),
        catalog,
        access,
        permit: WorkPermit {
            _principal: principal_permit,
            _budget: budget_permit,
        },
    })
}

fn publication_targets(
    configured: &[ManagedTarget],
    requested: &[Vec<u8>],
) -> Result<Vec<ManagedTarget>, Status> {
    if requested.is_empty() {
        return Err(Status::invalid_argument(
            "publication status requires at least one index_key",
        ));
    }
    let mut unique = std::collections::BTreeSet::new();
    requested
        .iter()
        .map(|key| {
            if !unique.insert(key.as_slice()) {
                return Err(Status::invalid_argument(
                    "publication status repeats an index_key",
                ));
            }
            configured
                .iter()
                .find(|target| target.index_key == *key)
                .cloned()
                .ok_or_else(|| Status::not_found("publication target is not configured"))
        })
        .collect()
}

fn extract_network_document(
    request: Request<DocumentWriteRequest>,
) -> Result<AcceptDocumentRequest, Status> {
    let document = request
        .into_inner()
        .document
        .ok_or_else(|| Status::invalid_argument("source write is missing document"))?;
    if document.contract_version != 2 || document.history_id.len() != 16 {
        return Err(Status::invalid_argument(
            "network source writes require contract_version 2 and an exact 16-byte history_id",
        ));
    }
    Ok(document)
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
                publication: BTreeMap::new(),
                budget: WriteBudget::new(limits)?,
            }),
        })
    }

    /// Register the managed source and serving targets visible to status reads.
    pub fn with_publication(
        mut self,
        source: Arc<ActiveManagedCatalog>,
        targets: Vec<ManagedTarget>,
    ) -> Result<Self, Status> {
        if targets.is_empty() {
            return Err(Status::invalid_argument(
                "publication status requires at least one target",
            ));
        }
        let collection = source.collection().to_string();
        let inner = Arc::get_mut(&mut self.inner).ok_or_else(|| {
            Status::failed_precondition("register publication before sharing the write service")
        })?;
        let publication = ManagedPublication { source, targets };
        if inner
            .publication
            .insert(collection.clone(), publication)
            .is_some()
        {
            return Err(Status::invalid_argument(format!(
                "publication status repeats collection {collection:?}"
            )));
        }
        Ok(self)
    }

    /// Use this builder when registering the adapter so tonic enforces the
    /// request byte limit during decoding as well as at execution admission.
    pub fn into_server(self) -> DocumentWriteServiceServer<Self> {
        let max_bytes = self.inner.budget.max_request_bytes();
        DocumentWriteServiceServer::new(self).max_decoding_message_size(max_bytes)
    }

    fn admit<T: Message>(
        &self,
        request: &Request<T>,
        collection: &str,
        write: bool,
    ) -> Result<(Arc<AccessControlledCatalog>, AccessPermit, WorkPermit), Status> {
        let admitted = admit_ingest(
            &self.inner.principals,
            &self.inner.catalogs,
            &self.inner.budget,
            request,
            collection,
            write,
        )?;
        Ok((admitted.catalog, admitted.access, admitted.permit))
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
        let publication = self
            .inner
            .publication
            .get(&collection)
            .cloned()
            .ok_or_else(|| {
                Status::failed_precondition(
                    "publication targets are not configured for this collection",
                )
            })?;
        let targets = publication_targets(&publication.targets, &request.get_ref().index_keys)?;
        let source = publication.source;
        let access = admitted.access.clone();
        let result = tokio::task::spawn_blocking(move || {
            let _permit = admitted.permit;
            let admission = source.local_admission(&admitted.actor)?;
            crate::node::publication_status_blocking(&source, &admission, sequence, &targets)
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
        let (catalog, access, admission) =
            self.admit(&request, &request.get_ref().collection, true)?;
        let document = extract_network_document(request)?;
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

impl_document_write_service!(DocumentWriteServiceImpl);

//! The collection-wide document catalog surface of an embedded runtime:
//! acceptance, projection staging and publication, and maintenance.

use std::sync::Arc;

use tonic::Status;

use super::EmbeddedSearch;
use crate::document_catalog::DocumentCatalog;
use crate::pb::*;

impl EmbeddedSearch {
    /// Persist the source/version and retry decision for this runtime's collection.
    /// This blocking local transaction does not yet publish an index projection.
    pub fn accept_document(
        &self,
        request: &AcceptDocumentRequest,
    ) -> Result<DocumentWriteReceipt, Status> {
        self.document_catalog()?.accept(request)
    }

    /// Trusted application-local lookup of an accepted source, including history.
    pub fn accepted_document(
        &self,
        key: &[u8],
        version: Option<u64>,
    ) -> Result<Option<(storage::DocumentVersion, Option<ProtobufSource>)>, Status> {
        self.document_catalog()?.get(key, version)
    }

    /// Trusted local source history for projection workers and recovery.
    pub fn read_accepted_documents(
        &self,
        request: &ReadAcceptedDocumentsRequest,
    ) -> Result<ReadAcceptedDocumentsResponse, Status> {
        self.document_catalog()?.read_accepted(request)
    }

    /// Prepare a complete, bounded batch from one accepted version. This is
    /// trusted local source access and publishes no search rows or receipts.
    pub fn prepare_document_projection(
        &self,
        request: &PrepareDocumentProjectionRequest,
    ) -> Result<PreparedDocumentProjection, Status> {
        self.document_catalog()?.prepare_projection(request)
    }

    /// Build private analyzed segments from this collection's accepted history.
    /// Staging does not mutate the serving shard or publish a write receipt.
    pub async fn stage_document_projection(
        &self,
        shard: usize,
        request: StageDocumentProjectionRequest,
    ) -> Result<crate::node::StagedDocumentCandidate, Status> {
        let node = self
            .nodes
            .get(shard)
            .ok_or_else(|| Status::invalid_argument("unknown embedded shard"))?;
        let catalog = self.document_catalog.as_ref().ok_or_else(|| {
            Status::failed_precondition("configure a document catalog before staging")
        })?;
        node.stage_document_projection(Arc::clone(catalog), request)
            .await
    }

    /// Activate a complete accepted candidate on one local shard. This returns
    /// a shard read-version observation, not a collection publication receipt.
    pub async fn publish_document_projection(
        &self,
        shard: usize,
        index_key: Vec<u8>,
        candidate: crate::node::StagedDocumentCandidate,
    ) -> Result<DocumentProjectionActivation, Status> {
        let node = Arc::clone(
            self.nodes
                .get(shard)
                .ok_or_else(|| Status::invalid_argument("unknown embedded shard"))?,
        );
        let catalog = Arc::clone(self.document_catalog.as_ref().ok_or_else(|| {
            Status::failed_precondition("configure a document catalog before publication")
        })?);
        tokio::task::spawn_blocking(move || {
            node.publish_document_projection_blocking(&catalog, &index_key, &candidate)
        })
        .await
        .map_err(|error| Status::internal(format!("projection publication worker: {error}")))?
    }

    /// Observe the reopened shard only after its artifact journal is reconciled.
    pub async fn recover_document_projection(
        &self,
        shard: usize,
        index_key: Vec<u8>,
    ) -> Result<Option<DocumentProjectionActivation>, Status> {
        let node = Arc::clone(
            self.nodes
                .get(shard)
                .ok_or_else(|| Status::invalid_argument("unknown embedded shard"))?,
        );
        let catalog = Arc::clone(self.document_catalog.as_ref().ok_or_else(|| {
            Status::failed_precondition("configure a document catalog before projection recovery")
        })?);
        tokio::task::spawn_blocking(move || {
            node.recover_document_projection_blocking(&catalog, &index_key)
        })
        .await
        .map_err(|error| Status::internal(format!("projection recovery worker: {error}")))?
    }

    /// Compact stored source rows locally, without analysis or network access.
    /// The blocking operation owns its node, source catalog and private files.
    /// Dropping the awaiter does not cancel an already running publication;
    /// recover_document_maintenance observes its durable outcome after reopen.
    pub async fn compact_document_index(
        &self,
        shard: usize,
        request: CompactDocumentIndexRequest,
    ) -> Result<DocumentMaintenanceActivation, Status> {
        let node = Arc::clone(
            self.nodes
                .get(shard)
                .ok_or_else(|| Status::invalid_argument("unknown embedded shard"))?,
        );
        let catalog = Arc::clone(self.document_catalog.as_ref().ok_or_else(|| {
            Status::failed_precondition("configure a document catalog before compaction")
        })?);
        tokio::task::spawn_blocking(move || node.compact_document_index_blocking(&catalog, request))
            .await
            .map_err(|error| Status::internal(format!("source compaction worker: {error}")))?
    }

    /// Reconcile and observe the reopened shard's current maintenance decision.
    pub async fn recover_document_maintenance(
        &self,
        shard: usize,
        index_key: Vec<u8>,
    ) -> Result<Option<DocumentMaintenanceActivation>, Status> {
        let node = Arc::clone(
            self.nodes
                .get(shard)
                .ok_or_else(|| Status::invalid_argument("unknown embedded shard"))?,
        );
        let catalog = Arc::clone(self.document_catalog.as_ref().ok_or_else(|| {
            Status::failed_precondition("configure a document catalog before maintenance recovery")
        })?);
        tokio::task::spawn_blocking(move || {
            node.recover_document_maintenance_blocking(&catalog, &index_key)
        })
        .await
        .map_err(|error| Status::internal(format!("maintenance recovery worker: {error}")))?
    }

    fn document_catalog(&self) -> Result<&DocumentCatalog, Status> {
        self.document_catalog.as_deref().ok_or_else(|| {
            Status::failed_precondition(
                "configure a collection-wide document catalog before accepting documents",
            )
        })
    }
}

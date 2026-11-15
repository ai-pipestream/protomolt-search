//! Logical source versions and persistent retry decisions, independent of rows.
//! Source acceptance does not publish a search projection.

use std::fs::{File, OpenOptions};
use std::path::Path;

use prost::Message;
use redb::{
    Database, Durability, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition,
};
use tonic::Status;

use crate::pb::storage::{
    DocumentCatalogHeader, DocumentOperation, DocumentVersion, DocumentVersionKey,
    SourceManagedActivation, SourceManagedBinding, SourceRecord, SourceResourceBinding,
    WriteOutcome,
};
use crate::pb::{
    accept_document_request::Mutation, AcceptDocumentRequest, AcceptedDocumentVersion,
    DocumentWriteReceipt, ProtobufSource, ReadAcceptedDocumentsRequest,
    ReadAcceptedDocumentsResponse,
};
use crate::sha256;

mod accept;
mod access;
mod actors;
mod backup;
mod checkpoint;
mod managed;
mod open;
mod outcome;
mod projection;
mod publication;
mod read;
mod restore;
mod retry;
mod seal;
pub use access::AccessControlledCatalog;
pub use backup::CapturedBackup;
pub use checkpoint::CatalogCheckpoint;
#[cfg(all(feature = "net", feature = "raft"))]
pub(crate) use managed::header_binding;
#[cfg(feature = "net")]
pub use managed::{Acceptance, ActiveManagedCatalog, PreparedManagedCatalog, Settlement};
pub use publication::{MaintenanceRecovery, ProjectionRecovery};
pub use restore::VerifiedSourceRestore;

const META: TableDefinition<&str, &[u8]> = TableDefinition::new("metadata");
const HEADS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("heads");
const VERSIONS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("versions");
const OPERATIONS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("operations");
const DESCRIPTORS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("descriptors");
const SOURCES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("sources");
const CHANGES: TableDefinition<u64, &[u8]> = TableDefinition::new("changes");
const FORMAT_VERSION: u32 = 3;
const SEALED_FORMAT_VERSION: u32 = 4;
const RETIRING_FORMAT_VERSION: u32 = 5;
const RETIRED_FORMAT_VERSION: u32 = 6;
const LEGACY_ACCESS_CONTROLLED_FORMAT: u32 = 7;
const ACCESS_CONTROLLED_FORMAT: u32 = 8;
const MANAGED_FORMAT: u32 = 9;
// Format 10 is the activated managed source without write outcomes;
// format 11 records an outcome and the write epoch on every version and
// operation.
const ACTIVE_MANAGED_FORMAT_BEFORE_OUTCOMES: u32 = 10;
pub(crate) const ACTIVE_MANAGED_FORMAT: u32 = 11;
const CACHE_BYTES: usize = 8 << 20;

fn storage(error: impl std::fmt::Display) -> Status {
    Status::internal(format!("document catalog: {error}"))
}
fn decode<T: Message + Default>(bytes: &[u8]) -> Result<T, Status> {
    T::decode(bytes).map_err(|e| Status::data_loss(format!("document catalog record: {e}")))
}

/// One accepted write as the catalog recorded it: the receipt and the
/// outcome the record holds.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Written {
    pub(crate) receipt: DocumentWriteReceipt,
    pub(crate) outcome: WriteOutcome,
}

fn decode_header(bytes: &[u8]) -> Result<DocumentCatalogHeader, Status> {
    if bytes.len() > 256 * 1024 {
        return Err(Status::data_loss("source catalog header exceeds 256KiB"));
    }
    let header: DocumentCatalogHeader = decode(bytes)?;
    if matches!(
        header.format_version,
        MANAGED_FORMAT | ACTIVE_MANAGED_FORMAT
    ) && header.encode_to_vec() != bytes
    {
        return Err(Status::data_loss(
            "managed source header has unknown or noncanonical fields",
        ));
    }
    Ok(header)
}

fn valid_history_id(id: &[u8]) -> bool {
    id.len() == 16 && id.iter().any(|byte| *byte != 0)
}

fn new_history_id() -> Result<Vec<u8>, Status> {
    let mut id = vec![0; 16];
    getrandom::fill(&mut id).map_err(storage)?;
    if !valid_history_id(&id) {
        return Err(Status::internal(
            "catalog history entropy returned an invalid identity",
        ));
    }
    Ok(id)
}

fn validate_current_header(header: &DocumentCatalogHeader) -> Result<(), Status> {
    if header.format_version == ACTIVE_MANAGED_FORMAT_BEFORE_OUTCOMES {
        return Err(Status::failed_precondition(
            "document catalog format 10 (an activated managed source from before write outcomes) is not served; format 11 records an outcome on every write, and the source is rebuilt from its authority rather than migrated",
        ));
    }
    if !matches!(
        header.format_version,
        FORMAT_VERSION
            | SEALED_FORMAT_VERSION
            | RETIRING_FORMAT_VERSION
            | RETIRED_FORMAT_VERSION
            | LEGACY_ACCESS_CONTROLLED_FORMAT
            | ACCESS_CONTROLLED_FORMAT
            | MANAGED_FORMAT
            | ACTIVE_MANAGED_FORMAT
    ) || !valid_history_id(&header.history_id)
        || header.legacy_receipts_through_sequence > header.accepted_sequence
    {
        return Err(Status::data_loss(
            "invalid document catalog history identity or migration boundary",
        ));
    }
    seal::validate_header_seal(header)?;
    actors::validate_namespace(header)?;
    managed::validate_header(header)?;
    Ok(())
}

pub struct DocumentCatalog {
    database: Database,
    durable: bool,
    resource_binding: Option<SourceResourceBinding>,
    managed_binding: Option<SourceManagedBinding>,
    // The committed fence this handle was activated under. A handle without
    // it never writes a managed source, whatever the file records.
    managed_activation: Option<SourceManagedActivation>,
    // redb's fallback backend does not lock on every mobile platform. Hold
    // the same open file description exclusively for this catalog's lifetime.
    // Drop the database before releasing this guard.
    _file_lock: Option<File>,
    // Fault injection: a pause inside the write transaction, after every
    // wait and before the write's final admission check.
    #[cfg(any(test, feature = "fault-injection"))]
    precommit_pause: std::sync::Mutex<Option<std::time::Duration>>,
    // Fault injection: a pause after the write's commit returned durable
    // and before its outcome is judged against the lease.
    #[cfg(any(test, feature = "fault-injection"))]
    postcommit_pause: std::sync::Mutex<Option<std::time::Duration>>,
}

impl DocumentCatalog {
    fn validate_resource_binding(&self, header: &DocumentCatalogHeader) -> Result<(), Status> {
        if header.managed_binding != self.managed_binding {
            return Err(Status::failed_precondition(
                "managed source binding differs; managed storage requires its exact owner and closed recovery adapter",
            ));
        }
        if header.managed_activation != self.managed_activation {
            return Err(Status::failed_precondition(
                "managed source activation differs; writes require the handle activated under the committed fence",
            ));
        }
        if header.resource_binding != self.resource_binding {
            return Err(Status::failed_precondition(
                "document catalog resource binding differs; access-controlled catalogs require their bound workspace and collection",
            ));
        }
        Ok(())
    }

    /// Immutable collection binding of this local source authority.
    pub fn collection(&self) -> Result<String, Status> {
        let transaction = self.database.begin_read().map_err(storage)?;
        let meta = transaction.open_table(META).map_err(storage)?;
        let header: DocumentCatalogHeader = decode_header(
            meta.get("header")
                .map_err(storage)?
                .ok_or_else(|| Status::data_loss("catalog header missing"))?
                .value(),
        )?;
        validate_current_header(&header)?;
        Ok(header.collection)
    }
}

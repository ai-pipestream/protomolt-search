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
    DocumentCatalogHeader, DocumentOperation, DocumentVersion, DocumentVersionKey, SourceRecord,
};
use crate::pb::{
    accept_document_request::Mutation, AcceptDocumentRequest, AcceptedDocumentVersion,
    DocumentWriteReceipt, ProtobufSource, ReadAcceptedDocumentsRequest,
    ReadAcceptedDocumentsResponse,
};
use crate::sha256;

const META: TableDefinition<&str, &[u8]> = TableDefinition::new("metadata");
const HEADS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("heads");
const VERSIONS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("versions");
const OPERATIONS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("operations");
const DESCRIPTORS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("descriptors");
const SOURCES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("sources");
const CHANGES: TableDefinition<u64, &[u8]> = TableDefinition::new("changes");
const FORMAT_VERSION: u32 = 2;
const CACHE_BYTES: usize = 8 << 20;

fn storage(error: impl std::fmt::Display) -> Status {
    Status::internal(format!("document catalog: {error}"))
}
fn decode<T: Message + Default>(bytes: &[u8]) -> Result<T, Status> {
    T::decode(bytes).map_err(|e| Status::data_loss(format!("document catalog record: {e}")))
}

pub struct DocumentCatalog {
    database: Database,
    durable: bool,
    // redb's fallback backend does not lock on every mobile platform. Hold
    // the same open file description exclusively for this catalog's lifetime.
    // Drop the database before releasing this guard.
    _file_lock: Option<File>,
}

impl DocumentCatalog {
    pub fn open(path: &Path, collection: &str) -> Result<Self, Status> {
        Self::open_file(path, collection, false)
    }

    pub fn create(path: &Path, collection: &str) -> Result<Self, Status> {
        Self::open_file(path, collection, true)
    }

    fn open_file(path: &Path, collection: &str, create: bool) -> Result<Self, Status> {
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        // The application supplies an existing durable container directory.
        // Creating ancestors here would require syncing each ancestor as well.
        let directory = File::open(parent).map_err(storage)?;
        let create_file = || {
            OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(path)
        };
        let opened = if create {
            create_file().map(|file| (file, true))
        } else {
            match OpenOptions::new().read(true).write(true).open(path) {
                Ok(file) => Ok((file, false)),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    create_file().map(|file| (file, true))
                }
                Err(error) => Err(error),
            }
        };
        let (file, new) = opened.map_err(|error| match error.kind() {
            std::io::ErrorKind::AlreadyExists => {
                Status::already_exists("document catalog already exists")
            }
            _ => storage(error),
        })?;
        file.try_lock().map_err(|e| {
            Status::failed_precondition(format!("exclusive document catalog lock unavailable: {e}"))
        })?;
        if !new && file.metadata().map_err(storage)?.len() == 0 {
            return Err(Status::data_loss("existing document catalog is empty"));
        }
        let mut builder = Database::builder();
        builder.set_cache_size(CACHE_BYTES);
        let database = builder
            .create_file(file.try_clone().map_err(storage)?)
            .map_err(storage)?;
        let catalog = Self {
            database,
            durable: true,
            _file_lock: Some(file),
        };
        catalog.initialize(collection, new)?;
        directory.sync_all().map_err(storage)?;
        Ok(catalog)
    }

    pub fn in_memory(collection: &str) -> Result<Self, Status> {
        let mut builder = Database::builder();
        builder.set_cache_size(CACHE_BYTES);
        let database = builder
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .map_err(storage)?;
        let catalog = Self {
            database,
            durable: false,
            _file_lock: None,
        };
        catalog.initialize(collection, true)?;
        Ok(catalog)
    }

    fn initialize(&self, collection: &str, new: bool) -> Result<(), Status> {
        if !new {
            let transaction = self.database.begin_read().map_err(storage)?;
            let table = transaction.open_table(META).map_err(storage)?;
            let bytes = table
                .get("header")
                .map_err(storage)?
                .ok_or_else(|| Status::data_loss("existing document catalog header missing"))?;
            let header: DocumentCatalogHeader = decode(bytes.value())?;
            if !(1..=FORMAT_VERSION).contains(&header.format_version)
                || header.collection != collection
            {
                return Err(Status::failed_precondition(
                    "document catalog format or collection differs",
                ));
            }
            for definition in [HEADS, VERSIONS, OPERATIONS, DESCRIPTORS, SOURCES] {
                transaction.open_table(definition).map_err(storage)?;
            }
            if header.format_version == 1 {
                drop(bytes);
                drop(table);
                drop(transaction);
                return self.upgrade_ordered_history();
            }
            transaction.open_table(CHANGES).map_err(storage)?;
            return Ok(());
        }
        let mut transaction = self.database.begin_write().map_err(storage)?;
        transaction
            .set_durability(Durability::Immediate)
            .map_err(storage)?;
        {
            let mut table = transaction.open_table(META).map_err(storage)?;
            let header = DocumentCatalogHeader {
                format_version: FORMAT_VERSION,
                collection: collection.into(),
                accepted_sequence: 0,
            }
            .encode_to_vec();
            table.insert("header", header.as_slice()).map_err(storage)?;
        }
        for definition in [HEADS, VERSIONS, OPERATIONS, DESCRIPTORS, SOURCES] {
            transaction.open_table(definition).map_err(storage)?;
        }
        transaction.open_table(CHANGES).map_err(storage)?;
        transaction.commit().map_err(storage)
    }

    fn upgrade_ordered_history(&self) -> Result<(), Status> {
        let mut transaction = self.database.begin_write().map_err(storage)?;
        transaction
            .set_durability(Durability::Immediate)
            .map_err(storage)?;
        {
            let mut meta = transaction.open_table(META).map_err(storage)?;
            let mut header: DocumentCatalogHeader = decode(
                meta.get("header")
                    .map_err(storage)?
                    .ok_or_else(|| Status::data_loss("catalog header missing"))?
                    .value(),
            )?;
            let versions = transaction.open_table(VERSIONS).map_err(storage)?;
            let mut changes = transaction.open_table(CHANGES).map_err(storage)?;
            if !changes.is_empty().map_err(storage)? {
                return Err(Status::data_loss(
                    "version-1 catalog already contains a change index",
                ));
            }
            for entry in versions.iter().map_err(storage)? {
                let (key, value) = entry.map_err(storage)?;
                let version: DocumentVersion = decode(value.value())?;
                let valid_key =
                    !version.document_key.is_empty() && version.document_key.len() <= 16 * 1024;
                let expected = DocumentVersionKey {
                    document_key: version.document_key,
                    version: version.version,
                }
                .encode_to_vec();
                if version.version == 0
                    || !valid_key
                    || key.value() != expected
                    || version.accepted_sequence == 0
                    || version.accepted_sequence > header.accepted_sequence
                {
                    return Err(Status::data_loss("invalid version in catalog history"));
                }
                if changes
                    .insert(version.accepted_sequence, key.value())
                    .map_err(storage)?
                    .is_some()
                {
                    return Err(Status::data_loss("duplicate sequence in catalog history"));
                }
            }
            if changes.len().map_err(storage)? != header.accepted_sequence {
                return Err(Status::data_loss(
                    "catalog history has missing accepted versions",
                ));
            }
            header.format_version = FORMAT_VERSION;
            meta.insert("header", header.encode_to_vec().as_slice())
                .map_err(storage)?;
        }
        transaction.commit().map_err(storage)
    }

    /// Atomically accept a version and its retry decision. A retry is resolved
    /// before the version precondition, including after later writes or delete.
    pub fn accept(&self, request: &AcceptDocumentRequest) -> Result<DocumentWriteReceipt, Status> {
        if request.contract_version != 1 {
            return Err(Status::invalid_argument(
                "document write contract_version must be 1",
            ));
        }
        if request.document_key.is_empty() || request.document_key.len() > 16 * 1024 {
            return Err(Status::invalid_argument(
                "document_key must contain 1 to 16384 bytes",
            ));
        }
        if request.operation_id.is_empty() || request.operation_id.len() > 1024 {
            return Err(Status::invalid_argument(
                "operation_id must contain 1 to 1024 bytes",
            ));
        }
        match &request.mutation {
            Some(Mutation::Source(source))
                if !source.descriptor_set.is_empty() && !source.message_type.is_empty() => {}
            Some(Mutation::Delete(true)) => {}
            _ => {
                return Err(Status::invalid_argument(
                    "write requires original descriptor/type bytes or delete=true",
                ))
            }
        }
        let request_sha = sha256::digest(&request.encode_to_vec());
        let mut transaction = self.database.begin_write().map_err(storage)?;
        transaction
            .set_durability(Durability::Immediate)
            .map_err(storage)?;
        {
            let operations = transaction.open_table(OPERATIONS).map_err(storage)?;
            if let Some(previous) = operations
                .get(request.operation_id.as_slice())
                .map_err(storage)?
            {
                let previous: DocumentOperation = decode(previous.value())?;
                if previous.request_sha256 != request_sha {
                    return Err(Status::already_exists(
                        "operation_id was used for a different document write",
                    ));
                }
                let mut receipt = previous
                    .receipt
                    .ok_or_else(|| Status::data_loss("operation receipt missing"))?;
                receipt.replayed = true;
                return Ok(receipt);
            };
        }
        let previous: Option<DocumentVersion> = {
            let heads = transaction.open_table(HEADS).map_err(storage)?;
            let previous = heads
                .get(request.document_key.as_slice())
                .map_err(storage)?
                .map(|v| decode(v.value()))
                .transpose()?;
            previous
        };
        let current_version = previous.as_ref().map_or(0, |v| v.version);
        if request
            .expected_version
            .is_some_and(|v| v != current_version)
        {
            return Err(Status::aborted("document version precondition failed"));
        }
        let version = current_version
            .checked_add(1)
            .ok_or_else(|| Status::out_of_range("document version exhausted"))?;
        let mut header: DocumentCatalogHeader = {
            let meta = transaction.open_table(META).map_err(storage)?;
            let header = decode(
                meta.get("header")
                    .map_err(storage)?
                    .ok_or_else(|| Status::data_loss("catalog header missing"))?
                    .value(),
            )?;
            header
        };
        header.accepted_sequence = header
            .accepted_sequence
            .checked_add(1)
            .ok_or_else(|| Status::out_of_range("catalog sequence exhausted"))?;
        let source_sha256 = match request.mutation.as_ref().expect("validated") {
            Mutation::Source(source) => {
                let descriptor_sha = sha256::digest(&source.descriptor_set);
                let record = SourceRecord {
                    descriptor_sha256: descriptor_sha.to_vec(),
                    message_type: source.message_type.clone(),
                    payload: source.payload.clone(),
                }
                .encode_to_vec();
                let source_sha = sha256::digest(&record);
                for (definition, hash, bytes) in [
                    (
                        DESCRIPTORS,
                        descriptor_sha,
                        source.descriptor_set.as_slice(),
                    ),
                    (SOURCES, source_sha, record.as_slice()),
                ] {
                    let mut table = transaction.open_table(definition).map_err(storage)?;
                    let existing = table
                        .get(hash.as_slice())
                        .map_err(storage)?
                        .map(|v| v.value().to_vec());
                    if let Some(existing) = existing {
                        if existing != bytes {
                            return Err(Status::data_loss("source content-address collision"));
                        }
                    } else {
                        table.insert(hash.as_slice(), bytes).map_err(storage)?;
                    }
                }
                source_sha.to_vec()
            }
            Mutation::Delete(_) => Vec::new(),
        };
        let document = DocumentVersion {
            document_key: request.document_key.clone(),
            version,
            accepted_sequence: header.accepted_sequence,
            source_sha256,
            deleted: matches!(request.mutation, Some(Mutation::Delete(_))),
        };
        let receipt = DocumentWriteReceipt {
            document_key: request.document_key.clone(),
            version,
            accepted_sequence: header.accepted_sequence,
            accepted: true,
            searchable: false,
            durable: self.durable,
            replayed: false,
        };
        let document_bytes = document.encode_to_vec();
        let version_key = DocumentVersionKey {
            document_key: request.document_key.clone(),
            version,
        }
        .encode_to_vec();
        transaction
            .open_table(HEADS)
            .map_err(storage)?
            .insert(request.document_key.as_slice(), document_bytes.as_slice())
            .map_err(storage)?;
        transaction
            .open_table(VERSIONS)
            .map_err(storage)?
            .insert(version_key.as_slice(), document_bytes.as_slice())
            .map_err(storage)?;
        if transaction
            .open_table(CHANGES)
            .map_err(storage)?
            .insert(header.accepted_sequence, version_key.as_slice())
            .map_err(storage)?
            .is_some()
        {
            return Err(Status::data_loss("accepted sequence already exists"));
        }
        let operation = DocumentOperation {
            request_sha256: request_sha.to_vec(),
            receipt: Some(receipt.clone()),
        }
        .encode_to_vec();
        transaction
            .open_table(OPERATIONS)
            .map_err(storage)?
            .insert(request.operation_id.as_slice(), operation.as_slice())
            .map_err(storage)?;
        transaction
            .open_table(META)
            .map_err(storage)?
            .insert("header", header.encode_to_vec().as_slice())
            .map_err(storage)?;
        transaction.commit().map_err(storage)?;
        Ok(receipt)
    }

    /// Trusted local source lookup. No network or search-result disclosure API.
    pub fn get(
        &self,
        key: &[u8],
        version: Option<u64>,
    ) -> Result<Option<(DocumentVersion, Option<ProtobufSource>)>, Status> {
        let transaction = self.database.begin_read().map_err(storage)?;
        Self::get_from(&transaction, key, version)
    }

    fn get_from(
        transaction: &redb::ReadTransaction,
        key: &[u8],
        version: Option<u64>,
    ) -> Result<Option<(DocumentVersion, Option<ProtobufSource>)>, Status> {
        let version_key = version.map(|version| {
            DocumentVersionKey {
                document_key: key.to_vec(),
                version,
            }
            .encode_to_vec()
        });
        let document = match &version_key {
            Some(key) => transaction
                .open_table(VERSIONS)
                .map_err(storage)?
                .get(key.as_slice())
                .map_err(storage)?
                .map(|v| v.value().to_vec()),
            None => transaction
                .open_table(HEADS)
                .map_err(storage)?
                .get(key)
                .map_err(storage)?
                .map(|v| v.value().to_vec()),
        };
        let Some(document) = document else {
            return Ok(None);
        };
        let document: DocumentVersion = decode(&document)?;
        if document.document_key != key || version.is_some_and(|v| document.version != v) {
            return Err(Status::data_loss("document key/version mismatch"));
        }
        if document.deleted {
            return Ok(Some((document, None)));
        }
        let sources = transaction.open_table(SOURCES).map_err(storage)?;
        let bytes = sources
            .get(document.source_sha256.as_slice())
            .map_err(storage)?
            .ok_or_else(|| Status::data_loss("document source missing"))?;
        if sha256::digest(bytes.value()).as_slice() != document.source_sha256 {
            return Err(Status::data_loss("document source checksum mismatch"));
        }
        let source: SourceRecord = decode(bytes.value())?;
        let descriptors = transaction.open_table(DESCRIPTORS).map_err(storage)?;
        let bytes = descriptors
            .get(source.descriptor_sha256.as_slice())
            .map_err(storage)?
            .ok_or_else(|| Status::data_loss("document descriptor missing"))?;
        if sha256::digest(bytes.value()).as_slice() != source.descriptor_sha256 {
            return Err(Status::data_loss("document descriptor checksum mismatch"));
        }
        Ok(Some((
            document,
            Some(ProtobufSource {
                descriptor_set: bytes.value().to_vec(),
                message_type: source.message_type,
                payload: source.payload,
            }),
        )))
    }

    /// Read an immutable prefix of accepted history, independent of current
    /// heads and physical rows. Every page uses one database read snapshot.
    pub fn read_accepted(
        &self,
        request: &ReadAcceptedDocumentsRequest,
    ) -> Result<ReadAcceptedDocumentsResponse, Status> {
        if !(1..=1000).contains(&request.limit) {
            return Err(Status::invalid_argument(
                "accepted history limit must be 1 to 1000",
            ));
        }
        if !(1..=64 * 1024 * 1024).contains(&request.max_bytes) {
            return Err(Status::invalid_argument(
                "accepted history max_bytes must be 1 to 64 MiB",
            ));
        }
        let transaction = self.database.begin_read().map_err(storage)?;
        let meta = transaction.open_table(META).map_err(storage)?;
        let header: DocumentCatalogHeader = decode(
            meta.get("header")
                .map_err(storage)?
                .ok_or_else(|| Status::data_loss("catalog header missing"))?
                .value(),
        )?;
        let fence = request.through_sequence.unwrap_or(header.accepted_sequence);
        if fence > header.accepted_sequence || request.after_sequence > fence {
            return Err(Status::invalid_argument(
                "accepted history cursor exceeds its fence or committed history",
            ));
        }
        let mut response = ReadAcceptedDocumentsResponse {
            through_sequence: fence,
            next_sequence: request.after_sequence,
            complete: request.after_sequence == fence,
            documents: Vec::new(),
        };
        if response.complete {
            return Ok(response);
        }
        let changes = transaction.open_table(CHANGES).map_err(storage)?;
        let versions = transaction.open_table(VERSIONS).map_err(storage)?;
        let mut remaining = request.max_bytes;
        for sequence in request.after_sequence + 1..=fence {
            if response.documents.len() == request.limit as usize {
                break;
            }
            let bytes = changes
                .get(sequence)
                .map_err(storage)?
                .ok_or_else(|| Status::data_loss("accepted history sequence missing"))?;
            let key: DocumentVersionKey = decode(bytes.value())?;
            let stored = versions
                .get(bytes.value())
                .map_err(storage)?
                .ok_or_else(|| Status::data_loss("accepted history version missing"))?;
            let metadata: DocumentVersion = decode(stored.value())?;
            if !metadata.deleted && !Self::source_fits(&transaction, &metadata, remaining)? {
                if response.documents.is_empty() {
                    return Err(Status::resource_exhausted(
                        "accepted version exceeds max_bytes",
                    ));
                }
                break;
            }
            let (version, source) =
                Self::get_from(&transaction, &key.document_key, Some(key.version))?
                    .ok_or_else(|| Status::data_loss("accepted history version missing"))?;
            if version.accepted_sequence != sequence {
                return Err(Status::data_loss(
                    "accepted history sequence/version mismatch",
                ));
            }
            let document = AcceptedDocumentVersion {
                document_key: version.document_key,
                version: version.version,
                accepted_sequence: sequence,
                mutation: Some(match source {
                    Some(source) => crate::pb::accepted_document_version::Mutation::Source(source),
                    None => crate::pb::accepted_document_version::Mutation::Deleted(true),
                }),
            };
            let size = document.encoded_len() as u64;
            if size > remaining {
                if response.documents.is_empty() {
                    return Err(Status::resource_exhausted(
                        "accepted version exceeds max_bytes",
                    ));
                }
                break;
            }
            remaining -= size;
            response.documents.push(document);
            response.next_sequence = sequence;
        }
        response.complete = response.next_sequence == fence;
        Ok(response)
    }

    // Inspect borrowed blob sizes before copying source/descriptor payloads.
    // SourceRecord's 32-byte descriptor hash contributes 34 encoded bytes,
    // replaced by the descriptor itself in the public source envelope.
    fn source_fits(
        transaction: &redb::ReadTransaction,
        version: &DocumentVersion,
        budget: u64,
    ) -> Result<bool, Status> {
        let sources = transaction.open_table(SOURCES).map_err(storage)?;
        let bytes = sources
            .get(version.source_sha256.as_slice())
            .map_err(storage)?
            .ok_or_else(|| Status::data_loss("document source missing"))?;
        let lower_bound = (bytes.value().len() as u64).saturating_sub(34);
        if lower_bound > budget {
            return Ok(false);
        }
        let source: SourceRecord = decode(bytes.value())?;
        let descriptors = transaction.open_table(DESCRIPTORS).map_err(storage)?;
        let descriptor = descriptors
            .get(source.descriptor_sha256.as_slice())
            .map_err(storage)?
            .ok_or_else(|| Status::data_loss("document descriptor missing"))?;
        Ok(lower_bound.saturating_add(descriptor.value().len() as u64) <= budget)
    }
}

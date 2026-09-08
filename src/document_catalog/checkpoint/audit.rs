//! Check accepted history without changing records or retaining a corpus-size set.
use super::*;

const SEEN: TableDefinition<u64, u8> = TableDefinition::new("receipt_sequences");
const BATCH: usize = 1024;

fn damaged(message: &str) -> Status {
    Status::data_loss(format!("source history audit: {message}"))
}
fn bounded<T: Message + Default>(bytes: &[u8], limit: usize) -> Result<T, Status> {
    if bytes.len() > limit {
        return Err(Status::resource_exhausted(
            "source history record exceeds source_batch_bytes",
        ));
    }
    decode(bytes)
}
fn version_key(document: &DocumentVersion) -> Vec<u8> {
    DocumentVersionKey {
        document_key: document.document_key.clone(),
        version: document.version,
    }
    .encode_to_vec()
}
fn version_valid(document: &DocumentVersion, through: u64) -> Result<(), Status> {
    if document.document_key.is_empty()
        || document.document_key.len() > 16 * 1024
        || document.version == 0
        || document.accepted_sequence == 0
        || document.accepted_sequence > through
        || if document.deleted {
            !document.source_sha256.is_empty()
        } else {
            document.source_sha256.len() != 32
        }
    {
        return Err(damaged("invalid accepted version"));
    }
    Ok(())
}

impl CatalogCheckpoint<'_> {
    /// Validate this pinned source history. Scratch is temporary, exclusively
    /// created, bounded on disk, and removed on success or failure. It indexes
    /// receipt sequences to prove one retry decision per accepted write.
    pub(in crate::document_catalog) fn verify_source_history(
        &self,
        scratch: &Path,
        record_bytes: usize,
        scratch_bytes: u64,
    ) -> Result<(), Status> {
        let header = self.metadata.header.as_ref().expect("captured header");
        publication::verify_checkpoint_chain(
            &self.read,
            header,
            &self.metadata.indexes,
            record_bytes,
        )?;
        let versions = self.read.open_table(VERSIONS).map_err(storage)?;
        let changes = self.read.open_table(CHANGES).map_err(storage)?;
        let heads = self.read.open_table(HEADS).map_err(storage)?;
        actors::validate_read_counts(&self.read, header)?;
        let sources = self.read.open_table(SOURCES).map_err(storage)?;
        let descriptors = self.read.open_table(DESCRIPTORS).map_err(storage)?;
        if versions.len().map_err(storage)? != header.accepted_sequence
            || changes.len().map_err(storage)? != header.accepted_sequence
        {
            return Err(damaged(
                "accepted history table counts differ from the header",
            ));
        }
        // Forward and reverse links plus exact counts establish the complete
        // sequence/version bijection, including history older than every tip.
        let mut expected = 0u64;
        for entry in changes.iter().map_err(storage)? {
            let (sequence, key) = entry.map_err(storage)?;
            expected = expected
                .checked_add(1)
                .ok_or_else(|| damaged("sequence overflow"))?;
            if sequence.value() != expected {
                return Err(damaged("accepted sequence is missing or out of order"));
            }
            let decoded: DocumentVersionKey = bounded(key.value(), record_bytes)?;
            let stored = versions
                .get(key.value())
                .map_err(storage)?
                .ok_or_else(|| damaged("accepted sequence has no version"))?;
            let document: DocumentVersion = bounded(stored.value(), record_bytes)?;
            version_valid(&document, header.accepted_sequence)?;
            if decoded.document_key != document.document_key
                || decoded.version != document.version
                || version_key(&document) != key.value()
                || document.accepted_sequence != expected
            {
                return Err(damaged("accepted sequence/version link differs"));
            }
        }
        for entry in versions.iter().map_err(storage)? {
            let (key, value) = entry.map_err(storage)?;
            let document: DocumentVersion = bounded(value.value(), record_bytes)?;
            version_valid(&document, header.accepted_sequence)?;
            if version_key(&document) != key.value()
                || changes
                    .get(document.accepted_sequence)
                    .map_err(storage)?
                    .is_none_or(|v| v.value() != key.value())
            {
                return Err(damaged("version has no matching accepted sequence"));
            }
            let head = heads
                .get(document.document_key.as_slice())
                .map_err(storage)?
                .ok_or_else(|| damaged("version has no document head"))?;
            let head: DocumentVersion = bounded(head.value(), record_bytes)?;
            if head.document_key != document.document_key || head.version < document.version {
                return Err(damaged("document head precedes accepted history"));
            }
            if document.version > 1 {
                let previous_key = DocumentVersionKey {
                    document_key: document.document_key.clone(),
                    version: document.version - 1,
                }
                .encode_to_vec();
                let previous = versions
                    .get(previous_key.as_slice())
                    .map_err(storage)?
                    .ok_or_else(|| damaged("document version predecessor is missing"))?;
                let previous: DocumentVersion = bounded(previous.value(), record_bytes)?;
                if previous.accepted_sequence >= document.accepted_sequence {
                    return Err(damaged("document version order contradicts accepted order"));
                }
            }
            if !document.deleted
                && sources
                    .get(document.source_sha256.as_slice())
                    .map_err(storage)?
                    .is_none()
            {
                return Err(damaged("accepted version source blob is missing"));
            }
        }
        for entry in heads.iter().map_err(storage)? {
            let (key, value) = entry.map_err(storage)?;
            let head: DocumentVersion = bounded(value.value(), record_bytes)?;
            version_valid(&head, header.accepted_sequence)?;
            if key.value() != head.document_key {
                return Err(damaged("document head key differs"));
            }
            let key = version_key(&head);
            let version = versions
                .get(key.as_slice())
                .map_err(storage)?
                .ok_or_else(|| damaged("document head version is missing"))?;
            if bounded::<DocumentVersion>(version.value(), record_bytes)? != head {
                return Err(damaged("document head differs from its version"));
            }
        }
        for entry in descriptors.iter().map_err(storage)? {
            let (key, bytes) = entry.map_err(storage)?;
            if bytes.value().len() > record_bytes {
                return Err(Status::resource_exhausted(
                    "source descriptor exceeds source_batch_bytes",
                ));
            }
            if bytes.value().is_empty() || sha256::digest(bytes.value()).as_slice() != key.value() {
                return Err(damaged("descriptor content-address mismatch"));
            }
        }
        for entry in sources.iter().map_err(storage)? {
            let (key, bytes) = entry.map_err(storage)?;
            let source: SourceRecord = bounded(bytes.value(), record_bytes)?;
            if sha256::digest(bytes.value()).as_slice() != key.value()
                || source.message_type.is_empty()
                || source.descriptor_sha256.len() != 32
            {
                return Err(damaged("source content-address or envelope mismatch"));
            }
            if descriptors
                .get(source.descriptor_sha256.as_slice())
                .map_err(storage)?
                .is_none()
            {
                return Err(damaged("source descriptor is missing"));
            }
        }

        // Counts alone would allow two operation IDs to refer to the same
        // receipt while another accepted write lost its retry decision.
        let mut owned = OwnedFile {
            path: scratch,
            keep: true,
        };
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(scratch).map_err(storage)?;
        owned.keep = false;
        let mut builder = Database::builder();
        builder.set_cache_size(CACHE_BYTES);
        let proof = builder
            .create_file(file.try_clone().map_err(storage)?)
            .map_err(storage)?;
        let within_budget = || {
            if file.metadata().map_err(storage)?.len() > scratch_bytes {
                Err(Status::resource_exhausted(
                    "source audit scratch exceeds max_bytes",
                ))
            } else {
                Ok(())
            }
        };
        within_budget()?;
        let mut attributed = 0u64;
        for definition in [OPERATIONS, actors::OPERATIONS] {
            let scoped = definition.name() == actors::OPERATIONS.name();
            if scoped && header.actor_namespace.is_none() {
                continue;
            }
            let operations = self.read.open_table(definition).map_err(storage)?;
            let mut entries = operations.iter().map_err(storage)?;
            loop {
                let mut tx = proof.begin_write().map_err(storage)?;
                tx.set_durability(Durability::None).map_err(storage)?;
                let mut seen = tx.open_table(SEEN).map_err(storage)?;
                let mut count = 0;
                for _ in 0..BATCH {
                    let Some(entry) = entries.next() else { break };
                    let (key, value) = entry.map_err(storage)?;
                    let operation: DocumentOperation = bounded(value.value(), record_bytes)?;
                    if scoped {
                        if key.value().len() > record_bytes {
                            return Err(Status::resource_exhausted(
                                "actor key exceeds source_batch_bytes",
                            ));
                        }
                        actors::validate_key(key.value())?;
                    }
                    if (!scoped && (key.value().is_empty() || key.value().len() > 1024))
                        || operation.request_sha256.len() != 32
                    {
                        return Err(damaged("invalid operation ID or request digest"));
                    }
                    let receipt = operation
                        .receipt
                        .ok_or_else(|| damaged("operation receipt is missing"))?;
                    let legacy = receipt.history_id.is_empty()
                        && receipt.accepted_sequence <= header.legacy_receipts_through_sequence;
                    if !receipt.accepted
                        || receipt.searchable
                        || !receipt.durable
                        || receipt.replayed
                        || receipt.accepted_sequence == 0
                        || receipt.accepted_sequence > header.accepted_sequence
                        || (!legacy && receipt.history_id != header.history_id)
                    {
                        return Err(damaged("invalid immutable acceptance receipt"));
                    }
                    if let Some(n) = &header.actor_namespace {
                        if receipt.accepted_sequence <= n.legacy_operations {
                            if scoped {
                                attributed = attributed
                                    .checked_add(1)
                                    .ok_or_else(|| damaged("attribution count overflow"))?;
                            }
                        } else if !scoped {
                            return Err(damaged(
                                "actorless operation exceeds legacy attribution watermark",
                            ));
                        }
                    }
                    let key = DocumentVersionKey {
                        document_key: receipt.document_key.clone(),
                        version: receipt.version,
                    }
                    .encode_to_vec();
                    let version = versions
                        .get(key.as_slice())
                        .map_err(storage)?
                        .ok_or_else(|| damaged("operation receipt has no accepted version"))?;
                    let version: DocumentVersion = bounded(version.value(), record_bytes)?;
                    if receipt.accepted_sequence != version.accepted_sequence {
                        return Err(damaged("operation receipt differs from accepted history"));
                    }
                    if seen
                        .insert(receipt.accepted_sequence, 1)
                        .map_err(storage)?
                        .is_some()
                    {
                        return Err(damaged("multiple operations claim one accepted sequence"));
                    }
                    count += 1;
                }
                drop(seen);
                if count == 0 {
                    break;
                }
                tx.commit().map_err(storage)?;
                within_budget()?;
            }
        }
        if header
            .actor_namespace
            .as_ref()
            .is_some_and(|n| n.assigned_operations != attributed)
        {
            return Err(damaged(
                "attributed receipt count differs from namespace watermark",
            ));
        }
        drop(proof);
        within_budget()?;
        drop(file);
        std::fs::remove_file(scratch).map_err(storage)?;
        owned.keep = true;
        Ok(())
    }
}

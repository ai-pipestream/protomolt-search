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

type Records = redb::ReadOnlyTable<&'static [u8], &'static [u8]>;

/// The pinned tables of one source-history audit.
struct History<'a> {
    header: &'a DocumentCatalogHeader,
    record_bytes: usize,
    versions: Records,
    changes: redb::ReadOnlyTable<u64, &'static [u8]>,
    heads: Records,
    sources: Records,
    descriptors: Records,
}

impl<'a> History<'a> {
    fn open(
        read: &ReadTransaction,
        header: &'a DocumentCatalogHeader,
        record_bytes: usize,
    ) -> Result<Self, Status> {
        let versions = read.open_table(VERSIONS).map_err(storage)?;
        let changes = read.open_table(CHANGES).map_err(storage)?;
        let heads = read.open_table(HEADS).map_err(storage)?;
        actors::validate_read_counts(read, header)?;
        let sources = read.open_table(SOURCES).map_err(storage)?;
        let descriptors = read.open_table(DESCRIPTORS).map_err(storage)?;
        if versions.len().map_err(storage)? != header.accepted_sequence
            || changes.len().map_err(storage)? != header.accepted_sequence
        {
            return Err(damaged(
                "accepted history table counts differ from the header",
            ));
        }
        Ok(Self {
            header,
            record_bytes,
            versions,
            changes,
            heads,
            sources,
            descriptors,
        })
    }

    /// Every accepted sequence names one version that names it back. With
    /// the reverse links and the exact counts this is the complete
    /// sequence/version bijection, including history older than every tip.
    fn sequences_name_versions(&self) -> Result<(), Status> {
        let mut expected = 0u64;
        for entry in self.changes.iter().map_err(storage)? {
            let (sequence, key) = entry.map_err(storage)?;
            expected = expected
                .checked_add(1)
                .ok_or_else(|| damaged("sequence overflow"))?;
            if sequence.value() != expected {
                return Err(damaged("accepted sequence is missing or out of order"));
            }
            let decoded: DocumentVersionKey = bounded(key.value(), self.record_bytes)?;
            let stored = self
                .versions
                .get(key.value())
                .map_err(storage)?
                .ok_or_else(|| damaged("accepted sequence has no version"))?;
            let document: DocumentVersion = bounded(stored.value(), self.record_bytes)?;
            version_valid(&document, self.header.accepted_sequence)?;
            if decoded.document_key != document.document_key
                || decoded.version != document.version
                || version_key(&document) != key.value()
                || document.accepted_sequence != expected
            {
                return Err(damaged("accepted sequence/version link differs"));
            }
        }
        Ok(())
    }

    /// Every version has its accepted sequence, a head at or past it, an
    /// earlier predecessor and, unless deleted, its source blob.
    fn versions_name_sequences(&self) -> Result<(), Status> {
        for entry in self.versions.iter().map_err(storage)? {
            let (key, value) = entry.map_err(storage)?;
            let document: DocumentVersion = bounded(value.value(), self.record_bytes)?;
            version_valid(&document, self.header.accepted_sequence)?;
            if version_key(&document) != key.value()
                || self
                    .changes
                    .get(document.accepted_sequence)
                    .map_err(storage)?
                    .is_none_or(|v| v.value() != key.value())
            {
                return Err(damaged("version has no matching accepted sequence"));
            }
            let head = self
                .heads
                .get(document.document_key.as_slice())
                .map_err(storage)?
                .ok_or_else(|| damaged("version has no document head"))?;
            let head: DocumentVersion = bounded(head.value(), self.record_bytes)?;
            if head.document_key != document.document_key || head.version < document.version {
                return Err(damaged("document head precedes accepted history"));
            }
            if document.version > 1 {
                let previous_key = DocumentVersionKey {
                    document_key: document.document_key.clone(),
                    version: document.version - 1,
                }
                .encode_to_vec();
                let previous = self
                    .versions
                    .get(previous_key.as_slice())
                    .map_err(storage)?
                    .ok_or_else(|| damaged("document version predecessor is missing"))?;
                let previous: DocumentVersion = bounded(previous.value(), self.record_bytes)?;
                if previous.accepted_sequence >= document.accepted_sequence {
                    return Err(damaged("document version order contradicts accepted order"));
                }
            }
            if !document.deleted
                && self
                    .sources
                    .get(document.source_sha256.as_slice())
                    .map_err(storage)?
                    .is_none()
            {
                return Err(damaged("accepted version source blob is missing"));
            }
        }
        Ok(())
    }

    fn heads_match_versions(&self) -> Result<(), Status> {
        for entry in self.heads.iter().map_err(storage)? {
            let (key, value) = entry.map_err(storage)?;
            let head: DocumentVersion = bounded(value.value(), self.record_bytes)?;
            version_valid(&head, self.header.accepted_sequence)?;
            if key.value() != head.document_key {
                return Err(damaged("document head key differs"));
            }
            let key = version_key(&head);
            let version = self
                .versions
                .get(key.as_slice())
                .map_err(storage)?
                .ok_or_else(|| damaged("document head version is missing"))?;
            if bounded::<DocumentVersion>(version.value(), self.record_bytes)? != head {
                return Err(damaged("document head differs from its version"));
            }
        }
        Ok(())
    }

    /// Descriptors and sources are content-addressed, and every source
    /// has its descriptor.
    fn blobs_are_content_addressed(&self) -> Result<(), Status> {
        for entry in self.descriptors.iter().map_err(storage)? {
            let (key, bytes) = entry.map_err(storage)?;
            if bytes.value().len() > self.record_bytes {
                return Err(Status::resource_exhausted(
                    "source descriptor exceeds source_batch_bytes",
                ));
            }
            if bytes.value().is_empty()
                || turbo_sha256::sha256(bytes.value()).0.as_slice() != key.value()
            {
                return Err(damaged("descriptor content-address mismatch"));
            }
        }
        for entry in self.sources.iter().map_err(storage)? {
            let (key, bytes) = entry.map_err(storage)?;
            let source: SourceRecord = bounded(bytes.value(), self.record_bytes)?;
            if turbo_sha256::sha256(bytes.value()).0.as_slice() != key.value()
                || source.message_type.is_empty()
                || source.descriptor_sha256.len() != 32
            {
                return Err(damaged("source content-address or envelope mismatch"));
            }
            if self
                .descriptors
                .get(source.descriptor_sha256.as_slice())
                .map_err(storage)?
                .is_none()
            {
                return Err(damaged("source descriptor is missing"));
            }
        }
        Ok(())
    }

    /// Check one operation record and return the accepted sequence its
    /// receipt claims, counting it in `attributed` when it is a scoped
    /// operation from before the legacy watermark.
    fn receipt_sequence(
        &self,
        scoped: bool,
        key: &[u8],
        value: &[u8],
        attributed: &mut u64,
    ) -> Result<u64, Status> {
        let operation: DocumentOperation = bounded(value, self.record_bytes)?;
        if scoped {
            if key.len() > self.record_bytes {
                return Err(Status::resource_exhausted(
                    "actor key exceeds source_batch_bytes",
                ));
            }
            actors::validate_key(key)?;
        }
        if (!scoped && (key.is_empty() || key.len() > 1024)) || operation.request_sha256.len() != 32
        {
            return Err(damaged("invalid operation ID or request digest"));
        }
        let receipt = operation
            .receipt
            .ok_or_else(|| damaged("operation receipt is missing"))?;
        let legacy = receipt.history_id.is_empty()
            && receipt.accepted_sequence <= self.header.legacy_receipts_through_sequence;
        if !receipt.accepted
            || receipt.searchable
            || !receipt.durable
            || receipt.replayed
            || receipt.accepted_sequence == 0
            || receipt.accepted_sequence > self.header.accepted_sequence
            || (!legacy && receipt.history_id != self.header.history_id)
        {
            return Err(damaged("invalid immutable acceptance receipt"));
        }
        if let Some(n) = &self.header.actor_namespace {
            if receipt.accepted_sequence <= n.legacy_operations {
                if scoped {
                    *attributed = attributed
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
        let version = self
            .versions
            .get(key.as_slice())
            .map_err(storage)?
            .ok_or_else(|| damaged("operation receipt has no accepted version"))?;
        let version: DocumentVersion = bounded(version.value(), self.record_bytes)?;
        if receipt.accepted_sequence != version.accepted_sequence {
            return Err(damaged("operation receipt differs from accepted history"));
        }
        Ok(receipt.accepted_sequence)
    }

    /// Record every receipt of one operation table in the proof, in
    /// bounded transactions; a sequence claimed twice is damage.
    fn claim_receipts(
        &self,
        read: &ReadTransaction,
        proof: &ReceiptProof<'_>,
        definition: BinaryTable,
        attributed: &mut u64,
    ) -> Result<(), Status> {
        let scoped = definition.name() == actors::OPERATIONS.name();
        if scoped && self.header.actor_namespace.is_none() {
            return Ok(());
        }
        let operations = read.open_table(definition).map_err(storage)?;
        let mut entries = operations.iter().map_err(storage)?;
        loop {
            let mut tx = proof.database.begin_write().map_err(storage)?;
            tx.set_durability(Durability::None).map_err(storage)?;
            let mut seen = tx.open_table(SEEN).map_err(storage)?;
            let mut count = 0;
            for _ in 0..BATCH {
                let Some(entry) = entries.next() else { break };
                let (key, value) = entry.map_err(storage)?;
                let sequence =
                    self.receipt_sequence(scoped, key.value(), value.value(), attributed)?;
                if seen.insert(sequence, 1).map_err(storage)?.is_some() {
                    return Err(damaged("multiple operations claim one accepted sequence"));
                }
                count += 1;
            }
            drop(seen);
            if count == 0 {
                break;
            }
            tx.commit().map_err(storage)?;
            proof.within_budget()?;
        }
        Ok(())
    }
}

/// The scratch database that indexes receipt sequences. The fields drop
/// in order: the database and the file close before the guard removes an
/// unfinished file.
struct ReceiptProof<'a> {
    database: Database,
    file: File,
    owned: OwnedFile<'a>,
    scratch_bytes: u64,
}

impl<'a> ReceiptProof<'a> {
    fn create(scratch: &'a Path, scratch_bytes: u64) -> Result<Self, Status> {
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
        let backend = redb::backends::FileBackend::new(file.try_clone().map_err(storage)?)
            .map_err(storage)?;
        let database = builder.create_with_backend(backend).map_err(storage)?;
        Ok(Self {
            database,
            file,
            owned,
            scratch_bytes,
        })
    }

    fn within_budget(&self) -> Result<(), Status> {
        if self.file.metadata().map_err(storage)?.len() > self.scratch_bytes {
            Err(Status::resource_exhausted(
                "source audit scratch exceeds max_bytes",
            ))
        } else {
            Ok(())
        }
    }

    /// Close the proof within its budget and remove the scratch file.
    fn finish(self) -> Result<(), Status> {
        let Self {
            database,
            file,
            mut owned,
            scratch_bytes,
        } = self;
        drop(database);
        let len = file.metadata().map_err(storage).map(|meta| meta.len());
        drop(file);
        if len? > scratch_bytes {
            return Err(Status::resource_exhausted(
                "source audit scratch exceeds max_bytes",
            ));
        }
        std::fs::remove_file(owned.path).map_err(storage)?;
        owned.keep = true;
        Ok(())
    }
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
        let history = History::open(&self.read, header, record_bytes)?;
        history.sequences_name_versions()?;
        history.versions_name_sequences()?;
        history.heads_match_versions()?;
        history.blobs_are_content_addressed()?;

        // Counts alone would allow two operation IDs to refer to the same
        // receipt while another accepted write lost its retry decision.
        let proof = ReceiptProof::create(scratch, scratch_bytes)?;
        proof.within_budget()?;
        let mut attributed = 0u64;
        for definition in [OPERATIONS, actors::OPERATIONS] {
            history.claim_receipts(&self.read, &proof, definition, &mut attributed)?;
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
        proof.finish()
    }
}

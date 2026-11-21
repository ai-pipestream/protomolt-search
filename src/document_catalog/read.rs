//! Trusted local reads: one version with its source, and ordered pages
//! of accepted history.
use super::*;

/// Check a history cursor against this catalog and return its fence:
/// the last sequence the read may return.
fn accepted_fence(
    request: &ReadAcceptedDocumentsRequest,
    header: &DocumentCatalogHeader,
) -> Result<u64, Status> {
    if request.history_id.is_empty() {
        if request.after_sequence != 0 || request.through_sequence.is_some_and(|fence| fence != 0) {
            return Err(Status::invalid_argument(
                "resuming accepted history requires history_id",
            ));
        }
    } else if !valid_history_id(&request.history_id) {
        return Err(Status::invalid_argument(
            "history_id must be a nonzero 16-byte value",
        ));
    } else if request.history_id != header.history_id {
        return Err(Status::failed_precondition(
            "accepted cursor belongs to another catalog history",
        ));
    }
    let fence = request.through_sequence.unwrap_or(header.accepted_sequence);
    if fence > header.accepted_sequence || request.after_sequence > fence {
        return Err(Status::invalid_argument(
            "accepted history cursor exceeds its fence or committed history",
        ));
    }
    Ok(fence)
}

impl DocumentCatalog {
    /// Trusted local source lookup. No network or search-result disclosure API.
    pub fn get(
        &self,
        key: &[u8],
        version: Option<u64>,
    ) -> Result<Option<(DocumentVersion, Option<ProtobufSource>)>, Status> {
        let transaction = self.database.begin_read().map_err(storage)?;
        Self::get_from(&transaction, key, version)
    }

    pub(super) fn get_from(
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
        if turbo_sha256::sha256(bytes.value()).0.as_slice() != document.source_sha256 {
            return Err(Status::data_loss("document source checksum mismatch"));
        }
        let source: SourceRecord = decode(bytes.value())?;
        let descriptors = transaction.open_table(DESCRIPTORS).map_err(storage)?;
        let bytes = descriptors
            .get(source.descriptor_sha256.as_slice())
            .map_err(storage)?
            .ok_or_else(|| Status::data_loss("document descriptor missing"))?;
        if turbo_sha256::sha256(bytes.value()).0.as_slice() != source.descriptor_sha256 {
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
        let header: DocumentCatalogHeader = decode_header(
            meta.get("header")
                .map_err(storage)?
                .ok_or_else(|| Status::data_loss("catalog header missing"))?
                .value(),
        )?;
        validate_current_header(&header)?;
        let fence = accepted_fence(request, &header)?;
        let mut response = ReadAcceptedDocumentsResponse {
            through_sequence: fence,
            next_sequence: request.after_sequence,
            complete: request.after_sequence == fence,
            documents: Vec::new(),
            history_id: header.history_id.clone(),
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
            let Some((document, size)) =
                Self::accepted_document(&transaction, &changes, &versions, sequence, remaining)?
            else {
                if response.documents.is_empty() {
                    return Err(Status::resource_exhausted(
                        "accepted version exceeds max_bytes",
                    ));
                }
                break;
            };
            remaining -= size;
            response.documents.push(document);
            response.next_sequence = sequence;
        }
        response.complete = response.next_sequence == fence;
        Ok(response)
    }

    /// The accepted version at `sequence` with its encoded size, or `None`
    /// when it does not fit in `budget` bytes.
    fn accepted_document(
        transaction: &redb::ReadTransaction,
        changes: &impl ReadableTable<u64, &'static [u8]>,
        versions: &impl ReadableTable<&'static [u8], &'static [u8]>,
        sequence: u64,
        budget: u64,
    ) -> Result<Option<(AcceptedDocumentVersion, u64)>, Status> {
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
        if !metadata.deleted && !Self::source_fits(transaction, &metadata, budget)? {
            return Ok(None);
        }
        let (version, source) = Self::get_from(transaction, &key.document_key, Some(key.version))?
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
            write_epoch: version.write_epoch,
            fenced: version.outcome == WriteOutcome::Fenced as i32,
            fenced_at_revision: version.fenced_at_revision,
        };
        let size = document.encoded_len() as u64;
        Ok((size <= budget).then_some((document, size)))
    }

    // Inspect borrowed blob sizes before copying source/descriptor payloads.
    // SourceRecord's 32-byte descriptor hash contributes 34 encoded bytes,
    // replaced by the descriptor itself in the public source envelope.
    pub(super) fn source_fits(
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

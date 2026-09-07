use super::*;
use crate::mapping::Extractor;
use crate::pb::{
    DocumentIdentity, DocumentProjectionRow, PrepareDocumentProjectionRequest,
    PreparedDocumentProjection,
};

impl DocumentCatalog {
    /// Extract one immutable accepted version into a complete private batch.
    /// Callers cannot supply source bytes, version metadata, or row identities.
    /// This read-only step performs no analysis, ingest, or publication.
    pub fn prepare_projection(
        &self,
        request: &PrepareDocumentProjectionRequest,
    ) -> Result<PreparedDocumentProjection, Status> {
        if !valid_history_id(&request.history_id)
            || request.document_key.is_empty()
            || request.document_key.len() > 16 * 1024
            || request.version == 0
        {
            return Err(Status::invalid_argument(
                "projection requires history_id, document_key and a positive exact version",
            ));
        }
        if !(1..=65536).contains(&request.max_rows)
            || !(1..=64 * 1024 * 1024).contains(&request.max_bytes)
        {
            return Err(Status::invalid_argument(
                "projection requires max_rows 1 to 65536 and max_bytes 1 to 64 MiB",
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
        validate_current_header(&header)?;
        if header.history_id != request.history_id {
            return Err(Status::failed_precondition(
                "projection belongs to another catalog history",
            ));
        }
        let key = DocumentVersionKey {
            document_key: request.document_key.clone(),
            version: request.version,
        }
        .encode_to_vec();
        let versions = transaction.open_table(VERSIONS).map_err(storage)?;
        let stored = versions
            .get(key.as_slice())
            .map_err(storage)?
            .ok_or_else(|| Status::not_found("accepted document version is missing"))?;
        let metadata: DocumentVersion = decode(stored.value())?;
        if !metadata.deleted && !Self::source_fits(&transaction, &metadata, request.max_bytes)? {
            return Err(Status::resource_exhausted(
                "projection source exceeds max_bytes",
            ));
        }
        let (version, source) =
            Self::get_from(&transaction, &request.document_key, Some(request.version))?
                .ok_or_else(|| Status::data_loss("accepted document version disappeared"))?;
        if version.accepted_sequence == 0
            || version.accepted_sequence > header.accepted_sequence
            || (version.deleted && !version.source_sha256.is_empty())
        {
            return Err(Status::data_loss(
                "invalid accepted projection version metadata",
            ));
        }
        let changes = transaction.open_table(CHANGES).map_err(storage)?;
        if changes
            .get(version.accepted_sequence)
            .map_err(storage)?
            .is_none_or(|entry| entry.value() != key.as_slice())
        {
            return Err(Status::data_loss(
                "projection version does not match accepted history",
            ));
        }
        let mut result = PreparedDocumentProjection {
            history_id: header.history_id,
            document_key: version.document_key,
            version: version.version,
            accepted_sequence: version.accepted_sequence,
            source,
            deleted: version.deleted,
            ..Default::default()
        };
        if result.deleted {
            if !request.expected_plan_fingerprint.is_empty()
                || !request.body_path.is_empty()
                || request.index_definition.is_some()
            {
                return Err(Status::invalid_argument(
                    "a deletion projection cannot declare a mapping",
                ));
            }
        } else {
            if request.expected_plan_fingerprint.is_empty() {
                return Err(Status::invalid_argument(
                    "projection requires the reviewed plan fingerprint",
                ));
            }
            let source = result.source.as_ref().expect("verified source version");
            let extractor = Extractor::with_definition(
                &source.descriptor_set,
                &source.message_type,
                &request.body_path,
                request.index_definition.as_ref(),
            )?;
            if extractor.plan().fingerprint != request.expected_plan_fingerprint {
                return Err(Status::failed_precondition(
                    "projection plan fingerprint differs from the reviewed plan",
                ));
            }
            result.plan_fingerprint = extractor.plan().fingerprint.clone();
            result.body_path = extractor.body_path().to_string();
            let mut used = result.encoded_len() as u64;
            if used > request.max_bytes {
                return Err(Status::resource_exhausted(
                    "projection metadata exceeds max_bytes",
                ));
            }
            extractor.visit_rows(
                &source.payload,
                request.max_rows as usize,
                |ordinal, mut row| {
                    row.request.identity = Some(DocumentIdentity {
                        document_key: result.document_key.clone(),
                        version: result.version,
                        chunk_ordinal: ordinal,
                    });
                    row.request.source_chunk_ordinal = ordinal;
                    let row = DocumentProjectionRow {
                        document: Some(row.request),
                        vector: row.vector,
                    };
                    let size = row.encoded_len() as u64;
                    let framed = 1 + prost::encoding::encoded_len_varint(size) as u64 + size;
                    if framed > request.max_bytes - used {
                        return Err(Status::resource_exhausted(
                            "document projection exceeds max_bytes",
                        ));
                    }
                    used += framed;
                    result.rows.push(row);
                    Ok(())
                },
            )?;
        }
        if result.encoded_len() as u64 > request.max_bytes {
            return Err(Status::resource_exhausted(
                "document projection exceeds max_bytes",
            ));
        }
        Ok(result)
    }
}

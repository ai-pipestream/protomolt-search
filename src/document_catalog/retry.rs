//! Read committed acceptance decisions independently of new-write admission.
use super::*;

pub(super) fn check_history(
    header: &DocumentCatalogHeader,
    request: &AcceptDocumentRequest,
) -> Result<(), Status> {
    if request.contract_version == 2 && request.history_id != header.history_id {
        return Err(Status::failed_precondition(
            "document write belongs to another catalog history",
        ));
    }
    Ok(())
}

pub(super) fn receipt(
    header: &DocumentCatalogHeader,
    bytes: &[u8],
    request_sha: &[u8; 32],
) -> Result<DocumentWriteReceipt, Status> {
    let previous: DocumentOperation = decode(bytes)?;
    if previous.request_sha256.as_slice() != request_sha {
        return Err(Status::already_exists(
            "operation_id was used for a different document write",
        ));
    }
    let mut receipt = previous
        .receipt
        .ok_or_else(|| Status::data_loss("operation receipt missing"))?;
    if receipt.history_id.is_empty()
        && receipt.accepted_sequence > 0
        && receipt.accepted_sequence <= header.legacy_receipts_through_sequence
    {
        // Enrich only pre-migration receipts; never rewrite the retry decision.
        receipt.history_id = header.history_id.clone();
    } else if receipt.history_id != header.history_id {
        return Err(Status::data_loss(
            "operation receipt belongs to another catalog history",
        ));
    }
    receipt.replayed = true;
    Ok(receipt)
}

impl DocumentCatalog {
    pub(super) fn accepted_retry(
        &self,
        request: &AcceptDocumentRequest,
        request_sha: &[u8; 32],
    ) -> Result<Option<DocumentWriteReceipt>, Status> {
        let read = self.database.begin_read().map_err(storage)?;
        let meta = read.open_table(META).map_err(storage)?;
        let header: DocumentCatalogHeader = decode(
            meta.get("header")
                .map_err(storage)?
                .ok_or_else(|| Status::data_loss("catalog header missing"))?
                .value(),
        )?;
        validate_current_header(&header)?;
        self.validate_resource_binding(&header)?;
        check_history(&header, request)?;
        let operations = read.open_table(OPERATIONS).map_err(storage)?;
        let result = operations
            .get(request.operation_id.as_slice())
            .map_err(storage)?
            .map(|previous| receipt(&header, previous.value(), request_sha))
            .transpose();
        result
    }
}

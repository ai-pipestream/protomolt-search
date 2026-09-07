//! Terminal local write retirement, serialized with every source transaction.
use super::*;
use crate::pb::storage::{SourceHistorySeal, SourceSealRequest};

pub(super) fn validate_header_seal(header: &DocumentCatalogHeader) -> Result<(), Status> {
    match (header.format_version, &header.history_seal) {
        (FORMAT_VERSION, None) => Ok(()),
        (SEALED_FORMAT_VERSION, Some(seal))
            if seal.format_version == 1
                && seal.history_id == header.history_id
                && seal.accepted_sequence == header.accepted_sequence
                && !seal.operation_id.is_empty()
                && seal.operation_id.len() <= 1024 =>
        {
            Ok(())
        }
        _ => Err(Status::data_loss(
            "document catalog format and terminal history seal disagree",
        )),
    }
}

fn header_from(tx: &redb::WriteTransaction) -> Result<DocumentCatalogHeader, Status> {
    let meta = tx.open_table(META).map_err(storage)?;
    let header = decode(
        meta.get("header")
            .map_err(storage)?
            .ok_or_else(|| Status::data_loss("catalog header missing"))?
            .value(),
    )?;
    validate_current_header(&header)?;
    Ok(header)
}

impl DocumentCatalog {
    /// Acquire the DB writer before checking the persistent fence. Checking in
    /// an earlier read would let a queued write cross a successful retirement.
    pub(super) fn writable_transaction(&self) -> Result<redb::WriteTransaction, Status> {
        let mut tx = self.database.begin_write().map_err(storage)?;
        tx.set_durability(Durability::Immediate).map_err(storage)?;
        if header_from(&tx)?.history_seal.is_some() {
            return Err(Status::failed_precondition(
                "source history is sealed; acceptance and index mutations are retired",
            ));
        }
        Ok(tx)
    }

    /// Read retirement evidence without granting replacement ownership. The
    /// source remains readable for backup and history inspection after sealing.
    pub fn history_seal(&self) -> Result<Option<SourceHistorySeal>, Status> {
        let read = self.database.begin_read().map_err(storage)?;
        let meta = read.open_table(META).map_err(storage)?;
        let header: DocumentCatalogHeader = decode(
            meta.get("header")
                .map_err(storage)?
                .ok_or_else(|| Status::data_loss("catalog header missing"))?
                .value(),
        )?;
        validate_current_header(&header)?;
        Ok(header.history_seal)
    }

    /// Durably retire this source store at an exact accepted watermark. Pending
    /// source/maintenance decisions must be resolved first. There is no unseal:
    /// a verified backup and this fence do not authorize a replacement writer.
    /// The trusted local collection owner performs authorization before calling.
    pub fn seal_history(&self, request: &SourceSealRequest) -> Result<SourceHistorySeal, Status> {
        if !self.durable {
            return Err(Status::failed_precondition(
                "source history sealing requires a durable catalog",
            ));
        }
        if !valid_history_id(&request.history_id)
            || request.operation_id.is_empty()
            || request.operation_id.len() > 1024
        {
            return Err(Status::invalid_argument(
                "source seal needs a nonzero 16-byte history_id and operation_id of 1..1024 bytes",
            ));
        }
        let mut tx = self.database.begin_write().map_err(storage)?;
        tx.set_durability(Durability::Immediate).map_err(storage)?;
        let mut header = header_from(&tx)?;
        if header.history_id != request.history_id
            || header.accepted_sequence != request.expected_accepted_sequence
        {
            return Err(Status::failed_precondition(
                "source seal history or expected accepted sequence differs",
            ));
        }
        if let Some(previous) = header.history_seal {
            return if previous.operation_id == request.operation_id {
                Ok(previous)
            } else {
                Err(Status::already_exists(
                    "source history was sealed by another operation",
                ))
            };
        }

        // No mutations have been made in tx. Holding its writer excludes new
        // accepts, preparations and recovery commits while this read validates
        // the last committed view. A prepared operation leaves a pending intent
        // until its durable artifact decision is resolved; it therefore blocks
        // sealing even while its filesystem work is outside a DB transaction.
        // Do not acquire node/segment locks here: publishers take them before
        // the DB writer. The checkpoint check bounds journal metadata at 64 MiB.
        let checkpoint = self.capture_checkpoint(64 << 20)?;
        if checkpoint.metadata().header.as_ref() != Some(&header) {
            return Err(Status::data_loss(
                "source seal checkpoint differs from the held writer",
            ));
        }
        drop(checkpoint);

        let seal = SourceHistorySeal {
            format_version: 1,
            history_id: header.history_id.clone(),
            accepted_sequence: header.accepted_sequence,
            operation_id: request.operation_id.clone(),
        };
        header.format_version = SEALED_FORMAT_VERSION;
        header.history_seal = Some(seal.clone());
        {
            let mut meta = tx.open_table(META).map_err(storage)?;
            meta.insert("header", header.encode_to_vec().as_slice())
                .map_err(storage)?;
        }
        tx.commit().map_err(storage)?;
        Ok(seal)
    }
}

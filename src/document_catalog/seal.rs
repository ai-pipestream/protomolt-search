//! Terminal local write retirement, serialized with every source transaction.
use super::*;
use crate::pb::storage::{
    SourceHistorySeal, SourceRetirementIntent, SourceRetirementRequest, SourceSealRequest,
};

pub(super) fn validate_header_seal(header: &DocumentCatalogHeader) -> Result<(), Status> {
    let seal_valid = header.history_seal.as_ref().is_some_and(|seal| {
        seal.format_version == 1
            && seal.history_id == header.history_id
            && seal.accepted_sequence == header.accepted_sequence
            && !seal.operation_id.is_empty()
            && seal.operation_id.len() <= 1024
    });
    let retirement_valid = header.retirement_intent.as_ref().is_some_and(|intent| {
        intent.format_version == 1
            && intent.history_id == header.history_id
            && intent.accepted_sequence == header.accepted_sequence
            && !intent.operation_id.is_empty()
            && intent.operation_id.len() <= 1024
    });
    let lifecycle_format = if matches!(
        header.format_version,
        LEGACY_ACCESS_CONTROLLED_FORMAT | ACCESS_CONTROLLED_FORMAT | MANAGED_FORMAT
    ) {
        let binding = header.resource_binding.as_ref().ok_or_else(|| {
            Status::data_loss("access-controlled catalog resource binding missing")
        })?;
        access::validate_binding(binding).map_err(|error| Status::data_loss(error.message()))?;
        if binding.collection != header.collection {
            return Err(Status::data_loss(
                "source resource binding and catalog collection disagree",
            ));
        }
        match (
            header.retirement_intent.is_some(),
            header.history_seal.is_some(),
        ) {
            (false, false) => FORMAT_VERSION,
            (false, true) => SEALED_FORMAT_VERSION,
            (true, false) => RETIRING_FORMAT_VERSION,
            (true, true) => RETIRED_FORMAT_VERSION,
        }
    } else {
        if header.resource_binding.is_some() {
            return Err(Status::data_loss(
                "source resource binding requires catalog format 7, 8 or 9",
            ));
        }
        header.format_version
    };
    let valid = match lifecycle_format {
        FORMAT_VERSION => header.history_seal.is_none() && header.retirement_intent.is_none(),
        SEALED_FORMAT_VERSION => seal_valid && header.retirement_intent.is_none(),
        RETIRING_FORMAT_VERSION => retirement_valid && header.history_seal.is_none(),
        RETIRED_FORMAT_VERSION => {
            seal_valid
                && retirement_valid
                && header.history_seal.as_ref().map(|seal| &seal.operation_id)
                    == header
                        .retirement_intent
                        .as_ref()
                        .map(|intent| &intent.operation_id)
        }
        _ => false,
    };
    if !valid {
        return Err(Status::data_loss(
            "document catalog format, retirement intent and terminal history seal disagree",
        ));
    }
    Ok(())
}

pub(super) fn header_from(
    catalog: &DocumentCatalog,
    tx: &redb::WriteTransaction,
) -> Result<DocumentCatalogHeader, Status> {
    let meta = tx.open_table(META).map_err(storage)?;
    let header = decode_header(
        meta.get("header")
            .map_err(storage)?
            .ok_or_else(|| Status::data_loss("catalog header missing"))?
            .value(),
    )?;
    validate_current_header(&header)?;
    catalog.validate_resource_binding(&header)?;
    if header.managed_binding.is_some() {
        return Err(Status::failed_precondition(
            "managed source admission is closed; preparation and persisted binding do not authorize mutations",
        ));
    }
    Ok(header)
}

/// Call only after validating the header while holding the database writer.
/// Acceptance resolves existing retries first; this gate controls new work.
pub(super) fn require_admission(
    header: &DocumentCatalogHeader,
    recovery: bool,
) -> Result<(), Status> {
    if header.history_seal.is_some() {
        return Err(Status::failed_precondition(
            "source history is sealed; acceptance and index mutations are retired",
        ));
    }
    if header.retirement_intent.is_some() && !recovery {
        return Err(Status::failed_precondition(
            "source history is retiring; new acceptance and index preparations are closed",
        ));
    }
    Ok(())
}

impl DocumentCatalog {
    /// Acquire the DB writer before checking the persistent fence. Checking in
    /// an earlier read would let a queued write cross a successful retirement.
    pub(super) fn writable_transaction(&self) -> Result<redb::WriteTransaction, Status> {
        self.source_transaction(false)
    }

    /// Resolve only already-prepared decisions during retirement. This must not
    /// be used by acceptance, journal enablement, or a new preparation.
    pub(super) fn recovery_transaction(&self) -> Result<redb::WriteTransaction, Status> {
        self.source_transaction(true)
    }

    fn source_transaction(&self, recovery: bool) -> Result<redb::WriteTransaction, Status> {
        let mut tx = self.database.begin_write().map_err(storage)?;
        tx.set_durability(Durability::Immediate).map_err(storage)?;
        let header = header_from(self, &tx)?;
        require_admission(&header, recovery)?;
        Ok(tx)
    }

    /// Read the durable admission decision, including after the final seal.
    pub fn retirement_intent(&self) -> Result<Option<SourceRetirementIntent>, Status> {
        let read = self.database.begin_read().map_err(storage)?;
        let meta = read.open_table(META).map_err(storage)?;
        let header: DocumentCatalogHeader = decode_header(
            meta.get("header")
                .map_err(storage)?
                .ok_or_else(|| Status::data_loss("catalog header missing"))?
                .value(),
        )?;
        validate_current_header(&header)?;
        self.validate_resource_binding(&header)?;
        Ok(header.retirement_intent)
    }

    /// Close new admission durably before draining existing index intents.
    /// The writer atomically captures every previously accepted write. There is
    /// no cancellation: reopen preserves closure, recovery may resolve pending
    /// work, and the same operation must finish through `seal_history`.
    /// This trusted local operation grants no replacement ownership.
    pub fn begin_retirement(
        &self,
        request: &SourceRetirementRequest,
    ) -> Result<SourceRetirementIntent, Status> {
        if !self.durable {
            return Err(Status::failed_precondition(
                "source retirement requires a durable catalog",
            ));
        }
        if !valid_history_id(&request.history_id)
            || request.operation_id.is_empty()
            || request.operation_id.len() > 1024
        {
            return Err(Status::invalid_argument(
                "source retirement needs a nonzero 16-byte history_id and operation_id of 1..1024 bytes",
            ));
        }
        let mut tx = self.database.begin_write().map_err(storage)?;
        tx.set_durability(Durability::Immediate).map_err(storage)?;
        let mut header = header_from(self, &tx)?;
        if header.history_id != request.history_id {
            return Err(Status::failed_precondition(
                "source retirement belongs to another catalog history",
            ));
        }
        if let Some(previous) = header.retirement_intent {
            return if previous.operation_id == request.operation_id {
                Ok(previous)
            } else {
                Err(Status::already_exists(
                    "source retirement was begun by another operation",
                ))
            };
        }
        if header.history_seal.is_some() {
            return Err(Status::failed_precondition(
                "source history is already sealed without a retirement intent",
            ));
        }
        let intent = SourceRetirementIntent {
            format_version: 1,
            history_id: header.history_id.clone(),
            accepted_sequence: header.accepted_sequence,
            operation_id: request.operation_id.clone(),
        };
        header.format_version = if self.resource_binding.is_some() {
            header.format_version
        } else {
            RETIRING_FORMAT_VERSION
        };
        header.retirement_intent = Some(intent.clone());
        {
            let mut meta = tx.open_table(META).map_err(storage)?;
            meta.insert("header", header.encode_to_vec().as_slice())
                .map_err(storage)?;
        }
        tx.commit().map_err(storage)?;
        Ok(intent)
    }

    /// Read retirement evidence without granting replacement ownership. The
    /// source remains readable for backup and history inspection after sealing.
    pub fn history_seal(&self) -> Result<Option<SourceHistorySeal>, Status> {
        let read = self.database.begin_read().map_err(storage)?;
        let meta = read.open_table(META).map_err(storage)?;
        let header: DocumentCatalogHeader = decode_header(
            meta.get("header")
                .map_err(storage)?
                .ok_or_else(|| Status::data_loss("catalog header missing"))?
                .value(),
        )?;
        validate_current_header(&header)?;
        self.validate_resource_binding(&header)?;
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
        let mut header = header_from(self, &tx)?;
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
        if header
            .retirement_intent
            .as_ref()
            .is_some_and(|intent| intent.operation_id != request.operation_id)
        {
            return Err(Status::already_exists(
                "source retirement was begun by another operation",
            ));
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
        header.format_version = if self.resource_binding.is_some() {
            header.format_version
        } else if header.retirement_intent.is_some() {
            RETIRED_FORMAT_VERSION
        } else {
            SEALED_FORMAT_VERSION
        };
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

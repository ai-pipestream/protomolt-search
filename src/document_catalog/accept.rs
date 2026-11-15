//! Source acceptance: one transaction that records a version, its
//! change-order entry and the operation's retry decision.
use super::*;

/// The facts of one new write, fixed before its records are written.
struct NewWrite<'a> {
    request: &'a AcceptDocumentRequest,
    operation_key: &'a actors::OperationKey,
    request_sha: &'a [u8; 32],
    version: u64,
    source_sha256: Vec<u8>,
    write_epoch: u64,
}

/// The shape a write must have, checked ahead of any storage read.
fn validate_write_request(request: &AcceptDocumentRequest) -> Result<(), Status> {
    match request.contract_version {
        1 if request.history_id.is_empty() => {}
        2 if valid_history_id(&request.history_id) => {}
        _ => return Err(Status::invalid_argument(
            "document write requires version 1 with no history_id or version 2 with a nonzero 16-byte history_id",
        )),
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
    Ok(())
}

/// The version this write creates: one past the document's current head,
/// which must be the request's expected version when it names one.
fn next_version(
    transaction: &redb::WriteTransaction,
    request: &AcceptDocumentRequest,
) -> Result<u64, Status> {
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
        return Err(crate::raft::reasons::reason(
            Status::aborted("document version precondition failed"),
            crate::raft::reasons::OUTCOME_VERSION_MISMATCH,
        ));
    }
    current_version
        .checked_add(1)
        .ok_or_else(|| Status::out_of_range("document version exhausted"))
}

/// Store a source write's descriptor and record under their content
/// addresses and return the record's digest. A delete stores nothing.
fn store_source(
    transaction: &redb::WriteTransaction,
    request: &AcceptDocumentRequest,
) -> Result<Vec<u8>, Status> {
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
    Ok(source_sha256)
}

impl DocumentCatalog {
    /// Atomically accept a version and its retry decision. A retry is resolved
    /// before the version precondition or retirement fence, including after
    /// later writes, deletion or sealing. A replay does not mutate the catalog.
    pub fn accept(&self, request: &AcceptDocumentRequest) -> Result<DocumentWriteReceipt, Status> {
        self.accept_as(request, None, None, 0)
            .map(|written| written.receipt)
    }

    /// Pause the next write inside its transaction, after the writer lock
    /// and every table write, just before its final admission check.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn arm_precommit_pause(&self, pause: std::time::Duration) {
        *self.precommit_pause.lock().unwrap() = Some(pause);
    }

    /// Pause the next write after its commit returned durable and before
    /// its outcome is judged against the lease: the window in which a
    /// write becomes durable after its admission lapsed.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn arm_postcommit_pause(&self, pause: std::time::Duration) {
        *self.postcommit_pause.lock().unwrap() = Some(pause);
    }

    /// `fence` is the write's final admission check, run inside the
    /// transaction after every wait and write and immediately before the
    /// commit: the boundary at which the write becomes authorized. A fence that
    /// refuses leaves no durable change and no retry record. `write_epoch`
    /// is the owner's epoch the write commits under, recorded on the
    /// version and the operation; zero when the catalog is not an
    /// activated managed source. The returned outcome is the recorded one:
    /// ACCEPTED for new work, and for a replay whatever the record settled
    /// to (a FENCED record replays as a rejection, in `retry`).
    pub(crate) fn accept_as(
        &self,
        request: &AcceptDocumentRequest,
        principal: Option<&str>,
        fence: Option<&dyn Fn() -> Result<(), Status>>,
        write_epoch: u64,
    ) -> Result<Written, Status> {
        let operation_key = actors::OperationKey::new(principal, &request.operation_id)?;
        validate_write_request(request)?;
        let request_sha = sha256::digest(&request.encode_to_vec());
        if let Some(receipt) = self.accepted_retry(request, &request_sha, &operation_key)? {
            return Ok(receipt);
        }
        // Another acceptance or retirement may commit after the read snapshot.
        // Recheck the retry under the writer before fencing genuinely new work.
        let mut transaction = self.database.begin_write().map_err(storage)?;
        transaction
            .set_durability(Durability::Immediate)
            .map_err(storage)?;
        let mut header = seal::header_from(self, &transaction)?;
        retry::check_history(&header, request)?;
        operation_key.check_header(&header)?;
        actors::validate_write_counts(&transaction, &header)?;
        {
            let operations = transaction
                .open_table(operation_key.table())
                .map_err(storage)?;
            if let Some(previous) = operations
                .get(operation_key.bytes.as_slice())
                .map_err(storage)?
            {
                return retry::receipt(&header, previous.value(), &request_sha);
            };
        }
        seal::require_admission(&header, false)?;
        let version = next_version(&transaction, request)?;
        header.accepted_sequence = header
            .accepted_sequence
            .checked_add(1)
            .ok_or_else(|| Status::out_of_range("catalog sequence exhausted"))?;
        let write = NewWrite {
            request,
            operation_key: &operation_key,
            request_sha: &request_sha,
            version,
            source_sha256: store_source(&transaction, request)?,
            write_epoch,
        };
        let receipt = self.record_write(&transaction, &header, write)?;
        #[cfg(any(test, feature = "fault-injection"))]
        if let Some(pause) = self.precommit_pause.lock().unwrap().take() {
            std::thread::sleep(pause);
        }
        if let Some(fence) = fence {
            fence()?;
        }
        transaction.commit().map_err(storage)?;
        #[cfg(any(test, feature = "fault-injection"))]
        if let Some(pause) = self.postcommit_pause.lock().unwrap().take() {
            std::thread::sleep(pause);
        }
        Ok(Written {
            receipt,
            outcome: WriteOutcome::Accepted,
        })
    }

    /// Write the new version under its head, version and change-order keys,
    /// then the operation's retry record and the advanced header. Returns
    /// the receipt the retry record holds.
    fn record_write(
        &self,
        transaction: &redb::WriteTransaction,
        header: &DocumentCatalogHeader,
        write: NewWrite<'_>,
    ) -> Result<DocumentWriteReceipt, Status> {
        let NewWrite {
            request,
            operation_key,
            request_sha,
            version,
            source_sha256,
            write_epoch,
        } = write;
        let document = DocumentVersion {
            document_key: request.document_key.clone(),
            version,
            accepted_sequence: header.accepted_sequence,
            source_sha256,
            deleted: matches!(request.mutation, Some(Mutation::Delete(_))),
            write_epoch,
            outcome: WriteOutcome::Accepted as i32,
            fenced_at_revision: 0,
        };
        let receipt = DocumentWriteReceipt {
            document_key: request.document_key.clone(),
            version,
            accepted_sequence: header.accepted_sequence,
            accepted: true,
            searchable: false,
            durable: self.durable,
            replayed: false,
            history_id: header.history_id.clone(),
            write_epoch,
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
            write_epoch,
            outcome: WriteOutcome::Accepted as i32,
            fenced_at_revision: 0,
        }
        .encode_to_vec();
        transaction
            .open_table(operation_key.table())
            .map_err(storage)?
            .insert(operation_key.bytes.as_slice(), operation.as_slice())
            .map_err(storage)?;
        transaction
            .open_table(META)
            .map_err(storage)?
            .insert("header", header.encode_to_vec().as_slice())
            .map_err(storage)?;
        Ok(receipt)
    }
}

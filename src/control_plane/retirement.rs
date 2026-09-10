//! Durable fence before importing a legacy authority into another store.
use super::*;
use crate::pb::storage as wire;
use prost::Message;
use std::fs::File;

const MAX_RECORD_BYTES: usize = 17 * 1024 * 1024;
const PREFIX_BYTES: usize = 40;

/// Privileged retirement evidence. Holding it retains exclusive ownership of
/// the retired file. It conveys neither activation nor source-write authority.
/// Intentionally neither Debug nor Clone: the checkpoint contains lease secrets.
pub struct RetiredLegacyControl {
    record: Arc<wire::LegacyControlRetirement>,
    _ownership_lock: Arc<File>,
}

impl RetiredLegacyControl {
    pub fn record(&self) -> &wire::LegacyControlRetirement {
        &self.record
    }

    pub fn checkpoint_bytes(&self) -> &[u8] {
        &self.record.checkpoint
    }
}

pub(crate) fn validate_request(
    request: &wire::LegacyControlRetirementRequest,
) -> Result<(), Status> {
    if request.format_version != 1 {
        return Err(Status::invalid_argument(
            "legacy retirement requires format 1",
        ));
    }
    let key = request
        .key
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("legacy retirement resource key is required"))?;
    crate::source_owner::key(key, false)?;
    if !key.owner_id.is_empty() {
        return Err(Status::invalid_argument(
            "legacy retirement requires an empty owner_id",
        ));
    }
    crate::source_owner::operation_id(&request.command_id)?;
    if request.expected_checkpoint_sha256.len() != 32 {
        return Err(Status::invalid_argument(
            "legacy retirement requires a 32-byte checkpoint SHA-256",
        ));
    }
    if request.expected_control_revision == 0 || request.expected_policy_revision == 0 {
        return Err(Status::invalid_argument(
            "legacy retirement requires nonzero control and policy revisions",
        ));
    }
    Ok(())
}

fn validate_record(record: &wire::LegacyControlRetirement) -> Result<(), Status> {
    if record.format_version != 1 {
        return Err(Status::invalid_argument(
            "legacy retirement record requires format 1",
        ));
    }
    let authority = record
        .authority
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("legacy retirement authority missing"))?;
    crate::source_owner::identity(authority)?;
    let request = record
        .request
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("legacy retirement request missing"))?;
    validate_request(request)?;
    let operation = record
        .operation
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("legacy retirement operation missing"))?;
    crate::source_owner::principal(&operation.principal)?;
    if operation.format_version != 1
        || operation.key != request.key
        || operation.command_id != request.command_id
    {
        return Err(Status::invalid_argument(
            "legacy retirement operation binding differs",
        ));
    }
    let checkpoint = LegacyControlCheckpoint::decode(&record.checkpoint)?;
    if checkpoint.sha256().as_slice() != request.expected_checkpoint_sha256
        || checkpoint.state().collection != request.key.as_ref().expect("validated key").collection
    {
        return Err(Status::invalid_argument(
            "legacy retirement checkpoint digest or collection differs",
        ));
    }
    Ok(())
}

fn frame(record: &wire::LegacyControlRetirement) -> Result<Vec<u8>, Status> {
    if record.encoded_len() > MAX_RECORD_BYTES - PREFIX_BYTES {
        return Err(Status::resource_exhausted(
            "legacy retirement record exceeds 17MiB",
        ));
    }
    validate_record(record)?;
    let payload = record.encode_to_vec();
    let mut bytes = Vec::with_capacity(PREFIX_BYTES + payload.len());
    bytes.extend_from_slice(RETIRED_CONTROL_MAGIC);
    bytes.extend_from_slice(&crate::sha256::digest(&payload));
    bytes.extend_from_slice(&payload);
    Ok(bytes)
}

fn decode_frame(bytes: &[u8]) -> Result<wire::LegacyControlRetirement, Status> {
    let corrupt = |message: &str| Status::data_loss(format!("legacy retirement record: {message}"));
    if bytes.len() > MAX_RECORD_BYTES
        || bytes.len() < PREFIX_BYTES
        || !bytes.starts_with(RETIRED_CONTROL_MAGIC)
    {
        return Err(corrupt("missing retirement frame or invalid size"));
    }
    let payload = &bytes[PREFIX_BYTES..];
    if crate::sha256::digest(payload).as_slice() != &bytes[8..PREFIX_BYTES] {
        return Err(corrupt("checksum mismatch"));
    }
    let record = wire::LegacyControlRetirement::decode(payload)
        .map_err(|_| corrupt("malformed protobuf"))?;
    if record.encode_to_vec() != payload {
        return Err(corrupt("unknown or noncanonical fields"));
    }
    validate_record(&record).map_err(|error| corrupt(error.message()))?;
    Ok(record)
}

fn same_retirement(
    record: &wire::LegacyControlRetirement,
    authority: &wire::SourceAuthorityIdentity,
    operation: &wire::SourceAuthorityOperationKey,
    request: &wire::LegacyControlRetirementRequest,
) -> Result<(), Status> {
    if record.authority.as_ref() != Some(authority) || record.operation.as_ref() != Some(operation)
    {
        return Err(Status::failed_precondition(
            "legacy control was retired for another authority, actor, resource or operation",
        ));
    }
    if record.request.as_ref() != Some(request) {
        return Err(Status::already_exists(
            "legacy retirement operation ID already binds a different request",
        ));
    }
    Ok(())
}

impl DurableControlPlane {
    pub(crate) fn retire_for_import(
        &self,
        authority: &wire::SourceAuthorityIdentity,
        operation: &wire::SourceAuthorityOperationKey,
        request: &wire::LegacyControlRetirementRequest,
        check_first_revision: impl FnOnce() -> Result<(), Status>,
    ) -> Result<RetiredLegacyControl, Status> {
        // Bypass lock_state only here: exact retries may read retirement evidence,
        // while all other authority operations remain permanently fenced.
        let state = self
            .state
            .lock()
            .map_err(|_| Status::internal("control state lock poisoned"))?;
        if self.persistence_uncertain.load(Ordering::Acquire) {
            return Err(Status::failed_precondition(UNCERTAIN_CONTROL_STATE));
        }
        let path = self.path.as_ref().ok_or_else(|| {
            Status::failed_precondition("legacy retirement requires a durable control file")
        })?;
        let ownership_lock = self._ownership_lock.as_ref().ok_or_else(|| {
            Status::failed_precondition("legacy retirement requires exclusive file ownership")
        })?;
        if let Some(record) = self.retirement.get() {
            same_retirement(record, authority, operation, request)?;
            return Ok(RetiredLegacyControl {
                record: record.clone(),
                _ownership_lock: ownership_lock.clone(),
            });
        }
        check_first_revision()?;
        let checkpoint = checkpoint::encode(&state, &self.policy)?;
        if request.expected_checkpoint_sha256 != crate::sha256::digest(&checkpoint)
            || request.key.as_ref().map(|key| key.collection.as_str())
                != Some(state.collection.as_str())
        {
            return Err(Status::failed_precondition(
                "legacy retirement checkpoint digest or collection changed",
            ));
        }
        let record = Arc::new(wire::LegacyControlRetirement {
            format_version: 1,
            authority: Some(authority.clone()),
            operation: Some(operation.clone()),
            request: Some(request.clone()),
            checkpoint,
        });
        let bytes = frame(&record)?;
        if let Err(error) = write_bytes(
            path,
            &bytes,
            #[cfg(test)]
            &self.write_fault,
        ) {
            if error.may_have_published {
                self.persistence_uncertain.store(true, Ordering::Release);
            }
            return Err(Status::internal(format!(
                "legacy retirement persistence: {}",
                error.message
            )));
        }
        // Only this method can set the cell, under the shared state mutex.
        if self.retirement.set(record.clone()).is_err() {
            self.persistence_uncertain.store(true, Ordering::Release);
            return Err(Status::internal(
                "legacy retirement marker changed under authority lock",
            ));
        }
        Ok(RetiredLegacyControl {
            record,
            _ownership_lock: ownership_lock.clone(),
        })
    }
}

pub(crate) fn recover(
    path: &Path,
    authority: &wire::SourceAuthorityIdentity,
    operation: &wire::SourceAuthorityOperationKey,
    request: &wire::LegacyControlRetirementRequest,
) -> Result<RetiredLegacyControl, Status> {
    let (path, ownership_lock) =
        storage::acquire(path, false).map_err(Status::failed_precondition)?;
    let (bytes, file) = storage::read_bounded(&path, MAX_RECORD_BYTES).map_err(|error| {
        if error.kind() == std::io::ErrorKind::InvalidData {
            Status::data_loss(format!("legacy retirement read: {error}"))
        } else {
            Status::internal(format!("legacy retirement read: {error}"))
        }
    })?;
    let record = decode_frame(&bytes)?;
    same_retirement(&record, authority, operation, request)?;
    // A visible rename from an uncertain attempt is not yet durable evidence.
    // Sync that exact opened inode and its directory before issuing the holder.
    file.sync_all()
        .and_then(|_| {
            File::open(path.parent().expect("canonical parent"))
                .and_then(|directory| directory.sync_all())
        })
        .map_err(|error| Status::internal(format!("legacy retirement recovery sync: {error}")))?;
    Ok(RetiredLegacyControl {
        record: Arc::new(record),
        _ownership_lock: ownership_lock,
    })
}

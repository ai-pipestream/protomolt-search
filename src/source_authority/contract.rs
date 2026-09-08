//! Validation and canonical encodings for our own versioned control records.
use super::*;

pub(super) const MAX_POLICY_BYTES: usize = 1024 * 1024;
pub(super) const MAX_RECORD_BYTES: usize = 2 * 1024 * 1024;

fn bounded(value: &[u8], max: usize, name: &str) -> Result<(), Status> {
    if value.is_empty() || value.len() > max {
        return Err(Status::invalid_argument(format!(
            "source authority {name} must contain 1..{max} bytes"
        )));
    }
    Ok(())
}

fn uuid(value: &[u8], name: &str) -> Result<(), Status> {
    if value.len() != 16 || value.iter().all(|byte| *byte == 0) {
        return Err(Status::invalid_argument(format!(
            "source authority {name} must be a nonzero 16-byte identity"
        )));
    }
    Ok(())
}

pub(super) fn identity(value: &SourceAuthorityIdentity) -> Result<(), Status> {
    if value.format_version != 1 {
        return Err(Status::invalid_argument(
            "source authority identity requires format 1",
        ));
    }
    uuid(&value.group_id, "group_id")?;
    uuid(&value.authority_incarnation, "authority_incarnation")
}

pub(super) fn limits(value: &SourceAuthorityLimits) -> Result<(), Status> {
    if !(1..=1_000_000).contains(&value.max_owners)
        || !(1..=1_000_000).contains(&value.max_decisions)
        || !(1..=1 << 40).contains(&value.max_payload_bytes)
        || !(1..=1024 * 1024).contains(&value.max_command_bytes)
        || u64::from(value.max_command_bytes) > value.max_payload_bytes
    {
        return Err(Status::invalid_argument("source authority limits require owners/decisions 1..1000000, payload 1..1TiB, and command 1..1MiB within payload capacity"));
    }
    Ok(())
}

pub(super) fn resource(workspace: &str, collection: &str) -> Result<(), Status> {
    if workspace.is_empty() {
        return Err(Status::invalid_argument(
            "source authority workspace is required",
        ));
    }
    crate::collections::validate_name(workspace).map_err(Status::invalid_argument)?;
    if !collection.is_empty() {
        crate::collections::validate_name(collection).map_err(Status::invalid_argument)?;
    }
    Ok(())
}

pub(super) fn key(value: &LogicalSourceOwner, owner_required: bool) -> Result<(), Status> {
    resource(&value.workspace, &value.collection)?;
    if owner_required || !value.owner_id.is_empty() {
        bounded(&value.owner_id, 1024, "logical owner_id")?;
    }
    Ok(())
}

pub(super) fn principal(value: &str) -> Result<(), Status> {
    bounded(value.as_bytes(), 16384, "principal")
}

pub(super) fn operation_id(value: &[u8]) -> Result<(), Status> {
    bounded(value, 1024, "command_id")
}

pub(super) fn workflow_id(value: &[u8]) -> Result<(), Status> {
    bounded(value, 1024, "workflow_id")
}

pub(super) fn target(value: &SourceStorageTarget) -> Result<(), Status> {
    bounded(value.node_id.as_bytes(), 1024, "node_id")?;
    uuid(&value.storage_incarnation, "storage_incarnation")?;
    uuid(&value.history_id, "history_id")?;
    match SourceResidency::try_from(value.residency) {
        Ok(SourceResidency::Server) if value.resident_device_id.is_empty() => Ok(()),
        Ok(SourceResidency::DeviceLocal) if value.resident_device_id == value.node_id => Ok(()),
        _ => Err(Status::invalid_argument("source authority residency requires SERVER without a device or DEVICE_LOCAL bound to this node_id")),
    }
}

pub(super) fn policy(value: &AccessPolicy) -> Result<(), Status> {
    if value.encoded_len() > MAX_POLICY_BYTES {
        return Err(Status::resource_exhausted(
            "source authority policy exceeds 1MiB",
        ));
    }
    for grant in &value.grants {
        principal(&grant.principal)?;
    }
    crate::authorization::validate_policy_snapshot(value).map_err(Status::invalid_argument)
}

pub(super) fn authorize(
    value: &AccessPolicy,
    actor: &str,
    key: &LogicalSourceOwner,
) -> Result<(), Status> {
    principal(actor)?;
    let decision = crate::authorization::authorize_policy_snapshot(
        value,
        actor,
        &key.collection,
        crate::pb::AccessAction::Admin,
    )?;
    if decision.workspace != key.workspace {
        return Err(Status::permission_denied(
            "source authority workspace differs from its committed collection binding",
        ));
    }
    Ok(())
}

pub(super) fn command(
    value: &SourceAuthorityCommand,
    expected: &SourceAuthorityIdentity,
    capacity: &SourceAuthorityLimits,
) -> Result<(), Status> {
    if value.encoded_len() > capacity.max_command_bytes as usize {
        return Err(Status::resource_exhausted(
            "source authority command exceeds max_command_bytes",
        ));
    }
    if value.format_version != 1 {
        return Err(Status::invalid_argument(
            "source authority command requires format 1",
        ));
    }
    if value.authority.as_ref() != Some(expected) {
        return Err(Status::failed_precondition(
            "source authority group or incarnation differs",
        ));
    }
    let own = value
        .key
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("source authority command key is required"))?;
    let policy_command = matches!(value.action, Some(Action::ReplaceGrants(_)));
    key(own, !policy_command)?;
    if policy_command && (!own.owner_id.is_empty() || value.expected_ownership_generation != 0) {
        return Err(Status::invalid_argument(
            "collection-policy commands require an empty owner and generation zero",
        ));
    }
    operation_id(&value.command_id)?;
    if value.action.is_none() {
        return Err(Status::invalid_argument(
            "source authority action is missing or unsupported",
        ));
    }
    Ok(())
}

pub(super) fn owner(value: &PreparedSourceOwner) -> Result<(), Status> {
    if value.format_version != 1 || value.ownership_generation == 0 || value.control_revision == 0 {
        return Err(Status::data_loss(
            "source authority owner format or counters are invalid",
        ));
    }
    key(
        value
            .key
            .as_ref()
            .ok_or_else(|| Status::data_loss("source authority owner key missing"))?,
        true,
    )?;
    target(
        value
            .target
            .as_ref()
            .ok_or_else(|| Status::data_loss("source authority owner target missing"))?,
    )?;
    workflow_id(&value.workflow_id)?;
    let command = value
        .last_command
        .as_ref()
        .ok_or_else(|| Status::data_loss("source authority owner last command missing"))?;
    if command.format_version != 1 || command.key != value.key {
        return Err(Status::data_loss(
            "source authority owner command binding differs",
        ));
    }
    principal(&command.principal)?;
    operation_id(&command.command_id)?;
    if !matches!(
        PreparedSourceOwnerPhase::try_from(value.phase),
        Ok(PreparedSourceOwnerPhase::Prepared | PreparedSourceOwnerPhase::Cancelled)
    ) {
        return Err(Status::data_loss(
            "source authority owner phase is unsupported",
        ));
    }
    Ok(())
}

pub(super) fn decode<T: Message + Default>(bytes: &[u8]) -> Result<T, Status> {
    if bytes.len() > MAX_RECORD_BYTES {
        return Err(Status::data_loss("source authority record exceeds 2MiB"));
    }
    let value = T::decode(bytes)
        .map_err(|error| Status::data_loss(format!("source authority protobuf: {error}")))?;
    // These are private records emitted canonically by our writer. Unknown
    // semantics, including future fields without a new format, cannot vanish.
    if value.encode_to_vec() != bytes {
        return Err(Status::data_loss(
            "source authority record has unknown or noncanonical fields",
        ));
    }
    Ok(value)
}

pub(super) fn operation_key(
    actor: &str,
    key: &LogicalSourceOwner,
    id: &[u8],
) -> SourceAuthorityOperationKey {
    SourceAuthorityOperationKey {
        format_version: 1,
        principal: actor.to_string(),
        key: Some(key.clone()),
        command_id: id.to_vec(),
    }
}

pub(super) fn workflow_key(key: &LogicalSourceOwner, id: &[u8]) -> SourceAuthorityWorkflowKey {
    SourceAuthorityWorkflowKey {
        key: Some(key.clone()),
        workflow_id: id.to_vec(),
    }
}

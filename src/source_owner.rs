//! Shared validation of persisted owner identities, including mobile readers.
use crate::pb::storage::*;
use tonic::Status;

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

pub(crate) fn identity(value: &SourceAuthorityIdentity) -> Result<(), Status> {
    if value.format_version != 1 {
        return Err(Status::invalid_argument(
            "source authority identity requires format 1",
        ));
    }
    uuid(&value.group_id, "group_id")?;
    uuid(&value.authority_incarnation, "authority_incarnation")
}

pub(crate) fn resource(workspace: &str, collection: &str) -> Result<(), Status> {
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

pub(crate) fn key(value: &LogicalSourceOwner, owner_required: bool) -> Result<(), Status> {
    resource(&value.workspace, &value.collection)?;
    if owner_required || !value.owner_id.is_empty() {
        bounded(&value.owner_id, 1024, "logical owner_id")?;
    }
    Ok(())
}

pub(crate) fn principal(value: &str) -> Result<(), Status> {
    bounded(value.as_bytes(), 16384, "principal")
}

pub(crate) fn operation_id(value: &[u8]) -> Result<(), Status> {
    bounded(value, 1024, "command_id")
}

pub(crate) fn workflow_id(value: &[u8]) -> Result<(), Status> {
    bounded(value, 1024, "workflow_id")
}

pub(crate) fn target(value: &SourceStorageTarget) -> Result<(), Status> {
    bounded(value.node_id.as_bytes(), 1024, "node_id")?;
    uuid(&value.storage_incarnation, "storage_incarnation")?;
    uuid(&value.history_id, "history_id")?;
    match SourceResidency::try_from(value.residency) {
        Ok(SourceResidency::Server) if value.resident_device_id.is_empty() => Ok(()),
        Ok(SourceResidency::DeviceLocal) if value.resident_device_id == value.node_id => Ok(()),
        _ => Err(Status::invalid_argument("source authority residency requires SERVER without a device or DEVICE_LOCAL bound to this node_id")),
    }
}

pub(crate) fn owner(value: &PreparedSourceOwner) -> Result<(), Status> {
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

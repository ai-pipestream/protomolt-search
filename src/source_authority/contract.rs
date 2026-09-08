//! Validation and canonical encodings for our own versioned control records.
use super::*;
pub(super) use crate::source_owner::{
    completion_matches, identity, key, operation_id, owner, principal, target, workflow_id,
};

pub(super) const MAX_POLICY_BYTES: usize = 1024 * 1024;
pub(super) const MAX_RECORD_BYTES: usize = 2 * 1024 * 1024;

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
    match value.action.as_ref() {
        None => Err(Status::invalid_argument(
            "source authority action is missing or unsupported",
        )),
        Some(Action::ConfirmReady(request)) => {
            workflow_id(&request.workflow_id)?;
            let completion = request.completion.as_ref().ok_or_else(|| {
                Status::invalid_argument("readiness confirmation requires the completion")
            })?;
            if completion.format_version != 1 || completion.binding_sha256.len() != 32 {
                return Err(Status::invalid_argument(
                    "source owner completion requires format 1 and a 32-byte binding digest",
                ));
            }
            bounded_field(completion.node_id.as_bytes(), 1024, "completion node_id")?;
            if completion.history_id.len() != 16 || completion.storage_incarnation.len() != 16 {
                return Err(Status::invalid_argument(
                    "source owner completion requires 16-byte history and storage identities",
                ));
            }
            Ok(())
        }
        Some(_) => Ok(()),
    }
}

fn bounded_field(value: &[u8], max: usize, what: &str) -> Result<(), Status> {
    if value.is_empty() || value.len() > max {
        return Err(Status::invalid_argument(format!(
            "{what} must be 1..{max} bytes"
        )));
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

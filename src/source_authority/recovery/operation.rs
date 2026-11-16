//! Audit of one retained command record against its key, its command and
//! the committed revisions.
use super::*;

pub(in crate::source_authority) fn operation(
    key: &SourceAuthorityOperationKey,
    value: &SourceAuthorityOperation,
    header: &SourceAuthorityHeader,
    policy: &AccessPolicy,
) -> Result<(), Status> {
    if key.format_version != 1 || value.format_version != 1 {
        return Err(corrupt("unsupported operation format"));
    }
    contract::principal(&key.principal).map_err(corrupt)?;
    contract::operation_id(&key.command_id).map_err(corrupt)?;
    let command = value
        .command
        .as_ref()
        .ok_or_else(|| missing("stored command"))?;
    contract::command(
        command,
        header
            .identity
            .as_ref()
            .ok_or_else(|| missing("identity"))?,
        header.limits.as_ref().ok_or_else(|| missing("limits"))?,
    )
    .map_err(corrupt)?;
    if key.key != command.key
        || key.command_id != command.command_id
        || value.request_sha256 != digest(command)
    {
        return Err(corrupt("operation key or request digest differs"));
    }
    let decision = value
        .decision
        .as_ref()
        .ok_or_else(|| missing("stored decision"))?;
    if decision.format_version != 1
        || decision.control_revision == 0
        || decision.control_revision > header.control_revision
        || decision.policy_revision == 0
        || decision.policy_revision > policy.revision
        || ![0, 3, 5, 6, 7, 8, 9].contains(&decision.code)
    {
        return Err(corrupt("decision format, status or revision is invalid"));
    }
    if decision.code != 0 {
        if decision.owner.is_some() || decision.message.is_empty() {
            return Err(corrupt(
                "rejected decision carries owner state or no reason",
            ));
        }
        return Ok(());
    }
    if !decision.message.is_empty()
        || command.expected_control_revision.checked_add(1) != Some(decision.control_revision)
    {
        return Err(corrupt(
            "accepted decision has an invalid control revision or reason",
        ));
    }
    let policy_changed = matches!(command.action, Some(Action::ReplaceGrants(_)));
    let expected_policy = if policy_changed {
        command.expected_policy_revision.checked_add(1)
    } else {
        Some(command.expected_policy_revision)
    };
    if expected_policy != Some(decision.policy_revision) {
        return Err(corrupt("accepted decision policy revision differs"));
    }
    match command.action.as_ref().ok_or_else(|| missing("action"))? {
        Action::ReplaceGrants(_) => {
            if decision.owner.is_some() {
                return Err(corrupt("policy decision carries owner state"));
            }
            Ok(())
        }
        action => owner_decision(key, command, decision, action),
    }
}

/// An accepted owner transition: the decision's owner row is bound to the
/// command and carries exactly the state the action produces.
fn owner_decision(
    key: &SourceAuthorityOperationKey,
    command: &SourceAuthorityCommand,
    decision: &SourceAuthorityDecision,
    action: &Action,
) -> Result<(), Status> {
    let owner = decision
        .owner
        .as_ref()
        .ok_or_else(|| missing("owner decision"))?;
    contract::owner(owner).map_err(corrupt)?;
    if owner.key != command.key
        || owner.last_command.as_ref() != Some(key)
        || owner.control_revision != decision.control_revision
    {
        return Err(corrupt("owner decision binding differs"));
    }
    match action {
        Action::Prepare(request) => {
            if owner.target != request.target
                || owner.workflow_id != request.workflow_id
                || owner.phase != PreparedSourceOwnerPhase::Prepared as i32
                || command.expected_ownership_generation.checked_add(1)
                    != Some(owner.ownership_generation)
            {
                return Err(corrupt("preparation decision differs from its command"));
            }
        }
        Action::Cancel(request) => {
            if owner.workflow_id != request.workflow_id
                || owner.phase != PreparedSourceOwnerPhase::Cancelled as i32
                || owner.ownership_generation != command.expected_ownership_generation
            {
                return Err(corrupt("cancellation decision differs from its command"));
            }
        }
        Action::ConfirmReady(request) => {
            if owner.workflow_id != request.workflow_id
                || owner.phase != PreparedSourceOwnerPhase::Ready as i32
                || owner.ownership_generation != command.expected_ownership_generation
                || owner.readiness.as_ref().and_then(|r| r.completion.as_ref())
                    != request.completion.as_ref()
            {
                return Err(corrupt("readiness decision differs from its command"));
            }
        }
        Action::Activate(request) => {
            if owner.workflow_id != request.workflow_id
                || owner.phase != PreparedSourceOwnerPhase::Active as i32
                || owner.ownership_generation != command.expected_ownership_generation
                || owner
                    .activation
                    .as_ref()
                    .map(|a| a.activated_control_revision)
                    != Some(decision.control_revision)
            {
                return Err(corrupt("activation decision differs from its command"));
            }
        }
        Action::ReplaceGrants(_) => {
            return Err(corrupt("policy command cannot carry an owner transition"))
        }
    }
    Ok(())
}

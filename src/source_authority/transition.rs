//! Pure control transitions over committed inputs. No IO, clock or live policy.
use super::*;

pub(super) struct Change {
    pub decision: SourceAuthorityDecision,
    pub owner: Option<PreparedSourceOwner>,
    pub policy: Option<AccessPolicy>,
    pub reserve_workflow: bool,
}

pub(super) fn apply(
    header: &SourceAuthorityHeader,
    policy: &AccessPolicy,
    before: Option<&PreparedSourceOwner>,
    command: &SourceAuthorityCommand,
    operation: &SourceAuthorityOperationKey,
    workflow_used: bool,
) -> Result<Change, Status> {
    contract::command(
        command,
        header
            .identity
            .as_ref()
            .ok_or_else(|| missing("identity"))?,
        header.limits.as_ref().ok_or_else(|| missing("limits"))?,
    )?;
    let key = command.key.as_ref().ok_or_else(|| missing("command key"))?;
    contract::authorize(policy, &operation.principal, key)?;
    if operation != &contract::operation_key(&operation.principal, key, &command.command_id) {
        return Err(corrupt("resolved command identity differs"));
    }
    if let Some(owner) = before {
        contract::owner(owner).map_err(corrupt)?;
        if owner.key != command.key || owner.control_revision > header.control_revision {
            return Err(corrupt(
                "current owner key or revision differs from the command state",
            ));
        }
    }
    let mut result = Change {
        decision: SourceAuthorityDecision {
            format_version: 1,
            control_revision: header.control_revision,
            policy_revision: policy.revision,
            ..Default::default()
        },
        owner: None,
        policy: None,
        reserve_workflow: false,
    };
    let transition = (|| -> Result<(), Status> {
        if command.expected_control_revision != header.control_revision {
            return Err(Status::failed_precondition(
                "source authority control revision differs",
            ));
        }
        if command.expected_policy_revision != policy.revision {
            return Err(Status::failed_precondition(
                "source authority policy revision differs",
            ));
        }
        let revision = header.control_revision.checked_add(1).ok_or_else(|| {
            Status::resource_exhausted("source authority control revision exhausted")
        })?;
        let key = command.key.as_ref().expect("validated command key");
        if !matches!(command.action, Some(Action::ReplaceGrants(_)))
            && command.expected_ownership_generation
                != before.map_or(0, |owner| owner.ownership_generation)
        {
            return Err(Status::failed_precondition(
                "source authority ownership generation differs",
            ));
        }
        match command.action.as_ref().expect("validated command action") {
            Action::Prepare(request) => {
                contract::workflow_id(&request.workflow_id)?;
                let target = request.target.as_ref().ok_or_else(|| {
                    Status::invalid_argument("source authority preparation target is required")
                })?;
                contract::target(target)?;
                if workflow_used {
                    return Err(Status::already_exists("source authority workflow is already reserved; retry its original command or use a new workflow"));
                }
                if let Some(held) = before {
                    if held.phase != PreparedSourceOwnerPhase::Cancelled as i32 {
                        return Err(Status::failed_precondition(
                            "source authority owner already has a preparation",
                        ));
                    }
                    let prior = held.target.as_ref().expect("validated owner target");
                    if prior.history_id != target.history_id
                        || prior.residency != target.residency
                        || (prior.residency == SourceResidency::DeviceLocal as i32
                            && prior.node_id != target.node_id)
                    {
                        return Err(Status::failed_precondition("source authority history or residency cannot change; a device-local source cannot move to another node"));
                    }
                }
                let generation = before
                    .map_or(0, |owner| owner.ownership_generation)
                    .checked_add(1)
                    .ok_or_else(|| {
                        Status::resource_exhausted(
                            "source authority ownership generation exhausted",
                        )
                    })?;
                result.owner = Some(PreparedSourceOwner {
                    format_version: 1,
                    key: Some(key.clone()),
                    target: Some(target.clone()),
                    workflow_id: request.workflow_id.clone(),
                    ownership_generation: generation,
                    phase: PreparedSourceOwnerPhase::Prepared as i32,
                    control_revision: revision,
                    last_command: Some(operation.clone()),
                    readiness: None,
                    activation: None,
                });
                result.reserve_workflow = true;
            }
            Action::Cancel(request) => {
                contract::workflow_id(&request.workflow_id)?;
                let held = before.ok_or_else(|| {
                    Status::not_found("source authority owner has no preparation")
                })?;
                if held.phase != PreparedSourceOwnerPhase::Prepared as i32
                    || held.workflow_id != request.workflow_id
                {
                    return Err(Status::failed_precondition(
                        "source authority cancellation requires the current prepared workflow",
                    ));
                }
                let mut cancelled = held.clone();
                cancelled.phase = PreparedSourceOwnerPhase::Cancelled as i32;
                cancelled.control_revision = revision;
                cancelled.last_command = Some(operation.clone());
                result.owner = Some(cancelled);
            }
            Action::ConfirmReady(request) => {
                // Admission (the adapter's held binding) is checked before this
                // command reaches the transition; apply trusts the committed
                // command, as a replica applying the same log entry must.
                let completion = request.completion.as_ref().expect("validated completion");
                let held = before.ok_or_else(|| {
                    Status::not_found("source authority owner has no preparation")
                })?;
                if held.phase != PreparedSourceOwnerPhase::Prepared as i32
                    || held.workflow_id != request.workflow_id
                {
                    return Err(Status::failed_precondition(
                        "source authority readiness requires the current prepared workflow",
                    ));
                }
                contract::completion_matches(
                    completion,
                    held.target.as_ref().expect("validated owner target"),
                )?;
                let mut ready = held.clone();
                ready.phase = PreparedSourceOwnerPhase::Ready as i32;
                ready.control_revision = revision;
                ready.last_command = Some(operation.clone());
                ready.readiness = Some(SourceOwnerReadiness {
                    format_version: 1,
                    completion: Some(completion.clone()),
                });
                result.owner = Some(ready);
            }
            Action::Activate(request) => {
                let held = before.ok_or_else(|| {
                    Status::not_found("source authority owner has no preparation")
                })?;
                if held.phase != PreparedSourceOwnerPhase::Ready as i32
                    || held.workflow_id != request.workflow_id
                {
                    return Err(Status::failed_precondition(
                        "source authority activation requires the READY owner of the current workflow",
                    ));
                }
                // The write epoch is the activated owner's generation: one
                // committed fence per ownership generation, never allocated
                // by a lease or a clock.
                let mut active = held.clone();
                active.phase = PreparedSourceOwnerPhase::Active as i32;
                active.control_revision = revision;
                active.last_command = Some(operation.clone());
                active.activation = Some(SourceOwnerActivation {
                    format_version: 1,
                    write_epoch: held.ownership_generation,
                    activated_control_revision: revision,
                });
                result.owner = Some(active);
            }
            Action::ReplaceGrants(request) => {
                if request.grants.iter().any(|grant| {
                    grant.workspace != key.workspace || grant.collection != key.collection
                }) {
                    return Err(Status::permission_denied(
                        "source authority grant replacement may only name its bound collection",
                    ));
                }
                let mut updated = policy.clone();
                updated.revision = policy.revision.checked_add(1).ok_or_else(|| {
                    Status::resource_exhausted("source authority policy revision exhausted")
                })?;
                updated
                    .grants
                    .retain(|grant| grant.collection != key.collection);
                updated.grants.extend(request.grants.iter().cloned());
                contract::policy(&updated)?;
                result.decision.policy_revision = updated.revision;
                result.policy = Some(updated);
            }
        }
        result.decision.control_revision = revision;
        result.decision.owner = result.owner.clone();
        Ok(())
    })();
    if let Err(error) = transition {
        result.decision.code = error.code() as u32;
        result.decision.message = error.message().to_string();
        result.owner = None;
        result.policy = None;
        result.reserve_workflow = false;
    }
    Ok(result)
}

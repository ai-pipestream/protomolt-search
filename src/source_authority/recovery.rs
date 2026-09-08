//! Bounded record validation before an existing authority becomes usable.
use super::*;

mod control;

pub(super) fn header(
    value: &SourceAuthorityHeader,
    expected: &SourceAuthorityIdentity,
) -> Result<(), Status> {
    if value.format_version != 1 || value.control_revision == 0 {
        return Err(corrupt(
            "unsupported header format or zero control revision",
        ));
    }
    let identity = value
        .identity
        .as_ref()
        .ok_or_else(|| missing("authority identity"))?;
    contract::identity(identity).map_err(corrupt)?;
    if identity != expected {
        return Err(Status::failed_precondition(
            "source authority group or incarnation differs",
        ));
    }
    let limits = value
        .limits
        .as_ref()
        .ok_or_else(|| missing("authority limits"))?;
    contract::limits(limits).map_err(corrupt)?;
    if value.owner_count > limits.max_owners
        || value.decision_count > limits.max_decisions
        || value.workflow_count > value.decision_count
        || value.payload_bytes > limits.max_payload_bytes
    {
        return Err(corrupt("header counters exceed configured capacity"));
    }
    Ok(())
}

pub(super) fn control_header(value: &ControlStoreHeader) -> Result<(), Status> {
    if value.format_version != import::STORE_FORMAT {
        return Err(corrupt(
            "unsupported control store format; this reader serves format 2 only",
        ));
    }
    Ok(())
}

pub(super) fn operation(
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
        }
        action => {
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
                Action::ReplaceGrants(_) => {
                    return Err(corrupt("policy command cannot carry an owner transition"))
                }
            }
        }
    }
    Ok(())
}

pub(super) fn validate(store: &SourceAuthorityStore) -> Result<(), Status> {
    let tx = store.inner.database.begin_read().map_err(storage)?;
    if tx.list_tables().map_err(storage)?.count() != import::FORMAT_2_TABLES
        || tx.list_multimap_tables().map_err(storage)?.next().is_some()
    {
        return Err(corrupt("unexpected or missing control tables"));
    }
    let meta = tx.open_table(META).map_err(corrupt)?;
    if meta.len().map_err(storage)? != 3 {
        return Err(corrupt("unexpected metadata keys"));
    }
    let control: ControlStoreHeader = contract::decode(
        meta.get(import::CONTROL_HEADER)
            .map_err(storage)?
            .ok_or_else(|| missing("control header"))?
            .value(),
    )?;
    control_header(&control)?;
    let state: SourceAuthorityHeader = contract::decode(
        meta.get("header")
            .map_err(storage)?
            .ok_or_else(|| missing("header"))?
            .value(),
    )?;
    header(&state, &store.inner.identity)?;
    let policy: AccessPolicy = contract::decode(
        meta.get("policy")
            .map_err(storage)?
            .ok_or_else(|| missing("policy"))?
            .value(),
    )?;
    contract::policy(&policy).map_err(corrupt)?;
    let owners = tx.open_table(OWNERS).map_err(corrupt)?;
    let decisions = tx.open_table(DECISIONS).map_err(corrupt)?;
    let workflows = tx.open_table(WORKFLOWS).map_err(corrupt)?;
    if owners.len().map_err(storage)? != state.owner_count
        || decisions.len().map_err(storage)? != state.decision_count
        || workflows.len().map_err(storage)? != state.workflow_count
    {
        return Err(corrupt("table counts differ from committed header"));
    }
    let mut payload = policy.encoded_len() as u64;
    let mut accepted = 0u64;
    for entry in decisions.iter().map_err(storage)? {
        let (key_bytes, value_bytes) = entry.map_err(storage)?;
        let key: SourceAuthorityOperationKey = contract::decode(key_bytes.value())?;
        let value: SourceAuthorityOperation = contract::decode(value_bytes.value())?;
        operation(&key, &value, &state, &policy)?;
        if value
            .decision
            .as_ref()
            .is_some_and(|decision| decision.code == 0)
        {
            accepted += 1;
        }
        payload = payload_change(
            payload,
            0,
            key_bytes.value().len() + value_bytes.value().len(),
        )?;
    }
    let (accepted_imports, control_payload) =
        control::validate(&tx, &state, &control, &policy, &decisions)?;
    payload = payload_change(payload, 0, control_payload)?;
    if accepted
        .checked_add(accepted_imports)
        .and_then(|n| n.checked_add(1))
        != Some(state.control_revision)
    {
        return Err(corrupt(
            "control revision differs from accepted decision count",
        ));
    }
    for entry in workflows.iter().map_err(storage)? {
        let (key_bytes, value_bytes) = entry.map_err(storage)?;
        let key: SourceAuthorityWorkflowKey = contract::decode(key_bytes.value())?;
        let workflow: SourceAuthorityWorkflow = contract::decode(value_bytes.value())?;
        let owner_key = key
            .key
            .as_ref()
            .ok_or_else(|| missing("workflow owner key"))?;
        contract::key(owner_key, true).map_err(corrupt)?;
        contract::workflow_id(&key.workflow_id).map_err(corrupt)?;
        let preparation = workflow
            .preparation_command
            .as_ref()
            .ok_or_else(|| missing("workflow preparation command"))?;
        let command_key = preparation.encode_to_vec();
        let operation: SourceAuthorityOperation = contract::decode(
            decisions
                .get(command_key.as_slice())
                .map_err(storage)?
                .ok_or_else(|| missing("workflow decision"))?
                .value(),
        )?;
        let decision = operation
            .decision
            .as_ref()
            .ok_or_else(|| missing("workflow result"))?;
        let owner = decision
            .owner
            .as_ref()
            .ok_or_else(|| missing("workflow prepared owner"))?;
        if decision.code != 0
            || owner.phase != PreparedSourceOwnerPhase::Prepared as i32
            || owner.key != key.key
            || owner.workflow_id != key.workflow_id
            || owner.ownership_generation != workflow.ownership_generation
        {
            return Err(corrupt("workflow differs from its preparation decision"));
        }
        payload = payload_change(
            payload,
            0,
            key_bytes.value().len() + value_bytes.value().len(),
        )?;
    }
    for entry in owners.iter().map_err(storage)? {
        let (key_bytes, value_bytes) = entry.map_err(storage)?;
        let key: LogicalSourceOwner = contract::decode(key_bytes.value())?;
        let owner: PreparedSourceOwner = contract::decode(value_bytes.value())?;
        contract::owner(&owner).map_err(corrupt)?;
        if owner.key.as_ref() != Some(&key)
            || owner.control_revision > state.control_revision
            || !policy.resources.iter().any(|resource| {
                resource.workspace == key.workspace && resource.collection == key.collection
            })
        {
            return Err(corrupt("owner key, resource or revision differs"));
        }
        let latest = owner
            .last_command
            .as_ref()
            .ok_or_else(|| missing("owner last command"))?
            .encode_to_vec();
        let operation: SourceAuthorityOperation = contract::decode(
            decisions
                .get(latest.as_slice())
                .map_err(storage)?
                .ok_or_else(|| missing("owner last decision"))?
                .value(),
        )?;
        if !operation
            .decision
            .as_ref()
            .is_some_and(|decision| decision.code == 0 && decision.owner.as_ref() == Some(&owner))
        {
            return Err(corrupt("owner differs from its last decision"));
        }
        let workflow_key = contract::workflow_key(&key, &owner.workflow_id).encode_to_vec();
        let workflow: SourceAuthorityWorkflow = contract::decode(
            workflows
                .get(workflow_key.as_slice())
                .map_err(storage)?
                .ok_or_else(|| missing("owner workflow reservation"))?
                .value(),
        )?;
        if workflow.ownership_generation != owner.ownership_generation {
            return Err(corrupt("owner workflow generation differs"));
        }
        payload = payload_change(
            payload,
            0,
            key_bytes.value().len() + value_bytes.value().len(),
        )?;
    }
    if payload != state.payload_bytes {
        return Err(corrupt("payload bytes differ from committed header"));
    }
    Ok(())
}

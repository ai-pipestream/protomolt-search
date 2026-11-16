//! Bounded record validation before an existing authority becomes usable.
use super::*;

mod control;
mod operation;

pub(super) use operation::operation;

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
    if value.format_version != super::control::STORE_FORMAT {
        return Err(corrupt(
            "unsupported control store format; this reader serves format 3 only",
        ));
    }
    Ok(())
}

/// The full audit an existing store passes before it serves: tables,
/// metadata keys, headers, every retained decision and every row that a
/// decision or a header accounts for. Nothing is repaired.
pub(super) fn validate(
    database: &Database,
    identity: &SourceAuthorityIdentity,
) -> Result<(), Status> {
    let tx = database.begin_read().map_err(storage)?;
    if tx.list_tables().map_err(storage)?.count() != STORE_TABLES
        || tx.list_multimap_tables().map_err(storage)?.next().is_some()
    {
        return Err(corrupt("unexpected or missing control tables"));
    }
    let meta = tx.open_table(META).map_err(corrupt)?;
    audit_meta_keys(&meta)?;
    let control: ControlStoreHeader = contract::decode(
        meta.get(super::control::CONTROL_HEADER)
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
    header(&state, identity)?;
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
    let payload = policy.encoded_len() as u64;
    let (accepted, mut payload) = audit_decisions(&decisions, &state, &policy, payload)?;
    let (accepted_installs, control_payload) =
        control::validate(&tx, &state, &control, &policy, &decisions)?;
    payload = payload_change(payload, 0, control_payload)?;
    let (accepted_configures, capacity_payload) =
        capacity::validate(&tx, &meta, &state, &policy, &decisions)?;
    payload = payload_change(payload, 0, capacity_payload)?;
    if accepted
        .checked_add(accepted_installs)
        .and_then(|n| n.checked_add(accepted_configures))
        .and_then(|n| n.checked_add(1))
        != Some(state.control_revision)
    {
        return Err(corrupt(
            "control revision differs from accepted decision count",
        ));
    }
    payload = audit_workflows(&workflows, &decisions, payload)?;
    payload = audit_owners(&owners, &decisions, &workflows, (&state, &policy), payload)?;
    if payload != state.payload_bytes {
        return Err(corrupt("payload bytes differ from committed header"));
    }
    Ok(())
}

type ReadTable = redb::ReadOnlyTable<&'static [u8], &'static [u8]>;

/// The metadata keys of a store: the three headers and the policy, an applied
/// Raft position, or the pending marker of a prepared member.
fn audit_meta_keys(meta: &redb::ReadOnlyTable<&'static str, &'static [u8]>) -> Result<(), Status> {
    let raft = meta.get(RAFT_META).map_err(storage)?.is_some();
    let member = meta.get(MEMBER_META).map_err(storage)?.is_some();
    if meta.len().map_err(storage)? != 4 + u64::from(raft) + u64::from(member) {
        return Err(corrupt("unexpected metadata keys"));
    }
    if member && raft {
        return Err(corrupt(
            "a prepared member store carries an applied position",
        ));
    }
    if raft {
        let applied: RaftApplied = contract::decode(
            meta.get(RAFT_META)
                .map_err(storage)?
                .ok_or_else(|| missing("raft applied position"))?
                .value(),
        )?;
        if applied.format_version != 1 || applied.last_applied.is_none() {
            return Err(corrupt("raft applied position format or log id is invalid"));
        }
    }
    Ok(())
}

/// Audit every retained decision. Returns the accepted count and `payload`
/// with the bytes of the decision table added.
fn audit_decisions(
    decisions: &ReadTable,
    state: &SourceAuthorityHeader,
    policy: &AccessPolicy,
    mut payload: u64,
) -> Result<(u64, u64), Status> {
    let mut accepted = 0u64;
    for entry in decisions.iter().map_err(storage)? {
        let (key_bytes, value_bytes) = entry.map_err(storage)?;
        let key: SourceAuthorityOperationKey = contract::decode(key_bytes.value())?;
        let value: SourceAuthorityOperation = contract::decode(value_bytes.value())?;
        operation(&key, &value, state, policy)?;
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
    Ok((accepted, payload))
}

/// Audit every workflow reservation against its preparation decision.
/// Returns `payload` with the bytes of the workflow table added.
fn audit_workflows(
    workflows: &ReadTable,
    decisions: &ReadTable,
    mut payload: u64,
) -> Result<u64, Status> {
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
    Ok(payload)
}

/// Audit every owner row against its last decision and its workflow
/// reservation. Returns `payload` with the bytes of the owner table added.
fn audit_owners(
    owners: &ReadTable,
    decisions: &ReadTable,
    workflows: &ReadTable,
    (state, policy): (&SourceAuthorityHeader, &AccessPolicy),
    mut payload: u64,
) -> Result<u64, Status> {
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
    Ok(payload)
}

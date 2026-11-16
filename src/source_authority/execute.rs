//! Execution of one control command: the transition, the current policy
//! check, the retry record and the counters in one durable transaction.
use super::*;

type MetaTable<'txn> = redb::Table<'txn, &'static str, &'static [u8]>;
type RowTable<'txn> = redb::Table<'txn, &'static [u8], &'static [u8]>;

/// The outcome of applying a command inside its write transaction.
enum Applied {
    /// An exact retry: the retained decision, with no application change.
    Retry(SourceAuthorityDecision),
    /// A newly recorded decision, published after the commit.
    Recorded(SourceAuthorityDecision),
}

/// The committed inputs one command resolves against, read once.
struct Resolved<'a> {
    key: &'a LogicalSourceOwner,
    header: SourceAuthorityHeader,
    policy: AccessPolicy,
    limits: SourceAuthorityLimits,
    operation_key: SourceAuthorityOperationKey,
    operation_bytes: Vec<u8>,
    request_sha256: Vec<u8>,
    retained: u64,
}

/// The committed owner row of a command's key: its stored bytes and the
/// validated record.
struct HeldOwner {
    bytes: Option<Vec<u8>>,
    row: Option<PreparedSourceOwner>,
}

impl SourceAuthorityStore {
    /// Execute one authenticated control command. The transition, current policy
    /// check, retry response and counters share one durable database transaction.
    /// Recorded business rejections are decisions with a nonzero status code;
    /// envelope/admission/capacity/storage errors are returned as Status.
    pub fn execute(
        &self,
        principal: &str,
        command: &SourceAuthorityCommand,
    ) -> Result<SourceAuthorityDecision, Status> {
        self.direct()?;
        if matches!(command.action, Some(Action::ConfirmReady(_))) {
            return Err(Status::permission_denied(
                "readiness confirmation requires the hosting adapter with the managed binding",
            ));
        }
        self.execute_admitted(principal, command, OwnerAdmission::None)
    }

    /// Apply a command that a trusted proposal already admitted: the
    /// committed-log path. A ConfirmReady replays without the managed
    /// binding; every other check runs against the committed rows.
    #[cfg_attr(not(feature = "raft"), allow(dead_code))]
    pub(crate) fn replay_command(
        &self,
        principal: &str,
        command: &SourceAuthorityCommand,
    ) -> Result<SourceAuthorityDecision, Status> {
        self.execute_admitted(principal, command, OwnerAdmission::Committed)
    }

    pub(super) fn execute_admitted(
        &self,
        principal: &str,
        command: &SourceAuthorityCommand,
        verified: OwnerAdmission<'_>,
    ) -> Result<SourceAuthorityDecision, Status> {
        let _exclusive = self.exclusive()?;
        self.guarded(|| {
            let decision = self.execute_locked(principal, command, verified)?;
            // A policy command can revoke its own issuer. Its durable decision
            // remains retryable, but disclosure still needs the current grant.
            let tx = self.inner.database().begin_read().map_err(storage)?;
            self.read_policy(
                &tx,
                principal,
                command.key.as_ref().ok_or_else(|| missing("command key"))?,
            )?;
            Ok(decision)
        })
    }

    fn execute_locked(
        &self,
        principal: &str,
        command: &SourceAuthorityCommand,
        verified: OwnerAdmission<'_>,
    ) -> Result<SourceAuthorityDecision, Status> {
        let mut tx = self.inner.database().begin_write().map_err(storage)?;
        tx.set_durability(Durability::Immediate).map_err(storage)?;
        let decision = match self.apply_command(&tx, principal, command, verified)? {
            Applied::Retry(decision) => {
                self.finish_retry(tx)?;
                return Ok(decision);
            }
            Applied::Recorded(decision) => decision,
        };
        #[cfg(any(test, feature = "fault-injection"))]
        self.inject(false)?;
        tx.commit().map_err(storage)?;
        self.inner.revisions.send_replace(decision.policy_revision);
        self.publish_applied(decision.control_revision);
        #[cfg(any(test, feature = "fault-injection"))]
        self.inject(true)?;
        Ok(decision)
    }

    /// Apply `command` to the tables of `tx` without committing it.
    fn apply_command(
        &self,
        tx: &redb::WriteTransaction,
        principal: &str,
        command: &SourceAuthorityCommand,
        verified: OwnerAdmission<'_>,
    ) -> Result<Applied, Status> {
        let mut meta = tx.open_table(META).map_err(storage)?;
        let mut resolved = self.resolve(&meta, principal, command)?;
        refuse_other_namespaces(tx, &resolved.operation_bytes)?;
        let mut decisions = tx.open_table(DECISIONS).map_err(storage)?;
        if let Some(decision) = retained_decision(&decisions, &resolved, command)? {
            // Under Raft the entry is consumed: the applied position
            // advances for an exact retry too, with no other change.
            self.write_pending_applied(&mut meta)?;
            return Ok(Applied::Retry(decision));
        }
        if resolved
            .header
            .decision_count
            .saturating_add(resolved.retained)
            >= resolved.limits.max_decisions
        {
            return Err(Status::resource_exhausted(
                "source authority decision capacity is full; retry history is never evicted",
            ));
        }
        let mut owners = tx.open_table(OWNERS).map_err(storage)?;
        let key_bytes = resolved.key.encode_to_vec();
        let held = held_owner(&owners, &key_bytes)?;
        admit_readiness(command, verified, held.row.as_ref())?;
        let mut workflows = tx.open_table(WORKFLOWS).map_err(storage)?;
        let workflow_bytes = match command.action.as_ref() {
            Some(Action::Prepare(request)) => {
                Some(contract::workflow_key(resolved.key, &request.workflow_id).encode_to_vec())
            }
            _ => None,
        };
        let workflow_used = if let Some(bytes) = &workflow_bytes {
            workflows.get(bytes.as_slice()).map_err(storage)?.is_some()
        } else {
            false
        };
        let change = transition::apply(
            &resolved.header,
            &resolved.policy,
            held.row.as_ref(),
            command,
            &resolved.operation_key,
            workflow_used,
        )?;
        let decision = change.decision;
        let encoded = record_decision(&mut resolved, command, &decision)?;
        if let Some(owner) = change.owner {
            store_owner(&mut resolved, &mut owners, &key_bytes, &held, &owner)?;
            if change.reserve_workflow {
                let generation = owner.ownership_generation;
                reserve_workflow(&mut resolved, &mut workflows, &workflow_bytes, generation)?;
            }
        }
        if let Some(updated) = change.policy {
            store_policy(&mut resolved, &mut meta, &updated)?;
        }
        if resolved.header.payload_bytes > resolved.limits.max_payload_bytes {
            return Err(Status::resource_exhausted(
                "source authority payload capacity is full; retry history is never evicted",
            ));
        }
        resolved.header.control_revision = decision.control_revision;
        decisions
            .insert(resolved.operation_bytes.as_slice(), encoded.as_slice())
            .map_err(storage)?;
        meta.insert("header", resolved.header.encode_to_vec().as_slice())
            .map_err(storage)?;
        self.write_pending_applied(&mut meta)?;
        Ok(Applied::Recorded(decision))
    }

    /// Read the committed header and policy, validate the command envelope
    /// against them and authorize the actor on the command's key.
    fn resolve<'a>(
        &self,
        meta: &MetaTable<'_>,
        principal: &str,
        command: &'a SourceAuthorityCommand,
    ) -> Result<Resolved<'a>, Status> {
        let header: SourceAuthorityHeader = contract::decode(
            meta.get("header")
                .map_err(storage)?
                .ok_or_else(|| missing("header"))?
                .value(),
        )?;
        let policy: AccessPolicy = contract::decode(
            meta.get("policy")
                .map_err(storage)?
                .ok_or_else(|| missing("policy"))?
                .value(),
        )?;
        contract::policy(&policy).map_err(corrupt)?;
        recovery::header(&header, &self.inner.identity)?;
        let limits = header.limits.ok_or_else(|| missing("limits"))?;
        contract::command(command, &self.inner.identity, &limits)?;
        let key = command.key.as_ref().ok_or_else(|| missing("command key"))?;
        contract::authorize(&policy, principal, key)?;
        let operation_key = contract::operation_key(principal, key, &command.command_id);
        let operation_bytes = operation_key.encode_to_vec();
        let request_sha256 = digest(command);
        let retained = control::retained_decisions(meta)?;
        Ok(Resolved {
            key,
            header,
            policy,
            limits,
            operation_key,
            operation_bytes,
            request_sha256,
            retained,
        })
    }
}

/// A command_id recorded by a control install or a capacity command never
/// names a command of this namespace.
fn refuse_other_namespaces(
    tx: &redb::WriteTransaction,
    operation_bytes: &[u8],
) -> Result<(), Status> {
    if tx
        .open_table(control::INSTALL_OPERATIONS)
        .map_err(storage)?
        .get(operation_bytes)
        .map_err(storage)?
        .is_some()
        || tx
            .open_table(capacity::CAPACITY_OPERATIONS)
            .map_err(storage)?
            .get(operation_bytes)
            .map_err(storage)?
            .is_some()
    {
        return Err(Status::failed_precondition(
            "command_id was already used by a control install or capacity command",
        ));
    }
    Ok(())
}

/// The retained decision of an exact retry; a reused command_id with other
/// content is refused.
fn retained_decision(
    decisions: &RowTable<'_>,
    resolved: &Resolved<'_>,
    command: &SourceAuthorityCommand,
) -> Result<Option<SourceAuthorityDecision>, Status> {
    let Some(saved) = decisions
        .get(resolved.operation_bytes.as_slice())
        .map_err(storage)?
    else {
        return Ok(None);
    };
    let operation: SourceAuthorityOperation = contract::decode(saved.value())?;
    recovery::operation(
        &resolved.operation_key,
        &operation,
        &resolved.header,
        &resolved.policy,
    )?;
    if operation.request_sha256 != resolved.request_sha256
        || operation.command.as_ref() != Some(command)
    {
        return Err(Status::failed_precondition(
            "source authority command_id was already used with different content",
        ));
    }
    operation
        .decision
        .ok_or_else(|| missing("retry decision"))
        .map(Some)
}

fn held_owner(owners: &RowTable<'_>, key_bytes: &[u8]) -> Result<HeldOwner, Status> {
    let bytes = owners
        .get(key_bytes)
        .map_err(storage)?
        .map(|value| value.value().to_vec());
    let row: Option<PreparedSourceOwner> = bytes.as_deref().map(contract::decode).transpose()?;
    if let Some(owner) = &row {
        contract::owner(owner).map_err(corrupt)?;
    }
    Ok(HeldOwner { bytes, row })
}

/// Admission of a readiness confirmation: the adapter's held binding must be
/// for exactly this pending owner and the command must carry that binding's
/// completion. Nothing here is recorded; a replica applying the committed
/// command performs no such check.
fn admit_readiness(
    command: &SourceAuthorityCommand,
    verified: OwnerAdmission<'_>,
    before: Option<&PreparedSourceOwner>,
) -> Result<(), Status> {
    let Some(Action::ConfirmReady(request)) = command.action.as_ref() else {
        return Ok(());
    };
    match verified {
        OwnerAdmission::Holder(verified) => {
            if verified.binding.preparation.as_ref() != before {
                return Err(Status::failed_precondition(
                    "managed binding preparation differs from the committed owner",
                ));
            }
            if request.completion.as_ref() != Some(&verified.completion) {
                return Err(Status::permission_denied(
                    "readiness confirmation differs from the held binding's completion",
                ));
            }
            Ok(())
        }
        OwnerAdmission::None => Err(Status::permission_denied(
            "readiness confirmation requires the hosting adapter with the managed binding",
        )),
        OwnerAdmission::Committed => Ok(()),
    }
}

/// Encode the retry record of `decision` and account for it in the header.
fn record_decision(
    resolved: &mut Resolved<'_>,
    command: &SourceAuthorityCommand,
    decision: &SourceAuthorityDecision,
) -> Result<Vec<u8>, Status> {
    let operation = SourceAuthorityOperation {
        format_version: 1,
        request_sha256: std::mem::take(&mut resolved.request_sha256),
        command: Some(command.clone()),
        decision: Some(decision.clone()),
    };
    let encoded = operation.encode_to_vec();
    if encoded.len() > contract::MAX_RECORD_BYTES {
        return Err(Status::resource_exhausted(
            "source authority decision record exceeds 2MiB",
        ));
    }
    let header = &mut resolved.header;
    header.payload_bytes = payload_change(
        header.payload_bytes,
        0,
        resolved.operation_bytes.len() + encoded.len(),
    )?;
    header.decision_count = header
        .decision_count
        .checked_add(1)
        .ok_or_else(|| corrupt("decision counter overflow"))?;
    Ok(encoded)
}

/// Write an accepted owner row and account for it in the header.
fn store_owner(
    resolved: &mut Resolved<'_>,
    owners: &mut RowTable<'_>,
    key_bytes: &[u8],
    held: &HeldOwner,
    owner: &PreparedSourceOwner,
) -> Result<(), Status> {
    let header = &mut resolved.header;
    let encoded_owner = owner.encode_to_vec();
    if held.bytes.is_none() {
        if header.owner_count >= resolved.limits.max_owners {
            return Err(Status::resource_exhausted(
                "source authority owner capacity is full",
            ));
        }
        header.owner_count = header
            .owner_count
            .checked_add(1)
            .ok_or_else(|| corrupt("owner counter overflow"))?;
        header.payload_bytes = payload_change(header.payload_bytes, 0, key_bytes.len())?;
    }
    header.payload_bytes = payload_change(
        header.payload_bytes,
        held.bytes.as_ref().map_or(0, Vec::len),
        encoded_owner.len(),
    )?;
    owners
        .insert(key_bytes, encoded_owner.as_slice())
        .map_err(storage)?;
    Ok(())
}

/// Reserve the workflow of an accepted preparation for its generation.
fn reserve_workflow(
    resolved: &mut Resolved<'_>,
    workflows: &mut RowTable<'_>,
    workflow_bytes: &Option<Vec<u8>>,
    ownership_generation: u64,
) -> Result<(), Status> {
    let header = &mut resolved.header;
    let workflow = SourceAuthorityWorkflow {
        preparation_command: Some(resolved.operation_key.clone()),
        ownership_generation,
    };
    let bytes = workflow.encode_to_vec();
    let workflow_key = workflow_bytes
        .as_ref()
        .ok_or_else(|| corrupt("accepted preparation has no workflow key"))?;
    header.payload_bytes =
        payload_change(header.payload_bytes, 0, workflow_key.len() + bytes.len())?;
    header.workflow_count = header
        .workflow_count
        .checked_add(1)
        .ok_or_else(|| corrupt("workflow counter overflow"))?;
    workflows
        .insert(workflow_key.as_slice(), bytes.as_slice())
        .map_err(storage)?;
    Ok(())
}

/// Write a replaced policy and account for its size change in the header.
fn store_policy(
    resolved: &mut Resolved<'_>,
    meta: &mut MetaTable<'_>,
    updated: &AccessPolicy,
) -> Result<(), Status> {
    let bytes = updated.encode_to_vec();
    resolved.header.payload_bytes = payload_change(
        resolved.header.payload_bytes,
        resolved.policy.encoded_len(),
        bytes.len(),
    )?;
    meta.insert("policy", bytes.as_slice()).map_err(storage)?;
    Ok(())
}

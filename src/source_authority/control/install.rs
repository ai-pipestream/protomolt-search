//! The control install command: a retained, retryable decision that writes
//! a collection's control rows once.
use super::rows::{rows, write_rows};
use super::validation::validate_command;
use super::*;

/// The outcome of applying an install command inside its transaction.
enum Installed {
    /// An exact retry: the retained decision, with no application change.
    Retry(ControlInstallDecision),
    /// A newly recorded decision, published after the commit.
    Recorded(ControlInstallDecision),
}

/// One install's decision, the bytes its rows added and, when accepted,
/// the revision and row counts it installed.
struct Decided {
    decision: ControlInstallDecision,
    added_bytes: usize,
    installed: Option<(u64, ControlStoreHeader)>,
}

/// The rows an accepted install wrote.
struct Written {
    delta: ControlStoreHeader,
    receipt: ControlInstallReceipt,
    bytes: usize,
}

impl SourceAuthorityStore {
    /// Install a collection's control state on the direct path.
    pub fn install_control(
        &self,
        principal: &str,
        command: &ControlInstallCommand,
    ) -> Result<ControlInstallDecision, Status> {
        self.direct()?;
        self.replay_control_install(principal, command)
    }

    /// The committed-log path of `install_control`: no direct-path check.
    pub(crate) fn replay_control_install(
        &self,
        principal: &str,
        command: &ControlInstallCommand,
    ) -> Result<ControlInstallDecision, Status> {
        let _exclusive = self.exclusive()?;
        self.guarded(|| {
            let decision = self.install_locked(principal, command)?;
            let tx = self.inner.database().begin_read().map_err(storage)?;
            self.read_policy(
                &tx,
                principal,
                command.key.as_ref().ok_or_else(|| missing("command key"))?,
            )?;
            Ok(decision)
        })
    }

    fn install_locked(
        &self,
        principal: &str,
        command: &ControlInstallCommand,
    ) -> Result<ControlInstallDecision, Status> {
        let mut tx = self.inner.database().begin_write().map_err(storage)?;
        tx.set_durability(Durability::Immediate).map_err(storage)?;
        let decision = match self.apply_install(&tx, principal, command)? {
            Installed::Retry(decision) => {
                self.finish_retry(tx)?;
                return Ok(decision);
            }
            Installed::Recorded(decision) => decision,
        };
        #[cfg(any(test, feature = "fault-injection"))]
        self.inject(false)?;
        tx.commit().map_err(storage)?;
        self.publish_applied(decision.control_revision);
        #[cfg(any(test, feature = "fault-injection"))]
        self.inject(true)?;
        Ok(decision)
    }

    /// Apply `command` to the tables of `tx` without committing it.
    fn apply_install(
        &self,
        tx: &redb::WriteTransaction,
        principal: &str,
        command: &ControlInstallCommand,
    ) -> Result<Installed, Status> {
        let mut meta = tx.open_table(META).map_err(storage)?;
        let (mut header, policy) = read_headers(&meta, &self.inner.identity)?;
        validate_command(command, &self.inner.identity, &header.limits)?;
        let key = command.key.as_ref().expect("validated key");
        contract::authorize(&policy, principal, key)?;
        let operation_key = contract::operation_key(principal, key, &command.command_id);
        let operation_bytes = operation_key.encode_to_vec();
        let request_sha256 = command_digest(command);
        refuse_other_namespaces(tx, &operation_bytes)?;
        let mut operations = tx.open_table(INSTALL_OPERATIONS).map_err(storage)?;
        if let Some(decision) =
            retained_decision(&operations, &operation_bytes, &request_sha256, command)?
        {
            // Under Raft the entry is consumed: the applied position
            // advances for an exact retry too, with no other change.
            self.write_pending_applied(&mut meta)?;
            return Ok(Installed::Retry(decision));
        }
        if header
            .source
            .decision_count
            .saturating_add(retained_decisions(&meta)?)
            >= header.limits.max_decisions
        {
            return Err(Status::resource_exhausted(
                "source authority decision capacity is full; retry history is never evicted",
            ));
        }
        let Decided {
            decision,
            mut added_bytes,
            installed,
        } = decide(tx, command, &header, &policy)?;
        let operation = ControlInstallOperation {
            format_version: 1,
            request_sha256,
            command: Some(command.clone()),
            decision: Some(decision.clone()),
        };
        let encoded = operation.encode_to_vec();
        if encoded.len() > contract::MAX_RECORD_BYTES {
            return Err(Status::resource_exhausted(
                "control install decision record exceeds 2MiB",
            ));
        }
        added_bytes += operation_bytes.len() + encoded.len();
        account(&mut header, added_bytes, installed)?;
        operations
            .insert(operation_bytes.as_slice(), encoded.as_slice())
            .map_err(storage)?;
        meta.insert("header", header.source.encode_to_vec().as_slice())
            .map_err(storage)?;
        self.write_pending_applied(&mut meta)?;
        meta.insert(CONTROL_HEADER, header.control.encode_to_vec().as_slice())
            .map_err(storage)?;
        Ok(Installed::Recorded(decision))
    }

    /// A stored install decision under this actor's key.
    pub fn control_install_decision(
        &self,
        principal: &str,
        key: &LogicalSourceOwner,
        command_id: &[u8],
    ) -> Result<ControlInstallDecision, Status> {
        self.guarded(|| {
            contract::operation_id(command_id)?;
            let tx = self.inner.database().begin_read().map_err(storage)?;
            self.read_policy(&tx, principal, key)?;
            let operations = tx.open_table(INSTALL_OPERATIONS).map_err(storage)?;
            let bytes = contract::operation_key(principal, key, command_id).encode_to_vec();
            let operation: ControlInstallOperation = contract::decode(
                operations
                    .get(bytes.as_slice())
                    .map_err(storage)?
                    .ok_or_else(|| Status::not_found("control install command has no decision"))?
                    .value(),
            )?;
            operation.decision.ok_or_else(|| missing("decision"))
        })
    }
}

/// A command_id recorded by a regular command or a capacity command never
/// names a control install.
fn refuse_other_namespaces(
    tx: &redb::WriteTransaction,
    operation_bytes: &[u8],
) -> Result<(), Status> {
    if tx
        .open_table(DECISIONS)
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
            "command_id was already used by another source authority command",
        ));
    }
    Ok(())
}

/// The retained decision of an exact retry; a reused command_id with other
/// content is refused.
fn retained_decision(
    operations: &redb::Table<'_, &'static [u8], &'static [u8]>,
    operation_bytes: &[u8],
    request_sha256: &[u8],
    command: &ControlInstallCommand,
) -> Result<Option<ControlInstallDecision>, Status> {
    let Some(saved) = operations.get(operation_bytes).map_err(storage)? else {
        return Ok(None);
    };
    let operation: ControlInstallOperation = contract::decode(saved.value())?;
    if operation.request_sha256 != request_sha256 || operation.command.as_ref() != Some(command) {
        return Err(Status::failed_precondition(
            "control install command_id was already used with different content",
        ));
    }
    operation
        .decision
        .ok_or_else(|| missing("retry decision"))
        .map(Some)
}

/// Decide one install against the committed revisions and rows.
fn decide(
    tx: &redb::WriteTransaction,
    command: &ControlInstallCommand,
    header: &Header,
    policy: &AccessPolicy,
) -> Result<Decided, Status> {
    let revision = header
        .source
        .control_revision
        .checked_add(1)
        .ok_or_else(|| Status::resource_exhausted("source authority control revision exhausted"))?;
    let mut decision = ControlInstallDecision {
        format_version: 1,
        control_revision: header.source.control_revision,
        policy_revision: policy.revision,
        ..Default::default()
    };
    let mut added_bytes = 0usize;
    let delta = match install_rows(tx, command, header, policy, revision) {
        Ok(written) => {
            added_bytes += written.bytes;
            decision.receipt = Some(written.receipt);
            decision.control_revision = revision;
            Some(written.delta)
        }
        Err(error)
            if matches!(
                error.code(),
                Code::FailedPrecondition | Code::AlreadyExists | Code::ResourceExhausted
            ) =>
        {
            // A recorded rejection: durable, deterministic, unchanged state.
            decision.code = error.code() as u32;
            decision.message = error.message().to_string();
            None
        }
        Err(error) => return Err(error),
    };
    Ok(Decided {
        decision,
        added_bytes,
        installed: delta.map(|delta| (revision, delta)),
    })
}

/// Check the command against the committed revisions and the resource's
/// current state, then write its rows. A refusal with a precondition,
/// existence or capacity status is recorded as the command's decision.
fn install_rows(
    tx: &redb::WriteTransaction,
    command: &ControlInstallCommand,
    header: &Header,
    policy: &AccessPolicy,
    revision: u64,
) -> Result<Written, Status> {
    let key = command.key.as_ref().expect("validated key");
    let state = command.state.as_ref().expect("validated state");
    if command.expected_control_revision != header.source.control_revision {
        return Err(reject(
            Code::FailedPrecondition,
            "source authority control revision differs",
        ));
    }
    if command.expected_policy_revision != policy.revision {
        return Err(reject(
            Code::FailedPrecondition,
            "source authority policy revision differs",
        ));
    }
    if tx
        .open_table(CONTROL)
        .map_err(storage)?
        .get(key.encode_to_vec().as_slice())
        .map_err(storage)?
        .is_some()
    {
        return Err(reject(
            Code::AlreadyExists,
            "resource already holds installed control state",
        ));
    }
    let rows = rows(key, state, revision)?;
    let new_bytes: usize = rows.iter().map(|(k, v)| k.len() + v.len()).sum();
    let bytes_left = header
        .limits
        .max_payload_bytes
        .saturating_sub(header.source.payload_bytes);
    if new_bytes as u64 > bytes_left {
        return Err(reject(
            Code::ResourceExhausted,
            format!("installed control state needs {new_bytes} bytes; {bytes_left} remain"),
        ));
    }
    let delta = write_rows(tx, &rows)?;
    Ok(Written {
        delta,
        receipt: rows.receipt,
        bytes: new_bytes,
    })
}

/// Account for the decision record, and for the installed rows of an
/// accepted install, in the source and control headers.
fn account(
    header: &mut Header,
    added_bytes: usize,
    installed: Option<(u64, ControlStoreHeader)>,
) -> Result<(), Status> {
    header.source.payload_bytes = payload_change(header.source.payload_bytes, 0, added_bytes)?;
    if header.source.payload_bytes > header.limits.max_payload_bytes {
        return Err(Status::resource_exhausted(
            "source authority payload capacity is full; retry history is never evicted",
        ));
    }
    let c = &mut header.control;
    c.install_decision_count = c
        .install_decision_count
        .checked_add(1)
        .ok_or_else(|| corrupt("install decision counter overflow"))?;
    if let Some((revision, delta)) = installed {
        header.source.control_revision = revision;
        c.control_count += delta.control_count;
        c.topology_count += delta.topology_count;
        c.node_count += delta.node_count;
        c.replica_count += delta.replica_count;
        c.action_count += delta.action_count;
        c.completed_count += delta.completed_count;
    }
    Ok(())
}

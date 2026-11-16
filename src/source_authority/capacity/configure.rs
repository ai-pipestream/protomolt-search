//! The capacity configure command: a retained, retryable control decision.
use super::validation::validate_configure;
use super::*;

type RowTable<'txn> = redb::Table<'txn, &'static [u8], &'static [u8]>;

/// The outcome of applying a configure command inside its transaction.
enum Configured {
    /// An exact retry: the retained decision, with no application change.
    Retry(CapacityConfigureDecision),
    /// A newly recorded decision, published after the commit.
    Recorded(CapacityConfigureDecision),
}

/// The committed inputs one configure command resolves against.
struct Resolved<'a> {
    key: &'a LogicalSourceOwner,
    header: control::Header,
    capacity_header: CapacityStoreHeader,
    policy: AccessPolicy,
    operation_bytes: Vec<u8>,
    request_sha256: Vec<u8>,
}

impl SourceAuthorityStore {
    /// Commit the planner configuration of one resource: a control command
    /// with a retained decision, current Admin, revision CAS. The cohort
    /// cannot change while observations are retained; bounds cannot drop
    /// below what is held.
    pub fn configure_capacity(
        &self,
        principal: &str,
        command: &CapacityConfigureCommand,
    ) -> Result<CapacityConfigureDecision, Status> {
        self.direct()?;
        self.replay_capacity_configure(principal, command)
    }

    /// The committed-log path of `configure_capacity`: no direct-path check.
    pub(crate) fn replay_capacity_configure(
        &self,
        principal: &str,
        command: &CapacityConfigureCommand,
    ) -> Result<CapacityConfigureDecision, Status> {
        let _exclusive = self.exclusive()?;
        self.guarded(|| {
            let decision = self.configure_locked(principal, command)?;
            let tx = self.inner.database().begin_read().map_err(storage)?;
            self.read_policy(
                &tx,
                principal,
                command.key.as_ref().ok_or_else(|| missing("command key"))?,
            )?;
            Ok(decision)
        })
    }

    fn configure_locked(
        &self,
        principal: &str,
        command: &CapacityConfigureCommand,
    ) -> Result<CapacityConfigureDecision, Status> {
        let mut tx = self.inner.database().begin_write().map_err(storage)?;
        tx.set_durability(Durability::Immediate).map_err(storage)?;
        let decision = match self.apply_configure(&tx, principal, command)? {
            Configured::Retry(decision) => {
                self.finish_retry(tx)?;
                return Ok(decision);
            }
            Configured::Recorded(decision) => decision,
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
    fn apply_configure(
        &self,
        tx: &redb::WriteTransaction,
        principal: &str,
        command: &CapacityConfigureCommand,
    ) -> Result<Configured, Status> {
        let mut meta = tx.open_table(META).map_err(storage)?;
        let mut resolved = self.resolve_configure(&meta, principal, command)?;
        refuse_other_namespaces(tx, &resolved.operation_bytes)?;
        let mut operations = tx.open_table(CAPACITY_OPERATIONS).map_err(storage)?;
        if let Some(decision) = retained_decision(&operations, &resolved, command)? {
            // Under Raft the entry is consumed: the applied position
            // advances for an exact retry too, with no other change.
            self.write_pending_applied(&mut meta)?;
            return Ok(Configured::Retry(decision));
        }
        let retained = control::retained_decisions(&meta)?;
        let header = &resolved.header;
        if header.source.decision_count.saturating_add(retained) >= header.limits.max_decisions {
            return Err(Status::resource_exhausted(
                "source authority decision capacity is full; retry history is never evicted",
            ));
        }
        let mut states = tx.open_table(CAPACITY).map_err(storage)?;
        let state_bytes = state_key(resolved.key);
        let existing: Option<CapacityState> = states
            .get(state_bytes.as_slice())
            .map_err(storage)?
            .map(|v| contract::decode(v.value()))
            .transpose()?;
        let revision = header
            .source
            .control_revision
            .checked_add(1)
            .ok_or_else(|| {
                Status::resource_exhausted("source authority control revision exhausted")
            })?;
        let mut decision = CapacityConfigureDecision {
            format_version: 1,
            control_revision: header.source.control_revision,
            policy_revision: resolved.policy.revision,
            observation_epoch: existing.as_ref().map_or(0, |s| s.observation_epoch),
            ..Default::default()
        };
        let mut state_change = None;
        match next_state(tx, command, &resolved, existing.as_ref(), revision) {
            Ok(next) => {
                decision.control_revision = revision;
                state_change = Some(next);
            }
            Err(error) => {
                decision.code = error.code() as u32;
                decision.message = error.message().to_string();
            }
        }
        let encoded = record_decision(&mut resolved, command, &decision)?;
        if let Some(next) = state_change {
            store_state(&mut resolved, &mut states, &state_bytes, &next)?;
            resolved.header.source.control_revision = revision;
        }
        let header = &resolved.header;
        if header.source.payload_bytes > header.limits.max_payload_bytes {
            return Err(Status::resource_exhausted(
                "source authority payload capacity is full; retry history is never evicted",
            ));
        }
        operations
            .insert(resolved.operation_bytes.as_slice(), encoded.as_slice())
            .map_err(storage)?;
        meta.insert("header", header.source.encode_to_vec().as_slice())
            .map_err(storage)?;
        meta.insert(
            CAPACITY_HEADER,
            resolved.capacity_header.encode_to_vec().as_slice(),
        )
        .map_err(storage)?;
        self.write_pending_applied(&mut meta)?;
        Ok(Configured::Recorded(decision))
    }

    /// Read the committed headers and policy, validate the command against
    /// them and authorize the actor on the command's resource.
    fn resolve_configure<'a>(
        &self,
        meta: &impl ReadableTable<&'static str, &'static [u8]>,
        principal: &str,
        command: &'a CapacityConfigureCommand,
    ) -> Result<Resolved<'a>, Status> {
        let (header, policy) = read_headers(meta, &self.inner.identity)?;
        let capacity_header = header_of(meta)?;
        validate_configure(command, &self.inner.identity, &header.limits)?;
        let key = command.key.as_ref().expect("validated key");
        contract::authorize(&policy, principal, key)?;
        let operation_key = contract::operation_key(principal, key, &command.command_id);
        Ok(Resolved {
            key,
            header,
            capacity_header,
            policy,
            operation_bytes: operation_key.encode_to_vec(),
            request_sha256: command_digest(command),
        })
    }
}

/// A command_id recorded by a regular command or a control install never
/// names a capacity command.
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
            .open_table(control::INSTALL_OPERATIONS)
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
    operations: &RowTable<'_>,
    resolved: &Resolved<'_>,
    command: &CapacityConfigureCommand,
) -> Result<Option<CapacityConfigureDecision>, Status> {
    let Some(saved) = operations
        .get(resolved.operation_bytes.as_slice())
        .map_err(storage)?
    else {
        return Ok(None);
    };
    let operation: CapacityConfigureOperation = contract::decode(saved.value())?;
    if operation.request_sha256 != resolved.request_sha256
        || operation.command.as_ref() != Some(command)
    {
        return Err(Status::failed_precondition(
            "capacity command_id was already used with different content",
        ));
    }
    operation
        .decision
        .ok_or_else(|| missing("retry decision"))
        .map(Some)
}

/// The state an accepted configure writes. Every refusal here is recorded
/// as the command's decision with unchanged state.
fn next_state(
    tx: &redb::WriteTransaction,
    command: &CapacityConfigureCommand,
    resolved: &Resolved<'_>,
    existing: Option<&CapacityState>,
    revision: u64,
) -> Result<CapacityState, Status> {
    let (header, key) = (&resolved.header, resolved.key);
    if command.expected_control_revision != header.source.control_revision {
        return Err(reject(
            Code::FailedPrecondition,
            "source authority control revision differs",
        ));
    }
    if command.expected_policy_revision != resolved.policy.revision {
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
        .is_none()
    {
        return Err(reject(
            Code::FailedPrecondition,
            "resource has no applied control state to plan over",
        ));
    }
    let configuration = command
        .configuration
        .clone()
        .expect("validated configuration");
    let mut next = existing.cloned().unwrap_or(CapacityState {
        format_version: 1,
        key: Some(key.clone()),
        ..Default::default()
    });
    if let Some(current) = existing {
        let held = current
            .configuration
            .as_ref()
            .ok_or_else(|| missing("configuration"))?;
        if current.observation_count > 0
            && (held.cohort_length_ms != configuration.cohort_length_ms
                || held.cohort_phase_unix_ms != configuration.cohort_phase_unix_ms)
        {
            return Err(reject(
                Code::FailedPrecondition,
                "cohort cannot change while observations are retained; commit an expiry first",
            ));
        }
        if current.observation_count > configuration.max_total_records
            || current.reporter_count > configuration.max_registered_nodes
        {
            return Err(reject(
                Code::FailedPrecondition,
                "configuration bounds are below the retained reporters or observations",
            ));
        }
    }
    next.configuration = Some(configuration);
    next.configured_control_revision = revision;
    Ok(next)
}

/// Encode the retry record of `decision` and account for it in the headers.
fn record_decision(
    resolved: &mut Resolved<'_>,
    command: &CapacityConfigureCommand,
    decision: &CapacityConfigureDecision,
) -> Result<Vec<u8>, Status> {
    let operation = CapacityConfigureOperation {
        format_version: 1,
        request_sha256: std::mem::take(&mut resolved.request_sha256),
        command: Some(command.clone()),
        decision: Some(decision.clone()),
    };
    let encoded = operation.encode_to_vec();
    if encoded.len() > contract::MAX_RECORD_BYTES {
        return Err(Status::resource_exhausted(
            "capacity decision record exceeds 2MiB",
        ));
    }
    resolved.header.source.payload_bytes = payload_change(
        resolved.header.source.payload_bytes,
        0,
        resolved.operation_bytes.len() + encoded.len(),
    )?;
    resolved.capacity_header.configure_decision_count = resolved
        .capacity_header
        .configure_decision_count
        .checked_add(1)
        .ok_or_else(|| corrupt("capacity decision counter overflow"))?;
    Ok(encoded)
}

/// Write an accepted capacity state and account for it in the headers.
fn store_state(
    resolved: &mut Resolved<'_>,
    states: &mut RowTable<'_>,
    state_bytes: &[u8],
    next: &CapacityState,
) -> Result<(), Status> {
    let source = &mut resolved.header.source;
    let bytes = next.encode_to_vec();
    let old = states
        .get(state_bytes)
        .map_err(storage)?
        .map_or(0, |v| v.value().len());
    if old == 0 {
        resolved.capacity_header.state_count = resolved
            .capacity_header
            .state_count
            .checked_add(1)
            .ok_or_else(|| corrupt("capacity state counter overflow"))?;
        source.payload_bytes = payload_change(source.payload_bytes, 0, state_bytes.len())?;
    }
    source.payload_bytes = payload_change(source.payload_bytes, old, bytes.len())?;
    states
        .insert(state_bytes, bytes.as_slice())
        .map_err(storage)?;
    Ok(())
}

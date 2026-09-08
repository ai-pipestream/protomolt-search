//! Bounded staged import of a retired legacy control authority, and the
//! unified collection control state it applies (docs/control-import.md).
//!
//! Every step is an actor-scoped, retryable command in one durable transaction.
//! Chunk bytes live once, in the chunk command's own retained retry record;
//! the staging table holds references. Nothing here activates an owner,
//! publishes a map or reconciles an imported action.
use super::*;
use crate::control_plane::{LegacyControlCheckpoint, RetiredLegacyControl};
use control_import_command::Action as ImportAction;
use std::collections::BTreeSet;

pub(super) const CONTROL_HEADER: &str = "control";
pub(super) const STORE_FORMAT: u32 = 2;
pub(super) const IMPORT_OPERATIONS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("source_authority_import_operations");
pub(super) const IMPORTS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("source_authority_imports");
pub(super) const CHUNKS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("source_authority_import_chunks");
pub(super) const CONTROL: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("source_authority_control");
pub(super) const TOPOLOGIES: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("source_authority_control_topologies");
pub(super) const NODES: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("source_authority_control_nodes");
pub(super) const REPLICAS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("source_authority_control_replicas");
pub(super) const ACTIONS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("source_authority_control_actions");
pub(super) const COMPLETED: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("source_authority_control_completed");
pub(super) const FORMAT_2_TABLES: usize = 13;

/// The retirement frame's 17 MiB plus a 1 MiB supplement.
pub const MAX_IMPORT_PAYLOAD_BYTES: u64 = 18 * 1024 * 1024;
pub const MAX_IMPORT_CHUNKS: u32 = 4096;
pub const MAX_SUPPLEMENT_BYTES: usize = 1024 * 1024;
/// A configuration whose chunks could carry less than this refuses at Begin.
pub const MIN_CHUNK_CAPACITY: usize = 4096;
/// Bytes held for each of the two terminal records (Commit, Abort).
const TERMINAL_RESERVE_BYTES: u64 = 64 * 1024;
const TERMINAL_RESERVE_DECISIONS: u64 = 2;
/// Encoding slack for a chunk record beyond its command bytes.
const RECORD_ENVELOPE: u64 = 256;
/// Applied rows are re-encoded from the checkpoint; twice its size bounds them.
const APPLIED_STATE_FACTOR: u64 = 2;

const PAYLOAD_DOMAIN: &[u8] = b"protomolt.control-import.payload.v1\0";
const CHUNK_DOMAIN: &[u8] = b"protomolt.control-import.chunk.v1\0";
const COMMAND_DOMAIN: &[u8] = b"protomolt.control-import.command.v1\0";
const SNAPSHOT_DOMAIN: &[u8] = b"protomolt.control-snapshot.v1\0";

fn hash(domain: &[u8], bytes: &[u8]) -> Vec<u8> {
    let mut hasher = crate::sha256::Sha256::new();
    hasher.update(domain);
    hasher.update(bytes);
    hasher.finalize().to_vec()
}

pub(super) fn command_digest(command: &ControlImportCommand) -> Vec<u8> {
    hash(COMMAND_DOMAIN, &command.encode_to_vec())
}

/// Digest of the staged payload bytes, bound at Begin.
pub fn payload_digest(bytes: &[u8]) -> Vec<u8> {
    hash(PAYLOAD_DOMAIN, bytes)
}

/// Digest of one chunk's bytes.
pub fn chunk_digest(bytes: &[u8]) -> Vec<u8> {
    hash(CHUNK_DOMAIN, bytes)
}

/// SHA-256 over the canonical retirement payload: the bytes its frame checksums.
pub fn retirement_digest(record: &LegacyControlRetirement) -> Vec<u8> {
    crate::sha256::digest(&record.encode_to_vec()).to_vec()
}

fn workflow_key(key: &LogicalSourceOwner, workflow_id: &[u8]) -> Vec<u8> {
    SourceAuthorityWorkflowKey {
        key: Some(key.clone()),
        workflow_id: workflow_id.to_vec(),
    }
    .encode_to_vec()
}

fn chunk_key(key: &LogicalSourceOwner, ordinal: u32) -> Vec<u8> {
    ControlImportChunkKey {
        key: Some(key.clone()),
        ordinal,
    }
    .encode_to_vec()
}

fn resource_prefix(key: &LogicalSourceOwner) -> Vec<u8> {
    // Field 1 of every keyed record is the resource, so its encoding is the
    // byte prefix shared by every row of that resource.
    let mut prefix = Vec::new();
    prost::encoding::message::encode(1, key, &mut prefix);
    prefix
}

fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last < u8::MAX {
            end.push(last + 1);
            return Some(end);
        }
    }
    None
}

/// The chunk bytes one command can carry under `limits` for this actor and
/// resource: the configured command bound less the encoded envelope of a
/// chunk command with the largest ids and ordinal.
pub fn chunk_capacity(
    limits: &SourceAuthorityLimits,
    identity: &SourceAuthorityIdentity,
    key: &LogicalSourceOwner,
) -> Result<usize, Status> {
    let envelope = ControlImportCommand {
        format_version: 1,
        authority: Some(identity.clone()),
        key: Some(key.clone()),
        command_id: vec![0xff; 1024],
        expected_control_revision: u64::MAX,
        expected_policy_revision: u64::MAX,
        workflow_id: vec![0xff; 1024],
        action: Some(ImportAction::Chunk(ControlImportChunk {
            ordinal: u32::MAX,
            // Filled to the command bound so every length varint is at its
            // widest; a chunk of the returned capacity then always fits.
            bytes: vec![0xff; limits.max_command_bytes as usize],
            sha256: vec![0; 32],
        })),
    }
    .encoded_len()
        - limits.max_command_bytes as usize;
    let capacity = (limits.max_command_bytes as usize).saturating_sub(envelope);
    if capacity < MIN_CHUNK_CAPACITY {
        return Err(Status::failed_precondition(format!(
            "max_command_bytes {} leaves {capacity} bytes per import chunk after a {envelope}-byte \
             envelope; at least {MIN_CHUNK_CAPACITY} are required",
            limits.max_command_bytes
        )));
    }
    Ok(capacity)
}

/// Split a payload into the Begin declaration and its chunks under `limits`.
pub fn plan_chunks(
    limits: &SourceAuthorityLimits,
    identity: &SourceAuthorityIdentity,
    key: &LogicalSourceOwner,
    payload: &[u8],
) -> Result<(u32, u32), Status> {
    let capacity = chunk_capacity(limits, identity, key)?;
    if payload.is_empty() || payload.len() as u64 > MAX_IMPORT_PAYLOAD_BYTES {
        return Err(Status::invalid_argument(
            "import payload must be 1 byte to 18 MiB",
        ));
    }
    let chunk_bytes = capacity.min(payload.len());
    let count = payload.len().div_ceil(chunk_bytes);
    if count as u64 > u64::from(MAX_IMPORT_CHUNKS) {
        return Err(Status::resource_exhausted(
            "import payload needs more chunks than the 4096 bound",
        ));
    }
    Ok((chunk_bytes as u32, count as u32))
}

/// Validated admission of a retirement holder: what Begin binds.
pub(super) struct AdmittedRetirement {
    sha256: Vec<u8>,
    operation: SourceAuthorityOperationKey,
}

pub(super) fn admit_retirement(
    identity: &SourceAuthorityIdentity,
    principal: &str,
    key: &LogicalSourceOwner,
    retired: &RetiredLegacyControl,
) -> Result<AdmittedRetirement, Status> {
    let record = retired.record();
    if record.authority.as_ref() != Some(identity) {
        return Err(Status::failed_precondition(
            "retirement was made for another destination authority",
        ));
    }
    let operation = record
        .operation
        .clone()
        .ok_or_else(|| missing("retirement operation"))?;
    if operation.principal != principal || operation.key.as_ref() != Some(key) {
        return Err(Status::permission_denied(
            "retirement belongs to another actor or resource; import requires the retiring actor",
        ));
    }
    Ok(AdmittedRetirement {
        sha256: retirement_digest(record),
        operation,
    })
}

fn validate_command(
    command: &ControlImportCommand,
    identity: &SourceAuthorityIdentity,
    limits: &SourceAuthorityLimits,
) -> Result<(), Status> {
    if command.encoded_len() > limits.max_command_bytes as usize {
        return Err(Status::resource_exhausted(
            "control import command exceeds max_command_bytes",
        ));
    }
    if command.format_version != 1 {
        return Err(Status::invalid_argument(
            "control import command requires format 1",
        ));
    }
    if command.authority.as_ref() != Some(identity) {
        return Err(Status::failed_precondition(
            "control import authority group or incarnation differs",
        ));
    }
    let key = command
        .key
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("control import resource key is required"))?;
    contract::key(key, false)?;
    if !key.owner_id.is_empty() {
        return Err(Status::invalid_argument(
            "control import commands name a collection, not an owner",
        ));
    }
    contract::operation_id(&command.command_id)?;
    contract::workflow_id(&command.workflow_id)?;
    match command.action.as_ref() {
        None => Err(Status::invalid_argument(
            "control import action is missing or unsupported",
        )),
        Some(ImportAction::Begin(begin)) => {
            if begin.retirement_sha256.len() != 32
                || begin.payload_sha256.len() != 32
                || begin.payload_bytes == 0
                || begin.payload_bytes > MAX_IMPORT_PAYLOAD_BYTES
                || begin.chunk_bytes == 0
                || begin.chunk_count == 0
                || begin.chunk_count > MAX_IMPORT_CHUNKS
                || begin.payload_bytes.div_ceil(u64::from(begin.chunk_bytes))
                    != u64::from(begin.chunk_count)
            {
                return Err(Status::invalid_argument(
                    "begin import requires 32-byte digests, a 1..18 MiB payload and a chunk plan \
                     with chunk_count = ceil(payload_bytes / chunk_bytes) within 4096 chunks",
                ));
            }
            let operation = begin.retirement_operation.as_ref().ok_or_else(|| {
                Status::invalid_argument("begin import requires the retirement operation")
            })?;
            contract::principal(&operation.principal)?;
            contract::operation_id(&operation.command_id)?;
            if operation.format_version != 1 || operation.key.as_ref() != Some(key) {
                return Err(Status::invalid_argument(
                    "retirement operation must name this resource",
                ));
            }
            Ok(())
        }
        Some(ImportAction::Chunk(chunk)) => {
            if chunk.sha256.len() != 32 || chunk.bytes.is_empty() {
                return Err(Status::invalid_argument(
                    "import chunk requires bytes and a 32-byte digest",
                ));
            }
            Ok(())
        }
        Some(ImportAction::Commit(_)) | Some(ImportAction::Abort(_)) => Ok(()),
    }
}

fn reject(code: Code, message: impl Into<String>) -> Status {
    Status::new(code, message.into())
}

struct Header {
    source: SourceAuthorityHeader,
    control: ControlStoreHeader,
    limits: SourceAuthorityLimits,
}

fn read_headers(
    meta: &impl ReadableTable<&'static str, &'static [u8]>,
    identity: &SourceAuthorityIdentity,
) -> Result<(Header, AccessPolicy), Status> {
    let source: SourceAuthorityHeader = contract::decode(
        meta.get("header")
            .map_err(storage)?
            .ok_or_else(|| missing("header"))?
            .value(),
    )?;
    recovery::header(&source, identity)?;
    let control: ControlStoreHeader = contract::decode(
        meta.get(CONTROL_HEADER)
            .map_err(storage)?
            .ok_or_else(|| missing("control header"))?
            .value(),
    )?;
    recovery::control_header(&control)?;
    let policy: AccessPolicy = contract::decode(
        meta.get("policy")
            .map_err(storage)?
            .ok_or_else(|| missing("policy"))?
            .value(),
    )?;
    contract::policy(&policy).map_err(corrupt)?;
    let limits = source.limits.ok_or_else(|| missing("limits"))?;
    Ok((
        Header {
            source,
            control,
            limits,
        },
        policy,
    ))
}

/// Capacity left for a new record after every pending reservation. `own`
/// is the reservation the executing workflow itself holds, which its own
/// step may spend.
fn remaining(header: &Header, own: (u64, u64)) -> Result<(u64, u64), Status> {
    let bytes = header
        .limits
        .max_payload_bytes
        .checked_sub(header.source.payload_bytes)
        .and_then(|n| n.checked_sub(header.control.reserved_bytes.saturating_sub(own.0)))
        .ok_or_else(|| corrupt("reservation exceeds payload accounting"))?;
    let decisions = header
        .limits
        .max_decisions
        .checked_sub(header.source.decision_count)
        .and_then(|n| n.checked_sub(header.control.import_decision_count))
        .and_then(|n| n.checked_sub(header.control.reserved_decisions.saturating_sub(own.1)))
        .ok_or_else(|| corrupt("reservation exceeds decision accounting"))?;
    Ok((bytes, decisions))
}

fn bounded_record(bytes: &[u8], what: &str) -> Result<(), Status> {
    if bytes.len() > contract::MAX_RECORD_BYTES {
        return Err(reject(
            Code::ResourceExhausted,
            format!("{what} exceeds the 2MiB record bound; the import cannot be represented"),
        ));
    }
    Ok(())
}

impl SourceAuthorityStore {
    /// Begin an import: the hosting adapter supplies the retirement holder,
    /// whose durable file fence is the proof; the command's digests are
    /// checked against it, never trusted from the caller.
    pub fn begin_control_import(
        &self,
        principal: &str,
        command: &ControlImportCommand,
        retired: &RetiredLegacyControl,
    ) -> Result<ControlImportDecision, Status> {
        let key = command
            .key
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("control import resource key is required"))?;
        if !matches!(command.action, Some(ImportAction::Begin(_))) {
            return Err(Status::invalid_argument(
                "begin_control_import takes a Begin command",
            ));
        }
        let admitted = admit_retirement(&self.inner.identity, principal, key, retired)?;
        self.import(principal, command, Some(admitted))
    }

    /// Chunk, Commit and Abort. A Begin here refuses: it needs the retirement
    /// holder through `begin_control_import`.
    pub fn execute_control_import(
        &self,
        principal: &str,
        command: &ControlImportCommand,
    ) -> Result<ControlImportDecision, Status> {
        if matches!(command.action, Some(ImportAction::Begin(_))) {
            return Err(Status::permission_denied(
                "begin import requires the hosting admission path with a retirement holder",
            ));
        }
        self.import(principal, command, None)
    }

    fn import(
        &self,
        principal: &str,
        command: &ControlImportCommand,
        admitted: Option<AdmittedRetirement>,
    ) -> Result<ControlImportDecision, Status> {
        let _exclusive = self.exclusive()?;
        self.guarded(|| {
            let decision = self.import_locked(principal, command, admitted)?;
            let tx = self.inner.database.begin_read().map_err(storage)?;
            self.read_policy(
                &tx,
                principal,
                command.key.as_ref().ok_or_else(|| missing("command key"))?,
            )?;
            Ok(decision)
        })
    }

    fn import_locked(
        &self,
        principal: &str,
        command: &ControlImportCommand,
        admitted: Option<AdmittedRetirement>,
    ) -> Result<ControlImportDecision, Status> {
        let mut tx = self.inner.database.begin_write().map_err(storage)?;
        tx.set_durability(Durability::Immediate).map_err(storage)?;
        let decision;
        {
            let mut meta = tx.open_table(META).map_err(storage)?;
            let (mut header, policy) = read_headers(&meta, &self.inner.identity)?;
            validate_command(command, &self.inner.identity, &header.limits)?;
            let key = command.key.as_ref().expect("validated key");
            contract::authorize(&policy, principal, key)?;
            let operation_key = contract::operation_key(principal, key, &command.command_id);
            let operation_bytes = operation_key.encode_to_vec();
            let request_sha256 = command_digest(command);
            let decisions = tx.open_table(DECISIONS).map_err(storage)?;
            if decisions
                .get(operation_bytes.as_slice())
                .map_err(storage)?
                .is_some()
            {
                return Err(Status::failed_precondition(
                    "command_id was already used by a source authority command",
                ));
            }
            let mut operations = tx.open_table(IMPORT_OPERATIONS).map_err(storage)?;
            if let Some(saved) = operations
                .get(operation_bytes.as_slice())
                .map_err(storage)?
            {
                let operation: ControlImportOperation = contract::decode(saved.value())?;
                if operation.request_sha256 != request_sha256
                    || operation.command.as_ref() != Some(command)
                {
                    return Err(Status::failed_precondition(
                        "control import command_id was already used with different content",
                    ));
                }
                return operation.decision.ok_or_else(|| missing("retry decision"));
            }
            let mut imports = tx.open_table(IMPORTS).map_err(storage)?;
            let workflow_bytes = workflow_key(key, &command.workflow_id);
            let mut workflow: Option<ControlImportWorkflow> = imports
                .get(workflow_bytes.as_slice())
                .map_err(storage)?
                .map(|v| contract::decode(v.value()))
                .transpose()?;
            let own = workflow
                .as_ref()
                .filter(|w| w.phase == ControlImportPhase::Staging as i32)
                .map_or((0, 0), |w| (w.reserved_bytes, w.reserved_decisions));
            let (bytes_left, decisions_left) = remaining(&header, own)?;
            if decisions_left == 0 {
                return Err(Status::resource_exhausted(
                    "source authority decision capacity is full; retry history is never evicted",
                ));
            }
            let mut chunks = tx.open_table(CHUNKS).map_err(storage)?;
            let mut result = ControlImportDecision {
                format_version: 1,
                control_revision: header.source.control_revision,
                policy_revision: policy.revision,
                ..Default::default()
            };
            let revision = header
                .source
                .control_revision
                .checked_add(1)
                .ok_or_else(|| {
                    Status::resource_exhausted("source authority control revision exhausted")
                })?;
            let mut removed_bytes = 0usize;
            let mut added_bytes = 0usize;
            let mut control_delta = ControlStoreHeader::default();
            let mut freed_reservation = (0u64, 0u64);
            let transition = (|| -> Result<(), Status> {
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
                match command.action.as_ref().expect("validated action") {
                    ImportAction::Begin(begin) => {
                        let admitted = admitted.as_ref().ok_or_else(|| {
                            Status::permission_denied(
                                "begin import requires the hosting admission path with a retirement holder",
                            )
                        })?;
                        if begin.retirement_sha256 != admitted.sha256
                            || begin.retirement_operation.as_ref() != Some(&admitted.operation)
                        {
                            return Err(Status::failed_precondition(
                                "begin import digests differ from the admitted retirement",
                            ));
                        }
                        if workflow.is_some() {
                            return Err(reject(
                                Code::AlreadyExists,
                                "import workflow id was already used for this resource",
                            ));
                        }
                        let control = tx.open_table(CONTROL).map_err(storage)?;
                        if control
                            .get(key.encode_to_vec().as_slice())
                            .map_err(storage)?
                            .is_some()
                        {
                            return Err(reject(
                                Code::AlreadyExists,
                                "resource already holds an applied control import",
                            ));
                        }
                        if pending_workflow(&imports, key)?.is_some() {
                            return Err(reject(
                                Code::FailedPrecondition,
                                "another import workflow is pending for this resource",
                            ));
                        }
                        let capacity = chunk_capacity(&header.limits, &self.inner.identity, key)?;
                        if begin.chunk_bytes as usize > capacity {
                            return Err(reject(
                                Code::InvalidArgument,
                                format!(
                                    "chunk_bytes {} exceeds the {capacity}-byte chunk capacity of this store",
                                    begin.chunk_bytes
                                ),
                            ));
                        }
                        let op_key_len = contract::operation_key(principal, key, &[0xff; 1024])
                            .encoded_len() as u64;
                        let per_chunk = u64::from(header.limits.max_command_bytes)
                            .checked_add(op_key_len)
                            .and_then(|n| n.checked_add(RECORD_ENVELOPE))
                            .ok_or_else(|| corrupt("reservation overflow"))?;
                        let reserved_bytes = per_chunk
                            .checked_mul(u64::from(begin.chunk_count))
                            .and_then(|n| {
                                n.checked_add(
                                    begin.payload_bytes.saturating_mul(APPLIED_STATE_FACTOR),
                                )
                            })
                            .and_then(|n| {
                                n.checked_add(TERMINAL_RESERVE_BYTES * TERMINAL_RESERVE_DECISIONS)
                            })
                            .ok_or_else(|| corrupt("reservation overflow"))?;
                        let reserved_decisions =
                            u64::from(begin.chunk_count) + TERMINAL_RESERVE_DECISIONS;
                        if reserved_decisions >= decisions_left || reserved_bytes >= bytes_left {
                            return Err(reject(
                                Code::ResourceExhausted,
                                format!(
                                    "import needs {reserved_decisions} decisions and {reserved_bytes} bytes of headroom; {decisions_left} decisions and {bytes_left} bytes remain"
                                ),
                            ));
                        }
                        let row = ControlImportWorkflow {
                            format_version: 1,
                            key: Some(key.clone()),
                            workflow_id: command.workflow_id.clone(),
                            principal: principal.into(),
                            begin: Some(operation_key.clone()),
                            declared: Some(begin.clone()),
                            phase: ControlImportPhase::Staging as i32,
                            begun_control_revision: revision,
                            begun_policy_revision: policy.revision,
                            staged_chunks: 0,
                            terminal: None,
                            reserved_bytes,
                            reserved_decisions,
                        };
                        let encoded = row.encode_to_vec();
                        added_bytes += workflow_bytes.len() + encoded.len();
                        imports
                            .insert(workflow_bytes.as_slice(), encoded.as_slice())
                            .map_err(storage)?;
                        control_delta.import_workflow_count = 1;
                        control_delta.reserved_bytes = reserved_bytes;
                        control_delta.reserved_decisions = reserved_decisions;
                        workflow = Some(row);
                    }
                    ImportAction::Chunk(chunk) => {
                        let row = staging(workflow.as_mut(), principal)?;
                        let declared = row.declared.as_ref().expect("validated declaration");
                        if chunk.ordinal >= declared.chunk_count {
                            return Err(reject(
                                Code::FailedPrecondition,
                                "chunk ordinal is outside the declared chunk count",
                            ));
                        }
                        let expected_len = if chunk.ordinal + 1 == declared.chunk_count {
                            let rest = declared.payload_bytes % u64::from(declared.chunk_bytes);
                            if rest == 0 {
                                u64::from(declared.chunk_bytes)
                            } else {
                                rest
                            }
                        } else {
                            u64::from(declared.chunk_bytes)
                        };
                        if chunk.bytes.len() as u64 != expected_len
                            || chunk.sha256 != chunk_digest(&chunk.bytes)
                        {
                            return Err(reject(
                                Code::FailedPrecondition,
                                "chunk length or digest differs from the declared plan",
                            ));
                        }
                        let chunk_bytes_key = chunk_key(key, chunk.ordinal);
                        if let Some(existing) =
                            chunks.get(chunk_bytes_key.as_slice()).map_err(storage)?
                        {
                            let existing: ControlImportChunkRef =
                                contract::decode(existing.value())?;
                            return Err(if existing.sha256 == chunk.sha256 {
                                reject(
                                    Code::AlreadyExists,
                                    "chunk ordinal is already staged with identical content",
                                )
                            } else {
                                reject(
                                    Code::FailedPrecondition,
                                    "chunk ordinal is already staged with different content",
                                )
                            });
                        }
                        let reference = ControlImportChunkRef {
                            format_version: 1,
                            command: Some(operation_key.clone()),
                            sha256: chunk.sha256.clone(),
                            length: chunk.bytes.len() as u32,
                        }
                        .encode_to_vec();
                        added_bytes += chunk_bytes_key.len() + reference.len();
                        chunks
                            .insert(chunk_bytes_key.as_slice(), reference.as_slice())
                            .map_err(storage)?;
                        control_delta.staged_chunk_count = 1;
                        let spent_bytes = u64::from(header.limits.max_command_bytes)
                            + contract::operation_key(principal, key, &[0xff; 1024]).encoded_len()
                                as u64
                            + RECORD_ENVELOPE;
                        row.reserved_bytes = row.reserved_bytes.saturating_sub(spent_bytes);
                        row.reserved_decisions = row.reserved_decisions.saturating_sub(1);
                        freed_reservation = (spent_bytes.min(own.0), 1u64.min(own.1));
                        row.staged_chunks += 1;
                    }
                    ImportAction::Commit(_) => {
                        let row = staging(workflow.as_mut(), principal)?;
                        let declared = row.declared.clone().expect("validated declaration");
                        if row.staged_chunks != declared.chunk_count {
                            return Err(reject(
                                Code::FailedPrecondition,
                                format!(
                                    "import has {} of {} chunks staged",
                                    row.staged_chunks, declared.chunk_count
                                ),
                            ));
                        }
                        let control = tx.open_table(CONTROL).map_err(storage)?;
                        if control
                            .get(key.encode_to_vec().as_slice())
                            .map_err(storage)?
                            .is_some()
                        {
                            return Err(reject(
                                Code::AlreadyExists,
                                "resource already holds an applied control import",
                            ));
                        }
                        drop(control);
                        // Assemble from the retained chunk commands, in order.
                        let mut payload = Vec::with_capacity(declared.payload_bytes as usize);
                        let mut chunk_rows = Vec::with_capacity(declared.chunk_count as usize);
                        for ordinal in 0..declared.chunk_count {
                            let chunk_bytes_key = chunk_key(key, ordinal);
                            let reference: ControlImportChunkRef = contract::decode(
                                chunks
                                    .get(chunk_bytes_key.as_slice())
                                    .map_err(storage)?
                                    .ok_or_else(|| corrupt("staged chunk reference is missing"))?
                                    .value(),
                            )?;
                            let command_key = reference
                                .command
                                .as_ref()
                                .ok_or_else(|| missing("chunk command reference"))?
                                .encode_to_vec();
                            let operation: ControlImportOperation = contract::decode(
                                operations
                                    .get(command_key.as_slice())
                                    .map_err(storage)?
                                    .ok_or_else(|| corrupt("staged chunk command is missing"))?
                                    .value(),
                            )?;
                            let bytes =
                                match operation.command.as_ref().and_then(|c| c.action.as_ref()) {
                                    Some(ImportAction::Chunk(chunk))
                                        if chunk.ordinal == ordinal =>
                                    {
                                        &chunk.bytes
                                    }
                                    _ => {
                                        return Err(corrupt(
                                            "staged chunk reference names a non-chunk command",
                                        ))
                                    }
                                };
                            if bytes.len() as u32 != reference.length
                                || chunk_digest(bytes) != reference.sha256
                            {
                                return Err(corrupt(
                                    "staged chunk bytes differ from their reference",
                                ));
                            }
                            payload.extend_from_slice(bytes);
                            chunk_rows.push((chunk_bytes_key, reference.encoded_len()));
                        }
                        if payload.len() as u64 != declared.payload_bytes
                            || payload_digest(&payload) != declared.payload_sha256
                        {
                            return Err(reject(
                                Code::FailedPrecondition,
                                "assembled payload length or digest differs from Begin",
                            ));
                        }
                        let applied =
                            apply(&self.inner.identity, key, &declared, &payload, revision)?;
                        let mut new_bytes = 0u64;
                        for (k, v) in applied.rows_iter() {
                            new_bytes += (k.len() + v.len()) as u64;
                        }
                        let freed: u64 = chunk_rows
                            .iter()
                            .map(|(k, len)| (k.len() + len) as u64)
                            .sum();
                        if new_bytes
                            .saturating_sub(freed)
                            .saturating_add(TERMINAL_RESERVE_BYTES)
                            > bytes_left
                        {
                            return Err(reject(
                                Code::ResourceExhausted,
                                format!(
                                    "applied control state needs {new_bytes} bytes; {bytes_left} remain"
                                ),
                            ));
                        }
                        let control_delta_applied = write_applied(&tx, &applied)?;
                        control_delta.control_count = control_delta_applied.control_count;
                        control_delta.topology_count = control_delta_applied.topology_count;
                        control_delta.node_count = control_delta_applied.node_count;
                        control_delta.replica_count = control_delta_applied.replica_count;
                        control_delta.action_count = control_delta_applied.action_count;
                        control_delta.completed_count = control_delta_applied.completed_count;
                        added_bytes += new_bytes as usize;
                        for (k, len) in &chunk_rows {
                            chunks.remove(k.as_slice()).map_err(storage)?;
                            removed_bytes += k.len() + len;
                        }
                        control_delta.staged_chunk_count = declared.chunk_count.into();
                        freed_reservation = (own.0, own.1);
                        row.phase = ControlImportPhase::Committed as i32;
                        row.terminal = Some(operation_key.clone());
                        row.reserved_bytes = 0;
                        row.reserved_decisions = 0;
                        result.receipt = Some(applied.receipt.clone());
                    }
                    ImportAction::Abort(_) => {
                        let row = staging(workflow.as_mut(), principal)?;
                        let declared = row.declared.clone().expect("validated declaration");
                        for ordinal in 0..declared.chunk_count {
                            let chunk_bytes_key = chunk_key(key, ordinal);
                            if let Some(existing) =
                                chunks.remove(chunk_bytes_key.as_slice()).map_err(storage)?
                            {
                                removed_bytes += chunk_bytes_key.len() + existing.value().len();
                                control_delta.staged_chunk_count += 1;
                            }
                        }
                        freed_reservation = (own.0, own.1);
                        row.phase = ControlImportPhase::Aborted as i32;
                        row.terminal = Some(operation_key.clone());
                        row.reserved_bytes = 0;
                        row.reserved_decisions = 0;
                    }
                }
                result.control_revision = revision;
                Ok(())
            })();
            let accepted = match transition {
                Ok(()) => true,
                Err(error)
                    if matches!(
                        error.code(),
                        Code::FailedPrecondition
                            | Code::AlreadyExists
                            | Code::InvalidArgument
                            | Code::ResourceExhausted
                            | Code::NotFound
                    ) && !error.message().starts_with("begin import")
                        && !error
                            .message()
                            .starts_with("source authority decision capacity") =>
                {
                    // A recorded rejection: durable, deterministic, unchanged state.
                    result.code = error.code() as u32;
                    result.message = error.message().to_string();
                    result.receipt = None;
                    result.control_revision = header.source.control_revision;
                    false
                }
                Err(error) => return Err(error),
            };
            if accepted {
                if let Some(row) = &workflow {
                    let encoded = row.encode_to_vec();
                    if !matches!(command.action, Some(ImportAction::Begin(_))) {
                        let previous = imports
                            .get(workflow_bytes.as_slice())
                            .map_err(storage)?
                            .map(|v| v.value().len())
                            .unwrap_or(0);
                        removed_bytes += previous;
                        added_bytes += encoded.len();
                    }
                    imports
                        .insert(workflow_bytes.as_slice(), encoded.as_slice())
                        .map_err(storage)?;
                }
            }
            let operation = ControlImportOperation {
                format_version: 1,
                request_sha256,
                command: Some(command.clone()),
                decision: Some(result.clone()),
            };
            let encoded = operation.encode_to_vec();
            if encoded.len() > contract::MAX_RECORD_BYTES {
                return Err(Status::resource_exhausted(
                    "control import decision record exceeds 2MiB",
                ));
            }
            added_bytes += operation_bytes.len() + encoded.len();
            header.source.payload_bytes =
                payload_change(header.source.payload_bytes, removed_bytes, added_bytes)?;
            header.control.import_decision_count = header
                .control
                .import_decision_count
                .checked_add(1)
                .ok_or_else(|| corrupt("import decision counter overflow"))?;
            if accepted {
                header.source.control_revision = revision;
                let c = &mut header.control;
                c.import_workflow_count += control_delta.import_workflow_count;
                c.control_count += control_delta.control_count;
                c.topology_count += control_delta.topology_count;
                c.node_count += control_delta.node_count;
                c.replica_count += control_delta.replica_count;
                c.action_count += control_delta.action_count;
                c.completed_count += control_delta.completed_count;
                match command.action.as_ref() {
                    Some(ImportAction::Chunk(_)) => c.staged_chunk_count += 1,
                    Some(ImportAction::Commit(_)) | Some(ImportAction::Abort(_)) => {
                        c.staged_chunk_count = c
                            .staged_chunk_count
                            .checked_sub(control_delta.staged_chunk_count)
                            .ok_or_else(|| corrupt("staged chunk accounting underflow"))?;
                    }
                    _ => {}
                }
                c.reserved_bytes = c
                    .reserved_bytes
                    .checked_add(control_delta.reserved_bytes)
                    .and_then(|n| n.checked_sub(freed_reservation.0))
                    .ok_or_else(|| corrupt("reservation accounting underflow"))?;
                c.reserved_decisions = c
                    .reserved_decisions
                    .checked_add(control_delta.reserved_decisions)
                    .and_then(|n| n.checked_sub(freed_reservation.1))
                    .ok_or_else(|| corrupt("reservation accounting underflow"))?;
            }
            if header.source.payload_bytes > header.limits.max_payload_bytes {
                return Err(Status::resource_exhausted(
                    "source authority payload capacity is full; retry history is never evicted",
                ));
            }
            operations
                .insert(operation_bytes.as_slice(), encoded.as_slice())
                .map_err(storage)?;
            meta.insert("header", header.source.encode_to_vec().as_slice())
                .map_err(storage)?;
            meta.insert(CONTROL_HEADER, header.control.encode_to_vec().as_slice())
                .map_err(storage)?;
            decision = result;
        }
        #[cfg(test)]
        self.inject(false)?;
        tx.commit().map_err(storage)?;
        #[cfg(test)]
        self.inject(true)?;
        Ok(decision)
    }

    /// A stored import decision under this actor's key.
    pub fn control_import_decision(
        &self,
        principal: &str,
        key: &LogicalSourceOwner,
        command_id: &[u8],
    ) -> Result<ControlImportDecision, Status> {
        self.guarded(|| {
            contract::operation_id(command_id)?;
            let tx = self.inner.database.begin_read().map_err(storage)?;
            self.read_policy(&tx, principal, key)?;
            let operations = tx.open_table(IMPORT_OPERATIONS).map_err(storage)?;
            let bytes = contract::operation_key(principal, key, command_id).encode_to_vec();
            let operation: ControlImportOperation = contract::decode(
                operations
                    .get(bytes.as_slice())
                    .map_err(storage)?
                    .ok_or_else(|| Status::not_found("control import command has no decision"))?
                    .value(),
            )?;
            operation.decision.ok_or_else(|| missing("decision"))
        })
    }

    /// The workflow row for a resource and workflow id, for administrators.
    pub fn control_import_workflow(
        &self,
        principal: &str,
        key: &LogicalSourceOwner,
        workflow_id: &[u8],
    ) -> Result<ControlImportWorkflow, Status> {
        self.guarded(|| {
            contract::workflow_id(workflow_id)?;
            let tx = self.inner.database.begin_read().map_err(storage)?;
            self.read_policy(&tx, principal, key)?;
            let imports = tx.open_table(IMPORTS).map_err(storage)?;
            let bytes = workflow_key(key, workflow_id);
            contract::decode(
                imports
                    .get(bytes.as_slice())
                    .map_err(storage)?
                    .ok_or_else(|| Status::not_found("control import workflow is unknown"))?
                    .value(),
            )
        })
    }

    /// The immutable committed view of a resource's applied control state.
    /// Lease tokens are omitted. Requires current resource Admin.
    pub fn control_snapshot(
        &self,
        principal: &str,
        key: &LogicalSourceOwner,
    ) -> Result<ControlCollectionSnapshot, Status> {
        self.guarded(|| {
            contract::key(key, false)?;
            let tx = self.inner.database.begin_read().map_err(storage)?;
            let policy = self.read_policy(&tx, principal, key)?;
            let meta = tx.open_table(META).map_err(storage)?;
            let (header, _) = read_headers(&meta, &self.inner.identity)?;
            let control = tx.open_table(CONTROL).map_err(storage)?;
            let state: ControlCollectionState = contract::decode(
                control
                    .get(key.encode_to_vec().as_slice())
                    .map_err(storage)?
                    .ok_or_else(|| Status::not_found("resource has no applied control state"))?
                    .value(),
            )?;
            let topologies = tx.open_table(TOPOLOGIES).map_err(storage)?;
            let topology: ControlTopology = contract::decode(
                topologies
                    .get(
                        ControlTopologyKey {
                            key: Some(key.clone()),
                            generation: state.topology_generation,
                        }
                        .encode_to_vec()
                        .as_slice(),
                    )
                    .map_err(storage)?
                    .ok_or_else(|| corrupt("current topology row is missing"))?
                    .value(),
            )?;
            let prefix = resource_prefix(key);
            let end = prefix_end(&prefix).ok_or_else(|| corrupt("resource prefix overflow"))?;
            let mut nodes = Vec::new();
            for entry in tx
                .open_table(NODES)
                .map_err(storage)?
                .range(prefix.as_slice()..end.as_slice())
                .map_err(storage)?
            {
                let (_, value) = entry.map_err(storage)?;
                let row: ControlNode = contract::decode(value.value())?;
                let node = row.node.ok_or_else(|| missing("node"))?;
                nodes.push(ControlNodeSnapshot {
                    node_id: node.node_id,
                    addr: node.addr,
                    state: node.state,
                    expires_unix_ms: node.expires_unix_ms,
                    capacity: node.capacity,
                    residency: row.residency,
                });
            }
            let mut replicas = Vec::new();
            for entry in tx
                .open_table(REPLICAS)
                .map_err(storage)?
                .range(prefix.as_slice()..end.as_slice())
                .map_err(storage)?
            {
                let (_, value) = entry.map_err(storage)?;
                replicas.push(contract::decode::<ControlReplica>(value.value())?);
            }
            let mut actions = Vec::new();
            for entry in tx
                .open_table(ACTIONS)
                .map_err(storage)?
                .range(prefix.as_slice()..end.as_slice())
                .map_err(storage)?
            {
                let (_, value) = entry.map_err(storage)?;
                actions.push(contract::decode::<ControlAction>(value.value())?);
            }
            actions.sort_by_key(|a| a.order);
            let mut completed_actions = Vec::new();
            for entry in tx
                .open_table(COMPLETED)
                .map_err(storage)?
                .range(prefix.as_slice()..end.as_slice())
                .map_err(storage)?
            {
                let (k, _) = entry.map_err(storage)?;
                let action_key: ControlActionKey = contract::decode(k.value())?;
                completed_actions.push(action_key.action_id);
            }
            let mut snapshot = ControlCollectionSnapshot {
                format_version: 1,
                authority: Some(self.inner.identity.clone()),
                key: Some(key.clone()),
                control_revision: header.source.control_revision,
                policy_revision: policy.revision,
                state: Some(state),
                topology: Some(topology),
                nodes,
                replicas,
                actions,
                completed_actions,
                digest: Vec::new(),
            };
            snapshot.digest = hash(SNAPSHOT_DOMAIN, &snapshot.encode_to_vec());
            Ok(snapshot)
        })
    }
}

fn staging<'a>(
    workflow: Option<&'a mut ControlImportWorkflow>,
    principal: &str,
) -> Result<&'a mut ControlImportWorkflow, Status> {
    let row = workflow.ok_or_else(|| {
        reject(
            Code::NotFound,
            "import workflow is unknown for this resource",
        )
    })?;
    if row.principal != principal {
        return Err(reject(
            Code::FailedPrecondition,
            "import workflow belongs to another actor; only its initiating administrator may continue or abort it",
        ));
    }
    if row.phase != ControlImportPhase::Staging as i32 {
        return Err(reject(
            Code::FailedPrecondition,
            "import workflow is already terminal",
        ));
    }
    Ok(row)
}

fn pending_workflow(
    imports: &impl ReadableTable<&'static [u8], &'static [u8]>,
    key: &LogicalSourceOwner,
) -> Result<Option<ControlImportWorkflow>, Status> {
    let prefix = resource_prefix(key);
    let end = prefix_end(&prefix).ok_or_else(|| corrupt("resource prefix overflow"))?;
    for entry in imports
        .range(prefix.as_slice()..end.as_slice())
        .map_err(storage)?
    {
        let (_, value) = entry.map_err(storage)?;
        let row: ControlImportWorkflow = contract::decode(value.value())?;
        if row.key.as_ref() == Some(key) && row.phase == ControlImportPhase::Staging as i32 {
            return Ok(Some(row));
        }
    }
    Ok(None)
}

/// Rows the commit writes, each already bounded.
struct Applied {
    receipt: ControlImportReceipt,
    control: (Vec<u8>, Vec<u8>),
    topologies: Vec<(Vec<u8>, Vec<u8>)>,
    nodes: Vec<(Vec<u8>, Vec<u8>)>,
    replicas: Vec<(Vec<u8>, Vec<u8>)>,
    actions: Vec<(Vec<u8>, Vec<u8>)>,
    completed: Vec<(Vec<u8>, Vec<u8>)>,
}

impl Applied {
    fn rows_iter(&self) -> impl Iterator<Item = &(Vec<u8>, Vec<u8>)> {
        std::iter::once(&self.control)
            .chain(&self.topologies)
            .chain(&self.nodes)
            .chain(&self.replicas)
            .chain(&self.actions)
            .chain(&self.completed)
    }
}

fn write_applied(
    tx: &redb::WriteTransaction,
    applied: &Applied,
) -> Result<ControlStoreHeader, Status> {
    let mut delta = ControlStoreHeader::default();
    {
        let mut table = tx.open_table(CONTROL).map_err(storage)?;
        table
            .insert(applied.control.0.as_slice(), applied.control.1.as_slice())
            .map_err(storage)?;
        delta.control_count = 1;
    }
    for (definition, rows, count) in [
        (TOPOLOGIES, &applied.topologies, &mut delta.topology_count),
        (NODES, &applied.nodes, &mut delta.node_count),
        (REPLICAS, &applied.replicas, &mut delta.replica_count),
        (ACTIONS, &applied.actions, &mut delta.action_count),
        (COMPLETED, &applied.completed, &mut delta.completed_count),
    ] {
        let mut table = tx.open_table(definition).map_err(storage)?;
        for (k, v) in rows {
            if table
                .insert(k.as_slice(), v.as_slice())
                .map_err(storage)?
                .is_some()
            {
                return Err(corrupt("applied control row already exists"));
            }
            *count += 1;
        }
    }
    Ok(delta)
}

/// Decode, validate and lay out the staged payload as bounded rows. Pure over
/// its inputs: no clock, no file, no live lookup.
fn apply(
    identity: &SourceAuthorityIdentity,
    key: &LogicalSourceOwner,
    declared: &BeginControlImport,
    payload: &[u8],
    revision: u64,
) -> Result<Applied, Status> {
    let bad = |message: &str| {
        reject(
            Code::FailedPrecondition,
            format!("import payload: {message}"),
        )
    };
    if payload.len() as u64 > MAX_IMPORT_PAYLOAD_BYTES {
        return Err(bad("exceeds 18 MiB"));
    }
    let staged = ControlImportPayload::decode(payload).map_err(|_| bad("malformed protobuf"))?;
    if staged.encode_to_vec() != payload || staged.format_version != 1 {
        return Err(bad("unknown, noncanonical or unsupported payload format"));
    }
    if crate::sha256::digest(&staged.retirement).as_slice() != declared.retirement_sha256 {
        return Err(bad("retirement bytes differ from the admitted retirement"));
    }
    let record = LegacyControlRetirement::decode(staged.retirement.as_slice())
        .map_err(|_| bad("malformed retirement record"))?;
    if record.encode_to_vec() != staged.retirement || record.format_version != 1 {
        return Err(bad("noncanonical or unsupported retirement record"));
    }
    if record.authority.as_ref() != Some(identity) {
        return Err(bad("retirement names another destination authority"));
    }
    if record.operation.as_ref() != declared.retirement_operation.as_ref() {
        return Err(bad(
            "retirement operation differs from the admitted operation",
        ));
    }
    let request = record
        .request
        .as_ref()
        .ok_or_else(|| bad("retirement request missing"))?;
    if request.key.as_ref() != Some(key) {
        return Err(bad("retirement resource differs from the import resource"));
    }
    let checkpoint = LegacyControlCheckpoint::decode(&record.checkpoint)
        .map_err(|error| bad(&format!("checkpoint: {}", error.message())))?;
    if checkpoint.sha256().as_slice() != request.expected_checkpoint_sha256 {
        return Err(bad("checkpoint digest differs from the retirement request"));
    }
    let state = checkpoint.state();
    if state.collection != key.collection {
        return Err(bad(
            "checkpoint collection differs from the import resource",
        ));
    }
    let supplement = staged
        .supplement
        .as_ref()
        .ok_or_else(|| bad("supplement is required"))?;
    if supplement.encoded_len() > MAX_SUPPLEMENT_BYTES {
        return Err(bad("supplement exceeds 1 MiB"));
    }
    validate_supplement(supplement, &checkpoint)?;
    let current = state
        .topology
        .as_ref()
        .ok_or_else(|| bad("checkpoint has no topology"))?;

    let mut topologies = Vec::with_capacity(1 + state.history.len());
    let mut generations = BTreeSet::new();
    let codes = match supplement.placement.as_ref() {
        Some(legacy_control_import_supplement::Placement::Tree(_)) => {
            supplement.route_codes.clone()
        }
        _ => Vec::new(),
    };
    let current_row = ControlTopology {
        format_version: 1,
        generation: current.generation,
        routes: current.routes.clone(),
        codes_available: !codes.is_empty(),
        codes,
        current: true,
        history_index: 0,
    };
    generations.insert(current.generation);
    topologies.push(topology_row(key, current_row)?);
    let mut history_generations = Vec::with_capacity(state.history.len());
    for (index, topology) in state.history.iter().enumerate() {
        if !generations.insert(topology.generation) {
            return Err(bad("history repeats a topology generation"));
        }
        history_generations.push(topology.generation);
        topologies.push(topology_row(
            key,
            ControlTopology {
                format_version: 1,
                generation: topology.generation,
                routes: topology.routes.clone(),
                codes_available: false,
                codes: Vec::new(),
                current: false,
                history_index: index as u32,
            },
        )?);
    }
    let mut nodes = Vec::with_capacity(state.nodes.len());
    for entry in &state.nodes {
        let node = entry
            .node
            .clone()
            .ok_or_else(|| bad("node entry without a node"))?;
        let k = ControlNodeKey {
            key: Some(key.clone()),
            node_id: entry.key.clone(),
        }
        .encode_to_vec();
        let v = ControlNode {
            format_version: 1,
            node: Some(node),
            residency: SourceResidency::Unspecified as i32,
            provenance: ControlFactProvenance::LegacyImport as i32,
        }
        .encode_to_vec();
        bounded_record(&v, "imported node")?;
        nodes.push((k, v));
    }
    let mut replicas = Vec::with_capacity(state.replicas.len());
    for entry in &state.replicas {
        let replica = entry
            .replica
            .clone()
            .ok_or_else(|| bad("replica entry without a replica"))?;
        let k = ControlReplicaKey {
            key: Some(key.clone()),
            replica_key: entry.key.clone(),
        }
        .encode_to_vec();
        let v = ControlReplica {
            format_version: 1,
            replica: Some(replica),
            provenance: ControlFactProvenance::LegacyImport as i32,
        }
        .encode_to_vec();
        bounded_record(&v, "imported replica")?;
        replicas.push((k, v));
    }
    let mut actions = Vec::with_capacity(state.actions.len());
    for (order, action) in state.actions.iter().enumerate() {
        let k = ControlActionKey {
            key: Some(key.clone()),
            action_id: action.action_id,
        }
        .encode_to_vec();
        let v = ControlAction {
            format_version: 1,
            action: Some(action.clone()),
            order: order as u32,
            state: ControlActionState::ImportedUnreconciled as i32,
            provenance: ControlFactProvenance::LegacyImport as i32,
        }
        .encode_to_vec();
        bounded_record(&v, "imported action")?;
        actions.push((k, v));
    }
    let mut completed = Vec::with_capacity(state.completed_actions.len());
    for id in &state.completed_actions {
        let k = ControlActionKey {
            key: Some(key.clone()),
            action_id: *id,
        }
        .encode_to_vec();
        let v = ControlCompletedAction {
            format_version: 1,
            provenance: ControlFactProvenance::LegacyImport as i32,
        }
        .encode_to_vec();
        completed.push((k, v));
    }
    let receipt = ControlImportReceipt {
        format_version: 1,
        retirement_sha256: declared.retirement_sha256.clone(),
        payload_sha256: declared.payload_sha256.clone(),
        imported_legacy_revision: state.revision,
        imported_generation: current.generation,
        control_revision: revision,
        routes: current.routes.len() as u64,
        history: state.history.len() as u64,
        nodes: nodes.len() as u64,
        replicas: replicas.len() as u64,
        actions: actions.len() as u64,
        completed_actions: completed.len() as u64,
    };
    let control_value = ControlCollectionState {
        format_version: 1,
        key: Some(key.clone()),
        import: Some(receipt.clone()),
        control_revision: revision,
        legacy_format: state.format,
        imported_legacy_revision: state.revision,
        next_token: state.next_token,
        next_action: state.next_action,
        topology_generation: current.generation,
        history_generations,
        configuration: Some(supplement.clone()),
        history_rollback_available: false,
    }
    .encode_to_vec();
    bounded_record(&control_value, "applied control state")?;
    Ok(Applied {
        receipt,
        control: (key.encode_to_vec(), control_value),
        topologies,
        nodes,
        replicas,
        actions,
        completed,
    })
}

fn topology_row(
    key: &LogicalSourceOwner,
    row: ControlTopology,
) -> Result<(Vec<u8>, Vec<u8>), Status> {
    let k = ControlTopologyKey {
        key: Some(key.clone()),
        generation: row.generation,
    }
    .encode_to_vec();
    let v = row.encode_to_vec();
    bounded_record(&v, "imported topology")?;
    Ok((k, v))
}

fn validate_supplement(
    supplement: &LegacyControlImportSupplement,
    checkpoint: &LegacyControlCheckpoint,
) -> Result<(), Status> {
    use legacy_control_import_supplement::{Derived, Placement, Provider};
    let bad = |message: &str| {
        reject(
            Code::FailedPrecondition,
            format!("import supplement: {message}"),
        )
    };
    if supplement.format_version != 1 {
        return Err(bad("requires format 1"));
    }
    if !supplement.history_placement_unavailable {
        return Err(bad("format 1 requires history_placement_unavailable = true; historical rollback stays unavailable until trees are supplied"));
    }
    let routes = checkpoint
        .state()
        .topology
        .as_ref()
        .map_or(0, |t| t.routes.len());
    match supplement.placement.as_ref() {
        Some(Placement::Tree(tree)) => {
            let placement = crate::placement::Placement::validate(
                &crate::placement::PlacementTreeConfig::from_proto(tree),
            )
            .map_err(|error| bad(&format!("placement tree: {error}")))?;
            if supplement.route_codes.len() != routes {
                return Err(bad(&format!(
                    "{} route codes for {routes} current routes",
                    supplement.route_codes.len()
                )));
            }
            for (index, code) in supplement.route_codes.iter().enumerate() {
                if !code.has_placement {
                    return Err(bad(&format!(
                        "route {index} has no placement code under a tree"
                    )));
                }
                let signed = i64::try_from(code.placement)
                    .map_err(|_| bad(&format!("route {index} placement code exceeds i64")))?;
                if placement.leaf_by_code(signed).is_none() {
                    return Err(bad(&format!("route {index} placement code names no leaf")));
                }
            }
        }
        Some(Placement::NoPlacement(true)) => {
            if !supplement.route_codes.is_empty() {
                return Err(bad("route codes are present without a tree"));
            }
        }
        _ => {
            return Err(bad(
                "placement must be an explicit tree or no_placement = true",
            ))
        }
    }
    match supplement.derived.as_ref() {
        Some(Derived::Declaration(declaration)) => {
            crate::derived::Declaration::compile(declaration)
                .map_err(|error| bad(&format!("derived declaration: {error}")))?;
            if crate::derived::fingerprint_of(declaration) != supplement.derived_fingerprint {
                return Err(bad("derived fingerprint differs from the declaration"));
            }
        }
        Some(Derived::NoDerived(true)) => {
            if !supplement.derived_fingerprint.is_empty() {
                return Err(bad("derived fingerprint present without a declaration"));
            }
        }
        _ => {
            return Err(bad(
                "derived must be an explicit declaration or no_derived = true",
            ))
        }
    }
    match supplement.provider.as_ref() {
        Some(Provider::Geometry(geometry)) => {
            if geometry.format_version != 1
                || geometry.backend_kind.is_empty()
                || geometry.config_format.is_empty()
                || geometry.dimension == 0
                || geometry.dimension % 8 != 0
                || !(2..=4).contains(&geometry.bits_per_component)
                || geometry.row_bytes_formula_version != 1
                || geometry.scoring_fingerprint.is_empty()
            {
                return Err(bad("provider geometry requires format 1, backend kind and config format, a dimension that is a positive multiple of 8, 2..4 bits, row-bytes formula 1 and a scoring fingerprint"));
            }
        }
        Some(Provider::NoProvider(true)) => {}
        _ => {
            return Err(bad(
                "provider must be explicit geometry or no_provider = true",
            ))
        }
    }
    let policy = supplement
        .policy
        .as_ref()
        .ok_or_else(|| bad("planner policy is required"))?;
    if policy.format_version != 1 || policy.planner_version == 0 {
        return Err(bad(
            "planner policy requires format 1 and a nonzero planner version",
        ));
    }
    if policy.control.as_ref() != Some(checkpoint.policy()) {
        return Err(bad(
            "control policy differs from the checkpoint's captured policy",
        ));
    }
    Ok(())
}

/// Versioned adoption of a format-1 store: the nine control tables and the
/// format-2 header are added in one transaction. A store that is neither a
/// complete format 1 nor a complete format 2 refuses; nothing is defaulted.
pub(super) fn adopt(store: &SourceAuthorityStore) -> Result<(), Status> {
    let (tables, has_control) = {
        let tx = store.inner.database.begin_read().map_err(storage)?;
        let tables = tx.list_tables().map_err(storage)?.count();
        let meta = tx.open_table(META).map_err(corrupt)?;
        let has_control = meta.get(CONTROL_HEADER).map_err(storage)?.is_some();
        (tables, has_control)
    };
    match (tables, has_control) {
        (FORMAT_2_TABLES, true) => Ok(()),
        (4, false) => {
            let mut tx = store.inner.database.begin_write().map_err(storage)?;
            tx.set_durability(Durability::Immediate).map_err(storage)?;
            {
                for definition in [
                    IMPORT_OPERATIONS,
                    IMPORTS,
                    CHUNKS,
                    CONTROL,
                    TOPOLOGIES,
                    NODES,
                    REPLICAS,
                    ACTIONS,
                    COMPLETED,
                ] {
                    tx.open_table(definition).map_err(storage)?;
                }
                let mut meta = tx.open_table(META).map_err(storage)?;
                meta.insert(
                    CONTROL_HEADER,
                    ControlStoreHeader {
                        format_version: STORE_FORMAT,
                        ..Default::default()
                    }
                    .encode_to_vec()
                    .as_slice(),
                )
                .map_err(storage)?;
            }
            tx.commit().map_err(storage)
        }
        _ => Err(corrupt(
            "control tables and header disagree; neither a complete format 1 nor format 2 store",
        )),
    }
}

pub(super) fn create_tables(tx: &redb::WriteTransaction) -> Result<(), Status> {
    for definition in [
        IMPORT_OPERATIONS,
        IMPORTS,
        CHUNKS,
        CONTROL,
        TOPOLOGIES,
        NODES,
        REPLICAS,
        ACTIONS,
        COMPLETED,
    ] {
        tx.open_table(definition).map_err(storage)?;
    }
    let mut meta = tx.open_table(META).map_err(storage)?;
    meta.insert(
        CONTROL_HEADER,
        ControlStoreHeader {
            format_version: STORE_FORMAT,
            ..Default::default()
        }
        .encode_to_vec()
        .as_slice(),
    )
    .map_err(storage)?;
    Ok(())
}

/// Reservation headroom every regular command must respect.
pub(super) fn reserved(
    meta: &impl ReadableTable<&'static str, &'static [u8]>,
) -> Result<(u64, u64, u64), Status> {
    let control: ControlStoreHeader = contract::decode(
        meta.get(CONTROL_HEADER)
            .map_err(storage)?
            .ok_or_else(|| missing("control header"))?
            .value(),
    )?;
    recovery::control_header(&control)?;
    Ok((
        control.reserved_bytes,
        control.reserved_decisions,
        control.import_decision_count,
    ))
}

pub(super) fn validate_stored_command(
    command: &ControlImportCommand,
    identity: &SourceAuthorityIdentity,
    limits: &SourceAuthorityLimits,
) -> Result<(), Status> {
    validate_command(command, identity, limits)
}

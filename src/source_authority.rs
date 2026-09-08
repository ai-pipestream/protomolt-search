//! Transactional server control state for logical source-owner preparation.
//!
//! The hosting adapter supplies an authenticated stable principal. This module
//! authorizes it against committed policy; it does not authenticate credentials.
//! There is no RPC or source admission adapter here. Prepared/cancelled records
//! never grant access to a catalog, activate a writer or authorize a transfer.

use crate::pb::storage::*;
use crate::pb::AccessPolicy;
use prost::Message;
use redb::{
    Database, Durability, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition,
};
use source_authority_command::Action;
use std::fs::{File, OpenOptions};
use std::path::Path;
use std::sync::{Arc, Mutex};
use tonic::{Code, Status};

mod contract;
mod recovery;
mod retirement;
mod transition;

const META: TableDefinition<&str, &[u8]> = TableDefinition::new("source_authority_meta");
const OWNERS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("source_authority_owners");
const DECISIONS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("source_authority_decisions");
const WORKFLOWS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("source_authority_workflows");
const CLOSED: &str = "source authority storage failed; close every clone and reopen existing state before continuing";

#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum Fault {
    BeforeCommit,
    AfterCommit,
    ExitBeforeCommit,
    ExitAfterCommit,
}

struct Inner {
    database: Database,
    // All public operations acquire this before opening a database transaction.
    // The boolean permanently closes the instance after a storage failure.
    closed: Mutex<bool>,
    identity: SourceAuthorityIdentity,
    #[cfg(test)]
    fault: Mutex<Option<Fault>>,
    // Keep the same open-file description locked until after Database drops.
    _file_lock: File,
}

#[derive(Clone)]
pub struct SourceAuthorityStore {
    inner: Arc<Inner>,
}

fn storage(error: impl std::fmt::Display) -> Status {
    Status::internal(format!("source authority storage: {error}"))
}

fn corrupt(error: impl std::fmt::Display) -> Status {
    Status::data_loss(format!("source authority state: {error}"))
}

fn missing(name: &str) -> Status {
    corrupt(format!("{name} is missing"))
}

fn digest(command: &SourceAuthorityCommand) -> Vec<u8> {
    let mut hash = crate::sha256::Sha256::new();
    hash.update(b"protomolt.source-authority.command.v1\0");
    hash.update(&command.encode_to_vec());
    hash.finalize().to_vec()
}

fn payload_change(total: u64, removed: usize, added: usize) -> Result<u64, Status> {
    total
        .checked_sub(removed as u64)
        .and_then(|n| n.checked_add(added as u64))
        .ok_or_else(|| corrupt("payload accounting overflow or underflow"))
}

impl SourceAuthorityStore {
    // Owner-side preparation work holds current authority through its source
    // commit. This is an adapter fence, never called from pure state application.
    pub(crate) fn with_prepared_owner<T>(
        &self,
        principal: &str,
        expected: &PreparedSourceOwner,
        run: impl FnOnce(&SourceAuthorityIdentity) -> Result<T, Status>,
    ) -> Result<T, Status> {
        contract::owner(expected).map_err(|e| Status::invalid_argument(e.message()))?;
        self.guarded(|| {
            let key = expected
                .key
                .as_ref()
                .ok_or_else(|| missing("prepared owner key"))?;
            let tx = self.inner.database.begin_read().map_err(storage)?;
            self.read_policy(&tx, principal, key)?;
            let table = tx.open_table(OWNERS).map_err(storage)?;
            let bytes = key.encode_to_vec();
            let held: PreparedSourceOwner = contract::decode(
                table
                    .get(bytes.as_slice())
                    .map_err(storage)?
                    .ok_or_else(|| {
                        Status::failed_precondition("source owner has no committed preparation")
                    })?
                    .value(),
            )?;
            contract::owner(&held).map_err(corrupt)?;
            if &held != expected || held.phase != PreparedSourceOwnerPhase::Prepared as i32 {
                return Err(Status::failed_precondition(
                    "source owner preparation changed or is no longer pending",
                ));
            }
            // Only failures of this control store latch its instance closed.
            // A source-side refusal/failure belongs to the source adapter.
            Ok(run(&self.inner.identity))
        })?
    }

    pub(crate) fn with_resource_admin<T>(
        &self,
        principal: &str,
        identity: &SourceAuthorityIdentity,
        key: &LogicalSourceOwner,
        run: impl FnOnce() -> Result<T, Status>,
    ) -> Result<T, Status> {
        self.guarded(|| {
            if identity != &self.inner.identity {
                return Err(Status::failed_precondition(
                    "managed source authority identity differs",
                ));
            }
            let tx = self.inner.database.begin_read().map_err(storage)?;
            self.read_policy(&tx, principal, key)?;
            Ok(run())
        })?
    }

    /// Explicit trusted bootstrap in an existing durable directory. The supplied
    /// identity, limits and resource bindings become immutable. No source exists
    /// as a consequence of creating this control database.
    pub fn create(
        path: &Path,
        identity: &SourceAuthorityIdentity,
        policy: &AccessPolicy,
        limits: &SourceAuthorityLimits,
    ) -> Result<Self, Status> {
        contract::identity(identity)?;
        contract::limits(limits)?;
        contract::policy(policy)?;
        if policy.encoded_len() as u64 > limits.max_payload_bytes {
            return Err(Status::resource_exhausted(
                "source authority bootstrap policy exceeds payload capacity",
            ));
        }
        let (store, parent) = Self::open_file(path, identity, true)?;
        let header = SourceAuthorityHeader {
            format_version: 1,
            identity: Some(identity.clone()),
            limits: Some(limits.clone()),
            control_revision: 1,
            payload_bytes: policy.encoded_len() as u64,
            ..Default::default()
        };
        let mut tx = store.inner.database.begin_write().map_err(storage)?;
        tx.set_durability(Durability::Immediate).map_err(storage)?;
        {
            let mut meta = tx.open_table(META).map_err(storage)?;
            meta.insert("header", header.encode_to_vec().as_slice())
                .map_err(storage)?;
            meta.insert("policy", policy.encode_to_vec().as_slice())
                .map_err(storage)?;
            tx.open_table(OWNERS).map_err(storage)?;
            tx.open_table(DECISIONS).map_err(storage)?;
            tx.open_table(WORKFLOWS).map_err(storage)?;
        }
        tx.commit().map_err(storage)?;
        parent.sync_all().map_err(storage)?;
        Ok(store)
    }

    /// Existing-state recovery only. An empty/missing/corrupt authority never
    /// initializes a fresh identity, policy or retry history.
    pub fn open(path: &Path, expected_identity: &SourceAuthorityIdentity) -> Result<Self, Status> {
        contract::identity(expected_identity)?;
        let (store, _) = Self::open_file(path, expected_identity, false)?;
        recovery::validate(&store)?;
        Ok(store)
    }

    fn open_file(
        path: &Path,
        identity: &SourceAuthorityIdentity,
        create: bool,
    ) -> Result<(Self, File), Status> {
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let directory = File::open(parent).map_err(storage)?;
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(create);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(path).map_err(|error| match error.kind() {
            std::io::ErrorKind::AlreadyExists => {
                Status::already_exists("source authority already exists")
            }
            std::io::ErrorKind::NotFound => Status::not_found(
                "source authority is missing; recovery never bootstraps a new history",
            ),
            _ => storage(error),
        })?;
        file.try_lock().map_err(|error| {
            Status::failed_precondition(format!("source authority exclusive file lock: {error}"))
        })?;
        if !create && file.metadata().map_err(storage)?.len() == 0 {
            return Err(corrupt("existing file is empty"));
        }
        let mut builder = Database::builder();
        builder.set_cache_size(8 * 1024 * 1024);
        let database = builder
            .create_file(file.try_clone().map_err(storage)?)
            .map_err(storage)?;
        Ok((
            Self {
                inner: Arc::new(Inner {
                    database,
                    closed: Mutex::new(false),
                    identity: identity.clone(),
                    #[cfg(test)]
                    fault: Mutex::new(None),
                    _file_lock: file,
                }),
            },
            directory,
        ))
    }

    fn guarded<T>(&self, run: impl FnOnce() -> Result<T, Status>) -> Result<T, Status> {
        let mut closed = self
            .inner
            .closed
            .lock()
            .map_err(|_| storage("authority lock poisoned"))?;
        if *closed {
            return Err(Status::failed_precondition(CLOSED));
        }
        let result = run();
        if result
            .as_ref()
            .is_err_and(|error| matches!(error.code(), Code::Internal | Code::DataLoss))
        {
            *closed = true;
        }
        result
    }

    /// Execute one authenticated control command. The transition, current policy
    /// check, retry response and counters share one durable database transaction.
    /// Recorded business rejections are decisions with a nonzero status code;
    /// envelope/admission/capacity/storage errors are returned as Status.
    pub fn execute(
        &self,
        principal: &str,
        command: &SourceAuthorityCommand,
    ) -> Result<SourceAuthorityDecision, Status> {
        self.guarded(|| {
            let decision = self.execute_locked(principal, command)?;
            // A policy command can revoke its own issuer. Its durable decision
            // remains retryable, but disclosure still needs the current grant.
            let tx = self.inner.database.begin_read().map_err(storage)?;
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
    ) -> Result<SourceAuthorityDecision, Status> {
        let mut tx = self.inner.database.begin_write().map_err(storage)?;
        tx.set_durability(Durability::Immediate).map_err(storage)?;
        let decision;
        {
            let mut meta = tx.open_table(META).map_err(storage)?;
            let mut header: SourceAuthorityHeader = contract::decode(
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
            let limits = header.limits.clone().ok_or_else(|| missing("limits"))?;
            contract::command(command, &self.inner.identity, &limits)?;
            let key = command.key.as_ref().ok_or_else(|| missing("command key"))?;
            contract::authorize(&policy, principal, key)?;
            let operation_key = contract::operation_key(principal, key, &command.command_id);
            let operation_bytes = operation_key.encode_to_vec();
            let request_sha256 = digest(command);
            let mut decisions = tx.open_table(DECISIONS).map_err(storage)?;
            if let Some(saved) = decisions.get(operation_bytes.as_slice()).map_err(storage)? {
                let operation: SourceAuthorityOperation = contract::decode(saved.value())?;
                recovery::operation(&operation_key, &operation, &header, &policy)?;
                if operation.request_sha256 != request_sha256
                    || operation.command.as_ref() != Some(command)
                {
                    return Err(Status::failed_precondition(
                        "source authority command_id was already used with different content",
                    ));
                }
                return operation.decision.ok_or_else(|| missing("retry decision"));
            }
            if header.decision_count >= limits.max_decisions {
                return Err(Status::resource_exhausted(
                    "source authority decision capacity is full; retry history is never evicted",
                ));
            }
            let mut owners = tx.open_table(OWNERS).map_err(storage)?;
            let key_bytes = key.encode_to_vec();
            let old_owner_bytes = owners
                .get(key_bytes.as_slice())
                .map_err(storage)?
                .map(|value| value.value().to_vec());
            let before: Option<PreparedSourceOwner> = old_owner_bytes
                .as_deref()
                .map(contract::decode)
                .transpose()?;
            if let Some(owner) = &before {
                contract::owner(owner).map_err(corrupt)?;
            }
            let mut workflows = tx.open_table(WORKFLOWS).map_err(storage)?;
            let workflow_key = match command.action.as_ref() {
                Some(Action::Prepare(request)) => {
                    Some(contract::workflow_key(key, &request.workflow_id))
                }
                _ => None,
            };
            let workflow_bytes = workflow_key.as_ref().map(Message::encode_to_vec);
            let workflow_used = if let Some(bytes) = &workflow_bytes {
                workflows.get(bytes.as_slice()).map_err(storage)?.is_some()
            } else {
                false
            };
            let change = transition::apply(
                &header,
                &policy,
                before.as_ref(),
                command,
                &operation_key,
                workflow_used,
            )?;
            decision = change.decision;
            let operation = SourceAuthorityOperation {
                format_version: 1,
                request_sha256,
                command: Some(command.clone()),
                decision: Some(decision.clone()),
            };
            let encoded = operation.encode_to_vec();
            if encoded.len() > contract::MAX_RECORD_BYTES {
                return Err(Status::resource_exhausted(
                    "source authority decision record exceeds 2MiB",
                ));
            }
            header.payload_bytes = payload_change(
                header.payload_bytes,
                0,
                operation_bytes.len() + encoded.len(),
            )?;
            header.decision_count = header
                .decision_count
                .checked_add(1)
                .ok_or_else(|| corrupt("decision counter overflow"))?;
            if let Some(owner) = change.owner {
                let encoded_owner = owner.encode_to_vec();
                if old_owner_bytes.is_none() {
                    if header.owner_count >= limits.max_owners {
                        return Err(Status::resource_exhausted(
                            "source authority owner capacity is full",
                        ));
                    }
                    header.owner_count = header
                        .owner_count
                        .checked_add(1)
                        .ok_or_else(|| corrupt("owner counter overflow"))?;
                    header.payload_bytes =
                        payload_change(header.payload_bytes, 0, key_bytes.len())?;
                }
                header.payload_bytes = payload_change(
                    header.payload_bytes,
                    old_owner_bytes.as_ref().map_or(0, Vec::len),
                    encoded_owner.len(),
                )?;
                owners
                    .insert(key_bytes.as_slice(), encoded_owner.as_slice())
                    .map_err(storage)?;
                if change.reserve_workflow {
                    let workflow = SourceAuthorityWorkflow {
                        preparation_command: Some(operation_key.clone()),
                        ownership_generation: owner.ownership_generation,
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
                }
            }
            if let Some(updated) = change.policy {
                let bytes = updated.encode_to_vec();
                header.payload_bytes =
                    payload_change(header.payload_bytes, policy.encoded_len(), bytes.len())?;
                meta.insert("policy", bytes.as_slice()).map_err(storage)?;
            }
            if header.payload_bytes > limits.max_payload_bytes {
                return Err(Status::resource_exhausted(
                    "source authority payload capacity is full; retry history is never evicted",
                ));
            }
            header.control_revision = decision.control_revision;
            decisions
                .insert(operation_bytes.as_slice(), encoded.as_slice())
                .map_err(storage)?;
            meta.insert("header", header.encode_to_vec().as_slice())
                .map_err(storage)?;
        }
        #[cfg(test)]
        self.inject(false)?;
        tx.commit().map_err(storage)?;
        #[cfg(test)]
        self.inject(true)?;
        Ok(decision)
    }

    #[cfg(test)]
    fn inject(&self, after: bool) -> Result<(), Status> {
        let mut fault = self.inner.fault.lock().unwrap();
        match (*fault, after) {
            (Some(Fault::BeforeCommit), false) => {
                *fault = None;
                Err(Status::aborted(
                    "injected source authority BeforeCommit failure",
                ))
            }
            (Some(Fault::AfterCommit), true) => {
                *fault = None;
                Err(storage("injected source authority AfterCommit failure"))
            }
            (Some(Fault::ExitBeforeCommit), false) | (Some(Fault::ExitAfterCommit), true) => {
                std::process::exit(87)
            }
            _ => Ok(()),
        }
    }

    fn read_policy(
        &self,
        tx: &redb::ReadTransaction,
        principal: &str,
        key: &LogicalSourceOwner,
    ) -> Result<AccessPolicy, Status> {
        contract::key(key, false)?;
        let meta = tx.open_table(META).map_err(storage)?;
        let header: SourceAuthorityHeader = contract::decode(
            meta.get("header")
                .map_err(storage)?
                .ok_or_else(|| missing("header"))?
                .value(),
        )?;
        recovery::header(&header, &self.inner.identity)?;
        let policy: AccessPolicy = contract::decode(
            meta.get("policy")
                .map_err(storage)?
                .ok_or_else(|| missing("policy"))?
                .value(),
        )?;
        contract::policy(&policy).map_err(corrupt)?;
        contract::authorize(&policy, principal, key)?;
        Ok(policy)
    }

    pub fn owner(
        &self,
        principal: &str,
        key: &LogicalSourceOwner,
    ) -> Result<PreparedSourceOwner, Status> {
        self.guarded(|| {
            contract::key(key, true)?;
            let tx = self.inner.database.begin_read().map_err(storage)?;
            self.read_policy(&tx, principal, key)?;
            let owners = tx.open_table(OWNERS).map_err(storage)?;
            let bytes = key.encode_to_vec();
            let owner: PreparedSourceOwner = contract::decode(
                owners
                    .get(bytes.as_slice())
                    .map_err(storage)?
                    .ok_or_else(|| Status::not_found("source authority owner is not prepared"))?
                    .value(),
            )?;
            contract::owner(&owner).map_err(corrupt)?;
            if owner.key.as_ref() != Some(key) {
                return Err(corrupt("owner key differs from table key"));
            }
            Ok(owner)
        })
    }

    pub fn decision(
        &self,
        principal: &str,
        key: &LogicalSourceOwner,
        command_id: &[u8],
    ) -> Result<SourceAuthorityDecision, Status> {
        self.guarded(|| {
            contract::operation_id(command_id)?;
            let tx = self.inner.database.begin_read().map_err(storage)?;
            let policy = self.read_policy(&tx, principal, key)?;
            let meta = tx.open_table(META).map_err(storage)?;
            let header: SourceAuthorityHeader = contract::decode(
                meta.get("header")
                    .map_err(storage)?
                    .ok_or_else(|| missing("header"))?
                    .value(),
            )?;
            let key = contract::operation_key(principal, key, command_id);
            let bytes = key.encode_to_vec();
            let decisions = tx.open_table(DECISIONS).map_err(storage)?;
            let operation: SourceAuthorityOperation = contract::decode(
                decisions
                    .get(bytes.as_slice())
                    .map_err(storage)?
                    .ok_or_else(|| Status::not_found("source authority command has no decision"))?
                    .value(),
            )?;
            recovery::operation(&key, &operation, &header, &policy)?;
            operation.decision.ok_or_else(|| missing("decision"))
        })
    }

    /// A current policy view restricted to one authorized resource. No grants or
    /// ownership metadata from another collection are returned.
    pub fn policy(
        &self,
        principal: &str,
        workspace: &str,
        collection: &str,
    ) -> Result<AccessPolicy, Status> {
        self.guarded(|| {
            let key = LogicalSourceOwner {
                workspace: workspace.into(),
                collection: collection.into(),
                owner_id: Vec::new(),
            };
            let tx = self.inner.database.begin_read().map_err(storage)?;
            let mut policy = self.read_policy(&tx, principal, &key)?;
            policy.resources.retain(|resource| {
                resource.workspace == workspace && resource.collection == collection
            });
            policy
                .grants
                .retain(|grant| grant.workspace == workspace && grant.collection == collection);
            Ok(policy)
        })
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod retirement_tests;

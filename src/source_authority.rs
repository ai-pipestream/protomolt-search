//! Transactional server control state for logical source-owner preparation.
//!
//! The hosting adapter supplies an authenticated stable principal. This module
//! authorizes it against committed policy; it does not authenticate credentials.
//! There is no RPC or source admission adapter here. Prepared/cancelled records
//! never grant access to a catalog, activate a writer or authorize a transfer.

use crate::authorization::{AuthorizationGuard, Authorizer};
use crate::pb::storage::*;
use crate::pb::{AccessAction, AccessDecision, AccessPolicy};
use prost::Message;
use redb::{
    Database, Durability, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition,
};
use source_authority_command::Action;
use std::fs::{File, OpenOptions};
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard};
use tokio::sync::watch;
use tonic::{Code, Status};

mod capacity;
mod contract;
mod import;
mod map_feed;
mod recovery;
#[cfg(feature = "net")]
mod relay_map;
mod retirement;
mod transition;

#[cfg(any(test, feature = "fault-injection"))]
pub use self::ExitFault as SourceExitFault;
pub use capacity::{
    PlanningContext, MAX_CAPACITY_BYTES, MAX_CAPACITY_RECORDS, MAX_CAPACITY_REPORTERS,
};
pub use import::{
    chunk_capacity, chunk_digest, payload_digest, plan_chunks, retirement_digest,
    MAX_IMPORT_CHUNKS, MAX_IMPORT_PAYLOAD_BYTES, MAX_SUPPLEMENT_BYTES, MIN_CHUNK_CAPACITY,
};
pub use map_feed::MapConsumer;
#[cfg(feature = "net")]
pub use relay_map::AuthorityMapSource;

const META: TableDefinition<&str, &[u8]> = TableDefinition::new("source_authority_meta");
/// Meta key of the Raft applied position (docs/raft-hosting.md).
pub(crate) const RAFT_META: &str = "raft";
/// Present only in a store prepared for a member that has not received its
/// first snapshot: log entries refuse to apply until the leader's image
/// replaces the file, so a member never replays onto its own genesis.
pub(crate) const MEMBER_META: &str = "member";
const MEMBER_PENDING: &[u8] = b"awaiting-snapshot";
const OWNERS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("source_authority_owners");
const DECISIONS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("source_authority_decisions");
const WORKFLOWS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("source_authority_workflows");
const CLOSED: &str = "source authority storage failed; close every clone and reopen existing state before continuing";

#[cfg(any(test, feature = "fault-injection"))]
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone, Copy, PartialEq, Eq)]
enum Fault {
    BeforeCommit,
    AfterCommit,
    ExitBeforeCommit,
    ExitAfterCommit,
}

/// A process-exit fault at the next store transaction boundary, for
/// crash-recovery harnesses: the process exits with code 87 either before
/// the transaction commits or right after it, before any reply.
#[cfg(any(test, feature = "fault-injection"))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExitFault {
    BeforeCommit,
    AfterCommit,
}

struct Inner {
    database: Database,
    // Admitted operations hold this shared; every control command holds it
    // exclusively, so a policy or ownership change waits for admitted work to
    // drain and no admitted operation observes a change under its guard.
    // Acquired before `closed`, never while `closed` is held.
    admission: RwLock<()>,
    // The committed policy revision, published after each commit under the
    // exclusive admission guard: one ordered history for permits.
    revisions: watch::Sender<u64>,
    // The applied control revision, published after each commit: the map
    // feed's wake-up. A reader then serves committed rows, never a proposal.
    applied: watch::Sender<u64>,
    // All public operations acquire this before opening a database transaction.
    // The boolean permanently closes the instance after a storage failure.
    closed: Mutex<bool>,
    identity: SourceAuthorityIdentity,
    path: PathBuf,
    // Raft hosting: while hosted, the direct command paths refuse and only
    // the committed-replay paths apply, each writing the pending applied
    // position in its own transaction (docs/raft-hosting.md).
    hosted: AtomicBool,
    raft_pending: Mutex<Option<RaftApplied>>,
    #[cfg(any(test, feature = "fault-injection"))]
    fault: Mutex<Option<Fault>>,
    // Keep the same open-file description locked until after Database drops.
    _file_lock: File,
}

#[derive(Clone)]
pub struct SourceAuthorityStore {
    inner: Arc<Inner>,
}

/// Why a snapshot swap did not happen.
pub(crate) enum ReplaceFailure {
    /// The handle is intact and usable; nothing changed on disk.
    Refused(SourceAuthorityStore, Status),
    /// The handle was closed and the reopen failed; reopen from durable state.
    Lost(Status),
}

impl std::fmt::Debug for SourceAuthorityStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceAuthorityStore")
            .field("group_id", &self.inner.identity.group_id)
            .finish_non_exhaustive()
    }
}

/// One admission acquisition: current committed policy and ownership resolved
/// under a guard that every control command waits for. Hold it through the
/// source commit it admits; never acquire a second one, pin a permit, or run
/// a control command while holding it — an exclusive waiter would then block
/// the second acquisition behind this one.
/// The validity interval of a Raft-hosted admission (docs/raft-admission.md).
/// `anchor` is taken before the host's read barrier is invoked, so it is at
/// or before the instant the barrier's heartbeats were sent; every follower
/// that acknowledged them refreshed its own leader lease at a later instant,
/// and none grants a vote before that instant plus the election ceiling.
/// The interval therefore includes every response, catch-up and scheduling
/// delay between the barrier and the grant. The wall-clock anchor catches a
/// suspend the monotonic clock does not count.
#[derive(Clone, Copy, Debug)]
pub struct AdmissionLease {
    pub anchor: std::time::Instant,
    pub anchor_wall: std::time::SystemTime,
    pub ttl: std::time::Duration,
}

impl AdmissionLease {
    pub fn deadline(&self) -> std::time::Instant {
        self.anchor + self.ttl
    }

    /// Expired on the monotonic clock, expired on the wall clock (a
    /// suspended process or machine), or a wall clock that moved backwards:
    /// each is a named refusal, never an extension.
    pub fn fresh(&self) -> Result<(), Status> {
        if std::time::Instant::now() >= self.deadline() {
            return Err(Status::failed_precondition(
                "admission lease expired; obtain a fresh admission through the host",
            ));
        }
        match std::time::SystemTime::now().duration_since(self.anchor_wall) {
            Ok(elapsed) if elapsed < self.ttl => Ok(()),
            Ok(_) => Err(Status::failed_precondition(
                "admission lease expired on the wall clock (the process or machine was suspended); obtain a fresh admission through the host",
            )),
            Err(_) => Err(Status::failed_precondition(
                "wall clock moved backwards under an admission lease; obtain a fresh admission through the host",
            )),
        }
    }
}

#[must_use = "dropping the admission releases the fence"]
pub struct SourceAdmission<'a> {
    store: &'a SourceAuthorityStore,
    principal: String,
    // A Raft-hosted admission is a lease: the host granted it after a
    // linearizable read anchored before the barrier, and it admits nothing
    // past the anchor plus the lease.
    lease: Option<AdmissionLease>,
    _shared: RwLockReadGuard<'a, ()>,
}

impl SourceAdmission<'_> {
    pub fn identity(&self) -> &SourceAuthorityIdentity {
        &self.store.inner.identity
    }

    /// A leased admission past its interval admits nothing; the lease is
    /// what keeps admission authoritative under leader isolation
    /// (docs/raft-admission.md).
    fn fresh(&self) -> Result<(), Status> {
        match &self.lease {
            Some(lease) => lease.fresh(),
            None => Ok(()),
        }
    }

    /// The final check a source write performs inside its own transaction,
    /// immediately before commit: the boundary at which the write becomes
    /// authorized (docs/raft-admission.md, "Source write boundary").
    pub fn check_fresh(&self) -> Result<(), Status> {
        self.fresh()
    }

    pub fn lease(&self) -> Option<&AdmissionLease> {
        self.lease.as_ref()
    }

    pub fn expires_at(&self) -> Option<std::time::Instant> {
        self.lease.as_ref().map(AdmissionLease::deadline)
    }

    pub fn principal(&self) -> &str {
        &self.principal
    }

    /// The current committed decision for this actor on one collection.
    pub fn authorize(
        &self,
        collection: &str,
        action: AccessAction,
    ) -> Result<AccessDecision, Status> {
        self.fresh()?;
        self.store.authorize(&self.principal, collection, action)
    }

    /// The committed owner row for `key`; `disclose` requires current Admin.
    fn owner_row(
        &self,
        key: &LogicalSourceOwner,
        disclose: bool,
    ) -> Result<PreparedSourceOwner, Status> {
        self.store.guarded(|| {
            let tx = self.store.inner.database.begin_read().map_err(storage)?;
            if disclose {
                self.store.read_policy(&tx, &self.principal, key)?;
            }
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
            Ok(held)
        })
    }

    /// Current Admin and the owner ACTIVE for exactly this managed binding:
    /// same key, target, workflow and generation as its preparation, and a
    /// readiness completion derived from these binding bytes. Returns the row
    /// with its committed fence, for the owner to persist before it admits
    /// any write.
    pub fn activated_owner(
        &self,
        binding: &SourceManagedBinding,
    ) -> Result<PreparedSourceOwner, Status> {
        self.fresh()?;
        let verified = VerifiedOwnerCompletion::from_binding(binding)?;
        if binding.authority.as_ref() != Some(&self.store.inner.identity) {
            return Err(Status::failed_precondition(
                "managed binding names another source authority",
            ));
        }
        let ready = binding.preparation.as_ref().expect("validated preparation");
        let key = ready.key.as_ref().expect("validated key");
        let held = self.owner_row(key, true)?;
        if held.phase != PreparedSourceOwnerPhase::Active as i32
            || held.target != ready.target
            || held.workflow_id != ready.workflow_id
            || held.ownership_generation != ready.ownership_generation
            || held.readiness.as_ref().and_then(|r| r.completion.as_ref())
                != Some(&verified.completion)
            || held.activation.is_none()
        {
            return Err(Status::failed_precondition(
                "source owner is not ACTIVE for this binding; READY alone admits no write",
            ));
        }
        Ok(held)
    }

    /// Admission of one source write: current permission for `action` on the
    /// owner's collection and the owner ACTIVE under exactly `write_epoch`.
    /// Held through the source commit, so no activation change can be missed.
    pub fn admit_write(
        &self,
        key: &LogicalSourceOwner,
        write_epoch: u64,
        action: AccessAction,
    ) -> Result<AccessDecision, Status> {
        self.fresh()?;
        contract::key(key, true)?;
        let decision = self.authorize(&key.collection, action)?;
        if decision.workspace != key.workspace {
            return Err(Status::permission_denied(
                "source write permission names another workspace",
            ));
        }
        let held = self.owner_row(key, false)?;
        if held.phase != PreparedSourceOwnerPhase::Active as i32
            || held.activation.as_ref().map(|a| a.write_epoch) != Some(write_epoch)
        {
            return Err(Status::failed_precondition(
                "source owner is not ACTIVE under this write epoch; the fence has moved",
            ));
        }
        Ok(decision)
    }

    /// Current Admin on the owner's collection and the exact pending
    /// preparation. Owner-side work runs under this admission through its
    /// source commit; no command can change the owner or the policy meanwhile.
    pub fn prepared_owner(&self, expected: &PreparedSourceOwner) -> Result<(), Status> {
        self.fresh()?;
        contract::owner(expected).map_err(|e| Status::invalid_argument(e.message()))?;
        self.store.guarded(|| {
            let key = expected
                .key
                .as_ref()
                .ok_or_else(|| missing("prepared owner key"))?;
            let tx = self.store.inner.database.begin_read().map_err(storage)?;
            self.store.read_policy(&tx, &self.principal, key)?;
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
            Ok(())
        })
    }
}

struct AdmittedDecision<'a> {
    _admission: SourceAdmission<'a>,
    decision: AccessDecision,
}

impl AuthorizationGuard for AdmittedDecision<'_> {
    fn decision(&self) -> &AccessDecision {
        &self.decision
    }
}

/// The committed collection policy is the workspace authority for managed
/// hosting: permits resolve against it, and a pin is one `SourceAdmission`.
impl Authorizer for SourceAuthorityStore {
    fn authorize(
        &self,
        principal: &str,
        collection: &str,
        action: AccessAction,
    ) -> Result<AccessDecision, Status> {
        SourceAuthorityStore::authorize(self, principal, collection, action)
    }

    fn subscribe(&self) -> watch::Receiver<u64> {
        self.inner.revisions.subscribe()
    }

    fn pin(&self, expected: &AccessDecision) -> Result<Box<dyn AuthorizationGuard + '_>, Status> {
        let admission = self.admission(&expected.principal)?;
        let action = AccessAction::try_from(expected.action)
            .map_err(|_| Status::permission_denied("invalid authorization action"))?;
        let decision = admission.authorize(&expected.collection, action)?;
        if &decision != expected {
            return Err(crate::error_disclosure::policy_changed());
        }
        Ok(Box::new(AdmittedDecision {
            _admission: admission,
            decision,
        }))
    }
}

/// How a control command reached the store. Admission happens once, at
/// proposal; application of a committed command performs no holder check.
enum OwnerAdmission<'a> {
    /// The general command path: no holder, so no readiness confirmation.
    None,
    /// The hosting adapter's held binding, checked before the transition.
    Holder(&'a VerifiedOwnerCompletion),
    /// A command from the committed log, replayed as committed evidence.
    Committed,
}

/// The verified completion a hosting adapter derives from the managed
/// binding it holds under its exclusive source lock. Only that adapter can
/// construct it; readiness is never confirmed from caller bytes.
pub struct VerifiedOwnerCompletion {
    binding: SourceManagedBinding,
    completion: SourceOwnerCompletion,
}

impl VerifiedOwnerCompletion {
    pub(crate) fn from_binding(binding: &SourceManagedBinding) -> Result<Self, Status> {
        let preparation = binding
            .preparation
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("managed binding has no preparation"))?;
        contract::owner(preparation).map_err(|e| Status::invalid_argument(e.message()))?;
        let target = preparation.target.as_ref().expect("validated target");
        Ok(Self {
            binding: binding.clone(),
            completion: SourceOwnerCompletion {
                format_version: 1,
                binding_sha256: crate::sha256::digest(&binding.encode_to_vec()).to_vec(),
                bound_at_sequence: binding.bound_at_sequence,
                history_id: target.history_id.clone(),
                node_id: target.node_id.clone(),
                storage_incarnation: target.storage_incarnation.clone(),
            },
        })
    }

    /// The completion the confirmation command must carry.
    pub fn completion(&self) -> &SourceOwnerCompletion {
        &self.completion
    }
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
    /// Acquire shared admission for one actor. The actor must be a valid
    /// principal; rights are resolved per collection by the admission's own
    /// methods, so a revoked actor holds a fence that admits nothing.
    pub fn admission(&self, principal: &str) -> Result<SourceAdmission<'_>, Status> {
        if self.raft_hosted() {
            return Err(Status::failed_precondition(
                "source authority is Raft-hosted; a local admission is not authoritative, obtain a leased admission through the host",
            ));
        }
        self.admission_with(principal, None)
    }

    /// A leased admission granted by the Raft host after a linearizable
    /// read; it admits nothing past the lease's interval, and is not
    /// granted at all when that interval has already elapsed.
    pub(crate) fn leased_admission(
        &self,
        principal: &str,
        lease: AdmissionLease,
    ) -> Result<SourceAdmission<'_>, Status> {
        lease.fresh().map_err(|e| {
            Status::failed_precondition(format!(
                "admission interval elapsed before the grant: {}",
                e.message()
            ))
        })?;
        self.admission_with(principal, Some(lease))
    }

    fn admission_with(
        &self,
        principal: &str,
        lease: Option<AdmissionLease>,
    ) -> Result<SourceAdmission<'_>, Status> {
        contract::principal(principal)?;
        let shared = self
            .inner
            .admission
            .read()
            .map_err(|_| storage("admission lock poisoned"))?;
        Ok(SourceAdmission {
            store: self,
            principal: principal.to_string(),
            lease,
            _shared: shared,
        })
    }

    pub fn identity(&self) -> &SourceAuthorityIdentity {
        &self.inner.identity
    }

    fn exclusive(&self) -> Result<std::sync::RwLockWriteGuard<'_, ()>, Status> {
        self.inner
            .admission
            .write()
            .map_err(|_| storage("admission lock poisoned"))
    }

    /// The current committed decision for an actor on one collection: the
    /// same evaluation the general authorizer performs on a policy snapshot.
    pub fn authorize(
        &self,
        principal: &str,
        collection: &str,
        action: AccessAction,
    ) -> Result<AccessDecision, Status> {
        contract::principal(principal)?;
        self.guarded(|| {
            let tx = self.inner.database.begin_read().map_err(storage)?;
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
            crate::authorization::authorize_policy_snapshot(&policy, principal, collection, action)
        })
    }

    /// PREPARED -> READY under the hosting adapter's verified completion. The
    /// general `execute` path refuses this action.
    pub fn confirm_owner_ready(
        &self,
        principal: &str,
        command: &SourceAuthorityCommand,
        verified: &VerifiedOwnerCompletion,
    ) -> Result<SourceAuthorityDecision, Status> {
        self.direct()?;
        if !matches!(command.action, Some(Action::ConfirmReady(_))) {
            return Err(Status::invalid_argument(
                "confirm_owner_ready takes a ConfirmReady command",
            ));
        }
        if verified.binding.authority.as_ref() != Some(&self.inner.identity) {
            return Err(Status::failed_precondition(
                "managed binding names another source authority",
            ));
        }
        self.execute_admitted(principal, command, OwnerAdmission::Holder(verified))
    }

    /// Apply a command that a trusted proposal already admitted: the
    /// committed-log path. A ConfirmReady replays without the managed
    /// binding; every other check runs against the committed rows.
    pub(crate) fn replay_command(
        &self,
        principal: &str,
        command: &SourceAuthorityCommand,
    ) -> Result<SourceAuthorityDecision, Status> {
        self.execute_admitted(principal, command, OwnerAdmission::Committed)
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
        import::create_tables(&tx)?;
        capacity::create_tables(&tx)?;
        tx.commit().map_err(storage)?;
        parent.sync_all().map_err(storage)?;
        store.inner.revisions.send_replace(policy.revision);
        store.inner.applied.send_replace(header.control_revision);
        Ok(store)
    }

    /// The store of a member that will be seeded by the group's snapshot:
    /// a bootstrap image marked pending, on which no log entry applies
    /// until the leader's image replaces it (docs/raft-hosting.md).
    pub(crate) fn create_member(
        path: &Path,
        identity: &SourceAuthorityIdentity,
        policy: &AccessPolicy,
        limits: &SourceAuthorityLimits,
    ) -> Result<Self, Status> {
        let store = Self::create(path, identity, policy, limits)?;
        let mut tx = store.inner.database.begin_write().map_err(storage)?;
        tx.set_durability(Durability::Immediate).map_err(storage)?;
        {
            let mut meta = tx.open_table(META).map_err(storage)?;
            meta.insert(MEMBER_META, MEMBER_PENDING).map_err(storage)?;
        }
        tx.commit().map_err(storage)?;
        Ok(store)
    }

    /// Whether this store is a prepared member still waiting for the
    /// group's first snapshot.
    pub(crate) fn awaiting_snapshot(&self) -> Result<bool, Status> {
        self.guarded(|| {
            let tx = self.inner.database.begin_read().map_err(storage)?;
            let meta = tx.open_table(META).map_err(storage)?;
            Ok(meta.get(MEMBER_META).map_err(storage)?.is_some())
        })
    }

    /// Existing-state recovery only. An empty/missing/corrupt authority never
    /// initializes a fresh identity, policy or retry history.
    pub fn open(path: &Path, expected_identity: &SourceAuthorityIdentity) -> Result<Self, Status> {
        contract::identity(expected_identity)?;
        let (store, _) = Self::open_file(path, expected_identity, false)?;
        import::adopt(&store)?;
        recovery::validate(&store)?;
        let (revision, applied) = {
            let tx = store.inner.database.begin_read().map_err(storage)?;
            let meta = tx.open_table(META).map_err(storage)?;
            let policy: AccessPolicy = contract::decode(
                meta.get("policy")
                    .map_err(storage)?
                    .ok_or_else(|| missing("policy"))?
                    .value(),
            )?;
            let header: SourceAuthorityHeader = contract::decode(
                meta.get("header")
                    .map_err(storage)?
                    .ok_or_else(|| missing("header"))?
                    .value(),
            )?;
            (policy.revision, header.control_revision)
        };
        store.inner.revisions.send_replace(revision);
        store.inner.applied.send_replace(applied);
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
        #[cfg(test)]
        let _handoff = crate::test_support::lock_handoff();
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
                    admission: RwLock::new(()),
                    revisions: watch::Sender::new(0),
                    applied: watch::Sender::new(0),
                    closed: Mutex::new(false),
                    identity: identity.clone(),
                    path: path.to_path_buf(),
                    hosted: AtomicBool::new(false),
                    raft_pending: Mutex::new(None),
                    #[cfg(any(test, feature = "fault-injection"))]
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
        self.direct()?;
        if matches!(command.action, Some(Action::ConfirmReady(_))) {
            return Err(Status::permission_denied(
                "readiness confirmation requires the hosting adapter with the managed binding",
            ));
        }
        self.execute_admitted(principal, command, OwnerAdmission::None)
    }

    fn execute_admitted(
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
        verified: OwnerAdmission<'_>,
    ) -> Result<SourceAuthorityDecision, Status> {
        let mut tx = self.inner.database.begin_write().map_err(storage)?;
        tx.set_durability(Durability::Immediate).map_err(storage)?;
        let decision;
        let mut retried = false;
        'apply: {
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
            let (reserved_bytes, reserved_decisions, import_decisions) = import::reserved(&meta)?;
            if tx
                .open_table(import::IMPORT_OPERATIONS)
                .map_err(storage)?
                .get(operation_bytes.as_slice())
                .map_err(storage)?
                .is_some()
                || tx
                    .open_table(capacity::CAPACITY_OPERATIONS)
                    .map_err(storage)?
                    .get(operation_bytes.as_slice())
                    .map_err(storage)?
                    .is_some()
            {
                return Err(Status::failed_precondition(
                    "command_id was already used by a control import or capacity command",
                ));
            }
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
                decision = operation
                    .decision
                    .ok_or_else(|| missing("retry decision"))?;
                retried = true;
                // Under Raft the entry is consumed: the applied position
                // advances for an exact retry too, with no other change.
                self.write_pending_applied(&mut meta)?;
                break 'apply;
            }
            if header
                .decision_count
                .saturating_add(import_decisions)
                .saturating_add(reserved_decisions)
                >= limits.max_decisions
            {
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
            // Admission of a readiness confirmation: the adapter's held binding
            // must be for exactly this pending owner and the command must carry
            // that binding's completion. Nothing here is recorded; a replica
            // applying the committed command performs no such check.
            if let Some(Action::ConfirmReady(request)) = command.action.as_ref() {
                match verified {
                    OwnerAdmission::Holder(verified) => {
                        if verified.binding.preparation.as_ref() != before.as_ref() {
                            return Err(Status::failed_precondition(
                                "managed binding preparation differs from the committed owner",
                            ));
                        }
                        if request.completion.as_ref() != Some(&verified.completion) {
                            return Err(Status::permission_denied(
                                "readiness confirmation differs from the held binding's completion",
                            ));
                        }
                    }
                    OwnerAdmission::None => {
                        return Err(Status::permission_denied(
                            "readiness confirmation requires the hosting adapter with the managed binding",
                        ))
                    }
                    OwnerAdmission::Committed => {}
                }
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
            if header.payload_bytes.saturating_add(reserved_bytes) > limits.max_payload_bytes {
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
            self.write_pending_applied(&mut meta)?;
        }
        if retried {
            self.finish_retry(tx)?;
            return Ok(decision);
        }
        #[cfg(any(test, feature = "fault-injection"))]
        self.inject(false)?;
        tx.commit().map_err(storage)?;
        self.inner.revisions.send_replace(decision.policy_revision);
        self.publish_applied(decision.control_revision);
        #[cfg(any(test, feature = "fault-injection"))]
        self.inject(true)?;
        Ok(decision)
    }

    /// Arm one process-exit fault for the next control transaction of this
    /// store: control commands, import steps and capacity configuration.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn arm_exit_fault(&self, fault: ExitFault) {
        *self.inner.fault.lock().unwrap() = Some(match fault {
            ExitFault::BeforeCommit => Fault::ExitBeforeCommit,
            ExitFault::AfterCommit => Fault::ExitAfterCommit,
        });
    }

    /// Proposal admission of a readiness confirmation, the same checks the
    /// direct path performs before its transition, as a read.
    pub(crate) fn proposal_admits_readiness(
        &self,
        principal: &str,
        command: &SourceAuthorityCommand,
        verified: &VerifiedOwnerCompletion,
    ) -> Result<(), Status> {
        let Some(Action::ConfirmReady(request)) = command.action.as_ref() else {
            return Err(Status::invalid_argument(
                "proposal_admits_readiness takes a ConfirmReady command",
            ));
        };
        if verified.binding.authority.as_ref() != Some(&self.inner.identity) {
            return Err(Status::failed_precondition(
                "managed binding names another source authority",
            ));
        }
        let key = command
            .key
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("source authority command key is required"))?;
        contract::key(key, true)?;
        contract::principal(principal)?;
        self.guarded(|| {
            let tx = self.inner.database.begin_read().map_err(storage)?;
            self.read_policy(&tx, principal, key)?;
            let owners = tx.open_table(OWNERS).map_err(storage)?;
            let before: Option<PreparedSourceOwner> = owners
                .get(key.encode_to_vec().as_slice())
                .map_err(storage)?
                .map(|v| contract::decode(v.value()))
                .transpose()?;
            if verified.binding.preparation.as_ref() != before.as_ref() {
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
        })
    }

    /// The direct command paths are closed while the store is Raft-hosted:
    /// every mutation then arrives as a committed log entry.
    fn direct(&self) -> Result<(), Status> {
        if self.inner.hosted.load(Ordering::Acquire) {
            return Err(Status::failed_precondition(
                "source authority is Raft-hosted; propose through the host, the direct command path is closed",
            ));
        }
        Ok(())
    }

    /// Write the pending Raft applied position, if any, into the transaction
    /// that applies the command it belongs to.
    fn write_pending_applied(
        &self,
        meta: &mut redb::Table<&'static str, &'static [u8]>,
    ) -> Result<(), Status> {
        match self.inner.raft_pending.lock().unwrap().as_ref() {
            Some(applied) => {
                Self::refuse_unseeded(meta)?;
                meta.insert(RAFT_META, applied.encode_to_vec().as_slice())
                    .map_err(storage)?;
                Ok(())
            }
            None if self.raft_hosted() => Err(Status::failed_precondition(
                "Raft-hosted store applied a command outside the state machine; no applied position is pending",
            )),
            None => Ok(()),
        }
    }

    /// A prepared member applies nothing before the group's snapshot has
    /// replaced its genesis image; replaying onto that image would diverge
    /// from the group silently.
    fn refuse_unseeded(meta: &redb::Table<&'static str, &'static [u8]>) -> Result<(), Status> {
        if meta.get(MEMBER_META).map_err(storage)?.is_some() {
            return Err(Status::failed_precondition(
                "prepared member store awaits the group's first snapshot; log entries do not apply to its genesis image",
            ));
        }
        Ok(())
    }

    /// An exact retry changes no application state; it commits only when a
    /// Raft applied position was written for the consumed entry.
    fn finish_retry(&self, tx: redb::WriteTransaction) -> Result<(), Status> {
        if self.inner.raft_pending.lock().unwrap().is_some() {
            tx.commit().map_err(storage)
        } else {
            tx.abort().map_err(storage)
        }
    }

    /// Mark the store as Raft-hosted; the direct command paths refuse from
    /// here on. The host owns proposal admission.
    pub(crate) fn set_raft_hosted(&self) {
        self.inner.hosted.store(true, Ordering::Release);
    }

    pub(crate) fn raft_hosted(&self) -> bool {
        self.inner.hosted.load(Ordering::Acquire)
    }

    pub(crate) fn path(&self) -> &Path {
        &self.inner.path
    }

    /// Run one committed-replay application with `applied` recorded in the
    /// same transaction. The state machine applies entries one at a time.
    pub(crate) fn raft_apply<T>(
        &self,
        applied: RaftApplied,
        run: impl FnOnce() -> Result<T, Status>,
    ) -> Result<T, Status> {
        *self.inner.raft_pending.lock().unwrap() = Some(applied);
        let result = run();
        *self.inner.raft_pending.lock().unwrap() = None;
        result
    }

    /// Record an applied position with no application change: blank and
    /// membership entries, and entries refused at their envelope.
    pub(crate) fn apply_raft_position(&self, applied: &RaftApplied) -> Result<(), Status> {
        let _exclusive = self.exclusive()?;
        self.guarded(|| {
            let mut tx = self.inner.database.begin_write().map_err(storage)?;
            tx.set_durability(Durability::Immediate).map_err(storage)?;
            {
                let mut meta = tx.open_table(META).map_err(storage)?;
                Self::refuse_unseeded(&meta)?;
                meta.insert(RAFT_META, applied.encode_to_vec().as_slice())
                    .map_err(storage)?;
            }
            tx.commit().map_err(storage)
        })
    }

    /// The applied Raft position recorded with the last applied entry.
    pub(crate) fn raft_applied(&self) -> Result<Option<RaftApplied>, Status> {
        self.guarded(|| {
            let tx = self.inner.database.begin_read().map_err(storage)?;
            let meta = tx.open_table(META).map_err(storage)?;
            meta.get(RAFT_META)
                .map_err(storage)?
                .map(|v| contract::decode(v.value()))
                .transpose()
        })
    }

    /// Hold every store transaction off while `run` reads the file: the
    /// snapshot builder copies a consistent image. The applied position is
    /// read under the same hold, so image and position agree.
    pub(crate) fn quiesced<T>(
        &self,
        run: impl FnOnce(&Path, Option<RaftApplied>) -> Result<T, Status>,
    ) -> Result<T, Status> {
        let _exclusive = self.exclusive()?;
        self.guarded(|| {
            let applied = {
                let tx = self.inner.database.begin_read().map_err(storage)?;
                let meta = tx.open_table(META).map_err(storage)?;
                meta.get(RAFT_META)
                    .map_err(storage)?
                    .map(|v| contract::decode(v.value()))
                    .transpose()?
            };
            run(&self.inner.path, applied)
        })
    }

    /// Replace this store's file with a verified staged image and reopen.
    /// Needs exclusive ownership of the handle: an outstanding clone keeps
    /// the old file open, so the swap refuses by name rather than racing it.
    /// On refusal the handle comes back unchanged, so the host keeps its
    /// usable store. Subscribers of the policy and applied watches stay
    /// attached: the channels move to the reopened store. `Lost` means the
    /// old handle is closed and the caller must reopen from durable state.
    pub(crate) fn replace_from(self, staged: &Path) -> Result<Self, ReplaceFailure> {
        let identity = self.inner.identity.clone();
        let path = self.inner.path.clone();
        let hosted = self.raft_hosted();
        // Short-lived read clones drain quickly; anything longer is refused.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while Arc::strong_count(&self.inner) != 1 {
            if std::time::Instant::now() >= deadline {
                return Err(ReplaceFailure::Refused(
                    self,
                    Status::failed_precondition(
                        "source authority store has outstanding handles; snapshot install needs exclusive ownership",
                    ),
                ));
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let inner = match Arc::try_unwrap(self.inner) {
            Ok(inner) => inner,
            Err(shared) => {
                return Err(ReplaceFailure::Refused(
                    Self { inner: shared },
                    Status::failed_precondition(
                        "source authority handle count changed during a snapshot swap",
                    ),
                ))
            }
        };
        let Inner {
            database,
            revisions,
            applied,
            _file_lock,
            ..
        } = inner;
        drop(database);
        drop(_file_lock);
        let reopen = (|| -> Result<Self, Status> {
            std::fs::rename(staged, &path).map_err(storage)?;
            let parent = path
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new("."));
            File::open(parent)
                .and_then(|d| d.sync_all())
                .map_err(storage)?;
            Self::open(&path, &identity)
        })();
        let mut store = match reopen {
            Ok(store) => store,
            // The old file may still be in place (rename failed) or the new
            // one may not open; either way this handle is gone.
            Err(error) => return Err(ReplaceFailure::Lost(error)),
        };
        let fresh = Arc::get_mut(&mut store.inner).expect("freshly opened store has one handle");
        revisions.send_replace(*fresh.revisions.borrow());
        applied.send_replace(*fresh.applied.borrow());
        fresh.revisions = revisions;
        fresh.applied = applied;
        if hosted {
            store.set_raft_hosted();
        }
        Ok(store)
    }

    /// Wake map-feed subscribers only when the applied revision moved: a
    /// recorded rejection commits a decision, not a new revision.
    fn publish_applied(&self, revision: u64) {
        self.inner.applied.send_if_modified(|current| {
            if *current == revision {
                false
            } else {
                *current = revision;
                true
            }
        });
    }

    #[cfg(any(test, feature = "fault-injection"))]
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
mod admission_tests;
#[cfg(test)]
mod capacity_tests;
#[cfg(test)]
mod import_tests;
#[cfg(test)]
mod map_feed_tests;
#[cfg(all(test, feature = "net"))]
mod relay_map_tests;
#[cfg(test)]
mod retirement_tests;

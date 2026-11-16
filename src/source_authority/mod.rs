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

mod admission;
mod authorizer;
mod capacity;
mod completion;
mod contract;
mod control;
mod execute;
mod lifecycle;
mod map_feed;
mod raft_host;
mod read;
mod recovery;
#[cfg(feature = "net")]
mod relay_map;
mod transition;

#[cfg(any(test, feature = "fault-injection"))]
pub use self::ExitFault as SourceExitFault;
pub use admission::{AdmissionLease, LeasedAdmission, SourceAdmission};
pub use capacity::{
    PlanningContext, MAX_CAPACITY_BYTES, MAX_CAPACITY_RECORDS, MAX_CAPACITY_REPORTERS,
};
use completion::OwnerAdmission;
pub use completion::VerifiedOwnerCompletion;
pub use map_feed::MapConsumer;
#[cfg(feature = "net")]
pub use relay_map::AuthorityMapSource;

const META: TableDefinition<&str, &[u8]> = TableDefinition::new("source_authority_meta");
/// Every table of a store: meta, owners, decisions and workflows, the seven
/// control tables and the four capacity tables.
const STORE_TABLES: usize = 15;
/// Meta key of the Raft applied position.
pub(crate) const RAFT_META: &str = "raft";
/// Present only in a store prepared for a member that has not received its
/// first snapshot: log entries refuse to apply until the leader's image
/// replaces the file, so a member never replays onto its own genesis.
pub(crate) const MEMBER_META: &str = "member";
#[cfg_attr(not(feature = "raft"), allow(dead_code))]
const MEMBER_PENDING: &[u8] = b"awaiting-snapshot";
const OWNERS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("source_authority_owners");
const DECISIONS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("source_authority_decisions");
const WORKFLOWS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("source_authority_workflows");
const CLOSED: &str = "source authority storage failed; close every clone and reopen existing state before continuing";

#[cfg(any(test, feature = "fault-injection"))]
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone, Copy, PartialEq, Eq)]
#[allow(clippy::enum_variant_names)]
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

/// The open database and the exclusive lock on its file, replaced together
/// by a snapshot install (`replace_from`).
struct Backing {
    database: Database,
    // Keep the same open-file description locked until after Database drops.
    _file_lock: File,
}

impl std::ops::Deref for Backing {
    type Target = Database;
    fn deref(&self) -> &Database {
        &self.database
    }
}

struct Inner {
    // Interior mutability for the in-place swap only: every operation runs
    // under `closed`, and the swap holds `closed` too, so no transaction
    // ever crosses a swap. Readers take this guard for the instant a
    // transaction begins (transactions do not borrow the database).
    backing: RwLock<Backing>,
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
    #[cfg_attr(not(feature = "raft"), allow(dead_code))]
    path: PathBuf,
    // Raft hosting: while hosted, the direct command paths refuse and only
    // the committed-replay paths apply, each writing the pending applied
    // position in its own transaction.
    hosted: AtomicBool,
    raft_pending: Mutex<Option<RaftApplied>>,
    #[cfg(any(test, feature = "fault-injection"))]
    fault: Mutex<Option<Fault>>,
}

impl Inner {
    fn database(&self) -> std::sync::RwLockReadGuard<'_, Backing> {
        self.backing
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[derive(Clone)]
pub struct SourceAuthorityStore {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for SourceAuthorityStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceAuthorityStore")
            .field("group_id", &self.inner.identity.group_id)
            .finish_non_exhaustive()
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
    pub fn identity(&self) -> &SourceAuthorityIdentity {
        &self.inner.identity
    }

    fn exclusive(&self) -> Result<std::sync::RwLockWriteGuard<'_, ()>, Status> {
        self.inner
            .admission
            .write()
            .map_err(|_| storage("admission lock poisoned"))
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

    /// Arm one process-exit fault for the next control transaction of this
    /// store: control commands, import steps and capacity configuration.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn arm_exit_fault(&self, fault: ExitFault) {
        *self.inner.fault.lock().unwrap() = Some(match fault {
            ExitFault::BeforeCommit => Fault::ExitBeforeCommit,
            ExitFault::AfterCommit => Fault::ExitAfterCommit,
        });
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
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod admission_tests;
#[cfg(test)]
mod capacity_tests;
#[cfg(test)]
pub(crate) mod control_tests;
#[cfg(test)]
mod map_feed_tests;
#[cfg(all(test, feature = "net"))]
mod relay_map_tests;

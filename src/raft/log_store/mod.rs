//! The Raft log store: a dedicated redb database beside the source authority
//! store, holding the group header (identity, node id, vote, committed and
//! purged positions) and the consecutive log entries as typed envelopes.
//! Every write is one immediate transaction; the flush callback fires only
//! after the commit. Truncate and purge are protocol boundaries, never file
//! cleanup guessed from an index.
mod open;
mod purge;
mod storage;

use super::types::{
    entry_from_proto, entry_to_proto, log_id_from_proto, log_id_to_proto, vote_from_proto,
    vote_to_proto, ControlRaft, NodeId,
};
use crate::pb::storage::{RaftEntry, RaftLogHeader, SourceAuthorityIdentity};
use openraft::storage::{LogFlushed, LogState, RaftLogStorage};
use openraft::{
    AnyError, Entry, ErrorSubject, ErrorVerb, LogId, RaftLogReader, StorageError, StorageIOError,
    Vote,
};
use prost::Message;
use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition};
use std::fs::{File, OpenOptions};
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tonic::Status;

const LOG_META: TableDefinition<&str, &[u8]> = TableDefinition::new("raft_log_meta");
const LOG: TableDefinition<u64, &[u8]> = TableDefinition::new("raft_log_entries");
const HEADER: &str = "header";
/// The library's purge position a bounded purge has not reached yet,
/// as a `RaftLogId`; absent when no purge is deferred.
const DEFERRED_PURGE: &str = "purge_deferred";
/// One entry may not exceed the command bound plus its envelope.
const MAX_ENTRY_BYTES: usize = 2 * 1024 * 1024;

struct Inner {
    database: Database,
    group: SourceAuthorityIdentity,
    node_id: NodeId,
    // Serializes every write: votes and entries share one order.
    write: Mutex<()>,
    /// The highest index the library asked to purge to in this run. A
    /// purge deferred by the store's position is completed at an append
    /// only up to this: the library records a purge as done before it asks
    /// for it, so a completion up to here removes entries it has given up,
    /// and a completion past here would remove entries it still reads
    /// (across a restart it reads the log state afresh, so a record from
    /// an earlier run completes at start or when it is asked again).
    requested_purge: Mutex<Option<u64>>,
    _file_lock: File,
    // A process-exit fault armed for the next purge transaction (the
    // crash target, `tests/control_raft_crash_faults.rs`).
    #[cfg(any(test, feature = "fault-injection"))]
    fault: Mutex<Option<crate::source_authority::ExitFault>>,
    // Dropped last after `database` and `_file_lock` close their file
    // descriptors and release the exclusive OS lock.
    released: tokio::sync::watch::Sender<()>,
}

/// The purge state of a raft log on disk, for crash-recovery harnesses.
#[cfg(any(test, feature = "fault-injection"))]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RaftLogInspection {
    /// The index the log records as purged.
    pub purged: Option<u64>,
    /// The library's purge position recorded as deferred.
    pub deferred: Option<u64>,
    /// The first and last entry the log holds.
    pub first: Option<u64>,
    pub last: Option<u64>,
}

/// Where a settle runs: at start, before the library reads the log state,
/// or at an append, in a run whose purge point the library holds.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Settle {
    Start,
    Append,
}

/// Cloneable handle; clones share the store and the write order.
#[derive(Clone)]
pub struct RaftLogStore {
    // The state machine's store, once hosted: a purge never passes its
    // applied position, so a snapshot install the state machine refuses
    // leaves the log consistent with the store it kept (the library purges
    // for an incoming snapshot before the state machine has installed it).
    // Declared before `inner` so replication readers drop their hosted
    // store handle before `inner` signals log release.
    floor: Option<super::state_machine::SharedStore>,
    inner: Arc<Inner>,
}

fn io<E: std::fmt::Display>(
    subject: ErrorSubject<NodeId>,
    verb: ErrorVerb,
    error: E,
) -> StorageError<NodeId> {
    StorageIOError::new(subject, verb, AnyError::error(error.to_string())).into()
}

fn storage_status(error: impl std::fmt::Display) -> Status {
    Status::internal(format!("raft log storage: {error}"))
}

/// What the hosted store has applied, as a bound on purges.
enum Floor {
    /// No store is bound (the log alone, in tests).
    Unbound,
    /// The store is between swaps or has applied nothing.
    Nothing,
    Applied(LogId<NodeId>),
}

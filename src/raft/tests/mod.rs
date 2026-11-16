//! In-process evidence for the Raft host: envelopes, the log store, the
//! state machine's snapshots and the admission the host grants.
mod admission;
mod envelopes;
mod fixtures;
mod purge;
mod purge_restart;
mod single_node;
mod snapshot_install;
mod snapshot_receive;
mod specification;

use super::log_store::RaftLogStore;
use super::state_machine::{
    format_snapshot_id, parse_snapshot_id, published_generation, ControlStateMachine,
    ImageSignature, SnapshotCompletion,
};
use super::types::*;
use super::{HostConfig, RaftHost};
use crate::pb::storage::LogicalSourceOwner;
use crate::pb::storage::{
    raft_entry::Payload, source_authority_command::Action, PrepareSourceOwner,
    PreparedSourceOwnerPhase, RaftEntry, RaftMembership, RaftNode, RaftProposal, RaftVoterSet,
    SourceAuthorityCommand, SourceAuthorityIdentity, SourceAuthorityLimits, SourceResidency,
    SourceStorageTarget,
};
use crate::pb::{AccessAction, AccessPolicy, CollectionGrant, CollectionResource};
use openraft::storage::{RaftLogStorage, RaftStateMachine};
use openraft::{BasicNode, Entry, EntryPayload, LeaderId, LogId, Membership, RaftLogReader, Vote};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tonic::Code;

use fixtures::{
    blanks, bootstrap_application_is_ready, log_id, proposal, replica_machine, snapshot_now,
    snapshots_after, Published,
};
pub(super) use fixtures::{config, identity, limits, owner, policy, prepare, Directory};

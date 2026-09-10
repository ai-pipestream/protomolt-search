//! Raft hosting of the source authority (docs/raft-hosting.md): typed
//! envelopes, the redb log store, the state machine over the store's
//! committed-replay paths and the in-process host.
//!
//! # Capability boundary
//!
//! Raw submission and committed application are not reachable by product
//! callers (review finding R1, repaired in 1f4ea13). The doctests below are
//! `compile_fail`: each names one surface that must stay out of reach, and
//! the boundary is checked at compile time rather than by a runtime
//! reproduction (`docs/control-authority-test-harness.md`).
//!
//! The raw proposal path is private; every public entry admits its command
//! first (`propose_command`, `propose_confirm_ready`, `propose_import`,
//! `propose_capacity_configure`, `propose_capacity_transition`):
//!
//! ```compile_fail
//! use pipestream_search::pb::storage::RaftProposal;
//! use pipestream_search::raft::RaftHost;
//! async fn raw(host: &RaftHost, proposal: RaftProposal) {
//!     let _ = host.propose(proposal).await;
//! }
//! ```
//!
//! The library handle is not exposed, so `client_write` cannot bypass
//! admission:
//!
//! ```compile_fail
//! use pipestream_search::raft::RaftHost;
//! fn handle(host: &RaftHost) {
//!     let _ = host.raft();
//! }
//! ```
//!
//! The committed-replay paths of the store apply only from the state
//! machine; a handle from `RaftHost::store` cannot reach them:
//!
//! ```compile_fail
//! use pipestream_search::pb::storage::SourceAuthorityCommand;
//! use pipestream_search::source_authority::SourceAuthorityStore;
//! fn replay(store: &SourceAuthorityStore, command: &SourceAuthorityCommand) {
//!     let _ = store.replay_command("alice", command);
//! }
//! ```
//!
//! ```compile_fail
//! use pipestream_search::pb::storage::ControlImportCommand;
//! use pipestream_search::source_authority::SourceAuthorityStore;
//! fn replay(store: &SourceAuthorityStore, command: &ControlImportCommand) {
//!     let _ = store.replay_control_import("alice", command);
//! }
//! ```
//!
//! ```compile_fail
//! use pipestream_search::pb::storage::CapacityConfigureCommand;
//! use pipestream_search::source_authority::SourceAuthorityStore;
//! fn replay(store: &SourceAuthorityStore, command: &CapacityConfigureCommand) {
//!     let _ = store.replay_capacity_configure("alice", command);
//! }
//! ```
//!
//! ```compile_fail
//! use pipestream_search::pb::storage::CapacityTransition;
//! use pipestream_search::source_authority::SourceAuthorityStore;
//! fn replay(store: &SourceAuthorityStore, transition: &CapacityTransition) {
//!     let _ = store.replay_capacity_transition("alice", transition);
//! }
//! ```
//!
//! The state machine and its store slot are constructed by the host only:
//!
//! ```compile_fail
//! use pipestream_search::raft::ControlStateMachine;
//! use pipestream_search::source_authority::SourceAuthorityStore;
//! fn machine(store: SourceAuthorityStore) {
//!     let _ = ControlStateMachine::new(store, std::path::Path::new("snapshots"), 1 << 30);
//! }
//! ```
//!
//! ```compile_fail
//! use pipestream_search::raft::ControlStateMachine;
//! fn slot(machine: &ControlStateMachine) {
//!     let _ = machine.shared_store();
//! }
//! ```
//!
//! The log store is created, opened and written by the host only:
//!
//! ```compile_fail
//! use pipestream_search::pb::storage::SourceAuthorityIdentity;
//! use pipestream_search::raft::RaftLogStore;
//! fn log(identity: &SourceAuthorityIdentity) {
//!     let _ = RaftLogStore::create(std::path::Path::new("raft-log.redb"), identity, 1);
//! }
//! ```
pub mod host;
pub mod log_store;
#[cfg(feature = "tls")]
pub mod operator;
pub mod state_machine;
#[cfg(feature = "tls")]
pub mod transport;
pub mod types;

#[cfg(feature = "tls")]
pub use host::ClusterTransport;
#[cfg(any(test, feature = "fault-injection"))]
pub use host::{inspect_node, NodeInspection};
pub use host::{HostConfig, NoNetwork, RaftHost};
#[cfg(any(test, feature = "fault-injection"))]
pub use log_store::RaftLogInspection;
pub use log_store::RaftLogStore;
pub use state_machine::{ControlStateMachine, SnapshotBuildReport};
#[cfg(feature = "tls")]
pub use transport::{PeerRejection, PeerRejections, SnapshotRejection, SnapshotRejections};
pub use types::ControlRaft;

#[cfg(test)]
mod tests;
#[cfg(all(test, feature = "tls"))]
mod transport_tests;

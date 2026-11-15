//! The capability boundary of Raft hosting: raw submission and committed
//! application are not reachable by product callers. Each example below is
//! `compile_fail` and names one surface that must stay out of reach, so the
//! boundary is checked when the doctests compile rather than by a runtime
//! reproduction.
//!
//! The raw proposal path is private; every public entry admits its command
//! first (`propose_command`, `propose_confirm_ready`, `propose_install`,
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
//! use pipestream_search::pb::storage::ControlInstallCommand;
//! use pipestream_search::source_authority::SourceAuthorityStore;
//! fn replay(store: &SourceAuthorityStore, command: &ControlInstallCommand) {
//!     let _ = store.replay_control_install("alice", command);
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

//! Raft hosting of the source authority: typed envelopes, the redb log
//! store, the state machine over the store's committed-replay paths, and
//! the in-process host. Raw submission and committed application are not
//! reachable by product callers; the `boundary` module pins that at
//! compile time.

mod boundary;
pub mod host;
pub mod log_store;
#[cfg(feature = "tls")]
pub mod operator;
#[cfg(all(feature = "raft", feature = "tls"))]
pub mod operator_service;
pub mod reasons;
#[cfg(feature = "tls")]
pub mod rejection_log;
#[cfg(any(test, feature = "fault-injection"))]
mod snapshot_gates;
pub mod state_machine;
#[cfg(feature = "tls")]
pub mod transport;
pub mod types;

pub use crate::source_authority::LeasedAdmission;
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

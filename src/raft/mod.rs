//! Raft hosting of the source authority (docs/raft-hosting.md): typed
//! envelopes, the redb log store, the state machine over the store's
//! committed-replay paths and the in-process host.
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
pub use host::{HostConfig, NoNetwork, RaftHost};
pub use log_store::RaftLogStore;
pub use state_machine::ControlStateMachine;
pub use types::ControlRaft;

#[cfg(test)]
mod tests;
#[cfg(all(test, feature = "tls"))]
mod transport_tests;

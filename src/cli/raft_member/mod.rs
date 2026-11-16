//! The Raft member this process may serve as, and the services it adds
//! to the process's listeners. A build without the `raft` and `tls`
//! features refuses a Raft configuration and adds nothing.

#[cfg(not(all(feature = "raft", feature = "tls")))]
mod disabled;
#[cfg(all(feature = "raft", feature = "tls"))]
mod enabled;
#[cfg(all(feature = "raft", feature = "tls"))]
mod services;

#[cfg(not(all(feature = "raft", feature = "tls")))]
pub(super) use disabled::RaftSurfaces;
#[cfg(all(feature = "raft", feature = "tls"))]
pub(super) use enabled::RaftSurfaces;

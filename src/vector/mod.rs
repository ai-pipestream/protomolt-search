//! Backend-neutral vector index contract.
//!
//! The search product owns document identity, filtering, hybrid semantics,
//! generation cutover, and public quality claims. A vector backend owns its
//! encoding, native score, query execution, and persisted image. Runtime code
//! outside this module talks only to [`VectorIndex`]; the concrete TurboVec
//! dependency is contained in [`embedded_turbovec`].

mod backend;
mod embedded_turbovec;
mod filter;
mod index;
mod options;
mod provider;
mod results;
#[cfg(test)]
mod tests;

pub use backend::*;
pub use filter::*;
pub use index::*;
pub use options::*;
pub use provider::*;
pub use results::*;

/// Stable identifier for the currently shipped embedded backend.
pub const EMBEDDED_TURBOVEC: &str = "embedded-turbovec";

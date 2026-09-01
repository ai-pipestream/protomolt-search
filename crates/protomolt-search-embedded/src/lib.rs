//! Mobile-facing package for the embedded Protomolt Search runtime.
//!
//! The implementation remains in `pipestream-search` so the server and
//! embedded products execute one engine. This package gives Android/iOS host
//! bridges a stable product-named dependency without importing the server
//! binary.

pub use pipestream_search::coordinator::FanoutLimits;
pub use pipestream_search::embedded::*;
pub use pipestream_search::node::NodeConfig;
pub use pipestream_search::{analyzer, bm25, pb, phrases, quality};

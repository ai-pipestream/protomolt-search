//! Generated protobuf bindings (see `build.rs` and `proto/`).

// Generated code: prost derives and tonic stubs are not held to this crate's
// lint bar.
#![allow(clippy::all, clippy::pedantic, clippy::nursery, missing_docs)]

tonic::include_proto!("turbovec.search.v1");

/// The vendored analysis-sidecar API (`proto/ai/pipestream/opennlp/analysis/v1/analysis.proto`,
/// copied from the grpc-opennlp-analysis repo — see the file header).
pub mod analysis {
    tonic::include_proto!("ai.pipestream.opennlp.analysis.v1");
}

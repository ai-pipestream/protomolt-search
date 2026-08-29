//! Generated protobuf bindings (see `build.rs` and `proto/`).

// Generated code: prost derives and tonic stubs are not held to this crate's
// lint bar.
#![allow(clippy::all, clippy::pedantic, clippy::nursery, missing_docs)]

tonic::include_proto!("ai.pipestream.search.v1");

/// The vendored analysis-sidecar API (`proto/ai/pipestream/opennlp/analysis/v1/analysis.proto`,
/// copied from the grpc-opennlp-analysis repo — see the file header).
pub mod analysis {
    tonic::include_proto!("ai.pipestream.opennlp.analysis.v1");
}

/// The vendored Text Embeddings Inference API (`proto/tei/v1/tei.proto`,
/// from huggingface/text-embeddings-inference v1.9.3 — see the file header).
pub mod tei {
    tonic::include_proto!("tei.v1");
}

/// The vendored descriptor exchange contract
/// (`proto/ai/pipestream/proto/schema/registry/v1/descriptor_exchange.proto`,
/// byte-identical to protomolt — `scripts/check-vendored-protos.sh`).
/// This engine consumes the exchange service as one gRPC client among
/// any; descriptor bytes stay opaque until the mapping layer derives a
/// plan from them (`docs/descriptor-mappings.md`).
pub mod exchange {
    tonic::include_proto!("ai.pipestream.proto.schema.registry.v1");
}

/// The vendored indexing-hint extension vocabulary
/// (`proto/ai/pipestream/proto/index/hints/v1/indexing_hints.proto`,
/// byte-identical to protomolt). Derivation decodes the
/// `(ai.pipestream.proto.index.hints.v1.index)` field-option extension
/// into these types; a proto annotated for protomolt's indexers is
/// understood here without modification.
pub mod hints {
    tonic::include_proto!("ai.pipestream.proto.index.hints.v1");
}

/// The per-shard write-ahead log envelope
/// (`proto/ai/pipestream/search/wal/v1/wal.proto`).
pub mod wal {
    pub mod v1 {
        tonic::include_proto!("ai.pipestream.search.wal.v1");
    }
    pub use v1::*;
}

/// Shim for the generated wal code: it references the search types it
/// reuses as `super::super::v1::X` (their package path), while this module
/// includes them flat — re-export the referenced ones under that name.
pub mod v1 {
    pub use super::{AddDocumentsRequest, AddVectorsRequest};
}

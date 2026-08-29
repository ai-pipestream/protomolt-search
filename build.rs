//! Wire codegen for the protobuf APIs.
//!
//! Regenerates the tonic stubs for `ai.pipestream.search.v1` (this service),
//! the `ai.pipestream.search.wal.v1` WAL envelope, and
//! `ai.pipestream.opennlp.analysis.v1` (the vendored analysis-sidecar API)
//! into `OUT_DIR` whenever a proto changes; `src/pb.rs` pulls them in with
//! `tonic::include_proto!`.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &[
                "proto/ai/pipestream/search/v1/search.proto",
                "proto/ai/pipestream/search/wal/v1/wal.proto",
                "proto/ai/pipestream/opennlp/analysis/v1/analysis.proto",
                "proto/tei/v1/tei.proto",
                // Vendored from protomolt, byte-identical (see
                // scripts/check-vendored-protos.sh): the descriptor
                // exchange contract this engine consumes as a client, and
                // the indexing-hint extension vocabulary the mapping
                // derivation reads off field options.
                "proto/ai/pipestream/proto/schema/registry/v1/descriptor_exchange.proto",
                "proto/ai/pipestream/proto/index/hints/v1/indexing_hints.proto",
            ],
            &["proto"],
        )?;
    Ok(())
}

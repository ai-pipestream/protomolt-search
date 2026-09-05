//! Wire codegen for the protobuf APIs.
//!
//! Regenerates the tonic stubs for `ai.protomolt.search.v1` (this service),
//! the `ai.protomolt.search.wal.v1` WAL envelope, and
//! `ai.pipestream.opennlp.analysis.v1` (the vendored analysis-sidecar API)
//! into `OUT_DIR` whenever a proto changes; `src/pb.rs` pulls them in with
//! `tonic::include_proto!`.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The generated clients' `connect()` helpers name tonic's transport;
    // they exist only when the `net` feature builds it.
    let net = std::env::var_os("CARGO_FEATURE_NET").is_some();
    // The console's JSON facade transcodes from this descriptor set at
    // run time (src/console.rs), so a new RPC needs no facade change.
    let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR")?);
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .build_transport(net)
        .file_descriptor_set_path(out_dir.join("search_descriptor.bin"))
        .compile_protos(
            &[
                "proto/ai/protomolt/search/v1/search.proto",
                "proto/ai/protomolt/search/v1/authorization.proto",
                "proto/ai/protomolt/search/v1/source.proto",
                "proto/ai/protomolt/search/v1/document_write.proto",
                "proto/ai/protomolt/search/v1/document_identity.proto",
                "proto/ai/protomolt/search/storage/v1/document_catalog.proto",
                "proto/ai/protomolt/search/v1/schema_report.proto",
                "proto/ai/protomolt/search/storage/v1/source_archive.proto",
                "proto/ai/protomolt/search/mobile/v1/mobile.proto",
                "proto/ai/protomolt/search/wal/v1/wal.proto",
                "proto/ai/pipestream/opennlp/analysis/v1/analysis.proto",
                "proto/tei/v1/tei.proto",
                // Vendored from protomolt, byte-identical (see
                // scripts/check-vendored-protos.sh): the descriptor
                // exchange contract this engine consumes as a client, and
                // the indexing-hint extension vocabulary the mapping
                // derivation reads off field options.
                "proto/ai/protomolt/proto/schema/registry/v1/descriptor_exchange.proto",
                "proto/ai/protomolt/proto/index/hints/v1/indexing_hints.proto",
            ],
            &["proto"],
        )?;
    Ok(())
}

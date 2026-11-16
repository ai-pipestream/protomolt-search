//! Wire codegen for the protobuf APIs.
//!
//! Regenerates the tonic stubs for `ai.protomolt.search.v1` (this service),
//! `ai.protomolt.search.storage.v1` (persisted control and vocabulary
//! snapshots), and the `ai.protomolt.search.wal.v1` WAL envelope
//! into `OUT_DIR` whenever a proto changes; `src/pb.rs` pulls them in with
//! `tonic::include_proto!`.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The generated clients' `connect()` helpers name tonic's transport;
    // they exist only when the `net` feature builds it.
    let net = std::env::var_os("CARGO_FEATURE_NET").is_some();
    // The console's JSON facade transcodes from this descriptor set at
    // run time (src/console.rs), so a new RPC needs no facade change.
    let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR")?);
    // These vendored comments spell placeholders as `<name>`, which rustdoc
    // reads as unclosed HTML tags; the protos themselves cannot be edited.
    let mut prost = tonic_prost_build::Config::new();
    prost.disable_comments([
        ".ai.protomolt.proto.index.hints.v1.ChunkingPolicy.vector_field",
        ".ai.protomolt.proto.index.hints.v1.BlockRole.BLOCK_ROLE_CHUNK_ID",
    ]);
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .build_transport(net)
        .file_descriptor_set_path(out_dir.join("search_descriptor.bin"))
        .compile_with_config(
            prost,
            &[
                "proto/ai/protomolt/search/v1/search.proto",
                "proto/google/rpc/status.proto",
                "proto/ai/protomolt/search/v1/authorization.proto",
                "proto/ai/protomolt/search/v1/query_cursor.proto",
                "proto/ai/protomolt/search/v1/source.proto",
                "proto/ai/protomolt/search/v1/document_write.proto",
                "proto/ai/protomolt/search/v1/raft_operator.proto",
                "proto/ai/protomolt/search/v1/document_projection.proto",
                "proto/ai/protomolt/search/v1/replay.proto",
                "proto/ai/protomolt/search/storage/v1/replay_journal.proto",
                "proto/ai/protomolt/search/v1/document_identity.proto",
                "proto/ai/protomolt/search/storage/v1/document_catalog.proto",
                "proto/ai/protomolt/search/v1/schema_report.proto",
                "proto/ai/protomolt/search/storage/v1/source_archive.proto",
                "proto/ai/protomolt/search/storage/v1/source_authority.proto",
                "proto/ai/protomolt/search/storage/v1/control_state.proto",
                "proto/ai/protomolt/search/storage/v1/capacity_observations.proto",
                "proto/ai/protomolt/search/storage/v1/published_map.proto",
                "proto/ai/protomolt/search/storage/v1/raft_control.proto",
                "proto/ai/protomolt/search/storage/v1/raft_transport.proto",
                "proto/ai/protomolt/search/storage/v1/vocab_snapshot.proto",
                "proto/ai/protomolt/search/mobile/v1/mobile.proto",
                "proto/ai/protomolt/search/wal/v1/wal.proto",
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

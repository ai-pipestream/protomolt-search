//! Wire codegen for the protobuf APIs.
//!
//! Regenerates the tonic stubs for `turbovec.search.v1` (this service) and
//! `ai.pipestream.opennlp.analysis.v1` (the vendored analysis-sidecar API)
//! into `OUT_DIR` whenever a proto changes; `src/pb.rs` pulls them in with
//! `tonic::include_proto!`.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &[
                "proto/turbovec/search/v1/search.proto",
                "proto/ai/pipestream/opennlp/analysis/v1/analysis.proto",
            ],
            &["proto"],
        )?;
    Ok(())
}

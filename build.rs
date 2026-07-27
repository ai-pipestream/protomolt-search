//! Wire codegen for the turbovec.search.v1 protobuf API.
//!
//! Regenerates `turbovec.search.v1.rs` (client + server stubs via tonic)
//! into `OUT_DIR` whenever the proto changes; `src/pb.rs` pulls it in with
//! `tonic::include_proto!`.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/turbovec/search/v1/search.proto"], &["proto"])?;
    Ok(())
}

//! Ask a running analysis sidecar what it serves: `GetCapabilities` as
//! the jar answers it, not as the checked-in proto or source says
//! (`docs/dual-cased.md`: an open port is not the jar). Exits non-zero
//! when the dual term identity is not served, so a deploy script can
//! gate on it.
//!
//!     cargo run --example sidecar_capabilities -- --addr=http://127.0.0.1:50051

use pipestream_search::pb::analysis::analysis_service_client::AnalysisServiceClient;
use pipestream_search::pb::analysis::GetCapabilitiesRequest;

fn arg(key: &str, default: &str) -> String {
    let prefix = format!("--{key}=");
    std::env::args()
        .find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
        .unwrap_or_else(|| default.to_string())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = arg("addr", "http://127.0.0.1:50051");
    let mut client = AnalysisServiceClient::connect(addr.clone()).await?;
    let caps = client
        .get_capabilities(GetCapabilitiesRequest {})
        .await?
        .into_inner();
    println!("sidecar {addr}");
    println!("  service_version              {}", caps.service_version);
    println!("  opennlp_version              {}", caps.opennlp_version);
    println!("  ner_available                {}", caps.ner_available);
    println!("  pos_tags_available           {}", caps.pos_tags_available);
    println!("  embeddings_enabled           {}", caps.embeddings_enabled);
    println!(
        "  dual_term_identity_available {}",
        caps.dual_term_identity_available
    );
    println!("  stemmers                     {:?}", caps.stemmers);
    println!("  normalizer_steps             {:?}", caps.normalizer_steps);
    if !caps.warnings.is_empty() {
        println!("  warnings                     {:?}", caps.warnings);
    }
    if !caps.dual_term_identity_available {
        eprintln!(
            "the sidecar at {addr} does not serve the dual term identity; a cased_field ingest \
             would be refused (rebuild grpc-opennlp-analysis from main: ./gradlew installDist)"
        );
        std::process::exit(2);
    }
    Ok(())
}

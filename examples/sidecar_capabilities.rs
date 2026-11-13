//! Ask a running `turbo-kserve` analysis server what capabilities it serves:
//! `Capabilities` on `turbo.result.v1.AnalysisService` (`docs/dual-cased.md`).
//! Exits non-zero when no profiles are advertised, so a deploy script can gate
//! on a live analysis server.
//!
//!     cargo run --example sidecar_capabilities -- --addr=http://127.0.0.1:50051

use turbo_client::proto::result::analysis_service_client::AnalysisServiceClient;
use turbo_client::proto::result::CapabilitiesRequest;

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
        .capabilities(CapabilitiesRequest {})
        .await?
        .into_inner();
    let ner_available = caps.profiles.iter().any(|p| {
        p.layers
            .iter()
            .any(|l| l == "turbo:entities" || l == "turbo:geo")
    });
    println!("turbo-kserve {addr}");
    println!("  profiles_count               {}", caps.profiles.len());
    println!("  ner_available                {ner_available}");
    for profile in &caps.profiles {
        println!("  profile {:<20} {:?}", profile.name, profile.layers);
    }
    if caps.profiles.is_empty() {
        eprintln!(
            "the turbo-kserve endpoint at {addr} reported no profiles; ingest would be refused"
        );
        std::process::exit(2);
    }
    Ok(())
}

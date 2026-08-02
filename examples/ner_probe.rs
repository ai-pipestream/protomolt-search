//! What entity types does a sidecar actually find?
//!
//! The stock English models cover one type each, so a deployment's
//! entity coverage is a property of which models it was given rather
//! than of the request. This prints what came back, typed, which is the
//! only way to tell a model that found nothing from a model that is not
//! loaded.
use turbovec_search::pb::analysis::analysis_service_client::AnalysisServiceClient;
use turbovec_search::pb::analysis::{AnalysisOptions, AnalyzeRequest};

fn arg(key: &str, default: &str) -> String {
    let p = format!("--{key}=");
    std::env::args()
        .find_map(|a| a.strip_prefix(&p).map(str::to_string))
        .unwrap_or_else(|| default.to_string())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = arg("addr", "http://127.0.0.1:59222");
    let text = arg(
        "text",
        "On March 3, 2019, Judge Sonia Sotomayor of the Supreme Court in \
         Washington ordered Acme Corporation to pay $4.5 million, roughly \
         12% of its revenue, to Maria Rodriguez by 5:00 p.m.",
    );
    let mut client = AnalysisServiceClient::connect(addr.clone()).await?;
    let r = client
        .analyze(AnalyzeRequest {
            text: text.clone(),
            options: Some(AnalysisOptions {
                language: "en".into(),
                sentence_detection: true,
                ner: true,
                ..Default::default()
            }),
        })
        .await?
        .into_inner();

    println!("text: {text}\n");
    if r.entities.is_empty() {
        println!("NO ENTITIES. Either no model is loaded or none matched.");
    }
    let mut by_type: std::collections::BTreeMap<String, Vec<String>> = Default::default();
    for e in &r.entities {
        by_type.entry(e.r#type.clone()).or_default().push(e.text.clone());
    }
    println!("{} entities across {} types:", r.entities.len(), by_type.len());
    for (t, vals) in &by_type {
        println!("  {t:<14} {}", vals.join(", "));
    }
    for w in &r.warnings {
        println!("warning: {w}");
    }
    Ok(())
}

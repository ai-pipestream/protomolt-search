//! What entity types does an analysis backend actually find?
//!
//! Runs a document through `turbo` (in-process) or a remote `turbo-kserve`
//! endpoint with entity recognition enabled and prints the discovered entity
//! types and surface strings.

use pipestream_search::analyzer::{
    analyze_document_with_options, body_spec, RemoteCallOptions, SessionLayers,
};

fn arg(key: &str, default: &str) -> String {
    let p = format!("--{key}=");
    std::env::args()
        .find_map(|a| a.strip_prefix(&p).map(str::to_string))
        .unwrap_or_else(|| default.to_string())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = arg("addr", "turbo");
    let text = arg(
        "text",
        "On March 3, 2019, Judge Sonia Sotomayor of the Supreme Court in \
         Washington ordered Acme Corporation to pay $4.5 million, roughly \
         12% of its revenue, to Maria Rodriguez by 5:00 p.m.",
    );
    let doc = analyze_document_with_options(
        &addr,
        &text,
        Some(&body_spec()),
        SessionLayers {
            sentences: true,
            entities: true,
            ..Default::default()
        },
        &RemoteCallOptions::default(),
    )
    .await?;

    println!("text: {text}\n");
    if doc.entities.is_empty() {
        println!("NO ENTITIES. Either no entity rules/models matched or none are loaded.");
    }
    let mut by_type: std::collections::BTreeMap<String, Vec<String>> = Default::default();
    for ent in &doc.entities {
        by_type
            .entry(ent.kind.clone())
            .or_default()
            .push(ent.text.clone());
    }
    println!(
        "{} entities across {} types:",
        doc.entities.len(),
        by_type.len()
    );
    for (t, vals) in &by_type {
        println!("  {t:<14} {}", vals.join(", "));
    }
    Ok(())
}

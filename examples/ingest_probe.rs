//! Smallest possible AddDocuments against a running node: send `--docs`
//! documents and print the server's status cleanly.
//!
//! A bulk driver that is still streaming when the server aborts sees only
//! `h2 protocol error`, which hides the real reason. This probe closes its
//! send side first, so the status comes back intact.
//!
//! ```text
//! ingest_probe --node=127.0.0.1:59800 --docs=3
//! ```

use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::{AddDocumentsRequest, AnalysisSpec, DocLineage};
use tokio_stream::wrappers::ReceiverStream;

fn arg(key: &str, default: &str) -> String {
    let prefix = format!("--{key}=");
    std::env::args()
        .find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
        .unwrap_or_else(|| default.to_string())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let node = arg("node", "127.0.0.1:59800");
    let node = if node.starts_with("http") {
        node
    } else {
        format!("http://{node}")
    };
    let docs: usize = arg("docs", "3").parse()?;
    let with_field = std::env::args().any(|a| a == "--with-field");

    let spec = pipestream_search::analyzer::body_spec();
    // Real chunk texts when asked: the synthetic sentence below exercises
    // neither long documents nor the corpus's own byte content.
    let chunks_path = arg("chunks", "");
    let texts: Vec<String> = if chunks_path.is_empty() {
        Vec::new()
    } else {
        use std::io::BufRead;
        let file = std::fs::File::open(&chunks_path)?;
        std::io::BufReader::new(file)
            .lines()
            .take(docs)
            .map(|l| -> Result<String, Box<dyn std::error::Error>> {
                let line = l?;
                let chunk: serde_json::Value = serde_json::from_str(&line)?;
                Ok(chunk["text"].as_str().unwrap_or_default().to_string())
            })
            .collect::<Result<_, _>>()?
    };

    let mut client = NodeServiceClient::connect(node.clone()).await?;
    let (tx, rx) = tokio::sync::mpsc::channel::<AddDocumentsRequest>(256);
    // Feed concurrently with the call, exactly as the bulk driver does:
    // a feeder that only fills a buffered channel hides ordering and
    // backpressure effects.
    let spec_feed = spec.clone();
    let feeder = tokio::spawn(async move {
        for i in 0..docs {
            tx.send(AddDocumentsRequest {
                materialize: None,
                map_numerics: Vec::new(),
                map_facets: Vec::new(),
                numerics: Vec::new(),
                facets: Vec::new(),
                text: texts.get(i).cloned().unwrap_or_else(|| {
                    format!("the appellant filed a motion number {i} in the district court")
                }),
                analysis: Some(spec_feed.clone()),
                lineage: Some(DocLineage {
                    parent_id: 1000 + i as u64,
                    group_id: 1,
                    span_start: 0,
                    span_end: 40,
                }),
                fields: if with_field {
                    vec![pipestream_search::pb::DocumentField {
                        field: "case_name".to_string(),
                        text: format!("Probe v. Shard {i}"),
                        analysis: Some(AnalysisSpec {
                            tokenizer: 1,
                            stemmer: 1,
                            term_vector_mode: 1,
                            term_vector_source: 1,
                            char_filters: vec![],
                        }),
                    }]
                } else {
                    Vec::new()
                },
                integers: Vec::new(),
                timestamps: Vec::new(),
                geo_points: Vec::new(),
                quality: None,
                geography: None,
            })
            .await
            .map_err(|e| format!("feeder send: {e}"))?;
        }
        drop(tx);
        Ok::<(), String>(())
    });
    match client.add_documents(ReceiverStream::new(rx)).await {
        Ok(r) => println!("OK {:?}", r.into_inner()),
        Err(status) => println!("STATUS {:?}: {}", status.code(), status.message()),
    }
    match feeder.await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => println!("FEEDER {e}"),
        Err(e) => println!("FEEDER PANIC {e}"),
    }
    Ok(())
}

//! Live-interop demo: ingest documents into a turbovec-search node through
//! the REAL OpenNLP analysis sidecar, then run distributed BM25 queries
//! and fetch raw texts for highlighting.
//!
//! Usage (sidecar and a turbovec-search node+coordinator already running):
//!
//! ```text
//! cargo run --example ingest_demo -- \
//!     --node=127.0.0.1:50051 --coordinator=127.0.0.1:50050
//! ```
//!
//! The analysis spec is WHITESPACE tokenizer + PORTER stemmer + term
//! vectors in MODE_FULL with SOURCE_STEMS — so the terms landing in the
//! postings are real OpenNLP Porter stems, and every occurrence keeps its
//! original-text span for highlighting.

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use turbovec_search::pb::node_service_client::NodeServiceClient;
use turbovec_search::pb::search_service_client::SearchServiceClient;
use turbovec_search::pb::{
    AddDocumentsRequest, AnalysisSpec, Bm25SearchRequest, GetDocumentsRequest, TermStatsRequest,
};

const DOCS: [&str; 4] = [
    "The dogs are barking loudly at the running foxes",
    "A single dog barks at every passing runner",
    "Running through the forest, the fox escaped the hounds",
    "Completely unrelated sentences about cooking recipes and kitchen knives",
];

fn arg(key: &str) -> Option<String> {
    let prefix = format!("--{key}=");
    std::env::args().find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let node = format!(
        "http://{}",
        arg("node").unwrap_or_else(|| "127.0.0.1:50051".into())
    );
    let coordinator = format!(
        "http://{}",
        arg("coordinator").unwrap_or_else(|| "127.0.0.1:50050".into())
    );

    // WHITESPACE tokenizer, PORTER stemmer, MODE_FULL, SOURCE_STEMS.
    let spec = AnalysisSpec {
        tokenizer: 1,
        stemmer: 2,
        term_vector_mode: 1,
        term_vector_source: 2,
        normalizer_rungs: vec![],
    };

    let mut node_client = NodeServiceClient::connect(node).await?;

    // --- Ingest ------------------------------------------------------------
    let (tx, rx) = mpsc::channel(8);
    for text in DOCS {
        tx.send(AddDocumentsRequest {
            text: text.to_string(),
            analysis: Some(spec.clone()),
        })
        .await?;
    }
    drop(tx);
    let ingested = node_client
        .add_documents(ReceiverStream::new(rx))
        .await?
        .into_inner();
    println!(
        "ingested {} documents (total {}, first global id {})\n",
        ingested.added, ingested.total, ingested.first_id
    );

    // --- What landed in the postings (TermStats) ---------------------------
    let stats = node_client
        .term_stats(TermStatsRequest {
            terms: ["dog", "bark", "run", "runner", "fox", "kitchen"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
        })
        .await?
        .into_inner();
    println!("TermStats (df per term — these are the REAL Porter stems in the postings):");
    for (term, df) in ["dog", "bark", "run", "runner", "fox", "kitchen"]
        .iter()
        .zip(stats.doc_frequencies.iter())
    {
        println!("  {term:<10} df={df}");
    }
    println!(
        "  (shard docs: {}, total doc length: {})\n",
        stats.doc_count, stats.total_doc_length
    );

    // --- Distributed BM25 queries ------------------------------------------
    let mut search_client = SearchServiceClient::connect(coordinator).await?;
    for query in ["dogs barking", "running", "fox", "kitchen"] {
        let response = search_client
            .bm25_search(Bm25SearchRequest {
                text: query.to_string(),
                k: 10,
                analysis: Some(spec.clone()),
            })
            .await?
            .into_inner();
        println!("query {query:?}:");
        for hit in &response.hits {
            let spans: Vec<String> = hit
                .terms
                .iter()
                .map(|t| {
                    let offs: Vec<String> = t
                        .offsets
                        .iter()
                        .map(|o| format!("[{},{})", o.start, o.end))
                        .collect();
                    format!("{}@{}", t.term, offs.join(","))
                })
                .collect();
            println!(
                "  doc {} score {:.4}  {}",
                hit.doc_id,
                hit.score,
                spans.join("  ")
            );
        }
        if response.hits.is_empty() {
            println!("  (no hits)");
        }

        // --- Highlight: slice one span out of the raw stored text ---------
        if let Some(hit) = response.hits.first() {
            if let Some(term) = hit.terms.first() {
                if let Some(span) = term.offsets.first() {
                    let docs = node_client
                        .get_documents(GetDocumentsRequest {
                            doc_ids: vec![hit.doc_id],
                        })
                        .await?
                        .into_inner();
                    let text = &docs.documents[0].text;
                    let slice = &text[span.start as usize..span.end as usize];
                    println!(
                        "  highlight: doc {} span [{},{}) of {text:?} = {slice:?} (term {:?})",
                        hit.doc_id, span.start, span.end, term.term
                    );
                }
            }
        }
        println!();
    }
    Ok(())
}

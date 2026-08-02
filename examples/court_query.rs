//! Hybrid query against a running court cluster (the two-machine
//! deployment): fetch a probe document's text and lineage from the doc
//! store, its vector from the embeddings file, and run a cascade hybrid
//! search, printing per-leg scores, lineage, and a highlighted span.
//!
//! ```text
//! court_query --nodes=127.0.0.1:50081,... --analysis-addr=http://127.0.0.1:59111 \
//!     --probe-id=12345 --docs-per-shard=340000
//! ```

use turbovec_search::coordinator::CoordinatorServiceImpl;
use turbovec_search::court;
use turbovec_search::pb::node_service_client::NodeServiceClient;
use turbovec_search::pb::{AnalysisSpec, GetDocumentsRequest};

fn arg(key: &str, default: &str) -> String {
    let prefix = format!("--{key}=");
    std::env::args()
        .find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
        .unwrap_or_else(|| default.to_string())
}

/// Longest prefix of `s` within `n` bytes that ends on a char boundary.
fn prefix(s: &str, n: usize) -> &str {
    let mut end = n.min(s.len());
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

fn analysis_spec() -> AnalysisSpec {
    AnalysisSpec {
        tokenizer: 1,
        stemmer: 2,
        term_vector_mode: 1,
        term_vector_source: 2,
        normalizer_steps: vec![],
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let nodes: Vec<String> = arg("nodes", "")
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| {
            if s.starts_with("http://") || s.starts_with("https://") {
                s.to_string()
            } else {
                format!("http://{s}")
            }
        })
        .collect();
    let analysis_addr = arg("analysis-addr", "http://127.0.0.1:59111");
    let embeddings_path = arg("embeddings", "/work/court-corpus/embeddings-static.bin");
    let docs_per_shard: u64 = arg("docs-per-shard", "0").parse()?;
    let probe_ids: Vec<u64> = arg("probe-ids", "0,100000,1000000")
        .split(',')
        .map(|s| s.trim().parse().expect("--probe-ids"))
        .collect();
    let k: u32 = arg("k", "5").parse()?;

    if nodes.is_empty() || docs_per_shard == 0 {
        return Err("--nodes and --docs-per-shard are required".into());
    }
    let coordinator = CoordinatorServiceImpl::new(nodes.clone())
        .with_bm25(Some(analysis_addr), Default::default());
    let spec = analysis_spec();

    for probe_id in probe_ids {
        let shard = ((probe_id / docs_per_shard) as usize).min(nodes.len() - 1);
        let mut client = NodeServiceClient::connect(nodes[shard].clone()).await?;
        let docs = client
            .get_documents(GetDocumentsRequest {
                doc_ids: vec![probe_id],
            })
            .await?
            .into_inner();
        let doc = docs
            .documents
            .first()
            .ok_or_else(|| format!("doc {probe_id} not in doc store"))?;
        let lineage = doc.lineage.expect("court docs carry lineage");

        // Probe vector by lineage key.
        let (_, reader) = court::EmbeddingReader::open(std::path::Path::new(&embeddings_path))?;
        let mut vector = None;
        for record in reader {
            let record = record?;
            if record.opinion_id == lineage.opinion_id {
                // The lineage has no ordinal field; take the first
                // embedding record of the probe's opinion (eyeball
                // probes only — the vector leg is self-matching either
                // way for ordinal 0).
                vector.get_or_insert(record.vector);
            }
        }
        let vector = vector.ok_or("no embedding found for probe lineage")?;

        println!(
            "\n=== query doc {probe_id} (opinion {} cluster {} span {}..{}):",
            lineage.opinion_id, lineage.cluster_id, lineage.span_start, lineage.span_end
        );
        println!("text: {:?}", prefix(&doc.text, 140));
        let hits = coordinator
            .fanout_cascade("court-query", &doc.text, &vector, k, Some(&spec), 0.0, false)
            .await?.0;
        for hit in &hits {
            println!(
                "  #{} doc {:>8} (shard {}) vector {:.4}  bm25 {:.4}",
                hit.rank, hit.doc_id, hit.shard, hit.vector_score, hit.bm25_score
            );
        }
        if let Some(top) = hits.first() {
            let owner = nodes[(top.shard as usize).min(nodes.len() - 1)].clone();
            let mut client = NodeServiceClient::connect(owner).await?;
            let top_docs = client
                .get_documents(GetDocumentsRequest {
                    doc_ids: vec![top.doc_id],
                })
                .await?
                .into_inner();
            if let Some(top_doc) = top_docs.documents.first() {
                let l = top_doc.lineage.as_ref().expect("lineage");
                println!(
                    "  top doc {}: opinion {} cluster {} span {}..{}",
                    top.doc_id, l.opinion_id, l.cluster_id, l.span_start, l.span_end
                );
                println!("  top text: {:?}", prefix(&top_doc.text, 200));
            }
        }
    }
    Ok(())
}

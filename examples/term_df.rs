//! Per-term document frequency across the fleet, for one query.
//!
//! A slow BM25 query is almost always a df problem: block-max pruning
//! can only skip a block whose best possible contribution cannot reach
//! the floor, and a term matching a large fraction of the corpus has both
//! a huge posting list and an idf too low to ever produce a skippable
//! bound. Several such terms in one query and the pruner degenerates to
//! an exhaustive walk over hundreds of millions of postings.
//!
//! This asks every shard for its share and sums them, which is exactly
//! what the coordinator does before scoring, so the numbers here are the
//! ones the scorer actually sees.
//!
//! ```text
//! term_df --nodes=127.0.0.1:59300,... --analysis-addr=http://127.0.0.1:59202 \
//!         --text="federal rules of civil procedure rule 12b6"
//! ```

use turbovec_search::analyzer;
use turbovec_search::pb::node_service_client::NodeServiceClient;
use turbovec_search::pb::{AnalysisSpec, FieldTerms, TermStatsRequest};

fn arg(key: &str, default: &str) -> String {
    let prefix = format!("--{key}=");
    std::env::args()
        .find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
        .unwrap_or_else(|| default.to_string())
}

fn body_spec() -> AnalysisSpec {
    AnalysisSpec {
        tokenizer: 1,
        stemmer: 2,
        term_vector_mode: 1,
        term_vector_source: 2,
        normalizer_rungs: vec![],
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let nodes: Vec<String> = arg("nodes", "127.0.0.1:59300")
        .split(',')
        .map(|s| {
            let s = s.trim();
            if s.starts_with("http") {
                s.to_string()
            } else {
                format!("http://{s}")
            }
        })
        .collect();
    let analysis_addr = arg("analysis-addr", "http://127.0.0.1:59202");
    let text = arg("text", "federal rules of civil procedure rule 12b6");
    let field = arg("field", "body");

    // Same analysis the index was built with, deduped in first-seen
    // order: this is the term set the scorer walks.
    let doc = analyzer::analyze_document(&analysis_addr, &text, Some(&body_spec())).await?;
    let mut terms: Vec<String> = Vec::new();
    for (t, _, _) in doc.into_body().terms {
        if !terms.contains(&t) {
            terms.push(t);
        }
    }
    if terms.is_empty() {
        println!("no terms after analysis");
        return Ok(());
    }

    let mut dfs = vec![0u64; terms.len()];
    let mut doc_count = 0u64;
    for node in &nodes {
        let mut client = NodeServiceClient::connect(node.clone()).await?;
        let r = client
            .term_stats(TermStatsRequest {
                terms: Vec::new(),
                fields: vec![FieldTerms {
                    field: field.clone(),
                    terms: terms.clone(),
                }],
            })
            .await?
            .into_inner();
        doc_count += r.doc_count;
        if let Some(fs) = r.field_stats.first() {
            for (acc, df) in dfs.iter_mut().zip(&fs.doc_frequencies) {
                *acc += u64::from(*df);
            }
        }
    }

    println!("field {field:?}, {doc_count} documents across {} shards", nodes.len());
    println!("  {:<16} {:>14} {:>8}   {}", "term", "df", "% corpus", "cost");
    let mut order: Vec<usize> = (0..terms.len()).collect();
    order.sort_by_key(|&i| std::cmp::Reverse(dfs[i]));
    let mut total = 0u64;
    for i in order {
        let pct = dfs[i] as f64 / doc_count.max(1) as f64 * 100.0;
        total += dfs[i];
        // A term in most of the corpus has near-zero idf and a posting
        // list the length of the corpus: it cannot discriminate and
        // cannot be skipped, so it is pure cost.
        let note = if pct > 50.0 {
            "DOMINATES: near-zero idf, unskippable"
        } else if pct > 10.0 {
            "expensive"
        } else {
            ""
        };
        println!("  {:<16} {:>14} {:>7.1}%   {note}", terms[i], dfs[i], pct);
    }
    println!("  {:<16} {:>14}  postings the scorer must consider", "TOTAL", total);
    Ok(())
}

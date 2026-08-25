//! End-to-end verification gate for the court demo deployment
//! (deploy/court-e2e): asserts the cluster actually serves what was
//! ingested. Exits non-zero on any failure, so it can gate
//! `docker compose up --exit-code-from pipeline`.
//!
//! Gates:
//! 1. VECTOR: the first embedding in the embeddings file must be its own
//!    top-1 hit (global id 0) in a coordinator `Search`.
//! 2. BM25: a probe term every legal corpus contains ("court") must
//!    return hits through the coordinator's distributed BM25 path
//!    (query analysis -> TermStats -> global stats -> Bm25Query).
//!
//! ```text
//! court_verify --coordinator=http://coordinator:50050 \
//!     --embeddings=/corpus/embeddings.bin [--k=10]
//! ```

use std::path::Path;

use turbovec_search::court;
use turbovec_search::pb::search_service_client::SearchServiceClient;
use turbovec_search::pb::{Bm25SearchRequest, SearchRequest};

fn arg(key: &str, default: &str) -> String {
    let prefix = format!("--{key}=");
    std::env::args()
        .find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
        .unwrap_or_else(|| default.to_string())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let coordinator = arg("coordinator", "http://127.0.0.1:50050");
    let embeddings = arg("embeddings", "/corpus/embeddings.bin");
    let k: u32 = arg("k", "10").parse()?;

    let mut client = SearchServiceClient::connect(coordinator.clone()).await?;

    // Gate 1: vector self-match on the file's first record (shard 0,
    // local slot 0 -> global id 0 by the ingest layout).
    let (dim, mut reader) = court::EmbeddingReader::open(Path::new(&embeddings))?;
    let first = reader.next().ok_or("embeddings file is empty")??;
    if first.vector.len() != dim as usize {
        return Err(format!(
            "embedding dim mismatch: record has {}, file header says {dim}",
            first.vector.len()
        )
        .into());
    }
    let response = client
        .search(SearchRequest {
            request_id: "verify".into(),
            k,
            vector: first.vector,
            collapse_parents: false,
            ..Default::default()
        })
        .await?
        .into_inner();
    if response.hits.len() != k as usize {
        return Err(format!(
            "vector gate: expected {k} hits, got {}",
            response.hits.len()
        )
        .into());
    }
    let top = &response.hits[0];
    if top.vector_id != 0 {
        return Err(format!(
            "vector gate: self-match expected global id 0 on top, got {} (score {})",
            top.vector_id, top.score
        )
        .into());
    }
    println!(
        "vector gate: PASS (top-1 self-match id 0, score {}, {} hits)",
        top.score,
        response.hits.len()
    );

    // Gate 2: distributed BM25. The analysis spec must match ingest
    // (WHITESPACE tokenizer, PORTER stemmer, MODE_FULL, SOURCE_STEMS) —
    // same values as court_ingest's analysis_spec().
    let bm25 = client
        .bm25_search(Bm25SearchRequest {
            projections: Vec::new(),
            filter: String::new(),
            map_facet_fields: Vec::new(),
            score_stages: Vec::new(),
            facet_fields: Vec::new(),
            text: "court".into(),
            k: 5,
            analysis: Some(turbovec_search::analyzer::body_spec()),
            min_score: 0.0,
            fields: Vec::new(),
            range_facet_fields: Vec::new(),
            geo_filters: Vec::new(),
            stats_fields: Vec::new(),
            cardinality_fields: Vec::new(),
        })
        .await?
        .into_inner();
    if bm25.hits.is_empty() {
        return Err("bm25 gate: probe term 'court' returned no hits".into());
    }
    println!(
        "bm25 gate: PASS ({} hits for 'court', top doc {} score {})",
        bm25.hits.len(),
        bm25.hits[0].doc_id,
        bm25.hits[0].score
    );

    println!("VERIFY OK");
    Ok(())
}

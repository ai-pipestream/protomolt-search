//! Time each retrieval leg on its own, so "the query took 460 ms" can be
//! attributed instead of guessed at.
//!
//! A hybrid query's `legs` phase runs the vector scan and the BM25 scan
//! together, so its duration says nothing about which one costs. This
//! issues the same query three ways against the same live cluster:
//!
//!   vector  SearchService.Search      -- pure semantic, no postings read
//!   bm25    SearchService.Bm25Search  -- pure lexical, no codes scanned
//!   hybrid  SearchService.HybridSearch
//!
//! Embedding happens once per query and is EXCLUDED from every timing:
//! it is the same fixed cost for any mode and would otherwise be counted
//! twice in the comparison.
//!
//! ```text
//! leg_latency --coord=127.0.0.1:59291 --analysis-addr=http://127.0.0.1:59202 \
//!             --queries=deploy/v7-rebuild/queries-case-folding.txt --k=10
//! ```

use std::time::Instant;

use turbovec_search::analyzer;
use turbovec_search::pb::search_service_client::SearchServiceClient;
use turbovec_search::pb::{
    AnalysisSpec, Bm25SearchRequest, FusionMode, HybridLegOptions, HybridSearchRequest,
    SearchRequest,
};

fn arg(key: &str, default: &str) -> String {
    let prefix = format!("--{key}=");
    std::env::args()
        .find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
        .unwrap_or_else(|| default.to_string())
}

/// The body field's ingest analysis; the BM25 leg must use it or its
/// terms will not match the index and the timing would be of a query
/// that finds nothing.
fn body_spec() -> AnalysisSpec {
    turbovec_search::analyzer::body_spec()
}

fn pct(sorted: &[f64], p: usize) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    sorted[((sorted.len() * p / 100).min(sorted.len() - 1)).max(0)]
}

fn report(label: &str, mut ms: Vec<f64>, hits: usize, queries: usize) {
    ms.sort_by(|a, b| a.partial_cmp(b).expect("no NaN timings"));
    println!(
        "  {label:<8} p50 {:7.1}  p90 {:7.1}  p99 {:7.1}  min {:7.1}  max {:7.1}   {:.1} hits/query avg",
        pct(&ms, 50),
        pct(&ms, 90),
        pct(&ms, 99),
        ms.first().copied().unwrap_or(0.0),
        ms.last().copied().unwrap_or(0.0),
        hits as f64 / queries.max(1) as f64,
    );
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let coord = arg("coord", "127.0.0.1:59291");
    let analysis_addr = arg("analysis-addr", "http://127.0.0.1:59202");
    let k: u32 = arg("k", "10").parse()?;
    let repeats: usize = arg("repeats", "1").parse()?;
    // Seeded floor for the BM25 leg. Lets a caller test whether a query's
    // cost is floor-bootstrap bound: MaxScore cannot demote a term until
    // the floor exceeds that term's max contribution, and the floor only
    // rises as documents are found in doc-id order.
    let min_score: f32 = arg("min-score", "0").parse()?;
    let queries: Vec<String> = match arg("queries", "").as_str() {
        "" => vec![arg("query", "qualified immunity clearly established right")],
        path => std::fs::read_to_string(path)?
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
            .map(str::to_string)
            .collect(),
    };
    if queries.is_empty() {
        eprintln!("no queries");
        std::process::exit(2);
    }

    let mut client = SearchServiceClient::connect(format!("http://{coord}")).await?;
    let (mut vec_ms, mut bm_ms, mut hyb_ms) = (Vec::new(), Vec::new(), Vec::new());
    let (mut vec_hits, mut bm_hits, mut hyb_hits) = (0usize, 0usize, 0usize);
    let mut embed_ms: Vec<f64> = Vec::new();
    let mut empty_bm25 = 0usize;

    for q in &queries {
        let t = Instant::now();
        let vector = analyzer::embed_text(&analysis_addr, q).await?;
        embed_ms.push(t.elapsed().as_secs_f64() * 1000.0);

        for _ in 0..repeats {
            // Vector only.
            let t = Instant::now();
            let r = client
                .search(SearchRequest {
                    request_id: String::new(),
                    k,
                    vector: vector.clone(),
                    collapse_parents: false,
                    ..Default::default()
                })
                .await?
                .into_inner();
            vec_ms.push(t.elapsed().as_secs_f64() * 1000.0);
            vec_hits += r.hits.len();

            // BM25 only.
            let t = Instant::now();
            let r = client
                .bm25_search(Bm25SearchRequest {
                    filter: String::new(),
                    map_facet_fields: Vec::new(),
                    score_stages: Vec::new(),
                    facet_fields: Vec::new(),
                    text: q.clone(),
                    k,
                    analysis: Some(body_spec()),
                    min_score,
                    fields: Vec::new(),
                    range_facet_fields: Vec::new(),
                    geo_filters: Vec::new(),
                    stats_fields: Vec::new(),
                    cardinality_fields: Vec::new(),
                })
                .await?
                .into_inner();
            bm_ms.push(t.elapsed().as_secs_f64() * 1000.0);
            bm_hits += r.hits.len();
            // A lexical leg that matches nothing is fast for the wrong
            // reason; count it rather than letting it flatter the p50.
            if r.hits.is_empty() {
                empty_bm25 += 1;
            }

            // Both.
            let t = Instant::now();
            let r = client
                .hybrid_search(HybridSearchRequest {
                    request_id: String::new(),
                    text: q.clone(),
                    vector: vector.clone(),
                    k,
                    analysis: Some(body_spec()),
                    legs: Some(HybridLegOptions {
                        fusion_mode: FusionMode::GlobalRank as i32,
                        ..Default::default()
                    }),
                    debug: false,
                    boost: None,
                    ..Default::default()
                })
                .await?
                .into_inner();
            hyb_ms.push(t.elapsed().as_secs_f64() * 1000.0);
            hyb_hits += r.hits.len();
        }
    }

    let n = queries.len() * repeats;
    println!(
        "{} queries x {repeats} repeats, k={k}, against {coord}\n\
         milliseconds, embedding excluded (measured separately below)",
        queries.len()
    );
    report("vector", vec_ms, vec_hits, n);
    report("bm25", bm_ms, bm_hits, n);
    report("hybrid", hyb_ms, hyb_hits, n);
    let mut e = embed_ms;
    e.sort_by(|a, b| a.partial_cmp(b).expect("no NaN timings"));
    println!(
        "  {:<8} p50 {:7.1}  (once per query, shared by all three)",
        "embed",
        pct(&e, 50)
    );
    if empty_bm25 > 0 {
        println!(
            "  note: {empty_bm25} of {n} bm25 queries matched NOTHING -- those are fast \
             because they found nothing, not because lexical is cheap"
        );
    }
    Ok(())
}

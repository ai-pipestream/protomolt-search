//! What floor sharing actually costs and saves, per query.
//!
//! `--floor-warmup-chunks` and `--floor-min-interval-ms` trade floor
//! messages for pruning: publishing later or less often means fewer
//! `FloorUpdate` messages on the wire, and a floor that arrives late
//! prunes less. Latency alone cannot tell those apart, because a change
//! that halves the message count and slightly weakens pruning can land
//! back on the same wall time.
//!
//! Cascade's phase-1 scan reports `ShardScanStats` in the hybrid debug
//! block, so this reads the counters directly:
//!
//!   chunk_calls           per-chunk searches the scan made
//!   candidates_collected  vectors that survived every floor in effect
//!   floors_offered        chunks whose heap was full enough to offer
//!   floors_published      offers that actually went on the wire
//!   floor_updates_applied chunks that ran under a pushed floor
//!
//! offered vs published IS the knobs' effect; published vs applied is
//! how much of it the other shards got to use.
//!
//! ```text
//! floor_stats --coord=127.0.0.1:59291 --analysis-addr=http://127.0.0.1:59202 \
//!             --queries=deploy/v7-rebuild/queries-case-folding.txt --k=10
//! ```

use turbovec_search::analyzer;
use turbovec_search::pb::search_service_client::SearchServiceClient;
use turbovec_search::pb::{AnalysisSpec, FusionMode, HybridLegOptions, HybridSearchRequest};

fn arg(key: &str, default: &str) -> String {
    let prefix = format!("--{key}=");
    std::env::args()
        .find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
        .unwrap_or_else(|| default.to_string())
}

fn body_spec() -> AnalysisSpec {
    turbovec_search::analyzer::body_spec()
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let coord = arg("coord", "127.0.0.1:59291");
    let analysis_addr = arg("analysis-addr", "http://127.0.0.1:59202");
    let k: u32 = arg("k", "10").parse()?;
    let queries: Vec<String> = match arg("queries", "").as_str() {
        "" => vec![arg("query", "qualified immunity clearly established right")],
        path => std::fs::read_to_string(path)?
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
            .map(str::to_string)
            .collect(),
    };

    let mut client = SearchServiceClient::connect(format!("http://{coord}")).await?;
    let (mut calls, mut cands, mut offered, mut pubs, mut applied) = (0u64, 0u64, 0u64, 0u64, 0u64);
    let mut shards_seen = 0usize;
    let mut no_scan = 0usize;

    for q in &queries {
        let vector = analyzer::embed_text(&analysis_addr, q).await?;
        let r = client
            .hybrid_search(HybridSearchRequest {
                request_id: String::new(),
                text: q.clone(),
                vector,
                k,
                analysis: Some(body_spec()),
                legs: Some(HybridLegOptions {
                    // Cascade is the only mode whose debug block carries
                    // the phase-1 scan stats; the counters are a property
                    // of the streaming vector path, which every mode that
                    // uses it shares.
                    fusion_mode: FusionMode::Cascade as i32,
                    ..Default::default()
                }),
                debug: true,
                boost: None,
            })
            .await?
            .into_inner();
        let Some(d) = r.debug else {
            eprintln!("no debug block returned; the server ignored `debug`");
            std::process::exit(1);
        };
        for s in &d.shards {
            shards_seen += 1;
            match &s.scan {
                Some(sc) => {
                    calls += u64::from(sc.chunk_calls);
                    cands += sc.candidates_collected;
                    offered += sc.floors_offered;
                    pubs += sc.floors_published;
                    applied += sc.floor_updates_applied;
                }
                // Counted, not skipped: averaging over the shards that
                // happened to report would understate every total below.
                None => no_scan += 1,
            }
        }
    }

    let n = queries.len().max(1) as f64;
    println!(
        "{} queries, k={k}, {shards_seen} shard scans",
        queries.len()
    );
    println!(
        "  chunk_calls            {:>12.1} per query",
        calls as f64 / n
    );
    println!(
        "  candidates_collected   {:>12.1} per query",
        cands as f64 / n
    );
    println!(
        "  floors_offered         {:>12.1} per query",
        offered as f64 / n
    );
    println!(
        "  floors_published       {:>12.1} per query   ({:.0}% of offers reached the wire)",
        pubs as f64 / n,
        if offered == 0 {
            0.0
        } else {
            pubs as f64 / offered as f64 * 100.0
        }
    );
    println!(
        "  floor_updates_applied  {:>12.1} per query",
        applied as f64 / n
    );
    if no_scan > 0 {
        println!(
            "  {no_scan} of {shards_seen} shard entries carried NO scan stats; \
             the totals above are missing their share"
        );
    }
    Ok(())
}

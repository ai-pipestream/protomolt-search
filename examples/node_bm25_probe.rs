//! Call `NodeService.Bm25Query` directly, both ways, and time it.
//!
//! `prune_probe` shows the two scorers are the same speed in-process and
//! `ab --arm=x=default --arm=y=body` shows the two coordinator routes are
//! hundreds of times apart. Between those sit the wire and the
//! coordinator. This issues the node RPC itself -- one shard, no
//! coordinator, no fan-out, no analysis -- so a gap here is the node or
//! the wire, and no gap here puts it in the coordinator.
//!
//! ```text
//! node_bm25_probe --node=127.0.0.1:59300 --terms=court,establish \
//!                 --n=86633399 --total-len=18607931055 --dfs=36113172,8809791
//! ```

use std::time::Instant;

use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::{Bm25FieldLeg, Bm25QueryRequest};

fn arg(key: &str, default: &str) -> String {
    let prefix = format!("--{key}=");
    std::env::args()
        .find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
        .unwrap_or_else(|| default.to_string())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let node = arg("node", "127.0.0.1:59300");
    let terms: Vec<String> = arg("terms", "court,establish")
        .split(',')
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect();
    let field = arg("field", "body");
    let k: u32 = arg("k", "5").parse()?;
    let doc_count: u64 = arg("n", "86633399").parse()?;
    let total_len: u64 = arg("total-len", "18607931055").parse()?;
    let dfs: Vec<u32> = arg("dfs", "")
        .split(',')
        .filter(|s| !s.is_empty())
        .map(str::parse)
        .collect::<Result<_, _>>()?;
    if dfs.len() != terms.len() {
        eprintln!("--dfs needs one value per term ({} terms)", terms.len());
        std::process::exit(2);
    }
    let repeats: usize = arg("repeats", "3").parse()?;

    let mut client = NodeServiceClient::connect(format!("http://{node}")).await?;

    let single = Bm25QueryRequest {
        projections: Vec::new(),
        filter: None,
        map_facet_fields: Vec::new(),
        score_stages: Vec::new(),
        facet_fields: Vec::new(),
        terms: terms.clone(),
        k,
        global_doc_count: doc_count,
        global_total_doc_length: total_len,
        global_doc_frequencies: dfs.clone(),
        k1: 1.2,
        b: 0.75,
        min_score: 0.0,
        fields: Vec::new(),
        expected_stats_epoch: 0,
        range_facet_fields: Vec::new(),
        geo_filters: Vec::new(),
        stats_fields: Vec::new(),
        cardinality_fields: Vec::new(),
    };
    // Exactly what `fanout_bm25_fused` sends: the per-field stats move
    // into the leg and the flat fields go empty.
    let fused = Bm25QueryRequest {
        projections: Vec::new(),
        filter: None,
        map_facet_fields: Vec::new(),
        score_stages: Vec::new(),
        facet_fields: Vec::new(),
        terms: Vec::new(),
        k,
        global_doc_count: doc_count,
        global_total_doc_length: 0,
        global_doc_frequencies: Vec::new(),
        k1: 0.0,
        b: 0.0,
        min_score: 0.0,
        fields: vec![Bm25FieldLeg {
            field: field.clone(),
            terms: terms.clone(),
            global_total_doc_length: total_len,
            global_doc_frequencies: dfs.clone(),
            weight: 1.0,
            k1: 1.2,
            b: 0.75,
            // 0, and deliberately: --terms are typed in already
            // analyzed, so this probe does not KNOW which analyzer
            // produced them and must not claim one. Declaring a spec it
            // did not use would refuse valid probes against a column
            // built any other way.
            analysis_fingerprint: 0,
        }],
        // Same reasoning as the fingerprint: hand-typed stats carry no
        // epoch claim.
        expected_stats_epoch: 0,
        range_facet_fields: Vec::new(),
        geo_filters: Vec::new(),
        stats_fields: Vec::new(),
        cardinality_fields: Vec::new(),
    };

    println!("{node}  terms {terms:?}  k={k}");
    for (label, req) in [("single", &single), ("fused", &fused)] {
        for i in 0..repeats {
            let t = Instant::now();
            let r = client.bm25_query(req.clone()).await?.into_inner();
            let ms = t.elapsed().as_secs_f64() * 1000.0;
            let offsets: usize = r
                .hits
                .iter()
                .flat_map(|h| &h.terms)
                .map(|t| t.offsets.len())
                .sum();
            println!(
                "  {label:<7} run {i}  {ms:9.1} ms   {} hits, {offsets} offset spans, top {:.6}",
                r.hits.len(),
                r.hits.first().map(|h| h.score).unwrap_or(0.0)
            );
        }
    }
    Ok(())
}

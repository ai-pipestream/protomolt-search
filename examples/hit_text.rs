//! What each retrieval leg actually puts in front of a reader.
//!
//! Scores and ranking diffs say how two configurations differ, never what
//! they returned. A complaint like "the results are all two-word section
//! headings" cannot be checked against a doc id, so this runs the same
//! query through the vector, lexical and hybrid paths and prints the
//! stored text of every hit next to its length.
//!
//! ```text
//! hit_text --coord=127.0.0.1:59291 --analysis-addr=http://127.0.0.1:59202 \
//!          --nodes=127.0.0.1:59300,... --offsets=0,21659648,... \
//!          --query="qualified immunity"
//! ```

use turbovec_search::analyzer;
use turbovec_search::pb::node_service_client::NodeServiceClient;
use turbovec_search::pb::search_service_client::SearchServiceClient;
use turbovec_search::pb::{
    AnalysisSpec, Bm25SearchRequest, FusionMode, GetDocumentsRequest, HybridLegOptions,
    HybridSearchRequest, QueryField, SearchRequest,
};

fn arg(key: &str, default: &str) -> String {
    let prefix = format!("--{key}=");
    std::env::args()
        .find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
        .unwrap_or_else(|| default.to_string())
}

fn body_spec() -> AnalysisSpec {
    turbovec_search::analyzer::body_spec()
}

/// Fetch stored text for global doc ids, asking each id's owning shard.
async fn texts(
    nodes: &[String],
    offsets: &[u64],
    ids: &[u64],
) -> Result<Vec<(u64, Option<String>)>, Box<dyn std::error::Error>> {
    let mut out = Vec::new();
    for &id in ids {
        // Shards are NOT evenly spaced in the global id space (the v7
        // cuts are block-aligned and the tail lands on the last shard),
        // so the owner is found by range, never by division.
        let shard = offsets
            .iter()
            .rposition(|&o| id >= o)
            .unwrap_or(0)
            .min(nodes.len() - 1);
        let mut client = NodeServiceClient::connect(nodes[shard].clone()).await?;
        let docs = client
            .get_documents(GetDocumentsRequest { doc_ids: vec![id] })
            .await?
            .into_inner()
            .documents;
        // None, not a placeholder string: a stand-in would be counted
        // as a real document by every length statistic below, which is
        // how a fetch bug turns into a confident claim about chunk size.
        out.push((id, docs.into_iter().next().map(|d| d.text)));
    }
    Ok(out)
}

fn report(label: &str, hits: &[(u64, f32)], fetched: &[(u64, Option<String>)]) {
    println!("\n[{label}]");
    let (mut total, mut counted, mut missing) = (0usize, 0usize, 0usize);
    for (i, (id, score)) in hits.iter().enumerate() {
        let text = fetched
            .iter()
            .find(|(f, _)| f == id)
            .and_then(|(_, t)| t.as_deref());
        match text {
            Some(t) => {
                let words = t.split_whitespace().count();
                total += words;
                counted += 1;
                let one: String = t.split_whitespace().collect::<Vec<_>>().join(" ");
                let cut: String = one.chars().take(96).collect();
                println!("  {:>2}. {id:<12} {score:9.4}  {words:>5}w  {cut:?}", i + 1);
            }
            None => {
                missing += 1;
                println!(
                    "  {:>2}. {id:<12} {score:9.4}      ?  NO STORED TEXT on its owning shard",
                    i + 1
                );
            }
        }
    }
    if counted > 0 {
        println!(
            "      mean length over the {counted} hits whose text was found: {:.0} words",
            total as f64 / counted as f64
        );
    }
    if missing > 0 {
        // Said out loud and kept out of the mean: a length claim made
        // from a third of the hits is not a length claim.
        println!(
            "      {missing} of {} hits had NO stored text -- excluded above",
            hits.len()
        );
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let coord = arg("coord", "127.0.0.1:59291");
    let analysis_addr = arg("analysis-addr", "http://127.0.0.1:59202");
    let offsets: Vec<u64> = arg("offsets", "0")
        .split(',')
        .filter(|s| !s.is_empty())
        .map(str::parse)
        .collect::<Result<_, _>>()?;
    let nodes: Vec<String> = arg("nodes", "")
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|s| {
            let s = s.trim();
            if s.starts_with("http") {
                s.to_string()
            } else {
                format!("http://{s}")
            }
        })
        .collect();
    if nodes.is_empty() {
        eprintln!("--nodes is required (text is stored per shard)");
        std::process::exit(2);
    }
    if offsets.len() != nodes.len() {
        eprintln!(
            "--offsets needs one slot offset per node ({} nodes, {} offsets). \
             Read them from the nodes' --slot-offset flags; guessing an even \
             split silently asks the wrong shard and every hit reads as missing.",
            nodes.len(),
            offsets.len()
        );
        std::process::exit(2);
    }
    let k: u32 = arg("k", "8").parse()?;
    let query = arg("query", "qualified immunity");
    let b: f32 = arg("b", "0").parse()?;

    let mut client = SearchServiceClient::connect(format!("http://{coord}")).await?;
    let vector = analyzer::embed_text(&analysis_addr, &query).await?;
    println!("query {query:?}   k={k}");

    let mut legs: Vec<(String, Vec<(u64, f32)>)> = Vec::new();

    let r = client
        .search(SearchRequest {
            request_id: String::new(),
            k,
            vector: vector.clone(),
            collapse_parents: false,
        })
        .await?
        .into_inner();
    legs.push((
        "vector".into(),
        r.hits.iter().map(|h| (h.vector_id, h.score)).collect(),
    ));

    let r = client
        .bm25_search(Bm25SearchRequest {
            filter: String::new(),
            map_facet_fields: Vec::new(),
            score_stages: Vec::new(),
            facet_fields: Vec::new(),
            text: query.clone(),
            k,
            analysis: None,
            min_score: 0.0,
            fields: vec![QueryField {
                field: "body".into(),
                analysis: Some(body_spec()),
                weight: 1.0,
                k1: 0.0,
                b,
            }],
            range_facet_fields: Vec::new(),
            geo_filters: Vec::new(),
        })
        .await?
        .into_inner();
    legs.push((
        if b == 0.0 {
            "bm25 (b=default)".into()
        } else {
            format!("bm25 (b={b})")
        },
        r.hits.iter().map(|h| (h.doc_id, h.score)).collect(),
    ));

    for (label, mode) in [
        ("hybrid cascade", FusionMode::Cascade),
        ("hybrid global_rank", FusionMode::GlobalRank),
    ] {
        let r = client
            .hybrid_search(HybridSearchRequest {
                request_id: String::new(),
                text: query.clone(),
                vector: vector.clone(),
                k,
                analysis: Some(body_spec()),
                legs: Some(HybridLegOptions {
                    fusion_mode: mode as i32,
                    ..Default::default()
                }),
                debug: false,
                boost: None,
            })
            .await?
            .into_inner();
        // Cascade reports in `cascade_hits`, the rest in `hits`.
        let hits: Vec<(u64, f32)> = if r.hits.is_empty() {
            r.cascade_hits
                .iter()
                .map(|h| (h.doc_id, h.bm25_score))
                .collect()
        } else {
            r.hits.iter().map(|h| (h.doc_id, h.fused_score)).collect()
        };
        legs.push((label.into(), hits));
    }

    for (label, hits) in &legs {
        let ids: Vec<u64> = hits.iter().map(|(id, _)| *id).collect();
        let fetched = texts(&nodes, &offsets, &ids).await?;
        report(label, hits, &fetched);
    }
    Ok(())
}

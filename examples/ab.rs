//! Drive `SearchService.VariantSearch` from the command line: run two or
//! more query configurations over the live cluster and print how far
//! apart their rankings are.
//!
//! An arm is written as `label=field[:weight][+field[:weight]...]`, so
//! the A/B that motivated this — one body column against another indexed
//! under a different analysis — is a one-liner:
//!
//! ```text
//! ab --coord=127.0.0.1:59291 --query="supreme court certiorari" \
//!    --arm=control=body --arm=folded=body_norm --interleave
//! ```
//!
//! Weights fuse fields within an arm, which is the other comparison
//! worth running on this corpus (does boosting the caption help?):
//!
//! ```text
//! ab --arm=body-only=body --arm=caption=body+case_name:3 --query="smith"
//! ```
//!
//! Analysis is left unset, so every field is queried with the
//! coordinator's configured spec. That is the correct default here: term
//! identity is per field and was fixed at ingest, so an arm comparing two
//! COLUMNS is already comparing two analyses.

use turbovec_search::pb::search_service_client::SearchServiceClient;
use turbovec_search::pb::{
    search_variant, Bm25SearchRequest, InterleaveTeam, QueryField, SearchVariant,
    VariantSearchRequest,
};

fn arg(key: &str, default: &str) -> String {
    let prefix = format!("--{key}=");
    std::env::args()
        .find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
        .unwrap_or_else(|| default.to_string())
}

fn flag(key: &str) -> bool {
    let want = format!("--{key}");
    std::env::args().any(|a| a == want)
}

fn args_all(key: &str) -> Vec<String> {
    let prefix = format!("--{key}=");
    std::env::args()
        .filter_map(|a| a.strip_prefix(&prefix).map(str::to_string))
        .collect()
}

/// `label=field[:weight][+field[:weight]...]`
fn parse_arm(spec: &str, text: &str) -> Result<SearchVariant, String> {
    let (label, fields) = spec
        .split_once('=')
        .ok_or_else(|| format!("arm {spec:?}: expected label=field[+field...]"))?;
    if label.is_empty() {
        return Err(format!("arm {spec:?}: empty label"));
    }
    let mut query_fields = Vec::new();
    for part in fields.split('+') {
        let (name, weight) = match part.split_once(':') {
            Some((n, w)) => (
                n,
                w.parse::<f32>()
                    .map_err(|e| format!("arm {label}: weight {w:?}: {e}"))?,
            ),
            None => (part, 1.0),
        };
        if name.is_empty() {
            return Err(format!("arm {label}: empty field name"));
        }
        query_fields.push(QueryField {
            field: name.to_string(),
            analysis: None,
            weight,
            k1: 0.0,
            b: 0.0,
        });
    }
    Ok(SearchVariant {
        label: label.to_string(),
        query: Some(search_variant::Query::Bm25(Bm25SearchRequest {
            text: text.to_string(),
            k: 0, // the request's shared k wins
            analysis: None,
            min_score: 0.0,
            fields: query_fields,
        })),
    })
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let coord = arg("coord", "127.0.0.1:59291");
    let query = arg("query", "supreme court certiorari");
    let k: u32 = arg("k", "10").parse()?;
    let specs = args_all("arm");
    if specs.len() < 2 {
        eprintln!(
            "need at least two --arm=label=field[+field:weight] (got {})\n\
             example: --arm=control=body --arm=folded=body_norm",
            specs.len()
        );
        std::process::exit(2);
    }
    let variants: Vec<SearchVariant> = specs
        .iter()
        .map(|s| parse_arm(s, &query))
        .collect::<Result<_, _>>()?;

    let mut client = SearchServiceClient::connect(format!("http://{coord}")).await?;
    let resp = client
        .variant_search(VariantSearchRequest {
            request_id: String::new(),
            variants,
            k,
            rbo_p: arg("rbo-p", "0").parse()?,
            interleave: flag("interleave"),
            interleave_seed: arg("seed", "0").parse()?,
        })
        .await?
        .into_inner();

    println!("query: {query:?}   k={k}   request {}", resp.request_id);
    println!();
    for r in &resp.results {
        println!("[{}]  {} hits  {:.1} ms", r.label, r.hits.len(), r.elapsed_ms);
        for (i, h) in r.hits.iter().enumerate() {
            println!("  {:>3}. {:<14} {:.6}", i + 1, h.doc_id, h.score);
        }
        println!();
    }

    println!(
        "{:<20} {:>6} {:>8} {:>8} {:>10} {:>8} {:>6}",
        "vs reference", "depth", "overlap", "tau-b", "rbo", "regret", "top1"
    );
    for d in &resp.diffs {
        println!(
            "{:<20} {:>6} {:>7.0}% {:>8.3} {:>10.4} {:>8.4} {:>6}",
            d.variant,
            d.depth,
            d.overlap_fraction * 100.0,
            d.kendall_tau,
            d.rbo,
            d.score_regret,
            if d.top1_flipped { "FLIP" } else { "same" },
        );
        if d.regret_unscored > 0 {
            // Not folded into the mean: these are documents the reference
            // never scored, so regret genuinely cannot judge them.
            println!(
                "{:<20} {} of {} results were outside the reference entirely",
                "", d.regret_unscored, d.depth
            );
        }
    }

    if let Some(il) = resp.interleaving {
        println!("\ninterleaved (seed {}):", il.seed);
        let label = |t: i32| match InterleaveTeam::try_from(t) {
            Ok(InterleaveTeam::A) => resp.results[0].label.as_str(),
            Ok(InterleaveTeam::B) => resp.results[1].label.as_str(),
            _ => "?",
        };
        for (i, (id, team)) in il.doc_ids.iter().zip(&il.teams).enumerate() {
            println!("  {:>3}. {:<14} <- {}", i + 1, id, label(*team));
        }
    }
    Ok(())
}

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

/// Running totals for a whole query set.
///
/// One query's diff is an observation, not a decision: the arms can
/// disagree wildly on a query neither answers well. A choice between two
/// analysis chains needs the aggregate, and the SPREAD matters as much as
/// the mean -- a chain that helps half the queries and hurts the other
/// half averages to "no change" and is not that.
#[derive(Default)]
struct Totals {
    queries: usize,
    overlap: f64,
    tau: f64,
    rbo: f64,
    flips: usize,
    diverged: usize,
    tau_worse: usize,
    tau_better: usize,
    incomparable: usize,
}

impl Totals {
    /// Returns false when the query could not be compared at all, so
    /// the caller can report it separately instead of averaging a
    /// non-measurement into the summary.
    fn add(&mut self, d: &turbovec_search::pb::RankingDiff) -> bool {
        if d.depth == 0 {
            self.incomparable += 1;
            return false;
        }
        self.queries += 1;
        self.overlap += f64::from(d.overlap_fraction);
        self.tau += f64::from(d.kendall_tau);
        self.rbo += f64::from(d.rbo);
        self.flips += usize::from(d.top1_flipped);
        self.diverged += usize::from(d.regret_unscored > 0);
        // Direction of disagreement, counted rather than averaged: the
        // mean hides a split, and a split is the interesting outcome.
        if d.kendall_tau < 0.99 {
            if d.overlap_fraction < 0.5 {
                self.tau_worse += 1;
            } else {
                self.tau_better += 1;
            }
        }
        true
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let coord = arg("coord", "127.0.0.1:59291");
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
    let queries: Vec<String> = match arg("queries", "").as_str() {
        "" => vec![arg("query", "supreme court certiorari")],
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

    // Query-set mode: per-query one-liners, then the aggregate.
    if queries.len() > 1 {
        let mut totals: Vec<(String, Totals)> = Vec::new();
        println!(
            "{:<44} {:>7} {:>7} {:>8} {:>5}",
            "query", "overlap", "tau-b", "rbo", "top1"
        );
        for q in &queries {
            let variants: Vec<SearchVariant> = specs
                .iter()
                .map(|s| parse_arm(s, q))
                .collect::<Result<_, _>>()?;
            let resp = client
                .variant_search(VariantSearchRequest {
                    request_id: String::new(),
                    variants,
                    k,
                    rbo_p: arg("rbo-p", "0").parse()?,
                    interleave: false,
                    interleave_seed: 0,
                })
                .await?
                .into_inner();
            for d in &resp.diffs {
                if totals.iter().all(|(l, _)| *l != d.variant) {
                    totals.push((d.variant.clone(), Totals::default()));
                }
                let slot = totals
                    .iter_mut()
                    .find(|(l, _)| *l == d.variant)
                    .expect("just inserted");
                let comparable = slot.1.add(d);
                if resp.diffs.len() == 1 {
                    let short: String = q.chars().take(44).collect();
                    if comparable {
                        println!(
                            "{:<44} {:>6.0}% {:>7.3} {:>8.4} {:>5}",
                            short,
                            d.overlap_fraction * 100.0,
                            d.kendall_tau,
                            d.rbo,
                            if d.top1_flipped { "FLIP" } else { "" }
                        );
                    } else {
                        println!("{short:<44}    NOT COMPARABLE (an arm returned no hits)");
                    }
                }
            }
        }
        println!("\nover {} queries:", queries.len());
        for (label, t) in &totals {
            let n = t.queries.max(1) as f64;
            println!(
                "  [{label}] mean overlap {:.0}%, mean tau {:.3}, mean rbo {:.4}",
                t.overlap / n * 100.0,
                t.tau / n,
                t.rbo / n
            );
            println!(
                "  {:<width$} top-1 changed on {} of {}; {} diverged past what regret can judge",
                "",
                t.flips,
                t.queries,
                t.diverged,
                width = label.len() + 2
            );
            if t.incomparable > 0 {
                // Excluded from every mean above, and said out loud: a
                // query one arm cannot answer is a finding, and silently
                // dropping it would shrink the denominator invisibly.
                println!(
                    "  {:<width$} {} more queries EXCLUDED: an arm returned no hits at all",
                    "",
                    t.incomparable,
                    width = label.len() + 2
                );
            }
            if t.tau_worse + t.tau_better > 0 {
                println!(
                    "  {:<width$} disagreed on {} ({} substantially, {} at the margins)",
                    "",
                    t.tau_worse + t.tau_better,
                    t.tau_worse,
                    t.tau_better,
                    width = label.len() + 2
                );
            }
        }
        println!(
            "\nThese measure disagreement, not quality. Pick the winner with labels \n\
             or by interleaving (--interleave on a single query), not from this table."
        );
        return Ok(());
    }

    let query = &queries[0];
    let variants: Vec<SearchVariant> = specs
        .iter()
        .map(|s| parse_arm(s, query))
        .collect::<Result<_, _>>()?;
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
        // An arm that matched nothing is a fact about the arm, not a
        // ranking to compare: say so where it cannot be missed, because
        // every measure below degenerates on it.
        if r.hits.is_empty() {
            println!("[{}]  NO HITS -- this arm matched no document", r.label);
            continue;
        }
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
        // depth 0 means one arm returned nothing, so there was no
        // comparison to make. Printing the numbers anyway would show a
        // row of plausible values for a measurement that did not happen.
        if d.depth == 0 {
            println!(
                "{:<20} {:>6} NOT COMPARABLE -- an arm returned no hits",
                d.variant, d.depth
            );
            continue;
        }
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
            // never scored, so regret genuinely cannot judge them. Their
            // presence also breaks the cancellation that makes regret's
            // sign readable, so say so rather than letting the number be
            // read as "the variant did better".
            println!(
                "{:<20} {} of {} outside the reference: regret is a subset \
                 comparison here, read tau/rbo instead",
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

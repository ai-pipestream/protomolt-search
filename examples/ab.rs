//! Drive `SearchService.VariantSearch` from the command line: run two or
//! more query configurations over the live cluster and print how far
//! apart their rankings are.
//!
//! An arm is written as `label=field[:weight][+field[:weight]...]`, so
//! the A/B that motivated this — one body column against another indexed
//! under a different analyzer — is a one-liner:
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
//! An arm may carry `@key=value` options after its fields. `@k1=` and
//! `@b=` override the BM25 parameters, which is how a length
//! normalization sweep is run without reindexing anything:
//!
//! ```text
//! ab --arm=default=body --arm=flat=body@b=0.3 --queries=...
//! ```
//!
//! The literal field name `hybrid` selects the hybrid path instead of the
//! lexical one, where `@fusion=` picks the strategy. This needs a query
//! vector, so `--analysis-addr` must point at the embedding sidecar:
//!
//! ```text
//! ab --arm=cascade=hybrid@fusion=cascade \
//!    --arm=two-level=hybrid@fusion=two_level --queries=...
//! ```
//!
//! Every arm is queried with the BODY FIELD'S INGEST ANALYZER by
//! default, because term identity is fixed at ingest: the analysis
//! sidecar's own default does not stem, so querying this index with it
//! matches only the tokens that happen to equal their own stem and
//! silently reports the ranking of that fragment. `--analyzer=server`
//! leaves the spec unset for callers that want the sidecar default.
//!
//! A field may name its OWN analyzer with `/name`, which is what an
//! arm comparing two columns indexed under different analyzers needs —
//! the comparison this tool exists for, and the one `--analyzer` alone
//! cannot express, since it sets one analyzer for every field:
//!
//! ```text
//! ab --arm=folded=body_norm/folded --arm=cased=body_cased/cased \
//!    --queries=... --interleave
//! ```
//!
//! Named analyzers come from `analyzer::analyzer_by_name`, so the CLI
//! and the ingest side cannot drift apart into two vocabularies.

use std::time::Instant;

use turbovec_search::analyzer;
use turbovec_search::pb::search_service_client::SearchServiceClient;
use turbovec_search::pb::{
    search_variant, AnalysisSpec, Bm25SearchRequest, FusionMode, HybridLegOptions,
    HybridSearchRequest, InterleaveTeam, QueryField, SearchVariant, VariantSearchRequest,
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

/// The unit an arm's scores are expressed in.
///
/// `score_regret` subtracts one arm's score from another's, which is a
/// number only when both came out of the same scoring function. Raw BM25,
/// an RRF rank sum, a normalized blend and a weighted fused sum are four
/// different units; differencing across them yields a value that formats
/// perfectly and means nothing. Arms carry their unit so the report can
/// refuse the subtraction instead of printing that value.
#[derive(PartialEq, Clone, Debug)]
enum Scale {
    /// Raw BM25 over the given normalized field spec.
    Bm25(String),
    /// Reciprocal rank fusion (GLOBAL_RANK, TWO_LEVEL).
    Rrf,
    /// Normalized-and-combined leg scores (SCORE_BLEND).
    Blend,
    /// Raw weighted sum of the legs (DECOMPOSED).
    FusedSum,
    /// Cascade reports the reranked BM25 score of a vector-gated pool.
    /// Not [`Scale::Bm25`]: the same document can be absent here for
    /// want of vector recall, so the two are the same unit over
    /// different populations, and only a same-mode pair is safe.
    CascadeBm25,
}

impl std::fmt::Display for Scale {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Scale::Bm25(spec) => write!(f, "bm25({spec})"),
            Scale::Rrf => write!(f, "rrf"),
            Scale::Blend => write!(f, "blend"),
            Scale::FusedSum => write!(f, "fused-sum"),
            Scale::CascadeBm25 => write!(f, "cascade-bm25"),
        }
    }
}

/// One configuration under test: everything except the query text, which
/// changes per query while the arm does not.
struct Arm {
    label: String,
    /// Lexical arm: the fields to score. Empty means the default
    /// single-field route (`fanout_bm25_seeded`) rather than the fused
    /// multi-field one — a different code path over the same body
    /// postings, which is worth being able to name as an arm.
    fields: Vec<QueryField>,
    /// Hybrid arm: the leg options. `None` for a lexical arm.
    legs: Option<HybridLegOptions>,
    /// Seeded lexical floor, forwarded to every shard. 0 = unseeded.
    min_score: f32,
    scale: Scale,
}

impl Arm {
    fn is_hybrid(&self) -> bool {
        self.legs.is_some()
    }

    fn build(&self, text: &str, vector: &[f32], analysis: Option<&AnalysisSpec>) -> SearchVariant {
        let query = match &self.legs {
            Some(legs) => search_variant::Query::Hybrid(HybridSearchRequest {
                request_id: String::new(),
                text: text.to_string(),
                vector: vector.to_vec(),
                k: 0, // the request's shared k wins
                analysis: analysis.cloned(),
                legs: Some(*legs),
                debug: false,
                boost: None,
            }),
            None => search_variant::Query::Bm25(Bm25SearchRequest {
                text: text.to_string(),
                k: 0,
                // Request-level analysis reaches the single-field route
                // only; a fused request carries the spec PER FIELD, and
                // setting both is refused. Put it where it is read.
                analysis: self.fields.is_empty().then(|| analysis.cloned()).flatten(),
                min_score: self.min_score,
                fields: self
                    .fields
                    .iter()
                    .map(|f| QueryField {
                        // A field's OWN analyzer wins; `--analyzer` is
                        // only the fallback for fields that named none.
                        // Overwriting here (the previous behavior) is
                        // what made a two-column comparison inexpressible.
                        analysis: f.analysis.clone().or_else(|| analysis.cloned()),
                        ..f.clone()
                    })
                    .collect(),
            }),
        };
        SearchVariant {
            label: self.label.clone(),
            query: Some(query),
        }
    }
}

fn parse_fusion(v: &str) -> Result<(FusionMode, Scale), String> {
    Ok(match v.replace('-', "_").as_str() {
        "cascade" => (FusionMode::Cascade, Scale::CascadeBm25),
        "global_rank" | "global" => (FusionMode::GlobalRank, Scale::Rrf),
        "two_level" => (FusionMode::TwoLevel, Scale::Rrf),
        "score_blend" | "blend" => (FusionMode::ScoreBlend, Scale::Blend),
        "decomposed" => (FusionMode::Decomposed, Scale::FusedSum),
        other => {
            return Err(format!(
                "unknown fusion mode {other:?}; expected one of \
                 cascade, global_rank, two_level, score_blend, decomposed"
            ))
        }
    })
}

/// `label=field[:weight][+field[:weight]...][@key=value...]`, or
/// `label=hybrid[@key=value...]`.
fn parse_arm(spec: &str) -> Result<Arm, String> {
    let (label, rest) = spec
        .split_once('=')
        .ok_or_else(|| format!("arm {spec:?}: expected label=field[+field...]"))?;
    if label.is_empty() {
        return Err(format!("arm {spec:?}: empty label"));
    }
    let mut parts = rest.split('@');
    let fields = parts.next().unwrap_or_default();
    let mut opts: Vec<(String, String)> = Vec::new();
    for o in parts {
        let (k, v) = o
            .split_once('=')
            .ok_or_else(|| format!("arm {label}: option {o:?}: expected key=value"))?;
        opts.push((k.to_string(), v.to_string()));
    }
    let num = |k: &str, v: &str| -> Result<f32, String> {
        v.parse::<f32>()
            .map_err(|e| format!("arm {label}: {k}={v:?}: {e}"))
    };

    if fields == "hybrid" {
        let mut legs = HybridLegOptions::default();
        // Absent fusion resolves to CASCADE at the server. Requiring it
        // keeps the arm's label honest about what actually ran: the
        // whole point of this comparison is which mode is which.
        let mut scale = None;
        for (k, v) in &opts {
            match k.as_str() {
                "fusion" => {
                    let (mode, s) = parse_fusion(v).map_err(|e| format!("arm {label}: {e}"))?;
                    legs.fusion_mode = mode as i32;
                    scale = Some(s);
                }
                "leg_k" => {
                    legs.leg_k = v
                        .parse()
                        .map_err(|e| format!("arm {label}: leg_k={v:?}: {e}"))?
                }
                "vw" => legs.vector_weight = Some(num(k, v)?),
                "bw" => legs.bm25_weight = Some(num(k, v)?),
                // Silently ignoring a misplaced knob would report a
                // comparison of two identical arms as "no difference".
                "k1" | "b" | "min_score" => {
                    return Err(format!(
                        "arm {label}: @{k} is a lexical parameter and the hybrid \
                         request has nowhere to put it. Sweep {k} on a lexical arm."
                    ))
                }
                other => return Err(format!("arm {label}: unknown option {other:?}")),
            }
        }
        let scale = scale.ok_or_else(|| {
            format!("arm {label}: hybrid arms need @fusion=<mode>; an unset mode resolves to cascade at the server, which makes the arm's label a guess")
        })?;
        return Ok(Arm {
            label: label.to_string(),
            fields: Vec::new(),
            legs: Some(legs),
            min_score: 0.0,
            scale,
        });
    }

    let (mut k1, mut b) = (0.0f32, 0.0f32);
    let mut min_score = 0.0f32;
    for (k, v) in &opts {
        match k.as_str() {
            "k1" => k1 = num(k, v)?,
            "b" => b = num(k, v)?,
            "min_score" => min_score = num(k, v)?,
            "fusion" | "leg_k" | "vw" | "bw" => {
                return Err(format!(
                    "arm {label}: @{k} is a hybrid option, but this arm scores \
                     fields directly. Write the field list as `hybrid` to use \
                     the fused path."
                ))
            }
            other => return Err(format!("arm {label}: unknown option {other:?}")),
        }
    }
    // The reserved spelling `default` sends NO field list, which is what
    // routes a query to `fanout_bm25_seeded`. Naming it as an arm is the
    // only way to A/B the deployed single-field route against the fused
    // multi-field one on the same postings.
    if fields == "default" {
        if k1 != 0.0 || b != 0.0 {
            return Err(format!(
                "arm {label}: @k1/@b are per-field parameters and the `default` route \
                 sends no field list. Write the field name instead."
            ));
        }
        return Ok(Arm {
            label: label.to_string(),
            fields: Vec::new(),
            legs: None,
            min_score,
            scale: Scale::Bm25("default-route".to_string()),
        });
    }
    let mut query_fields = Vec::new();
    let mut idents = Vec::new();
    for part in fields.split('+') {
        // `/analyzer` binds to the field, so it is split off before the
        // `:weight` suffix.
        let (field_spec, analyzer) = match part.split_once('/') {
            Some((f, a)) => (f, Some(a)),
            None => (part, None),
        };
        let (name, weight) = match field_spec.split_once(':') {
            Some((n, w)) => (
                n,
                w.parse::<f32>()
                    .map_err(|e| format!("arm {label}: weight {w:?}: {e}"))?,
            ),
            None => (field_spec, 1.0),
        };
        if name.is_empty() {
            return Err(format!("arm {label}: empty field name"));
        }
        let analysis = match analyzer {
            Some(a) => analyzer::analyzer_by_name(a)
                .map_err(|e| format!("arm {label}: field {name:?}: {e}"))?,
            None => None,
        };
        // Two columns under different analyzers hold DIFFERENT TERMS, so
        // their idf and their scores come out of different scoring
        // spaces. Naming the analyzer in the identity is what stops
        // `score_regret` subtracting across them.
        idents.push(format!(
            "{}:{}:{}:{}:{}",
            name,
            weight,
            k1,
            b,
            analyzer.unwrap_or("-")
        ));
        query_fields.push(QueryField {
            field: name.to_string(),
            analysis,
            weight,
            k1,
            b,
        });
    }
    // The scale identity includes k1/b: the same field at two different
    // b values is two different scoring functions, and their scores are
    // no more subtractable than BM25 and RRF are.
    let ident = idents.join("+");
    Ok(Arm {
        label: label.to_string(),
        fields: query_fields,
        legs: None,
        min_score,
        scale: Scale::Bm25(ident),
    })
}

/// Running totals for a whole query set.
///
/// One query's diff is an observation, not a decision: the arms can
/// disagree wildly on a query neither answers well. A choice between two
/// analysis chains needs the aggregate, and the SPREAD matters as much as
/// the mean -- a chain that helps half the queries and hurts the other
/// half averages to "no change" and is not that.
/// Per-arm cost, which is half of what a mode comparison is asked.
///
/// `elapsed_ms` comes from the server, where the arms run sequentially,
/// so it is the arm's own time rather than a measure of how hard the
/// other arms were hitting the same shards.
#[derive(Default)]
struct ArmObs {
    ms: Vec<f64>,
    hits: usize,
    empty: usize,
}

impl ArmObs {
    fn report(&self, label: &str, scale: &Scale, queries: usize) {
        let mut ms = self.ms.clone();
        ms.sort_by(|a, b| a.partial_cmp(b).expect("no NaN timings"));
        let p = |q: usize| -> f64 {
            if ms.is_empty() {
                return f64::NAN;
            }
            ms[(ms.len() * q / 100).min(ms.len() - 1)]
        };
        println!(
            "  {label:<14} p50 {:7.1}  p90 {:7.1}  p99 {:7.1}  max {:7.1}   {:.1} hits/query  [{scale}]",
            p(50),
            p(90),
            p(99),
            ms.last().copied().unwrap_or(f64::NAN),
            self.hits as f64 / queries.max(1) as f64,
        );
        if self.empty > 0 {
            // An arm that matches nothing is fast for the wrong reason.
            println!(
                "  {:<14} {} of {queries} queries returned NO HITS -- those timings are \
                 the cost of finding nothing",
                "", self.empty
            );
        }
    }
}

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
            "need at least two --arm=label=field[+field:weight][@k=v] (got {})\n\
             example: --arm=control=body --arm=folded=body_norm\n\
             example: --arm=cascade=hybrid@fusion=cascade --arm=gr=hybrid@fusion=global_rank",
            specs.len()
        );
        std::process::exit(2);
    }
    let arms: Vec<Arm> = specs
        .iter()
        .map(|s| parse_arm(s))
        .collect::<Result<_, String>>()?;
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

    // Default to the ingest spec, NOT the sidecar's default. Term
    // identity is fixed at ingest: querying a stemmed index with the
    // sidecar's unstemmed default silently drops most of the query and
    // reports whatever the surviving tokens matched.
    // The default fallback for fields that did not name their own.
    let analysis = match analyzer::analyzer_by_name(&arg("analyzer", "ingest")) {
        Ok(spec) => spec,
        Err(e) => {
            eprintln!("--analyzer: {e}");
            std::process::exit(2);
        }
    };
    // Only pay for the sidecar when an arm actually needs a vector, so a
    // purely lexical A/B still runs with no embedder in reach.
    let needs_vector = arms.iter().any(Arm::is_hybrid);
    let analysis_addr = arg("analysis-addr", "http://127.0.0.1:59202");
    let mut embed_ms: Vec<f64> = Vec::new();

    let mut client = SearchServiceClient::connect(format!("http://{coord}")).await?;
    let mut obs: Vec<ArmObs> = arms.iter().map(|_| ArmObs::default()).collect();

    // Query-set mode: per-query one-liners, then the aggregate.
    if queries.len() > 1 {
        let mut totals: Vec<(String, Totals)> = Vec::new();
        println!(
            "{:<44} {:>7} {:>7} {:>8} {:>5}",
            "query", "overlap", "tau-b", "rbo", "top1"
        );
        for q in &queries {
            let vector = if needs_vector {
                let t = Instant::now();
                let v = analyzer::embed_text(&analysis_addr, q).await?;
                embed_ms.push(t.elapsed().as_secs_f64() * 1000.0);
                v
            } else {
                Vec::new()
            };
            let variants: Vec<SearchVariant> = arms
                .iter()
                .map(|a| a.build(q, &vector, analysis.as_ref()))
                .collect();
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
            for (i, r) in resp.results.iter().enumerate() {
                obs[i].ms.push(f64::from(r.elapsed_ms));
                obs[i].hits += r.hits.len();
                if r.hits.is_empty() {
                    obs[i].empty += 1;
                }
            }
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
        println!(
            "\nper-arm cost over {} queries (server-side, arms run sequentially):",
            queries.len()
        );
        for (i, a) in arms.iter().enumerate() {
            obs[i].report(&a.label, &a.scale, queries.len());
        }
        if !embed_ms.is_empty() {
            embed_ms.sort_by(|a, b| a.partial_cmp(b).expect("no NaN timings"));
            println!(
                "  {:<14} p50 {:7.1}  (once per query, shared by every hybrid arm, \
                 excluded above)",
                "embed",
                embed_ms[embed_ms.len() / 2]
            );
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
            // Same rule as the single-query table: regret only speaks
            // when both arms scored the same way.
            let same_scale = arms
                .iter()
                .find(|a| a.label == *label)
                .is_some_and(|a| a.scale == arms[0].scale);
            let regret_note = if same_scale {
                format!("{} diverged past what regret can judge", t.diverged)
            } else {
                "regret N/A: this arm scores in a different unit than the reference".to_string()
            };
            println!(
                "  {:<width$} top-1 changed on {} of {}; {regret_note}",
                "",
                t.flips,
                t.queries,
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
    let vector = if needs_vector {
        analyzer::embed_text(&analysis_addr, query).await?
    } else {
        Vec::new()
    };
    let variants: Vec<SearchVariant> = arms
        .iter()
        .map(|a| a.build(query, &vector, analysis.as_ref()))
        .collect();
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
        let scale = arms
            .iter()
            .find(|a| a.label == r.label)
            .map(|a| a.scale.to_string())
            .unwrap_or_else(|| "?".to_string());
        println!(
            "[{}]  {} hits  {:.1} ms  scores in {scale}",
            r.label,
            r.hits.len(),
            r.elapsed_ms
        );
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
        // Regret differences two arms' scores, which is only a quantity
        // when both arms produced them the same way. Across scales the
        // subtraction is unit-mixing, so print the reason rather than
        // the number.
        let variant_scale = arms
            .iter()
            .find(|a| a.label == d.variant)
            .map(|a| a.scale.clone())
            .expect("every diff names an arm we sent");
        let comparable_scale = variant_scale == arms[0].scale;
        let regret = if comparable_scale {
            format!("{:8.4}", d.score_regret)
        } else {
            format!("{:>8}", "SCALE")
        };
        println!(
            "{:<20} {:>6} {:>7.0}% {:>8.3} {:>10.4} {regret} {:>6}",
            d.variant,
            d.depth,
            d.overlap_fraction * 100.0,
            d.kendall_tau,
            d.rbo,
            if d.top1_flipped { "FLIP" } else { "same" },
        );
        if !comparable_scale {
            println!(
                "{:<20} arms score in different units ({} vs {variant_scale}); regret \
                 would be a subtraction across scales. Read tau/rbo/overlap.",
                "", arms[0].scale
            );
        }
        if comparable_scale && d.regret_unscored > 0 {
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

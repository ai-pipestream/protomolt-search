//! Acceptance matrix for a freshly rebuilt cluster: every surface the v7
//! bundle turned on, checked against a live coordinator, printed as a
//! pass/fail table.
//!
//! This is the gate a rebuild has to clear before it can be called done.
//! It asserts properties, not numbers, so it is corpus-independent: shard
//! reachability, the two BM25 fields scoring separately and fused, the
//! DECOMPOSED fused score being exactly the weighted sum of its legs,
//! document mode returning parents with their cross-shard chunk groups
//! bounded by the reported floor, and determinism across repeats.
//!
//! ```text
//! v7_verify --coord=127.0.0.1:59291 --analysis-addr=http://127.0.0.1:59202 \
//!     --query="qualified immunity" --case-name-query="United States"
//! ```

use turbovec_search::analyzer;
use turbovec_search::pb::search_service_client::SearchServiceClient;
use turbovec_search::pb::{
    search_variant, AnalysisSpec, Bm25SearchRequest, ClusterHealthRequest, FusionMode,
    HybridLegOptions, HybridSearchRequest, QueryField, SearchRequest, SearchVariant,
    VariantSearchRequest,
};

fn arg(key: &str, default: &str) -> String {
    let prefix = format!("--{key}=");
    std::env::args()
        .find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
        .unwrap_or_else(|| default.to_string())
}

/// The body field's ingest analysis: whitespace tokens, Porter stems.
fn body_spec() -> AnalysisSpec {
    AnalysisSpec {
        tokenizer: 1,
        stemmer: 2,
        term_vector_mode: 1,
        term_vector_source: 2,
        normalizer_rungs: vec![],
    }
}

/// The case_name field's ingest analysis: UNSTEMMED, tokens as identity.
fn case_name_spec() -> AnalysisSpec {
    AnalysisSpec {
        tokenizer: 1,
        stemmer: 1,
        term_vector_mode: 1,
        term_vector_source: 1,
        normalizer_rungs: vec![],
    }
}

struct Report {
    passed: usize,
    failed: usize,
}

impl Report {
    fn check(&mut self, name: &str, outcome: Result<String, String>) {
        match outcome {
            Ok(detail) => {
                self.passed += 1;
                println!("  PASS  {name:<46} {detail}");
            }
            Err(why) => {
                self.failed += 1;
                println!("  FAIL  {name:<46} {why}");
            }
        }
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let coord = arg("coord", "127.0.0.1:59291");
    let coord = if coord.starts_with("http") {
        coord
    } else {
        format!("http://{coord}")
    };
    let analysis_addr = arg("analysis-addr", "http://127.0.0.1:59202");
    let query = arg("query", "qualified immunity");
    let case_query = arg("case-name-query", "United States");
    let k: u32 = arg("k", "10").parse()?;
    let expect_shards: usize = arg("shards", "0").parse()?;
    // The cluster's slot-offset stride, so the report can tell which
    // shard owns a returned id. 0 = do not report it.
    let offset_stride: u64 = arg("offset-stride", "0").parse()?;

    let mut client = SearchServiceClient::connect(coord.clone()).await?;
    let mut r = Report {
        passed: 0,
        failed: 0,
    };
    println!("v7 acceptance matrix against {coord}");

    // --- fleet ---------------------------------------------------------
    //
    // A node binds its listener BEFORE it opens its .bm25, and opening a
    // 50 GB postings file reads every doc_length to count documents. The
    // kernel accepts connections into the backlog the whole time, so a
    // port check reports "ready" minutes before the node can answer, and
    // health probes time out against a fleet that is merely still
    // loading. Wait for actual readiness rather than raising the probe
    // timeout, which would only hide a slow node behind a slower check.
    let wait_ready: u64 = arg("wait-ready", "0").parse()?;
    // Readiness probe only: exit 0 the moment every shard answers, exit 1
    // on timeout. Lets an orchestrator gate on real readiness instead of
    // on a bound port, without running the whole matrix in a poll loop.
    let ready_only = std::env::args().any(|a| a == "--ready-only");
    if wait_ready > 0 {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(wait_ready);
        let started = std::time::Instant::now();
        loop {
            let probe = client
                .cluster_health(ClusterHealthRequest {})
                .await?
                .into_inner();
            let up = probe
                .targets
                .iter()
                .filter(|t| !t.is_replica && t.reachable)
                .count();
            let want = if expect_shards > 0 {
                expect_shards
            } else {
                probe.targets.iter().filter(|t| !t.is_replica).count()
            };
            if want > 0 && up == want {
                println!(
                    "  ready   {up}/{want} shards after {:.0}s",
                    started.elapsed().as_secs_f64()
                );
                if ready_only {
                    return Ok(());
                }
                break;
            }
            if std::time::Instant::now() >= deadline {
                // Do NOT pass silently: fall through and let the health
                // check below fail on the record.
                println!("  ready   TIMED OUT at {up}/{want} shards after {wait_ready}s");
                if ready_only {
                    std::process::exit(1);
                }
                break;
            }
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }
    } else if ready_only {
        eprintln!("--ready-only needs --wait-ready=<seconds>");
        std::process::exit(2);
    }
    let health = client
        .cluster_health(ClusterHealthRequest {})
        .await?
        .into_inner();
    let primaries: Vec<_> = health.targets.iter().filter(|t| !t.is_replica).collect();
    let up = primaries.iter().filter(|t| t.reachable).count();
    let vectors: u64 = primaries
        .iter()
        .filter_map(|t| t.health.as_ref())
        .map(|h| h.num_vectors)
        .sum();
    let docs: u64 = primaries
        .iter()
        .filter_map(|t| t.health.as_ref())
        .map(|h| h.bm25_docs)
        .sum();
    r.check(
        "cluster health: every shard reachable",
        if up == primaries.len() && (expect_shards == 0 || up == expect_shards) {
            Ok(format!("{up} shards, {vectors} vectors, {docs} bm25 docs"))
        } else {
            Err(format!(
                "{up}/{} healthy{}",
                primaries.len(),
                if expect_shards > 0 {
                    format!(", expected {expect_shards}")
                } else {
                    String::new()
                }
            ))
        },
    );
    r.check(
        "every shard finished its bulk build and holds vectors == docs",
        {
            // A shard with no health report is a FAILURE of this check,
            // not a shard to skip. Skipping it (which `?` inside the
            // closure used to do) meant that when every shard was
            // unreachable this reported "all shards consistent" -- a pass
            // over zero shards examined, printed directly beneath the
            // health check that had just failed 0/8.
            let mut bad: Vec<String> = Vec::new();
            let mut examined = 0usize;
            for t in &primaries {
                let Some(h) = t.health.as_ref() else {
                    bad.push(format!("shard {}: no health report", t.shard));
                    continue;
                };
                examined += 1;
                if h.bm25_building || h.num_vectors != h.bm25_docs || h.num_vectors == 0 {
                    bad.push(format!(
                        "shard {} {} vectors / {} docs{}",
                        t.shard,
                        h.num_vectors,
                        h.bm25_docs,
                        if h.bm25_building {
                            " (still building)"
                        } else {
                            ""
                        }
                    ));
                }
            }
            if expect_shards > 0 && examined != expect_shards {
                bad.push(format!(
                    "examined {examined} shards, expected {expect_shards}"
                ));
            }
            if bad.is_empty() && examined > 0 {
                Ok(format!("all {examined} shards consistent"))
            } else if bad.is_empty() {
                Err("no shards examined: nothing was verified".to_string())
            } else {
                Err(bad.join("; "))
            }
        },
    );

    // --- the vector leg, and document mode -----------------------------
    let vector = analyzer::embed_text(&analysis_addr, &query).await?;
    let plain = client
        .search(SearchRequest {
            request_id: String::new(),
            k,
            vector: vector.clone(),
            collapse_parents: false,
        })
        .await?
        .into_inner();
    r.check(
        "vector search returns k hits, score descending",
        if plain.hits.len() as u32 == k && plain.hits.windows(2).all(|w| w[0].score >= w[1].score) {
            Ok(format!("top {:.4}", plain.hits[0].score))
        } else {
            Err(format!("{} hits", plain.hits.len()))
        },
    );

    let doc_mode = client
        .search(SearchRequest {
            request_id: String::new(),
            k,
            vector: vector.clone(),
            collapse_parents: true,
        })
        .await?
        .into_inner();
    r.check("document mode: k distinct parents, one group each", {
        let parents: std::collections::HashSet<u64> =
            doc_mode.hits.iter().map(|h| h.parent_id).collect();
        if doc_mode.hits.len() as u32 == k
            && parents.len() == doc_mode.hits.len()
            && doc_mode.groups.len() == doc_mode.hits.len()
        {
            Ok(format!(
                "{} parents, {} chunks retrieved, floor {:.6}",
                parents.len(),
                doc_mode
                    .groups
                    .iter()
                    .map(|g| g.chunks.len())
                    .sum::<usize>(),
                doc_mode.chunk_floor
            ))
        } else {
            Err(format!(
                "{} hits, {} distinct parents, {} groups",
                doc_mode.hits.len(),
                parents.len(),
                doc_mode.groups.len()
            ))
        }
    });

    // Every group is its hit's parent, holds that hit's chunk, and holds
    // nothing below the reported floor: the completeness bound the
    // response advertises.
    r.check(
        "document mode: groups match hits and respect chunk_floor",
        {
            let mut bad = Vec::new();
            for (i, hit) in doc_mode.hits.iter().enumerate() {
                let g = &doc_mode.groups[i];
                if g.parent_id != hit.parent_id {
                    bad.push(format!(
                        "group {i} parent {} != hit {}",
                        g.parent_id, hit.parent_id
                    ));
                }
                if !g.chunks.iter().any(|c| c.vector_id == hit.vector_id) {
                    bad.push(format!("group {i} misses its own best chunk"));
                }
                if let Some(c) = g.chunks.iter().find(|c| c.score < doc_mode.chunk_floor) {
                    bad.push(format!("group {i} chunk {} below floor", c.vector_id));
                }
            }
            if bad.is_empty() {
                Ok("consistent".to_string())
            } else {
                Err(bad.join("; "))
            }
        },
    );

    // A parent whose chunks live on more than one shard is the case a
    // collapse-only search cannot serve. Which shard owns an id is
    // `id / offset_stride`, so this is only reportable when the caller
    // passes the stride the cluster was built with.
    if offset_stride > 0 {
        let straddlers = doc_mode
            .groups
            .iter()
            .filter(|g| {
                let shards: std::collections::HashSet<u64> = g
                    .chunks
                    .iter()
                    .map(|c| c.vector_id / offset_stride)
                    .collect();
                shards.len() > 1
            })
            .count();
        println!("  note  parents whose chunks span a shard cut: {straddlers}");
    }

    // --- the lexical legs ----------------------------------------------
    let body_only = client
        .bm25_search(Bm25SearchRequest {
            text: query.clone(),
            k,
            analysis: Some(body_spec()),
            min_score: 0.0,
            fields: Vec::new(),
        })
        .await?
        .into_inner();
    r.check(
        "bm25 body field scores",
        if !body_only.hits.is_empty() && body_only.hits[0].score > 0.0 {
            Ok(format!(
                "{} hits, top {:.4}",
                body_only.hits.len(),
                body_only.hits[0].score
            ))
        } else {
            Err("no scoring hits".to_string())
        },
    );

    let case_only = client
        .bm25_search(Bm25SearchRequest {
            text: case_query.clone(),
            k,
            analysis: None,
            min_score: 0.0,
            fields: vec![QueryField {
                field: "case_name".to_string(),
                analysis: Some(case_name_spec()),
                weight: 1.0,
                k1: 0.0,
                b: 0.0,
            }],
        })
        .await?
        .into_inner();
    r.check(
        "bm25 case_name field scores (the new field is populated)",
        if !case_only.hits.is_empty() && case_only.hits[0].score > 0.0 {
            Ok(format!(
                "{} hits, top {:.4}",
                case_only.hits.len(),
                case_only.hits[0].score
            ))
        } else {
            Err("no scoring hits: case_name is empty or was not ingested".to_string())
        },
    );

    let fused = client
        .bm25_search(Bm25SearchRequest {
            text: query.clone(),
            k,
            analysis: None,
            min_score: 0.0,
            fields: vec![
                QueryField {
                    field: "body".to_string(),
                    analysis: Some(body_spec()),
                    weight: 1.0,
                    k1: 0.0,
                    b: 0.0,
                },
                QueryField {
                    field: "case_name".to_string(),
                    analysis: Some(case_name_spec()),
                    weight: 2.0,
                    k1: 0.0,
                    b: 0.0,
                },
            ],
        })
        .await?
        .into_inner();
    r.check(
        "bm25 fused body+case_name query",
        if !fused.hits.is_empty() {
            Ok(format!(
                "{} hits, top {:.4}",
                fused.hits.len(),
                fused.hits[0].score
            ))
        } else {
            Err("no hits".to_string())
        },
    );

    // --- decomposed hybrid ---------------------------------------------
    let (wv, wb) = (1.0f32, 0.5f32);
    let decomposed = client
        .hybrid_search(HybridSearchRequest {
            request_id: String::new(),
            text: query.clone(),
            vector: vector.clone(),
            k,
            analysis: Some(body_spec()),
            legs: Some(HybridLegOptions {
                fusion_mode: FusionMode::Decomposed as i32,
                leg_k: k * 10,
                vector_weight: Some(wv),
                bm25_weight: Some(wb),
                ..Default::default()
            }),
            debug: true,
            boost: None,
        })
        .await?
        .into_inner();
    r.check(
        "decomposed hybrid: fused == w_v*vector + w_b*bm25, exactly",
        {
            let mut worst = 0.0f64;
            for h in &decomposed.hits {
                let expect = f64::from(wv) * f64::from(h.vector_score)
                    + f64::from(wb) * f64::from(h.bm25_score);
                worst = worst.max((f64::from(h.fused_score) - expect).abs());
            }
            let ordered = decomposed
                .hits
                .windows(2)
                .all(|w| w[0].fused_score >= w[1].fused_score);
            if decomposed.hits.len() as u32 == k && ordered && worst <= 1e-6 {
                Ok(format!(
                    "{} hits, max residual {worst:.3e}",
                    decomposed.hits.len()
                ))
            } else {
                Err(format!(
                    "{} hits, ordered={ordered}, max residual {worst:.3e}",
                    decomposed.hits.len()
                ))
            }
        },
    );
    if let Some(d) = &decomposed.debug {
        println!(
            "  note  decomposed: mode {:?}, leg_k {}, terms {:?}, {:.1} ms over {} shards",
            FusionMode::try_from(d.fusion_mode).unwrap_or(FusionMode::Unspecified),
            d.leg_k,
            d.terms,
            d.total_ms,
            d.shards.len()
        );
    }

    // Rerunning must reproduce the answer bit for bit: floors arrive at
    // racy times, and a result that depended on their timing would be a
    // correctness bug, not a performance detail.
    let again = client
        .hybrid_search(HybridSearchRequest {
            request_id: String::new(),
            text: query.clone(),
            vector: vector.clone(),
            k,
            analysis: Some(body_spec()),
            legs: Some(HybridLegOptions {
                fusion_mode: FusionMode::Decomposed as i32,
                leg_k: k * 10,
                vector_weight: Some(wv),
                bm25_weight: Some(wb),
                ..Default::default()
            }),
            debug: false,
            boost: None,
        })
        .await?
        .into_inner();
    r.check("decomposed hybrid is deterministic across repeats", {
        let same = decomposed.hits.len() == again.hits.len()
            && decomposed.hits.iter().zip(&again.hits).all(|(a, b)| {
                a.doc_id == b.doc_id && a.fused_score.to_bits() == b.fused_score.to_bits()
            });
        if same {
            Ok("bitwise identical".to_string())
        } else {
            Err("results differ".to_string())
        }
    });

    let plain_again = client
        .search(SearchRequest {
            request_id: String::new(),
            k,
            vector,
            collapse_parents: false,
        })
        .await?
        .into_inner();
    r.check("streaming vector search is deterministic across repeats", {
        let same =
            plain.hits.len() == plain_again.hits.len()
                && plain.hits.iter().zip(&plain_again.hits).all(|(a, b)| {
                    a.vector_id == b.vector_id && a.score.to_bits() == b.score.to_bits()
                });
        if same {
            Ok("bitwise identical".to_string())
        } else {
            Err("results differ".to_string())
        }
    });

    // The A/B surface, against the real corpus: two arms over the same
    // 86M documents, one scoring body alone and one fusing the caption.
    // Weighting a field must move the ranking, and an arm named for a
    // field nothing indexes must be refused rather than silently scored
    // as the remaining fields.
    let ab_arm = |label: &str, fields: Vec<QueryField>| SearchVariant {
        label: label.to_string(),
        query: Some(search_variant::Query::Bm25(Bm25SearchRequest {
            text: query.clone(),
            k: 0,
            analysis: None,
            min_score: 0.0,
            fields,
        })),
    };
    let body_field = || QueryField {
        field: "body".to_string(),
        analysis: Some(body_spec()),
        weight: 1.0,
        k1: 0.0,
        b: 0.0,
    };
    let name_field = |w: f32| QueryField {
        field: "case_name".to_string(),
        analysis: Some(case_name_spec()),
        weight: w,
        k1: 0.0,
        b: 0.0,
    };
    let variants = client
        .variant_search(VariantSearchRequest {
            request_id: String::new(),
            variants: vec![
                ab_arm("body-only", vec![body_field()]),
                ab_arm("caption-boosted", vec![body_field(), name_field(4.0)]),
            ],
            k,
            rbo_p: 0.0,
            interleave: true,
            interleave_seed: 0,
        })
        .await?
        .into_inner();
    r.check("variant search: two arms, one diff, both ranked", {
        let d = variants.diffs.first();
        match d {
            Some(d) if variants.results.len() == 2 && d.reference == "body-only" => Ok(format!(
                "overlap {:.0}%, tau {:.3}, rbo {:.3}, regret {:.4}{}",
                d.overlap_fraction * 100.0,
                d.kendall_tau,
                d.rbo,
                d.score_regret,
                if d.top1_flipped { ", top1 FLIPPED" } else { "" }
            )),
            _ => Err(format!(
                "expected 2 results and 1 diff, got {} and {}",
                variants.results.len(),
                variants.diffs.len()
            )),
        }
    });
    r.check(
        "variant search: interleaving is balanced and duplicate-free",
        {
            match &variants.interleaving {
                Some(il) => {
                    let mut ids = il.doc_ids.clone();
                    ids.sort_unstable();
                    ids.dedup();
                    let a = il.teams.iter().filter(|t| **t == 1).count();
                    let b = il.teams.len() - a;
                    if ids.len() != il.doc_ids.len() {
                        Err("a document appears twice".to_string())
                    } else if a.abs_diff(b) > 1 {
                        Err(format!("lopsided exposure: {a} vs {b}"))
                    } else {
                        Ok(format!(
                            "{} results, {a}/{b} split, seed {}",
                            il.doc_ids.len(),
                            il.seed
                        ))
                    }
                }
                None => Err("interleaving was requested but absent".to_string()),
            }
        },
    );
    r.check(
        "variant search: an unindexed field is refused, not scored as 0",
        {
            let mut bogus = name_field(1.0);
            bogus.field = "case_nmae".to_string();
            match client
                .variant_search(VariantSearchRequest {
                    request_id: String::new(),
                    variants: vec![
                        ab_arm("body-only", vec![body_field()]),
                        ab_arm("typo", vec![body_field(), bogus]),
                    ],
                    k,
                    rbo_p: 0.0,
                    interleave: false,
                    interleave_seed: 0,
                })
                .await
            {
                Ok(_) => Err("a field no shard indexes was silently scored".to_string()),
                Err(e) if e.message().contains("no shard indexes") => {
                    Ok("refused, naming the field".to_string())
                }
                Err(e) => Err(format!("refused for the wrong reason: {}", e.message())),
            }
        },
    );

    println!("\n{} passed, {} failed", r.passed, r.failed);
    if r.failed > 0 {
        std::process::exit(1);
    }
    Ok(())
}

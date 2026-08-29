//! The debug console: a tiny HTTP/JSON gateway plus a single-file web UI
//! for exercising the cluster's hybrid search knobs by hand.
//!
//! The console is a CLIENT of a running cluster, never part of it: it
//! embeds query text through the analysis sidecar's embedding model,
//! calls the coordinator's `HybridSearch` over gRPC, fetches hit texts
//! from the owning nodes (`GetDocuments`), and serves the UI plus two
//! JSON endpoints:
//!
//! - `POST /api/search` — run one hybrid query with explicit knobs
//! - `GET  /api/health` — the coordinator's `ClusterHealth`
//!
//! The HTTP side is deliberately minimal (hand-rolled HTTP/1.1,
//! `Connection: close`, same-origin UI): this is a localhost test
//! harness for one operator, not a product server.
//!
//! Flags (all `--key=value`): `--listen` (default `127.0.0.1:8600`),
//! `--coordinator` (default `http://127.0.0.1:50050`), `--nodes`
//! (comma-separated shard-owner addresses IN SHARD ORDER, for doc-text
//! fetches), `--analysis` (sidecar address, required for embedding).

use std::collections::HashMap;
use std::sync::Arc;

use serde::Deserialize;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tonic::transport::Channel;

use pipestream_search::analyzer;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_client::SearchServiceClient;
use pipestream_search::pb::{
    Bm25SearchRequest, BoostRescore, ClusterHealthRequest, FusionMode, GetDocumentsRequest,
    HybridDebug, HybridLegOptions, HybridSearchRequest, ScoreCombination, ScoreNormalization,
};

const CONSOLE_HTML: &str = include_str!("console.html");
/// Request bodies are tiny JSON; anything bigger is a mistake.
const MAX_BODY_BYTES: usize = 1 << 20;

struct Ctx {
    coordinator: String,
    nodes: Vec<String>,
    analysis: Option<String>,
}

fn flag(args: &[String], key: &str) -> Option<String> {
    let prefix = format!("--{key}=");
    args.iter()
        .find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
}

/// `host:port` -> `http://host:port`; already-schemed addresses pass.
fn grpc_addr(addr: &str) -> String {
    if addr.contains("://") {
        addr.to_string()
    } else {
        format!("http://{addr}")
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let listen = flag(&args, "listen").unwrap_or_else(|| "127.0.0.1:8600".to_string());
    let coordinator = grpc_addr(
        &flag(&args, "coordinator").unwrap_or_else(|| "http://127.0.0.1:50050".to_string()),
    );
    let nodes: Vec<String> = flag(&args, "nodes")
        .map(|list| {
            list.split(',')
                .filter(|s| !s.is_empty())
                .map(grpc_addr)
                .collect()
        })
        .unwrap_or_default();
    let analysis = flag(&args, "analysis").map(|a| grpc_addr(&a));

    let ctx = Arc::new(Ctx {
        coordinator,
        nodes,
        analysis,
    });
    let listener = TcpListener::bind(&listen).await?;
    eprintln!(
        "console on http://{listen} -> coordinator {} ({} node(s) for doc text, analysis {})",
        ctx.coordinator,
        ctx.nodes.len(),
        ctx.analysis
            .as_deref()
            .unwrap_or("NONE: embedding disabled")
    );
    loop {
        let (stream, _) = listener.accept().await?;
        let ctx = Arc::clone(&ctx);
        tokio::spawn(async move {
            let _ = handle_conn(stream, ctx).await;
        });
    }
}

async fn handle_conn(mut stream: TcpStream, ctx: Arc<Ctx>) -> std::io::Result<()> {
    // Read until the header terminator (or give up at the body cap).
    let mut buf: Vec<u8> = Vec::with_capacity(4096);
    let header_end = loop {
        let mut chunk = [0u8; 4096];
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&chunk[..n]);
        if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
            break pos + 4;
        }
        if buf.len() > MAX_BODY_BYTES {
            return respond(&mut stream, 431, "text/plain", b"headers too large").await;
        }
    };
    let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let mut lines = head.lines();
    let request_line = lines.next().unwrap_or_default();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let path = parts.next().unwrap_or_default().to_string();
    let content_length = lines
        .filter_map(|l| l.split_once(':'))
        .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, v)| v.trim().parse::<usize>().ok())
        .unwrap_or(0);
    if content_length > MAX_BODY_BYTES {
        return respond(&mut stream, 413, "text/plain", b"body too large").await;
    }
    let mut body = buf[header_end..].to_vec();
    while body.len() < content_length {
        let mut chunk = [0u8; 4096];
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&chunk[..n]);
    }

    match (method.as_str(), path.as_str()) {
        ("GET", "/") => {
            respond(
                &mut stream,
                200,
                "text/html; charset=utf-8",
                CONSOLE_HTML.as_bytes(),
            )
            .await
        }
        ("GET", "/api/health") => {
            let out = match health(&ctx).await {
                Ok(v) => (200, v),
                Err(e) => (502, json!({ "error": e })),
            };
            respond(
                &mut stream,
                out.0,
                "application/json",
                out.1.to_string().as_bytes(),
            )
            .await
        }
        ("POST", "/api/search") => {
            let out = match search(&ctx, &body).await {
                Ok(v) => (200, v),
                Err(e) => (400, json!({ "error": e })),
            };
            respond(
                &mut stream,
                out.0,
                "application/json",
                out.1.to_string().as_bytes(),
            )
            .await
        }
        _ => respond(&mut stream, 404, "text/plain", b"not found").await,
    }
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

async fn respond(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &[u8],
) -> std::io::Result<()> {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        413 => "Payload Too Large",
        431 => "Request Header Fields Too Large",
        _ => "Error",
    };
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(body).await?;
    stream.flush().await
}

fn search_client(addr: &str) -> Result<SearchServiceClient<Channel>, String> {
    Ok(
        SearchServiceClient::new(analyzer::shared_channel(addr).map_err(|e| e.to_string())?)
            .max_decoding_message_size(pipestream_search::MAX_MESSAGE_BYTES)
            .max_encoding_message_size(pipestream_search::MAX_MESSAGE_BYTES),
    )
}

fn node_client(addr: &str) -> Result<NodeServiceClient<Channel>, String> {
    Ok(
        NodeServiceClient::new(analyzer::shared_channel(addr).map_err(|e| e.to_string())?)
            .max_decoding_message_size(pipestream_search::MAX_MESSAGE_BYTES)
            .max_encoding_message_size(pipestream_search::MAX_MESSAGE_BYTES),
    )
}

async fn health(ctx: &Ctx) -> Result<Value, String> {
    let mut client = search_client(&ctx.coordinator)?;
    let response = client
        .cluster_health(ClusterHealthRequest {})
        .await
        .map_err(|e| e.to_string())?
        .into_inner();
    let targets: Vec<Value> = response
        .targets
        .iter()
        .map(|t| {
            json!({
                "shard": t.shard,
                "addr": t.addr,
                "is_replica": t.is_replica,
                "reachable": t.reachable,
                "error": t.error,
                "num_vectors": t.health.as_ref().map(|h| h.num_vectors),
                "bm25_docs": t.health.as_ref().map(|h| h.bm25_docs),
                "dim": t.health.as_ref().map(|h| h.dim),
                "ingest_active": t.health.as_ref().map(|h| h.ingest_active),
            })
        })
        .collect();
    Ok(json!({ "targets": targets }))
}

/// The console's one query shape. Everything except `text` has a
/// sensible default; `vector` bypasses the sidecar for setups without
/// an embedding model.
#[derive(Deserialize)]
#[serde(default)]
struct SearchBody {
    text: String,
    vector: Option<Vec<f32>>,
    k: u32,
    mode: String,
    leg_k: u32,
    rrf_k: f32,
    vector_weight: f32,
    bm25_weight: f32,
    normalization: String,
    combination: String,
    boost_text: String,
    boost_window: u32,
    boost_base_weight: f32,
    boost_weight: f32,
    tokenizer: String,
    stemmer: String,
    term_source: String,
    vector_leg: bool,
    bm25_leg: bool,
    min_vector_score: f32,
    /// CEL filter (docs/cel-filters.md). Empty = off. With the vector
    /// leg on it rides the hybrid route, which filters BOTH legs
    /// (docs/vector-filters.md); with the vector leg off it takes the
    /// lexical Bm25Search route, which also carries facet counts.
    filter: String,
    fetch_docs: bool,
}

impl Default for SearchBody {
    fn default() -> Self {
        Self {
            text: String::new(),
            vector: None,
            k: 10,
            mode: "global_rank".to_string(),
            leg_k: 0,
            rrf_k: 0.0,
            vector_weight: 0.0,
            bm25_weight: 0.0,
            normalization: "min_max".to_string(),
            combination: "arithmetic".to_string(),
            boost_text: String::new(),
            boost_window: 0,
            boost_base_weight: 0.0,
            boost_weight: 0.0,
            tokenizer: String::new(),
            stemmer: String::new(),
            term_source: String::new(),
            vector_leg: true,
            bm25_leg: true,
            min_vector_score: 0.0,
            filter: String::new(),
            fetch_docs: true,
        }
    }
}

async fn search(ctx: &Ctx, body: &[u8]) -> Result<Value, String> {
    let req: SearchBody = serde_json::from_slice(body).map_err(|e| format!("bad request: {e}"))?;
    if req.text.is_empty() {
        return Err("text is required".to_string());
    }

    // GLOBAL_RANK by default, not CASCADE. Cascade generates candidates
    // from the vector leg and only RERANKS that pool by BM25, so the
    // lexical leg cannot introduce a document. Measured over 36 queries
    // on the 86.6M-chunk corpus: cascade retained 0% of the pure-lexical
    // top-10 on every one of them, where global_rank retained 41%. On a
    // short query the vector leg's own top hits are section headings
    // whose text IS the query ("QUALIFIED IMMUNITY", "B. Qualified
    // Immunity"), and cascade hands all of them straight through --
    // mean length of its top 6 was 3 words against global_rank's 130.
    // Global rank is also the faster of the two here (p50 380 vs 428 ms,
    // p99 579 vs 686) and is the mode documented as reproducing the
    // monolithic result exactly.
    let fusion_mode = match req.mode.as_str() {
        "global_rank" | "" => FusionMode::GlobalRank,
        "cascade" => FusionMode::Cascade,
        "score_blend" => FusionMode::ScoreBlend,
        "two_level" => FusionMode::TwoLevel,
        other => return Err(format!("unknown mode {other:?}")),
    };
    let normalization = match req.normalization.as_str() {
        "min_max" | "" => ScoreNormalization::MinMax,
        "z_score" => ScoreNormalization::ZScore,
        "none" => ScoreNormalization::None,
        other => return Err(format!("unknown normalization {other:?}")),
    };
    let combination = match req.combination.as_str() {
        "arithmetic" | "" => ScoreCombination::Arithmetic,
        "geometric" => ScoreCombination::Geometric,
        "harmonic" => ScoreCombination::Harmonic,
        other => return Err(format!("unknown combination {other:?}")),
    };
    // Analysis spec: must match how the corpus was ingested, because term
    // identity is the whole contract. "default" therefore means THE
    // CORPUS SPEC, never "let the sidecar pick".
    //
    // Sending no spec used to look harmless and was not: the sidecar
    // resolves an absent spec to its own defaults (token-sourced,
    // unstemmed), so every query term arrived as a raw token, missed a
    // corpus of Porter stems, and scored df = 0. BM25 then returned
    // nothing while the page still rendered a ranked list, because the
    // vector leg answered normally. A lexical leg that silently matches
    // nothing is indistinguishable from one that legitimately found
    // nothing, which is exactly the failure this default prevents.
    let pick = |v: &str, options: &[(&str, i32)], corpus_default: i32| -> Result<i32, String> {
        if v.is_empty() || v == "default" {
            return Ok(corpus_default);
        }
        options
            .iter()
            .find(|(name, _)| *name == v)
            .map(|&(_, n)| n)
            .ok_or_else(|| format!("unknown value {v:?}"))
    };
    // Every default here comes from the one corpus spec rather than a
    // literal repeated at this call site. An override is a deliberate
    // A/B; a default that drifts from the index is a silent mismatch
    // that scores different terms instead of failing.
    let corpus = pipestream_search::analyzer::body_spec();
    let tokenizer = pick(
        &req.tokenizer,
        &[("whitespace", 1), ("simple", 2)],
        corpus.tokenizer,
    )?;
    let stemmer = pick(&req.stemmer, &[("none", 1), ("porter", 2)], corpus.stemmer)?;
    let term_source = pick(
        &req.term_source,
        &[("tokens", 1), ("stems", 2), ("normalized_stems", 3)],
        corpus.term_vector_source,
    )?;
    let analysis = Some(pipestream_search::pb::AnalysisSpec {
        tokenizer,
        stemmer,
        term_vector_mode: 0,
        term_vector_source: term_source,
        char_filters: corpus.char_filters.clone(),
    });

    let boost = (!req.boost_text.is_empty()).then(|| BoostRescore {
        text: req.boost_text.clone(),
        window: req.boost_window,
        base_weight: req.boost_base_weight,
        boost_weight: req.boost_weight,
    });

    // A filtered query with the vector leg off has no vector work to
    // do, and the lexical route carries facet counts the hybrid route
    // does not, so it stays the better answer there. With the vector
    // leg on, the hybrid route now filters BOTH legs.
    if !req.filter.is_empty() && !req.vector_leg && boost.is_none() {
        return bm25_filtered_search(ctx, &req, analysis).await;
    }

    // Query vector: pasted, or embedded through the sidecar.
    let t_embed = std::time::Instant::now();
    let vector = match req.vector {
        Some(v) if !v.is_empty() => v,
        _ => {
            let addr = ctx
                .analysis
                .as_deref()
                .ok_or("no --analysis sidecar configured, pass a raw vector instead")?;
            analyzer::embed_text(addr, &req.text)
                .await
                .map_err(|e| format!("embedding failed: {e}"))?
        }
    };
    let embed_ms = t_embed.elapsed().as_secs_f32() * 1e3;

    let request = HybridSearchRequest {
        request_id: String::new(),
        text: req.text.clone(),
        vector,
        k: if req.k == 0 { 10 } else { req.k },
        analysis,
        geo_filters: Vec::new(),
        filter: req.filter.clone(),
        legs: Some(HybridLegOptions {
            fusion_mode: fusion_mode as i32,
            leg_k: req.leg_k,
            // Unchecked leg = explicit 0 (disabled); weight 0 in the
            // form = absent (server default 1.0).
            vector_weight: if req.vector_leg {
                (req.vector_weight != 0.0).then_some(req.vector_weight)
            } else {
                Some(0.0)
            },
            bm25_weight: if req.bm25_leg {
                (req.bm25_weight != 0.0).then_some(req.bm25_weight)
            } else {
                Some(0.0)
            },
            rrf_k: req.rrf_k,
            normalization: normalization as i32,
            combination: combination as i32,
            min_vector_score: req.min_vector_score,
        }),
        debug: true,
        boost,
    };
    let mut client = search_client(&ctx.coordinator)?;
    let response = client
        .hybrid_search(request)
        .await
        .map_err(|e| format!("search failed: {e}"))?
        .into_inner();

    // Normalize the two hit shapes into one list, order preserved.
    let mut hits: Vec<Value> = Vec::new();
    for h in &response.hits {
        hits.push(json!({
            "doc_id": h.doc_id,
            "shard": h.shard,
            "fused_score": h.fused_score,
            "vector_rank": h.vector_rank,
            "vector_score": h.vector_score,
            "bm25_rank": h.bm25_rank,
            "bm25_score": h.bm25_score,
            "boost_score": h.boost_score,
        }));
    }
    for h in &response.cascade_hits {
        hits.push(json!({
            "doc_id": h.doc_id,
            "shard": h.shard,
            "rank": h.rank,
            "vector_score": h.vector_score,
            "bm25_score": h.bm25_score,
            "boost_score": h.boost_score,
        }));
    }

    // Doc texts from the owning nodes, in one GetDocuments per shard.
    let mut docs: HashMap<u64, Value> = HashMap::new();
    if req.fetch_docs && !ctx.nodes.is_empty() {
        let mut by_shard: HashMap<u32, Vec<u64>> = HashMap::new();
        for (doc_id, shard) in response
            .hits
            .iter()
            .map(|h| (h.doc_id, h.shard))
            .chain(response.cascade_hits.iter().map(|h| (h.doc_id, h.shard)))
        {
            by_shard.entry(shard).or_default().push(doc_id);
        }
        for (shard, doc_ids) in by_shard {
            let Some(addr) = ctx.nodes.get(shard as usize) else {
                continue;
            };
            let mut client = node_client(addr)?;
            let found = client
                .get_documents(GetDocumentsRequest { doc_ids })
                .await
                .map_err(|e| format!("get_documents on shard {shard} failed: {e}"))?
                .into_inner();
            for doc in found.documents {
                docs.insert(
                    doc.doc_id,
                    json!({
                        "text": doc.text,
                        "lineage": doc.lineage.map(|l| json!({
                            "parent_id": l.parent_id,
                            "group_id": l.group_id,
                            "span_start": l.span_start,
                            "span_end": l.span_end,
                        })),
                    }),
                );
            }
        }
    }
    for hit in &mut hits {
        let id = hit["doc_id"].as_u64().unwrap_or(0);
        if let Some(doc) = docs.get(&id) {
            hit["text"] = doc["text"].clone();
            hit["lineage"] = doc["lineage"].clone();
        }
    }

    Ok(json!({
        "request_id": response.request_id,
        "mode": req.mode,
        "embed_ms": embed_ms,
        "hits": hits,
        "debug": response.debug.map(|d| debug_json(&d)),
    }))
}

/// The lexical route a CEL filter takes (`docs/cel-filters.md`): one
/// Bm25Search carrying the filter string, hits shaped like the hybrid
/// list's lexical half. Documents are fetched by probing every node
/// with the whole id list — a flat Bm25Hit does not carry its shard,
/// and a debug console prefers one honest broadcast over a guess.
async fn bm25_filtered_search(
    ctx: &Ctx,
    req: &SearchBody,
    analysis: Option<pipestream_search::pb::AnalysisSpec>,
) -> Result<Value, String> {
    let t0 = std::time::Instant::now();
    let mut client = search_client(&ctx.coordinator)?;
    let response = client
        .bm25_search(Bm25SearchRequest {
            text: req.text.clone(),
            k: if req.k == 0 { 10 } else { req.k },
            analysis,
            filter: req.filter.clone(),
            ..Default::default()
        })
        .await
        .map_err(|e| format!("search failed: {e}"))?
        .into_inner();
    let search_ms = t0.elapsed().as_secs_f32() * 1e3;
    let mut hits: Vec<Value> = response
        .hits
        .iter()
        .enumerate()
        .map(|(i, h)| {
            json!({
                "doc_id": h.doc_id,
                "shard": "-",
                "rank": i + 1,
                "bm25_score": h.score,
                "bm25_rank": i + 1,
            })
        })
        .collect();
    if req.fetch_docs && !ctx.nodes.is_empty() && !response.hits.is_empty() {
        let doc_ids: Vec<u64> = response.hits.iter().map(|h| h.doc_id).collect();
        for addr in &ctx.nodes {
            let mut node = node_client(addr)?;
            let found = node
                .get_documents(GetDocumentsRequest {
                    doc_ids: doc_ids.clone(),
                })
                .await
                .map_err(|e| format!("get_documents failed: {e}"))?
                .into_inner();
            for doc in found.documents {
                for hit in &mut hits {
                    if hit["doc_id"].as_u64() == Some(doc.doc_id) {
                        hit["text"] = json!(doc.text);
                        hit["lineage"] = doc
                            .lineage
                            .as_ref()
                            .map(|l| {
                                json!({
                                    "parent_id": l.parent_id,
                                    "group_id": l.group_id,
                                    "span_start": l.span_start,
                                    "span_end": l.span_end,
                                })
                            })
                            .into();
                    }
                }
            }
        }
    }
    Ok(json!({
        "request_id": "",
        "mode": "bm25+filter",
        "embed_ms": 0.0,
        "search_ms": search_ms,
        "kth_best": response.kth_best,
        "hits": hits,
        "debug": Value::Null,
    }))
}

fn debug_json(d: &HybridDebug) -> Value {
    json!({
        "fusion_mode": d.fusion_mode,
        "leg_k": d.leg_k,
        "terms": d.terms,
        "boost_terms": d.boost_terms,
        "analysis_ms": d.analysis_ms,
        "stats_ms": d.stats_ms,
        "legs_ms": d.legs_ms,
        "fusion_ms": d.fusion_ms,
        "boost_ms": d.boost_ms,
        "total_ms": d.total_ms,
        "shards": d.shards.iter().map(|s| json!({
            "shard": s.shard,
            "rpc_ms": s.rpc_ms,
            "vector_hits": s.vector_hits,
            "bm25_hits": s.bm25_hits,
            "scan": s.scan.as_ref().map(|scan| json!({
                "chunk_calls": scan.chunk_calls,
                "candidates_collected": scan.candidates_collected,
                "floors_published": scan.floors_published,
                "floor_updates_applied": scan.floor_updates_applied,
            })),
        })).collect::<Vec<_>>(),
    })
}

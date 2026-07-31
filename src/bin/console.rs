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

use turbovec_search::analyzer;
use turbovec_search::pb::node_service_client::NodeServiceClient;
use turbovec_search::pb::search_service_client::SearchServiceClient;
use turbovec_search::pb::{
    BoostRescore, ClusterHealthRequest, FusionMode, GetDocumentsRequest, HybridDebug,
    HybridLegOptions, HybridSearchRequest, ScoreCombination, ScoreNormalization,
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
        ctx.analysis.as_deref().unwrap_or("NONE: embedding disabled")
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
        ("GET", "/") => respond(&mut stream, 200, "text/html; charset=utf-8", CONSOLE_HTML.as_bytes()).await,
        ("GET", "/api/health") => {
            let out = match health(&ctx).await {
                Ok(v) => (200, v),
                Err(e) => (502, json!({ "error": e })),
            };
            respond(&mut stream, out.0, "application/json", out.1.to_string().as_bytes()).await
        }
        ("POST", "/api/search") => {
            let out = match search(&ctx, &body).await {
                Ok(v) => (200, v),
                Err(e) => (400, json!({ "error": e })),
            };
            respond(&mut stream, out.0, "application/json", out.1.to_string().as_bytes()).await
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
    Ok(SearchServiceClient::new(
        analyzer::shared_channel(addr).map_err(|e| e.to_string())?,
    )
    .max_decoding_message_size(turbovec_search::MAX_MESSAGE_BYTES)
    .max_encoding_message_size(turbovec_search::MAX_MESSAGE_BYTES))
}

fn node_client(addr: &str) -> Result<NodeServiceClient<Channel>, String> {
    Ok(NodeServiceClient::new(
        analyzer::shared_channel(addr).map_err(|e| e.to_string())?,
    )
    .max_decoding_message_size(turbovec_search::MAX_MESSAGE_BYTES)
    .max_encoding_message_size(turbovec_search::MAX_MESSAGE_BYTES))
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
    fetch_docs: bool,
}

impl Default for SearchBody {
    fn default() -> Self {
        Self {
            text: String::new(),
            vector: None,
            k: 10,
            mode: "cascade".to_string(),
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
            fetch_docs: true,
        }
    }
}

async fn search(ctx: &Ctx, body: &[u8]) -> Result<Value, String> {
    let req: SearchBody = serde_json::from_slice(body).map_err(|e| format!("bad request: {e}"))?;
    if req.text.is_empty() {
        return Err("text is required".to_string());
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

    let fusion_mode = match req.mode.as_str() {
        "cascade" | "" => FusionMode::Cascade,
        "global_rank" => FusionMode::GlobalRank,
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
    // Analysis spec: must match how the corpus was ingested (term
    // identity above all). Empty selects the sidecar defaults.
    let pick = |v: &str, options: &[(&str, i32)]| -> Result<i32, String> {
        if v.is_empty() || v == "default" {
            return Ok(0);
        }
        options
            .iter()
            .find(|(name, _)| *name == v)
            .map(|&(_, n)| n)
            .ok_or_else(|| format!("unknown value {v:?}"))
    };
    let tokenizer = pick(&req.tokenizer, &[("whitespace", 1), ("simple", 2)])?;
    let stemmer = pick(&req.stemmer, &[("none", 1), ("porter", 2)])?;
    let term_source = pick(&req.term_source, &[("tokens", 1), ("stems", 2)])?;
    let analysis = (tokenizer != 0 || stemmer != 0 || term_source != 0).then(|| {
        turbovec_search::pb::AnalysisSpec {
            tokenizer,
            stemmer,
            term_vector_mode: 0,
            term_vector_source: term_source,
            normalizer_rungs: Vec::new(),
        }
    });

    let boost = (!req.boost_text.is_empty()).then(|| BoostRescore {
        text: req.boost_text.clone(),
        window: req.boost_window,
        base_weight: req.boost_base_weight,
        boost_weight: req.boost_weight,
    });

    let request = HybridSearchRequest {
        request_id: String::new(),
        text: req.text.clone(),
        vector,
        k: if req.k == 0 { 10 } else { req.k },
        analysis,
        legs: Some(HybridLegOptions {
            fusion_mode: fusion_mode as i32,
            leg_k: req.leg_k,
            vector_weight: req.vector_weight,
            bm25_weight: req.bm25_weight,
            rrf_k: req.rrf_k,
            normalization: normalization as i32,
            combination: combination as i32,
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
                            "opinion_id": l.opinion_id,
                            "cluster_id": l.cluster_id,
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

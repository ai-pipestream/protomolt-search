//! The console facade (`docs/console-facade.md`, `src/console.rs`): the
//! JSON transcoding of the public RPCs against a live cluster, the
//! status mapping, the server-sent event stream, the convenience routes,
//! the static UI, and the loopback rule.

mod common;

use std::net::SocketAddr;
use std::sync::Arc;

use pipestream_search::console::{Console, ConsoleConfig};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::diagnostics::{CoordinatorDiagnostics, RecentRing};
use pipestream_search::node::NodeConfig;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_client::SearchServiceClient;
use pipestream_search::pb::{
    search_query, selection_query, AddDocumentsRequest, AddVectorsRequest, FacetValue,
    IntegerValue, LexicalQuery, QueryRequest, SearchQuery, SelectionQuery, SetCalibrationRequest,
};
use pipestream_search::security::ToolClient;
use pipestream_search::MAX_MESSAGE_BYTES;
use prost::Message;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;

use common::{fit_calibration, mock::start_mock_analysis, start_empty_node, unit_vectors};

const DIM: usize = 32;
const SHARD_DOCS: usize = 6;
const COURTS: [&str; 3] = ["ca9", "ca2", "scotus"];

struct Cluster {
    coordinator: String,
    nodes: Vec<String>,
    console: SocketAddr,
    _handles: Vec<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
}

/// Two shards of six documents (`zebra` on even ids, `year = id`, a
/// court facet), a coordinator with the lexical backend, and the facade
/// bound to loopback in-process.
async fn start() -> Cluster {
    let (analysis, mock) = start_mock_analysis().await;
    let corpus = unit_vectors(2 * SHARD_DOCS, DIM, 0xC0F_FEE);
    let (shift, scale) = fit_calibration(DIM, 4, &corpus);
    let mut handles = vec![mock];
    let mut nodes = Vec::new();
    for shard in 0..2usize {
        let (addr, handle) = start_empty_node(NodeConfig {
            slot_offset: (shard * SHARD_DOCS) as u64,
            analysis_addr: Some(analysis.clone()),
            integer_fields: vec!["year".into()],
            facet_fields: vec!["court".into()],
            ..Default::default()
        })
        .await;
        let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
        client
            .set_calibration(SetCalibrationRequest {
                dim: DIM as u32,
                bit_width: 4,
                shift: shift.clone(),
                scale: scale.clone(),
            })
            .await
            .unwrap();
        let (tx, rx) = mpsc::channel(16);
        for i in 0..SHARD_DOCS {
            let id = shard * SHARD_DOCS + i;
            let text = if id.is_multiple_of(2) {
                format!("zebra document {id} about coffee")
            } else {
                format!("plain document {id} about tea")
            };
            tx.send(AddDocumentsRequest {
                text,
                integers: vec![IntegerValue {
                    field: "year".into(),
                    value: 2000 + id as i64,
                }],
                facets: vec![FacetValue {
                    field: "court".into(),
                    value: COURTS[id % 3].into(),
                }],
                ..Default::default()
            })
            .await
            .unwrap();
        }
        drop(tx);
        client.add_documents(ReceiverStream::new(rx)).await.unwrap();
        let start = shard * SHARD_DOCS;
        let (vtx, vrx) = mpsc::channel(4);
        vtx.send(AddVectorsRequest {
            vectors: corpus[start * DIM..(start + SHARD_DOCS) * DIM].to_vec(),
            dim: DIM as u32,
        })
        .await
        .unwrap();
        drop(vtx);
        client.add_vectors(ReceiverStream::new(vrx)).await.unwrap();
        nodes.push(addr);
        handles.push(handle);
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let coordinator_addr = format!("http://{}", listener.local_addr().unwrap());
    let coordinator =
        CoordinatorServiceImpl::new(nodes.clone()).with_bm25(Some(analysis), Default::default());
    // The coordinator listener serves diagnostics next to search, as the
    // product's does (src/main.rs): one unnamed member, no principals.
    let diagnostics = CoordinatorDiagnostics::new(
        vec![(String::new(), coordinator.clone())],
        None,
        Arc::new(RecentRing::default()),
    )
    .into_server(MAX_MESSAGE_BYTES);
    handles.push(tokio::spawn(
        Server::builder()
            .add_service(CoordinatorServiceImpl::into_server(
                coordinator,
                MAX_MESSAGE_BYTES,
            ))
            .add_service(diagnostics)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    ));
    let console = Console::bind(ConsoleConfig {
        listen: "127.0.0.1:0".into(),
        coordinator: coordinator_addr.clone(),
        nodes: nodes.clone(),
        analysis: None,
        security: ToolClient::from_args(Vec::<String>::new()).unwrap(),
        allow_remote: false,
    })
    .await
    .unwrap();
    let console_addr = console.local_addr();
    tokio::spawn(async move {
        let _ = console.serve().await;
    });
    Cluster {
        coordinator: coordinator_addr,
        nodes,
        console: console_addr,
        _handles: handles,
    }
}

struct Http {
    status: u16,
    content_type: String,
    body: String,
}

/// One HTTP/1.1 request over a fresh connection, read to close.
async fn http(addr: SocketAddr, method: &str, path: &str, body: Option<&str>) -> Http {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let body = body.unwrap_or("");
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: console\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.unwrap();
    let text = String::from_utf8_lossy(&raw).into_owned();
    let (head, body) = text.split_once("\r\n\r\n").expect("a header terminator");
    let status = head
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .expect("a status line");
    let content_type = head
        .lines()
        .filter_map(|l| l.split_once(':'))
        .find(|(k, _)| k.eq_ignore_ascii_case("content-type"))
        .map(|(_, v)| v.trim().to_string())
        .unwrap_or_default();
    Http {
        status,
        content_type,
        body: body.to_string(),
    }
}

async fn rpc(addr: SocketAddr, service: &str, method: &str, body: Value) -> (u16, Value) {
    let r = http(
        addr,
        "POST",
        &format!("/api/rpc/{service}/{method}"),
        Some(&body.to_string()),
    )
    .await;
    let value: Value = serde_json::from_str(&r.body).unwrap_or_else(|e| {
        panic!(
            "{service}.{method} answered non-JSON ({}): {e}: {}",
            r.status, r.body
        )
    });
    (r.status, value)
}

fn lexical(text: &str) -> Value {
    json!({ "search": { "id": "lex", "lexical": { "text": text } } })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_facade_transcodes_search_and_diagnostics_and_serves_the_ui() {
    let cluster = start().await;
    let c = cluster.console;

    // Health, shaped for the UI.
    let health = http(c, "GET", "/api/health", None).await;
    assert_eq!(health.status, 200, "{}", health.body);
    let health: Value = serde_json::from_str(&health.body).unwrap();
    assert_eq!(health["targets"].as_array().unwrap().len(), 2);
    assert!(health["targets"][0]["reachable"].as_bool().unwrap());

    // Config: the exposed method list and the credential flags.
    let config = http(c, "GET", "/api/config", None).await;
    let config: Value = serde_json::from_str(&config.body).unwrap();
    assert_eq!(config["tls"], false);
    assert_eq!(config["analysis"], false);
    let methods = config["methods"].as_array().unwrap();
    assert!(methods
        .iter()
        .any(|m| m["service"] == "SearchService" && m["method"] == "Query"));
    assert!(methods.iter().any(|m| m["service"] == "DiagnosticsService"
        && m["method"] == "StreamMetrics"
        && m["server_streaming"] == true));

    // A lexical Query through the facade equals the same message decoded
    // through the descriptor from the typed client's bytes.
    let request = json!({
        "request_id": "fixed-id",
        "k": 4,
        "selection": lexical("zebra coffee"),
    });
    let (status, body) = rpc(c, "SearchService", "Query", request.clone()).await;
    assert_eq!(status, 200, "{body}");
    let hits = body["hits"].as_array().unwrap();
    assert_eq!(hits.len(), 4, "{body}");
    assert_eq!(body["requestId"], "fixed-id");
    assert_eq!(body["executed"], "bm25_search");
    for hit in hits {
        let id: u64 = hit["docId"].as_str().unwrap().parse().unwrap();
        assert!(id.is_multiple_of(2), "zebra is on even ids: {hit}");
    }
    let mut typed = SearchServiceClient::connect(cluster.coordinator.clone())
        .await
        .unwrap();
    let typed_response = typed
        .query(QueryRequest {
            request_id: "fixed-id".into(),
            k: 4,
            selection: Some(SelectionQuery {
                node: Some(selection_query::Node::Search(SearchQuery {
                    id: "lex".into(),
                    query: Some(search_query::Query::Lexical(LexicalQuery {
                        text: "zebra coffee".into(),
                        ..Default::default()
                    })),
                })),
            }),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let desc = pipestream_search::console::descriptor_pool()
        .get_message_by_name("ai.pipestream.search.v1.QueryResponse")
        .unwrap();
    let dynamic =
        prost_reflect::DynamicMessage::decode(desc, typed_response.encode_to_vec().as_slice())
            .unwrap();
    let mut buf = Vec::new();
    dynamic
        .serialize_with_options(
            &mut serde_json::Serializer::new(&mut buf),
            &prost_reflect::SerializeOptions::new().skip_default_fields(false),
        )
        .unwrap();
    let expected: Value = serde_json::from_slice(&buf).unwrap();
    assert_eq!(
        body, expected,
        "facade JSON equals the descriptor's rendering"
    );

    // A boolean root with a filter, explain, and an aggregation.
    let request = json!({
        "k": 10,
        "explain": true,
        "profile": true,
        "selection": { "boolean": {
            "must": [ { "filter": { "id": "recent", "cel": "year >= 2004" } } ],
            "should": [ lexical("document") ],
            "aggregate": {
                "group_by": "court",
                "aggregations": [ { "name": "n", "expression": "1", "op": "AGGREGATE_OP_COUNT" } ]
            }
        } },
    });
    let (status, body) = rpc(c, "SearchService", "Query", request).await;
    assert_eq!(status, 200, "{body}");
    let hits = body["hits"]
        .as_array()
        .unwrap_or_else(|| panic!("no hits: {body}"));
    assert_eq!(hits.len(), 8, "years 2004..=2011: {body}");
    assert!(hits.iter().all(|h| h["explain"]["description"].is_string()));
    assert_eq!(body["aggregate"]["matched"], "8");
    assert_eq!(body["aggregate"]["groups"].as_array().unwrap().len(), 3);
    assert!(body["profile"]["totalMs"].is_number());

    // A bad request keeps the gRPC message and maps to 400.
    let (status, body) = rpc(c, "SearchService", "Query", json!({ "k": 3 })).await;
    assert_eq!(status, 400, "{body}");
    assert_eq!(body["code"], "INVALID_ARGUMENT");
    assert!(
        body["error"].as_str().unwrap().contains("selection"),
        "{body}"
    );
    // Malformed JSON is the facade's own 400, naming the message type.
    let r = http(
        c,
        "POST",
        "/api/rpc/SearchService/Query",
        Some("{\"k\": \"three\"}"),
    )
    .await;
    assert_eq!(r.status, 400, "{}", r.body);
    assert!(r.body.contains("QueryRequest"), "{}", r.body);

    // Diagnostics are served on the coordinator and on a node, and the
    // knobs come back with their scope: coordinator knobs from the
    // coordinator, node knobs from a node.
    let (status, body) = rpc(c, "DiagnosticsService", "GetRuntimeKnobs", json!({})).await;
    assert_eq!(status, 200, "{body}");
    let knobs = body["knobs"].as_array().expect("knobs array");
    assert!(
        knobs
            .iter()
            .any(|k| k["name"] == "max_k" && k["scope"] == "KNOB_SCOPE_COORDINATOR"),
        "{body}"
    );
    let r = http(
        c,
        "POST",
        "/api/rpc/DiagnosticsService/GetRuntimeKnobs?target=node0",
        Some("{}"),
    )
    .await;
    assert_eq!(r.status, 200, "{}", r.body);
    assert!(
        r.body.contains("\"segment_pruning\"") && r.body.contains("KNOB_SCOPE_NODE"),
        "{}",
        r.body
    );
    let r = http(
        c,
        "POST",
        "/api/rpc/DiagnosticsService/GetRuntimeKnobs?target=node7",
        Some("{}"),
    )
    .await;
    assert_eq!(r.status, 400, "{}", r.body);
    assert!(r.body.contains("out of range"), "{}", r.body);

    // Internal services and unknown methods are not exposed.
    let (status, _) = rpc(c, "NodeService", "Health", json!({})).await;
    assert_eq!(status, 404);
    let (status, _) = rpc(c, "SearchService", "Nope", json!({})).await;
    assert_eq!(status, 404);
    let (status, body) = rpc(c, "SearchService", "QueryStream", json!({})).await;
    assert_eq!(status, 400, "a streaming method on the unary route: {body}");

    // Document text from the owning nodes, routed by slot range.
    let r = http(
        c,
        "POST",
        "/api/documents",
        Some("{\"doc_ids\": [\"0\", 7, \"11\", 500]}"),
    )
    .await;
    assert_eq!(r.status, 200, "{}", r.body);
    let docs: Value = serde_json::from_str(&r.body).unwrap();
    let documents = docs["documents"].as_array().unwrap();
    assert_eq!(documents.len(), 3, "{docs}");
    let seven = documents
        .iter()
        .find(|d| d["doc_id"] == "7")
        .expect("doc 7 from shard 1");
    assert_eq!(seven["shard"], 1);
    assert_eq!(seven["text"], "plain document 7 about tea");
    assert_eq!(docs["unrouted"], json!(["500"]));

    // The embedding route is a plain 501 without a sidecar.
    let r = http(c, "POST", "/api/embed", Some("{\"text\": \"x\"}")).await;
    assert_eq!(r.status, 501, "{}", r.body);

    // The UI serves, with the right content types.
    let r = http(c, "GET", "/", None).await;
    assert_eq!(r.status, 200);
    assert!(r.content_type.starts_with("text/html"));
    assert!(r.body.contains("Pipestream Search console"));
    let r = http(c, "GET", "/dashboard", None).await;
    assert_eq!(r.status, 200);
    assert!(r.body.contains("dashboard.js"));
    for asset in [
        "/app.css",
        "/common.js",
        "/search.js",
        "/dashboard.js",
        "/sparkline.js",
    ] {
        let r = http(c, "GET", asset, None).await;
        assert_eq!(r.status, 200, "{asset}");
        assert!(
            r.content_type.starts_with("text/css") || r.content_type.starts_with("text/javascript"),
            "{asset}: {}",
            r.content_type
        );
    }
    let r = http(c, "GET", "/nope", None).await;
    assert_eq!(r.status, 404);
    let r = http(c, "GET", "/api/rpc/SearchService/Query", None).await;
    assert_eq!(r.status, 405);

    // Suggest and TermSuggest ride the same route; both need the corpus
    // analysis spec, which /api/config hands the UI.
    let spec = pipestream_search::analyzer::body_spec();
    let analysis = json!({
        "tokenizer": spec.tokenizer,
        "stemmer": spec.stemmer,
        "term_vector_mode": spec.term_vector_mode,
        "term_vector_source": spec.term_vector_source,
        "char_filters": spec.char_filters,
    });
    assert_eq!(
        config["body_spec"], analysis,
        "the config route hands out the corpus spec"
    );
    let (status, body) = rpc(
        c,
        "SearchService",
        "Suggest",
        json!({ "field": "body", "prefix": "zeb", "limit": 3, "analysis": analysis }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["suggestions"][0]["term"], "zebra", "{body}");
    let (status, body) = rpc(
        c,
        "SearchService",
        "TermSuggest",
        json!({ "field": "body", "text": "zebro", "max_edits": 1, "limit": 2, "analysis": analysis }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["terms"][0]["candidates"][0]["term"], "zebra", "{body}");
    drop(cluster.nodes);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_stream_route_delivers_server_sent_events_and_ends() {
    let cluster = start().await;
    let c = cluster.console;
    let body = json!({ "query": { "k": 3, "selection": lexical("document") } }).to_string();
    let mut stream = TcpStream::connect(c).await.unwrap();
    let request = format!(
        "POST /api/stream/SearchService/QueryStream HTTP/1.1\r\nHost: console\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.unwrap();
    let text = String::from_utf8_lossy(&raw).into_owned();
    assert!(text.starts_with("HTTP/1.1 200 OK"), "{text}");
    assert!(text.contains("Content-Type: text/event-stream"), "{text}");
    let (_, events) = text.split_once("\r\n\r\n").unwrap();
    let frames: Vec<&str> = events
        .split("\n\n")
        .filter(|f| !f.trim().is_empty())
        .collect();
    assert!(
        frames.len() >= 2,
        "a revision or completion plus the end frame: {events}"
    );
    let data_frames: Vec<Value> = frames
        .iter()
        .filter(|f| !f.starts_with("event:"))
        .map(|f| serde_json::from_str(f.trim_start_matches("data:").trim()).unwrap())
        .collect();
    let completion = data_frames
        .iter()
        .find(|v| v.get("completion").is_some())
        .expect("a completion frame");
    assert_eq!(completion["completion"]["completed"], true, "{completion}");
    assert_eq!(
        completion["completion"]["response"]["hits"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    assert!(frames.last().unwrap().starts_with("event: end"), "{events}");

    // The unary route rejects a streaming call, and scalar query
    // parameters become top-level fields on a GET.
    let r = http(c, "GET", "/api/stream/SearchService/Query?k=1", None).await;
    assert_eq!(r.status, 400, "{}", r.body);
    // A GET with the request as a query parameter.
    let encoded = "%7B%22query%22%3A%7B%22k%22%3A1%2C%22selection%22%3A%7B%22search%22%3A%7B%22id%22%3A%22l%22%2C%22lexical%22%3A%7B%22text%22%3A%22zebra%22%7D%7D%7D%7D%7D";
    let r = http(
        c,
        "GET",
        &format!("/api/stream/SearchService/QueryStream?request={encoded}"),
        None,
    )
    .await;
    assert_eq!(r.status, 200, "{}", r.body);
    assert!(r.body.contains("\"completed\":true"), "{}", r.body);
    // The metrics stream is served and never ends on its own: read the
    // status line and the first event, then drop the connection.
    let first = sse_first_event(
        c,
        "/api/stream/DiagnosticsService/StreamMetrics?interval_ms=200",
    )
    .await;
    assert!(first.starts_with("HTTP/1.1 200"), "{first}");
    assert!(first.contains("text/event-stream"), "{first}");
    assert!(first.contains("\"samples\""), "{first}");
    // An interval under the floor is rejected before any event.
    let r = http(
        c,
        "GET",
        "/api/stream/DiagnosticsService/StreamMetrics?interval_ms=50",
        None,
    )
    .await;
    assert_eq!(r.status, 400, "{}", r.body);
}

/// GET `path` and return the response head plus the body through the end
/// of the first SSE event (the first blank line after a `data:` line).
async fn sse_first_event(addr: SocketAddr, path: &str) -> String {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let request = format!("GET {path} HTTP/1.1\r\nHost: console\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut raw = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let n = stream.read(&mut chunk).await.unwrap();
        assert!(
            n > 0,
            "stream ended before the first event: {}",
            String::from_utf8_lossy(&raw)
        );
        raw.extend_from_slice(&chunk[..n]);
        let text = String::from_utf8_lossy(&raw);
        if let Some(head_end) = text.find("\r\n\r\n") {
            let body = &text[head_end + 4..];
            if let Some(data) = body.find("data:") {
                if body[data..].contains("\n\n") {
                    return text.into_owned();
                }
            }
        }
    }
}

#[tokio::test]
async fn a_remote_bind_needs_the_flag() {
    let security = || ToolClient::from_args(Vec::<String>::new()).unwrap();
    let refused = Console::bind(ConsoleConfig {
        listen: "0.0.0.0:0".into(),
        coordinator: "http://127.0.0.1:1".into(),
        nodes: Vec::new(),
        analysis: None,
        security: security(),
        allow_remote: false,
    })
    .await;
    let message = refused.err().expect("a non-loopback bind is refused");
    assert!(message.contains("--allow-remote"), "{message}");
    let allowed = Console::bind(ConsoleConfig {
        listen: "0.0.0.0:0".into(),
        coordinator: "http://127.0.0.1:1".into(),
        nodes: Vec::new(),
        analysis: None,
        security: security(),
        allow_remote: true,
    })
    .await;
    assert!(allowed.is_ok());
    let bad = Console::bind(ConsoleConfig {
        listen: "not an address".into(),
        coordinator: "http://127.0.0.1:1".into(),
        nodes: Vec::new(),
        analysis: None,
        security: security(),
        allow_remote: false,
    })
    .await;
    assert!(bad.is_err());
}

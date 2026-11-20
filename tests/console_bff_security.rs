//! The console's BFF boundary over real loopback TCP: the raw services
//! stay unreachable by default, the raw explorer refuses a remote bind,
//! and the BFF answers Connect unary, Connect streaming and server-sent
//! events against a live two-shard cluster.

mod common;

use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use pipestream_search::console::{Console, ConsoleConfig};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::node::NodeConfig;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::{AddDocumentsRequest, AddVectorsRequest};
use pipestream_search::security::ToolClient;
use pipestream_search::MAX_MESSAGE_BYTES;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::transport::Server;

use common::{fit_calibration, mock::start_mock_analysis, start_empty_node, unit_vectors};

const DIM: usize = 32;
const SHARD_DOCS: usize = 4;
const BFF: &str = "/bff/v1/ai.protomolt.console.bff.v1.ConsoleBffService";

type Handle = tokio::task::JoinHandle<Result<(), tonic::transport::Error>>;

/// One shard of `SHARD_DOCS` documents, `zebra` on even ids.
async fn seed_shard(analysis: &str, corpus: &[f32], shard: usize) -> (String, Handle) {
    let (shift, scale) = fit_calibration(DIM, 4, corpus);
    let (addr, handle) = start_empty_node(NodeConfig {
        slot_offset: (shard * SHARD_DOCS) as u64,
        analysis_addr: Some(analysis.to_string()),
        ..Default::default()
    })
    .await;
    let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
    let backend = pipestream_search::harness::embedded_backend_request(DIM, 4, &shift, &scale);
    client.configure_vector_backend(backend).await.unwrap();
    let (tx, rx) = mpsc::channel(16);
    for id in shard * SHARD_DOCS..(shard + 1) * SHARD_DOCS {
        let word = if id.is_multiple_of(2) {
            "zebra"
        } else {
            "plain"
        };
        let text = format!("{word} document {id} about coffee");
        tx.send(AddDocumentsRequest {
            text,
            ..Default::default()
        })
        .await
        .unwrap();
    }
    drop(tx);
    client.add_documents(ReceiverStream::new(rx)).await.unwrap();
    let start = shard * SHARD_DOCS * DIM;
    let (vtx, vrx) = mpsc::channel(1);
    let vectors = corpus[start..start + SHARD_DOCS * DIM].to_vec();
    let rows = AddVectorsRequest {
        materialized_rows: Vec::new(),
        vectors,
        dim: DIM as u32,
    };
    vtx.send(rows).await.unwrap();
    drop(vtx);
    client.add_vectors(ReceiverStream::new(vrx)).await.unwrap();
    (addr, handle)
}

/// Two shards behind a coordinator; returns its address and the nodes.
async fn cluster() -> (String, Vec<String>, Vec<Handle>) {
    let (analysis, mock) = start_mock_analysis().await;
    let corpus = unit_vectors(2 * SHARD_DOCS, DIM, 0xB0FF);
    let mut handles = vec![mock];
    let mut nodes = Vec::new();
    for shard in 0..2 {
        let (addr, handle) = seed_shard(&analysis, &corpus, shard).await;
        nodes.push(addr);
        handles.push(handle);
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let coordinator = format!("http://{}", listener.local_addr().unwrap());
    let half = u64::MAX / 2;
    let service = CoordinatorServiceImpl::new(nodes.clone())
        .with_bm25(Some(analysis), Default::default())
        .with_topology_generation(1)
        .with_hot_topology(vec![Some((0, half)), Some((half + 1, u64::MAX))])
        .unwrap();
    handles.push(tokio::spawn(
        Server::builder()
            .add_service(CoordinatorServiceImpl::into_server(
                service,
                MAX_MESSAGE_BYTES,
            ))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    ));
    (coordinator, nodes, handles)
}

fn config(coordinator: &str, nodes: &[String], allow_remote: bool) -> ConsoleConfig {
    ConsoleConfig {
        listen: "127.0.0.1:0".into(),
        coordinator: coordinator.to_string(),
        nodes: nodes.to_vec(),
        analysis: None,
        security: ToolClient::from_args(Vec::<String>::new()).unwrap(),
        allow_remote,
    }
}

async fn serve(console: Console) -> SocketAddr {
    let addr = console.local_addr();
    tokio::spawn(async move {
        let _ = console.serve().await;
    });
    addr
}

/// One request over a fresh connection, read to close: status and body.
async fn send(addr: SocketAddr, head: &str, body: &[u8]) -> (u16, Vec<u8>) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(head.as_bytes()).await.unwrap();
    stream.write_all(body).await.unwrap();
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.unwrap();
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("a header terminator");
    let status = String::from_utf8_lossy(&raw[..split])
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .expect("a status line");
    (status, raw[split + 4..].to_vec())
}

fn head(addr: SocketAddr, path: &str, kind: &str, len: usize, extra: &str) -> String {
    format!(
        "POST {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Type: {kind}\r\nContent-Length: {len}\r\n{extra}Connection: close\r\n\r\n"
    )
}

async fn post(addr: SocketAddr, path: &str, body: &Value) -> (u16, Value) {
    let body = body.to_string();
    let (status, raw) = send(
        addr,
        &head(addr, path, "application/json", body.len(), ""),
        body.as_bytes(),
    )
    .await;
    let text = String::from_utf8_lossy(&raw).into_owned();
    (
        status,
        serde_json::from_str(&text).unwrap_or(Value::String(text)),
    )
}

fn query() -> Value {
    json!({ "k": 3, "selection": { "search": { "id": "lex", "lexical": { "text": "zebra" } } } })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raw_services_and_mutations_are_refused_by_default() {
    let (coordinator, nodes, _handles) = cluster().await;
    let c = serve(
        Console::bind(config(&coordinator, &nodes, false))
            .await
            .unwrap(),
    )
    .await;
    for path in [
        "/api/rpc/SearchService/Query",
        "/api/rpc/SearchService/RoutedIngestMapped",
        "/api/rpc/NodeService/AddDocuments",
        "/api/rpc/ClusterControl/PlanBalance",
        "/api/rpc/DiagnosticsService/GetRuntimeKnobs?target=node0",
        "/api/stream/SearchService/QueryStream",
        "/api/documents",
        "/bff/v1/ai.protomolt.search.v1.SearchService/Query",
        "/bff/v1/ai.protomolt.search.v1.SearchService/RoutedIngestMapped",
        "/bff/v1/ai.protomolt.search.v1.NodeService/AddDocuments",
        "/bff/v1/ai.protomolt.search.v1.ClusterControl/PlanBalance",
        &format!("{BFF}/IngestDocuments"),
        &format!("{BFF}/RoutedIngestMapped"),
    ] {
        let (status, body) = post(c, path, &json!({})).await;
        assert_eq!(status, 404, "{path}: {body}");
    }
    for page in ["/api/health", "/api/config", "/explorer", "/dashboard"] {
        let get = format!("GET {page} HTTP/1.1\r\nHost: {c}\r\nConnection: close\r\n\r\n");
        assert_eq!(send(c, &get, b"").await.0, 404, "{page}");
    }
    let (status, body) = post(c, &format!("{BFF}/Query?target=node0"), &query()).await;
    assert_eq!(
        (status, body["code"].as_str()),
        (403, Some("permission_denied")),
        "{body}"
    );
    let body = query().to_string();
    for extra in [
        "Origin: http://evil.example\r\n",
        "X-Forwarded-For: 1.2.3.4\r\nOrigin: null\r\n",
    ] {
        let h = head(
            c,
            &format!("{BFF}/Query"),
            "application/json",
            body.len(),
            extra,
        );
        assert_eq!(send(c, &h, body.as_bytes()).await.0, 403, "{extra}");
    }
    let rebound = format!(
        "POST {BFF}/Query HTTP/1.1\r\nHost: rebound.example\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    assert_eq!(send(c, &rebound, body.as_bytes()).await.0, 403);
    // The flag, not the route table, is what gates them on loopback.
    let raw = Console::bind(config(&coordinator, &nodes, false))
        .await
        .unwrap();
    let raw = serve(raw.with_raw_explorer().unwrap()).await;
    let (status, body) = post(raw, "/api/rpc/SearchService/Query", &query()).await;
    assert_eq!(status, 200, "{body}");
}

#[tokio::test]
async fn the_raw_explorer_refuses_a_remote_bind() {
    let remote = Console::bind(config(
        "http://127.0.0.1:1",
        &["http://127.0.0.1:2".into()],
        true,
    ));
    let refused = remote
        .await
        .unwrap()
        .with_raw_explorer()
        .err()
        .expect("a refusal");
    assert!(refused.contains("--allow-remote"), "{refused}");
    let run = std::process::Command::new(env!("CARGO_BIN_EXE_console"))
        .args([
            "--listen=127.0.0.1:0",
            "--enable-raw-explorer",
            "--allow-remote",
        ])
        .args([
            "--coordinator=http://127.0.0.1:1",
            "--nodes=http://127.0.0.1:2",
        ])
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&run.stderr);
    assert!(!run.status.success(), "the binary started: {stderr}");
    assert!(stderr.contains("--enable-raw-explorer"), "{stderr}");
}

/// Connect envelopes: (flags, JSON) in order.
fn envelopes(mut raw: &[u8]) -> Vec<(u8, Value)> {
    let mut out = Vec::new();
    while raw.len() >= 5 {
        let len = u32::from_be_bytes(raw[1..5].try_into().unwrap()) as usize;
        out.push((raw[0], serde_json::from_slice(&raw[5..5 + len]).unwrap()));
        raw = &raw[5 + len..];
    }
    assert!(raw.is_empty(), "a trailing partial envelope");
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_bff_answers_connect_unary_streaming_and_events() {
    let (coordinator, nodes, _handles) = cluster().await;
    let console = Console::bind(config(&coordinator, &nodes, false))
        .await
        .unwrap();
    let c = serve(console.with_sse_heartbeat(Duration::from_millis(20))).await;
    unary(c).await;
    own_ops(c).await;
    streams(c).await;
}

/// Connect unary: a query, and a malformed request as `invalid_argument`.
async fn unary(c: SocketAddr) {
    let (status, body) = post(c, &format!("{BFF}/Query"), &query()).await;
    assert_eq!(status, 200, "{body}");
    let hits = body["hits"].as_array().unwrap();
    assert_eq!(hits.len(), 3, "{body}");
    assert!(hits
        .iter()
        .all(|h| h["docId"].as_str().unwrap().parse::<u64>().unwrap() % 2 == 0));
    let (status, body) = post(c, &format!("{BFF}/Query"), &json!({ "k": "many" })).await;
    assert_eq!(
        (status, body["code"].as_str()),
        (400, Some("invalid_argument")),
        "{body}"
    );
}

/// The console's own reads: config, sanitized health, snippets, embeddings.
async fn own_ops(c: SocketAddr) {
    let (status, body) = post(c, &format!("{BFF}/GetConsoleConfig"), &json!({})).await;
    assert_eq!(
        (status, &body["rawExplorer"]),
        (200, &json!(false)),
        "{body}"
    );
    let (status, body) = post(c, &format!("{BFF}/HealthSummary"), &json!({})).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["shards"].as_array().unwrap().len(), 2, "{body}");
    assert!(
        !body.to_string().contains("127.0.0.1"),
        "addresses leak: {body}"
    );
    let (status, body) = post(
        c,
        &format!("{BFF}/PreviewDocuments"),
        &json!({ "docIds": ["0", "5"], "maxChars": 5 }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let docs = body["documents"].as_array().unwrap();
    assert_eq!(docs.len(), 2, "{body}");
    assert!(
        docs.iter()
            .all(|d| d["snippet"].as_str().unwrap().len() == 5 && d["truncated"] == true),
        "{body}"
    );
    let (status, body) = post(
        c,
        &format!("{BFF}/PreviewEmbedding"),
        &json!({ "text": "x" }),
    )
    .await;
    assert_eq!(
        (status, body["code"].as_str()),
        (501, Some("unimplemented")),
        "{body}"
    );
}

/// Connect streaming envelopes and the server-sent fallback.
async fn streams(c: SocketAddr) {
    let request = json!({ "query": query() }).to_string();
    let mut framed = vec![0u8];
    framed.extend_from_slice(&(request.len() as u32).to_be_bytes());
    framed.extend_from_slice(request.as_bytes());
    let h = head(
        c,
        &format!("{BFF}/QueryStream"),
        "application/connect+json",
        framed.len(),
        "",
    );
    let (status, raw) = send(c, &h, &framed).await;
    assert_eq!(status, 200);
    let frames = envelopes(&raw);
    let (last, rest) = frames.split_last().expect("an end-stream envelope");
    assert_eq!(last, &(2, json!({})), "{frames:?}");
    assert!(
        !rest.is_empty() && rest.iter().all(|(f, _)| *f == 0),
        "{frames:?}"
    );

    let h = head(
        c,
        "/bff/v1/sse/QueryStream",
        "application/json",
        request.len(),
        "",
    );
    let (status, raw) = send(c, &h, request.as_bytes()).await;
    let text = String::from_utf8(raw).unwrap();
    assert_eq!(status, 200, "{text}");
    assert!(text.starts_with(": keepalive\n\n"), "{text}");
    assert!(text.contains("event: message\ndata: {"), "{text}");
    assert!(text.ends_with("event: end\ndata: {}\n\n"), "{text}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connections_pipeline_bodies_are_capped_and_the_ui_dir_is_fenced() {
    let (coordinator, nodes, _handles) = cluster().await;
    let dir = std::env::temp_dir().join(format!("console-bff-ui-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("ui")).unwrap();
    std::fs::write(dir.join("secret.js"), "secret").unwrap();
    std::os::unix::fs::symlink(dir.join("secret.js"), dir.join("ui/leak.js")).unwrap();
    let console = Console::bind(config(&coordinator, &nodes, false))
        .await
        .unwrap();
    let c = serve(console.with_ui_dir(Path::new(&dir.join("ui"))).unwrap()).await;

    let body = query().to_string();
    let one = format!(
        "POST {BFF}/Query HTTP/1.1\r\nHost: {c}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    let last = head(
        c,
        &format!("{BFF}/GetConsoleConfig"),
        "application/json",
        2,
        "",
    );
    let (status, raw) = send(c, &format!("{one}{one}{last}"), b"{}").await;
    let text = String::from_utf8_lossy(&raw);
    assert_eq!(status, 200);
    assert_eq!(
        text.matches("HTTP/1.1 200").count(),
        2,
        "three answers on one connection: {text}"
    );
    assert_eq!(text.matches("\"hits\"").count(), 2, "{text}");
    assert!(text.contains("\"rawExplorer\""), "{text}");

    let over = 8 * 1024 * 1024 + 1;
    let (status, _) = send(
        c,
        &head(c, &format!("{BFF}/Query"), "application/json", over, ""),
        b"",
    )
    .await;
    assert_eq!(status, 413);
    let get = format!("GET /leak.js HTTP/1.1\r\nHost: {c}\r\nConnection: close\r\n\r\n");
    let (status, raw) = send(c, &get, b"").await;
    assert_eq!(status, 403, "{}", String::from_utf8_lossy(&raw));
    let _ = std::fs::remove_dir_all(&dir);
}

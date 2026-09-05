//! The console: a JSON facade over the cluster's gRPC plus the web UI it
//! serves (`docs/console-facade.md`).
//!
//! The facade is a client of a running cluster and never part of it. It
//! holds the TLS material and the bearer token, so a browser carries
//! neither. Its one real job is transcoding: `POST /api/rpc/<Service>/
//! <Method>` takes the request message as proto3 JSON, sends it over
//! gRPC, and answers with the response as proto3 JSON, for every unary
//! method of `SearchService` and `DiagnosticsService`; the
//! server-streaming methods are exposed as server-sent events under
//! `/api/stream/...`. The mapping is built from the compiled descriptor
//! set at run time, so an RPC added to the proto needs no change here.
//!
//! A few convenience routes exist for the UI: `/api/health`,
//! `/api/config`, `/api/embed` (the analysis sidecar's embedding, when
//! one is configured), and `/api/documents` (stored text from the owning
//! nodes, routed by the cluster's slot ranges).
//!
//! The HTTP side is hand-rolled HTTP/1.1 with `Connection: close`: this
//! is an operator's tool bound to loopback by default, not a product
//! server. Binding elsewhere needs `--allow-remote`, because whoever
//! reaches the facade acts as its principal.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use bytes::{Buf, BufMut, Bytes};
use prost::Message as _;
use prost_reflect::{DescriptorPool, DynamicMessage, MethodDescriptor, SerializeOptions};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::codegen::http::uri::PathAndQuery;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use tonic::{Code, Request, Status};

use crate::pb::node_service_client::NodeServiceClient;
use crate::pb::search_service_client::SearchServiceClient;
use crate::pb::{ClusterHealthRequest, GetDocumentsRequest};
use crate::security::{Bearer, PublicChannel, ToolClient};

/// The compiled descriptor set (`build.rs`): every service and message
/// the facade may transcode.
const DESCRIPTOR_SET: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/search_descriptor.bin"));
/// The proto package the exposed services live in.
const PACKAGE: &str = "ai.protomolt.search.v1";
/// The services a browser may reach through the facade. `NodeService`
/// and `ClusterControl` are the cluster's internal surfaces and are not
/// among them.
const EXPOSED_SERVICES: &[&str] = &["SearchService", "DiagnosticsService"];
/// Request bodies are JSON; anything past this is a mistake.
const MAX_BODY_BYTES: usize = 8 << 20;
/// `ClusterHealth` is re-read at most this often for document routing.
const HEALTH_TTL: Duration = Duration::from_secs(5);

const INDEX_HTML: &str = include_str!("bin/console-ui/index.html");
const DASHBOARD_HTML: &str = include_str!("bin/console-ui/dashboard.html");
const APP_CSS: &str = include_str!("bin/console-ui/app.css");
const COMMON_JS: &str = include_str!("bin/console-ui/common.js");
const SEARCH_JS: &str = include_str!("bin/console-ui/search.js");
const DASHBOARD_JS: &str = include_str!("bin/console-ui/dashboard.js");
const SPARKLINE_JS: &str = include_str!("bin/console-ui/sparkline.js");

/// What the facade needs to run, parsed from the tool flags.
pub struct ConsoleConfig {
    pub listen: String,
    pub coordinator: String,
    pub nodes: Vec<String>,
    pub analysis: Option<String>,
    pub security: ToolClient,
    pub allow_remote: bool,
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

impl ConsoleConfig {
    /// Flags, all `--key=value`: `--listen` (default `127.0.0.1:8600`),
    /// `--coordinator` (default `http://127.0.0.1:50050`), `--nodes`
    /// (comma-separated, in shard order), `--analysis` (the sidecar,
    /// for embeddings), `--allow-remote`, and the tool security flags
    /// `ToolClient` reads (`docs/security.md`).
    pub fn from_args(args: &[String]) -> Result<Self, String> {
        let security = ToolClient::from_args(args.iter().cloned())?;
        let coordinator = security.url(
            &flag(args, "coordinator").unwrap_or_else(|| "http://127.0.0.1:50050".to_string()),
        );
        let nodes = flag(args, "nodes")
            .map(|list| {
                list.split(',')
                    .filter(|s| !s.is_empty())
                    .map(|s| security.url(s))
                    .collect()
            })
            .unwrap_or_default();
        Ok(Self {
            listen: flag(args, "listen").unwrap_or_else(|| "127.0.0.1:8600".to_string()),
            coordinator,
            nodes,
            // The sidecar has no TLS (docs/security.md); its address
            // keeps its own scheme.
            analysis: flag(args, "analysis").map(|a| grpc_addr(&a)),
            security,
            allow_remote: args.iter().any(|a| a == "--allow-remote"),
        })
    }
}

struct Ctx {
    coordinator: String,
    nodes: Vec<String>,
    analysis: Option<String>,
    security: ToolClient,
    /// One channel per address, connected lazily and reused across
    /// requests (a handshake per request would dominate a TLS fleet).
    channels: Mutex<HashMap<String, Channel>>,
    /// The shard map for document routing, refreshed on a short TTL.
    health: Mutex<Option<(Instant, Vec<ShardRange>)>>,
}

#[derive(Clone)]
struct ShardRange {
    shard: u32,
    addr: String,
    slot_offset: u64,
    rows: u64,
}

impl Ctx {
    fn channel(&self, addr: &str) -> Result<Channel, String> {
        let mut channels = self.channels.lock().expect("channel map poisoned");
        if let Some(channel) = channels.get(addr) {
            return Ok(channel.clone());
        }
        let channel = self.security.channel(addr)?;
        channels.insert(addr.to_string(), channel.clone());
        Ok(channel)
    }

    fn search_client(&self, addr: &str) -> Result<SearchServiceClient<PublicChannel>, String> {
        Ok(
            SearchServiceClient::with_interceptor(self.channel(addr)?, self.security.bearer())
                .max_decoding_message_size(crate::MAX_MESSAGE_BYTES)
                .max_encoding_message_size(crate::MAX_MESSAGE_BYTES),
        )
    }

    fn node_client(&self, addr: &str) -> Result<NodeServiceClient<Channel>, String> {
        Ok(NodeServiceClient::new(self.channel(addr)?)
            .max_decoding_message_size(crate::MAX_MESSAGE_BYTES)
            .max_encoding_message_size(crate::MAX_MESSAGE_BYTES))
    }

    /// The address a `target` query parameter names: the coordinator
    /// by default, or `node<i>` from the configured node list. Never an
    /// arbitrary address from the browser.
    fn target(&self, target: Option<&str>) -> Result<String, Reply> {
        match target {
            None | Some("") | Some("coordinator") => Ok(self.coordinator.clone()),
            Some(name) => {
                let index = name
                    .strip_prefix("node")
                    .and_then(|i| i.parse::<usize>().ok())
                    .ok_or_else(|| {
                        Reply::error(
                            400,
                            "INVALID_ARGUMENT",
                            format!("target {name:?} is not `coordinator` or `node<i>`"),
                        )
                    })?;
                self.nodes.get(index).cloned().ok_or_else(|| {
                    Reply::error(
                        400,
                        "INVALID_ARGUMENT",
                        format!(
                            "target node{index} is out of range: {} node(s) configured",
                            self.nodes.len()
                        ),
                    )
                })
            }
        }
    }
}

/// A bound facade, ready to serve.
pub struct Console {
    listener: TcpListener,
    ctx: Arc<Ctx>,
}

impl Console {
    /// Binds the listener. A non-loopback address is rejected unless
    /// `allow_remote` is set.
    pub async fn bind(config: ConsoleConfig) -> Result<Self, String> {
        let addr: SocketAddr = config
            .listen
            .parse()
            .map_err(|e| format!("--listen {:?}: {e}", config.listen))?;
        if !config.allow_remote && !crate::security::is_loopback(&addr) {
            return Err(format!(
                "--listen {addr} is not a loopback address; whoever reaches the console acts \
                 as its principal, so a remote bind needs --allow-remote"
            ));
        }
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| format!("bind {addr}: {e}"))?;
        Ok(Self {
            listener,
            ctx: Arc::new(Ctx {
                coordinator: config.coordinator,
                nodes: config.nodes,
                analysis: config.analysis,
                security: config.security,
                channels: Mutex::new(HashMap::new()),
                health: Mutex::new(None),
            }),
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.listener
            .local_addr()
            .expect("bound listener has an address")
    }

    /// A one-line description for the startup log.
    pub fn describe(&self) -> String {
        format!(
            "console on http://{} -> coordinator {} ({} node(s), analysis {}, tls {}, bearer {})",
            self.local_addr(),
            self.ctx.coordinator,
            self.ctx.nodes.len(),
            self.ctx
                .analysis
                .as_deref()
                .unwrap_or("none: embedding disabled"),
            self.ctx.security.is_tls(),
            self.ctx.security.has_bearer(),
        )
    }

    /// Accepts connections until the listener fails.
    pub async fn serve(self) -> std::io::Result<()> {
        loop {
            let (stream, _) = self.listener.accept().await?;
            let ctx = Arc::clone(&self.ctx);
            tokio::spawn(async move {
                let _ = handle_conn(stream, ctx).await;
            });
        }
    }
}

// ---------------------------------------------------------------------------
// The descriptor pool and the raw-bytes codec
// ---------------------------------------------------------------------------

/// The descriptor pool decoded once from the compiled set.
pub fn descriptor_pool() -> &'static DescriptorPool {
    static POOL: OnceLock<DescriptorPool> = OnceLock::new();
    POOL.get_or_init(|| {
        DescriptorPool::decode(DESCRIPTOR_SET).expect("the compiled descriptor set decodes")
    })
}

/// The exposed services and their methods, for `/api/config` and for
/// tests: `(service, method, server_streaming)`.
pub fn exposed_methods() -> Vec<(String, String, bool)> {
    let pool = descriptor_pool();
    let mut out = Vec::new();
    for service in EXPOSED_SERVICES {
        let Some(desc) = pool.get_service_by_name(&format!("{PACKAGE}.{service}")) else {
            continue;
        };
        for method in desc.methods() {
            if method.is_client_streaming() {
                continue;
            }
            out.push((
                service.to_string(),
                method.name().to_string(),
                method.is_server_streaming(),
            ));
        }
    }
    out
}

fn resolve_method(service: &str, method: &str) -> Result<MethodDescriptor, Reply> {
    if !EXPOSED_SERVICES.contains(&service) {
        return Err(Reply::error(
            404,
            "NOT_FOUND",
            format!(
                "service {service:?} is not exposed by the console; the services are {}",
                EXPOSED_SERVICES.join(", ")
            ),
        ));
    }
    let desc = descriptor_pool()
        .get_service_by_name(&format!("{PACKAGE}.{service}"))
        .ok_or_else(|| {
            Reply::error(
                404,
                "NOT_FOUND",
                format!("service {service:?} is not in the descriptor set"),
            )
        })?;
    let found = desc.methods().find(|m| m.name() == method).ok_or_else(|| {
        Reply::error(
            404,
            "NOT_FOUND",
            format!("{service} has no method {method:?}"),
        )
    })?;
    if found.is_client_streaming() {
        return Err(Reply::error(
            501,
            "UNIMPLEMENTED",
            format!("{service}.{method} is client-streaming; the console does not expose it"),
        ));
    }
    Ok(found)
}

/// A codec that moves message bytes untouched: the facade encodes and
/// decodes through the descriptor, so the transport only frames.
#[derive(Default)]
struct RawCodec;

impl Codec for RawCodec {
    type Encode = Bytes;
    type Decode = Bytes;
    type Encoder = RawEncoder;
    type Decoder = RawDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        RawEncoder
    }

    fn decoder(&mut self) -> Self::Decoder {
        RawDecoder
    }
}

struct RawEncoder;

impl Encoder for RawEncoder {
    type Item = Bytes;
    type Error = Status;

    fn encode(&mut self, item: Bytes, dst: &mut EncodeBuf<'_>) -> Result<(), Status> {
        dst.put(item);
        Ok(())
    }
}

struct RawDecoder;

impl Decoder for RawDecoder {
    type Item = Bytes;
    type Error = Status;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Bytes>, Status> {
        let n = src.remaining();
        Ok(Some(src.copy_to_bytes(n)))
    }
}

fn method_path(method: &MethodDescriptor) -> PathAndQuery {
    let path = format!("/{}/{}", method.parent_service().full_name(), method.name());
    PathAndQuery::from_maybe_shared(path).expect("a proto method name is a valid path")
}

type RawGrpc = tonic::client::Grpc<InterceptedService<Channel, Bearer>>;

fn raw_grpc(ctx: &Ctx, addr: &str) -> Result<RawGrpc, Reply> {
    let channel = ctx
        .channel(addr)
        .map_err(|e| Reply::error(502, "UNAVAILABLE", e))?;
    Ok(
        tonic::client::Grpc::new(InterceptedService::new(channel, ctx.security.bearer()))
            .max_decoding_message_size(crate::MAX_MESSAGE_BYTES)
            .max_encoding_message_size(crate::MAX_MESSAGE_BYTES),
    )
}

/// proto3 JSON -> message bytes for `method`'s input. An empty body is
/// the empty message.
fn encode_request(method: &MethodDescriptor, json: &[u8]) -> Result<Bytes, Reply> {
    let body: &[u8] = if json.iter().all(u8::is_ascii_whitespace) {
        b"{}"
    } else {
        json
    };
    let mut deserializer = serde_json::Deserializer::from_slice(body);
    let message = DynamicMessage::deserialize(method.input(), &mut deserializer).map_err(|e| {
        Reply::error(
            400,
            "INVALID_ARGUMENT",
            format!("request JSON for {}: {e}", method.input().full_name()),
        )
    })?;
    Ok(Bytes::from(message.encode_to_vec()))
}

/// Message bytes -> proto3 JSON for `method`'s output.
fn decode_response(method: &MethodDescriptor, bytes: Bytes) -> Result<Vec<u8>, Reply> {
    let message = DynamicMessage::decode(method.output(), bytes).map_err(|e| {
        Reply::error(
            502,
            "INTERNAL",
            format!("response bytes for {}: {e}", method.output().full_name()),
        )
    })?;
    let mut out = Vec::new();
    let mut serializer = serde_json::Serializer::new(&mut out);
    // Every field is rendered, defaults included (what grpcurl calls
    // `-emit-defaults`): a document id of 0 or a score of 0 is a value a
    // reader needs to see, not an omission.
    message
        .serialize_with_options(
            &mut serializer,
            &SerializeOptions::new().skip_default_fields(false),
        )
        .map_err(|e| Reply::error(502, "INTERNAL", format!("response JSON: {e}")))?;
    Ok(out)
}

/// The doc's status-to-HTTP table.
fn http_status(code: Code) -> u16 {
    match code {
        Code::InvalidArgument => 400,
        Code::Unauthenticated => 401,
        Code::PermissionDenied => 403,
        Code::NotFound => 404,
        Code::ResourceExhausted => 429,
        Code::Unimplemented => 501,
        _ => 502,
    }
}

fn code_name(code: Code) -> &'static str {
    match code {
        Code::Ok => "OK",
        Code::Cancelled => "CANCELLED",
        Code::Unknown => "UNKNOWN",
        Code::InvalidArgument => "INVALID_ARGUMENT",
        Code::DeadlineExceeded => "DEADLINE_EXCEEDED",
        Code::NotFound => "NOT_FOUND",
        Code::AlreadyExists => "ALREADY_EXISTS",
        Code::PermissionDenied => "PERMISSION_DENIED",
        Code::ResourceExhausted => "RESOURCE_EXHAUSTED",
        Code::FailedPrecondition => "FAILED_PRECONDITION",
        Code::Aborted => "ABORTED",
        Code::OutOfRange => "OUT_OF_RANGE",
        Code::Unimplemented => "UNIMPLEMENTED",
        Code::Internal => "INTERNAL",
        Code::Unavailable => "UNAVAILABLE",
        Code::DataLoss => "DATA_LOSS",
        Code::Unauthenticated => "UNAUTHENTICATED",
    }
}

fn status_reply(status: &Status) -> Reply {
    Reply::error(
        http_status(status.code()),
        code_name(status.code()),
        status.message().to_string(),
    )
}

/// A status from a call, with an empty UNIMPLEMENTED (what a server
/// answers for a service it does not register) given words.
fn call_status_reply(status: &Status, method: &MethodDescriptor, addr: &str) -> Reply {
    if status.code() == Code::Unimplemented && status.message().is_empty() {
        return Reply::error(
            501,
            "UNIMPLEMENTED",
            format!(
                "{}.{} is not served by {addr}",
                method.parent_service().name(),
                method.name()
            ),
        );
    }
    status_reply(status)
}

// ---------------------------------------------------------------------------
// HTTP
// ---------------------------------------------------------------------------

/// One HTTP response.
#[derive(Debug)]
struct Reply {
    status: u16,
    content_type: &'static str,
    body: Vec<u8>,
}

impl Reply {
    fn json(status: u16, value: Value) -> Self {
        Self {
            status,
            content_type: "application/json",
            body: value.to_string().into_bytes(),
        }
    }

    fn json_bytes(body: Vec<u8>) -> Self {
        Self {
            status: 200,
            content_type: "application/json",
            body,
        }
    }

    fn error(status: u16, code: &str, message: impl Into<String>) -> Self {
        Self::json(status, json!({ "error": message.into(), "code": code }))
    }

    fn text(status: u16, content_type: &'static str, body: &str) -> Self {
        Self {
            status,
            content_type,
            body: body.as_bytes().to_vec(),
        }
    }
}

struct HttpRequest {
    method: String,
    path: String,
    query: Vec<(String, String)>,
    body: Vec<u8>,
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Percent-decoding with `+` as space, by hand (no regex, no crate).
fn url_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < bytes.len() => {
                let hex = &s[i + 1..i + 3];
                match u8::from_str_radix(hex, 16) {
                    Ok(v) => {
                        out.push(v);
                        i += 2;
                    }
                    Err(_) => out.push(b'%'),
                }
            }
            b => out.push(b),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn parse_query(query: &str) -> Vec<(String, String)> {
    query
        .split('&')
        .filter(|kv| !kv.is_empty())
        .map(|kv| match kv.split_once('=') {
            Some((k, v)) => (url_decode(k), url_decode(v)),
            None => (url_decode(kv), String::new()),
        })
        .collect()
}

fn param<'a>(query: &'a [(String, String)], key: &str) -> Option<&'a str> {
    query
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
}

async fn read_request(stream: &mut TcpStream) -> std::io::Result<Result<HttpRequest, Reply>> {
    let mut buf: Vec<u8> = Vec::with_capacity(4096);
    let header_end = loop {
        let mut chunk = [0u8; 4096];
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Ok(Err(Reply::text(400, "text/plain", "empty request")));
        }
        buf.extend_from_slice(&chunk[..n]);
        if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
            break pos + 4;
        }
        if buf.len() > MAX_BODY_BYTES {
            return Ok(Err(Reply::text(431, "text/plain", "headers too large")));
        }
    };
    let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let mut lines = head.lines();
    let request_line = lines.next().unwrap_or_default();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let target = parts.next().unwrap_or_default().to_string();
    let (path, query) = match target.split_once('?') {
        Some((p, q)) => (p.to_string(), parse_query(q)),
        None => (target.clone(), Vec::new()),
    };
    let content_length = lines
        .filter_map(|l| l.split_once(':'))
        .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, v)| v.trim().parse::<usize>().ok())
        .unwrap_or(0);
    if content_length > MAX_BODY_BYTES {
        return Ok(Err(Reply::text(413, "text/plain", "body too large")));
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
    body.truncate(content_length);
    Ok(Ok(HttpRequest {
        method,
        path,
        query,
        body,
    }))
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        413 => "Payload Too Large",
        429 => "Too Many Requests",
        431 => "Request Header Fields Too Large",
        501 => "Not Implemented",
        502 => "Bad Gateway",
        _ => "Error",
    }
}

async fn write_reply(stream: &mut TcpStream, reply: Reply) -> std::io::Result<()> {
    let head = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n",
        reply.status,
        reason(reply.status),
        reply.content_type,
        reply.body.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(&reply.body).await?;
    stream.flush().await
}

async fn handle_conn(mut stream: TcpStream, ctx: Arc<Ctx>) -> std::io::Result<()> {
    let request = match read_request(&mut stream).await? {
        Ok(request) => request,
        Err(reply) => return write_reply(&mut stream, reply).await,
    };
    let segments: Vec<&str> = request.path.split('/').filter(|s| !s.is_empty()).collect();
    match (request.method.as_str(), segments.as_slice()) {
        ("GET", ["api", "stream", service, method])
        | ("POST", ["api", "stream", service, method]) => {
            stream_rpc(&mut stream, &ctx, service, method, &request).await
        }
        _ => {
            let reply = route(&ctx, &request, &segments).await;
            write_reply(&mut stream, reply).await
        }
    }
}

fn asset(path: &str) -> Option<(&'static str, &'static str)> {
    Some(match path {
        "/" | "/index.html" => ("text/html; charset=utf-8", INDEX_HTML),
        "/dashboard" | "/dashboard.html" => ("text/html; charset=utf-8", DASHBOARD_HTML),
        "/app.css" => ("text/css; charset=utf-8", APP_CSS),
        "/common.js" => ("text/javascript; charset=utf-8", COMMON_JS),
        "/search.js" => ("text/javascript; charset=utf-8", SEARCH_JS),
        "/dashboard.js" => ("text/javascript; charset=utf-8", DASHBOARD_JS),
        "/sparkline.js" => ("text/javascript; charset=utf-8", SPARKLINE_JS),
        _ => return None,
    })
}

async fn route(ctx: &Ctx, request: &HttpRequest, segments: &[&str]) -> Reply {
    match (request.method.as_str(), segments) {
        ("GET", []) | ("GET", [_]) if asset(&request.path).is_some() => {
            let (content_type, body) = asset(&request.path).expect("checked");
            Reply::text(200, content_type, body)
        }
        ("GET", ["api", "health"]) => match health(ctx).await {
            Ok(v) => Reply::json(200, v),
            Err(e) => Reply::error(502, "UNAVAILABLE", e),
        },
        ("GET", ["api", "config"]) => Reply::json(200, config_json(ctx)),
        ("POST", ["api", "embed"]) => embed(ctx, &request.body).await,
        ("POST", ["api", "documents"]) => documents(ctx, &request.body).await,
        ("POST", ["api", "rpc", service, method]) => unary_rpc(ctx, service, method, request).await,
        ("GET", ["api", "rpc", ..]) => Reply::error(
            405,
            "INVALID_ARGUMENT",
            "POST the request message as proto3 JSON to /api/rpc/<Service>/<Method>",
        ),
        _ => Reply::error(404, "NOT_FOUND", format!("no route for {}", request.path)),
    }
}

fn config_json(ctx: &Ctx) -> Value {
    let methods: Vec<Value> = exposed_methods()
        .into_iter()
        .map(|(service, method, streaming)| {
            json!({ "service": service, "method": method, "server_streaming": streaming })
        })
        .collect();
    let spec = crate::analyzer::body_spec();
    json!({
        "coordinator": ctx.coordinator,
        "nodes": ctx.nodes,
        "analysis": ctx.analysis.is_some(),
        "tls": ctx.security.is_tls(),
        "bearer": ctx.security.has_bearer(),
        "methods": methods,
        "body_spec": {
            "tokenizer": spec.tokenizer,
            "stemmer": spec.stemmer,
            "term_vector_mode": spec.term_vector_mode,
            "term_vector_source": spec.term_vector_source,
            "char_filters": spec.char_filters,
        },
    })
}

async fn unary_rpc(ctx: &Ctx, service: &str, method: &str, request: &HttpRequest) -> Reply {
    let method = match resolve_method(service, method) {
        Ok(m) => m,
        Err(reply) => return reply,
    };
    if method.is_server_streaming() {
        return Reply::error(
            400,
            "INVALID_ARGUMENT",
            format!(
                "{service}.{} is server-streaming: use /api/stream/{service}/{}",
                method.name(),
                method.name()
            ),
        );
    }
    let addr = match ctx.target(param(&request.query, "target")) {
        Ok(addr) => addr,
        Err(reply) => return reply,
    };
    let bytes = match encode_request(&method, &request.body) {
        Ok(b) => b,
        Err(reply) => return reply,
    };
    let mut grpc = match raw_grpc(ctx, &addr) {
        Ok(g) => g,
        Err(reply) => return reply,
    };
    if let Err(e) = grpc.ready().await {
        return Reply::error(502, "UNAVAILABLE", format!("{addr}: {e}"));
    }
    let response = match grpc
        .unary(Request::new(bytes), method_path(&method), RawCodec)
        .await
    {
        Ok(r) => r.into_inner(),
        Err(status) => return call_status_reply(&status, &method, &addr),
    };
    match decode_response(&method, response) {
        Ok(json) => Reply::json_bytes(json),
        Err(reply) => reply,
    }
}

/// The request message for a streaming call: the POST body, else the
/// `request` query parameter (JSON), else the scalar query parameters
/// as top-level fields (`interval_ms=1000`).
fn streaming_request_json(request: &HttpRequest) -> Vec<u8> {
    if !request.body.iter().all(u8::is_ascii_whitespace) {
        return request.body.clone();
    }
    if let Some(json) = param(&request.query, "request") {
        return json.as_bytes().to_vec();
    }
    let mut object = serde_json::Map::new();
    for (k, v) in &request.query {
        if k == "target" {
            continue;
        }
        let value = if let Ok(n) = v.parse::<i64>() {
            Value::from(n)
        } else if let Ok(f) = v.parse::<f64>() {
            Value::from(f)
        } else if v == "true" || v == "false" {
            Value::Bool(v == "true")
        } else {
            Value::String(v.clone())
        };
        object.insert(k.clone(), value);
    }
    Value::Object(object).to_string().into_bytes()
}

async fn stream_rpc(
    stream: &mut TcpStream,
    ctx: &Ctx,
    service: &str,
    method: &str,
    request: &HttpRequest,
) -> std::io::Result<()> {
    let method = match resolve_method(service, method) {
        Ok(m) => m,
        Err(reply) => return write_reply(stream, reply).await,
    };
    if !method.is_server_streaming() {
        return write_reply(
            stream,
            Reply::error(
                400,
                "INVALID_ARGUMENT",
                format!(
                    "{service}.{} is unary: use POST /api/rpc/{service}/{}",
                    method.name(),
                    method.name()
                ),
            ),
        )
        .await;
    }
    let addr = match ctx.target(param(&request.query, "target")) {
        Ok(addr) => addr,
        Err(reply) => return write_reply(stream, reply).await,
    };
    let bytes = match encode_request(&method, &streaming_request_json(request)) {
        Ok(b) => b,
        Err(reply) => return write_reply(stream, reply).await,
    };
    let mut grpc = match raw_grpc(ctx, &addr) {
        Ok(g) => g,
        Err(reply) => return write_reply(stream, reply).await,
    };
    if let Err(e) = grpc.ready().await {
        return write_reply(
            stream,
            Reply::error(502, "UNAVAILABLE", format!("{addr}: {e}")),
        )
        .await;
    }
    let mut messages = match grpc
        .server_streaming(Request::new(bytes), method_path(&method), RawCodec)
        .await
    {
        Ok(r) => r.into_inner(),
        Err(status) => {
            return write_reply(stream, call_status_reply(&status, &method, &addr)).await
        }
    };
    stream
        .write_all(
            b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n",
        )
        .await?;
    stream.flush().await?;
    loop {
        match messages.message().await {
            Ok(Some(bytes)) => match decode_response(&method, bytes) {
                Ok(json) => {
                    stream.write_all(b"data: ").await?;
                    stream.write_all(&json).await?;
                    stream.write_all(b"\n\n").await?;
                    stream.flush().await?;
                }
                Err(reply) => {
                    let frame = format!(
                        "event: error\ndata: {}\n\n",
                        String::from_utf8_lossy(&reply.body)
                    );
                    stream.write_all(frame.as_bytes()).await?;
                    break;
                }
            },
            Ok(None) => {
                stream.write_all(b"event: end\ndata: {}\n\n").await?;
                break;
            }
            Err(status) => {
                let frame = format!(
                    "event: error\ndata: {}\n\n",
                    json!({ "error": status.message(), "code": code_name(status.code()) })
                );
                stream.write_all(frame.as_bytes()).await?;
                break;
            }
        }
    }
    stream.flush().await
}

// ---------------------------------------------------------------------------
// Convenience routes
// ---------------------------------------------------------------------------

async fn health(ctx: &Ctx) -> Result<Value, String> {
    let mut client = ctx.search_client(&ctx.coordinator)?;
    let response = client
        .cluster_health(ClusterHealthRequest {
            collection: String::new(),
        })
        .await
        .map_err(|e| e.to_string())?
        .into_inner();
    let mut ranges = Vec::new();
    let targets: Vec<Value> = response
        .targets
        .iter()
        .map(|t| {
            if let (false, true, Some(h)) = (t.is_replica, t.reachable, t.health.as_ref()) {
                ranges.push(ShardRange {
                    shard: t.shard,
                    addr: ctx.security.url(&t.addr),
                    slot_offset: h.slot_offset,
                    rows: h.document_slots.max(h.num_vectors),
                });
            }
            json!({
                "shard": t.shard,
                "addr": t.addr,
                "is_replica": t.is_replica,
                "reachable": t.reachable,
                "error": t.error,
                "num_vectors": t.health.as_ref().map(|h| h.num_vectors),
                "bm25_docs": t.health.as_ref().map(|h| h.bm25_docs),
                "live_docs": t.health.as_ref().map(|h| h.live_docs),
                "deleted_docs": t.health.as_ref().map(|h| h.deleted_docs),
                "dim": t.health.as_ref().map(|h| h.dim),
                "slot_offset": t.health.as_ref().map(|h| h.slot_offset),
                "ingest_active": t.health.as_ref().map(|h| h.ingest_active),
                "vector_backend": t.health.as_ref().map(|h| h.vector_backend.clone()),
                "scoring_fingerprint": t.health.as_ref().map(|h| h.scoring_fingerprint.clone()),
                "exact_vectors_available": t.health.as_ref().map(|h| h.exact_vectors_available),
                "collection": t.health.as_ref().map(|h| h.collection.clone()),
            })
        })
        .collect();
    *ctx.health.lock().expect("health cache poisoned") = Some((Instant::now(), ranges));
    Ok(json!({
        "targets": targets,
        "topology_generation": response.topology_generation,
        "provider_mismatch": response.provider_mismatch,
        "collections": response.collections.iter().map(|c| c.name.clone()).collect::<Vec<_>>(),
    }))
}

async fn shard_ranges(ctx: &Ctx) -> Result<Vec<ShardRange>, String> {
    if let Some((at, ranges)) = ctx.health.lock().expect("health cache poisoned").as_ref() {
        if at.elapsed() < HEALTH_TTL {
            return Ok(ranges.clone());
        }
    }
    health(ctx).await?;
    Ok(ctx
        .health
        .lock()
        .expect("health cache poisoned")
        .as_ref()
        .map(|(_, r)| r.clone())
        .unwrap_or_default())
}

#[derive(serde::Deserialize)]
struct DocumentsBody {
    #[serde(default)]
    doc_ids: Vec<Value>,
}

/// Stored text for global document ids, from the owning nodes: an id
/// belongs to the shard whose slot range holds it.
async fn documents(ctx: &Ctx, body: &[u8]) -> Reply {
    let parsed: DocumentsBody = match serde_json::from_slice(body) {
        Ok(b) => b,
        Err(e) => return Reply::error(400, "INVALID_ARGUMENT", format!("bad request: {e}")),
    };
    let mut ids = Vec::with_capacity(parsed.doc_ids.len());
    for value in &parsed.doc_ids {
        let id = match value {
            Value::Number(n) => n.as_u64(),
            Value::String(s) => s.parse::<u64>().ok(),
            _ => None,
        };
        match id {
            Some(id) => ids.push(id),
            None => {
                return Reply::error(
                    400,
                    "INVALID_ARGUMENT",
                    format!("doc_ids entries are unsigned integers, got {value}"),
                )
            }
        }
    }
    if ids.len() > 1000 {
        return Reply::error(400, "INVALID_ARGUMENT", "at most 1000 doc_ids per call");
    }
    let ranges = match shard_ranges(ctx).await {
        Ok(r) => r,
        Err(e) => return Reply::error(502, "UNAVAILABLE", e),
    };
    let mut by_shard: HashMap<u32, (String, Vec<u64>)> = HashMap::new();
    let mut unrouted = Vec::new();
    for id in ids {
        match ranges
            .iter()
            .find(|r| id >= r.slot_offset && id < r.slot_offset + r.rows)
        {
            Some(r) => by_shard
                .entry(r.shard)
                .or_insert_with(|| (r.addr.clone(), Vec::new()))
                .1
                .push(id),
            None => unrouted.push(id),
        }
    }
    let mut documents = Vec::new();
    for (shard, (addr, doc_ids)) in by_shard {
        let mut client = match ctx.node_client(&addr) {
            Ok(c) => c,
            Err(e) => return Reply::error(502, "UNAVAILABLE", e),
        };
        let found = match client.get_documents(GetDocumentsRequest { doc_ids }).await {
            Ok(r) => r.into_inner(),
            Err(status) => {
                return Reply::error(
                    http_status(status.code()),
                    code_name(status.code()),
                    format!("get_documents on shard {shard}: {}", status.message()),
                )
            }
        };
        for doc in found.documents {
            documents.push(json!({
                "doc_id": doc.doc_id.to_string(),
                "shard": shard,
                "text": doc.text,
                "lineage": doc.lineage.map(|l| json!({
                    "parent_id": l.parent_id.to_string(),
                    "group_id": l.group_id.to_string(),
                    "span_start": l.span_start,
                    "span_end": l.span_end,
                })),
            }));
        }
    }
    Reply::json(
        200,
        json!({
            "documents": documents,
            "unrouted": unrouted.iter().map(|id| id.to_string()).collect::<Vec<_>>(),
        }),
    )
}

#[derive(serde::Deserialize)]
struct EmbedBody {
    #[serde(default)]
    text: String,
}

async fn embed(ctx: &Ctx, body: &[u8]) -> Reply {
    let Some(addr) = ctx.analysis.as_deref() else {
        return Reply::error(
            501,
            "UNIMPLEMENTED",
            "no --analysis sidecar configured; dense queries need a pasted vector",
        );
    };
    let parsed: EmbedBody = match serde_json::from_slice(body) {
        Ok(b) => b,
        Err(e) => return Reply::error(400, "INVALID_ARGUMENT", format!("bad request: {e}")),
    };
    if parsed.text.trim().is_empty() {
        return Reply::error(400, "INVALID_ARGUMENT", "text is required");
    }
    let started = Instant::now();
    match crate::analyzer::embed_text(addr, &parsed.text).await {
        Ok(vector) => Reply::json(
            200,
            json!({
                "dim": vector.len(),
                "vector": vector,
                "ms": started.elapsed().as_secs_f32() * 1e3,
            }),
        ),
        Err(status) => status_reply(&status),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_decoding_handles_percent_and_plus() {
        assert_eq!(url_decode("a+b%20c%2Fd"), "a b c/d");
        assert_eq!(url_decode("%zz"), "%zz");
        assert_eq!(url_decode("trail%2"), "trail%2");
    }

    #[test]
    fn query_parsing_keeps_order_and_empty_values() {
        let q = parse_query("interval_ms=250&flag&x=%7B%22a%22%3A1%7D");
        assert_eq!(q[0], ("interval_ms".into(), "250".into()));
        assert_eq!(q[1], ("flag".into(), String::new()));
        assert_eq!(q[2].1, "{\"a\":1}");
    }

    #[test]
    fn the_descriptor_set_exposes_both_services() {
        let methods = exposed_methods();
        assert!(methods
            .iter()
            .any(|(s, m, streaming)| s == "SearchService" && m == "Query" && !streaming));
        assert!(methods
            .iter()
            .any(|(s, m, streaming)| s == "SearchService" && m == "QueryStream" && *streaming));
        assert!(methods
            .iter()
            .any(|(s, m, streaming)| s == "DiagnosticsService"
                && m == "StreamMetrics"
                && *streaming));
        assert!(!methods
            .iter()
            .any(|(s, m, _)| s == "SearchService" && m == "RoutedIngestMapped"));
        assert!(resolve_method("NodeService", "Health").is_err());
        assert!(resolve_method("SearchService", "Nope").is_err());
    }

    #[test]
    fn json_round_trips_through_the_descriptor() {
        let method = resolve_method("SearchService", "Query").unwrap();
        let json = br#"{"k": 3, "explain": true, "selection": {"search": {"id": "q", "lexical": {"text": "coffee"}}}}"#;
        let bytes = encode_request(&method, json).unwrap();
        let typed = <crate::pb::QueryRequest as prost::Message>::decode(bytes.as_ref()).unwrap();
        assert_eq!(typed.k, 3);
        assert!(typed.explain);
        let response = crate::pb::QueryResponse {
            request_id: "r".into(),
            hits: vec![crate::pb::QueryHit {
                doc_id: 7,
                score: 1.5,
                rank: 1,
                ..Default::default()
            }],
            ..Default::default()
        };
        let encoded = Bytes::from(prost::Message::encode_to_vec(&response));
        let out = decode_response(&method, encoded).unwrap();
        let value: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(value["requestId"], "r");
        assert_eq!(value["hits"][0]["docId"], "7");
        assert_eq!(value["hits"][0]["rank"], 1);
        assert!(encode_request(&method, b"{\"k\": \"three\"}").is_err());
    }

    #[test]
    fn scalar_query_parameters_become_top_level_fields() {
        let request = HttpRequest {
            method: "GET".into(),
            path: "/api/stream/DiagnosticsService/StreamMetrics".into(),
            query: parse_query("interval_ms=250&target=node0&verbose=true"),
            body: Vec::new(),
        };
        let json: Value = serde_json::from_slice(&streaming_request_json(&request)).unwrap();
        assert_eq!(json["interval_ms"], 250);
        assert_eq!(json["verbose"], true);
        assert!(json.get("target").is_none());
    }
}

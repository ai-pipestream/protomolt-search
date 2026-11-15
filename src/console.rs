//! The console: a JSON facade over the cluster's gRPC plus the web UI it
//! serves (`docs/console-facade.md`).
//!
//! The facade is a client of a running cluster and never part of it. It
//! holds the TLS material and the bearer token, so a browser carries
//! neither. Its one real job is transcoding: `POST /api/rpc/<Service>/
//! <Method>` takes the request message as proto3 JSON, sends it over
//! gRPC, and answers with the response as proto3 JSON, for every unary
//! method of `SearchService` and `DiagnosticsService`, plus the one
//! `ClusterControl` method the dashboard needs (`PlanBalance`, a
//! read-only dry run); the server-streaming methods are exposed as
//! server-sent events under `/api/stream/...`. The mapping is built from
//! the compiled descriptor set at run time, so an RPC added to an
//! exposed service needs no change here.
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
use prost_reflect::{
    DescriptorPool, DynamicMessage, EnumDescriptor, Kind, MessageDescriptor, MethodDescriptor,
    SerializeOptions,
};
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
/// What a browser may reach through the facade: a service with an empty
/// method list is exposed whole (unary and server-streaming methods);
/// a service with a list is exposed for those methods only.
/// `NodeService` is the cluster's internal surface and is absent;
/// `ClusterControl` is internal too, and only its read-only balance dry
/// run is listed, for the dashboard. The wire still applies cluster
/// trust to that call: the facade's own credentials must be a member's.
const EXPOSED: &[(&str, &[&str])] = &[
    ("SearchService", &[]),
    ("DiagnosticsService", &[]),
    ("ClusterControl", &["PlanBalance"]),
];

/// Whether `service.method` is reachable through the facade.
fn is_exposed(service: &str, method: &str) -> bool {
    EXPOSED
        .iter()
        .any(|(s, methods)| *s == service && (methods.is_empty() || methods.contains(&method)))
}

fn exposed_summary() -> String {
    EXPOSED
        .iter()
        .map(|(s, methods)| {
            if methods.is_empty() {
                s.to_string()
            } else {
                format!("{s} ({} only)", methods.join(", "))
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}
/// Request bodies are JSON; anything past this is a mistake.
const MAX_BODY_BYTES: usize = 8 << 20;
/// `ClusterHealth` is re-read at most this often for document routing.
const HEALTH_TTL: Duration = Duration::from_secs(5);
/// How long a stream's client may take to accept one frame.
const STREAM_WRITE_DEADLINE: Duration = Duration::from_secs(10);

const INDEX_HTML: &str = include_str!("bin/console-ui/index.html");
const DASHBOARD_HTML: &str = include_str!("bin/console-ui/dashboard.html");
const EXPLORER_HTML: &str = include_str!("bin/console-ui/explorer.html");
const EXPLORER_JS: &str = include_str!("bin/console-ui/explorer.js");
const APP_CSS: &str = include_str!("bin/console-ui/app.css");
const COMMON_JS: &str = include_str!("bin/console-ui/common.js");
const SEARCH_JS: &str = include_str!("bin/console-ui/search.js");
const QUERY_STATE_JS: &str = include_str!("bin/console-ui/query-state.js");
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

mod codec;
mod http;
mod routes;
mod rpc;
mod schema;
#[cfg(test)]
mod tests;

use self::codec::*;
pub use self::codec::{json_to_message_bytes, message_bytes_to_json};
use self::http::*;
use self::routes::*;
use self::rpc::*;
use self::schema::*;
pub use self::schema::{descriptor_pool, exposed_methods};

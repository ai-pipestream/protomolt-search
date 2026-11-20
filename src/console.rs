//! The console: a backend-for-frontend over the cluster's gRPC plus the web
//! UI it serves.
//!
//! The console is a client of a running cluster and never part of it. It
//! holds the TLS material and the bearer token, so a browser carries
//! neither. A browser reaches one surface, `ai.protomolt.console.bff.v1.
//! ConsoleBffService`, served under `/bff/v1` as the Connect protocol:
//! unary calls as `application/json`, server streams as
//! `application/connect+json` envelopes, and the same streams as
//! server-sent events under `/bff/v1/sse/<Method>` with `: keepalive`
//! comments for proxies that buffer. Every method is read-only, goes to the
//! coordinator, and is listed in one fixed table; no request names a node
//! or an address, and an upstream failure answers with its code and a generic
//! message rather than the cluster's addresses.
//!
//! The operator explorer is the older, raw surface: `POST /api/rpc/
//! <Service>/<Method>` transcodes proto3 JSON for every unary method of
//! `SearchService` and `DiagnosticsService` plus `ClusterControl.
//! PlanBalance`, `/api/stream/...` serves their streams as server-sent
//! events, `?target=node<i>` routes to a configured node, and `/api/health`,
//! `/api/config`, `/api/describe`, `/api/embed` and `/api/documents` back
//! the explorer and dashboard pages. It is off unless
//! [`Console::with_raw_explorer`] (`--enable-raw-explorer`) turns it on, and
//! that is refused for a console started with `--allow-remote`.
//!
//! The HTTP side is hand-rolled HTTP/1.1: persistent connections with
//! pipelined requests answered in order, `Content-Length` and chunked
//! request bodies up to 8 MiB, deadlines on reading a request's head and
//! body and on an idle connection, and a cap on open connections. Binding
//! a non-loopback address needs `--allow-remote`, because whoever reaches
//! the console acts as its principal.
//!
//! The UI is compiled in; `ui/` builds its Connect client into
//! `src/bin/console-ui/bff.js`. [`Console::with_ui_dir`] serves the files
//! of a directory instead, read on every request, so UI work needs a reload
//! rather than a rebuild; a file missing there falls back to the
//! compiled-in copy.

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use bytes::{Buf, BufMut, Bytes};
use prost::Message as _;
use prost_reflect::{
    DescriptorPool, DynamicMessage, EnumDescriptor, Kind, MessageDescriptor, MethodDescriptor,
    SerializeOptions,
};
use serde_json::{json, Value};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::codegen::http::uri::PathAndQuery;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use tonic::{Code, Request, Status};

use crate::analyzer::RemoteCallOptions;
use crate::pb::node_service_client::NodeServiceClient;
use crate::pb::search_service_client::SearchServiceClient;
use crate::pb::{ClusterHealthRequest, ClusterHealthResponse, GetDocumentsRequest, StoredDocument};
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

/// Connections served at once; further clients wait in the listen
/// backlog until one closes.
const MAX_CONNECTIONS: usize = 256;
/// How long one embedding call to the analysis endpoint may take.
const EMBED_DEADLINE: Duration = Duration::from_secs(30);
/// How often an idle server-sent stream carries a `: keepalive` comment.
const SSE_HEARTBEAT: Duration = Duration::from_secs(15);

struct Ctx {
    coordinator: String,
    nodes: Vec<String>,
    analysis: Option<String>,
    security: ToolClient,
    /// What every `/api/embed` call carries to the analysis endpoint.
    embed: RemoteCallOptions,
    /// The canonical `--ui-dir`, when the UI is served from disk.
    ui_dir: Option<PathBuf>,
    /// Whether the operator's `/api` routes and pages are served.
    raw_explorer: bool,
    /// Whether the listener is a loopback address, so `/bff` requests
    /// must name a loopback host (a rebound DNS name does not).
    loopback: bool,
    /// The `: keepalive` period of `/bff/v1/sse` streams.
    sse_heartbeat: Duration,
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
    max_connections: usize,
    allow_remote: bool,
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
                embed: embed_options(&config.security),
                security: config.security,
                ui_dir: None,
                raw_explorer: false,
                loopback: crate::security::is_loopback(&addr),
                sse_heartbeat: SSE_HEARTBEAT,
                channels: Mutex::new(HashMap::new()),
                health: Mutex::new(None),
            }),
            max_connections: MAX_CONNECTIONS,
            allow_remote: config.allow_remote,
        })
    }

    fn ctx_mut(&mut self) -> &mut Ctx {
        Arc::get_mut(&mut self.ctx).expect("the context is shared only once serving starts")
    }

    /// Serves the operator explorer: `/api/...`, `/explorer`, and
    /// `/dashboard`. Refused for a console bound with `allow_remote`.
    pub fn with_raw_explorer(mut self) -> Result<Self, String> {
        if self.allow_remote {
            return Err(RAW_WITH_REMOTE.to_string());
        }
        self.ctx_mut().raw_explorer = true;
        Ok(self)
    }

    /// Sets the `: keepalive` period of `/bff/v1/sse` streams (at least
    /// one millisecond).
    pub fn with_sse_heartbeat(mut self, period: Duration) -> Self {
        self.ctx_mut().sse_heartbeat = period.max(Duration::from_millis(1));
        self
    }

    /// Serves the UI files found in `dir`, read on every request, ahead
    /// of the compiled-in copies. Only single-segment `.html`, `.js` and
    /// `.css` names are looked up there, and a file that resolves outside
    /// `dir` is refused.
    pub fn with_ui_dir(mut self, dir: &Path) -> Result<Self, String> {
        let dir = dir
            .canonicalize()
            .map_err(|e| format!("--ui-dir {}: {e}", dir.display()))?;
        if !dir.is_dir() {
            return Err(format!("--ui-dir {} is not a directory", dir.display()));
        }
        self.ctx_mut().ui_dir = Some(dir);
        Ok(self)
    }

    /// Caps the connections served at once (at least one).
    pub fn with_max_connections(mut self, max: usize) -> Self {
        self.max_connections = max.max(1);
        self
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.listener
            .local_addr()
            .expect("bound listener has an address")
    }

    /// A one-line description for the startup log.
    pub fn describe(&self) -> String {
        format!(
            "console on http://{} -> coordinator {} ({} node(s), analysis {}, tls {}, bearer {}, ui {}, raw explorer {})",
            self.local_addr(),
            self.ctx.coordinator,
            self.ctx.nodes.len(),
            self.ctx
                .analysis
                .as_deref()
                .unwrap_or("none: embedding disabled"),
            self.ctx.security.is_tls(),
            self.ctx.security.has_bearer(),
            self.ctx
                .ui_dir
                .as_deref()
                .map_or_else(|| "compiled in".to_string(), |d| d.display().to_string()),
            if self.ctx.raw_explorer { "on" } else { "off" },
        )
    }

    /// Accepts connections until the listener fails, at most
    /// `max_connections` of them open at once.
    pub async fn serve(self) -> std::io::Result<()> {
        let slots = Arc::new(Semaphore::new(self.max_connections));
        loop {
            let slot = Arc::clone(&slots)
                .acquire_owned()
                .await
                .expect("the connection semaphore is never closed");
            let (stream, _) = self.listener.accept().await?;
            let ctx = Arc::clone(&self.ctx);
            tokio::spawn(async move {
                let _ = handle_conn(stream, ctx).await;
                drop(slot);
            });
        }
    }
}

mod assets;
mod bff;
mod bff_ops;
mod bff_stream;
#[cfg(test)]
mod bff_tests;
mod codec;
mod config;
mod conn;
mod http;
#[cfg(test)]
mod http_tests;
mod routes;
mod rpc;
mod schema;
#[cfg(test)]
mod serve_tests;
#[cfg(test)]
mod tests;

use self::assets::*;
use self::bff::*;
use self::bff_ops::*;
use self::bff_stream::*;
use self::codec::*;
pub use self::codec::{json_to_message_bytes, message_bytes_to_json};
use self::config::*;
pub use self::config::{raw_explorer_arg, ui_dir_arg, ConsoleConfig};
use self::conn::*;
use self::http::*;
use self::routes::*;
use self::rpc::*;
use self::schema::*;
pub use self::schema::{descriptor_pool, exposed_methods};

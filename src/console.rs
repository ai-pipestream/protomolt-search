//! The console: a JSON facade over the cluster's gRPC plus the web UI it
//! serves.
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
//! `/api/config`, `/api/embed` (the analysis endpoint's embedding, when
//! one is configured, dialed with the console's TLS material and bearer
//! token), and `/api/documents` (stored text from the owning nodes,
//! routed by the cluster's slot ranges and read from them concurrently).
//!
//! The HTTP side is hand-rolled HTTP/1.1: persistent connections with
//! pipelined requests answered in order, `Content-Length` and chunked
//! request bodies, deadlines on reading a request's head and body and on
//! an idle connection, and a cap on open connections. This is an
//! operator's tool bound to loopback by default, not a product server.
//! Binding elsewhere needs `--allow-remote`, because whoever reaches the
//! facade acts as its principal.
//!
//! The UI is compiled in. [`Console::with_ui_dir`] serves the files of a
//! directory instead, read on every request, so UI work needs a reload
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
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
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

/// Connections served at once; further clients wait in the listen
/// backlog until one closes.
const MAX_CONNECTIONS: usize = 256;
/// How long one embedding call to the analysis endpoint may take.
const EMBED_DEADLINE: Duration = Duration::from_secs(30);

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

/// The `--ui-dir` flag, as `--ui-dir=<path>` or `--ui-dir <path>`.
pub fn ui_dir_arg(args: &[String]) -> Option<PathBuf> {
    flag(args, "ui-dir").map(PathBuf::from).or_else(|| {
        let at = args.iter().position(|a| a == "--ui-dir")?;
        args.get(at + 1).map(PathBuf::from)
    })
}

/// The options every embedding call carries: the interactive lane, a
/// deadline, the tool's bearer token, and, when the tool dials TLS, its
/// client TLS material. `ToolClient` hands that material over through the
/// process-wide install, which this performs when nothing is installed
/// yet; an earlier install in the same process wins.
fn embed_options(security: &ToolClient) -> RemoteCallOptions {
    if security.is_tls() && crate::security::process_client_tls().is_none() {
        security.install();
    }
    RemoteCallOptions {
        bearer_token: security.raw_bearer_token().map(str::to_string),
        interactive: true,
        timeout: Some(EMBED_DEADLINE),
        tls: if security.is_tls() {
            crate::security::process_client_tls().cloned()
        } else {
            None
        },
        ..Default::default()
    }
}

impl ConsoleConfig {
    /// Flags, all `--key=value`: `--listen` (default `127.0.0.1:8600`),
    /// `--coordinator` (default `http://127.0.0.1:50050`), `--nodes`
    /// (comma-separated, in shard order), `--analysis` (the sidecar,
    /// for embeddings), `--allow-remote`, and the tool security flags
    /// `ToolClient` reads.
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
            // A bare address gets `http://`; with `--tls-ca` the embedding
            // call dials it as `https://` (see `embed_options`).
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
    /// What every `/api/embed` call carries to the analysis endpoint.
    embed: RemoteCallOptions,
    /// The canonical `--ui-dir`, when the UI is served from disk.
    ui_dir: Option<PathBuf>,
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
                channels: Mutex::new(HashMap::new()),
                health: Mutex::new(None),
            }),
            max_connections: MAX_CONNECTIONS,
        })
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
        Arc::get_mut(&mut self.ctx)
            .expect("the context is shared only once serving starts")
            .ui_dir = Some(dir);
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
            "console on http://{} -> coordinator {} ({} node(s), analysis {}, tls {}, bearer {}, ui {})",
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
mod codec;
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
use self::codec::*;
pub use self::codec::{json_to_message_bytes, message_bytes_to_json};
use self::conn::*;
use self::http::*;
use self::routes::*;
use self::rpc::*;
use self::schema::*;
pub use self::schema::{descriptor_pool, exposed_methods};

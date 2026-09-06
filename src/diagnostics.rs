//! The diagnostics service (`docs/diagnostics.md`): runtime knobs that
//! flip without a restart, structured metrics snapshots and a stream
//! of them, per-shard layout diagnostics, and the coordinator's ring of
//! recent public requests.
//!
//! The knobs are atomics read at request time. Each process role holds
//! its own [`Knobs`]: a node's carries the scan and floor settings, a
//! coordinator's the request caps. A setting the code still reads from
//! its construction-time config is listed as immutable, and setting it
//! is rejected by name; the list never claims more than the process
//! honors.

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use crate::authorization::{AccessPermit, AuthorizedStream};
use crate::metrics::Route;
use crate::pb::diagnostics_service_server::{DiagnosticsService, DiagnosticsServiceServer};
use crate::pb::{
    GetRuntimeKnobsRequest, KnobKind, KnobScope, MetricsSnapshot, MetricsSnapshotRequest,
    RecentQueriesRequest, RecentQueriesResponse, RecentQuery, RuntimeKnob, RuntimeKnobs,
    SetRuntimeKnobRequest, ShardDiagnostics, ShardDiagnosticsRequest, ShardLayoutDiagnostics,
    StreamMetricsRequest,
};
use crate::security::Principals;

/// The ring keeps this many recent requests.
pub const RECENT_RING: usize = 256;
/// `RecentQueriesRequest.limit == 0` selects this many.
pub const RECENT_DEFAULT: usize = 50;
/// `StreamMetricsRequest.interval_ms == 0` selects this.
pub const STREAM_DEFAULT_MS: u32 = 1000;
/// Below this the stream is rejected.
pub const STREAM_MIN_MS: u32 = 100;

/// One knob's static description.
struct KnobSpec {
    name: &'static str,
    scope: KnobScope,
    kind: KnobKind,
    description: &'static str,
}

const FLOOR_SHARING: KnobSpec = KnobSpec {
    name: "floor_sharing",
    scope: KnobScope::Node,
    kind: KnobKind::Bool,
    description: "Publish this shard's k-th best score to the coordinator and prune \
                  candidates under the shared cutoff (--floor-sharing).",
};
const SEGMENT_PRUNING: KnobSpec = KnobSpec {
    name: "segment_pruning",
    scope: KnobScope::Node,
    kind: KnobKind::Bool,
    description: "Skip sealed segments whose column summary rules out the request's filter \
                  (--segment-pruning).",
};
const FLOOR_DELTA: KnobSpec = KnobSpec {
    name: "floor_delta",
    scope: KnobScope::Node,
    kind: KnobKind::Float,
    description: "Smallest score movement that publishes a new floor (--floor-delta).",
};
const FLOOR_WARMUP_CHUNKS: KnobSpec = KnobSpec {
    name: "floor_warmup_chunks",
    scope: KnobScope::Node,
    kind: KnobKind::Int,
    description: "Scan chunks to finish before the first floor is published \
                  (--floor-warmup-chunks).",
};
const FLOOR_MIN_INTERVAL_MS: KnobSpec = KnobSpec {
    name: "floor_min_interval_ms",
    scope: KnobScope::Node,
    kind: KnobKind::Int,
    description: "Shortest gap between floor publications in milliseconds; 0 publishes on \
                  each movement (--floor-min-interval-ms).",
};
const MAX_K: KnobSpec = KnobSpec {
    name: "max_k",
    scope: KnobScope::Coordinator,
    kind: KnobKind::Int,
    description: "Largest k a request may ask for, and the depth an omitted k runs at \
                  (--max-k).",
};
const HEDGE_DELAY_MS: KnobSpec = KnobSpec {
    name: "hedge_delay_ms",
    scope: KnobScope::Coordinator,
    kind: KnobKind::Int,
    description: "Wait on a shard's primary before racing its replica; 0 disables hedging \
                  (--hedge-delay-ms).",
};
const SIGNAL_BATCH: KnobSpec = KnobSpec {
    name: "signal_batch",
    scope: KnobScope::Coordinator,
    kind: KnobKind::Int,
    description: "Candidate ids per rescore call (Bm25Rescore, VectorRescore) when a boolean \
                  group scores a clause over its surviving ids; the pieces multiply per-call \
                  cost, and max_k pieces are the earlier behavior (--signal-batch).",
};
const SHARD_PRUNING: KnobSpec = KnobSpec {
    name: "shard_pruning",
    scope: KnobScope::Coordinator,
    kind: KnobKind::Bool,
    description: "Skip shards whose placement leaf rules out the request's filter before \
                  fan-out (--shard-pruning, docs/placement.md).",
};

/// Live values of a node's knobs, taken once from its config.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct NodeKnobValues {
    pub share_floors: bool,
    pub segment_pruning: bool,
    pub floor_delta: f32,
    pub floor_warmup_chunks: u32,
    pub floor_min_interval_ms: u64,
}

/// Live values of a coordinator's knobs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoordinatorKnobValues {
    pub max_k: u32,
    /// 0 means no hedging.
    pub hedge_delay_ms: u64,
    pub shard_pruning: bool,
    /// Candidate ids per rescore call; at least 1.
    pub signal_batch: u32,
}

enum Live {
    Node {
        share_floors: AtomicBool,
        segment_pruning: AtomicBool,
        floor_delta: AtomicU32,
        floor_warmup_chunks: AtomicU32,
        floor_min_interval_ms: AtomicU64,
        startup: NodeKnobValues,
    },
    Coordinator {
        max_k: AtomicU32,
        hedge_delay_ms: AtomicU64,
        shard_pruning: AtomicBool,
        signal_batch: AtomicU32,
        startup: CoordinatorKnobValues,
    },
}

/// A process's runtime knobs: the mutable ones as atomics, the rest as
/// the text they were configured with.
pub struct Knobs {
    process: String,
    live: Live,
    fixed: std::sync::RwLock<Vec<RuntimeKnob>>,
}

impl std::fmt::Debug for Knobs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Knobs")
            .field("process", &self.process)
            .field("knobs", &self.list().knobs.len())
            .finish()
    }
}

/// One immutable setting to list beside the live ones.
pub struct FixedKnob {
    pub name: &'static str,
    pub kind: KnobKind,
    pub value: String,
    pub description: &'static str,
}

fn fixed_entry(scope: KnobScope, fixed: FixedKnob) -> RuntimeKnob {
    RuntimeKnob {
        name: fixed.name.to_string(),
        scope: scope as i32,
        kind: fixed.kind as i32,
        value: fixed.value.clone(),
        startup_value: fixed.value,
        mutable: false,
        description: fixed.description.to_string(),
    }
}

impl Knobs {
    /// A node's knobs from its construction-time values.
    pub fn node(process: impl Into<String>, values: NodeKnobValues, fixed: Vec<FixedKnob>) -> Self {
        Knobs {
            process: process.into(),
            live: Live::Node {
                share_floors: AtomicBool::new(values.share_floors),
                segment_pruning: AtomicBool::new(values.segment_pruning),
                floor_delta: AtomicU32::new(values.floor_delta.to_bits()),
                floor_warmup_chunks: AtomicU32::new(values.floor_warmup_chunks),
                floor_min_interval_ms: AtomicU64::new(values.floor_min_interval_ms),
                startup: values,
            },
            fixed: std::sync::RwLock::new(
                fixed
                    .into_iter()
                    .map(|f| fixed_entry(KnobScope::Node, f))
                    .collect(),
            ),
        }
    }

    /// A coordinator's knobs.
    pub fn coordinator(
        process: impl Into<String>,
        values: CoordinatorKnobValues,
        fixed: Vec<FixedKnob>,
    ) -> Self {
        Knobs {
            process: process.into(),
            live: Live::Coordinator {
                max_k: AtomicU32::new(values.max_k),
                hedge_delay_ms: AtomicU64::new(values.hedge_delay_ms),
                shard_pruning: AtomicBool::new(values.shard_pruning),
                signal_batch: AtomicU32::new(values.signal_batch),
                startup: values,
            },
            fixed: std::sync::RwLock::new(
                fixed
                    .into_iter()
                    .map(|f| fixed_entry(KnobScope::Coordinator, f))
                    .collect(),
            ),
        }
    }

    /// Replace the read-at-startup list (a coordinator's builders call
    /// this as they settle its configuration).
    pub fn set_fixed(&self, fixed: Vec<FixedKnob>) {
        let scope = match &self.live {
            Live::Node { .. } => KnobScope::Node,
            Live::Coordinator { .. } => KnobScope::Coordinator,
        };
        *self.fixed.write().expect("knob list lock poisoned") =
            fixed.into_iter().map(|f| fixed_entry(scope, f)).collect();
    }

    pub fn process(&self) -> &str {
        &self.process
    }

    // -- node reads -------------------------------------------------------

    pub fn share_floors(&self) -> bool {
        match &self.live {
            Live::Node { share_floors, .. } => share_floors.load(Ordering::Relaxed),
            Live::Coordinator { .. } => true,
        }
    }

    pub fn segment_pruning(&self) -> bool {
        match &self.live {
            Live::Node {
                segment_pruning, ..
            } => segment_pruning.load(Ordering::Relaxed),
            Live::Coordinator { .. } => true,
        }
    }

    /// Coordinator: whether a filter skips shards from their placement
    /// leaf (docs/placement.md). A node answers `true`; it holds no
    /// topology.
    pub fn shard_pruning(&self) -> bool {
        match &self.live {
            Live::Coordinator { shard_pruning, .. } => shard_pruning.load(Ordering::Relaxed),
            Live::Node { .. } => true,
        }
    }

    pub fn floor_delta(&self) -> f32 {
        match &self.live {
            Live::Node { floor_delta, .. } => f32::from_bits(floor_delta.load(Ordering::Relaxed)),
            Live::Coordinator { .. } => 0.0,
        }
    }

    pub fn floor_warmup_chunks(&self) -> u32 {
        match &self.live {
            Live::Node {
                floor_warmup_chunks,
                ..
            } => floor_warmup_chunks.load(Ordering::Relaxed),
            Live::Coordinator { .. } => 0,
        }
    }

    pub fn floor_min_interval_ms(&self) -> u64 {
        match &self.live {
            Live::Node {
                floor_min_interval_ms,
                ..
            } => floor_min_interval_ms.load(Ordering::Relaxed),
            Live::Coordinator { .. } => 0,
        }
    }

    // -- coordinator reads ------------------------------------------------

    pub fn max_k(&self) -> u32 {
        match &self.live {
            Live::Coordinator { max_k, .. } => max_k.load(Ordering::Relaxed),
            Live::Node { .. } => 0,
        }
    }

    /// Candidate ids per rescore call (coordinator only; 0 on a node,
    /// which has no such knob).
    pub fn signal_batch(&self) -> u32 {
        match &self.live {
            Live::Coordinator { signal_batch, .. } => signal_batch.load(Ordering::Relaxed),
            Live::Node { .. } => 0,
        }
    }

    pub fn hedge_delay(&self) -> Option<Duration> {
        match &self.live {
            Live::Coordinator { hedge_delay_ms, .. } => {
                let ms = hedge_delay_ms.load(Ordering::Relaxed);
                (ms > 0).then(|| Duration::from_millis(ms))
            }
            Live::Node { .. } => None,
        }
    }

    // -- listing and setting ----------------------------------------------

    fn live_entries(&self) -> Vec<RuntimeKnob> {
        fn entry(spec: &KnobSpec, value: String, startup: String) -> RuntimeKnob {
            RuntimeKnob {
                name: spec.name.to_string(),
                scope: spec.scope as i32,
                kind: spec.kind as i32,
                value,
                startup_value: startup,
                mutable: true,
                description: spec.description.to_string(),
            }
        }
        match &self.live {
            Live::Node { startup, .. } => vec![
                entry(
                    &FLOOR_SHARING,
                    self.share_floors().to_string(),
                    startup.share_floors.to_string(),
                ),
                entry(
                    &SEGMENT_PRUNING,
                    self.segment_pruning().to_string(),
                    startup.segment_pruning.to_string(),
                ),
                entry(
                    &FLOOR_DELTA,
                    self.floor_delta().to_string(),
                    startup.floor_delta.to_string(),
                ),
                entry(
                    &FLOOR_WARMUP_CHUNKS,
                    self.floor_warmup_chunks().to_string(),
                    startup.floor_warmup_chunks.to_string(),
                ),
                entry(
                    &FLOOR_MIN_INTERVAL_MS,
                    self.floor_min_interval_ms().to_string(),
                    startup.floor_min_interval_ms.to_string(),
                ),
            ],
            Live::Coordinator { startup, .. } => vec![
                entry(&MAX_K, self.max_k().to_string(), startup.max_k.to_string()),
                entry(
                    &HEDGE_DELAY_MS,
                    self.hedge_delay()
                        .map_or(0, |d| d.as_millis() as u64)
                        .to_string(),
                    startup.hedge_delay_ms.to_string(),
                ),
                entry(
                    &SHARD_PRUNING,
                    self.shard_pruning().to_string(),
                    startup.shard_pruning.to_string(),
                ),
                entry(
                    &SIGNAL_BATCH,
                    self.signal_batch().to_string(),
                    startup.signal_batch.to_string(),
                ),
            ],
        }
    }

    /// Every knob this process has, live ones first.
    pub fn list(&self) -> RuntimeKnobs {
        let mut knobs = self.live_entries();
        knobs.extend(
            self.fixed
                .read()
                .expect("knob list lock poisoned")
                .iter()
                .cloned(),
        );
        RuntimeKnobs {
            knobs,
            process: self.process.clone(),
        }
    }

    fn known_names(&self) -> Vec<String> {
        self.list().knobs.into_iter().map(|k| k.name).collect()
    }

    /// Set one live knob. Unknown names, immutable knobs, and values that
    /// do not parse for the knob's kind are rejected by name.
    pub fn set(&self, name: &str, value: &str) -> Result<(), Status> {
        let value = value.trim();
        fn parse_bool(name: &str, value: &str) -> Result<bool, Status> {
            match value {
                "true" | "on" | "1" => Ok(true),
                "false" | "off" | "0" => Ok(false),
                other => Err(Status::invalid_argument(format!(
                    "knob {name:?}: {other:?} is not a boolean (true/false)"
                ))),
            }
        }
        fn parse_u32(name: &str, value: &str) -> Result<u32, Status> {
            value.parse::<u32>().map_err(|e| {
                Status::invalid_argument(format!("knob {name:?}: {value:?} is not a u32 ({e})"))
            })
        }
        fn parse_u64(name: &str, value: &str) -> Result<u64, Status> {
            value.parse::<u64>().map_err(|e| {
                Status::invalid_argument(format!("knob {name:?}: {value:?} is not a u64 ({e})"))
            })
        }
        match &self.live {
            Live::Node {
                share_floors,
                segment_pruning,
                floor_delta,
                floor_warmup_chunks,
                floor_min_interval_ms,
                ..
            } => match name {
                "floor_sharing" => {
                    share_floors.store(parse_bool(name, value)?, Ordering::Relaxed);
                    Ok(())
                }
                "segment_pruning" => {
                    segment_pruning.store(parse_bool(name, value)?, Ordering::Relaxed);
                    Ok(())
                }
                "floor_delta" => {
                    let parsed = value.parse::<f32>().map_err(|e| {
                        Status::invalid_argument(format!(
                            "knob {name:?}: {value:?} is not a float ({e})"
                        ))
                    })?;
                    if !parsed.is_finite() || parsed < 0.0 {
                        return Err(Status::invalid_argument(format!(
                            "knob {name:?}: {parsed} must be finite and not negative"
                        )));
                    }
                    floor_delta.store(parsed.to_bits(), Ordering::Relaxed);
                    Ok(())
                }
                "floor_warmup_chunks" => {
                    floor_warmup_chunks.store(parse_u32(name, value)?, Ordering::Relaxed);
                    Ok(())
                }
                "floor_min_interval_ms" => {
                    floor_min_interval_ms.store(parse_u64(name, value)?, Ordering::Relaxed);
                    Ok(())
                }
                other => self.reject(other),
            },
            Live::Coordinator {
                max_k,
                hedge_delay_ms,
                shard_pruning,
                signal_batch,
                ..
            } => match name {
                "max_k" => {
                    let parsed = parse_u32(name, value)?;
                    if parsed == 0 {
                        return Err(Status::invalid_argument(
                            "knob \"max_k\": 0 is not a cap; give the largest k to admit",
                        ));
                    }
                    max_k.store(parsed, Ordering::Relaxed);
                    Ok(())
                }
                "hedge_delay_ms" => {
                    hedge_delay_ms.store(parse_u64(name, value)?, Ordering::Relaxed);
                    Ok(())
                }
                "shard_pruning" => {
                    shard_pruning.store(parse_bool(name, value)?, Ordering::Relaxed);
                    Ok(())
                }
                "signal_batch" => {
                    let parsed = parse_u32(name, value)?;
                    if parsed == 0 {
                        return Err(Status::invalid_argument(
                            "knob \"signal_batch\": 0 is not a batch; give the ids per call",
                        ));
                    }
                    signal_batch.store(parsed, Ordering::Relaxed);
                    Ok(())
                }
                other => self.reject(other),
            },
        }
    }

    fn reject(&self, name: &str) -> Result<(), Status> {
        if self
            .fixed
            .read()
            .expect("knob list lock poisoned")
            .iter()
            .any(|k| k.name == name)
        {
            return Err(Status::failed_precondition(format!(
                "knob {name:?} is read at startup only; restart the process to change it"
            )));
        }
        Err(Status::invalid_argument(format!(
            "knob {name:?} is not one this process has; known: {}",
            self.known_names().join(", ")
        )))
    }
}

// ---------------------------------------------------------------------
// The recent-request ring
// ---------------------------------------------------------------------

/// The coordinator's bounded ring of recent public requests.
#[derive(Debug, Default)]
pub struct RecentRing {
    entries: Mutex<VecDeque<RecentQuery>>,
    total_seen: AtomicU64,
}

impl RecentRing {
    pub fn push(&self, query: RecentQuery) {
        self.total_seen.fetch_add(1, Ordering::Relaxed);
        let mut entries = self.entries.lock().expect("recent ring mutex poisoned");
        if entries.len() == RECENT_RING {
            entries.pop_front();
        }
        entries.push_back(query);
    }

    /// The newest `limit` entries, newest first.
    pub fn recent(&self, limit: usize) -> Vec<RecentQuery> {
        let entries = self.entries.lock().expect("recent ring mutex poisoned");
        entries.iter().rev().take(limit).cloned().collect()
    }

    pub fn total_seen(&self) -> u64 {
        self.total_seen.load(Ordering::Relaxed)
    }
}

/// Milliseconds since the Unix epoch, for ring entries and snapshots.
pub fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Figures a public response contributes to its ring entry.
#[derive(Debug, Default, Clone)]
pub struct RecentFigures {
    pub hits: u32,
    pub segments_total: u32,
    pub segments_skipped: u32,
    pub candidates_collected: u64,
    pub executed: String,
}

// ---------------------------------------------------------------------
// Service implementations
// ---------------------------------------------------------------------

type SnapshotStream = Pin<Box<dyn Stream<Item = Result<MetricsSnapshot, Status>> + Send>>;

fn stream_interval(request: &StreamMetricsRequest) -> Result<Duration, Status> {
    let ms = if request.interval_ms == 0 {
        STREAM_DEFAULT_MS
    } else {
        request.interval_ms
    };
    if ms < STREAM_MIN_MS {
        return Err(Status::invalid_argument(format!(
            "StreamMetrics: interval_ms={ms} is under the {STREAM_MIN_MS} ms minimum"
        )));
    }
    Ok(Duration::from_millis(u64::from(ms)))
}

/// A stream of snapshots every `interval` until the receiver hangs up.
fn snapshot_stream(
    interval: Duration,
    snapshot: impl Fn() -> MetricsSnapshot + Send + 'static,
) -> SnapshotStream {
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = tx.closed() => break,
                _ = ticker.tick() => {},
            }
            if tx.send(Ok(snapshot())).await.is_err() {
                break;
            }
        }
    });
    Box::pin(ReceiverStream::new(rx))
}

fn check_access(permits: &[AccessPermit]) -> Result<(), Status> {
    for permit in permits {
        permit.check()?;
    }
    Ok(())
}

/// The service on a shard node.
#[derive(Clone)]
pub struct NodeDiagnostics {
    node: crate::node::NodeServiceImpl,
}

impl NodeDiagnostics {
    pub fn new(node: crate::node::NodeServiceImpl) -> Self {
        NodeDiagnostics { node }
    }

    pub fn into_server(self, max_message_bytes: usize) -> DiagnosticsServiceServer<Self> {
        DiagnosticsServiceServer::new(self)
            .max_decoding_message_size(max_message_bytes)
            .max_encoding_message_size(max_message_bytes)
    }

    fn snapshot(&self) -> MetricsSnapshot {
        crate::metrics::snapshot(self.node.knobs().process(), &[self.node.metrics_provider()])
    }
}

#[tonic::async_trait]
impl DiagnosticsService for NodeDiagnostics {
    type StreamMetricsStream = crate::metrics::Timed<SnapshotStream>;

    async fn get_runtime_knobs(
        &self,
        request: Request<GetRuntimeKnobsRequest>,
    ) -> Result<Response<RuntimeKnobs>, Status> {
        crate::metrics::timed(Route::GetRuntimeKnobs, request, |_| async move {
            Ok(Response::new(self.node.knobs().list()))
        })
        .await
    }

    async fn set_runtime_knob(
        &self,
        request: Request<SetRuntimeKnobRequest>,
    ) -> Result<Response<RuntimeKnobs>, Status> {
        crate::metrics::timed(Route::SetRuntimeKnob, request, |request| async move {
            let req = request.into_inner();
            self.node.knobs().set(&req.name, &req.value)?;
            Ok(Response::new(self.node.knobs().list()))
        })
        .await
    }

    async fn get_metrics_snapshot(
        &self,
        request: Request<MetricsSnapshotRequest>,
    ) -> Result<Response<MetricsSnapshot>, Status> {
        crate::metrics::timed(Route::GetMetricsSnapshot, request, |_| async move {
            Ok(Response::new(self.snapshot()))
        })
        .await
    }

    async fn stream_metrics(
        &self,
        request: Request<StreamMetricsRequest>,
    ) -> Result<Response<Self::StreamMetricsStream>, Status> {
        crate::metrics::timed_stream(Route::StreamMetrics, request, |request| async move {
            let interval = stream_interval(request.get_ref())?;
            let this = self.clone();
            Ok(Response::new(snapshot_stream(interval, move || {
                this.snapshot()
            })))
        })
        .await
    }

    async fn get_shard_diagnostics(
        &self,
        request: Request<ShardDiagnosticsRequest>,
    ) -> Result<Response<ShardDiagnostics>, Status> {
        crate::metrics::timed(Route::GetShardDiagnostics, request, |_| async move {
            Ok(Response::new(ShardDiagnostics {
                process: self.node.knobs().process().to_string(),
                topology_generation: 0,
                shards: vec![self.node.shard_diagnostics(0, String::new())],
            }))
        })
        .await
    }

    async fn recent_queries(
        &self,
        request: Request<RecentQueriesRequest>,
    ) -> Result<Response<RecentQueriesResponse>, Status> {
        crate::metrics::timed(Route::RecentQueries, request, |_| async move {
            Ok(Response::new(RecentQueriesResponse {
                queries: Vec::new(),
                total_seen: 0,
            }))
        })
        .await
    }
}

/// The service on a coordinator: its own knobs, its metrics, the nodes'
/// layouts through fan-out, and the recent-request ring.
#[derive(Clone)]
pub struct CoordinatorDiagnostics {
    members: Vec<(String, crate::coordinator::CoordinatorServiceImpl)>,
    principals: Option<Arc<Principals>>,
    ring: Arc<RecentRing>,
    gauges: Arc<Vec<crate::metrics::GaugeProvider>>,
}

impl CoordinatorDiagnostics {
    /// These routes inspect or change the whole served process. A collection
    /// administrator cannot use them to cross another collection's boundary.
    fn admit<T>(&self, request: &Request<T>) -> Result<Vec<AccessPermit>, Status> {
        let Some(principals) = &self.principals else {
            return Ok(Vec::new());
        };
        let principal = principals.authenticate(request.metadata())?;
        if !principal.admin {
            return Err(Status::permission_denied(format!(
                "principal {:?} is not an admin; the diagnostics service needs admin = true",
                principal.name
            )));
        }
        if self.members.is_empty() {
            return Err(Status::permission_denied("no authorized collections"));
        }
        let permits = self
            .members
            .iter()
            .map(|(name, _)| principals.authorize(&principal, name, crate::pb::AccessAction::Admin))
            .collect::<Result<Vec<_>, _>>()?;
        // Acquiring a later collection may span a policy replacement. No
        // response or mutation may proceed with a mixture of old and new grants.
        check_access(&permits)?;
        Ok(permits)
    }

    /// `members` in collection order, the unnamed one under `""`. Supply the
    /// complete collection set for this process, including collections behind
    /// the ring and gauge providers: metrics and controls are process-wide.
    pub fn new(
        members: Vec<(String, crate::coordinator::CoordinatorServiceImpl)>,
        principals: Option<Arc<Principals>>,
        ring: Arc<RecentRing>,
    ) -> Self {
        CoordinatorDiagnostics {
            members,
            principals,
            ring,
            gauges: Arc::new(Vec::new()),
        }
    }

    /// Shard gauges of nodes served in this process (`Role::Both`), so
    /// the coordinator's snapshot renders the same page the exporter
    /// does.
    pub fn with_gauges(mut self, gauges: Vec<crate::metrics::GaugeProvider>) -> Self {
        self.gauges = Arc::new(gauges);
        self
    }

    pub fn into_server(self, max_message_bytes: usize) -> DiagnosticsServiceServer<Self> {
        DiagnosticsServiceServer::new(self)
            .max_decoding_message_size(max_message_bytes)
            .max_encoding_message_size(max_message_bytes)
    }

    /// The default member: the unnamed one, else the first named one.
    fn primary(&self) -> Result<&crate::coordinator::CoordinatorServiceImpl, Status> {
        self.members
            .iter()
            .find(|(name, _)| name.is_empty())
            .or_else(|| self.members.first())
            .map(|(_, member)| member)
            .ok_or_else(|| Status::failed_precondition("this coordinator serves no collection"))
    }

    fn snapshot(&self) -> MetricsSnapshot {
        let process = self
            .primary()
            .map(|m| m.knobs().process().to_string())
            .unwrap_or_default();
        crate::metrics::snapshot(&process, &self.gauges)
    }
}

#[tonic::async_trait]
impl DiagnosticsService for CoordinatorDiagnostics {
    type StreamMetricsStream = crate::metrics::Timed<SnapshotStream>;

    async fn get_runtime_knobs(
        &self,
        request: Request<GetRuntimeKnobsRequest>,
    ) -> Result<Response<RuntimeKnobs>, Status> {
        crate::metrics::timed(Route::GetRuntimeKnobs, request, |request| async move {
            let access = self.admit(&request)?;
            let knobs = self.primary()?.knobs().list();
            check_access(&access)?;
            Ok(Response::new(knobs))
        })
        .await
    }

    async fn set_runtime_knob(
        &self,
        request: Request<SetRuntimeKnobRequest>,
    ) -> Result<Response<RuntimeKnobs>, Status> {
        crate::metrics::timed(Route::SetRuntimeKnob, request, |request| async move {
            let access = self.admit(&request)?;
            let req = request.into_inner();
            // Every collection's coordinator shares the process's caps.
            for (_, member) in &self.members {
                check_access(&access)?;
                let result = member.knobs().set(&req.name, &req.value);
                check_access(&access)?;
                result?;
            }
            let knobs = self.primary()?.knobs().list();
            check_access(&access)?;
            Ok(Response::new(knobs))
        })
        .await
    }

    async fn get_metrics_snapshot(
        &self,
        request: Request<MetricsSnapshotRequest>,
    ) -> Result<Response<MetricsSnapshot>, Status> {
        crate::metrics::timed(Route::GetMetricsSnapshot, request, |request| async move {
            let access = self.admit(&request)?;
            let snapshot = self.snapshot();
            check_access(&access)?;
            Ok(Response::new(snapshot))
        })
        .await
    }

    async fn stream_metrics(
        &self,
        request: Request<StreamMetricsRequest>,
    ) -> Result<Response<Self::StreamMetricsStream>, Status> {
        crate::metrics::timed_stream(Route::StreamMetrics, request, |request| async move {
            let access = self.admit(&request)?;
            let interval = stream_interval(request.get_ref())?;
            let this = self.clone();
            let stream = snapshot_stream(interval, move || this.snapshot());
            check_access(&access)?;
            let stream: SnapshotStream = Box::pin(AuthorizedStream::with_permits(stream, access));
            Ok(Response::new(stream))
        })
        .await
    }

    async fn get_shard_diagnostics(
        &self,
        request: Request<ShardDiagnosticsRequest>,
    ) -> Result<Response<ShardDiagnostics>, Status> {
        crate::metrics::timed(Route::GetShardDiagnostics, request, |request| async move {
            let access = self.admit(&request)?;
            let only = request.get_ref().shard;
            let primary = self.primary()?;
            let mut shards = Vec::new();
            for (_, member) in &self.members {
                check_access(&access)?;
                shards.extend(member.shard_diagnostics(only).await);
            }
            check_access(&access)?;
            Ok(Response::new(ShardDiagnostics {
                process: primary.knobs().process().to_string(),
                topology_generation: primary.current_topology_generation(),
                shards,
            }))
        })
        .await
    }

    async fn recent_queries(
        &self,
        request: Request<RecentQueriesRequest>,
    ) -> Result<Response<RecentQueriesResponse>, Status> {
        crate::metrics::timed(Route::RecentQueries, request, |request| async move {
            let access = self.admit(&request)?;
            let limit = match request.get_ref().limit {
                0 => RECENT_DEFAULT,
                n => (n as usize).min(RECENT_RING),
            };
            let recent = RecentQueriesResponse {
                queries: self.ring.recent(limit),
                total_seen: self.ring.total_seen(),
            };
            check_access(&access)?;
            Ok(Response::new(recent))
        })
        .await
    }
}

/// `ShardLayoutDiagnostics` for a node that does not serve the service.
pub fn unserved_layout(shard: u32, address: String, note: &str) -> ShardLayoutDiagnostics {
    ShardLayoutDiagnostics {
        shard,
        address,
        layout: note.to_string(),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn closing_an_idle_snapshot_stream_drops_its_producer_immediately() {
        struct Released(Option<tokio::sync::oneshot::Sender<()>>);
        impl Drop for Released {
            fn drop(&mut self) {
                let _ = self.0.take().unwrap().send(());
            }
        }
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let released = Released(Some(sender));
        let mut stream = snapshot_stream(Duration::from_secs(60), move || {
            let _ = &released;
            MetricsSnapshot::default()
        });
        use tokio_stream::StreamExt;
        stream.next().await.unwrap().unwrap();
        drop(stream);
        tokio::time::timeout(Duration::from_secs(1), receiver)
            .await
            .expect("dropping a revoked stream must release the producer before its next tick")
            .unwrap();
    }

    fn node_knobs() -> Knobs {
        Knobs::node(
            "node-test",
            NodeKnobValues {
                share_floors: true,
                segment_pruning: true,
                floor_delta: 0.0,
                floor_warmup_chunks: 0,
                floor_min_interval_ms: 0,
            },
            vec![FixedKnob {
                name: "chunk_blocks",
                kind: KnobKind::Int,
                value: "8192".to_string(),
                description: "SIMD blocks per scan chunk.",
            }],
        )
    }

    #[test]
    fn a_live_knob_flips_and_lists_its_startup_value() {
        let knobs = node_knobs();
        knobs.set("segment_pruning", "false").unwrap();
        assert!(!knobs.segment_pruning());
        let listed = knobs.list();
        let entry = listed
            .knobs
            .iter()
            .find(|k| k.name == "segment_pruning")
            .unwrap();
        assert_eq!(entry.value, "false");
        assert_eq!(entry.startup_value, "true");
        assert!(entry.mutable);
        knobs.set("floor_delta", "0.25").unwrap();
        assert_eq!(knobs.floor_delta(), 0.25);
        knobs.set("floor_min_interval_ms", "40").unwrap();
        assert_eq!(knobs.floor_min_interval_ms(), 40);
    }

    #[test]
    fn immutable_unknown_and_malformed_are_rejected_by_name() {
        let knobs = node_knobs();
        let fixed = knobs.set("chunk_blocks", "4096").unwrap_err();
        assert_eq!(fixed.code(), tonic::Code::FailedPrecondition);
        assert!(fixed.message().contains("chunk_blocks"));
        let unknown = knobs.set("max_k", "5").unwrap_err();
        assert_eq!(unknown.code(), tonic::Code::InvalidArgument);
        assert!(unknown.message().contains("segment_pruning"));
        let bad = knobs.set("segment_pruning", "maybe").unwrap_err();
        assert_eq!(bad.code(), tonic::Code::InvalidArgument);
        assert!(knobs.segment_pruning());
        let negative = knobs.set("floor_delta", "-1").unwrap_err();
        assert_eq!(negative.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn coordinator_knobs_cap_k_and_hedge() {
        let knobs = Knobs::coordinator(
            "coord-test",
            CoordinatorKnobValues {
                max_k: 100,
                hedge_delay_ms: 0,
                shard_pruning: true,
                signal_batch: 10_000,
            },
            Vec::new(),
        );
        assert_eq!(knobs.max_k(), 100);
        assert!(knobs.hedge_delay().is_none());
        knobs.set("max_k", "250").unwrap();
        knobs.set("hedge_delay_ms", "30").unwrap();
        assert_eq!(knobs.max_k(), 250);
        assert_eq!(knobs.hedge_delay(), Some(Duration::from_millis(30)));
        assert_eq!(
            knobs.set("max_k", "0").unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            knobs.set("segment_pruning", "false").unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }

    #[test]
    fn the_ring_is_bounded_and_newest_first() {
        let ring = RecentRing::default();
        for i in 0..(RECENT_RING as u64 + 10) {
            ring.push(RecentQuery {
                unix_ms: i,
                ..Default::default()
            });
        }
        assert_eq!(ring.total_seen(), RECENT_RING as u64 + 10);
        let recent = ring.recent(3);
        let stamps: Vec<u64> = recent.iter().map(|q| q.unix_ms).collect();
        assert_eq!(
            stamps,
            vec![
                RECENT_RING as u64 + 9,
                RECENT_RING as u64 + 8,
                RECENT_RING as u64 + 7
            ]
        );
        assert_eq!(ring.recent(1000).len(), RECENT_RING);
        assert_eq!(ring.recent(1000).last().unwrap().unix_ms, 10);
    }
}

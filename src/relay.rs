//! Relay coordinators (`docs/relay-coordinators.md`): a coordinator
//! process that presents itself to its parent as ONE shard.
//!
//! A relay serves the node-facing surface over its children (ordinary
//! shard nodes, or further relays) so a root coordinator fanning out to
//! a few hundred targets can stand over thousands of shards with a level
//! in between. Nothing new is spoken between levels: the parent calls
//! `NodeService` routes on the relay exactly as it calls them on a node,
//! and the relay calls the same routes on its children through the
//! coordinator's fan-out machinery.
//!
//! The scope here is the restricted, read-only surface the 2026-09-05
//! review cleared (`docs/scale-out-coordination-review-2026-09-05.md`):
//!
//! - `StreamSearch`: every qualifying child candidate is forwarded upward
//!   on one stream, batch bytes untouched (global ids, original scores,
//!   boundary ties preserved); the parent's monotone floor and its
//!   cancellation reach every child on the relay's own signed UDP
//!   sessions plus the gRPC twin; the relay's completion frame is issued
//!   only after every child's; a child error or a missing terminal
//!   certificate fails the attempt. The relay keeps no heap
//!   (`StartStreamSearch` carries no k) and raises no floor of its own.
//! - `TermStats`: children's shares summed with checked arithmetic (a
//!   `u32` document frequency that would overflow refuses by name),
//!   homogeneous field capabilities required, and the epoch reported as
//!   a relay token bound to the children's epochs (see [`TokenRegistry`]).
//! - `Health`: the children's reports merged, served only when the
//!   children's slot ranges are contiguous so the range the parent
//!   derives is real. A relay has no WAL and reports none.
//! - The keyword leg (`Bm25Query`, `Bm25PhraseQuery`, `Bm25QueryStream`,
//!   `Bm25Rescore`): the root's global statistics travel to each child
//!   unchanged, the parent's epoch claim (a relay token) is translated
//!   into each child's recorded claim, candidates and cutoffs pass
//!   untouched, and the children's terminal responses merge by value
//!   with checked arithmetic. Column statistics and exact cardinalities
//!   are refused by name (a fold whose order the root pins, and a union
//!   of values, are not this level's to compute). `Bm25Rescore` routes
//!   each candidate id to the child whose slot range holds it.
//! - `SearchShard` (the unary scan the cascade gates on): one child
//!   stream per child, the parent's floor raises forwarded down and each
//!   child's up, the children's terminal lists concatenated in score
//!   order so the parent's merge and the cascade's score-defined pool
//!   see the same union they see over leaves.
//! - `VectorRescore` and `ExactVectorRescore`: each id routed to the
//!   child whose slot range holds it, an id in no child's range dropped
//!   as a node drops one outside its own range, hits merged in the order
//!   a node answers in, byte and page counts summed with a check.
//! - The bitmap routes (`ResolveFilterBitmap`, `ResolveLexicalBitmap`,
//!   `ResolveVectorBitmap`): the children's bitmaps laid over the relay's
//!   one contiguous slot range ([`concat_bitmaps`]), a gap refused by
//!   name, known-column flags merged through each child's implied
//!   leaves, and the lexical epoch reported as a relay token.
//! - The dictionaries (`ExpandTermPrefix`, `SuggestTerms`): the union of
//!   the children's terms in byte order with df summed, a child past the
//!   cap or the scan bound making the relay answer as a node past it.
//!
//! The relay also serves `DiagnosticsService` on the same port
//! ([`RelayDiagnostics`]): the root asks each shard's address for its
//! layout, and the relay answers with its children's merged into one.
//!
//! Every other `NodeService` route refuses UNIMPLEMENTED naming the route
//! and the relay: no ingest, no administration, no aggregation, no
//! per-shard fusion, and no follow-up fetches by id through this level.
//!
//! The relay never reads its shard map from a file or from the
//! coordinator's authority directly. It consumes a [`MapSource`]: one
//! reading at a time, each stamped with the control revision and the
//! topology generation it came from, plus a change notification. Every
//! decision (the child set of a stream or a statistics call, the range
//! `Health` reports, the tuple behind an epoch token) pins the revision
//! it was made under and refuses by name when that revision is no longer
//! the current one. Today the source is the coordinator's file-polled
//! map with its generation as the revision; a replicated control state
//! implements the same interface behind the relay without touching it.

use crate::stats_identity::{StatsClaim, StatsIncarnation};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{mpsc, watch, Notify};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::coordinator::{CoordinatorServiceImpl, RequestFilters, StreamFanout, TopologyRoute};
use crate::metrics::Route;
use crate::node::STALE_STATS_EPOCH;
use crate::pb::node_service_server::{NodeService, NodeServiceServer};
use crate::pb::{
    bm25_query_stream_request, bm25_query_stream_response, search_shard_request,
    search_shard_response, stream_search_request, stream_search_response, Bm25PhraseQueryRequest,
    Bm25QueryRequest, Bm25QueryResponse, Bm25QueryStreamRequest, Bm25QueryStreamResponse,
    Bm25RescoreRequest, Bm25RescoreResponse, Bm25StreamCompletion, FloorUpdate, HealthRequest,
    HealthResponse, SearchShardRequest, SearchShardResponse, ShardLegsRequest, ShardLegsResponse,
    StopBm25Query, StreamSearchRequest, StreamSearchResponse, StreamSearchSummary,
    TermStatsRequest, TermStatsResponse,
};

/// Relay tokens retained per relay; the oldest is forgotten first. A
/// parent holding an older token gets the stale-epoch refusal and
/// refetches, which is the same thing it does when a child's epoch moves.
pub const RETAINED_TOKENS: usize = 256;

/// The relay's parent-facing stream signals: the parent's floor raises
/// (gRPC or UDP) fold into one watch cell, its cancellation into one
/// flag, and the forwarding loop wakes on either.
struct RelaySignals {
    floor: watch::Sender<f32>,
    cancelled: AtomicBool,
    cancel: Notify,
    /// The newest signed sequence applied; a datagram at or behind it
    /// is a replay and is ignored.
    last_seq: AtomicU32,
}

impl RelaySignals {
    fn new(initial_floor: f32) -> Self {
        let (floor, _) = watch::channel(initial_floor);
        Self {
            floor,
            cancelled: AtomicBool::new(false),
            cancel: Notify::new(),
            last_seq: AtomicU32::new(0),
        }
    }

    /// Monotone: a raise below the current floor is ignored, NaN too.
    fn raise(&self, floor: f32) {
        if floor.is_nan() {
            return;
        }
        self.floor.send_if_modified(|current| {
            if floor > *current {
                *current = floor;
                true
            } else {
                false
            }
        });
    }

    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        self.cancel.notify_one();
    }

    fn accept_seq(&self, seq: u32) -> bool {
        self.last_seq
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |last| {
                (seq > last).then_some(seq)
            })
            .is_ok()
    }
}

/// What one relay token stands for: the exact children and the epoch
/// each reported when the token was allocated. The parent's claim on
/// the token translates into one claim per child, which the child
/// enforces itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenTuple {
    pub collection: String,
    /// The map revision the children were read under.
    pub control_revision: u64,
    pub topology_generation: u64,
    pub children: Vec<String>,
    pub epochs: Vec<StatsClaim>,
}

/// Relay tokens (`docs/relay-coordinators.md`, "The epoch token"): a
/// nonzero allocation bound to a [`TokenTuple`], reused while the tuple
/// repeats so a parent's stats cache keeps hitting, replaced the moment
/// any child's lifetime or epoch moves, and retained for [`RETAINED_TOKENS`]
/// tuples. The relay's separate 32-byte identity fences a restart; the legacy
/// numeric clock prefix below is not sufficient to identify a lifetime.
pub struct TokenRegistry {
    incarnation: u64,
    counter: u32,
    entries: VecDeque<(u64, TokenTuple)>,
}

impl TokenRegistry {
    fn new(incarnation: u64) -> Self {
        Self {
            incarnation,
            counter: 0,
            entries: VecDeque::new(),
        }
    }

    fn allocate(&mut self, tuple: TokenTuple) -> Result<u64, Status> {
        if let Some(position) = self.entries.iter().position(|(_, t)| *t == tuple) {
            let entry = self.entries.remove(position).expect("position from iter");
            let token = entry.0;
            self.entries.push_back(entry);
            return Ok(token);
        }
        self.counter = self
            .counter
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("relay statistics token space exhausted"))?;
        let token = (self.incarnation << 32) | u64::from(self.counter);
        self.entries.push_back((token, tuple));
        while self.entries.len() > RETAINED_TOKENS {
            self.entries.pop_front();
        }
        Ok(token)
    }

    fn lookup(&self, token: u64) -> Option<&TokenTuple> {
        self.entries
            .iter()
            .rev()
            .find(|(t, _)| *t == token)
            .map(|(_, tuple)| tuple)
    }
}

/// One immutable reading of a relay's shard map: the children in shard
/// order with their placement codes, and the placement tree of that
/// generation when the map has one.
#[derive(Debug, Clone)]
pub struct RelayMap {
    pub routes: Vec<TopologyRoute>,
    pub placement: Option<Arc<crate::placement::Placement>>,
}

impl RelayMap {
    /// Child addresses in shard order.
    pub fn children(&self) -> Vec<String> {
        self.routes.iter().map(|route| route.addr.clone()).collect()
    }
}

/// A map reading stamped with where it came from. `control_revision` is
/// the number a decision pins; `topology_generation` is what the shards
/// and the parent see on the wire. Under the file-polled source the two
/// are the same number.
#[derive(Debug, Clone)]
pub struct MapSnapshot {
    pub control_revision: u64,
    pub topology_generation: u64,
    pub map: Arc<RelayMap>,
}

/// Where a relay's shard map comes from (`docs/relay-coordinators.md`,
/// "Map interface"). Minimal on purpose: the current reading, and a
/// receiver whose value changes whenever the current revision does.
pub trait MapSource: Send + Sync {
    /// The current reading. Cheap; called once per decision.
    fn current(&self) -> MapSnapshot;
    /// A receiver that changes whenever the current revision changes;
    /// its value is the new control revision.
    fn changes(&self) -> watch::Receiver<u64>;
}

/// The file-polled map as a [`MapSource`]: the coordinator's live
/// topology (or its static shard set), with the topology generation as
/// the control revision and the coordinator's publication watch as the
/// change notification. Every reading is one frozen snapshot, so the
/// children, their codes, and the generation come from one publication.
pub struct CoordinatorMapSource {
    coordinator: Arc<CoordinatorServiceImpl>,
}

impl CoordinatorMapSource {
    pub fn new(coordinator: Arc<CoordinatorServiceImpl>) -> Self {
        Self { coordinator }
    }
}

impl MapSource for CoordinatorMapSource {
    fn current(&self) -> MapSnapshot {
        let frozen = self
            .coordinator
            .request_snapshot()
            .unwrap_or_else(|| self.coordinator.as_ref().clone());
        let generation = frozen.current_topology_generation();
        MapSnapshot {
            control_revision: generation,
            topology_generation: generation,
            map: Arc::new(RelayMap {
                routes: frozen.current_topology_routes(),
                placement: frozen.current_placement(),
            }),
        }
    }

    fn changes(&self) -> watch::Receiver<u64> {
        self.coordinator.topology_changes()
    }
}

/// What a relay holds: the base coordinator (links, keys, limits), the
/// map source, the token registry, the parent-facing signal registry,
/// and the identity the parent sees.
pub struct RelayInner {
    base: Arc<CoordinatorServiceImpl>,
    map: Arc<dyn MapSource>,
    collection: String,
    tokens: Mutex<TokenRegistry>,
    stats_incarnation: StatsIncarnation,
    signals: Arc<Mutex<HashMap<u64, Arc<RelaySignals>>>>,
}

impl RelayInner {
    /// The current map reading and a coordinator frozen over exactly
    /// its children: the pair every decision starts from.
    fn pin(&self) -> (MapSnapshot, CoordinatorServiceImpl) {
        let snapshot = self.map.current();
        let frozen = self.base.frozen_over(
            snapshot.topology_generation,
            &snapshot.map.routes,
            snapshot.map.placement.clone(),
        );
        (snapshot, frozen)
    }

    /// Refuse by name when the map moved since `pinned` was taken.
    fn still_current(&self, pinned: &MapSnapshot, route: &str) -> Result<(), Status> {
        let now = self.map.current();
        if now.control_revision != pinned.control_revision {
            return Err(Status::failed_precondition(format!(
                "relay: the shard map moved from revision {} (generation {}) to revision {} \
                 (generation {}) during {route}; the answer was made under the older map and \
                 is not served; retry under the current one",
                pinned.control_revision,
                pinned.topology_generation,
                now.control_revision,
                now.topology_generation
            )));
        }
        Ok(())
    }
}

/// A relay coordinator's node-facing service. Cheap to clone: the server
/// takes one clone and a test or the process keeps another to read the
/// token registry or run the startup check.
#[derive(Clone)]
pub struct RelayService {
    inner: Arc<RelayInner>,
}

impl std::ops::Deref for RelayService {
    type Target = RelayInner;

    fn deref(&self) -> &RelayInner {
        &self.inner
    }
}

impl RelayService {
    /// A relay over `coordinator`'s shard set. The coordinator supplies
    /// the child links, the UDP key, the placement mask, and the stream
    /// fan-out; the relay adds the node-facing surface.
    pub fn new(coordinator: Arc<CoordinatorServiceImpl>) -> Self {
        let map = Arc::new(CoordinatorMapSource::new(Arc::clone(&coordinator)));
        Self::with_map(coordinator, map)
    }

    /// A relay whose shard map comes from `map` (a replicated control
    /// state, say) while `base` supplies the child links, the UDP key,
    /// and the fan-out limits.
    pub fn with_map(base: Arc<CoordinatorServiceImpl>, map: Arc<dyn MapSource>) -> Self {
        let collection = base.collection().to_string();
        let incarnation = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(1)
            & 0xffff_ffff;
        RelayService {
            inner: Arc::new(RelayInner {
                base,
                map,
                collection,
                tokens: Mutex::new(TokenRegistry::new(incarnation | 1)),
                stats_incarnation: Default::default(),
                signals: Arc::new(Mutex::new(HashMap::new())),
            }),
        }
    }

    pub fn into_server(self, max_message_bytes: usize) -> NodeServiceServer<Self> {
        NodeServiceServer::new(self)
            .max_decoding_message_size(max_message_bytes)
            .max_encoding_message_size(max_message_bytes)
    }

    /// The coordinator the relay fans out through: the child links, the
    /// UDP key, the limits, and (under the file-polled source) the map.
    pub fn base(&self) -> &Arc<CoordinatorServiceImpl> {
        &self.inner.base
    }

    /// The current map reading.
    pub fn map(&self) -> MapSnapshot {
        self.inner.map.current()
    }

    /// The children of the current map, in shard order.
    pub fn children(&self) -> Vec<String> {
        self.map().map.children()
    }

    /// The tuple a token stands for, or the stale-epoch refusal: unknown
    /// tokens, and tokens issued under a map revision that is no longer
    /// current, both refuse, because a claim on a child set the map no
    /// longer names is not a claim this relay can translate.
    pub fn token_tuple(&self, token: u64) -> Result<TokenTuple, Status> {
        let tuple = {
            let registry = self.tokens.lock().expect("relay token registry poisoned");
            registry.lookup(token).cloned()
        }
        .ok_or_else(|| {
            Status::failed_precondition(format!(
                "{STALE_STATS_EPOCH}: relay token {token} is not one this relay issued and \
                 still retains (a child's statistics moved, the token is older than the \
                 {RETAINED_TOKENS} newest, or the relay restarted); refetch TermStats"
            ))
        })?;
        let now = self.map();
        if now.control_revision != tuple.control_revision {
            return Err(Status::failed_precondition(format!(
                "{STALE_STATS_EPOCH}: relay token {token} was issued under map revision {} and \
                 the map is at revision {} now; refetch TermStats",
                tuple.control_revision, now.control_revision
            )));
        }
        Ok(tuple)
    }

    /// The per-child epoch claims a parent's token stands for, parallel
    /// to the token's children (the current map's, or the token would
    /// have refused). Token 0 is no claim and translates to no claim on
    /// any child, exactly as a node reads it.
    pub fn translate_epoch(&self, token: u64) -> Result<Vec<StatsClaim>, Status> {
        if token == 0 {
            return Ok(vec![StatsClaim::default(); self.children().len()]);
        }
        Ok(self.token_tuple(token)?.epochs)
    }

    fn read_receipt<R: crate::visibility::ScopedReadResponse>(
        &self,
        pinned: &MapSnapshot,
        children: Vec<String>,
        visibility: Option<&crate::pb::DocumentVisibility>,
        expected: Option<&[StatsClaim]>,
        shares: &[R],
    ) -> Result<crate::pb::VectorReadReceipt, Status> {
        let scope = crate::visibility::VisibilityScope::new(visibility)?;
        if shares.len() != children.len() || shares.is_empty() {
            return Err(Status::failed_precondition(
                "relay read receipt needs every child",
            ));
        }
        let mut known = vec![false; scope.column_count()];
        let mut epochs = Vec::with_capacity(shares.len());
        for (child, share) in shares.iter().enumerate() {
            let view = share.read_view();
            scope.validate_echo(view.fingerprint, view.columns_known)?;
            let claim = StatsClaim::required(view.epoch, view.incarnation)?;
            if expected.is_some_and(|claims| {
                claims
                    .get(child)
                    .is_none_or(|expected| expected.epoch != 0 && *expected != claim)
            }) {
                return Err(Status::failed_precondition(
                    "relay child read version changed",
                ));
            }
            for (held, present) in known.iter_mut().zip(view.columns_known) {
                *held |= present;
            }
            epochs.push(claim);
        }
        self.still_current(pinned, "read receipt")?;
        let stats_epoch = self
            .tokens
            .lock()
            .expect("relay token registry poisoned")
            .allocate(TokenTuple {
                collection: self.collection.clone(),
                control_revision: pinned.control_revision,
                topology_generation: pinned.topology_generation,
                children,
                epochs,
            })?;
        Ok(crate::pb::VectorReadReceipt {
            vector_binding: None,
            stats_epoch,
            stats_incarnation: self.stats_incarnation.bytes()?,
            visibility_fingerprint: scope.fingerprint().to_vec(),
            visibility_columns_known: known,
        })
    }

    /// Bind the relay's parent-facing UDP signal lane on `addr` (the
    /// same host:port as its gRPC listener) and fold typed floor and
    /// cancel datagrams into the matching stream. The same acceptance
    /// rule as a node's lane: signed when a key is configured, unsigned
    /// on loopback only.
    #[cfg(feature = "net")]
    pub fn spawn_floor_listener(&self, addr: std::net::SocketAddr) {
        let signals = Arc::clone(&self.signals);
        let key = self.base.udp_key().cloned();
        if key.is_none() && !crate::security::is_loopback(&addr) {
            eprintln!(
                "relay stream-signal UDP lane on {addr} stays closed: no --udp-hmac-key, and \
                 unsigned datagrams are accepted on loopback only; signals ride the gRPC streams"
            );
            return;
        }
        tokio::spawn(async move {
            let socket = match tokio::net::UdpSocket::bind(addr).await {
                Ok(socket) => socket,
                Err(e) => {
                    eprintln!(
                        "relay stream-signal UDP bind {addr}: {e}; signals ride the gRPC streams \
                         only"
                    );
                    return;
                }
            };
            let mut buf = [0u8; 64];
            loop {
                match socket.recv_from(&mut buf).await {
                    Ok((n, _peer)) => apply_datagram(&signals, key.as_ref(), &buf[..n]),
                    Err(_) => continue,
                }
            }
        });
    }

    /// Every child's health under one map reading, in shard order; a
    /// child that does not answer fails the call by name.
    async fn children_health(
        &self,
        frozen: &CoordinatorServiceImpl,
        timeout: Option<Duration>,
    ) -> Result<Vec<HealthResponse>, Status> {
        let children = frozen.node_addresses().to_vec();
        let mut tasks = Vec::with_capacity(children.len());
        for (shard, addr) in children.iter().enumerate() {
            let mut link = frozen.node_client(addr)?;
            let addr = addr.clone();
            tasks.push(tokio::spawn(async move {
                let mut request = Request::new(HealthRequest {});
                if let Some(timeout) = timeout {
                    request.set_timeout(timeout);
                }
                link.health(request)
                    .await
                    .map(|r| r.into_inner())
                    .map_err(|status| {
                        Status::new(
                            status.code(),
                            format!("relay child {shard} ({addr}) health: {}", status.message()),
                        )
                    })
            }));
        }
        let mut out = Vec::with_capacity(children.len());
        for task in tasks {
            out.push(
                task.await
                    .map_err(|e| Status::internal(format!("relay health task: {e}")))??,
            );
        }
        Ok(out)
    }

    /// The startup check and the `Health` body: the children's reports
    /// merged into the one shard the parent sees, refused by name when
    /// the children's slot ranges are not contiguous.
    pub async fn check_children(&self) -> Result<HealthResponse, Status> {
        let (pinned, frozen) = self.pin();
        let reports = self.children_health(&frozen, None).await?;
        let merged = merge_health(&self.collection, frozen.node_addresses(), &reports)?;
        self.still_current(&pinned, "Health")?;
        Ok(merged)
    }
}

/// The relay's `DiagnosticsService` (`docs/diagnostics.md`): the
/// process's own knobs, metrics, and ring as any coordinator's, and one
/// shard layout for the root, the children's merged
/// ([`merge_shard_layouts`]). The root asks each shard's address for
/// its layout, so a relay serves the service on the port the root
/// already talks to.
#[derive(Clone)]
pub struct RelayDiagnostics {
    relay: RelayService,
    process: crate::diagnostics::CoordinatorDiagnostics,
}

impl RelayService {
    /// The diagnostics service over this relay.
    pub fn diagnostics(&self) -> RelayDiagnostics {
        let base: CoordinatorServiceImpl = (**self.base()).clone();
        RelayDiagnostics {
            relay: self.clone(),
            process: crate::diagnostics::CoordinatorDiagnostics::new(
                vec![(String::new(), base)],
                None,
                Arc::new(crate::diagnostics::RecentRing::default()),
            ),
        }
    }

    pub fn diagnostics_server(
        &self,
        max_message_bytes: usize,
    ) -> crate::pb::diagnostics_service_server::DiagnosticsServiceServer<RelayDiagnostics> {
        self.diagnostics().into_server(max_message_bytes)
    }
}

impl RelayDiagnostics {
    pub fn into_server(
        self,
        max_message_bytes: usize,
    ) -> crate::pb::diagnostics_service_server::DiagnosticsServiceServer<Self> {
        crate::pb::diagnostics_service_server::DiagnosticsServiceServer::new(self)
            .max_decoding_message_size(max_message_bytes)
            .max_encoding_message_size(max_message_bytes)
    }
}

#[tonic::async_trait]
impl crate::pb::diagnostics_service_server::DiagnosticsService for RelayDiagnostics {
    type StreamMetricsStream =
        <crate::diagnostics::CoordinatorDiagnostics as crate::pb::diagnostics_service_server::DiagnosticsService>::StreamMetricsStream;

    async fn get_runtime_knobs(
        &self,
        request: Request<crate::pb::GetRuntimeKnobsRequest>,
    ) -> Result<Response<crate::pb::RuntimeKnobs>, Status> {
        self.process.get_runtime_knobs(request).await
    }

    async fn set_runtime_knob(
        &self,
        request: Request<crate::pb::SetRuntimeKnobRequest>,
    ) -> Result<Response<crate::pb::RuntimeKnobs>, Status> {
        self.process.set_runtime_knob(request).await
    }

    async fn get_metrics_snapshot(
        &self,
        request: Request<crate::pb::MetricsSnapshotRequest>,
    ) -> Result<Response<crate::pb::MetricsSnapshot>, Status> {
        self.process.get_metrics_snapshot(request).await
    }

    async fn stream_metrics(
        &self,
        request: Request<crate::pb::StreamMetricsRequest>,
    ) -> Result<Response<Self::StreamMetricsStream>, Status> {
        self.process.stream_metrics(request).await
    }

    async fn get_shard_diagnostics(
        &self,
        request: Request<crate::pb::ShardDiagnosticsRequest>,
    ) -> Result<Response<crate::pb::ShardDiagnostics>, Status> {
        crate::metrics::timed(Route::GetShardDiagnostics, request, |_| async move {
            let map = self.relay.map();
            let children = self.relay.children();
            let layouts = self.relay.base().shard_diagnostics(None).await;
            Ok(Response::new(crate::pb::ShardDiagnostics {
                process: self.relay.base().knobs().process().to_string(),
                topology_generation: map.topology_generation,
                shards: vec![merge_shard_layouts(&children, &layouts)],
            }))
        })
        .await
    }

    async fn recent_queries(
        &self,
        request: Request<crate::pb::RecentQueriesRequest>,
    ) -> Result<Response<crate::pb::RecentQueriesResponse>, Status> {
        self.process.recent_queries(request).await
    }
}

/// Apply one parent-facing datagram to the relay's stream registry.
#[cfg(feature = "net")]
fn apply_datagram(
    signals: &Mutex<HashMap<u64, Arc<RelaySignals>>>,
    key: Option<&crate::security::UdpKey>,
    datagram: &[u8],
) {
    let (signal, seq) = match key {
        Some(key) => match crate::stream_signal::decode_signed(key, datagram) {
            Some((signal, seq)) => (signal, Some(seq)),
            None => return,
        },
        None => match crate::stream_signal::decode(datagram) {
            Some(signal) => (signal, None),
            None => return,
        },
    };
    let token = match signal {
        crate::stream_signal::StreamSignal::RaiseFloor { token, .. }
        | crate::stream_signal::StreamSignal::Cancel { token } => token,
    };
    let state = signals
        .lock()
        .expect("relay stream signal registry poisoned")
        .get(&token)
        .cloned();
    let Some(state) = state else {
        return;
    };
    if let Some(seq) = seq {
        if !state.accept_seq(seq) {
            return;
        }
    }
    match signal {
        crate::stream_signal::StreamSignal::RaiseFloor { floor, .. } => state.raise(floor),
        crate::stream_signal::StreamSignal::Cancel { .. } => state.cancel(),
    }
}

/// The gRPC deadline of a request, from its `grpc-timeout` header.
fn grpc_timeout(metadata: &tonic::metadata::MetadataMap) -> Option<Duration> {
    let text = metadata.get("grpc-timeout")?.to_str().ok()?;
    let (digits, unit) = text.split_at(text.len().checked_sub(1)?);
    let value: u64 = digits.parse().ok()?;
    let duration = match unit {
        "H" => Duration::from_secs(value.checked_mul(3600)?),
        "M" => Duration::from_secs(value.checked_mul(60)?),
        "S" => Duration::from_secs(value),
        "m" => Duration::from_millis(value),
        "u" => Duration::from_micros(value),
        "n" => Duration::from_nanos(value),
        _ => return None,
    };
    Some(duration)
}

fn read_metadata(
    response: &impl crate::visibility::ScopedReadResponse,
) -> crate::pb::VectorReadReceipt {
    let view = response.read_view();
    crate::pb::VectorReadReceipt {
        vector_binding: None,
        stats_epoch: view.epoch,
        stats_incarnation: view.incarnation.to_vec(),
        visibility_fingerprint: view.fingerprint.to_vec(),
        visibility_columns_known: view.columns_known.to_vec(),
    }
}

/// The refusal every route outside the relay's scope answers with.
fn refused(route: &str) -> Status {
    Status::unimplemented(format!(
        "relay: NodeService.{route} is not served by a relay coordinator; a relay forwards \
         the read routes only: StreamSearch, SearchShard, TermStats, Health, \
         GetVectorBackend, the keyword leg (Bm25Query, Bm25PhraseQuery, Bm25QueryStream, \
         Bm25Rescore, ShardLegs), VectorRescore, ExactVectorRescore, the bitmap routes \
         (ResolveFilterBitmap, ResolveLexicalBitmap, ResolveVectorBitmap), and the \
         dictionaries (ExpandTermPrefix, SuggestTerms); see docs/relay-coordinators.md"
    ))
}

/// The children's vector backends as one: the descriptor and the
/// configuration must match child for child (the root's preflight
/// treats a shard's identity as generation-wide, so a relay hiding a
/// mismatch behind one answer would break that contract), rows sum
/// with checked arithmetic, and an unconfigured child refuses by name.
pub fn merge_vector_backend(
    children: &[String],
    reports: &[crate::pb::GetVectorBackendResponse],
) -> Result<crate::pb::GetVectorBackendResponse, Status> {
    let Some(first) = reports.first() else {
        return Err(Status::failed_precondition(
            "relay: a relay coordinator has no children",
        ));
    };
    let name = |shard: usize| children.get(shard).map_or("", String::as_str);
    let mut num_vectors: u64 = 0;
    for (shard, report) in reports.iter().enumerate() {
        if report.descriptor.is_none() {
            return Err(Status::failed_precondition(format!(
                "relay: child {shard} ({}) has no vector backend configured; a relay answers \
                 for configured children only",
                name(shard)
            )));
        }
        if report.descriptor != first.descriptor {
            return Err(Status::failed_precondition(format!(
                "relay: child {shard} ({}) advertises a different vector backend descriptor \
                 than child 0 ({}); a relay presents one provider identity",
                name(shard),
                name(0)
            )));
        }
        if report.config != first.config {
            return Err(Status::failed_precondition(format!(
                "relay: child {shard} ({}) runs a different vector backend configuration \
                 than child 0 ({}); a relay presents one provider identity",
                name(shard),
                name(0)
            )));
        }
        num_vectors = num_vectors.checked_add(report.num_vectors).ok_or_else(|| {
            Status::failed_precondition(format!(
                "relay: child {shard} ({}) overflows the summed vector count",
                name(shard)
            ))
        })?;
    }
    Ok(crate::pb::GetVectorBackendResponse {
        descriptor: first.descriptor.clone(),
        config: first.config.clone(),
        num_vectors,
    })
}

/// Sum the children's statistics shares into the parent's view of one
/// shard. Checked arithmetic throughout: a `u32` document frequency or
/// a `u64` total that would overflow refuses by name, because a wrong
/// idf is a wrong ranking and not a warning. Field capabilities must
/// agree across children: `known`, `positions`, and `sentences` are one
/// boolean each on the wire, and a mixed answer has no faithful spelling
/// in one (a phrase needs positions EVERYWHERE, a typo check needs the
/// field SOMEWHERE), so a relay refuses the mixture by name. The epoch is
/// left at 0 for the caller to replace with a token.
pub fn merge_term_stats(
    request: &TermStatsRequest,
    children: &[TermStatsResponse],
) -> Result<TermStatsResponse, Status> {
    crate::visibility::validate_stats_request(request)?;
    let scope = crate::visibility::VisibilityScope::new(request.visibility.as_ref())?;
    let mut visibility_columns_known = vec![false; scope.column_count()];
    if children.is_empty() {
        return Err(Status::failed_precondition(
            "relay: TermStats over no children",
        ));
    }
    let n_terms = request.terms.len();
    let mut doc_count: u64 = 0;
    let mut total_doc_length: u64 = 0;
    let mut doc_frequencies: Vec<u32> = vec![0; n_terms];
    let mut field_stats: Vec<crate::pb::FieldStats> = request
        .fields
        .iter()
        .map(|ft| crate::pb::FieldStats {
            total_doc_length: 0,
            doc_frequencies: vec![0; ft.terms.len()],
            known: false,
            positions: false,
            sentences: false,
        })
        .collect();
    for (shard, child) in children.iter().enumerate() {
        scope.validate_response(child)?;
        crate::visibility::validate_stats_mode(request.version_only, child)?;
        for (known, child_known) in visibility_columns_known
            .iter_mut()
            .zip(&child.visibility_columns_known)
        {
            *known |= child_known;
        }
        doc_count = doc_count.checked_add(child.doc_count).ok_or_else(|| {
            Status::failed_precondition(format!(
                "relay: document count sums past u64 at child {shard}"
            ))
        })?;
        total_doc_length = total_doc_length
            .checked_add(child.total_doc_length)
            .ok_or_else(|| {
                Status::failed_precondition(format!(
                    "relay: total document length sums past u64 at child {shard}"
                ))
            })?;
        if child.doc_frequencies.len() != n_terms {
            return Err(Status::internal(format!(
                "relay: child {shard} answered {} body document frequencies for {n_terms} terms",
                child.doc_frequencies.len()
            )));
        }
        for (ti, (acc, df)) in doc_frequencies
            .iter_mut()
            .zip(&child.doc_frequencies)
            .enumerate()
        {
            *acc = acc.checked_add(*df).ok_or_else(|| {
                Status::failed_precondition(format!(
                    "relay: document frequency of body term {:?} sums past the u32 the \
                     statistics contract carries, at child {shard}; the collection is \
                     larger than the contract admits",
                    request.terms[ti]
                ))
            })?;
        }
        if child.field_stats.len() != field_stats.len() {
            return Err(Status::internal(format!(
                "relay: child {shard} answered {} field shares for {} fields",
                child.field_stats.len(),
                field_stats.len()
            )));
        }
        for (fi, (acc, share)) in field_stats.iter_mut().zip(&child.field_stats).enumerate() {
            let field = &request.fields[fi].field;
            if shard == 0 {
                acc.known = share.known;
                acc.positions = share.positions;
                acc.sentences = share.sentences;
            } else {
                for (name, mine, theirs) in [
                    ("known", acc.known, share.known),
                    ("positions", acc.positions, share.positions),
                    ("sentences", acc.sentences, share.sentences),
                ] {
                    if mine != theirs {
                        return Err(Status::failed_precondition(format!(
                            "relay: field {field:?} is {name}={mine} on child 0 and \
                             {name}={theirs} on child {shard}; a relay requires homogeneous \
                             field capabilities across its children"
                        )));
                    }
                }
            }
            acc.total_doc_length = acc
                .total_doc_length
                .checked_add(share.total_doc_length)
                .ok_or_else(|| {
                    Status::failed_precondition(format!(
                        "relay: total length of field {field:?} sums past u64 at child {shard}"
                    ))
                })?;
            if share.doc_frequencies.len() != acc.doc_frequencies.len() {
                return Err(Status::internal(format!(
                    "relay: child {shard} answered {} document frequencies for {} terms of \
                     field {field:?}",
                    share.doc_frequencies.len(),
                    acc.doc_frequencies.len()
                )));
            }
            for (ti, (a, df)) in acc
                .doc_frequencies
                .iter_mut()
                .zip(&share.doc_frequencies)
                .enumerate()
            {
                *a = a.checked_add(*df).ok_or_else(|| {
                    Status::failed_precondition(format!(
                        "relay: document frequency of term {:?} in field {field:?} sums past \
                         the u32 the statistics contract carries, at child {shard}",
                        request.fields[fi].terms[ti]
                    ))
                })?;
            }
        }
    }
    Ok(TermStatsResponse {
        version_only: request.version_only,
        doc_count,
        total_doc_length,
        doc_frequencies,
        field_stats,
        stats_epoch: 0,
        stats_incarnation: Vec::new(),
        visibility_fingerprint: scope.fingerprint().to_vec(),
        visibility_columns_known,
    })
}

/// The span a child's health report covers in the global id space, the
/// same reading the root takes when it derives one interval per target.
fn health_span(report: &HealthResponse) -> u64 {
    report.num_vectors.max(report.bm25_docs)
}

/// Merge the children's health into the one shard the parent sees.
/// Refuses by name when the children's slot ranges are not contiguous
/// (a gap or an overlap would make the parent's derived range a lie),
/// when they disagree on the provider identity, or when there are none.
pub fn merge_health(
    collection: &str,
    children: &[String],
    reports: &[HealthResponse],
) -> Result<HealthResponse, Status> {
    if reports.is_empty() {
        return Err(Status::failed_precondition(
            "relay: a relay coordinator has no children",
        ));
    }
    let mut order: Vec<usize> = (0..reports.len()).collect();
    order.sort_by_key(|&i| reports[i].slot_offset);
    for pair in order.windows(2) {
        let (a, b) = (pair[0], pair[1]);
        let end = reports[a].slot_offset + health_span(&reports[a]);
        if end != reports[b].slot_offset {
            let relation = if end > reports[b].slot_offset {
                "overlaps"
            } else {
                "leaves a gap before"
            };
            return Err(Status::failed_precondition(format!(
                "relay: child {a} ({}) covers slots {}..{end} and {relation} child {b} ({}) at \
                 slot {}; a relay serves contiguous children only, so the range the parent \
                 derives from one health report is real",
                children.get(a).map_or("", String::as_str),
                reports[a].slot_offset,
                children.get(b).map_or("", String::as_str),
                reports[b].slot_offset
            )));
        }
    }
    let first = &reports[order[0]];
    let last = &reports[*order.last().expect("non-empty")];
    let mut merged = HealthResponse {
        collection: collection.to_string(),
        slot_offset: first.slot_offset,
        dim: first.dim,
        bits_per_dimension: first.bits_per_dimension,
        vector_backend: first.vector_backend.clone(),
        scoring_fingerprint: first.scoring_fingerprint.clone(),
        quality_contract: first.quality_contract.clone(),
        exact_vectors_available: true,
        exact_vectors_mmap: true,
        ..Default::default()
    };
    for (shard, report) in reports.iter().enumerate() {
        for (name, mine, theirs) in [
            (
                "vector_backend",
                &merged.vector_backend,
                &report.vector_backend,
            ),
            (
                "scoring_fingerprint",
                &merged.scoring_fingerprint,
                &report.scoring_fingerprint,
            ),
            (
                "quality_contract",
                &merged.quality_contract,
                &report.quality_contract,
            ),
        ] {
            if mine != theirs {
                return Err(Status::failed_precondition(format!(
                    "relay: child {shard} reports {name} {theirs:?} while child {} reports \
                     {mine:?}; one relay serves one score space",
                    order[0]
                )));
            }
        }
        if report.dim != 0 && merged.dim != 0 && report.dim != merged.dim {
            return Err(Status::failed_precondition(format!(
                "relay: child {shard} has dimension {} while child {} has {}",
                report.dim, order[0], merged.dim
            )));
        }
        if merged.dim == 0 {
            merged.dim = report.dim;
            merged.bits_per_dimension = report.bits_per_dimension;
        }
        merged.num_vectors += report.num_vectors;
        merged.bm25_docs += report.bm25_docs;
        merged.bm25_building |= report.bm25_building;
        merged.ingest_active |= report.ingest_active;
        merged.exact_vectors_available &= report.exact_vectors_available;
        merged.exact_vectors_mmap &= report.exact_vectors_mmap;
        merged.exact_vector_rows += report.exact_vector_rows;
        merged.live_docs += report.live_docs;
        merged.deleted_docs += report.deleted_docs;
    }
    // The replication tip relative to the relay's base: the last child's
    // tip, rebased. A relay has no WAL of its own and no live revision,
    // and says so with zeros and `wal_clocked = false`; it never invents
    // a watermark for a subtree.
    merged.document_slots = (last.slot_offset - first.slot_offset) + last.document_slots;
    merged.live_revision = 0;
    merged.wal_generation = 0;
    merged.wal_high_watermark = 0;
    merged.wal_clocked = false;
    Ok(merged)
}

/// Sums carried across the children's terminal summaries.
#[derive(Default)]
struct StreamTotals {
    emitted: u64,
    blocks_scanned: u64,
    floor_raises_applied: u64,
    segments_total: u32,
    segments_skipped: u32,
}

enum StreamEvent {
    Message(Option<(usize, StreamSearchResponse)>),
    Floor(f32),
    Cancel,
    Deadline,
    MapMoved(u64),
}

/// The forwarding loop of one relayed `StreamSearch`: children's batches
/// go up untouched, the parent's floor and cancel go down, and the
/// relay's summary follows the last child's.
#[allow(clippy::too_many_arguments)]
async fn relay_stream(
    coordinator: CoordinatorServiceImpl,
    pinned: MapSnapshot,
    mut map_changes: watch::Receiver<u64>,
    start: crate::pb::StartStreamSearch,
    filters: RequestFilters,
    signals: Arc<RelaySignals>,
    tx: mpsc::Sender<Result<StreamSearchResponse, Status>>,
    deadline: Option<tokio::time::Instant>,
    mut input: tokio::task::JoinHandle<Result<Option<crate::pb::ResolveStreamIdentities>, Status>>,
    identity_ready: Arc<AtomicBool>,
) -> Result<(), Status> {
    // A publication racing the pin: the receiver may already hold a newer
    // value than the reading, which the first poll below reports.
    map_changes.mark_unchanged();
    if *map_changes.borrow() != pinned.control_revision {
        map_changes.mark_changed();
    }
    let n = coordinator.node_addresses().len();
    if n == 0 {
        return Err(Status::failed_precondition(
            "relay: a relay coordinator has no children",
        ));
    }
    let identity_limits = start.identity_limits;
    if let Some(limits) = identity_limits.as_ref() {
        crate::query_identity::validate_limits(limits)?;
    }
    let stride = if start.collapse_parents { 20 } else { 12 };
    let mask = coordinator.shard_mask(filters.tree.as_ref());
    let mut fanout: StreamFanout = coordinator.open_stream_fanout_with_identities(
        &start.request_id,
        &start.vector,
        start.initial_floor,
        start.collapse_parents,
        &filters,
        identity_limits,
    )?;
    let mut summaries: Vec<Option<StreamSearchSummary>> = vec![None; n];
    let mut remaining = n;
    for (shard, summary) in summaries.iter_mut().enumerate() {
        if mask.as_ref().is_some_and(|m| m.skipped[shard]) {
            *summary = Some(StreamSearchSummary {
                completed: true,
                ..Default::default()
            });
            remaining -= 1;
        }
    }
    let mut terminal = summaries.clone();
    let mut ranges: Vec<Option<crate::pb::StreamIdentityRange>> = vec![None; n];
    let leaves = filters.tree.as_ref().map_or(0, crate::filter::leaf_count);
    let mut geo_known = vec![false; filters.geo.len()];
    let mut tree_known = vec![false; leaves];
    if let Some(mask) = mask.as_ref() {
        for &leaf in &mask.known {
            if let Some(flag) = tree_known.get_mut(leaf) {
                *flag = true;
            }
        }
    }
    let mut totals = StreamTotals::default();
    let mut scoring_fingerprint: Option<String> = None;
    let mut last_floor = start.initial_floor.unwrap_or(f32::NEG_INFINITY);
    let mut floor_rx = signals.floor.subscribe();
    let mut floor_open = true;
    let mut sleep = deadline.map(|at| Box::pin(tokio::time::sleep_until(at)));

    while remaining > 0 {
        let event = tokio::select! {
            biased;
            _ = signals.cancel.notified(), if !signals.cancelled.load(Ordering::Acquire) => StreamEvent::Cancel,
            _ = async { true }, if signals.cancelled.load(Ordering::Acquire) => StreamEvent::Cancel,
            _ = async { sleep.as_mut().expect("guarded").await }, if sleep.is_some() => StreamEvent::Deadline,
            moved = map_changes.changed() => match moved {
                Ok(()) => StreamEvent::MapMoved(*map_changes.borrow_and_update()),
                Err(_) => {
                    // The source is gone with its authority; nothing
                    // will move again, and the pinned reading stands.
                    map_changes = watch::channel(pinned.control_revision).1;
                    continue;
                }
            },
            changed = floor_rx.changed(), if floor_open => match changed {
                Ok(()) => StreamEvent::Floor(*floor_rx.borrow_and_update()),
                Err(_) => {
                    floor_open = false;
                    continue;
                }
            },
            message = fanout.next_message(&terminal) => match message {
                Ok(pair) => StreamEvent::Message(pair),
                Err(status) => return fanout.cancel_with(status).await,
            },
        };
        match event {
            StreamEvent::Cancel => {
                fanout.cancel().await;
                let summary = StreamSearchSummary {
                    completed: false,
                    emitted: totals.emitted,
                    blocks_scanned: totals.blocks_scanned,
                    floor_raises_applied: totals.floor_raises_applied,
                    geo_columns_known: geo_known,
                    filter_columns_known: tree_known,
                    scoring_fingerprint: scoring_fingerprint.unwrap_or_default(),
                    segments_total: totals.segments_total,
                    segments_skipped: totals.segments_skipped,
                };
                let _ = tx
                    .send(Ok(StreamSearchResponse {
                        payload: Some(stream_search_response::Payload::Summary(summary)),
                    }))
                    .await;
                return Ok(());
            }
            StreamEvent::Deadline => {
                return fanout
                    .cancel_with(Status::deadline_exceeded(
                        "relay: the parent's deadline passed before every child completed",
                    ))
                    .await;
            }
            StreamEvent::MapMoved(revision) => {
                if revision == pinned.control_revision {
                    continue;
                }
                return fanout
                    .cancel_with(Status::failed_precondition(format!(
                        "relay: the shard map moved from revision {} (generation {}) to \
                         revision {revision} during StreamSearch; the stream was opened under \
                         the older map and is not completed; retry under the current one",
                        pinned.control_revision, pinned.topology_generation
                    )))
                    .await;
            }
            StreamEvent::Floor(floor) => {
                if floor > last_floor {
                    last_floor = floor;
                    coordinator.push_stream_floor(&fanout, floor);
                }
            }
            StreamEvent::Message(None) => continue,
            StreamEvent::Message(Some((shard, message))) => {
                if summaries[shard].is_some() {
                    return fanout
                        .cancel_with(Status::internal(
                            "relay: child message after scan certificate",
                        ))
                        .await;
                }
                let payload = match message.payload {
                    Some(stream_search_response::Payload::IdentityReady(ready))
                        if identity_limits.is_some() =>
                    {
                        let summary = ready.scan.ok_or_else(|| {
                            Status::internal("relay: missing readiness certificate")
                        })?;
                        if let Some(range) = ready.range.as_ref() {
                            if range.first_id > range.last_id
                                || ranges.iter().flatten().any(|other| {
                                    range.first_id <= other.last_id
                                        && other.first_id <= range.last_id
                                })
                            {
                                return fanout
                                    .cancel_with(Status::failed_precondition(
                                        "relay: overlapping or invalid captured identity ranges",
                                    ))
                                    .await;
                            }
                        } else if summary.emitted != 0 {
                            return fanout
                                .cancel_with(Status::failed_precondition(
                                    "relay: candidates without a captured identity range",
                                ))
                                .await;
                        }
                        ranges[shard] = ready.range;
                        Some(stream_search_response::Payload::Summary(summary))
                    }
                    Some(stream_search_response::Payload::Summary(_))
                        if identity_limits.is_some() =>
                    {
                        return fanout
                            .cancel_with(Status::failed_precondition(
                                "relay: child lacks snapshot-bound identity support",
                            ))
                            .await;
                    }
                    payload => payload,
                };
                match payload {
                    Some(stream_search_response::Payload::Batch(batch)) => {
                        if batch.hits.is_empty() || batch.hits.len() % stride != 0 {
                            let status = Status::internal(format!(
                                "relay: child {shard} sent a batch of {} bytes, not a multiple of \
                             the {stride}-byte record",
                                batch.hits.len()
                            ));
                            return fanout.cancel_with(status).await;
                        }
                        if tx
                            .send(Ok(StreamSearchResponse {
                                payload: Some(stream_search_response::Payload::Batch(batch)),
                            }))
                            .await
                            .is_err()
                        {
                            // The parent is gone: nobody reads what the
                            // children still scan for.
                            fanout.cancel().await;
                            return Ok(());
                        }
                    }
                    Some(stream_search_response::Payload::Summary(summary)) => {
                        if !summary.completed {
                            let status = Status::internal(format!(
                                "relay: child {shard} stopped before completing its scan"
                            ));
                            return fanout.cancel_with(status).await;
                        }
                        if summary.scoring_fingerprint.is_empty() {
                            let status = Status::failed_precondition(format!(
                            "relay: child {shard} completed without a vector scoring fingerprint"
                        ));
                            return fanout.cancel_with(status).await;
                        }
                        match scoring_fingerprint.as_ref() {
                            Some(expected) if expected != &summary.scoring_fingerprint => {
                                let status = Status::failed_precondition(format!(
                                    "relay: child {shard} vector scoring fingerprint {} differs \
                                 from {expected}",
                                    summary.scoring_fingerprint
                                ));
                                return fanout.cancel_with(status).await;
                            }
                            None => scoring_fingerprint = Some(summary.scoring_fingerprint.clone()),
                            _ => {}
                        }
                        if summary.geo_columns_known.len() != geo_known.len()
                            || summary.filter_columns_known.len() != tree_known.len()
                        {
                            let status = Status::internal(format!(
                                "relay: child {shard} answered {} geo and {} filter flags for {} \
                             and {}",
                                summary.geo_columns_known.len(),
                                summary.filter_columns_known.len(),
                                geo_known.len(),
                                tree_known.len()
                            ));
                            return fanout.cancel_with(status).await;
                        }
                        for (acc, k) in geo_known.iter_mut().zip(&summary.geo_columns_known) {
                            *acc |= *k;
                        }
                        for (acc, k) in tree_known.iter_mut().zip(&summary.filter_columns_known) {
                            *acc |= *k;
                        }
                        totals.emitted += summary.emitted;
                        totals.blocks_scanned += summary.blocks_scanned;
                        totals.floor_raises_applied += summary.floor_raises_applied;
                        totals.segments_total =
                            totals.segments_total.saturating_add(summary.segments_total);
                        totals.segments_skipped = totals
                            .segments_skipped
                            .saturating_add(summary.segments_skipped);
                        summaries[shard] = Some(summary.clone());
                        if identity_limits.is_none() {
                            terminal[shard] = Some(summary);
                            fanout.mark_completed(shard);
                        }
                        remaining -= 1;
                    }
                    _ => {
                        return fanout
                            .cancel_with(Status::internal("relay: unexpected identity message"))
                            .await
                    }
                }
            }
        }
    }
    let summary = StreamSearchSummary {
        completed: true,
        emitted: totals.emitted,
        blocks_scanned: totals.blocks_scanned,
        floor_raises_applied: totals.floor_raises_applied,
        geo_columns_known: geo_known,
        filter_columns_known: tree_known,
        // Every child agreed, or the loop above refused; a relay over
        // masked-out children only has no fingerprint to report and the
        // parent refuses that, as it refuses a node without one.
        scoring_fingerprint: scoring_fingerprint.unwrap_or_default(),
        segments_total: totals.segments_total,
        segments_skipped: totals.segments_skipped,
    };
    if let Some(limits) = identity_limits {
        let range =
            ranges
                .iter()
                .flatten()
                .fold(None::<crate::pb::StreamIdentityRange>, |acc, range| {
                    Some(match acc {
                        None => *range,
                        Some(acc) => crate::pb::StreamIdentityRange {
                            first_id: acc.first_id.min(range.first_id),
                            last_id: acc.last_id.max(range.last_id),
                        },
                    })
                });
        let exchange = async {
            identity_ready.store(true, Ordering::Release);
            tx.send(Ok(StreamSearchResponse {
                payload: Some(stream_search_response::Payload::IdentityReady(
                    crate::pb::StreamIdentityReady {
                        scan: Some(summary.clone()),
                        range,
                    },
                )),
            }))
            .await
            .map_err(|_| Status::cancelled("relay: parent response closed"))?;
            let selection = (&mut input)
                .await
                .map_err(|e| Status::internal(format!("relay: request task failed: {e}")))??
                .ok_or_else(|| Status::cancelled("relay: selection stopped"))?;
            if selection.vector_ids.len() > limits.max_rows as usize {
                return Err(Status::resource_exhausted(
                    "relay: identity selection exceeds max_rows",
                ));
            }
            let mut seen = std::collections::HashSet::new();
            let mut hits = Vec::with_capacity(selection.vector_ids.len());
            for id in selection.vector_ids {
                if !seen.insert(id) {
                    return Err(Status::invalid_argument(
                        "relay: identity selection repeats an ID",
                    ));
                }
                let shard = ranges
                    .iter()
                    .position(|range| {
                        range
                            .as_ref()
                            .is_some_and(|r| r.first_id <= id && id <= r.last_id)
                    })
                    .ok_or_else(|| {
                        Status::invalid_argument(
                            "relay: identity ID is outside captured child ranges",
                        )
                    })?;
                hits.push(crate::merge::MergedHit {
                    shard: shard as u32,
                    vector_id: id,
                    score: 0.0,
                });
            }
            let mut identities = fanout
                .resolve_identities(&hits, &summaries, &mut terminal, &limits)
                .await?;
            let rows = hits
                .into_iter()
                .map(|hit| crate::pb::StreamIdentity {
                    vector_id: hit.vector_id,
                    identity: identities
                        .remove(&(hit.shard, hit.vector_id))
                        .expect("validated child response"),
                })
                .collect();
            let response = StreamSearchResponse {
                payload: Some(stream_search_response::Payload::Identities(
                    crate::pb::StreamIdentities { rows },
                )),
            };
            if prost::Message::encoded_len(&response) > limits.max_response_bytes as usize {
                return Err(Status::resource_exhausted(
                    "relay: identity response exceeds max_response_bytes",
                ));
            }
            tx.send(Ok(response))
                .await
                .map_err(|_| Status::cancelled("relay: parent response closed"))?;
            tx.send(Ok(StreamSearchResponse {
                payload: Some(stream_search_response::Payload::Summary(summary.clone())),
            }))
            .await
            .map_err(|_| Status::cancelled("relay: parent response closed"))
        };
        let result = tokio::select! {
            _ = signals.cancel.notified() => Err(Status::cancelled("relay: identity selection stopped")),
            _ = async {}, if signals.cancelled.load(Ordering::Acquire) => Err(Status::cancelled("relay: identity selection stopped")),
            _ = tx.closed() => Err(Status::cancelled("relay: parent response closed")),
            _ = async { sleep.as_mut().expect("guarded").await }, if sleep.is_some() => Err(Status::deadline_exceeded("relay: parent deadline passed during identity selection")),
            _ = async { loop {
                if map_changes.changed().await.is_err() { std::future::pending::<()>().await; }
                if *map_changes.borrow_and_update() != pinned.control_revision { break; }
            }} => Err(Status::failed_precondition("relay: map moved during identity selection")),
            result = tokio::time::timeout(Duration::from_millis(u64::from(limits.timeout_ms)), exchange) =>
                result.unwrap_or_else(|_| Err(Status::deadline_exceeded("relay: identity selection timed out"))),
        };
        if let Err(status) = result {
            fanout.cancel().await;
            if signals.cancelled.load(Ordering::Acquire) {
                let _ = tx
                    .send(Ok(StreamSearchResponse {
                        payload: Some(stream_search_response::Payload::Summary(
                            StreamSearchSummary {
                                completed: false,
                                ..summary
                            },
                        )),
                    }))
                    .await;
                return Ok(());
            }
            return Err(status);
        }
    } else {
        let _ = tx
            .send(Ok(StreamSearchResponse {
                payload: Some(stream_search_response::Payload::Summary(summary)),
            }))
            .await;
    }
    Ok(())
}

// --- The keyword leg ---------------------------------------------------

/// A child's error as the parent must read it. The stale-epoch prefix
/// stays at the front so the parent's retry rule fires; everything else
/// gets the child named after the code.
fn child_error(shard: usize, addr: &str, route: &str, status: Status) -> Status {
    let message = status.message();
    let text = match message.strip_prefix(STALE_STATS_EPOCH) {
        Some(rest) => format!(
            "{STALE_STATS_EPOCH}: relay child {shard} ({addr}) {route}{}",
            rest
        ),
        None => format!("relay child {shard} ({addr}) {route}: {message}"),
    };
    Status::new(status.code(), text)
}

/// The two request shapes this level does not merge: column statistics
/// fold floating-point partials in an order the root pins, and an exact
/// cardinality is a union of values, not a sum. Both are refused by name
/// rather than answered with a different reduction.
fn refuse_bm25_aggregates(
    route: &str,
    stats_fields: &[String],
    cardinality_fields: &[String],
) -> Result<(), Status> {
    if !stats_fields.is_empty() {
        return Err(Status::unimplemented(format!(
            "relay: {route} with stats_fields {stats_fields:?} is not served through a relay; \
             a column statistic folds in the root's shard order and this level would change \
             it"
        )));
    }
    if !cardinality_fields.is_empty() {
        return Err(Status::unimplemented(format!(
            "relay: {route} with cardinality_fields {cardinality_fields:?} is not served \
             through a relay; an exact cardinality is a union of values, not a sum of counts"
        )));
    }
    Ok(())
}

/// OR one child's per-column flags into the accumulator, which every
/// child must size the same way.
fn merge_known(
    what: &str,
    shard: usize,
    acc: &mut Option<Vec<bool>>,
    share: &[bool],
) -> Result<(), Status> {
    match acc {
        None => *acc = Some(share.to_vec()),
        Some(flags) => {
            if flags.len() != share.len() {
                return Err(Status::internal(format!(
                    "relay: child {shard} answered {} {what} flags while an earlier child \
                     answered {}",
                    share.len(),
                    flags.len()
                )));
            }
            for (a, b) in flags.iter_mut().zip(share) {
                *a |= *b;
            }
        }
    }
    Ok(())
}

/// The children's terminal responses merged into the one the parent
/// reads as a shard's: every child's local top-k concatenated (the
/// parent's global merge picks from the union, and nothing this level
/// could drop is provably outside the parent's top-k), facet counts
/// summed by value, range buckets summed by position, column-known
/// flags ORed, segment counts added with a check. A facet no child knows
/// stays `known = false` for the root's typo rule; refusing it here
/// would answer for shards this relay does not see.
/// The ranked union of the children's Boolean answers cut to `depth`,
/// score descending then doc id ascending (the root's own order), with
/// the match counts and segment counts summed and each leaf's known
/// flags joined by OR. A child answering a flag list of another shape
/// than the request's leaves is a protocol break.
pub fn merge_boolean_responses(
    req: &crate::pb::BooleanShardRequest,
    shares: &[crate::pb::BooleanShardResponse],
) -> Result<crate::pb::BooleanShardResponse, Status> {
    let leaves = req.leaves.len();
    let mut candidates = Vec::new();
    let mut matched = 0u64;
    let mut segments_total = 0u32;
    let mut segments_skipped = 0u32;
    let mut filters_known: Vec<Option<crate::pb::BooleanFilterKnown>> = vec![None; leaves];
    let mut stages_known: Vec<Option<crate::pb::BooleanStagesKnown>> = vec![None; leaves];
    for (shard, share) in shares.iter().enumerate() {
        if share.filters_known.len() != leaves || share.stages_known.len() != leaves {
            return Err(Status::internal(format!(
                "relay: child {shard} answered {} filter and {} stage flag lists for {leaves} leaves",
                share.filters_known.len(),
                share.stages_known.len()
            )));
        }
        candidates.extend(share.candidates.iter().cloned());
        matched = matched.checked_add(share.matched).ok_or_else(|| {
            Status::internal("relay: Boolean match count overflows u64 across children")
        })?;
        segments_total = segments_total.saturating_add(share.segments_total);
        segments_skipped = segments_skipped.saturating_add(share.segments_skipped);
        for (index, known) in share.filters_known.iter().enumerate() {
            join_flags(&mut filters_known[index], known, |acc, k| {
                or_flags(
                    &mut acc.geo_columns_known,
                    &k.geo_columns_known,
                    shard,
                    "geo",
                )?;
                or_flags(
                    &mut acc.filter_columns_known,
                    &k.filter_columns_known,
                    shard,
                    "filter-leaf",
                )
            })?;
        }
        for (index, known) in share.stages_known.iter().enumerate() {
            join_flags(&mut stages_known[index], known, |acc, k| {
                or_flags(
                    &mut acc.stage_columns_known,
                    &k.stage_columns_known,
                    shard,
                    "stage",
                )
            })?;
        }
    }
    candidates.sort_by(|a, b| {
        b.score
            .total_cmp(&a.score)
            .then_with(|| a.doc_id.cmp(&b.doc_id))
    });
    candidates.truncate(req.depth as usize);
    Ok(crate::pb::BooleanShardResponse {
        read_receipt: None,
        candidates,
        matched,
        segments_total,
        segments_skipped,
        stats_epoch: 0,
        filters_known: filters_known
            .into_iter()
            .map(Option::unwrap_or_default)
            .collect(),
        stages_known: stages_known
            .into_iter()
            .map(Option::unwrap_or_default)
            .collect(),
        aggregate: None,
    })
}

fn join_flags<T: Clone>(
    acc: &mut Option<T>,
    share: &T,
    join: impl FnOnce(&mut T, &T) -> Result<(), Status>,
) -> Result<(), Status> {
    match acc {
        None => {
            *acc = Some(share.clone());
            Ok(())
        }
        Some(acc) => join(acc, share),
    }
}

fn or_flags(acc: &mut [bool], share: &[bool], shard: usize, what: &str) -> Result<(), Status> {
    if acc.len() != share.len() {
        return Err(Status::internal(format!(
            "relay: child {shard} answered {} {what} flags while an earlier child answered {}",
            share.len(),
            acc.len()
        )));
    }
    for (a, k) in acc.iter_mut().zip(share) {
        *a |= *k;
    }
    Ok(())
}

pub fn merge_bm25_responses(
    req: &Bm25QueryRequest,
    shares: Vec<Bm25QueryResponse>,
) -> Result<Bm25QueryResponse, Status> {
    let facet_slots = req.facet_fields.len() + req.map_facet_fields.len();
    crate::node::validate_range_facet_fields(&req.range_facet_fields)?;
    let mut range_shares = Vec::new();
    let mut hits = Vec::new();
    let mut facet_known = vec![false; facet_slots];
    let mut facet_names: Vec<(String, String)> = Vec::new();
    let mut facet_sums: Vec<HashMap<String, u64>> =
        (0..facet_slots).map(|_| HashMap::new()).collect();
    let mut stage_known = None;
    let mut geo_known = None;
    let mut filter_known = None;
    let mut projection_known = None;
    let mut projection_types = vec![crate::pb::ScalarValueType::Unspecified; req.projections.len()];
    let mut segments_total: u32 = 0;
    let mut segments_skipped: u32 = 0;
    for (shard, share) in shares.into_iter().enumerate() {
        if share.facets.len() != facet_slots {
            return Err(Status::internal(format!(
                "relay: child {shard} answered {} facet fields for {facet_slots} requested",
                share.facets.len()
            )));
        }
        for (fi, ff) in share.facets.iter().enumerate() {
            if facet_names.len() <= fi {
                facet_names.push((ff.field.clone(), ff.key.clone()));
            }
            facet_known[fi] |= ff.known;
            for c in &ff.counts {
                let acc = facet_sums[fi].entry(c.value.clone()).or_default();
                *acc = acc.checked_add(c.count).ok_or_else(|| {
                    Status::internal(format!(
                        "relay: facet {:?} value {:?} count overflows u64 across children",
                        ff.field, c.value
                    ))
                })?;
            }
        }
        range_shares.push(share.range_facets);
        merge_known(
            "stage-column",
            shard,
            &mut stage_known,
            &share.stage_columns_known,
        )?;
        merge_known(
            "geo-column",
            shard,
            &mut geo_known,
            &share.geo_columns_known,
        )?;
        merge_known(
            "filter-leaf",
            shard,
            &mut filter_known,
            &share.filter_columns_known,
        )?;
        merge_known(
            "projection-leaf",
            shard,
            &mut projection_known,
            &share.projection_leaves_known,
        )?;
        segments_total = segments_total
            .checked_add(share.segments_total)
            .ok_or_else(|| Status::internal("relay: segments_total overflows u32"))?;
        segments_skipped = segments_skipped
            .checked_add(share.segments_skipped)
            .ok_or_else(|| Status::internal("relay: segments_skipped overflows u32"))?;
        crate::values::merge_projection_types(
            &req.projections,
            &mut projection_types,
            &share.projection_types,
        )?;
        for hit in &share.hits {
            crate::values::validate_projection_row(&hit.projected, &share.projection_types)?;
        }
        hits.extend(share.hits);
    }
    // The monolith's order: score, then id. The parent re-merges under
    // its own total order; this order only decides the k-th seed below.
    hits.sort_by(|a, b| {
        b.score
            .total_cmp(&a.score)
            .then_with(|| a.doc_id.cmp(&b.doc_id))
    });
    let k = req.k as usize;
    let kth_best = if k > 0 && hits.len() >= k {
        crate::bm25::floor_seed(hits[k - 1].score)
    } else {
        0.0
    };
    let facets = facet_names
        .into_iter()
        .zip(facet_known)
        .zip(facet_sums)
        .map(|(((field, key), known), sum)| {
            let mut counts: Vec<crate::pb::FacetCount> = sum
                .into_iter()
                .map(|(value, count)| crate::pb::FacetCount { value, count })
                .collect();
            counts.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.value.cmp(&b.value)));
            crate::pb::FacetFieldCounts {
                field,
                known,
                counts: if known { counts } else { Vec::new() },
                key,
            }
        })
        .collect();
    let range_facets = crate::rangefacet::merge(&req.range_facet_fields, &range_shares, false)?;
    Ok(Bm25QueryResponse {
        hits,
        kth_best,
        facets,
        stage_columns_known: stage_known.unwrap_or_default(),
        range_facets,
        geo_columns_known: geo_known.unwrap_or_default(),
        filter_columns_known: filter_known.unwrap_or_default(),
        stats: Vec::new(),
        distinct: Vec::new(),
        projection_leaves_known: projection_known.unwrap_or_default(),
        projection_types: projection_types.into_iter().map(|ty| ty as i32).collect(),
        segments_total,
        segments_skipped,
    })
}

/// Which child holds a global id: the one whose slot range (from its
/// health report) contains it. Contiguity was checked at startup; a gap
/// that appeared since refuses by name here.
fn child_of_id(ranges: &[(u64, u64)], id: u64) -> Option<usize> {
    ranges
        .iter()
        .position(|&(offset, span)| id >= offset && id - offset < span)
}

#[allow(clippy::large_enum_variant)]
enum Bm25Event {
    Child(usize, Result<Option<Bm25QueryStreamResponse>, Status>),
    Parent(Option<Result<Bm25QueryStreamRequest, Status>>),
    Deadline,
    MapMoved(u64),
}

impl RelayService {
    /// One request per child with the parent's claim translated into
    /// that child's, or the stale-epoch refusal when the token is not
    /// one this relay can translate under the current map.
    fn child_claims(
        &self,
        token: u64,
        incarnation: &[u8],
        children: usize,
    ) -> Result<Vec<StatsClaim>, Status> {
        self.stats_incarnation.check(token, token, incarnation)?;
        let claims = self.translate_epoch(token)?;
        if claims.len() != children {
            return Err(Status::failed_precondition(format!(
                "{STALE_STATS_EPOCH}: relay token {token} names {} children and the map has \
                 {children}; refetch TermStats",
                claims.len()
            )));
        }
        Ok(claims)
    }

    /// The forwarding loop of one relayed `Bm25QueryStream`: children's
    /// candidate batches go up untouched, their cutoff raises go up
    /// monotone, the parent's raises and its stop go down, and the
    /// relay's completion follows the last child's.
    #[allow(clippy::too_many_arguments)]
    async fn relay_bm25_stream(
        &self,
        frozen: CoordinatorServiceImpl,
        pinned: MapSnapshot,
        mut map_changes: watch::Receiver<u64>,
        req: Bm25QueryRequest,
        claims: Vec<StatsClaim>,
        mut inbound: Streaming<Bm25QueryStreamRequest>,
        tx: mpsc::Sender<Result<Bm25QueryStreamResponse, Status>>,
        deadline: Option<tokio::time::Instant>,
        timeout: Option<Duration>,
    ) -> Result<(), Status> {
        map_changes.mark_unchanged();
        if *map_changes.borrow() != pinned.control_revision {
            map_changes.mark_changed();
        }
        let children = frozen.node_addresses().to_vec();
        let n = children.len();
        // One outbound leg per child and one shared inbound lane the
        // children's reader tasks feed, tagged by child.
        let (events_tx, mut events_rx) =
            mpsc::channel::<(usize, Result<Option<Bm25QueryStreamResponse>, Status>)>(64);
        let mut legs: Vec<Option<mpsc::Sender<Bm25QueryStreamRequest>>> = Vec::with_capacity(n);
        let mut readers = Vec::with_capacity(n);
        for (shard, addr) in children.iter().enumerate() {
            let mut link = frozen.node_client(addr)?;
            let (out_tx, out_rx) = mpsc::channel::<Bm25QueryStreamRequest>(8);
            let mut child_req = req.clone();
            child_req.expected_stats_epoch = claims[shard].epoch;
            child_req.expected_stats_incarnation = claims[shard].incarnation();
            out_tx
                .send(Bm25QueryStreamRequest {
                    payload: Some(bm25_query_stream_request::Payload::Start(child_req)),
                })
                .await
                .map_err(|_| Status::internal("relay: child request channel closed at start"))?;
            let mut request = Request::new(ReceiverStream::new(out_rx));
            if let Some(timeout) = timeout {
                request.set_timeout(timeout);
            }
            let events = events_tx.clone();
            let addr = addr.clone();
            readers.push(tokio::spawn(async move {
                let mut stream = match link.bm25_query_stream(request).await {
                    Ok(response) => response.into_inner(),
                    Err(status) => {
                        let _ = events
                            .send((shard, Err(child_error(shard, &addr, "bm25 stream", status))))
                            .await;
                        return;
                    }
                };
                loop {
                    let next = stream.message().await;
                    let done = matches!(next, Ok(None) | Err(_));
                    let item =
                        next.map_err(|status| child_error(shard, &addr, "bm25 stream", status));
                    if events.send((shard, item)).await.is_err() || done {
                        return;
                    }
                }
            }));
            legs.push(Some(out_tx));
        }
        drop(events_tx);
        let stop_children = |legs: &mut Vec<Option<mpsc::Sender<Bm25QueryStreamRequest>>>| {
            let stops: Vec<mpsc::Sender<Bm25QueryStreamRequest>> =
                legs.iter_mut().filter_map(Option::take).collect();
            async move {
                for leg in stops {
                    let _ = leg
                        .send(Bm25QueryStreamRequest {
                            payload: Some(bm25_query_stream_request::Payload::Stop(
                                StopBm25Query {},
                            )),
                        })
                        .await;
                }
            }
        };
        let mut completions: Vec<Option<Bm25StreamCompletion>> = vec![None; n];
        let mut remaining = n;
        let mut forwarded: u64 = 0;
        let mut last_up = f32::NEG_INFINITY;
        let mut last_down = f32::NEG_INFINITY;
        let mut fingerprint: Option<String> = None;
        let mut parent_open = true;
        let mut sleep = deadline.map(|at| Box::pin(tokio::time::sleep_until(at)));
        let outcome: Result<Option<Bm25StreamCompletion>, Status> = loop {
            if remaining == 0 {
                break Ok(None);
            }
            let event = tokio::select! {
                biased;
                _ = async { sleep.as_mut().expect("guarded").await }, if sleep.is_some() => Bm25Event::Deadline,
                moved = map_changes.changed() => match moved {
                    Ok(()) => Bm25Event::MapMoved(*map_changes.borrow_and_update()),
                    Err(_) => {
                        map_changes = watch::channel(pinned.control_revision).1;
                        continue;
                    }
                },
                message = inbound.message(), if parent_open => Bm25Event::Parent(message.transpose()),
                event = events_rx.recv() => match event {
                    Some((shard, item)) => Bm25Event::Child(shard, item),
                    None => break Err(Status::internal("relay: every child reader ended before completion")),
                },
            };
            match event {
                Bm25Event::Deadline => {
                    break Err(Status::deadline_exceeded(
                        "relay: the parent's deadline passed before every child completed",
                    ));
                }
                Bm25Event::MapMoved(revision) => {
                    if revision == pinned.control_revision {
                        continue;
                    }
                    break Err(Status::failed_precondition(format!(
                        "relay: the shard map moved from revision {} (generation {}) to \
                         revision {revision} during Bm25QueryStream; the stream was opened \
                         under the older map and is not completed; retry under the current one",
                        pinned.control_revision, pinned.topology_generation
                    )));
                }
                Bm25Event::Parent(Some(Ok(Bm25QueryStreamRequest {
                    payload: Some(bm25_query_stream_request::Payload::FloorUpdate(u)),
                }))) => {
                    if !u.floor.is_nan() && u.floor > last_down {
                        last_down = u.floor;
                        for leg in legs.iter().flatten() {
                            let _ = leg
                                .send(Bm25QueryStreamRequest {
                                    payload: Some(bm25_query_stream_request::Payload::FloorUpdate(
                                        FloorUpdate { floor: u.floor },
                                    )),
                                })
                                .await;
                        }
                    }
                }
                Bm25Event::Parent(Some(Ok(Bm25QueryStreamRequest {
                    payload: Some(bm25_query_stream_request::Payload::Stop(_)),
                }))) => {
                    // The parent's stop: the children are told, and the
                    // relay certifies an incomplete scan without waiting
                    // on children that may hold their answer.
                    stop_children(&mut legs).await;
                    break Ok(Some(Bm25StreamCompletion {
                        completed: false,
                        response: None,
                        scoring_fingerprint: fingerprint.clone().unwrap_or_default(),
                        candidates_emitted: forwarded,
                    }));
                }
                Bm25Event::Parent(Some(Ok(_))) => {}
                Bm25Event::Parent(Some(Err(_))) | Bm25Event::Parent(None) => {
                    // The parent's leg closed: gone, or done sending.
                    // The children finish on their own unless the
                    // response side is closed too, which `tx.closed()`
                    // in the caller turns into a cancel.
                    parent_open = false;
                }
                Bm25Event::Child(_, Err(status)) => break Err(status),
                Bm25Event::Child(shard, Ok(None)) => {
                    if completions[shard].is_none() {
                        break Err(Status::data_loss(format!(
                            "relay child {shard} ({}): BM25 stream ended without a completion \
                             certificate",
                            children[shard]
                        )));
                    }
                }
                Bm25Event::Child(shard, Ok(Some(message))) => {
                    if completions[shard].is_some() {
                        break Err(Status::internal(format!(
                            "relay child {shard} ({}): message after its completion",
                            children[shard]
                        )));
                    }
                    match message.payload {
                        Some(bm25_query_stream_response::Payload::CandidateBatch(batch)) => {
                            if batch.candidates.len() % 12 != 0 {
                                break Err(Status::data_loss(format!(
                                    "relay child {shard} ({}): BM25 candidate batch has {} \
                                     bytes, not 12-byte records",
                                    children[shard],
                                    batch.candidates.len()
                                )));
                            }
                            forwarded += (batch.candidates.len() / 12) as u64;
                            if tx
                                .send(Ok(Bm25QueryStreamResponse {
                                    payload: Some(
                                        bm25_query_stream_response::Payload::CandidateBatch(batch),
                                    ),
                                }))
                                .await
                                .is_err()
                            {
                                break Err(Status::cancelled("relay: parent response closed"));
                            }
                        }
                        Some(bm25_query_stream_response::Payload::FloorUpdate(u)) => {
                            if !u.floor.is_nan() && u.floor > last_up {
                                last_up = u.floor;
                                if tx
                                    .send(Ok(Bm25QueryStreamResponse {
                                        payload: Some(
                                            bm25_query_stream_response::Payload::FloorUpdate(u),
                                        ),
                                    }))
                                    .await
                                    .is_err()
                                {
                                    break Err(Status::cancelled("relay: parent response closed"));
                                }
                            }
                        }
                        Some(bm25_query_stream_response::Payload::Completion(completion)) => {
                            if completion.scoring_fingerprint.is_empty() {
                                break Err(Status::data_loss(format!(
                                    "relay child {shard} ({}): BM25 completion omitted its \
                                     scoring fingerprint",
                                    children[shard]
                                )));
                            }
                            match fingerprint.as_ref() {
                                Some(seen) if seen != &completion.scoring_fingerprint => {
                                    break Err(Status::failed_precondition(format!(
                                        "relay child {shard} ({}): scoring fingerprint {} \
                                         differs from {seen}; one relay serves one score space",
                                        children[shard], completion.scoring_fingerprint
                                    )));
                                }
                                None => fingerprint = Some(completion.scoring_fingerprint.clone()),
                                _ => {}
                            }
                            completions[shard] = Some(completion);
                            legs[shard] = None;
                            remaining -= 1;
                        }
                        Some(bm25_query_stream_response::Payload::Done(_)) => {
                            break Err(Status::failed_precondition(format!(
                                "relay child {shard} ({}): BM25 stream used the obsolete \
                                 uncertified terminal response",
                                children[shard]
                            )));
                        }
                        None => {}
                    }
                }
            }
        };
        let completion = match outcome {
            Ok(Some(incomplete)) => incomplete,
            Ok(None) => {
                self.still_current(&pinned, "Bm25QueryStream")?;
                let mut completed = true;
                let mut emitted: u64 = 0;
                let mut responses = Vec::with_capacity(n);
                for (shard, completion) in completions.into_iter().enumerate() {
                    let completion = completion.expect("remaining reached zero");
                    completed &= completion.completed;
                    emitted = emitted
                        .checked_add(completion.candidates_emitted)
                        .ok_or_else(|| {
                            Status::internal("relay: candidates_emitted overflows u64")
                        })?;
                    match completion.response {
                        Some(response) => responses.push(response),
                        None if completion.completed => {
                            return Err(Status::data_loss(format!(
                                "relay child {shard} ({}): BM25 completion omitted its response",
                                children[shard]
                            )));
                        }
                        None => {}
                    }
                }
                if emitted != forwarded {
                    return Err(Status::data_loss(format!(
                        "relay: children certified {emitted} candidates but {forwarded} were \
                         forwarded"
                    )));
                }
                let response = if completed {
                    Some(merge_bm25_responses(&req, responses)?)
                } else {
                    None
                };
                Bm25StreamCompletion {
                    completed,
                    response,
                    scoring_fingerprint: fingerprint.unwrap_or_default(),
                    candidates_emitted: forwarded,
                }
            }
            Err(status) => {
                stop_children(&mut legs).await;
                for reader in &readers {
                    reader.abort();
                }
                return Err(status);
            }
        };
        for reader in &readers {
            reader.abort();
        }
        tx.send(Ok(Bm25QueryStreamResponse {
            payload: Some(bm25_query_stream_response::Payload::Completion(completion)),
        }))
        .await
        .map_err(|_| Status::cancelled("relay: parent response closed"))
    }
}

// --- The vector-side routes, the bitmap routes, and the dictionaries ---

/// The known-column accumulator of one fan-out at this level: geo flags
/// ORed, filter-leaf flags ORed over the request's tree with each
/// child's answer mapped through the leaves its placement code implied
/// away (`ShardMask::implied`: the child was sent a shorter tree and
/// answers in that order), and the leaves the mask itself resolved
/// marked from the start (`ShardMask::known`).
struct KnownFlags {
    geo: Vec<bool>,
    tree: Vec<bool>,
}

impl KnownFlags {
    fn new(filters: &RequestFilters, mask: Option<&crate::placement::ShardMask>) -> Self {
        let leaves = filters.tree.as_ref().map_or(0, crate::filter::leaf_count);
        let mut tree = vec![false; leaves];
        if let Some(mask) = mask {
            for &leaf in &mask.known {
                if let Some(flag) = tree.get_mut(leaf) {
                    *flag = true;
                }
            }
        }
        KnownFlags {
            geo: vec![false; filters.geo.len()],
            tree,
        }
    }

    fn merge(
        &mut self,
        child: usize,
        mask: Option<&crate::placement::ShardMask>,
        geo: &[bool],
        tree: &[bool],
    ) -> Result<(), Status> {
        if geo.len() != self.geo.len() {
            return Err(Status::internal(format!(
                "relay: child {child} answered {} geo-column flags for {} geo filters",
                geo.len(),
                self.geo.len()
            )));
        }
        for (acc, k) in self.geo.iter_mut().zip(geo) {
            *acc |= *k;
        }
        let dropped: &[usize] = mask
            .and_then(|m| m.implied.get(child))
            .map_or(&[], Vec::as_slice);
        let kept: Vec<usize> = (0..self.tree.len())
            .filter(|index| !dropped.contains(index))
            .collect();
        if tree.len() != kept.len() {
            return Err(Status::internal(format!(
                "relay: child {child} answered {} filter-leaf flags for the {} leaves it was \
                 sent (of {})",
                tree.len(),
                kept.len(),
                self.tree.len()
            )));
        }
        for (&index, k) in kept.iter().zip(tree) {
            self.tree[index] |= *k;
        }
        Ok(())
    }
}

/// The slot range of each child, `(offset, span)`, from the children's
/// health reports, with the contiguity rule applied: a gap or an overlap
/// refuses by name, because an id routed by these ranges, or a bitmap
/// laid out over them, would otherwise be wrong without a word.
fn child_ranges(
    collection: &str,
    children: &[String],
    reports: &[HealthResponse],
) -> Result<Vec<(u64, u64)>, Status> {
    merge_health(collection, children, reports)?;
    Ok(reports
        .iter()
        .map(|r| (r.slot_offset, health_span(r)))
        .collect())
}

/// Sort ids into the child whose slot range holds each. An id in no
/// child's range is another shard's and is dropped, which is what a
/// node does with an id outside its own range (the boolean planner and
/// the exact rerank send every shard every candidate; the cascade routes
/// by shard, and a relay is one shard to it).
fn route_ids(ranges: &[(u64, u64)], ids: &[u64]) -> Result<Vec<Vec<u64>>, Status> {
    let mut by_child: Vec<Vec<u64>> = vec![Vec::new(); ranges.len()];
    for &id in ids {
        if let Some(child) = child_of_id(ranges, id) {
            by_child[child].push(id);
        }
    }
    Ok(by_child)
}

/// Overflow-checked `u64` sum for a counter the children report.
fn add_count(acc: &mut u64, share: u64, child: usize, what: &str) -> Result<(), Status> {
    *acc = acc.checked_add(share).ok_or_else(|| {
        Status::failed_precondition(format!("relay: child {child} overflows the summed {what}"))
    })?;
    Ok(())
}

/// Overflow-checked `u32` sum for a counter the children report.
fn add_count32(acc: &mut u32, share: u32, child: usize, what: &str) -> Result<(), Status> {
    *acc = acc.checked_add(share).ok_or_else(|| {
        Status::failed_precondition(format!("relay: child {child} overflows the summed {what}"))
    })?;
    Ok(())
}

/// One child's packed bitmap as it came back, or nothing for a child the
/// placement mask skipped (it contributes no member).
pub struct ChildBitmap {
    pub base_label: u64,
    pub label_count: u64,
    pub bits: Vec<u8>,
}

/// The children's bitmaps laid over the relay's one slot range: child
/// `i`'s bits go at `offset_i - base` (its slot offset, which its
/// `base_label` must equal), every range checked for its declared
/// length and for zero padding, the relay's `label_count` running to
/// the end of the last labelled child. Between a child's last label and
/// the next child's first slot the bits are zero, which is what those
/// unfilled slots hold on a node too. Returns `(base_label, label_count,
/// bits)`.
pub fn concat_bitmaps(
    children: &[String],
    ranges: &[(u64, u64)],
    shares: &[Option<ChildBitmap>],
) -> Result<(u64, u64, Vec<u8>), Status> {
    let base = ranges
        .iter()
        .map(|&(offset, _)| offset)
        .min()
        .ok_or_else(|| Status::failed_precondition("relay: a relay coordinator has no children"))?;
    let name = |child: usize| children.get(child).map_or("", String::as_str);
    let mut total: u64 = 0;
    for (child, share) in shares.iter().enumerate() {
        let Some(share) = share else {
            continue;
        };
        let (offset, span) = ranges[child];
        if share.base_label != offset {
            return Err(Status::failed_precondition(format!(
                "relay: child {child} ({}) answered a bitmap based at label {} while its slot \
                 range starts at {offset}",
                name(child),
                share.base_label
            )));
        }
        if share.label_count > span {
            return Err(Status::failed_precondition(format!(
                "relay: child {child} ({}) answered {} labels over a slot range of {span}",
                name(child),
                share.label_count
            )));
        }
        let expected = usize::try_from(share.label_count.div_ceil(8)).map_err(|_| {
            Status::resource_exhausted("relay: membership bitmap does not fit usize")
        })?;
        if share.bits.len() != expected {
            return Err(Status::internal(format!(
                "relay: child {child} ({}) answered {} bitmap bytes for {} labels; expected \
                 {expected}",
                name(child),
                share.bits.len(),
                share.label_count
            )));
        }
        if !share.label_count.is_multiple_of(8)
            && share.bits.last().is_some_and(|last| {
                let used = (share.label_count % 8) as u8;
                *last & !((1u8 << used) - 1) != 0
            })
        {
            return Err(Status::internal(format!(
                "relay: child {child} ({}) sets padding bits beyond its label count",
                name(child)
            )));
        }
        let end = (offset - base)
            .checked_add(share.label_count)
            .ok_or_else(|| Status::internal("relay: membership label range overflows u64"))?;
        total = total.max(end);
    }
    let bytes = usize::try_from(total.div_ceil(8)).map_err(|_| {
        Status::resource_exhausted("relay: merged membership bitmap does not fit usize")
    })?;
    let mut bits = vec![0u8; bytes];
    for (child, share) in shares.iter().enumerate() {
        let Some(share) = share else {
            continue;
        };
        let at = usize::try_from(ranges[child].0 - base)
            .map_err(|_| Status::resource_exhausted("relay: bitmap offset does not fit usize"))?;
        let count = usize::try_from(share.label_count).map_err(|_| {
            Status::resource_exhausted("relay: bitmap label count does not fit usize")
        })?;
        or_bits(&mut bits, at, &share.bits, count);
    }
    Ok((base, total, bits))
}

/// OR `count` bits of `src` (LSB-first per byte, the wire packing) into
/// `dst` starting at bit `at`. Byte-aligned placements copy whole bytes;
/// the rest walk the set bits.
fn or_bits(dst: &mut [u8], at: usize, src: &[u8], count: usize) {
    if at.is_multiple_of(8) {
        let first = at / 8;
        for (i, byte) in src.iter().enumerate() {
            dst[first + i] |= byte;
        }
        return;
    }
    for (byte_index, byte) in src.iter().copied().enumerate() {
        let mut held = byte;
        while held != 0 {
            let bit = held.trailing_zeros() as usize;
            let position = byte_index * 8 + bit;
            if position < count {
                let target = at + position;
                dst[target / 8] |= 1 << (target % 8);
            }
            held &= held - 1;
        }
    }
}

/// The children's prefix expansions as one dictionary's: the union in
/// byte order and its exact size when every child stayed within the
/// cap; a child past the cap answers no terms and a count above it, and
/// the relay then answers the largest such count (a lower bound on the
/// subtree's, enough for the root's refusal) and no terms, as a node
/// past the cap does. `known` when any child knows the field.
pub fn merge_prefix_expansions(
    cap: u32,
    shares: &[crate::pb::ExpandTermPrefixResponse],
) -> crate::pb::ExpandTermPrefixResponse {
    let mut union = std::collections::BTreeSet::new();
    let mut known = false;
    let mut past_cap: u64 = 0;
    for share in shares {
        if !share.known {
            continue;
        }
        known = true;
        if share.count > u64::from(cap) {
            past_cap = past_cap.max(share.count);
            continue;
        }
        union.extend(share.terms.iter().cloned());
    }
    if past_cap > 0 {
        return crate::pb::ExpandTermPrefixResponse {
            terms: Vec::new(),
            count: past_cap,
            known,
            visibility_fingerprint: Vec::new(),
            visibility_columns_known: Vec::new(),
        };
    }
    let count = union.len() as u64;
    crate::pb::ExpandTermPrefixResponse {
        terms: if count > u64::from(cap) {
            Vec::new()
        } else {
            union.into_iter().collect()
        },
        count,
        known,
        visibility_fingerprint: Vec::new(),
        visibility_columns_known: Vec::new(),
    }
}

/// The children's dictionary scans as one: entries unioned in byte order
/// with each term's df summed (checked), the tombstone count summed, the
/// exact union size as the count within the bound; a child past the scan
/// bound makes the relay answer the largest such count and no entries,
/// as a node past the bound does.
pub fn merge_suggest_terms(
    max_scan: u64,
    shares: &[crate::pb::SuggestTermsResponse],
) -> Result<crate::pb::SuggestTermsResponse, Status> {
    let mut union: std::collections::BTreeMap<String, u64> = std::collections::BTreeMap::new();
    let mut known = false;
    let mut tombstoned: u64 = 0;
    let mut past_bound: u64 = 0;
    for (child, share) in shares.iter().enumerate() {
        add_count(
            &mut tombstoned,
            share.tombstoned_rows,
            child,
            "tombstone count",
        )?;
        if !share.known {
            continue;
        }
        known = true;
        if share.count > max_scan {
            past_bound = past_bound.max(share.count);
            continue;
        }
        for entry in &share.entries {
            let df = union.entry(entry.term.clone()).or_insert(0);
            add_count(df, entry.df, child, "posting frequency")?;
        }
    }
    if past_bound > 0 {
        return Ok(crate::pb::SuggestTermsResponse {
            entries: Vec::new(),
            count: past_bound,
            known,
            visibility_fingerprint: Vec::new(),
            visibility_columns_known: Vec::new(),
            tombstoned_rows: tombstoned,
        });
    }
    let count = union.len() as u64;
    Ok(crate::pb::SuggestTermsResponse {
        entries: if count > max_scan {
            Vec::new()
        } else {
            union
                .into_iter()
                .map(|(term, df)| crate::pb::SuggestTermEntry { term, df })
                .collect()
        },
        count,
        known,
        visibility_fingerprint: Vec::new(),
        visibility_columns_known: Vec::new(),
        tombstoned_rows: tombstoned,
    })
}

/// The children's layouts as the one shard the root lists
/// (`docs/diagnostics.md`): counts summed, the children's segments
/// concatenated with the child index in front of each segment id, the
/// knobs true only when every child has them, one placement code when
/// the children agree and `placement_mixed` when they do not, and a
/// child whose diagnostics are unserved named in `layout`. The root
/// relabels `shard` and `address` with the relay's.
pub fn merge_shard_layouts(
    children: &[String],
    layouts: &[crate::pb::ShardLayoutDiagnostics],
) -> crate::pb::ShardLayoutDiagnostics {
    let mut merged = crate::pb::ShardLayoutDiagnostics {
        layout: format!("relay over {} children", layouts.len()),
        segment_pruning: !layouts.is_empty(),
        floor_sharing: !layouts.is_empty(),
        ..Default::default()
    };
    let mut notes = Vec::new();
    let mut partition: Option<String> = None;
    let mut fingerprint: Option<String> = None;
    let mut placement: Option<(bool, u64)> = None;
    for (child, layout) in layouts.iter().enumerate() {
        let addr = children.get(child).map_or("", String::as_str);
        let served = layout.layout == "segments" || layout.layout == "single-image";
        if !served {
            notes.push(format!(
                "child {child} ({addr}) unserved: {}",
                layout.layout
            ));
        }
        merged.rows = merged.rows.saturating_add(layout.rows);
        merged.live_rows = merged.live_rows.saturating_add(layout.live_rows);
        merged.tombstones = merged.tombstones.saturating_add(layout.tombstones);
        merged.tail_rows = merged.tail_rows.saturating_add(layout.tail_rows);
        merged.catalog_epoch = merged.catalog_epoch.max(layout.catalog_epoch);
        merged.segment_pruning &= layout.segment_pruning;
        merged.floor_sharing &= layout.floor_sharing;
        for segment in &layout.segments {
            let mut segment = segment.clone();
            segment.segment_id = format!("child{child}:{}", segment.segment_id);
            merged.segments.push(segment);
        }
        if served {
            match partition.as_ref() {
                None => partition = Some(layout.partition_key.clone()),
                Some(key) if *key != layout.partition_key => {
                    notes.push(format!(
                        "child {child} ({addr}) partition key {:?} differs from {key:?}",
                        layout.partition_key
                    ));
                }
                _ => {}
            }
            match fingerprint.as_ref() {
                None => fingerprint = Some(layout.scoring_fingerprint.clone()),
                Some(seen) if *seen != layout.scoring_fingerprint => {
                    notes.push(format!(
                        "child {child} ({addr}) scoring fingerprint {} differs from {seen}",
                        layout.scoring_fingerprint
                    ));
                }
                _ => {}
            }
            merged.placement_mixed |= layout.placement_mixed;
            match placement {
                None => placement = Some((layout.has_placement, layout.placement)),
                Some(first) if first != (layout.has_placement, layout.placement) => {
                    merged.placement_mixed = true;
                }
                _ => {}
            }
        }
    }
    merged.partition_key = partition.unwrap_or_default();
    merged.scoring_fingerprint = fingerprint.unwrap_or_default();
    if let Some((has_placement, code)) = placement {
        merged.has_placement = has_placement;
        merged.placement = code;
    }
    if !notes.is_empty() {
        merged.layout = format!("{}; {}", merged.layout, notes.join("; "));
    }
    merged
}

/// The children's stream and the parent's leg of one relayed
/// `SearchShard`, dropped together: a return on any path closes the
/// children's request channels and stops the readers.
struct ShardLegs {
    senders: Vec<Option<mpsc::Sender<SearchShardRequest>>>,
    readers: Vec<tokio::task::JoinHandle<()>>,
    parent: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for ShardLegs {
    fn drop(&mut self) {
        for reader in &self.readers {
            reader.abort();
        }
        if let Some(parent) = &self.parent {
            parent.abort();
        }
    }
}

enum ShardEvent {
    Child(usize, Result<Option<SearchShardResponse>, Status>),
    Parent(Option<Result<SearchShardRequest, Status>>),
}

fn shard_floor(floor: f32) -> SearchShardRequest {
    SearchShardRequest {
        payload: Some(search_shard_request::Payload::FloorUpdate(FloorUpdate {
            floor,
        })),
    }
}

/// The forwarding loop of one relayed `SearchShard`: one child stream
/// per child the placement mask keeps, the parent's floor raises
/// forwarded to every child, each child's raises forwarded to the parent
/// (a child's k-th best is a lower bound on the relay's, so the parent's
/// running maximum only tightens), and the children's terminal lists
/// concatenated in score order. The relay keeps no heap: the parent's
/// merge picks the top-k from the union exactly as it does over leaves,
/// and with `tie_complete` the union holds every child's boundary tie
/// group, so the cascade's score-defined pool is the same set.
#[allow(clippy::too_many_arguments)]
async fn relay_shard_search(
    coordinator: CoordinatorServiceImpl,
    pinned: MapSnapshot,
    mut map_changes: watch::Receiver<u64>,
    start: crate::pb::StartShardSearch,
    filters: RequestFilters,
    mut inbound: Streaming<SearchShardRequest>,
    tx: mpsc::Sender<Result<SearchShardResponse, Status>>,
    deadline: Option<tokio::time::Instant>,
) -> Result<(), Status> {
    map_changes.mark_unchanged();
    if *map_changes.borrow() != pinned.control_revision {
        map_changes.mark_changed();
    }
    let children = coordinator.node_addresses().to_vec();
    let n = children.len();
    if n == 0 {
        return Err(Status::failed_precondition(
            "relay: a relay coordinator has no children",
        ));
    }
    let mask = coordinator.shard_mask(filters.tree.as_ref());
    let mut known = KnownFlags::new(&filters, mask.as_ref());
    let (event_tx, mut events) = mpsc::channel::<ShardEvent>(64);
    let mut legs = ShardLegs {
        senders: vec![None; n],
        readers: Vec::with_capacity(n),
        parent: None,
    };
    let mut remaining = 0;
    for (child, addr) in children.iter().enumerate() {
        if mask.as_ref().is_some_and(|m| m.skipped[child]) {
            continue;
        }
        remaining += 1;
        let (req_tx, req_rx) = mpsc::channel::<SearchShardRequest>(8);
        let child_start = crate::pb::StartShardSearch {
            read_context: None,
            request_id: start.request_id.clone(),
            k: start.k,
            vector: start.vector.clone(),
            tie_complete: start.tie_complete,
            collapse_parents: start.collapse_parents,
            geo_filters: filters.geo.clone(),
            filter: CoordinatorServiceImpl::shard_filter_tree(&filters, mask.as_ref(), child),
        };
        req_tx
            .try_send(SearchShardRequest {
                payload: Some(search_shard_request::Payload::Start(child_start)),
            })
            .map_err(|_| Status::internal("relay: child request channel refused the Start"))?;
        legs.senders[child] = Some(req_tx);
        let mut link = coordinator.node_client(addr)?;
        let addr = addr.clone();
        let events = event_tx.clone();
        legs.readers.push(tokio::spawn(async move {
            let mut request = Request::new(ReceiverStream::new(req_rx));
            if let Some(at) = deadline {
                request.set_timeout(at.saturating_duration_since(tokio::time::Instant::now()));
            }
            let mut stream = match link.search_shard(request).await {
                Ok(response) => response.into_inner(),
                Err(status) => {
                    let _ = events
                        .send(ShardEvent::Child(
                            child,
                            Err(child_error(child, &addr, "shard search", status)),
                        ))
                        .await;
                    return;
                }
            };
            loop {
                let message = stream.message().await;
                let terminal = !matches!(message, Ok(Some(_)))
                    || matches!(
                        message,
                        Ok(Some(SearchShardResponse {
                            payload: Some(search_shard_response::Payload::Done(_)),
                        }))
                    );
                let message =
                    message.map_err(|status| child_error(child, &addr, "shard search", status));
                if events
                    .send(ShardEvent::Child(child, message))
                    .await
                    .is_err()
                    || terminal
                {
                    return;
                }
            }
        }));
    }
    {
        let events = event_tx;
        legs.parent = Some(tokio::spawn(async move {
            loop {
                let message = inbound.message().await;
                let terminal = !matches!(message, Ok(Some(_)));
                let event = match message {
                    Ok(Some(message)) => ShardEvent::Parent(Some(Ok(message))),
                    Ok(None) => ShardEvent::Parent(None),
                    Err(status) => ShardEvent::Parent(Some(Err(status))),
                };
                if events.send(event).await.is_err() || terminal {
                    return;
                }
            }
        }));
    }
    let mut hits: Vec<crate::pb::ScoredHit> = Vec::new();
    let mut stats: Option<crate::pb::ShardScanStats> = None;
    let mut done = vec![false; n];
    let mut sleep = deadline.map(|at| Box::pin(tokio::time::sleep_until(at)));
    while remaining > 0 {
        let event = tokio::select! {
            biased;
            _ = async { sleep.as_mut().expect("guarded").await }, if sleep.is_some() => {
                return Err(Status::deadline_exceeded(
                    "relay: the parent's deadline passed before every child completed",
                ));
            }
            moved = map_changes.changed() => match moved {
                Ok(()) => {
                    let revision = *map_changes.borrow_and_update();
                    if revision == pinned.control_revision {
                        continue;
                    }
                    return Err(Status::failed_precondition(format!(
                        "relay: the shard map moved from revision {} (generation {}) to \
                         revision {revision} during SearchShard; the scan was opened under the \
                         older map and is not completed; retry under the current one",
                        pinned.control_revision, pinned.topology_generation
                    )));
                }
                Err(_) => {
                    map_changes = watch::channel(pinned.control_revision).1;
                    continue;
                }
            },
            event = events.recv() => match event {
                Some(event) => event,
                None => {
                    return Err(Status::internal(
                        "relay: the child readers ended before every child completed",
                    ))
                }
            },
        };
        match event {
            ShardEvent::Parent(Some(Ok(SearchShardRequest {
                payload: Some(search_shard_request::Payload::FloorUpdate(update)),
            }))) => {
                if update.floor.is_nan() {
                    continue;
                }
                for sender in legs.senders.iter().flatten() {
                    // Floors are monotone: a raise a full channel drops is
                    // superseded by the next, and a closed child is done.
                    let _ = sender.try_send(shard_floor(update.floor));
                }
            }
            ShardEvent::Parent(Some(Ok(SearchShardRequest {
                payload: Some(search_shard_request::Payload::Start(_)),
            }))) => {
                return Err(Status::invalid_argument(
                    "relay: a stream carries one StartShardSearch; another arrived",
                ));
            }
            ShardEvent::Parent(Some(Ok(_))) | ShardEvent::Parent(None) => {}
            ShardEvent::Parent(Some(Err(status))) => {
                return Err(Status::cancelled(format!(
                    "relay: the parent's request leg failed: {}",
                    status.message()
                )));
            }
            ShardEvent::Child(
                _,
                Ok(Some(SearchShardResponse {
                    payload: Some(search_shard_response::Payload::FloorUpdate(update)),
                })),
            ) => {
                if tx
                    .send(Ok(SearchShardResponse {
                        payload: Some(search_shard_response::Payload::FloorUpdate(update)),
                    }))
                    .await
                    .is_err()
                {
                    // The parent is gone: nobody reads what the children
                    // still scan for.
                    return Ok(());
                }
            }
            ShardEvent::Child(
                child,
                Ok(Some(SearchShardResponse {
                    payload: Some(search_shard_response::Payload::Done(share)),
                })),
            ) => {
                if done[child] {
                    return Err(Status::internal(format!(
                        "relay: child {child} ({}) sent a terminal message twice",
                        children[child]
                    )));
                }
                done[child] = true;
                remaining -= 1;
                known.merge(
                    child,
                    mask.as_ref(),
                    &share.geo_columns_known,
                    &share.filter_columns_known,
                )?;
                if let Some(share) = share.stats {
                    let acc = stats.get_or_insert_with(Default::default);
                    add_count32(
                        &mut acc.chunk_calls,
                        share.chunk_calls,
                        child,
                        "chunk calls",
                    )?;
                    add_count(
                        &mut acc.candidates_collected,
                        share.candidates_collected,
                        child,
                        "candidate count",
                    )?;
                    add_count(
                        &mut acc.floors_published,
                        share.floors_published,
                        child,
                        "published floor count",
                    )?;
                    add_count(
                        &mut acc.floor_updates_applied,
                        share.floor_updates_applied,
                        child,
                        "applied floor count",
                    )?;
                    add_count(
                        &mut acc.floors_offered,
                        share.floors_offered,
                        child,
                        "offered floor count",
                    )?;
                    add_count32(
                        &mut acc.segments_total,
                        share.segments_total,
                        child,
                        "segment count",
                    )?;
                    add_count32(
                        &mut acc.segments_skipped,
                        share.segments_skipped,
                        child,
                        "skipped segment count",
                    )?;
                }
                hits.extend(share.hits);
            }
            ShardEvent::Child(_, Ok(Some(_))) => {}
            ShardEvent::Child(child, Ok(None)) => {
                if !done[child] {
                    return Err(Status::data_loss(format!(
                        "relay: child {child} ({}) closed its stream before Done",
                        children[child]
                    )));
                }
            }
            ShardEvent::Child(_, Err(status)) => return Err(status),
        }
    }
    if *map_changes.borrow() != pinned.control_revision {
        return Err(Status::failed_precondition(format!(
            "relay: the shard map moved from revision {} (generation {}) during SearchShard; \
             retry under the current one",
            pinned.control_revision, pinned.topology_generation
        )));
    }
    hits.sort_by(|a, b| {
        b.score
            .total_cmp(&a.score)
            .then_with(|| a.vector_id.cmp(&b.vector_id))
    });
    let _ = tx
        .send(Ok(SearchShardResponse {
            payload: Some(search_shard_response::Payload::Done(
                crate::pb::SearchShardDone {
                    hits,
                    stats,
                    geo_columns_known: known.geo,
                    filter_columns_known: known.tree,
                },
            )),
        }))
        .await;
    Ok(())
}

#[tonic::async_trait]
impl NodeService for RelayService {
    type SearchShardStream =
        crate::metrics::Timed<ReceiverStream<Result<SearchShardResponse, Status>>>;
    type StreamSearchStream =
        crate::metrics::Timed<ReceiverStream<Result<StreamSearchResponse, Status>>>;
    type Bm25QueryStreamStream =
        crate::metrics::Timed<ReceiverStream<Result<Bm25QueryStreamResponse, Status>>>;
    type ReadWalStream = ReceiverStream<Result<crate::pb::ReadWalResponse, Status>>;
    type StreamSnapshotStream = ReceiverStream<Result<crate::pb::SnapshotChunk, Status>>;

    async fn stream_search(
        &self,
        request: Request<Streaming<StreamSearchRequest>>,
    ) -> Result<Response<Self::StreamSearchStream>, Status> {
        crate::metrics::timed_stream(Route::StreamSearch, request, |request| async move {
            let deadline =
                grpc_timeout(request.metadata()).map(|d| tokio::time::Instant::now() + d);
            let mut inbound = request.into_inner();
            let start = match inbound.message().await? {
                Some(StreamSearchRequest {
                    payload: Some(stream_search_request::Payload::Start(start)),
                }) => start,
                _ => {
                    return Err(Status::invalid_argument(
                        "first StreamSearchRequest must be StartStreamSearch",
                    ))
                }
            };
            if start.read_context.is_some() {
                return Err(Status::failed_precondition(
                    "relay does not yet compose scoped vector read receipts",
                ));
            }
            if start.initial_floor.is_some_and(f32::is_nan) {
                return Err(Status::invalid_argument("initial_floor must not be NaN"));
            }
            crate::node::validate_geo_filters(&start.geo_filters)?;
            if let Some(f) = start.filter.as_ref() {
                crate::filter::validate_filter(f)?;
            }
            let filters = RequestFilters {
                geo: start.geo_filters.clone(),
                tree: start.filter.clone(),
            };
            let signals = Arc::new(RelaySignals::new(
                start.initial_floor.unwrap_or(f32::NEG_INFINITY),
            ));
            let token = (start.floor_token != 0).then_some(start.floor_token);
            if let Some(token) = token {
                self.signals
                    .lock()
                    .expect("relay stream signal registry poisoned")
                    .insert(token, Arc::clone(&signals));
            }
            // The parent's gRPC leg: floor raises and the authoritative
            // Stop. A closed leg before the summary means the parent is
            // gone, and the children are told so.
            let pump = Arc::clone(&signals);
            let identity_enabled = start.identity_limits.is_some();
            let identity_ready = Arc::new(AtomicBool::new(false));
            let pump_ready = Arc::clone(&identity_ready);
            let input = tokio::spawn(async move {
                loop {
                    match inbound.message().await {
                        Ok(Some(StreamSearchRequest {
                            payload: Some(stream_search_request::Payload::FloorUpdate(u)),
                        })) => pump.raise(u.floor),
                        Ok(Some(StreamSearchRequest {
                            payload: Some(stream_search_request::Payload::Stop(_)),
                        })) => {
                            pump.cancel();
                            return Ok(None);
                        }
                        Ok(Some(StreamSearchRequest {
                            payload:
                                Some(stream_search_request::Payload::ResolveIdentities(selection)),
                        })) => {
                            if !identity_enabled || !pump_ready.load(Ordering::Acquire) {
                                pump.cancel();
                                return Err(Status::failed_precondition(
                                    "relay: identity selection requires IdentityReady",
                                ));
                            }
                            return Ok(Some(selection));
                        }
                        Ok(Some(_)) => {}
                        Ok(None) | Err(_) => {
                            if identity_enabled {
                                pump.cancel();
                            }
                            return Ok(None);
                        }
                    }
                }
            });
            let (tx, rx) = mpsc::channel::<Result<StreamSearchResponse, Status>>(64);
            let (pinned, frozen) = self.pin();
            let map_changes = self.inner.map.changes();
            let registry = Arc::clone(&self.signals);
            let loop_signals = Arc::clone(&signals);
            let input_abort = input.abort_handle();
            tokio::spawn(async move {
                let work = relay_stream(
                    frozen,
                    pinned,
                    map_changes,
                    start,
                    filters,
                    loop_signals,
                    tx.clone(),
                    deadline,
                    input,
                    identity_ready,
                );
                let result = tokio::select! {
                    _ = tx.closed() => Err(Status::cancelled("relay: parent response closed")),
                    _ = async { tokio::time::sleep_until(deadline.expect("guarded")).await }, if deadline.is_some() =>
                        Err(Status::deadline_exceeded("relay: parent deadline passed")),
                    result = work => result,
                };
                input_abort.abort();
                if let Some(token) = token {
                    registry
                        .lock()
                        .expect("relay stream signal registry poisoned")
                        .remove(&token);
                }
                if let Err(status) = result {
                    let _ = tokio::time::timeout(Duration::from_millis(250), tx.send(Err(status))).await;
                }
            });
            Ok(Response::new(ReceiverStream::new(rx)))
        })
        .await
    }

    async fn term_stats(
        &self,
        request: Request<TermStatsRequest>,
    ) -> Result<Response<TermStatsResponse>, Status> {
        crate::metrics::timed(Route::TermStats, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            crate::visibility::validate_stats_request(&req)?;
            crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
            let (pinned, frozen) = self.pin();
            let children = frozen.node_addresses().to_vec();
            if children.is_empty() {
                return Err(Status::failed_precondition(
                    "relay: a relay coordinator has no children",
                ));
            }
            let mut tasks = Vec::with_capacity(children.len());
            for (shard, addr) in children.iter().enumerate() {
                let mut link = frozen.node_client(addr)?;
                let req = req.clone();
                let addr = addr.clone();
                tasks.push(tokio::spawn(async move {
                    let mut request = Request::new(req);
                    if let Some(timeout) = timeout {
                        request.set_timeout(timeout);
                    }
                    link.term_stats(request)
                        .await
                        .map(|r| r.into_inner())
                        .map_err(|status| {
                            Status::new(
                                status.code(),
                                format!(
                                    "relay child {shard} ({addr}) term stats: {}",
                                    status.message()
                                ),
                            )
                        })
                }));
            }
            let mut shares = Vec::with_capacity(children.len());
            for task in tasks {
                shares.push(
                    task.await
                        .map_err(|e| Status::internal(format!("relay term stats task: {e}")))??,
                );
            }
            let mut merged = merge_term_stats(&req, &shares)?;
            merged.stats_incarnation = self.stats_incarnation.bytes()?;
            self.still_current(&pinned, "TermStats")?;
            let tuple = TokenTuple {
                collection: self.collection.clone(),
                control_revision: pinned.control_revision,
                topology_generation: pinned.topology_generation,
                children,
                epochs: shares
                    .iter()
                    .map(|s| StatsClaim::required(s.stats_epoch, &s.stats_incarnation))
                    .collect::<Result<_, _>>()?,
            };
            merged.stats_epoch = self
                .tokens
                .lock()
                .expect("relay token registry poisoned")
                .allocate(tuple)?;
            Ok(Response::new(merged))
        })
        .await
    }

    async fn health(
        &self,
        request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        let timeout = grpc_timeout(request.metadata());
        let (pinned, frozen) = self.pin();
        let reports = self.children_health(&frozen, timeout).await?;
        let merged = merge_health(&self.collection, frozen.node_addresses(), &reports)?;
        self.still_current(&pinned, "Health")?;
        Ok(Response::new(merged))
    }

    async fn search_shard(
        &self,
        request: Request<Streaming<SearchShardRequest>>,
    ) -> Result<Response<Self::SearchShardStream>, Status> {
        crate::metrics::timed_stream(Route::SearchShard, request, |request| async move {
            let deadline =
                grpc_timeout(request.metadata()).map(|d| tokio::time::Instant::now() + d);
            let mut inbound = request.into_inner();
            let start = match inbound.message().await? {
                Some(SearchShardRequest {
                    payload: Some(search_shard_request::Payload::Start(start)),
                }) => start,
                _ => {
                    return Err(Status::invalid_argument(
                        "first SearchShardRequest must be StartShardSearch",
                    ))
                }
            };
            if start.read_context.is_some() {
                return Err(Status::failed_precondition(
                    "relay: scoped vector scans require child read receipt composition",
                ));
            }
            crate::node::validate_geo_filters(&start.geo_filters)?;
            if let Some(f) = start.filter.as_ref() {
                crate::filter::validate_filter(f)?;
            }
            let filters = RequestFilters {
                geo: start.geo_filters.clone(),
                tree: start.filter.clone(),
            };
            let (tx, rx) = mpsc::channel::<Result<SearchShardResponse, Status>>(64);
            let (pinned, frozen) = self.pin();
            let map_changes = self.inner.map.changes();
            tokio::spawn(async move {
                let work = relay_shard_search(
                    frozen,
                    pinned,
                    map_changes,
                    start,
                    filters,
                    inbound,
                    tx.clone(),
                    deadline,
                );
                let result = tokio::select! {
                    _ = tx.closed() => Err(Status::cancelled("relay: parent response closed")),
                    result = work => result,
                };
                if let Err(status) = result {
                    let _ = tokio::time::timeout(Duration::from_millis(250), tx.send(Err(status)))
                        .await;
                }
            });
            Ok(Response::new(ReceiverStream::new(rx)))
        })
        .await
    }

    async fn add_vectors(
        &self,
        _request: Request<Streaming<crate::pb::AddVectorsRequest>>,
    ) -> Result<Response<crate::pb::AddVectorsResponse>, Status> {
        Err(refused("AddVectors"))
    }

    async fn install_snapshot(
        &self,
        _request: Request<Streaming<crate::pb::SnapshotChunk>>,
    ) -> Result<Response<crate::pb::InstallSnapshotResponse>, Status> {
        Err(refused("InstallSnapshot"))
    }

    async fn stream_snapshot(
        &self,
        _request: Request<crate::pb::StreamSnapshotRequest>,
    ) -> Result<Response<Self::StreamSnapshotStream>, Status> {
        Err(refused("StreamSnapshot"))
    }

    async fn add_documents(
        &self,
        _request: Request<Streaming<crate::pb::AddDocumentsRequest>>,
    ) -> Result<Response<crate::pb::AddDocumentsResponse>, Status> {
        Err(refused("AddDocuments"))
    }

    async fn ingest_mapped(
        &self,
        _request: Request<Streaming<crate::pb::IngestMappedRequest>>,
    ) -> Result<Response<crate::pb::IngestMappedResponse>, Status> {
        Err(refused("IngestMapped"))
    }

    async fn bm25_query_stream(
        &self,
        request: Request<Streaming<Bm25QueryStreamRequest>>,
    ) -> Result<Response<Self::Bm25QueryStreamStream>, Status> {
        crate::metrics::timed_stream(Route::Bm25QueryStream, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let deadline = timeout.map(|d| tokio::time::Instant::now() + d);
            let mut inbound = request.into_inner();
            let req = match inbound.message().await? {
                Some(Bm25QueryStreamRequest {
                    payload: Some(bm25_query_stream_request::Payload::Start(req)),
                }) => req,
                _ => {
                    return Err(Status::invalid_argument(
                        "first Bm25QueryStreamRequest must be a Bm25QueryRequest start",
                    ))
                }
            };
            refuse_bm25_aggregates(
                "Bm25QueryStream",
                &req.stats_fields,
                &req.cardinality_fields,
            )?;
            let (pinned, frozen) = self.pin();
            let claims = self.child_claims(
                req.expected_stats_epoch,
                &req.expected_stats_incarnation,
                frozen.node_addresses().len(),
            )?;
            if frozen.node_addresses().is_empty() {
                return Err(Status::failed_precondition(
                    "relay: a relay coordinator has no children",
                ));
            }
            let map_changes = self.inner.map.changes();
            let (tx, rx) = mpsc::channel::<Result<Bm25QueryStreamResponse, Status>>(64);
            let relay = self.clone();
            tokio::spawn(async move {
                let work = relay.relay_bm25_stream(
                    frozen,
                    pinned,
                    map_changes,
                    req,
                    claims,
                    inbound,
                    tx.clone(),
                    deadline,
                    timeout,
                );
                let result = tokio::select! {
                    _ = tx.closed() => Err(Status::cancelled("relay: parent response closed")),
                    result = work => result,
                };
                if let Err(status) = result {
                    let _ = tokio::time::timeout(Duration::from_millis(250), tx.send(Err(status)))
                        .await;
                }
            });
            Ok(Response::new(ReceiverStream::new(rx)))
        })
        .await
    }

    async fn read_wal(
        &self,
        _request: Request<crate::pb::ReadWalRequest>,
    ) -> Result<Response<Self::ReadWalStream>, Status> {
        Err(refused("ReadWal"))
    }

    /// The children's provider identity as the one shard the parent
    /// sees: the root's dense preflight asks each shard for it before a
    /// public query scores anything, so a relay answers with the
    /// descriptor and configuration its children share, rows summed,
    /// and refuses by name when a child differs.
    async fn get_vector_backend(
        &self,
        request: Request<crate::pb::GetVectorBackendRequest>,
    ) -> Result<Response<crate::pb::GetVectorBackendResponse>, Status> {
        let timeout = grpc_timeout(request.metadata());
        let (pinned, frozen) = self.pin();
        let children = frozen.node_addresses().to_vec();
        let mut tasks = Vec::with_capacity(children.len());
        for (shard, addr) in children.iter().enumerate() {
            let mut link = frozen.node_client(addr)?;
            let addr = addr.clone();
            tasks.push(tokio::spawn(async move {
                let mut request = Request::new(crate::pb::GetVectorBackendRequest {});
                if let Some(timeout) = timeout {
                    request.set_timeout(timeout);
                }
                link.get_vector_backend(request)
                    .await
                    .map(|r| r.into_inner())
                    .map_err(|status| {
                        Status::new(
                            status.code(),
                            format!(
                                "relay child {shard} ({addr}) vector backend: {}",
                                status.message()
                            ),
                        )
                    })
            }));
        }
        let mut reports = Vec::with_capacity(children.len());
        for task in tasks {
            reports.push(
                task.await
                    .map_err(|e| Status::internal(format!("relay vector backend task: {e}")))??,
            );
        }
        let merged = merge_vector_backend(&children, &reports)?;
        self.still_current(&pinned, "GetVectorBackend")?;
        Ok(Response::new(merged))
    }

    async fn configure_vector_backend(
        &self,
        _request: Request<crate::pb::ConfigureVectorBackendRequest>,
    ) -> Result<Response<crate::pb::ConfigureVectorBackendResponse>, Status> {
        Err(refused("ConfigureVectorBackend"))
    }

    async fn get_calibration(
        &self,
        _request: Request<crate::pb::GetCalibrationRequest>,
    ) -> Result<Response<crate::pb::GetCalibrationResponse>, Status> {
        Err(refused("GetCalibration"))
    }

    async fn set_calibration(
        &self,
        _request: Request<crate::pb::SetCalibrationRequest>,
    ) -> Result<Response<crate::pb::SetCalibrationResponse>, Status> {
        Err(refused("SetCalibration"))
    }

    async fn flush(
        &self,
        _request: Request<crate::pb::FlushRequest>,
    ) -> Result<Response<crate::pb::FlushResponse>, Status> {
        Err(refused("Flush"))
    }

    async fn export_snapshot(
        &self,
        _request: Request<crate::pb::ExportSnapshotRequest>,
    ) -> Result<Response<crate::pb::ExportSnapshotResponse>, Status> {
        Err(refused("ExportSnapshot"))
    }

    async fn install_snapshot_from(
        &self,
        _request: Request<crate::pb::InstallSnapshotFromRequest>,
    ) -> Result<Response<crate::pb::InstallSnapshotResponse>, Status> {
        Err(refused("InstallSnapshotFrom"))
    }

    async fn delete_documents(
        &self,
        _request: Request<crate::pb::DeleteDocumentsRequest>,
    ) -> Result<Response<crate::pb::DeleteDocumentsResponse>, Status> {
        Err(refused("DeleteDocuments"))
    }

    async fn commit_replacements(
        &self,
        _request: Request<crate::pb::CommitReplacementsRequest>,
    ) -> Result<Response<crate::pb::CommitReplacementsResponse>, Status> {
        Err(refused("CommitReplacements"))
    }

    async fn expand_term_prefix(
        &self,
        request: Request<crate::pb::ExpandTermPrefixRequest>,
    ) -> Result<Response<crate::pb::ExpandTermPrefixResponse>, Status> {
        crate::metrics::timed(Route::ExpandTermPrefix, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            let scope = crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
            let mut visibility_known = vec![false; scope.column_count()];
            let (pinned, frozen) = self.pin();
            let children = frozen.node_addresses().to_vec();
            if children.is_empty() {
                return Err(Status::failed_precondition(
                    "relay: a relay coordinator has no children",
                ));
            }
            let mut tasks = Vec::with_capacity(children.len());
            for (shard, addr) in children.iter().enumerate() {
                let mut link = frozen.node_client(addr)?;
                let child_req = req.clone();
                let addr = addr.clone();
                tasks.push(tokio::spawn(async move {
                    let mut request = Request::new(child_req);
                    if let Some(timeout) = timeout {
                        request.set_timeout(timeout);
                    }
                    link.expand_term_prefix(request)
                        .await
                        .map(|r| r.into_inner())
                        .map_err(|status| child_error(shard, &addr, "prefix expansion", status))
                }));
            }
            let mut shares = Vec::with_capacity(children.len());
            for task in tasks {
                shares.push(
                    task.await
                        .map_err(|e| Status::internal(format!("relay prefix task: {e}")))??,
                );
            }
            for share in &shares {
                scope.validate_echo(
                    &share.visibility_fingerprint,
                    &share.visibility_columns_known,
                )?;
                for (known, present) in visibility_known
                    .iter_mut()
                    .zip(&share.visibility_columns_known)
                {
                    *known |= present;
                }
            }
            let mut merged = merge_prefix_expansions(req.cap, &shares);
            merged.visibility_fingerprint = scope.fingerprint().to_vec();
            merged.visibility_columns_known = visibility_known;
            self.still_current(&pinned, "ExpandTermPrefix")?;
            Ok(Response::new(merged))
        })
        .await
    }

    async fn suggest_terms(
        &self,
        request: Request<crate::pb::SuggestTermsRequest>,
    ) -> Result<Response<crate::pb::SuggestTermsResponse>, Status> {
        crate::metrics::timed(Route::SuggestTerms, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            let scope = crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
            let mut visibility_known = vec![false; scope.column_count()];
            let (pinned, frozen) = self.pin();
            let children = frozen.node_addresses().to_vec();
            if children.is_empty() {
                return Err(Status::failed_precondition(
                    "relay: a relay coordinator has no children",
                ));
            }
            let mut tasks = Vec::with_capacity(children.len());
            for (shard, addr) in children.iter().enumerate() {
                let mut link = frozen.node_client(addr)?;
                let child_req = req.clone();
                let addr = addr.clone();
                tasks.push(tokio::spawn(async move {
                    let mut request = Request::new(child_req);
                    if let Some(timeout) = timeout {
                        request.set_timeout(timeout);
                    }
                    link.suggest_terms(request)
                        .await
                        .map(|r| r.into_inner())
                        .map_err(|status| child_error(shard, &addr, "dictionary scan", status))
                }));
            }
            let mut shares = Vec::with_capacity(children.len());
            for task in tasks {
                shares.push(
                    task.await
                        .map_err(|e| Status::internal(format!("relay suggest task: {e}")))??,
                );
            }
            for share in &shares {
                scope.validate_echo(
                    &share.visibility_fingerprint,
                    &share.visibility_columns_known,
                )?;
                for (known, present) in visibility_known
                    .iter_mut()
                    .zip(&share.visibility_columns_known)
                {
                    *known |= present;
                }
            }
            let mut merged = merge_suggest_terms(req.max_scan, &shares)?;
            merged.visibility_fingerprint = scope.fingerprint().to_vec();
            merged.visibility_columns_known = visibility_known;
            self.still_current(&pinned, "SuggestTerms")?;
            Ok(Response::new(merged))
        })
        .await
    }

    async fn bm25_query(
        &self,
        request: Request<Bm25QueryRequest>,
    ) -> Result<Response<Bm25QueryResponse>, Status> {
        crate::metrics::timed(Route::Bm25Query, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            refuse_bm25_aggregates("Bm25Query", &req.stats_fields, &req.cardinality_fields)?;
            let (pinned, frozen) = self.pin();
            let children = frozen.node_addresses().to_vec();
            if children.is_empty() {
                return Err(Status::failed_precondition(
                    "relay: a relay coordinator has no children",
                ));
            }
            let claims = self.child_claims(
                req.expected_stats_epoch,
                &req.expected_stats_incarnation,
                children.len(),
            )?;
            let mut tasks = Vec::with_capacity(children.len());
            for (shard, addr) in children.iter().enumerate() {
                let mut link = frozen.node_client(addr)?;
                let mut child_req = req.clone();
                child_req.expected_stats_epoch = claims[shard].epoch;
                child_req.expected_stats_incarnation = claims[shard].incarnation();
                let addr = addr.clone();
                tasks.push(tokio::spawn(async move {
                    let mut request = Request::new(child_req);
                    if let Some(timeout) = timeout {
                        request.set_timeout(timeout);
                    }
                    link.bm25_query(request)
                        .await
                        .map(|r| r.into_inner())
                        .map_err(|status| child_error(shard, &addr, "bm25 query", status))
                }));
            }
            let mut shares = Vec::with_capacity(children.len());
            for task in tasks {
                shares.push(
                    task.await
                        .map_err(|e| Status::internal(format!("relay bm25 query task: {e}")))??,
                );
            }
            let merged = merge_bm25_responses(&req, shares)?;
            self.still_current(&pinned, "Bm25Query")?;
            Ok(Response::new(merged))
        })
        .await
    }

    async fn bm25_phrase_query(
        &self,
        request: Request<Bm25PhraseQueryRequest>,
    ) -> Result<Response<Bm25QueryResponse>, Status> {
        crate::metrics::timed(Route::Bm25PhraseQuery, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            let query = req
                .query
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("Bm25PhraseQuery: query is absent"))?;
            refuse_bm25_aggregates(
                "Bm25PhraseQuery",
                &query.stats_fields,
                &query.cardinality_fields,
            )?;
            let (pinned, frozen) = self.pin();
            let children = frozen.node_addresses().to_vec();
            if children.is_empty() {
                return Err(Status::failed_precondition(
                    "relay: a relay coordinator has no children",
                ));
            }
            let claims = self.child_claims(
                query.expected_stats_epoch,
                &query.expected_stats_incarnation,
                children.len(),
            )?;
            let mut tasks = Vec::with_capacity(children.len());
            for (shard, addr) in children.iter().enumerate() {
                let mut link = frozen.node_client(addr)?;
                let mut child_req = req.clone();
                if let Some(q) = child_req.query.as_mut() {
                    q.expected_stats_epoch = claims[shard].epoch;
                    q.expected_stats_incarnation = claims[shard].incarnation();
                }
                let addr = addr.clone();
                tasks.push(tokio::spawn(async move {
                    let mut request = Request::new(child_req);
                    if let Some(timeout) = timeout {
                        request.set_timeout(timeout);
                    }
                    link.bm25_phrase_query(request)
                        .await
                        .map(|r| r.into_inner())
                        .map_err(|status| child_error(shard, &addr, "bm25 phrase query", status))
                }));
            }
            let mut shares = Vec::with_capacity(children.len());
            for task in tasks {
                shares.push(
                    task.await
                        .map_err(|e| Status::internal(format!("relay bm25 phrase task: {e}")))??,
                );
            }
            let merged = merge_bm25_responses(query, shares)?;
            self.still_current(&pinned, "Bm25PhraseQuery")?;
            Ok(Response::new(merged))
        })
        .await
    }

    async fn get_documents(
        &self,
        _request: Request<crate::pb::GetDocumentsRequest>,
    ) -> Result<Response<crate::pb::GetDocumentsResponse>, Status> {
        Err(refused("GetDocuments"))
    }

    async fn resolve_parents(
        &self,
        _request: Request<crate::pb::ResolveParentsRequest>,
    ) -> Result<Response<crate::pb::ResolveParentsResponse>, Status> {
        Err(refused("ResolveParents"))
    }

    async fn bm25_rescore(
        &self,
        request: Request<Bm25RescoreRequest>,
    ) -> Result<Response<Bm25RescoreResponse>, Status> {
        crate::metrics::timed(Route::Bm25Rescore, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            let (pinned, frozen) = self.pin();
            let children = frozen.node_addresses().to_vec();
            if children.is_empty() {
                return Err(Status::failed_precondition(
                    "relay: a relay coordinator has no children",
                ));
            }
            crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
            let claims = self.child_claims(
                req.expected_stats_epoch,
                &req.expected_stats_incarnation,
                children.len(),
            )?;
            // Each candidate goes to the child whose slot range holds it;
            // the ranges come from the children's health reports. An id
            // in no child's range is another shard's, and is ignored as
            // a node ignores an id outside its own range: the boolean
            // planner sends every shard every candidate.
            let reports = self.children_health(&frozen, timeout).await?;
            let ranges = child_ranges(&self.collection, &children, &reports)?;
            let by_child = route_ids(&ranges, &req.candidate_ids)?;
            let mut tasks = Vec::with_capacity(children.len());
            for (shard, ids) in by_child.into_iter().enumerate() {
                let requested: std::collections::HashSet<_> = ids.iter().copied().collect();
                let mut link = frozen.node_client(&children[shard])?;
                let mut child_req = req.clone();
                child_req.candidate_ids = ids;
                child_req.expected_stats_epoch = claims[shard].epoch;
                child_req.expected_stats_incarnation = claims[shard].incarnation();
                let addr = children[shard].clone();
                tasks.push((requested, tokio::spawn(async move {
                    let mut request = Request::new(child_req);
                    if let Some(timeout) = timeout {
                        request.set_timeout(timeout);
                    }
                    link.bm25_rescore(request)
                        .await
                        .map(|r| r.into_inner())
                        .map_err(|status| child_error(shard, &addr, "bm25 rescore", status))
                })));
            }
            let mut hits = Vec::new();
            let mut stage_known: Option<Vec<bool>> = None;
            let mut receipts = Vec::with_capacity(children.len());
            for (i, (requested, task)) in tasks.into_iter().enumerate() {
                let share = task
                    .await
                    .map_err(|e| Status::internal(format!("relay bm25 rescore task: {e}")))??;
                merge_known(
                    "stage-column",
                    i,
                    &mut stage_known,
                    &share.stage_columns_known,
                )?;
                let mut seen = std::collections::HashSet::new();
                for hit in &share.hits {
                    if !requested.contains(&hit.doc_id) || !hit.score.is_finite() || !seen.insert(hit.doc_id) {
                        return Err(Status::failed_precondition("relay: BM25 child returned an unrequested, duplicate or nonfinite score"));
                    }
                }
                receipts.push(read_metadata(&share));
                hits.extend(share.hits);
            }
            self.still_current(&pinned, "Bm25Rescore")?;
            let receipt = self.read_receipt(
                &pinned,
                children,
                req.visibility.as_ref(),
                Some(&claims),
                &receipts,
            )?;
            hits.sort_by_key(|hit| hit.doc_id);
            Ok(Response::new(Bm25RescoreResponse {
                stats_epoch: receipt.stats_epoch,
                stats_incarnation: receipt.stats_incarnation,
                visibility_fingerprint: receipt.visibility_fingerprint,
                visibility_columns_known: receipt.visibility_columns_known,
                hits,
                stage_columns_known: stage_known
                    .unwrap_or_else(|| vec![false; req.score_stages.len()]),
            }))
        })
        .await
    }

    async fn fetch_values(
        &self,
        _request: Request<crate::pb::FetchValuesRequest>,
    ) -> Result<Response<crate::pb::FetchValuesResponse>, Status> {
        Err(refused("FetchValues"))
    }

    async fn aggregate_shard(
        &self,
        _request: Request<crate::pb::AggregateShardRequest>,
    ) -> Result<Response<crate::pb::AggregateShardResponse>, Status> {
        Err(refused("AggregateShard"))
    }

    async fn quantile_counts(
        &self,
        _request: Request<crate::pb::QuantileCountsRequest>,
    ) -> Result<Response<crate::pb::QuantileCountsResponse>, Status> {
        Err(refused("QuantileCounts"))
    }

    async fn vector_rescore(
        &self,
        request: Request<crate::pb::VectorRescoreRequest>,
    ) -> Result<Response<crate::pb::VectorRescoreResponse>, Status> {
        crate::metrics::timed(Route::VectorRescore, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            let (pinned, frozen) = self.pin();
            let children = frozen.node_addresses().to_vec();
            if children.is_empty() {
                return Err(Status::failed_precondition(
                    "relay: a relay coordinator has no children",
                ));
            }
            crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
            let claims = self.child_claims(
                req.expected_stats_epoch,
                &req.expected_stats_incarnation,
                children.len(),
            )?;
            let reports = self.children_health(&frozen, timeout).await?;
            let ranges = child_ranges(&self.collection, &children, &reports)?;
            let by_child = route_ids(&ranges, &req.candidate_ids)?;
            let mut tasks = Vec::with_capacity(children.len());
            for (shard, ids) in by_child.into_iter().enumerate() {
                let mut link = frozen.node_client(&children[shard])?;
                let child_req = crate::pb::VectorRescoreRequest {
                    vector: req.vector.clone(),
                    candidate_ids: ids,
                    field: req.field.clone(),
                    visibility: req.visibility.clone(),
                    expected_stats_epoch: claims[shard].epoch,
                    expected_stats_incarnation: claims[shard].incarnation(),
                };
                let addr = children[shard].clone();
                tasks.push(tokio::spawn(async move {
                    let mut request = Request::new(child_req);
                    if let Some(timeout) = timeout {
                        request.set_timeout(timeout);
                    }
                    link.vector_rescore(request)
                        .await
                        .map(|r| r.into_inner())
                        .map_err(|status| child_error(shard, &addr, "vector rescore", status))
                }));
            }
            let mut receipts = Vec::with_capacity(children.len());
            let mut binding = None;
            let mut hits = Vec::new();
            for task in tasks {
                let share = task
                    .await
                    .map_err(|e| Status::internal(format!("relay vector rescore task: {e}")))??;
                crate::vector_read::check_binding(
                    &req.field,
                    share.vector_binding.as_ref(),
                    &mut binding,
                )?;
                receipts.push(read_metadata(&share));
                hits.extend(share.hits);
            }
            // Score descending, ids ascending: the order one node answers
            // in, over the union of the children's.
            hits.sort_by(|a, b| {
                b.score
                    .total_cmp(&a.score)
                    .then_with(|| a.doc_id.cmp(&b.doc_id))
            });
            self.still_current(&pinned, "VectorRescore")?;
            let receipt = self.read_receipt(
                &pinned,
                children,
                req.visibility.as_ref(),
                Some(&claims),
                &receipts,
            )?;
            Ok(Response::new(crate::pb::VectorRescoreResponse {
                hits,
                vector_binding: binding.flatten(),
                stats_epoch: receipt.stats_epoch,
                stats_incarnation: receipt.stats_incarnation,
                visibility_fingerprint: receipt.visibility_fingerprint,
                visibility_columns_known: receipt.visibility_columns_known,
            }))
        })
        .await
    }

    async fn exact_vector_rescore(
        &self,
        request: Request<crate::pb::ExactVectorRescoreRequest>,
    ) -> Result<Response<crate::pb::ExactVectorRescoreResponse>, Status> {
        crate::metrics::timed(Route::ExactVectorRescore, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            let (pinned, frozen) = self.pin();
            let children = frozen.node_addresses().to_vec();
            if children.is_empty() {
                return Err(Status::failed_precondition(
                    "relay: a relay coordinator has no children",
                ));
            }
            // The root sends every candidate to every shard and each
            // shard answers for the ids it owns; here the ids outside the
            // children's ranges are the other shards' and are ignored the
            // same way, and only a child that owns some is asked.
            crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
            let claims = self.child_claims(
                req.expected_stats_epoch,
                &req.expected_stats_incarnation,
                children.len(),
            )?;
            let reports = self.children_health(&frozen, timeout).await?;
            let ranges = child_ranges(&self.collection, &children, &reports)?;
            let mut requested = req.candidate_ids.clone();
            let mut seen = std::collections::HashSet::with_capacity(requested.len());
            requested.retain(|id| seen.insert(*id));
            let by_child = route_ids(&ranges, &requested)?;
            let mut tasks = Vec::with_capacity(children.len());
            for (shard, ids) in by_child.into_iter().enumerate() {
                let mut link = frozen.node_client(&children[shard])?;
                let child_req = crate::pb::ExactVectorRescoreRequest {
                    vector: req.vector.clone(),
                    candidate_ids: ids,
                    field: req.field.clone(),
                    visibility: req.visibility.clone(),
                    expected_stats_epoch: claims[shard].epoch,
                    expected_stats_incarnation: claims[shard].incarnation(),
                    max_logical_bytes: req.max_logical_bytes,
                };
                let addr = children[shard].clone();
                tasks.push(tokio::spawn(async move {
                    let mut request = Request::new(child_req);
                    if let Some(timeout) = timeout {
                        request.set_timeout(timeout);
                    }
                    link.exact_vector_rescore(request)
                        .await
                        .map(|r| r.into_inner())
                        .map_err(|status| child_error(shard, &addr, "exact vector rescore", status))
                }));
            }
            let mut receipts = Vec::with_capacity(children.len());
            let mut binding = None;
            let mut merged = crate::pb::ExactVectorRescoreResponse::default();
            let mut by_id = HashMap::new();
            for (i, task) in tasks.into_iter().enumerate() {
                let share = task.await.map_err(|e| {
                    Status::internal(format!("relay exact vector rescore task: {e}"))
                })??;
                crate::vector_read::check_binding(
                    &req.field,
                    share.vector_binding.as_ref(),
                    &mut binding,
                )?;
                receipts.push(read_metadata(&share));
                add_count(
                    &mut merged.logical_bytes,
                    share.logical_bytes,
                    i,
                    "logical bytes",
                )?;
                add_count(
                    &mut merged.pages_touched,
                    share.pages_touched,
                    i,
                    "page count",
                )?;
                merged.tasks = merged.tasks.saturating_add(share.tasks);
                for hit in share.hits {
                    if by_id.insert(hit.doc_id, hit).is_some() {
                        return Err(Status::failed_precondition(
                            "relay: exact vector rescore candidate answered by more than one \
                             child; slot ranges overlap",
                        ));
                    }
                }
            }
            // One row per owned id, in request order, as a node answers.
            merged.hits = requested.iter().filter_map(|id| by_id.remove(id)).collect();
            self.still_current(&pinned, "ExactVectorRescore")?;
            let receipt = self.read_receipt(
                &pinned,
                children,
                req.visibility.as_ref(),
                Some(&claims),
                &receipts,
            )?;
            merged.vector_binding = binding.flatten();
            merged.stats_epoch = receipt.stats_epoch;
            merged.stats_incarnation = receipt.stats_incarnation;
            merged.visibility_fingerprint = receipt.visibility_fingerprint;
            merged.visibility_columns_known = receipt.visibility_columns_known;
            Ok(Response::new(merged))
        })
        .await
    }

    async fn hybrid_shard(
        &self,
        _request: Request<crate::pb::HybridShardRequest>,
    ) -> Result<Response<crate::pb::HybridShardResponse>, Status> {
        Err(refused("HybridShard"))
    }

    async fn shard_legs(
        &self,
        request: Request<ShardLegsRequest>,
    ) -> Result<Response<ShardLegsResponse>, Status> {
        crate::metrics::timed(Route::ShardLegs, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            let (pinned, frozen) = self.pin();
            let children = frozen.node_addresses().to_vec();
            if children.is_empty() {
                return Err(Status::failed_precondition(
                    "relay: a relay coordinator has no children",
                ));
            }
            let claims = self.child_claims(
                req.expected_stats_epoch,
                &req.expected_stats_incarnation,
                children.len(),
            )?;
            let read_claims = req
                .read_context
                .as_ref()
                .map(|context| {
                    crate::visibility::VisibilityScope::new(context.visibility.as_ref())?;
                    self.child_claims(
                        context.expected_stats_epoch,
                        &context.expected_stats_incarnation,
                        children.len(),
                    )
                })
                .transpose()?;
            let mut tasks = Vec::with_capacity(children.len());
            for (shard, addr) in children.iter().enumerate() {
                let mut link = frozen.node_client(addr)?;
                let mut child_req = req.clone();
                child_req.expected_stats_epoch = claims[shard].epoch;
                child_req.expected_stats_incarnation = claims[shard].incarnation();
                if let (Some(context), Some(read_claims)) =
                    (&mut child_req.read_context, &read_claims)
                {
                    context.expected_stats_epoch = read_claims[shard].epoch;
                    context.expected_stats_incarnation = read_claims[shard].incarnation();
                }
                let addr = addr.clone();
                tasks.push(tokio::spawn(async move {
                    let mut request = Request::new(child_req);
                    if let Some(timeout) = timeout {
                        request.set_timeout(timeout);
                    }
                    link.shard_legs(request)
                        .await
                        .map(|r| r.into_inner())
                        .map_err(|status| child_error(shard, &addr, "shard legs", status))
                }));
            }
            // Raw per-leg lists: the parent merges them by score across
            // its shards, and the union of the children's lists holds the
            // subtree's top of each leg. Competition ranks at the parent
            // make the order of equal scores immaterial.
            let mut merged = ShardLegsResponse::default();
            let mut geo_known = None;
            let mut filter_known = None;
            let mut receipts = Vec::with_capacity(children.len());
            let mut binding = None;
            for (shard, task) in tasks.into_iter().enumerate() {
                let share = task
                    .await
                    .map_err(|e| Status::internal(format!("relay shard legs task: {e}")))??;
                match (&req.read_context, share.read_receipt.as_ref()) {
                    (Some(context), Some(receipt)) => {
                        crate::vector_read::check_binding(
                            &context.field,
                            receipt.vector_binding.as_ref(),
                            &mut binding,
                        )?;
                        receipts.push(receipt.clone());
                    }
                    (None, None) => {}
                    _ => {
                        return Err(Status::failed_precondition(
                            "relay: hybrid leg read receipt mismatch",
                        ))
                    }
                }
                merge_known(
                    "geo-column",
                    shard,
                    &mut geo_known,
                    &share.geo_columns_known,
                )?;
                merge_known(
                    "filter-leaf",
                    shard,
                    &mut filter_known,
                    &share.filter_columns_known,
                )?;
                merged.vector_hits.extend(share.vector_hits);
                merged.bm25_hits.extend(share.bm25_hits);
            }
            merged.geo_columns_known = geo_known.unwrap_or_default();
            merged.filter_columns_known = filter_known.unwrap_or_default();
            if let Some(context) = &req.read_context {
                let mut receipt = self.read_receipt(
                    &pinned,
                    children,
                    context.visibility.as_ref(),
                    read_claims.as_deref(),
                    &receipts,
                )?;
                receipt.vector_binding = binding.flatten();
                merged.read_receipt = Some(receipt);
            }
            self.still_current(&pinned, "ShardLegs")?;
            Ok(Response::new(merged))
        })
        .await
    }

    async fn browse_shard(
        &self,
        _request: Request<crate::pb::BrowseShardRequest>,
    ) -> Result<Response<crate::pb::BrowseShardResponse>, Status> {
        Err(refused("BrowseShard"))
    }

    async fn resolve_filter_bitmap(
        &self,
        request: Request<crate::pb::FilterBitmapRequest>,
    ) -> Result<Response<crate::pb::FilterBitmapResponse>, Status> {
        crate::metrics::timed(Route::ResolveFilterBitmap, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
            crate::node::validate_geo_filters(&req.geo_filters)?;
            if let Some(f) = req.filter.as_ref() {
                crate::filter::validate_filter(f)?;
            }
            let filters = RequestFilters {
                geo: req.geo_filters.clone(),
                tree: req.filter.clone(),
            };
            let (pinned, frozen) = self.pin();
            let children = frozen.node_addresses().to_vec();
            if children.is_empty() {
                return Err(Status::failed_precondition(
                    "relay: a relay coordinator has no children",
                ));
            }
            let reports = self.children_health(&frozen, timeout).await?;
            let ranges = child_ranges(&self.collection, &children, &reports)?;
            let mask = req
                .visibility
                .is_none()
                .then(|| frozen.shard_mask(filters.tree.as_ref()))
                .flatten();
            let mut known = KnownFlags::new(&filters, mask.as_ref());
            let mut tasks = Vec::with_capacity(children.len());
            for (shard, addr) in children.iter().enumerate() {
                if mask.as_ref().is_some_and(|m| m.skipped[shard]) {
                    tasks.push(None);
                    continue;
                }
                let mut link = frozen.node_client(addr)?;
                let child_req = crate::pb::FilterBitmapRequest {
                    visibility: req.visibility.clone(),
                    geo_filters: filters.geo.clone(),
                    filter: CoordinatorServiceImpl::shard_filter_tree(
                        &filters,
                        mask.as_ref(),
                        shard,
                    ),
                };
                let addr = addr.clone();
                tasks.push(Some(tokio::spawn(async move {
                    let mut request = Request::new(child_req);
                    if let Some(timeout) = timeout {
                        request.set_timeout(timeout);
                    }
                    link.resolve_filter_bitmap(request)
                        .await
                        .map(|r| r.into_inner())
                        .map_err(|status| child_error(shard, &addr, "filter bitmap", status))
                })));
            }
            let mut shares = Vec::with_capacity(children.len());
            let mut receipts = Vec::with_capacity(children.len());
            let mut segments_total: u32 = 0;
            let mut segments_skipped: u32 = 0;
            for (shard, task) in tasks.into_iter().enumerate() {
                let Some(task) = task else {
                    let mut request = Request::new(TermStatsRequest {
                        version_only: true,
                        ..Default::default()
                    });
                    if let Some(timeout) = timeout {
                        request.set_timeout(timeout);
                    }
                    let share = frozen
                        .node_client(&children[shard])?
                        .term_stats(request)
                        .await?
                        .into_inner();
                    crate::visibility::validate_stats_mode(true, &share)?;
                    receipts.push(crate::pb::VectorReadReceipt {
                        stats_epoch: share.stats_epoch,
                        stats_incarnation: share.stats_incarnation,
                        visibility_fingerprint: share.visibility_fingerprint,
                        visibility_columns_known: share.visibility_columns_known,
                        ..Default::default()
                    });
                    shares.push(None);
                    continue;
                };
                let share = task
                    .await
                    .map_err(|e| Status::internal(format!("relay filter bitmap task: {e}")))??;
                receipts.push(read_metadata(&share));
                known.merge(
                    shard,
                    mask.as_ref(),
                    &share.geo_columns_known,
                    &share.filter_columns_known,
                )?;
                add_count32(
                    &mut segments_total,
                    share.segments_total,
                    shard,
                    "segment count",
                )?;
                add_count32(
                    &mut segments_skipped,
                    share.segments_skipped,
                    shard,
                    "skipped segment count",
                )?;
                shares.push(Some(ChildBitmap {
                    base_label: share.base_label,
                    label_count: share.label_count,
                    bits: share.bits,
                }));
            }
            let (base_label, label_count, bits) = concat_bitmaps(&children, &ranges, &shares)?;
            self.still_current(&pinned, "ResolveFilterBitmap")?;
            let receipt =
                self.read_receipt(&pinned, children, req.visibility.as_ref(), None, &receipts)?;
            Ok(Response::new(crate::pb::FilterBitmapResponse {
                stats_epoch: receipt.stats_epoch,
                stats_incarnation: receipt.stats_incarnation,
                visibility_fingerprint: receipt.visibility_fingerprint,
                visibility_columns_known: receipt.visibility_columns_known,
                base_label,
                label_count,
                bits,
                geo_columns_known: known.geo,
                filter_columns_known: known.tree,
                segments_total,
                segments_skipped,
            }))
        })
        .await
    }

    async fn resolve_lexical_bitmap(
        &self,
        request: Request<crate::pb::LexicalBitmapRequest>,
    ) -> Result<Response<crate::pb::MembershipBitmapResponse>, Status> {
        let timeout = grpc_timeout(request.metadata());
        let req = request.into_inner();
        crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
        let (pinned, frozen) = self.pin();
        let children = frozen.node_addresses().to_vec();
        if children.is_empty() {
            return Err(Status::failed_precondition(
                "relay: a relay coordinator has no children",
            ));
        }
        let reports = self.children_health(&frozen, timeout).await?;
        let ranges = child_ranges(&self.collection, &children, &reports)?;
        let mut tasks = Vec::with_capacity(children.len());
        for (shard, addr) in children.iter().enumerate() {
            let mut link = frozen.node_client(addr)?;
            let child_req = req.clone();
            let addr = addr.clone();
            tasks.push(tokio::spawn(async move {
                let mut request = Request::new(child_req);
                if let Some(timeout) = timeout {
                    request.set_timeout(timeout);
                }
                link.resolve_lexical_bitmap(request)
                    .await
                    .map(|r| r.into_inner())
                    .map_err(|status| child_error(shard, &addr, "lexical bitmap", status))
            }));
        }
        let mut shares = Vec::with_capacity(children.len());
        let mut receipts = Vec::with_capacity(children.len());
        let mut binding = None;
        let mut segments_total: u32 = 0;
        let mut segments_skipped: u32 = 0;
        for (shard, task) in tasks.into_iter().enumerate() {
            let share = task
                .await
                .map_err(|e| Status::internal(format!("relay lexical bitmap task: {e}")))??;
            add_count32(
                &mut segments_total,
                share.segments_total,
                shard,
                "segment count",
            )?;
            add_count32(
                &mut segments_skipped,
                share.segments_skipped,
                shard,
                "skipped segment count",
            )?;
            crate::vector_read::check_binding("", None, &mut binding)?;
            receipts.push(read_metadata(&share));
            shares.push(Some(ChildBitmap {
                base_label: share.base_label,
                label_count: share.label_count,
                bits: share.bits,
            }));
        }
        let (base_label, label_count, bits) = concat_bitmaps(&children, &ranges, &shares)?;
        self.still_current(&pinned, "ResolveLexicalBitmap")?;
        let receipt =
            self.read_receipt(&pinned, children, req.visibility.as_ref(), None, &receipts)?;
        Ok(Response::new(crate::pb::MembershipBitmapResponse {
            vector_binding: binding.flatten(),
            stats_incarnation: receipt.stats_incarnation,
            visibility_fingerprint: receipt.visibility_fingerprint,
            visibility_columns_known: receipt.visibility_columns_known,
            base_label,
            label_count,
            bits,
            stats_epoch: receipt.stats_epoch,
            segments_total,
            segments_skipped,
        }))
    }

    async fn resolve_vector_bitmap(
        &self,
        request: Request<crate::pb::VectorBitmapRequest>,
    ) -> Result<Response<crate::pb::MembershipBitmapResponse>, Status> {
        let timeout = grpc_timeout(request.metadata());
        let req = request.into_inner();
        crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
        let (pinned, frozen) = self.pin();
        let children = frozen.node_addresses().to_vec();
        if children.is_empty() {
            return Err(Status::failed_precondition(
                "relay: a relay coordinator has no children",
            ));
        }
        let reports = self.children_health(&frozen, timeout).await?;
        let ranges = child_ranges(&self.collection, &children, &reports)?;
        let mut tasks = Vec::with_capacity(children.len());
        for (shard, addr) in children.iter().enumerate() {
            let mut link = frozen.node_client(addr)?;
            let addr = addr.clone();
            let req = req.clone();
            tasks.push(tokio::spawn(async move {
                let mut request = Request::new(req);
                if let Some(timeout) = timeout {
                    request.set_timeout(timeout);
                }
                link.resolve_vector_bitmap(request)
                    .await
                    .map(|r| r.into_inner())
                    .map_err(|status| child_error(shard, &addr, "vector bitmap", status))
            }));
        }
        let mut shares = Vec::with_capacity(children.len());
        let mut receipts = Vec::with_capacity(children.len());
        let mut binding = None;
        for task in tasks {
            let share = task
                .await
                .map_err(|e| Status::internal(format!("relay vector bitmap task: {e}")))??;
            crate::vector_read::check_binding(
                &req.field,
                share.vector_binding.as_ref(),
                &mut binding,
            )?;
            receipts.push(read_metadata(&share));
            shares.push(Some(ChildBitmap {
                base_label: share.base_label,
                label_count: share.label_count,
                bits: share.bits,
            }));
        }
        let (base_label, label_count, bits) = concat_bitmaps(&children, &ranges, &shares)?;
        self.still_current(&pinned, "ResolveVectorBitmap")?;
        let receipt =
            self.read_receipt(&pinned, children, req.visibility.as_ref(), None, &receipts)?;
        Ok(Response::new(crate::pb::MembershipBitmapResponse {
            vector_binding: binding.flatten(),
            stats_incarnation: receipt.stats_incarnation,
            visibility_fingerprint: receipt.visibility_fingerprint,
            visibility_columns_known: receipt.visibility_columns_known,
            base_label,
            label_count,
            bits,
            stats_epoch: receipt.stats_epoch,
            segments_total: 0,
            segments_skipped: 0,
        }))
    }

    /// One shard-side Boolean evaluation over the children: the parent's
    /// stats claim translates per child, each child answers its best
    /// `depth`, and the merge is the ranked union cut to `depth` with the
    /// counts summed and the known flags joined. An aggregate is not
    /// composed here (the fold order is the root's) and refuses by name.
    async fn evaluate_boolean(
        &self,
        request: Request<crate::pb::BooleanShardRequest>,
    ) -> Result<Response<crate::pb::BooleanShardResponse>, Status> {
        crate::metrics::timed(Route::EvaluateBoolean, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            if req.aggregate.is_some() {
                return Err(Status::unimplemented(
                    "relay: BooleanQuery.aggregate is not composed through a relay (the fold \
                     order is the root's); aggregate through a root over the shards directly",
                ));
            }
            if req.depth == 0 {
                return Err(Status::invalid_argument(
                    "EvaluateBoolean: depth 0 asks for no candidates",
                ));
            }
            let (pinned, frozen) = self.pin();
            let children = frozen.node_addresses().to_vec();
            if children.is_empty() {
                return Err(Status::failed_precondition(
                    "relay: a relay coordinator has no children",
                ));
            }
            let claims = self.child_claims(
                req.expected_stats_epoch,
                &req.expected_stats_incarnation,
                children.len(),
            )?;
            let mut tasks = Vec::with_capacity(children.len());
            for (shard, addr) in children.iter().enumerate() {
                let mut link = frozen.node_client(addr)?;
                let mut child_req = req.clone();
                child_req.expected_stats_epoch = claims[shard].epoch;
                child_req.expected_stats_incarnation = claims[shard].incarnation();
                let addr = addr.clone();
                tasks.push(tokio::spawn(async move {
                    let mut request = Request::new(child_req);
                    if let Some(timeout) = timeout {
                        request.set_timeout(timeout);
                    }
                    link.evaluate_boolean(request)
                        .await
                        .map(|r| r.into_inner())
                        .map_err(|status| child_error(shard, &addr, "boolean evaluation", status))
                }));
            }
            let mut shares = Vec::with_capacity(children.len());
            for task in tasks {
                shares.push(
                    task.await
                        .map_err(|e| Status::internal(format!("relay boolean task: {e}")))??,
                );
            }
            let mut merged = merge_boolean_responses(&req, &shares)?;
            self.still_current(&pinned, "EvaluateBoolean")?;
            let receipts = shares
                .iter()
                .map(|share| {
                    let receipt = share.read_receipt.clone().ok_or_else(|| {
                        Status::failed_precondition("relay child omitted Boolean read receipt")
                    })?;
                    if receipt.stats_epoch != share.stats_epoch {
                        return Err(Status::failed_precondition(
                            "Boolean receipt epoch mismatch",
                        ));
                    }
                    Ok(receipt)
                })
                .collect::<Result<Vec<_>, Status>>()?;
            let mut receipt = self.read_receipt(
                &pinned,
                children,
                req.visibility.as_ref(),
                Some(&claims),
                &receipts,
            )?;
            let mut binding = None;
            for leaf in &req.leaves {
                if let Some(crate::pb::boolean_plan_leaf::Leaf::Dense(dense)) = leaf.leaf.as_ref() {
                    for child in &receipts {
                        crate::vector_read::check_binding(
                            &dense.field,
                            child.vector_binding.as_ref(),
                            &mut binding,
                        )?;
                    }
                }
            }
            receipt.vector_binding = binding.flatten();
            merged.stats_epoch = receipt.stats_epoch;
            merged.read_receipt = Some(receipt);
            Ok(Response::new(merged))
        })
        .await
    }

    async fn apply_wal_binding(
        &self,
        _request: Request<crate::pb::ApplyWalBindingRequest>,
    ) -> Result<Response<crate::pb::ApplyWalBindingResponse>, Status> {
        Err(refused("ApplyWalBinding"))
    }

    async fn compact_shard(
        &self,
        _request: Request<crate::pb::CompactShardRequest>,
    ) -> Result<Response<crate::pb::CompactShardResponse>, Status> {
        Err(refused("CompactShard"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tuple(epochs: &[u64]) -> TokenTuple {
        TokenTuple {
            collection: String::new(),
            control_revision: 0,
            topology_generation: 0,
            children: vec!["a".into(), "b".into()],
            epochs: epochs
                .iter()
                .map(|&epoch| {
                    if epoch == 0 {
                        StatsClaim::default()
                    } else {
                        StatsClaim::required(epoch, &[1; 32]).unwrap()
                    }
                })
                .collect(),
        }
    }

    #[test]
    fn tokens_repeat_for_one_tuple_and_move_with_an_epoch() {
        let mut registry = TokenRegistry::new(7);
        let t1 = registry.allocate(tuple(&[1, 1])).unwrap();
        assert_ne!(t1, 0);
        assert_eq!(t1 >> 32, 7, "the incarnation is the high half");
        assert_eq!(
            registry.allocate(tuple(&[1, 1])).unwrap(),
            t1,
            "same tuple, same token"
        );
        let t2 = registry.allocate(tuple(&[2, 1])).unwrap();
        assert_ne!(t2, t1);
        assert_eq!(registry.lookup(t1).unwrap().epochs, tuple(&[1, 1]).epochs);
        assert_eq!(registry.lookup(t2).unwrap().epochs, tuple(&[2, 1]).epochs);
        assert!(registry.lookup(t1 + 1000).is_none());
    }

    #[test]
    fn token_exhaustion_refuses_instead_of_reusing_an_old_token() {
        let mut registry = TokenRegistry::new(1);
        let issued = registry.allocate(tuple(&[1, 1])).unwrap();
        registry.counter = u32::MAX;
        assert_eq!(registry.allocate(tuple(&[1, 1])).unwrap(), issued);
        assert_eq!(
            registry.allocate(tuple(&[2, 1])).unwrap_err().code(),
            tonic::Code::ResourceExhausted
        );
        assert_eq!(
            registry.lookup(issued).unwrap().epochs,
            tuple(&[1, 1]).epochs
        );
    }

    #[test]
    fn retention_is_bounded() {
        let mut registry = TokenRegistry::new(1);
        let first = registry.allocate(tuple(&[0, 0])).unwrap();
        for i in 1..=(RETAINED_TOKENS as u64) {
            registry.allocate(tuple(&[i, 0])).unwrap();
        }
        assert!(
            registry.lookup(first).is_none(),
            "the oldest token is forgotten"
        );
        assert_eq!(registry.entries.len(), RETAINED_TOKENS);
    }

    #[test]
    fn grpc_timeouts_parse() {
        let mut m = tonic::metadata::MetadataMap::new();
        assert_eq!(grpc_timeout(&m), None);
        m.insert("grpc-timeout", "1500m".parse().unwrap());
        assert_eq!(grpc_timeout(&m), Some(Duration::from_millis(1500)));
        m.insert("grpc-timeout", "2S".parse().unwrap());
        assert_eq!(grpc_timeout(&m), Some(Duration::from_secs(2)));
        m.insert("grpc-timeout", "3H".parse().unwrap());
        assert_eq!(grpc_timeout(&m), Some(Duration::from_secs(10800)));
        m.insert("grpc-timeout", "bogus".parse().unwrap());
        assert_eq!(grpc_timeout(&m), None);
    }

    fn share(doc_count: u64, dfs: &[u32], known: bool, positions: bool) -> TermStatsResponse {
        TermStatsResponse {
            version_only: false,
            visibility_fingerprint: Vec::new(),
            visibility_columns_known: Vec::new(),
            doc_count,
            total_doc_length: doc_count * 10,
            doc_frequencies: dfs.to_vec(),
            field_stats: vec![crate::pb::FieldStats {
                total_doc_length: doc_count,
                doc_frequencies: dfs.to_vec(),
                known,
                positions,
                sentences: false,
            }],
            stats_epoch: 3,
            stats_incarnation: vec![1; 32],
        }
    }

    fn request() -> TermStatsRequest {
        TermStatsRequest {
            version_only: false,
            visibility: None,
            terms: vec!["court".into(), "opinion".into()],
            fields: vec![crate::pb::FieldTerms {
                field: "title".into(),
                terms: vec!["court".into(), "opinion".into()],
            }],
        }
    }

    #[test]
    fn version_probes_refuse_legacy_mixed_or_nonempty_statistics() {
        let request = TermStatsRequest {
            version_only: true,
            ..Default::default()
        };
        let probe = TermStatsResponse {
            version_only: true,
            stats_epoch: 3,
            stats_incarnation: vec![1; 32],
            ..Default::default()
        };
        let merged = merge_term_stats(&request, &[probe.clone(), probe.clone()]).unwrap();
        assert!(merged.version_only);
        assert_eq!(merged.doc_count, 0);
        for case in 0..5 {
            let mut malformed = probe.clone();
            match case {
                0 => malformed.version_only = false,
                1 => malformed.doc_count = 1,
                2 => malformed.total_doc_length = 1,
                3 => malformed.doc_frequencies.push(0),
                _ => malformed.field_stats.push(Default::default()),
            }
            assert_eq!(
                merge_term_stats(&request, &[probe.clone(), malformed])
                    .unwrap_err()
                    .code(),
                tonic::Code::FailedPrecondition
            );
        }
        assert_eq!(
            merge_term_stats(&TermStatsRequest::default(), &[probe.clone()])
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
        for fields in [false, true] {
            let mut malformed = request.clone();
            if fields {
                malformed.fields.push(Default::default());
            } else {
                malformed.terms.push("term".into());
            }
            assert_eq!(
                merge_term_stats(&malformed, &[probe.clone()])
                    .unwrap_err()
                    .code(),
                tonic::Code::InvalidArgument
            );
        }
    }

    #[test]
    fn term_stats_sum_and_carry_the_shared_flags() {
        let merged = merge_term_stats(
            &request(),
            &[
                share(10, &[3, 0], true, true),
                share(5, &[1, 2], true, true),
            ],
        )
        .unwrap();
        assert_eq!(merged.doc_count, 15);
        assert_eq!(merged.total_doc_length, 150);
        assert_eq!(merged.doc_frequencies, vec![4, 2]);
        assert_eq!(merged.field_stats[0].doc_frequencies, vec![4, 2]);
        assert_eq!(merged.field_stats[0].total_doc_length, 15);
        assert!(merged.field_stats[0].known && merged.field_stats[0].positions);
        assert_eq!(merged.stats_epoch, 0, "the caller allocates the token");
    }

    #[test]
    fn a_u32_document_frequency_past_the_contract_refuses_by_name() {
        let err = merge_term_stats(
            &request(),
            &[
                share(1, &[3_000_000_000, 0], true, true),
                share(1, &[2_000_000_000, 0], true, true),
            ],
        )
        .unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(err.message().contains("\"court\""), "{}", err.message());
        assert!(err.message().contains("u32"), "{}", err.message());
        assert!(err.message().contains("child 1"), "{}", err.message());
    }

    #[test]
    fn mixed_field_capabilities_refuse_by_name() {
        let err = merge_term_stats(
            &request(),
            &[
                share(1, &[1, 0], true, true),
                share(1, &[1, 0], true, false),
            ],
        )
        .unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(err.message().contains("\"title\""), "{}", err.message());
        assert!(err.message().contains("positions"), "{}", err.message());
        let err = merge_term_stats(
            &request(),
            &[
                share(1, &[1, 0], false, false),
                share(1, &[1, 0], true, false),
            ],
        )
        .unwrap_err();
        assert!(err.message().contains("known"), "{}", err.message());
        assert!(merge_term_stats(&request(), &[]).is_err());
    }

    fn report(slot_offset: u64, num_vectors: u64, document_slots: u64) -> HealthResponse {
        HealthResponse {
            slot_offset,
            num_vectors,
            bm25_docs: num_vectors,
            document_slots,
            dim: 4,
            scoring_fingerprint: "fp".into(),
            vector_backend: "tv".into(),
            exact_vectors_available: true,
            exact_vectors_mmap: true,
            wal_clocked: true,
            wal_generation: 9,
            ..Default::default()
        }
    }

    #[test]
    fn health_merges_contiguous_children_and_refuses_gaps() {
        let children = vec!["a".to_string(), "b".to_string()];
        let merged =
            merge_health("", &children, &[report(100, 50, 50), report(0, 100, 100)]).unwrap();
        assert_eq!(merged.slot_offset, 0);
        assert_eq!(merged.num_vectors, 150);
        assert_eq!(merged.document_slots, 150);
        assert_eq!(merged.scoring_fingerprint, "fp");
        assert!(
            !merged.wal_clocked && merged.wal_generation == 0,
            "a relay has no WAL"
        );

        let err =
            merge_health("", &children, &[report(0, 100, 100), report(105, 5, 5)]).unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(err.message().contains("gap"), "{}", err.message());
        assert!(err.message().contains("0..100"), "{}", err.message());

        let err =
            merge_health("", &children, &[report(0, 100, 100), report(90, 5, 5)]).unwrap_err();
        assert!(err.message().contains("overlaps"), "{}", err.message());

        let mut other = report(100, 1, 1);
        other.scoring_fingerprint = "other".into();
        let err = merge_health("", &children, &[report(0, 100, 100), other]).unwrap_err();
        assert!(
            err.message().contains("scoring_fingerprint"),
            "{}",
            err.message()
        );
    }
}

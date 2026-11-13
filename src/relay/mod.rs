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
//! - The fetches by id (`GetDocuments`, `ResolveParents`, `FetchValues`):
//!   each id routed to the child whose slot range holds it, the answers
//!   put back in the caller's order (an id repeated answered once per
//!   occurrence, a foreign id left out, as a node does), the known flags
//!   and projection types merged, and the read receipt a relay token
//!   over the children's versions (`read_receipt`).
//! - `BrowseShard`: every child pages from the same boundary, the rows
//!   merged in the request's sort order (id order unsorted) and cut to
//!   `k`, the sort column types agreeing where known.
//! - The folds (`AggregateShard`, `QuantileCounts` with or without a
//!   Boolean plan, and `BooleanQuery.aggregate` inside `EvaluateBoolean`):
//!   the children's partials folded in child order through the root's
//!   own fold ([`merge_aggregate_shares`]) and answered as one shard's
//!   partial, the caps applied where the root applies them.
//!
//! Every other `NodeService` route refuses UNIMPLEMENTED naming the route
//! and the relay: no ingest, no administration, and no per-shard fusion
//! (`HybridShard`: the two-level mode fuses one list per shard at the
//! root and is partition dependent by design, so a relay standing as one
//! shard would change the answer).
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

mod map;
mod merge_lexical;
mod merge_values;
mod merge_vector;
#[cfg(test)]
mod tests;

pub use map::{
    merge_prefix_expansions, merge_shard_layouts, merge_suggest_terms, CoordinatorMapSource,
    MapSnapshot, MapSource, RelayMap, TokenRegistry, TokenTuple,
};
pub use merge_lexical::{
    concat_bitmaps, merge_bm25_responses, merge_boolean_responses, merge_term_stats, ChildBitmap,
};
pub use merge_values::{merge_aggregate_shares, merge_browse_pages, merge_health};
pub use merge_vector::merge_vector_backend;

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

    // fetch_update is deprecated in favor of try_update from Rust 1.99, but the
    // Pi binaries are built on 1.97, which lacks try_update.
    #[allow(deprecated)]
    fn accept_seq(&self, seq: u32) -> bool {
        self.last_seq
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |last| {
                (seq > last).then_some(seq)
            })
            .is_ok()
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
    pub(super) relay: RelayService,
    pub(super) process: crate::diagnostics::CoordinatorDiagnostics,
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
         (ResolveFilterBitmap, ResolveLexicalBitmap, ResolveVectorBitmap), the \
         dictionaries (ExpandTermPrefix, SuggestTerms), the fetches by id (GetDocuments, \
         ResolveParents, FetchValues, BrowseShard), the folds (AggregateShard, \
         QuantileCounts, EvaluateBoolean); see docs/relay-coordinators.md"
    ))
}
impl RelayService {
    /// The children, the map they were pinned under, and the parent's
    /// claim translated per child: the opening every composed unary
    /// route shares.
    fn open_children(
        &self,
        route: &str,
        visibility: Option<&crate::pb::DocumentVisibility>,
        epoch: u64,
        incarnation: &[u8],
    ) -> Result<
        (
            MapSnapshot,
            CoordinatorServiceImpl,
            Vec<String>,
            Vec<StatsClaim>,
        ),
        Status,
    > {
        crate::visibility::VisibilityScope::new(visibility)?;
        let (pinned, frozen) = self.pin();
        let children = frozen.node_addresses().to_vec();
        if children.is_empty() {
            return Err(Status::failed_precondition(format!(
                "relay: a relay coordinator has no children ({route})"
            )));
        }
        let claims = self.child_claims(epoch, incarnation, children.len())?;
        Ok((pinned, frozen, children, claims))
    }
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
        RelayService::stream_search(self, request).await
    }

    async fn term_stats(
        &self,
        request: Request<TermStatsRequest>,
    ) -> Result<Response<TermStatsResponse>, Status> {
        RelayService::term_stats(self, request).await
    }

    async fn health(
        &self,
        request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        RelayService::health(self, request).await
    }

    async fn search_shard(
        &self,
        request: Request<Streaming<SearchShardRequest>>,
    ) -> Result<Response<Self::SearchShardStream>, Status> {
        RelayService::search_shard(self, request).await
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
        RelayService::bm25_query_stream(self, request).await
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
        RelayService::get_vector_backend(self, request).await
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
        RelayService::expand_term_prefix(self, request).await
    }

    async fn suggest_terms(
        &self,
        request: Request<crate::pb::SuggestTermsRequest>,
    ) -> Result<Response<crate::pb::SuggestTermsResponse>, Status> {
        RelayService::suggest_terms(self, request).await
    }

    async fn bm25_query(
        &self,
        request: Request<Bm25QueryRequest>,
    ) -> Result<Response<Bm25QueryResponse>, Status> {
        RelayService::bm25_query(self, request).await
    }

    async fn bm25_phrase_query(
        &self,
        request: Request<Bm25PhraseQueryRequest>,
    ) -> Result<Response<Bm25QueryResponse>, Status> {
        RelayService::bm25_phrase_query(self, request).await
    }

    async fn get_documents(
        &self,
        request: Request<crate::pb::GetDocumentsRequest>,
    ) -> Result<Response<crate::pb::GetDocumentsResponse>, Status> {
        RelayService::get_documents(self, request).await
    }

    async fn resolve_parents(
        &self,
        request: Request<crate::pb::ResolveParentsRequest>,
    ) -> Result<Response<crate::pb::ResolveParentsResponse>, Status> {
        RelayService::resolve_parents(self, request).await
    }

    async fn bm25_rescore(
        &self,
        request: Request<Bm25RescoreRequest>,
    ) -> Result<Response<Bm25RescoreResponse>, Status> {
        RelayService::bm25_rescore(self, request).await
    }

    async fn fetch_values(
        &self,
        request: Request<crate::pb::FetchValuesRequest>,
    ) -> Result<Response<crate::pb::FetchValuesResponse>, Status> {
        RelayService::fetch_values(self, request).await
    }

    async fn aggregate_shard(
        &self,
        request: Request<crate::pb::AggregateShardRequest>,
    ) -> Result<Response<crate::pb::AggregateShardResponse>, Status> {
        RelayService::aggregate_shard(self, request).await
    }

    async fn quantile_counts(
        &self,
        request: Request<crate::pb::QuantileCountsRequest>,
    ) -> Result<Response<crate::pb::QuantileCountsResponse>, Status> {
        RelayService::quantile_counts(self, request).await
    }

    async fn vector_rescore(
        &self,
        request: Request<crate::pb::VectorRescoreRequest>,
    ) -> Result<Response<crate::pb::VectorRescoreResponse>, Status> {
        RelayService::vector_rescore(self, request).await
    }

    async fn exact_vector_rescore(
        &self,
        request: Request<crate::pb::ExactVectorRescoreRequest>,
    ) -> Result<Response<crate::pb::ExactVectorRescoreResponse>, Status> {
        RelayService::exact_vector_rescore(self, request).await
    }

    async fn hybrid_shard(
        &self,
        _request: Request<crate::pb::HybridShardRequest>,
    ) -> Result<Response<crate::pb::HybridShardResponse>, Status> {
        Err(Status::unimplemented(
            "relay: NodeService.HybridShard is not served by a relay coordinator: two-level \
             fusion fuses one list per shard at the root and is partition dependent by \
             design, so a relay standing as one shard would change the answer; use the \
             fused routes (ShardLegs, the cascade) through a relay",
        ))
    }

    async fn shard_legs(
        &self,
        request: Request<ShardLegsRequest>,
    ) -> Result<Response<ShardLegsResponse>, Status> {
        RelayService::shard_legs(self, request).await
    }

    async fn browse_shard(
        &self,
        request: Request<crate::pb::BrowseShardRequest>,
    ) -> Result<Response<crate::pb::BrowseShardResponse>, Status> {
        RelayService::browse_shard(self, request).await
    }

    async fn resolve_filter_bitmap(
        &self,
        request: Request<crate::pb::FilterBitmapRequest>,
    ) -> Result<Response<crate::pb::FilterBitmapResponse>, Status> {
        RelayService::resolve_filter_bitmap(self, request).await
    }

    async fn resolve_lexical_bitmap(
        &self,
        request: Request<crate::pb::LexicalBitmapRequest>,
    ) -> Result<Response<crate::pb::MembershipBitmapResponse>, Status> {
        RelayService::resolve_lexical_bitmap(self, request).await
    }

    async fn resolve_vector_bitmap(
        &self,
        request: Request<crate::pb::VectorBitmapRequest>,
    ) -> Result<Response<crate::pb::MembershipBitmapResponse>, Status> {
        RelayService::resolve_vector_bitmap(self, request).await
    }

    async fn evaluate_boolean(
        &self,
        request: Request<crate::pb::BooleanShardRequest>,
    ) -> Result<Response<crate::pb::BooleanShardResponse>, Status> {
        RelayService::evaluate_boolean(self, request).await
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

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
//!
//! Every other `NodeService` route refuses UNIMPLEMENTED naming the route
//! and the relay: no ingest, no administration, no aggregation, no
//! follow-up fetches through this level yet.
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
    stream_search_request, stream_search_response, HealthRequest, HealthResponse,
    StreamSearchRequest, StreamSearchResponse, StreamSearchSummary, TermStatsRequest,
    TermStatsResponse,
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
    pub epochs: Vec<u64>,
}

/// Relay tokens (`docs/relay-coordinators.md`, "The epoch token"): a
/// nonzero allocation bound to a [`TokenTuple`], reused while the tuple
/// repeats so a parent's stats cache keeps hitting, replaced the moment
/// any child's epoch moves, retained for [`RETAINED_TOKENS`] tuples, and
/// unknown after that or after a restart (the incarnation half of the
/// token differs), which the parent sees as the stale-epoch refusal.
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

    fn allocate(&mut self, tuple: TokenTuple) -> u64 {
        if let Some(position) = self.entries.iter().position(|(_, t)| *t == tuple) {
            let entry = self.entries.remove(position).expect("position from iter");
            let token = entry.0;
            self.entries.push_back(entry);
            return token;
        }
        self.counter = self.counter.wrapping_add(1).max(1);
        let token = (self.incarnation << 32) | u64::from(self.counter);
        self.entries.push_back((token, tuple));
        while self.entries.len() > RETAINED_TOKENS {
            self.entries.pop_front();
        }
        token
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
    pub fn translate_epoch(&self, token: u64) -> Result<Vec<u64>, Status> {
        if token == 0 {
            return Ok(vec![0; self.children().len()]);
        }
        Ok(self.token_tuple(token)?.epochs)
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

/// The refusal every route outside the relay's scope answers with.
fn refused(route: &str) -> Status {
    Status::unimplemented(format!(
        "relay: NodeService.{route} is not served by a relay coordinator; a relay forwards \
         StreamSearch, TermStats, and Health only (docs/relay-coordinators.md)"
    ))
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
        doc_count,
        total_doc_length,
        doc_frequencies,
        field_stats,
        stats_epoch: 0,
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
    let identity_limits = start.identity_limits.clone();
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
        identity_limits.clone(),
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
                        None => range.clone(),
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

#[tonic::async_trait]
impl NodeService for RelayService {
    type SearchShardStream = ReceiverStream<Result<crate::pb::SearchShardResponse, Status>>;
    type StreamSearchStream =
        crate::metrics::Timed<ReceiverStream<Result<StreamSearchResponse, Status>>>;
    type Bm25QueryStreamStream = ReceiverStream<Result<crate::pb::Bm25QueryStreamResponse, Status>>;
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
            self.still_current(&pinned, "TermStats")?;
            let tuple = TokenTuple {
                collection: self.collection.clone(),
                control_revision: pinned.control_revision,
                topology_generation: pinned.topology_generation,
                children,
                epochs: shares.iter().map(|s| s.stats_epoch).collect(),
            };
            merged.stats_epoch = self
                .tokens
                .lock()
                .expect("relay token registry poisoned")
                .allocate(tuple);
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
        _request: Request<Streaming<crate::pb::SearchShardRequest>>,
    ) -> Result<Response<Self::SearchShardStream>, Status> {
        Err(refused("SearchShard"))
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
        _request: Request<Streaming<crate::pb::Bm25QueryStreamRequest>>,
    ) -> Result<Response<Self::Bm25QueryStreamStream>, Status> {
        Err(refused("Bm25QueryStream"))
    }

    async fn read_wal(
        &self,
        _request: Request<crate::pb::ReadWalRequest>,
    ) -> Result<Response<Self::ReadWalStream>, Status> {
        Err(refused("ReadWal"))
    }

    async fn get_vector_backend(
        &self,
        _request: Request<crate::pb::GetVectorBackendRequest>,
    ) -> Result<Response<crate::pb::GetVectorBackendResponse>, Status> {
        Err(refused("GetVectorBackend"))
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
        _request: Request<crate::pb::ExpandTermPrefixRequest>,
    ) -> Result<Response<crate::pb::ExpandTermPrefixResponse>, Status> {
        Err(refused("ExpandTermPrefix"))
    }

    async fn suggest_terms(
        &self,
        _request: Request<crate::pb::SuggestTermsRequest>,
    ) -> Result<Response<crate::pb::SuggestTermsResponse>, Status> {
        Err(refused("SuggestTerms"))
    }

    async fn bm25_query(
        &self,
        _request: Request<crate::pb::Bm25QueryRequest>,
    ) -> Result<Response<crate::pb::Bm25QueryResponse>, Status> {
        Err(refused("Bm25Query"))
    }

    async fn bm25_phrase_query(
        &self,
        _request: Request<crate::pb::Bm25PhraseQueryRequest>,
    ) -> Result<Response<crate::pb::Bm25QueryResponse>, Status> {
        Err(refused("Bm25PhraseQuery"))
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
        _request: Request<crate::pb::Bm25RescoreRequest>,
    ) -> Result<Response<crate::pb::Bm25RescoreResponse>, Status> {
        Err(refused("Bm25Rescore"))
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
        _request: Request<crate::pb::VectorRescoreRequest>,
    ) -> Result<Response<crate::pb::VectorRescoreResponse>, Status> {
        Err(refused("VectorRescore"))
    }

    async fn exact_vector_rescore(
        &self,
        _request: Request<crate::pb::ExactVectorRescoreRequest>,
    ) -> Result<Response<crate::pb::ExactVectorRescoreResponse>, Status> {
        Err(refused("ExactVectorRescore"))
    }

    async fn hybrid_shard(
        &self,
        _request: Request<crate::pb::HybridShardRequest>,
    ) -> Result<Response<crate::pb::HybridShardResponse>, Status> {
        Err(refused("HybridShard"))
    }

    async fn shard_legs(
        &self,
        _request: Request<crate::pb::ShardLegsRequest>,
    ) -> Result<Response<crate::pb::ShardLegsResponse>, Status> {
        Err(refused("ShardLegs"))
    }

    async fn browse_shard(
        &self,
        _request: Request<crate::pb::BrowseShardRequest>,
    ) -> Result<Response<crate::pb::BrowseShardResponse>, Status> {
        Err(refused("BrowseShard"))
    }

    async fn resolve_filter_bitmap(
        &self,
        _request: Request<crate::pb::FilterBitmapRequest>,
    ) -> Result<Response<crate::pb::FilterBitmapResponse>, Status> {
        Err(refused("ResolveFilterBitmap"))
    }

    async fn resolve_lexical_bitmap(
        &self,
        _request: Request<crate::pb::LexicalBitmapRequest>,
    ) -> Result<Response<crate::pb::MembershipBitmapResponse>, Status> {
        Err(refused("ResolveLexicalBitmap"))
    }

    async fn resolve_vector_bitmap(
        &self,
        _request: Request<crate::pb::VectorBitmapRequest>,
    ) -> Result<Response<crate::pb::MembershipBitmapResponse>, Status> {
        Err(refused("ResolveVectorBitmap"))
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
            epochs: epochs.to_vec(),
        }
    }

    #[test]
    fn tokens_repeat_for_one_tuple_and_move_with_an_epoch() {
        let mut registry = TokenRegistry::new(7);
        let t1 = registry.allocate(tuple(&[1, 1]));
        assert_ne!(t1, 0);
        assert_eq!(t1 >> 32, 7, "the incarnation is the high half");
        assert_eq!(
            registry.allocate(tuple(&[1, 1])),
            t1,
            "same tuple, same token"
        );
        let t2 = registry.allocate(tuple(&[2, 1]));
        assert_ne!(t2, t1);
        assert_eq!(registry.lookup(t1).unwrap().epochs, vec![1, 1]);
        assert_eq!(registry.lookup(t2).unwrap().epochs, vec![2, 1]);
        assert!(registry.lookup(t1 + 1000).is_none());
    }

    #[test]
    fn retention_is_bounded() {
        let mut registry = TokenRegistry::new(1);
        let first = registry.allocate(tuple(&[0, 0]));
        for i in 1..=(RETAINED_TOKENS as u64) {
            registry.allocate(tuple(&[i, 0]));
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
        }
    }

    fn request() -> TermStatsRequest {
        TermStatsRequest {
            terms: vec!["court".into(), "opinion".into()],
            fields: vec![crate::pb::FieldTerms {
                field: "title".into(),
                terms: vec!["court".into(), "opinion".into()],
            }],
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

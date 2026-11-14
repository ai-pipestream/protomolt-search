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
//!   (`StartStreamSearch` carries a candidate depth, not a k; the relay
//!   forwards it unchanged) and raises no floor of its own.
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

mod bitmaps;
mod diagnostics;
mod health;
mod lexical;
mod map;
mod merge_lexical;
mod service;
mod shard_search;
mod signals;
mod stream_search;
#[cfg(test)]
mod tests;
mod values;
mod vector;

pub use self::bitmaps::{concat_bitmaps, ChildBitmap};
pub use self::diagnostics::RelayDiagnostics;
pub use self::health::{merge_health, merge_shard_layouts};
pub use self::map::{
    CoordinatorMapSource, MapSnapshot, MapSource, RelayMap, TokenRegistry, TokenTuple,
    RETAINED_TOKENS,
};
pub use self::merge_lexical::{
    merge_bm25_responses, merge_boolean_responses, merge_prefix_expansions, merge_suggest_terms,
    merge_term_stats,
};
pub use self::values::{merge_aggregate_shares, merge_browse_pages};
pub use self::vector::merge_vector_backend;

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tonic::{Request, Response, Status};

use crate::coordinator::CoordinatorServiceImpl;
use crate::node::STALE_STATS_EPOCH;
use crate::pb::node_service_server::NodeServiceServer;
use crate::pb::{HealthRequest, HealthResponse};
use crate::stats_identity::{StatsClaim, StatsIncarnation};

#[cfg(feature = "net")]
use self::signals::apply_datagram;
use self::signals::RelaySignals;
use self::stream_search::child_error;

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

/// The gRPC deadline of a request, from its `grpc-timeout` header.
pub(crate) fn grpc_timeout(metadata: &tonic::metadata::MetadataMap) -> Option<Duration> {
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

/// One request per child (`None` asks that child nothing), the answers
/// paired with their child index in child order; a child's error is
/// named by child and address.
async fn fan_out<Req, Resp, F, Fut>(
    frozen: &CoordinatorServiceImpl,
    children: &[String],
    requests: Vec<Option<Req>>,
    timeout: Option<Duration>,
    route: &'static str,
    call: F,
) -> Result<Vec<(usize, Resp)>, Status>
where
    Req: Send + 'static,
    Resp: Send + 'static,
    F: Fn(crate::link::NodeLink, Request<Req>) -> Fut + Clone + Send + 'static,
    Fut: std::future::Future<Output = Result<Response<Resp>, Status>> + Send + 'static,
{
    let mut tasks = Vec::with_capacity(children.len());
    for (shard, child_req) in requests.into_iter().enumerate() {
        let Some(child_req) = child_req else {
            continue;
        };
        let link = frozen.node_client(&children[shard])?;
        let addr = children[shard].clone();
        let call = call.clone();
        tasks.push((
            shard,
            tokio::spawn(async move {
                let mut request = Request::new(child_req);
                if let Some(timeout) = timeout {
                    request.set_timeout(timeout);
                }
                call(link, request)
                    .await
                    .map(|r| r.into_inner())
                    .map_err(|status| child_error(shard, &addr, route, status))
            }),
        ));
    }
    let mut out = Vec::with_capacity(tasks.len());
    for (shard, task) in tasks {
        let share = task
            .await
            .map_err(|e| Status::internal(format!("relay {route} task: {e}")))??;
        out.push((shard, share));
    }
    Ok(out)
}

/// The children's answers put back in the caller's id order, each
/// occurrence of an id served once, the way a node answers a list that
/// repeats an id. An id no child answered is absent, as a node leaves
/// out an id it does not serve.
fn in_caller_order<T>(ids: &[u64], items: Vec<T>, id_of: impl Fn(&T) -> u64) -> Vec<T> {
    let mut by_id: HashMap<u64, VecDeque<T>> = HashMap::new();
    for item in items {
        by_id.entry(id_of(&item)).or_default().push_back(item);
    }
    let mut out = Vec::with_capacity(ids.len());
    for id in ids {
        if let Some(item) = by_id.get_mut(id).and_then(VecDeque::pop_front) {
            out.push(item);
        }
    }
    out
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

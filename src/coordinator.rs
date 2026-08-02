//! Coordinator side: client-facing [`SearchService`] that fans queries out
//! to shard nodes, aggregates their floors mid-scan, and merges results.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status};

use crate::bm25::{self, Bm25Params, CorpusStats};
use crate::fusion::{self, Leg};
use crate::merge::{cmp_hits, merge_topk, FloorTracker, MergedHit};
use crate::pb::node_service_client::NodeServiceClient;
use crate::pb::search_service_server::{SearchService, SearchServiceServer};
use crate::pb::{
    search_shard_request, search_shard_response, Bm25Hit, Bm25QueryRequest, Bm25RescoreRequest,
    Bm25SearchRequest, Bm25SearchResponse, BroadcastCalibrationRequest,
    BroadcastCalibrationResponse, CalibrationApplyResult, CascadeHit, ClusterHealthRequest,
    ClusterHealthResponse, FloorUpdate, FusionMode, HealthRequest, HybridDebug, HybridHit,
    HybridSearchRequest, HybridSearchResponse, HybridShardDebug, HybridShardRequest, ParentGroup,
    ScoredHit, SearchRequest, SearchResponse, SearchShardDone, SearchShardRequest,
    SearchShardResponse, SetCalibrationRequest, ShardHealth, ShardLegsRequest, ShardScanStats,
    StartShardSearch, StartStreamSearch, StreamSearchRequest, StreamSearchResponse,
    StreamSearchSummary, TermStatsRequest, VectorRescoreRequest,
};
use crate::pb::{
    search_variant, InterleaveTeam, Interleaving, RankedHit, RankingDiff, VariantResult,
    VariantSearchRequest, VariantSearchResponse,
};
use crate::rankdiff;
use crate::pb::{stream_search_request, stream_search_response};

/// Process-unique request id counter for coordinator-assigned ids.
static REQUEST_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Per-shard timing controls for the fan-out (all off by default).
#[derive(Debug, Clone, Copy, Default)]
pub struct FanoutLimits {
    /// Hard bound on one shard's whole attempt (primary + any hedge).
    /// A shard that blows the deadline fails the query with
    /// DEADLINE_EXCEEDED rather than stalling it forever.
    pub shard_deadline: Option<Duration>,
    /// How long to wait on the primary before opening the identical
    /// search on the shard's replica and racing the two. Only acts on
    /// shards that have a replica configured.
    pub hedge_delay: Option<Duration>,
}

/// The coordinator gRPC service.
///
/// Membership is static: the node address list is fixed at construction
/// and every query fans out to every node. Connections are pooled: one
/// lazily-established HTTP/2 channel per node address, multiplexing every
/// concurrent query and reconnecting on its own after a node restart.
#[derive(Clone)]
pub struct CoordinatorServiceImpl {
    /// Node addresses in `http://host:port` form, in stable shard order
    /// (index in this list is the shard index used for tie-breaking).
    node_addrs: Vec<String>,
    /// Optional replica address per shard (same data, exact same
    /// results), the target for hedged retries.
    replica_addrs: Vec<Option<String>>,
    /// Analysis sidecar address for query analysis in Bm25Search.
    analysis_addr: Option<String>,
    /// BM25 tuning sent to every shard (identical scoring everywhere).
    bm25_params: Bm25Params,
    /// Per-shard deadline and hedging controls.
    limits: FanoutLimits,
    /// Serve `SearchService.Search` over the streaming protocol
    /// (`fanout_stream_search`) instead of the per-shard top-k fan-out.
    stream_search: bool,
    /// One reusable channel per address, created on first use.
    channels: Arc<Mutex<HashMap<String, Channel>>>,
    /// Lazily bound UDP socket for the floor fast lane (`None` when the
    /// bind failed; floors then ride the gRPC streams alone).
    floor_socket: Arc<std::sync::OnceLock<Option<std::net::UdpSocket>>>,
    /// Resolved UDP floor target per node address (`None` =
    /// unresolvable), cached on first use. IPv4 preferred.
    floor_targets: Arc<Mutex<HashMap<String, Option<std::net::SocketAddr>>>>,
}

/// A process-unique, well-mixed stream token for the UDP floor lane
/// (0 is reserved for "no UDP"). Tokens route datagrams to in-flight
/// streams on a trusted network; they are unique, not secret.
fn floor_token() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let raw = (u64::from(std::process::id()) << 32) ^ NEXT.fetch_add(1, AtomicOrdering::Relaxed);
    // splitmix64 finalizer.
    let mut z = raw.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^= z >> 31;
    z.max(1)
}

impl CoordinatorServiceImpl {
    /// A coordinator over the given shard nodes (fan-out order = shard
    /// index for merge tie-breaks).
    pub fn new(node_addrs: Vec<String>) -> Self {
        Self {
            node_addrs,
            replica_addrs: Vec::new(),
            analysis_addr: None,
            bm25_params: Bm25Params::default(),
            limits: FanoutLimits::default(),
            stream_search: false,
            channels: Arc::new(Mutex::new(HashMap::new())),
            floor_socket: Arc::new(std::sync::OnceLock::new()),
            floor_targets: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// The UDP floor socket, bound once (nonblocking: a full local
    /// buffer drops the datagram, which a monotone hint tolerates).
    fn floor_socket(&self) -> Option<&std::net::UdpSocket> {
        self.floor_socket
            .get_or_init(|| {
                std::net::UdpSocket::bind(("0.0.0.0", 0)).ok().map(|s| {
                    let _ = s.set_nonblocking(true);
                    s
                })
            })
            .as_ref()
    }

    /// The UDP floor target for a node address: the same host:port as
    /// its gRPC listener, in the UDP namespace. Resolved once and
    /// cached; IPv4 preferred (the fleet pins IPv4).
    fn floor_target(&self, addr: &str) -> Option<std::net::SocketAddr> {
        let mut cache = self
            .floor_targets
            .lock()
            .expect("floor target cache poisoned");
        if let Some(target) = cache.get(addr) {
            return *target;
        }
        let stripped = addr
            .strip_prefix("http://")
            .or_else(|| addr.strip_prefix("https://"))
            .unwrap_or(addr);
        let resolved = std::net::ToSocketAddrs::to_socket_addrs(stripped)
            .ok()
            .and_then(|addrs| {
                let all: Vec<std::net::SocketAddr> = addrs.collect();
                all.iter()
                    .find(|a| a.is_ipv4())
                    .copied()
                    .or(all.first().copied())
            });
        cache.insert(addr.to_string(), resolved);
        resolved
    }

    /// Serve plain vector `Search` over the streaming protocol: shards
    /// emit above the relayed floor and this coordinator holds the only
    /// top-k. Results are identical to the default fan-out; the modes
    /// differ in where pruning happens, not in what they return.
    pub fn with_stream_search(mut self, on: bool) -> Self {
        self.stream_search = on;
        self
    }

    /// Configure the BM25 path: analysis sidecar for query analysis and
    /// the scoring parameters every shard is told to use.
    pub fn with_bm25(mut self, analysis_addr: Option<String>, params: Bm25Params) -> Self {
        self.analysis_addr = analysis_addr;
        self.bm25_params = params;
        self
    }

    /// Configure per-shard deadlines and hedging.
    pub fn with_limits(mut self, limits: FanoutLimits) -> Self {
        self.limits = limits;
        self
    }

    /// Configure replica addresses (one optional entry per shard, same
    /// order as the node list). A replica must serve identical data —
    /// searches are exact, so either copy returns identical results.
    pub fn with_replicas(mut self, replica_addrs: Vec<Option<String>>) -> Self {
        self.replica_addrs = replica_addrs;
        self
    }

    /// The pooled channel for `addr`, created lazily on first use.
    /// `connect_lazy` defers the TCP/HTTP2 handshake to the first RPC and
    /// transparently reconnects after failures, so one entry serves the
    /// address for the process lifetime.
    fn channel_to(&self, addr: &str) -> Result<Channel, Status> {
        let mut cache = self.channels.lock().expect("channel cache mutex poisoned");
        if let Some(ch) = cache.get(addr) {
            return Ok(ch.clone());
        }
        let endpoint = Endpoint::from_shared(addr.to_string())
            .map_err(|e| Status::unavailable(format!("invalid node address {addr}: {e}")))?
            .tcp_nodelay(true)
            // The client end is the RECEIVER of stream batches, so these
            // windows are what let a shard's pre-floor burst flow without
            // window-update round trips (see H2_STREAM_WINDOW).
            .initial_stream_window_size(crate::H2_STREAM_WINDOW)
            .initial_connection_window_size(crate::H2_CONN_WINDOW);
        let ch = endpoint.connect_lazy();
        cache.insert(addr.to_string(), ch.clone());
        Ok(ch)
    }

    /// A client over the pooled channel for `addr` with the message size
    /// limits applied. Cheap: clones the channel, no new connection.
    fn node_client(&self, addr: &str) -> Result<NodeServiceClient<Channel>, Status> {
        Ok(NodeServiceClient::new(self.channel_to(addr)?)
            .max_decoding_message_size(crate::MAX_MESSAGE_BYTES)
            .max_encoding_message_size(crate::MAX_MESSAGE_BYTES))
    }

    /// Distributed BM25 with the two-phase global-stats flow (see the
    /// proto comments on `SearchService.Bm25Search`), unseeded.
    pub async fn fanout_bm25(
        &self,
        text: &str,
        k: u32,
        spec: Option<&crate::pb::AnalysisSpec>,
    ) -> Result<Vec<Bm25Hit>, Status> {
        self.fanout_bm25_seeded(text, k, spec, 0.0).await
    }

    /// [`Self::fanout_bm25`] with a client-supplied floor: `min_score`
    /// is forwarded verbatim to every shard's `Bm25QueryRequest`, which
    /// prunes postings that provably cannot reach it (docs/block-max.md,
    /// stage 4). 0 means unseeded. There is deliberately NO mid-query
    /// relay: `Bm25Query` is unary, so a fleet-wide floor can only ever
    /// arrive with the request.
    pub async fn fanout_bm25_seeded(
        &self,
        text: &str,
        k: u32,
        spec: Option<&crate::pb::AnalysisSpec>,
        min_score: f32,
    ) -> Result<Vec<Bm25Hit>, Status> {
        let addr = self.analysis_addr.clone().ok_or_else(|| {
            Status::unavailable("no analysis sidecar configured on the coordinator (analysis_addr)")
        })?;
        // (a) Query analysis with the SAME options as ingest: query terms
        // share identity with indexed terms (stems when SOURCE_STEMS).
        let analyzed = crate::analyzer::analyze_document(&addr, text, spec).await?;
        let mut terms: Vec<String> = Vec::new();
        for (term, _, _) in analyzed.into_body().terms {
            if !terms.contains(&term) {
                terms.push(term);
            }
        }
        if terms.is_empty() || k == 0 {
            return Ok(Vec::new());
        }

        // (b) TermStats fan-out: each shard's share of the corpus stats.
        let global: CorpusStats = self.global_bm25_stats(&terms).await?;

        // (c) Bm25Query fan-out with the GLOBAL stats: every shard scores
        // identically, so (d) the merge is a straight top-k.
        let mut query_tasks = Vec::with_capacity(self.node_addrs.len());
        for (shard, node) in self.node_addrs.iter().enumerate() {
            let request = Bm25QueryRequest {
                terms: terms.clone(),
                k,
                global_doc_count: global.doc_count,
                global_total_doc_length: global.total_doc_length,
                global_doc_frequencies: global.dfs.clone(),
                k1: self.bm25_params.k1 as f32,
                b: self.bm25_params.b as f32,
                min_score,
                fields: Vec::new(),
            };
            let mut client = self.node_client(node)?;
            query_tasks.push(tokio::spawn(async move {
                client
                    .bm25_query(request)
                    .await
                    .map(|r| (shard as u32, r.into_inner().hits))
            }));
        }
        let mut all: Vec<(u32, Bm25Hit)> = Vec::new();
        for task in query_tasks {
            let (shard, hits) = task
                .await
                .map_err(|e| Status::internal(format!("bm25 query task failed: {e}")))??;
            all.extend(hits.into_iter().map(|h| (shard, h)));
        }
        all.sort_by(|(sa, a), (sb, b)| {
            b.score
                .total_cmp(&a.score)
                .then_with(|| sa.cmp(sb))
                .then_with(|| a.doc_id.cmp(&b.doc_id))
        });
        all.truncate(k as usize);
        Ok(all.into_iter().map(|(_, h)| h).collect())
    }

    /// Fused multi-field Bm25Search (`docs/multi-field.md`): `text` is
    /// analyzed once per entry under THAT entry's analysis (term
    /// identity is per field), ONE TermStats round carries every
    /// field's terms, and the Bm25Query fan-out sends per-field legs in
    /// entry order — the pinned accumulation order, identical on every
    /// shard, so distributed fused scores match the monolith's bits.
    pub async fn fanout_bm25_fused(
        &self,
        text: &str,
        k: u32,
        fields: &[crate::pb::QueryField],
        min_score: f32,
    ) -> Result<Vec<Bm25Hit>, Status> {
        let addr = self.analysis_addr.clone().ok_or_else(|| {
            Status::unavailable("no analysis sidecar configured on the coordinator (analysis_addr)")
        })?;
        let mut seen: Vec<&str> = Vec::new();
        for f in fields {
            if f.field.is_empty() {
                return Err(Status::invalid_argument("QueryField.field must be named"));
            }
            if seen.contains(&f.field.as_str()) {
                return Err(Status::invalid_argument(format!(
                    "field {:?} repeats in the query",
                    f.field
                )));
            }
            if f.weight < 0.0 || f.weight.is_nan() {
                return Err(Status::invalid_argument(format!(
                    "field {:?}: weight must be >= 0",
                    f.field
                )));
            }
            seen.push(&f.field);
        }
        // (a) Query analysis per field, each under its own spec.
        let mut field_terms: Vec<Vec<String>> = Vec::with_capacity(fields.len());
        for f in fields {
            let analyzed =
                crate::analyzer::analyze_document(&addr, text, f.analysis.as_ref()).await?;
            let mut terms: Vec<String> = Vec::new();
            for (term, _, _) in analyzed.into_body().terms {
                if !terms.contains(&term) {
                    terms.push(term);
                }
            }
            field_terms.push(terms);
        }
        if k == 0 || field_terms.iter().all(|t| t.is_empty()) {
            return Ok(Vec::new());
        }
        // (b) One TermStats fan-out with every field's terms; shares
        // merge elementwise per field, N summed once (it is shared —
        // a document is a document).
        let stats_fields: Vec<crate::pb::FieldTerms> = fields
            .iter()
            .zip(&field_terms)
            .map(|(f, terms)| crate::pb::FieldTerms {
                field: f.field.clone(),
                terms: terms.clone(),
            })
            .collect();
        let mut share_tasks = Vec::with_capacity(self.node_addrs.len());
        for node in &self.node_addrs {
            let request = TermStatsRequest {
                terms: Vec::new(),
                fields: stats_fields.clone(),
            };
            let mut client = self.node_client(node)?;
            share_tasks.push(tokio::spawn(async move {
                client.term_stats(request).await.map(|r| r.into_inner())
            }));
        }
        let mut doc_count = 0u64;
        let mut totals = vec![0u64; fields.len()];
        let mut dfs: Vec<Vec<u32>> = field_terms.iter().map(|t| vec![0u32; t.len()]).collect();
        // Which fields any shard actually has. A field no shard knows is
        // a typo, not a query: scoring it as "contributes nothing" would
        // silently return the ranking of the REMAINING fields, so a
        // misspelled arm of an A/B reads as "no difference".
        let mut known_somewhere = vec![false; fields.len()];
        for task in share_tasks {
            let share = task
                .await
                .map_err(|e| Status::internal(format!("term stats task failed: {e}")))??;
            if share.field_stats.len() != fields.len() {
                return Err(Status::internal(format!(
                    "shard returned {} field stats for {} fields",
                    share.field_stats.len(),
                    fields.len()
                )));
            }
            doc_count += share.doc_count;
            for (fi, fs) in share.field_stats.iter().enumerate() {
                if fs.doc_frequencies.len() != dfs[fi].len() {
                    return Err(Status::internal("shard field stats df length mismatch"));
                }
                totals[fi] += fs.total_doc_length;
                known_somewhere[fi] |= fs.known;
                for (acc, df) in dfs[fi].iter_mut().zip(&fs.doc_frequencies) {
                    *acc += df;
                }
            }
        }
        // A partially-known field is tolerated: that is a real
        // heterogeneous fleet, and the shards that have it still
        // contribute. A field NO shard has is refused.
        let unknown: Vec<&str> = fields
            .iter()
            .zip(&known_somewhere)
            .filter(|(_, known)| !**known)
            .map(|(f, _)| f.field.as_str())
            .collect();
        if !unknown.is_empty() {
            return Err(Status::invalid_argument(format!(
                "no shard indexes {}: scoring an unknown field would silently return the \
                 remaining fields' ranking. Check the spelling, or the nodes' --bm25-fields.",
                unknown
                    .iter()
                    .map(|f| format!("{f:?}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }
        // (c) Bm25Query fan-out with per-field legs in entry order.
        // Entry k1/b of 0 pick up the coordinator's configured params,
        // so tuning reaches this path too.
        let legs: Vec<crate::pb::Bm25FieldLeg> = fields
            .iter()
            .enumerate()
            .map(|(fi, f)| crate::pb::Bm25FieldLeg {
                field: f.field.clone(),
                terms: field_terms[fi].clone(),
                global_total_doc_length: totals[fi],
                global_doc_frequencies: dfs[fi].clone(),
                weight: f.weight,
                k1: if f.k1 == 0.0 {
                    self.bm25_params.k1 as f32
                } else {
                    f.k1
                },
                b: if f.b == 0.0 {
                    self.bm25_params.b as f32
                } else {
                    f.b
                },
            })
            .collect();
        let mut query_tasks = Vec::with_capacity(self.node_addrs.len());
        for (shard, node) in self.node_addrs.iter().enumerate() {
            let request = Bm25QueryRequest {
                terms: Vec::new(),
                k,
                global_doc_count: doc_count,
                global_total_doc_length: 0,
                global_doc_frequencies: Vec::new(),
                k1: 0.0,
                b: 0.0,
                min_score,
                fields: legs.clone(),
            };
            let mut client = self.node_client(node)?;
            query_tasks.push(tokio::spawn(async move {
                client
                    .bm25_query(request)
                    .await
                    .map(|r| (shard as u32, r.into_inner().hits))
            }));
        }
        let mut all: Vec<(u32, Bm25Hit)> = Vec::new();
        for task in query_tasks {
            let (shard, hits) = task
                .await
                .map_err(|e| Status::internal(format!("bm25 query task failed: {e}")))??;
            all.extend(hits.into_iter().map(|h| (shard, h)));
        }
        all.sort_by(|(sa, a), (sb, b)| {
            b.score
                .total_cmp(&a.score)
                .then_with(|| sa.cmp(sb))
                .then_with(|| a.doc_id.cmp(&b.doc_id))
        });
        all.truncate(k as usize);
        Ok(all.into_iter().map(|(_, h)| h).collect())
    }

    /// Hybrid vector + BM25 search:
    ///
    /// 1. analyze `text` into query terms (same analysis options as
    ///    ingest, as in [`Self::fanout_bm25`]);
    /// 2. TermStats fan-out and merge into GLOBAL corpus stats;
    /// 3. fuse per `legs.fusion_mode`: GLOBAL_RANK and SCORE_BLEND fetch
    ///    raw shard legs and fuse once on the coordinator (RRF over
    ///    global ranks, or normalize-and-combine over global scores);
    ///    TWO_LEVEL lets each shard RRF-fuse locally and RRF-merges the
    ///    shard lists (the fallback for incomparable scores).
    pub async fn fanout_hybrid(
        &self,
        request_id: &str,
        text: &str,
        vector: &[f32],
        k: u32,
        spec: Option<&crate::pb::AnalysisSpec>,
        legs: HybridLegs,
        debug: bool,
    ) -> Result<(Vec<HybridHit>, Option<HybridDebug>), Status> {
        if k == 0 || vector.is_empty() {
            return Ok((Vec::new(), None));
        }
        let t_total = std::time::Instant::now();
        // Query analysis for the BM25 leg (same options as ingest).
        let addr = self.analysis_addr.clone().ok_or_else(|| {
            Status::unavailable("no analysis sidecar configured on the coordinator (analysis_addr)")
        })?;
        let t = std::time::Instant::now();
        let analyzed = crate::analyzer::analyze_document(&addr, text, spec).await?;
        let analysis_ms = t.elapsed().as_secs_f32() * 1e3;
        let mut terms: Vec<String> = Vec::new();
        for (term, _, _) in analyzed.into_body().terms {
            if !terms.contains(&term) {
                terms.push(term);
            }
        }

        let t = std::time::Instant::now();
        let global = self.global_bm25_stats(&terms).await?;
        let stats_ms = t.elapsed().as_secs_f32() * 1e3;
        let (hits, mut dbg) = match legs.fusion_mode {
            FusionMode::TwoLevel => {
                self.fanout_hybrid_two_level(request_id, vector, k, &terms, &global, legs, debug)
                    .await?
            }
            FusionMode::Decomposed => {
                self.fanout_hybrid_decomposed(request_id, vector, k, &terms, &global, legs, debug)
                    .await?
            }
            _ => {
                self.fanout_hybrid_global_rank(vector, k, &terms, &global, legs, debug)
                    .await?
            }
        };
        if let Some(d) = dbg.as_mut() {
            d.terms = terms;
            d.analysis_ms = analysis_ms;
            d.stats_ms = stats_ms;
            d.total_ms = t_total.elapsed().as_secs_f32() * 1e3;
        }
        Ok((hits, dbg))
    }

    /// TermStats fan-out: sum every shard's share into GLOBAL BM25 corpus
    /// stats for `terms`.
    async fn global_bm25_stats(&self, terms: &[String]) -> Result<CorpusStats, Status> {
        let mut share_tasks = Vec::with_capacity(self.node_addrs.len());
        for node in &self.node_addrs {
            let terms = terms.to_vec();
            let mut client = self.node_client(node)?;
            share_tasks.push(tokio::spawn(async move {
                client
                    .term_stats(TermStatsRequest {
                        terms,
                        fields: Vec::new(),
                    })
                    .await
                    .map(|r| r.into_inner())
            }));
        }
        let mut shares = Vec::with_capacity(share_tasks.len());
        for task in share_tasks {
            let stats = task
                .await
                .map_err(|e| Status::internal(format!("term stats task failed: {e}")))??;
            shares.push((
                stats.doc_count,
                stats.total_doc_length,
                stats.doc_frequencies,
            ));
        }
        Ok(bm25::merge_stats(&shares))
    }

    /// FUSION_MODE_GLOBAL_RANK: shards return RAW per-leg lists; the
    /// coordinator merges each leg across shards by raw score into global
    /// rankings and applies single-level RRF over them. With globally
    /// comparable scores per leg this is exactly the monolithic result
    /// for k <= leg_k (see the proto's FusionMode comments).
    async fn fanout_hybrid_global_rank(
        &self,
        vector: &[f32],
        k: u32,
        terms: &[String],
        global: &CorpusStats,
        legs: HybridLegs,
        debug: bool,
    ) -> Result<(Vec<HybridHit>, Option<HybridDebug>), Status> {
        let t_legs = std::time::Instant::now();
        let mut shard_tasks = Vec::with_capacity(self.node_addrs.len());
        for (shard, node) in self.node_addrs.iter().enumerate() {
            let request = ShardLegsRequest {
                request_id: String::new(),
                k: legs.leg_k,
                vector: vector.to_vec(),
                terms: terms.to_vec(),
                global_doc_count: global.doc_count,
                global_total_doc_length: global.total_doc_length,
                global_doc_frequencies: global.dfs.clone(),
                k1: self.bm25_params.k1 as f32,
                b: self.bm25_params.b as f32,
            };
            let mut client = self.node_client(node)?;
            shard_tasks.push(tokio::spawn(async move {
                let t0 = std::time::Instant::now();
                client.shard_legs(request).await.map(|r| {
                    (
                        shard as u32,
                        t0.elapsed().as_secs_f32() * 1e3,
                        r.into_inner(),
                    )
                })
            }));
        }
        let mut vector_shards = Vec::with_capacity(shard_tasks.len());
        let mut bm25_shards = Vec::with_capacity(shard_tasks.len());
        let mut shard_debug: Vec<HybridShardDebug> = Vec::new();
        let mut owner: std::collections::HashMap<u64, u32> = std::collections::HashMap::new();
        for task in shard_tasks {
            let (shard, rpc_ms, response) = task
                .await
                .map_err(|e| Status::internal(format!("shard legs task failed: {e}")))??;
            if debug {
                shard_debug.push(HybridShardDebug {
                    shard,
                    rpc_ms,
                    vector_hits: response.vector_hits.len() as u32,
                    bm25_hits: response.bm25_hits.len() as u32,
                    scan: None,
                });
            }
            for h in &response.vector_hits {
                owner.entry(h.doc_id).or_insert(shard);
            }
            for h in &response.bm25_hits {
                owner.entry(h.doc_id).or_insert(shard);
            }
            vector_shards.push((
                shard,
                response
                    .vector_hits
                    .into_iter()
                    .map(|h| (h.doc_id, f64::from(h.score)))
                    .collect::<Vec<_>>(),
            ));
            bm25_shards.push((
                shard,
                response
                    .bm25_hits
                    .into_iter()
                    .map(|h| (h.doc_id, f64::from(h.score)))
                    .collect::<Vec<_>>(),
            ));
        }

        let legs_ms = t_legs.elapsed().as_secs_f32() * 1e3;
        // Merge each leg into a GLOBAL ranking by raw score (deterministic
        // total order; see merge_legs_by_score), then fuse once: RRF for
        // GLOBAL_RANK, normalize-and-combine for SCORE_BLEND. The two
        // modes share this whole leg-fetch path; only the fusion function
        // differs. Weights arrive RESOLVED (the handler defaults absent
        // to 1.0); an exact 0 disables its leg, which both fusion
        // functions honor by skipping it.
        let t_fusion = std::time::Instant::now();
        let mut vector_global = fusion::merge_legs_by_score(vector_shards);
        let mut bm25_global = fusion::merge_legs_by_score(bm25_shards);
        // The vector-score floor: a hit must have a qualifying vector
        // score to survive, so docs below it (or absent from the vector
        // leg) drop from BOTH legs BEFORE fusion and truncation —
        // deeper qualifying docs are promoted, and blend statistics see
        // only the filtered set. Score-defined, hence layout-invariant.
        if legs.min_vector_score > 0.0 {
            let min = f64::from(legs.min_vector_score);
            vector_global.retain(|&(_, score, _)| score >= min);
            let allowed: std::collections::HashSet<u64> =
                vector_global.iter().map(|&(id, _, _)| id).collect();
            bm25_global.retain(|&(id, _, _)| allowed.contains(&id));
        }
        let leg_inputs = [
            Leg {
                hits: vector_global
                    .iter()
                    .map(|&(id, score, _)| (id, score))
                    .collect(),
                weight: f64::from(legs.vector_weight),
            },
            Leg {
                hits: bm25_global
                    .iter()
                    .map(|&(id, score, _)| (id, score))
                    .collect(),
                weight: f64::from(legs.bm25_weight),
            },
        ];
        let blend = legs.fusion_mode == FusionMode::ScoreBlend;
        let fused = if blend {
            fusion::blend_fuse(
                &leg_inputs,
                legs.leg_k as usize,
                legs.normalization,
                legs.combination,
                k as usize,
            )
        } else {
            fusion::rrf_fuse(&leg_inputs, legs.rrf_k, k as usize)
        };

        // Provenance: global per-leg ranks, raw scores, and the owning
        // shard (a doc lives on exactly one shard's lists).
        let hits: Vec<HybridHit> = fused
            .into_iter()
            .map(|f| HybridHit {
                doc_id: f.doc_id,
                fused_score: f.fused_score as f32,
                shard: owner.get(&f.doc_id).copied().unwrap_or(0),
                vector_rank: f.leg_ranks[0],
                vector_score: f.leg_scores[0].unwrap_or(0.0) as f32,
                bm25_rank: f.leg_ranks[1],
                bm25_score: f.leg_scores[1].unwrap_or(0.0) as f32,
                boost_score: 0.0,
            })
            .collect();
        let dbg = debug.then(|| {
            shard_debug.sort_by_key(|s| s.shard);
            HybridDebug {
                fusion_mode: if blend {
                    FusionMode::ScoreBlend as i32
                } else {
                    FusionMode::GlobalRank as i32
                },
                leg_k: legs.leg_k,
                terms: Vec::new(),
                analysis_ms: 0.0,
                stats_ms: 0.0,
                legs_ms,
                fusion_ms: t_fusion.elapsed().as_secs_f32() * 1e3,
                total_ms: 0.0,
                shards: shard_debug,
                boost_ms: 0.0,
                boost_terms: Vec::new(),
            }
        });
        Ok((hits, dbg))
    }

    /// FUSION_MODE_TWO_LEVEL (fallback for incomparable scores): each
    /// shard fuses locally; the coordinator RRF-merges the shard lists.
    /// NOT partition-independent — see the proto's FusionMode comments.
    async fn fanout_hybrid_two_level(
        &self,
        request_id: &str,
        vector: &[f32],
        k: u32,
        terms: &[String],
        global: &CorpusStats,
        legs: HybridLegs,
        debug: bool,
    ) -> Result<(Vec<HybridHit>, Option<HybridDebug>), Status> {
        let t_legs = std::time::Instant::now();
        // Level one: per-shard local fusion.
        let mut shard_tasks = Vec::with_capacity(self.node_addrs.len());
        for (shard, node) in self.node_addrs.iter().enumerate() {
            let request = HybridShardRequest {
                request_id: request_id.to_string(),
                k: legs.leg_k,
                vector: vector.to_vec(),
                terms: terms.to_vec(),
                global_doc_count: global.doc_count,
                global_total_doc_length: global.total_doc_length,
                global_doc_frequencies: global.dfs.clone(),
                vector_weight: legs.vector_weight,
                bm25_weight: legs.bm25_weight,
                rrf_k: legs.rrf_k as f32,
                k1: self.bm25_params.k1 as f32,
                b: self.bm25_params.b as f32,
            };
            let mut client = self.node_client(node)?;
            shard_tasks.push(tokio::spawn(async move {
                let t0 = std::time::Instant::now();
                client.hybrid_shard(request).await.map(|r| {
                    (
                        shard as u32,
                        t0.elapsed().as_secs_f32() * 1e3,
                        r.into_inner().hits,
                    )
                })
            }));
        }
        let mut shard_lists: Vec<(u32, Vec<crate::pb::HybridLegHit>)> = Vec::new();
        let mut shard_debug: Vec<HybridShardDebug> = Vec::new();
        for task in shard_tasks {
            let (shard, rpc_ms, mut hits) = task
                .await
                .map_err(|e| Status::internal(format!("hybrid shard task failed: {e}")))??;
            // Vector-score floor: drop non-qualifying docs from the
            // shard's fused list before level-two fusion.
            if legs.min_vector_score > 0.0 {
                hits.retain(|h| h.vector_rank.is_some() && h.vector_score >= legs.min_vector_score);
            }
            if debug {
                // A two-level shard returns one FUSED list; per-leg
                // membership is what provenance carries.
                shard_debug.push(HybridShardDebug {
                    shard,
                    rpc_ms,
                    vector_hits: hits.iter().filter(|h| h.vector_rank.is_some()).count() as u32,
                    bm25_hits: hits.iter().filter(|h| h.bm25_rank.is_some()).count() as u32,
                    scan: None,
                });
            }
            shard_lists.push((shard, hits));
        }
        let legs_ms = t_legs.elapsed().as_secs_f32() * 1e3;
        let t_fusion = std::time::Instant::now();

        // Level two: RRF over the per-shard fused lists (unweighted; the
        // leg weights already acted at shard level).
        let mut fused_legs: Vec<Leg> = Vec::with_capacity(shard_lists.len());
        for (_, hits) in &shard_lists {
            fused_legs.push(Leg {
                hits: hits
                    .iter()
                    .map(|h| (h.doc_id, f64::from(h.fused_score)))
                    .collect(),
                weight: 1.0,
            });
        }
        let fused = fusion::rrf_fuse(&fused_legs, legs.rrf_k, k as usize);

        // Attach per-leg provenance from the owning shard's HybridLegHit.
        let hits: Vec<HybridHit> = fused
            .into_iter()
            .map(|f| {
                let (shard, source) = shard_lists
                    .iter()
                    .find_map(|(shard, hits)| {
                        hits.iter()
                            .find(|h| h.doc_id == f.doc_id)
                            .map(|h| (*shard, h))
                    })
                    .expect("fused hit comes from some shard");
                HybridHit {
                    doc_id: f.doc_id,
                    fused_score: f.fused_score as f32,
                    shard,
                    vector_rank: source.vector_rank,
                    vector_score: source.vector_score,
                    bm25_rank: source.bm25_rank,
                    bm25_score: source.bm25_score,
                    boost_score: 0.0,
                }
            })
            .collect();
        let dbg = debug.then(|| {
            let mut shard_debug = shard_debug;
            shard_debug.sort_by_key(|s| s.shard);
            HybridDebug {
                fusion_mode: FusionMode::TwoLevel as i32,
                leg_k: legs.leg_k,
                terms: Vec::new(),
                analysis_ms: 0.0,
                stats_ms: 0.0,
                legs_ms,
                fusion_ms: t_fusion.elapsed().as_secs_f32() * 1e3,
                total_ms: 0.0,
                shards: shard_debug,
                boost_ms: 0.0,
                boost_terms: Vec::new(),
            }
        });
        Ok((hits, dbg))
    }

    /// FUSION_MODE_DECOMPOSED: the EXACT fused weighted-sum top-k
    ///
    ///   fused(d) = w_v * v(d) + w_b * b(d)
    ///
    /// executed BM25-first with decomposed floors over the streaming
    /// vector path (the proto's FusionMode comment walks the phases;
    /// docs/multi-field.md "Hybrid streaming interplay" has the floor
    /// algebra). The result set is the vector index: b(d) joins on the
    /// shared positional id space, exactly 0 for docs no query term
    /// matches.
    ///
    /// The exactness ledger, kept by construction:
    /// - the BM25 leg is the exact global top-leg_k, so its top score
    ///   `b_1` is the exact global maximum of b(d), and its boundary
    ///   score bounds b(d) for every doc outside the shard lists;
    /// - every vector floor F pushed here satisfies: v(d) < F implies
    ///   fused(d) < s_lb <= the final k-th best fused score, so a doc
    ///   the floor suppressed can never belong to the top-k, and ties
    ///   AT the floor survive shard-side (emission is score >= floor);
    /// - every candidate ends with EXACT leg scores: v from the stream
    ///   or from `VectorRescore` (bitwise identical — one kernel, one
    ///   calibration), b from the leg, from `Bm25Rescore`, or exactly
    ///   0 when the unfilled leg proves no further doc matches.
    async fn fanout_hybrid_decomposed(
        &self,
        request_id: &str,
        vector: &[f32],
        k: u32,
        terms: &[String],
        global: &CorpusStats,
        legs: HybridLegs,
        debug: bool,
    ) -> Result<(Vec<HybridHit>, Option<HybridDebug>), Status> {
        let n_nodes = self.node_addrs.len();
        if n_nodes == 0 {
            return Err(Status::failed_precondition("no shard nodes configured"));
        }
        let w_v = f64::from(legs.vector_weight);
        let w_b = f64::from(legs.bm25_weight);
        let fused_of = |v: f32, b: f32| w_v * f64::from(v) + w_b * f64::from(b);
        let min_v = (legs.min_vector_score > 0.0).then_some(legs.min_vector_score);
        let t_legs = std::time::Instant::now();

        // Phase 1: the BM25 leg, exact global top-leg_k. Every score
        // any shard returned is exact for its doc, so all of them pin
        // b(d) — the merge only decides b_1, the boundary, and ranks.
        let mut bm25_of: HashMap<u64, (f32, u32)> = HashMap::new();
        let mut leg_counts: HashMap<u32, u32> = HashMap::new();
        let mut merged: Vec<(u64, f32, u32)> = Vec::new();
        if !terms.is_empty() {
            let mut leg_tasks = Vec::with_capacity(n_nodes);
            for (shard, node) in self.node_addrs.iter().enumerate() {
                let request = Bm25QueryRequest {
                    terms: terms.to_vec(),
                    k: legs.leg_k,
                    global_doc_count: global.doc_count,
                    global_total_doc_length: global.total_doc_length,
                    global_doc_frequencies: global.dfs.clone(),
                    k1: self.bm25_params.k1 as f32,
                    b: self.bm25_params.b as f32,
                    min_score: 0.0,
                    fields: Vec::new(),
                };
                let mut client = self.node_client(node)?;
                leg_tasks.push(tokio::spawn(async move {
                    client
                        .bm25_query(request)
                        .await
                        .map(|r| (shard as u32, r.into_inner().hits))
                }));
            }
            for task in leg_tasks {
                let (shard, hits) = task
                    .await
                    .map_err(|e| Status::internal(format!("bm25 leg task failed: {e}")))??;
                leg_counts.insert(shard, hits.len() as u32);
                for h in &hits {
                    bm25_of.insert(h.doc_id, (h.score, shard));
                    merged.push((h.doc_id, h.score, shard));
                }
            }
            merged.sort_by(|a, b| {
                b.1.total_cmp(&a.1)
                    .then_with(|| a.2.cmp(&b.2))
                    .then_with(|| a.0.cmp(&b.0))
            });
            merged.truncate(legs.leg_k as usize);
        }
        let b_1 = merged.first().map_or(0.0f32, |&(_, s, _)| s);
        let wb_b1 = w_b * f64::from(b_1);
        // The leg filled: docs outside every shard list score at most
        // the boundary. It did not: every matching doc is in some list,
        // so an absent doc scores exactly 0.
        let filled = merged.len() == legs.leg_k as usize && !merged.is_empty();
        let b_out: f64 = if filled {
            f64::from(merged.last().expect("filled leg is non-empty").1)
        } else {
            0.0
        };
        let bm25_rank: HashMap<u64, u32> = merged
            .iter()
            .enumerate()
            .map(|(i, &(doc, _, _))| (doc, i as u32 + 1))
            .collect();

        // Phase 2: pin v(d) for the leg's top k docs. Their true fused
        // scores are the first k-th-best lower bound — the seed a
        // BM25-only bound can never provide (the top lexical doc could
        // sit at any vector score, so no shard-wide vector floor
        // follows from the leg alone).
        let mut seed_ids: HashMap<u32, Vec<u64>> = HashMap::new();
        for &(doc, _, shard) in merged.iter().take(k as usize) {
            seed_ids.entry(shard).or_default().push(doc);
        }
        let v_of = self.fanout_vector_rescore(vector, seed_ids).await?;

        // Known fused LOWER bounds, tracked as a k-sized min-heap: its
        // root is s_lb, the k-th best known bound, and every pushed
        // floor decomposes from it. A doc outside the leg contributes
        // w_v * v alone (b >= 0), still a valid lower bound.
        let mut flb_heap: std::collections::BinaryHeap<std::cmp::Reverse<F64Ord>> =
            std::collections::BinaryHeap::with_capacity(k as usize + 1);
        let push_flb = |heap: &mut std::collections::BinaryHeap<std::cmp::Reverse<F64Ord>>,
                        value: f64| {
            if heap.len() < k as usize {
                heap.push(std::cmp::Reverse(F64Ord(value)));
            } else if heap.peek().is_some_and(|r| value > r.0 .0) {
                heap.pop();
                heap.push(std::cmp::Reverse(F64Ord(value)));
            }
        };
        // Candidates: doc -> (v, owning shard). Seeded with the phase-2
        // rescores so a seed doc is a candidate even if the floor it
        // funded later suppresses its own emission.
        let mut v_seen: HashMap<u64, (f32, u32)> = HashMap::new();
        for (&doc, &v) in &v_of {
            let (b, shard) = bm25_of[&doc];
            v_seen.insert(doc, (v, shard));
            if min_v.is_none_or(|m| v >= m) {
                push_flb(&mut flb_heap, fused_of(v, b));
            }
        }
        let mut s_lb =
            (flb_heap.len() == k as usize).then(|| flb_heap.peek().expect("full heap").0 .0);
        let decomposed = s_lb.map(|s| decomposed_floor(s, wb_b1, w_v));
        // min_vector_score is a result-set gate, so it doubles as a
        // free starting floor: suppressed docs are excluded docs.
        let initial_floor = match (decomposed, min_v) {
            (Some(f), Some(m)) => Some(f.max(m)),
            (Some(f), None) => Some(f),
            (None, Some(m)) => Some(m),
            (None, None) => None,
        };

        // Phase 3: the streaming vector scan, floored from the first
        // block, with re-decomposed floors chasing the rising k-th
        // best known bound.
        let mut fanout = self.open_stream_fanout(request_id, vector, initial_floor, false)?;
        let mut summaries: Vec<Option<StreamSearchSummary>> = vec![None; n_nodes];
        let mut remaining = n_nodes;
        let mut last_floor = initial_floor.unwrap_or(f32::NEG_INFINITY);
        while remaining > 0 {
            let (shard, msg) = match fanout.next_message(&summaries).await? {
                Some(pair) => pair,
                None => continue,
            };
            match msg.payload {
                Some(stream_search_response::Payload::Batch(batch)) => {
                    if batch.hits.len() % 12 != 0 {
                        return Err(Status::internal(format!(
                            "shard {shard} sent a misaligned batch of {} bytes",
                            batch.hits.len()
                        )));
                    }
                    for rec in batch.hits.chunks_exact(12) {
                        let doc = u64::from_le_bytes(rec[..8].try_into().expect("8-byte id"));
                        let v = f32::from_le_bytes(rec[8..12].try_into().expect("4-byte score"));
                        // A re-emitted phase-2 seed carries the identical
                        // score (one kernel); keep the first sighting.
                        if v_seen.contains_key(&doc) {
                            continue;
                        }
                        v_seen.insert(doc, (v, shard as u32));
                        if min_v.is_some_and(|m| v < m) {
                            continue;
                        }
                        let b = bm25_of.get(&doc).map_or(0.0f32, |&(s, _)| s);
                        push_flb(&mut flb_heap, fused_of(v, b));
                    }
                    if flb_heap.len() == k as usize {
                        let m = flb_heap.peek().expect("full heap").0 .0;
                        if s_lb.is_none_or(|s| m > s) {
                            s_lb = Some(m);
                            let floor = decomposed_floor(m, wb_b1, w_v);
                            if floor > last_floor {
                                last_floor = floor;
                                self.push_stream_floor(&fanout, floor);
                            }
                        }
                    }
                }
                Some(stream_search_response::Payload::Summary(summary)) => {
                    if !summary.completed {
                        return Err(Status::internal(format!(
                            "shard {shard} stopped before completing its scan"
                        )));
                    }
                    summaries[shard] = Some(summary);
                    fanout.floor_txs[shard] = None;
                    remaining -= 1;
                }
                None => {}
            }
        }
        let legs_ms = t_legs.elapsed().as_secs_f32() * 1e3;

        // Close-out. Candidates with a leg score (or proven-zero b)
        // have exact fused scores already; the rest hold b in
        // [0, b_out] and need Bm25Rescore — unless even b_out cannot
        // lift them to the k-th best exact fused score known so far.
        let t_fusion = std::time::Instant::now();
        let mut exact: Vec<(u64, u32, f32, f32)> = Vec::new(); // (doc, shard, v, b)
        let mut unknown: Vec<(u64, u32, f32)> = Vec::new(); // (doc, shard, v)
        for (&doc, &(v, shard)) in &v_seen {
            if min_v.is_some_and(|m| v < m) {
                continue;
            }
            match bm25_of.get(&doc) {
                Some(&(b, _)) => exact.push((doc, shard, v, b)),
                None if !filled => exact.push((doc, shard, v, 0.0)),
                None => unknown.push((doc, shard, v)),
            }
        }
        let kth_known: f64 = {
            let mut fused: Vec<f64> = exact.iter().map(|&(_, _, v, b)| fused_of(v, b)).collect();
            if fused.len() >= k as usize {
                let idx = k as usize - 1;
                *fused.select_nth_unstable_by(idx, |a, b| b.total_cmp(a)).1
            } else {
                f64::NEG_INFINITY
            }
        };
        let mut rescore_ids: HashMap<u32, Vec<u64>> = HashMap::new();
        let mut rescore_docs: Vec<(u64, u32, f32)> = Vec::new();
        for (doc, shard, v) in unknown {
            // Conservative upper bound: round the two products and the
            // sum up by a magnitude-relative slack that dwarfs the f64
            // rounding error, so a doc dropped here provably cannot
            // reach the k-th best.
            let ub = w_v * f64::from(v) + w_b * b_out;
            let mag = ub.abs() + (w_v * f64::from(v)).abs() + w_b * b_out;
            let ub_safe = ub + mag * SLACK_REL + f64::MIN_POSITIVE;
            if ub_safe >= kth_known {
                rescore_ids.entry(shard).or_default().push(doc);
                rescore_docs.push((doc, shard, v));
            }
        }
        let rescored_b = self
            .fanout_bm25_rescore_scores(terms, global, rescore_ids)
            .await?;
        for (doc, shard, v) in rescore_docs {
            // Absent from the rescore response = no query term matches
            // the doc: b is exactly 0.
            let b = rescored_b.get(&doc).copied().unwrap_or(0.0);
            exact.push((doc, shard, v, b));
        }

        let mut ranked: Vec<(u64, u32, f32, f32, f64)> = exact
            .into_iter()
            .map(|(doc, shard, v, b)| (doc, shard, v, b, fused_of(v, b)))
            .collect();
        ranked.sort_by(|a, b| {
            b.4.total_cmp(&a.4)
                .then_with(|| a.1.cmp(&b.1))
                .then_with(|| a.0.cmp(&b.0))
        });
        ranked.truncate(k as usize);
        let hits: Vec<HybridHit> = ranked
            .into_iter()
            .map(|(doc, shard, v, b, fused)| HybridHit {
                doc_id: doc,
                fused_score: fused as f32,
                shard,
                vector_rank: None,
                vector_score: v,
                bm25_rank: bm25_rank.get(&doc).copied(),
                bm25_score: b,
                boost_score: 0.0,
            })
            .collect();
        let dbg = debug.then(|| {
            let shards: Vec<HybridShardDebug> = summaries
                .iter()
                .enumerate()
                .map(|(shard, summary)| HybridShardDebug {
                    shard: shard as u32,
                    rpc_ms: 0.0,
                    vector_hits: summary
                        .as_ref()
                        .map_or(0, |s| u32::try_from(s.emitted).unwrap_or(u32::MAX)),
                    bm25_hits: leg_counts.get(&(shard as u32)).copied().unwrap_or(0),
                    scan: None,
                })
                .collect();
            HybridDebug {
                fusion_mode: FusionMode::Decomposed as i32,
                leg_k: legs.leg_k,
                terms: Vec::new(),
                analysis_ms: 0.0,
                stats_ms: 0.0,
                legs_ms,
                fusion_ms: t_fusion.elapsed().as_secs_f32() * 1e3,
                total_ms: 0.0,
                shards,
                boost_ms: 0.0,
                boost_terms: Vec::new(),
            }
        });
        Ok((hits, dbg))
    }

    /// Candidate-scoped vector scoring fan-out: `VectorRescore` on
    /// every shard that owns candidates, merged into doc -> score.
    async fn fanout_vector_rescore(
        &self,
        vector: &[f32],
        by_shard: HashMap<u32, Vec<u64>>,
    ) -> Result<HashMap<u64, f32>, Status> {
        let mut tasks = Vec::with_capacity(by_shard.len());
        for (shard, ids) in by_shard {
            let request = VectorRescoreRequest {
                vector: vector.to_vec(),
                candidate_ids: ids,
            };
            let mut client = self.node_client(&self.node_addrs[shard as usize])?;
            tasks.push(tokio::spawn(async move {
                client
                    .vector_rescore(request)
                    .await
                    .map(|r| r.into_inner().hits)
            }));
        }
        let mut scores = HashMap::new();
        for task in tasks {
            let hits = task
                .await
                .map_err(|e| Status::internal(format!("vector rescore task failed: {e}")))??;
            for hit in hits {
                scores.insert(hit.doc_id, hit.score);
            }
        }
        Ok(scores)
    }

    /// Candidate-scoped BM25 fan-out (the cascade phase-2 seam),
    /// reduced to doc -> score. Docs absent from the response match no
    /// query term and score exactly 0.
    async fn fanout_bm25_rescore_scores(
        &self,
        terms: &[String],
        global: &CorpusStats,
        by_shard: HashMap<u32, Vec<u64>>,
    ) -> Result<HashMap<u64, f32>, Status> {
        let mut tasks = Vec::with_capacity(by_shard.len());
        for (shard, ids) in by_shard {
            let request = Bm25RescoreRequest {
                terms: terms.to_vec(),
                global_doc_count: global.doc_count,
                global_total_doc_length: global.total_doc_length,
                global_doc_frequencies: global.dfs.clone(),
                candidate_ids: ids,
                k1: self.bm25_params.k1 as f32,
                b: self.bm25_params.b as f32,
            };
            let mut client = self.node_client(&self.node_addrs[shard as usize])?;
            tasks.push(tokio::spawn(async move {
                client
                    .bm25_rescore(request)
                    .await
                    .map(|r| r.into_inner().hits)
            }));
        }
        let mut scores = HashMap::new();
        for task in tasks {
            let hits = task
                .await
                .map_err(|e| Status::internal(format!("bm25 rescore task failed: {e}")))??;
            for hit in hits {
                scores.insert(hit.doc_id, hit.score);
            }
        }
        Ok(scores)
    }

    /// Build the tonic server for this service with explicit message size
    /// limits (see [`crate::MAX_MESSAGE_BYTES`]).
    pub fn into_server(self, max_message_bytes: usize) -> SearchServiceServer<Self> {
        SearchServiceServer::new(self)
            .max_decoding_message_size(max_message_bytes)
            .max_encoding_message_size(max_message_bytes)
    }

    /// Run one fan-out search against every configured node. Broken out
    /// from the gRPC handler so tests and the binary can drive it directly.
    ///
    /// Floor flow per query:
    /// 1. open a `SearchShard` stream per node and send `StartShardSearch`;
    /// 2. each stream pump feeds shard floor updates into one shared
    ///    [`FloorTracker`]; raises land in a conflating `watch` cell that
    ///    per-stream forwarders relay — a burst of raises collapses to the
    ///    latest value instead of one message per raise;
    /// 3. each pump ends on the node's terminal `SearchShardDone`.
    ///
    /// Per shard, [`FanoutLimits`] adds a hedged retry to the shard's
    /// replica after `hedge_delay` (first success wins; identical data
    /// plus exact search means identical results either way) and bounds
    /// the whole attempt with `shard_deadline`.
    pub async fn fanout_search(
        &self,
        request_id: &str,
        vector: &[f32],
        k: u32,
        tie_complete: bool,
    ) -> Result<FanoutResult, Status> {
        let n_nodes = self.node_addrs.len();
        if n_nodes == 0 {
            return Err(Status::failed_precondition("no shard nodes configured"));
        }

        let ctx = ShardQueryCtx {
            request_id: Arc::from(request_id),
            vector: Arc::new(vector.to_vec()),
            k,
            tie_complete,
            collapse: false,
            tracker: Arc::new(Mutex::new(FloorTracker::new())),
            gfloor: Arc::new(watch::channel(f32::NEG_INFINITY).0),
            hedges: Arc::new(AtomicU64::new(0)),
            hedge_wins: Arc::new(AtomicU64::new(0)),
        };
        let (hedges, hedge_wins) = (Arc::clone(&ctx.hedges), Arc::clone(&ctx.hedge_wins));

        let (done_tx, mut done_rx) =
            mpsc::channel::<(u32, f32, Result<SearchShardDone, Status>)>(n_nodes);
        for shard in 0..n_nodes {
            let primary = self.node_client(&self.node_addrs[shard])?;
            let replica = match self.replica_addrs.get(shard).and_then(|r| r.as_deref()) {
                Some(addr) => Some(self.node_client(addr)?),
                None => None,
            };
            let ctx = ctx.clone();
            let limits = self.limits;
            let done_tx = done_tx.clone();
            tokio::spawn(async move {
                let t0 = std::time::Instant::now();
                let result =
                    run_shard_with_hedge(shard as u32, primary, replica, ctx, limits).await;
                let wall_ms = t0.elapsed().as_secs_f32() * 1e3;
                let _ = done_tx.send((shard as u32, wall_ms, result)).await;
            });
        }
        drop(done_tx);

        let mut shard_hits: Vec<(u32, Vec<(u64, f32)>)> = Vec::with_capacity(n_nodes);
        let mut shard_stats: Vec<Option<ShardScanStats>> = Vec::with_capacity(n_nodes);
        let mut shard_wall_ms: Vec<(u32, f32)> = Vec::with_capacity(n_nodes);
        for _ in 0..n_nodes {
            match done_rx.recv().await {
                Some((shard, wall_ms, Ok(done))) => {
                    shard_hits.push((
                        shard,
                        done.hits
                            .into_iter()
                            .map(|h| (h.vector_id, h.score))
                            .collect(),
                    ));
                    shard_stats.push(done.stats);
                    shard_wall_ms.push((shard, wall_ms));
                }
                Some((shard, _, Err(e))) => {
                    return Err(Status::internal(format!("shard {shard} failed: {e}")));
                }
                None => {
                    return Err(Status::internal("fan-out completed without all shards"));
                }
            }
        }

        let hits = merge_topk(shard_hits.iter().cloned(), k as usize)
            .into_iter()
            .map(|h| ScoredHit {
                vector_id: h.vector_id,
                score: h.score,
                parent_id: 0,
            })
            .collect();
        Ok(FanoutResult {
            hits,
            shard_stats,
            shard_hits,
            shard_wall_ms,
            hedges_fired: hedges.load(AtomicOrdering::Relaxed),
            hedge_wins: hedge_wins.load(AtomicOrdering::Relaxed),
        })
    }

    /// Open one `StreamSearch` per shard: Start flows through a held
    /// sender that later carries floor raises. Each stream also gets a
    /// UDP token so raises reach the shard on the fast lossy lane as
    /// well as the reliable stream.
    fn open_stream_fanout(
        &self,
        request_id: &str,
        vector: &[f32],
        initial_floor: Option<f32>,
        collapse_parents: bool,
    ) -> Result<StreamFanout, Status> {
        let n_nodes = self.node_addrs.len();
        let (merged_tx, merged_rx) =
            mpsc::channel::<(usize, Result<Option<StreamSearchResponse>, Status>)>(4 * n_nodes);
        let mut floor_txs: Vec<Option<mpsc::Sender<StreamSearchRequest>>> =
            Vec::with_capacity(n_nodes);
        let mut udp_lanes: Vec<Option<(u64, std::net::SocketAddr)>> = Vec::with_capacity(n_nodes);
        for shard in 0..n_nodes {
            let mut client = self.node_client(&self.node_addrs[shard])?;
            let lane = self
                .floor_target(&self.node_addrs[shard])
                .map(|target| (floor_token(), target));
            let (req_tx, req_rx) = mpsc::channel::<StreamSearchRequest>(64);
            req_tx
                .try_send(StreamSearchRequest {
                    payload: Some(stream_search_request::Payload::Start(StartStreamSearch {
                        request_id: request_id.to_string(),
                        vector: vector.to_vec(),
                        initial_floor,
                        floor_token: lane.map_or(0, |(token, _)| token),
                        collapse_parents,
                    })),
                })
                .expect("fresh channel accepts the Start message");
            floor_txs.push(Some(req_tx));
            udp_lanes.push(lane);
            let merged_tx = merged_tx.clone();
            tokio::spawn(async move {
                let mut inbound = match client
                    .stream_search(Request::new(ReceiverStream::new(req_rx)))
                    .await
                {
                    Ok(response) => response.into_inner(),
                    Err(e) => {
                        let _ = merged_tx.send((shard, Err(e))).await;
                        return;
                    }
                };
                loop {
                    match inbound.message().await {
                        Ok(Some(msg)) => {
                            if merged_tx.send((shard, Ok(Some(msg)))).await.is_err() {
                                return;
                            }
                        }
                        Ok(None) => {
                            let _ = merged_tx.send((shard, Ok(None))).await;
                            return;
                        }
                        Err(e) => {
                            let _ = merged_tx.send((shard, Err(e))).await;
                            return;
                        }
                    }
                }
            });
        }
        Ok(StreamFanout {
            merged_rx,
            floor_txs,
            udp_lanes,
        })
    }

    /// Push a floor raise to every still-open stream of `fanout`: UDP
    /// first (the fast lossy copy), then the reliable stream. Both are
    /// monotone max-folds shard-side, so double delivery and loss are
    /// equally free; a full stream channel just means the next raise
    /// supersedes this one.
    fn push_stream_floor(&self, fanout: &StreamFanout, floor: f32) {
        let update = StreamSearchRequest {
            payload: Some(stream_search_request::Payload::FloorUpdate(FloorUpdate {
                floor,
            })),
        };
        let socket = self.floor_socket();
        for (si, tx) in fanout.floor_txs.iter().enumerate() {
            let Some(tx) = tx.as_ref() else {
                continue;
            };
            if let (Some(socket), Some((token, target))) = (socket, fanout.udp_lanes[si]) {
                let mut dgram = [0u8; 12];
                dgram[..8].copy_from_slice(&token.to_le_bytes());
                dgram[8..].copy_from_slice(&floor.to_le_bytes());
                let _ = socket.send_to(&dgram, target);
            }
            let _ = tx.try_send(update.clone());
        }
    }

    /// [`Self::fanout_search`] over the streaming protocol
    /// (`NodeService.StreamSearch`): shards hold no top-k and emit
    /// every candidate at or above the live floor; the heap here — the
    /// only one in the system — defines k. Whenever its k-th best
    /// tightens, `floor_seed(kth)` (one f32 ULP below, so boundary ties
    /// survive) is pushed to every open stream.
    ///
    /// Exactness: this path never sends Stop, every shard's terminal
    /// summary must certify `completed = true`, every emission scored
    /// at or above the floor in effect when its block was scanned, and
    /// every pushed floor is a lower bound on the global k-th best —
    /// so nothing that belongs in the top-k was withheld. Results are
    /// identical to [`Self::fanout_search`] (same scores, same
    /// `merge_topk` total order).
    ///
    /// `initial_floor` seeds every shard's starting floor — the hybrid
    /// seam, where a finished BM25 leg's decomposed floor prunes the
    /// vector scan from the first block.
    pub async fn fanout_stream_search(
        &self,
        request_id: &str,
        vector: &[f32],
        k: u32,
        initial_floor: Option<f32>,
    ) -> Result<StreamFanoutResult, Status> {
        let n_nodes = self.node_addrs.len();
        if n_nodes == 0 {
            return Err(Status::failed_precondition("no shard nodes configured"));
        }
        if initial_floor.is_some_and(f32::is_nan) {
            return Err(Status::invalid_argument("initial_floor must not be NaN"));
        }
        // Without k the heap never fills, no floor ever rises, and
        // every shard would emit itself entirely for nothing.
        if k == 0 {
            return Ok(StreamFanoutResult {
                hits: Vec::new(),
                summaries: Vec::new(),
                floors_sent: 0,
            });
        }

        let mut fanout = self.open_stream_fanout(request_id, vector, initial_floor, false)?;

        // The global top-k: a max-heap whose top is the WORST survivor
        // under the merge's total order, so peek() is the k-th best.
        let mut heap: std::collections::BinaryHeap<StreamHeapEntry> =
            std::collections::BinaryHeap::with_capacity(k as usize + 1);
        let mut summaries: Vec<Option<StreamSearchSummary>> = vec![None; n_nodes];
        let mut remaining = n_nodes;
        let mut last_floor = initial_floor.unwrap_or(f32::NEG_INFINITY);
        let mut floors_sent = 0u64;
        while remaining > 0 {
            let (shard, msg) = match fanout.next_message(&summaries).await? {
                Some(pair) => pair,
                None => continue,
            };
            match msg.payload {
                Some(stream_search_response::Payload::Batch(batch)) => {
                    // Packed 12-byte LE records: u64 global id, f32
                    // score (see StreamSearchBatch).
                    if batch.hits.len() % 12 != 0 {
                        return Err(Status::internal(format!(
                            "shard {shard} sent a misaligned batch of {} bytes",
                            batch.hits.len()
                        )));
                    }
                    for rec in batch.hits.chunks_exact(12) {
                        let entry = StreamHeapEntry(MergedHit {
                            vector_id: u64::from_le_bytes(rec[..8].try_into().expect("8-byte id")),
                            shard: shard as u32,
                            score: f32::from_le_bytes(rec[8..12].try_into().expect("4-byte score")),
                        });
                        if heap.len() < k as usize {
                            heap.push(entry);
                        } else if cmp_hits(&entry.0, &heap.peek().expect("heap is full").0)
                            == std::cmp::Ordering::Less
                        {
                            heap.pop();
                            heap.push(entry);
                        }
                    }
                    // Full heap: its worst survivor bounds the global
                    // k-th best from below; seed one ULP down so shard
                    // ties at the boundary keep flowing. next_down, not
                    // bm25::floor_seed: vector scores are SIGNED (dot
                    // products), and clamping a negative k-th best to 0
                    // would push a floor ABOVE it — a recall bug on any
                    // corpus whose top-k dips below zero.
                    if heap.len() == k as usize {
                        let kth = heap.peek().expect("heap is full").0.score;
                        let floor = kth.next_down();
                        if floor > last_floor {
                            last_floor = floor;
                            floors_sent += 1;
                            self.push_stream_floor(&fanout, floor);
                        }
                    }
                }
                Some(stream_search_response::Payload::Summary(summary)) => {
                    if !summary.completed {
                        return Err(Status::internal(format!(
                            "shard {shard} stopped before completing its scan"
                        )));
                    }
                    summaries[shard] = Some(summary);
                    fanout.floor_txs[shard] = None;
                    remaining -= 1;
                }
                None => {}
            }
        }

        let mut all: Vec<MergedHit> = heap.into_iter().map(|e| e.0).collect();
        all.sort_by(cmp_hits);
        Ok(StreamFanoutResult {
            hits: all
                .into_iter()
                .map(|h| ScoredHit {
                    vector_id: h.vector_id,
                    score: h.score,
                    parent_id: 0,
                })
                .collect(),
            summaries: summaries
                .into_iter()
                .map(|s| s.expect("all summaries arrived"))
                .collect(),
            floors_sent,
        })
    }

    /// Document-mode streaming: [`Self::fanout_stream_search`] with
    /// `k` meaning k distinct PARENT documents. Shards emit chunks
    /// tagged with their parents (lineage `opinion_id`, or tagged
    /// self-parents); the coordinator owns the whole document
    /// aggregation — a parent's score is its best chunk's, the floor
    /// is one ULP below the k-th best parent score, and each returned
    /// parent carries EVERY chunk at or above the final floor,
    /// whichever shards held them. No colocation: an opinion whose
    /// chunks straddle a shard cut aggregates here exactly like one
    /// whose chunks share a shard.
    ///
    /// Lossless: a chunk below the parent floor can neither beat its
    /// own parent's best nor introduce a new top-k parent, and every
    /// chunk scoring at or above the FINAL floor cleared every earlier
    /// (lower) floor, so filtering the retained chunks to the final
    /// floor makes the groups exact and layout-invariant.
    pub async fn fanout_stream_search_collapse(
        &self,
        request_id: &str,
        vector: &[f32],
        k: u32,
    ) -> Result<CollapseStreamResult, Status> {
        struct ParentAgg {
            best_score: f32,
            best_id: u64,
            chunks: Vec<(u64, f32)>,
        }
        let n_nodes = self.node_addrs.len();
        if n_nodes == 0 {
            return Err(Status::failed_precondition("no shard nodes configured"));
        }
        if k == 0 {
            return Ok(CollapseStreamResult {
                hits: Vec::new(),
                groups: Vec::new(),
                chunk_floor: f32::NEG_INFINITY,
                summaries: Vec::new(),
                floors_sent: 0,
            });
        }
        let mut fanout = self.open_stream_fanout(request_id, vector, None, true)?;
        let mut parents: HashMap<u64, ParentAgg> = HashMap::new();
        let mut summaries: Vec<Option<StreamSearchSummary>> = vec![None; n_nodes];
        let mut remaining = n_nodes;
        // The k-th best parent score, recomputed lazily: only a batch
        // that raised some parent's best (or added a parent) can move
        // it, and floors derive from nothing else.
        let mut kth = f32::NEG_INFINITY;
        let mut last_floor = f32::NEG_INFINITY;
        let mut floors_sent = 0u64;
        while remaining > 0 {
            let (shard, msg) = match fanout.next_message(&summaries).await? {
                Some(pair) => pair,
                None => continue,
            };
            match msg.payload {
                Some(stream_search_response::Payload::Batch(batch)) => {
                    // Packed 20-byte LE records: u64 global id, f32
                    // score, u64 parent (see StreamSearchBatch).
                    if batch.hits.len() % 20 != 0 {
                        return Err(Status::internal(format!(
                            "shard {shard} sent a misaligned collapse batch of {} bytes",
                            batch.hits.len()
                        )));
                    }
                    let mut dirty = false;
                    for rec in batch.hits.chunks_exact(20) {
                        let doc = u64::from_le_bytes(rec[..8].try_into().expect("8-byte id"));
                        let score =
                            f32::from_le_bytes(rec[8..12].try_into().expect("4-byte score"));
                        let parent =
                            u64::from_le_bytes(rec[12..20].try_into().expect("8-byte parent"));
                        let agg = parents.entry(parent).or_insert_with(|| {
                            dirty = true;
                            ParentAgg {
                                best_score: f32::NEG_INFINITY,
                                best_id: u64::MAX,
                                chunks: Vec::new(),
                            }
                        });
                        agg.chunks.push((doc, score));
                        if score > agg.best_score || (score == agg.best_score && doc < agg.best_id)
                        {
                            if score > agg.best_score && score > kth {
                                dirty = true;
                            }
                            agg.best_score = score;
                            agg.best_id = doc;
                        }
                    }
                    if dirty && parents.len() >= k as usize {
                        let mut bests: Vec<f32> = parents.values().map(|a| a.best_score).collect();
                        let idx = k as usize - 1;
                        let new_kth = *bests.select_nth_unstable_by(idx, |a, b| b.total_cmp(a)).1;
                        if new_kth > kth {
                            kth = new_kth;
                            // next_down, not bm25::floor_seed: parent
                            // scores are signed vector scores.
                            let floor = kth.next_down();
                            if floor > last_floor {
                                last_floor = floor;
                                floors_sent += 1;
                                self.push_stream_floor(&fanout, floor);
                            }
                        }
                    }
                }
                Some(stream_search_response::Payload::Summary(summary)) => {
                    if !summary.completed {
                        return Err(Status::internal(format!(
                            "shard {shard} stopped before completing its scan"
                        )));
                    }
                    summaries[shard] = Some(summary);
                    fanout.floor_txs[shard] = None;
                    remaining -= 1;
                }
                None => {}
            }
        }

        // Rank parents (best desc, best chunk id asc — the collapse
        // fan-out's order), keep k, and filter each kept parent's
        // chunks to the final floor for a deterministic group: every
        // chunk at or above it was emitted, every chunk below it is
        // dropped whether or not it happened to arrive.
        let mut ranked: Vec<(u64, ParentAgg)> = parents.into_iter().collect();
        ranked.sort_by(|a, b| {
            b.1.best_score
                .total_cmp(&a.1.best_score)
                .then_with(|| a.1.best_id.cmp(&b.1.best_id))
        });
        let chunk_floor = if ranked.len() >= k as usize {
            ranked[k as usize - 1].1.best_score.next_down()
        } else {
            f32::NEG_INFINITY
        };
        ranked.truncate(k as usize);
        let mut hits = Vec::with_capacity(ranked.len());
        let mut groups = Vec::with_capacity(ranked.len());
        for (parent, agg) in ranked {
            hits.push(ScoredHit {
                vector_id: agg.best_id,
                score: agg.best_score,
                parent_id: parent,
            });
            let mut chunks: Vec<(u64, f32)> = agg
                .chunks
                .into_iter()
                .filter(|&(_, score)| score >= chunk_floor)
                .collect();
            chunks.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            groups.push(ParentGroup {
                parent_id: parent,
                chunks: chunks
                    .into_iter()
                    .map(|(doc, score)| ScoredHit {
                        vector_id: doc,
                        score,
                        parent_id: parent,
                    })
                    .collect(),
            });
        }
        Ok(CollapseStreamResult {
            hits,
            groups,
            chunk_floor,
            summaries: summaries
                .into_iter()
                .map(|s| s.expect("all summaries arrived"))
                .collect(),
            floors_sent,
        })
    }

    /// [`Self::fanout_search`] in collapse-by-parent mode: `k` means k
    /// distinct parent documents, each represented by its best chunk.
    /// Shards collapse locally (their floors are k-th best PARENT
    /// scores, so collaborative pruning strengthens); the coordinator
    /// dedupes parents that appear on multiple shards (opinions
    /// straddling a shard cut) keeping the better representative, then
    /// takes the global top-k parents.
    pub async fn fanout_search_collapse(
        &self,
        request_id: &str,
        vector: &[f32],
        k: u32,
    ) -> Result<FanoutResult, Status> {
        let n_nodes = self.node_addrs.len();
        if n_nodes == 0 {
            return Err(Status::failed_precondition("no shard nodes configured"));
        }
        let ctx = ShardQueryCtx {
            request_id: Arc::from(request_id),
            vector: Arc::new(vector.to_vec()),
            k,
            tie_complete: false,
            collapse: true,
            tracker: Arc::new(Mutex::new(FloorTracker::new())),
            gfloor: Arc::new(watch::channel(f32::NEG_INFINITY).0),
            hedges: Arc::new(AtomicU64::new(0)),
            hedge_wins: Arc::new(AtomicU64::new(0)),
        };
        let (hedges, hedge_wins) = (Arc::clone(&ctx.hedges), Arc::clone(&ctx.hedge_wins));

        let (done_tx, mut done_rx) =
            mpsc::channel::<(u32, f32, Result<SearchShardDone, Status>)>(n_nodes);
        for shard in 0..n_nodes {
            let primary = self.node_client(&self.node_addrs[shard])?;
            let replica = match self.replica_addrs.get(shard).and_then(|r| r.as_deref()) {
                Some(addr) => Some(self.node_client(addr)?),
                None => None,
            };
            let ctx = ctx.clone();
            let limits = self.limits;
            let done_tx = done_tx.clone();
            tokio::spawn(async move {
                let t0 = std::time::Instant::now();
                let result =
                    run_shard_with_hedge(shard as u32, primary, replica, ctx, limits).await;
                let wall_ms = t0.elapsed().as_secs_f32() * 1e3;
                let _ = done_tx.send((shard as u32, wall_ms, result)).await;
            });
        }
        drop(done_tx);

        let mut shard_hits: Vec<(u32, Vec<(u64, f32)>)> = Vec::with_capacity(n_nodes);
        let mut shard_stats: Vec<Option<ShardScanStats>> = Vec::with_capacity(n_nodes);
        let mut shard_wall_ms: Vec<(u32, f32)> = Vec::with_capacity(n_nodes);
        // Parent -> best hit. Tie-break inside a parent: score desc, then
        // vector id asc (globally unique), deterministic across arrival
        // orders.
        let mut best: HashMap<u64, ScoredHit> = HashMap::new();
        for _ in 0..n_nodes {
            match done_rx.recv().await {
                Some((shard, wall_ms, Ok(done))) => {
                    shard_hits.push((
                        shard,
                        done.hits.iter().map(|h| (h.vector_id, h.score)).collect(),
                    ));
                    for hit in done.hits {
                        let entry = best.entry(hit.parent_id).or_insert_with(|| hit.clone());
                        if hit.score > entry.score
                            || (hit.score == entry.score && hit.vector_id < entry.vector_id)
                        {
                            *entry = hit;
                        }
                    }
                    shard_stats.push(done.stats);
                    shard_wall_ms.push((shard, wall_ms));
                }
                Some((shard, _, Err(e))) => {
                    return Err(Status::internal(format!("shard {shard} failed: {e}")));
                }
                None => {
                    return Err(Status::internal("fan-out completed without all shards"));
                }
            }
        }

        let mut hits: Vec<ScoredHit> = best.into_values().collect();
        hits.sort_by(|a, b| {
            b.score
                .total_cmp(&a.score)
                .then_with(|| a.vector_id.cmp(&b.vector_id))
        });
        hits.truncate(k as usize);
        Ok(FanoutResult {
            hits,
            shard_stats,
            shard_hits,
            shard_wall_ms,
            hedges_fired: hedges.load(AtomicOrdering::Relaxed),
            hedge_wins: hedge_wins.load(AtomicOrdering::Relaxed),
        })
    }

    /// Cascade hybrid (FUSION_MODE_CASCADE): no score fusion.
    ///
    /// Phase 1 — candidate generation through the EXISTING floor-sharing
    /// bidi vector path with the tie-complete option: cross-shard early
    /// termination applies (the cutoff is the savings), and every doc
    /// tied at the boundary score survives on every shard. The pool is
    /// `{score >= s_k}` where `s_k` is the global k-th vector score, so
    /// it can exceed k by the boundary tie-group size — score-defined,
    /// hence layout-invariant.
    ///
    /// Phase 2 — BM25 rerank behind the rescore seam: analyze the query,
    /// route each candidate to its owning shard (Bm25Rescore with the
    /// global stats), then rerank the pool by BM25 desc, vector desc,
    /// doc id asc, and return the top `k`. More rankers plug in behind
    /// this same seam later (one stage, no framework).
    pub async fn fanout_cascade(
        &self,
        request_id: &str,
        text: &str,
        vector: &[f32],
        k: u32,
        spec: Option<&crate::pb::AnalysisSpec>,
        min_vector_score: f32,
        debug: bool,
    ) -> Result<(Vec<CascadeHit>, Option<HybridDebug>), Status> {
        if k == 0 || vector.is_empty() {
            return Ok((Vec::new(), None));
        }
        let t_total = std::time::Instant::now();
        // Phase 1: floor-shared, tie-complete vector candidates.
        let t_legs = std::time::Instant::now();
        let phase1 = self.fanout_search(request_id, vector, k, true).await?;
        let phase1_ms = t_legs.elapsed().as_secs_f32() * 1e3;
        let mut all: Vec<(u64, u32, f64)> = Vec::new(); // (doc_id, shard, score)
        for (shard, hits) in &phase1.shard_hits {
            for &(doc_id, score) in hits {
                all.push((doc_id, *shard, f64::from(score)));
            }
        }
        all.sort_by(|a, b| b.2.total_cmp(&a.2).then_with(|| a.0.cmp(&b.0)));
        let boundary = if all.len() >= k as usize {
            all[k as usize - 1].2
        } else {
            f64::NEG_INFINITY
        };
        // The vector-score floor tightens the gate: the effective cutoff
        // is max(k-th score, floor), applied before the rescore fan-out
        // so filtered-out candidates cost nothing in phase 2.
        let boundary = boundary.max(if min_vector_score > 0.0 {
            f64::from(min_vector_score)
        } else {
            f64::NEG_INFINITY
        });
        let pool: Vec<(u64, u32, f64)> = all.into_iter().filter(|h| h.2 >= boundary).collect();

        // Query analysis + global BM25 stats for phase 2.
        let addr = self.analysis_addr.clone().ok_or_else(|| {
            Status::unavailable("no analysis sidecar configured on the coordinator (analysis_addr)")
        })?;
        let t = std::time::Instant::now();
        let analyzed = crate::analyzer::analyze_document(&addr, text, spec).await?;
        let analysis_ms = t.elapsed().as_secs_f32() * 1e3;
        let mut terms: Vec<String> = Vec::new();
        for (term, _, _) in analyzed.into_body().terms {
            if !terms.contains(&term) {
                terms.push(term);
            }
        }
        let t = std::time::Instant::now();
        let global = self.global_bm25_stats(&terms).await?;
        let stats_ms = t.elapsed().as_secs_f32() * 1e3;

        // Phase 2: route candidates to their owning shards for rescoring.
        let t_rescore = std::time::Instant::now();
        let mut by_shard: std::collections::HashMap<u32, Vec<u64>> =
            std::collections::HashMap::new();
        for (doc_id, shard, _) in &pool {
            by_shard.entry(*shard).or_default().push(*doc_id);
        }
        let mut rescore_tasks = Vec::with_capacity(by_shard.len());
        for (shard, ids) in by_shard {
            let node = &self.node_addrs[shard as usize];
            let request = Bm25RescoreRequest {
                terms: terms.clone(),
                global_doc_count: global.doc_count,
                global_total_doc_length: global.total_doc_length,
                global_doc_frequencies: global.dfs.clone(),
                candidate_ids: ids,
                k1: self.bm25_params.k1 as f32,
                b: self.bm25_params.b as f32,
            };
            let mut client = self.node_client(node)?;
            rescore_tasks.push(tokio::spawn(async move {
                let t0 = std::time::Instant::now();
                client
                    .bm25_rescore(request)
                    .await
                    .map(|r| (shard, t0.elapsed().as_secs_f32() * 1e3, r.into_inner().hits))
            }));
        }
        let mut bm25_of: std::collections::HashMap<u64, f64> = std::collections::HashMap::new();
        let mut rescore_debug: std::collections::HashMap<u32, (f32, u32)> =
            std::collections::HashMap::new();
        for task in rescore_tasks {
            let (shard, rpc_ms, hits) = task
                .await
                .map_err(|e| Status::internal(format!("bm25 rescore task failed: {e}")))??;
            rescore_debug.insert(shard, (rpc_ms, hits.len() as u32));
            for hit in hits {
                bm25_of.insert(hit.doc_id, f64::from(hit.score));
            }
        }
        let rescore_ms = t_rescore.elapsed().as_secs_f32() * 1e3;

        // Rerank: BM25 desc, vector score desc, doc id asc. Top k of the
        // (possibly larger) tie-extended pool.
        let t_fusion = std::time::Instant::now();
        let mut ranked: Vec<CascadeHit> = pool
            .into_iter()
            .map(|(doc_id, shard, vector_score)| CascadeHit {
                doc_id,
                rank: 0,
                shard,
                vector_score: vector_score as f32,
                bm25_score: bm25_of.get(&doc_id).copied().unwrap_or(0.0) as f32,
                boost_score: 0.0,
            })
            .collect();
        ranked.sort_by(|a, b| {
            b.bm25_score
                .total_cmp(&a.bm25_score)
                .then_with(|| b.vector_score.total_cmp(&a.vector_score))
                .then_with(|| a.doc_id.cmp(&b.doc_id))
        });
        ranked.truncate(k as usize);
        for (i, hit) in ranked.iter_mut().enumerate() {
            hit.rank = i as u32 + 1;
        }
        let dbg = debug.then(|| {
            // Per-shard: phase-1 wall + this shard's rescore wall, the
            // phase-1 candidate count, the rescored count, and the vector
            // scan's stats. Vectors from fanout_search are in completion
            // order; re-key by shard.
            let mut walls: std::collections::HashMap<u32, f32> =
                phase1.shard_wall_ms.iter().copied().collect();
            let mut scans: std::collections::HashMap<u32, Option<ShardScanStats>> = phase1
                .shard_hits
                .iter()
                .zip(&phase1.shard_stats)
                .map(|((shard, _), stats)| (*shard, *stats))
                .collect();
            let mut shards: Vec<HybridShardDebug> = phase1
                .shard_hits
                .iter()
                .map(|(shard, hits)| {
                    let (rescore_wall, rescored) =
                        rescore_debug.get(shard).copied().unwrap_or((0.0, 0));
                    HybridShardDebug {
                        shard: *shard,
                        rpc_ms: walls.remove(shard).unwrap_or(0.0) + rescore_wall,
                        vector_hits: hits.len() as u32,
                        bm25_hits: rescored,
                        scan: scans.remove(shard).flatten(),
                    }
                })
                .collect();
            shards.sort_by_key(|s| s.shard);
            HybridDebug {
                fusion_mode: FusionMode::Cascade as i32,
                leg_k: k,
                terms,
                analysis_ms,
                stats_ms,
                legs_ms: phase1_ms + rescore_ms,
                fusion_ms: t_fusion.elapsed().as_secs_f32() * 1e3,
                total_ms: t_total.elapsed().as_secs_f32() * 1e3,
                shards,
                boost_ms: 0.0,
                boost_terms: Vec::new(),
            }
        });
        Ok((ranked, dbg))
    }
    /// Second-pass lexical boost (see the proto's `BoostRescore`): score
    /// the top-`window` hits against the boost query's terms through the
    /// candidate-scoped `Bm25Rescore` seam and reorder the window by
    /// `base_weight * base + boost_weight * boost`; hits beyond the
    /// window keep their relative order after it. Exactly one of
    /// `hits` / `cascade_hits` is non-empty; `base` is the fused score
    /// for the former, the phase-2 BM25 score for the latter (whose
    /// ranks are reassigned to the final order).
    pub async fn apply_boost(
        &self,
        boost: &crate::pb::BoostRescore,
        spec: Option<&crate::pb::AnalysisSpec>,
        hits: &mut [HybridHit],
        cascade_hits: &mut [CascadeHit],
        debug: &mut Option<HybridDebug>,
    ) -> Result<(), Status> {
        if boost.text.is_empty() {
            return Err(Status::invalid_argument(
                "boost.text must be non-empty when boost is present",
            ));
        }
        let t0 = std::time::Instant::now();
        let base_w = if boost.base_weight == 0.0 {
            1.0
        } else {
            f64::from(boost.base_weight)
        };
        let boost_w = if boost.boost_weight == 0.0 {
            1.0
        } else {
            f64::from(boost.boost_weight)
        };
        let len = hits.len().max(cascade_hits.len());
        let window = if boost.window == 0 {
            len
        } else {
            (boost.window as usize).min(len)
        };

        // Analyze the boost text with the SAME options as the main query
        // (term identity must match the index, as everywhere).
        let addr = self.analysis_addr.clone().ok_or_else(|| {
            Status::unavailable("no analysis sidecar configured on the coordinator (analysis_addr)")
        })?;
        let analyzed = crate::analyzer::analyze_document(&addr, &boost.text, spec).await?;
        let mut terms: Vec<String> = Vec::new();
        for (term, _, _) in analyzed.into_body().terms {
            if !terms.contains(&term) {
                terms.push(term);
            }
        }

        // Candidate-scoped scoring of the window, routed by owning shard.
        let mut scores: HashMap<u64, f64> = HashMap::new();
        if window > 0 && !terms.is_empty() {
            let global = self.global_bm25_stats(&terms).await?;
            let mut by_shard: HashMap<u32, Vec<u64>> = HashMap::new();
            for (doc_id, shard) in hits[..window.min(hits.len())]
                .iter()
                .map(|h| (h.doc_id, h.shard))
                .chain(
                    cascade_hits[..window.min(cascade_hits.len())]
                        .iter()
                        .map(|h| (h.doc_id, h.shard)),
                )
            {
                by_shard.entry(shard).or_default().push(doc_id);
            }
            let mut rescore_tasks = Vec::with_capacity(by_shard.len());
            for (shard, ids) in by_shard {
                let node = &self.node_addrs[shard as usize];
                let request = Bm25RescoreRequest {
                    terms: terms.clone(),
                    global_doc_count: global.doc_count,
                    global_total_doc_length: global.total_doc_length,
                    global_doc_frequencies: global.dfs.clone(),
                    candidate_ids: ids,
                    k1: self.bm25_params.k1 as f32,
                    b: self.bm25_params.b as f32,
                };
                let mut client = self.node_client(node)?;
                rescore_tasks.push(tokio::spawn(async move {
                    client
                        .bm25_rescore(request)
                        .await
                        .map(|r| r.into_inner().hits)
                }));
            }
            for task in rescore_tasks {
                let shard_hits = task
                    .await
                    .map_err(|e| Status::internal(format!("boost rescore task failed: {e}")))??;
                for hit in shard_hits {
                    scores.insert(hit.doc_id, f64::from(hit.score));
                }
            }
        }

        if !hits.is_empty() {
            let window = window.min(hits.len());
            for h in &mut hits[..window] {
                h.boost_score = scores.get(&h.doc_id).copied().unwrap_or(0.0) as f32;
            }
            hits[..window].sort_by(|a, b| {
                let fa = base_w * f64::from(a.fused_score) + boost_w * f64::from(a.boost_score);
                let fb = base_w * f64::from(b.fused_score) + boost_w * f64::from(b.boost_score);
                fb.total_cmp(&fa)
                    .then_with(|| a.shard.cmp(&b.shard))
                    .then_with(|| a.doc_id.cmp(&b.doc_id))
            });
        } else if !cascade_hits.is_empty() {
            let window = window.min(cascade_hits.len());
            for h in &mut cascade_hits[..window] {
                h.boost_score = scores.get(&h.doc_id).copied().unwrap_or(0.0) as f32;
            }
            cascade_hits[..window].sort_by(|a, b| {
                let fa = base_w * f64::from(a.bm25_score) + boost_w * f64::from(a.boost_score);
                let fb = base_w * f64::from(b.bm25_score) + boost_w * f64::from(b.boost_score);
                fb.total_cmp(&fa)
                    .then_with(|| b.vector_score.total_cmp(&a.vector_score))
                    .then_with(|| a.doc_id.cmp(&b.doc_id))
            });
            for (i, hit) in cascade_hits.iter_mut().enumerate() {
                hit.rank = i as u32 + 1;
            }
        }

        if let Some(d) = debug.as_mut() {
            d.boost_terms = terms;
            d.boost_ms = t0.elapsed().as_secs_f32() * 1e3;
            d.total_ms += d.boost_ms;
        }
        Ok(())
    }

    /// Push one TQ+ calibration to every configured node (the
    /// shared-calibration handshake that makes vector scores globally
    /// comparable). Per-node outcomes are reported, not fail-fast: a
    /// non-empty shard legitimately refuses (calibration is locked for
    /// the index lifetime), and the caller needs to know which nodes
    /// diverged.
    pub async fn fanout_calibration(
        &self,
        req: &BroadcastCalibrationRequest,
    ) -> Vec<CalibrationApplyResult> {
        let mut tasks = Vec::with_capacity(self.node_addrs.len());
        for node in &self.node_addrs {
            let node = node.clone();
            let request = SetCalibrationRequest {
                dim: req.dim,
                bit_width: req.bit_width,
                shift: req.shift.clone(),
                scale: req.scale.clone(),
            };
            let client = self.node_client(&node);
            tasks.push(tokio::spawn(async move {
                let result = match client {
                    Ok(mut client) => client.set_calibration(request).await,
                    Err(e) => Err(e),
                };
                match result {
                    Ok(resp) => CalibrationApplyResult {
                        node: node.clone(),
                        ok: true,
                        already_seeded: resp.into_inner().already_seeded,
                        error: String::new(),
                    },
                    Err(e) => CalibrationApplyResult {
                        node: node.clone(),
                        ok: false,
                        already_seeded: false,
                        error: e.message().to_string(),
                    },
                }
            }));
        }
        let mut results = Vec::with_capacity(tasks.len());
        for task in tasks {
            match task.await {
                Ok(r) => results.push(r),
                Err(e) => results.push(CalibrationApplyResult {
                    node: String::new(),
                    ok: false,
                    already_seeded: false,
                    error: format!("task failed: {e}"),
                }),
            }
        }
        results
    }

    /// Run one A/B arm through the ordinary handler for its kind.
    ///
    /// This dispatches to `bm25_search`/`hybrid_search` rather than
    /// reimplementing them: the comparison is only trustworthy if each
    /// arm executes the path it would execute in production, and a
    /// parallel scoring path written for the A/B would drift from the
    /// served one until the diff was measuring the drift.
    async fn run_variant(
        &self,
        query: &search_variant::Query,
        k: u32,
    ) -> Result<Vec<RankedHit>, Status> {
        match query {
            search_variant::Query::Bm25(req) => {
                // `k` is the request's shared depth; an arm's own k is
                // ignored so the rankings stay comparable.
                let mut req = req.clone();
                req.k = k;
                let resp = SearchService::bm25_search(self, Request::new(req))
                    .await?
                    .into_inner();
                Ok(resp
                    .hits
                    .into_iter()
                    .map(|h| RankedHit {
                        doc_id: h.doc_id,
                        score: h.score,
                    })
                    .collect())
            }
            search_variant::Query::Hybrid(req) => {
                let mut req = req.clone();
                req.k = k;
                // The profile block is per-arm noise here and the caller
                // asked for a comparison, not a trace.
                req.debug = false;
                let resp = SearchService::hybrid_search(self, Request::new(req))
                    .await?
                    .into_inner();
                // CASCADE reports in `cascade_hits` and leaves `hits`
                // empty; the other modes do the reverse. Both are a
                // ranking, which is all a diff needs.
                if resp.hits.is_empty() {
                    Ok(resp
                        .cascade_hits
                        .into_iter()
                        .map(|h| RankedHit {
                            doc_id: h.doc_id,
                            score: h.bm25_score,
                        })
                        .collect())
                } else {
                    Ok(resp
                        .hits
                        .into_iter()
                        .map(|h| RankedHit {
                            doc_id: h.doc_id,
                            score: h.fused_score,
                        })
                        .collect())
                }
            }
        }
    }
}

/// Diff one arm against the reference arm.
///
/// Split out from the handler so the measure wiring is testable without a
/// cluster: everything below the fan-out is a pure function of two
/// rankings.
fn diff_against(
    reference: &VariantResult,
    variant: &VariantResult,
    k: usize,
    rbo_p: f64,
) -> RankingDiff {
    let ref_ids: Vec<u64> = reference.hits.iter().map(|h| h.doc_id).collect();
    let var_ids: Vec<u64> = variant.hits.iter().map(|h| h.doc_id).collect();
    let depth = k.min(ref_ids.len()).min(var_ids.len());
    let overlap_fraction = rankdiff::overlap_at_k(&ref_ids, &var_ids, k);
    // The reference's own scores are the yardstick for both sides, so
    // regret never compares a BM25 score with a fused one.
    let scored: Vec<(u64, f32)> = reference
        .hits
        .iter()
        .map(|h| (h.doc_id, h.score))
        .collect();
    let regret = rankdiff::score_regret(&scored, &var_ids, k);
    RankingDiff {
        reference: reference.label.clone(),
        variant: variant.label.clone(),
        depth: depth as u32,
        // Recovered from the fraction rather than recounted, so the two
        // can never disagree.
        overlap: (overlap_fraction * depth as f64).round() as u32,
        overlap_fraction: overlap_fraction as f32,
        kendall_tau: rankdiff::kendall_tau(&ref_ids, &var_ids) as f32,
        rbo: rankdiff::rbo(&ref_ids, &var_ids, rbo_p) as f32,
        score_regret: regret.mean as f32,
        regret_counted: regret.counted as u32,
        regret_unscored: regret.unscored as u32,
        top1_flipped: rankdiff::top1_flipped(&ref_ids, &var_ids),
    }
}

/// Total-order f64 wrapper for heap keys.
#[derive(PartialEq)]
struct F64Ord(f64);
impl Eq for F64Ord {}
impl PartialOrd for F64Ord {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for F64Ord {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}

/// Magnitude-relative slack for the decomposed-floor bounds: 2^-40 of
/// the participating magnitudes, about four thousand times the worst
/// accumulated f64 rounding error of the three operations involved and
/// one part in 10^12 of the scores — provably conservative, immeasurably
/// loose.
const SLACK_REL: f64 = 1.0 / (1u64 << 40) as f64;

/// The decomposed vector floor for a fused lower bound `s_lb`:
/// mathematically (s_lb - w_b * b_1) / w_v, rounded DOWN past every
/// f64 rounding error and the f32 cast, so that v(d) < result implies
/// w_v * v(d) + w_b * b(d) < s_lb for every doc (b(d) <= b_1 by the
/// exactness of the BM25 leg). `wb_b1` arrives premultiplied.
fn decomposed_floor(s_lb: f64, wb_b1: f64, w_v: f64) -> f32 {
    let t = (s_lb - wb_b1) / w_v;
    let mag = t.abs() + (s_lb.abs() + wb_b1.abs()) / w_v;
    let safe = t - mag * SLACK_REL - f64::MIN_POSITIVE;
    (safe as f32).next_down()
}

/// One open `StreamSearch` fan-out: a merged inbound lane plus the
/// per-shard floor lanes (reliable stream sender + optional UDP token).
/// Shared by every streaming consumer — plain top-k, document mode,
/// and the decomposed hybrid — which differ only in what they do with
/// the batches and which floor they derive.
struct StreamFanout {
    merged_rx: mpsc::Receiver<(usize, Result<Option<StreamSearchResponse>, Status>)>,
    floor_txs: Vec<Option<mpsc::Sender<StreamSearchRequest>>>,
    udp_lanes: Vec<Option<(u64, std::net::SocketAddr)>>,
}

impl StreamFanout {
    /// The next inbound message: `Ok(Some((shard, msg)))` for a payload,
    /// `Ok(None)` for a clean post-summary stream close (callers just
    /// continue), and an error for a shard failure or a close without a
    /// summary (a protocol break — the summary is the exactness
    /// certificate, so a stream that vanishes without one aborts the
    /// query).
    async fn next_message(
        &mut self,
        summaries: &[Option<StreamSearchSummary>],
    ) -> Result<Option<(usize, StreamSearchResponse)>, Status> {
        let Some((shard, item)) = self.merged_rx.recv().await else {
            return Err(Status::internal("stream fan-out ended without all shards"));
        };
        match item {
            Ok(Some(msg)) => Ok(Some((shard, msg))),
            Ok(None) => {
                if summaries[shard].is_none() {
                    return Err(Status::internal(format!(
                        "shard {shard} closed its stream without a summary"
                    )));
                }
                Ok(None)
            }
            Err(e) => Err(Status::internal(format!("shard {shard} failed: {e}"))),
        }
    }
}

/// Everything one shard-stream attempt needs, cheap to clone per attempt
/// (a hedged retry is just a second attempt with the same context).
#[derive(Clone)]
struct ShardQueryCtx {
    request_id: Arc<str>,
    vector: Arc<Vec<f32>>,
    k: u32,
    tie_complete: bool,
    /// Collapse-by-parent mode (see StartShardSearch.collapse_parents).
    collapse: bool,
    /// Merges every shard's published floor into the running global max.
    tracker: Arc<Mutex<FloorTracker>>,
    /// Conflating broadcast cell for the global floor: pumps write raises
    /// here; per-stream forwarders relay whatever is LATEST when they
    /// wake, so a burst of raises becomes one message per stream.
    gfloor: Arc<watch::Sender<f32>>,
    /// Hedge legs launched (a shard's primary outran its hedge delay) and
    /// hedge legs that beat their primary. Both are pure accounting: a
    /// benchmark cannot otherwise tell "no hedge fired" from "the hedge
    /// fired and did not help".
    hedges: Arc<AtomicU64>,
    hedge_wins: Arc<AtomicU64>,
}

fn floor_message(floor: f32) -> SearchShardRequest {
    SearchShardRequest {
        payload: Some(search_shard_request::Payload::FloorUpdate(FloorUpdate {
            floor,
        })),
    }
}

/// One `SearchShard` stream attempt against one node: Start, pump floors
/// both ways, return the terminal Done.
async fn run_shard_stream(
    shard: u32,
    mut client: NodeServiceClient<Channel>,
    ctx: ShardQueryCtx,
) -> Result<SearchShardDone, Status> {
    let (req_tx, req_rx) = mpsc::channel::<SearchShardRequest>(8);
    req_tx
        .send(SearchShardRequest {
            payload: Some(search_shard_request::Payload::Start(StartShardSearch {
                request_id: ctx.request_id.to_string(),
                k: ctx.k,
                vector: ctx.vector.as_ref().clone(),
                tie_complete: ctx.tie_complete,
                collapse_parents: ctx.collapse,
            })),
        })
        .await
        .map_err(|_| Status::internal("node request channel closed before Start"))?;
    let mut responses = client
        .search_shard(ReceiverStream::new(req_rx))
        .await?
        .into_inner();

    // A late starter (a hedged replica) joins with the floor already
    // raised — seed it immediately instead of waiting for the next raise.
    let current = *ctx.gfloor.borrow();
    if current != f32::NEG_INFINITY {
        let _ = req_tx.try_send(floor_message(current));
    }

    // Conflating forwarder: on every wake, relay only the LATEST floor.
    // try_send on a full channel just drops this raise — floors are
    // monotone, so the next raise supersedes it; a dropped floor delays
    // pruning but never affects results.
    let mut floor_rx = ctx.gfloor.subscribe();
    let forwarder = tokio::spawn(async move {
        while floor_rx.changed().await.is_ok() {
            let floor = *floor_rx.borrow_and_update();
            if let Err(mpsc::error::TrySendError::Closed(_)) = req_tx.try_send(floor_message(floor))
            {
                break;
            }
        }
    });

    let result = loop {
        match responses.message().await {
            Ok(Some(SearchShardResponse {
                payload: Some(search_shard_response::Payload::FloorUpdate(u)),
            })) => {
                let raised = ctx
                    .tracker
                    .lock()
                    .expect("floor tracker mutex poisoned")
                    .observe(u.floor);
                if let Some(floor) = raised {
                    // send_if_modified with a strict raise: two racing
                    // pumps can never lower the broadcast value.
                    ctx.gfloor.send_if_modified(|cur| {
                        if floor > *cur {
                            *cur = floor;
                            true
                        } else {
                            false
                        }
                    });
                }
            }
            Ok(Some(SearchShardResponse {
                payload: Some(search_shard_response::Payload::Done(done)),
            })) => break Ok(done),
            Ok(Some(_)) => {}
            Ok(None) => {
                break Err(Status::data_loss(format!(
                    "shard {shard}: stream closed before Done"
                )))
            }
            Err(e) => break Err(e),
        }
    };
    forwarder.abort();
    result
}

/// One shard's full query attempt: primary stream, hedged replica after
/// `hedge_delay` (first success wins), failover on primary error, all
/// bounded by `shard_deadline`.
async fn run_shard_with_hedge(
    shard: u32,
    primary: NodeServiceClient<Channel>,
    replica: Option<NodeServiceClient<Channel>>,
    ctx: ShardQueryCtx,
    limits: FanoutLimits,
) -> Result<SearchShardDone, Status> {
    let attempt = async {
        let primary_run = run_shard_stream(shard, primary, ctx.clone());
        match (replica, limits.hedge_delay) {
            (Some(rep), Some(delay)) => {
                tokio::pin!(primary_run);
                tokio::select! {
                    r = &mut primary_run => match r {
                        Ok(done) => Ok(done),
                        // Primary failed before the hedge window: go
                        // straight to the replica.
                        Err(pe) => run_shard_stream(shard, rep, ctx.clone())
                            .await
                            .map_err(|re| both_failed(shard, &pe, &re)),
                    },
                    _ = tokio::time::sleep(delay) => {
                        ctx.hedges.fetch_add(1, AtomicOrdering::Relaxed);
                        let replica_run = run_shard_stream(shard, rep, ctx.clone());
                        tokio::pin!(replica_run);
                        tokio::select! {
                            r = &mut primary_run => match r {
                                Ok(done) => Ok(done),
                                Err(pe) => replica_run
                                    .await
                                    .map_err(|re| both_failed(shard, &pe, &re)),
                            },
                            r = &mut replica_run => match r {
                                Ok(done) => {
                                    ctx.hedge_wins.fetch_add(1, AtomicOrdering::Relaxed);
                                    Ok(done)
                                }
                                Err(re) => primary_run
                                    .await
                                    .map_err(|pe| both_failed(shard, &pe, &re)),
                            },
                        }
                    }
                }
            }
            // Replica without a hedge delay: pure failover.
            (Some(rep), None) => match primary_run.await {
                Ok(done) => Ok(done),
                Err(pe) => run_shard_stream(shard, rep, ctx.clone())
                    .await
                    .map_err(|re| both_failed(shard, &pe, &re)),
            },
            (None, _) => primary_run.await,
        }
    };
    match limits.shard_deadline {
        Some(deadline) => tokio::time::timeout(deadline, attempt).await.map_err(|_| {
            Status::deadline_exceeded(format!(
                "shard {shard} exceeded its {}ms deadline",
                deadline.as_millis()
            ))
        })?,
        None => attempt.await,
    }
}

fn both_failed(shard: u32, primary: &Status, replica: &Status) -> Status {
    Status::unavailable(format!(
        "shard {shard}: primary failed ({primary}); replica failed ({replica})"
    ))
}

/// Resolved per-leg options for one hybrid query.
#[derive(Debug, Clone, Copy)]
pub struct HybridLegs {
    /// Depth each leg (and each shard's fused list) is fetched to.
    pub leg_k: u32,
    /// Vector-leg weight (RRF weight, or blend weight under
    /// SCORE_BLEND). RESOLVED: exactly 0 disables the leg.
    pub vector_weight: f32,
    /// BM25-leg weight (same dual role, same disable rule).
    pub bm25_weight: f32,
    /// RRF constant.
    pub rrf_k: f64,
    /// Fusion strategy (default GLOBAL_RANK).
    pub fusion_mode: FusionMode,
    /// SCORE_BLEND: per-leg score normalization.
    pub normalization: fusion::Normalization,
    /// SCORE_BLEND: combination of normalized leg scores.
    pub combination: fusion::Combination,
    /// Vector-score floor on the result set (see the proto); 0 = off.
    pub min_vector_score: f32,
}

/// Outcome of one coordinator fan-out: the merged global top-k plus the
/// per-shard scan statistics (in completion order), the latter powering the
/// floor-sharing benchmark.
#[derive(Debug)]
pub struct FanoutResult {
    /// Merged global top-k (score desc, shard/id tie-break).
    pub hits: Vec<ScoredHit>,
    /// Per-shard scan stats, one entry per shard, in completion order.
    pub shard_stats: Vec<Option<ShardScanStats>>,
    /// The raw per-shard lists (shard index, `(doc_id, score)`), in
    /// completion order. With `tie_complete` set, a shard's list may
    /// exceed k by its boundary tie group. The cascade pipeline routes
    /// candidates by these shard assignments.
    pub shard_hits: Vec<(u32, Vec<(u64, f32)>)>,
    /// Per-shard wall time in milliseconds as measured at the
    /// coordinator (shard index, ms), in completion order.
    pub shard_wall_ms: Vec<(u32, f32)>,
    /// Hedge legs launched during this fan-out, and how many of them
    /// returned before their primary.
    pub hedges_fired: u64,
    pub hedge_wins: u64,
}

/// Result of [`CoordinatorServiceImpl::fanout_stream_search`].
/// What a document-mode streaming fan-out returns (see
/// [`CoordinatorServiceImpl::fanout_stream_search_collapse`]).
pub struct CollapseStreamResult {
    /// The top-k parents' representatives: each parent's best chunk,
    /// score descending, ties by chunk id; `parent_id` set.
    pub hits: Vec<ScoredHit>,
    /// One group per entry of `hits`, same order: every chunk of that
    /// parent scoring at or above `chunk_floor`.
    pub groups: Vec<ParentGroup>,
    /// One ULP below the k-th best parent score, or -inf when fewer
    /// than k parents exist.
    pub chunk_floor: f32,
    /// Every shard's terminal summary (all certified `completed`).
    pub summaries: Vec<StreamSearchSummary>,
    /// Parent-floor raises pushed to the fleet.
    pub floors_sent: u64,
}

pub struct StreamFanoutResult {
    /// Merged global top-k, in the same total order as
    /// [`FanoutResult::hits`].
    pub hits: Vec<ScoredHit>,
    /// Per-shard terminal summaries, shard order; every one certified
    /// `completed`.
    pub summaries: Vec<StreamSearchSummary>,
    /// Floor raises this coordinator broadcast.
    pub floors_sent: u64,
}

/// Heap wrapper whose `Ord` IS the merge's total order ([`cmp_hits`]:
/// better entries compare Less), so a max-heap's peek is the worst
/// survivor — the running k-th best.
struct StreamHeapEntry(MergedHit);

impl PartialEq for StreamHeapEntry {
    fn eq(&self, other: &Self) -> bool {
        cmp_hits(&self.0, &other.0) == std::cmp::Ordering::Equal
    }
}
impl Eq for StreamHeapEntry {}
impl PartialOrd for StreamHeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for StreamHeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        cmp_hits(&self.0, &other.0)
    }
}

#[tonic::async_trait]
impl SearchService for CoordinatorServiceImpl {
    async fn search(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        let req = request.into_inner();
        if req.vector.is_empty() {
            return Err(Status::invalid_argument("empty query vector"));
        }
        if req.vector.iter().any(|x| !x.is_finite()) {
            return Err(Status::invalid_argument(
                "query vector has non-finite coordinates",
            ));
        }
        let request_id = if req.request_id.is_empty() {
            format!(
                "req-{}-{}",
                std::process::id(),
                REQUEST_COUNTER.fetch_add(1, AtomicOrdering::Relaxed)
            )
        } else {
            req.request_id.clone()
        };

        let result = if req.collapse_parents {
            // Document mode on a streaming coordinator: parents
            // aggregate here from tagged chunk emissions, and the
            // response carries the per-parent chunk groups. The bidi
            // path collapses shard-side and returns representatives
            // only.
            if self.stream_search {
                let doc = self
                    .fanout_stream_search_collapse(&request_id, &req.vector, req.k)
                    .await?;
                return Ok(Response::new(SearchResponse {
                    request_id,
                    hits: doc.hits,
                    groups: doc.groups,
                    chunk_floor: doc.chunk_floor,
                }));
            }
            self.fanout_search_collapse(&request_id, &req.vector, req.k)
                .await?
        } else if self.stream_search {
            let streamed = self
                .fanout_stream_search(&request_id, &req.vector, req.k, None)
                .await?;
            return Ok(Response::new(SearchResponse {
                request_id,
                hits: streamed.hits,
                groups: Vec::new(),
                chunk_floor: 0.0,
            }));
        } else {
            self.fanout_search(&request_id, &req.vector, req.k, false)
                .await?
        };
        Ok(Response::new(SearchResponse {
            request_id,
            hits: result.hits,
            groups: Vec::new(),
            chunk_floor: 0.0,
        }))
    }

    async fn bm25_search(
        &self,
        request: Request<Bm25SearchRequest>,
    ) -> Result<Response<Bm25SearchResponse>, Status> {
        let req = request.into_inner();
        if req.min_score.is_nan() || req.min_score == f32::NEG_INFINITY {
            return Err(Status::invalid_argument(
                "min_score must be finite (NaN and -inf are not valid floors)",
            ));
        }
        let hits = if req.fields.is_empty() {
            self.fanout_bm25_seeded(&req.text, req.k, req.analysis.as_ref(), req.min_score)
                .await?
        } else {
            self.fanout_bm25_fused(&req.text, req.k, &req.fields, req.min_score)
                .await?
        };
        // The merged k-th best: one f32 ULP below the last hit's score
        // when k hits were returned (see `bm25::floor_seed` — a later
        // seed can never exceed the true k-th best), 0 otherwise.
        let kth_best = if hits.len() == req.k as usize {
            hits.last()
                .map(|h| crate::bm25::floor_seed(h.score))
                .unwrap_or(0.0)
        } else {
            0.0
        };
        Ok(Response::new(Bm25SearchResponse { hits, kth_best }))
    }

    async fn hybrid_search(
        &self,
        request: Request<HybridSearchRequest>,
    ) -> Result<Response<HybridSearchResponse>, Status> {
        let req = request.into_inner();
        if req.text.is_empty() {
            return Err(Status::invalid_argument(
                "hybrid search requires query text",
            ));
        }
        if req.vector.is_empty() {
            return Err(Status::invalid_argument(
                "hybrid search requires a query vector",
            ));
        }
        if req.vector.iter().any(|x| !x.is_finite()) {
            return Err(Status::invalid_argument(
                "query vector has non-finite coordinates",
            ));
        }
        let request_id = if req.request_id.is_empty() {
            format!(
                "req-{}-{}",
                std::process::id(),
                REQUEST_COUNTER.fetch_add(1, AtomicOrdering::Relaxed)
            )
        } else {
            req.request_id.clone()
        };
        // leg_k: default max(k, rrf_k) so the RRF constant never exceeds a
        // leg's depth; explicit values below k are clamped to k.
        let options = req.legs.unwrap_or_default();
        let rrf_k = if options.rrf_k == 0.0 {
            fusion::DEFAULT_RRF_K
        } else {
            f64::from(options.rrf_k)
        };
        // Weights: absent = 1.0; an explicit 0 disables the leg (the
        // proto documents which modes support that).
        let vector_weight = options.vector_weight.unwrap_or(1.0);
        let bm25_weight = options.bm25_weight.unwrap_or(1.0);
        if vector_weight == 0.0 && bm25_weight == 0.0 {
            return Err(Status::invalid_argument(
                "both legs disabled: at least one of vector_weight/bm25_weight must be nonzero",
            ));
        }
        if options.fusion_mode() == FusionMode::TwoLevel
            && (vector_weight == 0.0 || bm25_weight == 0.0)
        {
            return Err(Status::invalid_argument(
                "TWO_LEVEL cannot disable a leg (its node wire format cannot distinguish \
                 0 from unset); use GLOBAL_RANK or SCORE_BLEND",
            ));
        }
        // The decomposed floor algebra divides by vector_weight and
        // scales bounds by bm25_weight; both must be strictly positive
        // (a single-leg query belongs to GLOBAL_RANK or SCORE_BLEND).
        if options.fusion_mode() == FusionMode::Decomposed
            && !(vector_weight > 0.0
                && vector_weight.is_finite()
                && bm25_weight > 0.0
                && bm25_weight.is_finite())
        {
            return Err(Status::invalid_argument(
                "DECOMPOSED requires finite leg weights > 0 for both legs",
            ));
        }
        if options.min_vector_score.is_nan() {
            return Err(Status::invalid_argument("min_vector_score must not be NaN"));
        }
        let legs = HybridLegs {
            leg_k: if options.leg_k == 0 {
                req.k.max(rrf_k as u32)
            } else {
                options.leg_k.max(req.k)
            },
            vector_weight,
            bm25_weight,
            rrf_k,
            fusion_mode: options.fusion_mode(),
            normalization: match options.normalization() {
                crate::pb::ScoreNormalization::ZScore => fusion::Normalization::ZScore,
                crate::pb::ScoreNormalization::None => fusion::Normalization::None,
                _ => fusion::Normalization::MinMax,
            },
            combination: match options.combination() {
                crate::pb::ScoreCombination::Geometric => fusion::Combination::Geometric,
                crate::pb::ScoreCombination::Harmonic => fusion::Combination::Harmonic,
                _ => fusion::Combination::Arithmetic,
            },
            min_vector_score: options.min_vector_score,
        };
        let (mut hits, mut cascade_hits, mut debug) = match legs.fusion_mode {
            FusionMode::Cascade | FusionMode::Unspecified => {
                let (cascade_hits, debug) = self
                    .fanout_cascade(
                        &request_id,
                        &req.text,
                        &req.vector,
                        req.k,
                        req.analysis.as_ref(),
                        legs.min_vector_score,
                        req.debug,
                    )
                    .await?;
                (Vec::new(), cascade_hits, debug)
            }
            _ => {
                let (hits, debug) = self
                    .fanout_hybrid(
                        &request_id,
                        &req.text,
                        &req.vector,
                        req.k,
                        req.analysis.as_ref(),
                        legs,
                        req.debug,
                    )
                    .await?;
                (hits, Vec::new(), debug)
            }
        };
        if let Some(boost) = &req.boost {
            self.apply_boost(
                boost,
                req.analysis.as_ref(),
                &mut hits,
                &mut cascade_hits,
                &mut debug,
            )
            .await?;
        }
        Ok(Response::new(HybridSearchResponse {
            request_id,
            hits,
            cascade_hits,
            debug,
        }))
    }

    async fn cluster_health(
        &self,
        _request: Request<ClusterHealthRequest>,
    ) -> Result<Response<ClusterHealthResponse>, Status> {
        // Probe every primary, then every configured replica. Unreachable
        // is a reported outcome, never an error; a 2s probe timeout keeps
        // one filtered port from stalling the whole report.
        let mut probes: Vec<(u32, String, bool)> = Vec::new();
        for (shard, addr) in self.node_addrs.iter().enumerate() {
            probes.push((shard as u32, addr.clone(), false));
        }
        for (shard, replica) in self.replica_addrs.iter().enumerate() {
            if let Some(addr) = replica {
                probes.push((shard as u32, addr.clone(), true));
            }
        }
        let mut tasks = Vec::with_capacity(probes.len());
        for (shard, addr, is_replica) in probes {
            let client = self.node_client(&addr);
            tasks.push(tokio::spawn(async move {
                let outcome = match client {
                    Ok(mut client) => {
                        match tokio::time::timeout(
                            Duration::from_secs(2),
                            client.health(HealthRequest {}),
                        )
                        .await
                        {
                            Ok(reply) => reply.map(|r| r.into_inner()),
                            Err(_) => Err(Status::deadline_exceeded("health probe timed out")),
                        }
                    }
                    Err(e) => Err(e),
                };
                match outcome {
                    Ok(health) => ShardHealth {
                        shard,
                        addr,
                        is_replica,
                        reachable: true,
                        error: String::new(),
                        health: Some(health),
                    },
                    Err(e) => ShardHealth {
                        shard,
                        addr,
                        is_replica,
                        reachable: false,
                        error: e.message().to_string(),
                        health: None,
                    },
                }
            }));
        }
        let mut targets = Vec::with_capacity(tasks.len());
        for task in tasks {
            match task.await {
                Ok(target) => targets.push(target),
                Err(e) => return Err(Status::internal(format!("health probe task failed: {e}"))),
            }
        }
        Ok(Response::new(ClusterHealthResponse { targets }))
    }

    async fn broadcast_calibration(
        &self,
        request: Request<BroadcastCalibrationRequest>,
    ) -> Result<Response<BroadcastCalibrationResponse>, Status> {
        let req = request.into_inner();
        if req.shift.len() != req.dim as usize || req.scale.len() != req.dim as usize {
            return Err(Status::invalid_argument(
                "shift and scale must have length dim",
            ));
        }
        let results = self.fanout_calibration(&req).await;
        Ok(Response::new(BroadcastCalibrationResponse { results }))
    }

    async fn variant_search(
        &self,
        request: Request<VariantSearchRequest>,
    ) -> Result<Response<VariantSearchResponse>, Status> {
        let req = request.into_inner();
        if req.variants.len() < 2 {
            return Err(Status::invalid_argument(format!(
                "variant search compares configurations: at least 2 variants required, got {}",
                req.variants.len()
            )));
        }
        if req.k == 0 {
            return Err(Status::invalid_argument("k must be positive"));
        }
        // Labels carry the whole result: a blank or duplicated one makes
        // the diffs unreadable, so reject rather than disambiguate.
        let mut seen: Vec<&str> = Vec::with_capacity(req.variants.len());
        for (i, v) in req.variants.iter().enumerate() {
            if v.label.is_empty() {
                return Err(Status::invalid_argument(format!(
                    "variant {i} has an empty label; every arm must be named"
                )));
            }
            if seen.contains(&v.label.as_str()) {
                return Err(Status::invalid_argument(format!(
                    "duplicate variant label {:?}: labels identify arms in the diffs",
                    v.label
                )));
            }
            seen.push(&v.label);
            if v.query.is_none() {
                return Err(Status::invalid_argument(format!(
                    "variant {:?} has no query set (expected bm25 or hybrid)",
                    v.label
                )));
            }
        }
        // RBO's persistence is a probability; 1.0 would never terminate
        // its weighting and is as much an error as 2.0.
        let rbo_p = if req.rbo_p == 0.0 {
            0.9
        } else {
            f64::from(req.rbo_p)
        };
        if !(rbo_p.is_finite() && rbo_p > 0.0 && rbo_p < 1.0) {
            return Err(Status::invalid_argument(format!(
                "rbo_p must be in (0, 1); got {}",
                req.rbo_p
            )));
        }
        if req.interleave && req.variants.len() != 2 {
            return Err(Status::invalid_argument(format!(
                "interleaving is a two-way method (team draft); got {} variants. \
                 Compare more arms with diffs, or interleave them pairwise.",
                req.variants.len()
            )));
        }
        let request_id = if req.request_id.is_empty() {
            format!(
                "req-{}-{}",
                std::process::id(),
                REQUEST_COUNTER.fetch_add(1, AtomicOrdering::Relaxed)
            )
        } else {
            req.request_id.clone()
        };

        // Sequential, not concurrent. Running the arms together would
        // make each one's `elapsed_ms` a measure of how hard the other
        // arms were hitting the same shards, and per-arm cost is part of
        // what an A/B is asked to report.
        let mut results = Vec::with_capacity(req.variants.len());
        for variant in &req.variants {
            let query = variant
                .query
                .as_ref()
                .expect("query presence checked above");
            let started = std::time::Instant::now();
            let hits = self.run_variant(query, req.k).await.map_err(|e| {
                // Name the arm: with several in flight, an unadorned
                // status leaves the caller guessing which one failed.
                Status::new(
                    e.code(),
                    format!("variant {:?}: {}", variant.label, e.message()),
                )
            })?;
            results.push(VariantResult {
                label: variant.label.clone(),
                hits,
                elapsed_ms: started.elapsed().as_secs_f32() * 1000.0,
            });
        }

        let diffs: Vec<RankingDiff> = results[1..]
            .iter()
            .map(|v| diff_against(&results[0], v, req.k as usize, rbo_p))
            .collect();

        let interleaving = req.interleave.then(|| {
            let a: Vec<u64> = results[0].hits.iter().map(|h| h.doc_id).collect();
            let b: Vec<u64> = results[1].hits.iter().map(|h| h.doc_id).collect();
            // A seed derived from the query text keeps a re-run of the
            // same query byte-identical while still varying across
            // queries, so determinism does not become a first-position
            // bias for one arm.
            let seed = if req.interleave_seed == 0 {
                crate::interleave::seed_for(variant_text(&req.variants[0]))
            } else {
                req.interleave_seed
            };
            let merged = crate::interleave::team_draft(&a, &b, req.k as usize, seed);
            Interleaving {
                doc_ids: merged.ids,
                teams: merged
                    .team
                    .into_iter()
                    .map(|t| match t {
                        crate::interleave::Team::A => InterleaveTeam::A as i32,
                        crate::interleave::Team::B => InterleaveTeam::B as i32,
                    })
                    .collect(),
                seed,
            }
        });

        Ok(Response::new(VariantSearchResponse {
            request_id,
            results,
            diffs,
            interleaving,
        }))
    }
}

/// The query text of an arm, whatever kind it is — the stable thing to
/// seed an interleaving from.
fn variant_text(variant: &crate::pb::SearchVariant) -> &str {
    match &variant.query {
        Some(search_variant::Query::Bm25(r)) => &r.text,
        Some(search_variant::Query::Hybrid(r)) => &r.text,
        None => "",
    }
}

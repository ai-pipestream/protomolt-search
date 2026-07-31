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
use crate::merge::{merge_topk, FloorTracker};
use crate::pb::node_service_client::NodeServiceClient;
use crate::pb::search_service_server::{SearchService, SearchServiceServer};
use crate::pb::{
    search_shard_request, search_shard_response, Bm25Hit, Bm25QueryRequest, Bm25RescoreRequest,
    Bm25SearchRequest, Bm25SearchResponse, BroadcastCalibrationRequest,
    BroadcastCalibrationResponse, CalibrationApplyResult, CascadeHit, ClusterHealthRequest,
    ClusterHealthResponse, FloorUpdate, FusionMode, HealthRequest, HybridDebug, HybridHit,
    HybridSearchRequest, HybridShardDebug,
    HybridSearchResponse, HybridShardRequest, ScoredHit, SearchRequest, SearchResponse,
    SearchShardDone, SearchShardRequest, SearchShardResponse, SetCalibrationRequest, ShardHealth,
    ShardLegsRequest, ShardScanStats, StartShardSearch, TermStatsRequest,
};

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
    /// One reusable channel per address, created on first use.
    channels: Arc<Mutex<HashMap<String, Channel>>>,
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
            channels: Arc::new(Mutex::new(HashMap::new())),
        }
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
            .tcp_nodelay(true);
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
        for (term, _, _) in analyzed.terms {
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
        for (term, _, _) in analyzed.terms {
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
                    .term_stats(TermStatsRequest { terms })
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
            };
            let mut client = self.node_client(node)?;
            shard_tasks.push(tokio::spawn(async move {
                let t0 = std::time::Instant::now();
                client
                    .shard_legs(request)
                    .await
                    .map(|r| (shard as u32, t0.elapsed().as_secs_f32() * 1e3, r.into_inner()))
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
            };
            let mut client = self.node_client(node)?;
            shard_tasks.push(tokio::spawn(async move {
                let t0 = std::time::Instant::now();
                client
                    .hybrid_shard(request)
                    .await
                    .map(|r| (shard as u32, t0.elapsed().as_secs_f32() * 1e3, r.into_inner().hits))
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
                hits.retain(|h| {
                    h.vector_rank.is_some() && h.vector_score >= legs.min_vector_score
                });
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
        for (term, _, _) in analyzed.terms {
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
        for (term, _, _) in analyzed.terms {
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
                    client.bm25_rescore(request).await.map(|r| r.into_inner().hits)
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
}

/// Everything one shard-stream attempt needs, cheap to clone per attempt
/// (a hedged retry is just a second attempt with the same context).
#[derive(Clone)]
struct ShardQueryCtx {
    request_id: Arc<str>,
    vector: Arc<Vec<f32>>,
    k: u32,
    tie_complete: bool,
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
        Some(deadline) => tokio::time::timeout(deadline, attempt)
            .await
            .map_err(|_| {
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

        let result = self
            .fanout_search(&request_id, &req.vector, req.k, false)
            .await?;
        Ok(Response::new(SearchResponse {
            request_id,
            hits: result.hits,
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
        let hits = self
            .fanout_bm25_seeded(&req.text, req.k, req.analysis.as_ref(), req.min_score)
            .await?;
        // The merged k-th best: one f32 ULP below the last hit's score
        // when k hits were returned (see `bm25::floor_seed` — a later
        // seed can never exceed the true k-th best), 0 otherwise.
        let kth_best = if hits.len() == req.k as usize {
            hits
                .last()
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
                Err(e) => {
                    return Err(Status::internal(format!("health probe task failed: {e}")))
                }
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
}

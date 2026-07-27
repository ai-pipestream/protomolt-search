//! Coordinator side: client-facing [`SearchService`] that fans queries out
//! to shard nodes, aggregates their floors mid-scan, and merges results.

use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::bm25::{self, Bm25Params, CorpusStats};
use crate::fusion::{self, Leg};
use crate::merge::{merge_topk, FloorTracker};
use crate::pb::node_service_client::NodeServiceClient;
use crate::pb::search_service_server::{SearchService, SearchServiceServer};
use crate::pb::{
    search_shard_request, search_shard_response, Bm25Hit, Bm25QueryRequest, Bm25SearchRequest,
    Bm25SearchResponse, FloorUpdate, HybridHit, HybridSearchRequest, HybridSearchResponse,
    HybridShardRequest, ScoredHit, SearchRequest, SearchResponse, SearchShardDone,
    SearchShardRequest, SearchShardResponse, ShardScanStats, StartShardSearch, TermStatsRequest,
};

/// Process-unique request id counter for coordinator-assigned ids.
static REQUEST_COUNTER: AtomicU64 = AtomicU64::new(1);

/// The coordinator gRPC service.
///
/// Phase 1 keeps membership static: the node address list is fixed at
/// construction and every query fans out to every node. Each query opens a
/// fresh `SearchShard` stream per node (no connection pooling yet — tonic
/// channels are per-query here, which is simple and correct, if not the
/// cheapest).
#[derive(Clone)]
pub struct CoordinatorServiceImpl {
    /// Node addresses in `http://host:port` form, in stable shard order
    /// (index in this list is the shard index used for tie-breaking).
    node_addrs: Vec<String>,
    /// Analysis sidecar address for query analysis in Bm25Search.
    analysis_addr: Option<String>,
    /// BM25 tuning sent to every shard (identical scoring everywhere).
    bm25_params: Bm25Params,
}

impl CoordinatorServiceImpl {
    /// A coordinator over the given shard nodes (fan-out order = shard
    /// index for merge tie-breaks).
    pub fn new(node_addrs: Vec<String>) -> Self {
        Self {
            node_addrs,
            analysis_addr: None,
            bm25_params: Bm25Params::default(),
        }
    }

    /// Configure the BM25 path: analysis sidecar for query analysis and
    /// the scoring parameters every shard is told to use.
    pub fn with_bm25(mut self, analysis_addr: Option<String>, params: Bm25Params) -> Self {
        self.analysis_addr = analysis_addr;
        self.bm25_params = params;
        self
    }

    /// Distributed BM25 with the two-phase global-stats flow (see the
    /// proto comments on `SearchService.Bm25Search`).
    pub async fn fanout_bm25(
        &self,
        text: &str,
        k: u32,
        spec: Option<&crate::pb::AnalysisSpec>,
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
        let mut share_tasks = Vec::with_capacity(self.node_addrs.len());
        for node in &self.node_addrs {
            let node = node.clone();
            let terms = terms.clone();
            share_tasks.push(tokio::spawn(async move {
                let mut client = NodeServiceClient::connect(node.clone())
                    .await
                    .map_err(|e| Status::unavailable(format!("connect {node}: {e}")))?
                    .max_decoding_message_size(crate::MAX_MESSAGE_BYTES)
                    .max_encoding_message_size(crate::MAX_MESSAGE_BYTES);
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
        let global: CorpusStats = bm25::merge_stats(&shares);

        // (c) Bm25Query fan-out with the GLOBAL stats: every shard scores
        // identically, so (d) the merge is a straight top-k.
        let mut query_tasks = Vec::with_capacity(self.node_addrs.len());
        for (shard, node) in self.node_addrs.iter().enumerate() {
            let node = node.clone();
            let request = Bm25QueryRequest {
                terms: terms.clone(),
                k,
                global_doc_count: global.doc_count,
                global_total_doc_length: global.total_doc_length,
                global_doc_frequencies: global.dfs.clone(),
                k1: self.bm25_params.k1 as f32,
                b: self.bm25_params.b as f32,
            };
            query_tasks.push(tokio::spawn(async move {
                let mut client = NodeServiceClient::connect(node.clone())
                    .await
                    .map_err(|e| Status::unavailable(format!("connect {node}: {e}")))?
                    .max_decoding_message_size(crate::MAX_MESSAGE_BYTES)
                    .max_encoding_message_size(crate::MAX_MESSAGE_BYTES);
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

    /// Hybrid vector + BM25 search with two-level RRF fusion:
    ///
    /// 1. analyze `text` into query terms (same analysis options as
    ///    ingest, as in [`Self::fanout_bm25`]);
    /// 2. TermStats fan-out and merge into GLOBAL corpus stats;
    /// 3. `HybridShard` fan-out: each shard runs both legs and RRF-fuses
    ///    them locally (level one);
    /// 4. the coordinator RRF-merges the per-shard fused lists (level
    ///    two) and attaches per-leg provenance from the owning shard.
    pub async fn fanout_hybrid(
        &self,
        request_id: &str,
        text: &str,
        vector: &[f32],
        k: u32,
        spec: Option<&crate::pb::AnalysisSpec>,
        legs: HybridLegs,
    ) -> Result<Vec<HybridHit>, Status> {
        if k == 0 || vector.is_empty() {
            return Ok(Vec::new());
        }
        // Query analysis for the BM25 leg (same options as ingest).
        let addr = self.analysis_addr.clone().ok_or_else(|| {
            Status::unavailable("no analysis sidecar configured on the coordinator (analysis_addr)")
        })?;
        let analyzed = crate::analyzer::analyze_document(&addr, text, spec).await?;
        let mut terms: Vec<String> = Vec::new();
        for (term, _, _) in analyzed.terms {
            if !terms.contains(&term) {
                terms.push(term);
            }
        }

        // Global corpus stats for the BM25 leg.
        let mut share_tasks = Vec::with_capacity(self.node_addrs.len());
        for node in &self.node_addrs {
            let node = node.clone();
            let terms = terms.clone();
            share_tasks.push(tokio::spawn(async move {
                let mut client = NodeServiceClient::connect(node.clone())
                    .await
                    .map_err(|e| Status::unavailable(format!("connect {node}: {e}")))?
                    .max_decoding_message_size(crate::MAX_MESSAGE_BYTES)
                    .max_encoding_message_size(crate::MAX_MESSAGE_BYTES);
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
        let global: CorpusStats = bm25::merge_stats(&shares);

        // Level one: per-shard local fusion.
        let mut shard_tasks = Vec::with_capacity(self.node_addrs.len());
        for (shard, node) in self.node_addrs.iter().enumerate() {
            let node = node.clone();
            let request = HybridShardRequest {
                request_id: request_id.to_string(),
                k: legs.leg_k,
                vector: vector.to_vec(),
                terms: terms.clone(),
                global_doc_count: global.doc_count,
                global_total_doc_length: global.total_doc_length,
                global_doc_frequencies: global.dfs.clone(),
                vector_weight: legs.vector_weight,
                bm25_weight: legs.bm25_weight,
                rrf_k: legs.rrf_k as f32,
            };
            shard_tasks.push(tokio::spawn(async move {
                let mut client = NodeServiceClient::connect(node.clone())
                    .await
                    .map_err(|e| Status::unavailable(format!("connect {node}: {e}")))?
                    .max_decoding_message_size(crate::MAX_MESSAGE_BYTES)
                    .max_encoding_message_size(crate::MAX_MESSAGE_BYTES);
                client
                    .hybrid_shard(request)
                    .await
                    .map(|r| (shard as u32, r.into_inner().hits))
            }));
        }
        let mut shard_lists: Vec<(u32, Vec<crate::pb::HybridLegHit>)> = Vec::new();
        for task in shard_tasks {
            let (shard, hits) = task
                .await
                .map_err(|e| Status::internal(format!("hybrid shard task failed: {e}")))??;
            shard_lists.push((shard, hits));
        }

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
        Ok(fused
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
                }
            })
            .collect())
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
    /// 2. each node pump task feeds shard floor updates into one shared
    ///    [`FloorTracker`]; every raise is broadcast to all node streams;
    /// 3. each pump ends on the node's terminal `SearchShardDone`.
    pub async fn fanout_search(
        &self,
        request_id: &str,
        vector: &[f32],
        k: u32,
    ) -> Result<FanoutResult, Status> {
        let n_nodes = self.node_addrs.len();
        if n_nodes == 0 {
            return Err(Status::failed_precondition("no shard nodes configured"));
        }

        let (done_tx, mut done_rx) =
            mpsc::channel::<(u32, Result<SearchShardDone, Status>)>(n_nodes);
        let senders: Arc<Mutex<Vec<mpsc::Sender<SearchShardRequest>>>> =
            Arc::new(Mutex::new(Vec::with_capacity(n_nodes)));
        let tracker = Arc::new(Mutex::new(FloorTracker::new()));

        for (shard, addr) in self.node_addrs.iter().enumerate() {
            let mut client = NodeServiceClient::connect(addr.clone())
                .await
                .map_err(|e| Status::unavailable(format!("connect {addr}: {e}")))?
                .max_decoding_message_size(crate::MAX_MESSAGE_BYTES)
                .max_encoding_message_size(crate::MAX_MESSAGE_BYTES);

            let (req_tx, req_rx) = mpsc::channel::<SearchShardRequest>(64);
            req_tx
                .send(SearchShardRequest {
                    payload: Some(search_shard_request::Payload::Start(StartShardSearch {
                        request_id: request_id.to_string(),
                        k,
                        vector: vector.to_vec(),
                    })),
                })
                .await
                .map_err(|_| Status::internal("node request channel closed before Start"))?;
            senders.lock().expect("senders mutex poisoned").push(req_tx);

            let mut responses = client
                .search_shard(ReceiverStream::new(req_rx))
                .await?
                .into_inner();

            let tracker = tracker.clone();
            let senders = senders.clone();
            let done_tx = done_tx.clone();
            let shard = shard as u32;
            tokio::spawn(async move {
                let result = loop {
                    match responses.message().await {
                        Ok(Some(SearchShardResponse {
                            payload: Some(search_shard_response::Payload::FloorUpdate(u)),
                        })) => {
                            let raised = tracker
                                .lock()
                                .expect("floor tracker mutex poisoned")
                                .observe(u.floor);
                            if let Some(floor) = raised {
                                // Broadcast the new max to every node,
                                // including the publisher (a no-op there).
                                // try_send: a full channel only delays a
                                // floor, never corrupts results.
                                let txs: Vec<_> =
                                    senders.lock().expect("senders mutex poisoned").clone();
                                for tx in txs {
                                    let _ = tx.try_send(SearchShardRequest {
                                        payload: Some(search_shard_request::Payload::FloorUpdate(
                                            FloorUpdate { floor },
                                        )),
                                    });
                                }
                            }
                        }
                        Ok(Some(SearchShardResponse {
                            payload: Some(search_shard_response::Payload::Done(done)),
                        })) => {
                            break Ok(done);
                        }
                        Ok(Some(_)) => {}
                        Ok(None) => {
                            break Err(Status::data_loss(format!(
                                "shard {shard}: stream closed before Done"
                            )));
                        }
                        Err(e) => break Err(e),
                    }
                };
                let _ = done_tx.send((shard, result)).await;
            });
        }
        drop(done_tx);

        let mut shard_hits: Vec<(u32, Vec<(u64, f32)>)> = Vec::with_capacity(n_nodes);
        let mut shard_stats: Vec<Option<ShardScanStats>> = Vec::with_capacity(n_nodes);
        for _ in 0..n_nodes {
            match done_rx.recv().await {
                Some((shard, Ok(done))) => {
                    shard_hits.push((
                        shard,
                        done.hits
                            .into_iter()
                            .map(|h| (h.vector_id, h.score))
                            .collect(),
                    ));
                    shard_stats.push(done.stats);
                }
                Some((shard, Err(e))) => {
                    return Err(Status::internal(format!("shard {shard} failed: {e}")));
                }
                None => {
                    return Err(Status::internal("fan-out completed without all shards"));
                }
            }
        }

        let hits = merge_topk(shard_hits, k as usize)
            .into_iter()
            .map(|h| ScoredHit {
                vector_id: h.vector_id,
                score: h.score,
            })
            .collect();
        Ok(FanoutResult { hits, shard_stats })
    }
}

/// Resolved per-leg options for one hybrid query.
#[derive(Debug, Clone, Copy)]
pub struct HybridLegs {
    /// Depth each leg (and each shard's fused list) is fetched to.
    pub leg_k: u32,
    /// Vector-leg RRF weight.
    pub vector_weight: f32,
    /// BM25-leg RRF weight.
    pub bm25_weight: f32,
    /// RRF constant.
    pub rrf_k: f64,
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

        let result = self.fanout_search(&request_id, &req.vector, req.k).await?;
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
        let hits = self
            .fanout_bm25(&req.text, req.k, req.analysis.as_ref())
            .await?;
        Ok(Response::new(Bm25SearchResponse { hits }))
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
        let legs = HybridLegs {
            leg_k: if options.leg_k == 0 {
                req.k.max(rrf_k as u32)
            } else {
                options.leg_k.max(req.k)
            },
            vector_weight: options.vector_weight,
            bm25_weight: options.bm25_weight,
            rrf_k,
        };
        let hits = self
            .fanout_hybrid(
                &request_id,
                &req.text,
                &req.vector,
                req.k,
                req.analysis.as_ref(),
                legs,
            )
            .await?;
        Ok(Response::new(HybridSearchResponse { request_id, hits }))
    }
}

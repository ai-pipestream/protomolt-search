//! Coordinator side: client-facing [`SearchService`] that fans queries out
//! to shard nodes, aggregates their floors mid-scan, and merges results.

use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::merge::{merge_topk, FloorTracker};
use crate::pb::node_service_client::NodeServiceClient;
use crate::pb::search_service_server::SearchService;
use crate::pb::{
    search_shard_request, search_shard_response, FloorUpdate, ScoredHit, SearchRequest,
    SearchResponse, SearchShardDone, SearchShardRequest, SearchShardResponse, ShardScanStats,
    StartShardSearch,
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
}

impl CoordinatorServiceImpl {
    /// A coordinator over the given shard nodes (fan-out order = shard
    /// index for merge tie-breaks).
    pub fn new(node_addrs: Vec<String>) -> Self {
        Self { node_addrs }
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
                .map_err(|e| Status::unavailable(format!("connect {addr}: {e}")))?;

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
}

//! Shard-owner side: serves [`NodeService`] over one turbovec index.

use std::sync::Arc;

use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use turbovec::TurboQuantIndex;

use crate::chunked::{chunked_topk, DEFAULT_CHUNK_BLOCKS};
use crate::pb::node_service_server::NodeService;
use crate::pb::{
    search_shard_request, search_shard_response, FloorUpdate, GetCalibrationRequest,
    GetCalibrationResponse, ScoredHit, SearchShardDone, SearchShardRequest, SearchShardResponse,
    ShardScanStats, StartShardSearch,
};

/// How a node scans and whether it participates in floor sharing.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    /// Added to every local slot to produce the global vector id reported
    /// in [`SearchShardDone`]. Shards must have disjoint ranges.
    pub slot_offset: u64,
    /// Chunk size in SIMD blocks for the scan (see [`chunked_topk`]).
    pub chunk_blocks: usize,
    /// When false, the node still scans in chunks but ignores coordinator
    /// floor updates and does not publish its own floor — the
    /// "sharing disabled" baseline for A/B benchmarking.
    pub share_floors: bool,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            slot_offset: 0,
            chunk_blocks: DEFAULT_CHUNK_BLOCKS,
            share_floors: true,
        }
    }
}

/// The shard-owner gRPC service. Cheap to clone (the index is shared).
#[derive(Clone)]
pub struct NodeServiceImpl {
    index: Arc<TurboQuantIndex>,
    config: NodeConfig,
}

impl NodeServiceImpl {
    /// Wrap a loaded/built index in a node service.
    pub fn new(index: Arc<TurboQuantIndex>, config: NodeConfig) -> Self {
        Self { index, config }
    }

    /// Validate an incoming `StartShardSearch` against the index shape.
    /// turbovec panics on wrong-dim or non-finite queries; the service
    /// turns both into `INVALID_ARGUMENT` before the scan starts.
    // Status is the natural error type for a gRPC handler; boxing it to
    // satisfy result_large_err would just add an allocation.
    #[allow(clippy::result_large_err)]
    fn validate_start(index: &TurboQuantIndex, start: &StartShardSearch) -> Result<(), Status> {
        let dim = index
            .dim_opt()
            .ok_or_else(|| Status::failed_precondition("index has no vectors"))?;
        if start.vector.len() != dim {
            return Err(Status::invalid_argument(format!(
                "query vector has dim {}, index expects {dim}",
                start.vector.len()
            )));
        }
        if let Some((_, coord, value)) = turbovec::first_invalid_coord(&start.vector, dim) {
            return Err(Status::invalid_argument(format!(
                "query coordinate {coord} is invalid: {value}"
            )));
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl NodeService for NodeServiceImpl {
    type SearchShardStream = ReceiverStream<Result<SearchShardResponse, Status>>;

    async fn search_shard(
        &self,
        request: Request<Streaming<SearchShardRequest>>,
    ) -> Result<Response<Self::SearchShardStream>, Status> {
        let mut inbound = request.into_inner();
        let (tx, rx) = mpsc::channel::<Result<SearchShardResponse, Status>>(64);
        let index = self.index.clone();
        let config = self.config.clone();

        tokio::spawn(async move {
            // Protocol: the first message must be Start.
            let start = match inbound.message().await {
                Ok(Some(SearchShardRequest {
                    payload: Some(search_shard_request::Payload::Start(start)),
                })) => start,
                Ok(_) => {
                    let _ = tx
                        .send(Err(Status::invalid_argument(
                            "first SearchShardRequest must be StartShardSearch",
                        )))
                        .await;
                    return;
                }
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    return;
                }
            };
            if let Err(e) = Self::validate_start(&index, &start) {
                let _ = tx.send(Err(e)).await;
                return;
            }

            // Floor updates arrive on the same stream; a pump task folds
            // them into a watch cell the blocking scan polls between chunks.
            // Updates are monotone maxes, so only raises are stored.
            let (floor_tx, floor_rx) = watch::channel(f32::NEG_INFINITY);
            tokio::spawn(async move {
                loop {
                    match inbound.message().await {
                        Ok(Some(SearchShardRequest {
                            payload: Some(search_shard_request::Payload::FloorUpdate(u)),
                        })) => {
                            floor_tx.send_if_modified(|cur| {
                                if !u.floor.is_nan() && u.floor > *cur {
                                    *cur = u.floor;
                                    true
                                } else {
                                    false
                                }
                            });
                        }
                        // Duplicate Start or empty payload: ignore.
                        Ok(Some(_)) => {}
                        // Client closed (end of updates or cancellation) or
                        // the stream broke: stop pumping; the scan finishes
                        // on its own either way.
                        Ok(None) | Err(_) => break,
                    }
                }
            });

            let share = config.share_floors;
            let chunk_blocks = config.chunk_blocks;
            let slot_offset = config.slot_offset;
            let scan_tx = tx.clone();
            let scan = tokio::task::spawn_blocking(move || {
                let mut external_floor = || {
                    if share {
                        let f = *floor_rx.borrow();
                        (f != f32::NEG_INFINITY).then_some(f)
                    } else {
                        None
                    }
                };
                // Publish only raises, and never block the scan on a full
                // channel: intermediate floors are disposable (they are
                // monotone, so the next chunk's publish supersedes any
                // dropped one). The terminal Done is sent with `.await`
                // below and cannot be dropped.
                let mut last_published = f32::NEG_INFINITY;
                let mut publish_floor = |floor: f32| {
                    if share && floor > last_published {
                        last_published = floor;
                        let _ = scan_tx.try_send(Ok(SearchShardResponse {
                            payload: Some(search_shard_response::Payload::FloorUpdate(
                                FloorUpdate { floor },
                            )),
                        }));
                    }
                };
                chunked_topk(
                    &index,
                    &start.vector,
                    start.k as usize,
                    chunk_blocks,
                    &mut external_floor,
                    &mut publish_floor,
                )
            });

            match scan.await {
                Ok((hits, stats)) => {
                    let done = SearchShardDone {
                        hits: hits
                            .into_iter()
                            .map(|h| ScoredHit {
                                vector_id: slot_offset + u64::from(h.slot),
                                score: h.score,
                            })
                            .collect(),
                        stats: Some(ShardScanStats {
                            chunk_calls: stats.chunk_calls,
                            candidates_collected: stats.candidates_collected,
                            floors_published: stats.floors_published,
                            floor_updates_applied: stats.floor_updates_applied,
                        }),
                    };
                    let _ = tx
                        .send(Ok(SearchShardResponse {
                            payload: Some(search_shard_response::Payload::Done(done)),
                        }))
                        .await;
                }
                Err(e) => {
                    let _ = tx
                        .send(Err(Status::internal(format!("scan task failed: {e}"))))
                        .await;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn get_calibration(
        &self,
        _request: Request<GetCalibrationRequest>,
    ) -> Result<Response<GetCalibrationResponse>, Status> {
        let (shift, scale) = self
            .index
            .calibration()
            .map(|(s, c)| (s.to_vec(), c.to_vec()))
            .unwrap_or_default();
        Ok(Response::new(GetCalibrationResponse {
            dim: self.index.dim_opt().unwrap_or(0) as u32,
            bit_width: self.index.bit_width() as u32,
            num_vectors: self.index.len() as u64,
            shift,
            scale,
        }))
    }
}

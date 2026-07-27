//! Shard-owner side: serves [`NodeService`] over one turbovec index.
//!
//! The shard is a small state machine behind a write lock:
//!
//! ```text
//! empty (no index) ──SetCalibration──▶ seeded empty index ──AddVectors──▶ live index
//!       │                                    │
//!       └──AddVectors(dim=..)──▶ unseeded index (calibration fitted from first batch)
//! ```
//!
//! Calibration locks for the index's lifetime (turbovec's own rule):
//! `SetCalibration` is only ever accepted on an empty shard. Adds hold the
//! write lock on the blocking pool; searches hold the read lock for the
//! duration of their chunked scan, so a search never observes a
//! half-applied batch.

use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use turbovec::TurboQuantIndex;

use crate::chunked::{chunked_topk, DEFAULT_CHUNK_BLOCKS};
use crate::pb::node_service_server::{NodeService, NodeServiceServer};
use crate::pb::{
    search_shard_request, search_shard_response, AddVectorsRequest, AddVectorsResponse,
    FloorUpdate, FlushRequest, FlushResponse, GetCalibrationRequest, GetCalibrationResponse,
    ScoredHit, SearchShardDone, SearchShardRequest, SearchShardResponse, SetCalibrationRequest,
    SetCalibrationResponse, ShardScanStats, StartShardSearch,
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
    /// Bit width used when `AddVectors` constructs an index from scratch
    /// (no loaded index, no seeded calibration).
    pub bit_width: usize,
    /// Persistence target for `Flush` / save-on-shutdown. `None` makes the
    /// shard purely in-memory (flush is a no-op).
    pub index_path: Option<PathBuf>,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            slot_offset: 0,
            chunk_blocks: DEFAULT_CHUNK_BLOCKS,
            share_floors: true,
            bit_width: 4,
            index_path: None,
        }
    }
}

/// The shard-owner gRPC service. Cheap to clone (state is shared).
#[derive(Clone)]
pub struct NodeServiceImpl {
    /// `None` until the shard has an index (loaded, seeded via
    /// `SetCalibration`, or constructed by the first `AddVectors` batch).
    state: Arc<RwLock<Option<TurboQuantIndex>>>,
    config: NodeConfig,
}

impl NodeServiceImpl {
    /// Wrap an optional preloaded index in a node service.
    pub fn new(index: Option<TurboQuantIndex>, config: NodeConfig) -> Self {
        Self {
            state: Arc::new(RwLock::new(index)),
            config,
        }
    }

    /// Build the tonic server for this service with explicit message size
    /// limits (see [`crate::MAX_MESSAGE_BYTES`]). tonic's 4 MiB default
    /// decoding cap is comfortably above even k=10000 shard responses
    /// (~160 KiB), but the limit is set explicitly so it never silently
    /// depends on a library default. NOTE: the cap also bounds AddVectors
    /// batch messages; clients should keep batches well under it.
    pub fn into_server(self, max_message_bytes: usize) -> NodeServiceServer<Self> {
        NodeServiceServer::new(self)
            .max_decoding_message_size(max_message_bytes)
            .max_encoding_message_size(max_message_bytes)
    }

    /// Validate an incoming `StartShardSearch` against the index shape.
    /// turbovec panics on wrong-dim or non-finite queries; the service
    /// turns both into `INVALID_ARGUMENT` before the scan starts.
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

    /// Persist the index to its configured path, if any. Shared by the
    /// `Flush` RPC and save-on-shutdown in the binary.
    pub fn flush_index(&self) -> Result<FlushResponse, Status> {
        let guard = self.state.read().expect("shard state lock poisoned");
        let Some(index) = guard.as_ref() else {
            return Ok(FlushResponse {
                path: String::new(),
                num_vectors: 0,
                written: false,
            });
        };
        let Some(path) = self.config.index_path.clone() else {
            return Ok(FlushResponse {
                path: String::new(),
                num_vectors: index.len() as u64,
                written: false,
            });
        };
        index
            .write(&path)
            .map_err(|e| Status::internal(format!("write {}: {e}", path.display())))?;
        Ok(FlushResponse {
            path: path.display().to_string(),
            num_vectors: index.len() as u64,
            written: true,
        })
    }

    /// Apply one `SetCalibration`: lock the calibration on an empty shard.
    fn apply_calibration(&self, req: &SetCalibrationRequest) -> Result<bool, Status> {
        let dim = req.dim as usize;
        let bit_width = req.bit_width as usize;
        let build = || {
            TurboQuantIndex::new_with_calibration(dim, bit_width, &req.shift, &req.scale)
                .map_err(|e| Status::invalid_argument(format!("invalid calibration: {e}")))
        };
        let mut guard = self.state.write().expect("shard state lock poisoned");
        match guard.as_ref() {
            Some(index) if !index.is_empty() => Err(Status::failed_precondition(format!(
                "shard holds {} vectors; calibration is locked for the index lifetime",
                index.len()
            ))),
            Some(index) => {
                let same = index.dim_opt() == Some(dim)
                    && index.bit_width() == bit_width
                    && index.calibration().is_some_and(|(s, c)| {
                        s == req.shift.as_slice() && c == req.scale.as_slice()
                    });
                if same {
                    return Ok(true); // idempotent retry
                }
                if index.calibration().is_some() {
                    return Err(Status::already_exists(
                        "a different calibration is already locked on this shard",
                    ));
                }
                // Empty, unseeded index: replace with the seeded one.
                *guard = Some(build()?);
                Ok(false)
            }
            None => {
                *guard = Some(build()?);
                Ok(false)
            }
        }
    }

    /// Apply one ingested batch under the write lock. Returns
    /// `(added, global id of the batch's first vector)`.
    fn apply_batch(&self, batch: AddVectorsRequest) -> Result<(u64, u64), Status> {
        if batch.vectors.is_empty() {
            return Ok((0, 0));
        }
        let mut guard = self.state.write().expect("shard state lock poisoned");
        let known_dim = guard.as_ref().and_then(|i| i.dim_opt());
        let dim = if batch.dim != 0 {
            let d = batch.dim as usize;
            if let Some(known) = known_dim {
                if known != d {
                    return Err(Status::invalid_argument(format!(
                        "batch dim {d} does not match shard dim {known}"
                    )));
                }
            }
            d
        } else {
            known_dim.ok_or_else(|| {
                Status::failed_precondition(
                    "shard has no index or calibration yet; set calibration first or pass dim",
                )
            })?
        };
        if !batch.vectors.len().is_multiple_of(dim) {
            return Err(Status::invalid_argument(format!(
                "batch of {} floats is not a multiple of dim {dim}",
                batch.vectors.len()
            )));
        }
        if let Some((vi, ci, v)) = turbovec::first_invalid_coord(&batch.vectors, dim) {
            return Err(Status::invalid_argument(format!(
                "invalid input value at vector {vi}, coord {ci}: {v}"
            )));
        }
        let index = match guard.as_mut() {
            Some(index) => index,
            None => {
                // From-scratch, unseeded: turbovec fits calibration from
                // this first batch. Seeded deployment is the SetCalibration
                // path; this exists for single-shard convenience.
                *guard = Some(
                    TurboQuantIndex::new(dim, self.config.bit_width)
                        .map_err(|e| Status::invalid_argument(format!("{e}")))?,
                );
                guard.as_mut().expect("just constructed")
            }
        };
        let first_id = self.config.slot_offset + index.len() as u64;
        index
            .add_2d(&batch.vectors, dim)
            .map_err(|e| Status::invalid_argument(format!("{e}")))?;
        Ok(((batch.vectors.len() / dim) as u64, first_id))
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
        let state = self.state.clone();
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
                // The read guard is held for the whole chunked scan: adds
                // (write lock) never interleave with a scan, so a search
                // sees one consistent index snapshot.
                let guard = state.read().expect("shard state lock poisoned");
                let index = guard.as_ref().ok_or_else(|| {
                    Status::failed_precondition(
                        "shard has no index yet (set calibration or add vectors)",
                    )
                })?;
                Self::validate_start(index, &start)?;
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
                let mut external_floor = || {
                    if share {
                        let f = *floor_rx.borrow();
                        (f != f32::NEG_INFINITY).then_some(f)
                    } else {
                        None
                    }
                };
                Ok(chunked_topk(
                    index,
                    &start.vector,
                    start.k as usize,
                    chunk_blocks,
                    &mut external_floor,
                    &mut publish_floor,
                ))
            });

            match scan.await {
                Ok(Ok((hits, stats))) => {
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
                Ok(Err(e)) => {
                    let _ = tx.send(Err(e)).await;
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
        let guard = self.state.read().expect("shard state lock poisoned");
        let (dim, bit_width, num_vectors, shift, scale) = match guard.as_ref() {
            Some(index) => {
                let (shift, scale) = index
                    .calibration()
                    .map(|(s, c)| (s.to_vec(), c.to_vec()))
                    .unwrap_or_default();
                (
                    index.dim_opt().unwrap_or(0) as u32,
                    index.bit_width() as u32,
                    index.len() as u64,
                    shift,
                    scale,
                )
            }
            None => (0, 0, 0, Vec::new(), Vec::new()),
        };
        Ok(Response::new(GetCalibrationResponse {
            dim,
            bit_width,
            num_vectors,
            shift,
            scale,
        }))
    }

    async fn set_calibration(
        &self,
        request: Request<SetCalibrationRequest>,
    ) -> Result<Response<SetCalibrationResponse>, Status> {
        let already_seeded = self.apply_calibration(&request.into_inner())?;
        Ok(Response::new(SetCalibrationResponse { already_seeded }))
    }

    async fn add_vectors(
        &self,
        request: Request<Streaming<AddVectorsRequest>>,
    ) -> Result<Response<AddVectorsResponse>, Status> {
        let mut inbound = request.into_inner();
        let mut added = 0u64;
        let mut first_id = 0u64;
        while let Some(batch) = inbound.message().await? {
            let service = self.clone();
            let (batch_added, batch_first_id) =
                tokio::task::spawn_blocking(move || service.apply_batch(batch))
                    .await
                    .map_err(|e| Status::internal(format!("add task failed: {e}")))??;
            if added == 0 && batch_added > 0 {
                first_id = batch_first_id;
            }
            added += batch_added;
        }
        let total = self
            .state
            .read()
            .expect("shard state lock poisoned")
            .as_ref()
            .map_or(0, |i| i.len() as u64);
        Ok(Response::new(AddVectorsResponse {
            added,
            total,
            first_id,
        }))
    }

    async fn flush(
        &self,
        _request: Request<FlushRequest>,
    ) -> Result<Response<FlushResponse>, Status> {
        let service = self.clone();
        tokio::task::spawn_blocking(move || service.flush_index())
            .await
            .map_err(|e| Status::internal(format!("flush task failed: {e}")))?
            .map(Response::new)
    }
}

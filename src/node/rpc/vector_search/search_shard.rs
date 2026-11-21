//! Bidirectional chunked `SearchShard` RPC handler.

use crate::node::*;

use super::exact_scan::run_exact_fp32_shard_scan;
use super::{
    build_search_shard_done, collect_rerank_candidates, validate_scan_filters, DonePayload,
};

pub(super) struct SearchShardExec {
    pub(super) state: Arc<RwLock<ShardState>>,
    pub(super) start: StartShardSearch,
    pub(super) geo_regions: Vec<crate::geo::GeoRegion>,
    pub(super) ready_tx: mpsc::Sender<Result<SearchShardResponse, Status>>,
    pub(super) external: Box<dyn FnMut() -> Option<f32> + Send>,
    pub(super) publish: Box<dyn FnMut(f32) -> bool + Send>,
    pub(super) chunk_blocks: usize,
    pub(super) slot_offset: u64,
    pub(super) segment_pruning: bool,
}

fn spawn_floor_pump(mut inbound: Streaming<SearchShardRequest>) -> watch::Receiver<f32> {
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
                Ok(Some(_)) => {}
                Ok(None) | Err(_) => break,
            }
        }
    });
    floor_rx
}

type FloorCallbacks = (
    Box<dyn FnMut() -> Option<f32> + Send>,
    Box<dyn FnMut(f32) -> bool + Send>,
);

fn make_floor_callbacks(
    knobs: &crate::diagnostics::Knobs,
    floor_rx: watch::Receiver<f32>,
    scan_tx: mpsc::Sender<Result<SearchShardResponse, Status>>,
) -> FloorCallbacks {
    let share = knobs.share_floors();
    let floor_delta = knobs.floor_delta();
    let warmup = knobs.floor_warmup_chunks();
    let floor_min_interval_ms = knobs.floor_min_interval_ms();
    let min_interval = (floor_min_interval_ms > 0)
        .then(|| std::time::Duration::from_millis(floor_min_interval_ms));
    let mut last_published = f32::NEG_INFINITY;
    let mut offers = 0u32;
    let mut last_at: Option<std::time::Instant> = None;

    let publish = Box::new(move |floor: f32| -> bool {
        if !share {
            return false;
        }
        offers += 1;
        if offers <= warmup || floor <= last_published + floor_delta {
            return false;
        }
        if let (Some(interval), Some(at)) = (min_interval, last_at) {
            if at.elapsed() < interval {
                return false;
            }
        }
        last_published = floor;
        last_at = Some(std::time::Instant::now());
        let _ = scan_tx.try_send(Ok(SearchShardResponse {
            payload: Some(search_shard_response::Payload::FloorUpdate(FloorUpdate {
                floor,
            })),
        }));
        true
    });

    let external = Box::new(move || {
        if share {
            let f = *floor_rx.borrow();
            (f != f32::NEG_INFINITY).then_some(f)
        } else {
            None
        }
    });
    (external, publish)
}

pub(super) struct PreparedExecFilters {
    pub(super) allow: Option<crate::boolean_bits::Bits>,
    pub(super) prune: crate::segment_prune::PruneStats,
    pub(super) geo_columns_known: Vec<bool>,
    pub(super) filter_columns_known: Vec<bool>,
}

pub(super) fn prepare_exec_filters(
    guard: &ShardState,
    rows: usize,
    exec: &SearchShardExec,
) -> Result<PreparedExecFilters, Status> {
    let ((_, allow, prune), receipt) = guard.vector_scan_filters(
        rows,
        &exec.start.geo_filters,
        &exec.geo_regions,
        exec.start.filter.as_ref(),
        exec.start.read_context.as_ref(),
        exec.segment_pruning,
    )?;
    let (geo_columns_known, filter_columns_known) = filter_known_flags(
        guard.bm25.as_ref(),
        &exec.start.geo_filters,
        exec.start.filter.as_ref(),
    );
    if let Some(receipt) = receipt {
        exec.ready_tx
            .blocking_send(Ok(SearchShardResponse {
                payload: Some(search_shard_response::Payload::ReadReady(receipt)),
            }))
            .map_err(|_| Status::cancelled("scoped scan response closed"))?;
    }
    Ok(PreparedExecFilters {
        allow,
        prune,
        geo_columns_known,
        filter_columns_known,
    })
}

fn run_collapsed_blocking(mut exec: SearchShardExec) -> Result<DonePayload, Status> {
    let n = {
        let guard = read_shard(&exec.state);
        let index = guard.index.as_ref().ok_or_else(|| {
            Status::failed_precondition("shard has no index yet (set calibration or add vectors)")
        })?;
        NodeServiceImpl::validate_start(index, &exec.start)?;
        index.len()
    };
    let parents = NodeServiceImpl::parent_map(&exec.state, exec.slot_offset, n)?;
    let guard = read_shard(&exec.state);
    let index = guard
        .index
        .as_ref()
        .ok_or_else(|| Status::failed_precondition("shard index disappeared mid-setup"))?;
    guard.check_stats_epoch(parents.claim.epoch, &parents.claim.incarnation())?;
    if index.len() != parents.rows.len() {
        return Err(Status::aborted("shard grew between setup and scan; retry"));
    }
    let filters = prepare_exec_filters(&guard, index.len(), &exec)?;
    let (hits, mut stats) = chunked_topk_collapsed(
        crate::chunked::ChunkedScan {
            index,
            query: &exec.start.vector,
            k: exec.start.k as usize,
            chunk_blocks: exec.chunk_blocks,
            allow: filters.allow.as_ref().map(crate::vector::Mask::Bits),
        },
        &parents.rows,
        &mut *exec.external,
        &mut *exec.publish,
    );
    stats.segments_total = filters.prune.segments_total;
    stats.segments_skipped = filters.prune.segments_skipped;
    crate::metrics::record_scan(&stats);
    observe_scan(stats.bytes_scanned, stats.scan_nanos);
    let identities = guard
        .bm25
        .as_ref()
        .map(Bm25Shard::identity_snapshot)
        .transpose()
        .map_err(Status::internal)?
        .unwrap_or_default();
    let rerank_candidates = collect_rerank_candidates(
        &guard,
        exec.start.include_rerank_candidate,
        hits.iter().map(|h| h.slot),
    );
    Ok(DonePayload {
        hits: hits
            .into_iter()
            .map(|h| (h.slot, h.score, h.parent))
            .collect(),
        stats,
        identities,
        rerank_candidates,
        geo_columns_known: filters.geo_columns_known,
        filter_columns_known: filters.filter_columns_known,
    })
}

fn run_solo_blocking(mut exec: SearchShardExec) -> Result<ScanOutcome, Status> {
    let guard = read_shard(&exec.state);
    let index = guard.index.as_ref().ok_or_else(|| {
        Status::failed_precondition("shard has no index yet (set calibration or add vectors)")
    })?;
    NodeServiceImpl::validate_start(index, &exec.start)?;
    let filters = prepare_exec_filters(&guard, index.len(), &exec)?;
    let patience = check_patience_options(
        index,
        exec.start.patience_saturation_ppm,
        exec.start.patience_hops,
    )?;
    let floor_prune = check_floor_prune_options(
        index,
        exec.start.floor_prune,
        exec.start.floor_k,
        exec.start.k,
        exec.start.floor_share_ppm,
        exec.start.floor_greediness_ppm,
        exec.start.floor_clamp_min,
    )?;
    let solo_queries = [BatchQuery {
        vector: &exec.start.vector,
        k: exec.start.k as usize,
        keep_ties: exec.start.tie_complete,
        allow: filters.allow.as_ref().map(crate::vector::Mask::Bits),
        enumerate_max_rows: exec.start.enumerate_max_rows.map(|r| r as usize),
        controls: selection_controls(
            index,
            exec.start.filter_strategy,
            exec.start.enumerate_all,
            exec.start.staged_selection,
        )?,
        patience,
        floor_prune,
    }];
    let (hits, mut stats) = crate::chunked::chunked_topk_batch(
        index,
        &solo_queries,
        exec.chunk_blocks,
        &mut |_| (exec.external)(),
        &mut |_, f| (exec.publish)(f),
    )
    .pop()
    .expect("one query in, one result out");
    stats.segments_total = filters.prune.segments_total;
    stats.segments_skipped = filters.prune.segments_skipped;
    crate::metrics::record_scan(&stats);
    observe_scan(stats.bytes_scanned, stats.scan_nanos);
    let rerank_candidates = collect_rerank_candidates(
        &guard,
        exec.start.include_rerank_candidate,
        hits.iter().map(|h| h.slot),
    );
    Ok(ScanOutcome {
        hits,
        identities: guard
            .bm25
            .as_ref()
            .map(Bm25Shard::identity_snapshot)
            .transpose()
            .map_err(Status::internal)?
            .unwrap_or_default(),
        rerank_candidates,
        stats,
        geo_columns_known: filters.geo_columns_known,
        filter_columns_known: filters.filter_columns_known,
    })
}

async fn enqueue_coalesced_scan(
    jobs: mpsc::Sender<ScanJob>,
    exec: SearchShardExec,
) -> Result<ScanOutcome, Status> {
    {
        let guard = read_shard(&exec.state);
        let index = guard.index.as_ref().ok_or_else(|| {
            Status::failed_precondition("shard has no index yet (set calibration or add vectors)")
        })?;
        NodeServiceImpl::validate_start(index, &exec.start)?;
    }
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    let job = ScanJob {
        read_context: exec.start.read_context,
        ready_tx: exec.ready_tx,
        vector: exec.start.vector,
        k: exec.start.k as usize,
        tie_complete: exec.start.tie_complete,
        include_rerank_candidate: exec.start.include_rerank_candidate,
        geo_filters: exec.start.geo_filters,
        geo_regions: exec.geo_regions,
        filter: exec.start.filter,
        enumerate_max_rows: exec.start.enumerate_max_rows,
        filter_strategy: exec.start.filter_strategy,
        enumerate_all: exec.start.enumerate_all,
        staged_selection: exec.start.staged_selection,
        patience_saturation_ppm: exec.start.patience_saturation_ppm,
        patience_hops: exec.start.patience_hops,
        floor_prune: exec.start.floor_prune,
        floor_k: exec.start.floor_k,
        floor_share_ppm: exec.start.floor_share_ppm,
        floor_greediness_ppm: exec.start.floor_greediness_ppm,
        floor_clamp_min: exec.start.floor_clamp_min,
        external: exec.external,
        publish: exec.publish,
        done: done_tx,
    };
    jobs.send(job)
        .await
        .map_err(|_| Status::internal("scan scheduler unavailable"))?;
    done_rx
        .await
        .map_err(|_| Status::internal("scan batch dropped before finishing"))?
}

async fn send_done_or_err(
    tx: &mpsc::Sender<Result<SearchShardResponse, Status>>,
    slot_offset: u64,
    outcome: Result<DonePayload, Status>,
) {
    match outcome {
        Ok(payload) => {
            let done = build_search_shard_done(slot_offset, payload);
            let _ = tx
                .send(Ok(SearchShardResponse {
                    payload: Some(search_shard_response::Payload::Done(done)),
                }))
                .await;
        }
        Err(e) => {
            let _ = tx.send(Err(e)).await;
        }
    }
}

struct ShardDriver {
    state: Arc<RwLock<ShardState>>,
    config: NodeConfig,
    knobs: Arc<crate::diagnostics::Knobs>,
    scan_queue: Option<mpsc::Sender<ScanJob>>,
    rerank_slots: Arc<tokio::sync::Semaphore>,
}

async fn run_exact_fp32_driver(
    drv: &ShardDriver,
    exec: SearchShardExec,
) -> Result<DonePayload, Status> {
    let parallel = resolved_rerank_parallel(drv.config.rerank_parallel);
    let permits = drv
        .rerank_slots
        .clone()
        .acquire_many_owned(parallel as u32)
        .await
        .map_err(|_| Status::unavailable("exact rerank worker budget closed"))?;
    tokio::task::spawn_blocking(move || {
        let _permits = permits;
        run_exact_fp32_shard_scan(exec, parallel)
    })
    .await
    .map_err(|e| Status::internal(format!("exact FP32 scan task failed: {e}")))?
}

async fn recv_start_shard_search(
    inbound: &mut Streaming<SearchShardRequest>,
) -> Result<StartShardSearch, Status> {
    match inbound.message().await? {
        Some(SearchShardRequest {
            payload: Some(search_shard_request::Payload::Start(start)),
        }) => Ok(start),
        _ => Err(Status::invalid_argument(
            "first SearchShardRequest must be StartShardSearch",
        )),
    }
}

pub(crate) fn collapse_conflict(start: &StartShardSearch) -> Option<&'static str> {
    if !start.collapse_parents {
        None
    } else if start.exact_fp32_scan {
        Some("collapse_parents and exact_fp32_scan are mutually exclusive")
    } else if start.tie_complete {
        Some("collapse_parents and tie_complete are mutually exclusive")
    } else if start.staged_selection {
        Some("collapse_parents and staged_selection are mutually exclusive")
    } else {
        None
    }
}

async fn drive_search_shard(
    mut inbound: Streaming<SearchShardRequest>,
    tx: mpsc::Sender<Result<SearchShardResponse, Status>>,
    drv: ShardDriver,
) {
    let start = match recv_start_shard_search(&mut inbound).await {
        Ok(start) => start,
        Err(e) => {
            let _ = tx.send(Err(e)).await;
            return;
        }
    };
    let geo_regions = match validate_scan_filters(&start.geo_filters, start.filter.as_ref()) {
        Ok(regions) => regions,
        Err(e) => {
            let _ = tx.send(Err(e)).await;
            return;
        }
    };
    let floor_rx = spawn_floor_pump(inbound);
    let (external, publish) = make_floor_callbacks(&drv.knobs, floor_rx, tx.clone());
    let slot_offset = drv.config.slot_offset;
    if let Some(msg) = collapse_conflict(&start) {
        let _ = tx.send(Err(Status::invalid_argument(msg))).await;
        return;
    }
    let collapse_parents = start.collapse_parents;
    let exact_fp32 = start.exact_fp32_scan;
    let exec = SearchShardExec {
        state: Arc::clone(&drv.state),
        start,
        geo_regions,
        ready_tx: tx.clone(),
        external,
        publish,
        chunk_blocks: drv.config.chunk_blocks,
        slot_offset,
        segment_pruning: drv.knobs.segment_pruning(),
    };
    if exact_fp32 {
        let outcome = run_exact_fp32_driver(&drv, exec).await;
        send_done_or_err(&tx, slot_offset, outcome).await;
        return;
    }
    if collapse_parents {
        let outcome = tokio::task::spawn_blocking(move || run_collapsed_blocking(exec))
            .await
            .map_err(|e| Status::internal(format!("collapse scan task failed: {e}")))
            .and_then(|r| r);
        send_done_or_err(&tx, slot_offset, outcome).await;
        return;
    }
    let outcome = match drv.scan_queue {
        Some(jobs) => enqueue_coalesced_scan(jobs, exec).await,
        None => tokio::task::spawn_blocking(move || run_solo_blocking(exec))
            .await
            .map_err(|e| Status::internal(format!("scan task failed: {e}")))
            .and_then(|r| r),
    };
    let done = outcome.map(|o| DonePayload {
        hits: o.hits.into_iter().map(|h| (h.slot, h.score, 0)).collect(),
        stats: o.stats,
        identities: o.identities,
        rerank_candidates: o.rerank_candidates,
        geo_columns_known: o.geo_columns_known,
        filter_columns_known: o.filter_columns_known,
    });
    send_done_or_err(&tx, slot_offset, done).await;
}

impl NodeServiceImpl {
    pub(in crate::node::rpc) async fn handle_search_shard(
        &self,
        request: Request<Streaming<SearchShardRequest>>,
    ) -> Result<Response<<Self as NodeService>::SearchShardStream>, Status> {
        crate::metrics::timed_stream(Route::SearchShard, request, |request| async move {
            let inbound = request.into_inner();
            let (tx, rx) = mpsc::channel::<Result<SearchShardResponse, Status>>(64);
            let drv = ShardDriver {
                state: self.state.clone(),
                config: self.config.clone(),
                knobs: Arc::clone(&self.knobs),
                scan_queue: self.config.coalesce.then(|| self.scan_queue()),
                rerank_slots: Arc::clone(&self.rerank_slots),
            };
            tokio::spawn(drive_search_shard(inbound, tx, drv));
            Ok(Response::new(ReceiverStream::new(rx)))
        })
        .await
    }
}

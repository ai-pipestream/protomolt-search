//! Streaming `StreamSearch` RPC coordination, request pumping, and identity resolution.

use crate::node::*;
use crate::pb::{StartStreamSearch, StreamIdentityLimits};

use super::exact_scan::run_exact_fp32_stream_scan;
use super::stream_scan::{run_stream_scan_blocking, StreamScanCtx, StreamScanResult};
use super::validate_scan_filters;

fn spawn_stream_pump(
    mut inbound: Streaming<StreamSearchRequest>,
    pump_signals: Arc<StreamSignals>,
    pump_ready: Arc<AtomicBool>,
    identity_enabled: bool,
) -> tokio::task::JoinHandle<Result<Option<crate::pb::ResolveStreamIdentities>, Status>> {
    tokio::spawn(async move {
        loop {
            match inbound.message().await {
                Ok(Some(StreamSearchRequest {
                    payload: Some(stream_search_request::Payload::FloorUpdate(u)),
                })) => raise_floor_cell(&pump_signals.floor, u.floor),
                Ok(Some(StreamSearchRequest {
                    payload: Some(stream_search_request::Payload::Stop(_)),
                })) => {
                    pump_signals.cancelled.store(true, AtomicOrdering::Release);
                    return Ok(None);
                }
                Ok(Some(StreamSearchRequest {
                    payload: Some(stream_search_request::Payload::Finish(f)),
                })) => {
                    pump_signals.request_finish(f.deadline);
                }
                Ok(Some(StreamSearchRequest {
                    payload: Some(stream_search_request::Payload::ResolveIdentities(selection)),
                })) => {
                    if !identity_enabled || !pump_ready.load(AtomicOrdering::Acquire) {
                        pump_signals.cancelled.store(true, AtomicOrdering::Release);
                        return Err(Status::failed_precondition(
                            "ResolveStreamIdentities requires this stream's IdentityReady",
                        ));
                    }
                    return Ok(Some(selection));
                }
                Ok(Some(_)) => {}
                Ok(None) | Err(_) => {
                    if identity_enabled {
                        pump_signals.cancelled.store(true, AtomicOrdering::Release);
                        return Err(Status::cancelled("identity request stream closed"));
                    }
                    return Ok(None);
                }
            }
        }
    })
}

struct IdentityExchangeCtx {
    limits: StreamIdentityLimits,
    deadline: std::time::Instant,
    identities: crate::query_identity::ScanIdentities,
}

async fn run_identity_step(
    tx: &mpsc::Sender<Result<StreamSearchResponse, Status>>,
    pump: &mut tokio::task::JoinHandle<Result<Option<crate::pb::ResolveStreamIdentities>, Status>>,
    signals: &Arc<StreamSignals>,
    identity_ready: &AtomicBool,
    ctx: IdentityExchangeCtx,
    summary: &mut StreamSearchSummary,
) -> Result<(), Status> {
    if !summary.completed {
        if pump.is_finished() {
            pump.await
                .map_err(|e| Status::internal(format!("identity request task failed: {e}")))??;
        }
        return Ok(());
    }
    identity_ready.store(true, AtomicOrdering::Release);
    tx.send(Ok(StreamSearchResponse {
        payload: Some(stream_search_response::Payload::IdentityReady(
            crate::pb::StreamIdentityReady {
                scan: Some(summary.clone()),
                range: ctx.identities.range(),
            },
        )),
    }))
    .await
    .map_err(|_| Status::cancelled("identity response stream closed"))?;
    let selection = pump
        .await
        .map_err(|e| Status::internal(format!("identity request task failed: {e}")))??;
    match selection {
        Some(selection) => {
            let limits = ctx.limits;
            let deadline = ctx.deadline;
            let identities = ctx.identities;
            let cancelled = Arc::clone(signals);
            let response = tokio::task::spawn_blocking(move || {
                identities.resolve_until(selection, &limits, deadline, &cancelled.cancelled)
            })
            .await
            .map_err(|e| Status::internal(format!("identity selection task failed: {e}")))??;
            tx.send(Ok(response))
                .await
                .map_err(|_| Status::cancelled("identity response stream closed"))?;
        }
        None => {
            summary.completed = false;
            summary.end_reason = crate::pb::StreamEndReason::Abandoned as i32;
        }
    }
    Ok(())
}

async fn read_validated_stream_start(
    inbound: &mut Streaming<StreamSearchRequest>,
) -> Result<
    (
        StartStreamSearch,
        Option<std::time::Duration>,
        Vec<crate::geo::GeoRegion>,
    ),
    Status,
> {
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
    let timeout = start
        .identity_limits
        .as_ref()
        .map(crate::query_identity::validate_limits)
        .transpose()?;
    let geo_regions = validate_scan_filters(&start.geo_filters, start.filter.as_ref())?;
    Ok((start, timeout, geo_regions))
}

struct StreamDriver {
    state: Arc<RwLock<ShardState>>,
    slot_offset: u64,
    segment_pruning: bool,
    share_floors: bool,
    stream_poll_delay: Option<std::time::Duration>,
    stream_signals: Arc<std::sync::Mutex<HashMap<u64, Arc<StreamSignals>>>>,
    rerank_parallel: usize,
    rerank_slots: Arc<tokio::sync::Semaphore>,
}

async fn drive_stream_search(
    mut inbound: Streaming<StreamSearchRequest>,
    tx: mpsc::Sender<Result<StreamSearchResponse, Status>>,
    drv: StreamDriver,
) {
    let (start, identity_timeout, geo_regions) =
        match read_validated_stream_start(&mut inbound).await {
            Ok(validated) => validated,
            Err(status) => {
                let _ = tx.send(Err(status)).await;
                return;
            }
        };
    let identity_limits = start.identity_limits;
    let signals = Arc::new(StreamSignals::new(
        start.initial_floor.unwrap_or(f32::NEG_INFINITY),
    ));
    let udp_token = (start.floor_token != 0).then_some(start.floor_token);
    if let Some(token) = udp_token {
        drv.stream_signals
            .lock()
            .expect("stream signal registry poisoned")
            .insert(token, Arc::clone(&signals));
    }
    let identity_ready = Arc::new(AtomicBool::new(false));
    let identity_enabled = identity_limits.is_some();
    let mut pump = spawn_stream_pump(
        inbound,
        Arc::clone(&signals),
        Arc::clone(&identity_ready),
        identity_enabled,
    );
    let scan_ctx = StreamScanCtx {
        state: drv.state,
        start,
        geo_regions,
        scan_tx: tx.clone(),
        scan_signals: Arc::clone(&signals),
        slot_offset: drv.slot_offset,
        segment_pruning: drv.segment_pruning,
        share_floors: drv.share_floors,
        stream_poll_delay: drv.stream_poll_delay,
        identity_enabled,
    };
    let exact_fp32 = scan_ctx.start.exact_fp32_scan;
    let outcome = if exact_fp32 {
        let parallel = drv.rerank_parallel;
        match drv.rerank_slots.acquire_many_owned(parallel as u32).await {
            Ok(permits) => {
                tokio::task::spawn_blocking(move || {
                    let _permits = permits;
                    run_exact_fp32_stream_scan(scan_ctx, parallel)
                })
                .await
            }
            Err(_) => Ok(Err(Status::unavailable(
                "exact rerank worker budget closed",
            ))),
        }
    } else {
        tokio::task::spawn_blocking(move || run_stream_scan_blocking(scan_ctx)).await
    };
    if let Some(token) = udp_token {
        drv.stream_signals
            .lock()
            .expect("stream signal registry poisoned")
            .remove(&token);
    }
    finish_stream_outcome(
        &tx,
        &mut pump,
        &signals,
        &identity_ready,
        identity_limits,
        identity_timeout,
        outcome,
    )
    .await;
}

async fn finish_stream_outcome(
    tx: &mpsc::Sender<Result<StreamSearchResponse, Status>>,
    pump: &mut tokio::task::JoinHandle<Result<Option<crate::pb::ResolveStreamIdentities>, Status>>,
    signals: &Arc<StreamSignals>,
    identity_ready: &AtomicBool,
    identity_limits: Option<StreamIdentityLimits>,
    identity_timeout: Option<std::time::Duration>,
    outcome: Result<StreamScanResult, tokio::task::JoinError>,
) {
    let deadline = identity_timeout.map(|t| std::time::Instant::now() + t);
    let exchange = async {
        let (mut summary, identities) =
            outcome.map_err(|e| Status::internal(format!("stream scan panicked: {e}")))??;
        if let (Some(limits), Some(identities)) = (identity_limits, identities) {
            let ctx = IdentityExchangeCtx {
                limits,
                deadline: deadline.expect("identity timeout validated"),
                identities,
            };
            run_identity_step(tx, pump, signals, identity_ready, ctx, &mut summary).await?;
        } else if pump.is_finished() {
            (&mut *pump)
                .await
                .map_err(|e| Status::internal(format!("stream request task failed: {e}")))??;
        }
        tx.send(Ok(StreamSearchResponse {
            payload: Some(stream_search_response::Payload::Summary(summary)),
        }))
        .await
        .map_err(|_| Status::cancelled("response stream closed"))
    };
    let result = if let Some(timeout) = identity_timeout {
        tokio::select! {
            _ = tx.closed() => Err(Status::cancelled("identity response stream closed")),
            res = tokio::time::timeout(timeout, exchange) => res.unwrap_or_else(|_| {
                Err(Status::deadline_exceeded("stream identity resolution timed out"))
            }),
        }
    } else {
        exchange.await
    };
    pump.abort();
    if let Err(status) = result {
        signals.cancelled.store(true, AtomicOrdering::Release);
        let _ = tx.send(Err(status)).await;
    }
}

impl NodeServiceImpl {
    pub(in crate::node::rpc) async fn handle_stream_search(
        &self,
        request: Request<Streaming<StreamSearchRequest>>,
    ) -> Result<Response<<Self as NodeService>::StreamSearchStream>, Status> {
        crate::metrics::timed_stream(Route::StreamSearch, request, |request| async move {
            let inbound = request.into_inner();
            let (tx, rx) = mpsc::channel::<Result<StreamSearchResponse, Status>>(64);
            let drv = StreamDriver {
                state: self.state.clone(),
                slot_offset: self.config.slot_offset,
                segment_pruning: self.knobs.segment_pruning(),
                share_floors: self.knobs.share_floors(),
                stream_poll_delay: self.stream_poll_delay,
                stream_signals: Arc::clone(&self.stream_signals),
                rerank_parallel: resolved_rerank_parallel(self.config.rerank_parallel),
                rerank_slots: Arc::clone(&self.rerank_slots),
            };
            tokio::spawn(drive_stream_search(inbound, tx, drv));
            Ok(Response::new(ReceiverStream::new(rx)))
        })
        .await
    }
}

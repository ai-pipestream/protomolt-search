//! Streaming BM25 query: candidate batches, floor exchange, and completion.

use crate::node::*;

type StreamTx = mpsc::Sender<Result<Bm25QueryStreamResponse, Status>>;

/// Protocol: the first message must be Start. A violation is answered on
/// `tx` and yields `None`.
async fn await_start(
    inbound: &mut Streaming<Bm25QueryStreamRequest>,
    tx: &StreamTx,
) -> Option<Bm25QueryRequest> {
    match inbound.message().await {
        Ok(Some(Bm25QueryStreamRequest {
            payload: Some(bm25_query_stream_request::Payload::Start(req)),
        })) => Some(req),
        Ok(_) => {
            let _ = tx
                .send(Err(Status::invalid_argument(
                    "first Bm25QueryStreamRequest must be a Bm25QueryRequest start",
                )))
                .await;
            None
        }
        Err(e) => {
            let _ = tx.send(Err(e)).await;
            None
        }
    }
}

/// Floor updates arrive on the same stream; a pump task folds them into a
/// watch cell the blocking scan polls each loop iteration. Updates are
/// monotone maxes, so only raises are stored — exactly the SearchShard pump.
fn spawn_floor_pump(
    mut inbound: Streaming<Bm25QueryStreamRequest>,
    floor_tx: watch::Sender<f32>,
    pump_cancelled: Arc<AtomicBool>,
) {
    tokio::spawn(async move {
        loop {
            match inbound.message().await {
                Ok(Some(Bm25QueryStreamRequest {
                    payload: Some(bm25_query_stream_request::Payload::FloorUpdate(u)),
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
                Ok(Some(Bm25QueryStreamRequest {
                    payload: Some(bm25_query_stream_request::Payload::Stop(_)),
                })) => {
                    pump_cancelled.store(true, AtomicOrdering::Release);
                    break;
                }
                // Duplicate Start or empty payload: ignore.
                Ok(Some(_)) => {}
                // Client closed or the stream broke: stop pumping;
                // the scan finishes on its own either way.
                Ok(None) | Err(_) => break,
            }
        }
    });
}

/// The scorer-side hook: publish strict raises of the running k-th best
/// (gated by the node's floor knobs, never blocking the scan — dropped raises
/// are superseded by the next one), and hand back the highest coordinator
/// floor seen.
fn floor_hook(
    service: &NodeServiceImpl,
    scan_tx: StreamTx,
    hook_cancelled: Arc<AtomicBool>,
    floor_rx: watch::Receiver<f32>,
) -> impl FnMut(Option<f32>) -> Option<f32> {
    let share = service.knobs.share_floors();
    let floor_delta = service.knobs.floor_delta();
    let floor_min_interval_ms = service.knobs.floor_min_interval_ms();
    let min_interval = (floor_min_interval_ms > 0)
        .then(|| std::time::Duration::from_millis(floor_min_interval_ms));
    let mut last_published = f32::NEG_INFINITY;
    let mut last_at: Option<std::time::Instant> = None;
    move |seed: Option<f32>| -> Option<f32> {
        if hook_cancelled.load(AtomicOrdering::Acquire) || scan_tx.is_closed() {
            hook_cancelled.store(true, AtomicOrdering::Release);
            return Some(f32::INFINITY);
        }
        if !share {
            return None;
        }
        if let Some(seed) = seed {
            let debounced = matches!(
                (min_interval, last_at),
                (Some(interval), Some(at)) if at.elapsed() < interval
            );
            if seed > last_published + floor_delta && !debounced {
                last_published = seed;
                last_at = Some(std::time::Instant::now());
                let _ = scan_tx.try_send(Ok(Bm25QueryStreamResponse {
                    payload: Some(bm25_query_stream_response::Payload::FloorUpdate(
                        FloorUpdate { floor: seed },
                    )),
                }));
            }
        }
        let f = *floor_rx.borrow();
        (f != f32::NEG_INFINITY).then_some(f)
    }
}

/// Send one packed candidate batch, returning whether the client took it.
fn send_candidates(candidate_tx: &StreamTx, candidates: Vec<u8>) -> bool {
    candidate_tx
        .blocking_send(Ok(Bm25QueryStreamResponse {
            payload: Some(bm25_query_stream_response::Payload::CandidateBatch(
                Bm25CandidateBatch { candidates },
            )),
        }))
        .is_ok()
}

/// Run the scan on the blocking pool, streaming 12-byte `(id, score)` records.
/// Returns the scan's response, whether it ran to completion, and the records
/// the client accepted.
fn scan_candidates(
    service: &NodeServiceImpl,
    req: Bm25QueryRequest,
    mut hook: impl FnMut(Option<f32>) -> Option<f32>,
    candidate_tx: &StreamTx,
    scan_cancelled: &AtomicBool,
) -> (Result<Bm25QueryResponse, Status>, bool, u64) {
    // Stay below tonic's default 4 MiB even if a caller lowers
    // the process-wide cap. 64 KiB amortizes framing without
    // delaying the first useful candidates.
    const BATCH_BYTES: usize = (64 * 1024 / 12) * 12;
    let mut pending = Vec::with_capacity(BATCH_BYTES);
    let mut emitted = 0u64;
    let response = {
        let mut emit = |doc_id: u32, score: f32| {
            pending
                .extend_from_slice(&(service.config.slot_offset + u64::from(doc_id)).to_le_bytes());
            pending.extend_from_slice(&score.to_le_bytes());
            if pending.len() >= BATCH_BYTES {
                let records = (pending.len() / 12) as u64;
                let batch = std::mem::replace(&mut pending, Vec::with_capacity(BATCH_BYTES));
                if send_candidates(candidate_tx, batch) {
                    emitted += records;
                } else {
                    scan_cancelled.store(true, AtomicOrdering::Release);
                }
            }
        };
        service.run_bm25_query_live(req, Some(&mut hook), Some(&mut emit))
    };
    if response.is_ok() && !pending.is_empty() {
        let records = (pending.len() / 12) as u64;
        if send_candidates(candidate_tx, pending) {
            emitted += records;
        } else {
            scan_cancelled.store(true, AtomicOrdering::Release);
        }
    }
    let completed = !scan_cancelled.load(AtomicOrdering::Acquire);
    (response, completed, emitted)
}

async fn run_query_stream(
    service: NodeServiceImpl,
    mut inbound: Streaming<Bm25QueryStreamRequest>,
    tx: StreamTx,
) {
    let Some(req) = await_start(&mut inbound, &tx).await else {
        return;
    };
    let (floor_tx, floor_rx) = watch::channel(f32::NEG_INFINITY);
    let cancelled = Arc::new(AtomicBool::new(false));
    spawn_floor_pump(inbound, floor_tx, Arc::clone(&cancelled));
    let hook = floor_hook(&service, tx.clone(), Arc::clone(&cancelled), floor_rx);
    let scoring_fingerprint = bm25_scoring_fingerprint(&req);
    let candidate_tx = tx.clone();
    let scan_cancelled = Arc::clone(&cancelled);
    let outcome = tokio::task::spawn_blocking(move || {
        scan_candidates(&service, req, hook, &candidate_tx, &scan_cancelled)
    })
    .await
    .unwrap_or_else(|e| {
        (
            Err(Status::internal(format!("bm25 stream task failed: {e}"))),
            false,
            0,
        )
    });
    let _ = match outcome {
        (Ok(response), completed, candidates_emitted) => {
            tx.send(Ok(Bm25QueryStreamResponse {
                payload: Some(bm25_query_stream_response::Payload::Completion(
                    Bm25StreamCompletion {
                        completed,
                        response: completed.then_some(response),
                        scoring_fingerprint,
                        candidates_emitted,
                    },
                )),
            }))
            .await
        }
        (Err(e), _, _) => tx.send(Err(e)).await,
    };
}

impl NodeServiceImpl {
    pub(in crate::node::rpc) async fn handle_bm25_query_stream(
        &self,
        request: Request<Streaming<Bm25QueryStreamRequest>>,
    ) -> Result<Response<<Self as NodeService>::Bm25QueryStreamStream>, Status> {
        crate::metrics::timed_stream(Route::Bm25QueryStream, request, |request| async move {
            let inbound = request.into_inner();
            let (tx, rx) = mpsc::channel::<Result<Bm25QueryStreamResponse, Status>>(64);
            tokio::spawn(run_query_stream(self.clone(), inbound, tx));
            Ok(Response::new(ReceiverStream::new(rx)))
        })
        .await
    }
}

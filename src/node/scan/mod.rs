//! Coalesced vector scan scheduling, scan-rate windowing, and UDP floor signals.

use crate::node::*;

mod batch;
mod window;

pub(in crate::node) use batch::run_scan_batch;
pub use window::{
    observe_scan, scan_rate, ScanRate, ScanWindow, SCAN_WINDOW_MIN_SAMPLES, SCAN_WINDOW_MS,
    SCAN_WINDOW_SAMPLES,
};

pub(in crate::node) fn proto_scan_stats(stats: ScanStats) -> ShardScanStats {
    ShardScanStats {
        chunk_calls: stats.chunk_calls,
        candidates_collected: stats.candidates_collected,
        floors_published: stats.floors_published,
        floor_updates_applied: stats.floor_updates_applied,
        floors_offered: stats.floors_offered,
        segments_total: stats.segments_total,
        segments_skipped: stats.segments_skipped,
        filter_regimes: stats.regimes.to_proto(),
        tail_enumerated_rows: stats.tail_enumerated_rows,
        patience_stopped: stats.patience_stopped,
        floor_pruned: stats.floor_pruned,
        parts_total: stats.parts.total,
        parts_skipped: stats.parts.skipped,
        parts_walked: stats.parts.walked,
        parts_enumerated: stats.parts.enumerated,
        parts_scanned: stats.parts.scanned,
        parts_staged: stats.parts.staged,
        fp32_rows_scored: stats.fp32_rows_scored,
        fp32_logical_bytes: stats.fp32_logical_bytes,
        fp32_pages_touched: stats.fp32_pages_touched,
        fp32_tasks: stats.fp32_tasks,
    }
}

/// Raise a floor cell (f32 bits) to `floor` if that is higher. Monotone
/// under any interleaving of the gRPC and UDP lanes; NaN is ignored.
// fetch_update is deprecated in favor of try_update from Rust 1.99, but the
// Pi binaries are built on 1.97, which lacks try_update.
#[allow(deprecated)]
pub(in crate::node) fn raise_floor_cell(cell: &std::sync::atomic::AtomicU32, floor: f32) {
    if floor.is_nan() {
        return;
    }
    let _ = cell.fetch_update(
        std::sync::atomic::Ordering::AcqRel,
        std::sync::atomic::Ordering::Acquire,
        |cur| (floor > f32::from_bits(cur)).then(|| floor.to_bits()),
    );
}

#[cfg_attr(not(feature = "net"), allow(dead_code))]
pub(in crate::node) struct StreamSignals {
    pub(in crate::node) floor: std::sync::atomic::AtomicU32,
    pub(in crate::node) cancelled: std::sync::atomic::AtomicBool,
    /// `0` = running, `1` = coordinator settle finish (`deadline = false`),
    /// `2` = coordinator deadline finish (`deadline = true`).
    pub(in crate::node) finish_state: std::sync::atomic::AtomicU8,
    /// The newest signed sequence applied to this stream; a datagram
    /// at or behind it is a replay and is ignored.
    pub(in crate::node) last_seq: std::sync::atomic::AtomicU32,
}

#[cfg_attr(not(feature = "net"), allow(dead_code))]
impl StreamSignals {
    pub(in crate::node) fn new(initial_floor: f32) -> Self {
        Self {
            floor: std::sync::atomic::AtomicU32::new(initial_floor.to_bits()),
            cancelled: std::sync::atomic::AtomicBool::new(false),
            finish_state: std::sync::atomic::AtomicU8::new(0),
            last_seq: std::sync::atomic::AtomicU32::new(0),
        }
    }

    pub(in crate::node) fn request_finish(&self, deadline: bool) {
        let code = if deadline { 2 } else { 1 };
        self.finish_state
            .fetch_max(code, std::sync::atomic::Ordering::AcqRel);
    }

    pub(in crate::node) fn finish_state(&self) -> Option<bool> {
        match self.finish_state.load(std::sync::atomic::Ordering::Acquire) {
            1 => Some(false),
            2 => Some(true),
            _ => None,
        }
    }

    /// Accept `seq` when it is newer than the last applied one.
    pub(in crate::node) fn accept_seq(&self, seq: u32) -> bool {
        let mut current = self.last_seq.load(std::sync::atomic::Ordering::Acquire);
        loop {
            if seq <= current {
                return false;
            }
            match self.last_seq.compare_exchange_weak(
                current,
                seq,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(seen) => current = seen,
            }
        }
    }
}

#[cfg(feature = "net")]
/// Fold one typed UDP signal into its stream state. Anything malformed or
/// addressed to an unknown token is dropped. A UDP cancel is advisory only:
/// it makes the node return `completed = false`, and the authoritative gRPC
/// stream carries the matching `StopStreamSearch`.
pub(in crate::node) fn apply_stream_datagram(
    signals: &std::sync::Mutex<HashMap<u64, Arc<StreamSignals>>>,
    key: Option<&crate::security::UdpKey>,
    datagram: &[u8],
) {
    // Require a signed datagram when a key is configured; plain frames are loopback-only.
    let (signal, seq) = match key {
        Some(key) => match crate::stream_signal::decode_signed(key, datagram) {
            Some((signal, seq)) => (Some(signal), Some(seq)),
            None => return,
        },
        None => (crate::stream_signal::decode(datagram), None),
    };
    let token = match signal {
        Some(crate::stream_signal::StreamSignal::RaiseFloor { token, .. })
        | Some(crate::stream_signal::StreamSignal::Cancel { token })
        | Some(crate::stream_signal::StreamSignal::Finish { token, .. }) => token,
        None => return,
    };
    let state = signals
        .lock()
        .expect("stream signal registry poisoned")
        .get(&token)
        .cloned();
    let Some(state) = state else {
        return;
    };
    if let Some(seq) = seq {
        if !state.accept_seq(seq) {
            return;
        }
    }
    match signal.expect("validated above") {
        crate::stream_signal::StreamSignal::RaiseFloor { floor, .. } => {
            raise_floor_cell(&state.floor, floor);
        }
        crate::stream_signal::StreamSignal::Cancel { .. } => {
            state
                .cancelled
                .store(true, std::sync::atomic::Ordering::Release);
        }
        crate::stream_signal::StreamSignal::Finish { deadline, .. } => {
            state.request_finish(deadline);
        }
    }
}

/// Kernel batch width: turbovec's multi-query scan scores up to four
/// queries per pass over each block, so batches beyond four stop
/// amortizing memory traffic.
pub(in crate::node) const MAX_COALESCE: usize = 4;

static SCAN_BATCHES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static SCAN_BATCHED_JOBS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// Process-wide coalescing telemetry: `(batches formed, jobs in them)`.
/// Jobs exceeding batches means multi-query batches actually formed —
/// the observable that coalescing engaged, used by tests and benchmarks.
pub fn scan_batch_counters() -> (u64, u64) {
    (
        SCAN_BATCHES.load(std::sync::atomic::Ordering::Relaxed),
        SCAN_BATCHED_JOBS.load(std::sync::atomic::Ordering::Relaxed),
    )
}

/// One completed shard scan, with the handshake flags that let the
/// coordinator refuse a filter column no shard resolves.
pub(in crate::node) struct ScanOutcome {
    pub(in crate::node) hits: Vec<ChunkHit>,
    pub(in crate::node) identities: crate::source_archive::IdentitySnapshot,
    pub(in crate::node) rerank_candidates: HashMap<u32, crate::pb::RerankCandidatePayload>,
    pub(in crate::node) stats: ScanStats,
    pub(in crate::node) geo_columns_known: Vec<bool>,
    pub(in crate::node) filter_columns_known: Vec<bool>,
}

/// One shard scan queued for a batched kernel pass.
pub(in crate::node) struct ScanJob {
    pub(in crate::node) read_context: Option<crate::pb::VectorReadContext>,
    pub(in crate::node) ready_tx: mpsc::Sender<Result<SearchShardResponse, Status>>,
    pub(in crate::node) vector: Vec<f32>,
    pub(in crate::node) k: usize,
    pub(in crate::node) tie_complete: bool,
    pub(in crate::node) include_rerank_candidate: bool,
    /// The request's filters, resolved into an allowlist by the batch
    /// runner under the SAME read guard the scan holds, so the columns
    /// and the index a scan sees are one snapshot.
    pub(in crate::node) geo_filters: Vec<crate::pb::GeoFilter>,
    pub(in crate::node) geo_regions: Vec<crate::geo::GeoRegion>,
    pub(in crate::node) filter: Option<crate::pb::FilterExpr>,
    /// The request's enumerate threshold (`StartShardSearch`).
    pub(in crate::node) enumerate_max_rows: Option<u32>,
    /// The plan's filter routing and whole-set enumeration (`StartShardSearch`).
    pub(in crate::node) filter_strategy: i32,
    pub(in crate::node) enumerate_all: bool,
    /// The request's patience early-stop parameters (`StartShardSearch`).
    pub(in crate::node) patience_saturation_ppm: Option<u32>,
    pub(in crate::node) patience_hops: Option<u32>,
    /// The request's shared-floor candidate pruning parameters (`StartShardSearch`).
    pub(in crate::node) floor_prune: Option<bool>,
    pub(in crate::node) floor_k: Option<u32>,
    pub(in crate::node) floor_share_ppm: Option<u32>,
    pub(in crate::node) floor_greediness_ppm: Option<u32>,
    pub(in crate::node) floor_clamp_min: Option<u32>,
    /// Polled between chunks for the best coordinator-pushed floor
    /// (returns `None` when floor sharing is off or no floor arrived).
    pub(in crate::node) external: Box<dyn FnMut() -> Option<f32> + Send>,
    /// Receives this query's k-th-best raises (the caller bakes in the
    /// share gate and delta filter).
    pub(in crate::node) publish: Box<dyn FnMut(f32) -> bool + Send>,
    pub(in crate::node) done: tokio::sync::oneshot::Sender<Result<ScanOutcome, Status>>,
}

/// Batch former: one scan slot at a time per permit, and every job that
/// queued while all slots were busy coalesces into the next drain. Under
/// light load batches are singletons and scans run as parallel as before;
/// under heavy load freed slots pick up to [`MAX_COALESCE`] waiting
/// queries and score them in one pass over the packed codes.
async fn scan_scheduler(
    state: Arc<std::sync::RwLock<ShardState>>,
    chunk_blocks: usize,
    knobs: Arc<crate::diagnostics::Knobs>,
    parallel: usize,
    mut jobs: mpsc::Receiver<ScanJob>,
) {
    let slots = Arc::new(tokio::sync::Semaphore::new(parallel.max(1)));
    loop {
        // A slot first, then the batch: the wait for a slot is exactly
        // when coalescable jobs accumulate.
        let permit = slots
            .clone()
            .acquire_owned()
            .await
            .expect("scan semaphore never closes");
        let Some(first) = jobs.recv().await else {
            break;
        };
        let mut batch = vec![first];
        while batch.len() < MAX_COALESCE {
            match jobs.try_recv() {
                Ok(job) => batch.push(job),
                Err(_) => break,
            }
        }
        SCAN_BATCHES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        SCAN_BATCHED_JOBS.fetch_add(batch.len() as u64, std::sync::atomic::Ordering::Relaxed);
        let state = state.clone();
        let segment_pruning = knobs.segment_pruning();
        tokio::task::spawn_blocking(move || {
            let _slot = permit;
            run_scan_batch(&state, chunk_blocks, segment_pruning, batch);
        });
    }
}
impl NodeServiceImpl {
    /// Bind the UDP signal lane on `addr` — the same host:port as the
    /// gRPC listener, UDP namespace — and fold typed floor/cancel datagrams
    /// into the matching stream state (see `apply_stream_datagram`). A
    /// failed bind only loses the fast lane: both signals still travel on
    /// every stream's authoritative gRPC leg.
    #[cfg(feature = "net")]
    pub fn spawn_floor_listener(&self, addr: std::net::SocketAddr) {
        let signals = Arc::clone(&self.stream_signals);
        let key = self.config.udp_hmac_key.clone();
        // Unsigned floors are accepted on loopback only: off loopback, a
        // forged floor could cut candidates, so the lane stays closed
        // without a key and the gRPC streams carry every signal.
        if key.is_none() && !crate::security::is_loopback(&addr) {
            eprintln!(
                "stream-signal UDP lane on {addr} stays closed: no --udp-hmac-key, and unsigned \
                 datagrams are accepted on loopback only; signals ride the gRPC streams"
            );
            return;
        }
        tokio::spawn(async move {
            let socket = match tokio::net::UdpSocket::bind(addr).await {
                Ok(socket) => socket,
                Err(e) => {
                    eprintln!(
                        "stream-signal UDP bind {addr}: {e}; signals ride the gRPC streams only"
                    );
                    return;
                }
            };
            let mut buf = [0u8; 64];
            loop {
                match socket.recv_from(&mut buf).await {
                    Ok((n, _peer)) => apply_stream_datagram(&signals, key.as_ref(), &buf[..n]),
                    Err(_) => continue,
                }
            }
        });
    }

    /// The shared scan queue, spawning the scheduler on first use (RPC
    /// handlers guarantee a runtime here).
    pub(in crate::node) fn scan_queue(&self) -> mpsc::Sender<ScanJob> {
        self.scan_jobs
            .get_or_init(|| {
                let (tx, rx) = mpsc::channel(4096);
                let parallel = if self.config.scan_parallel > 0 {
                    self.config.scan_parallel
                } else {
                    std::thread::available_parallelism()
                        .map(|n| n.get())
                        .unwrap_or(2)
                        .div_ceil(2)
                };
                tokio::spawn(scan_scheduler(
                    self.state.clone(),
                    self.config.chunk_blocks,
                    Arc::clone(&self.knobs),
                    parallel,
                    rx,
                ));
                tx
            })
            .clone()
    }

    /// Validate query coordinates before calling the vector kernel.
    pub(in crate::node) fn validate_start(
        index: &VectorIndex,
        start: &StartShardSearch,
    ) -> Result<(), Status> {
        let dim = index
            .dim_opt()
            .ok_or_else(|| Status::failed_precondition("index has no vectors"))?;
        if start.vector.len() != dim {
            return Err(Status::invalid_argument(format!(
                "query vector has dim {}, index expects {dim}",
                start.vector.len()
            )));
        }
        if let Some((_, coord, value)) = first_invalid_coordinate(&start.vector, dim) {
            return Err(Status::invalid_argument(format!(
                "query coordinate {coord} is invalid: {value}"
            )));
        }
        check_enumerate_threshold(index, start.enumerate_max_rows)?;
        selection_controls(index, start.filter_strategy, start.enumerate_all)?;
        check_patience_options(index, start.patience_saturation_ppm, start.patience_hops)?;
        check_floor_prune_options(
            index,
            start.floor_prune,
            start.floor_k,
            start.k,
            start.floor_share_ppm,
            start.floor_greediness_ppm,
            start.floor_clamp_min,
        )?;
        Ok(())
    }
}

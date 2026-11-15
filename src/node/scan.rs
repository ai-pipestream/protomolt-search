//! Coalesced vector scan scheduling, scan-rate windowing, and UDP floor signals.

use crate::node::*;

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

/// Scans the window keeps at most.
pub const SCAN_WINDOW_SAMPLES: usize = 64;
/// A sample older than this leaves the window.
pub const SCAN_WINDOW_MS: u64 = 10 * 60 * 1000;
/// Fewer samples than this report an unknown rate.
pub const SCAN_WINDOW_MIN_SAMPLES: usize = 4;

/// One scan's cost as the window keeps it.
#[derive(Debug, Clone, Copy)]
pub(in crate::node) struct ScanSample {
    pub(in crate::node) at: std::time::Instant,
    pub(in crate::node) unix_ms: u64,
    pub(in crate::node) bytes: u64,
    pub(in crate::node) nanos: u64,
}

/// Observed scan throughput over recent windowed scans (`0` until at least
/// [`SCAN_WINDOW_MIN_SAMPLES`] non-zero kernel passes have been recorded).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ScanRate {
    pub bytes_per_second: u64,
    /// When the newest sample was taken (unix ms); 0 with no sample.
    pub observed_unix_ms: u64,
    pub samples: u32,
    pub window_ms: u64,
}

/// The bounded window of recent scans behind [`scan_rate`].
pub struct ScanWindow {
    pub(in crate::node) samples: std::sync::Mutex<std::collections::VecDeque<ScanSample>>,
}

impl ScanWindow {
    pub const fn new() -> Self {
        ScanWindow {
            samples: std::sync::Mutex::new(std::collections::VecDeque::new()),
        }
    }

    /// Record one scan: the bytes its kernel calls streamed and their
    /// active time. A scan that streamed nothing (a lazy index, or a
    /// filter that emptied it) is not a rate sample.
    pub fn observe(&self, bytes: u64, nanos: u64) {
        if bytes == 0 {
            return;
        }
        let now = std::time::Instant::now();
        let sample = ScanSample {
            at: now,
            unix_ms: crate::diagnostics::unix_ms(),
            bytes,
            nanos,
        };
        let mut samples = self.samples.lock().expect("scan window poisoned");
        Self::expire(&mut samples, now);
        if samples.len() == SCAN_WINDOW_SAMPLES {
            samples.pop_front();
        }
        samples.push_back(sample);
    }

    pub(in crate::node) fn expire(
        samples: &mut std::collections::VecDeque<ScanSample>,
        now: std::time::Instant,
    ) {
        let horizon = std::time::Duration::from_millis(SCAN_WINDOW_MS);
        while let Some(oldest) = samples.front() {
            if now.duration_since(oldest.at) > horizon {
                samples.pop_front();
            } else {
                break;
            }
        }
    }

    pub fn rate(&self) -> ScanRate {
        let mut samples = self.samples.lock().expect("scan window poisoned");
        Self::expire(&mut samples, std::time::Instant::now());
        let count = samples.len();
        let observed_unix_ms = samples.back().map_or(0, |s| s.unix_ms);
        let (bytes, nanos) = samples.iter().fold((0u128, 0u128), |(b, n), s| {
            (b + u128::from(s.bytes), n + u128::from(s.nanos))
        });
        let bytes_per_second = if count < SCAN_WINDOW_MIN_SAMPLES || nanos == 0 {
            0
        } else {
            u64::try_from(bytes * 1_000_000_000 / nanos).unwrap_or(u64::MAX)
        };
        ScanRate {
            bytes_per_second,
            observed_unix_ms,
            samples: count as u32,
            window_ms: SCAN_WINDOW_MS,
        }
    }

    #[cfg(test)]
    pub(in crate::node) fn clear(&self) {
        self.samples.lock().expect("scan window poisoned").clear();
    }
}

impl Default for ScanWindow {
    fn default() -> Self {
        Self::new()
    }
}

/// Process-wide scan throughput window reported on lease renewal.
static SCAN_WINDOW: ScanWindow = ScanWindow::new();

/// The node's observed scan rate for the lease renewal.
pub fn scan_rate() -> ScanRate {
    SCAN_WINDOW.rate()
}

/// Account one scan over this shard: the process counters take the pass
/// bytes and time once, and the window takes them as one sample.
pub fn observe_scan(bytes: u64, nanos: u64) {
    crate::metrics::record_scan_pass(bytes, nanos);
    SCAN_WINDOW.observe(bytes, nanos);
}

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

struct QueuedQuerySpec {
    pub(in crate::node) vector: Vec<f32>,
    pub(in crate::node) k: usize,
    pub(in crate::node) keep_ties: bool,
    pub(in crate::node) include_rerank_candidate: bool,
    pub(in crate::node) enumerate_max_rows: Option<usize>,
    pub(in crate::node) patience: Option<crate::vector::PatienceSpec>,
    pub(in crate::node) floor_prune: Option<crate::vector::FloorPruneSpec>,
}

/// Run one batched scan under the shard read lock and deliver every job's
/// result. Blocking-pool context.
pub(in crate::node) fn run_scan_batch(
    state: &std::sync::RwLock<ShardState>,
    chunk_blocks: usize,
    segment_pruning: bool,
    batch: Vec<ScanJob>,
) {
    let guard = read_shard(state);
    let index = match guard.index.as_ref() {
        Some(index) => index,
        None => {
            for job in batch {
                let _ = job.done.send(Err(Status::failed_precondition(
                    "shard has no index yet (set calibration or add vectors)",
                )));
            }
            return;
        }
    };
    // Re-validate dimensions against the CURRENT index: the shard may have
    // been swapped (InstallSnapshot) between the RPC's validation and this
    // batch winning a slot.
    let dim = index.dim_opt();
    let slots = index.len();
    let mut specs: Vec<QueuedQuerySpec> = Vec::with_capacity(batch.len());
    let mut allows: Vec<Option<crate::boolean_bits::Bits>> = Vec::with_capacity(batch.len());
    let mut prunes: Vec<crate::segment_prune::PruneStats> = Vec::with_capacity(batch.len());
    let mut knowns: Vec<(Vec<bool>, Vec<bool>)> = Vec::with_capacity(batch.len());
    let mut externals: Vec<Box<dyn FnMut() -> Option<f32> + Send>> = Vec::new();
    let mut publishers: Vec<Box<dyn FnMut(f32) -> bool + Send>> = Vec::new();
    let mut dones = Vec::new();
    for job in batch {
        if Some(job.vector.len()) != dim {
            let _ = job.done.send(Err(Status::failed_precondition(format!(
                "query dim {} no longer matches the index",
                job.vector.len()
            ))));
            continue;
        }
        if let Err(refused) = check_enumerate_threshold(index, job.enumerate_max_rows) {
            let _ = job.done.send(Err(refused));
            continue;
        }
        let patience =
            match check_patience_options(index, job.patience_saturation_ppm, job.patience_hops) {
                Ok(spec) => spec,
                Err(refused) => {
                    let _ = job.done.send(Err(refused));
                    continue;
                }
            };
        let floor_prune = match check_floor_prune_options(
            index,
            job.floor_prune,
            job.floor_k,
            job.k as u32,
            job.floor_share_ppm,
            job.floor_greediness_ppm,
            job.floor_clamp_min,
        ) {
            Ok(spec) => spec,
            Err(refused) => {
                let _ = job.done.send(Err(refused));
                continue;
            }
        };
        let resolved = guard.vector_scan_filters(
            slots,
            &job.geo_filters,
            &job.geo_regions,
            job.filter.as_ref(),
            job.read_context.as_ref(),
            segment_pruning,
        );
        let (allow, prune, receipt) = match resolved {
            Ok(((_, allow, prune), receipt)) => (allow, prune, receipt),
            Err(e) => {
                let _ = job.done.send(Err(e));
                continue;
            }
        };
        if let Some(receipt) = receipt {
            if job
                .ready_tx
                .blocking_send(Ok(SearchShardResponse {
                    payload: Some(search_shard_response::Payload::ReadReady(receipt)),
                }))
                .is_err()
            {
                let _ = job
                    .done
                    .send(Err(Status::cancelled("scoped scan response closed")));
                continue;
            }
        }
        prunes.push(prune);
        knowns.push(filter_known_flags(
            guard.bm25.as_ref(),
            &job.geo_filters,
            job.filter.as_ref(),
        ));
        allows.push(allow);
        specs.push(QueuedQuerySpec {
            vector: job.vector,
            k: job.k,
            keep_ties: job.tie_complete,
            include_rerank_candidate: job.include_rerank_candidate,
            enumerate_max_rows: job.enumerate_max_rows.map(|rows| rows as usize),
            patience,
            floor_prune,
        });
        externals.push(job.external);
        publishers.push(job.publish);
        dones.push(job.done);
    }
    if dones.is_empty() {
        return;
    }
    let identities = match guard
        .bm25
        .as_ref()
        .map(Bm25Shard::identity_snapshot)
        .transpose()
    {
        Ok(identities) => identities.unwrap_or_default(),
        Err(error) => {
            for done in dones {
                let _ = done.send(Err(Status::internal(error.clone())));
            }
            return;
        }
    };
    let queries: Vec<BatchQuery> = specs
        .iter()
        .zip(&allows)
        .map(|(spec, allow)| BatchQuery {
            vector: &spec.vector,
            k: spec.k,
            keep_ties: spec.keep_ties,
            allow: allow.as_ref().map(crate::vector::Mask::Bits),
            enumerate_max_rows: spec.enumerate_max_rows,
            patience: spec.patience,
            floor_prune: spec.floor_prune,
        })
        .collect();
    let mut pass_bytes = 0u64;
    let mut pass_nanos = 0u64;
    let results = crate::chunked::chunked_topk_batch_observed(
        index,
        &queries,
        chunk_blocks,
        &mut |qi| (externals[qi])(),
        &mut |qi, floor| (publishers[qi])(floor),
        &mut |pass| {
            pass_bytes = pass_bytes.saturating_add(pass.bytes);
            pass_nanos = pass_nanos.saturating_add(pass.nanos);
        },
    );
    // Record one sample per batched shard pass regardless of batch width.
    observe_scan(pass_bytes, pass_nanos);
    for ((((done, (hits, mut stats)), (geo_columns_known, filter_columns_known)), prune), spec) in
        dones
            .into_iter()
            .zip(results)
            .zip(knowns)
            .zip(prunes)
            .zip(specs)
    {
        stats.segments_total = prune.segments_total;
        stats.segments_skipped = prune.segments_skipped;
        crate::metrics::record_scan(&stats);
        let mut rerank_candidates = HashMap::new();
        if spec.include_rerank_candidate {
            for h in &hits {
                if let Some(payload) = guard.rerank_candidate_for(h.slot) {
                    rerank_candidates.insert(h.slot, payload);
                }
            }
        }
        let _ = done.send(Ok(ScanOutcome {
            hits,
            identities: identities.clone(),
            rerank_candidates,
            stats,
            geo_columns_known,
            filter_columns_known,
        }));
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

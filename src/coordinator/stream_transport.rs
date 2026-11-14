//! Validated shard stream setup, signaling, and identity exchange.

use super::*;

/// Validated inputs for opening a shard stream fan-out.
pub(crate) struct StreamOpen<'a> {
    request_id: &'a str,
    vector: &'a [f32],
    initial_floor: Option<f32>,
    candidate_depth: Option<u32>,
    collapse_parents: bool,
    filters: &'a RequestFilters,
    identity_limits: Option<crate::pb::StreamIdentityLimits>,
}

impl<'a> StreamOpen<'a> {
    pub(crate) fn new(
        request_id: &'a str,
        vector: &'a [f32],
        initial_floor: Option<f32>,
        candidate_depth: Option<u32>,
        collapse_parents: bool,
        filters: &'a RequestFilters,
        identity_limits: Option<crate::pb::StreamIdentityLimits>,
    ) -> Result<Self, Status> {
        if initial_floor.is_some_and(f32::is_nan) {
            return Err(Status::invalid_argument("initial_floor must not be NaN"));
        }
        if let Some(limits) = &identity_limits {
            crate::query_identity::validate_limits(limits)?;
            if collapse_parents {
                return Err(Status::invalid_argument(
                    "collapse streams do not support identity limits",
                ));
            }
        }
        Ok(Self {
            request_id,
            vector,
            initial_floor,
            candidate_depth,
            collapse_parents,
            filters,
            identity_limits,
        })
    }

    fn start(
        &self,
        read_context: Option<crate::pb::VectorReadContext>,
        floor_token: u64,
        filter: Option<crate::pb::FilterExpr>,
        floor_share_ppm: Option<u32>,
    ) -> StreamSearchRequest {
        let filters = self.filters;
        StreamSearchRequest {
            payload: Some(stream_search_request::Payload::Start(StartStreamSearch {
                read_context,
                request_id: self.request_id.to_string(),
                vector: self.vector.to_vec(),
                initial_floor: self.initial_floor,
                candidate_depth: self.candidate_depth,
                enumerate_max_rows: filters.enumerate_max_rows,
                patience_saturation_ppm: filters.patience_saturation_ppm,
                patience_hops: filters.patience_hops,
                floor_token,
                collapse_parents: self.collapse_parents,
                geo_filters: filters.geo.clone(),
                filter,
                identity_limits: self.identity_limits,
                floor_prune: filters.floor_prune,
                floor_k: filters.floor_k.or(self.candidate_depth),
                floor_share_ppm,
                floor_greediness_ppm: filters.floor_greediness_ppm,
                floor_clamp_min: filters.floor_clamp_min,
                include_rerank_candidate: filters.include_rerank_candidate,
            })),
        }
    }
}

type StreamInboundSender = mpsc::Sender<(usize, Result<Option<StreamSearchResponse>, Status>)>;

struct StreamReader {
    shard: usize,
    client: crate::link::NodeLink,
    requests: mpsc::Receiver<StreamSearchRequest>,
    merged: StreamInboundSender,
    admission: Option<Arc<crate::vector_read::VectorReadBarrier>>,
    deadline: Option<Duration>,
}

impl StreamReader {
    async fn run(self) {
        let Self {
            shard,
            mut client,
            requests,
            merged,
            admission,
            deadline,
        } = self;
        let result = {
            let read =
                forward_stream_reader(&mut client, shard, requests, &merged, admission.as_deref());
            if let Some(deadline) = deadline {
                tokio::time::timeout(deadline, read)
                    .await
                    .unwrap_or_else(|_| {
                        Err(Status::deadline_exceeded(
                            "vector stream exceeded shard deadline",
                        ))
                    })
            } else {
                read.await
            }
        };
        if let Err(error) = result {
            if let Some(admission) = &admission {
                admission.fail(error.clone());
            }
            let _ = merged.send((shard, Err(error))).await;
        }
    }
}

async fn forward_stream_reader(
    client: &mut crate::link::NodeLink,
    shard: usize,
    requests: mpsc::Receiver<StreamSearchRequest>,
    merged: &StreamInboundSender,
    admission: Option<&crate::vector_read::VectorReadBarrier>,
) -> Result<(), Status> {
    let mut inbound = client
        .stream_search(Request::new(ReceiverStream::new(requests)))
        .await?
        .into_inner();
    if let Some(admission) = admission {
        admission.admit(shard, &mut inbound).await?;
    }
    loop {
        match crate::vector_read::next(&mut inbound).await {
            Ok(Some(message)) => {
                if merged.send((shard, Ok(Some(message)))).await.is_err() {
                    return Ok(());
                }
            }
            Ok(None) => {
                let _ = merged.send((shard, Ok(None))).await;
                return Ok(());
            }
            Err(error) => return Err(error),
        }
    }
}

/// One open `StreamSearch` fan-out: a merged inbound lane plus the
/// per-shard signal lanes (authoritative stream sender + optional UDP token).
/// Shared by every streaming consumer — plain top-k, document mode,
/// and the decomposed hybrid — which differ only in what they do with
/// the batches and which floor they derive.
pub(crate) struct StreamFanout {
    pub(super) scoped: bool,
    // Own response readers so cancellation, setup failure and successful
    // completion all release the underlying RPC streams.
    pub(super) readers: tokio::task::JoinSet<()>,
    pub(super) merged_rx: mpsc::Receiver<(usize, Result<Option<StreamSearchResponse>, Status>)>,
    pub(super) floor_txs: Vec<Option<mpsc::Sender<StreamSearchRequest>>>,
    pub(super) udp_lanes: Vec<Option<(u64, std::net::SocketAddr)>>,
    pub(super) udp_socket: Option<Arc<std::net::UdpSocket>>,
    /// The signing key and a sequence shared by every lane of this
    /// fan-out (docs/security.md); a node ignores a sequence at or
    /// behind the newest it applied, so each token sees them ascend.
    pub(super) udp_key: Option<crate::security::UdpKey>,
    pub(super) udp_seq: Arc<std::sync::atomic::AtomicU32>,
}

impl StreamFanout {
    #[cfg(test)]
    pub(crate) async fn resolve_identities(
        &mut self,
        hits: &[MergedHit],
        scans: &[Option<StreamSearchSummary>],
        terminal: &mut [Option<StreamSearchSummary>],
        limits: &crate::pb::StreamIdentityLimits,
    ) -> Result<HashMap<(u32, u64), Option<crate::pb::DocumentIdentity>>, Status> {
        self.resolve_identities_and_candidates(hits, scans, terminal, limits)
            .await
            .map(|(identities, _)| identities)
    }

    #[allow(clippy::type_complexity)]
    pub(crate) async fn resolve_identities_and_candidates(
        &mut self,
        hits: &[MergedHit],
        scans: &[Option<StreamSearchSummary>],
        terminal: &mut [Option<StreamSearchSummary>],
        limits: &crate::pb::StreamIdentityLimits,
    ) -> Result<
        (
            HashMap<(u32, u64), Option<crate::pb::DocumentIdentity>>,
            HashMap<(u32, u64), Option<crate::pb::RerankCandidatePayload>>,
        ),
        Status,
    > {
        let exchange = async {
            let mut expected = vec![Vec::new(); self.floor_txs.len()];
            for hit in hits {
                expected[hit.shard as usize].push(hit.vector_id);
            }
            let mut remaining = 0usize;
            for (shard, tx) in self.floor_txs.iter().enumerate() {
                if let Some(tx) = tx {
                    remaining += 1;
                    tx.send(StreamSearchRequest {
                        payload: Some(stream_search_request::Payload::ResolveIdentities(
                            crate::pb::ResolveStreamIdentities {
                                vector_ids: expected[shard].clone(),
                            },
                        )),
                    })
                    .await
                    .map_err(|_| Status::unavailable("identity request stream closed"))?;
                }
            }
            let mut received = vec![false; expected.len()];
            let mut identities = HashMap::with_capacity(hits.len());
            let mut candidates = HashMap::with_capacity(hits.len());
            let mut payload_bytes = 0usize;
            while remaining > 0 {
                let Some((shard, message)) = self.next_message(terminal).await? else {
                    continue;
                };
                if terminal[shard].is_some() {
                    return Err(Status::internal("identity message after terminal summary"));
                }
                if matches!(
                    message.payload.as_ref(),
                    Some(stream_search_response::Payload::Identities(_))
                ) && prost::Message::encoded_len(&message) > limits.max_response_bytes as usize
                {
                    return Err(Status::resource_exhausted(
                        "shard exceeded the identity response budget",
                    ));
                }
                match message.payload {
                    Some(stream_search_response::Payload::Identities(found)) => {
                        if received[shard] || found.rows.len() != expected[shard].len() {
                            return Err(Status::internal(
                                "shard identity response count or sequence differs",
                            ));
                        }
                        for (row, expected_id) in found.rows.into_iter().zip(&expected[shard]) {
                            if row.vector_id != *expected_id {
                                return Err(Status::internal(
                                    "shard returned another ID's identity",
                                ));
                            }
                            let len = prost::Message::encoded_len(&row);
                            payload_bytes = payload_bytes
                                .checked_add(
                                    1 + prost::encoding::encoded_len_varint(len as u64) + len,
                                )
                                .ok_or_else(|| {
                                    Status::resource_exhausted("identity response length overflow")
                                })?;
                            let response_bytes = 1
                                + prost::encoding::encoded_len_varint(payload_bytes as u64)
                                + payload_bytes;
                            if response_bytes > limits.max_response_bytes as usize {
                                return Err(Status::resource_exhausted(
                                    "combined identity response exceeds max_response_bytes",
                                ));
                            }
                            identities.insert((shard as u32, row.vector_id), row.identity);
                            candidates.insert((shard as u32, row.vector_id), row.rerank_candidate);
                        }
                        received[shard] = true;
                    }
                    Some(stream_search_response::Payload::Summary(summary)) => {
                        if !received[shard] || Some(&summary) != scans[shard].as_ref() {
                            return Err(Status::failed_precondition(
                                "identity exchange did not certify the captured scan",
                            ));
                        }
                        terminal[shard] = Some(summary);
                        self.mark_completed(shard);
                        remaining -= 1;
                    }
                    _ => {
                        return Err(Status::internal(
                            "unexpected message during identity selection",
                        ))
                    }
                }
            }
            Ok((identities, candidates))
        };
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(u64::from(limits.timeout_ms)),
            exchange,
        )
        .await
        .unwrap_or_else(|_| Err(Status::deadline_exceeded("identity fan-out timed out")));
        match result {
            Ok(pair) => Ok(pair),
            Err(status) => self.cancel_with(status).await,
        }
    }

    /// Send one typed frame to a lane, signed when a key is configured.
    pub(super) fn send_signal(
        &self,
        socket: &std::net::UdpSocket,
        target: std::net::SocketAddr,
        frame: [u8; crate::stream_signal::FRAME_LEN],
    ) {
        match &self.udp_key {
            Some(key) => {
                let seq = self
                    .udp_seq
                    .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
                    + 1;
                let signed = crate::stream_signal::sign(key, seq, &frame);
                let _ = socket.send_to(&signed, target);
            }
            None => {
                let _ = socket.send_to(&frame, target);
            }
        }
    }

    pub(super) fn send_udp_cancel(&self) {
        let Some(socket) = self.udp_socket.as_deref() else {
            return;
        };
        for (shard, tx) in self.floor_txs.iter().enumerate() {
            if tx.is_none() {
                continue;
            }
            if let Some((token, target)) = self.udp_lanes[shard] {
                self.send_signal(socket, target, crate::stream_signal::encode_cancel(token));
            }
        }
    }

    /// Abandon every unfinished shard. UDP goes first for low latency; the
    /// matching gRPC Stop is enqueued within one bounded grace period. If a
    /// peer no longer drains requests, abort its response reader to close the
    /// RPC instead of waiting forever. Neither path certifies completion.
    pub(crate) async fn cancel(&mut self) {
        self.send_udp_cancel();
        let senders: Vec<mpsc::Sender<StreamSearchRequest>> =
            self.floor_txs.iter_mut().filter_map(Option::take).collect();
        send_stream_stops(senders).await;
        self.readers.abort_all();
    }

    pub(crate) async fn cancel_with<T>(&mut self, status: Status) -> Result<T, Status> {
        self.cancel().await;
        Err(status)
    }

    pub(crate) fn mark_completed(&mut self, shard: usize) {
        self.floor_txs[shard] = None;
        self.udp_lanes[shard] = None;
    }

    /// The next inbound message: `Ok(Some((shard, msg)))` for a payload,
    /// `Ok(None)` for a clean post-summary stream close (callers just
    /// continue), and an error for a shard failure or a close without a
    /// summary (a protocol break — the summary is the exactness
    /// certificate, so a stream that vanishes without one aborts the
    /// query).
    pub(crate) async fn next_message(
        &mut self,
        summaries: &[Option<StreamSearchSummary>],
    ) -> Result<Option<(usize, StreamSearchResponse)>, Status> {
        let Some((shard, item)) = self.merged_rx.recv().await else {
            return Err(Status::internal("stream fan-out ended without all shards"));
        };
        match item {
            Ok(Some(msg)) => Ok(Some((shard, msg))),
            Ok(None) => {
                if summaries[shard].is_none() {
                    return Err(Status::internal(format!(
                        "shard {shard} closed its stream without a summary"
                    )));
                }
                Ok(None)
            }
            Err(e) => Err(Status::new(
                if self.scoped {
                    e.code()
                } else {
                    tonic::Code::Internal
                },
                format!("shard {shard} failed: {e}"),
            )),
        }
    }
}

impl Drop for StreamFanout {
    fn drop(&mut self) {
        if self.floor_txs.iter().all(Option::is_none) {
            return;
        }
        self.send_udp_cancel();
        let senders: Vec<mpsc::Sender<StreamSearchRequest>> =
            self.floor_txs.iter_mut().filter_map(Option::take).collect();
        let readers = std::mem::take(&mut self.readers);
        let send_stops = async move {
            send_stream_stops(senders).await;
            drop(readers);
        };
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(send_stops);
        }
    }
}

/// One grace period for the entire fan-out, including full request lanes.
/// Dropping the owned senders and reader tasks then closes abandoned RPCs.
pub(super) async fn send_stream_stops(senders: Vec<mpsc::Sender<StreamSearchRequest>>) {
    let _ = tokio::time::timeout(Duration::from_millis(250), async move {
        let mut sends = tokio::task::JoinSet::new();
        for tx in senders {
            sends.spawn(async move {
                let _ = tx
                    .send(StreamSearchRequest {
                        payload: Some(stream_search_request::Payload::Stop(StopStreamSearch {})),
                    })
                    .await;
            });
        }
        while sends.join_next().await.is_some() {}
    })
    .await;
}

impl CoordinatorServiceImpl {
    /// Open one `StreamSearch` per shard: Start flows through a held
    /// sender that later carries floor raises or an authoritative Stop. Each
    /// stream also gets a UDP token so both signals reach the shard first on
    /// the fast lossy lane and then on the gRPC stream.
    pub(crate) fn open_stream_fanout(
        &self,
        request_id: &str,
        vector: &[f32],
        initial_floor: Option<f32>,
        candidate_depth: Option<u32>,
        collapse_parents: bool,
        filters: &RequestFilters,
    ) -> Result<StreamFanout, Status> {
        self.open_validated_stream(StreamOpen::new(
            request_id,
            vector,
            initial_floor,
            candidate_depth,
            collapse_parents,
            filters,
            None,
        )?)
    }

    /// `candidate_depth` is how many candidates the shard should consider
    /// for the query (the request's `k`): a configured-ANN provider walks
    /// its graph to it and refuses the scan without it (`docs/hnsw.md`).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn open_stream_fanout_with_identities(
        &self,
        request_id: &str,
        vector: &[f32],
        initial_floor: Option<f32>,
        candidate_depth: Option<u32>,
        collapse_parents: bool,
        filters: &RequestFilters,
        identity_limits: Option<crate::pb::StreamIdentityLimits>,
    ) -> Result<StreamFanout, Status> {
        self.open_stream_context(StreamOpen::new(
            request_id,
            vector,
            initial_floor,
            candidate_depth,
            collapse_parents,
            filters,
            identity_limits,
        )?)
    }

    pub(crate) fn open_stream_context(
        &self,
        query: StreamOpen<'_>,
    ) -> Result<StreamFanout, Status> {
        if query.identity_limits.is_none() {
            return self.open_stream_fanout(
                query.request_id,
                query.vector,
                query.initial_floor,
                query.candidate_depth,
                query.collapse_parents,
                query.filters,
            );
        }
        self.open_validated_stream(query)
    }

    fn open_validated_stream(&self, query: StreamOpen<'_>) -> Result<StreamFanout, Status> {
        let n_nodes = self.node_addrs.len();
        if n_nodes == 0 {
            return Err(Status::failed_precondition("no shard nodes configured"));
        }
        self.check_vector_scan(query.filters, query.collapse_parents)?;
        let admission = self.vector_read_barrier()?;
        let udp_socket = self.floor_socket().cloned();
        let (merged_tx, merged_rx) = mpsc::channel(4 * n_nodes);
        let mut floor_txs = Vec::with_capacity(n_nodes);
        let mut udp_lanes = Vec::with_capacity(n_nodes);
        let mut readers = tokio::task::JoinSet::new();
        let mask = self.vector_scan_mask(query.filters.tree.as_ref());
        let floor_shares = self.shard_floor_share_ppms(query.filters, mask.as_ref());
        for shard in 0..n_nodes {
            if mask.as_ref().is_some_and(|m| m.skipped[shard]) {
                floor_txs.push(None);
                udp_lanes.push(None);
                continue;
            }
            let client = self.node_client(&self.node_addrs[shard])?;
            let lane = self
                .floor_target(&self.node_addrs[shard])
                .map(|target| (floor_token(), target));
            let (req_tx, req_rx) = mpsc::channel(64);
            req_tx
                .try_send(
                    query.start(
                        admission
                            .as_ref()
                            .map(|barrier| barrier.context(shard))
                            .transpose()?,
                        lane.map_or(0, |(token, _)| token),
                        Self::shard_filter_tree(query.filters, mask.as_ref(), shard),
                        floor_shares.get(shard).copied().flatten(),
                    ),
                )
                .expect("fresh channel accepts the Start message");
            floor_txs.push(Some(req_tx));
            udp_lanes.push(lane);
            readers.spawn(
                StreamReader {
                    shard,
                    client,
                    requests: req_rx,
                    merged: merged_tx.clone(),
                    admission: admission.clone(),
                    deadline: self.limits().shard_deadline.filter(|_| admission.is_some()),
                }
                .run(),
            );
        }
        Ok(StreamFanout {
            scoped: admission.is_some(),
            readers,
            merged_rx,
            floor_txs,
            udp_lanes,
            udp_socket,
            udp_key: self.udp_hmac_key.clone(),
            udp_seq: Arc::new(std::sync::atomic::AtomicU32::new(0)),
        })
    }

    /// Push a floor raise to every still-open stream of `fanout`: UDP
    /// first (the fast lossy copy), then the reliable stream. Both are
    /// monotone max-folds shard-side, so double delivery and loss are
    /// equally free; a full stream channel just means the next raise
    /// supersedes this one.
    pub(crate) fn push_stream_floor(&self, fanout: &StreamFanout, floor: f32) {
        let update = StreamSearchRequest {
            payload: Some(stream_search_request::Payload::FloorUpdate(FloorUpdate {
                floor,
            })),
        };
        for (si, tx) in fanout.floor_txs.iter().enumerate() {
            let Some(tx) = tx.as_ref() else {
                continue;
            };
            if let (Some(socket), Some((token, target))) =
                (fanout.udp_socket.as_deref(), fanout.udp_lanes[si])
            {
                fanout.send_signal(
                    socket,
                    target,
                    crate::stream_signal::encode_floor(token, floor),
                );
            }
            let _ = tx.try_send(update.clone());
        }
    }

    /// Push a graceful finish request (`FinishStreamSearch` + advisory UDP
    /// `FINISH`) to every shard of `fanout` that has not yet delivered its
    /// scan summary. Unlike `cancel`, finished ANN shards flush buffered hits
    /// `>= floor` and report `completed = true` so `IdentityReady` succeeds.
    pub(crate) async fn push_stream_finish(
        &self,
        fanout: &StreamFanout,
        done: &[Option<StreamSearchSummary>],
        deadline: bool,
    ) {
        let req = StreamSearchRequest {
            payload: Some(stream_search_request::Payload::Finish(
                crate::pb::FinishStreamSearch { deadline },
            )),
        };
        let mut pending = Vec::new();
        for (si, tx) in fanout.floor_txs.iter().enumerate() {
            if done.get(si).is_some_and(Option::is_some) {
                continue;
            }
            let Some(tx) = tx.as_ref() else {
                continue;
            };
            if let (Some(socket), Some((token, target))) =
                (fanout.udp_socket.as_deref(), fanout.udp_lanes[si])
            {
                fanout.send_signal(
                    socket,
                    target,
                    crate::stream_signal::encode_finish(token, deadline),
                );
            }
            match tx.try_send(req.clone()) {
                Ok(()) | Err(mpsc::error::TrySendError::Closed(_)) => {}
                Err(mpsc::error::TrySendError::Full(_)) => pending.push(tx.clone()),
            }
        }
        for tx in pending {
            let _ = tokio::time::timeout(Duration::from_millis(50), tx.send(req.clone())).await;
        }
    }

    pub(super) fn resolve_finish_windows(
        &self,
        filters: &RequestFilters,
    ) -> Result<(Option<Duration>, Option<Duration>), Status> {
        let parse_opt = |name: &str, raw: Option<u32>, fallback: Option<Duration>| match raw {
            Some(us) => {
                if !(1..=crate::dense_policy::MAX_FINISH_SETTLE_US).contains(&us) {
                    return Err(Status::invalid_argument(format!(
                        "{name} {us} is outside 1..={}",
                        crate::dense_policy::MAX_FINISH_SETTLE_US
                    )));
                }
                Ok(Some(Duration::from_micros(u64::from(us))))
            }
            None => Ok(fallback),
        };
        Ok((
            parse_opt(
                "finish_settle_us",
                filters.finish_settle_us,
                self.stream_finish_settle,
            )?,
            parse_opt(
                "finish_deadline_us",
                filters.finish_deadline_us,
                self.stream_finish_deadline,
            )?,
        ))
    }

    /// Best-effort refresh of per-shard row counts (`self.shard_vector_rows`)
    /// when a caller invokes a fan-out helper directly without going through
    /// `SearchService::search` or `resolve_dense_execution`. Errors on
    /// unreachable or unconfigured shards are ignored here so the subsequent
    /// RPC surfaces its canonical failure untouched.
    pub(crate) async fn ensure_shard_vector_rows(&self) {
        self.shard_vector_row_probe().wait().await;
    }

    /// Per-shard proportional share (`0..=1_000_000`) of the parent's
    /// `floor_share_ppm` across active (non-skipped) shards (`#72`).
    pub(crate) fn shard_floor_share_ppms(
        &self,
        filters: &RequestFilters,
        mask: Option<&crate::placement::ShardMask>,
    ) -> Vec<Option<u32>> {
        let n_nodes = self.node_addrs.len();
        let active: Vec<usize> = (0..n_nodes)
            .filter(|&shard| !mask.is_some_and(|m| m.skipped[shard]))
            .collect();
        let mut out = vec![None; n_nodes];
        if active.is_empty() {
            return out;
        }
        let parent_share = u128::from(filters.parent_floor_share_ppm.unwrap_or(1_000_000));
        let cached: Option<Vec<(usize, u64)>> = {
            let cache = self
                .shard_vector_rows
                .lock()
                .expect("shard vector row cache poisoned");
            active
                .iter()
                .map(|&shard| {
                    cache
                        .get(&self.node_addrs[shard])
                        .map(|&rows| (shard, rows))
                })
                .collect()
        };
        if let Some(rows_by_shard) = cached {
            let total_rows: u128 = rows_by_shard
                .iter()
                .map(|&(_, rows)| u128::from(rows))
                .sum();
            for (shard, rows) in rows_by_shard {
                let share = if total_rows == 0 || rows == 0 {
                    0
                } else {
                    (parent_share * u128::from(rows))
                        .div_ceil(total_rows)
                        .min(1_000_000) as u32
                };
                out[shard] = Some(share);
            }
        } else {
            let uniform = parent_share.div_ceil(active.len() as u128).min(1_000_000) as u32;
            for shard in active {
                out[shard] = Some(uniform);
            }
        }
        out
    }
}

//! The open stream fan-out: signal lanes, cancellation and inbound messages.

use super::*;

/// One open `StreamSearch` fan-out: a merged inbound lane plus the
/// per-shard signal lanes (authoritative stream sender + optional UDP token).
/// Shared by every streaming consumer — plain top-k, document mode,
/// and the decomposed hybrid — which differ only in what they do with
/// the batches and which floor they derive.
pub(crate) struct StreamFanout {
    pub(in crate::coordinator) scoped: bool,
    // Own response readers so cancellation, setup failure and successful
    // completion all release the underlying RPC streams.
    pub(in crate::coordinator) readers: tokio::task::JoinSet<()>,
    pub(in crate::coordinator) merged_rx:
        mpsc::Receiver<(usize, Result<Option<StreamSearchResponse>, Status>)>,
    pub(in crate::coordinator) floor_txs: Vec<Option<mpsc::Sender<StreamSearchRequest>>>,
    pub(in crate::coordinator) udp_lanes: Vec<Option<(u64, std::net::SocketAddr)>>,
    pub(in crate::coordinator) udp_socket: Option<Arc<std::net::UdpSocket>>,
    /// Signing key and monotonic sequence shared across all UDP lanes of this
    /// fan-out; nodes ignore sequences at or behind the newest applied frame.
    pub(in crate::coordinator) udp_key: Option<crate::security::UdpKey>,
    pub(in crate::coordinator) udp_seq: Arc<std::sync::atomic::AtomicU32>,
}

impl StreamFanout {
    /// Send one typed frame to a lane, signed when a key is configured.
    pub(in crate::coordinator) fn send_signal(
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

    pub(in crate::coordinator) fn send_udp_cancel(&self) {
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
pub(in crate::coordinator) async fn send_stream_stops(
    senders: Vec<mpsc::Sender<StreamSearchRequest>>,
) {
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

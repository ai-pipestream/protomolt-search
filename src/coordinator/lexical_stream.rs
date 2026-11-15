//! BM25 candidate streams and certified shard responses.

use super::*;

/// A process-unique, well-mixed stream token for the UDP signal lane
/// (0 is reserved for "no UDP"). Tokens route datagrams to in-flight
/// streams on a trusted network; they are unique, not secret.
pub(super) fn floor_token() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let raw = (u64::from(std::process::id()) << 32) ^ NEXT.fetch_add(1, AtomicOrdering::Relaxed);
    // splitmix64 finalizer.
    let mut z = raw.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^= z >> 31;
    z.max(1)
}

/// A stale lifetime or epoch invalidates cached shares. Retry once with fresh
/// statistics and their complete claim; a second mutation returns the refusal.
/// Never drop the fence to manufacture success under continuous writes.
pub(crate) fn is_stale_stats(status: &Status) -> bool {
    crate::stats_identity::is_stale_stats_epoch(status)
}

pub(super) struct Bm25StreamHeap {
    pub(super) heap: std::collections::BinaryHeap<crate::merge::LexicalHeapEntry>,
    pub(super) floors_sent: u64,
    pub(super) progress: Option<Arc<QueryProgressPublisher>>,
}

pub(super) struct Bm25StreamShardResult {
    pub(super) response: Bm25QueryResponse,
    pub(super) scoring_fingerprint: String,
}

/// Receive compact lexical candidates and require a matching completion certificate.
pub(super) async fn stream_bm25_shard(
    shard: u32,
    k: usize,
    mut client: crate::link::NodeLink,
    request: Bm25QueryRequest,
    floor_tx: Arc<watch::Sender<f32>>,
    floor_rx: watch::Receiver<f32>,
    global_heap: Arc<Mutex<Bm25StreamHeap>>,
) -> Result<Bm25StreamShardResult, Status> {
    let (out_tx, out_rx) = mpsc::channel::<Bm25QueryStreamRequest>(8);
    out_tx
        .send(Bm25QueryStreamRequest {
            payload: Some(bm25_query_stream_request::Payload::Start(request)),
        })
        .await
        .map_err(|_| Status::internal("bm25 stream request channel closed before start"))?;
    let mut inbound = client
        .bm25_query_stream(ReceiverStream::new(out_rx))
        .await?
        .into_inner();
    let forwarder = tokio::spawn(forward_floors(out_tx.clone(), floor_rx));
    let mut state = LexicalStream {
        shard,
        k,
        received: 0,
        floor_tx,
        global_heap,
    };
    let result = state.receive(&mut inbound).await;
    if result.is_err() {
        let _ = out_tx
            .send(Bm25QueryStreamRequest {
                payload: Some(bm25_query_stream_request::Payload::Stop(
                    crate::pb::StopBm25Query {},
                )),
            })
            .await;
    }
    forwarder.abort();
    result
}

async fn forward_floors(
    out: mpsc::Sender<Bm25QueryStreamRequest>,
    mut floors: watch::Receiver<f32>,
) {
    let mut last = *floors.borrow_and_update();
    while floors.changed().await.is_ok() {
        let floor = *floors.borrow_and_update();
        if floor > last {
            last = floor;
            let message = Bm25QueryStreamRequest {
                payload: Some(bm25_query_stream_request::Payload::FloorUpdate(
                    FloorUpdate { floor },
                )),
            };
            if out.send(message).await.is_err() {
                break;
            }
        }
    }
}

struct LexicalStream {
    shard: u32,
    k: usize,
    received: u64,
    floor_tx: Arc<watch::Sender<f32>>,
    global_heap: Arc<Mutex<Bm25StreamHeap>>,
}

impl LexicalStream {
    async fn receive(
        &mut self,
        inbound: &mut crate::link::LinkStream<Bm25QueryStreamResponse>,
    ) -> Result<Bm25StreamShardResult, Status> {
        use bm25_query_stream_response::Payload;
        loop {
            let message = inbound.message().await?.ok_or_else(|| {
                Status::data_loss(format!(
                    "shard {}: BM25 stream ended without a completion certificate",
                    self.shard
                ))
            })?;
            match message.payload {
                Some(Payload::FloorUpdate(update)) => self.raise(update.floor),
                Some(Payload::CandidateBatch(batch)) => self.candidates(&batch.candidates)?,
                Some(Payload::Completion(completion)) => return self.complete(completion),
                Some(Payload::Done(_)) => {
                    return Err(Status::failed_precondition(format!(
                        "shard {}: BM25 stream used the obsolete uncertified terminal response",
                        self.shard
                    )))
                }
                None => {}
            }
        }
    }

    fn raise(&self, floor: f32) {
        self.floor_tx.send_if_modified(|held| {
            if !floor.is_nan() && floor > *held {
                *held = floor;
                true
            } else {
                false
            }
        });
    }

    fn candidates(&mut self, bytes: &[u8]) -> Result<(), Status> {
        if !bytes.len().is_multiple_of(12) {
            return Err(Status::data_loss(format!(
                "shard {}: BM25 candidate batch has {} bytes, not 12-byte records",
                self.shard,
                bytes.len()
            )));
        }
        if bytes.as_chunks::<12>().0.iter().any(|record| {
            !f32::from_le_bytes(record[8..12].try_into().expect("4-byte score")).is_finite()
        }) {
            return Err(Status::data_loss(format!(
                "shard {}: BM25 candidate batch contains a non-finite score",
                self.shard
            )));
        }
        self.received += (bytes.len() / 12) as u64;
        let mut state = self.global_heap.lock().expect("BM25 stream heap poisoned");
        for record in bytes.as_chunks::<12>().0 {
            if self.k == 0 {
                continue;
            }
            let entry = crate::merge::LexicalHeapEntry(MergedHit {
                shard: self.shard,
                vector_id: u64::from_le_bytes(record[..8].try_into().expect("8-byte id")),
                score: f32::from_le_bytes(record[8..12].try_into().expect("4-byte score")),
            });
            if state.heap.len() < self.k {
                state.heap.push(entry);
            } else if entry < *state.heap.peek().expect("full heap") {
                state.heap.pop();
                state.heap.push(entry);
            }
        }
        let floor = (self.k > 0 && state.heap.len() == self.k)
            .then(|| state.heap.peek().expect("full heap").0.score.next_down());
        let raised = floor.filter(|floor| *floor > *self.floor_tx.borrow());
        if raised.is_some() {
            state.floors_sent += 1;
        }
        let progress = state.progress.clone();
        let snapshot = lexical_snapshot(&state);
        drop(state);
        publish_lexical_progress(progress.as_deref(), snapshot, String::new());
        if let Some(floor) = raised {
            self.raise(floor);
        }
        Ok(())
    }

    fn complete(
        &self,
        completion: crate::pb::Bm25StreamCompletion,
    ) -> Result<Bm25StreamShardResult, Status> {
        if !completion.completed {
            return Err(Status::cancelled(format!(
                "shard {}: BM25 scan did not complete",
                self.shard
            )));
        }
        if completion.scoring_fingerprint.is_empty() {
            return Err(Status::data_loss(format!(
                "shard {}: BM25 completion omitted scoring fingerprint",
                self.shard
            )));
        }
        if completion.candidates_emitted != self.received {
            return Err(Status::data_loss(format!(
                "shard {}: completion counted {} candidates but {} arrived",
                self.shard, completion.candidates_emitted, self.received
            )));
        }
        let response = completion.response.ok_or_else(|| {
            Status::data_loss(format!(
                "shard {}: BM25 completion omitted its response",
                self.shard
            ))
        })?;
        Ok(Bm25StreamShardResult {
            response,
            scoring_fingerprint: completion.scoring_fingerprint,
        })
    }
}

pub(super) fn lexical_snapshot(state: &Bm25StreamHeap) -> Vec<(u64, f32)> {
    let mut hits: Vec<_> = state.heap.iter().map(|entry| entry.0).collect();
    hits.sort_by(crate::merge::cmp_lexical_hits);
    hits.into_iter()
        .map(|hit| (hit.vector_id, hit.score))
        .collect()
}

pub(super) fn publish_lexical_progress(
    progress: Option<&QueryProgressPublisher>,
    hits: Vec<(u64, f32)>,
    scoring_fingerprint: String,
) {
    if let Some(progress) = progress {
        let next = QueryProgress {
            phase: crate::pb::QueryStreamPhase::Lexical,
            hits,
            scoring_fingerprint,
        };
        progress.sender.send_if_modified(|current| {
            if current.as_ref() == Some(&next) {
                false
            } else {
                *current = Some(next);
                true
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stream(shard: u32, heap: Arc<Mutex<Bm25StreamHeap>>) -> LexicalStream {
        let (tx, _) = watch::channel(f32::NEG_INFINITY);
        LexicalStream {
            shard,
            k: 1,
            received: 0,
            floor_tx: Arc::new(tx),
            global_heap: heap,
        }
    }

    fn heap() -> Arc<Mutex<Bm25StreamHeap>> {
        Arc::new(Mutex::new(Bm25StreamHeap {
            heap: Default::default(),
            floors_sent: 0,
            progress: None,
        }))
    }

    fn candidate(doc: u64, score: f32) -> Vec<u8> {
        [doc.to_le_bytes().as_slice(), score.to_le_bytes().as_slice()].concat()
    }

    #[test]
    fn negative_candidates_raise_signed_live_floors() {
        let mut stream = stream(0, heap());
        for score in [-5.0_f32, -1.0, 0.0] {
            stream.candidates(&candidate(0, score)).unwrap();
            assert_eq!(*stream.floor_tx.borrow(), score.next_down());
        }
        stream.raise(-100.0);
        assert_eq!(*stream.floor_tx.borrow(), 0.0_f32.next_down());
    }

    #[test]
    fn signed_zero_candidates_tie_by_shard_then_document() {
        let shared = heap();
        let mut second = stream(1, shared.clone());
        second.candidates(&candidate(2, 0.0)).unwrap();
        let mut first = stream(0, shared.clone());
        first.candidates(&candidate(9, -0.0)).unwrap();
        let held = shared.lock().unwrap();
        let winner = held.heap.peek().unwrap().0;
        assert_eq!((winner.shard, winner.vector_id), (0, 9));
        assert_eq!(winner.score.to_bits(), (-0.0_f32).to_bits());
    }
}

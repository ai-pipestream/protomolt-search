//! Lexical stream.

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
    status.code() == tonic::Code::FailedPrecondition
        && status.message().starts_with(crate::node::STALE_STATS_EPOCH)
}

pub(super) struct Bm25StreamHeap {
    pub(super) heap: std::collections::BinaryHeap<StreamHeapEntry>,
    pub(super) floors_sent: u64,
    pub(super) progress: Option<Arc<QueryProgressPublisher>>,
}

pub(super) struct Bm25StreamShardResult {
    pub(super) response: Bm25QueryResponse,
    pub(super) scoring_fingerprint: String,
}

/// One shard's leg of the exact lexical candidate stream. Compact
/// candidates feed the coordinator-owned global heap immediately; its
/// emission-safe k-th score shares the same conflated watch cell as local
/// shard floors. The terminal completion supplies rich local top-k details
/// for the global winners and certifies that the whole shard was scanned.
pub(super) async fn stream_bm25_shard(
    shard: u32,
    k: usize,
    mut client: crate::link::NodeLink,
    request: Bm25QueryRequest,
    floor_tx: Arc<watch::Sender<f32>>,
    mut floor_rx: watch::Receiver<f32>,
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
    let control_tx = out_tx.clone();
    // Forward only raises above what this stream last saw. Conflation
    // is the watch cell itself: a burst of raises collapses to whatever
    // is newest when the forwarder wakes, and a dropped intermediate
    // value loses nothing because floors are monotone.
    let forwarder = tokio::spawn(async move {
        let mut last = *floor_rx.borrow_and_update();
        while floor_rx.changed().await.is_ok() {
            let floor = *floor_rx.borrow_and_update();
            if floor > last {
                last = floor;
                let update = Bm25QueryStreamRequest {
                    payload: Some(bm25_query_stream_request::Payload::FloorUpdate(
                        FloorUpdate { floor },
                    )),
                };
                if out_tx.send(update).await.is_err() {
                    break;
                }
            }
        }
    });
    let mut received = 0u64;
    let result = loop {
        match inbound.message().await {
            Ok(Some(Bm25QueryStreamResponse {
                payload: Some(bm25_query_stream_response::Payload::FloorUpdate(u)),
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
            Ok(Some(Bm25QueryStreamResponse {
                payload: Some(bm25_query_stream_response::Payload::CandidateBatch(batch)),
            })) => {
                if batch.candidates.len() % 12 != 0 {
                    break Err(Status::data_loss(format!(
                        "shard {shard}: BM25 candidate batch has {} bytes, not 12-byte records",
                        batch.candidates.len()
                    )));
                }
                if batch.candidates.as_chunks::<12>().0.iter().any(|rec| {
                    !f32::from_le_bytes(rec[8..12].try_into().expect("4-byte score")).is_finite()
                }) {
                    break Err(Status::data_loss(format!(
                        "shard {shard}: BM25 candidate batch contains a non-finite score"
                    )));
                }
                received += (batch.candidates.len() / 12) as u64;
                let mut state = global_heap.lock().expect("BM25 stream heap poisoned");
                let mut raised = None;
                for rec in batch.candidates.as_chunks::<12>().0 {
                    if k == 0 {
                        continue;
                    }
                    let entry = StreamHeapEntry(MergedHit {
                        vector_id: u64::from_le_bytes(rec[..8].try_into().expect("8-byte id")),
                        shard,
                        score: f32::from_le_bytes(rec[8..12].try_into().expect("4-byte score")),
                    });
                    if state.heap.len() < k {
                        state.heap.push(entry);
                    } else if cmp_hits(&entry.0, &state.heap.peek().expect("heap is full").0)
                        == std::cmp::Ordering::Less
                    {
                        state.heap.pop();
                        state.heap.push(entry);
                    }
                }
                if state.heap.len() == k {
                    let floor =
                        crate::bm25::floor_seed(state.heap.peek().expect("heap is full").0.score);
                    if floor > *floor_tx.borrow() {
                        state.floors_sent += 1;
                        raised = Some(floor);
                    }
                }
                let progress = state.progress.clone();
                let mut snapshot: Vec<(u64, f32)> = state
                    .heap
                    .iter()
                    .map(|entry| (entry.0.vector_id, entry.0.score))
                    .collect();
                snapshot.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
                drop(state);
                if let Some(progress) = progress {
                    let next = QueryProgress {
                        phase: crate::pb::QueryStreamPhase::Lexical,
                        hits: snapshot,
                        scoring_fingerprint: String::new(),
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
                if let Some(floor) = raised {
                    floor_tx.send_if_modified(|cur| {
                        if floor > *cur {
                            *cur = floor;
                            true
                        } else {
                            false
                        }
                    });
                }
            }
            Ok(Some(Bm25QueryStreamResponse {
                payload: Some(bm25_query_stream_response::Payload::Completion(completion)),
            })) => {
                if !completion.completed {
                    break Err(Status::cancelled(format!(
                        "shard {shard}: BM25 scan did not complete"
                    )));
                }
                if completion.scoring_fingerprint.is_empty() {
                    break Err(Status::data_loss(format!(
                        "shard {shard}: BM25 completion omitted scoring fingerprint"
                    )));
                }
                if completion.candidates_emitted != received {
                    break Err(Status::data_loss(format!(
                        "shard {shard}: completion counted {} candidates but {} arrived",
                        completion.candidates_emitted, received
                    )));
                }
                let Some(response) = completion.response else {
                    break Err(Status::data_loss(format!(
                        "shard {shard}: BM25 completion omitted its response"
                    )));
                };
                break Ok(Bm25StreamShardResult {
                    response,
                    scoring_fingerprint: completion.scoring_fingerprint,
                });
            }
            Ok(Some(Bm25QueryStreamResponse {
                payload: Some(bm25_query_stream_response::Payload::Done(_)),
            })) => {
                break Err(Status::failed_precondition(format!(
                    "shard {shard}: BM25 stream used the obsolete uncertified terminal response"
                )));
            }
            // Empty payload: ignore (forward compatibility).
            Ok(Some(_)) => {}
            Ok(None) => {
                break Err(Status::data_loss(format!(
                    "shard {shard}: BM25 stream ended without a completion certificate"
                )))
            }
            Err(e) => break Err(e),
        }
    };
    if result.is_err() {
        let _ = control_tx
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

//! Stream search.

use super::*;

impl CoordinatorServiceImpl {
    /// [`Self::fanout_search`] over the streaming protocol
    /// (`NodeService.StreamSearch`): shards hold no top-k and emit
    /// every candidate at or above the live floor; the heap here — the
    /// only one in the system — defines k. Whenever its k-th best
    /// tightens, `floor_seed(kth)` (one f32 ULP below, so boundary ties
    /// survive) is pushed to every open stream.
    ///
    /// Exactness: the successful path never sends Stop, every shard's
    /// terminal summary must certify `completed = true`, every emission
    /// scored at or above the floor in effect when its block was scanned,
    /// and every pushed floor is a lower bound on the global k-th best. An
    /// error cancels unfinished streams, whose summaries are necessarily
    /// incomplete and unusable. Successful results are identical to
    /// [`Self::fanout_search`] (same scores, same `merge_topk` total order).
    ///
    /// `initial_floor` seeds every shard's starting floor — the hybrid
    /// seam, where a finished BM25 leg's decomposed floor prunes the
    /// vector scan from the first block.
    pub async fn fanout_stream_search(
        &self,
        request_id: &str,
        vector: &[f32],
        k: u32,
        initial_floor: Option<f32>,
        filters: &RequestFilters,
    ) -> Result<StreamFanoutResult, Status> {
        self.check_vector_scan(filters, false)?;
        if self.scoped_vector_scan() && self.query_read_versions.is_none() {
            let (pinned, reads) = self.pin_read_versions().await?;
            let result = Box::pin(pinned.fanout_stream_search(
                request_id,
                vector,
                k,
                initial_floor,
                filters,
            ))
            .await;
            pinned.validate_read_versions(&reads).await?;
            return result;
        }
        let n_nodes = self.node_addrs.len();
        if n_nodes == 0 {
            return Err(Status::failed_precondition("no shard nodes configured"));
        }
        if initial_floor.is_some_and(f32::is_nan) {
            return Err(Status::invalid_argument("initial_floor must not be NaN"));
        }
        // Without k the heap never fills, no floor ever rises, and
        // every shard would emit itself entirely for nothing.
        let (settle_window, deadline_window) = self.resolve_finish_windows(filters)?;
        if k == 0 {
            if self.scoped_vector_scan() {
                return Err(Status::invalid_argument(
                    "a scoped vector scan requires positive k",
                ));
            }
            return Ok(StreamFanoutResult {
                hits: Vec::new(),
                summaries: Vec::new(),
                floors_sent: 0,
                shards_finished_early: 0,
                shards_floor_pruned: 0,
            });
        }

        self.ensure_shard_vector_rows().await;
        let mask = self.vector_scan_mask(filters.tree.as_ref());
        let mut known = Self::filter_known(filters, mask.as_ref());
        let identity_limits = crate::pb::StreamIdentityLimits {
            max_rows: k,
            max_response_bytes: 32 * 1024 * 1024,
            timeout_ms: 60_000,
        };
        crate::query_identity::validate_limits(&identity_limits)?;
        let mut fanout = self.open_stream_fanout_with_identities(
            request_id,
            vector,
            initial_floor,
            Some(k),
            false,
            filters,
            Some(identity_limits),
        )?;

        // The global top-k: a max-heap whose top is the WORST survivor
        // under the merge's total order, so peek() is the k-th best.
        let mut heap: std::collections::BinaryHeap<StreamHeapEntry> =
            std::collections::BinaryHeap::with_capacity(k as usize + 1);
        let mut seen_rows: std::collections::HashSet<(u32, u64)> =
            std::collections::HashSet::with_capacity(k as usize * 2);
        let mut summaries: Vec<Option<StreamSearchSummary>> = vec![None; n_nodes];
        let mut remaining = n_nodes;
        for (shard, summary) in summaries.iter_mut().enumerate() {
            if mask.as_ref().is_some_and(|m| m.skipped[shard]) {
                *summary = Some(StreamSearchSummary {
                    completed: true,
                    ..Default::default()
                });
                remaining -= 1;
            }
        }
        let mut terminal = summaries.clone();
        let active_shards = remaining;
        let quorum = finish_quorum(active_shards);
        let mut completed_active = 0usize;
        let mut finish_sent = false;
        let mut settle_at: Option<tokio::time::Instant> = None;
        let mut deadline_at: Option<tokio::time::Instant> =
            deadline_window.map(|d| tokio::time::Instant::now() + d);
        let mut last_floor = initial_floor.unwrap_or(f32::NEG_INFINITY);
        let mut floors_sent = 0u64;
        let mut scoring_fingerprint: Option<String> = None;
        enum StreamLoopStep {
            Settle,
            Deadline,
            Message(Result<Option<(usize, StreamSearchResponse)>, Status>),
        }
        while remaining > 0 {
            let step = tokio::select! {
                biased;
                _ = async { tokio::time::sleep_until(deadline_at.expect("guarded")).await }, if deadline_at.is_some() => StreamLoopStep::Deadline,
                _ = async { tokio::time::sleep_until(settle_at.expect("guarded")).await }, if settle_at.is_some() => StreamLoopStep::Settle,
                msg = fanout.next_message(&terminal) => StreamLoopStep::Message(msg),
            };
            let (shard, msg) = match step {
                StreamLoopStep::Deadline => {
                    deadline_at = None;
                    settle_at = None;
                    finish_sent = true;
                    self.push_stream_finish(&fanout, &summaries, true).await;
                    continue;
                }
                StreamLoopStep::Settle => {
                    settle_at = None;
                    if !finish_sent {
                        finish_sent = true;
                        self.push_stream_finish(&fanout, &summaries, false).await;
                    }
                    continue;
                }
                StreamLoopStep::Message(Ok(Some(pair))) => pair,
                StreamLoopStep::Message(Ok(None)) => continue,
                StreamLoopStep::Message(Err(status)) => return fanout.cancel_with(status).await,
            };
            match msg.payload {
                Some(stream_search_response::Payload::Batch(batch)) => {
                    if summaries[shard].is_some() {
                        return fanout
                            .cancel_with(Status::internal("candidate batch after IdentityReady"))
                            .await;
                    }
                    // Packed 12-byte LE records: u64 global id, f32
                    // score (see StreamSearchBatch).
                    if batch.hits.len() % 12 != 0 {
                        let status = Status::internal(format!(
                            "shard {shard} sent a misaligned batch of {} bytes",
                            batch.hits.len()
                        ));
                        return fanout.cancel_with(status).await;
                    }
                    let mut heap_changed = false;
                    for rec in batch.hits.as_chunks::<12>().0 {
                        let vector_id = u64::from_le_bytes(rec[..8].try_into().expect("8-byte id"));
                        if !seen_rows.insert((shard as u32, vector_id)) {
                            continue;
                        }
                        let entry = StreamHeapEntry(MergedHit {
                            vector_id,
                            shard: shard as u32,
                            score: f32::from_le_bytes(rec[8..12].try_into().expect("4-byte score")),
                        });
                        if heap.len() < k as usize {
                            heap.push(entry);
                            heap_changed = true;
                        } else if cmp_hits(&entry.0, &heap.peek().expect("heap is full").0)
                            == std::cmp::Ordering::Less
                        {
                            heap.pop();
                            heap.push(entry);
                            heap_changed = true;
                        }
                    }
                    // Full heap: its worst survivor bounds the global
                    // k-th best from below; seed one ULP down so shard
                    // ties at the boundary keep flowing. next_down, not
                    // bm25::floor_seed: vector scores are SIGNED (dot
                    // products), and clamping a negative k-th best to 0
                    // would push a floor ABOVE it — a recall bug on any
                    // corpus whose top-k dips below zero.
                    if heap.len() == k as usize {
                        let kth = heap.peek().expect("heap is full").0.score;
                        let floor = kth.next_down();
                        if floor > last_floor {
                            last_floor = floor;
                            floors_sent += 1;
                            self.push_stream_floor(&fanout, floor);
                        }
                        if !finish_sent && completed_active >= quorum {
                            if let Some(window) = settle_window {
                                if heap_changed || settle_at.is_none() {
                                    settle_at = Some(tokio::time::Instant::now() + window);
                                }
                            }
                        }
                    }
                    self.publish_progress(
                        crate::pb::QueryStreamPhase::Dense,
                        heap.iter()
                            .map(|entry| (entry.0.vector_id, entry.0.score))
                            .collect(),
                        scoring_fingerprint.clone().unwrap_or_default(),
                    );
                }
                Some(stream_search_response::Payload::IdentityReady(ready)) => {
                    if summaries[shard].is_some() {
                        return fanout
                            .cancel_with(Status::internal("duplicate IdentityReady"))
                            .await;
                    }
                    let Some(summary) = ready.scan else {
                        return fanout
                            .cancel_with(Status::internal("IdentityReady has no scan certificate"))
                            .await;
                    };
                    if !summary.completed {
                        let status = Status::internal(format!(
                            "shard {shard} stopped before completing its scan"
                        ));
                        return fanout.cancel_with(status).await;
                    }
                    if summary.scoring_fingerprint.is_empty() {
                        let status = Status::failed_precondition(format!(
                            "shard {shard} completed without a vector scoring fingerprint"
                        ));
                        return fanout.cancel_with(status).await;
                    }
                    match scoring_fingerprint.as_ref() {
                        Some(expected) if expected != &summary.scoring_fingerprint => {
                            let status = Status::failed_precondition(format!(
                                "shard {shard} vector scoring fingerprint {} differs from {expected}",
                                summary.scoring_fingerprint
                            ));
                            return fanout.cancel_with(status).await;
                        }
                        None => scoring_fingerprint = Some(summary.scoring_fingerprint.clone()),
                        _ => {}
                    }
                    // The vector leg's half of the typo handshake: a
                    // filter column no shard resolves must refuse even
                    // when the stream completed cleanly.
                    if let Err(e) = known.merge_shard(
                        shard,
                        &summary.geo_columns_known,
                        &summary.filter_columns_known,
                    ) {
                        return fanout.cancel_with(e).await;
                    }
                    summaries[shard] = Some(summary);
                    completed_active += 1;
                    remaining -= 1;
                    if remaining > 0
                        && !finish_sent
                        && settle_at.is_none()
                        && heap.len() == k as usize
                        && completed_active >= quorum
                    {
                        if let Some(window) = settle_window {
                            settle_at = Some(tokio::time::Instant::now() + window);
                        }
                    }
                }
                Some(stream_search_response::Payload::Summary(_)) => {
                    return fanout
                        .cancel_with(Status::failed_precondition(
                            "stream ended without snapshot-bound identity support or completion",
                        ))
                        .await;
                }
                Some(stream_search_response::Payload::Identities(_))
                | Some(stream_search_response::Payload::ReadReady(_))
                | None => {
                    return fanout
                        .cancel_with(Status::internal("unexpected message before IdentityReady"))
                        .await;
                }
            }
        }

        known.refuse_unknown(filters)?;

        self.publish_progress(
            crate::pb::QueryStreamPhase::Dense,
            heap.iter()
                .map(|entry| (entry.0.vector_id, entry.0.score))
                .collect(),
            scoring_fingerprint.unwrap_or_default(),
        );

        let mut all: Vec<MergedHit> = heap.into_iter().map(|e| e.0).collect();
        all.sort_by(cmp_hits);
        let (identities, candidates) = fanout
            .resolve_identities_and_candidates(&all, &summaries, &mut terminal, &identity_limits)
            .await?;
        let summaries: Vec<StreamSearchSummary> = summaries
            .into_iter()
            .map(|s| s.expect("all summaries arrived"))
            .collect();
        let shards_finished_early = count_shards_finished_early(&summaries);
        let shards_floor_pruned = count_shards_floor_pruned(&summaries);
        Ok(StreamFanoutResult {
            hits: all
                .into_iter()
                .map(|h| ScoredHit {
                    vector_id: h.vector_id,
                    score: h.score,
                    parent_id: 0,
                    identity: identities.get(&(h.shard, h.vector_id)).cloned().flatten(),
                    rerank_candidate: candidates.get(&(h.shard, h.vector_id)).cloned().flatten(),
                })
                .collect(),
            summaries,
            floors_sent,
            shards_finished_early,
            shards_floor_pruned,
        })
    }
}

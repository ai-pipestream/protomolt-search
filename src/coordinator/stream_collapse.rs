//! Stream collapse.

use super::*;

impl CoordinatorServiceImpl {
    /// Document-mode streaming: [`Self::fanout_stream_search`] with
    /// `k` meaning k distinct PARENT documents. Shards emit chunks
    /// tagged with their parents (lineage `parent_id`, or tagged
    /// self-parents); the coordinator owns the whole document
    /// aggregation — a parent's score is its best chunk's, the floor
    /// is one ULP below the k-th best parent score, and each returned
    /// parent carries EVERY chunk at or above the final floor,
    /// whichever shards held them. No colocation: an opinion whose
    /// chunks straddle a shard cut aggregates here exactly like one
    /// whose chunks share a shard.
    ///
    /// Lossless: a chunk below the parent floor can neither beat its
    /// own parent's best nor introduce a new top-k parent, and every
    /// chunk scoring at or above the FINAL floor cleared every earlier
    /// (lower) floor, so filtering the retained chunks to the final
    /// floor makes the groups exact and layout-invariant.
    pub async fn fanout_stream_search_collapse(
        &self,
        request_id: &str,
        vector: &[f32],
        k: u32,
        filters: &RequestFilters,
    ) -> Result<CollapseStreamResult, Status> {
        self.check_vector_scan(filters, true)?;
        if self.scoped_vector_scan() && self.query_read_versions.is_none() {
            let (pinned, reads) = self.pin_read_versions().await?;
            let result =
                Box::pin(pinned.fanout_stream_search_collapse(request_id, vector, k, filters))
                    .await;
            pinned.validate_read_versions(&reads).await?;
            return result;
        }
        struct ParentAgg {
            best_score: f32,
            best_id: u64,
            chunks: Vec<(u64, f32)>,
        }
        let n_nodes = self.node_addrs.len();
        if n_nodes == 0 {
            return Err(Status::failed_precondition("no shard nodes configured"));
        }
        let (settle_window, deadline_window) = self.resolve_finish_windows(filters)?;
        if k == 0 {
            if self.scoped_vector_scan() {
                return Err(Status::invalid_argument(
                    "a scoped vector scan requires positive k",
                ));
            }
            return Ok(CollapseStreamResult {
                hits: Vec::new(),
                groups: Vec::new(),
                chunk_floor: f32::NEG_INFINITY,
                summaries: Vec::new(),
                floors_sent: 0,
                shards_finished_early: 0,
                shards_floor_pruned: 0,
            });
        }
        self.ensure_shard_vector_rows().await;
        let mask = self.vector_scan_mask(filters.tree.as_ref());
        let mut known = Self::filter_known(filters, mask.as_ref());
        let mut fanout =
            self.open_stream_fanout(request_id, vector, None, Some(k), true, filters)?;
        let mut parents: HashMap<u64, ParentAgg> = HashMap::new();
        let mut seen_chunks: std::collections::HashSet<(u32, u64)> =
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
        let active_shards = remaining;
        let quorum = finish_quorum(active_shards);
        let mut completed_active = 0usize;
        let mut finish_sent = false;
        let mut settle_at: Option<tokio::time::Instant> = None;
        let mut deadline_at: Option<tokio::time::Instant> =
            deadline_window.map(|d| tokio::time::Instant::now() + d);
        // The k-th best parent score, recomputed lazily: only a batch
        // that raised some parent's best (or added a parent) can move
        // it, and floors derive from nothing else.
        let mut kth = f32::NEG_INFINITY;
        let mut last_floor = f32::NEG_INFINITY;
        let mut floors_sent = 0u64;
        enum CollapseLoopStep {
            Settle,
            Deadline,
            Message(Result<Option<(usize, StreamSearchResponse)>, Status>),
        }
        while remaining > 0 {
            let step = tokio::select! {
                biased;
                _ = async { tokio::time::sleep_until(deadline_at.expect("guarded")).await }, if deadline_at.is_some() => CollapseLoopStep::Deadline,
                _ = async { tokio::time::sleep_until(settle_at.expect("guarded")).await }, if settle_at.is_some() => CollapseLoopStep::Settle,
                msg = fanout.next_message(&summaries) => CollapseLoopStep::Message(msg),
            };
            let (shard, msg) = match step {
                CollapseLoopStep::Deadline => {
                    deadline_at = None;
                    settle_at = None;
                    finish_sent = true;
                    self.push_stream_finish(&fanout, &summaries, true).await;
                    continue;
                }
                CollapseLoopStep::Settle => {
                    settle_at = None;
                    if !finish_sent {
                        finish_sent = true;
                        self.push_stream_finish(&fanout, &summaries, false).await;
                    }
                    continue;
                }
                CollapseLoopStep::Message(Ok(Some(pair))) => pair,
                CollapseLoopStep::Message(Ok(None)) => continue,
                CollapseLoopStep::Message(Err(status)) => return fanout.cancel_with(status).await,
            };
            match msg.payload {
                Some(stream_search_response::Payload::Batch(batch)) => {
                    // Packed 20-byte LE records: u64 global id, f32
                    // score, u64 parent (see StreamSearchBatch).
                    if batch.hits.len() % 20 != 0 {
                        let status = Status::internal(format!(
                            "shard {shard} sent a misaligned collapse batch of {} bytes",
                            batch.hits.len()
                        ));
                        return fanout.cancel_with(status).await;
                    }
                    let mut dirty = false;
                    for rec in batch.hits.as_chunks::<20>().0 {
                        let doc = u64::from_le_bytes(rec[..8].try_into().expect("8-byte id"));
                        if !seen_chunks.insert((shard as u32, doc)) {
                            continue;
                        }
                        let score =
                            f32::from_le_bytes(rec[8..12].try_into().expect("4-byte score"));
                        let parent =
                            u64::from_le_bytes(rec[12..20].try_into().expect("8-byte parent"));
                        let agg = parents.entry(parent).or_insert_with(|| {
                            dirty = true;
                            ParentAgg {
                                best_score: f32::NEG_INFINITY,
                                best_id: u64::MAX,
                                chunks: Vec::new(),
                            }
                        });
                        agg.chunks.push((doc, score));
                        if score > agg.best_score || (score == agg.best_score && doc < agg.best_id)
                        {
                            if score > agg.best_score && score > kth {
                                dirty = true;
                            }
                            agg.best_score = score;
                            agg.best_id = doc;
                        }
                    }
                    if parents.len() >= k as usize {
                        if dirty {
                            let mut bests: Vec<f32> =
                                parents.values().map(|a| a.best_score).collect();
                            let idx = k as usize - 1;
                            let new_kth =
                                *bests.select_nth_unstable_by(idx, |a, b| b.total_cmp(a)).1;
                            if new_kth > kth {
                                kth = new_kth;
                                // next_down, not bm25::floor_seed: parent
                                // scores are signed vector scores.
                                let floor = kth.next_down();
                                if floor > last_floor {
                                    last_floor = floor;
                                    floors_sent += 1;
                                    self.push_stream_floor(&fanout, floor);
                                }
                            }
                        }
                        if !finish_sent && completed_active >= quorum {
                            if let Some(window) = settle_window {
                                if dirty || settle_at.is_none() {
                                    settle_at = Some(tokio::time::Instant::now() + window);
                                }
                            }
                        }
                    }
                }
                Some(stream_search_response::Payload::Summary(summary)) => {
                    if !summary.completed {
                        let status = Status::internal(format!(
                            "shard {shard} stopped before completing its scan"
                        ));
                        return fanout.cancel_with(status).await;
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
                    fanout.mark_completed(shard);
                    completed_active += 1;
                    remaining -= 1;
                    if remaining > 0
                        && !finish_sent
                        && settle_at.is_none()
                        && parents.len() >= k as usize
                        && completed_active >= quorum
                    {
                        if let Some(window) = settle_window {
                            settle_at = Some(tokio::time::Instant::now() + window);
                        }
                    }
                }
                Some(_) => {
                    return fanout
                        .cancel_with(Status::internal(
                            "unexpected identity exchange on a legacy stream",
                        ))
                        .await
                }
                None => {}
            }
        }

        known.refuse_unknown(filters)?;

        // Rank parents (best desc, best chunk id asc — the collapse
        // fan-out's order), keep k, and filter each kept parent's
        // chunks to the final floor for a deterministic group: every
        // chunk at or above it was emitted, every chunk below it is
        // dropped whether or not it happened to arrive.
        let mut ranked: Vec<(u64, ParentAgg)> = parents.into_iter().collect();
        ranked.sort_by(|a, b| {
            b.1.best_score
                .total_cmp(&a.1.best_score)
                .then_with(|| a.1.best_id.cmp(&b.1.best_id))
        });
        let chunk_floor = if ranked.len() >= k as usize {
            ranked[k as usize - 1].1.best_score.next_down()
        } else {
            f32::NEG_INFINITY
        };
        ranked.truncate(k as usize);
        let mut hits = Vec::with_capacity(ranked.len());
        let mut groups = Vec::with_capacity(ranked.len());
        for (parent, agg) in ranked {
            hits.push(ScoredHit {
                identity: None,
                vector_id: agg.best_id,
                score: agg.best_score,
                parent_id: parent,
                rerank_candidate: None,
            });
            let mut chunks: Vec<(u64, f32)> = agg
                .chunks
                .into_iter()
                .filter(|&(_, score)| score >= chunk_floor)
                .collect();
            chunks.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            groups.push(ParentGroup {
                parent_id: parent,
                chunks: chunks
                    .into_iter()
                    .map(|(doc, score)| ScoredHit {
                        identity: None,
                        vector_id: doc,
                        score,
                        parent_id: parent,
                        rerank_candidate: None,
                    })
                    .collect(),
            });
        }
        let summaries: Vec<StreamSearchSummary> = summaries
            .into_iter()
            .map(|s| s.expect("all summaries arrived"))
            .collect();
        let shards_finished_early = count_shards_finished_early(&summaries);
        let shards_floor_pruned = count_shards_floor_pruned(&summaries);
        Ok(CollapseStreamResult {
            hits,
            groups,
            chunk_floor,
            summaries,
            floors_sent,
            shards_finished_early,
            shards_floor_pruned,
        })
    }
}

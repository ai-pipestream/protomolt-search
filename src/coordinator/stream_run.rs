//! Shared streaming lifecycle with separate row and parent collectors.

use super::stream_collapse::ParentCollector;
use super::stream_search::RowCollector;
use super::*;

#[derive(Clone, Copy)]
pub(super) struct StreamQuery<'a> {
    pub request_id: &'a str,
    pub vector: &'a [f32],
    pub k: u32,
    pub initial_floor: Option<f32>,
    pub filters: &'a RequestFilters,
    pub collapse: bool,
}

pub(super) struct StreamRunResult {
    pub hits: Vec<ScoredHit>,
    pub groups: Vec<ParentGroup>,
    pub chunk_floor: f32,
    pub summaries: Vec<StreamSearchSummary>,
    pub floors_sent: u64,
    pub shards_finished_early: u32,
    pub shards_floor_pruned: u32,
}

impl StreamRunResult {
    fn empty() -> Self {
        Self {
            hits: Vec::new(),
            groups: Vec::new(),
            chunk_floor: f32::NEG_INFINITY,
            summaries: Vec::new(),
            floors_sent: 0,
            shards_finished_early: 0,
            shards_floor_pruned: 0,
        }
    }
}

pub(super) struct BatchUpdate {
    pub full: bool,
    pub changed: bool,
    pub kth: Option<f32>,
}

enum StreamCollector {
    Rows(RowCollector),
    Parents(ParentCollector),
}

impl StreamCollector {
    fn new(k: u32, collapse: bool) -> Self {
        if collapse {
            Self::Parents(ParentCollector::new(k))
        } else {
            Self::Rows(RowCollector::new(k))
        }
    }

    fn is_full(&self, k: u32) -> bool {
        match self {
            Self::Rows(rows) => rows.is_full(k),
            Self::Parents(parents) => parents.is_full(k),
        }
    }

    fn batch(&mut self, shard: usize, bytes: &[u8], k: u32) -> Result<BatchUpdate, Status> {
        match self {
            Self::Rows(rows) => rows.batch(shard, bytes, k),
            Self::Parents(parents) => parents.batch(shard, bytes, k),
        }
    }

    fn publish(&self, coordinator: &CoordinatorServiceImpl, fingerprint: &str) {
        if let Self::Rows(rows) = self {
            coordinator.publish_progress(
                crate::pb::QueryStreamPhase::Dense,
                rows.snapshot(),
                fingerprint.to_string(),
            );
        }
    }
}

struct StreamCompletion {
    summaries: Vec<Option<StreamSearchSummary>>,
    terminal: Vec<Option<StreamSearchSummary>>,
    remaining: usize,
    quorum: usize,
    completed_active: usize,
    finish_sent: bool,
    settle_window: Option<Duration>,
    settle_at: Option<tokio::time::Instant>,
    deadline_at: Option<tokio::time::Instant>,
    last_floor: f32,
    floors_sent: u64,
}

enum StreamLoopStep {
    Settle,
    Deadline,
    Message(Result<Option<(usize, StreamSearchResponse)>, Status>),
}

impl StreamCompletion {
    fn new(
        n_nodes: usize,
        skipped: Option<&[bool]>,
        windows: (Option<Duration>, Option<Duration>),
        initial_floor: Option<f32>,
    ) -> Self {
        let mut summaries = vec![None; n_nodes];
        let mut remaining = n_nodes;
        for (shard, summary) in summaries.iter_mut().enumerate() {
            if skipped.is_some_and(|mask| mask[shard]) {
                *summary = Some(StreamSearchSummary {
                    completed: true,
                    ..Default::default()
                });
                remaining -= 1;
            }
        }
        Self {
            terminal: summaries.clone(),
            summaries,
            remaining,
            quorum: finish_quorum(remaining),
            completed_active: 0,
            finish_sent: false,
            settle_window: windows.0,
            settle_at: None,
            deadline_at: windows.1.map(|d| tokio::time::Instant::now() + d),
            last_floor: initial_floor.unwrap_or(f32::NEG_INFINITY),
            floors_sent: 0,
        }
    }

    async fn next_step(&self, fanout: &mut StreamFanout, collapse: bool) -> StreamLoopStep {
        let terminal = if collapse {
            &self.summaries
        } else {
            &self.terminal
        };
        tokio::select! {
            biased;
            _ = async { tokio::time::sleep_until(self.deadline_at.expect("guarded")).await }, if self.deadline_at.is_some() => StreamLoopStep::Deadline,
            _ = async { tokio::time::sleep_until(self.settle_at.expect("guarded")).await }, if self.settle_at.is_some() => StreamLoopStep::Settle,
            msg = fanout.next_message(terminal) => StreamLoopStep::Message(msg),
        }
    }

    fn batch(&mut self, update: BatchUpdate) -> Option<f32> {
        let mut raised = None;
        if update.full {
            if let Some(kth) = update.kth {
                // Vector scores are signed; clamping to zero would lose hits.
                let floor = kth.next_down();
                if floor > self.last_floor {
                    self.last_floor = floor;
                    self.floors_sent += 1;
                    raised = Some(floor);
                }
            }
            if !self.finish_sent && self.completed_active >= self.quorum {
                if let Some(window) = self.settle_window {
                    if update.changed || self.settle_at.is_none() {
                        self.settle_at = Some(tokio::time::Instant::now() + window);
                    }
                }
            }
        }
        raised
    }

    fn completed(&mut self, shard: usize, summary: StreamSearchSummary, full: bool) {
        self.summaries[shard] = Some(summary);
        self.completed_active += 1;
        self.remaining -= 1;
        if self.remaining > 0
            && !self.finish_sent
            && self.settle_at.is_none()
            && full
            && self.completed_active >= self.quorum
        {
            if let Some(window) = self.settle_window {
                self.settle_at = Some(tokio::time::Instant::now() + window);
            }
        }
    }

    fn finish(self, mut result: StreamRunResult) -> StreamRunResult {
        result.summaries = self
            .summaries
            .into_iter()
            .map(|s| s.expect("all summaries arrived"))
            .collect();
        result.shards_finished_early = count_shards_finished_early(&result.summaries);
        result.shards_floor_pruned = count_shards_floor_pruned(&result.summaries);
        result.floors_sent = self.floors_sent;
        result
    }
}

impl CoordinatorServiceImpl {
    pub(super) async fn run_stream_query(
        &self,
        query: StreamQuery<'_>,
    ) -> Result<StreamRunResult, Status> {
        self.check_vector_scan(query.filters, query.collapse)?;
        if self.scoped_vector_scan() && self.query_read_versions.is_none() {
            let (pinned, reads) = self.pin_read_versions().await?;
            let result = Box::pin(pinned.run_stream_query(query)).await;
            pinned.validate_read_versions(&reads).await?;
            return result;
        }
        let n_nodes = self.node_addrs.len();
        if n_nodes == 0 {
            return Err(Status::failed_precondition("no shard nodes configured"));
        }
        if query.initial_floor.is_some_and(f32::is_nan) {
            return Err(Status::invalid_argument("initial_floor must not be NaN"));
        }
        let windows = self.resolve_finish_windows(query.filters)?;
        if query.k == 0 {
            if self.scoped_vector_scan() {
                return Err(Status::invalid_argument(
                    "a scoped vector scan requires positive k",
                ));
            }
            return Ok(StreamRunResult::empty());
        }
        self.ensure_shard_vector_rows().await;
        let mask = self.vector_scan_mask(query.filters.tree.as_ref());
        let mut known = Self::filter_known(query.filters, mask.as_ref());
        let identity_limits = if query.collapse {
            None
        } else {
            let limits = crate::pb::StreamIdentityLimits {
                max_rows: query.k,
                max_response_bytes: 32 * 1024 * 1024,
                timeout_ms: 60_000,
            };
            crate::query_identity::validate_limits(&limits)?;
            Some(limits)
        };
        let mut fanout = if query.collapse {
            self.open_stream_fanout(
                query.request_id,
                query.vector,
                query.initial_floor,
                Some(query.k),
                true,
                query.filters,
            )
        } else {
            self.open_stream_context(StreamOpen {
                request_id: query.request_id,
                vector: query.vector,
                initial_floor: query.initial_floor,
                candidate_depth: Some(query.k),
                collapse_parents: query.collapse,
                filters: query.filters,
                identity_limits,
            })
        }?;
        let mut collector = StreamCollector::new(query.k, query.collapse);
        let mut completion = StreamCompletion::new(
            n_nodes,
            mask.as_ref().map(|mask| mask.skipped.as_slice()),
            windows,
            query.initial_floor,
        );
        let mut scoring_fingerprint: Option<String> = None;
        while completion.remaining > 0 {
            let (shard, msg) = match completion.next_step(&mut fanout, query.collapse).await {
                StreamLoopStep::Deadline => {
                    completion.deadline_at = None;
                    completion.settle_at = None;
                    completion.finish_sent = true;
                    self.push_stream_finish(&fanout, &completion.summaries, true)
                        .await;
                    continue;
                }
                StreamLoopStep::Settle => {
                    completion.settle_at = None;
                    if !completion.finish_sent {
                        completion.finish_sent = true;
                        self.push_stream_finish(&fanout, &completion.summaries, false)
                            .await;
                    }
                    continue;
                }
                StreamLoopStep::Message(Ok(Some(pair))) => pair,
                StreamLoopStep::Message(Ok(None)) => continue,
                StreamLoopStep::Message(Err(status)) => return fanout.cancel_with(status).await,
            };
            let summary = match msg.payload {
                Some(stream_search_response::Payload::Batch(batch)) => {
                    if !query.collapse && completion.summaries[shard].is_some() {
                        return fanout
                            .cancel_with(Status::internal("candidate batch after IdentityReady"))
                            .await;
                    }
                    let update = match collector.batch(shard, &batch.hits, query.k) {
                        Ok(update) => update,
                        Err(status) => return fanout.cancel_with(status).await,
                    };
                    if let Some(floor) = completion.batch(update) {
                        self.push_stream_floor(&fanout, floor);
                    }
                    collector.publish(self, scoring_fingerprint.as_deref().unwrap_or_default());
                    continue;
                }
                Some(stream_search_response::Payload::IdentityReady(ready)) if !query.collapse => {
                    if completion.summaries[shard].is_some() {
                        return fanout
                            .cancel_with(Status::internal("duplicate IdentityReady"))
                            .await;
                    }
                    let Some(summary) = ready.scan else {
                        return fanout
                            .cancel_with(Status::internal("IdentityReady has no scan certificate"))
                            .await;
                    };
                    summary
                }
                Some(stream_search_response::Payload::Summary(summary)) if query.collapse => {
                    summary
                }
                Some(stream_search_response::Payload::Summary(_)) => {
                    return fanout
                        .cancel_with(Status::failed_precondition(
                            "stream ended without snapshot-bound identity support or completion",
                        ))
                        .await;
                }
                None if query.collapse => continue,
                _ => {
                    let message = if query.collapse {
                        "unexpected identity exchange on a legacy stream"
                    } else {
                        "unexpected message before IdentityReady"
                    };
                    return fanout.cancel_with(Status::internal(message)).await;
                }
            };
            if !summary.completed {
                return fanout
                    .cancel_with(Status::internal(format!(
                        "shard {shard} stopped before completing its scan"
                    )))
                    .await;
            }
            if !query.collapse {
                if summary.scoring_fingerprint.is_empty() {
                    return fanout
                        .cancel_with(Status::failed_precondition(format!(
                            "shard {shard} completed without a vector scoring fingerprint"
                        )))
                        .await;
                }
                match scoring_fingerprint.as_ref() {
                    Some(expected) if expected != &summary.scoring_fingerprint => {
                        return fanout
                            .cancel_with(Status::failed_precondition(format!(
                            "shard {shard} vector scoring fingerprint {} differs from {expected}",
                            summary.scoring_fingerprint
                        )))
                            .await;
                    }
                    None => scoring_fingerprint = Some(summary.scoring_fingerprint.clone()),
                    _ => {}
                }
            }
            if let Err(error) = known.merge_shard(
                shard,
                &summary.geo_columns_known,
                &summary.filter_columns_known,
            ) {
                return fanout.cancel_with(error).await;
            }
            completion.completed(shard, summary, collector.is_full(query.k));
            if query.collapse {
                fanout.mark_completed(shard);
            }
        }
        known.refuse_unknown(query.filters)?;
        collector.publish(self, scoring_fingerprint.as_deref().unwrap_or_default());
        let mut result = StreamRunResult::empty();
        match collector {
            StreamCollector::Rows(rows) => {
                let all = rows.finish();
                let (identities, candidates) = fanout
                    .resolve_identities_and_candidates(
                        &all,
                        &completion.summaries,
                        &mut completion.terminal,
                        &identity_limits.expect("row collector requests identities"),
                    )
                    .await?;
                result.hits = all
                    .into_iter()
                    .map(|h| ScoredHit {
                        vector_id: h.vector_id,
                        score: h.score,
                        parent_id: 0,
                        identity: identities.get(&(h.shard, h.vector_id)).cloned().flatten(),
                        rerank_candidate: candidates
                            .get(&(h.shard, h.vector_id))
                            .cloned()
                            .flatten(),
                    })
                    .collect();
            }
            StreamCollector::Parents(parents) => {
                (result.hits, result.groups, result.chunk_floor) = parents.finish(query.k);
            }
        }
        Ok(completion.finish(result))
    }
}

#[cfg(test)]
#[path = "stream_run_tests.rs"]
mod tests;

//! The collection loop of one streaming fan-out and its completion checks.

use super::*;

impl StreamRun<'_> {
    pub(super) async fn collect(
        &mut self,
        coordinator: &CoordinatorServiceImpl,
    ) -> Result<(), Status> {
        while self.completion.remaining > 0 {
            let step = self
                .completion
                .next_step(&mut self.fanout, self.query.collapse)
                .await;
            let message = match step {
                StreamLoopStep::Deadline => {
                    self.completion.deadline_at = None;
                    self.completion.settle_at = None;
                    self.completion.finish_sent = true;
                    coordinator
                        .push_stream_finish(&self.fanout, &self.completion.summaries, true)
                        .await;
                    continue;
                }
                StreamLoopStep::Settle => {
                    self.completion.settle_at = None;
                    if !self.completion.finish_sent {
                        self.completion.finish_sent = true;
                        coordinator
                            .push_stream_finish(&self.fanout, &self.completion.summaries, false)
                            .await;
                    }
                    continue;
                }
                StreamLoopStep::Message(message) => message,
            };
            let result = match message {
                Ok(Some((shard, message))) => self.message(coordinator, shard, *message),
                Ok(None) => continue,
                Err(status) => Err(status),
            };
            if let Err(status) = result {
                return self.fanout.cancel_with(status).await;
            }
        }
        Ok(())
    }

    fn message(
        &mut self,
        coordinator: &CoordinatorServiceImpl,
        shard: usize,
        message: StreamSearchResponse,
    ) -> Result<(), Status> {
        let summary = match message.payload {
            Some(stream_search_response::Payload::Batch(batch)) => {
                return self.batch(coordinator, shard, &batch.hits);
            }
            Some(stream_search_response::Payload::IdentityReady(ready)) if !self.query.collapse => {
                if self.completion.summaries[shard].is_some() {
                    return Err(Status::internal("duplicate IdentityReady"));
                }
                ready
                    .scan
                    .ok_or_else(|| Status::internal("IdentityReady has no scan certificate"))?
            }
            Some(stream_search_response::Payload::Summary(summary)) if self.query.collapse => {
                summary
            }
            Some(stream_search_response::Payload::Summary(_)) => {
                return Err(Status::failed_precondition(
                    "stream ended without snapshot-bound identity support or completion",
                ));
            }
            None if self.query.collapse => return Ok(()),
            _ => {
                return Err(Status::internal(if self.query.collapse {
                    "unexpected identity exchange on a legacy stream"
                } else {
                    "unexpected message before IdentityReady"
                }))
            }
        };
        self.validate_summary(shard, &summary)?;
        self.known.merge_shard(
            shard,
            &summary.geo_columns_known,
            &summary.filter_columns_known,
        )?;
        self.completion
            .completed(shard, summary, self.collector.is_full(self.query.k));
        if self.query.collapse {
            self.fanout.mark_completed(shard);
        }
        Ok(())
    }

    fn batch(
        &mut self,
        coordinator: &CoordinatorServiceImpl,
        shard: usize,
        bytes: &[u8],
    ) -> Result<(), Status> {
        if !self.query.collapse && self.completion.summaries[shard].is_some() {
            return Err(Status::internal("candidate batch after IdentityReady"));
        }
        let update = self.collector.batch(shard, bytes, self.query.k)?;
        if let Some(floor) = self.completion.batch(update) {
            coordinator.push_stream_floor(&self.fanout, floor);
        }
        self.collector.publish(
            coordinator,
            self.scoring_fingerprint.as_deref().unwrap_or_default(),
        );
        Ok(())
    }

    fn validate_summary(
        &mut self,
        shard: usize,
        summary: &StreamSearchSummary,
    ) -> Result<(), Status> {
        if !summary.completed {
            return Err(Status::internal(format!(
                "shard {shard} stopped before completing its scan"
            )));
        }
        if !self.query.collapse {
            if summary.scoring_fingerprint.is_empty() {
                return Err(Status::failed_precondition(format!(
                    "shard {shard} completed without a vector scoring fingerprint"
                )));
            }
            match self.scoring_fingerprint.as_ref() {
                Some(expected) if expected != &summary.scoring_fingerprint => {
                    return Err(Status::failed_precondition(format!(
                        "shard {shard} vector scoring fingerprint {} differs from {expected}",
                        summary.scoring_fingerprint
                    )));
                }
                None => self.scoring_fingerprint = Some(summary.scoring_fingerprint.clone()),
                _ => {}
            }
        }
        Ok(())
    }

    pub(super) async fn finish(
        mut self,
        coordinator: &CoordinatorServiceImpl,
    ) -> Result<StreamRunResult, Status> {
        self.known.refuse_unknown(self.query.filters)?;
        self.collector.publish(
            coordinator,
            self.scoring_fingerprint.as_deref().unwrap_or_default(),
        );
        let mut result = StreamRunResult::empty();
        match self.collector {
            StreamCollector::Rows(rows) => {
                let all = rows.finish();
                let (identities, candidates) = self
                    .fanout
                    .resolve_identities_and_candidates(
                        &all,
                        &self.completion.summaries,
                        &mut self.completion.terminal,
                        &self
                            .identity_limits
                            .expect("row collector requests identities"),
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
                (result.hits, result.groups, result.chunk_floor) = parents.finish(self.query.k);
            }
        }
        Ok(self.completion.finish(result))
    }
}

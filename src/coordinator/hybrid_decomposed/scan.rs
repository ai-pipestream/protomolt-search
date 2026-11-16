//! Streaming vector scan over the clustered backend or the shard fan-out.

use super::run::IdentitySession;
use super::*;

impl DecomposedRun<'_> {
    pub(super) fn observe_vector(&mut self, doc: u64, vector: f32, shard: u32) {
        if self.vectors.contains_key(&doc) {
            return;
        }
        self.vectors.insert(doc, (vector, shard));
        let bm25 = self.bm25.get(&doc).map_or(0.0, |&(score, _)| score);
        self.bounds.push(vector, bm25);
    }

    pub(super) fn stream_floor(&self, floor: f32) -> f32 {
        crate::hnsw::distance::simd_score_floor(self.round.query().input().vector().len(), floor)
    }

    pub(super) async fn scan_vectors(
        &mut self,
        coordinator: &CoordinatorServiceImpl,
        initial_floor: Option<f32>,
    ) -> Result<(), Status> {
        let initial_floor = initial_floor.map(|floor| self.stream_floor(floor));
        if coordinator.clustered_vectors.is_some() {
            self.scan_clustered(coordinator, initial_floor).await
        } else {
            self.scan_plain(coordinator, initial_floor).await
        }
    }

    pub(super) async fn scan_clustered(
        &mut self,
        coordinator: &CoordinatorServiceImpl,
        initial_floor: Option<f32>,
    ) -> Result<(), Status> {
        let input = self.round.query().input();
        let backend = coordinator
            .clustered_vectors
            .as_ref()
            .expect("clustered route selected");
        let ranges = coordinator.product_label_ranges().await?;
        let allowed = coordinator
            .clustered_allowed_labels(input.filters())
            .await?;
        let mut stream = backend
            .candidate_stream(
                input.request_id(),
                input.vector().to_vec(),
                allowed,
                initial_floor,
            )
            .await?;
        let mut labels = std::collections::HashSet::new();
        let completion = loop {
            match stream.next_event().await? {
                ClusteredCandidateEvent::Batch(batch) => {
                    for candidate in batch {
                        if !candidate.score.is_finite() {
                            return Err(Status::internal(format!(
                                "clustered TurboVec label {} has non-finite score {}",
                                candidate.label, candidate.score
                            )));
                        }
                        if !labels.insert(candidate.label) {
                            return Err(Status::failed_precondition(format!(
                                "clustered TurboVec emitted duplicate stable label {}",
                                candidate.label
                            )));
                        }
                        let shard =
                            CoordinatorServiceImpl::product_owner(&ranges, candidate.label)?;
                        self.clustered_counts[shard as usize] =
                            self.clustered_counts[shard as usize].saturating_add(1);
                        self.observe_vector(candidate.label, candidate.score, shard);
                    }
                    if let Some(floor) = self.bounds.raised_floor() {
                        stream.raise_floor(self.stream_floor(floor))?;
                    }
                }
                ClusteredCandidateEvent::Completion(completion) => break completion,
            }
        };
        if completion.emitted != labels.len() as u64 {
            return Err(Status::internal(format!("clustered TurboVec completion counted {} candidates but decomposed search received {}", completion.emitted, labels.len())));
        }
        Ok(())
    }

    pub(super) async fn scan_plain(
        &mut self,
        coordinator: &CoordinatorServiceImpl,
        initial_floor: Option<f32>,
    ) -> Result<(), Status> {
        coordinator.ensure_shard_vector_rows().await;
        let input = self.round.query().input();
        let limits =
            input
                .filters()
                .include_rerank_candidate
                .then_some(crate::pb::StreamIdentityLimits {
                    max_rows: input.k(),
                    max_response_bytes: 32 * 1024 * 1024,
                    timeout_ms: 60_000,
                });
        let mut fanout = coordinator.open_stream_fanout_with_identities(
            input.request_id(),
            input.vector(),
            initial_floor,
            Some(input.k()),
            false,
            input.filters(),
            limits,
        )?;
        let mut remaining = self.summaries.len();
        for (shard, summary) in self.summaries.iter_mut().enumerate() {
            if self.mask.as_ref().is_some_and(|mask| mask.skipped[shard]) {
                *summary = Some(StreamSearchSummary {
                    completed: true,
                    ..Default::default()
                });
                remaining -= 1;
            }
        }
        let terminal = self.summaries.clone();
        while remaining > 0 {
            let (shard, message) = match fanout.next_message(&self.summaries).await {
                Ok(Some(pair)) => pair,
                Ok(None) => continue,
                Err(error) => return fanout.cancel_with(error).await,
            };
            match self.accept_stream_message(
                coordinator,
                &mut fanout,
                shard,
                message,
                limits.is_some(),
            ) {
                Ok(true) => remaining -= 1,
                Ok(false) => {}
                Err(error) => return fanout.cancel_with(error).await,
            }
        }
        self.known.refuse_unknown(input.filters())?;
        if let Some(limits) = limits {
            self.identity = Some(IdentitySession {
                fanout,
                terminal,
                limits,
            });
        }
        Ok(())
    }

    pub(super) fn accept_stream_message(
        &mut self,
        coordinator: &CoordinatorServiceImpl,
        fanout: &mut StreamFanout,
        shard: usize,
        message: StreamSearchResponse,
        identities: bool,
    ) -> Result<bool, Status> {
        let summary = match message.payload {
            Some(stream_search_response::Payload::Batch(batch)) => {
                if batch.hits.len() % 12 != 0 {
                    return Err(Status::internal(format!(
                        "shard {shard} sent a misaligned batch of {} bytes",
                        batch.hits.len()
                    )));
                }
                for record in batch.hits.as_chunks::<12>().0 {
                    let doc = u64::from_le_bytes(record[..8].try_into().expect("8-byte id"));
                    let vector =
                        f32::from_le_bytes(record[8..12].try_into().expect("4-byte score"));
                    self.observe_vector(doc, vector, shard as u32);
                }
                if let Some(floor) = self.bounds.raised_floor() {
                    coordinator.push_stream_floor(fanout, self.stream_floor(floor));
                }
                return Ok(false);
            }
            Some(stream_search_response::Payload::IdentityReady(ready)) if identities => ready
                .scan
                .ok_or_else(|| Status::internal("IdentityReady has no scan certificate"))?,
            Some(stream_search_response::Payload::Summary(summary)) if !identities => summary,
            Some(_) => return Err(Status::internal("unexpected identity exchange on stream")),
            None => return Ok(false),
        };
        if !summary.completed {
            return Err(Status::internal(format!(
                "shard {shard} stopped before completing its scan"
            )));
        }
        self.known.merge_shard(
            shard,
            &summary.geo_columns_known,
            &summary.filter_columns_known,
        )?;
        self.summaries[shard] = Some(summary);
        if !identities {
            fanout.mark_completed(shard);
        }
        Ok(true)
    }
}

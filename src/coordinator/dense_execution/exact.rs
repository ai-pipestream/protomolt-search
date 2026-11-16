//! Exact FP32 rescoring of a fixed candidate set on the product shards.

use super::*;

type RescoreTask = tokio::task::JoinHandle<Result<crate::pb::ExactVectorRescoreResponse, Status>>;

/// Scores and read metrics accumulated across the product shards.
struct RescoreFold {
    scores: HashMap<u64, f32>,
    observed_bytes: u64,
    pages_touched: u64,
    worker_tasks: u32,
}

impl RescoreFold {
    fn absorb(
        &mut self,
        requested: &[u64],
        response: crate::pb::ExactVectorRescoreResponse,
    ) -> Result<(), Status> {
        self.observed_bytes = self
            .observed_bytes
            .checked_add(response.logical_bytes)
            .ok_or_else(|| Status::internal("exact rerank byte metrics overflow"))?;
        self.pages_touched = self
            .pages_touched
            .checked_add(response.pages_touched)
            .ok_or_else(|| Status::internal("exact rerank page metrics overflow"))?;
        self.worker_tasks = self.worker_tasks.saturating_add(response.tasks);
        for hit in response.hits {
            if requested.binary_search(&hit.doc_id).is_err()
                || !hit.score.is_finite()
                || self.scores.insert(hit.doc_id, hit.score).is_some()
            {
                return Err(Status::failed_precondition(format!(
                    "FP32 rerank returned an unrequested, duplicate or invalid score for candidate {}",
                    hit.doc_id
                )));
            }
        }
        Ok(())
    }

    /// Every requested id must have resolved, under exactly the row bytes
    /// the request implied.
    fn finish(self, requested: &[u64], logical_bytes: u64) -> Result<ExactRerankScores, Status> {
        if let Some(missing) = requested.iter().find(|id| !self.scores.contains_key(id)) {
            return Err(Status::failed_precondition(format!(
                "FP32 rerank candidate {missing} has no exact-vector row on any product shard"
            )));
        }
        if self.observed_bytes != logical_bytes {
            return Err(Status::failed_precondition(format!(
                "FP32 rerank shards reported {} logical bytes, expected {logical_bytes}; exact-row dimensions or ownership disagree",
                self.observed_bytes
            )));
        }
        Ok(ExactRerankScores {
            scores: self.scores,
            rows: requested.len() as u64,
            logical_bytes,
            pages_touched: self.pages_touched,
            tasks: self.worker_tasks,
        })
    }
}

impl CoordinatorServiceImpl {
    /// Score a fixed candidate set against the product-owned original FP32
    /// rows. Every requested id must resolve exactly once across the product
    /// shards. Selection may come from the embedded or clustered provider;
    /// stable labels route back to these product-owned rows in either case.
    pub(crate) async fn exact_vector_scores(
        &self,
        vector: &[f32],
        ids: &[u64],
        field: &str,
    ) -> Result<super::super::ExactRerankScores, Status> {
        if let Some(fields) = &self.field_permissions {
            fields.vector(field)?;
        }
        if self.query_read_versions.is_none() {
            let (pinned, reads) = self.pin_read_versions().await?;
            let result = Box::pin(pinned.exact_vector_scores(vector, ids, field)).await;
            pinned.validate_read_versions(&reads).await?;
            return result;
        }
        let scope = crate::visibility::VisibilityScope::new(self.document_visibility.as_ref())?;
        let mut visibility_known = vec![false; scope.column_count()];
        if vector.is_empty() {
            return Err(Status::invalid_argument(
                "FP32 rerank needs a non-empty query vector",
            ));
        }
        if ids.is_empty() && field.is_empty() && self.document_visibility.is_none() {
            return Ok(ExactRerankScores {
                scores: HashMap::new(),
                rows: 0,
                logical_bytes: 0,
                pages_touched: 0,
                tasks: 0,
            });
        }
        let mut requested = ids.to_vec();
        requested.sort_unstable();
        requested.dedup();
        let logical_bytes = self.rerank_logical_bytes(requested.len(), vector.len())?;
        let tasks = self.spawn_exact_rescore(vector, &requested, field)?;
        let mut fold = RescoreFold {
            scores: HashMap::with_capacity(requested.len()),
            observed_bytes: 0,
            pages_touched: 0,
            worker_tasks: 0,
        };
        let mut held_binding = None;
        for (shard, task) in tasks.into_iter().enumerate() {
            let response = task.await.map_err(|e| {
                Status::internal(format!("exact vector rescore task failed: {e}"))
            })??;
            Self::check_vector_binding(field, response.vector_binding.as_ref(), &mut held_binding)?;
            self.check_read_view(shard, &scope, &response, &mut visibility_known)?;
            fold.absorb(&requested, response)?;
        }
        self.check_visibility_columns(&visibility_known)?;
        fold.finish(&requested, logical_bytes)
    }

    /// The logical FP32 row bytes a rerank of `candidates` rows at `dim`
    /// reads, refused when above the coordinator's rerank budget.
    fn rerank_logical_bytes(&self, candidates: usize, dim: usize) -> Result<u64, Status> {
        let logical_bytes = (candidates as u64)
            .checked_mul(dim as u64)
            .and_then(|bytes| bytes.checked_mul(4))
            .ok_or_else(|| Status::resource_exhausted("FP32 rerank byte count overflow"))?;
        if logical_bytes > self.max_rerank_bytes {
            return Err(Status::resource_exhausted(format!(
                "FP32 rerank needs {logical_bytes} logical row bytes for {candidates} candidates at dim {dim}, above coordinator max_rerank_bytes={}",
                self.max_rerank_bytes
            )));
        }
        Ok(logical_bytes)
    }

    /// Start one pinned exact-rescore call per product shard.
    fn spawn_exact_rescore(
        &self,
        vector: &[f32],
        requested: &[u64],
        field: &str,
    ) -> Result<Vec<RescoreTask>, Status> {
        let mut tasks = Vec::with_capacity(self.node_addrs.len());
        for (shard, addr) in self.node_addrs.iter().enumerate() {
            let claim = self.query_read_versions.as_ref().expect("pinned reads")[shard];
            let request = ExactVectorRescoreRequest {
                field: field.into(),
                visibility: self.document_visibility.clone(),
                expected_stats_epoch: claim.epoch,
                expected_stats_incarnation: claim.incarnation(),
                vector: vector.to_vec(),
                candidate_ids: requested.to_vec(),
                max_logical_bytes: self.max_rerank_bytes,
            };
            let mut client = self.node_client(addr)?;
            let deadline = self.limits.shard_deadline;
            tasks.push(tokio::spawn(async move {
                let call = client.exact_vector_rescore(request);
                match deadline {
                    Some(limit) => tokio::time::timeout(limit, call)
                        .await
                        .map_err(|_| {
                            Status::deadline_exceeded(
                                "exact vector rescore shard deadline exceeded",
                            )
                        })?
                        .map(tonic::Response::into_inner),
                    None => call.await.map(tonic::Response::into_inner),
                }
            }));
        }
        Ok(tasks)
    }
}

//! Embedding backend checks, Matryoshka shortlist rescoring, and concurrent SPLADE fan-out.

use std::collections::BTreeMap;

use super::*;

/// SPLADE inputs after top-k range and deadline admission.
pub(super) struct SpladeFanout<'a> {
    field: &'a str,
    query_weights: &'a [(String, f32)],
    query_l1_slack: f32,
    k: usize,
    wire_k: u32,
    filter_cel: &'a str,
    finish_deadline_us: u64,
}

type SpladeReply = (usize, Result<crate::pb::ScoreSpladeResponse, Status>);
type MatryoshkaReply = (usize, Result<crate::pb::RescoreMatryoshkaResponse, Status>);

fn effective_shard_timeout(
    finish_deadline_us: u64,
    shard_deadline: Option<Duration>,
) -> Result<Option<Duration>, Status> {
    let remaining = crate::turbo_bridge::remaining_deadline_duration(finish_deadline_us)?;
    Ok(match (remaining, shard_deadline) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    })
}

async fn run_with_deadline<T, F>(
    finish_deadline_us: u64,
    remaining: Option<Duration>,
    expired_msg: &'static str,
    future: F,
) -> Result<T, Status>
where
    F: std::future::Future<Output = Result<T, Status>>,
{
    let result = match remaining {
        Some(remaining) => tokio::time::timeout(remaining, future)
            .await
            .map_err(|_| Status::deadline_exceeded(expired_msg))?,
        None => future.await,
    };
    if matches!(&result, Err(status) if status.code() == tonic::Code::Cancelled)
        && crate::turbo_bridge::remaining_deadline_duration(finish_deadline_us).is_err()
    {
        return Err(Status::deadline_exceeded(expired_msg));
    }
    result
}

impl CoordinatorServiceImpl {
    fn shard_read_claim(&self, shard: usize) -> Result<StatsClaim, Status> {
        let Some(claims) = self.query_read_versions.as_deref() else {
            return Ok(StatsClaim::default());
        };
        claims.get(shard).copied().ok_or_else(|| {
            Status::failed_precondition(format!(
                "pinned read set has {} claims, missing shard {shard}",
                claims.len()
            ))
        })
    }

    /// Verify a remote `turbo-kserve` endpoint against an expected index-generation handshake.
    #[cfg(feature = "net")]
    pub async fn verify_remote_turbo_handshake(
        &self,
        endpoint: &str,
        expected: turbo_client::IndexGenerationHandshake,
    ) -> Result<(), Status> {
        let opts = crate::analyzer::RemoteCallOptions {
            tls: self.client_tls.clone(),
            expected_handshake: Some(expected.clone()),
            ..Default::default()
        };
        let _ =
            crate::analyzer::connect_verified_turbo_client(endpoint, None, &opts, expected).await?;
        Ok(())
    }

    /// Rescore a Stage-1 `(doc_id, score)` shortlist against a higher-dimensional
    /// Matryoshka tier across all local and remote shards, returning the re-sorted shortlist.
    pub async fn rescore_matryoshka_shortlist(
        &self,
        candidates: &[(u64, f32)],
        query: &[f32],
        target_dim: u32,
    ) -> Result<Vec<(u64, f32)>, Status> {
        self.rescore_matryoshka_shortlist_with_deadline(candidates, query, target_dim, 0)
            .await
    }

    /// Rescore a Stage-1 shortlist against a retained Matryoshka tier bounded by
    /// `finish_deadline_us` (`0` = unbounded). Dropping this future aborts all shard RPCs.
    pub async fn rescore_matryoshka_shortlist_with_deadline(
        &self,
        candidates: &[(u64, f32)],
        query: &[f32],
        target_dim: u32,
        finish_deadline_us: u64,
    ) -> Result<Vec<(u64, f32)>, Status> {
        let coordinator = self.request_snapshot().unwrap_or_else(|| self.clone());
        let remaining = crate::turbo_bridge::remaining_deadline_duration(finish_deadline_us)?;
        if target_dim == 0 || query.len() < target_dim as usize {
            return Err(Status::invalid_argument(format!(
                "query vector len {} is shorter than matryoshka tier_dim {target_dim}",
                query.len()
            )));
        }
        if query.iter().any(|x| !x.is_finite()) {
            return Err(Status::invalid_argument(
                "matryoshka query vector contains non-finite values",
            ));
        }
        if candidates.is_empty()
            && coordinator.query_read_versions.is_none()
            && coordinator.document_visibility.is_none()
        {
            return Ok(Vec::new());
        }
        let run =
            coordinator.rescore_matryoshka_inner(candidates, query, target_dim, finish_deadline_us);
        run_with_deadline(
            finish_deadline_us,
            remaining,
            "matryoshka rescore deadline expired",
            run,
        )
        .await
    }

    async fn rescore_matryoshka_inner(
        &self,
        candidates: &[(u64, f32)],
        query: &[f32],
        target_dim: u32,
        finish_deadline_us: u64,
    ) -> Result<Vec<(u64, f32)>, Status> {
        let scope = crate::visibility::VisibilityScope::new(self.document_visibility.as_ref())?;
        let mut visibility_known = vec![false; scope.column_count()];
        let mut requested: Vec<u64> = candidates.iter().map(|&(id, _)| id).collect();
        requested.sort_unstable();
        requested.dedup();
        let mut tasks =
            self.spawn_matryoshka_tasks(&requested, query, target_dim, finish_deadline_us)?;
        let mut replies = BTreeMap::new();
        while let Some(joined) = tasks.join_next().await {
            let (shard, resp) = joined
                .map_err(|e| Status::internal(format!("matryoshka rescore task failed: {e}")))?;
            replies.insert(shard, resp?);
        }
        let mut scores = HashMap::with_capacity(requested.len());
        for (shard, resp) in replies {
            self.check_read_view(shard, &scope, &resp, &mut visibility_known)?;
            for hit in resp.hits {
                if requested.binary_search(&hit.doc_id).is_err()
                    || !hit.score.is_finite()
                    || scores.insert(hit.doc_id, hit.score).is_some()
                {
                    return Err(Status::failed_precondition(format!(
                        "matryoshka rescore returned an unrequested, duplicate, or invalid score for candidate {}",
                        hit.doc_id
                    )));
                }
            }
        }
        self.check_visibility_columns(&visibility_known)?;
        let mut rescored: Vec<(u64, f32)> = candidates
            .iter()
            .filter_map(|&(id, _)| scores.get(&id).copied().map(|score| (id, score)))
            .collect();
        rescored.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        Ok(rescored)
    }

    fn spawn_matryoshka_tasks(
        &self,
        requested: &[u64],
        query: &[f32],
        target_dim: u32,
        finish_deadline_us: u64,
    ) -> Result<tokio::task::JoinSet<MatryoshkaReply>, Status> {
        let mut tasks = tokio::task::JoinSet::new();
        for (shard, addr) in self.node_addrs.iter().enumerate() {
            let claim = self.shard_read_claim(shard)?;
            let mut link = self.node_client(addr)?;
            let mut request = Request::new(crate::pb::RescoreMatryoshkaRequest {
                query: query.to_vec(),
                target_dim,
                candidate_ids: requested.to_vec(),
                visibility: self.document_visibility.clone(),
                expected_stats_epoch: claim.epoch,
                expected_stats_incarnation: claim.incarnation(),
            });
            if let Some(timeout) =
                effective_shard_timeout(finish_deadline_us, self.limits.shard_deadline)?
            {
                request.set_timeout(timeout);
            }
            tasks.spawn(async move {
                (
                    shard,
                    link.rescore_matryoshka(request)
                        .await
                        .map(|r| r.into_inner()),
                )
            });
        }
        Ok(tasks)
    }

    /// Fan out quantized SPLADE 128-doc Block-Max scoring concurrently across all
    /// unmasked local and remote shards with CEL filter admission.
    pub async fn score_splade(
        &self,
        field: &str,
        query_weights: &[(String, f32)],
        query_l1_slack: f32,
        k: usize,
        filter_cel: &str,
    ) -> Result<Vec<(u64, f32)>, Status> {
        self.score_splade_with_deadline(field, query_weights, query_l1_slack, k, filter_cel, 0)
            .await
    }

    /// Fan out quantized SPLADE scoring bounded by an absolute microsecond deadline (`0` = unbounded).
    /// Dropping this future aborts every in-flight shard RPC via its `JoinSet`.
    pub async fn score_splade_with_deadline(
        &self,
        field: &str,
        query_weights: &[(String, f32)],
        query_l1_slack: f32,
        k: usize,
        filter_cel: &str,
        finish_deadline_us: u64,
    ) -> Result<Vec<(u64, f32)>, Status> {
        let coordinator = self.request_snapshot().unwrap_or_else(|| self.clone());
        let max_k = coordinator.knobs.max_k();
        if k > max_k as usize {
            return Err(Status::invalid_argument(format!(
                "k={k} exceeds this coordinator's max_k={max_k}; lower k or raise --max-k"
            )));
        }
        let wire_k = u32::try_from(k)
            .map_err(|_| Status::invalid_argument("SPLADE k exceeds the wire range"))?;
        let remaining = crate::turbo_bridge::remaining_deadline_duration(finish_deadline_us)?;
        if (k == 0 || query_weights.is_empty())
            && coordinator.query_read_versions.is_none()
            && coordinator.document_visibility.is_none()
        {
            return Ok(Vec::new());
        }
        let run = coordinator.score_splade_inner(SpladeFanout {
            field,
            query_weights,
            query_l1_slack,
            k,
            wire_k,
            filter_cel,
            finish_deadline_us,
        });
        run_with_deadline(
            finish_deadline_us,
            remaining,
            "SPLADE query deadline expired",
            run,
        )
        .await
    }

    pub(super) async fn score_splade_inner(
        &self,
        query: SpladeFanout<'_>,
    ) -> Result<Vec<(u64, f32)>, Status> {
        let filters = RequestFilters::compile(&[], query.filter_cel)?;
        if let Some(fields) = &self.field_permissions {
            fields.filter(&[], filters.tree.as_ref())?;
        }
        let scope = crate::visibility::VisibilityScope::new(self.document_visibility.as_ref())?;
        let mut visibility_known = vec![false; scope.column_count()];
        let mask = self
            .document_visibility
            .is_none()
            .then(|| self.shard_mask(filters.tree.as_ref()))
            .flatten();
        let trees = self.shard_filter_trees(&filters, mask.as_ref());
        let mut known = Self::filter_known(&filters, mask.as_ref());
        let mut tasks = self.spawn_splade_tasks(&query, mask.as_ref(), &trees)?;
        let mut replies = BTreeMap::new();
        while let Some(joined) = tasks.join_next().await {
            let (shard, resp) =
                joined.map_err(|e| Status::internal(format!("SPLADE shard task failed: {e}")))?;
            replies.insert(shard, resp?);
        }
        let mut merged: Vec<(u64, f32)> = Vec::new();
        for (shard, resp) in replies {
            self.check_read_view(shard, &scope, &resp, &mut visibility_known)?;
            known.merge_shard(shard, &[], &resp.filter_columns_known)?;
            merged.extend(resp.hits.into_iter().map(|hit| (hit.doc_id, hit.score)));
        }
        self.check_visibility_columns(&visibility_known)?;
        known.refuse_unknown(&filters)?;
        merged.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        merged.truncate(query.k);
        Ok(merged)
    }

    fn spawn_splade_tasks(
        &self,
        query: &SpladeFanout<'_>,
        mask: Option<&crate::placement::ShardMask>,
        trees: &[Option<crate::pb::FilterExpr>],
    ) -> Result<tokio::task::JoinSet<SpladeReply>, Status> {
        let wire_weights: Vec<crate::pb::SpladeTermWeight> = query
            .query_weights
            .iter()
            .map(|(term, weight)| crate::pb::SpladeTermWeight {
                term: term.clone(),
                weight: *weight,
            })
            .collect();
        let mut tasks = tokio::task::JoinSet::new();
        for (shard, addr) in self.node_addrs.iter().enumerate() {
            if mask.is_some_and(|m| m.skipped[shard]) {
                continue;
            }
            let claim = self.shard_read_claim(shard)?;
            let mut link = self.node_client(addr)?;
            let mut request = Request::new(crate::pb::ScoreSpladeRequest {
                field: query.field.to_string(),
                query_weights: wire_weights.clone(),
                query_l1_slack: query.query_l1_slack,
                k: query.wire_k,
                filter: trees[shard].clone(),
                initial_floor: 0.0,
                visibility: self.document_visibility.clone(),
                expected_stats_epoch: claim.epoch,
                expected_stats_incarnation: claim.incarnation(),
            });
            if let Some(timeout) =
                effective_shard_timeout(query.finish_deadline_us, self.limits.shard_deadline)?
            {
                request.set_timeout(timeout);
            }
            tasks.spawn(async move {
                (
                    shard,
                    link.score_splade(request).await.map(|r| r.into_inner()),
                )
            });
        }
        Ok(tasks)
    }
}

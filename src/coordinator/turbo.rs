//! Turbo.

use super::*;

impl CoordinatorServiceImpl {
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
    /// Matryoshka tier stored on local shards, returning the re-sorted shortlist.
    pub fn rescore_matryoshka_shortlist(
        &self,
        candidates: &[(u64, f32)],
        query: &[f32],
        target_dim: u32,
    ) -> Result<Vec<(u64, f32)>, Status> {
        if candidates.is_empty() {
            return Ok(Vec::new());
        }
        let coordinator = self.request_snapshot().unwrap_or_else(|| self.clone());
        let candidate_ids: Vec<u64> = candidates.iter().map(|&(id, _)| id).collect();
        let mut rescored: Vec<(u64, f32)> = candidates.to_vec();
        for addr in &coordinator.node_addrs {
            match coordinator.node_client(addr)? {
                crate::link::NodeLink::Local(node) => {
                    let updated =
                        node.rescore_matryoshka_tier(target_dim, query, &candidate_ids)?;
                    for (gid, new_score) in updated {
                        if let Some(slot) = rescored.iter_mut().find(|(id, _)| *id == gid) {
                            slot.1 = new_score;
                        }
                    }
                }
                #[cfg(feature = "net")]
                crate::link::NodeLink::Remote(_) => {}
            }
        }
        rescored.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        Ok(rescored)
    }

    /// Fan out quantized SPLADE 128-doc Block-Max scoring across all unmasked
    /// local and remote shards with CEL filter admission and running top-k floor propagation.
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
        if k == 0 || query_weights.is_empty() {
            return Ok(Vec::new());
        }
        let run = coordinator.score_splade_inner(
            field,
            query_weights,
            query_l1_slack,
            k,
            wire_k,
            filter_cel,
            finish_deadline_us,
        );
        let result = match remaining {
            Some(remaining) => tokio::time::timeout(remaining, run)
                .await
                .map_err(|_| Status::deadline_exceeded("SPLADE query deadline expired"))?,
            None => run.await,
        };
        if matches!(&result, Err(status) if status.code() == tonic::Code::Cancelled)
            && crate::turbo_bridge::remaining_deadline_duration(finish_deadline_us).is_err()
        {
            return Err(Status::deadline_exceeded("SPLADE query deadline expired"));
        }
        result
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn score_splade_inner(
        &self,
        field: &str,
        query_weights: &[(String, f32)],
        query_l1_slack: f32,
        k: usize,
        wire_k: u32,
        filter_cel: &str,
        finish_deadline_us: u64,
    ) -> Result<Vec<(u64, f32)>, Status> {
        let filters = RequestFilters::compile(&[], filter_cel)?;
        let mask = self.shard_mask(filters.tree.as_ref());
        let trees = self.shard_filter_trees(&filters, mask.as_ref());
        let mut known = Self::filter_known(&filters, mask.as_ref());
        let wire_weights: Vec<crate::pb::SpladeTermWeight> = query_weights
            .iter()
            .map(|(term, weight)| crate::pb::SpladeTermWeight {
                term: term.clone(),
                weight: *weight,
            })
            .collect();
        let mut merged: Vec<(u64, f32)> = Vec::new();
        let mut running_floor: f32 = 0.0;
        for (shard, addr) in self.node_addrs.iter().enumerate() {
            if mask.as_ref().is_some_and(|m| m.skipped[shard]) {
                continue;
            }
            let mut link = self.node_client(addr)?;
            let mut request = Request::new(crate::pb::ScoreSpladeRequest {
                field: field.to_string(),
                query_weights: wire_weights.clone(),
                query_l1_slack,
                k: wire_k,
                filter: trees[shard].clone(),
                initial_floor: running_floor,
                visibility: self.document_visibility.clone(),
                expected_stats_epoch: 0,
                expected_stats_incarnation: Vec::new(),
            });
            if let Some(remaining) =
                crate::turbo_bridge::remaining_deadline_duration(finish_deadline_us)?
            {
                request.set_timeout(remaining);
            }
            let resp = link.score_splade(request).await?.into_inner();
            known.merge_shard(shard, &[], &resp.filter_columns_known)?;
            for hit in resp.hits {
                merged.push((hit.doc_id, hit.score));
            }
            merged.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            if merged.len() > k {
                merged.truncate(k);
            }
            if merged.len() == k {
                running_floor = running_floor.max(merged[k - 1].1);
            }
        }
        known.refuse_unknown(&filters)?;
        Ok(merged)
    }
}

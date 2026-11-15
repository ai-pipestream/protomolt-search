//! Composed vector routes: backend discovery and sparse scoring.

use tonic::{Request, Response, Status};

use crate::metrics::Route;

use super::fanout::{fan_out, fan_out_claimed, same_requests, shares_only};
use super::merge_util::{by_score_then_id, merge_known};
use super::{grpc_timeout, RelayService};

/// The children's vector backends as one: the descriptor and the
/// configuration must match child for child (the root's preflight
/// treats a shard's identity as generation-wide, so a relay hiding a
/// mismatch behind one answer would break that contract), rows sum
/// with checked arithmetic, and an unconfigured child refuses by name.
pub fn merge_vector_backend(
    children: &[String],
    reports: &[crate::pb::GetVectorBackendResponse],
) -> Result<crate::pb::GetVectorBackendResponse, Status> {
    let Some(first) = reports.first() else {
        return Err(Status::failed_precondition(
            "relay: a relay coordinator has no children",
        ));
    };
    let name = |shard: usize| children.get(shard).map_or("", String::as_str);
    let mut num_vectors: u64 = 0;
    for (shard, report) in reports.iter().enumerate() {
        if report.descriptor.is_none() {
            return Err(Status::failed_precondition(format!(
                "relay: child {shard} ({}) has no vector backend configured; a relay answers \
                 for configured children only",
                name(shard)
            )));
        }
        if report.descriptor != first.descriptor {
            return Err(Status::failed_precondition(format!(
                "relay: child {shard} ({}) advertises a different vector backend descriptor \
                 than child 0 ({}); a relay presents one provider identity",
                name(shard),
                name(0)
            )));
        }
        if report.config != first.config {
            return Err(Status::failed_precondition(format!(
                "relay: child {shard} ({}) runs a different vector backend configuration \
                 than child 0 ({}); a relay presents one provider identity",
                name(shard),
                name(0)
            )));
        }
        num_vectors = num_vectors.checked_add(report.num_vectors).ok_or_else(|| {
            Status::failed_precondition(format!(
                "relay: child {shard} ({}) overflows the summed vector count",
                name(shard)
            ))
        })?;
    }
    Ok(crate::pb::GetVectorBackendResponse {
        descriptor: first.descriptor.clone(),
        config: first.config.clone(),
        num_vectors,
    })
}

impl RelayService {
    /// The children's provider identity as the one shard the parent
    /// sees: the root's dense preflight asks each shard for it before a
    /// public query scores anything, so a relay answers with the
    /// descriptor and configuration its children share, rows summed,
    /// and refuses by name when a child differs.
    pub(super) async fn handle_get_vector_backend(
        &self,
        request: Request<crate::pb::GetVectorBackendRequest>,
    ) -> Result<Response<crate::pb::GetVectorBackendResponse>, Status> {
        let timeout = grpc_timeout(request.metadata());
        let (pinned, frozen) = self.pin();
        let children = frozen.node_addresses().to_vec();
        let requests = same_requests(&crate::pb::GetVectorBackendRequest {}, children.len());
        let reports = shares_only(
            fan_out(
                &frozen,
                &children,
                requests,
                timeout,
                "vector backend",
                |mut link, request| async move { link.get_vector_backend(request).await },
            )
            .await?,
        );
        let merged = merge_vector_backend(&children, &reports)?;
        self.still_current(&pinned, "GetVectorBackend")?;
        Ok(Response::new(merged))
    }

    pub(super) async fn handle_score_splade(
        &self,
        request: Request<crate::pb::ScoreSpladeRequest>,
    ) -> Result<Response<crate::pb::ScoreSpladeResponse>, Status> {
        crate::metrics::timed(Route::ScoreSplade, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            if let Some(f) = req.filter.as_ref() {
                crate::filter::validate_filter(f)?;
            }
            let opened = self.open_children(
                "ScoreSplade",
                req.visibility.as_ref(),
                req.expected_stats_epoch,
                &req.expected_stats_incarnation,
            )?;
            let shares: Vec<crate::pb::ScoreSpladeResponse> = fan_out_claimed(
                &opened,
                &req,
                |child, claim| {
                    child.expected_stats_epoch = claim.epoch;
                    child.expected_stats_incarnation = claim.incarnation();
                },
                timeout,
                "score_splade",
                |mut link, request| async move { link.score_splade(request).await },
            )
            .await?;
            let mut merged = merge_splade_shares(&req, &shares)?;
            let receipt = self.read_receipt(
                &opened.pinned,
                opened.children.clone(),
                req.visibility.as_ref(),
                Some(&opened.claims),
                &shares,
            )?;
            merged.stats_epoch = receipt.stats_epoch;
            merged.stats_incarnation = receipt.stats_incarnation;
            merged.visibility_fingerprint = receipt.visibility_fingerprint;
            merged.visibility_columns_known = receipt.visibility_columns_known;
            self.still_current(&opened.pinned, "ScoreSplade")?;
            Ok(Response::new(merged))
        })
        .await
    }
}

/// The children's sparse scores as one shard's: hits ranked score
/// descending then id ascending and cut to `k`, counters summed, the
/// filter flags ORed, and the final floor the k-th score (or the
/// request's floor when fewer than `k` matched).
fn merge_splade_shares(
    req: &crate::pb::ScoreSpladeRequest,
    shares: &[crate::pb::ScoreSpladeResponse],
) -> Result<crate::pb::ScoreSpladeResponse, Status> {
    let mut merged = crate::pb::ScoreSpladeResponse::default();
    let mut filter_known = None;
    let k = req.k as usize;
    for (shard, share) in shares.iter().enumerate() {
        merge_known(
            "filter-leaf",
            shard,
            &mut filter_known,
            &share.filter_columns_known,
        )?;
        merged.field_known |= share.field_known;
        merged.blocks_visited = merged.blocks_visited.saturating_add(share.blocks_visited);
        merged.blocks_skipped = merged.blocks_skipped.saturating_add(share.blocks_skipped);
        merged.postings_scored = merged.postings_scored.saturating_add(share.postings_scored);
        merged.hits.extend(share.hits.iter().copied());
    }
    merged
        .hits
        .sort_unstable_by(|a, b| by_score_then_id((a.score, a.doc_id), (b.score, b.doc_id)));
    if k > 0 && merged.hits.len() > k {
        merged.hits.truncate(k);
    } else if k == 0 {
        merged.hits.clear();
    }
    let floor = req.initial_floor.max(0.0);
    merged.final_floor = match merged.hits.last() {
        Some(last) if k > 0 && merged.hits.len() == k => last.score.max(floor),
        _ => floor,
    };
    merged.filter_columns_known = filter_known.unwrap_or_default();
    Ok(merged)
}

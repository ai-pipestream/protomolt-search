//! Composed vector routes: backend discovery and sparse scoring.

use tonic::{Request, Response, Status};

use crate::metrics::Route;

use super::fanout::{fan_out, fan_out_claimed, same_requests, shares_only};
use super::merge_util::{by_score_then_id, merge_known};
use super::{grpc_timeout, RelayService};

/// The children's vector backends as one: static provider identity and
/// configuration must match child for child while dynamic execution regime
/// capabilities intersect across children, rows sum with checked arithmetic,
/// and an unconfigured child refuses by name.
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
    let Some(first_desc) = first.descriptor.as_ref() else {
        return Err(unconfigured_child_error(0, name(0)));
    };
    let mut num_vectors: u64 = 0;
    let mut deleted_docs: u64 = 0;
    let mut state_hash = turbo_sha256::Sha256::new();
    state_hash.update(b"relay-vector-state-v1");
    for (shard, report) in reports.iter().enumerate() {
        check_child_backend(shard, name(shard), name(0), first_desc, first, report)?;
        num_vectors = num_vectors.checked_add(report.num_vectors).ok_or_else(|| {
            Status::failed_precondition(format!(
                "relay: child {shard} ({}) overflows the summed vector count",
                name(shard)
            ))
        })?;
        deleted_docs = deleted_docs
            .checked_add(report.deleted_docs)
            .ok_or_else(|| {
                Status::failed_precondition(format!(
                    "relay: child {shard} ({}) overflows the summed deleted_docs count",
                    name(shard)
                ))
            })?;
        fold_child_mutation_state(&mut state_hash, shard, name(shard), report)?;
    }
    let mut merged_descriptor = first_desc.clone();
    merged_descriptor.capabilities = intersect_capabilities(first_desc, reports);
    Ok(crate::pb::GetVectorBackendResponse {
        descriptor: Some(merged_descriptor),
        config: first.config.clone(),
        num_vectors,
        deleted_docs,
        wal_generation: reports.iter().map(|r| r.wal_generation).max().unwrap_or(0),
        wal_high_watermark: reports
            .iter()
            .fold(0u64, |acc, r| acc.wrapping_add(r.wal_high_watermark)),
        stats_epoch: reports
            .iter()
            .fold(0u64, |acc, r| acc.wrapping_add(r.stats_epoch)),
        stats_incarnation: state_hash.finish().0.to_vec(),
    })
}

fn unconfigured_child_error(shard: usize, child_name: &str) -> Status {
    Status::failed_precondition(format!(
        "relay: child {shard} ({child_name}) has no vector backend configured; a relay answers \
         for configured children only"
    ))
}

fn check_child_backend(
    shard: usize,
    child_name: &str,
    first_name: &str,
    first_desc: &crate::pb::VectorBackendDescriptor,
    first: &crate::pb::GetVectorBackendResponse,
    report: &crate::pb::GetVectorBackendResponse,
) -> Result<(), Status> {
    let Some(desc) = report.descriptor.as_ref() else {
        return Err(unconfigured_child_error(shard, child_name));
    };
    if !same_descriptor_identity(desc, first_desc) {
        return Err(Status::failed_precondition(format!(
            "relay: child {shard} ({child_name}) advertises a different vector backend descriptor \
             than child 0 ({first_name}); a relay presents one provider identity"
        )));
    }
    if report.config != first.config {
        return Err(Status::failed_precondition(format!(
            "relay: child {shard} ({child_name}) runs a different vector backend configuration \
             than child 0 ({first_name}); a relay presents one provider identity"
        )));
    }
    Ok(())
}

fn same_descriptor_identity(
    a: &crate::pb::VectorBackendDescriptor,
    b: &crate::pb::VectorBackendDescriptor,
) -> bool {
    let exhaustive_cap = |d: &crate::pb::VectorBackendDescriptor| {
        d.quality_contract == crate::pb::VectorQualityContract::ExhaustiveNativeScore as i32
            && d.capabilities.iter().any(|c| c == "exhaustive_completion")
    };
    a.backend_kind == b.backend_kind
        && a.backend_version == b.backend_version
        && a.dim == b.dim
        && a.bits_per_dimension == b.bits_per_dimension
        && a.metric == b.metric
        && a.score_direction == b.score_direction
        && a.scoring_fingerprint == b.scoring_fingerprint
        && a.quality_contract == b.quality_contract
        && exhaustive_cap(a) == exhaustive_cap(b)
}

fn intersect_capabilities(
    first_desc: &crate::pb::VectorBackendDescriptor,
    reports: &[crate::pb::GetVectorBackendResponse],
) -> Vec<String> {
    let mut capabilities = Vec::with_capacity(first_desc.capabilities.len());
    for cap in &first_desc.capabilities {
        let shared = !capabilities.contains(cap)
            && reports.iter().all(|r| {
                r.descriptor
                    .as_ref()
                    .is_some_and(|d| d.capabilities.contains(cap))
            });
        if shared {
            capabilities.push(cap.clone());
        }
    }
    capabilities
}

fn fold_child_mutation_state(
    hasher: &mut turbo_sha256::Sha256,
    shard: usize,
    child_name: &str,
    report: &crate::pb::GetVectorBackendResponse,
) -> Result<(), Status> {
    if report.stats_incarnation.is_empty() {
        return Err(Status::failed_precondition(format!(
            "relay: child {shard} ({child_name}) reported an empty stats_incarnation"
        )));
    }
    hasher.update(&(shard as u64).to_le_bytes());
    hasher.update(&report.num_vectors.to_le_bytes());
    hasher.update(&report.deleted_docs.to_le_bytes());
    hasher.update(&report.wal_generation.to_le_bytes());
    hasher.update(&report.wal_high_watermark.to_le_bytes());
    hasher.update(&report.stats_epoch.to_le_bytes());
    hasher.update(&(report.stats_incarnation.len() as u64).to_le_bytes());
    hasher.update(&report.stats_incarnation);
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::merge_vector_backend;
    use crate::pb::{GetVectorBackendResponse, VectorBackendDescriptor, VectorQualityContract};

    fn child_report(rows: u64, caps: &[&str]) -> GetVectorBackendResponse {
        GetVectorBackendResponse {
            descriptor: Some(VectorBackendDescriptor {
                backend_kind: "embedded-turbovec".into(),
                backend_version: "1".into(),
                dim: 32,
                bits_per_dimension: 4,
                metric: "dot".into(),
                score_direction: 1,
                scoring_fingerprint: "fp-v1".into(),
                quality_contract: VectorQualityContract::ExhaustiveNativeScore as i32,
                capabilities: caps.iter().map(|s| (*s).to_string()).collect(),
            }),
            num_vectors: rows,
            stats_incarnation: vec![1; 32],
            ..Default::default()
        }
    }

    #[test]
    fn merge_vector_backend_intersects_dynamic_capabilities_in_either_child_order() {
        let children = vec!["c0".to_string(), "c1".to_string()];
        let staged = child_report(
            40_000,
            &["batch_query", "exhaustive_completion", "staged_top_k"],
        );
        let small = child_report(2_048, &["batch_query", "exhaustive_completion"]);

        for pair in [[staged.clone(), small.clone()], [small, staged.clone()]] {
            let merged = merge_vector_backend(&children, &pair).unwrap();
            assert_eq!(merged.num_vectors, 42_048);
            assert_eq!(
                merged.descriptor.unwrap().capabilities,
                vec![
                    "batch_query".to_string(),
                    "exhaustive_completion".to_string()
                ]
            );
        }

        let missing_completion = child_report(2_048, &["batch_query"]);
        assert!(merge_vector_backend(&children, &[staged.clone(), missing_completion]).is_err());

        let mut missing_incarnation = staged.clone();
        missing_incarnation.stats_incarnation.clear();
        let err = merge_vector_backend(&children, &[staged, missing_incarnation]).unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(err.message().contains("stats_incarnation"), "{err}");
    }
}

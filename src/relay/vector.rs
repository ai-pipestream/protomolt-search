//! Composed vector routes: backend discovery, rescoring, and sparse scoring.

use std::collections::HashMap;

use tonic::{Request, Response, Status};

use crate::metrics::Route;

use super::merge_lexical::{add_count, child_ranges, merge_known, route_ids};
use super::stream_search::child_error;
use super::{fan_out, grpc_timeout, read_metadata, RelayService};

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
        let mut tasks = Vec::with_capacity(children.len());
        for (shard, addr) in children.iter().enumerate() {
            let mut link = frozen.node_client(addr)?;
            let addr = addr.clone();
            tasks.push(tokio::spawn(async move {
                let mut request = Request::new(crate::pb::GetVectorBackendRequest {});
                if let Some(timeout) = timeout {
                    request.set_timeout(timeout);
                }
                link.get_vector_backend(request)
                    .await
                    .map(|r| r.into_inner())
                    .map_err(|status| {
                        Status::new(
                            status.code(),
                            format!(
                                "relay child {shard} ({addr}) vector backend: {}",
                                status.message()
                            ),
                        )
                    })
            }));
        }
        let mut reports = Vec::with_capacity(children.len());
        for task in tasks {
            reports.push(
                task.await
                    .map_err(|e| Status::internal(format!("relay vector backend task: {e}")))??,
            );
        }
        let merged = merge_vector_backend(&children, &reports)?;
        self.still_current(&pinned, "GetVectorBackend")?;
        Ok(Response::new(merged))
    }

    pub(super) async fn handle_vector_rescore(
        &self,
        request: Request<crate::pb::VectorRescoreRequest>,
    ) -> Result<Response<crate::pb::VectorRescoreResponse>, Status> {
        crate::metrics::timed(Route::VectorRescore, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            let (pinned, frozen) = self.pin();
            let children = frozen.node_addresses().to_vec();
            if children.is_empty() {
                return Err(Status::failed_precondition(
                    "relay: a relay coordinator has no children",
                ));
            }
            crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
            let claims = self.child_claims(
                req.expected_stats_epoch,
                &req.expected_stats_incarnation,
                children.len(),
            )?;
            let reports = self.children_health(&frozen, timeout).await?;
            let ranges = child_ranges(&self.collection, &children, &reports)?;
            let by_child = route_ids(&ranges, &req.candidate_ids)?;
            let mut tasks = Vec::with_capacity(children.len());
            for (shard, ids) in by_child.into_iter().enumerate() {
                let mut link = frozen.node_client(&children[shard])?;
                let child_req = crate::pb::VectorRescoreRequest {
                    vector: req.vector.clone(),
                    candidate_ids: ids,
                    field: req.field.clone(),
                    visibility: req.visibility.clone(),
                    expected_stats_epoch: claims[shard].epoch,
                    expected_stats_incarnation: claims[shard].incarnation(),
                };
                let addr = children[shard].clone();
                tasks.push(tokio::spawn(async move {
                    let mut request = Request::new(child_req);
                    if let Some(timeout) = timeout {
                        request.set_timeout(timeout);
                    }
                    link.vector_rescore(request)
                        .await
                        .map(|r| r.into_inner())
                        .map_err(|status| child_error(shard, &addr, "vector rescore", status))
                }));
            }
            let mut receipts = Vec::with_capacity(children.len());
            let mut binding = None;
            let mut hits = Vec::new();
            for task in tasks {
                let share = task
                    .await
                    .map_err(|e| Status::internal(format!("relay vector rescore task: {e}")))??;
                crate::vector_read::check_binding(
                    &req.field,
                    share.vector_binding.as_ref(),
                    &mut binding,
                )?;
                receipts.push(read_metadata(&share));
                hits.extend(share.hits);
            }
            // Score descending, ids ascending: the order one node answers
            // in, over the union of the children's.
            hits.sort_by(|a, b| {
                b.score
                    .total_cmp(&a.score)
                    .then_with(|| a.doc_id.cmp(&b.doc_id))
            });
            self.still_current(&pinned, "VectorRescore")?;
            let receipt = self.read_receipt(
                &pinned,
                children,
                req.visibility.as_ref(),
                Some(&claims),
                &receipts,
            )?;
            Ok(Response::new(crate::pb::VectorRescoreResponse {
                hits,
                vector_binding: binding.flatten(),
                stats_epoch: receipt.stats_epoch,
                stats_incarnation: receipt.stats_incarnation,
                visibility_fingerprint: receipt.visibility_fingerprint,
                visibility_columns_known: receipt.visibility_columns_known,
            }))
        })
        .await
    }

    pub(super) async fn handle_exact_vector_rescore(
        &self,
        request: Request<crate::pb::ExactVectorRescoreRequest>,
    ) -> Result<Response<crate::pb::ExactVectorRescoreResponse>, Status> {
        crate::metrics::timed(Route::ExactVectorRescore, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            let (pinned, frozen) = self.pin();
            let children = frozen.node_addresses().to_vec();
            if children.is_empty() {
                return Err(Status::failed_precondition(
                    "relay: a relay coordinator has no children",
                ));
            }
            // The root sends every candidate to every shard and each
            // shard answers for the ids it owns; here the ids outside the
            // children's ranges are the other shards' and are ignored the
            // same way, and only a child that owns some is asked.
            crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
            let claims = self.child_claims(
                req.expected_stats_epoch,
                &req.expected_stats_incarnation,
                children.len(),
            )?;
            let reports = self.children_health(&frozen, timeout).await?;
            let ranges = child_ranges(&self.collection, &children, &reports)?;
            let mut requested = req.candidate_ids.clone();
            let mut seen = std::collections::HashSet::with_capacity(requested.len());
            requested.retain(|id| seen.insert(*id));
            let by_child = route_ids(&ranges, &requested)?;
            let mut tasks = Vec::with_capacity(children.len());
            for (shard, ids) in by_child.into_iter().enumerate() {
                let mut link = frozen.node_client(&children[shard])?;
                let child_req = crate::pb::ExactVectorRescoreRequest {
                    vector: req.vector.clone(),
                    candidate_ids: ids,
                    field: req.field.clone(),
                    visibility: req.visibility.clone(),
                    expected_stats_epoch: claims[shard].epoch,
                    expected_stats_incarnation: claims[shard].incarnation(),
                    max_logical_bytes: req.max_logical_bytes,
                };
                let addr = children[shard].clone();
                tasks.push(tokio::spawn(async move {
                    let mut request = Request::new(child_req);
                    if let Some(timeout) = timeout {
                        request.set_timeout(timeout);
                    }
                    link.exact_vector_rescore(request)
                        .await
                        .map(|r| r.into_inner())
                        .map_err(|status| child_error(shard, &addr, "exact vector rescore", status))
                }));
            }
            let mut receipts = Vec::with_capacity(children.len());
            let mut binding = None;
            let mut merged = crate::pb::ExactVectorRescoreResponse::default();
            let mut by_id = HashMap::new();
            for (i, task) in tasks.into_iter().enumerate() {
                let share = task.await.map_err(|e| {
                    Status::internal(format!("relay exact vector rescore task: {e}"))
                })??;
                crate::vector_read::check_binding(
                    &req.field,
                    share.vector_binding.as_ref(),
                    &mut binding,
                )?;
                receipts.push(read_metadata(&share));
                add_count(
                    &mut merged.logical_bytes,
                    share.logical_bytes,
                    i,
                    "logical bytes",
                )?;
                add_count(
                    &mut merged.pages_touched,
                    share.pages_touched,
                    i,
                    "page count",
                )?;
                merged.tasks = merged.tasks.saturating_add(share.tasks);
                for hit in share.hits {
                    if by_id.insert(hit.doc_id, hit).is_some() {
                        return Err(Status::failed_precondition(
                            "relay: exact vector rescore candidate answered by more than one \
                             child; slot ranges overlap",
                        ));
                    }
                }
            }
            // One row per owned id, in request order, as a node answers.
            merged.hits = requested.iter().filter_map(|id| by_id.remove(id)).collect();
            self.still_current(&pinned, "ExactVectorRescore")?;
            let receipt = self.read_receipt(
                &pinned,
                children,
                req.visibility.as_ref(),
                Some(&claims),
                &receipts,
            )?;
            merged.vector_binding = binding.flatten();
            merged.stats_epoch = receipt.stats_epoch;
            merged.stats_incarnation = receipt.stats_incarnation;
            merged.visibility_fingerprint = receipt.visibility_fingerprint;
            merged.visibility_columns_known = receipt.visibility_columns_known;
            Ok(Response::new(merged))
        })
        .await
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
            let (pinned, frozen, children, claims) = self.open_children(
                "ScoreSplade",
                req.visibility.as_ref(),
                req.expected_stats_epoch,
                &req.expected_stats_incarnation,
            )?;
            let requests = claims
                .iter()
                .map(|claim| {
                    let mut child_req = req.clone();
                    child_req.expected_stats_epoch = claim.epoch;
                    child_req.expected_stats_incarnation = claim.incarnation();
                    Some(child_req)
                })
                .collect();
            let shares: Vec<crate::pb::ScoreSpladeResponse> = fan_out(
                &frozen,
                &children,
                requests,
                timeout,
                "score_splade",
                |mut link, request| async move { link.score_splade(request).await },
            )
            .await?
            .into_iter()
            .map(|(_, share)| share)
            .collect();
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
                merged.postings_scored =
                    merged.postings_scored.saturating_add(share.postings_scored);
                merged.hits.extend(share.hits.iter().copied());
            }
            merged.hits.sort_unstable_by(|a, b| {
                b.score
                    .total_cmp(&a.score)
                    .then_with(|| a.doc_id.cmp(&b.doc_id))
            });
            if k > 0 && merged.hits.len() > k {
                merged.hits.truncate(k);
            } else if k == 0 {
                merged.hits.clear();
            }
            merged.final_floor = if k > 0 && merged.hits.len() == k {
                merged
                    .hits
                    .last()
                    .unwrap()
                    .score
                    .max(req.initial_floor.max(0.0))
            } else {
                req.initial_floor.max(0.0)
            };
            merged.filter_columns_known = filter_known.unwrap_or_default();
            let receipt = self.read_receipt(
                &pinned,
                children,
                req.visibility.as_ref(),
                Some(&claims),
                &shares,
            )?;
            merged.stats_epoch = receipt.stats_epoch;
            merged.stats_incarnation = receipt.stats_incarnation;
            merged.visibility_fingerprint = receipt.visibility_fingerprint;
            merged.visibility_columns_known = receipt.visibility_columns_known;
            self.still_current(&pinned, "ScoreSplade")?;
            Ok(Response::new(merged))
        })
        .await
    }
}

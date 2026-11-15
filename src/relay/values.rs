//! Composed value routes: documents, stored values, aggregates, and browse pages.

use tonic::{Request, Response, Status};

use crate::metrics::Route;

use super::fanout::{fan_out, fan_out_claimed, in_caller_order, read_metadata, Opened};
use super::merge_browse::merge_browse_pages;
use super::merge_util::{add_count, route_ids};
use super::merge_values::merge_aggregate_shares;
use super::{grpc_timeout, RelayService};

/// Write a child's claim into a request that carries one at the top.
macro_rules! set_claim {
    () => {
        |child, claim: &$crate::stats_identity::StatsClaim| {
            child.expected_stats_epoch = claim.epoch;
            child.expected_stats_incarnation = claim.incarnation();
        }
    };
}
pub(super) use set_claim;

impl RelayService {
    /// The relay's receipt over the children's, under the parent's claim.
    pub(super) fn opened_receipt<R: crate::visibility::ScopedReadResponse>(
        &self,
        opened: &Opened,
        visibility: Option<&crate::pb::DocumentVisibility>,
        shares: &[R],
    ) -> Result<crate::pb::VectorReadReceipt, Status> {
        self.read_receipt(
            &opened.pinned,
            opened.children.clone(),
            visibility,
            Some(&opened.claims),
            shares,
        )
    }

    pub(super) async fn handle_get_documents(
        &self,
        request: Request<crate::pb::GetDocumentsRequest>,
    ) -> Result<Response<crate::pb::GetDocumentsResponse>, Status> {
        crate::metrics::timed(Route::GetDocuments, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            let opened = self.open_children("GetDocuments", None, 0, &[])?;
            // Each id goes to the child whose slot range holds it; a
            // child asked for none is not asked.
            let ranges = self
                .ranges_of(&opened.frozen, &opened.children, timeout)
                .await?;
            let requests = route_ids(&ranges, &req.doc_ids)?
                .into_iter()
                .map(|doc_ids| {
                    (!doc_ids.is_empty()).then_some(crate::pb::GetDocumentsRequest { doc_ids })
                })
                .collect();
            let shares = fan_out(
                &opened.frozen,
                &opened.children,
                requests,
                timeout,
                "get documents",
                |mut link, request| async move { link.get_documents(request).await },
            )
            .await?;
            let documents = shares
                .into_iter()
                .flat_map(|(_, share)| share.documents)
                .collect();
            let documents = in_caller_order(&req.doc_ids, documents, |d| d.doc_id);
            self.still_current(&opened.pinned, "GetDocuments")?;
            Ok(Response::new(crate::pb::GetDocumentsResponse { documents }))
        })
        .await
    }

    pub(super) async fn handle_resolve_parents(
        &self,
        request: Request<crate::pb::ResolveParentsRequest>,
    ) -> Result<Response<crate::pb::ResolveParentsResponse>, Status> {
        crate::metrics::timed(Route::ResolveParents, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            let opened = self.open_children(
                "ResolveParents",
                req.visibility.as_ref(),
                req.expected_stats_epoch,
                &req.expected_stats_incarnation,
            )?;
            // Ids routed by slot range; every child answers, an empty
            // list included, since the receipt needs every child.
            let ranges = self
                .ranges_of(&opened.frozen, &opened.children, timeout)
                .await?;
            let requests = route_ids(&ranges, &req.doc_ids)?
                .into_iter()
                .zip(&opened.claims)
                .map(|(doc_ids, claim)| {
                    Some(crate::pb::ResolveParentsRequest {
                        doc_ids,
                        visibility: req.visibility.clone(),
                        expected_stats_epoch: claim.epoch,
                        expected_stats_incarnation: claim.incarnation(),
                        fields: req.fields.clone(),
                    })
                })
                .collect();
            let shares = fan_out(
                &opened.frozen,
                &opened.children,
                requests,
                timeout,
                "resolve parents",
                |mut link, request| async move { link.resolve_parents(request).await },
            )
            .await?;
            let mut receipts = Vec::with_capacity(shares.len());
            let mut parents = Vec::new();
            for (shard, share) in shares {
                if share.fields != req.fields {
                    return Err(Status::failed_precondition(format!(
                        "relay: child {shard} answered lineage fields {:?} for a request \
                         naming {:?}",
                        share.fields, req.fields
                    )));
                }
                receipts.push(read_metadata(&share));
                parents.extend(share.parents);
            }
            let parents = in_caller_order(&req.doc_ids, parents, |p| p.doc_id);
            let receipt = self.opened_receipt(&opened, req.visibility.as_ref(), &receipts)?;
            Ok(Response::new(crate::pb::ResolveParentsResponse {
                parents,
                stats_epoch: receipt.stats_epoch,
                stats_incarnation: receipt.stats_incarnation,
                visibility_fingerprint: receipt.visibility_fingerprint,
                visibility_columns_known: receipt.visibility_columns_known,
                fields: req.fields,
            }))
        })
        .await
    }

    pub(super) async fn handle_aggregate_shard(
        &self,
        request: Request<crate::pb::AggregateShardRequest>,
    ) -> Result<Response<crate::pb::AggregateShardResponse>, Status> {
        crate::metrics::timed(Route::AggregateShard, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            let opened = self.open_children(
                "AggregateShard",
                req.visibility.as_ref(),
                req.expected_stats_epoch,
                &req.expected_stats_incarnation,
            )?;
            // Every child folds the request as sent; an id list names
            // rows a child may not serve, which it leaves out as a node
            // leaves out an id outside its range.
            let shares: Vec<crate::pb::AggregateShardResponse> = fan_out_claimed(
                &opened,
                &req,
                set_claim!(),
                timeout,
                "aggregate",
                |mut link, request| async move { link.aggregate_shard(request).await },
            )
            .await?;
            let mut merged = merge_aggregate_shares(
                &req.aggregations,
                &req.group_by,
                req.max_groups,
                &req.histograms,
                &req.percentiles,
                &shares,
            )?;
            let receipt = self.opened_receipt(&opened, req.visibility.as_ref(), &shares)?;
            merged.stats_epoch = receipt.stats_epoch;
            merged.stats_incarnation = receipt.stats_incarnation;
            merged.visibility_fingerprint = receipt.visibility_fingerprint;
            merged.visibility_columns_known = receipt.visibility_columns_known;
            Ok(Response::new(merged))
        })
        .await
    }

    pub(super) async fn handle_quantile_counts(
        &self,
        request: Request<crate::pb::QuantileCountsRequest>,
    ) -> Result<Response<crate::pb::QuantileCountsResponse>, Status> {
        crate::metrics::timed(Route::QuantileCounts, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            let opened = self.open_children(
                "QuantileCounts",
                req.visibility.as_ref(),
                req.expected_stats_epoch,
                &req.expected_stats_incarnation,
            )?;
            // A Boolean round carries the plan's own claim, translated
            // per child as the plan route translates it.
            let plan_claims = match req.boolean.as_ref() {
                Some(plan) => Some(self.child_claims(
                    plan.expected_stats_epoch,
                    &plan.expected_stats_incarnation,
                    opened.children.len(),
                )?),
                None => None,
            };
            let requests = opened
                .claims
                .iter()
                .enumerate()
                .map(|(shard, claim)| {
                    let mut child_req = req.clone();
                    child_req.expected_stats_epoch = claim.epoch;
                    child_req.expected_stats_incarnation = claim.incarnation();
                    if let (Some(plan), Some(plan_claims)) =
                        (child_req.boolean.as_mut(), plan_claims.as_ref())
                    {
                        plan.expected_stats_epoch = plan_claims[shard].epoch;
                        plan.expected_stats_incarnation = plan_claims[shard].incarnation();
                    }
                    Some(child_req)
                })
                .collect();
            let shares = super::fanout::shares_only(
                fan_out(
                    &opened.frozen,
                    &opened.children,
                    requests,
                    timeout,
                    "quantile counts",
                    |mut link, request| async move { link.quantile_counts(request).await },
                )
                .await?,
            );
            let counts = sum_quantile_counts(req.targets.len(), &shares)?;
            let receipt = self.opened_receipt(&opened, req.visibility.as_ref(), &shares)?;
            Ok(Response::new(crate::pb::QuantileCountsResponse {
                counts,
                stats_epoch: receipt.stats_epoch,
                stats_incarnation: receipt.stats_incarnation,
                visibility_fingerprint: receipt.visibility_fingerprint,
                visibility_columns_known: receipt.visibility_columns_known,
            }))
        })
        .await
    }

    pub(super) async fn handle_browse_shard(
        &self,
        request: Request<crate::pb::BrowseShardRequest>,
    ) -> Result<Response<crate::pb::BrowseShardResponse>, Status> {
        crate::metrics::timed(Route::BrowseShard, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            if req.k == 0 {
                return Err(Status::invalid_argument("browse requires k > 0"));
            }
            for sort in &req.sort {
                crate::sortkeys::target_field(&sort.column, sort.map.as_ref())?;
            }
            let opened = self.open_children(
                "BrowseShard",
                req.visibility.as_ref(),
                req.expected_stats_epoch,
                &req.expected_stats_incarnation,
            )?;
            // The boundary is a row's keys and id, which each child
            // applies to its rows as sent.
            let shares: Vec<crate::pb::BrowseShardResponse> = fan_out_claimed(
                &opened,
                &req,
                set_claim!(),
                timeout,
                "browse",
                |mut link, request| async move { link.browse_shard(request).await },
            )
            .await?;
            let mut merged = merge_browse_pages(&req, &shares)?;
            let receipt = self.opened_receipt(&opened, req.visibility.as_ref(), &shares)?;
            merged.stats_epoch = receipt.stats_epoch;
            merged.stats_incarnation = receipt.stats_incarnation;
            merged.visibility_fingerprint = receipt.visibility_fingerprint;
            merged.visibility_columns_known = receipt.visibility_columns_known;
            Ok(Response::new(merged))
        })
        .await
    }
}

/// The per-target counts summed across children, each child sized as
/// the request's targets.
fn sum_quantile_counts(
    targets: usize,
    shares: &[crate::pb::QuantileCountsResponse],
) -> Result<Vec<u64>, Status> {
    let mut counts = vec![0u64; targets];
    for (shard, share) in shares.iter().enumerate() {
        if share.counts.len() != counts.len() {
            return Err(Status::internal(format!(
                "relay: child {shard} answered {} quantile counts for {} targets",
                share.counts.len(),
                counts.len()
            )));
        }
        for (total, &count) in counts.iter_mut().zip(&share.counts) {
            add_count(total, count, shard, "quantile count")?;
        }
    }
    Ok(counts)
}

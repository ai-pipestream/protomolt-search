//! The composed `FetchValues` route: ids routed by slot range, stored
//! values and identities merged, and one read receipt.

use std::collections::HashSet;

use tonic::{Request, Response, Status};

use crate::metrics::Route;

use super::fanout::{fan_out, in_caller_order, read_metadata};
use super::merge_util::{child_of_id, merge_known, route_ids};
use super::{grpc_timeout, RelayService};

impl RelayService {
    pub(super) async fn handle_fetch_values(
        &self,
        request: Request<crate::pb::FetchValuesRequest>,
    ) -> Result<Response<crate::pb::FetchValuesResponse>, Status> {
        crate::metrics::timed(Route::FetchValues, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            if req.include_identities && req.candidate_ids.len() > 1_000_000 {
                return Err(Status::resource_exhausted(
                    "identity fetch exceeds 1000000 input IDs",
                ));
            }
            let opened = self.open_children(
                "FetchValues",
                req.visibility.as_ref(),
                req.expected_stats_epoch,
                &req.expected_stats_incarnation,
            )?;
            // Ids routed by slot range; every child answers, since the
            // known flags the root's typo rule runs on come from every
            // shard, candidates or none.
            let ranges = self
                .ranges_of(&opened.frozen, &opened.children, timeout)
                .await?;
            let requests = route_ids(&ranges, &req.candidate_ids)?
                .into_iter()
                .zip(&opened.claims)
                .map(|(candidate_ids, claim)| {
                    Some(crate::pb::FetchValuesRequest {
                        candidate_ids,
                        include_identities: req.include_identities,
                        projections: req.projections.clone(),
                        stages: req.stages.clone(),
                        visibility: req.visibility.clone(),
                        expected_stats_epoch: claim.epoch,
                        expected_stats_incarnation: claim.incarnation(),
                    })
                })
                .collect();
            let shares = fan_out(
                &opened.frozen,
                &opened.children,
                requests,
                timeout,
                "fetch values",
                |mut link, request| async move { link.fetch_values(request).await },
            )
            .await?;
            let mut merged = merge_fetch_shares(&req, &ranges, shares)?;
            let receipt =
                self.opened_receipt(&opened, req.visibility.as_ref(), &merged.receipts)?;
            merged.response.visibility_fingerprint = receipt.visibility_fingerprint;
            merged.response.visibility_columns_known = receipt.visibility_columns_known;
            merged.response.stats_epoch = receipt.stats_epoch;
            merged.response.stats_incarnation = receipt.stats_incarnation;
            Ok(Response::new(merged.response))
        })
        .await
    }
}

/// The fetched values as one answer, and the children's receipts.
struct FetchMerge {
    response: crate::pb::FetchValuesResponse,
    receipts: Vec<crate::pb::VectorReadReceipt>,
}

/// The children's fetches merged: identity certificates checked, every
/// identity one the child owns and was asked for, the stage and
/// projection flags ORed, the projection types agreeing, and the rows
/// put back in the caller's order. The receipt fields are the caller's.
fn merge_fetch_shares(
    req: &crate::pb::FetchValuesRequest,
    ranges: &[(u64, u64)],
    shares: Vec<(usize, crate::pb::FetchValuesResponse)>,
) -> Result<FetchMerge, Status> {
    let mut receipts = Vec::with_capacity(shares.len());
    let mut stage_known: Option<Vec<bool>> = None;
    let mut projection_known: Option<Vec<bool>> = None;
    let mut projection_types = vec![crate::pb::ScalarValueType::Unspecified; req.projections.len()];
    let mut rows = Vec::new();
    let mut identities = Vec::new();
    let mut checker = IdentityCheck::new(req, ranges);
    for (shard, share) in shares {
        crate::values::validate_map_fetch_contract(
            &req.projections,
            share.typed_map_contract_version,
        )?;
        checker.check(shard, &share)?;
        merge_known(
            "stage-column",
            shard,
            &mut stage_known,
            &share.stage_columns_known,
        )?;
        merge_known(
            "projection-leaf",
            shard,
            &mut projection_known,
            &share.projection_leaves_known,
        )?;
        crate::values::merge_projection_types(
            &req.projections,
            &mut projection_types,
            &share.projection_types,
        )?;
        receipts.push(read_metadata(&share));
        rows.extend(share.rows);
        identities.extend(share.identities);
    }
    let response = crate::pb::FetchValuesResponse {
        typed_map_contract_version: crate::values::TYPED_MAP_FETCH_VERSION,
        rows: in_caller_order(&req.candidate_ids, rows, |r| r.doc_id),
        identities,
        identities_included: req.include_identities,
        stage_columns_known: stage_known.unwrap_or_default(),
        projection_leaves_known: projection_known.unwrap_or_default(),
        projection_types: projection_types.into_iter().map(i32::from).collect(),
        ..Default::default()
    };
    Ok(FetchMerge { response, receipts })
}

/// The identity certificate rule of a fetch: each child's answer must
/// match the request's `include_identities`, and every identity must be
/// one requested, owned by that child, and seen once, within the byte
/// budget.
struct IdentityCheck<'a> {
    include: bool,
    requested: HashSet<u64>,
    ranges: &'a [(u64, u64)],
    seen: HashSet<u64>,
    bytes: usize,
}

impl<'a> IdentityCheck<'a> {
    fn new(req: &crate::pb::FetchValuesRequest, ranges: &'a [(u64, u64)]) -> Self {
        IdentityCheck {
            include: req.include_identities,
            requested: req.candidate_ids.iter().copied().collect(),
            ranges,
            seen: HashSet::new(),
            bytes: 0,
        }
    }

    fn check(
        &mut self,
        shard: usize,
        share: &crate::pb::FetchValuesResponse,
    ) -> Result<(), Status> {
        if share.identities_included != self.include
            || (!self.include && !share.identities.is_empty())
        {
            return Err(Status::failed_precondition(
                "child omitted or changed identity fetch certificate",
            ));
        }
        for row in &share.identities {
            if !self.requested.contains(&row.doc_id)
                || child_of_id(self.ranges, row.doc_id) != Some(shard)
                || !self.seen.insert(row.doc_id)
            {
                return Err(Status::failed_precondition(
                    "child returned an unrequested or duplicate identity",
                ));
            }
            crate::query_identity::charge_candidate_identity(row, &mut self.bytes)?;
        }
        Ok(())
    }
}

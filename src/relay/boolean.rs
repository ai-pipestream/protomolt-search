//! The composed Boolean evaluation and the aggregate it may carry.

use tonic::{Request, Response, Status};

use crate::metrics::Route;
use crate::pb::{BooleanShardRequest, BooleanShardResponse};

use super::fanout::{fan_out_claimed, Opened};
use super::merge_lexical::merge_boolean_responses;
use super::merge_values::merge_aggregate_shares;
use super::values::set_claim;
use super::{grpc_timeout, RelayService};

/// Each child's Boolean read receipt, which must be present and agree
/// with the epoch the child answered.
fn boolean_receipts(
    shares: &[BooleanShardResponse],
) -> Result<Vec<crate::pb::VectorReadReceipt>, Status> {
    shares
        .iter()
        .map(|share| {
            let receipt = share.read_receipt.clone().ok_or_else(|| {
                Status::failed_precondition("relay child omitted Boolean read receipt")
            })?;
            if receipt.stats_epoch != share.stats_epoch {
                return Err(Status::failed_precondition(
                    "Boolean receipt epoch mismatch",
                ));
            }
            Ok(receipt)
        })
        .collect()
}

/// The children's bindings for every dense leaf of the plan, which must
/// all name the same vector field binding.
fn dense_binding(
    req: &BooleanShardRequest,
    receipts: &[crate::pb::VectorReadReceipt],
) -> Result<Option<crate::pb::MappedVectorBinding>, Status> {
    let mut binding = None;
    for leaf in &req.leaves {
        if let Some(crate::pb::boolean_plan_leaf::Leaf::Dense(dense)) = leaf.leaf.as_ref() {
            for child in receipts {
                let bound = child.vector_binding.as_ref();
                crate::vector_read::check_binding(&dense.field, bound, &mut binding)?;
            }
        }
    }
    Ok(binding.flatten())
}

impl RelayService {
    /// One shard-side Boolean evaluation over the children: the parent's
    /// stats claim translates per child, each child answers its best
    /// `depth`, and the merge is the ranked union cut to `depth` with the
    /// counts summed and the known flags joined.
    pub(super) async fn handle_evaluate_boolean(
        &self,
        request: Request<BooleanShardRequest>,
    ) -> Result<Response<BooleanShardResponse>, Status> {
        crate::metrics::timed(Route::EvaluateBoolean, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            if req.depth == 0 {
                return Err(Status::invalid_argument(
                    "EvaluateBoolean: depth 0 asks for no candidates",
                ));
            }
            let opened = self.open_children(
                "EvaluateBoolean",
                None,
                req.expected_stats_epoch,
                &req.expected_stats_incarnation,
            )?;
            let shares: Vec<BooleanShardResponse> = fan_out_claimed(
                &opened,
                &req,
                set_claim!(),
                timeout,
                "boolean evaluation",
                |mut link, request| async move { link.evaluate_boolean(request).await },
            )
            .await?;
            let mut merged = merge_boolean_responses(&req, &shares)?;
            self.still_current(&opened.pinned, "EvaluateBoolean")?;
            let receipts = boolean_receipts(&shares)?;
            let mut receipt = self.opened_receipt(&opened, req.visibility.as_ref(), &receipts)?;
            receipt.vector_binding = dense_binding(&req, &receipts)?;
            merged.stats_epoch = receipt.stats_epoch;
            merged.read_receipt = Some(receipt);
            if let Some(spec) = req.aggregate.as_ref() {
                merged.aggregate = Some(self.boolean_aggregate(&opened, &req, spec, &shares)?);
            }
            Ok(Response::new(merged))
        })
        .await
    }

    /// The root aggregate each child folded over its match set, folded
    /// in child order, with a receipt of its own the root checks as it
    /// checks a shard's. An aggregate fold is the root's order, so a
    /// child that answered none for a plan that asked refuses by name.
    fn boolean_aggregate(
        &self,
        opened: &Opened,
        req: &BooleanShardRequest,
        spec: &crate::pb::BooleanShardAggregate,
        shares: &[BooleanShardResponse],
    ) -> Result<crate::pb::AggregateShardResponse, Status> {
        let folds = shares
            .iter()
            .enumerate()
            .map(|(shard, share)| {
                share.aggregate.clone().ok_or_else(|| {
                    Status::failed_precondition(format!(
                        "relay: child {shard} answered no aggregate fold for a Boolean \
                         plan that asked for one"
                    ))
                })
            })
            .collect::<Result<Vec<_>, Status>>()?;
        let mut fold = merge_aggregate_shares(
            &spec.aggregations,
            &spec.group_by,
            spec.max_groups,
            &spec.histograms,
            &spec.percentiles,
            &folds,
        )?;
        let receipt = self.read_receipt(
            &opened.pinned,
            self.children(),
            req.visibility.as_ref(),
            Some(&opened.claims),
            &folds,
        )?;
        fold.stats_epoch = receipt.stats_epoch;
        fold.stats_incarnation = receipt.stats_incarnation;
        fold.visibility_fingerprint = receipt.visibility_fingerprint;
        fold.visibility_columns_known = receipt.visibility_columns_known;
        Ok(fold)
    }
}

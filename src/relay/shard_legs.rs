//! The hybrid legs composed over a relay's children.

use tonic::{Request, Response, Status};

use crate::metrics::Route;
use crate::pb::{ShardLegsRequest, ShardLegsResponse};
use crate::stats_identity::StatsClaim;

use super::fanout::{fan_out, shares_only, Opened};
use super::merge_util::merge_known;
use super::{grpc_timeout, RelayService};

type Binding = Option<Option<crate::pb::MappedVectorBinding>>;

impl RelayService {
    pub(super) async fn handle_hybrid_shard(
        &self,
        _request: Request<crate::pb::HybridShardRequest>,
    ) -> Result<Response<crate::pb::HybridShardResponse>, Status> {
        Err(Status::unimplemented(
            "relay: NodeService.HybridShard is not served by a relay coordinator: two-level \
             fusion fuses one list per shard at the root and is partition dependent by \
             design, so a relay standing as one shard would change the answer; use the \
             fused routes (ShardLegs, the cascade) through a relay",
        ))
    }

    pub(super) async fn handle_shard_legs(
        &self,
        request: Request<ShardLegsRequest>,
    ) -> Result<Response<ShardLegsResponse>, Status> {
        crate::metrics::timed(Route::ShardLegs, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            let opened = self.open_children(
                "ShardLegs",
                None,
                req.expected_stats_epoch,
                &req.expected_stats_incarnation,
            )?;
            let read_claims = req
                .read_context
                .as_ref()
                .map(|context| {
                    crate::visibility::VisibilityScope::new(context.visibility.as_ref())?;
                    self.child_claims(
                        context.expected_stats_epoch,
                        &context.expected_stats_incarnation,
                        opened.children.len(),
                    )
                })
                .transpose()?;
            let shares = shard_legs_shares(&opened, &req, read_claims.as_deref(), timeout).await?;
            let (mut merged, receipts, binding) = merge_leg_shares(&req, shares)?;
            if let Some(context) = &req.read_context {
                let mut receipt = self.read_receipt(
                    &opened.pinned,
                    opened.children.clone(),
                    context.visibility.as_ref(),
                    read_claims.as_deref(),
                    &receipts,
                )?;
                receipt.vector_binding = binding.flatten();
                merged.read_receipt = Some(receipt);
            }
            self.still_current(&opened.pinned, "ShardLegs")?;
            Ok(Response::new(merged))
        })
        .await
    }
}

/// Every child's legs, each request carrying the child's stats claim
/// and, under a read context, its read claim.
async fn shard_legs_shares(
    opened: &Opened,
    req: &ShardLegsRequest,
    read_claims: Option<&[StatsClaim]>,
    timeout: Option<std::time::Duration>,
) -> Result<Vec<ShardLegsResponse>, Status> {
    let requests = opened
        .claims
        .iter()
        .enumerate()
        .map(|(shard, claim)| {
            let mut child = req.clone();
            child.expected_stats_epoch = claim.epoch;
            child.expected_stats_incarnation = claim.incarnation();
            if let (Some(context), Some(read)) = (&mut child.read_context, read_claims) {
                context.expected_stats_epoch = read[shard].epoch;
                context.expected_stats_incarnation = read[shard].incarnation();
            }
            Some(child)
        })
        .collect();
    let pairs = fan_out(
        &opened.frozen,
        &opened.children,
        requests,
        timeout,
        "shard legs",
        |mut link, request| async move { link.shard_legs(request).await },
    )
    .await?;
    Ok(shares_only(pairs))
}

/// Raw per-leg lists: the parent merges them by score across its shards,
/// and the union of the children's lists holds the subtree's top of each
/// leg. Competition ranks at the parent make the order of equal scores
/// immaterial. Under a read context every child must answer a receipt
/// bound to the same vector field; without one, none may.
fn merge_leg_shares(
    req: &ShardLegsRequest,
    shares: Vec<ShardLegsResponse>,
) -> Result<
    (
        ShardLegsResponse,
        Vec<crate::pb::VectorReadReceipt>,
        Binding,
    ),
    Status,
> {
    let mut merged = ShardLegsResponse::default();
    let mut geo_known = None;
    let mut filter_known = None;
    let mut receipts = Vec::with_capacity(shares.len());
    let mut binding = None;
    for (shard, share) in shares.into_iter().enumerate() {
        match (&req.read_context, share.read_receipt.as_ref()) {
            (Some(context), Some(receipt)) => {
                let bound = receipt.vector_binding.as_ref();
                crate::vector_read::check_binding(&context.field, bound, &mut binding)?;
                receipts.push(receipt.clone());
            }
            (None, None) => {}
            _ => {
                return Err(Status::failed_precondition(
                    "relay: hybrid leg read receipt mismatch",
                ))
            }
        }
        merge_known(
            "geo-column",
            shard,
            &mut geo_known,
            &share.geo_columns_known,
        )?;
        merge_known(
            "filter-leaf",
            shard,
            &mut filter_known,
            &share.filter_columns_known,
        )?;
        merged.vector_hits.extend(share.vector_hits);
        merged.bm25_hits.extend(share.bm25_hits);
    }
    merged.geo_columns_known = geo_known.unwrap_or_default();
    merged.filter_columns_known = filter_known.unwrap_or_default();
    Ok((merged, receipts, binding))
}
